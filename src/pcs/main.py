import json
import os
from datetime import timezone, datetime, timedelta
from typing import Any

import redis
from kafka import KafkaConsumer, KafkaProducer, JsonSerializer

TAPPER_COMMANDS_TOPIC = "tappers-commands"
TAPPER_MESSAGES_TOPIC = "tappers-messages"

SEALER_COMMANDS_TOPIC = "sealers-commands"
SEALER_MESSAGES_TOPIC = "sealers-messages"

PCS_LIFETIME = 30000
LOG_FILE = os.getenv("LOG_FILE", "logs/pcs.log")

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))

redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

def get_machines_on_line(line_id: str, machine_type: str | None) -> list[str]:
    """Fetch the list of machines on a specific line and of a specific type from Redis."""
    key_suffix = machine_type if machine_type else "machines"
    return redis_client.zrange(f"line:{line_id}:{key_suffix}", 0, -1)

def get_machine_config(machine_id: int | str, machine_type: str) -> dict[str, str]:
    """Fetch the configuration for a specific machine from Redis."""
    return redis_client.hgetall(f"{machine_type}:{machine_id}")

def get_rule(rule_id: str) -> dict[str, str]:
    """Fetch a specific rule from Redis."""
    return redis_client.hgetall(f"rule:{rule_id}")

def utc_timestamp() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace(
        "+00:00", "Z"
    )

def write_log_entry(entry: Any) -> None:
    """Append a timestamped JSON entry to the local log file."""
    directory = os.path.dirname(LOG_FILE)
    if directory:
        os.makedirs(directory, exist_ok=True)

    with open(LOG_FILE, "a", encoding="utf-8") as file:
        file.write(f"{utc_timestamp()} {json.dumps(entry)}\n")

def make_command(machine_id: int, command: str) -> dict[str, Any]:
    return {
        "targetMachine": machine_id,
        "timestamp": utc_timestamp(),
        "command": command,
    }

def send_command_to_all_machines(producer: KafkaProducer, command: str):
    for machine_member in get_machines_on_line("A", None):
        try:
            machine_type, machine_id = machine_member.rsplit(":", 1)
            numeric_machine_id = int(machine_id)
        except ValueError:
            write_log_entry(f"Ignoring invalid Redis machine entry: {machine_member}")
            continue

        send_command_to_machine(
            producer, numeric_machine_id, machine_type, command
        )


def send_command_to_machine(producer: KafkaProducer, machine_id: int, machine_type: str, command: str):
    match machine_type:
        case "SEALER":
            producer.send(SEALER_COMMANDS_TOPIC, make_command(machine_id, command))
        case "TAPPER":
            producer.send(TAPPER_COMMANDS_TOPIC, make_command(machine_id, command))
        case _:
            print("SEND_COMMAND_TO_MACHINE", "Unknown machine type: ", machine_type)

def send_command_to_machine_type(producer: KafkaProducer, machine_members: list[str], machine_type: str, command: str):
    topic: str

    match machine_type:
        case "TAPPER":
            topic = TAPPER_COMMANDS_TOPIC
        case "SEALER":
            topic = SEALER_COMMANDS_TOPIC
        case _:
            print("SEND_COMMAND_TO_ALL_MACHINES", "Unknown machine type: ", machine_type)
            return

    for machine_member in machine_members:
        try:
            member_type, machine_id = machine_member.rsplit(":", 1)
            if member_type != machine_type:
                raise ValueError
            numeric_machine_id = int(machine_id)
        except ValueError:
            write_log_entry(f"Ignoring invalid Redis machine entry: {machine_member}")
            continue

        producer.send(topic, make_command(numeric_machine_id, command))

def main() :
    write_log_entry("pcs started")

    producer = KafkaProducer(
        bootstrap_servers='kafka:9092',
        value_serializer=JsonSerializer()
    )

    consumer = KafkaConsumer(
        bootstrap_servers='kafka:9092',
        auto_offset_reset="latest",
        enable_auto_commit=False,
        value_deserializer=JsonSerializer()
    )
    consumer.subscribe([TAPPER_MESSAGES_TOPIC, SEALER_MESSAGES_TOPIC])

    # Start all machines
    write_log_entry(f"pcs starting machines")
    send_command_to_all_machines(producer, "START")

    # Init state variables
    time_start = datetime.now(timezone.utc)
    overloaded_machine = 0
    overloaded_tappers: list[str] = []

    write_log_entry("pcs listening for messages")
    for consumerRecord in consumer:
        message = consumerRecord.value

        match message["payload"]["status"]:
            case "IDLE":
                continue

            case "RUNNING":
                if overloaded_machine == message["machineId"]:
                    write_log_entry("Overload stopped")
                    send_command_to_machine_type(
                        producer, overloaded_tappers, "TAPPER", "START"
                    )
                    overloaded_machine = 0
                    overloaded_tappers = []
                    write_log_entry("Restarted machines after overload")

            case "ERROR":
                continue

            case "OVERLOADED":
                # if overloaded_machine == message["machineId"]:
                #    continue

                write_log_entry("Overload received")
                machine_id = message["machineId"]
                line_id = get_machine_config(machine_id, "SEALER").get("line_id")
                if not line_id:
                    write_log_entry(f"Overloaded machine {machine_id} has no line_id in config")
                    continue

                rule = get_rule("overload")
                if rule.get("scope") != "same_line" or rule.get("target") != "TAPPER":
                    write_log_entry(f"Unsupported overload rule: {rule}")
                    continue
                if rule.get("action") != "STOP":
                    write_log_entry(f"Unsupported overload action: {rule.get('action')}")
                    continue

                overloaded_tappers = get_machines_on_line(line_id, "TAPPER")
                overloaded_machine = machine_id
                send_command_to_machine_type(
                    producer, overloaded_tappers, "TAPPER", rule["action"]
                )
                write_log_entry("Overload handled")

        # Stop the experiment after the defined lifetime
        if datetime.now(timezone.utc) - time_start > timedelta(seconds=PCS_LIFETIME):
            write_log_entry("Timeout reached")
            send_command_to_all_machines(producer, "STOP")
            write_log_entry("Stopped machines")
            break

    write_log_entry("pcs finished")

if __name__ == '__main__':
    main()