import json
import os
from datetime import timezone, datetime, timedelta
from typing import Any

from kafka import KafkaConsumer, KafkaProducer, JsonSerializer

TAPPER_COMMANDS_TOPIC = "tappers-commands"
TAPPER_MESSAGES_TOPIC = "tappers-messages"

SEALER_COMMANDS_TOPIC = "sealers-commands"
SEALER_MESSAGES_TOPIC = "sealers-messages"

MACHINES_BY_TYPE: dict[str, list[int]] = {
    "SEALER": [1],
    "TAPPER": [1, 2, 3]
}

PCS_LIFETIME = 30
LOG_FILE = os.getenv("LOG_FILE", "logs/pcs.log")

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

def send_command_to_all_machines(producer: KafkaProducer, machine_ids_by_type: dict[str, list[int]], command: str):
    for machine_type, machine_ids in machine_ids_by_type.items():
        topic: str

        match machine_type:
            case "TAPPER":
                topic = TAPPER_COMMANDS_TOPIC
            case "SEALER":
                topic = SEALER_COMMANDS_TOPIC
            case _:
                print("SEND_COMMAND_TO_ALL_MACHINES", "Unknown machine type: ", machine_type)
                continue

        for machine_id in machine_ids:
            producer.send(topic, make_command(machine_id, command))


def send_command_to_machine(producer: KafkaProducer, machine_id: int, machine_type: str, command: str):
    match machine_type:
        case "SEALER":
            producer.send(SEALER_COMMANDS_TOPIC, make_command(machine_id, command))
        case "TAPPER":
            producer.send(TAPPER_COMMANDS_TOPIC, make_command(machine_id, command))
        case _:
            print("SEND_COMMAND_TO_MACHINE", "Unknown machine type: ", machine_type)

def send_command_to_machine_type(producer: KafkaProducer, machine_ids_in_type: list[int], machine_type: str, command: str):
    topic: str

    match machine_type:
        case "TAPPER":
            topic = TAPPER_COMMANDS_TOPIC
        case "SEALER":
            topic = SEALER_COMMANDS_TOPIC
        case _:
            print("SEND_COMMAND_TO_ALL_MACHINES", "Unknown machine type: ", machine_type)
            return

    for machine_id in machine_ids_in_type:
        producer.send(topic, make_command(machine_id, command))

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
    send_command_to_all_machines(producer, MACHINES_BY_TYPE, "START")

    # Init state variables
    time_start = datetime.now(timezone.utc)
    overloaded_machine = 0
    overloaded = False

    write_log_entry("pcs listening for messages")
    for consumerRecord in consumer:
        message = consumerRecord.value

        match message["payload"]["status"]:
            case "IDLE":
                continue

            case "RUNNING":
                if overloaded and overloaded_machine == message["machineId"]:
                    write_log_entry("Overload stopped")
                    overloaded = False
                    overloaded_machine = 0
                    send_command_to_machine_type(producer, MACHINES_BY_TYPE["TAPPER"], "TAPPER", "START")
                    write_log_entry("Restarted machines after overload")

            case "ERROR":
                continue

            case "OVERLOADED":
                # Ignore duplicate message from same machine
                #if overloaded and overloaded_machine == message["machineId"]:
                #    continue

                write_log_entry("Overload received")
                overloaded = True
                overloaded_machine = message["machineId"]
                send_command_to_machine_type(producer, MACHINES_BY_TYPE["TAPPER"], "TAPPER", "STOP")
                write_log_entry("Overload handled")

        # Stop the experiment after the defined lifetime
        if datetime.now(timezone.utc) - time_start > timedelta(seconds=PCS_LIFETIME):
            write_log_entry("Timeout reached")
            send_command_to_all_machines(producer, MACHINES_BY_TYPE, "STOP")
            write_log_entry("Stopped machines")
            break

    write_log_entry("pcs finished")

if __name__ == '__main__':
    main()