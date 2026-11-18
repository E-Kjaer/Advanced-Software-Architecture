import json
import os
import random
import threading
from datetime import datetime, timezone
from typing import Any

from kafka import KafkaConsumer, KafkaProducer, JsonSerializer

BOOTSTRAP_SERVERS = "kafka:9092"
COMMANDS_TOPIC = "sealers-commands"
MESSAGES_TOPIC = "sealers-messages"
COMMANDS_FILE = os.getenv("COMMANDS_FILE", "command-logs/commands.log")
MACHINE_ID = int(os.getenv("MACHINE_ID", 1))
GROUP_ID = f"sealer_{MACHINE_ID}"
POLL_INTERVAL_MS = 10
HEARTBEAT_INTERVAL_MS = 100
OVERLOAD_MIN_DELAY_S = 5.0
OVERLOAD_MAX_DELAY_S = 15.0
OVERLOAD_DURATION_MIN_S = 5.0
OVERLOAD_DURATION_MAX_S = 15.0
RUNNING_STATUS = "RUNNING"
OVERLOAD_STATUS = "OVERLOADED"


def utc_timestamp() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace(
        "+00:00", "Z"
    )


def make_event(machine_id: str | int, event_type: str, payload: dict[str, Any]) -> dict[str, Any]:
    return {
        "machineId": machine_id,
        "eventType": event_type,
        "timestamp": utc_timestamp(),
        "payload": payload,
    }


class SealerStatus:
    def __init__(self, initial: str = "IDLE") -> None:
        self._lock = threading.Lock()
        self._value = initial

    @property
    def value(self) -> str:
        with self._lock:
            return self._value

    def update(self, value: str) -> None:
        with self._lock:
            self._value = value


def write_command(command: Any) -> None:
    """Append a received command and its receive timestamp to the command log."""
    directory = os.path.dirname(COMMANDS_FILE)
    if directory:
        os.makedirs(directory, exist_ok=True)

    with open(COMMANDS_FILE, "a", encoding="utf-8") as file:
        file.write(f"{utc_timestamp()} {json.dumps(command)}\n")


def is_targeted(command: Any) -> bool:
    """Return True when the command is addressed to this sealer."""
    return isinstance(command, dict) and command.get("targetMachine") == MACHINE_ID


def handle_command(command: dict[str, Any]) -> str | None:
    """Handle a START or STOP command and return the resulting sealer status."""
    if not isinstance(command, dict):
        print(f"Ignoring invalid command: {command!r}")
        return None

    command_type = command.get("command")

    if command_type == "START":
        print(f"Sealer {MACHINE_ID} starting")
        return RUNNING_STATUS

    if command_type == "STOP":
        print(f"Sealer {MACHINE_ID} stopping")
        return "IDLE"

    print(f"Ignoring unknown command: {command_type!r}")
    return None


def consume_commands(stop_event: threading.Event, status: SealerStatus) -> None:
    consumer = KafkaConsumer(
        COMMANDS_TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id=GROUP_ID,
        auto_offset_reset="latest",
        enable_auto_commit=False,
        value_deserializer=JsonSerializer(),
    )

    try:
        while not stop_event.is_set():
            records = consumer.poll(timeout_ms=POLL_INTERVAL_MS)

            for topic_partition, messages in records.items():
                for message in messages:
                    command = message.value

                    if not is_targeted(command):
                        print(f"Ignoring command for another machine: {command}")
                        continue

                    print(f"Received command on {topic_partition}: {command}")

                    write_command(command)

                    new_status = handle_command(command)
                    if new_status is not None:
                        status.update(new_status)

            if records:
                consumer.commit()
    finally:
        consumer.close()
        print("Command consumer stopped")


def send_heartbeats(
    producer: KafkaProducer,
    status: SealerStatus,
    stop_event: threading.Event,
) -> None:
    while not stop_event.wait(HEARTBEAT_INTERVAL_MS / 1000):
        producer.send(
            MESSAGES_TOPIC,
            make_event(MACHINE_ID, "STATUS", {"status": status.value}),
        )


def simulate_overload(
    producer: KafkaProducer,
    status: SealerStatus,
    stop_event: threading.Event,
) -> None:
    overload_delay = random.uniform(OVERLOAD_MIN_DELAY_S, OVERLOAD_MAX_DELAY_S)
    print(f"Simulating overload in {overload_delay:.1f} seconds")

    if stop_event.wait(overload_delay):
        return

    status.update(OVERLOAD_STATUS)
    producer.send(
        MESSAGES_TOPIC,
        make_event(MACHINE_ID, "STATUS", {"status": OVERLOAD_STATUS}),
    )
    print(f"Sealer {MACHINE_ID} overloaded")

    overload_duration = random.uniform(
        OVERLOAD_DURATION_MIN_S, OVERLOAD_DURATION_MAX_S
    )
    print(f"Recovering from overload in {overload_duration:.1f} seconds")

    if stop_event.wait(overload_duration):
        return

    if status.value != OVERLOAD_STATUS:
        print("Overload cleared by a command; skipping recovery")
        return

    status.update(RUNNING_STATUS)
    producer.send(
        MESSAGES_TOPIC,
        make_event(MACHINE_ID, "STATUS", {"status": RUNNING_STATUS}),
    )
    print(f"Sealer {MACHINE_ID} running")


def main():
    print(f"Sealer '{MACHINE_ID}' is operational")
    print(f"Listening for commands on '{COMMANDS_TOPIC}'")
    print(
        f"Publishing heartbeats to '{MESSAGES_TOPIC}' "
        f"every {HEARTBEAT_INTERVAL_MS} ms"
    )
    print(f"Logging commands to '{COMMANDS_FILE}'")
    print(
        "Simulating an overload after a random delay of "
        f"{OVERLOAD_MIN_DELAY_S:g}-{OVERLOAD_MAX_DELAY_S:g} seconds, "
        "recovering after a random "
        f"{OVERLOAD_DURATION_MIN_S:g}-{OVERLOAD_DURATION_MAX_S:g} seconds"
    )

    status = SealerStatus()
    stop_event = threading.Event()

    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=JsonSerializer(),
    )

    consumer_thread = threading.Thread(
        target=consume_commands,
        args=(stop_event, status),
        name="kafka-consumer",
        daemon=True,
    )
    consumer_thread.start()

    overload_thread = threading.Thread(
        target=simulate_overload,
        args=(producer, status, stop_event),
        name="overload-simulator",
        daemon=True,
    )
    overload_thread.start()

    try:
        send_heartbeats(producer, status, stop_event)
    except KeyboardInterrupt:
        print("Sealer stopping")
    finally:
        stop_event.set()
        consumer_thread.join()
        overload_thread.join()
        producer.close()


if __name__ == '__main__':
    main()
