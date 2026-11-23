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
LOG_FILE = os.getenv("LOG_FILE", "logs/sealer.log")
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

# Experiment mode: OVERLOAD_MESSAGE_COUNT > 0 sends that many OVERLOAD events
# instead of running the single random overload simulation.
OVERLOAD_MESSAGE_COUNT = int(os.getenv("OVERLOAD_MESSAGE_COUNT", "0"))
OVERLOAD_MESSAGE_INTERVAL_MS = int(os.getenv("OVERLOAD_MESSAGE_INTERVAL_MS", "50"))
BENCHMARK_WARMUP_MS = int(os.getenv("BENCHMARK_WARMUP_MS", "5000"))
RUN_ID = os.getenv("RUN_ID", "")


def utc_timestamp() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace(
        "+00:00", "Z"
    )


def make_event(
    machine_id: str | int,
    event_type: str,
    payload: dict[str, Any],
    heartbeat: bool = False,
) -> dict[str, Any]:
    event = {
        "machineId": machine_id,
        "eventType": event_type,
        "timestamp": utc_timestamp(),
        "payload": payload,
    }
    if heartbeat:
        event["heartbeat"] = True
    return event


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


def write_log_entry(entry: Any) -> None:
    """Append a timestamped JSON entry to the local log file."""
    directory = os.path.dirname(LOG_FILE)
    if directory:
        os.makedirs(directory, exist_ok=True)

    with open(LOG_FILE, "a", encoding="utf-8") as file:
        file.write(f"{utc_timestamp()} {json.dumps(entry)}\n")


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
        continue
        #producer.send(
        #    MESSAGES_TOPIC,
        #    make_event(
        #        MACHINE_ID,
        #        "STATUS",
        #        {"status": status.value},
        #        heartbeat=True,
        #    ),
        #)


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
    overload_event = make_event(MACHINE_ID, "STATUS", {"status": OVERLOAD_STATUS})
    producer.send(MESSAGES_TOPIC, overload_event)
    write_log_entry(overload_event)
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


def send_overload_messages(
    producer: KafkaProducer,
    status: SealerStatus,
    stop_event: threading.Event,
) -> None:
    """Publish a fixed number of OVERLOAD events for the latency experiment.

    Each event gets a unique scenarioId and its own overloadTimestamp so the
    benchmark can pair it with the STOP command produced by the PCS.
    """
    count = OVERLOAD_MESSAGE_COUNT
    interval_ms = OVERLOAD_MESSAGE_INTERVAL_MS
    warmup_ms = BENCHMARK_WARMUP_MS
    run_id = RUN_ID or utc_timestamp()

    print(
        f"Sending {count} overload messages every {interval_ms} ms "
        f"after a {warmup_ms} ms warmup"
    )

    if stop_event.wait(warmup_ms / 1000):
        return

    status.update(OVERLOAD_STATUS)

    sent = 0
    for sequence in range(1, count + 1):
        if stop_event.is_set():
            break

        scenario_id = f"{run_id}-{MACHINE_ID}-{sequence:06d}"
        overload_event = make_event(
            MACHINE_ID,
            "STATUS",
            {
                "status": OVERLOAD_STATUS,
                "scenarioId": scenario_id,
                "overloadTimestamp": utc_timestamp(),
            },
        )

        producer.send(MESSAGES_TOPIC, overload_event)
        write_log_entry(overload_event)
        sent += 1

        if stop_event.wait(interval_ms / 1000):
            break

    print(f"Sent {sent} overload messages")

    if not stop_event.is_set():
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
    print(f"Logging overload events to '{LOG_FILE}'")
    if OVERLOAD_MESSAGE_COUNT > 0:
        print(
            f"Experiment mode: sending {OVERLOAD_MESSAGE_COUNT} overload messages "
            f"every {OVERLOAD_MESSAGE_INTERVAL_MS} ms after "
            f"{BENCHMARK_WARMUP_MS} ms warmup"
        )
    else:
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

    if OVERLOAD_MESSAGE_COUNT > 0:
        overload_target = send_overload_messages
        overload_thread_name = "overload-message-sender"
    else:
        overload_target = simulate_overload
        overload_thread_name = "overload-simulator"

    overload_thread = threading.Thread(
        target=overload_target,
        args=(producer, status, stop_event),
        name=overload_thread_name,
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
