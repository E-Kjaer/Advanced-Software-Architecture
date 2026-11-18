from datetime import datetime, timezone
import os
from threading import Event, Lock, Thread
from typing import Any

from kafka import KafkaConsumer, KafkaProducer, JsonSerializer

COMMANDS_TOPIC = "tappers-commands"
MESSAGES_TOPIC = "tappers-messages"
STATUS_INTERVAL_SECONDS = 0.1

def utc_timestamp() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace(
        "+00:00", "Z"
    )


def make_event(machine_id: int, event_type: str, payload: dict[str, Any]) -> dict[str, Any]:
    return {
        "machineId": machine_id,
        "eventType": event_type,
        "timestamp": utc_timestamp(),
        "payload": payload,
    }

def main() :
    print("Tapper started")
    machine_id = int(os.environ.get("MACHINE_ID", 1))

    producer = KafkaProducer(
        bootstrap_servers='kafka:9092', 
        value_serializer=JsonSerializer()
    )

    consumer = KafkaConsumer(
        bootstrap_servers='kafka:9092',
        group_id=f"tappaer_{machine_id}",
        auto_offset_reset="latest",
        enable_auto_commit=False,
        value_deserializer=JsonSerializer()
    )
    consumer.subscribe([COMMANDS_TOPIC])

    status = "IDLE"
    status_lock = Lock()
    stop_status_updates = Event()

    def update_status(producer: KafkaProducer, machine_id: int, new_status: str) -> None:
        nonlocal status

        if new_status not in ["IDLE", "RUNNING"]:
            raise ValueError(f"Invalid status: {new_status}")

        with status_lock:
            if status == new_status:
                return

            status = new_status
            event = make_event(machine_id, "STATUS", {"status": new_status})
            producer.send(MESSAGES_TOPIC, event)
            print(f"Sent status update: {event}")

    def send_status_updates() -> None:
        while not stop_status_updates.is_set():
            with status_lock:
                event = make_event(machine_id, "STATUS", {"status": status})
                producer.send(MESSAGES_TOPIC, event)

            if stop_status_updates.wait(STATUS_INTERVAL_SECONDS):
                break

    status_thread = Thread(target=send_status_updates, name="status-producer")
    status_thread.start()
    try:
        while True:
            records = consumer.poll(timeout_ms=100)
            for partition_records in records.values():
                for record in partition_records:
                    command = record.value
                    
                    if command.get("targetMachine") != machine_id:
                        continue
                    
                    print(f"Received command: {command}")

                    if command.get("command") == "START":
                        update_status(producer, machine_id, "RUNNING")

                    elif command.get("command") == "STOP":
                        update_status(producer, machine_id, "IDLE")
    finally:
        stop_status_updates.set()
        status_thread.join()
        consumer.close()
        producer.flush()
        producer.close()

if __name__ == '__main__':
    main()