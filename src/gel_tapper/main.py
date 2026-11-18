from datetime import datetime, timezone
from typing import Any

from kafka import KafkaConsumer, KafkaProducer, JsonSerializer

COMMANDS_TOPIC = "tapper-commands"
MESSAGES_TOPIC = "tapper-messages"

def utc_timestamp() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace(
        "+00:00", "Z"
    )


def make_event(machine_id: str, event_type: str, payload: dict[str, Any]) -> dict[str, Any]:
    return {
        "machineId": machine_id,
        "eventType": event_type,
        "timestamp": utc_timestamp(),
        "payload": payload,
    }


def main() :
    print("Tapper started")

    producer = KafkaProducer(bootstrap_servers='kafka:9092', value_serializer=JsonSerializer())
    consumer = KafkaConsumer(
        bootstrap_servers='kafka:9092',
        group_id=f"tapper_1",
        auto_offset_reset="latest",
        enable_auto_commit=False,
    )

    status = "IDLE"

    while True:
        producer.send(MESSAGES_TOPIC, make_event("tapper_1", "STATUS", {"status": status}))
        print("Sent STATUS event")
        records = consumer.poll(timeout_ms=100)

        for topic_partition, messages in records.items():
            for message in messages:
                print(f"Received message on {topic_partition}: {message.value}")

if __name__ == '__main__':
    main()