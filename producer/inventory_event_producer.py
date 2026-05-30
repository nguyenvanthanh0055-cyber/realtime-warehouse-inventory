import argparse
import json
import os
import random
import time
from datetime import datetime, timezone
from typing import List

from confluent_kafka import Producer

from event_generator import (
    generate_random_event_or_flow,
    generate_chaos_event_or_flow,
)


DEFAULT_BOOTSTRAP_SERVERS = "localhost:9092"
DEFAULT_TOPIC = "inventory-events"

DEFAULT_CAMPAIGN_ID = "CAMPAIGN_FLASH_0527"
DEFAULT_WAREHOUSE_ID = "WH_HCM_01"

FAILED_EVENTS_PATH = "data/lake/raw/failed_producer_events.jsonl"

SKU_IDS = [
    "SKU_IPHONE_15",
    "SKU_AIRPODS_PRO",
    "SKU_IPAD_AIR",
]


def create_kafka_producer(bootstrap_servers: str) -> Producer:
    config = {
        "bootstrap.servers": bootstrap_servers,
        "client.id": "inventory-event-producer",
        "acks": "all",
        "retries": 3,
    }

    return Producer(config)


def delivery_report(err, msg) -> None:
    if err is not None:
        print(f"[ERROR] Failed to deliver message: {err}")
        return

    print(
        "[OK] Delivered message "
        f"topic={msg.topic()} "
        f"partition={msg.partition()} "
        f"offset={msg.offset()}"
    )


def build_message_key(event: dict) -> str:
    return (
        f"{event['campaign_id']}|"
        f"{event['sku_id']}|"
        f"{event['warehouse_id']}"
    )


def write_failed_event(event: dict, error_message: str) -> None:
    os.makedirs(os.path.dirname(FAILED_EVENTS_PATH), exist_ok=True)

    failed_record = {
        "failed_at": datetime.now(timezone.utc).isoformat(),
        "error_message": error_message,
        "event": event,
    }

    with open(FAILED_EVENTS_PATH, "a", encoding="utf-8") as f:
        f.write(json.dumps(failed_record, ensure_ascii=False) + "\n")


def send_event(
    producer: Producer,
    topic: str,
    event: dict,
) -> None:
    try:
        key = build_message_key(event)
        value = json.dumps(event, ensure_ascii=False).encode("utf-8")

        producer.produce(
            topic=topic,
            key=key.encode("utf-8"),
            value=value,
            callback=delivery_report,
        )

        producer.poll(0)

    except BufferError as e:
        error_message = f"Producer local queue is full: {str(e)}"
        print(f"[ERROR] {error_message}")
        write_failed_event(event, error_message)

    except Exception as e:
        error_message = f"Failed to produce event: {str(e)}"
        print(f"[ERROR] {error_message}")
        write_failed_event(event, error_message)


def choose_random_sku() -> str:
    return random.choice(SKU_IDS)


def generate_events_for_producer(
    campaign_id: str,
    warehouse_id: str,
    mode: str,
) -> List[dict]:
    sku_id = choose_random_sku()

    if mode == "chaos":
        return generate_chaos_event_or_flow(
            campaign_id=campaign_id,
            sku_id=sku_id,
            warehouse_id=warehouse_id,
        )

    return generate_random_event_or_flow(
        campaign_id=campaign_id,
        sku_id=sku_id,
        warehouse_id=warehouse_id,
    )


def main():
    parser = argparse.ArgumentParser(
        description="Produce inventory event flows to Kafka"
    )

    parser.add_argument(
        "--bootstrap-servers",
        default=DEFAULT_BOOTSTRAP_SERVERS,
        help="Kafka bootstrap servers",
    )

    parser.add_argument(
        "--topic",
        default=DEFAULT_TOPIC,
        help="Kafka topic name",
    )

    parser.add_argument(
        "--campaign-id",
        default=DEFAULT_CAMPAIGN_ID,
        help="Campaign ID",
    )

    parser.add_argument(
        "--warehouse-id",
        default=DEFAULT_WAREHOUSE_ID,
        help="Warehouse ID",
    )

    parser.add_argument(
        "--count",
        type=int,
        default=10,
        help="Number of random event flows to generate",
    )

    parser.add_argument(
        "--interval-seconds",
        type=float,
        default=1.0,
        help="Delay between events",
    )

    parser.add_argument(
        "--mode",
        choices=["normal", "chaos"],
        default="normal",
        help="Producer mode. Use normal for MVP, chaos for future robustness tests.",
    )

    args = parser.parse_args()

    producer = create_kafka_producer(args.bootstrap_servers)

    print(
        f"Starting inventory event producer "
        f"bootstrap_servers={args.bootstrap_servers}, "
        f"topic={args.topic}, "
        f"campaign_id={args.campaign_id}, "
        f"warehouse_id={args.warehouse_id}, "
        f"count={args.count}, "
        f"interval_seconds={args.interval_seconds}, "
        f"mode={args.mode}"
    )

    try:
        for i in range(args.count):
            try:
                events = generate_events_for_producer(
                    campaign_id=args.campaign_id,
                    warehouse_id=args.warehouse_id,
                    mode=args.mode,
                )

                print(
                    f"[FLOW {i + 1}/{args.count}] "
                    f"Generated {len(events)} event(s)"
                )

                for event_index, event in enumerate(events, start=1):
                    send_event(
                        producer=producer,
                        topic=args.topic,
                        event=event,
                    )

                    print(
                        f"[FLOW {i + 1}/{args.count} | "
                        f"EVENT {event_index}/{len(events)}] Sent event: "
                        f"{json.dumps(event, ensure_ascii=False)}"
                    )

                    time.sleep(args.interval_seconds)

            except Exception as e:
                print(f"[ERROR] Failed at flow index {i + 1}: {str(e)}")
                continue

    except KeyboardInterrupt:
        print("Producer stopped by user.")

    finally:
        print("Flushing producer...")
        producer.flush()
        print("Done.")


if __name__ == "__main__":
    main()