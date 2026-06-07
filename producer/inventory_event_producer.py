import argparse
import json
import logging
import os
import random
import time
import uuid
from datetime import datetime, timezone
from typing import List
from urllib.parse import urlparse

import boto3
from aws_msk_iam_sasl_signer import MSKAuthTokenProvider
from confluent_kafka import Producer
from event_generator import generate_random_event_or_flow


BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
TOPIC = os.getenv("KAFKA_TOPIC", "inventory-events")
AUTH_MODE = os.getenv("MSK_AUTH_MODE", "iam")
AWS_REGION = os.getenv("AWS_REGION") or os.getenv("AWS_DEFAULT_REGION")
CAMPAIGN_ID = os.getenv("CAMPAIGN_ID", "CAMPAIGN_FLASH_0527")
WAREHOUSE_ID = os.getenv("WAREHOUSE_ID", "WH_HCM_01")

FAILED_EVENTS_S3_PATH = os.getenv(
    "FAILED_EVENTS_S3_PATH",
    "s3://inventory-lake-fox/bronze/failed_producer_events/",
)

SKU_IDS = [
    "SKU_IPHONE_15",
    "SKU_AIRPODS_PRO",
    "SKU_IPAD_AIR",
]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)


def oauth_cb(_oauth_config):
    

    if not AWS_REGION:
        raise ValueError("AWS_REGION or AWS_DEFAULT_REGION is required for IAM auth")

    token, expiry_ms = MSKAuthTokenProvider.generate_auth_token(AWS_REGION)
    return token, expiry_ms / 1000


def create_kafka_producer(bootstrap_servers: str, auth_mode: str):
    

    config = {
        "bootstrap.servers": bootstrap_servers,
        "client.id": "inventory-event-producer",
        "acks": "all",
        "retries": 3,
    }

    if auth_mode == "iam":
        config.update(
            {
                "security.protocol": "SASL_SSL",
                "sasl.mechanism": "OAUTHBEARER",
                "oauth_cb": oauth_cb,
            }
        )

    return Producer(config)


def delivery_report(err, msg) -> None:
    if err is not None:
        logger.error("Failed to deliver message: %s", err)
        return

    logger.info(
        "Delivered message topic=%s partition=%s offset=%s",
        msg.topic(),
        msg.partition(),
        msg.offset(),
    )


def build_message_key(event: dict) -> str:
    return (
        f"{event['campaign_id']}|"
        f"{event['sku_id']}|"
        f"{event['warehouse_id']}"
    )


def write_failed_event(event: dict, error_message: str) -> None:
    parsed_s3_path = urlparse(FAILED_EVENTS_S3_PATH)
    if parsed_s3_path.scheme != "s3" or not parsed_s3_path.netloc:
        raise ValueError(
            "FAILED_EVENTS_S3_PATH must be an S3 URI, "
            "for example s3://inventory-lake-fox/bronze/failed_producer_events/"
        )

    failed_at = datetime.now(timezone.utc)
    failed_record = {
        "failed_at": failed_at.isoformat(),
        "error_message": error_message,
        "event": event,
    }

    key_prefix = parsed_s3_path.path.lstrip("/").rstrip("/")
    object_key = (
        f"{key_prefix}/date={failed_at.date().isoformat()}/"
        f"failed_event_{uuid.uuid4().hex}.json"
    )

    s3_client = boto3.client("s3", region_name=AWS_REGION)
    s3_client.put_object(
        Bucket=parsed_s3_path.netloc,
        Key=object_key,
        Body=json.dumps(failed_record, ensure_ascii=False).encode("utf-8"),
        ContentType="application/json",
    )

    logger.info(
        "Wrote failed producer event to s3://%s/%s",
        parsed_s3_path.netloc,
        object_key,
    )


def send_event(
    producer,
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
        logger.error(error_message)
        write_failed_event(event, error_message)

    except Exception as e:
        error_message = f"Failed to produce event: {str(e)}"
        logger.exception(error_message)
        write_failed_event(event, error_message)


def choose_random_sku() -> str:
    return random.choice(SKU_IDS)


def generate_events_for_producer(
    campaign_id: str,
    warehouse_id: str,
) -> List[dict]:
    sku_id = choose_random_sku()

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
        default=BOOTSTRAP_SERVERS,
        help="Kafka/MSK bootstrap servers",
    )

    parser.add_argument(
        "--topic",
        default=TOPIC,
        help="Kafka topic name",
    )

    parser.add_argument(
        "--auth-mode",
        choices=["iam", "plaintext"],
        default=AUTH_MODE
    )

    parser.add_argument(
        "--campaign-id",
        default=CAMPAIGN_ID
    )

    parser.add_argument(
        "--warehouse-id",
        default=WAREHOUSE_ID
    )

    parser.add_argument(
        "--count",
        type=int,
        default=10
    )

    parser.add_argument(
        "--interval-seconds",
        type=float,
        default=1.0
    )

    args = parser.parse_args()

    if not args.bootstrap_servers:
        raise ValueError("KAFKA_BOOTSTRAP_SERVERS or --bootstrap-servers is required")

    producer = create_kafka_producer(args.bootstrap_servers, args.auth_mode)

    logger.info(
        "Starting inventory event producer bootstrap_servers=%s topic=%s "
        "auth_mode=%s campaign_id=%s warehouse_id=%s count=%s "
        "interval_seconds=%s",
        args.bootstrap_servers,
        args.topic,
        args.auth_mode,
        args.campaign_id,
        args.warehouse_id,
        args.count,
        args.interval_seconds,
    )

    try:
        for i in range(args.count):
            try:
                events = generate_events_for_producer(
                    campaign_id=args.campaign_id,
                    warehouse_id=args.warehouse_id,
                )

                logger.info(
                    "Generated flow %s/%s with %s event(s)",
                    i + 1,
                    args.count,
                    len(events),
                )

                for event_index, event in enumerate(events, start=1):
                    send_event(
                        producer=producer,
                        topic=args.topic,
                        event=event,
                    )

                    logger.info(
                        "Queued event flow=%s/%s event=%s/%s payload=%s",
                        i + 1,
                        args.count,
                        event_index,
                        len(events),
                        json.dumps(event, ensure_ascii=False),
                    )

                    time.sleep(args.interval_seconds)

            except Exception as e:
                logger.exception("Failed at flow index %s: %s", i + 1, str(e))
                continue

    except KeyboardInterrupt:
        logger.info("Producer stopped by user.")

    finally:
        logger.info("Flushing producer...")
        producer.flush()
        logger.info("Done.")


if __name__ == "__main__":
    main()
