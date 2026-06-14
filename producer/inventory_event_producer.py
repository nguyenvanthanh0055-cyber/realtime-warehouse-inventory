import argparse
import csv
import io
import json
import logging
import os
import random
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from urllib.parse import urlparse

import boto3
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
PROMOTION_CONFIG_URI = os.getenv("PROMOTION_CONFIG_URI")

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
    from aws_msk_iam_sasl_signer import MSKAuthTokenProvider

    if not AWS_REGION:
        raise ValueError("AWS_REGION or AWS_DEFAULT_REGION is required for IAM auth")

    token, expiry_ms = MSKAuthTokenProvider.generate_auth_token(AWS_REGION)
    return token, expiry_ms / 1000


def create_kafka_producer(bootstrap_servers: str, auth_mode: str):
    from confluent_kafka import Producer

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


def parse_timestamp(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)


def parse_s3_uri(s3_uri: str) -> Tuple[str, str]:
    parsed_uri = urlparse(s3_uri)
    if parsed_uri.scheme != "s3" or not parsed_uri.netloc or not parsed_uri.path:
        raise ValueError(f"Invalid S3 URI: {s3_uri}")

    return parsed_uri.netloc, parsed_uri.path.lstrip("/")


def read_text_from_uri(uri: str) -> str:
    parsed_uri = urlparse(uri)
    if parsed_uri.scheme == "s3":
        bucket, key = parse_s3_uri(uri)
        s3_client = boto3.client("s3", region_name=AWS_REGION)
        response = s3_client.get_object(Bucket=bucket, Key=key)
        return response["Body"].read().decode("utf-8")

    return Path(uri).read_text(encoding="utf-8")


def load_promotion_config(
    promotion_config_uri: Optional[str],
) -> Dict[Tuple[str, str, str, str], dict]:
    if not promotion_config_uri:
        raise ValueError(
            "PROMOTION_CONFIG_URI or --promotion-config-uri is required. "
            "The producer needs promotion_config.csv to enforce flash-sale windows."
        )

    content = read_text_from_uri(promotion_config_uri)
    promotions = {}

    for row in csv.DictReader(io.StringIO(content)):
        key = (
            row["campaign_id"],
            row["promotion_id"],
            row["sku_id"],
            row["warehouse_id"],
        )
        promotions[key] = {
            "start_time": parse_timestamp(row["start_time"]),
            "end_time": parse_timestamp(row["end_time"]),
            "normal_price": int(row["normal_price"]),
            "sale_price": int(row["sale_price"]),
        }

    logger.info(
        "Loaded promotion config rows=%s uri=%s",
        len(promotions),
        promotion_config_uri,
    )
    if not promotions:
        raise ValueError(f"No promotion rows found in {promotion_config_uri}")

    return promotions


def get_active_promotion(
    event: dict,
    promotions: Dict[Tuple[str, str, str, str], dict],
) -> Optional[dict]:
    promotion_id = event.get("promotion_id")
    if not promotion_id:
        return None

    key = (
        event["campaign_id"],
        promotion_id,
        event["sku_id"],
        event["warehouse_id"],
    )
    promotion = promotions.get(key)
    if promotion is None:
        return None

    event_time = parse_timestamp(event["event_time"])
    if promotion["start_time"] <= event_time <= promotion["end_time"]:
        return promotion

    return None


def enforce_promotion_window(
    event: dict,
    promotions: Dict[Tuple[str, str, str, str], dict],
) -> dict:
    if not event.get("promotion_applied") or not event.get("promotion_id"):
        return event

    active_promotion = get_active_promotion(event, promotions)
    if active_promotion:
        event["unit_price"] = active_promotion["sale_price"]
        return event

    original_promotion_id = event.get("promotion_id")
    key = (
        event["campaign_id"],
        original_promotion_id,
        event["sku_id"],
        event["warehouse_id"],
    )
    promotion = promotions.get(key)

    event["promotion_id"] = None
    event["promotion_applied"] = False
    if promotion:
        event["unit_price"] = promotion["normal_price"]

    logger.info(
        "Removed inactive promotion from event_id=%s promotion_id=%s sku_id=%s "
        "warehouse_id=%s event_time=%s",
        event.get("event_id"),
        original_promotion_id,
        event.get("sku_id"),
        event.get("warehouse_id"),
        event.get("event_time"),
    )
    return event


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

    parser.add_argument(
        "--promotion-config-uri",
        default=PROMOTION_CONFIG_URI,
        help="S3 URI or local path for promotion_config.csv.",
    )

    args = parser.parse_args()

    if not args.bootstrap_servers:
        raise ValueError("KAFKA_BOOTSTRAP_SERVERS or --bootstrap-servers is required")

    promotion_config = load_promotion_config(args.promotion_config_uri)
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
                    event = enforce_promotion_window(event, promotion_config)

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
