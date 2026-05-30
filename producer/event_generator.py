import argparse
import json
import random
import uuid
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, Any, List


EVENT_TYPES = [
    "STOCK_RESERVED",
    "PAYMENT_CONFIRMED",
    "COD_CONFIRMED",
    "RESERVATION_EXPIRED",
    "ORDER_CANCELLED",
    "RETURN_RECEIVED",
    "STOCK_REPLENISHED",
]


PAYMENT_METHODS = [
    "COD",
    "E_WALLET",
]


FLOW_TYPES = [
    "E_WALLET_PAID",
#   "E_WALLET_EXPIRED",
    "COD_CONFIRMED",
    "ORDER_CANCELLED",
    "RETURN_RECEIVED",
    "STOCK_REPLENISHED",
]


EVENTS_CREATE_NEW_ORDER_ID = [
    "STOCK_RESERVED",
    "COD_CONFIRMED",
]


EVENTS_REQUIRE_EXISTING_ORDER_ID = [
    "PAYMENT_CONFIRMED",
    "RESERVATION_EXPIRED",
    "ORDER_CANCELLED",
]


EVENTS_WITHOUT_ORDER_ID = [
    "STOCK_REPLENISHED",
]


SKU_CONFIG = {
    "SKU_IPHONE_15": {
        "product_name": "iPhone 15",
        "normal_price": 22990000,
        "sale_price": 18990000,
        "promotion_id": "FLASH_SALE_001",
    },
    "SKU_AIRPODS_PRO": {
        "product_name": "AirPods Pro",
        "normal_price": 5490000,
        "sale_price": 3990000,
        "promotion_id": "FLASH_SALE_002",
    },
    "SKU_IPAD_AIR": {
        "product_name": "iPad Air",
        "normal_price": 16990000,
        "sale_price": 13990000,
        "promotion_id": None,
    },
}


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def to_iso_z(dt: datetime) -> str:
    return dt.isoformat().replace("+00:00", "Z")


def event_time_from_iso_z(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def generate_event_id() -> str:
    return f"evt_{uuid.uuid4().hex[:12]}"


def generate_order_id() -> str:
    return f"ORD_{uuid.uuid4().hex[:10]}"


def validate_sku_id(sku_id: str) -> None:
    if sku_id not in SKU_CONFIG:
        raise ValueError(f"Unknown sku_id: {sku_id}")


def get_payment_method(
    event_type: str,
    payment_method: Optional[str],
) -> str:
    if event_type in [
        "STOCK_RESERVED",
        "PAYMENT_CONFIRMED",
        "RESERVATION_EXPIRED",
    ]:
        if payment_method is not None and payment_method != "E_WALLET":
            raise ValueError(
                f"Invalid payment_method for {event_type}. "
                f"Expected E_WALLET, got {payment_method}"
            )
        return "E_WALLET"

    if event_type == "COD_CONFIRMED":
        if payment_method is not None and payment_method != "COD":
            raise ValueError(
                f"Invalid payment_method for {event_type}. "
                f"Expected COD, got {payment_method}"
            )
        return "COD"

    if event_type == "ORDER_CANCELLED":
        if payment_method is None:
            return random.choice(PAYMENT_METHODS)

        if payment_method not in PAYMENT_METHODS:
            raise ValueError(
                f"Invalid payment_method for ORDER_CANCELLED. "
                f"Expected COD or E_WALLET, got {payment_method}"
            )

        return payment_method

    return "NOT_APPLICABLE"


def get_payment_status(event_type: str) -> str:
    if event_type == "STOCK_RESERVED":
        return "PENDING"

    if event_type == "PAYMENT_CONFIRMED":
        return "PAID"

    if event_type == "RESERVATION_EXPIRED":
        return "EXPIRED"

    if event_type == "COD_CONFIRMED":
        return "COD_CONFIRMED"

    if event_type == "ORDER_CANCELLED":
        return "CANCELLED"

    return "NOT_APPLICABLE"


def resolve_order_id(
    event_type: str,
    order_id: Optional[str],
) -> Optional[str]:
    if event_type in EVENTS_CREATE_NEW_ORDER_ID:
        return order_id or generate_order_id()

    if event_type in EVENTS_REQUIRE_EXISTING_ORDER_ID:
        if order_id is None:
            raise ValueError(
                f"{event_type} requires an existing order_id. "
                f"Generate the previous order event first and reuse its order_id."
            )
        return order_id

    if event_type == "RETURN_RECEIVED":
        return order_id

    if event_type in EVENTS_WITHOUT_ORDER_ID:
        return None

    return order_id


def should_apply_promotion(
    event_type: str,
    promotion_id: Optional[str],
) -> bool:
    """
    Promotion quota chỉ consume tại:
    - STOCK_RESERVED
    - COD_CONFIRMED

    PAYMENT_CONFIRMED / RESERVATION_EXPIRED chỉ giữ context promotion để trace,
    không consume thêm và không trả lại quota.
    """
    if event_type not in [
        "STOCK_RESERVED",
        "COD_CONFIRMED",
    ]:
        return False

    if promotion_id is None:
        return False

    return random.random() < 0.7


def get_unit_price(
    promotion_applied: bool,
    sale_price: int,
    normal_price: int,
) -> int:
    if promotion_applied:
        return sale_price

    return normal_price


def get_reservation_expires_at(
    event_type: str,
    event_time: datetime,
) -> Optional[str]:
    if event_type != "STOCK_RESERVED":
        return None

    return to_iso_z(event_time + timedelta(hours=24))


def validate_event_payment_logic(
    event_type: str,
    payment_method: str,
    payment_status: str,
) -> None:
    valid_rules = {
        "STOCK_RESERVED": ("E_WALLET", "PENDING"),
        "PAYMENT_CONFIRMED": ("E_WALLET", "PAID"),
        "RESERVATION_EXPIRED": ("E_WALLET", "EXPIRED"),
        "COD_CONFIRMED": ("COD", "COD_CONFIRMED"),
        "RETURN_RECEIVED": ("NOT_APPLICABLE", "NOT_APPLICABLE"),
        "STOCK_REPLENISHED": ("NOT_APPLICABLE", "NOT_APPLICABLE"),
    }

    if event_type in valid_rules:
        expected_method, expected_status = valid_rules[event_type]

        if payment_method != expected_method or payment_status != expected_status:
            raise ValueError(
                f"Invalid payment logic for {event_type}. "
                f"Expected ({expected_method}, {expected_status}), "
                f"got ({payment_method}, {payment_status})"
            )

    if event_type == "ORDER_CANCELLED":
        if payment_method not in PAYMENT_METHODS:
            raise ValueError(
                "ORDER_CANCELLED must have payment_method COD or E_WALLET"
            )

        if payment_status != "CANCELLED":
            raise ValueError(
                "ORDER_CANCELLED must have payment_status CANCELLED"
            )


def generate_inventory_event(
    campaign_id: str,
    sku_id: str,
    warehouse_id: str,
    event_type: str,
    quantity: Optional[int] = None,
    payment_method: Optional[str] = None,
    order_id: Optional[str] = None,
    event_time: Optional[datetime] = None,
    promotion_id_override: Optional[str] = None,
    promotion_applied_override: Optional[bool] = None,
    unit_price_override: Optional[int] = None,
) -> Dict[str, Any]:
    validate_sku_id(sku_id)

    if event_type not in EVENT_TYPES:
        raise ValueError(f"Invalid event_type: {event_type}")

    if quantity is None:
        quantity = random.randint(1, 5)

    if quantity <= 0:
        raise ValueError(f"quantity must be positive, got {quantity}")

    sku = SKU_CONFIG[sku_id]

    resolved_order_id = resolve_order_id(
        event_type=event_type,
        order_id=order_id,
    )

    resolved_payment_method = get_payment_method(
        event_type=event_type,
        payment_method=payment_method,
    )

    payment_status = get_payment_status(event_type)

    validate_event_payment_logic(
        event_type=event_type,
        payment_method=resolved_payment_method,
        payment_status=payment_status,
    )

    event_time = event_time or utc_now()

    if promotion_applied_override is not None:
        promotion_applied = promotion_applied_override
    else:
        promotion_applied = should_apply_promotion(
            event_type=event_type,
            promotion_id=sku["promotion_id"],
        )

    if promotion_id_override is not None:
        promotion_id = promotion_id_override
    else:
        promotion_id = sku["promotion_id"] if promotion_applied else None

    if unit_price_override is not None:
        unit_price = unit_price_override
    else:
        unit_price = get_unit_price(
            promotion_applied=promotion_applied,
            sale_price=sku["sale_price"],
            normal_price=sku["normal_price"],
        )

    reservation_expires_at = get_reservation_expires_at(
        event_type=event_type,
        event_time=event_time,
    )

    return {
        "event_id": generate_event_id(),
        "campaign_id": campaign_id,
        "event_time": to_iso_z(event_time),
        "event_type": event_type,
        "order_id": resolved_order_id,
        "sku_id": sku_id,
        "warehouse_id": warehouse_id,
        "quantity": quantity,
        "unit_price": unit_price,
        "promotion_id": promotion_id,
        "promotion_applied": promotion_applied,
        "payment_method": resolved_payment_method,
        "payment_status": payment_status,
        "reservation_expires_at": reservation_expires_at,
        "source": "mock_order_service",
    }


def generate_ewallet_paid_flow(
    campaign_id: str,
    sku_id: str,
    warehouse_id: str,
    quantity: Optional[int] = None,
) -> List[Dict[str, Any]]:
    """
    STOCK_RESERVED -> PAYMENT_CONFIRMED

    payment event_time sẽ cách reserved event_time 5-180 giây
    để mock thực tế hơn.
    """
    if quantity is None:
        quantity = random.randint(1, 5)

    order_id = generate_order_id()

    reserved_event = generate_inventory_event(
        campaign_id=campaign_id,
        sku_id=sku_id,
        warehouse_id=warehouse_id,
        event_type="STOCK_RESERVED",
        quantity=quantity,
        payment_method="E_WALLET",
        order_id=order_id,
    )

    reserved_event_time = event_time_from_iso_z(reserved_event["event_time"])

    payment_event_time = reserved_event_time + timedelta(
        seconds=random.randint(5, 180)
    )

    payment_event = generate_inventory_event(
        campaign_id=campaign_id,
        sku_id=sku_id,
        warehouse_id=warehouse_id,
        event_type="PAYMENT_CONFIRMED",
        quantity=quantity,
        payment_method="E_WALLET",
        order_id=order_id,
        event_time=payment_event_time,
        promotion_id_override=reserved_event["promotion_id"],
        promotion_applied_override=reserved_event["promotion_applied"],
        unit_price_override=reserved_event["unit_price"],
    )

    return [reserved_event, payment_event]


def generate_ewallet_expired_flow(
    campaign_id: str,
    sku_id: str,
    warehouse_id: str,
    quantity: Optional[int] = None,
) -> List[Dict[str, Any]]:
    """
    STOCK_RESERVED -> RESERVATION_EXPIRED

    expired event_time = reservation_expires_at của STOCK_RESERVED.
    """
    if quantity is None:
        quantity = random.randint(1, 5)

    order_id = generate_order_id()

    reserved_event = generate_inventory_event(
        campaign_id=campaign_id,
        sku_id=sku_id,
        warehouse_id=warehouse_id,
        event_type="STOCK_RESERVED",
        quantity=quantity,
        payment_method="E_WALLET",
        order_id=order_id,
    )

    expired_event_time = event_time_from_iso_z(
        reserved_event["reservation_expires_at"]
    )

    expired_event = generate_inventory_event(
        campaign_id=campaign_id,
        sku_id=sku_id,
        warehouse_id=warehouse_id,
        event_type="RESERVATION_EXPIRED",
        quantity=quantity,
        payment_method="E_WALLET",
        order_id=order_id,
        event_time=expired_event_time,
        promotion_id_override=reserved_event["promotion_id"],
        promotion_applied_override=reserved_event["promotion_applied"],
        unit_price_override=reserved_event["unit_price"],
    )

    return [reserved_event, expired_event]


def generate_cod_confirmed_event(
    campaign_id: str,
    sku_id: str,
    warehouse_id: str,
    quantity: Optional[int] = None,
) -> Dict[str, Any]:
    if quantity is None:
        quantity = random.randint(1, 5)

    return generate_inventory_event(
        campaign_id=campaign_id,
        sku_id=sku_id,
        warehouse_id=warehouse_id,
        event_type="COD_CONFIRMED",
        quantity=quantity,
        payment_method="COD",
    )


def generate_stock_replenished_event(
    campaign_id: str,
    sku_id: str,
    warehouse_id: str,
    quantity: Optional[int] = None,
) -> Dict[str, Any]:
    if quantity is None:
        quantity = random.randint(20, 100)

    return generate_inventory_event(
        campaign_id=campaign_id,
        sku_id=sku_id,
        warehouse_id=warehouse_id,
        event_type="STOCK_REPLENISHED",
        quantity=quantity,
        payment_method="NOT_APPLICABLE",
    )


def generate_return_received_event(
    campaign_id: str,
    sku_id: str,
    warehouse_id: str,
    quantity: Optional[int] = None,
    order_id: Optional[str] = None,
) -> Dict[str, Any]:
    if quantity is None:
        quantity = random.randint(1, 3)

    return generate_inventory_event(
        campaign_id=campaign_id,
        sku_id=sku_id,
        warehouse_id=warehouse_id,
        event_type="RETURN_RECEIVED",
        quantity=quantity,
        payment_method="NOT_APPLICABLE",
        order_id=order_id,
    )


def generate_order_cancelled_event(
    campaign_id: str,
    sku_id: str,
    warehouse_id: str,
    order_id: str,
    payment_method: str,
    quantity: Optional[int] = None,
    promotion_id: Optional[str] = None,
    promotion_applied: Optional[bool] = None,
    unit_price: Optional[int] = None,
) -> Dict[str, Any]:
    if quantity is None:
        quantity = random.randint(1, 5)

    return generate_inventory_event(
        campaign_id=campaign_id,
        sku_id=sku_id,
        warehouse_id=warehouse_id,
        event_type="ORDER_CANCELLED",
        quantity=quantity,
        payment_method=payment_method,
        order_id=order_id,
        promotion_id_override=promotion_id,
        promotion_applied_override=promotion_applied,
        unit_price_override=unit_price,
    )


def generate_random_event_or_flow(
    campaign_id: str,
    sku_id: str,
    warehouse_id: str,
) -> List[Dict[str, Any]]:
    """
    Normal mode:
    - Sinh flow hợp lệ.
    - Không sinh PAYMENT_CONFIRMED lẻ.
    - Không sinh RESERVATION_EXPIRED lẻ.
    """
    flow_type = random.choices(
        FLOW_TYPES,
        weights=[
            45,  # E_WALLET_PAID
            # 5,   # E_WALLET_EXPIRED
            35,  # COD_CONFIRMED
            10,   # ORDER_CANCELLED
            3,   # RETURN_RECEIVED
            7,   # STOCK_REPLENISHED
        ],
        k=1,
    )[0]

    quantity = random.randint(1, 5)

    if flow_type == "E_WALLET_PAID":
        return generate_ewallet_paid_flow(
            campaign_id=campaign_id,
            sku_id=sku_id,
            warehouse_id=warehouse_id,
            quantity=quantity,
        )

    if flow_type == "E_WALLET_EXPIRED":
        return generate_ewallet_expired_flow(
            campaign_id=campaign_id,
            sku_id=sku_id,
            warehouse_id=warehouse_id,
            quantity=quantity,
        )

    if flow_type == "COD_CONFIRMED":
        return [
            generate_cod_confirmed_event(
                campaign_id=campaign_id,
                sku_id=sku_id,
                warehouse_id=warehouse_id,
                quantity=quantity,
            )
        ]

    if flow_type == "STOCK_REPLENISHED":
        return [
            generate_stock_replenished_event(
                campaign_id=campaign_id,
                sku_id=sku_id,
                warehouse_id=warehouse_id,
            )
        ]

    if flow_type == "RETURN_RECEIVED":
        return [
            generate_return_received_event(
                campaign_id=campaign_id,
                sku_id=sku_id,
                warehouse_id=warehouse_id,
                quantity=quantity,
            )
        ]

    if flow_type == "ORDER_CANCELLED":

        return [
            generate_order_cancelled_event(
                campaign_id=campaign_id,
                sku_id=sku_id,
                warehouse_id=warehouse_id,
                order_id=generate_order_id(),
                payment_method=random.choice(PAYMENT_METHODS),
                quantity=quantity,
            )
        ]

    raise ValueError(f"Unsupported flow_type: {flow_type}")


def generate_chaos_event_or_flow(
    campaign_id: str,
    sku_id: str,
    warehouse_id: str,
) -> List[Dict[str, Any]]:
    """
    Chaos mode skeleton.

    Hiện tại vẫn sinh normal flow để không làm rối Phase 5.
    Sau Phase 6/7 có thể nâng cấp:
    - duplicate event
    - out-of-order event
    - late event
    - invalid event
    - conflict event
    """
    return generate_random_event_or_flow(
        campaign_id=campaign_id,
        sku_id=sku_id,
        warehouse_id=warehouse_id,
    )


def print_events(events: List[Dict[str, Any]]) -> None:
    for event in events:
        print(json.dumps(event, ensure_ascii=False))


def main():
    parser = argparse.ArgumentParser(
        description="Generate mock inventory events"
    )

    parser.add_argument("--campaign-id", required=True)
    parser.add_argument("--sku-id", required=True)
    parser.add_argument("--warehouse-id", required=True)

    parser.add_argument(
        "--event-type",
        choices=EVENT_TYPES,
        required=False,
        help="Generate a single event. Some event types require --order-id.",
    )

    parser.add_argument(
        "--flow-type",
        choices=FLOW_TYPES,
        required=False,
        help="Generate a valid event flow.",
    )

    parser.add_argument(
        "--payment-method",
        choices=PAYMENT_METHODS,
        required=False,
    )

    parser.add_argument(
        "--mode",
        choices=["normal", "chaos"],
        default="normal",
    )

    parser.add_argument("--quantity", type=int, required=False)
    parser.add_argument("--order-id", required=False)
    parser.add_argument("--count", type=int, default=1)

    args = parser.parse_args()

    if args.event_type and args.flow_type:
        raise ValueError("Use either --event-type or --flow-type, not both.")

    for _ in range(args.count):
        if args.flow_type == "E_WALLET_PAID":
            events = generate_ewallet_paid_flow(
                campaign_id=args.campaign_id,
                sku_id=args.sku_id,
                warehouse_id=args.warehouse_id,
                quantity=args.quantity,
            )

        elif args.flow_type == "E_WALLET_EXPIRED":
            events = generate_ewallet_expired_flow(
                campaign_id=args.campaign_id,
                sku_id=args.sku_id,
                warehouse_id=args.warehouse_id,
                quantity=args.quantity,
            )

        elif args.flow_type == "COD_CONFIRMED":
            events = [
                generate_cod_confirmed_event(
                    campaign_id=args.campaign_id,
                    sku_id=args.sku_id,
                    warehouse_id=args.warehouse_id,
                    quantity=args.quantity,
                )
            ]

        elif args.flow_type == "STOCK_REPLENISHED":
            events = [
                generate_stock_replenished_event(
                    campaign_id=args.campaign_id,
                    sku_id=args.sku_id,
                    warehouse_id=args.warehouse_id,
                    quantity=args.quantity,
                )
            ]

        elif args.flow_type == "RETURN_RECEIVED":
            events = [
                generate_return_received_event(
                    campaign_id=args.campaign_id,
                    sku_id=args.sku_id,
                    warehouse_id=args.warehouse_id,
                    quantity=args.quantity,
                    order_id=args.order_id,
                )
            ]

        elif args.flow_type == "ORDER_CANCELLED":
            if args.order_id is None:
                raise ValueError(
                    "ORDER_CANCELLED flow requires --order-id "
                    "because cancellation should reference an existing order."
                )

            events = [
                generate_order_cancelled_event(
                    campaign_id=args.campaign_id,
                    sku_id=args.sku_id,
                    warehouse_id=args.warehouse_id,
                    order_id=args.order_id,
                    payment_method=args.payment_method or random.choice(PAYMENT_METHODS),
                    quantity=args.quantity,
                )
            ]

        elif args.event_type:
            events = [
                generate_inventory_event(
                    campaign_id=args.campaign_id,
                    sku_id=args.sku_id,
                    warehouse_id=args.warehouse_id,
                    event_type=args.event_type,
                    quantity=args.quantity,
                    payment_method=args.payment_method,
                    order_id=args.order_id,
                )
            ]

        else:
            if args.mode == "chaos":
                events = generate_chaos_event_or_flow(
                    campaign_id=args.campaign_id,
                    sku_id=args.sku_id,
                    warehouse_id=args.warehouse_id,
                )
            else:
                events = generate_random_event_or_flow(
                    campaign_id=args.campaign_id,
                    sku_id=args.sku_id,
                    warehouse_id=args.warehouse_id,
                )

        print_events(events)


if __name__ == "__main__":
    main()