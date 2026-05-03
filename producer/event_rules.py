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

PAYMENT_LOGIC_RULES = {
    "STOCK_RESERVED": {
        "payment_methods": ["E_WALLET"],
        "payment_statuses": ["PENDING"],
    },
    "PAYMENT_CONFIRMED": {
        "payment_methods": ["E_WALLET"],
        "payment_statuses": ["PAID"],
    },
    "COD_CONFIRMED": {
        "payment_methods": ["COD"],
        "payment_statuses": ["COD_CONFIRMED"],
    },
    "RESERVATION_EXPIRED": {
        "payment_methods": ["E_WALLET"],
        "payment_statuses": ["EXPIRED"],
    },
    "ORDER_CANCELLED": {
        "payment_methods": ["COD", "E_WALLET"],
        "payment_statuses": ["CANCELLED"],
    },
    "RETURN_RECEIVED": {
        "payment_methods": ["NOT_APPLICABLE"],
        "payment_statuses": ["NOT_APPLICABLE"],
    },
    "STOCK_REPLENISHED": {
        "payment_methods": ["NOT_APPLICABLE"],
        "payment_statuses": ["NOT_APPLICABLE"],
    },
}