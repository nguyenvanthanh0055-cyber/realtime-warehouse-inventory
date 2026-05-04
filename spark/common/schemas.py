from pyspark.sql.types import(
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
    BooleanType
)

inventory_event_schema = StructType([
    StructField("event_id", StringType(), False),
    StructField("campaign_id", StringType(), False),
    StructField("event_time", StringType(), False),
    StructField("event_type", StringType(), False),
    StructField("order_id", StringType(), True),
    StructField("sku_id", StringType(), False),
    StructField("warehouse_id", StringType(), False),
    StructField("quantity", IntegerType(), False),
    StructField("unit_price", DoubleType(), False),
    StructField("promotion_id", StringType(), True),
    StructField("promotion_applied", BooleanType(), False),
    StructField("payment_method", StringType(), False),
    StructField("payment_status", StringType(), False),
    StructField("reservation_expires_at", StringType(), False),
    StructField("source", StringType(), False),    
])


