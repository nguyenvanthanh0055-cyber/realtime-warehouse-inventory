

CREATE TABLE IF NOT EXISTS current_inventory (
    campaign_id VARCHAR(100) NOT NULL,
    sku_id VARCHAR(100) NOT NULL,
    warehouse_id VARCHAR(100) NOT NULL,
    product_name VARCHAR(255),

    initial_sellable_stock INTEGER NOT NULL DEFAULT 0,
    current_sellable_stock INTEGER NOT NULL DEFAULT 0,
    low_stock_threshold INTEGER NOT NULL DEFAULT 0,
    status VARCHAR(30) NOT NULL DEFAULT 'NORMAL',
    last_event_id VARCHAR(100),
    last_event_time TIMESTAMPTZ,

    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    PRIMARY KEY (campaign_id, sku_id, warehouse_id),

    CONSTRAINT chk_current_inventory_status
        CHECK (status IN ('NORMAL', 'LOW_STOCK', 'OVERSELL')),

    CONSTRAINT chk_initial_sellable_stock_non_negative
        CHECK (initial_sellable_stock >= 0),

    CONSTRAINT chk_low_stock_threshold_non_negative
        CHECK (low_stock_threshold >= 0)
);

CREATE TABLE IF NOT EXISTS current_inventory_state_history(
    event_id VARCHAR(100) NOT NULL,           
    campaign_id VARCHAR(100) NOT NULL,
    sku_id VARCHAR(50) NOT NULL,
    warehouse_id VARCHAR(50) NOT NULL,
    event_time TIMESTAMPTZ ,
    business_timestamp TIMESTAMPTZ,
    business_date DATE,
    event_type VARCHAR(100),
    quantity INTEGER DEFAULT 0,
    movement_qty INTEGER DEFAULT 0,
    previous_sellable_stock INTEGER DEFAULT 0,
    current_sellable_stock INTEGER DEFAULT 0,
    status VARCHAR(50),
    processed_at TIMESTAMPTZ
);


CREATE TABLE IF NOT EXISTS processed_events (
    event_id VARCHAR(100) PRIMARY KEY,

    campaign_id VARCHAR(100) NOT NULL,
    event_time TIMESTAMPTZ NOT NULL,
    event_type VARCHAR(50) NOT NULL,

    sku_id VARCHAR(100) NOT NULL,
    warehouse_id VARCHAR(100) NOT NULL,

    processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);


CREATE TABLE IF NOT EXISTS inventory_alerts (
    alert_id BIGSERIAL PRIMARY KEY,

    campaign_id VARCHAR(100) NOT NULL,
    alert_type VARCHAR(50) NOT NULL,

    sku_id VARCHAR(100),
    warehouse_id VARCHAR(100),

    current_sellable_stock INTEGER,
    event_id VARCHAR(100),
    message TEXT,

    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT chk_inventory_alert_type
        CHECK (
            alert_type IN (
                'LOW_STOCK',
                'OVERSELL',
                'INVALID_EVENT',
                'UNKNOWN_SKU'
            )
        )
);


CREATE TABLE IF NOT EXISTS sales_velocity_window (
    campaign_id VARCHAR(100) NOT NULL,

    window_start TIMESTAMPTZ NOT NULL,
    window_end TIMESTAMPTZ NOT NULL,
    window_size_minutes INTEGER NOT NULL DEFAULT 5,

    sku_id VARCHAR(100) NOT NULL,
    warehouse_id VARCHAR(100) NOT NULL,
    promotion_id VARCHAR(100),

    paid_order_count INTEGER NOT NULL DEFAULT 0,
    sold_qty INTEGER NOT NULL DEFAULT 0,

    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    PRIMARY KEY (
        campaign_id,
        window_start,
        window_end,
        sku_id,
        warehouse_id
    ),

    CONSTRAINT chk_sales_velocity_window_size
        CHECK (window_size_minutes > 0),

    CONSTRAINT chk_sales_velocity_paid_order_count
        CHECK (paid_order_count >= 0),

    CONSTRAINT chk_sales_velocity_sold_qty
        CHECK (sold_qty >= 0)
);

CREATE TABLE IF NOT EXISTS promotion_metrics (
    campaign_id VARCHAR(100) NOT NULL,
    promotion_id VARCHAR(100) NOT NULL,
    sku_id VARCHAR(100) NOT NULL,
    warehouse_id VARCHAR(100) NOT NULL,

    promotion_quota INTEGER NOT NULL DEFAULT 0,
    promotion_consumed_qty INTEGER NOT NULL DEFAULT 0,
    promotion_cancelled_qty INTEGER NOT NULL DEFAULT 0,

    deal_sold_out_at TIMESTAMPTZ,

    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    PRIMARY KEY (
        campaign_id,
        promotion_id,
        sku_id,
        warehouse_id
    ),

    CONSTRAINT chk_promotion_quota_non_negative
        CHECK (promotion_quota >= 0),

    CONSTRAINT chk_promotion_consumed_qty_non_negative
        CHECK (promotion_consumed_qty >= 0),

    CONSTRAINT chk_promotion_cancelled_qty_non_negative
        CHECK (promotion_cancelled_qty >= 0)
);


CREATE TABLE IF NOT EXISTS reconciliation_result (
    recon_date DATE NOT NULL,
    campaign_id VARCHAR(100) NOT NULL,
    sku_id VARCHAR(100) NOT NULL,
    warehouse_id VARCHAR(100) NOT NULL,
    opening_sellable_stock INTEGER NOT NULL DEFAULT 0,
    net_movement_qty INTEGER NOT NULL DEFAULT 0,
    batch_recomputed_sellable_stock INTEGER NOT NULL DEFAULT 0,
    streaming_sellable_stock INTEGER NOT NULL DEFAULT 0,
    diff_qty INTEGER NOT NULL DEFAULT 0,
    status VARCHAR(30) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (
        recon_date,
        campaign_id,
        sku_id,
        warehouse_id
    ),

    CONSTRAINT chk_reconciliation_status
        CHECK (status IN ('MATCH', 'MISMATCH'))
);



CREATE TABLE IF NOT EXISTS raw_inventory_events (
    event_id VARCHAR(100) PRIMARY KEY,

    campaign_id VARCHAR(100),
    event_time TIMESTAMPTZ,
    event_type VARCHAR(50),

    order_id VARCHAR(100),
    sku_id VARCHAR(100),
    warehouse_id VARCHAR(100),

    quantity INTEGER,
    unit_price NUMERIC(18, 2),

    promotion_id VARCHAR(100),
    promotion_applied BOOLEAN,

    payment_method VARCHAR(50),
    payment_status VARCHAR(50),
    reservation_expires_at TIMESTAMPTZ,

    source VARCHAR(100),

    movement_qty INTEGER,
    is_valid_event BOOLEAN,
    invalid_reason TEXT,

    kafka_topic VARCHAR(100),
    kafka_partition INTEGER,
    kafka_offset BIGINT,

    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);


CREATE INDEX IF NOT EXISTS idx_current_inventory_campaign
    ON current_inventory (campaign_id);

CREATE INDEX IF NOT EXISTS idx_current_inventory_status
    ON current_inventory (status);

CREATE INDEX IF NOT EXISTS idx_processed_events_campaign_time
    ON processed_events (campaign_id, event_time);

CREATE INDEX IF NOT EXISTS idx_processed_events_sku_time
    ON processed_events (sku_id, warehouse_id, event_time);

CREATE INDEX IF NOT EXISTS idx_inventory_alerts_campaign_created
    ON inventory_alerts (campaign_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_inventory_alerts_type_created
    ON inventory_alerts (alert_type, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_sales_velocity_campaign_window
    ON sales_velocity_window (campaign_id, window_start, window_end);

CREATE INDEX IF NOT EXISTS idx_promotion_metrics_campaign
    ON promotion_metrics (campaign_id, promotion_id);

CREATE INDEX IF NOT EXISTS idx_reconciliation_campaign_date
    ON reconciliation_result (campaign_id, recon_date);

CREATE INDEX IF NOT EXISTS idx_raw_inventory_events_campaign_time
    ON raw_inventory_events (campaign_id, event_time);

CREATE INDEX IF NOT EXISTS idx_raw_inventory_events_sku_time
    ON raw_inventory_events (sku_id, warehouse_id, event_time);

