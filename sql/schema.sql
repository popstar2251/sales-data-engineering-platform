-- Star schema for the Sales Data Warehouse.
-- Idempotent: safe to run repeatedly (used by the ETL job before each load).

CREATE TABLE IF NOT EXISTS dim_customer (
    customer_id   VARCHAR(20) PRIMARY KEY,
    customer_name VARCHAR(200) NOT NULL,
    segment       VARCHAR(50),
    country       VARCHAR(100),
    city          VARCHAR(100),
    state         VARCHAR(100),
    postal_code   VARCHAR(20),
    region        VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS dim_product (
    product_id   VARCHAR(30) PRIMARY KEY,
    category     VARCHAR(100),
    sub_category VARCHAR(100),
    product_name VARCHAR(300) NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_date (
    date_id      INTEGER PRIMARY KEY,      -- YYYYMMDD
    full_date    DATE NOT NULL,
    year         INTEGER NOT NULL,
    quarter      INTEGER NOT NULL,
    month        INTEGER NOT NULL,
    month_name   VARCHAR(20) NOT NULL,
    day          INTEGER NOT NULL,
    day_of_week  VARCHAR(20) NOT NULL,
    is_weekend   BOOLEAN NOT NULL
);

CREATE TABLE IF NOT EXISTS fact_sales (
    row_id       INTEGER PRIMARY KEY,
    order_id     VARCHAR(30) NOT NULL,
    order_date_id INTEGER NOT NULL REFERENCES dim_date(date_id),
    ship_date_id  INTEGER NOT NULL REFERENCES dim_date(date_id),
    ship_mode    VARCHAR(50),
    customer_id  VARCHAR(20) NOT NULL REFERENCES dim_customer(customer_id),
    product_id   VARCHAR(30) NOT NULL REFERENCES dim_product(product_id),
    sales        NUMERIC(12, 4) NOT NULL,
    quantity     INTEGER NOT NULL,
    discount     NUMERIC(5, 4) DEFAULT 0,
    profit       NUMERIC(12, 4)
);

CREATE INDEX IF NOT EXISTS idx_fact_sales_customer ON fact_sales(customer_id);
CREATE INDEX IF NOT EXISTS idx_fact_sales_product ON fact_sales(product_id);
CREATE INDEX IF NOT EXISTS idx_fact_sales_order_date ON fact_sales(order_date_id);
