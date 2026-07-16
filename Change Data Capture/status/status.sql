CREATE OR REFRESH STREAMING TABLE my_catalog.1_bronze_db.status_bronze
COMMENT "Raw status events for orders"
TBLPROPERTIES(
  "quality" = "bronze",
  "pipelines.reset.allowed" = false
)
AS
SELECT
  *,
  current_timestamp() AS processing_time,
  _metadata.file_name AS source_file
FROM STREAM read_files(
  "${source_status}",   -- Ruta configurada en el pipeline
  format => "json",
  multiLine => "true"
);

-- 2. Silver con constraints
CREATE OR REFRESH STREAMING TABLE my_catalog.2_silver_db.status_silver
(
  CONSTRAINT valid_order EXPECT (order_id IS NOT NULL) ON VIOLATION FAIL UPDATE,
  CONSTRAINT valid_status EXPECT (status IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_timestamp EXPECT (status_timestamp > "2021-01-01") ON VIOLATION DROP ROW
)
COMMENT "Cleaned status events"
TBLPROPERTIES("quality" = "silver")
AS
SELECT
  order_id,
  status,
  to_timestamp(status_timestamp) AS status_timestamp,
  processing_time,
  source_file
FROM STREAM my_catalog.1_bronze_db.status_bronze;

-- 3. Gold : Full order status (materialized view)
CREATE OR REFRESH MATERIALIZED VIEW my_catalog.3_gold_db.full_order_status_gold
COMMENT "Latest status for each order"
TBLPROPERTIES("quality" = "gold")
AS
SELECT
  o.order_id,
  o.order_timestamp,
  o.customer_id,
  o.notifications,
  s.status,
  s.status_timestamp
FROM my_catalog.2_silver_db.orders_silver o
LEFT JOIN (
    SELECT
      order_id,
      status,
      status_timestamp,
      ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY status_timestamp DESC) AS rn
    FROM my_catalog.2_silver_db.status_silver
) s
ON o.order_id = s.order_id AND s.rn = 1;

-- 4. Gold : Cancelled orders
CREATE OR REFRESH MATERIALIZED VIEW my_catalog.3_gold_db.cancelled_orders_gold
COMMENT "Cancelled orders and days until cancellation"
TBLPROPERTIES("quality" = "gold")
AS
SELECT
  order_id,
  order_timestamp,
  status_timestamp AS cancellation_timestamp,
  datediff(status_timestamp, order_timestamp) AS days_until_cancellation
FROM my_catalog.3_gold_db.full_order_status_gold
WHERE status = 'CANCELLED';

CREATE OR REFRESH MATERIALIZED VIEW my_catalog.3_gold_db.delivered_orders_gold
COMMENT "dELIVERED orders and days until cancellation"
TBLPROPERTIES("quality" = "gold")
AS
SELECT
  order_id,
  order_timestamp,
  status_timestamp AS cancellation_timestamp,
  datediff(status_timestamp, order_timestamp) AS days_until_delivered
FROM my_catalog.3_gold_db.full_order_status_gold
WHERE status <> 'DELIVERED';