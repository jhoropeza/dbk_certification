-- 1 Ingest order JSON files from cloud storage
CREATE OR REFRESH STREAMING TABLE my_catalog.1_bronze_db.orders_bronze
 COMMENT "Ingest order JSON files from cloud storage" -- Adds a comment to the table
 TBLPROPERTIES(
  "quality" = "bronze",
  "pipelines.reset.allowed" = false  -- prevent full table refreshes on the bronze table

 )
AS
 SELECT 
 *,
 current_timestamp() AS processing_time,
 _metadata.file_name AS source_file
 FROM STREAM read_files(
  "${source_orders}", -- Uses the source configuration variable set in the pipeline settings
  format => "JSON",
  multiLine => 'true'
 );


 -- 2 Ingest to Silver con expects
 CREATE OR REFRESH STREAMING TABLE my_catalog.2_SILVER_db.orders_silver
 (
  -- Check for a 'Y' or  'N' in the notification column, retuns a warning
  CONSTRAINT valid_notifications EXPECT (notifications IN ('Y', 'N')), 
  -- Drop row if not a valid date (set to 2021-01-01)
  CONSTRAINT valid_date EXPECT(order_timestamp > "2021-01-01") ON VIOLATION DROP ROW,
  -- Fail pipeline if null
  CONSTRAINT valid_id EXPECT (customer_id IS NOT NULL) ON VIOLATION FAIL UPDATE
 ) 
 COMMENT "Silver clean orders table" --Adds a comment to the table
 TBLPROPERTIES("quality" = "silver")
 AS
 SELECT
  order_id,
  timestamp(order_timestamp),
  customer_id,
  notifications
  FROM STREAM my_catalog.1_bronze_db.orders_bronze;

  -- 3 Create the materialized view aggregation from the order_silve table with the summarization
 CREATE OR REFRESH MATERIALIZED VIEW my_catalog.3_gold_db.orders_by_date_gold
  COMMENT "Gold orders by date table" --Adds a comment to the table
  TBLPROPERTIES("quality" = "gold")   -- Adds a simple table property to the table
  AS
 SELECT
  date(order_timestamp) AS order_date ,
  count(*) AS total_daily_orders
  FROM my_catalog.2_silver_db.orders_silver
  GROUP BY date(order_timestamp);



