-- Bronze raw data
CREATE OR REFRESH STREAMING TABLE my_catalog.`1_bronze_db`.customer_bronze_raw
  COMMENT "Raw data from customers CDC feed"
  TBLPROPERTIES(
    "quality" = "bronze",
    "pipelines.reset.allowed" = false
  )
  AS
SELECT 
  *, 
  CURRENT_TIMESTAMP() AS processing_time,
  _metadata.file_name AS source_file
FROM STREAM read_files(
  '${source_customers}', -- Replace :source_vol with literal path
  format => "json",
  multiLine => 'true'
);

-- 2 Bronze Cleaned
CREATE STREAMING TABLE IF NOT EXISTS my_catalog.1_bronze_db.customer_bronze_clean
(
  CONSTRAINT valid_id EXPECT(customer_id IS NOT NULL) ON VIOLATION FAIL UPDATE,
  CONSTRAINT valid_operation EXPECT (operation IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_name EXPECT (name IS NOT NULL OR operation = "DELETE"),
  CONSTRAINT valid_address EXPECT (
    (address IS NOT NULL AND 
    city IS NOT NULL AND 
    state IS NOT NULL AND 
    zip_code IS NOT NULL and
    timestamp IS NOT NULL) OR operation = "DELETE"),
  CONSTRAINT valid_email EXPECT
    (
      rlike(email, '^([a-zA-Z0-9_\\-\\.]+)@[a-zA-Z0-9_\\-\\.]+\\.([a-zA-Z]{2,4})$')
      OR operation = "DELETE"
    ) ON VIOLATION DROP ROW
)
COMMENT "Cleaned RAW BRONZE TIMESTAMP COLUMN AND DATA QUALITY CONSTRAINTS"
AS SELECT 
  *,
  to_timestamp(`timestamp`) AS timestamp_datetime
 FROM STREAM my_catalog.`1_bronze_db`.customer_bronze_raw;

-- 3 Silver type 1


-- 3 Silver type 1
CREATE OR REFRESH STREAMING TABLE my_catalog.`2_silver_db`.scd_type_1_customers_silver
  COMMENT 'SCD Type 1 Historical Customer Data';

CREATE FLOW scd_type_1_flow AS
AUTO CDC INTO  my_catalog.`2_silver_db`.scd_type_1_customers_silver --TARGET TABLE TO UPDATE WITH scd tYPE 1(OR 2)
FROM STREAM my_catalog.`1_bronze_db`.customer_bronze_clean -- Source records to determine updates, deletes and inserts
KEYS (customer_id)                         -- Primary key for identifying records
APPLY AS DELETE WHEN operation = "DELETE"  -- Handle deletes from source to the target
SEQUENCE BY timestamp_datetime             -- Defines order of operations for applying changes
COLUMNS * EXCEPT (timestamp, _rescued_data, operation)  --Select columns and exclude metada fields
STORED AS SCD TYPE 1;                      -- Use SCD type 1 TO update the target table (no historical information)