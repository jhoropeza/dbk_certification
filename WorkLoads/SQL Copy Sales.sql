CREATE TABLE IF NOT EXISTS my_catalog.bronze_wl.sales_bronze
AS 
SELECT * FROM my_catalog.default.sales;
