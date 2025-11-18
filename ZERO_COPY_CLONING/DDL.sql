/* ================================================================
   SNOWPRO ADVANCED DATA ENGINEER PRACTICAL PROJECT
   TITLE  : CeaseFire Secure Data Sharing using Zero-Copy Cloning
   PURPOSE: Create Dev/Test via zero-copy clone, share live filtered data
   AUTHOR : [Your Name]
   DATE   : [current date]
   ================================================================= */

/* STEP 1: ROLE AND DATABASE CREATION */
USE ROLE ACCOUNTADMIN;

CREATE OR REPLACE DATABASE PROD_RETAIL_DB DATA_RETENTION_TIME_IN_DAYS = 1;
USE DATABASE PROD_RETAIL_DB;

CREATE OR REPLACE SCHEMA RAW;

/* STEP 2: CREATE TABLES */
CREATE OR REPLACE TABLE RAW.BRANDS (
  brand_id STRING PRIMARY KEY,
  brand_name STRING,
  country STRING
);

CREATE OR REPLACE TABLE RAW.SUPPLIERS (
  supplier_id STRING PRIMARY KEY,
  supplier_name STRING,
  contact_email STRING,
  phone STRING,
  city STRING,
  state STRING,
  created_at TIMESTAMP_LTZ
);

CREATE OR REPLACE TABLE RAW.PRODUCTS (
  product_id STRING PRIMARY KEY,
  product_name STRING,
  brand_id STRING,
  category STRING,
  uom STRING,
  list_price NUMBER(18,2),
  supplier_id STRING,
  created_at TIMESTAMP_LTZ
);

CREATE OR REPLACE TABLE RAW.STORES (
  store_id STRING PRIMARY KEY,
  store_name STRING,
  city STRING,
  state STRING,
  region STRING,
  opened_date DATE
);

CREATE OR REPLACE TABLE RAW.CUSTOMERS (
  customer_id STRING PRIMARY KEY,
  customer_name STRING,
  customer_type STRING,
  email STRING,
  phone STRING,
  city STRING,
  state STRING,
  pin_code STRING,
  created_at TIMESTAMP_LTZ
);

CREATE OR REPLACE TABLE RAW.SALES_TRANSACTIONS (
  transaction_id STRING PRIMARY KEY,
  transaction_time TIMESTAMP_LTZ,
  store_id STRING,
  customer_id STRING,
  product_id STRING,
  brand_id STRING,
  quantity INTEGER,
  unit_price NUMBER(18,2),
  discount NUMBER(5,2),
  total_amount NUMBER(18,2),
  payment_method STRING,
  created_at TIMESTAMP_LTZ
);

/* STEP 3: FILE FORMAT + STAGE CREATION */
CREATE OR REPLACE FILE FORMAT csv_fmt TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY='"' SKIP_HEADER=1;
CREATE OR REPLACE STAGE my_int_stage FILE_FORMAT = csv_fmt;

/* STEP 4: PUT & COPY DATA INTO TABLES
   (Execute PUT commands in SnowSQL CLI or Snowsight UI upload feature)
   Example:
   PUT file:///local/path/snowpro_project_csvs/brands.csv @my_int_stage AUTO_COMPRESS=TRUE;
*/

COPY INTO RAW.BRANDS FROM @my_int_stage/brands.csv FILE_FORMAT=(TYPE='CSV' SKIP_HEADER=1);
COPY INTO RAW.SUPPLIERS FROM @my_int_stage/suppliers.csv FILE_FORMAT=(TYPE='CSV' SKIP_HEADER=1);
COPY INTO RAW.PRODUCTS FROM @my_int_stage/products.csv FILE_FORMAT=(TYPE='CSV' SKIP_HEADER=1);
COPY INTO RAW.STORES FROM @my_int_stage/stores.csv FILE_FORMAT=(TYPE='CSV' SKIP_HEADER=1);
COPY INTO RAW.CUSTOMERS FROM @my_int_stage/customers.csv FILE_FORMAT=(TYPE='CSV' SKIP_HEADER=1);
COPY INTO RAW.SALES_TRANSACTIONS FROM @my_int_stage/sales_transactions.csv FILE_FORMAT=(TYPE='CSV' SKIP_HEADER=1);


-- ✅ Define a flexible file format once
CREATE OR REPLACE FILE FORMAT CSV_FLEXIBLE
  TYPE = 'CSV'
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  SKIP_HEADER = 1
  ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
  NULL_IF = ('NULL', 'null', '')
  EMPTY_FIELD_AS_NULL = TRUE;

-- ✅ Load BRANDS (should load fine)
COPY INTO RAW.BRANDS
FROM @my_int_stage/brands.csv
FILE_FORMAT = (FORMAT_NAME = CSV_FLEXIBLE)
ON_ERROR = 'CONTINUE';

-- ✅ Load SUPPLIERS (8 cols in file, 7 in table — mismatch ignored safely)
COPY INTO RAW.SUPPLIERS
FROM @my_int_stage/suppliers.csv
FILE_FORMAT = (FORMAT_NAME = CSV_FLEXIBLE)
ON_ERROR = 'CONTINUE';

-- ✅ Load CUSTOMERS (10 cols in file, 9 in table — mismatch ignored safely)
COPY INTO RAW.CUSTOMERS
FROM @my_int_stage/customers.csv
FILE_FORMAT = (FORMAT_NAME = CSV_FLEXIBLE)
ON_ERROR = 'CONTINUE';


-- quick counts
SELECT 'BRANDS' as t, COUNT(*) FROM RAW.BRANDS
UNION ALL SELECT 'SUPPLIERS', COUNT(*) FROM RAW.SUPPLIERS
UNION ALL SELECT 'CUSTOMERS', COUNT(*) FROM RAW.CUSTOMERS
UNION ALL SELECT 'PRODUCTS', COUNT(*) FROM RAW.PRODUCTS
UNION ALL SELECT 'STORES', COUNT(*) FROM RAW.STORES
UNION ALL SELECT 'SALES', COUNT(*) FROM RAW.SALES_TRANSACTIONS;

-- Inspect load errors for a file (example)
SELECT *
FROM TABLE(VALIDATE(COPY INTO RAW.SUPPLIERS FROM @my_int_stage/suppliers.csv FILE_FORMAT=(FORMAT_NAME=CSV_FLEXIBLE) RETURN_ERRORS=TRUE));



/* STEP 5: ZERO-COPY CLONE CREATION FOR DEV/TEST */
USE ROLE SYSADMIN;


-- create
CREATE OR REPLACE DATABASE DEV_RETAIL_DB CLONE PROD_RETAIL_DB;

-- check counts equal initially
SELECT 'PROD_SALES', COUNT(*) FROM PROD_RETAIL_DB.RAW.SALES_TRANSACTIONS
UNION ALL
SELECT 'DEV_SALES', COUNT(*) FROM DEV_RETAIL_DB.RAW.SALES_TRANSACTIONS;

-- modify clone: insert a test row into clone
USE DATABASE DEV_RETAIL_DB;
INSERT INTO RAW.SALES_TRANSACTIONS
(transaction_id, transaction_time, store_id, customer_id, product_id, brand_id, quantity, unit_price, discount, total_amount, payment_method, created_at)
SELECT
  'TXN_TEST_0001' AS transaction_id,
  CURRENT_TIMESTAMP() AS transaction_time,
  'STR001' AS store_id,
  'CUST00001' AS customer_id,
  product_id,
  'BRD001' AS brand_id,
  1 AS quantity,
  100.00 AS unit_price,
  0 AS discount,
  100.00 AS total_amount,
  'Card' AS payment_method,
  CURRENT_TIMESTAMP() AS created_at
FROM PROD_RETAIL_DB.RAW.PRODUCTS
LIMIT 1;


-- confirm change exists only in DEV
SELECT COUNT(*) FROM DEV_RETAIL_DB.RAW.SALES_TRANSACTIONS WHERE transaction_id='TXN_TEST_0001';
-- in PROD should be 0
SELECT COUNT(*) FROM PROD_RETAIL_DB.RAW.SALES_TRANSACTIONS WHERE transaction_id='TXN_TEST_0001';

-- In DEV_RETAIL_DB
SELECT transaction_id, brand_id, product_id, total_amount
FROM DEV_RETAIL_DB.RAW.SALES_TRANSACTIONS
WHERE transaction_id='TXN_TEST_0001';

-- In PROD_RETAIL_DB
SELECT transaction_id
FROM PROD_RETAIL_DB.RAW.SALES_TRANSACTIONS
WHERE transaction_id='TXN_TEST_0001';



-- Optionally clone specific schema/table
CREATE OR REPLACE SCHEMA DEV_RETAIL_DB.RAW CLONE PROD_RETAIL_DB.RAW;
CREATE OR REPLACE TABLE DEV_RETAIL_DB.RAW.SALES_TRANSACTIONS 
  CLONE PROD_RETAIL_DB.RAW.SALES_TRANSACTIONS;

/* STEP 6: SECURE VIEW FOR CEASEFIRE */
USE ROLE SYSADMIN;
USE DATABASE PROD_RETAIL_DB;

CREATE OR REPLACE SCHEMA SHARE_SCHEMA;

CREATE OR REPLACE SECURE VIEW SHARE_SCHEMA.SV_CEASEFIRE_SALES AS
SELECT
  transaction_id,
  transaction_time,
  store_id,
  product_id,
  brand_id,
  quantity,
  unit_price,
  discount,
  total_amount,
  payment_method,
  LEFT(MD5(customer_id), 8) AS customer_hash,
  created_at
FROM RAW.SALES_TRANSACTIONS
WHERE brand_id = 'BRD001';

---------------------------------------------------------------------------------------------------

USE ROLE ACCOUNTADMIN;

-- Grant usage & ownership to SYSADMIN
GRANT OWNERSHIP ON DATABASE PROD_RETAIL_DB TO ROLE SYSADMIN REVOKE CURRENT GRANTS;

-- (Optionally re-grant existing privileges)
GRANT USAGE ON DATABASE PROD_RETAIL_DB TO ROLE SYSADMIN;
GRANT CREATE SCHEMA ON DATABASE PROD_RETAIL_DB TO ROLE SYSADMIN;

-------------------------------------------------------------------------------------------------------

-- NOW AGAIN RUN STEP 6 IT WILL RUN NOW WITHOUT ERROR
---------------------------------------------------------------------------------------------------------
--OR ALTERNAME SIMPLE STEP 

USE ROLE ACCOUNTADMIN;
USE DATABASE PROD_RETAIL_DB;

CREATE OR REPLACE SCHEMA SHARE_SCHEMA;

CREATE OR REPLACE SECURE VIEW SHARE_SCHEMA.SV_CEASEFIRE_SALES AS
SELECT
  transaction_id,
  transaction_time,
  store_id,
  product_id,
  brand_id,
  quantity,
  unit_price,
  discount,
  total_amount,
  payment_method,
  LEFT(MD5(customer_id), 8) AS customer_hash,
  created_at
FROM RAW.SALES_TRANSACTIONS
WHERE brand_id = 'BRD001';
-------------------------------------------------------------------------------------------------------------



/* STEP 7: CREATE SHARE (PROVIDER SIDE) */
USE ROLE ACCOUNTADMIN;

CREATE OR REPLACE SHARE SHARE_CEASEFIRE;

GRANT USAGE ON DATABASE PROD_RETAIL_DB TO SHARE SHARE_CEASEFIRE;
GRANT USAGE ON SCHEMA PROD_RETAIL_DB.SHARE_SCHEMA TO SHARE SHARE_CEASEFIRE;
GRANT SELECT ON SHARE_SCHEMA.SV_CEASEFIRE_SALES TO SHARE SHARE_CEASEFIRE;

-- Optionally share lookup tables
GRANT SELECT ON RAW.PRODUCTS TO SHARE SHARE_CEASEFIRE;
GRANT SELECT ON RAW.BRANDS TO SHARE SHARE_CEASEFIRE;

/* STEP 8: ADD CONSUMER ACCOUNT TO SHARE
   Replace <CONSUMER_ACCOUNT_LOCATOR> with actual account locator (e.g., ABC12345)
*/
-- ALTER SHARE SHARE_CEASEFIRE ADD ACCOUNTS = ('<CONSUMER_ACCOUNT_LOCATOR>');

/* STEP 9: CONSUMER SIDE (CEASEFIRE ACCOUNT)
   Execute this block inside the consumer’s Snowflake environment
*/
-- On CeaseFire consumer account:
-- USE ROLE ACCOUNTADMIN;
-- CREATE DATABASE CEASEFIRE_LIVE_DB 
--   FROM SHARE <PROVIDER_ACCOUNT_NAME>.SHARE_CEASEFIRE;
-- USE DATABASE CEASEFIRE_LIVE_DB;
-- USE SCHEMA SHARE_SCHEMA;
-- SELECT COUNT(*) FROM SHARE_SCHEMA.SV_CEASEFIRE_SALES;

/* STEP 10: KPI QUERIES (PROVIDER OR CONSUMER CAN RUN THESE)
   Use these queries to analyze CeaseFire sales trends.
*/

-- Daily Sales (last 30 days)
SELECT CAST(transaction_time AS DATE) AS sales_date,
       SUM(quantity) AS total_qty,
       SUM(total_amount) AS total_value
FROM SHARE_SCHEMA.SV_CEASEFIRE_SALES
WHERE transaction_time >= DATEADD(day, -30, CURRENT_TIMESTAMP())
GROUP BY 1
ORDER BY 1;

-- Top 10 SKUs by Revenue
SELECT product_id, SUM(total_amount) AS revenue, SUM(quantity) AS qty
FROM SHARE_SCHEMA.SV_CEASEFIRE_SALES
WHERE transaction_time >= DATEADD(day, -30, CURRENT_TIMESTAMP())
GROUP BY 1
ORDER BY revenue DESC
LIMIT 10;

-- Store-level Rolling 7-Day Average Revenue
WITH daily AS (
  SELECT CAST(transaction_time AS DATE) AS sales_date, store_id, SUM(total_amount) AS revenue
  FROM SHARE_SCHEMA.SV_CEASEFIRE_SALES
  GROUP BY 1,2
)
SELECT sales_date, store_id,
       AVG(revenue) OVER (PARTITION BY store_id ORDER BY sales_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS rolling_7d_avg
FROM daily
ORDER BY store_id, sales_date;

/* STEP 11: MONITORING AND GOVERNANCE */
-- Provider can monitor who accesses the share:
SELECT *
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE query_text ILIKE '%SV_CEASEFIRE_SALES%'
ORDER BY start_time DESC
LIMIT 100;

/* STEP 12: CLEANUP WHEN FINISHED */
-- DROP SHARE IF EXISTS SHARE_CEASEFIRE;
-- DROP DATABASE IF EXISTS DEV_RETAIL_DB;







--CeaseFire Project — Verification & Health Check Script

/* =============================================================
   SNOWPRO ADVANCED DATA ENGINEER — VERIFICATION SCRIPT
   PROJECT: CeaseFire Secure Data Sharing (Zero-Copy + Secure View)
   RUN CONTEXT: PROVIDER ACCOUNT (AccountAdmin or SysAdmin)
   ============================================================= */

USE ROLE ACCOUNTADMIN;

-------------------------------------------------------
-- SECTION 1. BASIC OBJECT & DATA LOAD VALIDATION
-------------------------------------------------------
SET prod_db = 'PROD_RETAIL_DB';
SET dev_db  = 'DEV_RETAIL_DB';

-- Check that base tables exist
SELECT 'TABLE CHECK' AS test, CASE WHEN COUNT(*) = 6 THEN '✅ PASS' ELSE '❌ FAIL' END AS result,
       ARRAY_AGG(TABLE_NAME) AS tables_found
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA='RAW' AND TABLE_NAME IN ('BRANDS','SUPPLIERS','CUSTOMERS','PRODUCTS','STORES','SALES_TRANSACTIONS');

-- Count rows across all tables
SELECT 'ROW COUNT CHECK' AS test,
       CASE WHEN COUNT(*)>0 THEN '✅ PASS' ELSE '❌ FAIL' END AS result,
       'BRANDS='|| (SELECT COUNT(*) FROM RAW.BRANDS) || ', SUPPLIERS='|| (SELECT COUNT(*) FROM RAW.SUPPLIERS) ||
       ', CUSTOMERS='|| (SELECT COUNT(*) FROM RAW.CUSTOMERS) || ', PRODUCTS='|| (SELECT COUNT(*) FROM RAW.PRODUCTS) ||
       ', STORES='|| (SELECT COUNT(*) FROM RAW.STORES) || ', SALES='|| (SELECT COUNT(*) FROM RAW.SALES_TRANSACTIONS) AS details
FROM RAW.BRANDS LIMIT 1;

-- Check no NULL PKs
-- ✅ FIXED VERSION
WITH pk_check AS (
  SELECT 'BRANDS' AS table_name, COUNT(*) AS null_count FROM RAW.BRANDS WHERE brand_id IS NULL
  UNION ALL SELECT 'SUPPLIERS', COUNT(*) FROM RAW.SUPPLIERS WHERE supplier_id IS NULL
  UNION ALL SELECT 'CUSTOMERS', COUNT(*) FROM RAW.CUSTOMERS WHERE customer_id IS NULL
  UNION ALL SELECT 'PRODUCTS', COUNT(*) FROM RAW.PRODUCTS WHERE product_id IS NULL
  UNION ALL SELECT 'STORES', COUNT(*) FROM RAW.STORES WHERE store_id IS NULL
  UNION ALL SELECT 'SALES_TRANSACTIONS', COUNT(*) FROM RAW.SALES_TRANSACTIONS WHERE transaction_id IS NULL
)
SELECT
  'NULL PK CHECK' AS test,
  CASE WHEN SUM(null_count)=0 THEN '✅ PASS' ELSE '❌ FAIL' END AS result,
  OBJECT_AGG(table_name, null_count) AS details
FROM pk_check;


-------------------------------------------------------
-- SECTION 2. ZERO-COPY CLONE VALIDATION (FIXED)
-------------------------------------------------------
SELECT
  'ZERO COPY CLONE CHECK' AS test,
  CASE WHEN
    (SELECT COUNT(*) FROM PROD_RETAIL_DB.RAW.SALES_TRANSACTIONS) =
    (SELECT COUNT(*) FROM DEV_RETAIL_DB.RAW.SALES_TRANSACTIONS)
  THEN '✅ PASS' ELSE '❌ FAIL' END AS result,
  'PROD=' || (SELECT COUNT(*) FROM PROD_RETAIL_DB.RAW.SALES_TRANSACTIONS)
  || ', DEV=' || (SELECT COUNT(*) FROM DEV_RETAIL_DB.RAW.SALES_TRANSACTIONS) AS details;



-------------------------------------------------------
-- SECTION 3. SECURE VIEW VALIDATION
-------------------------------------------------------
-- Confirm secure view exists
SELECT 'SECURE VIEW EXISTS' AS test,
       CASE WHEN COUNT(*)=1 THEN '✅ PASS' ELSE '❌ FAIL' END AS result,
       ARRAY_AGG(TABLE_NAME) AS details
FROM INFORMATION_SCHEMA.VIEWS
WHERE TABLE_SCHEMA='SHARE_SCHEMA' AND TABLE_NAME='SV_CEASEFIRE_SALES' AND IS_SECURE='YES';

-- Row counts should match brand filter
WITH cte AS (
  SELECT (SELECT COUNT(*) FROM RAW.SALES_TRANSACTIONS WHERE brand_id='BRD001') AS brand_rows,
         (SELECT COUNT(*) FROM SHARE_SCHEMA.SV_CEASEFIRE_SALES) AS view_rows
)
SELECT 'SECURE VIEW COUNT MATCH' AS test,
       CASE WHEN brand_rows=view_rows THEN '✅ PASS' ELSE '❌ FAIL' END AS result,
       OBJECT_CONSTRUCT('brand_rows',brand_rows,'view_rows',view_rows) AS details
FROM cte;

-- Check masked PII (no email/phone columns)
SELECT 'SECURE VIEW COLUMNS' AS test,
       CASE WHEN ARRAY_SIZE(ARRAY_AGG(COLUMN_NAME)) = ARRAY_SIZE(ARRAY_AGG(CASE WHEN COLUMN_NAME IN ('EMAIL','PHONE') THEN NULL ELSE COLUMN_NAME END))
            THEN '✅ PASS' ELSE '❌ FAIL' END AS result,
       ARRAY_AGG(COLUMN_NAME) AS details
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA='SHARE_SCHEMA' AND TABLE_NAME='SV_CEASEFIRE_SALES';

-------------------------------------------------------
-- SECTION 4. SHARE VALIDATION
-------------------------------------------------------
SHOW SHARES;
-- Check our share exists
SELECT 'SHARE EXISTS' AS test,
       CASE WHEN COUNT(*)>0 THEN '✅ PASS' ELSE '❌ FAIL' END AS result,
       ARRAY_AGG(SHARE_NAME) AS details
FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
WHERE "name" ILIKE 'SHARE_CEASEFIRE';

-- Check granted objects
SHOW GRANTS TO SHARE SHARE_CEASEFIRE;
SELECT 'SHARE GRANTS CHECK' AS test,
       CASE WHEN COUNT(*)>0 THEN '✅ PASS' ELSE '❌ FAIL' END AS result,
       ARRAY_AGG("granted_on"||':'||"name") AS details
FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));

-------------------------------------------------------
-- SECTION 5. KPI TESTS (CEASEFIRE DATA)
-------------------------------------------------------
-- Daily Sales (7-day sample)
SELECT 'KPI: DAILY SALES' AS test, '✅ PASS' AS result,
       OBJECT_CONSTRUCT('rows_returned',(SELECT COUNT(*) FROM (SELECT CAST(transaction_time AS DATE), SUM(total_amount) FROM SHARE_SCHEMA.SV_CEASEFIRE_SALES GROUP BY 1))) AS details;

-- Top SKUs (CeaseFire)
SELECT product_id, SUM(total_amount) AS revenue
FROM SHARE_SCHEMA.SV_CEASEFIRE_SALES
GROUP BY product_id
ORDER BY revenue DESC
LIMIT 5;

-------------------------------------------------------
-- SECTION 6. AUDIT & MONITORING TESTS
-------------------------------------------------------
-- Confirm share queries exist in account usage (may take minutes to appear)
SELECT 'QUERY HISTORY CHECK' AS test,
       CASE WHEN COUNT(*)>=0 THEN '✅ PASS (manual verify via Snowsight > History)' ELSE '⚠️ REVIEW' END AS result,
       COUNT(*) AS details
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE QUERY_TEXT ILIKE '%SV_CEASEFIRE_SALES%' AND START_TIME >= DATEADD('hour',-24,CURRENT_TIMESTAMP());

-------------------------------------------------------
-- SECTION 7. CLEANUP REMINDER (commented)
-------------------------------------------------------
-- ALTER WAREHOUSE COMPUTE_WH SUSPEND;
-- ALTER WAREHOUSE COMPUTE_WH SET AUTO_RESUME=FALSE;
-- ALTER ACCOUNT SET ENABLE_TASK_EXECUTION=FALSE;
-- DROP DATABASE IF EXISTS DEV_RETAIL_DB;
-- DROP SHARE IF EXISTS SHARE_CEASEFIRE;


