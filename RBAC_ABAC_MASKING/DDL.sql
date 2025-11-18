-- ==========================================================
-- Project: Falhari & Frugivore (Packaged fruit salad companies) — GDPR/CCPA Compliant Framework
-- Topic Coverage:
--   • RBAC Best Practices
--   • Column-level Security with Dynamic Data Masking
--   • Row-level Security with Row Access Policies
--   • Policy-driven (ABAC-style) security using attributes (REGION, STORE_ID)
-- ==========================================================

--------------------------------------------------------------
-- 0️⃣  SETUP: ACCOUNTADMIN, WAREHOUSE
--------------------------------------------------------------
USE ROLE ACCOUNTADMIN;

-- Create a small shared warehouse for this lab/demo
CREATE WAREHOUSE IF NOT EXISTS COMPLIANCE_WH
  WAREHOUSE_SIZE = 'XSMALL'
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE
  INITIALLY_SUSPENDED = TRUE;

--------------------------------------------------------------
-- 1️⃣  CREATE DATABASE, SCHEMA, AND FILE FORMAT
--------------------------------------------------------------
CREATE DATABASE IF NOT EXISTS COMPLIANCE_DB;
USE DATABASE COMPLIANCE_DB;
CREATE SCHEMA IF NOT EXISTS PUBLIC;
USE SCHEMA PUBLIC;

CREATE OR REPLACE FILE FORMAT CSV_FORMAT
  TYPE = 'CSV'
  FIELD_DELIMITER = ','
  SKIP_HEADER = 1
  NULL_IF = ('\\N','NULL','');

--------------------------------------------------------------
-- 2️⃣  CREATE INTERNAL STAGE FOR DATA LOADS
--------------------------------------------------------------
CREATE OR REPLACE STAGE falhari_stage
  FILE_FORMAT = CSV_FORMAT
  COMMENT = 'Internal stage for Falhari & Frugivore data ingestion';

-- Optional: Inspect stage
-- DESC STAGE falhari_stage;

--------------------------------------------------------------
-- 3️⃣  TABLE CREATION (DDL)
--------------------------------------------------------------
CREATE OR REPLACE TABLE STORES (
  STORE_ID STRING PRIMARY KEY,
  STORE_NAME STRING,
  CITY STRING,
  STATE STRING,
  REGION STRING,
  OPENED_AT DATE
);

CREATE OR REPLACE TABLE PRODUCTS (
  PRODUCT_ID STRING PRIMARY KEY,
  PRODUCT_NAME STRING,
  CATEGORY STRING,
  PRICE NUMBER(10,2),
  PACKAGE_SIZE STRING
);

CREATE OR REPLACE TABLE CUSTOMERS (
  CUSTOMER_ID STRING PRIMARY KEY,
  CUSTOMER_NAME STRING,
  EMAIL STRING,
  PHONE STRING,
  CITY STRING,
  STATE STRING,
  PIN_CODE STRING,
  DIVISION STRING,
  CREATED_AT TIMESTAMP_LTZ
);

CREATE OR REPLACE TABLE EMPLOYEES (
  EMPLOYEE_ID STRING PRIMARY KEY,
  EMPLOYEE_NAME STRING,
  USERNAME STRING,
  ROLE_NAME STRING,
  STORE_ID STRING,
  REGION STRING,
  EMAIL STRING,
  PHONE STRING,
  JOINED_AT DATE
);

CREATE OR REPLACE TABLE FCT_SALES (
  TRANSACTION_ID STRING PRIMARY KEY,
  STORE_ID STRING,
  PRODUCT_ID STRING,
  CUSTOMER_ID STRING,
  QUANTITY NUMBER,
  UNIT_PRICE NUMBER(10,2),
  TOTAL_AMOUNT NUMBER(12,2),
  TRANSACTION_DT TIMESTAMP_LTZ
);

-- Credit-card fact table 
CREATE OR REPLACE TABLE CREDIT_CARD_CUSTOMERS (
    CUST_ID VARCHAR(20) PRIMARY KEY,
    CREDIT_CARD_NUMBER VARCHAR(19),
    BALANCE NUMBER(10,2),
    PURCHASES NUMBER(10,2),
    INSTALLMENTS_PURCHASES NUMBER(10,2),
    CASH_ADVANCE NUMBER(10,2),
    CREDIT_LIMIT NUMBER(10,2),
    PAYMENTS NUMBER(10,2),
    MINIMUM_PAYMENTS NUMBER(10,2),
    TENURE INTEGER,
    DATE_OF_TXN DATE
);

--------------------------------------------------------------
-- 4️⃣  COPY INTO COMMANDS 
--     Upload CSVs from your local machine to @falhari_stage first.
--------------------------------------------------------------
-- Example PUT commands to run in SnowSQL / Worksheet:
-- PUT file://C:\\path\\to\\stores.csv @falhari_stage AUTO_COMPRESS=FALSE;
-- PUT file://C:\\path\\to\\products.csv @falhari_stage AUTO_COMPRESS=FALSE;
-- PUT file://C:\\path\\to\\customers.csv @falhari_stage AUTO_COMPRESS=FALSE;
-- PUT file://C:\\path\\to\\employees.csv @falhari_stage AUTO_COMPRESS=FALSE;
-- PUT file://C:\\path\\to\\sales.csv @falhari_stage AUTO_COMPRESS=FALSE;
-- PUT file://C:\\path\\to\\credit_card_customers.csv @falhari_stage AUTO_COMPRESS=FALSE;

COPY INTO STORES
  FROM @falhari_stage/stores.csv
  FILE_FORMAT = CSV_FORMAT
  ON_ERROR = 'ABORT_STATEMENT';

COPY INTO PRODUCTS
  FROM @falhari_stage/products.csv
  FILE_FORMAT = CSV_FORMAT
  ON_ERROR = 'ABORT_STATEMENT';

COPY INTO CUSTOMERS
  FROM @falhari_stage/customers.csv
  FILE_FORMAT = CSV_FORMAT
  ON_ERROR = 'ABORT_STATEMENT';

COPY INTO EMPLOYEES
  FROM @falhari_stage/employees.csv
  FILE_FORMAT = CSV_FORMAT
  ON_ERROR = 'ABORT_STATEMENT';

COPY INTO FCT_SALES
  FROM @falhari_stage/sales.csv
  FILE_FORMAT = CSV_FORMAT
  ON_ERROR = 'ABORT_STATEMENT';

COPY INTO CREDIT_CARD_CUSTOMERS
  FROM @falhari_stage/credit_card_customers.csv
  FILE_FORMAT = CSV_FORMAT
  ON_ERROR = 'ABORT_STATEMENT';

--------------------------------------------------------------
-- 5️⃣  RBAC: ROLES, USERS & GRANTS (Best Practices)
--     Unified roles: STORE_MANAGER, MARKETING_ANALYST, REGIONAL_MANAGER,
--                    DATA_PRIVILEGED (super-analyst), DATA_LOADER (ETL)
--------------------------------------------------------------
USE ROLE ACCOUNTADMIN;

CREATE ROLE IF NOT EXISTS ROLE_STORE_MANAGER;
CREATE ROLE IF NOT EXISTS ROLE_MARKETING_ANALYST;
CREATE ROLE IF NOT EXISTS ROLE_REGIONAL_MANAGER;
CREATE ROLE IF NOT EXISTS ROLE_DATA_PRIVILEGED;
CREATE ROLE IF NOT EXISTS ROLE_DATA_LOADER;

-- Role hierarchy: Regional managers can act as store managers
GRANT ROLE ROLE_STORE_MANAGER TO ROLE ROLE_REGIONAL_MANAGER;

-- LAB USERS (you can adjust names if needed)
CREATE USER IF NOT EXISTS user_store_mgr 
  PASSWORD='ChangeIt123!' 
  DEFAULT_ROLE=ROLE_STORE_MANAGER 
  DEFAULT_WAREHOUSE=COMPLIANCE_WH
  MUST_CHANGE_PASSWORD=FALSE;

CREATE USER IF NOT EXISTS user_marketing_analyst 
  PASSWORD='ChangeIt123!' 
  DEFAULT_ROLE=ROLE_MARKETING_ANALYST 
  DEFAULT_WAREHOUSE=COMPLIANCE_WH
  MUST_CHANGE_PASSWORD=FALSE;

CREATE USER IF NOT EXISTS user_regional_mgr 
  PASSWORD='ChangeIt123!' 
  DEFAULT_ROLE=ROLE_REGIONAL_MANAGER 
  DEFAULT_WAREHOUSE=COMPLIANCE_WH
  MUST_CHANGE_PASSWORD=FALSE;

CREATE USER IF NOT EXISTS user_privileged 
  PASSWORD='ChangeIt123!' 
  DEFAULT_ROLE=ROLE_DATA_PRIVILEGED 
  DEFAULT_WAREHOUSE=COMPLIANCE_WH
  MUST_CHANGE_PASSWORD=FALSE;

CREATE USER IF NOT EXISTS falhari_tester 
  PASSWORD='ChangeIt123!' 
  DEFAULT_ROLE=ROLE_DATA_PRIVILEGED 
  DEFAULT_WAREHOUSE=COMPLIANCE_WH
  MUST_CHANGE_PASSWORD=FALSE;

-- Assign roles to tester user (give your username in place of LEARNINGJOURNEY) so you can switch roles in UI
GRANT ROLE ROLE_STORE_MANAGER     TO USER LEARNINGJOURNEY;
GRANT ROLE ROLE_MARKETING_ANALYST TO USER LEARNINGJOURNEY;
GRANT ROLE ROLE_REGIONAL_MANAGER  TO USER LEARNINGJOURNEY;
GRANT ROLE ROLE_DATA_PRIVILEGED   TO USER LEARNINGJOURNEY;

-- Database & schema usage
GRANT USAGE ON DATABASE COMPLIANCE_DB TO ROLE ROLE_STORE_MANAGER;
GRANT USAGE ON DATABASE COMPLIANCE_DB TO ROLE ROLE_MARKETING_ANALYST;
GRANT USAGE ON DATABASE COMPLIANCE_DB TO ROLE ROLE_REGIONAL_MANAGER;
GRANT USAGE ON DATABASE COMPLIANCE_DB TO ROLE ROLE_DATA_PRIVILEGED;
GRANT USAGE ON DATABASE COMPLIANCE_DB TO ROLE ROLE_DATA_LOADER;

GRANT USAGE ON SCHEMA COMPLIANCE_DB.PUBLIC TO ROLE ROLE_STORE_MANAGER;
GRANT USAGE ON SCHEMA COMPLIANCE_DB.PUBLIC TO ROLE ROLE_MARKETING_ANALYST;
GRANT USAGE ON SCHEMA COMPLIANCE_DB.PUBLIC TO ROLE ROLE_REGIONAL_MANAGER;
GRANT USAGE ON SCHEMA COMPLIANCE_DB.PUBLIC TO ROLE ROLE_DATA_PRIVILEGED;
GRANT USAGE ON SCHEMA COMPLIANCE_DB.PUBLIC TO ROLE ROLE_DATA_LOADER;

-- Warehouse usage
GRANT USAGE ON WAREHOUSE COMPLIANCE_WH TO ROLE ROLE_STORE_MANAGER;
GRANT USAGE ON WAREHOUSE COMPLIANCE_WH TO ROLE ROLE_MARKETING_ANALYST;
GRANT USAGE ON WAREHOUSE COMPLIANCE_WH TO ROLE ROLE_REGIONAL_MANAGER;
GRANT USAGE ON WAREHOUSE COMPLIANCE_WH TO ROLE ROLE_DATA_PRIVILEGED;
GRANT USAGE ON WAREHOUSE COMPLIANCE_WH TO ROLE ROLE_DATA_LOADER;

-- Loader role: stage & file format
GRANT USAGE ON FILE FORMAT CSV_FORMAT TO ROLE ROLE_DATA_LOADER;
GRANT READ, WRITE ON STAGE falhari_stage TO ROLE ROLE_DATA_LOADER;
GRANT INSERT, SELECT ON ALL TABLES IN SCHEMA COMPLIANCE_DB.PUBLIC TO ROLE ROLE_DATA_LOADER;

-- Table-level access (RBAC)
GRANT SELECT ON ALL TABLES IN SCHEMA COMPLIANCE_DB.PUBLIC TO ROLE ROLE_DATA_PRIVILEGED;

-- Marketing analyst: can query all tables but PII masked
GRANT SELECT ON ALL TABLES IN SCHEMA COMPLIANCE_DB.PUBLIC TO ROLE ROLE_MARKETING_ANALYST;

-- Store manager: only business-critical tables
GRANT SELECT ON TABLE FCT_SALES  TO ROLE ROLE_STORE_MANAGER;
GRANT SELECT ON TABLE STORES     TO ROLE ROLE_STORE_MANAGER;
GRANT SELECT ON TABLE PRODUCTS   TO ROLE ROLE_STORE_MANAGER;

-- Regional manager: similar to store manager, but row policy will filter on REGION
GRANT SELECT ON TABLE FCT_SALES  TO ROLE ROLE_REGIONAL_MANAGER;
GRANT SELECT ON TABLE STORES     TO ROLE ROLE_REGIONAL_MANAGER;
GRANT SELECT ON TABLE PRODUCTS   TO ROLE ROLE_REGIONAL_MANAGER;

-- Credit card demo table grants
GRANT SELECT ON TABLE CREDIT_CARD_CUSTOMERS TO ROLE ROLE_DATA_PRIVILEGED;
GRANT SELECT ON TABLE CREDIT_CARD_CUSTOMERS TO ROLE ROLE_STORE_MANAGER;
GRANT SELECT ON TABLE CREDIT_CARD_CUSTOMERS TO ROLE ROLE_MARKETING_ANALYST;
GRANT SELECT ON TABLE CREDIT_CARD_CUSTOMERS TO ROLE ROLE_REGIONAL_MANAGER;

--------------------------------------------------------------
-- 6️⃣  DYNAMIC DATA MASKING (Column-level Security / PII)
--     Requirement (b): Mask email/phone for MARKETING_ANALYST
--     but show to privileged roles (DATA_PRIVILEGED, admins).
--------------------------------------------------------------
USE DATABASE COMPLIANCE_DB;
USE SCHEMA PUBLIC;

CREATE OR REPLACE MASKING POLICY mask_email_policy
  AS (val STRING) RETURNS STRING ->
    CASE
      WHEN CURRENT_ROLE() IN ('ROLE_DATA_PRIVILEGED','SYSADMIN','ACCOUNTADMIN') THEN val
      WHEN CURRENT_ROLE() IN ('ROLE_REGIONAL_MANAGER','ROLE_STORE_MANAGER') THEN CONCAT(SPLIT_PART(val,'@',1),'@****')
      WHEN CURRENT_ROLE() IN ('ROLE_MARKETING_ANALYST') THEN CONCAT('****@', SPLIT_PART(val,'@',2))
      ELSE '***MASKED***'
    END;

CREATE OR REPLACE MASKING POLICY mask_phone_policy
  AS (val STRING) RETURNS STRING ->
    CASE
      WHEN CURRENT_ROLE() IN ('ROLE_DATA_PRIVILEGED','SYSADMIN','ACCOUNTADMIN') THEN val
      WHEN CURRENT_ROLE() IN ('ROLE_REGIONAL_MANAGER','ROLE_STORE_MANAGER') THEN CONCAT('*****',RIGHT(val,5))
      WHEN CURRENT_ROLE() IN ('ROLE_MARKETING_ANALYST') THEN '**********'
      ELSE '***MASKED***'
    END;

ALTER TABLE CUSTOMERS MODIFY COLUMN EMAIL SET MASKING POLICY mask_email_policy;
ALTER TABLE CUSTOMERS MODIFY COLUMN PHONE SET MASKING POLICY mask_phone_policy;

-- Credit card number masking 
CREATE OR REPLACE MASKING POLICY mask_credit_card_policy
  AS (val STRING) RETURNS STRING ->
    CASE
      WHEN CURRENT_ROLE() IN ('ROLE_DATA_PRIVILEGED','SYSADMIN','ACCOUNTADMIN') THEN val
      WHEN CURRENT_ROLE() IN ('ROLE_REGIONAL_MANAGER','ROLE_STORE_MANAGER') THEN 
           REGEXP_REPLACE(val, '^.{12}', '************')
      WHEN CURRENT_ROLE() IN ('ROLE_MARKETING_ANALYST') THEN '****MASKED****'
      ELSE '*** NOT AUTHORIZED ***'
    END;

ALTER TABLE CREDIT_CARD_CUSTOMERS 
  MODIFY COLUMN CREDIT_CARD_NUMBER 
  SET MASKING POLICY mask_credit_card_policy;

--------------------------------------------------------------
-- 7️⃣  ROW-LEVEL SECURITY (Row Access Policy on FCT_SALES)
--     Requirement (c): STORE_MANAGER can only see sales for their STORE_ID.
--     ABAC-style logic: based on EMPLOYEES.USERNAME, ROLE_NAME, REGION.
--------------------------------------------------------------
CREATE OR REPLACE ROW ACCESS POLICY store_row_policy
  AS (r_store_id STRING) RETURNS BOOLEAN ->
    CASE
      -- Fully privileged roles see all rows
      WHEN CURRENT_ROLE() IN ('ROLE_DATA_PRIVILEGED','SYSADMIN','ACCOUNTADMIN') THEN TRUE

      -- Regional Manager: can see rows for stores in their REGION (ABAC)
      WHEN EXISTS (
        SELECT 1 
        FROM EMPLOYEES e
        JOIN STORES s ON s.STORE_ID = r_store_id
        WHERE e.USERNAME = CURRENT_USER()
          AND e.ROLE_NAME = 'REGIONAL_MANAGER'
          AND e.REGION = s.REGION
      ) THEN TRUE

      -- Store Manager: can see only their own STORE_ID
      WHEN EXISTS (
        SELECT 1 
        FROM EMPLOYEES e
        WHERE e.USERNAME = CURRENT_USER()
          AND e.ROLE_NAME = 'STORE_MANAGER'
          AND e.STORE_ID = r_store_id
      ) THEN TRUE

      ELSE FALSE
    END;

ALTER TABLE FCT_SALES 
  ADD ROW ACCESS POLICY store_row_policy ON (STORE_ID);

--------------------------------------------------------------
-- 8️⃣  OPTIONAL: MAP LAB USERS TO EMPLOYEES TABLE
--     (So row access works when you log in with those users.)
--------------------------------------------------------------
-- You can insert rows into EMPLOYEES mapping CURRENT_USERs:
-- Example:
-- INSERT INTO EMPLOYEES (EMPLOYEE_ID, EMPLOYEE_NAME, USERNAME, ROLE_NAME, STORE_ID, REGION, EMAIL, PHONE, JOINED_AT)
-- VALUES ('E100','Store Manager Delhi','USER_STORE_MGR','STORE_MANAGER','S001','NORTH',
--         'sm.delhi@falhari.in','+911140009999','2021-01-01');


--------------------------------------------------------------
-- 9️⃣  VALIDATION QUERIES (Hands-on checks)
--------------------------------------------------------------
-- Log in as different roles (or use falhari_tester and SET ROLE).

SELECT CURRENT_USER(), CURRENT_ROLE();
SHOW GRANTS TO USER LEARNINGJOURNEY;



-- 9.1 Privileged user: full PII, full sales
USE ROLE ROLE_DATA_PRIVILEGED;
USE WAREHOUSE COMPLIANCE_WH;
SELECT CURRENT_ROLE(), CURRENT_USER();

SELECT CUSTOMER_ID, CUSTOMER_NAME, EMAIL, PHONE 
FROM CUSTOMERS 
LIMIT 5;

SELECT COUNT(*) AS TOTAL_SALES_ROWS 
FROM FCT_SALES;

SELECT CUST_ID, CREDIT_CARD_NUMBER 
FROM CREDIT_CARD_CUSTOMERS
LIMIT 5;

-- 9.2 Masked PII for Marketing Analyst
USE ROLE ROLE_MARKETING_ANALYST;
SELECT CUSTOMER_ID, CUSTOMER_NAME, EMAIL, PHONE 
FROM CUSTOMERS 
LIMIT 5;

SELECT CUST_ID, CREDIT_CARD_NUMBER 
FROM CREDIT_CARD_CUSTOMERS
LIMIT 5;

-- 9.3 Store-level access (STORE_MANAGER)
USE ROLE ROLE_STORE_MANAGER;
SELECT DISTINCT STORE_ID FROM FCT_SALES;   -- Should show only that manager's store (based on EMPLOYEES)

-- 9.4 Region-restricted access (REGIONAL_MANAGER)
USE ROLE ROLE_REGIONAL_MANAGER;
SELECT DISTINCT s.REGION
FROM STORES s 
JOIN FCT_SALES f ON s.STORE_ID = f.STORE_ID;




-- 9.5 Clean-up examples (optional)
-- ALTER TABLE CUSTOMERS MODIFY COLUMN EMAIL UNSET MASKING POLICY;
-- ALTER TABLE CUSTOMERS MODIFY COLUMN PHONE UNSET MASKING POLICY;
-- ALTER TABLE CREDIT_CARD_CUSTOMERS MODIFY COLUMN CREDIT_CARD_NUMBER UNSET MASKING POLICY;
-- DROP ROW ACCESS POLICY store_row_policy;
-- DROP MASKING POLICY mask_email_policy;
-- DROP MASKING POLICY mask_phone_policy;
-- DROP MASKING POLICY mask_credit_card_policy;

-- ==========================================================
-- ✅ END OF MASTER SCRIPT
-- ==========================================================


-- ==========================================================
-- 🔧 EMPLOYEE MAPPING BLOCK — enables ABAC for test users
-- ==========================================================

USE ROLE ROLE_DATA_PRIVILEGED;
USE DATABASE COMPLIANCE_DB;
USE SCHEMA PUBLIC;

--------------------------------------------------------------
-- Step 1️⃣: Get a sample store & region (for assigning mappings)
--------------------------------------------------------------
-- You can adjust if you want specific stores per user
SELECT STORE_ID, REGION FROM STORES LIMIT 5;

-- Assume:
--   S001 → REGION 'NORTH'
--   S002 → REGION 'SOUTH'
-- (Update below values if your data differs)

--------------------------------------------------------------
-- Step 2️⃣: Insert EMPLOYEE mappings for demo/test users
--------------------------------------------------------------
INSERT INTO EMPLOYEES (EMPLOYEE_ID, EMPLOYEE_NAME, USERNAME, ROLE_NAME, STORE_ID, REGION, EMAIL, PHONE, JOINED_AT)
SELECT 
  'E90001', 'Falhari Store Manager', 'USER_STORE_MGR', 'STORE_MANAGER', 'S001', 'NORTH', 
  'user_store_mgr@falhari.in', '+919811111111', '2022-01-01'
WHERE NOT EXISTS (SELECT 1 FROM EMPLOYEES WHERE USERNAME='USER_STORE_MGR');

INSERT INTO EMPLOYEES (EMPLOYEE_ID, EMPLOYEE_NAME, USERNAME, ROLE_NAME, STORE_ID, REGION, EMAIL, PHONE, JOINED_AT)
SELECT 
  'E90002', 'Falhari Regional Manager', 'USER_REGIONAL_MGR', 'REGIONAL_MANAGER', 'S001', 'NORTH',
  'user_regional_mgr@falhari.in', '+919822222222', '2021-12-01'
WHERE NOT EXISTS (SELECT 1 FROM EMPLOYEES WHERE USERNAME='USER_REGIONAL_MGR');

INSERT INTO EMPLOYEES (EMPLOYEE_ID, EMPLOYEE_NAME, USERNAME, ROLE_NAME, STORE_ID, REGION, EMAIL, PHONE, JOINED_AT)
SELECT 
  'E90003', 'Falhari Data Privileged', 'USER_PRIVILEGED', 'DATA_PRIVILEGED', 'S002', 'SOUTH',
  'user_privileged@falhari.in', '+919833333333', '2020-06-15'
WHERE NOT EXISTS (SELECT 1 FROM EMPLOYEES WHERE USERNAME='USER_PRIVILEGED');

INSERT INTO EMPLOYEES (EMPLOYEE_ID, EMPLOYEE_NAME, USERNAME, ROLE_NAME, STORE_ID, REGION, EMAIL, PHONE, JOINED_AT)
SELECT 
  'E90004', 'Falhari Tester Regional', 'FALHARI_TESTER', 'REGIONAL_MANAGER', 'S002', 'SOUTH',
  'falhari_tester@falhari.in', '+919844444444', '2023-04-01'
WHERE NOT EXISTS (SELECT 1 FROM EMPLOYEES WHERE USERNAME='FALHARI_TESTER');

--------------------------------------------------------------
-- Step 3️⃣: Map your main Snowflake user 
--------------------------------------------------------------
INSERT INTO EMPLOYEES (EMPLOYEE_ID, EMPLOYEE_NAME, USERNAME, ROLE_NAME, STORE_ID, REGION, EMAIL, PHONE, JOINED_AT)
SELECT 
  'E99999', 'My Main Account', 'LEARNINGJOURNEY', 'STORE_MANAGER', 'S001', 'NORTH',
  'my@falhari.in', '+919855555555', '2022-05-10'
WHERE NOT EXISTS (SELECT 1 FROM EMPLOYEES WHERE USERNAME='LEARNINGJOURNEY');


SHOW USERS;


--------------------------------------------------------------
-- Step 4️⃣: Verify mappings
--------------------------------------------------------------
SELECT USERNAME, ROLE_NAME, STORE_ID, REGION
FROM EMPLOYEES
WHERE USERNAME IN ('USER_STORE_MGR','USER_REGIONAL_MGR','USER_PRIVILEGED','FALHARI_TESTER','LEARNINGJOURNEY')
ORDER BY USERNAME;

-- ==========================================================
-- ✅ DONE — ABAC (Row Access Policy) is now fully functional
-- ==========================================================





