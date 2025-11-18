-- ====================================================================================
-- PaintCo — Production-ready CDC ELT Pipeline (single-file deploy)
-- Warehouse used for tasks & dynamic tables: COMPUTE_WH
-- ====================================================================================

-- 0. Safety: use a role/user with sufficient rights to create DB/objects.
-- Run as ACCOUNTADMIN or similarly privileged dev role for initial deploy.

----------------------------------------
-- 1) Create database + schemas + file format + stages
----------------------------------------
CREATE OR REPLACE DATABASE PAINTCO_DB;
USE DATABASE PAINTCO_DB;

CREATE OR REPLACE SCHEMA STG;
CREATE OR REPLACE SCHEMA RAW;
CREATE OR REPLACE SCHEMA CORE;
CREATE OR REPLACE SCHEMA MONITORING;
CREATE OR REPLACE SCHEMA PUBLIC; -- ensure public exists for file format location (optional)

-- File format (fully-qualified name used later)
CREATE OR REPLACE FILE FORMAT PAINTCO_DB.PUBLIC.PAINTCO_CSV_FORMAT
  TYPE = 'CSV'
  FIELD_DELIMITER = ','
  SKIP_HEADER = 1
  NULL_IF = ('', 'NULL')
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  TRIM_SPACE = TRUE;

-- Internal stages (one per object)
CREATE OR REPLACE STAGE STG.CUSTOMERS_STAGE FILE_FORMAT = PAINTCO_DB.PUBLIC.PAINTCO_CSV_FORMAT;
CREATE OR REPLACE STAGE STG.PRODUCTS_STAGE  FILE_FORMAT = PAINTCO_DB.PUBLIC.PAINTCO_CSV_FORMAT;
CREATE OR REPLACE STAGE STG.STORES_STAGE    FILE_FORMAT = PAINTCO_DB.PUBLIC.PAINTCO_CSV_FORMAT;
CREATE OR REPLACE STAGE STG.SUPPLIERS_STAGE FILE_FORMAT = PAINTCO_DB.PUBLIC.PAINTCO_CSV_FORMAT;
CREATE OR REPLACE STAGE STG.DISTRIBUTORS_STAGE FILE_FORMAT = PAINTCO_DB.PUBLIC.PAINTCO_CSV_FORMAT;
CREATE OR REPLACE STAGE STG.DEALERS_STAGE FILE_FORMAT = PAINTCO_DB.PUBLIC.PAINTCO_CSV_FORMAT;
CREATE OR REPLACE STAGE STG.BRANDSTORES_STAGE FILE_FORMAT = PAINTCO_DB.PUBLIC.PAINTCO_CSV_FORMAT;
CREATE OR REPLACE STAGE STG.LOCALSHOPS_STAGE FILE_FORMAT = PAINTCO_DB.PUBLIC.PAINTCO_CSV_FORMAT;
CREATE OR REPLACE STAGE STG.INDUSTRIAL_CLIENTS_STAGE FILE_FORMAT = PAINTCO_DB.PUBLIC.PAINTCO_CSV_FORMAT;
CREATE OR REPLACE STAGE STG.PROJECTS_STAGE FILE_FORMAT = PAINTCO_DB.PUBLIC.PAINTCO_CSV_FORMAT;
CREATE OR REPLACE STAGE STG.INVENTORY_STAGE FILE_FORMAT = PAINTCO_DB.PUBLIC.PAINTCO_CSV_FORMAT;
CREATE OR REPLACE STAGE STG.PURCHASE_ORDERS_STAGE FILE_FORMAT = PAINTCO_DB.PUBLIC.PAINTCO_CSV_FORMAT;
CREATE OR REPLACE STAGE STG.SHIPMENTS_STAGE FILE_FORMAT = PAINTCO_DB.PUBLIC.PAINTCO_CSV_FORMAT;
CREATE OR REPLACE STAGE STG.PROMOTIONS_STAGE FILE_FORMAT = PAINTCO_DB.PUBLIC.PAINTCO_CSV_FORMAT;
CREATE OR REPLACE STAGE STG.SALES_STAGE FILE_FORMAT = PAINTCO_DB.PUBLIC.PAINTCO_CSV_FORMAT;

----------------------------------------
-- 2) Create RAW landing tables
----------------------------------------
USE SCHEMA RAW;

CREATE OR REPLACE TABLE RAW.CUSTOMERS_RAW (
  file_id STRING,
  customer_id STRING,
  customer_name STRING,
  customer_type STRING,
  email STRING,
  phone STRING,
  city STRING,
  state STRING,
  pin_code STRING,
  division STRING,
  created_at TIMESTAMP_LTZ
);

CREATE OR REPLACE TABLE RAW.PRODUCTS_RAW (
  file_id STRING,
  product_id STRING,
  sku STRING,
  product_name STRING,
  brand STRING,
  category STRING,
  sub_category STRING,
  unit_price NUMBER(10,2),
  uom STRING,
  pack_size STRING,
  created_at TIMESTAMP_LTZ
);

CREATE OR REPLACE TABLE RAW.STORES_RAW (
  file_id STRING,
  store_id STRING,
  store_name STRING,
  store_type STRING,
  city STRING,
  state STRING,
  region STRING,
  created_at TIMESTAMP_LTZ
);

CREATE OR REPLACE TABLE RAW.SUPPLIERS_RAW (
  file_id STRING,
  supplier_id STRING,
  supplier_name STRING,
  contact_email STRING,
  city STRING,
  state STRING,
  created_at TIMESTAMP_LTZ
);

CREATE OR REPLACE TABLE RAW.DISTRIBUTORS_RAW (
  file_id STRING,
  distributor_id STRING,
  distributor_name STRING,
  city STRING,
  state STRING,
  tier STRING,
  created_at TIMESTAMP_LTZ
);

CREATE OR REPLACE TABLE RAW.DEALERS_RAW (
  file_id STRING,
  dealer_id STRING,
  dealer_name STRING,
  city STRING,
  state STRING,
  created_at TIMESTAMP_LTZ
);

CREATE OR REPLACE TABLE RAW.BRANDSTORES_RAW (
  file_id STRING,
  brand_store_id STRING,
  name STRING,
  city STRING,
  state STRING,
  created_at TIMESTAMP_LTZ
);

CREATE OR REPLACE TABLE RAW.LOCALSHOPS_RAW (
  file_id STRING,
  shop_id STRING,
  shop_name STRING,
  owner_name STRING,
  city STRING,
  state STRING,
  created_at TIMESTAMP_LTZ
);

CREATE OR REPLACE TABLE RAW.INDUSTRIAL_CLIENTS_RAW (
  file_id STRING,
  client_id STRING,
  client_name STRING,
  industry_segment STRING,
  contact_person STRING,
  contact_email STRING,
  city STRING,
  state STRING,
  created_at TIMESTAMP_LTZ
);

CREATE OR REPLACE TABLE RAW.PROJECTS_RAW (
  file_id STRING,
  project_id STRING,
  project_name STRING,
  client_id STRING,
  start_date TIMESTAMP_LTZ,
  end_date TIMESTAMP_LTZ,
  city STRING,
  state STRING,
  status STRING,
  created_at TIMESTAMP_LTZ
);

CREATE OR REPLACE TABLE RAW.INVENTORY_RAW (
  file_id STRING,
  product_id STRING,
  location_id STRING,
  location_type STRING,
  quantity NUMBER,
  last_updated TIMESTAMP_LTZ
);

CREATE OR REPLACE TABLE RAW.SALES_RAW (
  file_id STRING,
  sale_id STRING,
  customer_id STRING,
  customer_type STRING,
  product_id STRING,
  store_id STRING,
  distributor_id STRING,
  dealer_id STRING,
  brand_store_id STRING,
  supplier_id STRING,
  division STRING,
  quantity NUMBER,
  sale_amount NUMBER(12,2),
  discount_amount NUMBER(12,2),
  sale_ts TIMESTAMP_LTZ,
  channel STRING
);

CREATE OR REPLACE TABLE RAW.PURCHASE_ORDERS_RAW (
  file_id STRING,
  po_id STRING,
  supplier_id STRING,
  product_id STRING,
  qty NUMBER,
  price NUMBER(12,2),
  po_ts TIMESTAMP_LTZ
);

CREATE OR REPLACE TABLE RAW.SHIPMENTS_RAW (
  file_id STRING,
  shipment_id STRING,
  po_id STRING,
  carrier STRING,
  tracking_id STRING,
  shipped_ts TIMESTAMP_LTZ,
  delivered_ts TIMESTAMP_LTZ
);

CREATE OR REPLACE TABLE RAW.PROMOTIONS_RAW (
  file_id STRING,
  promo_id STRING,
  promo_name STRING,
  start_ts TIMESTAMP_LTZ,
  end_ts TIMESTAMP_LTZ,
  discount_pct NUMBER
);

----------------------------------------
-- 3) Create CORE curated tables
----------------------------------------
USE SCHEMA CORE;

CREATE OR REPLACE TABLE CORE.CUSTOMERS (
  customer_id STRING PRIMARY KEY,
  customer_name STRING,
  customer_type STRING,
  email STRING,
  phone STRING,
  city STRING,
  state STRING,
  pin_code STRING,
  division STRING,
  created_at TIMESTAMP_LTZ
);

CREATE OR REPLACE TABLE CORE.PRODUCTS (
  product_id STRING PRIMARY KEY,
  sku STRING,
  product_name STRING,
  brand STRING,
  category STRING,
  sub_category STRING,
  unit_price NUMBER(10,2),
  uom STRING,
  pack_size STRING,
  created_at TIMESTAMP_LTZ
);

CREATE OR REPLACE TABLE CORE.STORES (
  store_id STRING PRIMARY KEY,
  store_name STRING,
  store_type STRING,
  city STRING,
  state STRING,
  region STRING,
  created_at TIMESTAMP_LTZ
);

CREATE OR REPLACE TABLE CORE.SUPPLIERS (
  supplier_id STRING PRIMARY KEY,
  supplier_name STRING,
  contact_email STRING,
  city STRING,
  state STRING,
  created_at TIMESTAMP_LTZ
);

CREATE OR REPLACE TABLE CORE.DISTRIBUTORS (
  distributor_id STRING PRIMARY KEY,
  distributor_name STRING,
  city STRING,
  state STRING,
  tier STRING,
  created_at TIMESTAMP_LTZ
);

CREATE OR REPLACE TABLE CORE.DEALERS (
  dealer_id STRING PRIMARY KEY,
  dealer_name STRING,
  city STRING,
  state STRING,
  created_at TIMESTAMP_LTZ
);

CREATE OR REPLACE TABLE CORE.BRAND_STORES (
  brand_store_id STRING PRIMARY KEY,
  name STRING,
  city STRING,
  state STRING,
  created_at TIMESTAMP_LTZ
);

CREATE OR REPLACE TABLE CORE.LOCAL_SHOPS (
  shop_id STRING PRIMARY KEY,
  shop_name STRING,
  owner_name STRING,
  city STRING,
  state STRING,
  created_at TIMESTAMP_LTZ
);

CREATE OR REPLACE TABLE CORE.INDUSTRIAL_CLIENTS (
  client_id STRING PRIMARY KEY,
  client_name STRING,
  industry_segment STRING,
  contact_person STRING,
  contact_email STRING,
  city STRING,
  state STRING,
  created_at TIMESTAMP_LTZ
);

CREATE OR REPLACE TABLE CORE.PROJECTS (
  project_id STRING PRIMARY KEY,
  project_name STRING,
  client_id STRING,
  start_date TIMESTAMP_LTZ,
  end_date TIMESTAMP_LTZ,
  city STRING,
  state STRING,
  status STRING,
  created_at TIMESTAMP_LTZ
);

CREATE OR REPLACE TABLE CORE.INVENTORY (
  product_id STRING,
  location_id STRING,
  location_type STRING,
  quantity NUMBER,
  last_updated TIMESTAMP_LTZ
);

CREATE OR REPLACE TABLE CORE.SALES (
  sale_id STRING PRIMARY KEY,
  customer_id STRING,
  customer_type STRING,
  product_id STRING,
  store_id STRING,
  distributor_id STRING,
  dealer_id STRING,
  brand_store_id STRING,
  supplier_id STRING,
  division STRING,
  quantity NUMBER,
  sale_amount NUMBER(12,2),
  discount_amount NUMBER(12,2),
  sale_ts TIMESTAMP_LTZ,
  channel STRING
);

CREATE OR REPLACE TABLE CORE.PURCHASE_ORDERS (
  po_id STRING PRIMARY KEY,
  supplier_id STRING,
  product_id STRING,
  qty NUMBER,
  price NUMBER(12,2),
  po_ts TIMESTAMP_LTZ
);

CREATE OR REPLACE TABLE CORE.SHIPMENTS (
  shipment_id STRING PRIMARY KEY,
  po_id STRING,
  carrier STRING,
  tracking_id STRING,
  shipped_ts TIMESTAMP_LTZ,
  delivered_ts TIMESTAMP_LTZ
);

CREATE OR REPLACE TABLE CORE.PROMOTIONS (
  promo_id STRING PRIMARY KEY,
  promo_name STRING,
  start_ts TIMESTAMP_LTZ,
  end_ts TIMESTAMP_LTZ,
  discount_pct NUMBER
);

----------------------------------------
-- 4) Monitoring tables
----------------------------------------
USE SCHEMA MONITORING;

CREATE OR REPLACE TABLE MONITORING.PROCESSED_FILES (
  file_name STRING PRIMARY KEY,
  stage_name STRING,
  file_hash STRING,
  file_size NUMBER,
  loaded_ts TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP,
  notes STRING
);

CREATE OR REPLACE TABLE MONITORING.PIPE_LOG (
  log_ts TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP,
  source_stage STRING,
  source_file STRING,
  target_table STRING,
  action STRING,
  rows_loaded NUMBER,
  notes STRING
);

CREATE OR REPLACE TABLE MONITORING.ALERT_QUEUE (
  alert_ts TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP,
  stage_name STRING,
  file_name STRING,
  error_message STRING,
  processed BOOLEAN DEFAULT FALSE
);

CREATE OR REPLACE TABLE MONITORING.COPY_LOAD_ERRORS (
  stage_name STRING,
  file_name STRING,
  error_line STRING,
  error_message STRING,
  raw_json STRING,
  recorded_ts TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP
);

----------------------------------------
-- 5) Streams on RAW tables
----------------------------------------
USE SCHEMA RAW;

CREATE OR REPLACE STREAM RAW.CUSTOMERS_RAW_STREAM ON TABLE RAW.CUSTOMERS_RAW APPEND_ONLY = FALSE;
CREATE OR REPLACE STREAM RAW.PRODUCTS_RAW_STREAM ON TABLE RAW.PRODUCTS_RAW APPEND_ONLY = FALSE;
CREATE OR REPLACE STREAM RAW.STORES_RAW_STREAM ON TABLE RAW.STORES_RAW APPEND_ONLY = FALSE;
CREATE OR REPLACE STREAM RAW.SUPPLIERS_RAW_STREAM ON TABLE RAW.SUPPLIERS_RAW APPEND_ONLY = FALSE;
CREATE OR REPLACE STREAM RAW.DISTRIBUTORS_RAW_STREAM ON TABLE RAW.DISTRIBUTORS_RAW APPEND_ONLY = FALSE;
CREATE OR REPLACE STREAM RAW.DEALERS_RAW_STREAM ON TABLE RAW.DEALERS_RAW APPEND_ONLY = FALSE;
CREATE OR REPLACE STREAM RAW.BRANDSTORES_RAW_STREAM ON TABLE RAW.BRANDSTORES_RAW APPEND_ONLY = FALSE;
CREATE OR REPLACE STREAM RAW.LOCALSHOPS_RAW_STREAM ON TABLE RAW.LOCALSHOPS_RAW APPEND_ONLY = FALSE;
CREATE OR REPLACE STREAM RAW.INDUSTRIAL_CLIENTS_RAW_STREAM ON TABLE RAW.INDUSTRIAL_CLIENTS_RAW APPEND_ONLY = FALSE;
CREATE OR REPLACE STREAM RAW.PROJECTS_RAW_STREAM ON TABLE RAW.PROJECTS_RAW APPEND_ONLY = FALSE;
CREATE OR REPLACE STREAM RAW.INVENTORY_RAW_STREAM ON TABLE RAW.INVENTORY_RAW APPEND_ONLY = FALSE;
CREATE OR REPLACE STREAM RAW.SALES_RAW_STREAM ON TABLE RAW.SALES_RAW APPEND_ONLY = FALSE;
CREATE OR REPLACE STREAM RAW.PURCHASE_ORDERS_RAW_STREAM ON TABLE RAW.PURCHASE_ORDERS_RAW APPEND_ONLY = FALSE;
CREATE OR REPLACE STREAM RAW.SHIPMENTS_RAW_STREAM ON TABLE RAW.SHIPMENTS_RAW APPEND_ONLY = FALSE;
CREATE OR REPLACE STREAM RAW.PROMOTIONS_RAW_STREAM ON TABLE RAW.PROMOTIONS_RAW APPEND_ONLY = FALSE;

----------------------------------------
-- 6) Stream-based MERGE procedures (one per object)
-- Each proc reads the corresponding RAW_*_STREAM and applies INSERT/UPDATE/DELETE to CORE.*
----------------------------------------
USE SCHEMA RAW;

-- Format: MERGE into CORE.<TABLE> using RAW.<TABLE>_STREAM where METADATA$ACTION indicates 'INSERT','UPDATE','DELETE'
-- Customers
CREATE OR REPLACE PROCEDURE RAW.MERGE_CUSTOMERS_STREAM()
RETURNS STRING
LANGUAGE SQL
AS
$$
MERGE INTO CORE.CUSTOMERS tgt
USING (SELECT *, METADATA$ACTION AS __ACTION FROM RAW.CUSTOMERS_RAW_STREAM) src
ON tgt.customer_id = src.customer_id
WHEN MATCHED AND src.__ACTION = 'DELETE' THEN DELETE
WHEN MATCHED THEN UPDATE SET
  customer_name = src.customer_name,
  customer_type = src.customer_type,
  email = src.email,
  phone = src.phone,
  city = src.city,
  state = src.state,
  pin_code = src.pin_code,
  division = src.division,
  created_at = src.created_at
WHEN NOT MATCHED AND src.__ACTION != 'DELETE' THEN INSERT (customer_id, customer_name, customer_type, email, phone, city, state, pin_code, division, created_at)
VALUES (src.customer_id, src.customer_name, src.customer_type, src.email, src.phone, src.city, src.state, src.pin_code, src.division, src.created_at);
$$;

-- Products
CREATE OR REPLACE PROCEDURE RAW.MERGE_PRODUCTS_STREAM()
RETURNS STRING
LANGUAGE SQL
AS
$$
MERGE INTO CORE.PRODUCTS tgt
USING (SELECT *, METADATA$ACTION AS __ACTION FROM RAW.PRODUCTS_RAW_STREAM) src
ON tgt.product_id = src.product_id
WHEN MATCHED AND src.__ACTION = 'DELETE' THEN DELETE
WHEN MATCHED THEN UPDATE SET
  sku = src.sku,
  product_name = src.product_name,
  brand = src.brand,
  category = src.category,
  sub_category = src.sub_category,
  unit_price = src.unit_price,
  uom = src.uom,
  pack_size = src.pack_size,
  created_at = src.created_at
WHEN NOT MATCHED AND src.__ACTION != 'DELETE' THEN INSERT (product_id, sku, product_name, brand, category, sub_category, unit_price, uom, pack_size, created_at)
VALUES (src.product_id, src.sku, src.product_name, src.brand, src.category, src.sub_category, src.unit_price, src.uom, src.pack_size, src.created_at);
$$;

-- Stores
CREATE OR REPLACE PROCEDURE RAW.MERGE_STORES_STREAM()
RETURNS STRING
LANGUAGE SQL
AS
$$
MERGE INTO CORE.STORES tgt
USING (SELECT *, METADATA$ACTION AS __ACTION FROM RAW.STORES_RAW_STREAM) src
ON tgt.store_id = src.store_id
WHEN MATCHED AND src.__ACTION = 'DELETE' THEN DELETE
WHEN MATCHED THEN UPDATE SET
  store_name = src.store_name,
  store_type = src.store_type,
  city = src.city,
  state = src.state,
  region = src.region,
  created_at = src.created_at
WHEN NOT MATCHED AND src.__ACTION != 'DELETE' THEN INSERT (store_id, store_name, store_type, city, state, region, created_at)
VALUES (src.store_id, src.store_name, src.store_type, src.city, src.state, src.region, src.created_at);
$$;

-- Suppliers
CREATE OR REPLACE PROCEDURE RAW.MERGE_SUPPLIERS_STREAM()
RETURNS STRING
LANGUAGE SQL
AS
$$
MERGE INTO CORE.SUPPLIERS tgt
USING (SELECT *, METADATA$ACTION AS __ACTION FROM RAW.SUPPLIERS_RAW_STREAM) src
ON tgt.supplier_id = src.supplier_id
WHEN MATCHED AND src.__ACTION = 'DELETE' THEN DELETE
WHEN MATCHED THEN UPDATE SET
  supplier_name = src.supplier_name,
  contact_email = src.contact_email,
  city = src.city,
  state = src.state,
  created_at = src.created_at
WHEN NOT MATCHED AND src.__ACTION != 'DELETE' THEN INSERT (supplier_id, supplier_name, contact_email, city, state, created_at)
VALUES (src.supplier_id, src.supplier_name, src.contact_email, src.city, src.state, src.created_at);
$$;

-- Distributors
CREATE OR REPLACE PROCEDURE RAW.MERGE_DISTRIBUTORS_STREAM()
RETURNS STRING
LANGUAGE SQL
AS
$$
MERGE INTO CORE.DISTRIBUTORS tgt
USING (SELECT *, METADATA$ACTION AS __ACTION FROM RAW.DISTRIBUTORS_RAW_STREAM) src
ON tgt.distributor_id = src.distributor_id
WHEN MATCHED AND src.__ACTION = 'DELETE' THEN DELETE
WHEN MATCHED THEN UPDATE SET
  distributor_name = src.distributor_name,
  city = src.city,
  state = src.state,
  tier = src.tier,
  created_at = src.created_at
WHEN NOT MATCHED AND src.__ACTION != 'DELETE' THEN INSERT (distributor_id, distributor_name, city, state, tier, created_at)
VALUES (src.distributor_id, src.distributor_name, src.city, src.state, src.tier, src.created_at);
$$;

-- Dealers
CREATE OR REPLACE PROCEDURE RAW.MERGE_DEALERS_STREAM()
RETURNS STRING
LANGUAGE SQL
AS
$$
MERGE INTO CORE.DEALERS tgt
USING (SELECT *, METADATA$ACTION AS __ACTION FROM RAW.DEALERS_RAW_STREAM) src
ON tgt.dealer_id = src.dealer_id
WHEN MATCHED AND src.__ACTION = 'DELETE' THEN DELETE
WHEN MATCHED THEN UPDATE SET
  dealer_name = src.dealer_name,
  city = src.city,
  state = src.state,
  created_at = src.created_at
WHEN NOT MATCHED AND src.__ACTION != 'DELETE' THEN INSERT (dealer_id, dealer_name, city, state, created_at)
VALUES (src.dealer_id, src.dealer_name, src.city, src.state, src.created_at);
$$;

-- Brand Stores
CREATE OR REPLACE PROCEDURE RAW.MERGE_BRAND_STORES_STREAM()
RETURNS STRING
LANGUAGE SQL
AS
$$
MERGE INTO CORE.BRAND_STORES tgt
USING (SELECT *, METADATA$ACTION AS __ACTION FROM RAW.BRANDSTORES_RAW_STREAM) src
ON tgt.brand_store_id = src.brand_store_id
WHEN MATCHED AND src.__ACTION = 'DELETE' THEN DELETE
WHEN MATCHED THEN UPDATE SET
  name = src.name,
  city = src.city,
  state = src.state,
  created_at = src.created_at
WHEN NOT MATCHED AND src.__ACTION != 'DELETE' THEN INSERT (brand_store_id, name, city, state, created_at)
VALUES (src.brand_store_id, src.name, src.city, src.state, src.created_at);
$$;

-- Local Shops
CREATE OR REPLACE PROCEDURE RAW.MERGE_LOCAL_SHOPS_STREAM()
RETURNS STRING
LANGUAGE SQL
AS
$$
MERGE INTO CORE.LOCAL_SHOPS tgt
USING (SELECT *, METADATA$ACTION AS __ACTION FROM RAW.LOCALSHOPS_RAW_STREAM) src
ON tgt.shop_id = src.shop_id
WHEN MATCHED AND src.__ACTION = 'DELETE' THEN DELETE
WHEN MATCHED THEN UPDATE SET
  shop_name = src.shop_name,
  owner_name = src.owner_name,
  city = src.city,
  state = src.state,
  created_at = src.created_at
WHEN NOT MATCHED AND src.__ACTION != 'DELETE' THEN INSERT (shop_id, shop_name, owner_name, city, state, created_at)
VALUES (src.shop_id, src.shop_name, src.owner_name, src.city, src.state, src.created_at);
$$;

-- Industrial Clients
CREATE OR REPLACE PROCEDURE RAW.MERGE_INDUSTRIAL_CLIENTS_STREAM()
RETURNS STRING
LANGUAGE SQL
AS
$$
MERGE INTO CORE.INDUSTRIAL_CLIENTS tgt
USING (SELECT *, METADATA$ACTION AS __ACTION FROM RAW.INDUSTRIAL_CLIENTS_RAW_STREAM) src
ON tgt.client_id = src.client_id
WHEN MATCHED AND src.__ACTION = 'DELETE' THEN DELETE
WHEN MATCHED THEN UPDATE SET
  client_name = src.client_name,
  industry_segment = src.industry_segment,
  contact_person = src.contact_person,
  contact_email = src.contact_email,
  city = src.city,
  state = src.state,
  created_at = src.created_at
WHEN NOT MATCHED AND src.__ACTION != 'DELETE' THEN INSERT (client_id, client_name, industry_segment, contact_person, contact_email, city, state, created_at)
VALUES (src.client_id, src.client_name, src.industry_segment, src.contact_person, src.contact_email, src.city, src.state, src.created_at);
$$;

-- Projects
CREATE OR REPLACE PROCEDURE RAW.MERGE_PROJECTS_STREAM()
RETURNS STRING
LANGUAGE SQL
AS
$$
MERGE INTO CORE.PROJECTS tgt
USING (SELECT *, METADATA$ACTION AS __ACTION FROM RAW.PROJECTS_RAW_STREAM) src
ON tgt.project_id = src.project_id
WHEN MATCHED AND src.__ACTION = 'DELETE' THEN DELETE
WHEN MATCHED THEN UPDATE SET
  project_name = src.project_name,
  client_id = src.client_id,
  start_date = src.start_date,
  end_date = src.end_date,
  city = src.city,
  state = src.state,
  status = src.status,
  created_at = src.created_at
WHEN NOT MATCHED AND src.__ACTION != 'DELETE' THEN INSERT (project_id, project_name, client_id, start_date, end_date, city, state, status, created_at)
VALUES (src.project_id, src.project_name, src.client_id, src.start_date, src.end_date, src.city, src.state, src.status, src.created_at);
$$;

-- Inventory
CREATE OR REPLACE PROCEDURE RAW.MERGE_INVENTORY_STREAM()
RETURNS STRING
LANGUAGE SQL
AS
$$
MERGE INTO CORE.INVENTORY tgt
USING (SELECT *, METADATA$ACTION AS __ACTION FROM RAW.INVENTORY_RAW_STREAM) src
ON tgt.product_id = src.product_id AND tgt.location_id = src.location_id
WHEN MATCHED AND src.__ACTION = 'DELETE' THEN DELETE
WHEN MATCHED THEN UPDATE SET
  location_type = src.location_type,
  quantity = src.quantity,
  last_updated = src.last_updated
WHEN NOT MATCHED AND src.__ACTION != 'DELETE' THEN INSERT (product_id, location_id, location_type, quantity, last_updated)
VALUES (src.product_id, src.location_id, src.location_type, src.quantity, src.last_updated);
$$;

-- Purchase Orders
CREATE OR REPLACE PROCEDURE RAW.MERGE_PURCHASE_ORDERS_STREAM()
RETURNS STRING
LANGUAGE SQL
AS
$$
MERGE INTO CORE.PURCHASE_ORDERS tgt
USING (SELECT *, METADATA$ACTION AS __ACTION FROM RAW.PURCHASE_ORDERS_RAW_STREAM) src
ON tgt.po_id = src.po_id
WHEN MATCHED AND src.__ACTION = 'DELETE' THEN DELETE
WHEN MATCHED THEN UPDATE SET
  supplier_id = src.supplier_id,
  product_id = src.product_id,
  qty = src.qty,
  price = src.price,
  po_ts = src.po_ts
WHEN NOT MATCHED AND src.__ACTION != 'DELETE' THEN INSERT (po_id, supplier_id, product_id, qty, price, po_ts)
VALUES (src.po_id, src.supplier_id, src.product_id, src.qty, src.price, src.po_ts);
$$;

-- Shipments
CREATE OR REPLACE PROCEDURE RAW.MERGE_SHIPMENTS_STREAM()
RETURNS STRING
LANGUAGE SQL
AS
$$
MERGE INTO CORE.SHIPMENTS tgt
USING (SELECT *, METADATA$ACTION AS __ACTION FROM RAW.SHIPMENTS_RAW_STREAM) src
ON tgt.shipment_id = src.shipment_id
WHEN MATCHED AND src.__ACTION = 'DELETE' THEN DELETE
WHEN MATCHED THEN UPDATE SET
  po_id = src.po_id,
  carrier = src.carrier,
  tracking_id = src.tracking_id,
  shipped_ts = src.shipped_ts,
  delivered_ts = src.delivered_ts
WHEN NOT MATCHED AND src.__ACTION != 'DELETE' THEN INSERT (shipment_id, po_id, carrier, tracking_id, shipped_ts, delivered_ts)
VALUES (src.shipment_id, src.po_id, src.carrier, src.tracking_id, src.shipped_ts, src.delivered_ts);
$$;

-- Promotions
CREATE OR REPLACE PROCEDURE RAW.MERGE_PROMOTIONS_STREAM()
RETURNS STRING
LANGUAGE SQL
AS
$$
MERGE INTO CORE.PROMOTIONS tgt
USING (SELECT *, METADATA$ACTION AS __ACTION FROM RAW.PROMOTIONS_RAW_STREAM) src
ON tgt.promo_id = src.promo_id
WHEN MATCHED AND src.__ACTION = 'DELETE' THEN DELETE
WHEN MATCHED THEN UPDATE SET
  promo_name = src.promo_name,
  start_ts = src.start_ts,
  end_ts = src.end_ts,
  discount_pct = src.discount_pct
WHEN NOT MATCHED AND src.__ACTION != 'DELETE' THEN INSERT (promo_id, promo_name, start_ts, end_ts, discount_pct)
VALUES (src.promo_id, src.promo_name, src.start_ts, src.end_ts, src.discount_pct);
$$;

-- Sales (final)
CREATE OR REPLACE PROCEDURE RAW.MERGE_SALES_STREAM()
RETURNS STRING
LANGUAGE SQL
AS
$$
MERGE INTO CORE.SALES tgt
USING (SELECT *, METADATA$ACTION AS __ACTION FROM RAW.SALES_RAW_STREAM) src
ON tgt.sale_id = src.sale_id
WHEN MATCHED AND src.__ACTION = 'DELETE' THEN DELETE
WHEN MATCHED THEN UPDATE SET
  customer_id = src.customer_id,
  customer_type = src.customer_type,
  product_id = src.product_id,
  store_id = src.store_id,
  distributor_id = src.distributor_id,
  dealer_id = src.dealer_id,
  brand_store_id = src.brand_store_id,
  supplier_id = src.supplier_id,
  division = src.division,
  quantity = src.quantity,
  sale_amount = src.sale_amount,
  discount_amount = src.discount_amount,
  sale_ts = src.sale_ts,
  channel = src.channel
WHEN NOT MATCHED AND src.__ACTION != 'DELETE' THEN INSERT (sale_id, customer_id, customer_type, product_id, store_id, distributor_id, dealer_id, brand_store_id, supplier_id, division, quantity, sale_amount, discount_amount, sale_ts, channel)
VALUES (src.sale_id, src.customer_id, src.customer_type, src.product_id, src.store_id, src.distributor_id, src.dealer_id, src.brand_store_id, src.supplier_id, src.division, src.quantity, src.sale_amount, src.discount_amount, src.sale_ts, src.channel);
$$;

----------------------------------------
-- 7) JS orchestrator: LOAD_AND_MERGE_ALL_STREAM()
--    This procedure:
--     - LISTs each fully-qualified stage (LIST @PAINTCO_DB.STG.X_STAGE)
--     - Iterates rows returned by LIST (no getMetaData used)
--     - For each file name returned (including nested folder names), COPY into RAW.<table>
--     - CALL the stream-based MERGE procedure for the corresponding table
--     - INSERT a row into PROCESSED_FILES and PIPE_LOG; REMOVE the file from the stage
--     - On error logs to ALERT_QUEUE and PIPE_LOG
----------------------------------------
USE SCHEMA RAW;

CREATE OR REPLACE PROCEDURE RAW.LOAD_AND_MERGE_ALL_STREAM()
RETURNS VARIANT
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
/*
  LOAD_AND_MERGE_ALL_STREAM - JS stored procedure
  - Handles nested file names returned by LIST (e.g., 'customers_stage/customers_5k.csv')
  - Uses fully-qualified stage reference strings like '@PAINTCO_DB.STG.CUSTOMERS_STAGE'
  - Uses file format PAINTCO_DB.PUBLIC.PAINTCO_CSV_FORMAT
*/

var DB = 'PAINTCO_DB';
var mappings = [
  {stage_ref: '@' + DB + '.STG.CUSTOMERS_STAGE', fq_stage: DB + '.STG.CUSTOMERS_STAGE', raw_table: 'RAW.CUSTOMERS_RAW', merge_proc: 'RAW.MERGE_CUSTOMERS_STREAM'},
  {stage_ref: '@' + DB + '.STG.PRODUCTS_STAGE',  fq_stage: DB + '.STG.PRODUCTS_STAGE',  raw_table: 'RAW.PRODUCTS_RAW',  merge_proc: 'RAW.MERGE_PRODUCTS_STREAM'},
  {stage_ref: '@' + DB + '.STG.STORES_STAGE',    fq_stage: DB + '.STG.STORES_STAGE',    raw_table: 'RAW.STORES_RAW',    merge_proc: 'RAW.MERGE_STORES_STREAM'},
  {stage_ref: '@' + DB + '.STG.SUPPLIERS_STAGE', fq_stage: DB + '.STG.SUPPLIERS_STAGE', raw_table: 'RAW.SUPPLIERS_RAW', merge_proc: 'RAW.MERGE_SUPPLIERS_STREAM'},
  {stage_ref: '@' + DB + '.STG.DISTRIBUTORS_STAGE', fq_stage: DB + '.STG.DISTRIBUTORS_STAGE', raw_table: 'RAW.DISTRIBUTORS_RAW', merge_proc: 'RAW.MERGE_DISTRIBUTORS_STREAM'},
  {stage_ref: '@' + DB + '.STG.DEALERS_STAGE', fq_stage: DB + '.STG.DEALERS_STAGE', raw_table: 'RAW.DEALERS_RAW', merge_proc: 'RAW.MERGE_DEALERS_STREAM'},
  {stage_ref: '@' + DB + '.STG.BRANDSTORES_STAGE', fq_stage: DB + '.STG.BRANDSTORES_STAGE', raw_table: 'RAW.BRANDSTORES_RAW', merge_proc: 'RAW.MERGE_BRAND_STORES_STREAM'},
  {stage_ref: '@' + DB + '.STG.LOCALSHOPS_STAGE', fq_stage: DB + '.STG.LOCALSHOPS_STAGE', raw_table: 'RAW.LOCALSHOPS_RAW', merge_proc: 'RAW.MERGE_LOCAL_SHOPS_STREAM'},
  {stage_ref: '@' + DB + '.STG.INDUSTRIAL_CLIENTS_STAGE', fq_stage: DB + '.STG.INDUSTRIAL_CLIENTS_STAGE', raw_table: 'RAW.INDUSTRIAL_CLIENTS_RAW', merge_proc: 'RAW.MERGE_INDUSTRIAL_CLIENTS_STREAM'},
  {stage_ref: '@' + DB + '.STG.PROJECTS_STAGE', fq_stage: DB + '.STG.PROJECTS_STAGE', raw_table: 'RAW.PROJECTS_RAW', merge_proc: 'RAW.MERGE_PROJECTS_STREAM'},
  {stage_ref: '@' + DB + '.STG.INVENTORY_STAGE', fq_stage: DB + '.STG.INVENTORY_STAGE', raw_table: 'RAW.INVENTORY_RAW', merge_proc: 'RAW.MERGE_INVENTORY_STREAM'},
  {stage_ref: '@' + DB + '.STG.PURCHASE_ORDERS_STAGE', fq_stage: DB + '.STG.PURCHASE_ORDERS_STAGE', raw_table: 'RAW.PURCHASE_ORDERS_RAW', merge_proc: 'RAW.MERGE_PURCHASE_ORDERS_STREAM'},
  {stage_ref: '@' + DB + '.STG.SHIPMENTS_STAGE', fq_stage: DB + '.STG.SHIPMENTS_STAGE', raw_table: 'RAW.SHIPMENTS_RAW', merge_proc: 'RAW.MERGE_SHIPMENTS_STREAM'},
  {stage_ref: '@' + DB + '.STG.PROMOTIONS_STAGE', fq_stage: DB + '.STG.PROMOTIONS_STAGE', raw_table: 'RAW.PROMOTIONS_RAW', merge_proc: 'RAW.MERGE_PROMOTIONS_STREAM'},
  {stage_ref: '@' + DB + '.STG.SALES_STAGE', fq_stage: DB + '.STG.SALES_STAGE', raw_table: 'RAW.SALES_RAW', merge_proc: 'RAW.MERGE_SALES_STREAM'}
];

// helper: execute a statement (no result expected)
function exec(sql, binds) {
  if (!binds) binds = [];
  var s = snowflake.createStatement({sqlText: sql, binds: binds});
  return s.execute();
}

// helper: safe log to PIPE_LOG
function logPipe(stage, file, table, action, rows, notes) {
  try {
    var sql = "INSERT INTO MONITORING.PIPE_LOG(source_stage, source_file, target_table, action, rows_loaded, notes) VALUES(?,?,?,?,?,?)";
    exec(sql, [stage, file, table, action, rows, notes]);
  } catch(e) {}
}

// helper: safe insert into PROCESSED_FILES
function recordProcessed(stage, file, md5, size, notes) {
  try {
    var sql = "INSERT INTO MONITORING.PROCESSED_FILES(file_name, stage_name, file_hash, file_size, notes) VALUES(?,?,?,?,?)";
    exec(sql, [file, stage, md5, size, notes]);
  } catch(e) {}
}

// helper: safe alert
function raiseAlert(stage, file, message) {
  try {
    var sql = "INSERT INTO MONITORING.ALERT_QUEUE(stage_name, file_name, error_message) VALUES(?,?,?)";
    exec(sql, [stage, file, message]);
  } catch(e) {}
}

// helper: escape single quotes in JS for SQL injection safety
function esc(s) {
  if (s === null || s === undefined) return '';
  return s.replace(/'/g, "''");
}

try {
  for (var m = 0; m < mappings.length; m++) {
    var map = mappings[m];
    var stageRef = map.stage_ref;   // e.g., @PAINTCO_DB.STG.CUSTOMERS_STAGE
    var fqStage  = map.fq_stage;    // e.g., PAINTCO_DB.STG.CUSTOMERS_STAGE
    var rawTable = map.raw_table;
    var mergeProc = map.merge_proc;

    // LIST files in stage (returns rows with columns: name, size, md5, last_modified)
    var listSql = "LIST " + stageRef;
    var listStmt = snowflake.createStatement({sqlText: listSql});
    var rs = listStmt.execute();

    var files = [];
    while (rs.next()) {
      // column positions from LIST: 1 = name, 2 = size, 3 = md5, 4 = last_modified
      var fname = rs.getColumnValue(1);
      var fsize = rs.getColumnValue(2);
      var fmd5  = rs.getColumnValue(3);
      // skip folder markers (end with '/')
      if (!fname) continue;
      files.push({name: fname, size: fsize, md5: fmd5});
    }

    if (files.length === 0) {
      logPipe(fqStage, '(none)', rawTable, 'COPY_EMPTY', 0, 'No files in stage');
      continue;
    }

    // iterate each file row that LIST returned
    for (var i = 0; i < files.length; i++) {
      var f = files[i];
      try {
        if (!f.name || f.name.slice(-1) === '/') {
          // skip directory pseudo entries
          continue;
        }

        // COPY INTO RAW: use fully-qualified file format and fully-qualified stage reference
        // IMPORTANT: COPY FROM requires the stage reference without quotes; we use stageRef variable already with leading '@'
      // Detect nested prefix and split it
var prefix = '';
var fileNameOnly = f.name;
if (f.name.includes('/')) {
    var parts = f.name.split('/');
    prefix = parts.slice(0, -1).join('/') + '/';
    fileNameOnly = parts[parts.length - 1];
}

var copySql = "COPY INTO " + rawTable +
              " FROM " + stageRef + "/" + prefix +
              " FILES = ('" + esc(fileNameOnly) + "')" +
              " FILE_FORMAT = (FORMAT_NAME = 'PAINTCO_DB.PUBLIC.PAINTCO_CSV_FORMAT')" +
              " ON_ERROR = 'CONTINUE'";


        // execute COPY; returns a ResultSet we can inspect
        var copyStmt = snowflake.createStatement({sqlText: copySql});
        var copyRs = copyStmt.execute();

        // COPY returns 1 row per file, attempt to read rows_loaded column if present
        var rowsLoaded = 0;
        try {
          // move cursor if possible and read the columns returned (defensively)
          if (copyRs.next()) {
            // Common COPY result columns: file, status, rows_parsed, rows_loaded, error_limit, errors_seen, first_error
            // We'll try to read column 4 (rows_loaded) if present.
            try { rowsLoaded = Number(copyRs.getColumnValue(4)) || 0; } catch(e) { rowsLoaded = 0; }
          }
        } catch(e) {
          rowsLoaded = 0;
        }

        // Log COPY result
        logPipe(fqStage, f.name, rawTable, 'COPY_RESULT', rowsLoaded, 'Copied file');

        // If no rows loaded, record and continue (do not remove file so it can be examined)
        if (rowsLoaded === 0) {
          // record as zero; keep file for inspection
          recordProcessed(fqStage, f.name, f.md5, Number(f.size || 0), 'COPY_ZERO_ROWS');
          continue;
        }

        // Call merge procedure which reads the RAW.*_STREAM and applies CDC merge to CORE
        try {
          var callSql = "CALL " + mergeProc + "();";
          exec(callSql);
          logPipe(fqStage, f.name, rawTable, 'MERGE_OK', rowsLoaded, 'Merge applied');
        } catch(mergeErr) {
          logPipe(fqStage, f.name, rawTable, 'MERGE_ERROR', rowsLoaded, mergeErr.message);
          raiseAlert(fqStage, f.name, 'MERGE_ERROR: ' + mergeErr.message);
          // keep file for investigation, do NOT remove
          continue;
        }

        // Insert PROCESSED_FILES record
        recordProcessed(fqStage, f.name, f.md5, Number(f.size || 0), 'loaded');

        // Remove processed file from stage to prevent reprocessing
        try {
          var removeSql = "REMOVE " + stageRef + " '" + esc(f.name) + "'";
          exec(removeSql);
          logPipe(fqStage, f.name, rawTable, 'REMOVE_OK', rowsLoaded, 'Removed file from stage');
        } catch(removeErr) {
          logPipe(fqStage, f.name, rawTable, 'REMOVE_ERROR', rowsLoaded, removeErr.message);
          raiseAlert(fqStage, f.name, 'REMOVE_ERROR: ' + removeErr.message);
        }
      } catch(fileErr) {
        logPipe(fqStage, f.name || '(unknown)', rawTable, 'FILE_PROCESS_ERROR', 0, fileErr.message);
        raiseAlert(fqStage, f.name || '(unknown)', fileErr.message);
        // continue to next file
      }
    } // end files loop
  } // end mappings loop

  return {status: 'OK', ts: (new Date()).toISOString()};
} catch(e) {
  try {
    var errSql = "INSERT INTO MONITORING.ALERT_QUEUE(stage_name, file_name, error_message) VALUES(?,?,?)";
    snowflake.createStatement({sqlText: errSql, binds: ['LOAD_AND_MERGE_ALL_STREAM','(global)', e.message]}).execute();
  } catch(ee) {}
  throw e;
}
$$;

----------------------------------------
-- 8) Optional helper: Flatten nested folder names into root (if you want persistent root names)
--    (This SP attempts to copy file from nested prefix to root name and remove the nested file.)
--    Note: Snowflake does not support server-to-server COPY FILE command in older accounts; this SP tries COPY INTO @stage/target with GET/PUT is not possible inside Snowflake.
--    Keep it for manual attempts; you can skip using it. The orchestrator already handles nested names.
----------------------------------------

USE SCHEMA STG;
CREATE OR REPLACE PROCEDURE STG.NOOP_CLEANUP_STAGE()
RETURNS STRING
LANGUAGE SQL
AS
$$
/* Placeholder - not required. We rely on orchestrator to process nested filenames directly. */
SELECT 'noop';
$$;

----------------------------------------
-- 9) Create a Task to schedule the orchestrator (every 5 minutes)
----------------------------------------
CREATE OR REPLACE TASK RAW.LOAD_AND_MERGE_TASK_STREAM
  WAREHOUSE = 'COMPUTE_WH'
  SCHEDULE = 'USING CRON 0/5 * * * * UTC'
AS
  CALL RAW.LOAD_AND_MERGE_ALL_STREAM();

-- Enable the task when you're ready:
-- ALTER TASK RAW.LOAD_AND_MERGE_TASK_STREAM RESUME;

----------------------------------------
-- 10) Dynamic tables (examples) and monitoring capture
----------------------------------------
USE SCHEMA CORE;

CREATE OR REPLACE DYNAMIC TABLE CORE.SALES_CLEANED
  TARGET_LAG = 'DOWNSTREAM'
  WAREHOUSE = 'COMPUTE_WH'
  REFRESH_MODE = 'INCREMENTAL'
  AS
  SELECT
    s.sale_id,
    s.customer_id,
    c.customer_name,
    s.product_id,
    p.product_name,
    p.brand,
    s.store_id,
    st.store_name,
    s.distributor_id,
    s.dealer_id,
    s.brand_store_id,
    s.supplier_id,
    s.division,
    s.quantity,
    s.sale_amount,
    s.discount_amount,
    s.sale_ts,
    s.channel
  FROM CORE.SALES s
  LEFT JOIN CORE.CUSTOMERS c ON s.customer_id = c.customer_id
  LEFT JOIN CORE.PRODUCTS p ON s.product_id = p.product_id
  LEFT JOIN CORE.STORES st ON s.store_id = st.store_id;

CREATE OR REPLACE DYNAMIC TABLE CORE.DAILY_DIVISION_BRAND_SALES
  TARGET_LAG = '5 minutes'
  WAREHOUSE = 'COMPUTE_WH'
  REFRESH_MODE = 'INCREMENTAL'
  AS
  SELECT
    DATE_TRUNC('day', sale_ts) AS sale_date,
    division,
    brand,
    COUNT(DISTINCT sale_id) AS transactions,
    SUM(quantity) AS units_sold,
    SUM(sale_amount) AS gross_revenue,
    SUM(discount_amount) AS total_discount,
    SUM(sale_amount) - SUM(discount_amount) AS net_revenue
  FROM CORE.SALES_CLEANED
  GROUP BY 1,2,3;

-- Monitoring dynamic table refreshes into PIPE_LOG
USE SCHEMA MONITORING;

CREATE OR REPLACE PROCEDURE MONITORING.CAPTURE_DYNAMIC_REFRESHES()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
    INSERT INTO MONITORING.PIPE_LOG (source_stage, source_file, target_table, action, notes)
    SELECT 
        NAME, 
        NULL, 
        'DYNAMIC_REFRESH', 
        'DYNAMIC_REFRESH', 
        CONCAT('status=', STATUS, ';rows_changed=', ROWS_CHANGED)
    FROM TABLE(INFORMATION_SCHEMA.DYNAMIC_TABLE_REFRESH_HISTORY())
    WHERE DATA_TIMESTAMP >= DATEADD(day, -1, CURRENT_TIMESTAMP());

    RETURN 'CAPTURED_REFRESHES';
END;
$$;


----------------------------------------
-- 11) Runbook: Quick step-by-step to test the full flow
----------------------------------------
-- A) Upload test CSV(s) to internal stage(s) via Snowsight or SnowSQL.
--    NOTE: Snowsight tends to create subfolder names like "customers_stage/customers_5k.csv".
--    Example SnowSQL upload:
--      snowsql -a <account> -u <user> -q "PUT file:///local/path/customers_5k.csv @STG.CUSTOMERS_STAGE AUTO_COMPRESS=FALSE;"
--
-- B) Verify file is visible (example):
--      LIST @PAINTCO_DB.STG.CUSTOMERS_STAGE;
--    Expected: a row with name like "customers_stage/customers_5k.csv"
--      
--LIST @STG.CUSTOMERS_STAGE;
--LIST @STG.PRODUCTS_STAGE;
--LIST @STG.STORES_STAGE;
--LIST @STG.SUPPLIERS_STAGE;
--LIST @STG.DISTRIBUTORS_STAGE;
--LIST @STG.DEALERS_STAGE;
--LIST @STG.BRANDSTORES_STAGE;
--LIST @STG.LOCALSHOPS_STAGE;
--LIST @STG.INDUSTRIAL_CLIENTS_STAGE;
--LIST @STG.PROJECTS_STAGE;
--LIST @STG.INVENTORY_STAGE;
--LIST @STG.PURCHASE_ORDERS_STAGE;
--LIST @STG.SHIPMENTS_STAGE;
--LIST @STG.PROMOTIONS_STAGE;
--LIST @STG.SALES_STAGE;

-- C) Run orchestrator manually (first time):
--      CALL RAW.LOAD_AND_MERGE_ALL_STREAM();
--    The stored procedure will:
--     - COPY listed files into RAW.<table>
--     - CALL corresponding merge proc to move delta rows into CORE.<table>
--     - Remove file from stage on success and log actions
--
-- D) Verify monitoring and data:
--      SELECT * FROM MONITORING.PIPE_LOG ORDER BY log_ts DESC LIMIT 50;
--      SELECT * FROM MONITORING.PROCESSED_FILES ORDER BY loaded_ts DESC LIMIT 50;
--      SELECT COUNT(*) FROM RAW.CUSTOMERS_RAW;
--      SELECT COUNT(*) FROM CORE.CUSTOMERS;


--      SELECT COUNT(*) FROM RAW.SALES_RAW;
--      SELECT COUNT(*) FROM CORE.SALES;
--
-- E) If you want the task to run automatically:
--      ALTER TASK RAW.LOAD_AND_MERGE_TASK_STREAM RESUME;
--    Confirm via:
--      SELECT * FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY()) WHERE NAME = 'LOAD_AND_MERGE_TASK_STREAM' ORDER BY SCHEDULED_TIME DESC LIMIT 10;
--
-- F) Troubleshooting tips:
--   - If COPY fails with "file not found", use LIST @PAINTCO_DB.STG.<stage> to confirm exact name string; the JS SP uses that exact name.
--   - If merges don't apply, check RAW.<table>_RAW_STREAM for rows: SELECT * FROM RAW.CUSTOMERS_RAW_STREAM;
--   - If dynamic tables not refreshing, call MONITORING.CAPTURE_DYNAMIC_REFRESHES() and inspect PIPE_LOG.

----------------------------------------
-- 12) Quick verification queries (run after manual CALL)
----------------------------------------
-- SELECT * FROM MONITORING.PIPE_LOG ORDER BY log_ts DESC LIMIT 50;
-- SELECT * FROM MONITORING.PROCESSED_FILES ORDER BY loaded_ts DESC LIMIT 50;
-- SELECT COUNT(*) FROM RAW.CUSTOMERS_RAW;
-- SELECT COUNT(*) FROM CORE.CUSTOMERS;
-- SELECT COUNT(*) FROM RAW.SALES_RAW;
-- SELECT COUNT(*) FROM CORE.SALES;

-- End of single deploy script for PaintCo ELT CDC pipeline
-- ====================================================================================
