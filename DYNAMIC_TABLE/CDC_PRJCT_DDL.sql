-- ====================================================================================
-- PAINTCO_DB — FINAL SINGLE-FILE DEPLOY
-- RAW = CSV schema + file_id + loaded_at
-- Simple loader 
-- Single .sql file (create DB, schemas, file formats, stage, tables, streams, procs, tasks)
-- ====================================================================================

-- 0) Create database + schemas + file format + unified stage
CREATE OR REPLACE DATABASE PAINTCO_DB;
USE DATABASE PAINTCO_DB;

CREATE OR REPLACE SCHEMA STG;
CREATE OR REPLACE SCHEMA RAW;
CREATE OR REPLACE SCHEMA CORE;
CREATE OR REPLACE SCHEMA MONITORING;
CREATE OR REPLACE SCHEMA PUBLIC;

-- File formats
CREATE OR REPLACE FILE FORMAT PAINTCO_DB.PUBLIC.PAINTCO_CSV_FORMAT
  TYPE = 'CSV'
  FIELD_DELIMITER = ','
  SKIP_HEADER = 1
  NULL_IF = ('', 'NULL')
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  TRIM_SPACE = TRUE
  ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE;

CREATE OR REPLACE FILE FORMAT PAINTCO_DB.PUBLIC.PAINTCO_CSV_FORMAT_HEADER
  TYPE = 'CSV'
  FIELD_DELIMITER = ','
  SKIP_HEADER = 0
  NULL_IF = ('', 'NULL')
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  TRIM_SPACE = TRUE;

-- Unified internal stage
CREATE OR REPLACE STAGE STG.PAINTCO_UNIFIED_STAGE
  FILE_FORMAT = PAINTCO_DB.PUBLIC.PAINTCO_CSV_FORMAT;

-- ====================================================================================
-- 1) MONITORING tables 
-- ====================================================================================
USE SCHEMA MONITORING;

CREATE OR REPLACE TABLE MONITORING.PROCESSED_FILES (
  file_name STRING PRIMARY KEY,
  stage_name STRING,
  target_table STRING,
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

-- ====================================================================================
-- 2) RAW tables 
-- ====================================================================================
USE SCHEMA RAW;

-- 1 CUSTOMERS_RAW
CREATE OR REPLACE TABLE RAW.CUSTOMERS_RAW (
  customer_id STRING,
  customer_name STRING,
  customer_type STRING,
  email STRING,
  phone STRING,
  city STRING,
  state STRING,
  pin_code STRING,
  division STRING,
  created_at TIMESTAMP_LTZ,
  file_id STRING,
  loaded_at TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP
);

-- 2 PRODUCTS_RAW
CREATE OR REPLACE TABLE RAW.PRODUCTS_RAW (
  product_id STRING,
  sku STRING,
  product_name STRING,
  brand STRING,
  category STRING,
  sub_category STRING,
  unit_price NUMBER(12,2),
  uom STRING,
  pack_size STRING,
  created_at TIMESTAMP_LTZ,
  file_id STRING,
  loaded_at TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP
);

-- 3 STORES_RAW
CREATE OR REPLACE TABLE RAW.STORES_RAW (
  store_id STRING,
  store_name STRING,
  store_type STRING,
  city STRING,
  state STRING,
  region STRING,
  created_at TIMESTAMP_LTZ,
  file_id STRING,
  loaded_at TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP
);

-- 4 DISTRIBUTORS_RAW
CREATE OR REPLACE TABLE RAW.DISTRIBUTORS_RAW (
  distributor_id STRING,
  distributor_name STRING,
  city STRING,
  state STRING,
  tier STRING,
  created_at TIMESTAMP_LTZ,
  file_id STRING,
  loaded_at TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP
);

-- 5 DEALERS_RAW
CREATE OR REPLACE TABLE RAW.DEALERS_RAW (
  dealer_id STRING,
  dealer_name STRING,
  city STRING,
  state STRING,
  created_at TIMESTAMP_LTZ,
  file_id STRING,
  loaded_at TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP
);

-- 6 BRANDSTORES_RAW
CREATE OR REPLACE TABLE RAW.BRANDSTORES_RAW (
  brand_store_id STRING,
  name STRING,
  city STRING,
  state STRING,
  created_at TIMESTAMP_LTZ,
  file_id STRING,
  loaded_at TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP
);

-- 7 LOCALSHOPS_RAW
CREATE OR REPLACE TABLE RAW.LOCALSHOPS_RAW (
  shop_id STRING,
  shop_name STRING,
  owner_name STRING,
  city STRING,
  state STRING,
  created_at TIMESTAMP_LTZ,
  file_id STRING,
  loaded_at TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP
);

-- 8 INDUSTRIAL_CLIENTS_RAW
CREATE OR REPLACE TABLE RAW.INDUSTRIAL_CLIENTS_RAW (
  client_id STRING,
  client_name STRING,
  industry_segment STRING,
  contact_person STRING,
  contact_email STRING,
  city STRING,
  state STRING,
  created_at TIMESTAMP_LTZ,
  file_id STRING,
  loaded_at TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP
);

-- 9 PROJECTS_RAW
CREATE OR REPLACE TABLE RAW.PROJECTS_RAW (
  project_id STRING,
  project_name STRING,
  client_id STRING,
  start_date TIMESTAMP_LTZ,
  end_date TIMESTAMP_LTZ,
  city STRING,
  state STRING,
  status STRING,
  created_at TIMESTAMP_LTZ,
  file_id STRING,
  loaded_at TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP
);

-- 10 SUPPLIERS_RAW
CREATE OR REPLACE TABLE RAW.SUPPLIERS_RAW (
  supplier_id STRING,
  supplier_name STRING,
  contact_email STRING,
  city STRING,
  state STRING,
  created_at TIMESTAMP_LTZ,
  file_id STRING,
  loaded_at TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP
);

-- 11 INVENTORY_RAW
CREATE OR REPLACE TABLE RAW.INVENTORY_RAW (
  product_id STRING,
  location_id STRING,
  location_type STRING,
  quantity NUMBER,
  last_updated TIMESTAMP_LTZ,
  file_id STRING,
  loaded_at TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP
);

-- 12 PURCHASE_ORDERS_RAW
CREATE OR REPLACE TABLE RAW.PURCHASE_ORDERS_RAW (
  po_id STRING,
  supplier_id STRING,
  product_id STRING,
  qty NUMBER,
  price NUMBER(18,2),
  po_ts TIMESTAMP_LTZ,
  file_id STRING,
  loaded_at TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP
);

-- 13 SHIPMENTS_RAW
CREATE OR REPLACE TABLE RAW.SHIPMENTS_RAW (
  shipment_id STRING,
  po_id STRING,
  carrier STRING,
  tracking_id STRING,
  shipped_ts TIMESTAMP_LTZ,
  delivered_ts TIMESTAMP_LTZ,
  file_id STRING,
  loaded_at TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP
);

-- 14 PROMOTIONS_RAW
CREATE OR REPLACE TABLE RAW.PROMOTIONS_RAW (
  promo_id STRING,
  promo_name STRING,
  start_ts TIMESTAMP_LTZ,
  end_ts TIMESTAMP_LTZ,
  discount_pct NUMBER,
  file_id STRING,
  loaded_at TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP
);

-- 15 SALES_RAW
CREATE OR REPLACE TABLE RAW.SALES_RAW (
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
  sale_amount NUMBER(18,2),
  discount_amount NUMBER(18,2),
  sale_ts TIMESTAMP_LTZ,
  channel STRING,
  file_id STRING,
  loaded_at TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP
);

-- ====================================================================================
-- 3) CORE curated tables 
-- ====================================================================================
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
  unit_price NUMBER(12,2),
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

CREATE OR REPLACE TABLE CORE.SUPPLIERS (
  supplier_id STRING PRIMARY KEY,
  supplier_name STRING,
  contact_email STRING,
  city STRING,
  state STRING,
  created_at TIMESTAMP_LTZ
);

CREATE OR REPLACE TABLE CORE.INVENTORY (
  product_id STRING,
  location_id STRING,
  location_type STRING,
  quantity NUMBER,
  last_updated TIMESTAMP_LTZ,
  PRIMARY KEY (product_id, location_id)
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
  sale_amount NUMBER(18,2),
  discount_amount NUMBER(18,2),
  sale_ts TIMESTAMP_LTZ,
  channel STRING
);

CREATE OR REPLACE TABLE CORE.PURCHASE_ORDERS (
  po_id STRING PRIMARY KEY,
  supplier_id STRING,
  product_id STRING,
  qty NUMBER,
  price NUMBER(18,2),
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

-- ====================================================================================
-- 4) Streams on RAW tables
-- ====================================================================================
USE SCHEMA RAW;

SELECT GET_DDL('procedure', 'RAW.UNIVERSAL_MERGE(VARCHAR, VARCHAR)');


CREATE OR REPLACE STREAM RAW.CUSTOMERS_RAW_STREAM ON TABLE RAW.CUSTOMERS_RAW APPEND_ONLY = FALSE;
CREATE OR REPLACE STREAM RAW.PRODUCTS_RAW_STREAM ON TABLE RAW.PRODUCTS_RAW APPEND_ONLY = FALSE;
CREATE OR REPLACE STREAM RAW.STORES_RAW_STREAM ON TABLE RAW.STORES_RAW APPEND_ONLY = FALSE;
CREATE OR REPLACE STREAM RAW.DISTRIBUTORS_RAW_STREAM ON TABLE RAW.DISTRIBUTORS_RAW APPEND_ONLY = FALSE;
CREATE OR REPLACE STREAM RAW.DEALERS_RAW_STREAM ON TABLE RAW.DEALERS_RAW APPEND_ONLY = FALSE;
CREATE OR REPLACE STREAM RAW.BRANDSTORES_RAW_STREAM ON TABLE RAW.BRANDSTORES_RAW APPEND_ONLY = FALSE;
CREATE OR REPLACE STREAM RAW.LOCALSHOPS_RAW_STREAM ON TABLE RAW.LOCALSHOPS_RAW APPEND_ONLY = FALSE;
CREATE OR REPLACE STREAM RAW.INDUSTRIAL_CLIENTS_RAW_STREAM ON TABLE RAW.INDUSTRIAL_CLIENTS_RAW APPEND_ONLY = FALSE;
CREATE OR REPLACE STREAM RAW.PROJECTS_RAW_STREAM ON TABLE RAW.PROJECTS_RAW APPEND_ONLY = FALSE;
CREATE OR REPLACE STREAM RAW.SUPPLIERS_RAW_STREAM ON TABLE RAW.SUPPLIERS_RAW APPEND_ONLY = FALSE;
CREATE OR REPLACE STREAM RAW.INVENTORY_RAW_STREAM ON TABLE RAW.INVENTORY_RAW APPEND_ONLY = FALSE;
CREATE OR REPLACE STREAM RAW.PURCHASE_ORDERS_RAW_STREAM ON TABLE RAW.PURCHASE_ORDERS_RAW APPEND_ONLY = FALSE;
CREATE OR REPLACE STREAM RAW.SHIPMENTS_RAW_STREAM ON TABLE RAW.SHIPMENTS_RAW APPEND_ONLY = FALSE;
CREATE OR REPLACE STREAM RAW.PROMOTIONS_RAW_STREAM ON TABLE RAW.PROMOTIONS_RAW APPEND_ONLY = FALSE;
CREATE OR REPLACE STREAM RAW.SALES_RAW_STREAM ON TABLE RAW.SALES_RAW APPEND_ONLY = FALSE;

-- ====================================================================================
-- 5) MERGE 
-- ====================================================================================

USE SCHEMA RAW;
CREATE OR REPLACE PROCEDURE RAW.UNIVERSAL_MERGE(RAW_TABLE_NAME_IN VARCHAR, FILE_NAME VARCHAR)
RETURNS VARIANT
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
function exec(sql, binds){binds=binds||[];return snowflake.createStatement({sqlText:sql,binds:binds}).execute();}

try {
  var raw_in = String(RAW_TABLE_NAME_IN||'').trim();
  if(raw_in==='') throw "RAW_TABLE_NAME_IN is empty";

  var parts = raw_in.split('.');
  var rawOnly = parts[parts.length-1].toUpperCase();

  // Build FQN for RAW table
  var fqRaw;
  if(parts.length===1){
    var dbRs=exec("SELECT CURRENT_DATABASE()");dbRs.next();
    fqRaw=dbRs.getColumnValue(1)+".RAW."+rawOnly;
  } else if(parts.length===2){
    var dbRs2=exec("SELECT CURRENT_DATABASE()");dbRs2.next();
    fqRaw=dbRs2.getColumnValue(1)+"."+raw_in.toUpperCase();
  } else fqRaw=raw_in.toUpperCase();

  // Mapping RAW -> CORE and PKs
  var mapping={
    'CUSTOMERS_RAW':'CORE.CUSTOMERS','PRODUCTS_RAW':'CORE.PRODUCTS','STORES_RAW':'CORE.STORES',
    'DISTRIBUTORS_RAW':'CORE.DISTRIBUTORS','DEALERS_RAW':'CORE.DEALERS','BRANDSTORES_RAW':'CORE.BRAND_STORES',
    'LOCALSHOPS_RAW':'CORE.LOCAL_SHOPS','INDUSTRIAL_CLIENTS_RAW':'CORE.INDUSTRIAL_CLIENTS','PROJECTS_RAW':'CORE.PROJECTS',
    'SUPPLIERS_RAW':'CORE.SUPPLIERS','INVENTORY_RAW':'CORE.INVENTORY','PURCHASE_ORDERS_RAW':'CORE.PURCHASE_ORDERS',
    'SHIPMENTS_RAW':'CORE.SHIPMENTS','PROMOTIONS_RAW':'CORE.PROMOTIONS','SALES_RAW':'CORE.SALES'
  };
  var pkMap={
    'CUSTOMERS_RAW':['CUSTOMER_ID'],'PRODUCTS_RAW':['PRODUCT_ID'],'STORES_RAW':['STORE_ID'],
    'DISTRIBUTORS_RAW':['DISTRIBUTOR_ID'],'DEALERS_RAW':['DEALER_ID'],'BRANDSTORES_RAW':['BRAND_STORE_ID'],
    'LOCALSHOPS_RAW':['SHOP_ID'],'INDUSTRIAL_CLIENTS_RAW':['CLIENT_ID'],'PROJECTS_RAW':['PROJECT_ID'],
    'SUPPLIERS_RAW':['SUPPLIER_ID'],'INVENTORY_RAW':['PRODUCT_ID','LOCATION_ID'],
    'PURCHASE_ORDERS_RAW':['PO_ID'],'SHIPMENTS_RAW':['SHIPMENT_ID'],'PROMOTIONS_RAW':['PROMO_ID'],'SALES_RAW':['SALE_ID']
  };
  if(!mapping[rawOnly]) throw "No mapping for "+rawOnly;
  var coreTable=mapping[rawOnly], pkCols=pkMap[rawOnly];

  // Get RAW column names
  var rs=exec(`SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS 
               WHERE TABLE_SCHEMA='RAW' AND TABLE_NAME=? ORDER BY ORDINAL_POSITION`,[rawOnly]);
  var cols=[];while(rs.next())cols.push(rs.getColumnValue(1));
  if(cols.length===0) throw "No columns found for "+rawOnly;

  // Exclude technical columns from MERGE (FILE_ID, LOADED_AT)
  var dataCols=cols.filter(c=>!['FILE_ID','LOADED_AT'].includes(c.toUpperCase()));
  var srcSelect=dataCols.map(c=>'"'+c+'"').join(', ');
  var onClause=pkCols.map(c=>`tgt."${c}"=src."${c}"`).join(' AND ');
  var updateSets=dataCols.filter(c=>!pkCols.includes(c)).map(c=>`tgt."${c}"=src."${c}"`);
  var insertCols=dataCols.map(c=>'"'+c+'"').join(', ');
  var insertVals=dataCols.map(c=>'src."'+c+'"').join(', ');
  if(updateSets.length===0)updateSets.push(`tgt."${pkCols[0]}"=tgt."${pkCols[0]}"`);

  var mergeSql=`
    MERGE INTO ${coreTable} tgt
    USING (SELECT ${srcSelect} FROM ${fqRaw} WHERE file_id = ?) src
    ON ${onClause}
    WHEN MATCHED THEN UPDATE SET ${updateSets.join(', ')}
    WHEN NOT MATCHED THEN INSERT (${insertCols}) VALUES (${insertVals});
  `;
  exec(mergeSql,[FILE_NAME]);
  return {status:'MERGE_OK',raw:fqRaw,core:coreTable,file:FILE_NAME};

}catch(err){
  try{exec("INSERT INTO MONITORING.ALERT_QUEUE(stage_name,file_name,error_message) VALUES(?,?,?)",
           ['UNIVERSAL_MERGE',FILE_NAME,String(err)]);}catch(e){}
  throw err;
}
$$;

-- ====================================================================================
-- 6) Simple loader  — COPY -> SET file_id -> CALL MERGE -> REMOVE
-- ====================================================================================

USE SCHEMA RAW;
CREATE OR REPLACE PROCEDURE RAW.LOAD_AND_MERGE_SIMPLE()
RETURNS VARIANT
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
function exec(sql,binds){binds=binds||[];return snowflake.createStatement({sqlText:sql,binds:binds}).execute();}
function safeLog(stage,file,table,action,rows,notes){
  try{exec("INSERT INTO MONITORING.PIPE_LOG(source_stage,source_file,target_table,action,rows_loaded,notes) VALUES(?,?,?,?,?,?)",
           [stage,file,table,action,rows,notes]);}catch(e){}
}
function safeProcessed(stage, file, table, md5, size, notes){
  try{
    exec("INSERT INTO MONITORING.PROCESSED_FILES(file_name, stage_name, target_table, file_hash, file_size, notes) VALUES(?,?,?,?,?,?)",
         [file, stage, table, md5, size, notes]);
  }catch(e){}
}
function safeAlert(stage,file,msg){
  try{exec("INSERT INTO MONITORING.ALERT_QUEUE(stage_name,file_name,error_message) VALUES(?,?,?)",
           [stage,file,msg]);}catch(e){}
}

try{
  const DB='PAINTCO_DB';
  const stageRef='@'+DB+'.STG.PAINTCO_UNIFIED_STAGE';
  const fqStage=DB+'.STG.PAINTCO_UNIFIED_STAGE';
  const mapping={
    'customers':'RAW.CUSTOMERS_RAW','products':'RAW.PRODUCTS_RAW','stores':'RAW.STORES_RAW',
    'suppliers':'RAW.SUPPLIERS_RAW','distributors':'RAW.DISTRIBUTORS_RAW','dealers':'RAW.DEALERS_RAW',
    'brandstores':'RAW.BRANDSTORES_RAW','localshops':'RAW.LOCALSHOPS_RAW','industrial_clients':'RAW.INDUSTRIAL_CLIENTS_RAW',
    'projects':'RAW.PROJECTS_RAW','inventory':'RAW.INVENTORY_RAW','purchase_orders':'RAW.PURCHASE_ORDERS_RAW',
    'shipments':'RAW.SHIPMENTS_RAW','promotions':'RAW.PROMOTIONS_RAW','sales':'RAW.SALES_RAW'
  };

  var listRs=exec("LIST "+stageRef); var files=[];
  while(listRs.next()){
    var n=listRs.getColumnValue(1);
    if(!n||n.endsWith('/'))continue;
    files.push({fullPath:n,fileName:n.split('/').pop(),size:Number(listRs.getColumnValue(2)||0),md5:listRs.getColumnValue(3)});
  }
  if(files.length===0){safeLog(fqStage,'(none)','(none)','NO_FILES',0,'Stage empty');return{status:'NO_FILES'};}

  var summary={files:files.length,copied:0,merged:0,removed:0};
  for(let f of files){
    let lower=f.fileName.toLowerCase();let matched=null;
    for(let pref in mapping){if(lower.startsWith(pref)){matched=pref;break;}}
    if(!matched){for(let pref2 in mapping){if(lower.indexOf(pref2)>=0){matched=pref2;break;}}}
    if(!matched){safeAlert(fqStage,f.fullPath,'NO_MAPPING');continue;}
    let rawShort=mapping[matched];
    let fqRaw=DB+'.'+rawShort.toUpperCase();

    // ✅ COPY
    let rowsLoaded=0;
    try{
      let copySql=`COPY INTO ${fqRaw} FROM ${stageRef}
                   FILES=('${f.fileName.replace(/'/g,"''")}')
                   FILE_FORMAT=(FORMAT_NAME='PAINTCO_DB.PUBLIC.PAINTCO_CSV_FORMAT')
                   ON_ERROR='CONTINUE' FORCE=TRUE`;
      let rs=exec(copySql);while(rs.next()){rowsLoaded+=Number(rs.getColumnValue(4)||0);}
    }catch(e){safeAlert(fqStage,f.fullPath,'COPY_FAILED:'+e);continue;}
    summary.copied++;safeLog(fqStage,f.fullPath,fqRaw,'COPIED',rowsLoaded,'Copy finished');

    // Update FILE_ID
    try{exec(`UPDATE ${fqRaw} SET file_id=? WHERE file_id IS NULL OR file_id=''`,[f.fullPath]);}
    catch(u){safeLog(fqStage,f.fullPath,fqRaw,'FILE_ID_UPDATE_FAILED',rowsLoaded,String(u));}

// ✅ MERGE 
try {
  const shortName = rawShort.split('.')[1]; // e.g., CUSTOMERS_RAW
  const mergeStmt = snowflake.createStatement({
    sqlText: `CALL RAW.UNIVERSAL_MERGE(?, ?)`,
    binds: [shortName, f.fullPath]
  });
  mergeStmt.execute();
  summary.merged++;
  safeLog(fqStage, f.fullPath, fqRaw, 'MERGED', rowsLoaded, 'Merge OK');
} catch (e) {
  safeAlert(fqStage, f.fullPath, 'MERGE_ERROR:' + e);
  continue;
}


    // ✅ REMOVE 
    try {
      safeProcessed(fqStage, f.fullPath, fqRaw, f.md5, f.size, 'LOADED');
      const removeSql = `REMOVE @${DB}.STG.PAINTCO_UNIFIED_STAGE PATTERN='${f.fileName.replace(/'/g, "''")}'`;
      exec(removeSql);
      summary.removed++;
      safeLog(fqStage, f.fullPath, fqRaw, 'REMOVED', rowsLoaded, 'Removed from stage');
    } catch (rem) {
      safeAlert(fqStage, f.fullPath, 'REMOVE_FAILED:' + rem);
    }
  }

  return{status:'OK',summary:summary};
}catch(e){
  try{exec("INSERT INTO MONITORING.ALERT_QUEUE(stage_name,file_name,error_message) VALUES(?,?,?)",
           ['LOAD_AND_MERGE_SIMPLE','(global)',String(e)]);}catch(ee){}
  throw e;
}
$$;


-- ====================================================================================
-- 7) Task to schedule loader (example every 5 minutes). Modify as needed.
-- ====================================================================================
CREATE OR REPLACE TASK RAW.LOAD_AND_MERGE_SIMPLE_TASK
  WAREHOUSE = 'COMPUTE_WH'
  SCHEDULE = 'USING CRON 0/5 * * * * UTC'
AS
  CALL RAW.LOAD_AND_MERGE_SIMPLE();

  ---- for testing counts ----

  CALL RAW.LOAD_AND_MERGE_SIMPLE();
  
  SELECT * FROM MONITORING.PIPE_LOG ORDER BY log_ts DESC LIMIT 50;

-- Enable the task when you're ready:
-- ALTER TASK RAW.LOAD_AND_MERGE_TASK_STREAM RESUME;

-- Disable the task when you're ready:
-- ALTER TASK RAW.LOAD_AND_MERGE_TASK_STREAM SUSPEND;
  
-- ====================================================================================
-- 8) Optional: lightweight dynamic examples (unchanged)
-- ====================================================================================
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

-- ====================================================================================
-- End of single-file deployment script
-- ====================================================================================



USE SCHEMA MONITORING;

CREATE OR REPLACE VIEW MONITORING.PIPELINE_STATUS AS
SELECT 
  'PIPE_LOG' AS SOURCE,
  log_ts AS EVENT_TS,
  source_stage,
  source_file,
  target_table,
  action,
  rows_loaded,
  notes
FROM MONITORING.PIPE_LOG

UNION ALL

SELECT 
  'ALERT_QUEUE' AS SOURCE,
  alert_ts AS EVENT_TS,
  stage_name AS source_stage,
  file_name AS source_file,
  NULL AS target_table,
  'ALERT' AS action,
  NULL AS rows_loaded,
  error_message AS notes
FROM MONITORING.ALERT_QUEUE

UNION ALL

SELECT 
  'PROCESSED_FILES' AS SOURCE,
  loaded_ts AS EVENT_TS,
  stage_name AS source_stage,
  file_name AS source_file,
  target_table,
  'PROCESSED' AS action,
  file_size AS rows_loaded,
  notes
FROM MONITORING.PROCESSED_FILES

ORDER BY EVENT_TS DESC;


SELECT * FROM MONITORING.PIPELINE_STATUS;
SELECT * FROM MONITORING.PIPELINE_STATUS ORDER BY EVENT_TS DESC;


--=============================================================================================================================================================================================


