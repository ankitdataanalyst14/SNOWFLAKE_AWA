Swiggy AI_SQL Intelligence Platform


Demonstrating Snowflake AI_SQL (Cortex) Capabilities with Unified Analytics


Project Overview

This document outlines the setup and implementation of the Swiggy AI_SQL Intelligence Platform, showcasing the full capabilities of Snowflake AI_SQL (Cortex) for unified analytics. It includes both AI_SQL implementations (requiring Cortex) and their runnable regular SQL equivalents for environments where Cortex is not yet enabled.


1. Setup: Role, Database, Schema

Create and set the necessary role to  ACCOUNTADMIN .
Create the database  SWIGGY_AI_SQL_DB  and schema  CX_ANALYTICS .
Set the current role, database, and schema to the newly created ones.


2. Create File Format & Stage

Define a CSV file format  FF_SWIGGY_CSV  with specific delimiters and null handling.
Create a Snowflake stage named  SWIGGY_STAGE  using the defined file format.
Instructions are provided for uploading the dataset to the stage.


3. Create Main Table

A table named  SWIGGY_INTERACTIONS  is created with detailed columns to store customer interaction data, including:
Interaction and ticket details
Customer information (ID, name, type)
Location data (region, state, city, pin code)
Order specifics (ID, restaurant, cuisine, value, discounts)
Feedback details (language, text, social posts)
Complaint and resolution status
Pointers to media files (audio, invoices)


4. Load Data from Stage

The  COPY INTO  command is used to load data from the  SWIGGY_STAGE  into the  SWIGGY_INTERACTIONS  table, with error handling set to abort on statement failure.


5. Enable AI_SQL Access

Create an  ANALYST_ROLE  if it does not exist.
Grant the  ANALYST_ROLE  to the specified user (e.g.,  LEARNINGJOURNEY ).
Grant  SNOWFLAKE.CORTEX_USER  role to the  ANALYST_ROLE  to enable AI_SQL functions.
Grant necessary privileges on warehouse, database, schema, and tables to the  ANALYST_ROLE .
Set the current role and database/schema context to  ANALYST_ROLE  and  SWIGGY_AI_SQL_DB.CX_ANALYTICS , respectively.


6. AI_SQL Functions Implementation (All 16)

This section details the implementation of 16 AI_SQL functions, each with a commented-out AI_SQL (Cortex) version and a runnable regular SQL equivalent:

1. AI_SENTIMENT: Detects sentiment from feedback text. Regular SQL uses keyword-based rules.
2. AI_CLASSIFY: Categorizes complaints. Regular SQL uses pattern matching with  ILIKE .
3. AI_FILTER: Identifies critical complaints. Regular SQL uses keyword-based filtering.
4. AI_SUMMARIZE_AGG: Monthly feedback summary. Regular SQL uses  LISTAGG  to combine text snippets.
5. AI_AGG: Summarizes specific regional complaints. Regular SQL aggregates raw text.
6. AI_EMBED: Generates text embeddings (mocked with a fake JSON array in regular SQL).
7. AI_SIMILARITY: Finds semantically similar complaints. Regular SQL approximates similarity using shared keywords.
8. AI_EXTRACT: Extracts structured fields from text. Regular SQL uses  OBJECT_CONSTRUCT  and conditional logic.
9. AI_TRANSLATE: Translates text between languages (mocked to label language in regular SQL).
10. AI_COMPLETE: Generates narrative summaries. Regular SQL provides a static summary based on calculated metrics.
11. AI_AGG + AI_SENTIMENT: Calculates regional sentiment index. Regular SQL uses an average sentiment score based on keywords.
12. AI_TRANSCRIBE: Transcribes audio calls (mocked as a placeholder string).
13. AI_PARSE_DOCUMENT: Extracts text from PDFs (mocked as a placeholder string).
14. AI_COUNT_TOKENS: Estimates token count for text. Regular SQL approximates with word count using  SPLIT  and  ARRAY_SIZE .
15. PROMPT + AI_COMPLETE: Rewrites messages professionally. Regular SQL uses  INITCAP  for basic capitalization.
16. TRY_COMPLETE: Safely generates partner-facing summaries (mocked with a generic message).


7. Business KPIs (Swiggy CX Analytics)

Key performance indicators are calculated using both AI_SQL and regular SQL equivalents:

KPI 1: Regional Sentiment & Complaint Density: Calculates total orders, complaint rate, net sales, and an overview of regional sentiment. The regular SQL version consolidates sentiment scoring within the main aggregation.

KPI 2: Channel-wise Dominant Complaint Class: Identifies the most frequent complaint category per channel. The regular SQL version uses  GREATEST  to find the most prevalent issue based on keyword counts.

The script successfully demonstrates the application of various AI_SQL functions for customer experience analytics at Swiggy. It provides a comprehensive guide for both environments with and without Cortex enabled, ensuring broad applicability.



/***************************************************************************************************
🍽️ PROJECT: SWIGGY AI_SQL INTELLIGENCE PLATFORM (DUAL MODE)
🏢 COMPANY: Swiggy India Pvt Ltd
📍 REGION: AWS_US_WEST_2 | VERSION: 9.36.3 | CORTEX AI_SQL: ❌ NOT ENABLED
💡 PURPOSE:
    - Show FULL AI_SQL (Cortex) usage for Swiggy CX analytics
    - Provide equivalent REGULAR SNOWFLAKE SQL for each use case (runnable today)
***************************************************************************************************/


------------------------------------------
-- 1️⃣ SETUP: ROLE, DATABASE, SCHEMA
------------------------------------------

USE ROLE ACCOUNTADMIN;

CREATE OR REPLACE DATABASE SWIGGY_AI_SQL_DB;
CREATE OR REPLACE SCHEMA SWIGGY_AI_SQL_DB.CX_ANALYTICS;

USE DATABASE SWIGGY_AI_SQL_DB;
USE SCHEMA CX_ANALYTICS;


------------------------------------------
-- 2️⃣ CREATE FILE FORMAT & STAGE
------------------------------------------

CREATE OR REPLACE FILE FORMAT FF_SWIGGY_CSV
  TYPE = CSV
  SKIP_HEADER = 1
  FIELD_DELIMITER = ','
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  NULL_IF = ('', 'NULL');

CREATE OR REPLACE STAGE SWIGGY_STAGE
  FILE_FORMAT = FF_SWIGGY_CSV;

-- TODO: Upload dataset once (from UI or SnowSQL):
-- PUT file://local/path/to/swiggy_ai_sql_unified_dataset.csv @SWIGGY_STAGE;


------------------------------------------
-- 3️⃣ CREATE MAIN TABLE
------------------------------------------

CREATE OR REPLACE TABLE SWIGGY_INTERACTIONS (
    interaction_id              NUMBER,
    ticket_id                   STRING,
    order_id                    STRING,
    interaction_date            DATE,
    customer_id                 NUMBER,
    customer_name               STRING,
    customer_type               STRING,   -- NEW / POWER_USER / CORPORATE / LATE_NIGHT
    channel                     STRING,   -- APP / WEB / CALL_CENTER / WHATSAPP / EMAIL
    region                      STRING,   -- North / South / East / West
    state                       STRING,
    city                        STRING,
    pin_code                    STRING,
    restaurant_name             STRING,
    cuisine_type                STRING,
    order_value                 NUMBER(12,2),
    discount_amount             NUMBER(12,2),
    net_value                   NUMBER(12,2),
    feedback_language           STRING,   -- en / hi / ta / te / bn / mr / kn / ml
    feedback_text               STRING,
    social_post_text            STRING,
    complaint_flag              NUMBER(1), -- 0 / 1
    complaint_category_manual   STRING,
    sentiment_manual            STRING,   -- Positive / Negative / Neutral
    resolution_status           STRING,   -- OPEN / IN_PROGRESS / RESOLVED / ESCALATED
    call_audio_stage_path       STRING,   -- @swiggy_media_stage/calls/...
    invoice_pdf_stage_path      STRING    -- @swiggy_media_stage/invoices/...
);


------------------------------------------
-- 4️⃣ LOAD DATA FROM STAGE
------------------------------------------

COPY INTO SWIGGY_INTERACTIONS
FROM @SWIGGY_STAGE/swiggy_ai_sql_unified_dataset.csv
FILE_FORMAT = (FORMAT_NAME = FF_SWIGGY_CSV)
ON_ERROR = 'ABORT_STATEMENT';


------------------------------------------
-- 5️⃣ ROLE & PRIVILEGES (NO CORTEX YET)
------------------------------------------

CREATE ROLE IF NOT EXISTS ANALYST_ROLE;

-- Adjust username if needed
GRANT ROLE ANALYST_ROLE TO USER <username>;

-- ❌ Cortex role DOES NOT exist in your account yet; keep for future:
-- GRANT DATABASE ROLE SNOWFLAKE.CORTEX_USER TO ROLE ANALYST_ROLE;

-- Warehouse / DB / Schema access
GRANT USAGE ON WAREHOUSE COMPUTE_WH TO ROLE ANALYST_ROLE;
GRANT USAGE ON DATABASE SWIGGY_AI_SQL_DB TO ROLE ANALYST_ROLE;
GRANT USAGE ON SCHEMA SWIGGY_AI_SQL_DB.CX_ANALYTICS TO ROLE ANALYST_ROLE;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA SWIGGY_AI_SQL_DB.CX_ANALYTICS TO ROLE ANALYST_ROLE;
GRANT CREATE TABLE ON SCHEMA SWIGGY_AI_SQL_DB.CX_ANALYTICS TO ROLE ANALYST_ROLE;

USE ROLE ANALYST_ROLE;
USE DATABASE SWIGGY_AI_SQL_DB;
USE SCHEMA CX_ANALYTICS;


/***************************************************************************************************
   🔥 6️⃣ AI_SQL FUNCTIONS IMPLEMENTATION (All 16)
   For EACH block:
      - AI_SQL version (commented, needs Cortex)
      - Regular SQL equivalent (runnable now)
***************************************************************************************************/


/***************************************************************************************************
(1) AI_SENTIMENT — Detect sentiment of customer feedback
***************************************************************************************************/

-- ⚙️ AI_SQL VERSION (CORTEX REQUIRED ❌)
-- SELECT
--   interaction_id,
--   city,
--   channel,
--   feedback_text,
--   sentiment_manual,
--   AI_SENTIMENT(feedback_text) AS sentiment_ai
-- FROM SWIGGY_INTERACTIONS
-- SAMPLE (0.10);

-- ✅ REGULAR SNOWFLAKE SQL EQUIVALENT (KEYWORD-BASED SENTIMENT)
SELECT
  interaction_id,
  city,
  channel,
  feedback_text,
  sentiment_manual,
  CASE
    WHEN feedback_text ILIKE '%horrible%' OR feedback_text ILIKE '%worst%' 
      OR feedback_text ILIKE '%late%' OR feedback_text ILIKE '%cold%' THEN 'Negative'
    WHEN feedback_text ILIKE '%awesome%' OR feedback_text ILIKE '%great%' 
      OR feedback_text ILIKE '%hot and fresh%' THEN 'Positive'
    ELSE 'Neutral'
  END AS sentiment_ai_rule
FROM SWIGGY_INTERACTIONS
SAMPLE (0.10);


 /***************************************************************************************************
(2) AI_CLASSIFY — Categorize complaints automatically
***************************************************************************************************/

-- ⚙️ AI_SQL VERSION (CORTEX REQUIRED ❌)
-- SELECT
--   interaction_id,
--   feedback_text,
--   complaint_category_manual,
--   AI_CLASSIFY(
--       feedback_text,
--       ['Late Delivery','Cold Food','Wrong Item','Refund Delay',
--        'App Crash','Rude Partner','Restaurant Hygiene','General Feedback']
--   ) AS complaint_category_ai
-- FROM SWIGGY_INTERACTIONS
-- WHERE complaint_flag = 1
-- SAMPLE (0.20);

-- ✅ REGULAR SQL EQUIVALENT (RULE-BASED CATEGORIZATION)
SELECT
  interaction_id,
  feedback_text,
  complaint_category_manual,
  CASE
    WHEN feedback_text ILIKE '%late%' OR feedback_text ILIKE '%delay%' THEN 'Late Delivery'
    WHEN feedback_text ILIKE '%cold%' OR feedback_text ILIKE '%not hot%' THEN 'Cold Food'
    WHEN feedback_text ILIKE '%wrong item%' OR feedback_text ILIKE '%different item%' THEN 'Wrong Item'
    WHEN feedback_text ILIKE '%refund%' OR feedback_text ILIKE '%money back%' THEN 'Refund Delay'
    WHEN feedback_text ILIKE '%app%' OR feedback_text ILIKE '%crash%' OR feedback_text ILIKE '%bug%' THEN 'App Crash'
    WHEN feedback_text ILIKE '%rude%' OR feedback_text ILIKE '%impolite%' THEN 'Rude Partner'
    WHEN feedback_text ILIKE '%hygiene%' OR feedback_text ILIKE '%dirty%' OR feedback_text ILIKE '%unclean%' THEN 'Restaurant Hygiene'
    ELSE 'General Feedback'
  END AS complaint_category_rule
FROM SWIGGY_INTERACTIONS SAMPLE (0.20)
WHERE complaint_flag = 1;


/***************************************************************************************************
(3) AI_FILTER — Find severe or critical complaints
***************************************************************************************************/

-- ⚙️ AI_SQL VERSION (CORTEX REQUIRED ❌)
-- SELECT
--   interaction_id,
--   city,
--   feedback_text
-- FROM SWIGGY_INTERACTIONS
-- WHERE AI_FILTER(
--         PROMPT(
--           'Return TRUE if this feedback describes food safety issue, hygiene concern, or refund/refund delay problem: {0}',
--           feedback_text
--         )
--       ) = TRUE;

-- ✅ REGULAR SQL EQUIVALENT (BOOLEAN FILTER USING KEYWORDS)
SELECT
  interaction_id,
  city,
  feedback_text
FROM SWIGGY_INTERACTIONS
WHERE feedback_text ILIKE '%food poisoning%'
   OR feedback_text ILIKE '%stale%'
   OR feedback_text ILIKE '%smell%'
   OR feedback_text ILIKE '%refund%'
   OR feedback_text ILIKE '%hygiene%'
   OR feedback_text ILIKE '%dirty%';


/***************************************************************************************************
(4) AI_SUMMARIZE_AGG — Monthly summary of national feedback
***************************************************************************************************/

-- ⚙️ AI_SQL VERSION (CORTEX REQUIRED ❌)
-- SELECT
--   DATE_TRUNC('month', interaction_date) AS month,
--   AI_SUMMARIZE_AGG(feedback_text)      AS feedback_summary
-- FROM SWIGGY_INTERACTIONS
-- GROUP BY month
-- ORDER BY month;

-- ✅ REGULAR SQL EQUIVALENT (LISTAGG SHORT SNIPPETS)
SELECT
  DATE_TRUNC('month', interaction_date) AS month,
  LISTAGG(SUBSTR(feedback_text, 1, 120), ' || ') AS feedback_summary_rule
FROM SWIGGY_INTERACTIONS
GROUP BY month
ORDER BY month;


/***************************************************************************************************
(5) AI_AGG — Summarize South India delivery complaints
***************************************************************************************************/

-- ⚙️ AI_SQL VERSION (CORTEX REQUIRED ❌)
-- SELECT
--   AI_AGG(
--     feedback_text,
--     'Summarize top 5 customer complaints in South India related to late delivery and cold food.'
--   ) AS south_complaint_summary
-- FROM SWIGGY_INTERACTIONS
-- WHERE region = 'South'
--   AND complaint_flag = 1;

-- ✅ REGULAR SQL EQUIVALENT (AGGREGATE RAW TEXT)
SELECT
  LISTAGG(SUBSTR(feedback_text, 1, 150), ' || ') AS south_complaint_summary_rule
FROM SWIGGY_INTERACTIONS
WHERE region = 'South'
  AND complaint_flag = 1;


/***************************************************************************************************
(6) AI_EMBED — Example embedding (not available; mock only)
***************************************************************************************************/

-- ⚙️ AI_SQL VERSION (CORTEX REQUIRED ❌ AND NOT ACTIVE)
-- SELECT
--   AI_EMBED('This is an example Swiggy customer complaint about late delivery and cold biryani.')
--     AS example_embedding_sample;

-- ✅ REGULAR SQL MOCK (FAKE EMBEDDING VECTOR)
SELECT
  PARSE_JSON('[0.12, 0.44, 0.88, 0.37]') AS example_embedding_mock;


/***************************************************************************************************
(7) AI_SIMILARITY — Find semantically similar complaints
***************************************************************************************************/

-- ⚙️ AI_SQL VERSION (CORTEX REQUIRED ❌)
-- SET seed_interaction_id = 500;
--
-- WITH seed AS (
--   SELECT feedback_text AS seed_text
--   FROM SWIGGY_INTERACTIONS
--   WHERE interaction_id = $seed_interaction_id
-- ),
-- scored AS (
--   SELECT
--     s.interaction_id,
--     s.complaint_category_manual,
--     s.feedback_text,
--     AI_SIMILARITY(s.feedback_text, seed.seed_text) AS similarity_score
--   FROM SWIGGY_INTERACTIONS s
--   CROSS JOIN seed
--   WHERE s.complaint_flag = 1
--     AND s.interaction_id <> $seed_interaction_id
-- )
-- SELECT *
-- FROM scored
-- ORDER BY similarity_score DESC
-- LIMIT 10;

-- ✅ REGULAR SQL APPROXIMATION (FIND SIMILAR BY SIMPLE TEXT HEURISTICS)
SET seed_interaction_id = 500;

WITH seed AS (
  SELECT feedback_text AS seed_text
  FROM SWIGGY_INTERACTIONS
  WHERE interaction_id = $seed_interaction_id
),
scored AS (
  SELECT
    s.interaction_id,
    s.complaint_category_manual,
    s.feedback_text,
    -- crude "similarity": count of shared keywords (delay, cold, refund, hygiene)
    (
      (CASE WHEN s.feedback_text ILIKE '%late%' AND seed.seed_text ILIKE '%late%' THEN 1 ELSE 0 END) +
      (CASE WHEN s.feedback_text ILIKE '%cold%' AND seed.seed_text ILIKE '%cold%' THEN 1 ELSE 0 END) +
      (CASE WHEN s.feedback_text ILIKE '%refund%' AND seed.seed_text ILIKE '%refund%' THEN 1 ELSE 0 END) +
      (CASE WHEN s.feedback_text ILIKE '%hygiene%' AND seed.seed_text ILIKE '%hygiene%' THEN 1 ELSE 0 END)
    ) AS similarity_score_rule
  FROM SWIGGY_INTERACTIONS s
  CROSS JOIN seed
  WHERE s.complaint_flag = 1
    AND s.interaction_id <> $seed_interaction_id
)
SELECT *
FROM scored
ORDER BY similarity_score_rule DESC
LIMIT 10;


/***************************************************************************************************
(8) AI_EXTRACT — Extract structured fields from free-text complaints
***************************************************************************************************/

-- ⚙️ AI_SQL VERSION (CORTEX REQUIRED ❌)
-- SELECT
--   interaction_id,
--   feedback_text,
--   AI_EXTRACT(
--     text => feedback_text,
--     responseFormat => PARSE_JSON(
--       '[
--         "issue_type: What is the main issue in this complaint? (Late Delivery, Cold Food, Wrong Item, Hygiene, Refund, Other)",
--         "delay_minutes: How many minutes of delay are mentioned, if any?",
--         "restaurant_name: Which restaurant is mentioned, if any?",
--         "mentions_refund: Does the customer mention refund, compensation, or money back?"
--       ]'
--     )
--   ) AS extracted_info
-- FROM SWIGGY_INTERACTIONS
-- WHERE complaint_flag = 1
-- SAMPLE (0.10);

-- ✅ REGULAR SQL EQUIVALENT (OBJECT_CONSTRUCT + RULES)
SELECT
  interaction_id,
  feedback_text,
  OBJECT_CONSTRUCT(
    'issue_type',
      CASE
        WHEN feedback_text ILIKE '%late%' THEN 'Late Delivery'
        WHEN feedback_text ILIKE '%cold%' THEN 'Cold Food'
        WHEN feedback_text ILIKE '%refund%' THEN 'Refund'
        WHEN feedback_text ILIKE '%hygiene%' OR feedback_text ILIKE '%dirty%' THEN 'Hygiene'
        ELSE 'Other'
      END,
    'delay_minutes',
      CASE
        WHEN feedback_text ILIKE '%30 min%' THEN 30
        WHEN feedback_text ILIKE '%45 min%' THEN 45
        ELSE NULL
      END,
    'mentions_refund',
      (feedback_text ILIKE '%refund%' OR feedback_text ILIKE '%money back%')
  ) AS extracted_info_rule
FROM SWIGGY_INTERACTIONS SAMPLE (0.10)
WHERE complaint_flag = 1;


/***************************************************************************************************
(9) AI_TRANSLATE — Handle multilingual feedback
***************************************************************************************************/

-- ⚙️ AI_SQL VERSION (CORTEX REQUIRED ❌)
-- SELECT
--   interaction_id,
--   feedback_language,
--   feedback_text,
--   AI_TRANSLATE(feedback_text, '', 'en') AS feedback_text_en
-- FROM SWIGGY_INTERACTIONS
-- WHERE feedback_language <> 'en'
-- SAMPLE (0.10);

-- ✅ REGULAR SQL MOCK (LABEL LANGUAGE)
SELECT
  interaction_id,
  feedback_language,
  feedback_text,
  CASE
    WHEN feedback_language = 'en' THEN feedback_text
    ELSE CONCAT('[Translated from ', feedback_language, ' – AI not enabled]')
  END AS feedback_text_en_mock
FROM SWIGGY_INTERACTIONS SAMPLE (0.10)
WHERE feedback_language <> 'en';


/***************************************************************************************************
(10) AI_COMPLETE — Generate narrative for leadership summary
***************************************************************************************************/

-- ⚙️ AI_SQL VERSION (CORTEX REQUIRED ❌)
-- WITH metrics AS (
--   SELECT
--     COUNT_IF(complaint_flag = 1) AS total_complaints,
--     ROUND(
--       100.0 * COUNT_IF(sentiment_manual = 'Negative') / NULLIF(COUNT(*), 0),
--       2
--     ) AS negative_share_pct
--   FROM SWIGGY_INTERACTIONS
-- )
-- SELECT
--   AI_COMPLETE(
--     'openai-gpt-4.1',
--     OBJECT_CONSTRUCT(
--       'prompt',
--       CONCAT(
--         'You are Swiggy’s CX Head for India. ',
--         'Summarize this month''s national customer sentiment. ',
--         'Highlight cities with maximum complaints and appreciation. ',
--         'Total complaints: ', total_complaints,
--         ', Negative Sentiment Share: ', negative_share_pct,
--         '%. Suggest 3 concrete actions to improve delivery performance and food quality.'
--       )
--     )
--   ) AS monthly_cx_summary
-- FROM metrics;

-- ✅ REGULAR SQL EQUIVALENT (STATIC SUMMARY BASED ON METRICS)
WITH metrics AS (
  SELECT
    COUNT_IF(complaint_flag = 1) AS total_complaints,
    ROUND(
      100.0 * COUNT_IF(sentiment_manual = 'Negative') / NULLIF(COUNT(*), 0),
      2
    ) AS negative_share_pct
  FROM SWIGGY_INTERACTIONS
)
SELECT
  CONCAT(
    'CX Summary: Total complaints = ', total_complaints,
    ', Negative share = ', negative_share_pct,
    '%. Key actions: improve on-time delivery, strengthen refund SLAs, and enforce hygiene audits in top cities.'
  ) AS monthly_cx_summary_rule
FROM metrics;


/***************************************************************************************************
(11) AI_AGG + AI_SENTIMENT — Regional sentiment index
***************************************************************************************************/

-- ⚙️ AI_SQL VERSION (CORTEX REQUIRED ❌)
-- WITH region_text AS (
--   SELECT
--     region,
--     AI_SUMMARIZE_AGG(feedback_text) AS region_text_summary
--   FROM SWIGGY_INTERACTIONS
--   GROUP BY region
-- )
-- SELECT
--   region,
--   AI_SENTIMENT(region_text_summary) AS region_sentiment
-- FROM region_text
-- ORDER BY region;

-- ✅ REGULAR SQL EQUIVALENT
WITH region_scores AS (
  SELECT
    region,
    AVG(
      CASE
        WHEN feedback_text ILIKE '%bad%' OR feedback_text ILIKE '%late%' OR feedback_text ILIKE '%cold%' THEN -1
        WHEN feedback_text ILIKE '%good%' OR feedback_text ILIKE '%awesome%' OR feedback_text ILIKE '%hot and fresh%' THEN 1
        ELSE 0
      END
    ) AS sentiment_score
  FROM SWIGGY_INTERACTIONS
  GROUP BY region
)
SELECT
  region,
  CASE
    WHEN sentiment_score > 0.2 THEN 'Positive'
    WHEN sentiment_score < -0.2 THEN 'Negative'
    ELSE 'Mixed'
  END AS region_sentiment_rule
FROM region_scores
ORDER BY region;


/***************************************************************************************************
(12) AI_TRANSCRIBE — Transcribe customer care audio calls (Mock)
***************************************************************************************************/

-- ⚙️ AI_SQL VERSION (CORTEX + MEDIA STAGE REQUIRED ❌)
-- SELECT
--   interaction_id,
--   call_audio_stage_path,
--   AI_TRANSCRIBE(
--     TO_FILE('@swiggy_media_stage', SPLIT_PART(call_audio_stage_path, '/', -1))
--   ) AS transcript
-- FROM SWIGGY_INTERACTIONS
-- WHERE channel = 'CALL_CENTER'
--   AND call_audio_stage_path IS NOT NULL
-- SAMPLE (0.05);

-- ✅ REGULAR SQL MOCK
SELECT
  interaction_id,
  call_audio_stage_path,
  '[Transcript not available – Cortex media not enabled]' AS transcript_mock
FROM SWIGGY_INTERACTIONS 
WHERE channel = 'CALL_CENTER' --SAMPLE (0.05)
  AND call_audio_stage_path IS NOT NULL;


/***************************************************************************************************
(13) AI_PARSE_DOCUMENT — Extract text from PDF invoices (Mock)
***************************************************************************************************/

-- ⚙️ AI_SQL VERSION (CORTEX + MEDIA STAGE REQUIRED ❌)
-- SELECT
--   interaction_id,
--   invoice_pdf_stage_path,
--   AI_PARSE_DOCUMENT(
--     '@swiggy_media_stage',
--     SPLIT_PART(invoice_pdf_stage_path, '/', -1),
--     OBJECT_CONSTRUCT('mode','LAYOUT')
--   ) AS invoice_parsed
-- FROM SWIGGY_INTERACTIONS
-- WHERE invoice_pdf_stage_path IS NOT NULL
-- SAMPLE (0.01);

-- ✅ REGULAR SQL MOCK
SELECT
  interaction_id,
  invoice_pdf_stage_path,
  '[Invoice text extraction simulated – Cortex document AI not enabled]' AS invoice_parsed_mock
FROM SWIGGY_INTERACTIONS SAMPLE (0.01)
WHERE invoice_pdf_stage_path IS NOT NULL;


/***************************************************************************************************
(14) AI_COUNT_TOKENS — Estimate tokens for a North-region narrative
***************************************************************************************************/

-- ⚙️ AI_SQL VERSION (CORTEX REQUIRED ❌)
-- SELECT
--   AI_COUNT_TOKENS(
--     'ai_complete',
--     'llama3.3-70b',
--     (
--       SELECT AI_AGG(
--                feedback_text,
--                'Combine North India feedback into a single paragraph for CX leadership.'
--              )
--       FROM SWIGGY_INTERACTIONS
--       WHERE region = 'North'
--     )
--   ) AS token_count_estimate;

-- ✅ REGULAR SQL EQUIVALENT (WORD COUNT)
WITH north_text AS (
  SELECT
    LISTAGG(SUBSTR(feedback_text, 1, 200), ' ') AS combined_text
  FROM SWIGGY_INTERACTIONS
  WHERE region = 'North'
)
SELECT
  ARRAY_SIZE(SPLIT(combined_text, ' ')) AS token_count_estimate_rule
FROM north_text;



/***************************************************************************************************
(15) PROMPT + AI_COMPLETE — Rewrite customer message professionally
***************************************************************************************************/

-- ⚙️ AI_SQL VERSION (CORTEX REQUIRED ❌)
-- WITH samples AS (
--   SELECT interaction_id, feedback_text
--   FROM SWIGGY_INTERACTIONS
--   SAMPLE (0.01)
-- )
-- SELECT
--   interaction_id,
--   AI_COMPLETE(
--     'openai-gpt-4.1',
--     PROMPT(
--       'Rewrite the following customer feedback professionally for the Swiggy CX dashboard: {0}',
--       feedback_text
--     )
--   ) AS rewritten_feedback
-- FROM samples;

-- ✅ REGULAR SQL EQUIVALENT (SIMPLIFIED REWRITE)
WITH samples AS (
  SELECT interaction_id, feedback_text
  FROM SWIGGY_INTERACTIONS
  SAMPLE (0.01)
)
SELECT
  interaction_id,
  INITCAP(feedback_text) AS rewritten_feedback_rule
FROM samples;


/***************************************************************************************************
(16) TRY_COMPLETE — Safe execution for partner-facing summary (Mock)
***************************************************************************************************/

-- ⚙️ AI_SQL VERSION (CORTEX REQUIRED ❌)
-- SELECT
--   interaction_id,
--   SNOWFLAKE.CORTEX.TRY_COMPLETE(
--     'openai-gpt-4.1',
--     CONCAT(
--       'Summarize this complaint in one short, polite sentence suitable to show to a Swiggy delivery partner: ',
--       feedback_text
--     )
--   ) AS safe_partner_reply
-- FROM SWIGGY_INTERACTIONS
-- SAMPLE (0.01);

-- ✅ REGULAR SQL MOCK
SELECT
  interaction_id,
  '[Partner-safe message: Please review this order issue carefully and assist the customer politely.]' 
    AS safe_partner_reply_mock
FROM SWIGGY_INTERACTIONS
SAMPLE (0.01);



/***************************************************************************************************
   7️⃣ BUSINESS KPIs (Swiggy CX Analytics) – AI_SQL + Regular SQL
***************************************************************************************************/


-- KPI 1: Regional sentiment & complaint density
-- ⚙️ AI_SQL VERSION (CORTEX REQUIRED ❌)
-- WITH region_stats AS (
--   SELECT
--     region,
--     COUNT(*)                                  AS total_orders,
--     COUNT_IF(complaint_flag = 1)             AS complaints,
--     ROUND(100.0 * COUNT_IF(complaint_flag = 1) / NULLIF(COUNT(*),0), 2)
--       AS complaint_rate_pct,
--     SUM(net_value)                           AS total_net_sales
--   FROM SWIGGY_INTERACTIONS
--   GROUP BY region
-- ),
-- region_text AS (
--   SELECT
--     region,
--     AI_SUMMARIZE_AGG(feedback_text) AS region_feedback_summary
--   FROM SWIGGY_INTERACTIONS
--   GROUP BY region
-- )
-- SELECT
--   rs.region,
--   rs.total_orders,
--   rs.complaints,
--   rs.complaint_rate_pct,
--   ROUND(rs.total_net_sales, 2) AS total_net_sales,
--   AI_SENTIMENT(rt.region_feedback_summary) AS sentiment_overview
-- FROM region_stats rs
-- JOIN region_text rt
--   ON rs.region = rt.region
-- ORDER BY rs.total_net_sales DESC;

-- ✅ REGULAR SQL EQUIVALENT
WITH region_stats AS (
  SELECT
    region,
    COUNT(*)                                  AS total_orders,
    COUNT_IF(complaint_flag = 1)             AS complaints,
    ROUND(100.0 * COUNT_IF(complaint_flag = 1) / NULLIF(COUNT(*),0), 2)
      AS complaint_rate_pct,
    SUM(net_value)                           AS total_net_sales,
    AVG(
      CASE
        WHEN feedback_text ILIKE '%bad%' OR feedback_text ILIKE '%late%' OR feedback_text ILIKE '%cold%' THEN -1
        WHEN feedback_text ILIKE '%good%' OR feedback_text ILIKE '%awesome%' OR feedback_text ILIKE '%hot and fresh%' THEN 1
        ELSE 0
      END
    ) AS sentiment_score
  FROM SWIGGY_INTERACTIONS
  GROUP BY region
)
SELECT
  region,
  total_orders,
  complaints,
  complaint_rate_pct,
  ROUND(total_net_sales, 2) AS total_net_sales,
  CASE
    WHEN sentiment_score > 0.2 THEN 'Positive'
    WHEN sentiment_score < -0.2 THEN 'Negative'
    ELSE 'Mixed'
  END AS sentiment_overview_rule
FROM region_stats
ORDER BY total_net_sales DESC;


-- KPI 2: Channel-wise dominant complaint class
-- ⚙️ AI_SQL VERSION (CORTEX REQUIRED ❌)
-- SELECT
--   channel,
--   AI_CLASSIFY(
--     AI_SUMMARIZE_AGG(feedback_text),
--     ['Late Delivery','Cold Food','Wrong Item',
--      'Refund Delay','App Crash','Partner Behaviour']
--   ) AS top_issue
-- FROM SWIGGY_INTERACTIONS
-- GROUP BY channel;

-- ✅ REGULAR SQL EQUIVALENT
SELECT
  channel,
  CASE
    WHEN SUM(CASE WHEN feedback_text ILIKE '%late%' THEN 1 ELSE 0 END) =
         GREATEST(
           SUM(CASE WHEN feedback_text ILIKE '%late%' THEN 1 ELSE 0 END),
           SUM(CASE WHEN feedback_text ILIKE '%cold%' THEN 1 ELSE 0 END),
           SUM(CASE WHEN feedback_text ILIKE '%refund%' THEN 1 ELSE 0 END),
           SUM(CASE WHEN feedback_text ILIKE '%app%' THEN 1 ELSE 0 END),
           SUM(CASE WHEN feedback_text ILIKE '%rude%' THEN 1 ELSE 0 END)
         ) THEN 'Late Delivery'
    WHEN SUM(CASE WHEN feedback_text ILIKE '%cold%' THEN 1 ELSE 0 END) =
         GREATEST(
           SUM(CASE WHEN feedback_text ILIKE '%late%' THEN 1 ELSE 0 END),
           SUM(CASE WHEN feedback_text ILIKE '%cold%' THEN 1 ELSE 0 END),
           SUM(CASE WHEN feedback_text ILIKE '%refund%' THEN 1 ELSE 0 END),
           SUM(CASE WHEN feedback_text ILIKE '%app%' THEN 1 ELSE 0 END),
           SUM(CASE WHEN feedback_text ILIKE '%rude%' THEN 1 ELSE 0 END)
         ) THEN 'Cold Food'
    WHEN SUM(CASE WHEN feedback_text ILIKE '%refund%' THEN 1 ELSE 0 END) =
         GREATEST(
           SUM(CASE WHEN feedback_text ILIKE '%late%' THEN 1 ELSE 0 END),
           SUM(CASE WHEN feedback_text ILIKE '%cold%' THEN 1 ELSE 0 END),
           SUM(CASE WHEN feedback_text ILIKE '%refund%' THEN 1 ELSE 0 END),
           SUM(CASE WHEN feedback_text ILIKE '%app%' THEN 1 ELSE 0 END),
           SUM(CASE WHEN feedback_text ILIKE '%rude%' THEN 1 ELSE 0 END)
         ) THEN 'Refund Delay'
    WHEN SUM(CASE WHEN feedback_text ILIKE '%app%' THEN 1 ELSE 0 END) =
         GREATEST(
           SUM(CASE WHEN feedback_text ILIKE '%late%' THEN 1 ELSE 0 END),
           SUM(CASE WHEN feedback_text ILIKE '%cold%' THEN 1 ELSE 0 END),
           SUM(CASE WHEN feedback_text ILIKE '%refund%' THEN 1 ELSE 0 END),
           SUM(CASE WHEN feedback_text ILIKE '%app%' THEN 1 ELSE 0 END),
           SUM(CASE WHEN feedback_text ILIKE '%rude%' THEN 1 ELSE 0 END)
         ) THEN 'App Crash'
    ELSE 'Partner Behaviour'
  END AS top_issue_rule
FROM SWIGGY_INTERACTIONS
GROUP BY channel;


---------------------------------------------------------------------------------------------------
✅ END OF MASTER SCRIPT
- AI_SQL blocks: commented, for future Cortex-enabled environment.
- Regular SQL blocks: fully runnable today in AWS_US_WEST_2, Snowflake 9.36.3.
---------------------------------------------------------------------------------------------------

