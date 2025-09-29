/* ============================================================
   Synapse Serverless SQL – Silver → Gold with pct_verified
   Database : AmazonAwDatabase
   Storage  : abfss://amazon2023data@amazondatastorageaccount.dfs.core.windows.net
   Pattern  : Medallion (Silver in Parquet; Gold via CETAS)
   Security : Synapse Workspace Managed Identity
============================================================ */

------------------------------
-- 0) Database & connectivity
------------------------------
IF DB_ID(N'AmazonAwDatabase') IS NULL
BEGIN
  CREATE DATABASE [AmazonAwDatabase];
END;
GO
USE [AmazonAwDatabase];
GO

IF NOT EXISTS (SELECT 1 FROM sys.database_scoped_credentials WHERE name = N'msi_cred')
  CREATE DATABASE SCOPED CREDENTIAL [msi_cred] WITH IDENTITY = 'Managed Identity';
GO

IF NOT EXISTS (SELECT 1 FROM sys.external_data_sources WHERE name = N'adls_amazon')
  CREATE EXTERNAL DATA SOURCE [adls_amazon]
  WITH (
    LOCATION  = 'abfss://amazon2023data@amazondatastorageaccount.dfs.core.windows.net',
    CREDENTIAL = [msi_cred]
  );
GO

IF NOT EXISTS (SELECT 1 FROM sys.external_file_formats WHERE name = N'parquet_ff')
  CREATE EXTERNAL FILE FORMAT [parquet_ff] WITH (FORMAT_TYPE = PARQUET);
GO


---------------------------------------------
-- 1) (Optional) Inspect Silver quickly
---------------------------------------------
SELECT TOP 100 *
FROM OPENROWSET(
  BULK 'silver/*',
  DATA_SOURCE = 'adls_amazon',
  FORMAT = 'PARQUET'
) AS rows;

SELECT review_year, COUNT(*) AS rows_per_year
FROM OPENROWSET(
  BULK 'silver/*',
  DATA_SOURCE = 'adls_amazon',
  FORMAT = 'PARQUET'
) AS rows
GROUP BY review_year
ORDER BY review_year;


--------------------------------------------------------
-- 2) Strongly typed Silver view with verified_flag
--------------------------------------------------------
/* Normalizes column names (rating/overall, helpful_vote/helpful_votes,
   rating_number/rating_count) and computes a robust verified_flag. */
CREATE OR ALTER VIEW dbo.vw_silver AS
SELECT
    S.review_year,
    S.event_ts,
    COALESCE(S.rating, S.overall)                         AS rating,
    S.product_price,
    COALESCE(S.helpful_vote, S.helpful_votes)             AS helpful_vote,
    COALESCE(S.rating_number, S.rating_count)             AS rating_number,
    S.verified_purchase,
    CASE
      WHEN TRY_CAST(S.verified_purchase AS bit) = 1 THEN 1
      WHEN LOWER(LTRIM(RTRIM(S.verified_purchase))) IN ('true','t','yes','y','1') THEN 1
      ELSE 0
    END AS verified_flag,
    S.parent_asin,
    S.product_title,
    S.main_category,
    S.product_details,
    S.title,
    S.[text]
FROM OPENROWSET(
       BULK 'silver/',
       DATA_SOURCE = 'adls_amazon',
       FORMAT = 'PARQUET'
     )
WITH (
    review_year       int,
    event_ts          datetime2,
    rating            float,
    overall           float,
    product_price     float,
    helpful_vote      int,
    helpful_votes     int,
    rating_number     int,
    rating_count      int,
    verified_purchase nvarchar(20),
    parent_asin       varchar(64),
    product_title     nvarchar(400),
    main_category     nvarchar(200),
    product_details   nvarchar(max),
    title             nvarchar(400),
    [text]            nvarchar(max)
) AS S;
GO


-------------------------------------------------------------
-- 3) GOLD (CETAS) – including pct_verified
--    Ensure gold/<table>/ folders are empty if re-running.
-------------------------------------------------------------

/* 3.1 year_summary */
CREATE EXTERNAL TABLE dbo.ext_gold_year_summary
WITH (LOCATION='gold/year_summary/', DATA_SOURCE=adls_amazon, FILE_FORMAT=parquet_ff)
AS
SELECT
  review_year                                AS year,
  COUNT(*)                                   AS reviews_cnt,
  AVG(CAST(rating AS float))                 AS avg_rating,
  AVG(CASE WHEN rating >= 4 THEN 1.0 ELSE 0 END) AS positive_pct,
  AVG(CASE WHEN rating <= 2 THEN 1.0 ELSE 0 END) AS negative_pct,
  AVG(CAST(verified_flag AS float))          AS pct_verified
FROM dbo.vw_silver
GROUP BY review_year;

/* 3.2 brand_year */
CREATE EXTERNAL TABLE dbo.ext_gold_brand_year
WITH (LOCATION='gold/brand_year/', DATA_SOURCE=adls_amazon, FILE_FORMAT=parquet_ff)
AS
SELECT
  review_year AS year,
  COALESCE(JSON_VALUE(product_details,'$.brand'),'Unknown') AS brand,
  COUNT(*)                                   AS reviews_cnt,
  AVG(CAST(rating AS float))                 AS avg_rating,
  AVG(CAST(verified_flag AS float))          AS pct_verified
FROM dbo.vw_silver
GROUP BY review_year, COALESCE(JSON_VALUE(product_details,'$.brand'),'Unknown');

/* 3.3 category_year */
CREATE EXTERNAL TABLE dbo.ext_gold_category_year
WITH (LOCATION='gold/category_year/', DATA_SOURCE=adls_amazon, FILE_FORMAT=parquet_ff)
AS
SELECT
  review_year   AS year,
  main_category AS category,
  COUNT(*)                                   AS reviews_cnt,
  AVG(CAST(rating AS float))                 AS avg_rating,
  AVG(CAST(verified_flag AS float))          AS pct_verified
FROM dbo.vw_silver
GROUP BY review_year, main_category;

/* 3.4 month_trend */
CREATE EXTERNAL TABLE dbo.ext_gold_month_trend
WITH (LOCATION='gold/month_trend/', DATA_SOURCE=adls_amazon, FILE_FORMAT=parquet_ff)
AS
SELECT
  review_year     AS year,
  MONTH(event_ts) AS month,
  COUNT(*)                                   AS reviews_cnt,
  AVG(CAST(rating AS float))                 AS avg_rating,
  AVG(CAST(verified_flag AS float))          AS pct_verified
FROM dbo.vw_silver
GROUP BY review_year, MONTH(event_ts);

/* 3.5 price_rating (bands) */
CREATE EXTERNAL TABLE dbo.ext_gold_price_rating
WITH (LOCATION='gold/price_rating/', DATA_SOURCE=adls_amazon, FILE_FORMAT=parquet_ff)
AS
SELECT
  review_year AS year,
  CASE
    WHEN product_price IS NULL THEN 'Unknown'
    WHEN product_price < 10  THEN '<$10'
    WHEN product_price < 25  THEN '$10–24.99'
    WHEN product_price < 50  THEN '$25–49.99'
    WHEN product_price < 100 THEN '$50–99.99'
    ELSE '>= $100'
  END AS price_band,
  COUNT(*)                                   AS reviews_cnt,
  AVG(CAST(rating AS float))                 AS avg_rating,
  AVG(CAST(verified_flag AS float))          AS pct_verified
FROM dbo.vw_silver
GROUP BY review_year,
  CASE
    WHEN product_price IS NULL THEN 'Unknown'
    WHEN product_price < 10  THEN '<$10'
    WHEN product_price < 25  THEN '$10–24.99'
    WHEN product_price < 50  THEN '$25–49.99'
    WHEN product_price < 100 THEN '$50–99.99'
    ELSE '>= $100'
  END;

/* 3.6 product_year */
CREATE EXTERNAL TABLE dbo.ext_gold_product_year
WITH (LOCATION='gold/product_year/', DATA_SOURCE=adls_amazon, FILE_FORMAT=parquet_ff)
AS
SELECT
  review_year   AS year,
  parent_asin,
  product_title,
  COUNT(*)                                   AS reviews_cnt,
  AVG(CAST(rating AS float))                 AS avg_rating,
  AVG(CAST(product_price AS float))          AS avg_price,
  AVG(CAST(verified_flag AS float))          AS pct_verified
FROM dbo.vw_silver
GROUP BY review_year, parent_asin, product_title;

/* 3.7 top_reviews (row-level; keep original verified_purchase) */
CREATE EXTERNAL TABLE dbo.ext_gold_top_reviews
WITH (LOCATION='gold/top_reviews/', DATA_SOURCE=adls_amazon, FILE_FORMAT=parquet_ff)
AS
WITH ranked AS (
  SELECT
    review_year AS year, parent_asin, product_title,
    CAST(rating AS float)                     AS rating,
    CAST(TRY_CAST(helpful_vote AS int) AS int) AS helpful_vote,
    title, [text] AS review_text, verified_purchase, event_ts,
    ROW_NUMBER() OVER (
      PARTITION BY review_year
      ORDER BY CAST(TRY_CAST(helpful_vote AS int) AS int) DESC, CAST(rating AS float) DESC
    ) AS rk
  FROM dbo.vw_silver
)
SELECT * FROM ranked WHERE rk <= 100;
GO


---------------------------------------------------
-- 4) Optional: clean views for BI (stable naming)
---------------------------------------------------
CREATE OR ALTER VIEW dbo.gold_year_summary   AS SELECT * FROM dbo.ext_gold_year_summary;
CREATE OR ALTER VIEW dbo.gold_brand_year     AS SELECT * FROM dbo.ext_gold_brand_year;
CREATE OR ALTER VIEW dbo.gold_category_year  AS SELECT * FROM dbo.ext_gold_category_year;
CREATE OR ALTER VIEW dbo.gold_month_trend    AS SELECT * FROM dbo.ext_gold_month_trend;
CREATE OR ALTER VIEW dbo.gold_price_rating   AS SELECT * FROM dbo.ext_gold_price_rating;
CREATE OR ALTER VIEW dbo.gold_product_year   AS SELECT * FROM dbo.ext_gold_product_year;
CREATE OR ALTER VIEW dbo.gold_top_reviews    AS SELECT * FROM dbo.ext_gold_top_reviews;
GO
