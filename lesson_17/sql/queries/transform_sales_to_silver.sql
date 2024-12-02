CREATE OR REPLACE TABLE sep2024-volodymyr-safonov.silver.sales
PARTITION BY purchase_date AS
SELECT
    SAFE_CAST(CustomerId AS INT64) AS client_id,
    SAFE.PARSE_DATE("%Y-%m-%d", PurchaseDate) AS purchase_date,
    SAFE_CAST(Product AS STRING) AS product_name,
    SAFE_CAST(REGEXP_REPLACE(Price, r"[^0-9.]", "") AS FLOAT64) AS price
FROM sep2024-volodymyr-safonov.bronze.sales
WHERE
    CustomerId IS NOT NULL
    AND PurchaseDate IS NOT NULL
    AND Price IS NOT NULL
    AND Product IS NOT NULL