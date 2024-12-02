CREATE OR REPLACE TABLE sep2024-volodymyr-safonov.silver.customers AS
SELECT
    SAFE_CAST(Id AS INT64) AS client_id,
    SAFE_CAST(FirstName AS STRING) AS first_name,
    SAFE_CAST(LastName AS STRING) AS last_name,
    SAFE_CAST(Email AS STRING) AS email,
    SAFE.PARSE_DATE("%Y-%m-%d", RegistrationDate) AS registration_date,
    SAFE_CAST(State AS STRING) AS state
FROM sep2024-volodymyr-safonov.bronze.customers
WHERE
    Id IS NOT NULL
    AND Email IS NOT NULL
    AND RegistrationDate IS NOT NULL