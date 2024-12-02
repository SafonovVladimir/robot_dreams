MERGE INTO sep2024-volodymyr-safonov.bronze.sales AS target
USING sep2024-volodymyr-safonov.bronze.staging_sales AS source
ON target.CustomerId = source.CustomerId
   AND target.PurchaseDate = source.PurchaseDate
   AND target.Product = source.Product
   AND target.Price = source.Price
WHEN NOT MATCHED BY TARGET THEN
    INSERT (CustomerId, PurchaseDate, Product, Price)
    VALUES (source.CustomerId, source.PurchaseDate, source.Product, source.Price)