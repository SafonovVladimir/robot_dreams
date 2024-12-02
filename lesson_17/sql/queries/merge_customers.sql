MERGE INTO sep2024-volodymyr-safonov.bronze.customers AS target
USING sep2024-volodymyr-safonov.bronze.staging_customers AS source
ON target.Id = source.Id
WHEN MATCHED THEN
    UPDATE SET
        FirstName = source.FirstName,
        LastName = source.LastName,
        Email = source.Email,
        RegistrationDate = source.RegistrationDate,
        State = source.State
WHEN NOT MATCHED BY TARGET THEN
    INSERT (Id, FirstName, LastName, Email, RegistrationDate, State)
    VALUES (source.Id, source.FirstName, source.LastName, source.Email, source.RegistrationDate, source.State);
