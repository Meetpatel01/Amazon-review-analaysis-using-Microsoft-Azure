-- Create a new database
CREATE DATABASE AmazonReviewAnalytics;
GO

-- Switch to the new database
USE AmazonReviewAnalytics;
GO

-- Create master key (required for database-scoped credentials)
IF NOT EXISTS (SELECT * FROM sys.symmetric_keys WHERE name = '##MS_DatabaseMasterKey##')
BEGIN
    CREATE MASTER KEY ENCRYPTION BY PASSWORD = 'rishiAzure2025';
END

-- Create database scoped credential for managed identity
CREATE DATABASE SCOPED CREDENTIAL [DataLakeCredential]
WITH IDENTITY = 'Managed Identity';
GO

-- Create external data source pointing to your data lake
CREATE EXTERNAL DATA SOURCE [DataLakeDataSource]
WITH (
    LOCATION = 'abfss://datalake@amzdata20376.dfs.core.windows.net/',
    CREDENTIAL = [DataLakeCredential]
);
GO

-- Create external file format for Delta files
CREATE EXTERNAL FILE FORMAT [DeltaFormat]
WITH (
    FORMAT_TYPE = DELTA
);
GO