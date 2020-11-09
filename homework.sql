	CREATE SCHEMA [grigorenko_schema]
	GO

	CREATE LOGIN grigorenko_log WITH PASSWORD = '*';
	GO


	CREATE USER grigorenko
		FOR LOGIN grigorenko_log
		WITH DEFAULT_SCHEMA = [grigorenko_schema]
	GO

	-- Add user to the database owner role
	EXEC sp_addrolemember N'db_owner', N'grigorenko'
	GO


	-- Create a database scoped credential with Azure storage account key as the secret.
	CREATE DATABASE SCOPED CREDENTIAL GrigorenkoAzureStorageCredential
	WITH
	  IDENTITY = 'bdsgrigorenko' ,
	  SECRET = '9leiUlPw0kYDH0waTfmkBkVAvqm8USKmTDUuUcQS5A3KWchbggfjv95d0ja98f6V0ws06hDNMpelt5kAABh7IA==' ;
	GO

	CREATE EXTERNAL DATA SOURCE GrigorenkoAzureStorage
	WITH
	  ( LOCATION = 'wasbs://traincontainer@bdsgrigorenko.blob.core.windows.net/' ,
		CREDENTIAL = GrigorenkoAzureStorageCredential ,
		TYPE = HADOOP 
	  ) ;

	 GO




CREATE EXTERNAL TABLE [sqldwschool].[grigorenko_schema].[grigorenko_schema.fact_tripdata_external]
    (
	[VendorID] [int] NULL,
	[tpep_pickup_datetime] [datetime] NULL,
	[tpep_dropoff_datetime] [datetime] NULL,
	[passenger_count] [int] NULL,
	[Trip_distance] [real] NULL,
	[RatecodeID] [int] NULL,
	[store_and_fwd_flag] [char](1) NULL,
	[PULocationID] [int] NULL,
	[DOLocationID] [int] NULL,
	[payment_type] [int] NULL,
	[fare_amount] [real] NULL,
	[extra] [real] NULL,
	[mta_tax] [real] NULL,
	[tip_amount] [real] NULL,
	[tolls_amount] [real] NULL,
	[improvement_surcharge] [real] NULL,
	[total_amount] [real] NULL,
	[congestion_surcharge] [real] NULL
)
	WITH 
       (
		LOCATION = 'yellow_tripdata_2020-01.csv',
        DATA_SOURCE = GrigorenkoAzureStorage,
		FILE_FORMAT = CSV
			)
         
	CREATE TABLE [grigorenko_schema.fact_tripdata]
	WITH
	(
	 DISTRIBUTION = HASH([tpep_pickup_datetime])
	 ,CLUSTERED COLUMNSTORE INDEX
	)
	AS
	SELECT  *
	FROM [grigorenko_schema].[grigorenko_schema.fact_tripdata_external];


	CREATE TABLE [grigorenko_schema.vendor]
	(
		[ID] int NULL,
		[Name] VARCHAR(12) NULL
	)
	WITH
	(
	  DISTRIBUTION = REPLICATE 
	 ,CLUSTERED COLUMNSTORE INDEX
	)

	CREATE TABLE [grigorenko_schema.RateCode]
	(
		[ID] int NULL,
		[Name] VARCHAR(12) NULL
	)
	WITH
	(
	  DISTRIBUTION = REPLICATE 
	 ,CLUSTERED COLUMNSTORE INDEX
	)


	CREATE TABLE [grigorenko_schema.Payment_type]
	(
		[ID] int NULL,
		[Name] VARCHAR(12) NULL
	)
	WITH
	(
	  DISTRIBUTION = REPLICATE 
	 ,CLUSTERED COLUMNSTORE INDEX
	)