 CREATE OR REPLACE STORAGE INTEGRATION S3_Integration
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::016065104044:role/snowflake_train'
  STORAGE_ALLOWED_LOCATIONS = ('s3://csv.train.file/','s3://csv.processed/',
    's3://csv.predicted/')
  COMMENT = 'Optional Comment'



  desc integration S3_Integration;


CREATE database sales;
create schema sale;

CREATE OR REPLACE TABLE CLEAN_SALES (
  Weight FLOAT,
  ProductVisibility FLOAT,
  MRP FLOAT,
  OutletSales FLOAT,
  FatContent_Regular BOOLEAN,
  ProductType_Breads BOOLEAN,
  ProductType_Breakfast BOOLEAN,
  ProductType_Canned BOOLEAN,
  ProductType_Dairy BOOLEAN,
  ProductType_Frozen_Foods BOOLEAN,
  ProductType_Fruits_and_Vegetables BOOLEAN,
  ProductType_Hard_Drinks BOOLEAN,
  ProductType_Health_and_Hygiene BOOLEAN,
  ProductType_Household BOOLEAN,
  ProductType_Meat BOOLEAN,
  ProductType_Others BOOLEAN,
  ProductType_Seafood BOOLEAN,
  ProductType_Snack_Foods BOOLEAN,
  ProductType_Soft_Drinks BOOLEAN,
  ProductType_Starchy_Foods BOOLEAN,
  OutletSize_Medium BOOLEAN,
  OutletSize_Small BOOLEAN,
  LocationType_Tier_2 BOOLEAN,
  LocationType_Tier_3 BOOLEAN,
  OutletType_Supermarket_Type1 BOOLEAN,
  OutletType_Supermarket_Type2 BOOLEAN,
  OutletType_Supermarket_Type3 BOOLEAN,
  OutletAge INT
);


select * from CLEAN_SALES;

CREATE STAGE SALES.SALE.STAGE
url='s3://csv.processed/'
 STORAGE_INTEGRATION = S3_Integration

 copy into CLEAN_SALES
 from @STAGE
 FILE_FORMAT = (TYPE = CSV FIELD_DELIMITER = ',' SKIP_HEADER = 1
                 FIELD_OPTIONALLY_ENCLOSED_BY = '"');

CREATE OR REPLACE TABLE PRED_SALES (
  Weight FLOAT,
  ProductVisibility FLOAT,
  MRP FLOAT,
  FatContent_Regular BOOLEAN,
  ProductType_Breads BOOLEAN,
  ProductType_Breakfast BOOLEAN,
  ProductType_Canned BOOLEAN,
  ProductType_Dairy BOOLEAN,
  ProductType_Frozen_Foods BOOLEAN,
  ProductType_Fruits_and_Vegetables BOOLEAN,
  ProductType_Hard_Drinks BOOLEAN,
  ProductType_Health_and_Hygiene BOOLEAN,
  ProductType_Household BOOLEAN,
  ProductType_Meat BOOLEAN,
  ProductType_Others BOOLEAN,
  ProductType_Seafood BOOLEAN,
  ProductType_Snack_Foods BOOLEAN,
  ProductType_Soft_Drinks BOOLEAN,
  ProductType_Starchy_Foods BOOLEAN,
  OutletSize_Medium BOOLEAN,
  OutletSize_Small BOOLEAN,
  LocationType_Tier_2 BOOLEAN,
  LocationType_Tier_3 BOOLEAN,
  OutletType_Supermarket_Type1 BOOLEAN,
  OutletType_Supermarket_Type2 BOOLEAN,
  OutletType_Supermarket_Type3 BOOLEAN,
  OutletAge INT,
  Actual_OutletSales FLOAT,
  Predicted_OutletSales FLOAT
);

CREATE STAGE SALES.SALE.STAGE1
url='s3://csv.predicted/'
 STORAGE_INTEGRATION = S3_Integration

 copy into PRED_SALES
 from @STAGE1
 FILE_FORMAT = (TYPE = CSV FIELD_DELIMITER = ',' SKIP_HEADER = 1
                 FIELD_OPTIONALLY_ENCLOSED_BY = '"');

select * from PRED_SALES;


SELECT COUNT(*) AS clean_rows FROM CLEAN_SALES;
SELECT COUNT(*) AS pred_rows FROM PRED_SALES;

-- Quick peek
SELECT * FROM CLEAN_SALES LIMIT 5;
SELECT * FROM PRED_SALES LIMIT 5;

CREATE OR REPLACE TABLE FINAL_SALES AS
SELECT
  p.MRP,
  p.Weight,
  p.ProductVisibility,
  p.OutletAge,
  p.Actual_OutletSales AS ActualSales,
  p.Predicted_OutletSales AS PredictedSales,
  (p.Predicted_OutletSales - p.Actual_OutletSales) AS Error,
  ABS(p.Predicted_OutletSales - p.Actual_OutletSales) AS AbsError,
  c.OutletSales AS Training_OutletSales,
  c.OutletSize_Medium,
  c.OutletSize_Small,
  c.LocationType_Tier_2,
  c.LocationType_Tier_3,
  c.OutletType_Supermarket_Type1,
  c.OutletType_Supermarket_Type2,
  c.OutletType_Supermarket_Type3
FROM PRED_SALES p
LEFT JOIN CLEAN_SALES c
  ON p.MRP = c.MRP
  AND p.Weight = c.Weight
  AND p.OutletAge = c.OutletAge;


SELECT * FROM FINAL_SALES;

                 

 
