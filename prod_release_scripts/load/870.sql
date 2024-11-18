Truncate TABLE PROD_DNA_LOAD.THASDL_RAW.sdl_ecom_shopee_compensation;

COPY INTO PROD_DNA_LOAD.THASDL_RAW.sdl_ecom_shopee_compensation
  FROM @PROD_DNA_LOAD.THASDL_RAW.PROD_LOAD_STAGE_ADLS/prd/eCOM_Data/transaction/Shopee_Compensation/Compensation.csv
FILE_FORMAT = (TYPE = CSV FIELD_DELIMITER = ','  SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '\"' );
