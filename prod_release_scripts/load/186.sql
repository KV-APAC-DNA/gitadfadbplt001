use schema VNMSDL_RAW;

CREATE OR REPLACE PROCEDURE VNMSDL_RAW.VN_DMS_DISTRIBUTOR("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '

from snowflake.snowpark.functions import col, lit, current_timestamp, trim, upper
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DecimalType,DateType
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark

def main(session:snowpark.Session, Param):

    #Param=["OUT_CON_DT_VN_20240229000720.csv","VNMSDL_RAW.DEV_LOAD_STAGE_ADLS","dev/dms/dms_source","SDL_VN_DMS_DISTRIBUTOR_DIM"]
    
    try:

        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]

        df_schema = StructType([
            StructField("TERRITORY_DIST", StringType()),
            StructField("MAPPED_SPK", StringType()),
            StructField("DSTRBTR_ID", StringType()),
            StructField("DSTRBTR_TYPE", StringType()),
            StructField("DSTRBTR_NAME", StringType()),
            StructField("DSTRBTR_ADDRESS", StringType()),
            StructField("LONGITUDE", StringType()),
            StructField("LATITUDE", StringType()),
            StructField("REGION", StringType()),
            StructField("PROVINCE", StringType()),
            StructField("ACTIVE", StringType()),
            StructField("ASM_ID", StringType())
        ])

        df = session.read\\
        .schema(df_schema)\\
        .option("skip_header",1)\\
        .option("field_delimiter", "|")\\
        .option("encoding", "ISO-8859-15") \\
        .option("field_optionally_enclosed_by", "\\"") \\
        .option("null_if", "") \\
        .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        # Adding transformations for ACTIVE and ASM_ID columns
        df = df.with_column("ACTIVE", trim(upper(col("ACTIVE"))))\\
               .with_column("ASM_ID", trim(upper(col("ASM_ID"))))\\
               .with_column("CURR_DATE", lit(datetime.now().strftime("%Y%m%d%H%M%S")))\\
               .with_column("RUN_ID", lit(None).cast(DecimalType(14,0)))\\
               .with_column("FILE_NAME", lit(file_name))

        snowdf = df.select(
            "TERRITORY_DIST",
            "MAPPED_SPK",
            "DSTRBTR_ID",
            "DSTRBTR_TYPE",
            "DSTRBTR_NAME",
            "DSTRBTR_ADDRESS",
            "LONGITUDE",
            "LATITUDE",
            "REGION",
            "PROVINCE",
            "ACTIVE",
            "ASM_ID",
            "CURR_DATE",
            "RUN_ID",
            "FILE_NAME"
        )

        # Check if DataFrame is empty
        if snowdf.count() == 0:
            return "No Data in table"

        # Archive
        file_name1=("_").join(file_name.split("_")[0:5])+"_"+datetime.now().strftime("%Y%m%d%H%M%S")
        snowdf.write.copy_into_location("@"+Param[1]+"/"+Param[2]+"/success/"+file_name1,header=True,OVERWRITE=True)

        # Write operation
        snowdf.write.mode("append").save_as_table(stage_name.split(".")[0]+"."+target_table)

        # Success message
        return "Success"
    except KeyError as key_error:
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message

    except pd.errors.MergeError as merge_error:
        error_message = f"DataFrame merging error: {str(merge_error)}"
        return error_message
    
    except Exception as e:
        error_message = f"Error: {str(e)}"
        return error_message
';


create or replace TABLE PROD_DNA_LOAD.VNMSDL_RAW.SDL_VN_DMS_CUSTOMER_DIM (
	SAP_ID VARCHAR(30),
	DSTRBTR_ID VARCHAR(30),
	CNTRY_CODE VARCHAR(10),
	OUTLET_ID VARCHAR(30),
	OUTLET_NAME VARCHAR(100),
	ADDRESS_1 VARCHAR(200),
	ADDRESS_2 VARCHAR(200),
	TELEPHONE VARCHAR(100),
	FAX VARCHAR(20),
	CITY VARCHAR(50),
	POSTCODE VARCHAR(20),
	REGION VARCHAR(20),
	CHANNEL_GROUP VARCHAR(30),
	SUB_CHANNEL VARCHAR(20),
	SALES_ROUTE_ID VARCHAR(30),
	SALES_ROUTE_NAME VARCHAR(100),
	SALES_GROUP VARCHAR(30),
	SALESREP_ID VARCHAR(30),
	SALESREP_NAME VARCHAR(100),
	GPS_LAT VARCHAR(30),
	GPS_LONG VARCHAR(30),
	STATUS VARCHAR(1),
	DISTRICT VARCHAR(30),
	PROVINCE VARCHAR(30),
	CRT_DATE VARCHAR(50),
	DATE_OFF VARCHAR(50),
	CURR_DATE TIMESTAMP_NTZ(9) DEFAULT CONVERT_TIMEZONE('SGT', CAST(CAST('CURRENT_TIMESTAMP()' AS TIMESTAMP_NTZ(9)) AS TIMESTAMP_TZ(9))),
	RUN_ID NUMBER(14,0),
	SOURCE_FILE_NAME VARCHAR(256),
	SHOP_TYPE VARCHAR(100)
);
