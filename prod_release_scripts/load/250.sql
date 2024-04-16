USE SCHEMA VNMSDL_RAW;

CREATE OR REPLACE PROCEDURE VNMSDL_RAW.VN_DMS_DISTRIBUTOR("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS 'from snowflake.snowpark.functions import col, lit, current_timestamp, trim, upper
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DecimalType,DateType
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz

def main(session:snowpark.Session,Param):

    #Param=["OUT_CON_DT_VN_20240405000736.csv","VNMSDL_RAW.DEV_LOAD_STAGE_ADLS","dev/dms/dms_source","SDL_VN_DMS_DISTRIBUTOR_DIM"]
    
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
        .option("encoding", "UTF-8") \\
        .option("field_optionally_enclosed_by", "\\"") \\
        .option("null_if", "") \\
        .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        # Adding transformations for ACTIVE and ASM_ID columns
        df = df.withColumn("CURR_DATE",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))\\
               .with_column("RUN_ID", lit(lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S"))).cast(DecimalType(14,0)))\\
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

        # Write operation
        snowdf.write.mode("append").save_as_table(stage_name.split(".")[0]+"."+target_table)

        # Archive
        file_name1=("_").join(file_name.split("_")[0:5])+"_"+datetime.now().strftime("%Y%m%d%H%M%S")
        snowdf.write.copy_into_location("@"+Param[1]+"/"+Param[2]+"/success/"+file_name1,header=True,OVERWRITE=True)


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
        return error_message';


CREATE OR REPLACE PROCEDURE VNMSDL_RAW.SDL_VN_GT_TOPDOOR_TARGET("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '
from snowflake.snowpark.functions import col, lit, current_timestamp, trim, upper, right
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DecimalType,DateType
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz
def main(session:snowpark.Session, Param):
    try:
        file_name       = Param[0]  # File name
        stage_name      = Param[1]  # Stage name where files are located
        temp_stage_path = Param[2]  # Path in the stage
        target_table    = Param[3]
        # Define the schema based on the provided table structure
        df_schema = StructType([
            StructField("TD_ID", StringType()),
            StructField("TD_ZONE", StringType()),
            StructField("FROM_CYCLE", StringType()),
            StructField("TO_CYCLE", StringType()),
            StructField("DISTRIBUTOR_CODE", StringType()),
            StructField("CUSTOMER_CODE", StringType()),
            StructField("CUSTOMER_NAME", StringType()),
            StructField("SHOPTYPE", StringType()),
            StructField("TARGET_VALUE", StringType())
        ])
        df = session.read\\
        .schema(df_schema)\\
        .option("skip_header",1)\\
        .option("field_delimiter", ''\\x01'')\\
        .option("encoding", "UTF-8")\\
        .option("field_optionally_enclosed_by", "\\"") \\
        .option("null_if", "") \\
        .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        # Checking for null values    
        df=df.na.drop("all")
        if df.count()==0 :
            return "No Data in file"
        
        # Filtering based on specified conditions
        filtered_df = df.filter(
            (upper(col("TD_ID")).like(''TOPDOORC%'')) &
            (upper(right(col("TD_ID"), 2)) != ''BS'') &
            (col("TARGET_VALUE") != 0)
        )
        # Adding additional fields
        filtered_df = filtered_df.with_column("CRT_DTTM", lit(datetime.now().strftime("%Y%m%d%H%M%S")))\\
                                 .with_column("FILE_NAME", lit(file_name))\\
                                 .with_column("RUN_ID", lit(None).cast(DecimalType(14,0)))
        snowdf = filtered_df.select(
            "TD_ID",
            "TD_ZONE",
            "FROM_CYCLE",
            "TO_CYCLE",
            "DISTRIBUTOR_CODE",
            "CUSTOMER_CODE",
            "CUSTOMER_NAME",
            "SHOPTYPE",
            "TARGET_VALUE",
            "CRT_DTTM",
            "FILE_NAME",
            "RUN_ID"
        )
        
           
        # Archive
        file_name=file_name.split(".")[0]+"_"+datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        
        snowdf.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)
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
