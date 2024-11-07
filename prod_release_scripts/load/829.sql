CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.INDSDL_RAW.CSL_SALESINVOICEORDERS_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS 'from snowflake.snowpark.functions import lit, col
from snowflake.snowpark.types import StringType, StructType, StructField
from datetime import datetime
import snowflake.snowpark as snowpark
import pandas as pd
import pytz

def main(session: snowpark.Session,Param):
    # Param = [''salesinvoiceorders20240527061541.csv'',''INDSDL_RAW.DEV_LOAD_STAGE_ADLS'',''dev/india_xdm/transaction/salesinvoiceorders'',''SDL_CSL_SALESINVOICEORDERS'']

    try:
        file_name = Param[0]
        stage_name = Param[1]
        temp_stage_path = Param[2]
        target_table = Param[3]

        # Define schema
        df_schema = StructType([
            StructField("distcode", StringType(), True),
            StructField("salinvno", StringType(), True),
            StructField("orderno", StringType(), True),
            StructField("orderdate", StringType(), True),
            StructField("createddate", StringType(), True),
            StructField("syncid", StringType(), True),
            StructField("createddt", StringType(), True),
        ])

        # Read the CSV file into a Snowflake DataFrame
        df = session.read \\
            .schema(df_schema) \\
            .option("skip_header", 1) \\
            .option("field_delimiter", "") \\
            .option("field_optionally_enclosed_by", "\\"") \\
            .csv(f"@{stage_name}/{temp_stage_path}/{file_name}")

        df = df.na.drop("all")

        if df.count() == 0:
            return "No Data in file"

        def removecsv(file_name):
            return file_name.split(''.'')[0] 

        df = df.withColumn("uploadflag", lit(None))

        df = df.withColumn(''filename'', lit(removecsv(file_name)))  
        df = df.withColumn("crtd_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        df = df.withColumn("run_id", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))

        snowdf = df.select(
            "distcode", 
            "salinvno", 
            "orderno", 
            "orderdate", 
            "uploadflag",
            "createddate", 
            "syncid", 
            "createddt",
            "run_id",
            "crtd_dttm",
            "filename"  
        )

        file_name = file_name.split(".")[0] + "_" + datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        
        # Move to success
        snowdf.write.mode("append").saveAsTable(stage_name.split(".")[0] + "." + target_table)
        # snowdf.write.copy_into_location(f"@{stage_name}/{temp_stage_path}/processed/success/{formatted_year}/{formatted_month}/{file_name}", header=True, OVERWRITE=True)
        
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