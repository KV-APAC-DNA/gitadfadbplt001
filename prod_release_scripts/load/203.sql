CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.VNMSDL_RAW.SDL_VN_DMS_DATA_EXTRACT_SUMMARY("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS 'from snowflake.snowpark.functions import col, lit, current_timestamp, trunc
from snowflake.snowpark.types import StringType, StructType, StructField
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz
import pandas as pd

def main(session:snowpark.Session,Param):

    try:
        file_name       = Param[0]  # File name
        stage_name      = Param[1]  # Stage name where files are located
        temp_stage_path = Param[2]  # Path in the stage
        target_table    = Param[3]  # Target table for the data

        
        df_schema = StructType([
            StructField("SOURCE_FILE_NAME", StringType()),
            StructField("DATE_OF_EXTRACTION", StringType()),
            StructField("RECORD_COUNT", StringType())
        ])

        df = session.read \
            .schema(df_schema) \
            .option("skip_header",1) \
            .option("field_delimiter", "|") \
            .option("encoding", "ISO-8859-15") \
            .option("field_optionally_enclosed_by", "\"") \
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        # Check if DataFrame is empty
        df = df.na.drop("all")
        
        if df.count() == 0:
            return "No Data in file"

        snowdf = df.select("SOURCE_FILE_NAME", "DATE_OF_EXTRACTION", "RECORD_COUNT")

        file_name = file_name.split(".")[0] + "_" + datetime.now().strftime("%Y%m%d%H%M%S")
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")

        snowdf.write.mode("append").save_as_table(stage_name.split(".")[0] + "." + target_table)

        snowdf.write.copy_into_location("@" + stage_name + "/" + temp_stage_path + "/processed/success/" + formatted_year + "/" + formatted_month + "/" + file_name, header=True, OVERWRITE=True)

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
