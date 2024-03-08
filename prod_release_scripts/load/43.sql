CREATE OR REPLACE PROCEDURE SGPSDL_RAW.SG_REDMART_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them.

import snowflake.snowpark as snowpark
from snowflake import snowpark
from snowflake.snowpark.functions import col,lit,date_format,current_timestamp,to_timestamp
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField,DecimalType,DateType
import pandas as pd
from datetime import datetime
import pytz

def main(session: snowpark.Session,Param):

    try:
        #Param=[''Redmart_202402.csv'',''DEV_DNA_LOAD.SGPSDL_RAW.DEV_LOAD_STAGE_ADLS'',''dev/scan360/redmart'',''SGPSDL_RAW.SDL_SG_SCAN_DATA_REDMART'']
        # Extracting parameters from the input
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]

        # Define the schema for the DataFrame
        df_schema=StructType([
            StructField("TRX_DATE",DateType()),
            StructField("ITEM_CODE",StringType()),
            StructField("PRODUCT_CODE",StringType()),
            StructField("ITEM_DESC",StringType()),
            StructField("PACKSIZE",StringType()),
            StructField("BRAND",StringType()),
            StructField("SUPPLIER_ID",StringType()),
            StructField("SUPPLIER_NAME",StringType()),
            StructField("MANUFACTURER",StringType()),
            StructField("CATEGORY_1",StringType()),
            StructField("CATEGORY_2",StringType()),
            StructField("CATEGORY_3",StringType()),
            StructField("CATEGORY_4",StringType()),
            StructField("GMV",DecimalType(precision=38, scale=4)),
            StructField("UNITS_SOLD",DecimalType(precision=38, scale=0))
            
            ])

        # Read the CSV file into a DataFrame
    
        dataframe = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "\\u0001")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)


         #---------------------------Transformation logic ------------------------------
    
    
        # Add "STORE", "STORE_NAME","CDL_DTTM", "CRTD_DTTM", "FILE_NAME" to the Dataframe

        dataframe = dataframe.withColumn("STORE",lit("Redmart").cast("string"))
        dataframe = dataframe.withColumn("STORE_NAME",lit("Redmart").cast("string"))
        dataframe = dataframe.withColumn("CDL_DTTM",lit(None).cast("string"))

        #convertin time stamp into sg timezone
        dataframe = dataframe.with_column("CRTD_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        dataframe = dataframe.withColumn("FILE_NAME",lit(file_name).cast("string"))
        dataframe = dataframe.withColumn("RUN_ID",lit(None).cast(DecimalType(14,0)))
        

        # Creating copy of the Dataframe
        final_df = dataframe.alias("final_df")

        if final_df.count()==0:
            return "No Data in file"

        final_df=final_df.filter(col("ITEM_CODE").is_not_null()) and final_df.filter(col("TRX_DATE").is_not_null())\\
        and final_df.filter(col("PRODUCT_CODE").is_not_null()) and final_df.filter(col("ITEM_DESC").is_not_null())\\
        and final_df.filter(col("PACKSIZE").is_not_null()) and final_df.filter(col("BRAND").is_not_null())\\
        and final_df.filter(col("SUPPLIER_ID").is_not_null()) and final_df.filter(col("SUPPLIER_NAME").is_not_null())\\
        and final_df.filter(col("MANUFACTURER").is_not_null()) and final_df.filter(col("CATEGORY_1").is_not_null())\\
        and final_df.filter(col("CATEGORY_2").is_not_null()) and final_df.filter(col("CATEGORY_3").is_not_null())\\
        and final_df.filter(col("CATEGORY_4").is_not_null()) and final_df.filter(col("GMV").is_not_null()) and final_df.filter(col("UNITS_SOLD").is_not_null())

        
        # Load Data to the target table
        final_df.write.mode("append").saveAsTable(stage_name.split(".")[0]+"."+target_table)

    
        # write to success folder
    
        file_name=file_name.split(".")[0]+''_''+datetime.now().strftime("%Y%m%d%H%M%S")
        final_df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/success/"+file_name,header=True,OVERWRITE=True)
        
        
        return "Success"

    except KeyError as key_error:
        # Handle KeyError (missing columns) here
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message
    except pd.errors.MergeError as merge_error:
        # Handle DataFrame merging error
        error_message = f"DataFrame merging error: {str(merge_error)}"
        return error_message
    except Exception as e:
        # Handle exceptions here
        error_message = f"Error: {str(e)}"
        return error_message

';
