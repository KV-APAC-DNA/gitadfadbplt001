create or replace TABLE PROD_DNA_LOAD.VNMSDL_RAW.SDL_VN_DKSH_SDL_OTC (
	  BUSINESS_UNIT VARCHAR(100),
	  DATE VARCHAR(100),
	  STOCK_TYPE VARCHAR(100),
	  PRODUCT_CODE VARCHAR(100),
	  PRODUCT_NAME VARCHAR(100),
    BATCH VARCHAR(100),
    EXPIRY_DATE VARCHAR(100),
    BASE_UOM VARCHAR(100),
    SHELF_LIFE VARCHAR(100),
    COGS VARCHAR(100),
    REGION VARCHAR(100),   
	  QUANTITY NUMBER(20,0),
    VALUE VARCHAR(100),
    PLANT_CODE VARCHAR(100),
    PLANT_NAME VARCHAR(100),
    SYSLOT VARCHAR(100),    
	CRTD_DTTM TIMESTAMP_NTZ(9),
	FILE_NAME VARCHAR(100)    
);

CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.VNMSDL_RAW.VN_DKSH_OTC_PREPROCESSING("PARAM" ARRAY)
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
from snowflake.snowpark.functions import col,lit
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField
import pandas as pd
from datetime import datetime
import pytz

def main(session: snowpark.Session,Param): 

    try:

        #Param=[''DKSH_OTC_inventory_20230630.csv'',''VNMSDL_RAW.DEV_LOAD_STAGE_ADLS'',''dev/dksh/transaction/dksh_otc/'',''SDL_VN_DKSH_SDL_OTC'']

        # Extracting parameters from the input
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]

														

        # Define the schema for the DataFrame
        df_schema=StructType([
            StructField("BUSINESS_UNIT",StringType()),
            StructField("DATE",StringType()),
            StructField("STOCK_TYPE",StringType()),
            StructField("PRODUCT_CODE",StringType()),
            StructField("PRODUCT_NAME",StringType()),
            StructField("BATCH",StringType()),
            StructField("EXPIRY_DATE",StringType()),
            StructField("BASE_UOM",StringType()),
            StructField("SHELF_LIFE",StringType()),
            StructField("COGS",StringType()),
            StructField("REGION",StringType()),
            StructField("QUANTITY",StringType()),
            StructField("VALUE",StringType()),
            StructField("PLANT_CODE",StringType()),
            StructField("PLANT_NAME",StringType()),
            StructField("SYSLOT",StringType())
            ])


        # Read the CSV file into a DataFrame
    
        dataframe = session.read\\
            .schema(df_schema)\\
            .option("skip_header",2)\\
            .option("field_delimiter", "\\u0001")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .option("skip_blank_lines", True)\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        #---------------------------Transformation logic ------------------------------#

        # Handle null values or empty rows
        dataframe=dataframe.na.drop("all")

        # Check for empty Dataframe
        if dataframe.count()==0:
            return "No Data in file"

        # Add file_date, RUN_ID , filename columns
        
        filedate= file_name.split("_")[3].split(".")[0]
        
        dataframe = dataframe.withColumn("FILE_DATE",lit(filedate).cast("string"))
        dataframe = dataframe.with_column("FILE_NAME",lit(file_name))

        # Creating Final Dataframe
        final_df = dataframe.alias("final_df")

        # Delete existing Data for the current file
        
        del_sql = "DELETE FROM " + target_table + " WHERE FILE_NAME ="+"''" + (file_name)+"''"
        session.sql(del_sql).collect()


        # Load Data to the target table
        final_df.write.mode("append").saveAsTable(target_table)

        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")        
        formatted_month = current_date.strftime("%m")

        # write to success folder
    
        file_name=file_name.split(".")[0]+''_''+datetime.now().strftime("%Y%m%d%H%M%S")
        final_df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)
        
        return "Success"

        
    except KeyError as key_error:
        # Handle KeyError (missing columns) here
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message
        
    except Exception as e:
        # Handle exceptions here
        error_message = f"Error: {str(e)}"
        return error_message';
