CREATE OR REPLACE PROCEDURE THASDL_RAW.LA_GT_SELLOUT_PREPROCESSING("PARAM" ARRAY)
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
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField,DecimalType,DateType
import pandas as pd
from datetime import datetime
import pytz

def main(session: snowpark.Session,Param): 

    try:

        #Param=[''S_LAO2403132335_20240314040335.txt'',''THASDL_RAW.DEV_LOAD_STAGE_ADLS'',''dev/cert_data_lake/LCM/Laos_Sellout_Data/'',''SDL_LA_GT_SELLOUT_FACT'']

        # Extracting parameters from the input
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        

        # Define the schema for the DataFrame
        df_schema=StructType([
            StructField("DISTRIBUTORID",StringType()),
            StructField("ORDERNO",StringType()),
            StructField("ORDERDATE",StringType()),
            StructField("ARCODE",StringType()),
            StructField("ARNAME",StringType()),
            StructField("CITY",StringType()),
            StructField("REGION",StringType()),
            StructField("SALEDISTRICT",StringType()),
            StructField("SALEOFFICE",StringType()),
            StructField("SALEGROUP",StringType()),
            StructField("ARTYPECODE",StringType()),
            StructField("SALEEMPLOYEE",StringType()),
            StructField("SALENAME",StringType()),
            StructField("PRODUCTCODE",StringType()),
            StructField("PRODUCTDESC",StringType()),
            StructField("MEGABRAND",StringType()),
            StructField("BRAND",StringType()),
            StructField("BASEPRODUCT",StringType()),
            StructField("VARIANT",StringType()),
            StructField("PUTUP",StringType()),
            StructField("GROSSPRICE",StringType()),
            StructField("QTY",StringType()),
            StructField("SUBAMT1",StringType()),
            StructField("DISCOUNT",StringType()),
            StructField("SUBAMT2",StringType()),
            StructField("DISCOUNTBTLINE",StringType()),
            StructField("TOTALBEFOREVAT",StringType()),
            StructField("TOTAL",StringType()),
            StructField("LINENUMBER",StringType()),
            StructField("ISCANCEL",StringType()),
            StructField("CNDOCNO",StringType()),
            StructField("CNREASONCODE",StringType()),
            StructField("PROMOTIONHEADER1",StringType()),
            StructField("PROMOTIONHEADER2",StringType()),
            StructField("PROMOTIONHEADER3",StringType()),
            StructField("PROMODESC1",StringType()),
            StructField("PROMODESC2",StringType()),
            StructField("PROMODESC3",StringType()),
            StructField("PROMOCODE1",StringType()),
            StructField("PROMOCODE2",StringType()),
            StructField("PROMOCODE3",StringType()),
            StructField("AVGDISCOUNT",StringType())
            ])


        # Read the CSV file into a DataFrame
    
        dataframe = session.read\\
            .schema(df_schema)\\
            .option("skip_header",0)\\
            .option("field_delimiter", "\\t")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
        

        #---------------------------Transformation logic ------------------------------#

        # Handle null values or empty rows
        dataframe=dataframe.na.drop("all")

        # Check for empty Dataframe
        if dataframe.count()==0:
            return "No Data in file"

        # Add File name, run_id and crt_dttm to the dataframe
        run_id=datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        new_file_name=file_name.split(".")[0]+''_''+run_id+''.txt''
        dataframe = dataframe.withColumn("FILENAME",lit(new_file_name).cast("string"))
        dataframe = dataframe.with_column("RUN_ID",lit(run_id).cast("string"))
        dataframe = dataframe.with_column("CRT_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))

        # Creating copy of the Dataframe
        final_df = dataframe.alias("final_df")

        # Load Data to the target table
        final_df.write.mode("append").saveAsTable(target_table)

        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")

        # write to success folder
    
        file_name=file_name.split(".")[0]+''_''+run_id
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
