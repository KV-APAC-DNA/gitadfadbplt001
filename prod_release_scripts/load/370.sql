CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.THASDL_RAW.PERFECT_STORE_CONSUMERREACH_CVS_711_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '# This Preprocessing Code can be used for both Perfect store- 
# consumerreach_cvs and consumerreach_711

import snowflake.snowpark as snowpark
from snowflake import snowpark
from snowflake.snowpark.functions import col,lit
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField,DecimalType,DateType
import pandas as pd
from datetime import datetime
import pytz

def main(session: snowpark.Session,Param):

    
    try:

        # Parameters for consumerreach_cvs
        #Param=[''jnj_consumerreach_cvs_202401.csv'',''THASDL_RAW.DEV_LOAD_STAGE_ADLS'',''dev/perfect-store/transaction/'',''SDL_JNJ_CONSUMERREACH_CVS'']

        # Parameters for consumerreach_711
        #Param=[''jnj_consumerreach_711_202401.csv'',''THASDL_RAW.DEV_LOAD_STAGE_ADLS'',''dev/perfect-store/transaction/'',''SDL_JNJ_CONSUMERREACH_711'']
        
        # Extracting parameters from the input
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]

        # Define the schema for the DataFrame
        df_schema=StructType([
            StructField("ID",StringType()),
            StructField("CDATE",StringType()),
            StructField("RETAIL",StringType()),
            StructField("RETAILNAME",StringType()),
            StructField("RETAILBRANCH",StringType()),
            StructField("RETAILPROVINCE",StringType()),
            StructField("JJSKUBARCODE",StringType()),
            StructField("JJSKUNAME",StringType()),
            StructField("JJCORE",StringType()),
            StructField("DISTRIBUTION",StringType()),
            StructField("STATUS",StringType())
            ])

        
        # Read the CSV file into a DataFrame
    
        dataframe = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "\\u0001")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)


        #---------------------------Transformation logic ------------------------------#

        # Handle null values or empty rows
        dataframe=dataframe.na.drop("all")

        # Check if the Dataframe is having Data

        if dataframe.count()==0:
            return "No Data in file"

        # Add RUN_ID, FILE NAME and YEARMO columns

        dataframe = dataframe.with_column("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        dataframe = dataframe.with_column("FILE_NAME",lit(file_name))

        # Extract year month from the file
        yearmo= file_name.split("_")[-1].split(".")[0]

        dataframe = dataframe.with_column("YEARMO",lit(yearmo))


        # Creating Final Dataframe
        final_df = dataframe.alias("final_df")


        # Delete existing Data for the current file
        
        # del_sql = "DELETE FROM " + target_table + " WHERE FILE_NAME ="+"''" + (file_name)+"''"
        # session.sql(del_sql).collect()
        
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
