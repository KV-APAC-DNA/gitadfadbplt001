update meta_raw.parameters
set parameter_value = '5'
where parameter_id = 4153;

INSERT INTO meta_raw.PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (7080,314,'Fssi_week_data_group','is_direct_load','Y',FALSE,TRUE);

CREATE OR REPLACE PROCEDURE PCFSDL_RAW.ANZ_PERENSO_FOODSTUFF_FSSI_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('openpyxl==3.0.10','snowflake-snowpark-python==*','xlrd==2.0.1')
HANDLER = 'main'
EXECUTE AS OWNER
AS '# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them.

import snowflake.snowpark as snowpark
from snowflake import snowpark
from snowflake.snowpark.functions import col,lit,trim, substring,current_timestamp,to_timestamp
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField,FloatType
import pandas as pd
from datetime import datetime
from snowflake.snowpark.files import SnowflakeFile
import pytz

def main(session: snowpark.Session,Param): 

    try:

        #Param=[''FSSI Week 17 2024.xls'',''PCFSDL_RAW.DEV_LOAD_STAGE_ADLS'',''dev/ap_perenso/transaction/weekly_load/foodstuff/fssi_week/'',''SDL_PERENSO_FSSI_SALES'']

        # Extracting parameters from the input
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]

        full_path = "@"+stage_name+"/"+temp_stage_path+"/"+file_name
        with SnowflakeFile.open(full_path, "rb", require_scoped_url = False) as f:
            header_names=["ARTICLE","ARTICLE_DESCRIPTION","OLD_ARTICLE_NUM","EAN","GROSS_WEIGHT","SHIP_TO_STORE","STORE_NAME","SALES_VOLUME","SALES_VALUE"]
            df_pandas=pd.read_excel(f,header=6,names=header_names)


        dataframe = session.create_dataframe(df_pandas)


         #---------------------------Transformation logic ------------------------------#

        # Handle null values or empty rows
        dataframe= dataframe.na.drop("all")

        # Filter Result and null value in SHIP_TO_STORE and STORE_NAME
        dataframe= dataframe.filter((dataframe["SHIP_TO_STORE"] != "result") & dataframe["STORE_NAME"].isNotNull())

        # Check for empty Dataframe
        if dataframe.count()==0:
            return "No Data in file"

        # # Add filename and CRT_DTTM columns
        new_file_name=file_name.replace("_"," ").split(".")[0]+''.xls''
        dataframe = dataframe.with_column("FILE_NAME",lit(new_file_name))
        dataframe = dataframe.withColumn("CRT_DTTM",lit(to_timestamp(current_timestamp())))

        # Creating Final Dataframe
        final_df = dataframe.alias("final_df")

        # Load Data to the target table
        final_df.write.mode("append").saveAsTable(target_table)

        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")

        # write to success folder
    
        file_name=file_name.replace("_"," ").split(".")[0]+''_''+datetime.now().strftime("%Y%m%d%H%M%S")
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
