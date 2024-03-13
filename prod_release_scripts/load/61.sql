CREATE OR REPLACE PROCEDURE MYSSDL_RAW.MY_SELLOUT_GT_IN_TRANSIT_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them.

import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col,lit,to_date, date_format
from snowflake.snowpark.types import  StringType, StructType, StructField
import pandas as pd
from datetime import datetime
import pytz


def main(session: snowpark.Session,Param): 

    try:
        #Param=[''MY_GT_In_Transit_Inv.csv'',''MYSSDL_RAW.DEV_LOAD_STAGE_ADLS'',''dev/transaction/sellout/others'',''sdl_MY_IN_TRANSIT'']

        # Extracting parameters from the input
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]

         # Define the schema
        df_schema = StructType([
                StructField("billing_doc", StringType()),
                StructField("billing_date", StringType()),
                StructField("gr_date", StringType()),
                StructField("closing_date", StringType()),
                StructField("remarks", StringType())
            ])
        
               
        dataframe = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "\\u0001")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
        dataframe=dataframe.na.drop("all")
        if dataframe.count()==0:
            return "No Data in file"

        # Add CDL_DTTM and CURR_DT to the dataframe
        dataframe = dataframe.with_column("CDL_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        dataframe = dataframe.with_column("CURR_DT",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))

       
        dataframe=dataframe.to_pandas()
        # Convert the date column to datetime format
        dataframe["BILLING_DATE"] = pd.to_datetime(dataframe["BILLING_DATE"]).dt.strftime("%d-%m-%Y")
        dataframe["GR_DATE"] = pd.to_datetime(dataframe["GR_DATE"]).dt.strftime("%d-%m-%Y")
        dataframe["CLOSING_DATE"] = pd.to_datetime(dataframe["CLOSING_DATE"]).dt.strftime("%d-%m-%Y")
       
        dataframe = session.create_dataframe(dataframe)
        
         # Load Data to the target table
        dataframe.write.mode("append").saveAsTable(target_table)

        # write to success folder
        file_name=file_name.split(".")[0]+''_''+datetime.now().strftime("%Y%m%d%H%M%S")
        dataframe.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/success/"+file_name,file_format_type="csv",header=True,OVERWRITE=True)

        
        return ''Success''

    except KeyError as key_error:
    # Handle KeyError (missing columns) here
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message
    
    except Exception as e:
    # Handle exceptions here
        error_message = f"Error: {str(e)}"
        return error_message


    ';
