update PROD_DNA_LOAD.meta_raw.parameters set parameter_value='PRPCFSDL_RAW.anz_perenso_over_and_above_PREPROCESSING' where parameter_id=3549;
CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.PRPCFSDL_RAW.ANZ_PERENSO_OVER_AND_ABOVE_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them.

import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col,lit,trim,when
from snowflake.snowpark.types import  StringType, StructType, StructField,IntegerType
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz



def main(session: snowpark.Session,Param): 
       
    try :

        #Param=["OverandAbove_20240320223707.csv","PCFSDL_RAW.DEV_LOAD_STAGE_ADLS","dev/ap_perenso/transaction/overandabove","sdl_perenso_Over_and_Above"]

        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        # Define the schema for the DataFrame
        
        df_schema = StructType([
                StructField("over_and_above_key", IntegerType()),
                StructField("acct_key", IntegerType()),
                StructField("todo_option_key", IntegerType()),
                StructField("prod_grp_key", IntegerType()),
                StructField("activated", StringType()),
                StructField("notes", StringType())
            ])

   
        
        dataframe = session.read.schema(df_schema) \\
        .option("skip_header",1) \\
        .option("field_delimiter", ",") \\
        .option("field_optionally_enclosed_by", "\\"")  \\
        .option("encoding","UTF-8")\\
        .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        
        dataframe=dataframe.na.drop("all")
        if dataframe.count()==0:
            return "No Data in file"

        dataframe = dataframe.with_column("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        dataframe = dataframe.with_column("CRTD_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))

        # Load Data to the target table
        dataframe.write.mode("append").saveAsTable(target_table)
       
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        #move to success
        dataframe.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)

        
        return ''Success''
        
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
