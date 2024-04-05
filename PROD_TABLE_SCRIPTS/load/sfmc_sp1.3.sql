CREATE OR REPLACE PROCEDURE THASDL_RAW.TH_ACTION_BOUNCE_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them.

import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col,lit
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz

def main(session: snowpark.Session,Param): 
    try :
        #Param=["TH_Action_Bounce_20230621_20230621170609.csv","THASDL_RAW.DEV_LOAD_STAGE_ADLS","dev/cert_data_lake/SFMC/TH_Action_Bounce","SDL_TH_SFMC_BOUNCE_DATA"]

        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        # Define the schema for the DataFrame
        df_schema =  StructType([
                    StructField("oyb_account_id", StringType()),
                    StructField("job_id", StringType()),
                    StructField("list_id", StringType()),
                    StructField("batch_id", StringType()),
                    StructField("subscriber_id", StringType()),
                    StructField("subscriber_key", StringType()),
                    StructField("event_date", StringType()),
                    StructField("is_unique", StringType()),
                    StructField("domain", StringType()),
                    StructField("bounce_category_id", StringType()),
                    StructField("bounce_category", StringType()),
                    StructField("bounce_subcategory_id", StringType()),
                    StructField("bounce_subcategory", StringType()),
                    StructField("bounce_type_id", StringType()),
                    StructField("bounce_type", StringType()),
                    StructField("smtp_bounce_reason", StringType()),
                    StructField("smtp_message", StringType()),
                    StructField("smtp_code", StringType()),
                    StructField("triggerer_send_definition_object_id", StringType()),
                    StructField("triggered_send_customer_key", StringType()),
                    StructField("email_subject", StringType()),
                    StructField("bcc_email", StringType()),
                    StructField("email_name", StringType()),
                    StructField("email_id", StringType()),
                    StructField("email_address", StringType())
                ])
        
        dataframe = session.read\\
        .schema(df_schema)\\
        .option("skip_header",1)\\
        .option("field_delimiter", "|")\\
        .option("field_optionally_enclosed_by", "\\"") \\
        .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
	for i in dataframe.columns:
            dataframe = dataframe.withColumn(f"{i}", regexp_replace(f"{i}", ''\\\\x00'',None))
	dataframe=dataframe.na.drop("all")
        if dataframe.count()==0:
            return "No Data in file"
        #---------------------------Transformation logic ------------------------------#

        # Add FILE_NAME and CRTD_DTTM to the dataframe
        new_file_name=file_name[0:25] +".csv"
        dataframe = dataframe.with_column("FILE_NAME",lit(new_file_name))
        dataframe = dataframe.with_column("CRTD_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))


        # Creating copy of the Dataframe
        final_df = dataframe.alias("final_df")

       
        # Load Data to the target table
        final_df.write.mode("append").saveAsTable(target_table)

        # write to success folder
        file_name=file_name[0:25]+''_''+datetime.now().strftime("%Y%m%d%H%M%S")
		
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
		
        final_df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)
        
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
        return error_message';
