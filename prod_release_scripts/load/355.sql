CREATE OR REPLACE PROCEDURE PHLSDL_RAW.PH_CRM_CONSUMER_MASTER_PREPROCESSING("PARAM" ARRAY)
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
            
        #Param=[''PH_CRM_Consumer_Master_20240327_20240327173049.csv'',''PHLSDL_RAW.DEV_LOAD_STAGE_ADLS'',''dev/SFMC/PH_CRM_Consumer_Master_Primary'',''SDL_PH_SFMC_consumer_master'']

        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        # Define the schema for the DataFrame
        
        df_schema=StructType([
                        StructField("first_name", StringType()),
                        StructField("last_name", StringType()),
                        StructField("mobile_num", StringType()),
                        StructField("mobile_cntry_cd", StringType()),
                        StructField("birthday_mnth", StringType()),
                        StructField("birthday_year", StringType()),
                        StructField("address_1", StringType()),
                        StructField("address_2", StringType()),
                        StructField("address_city", StringType()),
                        StructField("address_zipcode", StringType()),
                        StructField("subscriber_key", StringType()),
                        StructField("website_unique_id", StringType()),
                        StructField("source", StringType()),
                        StructField("medium", StringType()),
                        StructField("brand", StringType()),
                        StructField("address_cntry", StringType()),
                        StructField("campaign_id", StringType()),
                        StructField("created_date", StringType()),
                        StructField("updated_date", StringType()),
                        StructField("unsubscribe_date", StringType()),
                        StructField("email", StringType()),
                        StructField("full_name", StringType()),
                        StructField("last_logon_time", StringType()),
                        StructField("remaining_points", StringType()),
                        StructField("redeemed_points", StringType()),
                        StructField("total_points", StringType()),
                        StructField("gender", StringType()),
                        StructField("line_id", StringType()),
                        StructField("line_name", StringType()),
                        StructField("line_email", StringType()),
                        StructField("line_channel_id", StringType()),
                        StructField("address_region", StringType()),
                        StructField("tier", StringType()),
                        StructField("Opt_In_For_Communication", StringType()),
                        StructField("Subscriber_Status", StringType()),
                        StructField("Viber_Engaged", StringType()),
                        StructField("Opt_In_For_JNJ_Communication", StringType()),
                        StructField("Opt_In_For_Campaign", StringType())
                        ])
                
        dataframe = session.read.schema(df_schema) \\
        .option("skip_header",1) \\
        .option("field_delimiter", "|") \\
        .option("encoding","UTF-16")\\
        .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
        

        
        dataframe=dataframe.na.drop("all")
        if dataframe.count()==0:
            return "No Data in file"

        #logic as per sql script 
        dataframe= dataframe.filter((dataframe["first_name"].isNotNull()) & (dataframe["last_name"].isNotNull()) & (dataframe["mobile_num"].isNotNull()) & (dataframe["subscriber_key"].isNotNull()) & (dataframe["brand"].isNotNull())  )

        new_file_name=file_name[0:31] +".csv"
        dataframe = dataframe.with_column("FILE_NAME",lit(new_file_name))
        dataframe = dataframe.with_column("CRTD_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))

        # Load Data to the target table
        dataframe.write.mode("append").saveAsTable(target_table)
       
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        #move to success
        file_name=file_name[0:31]+''_''+datetime.now().strftime("%Y%m%d%H%M%S")
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
