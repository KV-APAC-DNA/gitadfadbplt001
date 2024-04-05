CREATE OR REPLACE PROCEDURE THASDL_RAW.TH_ACTION_SENT_PREPROCESSING("PARAM" ARRAY)
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
from snowflake.snowpark.functions import col,lit,trim
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField,DecimalType,DateType,TimestampType
from datetime import datetime
import pytz

def main(session: snowpark.Session,Param): 

    try:

        #Param=[''TH_Action_Sent_20230621_20230621170614.csv'',''THASDL_RAW.DEV_LOAD_STAGE_ADLS'',''dev/cert_data_lake/SFMC/TH_Action_Sent/'',''SDL_TH_SFMC_SENT_DATA'']

        # Extracting parameters from the input
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        

        # Define the schema for the DataFrame
        df_schema=StructType([
            StructField("OYB_ACCOUNT_ID",StringType()),
            StructField("JOB_ID",StringType()),
            StructField("LIST_ID",StringType()),
            StructField("BATCH_ID",StringType()),
            StructField("SUBSCRIBER_ID",StringType()),
            StructField("SUBSCRIBER_KEY",StringType()),
            StructField("EVENT_DATE",TimestampType()),
            StructField("DOMAIN",StringType()),
            StructField("TRIGGERER_SEND_DEFINITION_OBJECT_ID",StringType()),
            StructField("TRIGGERED_SEND_CUSTOMER_KEY",StringType()),
            StructField("EMAIL_NAME",StringType()),
            StructField("EMAIL_SUBJECT",StringType()),
            StructField("EMAIL_ID",StringType())
            ])


        # Read the CSV file into a DataFrame
    
        dataframe = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "|")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)


        #---------------------------Transformation logic ------------------------------#
        for i in dataframe.columns:
            dataframe = dataframe.withColumn(f"{i}", regexp_replace(f"{i}", ''\\\\x00'',None))
        # Handle null values or empty rows
        dataframe=dataframe.na.drop("all")

        # Trim Spaces in Email Subject column
        dataframe = dataframe.withColumn("EMAIL_SUBJECT", trim(dataframe["EMAIL_SUBJECT"]))

         # Add FILE_NAME and CRTD_DTTM to the dataframe
        new_file_name=file_name[0:23] +".csv"

        dataframe = dataframe.with_column("FILE_NAME",lit(new_file_name))
        dataframe = dataframe.with_column("CRTD_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))

        dataframe=dataframe.select("OYB_ACCOUNT_ID","JOB_ID","LIST_ID","BATCH_ID","SUBSCRIBER_ID","SUBSCRIBER_KEY","EVENT_DATE","DOMAIN","TRIGGERER_SEND_DEFINITION_OBJECT_ID","TRIGGERED_SEND_CUSTOMER_KEY","EMAIL_NAME","EMAIL_SUBJECT","EMAIL_ID","FILE_NAME","CRTD_DTTM")

        # Creating copy of the Dataframe
        final_df = dataframe.alias("final_df")

        if final_df.count()==0:
            return "No Data in file"


        # Load Data to the target table
        final_df.write.mode("append").saveAsTable(target_table)
        
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")

        # write to success folder
    
        file_name=file_name.split(".")[0]
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
