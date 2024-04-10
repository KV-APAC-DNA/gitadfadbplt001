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
from snowflake.snowpark.functions import col,lit,trim,regexp_replace
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

CREATE OR REPLACE PROCEDURE THASDL_RAW.TH_ACTION_UNSUBSCRIBE_PREPROCESSING("PARAM" ARRAY)
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
from snowflake.snowpark.functions import col,lit,trim,regexp_replace
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField,DecimalType,DateType,TimestampType
from datetime import datetime
import pytz

def main(session: snowpark.Session,Param): 

    try:

        #Param=[''TH_Action_Unsubscribe_20230627_202306271706'',''THASDL_RAW.DEV_LOAD_STAGE_ADLS'',''dev/cert_data_lake/SFMC/TH_Action_Unsubscribe/'',''SDL_TH_SFMC_UNSUBSCRIBE_DATA'']

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
            StructField("EMAIL_NAME",StringType()),
            StructField("EMAIL_SUBJECT",StringType()),
            StructField("EMAIL_ID",StringType()),
            StructField("IS_UNIQUE",StringType())
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

         # Add FILE_NAME and CRTD_DTTM to the dataframe
        new_file_name=file_name[0:30] +".csv"

        dataframe = dataframe.with_column("FILE_NAME",lit(new_file_name))
        dataframe = dataframe.with_column("CRTD_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))


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
from snowflake.snowpark.functions import col,lit,regexp_replace
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

        
        #---------------------------Transformation logic ------------------------------#
        for i in dataframe.columns:
            dataframe = dataframe.withColumn(f"{i}", regexp_replace(f"{i}", ''\\\\x00'',None))
        dataframe=dataframe.na.drop("all")
        if dataframe.count()==0:
            return "No Data in file"

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


CREATE OR REPLACE PROCEDURE THASDL_RAW.TH_ACTION_CLICK_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them.

import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col,lit,regexp_replace
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DecimalType,DateType
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz

def main(session: snowpark.Session,Param): 
    try :
        

        #Param=["TH_Action_Click_20230621_20230621170615.csv","THASDL_RAW.DEV_LOAD_STAGE_ADLS","dev/cert_data_lake/SFMC/TH_Action_Click","SDL_TH_SFMC_CLICK_DATA"]

        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        # Define the schema for the DataFrame
        df_schema = StructType([
                StructField("oyb_account_id", StringType()),
                StructField("job_id", StringType()),
                StructField("list_id", StringType()),
                StructField("batch_id", StringType()),
                StructField("subscriber_id", StringType()),
                StructField("subscriber_key", StringType()),
                StructField("event_date", StringType()),
                StructField("domain", StringType()),
                StructField("url", StringType()),
                StructField("link_name", StringType()),
                StructField("link_content", StringType()),
                StructField("is_unique", StringType()),
                StructField("email_name", StringType()),
                StructField("email_subject", StringType())
            ])
        
        dataframe = session.read\\
        .schema(df_schema)\\
        .option("skip_header",1)\\
        .option("field_delimiter", "|")\\
        .option("field_optionally_enclosed_by", "\\"") \\
        .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)


        #---------------------------Transformation logic ------------------------------#
        for i in dataframe.columns:
            dataframe = dataframe.withColumn(f"{i}", regexp_replace(f"{i}", ''\\\\x00'',None))
        dataframe=dataframe.na.drop("all")
        if dataframe.count()==0:
            return "No Data in file"


        # Add FILE_NAME and CRTD_DTTM to the dataframe
        new_file_name=file_name[0:24] +".csv"
        dataframe = dataframe.with_column("FILE_NAME",lit(new_file_name))
        dataframe = dataframe.with_column("CRTD_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))


        # Creating copy of the Dataframe
        final_df = dataframe.alias("final_df")

        
        # Load Data to the target table
        final_df.write.mode("append").saveAsTable(target_table)

        # write to success folder
        file_name=file_name[0:24]+''_''+datetime.now().strftime("%Y%m%d%H%M%S")
		
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

CREATE OR REPLACE PROCEDURE THASDL_RAW.TH_ACTION_OPEN_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them.

import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col,lit,regexp_replace
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DecimalType,DateType
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz



def main(session: snowpark.Session,Param): 
    
    try :

        #Param=["TH_Action_Open_20231205_20231212083337.csv","THASDL_RAW.DEV_LOAD_STAGE_ADLS","dev/cert_data_lake/SFMC/TH_Action_Open/","sdl_TH_sfmc_open_data"]

        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        # Define the schema for the DataFrame
        df_schema = StructType([
            StructField("oyb_account_id", StringType()),
            StructField("job_id", StringType()),
            StructField("list_id", StringType()),
            StructField("batch_id", StringType()),
            StructField("subscriber_id", StringType()),
            StructField("subscriber_key", StringType()),
            StructField("email_name", StringType()),
            StructField("email_subject", StringType()),
            StructField("bcc_email", StringType()),
            StructField("email_id", StringType()),
            StructField("event_date", StringType()),
            StructField("domain", StringType()),
            StructField("is_unique", StringType())
        ])
        
        dataframe = session.read.schema(df_schema) \\
        .option("skip_header",1) \\
        .option("field_delimiter", "|") \\
        .option("field_optionally_enclosed_by", "\\"")  \\
        .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        #---------------------------Transformation logic ------------------------------#
        for i in dataframe.columns:
            dataframe = dataframe.withColumn(f"{i}", regexp_replace(f"{i}", ''\\\\x00'',None))
        dataframe=dataframe.na.drop("all")
        if dataframe.count()==0:
            return "No Data in file"

        # Add FILE_NAME and CRTD_DTTM to the dataframe
        new_file_name=file_name[0:23] +".csv"
        dataframe = dataframe.with_column("FILE_NAME",lit(new_file_name))
        dataframe = dataframe.with_column("CRTD_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))


        # Creating copy of the Dataframe
        final_df = dataframe.alias("final_df")

        
        # Load Data to the target table
        final_df.write.mode("append").saveAsTable(target_table)

        # write to success folder
        file_name=file_name[0:23]+''_''+datetime.now().strftime("%Y%m%d%H%M%S")
		
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
        return error_message
        ';

create or replace TABLE META_RAW.CATEGORY_MARKET_MAPPING (
	CATEGORY VARCHAR(200) NOT NULL,
	MARKET VARCHAR(200) NOT NULL
);

INSERT INTO META_RAW.CATEGORY_MARKET_MAPPING (CATEGORY, MARKET) VALUES ('REGIONAL','REGIONAL');
INSERT INTO META_RAW.CATEGORY_MARKET_MAPPING (CATEGORY, MARKET) VALUES ('SINGAPORE_TRANSACTIONAL','SINGAPORE');
INSERT INTO META_RAW.CATEGORY_MARKET_MAPPING (CATEGORY, MARKET) VALUES ('REGIONAL_REFRESH','REGIONAL');
INSERT INTO META_RAW.CATEGORY_MARKET_MAPPING (CATEGORY, MARKET) VALUES ('REGIONAL_MDS','REGIONAL');
INSERT INTO META_RAW.CATEGORY_MARKET_MAPPING (CATEGORY, MARKET) VALUES ('REGIONAL_PKA','REGIONAL');
INSERT INTO META_RAW.CATEGORY_MARKET_MAPPING (CATEGORY, MARKET) VALUES ('SINGAPORE_SELLOUT','SINGAPORE');
INSERT INTO META_RAW.CATEGORY_MARKET_MAPPING (CATEGORY, MARKET) VALUES ('POP6_refresh','REGIONAL');
INSERT INTO META_RAW.CATEGORY_MARKET_MAPPING (CATEGORY, MARKET) VALUES ('SAP_ECC_MASTER','REGIONAL');
INSERT INTO META_RAW.CATEGORY_MARKET_MAPPING (CATEGORY, MARKET) VALUES ('SAP_BW_MASTER_AND_ACCT_ATTR','REGIONAL');
INSERT INTO META_RAW.CATEGORY_MARKET_MAPPING (CATEGORY, MARKET) VALUES ('SAP_BW_BILLING','REGIONAL');
INSERT INTO META_RAW.CATEGORY_MARKET_MAPPING (CATEGORY, MARKET) VALUES ('SAP_BW_BILLING_COND','REGIONAL');
INSERT INTO META_RAW.CATEGORY_MARKET_MAPPING (CATEGORY, MARKET) VALUES ('SAP_BW_COPA17','REGIONAL');
INSERT INTO META_RAW.CATEGORY_MARKET_MAPPING (CATEGORY, MARKET) VALUES ('SAP_BW_INVENTORY','REGIONAL');
INSERT INTO META_RAW.CATEGORY_MARKET_MAPPING (CATEGORY, MARKET) VALUES ('SAP_BW_INVOICE','REGIONAL');
INSERT INTO META_RAW.CATEGORY_MARKET_MAPPING (CATEGORY, MARKET) VALUES ('GCH','REGIONAL');
INSERT INTO META_RAW.CATEGORY_MARKET_MAPPING (CATEGORY, MARKET) VALUES ('MY_POS','MALAYSIA');
INSERT INTO META_RAW.CATEGORY_MARKET_MAPPING (CATEGORY, MARKET) VALUES ('MY_JOINT_MONTHLY','MALAYSIA');
INSERT INTO META_RAW.CATEGORY_MARKET_MAPPING (CATEGORY, MARKET) VALUES ('MY_SELLIN','MALAYSIA');
INSERT INTO META_RAW.CATEGORY_MARKET_MAPPING (CATEGORY, MARKET) VALUES ('MY_MDS','MALAYSIA');
INSERT INTO META_RAW.CATEGORY_MARKET_MAPPING (CATEGORY, MARKET) VALUES ('MY_SELLOUT_SISO','MALAYSIA');
INSERT INTO META_RAW.CATEGORY_MARKET_MAPPING (CATEGORY, MARKET) VALUES ('MY_SELLIN_ANALYSIS','MALAYSIA');
INSERT INTO META_RAW.CATEGORY_MARKET_MAPPING (CATEGORY, MARKET) VALUES ('MY_LIST_PRICE','MALAYSIA');
INSERT INTO META_RAW.CATEGORY_MARKET_MAPPING (CATEGORY, MARKET) VALUES ('TH_CUSTOMER360','THAILAND');
INSERT INTO META_RAW.CATEGORY_MARKET_MAPPING (CATEGORY, MARKET) VALUES ('VN_DMS','VIETNAM');
INSERT INTO META_RAW.CATEGORY_MARKET_MAPPING (CATEGORY, MARKET) VALUES ('VN_TARGET_TOPDOOR','VIETNAM');
INSERT INTO META_RAW.CATEGORY_MARKET_MAPPING (CATEGORY, MARKET) VALUES ('SAP_BW_MATERIAL_UOM','REGIONAL');
INSERT INTO META_RAW.CATEGORY_MARKET_MAPPING (CATEGORY, MARKET) VALUES ('SAP_BW_SALES','REGIONAL');
INSERT INTO META_RAW.CATEGORY_MARKET_MAPPING (CATEGORY, MARKET) VALUES ('MY_POS_SIPOS','MALAYSIA');
INSERT INTO META_RAW.CATEGORY_MARKET_MAPPING (CATEGORY, MARKET) VALUES ('HEALTH_INVENTORY_TAB_REFRESH','REGIONAL');
INSERT INTO META_RAW.CATEGORY_MARKET_MAPPING (CATEGORY, MARKET) VALUES ('TH_MT_Daily_price_load','THAILAND');
INSERT INTO META_RAW.CATEGORY_MARKET_MAPPING (CATEGORY, MARKET) VALUES ('THAILAND','THAILAND');
INSERT INTO META_RAW.CATEGORY_MARKET_MAPPING (CATEGORY, MARKET) VALUES ('TH_FILE_INGESTION','THAILAND');
INSERT INTO META_RAW.CATEGORY_MARKET_MAPPING (CATEGORY, MARKET) VALUES ('SAP_BW_LISTPRICE','REGIONAL');
INSERT INTO META_RAW.CATEGORY_MARKET_MAPPING (CATEGORY, MARKET) VALUES ('CUSTOMER360_refresh','REGIONAL');
INSERT INTO META_RAW.CATEGORY_MARKET_MAPPING (CATEGORY, MARKET) VALUES ('MY_WATSONS_INV','MALAYSIA');
INSERT INTO META_RAW.CATEGORY_MARKET_MAPPING (CATEGORY, MARKET) VALUES ('MY_SELLOUT','MALAYSIA');
INSERT INTO META_RAW.CATEGORY_MARKET_MAPPING (CATEGORY, MARKET) VALUES ('my_sellout_sales','MALAYSIA');
INSERT INTO META_RAW.CATEGORY_MARKET_MAPPING (CATEGORY, MARKET) VALUES ('my_gt_sales_dna_to_mds','MALAYSIA');
INSERT INTO META_RAW.CATEGORY_MARKET_MAPPING (CATEGORY, MARKET) VALUES ('my_sellout_inv','MALAYSIA');
INSERT INTO META_RAW.CATEGORY_MARKET_MAPPING (CATEGORY, MARKET) VALUES ('TH_CRM_SFMC','THAILAND');
INSERT INTO META_RAW.CATEGORY_MARKET_MAPPING (CATEGORY, MARKET) VALUES ('TH_PERFECT_STORE','THAILAND');
INSERT INTO META_RAW.CATEGORY_MARKET_MAPPING (CATEGORY, MARKET) VALUES ('VN_MT','VIETNAM');
INSERT INTO META_RAW.CATEGORY_MARKET_MAPPING (CATEGORY, MARKET) VALUES ('VN_MT_PERFECTSTORE','VIETNAM');
INSERT INTO META_RAW.CATEGORY_MARKET_MAPPING (CATEGORY, MARKET) VALUES ('SAP_BW_DELIVERY','REGIONAL');
