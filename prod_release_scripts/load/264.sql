update meta_raw.parameters
set PARAMETER_VALUE = 'Objectivekey,Created_Date,Description,Account_Key,AssignedUserKey,CreatedUserKey,Due_By,Date_Completed,Status'
where PARAMETER_GROUP_ID = 283 and PARAMETER_NAME = 'val_file_header';

update meta_raw.process
set usecase_id = 104
where process_id = 432;

update meta_raw.parameters
set PARAMETER_VALUE = 'PCFSDL_RAW.PCF_PERENSO_ORDER_DEALDISCOUNT_PREPROCESSING'
where PARAMETER_GROUP_ID = 400 and PARAMETER_NAME = 'sp_name';

update meta_raw.parameters
set PARAMETER_VALUE = 'PCFSDL_RAW.PCF_PERENSO_ORDER_CONSTANTS_PREPROCESSING'
where PARAMETER_GROUP_ID = 401 and PARAMETER_NAME = 'sp_name';


CREATE OR REPLACE PROCEDURE PCFSDL_RAW.PCF_PERENSO_HEAD_OFFICE_REQ_STATE_PREPROCESSING("PARAM" ARRAY)
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
 
        #Param=[''HeadOfficeReqState.csv'',''PCFSDL_RAW.DEV_LOAD_STAGE_ADLS'',''dev/ap_perenso/transaction/headoffice/'',''sdl_perenso_head_office_req_state'']
 
        # Extracting parameters from the input
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
 
 
        # Define the schema for the DataFrame
        df_schema=StructType([
            StructField("ACCT_KEY",StringType()),
            StructField("TODO_KEY",StringType()),
            StructField("PROD_GRP_KEY",StringType()),
            StructField("START_DATE",StringType()),
            StructField("END_DATE",StringType()),
            StructField("REQ_STATE",StringType()),
            StructField("STORE_CHK_HDR_KEY",StringType())
            
            ])
 
        # Read the CSV file into a DataFrame
        dataframe = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", ",")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .option("skip_blank_lines", True)\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
 
        #---------------------------Transformation logic ------------------------------#
 
        # Handle null values or empty rows
        dataframe=dataframe.na.drop("all")
 
        # Check for empty Dataframe
        if dataframe.count()==0:
            return "No Data in file"
    
        dataframe = dataframe.with_column("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        dataframe = dataframe.with_column("CREATE_DT",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        # Creating copy of the Dataframe
        final_df = dataframe.select("ACCT_KEY", "TODO_KEY", "PROD_GRP_KEY", "START_DATE", "END_DATE","REQ_STATE",\\
                                    "STORE_CHK_HDR_KEY","RUN_ID","CREATE_DT")
 
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

CREATE OR REPLACE PROCEDURE PCFSDL_RAW.PCF_PERENSO_RANGING_PRODUCT_PREPROCESSING("PARAM" ARRAY)
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
 
        #Param=[''RangingProduct_20240320223707.csv'',''PCFSDL_RAW.DEV_LOAD_STAGE_ADLS'',''dev/ap_perenso/transation/ranging/'',''sdl_perenso_ranging_product'']
 
        # Extracting parameters from the input
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
 
 
        # Define the schema for the DataFrame
        df_schema=StructType([
            StructField("RANGING_KEY",IntegerType()),
            StructField("PROD_KEY",IntegerType()),
            StructField("ACCT_GRP_KEY",IntegerType()),
            StructField("CORE",StringType()),
            StructField("RANGE_RANK",StringType())
            
            ])
 
        # Read the CSV file into a DataFrame
        dataframe = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", ",")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .option("skip_blank_lines", True)\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
 
        #---------------------------Transformation logic ------------------------------#
 
        # Handle null values or empty rows
        dataframe=dataframe.na.drop("all")
 
        # Check for empty Dataframe
        if dataframe.count()==0:
            return "No Data in file"
    
        dataframe = dataframe.with_column("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        dataframe = dataframe.with_column("CREATE_DT",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        # Creating copy of the Dataframe
        final_df = dataframe.select("RANGING_KEY", "PROD_KEY", "ACCT_GRP_KEY", "CORE", "RANGE_RANK","RUN_ID","CREATE_DT")
 
        # Load Data to the target table
        final_df.write.mode("append").saveAsTable(target_table)
 
         
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
 
        # write to success folder
        file_name=file_name.split(".")[0]
        final_df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True,file_format_name="PCFSDL_RAW.MY_CSV_FORMAT")
        return "Success"
 
    
 
    except KeyError as key_error:
        # Handle KeyError (missing columns) here
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message
    except Exception as e:
        # Handle exceptions here
        error_message = f"Error: {str(e)}"
        return error_message';


CREATE OR REPLACE PROCEDURE PCFSDL_RAW.PCF_PERENSO_ORDER_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
PACKAGES = ('openpyxl==3.0.10','snowflake-snowpark-python==*')
HANDLER = 'main'
EXECUTE AS OWNER
AS 'import snowflake.snowpark as snowpark
from snowflake.snowpark.types import StringType, StructType, StructField ,DecimalType
from snowflake.snowpark.functions import lit
from datetime import datetime
import pytz


def get_schema(file_name):
    df_schema=""
    select_col=""
    if file_name.startswith("OrderHdr"):
        df_schema=StructType([
        StructField("ORDER_KEY" , StringType()),
        StructField("ORDER_TYPE_KEY" , StringType()),
        StructField("ACCT_KEY" , StringType()),
        StructField("ORDER_DATE" , StringType()),
        StructField("STATUS" , StringType()),
        StructField("CHARGE" , StringType()),
        StructField("CONFIRMATION" , StringType()),
        StructField("DIARY_ITEM_KEY" , StringType()),
        StructField("WORK_ITEM_KEY" , StringType()),
        StructField("ACCOUNT_ORDER_NO" , StringType()),
        StructField("DELVRY_INSTNS" , StringType()),
            ])
        select_col=["ORDER_KEY","ORDER_TYPE_KEY","ACCT_KEY","ORDER_DATE","STATUS","CHARGE","CONFIRMATION","DIARY_ITEM_KEY","WORK_ITEM_KEY","ACCOUNT_ORDER_NO","DELVRY_INSTNS","RUN_ID","CREATE_DT"]
    if file_name.startswith("OrderType"):
        df_schema=StructType([
        StructField("ORDER_TYPE_KEY" , StringType()),
        StructField("ORDER_TYPE_DESC" , StringType()),
        StructField("SOURCE" , StringType())
            ])
        select_col=["ORDER_TYPE_KEY","ORDER_TYPE_DESC","SOURCE","run_id","create_dt"]
    if file_name.startswith("OrderBatch"):
        df_schema=StructType([
        StructField("ORDER_KEY" , StringType()),
        StructField("BATCH_KEY" , StringType()),
        StructField("BRANCH_KEY" , StringType()),
        StructField("DIST_ACCT" , StringType()),
        StructField("DELVRY_DT" , StringType()),
        StructField("STATUS" , StringType()),
        StructField("SUFFIX" , StringType()),
        StructField("SENT_DT" , StringType()),
            ])
        select_col=["ORDER_KEY","BATCH_KEY","BRANCH_KEY","DIST_ACCT","DELVRY_DT","STATUS","SUFFIX","SENT_DT","RUN_ID","CREATE_DT"]
    if file_name.startswith("OrderDetail"):
        df_schema=StructType([
         StructField("ORDER_KEY", StringType()),
        StructField("BATCH_KEY", StringType()),
        StructField("LINE_KEY", StringType()),
        StructField("PROD_KEY", StringType()),
        StructField("UNIT_QTY", StringType()),
        StructField("ENTERED_QTY", StringType()),
        StructField("ENTERED_UNIT_KEY", StringType()),
        StructField("LIST_PRICE", StringType()),
        StructField("NIS", StringType()),
        StructField("RRP", StringType()),
        StructField("DISC_KEY", StringType()),
        StructField("CREDIT_LINE_KEY", StringType()),
        StructField("CREDITED", StringType())
            ])
        select_col=["ORDER_KEY","BATCH_KEY","LINE_KEY","PROD_KEY","UNIT_QTY","ENTERED_QTY","ENTERED_UNIT_KEY","LIST_PRICE","NIS","RRP","DISC_KEY","CREDIT_LINE_KEY","CREDITED", "RUN_ID", "CREATE_DT"]
    return df_schema,select_col
    

def main(session: snowpark.Session,Param):
    try:
        # Extracting parameters from the input

        file_name       = Param[0]     
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        db_name         = stage_name.split(".")[0]
        target_table    = db_name+"."+Param[3]
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")


        # Define the schema for the DataFrame
        df_schema,select_col=get_schema(file_name)
        

        df = session.read\\
        .schema(df_schema)\\
        .option("skip_header",1)\\
        .option("field_delimiter", ",")\\
        .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
        
        df=df.na.drop("all")
        if df.count()==0:
            return "No Data in file"

            
        df = df.withColumn("run_id",lit(lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S"))))
        df = df.withColumn("create_dt",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
            
        
                    
        final_df=df.select(select_col)
                        


        file_name=file_name+"_"+datetime.now().strftime("%Y%m%d%H%M%S")
        
        # write to target table
        final_df.write.mode("append").saveAsTable(target_table)
        
        #move to success
        final_df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)


        return "Success"

    except KeyError as key_error:
        # Handle KeyError (missing columns) here
        error_message = f"KeyError: {str(key_error)}"
        return error_message

    
    except Exception as e:
        # Handle exceptions here
        error_message = f"Error: {str(e)}"
        return error_message';


CREATE OR REPLACE PROCEDURE PCFSDL_RAW.PCF_PERENSO_ORDER_CONSTANTS_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them.
 
import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col, lit, date_format, current_timestamp, to_date, year, month, concat, format_number, regexp_replace, to_timestamp, when, trim, upper

from snowflake.snowpark.files import SnowflakeFile
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz
import csv
def main(session: snowpark.Session,Param):

    try:

        #Param = [''Constants.csv'', ''PCFSDL_RAW.DEV_LOAD_STAGE_ADLS'', ''dev/ap_perenso/transaction/order'', ''sdl_perenso_constants'']

        # Extracting parameters from the input
        file_name = Param[0]
        stage_name = Param[1]
        temp_stage_path = Param[2]
        target_table = Param[3]

        # Read the CSV file into a DataFrame
        full_path = "@" + stage_name + "/" + temp_stage_path + "/" + file_name
        with SnowflakeFile.open(full_path, ''rb'', require_scoped_url=False) as f:
            df1 = pd.read_csv(f)
        
        df1[''RUN_ID''] = datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        df1[''CREATE_DT''] = datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")


        df1 = df1[["constkey", "constdesc","consttype","dsporder","RUN_ID","CREATE_DT"]]
        dataframe = session.create_dataframe(df1)

        #---------------------------Transformation logic ------------------------------#
 
        #Handle null values or empty rows
        dataframe=dataframe.na.drop("all")
 
        # Check for empty Dataframe
        if dataframe.count()==0:
            return "No Data in file"
        # Creating copy of the Datafram
        
                     
 
        # Load Data to the target table
        dataframe.write.mode("append").saveAsTable(target_table)
 
         
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
 
        # write to success folder
        file_name=file_name.split(".")[0]
        dataframe.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)
        return "Success"
 
    
 
    except KeyError as key_error:
        # Handle KeyError (missing columns) here
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message
    except Exception as e:
        # Handle exceptions here
        error_message = f"Error: {str(e)}"
        return error_message';

CREATE OR REPLACE PROCEDURE PCFSDL_RAW.PCF_PERENSO_ORDER_DEALDISCOUNT_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them.
 
import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col, lit, date_format, current_timestamp, to_date, year, month, concat, format_number, regexp_replace, to_timestamp, when, trim, upper

from snowflake.snowpark.files import SnowflakeFile
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz
import csv
def main(session: snowpark.Session,Param):

    try:

        #Param = [''DealDiscount.csv'', ''PCFSDL_RAW.DEV_LOAD_STAGE_ADLS'', ''dev/ap_perenso/transaction/order'', ''sdl_perenso_deal_discount'']

        # Extracting parameters from the input
        file_name = Param[0]
        stage_name = Param[1]
        temp_stage_path = Param[2]
        target_table = Param[3]

        # Read the CSV file into a DataFrame
        full_path = "@" + stage_name + "/" + temp_stage_path + "/" + file_name
        with SnowflakeFile.open(full_path, ''rb'', require_scoped_url=False) as f:
            df1 = pd.read_csv(f)
        
        df1[''RUN_ID''] = datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        df1[''CREATE_DT''] = datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")


        dataframe = session.create_dataframe(df1)


        #---------------------------Transformation logic ------------------------------#
 
        #Handle null values or empty rows
        dataframe=dataframe.na.drop("all")
 
        # Check for empty Dataframe
        if dataframe.count()==0:
            return "No Data in file"
        # Creating copy of the Datafram
        
                     
 
        # Load Data to the target table
        dataframe.write.mode("append").saveAsTable(target_table)
 
         
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
 
        # write to success folder
        file_name=file_name.split(".")[0]
        dataframe.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)
        return "Success"
 
    
 
    except KeyError as key_error:
        # Handle KeyError (missing columns) here
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message
    except Exception as e:
        # Handle exceptions here
        error_message = f"Error: {str(e)}"
        return error_message';
