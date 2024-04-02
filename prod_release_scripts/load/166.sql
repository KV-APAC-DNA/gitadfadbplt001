USE SCHEMA VNMSDL_RAW;

CREATE OR REPLACE PROCEDURE VNMSDL_RAW.VN_INTERFACE_CHOICE("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS 'from snowflake.snowpark.functions import col, lit, date_format, current_timestamp, to_date, year, month, concat, format_number, regexp_replace,to_timestamp,when,trim,upper
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DecimalType
import snowflake.snowpark
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz
import pandas as pd

def main(session: snowpark.Session,Param):
    #Param= [''Interface_Choices_20220905.csv'',''VNMSDL_RAW.DEV_LOAD_STAGE_ADLS'',''dev/VN_PS/Interface_Choices'','''']
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        
        df_schema = StructType([
                StructField("ise_id", StringType(), True),
                StructField("channel_id", StringType(), True),
                StructField("channel_name", StringType(), True),
                StructField("ques_no", StringType(), True),
                StructField("answer_seq", StringType(), True),
                StructField("sku_group", StringType(), True),
                StructField("rep_param", StringType(), True),
                StructField("putup_id", StringType(), True),
                StructField("description", StringType(), True),
                StructField("score", StringType(), True),
                StructField("sfa", StringType(), True),
                StructField("brand_id", StringType(), True),
                StructField("brand_name", StringType(), True)
            ])
        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "|")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
        df = df.na.drop("all")
        if df.count() == 0:
            return "No Data in file"

        columns_to_check = ["ise_id", "ques_no", "answer_seq"]
        
        for i in columns_to_check:
            df_na=df.na.drop(subset=[i])
            if df_na.count()!=df.count():
                return f"null value in column {i}"
            
        df_dup1=df.select("ise_id", "ques_no", "answer_seq")
        
            
        df_dup2 = df_dup1.dropDuplicates()
        
        if df_dup1.count() != df_dup2.count():
            return "Duplicate values" 
            
        df = df.with_column("run_id", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        file_name1=file_name + datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        df = df.with_column("file_name", lit(file_name1))
        df = df.with_column("crtd_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))

        snowdf = df.select(
                    "ise_id",
                    "channel_id",
                    "channel_name",
                    "ques_no",
                    "answer_seq",
                    "sku_group",
                    "rep_param",
                    "putup_id",
                    "description",
                    "score",
                    "sfa",
                    "brand_id",
                    "brand_name",
                    "file_name",
                    "crtd_dttm"
                    )
        file_name = file_name.split(".")[0] + "_" + datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        
        #move to success
        snowdf.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)
        
        snowdf.write.mode("append").saveAsTable(stage_name.split(".")[0] + "." + target_table)
        
        return "Success"

    
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

CREATE OR REPLACE PROCEDURE VNMSDL_RAW.VN_INTERFACE_CHOICE_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS 'from snowflake.snowpark.functions import col, lit, date_format, current_timestamp, to_date, year, month, concat, format_number, regexp_replace,to_timestamp,when,trim,upper
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DecimalType
import snowflake.snowpark
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz
import pandas as pd

def main(session: snowpark.Session,Param):
    #Param= [''Interface_Choices_20240202.csv'',''VNMSDL_RAW.DEV_LOAD_STAGE_ADLS'',''dev/VN_PS/transaction/Interface_Choices'',''sdl_vn_interface_choices'']
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        
        df_schema = StructType([
                StructField("ise_id", StringType(), True),
                StructField("channel_id", StringType(), True),
                StructField("channel_name", StringType(), True),
                StructField("ques_no", StringType(), True),
                StructField("answer_seq", StringType(), True),
                StructField("sku_group", StringType(), True),
                StructField("rep_param", StringType(), True),
                StructField("putup_id", StringType(), True),
                StructField("description", StringType(), True),
                StructField("score", StringType(), True),
                StructField("sfa", StringType(), True),
                StructField("brand_id", StringType(), True),
                StructField("brand_name", StringType(), True)
            ])
        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "|")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
        df = df.na.drop("all")
        if df.count() == 0:
            return "No Data in file"

        columns_to_check = ["ise_id", "ques_no", "answer_seq"]
        
        for i in columns_to_check:
            df_na=df.na.drop(subset=[i])
            if df_na.count()!=df.count():
                return f"null value in column {i}"
            
        df_dup1=df.select("ise_id", "ques_no", "answer_seq")
        
            
        df_dup2 = df_dup1.dropDuplicates()
        
        if df_dup1.count() != df_dup2.count():
            return "Duplicate values" 
            
        df = df.with_column("run_id", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        file_name1=file_name.split(".")[0]+"_"+ datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")+".csv"
        df = df.with_column("file_name", lit(file_name1))
        df = df.with_column("crtd_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))

        snowdf = df.select(
                    "ise_id",
                    "channel_id",
                    "channel_name",
                    "ques_no",
                    "answer_seq",
                    "sku_group",
                    "rep_param",
                    "putup_id",
                    "description",
                    "score",
                    "sfa",
                    "brand_id",
                    "brand_name",
                    "file_name",
                    "crtd_dttm"
                    )
        file_name = file_name.split(".")[0] + "_" + datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        
        #move to success
        snowdf.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)
        
        snowdf.write.mode("append").saveAsTable(stage_name.split(".")[0] + "." + target_table)
        
        return "Success"

    
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


CREATE OR REPLACE PROCEDURE VNMSDL_RAW.VN_INTERFACE_CPG_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS 'from snowflake.snowpark.functions import col, lit, date_format, current_timestamp, to_date, year, month, concat, format_number, regexp_replace, to_timestamp, when, trim, upper
from snowflake.snowpark.types import StringType, StructType, StructField
import snowflake.snowpark
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz

def main(session: snowpark.Session,Param):
    #Param=["Interface_CPG_20220905.csv","VNMSDL_RAW.DEV_LOAD_STAGE_ADLS","dev/VN_PS/Interface_CPG",""]
    try:
        file_name = Param[0]
        stage_name = Param[1]
        temp_stage_path = Param[2]
        target_table = Param[3]
        
        df_schema = StructType([
            StructField("slsper_id", StringType(), True),
            StructField("branch_code", StringType(), True),
            StructField("createddate", StringType(), True),
            StructField("visitdate", StringType(), True),
        ])
        
        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "|")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
        
        df = df.na.drop("all")
        
        if df.count() == 0:
            return "No Data in file"

        columns_to_check = ["slsper_id","branch_code","visitdate"]
        
        for i in columns_to_check:
            df_na=df.na.drop(subset=[i])
            if df_na.count()!=df.count():
                return f"null value in column {i}"
            
        df_dup1=df.select("slsper_id","branch_code","visitdate" )
        
            
        df_dup2 = df_dup1.dropDuplicates()
        
        if df_dup1.count() != df_dup2.count():
            return "Duplicate values" 
            
        df = df.withColumn("run_id", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        file_name1=file_name.split(".")[0]+"_"+ datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")+".csv"
        df = df.withColumn("file_name", lit(file_name1))
        df = df.withColumn("crtd_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))

        snowdf = df.select(
            "slsper_id",
            "branch_code",
            "createddate",
            "visitdate",
            "file_name",
            "crtd_dttm"
        )
        
        file_name=file_name.split(".")[0]+"_"+ datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        
        #move to success
        snowdf.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)
        
        snowdf.write.mode("append").saveAsTable(stage_name.split(".")[0] + "." + target_table)
        
        return "Success"

    except KeyError as key_error:
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message

    except pd.errors.MergeError as merge_error:
        error_message = f"DataFrame merging error: {str(merge_error)}"
        return error_message

    except Exception as e:
        error_message = f"Error: {str(e)}"
        return error_message
';

CREATE OR REPLACE PROCEDURE VNMSDL_RAW.VN_INTERFACE_CUSTOMERVISITED_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS 'from snowflake.snowpark.functions import col, lit, date_format, current_timestamp, to_date, year, month, concat, format_number, regexp_replace, to_timestamp, when, trim, upper
from snowflake.snowpark.types import StringType, StructType, StructField
import snowflake.snowpark
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz

def main(session: snowpark.Session,Param):
    #Param=["Interface_CustomerVisited_20220905.csv","VNMSDL_RAW.DEV_LOAD_STAGE_ADLS","dev/VN_PS/Interface_CustomerVisited",""]
    try:
        file_name = Param[0]
        stage_name = Param[1]
        temp_stage_path = Param[2]
        target_table = Param[3]
        
        df_schema = StructType([
            StructField("cust_code", StringType(), True),
            StructField("slsper_id", StringType(), True),
            StructField("branch_code", StringType(), True),
            StructField("ise_id", StringType(), True),
            StructField("created_date", StringType(), True),
            StructField("visit_date", StringType(), True),
        ])
        
        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "|")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
        
        df = df.na.drop("all")
        
        if df.count() == 0:
            return "No Data in file"

        columns_to_check = [ "slsper_id", "cust_code", "ise_id", "branch_code", "visit_date"]
        
        for i in columns_to_check:
            df_na=df.na.drop(subset=[i])
            if df_na.count()!=df.count():
                return f"null value in column {i}"

        


        df_dup1=df.select( "slsper_id", "cust_code", "ise_id", "branch_code", "visit_date")
        
            
        df_dup2 = df_dup1.dropDuplicates()
        
        if df_dup1.count() != df_dup2.count():
            return "Duplicate values"
        
            
        df = df.withColumn("run_id", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        file_name1=file_name.split(".")[0]+"_"+ datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")+".csv"
        df = df.withColumn("file_name", lit(file_name1))
        df = df.withColumn("crtd_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))

        snowdf = df.select(
            "cust_code",
            "slsper_id",
            "branch_code",
            "ise_id",
            "created_date",
            "visit_date",
            "file_name",
            "crtd_dttm"
        )
        
        file_name=file_name.split(".")[0]+"_"+ datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        
        #move to success
        snowdf.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)
        
        snowdf.write.mode("append").saveAsTable(stage_name.split(".")[0] + "." + target_table)
        
        return "Success"

    except KeyError as key_error:
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message

    except pd.errors.MergeError as merge_error:
        error_message = f"DataFrame merging error: {str(merge_error)}"
        return error_message

    except Exception as e:
        error_message = f"Error: {str(e)}"
        return error_message
';

CREATE OR REPLACE PROCEDURE VNMSDL_RAW.VN_ISE_HEADER_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS 'from snowflake.snowpark.functions import col, lit, date_format, current_timestamp, to_date, year, month, concat, format_number, regexp_replace, to_timestamp, when, trim, upper
from snowflake.snowpark.types import StringType, StructType, StructField
import snowflake.snowpark
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz

def main(session: snowpark.Session,Param):
    #Param=["Interface_ISE_Header_20220905.csv","VNMSDL_RAW.DEV_LOAD_STAGE_ADLS","dev/VN_PS/Interface_ISEHeader",""]
    try:
        file_name = Param[0]
        stage_name = Param[1]
        temp_stage_path = Param[2]
        target_table = Param[3]
        
        df_schema = StructType([
            StructField("ise_id", StringType(), True),
            StructField("ise_desc", StringType(), True),
            StructField("channel_code", StringType(), True),
            StructField("channel_desc", StringType(), True),
            StructField("startdate", StringType(), True),
            StructField("enddate", StringType(), True),
        ])
        
        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "|")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
        
        df = df.na.drop("all")
        
        if df.count() == 0:
            return "No Data in file"

        columns_to_check = ["ise_id", "ise_desc"]
        
        for i in columns_to_check:
            df_na=df.na.drop(subset=[i])
            if df_na.count()!=df.count():
                return f"null value in column {i}"

        df_dup1 = df.select("ise_id", "ise_desc")
            
        df_dup2 = df_dup1.dropDuplicates()
        
        if df_dup1.count() != df_dup2.count():
            return "Duplicate values" 
            
        df = df.withColumn("run_id", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        file_name1=file_name.split(".")[0]+"_"+ datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")+".csv"
        df = df.withColumn("file_name", lit(file_name1))
        df = df.withColumn("crtd_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))

        snowdf = df.select(
            "ise_id",
            "ise_desc",
            "channel_code",
            "channel_desc",
            "startdate",
            "enddate",
            "file_name",
            "crtd_dttm"
        )
        
        file_name=file_name.split(".")[0]+"_"+ datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        
        #move to success
        snowdf.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)
        
        snowdf.write.mode("append").saveAsTable(stage_name.split(".")[0] + "." + target_table)
        
        return "Success"

    except KeyError as key_error:
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message

    except pd.errors.MergeError as merge_error:
        error_message = f"DataFrame merging error: {str(merge_error)}"
        return error_message

    except Exception as e:
        error_message = f"Error: {str(e)}"
        return error_message
';

CREATE OR REPLACE PROCEDURE VNMSDL_RAW.VN_MT_POS_AEON("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python','xlrd')
HANDLER = 'main'
EXECUTE AS OWNER
AS '

from snowflake.snowpark.functions import col, lit, current_timestamp, trim, upper
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DecimalType,DateType
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
from snowflake.snowpark.files import SnowflakeFile

def main(session:snowpark.Session, Param):
    try:
        file_name = Param[0]
        stage_name = Param[1]
        temp_stage_path = Param[2]
        target_table = Param[3]
        

        # Updated schema based on Redshift table structure
        df_schema = StructType([
            StructField("STORE", StringType()),
            StructField("DEPARTMENT", StringType()),
            StructField("SUPPLIER_CODE", StringType()),
            StructField("SUPPLIER_NAME", StringType()),
            StructField("ITEM", StringType()),
            StructField("ITEM_NAME", StringType()),
            StructField("SALES_QUANTITY", StringType()),
            StructField("SALES_AMOUNT", StringType())
        ])
        
        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header", 1)\\
            .option("field_delimiter", "\\x01")\\
            .option("encoding", "UTF-8")\\
            .option("REPLACE_INVALID_CHARACTERS", True)\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .option("null_if", "")\\
            .csv(f"@{stage_name}/{temp_stage_path}/{file_name}")

        current_timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        
        # Adding file name and current timestamp as additional columns
        df = df.with_column("FILENAME", lit(file_name))\\
               .with_column("RUN_ID", lit(current_timestamp).cast(DecimalType(14,0)))\\
               .with_column("CRTD_DTTM", lit(datetime.now().strftime("%Y%m%d%H%M%S")))

        # Selecting the updated list of columns including new ones
        snowdf = df.select(
            "STORE",
            "DEPARTMENT",
            "SUPPLIER_CODE",
            "SUPPLIER_NAME",
            "ITEM",
            "ITEM_NAME",
            "SALES_QUANTITY",
            "SALES_AMOUNT",
            "FILENAME",
            "RUN_ID",
            "CRTD_DTTM"
        )


        # Check if DataFrame is empty
        snowdf = snowdf.na.drop("all")
        if snowdf.count() == 0:
            return "No Data in table"

        # Archive to success
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")

        snowdf.write.copy_into_location(
            f"@{stage_name}/{temp_stage_path}/processed/success/{formatted_year}/{formatted_month}/{file_name}",
            header=True,
            overwrite=True
        )
        
        # Write operation
        snowdf.write.mode("append").save_as_table(stage_name.split(".")[0]+"."+target_table)
        
        # Success message
        return "Success"

    except KeyError as key_error:
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message

    except pd.errors.MergeError as merge_error:
        error_message = f"DataFrame merging error: {str(merge_error)}"
        return error_message
    
    except Exception as e:
        error_message = f"Error: {str(e)}"
        return error_message
';

CREATE OR REPLACE PROCEDURE VNMSDL_RAW.VN_MT_POS_BHX("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python','xlrd')
HANDLER = 'main'
EXECUTE AS OWNER
AS '

from snowflake.snowpark.functions import col, lit, current_timestamp, trim, upper
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DecimalType,DateType
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
from snowflake.snowpark.files import SnowflakeFile

def main(session:snowpark.Session, Param):
    try:
        file_name = Param[0]
        stage_name = Param[1]
        temp_stage_path = Param[2]
        target_table = Param[3]
        

        df_schema = StructType([
            StructField("PRO_CODE", StringType()),
            StructField("PRO_NAME", StringType()),
            StructField("CUST_CODE", StringType()),
            StructField("CUST_NAME", StringType()),
            StructField("CAT_STORE", StringType()),
            StructField("QUANTITY", StringType()),
            StructField("AMOUNT", StringType())
        ])
        
        # Read the data into a DataFrame with the specified schema and options
        df = session.read \\
            .schema(df_schema) \\
            .option("skip_header", 1) \\
            .option("field_delimiter", "\\x01") \\
            .option("encoding", "UTF-8") \\
            .option("REPLACE_INVALID_CHARACTERS", True) \\
            .option("field_optionally_enclosed_by", "\\"") \\
            .option("null_if", "") \\
            .csv(f"@{stage_name}/{temp_stage_path}/{file_name}")
        
        current_timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        
        # Adding file name and run_id as additional columns
        df = df.with_column("FILENAME", lit(file_name)) \\
               .with_column("RUN_ID", lit(current_timestamp).cast(DecimalType(14,0)))\\
               .with_column("CRTD_DTTM", lit(datetime.now().strftime("%Y%m%d%H%M%S")))
        
        # Selecting the updated list of columns including the new ones for insertion into the target table
        snowdf = df.select(
            "PRO_CODE",
            "PRO_NAME",
            "CUST_CODE",
            "CUST_NAME",
            "CAT_STORE",
            "QUANTITY",
            "AMOUNT",
            "FILENAME",
            "RUN_ID",
            "CRTD_DTTM"
        )


        # Check if DataFrame is empty
        snowdf = snowdf.na.drop("all")
        if snowdf.count() == 0:
            return "No Data in table"

        # Archive to success
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")

        snowdf.write.copy_into_location(
            f"@{stage_name}/{temp_stage_path}/processed/success/{formatted_year}/{formatted_month}/{file_name}",
            header=True,
            overwrite=True
        )
        
        # Write operation
        snowdf.write.mode("append").save_as_table(stage_name.split(".")[0]+"."+target_table)
        
        # Success message
        return "Success"

    except KeyError as key_error:
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message

    except pd.errors.MergeError as merge_error:
        error_message = f"DataFrame merging error: {str(merge_error)}"
        return error_message
    
    except Exception as e:
        error_message = f"Error: {str(e)}"
        return error_message
';

CREATE OR REPLACE PROCEDURE VNMSDL_RAW.VN_MT_POS_CON_CUNG("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python','xlrd')
HANDLER = 'main'
EXECUTE AS OWNER
AS '

from snowflake.snowpark.functions import col, lit, current_timestamp, trim, upper
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DecimalType,DateType
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
from snowflake.snowpark.files import SnowflakeFile

def main(session:snowpark.Session, Param):
    try:
        file_name = Param[0]
        stage_name = Param[1]
        temp_stage_path = Param[2]
        target_table = Param[3]
        

        # Schema definition based on the Con Cung dataset structure
        df_schema = StructType([
            StructField("DELIVERY_CODE", StringType()),
            StructField("STORE", StringType()),  # nvarchar maps to StringType in Snowflake
            StructField("PRODUCT_CODE", StringType()),
            StructField("PRODUCT_NAME", StringType()),  # nvarchar maps to StringType in Snowflake
            StructField("QUANTITY", StringType())  # Quantities are integers but handling as strings for ingestion and will convert later if necessary
        ])
        
        # Reading the data into a DataFrame with the specified schema and options
        df = session.read \\
            .schema(df_schema) \\
            .option("skip_header", 1) \\
            .option("field_delimiter", "\\x01") \\
            .option("encoding", "UTF-8") \\
            .option("REPLACE_INVALID_CHARACTERS", True) \\
            .option("field_optionally_enclosed_by", "\\"") \\
            .option("null_if", "") \\
            .csv(f"@{stage_name}/{temp_stage_path}/{file_name}")
        
        current_timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        
        # Adding additional columns for the file name and the run ID
        df = df.with_column("FILENAME", lit(file_name)) \\
               .with_column("RUN_ID", lit(current_timestamp).cast(DecimalType(14,0)))\\
               .with_column("CRTD_DTTM", lit(datetime.now().strftime("%Y%m%d%H%M%S")))
        
        # Preparing the DataFrame for insertion into the target table by selecting the appropriate columns
        snowdf = df.select(
            "DELIVERY_CODE",
            "STORE",
            "PRODUCT_CODE",
            "PRODUCT_NAME",
            "QUANTITY",
            "FILENAME",
            "RUN_ID",
            "CRTD_DTTM"
        )


        # Check if DataFrame is empty
        snowdf = snowdf.na.drop("all")
        if snowdf.count() == 0:
            return "No Data in table"

        # Archive to success
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")

        snowdf.write.copy_into_location(
            f"@{stage_name}/{temp_stage_path}/processed/success/{formatted_year}/{formatted_month}/{file_name}",
            header=True,
            overwrite=True
        )
        
        # Write operation
        snowdf.write.mode("append").save_as_table(stage_name.split(".")[0]+"."+target_table)
        
        # Success message
        return "Success"

    except KeyError as key_error:
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message

    except pd.errors.MergeError as merge_error:
        error_message = f"DataFrame merging error: {str(merge_error)}"
        return error_message
    
    except Exception as e:
        error_message = f"Error: {str(e)}"
        return error_message
';

CREATE OR REPLACE PROCEDURE VNMSDL_RAW.VN_MT_POS_COOP("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python','xlrd','openpyxl')
HANDLER = 'main'
EXECUTE AS OWNER
AS '

from snowflake.snowpark.functions import col, lit, current_timestamp, trim, upper
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DecimalType,DateType
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
from snowflake.snowpark.files import SnowflakeFile

def main(session:snowpark.Session, Param):
    try:
        file_name = Param[0]
        stage_name = Param[1]
        temp_stage_path = Param[2]
        target_table = Param[3]

        full_path = "@"+stage_name+"/"+temp_stage_path+"/"+file_name
        
        with SnowflakeFile.open(full_path, "rb", require_scoped_url = False) as f:
            df_full = pd.read_excel(f)

        # Split the DataFrame
        df_first_12 = df_full.iloc[:, :12]  # First 12 columns
        df_remaining = df_full.iloc[:, 12:]  # Columns after the 12th
        
        # Melt the remaining columns DataFrame
        df_melted = df_remaining.melt(var_name="STORE", value_name="SALES_AMOUNT", ignore_index=False)
        
        # Reset index on the melted DataFrame to avoid losing the index during the upcoming merge
        df_melted.reset_index(inplace=True)
        
        # Merge the first 12 columns DataFrame with the melted one
        # This assumes the original index is meaningful and should be preserved. If not, you may need to adjust this step.
        excel_data = pd.merge(df_first_12.reset_index(), df_melted, on="index", how="right")
        
        # Drop the extra index column if not needed
        excel_data.drop(columns=["index"], inplace=True)

        excel_data.columns = [col.strip().upper().replace("\\n", "_").replace(" ", "_") for col in excel_data.columns]
        columns_to_rename = {
            "NO": "SERIAL_NO",
            "DESCRIPTION__(VIETNAMESE)": "DESCRIPTION_VIETNAMESE",
            "TOTAL_AMOUNT__W/O_VAT": "AMOUNT"
        }
        excel_data.rename(columns=columns_to_rename, inplace=True)
    
        df = session.create_dataframe(excel_data)
        
        current_timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        
        # Adding the file name and run_id as additional columns
        df = df.with_column("FILE_NAME", lit(file_name)) \\
               .with_column("RUN_ID", lit(current_timestamp).cast(DecimalType(14,0)))\\
               .with_column("CRTD_DTTM", lit(datetime.now().strftime("%Y%m%d%H%M%S")))
        
        # Preparing the DataFrame for insertion into the target table by selecting the appropriate columns
        snowdf = df.select(
            "THANG",
            "DESC_A",
            "IDEPT",
            "ISDEPT",
            "ICLAS",
            "ISCLAS",
            "SKU",
            "TENVT",
            "BRAND_SPM",
            "MADV",
            "SUMOFLG",
            "SUMOFTTBVNCKHD",
            "STORE",
            "SALES_AMOUNT",
            "FILE_NAME",
            "RUN_ID",
            "CRTD_DTTM"
        )


        # Check if DataFrame is empty
        snowdf = snowdf.na.drop("all")
        if snowdf.count() == 0:
            return "No Data in table"

        # Archive to success
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")

        snowdf.write.copy_into_location(
            f"@{stage_name}/{temp_stage_path}/processed/success/{formatted_year}/{formatted_month}/{file_name}",
            header=True,
            overwrite=True
        )
        
        # Write operation
        snowdf.write.mode("append").save_as_table(stage_name.split(".")[0]+"."+target_table)
        
        # Success message
        return "Success"

    except KeyError as key_error:
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message

    except pd.errors.MergeError as merge_error:
        error_message = f"DataFrame merging error: {str(merge_error)}"
        return error_message
    
    except Exception as e:
        error_message = f"Error: {str(e)}"
        return error_message
';

CREATE OR REPLACE PROCEDURE VNMSDL_RAW.VN_MT_POS_GUARDIAN("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python','xlrd','openpyxl')
HANDLER = 'main'
EXECUTE AS OWNER
AS '

from snowflake.snowpark.functions import col, lit, current_timestamp, trim, upper
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DecimalType,DateType
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
from snowflake.snowpark.files import SnowflakeFile

def main(session:snowpark.Session, Param):
    try:
        file_name = Param[0]
        stage_name = Param[1]
        temp_stage_path = Param[2]
        target_table = Param[3]

        full_path = "@"+stage_name+"/"+temp_stage_path+"/"+file_name
        
        with SnowflakeFile.open(full_path, "rb", require_scoped_url = False) as f:
            excel_data = pd.read_excel(f)

        excel_data.columns = [col.strip().upper().replace("\\n", "_").replace(" ", "_") for col in excel_data.columns]
        columns_to_rename = {
            "NO": "SERIAL_NO",
            "DESCRIPTION__(VIETNAMESE)": "DESCRIPTION_VIETNAMESE",
            "TOTAL_AMOUNT__W/O_VAT": "AMOUNT"
        }
        excel_data.rename(columns=columns_to_rename, inplace=True)
    
        df = session.create_dataframe(excel_data)
        
        current_timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        
        # Adding the file name and run_id as additional columns
        df = df.with_column("FILE_NAME", lit(file_name)) \\
               .with_column("RUN_ID", lit(current_timestamp).cast(DecimalType(14,0)))\\
               .with_column("CRTD_DTTM", lit(datetime.now().strftime("%Y%m%d%H%M%S")))
        
        # Preparing the DataFrame for insertion into the target table by selecting the appropriate columns
        snowdf = df.select(
            "SERIAL_NO",
            "STORE_CODE",
            "STORE_NAME",
            "SKU",
            "BARCODE",
            "DESCRIPTION_VIETNAMESE",
            "BRAND",
            "DIVISION",
            "DEPARTMENT",
            "CATEGORY",
            "SUB_CATEGORY",
            "SALES_SUPPLIER",
            "AMOUNT",
            "FILE_NAME",
            "RUN_ID",
            "CRTD_DTTM"
        )


        # Check if DataFrame is empty
        snowdf = snowdf.na.drop("all")
        if snowdf.count() == 0:
            return "No Data in table"

        # Archive to success
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")

        snowdf.write.copy_into_location(
            f"@{stage_name}/{temp_stage_path}/processed/success/{formatted_year}/{formatted_month}/{file_name}",
            header=True,
            overwrite=True
        )
        
        # Write operation
        snowdf.write.mode("append").save_as_table(stage_name.split(".")[0]+"."+target_table)
        
        # Success message
        return "Success"

    except KeyError as key_error:
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message

    except pd.errors.MergeError as merge_error:
        error_message = f"DataFrame merging error: {str(merge_error)}"
        return error_message
    
    except Exception as e:
        error_message = f"Error: {str(e)}"
        return error_message
';

CREATE OR REPLACE PROCEDURE VNMSDL_RAW.VN_MT_POS_LOTTE("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python','xlrd','openpyxl')
HANDLER = 'main'
EXECUTE AS OWNER
AS '

from snowflake.snowpark.functions import col, lit, current_timestamp, trim, upper
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DecimalType,DateType
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
from snowflake.snowpark.files import SnowflakeFile

def main(session:snowpark.Session, Param):
    try:
        file_name = Param[0]
        stage_name = Param[1]
        temp_stage_path = Param[2]
        target_table = Param[3]

        full_path = "@"+stage_name+"/"+temp_stage_path+"/"+file_name
        with SnowflakeFile.open(full_path, "rb", require_scoped_url = False) as f:
            xls = pd.ExcelFile(f)
            excel_dfs = [pd.read_excel(f, sheet_name=sheet) for sheet in xls.sheet_names]
            excel_data = pd.concat(excel_dfs, ignore_index=True)
            # Convert all columns to string to avoid data type inference issues
            excel_data = excel_data.astype(str)

        excel_data.columns = [col.strip().upper().replace(" ", "_") for col in excel_data.columns]
        columns_to_rename = {
            "1-CAT_NM": "CAT_NM_1",
            "2-CAT_NM": "CAT_NM_2",
            "3-CAT_NM": "CAT_NM_3",
            "4-CAT_NM": "CAT_NM_4"
        }
        excel_data.rename(columns=columns_to_rename, inplace=True)
    
        df = session.create_dataframe(excel_data)
        
        current_timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        
        # Adding the file name and run_id as additional columns
        df = df.with_column("FILE_NAME", lit(file_name)) \\
               .with_column("RUN_ID", lit(current_timestamp).cast(DecimalType(14,0)))\\
               .with_column("CRTD_DTTM", lit(datetime.now().strftime("%Y%m%d%H%M%S")))
        
        # Preparing the DataFrame for insertion into the target table by selecting the appropriate columns
        snowdf = df.select(
            "STR",
            "STR_NM",
            "CAT_NM_1",
            "CAT_NM_2",
            "CAT_NM_3",
            "CAT_NM_4",
            "PROD_CD",
            "SALE_CD",
            "PROD_NM",
            "VEN",
            "VEN_NM",
            "SALE_QTY",
            "TOT_SALE_AMT",
            "FILE_NAME",
            "RUN_ID",
            "CRTD_DTTM"
        )


        # Check if DataFrame is empty
        snowdf = snowdf.na.drop("all")
        if snowdf.count() == 0:
            return "No Data in table"

        # Archive to success
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")

        snowdf.write.copy_into_location(
            f"@{stage_name}/{temp_stage_path}/processed/success/{formatted_year}/{formatted_month}/{file_name}",
            header=True,
            overwrite=True
        )
        
        # Write operation
        snowdf.write.mode("append").save_as_table(stage_name.split(".")[0]+"."+target_table)
        
        # Success message
        return "Success"

    except KeyError as key_error:
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message

    except pd.errors.MergeError as merge_error:
        error_message = f"DataFrame merging error: {str(merge_error)}"
        return error_message
    
    except Exception as e:
        error_message = f"Error: {str(e)}"
        return error_message
';

CREATE OR REPLACE PROCEDURE VNMSDL_RAW.VN_MT_POS_MEGA("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python','xlrd')
HANDLER = 'main'
EXECUTE AS OWNER
AS '

from snowflake.snowpark.functions import col, lit, current_timestamp, trim, upper
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DecimalType,DateType
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
from snowflake.snowpark.files import SnowflakeFile

def main(session:snowpark.Session, Param):
    try:
        file_name = Param[0]
        stage_name = Param[1]
        temp_stage_path = Param[2]
        target_table = Param[3]
        

        # Schema definition for the MEGA dataset
        df_schema = StructType([
            StructField("SITE_NO", StringType()),
            StructField("SITE_NAME", StringType()),  # nvarchar maps to StringType in Snowflake
            StructField("PERIOD", StringType()),
            StructField("ART_NO", StringType()),
            StructField("ART_SV_NAME", StringType()),  # nvarchar maps to StringType in Snowflake
            StructField("SUPPL_NO", StringType()),
            StructField("SUPPL_NAME", StringType()),  # nvarchar maps to StringType in Snowflake
            StructField("SALE_QTY", StringType()),
            StructField("COGS_AMT", StringType())
        ])
        
        # Reading the data into a DataFrame with the specified schema and options
        df = session.read \\
            .schema(df_schema) \\
            .option("skip_header", 1) \\
            .option("field_delimiter", "\\x01") \\
            .option("encoding", "UTF-8") \\
            .option("REPLACE_INVALID_CHARACTERS", True) \\
            .option("field_optionally_enclosed_by", "\\"") \\
            .option("null_if", "") \\
            .csv(f"@{stage_name}/{temp_stage_path}/{file_name}")
        
        current_timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        
        # Adding the file name and run_id as additional columns
        df = df.with_column("FILE_NAME", lit(file_name)) \\
               .with_column("RUN_ID", lit(current_timestamp).cast(DecimalType(14,0)))\\
               .with_column("CRTD_DTTM", lit(datetime.now().strftime("%Y%m%d%H%M%S")))
        
        # Preparing the DataFrame for insertion into the target table by selecting the appropriate columns
        snowdf = df.select(
            "SITE_NO",
            "SITE_NAME",
            "PERIOD",
            "ART_NO",
            "ART_SV_NAME",
            "SUPPL_NO",
            "SUPPL_NAME",
            "SALE_QTY",
            "COGS_AMT",
            "FILE_NAME",
            "RUN_ID",
            "CRTD_DTTM"
        )


        # Check if DataFrame is empty
        snowdf = snowdf.na.drop("all")
        if snowdf.count() == 0:
            return "No Data in table"

        # Archive to success
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")

        snowdf.write.copy_into_location(
            f"@{stage_name}/{temp_stage_path}/processed/success/{formatted_year}/{formatted_month}/{file_name}",
            header=True,
            overwrite=True
        )
        
        # Write operation
        snowdf.write.mode("append").save_as_table(stage_name.split(".")[0]+"."+target_table)
        
        # Success message
        return "Success"

    except KeyError as key_error:
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message

    except pd.errors.MergeError as merge_error:
        error_message = f"DataFrame merging error: {str(merge_error)}"
        return error_message
    
    except Exception as e:
        error_message = f"Error: {str(e)}"
        return error_message
';

CREATE OR REPLACE PROCEDURE VNMSDL_RAW.VN_MT_POS_VINMART("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python','xlrd')
HANDLER = 'main'
EXECUTE AS OWNER
AS '

from snowflake.snowpark.functions import col, lit, current_timestamp, trim, upper
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DecimalType,DateType
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
from snowflake.snowpark.files import SnowflakeFile

def main(session:snowpark.Session, Param):
    try:
        file_name = Param[0]
        stage_name = Param[1]
        temp_stage_path = Param[2]
        target_table = Param[3]
        

        # Schema definition for the VinMart dataset
        df_schema = StructType([
            StructField("STORE", StringType()),
            StructField("STORE_NAME", StringType()),  # nvarchar maps to StringType in Snowflake
            StructField("MCH5_MC", StringType()),
            StructField("MC", StringType()),  # nvarchar maps to StringType in Snowflake
            StructField("ARTICLE", StringType()),
            StructField("ARTICLE_NAME", StringType()),  # nvarchar maps to StringType in Snowflake
            StructField("MANUFACTURER", StringType()),  # nvarchar maps to StringType in Snowflake
            StructField("REGION", StringType()),  # nvarchar maps to StringType in Snowflake
            StructField("POS_QUANTITY", StringType()),
            StructField("POS_REVENUE", StringType())
        ])
        
        # Reading the data into a DataFrame with the specified schema and options
        df = session.read \\
            .schema(df_schema) \\
            .option("skip_header", 1) \\
            .option("field_delimiter", "\\x01") \\
            .option("encoding", "UTF-8") \\
            .option("REPLACE_INVALID_CHARACTERS", True) \\
            .option("field_optionally_enclosed_by", "\\"") \\
            .option("null_if", "") \\
            .csv(f"@{stage_name}/{temp_stage_path}/{file_name}")
        
        current_timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        
        # Adding the file name and run_id as additional columns
        df = df.with_column("FILE_NAME", lit(file_name)) \\
               .with_column("RUN_ID", lit(current_timestamp).cast(DecimalType(14,0)))\\
               .with_column("CRTD_DTTM", lit(datetime.now().strftime("%Y%m%d%H%M%S")))
        
        # Preparing the DataFrame for insertion into the target table by selecting the appropriate columns
        snowdf = df.select(
            "STORE",
            "STORE_NAME",
            "MCH5_MC",
            "MC",
            "ARTICLE",
            "ARTICLE_NAME",
            "MANUFACTURER",
            "REGION",
            "POS_QUANTITY",
            "POS_REVENUE",
            "FILE_NAME",
            "RUN_ID",
            "CRTD_DTTM"
        )


        # Check if DataFrame is empty
        snowdf = snowdf.na.drop("all")
        if snowdf.count() == 0:
            return "No Data in table"

        # Archive to success
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")

        snowdf.write.copy_into_location(
            f"@{stage_name}/{temp_stage_path}/processed/success/{formatted_year}/{formatted_month}/{file_name}",
            header=True,
            overwrite=True
        )
        
        # Write operation
        snowdf.write.mode("append").save_as_table(stage_name.split(".")[0]+"."+target_table)
        
        # Success message
        return "Success"

    except KeyError as key_error:
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message

    except pd.errors.MergeError as merge_error:
        error_message = f"DataFrame merging error: {str(merge_error)}"
        return error_message
    
    except Exception as e:
        error_message = f"Error: {str(e)}"
        return error_message
';

CREATE OR REPLACE PROCEDURE VNMSDL_RAW.VN_MT_SELLIN_TARGET_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
PACKAGES = ('openpyxl==3.0.10','snowflake-snowpark-python==*')
HANDLER = 'main'
EXECUTE AS CALLER
AS 'import snowflake.snowpark as snowpark
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField ,DecimalType
from snowflake.snowpark.functions import lit
from datetime import datetime
import pytz

def main(session: snowpark.Session ,Param): 
    
	# SP call and parameters to pass.
	# CALL VNMSDL_RAW.VN_MT_SELLIN_TARGET_PREPROCESSING([''Target_Sell_In_Nov2021.csv'',''VNMSDL_RAW.PROD_LOAD_STAGE_ADLS'',''MT/transaction/Sellin/Transaction'',''sdl_vn_mt_sellin_target''])
	
    try:
        # Extracting parameters from the input

        file_name       = Param[0]   
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        db_name         = stage_name.split(''.'')[0]
        target_table    = db_name+"."+Param[3]
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")

        # Define the schema for the DataFrame
        df_schema=StructType([
            StructField(''mtd_code'' , StringType()),
            StructField(''mti_code'' , StringType()),
            StructField(''target'' , DecimalType()),
            StructField(''sellin_cycle'' , IntegerType()),
            StructField(''sellin_year'' , StringType()),
            StructField(''visit'' , StringType())
            ])
        # Set the current session schema
        
        session.use_schema(stage_name.split(''.'')[0])

        
        stage_path="@{0}/{1}/{2}".format(stage_name,temp_stage_path,file_name)
        print(stage_path)
        
        temp_df = session.read\\
        .schema(df_schema)\\
        .option("skip_header",1)\\
        .option("field_delimiter", "\\u0001")\\
        .option("field_optionally_enclosed_by", "\\"") \\
        .csv(stage_path)
        
        temp_df=temp_df.na.drop("all")
        if temp_df.count()==0:
            return "No Data in file"

        
        temp_df=temp_df.withColumn("filename",lit(file_name))
        temp_df = temp_df.withColumn("run_id",lit(lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S"))))
        temp_df = temp_df.withColumn("crtd_dttm",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
            
        
                    
        final_df=temp_df.select("mtd_code","mti_code","target","sellin_cycle","sellin_year","visit","filename","run_id","crtd_dttm")
                        


        file_name=file_name+''_''+datetime.now().strftime("%Y%m%d%H%M%S")
        
        # write to target table
        final_df.write.mode("append").saveAsTable(target_table)
        
        #move to success
        final_df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)


        return ''Success''

    except KeyError as key_error:
        # Handle KeyError (missing columns) here
        error_message = f"KeyError: {str(key_error)}"
        return error_message

    
    except Exception as e:
        # Handle exceptions here
        error_message = f"Error: {str(e)}"
        return error_message
';

CREATE OR REPLACE PROCEDURE VNMSDL_RAW.VN_NOTES_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS 'from snowflake.snowpark.functions import col, lit, date_format, current_timestamp, to_date, year, month, concat, format_number, regexp_replace, to_timestamp, when, trim, upper
from snowflake.snowpark.types import StringType, StructType, StructField
import snowflake.snowpark
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz

def main(session: snowpark.Session,Param):
    #Param=["Interface_Notes_20220905.csv","VNMSDL_RAW.DEV_LOAD_STAGE_ADLS","dev/VN_PS/Interface_Notes",""]
    try:
        file_name = Param[0]
        stage_name = Param[1]
        temp_stage_path = Param[2]
        target_table = Param[3]
        
        df_schema = StructType([
            StructField("cust_code", StringType(), True),
            StructField("slsper_id", StringType(), True),
            StructField("shop_code", StringType(), True),
            StructField("ise_id", StringType(), True),
            StructField("ques_no", StringType(), True),
            StructField("answer_seq", StringType(), True),
            StructField("answer_value", StringType(), True),
            StructField("createddate", StringType(), True),
        ])
        
        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "|")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
        
        df = df.na.drop("all")
        
        if df.count() == 0:
            return "No Data in file"

        columns_to_check = ["ise_id", "ques_no", "cust_code", "shop_code", "ques_no", "answer_seq", "createddate"]
        
        for i in columns_to_check:
            df_na=df.na.drop(subset=[i])
            if df_na.count()!=df.count():
                return f"null value in column {i}"

        df_dup1 = df.select("ise_id", "slsper_id", "cust_code", "shop_code", "ques_no", "answer_seq", "createddate")
            
        df_dup2 = df_dup1.dropDuplicates()
        
        if df_dup1.count() != df_dup2.count():
            return "Duplicate values" 
            
        df = df.withColumn("run_id", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        file_name1=file_name.split(".")[0]+"_"+ datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")+".csv"
        df = df.withColumn("file_name", lit(file_name1))
        df = df.withColumn("crtd_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))

        snowdf = df.select(
            "cust_code",
            "slsper_id",
            "shop_code",
            "ise_id",
            "ques_no",
            "answer_seq",
            "answer_value",
            "createddate",
            "file_name",
            "crtd_dttm"
        )
        
        file_name=file_name.split(".")[0]+"_"+ datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        
        #move to success
        snowdf.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)
        
        snowdf.write.mode("append").saveAsTable(stage_name.split(".")[0] + "." + target_table)
        
        return "Success"

    except KeyError as key_error:
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message

    except pd.errors.MergeError as merge_error:
        error_message = f"DataFrame merging error: {str(merge_error)}"
        return error_message

    except Exception as e:
        error_message = f"Error: {str(e)}"
        return error_message
';

CREATE OR REPLACE PROCEDURE VNMSDL_RAW.VN_PRODUCT_MAPPING_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS 'from snowflake.snowpark.functions import col, lit, date_format, current_timestamp, to_date, year, month, concat, format_number, regexp_replace, to_timestamp, when, trim, upper
from snowflake.snowpark.types import StringType, StructType, StructField
import snowflake.snowpark
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz

def main(session: snowpark.Session, Param):
    try:
        file_name = Param[0]
        stage_name = Param[1]
        temp_stage_path = Param[2]
        target_table = Param[3]
        
        df_schema = StructType([
            StructField("putupid", StringType(), True),
            StructField("barcode", StringType(), True),
            StructField("productname", StringType(), True),
        ])
        
        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "\\u0001")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
        
        df = df.na.drop("all")
        
        if df.count() == 0:
            return "No Data in file"

        columns_to_check = ["putupid", "barcode", "productname"]
        
        for i in columns_to_check:
            df_na=df.na.drop(subset=[i])
            if df_na.count()!=df.count():
                return f"null value in column {i}"

        df_dup1 = df.select("putupid", "barcode", "productname")
            
        df_dup2 = df_dup1.dropDuplicates()
        
        if df_dup1.count() != df_dup2.count():
            return "Duplicate values" 
            
        df = df.withColumn("run_id", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        file_name1=file_name.split(".")[0]+"_"+ datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")+".csv"
        df = df.withColumn("file_name", lit(file_name1))
        df = df.withColumn("crtd_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))

        snowdf = df.select(
            "putupid",
            "barcode",
            "productname",
            "file_name",
            "crtd_dttm"
        )
        
        file_name=file_name.split(".")[0]+"_"+ datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        
        #move to success
        snowdf.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)
        
        snowdf.write.mode("append").saveAsTable(stage_name.split(".")[0] + "." + target_table)
        
        return "Success"

    except KeyError as key_error:
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message

    except pd.errors.MergeError as merge_error:
        error_message = f"DataFrame merging error: {str(merge_error)}"
        return error_message

    except Exception as e:
        error_message = f"Error: {str(e)}"
        return error_message
';


CREATE OR REPLACE PROCEDURE VNMSDL_RAW.VN_QUESTION_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS 'from snowflake.snowpark.functions import col, lit, date_format, current_timestamp, to_date, year, month, concat, format_number, regexp_replace, to_timestamp, when, trim, upper
from snowflake.snowpark.types import StringType, StructType, StructField
import snowflake.snowpark
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz

def main(session: snowpark.Session,Param):
    #Param=["Interface_Question_20220905.csv","VNMSDL_RAW.DEV_LOAD_STAGE_ADLS","dev/VN_PS/Interface_Questions",""]
    try:
        file_name = Param[0]
        stage_name = Param[1]
        temp_stage_path = Param[2]
        target_table = Param[3]
        
        df_schema = StructType([
            StructField("ise_id", StringType(), True),
            StructField("channel_id", StringType(), True),
            StructField("channel", StringType(), True),
            StructField("ques_no", StringType(), True),
            StructField("ques_code", StringType(), True),
            StructField("ques_desc", StringType(), True),
            StructField("standard_ques", StringType(), True),
            StructField("ques_class_code", StringType(), True),
            StructField("ques_class_desc", StringType(), True),
            StructField("weigh", StringType(), True),
            StructField("total_score", StringType(), True),
            StructField("answer_code", StringType(), True),
            StructField("answer_desc", StringType(), True),
            StructField("franchise_code", StringType(), True),
            StructField("franchise_name", StringType(), True)
        ])
        
        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "|")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
        
        df = df.na.drop("all")
        
        if df.count() == 0:
            return "No Data in file"

        columns_to_check = ["ise_id", "ques_no"]
        
        for i in columns_to_check:
            df_na=df.na.drop(subset=[i])
            if df_na.count()!=df.count():
                return f"null value in column {i}"

        df_dup1 = df.select("ise_id", "ques_no")
            
        df_dup2 = df_dup1.dropDuplicates()
        
        if df_dup1.count() != df_dup2.count():
            return "Duplicate values" 
            
        df = df.withColumn("run_id", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        file_name1=file_name.split(".")[0]+"_"+ datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")+".csv"
        df = df.withColumn("file_name", lit(file_name1))
        df = df.withColumn("crtd_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))

        snowdf = df.select(
            "ise_id",
            "channel_id",
            "channel",
            "ques_no",
            "ques_code",
            "ques_desc",
            "standard_ques",
            "ques_class_code",
            "ques_class_desc",
            "weigh",
            "total_score",
            "answer_code",
            "answer_desc",
            "franchise_code",
            "franchise_name",
            "file_name",
            "crtd_dttm"
        )
        
        file_name=file_name.split(".")[0]+"_"+ datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        
        #move to success
        snowdf.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)
        
        snowdf.write.mode("append").saveAsTable(stage_name.split(".")[0] + "." + target_table)
        
        return "Success"

    except KeyError as key_error:
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message

    except pd.errors.MergeError as merge_error:
        error_message = f"DataFrame merging error: {str(merge_error)}"
        return error_message

    except Exception as e:
        error_message = f"Error: {str(e)}"
        return error_message
';
