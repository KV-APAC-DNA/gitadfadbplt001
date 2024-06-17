SET ENV ='DEV';

SET DB = $ENV||'_DNA_LOAD';

USE DATABASE identifier($DB);

USE SCHEMA HCPSDL_RAW;


CREATE OR REPLACE PROCEDURE HCPSDL_RAW.HCP360_IN_SFMC_LOAD_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS
$$
from snowflake.snowpark.functions import col, lit, date_format, current_timestamp, to_date, year, month, concat, format_number, regexp_replace,to_timestamp,when,trim,upper
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DecimalType,DateType
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz


def main(session: snowpark.Session,Param):
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        sch_name        = stage_name.split('.')[0]
        target_table    = sch_name+"."+Param[3]
       
        df = session.read\
            .option("INFER_SCHEMA", True)\
            .option("skip_header",0)\
            .option("field_delimiter", "|")\
            .option("field_optionally_enclosed_by", "\"") \
            .option("encoding","UTF-16LE") \
            .option("trim_space", True) \
            .with_metadata("METADATA$FILE_ROW_NUMBER") \
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        df = df.filter(col("METADATA$FILE_ROW_NUMBER") > lit(1))
        
        df = df.drop("METADATA$FILE_ROW_NUMBER")

        df = df.withColumn("CRT_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        
        final_df = df

        if final_df.count()==0:
            return "No Data in file"
        
        # Load Data to the target table
        final_df.write.mode("append").saveAsTable(target_table)

        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")

        # write to success folder
    
        file_name=file_name.split(".")[0] + '_' + datetime.now().strftime("%Y%m%d%H%M%S")
        final_df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,file_format_type="csv",OVERWRITE=True,header=True)
   
        return "Success"
        
    except KeyError as key_error:
        
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message
        
    except Exception as e:
        
        error_message = f"Error: {str(e)}" +str(df.columns)
        return error_message
$$;



CREATE OR REPLACE PROCEDURE HCPSDL_RAW.HCP360_IN_VENTASYS_LOAD_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS
$$
from snowflake.snowpark.functions import col, lit, date_format, current_timestamp, to_date, year, month, concat, format_number, regexp_replace,to_timestamp,when,trim,upper
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DecimalType,DateType
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz


def main(session: snowpark.Session,Param):
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        sch_name        = stage_name.split('.')[0]
        target_table    = sch_name+"."+Param[3]
       
        df = session.read\
            .option("INFER_SCHEMA", True)\
            .option("skip_header",0)\
            .option("field_delimiter", "~")\
            .option("field_optionally_enclosed_by", "\"") \
            .option("REPLACE_INVALID_CHARACTERS",True) \
            .option("trim_space", True) \
            .with_metadata("METADATA$FILE_ROW_NUMBER") \
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        df = df.filter(col("METADATA$FILE_ROW_NUMBER") > lit(1))
        
        df = df.drop("METADATA$FILE_ROW_NUMBER")

        df = df.withColumn("CRT_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        df = df.withColumn("FILE_NAME",lit(file_name).cast("string"))
        
        final_df = df

        if final_df.count()==0:
            return "No Data in file"
        
        # Load Data to the target table
        final_df.write.mode("append").saveAsTable(target_table)

        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")

        # write to success folder
    
        file_name=file_name.split(".")[0] + '_' + datetime.now().strftime("%Y%m%d%H%M%S")
        final_df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,file_format_type="csv",OVERWRITE=True,header=True)
   
        return "Success"
        
    except KeyError as key_error:
        
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message
        
    except Exception as e:
        
        error_message = f"Error: {str(e)}" +str(df.columns)
        return error_message
$$;



CREATE OR REPLACE PROCEDURE HCPSDL_RAW.HCP360_IN_VENTASYS_SAMPLEDATA_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS
$$
from snowflake.snowpark.functions import col, lit, date_format, current_timestamp, to_date, year, month, concat, format_number, regexp_replace,to_timestamp,when,trim,upper
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DecimalType,DateType
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz


def main(session: snowpark.Session,Param):
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        sch_name        = stage_name.split('.')[0]
        target_table    = sch_name+"."+Param[3]
    
        df_schema=StructType([
            StructField("TEAM_NAME", StringType()),
            StructField("V_SAMPLEID", StringType()),
            StructField("V_EMPID", StringType()),
            StructField("V_CUSTID", StringType()),
            StructField("DCR_DT", StringType()),
            StructField("SAMPLE_PRODUCT", StringType()),
            StructField("SAMPLE_UNITS", StringType()),
            StructField("CATEGORY", StringType())
        ])


        df = session.read\
            .schema(df_schema)\
            .option("skip_header",1)\
            .option("field_delimiter", "~")\
            .option("field_optionally_enclosed_by", None) \
            .option("REPLACE_INVALID_CHARACTERS",True) \
            .option("trim_space", True) \
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        df = df.withColumn("CRT_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        df = df.withColumn("FILE_NAME",lit(file_name).cast("string"))
        
        final_df = df.select("TEAM_NAME", "V_SAMPLEID", "V_EMPID", "V_CUSTID", "DCR_DT", \
            "SAMPLE_PRODUCT", "SAMPLE_UNITS", "CRT_DTTM", "FILE_NAME", "CATEGORY" ).alias("final_df")

        if final_df.count()==0:
            return "No Data in file"
        
        # Load Data to the target table
        final_df.write.mode("append").saveAsTable(target_table)

        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")

        # write to success folder
    
        file_name=file_name.split(".")[0] + '_' + datetime.now().strftime("%Y%m%d%H%M%S")
        final_df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,file_format_type="csv",OVERWRITE=True,header=True)
   
        return "Success"
        
    except KeyError as key_error:
        
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message
        
    except Exception as e:
        
        error_message = f"Error: {str(e)}" +str(df.columns)
        return error_message
$$;


CREATE OR REPLACE PROCEDURE HCPSDL_RAW.HCP360_IN_VENTASYS_HCP_MASTER_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS
$$
from snowflake.snowpark.functions import col, lit, date_format, current_timestamp, to_date, year, month, concat, format_number, regexp_replace,to_timestamp,when,trim,upper
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DecimalType,DateType
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz


def main(session: snowpark.Session,Param):
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        sch_name        = stage_name.split('.')[0]
        target_table    = sch_name+"."+Param[3]
    
        df_schema=StructType([
            StructField("TEAM_NAME", StringType()),
            StructField("V_CUSTID", StringType()),
            StructField("V_TERRID", StringType()),
            StructField("CUST_NAME", StringType()),
            StructField("CUST_TYPE", StringType()),
            StructField("CUST_QUAL", StringType()),
            StructField("CUST_SPEC", StringType()),
            StructField("CORE_NONCORE", StringType()),
            StructField("CLASSIFICATION", StringType()),
            StructField("IS_FBM_ADOPTED", StringType()),
            StructField("VISITS_PER_MONTH", StringType()),
            StructField("CELL_PHONE", StringType()),
            StructField("PHONE", StringType()),
            StructField("EMAIL", StringType()),
            StructField("CITY", StringType()),
            StructField("STATE", StringType()),
            StructField("IS_ACTIVE", StringType()),
            StructField("FIRST_RX_DATE", StringType()),
            StructField("CUST_ENDTERED_DATE", StringType()),
            StructField("Consent_Flag", StringType()),
            StructField("Consent_Update_Datetime", StringType())
        ])


        df = session.read\
            .schema(df_schema)\
            .option("skip_header",1)\
            .option("field_delimiter", "~")\
            .option("field_optionally_enclosed_by", None) \
            .option("REPLACE_INVALID_CHARACTERS",True) \
            .option("trim_space", True) \
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        df = df.withColumn("CRT_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        df = df.withColumn("FILE_NAME",lit(file_name).cast("string"))
        
        final_df = df.select(
            "TEAM_NAME", "V_CUSTID", "V_TERRID", "CUST_NAME", "CUST_TYPE", "CUST_QUAL", "CUST_SPEC", "CORE_NONCORE", "CLASSIFICATION",\
            "IS_FBM_ADOPTED", "VISITS_PER_MONTH", "CELL_PHONE", "PHONE", "EMAIL", "CITY", "STATE", "IS_ACTIVE", "FIRST_RX_DATE", \
            "CRT_DTTM", "FILE_NAME", "CUST_ENDTERED_DATE", "Consent_Flag", \
            to_timestamp("Consent_Update_Datetime", lit("MM/DD/YYYY HH12:MI:SS AM")).as_("Consent_Update_Datetime"), \

        ).alias("final_df")

        if final_df.count()==0:
            return "No Data in file"
        
        # Load Data to the target table
        final_df.write.mode("append").saveAsTable(target_table)

        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")

        # write to success folder
    
        file_name=file_name.split(".")[0] + '_' + datetime.now().strftime("%Y%m%d%H%M%S")
        final_df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,file_format_type="csv",OVERWRITE=True,header=True)
   
        return "Success"
        
    except KeyError as key_error:
        
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message
        
    except Exception as e:
        
        error_message = f"Error: {str(e)}" +str(df.columns)
        return error_message
$$;