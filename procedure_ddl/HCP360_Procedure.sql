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
import pandas as pd
import zipfile
from snowflake.snowpark.files import SnowflakeFile

def main(session: snowpark.Session,Param):
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        sch_name        = stage_name.split('.')[0]
        target_table    = sch_name+"."+Param[3]

        full_path = "@"+stage_name+"/"+temp_stage_path+"/"+file_name
        pdf = pd.DataFrame()
        with SnowflakeFile.open(full_path,'rb',require_scoped_url=False) as fhandle:
            with zipfile.ZipFile(fhandle,"r") as zip_ref:
            # Iterate over each file in the zip archive
                for i_file_name in zip_ref.namelist():
                    with zip_ref.open(i_file_name) as file:
                        source_df = pd.read_csv(file, delimiter="~", header =0)
                        pdf = pd.concat([pdf, source_df])
                       
        df= session.create_dataframe(pdf)

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
        final_df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,file_format_type="csv",OVERWRITE=True,header=True,format_type_options=dict(FIELD_OPTIONALLY_ENCLOSED_BY="\""))
   
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
import pandas as pd
import zipfile
from snowflake.snowpark.files import SnowflakeFile


def main(session: snowpark.Session,Param):
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        sch_name        = stage_name.split('.')[0]
        target_table    = sch_name+"."+Param[3]

        full_path = "@"+stage_name+"/"+temp_stage_path+"/"+file_name
        pdf = pd.DataFrame()
        with SnowflakeFile.open(full_path,'rb',require_scoped_url=False) as fhandle:
            with zipfile.ZipFile(fhandle,"r") as zip_ref:
            # Iterate over each file in the zip archive
                for i_file_name in zip_ref.namelist():
                    with zip_ref.open(i_file_name) as file:
                        source_df = pd.read_csv(file, delimiter="~", header =0)
                        pdf = pd.concat([pdf, source_df])
                       
        df= session.create_dataframe(pdf)

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
import pandas as pd
import zipfile
from snowflake.snowpark.files import SnowflakeFile


def main(session: snowpark.Session,Param):
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        sch_name        = stage_name.split('.')[0]
        target_table    = sch_name+"."+Param[3]

        full_path = "@"+stage_name+"/"+temp_stage_path+"/"+file_name
        pdf = pd.DataFrame()
        with SnowflakeFile.open(full_path,'rb',require_scoped_url=False) as fhandle:
            with zipfile.ZipFile(fhandle,"r") as zip_ref:
            # Iterate over each file in the zip archive
                for i_file_name in zip_ref.namelist():
                    with zip_ref.open(i_file_name) as file:
                        source_df = pd.read_csv(file, delimiter="~", header =0, encoding_errors = "replace", quoting=3, na_filter=False)
                        for column in source_df.columns:
                            source_df[column] = source_df[column].apply(str)
                        pdf = pd.concat([pdf, source_df])
                       
        df= session.create_dataframe(pdf)

        df = df.withColumn("CRT_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        df = df.withColumn("FILE_NAME",lit(file_name).cast("string"))

        df = df.withColumn("CONSENTFLAG", col('"Consent_Flag"'))
        df = df.withColumn("CONSENTUPDATETIME", when(col('"Consent_Update_Datetime"') == lit(""), lit(None)).otherwise(col('"Consent_Update_Datetime"')))
        df = df.withColumn("FIRSTRXDATE", when(col("FIRST_RX_DATE") == lit(""), lit(None)).otherwise(col("FIRST_RX_DATE")))
        df = df.withColumn("CUSTENDTEREDDATE", when(col("CUST_ENDTERED_DATE") == lit(""), lit(None)).otherwise(col("CUST_ENDTERED_DATE")))
        
        final_df = df.select(
            "TEAM_NAME", "V_CUSTID", "V_TERRID", "CUST_NAME", "CUST_TYPE", "CUST_QUAL", "CUST_SPEC", "CORE_NONCORE", "CLASSIFICATION",\
            "IS_FBM_ADOPTED", "VISITS_PER_MONTH", "CELL_PHONE", "PHONE", "EMAIL", "CITY", "STATE", "IS_ACTIVE", "FIRSTRXDATE", \
            "CRT_DTTM", "FILE_NAME", "CUSTENDTEREDDATE", "CONSENTFLAG", \
            to_timestamp("CONSENTUPDATETIME", lit("MM/DD/YYYY HH12:MI:SS AM")).as_("CONSENTUPDATETIME"), \

        ).alias("final_df")

        if final_df.count()==0:
            return "No Data in file"

        
        file_df = df.select(
            "TEAM_NAME", "V_CUSTID", "V_TERRID", "CUST_NAME", "CUST_TYPE", "CUST_QUAL", "CUST_SPEC", "CORE_NONCORE", "CLASSIFICATION",\
            "IS_FBM_ADOPTED", "VISITS_PER_MONTH", "CELL_PHONE", "PHONE", "EMAIL", "CITY", "STATE", "IS_ACTIVE", "FIRST_RX_DATE", \
            "CRT_DTTM", "FILE_NAME", "CUST_ENDTERED_DATE", '"Consent_Flag"', \
            '"Consent_Update_Datetime"', \
        ).alias("file_df")
        
        # Load Data to the target table
        final_df.write.mode("append").saveAsTable(target_table)

        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")

        # write to success folder
    
        file_name=file_name.split(".")[0] + '_' + datetime.now().strftime("%Y%m%d%H%M%S")
        file_df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,file_format_type="csv",OVERWRITE=True,header=True,format_type_options=dict(FIELD_OPTIONALLY_ENCLOSED_BY="\""))
   
        return "Success"
        
        
    except KeyError as key_error:
        
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message
        
    except Exception as e:
        
        error_message = f"Error: {str(e)}" +str(df.columns)
        return error_message
$$;