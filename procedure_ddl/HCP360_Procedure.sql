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
