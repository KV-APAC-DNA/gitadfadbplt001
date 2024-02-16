SET ENV ='DEV';

SET DB = $ENV||'_DNA_LOAD';

USE DATABASE identifier($DB);

USE SCHEMA MYSSDL_RAW;


CREATE OR REPLACE PROCEDURE MY_AS_WATSONS_INV_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS CALLER
AS '

import snowflake.snowpark as snowpark
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DateType
from snowflake.snowpark.functions import col,lit,date_format,current_timestamp,to_timestamp,replace
import pandas as pd
from datetime import datetime
import pytz


def main(session: snowpark.Session, Param): 
    
    try:
        # Extracting parameters from the input

        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        db_name         = stage_name.split(''.'')[0]
        target_table    = db_name+"."+Param[3]
        
        # Define the schema for the DataFrame
        df_schema=StructType([
            StructField("CUSTOMER_CODE",StringType()),
            StructField("STORE_CODE",StringType()),
            StructField("YEAR",StringType()),
            StructField("MTH_CODE",StringType()),
            StructField("MATERIAL_CODE",StringType()),
            StructField("INV_QTY(PC)",StringType()),
            StructField("INV_VALUE(LP)",StringType())
            ])
        # Set the current session schema
       
        session.use_schema(stage_name.split(''.'')[0])
        
        # Read the CSV file into a DataFrame
        dataframe = session.read\\
        .schema(df_schema)\\
        .option("skip_header",1)\\
        .option("field_delimiter", "\\u0001")\\
        .option("field_optionally_enclosed_by", "\\"") \\
        .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
    
        # Add "FILENAME", "CRTD_DTTM" to the Dataframe

        dataframe = dataframe.withColumn("FILENAME",lit(file_name.replace(".csv",".xlsx")).cast("string"))
        
        #convertin time stamp into sg timezone
        dataframe = dataframe.with_column("CRTD_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        
        
        df = dataframe.select("CUSTOMER_CODE","STORE_CODE","YEAR",replace(col("MTH_CODE"), lit("/"), lit("")).alias("MTH_CODE"), "MATERIAL_CODE","INV_QTY(PC)","INV_VALUE(LP)", "FILENAME", "CRTD_DTTM")
        # Creating copy of the Dataframe
        final_df = df.alias("final_df")

        if final_df.count()==0:
            return "No Data in file"

        final_df=final_df.filter(col("CUSTOMER_CODE").is_not_null())

        
        # Load Data to the target table
        final_df.write.mode("append").saveAsTable(target_table)

    
        # write to success folder
    
        file_name=file_name.split(".")[0]+''_''+datetime.now().strftime("%Y%m%d%H%M%S")
        final_df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/success/"+file_name,file_format_type="csv",header=True,OVERWRITE=True)
        
        


        return "Success"

    except KeyError as key_error:
        # Handle KeyError (missing columns) here
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message
        
    except pd.errors.MergeError as merge_error:
        # Handle DataFrame merging error
        error_message = f"DataFrame merging error: {str(merge_error)}"
        return error_mess
    
    except Exception as e:
        # Handle exceptions here
        error_message = f"Error: {str(e)}"
        return error_message
';
----------------------------------------------------------------------------

CREATE OR REPLACE PROCEDURE MY_POS_MT_CUST_SALES_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS CALLER
AS '

import snowflake.snowpark as snowpark
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DateType
from snowflake.snowpark.functions import col,lit,date_format,current_timestamp,to_timestamp
import pandas as pd
from datetime import datetime
import pytz


def main(session: snowpark.Session, Param): 
    
    try:
        # Extracting parameters from the input

        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        db_name         = stage_name.split(''.'')[0]
        target_table    = db_name+"."+Param[3]
        
    
        # Define the schema for the DataFrame
        df_schema=StructType([
            StructField("CUSTOMER",StringType()),
            StructField("CUSTOMER_NAME",StringType()),
            StructField("STORE_CODE",StringType()),
            StructField("STORE_NAME",StringType()),
            StructField("DEPT_CODE",StringType()),
            StructField("DEPT_NAME",StringType()),
            StructField("ITEM_CODE",StringType()),
            StructField("ITEM_DESC",StringType()),
            StructField("YEAR_MTH",StringType()),
            StructField("WEEK_NO",StringType()),
            StructField("QTY",StringType()),
            StructField("SELLOUT_VALUE",StringType()),
            StructField("SAP_CODE",StringType())
            ])
            
        # Set the current session schema
        
        session.use_schema(stage_name.split(''.'')[0])
        
        # Read the CSV file into a DataFrame
        df = session.read\\
        .schema(df_schema)\\
        .option("skip_header",1)\\
        .option("field_delimiter", "\\u0001")\\
        .option("field_optionally_enclosed_by", "\\"") \\
        .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
    
         # Add  "FILE_NAME" to the Dataframe
        df = df.withColumn("FILE_NAME",lit(file_name).cast("string"))

        
# Add "CDL_DTTM", "CRTD_DTTM" to the Dataframe

        df = df.with_column("CDL_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        #convertin time stamp into sg timezone
        df = df.with_column("CRTD_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        # Creating copy of the Dataframe
        final_df = df.alias("final_df")

        if final_df.count()==0:
            return "No Data in file"

        final_df=final_df.filter(col("CUSTOMER").is_not_null())

        
        # Load Data to the target table
        final_df.write.mode("append").saveAsTable(target_table)

    
        # write to success folder
    
        file_name=file_name.split(".")[0]+''_''+datetime.now().strftime("%Y%m%d%H%M%S")
        final_df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/success/"+file_name,file_format_type="csv",header=True,OVERWRITE=True)
        
        


        return "Success"

    except KeyError as key_error:
        # Handle KeyError (missing columns) here
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message
        
    except pd.errors.MergeError as merge_error:
        # Handle DataFrame merging error
        error_message = f"DataFrame merging error: {str(merge_error)}"
        return error_mess
    
    except Exception as e:
        # Handle exceptions here
        error_message = f"Error: {str(e)}"
        return error_message
';

-------------------------------------------------------------
CREATE OR REPLACE PROCEDURE MY_POS_OUTLETMASTER_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS CALLER
AS '

import snowflake.snowpark as snowpark
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField
from snowflake.snowpark.functions import col,lit,date_format,current_timestamp,to_timestamp
import pandas as pd
from datetime import datetime
import pytz


def main(session: snowpark.Session, Param): 
    
    try:
        # Extracting parameters from the input

        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        db_name         = stage_name.split(''.'')[0]
        target_table    = db_name+"."+Param[3]
        
    
        # Define the schema for the DataFrame
        df_schema=StructType([
            StructField("CUSTOMER_CODE",StringType()),
            StructField("CUSTOMER",StringType()),
            StructField("STORE_CODE",StringType()),
            StructField("STORE_NAME",StringType()),
            StructField("DEPT_CODE",StringType()),
            StructField("DEPT_NAME",StringType()),
            StructField("REGION",StringType()),
            StructField("STORE_FORMAT",StringType()),
            StructField("STORE_TYPE",StringType())
            ])
        # Set the current session schema
        
        session.use_schema(stage_name.split(''.'')[0])
        
        # Read the CSV file into a DataFrame
        df = session.read\\
        .schema(df_schema)\\
        .option("skip_header",1)\\
        .option("field_delimiter", "\\u0001")\\
        .option("field_optionally_enclosed_by", "\\"") \\
        .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
    
        # Add "CDL_DTTM", "CRTD_DTTM" to the Dataframe

        df = df.with_column("CDL_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        #convertin time stamp into sg timezone
        df = df.with_column("CRTD_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        

        # Creating copy of the Dataframe
        final_df = df.alias("final_df")

        if final_df.count()==0:
            return "No Data in file"

        final_df=final_df.filter(col("CUSTOMER_CODE").is_not_null()) 

        
        # Load Data to the target table
        final_df.write.mode("append").saveAsTable(target_table)

    
        # write to success folder
    
        file_name=file_name.split(".")[0]+''_''+datetime.now().strftime("%Y%m%d%H%M%S")
        final_df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/success/"+file_name,file_format_type="csv",header=True,OVERWRITE=True)
        
        


        return "Success"

    except KeyError as key_error:
        # Handle KeyError (missing columns) here
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message
        
    except pd.errors.MergeError as merge_error:
        # Handle DataFrame merging error
        error_message = f"DataFrame merging error: {str(merge_error)}"
        return error_mess
    
    except Exception as e:
        # Handle exceptions here
        error_message = f"Error: {str(e)}"
        return error_message
';