CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.MYSSDL_RAW.MY_SELLIN_CUSTMASTER_PREPROCESSING("PARAM" ARRAY)
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
from snowflake.snowpark.functions import col,lit,date_format,current_timestamp,to_timestamp
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField,DecimalType,DateType
import pandas as pd
from datetime import datetime
import pytz


def main(session: snowpark.Session,Param): 

    try:

        #Param=[''MY_Sellin_CustomerMaster.csv'',''MYSSDL_RAW.DEV_LOAD_STAGE_ADLS'',''dev/Sellin/Masters'',''SDL_MY_CUSTOMER_DIM'']

        # Extracting parameters from the input
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        

        # Define the schema for the DataFrame
        df_schema=StructType([
            StructField("CUST_ID",StringType()),
            StructField("CUST_NM",StringType()),
            StructField("DSTRBTR_GRP_CD",StringType()),
            StructField("DSTRBTR_GRP_NM",StringType()),
            StructField("ULLAGE",StringType()),
            StructField("CHNL",StringType()),
            StructField("TERRITORY",StringType()),
            StructField("RETAIL_ENV",StringType()),
            StructField("TRDNG_TERM_VAL",StringType()),
            StructField("RDD_IND",StringType())
            
            ])


        # Read the CSV file into a DataFrame
    
        dataframe = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "\\u0001")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
        

            #---------------------------Transformation logic ------------------------------#

        # Add CDL_DTTM and CURR_DT to the dataframe

        dataframe = dataframe.with_column("CDL_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        dataframe = dataframe.with_column("CURR_DT",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        dataframe = dataframe.with_column("FILE_NAME",lit(Param[0].split(".")[0]+ ".xlsx"))


        # Creating copy of the Dataframe
        final_df = dataframe.alias("final_df")

        if final_df.count()==0:
            return "No Data in file"


        final_df=final_df.filter(col("CUST_ID").is_not_null()) and final_df.filter(col("DSTRBTR_GRP_CD").is_not_null())

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
        
    except Exception as e:
        # Handle exceptions here
        error_message = f"Error: {str(e)}"
        return error_message';

CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.MYSSDL_RAW.MY_SELLOUT_DISTRIBUTOR_HIERARCHY_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
COMMENT='preprocessing structural changes for file MY_GT_Distributor_Hierarchy.csv'
EXECUTE AS OWNER
AS '# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them.

import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col,lit
from snowflake.snowpark.types import  StringType, StructType, StructField
import pandas as pd
from datetime import datetime
import pytz


def main(session: snowpark.Session,Param): 

    try:
        #Param=[''MY_GT_Distributor_Hierarchy.csv'',''MYSSDL_RAW.DEV_LOAD_STAGE_ADLS'',''dev/master/sellout'',''SDL_MY_DSTRBTRR_DIM'']

        # Extracting parameters from the input
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]

         # Define the schema
        df_schema = StructType([
            StructField("distributor_id", StringType()),
            StructField("distributor_name", StringType()),
            StructField("level_1", StringType()),
            StructField("level_2", StringType()),
            StructField("level_3", StringType()),
            StructField("level_4", StringType()),
            StructField("level_5", StringType()),
            StructField("tradeterm", StringType()),
            StructField("abbreviation", StringType()),
            StructField("buyer_gln", StringType()),
            StructField("location_gln", StringType()),
            StructField("channel_manager", StringType()),
            StructField("cdm", StringType()),
            StructField("region", StringType())
        ])
        
                
        dataframe = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "\\u0001")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        # Add CDL_DTTM and CURR_DT to the dataframe
        dataframe = dataframe.with_column("CDL_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        dataframe = dataframe.with_column("CURR_DT",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        dataframe = dataframe.with_column("FILE_NAME",lit(file_name.split(".")[0]+".xlsx"))

        #drop the column which has no DISTRIBUTOR_ID 
        dataframe=dataframe.filter(col("DISTRIBUTOR_ID").is_not_null())

        #if file has not 
        if dataframe.count()==0:
            return "No Data in file"

         # Load Data to the target table
        dataframe.write.mode("append").saveAsTable(target_table)

        # write to success folder
        #file_name=file_name.split(".")[0]+''_''+datetime.now().strftime("%Y%m%d%H%M%S")
        
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m") 
        dataframe.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,file_format_type="csv",header=True,OVERWRITE=True)

    
        return ''Success''

    except KeyError as key_error:
    # Handle KeyError (missing columns) here
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message
    
    except Exception as e:
    # Handle exceptions here
        error_message = f"Error: {str(e)}"
        return error_message


    ';

CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.MYSSDL_RAW.MY_SELLOUT_DISTRIBUTOR_DOC_PREPROCESSING("PARAM" ARRAY)
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
from snowflake.snowpark.types import  StringType, StructType, StructField
import pandas as pd
from datetime import datetime
import pytz


def main(session: snowpark.Session,Param): 

    try:
        #Param=[''MY_GT_Distributor_Doc_Type.csv'',''MYSSDL_RAW.DEV_LOAD_STAGE_ADLS'',''dev/master/sellout'',''SDL_MY_DSTRBTR_DOC_TYPE'']

        # Extracting parameters from the input
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]

         # Define the schema
        df_schema = StructType([
                StructField("dist_code", StringType()),
                StructField("dist_name", StringType()),
                StructField("level1", StringType()),
                StructField("level2", StringType()),
                StructField("wh_id", StringType()),
                StructField("doc_type", StringType()),
                StructField("doc_type_des", StringType())
            ])
        
                
        dataframe = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "\\u0001")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        # Add CDL_DTTM and CURR_DT to the dataframe
        dataframe = dataframe.with_column("CDL_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        dataframe = dataframe.with_column("CURR_DT",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        dataframe = dataframe.with_column("FILE_NAME", lit(file_name.split(".")[0]+ ".xlsx"))

        #drop the column which has no DISTRIBUTOR_ID 
        dataframe=dataframe.filter(col("dist_code").is_not_null())

        #if file has not 
        if dataframe.count()==0:
            return "No Data in file"

        #print(dataframe.show())

         # Load Data to the target table
        dataframe.write.mode("append").saveAsTable(target_table)

        # write to success folder
        file_name=file_name.split(".")[0]+''_''+datetime.now().strftime("%Y%m%d%H%M%S")
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        dataframe.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,file_format_type="csv",header=True,OVERWRITE=True)

    
        return ''Success''

    except KeyError as key_error:
    # Handle KeyError (missing columns) here
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message
    
    except Exception as e:
    # Handle exceptions here
        error_message = f"Error: {str(e)}"
        return error_message


    ';

CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.MYSSDL_RAW.MY_ACCRUAL_PREPROCESSING("PARAM" ARRAY)
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
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField,DecimalType,DateType
import pandas as pd
from datetime import datetime
import pytz

def main(session: snowpark.Session,Param): 

    try:

        #Param=[''MY_Accrual.csv'',''MYSSDL_RAW.DEV_LOAD_STAGE_ADLS'',''dev/others/sellin/'',''SDL_MY_ACCRUALS'']

        # Extracting parameters from the input
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        

        # Define the schema for the DataFrame
        df_schema=StructType([
            StructField("CUST_ID",StringType()),
            StructField("CUST_NM",StringType()),
            StructField("JJ_YEAR",StringType()),
            StructField("FILE_TYPE",StringType()),
            StructField("JAN_VAL",StringType()),
            StructField("FEB_VAL",StringType()),
            StructField("MAR_VAL",StringType()),
            StructField("APR_VAL",StringType()),
            StructField("MAY_VAL",StringType()),
            StructField("JUN_VAL",StringType()),
            StructField("JUL_VAL",StringType()),
            StructField("AUG_VAL",StringType()),
            StructField("SEP_VAL",StringType()),
            StructField("OCT_VAL",StringType()),
            StructField("NOV_VAL",StringType()),
            StructField("DEC_VAL",StringType()),
            ])


        # Read the CSV file into a DataFrame
    
        dataframe = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "\\u0001")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
        

            #---------------------------Transformation logic ------------------------------#

         # Add CDL_DTTM and CURR_DT to the dataframe

        dataframe = dataframe.with_column("CDL_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        dataframe = dataframe.with_column("CURR_DT",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        dataframe = dataframe.with_column("FILE_NAME", lit(file_name.split(".")[0]+ ".xlsx"))


        # Creating copy of the Dataframe
        final_df = dataframe.alias("final_df")

        if final_df.count()==0:
            return "No Data in file"

        # Handle null values or empty rows
        
        final_df=final_df.filter(col("CUST_ID").is_not_null()) and final_df.filter(col("CUST_NM").is_not_null())

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
        
    except Exception as e:
        # Handle exceptions here
        error_message = f"Error: {str(e)}"
        return error_message
    
';
CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.MYSSDL_RAW.MY_POS_OUTLETMASTER_PREPROCESSING("PARAM" ARRAY)
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
            StructField("STORE_TYPE",StringType()),
            StructField("FILE_NAME",StringType())
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
        df = df.with_column("FILE_NAME", lit(file_name.split(".")[0]+ ".xlsx"))

        # Creating copy of the Dataframe
        final_df = df.alias("final_df")

        if final_df.count()==0:
            return "No Data in file"

        final_df=final_df.filter(col("CUSTOMER_CODE").is_not_null()) 

        
        # Load Data to the target table
        final_df.write.mode("append").saveAsTable(target_table)

    
        # write to success folder
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        file_name=file_name.split(".")[0]+''_''+datetime.now().strftime("%Y%m%d%H%M%S")
        final_df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,file_format_type="csv",header=True,OVERWRITE=True)
        
        


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

CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.myssdl_raw.MY_SELLOUT_GT_IN_TRANSIT_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them.

import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col,lit,to_date, date_format
from snowflake.snowpark.types import  StringType, StructType, StructField
import pandas as pd
from datetime import datetime
import pytz


def main(session: snowpark.Session,Param): 

    try:
        #Param=[''MY_GT_In_Transit_Inv.csv'',''MYSSDL_RAW.DEV_LOAD_STAGE_ADLS'',''dev/transaction/sellout/others'',''sdl_MY_IN_TRANSIT'']

        # Extracting parameters from the input
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]

         # Define the schema
        df_schema = StructType([
                StructField("billing_doc", StringType()),
                StructField("billing_date", StringType()),
                StructField("gr_date", StringType()),
                StructField("closing_date", StringType()),
                StructField("remarks", StringType())
            ])
        
               
        dataframe = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "\\u0001")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
        dataframe=dataframe.na.drop("all")
        if dataframe.count()==0:
            return "No Data in file"

        # Add CDL_DTTM and CURR_DT to the dataframe
        dataframe = dataframe.with_column("CDL_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        dataframe = dataframe.with_column("CURR_DT",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))

       
        dataframe=dataframe.to_pandas()
        # Convert the date column to datetime format
        dataframe["BILLING_DATE"] = pd.to_datetime(dataframe["BILLING_DATE"]).dt.strftime("%d-%m-%Y")
        dataframe["GR_DATE"] = pd.to_datetime(dataframe["GR_DATE"]).dt.strftime("%d-%m-%Y")
        dataframe["CLOSING_DATE"] = pd.to_datetime(dataframe["CLOSING_DATE"]).dt.strftime("%d-%m-%Y")
       
        dataframe = session.create_dataframe(dataframe)
        
         # Load Data to the target table
        dataframe.write.mode("append").saveAsTable(target_table)

        # write to success folder
        file_name=file_name.split(".")[0]+''_''+datetime.now().strftime("%Y%m%d%H%M%S")
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        dataframe.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,file_format_type="csv",header=True,OVERWRITE=True)

        
        return ''Success''

    except KeyError as key_error:
    # Handle KeyError (missing columns) here
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message
    
    except Exception as e:
    # Handle exceptions here
        error_message = f"Error: {str(e)}"
        return error_message


    ';

CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.MYSSDL_RAW.MY_TARGETS_PREPROCESSING("PARAM" ARRAY)
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
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField,DecimalType,DateType
import pandas as pd
from datetime import datetime
import pytz

def main(session: snowpark.Session,Param):

    
    try:

        #Param=[''MY_Targets.csv'',''MYSSDL_RAW.DEV_LOAD_STAGE_ADLS'',''dev/targets/sellin/'',''SDL_MY_TRGTS'']

        # Extracting parameters from the input
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]

        # Define the schema for the DataFrame
        df_schema=StructType([
            StructField("CUST_ID",StringType()),
            StructField("CUST_NM",StringType()),
            StructField("BRND_DESC",StringType()),
            StructField("SUB_SEGMENT",StringType()),
            StructField("JJ_YEAR",StringType()),
            StructField("TRGT_TYPE",StringType()),
            StructField("TRGT_VAL_TYPE",StringType()),
            StructField("JAN_VAL",StringType()),
            StructField("FEB_VAL",StringType()),
            StructField("MAR_VAL",StringType()),
            StructField("APR_VAL",StringType()),
            StructField("MAY_VAL",StringType()),
            StructField("JUN_VAL",StringType()),
            StructField("JUL_VAL",StringType()),
            StructField("AUG_VAL",StringType()),
            StructField("SEP_VAL",StringType()),
            StructField("OCT_VAL",StringType()),
            StructField("NOV_VAL",StringType()),
            StructField("DEC_VAL",StringType())
            ])

        
        # Read the CSV file into a DataFrame
    
        dataframe = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "\\u0001")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
        

        #---------------------------Transformation logic ------------------------------#

         # Add CDL_DTTM and CURR_DT to the dataframe

        dataframe = dataframe.with_column("CDL_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))

        dataframe = dataframe.with_column("CURR_DT",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        dataframe = dataframe.with_column("FILE_NAME", lit(file_name.split(".")[0]+ ".xlsx"))

        # Creating copy of the Dataframe
        final_df = dataframe.alias("final_df")

        if final_df.count()==0:
            return "No Data in file"

        # Handle null values or empty rows
        
        final_df=final_df.filter(col("CUST_ID").is_not_null()) and final_df.filter(col("CUST_NM").is_not_null())


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
        
    except Exception as e:
        # Handle exceptions here
        error_message = f"Error: {str(e)}"
        return error_message';

CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.MYSSDL_RAW.MY_AFGR_PREPROCESSING("PARAM" ARRAY)
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
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField,DecimalType,DateType
import pandas as pd
from datetime import datetime
import pytz

def main(session: snowpark.Session,Param): 
    # Your code goes here, inside the "main" handler.

    try:

        #Param=[''MY_AFGR.csv'',''MYSSDL_RAW.DEV_LOAD_STAGE_ADLS'',''dev/others/sellin/'',''SDL_MY_AFGR'']

        # Extracting parameters from the input
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        

        # Define the schema for the DataFrame
        df_schema=StructType([
            StructField("CUST_ID",StringType()),
            StructField("CUST_NM",StringType()),
            StructField("AFGR_NUM",StringType()),
            StructField("CUST_DN_NUM",StringType()),
            StructField("DN_AMT_EXC_GST_VAL",StringType()),
            StructField("AFGR_AMT",StringType()),
            StructField("DT_TO_SC",StringType()),
            StructField("SC_VALIDATION",StringType()),
            StructField("RTN_ORD_NUM",StringType()),
            StructField("RTN_ORD_DT",StringType()),
            StructField("RTN_ORD_AMT",StringType()),
            StructField("CN_EXP_ISSUE_DT",StringType()),
            StructField("BILL_NUM",StringType()),
            StructField("BILL_DT",StringType()),
            StructField("CN_AMT",StringType())
            ])


        # Read the CSV file into a DataFrame
    
        dataframe = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "\\u0001")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
        

    #---------------------------Transformation logic ------------------------------#

        # Add CDL_DTTM and CURR_DT to the dataframe

        dataframe = dataframe.with_column("CDL_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))

        dataframe = dataframe.with_column("CURR_DT",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        dataframe = dataframe.with_column("FILE_NAME", lit(file_name.split(".")[0]+ ".xlsx"))

        # Creating copy of the Dataframe
        final_df = dataframe.alias("final_df")

        if final_df.count()==0:
            return "No Data in file"

        # Handle null values or empty rows
        
        final_df=final_df.filter(col("CUST_ID").is_not_null()) and final_df.filter(col("AFGR_NUM").is_not_null())

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
        
    except Exception as e:
        # Handle exceptions here
        error_message = f"Error: {str(e)}"
        return error_message';

CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.MYSSDL_RAW.MY_LE_TARGET_PREPROCESSING("PARAM" ARRAY)
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
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField,DecimalType,DateType
import pandas as pd
from datetime import datetime
import pytz

def main(session: snowpark.Session,Param):

    try:

        #Param=[''MY_LE_Target.csv'',''MYSSDL_RAW.DEV_LOAD_STAGE_ADLS'',''dev/targets/sellin/'',''SDL_MY_LE_TRGT'']

        # Extracting parameters from the input
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]

        # Define the schema for the DataFrame
        df_schema=StructType([
            StructField("CUST_ID",StringType()),
            StructField("CUST_NM",StringType()),
            StructField("JJ_YEAR",StringType()),
            StructField("MNTH_NM",StringType()),
            StructField("TRGT_TYPE",StringType()),
            StructField("TRGT_VAL_TYPE",StringType()),
            StructField("WK1",StringType()),
            StructField("WK2",StringType()),
            StructField("WK3",StringType()),
            StructField("WK4",StringType()),
            StructField("WK5",StringType())
            ])


        # Read the CSV file into a DataFrame
    
        dataframe = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "\\u0001")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        #---------------------------Transformation logic ------------------------------#

         # Add CDL_DTTM and CURR_DT to the dataframe

        dataframe = dataframe.with_column("CDL_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))

        dataframe = dataframe.with_column("CURR_DT",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        dataframe = dataframe.with_column("FILE_NAME", lit(file_name.split(".")[0]+ ".xlsx"))
        

        # Creating copy of the Dataframe
        final_df = dataframe.alias("final_df")

        if final_df.count()==0:
            return "No Data in file"

        # Handle null values or empty rows
        
        final_df=final_df.filter(col("CUST_ID").is_not_null()) and final_df.filter(col("CUST_NM").is_not_null())


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
        
    except Exception as e:
        # Handle exceptions here
        error_message = f"Error: {str(e)}"
        return error_message';

CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.MYSSDL_RAW.MY_CIW_MAP_PREPROCESSING("PARAM" ARRAY)
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
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField,DecimalType,DateType
import pandas as pd
from datetime import datetime
import pytz

def main(session: snowpark.Session,Param): 
    # Your code goes here, inside the "main" handler.

    try:

        #Param=[''MY_CIW.csv'',''MYSSDL_RAW.DEV_LOAD_STAGE_ADLS'',''dev/others/sellin/'',''SDL_MY_CIW_MAP'']

        # Extracting parameters from the input
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        

        # Define the schema for the DataFrame
        df_schema=StructType([
            StructField("CIW_CTGRY",StringType()),
            StructField("CIW_BUCKT1",StringType()),
            StructField("CIW_BUCKT2",StringType()),
            StructField("BRAVO_CD1",StringType()),
            StructField("BRAVO_DESC1",StringType()),
            StructField("BRAVO_CD2",StringType()),
            StructField("BRAVO_DESC2",StringType()),
            StructField("ACCT_TYPE",StringType()),
            StructField("ACCT_NUM",StringType()),
            StructField("ACCT_DESC",StringType()),
            StructField("ACCT_TYPE1",StringType())
            ])


        # Read the CSV file into a DataFrame
    
        dataframe = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "\\u0001")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
        

    #---------------------------Transformation logic ------------------------------#

        # Add CDL_DTTM and CURR_DT to the dataframe

        dataframe = dataframe.with_column("CDL_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))

        dataframe = dataframe.with_column("CURR_DT",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        dataframe = dataframe.with_column("FILE_NAME", lit(file_name.split(".")[0]+ ".xlsx"))

        # Creating copy of the Dataframe
        final_df = dataframe.alias("final_df")

        # Handle null values or empty rows
        
        final_df=final_df.filter(col("ACCT_TYPE").is_not_null()) and final_df.filter(col("ACCT_NUM").is_not_null())

        if final_df.count()==0:
            return "No Data in file"

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
        
    except Exception as e:
        # Handle exceptions here
        error_message = f"Error: {str(e)}"
        return error_message';

CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.MYSSDL_RAW.MY_SELLOUT_GT_IDS_EXCHNG_PREPROCESSING("PARAM" ARRAY)
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
from snowflake.snowpark.types import  StringType, StructType, StructField
import pandas as pd
from datetime import datetime
import pytz


def main(session: snowpark.Session,Param): 

    try:
        #Param=[''MY_GT_IDS_EXCHNG_RATE.csv'',''MYSSDL_RAW.DEV_LOAD_STAGE_ADLS'',''dev/transaction/sellout/others'',''SDL_RAW_MY_IDS_RATE'']

        # Extracting parameters from the input
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]

         # Define the schema
        df_schema = StructType([
                    StructField("distributor_code", StringType()),
                    StructField("distributor_name", StringType()),
                    StructField("exchange_rate", StringType()),
                    StructField("effective_month", StringType())
                ])
        
               
        dataframe = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "\\u0001")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        # Add CDL_DTTM and CURR_DT to the dataframe
        dataframe = dataframe.with_column("CDL_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        dataframe = dataframe.with_column("CURR_DT",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        dataframe = dataframe.with_column("FILE_NAME", lit(file_name.split(".")[0]+ ".xlsx"))

        #drop the column which has no DISTRIBUTOR_ID 
        dataframe=dataframe.filter(col("distributor_code").is_not_null())

        #if file has not 
        if dataframe.count()==0:
            return "No Data in file"

        
         # Load Data to the target table
        dataframe.write.mode("append").saveAsTable(target_table)

        # write to success folder
        file_name=file_name.split(".")[0]+''_''+datetime.now().strftime("%Y%m%d%H%M%S")
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")        
        dataframe.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,file_format_type="csv",header=True,OVERWRITE=True)

    
        return ''Success''

    except KeyError as key_error:
    # Handle KeyError (missing columns) here
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message
    
    except Exception as e:
    # Handle exceptions here
        error_message = f"Error: {str(e)}"
        return error_message


    ';

CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.VNMSDL_RAW.VN_DMS_YEARLY_TARGET_PREPROCESSING("PARAM" ARRAY)
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
    #Param=["OUT_CON_Yeartarget_2019.csv","VNMSDL_RAW.DEV_LOAD_STAGE_ADLS","dev/dms/master/manual_file","sdl_raw_vn_dms_yearly_target"]
    try:
        file_name = Param[0]
        stage_name = Param[1]
        temp_stage_path = Param[2]
        target_table = Param[3]

        df_schema = StructType([
            StructField("year", StringType(), True),
            StructField("kpi", StringType(), True),
            StructField("category", StringType(), True),
            StructField("target", StringType(), True),
        ])

        df = session.read \\
            .schema(df_schema) \\
            .option("skip_header", 1) \\
            .option("field_delimiter", "|") \\
            .option("field_optionally_enclosed_by", "\\"") \\
            .csv("@" + stage_name + "/" + temp_stage_path + "/" + file_name)

        df = df.na.drop("all")

        if df.count() == 0:
            return "No Data in file"

        df = df.withColumn("run_id", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        df = df.withColumn("file_name", lit(file_name))
        df = df.withColumn("crtd_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))

        snowdf = df.select(
            "year",
            "kpi",
            "category",
            "target",
            "crtd_dttm",
            "run_id",
            "file_name",
        )

        file_name = file_name.split(".")[0] + "_" + datetime.now(pytz.timezone("Asia/Singapore")).strftime(
            "%Y%m%d%H%M%S")

        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")

        # move to success
        snowdf.write.copy_into_location(
            "@" + stage_name + "/" + temp_stage_path + "/processed/success/" + formatted_year + "/" + formatted_month + "/" + file_name,
            header=True, OVERWRITE=True)

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


CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.VNMSDL_RAW.SDL_VN_DMS_KPI_SELLIN_SELLTHRGH("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '
from snowflake.snowpark.functions import col, lit, current_timestamp, trim, upper
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DecimalType,DateType
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz
def main(session:snowpark.Session, Param):
 
    try:
        file_name       = Param[0]  # File name
        stage_name      = Param[1]  # Stage name where files are located
        temp_stage_path = Param[2]  # Path in the stage
        target_table    = Param[3]  # Target table for the data
        df_schema = StructType([
            StructField("DSTRBTR_ID", StringType()),
            StructField("DSTRBTR_TYPE", StringType()),
            StructField("DSTRBTR_NAME", StringType()),
            StructField("CYCLE", StringType()),
            StructField("ORDERTYPE", StringType()),
            StructField("SELLIN_TG", StringType()),
            StructField("SELLIN_AC", StringType())
        ])
        df = session.read\\
        .schema(df_schema)\\
        .option("skip_header",1)\\
        .option("field_delimiter", "|")\\
        .option("encoding", "UTF-8") \\
        .option("field_optionally_enclosed_by", "\\"") \\
        .option("null_if", "") \\
        .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        #Checking for the null values 
        df=df.na.drop("all")
        if df.count()==0:
            return "No Data in file"

        
        # Perform necessary transformations
        df = df.with_column("CURR_DATE", current_timestamp())\\
               .with_column("RUN_ID", lit(None).cast(DecimalType(14,0)))\\
               .with_column("FILE_NAME", lit(file_name))
               
        snowdf = df.select(
            "DSTRBTR_ID",
            "DSTRBTR_TYPE",
            "DSTRBTR_NAME",
            "CYCLE",
            "ORDERTYPE",
            "SELLIN_TG",
            "SELLIN_AC",
            "CURR_DATE",
            "RUN_ID",
            "FILE_NAME"
        )
        
        # Archive successful loads for auditability
        file_name=file_name.split(".")[0]+"_"+datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        
        snowdf.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)
        
        # Write operation to append data to the target table
        snowdf.write.mode("append").save_as_table(stage_name.split(".")[0]+"."+target_table)
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


CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.VNMSDL_RAW.SDL_VN_DMS_PROMOTION_LIST("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '
from snowflake.snowpark.functions import col, lit, current_timestamp, trim, upper
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DecimalType,DateType
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
def main(session:snowpark.Session, Param):
    try:
        file_name       = Param[0]  # File name
        stage_name      = Param[1]  # Stage name where files are located
        temp_stage_path = Param[2]  # Path in the stage
        target_table    = Param[3]  # Target table for the data
        df_schema = StructType([
            StructField("DSTRBTR_ID", StringType()),
            StructField("CNTRY_CODE", StringType()),
            StructField("PROMOTION_ID", StringType()),
            StructField("PROMOTION_NAME", StringType()),
            StructField("PROMOTION_DESC", StringType()),
            StructField("START_DATE", StringType()),
            StructField("END_DATE", StringType()),
            StructField("STATUS", StringType())
        ])
        df = session.read\\
        .schema(df_schema)\\
        .option("skip_header",1)\\
        .option("field_delimiter", "|")\\
        .option("encoding", "UTF-8") \\
        .option("field_optionally_enclosed_by", "\\"") \\
        .option("null_if", "") \\
        .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
        df = df.with_column("STATUS", trim(col("STATUS"), lit('','')))\\
               .with_column("CURR_DATE", current_timestamp())\\
               .with_column("RUN_ID", lit(None).cast(DecimalType(14,0)))\\
               .with_column("FILE_NAME", lit(file_name))
               
        snowdf = df.select(
            "DSTRBTR_ID",
            "CNTRY_CODE",
            "PROMOTION_ID",
            "PROMOTION_NAME",
            "PROMOTION_DESC",
            "START_DATE",
            "END_DATE",
            "STATUS",
            "CURR_DATE",
            "RUN_ID",
            "FILE_NAME"
        )
        # Check if DataFrame is empty
        df = df.na.drop("all")
        
        if df.count() == 0:
            return "No Data in file"
          
          # Archive successful loads for auditability
        file_name1 = ("_").join(file_name.split("_")[0:5]) + "_" + datetime.now().strftime("%Y%m%d%H%M%S")
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        
        #move to success
        snowdf.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name1,header=True,OVERWRITE=True)
        # Write operation to append data to the target table
        snowdf.write.mode("append").save_as_table(stage_name.split(".")[0]+"."+target_table)
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


CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.VNMSDL_RAW.SDL_VN_DMS_SALES_ORG_DIM("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '
from snowflake.snowpark.functions import col, lit, current_timestamp, trim, upper
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DecimalType,DateType
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz

def main(session:snowpark.Session, Param):
##Param=["OUT_CON_CD_VN_20230727000503.csv","VNMSDL_RAW.DEV_LOAD_STAGE_ADLS","dev/dms/dms_source","SDL_VN_DMS_SALES_ORG_DIM"]
    try:
        file_name       = Param[0]  # File name
        stage_name      = Param[1]  # Stage name where files are located
        temp_stage_path = Param[2]  # Path in the stage
        target_table    = Param[3]  # Target table for the data
        df_schema = StructType([
            StructField("DSTRBTR_ID", StringType()),
            StructField("SALESREP_ID", StringType()),
            StructField("SALESREP_NAME", StringType()),
            StructField("SUP_CODE", StringType()),
            StructField("SALESREP_CRTDATE", StringType()),
            StructField("SALESREP_DATEOFF", StringType()),
            StructField("SUP_NAME", StringType()),
            StructField("SUP_ACTIVE", StringType()),
            StructField("SUP_CRTDATE", StringType()),
            StructField("SUP_DATEOFF", StringType()),
            StructField("ASM_ID", StringType()),
            StructField("ASM_NAME", StringType()),
            StructField("ACTIVE", StringType())
        ])
        df = session.read\\
        .schema(df_schema)\\
        .option("skip_header",1)\\
        .option("field_delimiter", "|")\\
        .option("encoding", "UTF-8") \\
        .option("field_optionally_enclosed_by", "\\"") \\
        .option("null_if", "") \\
        .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        df=df.na.drop("all")
        if df.count()==0 :
            return "No Data in file"
            
        df = df.with_column("SUP_ACTIVE", trim(upper(col("SUP_ACTIVE")), lit('','')))\\
               .with_column("ACTIVE", trim(upper(col("ACTIVE")), lit('','')))\\
               .with_column("CURR_DATE", current_timestamp())\\
               .with_column("RUN_ID", lit(None).cast(DecimalType(14,0)))\\
               .with_column("FILE_NAME", lit(file_name))
               
        snowdf = df.select(
            "DSTRBTR_ID",
            "SALESREP_ID",
            "SALESREP_NAME",
            "SUP_CODE",
            "SALESREP_CRTDATE",
            "SALESREP_DATEOFF",
            "SUP_NAME",
            "SUP_ACTIVE",
            "SUP_CRTDATE",
            "SUP_DATEOFF",
            "ASM_ID",
            "ASM_NAME",
            "ACTIVE",
            "CURR_DATE",
            "RUN_ID",
            "FILE_NAME"
        )
        
            
        # Archive successful loads for auditability
        file_name=file_name.split(".")[0]+"_"+datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        snowdf.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)
        
        # Write operation to append data to the target table
        snowdf.write.mode("append").save_as_table(stage_name.split(".")[0]+"."+target_table)
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


CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.VNMSDL_RAW.SDL_VN_DMS_PRODUCT_DIM("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '
from snowflake.snowpark.functions import col, lit, current_timestamp, trim, upper
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DecimalType,DateType
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
def main(session:snowpark.Session, Param):
    try:
        file_name       = Param[0]  # File name
        stage_name      = Param[1]  # Stage name where files are located
        temp_stage_path = Param[2]  # Path in the stage
        target_table    = Param[3]  # Target table for the data
        df_schema = StructType([
            StructField("PRODUCT_CODE", StringType()),
            StructField("PRODUCT_NAME", StringType()),
            StructField("PRODUCTCODESAP", StringType()),
            StructField("PRODUCTNAMESAP", StringType()),
            StructField("UNIT", StringType()),
            StructField("TAX_RATE", StringType()),
            StructField("WEIGHT", StringType()),
            StructField("VOLUME", StringType()),
            StructField("GROUPJB", StringType()),
            StructField("FRANCHISE", StringType()),
            StructField("BRAND", StringType()),
            StructField("VARIANT", StringType()),
            StructField("PRODUCT_GROUP", StringType()),
            StructField("ACTIVE", StringType())
        ])
        df = session.read\\
        .schema(df_schema)\\
        .option("skip_header",1)\\
        .option("field_delimiter", "|")\\
        .option("encoding", "UTF-8") \\
        .option("field_optionally_enclosed_by", "\\"") \\
        .option("null_if", "") \\
        .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
        # Perform transformations
        df = df.with_column("ACTIVE", trim(upper(col("ACTIVE"))))\\
               .with_column("CURR_DATE", current_timestamp())\\
               .with_column("RUN_ID", lit(None).cast(DecimalType(14,0)))\\
               .with_column("FILE_NAME", lit(file_name))
               
        snowdf = df.select(
            "PRODUCT_CODE",
            "PRODUCT_NAME",
            "PRODUCTCODESAP",
            "PRODUCTNAMESAP",
            "UNIT",
            "TAX_RATE",
            "WEIGHT",
            "VOLUME",
            "GROUPJB",
            "FRANCHISE",
            "BRAND",
            "VARIANT",
            "PRODUCT_GROUP",
            "ACTIVE",
            "CURR_DATE",
            "RUN_ID",
            "FILE_NAME"
        )
        # Check if DataFrame is empty
        df = df.na.drop("all")
        
        if df.count() == 0:
            return "No Data in file"
        # Archive successful loads for auditability
        file_name1 = ("_").join(file_name.split("_")[0:5]) + "_" + datetime.now().strftime("%Y%m%d%H%M%S")
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        
        #move to success
        snowdf.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name1,header=True,OVERWRITE=True)
        # Write operation to append data to the target table
        snowdf.write.mode("append").save_as_table(stage_name.split(".")[0]+"."+target_table)
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


CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.VNMSDL_RAW.VN_DMS_SELLOUT_DETAILS_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('regex==2023.10.3','snowflake-snowpark-python==*')
HANDLER = 'main'
EXECUTE AS OWNER
AS '# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them.

import snowflake.snowpark as snowpark
from snowflake import snowpark
from snowflake.snowpark.functions import col,lit,trim, substring
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField
import pandas as pd
from datetime import datetime
import pytz

def main(session: snowpark.Session,Param): 

    try:

        #Param=[''OUT_CON_D_VN_20240320000543.csv'',''VNMSDL_RAW.DEV_LOAD_STAGE_ADLS'',''dev/dms/transaction/dms_source/sellout_details'',''SDL_VN_DMS_D_SELLOUT_SALES_FACT'']

        # Extracting parameters from the input
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]


        # Define the schema for the DataFrame
        df_schema=StructType([
            StructField("DSTRBTR_ID",StringType()),
            StructField("CNTRY_CODE",StringType()),
            StructField("OUTLET_ID",StringType()),
            StructField("ORDER_DATE",StringType()),
            StructField("INVOICE_DATE",StringType()),
            StructField("ORDER_NO",StringType()),
            StructField("INVOICE_NO",StringType()),
            StructField("SALES_ROUTE_ID",StringType()),
            StructField("SALE_ROUTE_NAME",StringType()),
            StructField("SALES_GROUP",StringType()),
            StructField("SALESREP_ID",StringType()),
            StructField("SALESREP_NAME",StringType()),
            StructField("MATERIAL_CODE",StringType()),
            StructField("UOM",StringType()),
            StructField("GROSS_PRICE",StringType()),
            StructField("ORDERQTY",StringType()),
            StructField("QUANTITY",StringType()),
            StructField("TOTAL_SELLOUT_AFVAT_BFDISC",StringType()),
            StructField("DISCOUNT",StringType()),
            StructField("TOTAL_SELLOUT_AFVAT_AFDISC",StringType()),
            StructField("LINE_NUMBER",StringType()),
            StructField("PROMOTION_ID",StringType()),
            StructField("STATUS",StringType())
            ])


        # Read the CSV file into a DataFrame
    
        dataframe = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "|")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .option("skip_blank_lines", True)\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        #---------------------------Transformation logic ------------------------------#

        # Handle null values or empty rows
        dataframe=dataframe.na.drop("all")

        # Check for empty Dataframe
        if dataframe.count()==0:
            return "No Data in file"

        # Add curr_date, RUN_ID and source_file_name in Dataframe

        column_to_trim=["PROMOTION_ID","STATUS"]
        for col_name in column_to_trim:
            dataframe = dataframe.withColumn(col_name, substring(dataframe[col_name],1,50))


        
        dataframe = dataframe.with_column("CURR_DATE",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        dataframe = dataframe.with_column("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")).cast("integer"))
        dataframe = dataframe.with_column("FILE_NAME", lit(flie_name))


        # Creating Final Dataframe
        final_df = dataframe.alias("final_df")

        # Load Data to the target table
        final_df.write.mode("append").saveAsTable(target_table)
        

        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")

        # write to success folder
    
        file_name=file_name.split(".")[0]+''_''+datetime.now().strftime("%Y%m%d%H%M%S")
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


CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.VNMSDL_RAW.SDL_VN_DMS_CALL_DETAILS("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '
from snowflake.snowpark.functions import col, lit, current_timestamp, trim, upper
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DecimalType,DateType
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
def main(session:snowpark.Session, Param):
    try:
        file_name       = Param[0]  # File name
        stage_name      = Param[1]  # Stage name where files are located
        temp_stage_path = Param[2]  # Path in the stage
        target_table    = Param[3]  # Target table for the data
        df_schema = StructType([
            StructField("DSTRBTR_ID", StringType()),
            StructField("SALESREP_ID", StringType()),
            StructField("OUTLET_ID", StringType()),
            StructField("VISIT_DATE", StringType()),
            StructField("CHECKIN_TIME", StringType()),
            StructField("CHECKOUT_TIME", StringType()),
            StructField("REASON", StringType()),
            StructField("DISTANCE", StringType()),
            StructField("ORDERVISIT", StringType())
        ])
        df = session.read\\
        .schema(df_schema)\\
        .option("skip_header",1)\\
        .option("field_delimiter", "|")\\
        .option("encoding", "UTF-8") \\
        .option("field_optionally_enclosed_by", "\\"") \\
        .option("null_if", "") \\
        .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
        # Perform necessary transformations
        df = df.with_column("ORDERVISIT", trim(col("ORDERVISIT"), lit('','')))\\
               .with_column("CURR_DATE", current_timestamp())\\
               .with_column("RUN_ID", lit(None).cast(DecimalType(14,0)))\\
               .with_column("FILE_NAME", lit(file_name))
               
        snowdf = df.select(
            "DSTRBTR_ID",
            "SALESREP_ID",
            "OUTLET_ID",
            "VISIT_DATE",
            "CHECKIN_TIME",
            "CHECKOUT_TIME",
            "REASON",
            "DISTANCE",
            "ORDERVISIT",
            "CURR_DATE",
            "RUN_ID",
            "FILE_NAME"
        )
        df = df.na.drop("all")
        
        if df.count() == 0:
            return "No Data in file"
            
        # Archive successful loads for auditability
        file_name1 = ("_").join(file_name.split("_")[0:5]) + "_" + datetime.now().strftime("%Y%m%d%H%M%S")
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        
        #move to success
        snowdf.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name1,header=True,OVERWRITE=True)
        # Write operation to append data to the target table
        snowdf.write.mode("append").save_as_table(stage_name.split(".")[0]+"."+target_table)
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


CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.VNMSDL_RAW.SDL_VN_DMS_ORDER_PROMOTION("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '
from snowflake.snowpark.functions import col, lit, current_timestamp, trim, upper
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DecimalType,DateType
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
def main(session:snowpark.Session, Param):
    try:
        file_name       = Param[0]  # File name
        stage_name      = Param[1]  # Stage name where files are located
        temp_stage_path = Param[2]  # Path in the stage
        target_table    = Param[3]  # Target table for the data
        df_schema = StructType([
            StructField("BRANCH_ID", StringType()),
            StructField("PRO_ID", StringType()),
            StructField("ORD_NO", StringType()),
            StructField("LINE_REF", StringType()),
            StructField("DISC_TYPE", StringType()),
            StructField("BREAK_BY", StringType()),
            StructField("DISC_BREAK_LINE_REF", StringType()),
            StructField("DISCT_BL_AMT", StringType()),
            StructField("DISCT_BL_QTY", StringType()),
            StructField("FREE_ITEM_CODE", StringType()),
            StructField("FREE_ITEM_QTY", StringType()),
            StructField("DISC_AMT", StringType())
        ])
        df = session.read\\
        .schema(df_schema)\\
        .option("skip_header",1)\\
        .option("field_delimiter", "|")\\
        .option("encoding", "UTF-8") \\
        .option("field_optionally_enclosed_by", "\\"") \\
        .option("null_if", "") \\
        .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
        # Perform necessary transformations
        df = df.with_column("CURR_DATE", current_timestamp())\\
               .with_column("RUN_ID", lit(None).cast(DecimalType(14,0)))\\
               .with_column("FILE_NAME", lit(file_name))
               
        snowdf = df.select(
            "BRANCH_ID",
            "PRO_ID",
            "ORD_NO",
            "LINE_REF",
            "DISC_TYPE",
            "BREAK_BY",
            "DISC_BREAK_LINE_REF",
            "DISCT_BL_AMT",
            "DISCT_BL_QTY",
            "FREE_ITEM_CODE",
            "FREE_ITEM_QTY",
            "DISC_AMT",
            "CURR_DATE",
            "RUN_ID",
            "FILE_NAME"
        )
        df = df.na.drop("all")
        
        if df.count() == 0:
            return "No Data in file"
        # Archive successful loads for auditability
        file_name1 = ("_").join(file_name.split("_")[0:5]) + "_" + datetime.now().strftime("%Y%m%d%H%M%S")
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        
        #move to success
        snowdf.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name1,header=True,OVERWRITE=True)
        
        # Write operation to append data to the target table
        snowdf.write.mode("append").save_as_table(stage_name.split(".")[0]+"."+target_table)
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


CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.VNMSDL_RAW.SDL_VN_DMS_MSL("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '
from snowflake.snowpark.functions import col, lit, current_timestamp, trim, upper
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DecimalType,DateType
import pandas as pd
from datetime import datetime
import pytz
import snowflake.snowpark as snowpark
def main(session:snowpark.Session, Param):
    try:
        file_name       = Param[0]  # File name
        stage_name      = Param[1]  # Stage name where files are located
        temp_stage_path = Param[2]  # Path in the stage
        target_table    = Param[3]  # Target table for the data
        df_schema = StructType([
            StructField("MSL_ID", StringType()),
            StructField("SUB_CHANNEL", StringType()),
            StructField("FROM_CYCLE", StringType()),
            StructField("TO_CYCLE", StringType()),
            StructField("PRODUCT_ID", StringType()),
            StructField("PROUCT_NAME", StringType()),
            StructField("ACTIVE", StringType()),
            StructField("GROUPMSL", StringType())
        ])
        
        df = session.read\\
        .schema(df_schema)\\
        .option("skip_header",1)\\
        .option("field_delimiter", "|")\\
        .option("encoding", "UTF-8") \\
        .option("field_optionally_enclosed_by", "\\"") \\
        .option("null_if", "") \\
        .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        df=df.na.drop("all")
        if df.count()==0 :
            return "No Data in file"


        
        # Perform necessary transformations
        df = df.with_column("PROUCT_NAME", trim(upper(col("PROUCT_NAME")), lit('','')))\\
               .with_column("ACTIVE", trim(upper(col("ACTIVE"))))\\
               .with_column("GROUPMSL", trim(upper(col("GROUPMSL"))))\\
               .with_column("CURR_DATE", current_timestamp())\\
               .with_column("RUN_ID", lit(None).cast(DecimalType(14,0)))\\
               .with_column("FILE_NAME", lit(file_name))
               
        snowdf = df.select(
            "MSL_ID",
            "SUB_CHANNEL",
            "FROM_CYCLE",
            "TO_CYCLE",
            "PRODUCT_ID",
            "PROUCT_NAME",
            "ACTIVE",
            "CURR_DATE",
            "RUN_ID",
            "GROUPMSL",
            "FILE_NAME"
        )
        
        # Archive successful loads for auditability
        file_name=file_name.split(".")[0]+"_"+datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        snowdf.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)
        
        # Write operation to append data to the target table
        snowdf.write.mode("append").save_as_table(stage_name.split(".")[0]+"."+target_table)
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



CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.VNMSDL_RAW.VN_DMS_DISTRIBUTOR("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS 'from snowflake.snowpark.functions import col, lit, current_timestamp, trim, upper
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DecimalType,DateType
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz

def main(session:snowpark.Session,Param):

    #Param=["OUT_CON_DT_VN_20240405000736.csv","VNMSDL_RAW.DEV_LOAD_STAGE_ADLS","dev/dms/dms_source","SDL_VN_DMS_DISTRIBUTOR_DIM"]
    
    try:

        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]

        df_schema = StructType([
            StructField("TERRITORY_DIST", StringType()),
            StructField("MAPPED_SPK", StringType()),
            StructField("DSTRBTR_ID", StringType()),
            StructField("DSTRBTR_TYPE", StringType()),
            StructField("DSTRBTR_NAME", StringType()),
            StructField("DSTRBTR_ADDRESS", StringType()),
            StructField("LONGITUDE", StringType()),
            StructField("LATITUDE", StringType()),
            StructField("REGION", StringType()),
            StructField("PROVINCE", StringType()),
            StructField("ACTIVE", StringType()),
            StructField("ASM_ID", StringType())
        ])

        df = session.read\\
        .schema(df_schema)\\
        .option("skip_header",1)\\
        .option("field_delimiter", "|")\\
        .option("encoding", "UTF-8") \\
        .option("field_optionally_enclosed_by", "\\"") \\
        .option("null_if", "") \\
        .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        # Adding transformations for ACTIVE and ASM_ID columns
        df = df.withColumn("CURR_DATE",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))\\
               .with_column("RUN_ID", lit(lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S"))).cast(DecimalType(14,0)))\\
               .with_column("FILE_NAME", lit(file_name))

        snowdf = df.select(
            "TERRITORY_DIST",
            "MAPPED_SPK",
            "DSTRBTR_ID",
            "DSTRBTR_TYPE",
            "DSTRBTR_NAME",
            "DSTRBTR_ADDRESS",
            "LONGITUDE",
            "LATITUDE",
            "REGION",
            "PROVINCE",
            "ACTIVE",
            "ASM_ID",
            "CURR_DATE",
            "RUN_ID",
            "FILE_NAME"
        )

        # Check if DataFrame is empty
        if snowdf.count() == 0:
            return "No Data in table"

        # Write operation
        snowdf.write.mode("append").save_as_table(stage_name.split(".")[0]+"."+target_table)

        # Archive
        file_name1=("_").join(file_name.split("_")[0:5])+"_"+datetime.now().strftime("%Y%m%d%H%M%S")
        snowdf.write.copy_into_location("@"+Param[1]+"/"+Param[2]+"/success/"+file_name1,header=True,OVERWRITE=True)


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
        return error_message';



CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.VNMSDL_RAW.VN_DMS_SELLOUT_HEADER_PREPROCESSING("PARAM" ARRAY)
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
from snowflake.snowpark.functions import col,lit,trim, substring
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField
import pandas as pd
from datetime import datetime
import pytz

def main(session: snowpark.Session,Param): 

    try:

        #Param=[''OUT_CON_H_VN_20240322000800.csv'',''VNMSDL_RAW.DEV_LOAD_STAGE_ADLS'',''dev/dms/transaction/dms_source/sellout_header/'',''SDL_VN_DMS_H_SELLOUT_SALES_FACT'']

        # Extracting parameters from the input
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        sch_name        = stage_name.split(''.'')[0]
        target_table    = sch_name+"."+Param[3]


        # Define the schema for the DataFrame
        df_schema=StructType([
            StructField("DSTRBTR_ID",StringType()),
            StructField("CNTRY_CODE",StringType()),
            StructField("OUTLET_ID",StringType()),
            StructField("ORDER_DATE",StringType()),
            StructField("INVOICE_DATE",StringType()),
            StructField("ORDER_NO",StringType()),
            StructField("INVOICE_NO",StringType()),
            StructField("SELLOUT_AFVAT_BFDISC",StringType()),
            StructField("TOTAL_DISCOUNT",StringType()),
            StructField("INVOICE_DISCOUNT",StringType()),
            StructField("SELLOUT_AFVAT_AFDISC",StringType()),
            StructField("STATUS",StringType())
            ])


        # Read the CSV file into a DataFrame
    
        dataframe = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "|")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .option("skip_blank_lines", True)\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)


        #---------------------------Transformation logic ------------------------------#
        

        # Handle null values or empty rows
        dataframe=dataframe.na.drop("all")

        # Check for empty Dataframe
        if dataframe.count()==0:
            return "No Data in file"

        # Add curr_date, RUN_ID and source_file_name in Dataframe
        
        dataframe = dataframe.with_column("CURR_DATE",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        dataframe = dataframe.with_column("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")).cast("integer"))
        dataframe = dataframe.with_column("FILE_NAME", lit(file_name))

        # Creating Final Dataframe
        final_df = dataframe.alias("final_df")

        # Load Data to the target table
        
        final_df.write.mode("append").saveAsTable(target_table)

        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")

        # write to success folder
    
        file_name=file_name.split(".")[0]+''_''+datetime.now().strftime("%Y%m%d%H%M%S")
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


CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.VNMSDL_RAW.DATA_EXTRACT_SUMMARY_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS 'from snowflake.snowpark.functions import col, lit, current_timestamp
from snowflake.snowpark.types import StringType, StructType, StructField
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz

def main(session:snowpark.Session,Param):

    #Param=[''Data_Extract_Summary_VN_20240507003000.csv'',''VNMSDL_RAW.DEV_LOAD_STAGE_ADLS'',''dev/dms/dms_source/'',''SDL_VN_DMS_DATA_EXTRACT_SUMMARY'']

    try:
        file_name       = Param[0]  # File name
        stage_name      = Param[1]  # Stage name where files are located
        temp_stage_path = Param[2]  # Path in the stage
        target_table    = Param[3]  # Target table for the data

        # Define the schema based on the table structure
        df_schema = StructType([
            StructField("A", StringType()),
            StructField("B", StringType()),
            StructField("C", StringType())
        ])

        df = session.read\\
        .schema(df_schema)\\
        .option("skip_header",1)\\
        .option("field_delimiter", "|")\\
        .option("encoding", "UTF-8")\\
        .option("field_optionally_enclosed_by", "\\"")\\
        .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        
        # Check if DataFrame is empty
        df = df.na.drop("all")
        
        if df.count() == 0:
             return "No Data in file"


        # Archive successful loads for auditability
        file_name=file_name.split(".")[0]+''_''+datetime.now().strftime("%Y%m%d%H%M%S")
        
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        df = df.with_column("FILE_NAME", lit(Param[0].split(".")[0]))


         # Write operation to append data to the target table
        df.write.mode("append").save_as_table(stage_name.split(".")[0]+"."+target_table)
        
        #move to success
        df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)
        
        
        return "Success"

    except KeyError as key_error:
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message

    except pd.errors.MergeError as merge_error:
        error_message = f"DataFrame merging error: {str(merge_error)}"
        return error_message
    
    except Exception as e:
        error_message = f"Error: {str(e)}"
        return error_message';


CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.VNMSDL_RAW.SDL_VN_DMS_CUSTOMER_DIM("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS 'from snowflake.snowpark.functions import col, lit, current_timestamp, trim, upper,to_timestamp
from snowflake.snowpark.types import StringType, StructType, StructField, DecimalType
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark

def main(session:snowpark.Session,Param):
    
    try:

        #Param=[''OUT_CON_C_VN_20240415000502.csv'',''VNMSDL_RAW.DEV_LOAD_STAGE_ADLS'',''dev/dms/dms_source/'',''sdl_vn_dms_customer_dim'']

        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]

        df_schema = StructType([
            StructField("SAP_ID", StringType()),
            StructField("DSTRBTR_ID", StringType()),
            StructField("CNTRY_CODE", StringType()),
            StructField("OUTLET_ID", StringType()),
            StructField("OUTLET_NAME", StringType()),
            StructField("ADDRESS_1", StringType()),
            StructField("ADDRESS_2", StringType()),
            StructField("SHOP_TYPE", StringType()),
            StructField("TELEPHONE", StringType()),
            StructField("FAX", StringType()),
            StructField("CITY", StringType()),
            StructField("POSTCODE", StringType()),
            StructField("REGION", StringType()),
            StructField("CHANNEL_GROUP", StringType()),
            StructField("SUB_CHANNEL", StringType()),
            StructField("SALES_ROUTE_ID", StringType()),
            StructField("SALES_ROUTE_NAME", StringType()),
            StructField("SALES_GROUP", StringType()),
            StructField("SALESREP_ID", StringType()),
            StructField("SALESREP_NAME", StringType()),
            StructField("GPS_LAT", StringType()),
            StructField("GPS_LONG", StringType()),
            StructField("STATUS", StringType()),
            StructField("DISTRICT", StringType()),
            StructField("PROVINCE", StringType()),
            StructField("CRT_DATE", StringType()),
            StructField("DATE_OFF", StringType())
        ])

        df = session.read\\
        .schema(df_schema)\\
        .option("skip_header",1)\\
        .option("field_delimiter", "|")\\
        .option("encoding", "UTF-8") \\
        .option("field_optionally_enclosed_by", "\\"") \\
        .option("null_if", "") \\
        .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        df = df.na.drop("all")
        
        if  df.count() == 0:
            return "No Data in file"

        # Adding transformations for specific columns if needed
        df=  df.with_column("CURR_DATE", lit(to_timestamp(current_timestamp())))\\
               .with_column("RUN_ID", lit(None).cast(DecimalType(14,0)))\\
               .with_column("FILE_NAME", lit(file_name))

        snowdf = df.select(
            	"SAP_ID",
            	"DSTRBTR_ID",
            	"CNTRY_CODE",
            	"OUTLET_ID",
            	"OUTLET_NAME",
            	"ADDRESS_1",
            	"ADDRESS_2",
            	"TELEPHONE",
            	"FAX",
            	"CITY",
            	"POSTCODE",
            	"REGION",
            	"CHANNEL_GROUP",
            	"SUB_CHANNEL",
            	"SALES_ROUTE_ID",
            	"SALES_ROUTE_NAME",
            	"SALES_GROUP",
            	"SALESREP_ID",
            	"SALESREP_NAME",
            	"GPS_LAT",
            	"GPS_LONG",
            	"STATUS",
            	"DISTRICT",
            	"PROVINCE",
            	"CRT_DATE",
            	"DATE_OFF",
            	"CURR_DATE",
            	"RUN_ID",
            	"SHOP_TYPE",
                "FILE_NAME"
        )

       
        file_name1 = ("_").join(file_name.split("_")[0:5]) + "_" + datetime.now().strftime("%Y%m%d%H%M%S")
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")


        # Write operation
        snowdf.write.mode("append").save_as_table(stage_name.split(".")[0]+"."+target_table)

        #move to success
        snowdf.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name1,header=True,OVERWRITE=True)

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
        return error_message';


CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.VNMSDL_RAW.SDL_VN_DMS_SELLTHRGH_SALES_FACT("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '
from snowflake.snowpark.functions import col, lit, current_timestamp, trim, upper
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DecimalType,DateType
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
def main(session:snowpark.Session, Param):
    try:
        file_name       = Param[0]  # File name
        stage_name      = Param[1]  # Stage name where files are located
        temp_stage_path = Param[2]  # Path in the stage
        target_table    = Param[3]  # Target table for the data
        df_schema = StructType([
            StructField("DSTRBTR_ID", StringType()),
            StructField("DSTRBTR_TYPE", StringType()),
            StructField("MAPPED_SPK", StringType()),
            StructField("DOC_NUMBER", StringType()),
            StructField("REF_NUMBER", StringType()),
            StructField("RECEIPT_DATE", StringType()),
            StructField("ORDER_TYPE", StringType()),
            StructField("VAT_INVOICE_NUMBER", StringType()),
            StructField("VAT_INVOICE_NOTE", StringType()),
            StructField("VAT_INVOICE_DATE", StringType()),
            StructField("PON_NUMBER", StringType()),
            StructField("LINE_REF", StringType()),
            StructField("PRODUCT_CODE", StringType()),
            StructField("UNIT", StringType()),
            StructField("QUANTITY", StringType()),
            StructField("PRICE", StringType()),
            StructField("AMOUNT", StringType()),
            StructField("TAX_AMOUNT", StringType()),
            StructField("TAX_ID", StringType()),
            StructField("TAX_RATE", StringType()),
            StructField("VALUES", StringType()),
            StructField("LINE_DISCOUNT", StringType()),
            StructField("DOC_DISCOUNT", StringType()),
            StructField("STATUS", StringType())
        ])
        df = session.read\\
        .schema(df_schema)\\
        .option("skip_header",1)\\
        .option("field_delimiter", "|")\\
        .option("encoding", "UTF-8") \\
        .option("field_optionally_enclosed_by", "\\"") \\
        .option("null_if", "") \\
        .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
        # Perform necessary transformations
        df = df.with_column("CURR_DATE", current_timestamp())\\
               .with_column("RUN_ID", lit(None).cast(DecimalType(14,0)))\\
               .with_column("FILE_NAME", lit(file_name))
        snowdf = df.select(
            "DSTRBTR_ID",
            "DSTRBTR_TYPE",
            "MAPPED_SPK",
            "DOC_NUMBER",
            "REF_NUMBER",
            "RECEIPT_DATE",
            "ORDER_TYPE",
            "VAT_INVOICE_NUMBER",
            "VAT_INVOICE_NOTE",
            "VAT_INVOICE_DATE",
            "PON_NUMBER",
            "LINE_REF",
            "PRODUCT_CODE",
            "UNIT",
            "QUANTITY",
            "PRICE",
            "AMOUNT",
            "TAX_AMOUNT",
            "TAX_ID",
            "TAX_RATE",
            "VALUES",
            "LINE_DISCOUNT",
            "DOC_DISCOUNT",
            "STATUS",
            "CURR_DATE",
            "RUN_ID",
            "FILE_NAME"
        )
        # Check if DataFrame is empty
        df = df.na.drop("all")
        
        if df.count() == 0:
            return "No Data in file"
        # Archive successful loads for auditability
        file_name1 = ("_").join(file_name.split("_")[0:5]) + "_" + datetime.now().strftime("%Y%m%d%H%M%S")
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        
        #move to success
        snowdf.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name1,header=True,OVERWRITE=True)
        # Write operation to append data to the target table
        snowdf.write.mode("append").save_as_table(stage_name.split(".")[0]+"."+target_table)
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


CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.VNMSDL_RAW.SDL_VN_DMS_SALES_STOCK_FACT("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '
from snowflake.snowpark.functions import col, lit, current_timestamp, trim, upper
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DecimalType,DateType
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
def main(session:snowpark.Session, Param):
    try:
        file_name       = Param[0]  # File name
        stage_name      = Param[1]  # Stage name where files are located
        temp_stage_path = Param[2]  # Path in the stage
        target_table    = Param[3]  # Target table for the data
        df_schema = StructType([
            StructField("DSTRBTR_ID", StringType()),
            StructField("CNTRY_CODE", StringType()),
            StructField("WH_CODE", StringType()),
            StructField("DATE", StringType()),
            StructField("MATERIAL_CODE", StringType()),
            StructField("BAT_NUMBER", StringType()),
            StructField("EXPIRY_DATE", StringType()),
            StructField("QUANTITY", StringType()),
            StructField("UOM", StringType()),
            StructField("AMOUNT", StringType())
        ])
        df = session.read\\
        .schema(df_schema)\\
        .option("skip_header",1)\\
        .option("field_delimiter", "|")\\
        .option("encoding", "UTF-8") \\
        .option("field_optionally_enclosed_by", "\\"") \\
        .option("null_if", "") \\
        .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
        # Perform necessary transformations
        df = df.with_column("CURR_DATE", current_timestamp())\\
               .with_column("RUN_ID", lit(None).cast(DecimalType(14,0)))\\
               .with_column("SOURCE_FILE_NAME", lit(file_name))\\
               .with_column("FILE_NAME", lit(file_name))
        snowdf = df.select(
            "DSTRBTR_ID",
            "CNTRY_CODE",
            "WH_CODE",
            "DATE",
            "MATERIAL_CODE",
            "BAT_NUMBER",
            "EXPIRY_DATE",
            "QUANTITY",
            "UOM",
            "AMOUNT",
            "CURR_DATE",
            "RUN_ID",
            "FILE_NAME"
        )
        # Check if DataFrame is empty
        df = df.na.drop("all")
        
        if df.count() == 0:
            return "No Data in file"
        # Archive successful loads for auditability
        file_name1 = ("_").join(file_name.split("_")[0:5]) + "_" + datetime.now().strftime("%Y%m%d%H%M%S")
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        
        #move to success
        snowdf.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name1,header=True,OVERWRITE=True)
        # Write operation to append data to the target table
        snowdf.write.mode("append").save_as_table(stage_name.split(".")[0]+"."+target_table)
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


CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.VNMSDL_RAW.SDL_VN_DMS_KPI("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '
from snowflake.snowpark.functions import col, lit, current_timestamp, trim, upper
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DecimalType,DateType
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
def main(session:snowpark.Session, Param):
    try:
        file_name       = Param[0]  # File name
        stage_name      = Param[1]  # Stage name where files are located
        temp_stage_path = Param[2]  # Path in the stage
        target_table    = Param[3]  # Target table for the data
        df_schema = StructType([
            StructField("DSTRBTR_ID", StringType()),
            StructField("SALEMAN_CODE", StringType()),
            StructField("SALEMAN_NAME", StringType()),
            StructField("CYCLE", StringType()),
            StructField("EXPORT_DATE", StringType()),
            StructField("KPI_TYPE", StringType()),
            StructField("TARGET_VALUE", StringType()),
            StructField("ACTUAL_VALUE", StringType())
        ])
        df = session.read\\
        .schema(df_schema)\\
        .option("skip_header",1)\\
        .option("field_delimiter", "|")\\
        .option("encoding", "UTF-8") \\
        .option("field_optionally_enclosed_by", "\\"") \\
        .option("null_if", "") \\
        .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
        # Perform necessary transformations, if any
        # Include conversion to correct data types if needed, here assuming all stays as string
        # Adding a timestamp and file name for tracking
        df = df.with_column("CURR_DATE", lit(datetime.now().strftime("%Y%m%d%H%M%S")))\\
               .with_column("RUN_ID", lit(None).cast(DecimalType(14,0)))\\
               .with_column("SOURCE_FILE_NAME", lit(file_name))\\
               .with_column("FILE_NAME", lit(file_name))
        snowdf = df.select(
            "DSTRBTR_ID",
            "SALEMAN_CODE",
            "SALEMAN_NAME",
            "CYCLE",
            "EXPORT_DATE",
            "KPI_TYPE",
            "TARGET_VALUE",
            "ACTUAL_VALUE",
            "CURR_DATE",
            "RUN_ID",
            "FILE_NAME"
        )
        # Check if DataFrame is empty
        df = df.na.drop("all")
        
        if df.count() == 0:
            return "No Data in file"
        # Archive successful loads for auditability
        file_name1 = ("_").join(file_name.split("_")[0:5]) + "_" + datetime.now().strftime("%Y%m%d%H%M%S")
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        
        #move to success
        snowdf.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name1,header=True,OVERWRITE=True)
        # Write operation to append data to the target table
        snowdf.write.mode("append").save_as_table(stage_name.split(".")[0]+"."+target_table)
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


CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.VNMSDL_RAW.VN_DMS_FORECAST_PREPROCESSING("PARAM" ARRAY)
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
    #Param=["OUT_CON_Forecast_VN_20191118.csv","VNMSDL_RAW.DEV_LOAD_STAGE_ADLS","dev/dms/transaction/manual_file","sdl_vn_dms_forecast"]
    try:
        file_name = Param[0]
        stage_name = Param[1]
        temp_stage_path = Param[2]
        target_table = Param[3]

        df_schema = StructType([
            StructField("cycle", StringType(), True),
            StructField("channel", StringType(), True),
            StructField("territory_dist", StringType(), True),
            StructField("warehouse", StringType(), True),
            StructField("franchise", StringType(), True),
            StructField("brand", StringType(), True),
            StructField("variant", StringType(), True),
            StructField("forecastso_mil", StringType(), True),
        ])

        df = session.read \\
            .schema(df_schema) \\
            .option("skip_header", 1) \\
            .option("field_delimiter", "|") \\
            .option("field_optionally_enclosed_by", "\\"") \\
            .csv("@" + stage_name + "/" + temp_stage_path + "/" + file_name)

        df = df.na.drop("all")

        if df.count() == 0:
            return "No Data in file"

        df = df.withColumn("run_id", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        df = df.withColumn("FILE_NAME", lit(file_name))
        df = df.withColumn("crtd_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))

        snowdf = df.select(
            "cycle",
            "channel",
            "territory_dist",
            "warehouse",
            "franchise",
            "brand",
            "variant",
            "forecastso_mil",
            "crtd_dttm",
            "run_id",
            "file_name"
            
        )

        file_name = file_name.split(".")[0] + "_" + datetime.now(pytz.timezone("Asia/Singapore")).strftime(
            "%Y%m%d%H%M%S")

        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")

        # move to success
        snowdf.write.copy_into_location(
            "@" + stage_name + "/" + temp_stage_path + "/processed/success/" + formatted_year + "/" + formatted_month + "/" + file_name,
            header=True, OVERWRITE=True)

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

