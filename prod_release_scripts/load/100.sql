USE SCHEMA THASDL_RAW;

CREATE OR REPLACE PROCEDURE PERFECT_STORE_CONSUMERREACH_CVS_711_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '# This Preprocessing Code can be used for both Perfect store- 
# consumerreach_cvs and consumerreach_711

import snowflake.snowpark as snowpark
from snowflake import snowpark
from snowflake.snowpark.functions import col,lit
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField,DecimalType,DateType
import pandas as pd
from datetime import datetime
import pytz

def main(session: snowpark.Session,Param):

    
    try:

        # Parameters for consumerreach_cvs
        #Param=[''jnj_consumerreach_cvs_202401.csv'',''THASDL_RAW.DEV_LOAD_STAGE_ADLS'',''dev/perfect-store/transaction/'',''SDL_JNJ_CONSUMERREACH_CVS'']

        # Parameters for consumerreach_711
        #Param=[''jnj_consumerreach_711_202401.csv'',''THASDL_RAW.DEV_LOAD_STAGE_ADLS'',''dev/perfect-store/transaction/'',''SDL_JNJ_CONSUMERREACH_711'']
        
        # Extracting parameters from the input
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]

        # Define the schema for the DataFrame
        df_schema=StructType([
            StructField("ID",StringType()),
            StructField("CDATE",StringType()),
            StructField("RETAIL",StringType()),
            StructField("RETAILNAME",StringType()),
            StructField("RETAILBRANCH",StringType()),
            StructField("RETAILPROVINCE",StringType()),
            StructField("JJSKUBARCODE",StringType()),
            StructField("JJSKUNAME",StringType()),
            StructField("JJCORE",StringType()),
            StructField("DISTRIBUTION",StringType()),
            StructField("STATUS",StringType())
            ])

        
        # Read the CSV file into a DataFrame
    
        dataframe = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "\\u0001")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)


        #---------------------------Transformation logic ------------------------------#

        # Handle null values or empty rows
        dataframe=dataframe.na.drop("all")

        # Check if the Dataframe is having Data

        if dataframe.count()==0:
            return "No Data in file"

        # Add RUN_ID, FILE NAME and YEARMO columns

        dataframe = dataframe.with_column("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        dataframe = dataframe.with_column("FILE_NAME",lit(file_name))

        # Extract year month from the file
        yearmo= file_name.split("_")[-1].split(".")[0]

        dataframe = dataframe.with_column("YEARMO",lit(yearmo))


        # Creating Final Dataframe
        final_df = dataframe.alias("final_df")


        # Delete existing Data for the current file
        
        del_sql = "DELETE FROM " + target_table + " WHERE FILE_NAME ="+"''" + (file_name)+"''"
        session.sql(del_sql).collect()
        
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
CREATE OR REPLACE PROCEDURE PERFECT_STORE_COP_PREPROCESSING("PARAM" ARRAY)
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
from snowflake.snowpark.functions import col,lit,concat
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField,DecimalType,DateType
import pandas as pd
from datetime import datetime
import pytz

def main(session: snowpark.Session,Param):

    try:

        #Param=[''JNJ_Mer_-_COP_1_202401.csv'',''THASDL_RAW.DEV_LOAD_STAGE_ADLS'',''dev/perfect-store/transaction/'',''SDL_JNJ_MER_COP'']

        # Extracting parameters from the input
        file_name       = Param[0]
        escaped_file_name = file_name.replace("(", "").replace(")", "").replace(" ",''_'')
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]

        # Define the schema for the DataFrame
        df_schema=StructType([
            StructField("COP_DATE",StringType()),
            StructField("EMP_ADDRESS_PC",StringType()),
            StructField("PC_NAME",StringType()),
            StructField("SURVEY_NAME",StringType()),
            StructField("EMP_ADDRESS_SUPERVISOR",StringType()),
            StructField("SUPERVISOR_NAME",StringType()),
            StructField("ACTIVITY",StringType()),
            StructField("CATEGORY",StringType()),
            StructField("BRAND",StringType()),
            StructField("START_DATE",StringType()),
            StructField("END_DATE",StringType()),
            StructField("AREA",StringType()),
            StructField("CHANNEL",StringType()),
            StructField("ACCOUNT",StringType()),
            StructField("STORE_ID",StringType()),
            StructField("STORE_NAME",StringType()),
            StructField("COMPLIANCE",StringType()),
            StructField("QUESTION",StringType()),
            StructField("ANSWER",StringType())
            ])


        # Read the CSV file into a DataFrame
        # escaped_file_name = file_name.replace("(", "").replace(")", "")
        # print(escaped_file_name)
        
        dataframe = session.read\\
            .schema(df_schema)\\
            .option("skip_header",2)\\
            .option("field_delimiter", "\\u0001")\\
            .option("encoding", "UTF-8")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+escaped_file_name)
         

        #---------------------------Transformation logic ------------------------------#

        # Handle null values or empty rows
        dataframe=dataframe.na.drop("all")
        
        # Check if the Dataframe is having Data

        if dataframe.count()==0:
            return "No Data in file"

        # Add COP_PRIORITY,RUN_ID, FILE NAME and YEARMO columns
        
        dataframe = dataframe.withColumn("COP_PRIORITY", lit(dataframe["ACTIVITY"]))
        dataframe = dataframe.with_column("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))

        # Extract year month from the file
        yearmo= file_name.split("_")[-1].split(".")[0]

        dataframe = dataframe.with_column("YEARMO",lit(yearmo))
        
        # Preparing the File_name column
        new_file_name="(JNJ Mer) - COP_1_"+yearmo+".csv"
        dataframe = dataframe.with_column("FILE_NAME",lit(new_file_name))


        # Creating Final Dataframe
        
        final_df = dataframe.select("COP_DATE","EMP_ADDRESS_PC","PC_NAME",
                                    "SURVEY_NAME","EMP_ADDRESS_SUPERVISOR","SUPERVISOR_NAME","COP_PRIORITY","START_DATE",
                                    "END_DATE","AREA","CHANNEL","ACCOUNT","STORE_ID","STORE_NAME","QUESTION","ANSWER",
                                    "RUN_ID","FILE_NAME","YEARMO","ACTIVITY","CATEGORY","BRAND","COMPLIANCE")
         
        # Delete existing Data for the current file
        
        del_sql = "DELETE FROM " + target_table + " WHERE FILE_NAME ="+"''" + (new_file_name)+"''"
        session.sql(del_sql).collect()
        
        # Load Data to the target table
        final_df.write.mode("append").saveAsTable(target_table)

        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")

        # write to success folder
    
        file_name=new_file_name.split(".")[0]+''_''+datetime.now().strftime("%Y%m%d%H%M%S")
        
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
CREATE OR REPLACE PROCEDURE PERFECT_STORE_OSA_OSS_REPORT_PREPROCESSING("PARAM" ARRAY)
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
from snowflake.snowpark.functions import col,lit,concat
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField,DecimalType,DateType
import pandas as pd
from datetime import datetime
import pytz

def main(session: snowpark.Session,Param):

    try:

        #Param=[''JNJ_OSA_and_OOS_Report_202401.csv'',''THASDL_RAW.DEV_LOAD_STAGE_ADLS'',''dev/perfect-store/transaction/'',''SDL_JNJ_OSA_OOS_REPORT'']

        # Extracting parameters from the input
        file_name       = Param[0]
        escaped_file_name = file_name.replace("(", "").replace(")", "").replace(" ",''_'')
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]

        # Define the schema for the DataFrame
        df_schema=StructType([
            StructField("OSA_OOS_DATE",StringType()),
            StructField("WEEK",StringType()),
            StructField("EMP_ADDRESS_PC",StringType()),
            StructField("PC_NAME",StringType()),
            StructField("EMP_ADDRESS_SUPERVISOR",StringType()),
            StructField("SUPERVISOR_NAME",StringType()),
            StructField("AREA",StringType()),
            StructField("CHANNEL",StringType()),
            StructField("ACCOUNT",StringType()),
            StructField("STORE_ID",StringType()),
            StructField("STORE_NAME",StringType()),
            StructField("SHOP_TYPE",StringType()),
            StructField("BRAND",StringType()),
            StructField("CATEGORY",StringType()),
            StructField("BARCODE",StringType()),
            StructField("SKU",StringType()),
            StructField("MSL_PRICE_TAG",StringType()),
            StructField("OOS",StringType()),
            StructField("OOS_REASON",StringType())
            ])

        # Read the CSV file into a DataFrame
        
        dataframe = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "\\u0001")\\
            .option("encoding", "UTF-8")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+escaped_file_name)
        

        #---------------------------Transformation logic ------------------------------#

        # Handle null values or empty rows
        dataframe=dataframe.na.drop("all")
        
        # Check if the Dataframe is having Data

        if dataframe.count()==0:
            return "No Data in file"

        # Add RUN_ID, FILE NAME and YEARMO columns
        
        dataframe = dataframe.with_column("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))

        # Extract year month from the file
        yearmo= file_name.split("_")[-1].split(".")[0]

        # Preparing the File_name column
        new_file_name="(JNJ) OSA and OOS Report_"+yearmo+".csv"
        dataframe = dataframe.with_column("FILE_NAME",lit(new_file_name))

        dataframe = dataframe.with_column("YEARMO",lit(yearmo))


        # Creating Final Dataframe
        final_df = dataframe.alias("final_df")

        # Delete existing Data for the current file
        
        del_sql = "DELETE FROM " + target_table + " WHERE FILE_NAME ="+"''" + (new_file_name)+"''"
        session.sql(del_sql).collect()
        
        # Load Data to the target table
        final_df.write.mode("append").saveAsTable(target_table)

        
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")

        # write to success folder
    
        file_name=new_file_name.split(".")[0]+''_''+datetime.now().strftime("%Y%m%d%H%M%S")
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
CREATE OR REPLACE PROCEDURE PERFECT_STORE_SHARE_OF_SHELF_PREPROCESSING("PARAM" ARRAY)
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
from snowflake.snowpark.functions import col,lit,concat
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField,DecimalType,DateType
import pandas as pd
from datetime import datetime
import pytz

def main(session: snowpark.Session,Param):

    try:

        #Param=[''(JNJ Mer) - Share of Shelf_202401.csv'',''THASDL_RAW.DEV_LOAD_STAGE_ADLS'',''dev/perfect-store/transaction/'',''SDL_JNJ_MER_SHARE_OF_SHELF'']

        # Extracting parameters from the input
        file_name       = Param[0]
        escaped_file_name = file_name.replace("(", "").replace(")", "").replace(" ",''_'')
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]

        # Define the schema for the DataFrame
        df_schema=StructType([
            StructField("SOS_DATE",StringType()),
            StructField("MERCHANDISER_NAME",StringType()),
            StructField("SUPERVISOR_NAME",StringType()),
            StructField("AREA",StringType()),
            StructField("CHANNEL",StringType()),
            StructField("ACCOUNT",StringType()),
            StructField("STORE_ID",StringType()),
            StructField("STORE_NAME",StringType()),
            StructField("CATEGORY",StringType()),
            StructField("AGENCY",StringType()),
            StructField("BRAND",StringType()),
            StructField("SIZE",StringType())
            ])

        # Read the CSV file into a DataFrame
        
        dataframe = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "\\u0001")\\
            .option("encoding", "UTF-8")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+escaped_file_name)
        

        #---------------------------Transformation logic ------------------------------#

        # Handle null values or empty rows
        dataframe=dataframe.na.drop("all")
        
        # Check if the Dataframe is having Data

        if dataframe.count()==0:
            return "No Data in file"

        # Add RUN_ID, FILE NAME and YEARMO columns
        
        dataframe = dataframe.with_column("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))

        # Extract year month from the file
        yearmo= file_name.split("_")[-1].split(".")[0]

        # Preparing the File_name column
        new_file_name="(JNJ Mer) - Share of Shelf_"+yearmo+".csv"
        dataframe = dataframe.with_column("FILE_NAME",lit(new_file_name))

        dataframe = dataframe.with_column("YEARMO",lit(yearmo))


        # Creating Final Dataframe
        final_df = dataframe.alias("final_df")


        # Delete existing Data for the current file
        
        del_sql = "DELETE FROM " + target_table + " WHERE FILE_NAME ="+"''" + (new_file_name)+"''"
        session.sql(del_sql).collect()
        
        # Load Data to the target table
        final_df.write.mode("append").saveAsTable(target_table)
        
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")

        # write to success folder
    
        file_name=new_file_name.split(".")[0]+''_''+datetime.now().strftime("%Y%m%d%H%M%S")
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
