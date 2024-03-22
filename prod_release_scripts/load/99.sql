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
CREATE OR REPLACE PROCEDURE SDL_TH_MT_BIGC("PARAM" ARRAY)
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
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        df_schema = StructType([
            StructField("REPORT_CODE", StringType()),
            StructField("SUPPLIER", StringType()),
            StructField("BUSINESS_FORMAT", StringType()),
            StructField("COMPARE", StringType()),
            StructField("STORE", StringType()),
            StructField("TRANSACTION_DATE", StringType()),
            StructField("LY_COMPARE_DATE", StringType()),
            StructField("REPORT_DATE", StringType()),
            StructField("DIVISION", StringType()),
            StructField("DEPARTMENT", StringType()),
            StructField("SUBDEPARTMENT", StringType()),
            StructField("CLASS", StringType()),
            StructField("SUBCLASS", StringType()),
            StructField("BARCODE", StringType()),
            StructField("ARTICLE", StringType()),
            StructField("ARTICLE_NAME", StringType()),
            StructField("BRAND", StringType()),
            StructField("MODEL", StringType()),
            StructField("SALE_AMT_TY_BAHT", StringType()),
            StructField("SALE_AMT_LY_BAHT", StringType()),
            StructField("SALE_AMT_VAR", StringType()),
            StructField("SALE_QTY_TY", StringType()),
            StructField("SALE_QTY_LY", StringType()),
            StructField("SALE_QTY_VAR", StringType()),
            StructField("STOCK_TY_BAHT", StringType()),
            StructField("STOCK_LY_BAHT", StringType()),
            StructField("STOCK_VAR", StringType()),
            StructField("STOCK_QTY_TY", StringType()),
            StructField("STOCK_QTY_LY", StringType()),
            StructField("STOCK_QTY_VAR", StringType()),
            StructField("DAY_ON_HAND_TY", StringType()),
            StructField("DAY_ON_HAND_LY", StringType()),
            StructField("DAY_ON_HAND_DIFF", StringType())
        ])
        
        df = session.read\\
        .schema(df_schema)\\
        .option("skip_header",1)\\
        .option("field_delimiter", ",")\\
        .option("encoding", "UTF-8") \\
        .option("REPLACE_INVALID_CHARACTERS", True) \\
        .option("field_optionally_enclosed_by", "\\"") \\
        .option("null_if", "") \\
        .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        df = df.with_column("CRT_DTTM", lit(datetime.now().strftime("%Y%m%d%H%M%S"))) \\
               .with_column("FILE_NAME", lit(file_name))

        
        snowdf = df.select(
            "REPORT_CODE",
            "SUPPLIER",
            "BUSINESS_FORMAT",
            "COMPARE",
            "STORE",
            "TRANSACTION_DATE",
            "LY_COMPARE_DATE",
            "REPORT_DATE",
            "DIVISION",
            "DEPARTMENT",
            "SUBDEPARTMENT",
            "CLASS",
            "SUBCLASS",
            "BARCODE",
            "ARTICLE",
            "ARTICLE_NAME",
            "BRAND",
            "MODEL",
            "SALE_AMT_TY_BAHT",
            "SALE_AMT_LY_BAHT",
            "SALE_AMT_VAR",
            "SALE_QTY_TY",
            "SALE_QTY_LY",
            "SALE_QTY_VAR",
            "STOCK_TY_BAHT",
            "STOCK_LY_BAHT",
            "STOCK_VAR",
            "STOCK_QTY_TY",
            "STOCK_QTY_LY",
            "STOCK_QTY_VAR",
            "DAY_ON_HAND_TY",
            "DAY_ON_HAND_LY",
            "DAY_ON_HAND_DIFF",
            "FILE_NAME",
            "CRT_DTTM"
        )
        
        # Check if DataFrame is empty
		snowdf=snowdf.na.drop("all")
        if snowdf.count() == 0:
            return "No Data in table"
        
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")

        # write to success folder    
        # Archive
        file_name1=("_").join(file_name.split("_")[0:5])+"_"+datetime.now().strftime("%Y%m%d%H%M%S")
        snowdf.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name1,header=True,OVERWRITE=True)

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
CREATE OR REPLACE PROCEDURE SDL_TH_MT_MAKRO("PARAM" ARRAY)
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
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]

        df_schema = StructType([
            StructField("SUPPLIER_NUMBER", StringType()),
            StructField("LOCATION_NUMBER", StringType()),
            StructField("LOCATION_NAME", StringType()),
            StructField("CLASS_NUMBER", StringType()),
            StructField("SUBCLASS_NUMBER", StringType()),
            StructField("ITEM_NUMBER", StringType()),
            StructField("BARCODE", StringType()),
            StructField("ITEM_DESC", StringType()),
            StructField("EOH_QTY", StringType()),
            StructField("ORDER_IN_TRANSIT_QTY", StringType()),
            StructField("PACK_TYPE", StringType()),
            StructField("MAKRO_UNIT", StringType()),
            StructField("AVG_NET_SALES_QTY", StringType()),
            StructField("NET_SALES_QTY_YTD", StringType()),
            StructField("LAST_RECV_DT", StringType()),
            StructField("LAST_SOLD_DT", StringType()),
            StructField("STOCK_COVER_DAYS", StringType()),
            StructField("NET_SALES_QTY_MTD", StringType()),
            StructField("DAY_1", StringType()),
            StructField("DAY_2", StringType()),
            StructField("DAY_3", StringType()),
            StructField("DAY_4", StringType()),
            StructField("DAY_5", StringType()),
            StructField("DAY_6", StringType()),
            StructField("DAY_7", StringType()),
            StructField("DAY_8", StringType()),
            StructField("DAY_9", StringType()),
            StructField("DAY_10", StringType()),
            StructField("DAY_11", StringType()),
            StructField("DAY_12", StringType()),
            StructField("DAY_13", StringType()),
            StructField("DAY_14", StringType()),
            StructField("DAY_15", StringType()),
            StructField("DAY_16", StringType()),
            StructField("DAY_17", StringType()),
            StructField("DAY_18", StringType()),
            StructField("DAY_19", StringType()),
            StructField("DAY_20", StringType()),
            StructField("DAY_21", StringType()),
            StructField("DAY_22", StringType()),
            StructField("DAY_23", StringType()),
            StructField("DAY_24", StringType()),
            StructField("DAY_25", StringType()),
            StructField("DAY_26", StringType()),
            StructField("DAY_27", StringType()),
            StructField("DAY_28", StringType()),
            StructField("DAY_29", StringType()),
            StructField("DAY_30", StringType()),
            StructField("DAY_31", StringType())
        ])


        df = session.read\\
        .schema(df_schema)\\
        .option("skip_header",1)\\
        .option("field_delimiter", ''\\x01'')\\
        .option("encoding", "UTF-8")\\
        .option("field_optionally_enclosed_by", "\\"")\\
        .option("null_if", "")\\
        .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        # Calculate FILE_NAME from file_name
        # Assuming "Makro_202108_20211117200459.xls" is the file format,
        # and we want to extract "202108" for the CRTD_DTM
        file_date_part = file_name[6:12]  # This will extract "202108"
        df = df.with_column("TRANSACTION_DATE", lit(datetime.strptime(file_date_part, "%Y%m"))) \\
               .with_column("FILE_NAME", lit(file_name)) \\
               .with_column("CRTD_DTM", lit(datetime.now().strftime("%Y%m%d%H%M%S")))


        snowdf = df.select(
            "TRANSACTION_DATE",
            "SUPPLIER_NUMBER",
            "LOCATION_NUMBER",
            "LOCATION_NAME",
            "CLASS_NUMBER",
            "SUBCLASS_NUMBER",
            "ITEM_NUMBER",
            "BARCODE",
            "ITEM_DESC",
            "EOH_QTY",
            "ORDER_IN_TRANSIT_QTY",
            "PACK_TYPE",
            "MAKRO_UNIT",
            "AVG_NET_SALES_QTY",
            "NET_SALES_QTY_YTD",
            "LAST_RECV_DT",
            "LAST_SOLD_DT",
            "STOCK_COVER_DAYS",
            "NET_SALES_QTY_MTD",
            "DAY_1",
            "DAY_2",
            "DAY_3",
            "DAY_4",
            "DAY_5",
            "DAY_6",
            "DAY_7",
            "DAY_8",
            "DAY_9",
            "DAY_10",
            "DAY_11",
            "DAY_12",
            "DAY_13",
            "DAY_14",
            "DAY_15",
            "DAY_16",
            "DAY_17",
            "DAY_18",
            "DAY_19",
            "DAY_20",
            "DAY_21",
            "DAY_22",
            "DAY_23",
            "DAY_24",
            "DAY_25",
            "DAY_26",
            "DAY_27",
            "DAY_28",
            "DAY_29",
            "DAY_30",
            "DAY_31",
            "FILE_NAME",
            "CRTD_DTM"
        )

        # Check if DataFrame is empty
		snowdf=snowdf.na.drop("all")
        if snowdf.count() == 0:
            return "No Data in table"

        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")

        
        # Archive
        file_name1=("_").join(file_name.split("_")[0:5])+"_"+datetime.now().strftime("%Y%m%d%H%M%S")
        snowdf.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name1,header=True,OVERWRITE=True)
        
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
CREATE OR REPLACE PROCEDURE SDL_TH_MT_WATSONS("PARAM" ARRAY)
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
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        df_schema = StructType([
            StructField("DIV", StringType()),
            StructField("DEPT", StringType()),
            StructField("CLASS", StringType()),
            StructField("SUBCLASS", StringType()),
            StructField("ITEM", StringType()),
            StructField("ITEM_DESC", StringType()),
            StructField("NON_SLOW", StringType()),
            StructField("NON_SLOW2", StringType()),
            StructField("FINANCE_STATUS", StringType()),
            StructField("CREATE_DATETIME", StringType()),
            StructField("PRIM_SUPPLIER", StringType()),
            StructField("OLD_SUPP_NO", StringType()),
            StructField("SUPP_DESC", StringType()),
            StructField("LEAD_TIME", StringType()),
            StructField("UNIT_COST", StringType()),
            StructField("UNIT_RETAIL_ZONE5", StringType()),
            StructField("ITEM_STATUS", StringType()),
            StructField("STATUS_WH", StringType()),
            StructField("STATUS_WH_UPDATE_DATE", StringType()),
            StructField("STATUS_STORE", StringType()),
            StructField("STATUS_STORE_UPDATE_DATE", StringType()),
            StructField("STATUS_XDOCK", StringType()),
            StructField("STATUS_XDOCK_UPDATE_DATE", StringType()),
            StructField("SOURCE_METHOD", StringType()),
            StructField("SOURCE_WH", StringType()),
            StructField("POG", StringType()),
            StructField("PRODUCT_TYPE", StringType()),
            StructField("LABEL_UDA", StringType()),
            StructField("BRAND", StringType()),
            StructField("ITEM_TYPE", StringType()),
            StructField("RETURN_POLICY", StringType()),
            StructField("RETURN_TYPE", StringType()),
            StructField("WH_WAC", StringType()),
            StructField("IN_TAX", StringType()),
            StructField("TAX_RATE", StringType()),
            StructField("STOCK_CAT", StringType()),
            StructField("ORDER_FLAG", StringType()),
            StructField("NEW_ITEM_13WEEK", StringType()),
            StructField("DEACTIVATE_DATE", StringType()),
            StructField("WH_ON_ORDER", StringType()),
            StructField("FIRST_RCV", StringType()),
            StructField("PROMO_MONTH", StringType()),
            StructField("SALES_TW", StringType()),
            StructField("NET_AMT", StringType()),
            StructField("NET_COST", StringType()),
            StructField("SALE_AVG_QTY_13WEEKS", StringType()),
            StructField("SALE_AVG_AMT_13WEEKS", StringType()),
            StructField("SALE_AVG_COST13WEEKS", StringType()),
            StructField("NET_QTY_YTD", StringType()),
            StructField("NET_AMT_YTD", StringType()),
            StructField("NET_COST_YTD", StringType()),
            StructField("TURN_WK", StringType()),
            StructField("WH_SOH", StringType()),
            StructField("STORE_TOTAL_STOCK", StringType()),
            StructField("TOTAL_STOCK_QTY", StringType()),
            StructField("WH_STOCK_AMT", StringType()),
            StructField("STORE_TOTAL_STOCK_AMT", StringType()),
            StructField("TOTAL_STOCK_XVAT", StringType()),
            StructField("PRO2", StringType()),
            StructField("DISC", StringType()),
            StructField("PRO22", StringType()),
            StructField("PRO2_PERT_DISC", StringType()),
            StructField("FIRST_DATE_SMS", StringType()),
            StructField("AGING_SMS", StringType()),
            StructField("GROUP_W", StringType()),
            StructField("WIN", StringType()),
            StructField("POG_2", StringType()),
            StructField("FILE_NAME", StringType()),
            StructField("DATE", StringType())
        ])
        df = session.read\\
        .schema(df_schema)\\
        .option("skip_header",1)\\
        .option("field_delimiter", ''\\x01'')\\
        .option("encoding", "UTF-8")\\
        .option("field_optionally_enclosed_by", "\\"")\\
        .option("null_if", "")\\
        .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
        # Adding transformations for specific columns if needed
        df = df.with_column("DATE", lit(datetime.now().strftime("%Y%m%d")))\\
               .with_column("FILE_NAME", lit(file_name))
        snowdf = df.select(
            "DIV",
            "DEPT",
            "CLASS",
            "SUBCLASS",
            "ITEM",
            "ITEM_DESC",
            "NON_SLOW",
            "NON_SLOW2",
            "FINANCE_STATUS",
            "CREATE_DATETIME",
            "PRIM_SUPPLIER",
            "OLD_SUPP_NO",
            "SUPP_DESC",
            "LEAD_TIME",
            "UNIT_COST",
            "UNIT_RETAIL_ZONE5",
            "ITEM_STATUS",
            "STATUS_WH",
            "STATUS_WH_UPDATE_DATE",
            "STATUS_STORE",
            "STATUS_STORE_UPDATE_DATE",
            "STATUS_XDOCK",
            "STATUS_XDOCK_UPDATE_DATE",
            "SOURCE_METHOD",
            "SOURCE_WH",
            "POG",
            "PRODUCT_TYPE",
            "LABEL_UDA",
            "BRAND",
            "ITEM_TYPE",
            "RETURN_POLICY",
            "RETURN_TYPE",
            "WH_WAC",
            "IN_TAX",
            "TAX_RATE",
            "STOCK_CAT",
            "ORDER_FLAG",
            "NEW_ITEM_13WEEK",
            "DEACTIVATE_DATE",
            "WH_ON_ORDER",
            "FIRST_RCV",
            "PROMO_MONTH",
            "SALES_TW",
            "NET_AMT",
            "NET_COST",
            "SALE_AVG_QTY_13WEEKS",
            "SALE_AVG_AMT_13WEEKS",
            "SALE_AVG_COST13WEEKS",
            "NET_QTY_YTD",
            "NET_AMT_YTD",
            "NET_COST_YTD",
            "TURN_WK",
            "WH_SOH",
            "STORE_TOTAL_STOCK",
            "TOTAL_STOCK_QTY",
            "WH_STOCK_AMT",
            "STORE_TOTAL_STOCK_AMT",
            "TOTAL_STOCK_XVAT",
            "PRO2",
            "DISC",
            "PRO22",
            "PRO2_PERT_DISC",
            "FIRST_DATE_SMS",
            "AGING_SMS",
            "GROUP_W",
            "WIN",
            "POG_2",
            "FILE_NAME",
            "DATE"
        )
        
        # Check if DataFrame is empty
		snowdf=snowdf.na.drop("all")
        if snowdf.count() == 0:
            return "No Data in table"
        
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
            
        # Archive
        file_name1=("_").join(file_name.split("_")[0:5])+"_"+datetime.now().strftime("%Y%m%d%H%M%S")
        snowdf.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name1,header=True,OVERWRITE=True)
        
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
CREATE OR REPLACE PROCEDURE SDL_TH_TESCO_TRANSDATA("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '

import snowflake.snowpark as snowpark
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DateType
from snowflake.snowpark.functions import col,lit,date_format,current_timestamp,to_timestamp, xmlget, flatten, get, when, substring, row_number
from snowflake.snowpark import Window
import pandas as pd
from datetime import datetime
import pytz

def get_xml_element(
        column:str,
        element:str,
        datatype:str,
        with_alias:bool = True
):
    new_element = (
        get(
            xmlget(
                col(column),
                lit(element),
            ),
            lit(''$'')
        )
        .cast("string")
    )

    new_element = when(new_element=='''', None).otherwise(new_element).cast(datatype)

    # alias needs to be optional
    return (
        new_element.alias(element) if with_alias else new_element
    )
    



def main(session:snowpark.Session, Param):

    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]

        df = session.read\\
        .option("STRIP_OUTER_ELEMENT", False) \\
        .xml("@" + stage_name + "/" + temp_stage_path + "/" + file_name) \\
        .select(
            xmlget(col("$1"), lit("invrptth")).alias("invrptth"),
            xmlget(xmlget(col("$1"), lit("invrptth")), lit("ir_header")).alias("ir_header"),
            xmlget(xmlget(col("$1"), lit("invrptth")), lit("ir_items")).alias("ir_items"),

            get_xml_element("ir_header", "creation_date", "string"),
            get_xml_element("ir_header", "supplier_ID", "string"),
            get_xml_element("ir_header", "supplier_name", "string"),
            get_xml_element("ir_header", "warehouse", "string"),
            get_xml_element("ir_header", "delivery_point_name", "string"),
            get_xml_element("ir_header", "IR_date", "string")
        ) \\
        .select(
            "creation_date",
            "supplier_ID",
            "supplier_name",
            "warehouse",
            "delivery_point_name",
            "IR_date",
            flatten(col("ir_items"), "$")
        ) \\
        .select (
            "creation_date",
            "supplier_ID",
            "supplier_name",
            "warehouse",
            "delivery_point_name",
            "IR_date",
            get_xml_element("value", "EANSKU", "string"),
            get_xml_element("value", "article_ID", "string"),
            get_xml_element("value", "SPN", "string"),
            get_xml_element("value", "article_name", "string"),
            get_xml_element("value", "stock", "string"),
            get_xml_element("value", "sales", "string"),
            get_xml_element("value", "sales_amount", "string"),
        )

        # Adding transformations for specific columns if needed
        df = df.with_column("FOLDER_NAME", lit(temp_stage_path))\\
               .with_column("FILE_NAME", lit(file_name))

        # Remove entries with null IR_DATE
        df_filtered = df.filter((col("IR_DATE").is_not_null()) & (col("IR_DATE").cast(StringType()) != ''''))

        # Continue from the window specification and ranking
        windowSpec = Window.partitionBy("IR_DATE", "WAREHOUSE", "SUPPLIER_ID").orderBy(substring(col("FILE_NAME"), 8, 26).desc())
        df_with_rank = df_filtered.withColumn("RANK", row_number().over(windowSpec))

        # Create a DataFrame of distinct file names where rank = 1
        df_rank_1_files = df_with_rank.filter(col("RANK") == 1).select("FILE_NAME").distinct()

        # Use a join operation to filter df_filtered based on the file names that have rank = 1
        df_filtered = df_filtered.join(df_rank_1_files, "FILE_NAME", "inner")

        snowdf = df_filtered.select(
            "CREATION_DATE",
            "SUPPLIER_ID",
            "SUPPLIER_NAME",
            "WAREHOUSE",
            "DELIVERY_POINT_NAME",
            "IR_DATE",
            "EANSKU",
            "ARTICLE_ID",
            "SPN",
            "ARTICLE_NAME",
            "STOCK",
            "SALES",
            "SALES_AMOUNT",
            "FILE_NAME",
            "FOLDER_NAME"
        )

        # Check if DataFrame is empty
		snowdf=snowdf.na.drop("all")
        if snowdf.count() == 0:
            return "No Data in table"

        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")

        # Archive
        file_name1=("_").join(file_name.split("_")[0:5])+"_"+datetime.now().strftime("%Y%m%d%H%M%S")
        snowdf.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name1,header=True,OVERWRITE=True)
        
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
