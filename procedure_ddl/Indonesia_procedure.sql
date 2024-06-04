CREATE or replace FILE FORMAT PROD_DNA_LOAD.IDNSDL_RAW.FILE_FORMAT_COMMA
FIELD_OPTIONALLY_ENCLOSED_BY = '\"'
FIELD_DELIMITER = ',';

CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.IDNSDL_RAW.ID_SERVER_DATA_CUSTOMERMASTER_PREPROCESSING("PARAM" ARRAY)
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
from snowflake.snowpark.types import StringType, StructType, StructField
import pandas as pd
from datetime import datetime
import pytz
 
def main(session: snowpark.Session,Param):
 
    try:
 
        #Param=[''Customer_1.csv'',''IDNSDL_RAW.DEV_LOAD_STAGE_ADLS_IDN'',''dev/alteryx/transaction/customermaster'',''SDL_DISTRIBUTOR_CUSTOMER_DIM'']
 
        # Extracting parameters from the input
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
 
 
        # Define the schema for the DataFrame
        df_schema=StructType([
            
            StructField("KEY_OUTLET", StringType()),
            StructField("JJ_SAP_DSTRBTR_ID", StringType()),
            StructField("JJ_SAP_DSTRBTR_NM", StringType()),
            StructField("CUST_ID", StringType()),
            StructField("CUST_NM", StringType()),
            StructField("ADDRESS", StringType()),
            StructField("CITY", StringType()),
            StructField("CUST_GRP", StringType()),
            StructField("CHNL", StringType()),
            StructField("OUTLET_TYPE", StringType()),
            StructField("CHNL_GRP", StringType()),
            StructField("JJID", StringType()),
            StructField("PST_CD", StringType()),
            StructField("CUST_ID_MAP", StringType()),
            StructField("CUST_NM_MAP", StringType()),
            StructField("CHNL_GRP2", StringType()),
            StructField("CUST_CRTD_DT", StringType()),
            StructField("CUST_GRP2", StringType()),
            StructField("CRTD_DTTM", StringType()),
            StructField("UPDT_DTTM", StringType())
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


        
        #Creating copy of the Dataframe
        final_df = final_df = dataframe.select("KEY_OUTLET","JJ_SAP_DSTRBTR_ID","JJ_SAP_DSTRBTR_NM","CUST_ID","CUST_NM","ADDRESS","CITY","CUST_GRP","CHNL","OUTLET_TYPE","CHNL_GRP","JJID","PST_CD","CUST_ID_MAP","CUST_NM_MAP",\\
        "CHNL_GRP2","CUST_CRTD_DT","CUST_GRP2","CRTD_DTTM","UPDT_DTTM"   )

        #return final_df
        
       # Load Data to the target table
        final_df.write.mode("append").saveAsTable(target_table)
 
         
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
 
        # write to success folder
        file_name=file_name.split(".")[0]
        final_df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True,file_format = "idnsdl_raw.CSV_FORMAT_PIPE")
        return "Success"
        #return final_df
 
    
 
    except KeyError as key_error:
        # Handle KeyError (missing columns) here
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message
    except Exception as e:
        # Handle exceptions here
        error_message = f"Error: {str(e)}"
        return error_message';


CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.IDNSDL_RAW.ID_SERVER_DATA_SELLIN_PREPROCESSING("PARAM" ARRAY)
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
from snowflake.snowpark.types import StringType, StructType, StructField
import pandas as pd
from datetime import datetime
import pytz
 
def main(session: snowpark.Session,Param):
 
    try:
 
        #Param=[''Sellin_1.csv'',''IDNSDL_RAW.DEV_LOAD_STAGE_ADLS_IDN'',''dev/alteryx/transaction/sellin'',''sdl_all_distributor_sellin_sales_fact'']
 
        # Extracting parameters from the input
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
 
 
        # Define the schema for the DataFrame
        df_schema=StructType([
            StructField("BILL_DT", StringType()),
            StructField("BILL_DOC", StringType()),
            StructField("ITEM", StringType()),
            StructField("SLS_DOC", StringType()),
            StructField("ITEM1", StringType()),
            StructField("REF_DOC", StringType()),
            StructField("BILL_TYPE_ID", StringType()),
            StructField("BILL_TYPE", StringType()),
            StructField("JJ_SAP_DSTRBTR_ID", StringType()),
            StructField("JJ_SAP_DSTRBTR_NM", StringType()),
            StructField("JJ_SAP_PROD_ID", StringType()),
            StructField("JJ_SAP_PROD_DESC", StringType()),
            StructField("NUMERATOR", StringType()),
            StructField("QTY", StringType()),
            StructField("UOM", StringType()),
            StructField("NET_VAL", StringType()),
            StructField("CURR", StringType())
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

        dataframe = dataframe.withColumn("FILENAME",lit(file_name))
        
        #Creating copy of the Dataframe
        final_df = final_df = dataframe.select("BILL_DT" ,"BILL_DOC" ,"ITEM" ,"SLS_DOC" ,"ITEM1" ,"REF_DOC" ,"BILL_TYPE_ID" ,"BILL_TYPE" ,"JJ_SAP_DSTRBTR_ID" ,"JJ_SAP_DSTRBTR_NM" ,"JJ_SAP_PROD_ID" ,"JJ_SAP_PROD_DESC" ,"NUMERATOR","QTY",\\
                                               "UOM" ,"NET_VAL","CURR" ,"FILENAME")

        #return final_df
        
       # Load Data to the target table
        final_df.write.mode("append").saveAsTable(target_table)
 
         
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
 
        # write to success folder
        file_name=file_name.split(".")[0]
        final_df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True,file_format = "idnsdl_raw.CSV_FORMAT_PIPE")
        return "Success"
        # #return final_df
 
    
 
    except KeyError as key_error:
        # Handle KeyError (missing columns) here
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message
    except Exception as e:
        # Handle exceptions here
        error_message = f"Error: {str(e)}"
        return error_message';



CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.IDNSDL_RAW.ID_SERVER_DATA_SELLOUT_PREPROCESSING("PARAM" ARRAY)
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
from snowflake.snowpark.types import StringType, StructType, StructField
import pandas as pd
from datetime import datetime
import pytz
 
def main(session: snowpark.Session,Param):
 
    try:
 
        #Param=[''Sellout_1.csv'',''IDNSDL_RAW.DEV_LOAD_STAGE_ADLS_IDN'',''dev/alteryx/transaction/sellout'',''sdl_all_distributor_sellout_sales_fact'']
 
        # Extracting parameters from the input
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
 
 
        # Define the schema for the DataFrame
        df_schema=StructType([
            StructField("TRANS_KEY" ,StringType()), 
            StructField("BILL_DOC" ,StringType()), 
            StructField("BILL_DT" ,StringType()), 
            StructField("JJ_MNTH_ID" ,StringType()), 
            StructField("JJ_WK" ,StringType()), 
            StructField("DSTRBTR_CD" ,StringType()), 
            StructField("DSTRBTR_ID" ,StringType()), 
            StructField("JJ_SAP_DSTRBTR_ID" ,StringType()), 
            StructField("DSTRBTR_CUST_ID" ,StringType()), 
            StructField("DSTRBTR_PROD_ID" ,StringType()), 
            StructField("JJ_SAP_PROD_ID" ,StringType()), 
            StructField("DSTRBTN_CHNL" ,StringType()), 
            StructField("GRP_OUTLET" ,StringType()), 
            StructField("DSTRBTR_SLSMN_ID" ,StringType()), 
            StructField("SLS_QTY" ,StringType()), 
            StructField("GRS_VAL" ,StringType()),
            StructField("JJ_NET_VAL" ,StringType()),
            StructField("TRD_DSCNT" ,StringType()),
            StructField("DSTRBTR_NET_VAL" ,StringType()),
            StructField("RTRN_QTY" ,StringType()),
            StructField("RTRN_VAL" ,StringType())
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

        dataframe = dataframe.withColumn("FILENAME",lit(file_name))
        dataframe = dataframe.withColumn("SLS_QTY", regexp_replace(col("SLS_QTY"), '''', None))
        dataframe = dataframe.withColumn("GRS_VAL", regexp_replace(col("GRS_VAL"), '''', None))
        dataframe = dataframe.withColumn("JJ_NET_VAL", regexp_replace(col("JJ_NET_VAL"), '''', None))
        dataframe = dataframe.withColumn("TRD_DSCNT", regexp_replace(col("TRD_DSCNT"), '''', None))
        dataframe = dataframe.withColumn("DSTRBTR_NET_VAL", regexp_replace(col("DSTRBTR_NET_VAL"), '''', None))
        dataframe = dataframe.withColumn("RTRN_QTY", regexp_replace(col("RTRN_QTY"), '''', None))
        dataframe = dataframe.withColumn("RTRN_VAL", regexp_replace(col("RTRN_VAL"), '''', None))


        
        #Creating copy of the Dataframe
        final_df = final_df = dataframe.select("TRANS_KEY" ,"BILL_DOC" ,"BILL_DT" ,"JJ_MNTH_ID" ,"JJ_WK" ,"DSTRBTR_CD" ,"DSTRBTR_ID" ,"JJ_SAP_DSTRBTR_ID" ,"DSTRBTR_CUST_ID" ,"DSTRBTR_PROD_ID" ,"JJ_SAP_PROD_ID" ,"DSTRBTN_CHNL" ,"GRP_OUTLET" ,"DSTRBTR_SLSMN_ID" ,"SLS_QTY" ,"GRS_VAL",\\
                                               "JJ_NET_VAL","TRD_DSCNT","DSTRBTR_NET_VAL","RTRN_QTY","RTRN_VAL","FILENAME")

        #return final_df
        
       # Load Data to the target table
        final_df.write.mode("append").saveAsTable(target_table)
 
         
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
 
        # write to success folder
        file_name=file_name.split(".")[0]
        final_df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True,file_format = "idnsdl_raw.CSV_FORMAT_PIPE")
        return "Success"
        # #return final_df
 
    
 
    except KeyError as key_error:
        # Handle KeyError (missing columns) here
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message
    except Exception as e:
        # Handle exceptions here
        error_message = f"Error: {str(e)}"
        return error_message';


CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.IDNSDL_RAW.ID_STOCK_PREPROCESSING("PARAM" ARRAY)
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
from snowflake.snowpark.types import StringType, StructType, StructField
import pandas as pd
from datetime import datetime
import pytz
 
def main(session: snowpark.Session,Param):
 
    try:
 
        #Param=[''Stock_1_20240524060503.csv'',''IDNSDL_RAW.DEV_LOAD_STAGE_ADLS_IDN'',''dev/certified_data_lake/transaction/stock'',''SDL_STOCK_DIST_MAP'']
 
        # Extracting parameters from the input
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
 
 
        # Define the schema for the DataFrame
        df_schema=StructType([
            StructField("DSTRBTR_CD", StringType()),
            StructField("STOCK_DT", StringType()),
            StructField("DSTRBTR_ID", StringType()),
            StructField("DSTRBTR_PROD_ID", StringType()),
            StructField("GIT", StringType()),
            StructField("TOT_STOCK", StringType()),
            StructField("STOCK_KEY", StringType()),
            StructField("STOCK_VAL", StringType()),
            StructField("STOCK_NIV", StringType()),
            StructField("JJ_SAP_PROD_ID", StringType()),
            StructField("JJ_SAP_DSTRBTR_ID", StringType()),
            StructField("QUARTER", StringType()),
            StructField("JJ_WK", StringType()),
            StructField("JJ_MNTH_ID", StringType()),
            StructField("JJ_YEAR", StringType()),
            StructField("YEARMONTH", StringType()),
            StructField("WEEK_2", StringType()),
            StructField("SALEABLE_STOCK_QTY", StringType()),
            StructField("SALEABLE_STOCK_VALUE", StringType()),
            StructField("NON_SALEABLE_STOCK_QTY", StringType()), 
            StructField("NON_SALEABLE_STOCK_VALUE", StringType()) 
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

        dataframe = dataframe.withColumn("FILENAME",lit(file_name))
        dataframe = dataframe.withColumn("RUN_ID",lit(lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S"))))
        dataframe = dataframe.withColumn("GIT", regexp_replace(col("GIT"), '''', None))


        


        #Creating copy of the Dataframe
        final_df = final_df = dataframe.select("DSTRBTR_CD","STOCK_DT","DSTRBTR_ID","DSTRBTR_PROD_ID","GIT","TOT_STOCK","STOCK_KEY","STOCK_VAL","STOCK_NIV","JJ_SAP_PROD_ID","JJ_SAP_DSTRBTR_ID","QUARTER","JJ_WK","JJ_MNTH_ID","JJ_YEAR","YEARMONTH","WEEK_2",\\
                                               "FILENAME","RUN_ID","SALEABLE_STOCK_QTY","SALEABLE_STOCK_VALUE","NON_SALEABLE_STOCK_QTY","NON_SALEABLE_STOCK_VALUE")

        #return final_df
        
       # Load Data to the target table
        final_df.write.mode("append").saveAsTable(target_table)
 
         
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
 
        # write to success folder
        file_name=file_name.split(".")[0]
        final_df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True,file_format = "idnsdl_raw.CSV_FORMAT_PIPE")
        return "Success"
        # #return final_df
 
    
 
    except KeyError as key_error:
        # Handle KeyError (missing columns) here
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message
    except Exception as e:
        # Handle exceptions here
        error_message = f"Error: {str(e)}"
        return error_message';


CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.IDNSDL_RAW.ID_PS_VISIBILITY_PREPROCESSING("PARAM" ARRAY)
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
from snowflake.snowpark.functions import col,lit,trim
from snowflake.snowpark.types import StringType, StructType, StructField
import pandas as pd
from datetime import datetime
import pytz
 
def main(session: snowpark.Session,Param):
 
    try:
 
        #Param=[''VISIBILITY - Y2024 M5 - CREATE AT 2024.05.20 01.46.49.txt'',''IDNSDL_RAW.DEV_LOAD_STAGE_ADLS_IDN'',''dev/5Ps/transaction/Visibility'',''SDL_ID_PS_VISIBILITY'']
 
        # Extracting parameters from the input
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
 
 
        # Define the schema for the DataFrame
        df_schema=StructType([
            StructField("OUTLET_ID", StringType()),
            StructField("OUTLET_CUSTOM_CODE", StringType()),
            StructField("OUTLET_NAME", StringType()),
            StructField("PROVINCE", StringType()),
            StructField("CITY", StringType()),
            StructField("CHANNEL", StringType()),
            StructField("MERCHANDISER_ID", StringType()),
            StructField("MERCHANDISER_NAME", StringType()),
            StructField("CUST_GROUP", StringType()),
            StructField("ADDRESS", StringType()),
            StructField("JNJ_YEAR", StringType()),
            StructField("JNJ_MONTH", StringType()),
            StructField("JNJ_WEEK", StringType()),
            StructField("DAY_NAME", StringType()),
            StructField("INPUT_DATE", StringType()),
            StructField("FRANCHISE", StringType()),
            StructField("PRODUCT_CMP_COMPETITOR_JNJ", StringType()),
            StructField("NUMBER_OF_FACING", StringType()),
            StructField("SHARE_OF_SHELF", StringType()),
            StructField("PHOTO_LINK", StringType()) 
            ])
        modified_file_name = file_name.split("_")[0]
 
 
        # Read the CSV file into a DataFrame
        dataframe = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "\\t")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .option("skip_blank_lines", True)\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+modified_file_name)
 
        #---------------------------Transformation logic ------------------------------#
 
        # Handle null values or empty rows
        dataframe=dataframe.na.drop("all")
 
        # Check for empty Dataframe
        if dataframe.count()==0:
            return "No Data in file"

        dataframe = dataframe.withColumn("file_name",lit(file_name))


        #Creating copy of the Dataframe
        final_df = final_df = dataframe.select(trim(col("OUTLET_ID")),trim(col("OUTLET_CUSTOM_CODE")),trim(col("OUTLET_NAME")),trim(col("PROVINCE")),trim(col("CITY")),trim(col("CHANNEL")),trim(col("MERCHANDISER_ID")),\\
                                               trim(col("MERCHANDISER_NAME")),trim(col("CUST_GROUP")),trim(col("ADDRESS")),trim(col("JNJ_YEAR")),trim(col("JNJ_MONTH")), trim(col("JNJ_WEEK")),trim(col("DAY_NAME")),trim(col("INPUT_DATE")),\\
                                               trim(col("FRANCHISE")),trim(col("PRODUCT_CMP_COMPETITOR_JNJ")),trim(col("NUMBER_OF_FACING")),trim(col("SHARE_OF_SHELF")),\\
                                               trim(col("PHOTO_LINK")),"file_name")

        
        # Load Data to the target table
        final_df.write.mode("append").saveAsTable(target_table)
 
         
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
 
        # write to success folder
        file_name=file_name.split(".")[0]
        final_df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)
        return "Success"
        # #return final_df
 
    
 
    except KeyError as key_error:
        # Handle KeyError (missing columns) here
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message
    except Exception as e:
        # Handle exceptions here
        error_message = f"Error: {str(e)}"
        return error_message';


CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.IDNSDL_RAW.ID_PS_BRAND_PLANOGRAM_PREPROCESSING("PARAM" ARRAY)
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
from snowflake.snowpark.functions import col,lit,trim
from snowflake.snowpark.types import StringType, StructType, StructField
import pandas as pd
from datetime import datetime
import pytz
 
def main(session: snowpark.Session,Param):
 
    try:
 
        #Param=[''BRAND BLOCKING - Y2024 M5 - CREATE AT 2024.05.20 01.49.02.txt'',''IDNSDL_RAW.DEV_LOAD_STAGE_ADLS_IDN'',''dev/5Ps/transaction/Brand_Blocking'',''SDL_ID_PS_BRAND_BLOCKING'']
        #Param=[''PLANOGRAM - Y2024 M5 - CREATE AT 2024.05.20 01.48.50.txt'',''IDNSDL_RAW.DEV_LOAD_STAGE_ADLS_IDN'',''dev/5Ps/transaction/Planogram'',''SDL_ID_PS_PLANOGRAM'']
        # Extracting parameters from the input
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
 
 
        # Define the schema for the DataFrame
        df_schema=StructType([
            StructField("OUTLET_ID", StringType()),
            StructField("OUTLET_CUSTOM_CODE", StringType()),
            StructField("OUTLET_NAME", StringType()),
            StructField("PROVINCE", StringType()),
            StructField("CITY", StringType()),
            StructField("CHANNEL", StringType()),
            StructField("MERCHANDISER_ID", StringType()),
            StructField("MERCHANDISER_NAME", StringType()),
            StructField("CUST_GROUP", StringType()),
            StructField("ADDRESS", StringType()),
            StructField("JNJ_YEAR", StringType()),
            StructField("JNJ_MONTH", StringType()),
            StructField("JNJ_WEEK", StringType()),
            StructField("DAY_NAME", StringType()),
            StructField("INPUT_DATE", StringType()),
            StructField("FRANCHISE", StringType()),
            StructField("PHOTO_LINK", StringType())
            
            ])
        modified_file_name = file_name.split("_")[0]
 
 
        # Read the CSV file into a DataFrame
        dataframe = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "\\t")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .option("skip_blank_lines", True)\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+modified_file_name)
 
        #---------------------------Transformation logic ------------------------------#
 
        # Handle null values or empty rows
        dataframe=dataframe.na.drop("all")
 
        # Check for empty Dataframe
        if dataframe.count()==0:
            return "No Data in file"

        dataframe = dataframe.withColumn("file_name",lit(file_name))


        #Creating copy of the Dataframe
        final_df = final_df = dataframe.select(trim(col("OUTLET_ID")),trim(col("OUTLET_CUSTOM_CODE")),trim(col("OUTLET_NAME")),trim(col("PROVINCE")),trim(col("CITY")),trim(col("CHANNEL")),trim(col("MERCHANDISER_ID")),\\
                                               trim(col("MERCHANDISER_NAME")),trim(col("CUST_GROUP")),trim(col("ADDRESS")),trim(col("JNJ_YEAR")),trim(col("JNJ_MONTH")), trim(col("JNJ_WEEK")),trim(col("DAY_NAME")),trim(col("INPUT_DATE")),\\
                                               trim(col("FRANCHISE")),trim(col("PHOTO_LINK")),"file_name")

        
        # Load Data to the target table
        final_df.write.mode("append").saveAsTable(target_table)
 
         
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
 
        # write to success folder
        file_name=file_name.split(".")[0]
        final_df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)
        return "Success"
        # #return final_df
 
    
 
    except KeyError as key_error:
        # Handle KeyError (missing columns) here
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message
    except Exception as e:
        # Handle exceptions here
        error_message = f"Error: {str(e)}"
        return error_message';


CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.IDNSDL_RAW.ID_PS_PRICING_PREPROCESSING("PARAM" ARRAY)
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
from snowflake.snowpark.functions import col,lit,trim
from snowflake.snowpark.types import StringType, StructType, StructField
import pandas as pd
from datetime import datetime
import pytz
 
def main(session: snowpark.Session,Param):
 
    try:
 
        #Param=[''PRICING - Y2024 M5 - CREATE AT 2024.05.20 01.49.14.txt'',''IDNSDL_RAW.DEV_LOAD_STAGE_ADLS_IDN'',''dev/5Ps/transaction/Pricing'',''SDL_ID_PS_PRICING'']
 
        # Extracting parameters from the input
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
 
 
        # Define the schema for the DataFrame
        df_schema=StructType([
            StructField("OUTLET_ID", StringType()),
            StructField("OUTLET_CUSTOM_CODE", StringType()),
            StructField("OUTLET_NAME", StringType()),
            StructField("PROVINCE", StringType()),
            StructField("CITY", StringType()),
            StructField("CHANNEL", StringType()),
            StructField("MERCHANDISER_ID", StringType()),
            StructField("MERCHANDISER_NAME", StringType()),
            StructField("CUST_GROUP", StringType()),
            StructField("ADDRESS", StringType()),
            StructField("JNJ_YEAR", StringType()),
            StructField("JNJ_MONTH", StringType()),
            StructField("JNJ_WEEK", StringType()),
            StructField("DAY_NAME", StringType()),
            StructField("INPUT_DATE", StringType()),
            StructField("FRANCHISE", StringType()),
            StructField("PUT_UP", StringType()),
            StructField("COMPETITOR_VARIANT", StringType()),
            StructField("COMPETITOR", StringType()),
            StructField("PRICE_TYPE", StringType()),
            StructField("PRICE", StringType())
            
            ])
        modified_file_name = file_name.split("_")[0]
 
 
        # Read the CSV file into a DataFrame
        dataframe = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "\\t")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .option("skip_blank_lines", True)\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+modified_file_name)
 
        #---------------------------Transformation logic ------------------------------#
 
        # Handle null values or empty rows
        dataframe=dataframe.na.drop("all")
 
        # Check for empty Dataframe
        if dataframe.count()==0:
            return "No Data in file"

        dataframe = dataframe.withColumn("file_name",lit(file_name))


        #Creating copy of the Dataframe
        final_df = final_df = dataframe.select(trim(col("OUTLET_ID")),trim(col("OUTLET_CUSTOM_CODE")),trim(col("OUTLET_NAME")),trim(col("PROVINCE")),trim(col("CITY")),trim(col("CHANNEL")),trim(col("MERCHANDISER_ID")),\\
                                               trim(col("MERCHANDISER_NAME")),trim(col("CUST_GROUP")),trim(col("ADDRESS")),trim(col("JNJ_YEAR")),trim(col("JNJ_MONTH")), trim(col("JNJ_WEEK")),trim(col("DAY_NAME")),trim(col("INPUT_DATE")),\\
                                               trim(col("FRANCHISE")),trim(col("PUT_UP")),trim(col("COMPETITOR_VARIANT")),trim(col("COMPETITOR")),\\
                                               trim(col("PRICE_TYPE")),trim(col("PRICE")),"file_name")

        
        # Load Data to the target table
        final_df.write.mode("append").saveAsTable(target_table)
 
         
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
 
        # write to success folder
        file_name=file_name.split(".")[0]
        final_df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)
        return "Success"
        # #return final_df
 
    
 
    except KeyError as key_error:
        # Handle KeyError (missing columns) here
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message
    except Exception as e:
        # Handle exceptions here
        error_message = f"Error: {str(e)}"
        return error_message';


CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.IDNSDL_RAW.ID_PS_PRODUCT_AVAILABILITY_PREPROCESSING("PARAM" VARIANT)
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
from snowflake.snowpark.functions import col,lit,trim
from snowflake.snowpark.types import StringType, StructType, StructField
import pandas as pd
from datetime import datetime
import pytz
 
def main(session: snowpark.Session,Param):
 
    try:
 
        #Param=[''PRODUCT AVAILABILITY - Y2024 M5 - CREATE AT 2024.05.20 01.43.31.txt'',''IDNSDL_RAW.DEV_LOAD_STAGE_ADLS_IDN'',''dev/5Ps/transaction/product_availability'',''SDL_ID_PS_PRODUCT_AVAILABILITY'']
 
        # Extracting parameters from the input
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
 
 
        # Define the schema for the DataFrame
        df_schema=StructType([
            StructField("OUTLET_ID", StringType()),
            StructField("OUTLET_CUSTOM_CODE", StringType()),
            StructField("OUTLET_NAME", StringType()),
            StructField("PROVINCE", StringType()),
            StructField("CITY", StringType()),
            StructField("CHANNEL", StringType()),
            StructField("MERCHANDISER_ID", StringType()),
            StructField("MERCHANDISER_NAME", StringType()),
            StructField("CUST_GROUP", StringType()),
            StructField("ADDRESS", StringType()),
            StructField("JNJ_YEAR", StringType()),
            StructField("JNJ_MONTH", StringType()),
            StructField("JNJ_WEEK", StringType()),
            StructField("DAY_NAME", StringType()),
            StructField("INPUT_DATE", StringType()),
            StructField("FRANCHISE", StringType()),
            StructField("PUT_UP_SKU", StringType()),
            StructField("STOCK_QTY_PCS", StringType()),
            StructField("OSA_FLAG", StringType()),
            StructField("AVAILABILITY_PER_FRANCHISE", StringType()),
            StructField("AVAILABILITY_OSA", StringType())

            
            ])
        modified_file_name = file_name.split("_")[0]
 
 
        # Read the CSV file into a DataFrame
        dataframe = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "\\t")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .option("skip_blank_lines", True)\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+modified_file_name)
 
        #---------------------------Transformation logic ------------------------------#
 
        # Handle null values or empty rows
        dataframe=dataframe.na.drop("all")
 
        # Check for empty Dataframe
        if dataframe.count()==0:
            return "No Data in file"

        dataframe = dataframe.withColumn("file_name",lit(file_name))


        #Creating copy of the Dataframe
        final_df = final_df = dataframe.select(trim(col("OUTLET_ID")),trim(col("OUTLET_CUSTOM_CODE")),trim(col("OUTLET_NAME")),trim(col("PROVINCE")),trim(col("CITY")),trim(col("CHANNEL")),trim(col("MERCHANDISER_ID")),\\
                                               trim(col("MERCHANDISER_NAME")),trim(col("CUST_GROUP")),trim(col("ADDRESS")),trim(col("JNJ_YEAR")),trim(col("JNJ_MONTH")), trim(col("JNJ_WEEK")),trim(col("DAY_NAME")),trim(col("INPUT_DATE")),\\
                                               trim(col("FRANCHISE")),trim(col("PUT_UP_SKU")),trim(col("STOCK_QTY_PCS")),trim(col("OSA_FLAG")),\\
                                               trim(col("AVAILABILITY_PER_FRANCHISE")),trim(col("AVAILABILITY_OSA")),"file_name")

        
        # Load Data to the target table
        final_df.write.mode("append").saveAsTable(target_table)
 
         
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
 
        # write to success folder
        file_name=file_name.split(".")[0]
        final_df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)
        return "Success"
        # #return final_df
 
    
 
    except KeyError as key_error:
        # Handle KeyError (missing columns) here
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message
    except Exception as e:
        # Handle exceptions here
        error_message = f"Error: {str(e)}"
        return error_message';


CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.IDNSDL_RAW.ID_PS_PROMO_COMPETITOR_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS 'from snowflake.snowpark.functions import col, lit, date_format, current_timestamp, to_date, year, month, concat, format_number, regexp_replace,to_timestamp,when,trim,upper, substring
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DecimalType
import snowflake.snowpark
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz
import pandas as pd

def main(session: snowpark.Session,Param):
    # Param=["PROMO COMPETITOR - Y2024 M5 - CREATE AT 2024.05.14 03.10.15.txt","IDNSDL_RAW.DEV_LOAD_STAGE_ADLS_IDN","dev/5Ps/transaction/promo_competitor/","SDL_ID_PS_PROMOTION_COMPETITOR"]
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]

        file_name_new= file_name.split("_")[0]
        # print(file_name_new)
        
        df_schema = StructType([
                # StructField("mnth_id",StringType(), True),
                StructField("outlet_id",StringType(), True),
                StructField("outlet_custom_code",StringType(), True),
                StructField("outlet_name",StringType(), True),
                StructField("province",StringType(), True),
                StructField("city",StringType(), True),
                StructField("channel",StringType(), True),
                StructField("merchandiser_id",StringType(), True),
                StructField("merchandiser_name",StringType(), True),
                StructField("cust_group",StringType(), True),
                StructField("address",StringType(), True),
                StructField("jnj_year",StringType(), True),
                StructField("jnj_month",StringType(), True),
                StructField("jnj_week",StringType(), True),
                StructField("day_name",StringType(), True),
                StructField("input_date",StringType(), True),
                StructField("franchise",StringType(), True),
                StructField("photo_link",StringType(), True),
                StructField("description",StringType(), True),
                # StructField("file_name",StringType(), True)
               
            ])
        
        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter","\\t")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name_new)
            # .option("field_delimiter", "\\u0001")\\

        
        
        df=df.na.drop("all")
        if df.count()==0 :
            return "No Data in file"

        
        # df = df.with_column("cdl_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        file_name1=file_name.split(".")[0]+"_"+ datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")+".csv"
        df = df.with_column("file_name", lit(file_name1))
        df = df.with_column("crtd_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        # df = df.with_column("mnth_id", file_name.split("_")[1])
        df = df.with_column("run_id", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        # df = df.with_column("pos_cust", lit(file_name.split("_")[1]))
        # df = df.with_column("yearmonth", lit(file_name.split("_")[2].split(".")[0]))
        
        # date=file_name.split("_")[3].split(".")[0]
        # year=date[0:4]
        # mnth_no=date[4:6]
        # inv_week=date[6:]
        # df = df.withColumn("year", lit(year))
        # df = df.with_column("mnth_no", lit(mnth_no))
        # df = df.with_column("inv_week", lit(inv_week))  
        
        snowdf= df.select(
                    # "mnth_id",
                    "outlet_id",
                    "outlet_custom_code",
                    "outlet_name",
                    "province",
                    "city",
                    "channel",
                    "merchandiser_id",
                    "merchandiser_name",
                    "cust_group",
                    "address",
                    "jnj_year",
                    "jnj_month",
                    "jnj_week",
                    "day_name",
                    "input_date",
                    "franchise",
                    "photo_link",
                    "description",
                    "file_name"              
                    )
        file_name=file_name.split(".")[0]+"_"+datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")

        # df.show()
        
        #move to success
        
        # snowdf.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)
        # snowdf.write.mode("append").saveAsTable(stage_name.split(".")[0] + "." + target_table)
        
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


CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.IDNSDL_RAW.ID_PS_PROMO_PREPROCESSING("PARAM" ARRAY)
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
from snowflake.snowpark.functions import col,lit,trim
from snowflake.snowpark.types import StringType, StructType, StructField
import pandas as pd
from datetime import datetime
import pytz
 
def main(session: snowpark.Session,Param):
 
    try:
 
        #Param=[''PROMO - Y2024 M5 - CREATE AT 2024.05.20 02.01.05.txt'',''IDNSDL_RAW.DEV_LOAD_STAGE_ADLS_IDN'',''dev/5Ps/transaction/promo'',''SDL_ID_PS_PROMOTION'']
 
        # Extracting parameters from the input
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
 
 
        # Define the schema for the DataFrame
        df_schema=StructType([
            StructField("OUTLET_ID", StringType()),
            StructField("OUTLET_CUSTOM_CODE", StringType()),
            StructField("OUTLET_NAME", StringType()),
            StructField("PROVINCE", StringType()),
            StructField("CITY", StringType()),
            StructField("CHANNEL", StringType()),
            StructField("MERCHANDISER_ID", StringType()),
            StructField("MERCHANDISER_NAME", StringType()),
            StructField("CUST_GROUP", StringType()),
            StructField("ADDRESS", StringType()),
            StructField("JNJ_YEAR", StringType()),
            StructField("JNJ_MONTH", StringType()),
            StructField("JNJ_WEEK", StringType()),
            StructField("DAY_NAME", StringType()),
            StructField("INPUT_DATE", StringType()),
            StructField("PROMO_DESC", StringType()),
            StructField("PHOTO_LINK", StringType()),
            StructField("POSM_EXECUTION_FLAG", StringType())
            ])
        modified_file_name = file_name.split("_")[0]
 
 
        # Read the CSV file into a DataFrame
        dataframe = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "\\t")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .option("skip_blank_lines", True)\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+modified_file_name)
 
        #---------------------------Transformation logic ------------------------------#
 
        # Handle null values or empty rows
        dataframe=dataframe.na.drop("all")
 
        # Check for empty Dataframe
        if dataframe.count()==0:
            return "No Data in file"

        dataframe = dataframe.withColumn("file_name",lit(file_name))


        #Creating copy of the Dataframe
        final_df = final_df = dataframe.select(trim(col("OUTLET_ID")),trim(col("OUTLET_CUSTOM_CODE")),trim(col("OUTLET_NAME")),trim(col("PROVINCE")),trim(col("CITY")),trim(col("CHANNEL")),trim(col("MERCHANDISER_ID")),\\
                                               trim(col("MERCHANDISER_NAME")),trim(col("CUST_GROUP")),trim(col("ADDRESS")),trim(col("JNJ_YEAR")),trim(col("JNJ_MONTH")), trim(col("JNJ_WEEK")),trim(col("DAY_NAME")),trim(col("INPUT_DATE")),\\
                                               trim(col("PROMO_DESC")),trim(col("PHOTO_LINK")),trim(col("POSM_EXECUTION_FLAG")),"file_name")

        
        # Load Data to the target table
        final_df.write.mode("append").saveAsTable(target_table)
 
         
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
 
        # write to success folder
        file_name=file_name.split(".")[0]
        final_df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)
        return "Success"
        # #return final_df
 
    
 
    except KeyError as key_error:
        # Handle KeyError (missing columns) here
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message
    except Exception as e:
        # Handle exceptions here
        error_message = f"Error: {str(e)}"
        return error_message';


CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.IDNSDL_RAW.ID_PS_SECONDARY_DISPLAY_PREPROCESSING("PARAM" ARRAY)
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
from snowflake.snowpark.functions import col,lit,trim
from snowflake.snowpark.types import StringType, StructType, StructField
import pandas as pd
from datetime import datetime
import pytz
 
def main(session: snowpark.Session,Param):
 
    try:
 
        #Param=[''SECONDARY DISPLAY - Y2024 M5 - CREATE AT 2024.05.20 02.01.04.txt'',''IDNSDL_RAW.DEV_LOAD_STAGE_ADLS_IDN'',''dev/5Ps/transaction/secondary_display'',''SDL_ID_PS_SECONDARY_DISPLAY'']
 
        # Extracting parameters from the input
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
 
 
        # Define the schema for the DataFrame
        df_schema=StructType([
            StructField("OUTLET_ID", StringType()),
            StructField("OUTLET_CUSTOM_CODE", StringType()),
            StructField("OUTLET_NAME", StringType()),
            StructField("PROVINCE", StringType()),
            StructField("CITY", StringType()),
            StructField("CHANNEL", StringType()),
            StructField("MERCHANDISER_ID", StringType()),
            StructField("MERCHANDISER_NAME", StringType()),
            StructField("CUST_GROUP", StringType()),
            StructField("ADDRESS", StringType()),
            StructField("JNJ_YEAR", StringType()),
            StructField("JNJ_MONTH", StringType()),
            StructField("JNJ_WEEK", StringType()),
            StructField("DAY_NAME", StringType()),
            StructField("INPUT_DATE", StringType()),
            StructField("FRANCHISE", StringType()),
            StructField("PHOTO_LINK", StringType()),
            StructField("RENT", StringType())

            
            ])
        modified_file_name = file_name.split("_")[0]
 
 
        # Read the CSV file into a DataFrame
        dataframe = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "\\t")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .option("skip_blank_lines", True)\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+modified_file_name)
 
        #---------------------------Transformation logic ------------------------------#
 
        # Handle null values or empty rows
        dataframe=dataframe.na.drop("all")
 
        # Check for empty Dataframe
        if dataframe.count()==0:
            return "No Data in file"

        dataframe = dataframe.withColumn("file_name",lit(file_name))


        #Creating copy of the Dataframe
        final_df = final_df = dataframe.select(trim(col("OUTLET_ID")),trim(col("OUTLET_CUSTOM_CODE")),trim(col("OUTLET_NAME")),trim(col("PROVINCE")),trim(col("CITY")),trim(col("CHANNEL")),trim(col("MERCHANDISER_ID")),\\
                                               trim(col("MERCHANDISER_NAME")),trim(col("CUST_GROUP")),trim(col("ADDRESS")),trim(col("JNJ_YEAR")),trim(col("JNJ_MONTH")), trim(col("JNJ_WEEK")),trim(col("DAY_NAME")),trim(col("INPUT_DATE")),\\
                                               trim(col("FRANCHISE")),trim(col("PHOTO_LINK")),trim(col("RENT")),"file_name")

        
        # Load Data to the target table
        final_df.write.mode("append").saveAsTable(target_table)
 
         
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
 
        # write to success folder
        file_name=file_name.split(".")[0]
        final_df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)
        return "Success"
        # #return final_df
 
    
 
    except KeyError as key_error:
        # Handle KeyError (missing columns) here
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message
    except Exception as e:
        # Handle exceptions here
        error_message = f"Error: {str(e)}"
        return error_message';


CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.IDNSDL_RAW.POS_ALFAMIDI_SELLOUT_STOCK_PREPROCESSING("PARAM" ARRAY)
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

        #Param=[''Sellout_Alfamidi_202404.csv'',''IDNSDL_RAW.DEV_LOAD_STAGE_ADLS_IDN'',''dev/POS/transaction/MIDI/sellout/'',''SDL_ID_POS_MIDI_SELLOUT'']
        #Param=[''Stock_Alfamidi_202403.csv'',''IDNSDL_RAW.DEV_LOAD_STAGE_ADLS_IDN'',''dev/POS/transaction/MIDI/stock/'',''SDL_ID_POS_MIDI_STOCK'']

        # Extracting parameters from the input
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        sch_name        = stage_name.split(''.'')[0]
        target_table    = sch_name+"."+Param[3]

        if target_table.upper()=="IDNSDL_RAW.SDL_ID_POS_MIDI_SELLOUT":


            # Define the schema for the DataFrame
            df_schema_sellout=StructType([
                StructField("NO",StringType()),
                StructField("PLU",StringType()),
                StructField("DESCRIPTION",StringType()),
                StructField("BRANCH",StringType()),
                StructField("TYPE",StringType()),
                StructField("VALUES",StringType())
                ])
    
    
            # Read the CSV file into a DataFrame
        
            dataframe = session.read\\
                .schema(df_schema_sellout)\\
                .option("skip_header",1)\\
                .option("field_delimiter", ",")\\
                .option("field_optionally_enclosed_by", "\\"")\\
                .option("encoding","UTF-8")\\
                .option("skip_blank_lines", True)\\
                .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        elif target_table.upper()=="IDNSDL_RAW.SDL_ID_POS_MIDI_STOCK":

            # Define the schema for the DataFrame
            df_schema_stock=StructType([
                StructField("NO",StringType()),
                StructField("PLU",StringType()),
                StructField("DESCRIPTION",StringType()),
                StructField("BRANCH",StringType()),
                StructField("STORE_DC",StringType()),
                StructField("TYPE",StringType()),
                StructField("VALUES",StringType())
                ])
    
    
            # Read the CSV file into a DataFrame
        
            dataframe = session.read\\
                .schema(df_schema_stock)\\
                .option("skip_header",1)\\
                .option("field_delimiter", ",")\\
                .option("field_optionally_enclosed_by", "\\"")\\
                .option("encoding","UTF-8")\\
                .option("skip_blank_lines", True)\\
                .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
            

        #---------------------------Transformation logic ------------------------------#
        

        # Handle null values or empty rows
        dataframe=dataframe.na.drop("all")

        # Check for empty Dataframe
        if dataframe.count()==0:
            return "No Data in file"

        # Add column POS_CUST, YEARMONTH, RUN_ID, CRTD_DTTM, file_name
        POS_CUST="ALFAMIDI"
        yearmonth=file_name.split("_")[2].split(".")[0]

        dataframe = dataframe.with_column("POS_CUST",lit(POS_CUST))
        dataframe = dataframe.with_column("YEARMONTH",lit(yearmonth))
        dataframe = dataframe.with_column("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")).cast("integer"))
        dataframe = dataframe.with_column("CRTD_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        dataframe = dataframe.with_column("FILENAME",lit(file_name))

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


CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.IDNSDL_RAW.POS_CARREFOUR_SELLOUT_STOCK_PREPROCESSING("PARAM" ARRAY)
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

        #Param=[''Sellout_Carrefour_202012.csv'',''IDNSDL_RAW.DEV_LOAD_STAGE_ADLS_IDN'',''dev/POS/transaction/Carrefour/sellout/'',''SDL_ID_POS_CARREFOUR_SELLOUT'']
        #Param=[''Stock_Carrefour_202012.csv'',''IDNSDL_RAW.DEV_LOAD_STAGE_ADLS_IDN'',''dev/POS/transaction/Carrefour/stock/'',''SDL_ID_POS_CARREFOUR_STOCK'']

        # Extracting parameters from the input
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        sch_name        = stage_name.split(''.'')[0]
        target_table    = sch_name+"."+Param[3]

        if target_table.upper()=="IDNSDL_RAW.SDL_ID_POS_CARREFOUR_SELLOUT":


            # Define the schema for the DataFrame
            df_schema=StructType([
                StructField("FDESC",StringType()),
                StructField("SCC",StringType()),
                StructField("SCC_NAME",StringType()),
                StructField("SALES_QTY",StringType()),
                StructField("NET_SALES",StringType()),
                StructField("SHARE",StringType())
                ])


            # Read the CSV file into a DataFrame
        
            dataframe = session.read\\
                .schema(df_schema)\\
                .option("skip_header",1)\\
                .option("field_delimiter", "\\u0001")\\
                .option("field_optionally_enclosed_by", "\\"")\\
                .option("skip_blank_lines", True)\\
                .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
            

        elif target_table.upper()=="IDNSDL_RAW.SDL_ID_POS_CARREFOUR_STOCK":
            
            # Define the schema for the DataFrame
            df_schema_stock=StructType([
                StructField("DEP_DESC",StringType()),
                StructField("STOCK_QTY",StringType()),
                StructField("STOCK_AMT",StringType()),
                StructField("STOCK_DAYS",StringType())
                ])
            

            # Read the CSV file into a DataFrame
    
            dataframe = session.read\\
            .schema(df_schema_stock)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "\\u0001")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .option("skip_blank_lines", True)\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        

        #---------------------------Transformation logic ------------------------------#
        

        # Handle null values or empty rows
        dataframe=dataframe.na.drop("all")

        # Check for empty Dataframe
        if dataframe.count()==0:
            return "No Data in file"

        # Add column POS_CUST, YEARMONTH, RUN_ID, CRTD_DTTM, file_name
        POS_CUST="CARREFOUR"
        yearmonth=file_name.split("_")[2].split(".")[0]

        dataframe = dataframe.with_column("POS_CUST",lit(POS_CUST))
        dataframe = dataframe.with_column("YEARMONTH",lit(yearmonth))
        dataframe = dataframe.with_column("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")).cast("integer"))
        dataframe = dataframe.with_column("CRTD_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        dataframe = dataframe.with_column("FILENAME",lit(file_name))

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


CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.IDNSDL_RAW.POS_SAT_SELLOUT_STOCK_PREPROCESSING("PARAM" ARRAY)
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

        #Param=[''Stock_Alfamart_202404.csv'',''IDNSDL_RAW.DEV_LOAD_STAGE_ADLS_IDN'',''dev/POS/transaction/SAT/stock/'',''SDL_ID_POS_SAT_STOCK'']

        # Extracting parameters from the input
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        sch_name        = stage_name.split(''.'')[0]
        target_table    = sch_name+"."+Param[3]

        if target_table.upper()=="IDNSDL_RAW.SDL_ID_POS_SAT_SELLOUT":
            
            # Define the schema for the DataFrame
            df_schema_sellout=StructType([
                StructField("NO",StringType()),
                StructField("PLU",StringType()),
                StructField("DESCRIPTION",StringType()),
                StructField("BRANCH",StringType()),
                StructField("TYPE",StringType()),
                StructField("VALUES",StringType())
                ])
            

            # Read the CSV file into a DataFrame
    
            dataframe = session.read\\
            .schema(df_schema_sellout)\\
            .option("skip_header",1)\\
            .option("field_delimiter", ",")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .option("skip_blank_lines", True)\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        elif target_table.upper()=="IDNSDL_RAW.SDL_ID_POS_SAT_STOCK":
            
            # Define the schema for the DataFrame
            df_schema_stock=StructType([
                StructField("NO",StringType()),
                StructField("PLU",StringType()),
                StructField("DESCRIPTION",StringType()),
                StructField("BRANCH",StringType()),
                StructField("STORE_DC",StringType()),
                StructField("TYPE",StringType()),
                StructField("VALUES",StringType())
                ])
            

            # Read the CSV file into a DataFrame
    
            dataframe = session.read\\
            .schema(df_schema_stock)\\
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

        # Add column POS_CUST, YEARMONTH, RUN_ID, CRTD_DTTM, file_name
        POS_CUST="ALFA"
        yearmonth=file_name.split("_")[2].split(".")[0]

        dataframe = dataframe.with_column("POS_CUST",lit(POS_CUST))
        dataframe = dataframe.with_column("YEARMONTH",lit(yearmonth))
        dataframe = dataframe.with_column("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")).cast("integer"))
        dataframe = dataframe.with_column("CRTD_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        dataframe = dataframe.with_column("FILENAME",lit(file_name))

        # Creating Final Dataframe
        final_df = dataframe.alias("final_df")

        #Load Data to the target table
        
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


CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.IDNSDL_RAW.POS_SUPERINDO_SELLOUT_PREPROCESSING("PARAM" ARRAY)
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

        #Param=[''Sellout_Superindo_202001.csv'',''IDNSDL_RAW.DEV_LOAD_STAGE_ADLS_IDN'',''dev/POS/transaction/Superindo/sellout/'',''SDL_ID_POS_SUPERINDO_SELLOUT'']

        # Extracting parameters from the input
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        sch_name        = stage_name.split(''.'')[0]
        target_table    = sch_name+"."+Param[3]


        # Define the schema for the DataFrame
        df_schema=StructType([
            StructField("COMPANY",StringType()),
            StructField("COMPANY_ORIGINAL",StringType()),
            StructField("REGION",StringType()),
            StructField("GRP",StringType()),
            StructField("PRODUCT",StringType()),
            StructField("MON_SALES_PERCENT",StringType()),
            StructField("MON_ORDER",StringType()),
            StructField("MON_SUPPLY",StringType())
            ])


        # Read the CSV file into a DataFrame
    
        dataframe = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "\\u0001")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .option("skip_blank_lines", True)\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        #---------------------------Transformation logic ------------------------------#
        

        # Handle null values or empty rows
        dataframe=dataframe.na.drop("all")

        # Check for empty Dataframe
        if dataframe.count()==0:
            return "No Data in file"

        # Add column POS_CUST, YEARMONTH, RUN_ID, CRTD_DTTM, file_name
        POS_CUST="SUPER INDO"
        yearmonth=file_name.split("_")[2].split(".")[0]

        dataframe = dataframe.with_column("POS_CUST",lit(POS_CUST))
        dataframe = dataframe.with_column("YEARMONTH",lit(yearmonth))
        dataframe = dataframe.with_column("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")).cast("integer"))
        dataframe = dataframe.with_column("CRTD_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        dataframe = dataframe.with_column("FILENAME",lit(file_name))
        

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


CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.IDNSDL_RAW.POS_SUPERINDO_STOCK_PREPROCESSING("PARAM" ARRAY)
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

        #Param=[''Stock_Superindo_202112.csv'',''IDNSDL_RAW.DEV_LOAD_STAGE_ADLS_IDN'',''dev/POS/transaction/Superindo/stock/'',''SDL_ID_POS_SUPERINDO_STOCK'']

        # Extracting parameters from the input
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        sch_name        = stage_name.split(''.'')[0]
        target_table    = sch_name+"."+Param[3]


        # Define the schema for the DataFrame
        df_schema=StructType([
            StructField("COMPANY",StringType()),
            StructField("CODE",StringType()),
            StructField("DESCRIPTION",StringType()),
            StructField("TAG1",StringType()),
            StructField("CNV",StringType()),
            StructField("EQ",StringType()),
            StructField("STOCK_DC_REGULAR_QTY",StringType()),
            StructField("STOCK_DC_REGULAR_DSI",StringType()),
            StructField("STOCK_ALL_STORES_QTY",StringType()),
            StructField("STOCK_ALL_STORES_DSI",StringType()),
            StructField("STOK_ALL",StringType()),
            StructField("DAY_SALES",StringType()),
            StructField("DSI",StringType())
            ])


        # Read the CSV file into a DataFrame
    
        dataframe = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "\\u0001")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .option("skip_blank_lines", True)\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        #---------------------------Transformation logic ------------------------------#
        

        # Handle null values or empty rows
        dataframe=dataframe.na.drop("all")

        # Check for empty Dataframe
        if dataframe.count()==0:
            return "No Data in file"

        # Add column POS_CUST, YEARMONTH, RUN_ID, CRTD_DTTM, file_name
        POS_CUST="SUPER INDO"
        yearmonth=file_name.split("_")[2].split(".")[0]

        dataframe = dataframe.with_column("POS_CUST",lit(POS_CUST))
        dataframe = dataframe.with_column("YEARMONTH",lit(yearmonth))
        dataframe = dataframe.with_column("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")).cast("integer"))
        dataframe = dataframe.with_column("CRTD_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        dataframe = dataframe.with_column("FILENAME",lit(file_name))

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


CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.IDNSDL_RAW.POS_WATSON_SELLOUT_PREPROCESSING("PARAM" ARRAY)
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

        #Param=[''Sellout_Watsons_202111.csv'',''IDNSDL_RAW.DEV_LOAD_STAGE_ADLS_IDN'',''dev/POS/transaction/Watson/sellout/'',''SDL_ID_POS_WATSON_SELLOUT'']

        # Extracting parameters from the input
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        sch_name        = stage_name.split(''.'')[0]
        target_table    = sch_name+"."+Param[3]


        # Define the schema for the DataFrame
        df_schema=StructType([
            StructField("LOC_IDNT",StringType()),
            StructField("SALE_DATE",StringType()),
            StructField("STR_CODE",StringType()),
            StructField("STR_NAME",StringType()),
            StructField("STR_CLASS",StringType()),
            StructField("STR_FORMAT",StringType()),
            StructField("DIVISION",StringType()),
            StructField("DIV_DESC",StringType()),
            StructField("DEPT_IDNT",StringType()),
            StructField("DEPT_DESC",StringType()),
            StructField("SUP_CODE",StringType()),
            StructField("SUP_NAME",StringType()),
            StructField("PRDT_CODE",StringType()),
            StructField("PRDT_DESC",StringType()),
            StructField("BRAND",StringType()),
            StructField("SALE_QTY",StringType()),
            StructField("NET_SALE",StringType()),
            StructField("WEEK",StringType()),
            StructField("YEAR",StringType()),
            StructField("RANGE_DESC",StringType())
            ])


        # Read the CSV file into a DataFrame
    
        dataframe = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "\\u0001")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .option("skip_blank_lines", True)\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        #---------------------------Transformation logic ------------------------------#
        

        # Handle null values or empty rows
        dataframe=dataframe.na.drop("all")

        # Check for empty Dataframe
        if dataframe.count()==0:
            return "No Data in file"

        # Add column POS_CUST, YEARMONTH, RUN_ID, CRTD_DTTM, file_name
        POS_CUST="WATSON"
        yearmonth=file_name.split("_")[2].split(".")[0]

        dataframe = dataframe.with_column("POS_CUST",lit(POS_CUST))
        dataframe = dataframe.with_column("YEARMONTH",lit(yearmonth))
        dataframe = dataframe.with_column("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")).cast("integer"))
        dataframe = dataframe.with_column("CRTD_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        dataframe = dataframe.with_column("FILENAME",lit(file_name))

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

CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.IDNSDL_RAW.POS_WATSON_STOCK_PREPROCESSING("PARAM" ARRAY)
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

        #Param=[''Stock_Watson_202108.csv'',''IDNSDL_RAW.DEV_LOAD_STAGE_ADLS_IDN'',''dev/POS/transaction/Watson/stock/'',''SDL_ID_POS_WATSON_STOCK'']

        # Extracting parameters from the input
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        sch_name        = stage_name.split(''.'')[0]
        target_table    = sch_name+"."+Param[3]


        # Define the schema for the DataFrame
        df_schema=StructType([
            StructField("GRP",StringType()),
            StructField("GROUP_NAME",StringType()),
            StructField("DEPT",StringType()),
            StructField("DEPT_NAME",StringType()),
            StructField("ITEM",StringType()),
            StructField("ITEM_NAME",StringType()),
            StructField("PRODUCT_TYPE",StringType()),
            StructField("ITEM_BRAND",StringType()),
            StructField("SUPPLIER",StringType()),
            StructField("SUPPLIER_NAME",StringType()),
            StructField("SUP_CODE2",StringType()),
            StructField("PRODUCT_PLNMOD",StringType()),
            StructField("POG",StringType()),
            StructField("TOP500",StringType()),
            StructField("SOH_QTY",StringType()),
            StructField("AVG_SALES",StringType()),
            StructField("WEEK_COVER",StringType()),
            StructField("STORE",StringType()),
            StructField("values",StringType())
            ])


        # Read the CSV file into a DataFrame
    
        dataframe = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", ",")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .option("encoding","UTF-8")\\
            .option("skip_blank_lines", True)\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        #---------------------------Transformation logic ------------------------------#
        

        # Handle null values or empty rows
        dataframe=dataframe.na.drop("all")

        # Check for empty Dataframe
        if dataframe.count()==0:
            return "No Data in file"

        # Add column POS_CUST, YEARMONTH, RUN_ID, CRTD_DTTM, file_name
        POS_CUST="WATSON"
        yearmonth=file_name.split("_")[2].split(".")[0]

        dataframe = dataframe.with_column("POS_CUST",lit(POS_CUST))
        dataframe = dataframe.with_column("YEARMONTH",lit(yearmonth))
        dataframe = dataframe.with_column("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")).cast("integer"))
        dataframe = dataframe.with_column("CRTD_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        dataframe = dataframe.with_column("FILENAME",lit(file_name))

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


CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.IDNSDL_RAW.IVY_INVENTORY_PREPROCESSING("PARAM" ARRAY)
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
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField,DecimalType
import pandas as pd
import numpy as np
from datetime import datetime
import pytz

def main(session: snowpark.Session,Param): 

    try:

        #Param=[''Inventory_20240521_0130.csv'',''IDNSDL_RAW.DEV_LOAD_STAGE_ADLS_IDN'',''dev/IVY/transaction/Inventory/'',''SDL_DISTRIBUTOR_IVY_INVENTORY'']

        # Extracting parameters from the input
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        sch_name        = stage_name.split(''.'')[0]
        target_table    = sch_name+"."+Param[3]


        # Define the schema for the DataFrame
        df_schema=StructType([
            StructField("DISTRIBUTOR_CODE",StringType()),
            StructField("WAREHOUSE_CODE",StringType()),
            StructField("PRODUCT_CODE",StringType()),
            StructField("BATCH_CODE",StringType()),
            StructField("BATCH_EXPIRY_DATE",StringType()),
            StructField("UOM",StringType()),
            StructField("QTY",StringType())
            ])


        # Read the CSV file into a DataFrame
    
        dataframe = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "|")\\
            .option("encoding","UTF-8")\\
            .option("skip_blank_lines", True)\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

         #---------------------------Transformation logic ------------------------------#
        

        # Handle null values or empty rows
        dataframe=dataframe.na.drop("all")

        # Check for empty Dataframe
        if dataframe.count()==0:
            return "No Data in file"

        # Add CDL_DTTM, RUNID and filename
        
        
        dataframe = dataframe.with_column("CDL_DTTM",lit(datetime.now(pytz.timezone("GMT")).strftime("%Y-%m-%d %H:%M:%S")))
        dataframe = dataframe.with_column("RUN_ID",lit(datetime.now(pytz.timezone("GMT")).strftime("%Y%m%d%H%M%S")).cast("integer"))
        dataframe = dataframe.with_column("SOURCE_FILE_NAME",lit(file_name))

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

CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.IDNSDL_RAW.IVY_INVOICE_PREPROCESSING("PARAM" ARRAY)
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
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField,DecimalType
import pandas as pd
import numpy as np
from datetime import datetime
import pytz
import re

def main(session: snowpark.Session,Param): 

    try:

        #Param=[''Invoice_20240523_0130.csv'',''IDNSDL_RAW.DEV_LOAD_STAGE_ADLS_IDN'',''dev/IVY/transaction/Invoice/'',''SDL_DISTRIBUTOR_IVY_INVOICE'']

        # Extracting parameters from the input
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        sch_name        = stage_name.split(''.'')[0]
        target_table    = sch_name+"."+Param[3]


        # Define the schema for the DataFrame
        df_schema=StructType([
            StructField("DISTRIBUTOR_CODE",StringType()),
            StructField("USER_CODE",StringType()),
            StructField("RETAILER_CODE",StringType()),
            StructField("INVOICE_DATE",StringType()),
            StructField("ORDER_ID",StringType()),
            StructField("INVOICE_NO",StringType()),
            StructField("PRODUCT_CODE",StringType()),
            StructField("UOM",StringType()),
            StructField("UOM_COUNT",StringType()),
            StructField("QTY",StringType()),
            StructField("PIECE_PRICE",StringType()),
            StructField("LINE_VALUE",StringType()),
            StructField("INVOICE_AMOUNT",StringType()),
            StructField("LINES_PER_CALL",StringType()),
            StructField("SCHEME_CODE",StringType()),
            StructField("SCHEME_DESCRIPTION",StringType()),
            StructField("SCHEME_DISCOUNT",StringType()),
            StructField("SCHEME_PRECENTAGE",StringType()),
            StructField("BILLDISCOUNT",StringType()),
            StructField("BILLDISC_PERCENTAGE",StringType()),
            StructField("PO_NUMBER",StringType()),
            StructField("PAYMENT_TYPE",StringType()),
            StructField("EXP_DELIVERY_DATE",StringType()),
            StructField("INVOICE_ADDRESS",StringType()),
            StructField("SHIPPING_ADDRESS",StringType()),
            StructField("INVOICE_STATUS",StringType()),
            StructField("EFAKTUR_NO",StringType()),
            StructField("TAX_VALUE",StringType()),
            StructField("BATCH_NO",StringType())
            ])


        # Read the CSV file into a DataFrame
    
        dataframe = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "|")\\
            .option("encoding","UTF-8")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .option("skip_blank_lines", True)\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        df_pandas=dataframe.to_pandas()

        #df_pandas[''ORDER_ID''] = df_pandas[''ORDER_ID''].apply(lambda x: "" if x == np.nan else x)
        df_pandas[''ORDER_ID''] = df_pandas[''ORDER_ID''].fillna("")
        df_pandas[''INVOICE_ADDRESS''] = df_pandas[''INVOICE_ADDRESS''].fillna("")
        df_pandas[''SHIPPING_ADDRESS''] = df_pandas[''SHIPPING_ADDRESS''].fillna("")
        df_pandas[''INVOICE_STATUS''] = df_pandas[''INVOICE_STATUS''].fillna("")

        df_pandas[''EXP_DELIVERY_DATE''] = df_pandas[''EXP_DELIVERY_DATE''].apply(lambda x: pd.to_datetime(x, format=''%Y-%m-%d'', errors=''coerce'') 
              if re.match(r''\\d{1,4}-\\d{1,2}-\\d{1,2}'', str(x)) 
              else pd.NaT)

        dataframe=session.create_dataframe(df_pandas)

        dataframe = dataframe.with_column("CDL_DTTM",lit(datetime.now(pytz.timezone("GMT")).strftime("%Y-%m-%d %H:%M:%S")))
        dataframe = dataframe.with_column("RUN_ID",lit(datetime.now(pytz.timezone("GMT")).strftime("%Y%m%d%H%M%S")).cast("integer"))
        dataframe = dataframe.with_column("SOURCE_FILE_NAME",lit(file_name))

        # Creating Final Dataframe
        final_df = dataframe.alias("final_df")

        # Load Data to the target table
        
        final_df.write.mode("append").saveAsTable(target_table)
        

        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")

         # write to success folder
    
        file_name=file_name.split(".")[0]+''_''+datetime.now().strftime("%Y%m%d%H%M%S")
        final_df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True,file_format_name=''FILE_FORMAT_COMMA'')
        
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


CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.IDNSDL_RAW.IVY_MERCHANDISING_PREPROCESSING("PARAM" ARRAY)
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
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField,DecimalType
import pandas as pd
import numpy as np
from datetime import datetime
import pytz

def main(session: snowpark.Session,Param): 

    try:

        #Param=[''Merchandising_20240522_0130.csv'',''IDNSDL_RAW.DEV_LOAD_STAGE_ADLS_IDN'',''dev/IVY/transaction/Merchandising/'',''SDL_DISTRIBUTOR_IVY_MERCHANDISING'']

        # Extracting parameters from the input
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        sch_name        = stage_name.split(''.'')[0]
        target_table    = sch_name+"."+Param[3]


        # Define the schema for the DataFrame
        df_schema=StructType([
            StructField("DISTRIBUTOR_CODE",StringType()),
            StructField("DISTRIBUTOR_NAME",StringType()),
            StructField("SALES_REPCODE",StringType()),
            StructField("SALES_REPNAME",StringType()),
            StructField("CHANNEL_NAME",StringType()),
            StructField("SUB_CHANNEL_NAME",StringType()),
            StructField("RETAILER_CODE",StringType()),
            StructField("RETAILER_NAME",StringType()),
            StructField("MONTH",StringType()),
            StructField("SURVEYDATE",StringType()),
            StructField("AQ_NAME",StringType()),
            StructField("SRD_ANSWER",StringType()),
            StructField("LINK",StringType())
            ])


        # Read the CSV file into a DataFrame
    
        dataframe = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "|")\\
            .option("encoding","UTF-8")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .option("skip_blank_lines", True)\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        df_pandas=dataframe.to_pandas()

        # Remove spaces in the values using strip

        #df_pandas["DISTRIBUTOR_CODE"] = df_pandas["DISTRIBUTOR_CODE"].apply(lambda x: x.strip()[:6] if len(x.strip()) >= 7 else x.strip())
        df_pandas[''DISTRIBUTOR_CODE''] = df_pandas[''DISTRIBUTOR_CODE''].str.strip()
        df_pandas[''DISTRIBUTOR_CODE''] = np.where(df_pandas[''DISTRIBUTOR_CODE''].str.len() >= 7, df_pandas[''DISTRIBUTOR_CODE''].str[0:6], df_pandas[''DISTRIBUTOR_CODE''])
        df_pandas[''DISTRIBUTOR_NAME''] = df_pandas[''DISTRIBUTOR_NAME''].str.strip()
        df_pandas[''SALES_REPCODE''] = df_pandas[''SALES_REPCODE''].str.strip()
        df_pandas[''SALES_REPNAME''] = df_pandas[''SALES_REPNAME''].str.strip()
        df_pandas[''CHANNEL_NAME''] = df_pandas[''CHANNEL_NAME''].str.strip()
        df_pandas[''SUB_CHANNEL_NAME''] = df_pandas[''SUB_CHANNEL_NAME''].str.strip()
        df_pandas[''RETAILER_CODE''] = np.where(df_pandas[''RETAILER_CODE''].str.strip().str.upper().str.endswith(''\\\\D''), df_pandas[''RETAILER_CODE''].str.split(''_'').str[0], df_pandas[''RETAILER_CODE''])
        df_pandas[''RETAILER_NAME''] = df_pandas[''RETAILER_NAME''].str.strip().str.replace("''''","")
        df_pandas[''MONTH''] = df_pandas[''MONTH''].str.strip()
        df_pandas[''SURVEYDATE''] = df_pandas[''SURVEYDATE''].str.strip()
        df_pandas[''AQ_NAME''] = df_pandas[''AQ_NAME''].str.strip()
        df_pandas[''SRD_ANSWER''] = df_pandas[''SRD_ANSWER''].str.strip()
        df_pandas[''LINK''] = df_pandas[''LINK''].str.strip()

        # Apply groupby / row number logic to filter out duplicates based on specific columns

        df_pandas[''row_number''] = df_pandas.groupby([''DISTRIBUTOR_CODE'', ''SALES_REPCODE'', ''RETAILER_CODE'', ''SURVEYDATE'', ''AQ_NAME'', df_pandas[''LINK''].fillna(''NA'')])[''SRD_ANSWER''].rank(method=''first'', ascending=False)
        print(df_pandas[''row_number''].value_counts())
        result_df = df_pandas[df_pandas[''row_number''] == 1].drop(columns=[''row_number''])
        

        #df_pandas.drop_duplicates(subset=["DISTRIBUTOR_CODE","SALES_REPCODE","RETAILER_CODE","SURVEYDATE","AQ_NAME","LINK"],keep=''first'')

        dataframe=session.create_dataframe(result_df)

        # Add CDL_DTTM, RUNID and filename
        
        
        dataframe = dataframe.with_column("CDL_DTTM",lit(datetime.now(pytz.timezone("GMT")).strftime("%Y-%m-%d %H:%M:%S")))
        dataframe = dataframe.with_column("RUN_ID",lit(datetime.now(pytz.timezone("GMT")).strftime("%Y%m%d%H%M%S")).cast("integer"))
        dataframe = dataframe.with_column("SOURCE_FILE_NAME",lit(file_name))

        # Creating Final Dataframe
        final_df = dataframe.alias("final_df")


        # Load Data to the target table
        
        final_df.write.mode("append").saveAsTable(target_table)


        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")

         # write to success folder
    
        file_name=file_name.split(".")[0]+''_''+datetime.now().strftime("%Y%m%d%H%M%S")
        final_df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True,file_format_name=''FILE_FORMAT_COMMA'')
        
        return "Success"



    except KeyError as key_error:
        # Handle KeyError (missing columns) here
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message
        
    except Exception as e:
        # Handle exceptions here
        error_message = f"Error: {str(e)}"
        return error_message';


CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.IDNSDL_RAW.IVY_ORDERS_PREPROCESSING("PARAM" ARRAY)
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
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField,DecimalType
import pandas as pd
import numpy as np
from datetime import datetime
import pytz

def main(session: snowpark.Session,Param): 

    try:

        #Param=[''OrderDetail_20240523_0130.csv'',''IDNSDL_RAW.DEV_LOAD_STAGE_ADLS_IDN'',''dev/IVY/transaction/Order/'',''SDL_DISTRIBUTOR_IVY_ORDER'']

        # Extracting parameters from the input
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        sch_name        = stage_name.split(''.'')[0]
        target_table    = sch_name+"."+Param[3]


        # Define the schema for the DataFrame
        df_schema=StructType([
            StructField("DISTRIBUTOR_CODE",StringType()),
            StructField("USER_CODE",StringType()),
            StructField("RETAILER_CODE",StringType()),
            StructField("ROUTE_CODE",StringType()),
            StructField("ORDER_DATE",StringType()),
            StructField("ORDER_ID",StringType()),
            StructField("PRODUCT_CODE",StringType()),
            StructField("UOM",StringType()),
            StructField("UOM_COUNT",StringType()),
            StructField("QTY",StringType()),
            StructField("LINE_VALUE",StringType()),
            StructField("PIECE_PRICE",StringType()),
            StructField("ORDER_VALUE",StringType()),
            StructField("LINES_PER_CALL",StringType()),
            StructField("SCHEME_CODE",StringType()),
            StructField("SCHEME_DESCRIPTION",StringType()),
            StructField("SCHEME_DISCOUNT",StringType()),
            StructField("SCHEME_PRECENTAGE",StringType()),
            StructField("BILLDISCOUNT",StringType()),
            StructField("BILLDISC_PERCENTAGE",StringType()),
            StructField("PO_NUMBER",StringType()),
            StructField("PAYMENT_TYPE",StringType()),
            StructField("DELIVERY_DATE",StringType()),
            StructField("INVOICE_ADDRESS",StringType()),
            StructField("SHIPPING_ADDRESS",StringType()),
            StructField("ORDER_STATUS",StringType()),
            StructField("ORDER_LATITUDE",StringType()),
            StructField("ORDER_LONGITUDE",StringType())
            ])


        # Read the CSV file into a DataFrame
    
        dataframe = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "|")\\
            .option("encoding","UTF-8")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .option("skip_blank_lines", True)\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        df_pandas=dataframe.to_pandas()

        df_pandas[''QTY''] = df_pandas[''QTY''].apply(lambda x: np.nan if x == '''' else x)

        dataframe=session.create_dataframe(df_pandas)

        dataframe = dataframe.with_column("CDL_DTTM",lit(datetime.now(pytz.timezone("GMT")).strftime("%Y-%m-%d %H:%M:%S")))
        dataframe = dataframe.with_column("RUN_ID",lit(datetime.now(pytz.timezone("GMT")).strftime("%Y%m%d%H%M%S")).cast("integer"))
        dataframe = dataframe.with_column("SOURCE_FILE_NAME",lit(file_name))

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
        return error_message
        ';

CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.IDNSDL_RAW.IVY_OUTLETMASTER_PREPROCESSING("PARAM" ARRAY)
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
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField,DecimalType
import pandas as pd
import numpy as np
from datetime import datetime
import pytz

def main(session: snowpark.Session,Param): 

    try:

        #Param=[''OutletMaster_20240518_0130.csv'',''IDNSDL_RAW.DEV_LOAD_STAGE_ADLS_IDN'',''dev/IVY/transaction/OutletMaster/'',''SDL_DISTRIBUTOR_IVY_OUTLET_MASTER'']

        # Extracting parameters from the input
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        sch_name        = stage_name.split(''.'')[0]
        target_table    = sch_name+"."+Param[3]


        # Define the schema for the DataFrame
        df_schema=StructType([
            StructField("DISTRIBUTORCODE",StringType()),
            StructField("USERCODE",StringType()),
            StructField("LOCATIONCODE",StringType()),
            StructField("OUTLETCODE",StringType()),
            StructField("OUTLETNAME",StringType()),
            StructField("OUTLETADDRESS",StringType()),
            StructField("PINCODE",StringType()),
            StructField("CHANNELCODE",StringType()),
            StructField("CHANNELNAME",StringType()),
            StructField("SUBCHANNELCODE",StringType()),
            StructField("SUBCHANNELNAME",StringType()),
            StructField("TIERINGCODE",StringType()),
            StructField("TIERINGNAME",StringType()),
            StructField("CLASSCODE",StringType()),
            StructField("ROUTECODE",StringType()),
            StructField("VISIT_FREQUENCY",StringType()),
            StructField("VISITDAY",StringType()),
            StructField("JNJ_ID",StringType()),
            StructField("CONTACTPERSON",StringType()),
            StructField("CREDIT_LIMIT",StringType()),
            StructField("INVOICE_LIMIT",StringType()),
            StructField("CREDIT_PERIOD",StringType()),
            StructField("TIN",StringType()),
            StructField("IS_DIAMOND_STORE",StringType()),
            StructField("ID_NO",StringType()),
            StructField("MASTER_CODE",StringType()),
            StructField("STORE_CLUSTER",StringType()),
            StructField("LATTITUDE",StringType()),
            StructField("LONGITUDE",StringType())
            ])


        # Read the CSV file into a DataFrame
    
        dataframe = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "|")\\
            .option("encoding","UTF-8")\\
            .option("skip_blank_lines", True)\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        #---------------------------Transformation logic ------------------------------#
        

        # Handle null values or empty rows
        dataframe=dataframe.na.drop("all")

        # Check for empty Dataframe
        if dataframe.count()==0:
            return "No Data in file"

        df_pandas=dataframe.to_pandas()

        columns_to_check = ["CREDIT_LIMIT", "INVOICE_LIMIT", "CREDIT_PERIOD"]
        df_pandas[columns_to_check] = df_pandas[columns_to_check].apply(lambda x: x.str.strip()).replace('''', np.nan)


        dataframe=session.create_dataframe(df_pandas)

        df=dataframe.select("DISTRIBUTORCODE","USERCODE","LOCATIONCODE","OUTLETCODE","OUTLETNAME","OUTLETADDRESS","PINCODE","CHANNELCODE","CHANNELNAME","SUBCHANNELCODE",\\
                           "SUBCHANNELNAME","TIERINGCODE","TIERINGNAME","CLASSCODE","ROUTECODE","VISIT_FREQUENCY","VISITDAY","JNJ_ID","CONTACTPERSON","CREDIT_LIMIT",\\
                           "INVOICE_LIMIT","CREDIT_PERIOD","TIN","IS_DIAMOND_STORE","ID_NO","MASTER_CODE","STORE_CLUSTER","LATTITUDE","LONGITUDE")

        # Add CDL_DTTM, RUNID and filename
        
        
        df = df.with_column("CDL_DTTM",lit(datetime.now(pytz.timezone("GMT")).strftime("%Y-%m-%d %H:%M:%S")))
        df = df.with_column("RUN_ID",lit(datetime.now(pytz.timezone("GMT")).strftime("%Y%m%d%H%M%S")).cast("integer"))
        df = df.with_column("SOURCE_FILE_NAME",lit(file_name))

        # Creating Final Dataframe
        final_df = df.alias("final_df")
        
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


CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.IDNSDL_RAW.IVY_USERMASTER_PREPROCESSING("PARAM" ARRAY)
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

        #Param=[''UserMaster_20240423_0130.csv'',''IDNSDL_RAW.DEV_LOAD_STAGE_ADLS_IDN'',''dev/IVY/transaction/UserMaster/'',''SDL_DISTRIBUTOR_IVY_USER_MASTER'']

        # Extracting parameters from the input
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        sch_name        = stage_name.split(''.'')[0]
        target_table    = sch_name+"."+Param[3]


        # Define the schema for the DataFrame
        df_schema=StructType([
            StructField("MD_LOCATION",StringType()),
            StructField("MD_CODE",StringType()),
            StructField("MD_NAME",StringType()),
            StructField("SD_LOCATION",StringType()),
            StructField("SD_CODE",StringType()),
            StructField("SD_NAME",StringType()),
            StructField("RBDM_LOCATION",StringType()),
            StructField("RBDM_CODE",StringType()),
            StructField("RBDM_NAME",StringType()),
            StructField("BDM_LOCATION",StringType()),
            StructField("BDM_CODE",StringType()),
            StructField("BDM_NAME",StringType()),
            StructField("BDR_LOCATION",StringType()),
            StructField("BDR_CODE",StringType()),
            StructField("BDR_NAME",StringType()),
            StructField("DIS_LOCATION",StringType()),
            StructField("DIS_CODE",StringType()),
            StructField("DIS_NAME",StringType()),
            StructField("RSM_CODE",StringType()),
            StructField("RSM_NAME",StringType()),
            StructField("SUP_CODE",StringType()),
            StructField("SUP_NAME",StringType()),
            StructField("SR_CODE",StringType()),
            StructField("SR_NAME",StringType())
            ])


        # Read the CSV file into a DataFrame
    
        dataframe = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "|")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .option("encoding","UTF-8")\\
            .option("skip_blank_lines", True)\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        #---------------------------Transformation logic ------------------------------#
        

        # Handle null values or empty rows
        dataframe=dataframe.na.drop("all")

        # Check for empty Dataframe
        if dataframe.count()==0:
            return "No Data in file"

        # Replace null values with an empty string for multiple columns
        columns_to_fill = ["DIS_LOCATION", "DIS_CODE", "DIS_NAME","RSM_CODE","RSM_NAME","SUP_CODE","SUP_NAME","SR_CODE","SR_NAME"]
        dataframe = dataframe.na.fill('''', subset=columns_to_fill)

        # Add CDL_DTTM, RUNID and filename
        
        
        dataframe = dataframe.with_column("CDL_DTTM",lit(datetime.now(pytz.timezone("GMT")).strftime("%Y-%m-%d %H:%M:%S")))
        dataframe = dataframe.with_column("RUN_ID",lit(datetime.now(pytz.timezone("GMT")).strftime("%Y%m%d%H%M%S")).cast("integer"))
        dataframe = dataframe.with_column("SOURCE_FILE_NAME",lit(file_name))

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


CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.IDNSDL_RAW.ID_POS_MATAHARI_OTC_STOCK_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS 'from snowflake.snowpark.functions import col, lit, date_format, current_timestamp, to_date, year, month, concat, format_number, regexp_replace,to_timestamp,when,trim,upper, substring
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DecimalType
import snowflake.snowpark
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz
import pandas as pd

def main(session: snowpark.Session,Param):
    # Param=["Stock_OTC_Matahari_202111.csv","IDNSDL_RAW.DEV_LOAD_STAGE_ADLS_IDN","dev/POS/transaction/matahari/","SDL_ID_POS_MATAHARI_OTC_STOCK"]
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        
        df_schema = StructType([
                # StructField("mnth_id",StringType(), True),
                StructField("type",StringType(), True),
                StructField("loc",StringType(), True),
                StructField("store_name",StringType(), True),
                StructField("item",StringType(), True),
                StructField("item_desc",StringType(), True),
                StructField("soh",StringType(), True),
                # StructField("pos_cust",StringType(), True),
                # StructField("yearmonth",StringType(), True),
                # StructField("run_id",StringType(), True),
                # StructField("crtd_dttm",StringType(), True),
                # StructField("filename",StringType(), True)
                
            ])
        
        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "\\u0001")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        
        
        df=df.na.drop("all")
        if df.count()==0 :
            return "No Data in file"

        
        # df = df.with_column("cdl_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        file_name1=file_name.split(".")[0]+"_"+ datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")+".csv"
        df = df.with_column("filename", lit(file_name))
        df = df.with_column("crtd_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        # df = df.with_column("mnth_id", file_name.split("_")[1])
        df = df.with_column("run_id", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        df = df.with_column("pos_cust", lit(file_name.split("_")[2]))
        df = df.with_column("yearmonth", lit(file_name.split("_")[3].split(".")[0]))
        
        # date=file_name.split("_")[3].split(".")[0]
        # year=date[0:4]
        # mnth_no=date[4:6]
        # inv_week=date[6:]
        # df = df.withColumn("year", lit(year))
        # df = df.with_column("mnth_no", lit(mnth_no))
        # df = df.with_column("inv_week", lit(inv_week))  
        
        snowdf= df.select(
                    # "mnth_id",
                    "type",
                    "loc",
                    "store_name",
                    "item",
                    "item_desc",
                    "soh",
                    "pos_cust",
                    "yearmonth",
                    "run_id",
                    "crtd_dttm",
                    "filename"                 
                    )
        file_name=file_name.split(".")[0]+"_"+datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")

        # df.show()
        
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


CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.IDNSDL_RAW.ID_POS_MATAHARI_JB_STOCK_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS 'from snowflake.snowpark.functions import col, lit, date_format, current_timestamp, to_date, year, month, concat, format_number, regexp_replace,to_timestamp,when,trim,upper, substring
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DecimalType
import snowflake.snowpark
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz
import pandas as pd

def main(session: snowpark.Session,Param):
    # Param=["Stock_JB_Matahari_202111.csv","IDNSDL_RAW.DEV_LOAD_STAGE_ADLS_IDN","dev/POS/transaction/matahari/","SDL_ID_POS_MATAHARI_JB_STOCK"]
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        
        df_schema = StructType([
                # StructField("mnth_id",StringType(), True),
                StructField("sku",StringType(), True),
                StructField("sku_desc",StringType(), True),
                StructField("qty",StringType(), True),
                StructField("retail_values",StringType(), True),
                # StructField("pos_cust",StringType(), True),
                # StructField("yearmonth",StringType(), True),
                # StructField("run_id",StringType(), True),
                # StructField("crtd_dttm",StringType(), True),
                # StructField("filename",StringType(), True)
                
            ])
        
        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "\\u0001")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        
        
        df=df.na.drop("all")
        if df.count()==0 :
            return "No Data in file"

        
        # df = df.with_column("cdl_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        file_name1=file_name.split(".")[0]+"_"+ datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")+".csv"
        df = df.with_column("filename", lit(file_name))
        df = df.with_column("crtd_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        # df = df.with_column("mnth_id", file_name.split("_")[1])
        df = df.with_column("run_id", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        df = df.with_column("pos_cust", lit(file_name.split("_")[2]))
        df = df.with_column("yearmonth", lit(file_name.split("_")[3].split(".")[0]))
        
        # date=file_name.split("_")[3].split(".")[0]
        # year=date[0:4]
        # mnth_no=date[4:6]
        # inv_week=date[6:]
        # df = df.withColumn("year", lit(year))
        # df = df.with_column("mnth_no", lit(mnth_no))
        # df = df.with_column("inv_week", lit(inv_week))  
        
        snowdf= df.select(
                    # "mnth_id",
                    "sku",
                    "sku_desc",
                    "qty",
                    "retail_values",
                    "pos_cust",
                    "yearmonth",
                    "run_id",
                    "crtd_dttm",
                    "filename"                 
                    )
        file_name=file_name.split(".")[0]+"_"+datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")

        # df.show()
        
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


CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.IDNSDL_RAW.ID_POS_MATAHARI_BEAUTY_STOCK_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS 'from snowflake.snowpark.functions import col, lit, date_format, current_timestamp, to_date, year, month, concat, format_number, regexp_replace,to_timestamp,when,trim,upper, substring
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DecimalType
import snowflake.snowpark
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz
import pandas as pd

def main(session: snowpark.Session,Param):
    # Param=["Stock_Beauty_Matahari_202111.csv","IDNSDL_RAW.DEV_LOAD_STAGE_ADLS_IDN","dev/POS/transaction/matahari/","SDL_ID_POS_MATAHARI_BEAUTY_STOCK"]
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        
        df_schema = StructType([
                # StructField("mnth_id",StringType(), True),
                StructField("month",StringType(), True),
                StructField("sku",StringType(), True),
                StructField("sku_desc",StringType(), True),
                StructField("year_qty",StringType(), True),
                StructField("retail_values",StringType(), True),
                # StructField("pos_cust",StringType(), True),
                # StructField("yearmonth",StringType(), True),
                # StructField("run_id",StringType(), True),
                # StructField("crtd_dttm",StringType(), True),
                # StructField("filename",StringType(), True)

                
            ])
        
        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "\\u0001")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        
        
        df=df.na.drop("all")
        if df.count()==0 :
            return "No Data in file"

        
        # df = df.with_column("cdl_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        file_name1=file_name.split(".")[0]+"_"+ datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")+".csv"
        df = df.with_column("filename", lit(file_name))
        df = df.with_column("crtd_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        # df = df.with_column("mnth_id", file_name.split("_")[1])
        df = df.with_column("run_id", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        df = df.with_column("pos_cust", lit(file_name.split("_")[2]))
        df = df.with_column("yearmonth", lit(file_name.split("_")[3].split(".")[0]))
        
        # date=file_name.split("_")[3].split(".")[0]
        # year=date[0:4]
        # mnth_no=date[4:6]
        # inv_week=date[6:]
        # df = df.withColumn("year", lit(year))
        # df = df.with_column("mnth_no", lit(mnth_no))
        # df = df.with_column("inv_week", lit(inv_week))  
        
        snowdf= df.select(
                    # "mnth_id",
                    "month",
                    "sku",
                    "sku_desc",
                    "year_qty",
                    "retail_values",
                    "pos_cust",
                    "yearmonth",
                    "run_id",
                    "crtd_dttm",
                    "filename"                 
                    )
        file_name=file_name.split(".")[0]+"_"+datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")

        # df.show()
        
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


CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.IDNSDL_RAW.ID_POS_IGR_STOCK_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS 'from snowflake.snowpark.functions import col, lit, date_format, current_timestamp, to_date, year, month, concat, format_number, regexp_replace,to_timestamp,when,trim,upper, substring
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DecimalType
import snowflake.snowpark
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz
import pandas as pd

def main(session: snowpark.Session,Param):
    # Param=["Stock_Indogrosir_202404.csv","IDNSDL_RAW.DEV_LOAD_STAGE_ADLS_IDN","dev/POS/transaction/igr/","SDL_ID_POS_IGR_STOCK"]
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        
        df_schema = StructType([
                # StructField("mnth_id",StringType(), True),
                StructField("item",StringType(), True),
                StructField("branch",StringType(), True),
                StructField("qty",StringType(), True),
                StructField("unit",StringType(), True),
                # StructField("pos_cust",StringType(), True),
                # StructField("yearmonth",StringType(), True),
                # StructField("run_id",StringType(), True),
                # StructField("crtd_dttm",StringType(), True),
                # StructField("filename",StringType(), True)
                
            ])
        
        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "\\u0001")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        
        
        df=df.na.drop("all")
        if df.count()==0 :
            return "No Data in file"

        
        # df = df.with_column("cdl_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        file_name1=file_name.split(".")[0]+"_"+ datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")+".csv"
        df = df.with_column("filename", lit(file_name))
        df = df.with_column("crtd_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        # df = df.with_column("mnth_id", file_name.split("_")[1])
        df = df.with_column("run_id", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        df = df.with_column("pos_cust", lit(file_name.split("_")[1]))
        df = df.with_column("yearmonth", lit(file_name.split("_")[2].split(".")[0]))
        
        # date=file_name.split("_")[3].split(".")[0]
        # year=date[0:4]
        # mnth_no=date[4:6]
        # inv_week=date[6:]
        # df = df.withColumn("year", lit(year))
        # df = df.with_column("mnth_no", lit(mnth_no))
        # df = df.with_column("inv_week", lit(inv_week))  
        
        snowdf= df.select(
                    # "mnth_id",
                    "item",
                    "branch",
                    "qty",
                    "unit",
                    "pos_cust",
                    "yearmonth",
                    "run_id",
                    "crtd_dttm",
                    "filename"                 
                    )
        file_name=file_name.split(".")[0]+"_"+datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")

        # df.show()
        
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

CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.IDNSDL_RAW.ID_POS_IGR_SELLOUT_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS 'from snowflake.snowpark.functions import col, lit, date_format, current_timestamp, to_date, year, month, concat, format_number, regexp_replace,to_timestamp,when,trim,upper, substring
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DecimalType
import snowflake.snowpark
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz
import pandas as pd

def main(session: snowpark.Session,Param):
    # Param=["Sellout_Indogrosir_202403.csv","IDNSDL_RAW.DEV_LOAD_STAGE_ADLS_IDN","dev/POS/transaction/igr/","SDL_ID_POS_IGR_SELLOUT"]
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        
        df_schema = StructType([
                # StructField("mnth_id",StringType(), True),
                StructField("no",StringType(), True),
                StructField("description",StringType(), True),
                StructField("branch",StringType(), True),
                StructField("type",StringType(), True),
                StructField("values",StringType(), True),
                # StructField("pos_cust",StringType(), True),
                # StructField("yearmonth",StringType(), True),
                # StructField("run_id",StringType(), True),
                # StructField("crtd_dttm",StringType(), True),
                # StructField("filename",StringType(), True)                                
            ])
        
        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", ",")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        
        
        df=df.na.drop("all")
        if df.count()==0 :
            return "No Data in file"

        
        # df = df.with_column("cdl_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        file_name1=file_name.split(".")[0]+"_"+ datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")+".csv"
        df = df.with_column("filename", lit(file_name))
        df = df.with_column("crtd_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        # df = df.with_column("mnth_id", file_name.split("_")[1])
        df = df.with_column("run_id", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        df = df.with_column("pos_cust", lit(file_name.split("_")[1]))
        df = df.with_column("yearmonth", lit(file_name.split("_")[2].split(".")[0]))
        
        # date=file_name.split("_")[3].split(".")[0]
        # year=date[0:4]
        # mnth_no=date[4:6]
        # inv_week=date[6:]
        # df = df.withColumn("year", lit(year))
        # df = df.with_column("mnth_no", lit(mnth_no))
        # df = df.with_column("inv_week", lit(inv_week))  
        
        snowdf= df.select(
                    # "mnth_id",
                    "no",
                    "description",
                    "branch",
                    "type",
                    "values",
                    "pos_cust",
                    "yearmonth",
                    "run_id",
                    "crtd_dttm",
                    "filename"                   
                    )
        file_name=file_name.split(".")[0]+"_"+datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")

        # df.show()
        
        #move to success
        
        snowdf.write.mode("append").saveAsTable(stage_name.split(".")[0] + "." + target_table)
        snowdf.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)
        
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


CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.IDNSDL_RAW.ID_POS_IDM_STOCK_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS 'from snowflake.snowpark.functions import col, lit, date_format, current_timestamp, to_date, year, month, concat, format_number, regexp_replace,to_timestamp,when,trim,upper, substring
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DecimalType
import snowflake.snowpark
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz
import pandas as pd

def main(session: snowpark.Session,Param):
    # Param=["Stock_Indomaret_202404.csv","IDNSDL_RAW.DEV_LOAD_STAGE_ADLS_IDN","dev/POS/transaction/idm/","SDL_ID_POS_IDM_STOCK"]
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        
        df_schema = StructType([
                # StructField("mnth_id",StringType(), True),
                StructField("no",StringType(), True),
                StructField("item",StringType(), True),
                StructField("branch",StringType(), True),
                StructField("dc_qty",StringType(), True),
                StructField("store_qty",StringType(), True),
                StructField("units",StringType(), True),
                # StructField("pos_cust",StringType(), True),
                # StructField("yearmonth",StringType(), True),
                # StructField("run_id",StringType(), True),
                # StructField("crtd_dttm",StringType(), True),
                # StructField("filename",StringType(), True),
                
            ])
        
        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "\\u0001")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        
        
        df=df.na.drop("all")
        if df.count()==0 :
            return "No Data in file"

        
        # df = df.with_column("cdl_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        file_name1=file_name.split(".")[0]+"_"+ datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")+".csv"
        df = df.with_column("filename", lit(file_name))
        df = df.with_column("crtd_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        # df = df.with_column("mnth_id", file_name.split("_")[1])
        df = df.with_column("run_id", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        df = df.with_column("pos_cust", lit(file_name.split("_")[1]))
        df = df.with_column("yearmonth", lit(file_name.split("_")[2].split(".")[0]))
        
        # date=file_name.split("_")[3].split(".")[0]
        # year=date[0:4]
        # mnth_no=date[4:6]
        # inv_week=date[6:]
        # df = df.withColumn("year", lit(year))
        # df = df.with_column("mnth_no", lit(mnth_no))
        # df = df.with_column("inv_week", lit(inv_week))  
        
        snowdf= df.select(
                    # "mnth_id",
                    "no",
                    "item",
                    "branch",
                    "dc_qty",
                    "store_qty",
                    "units",
                    "pos_cust",
                    "yearmonth",
                    "run_id",
                    "crtd_dttm",
                    "filename"                   
                    )
        file_name=file_name.split(".")[0]+"_"+datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")

        # df.show()
        
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


CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.IDNSDL_RAW.ID_POS_IDM_SELLOUT_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS 'from snowflake.snowpark.functions import col, lit, date_format, current_timestamp, to_date, year, month, concat, format_number, regexp_replace,to_timestamp,when,trim,upper, substring
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DecimalType
import snowflake.snowpark
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz
import pandas as pd

def main(session: snowpark.Session,Param):
    # Param=["Sellout_Indomaret_202403.csv","IDNSDL_RAW.DEV_LOAD_STAGE_ADLS_IDN","dev/POS/transaction/idm/","SDL_ID_POS_IDM_SELLOUT"]
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        
        df_schema = StructType([
                # StructField("mnth_id",StringType(), True),
                StructField("item",StringType(), True),
                StructField("description",StringType(), True),
                StructField("plu",StringType(), True),
                StructField("branch",StringType(), True),
                StructField("type",StringType(), True),
                StructField("values",StringType(), True),
                StructField("pos_cust",StringType(), True),
                StructField("yearmonth",StringType(), True),
                StructField("run_id",StringType(), True),
                StructField("crtd_dttm",StringType(), True),
                StructField("filename",StringType(), True)                
            ])
        
        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", ",")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        
        
        df=df.na.drop("all")
        if df.count()==0 :
            return "No Data in file"

        
        # df = df.with_column("cdl_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        file_name1=file_name.split(".")[0]+"_"+ datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")+".csv"
        df = df.with_column("filename", lit(file_name))
        df = df.with_column("crtd_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        # df = df.with_column("mnth_id", file_name.split("_")[1])
        df = df.with_column("run_id", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        df = df.with_column("pos_cust", lit(file_name.split("_")[1]))
        df = df.with_column("yearmonth", lit(file_name.split("_")[2].split(".")[0]))
        
        # date=file_name.split("_")[3].split(".")[0]
        # year=date[0:4]
        # mnth_no=date[4:6]
        # inv_week=date[6:]
        # df = df.withColumn("year", lit(year))
        # df = df.with_column("mnth_no", lit(mnth_no))
        # df = df.with_column("inv_week", lit(inv_week))  
        
        snowdf= df.select(
                    # "mnth_id",
                    "item",
                    "description",
                    "plu",
                    "branch",
                    "type",
                    "values",
                    "pos_cust",
                    "yearmonth",
                    "run_id",
                    "crtd_dttm",
                    "filename"                   
                    )
        file_name=file_name.split(".")[0]+"_"+datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")

        df.show()
        
        #move to success
        snowdf.write.mode("append").saveAsTable(stage_name.split(".")[0] + "." + target_table)
        
        snowdf.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)
        
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

CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.IDNSDL_RAW.ID_POS_IDM_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS 'from snowflake.snowpark.functions import col, lit, date_format, current_timestamp, to_date, year, month, concat, format_number, regexp_replace,to_timestamp,when,trim,upper, substring
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DecimalType
import snowflake.snowpark
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz
import pandas as pd

def main(session: snowpark.Session,Param):
    # Param=["Sellout_Indomaret_202403.csv","IDNSDL_RAW.DEV_LOAD_STAGE_ADLS_IDN","dev/POS/transaction/idm/","SDL_ID_POS_IDM_SELLOUT"]
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        
        df_schema = StructType([
                # StructField("mnth_id",StringType(), True),
                StructField("item",StringType(), True),
                StructField("description",StringType(), True),
                StructField("plu",StringType(), True),
                StructField("branch",StringType(), True),
                StructField("type",StringType(), True),
                StructField("values",StringType(), True),
                StructField("pos_cust",StringType(), True),
                StructField("yearmonth",StringType(), True),
                StructField("run_id",StringType(), True),
                StructField("crtd_dttm",StringType(), True),
                StructField("filename",StringType(), True)                
            ])
        
        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", ",")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        
        
        df=df.na.drop("all")
        if df.count()==0 :
            return "No Data in file"

        
        # df = df.with_column("cdl_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        file_name1=file_name.split(".")[0]+"_"+ datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")+".csv"
        df = df.with_column("filename", lit(file_name))
        df = df.with_column("crtd_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        # df = df.with_column("mnth_id", file_name.split("_")[1])
        df = df.with_column("run_id", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        df = df.with_column("pos_cust", lit(file_name.split("_")[1]))
        df = df.with_column("yearmonth", lit(file_name.split("_")[2].split(".")[0]))
        
        # date=file_name.split("_")[3].split(".")[0]
        # year=date[0:4]
        # mnth_no=date[4:6]
        # inv_week=date[6:]
        # df = df.withColumn("year", lit(year))
        # df = df.with_column("mnth_no", lit(mnth_no))
        # df = df.with_column("inv_week", lit(inv_week))  
        
        snowdf= df.select(
                    # "mnth_id",
                    "item",
                    "description",
                    "plu",
                    "branch",
                    "type",
                    "values",
                    "pos_cust",
                    "yearmonth",
                    "run_id",
                    "crtd_dttm",
                    "filename"                   
                    )
        file_name=file_name.split(".")[0]+"_"+datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")

        df.show()
        
        #move to success
        snowdf.write.mode("append").saveAsTable(stage_name.split(".")[0] + "." + target_table)
        
        snowdf.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)
        
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


CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.IDNSDL_RAW.ID_POS_GUARDIAN_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS 'from snowflake.snowpark.functions import col, lit, date_format, current_timestamp, to_date, year, month, concat, format_number, regexp_replace,to_timestamp,when,trim,upper, substring
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DecimalType
import snowflake.snowpark
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz
import pandas as pd

def main(session: snowpark.Session,Param):
    Param=["Stock_Guardian_202001.csv","IDNSDL_RAW.DEV_LOAD_STAGE_ADLS_IDN","dev/POS/transaction/guardian/","SDL_ID_POS_GUARDIAN_STOCK"]
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        
        df_schema = StructType([
                # StructField("mnth_id",StringType(), True),
                StructField("article",StringType(), True),
                StructField("article_desc",StringType(), True),
                StructField("category",StringType(), True),
                StructField("soh_stores",StringType(), True),
                StructField("soh_dc",StringType(), True),
                StructField("pos_cust",StringType(), True),
                StructField("yearmonth",StringType(), True),
                
            ])
        
        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        
        
        df=df.na.drop("all")
        if df.count()==0 :
            return "No Data in file"

        
        df = df.with_column("cdl_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        file_name1=file_name.split(".")[0]+"_"+ datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")+".csv"
        df = df.with_column("filename", lit(file_name))
        df = df.with_column("crtd_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        # df = df.with_column("mnth_id", file_name.split("_")[1])
        df = df.with_column("run_id", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        df = df.with_column("pos_cust", lit("Guardian"))
        df = df.with_column("yearmonth", lit(file_name.split("_")[2].split(".")[0]))
        
        # date=file_name.split("_")[3].split(".")[0]
        # year=date[0:4]
        # mnth_no=date[4:6]
        # inv_week=date[6:]
        # df = df.withColumn("year", lit(year))
        # df = df.with_column("mnth_no", lit(mnth_no))
        # df = df.with_column("inv_week", lit(inv_week))  
        
        snowdf= df.select(
                    # "mnth_id",
                    "article",
                    "article_desc",
                    "category",
                    "soh_stores",
                    "soh_dc",
                    "pos_cust",
                    "yearmonth",
                    "run_id",
                    "crtd_dttm",
                    "filename"                    
                    )
        file_name=file_name.split(".")[0]+"_"+datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
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


CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.IDNSDL_RAW.ID_POS_DIAMOND_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS 'from snowflake.snowpark.functions import col, lit, date_format, current_timestamp, to_date, year, month, concat, format_number, regexp_replace,to_timestamp,when,trim,upper, substring
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DecimalType
import snowflake.snowpark
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz
import pandas as pd

def main(session: snowpark.Session,Param):
    # Param=["Sellout_Diamond_202105.csv","IDNSDL_RAW.DEV_LOAD_STAGE_ADLS_IDN","dev/POS/transaction/diamond/","SDL_ID_POS_DIAMOND_SELLOUT"]
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        
        df_schema = StructType([
                # StructField("mnth_id",StringType(), True),
                StructField("nama_barang",StringType(), True),
                StructField("qty",StringType(), True),
                StructField("sales",StringType(), True),
                # StructField("pos_cust",StringType(), True),
                # StructField("branch",StringType(), True),
                # StructField("yearmonth",StringType(), True),
                # StructField("run_id",StringType(), True),
                # StructField("crtd_dttm",StringType(), True),
                # StructField("filename",StringType(), True)               
            ])
        
        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "\\u0001")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        
        
        df=df.na.drop("all")
        if df.count()==0 :
            return "No Data in file"

        
        # df = df.with_column("cdl_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        file_name1=file_name.split(".")[0]+"_"+ datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")+".csv"
        df = df.with_column("filename", lit(file_name))
        df = df.with_column("crtd_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        # df = df.with_column("mnth_id", file_name.split("_")[1])
        df = df.with_column("run_id", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        df = df.with_column("pos_cust", lit(file_name.split("_")[1]))
        df = df.with_column("yearmonth", lit(file_name.split("_")[2].split(".")[0]))
        df = df.with_column("branch", lit("Palembang"))
        
        
        # date=file_name.split("_")[3].split(".")[0]
        # year=date[0:4]
        # mnth_no=date[4:6]
        # inv_week=date[6:]
        # df = df.withColumn("year", lit(year))
        # df = df.with_column("mnth_no", lit(mnth_no))
        # df = df.with_column("inv_week", lit(inv_week))  
        
        snowdf= df.select(
                    # "mnth_id",
                    "nama_barang",
                    "qty",
                    "sales",
                    "pos_cust",
                    "branch",
                    "yearmonth",
                    "run_id",
                    "crtd_dttm",
                    "filename"                 
                    )
        file_name=file_name.split(".")[0]+"_"+datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")

        # df.show()
        
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


CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.IDNSDL_RAW.ID_POS_DAILY_SAT_SELLOUT_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS 'from snowflake.snowpark.functions import col, lit, date_format, current_timestamp, to_date, year, month, concat, format_number, regexp_replace,to_timestamp,when,trim,upper, substring
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DecimalType
import snowflake.snowpark
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz
import pandas as pd

def main(session: snowpark.Session,Param):
    # Param=["Sellout_Alfamart_202405_20240509010011.csv","IDNSDL_RAW.DEV_LOAD_STAGE_ADLS_IDN","dev/POS/transaction/daily_sat_sellout/","SDL_ID_POS_DAILY_SAT_SELLOUT"]
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        
        df_schema = StructType([
                # StructField("mnth_id",StringType(), True),
                StructField("account",StringType(), True),
                StructField("kode_branch",StringType(), True),
                StructField("branch_name",StringType(), True),
                StructField("tgl",StringType(), True),
                StructField("plu",StringType(), True),
                StructField("descp",StringType(), True),
                StructField("type",StringType(), True),
                StructField("value",StringType(), True),
                # StructField("pos_cust",StringType(), True),
                # StructField("yearmonth",StringType(), True),
                # StructField("run_id",StringType(), True),
                # StructField("crtd_dttm",StringType(), True),
                # StructField("filename",StringType(), True)                
            ])
        
        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter",",")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
            # .option("field_delimiter", "\\u0001")\\

        
        
        df=df.na.drop("all")
        if df.count()==0 :
            return "No Data in file"

        
        # df = df.with_column("cdl_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        file_name1=file_name.split(".")[0]+"_"+ datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")+".csv"
        df = df.with_column("filename", lit(file_name))
        df = df.with_column("crtd_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        # df = df.with_column("mnth_id", file_name.split("_")[1])
        df = df.with_column("run_id", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        df = df.with_column("pos_cust", lit(file_name.split("_")[1]))
        df = df.with_column("yearmonth", lit(file_name.split("_")[2].split(".")[0]))
        
        # date=file_name.split("_")[3].split(".")[0]
        # year=date[0:4]
        # mnth_no=date[4:6]
        # inv_week=date[6:]
        # df = df.withColumn("year", lit(year))
        # df = df.with_column("mnth_no", lit(mnth_no))
        # df = df.with_column("inv_week", lit(inv_week))  
        
        snowdf= df.select(
                    # "mnth_id",
                    "account",
                    "kode_branch",
                    "branch_name",
                    "tgl",
                    "plu",
                    "descp",
                    "type",
                    "value",
                    "pos_cust",
                    "yearmonth",
                    "run_id",
                    "crtd_dttm",
                    "filename"                
                    )
        file_name=file_name.split(".")[0]+"_"+datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")

        # df.show()
        
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


CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.IDNSDL_RAW.ID_POS_DAILY_IDM_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS 'from snowflake.snowpark.functions import col, lit, date_format, current_timestamp, to_date, year, month, concat, format_number, regexp_replace,to_timestamp,when,trim,upper, substring
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DecimalType
import snowflake.snowpark
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz
import pandas as pd

def main(session: snowpark.Session,Param):
    # Param=["Sellout_Indomaret_202405_20240503010012.csv","IDNSDL_RAW.DEV_LOAD_STAGE_ADLS_IDN","dev/POS/transaction/daily_idm/","SDL_ID_POS_DAILY_IDM_SELLOUT"]
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        
        df_schema = StructType([
                # StructField("mnth_id",StringType(), True),
                StructField("account",StringType(), True),
                StructField("kode_branch",StringType(), True),
                StructField("branch_name",StringType(), True),
                StructField("tgl",StringType(), True),
                StructField("plu",StringType(), True),
                StructField("descp",StringType(), True),
                StructField("type",StringType(), True),
                StructField("value",StringType(), True),
                # StructField("pos_cust",StringType(), True),
                # StructField("yearmonth",StringType(), True),
                # StructField("run_id",StringType(), True),
                # StructField("crtd_dttm",StringType(), True),
                # StructField("filename",StringType(), True)                
            ])
        
        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter",",")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
            # .option("field_delimiter", "\\u0001")\\

        
        
        df=df.na.drop("all")
        if df.count()==0 :
            return "No Data in file"

        
        # df = df.with_column("cdl_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        file_name1=file_name.split(".")[0]+"_"+ datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")+".csv"
        df = df.with_column("filename", lit(file_name))
        df = df.with_column("crtd_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        # df = df.with_column("mnth_id", file_name.split("_")[1])
        df = df.with_column("run_id", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        df = df.with_column("pos_cust", lit(file_name.split("_")[1]))
        df = df.with_column("yearmonth", lit(file_name.split("_")[2].split(".")[0]))
        
        # date=file_name.split("_")[3].split(".")[0]
        # year=date[0:4]
        # mnth_no=date[4:6]
        # inv_week=date[6:]
        # df = df.withColumn("year", lit(year))
        # df = df.with_column("mnth_no", lit(mnth_no))
        # df = df.with_column("inv_week", lit(inv_week))  
        
        snowdf= df.select(
                    # "mnth_id",
                    "account",
                    "kode_branch",
                    "branch_name",
                    "tgl",
                    "plu",
                    "descp",
                    "type",
                    "value",
                    "pos_cust",
                    "yearmonth",
                    "run_id",
                    "crtd_dttm",
                    "filename"                 
                    )
        file_name=file_name.split(".")[0]+"_"+datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")

        # df.show()
        
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


CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.IDNSDL_RAW.ID_POS_DAILY_BASEDLINE_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS 'from snowflake.snowpark.functions import col, lit, date_format, current_timestamp, to_date, year, month, concat, format_number, regexp_replace,to_timestamp,when,trim,upper, substring
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DecimalType
import snowflake.snowpark
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz
import pandas as pd

def main(session: snowpark.Session,Param):
    # Param=["Basedline_20240515010016.csv","IDNSDL_RAW.DEV_LOAD_STAGE_ADLS_IDN","dev/POS/transaction/daily_basedline_sellout/","SDL_ID_POS_DAILY_BASEDLINE_SELLOUT"]
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        
        df_schema = StructType([
                # StructField("mnth_id",StringType(), True),
                StructField("key_account",StringType(), True),
                StructField("plu",StringType(), True),
                StructField("month",StringType(), True),
                StructField("year",StringType(), True),
                StructField("roi_month",StringType(), True),
                StructField("qty_trx",StringType(), True),
                StructField("cum_ytm_qty",StringType(), True),
                StructField("promo_qty",StringType(), True),
                StructField("cum_promo_qty",StringType(), True),
                StructField("basedline_total_qty",StringType(), True),
                StructField("cum_basedline_total_qty",StringType(), True),
                StructField("total_days",StringType(), True),
                StructField("cum_total_days",StringType(), True),
                StructField("promo_days",StringType(), True),
                StructField("cum_promo_days",StringType(), True),
                StructField("baselined_total_days",StringType(), True),
                StructField("cum_baselined_total_days",StringType(), True),
                StructField("total_qty_baselined",StringType(), True),
                StructField("indirect_qty_trx",StringType(), True),
                StructField("indirect_cum_qty",StringType(), True),
                StructField("indirect_promo_qty",StringType(), True),
                StructField("indirect_cum_promo_qty",StringType(), True),
                StructField("indirect_basedline_total_qty",StringType(), True),
                StructField("indirect_cum_basedline_total_qty",StringType(), True),
                StructField("indirect_qty_basedlined",StringType(), True),
                # StructField("pos_cust",StringType(), True),
                # StructField("yearmonth",StringType(), True),
                # StructField("run_id",StringType(), True),
                # StructField("crtd_dttm",StringType(), True),
                # StructField("file_name",StringType(), True)                
            ])
        
        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "\\u0001")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
            # .option("field_delimiter",",")\\

        
        
        df=df.na.drop("all")
        if df.count()==0 :
            return "No Data in file"

        
        # df = df.with_column("cdl_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        file_name1=file_name.split(".")[0]+"_"+ datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")+".csv"
        df = df.with_column("file_name", lit(file_name))
        df = df.with_column("crtd_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        # df = df.with_column("mnth_id", file_name.split("_")[1])
        df = df.with_column("run_id", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        df = df.with_column("pos_cust", lit(file_name.split("_")[0]))
        df = df.with_column("yearmonth", lit(file_name.split("_")[1].split(".")[0]))
        
        # date=file_name.split("_")[3].split(".")[0]
        # year=date[0:4]
        # mnth_no=date[4:6]
        # inv_week=date[6:]
        # df = df.withColumn("year", lit(year))
        # df = df.with_column("mnth_no", lit(mnth_no))
        # df = df.with_column("inv_week", lit(inv_week))  
        
        snowdf= df.select(
                    # "mnth_id",
                    "key_account",
                    "plu",
                    "month",
                    "year",
                    "roi_month",
                    "qty_trx",
                    "cum_ytm_qty",
                    "promo_qty",
                    "cum_promo_qty",
                    "basedline_total_qty",
                    "cum_basedline_total_qty",
                    "total_days",
                    "cum_total_days",
                    "promo_days",
                    "cum_promo_days",
                    "baselined_total_days",
                    "cum_baselined_total_days",
                    "total_qty_baselined",
                    "indirect_qty_trx",
                    "indirect_cum_qty",
                    "indirect_promo_qty",
                    "indirect_cum_promo_qty",
                    "indirect_basedline_total_qty",
                    "indirect_cum_basedline_total_qty",
                    "indirect_qty_basedlined",
                    "pos_cust",
                    "yearmonth",
                    "run_id",
                    "crtd_dttm",
                    "file_name"                 
                    )
        file_name=file_name.split(".")[0]+"_"+datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")

        # df.show()
        
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
