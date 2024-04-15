USE SCHEMA PCFSDL_RAW;

CREATE OR REPLACE PROCEDURE ACCTCUSTOMLIST_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS 'from snowflake.snowpark.functions import col, lit, date_format, current_timestamp, to_date, year, month, concat, format_number, regexp_replace,to_timestamp,when,trim,upper
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DecimalType
#import snowflake.snowpark
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz
import pandas as pd

def main(session: snowpark.Session,Param):
    #Param=["AcctCustomList_20240326223026.csv","PCFSDL_RAW.DEV_LOAD_STAGE_ADLS","dev/ap_perenso/master/account/","sdl_perenso_account_custom_list"]
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        
        df_schema = StructType([
                StructField("acct_key",IntegerType(), True),
                StructField("field_key", IntegerType(), True),
                StructField("option_desc", StringType(), True)
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

       
        df = df.with_column("run_id", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        #file_name1=file_name.split(".")[0]+"_"+ datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")+".csv"
       # df = df.with_column("file_name", lit(file_name1))
        df = df.with_column("create_dt", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))

        snowdf= df.select(
                    "acct_key",
                    "field_key",
                    "option_desc",
                    "run_id",
                    "create_dt"
                    )
        file_name=file_name.split(".")[0]+"_"+datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")

        snowdf.write.mode("append").saveAsTable(stage_name.split(".")[0] + "." + target_table)
        
        #move to success
        snowdf.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)
        
        
        
        return ''Success''

    
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
        return error_message';
CREATE OR REPLACE PROCEDURE  ANZ_PERENSO_FOODSTUFF_FSSI_PREPROCESSING("PARAM" ARRAY)  
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
from snowflake.snowpark.functions import col,lit,trim, substring,current_timestamp,to_timestamp
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField,FloatType
import pandas as pd
from datetime import datetime
import pytz

def main(session: snowpark.Session,Param): 

    try:

        #Param=[''FSSI_Week_12_2024.csv'',''PCFSDL_RAW.DEV_LOAD_STAGE_ADLS'',''dev/ap_perenso/master/weekly_load/foodstuff/'',''SDL_PERENSO_FSSI_SALES'']

        # Extracting parameters from the input
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]


        # Define the schema for the DataFrame
        df_schema=StructType([
            StructField("ARTICLE",StringType()),
            StructField("ARTICLE_DESCRIPTION",StringType()),
            StructField("OLD_ARTICLE_NUM",StringType()),
            StructField("EAN",StringType()),
            StructField("GROSS_WEIGHT",StringType()),
            StructField("SHIP_TO_STORE",StringType()),
            StructField("STORE_NAME",StringType()),
            StructField("SALES_VOLUME",IntegerType()),
            StructField("SALES_VALUE",FloatType())
            ])


        # Read the CSV file into a DataFrame
    
        dataframe = session.read\\
            .schema(df_schema)\\
            .option("skip_header",4)\\
            .option("field_delimiter", "\\u0001")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .option("skip_blank_lines", True)\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        #---------------------------Transformation logic ------------------------------#

        # Handle null values or empty rows
        dataframe= dataframe.na.drop("all")

        # Filter Result and null value in SHIP_TO_STORE and STORE_NAME
        dataframe= dataframe.filter((dataframe["SHIP_TO_STORE"] != "result") & dataframe["STORE_NAME"].isNotNull())

        # Check for empty Dataframe
        if dataframe.count()==0:
            return "No Data in file"

        # Add filename and CRT_DTTM columns
        new_file_name=file_name.replace("_"," ").split(".")[0]+''.xls''
        dataframe = dataframe.with_column("FILE_NAME",lit(new_file_name))
        dataframe = dataframe.withColumn("CRT_DTTM",lit(to_timestamp(current_timestamp())))

        # Creating Final Dataframe
        final_df = dataframe.alias("final_df")

        # Load Data to the target table
        final_df.write.mode("append").saveAsTable(target_table)

        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")

        # write to success folder
    
        file_name=file_name.replace("_"," ").split(".")[0]+''_''+datetime.now().strftime("%Y%m%d%H%M%S")
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
CREATE OR REPLACE PROCEDURE  ANZ_PERENSO_FOODSTUFF_WEEKLY_SALES_REPORT_PREPROCESSING("PARAM" ARRAY)  
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
from snowflake.snowpark.functions import col,lit,trim, substring,current_timestamp,to_timestamp
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField,FloatType
import pandas as pd
from datetime import datetime
import pytz

def main(session: snowpark.Session,Param): 

    try:

        #Param=[''Weekly_Sales_Report_-_Kenvue_-_2024-03-25.csv'',''PCFSDL_RAW.DEV_LOAD_STAGE_ADLS'',''dev/ap_perenso/master/weekly_load/foodstuff/weekly_sales_report/'',''SDL_PERENSO_FSNI_SALES'']

        # Extracting parameters from the input
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]


        # Define the schema for the DataFrame
        df_schema=StructType([
            StructField("SITE",StringType()),
            StructField("VENDOR_ID",StringType()),
            StructField("VENDOR_NAME",StringType()),
            StructField("DEPARTMENT",StringType()),
            StructField("MERCHANDISE_GROUP",StringType()),
            StructField("ARTICLE",StringType()),
            StructField("ARTICLE_DESCRIPTION",StringType()),
            StructField("RETAIL_BARCODE",StringType()),
            StructField("BANNER",StringType()),
            StructField("STORE_NUMBER",StringType()),
            StructField("STORE_NAME",StringType()),
            StructField("BILLING_DOCUMENT",StringType()),
            StructField("BILLING_DATE",StringType()),
            StructField("PRICING_DATE",StringType()),
            StructField("NET_SALES",FloatType()),
            StructField("QTY_IN_SALES",StringType()),
            StructField("SALES_UOM",StringType()),
            StructField("QTY_IN_BASE",IntegerType()),
            StructField("BASE_UOM",StringType())
            ])


        # Read the CSV file into a DataFrame
    
        dataframe = session.read\\
            .schema(df_schema)\\
            .option("skip_header",4)\\
            .option("field_delimiter", "\\u0001")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .option("skip_blank_lines", True)\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        #---------------------------Transformation logic ------------------------------#

        # Handle null values or empty rows
        dataframe= dataframe.na.drop("all")

        # Filter Result and null value in SHIP_TO_STORE and STORE_NAME
        dataframe= dataframe.filter((dataframe["QTY_IN_SALES"] != "Total Base Unom") & dataframe["SALES_UOM"].isNotNull())

        # Check for empty Dataframe
        if dataframe.count()==0:
            return "No Data in file"

        dataframe = dataframe.withColumn("QTY_IN_SALES", col("QTY_IN_SALES").cast("integer"))

         # Add filename and CRT_DTTM columns
        new_file_name=file_name.replace("_"," ").split(".")[0]+''.xlsx''
        dataframe = dataframe.with_column("FILE_NAME",lit(new_file_name))
        dataframe = dataframe.withColumn("CRT_DTTM",lit(to_timestamp(current_timestamp())))

        final_df=dataframe.select("SITE","VENDOR_ID","VENDOR_NAME","DEPARTMENT","MERCHANDISE_GROUP","ARTICLE","ARTICLE_DESCRIPTION","RETAIL_BARCODE",\\
                                 "BANNER","STORE_NUMBER","STORE_NAME","BILLING_DOCUMENT","BILLING_DATE","PRICING_DATE","NET_SALES","QTY_IN_SALES",\\
                                 "SALES_UOM","QTY_IN_BASE","BASE_UOM","FILE_NAME","CRT_DTTM")

        # Load Data to the target table
        final_df.write.mode("append").saveAsTable(target_table)
        

        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")

        # write to success folder
    
        file_name=file_name.replace("_"," ").split(".")[0]+''_''+datetime.now().strftime("%Y%m%d%H%M%S")
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
CREATE OR REPLACE PROCEDURE  ANZ_PERENSO_OVER_AND_ABOVE_PREPROCESSING("PARAM" ARRAY)  
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them.

import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col,lit,trim,when
from snowflake.snowpark.types import  StringType, StructType, StructField,IntegerType
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz



def main(session: snowpark.Session,Param): 
       
    try :

        #Param=["OverandAbove_20240320223707.csv","PCFSDL_RAW.DEV_LOAD_STAGE_ADLS","dev/ap_perenso/transaction/overandabove","sdl_perenso_Over_and_Above"]

        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        # Define the schema for the DataFrame
        
        df_schema = StructType([
                StructField("over_and_above_key", IntegerType()),
                StructField("acct_key", IntegerType()),
                StructField("todo_option_key", IntegerType()),
                StructField("prod_grp_key", IntegerType()),
                StructField("activated", StringType()),
                StructField("notes", StringType())
            ])

   
        
        dataframe = session.read.schema(df_schema) \\
        .option("skip_header",1) \\
        .option("field_delimiter", ",") \\
        .option("field_optionally_enclosed_by", "\\"")  \\
        .option("encoding","UTF-8")\\
        .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        
        dataframe=dataframe.na.drop("all")
        if dataframe.count()==0:
            return "No Data in file"

        dataframe = dataframe.with_column("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        dataframe = dataframe.with_column("CRTD_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))

        # Load Data to the target table
        dataframe.write.mode("append").saveAsTable(target_table)
       
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        #move to success
        dataframe.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)

        
        return ''Success''
        
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
CREATE OR REPLACE PROCEDURE  ANZ_PERENSO_OVER_AND_ABOVE_STATE_PREPROCESSING("PARAM" ARRAY)  
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them.

import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col,lit,trim,when
from snowflake.snowpark.types import  StringType, StructType, StructField,IntegerType
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz



def main(session: snowpark.Session,Param): 
       
    try :
            
        # Param=[''OverandAboveState_20240320223707.csv'',''PCFSDL_RAW.DEV_LOAD_STAGE_ADLS'',''dev/ap_perenso/transaction/overandabove'',''sdl_perenso_over_and_above_state'']

        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        # Define the schema for the DataFrame
        
        df_schema = StructType([
                StructField("over_and_above_key", IntegerType()),
                StructField("start_date", StringType()),
                StructField("end_date", StringType()),
                StructField("batch_count", IntegerType()),
                StructField("store_chk_hdr_key", IntegerType())
            ])

  
        
        dataframe = session.read.schema(df_schema) \\
        .option("skip_header",1) \\
        .option("field_delimiter", ",") \\
        .option("field_optionally_enclosed_by", "\\"")  \\
        .option("encoding","UTF-8")\\
        .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        
        dataframe=dataframe.na.drop("all")
        if dataframe.count()==0:
            return "No Data in file"

        dataframe = dataframe.with_column("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        dataframe = dataframe.with_column("CRTD_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))

        # Load Data to the target table
        dataframe.write.mode("append").saveAsTable(target_table)
       
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        #move to success
        dataframe.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)

        
        return ''Success''
        
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
CREATE OR REPLACE PROCEDURE  ANZ_PERENSO_PRODCHKDISTRIBUTION_PREPROCESSING("PARAM" ARRAY)  
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them.

import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col,lit,trim,when
from snowflake.snowpark.types import  StringType, StructType, StructField,IntegerType
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz



def main(session: snowpark.Session,Param): 
       
    try :
            
        #Param=[''ProdChkDistribution_20240320223707.csv'',''PCFSDL_RAW.DEV_LOAD_STAGE_ADLS'',''dev/ap_perenso/transaction/overandabove'',''sdl_perenso_Prod_Chk_Distribution'']

        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        # Define the schema for the DataFrame
        
        df_schema = StructType([
                StructField("acct_key", IntegerType()),
                StructField("prod_key", IntegerType()),
                StructField("start_date", StringType()),
                StructField("end_date", StringType()),
                StructField("in_distribution", StringType())
            ])


        
        dataframe = session.read.schema(df_schema) \\
        .option("skip_header",1) \\
        .option("field_delimiter", ",") \\
        .option("field_optionally_enclosed_by", "\\"")  \\
        .option("encoding","UTF-8")\\
        .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        
        dataframe=dataframe.na.drop("all")
        if dataframe.count()==0:
            return "No Data in file"

        dataframe = dataframe.with_column("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        dataframe = dataframe.with_column("CRTD_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))

        # Load Data to the target table
        dataframe.write.mode("append").saveAsTable(target_table)
       
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        #move to success
        dataframe.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)

        
        return ''Success''
        
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
CREATE OR REPLACE PROCEDURE  CHW_ECOMM_DATA_PREPROCESSING("PARAM" ARRAY)  
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS 'from snowflake.snowpark.functions import col, lit, date_format, current_timestamp, to_date, year, month, concat, format_number, regexp_replace, to_timestamp, when, trim, upper
from snowflake.snowpark.types import StringType, StructType, StructField
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz

def main(session: snowpark.Session,Param):
    #Param=["Kenvue_Weekly_CHW_eCommerce_Data_022024.csv","PCFSDL_RAW.DEV_LOAD_STAGE_ADLS","dev/pharm_ecommerce/transaction/kenvue_weekly_chw_ecommerce_data","SDL_CHW_ECOMM_DATA"]
    try:
        file_name = Param[0]
        stage_name = Param[1]
        temp_stage_path = Param[2]
        target_table = Param[3]
        
        df_schema = StructType([
            StructField("pfc", StringType(), True),
            StructField("APN", StringType(), True),     
            StructField("skuname", StringType(), True),
            StructField("nec1_desc", StringType(), True),
            StructField("nec2_desc", StringType(), True),
            StructField("nec3_desc", StringType(), True),
            StructField("brand", StringType(), True),
            StructField("owner", StringType(), True),
            StructField("manufacturer", StringType(), True),
            StructField("category", StringType(), True),
            StructField("mat_year", StringType(), True),
            StructField("periodid", StringType(), True),
            StructField("sales_online", StringType(), True),
            StructField("unit_online", StringType(), True),
            StructField("week_end", StringType(), True),
        ])

        
        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", ",")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
        
        df = df.na.drop("all")
        
        if df.count() == 0:
            return "No Data in file"
            
        df = df.withColumn("file_name", lit(file_name))
        df = df.withColumn("crtd_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))

        snowdf = df.select(
            "pfc",
            "skuname",
            "nec1_desc",
            "nec2_desc",
            "nec3_desc",
            "brand",
            "owner",
            "manufacturer",
            "category",
            "mat_year",
            "periodid",
            "sales_online",
            "unit_online",
            "week_end",
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

CREATE OR REPLACE PROCEDURE  IRI_SCAN_SALES_PREPROCESSING("PARAM" ARRAY)  
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
from snowflake.snowpark.functions import col,lit,trim, substring,current_timestamp,to_timestamp
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField,FloatType
import pandas as pd
from datetime import datetime
import pytz

def main(session: snowpark.Session,Param): 

    try:

        #Param=[''All_J_J_Items_WE040224.csv'',''PCFSDL_RAW.DEV_LOAD_STAGE_ADLS'',''dev/IRI_Scan_Data/master/all_jj_item_we/'',''SDL_IRI_SCAN_SALES'']

        # Extracting parameters from the input
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]


        # Define the schema for the DataFrame
        df_schema=StructType([
            StructField("IRI_MARKET",StringType()),
            StructField("WK_END_DT",StringType()),
            StructField("IRI_PROD_DESC",StringType()),
            StructField("IRI_EAN",StringType()),
            StructField("SCAN_SALES",FloatType()),
            StructField("SCAN_UNITS",FloatType()),
            StructField("AC_NIELSENCODE",StringType())
            ])


        # Read the CSV file into a DataFrame
    
        dataframe = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "\\u0001")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
        

        #---------------------------Transformation logic ------------------------------#

        # Handle empty rows
        dataframe= dataframe.na.drop("all")


        dataframe=dataframe.filter(col("IRI_MARKET").is_not_null()) and dataframe.filter(col("WK_END_DT").is_not_null()) and \\
        dataframe.filter(col("IRI_PROD_DESC").is_not_null()) and dataframe.filter(col("IRI_EAN").is_not_null()) and \\
        dataframe.filter(col("SCAN_SALES").is_not_null()) and dataframe.filter(col("SCAN_UNITS").is_not_null())

        # Check for empty Dataframe
        if dataframe.count()==0:
            return "No Data in file"

        # Add filename and CRT_DTTM columns

        dataframe = dataframe.withColumn("CRTD_DTTM",lit(to_timestamp(current_timestamp())))
        
        file_name_without_extension = file_name.split(".")[0]
        date_string= file_name_without_extension[16:]
        new_file_name = "All J_J Items WE"+date_string + ".xlsx"
        dataframe = dataframe.with_column("FILE_NAME",lit(new_file_name))

        # Creating Final Dataframe
        final_df = dataframe.alias("final_df")

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

CREATE OR REPLACE PROCEDURE J_PAC_PHARM_OUTLET_PREPROCESSING("PARAM" ARRAY)  
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
    #Param=["Outlet_20240323_20240328213034.csv.gz","PCFSDL_RAW.DEV_LOAD_STAGE_ADLS","dev/IQVIA/transaction/Weekly_Data","sdl_pharm_sellout_outlet"]
    try:
        file_name = Param[0]
        stage_name = Param[1]
        temp_stage_path = Param[2]
        target_table = Param[3]
        
        df_schema = StructType([
                StructField("WeekEndingDate", StringType(), True),
                StructField("OutletNumber", StringType(), True),
                StructField("Name", StringType(), True),
                StructField("Address1", StringType(), True),
                StructField("Address2", StringType(), True),
                StructField("Suburb", StringType(), True),
                StructField("State", StringType(), True),
                StructField("Postcode", StringType(), True),
                StructField("BannerGroupCode", StringType(), True),
                StructField("BannerGroupDescription", StringType(), True),
                StructField("EntityTypeCode", StringType(), True),
                StructField("EntityTypeDescription", StringType(), True),
                StructField("OutletTypeDescription", StringType(), True),
                StructField("Status", StringType(), True),
                StructField("BRICK", StringType(), True),
                StructField("ActualRetailBRICK", StringType(), True),
                StructField("ActualOutlet", StringType(), True),
            ])
        
        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", ",")\\
            .option("compression", "gzip")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
        
        df = df.na.drop("all")
        
        if df.count() == 0:
            return "No Data in file"

        df_dup1 = df.select(''WeekEndingDate'', ''OutletNumber'', ''ActualOutlet'')
            
        df_dup2 = df_dup1.dropDuplicates()
        
        if df_dup1.count() != df_dup2.count():
            return "Duplicate values" 
            
        file_name1 = file_name.split(".")[0] + ".csv"
        df = df.withColumn("filename", lit(file_name1))
        df = df.withColumn("crt_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))

        snowdf = df.select(
            ''WeekEndingDate'', ''OutletNumber'', ''Name'', ''Address1'', ''Address2'', ''Suburb'',
            ''State'', ''Postcode'', ''BannerGroupCode'', ''BannerGroupDescription'', ''EntityTypeCode'',
            ''EntityTypeDescription'', ''OutletTypeDescription'', ''Status'', ''BRICK'', ''ActualRetailBRICK'',
            ''ActualOutlet'', ''filename'', ''crt_dttm''
        )
        
        file_name = file_name.split(".")[0] + "_" + datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        

        
        
        snowdf.write.mode("append").saveAsTable(stage_name.split(".")[0] + "." + target_table)
        
        snowdf.write.copy_into_location("@" + stage_name + "/" + temp_stage_path + "/processed/success/" + formatted_year + "/" + formatted_month + "/" + file_name, header=True, OVERWRITE=True,file_format_name=''file_format_zip'')
        
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
CREATE OR REPLACE PROCEDURE  J_PAC_PHARM_PRODUCT_PREPROCESSING("PARAM" ARRAY)  
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
    #Param=["Product_20240323_20240328213034.csv.gz","PCFSDL_RAW.DEV_LOAD_STAGE_ADLS","dev/IQVIA/transaction","sdl_pharm_sellout_product"]
    try:
        file_name = Param[0]
        stage_name = Param[1]
        temp_stage_path = Param[2]
        target_table = Param[3]
        
        df_schema = StructType([
                StructField("WeekEndingDate", StringType(), True),
                StructField("PFC", StringType(), True),
                StructField("ProductCode", StringType(), True),
                StructField("PackCode", StringType(), True),
                StructField("Product_Long_Desc", StringType(), True),
                StructField("Pack_Long_Desc", StringType(), True),
                StructField("Org_Abbr", StringType(), True),
                StructField("Org_Long_Name", StringType(), True),
                StructField("APN", StringType(), True),
                StructField("PackSize", StringType(), True),
                StructField("PackStrengthUnits", StringType(), True),
                StructField("PackStrengthMeasure", StringType(), True),
                StructField("PackStrengthAdditional", StringType(), True),
                StructField("PackVolumeUnits", StringType(), True),
                StructField("PackVolumeMeasure", StringType(), True),
                StructField("PackForm", StringType(), True),
                StructField("ProdLaunchDate", StringType(), True),
                StructField("PackLaunchDate", StringType(), True),
                StructField("ATC1Code", StringType(), True),
                StructField("ATC1Desc", StringType(), True),
                StructField("ATC2Code", StringType(), True),
                StructField("ATC2Desc", StringType(), True),
                StructField("ATC3Code", StringType(), True),
                StructField("ATC3Desc", StringType(), True),
                StructField("ATC4Code", StringType(), True),
                StructField("ATC4Desc", StringType(), True),
                StructField("CHSegmentCode", StringType(), True),
                StructField("CHSegmentDesc", StringType(), True),
                StructField("NEC1Code", StringType(), True),
                StructField("NEC1Desc", StringType(), True),
                StructField("NEC2Code", StringType(), True),
                StructField("NEC2Desc", StringType(), True),
                StructField("NEC3Code", StringType(), True),
                StructField("NEC3Desc", StringType(), True),
                StructField("NEC4Code", StringType(), True),
                StructField("NEC4Desc", StringType(), True),
                StructField("Form1Code", StringType(), True),
                StructField("Form1Desc", StringType(), True),
                StructField("Form2Code", StringType(), True),
                StructField("Form2Desc", StringType(), True),
                StructField("Form3Code", StringType(), True),
                StructField("Form3Desc", StringType(), True),
                StructField("ConcentrationMeasure", StringType(), True),
                StructField("ConcentrationUnit", StringType(), True),
                StructField("PrescriptionStatusCode", StringType(), True),
                StructField("PrescriptionStatusDesc", StringType(), True),
                StructField("PoisonScheduleCode", StringType(), True),
                StructField("PoisonScheduleDesc", StringType(), True),
                StructField("EthicalStatus", StringType(), True),
                StructField("PBSFormulary", StringType(), True),
                StructField("PBSStatus", StringType(), True),
                StructField("BrandPricePremium", StringType(), True)
            ])
        
        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", ",")\\
            .option("compression", "gzip")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        df=df.na.replace({"":None})      
        df = df.na.drop("all")
        
        if df.count() == 0:
            return "No Data in file"

        df_dup1 = df.select(''WeekEndingDate'', ''PFC'')
            
        df_dup2 = df_dup1.dropDuplicates()
        
        if df_dup1.count() != df_dup2.count():
            return "Duplicate values" 
            
        file_name1 = file_name.split(".")[0] + "_" + datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S") + ".csv"
        df = df.withColumn("filename", lit(file_name1))
        df = df.withColumn("crt_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))

        snowdf = df.select(
            ''WeekEndingDate'', ''PFC'', ''ProductCode'', ''PackCode'', ''Product_Long_Desc'', ''Pack_Long_Desc'', ''Org_Abbr'',
            ''Org_Long_Name'', ''APN'', ''PackSize'', ''PackStrengthUnits'', ''PackStrengthMeasure'', ''PackStrengthAdditional'',
            ''PackVolumeUnits'', ''PackVolumeMeasure'', ''PackForm'', ''ProdLaunchDate'', ''PackLaunchDate'', ''ATC1Code'',
            ''ATC1Desc'', ''ATC2Code'', ''ATC2Desc'', ''ATC3Code'', ''ATC3Desc'', ''ATC4Code'', ''ATC4Desc'', ''CHSegmentCode'',
            ''CHSegmentDesc'', ''NEC1Code'', ''NEC1Desc'', ''NEC2Code'', ''NEC2Desc'', ''NEC3Code'', ''NEC3Desc'', ''NEC4Code'',
            ''NEC4Desc'', ''Form1Code'', ''Form1Desc'', ''Form2Code'', ''Form2Desc'', ''Form3Code'', ''Form3Desc'',
            ''ConcentrationMeasure'', ''ConcentrationUnit'', ''PrescriptionStatusCode'', ''PrescriptionStatusDesc'',
            ''PoisonScheduleCode'', ''PoisonScheduleDesc'', ''EthicalStatus'', ''PBSFormulary'', ''PBSStatus'',
            ''BrandPricePremium'', ''filename'', ''crt_dttm''
        )
        
        file_name = file_name.split(".")[0] + "_" + datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        
        #move to success
        
        
        snowdf.write.mode("append").saveAsTable(stage_name.split(".")[0] + "." + target_table)
        snowdf.write.copy_into_location("@" + stage_name + "/" + temp_stage_path + "/processed/success/" + formatted_year + "/" + formatted_month + "/" + file_name, header=True, OVERWRITE=True,file_format_name=''file_format_zip'')
        
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
CREATE OR REPLACE PROCEDURE NATIONAL_ECOMM_DATA_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS 'from snowflake.snowpark.functions import col, lit, date_format, current_timestamp, to_date, year, month, concat, format_number, regexp_replace, to_timestamp, when, trim, upper
from snowflake.snowpark.types import StringType, StructType, StructField
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz

def main(session: snowpark.Session,Param):
    #Param=["Kenvue_Weekly_National_eCommerce_Data_022024.csv","PCFSDL_RAW.DEV_LOAD_STAGE_ADLS","dev/pharm_ecommerce/transaction/kenvue_weekly_national_ecommerce_data","SDL_NATIONAL_ECOMM_DATA"]
    try:
        file_name = Param[0]
        stage_name = Param[1]
        temp_stage_path = Param[2]
        target_table = Param[3]
        
        df_schema = StructType([
            StructField("pfc", StringType(), True),
            StructField("apn", StringType(), True),     
            StructField("skuname", StringType(), True),
            StructField("nec1_desc", StringType(), True),
            StructField("nec2_desc", StringType(), True),
            StructField("nec3_desc", StringType(), True),
            StructField("brand", StringType(), True),
            StructField("owner", StringType(), True),
            StructField("manufacturer", StringType(), True),
            StructField("category", StringType(), True),
            StructField("mat_year", StringType(), True),
            StructField("periodid", StringType(), True),
            StructField("sales_online", StringType(), True),
            StructField("unit_online", StringType(), True),
            StructField("week_end", StringType(), True),
        ])

        
        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", ",")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
        
        df = df.na.drop("all")
        
        if df.count() == 0:
            return "No Data in file"
            
        df = df.withColumn("file_name", lit(file_name))
        df = df.withColumn("crtd_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))

        snowdf = df.select(
            "pfc",
            "skuname",
            "nec1_desc",
            "nec2_desc",
            "nec3_desc",
            "brand",
            "owner",
            "manufacturer",
            "category",
            "mat_year",
            "periodid",
            "sales_online",
            "unit_online",
            "week_end",
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
CREATE OR REPLACE PROCEDURE OVER_AND_ABOVE_POINTS("PARAM" ARRAY)
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
    #Param=["Over_and_Above_Points.csv","PCFSDL_RAW.DEV_LOAD_STAGE_ADLS","dev/ap_perenso/user_files","sdl_over_and_above_points"]
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        
        df_schema = StructType([
                StructField("display_type", StringType(), True),
                StructField("points", IntegerType(), True)
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

       


       
        df = df.with_column("run_id", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        #file_name1=file_name.split(".")[0]+"_"+ datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")+".csv"
        #df = df.with_column("file_name", lit(file_name1))
        df = df.with_column("create_dt", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))

        snowdf= df.select(
                    "display_type",
                    "points",
                    "run_id",
                    "create_dt"
                    )
        file_name=file_name.split(".")[0]+"_"+datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        
        #move to success
        snowdf.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)
        
        snowdf.write.mode("append").saveAsTable(stage_name.split(".")[0] + "." + target_table)
        
        return "success"

    
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
        return error_message';
