CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.NTASDL_RAW.TW_CRM_Invoice_preprocessing("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('openpyxl==3.0.10','snowflake-snowpark-python==*','unidecode==1.2.0','chardet')
HANDLER = 'main'
EXECUTE AS OWNER
AS 'from snowflake.snowpark.functions import col, lit, date_format, current_timestamp, to_date, year, month, concat, format_number, regexp_replace, to_timestamp, when, trim, upper
from snowflake.snowpark.types import StringType, StructType, StructField
import pandas as pd
from datetime import datetime
from snowflake.snowpark.window import Window
from snowflake.snowpark.functions import col,lit,concat,regexp_replace,trim,split,rtrim,upper,coalesce,row_number,when,to_date,is_null
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField,DecimalType,DateType
from snowflake.snowpark.files import SnowflakeFile
import snowflake.snowpark as snowpark
from unidecode import unidecode
from re import sub
import pytz
import csv
import chardet
def main(session: snowpark.Session,Param):
    #Param=["TW_CRM_Invoice_20240430.csv","NTASDL_RAW.DEV_LOAD_STAGE_ADLS_TWN","dev/SFMC/transaction/tw_crm_invoice/","SDL_TW_SFMC_INVOICE_DATA"]
    try:
        file_name = Param[0]
        stage_name = Param[1]
        temp_stage_path = Param[2]
        target_table = Param[3]
        #Reading data from adls
        full_path = "@"+stage_name+"/"+temp_stage_path+file_name
        
  # Reading the CSV file with the detected encoding
        with SnowflakeFile.open(full_path, ''rb'', require_scoped_url=False) as f:
            df = pd.read_csv(f, delimiter=''|'', engine=''python'',quoting=csv.QUOTE_MINIMAL, quotechar=''"'', encoding=''utf-16'')
           
        #df = df.applymap(lambda x: unidecode(str(x)))

        pandas_df = pd.DataFrame(df)

        pandas_df[''Epsilon_Price_Per_Unit''] = pandas_df[''Epsilon_Price_Per_Unit''].replace(r''[^0-9]'', '''', regex=True)
        pandas_df[''Epsilon_Amount''] = pandas_df[''Epsilon_Price_Per_Unit''].replace(r''[^0-9]'', '''', regex=True)
       
        df_schema = StructType([
            StructField("purchase_date", StringType(), True),
            StructField("channel", StringType(), True),
            StructField("product", StringType(), True),
            StructField("status", StringType(), True),
            StructField("created_date", StringType(), True),
            StructField("completed_date", StringType(), True),
            StructField("subscriber_key", StringType(), True),
            StructField("points", StringType(), True),
            StructField("show_record", StringType(), True),
            StructField("qty", StringType(), True),
            StructField("invoice_type", StringType(), True),
            StructField("seller_nm", StringType(), True),
            StructField("product_category", StringType(), True),
            StructField("website_unique_id", StringType(), True),
            StructField("invoice_num", StringType(), True),
            StructField("epsilon_price_per_unit", StringType(), True),
            StructField("epsilon_amount", StringType(), True),
            StructField("epsilon_total_amount", StringType(), True),
        ])

        # Create a Snowpark DataFrame from the pandas DataFrame
        snowpark_df = session.create_dataframe(pandas_df, schema=df_schema)

        snowpark_df = snowpark_df.na.drop("all")
        if snowpark_df.count() == 0:
            return "No Data in file"

        snowpark_df = snowpark_df.withColumn("filename", lit(file_name))        
        snowpark_df = snowpark_df.withColumn("crtd_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))

        # df = df.withColumn("run_id", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        file_name=file_name.split(".")[0]+"_"+ datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        #move to success
        snowpark_df.write.mode("append").saveAsTable(stage_name.split(".")[0] + "." + target_table)
        snowpark_df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)
        return ''Success''
    except KeyError as key_error:
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message
    except pd.errors.MergeError as merge_error:
        error_message = f"DataFrame merging error: {str(merge_error)}"
        return error_message
    except Exception as e:
        error_message = f"Error: {str(e)}"
        return error_message';
