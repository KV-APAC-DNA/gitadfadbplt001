CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.NTASDL_RAW.TW_CRM_Invoice_Preprocessing("PARAM" ARRAY)
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
        # Rename multiple columns
        pandas_df.rename(columns={
        pandas_df.columns[0]: ''PURCHASE_DATE'',   
        pandas_df.columns[1]: ''CHANNEL'',  
        pandas_df.columns[2]: ''PRODUCT'',   
        pandas_df.columns[3]: ''STATUS'',
        pandas_df.columns[4]: ''CREATED_DATE'',
        pandas_df.columns[5]: ''COMPLETED_DATE'',
        pandas_df.columns[6]: ''SUBSCRIBER_KEY'',
        pandas_df.columns[7]: ''POINTS'',
        pandas_df.columns[8]: ''SHOW_RECORD'',
        pandas_df.columns[9]: ''QUANTITY'',
        pandas_df.columns[10]: ''INVOICE_TYPE'',
        pandas_df.columns[11]: ''SELLER_NAME'',
        pandas_df.columns[12]: ''PRODUCT_CATEGORY'',
        pandas_df.columns[13]: ''WEBSITE_UNIQUE_ID'',
        pandas_df.columns[14]: ''INVOICE_NUMBER'',
        pandas_df.columns[15]: ''EPSILON_PRICE_PER_UNIT'',
        pandas_df.columns[16]: ''EPSILON_AMOUNT'',
        pandas_df.columns[17]: ''EPSILON_TOTAL_AMOUNT''
         }, inplace=True)
       
        
        # Create a Snowpark DataFrame from the pandas DataFrame
        #snowpark_df = session.create_dataframe(pandas_df, schema=df_schema)
        snowpark_df = session.create_dataframe(pandas_df)
        snowpark_df = snowpark_df.na.drop("all")
        if snowpark_df.count() == 0:
            return "No Data in file"
        snowpark_df = snowpark_df.withColumn("filename", lit(file_name))        
        snowpark_df = snowpark_df.withColumn("crtd_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
                    #Converting columns similar to table column names
        snowpark_df = snowpark_df.rename({col(''PURCHASE_DATE''):''purchase_date'',col(''CHANNEL''):''channel'',col(''PRODUCT''):''product'',\\
                                        col(''STATUS''):''status'',col(''CREATED_DATE''):''created_date'',\\
                                        col(''COMPLETED_DATE''):''COMPLETED_DATE'',\\
                                        col(''SUBSCRIBER_KEY''):''subscriber_key'',\\
                                        col(''POINTS''):''points'',\\
                                        col(''SHOW_RECORD''):''show_record'',col(''QUANTITY''):''quantity'',\\
                                        col(''INVOICE_TYPE''):''invoice_type'',col(''SELLER_NAME''):''seller_nm''\\
                                        ,col(''PRODUCT_CATEGORY''):''product_category'',\\
                                        col(''WEBSITE_UNIQUE_ID''):''website_unique_id'',\\
                                        col(''INVOICE_NUMBER''):''invoice_number'',\\
                                        col(''"EPSILON_PRICE_PER_UNIT"''):''epsilon_price_per_unit'',\\
                                        col(''"EPSILON_AMOUNT"''):''epsilon_amount'',\\
                                        col(''"EPSILON_TOTAL_AMOUNT"''):''epsilon_total_amount'' })        
        snowdf = snowpark_df.select(
           "purchase_date",
           "channel",
           "product",
           "status",
           "created_date",
           "completed_date",
           "subscriber_key",
           "points",
           "show_record",
           "qty",
           "invoice_type",
           "seller_nm",
           "product_category",
           "website_unique_id",
           "invoice_num",
           "epsilon_price_per_unit",
           "epsilon_amount",
           "epsilon_total_amount",
            "file_name",
            "crtd_dttm"
            )
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
