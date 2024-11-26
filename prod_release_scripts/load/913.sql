CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.NTASDL_RAW.TW_CRM_Invoice_Preprocessing("PARAM" ARRAY)
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
    #Param=["TW_CRM_Invoice_20240430.csv","NTASDL_RAW.DEV_LOAD_STAGE_ADLS_TWN","dev/SFMC/transaction/tw_crm_invoice/","SDL_TW_SFMC_INVOICE_DATA"]
    try:
        file_name = Param[0]
        stage_name = Param[1]
        temp_stage_path = Param[2]
        target_table = Param[3]
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
        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "|")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .option("encoding","UTF-16")\\
            .option("record_delimiter","\\n")\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
        df = df.na.drop("all")
        if df.count() == 0:
            return "No Data in file"
        df = df.withColumn("EPSILON_PRICE_PER_UNIT",regexp_replace(lit(col("EPSILON_PRICE_PER_UNIT")),"''",''''))
        df = df.withColumn("EPSILON_AMOUNT",regexp_replace(lit(col("EPSILON_AMOUNT")),"''",''''))
        
        df = df.withColumn("crtd_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        df = df.withColumn("filename", lit(file_name))
        # df = df.withColumn("run_id", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        snowdf = df.select(
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
            "filename",
            "crtd_dttm"
            )
        file_name=file_name.split(".")[0]+"_"+ datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        #move to success
        snowdf.write.mode("append").saveAsTable(stage_name.split(".")[0] + "." + target_table)
        snowdf.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)
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
