use schema THASDL_RAW;
CREATE OR REPLACE PROCEDURE THASDL_RAW.SDL_TH_MT_BIGC("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '
from snowflake.snowpark.functions import col, lit, date_format, current_timestamp, to_date, year, month, concat, format_number, regexp_replace, to_timestamp, when, trim, upper
from snowflake.snowpark.types import StringType, StructType, StructField,DecimalType
import snowflake.snowpark
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz

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

        df = df.na.drop("all")
        if df.count() == 0:
            return "No Data in table"

        file_name1 = file_name.split(".")[0] + "_" + datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S") + ".csv"

        df = df.with_column("CRT_DTTM", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S"))) \\
               .with_column("FILE_NAME", lit(file_name1))

        
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
        

        # Archive to success
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        file_name=file_name.split(".")[0]+"_"+ datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")

        snowdf.write.copy_into_location(
            "@"+stage_name+
            "/"+temp_stage_path+
            "/"+"processed/success/"+formatted_year+
            "/"+formatted_month+
            "/"+file_name,
            header=True,
            OVERWRITE=True
        )
        
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
