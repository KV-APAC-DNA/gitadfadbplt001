CREATE OR REPLACE PROCEDURE PHLSDL_RAW.PH_DMS_SELLOUT_STOCK_FACT_PREPROCESSING("PARAM" ARRAY)
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
    # Param=["OUT_CON_I_PH_20240331224500_20240401014431.csv","PHLSDL_RAW.DEV_LOAD_STAGE_ADLS","dev/ap_ph_metadata/dms/inventory","sdl_ph_dms_sellout_stock_fact"]
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        
        df_schema = StructType([
                StructField("dstrbtr_grp_cd",StringType(), True),
                StructField("cntry_cd",StringType(), True),
                StructField("wh_cd",StringType(), True),
                StructField("invoice_dt",StringType(), True),
                StructField("dstrbtr_prod_id",StringType(), True),
                StructField("qty",StringType(), True),
                StructField("uom",StringType(), True),
                StructField("amt",StringType(), True)
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

        
        df = df.with_column("cdl_dttm", lit(file_name.split("_")[-1].split(".")[0]))
        file_name1=file_name.split(".")[0]+"_"+ datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")+".csv"
        # df = df.with_column("file_name", lit(file_name1))
        df = df.with_column("curr_date", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        df = df.withColumn("amt", regexp_replace(col("amt"), "[^0-9]", ""))
        df = df.withColumn("qty", regexp_replace(col("qty"), "[^0-9]", ""))


        snowdf= df.select(
                    "dstrbtr_grp_cd",
                    "cntry_cd",
                    "wh_cd",
                    "invoice_dt",
                    "dstrbtr_prod_id",
                    "qty",
                    "uom",
                    "amt",
                    "cdl_dttm",
                    "curr_date"
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
