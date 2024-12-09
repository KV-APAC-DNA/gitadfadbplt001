CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.HCPSDL_RAW.HCP360_IN_VENTASYS_EDETAILING_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '
from snowflake.snowpark.functions import col, lit, date_format, current_timestamp, to_date, year, month, concat, format_number, regexp_replace,to_timestamp,when,trim,upper
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DecimalType,DateType,TimestampType
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz


def main(session: snowpark.Session,Param):
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        sch_name        = stage_name.split(''.'')[0]
        target_table    = sch_name+"."+Param[3]

        df_schema = StructType([
                    StructField("Team", StringType()),
                    StructField("Month", StringType()),
                    StructField("RBM", StringType()),
                    StructField("ZBM", StringType()),
                    StructField("FBM", StringType()),
                    StructField("MSR", StringType()),
                    StructField("HQ_Code", StringType()),
                    StructField("MSR_Name", StringType()),
                    StructField("Dsg", StringType()),
                    StructField("Customer_Name", StringType()),
                    StructField("CID", StringType()),
					StructField("Speciality", StringType()),
					StructField("Customer_Type", StringType()),
					StructField("Active", StringType()),
					StructField("DCR_Date", DateType()),
					StructField("VA_Name", StringType()),
					StructField("Page_Name", StringType()),
					StructField("Page_Start_Time", TimestampType()),
					StructField("Page_End_Time", TimestampType()),
					StructField("Seconds", StringType()),
					StructField("Capped_Seconds", StringType()),
					StructField("As_On", TimestampType()),
                ])

        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "\\u0001")\\
            .option("field_optionally_enclosed_by", "\\x01") \\
            .option("field_optionally_enclosed_by", "\\"") \\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
        df = df.withColumn("crt_dttm",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        df = df.withColumn("file_name",lit(file_name).cast("string"))
        df = df.withColumn("run_id",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))

        a = file_name.split("_")[-1].split(".")[0]
        a = datetime.strptime(a, "%Y%m").date()
        df = df.withColumn("upload_date", lit(a))

        final_df = df.select("Team", "Month", "RBM", "ZBM", "FBM", "MSR", "HQ_Code","MSR_Name","Dsg", \\
                            "Customer_Name", "CID", "Speciality","Customer_Type","Active","DCR_Date","VA_Name","Page_Name","Page_Start_Time", \\
							"Page_End_Time","Seconds","Capped_Seconds","As_On",\\
                            "upload_date","crt_dttm","file_name","run_id"\\
                           ).alias("final_df")

        if final_df.count()==0:
            return "No Data in file"
        
        # Load Data to the target table
        final_df.write.mode("append").saveAsTable(target_table)

        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")

        # write to success folder
    
        file_name=file_name.split(".")[0] + ''_'' + datetime.now().strftime("%Y%m%d%H%M%S")
        final_df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,file_format_type="csv",OVERWRITE=True,header=True)
   
        return "Success"
        
    except KeyError as key_error:
        
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message
        
    except Exception as e:
        
        error_message = f"Error: {str(e)}" +str(df.columns)
        return error_message
';