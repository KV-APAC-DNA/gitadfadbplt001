update meta_raw.parameters
set PARAMETER_VALUE = 'xlsx'
where parameter_group_id in (442,324) and PARAMETER_NAME in ('val_file_extn','source_extn');

update meta_raw.parameters
set PARAMETER_VALUE = 'PHLSDL_RAW.PH_IOP_TRGT_PREPROCESSING'
where PARAMETER_ID = 4326;

CREATE OR REPLACE PROCEDURE PHLSDL_RAW.PH_BP_TRGT_PREPROCESSING("PARAM" ARRAY)
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
    # Param=["PH_BP_2021_20221114161755.csv","PHLSDL_RAW.DEV_LOAD_STAGE_ADLS","dev/ph_bp/master/ph_bp_target","SDL_PH_BP_TRGT"]
    try:
        file_name = Param[0]
        stage_name = Param[1]
        temp_stage_path = Param[2]
        target_table = Param[3]
        
        df_schema = StructType([
            StructField("yearmo", StringType(), True),
            StructField("soldto_code", StringType(), True),     
            StructField("target_type", StringType(), True),
            StructField("brand_code", StringType(), True),
            StructField("amount", StringType(), True),      
        ])
        
        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
        
        df = df.na.drop("all")
        
        if df.count() == 0:
            return "No Data in file"

        # Filtering out rows with empty values in specified columns
        # df = df.filter((trim(col("yearmo")) == "") & (col("yearmo").isNull()) &
        #                (trim(col("soldto_code")) == "") & (col("soldto_code").isNull()) &
        #                (trim(col("target_type")) == "") & (col("target_type").isNull()) &
        #                (trim(col("brand_code")) == "") & (col("brand_code").isNull()) &             
                         # (trim(col("amount")) == "") & (col("amount").isNull()))
        
        # if df.count() == 0:
        #     return "No valid data in file after filtering empty rows"
        

        df = df.withColumn("crtd_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        df = df.withColumn("filename", lit(file_name))
        df = df.withColumn("run_id", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))

        df = df.withColumnRenamed("yearmo", "jj_mnth_id")
        df = df.withColumnRenamed("soldto_code", "CUST_ID")
        df = df.withColumnRenamed("target_type", "TRGT_TYPE")
        df = df.withColumnRenamed("brand_code", "BRND_CD")
        df = df.withColumnRenamed("amount", "TP_TRGT_AMT")
        
        for i in ["jj_mnth_id"]:
            df=df.na.fill("",subset=f''{i}'')
        # return df
        ex_df=session.sql("select * from phlsdl_raw.SDL_PH_BP_TRGT")
        for i in ["jj_mnth_id"]:
            ex_df=ex_df.na.fill("",subset=f''{i}'')
            # return ex_df
        df_j=df.join(ex_df,["jj_mnth_id"]).select(ex_df.jj_mnth_id,
                                                    ex_df.CUST_ID,
                                                    ex_df.TRGT_TYPE,
                                                    ex_df.BRND_CD,
                                                    ex_df.TP_TRGT_AMT,
                                                    ex_df.FILENAME,
                                                    ex_df.run_id,
                                                    ex_df.CRTD_DTTM
                                                  )
        
        # return df_j
        df_f= ex_df.subtract(df_j)
        snowdf=df_f.union_all(df)
        for i in ["yearmo"]:
            snowdf=snowdf.na.replace({"":None},[f''{i}''])
            # return snowdf


        file_name=file_name.split(".")[0]+"_"+ datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        
        #move to success
        snowdf.write.mode("append").saveAsTable(stage_name.split(".")[0] + "." + target_table)
        snowdf.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)
        
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

CREATE OR REPLACE PROCEDURE PHLSDL_RAW.PH_IOP_TRGT_PREPROCESSING("PARAM" ARRAY)
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
    # Param=["PH_IOP_2021_20220425230911.csv","PHLSDL_RAW.DEV_LOAD_STAGE_ADLS","dev/ph_iop/master/iop/ph_iop_target","SDL_PH_IOP_TRGT"]
    try:
        file_name = Param[0]
        stage_name = Param[1]
        temp_stage_path = Param[2]
        target_table = Param[3]
        
        df_schema = StructType([
            StructField("measure", StringType(), True),
            StructField("target_type", StringType(), True),     
            StructField("year", StringType(), True),
            StructField("brand", StringType(), True),
            StructField("segment", StringType(), True),
            StructField("customer_code", StringType(), True),
            StructField("account", StringType(), True),
            StructField("jan", StringType(), True),
            StructField("feb", StringType(), True),
            StructField("mar", StringType(), True),
            StructField("apr", StringType(), True),
            StructField("may", StringType(), True),
            StructField("jun", StringType(), True),
            StructField("jul", StringType(), True),
            StructField("aug", StringType(), True),
             StructField("sep", StringType(), True),
            StructField("oct", StringType(), True),
            StructField("nov", StringType(), True),
            StructField("dec", StringType(), True),    
        ])
        
        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
        
        df = df.na.drop("all")
        
        if df.count() == 0:
            return "No Data in file"
            
        df = df.withColumn("crt_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        df = df.withColumn("file_name", lit(file_name))
        df = df.withColumn("run_id", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))

        snowdf = df.select(
            "measure",
            "target_type",
            "year",
            "brand",
            "segment",
            "customer_code",
            "account",
            "jan",
            "feb",
            "mar",
            "apr",
            "may",
            "jun",
            "jul",
            "aug",
            "sep",
            "oct",
            "nov",
            "dec",
            "run_id",
            "crt_dttm",
            "file_name",
        )

        
        file_name=file_name.split(".")[0]+"_"+ datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        
        #move to success
        snowdf.write.mode("append").saveAsTable(stage_name.split(".")[0] + "." + target_table)
        snowdf.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True,file_format_name="my_csv_format")
        
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
