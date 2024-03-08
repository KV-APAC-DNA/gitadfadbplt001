CREATE OR REPLACE PROCEDURE SGPSDL_RAW.SG_CIW_MAP_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS 'from snowflake.snowpark.functions import col, lit, date_format, current_timestamp, to_date, year, month, concat, format_number, regexp_replace,to_timestamp,when,trim,upper
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DecimalType,DateType, TimestampType
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz

def main(session: snowpark.Session,Param):
    #Param=["ciw_map.csv","SGPSDL_RAW.DEV_LOAD_STAGE_ADLS","dev/master_sellout/CIW_MAP","SDL_SG_CIW_MAPPING"]

    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]

        df_schema = StructType([
            StructField("condition_type", StringType()),
            StructField("gl", StringType()),
            StructField("gl_description", StringType()),
            StructField("posted_where", StringType()),
            StructField("purpose", StringType()),
            StructField("ciw_bucket", StringType()),
            StructField("cdl_dttm", StringType()),
            StructField("curr_date", TimestampType())
        ])
        
        df = session.read\\
        .schema(df_schema)\\
        .option("skip_header",1)\\
        .option("field_delimiter", "\\u0001")\\
        .option("field_optionally_enclosed_by", "\\"") \\
        .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        df = df\\
            .withColumn("cdl_dttm", lit(None))\\
            .with_column("curr_date",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))\\
            .with_column("file_name",lit(file_name))\\
            .with_column("run_id", lit(None).cast(DecimalType(14,0)))
        

        #deleteing the data from table 
        
        if df.count()==0 :
            return "No Data in table"

        #del_sql = "DELETE FROM " + stage_name.split(".")[0]+"."+target_table
        #session.sql(del_sql).collect()
    

        snowdf=df.select(''condition_type'', ''gl'', ''gl_description'', ''posted_where'', ''purpose'', ''ciw_bucket'', ''cdl_dttm'', ''curr_date'', ''file_name'', ''run_id'')
        snowdf = snowdf.filter(
                                ~(
                                    (snowdf["condition_type"].isNull()) &
                                    (snowdf["gl"].isNull()) &
                                    (snowdf["gl_description"].isNull()) &
                                    (snowdf["posted_where"].isNull()) &
                                    (snowdf["purpose"].isNull()) &
                                    (snowdf["ciw_bucket"].isNull()) &
                                    (snowdf["cdl_dttm"].isNull()) &
                                    (snowdf["curr_date"].isNull()) &
                                    (snowdf["file_name"].isNull()) &
                                    (snowdf["run_id"].isNull())
                                )
                            )
        
        #move file into success folder
        file_name=file_name.split(".")[0]+"_"+datetime.now().strftime("%Y%m%d%H%M%S")
        snowdf.write.copy_into_location("@"+Param[1]+"/"+Param[2]+"/success/"+file_name,header=True,OVERWRITE=True)

        #write on sdl layer
    
        snowdf.write.mode("append").saveAsTable(stage_name.split(".")[0]+"."+target_table)
        
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
