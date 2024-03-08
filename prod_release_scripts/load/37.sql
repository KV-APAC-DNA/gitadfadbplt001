CREATE OR REPLACE PROCEDURE SGPSDL_RAW.SG_AMAZON_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS 'from snowflake.snowpark.functions import col,lit,date_format,current_timestamp,to_date,year,month,concat,format_number,to_timestamp
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField,DecimalType
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col
import pytz

def main(session: snowpark.Session,Param): 
    try:
        #Param= [''Amazon_202312.csv'',''SGPSDL_RAW.DEV_LOAD_STAGE_ADLS'',''dev'',''sdl_sg_scan_data_amazon'']
        
    # Extracting parameters from the input
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
    
        df_schema = StructType([
            StructField("rm", StringType()),
            StructField("merchant_customer_id", StringType()),
            StructField("gl", StringType()),
            StructField("category", StringType()),
            StructField("subcategory", StringType()),
            StructField("brand", StringType()),
            StructField("item_code", StringType()),
            StructField("item_desc", StringType()),
            StructField("net_sales", DecimalType(10, 4)),
            StructField("pcogs", DecimalType(10, 4)),
            StructField("SALES_QTY", DecimalType(10, 0)),
            StructField("ppmpercent", DecimalType(10, 5)),
            StructField("ppmdollar", DecimalType(10, 5)),
            StructField("month", IntegerType()),
            StructField("year", IntegerType()),
            StructField("vendor_code", StringType()),
            StructField("vendor_name", StringType())
        ]
        )
    
        df = session.read\\
        .schema(df_schema)\\
        .option("skip_header",1)\\
        .option("field_delimiter", "\\u0001")\\
        .option("field_optionally_enclosed_by", "\\"") \\
        .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
        

        df=df.with_column("trx_date",lit(None).cast("date"))
        df=df.with_column("store",lit("Amazon"))
        df=df.with_column("store_name",lit("Amazon"))
        df=df.with_column("cdl_dttm",lit(None))
        #convertin time stamp into sg timezone
        df=df.with_column("crtd_dttm",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        df=df.with_column("file_name",lit(file_name))
        df=df.with_column("run_id",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        #df=df.with_column("run_id",col("run_id").cast(DecimalType(14,0)))

         

        #del_sql = "DELETE FROM " + stage_name.split(".")[0]+"."+target_table
        #session.sql(del_sql).collect()
    

        snowdf=df.select("trx_date","rm","merchant_customer_id","gl","category","subcategory","brand","item_code","item_desc","net_sales","pcogs","sales_qty","ppmpercent","ppmdollar","month","year","vendor_code","vendor_name","store","store_name","cdl_dttm","crtd_dttm","file_name","run_id")
        snowdf = snowdf.filter(
                                ~(
                                    (snowdf["trx_date"].isNull()) &
                                    (snowdf["rm"].isNull()) &
                                    (snowdf["merchant_customer_id"].isNull()) &
                                    (snowdf["gl"].isNull()) &
                                    (snowdf["category"].isNull()) &
                                    (snowdf["subcategory"].isNull()) &
                                    (snowdf["brand"].isNull()) &
                                    (snowdf["item_code"].isNull()) &
                                    (snowdf["item_desc"].isNull()) &
                                    (snowdf["net_sales"].isNull()) &
                                    (snowdf["pcogs"].isNull()) &
                                    (snowdf["sales_qty"].isNull()) &
                                    (snowdf["ppmpercent"].isNull()) &
                                    (snowdf["ppmdollar"].isNull()) &
                                    (snowdf["month"].isNull()) &
                                    (snowdf["year"].isNull()) &
                                    (snowdf["vendor_code"].isNull()) &
                                    (snowdf["vendor_name"].isNull()) &
                                    (snowdf["store"].isNull()) &
                                    (snowdf["store_name"].isNull()) &
                                    (snowdf["cdl_dttm"].isNull()) &
                                    (snowdf["crtd_dttm"].isNull()) &
                                    (snowdf["file_name"].isNull()) &
                                    (snowdf["run_id"].isNull())
                                )
                            )


        if snowdf.count()==0 :
            return "No Data in file" 
            

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
