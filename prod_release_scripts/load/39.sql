CREATE OR REPLACE PROCEDURE SGPSDL_RAW.SG_DFI_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS 'from snowflake.snowpark.functions import col, lit, date_format, current_timestamp, to_date, year, month, concat, format_number, regexp_replace,to_timestamp,when,trim,upper
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DecimalType,DateType
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz

def main(session: snowpark.Session,Param):
    try: 
        #Param=["DFI_202312_new.csv",''SGPSDL_RAW.DEV_LOAD_STAGE_ADLS'',''dev/scan360/dfi'',''sdl_sg_scan_data_dfi'']
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        
        df_schema = StructType([
            StructField("TRXDATE", StringType()),
            StructField("BUYERCODE", StringType()),
            StructField("VENDORCODE", StringType()),
            StructField("STORECODE", StringType()),
            StructField("STORESHORTCODE", StringType()),
            StructField("STOREDESC", StringType()),
            StructField("BRAND", StringType()),
            StructField("ITEMCODE", StringType()),
            StructField("SUPPLIERITEMCODE", StringType()),
            StructField("ITEMDESC", StringType()),
            StructField("SIZE", StringType()),
            StructField("UOM", StringType()),
            StructField("PUF", StringType()),
            StructField("BARCODE", StringType()),
            StructField("SALESAMOUNT", DecimalType(10, 2)),  
            StructField("SALESQTY", DecimalType(10, 2))
        ])
        df = session.read\\
        .schema(df_schema)\\
        .option("skip_header",1)\\
        .option("field_delimiter", ",")\\
        .option("field_optionally_enclosed_by", "\\"") \\
        .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)


        df = df.withColumn("CUST_NAME", when(col("STORECODE").like("G%"),"Giant").otherwise(
                                      when(col("STORECODE").like("Q%"), "Cold Storage").otherwise("711")))
        df = df.with_column("cdl_dttm", lit(None))
        df = df.with_column("run_id", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S"))) 
        df=df.with_column("file_name",lit(file_name))
        df=df.with_column("crtd_dttm",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        df = df.withColumn("TRXDATE", regexp_replace(col("TRXDATE"), "\\\\.", "-").cast(DateType()))
        

    

        snowdf=df.select("trxdate", "buyercode", "vendorcode", "storecode", "storeshortcode", "storedesc", "brand", "itemcode", "supplieritemcode", "itemdesc", "size", "uom", "puf", "barcode", "salesamount", "salesqty", "CUST_NAME", "cdl_dttm", "crtd_dttm", "file_name", "run_id")
        
        snowdf = snowdf.filter(
                                ~(
                                    (snowdf["trxdate"].isNull()) &
                                    (snowdf["buyercode"].isNull()) &
                                    (snowdf["vendorcode"].isNull()) &
                                    (snowdf["storecode"].isNull()) &
                                    (snowdf["storeshortcode"].isNull()) &
                                    (snowdf["storedesc"].isNull()) &
                                    (snowdf["brand"].isNull()) &
                                    (snowdf["itemcode"].isNull()) &
                                    (snowdf["supplieritemcode"].isNull()) &
                                    (snowdf["itemdesc"].isNull()) &
                                    (snowdf["size"].isNull()) &
                                    (snowdf["uom"].isNull()) &
                                    (snowdf["puf"].isNull()) &
                                    (snowdf["barcode"].isNull()) &
                                    (snowdf["salesamount"].isNull()) &
                                    (snowdf["salesqty"].isNull()) &
                                    (snowdf["CUST_NAME"].isNull()) &
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
        return error_message';
