CREATE OR REPLACE PROCEDURE SGPSDL_RAW.SG_GUARDIAN_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them.

from snowflake.snowpark.functions import col, lit, date_format, current_timestamp, to_date, year, month, concat, format_number, to_timestamp
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DecimalType,DateType
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz

def main(session: snowpark.Session, Param):
    try:
        #Param=["Guardian_202312.csv", "SBX_DNA_LAB1.NBANGA01_RAW.DEV_LOAD_STAGE_ADLS_2","test5",s]
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        
        # Define the schema
        df_schema = StructType([
				StructField("trxdate", DateType()),
				StructField("buyercode", StringType()),
				StructField("vendorcode", StringType()),
				StructField("storecode", StringType()),
				StructField("storeshortcode", StringType()),
				StructField("storepostalcode", StringType()),
				StructField("storeaddress1", StringType()),
				StructField("storeaddress2", StringType()),
				StructField("storeaddress3", StringType()),
				StructField("storecountry", StringType()),
				StructField("storedesc", StringType()),
				StructField("brand", StringType()),
				StructField("itemcode", StringType()),
				StructField("supplieritemcode", StringType()),
				StructField("itemdesc", StringType()),
				StructField("size", StringType()),
				StructField("uom", StringType()),
				StructField("puf", DecimalType(10, 0)),
				StructField("salesqty", DecimalType(10, 0)),
				StructField("salesamount", StringType()),
				StructField("inventoryonhand", DecimalType(10, 0)),
				StructField("barcode", StringType()),
				StructField("barcode2", StringType()),
				StructField("barcode3", StringType()),
				StructField("barcode4", StringType()),
				StructField("barcode5", StringType()),
				StructField("barcode6", StringType()),
				StructField("barcode7", StringType()),
				StructField("barcode8", StringType()),
				StructField("barcode9", StringType()),
				StructField("barcode10", StringType())
                ])
        
        df = session.read \\
            .schema(df_schema)\\
            .option("skip_header", 1)\\
            .option("field_delimiter", ",")\\
            .option("field_optionally_enclosed_by", "\\"") \\
            .csv("@" + stage_name + "/" + temp_stage_path + "/" + file_name)

        df = df.withColumn("CUST_NAME", lit("Guardian").cast("string"))
        df = df.with_column("cdl_dttm", lit(None).cast("string"))
        df=df.with_column("crtd_dttm",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        df=df.with_column("file_name",lit(file_name).cast("string"))
        df = df.with_column("run_id", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))


        snowdf=df.select("trxdate","buyercode","vendorcode","storecode","storeshortcode","storepostalcode","storeaddress1","storeaddress2","storeaddress3","storecountry","storedesc","brand","itemcode","supplieritemcode","itemdesc","size","uom","puf","salesqty","salesamount","inventoryonhand","barcode","CUST_NAME","cdl_dttm","crtd_dttm","file_name","run_id")
        snowdf= snowdf.filter(snowdf["storecode"].isNotNull()) and snowdf.filter(snowdf["trxdate"].isNotNull()) and snowdf.filter(snowdf["buyercode"].isNotNull()) and snowdf.filter(snowdf["vendorcode"].isNotNull()) and snowdf.filter(snowdf["storeshortcode"].isNotNull()) and snowdf.filter(snowdf["storepostalcode"].isNotNull()) and snowdf.filter(snowdf["storeaddress1"].isNotNull()) and snowdf.filter(snowdf["storeaddress2"].isNotNull()) and snowdf.filter(snowdf["storeaddress3"].isNotNull()) and snowdf.filter(snowdf["storecountry"].isNotNull()) and snowdf.filter(snowdf["brand"].isNotNull()) and snowdf.filter(snowdf["itemcode"].isNotNull()) and snowdf.filter(snowdf["supplieritemcode"].isNotNull()) and snowdf.filter(snowdf["itemdesc"].isNotNull()) and snowdf.filter(snowdf["size"].isNotNull()) and snowdf.filter(snowdf["uom"].isNotNull()) and snowdf.filter(snowdf["puf"].isNotNull()) and snowdf.filter(snowdf["salesqty"].isNotNull()) and snowdf.filter(snowdf["salesamount"].isNotNull()) and snowdf.filter(snowdf["inventoryonhand"].isNotNull()) and snowdf.filter(snowdf["barcode"].isNotNull())
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
