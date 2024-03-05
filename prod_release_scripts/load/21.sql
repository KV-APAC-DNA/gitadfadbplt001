CREATE OR REPLACE PROCEDURE MYSSDL_RAW.MY_SELLOUT_INV_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '

from snowflake.snowpark.functions import col, lit, date_format, current_timestamp, to_date, year, month, concat, format_number, regexp_replace,to_timestamp,when,trim,upper
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DecimalType,DateType
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
            StructField("DISTRIBUTOR_ID", StringType()),
            StructField("DATE", StringType()),
            StructField("DISTRIBUTOR_WH_ID", StringType()),
            StructField("SAP_MATERIAL_ID", StringType()),
            StructField("PRODUCT_CODE", StringType()),
            StructField("PRODUCT_EAN_CODE", StringType()),
            StructField("PRODUCT_DESCRIPTION", StringType()),
            StructField("QUANTITY_AVAILABLE", StringType()),
            StructField("UOM_AVAILABLE", StringType()),
            StructField("QUANTITY_ON_ORDER", StringType()),
            StructField("UOM_ON_ORDER", StringType()),
            StructField("QUANTITY_COMMITTED", StringType()),
            StructField("UOM_COMMITTED", StringType()),
            StructField("QUANTITY_AVAILABLE_IN_PIECES", StringType()),
            StructField("QUANTITY_ON_ORDER_IN_PIECES", StringType()),
            StructField("QUANTITY_COMMITTED_IN_PIECES", StringType()),
            StructField("UNIT_PRICE", StringType()),
            StructField("TOTAL_VALUE_AVAILABLE", StringType()),
            StructField("CUSTOM_FIELD_1", StringType()),
            StructField("CUSTOM_FIELD_2", StringType())
        ])

        df = session.read\\
        .schema(df_schema)\\
        .option("skip_header",0)\\
        .option("field_delimiter", ",")\\
        .option("encoding", "ISO-8859-15") \\
        .option("field_optionally_enclosed_by", "\\"") \\
        .option("null_if", "") \\
        .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        #df = df.with_column("run_id", lit(None).cast(DecimalType(14,0)))
        df = df.with_column("cdl_dttm", lit(datetime.now().strftime("%Y%m%d%H%M%S")))
        df=df.with_column("file_name",lit(file_name))
        df = df.withColumn("crtd_dttm", current_timestamp())

        snowdf=df.select(
                            "DISTRIBUTOR_ID",
                            "DATE",
                            "DISTRIBUTOR_WH_ID",
                            "SAP_MATERIAL_ID",
                            "PRODUCT_CODE",
                            "PRODUCT_EAN_CODE",
                            "PRODUCT_DESCRIPTION",
                            "QUANTITY_AVAILABLE",
                            "UOM_AVAILABLE",
                            "QUANTITY_ON_ORDER",
                            "UOM_ON_ORDER",
                            "QUANTITY_COMMITTED",
                            "UOM_COMMITTED",
                            "QUANTITY_AVAILABLE_IN_PIECES",
                            "QUANTITY_ON_ORDER_IN_PIECES",
                            "QUANTITY_COMMITTED_IN_PIECES",
                            "UNIT_PRICE",
                            "TOTAL_VALUE_AVAILABLE",
                            "CUSTOM_FIELD_1",
                            "CUSTOM_FIELD_2",
                            "FILE_NAME")
        
        snowdf= snowdf.filter(snowdf["DISTRIBUTOR_ID"].isNotNull())
        
        if snowdf.count()==0 :
            return "No Data in table"


        file_name1=("_").join(file_name.split("_")[0:5])+"_"+datetime.now().strftime("%Y%m%d%H%M%S")
        snowdf.write.copy_into_location("@"+Param[1]+"/"+Param[2]+"/success/"+file_name1,header=True,OVERWRITE=True)
        

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
