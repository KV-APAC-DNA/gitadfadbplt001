USE SCHEMA VNMSDL_RAW;

CREATE OR REPLACE PROCEDURE VNMSDL_RAW.SDL_VN_DMS_CUSTOMER_DIM("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS 'from snowflake.snowpark.functions import col, lit, current_timestamp, trim, upper,to_timestamp
from snowflake.snowpark.types import StringType, StructType, StructField, DecimalType
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark

def main(session:snowpark.Session,Param):
    
    try:

        #Param=[''OUT_CON_C_VN_20240415000502.csv'',''VNMSDL_RAW.DEV_LOAD_STAGE_ADLS'',''dev/dms/dms_source/'',''sdl_vn_dms_customer_dim'']

        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]

        df_schema = StructType([
            StructField("SAP_ID", StringType()),
            StructField("DSTRBTR_ID", StringType()),
            StructField("CNTRY_CODE", StringType()),
            StructField("OUTLET_ID", StringType()),
            StructField("OUTLET_NAME", StringType()),
            StructField("ADDRESS_1", StringType()),
            StructField("ADDRESS_2", StringType()),
            StructField("SHOP_TYPE", StringType()),
            StructField("TELEPHONE", StringType()),
            StructField("FAX", StringType()),
            StructField("CITY", StringType()),
            StructField("POSTCODE", StringType()),
            StructField("REGION", StringType()),
            StructField("CHANNEL_GROUP", StringType()),
            StructField("SUB_CHANNEL", StringType()),
            StructField("SALES_ROUTE_ID", StringType()),
            StructField("SALES_ROUTE_NAME", StringType()),
            StructField("SALES_GROUP", StringType()),
            StructField("SALESREP_ID", StringType()),
            StructField("SALESREP_NAME", StringType()),
            StructField("GPS_LAT", StringType()),
            StructField("GPS_LONG", StringType()),
            StructField("STATUS", StringType()),
            StructField("DISTRICT", StringType()),
            StructField("PROVINCE", StringType()),
            StructField("CRT_DATE", StringType()),
            StructField("DATE_OFF", StringType())
        ])

        df = session.read\\
        .schema(df_schema)\\
        .option("skip_header",1)\\
        .option("field_delimiter", "|")\\
        .option("encoding", "UTF-8") \\
        .option("field_optionally_enclosed_by", "\\"") \\
        .option("null_if", "") \\
        .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        df = df.na.drop("all")
        
        if  df.count() == 0:
            return "No Data in file"

        # Adding transformations for specific columns if needed
        df=  df.with_column("CURR_DATE", lit(to_timestamp(current_timestamp())))\\
               .with_column("RUN_ID", lit(None).cast(DecimalType(14,0)))\\
               .with_column("SOURCE_FILE_NAME", lit(file_name))

        snowdf = df.select(
            	"SAP_ID",
            	"DSTRBTR_ID",
            	"CNTRY_CODE",
            	"OUTLET_ID",
            	"OUTLET_NAME",
            	"ADDRESS_1",
            	"ADDRESS_2",
            	"TELEPHONE",
            	"FAX",
            	"CITY",
            	"POSTCODE",
            	"REGION",
            	"CHANNEL_GROUP",
            	"SUB_CHANNEL",
            	"SALES_ROUTE_ID",
            	"SALES_ROUTE_NAME",
            	"SALES_GROUP",
            	"SALESREP_ID",
            	"SALESREP_NAME",
            	"GPS_LAT",
            	"GPS_LONG",
            	"STATUS",
            	"DISTRICT",
            	"PROVINCE",
            	"CRT_DATE",
            	"DATE_OFF",
            	"CURR_DATE",
            	"RUN_ID",
            	"SOURCE_FILE_NAME",
            	"SHOP_TYPE"
        )

       
        file_name1 = ("_").join(file_name.split("_")[0:5]) + "_" + datetime.now().strftime("%Y%m%d%H%M%S")
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")


        # Write operation
        snowdf.write.mode("append").save_as_table(stage_name.split(".")[0]+"."+target_table)

        #move to success
        snowdf.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name1,header=True,OVERWRITE=True)

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
        return error_message';
