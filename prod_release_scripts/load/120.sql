CREATE OR REPLACE PROCEDURE VNMSDL_RAW.SDL_VN_DMS_CALL_DETAILS("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS CALLER
AS '
from snowflake.snowpark.functions import col, lit, current_timestamp, trim, upper
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DecimalType,DateType
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
def main(session:snowpark.Session, Param):
    try:
        file_name       = Param[0]  # File name
        stage_name      = Param[1]  # Stage name where files are located
        temp_stage_path = Param[2]  # Path in the stage
        target_table    = Param[3]  # Target table for the data
        df_schema = StructType([
            StructField("DSTRBTR_ID", StringType()),
            StructField("SALESREP_ID", StringType()),
            StructField("OUTLET_ID", StringType()),
            StructField("VISIT_DATE", StringType()),
            StructField("CHECKIN_TIME", StringType()),
            StructField("CHECKOUT_TIME", StringType()),
            StructField("REASON", StringType()),
            StructField("DISTANCE", StringType()),
            StructField("ORDERVISIT", StringType())
        ])
        df = session.read\\
        .schema(df_schema)\\
        .option("skip_header",1)\\
        .option("field_delimiter", "|")\\
        .option("encoding", "ISO-8859-15") \\
        .option("field_optionally_enclosed_by", "\\"") \\
        .option("null_if", "") \\
        .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
        # Perform necessary transformations
        df = df.with_column("ORDERVISIT", trim(col("ORDERVISIT"), lit('','')))\\
               .with_column("CURR_DATE", current_timestamp())\\
               .with_column("RUN_ID", lit(None).cast(DecimalType(14,0)))\\
               .with_column("SOURCE_FILE_NAME", lit(file_name))
        snowdf = df.select(
            "DSTRBTR_ID",
            "SALESREP_ID",
            "OUTLET_ID",
            "VISIT_DATE",
            "CHECKIN_TIME",
            "CHECKOUT_TIME",
            "REASON",
            "DISTANCE",
            "ORDERVISIT",
            "CURR_DATE",
            "RUN_ID",
            "SOURCE_FILE_NAME"
        )
        df = df.na.drop("all")
        
        if df.count() == 0:
            return "No Data in file"
            
        # Archive successful loads for auditability
        file_name1 = ("_").join(file_name.split("_")[0:5]) + "_" + datetime.now().strftime("%Y%m%d%H%M%S")
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        
        #move to success
        snowdf.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name1,header=True,OVERWRITE=True)
        # Write operation to append data to the target table
        snowdf.write.mode("append").save_as_table(stage_name.split(".")[0]+"."+target_table)
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



CREATE OR REPLACE PROCEDURE VNMSDL_RAW.SDL_VN_DMS_DATA_EXTRACT_SUMMARY("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS CALLER
AS '

from snowflake.snowpark.functions import col, lit, current_timestamp, trunc
from snowflake.snowpark.types import StringType, StructType, StructField
from datetime import datetime
import snowflake.snowpark as snowpark

def main(session:snowpark.Session, Param):

    try:
        file_name       = Param[0]  # File name
        stage_name      = Param[1]  # Stage name where files are located
        temp_stage_path = Param[2]  # Path in the stage
        target_table    = Param[3]  # Target table for the data

        # Define the schema based on the table structure
        df_schema = StructType([
            StructField("SOURCE_FILE_NAME", StringType()),
            StructField("DATE_OF_EXTRACTION", StringType()),
            StructField("RECORD_COUNT", StringType())
        ])

        df = session.read\\
        .schema(df_schema)\\
        .option("skip_header",1)\\
        .option("field_delimiter", "|")\\
        .option("encoding", "ISO-8859-15") \\
        .option("field_optionally_enclosed_by", "\\"") \\
        .option("null_if", "") \\
        .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        snowdf = df.select(
            "SOURCE_FILE_NAME",
            "DATE_OF_EXTRACTION",
            "RECORD_COUNT"
        )
        # Check if DataFrame is empty
        df = df.na.drop("all")
        
        if df.count() == 0:
            return "No Data in file"

        # Archive successful loads for auditability
        file_name1 = ("_").join(file_name.split("_")[0:5]) + "_" + datetime.now().strftime("%Y%m%d%H%M%S")
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        
        #move to success
        snowdf.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name1,header=True,OVERWRITE=True)
        
        # Write operation to append data to the target table
        snowdf.write.mode("append").save_as_table(stage_name.split(".")[0]+"."+target_table)
        return "Success"

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



CREATE OR REPLACE PROCEDURE VNMSDL_RAW.SDL_VN_DMS_ORDER_PROMOTION("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS CALLER
AS '
from snowflake.snowpark.functions import col, lit, current_timestamp, trim, upper
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DecimalType,DateType
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
def main(session:snowpark.Session, Param):
    try:
        file_name       = Param[0]  # File name
        stage_name      = Param[1]  # Stage name where files are located
        temp_stage_path = Param[2]  # Path in the stage
        target_table    = Param[3]  # Target table for the data
        df_schema = StructType([
            StructField("BRANCH_ID", StringType()),
            StructField("PRO_ID", StringType()),
            StructField("ORD_NO", StringType()),
            StructField("LINE_REF", StringType()),
            StructField("DISC_TYPE", StringType()),
            StructField("BREAK_BY", StringType()),
            StructField("DISC_BREAK_LINE_REF", StringType()),
            StructField("DISCT_BL_AMT", StringType()),
            StructField("DISCT_BL_QTY", StringType()),
            StructField("FREE_ITEM_CODE", StringType()),
            StructField("FREE_ITEM_QTY", StringType()),
            StructField("DISC_AMT", StringType())
        ])
        df = session.read\\
        .schema(df_schema)\\
        .option("skip_header",1)\\
        .option("field_delimiter", "|")\\
        .option("encoding", "ISO-8859-15") \\
        .option("field_optionally_enclosed_by", "\\"") \\
        .option("null_if", "") \\
        .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
        # Perform necessary transformations
        df = df.with_column("CURR_DATE", current_timestamp())\\
               .with_column("RUN_ID", lit(None).cast(DecimalType(14,0)))\\
               .with_column("SOURCE_FILE_NAME", lit(file_name))
        snowdf = df.select(
            "BRANCH_ID",
            "PRO_ID",
            "ORD_NO",
            "LINE_REF",
            "DISC_TYPE",
            "BREAK_BY",
            "DISC_BREAK_LINE_REF",
            "DISCT_BL_AMT",
            "DISCT_BL_QTY",
            "FREE_ITEM_CODE",
            "FREE_ITEM_QTY",
            "DISC_AMT",
            "CURR_DATE",
            "RUN_ID",
            "SOURCE_FILE_NAME"
        )
        df = df.na.drop("all")
        
        if df.count() == 0:
            return "No Data in file"
        # Archive successful loads for auditability
        file_name1 = ("_").join(file_name.split("_")[0:5]) + "_" + datetime.now().strftime("%Y%m%d%H%M%S")
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        
        #move to success
        snowdf.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name1,header=True,OVERWRITE=True)
        
        # Write operation to append data to the target table
        snowdf.write.mode("append").save_as_table(stage_name.split(".")[0]+"."+target_table)
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



CREATE OR REPLACE PROCEDURE VNMSDL_RAW.SDL_VN_DMS_SELLTHRGH_SALES_FACT("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS CALLER
AS '
from snowflake.snowpark.functions import col, lit, current_timestamp, trim, upper
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DecimalType,DateType
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
def main(session:snowpark.Session, Param):
    try:
        file_name       = Param[0]  # File name
        stage_name      = Param[1]  # Stage name where files are located
        temp_stage_path = Param[2]  # Path in the stage
        target_table    = Param[3]  # Target table for the data
        df_schema = StructType([
            StructField("DSTRBTR_ID", StringType()),
            StructField("DSTRBTR_TYPE", StringType()),
            StructField("MAPPED_SPK", StringType()),
            StructField("DOC_NUMBER", StringType()),
            StructField("REF_NUMBER", StringType()),
            StructField("RECEIPT_DATE", StringType()),
            StructField("ORDER_TYPE", StringType()),
            StructField("VAT_INVOICE_NUMBER", StringType()),
            StructField("VAT_INVOICE_NOTE", StringType()),
            StructField("VAT_INVOICE_DATE", StringType()),
            StructField("PON_NUMBER", StringType()),
            StructField("LINE_REF", StringType()),
            StructField("PRODUCT_CODE", StringType()),
            StructField("UNIT", StringType()),
            StructField("QUANTITY", StringType()),
            StructField("PRICE", StringType()),
            StructField("AMOUNT", StringType()),
            StructField("TAX_AMOUNT", StringType()),
            StructField("TAX_ID", StringType()),
            StructField("TAX_RATE", StringType()),
            StructField("VALUES", StringType()),
            StructField("LINE_DISCOUNT", StringType()),
            StructField("DOC_DISCOUNT", StringType()),
            StructField("STATUS", StringType())
        ])
        df = session.read\\
        .schema(df_schema)\\
        .option("skip_header",1)\\
        .option("field_delimiter", "|")\\
        .option("encoding", "ISO-8859-15") \\
        .option("field_optionally_enclosed_by", "\\"") \\
        .option("null_if", "") \\
        .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
        # Perform necessary transformations
        df = df.with_column("CURR_DATE", current_timestamp())\\
               .with_column("RUN_ID", lit(None).cast(DecimalType(14,0)))\\
               .with_column("SOURCE_FILE_NAME", lit(file_name))
        snowdf = df.select(
            "DSTRBTR_ID",
            "DSTRBTR_TYPE",
            "MAPPED_SPK",
            "DOC_NUMBER",
            "REF_NUMBER",
            "RECEIPT_DATE",
            "ORDER_TYPE",
            "VAT_INVOICE_NUMBER",
            "VAT_INVOICE_NOTE",
            "VAT_INVOICE_DATE",
            "PON_NUMBER",
            "LINE_REF",
            "PRODUCT_CODE",
            "UNIT",
            "QUANTITY",
            "PRICE",
            "AMOUNT",
            "TAX_AMOUNT",
            "TAX_ID",
            "TAX_RATE",
            "VALUES",
            "LINE_DISCOUNT",
            "DOC_DISCOUNT",
            "STATUS",
            "CURR_DATE",
            "RUN_ID",
            "SOURCE_FILE_NAME"
        )
        # Check if DataFrame is empty
        df = df.na.drop("all")
        
        if df.count() == 0:
            return "No Data in file"
        # Archive successful loads for auditability
        file_name1 = ("_").join(file_name.split("_")[0:5]) + "_" + datetime.now().strftime("%Y%m%d%H%M%S")
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        
        #move to success
        snowdf.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name1,header=True,OVERWRITE=True)
        # Write operation to append data to the target table
        snowdf.write.mode("append").save_as_table(stage_name.split(".")[0]+"."+target_table)
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



CREATE OR REPLACE PROCEDURE VNMSDL_RAW.SDL_VN_DMS_SALES_STOCK_FACT("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS CALLER
AS '
from snowflake.snowpark.functions import col, lit, current_timestamp, trim, upper
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DecimalType,DateType
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
def main(session:snowpark.Session, Param):
    try:
        file_name       = Param[0]  # File name
        stage_name      = Param[1]  # Stage name where files are located
        temp_stage_path = Param[2]  # Path in the stage
        target_table    = Param[3]  # Target table for the data
        df_schema = StructType([
            StructField("DSTRBTR_ID", StringType()),
            StructField("CNTRY_CODE", StringType()),
            StructField("WH_CODE", StringType()),
            StructField("DATE", StringType()),
            StructField("MATERIAL_CODE", StringType()),
            StructField("BAT_NUMBER", StringType()),
            StructField("EXPIRY_DATE", StringType()),
            StructField("QUANTITY", StringType()),
            StructField("UOM", StringType()),
            StructField("AMOUNT", StringType())
        ])
        df = session.read\\
        .schema(df_schema)\\
        .option("skip_header",1)\\
        .option("field_delimiter", "|")\\
        .option("encoding", "ISO-8859-15") \\
        .option("field_optionally_enclosed_by", "\\"") \\
        .option("null_if", "") \\
        .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
        # Perform necessary transformations
        df = df.with_column("CURR_DATE", current_timestamp())\\
               .with_column("RUN_ID", lit(None).cast(DecimalType(14,0)))\\
               .with_column("SOURCE_FILE_NAME", lit(file_name))
        snowdf = df.select(
            "DSTRBTR_ID",
            "CNTRY_CODE",
            "WH_CODE",
            "DATE",
            "MATERIAL_CODE",
            "BAT_NUMBER",
            "EXPIRY_DATE",
            "QUANTITY",
            "UOM",
            "AMOUNT",
            "CURR_DATE",
            "RUN_ID",
            "SOURCE_FILE_NAME"
        )
        # Check if DataFrame is empty
        df = df.na.drop("all")
        
        if df.count() == 0:
            return "No Data in file"
        # Archive successful loads for auditability
        file_name1 = ("_").join(file_name.split("_")[0:5]) + "_" + datetime.now().strftime("%Y%m%d%H%M%S")
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        
        #move to success
        snowdf.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name1,header=True,OVERWRITE=True)
        # Write operation to append data to the target table
        snowdf.write.mode("append").save_as_table(stage_name.split(".")[0]+"."+target_table)
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




CREATE OR REPLACE PROCEDURE VNMSDL_RAW.SDL_VN_DMS_CUSTOMER_DIM("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS CALLER
AS '

from snowflake.snowpark.functions import col, lit, current_timestamp, trim, upper
from snowflake.snowpark.types import StringType, StructType, StructField, DecimalType
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark

def main(session:snowpark.Session, Param):
    
    try:

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
        .option("encoding", "ISO-8859-15") \\
        .option("field_optionally_enclosed_by", "\\"") \\
        .option("null_if", "") \\
        .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        df = df.na.drop("all")
        
        if  df.count() == 0:
            return "No Data in file"

        # Adding transformations for specific columns if needed
        df = df.with_column("PROVINCE", trim(upper(col("PROVINCE"))))\\
               .with_column("CURR_DATE", lit(datetime.now().strftime("%Y%m%d%H%M%S")))\\
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
       
       #move to success
        snowdf.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name1,header=True,OVERWRITE=True)


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




CREATE OR REPLACE PROCEDURE VNMSDL_RAW.SDL_VN_DMS_KPI("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS CALLER
AS '
from snowflake.snowpark.functions import col, lit, current_timestamp, trim, upper
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DecimalType,DateType
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
def main(session:snowpark.Session, Param):
    try:
        file_name       = Param[0]  # File name
        stage_name      = Param[1]  # Stage name where files are located
        temp_stage_path = Param[2]  # Path in the stage
        target_table    = Param[3]  # Target table for the data
        df_schema = StructType([
            StructField("DSTRBTR_ID", StringType()),
            StructField("SALEMAN_CODE", StringType()),
            StructField("SALEMAN_NAME", StringType()),
            StructField("CYCLE", StringType()),
            StructField("EXPORT_DATE", StringType()),
            StructField("KPI_TYPE", StringType()),
            StructField("TARGET_VALUE", StringType()),
            StructField("ACTUAL_VALUE", StringType())
        ])
        df = session.read\\
        .schema(df_schema)\\
        .option("skip_header",1)\\
        .option("field_delimiter", "|")\\
        .option("encoding", "ISO-8859-15") \\
        .option("field_optionally_enclosed_by", "\\"") \\
        .option("null_if", "") \\
        .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
        # Perform necessary transformations, if any
        # Include conversion to correct data types if needed, here assuming all stays as string
        # Adding a timestamp and file name for tracking
        df = df.with_column("CURR_DATE", lit(datetime.now().strftime("%Y%m%d%H%M%S")))\\
               .with_column("RUN_ID", lit(None).cast(DecimalType(14,0)))\\
               .with_column("SOURCE_FILE_NAME", lit(file_name))
        snowdf = df.select(
            "DSTRBTR_ID",
            "SALEMAN_CODE",
            "SALEMAN_NAME",
            "CYCLE",
            "EXPORT_DATE",
            "KPI_TYPE",
            "TARGET_VALUE",
            "ACTUAL_VALUE",
            "CURR_DATE",
            "RUN_ID",
            "SOURCE_FILE_NAME"
        )
        # Check if DataFrame is empty
        df = df.na.drop("all")
        
        if df.count() == 0:
            return "No Data in file"
        # Archive successful loads for auditability
        file_name1 = ("_").join(file_name.split("_")[0:5]) + "_" + datetime.now().strftime("%Y%m%d%H%M%S")
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        
        #move to success
        snowdf.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name1,header=True,OVERWRITE=True)
        # Write operation to append data to the target table
        snowdf.write.mode("append").save_as_table(stage_name.split(".")[0]+"."+target_table)
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





CREATE OR REPLACE PROCEDURE VNMSDL_RAW.SDL_VN_DMS_PROMOTION_LIST("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS CALLER
AS '
from snowflake.snowpark.functions import col, lit, current_timestamp, trim, upper
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DecimalType,DateType
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
def main(session:snowpark.Session, Param):
    try:
        file_name       = Param[0]  # File name
        stage_name      = Param[1]  # Stage name where files are located
        temp_stage_path = Param[2]  # Path in the stage
        target_table    = Param[3]  # Target table for the data
        df_schema = StructType([
            StructField("DSTRBTR_ID", StringType()),
            StructField("CNTRY_CODE", StringType()),
            StructField("PROMOTION_ID", StringType()),
            StructField("PROMOTION_NAME", StringType()),
            StructField("PROMOTION_DESC", StringType()),
            StructField("START_DATE", StringType()),
            StructField("END_DATE", StringType()),
            StructField("STATUS", StringType())
        ])
        df = session.read\\
        .schema(df_schema)\\
        .option("skip_header",1)\\
        .option("field_delimiter", "|")\\
        .option("encoding", "ISO-8859-15") \\
        .option("field_optionally_enclosed_by", "\\"") \\
        .option("null_if", "") \\
        .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
        df = df.with_column("STATUS", trim(col("STATUS"), lit('','')))\\
               .with_column("CURR_DATE", current_timestamp())\\
               .with_column("RUN_ID", lit(None).cast(DecimalType(14,0)))\\
               .with_column("SOURCE_FILE_NAME", lit(file_name))
        snowdf = df.select(
            "DSTRBTR_ID",
            "CNTRY_CODE",
            "PROMOTION_ID",
            "PROMOTION_NAME",
            "PROMOTION_DESC",
            "START_DATE",
            "END_DATE",
            "STATUS",
            "CURR_DATE",
            "RUN_ID",
            "SOURCE_FILE_NAME"
        )
        # Check if DataFrame is empty
        df = df.na.drop("all")
        
        if df.count() == 0:
            return "No Data in file"
          
          # Archive successful loads for auditability
        file_name1 = ("_").join(file_name.split("_")[0:5]) + "_" + datetime.now().strftime("%Y%m%d%H%M%S")
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        
        #move to success
        snowdf.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name1,header=True,OVERWRITE=True)
        # Write operation to append data to the target table
        snowdf.write.mode("append").save_as_table(stage_name.split(".")[0]+"."+target_table)
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


CREATE OR REPLACE PROCEDURE VNMSDL_RAW.SDL_VN_DMS_PRODUCT_DIM("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS CALLER
AS '
from snowflake.snowpark.functions import col, lit, current_timestamp, trim, upper
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DecimalType,DateType
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
def main(session:snowpark.Session, Param):
    try:
        file_name       = Param[0]  # File name
        stage_name      = Param[1]  # Stage name where files are located
        temp_stage_path = Param[2]  # Path in the stage
        target_table    = Param[3]  # Target table for the data
        df_schema = StructType([
            StructField("PRODUCT_CODE", StringType()),
            StructField("PRODUCT_NAME", StringType()),
            StructField("PRODUCTCODESAP", StringType()),
            StructField("PRODUCTNAMESAP", StringType()),
            StructField("UNIT", StringType()),
            StructField("TAX_RATE", StringType()),
            StructField("WEIGHT", StringType()),
            StructField("VOLUME", StringType()),
            StructField("GROUPJB", StringType()),
            StructField("FRANCHISE", StringType()),
            StructField("BRAND", StringType()),
            StructField("VARIANT", StringType()),
            StructField("PRODUCT_GROUP", StringType()),
            StructField("ACTIVE", StringType())
        ])
        df = session.read\\
        .schema(df_schema)\\
        .option("skip_header",1)\\
        .option("field_delimiter", "|")\\
        .option("encoding", "ISO-8859-15") \\
        .option("field_optionally_enclosed_by", "\\"") \\
        .option("null_if", "") \\
        .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
        # Perform transformations
        df = df.with_column("ACTIVE", trim(upper(col("ACTIVE"))))\\
               .with_column("CURR_DATE", current_timestamp())\\
               .with_column("RUN_ID", lit(None).cast(DecimalType(14,0)))\\
               .with_column("SOURCE_FILE_NAME", lit(file_name))
        snowdf = df.select(
            "PRODUCT_CODE",
            "PRODUCT_NAME",
            "PRODUCTCODESAP",
            "PRODUCTNAMESAP",
            "UNIT",
            "TAX_RATE",
            "WEIGHT",
            "VOLUME",
            "GROUPJB",
            "FRANCHISE",
            "BRAND",
            "VARIANT",
            "PRODUCT_GROUP",
            "ACTIVE",
            "CURR_DATE",
            "RUN_ID",
            "SOURCE_FILE_NAME"
        )
        # Check if DataFrame is empty
        df = df.na.drop("all")
        
        if df.count() == 0:
            return "No Data in file"
        # Archive successful loads for auditability
        file_name1 = ("_").join(file_name.split("_")[0:5]) + "_" + datetime.now().strftime("%Y%m%d%H%M%S")
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        
        #move to success
        snowdf.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name1,header=True,OVERWRITE=True)
        # Write operation to append data to the target table
        snowdf.write.mode("append").save_as_table(stage_name.split(".")[0]+"."+target_table)
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


CREATE OR REPLACE PROCEDURE VNMSDL_RAW.VN_INTERFACE_ANSWERS_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS CALLER
AS 'from snowflake.snowpark.functions import col, lit, date_format, current_timestamp, to_date, year, month, concat, format_number, regexp_replace,to_timestamp,when,trim,upper
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DecimalType
import snowflake.snowpark
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz
import pandas as pd

def main(session: snowpark.Session,Param):
    #Param=["Interface_Answers_20220905.csv","VNMSDL_RAW.DEV_LOAD_STAGE_ADLS","dev/VN_PS/transaction/Interface_Answers","sdl_vn_interface_answers"]
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        
        df_schema = StructType([
                StructField("cust_code", StringType(), True),
                StructField("slsper_id", StringType(), True),
                StructField("shop_code", StringType(), True),
                StructField("ise_id", StringType(), True),
                StructField("ques_no", StringType(), True),
                StructField("answer_seq", StringType(), True),
                StructField("answer_value", StringType(), True),
                StructField("score", StringType(), True),
                StructField("oos", StringType(), True),
                StructField("createddate", StringType(), True)
            ])
        
        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "|")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

       
        
        df=df.na.drop("all")
        if df.count()==0 :
            return "No Data in file"

        columns_to_check = ["ise_id", "slsper_id", "cust_code", "ques_no", "answer_seq", "createddate", "shop_code"]
        
        for i in columns_to_check:
            df_na=df.na.drop(subset=[i])
            if df_na.count()!=df.count():
                return f"null value in column {i}"


        df_dup1=df.select("ise_id", "slsper_id", "cust_code", "ques_no", "shop_code", "answer_seq", "createddate")
        
            
        df_dup2 = df_dup1.dropDuplicates()
        
        if df_dup1.count() != df_dup2.count():
            return "Duplicate values"
            
        df = df.dropDuplicates()
        df = df.with_column("run_id", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        file_name1=file_name.split(".")[0]+"_"+ datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")+".csv"
        df = df.with_column("file_name", lit(file_name1))
        df = df.with_column("crtd_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))

        snowdf= df.select(
                    "cust_code",
                    "slsper_id",
                    "shop_code",
                    "ise_id",
                    "ques_no",
                    "answer_seq",
                    "answer_value",
                    "score",
                    "oos",
                    "createddate",
                    "file_name",
                    "crtd_dttm"
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


CREATE OR REPLACE PROCEDURE VNMSDL_RAW.VN_INTERFACE_BRANCH_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS CALLER
AS 'from snowflake.snowpark.functions import col, lit, date_format, current_timestamp, to_date, year, month, concat, format_number, regexp_replace,to_timestamp,when,trim,upper
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DecimalType
import snowflake.snowpark
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz
import pandas as pd

def main(session: snowpark.Session,Param):
    #Param= [''Interface_Branch_20220905.csv'',''VNMSDL_RAW.DEV_LOAD_STAGE_ADLS'',''dev/VN_PS/Interface_Branch'','''']
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        
        df_schema = StructType([
                                StructField("parent_cust_code", StringType(), True),
                                StructField("parent_cust_name", StringType(), True),
                                StructField("branch_code", StringType(), True),
                                StructField("branch_name", StringType(), True),
                                StructField("channel_code", StringType(), True),
                                StructField("channel_desc", StringType(), True),
                                StructField("sales_group", StringType(), True),
                                StructField("region", StringType(), True),
                                StructField("state", StringType(), True),
                                StructField("city", StringType(), True),
                                StructField("district", StringType(), True),
                                StructField("trade_type", StringType(), True),
                                StructField("store_prioritization", StringType(), True),
                                StructField("latitude", StringType(), True),
                                StructField("longitude", StringType(), True)
                            ])
        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "|")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
        df = df.na.drop("all")
        if df.count() == 0:
            return "No Data in file"

        columns_to_check = [''branch_code'', ''parent_cust_code'']
        
        for i in columns_to_check:
            df_na=df.na.drop(subset=[i])
            if df_na.count()!=df.count():
                return f"null value in column {i}"

        df_dup1=df.select("branch_code", "parent_cust_code", )
        
            
        df_dup2 = df_dup1.dropDuplicates()
        
        if df_dup1.count() != df_dup2.count():
            return "Duplicate values" 
        
            
        df = df.dropDuplicates()
        df = df.with_column("run_id", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        file_name1=file_name.split(".")[0] + "_" + datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")+".csv"
        df = df.with_column("file_name", lit(file_name1))
        df = df.with_column("crtd_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))

        snowdf = df.select(
                    "parent_cust_code",
                    "parent_cust_name",
                    "branch_code",
                    "branch_name",
                    "channel_code",
                    "channel_desc",
                    "sales_group",
                    "region",
                    "state",
                    "city",
                    "district",
                    "trade_type",
                    "store_prioritization",
                    "latitude",
                    "longitude",
                    "file_name",
                    "crtd_dttm"
                    )
        file_name = file_name.split(".")[0] + "_" + datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        
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


CREATE OR REPLACE PROCEDURE VNMSDL_RAW.VN_INTERFACE_CHOICE_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS CALLER
AS 'from snowflake.snowpark.functions import col, lit, date_format, current_timestamp, to_date, year, month, concat, format_number, regexp_replace,to_timestamp,when,trim,upper
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DecimalType
import snowflake.snowpark
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz
import pandas as pd

def main(session: snowpark.Session,Param):
    #Param= [''Interface_Choices_20240202.csv'',''VNMSDL_RAW.DEV_LOAD_STAGE_ADLS'',''dev/VN_PS/transaction/Interface_Choices'',''sdl_vn_interface_choices'']
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        
        df_schema = StructType([
                StructField("ise_id", StringType(), True),
                StructField("channel_id", StringType(), True),
                StructField("channel_name", StringType(), True),
                StructField("ques_no", StringType(), True),
                StructField("answer_seq", StringType(), True),
                StructField("sku_group", StringType(), True),
                StructField("rep_param", StringType(), True),
                StructField("putup_id", StringType(), True),
                StructField("description", StringType(), True),
                StructField("score", StringType(), True),
                StructField("sfa", StringType(), True),
                StructField("brand_id", StringType(), True),
                StructField("brand_name", StringType(), True)
            ])
        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "|")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
        df = df.na.drop("all")
        if df.count() == 0:
            return "No Data in file"

        columns_to_check = ["ise_id", "ques_no", "answer_seq"]
        
        for i in columns_to_check:
            df_na=df.na.drop(subset=[i])
            if df_na.count()!=df.count():
                return f"null value in column {i}"
            
        df_dup1=df.select("ise_id", "ques_no", "answer_seq")
        
            
        df_dup2 = df_dup1.dropDuplicates()
        
        if df_dup1.count() != df_dup2.count():
            return "Duplicate values" 
            
        df = df.with_column("run_id", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        file_name1=file_name.split(".")[0]+"_"+ datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")+".csv"
        df = df.with_column("file_name", lit(file_name1))
        df = df.with_column("crtd_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))

        snowdf = df.select(
                    "ise_id",
                    "channel_id",
                    "channel_name",
                    "ques_no",
                    "answer_seq",
                    "sku_group",
                    "rep_param",
                    "putup_id",
                    "description",
                    "score",
                    "sfa",
                    "brand_id",
                    "brand_name",
                    "file_name",
                    "crtd_dttm"
                    )
        file_name = file_name.split(".")[0] + "_" + datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        
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


CREATE OR REPLACE PROCEDURE VNMSDL_RAW.VN_INTERFACE_CPG_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS CALLER
AS 'from snowflake.snowpark.functions import col, lit, date_format, current_timestamp, to_date, year, month, concat, format_number, regexp_replace, to_timestamp, when, trim, upper
from snowflake.snowpark.types import StringType, StructType, StructField
import snowflake.snowpark
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz

def main(session: snowpark.Session,Param):
    #Param=["Interface_CPG_20220905.csv","VNMSDL_RAW.DEV_LOAD_STAGE_ADLS","dev/VN_PS/Interface_CPG",""]
    try:
        file_name = Param[0]
        stage_name = Param[1]
        temp_stage_path = Param[2]
        target_table = Param[3]
        
        df_schema = StructType([
            StructField("slsper_id", StringType(), True),
            StructField("branch_code", StringType(), True),
            StructField("createddate", StringType(), True),
            StructField("visitdate", StringType(), True),
        ])
        
        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "|")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
        
        df = df.na.drop("all")
        
        if df.count() == 0:
            return "No Data in file"

        columns_to_check = ["slsper_id","branch_code","visitdate"]
        
        for i in columns_to_check:
            df_na=df.na.drop(subset=[i])
            if df_na.count()!=df.count():
                return f"null value in column {i}"
            
        df_dup1=df.select("slsper_id","branch_code","visitdate" )
        
            
        df_dup2 = df_dup1.dropDuplicates()
        
        if df_dup1.count() != df_dup2.count():
            return "Duplicate values" 
            
        df = df.withColumn("run_id", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        file_name1=file_name.split(".")[0]+"_"+ datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")+".csv"
        df = df.withColumn("file_name", lit(file_name1))
        df = df.withColumn("crtd_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))

        snowdf = df.select(
            "slsper_id",
            "branch_code",
            "createddate",
            "visitdate",
            "file_name",
            "crtd_dttm"
        )
        
        file_name=file_name.split(".")[0]+"_"+ datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        
        #move to success
        snowdf.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)
        
        snowdf.write.mode("append").saveAsTable(stage_name.split(".")[0] + "." + target_table)
        
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


CREATE OR REPLACE PROCEDURE VNMSDL_RAW.VN_INTERFACE_CUSTOMERVISITED_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS CALLER
AS 'from snowflake.snowpark.functions import col, lit, date_format, current_timestamp, to_date, year, month, concat, format_number, regexp_replace, to_timestamp, when, trim, upper
from snowflake.snowpark.types import StringType, StructType, StructField
import snowflake.snowpark
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz

def main(session: snowpark.Session,Param):
    #Param=["Interface_CustomerVisited_20220905.csv","VNMSDL_RAW.DEV_LOAD_STAGE_ADLS","dev/VN_PS/Interface_CustomerVisited",""]
    try:
        file_name = Param[0]
        stage_name = Param[1]
        temp_stage_path = Param[2]
        target_table = Param[3]
        
        df_schema = StructType([
            StructField("cust_code", StringType(), True),
            StructField("slsper_id", StringType(), True),
            StructField("branch_code", StringType(), True),
            StructField("ise_id", StringType(), True),
            StructField("created_date", StringType(), True),
            StructField("visit_date", StringType(), True),
        ])
        
        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "|")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
        
        df = df.na.drop("all")
        
        if df.count() == 0:
            return "No Data in file"

        columns_to_check = [ "slsper_id", "cust_code", "ise_id", "branch_code", "visit_date"]
        
        for i in columns_to_check:
            df_na=df.na.drop(subset=[i])
            if df_na.count()!=df.count():
                return f"null value in column {i}"

        


        df_dup1=df.select( "slsper_id", "cust_code", "ise_id", "branch_code", "visit_date")
        
            
        df_dup2 = df_dup1.dropDuplicates()
        
        if df_dup1.count() != df_dup2.count():
            return "Duplicate values"
        
            
        df = df.withColumn("run_id", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        file_name1=file_name.split(".")[0]+"_"+ datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")+".csv"
        df = df.withColumn("file_name", lit(file_name1))
        df = df.withColumn("crtd_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))

        snowdf = df.select(
            "cust_code",
            "slsper_id",
            "branch_code",
            "ise_id",
            "created_date",
            "visit_date",
            "file_name",
            "crtd_dttm"
        )
        
        file_name=file_name.split(".")[0]+"_"+ datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        
        #move to success
        snowdf.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)
        
        snowdf.write.mode("append").saveAsTable(stage_name.split(".")[0] + "." + target_table)
        
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


CREATE OR REPLACE PROCEDURE VNMSDL_RAW.VN_ISE_HEADER_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS CALLER
AS 'from snowflake.snowpark.functions import col, lit, date_format, current_timestamp, to_date, year, month, concat, format_number, regexp_replace, to_timestamp, when, trim, upper
from snowflake.snowpark.types import StringType, StructType, StructField
import snowflake.snowpark
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz

def main(session: snowpark.Session,Param):
    #Param=["Interface_ISE_Header_20220905.csv","VNMSDL_RAW.DEV_LOAD_STAGE_ADLS","dev/VN_PS/Interface_ISEHeader",""]
    try:
        file_name = Param[0]
        stage_name = Param[1]
        temp_stage_path = Param[2]
        target_table = Param[3]
        
        df_schema = StructType([
            StructField("ise_id", StringType(), True),
            StructField("ise_desc", StringType(), True),
            StructField("channel_code", StringType(), True),
            StructField("channel_desc", StringType(), True),
            StructField("startdate", StringType(), True),
            StructField("enddate", StringType(), True),
        ])
        
        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "|")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
        
        df = df.na.drop("all")
        
        if df.count() == 0:
            return "No Data in file"

        columns_to_check = ["ise_id", "ise_desc"]
        
        for i in columns_to_check:
            df_na=df.na.drop(subset=[i])
            if df_na.count()!=df.count():
                return f"null value in column {i}"

        df_dup1 = df.select("ise_id", "ise_desc")
            
        df_dup2 = df_dup1.dropDuplicates()
        
        if df_dup1.count() != df_dup2.count():
            return "Duplicate values" 
            
        df = df.withColumn("run_id", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        file_name1=file_name.split(".")[0]+"_"+ datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")+".csv"
        df = df.withColumn("file_name", lit(file_name1))
        df = df.withColumn("crtd_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))

        snowdf = df.select(
            "ise_id",
            "ise_desc",
            "channel_code",
            "channel_desc",
            "startdate",
            "enddate",
            "file_name",
            "crtd_dttm"
        )
        
        file_name=file_name.split(".")[0]+"_"+ datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        
        #move to success
        snowdf.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)
        
        snowdf.write.mode("append").saveAsTable(stage_name.split(".")[0] + "." + target_table)
        
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


CREATE OR REPLACE PROCEDURE VNMSDL_RAW.VN_NOTES_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS CALLER
AS 'from snowflake.snowpark.functions import col, lit, date_format, current_timestamp, to_date, year, month, concat, format_number, regexp_replace, to_timestamp, when, trim, upper
from snowflake.snowpark.types import StringType, StructType, StructField
import snowflake.snowpark
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz

def main(session: snowpark.Session,Param):
    #Param=["Interface_Notes_20220905.csv","VNMSDL_RAW.DEV_LOAD_STAGE_ADLS","dev/VN_PS/Interface_Notes",""]
    try:
        file_name = Param[0]
        stage_name = Param[1]
        temp_stage_path = Param[2]
        target_table = Param[3]
        
        df_schema = StructType([
            StructField("cust_code", StringType(), True),
            StructField("slsper_id", StringType(), True),
            StructField("shop_code", StringType(), True),
            StructField("ise_id", StringType(), True),
            StructField("ques_no", StringType(), True),
            StructField("answer_seq", StringType(), True),
            StructField("answer_value", StringType(), True),
            StructField("createddate", StringType(), True),
        ])
        
        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "|")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
        
        df = df.na.drop("all")
        
        if df.count() == 0:
            return "No Data in file"

        columns_to_check = ["ise_id", "ques_no", "cust_code", "shop_code", "ques_no", "answer_seq", "createddate"]
        
        for i in columns_to_check:
            df_na=df.na.drop(subset=[i])
            if df_na.count()!=df.count():
                return f"null value in column {i}"

        df_dup1 = df.select("ise_id", "slsper_id", "cust_code", "shop_code", "ques_no", "answer_seq", "createddate")
            
        df_dup2 = df_dup1.dropDuplicates()
        
        if df_dup1.count() != df_dup2.count():
            return "Duplicate values" 
            
        df = df.withColumn("run_id", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        file_name1=file_name.split(".")[0]+"_"+ datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")+".csv"
        df = df.withColumn("file_name", lit(file_name1))
        df = df.withColumn("crtd_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))

        snowdf = df.select(
            "cust_code",
            "slsper_id",
            "shop_code",
            "ise_id",
            "ques_no",
            "answer_seq",
            "answer_value",
            "createddate",
            "file_name",
            "crtd_dttm"
        )
        
        file_name=file_name.split(".")[0]+"_"+ datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        
        #move to success
        snowdf.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)
        
        snowdf.write.mode("append").saveAsTable(stage_name.split(".")[0] + "." + target_table)
        
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


CREATE OR REPLACE PROCEDURE VNMSDL_RAW.VN_QUESTION_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS CALLER
AS 'from snowflake.snowpark.functions import col, lit, date_format, current_timestamp, to_date, year, month, concat, format_number, regexp_replace, to_timestamp, when, trim, upper
from snowflake.snowpark.types import StringType, StructType, StructField
import snowflake.snowpark
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz

def main(session: snowpark.Session,Param):
    #Param=["Interface_Question_20220905.csv","VNMSDL_RAW.DEV_LOAD_STAGE_ADLS","dev/VN_PS/Interface_Questions",""]
    try:
        file_name = Param[0]
        stage_name = Param[1]
        temp_stage_path = Param[2]
        target_table = Param[3]
        
        df_schema = StructType([
            StructField("ise_id", StringType(), True),
            StructField("channel_id", StringType(), True),
            StructField("channel", StringType(), True),
            StructField("ques_no", StringType(), True),
            StructField("ques_code", StringType(), True),
            StructField("ques_desc", StringType(), True),
            StructField("standard_ques", StringType(), True),
            StructField("ques_class_code", StringType(), True),
            StructField("ques_class_desc", StringType(), True),
            StructField("weigh", StringType(), True),
            StructField("total_score", StringType(), True),
            StructField("answer_code", StringType(), True),
            StructField("answer_desc", StringType(), True),
            StructField("franchise_code", StringType(), True),
            StructField("franchise_name", StringType(), True)
        ])
        
        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "|")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
        
        df = df.na.drop("all")
        
        if df.count() == 0:
            return "No Data in file"

        columns_to_check = ["ise_id", "ques_no"]
        
        for i in columns_to_check:
            df_na=df.na.drop(subset=[i])
            if df_na.count()!=df.count():
                return f"null value in column {i}"

        df_dup1 = df.select("ise_id", "ques_no")
            
        df_dup2 = df_dup1.dropDuplicates()
        
        if df_dup1.count() != df_dup2.count():
            return "Duplicate values" 
            
        df = df.withColumn("run_id", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        file_name1=file_name.split(".")[0]+"_"+ datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")+".csv"
        df = df.withColumn("file_name", lit(file_name1))
        df = df.withColumn("crtd_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))

        snowdf = df.select(
            "ise_id",
            "channel_id",
            "channel",
            "ques_no",
            "ques_code",
            "ques_desc",
            "standard_ques",
            "ques_class_code",
            "ques_class_desc",
            "weigh",
            "total_score",
            "answer_code",
            "answer_desc",
            "franchise_code",
            "franchise_name",
            "file_name",
            "crtd_dttm"
        )
        
        file_name=file_name.split(".")[0]+"_"+ datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        
        #move to success
        snowdf.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)
        
        snowdf.write.mode("append").saveAsTable(stage_name.split(".")[0] + "." + target_table)
        
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


CREATE OR REPLACE PROCEDURE VNMSDL_RAW.VN_PRODUCT_MAPPING_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS CALLER
AS 'from snowflake.snowpark.functions import col, lit, date_format, current_timestamp, to_date, year, month, concat, format_number, regexp_replace, to_timestamp, when, trim, upper
from snowflake.snowpark.types import StringType, StructType, StructField
import snowflake.snowpark
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz

def main(session: snowpark.Session, Param):
    try:
        file_name = Param[0]
        stage_name = Param[1]
        temp_stage_path = Param[2]
        target_table = Param[3]
        
        df_schema = StructType([
            StructField("putupid", StringType(), True),
            StructField("barcode", StringType(), True),
            StructField("productname", StringType(), True),
        ])
        
        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "\\u0001")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
        
        df = df.na.drop("all")
        
        if df.count() == 0:
            return "No Data in file"

        columns_to_check = ["putupid", "barcode", "productname"]
        
        for i in columns_to_check:
            df_na=df.na.drop(subset=[i])
            if df_na.count()!=df.count():
                return f"null value in column {i}"

        df_dup1 = df.select("putupid", "barcode", "productname")
            
        df_dup2 = df_dup1.dropDuplicates()
        
        if df_dup1.count() != df_dup2.count():
            return "Duplicate values" 
            
        df = df.withColumn("run_id", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        file_name1=file_name.split(".")[0]+"_"+ datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")+".csv"
        df = df.withColumn("file_name", lit(file_name1))
        df = df.withColumn("crtd_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))

        snowdf = df.select(
            "putupid",
            "barcode",
            "productname",
            "file_name",
            "crtd_dttm"
        )
        
        file_name=file_name.split(".")[0]+"_"+ datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        
        #move to success
        snowdf.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)
        
        snowdf.write.mode("append").saveAsTable(stage_name.split(".")[0] + "." + target_table)
        
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


CREATE OR REPLACE PROCEDURE VNMSDL_RAW.SDL_VN_DMS_SALES_ORG_DIM("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS CALLER
AS '
from snowflake.snowpark.functions import col, lit, current_timestamp, trim, upper
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DecimalType,DateType
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz

def main(session:snowpark.Session, Param):
##Param=["OUT_CON_CD_VN_20230727000503.csv","VNMSDL_RAW.DEV_LOAD_STAGE_ADLS","dev/dms/dms_source","SDL_VN_DMS_SALES_ORG_DIM"]
    try:
        file_name       = Param[0]  # File name
        stage_name      = Param[1]  # Stage name where files are located
        temp_stage_path = Param[2]  # Path in the stage
        target_table    = Param[3]  # Target table for the data
        df_schema = StructType([
            StructField("DSTRBTR_ID", StringType()),
            StructField("SALESREP_ID", StringType()),
            StructField("SALESREP_NAME", StringType()),
            StructField("SUP_CODE", StringType()),
            StructField("SALESREP_CRTDATE", StringType()),
            StructField("SALESREP_DATEOFF", StringType()),
            StructField("SUP_NAME", StringType()),
            StructField("SUP_ACTIVE", StringType()),
            StructField("SUP_CRTDATE", StringType()),
            StructField("SUP_DATEOFF", StringType()),
            StructField("ASM_ID", StringType()),
            StructField("ASM_NAME", StringType()),
            StructField("ACTIVE", StringType())
        ])
        df = session.read\\
        .schema(df_schema)\\
        .option("skip_header",1)\\
        .option("field_delimiter", "|")\\
        .option("encoding", "ISO-8859-15") \\
        .option("field_optionally_enclosed_by", "\\"") \\
        .option("null_if", "") \\
        .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        df=df.na.drop("all")
        if df.count()==0 :
            return "No Data in file"
            
        df = df.with_column("SUP_ACTIVE", trim(upper(col("SUP_ACTIVE")), lit('','')))\\
               .with_column("ACTIVE", trim(upper(col("ACTIVE")), lit('','')))\\
               .with_column("CURR_DATE", current_timestamp())\\
               .with_column("RUN_ID", lit(None).cast(DecimalType(14,0)))\\
               .with_column("SOURCE_FILE_NAME", lit(file_name))
        snowdf = df.select(
            "DSTRBTR_ID",
            "SALESREP_ID",
            "SALESREP_NAME",
            "SUP_CODE",
            "SALESREP_CRTDATE",
            "SALESREP_DATEOFF",
            "SUP_NAME",
            "SUP_ACTIVE",
            "SUP_CRTDATE",
            "SUP_DATEOFF",
            "ASM_ID",
            "ASM_NAME",
            "ACTIVE",
            "CURR_DATE",
            "RUN_ID",
            "SOURCE_FILE_NAME"
        )
        
            
        # Archive successful loads for auditability
        file_name=file_name.split(".")[0]+"_"+datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        snowdf.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)
        
        # Write operation to append data to the target table
        snowdf.write.mode("append").save_as_table(stage_name.split(".")[0]+"."+target_table)
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

-----------------------------------------------------------------------

CREATE OR REPLACE PROCEDURE VNMSDL_RAW.SDL_VN_DMS_MSL("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS CALLER
AS '
from snowflake.snowpark.functions import col, lit, current_timestamp, trim, upper
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DecimalType,DateType
import pandas as pd
from datetime import datetime
import pytz
import snowflake.snowpark as snowpark
def main(session:snowpark.Session, Param):
#Param=["OUT_CON_I_VN_20230729001155.csv","VNMSDL_RAW.DEV_LOAD_STAGE_ADLS","dev/dms/dms_source","VNMSDL_RAW.SDL_VN_DMS_MSL"]
    try:
        file_name       = Param[0]  # File name
        stage_name      = Param[1]  # Stage name where files are located
        temp_stage_path = Param[2]  # Path in the stage
        target_table    = Param[3]  # Target table for the data
        df_schema = StructType([
            StructField("MSL_ID", StringType()),
            StructField("SUB_CHANNEL", StringType()),
            StructField("FROM_CYCLE", StringType()),
            StructField("TO_CYCLE", StringType()),
            StructField("PRODUCT_ID", StringType()),
            StructField("PROUCT_NAME", StringType()),
            StructField("ACTIVE", StringType()),
            StructField("GROUPMSL", StringType())
        ])
        
        df = session.read\\
        .schema(df_schema)\\
        .option("skip_header",1)\\
        .option("field_delimiter", "|")\\
        .option("encoding", "ISO-8859-15") \\
        .option("field_optionally_enclosed_by", "\\"") \\
        .option("null_if", "") \\
        .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        df=df.na.drop("all")
        if df.count()==0 :
            return "No Data in file"


        
        # Perform necessary transformations
        df = df.with_column("PROUCT_NAME", trim(upper(col("PROUCT_NAME")), lit('','')))\\
               .with_column("ACTIVE", trim(upper(col("ACTIVE"))))\\
               .with_column("GROUPMSL", trim(upper(col("GROUPMSL"))))\\
               .with_column("CURR_DATE", current_timestamp())\\
               .with_column("RUN_ID", lit(None).cast(DecimalType(14,0)))\\
               .with_column("SOURCE_FILE_NAME", lit(file_name))
        snowdf = df.select(
            "MSL_ID",
            "SUB_CHANNEL",
            "FROM_CYCLE",
            "TO_CYCLE",
            "PRODUCT_ID",
            "PROUCT_NAME",
            "ACTIVE",
            "CURR_DATE",
            "RUN_ID",
            "SOURCE_FILE_NAME",
            "GROUPMSL"
        )
        
        # Archive successful loads for auditability
        file_name=file_name.split(".")[0]+"_"+datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        snowdf.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)
        
        # Write operation to append data to the target table
        snowdf.write.mode("append").save_as_table(stage_name.split(".")[0]+"."+target_table)
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

------------------------------------------------------------------------

CREATE OR REPLACE PROCEDURE VNMSDL_RAW.SDL_VN_DMS_KPI_SELLIN_SELLTHRGH("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS CALLER
AS '
from snowflake.snowpark.functions import col, lit, current_timestamp, trim, upper
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DecimalType,DateType
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz
def main(session:snowpark.Session, Param):
 
    try:
        file_name       = Param[0]  # File name
        stage_name      = Param[1]  # Stage name where files are located
        temp_stage_path = Param[2]  # Path in the stage
        target_table    = Param[3]  # Target table for the data
        df_schema = StructType([
            StructField("DSTRBTR_ID", StringType()),
            StructField("DSTRBTR_TYPE", StringType()),
            StructField("DSTRBTR_NAME", StringType()),
            StructField("CYCLE", StringType()),
            StructField("ORDERTYPE", StringType()),
            StructField("SELLIN_TG", StringType()),
            StructField("SELLIN_AC", StringType())
        ])
        df = session.read\\
        .schema(df_schema)\\
        .option("skip_header",1)\\
        .option("field_delimiter", "|")\\
        .option("encoding", "ISO-8859-15") \\
        .option("field_optionally_enclosed_by", "\\"") \\
        .option("null_if", "") \\
        .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        #Checking for the null values 
        df=df.na.drop("all")
        if df.count()==0:
            return "No Data in file"

        
        # Perform necessary transformations
        df = df.with_column("CURR_DATE", current_timestamp())\\
               .with_column("RUN_ID", lit(None).cast(DecimalType(14,0)))\\
               .with_column("SOURCE_FILE_NAME", lit(file_name))
        snowdf = df.select(
            "DSTRBTR_ID",
            "DSTRBTR_TYPE",
            "DSTRBTR_NAME",
            "CYCLE",
            "ORDERTYPE",
            "SELLIN_TG",
            "SELLIN_AC",
            "CURR_DATE",
            "RUN_ID",
            "SOURCE_FILE_NAME"
        )
        
        # Archive successful loads for auditability
        file_name=file_name.split(".")[0]+"_"+datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        
        snowdf.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)
        
        # Write operation to append data to the target table
        snowdf.write.mode("append").save_as_table(stage_name.split(".")[0]+"."+target_table)
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

-----------------------------------------------------------------


CREATE OR REPLACE PROCEDURE VNMSDL_RAW.SDL_VN_GT_TOPDOOR_TARGET("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS CALLER
AS '
from snowflake.snowpark.functions import col, lit, current_timestamp, trim, upper, right
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DecimalType,DateType
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz
def main(session:snowpark.Session, Param):
    try:
        file_name       = Param[0]  # File name
        stage_name      = Param[1]  # Stage name where files are located
        temp_stage_path = Param[2]  # Path in the stage
        target_table    = Param[3]
        # Define the schema based on the provided table structure
        df_schema = StructType([
            StructField("TD_ID", StringType()),
            StructField("TD_ZONE", StringType()),
            StructField("FROM_CYCLE", StringType()),
            StructField("TO_CYCLE", StringType()),
            StructField("DISTRIBUTOR_CODE", StringType()),
            StructField("CUSTOMER_CODE", StringType()),
            StructField("CUSTOMER_NAME", StringType()),
            StructField("SHOPTYPE", StringType()),
            StructField("TARGET_VALUE", StringType())
        ])
        df = session.read\\
        .schema(df_schema)\\
        .option("skip_header",1)\\
        .option("field_delimiter", ''\\x01'')\\
        .option("encoding", "ISO-8859-15")\\
        .option("field_optionally_enclosed_by", "\\"") \\
        .option("null_if", "") \\
        .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        # Checking for null values    
        df=df.na.drop("all")
        if df.count()==0 :
            return "No Data in file"
        
        # Filtering based on specified conditions
        filtered_df = df.filter(
            (upper(col("TD_ID")).like(''TOPDOORC%'')) &
            (upper(right(col("TD_ID"), 2)) != ''BS'') &
            (col("TARGET_VALUE") != 0)
        )
        # Adding additional fields
        filtered_df = filtered_df.with_column("CRT_DTTM", lit(datetime.now().strftime("%Y%m%d%H%M%S")))\\
                                 .with_column("FILE_NAME", lit(file_name))\\
                                 .with_column("RUN_ID", lit(None).cast(DecimalType(14,0)))
        snowdf = filtered_df.select(
            "TD_ID",
            "TD_ZONE",
            "FROM_CYCLE",
            "TO_CYCLE",
            "DISTRIBUTOR_CODE",
            "CUSTOMER_CODE",
            "CUSTOMER_NAME",
            "SHOPTYPE",
            "TARGET_VALUE",
            "CRT_DTTM",
            "FILE_NAME",
            "RUN_ID"
        )
        
           
        # Archive
        file_name=file_name.split(".")[0]+"_"+datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        
        snowdf.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)
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

--------------------------------------------------------------------

CREATE OR REPLACE PROCEDURE VNMSDL_RAW.SDL_VN_ONEVIEW_OTC("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS CALLER
AS '
from snowflake.snowpark.functions import col, lit, current_timestamp, trim, upper
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
        # Schema inferred from CREATE TABLE statement
        df_schema = StructType([
            StructField("PLANT", StringType()),
            StructField("PRINCIPALCODE", StringType()),
            StructField("PRINCIPAL", StringType()),
            StructField("PRODUCT", StringType()),
            StructField("PRODUCTNAME", StringType()),
            StructField("KUNNR", StringType()),
            StructField("NAME1", StringType()),
            StructField("NAME2", StringType()),
            StructField("ADDRESS", StringType()),
            StructField("PROVINCE", StringType()),
            StructField("ZTERM", StringType()),
            StructField("KDGRP", StringType()),
            StructField("CUSTGROUP", StringType()),
            StructField("REGION", StringType()),
            StructField("DISTRICT", StringType()),
            StructField("VBELN", StringType()),
            StructField("BILLINGDATE", StringType()),
            StructField("REASON", StringType()),
            StructField("Qty", StringType()),
            StructField("DGLE", StringType()),
            StructField("PERNR", StringType()),
            StructField("VAT", StringType()),
            StructField("SUOM", StringType()),
            StructField("CUSTPAYTO", StringType()),
            StructField("TT", StringType()),
            StructField("NGUYENGIA", StringType()),
            StructField("TTV", StringType()),
            StructField("DISCOUNT", StringType()),
            StructField("DEVICE_CODE", StringType()),
            StructField("DEVICE", StringType()),
            StructField("ORDER_NO", StringType()),
            StructField("ORGINV", StringType()),
            StructField("BATCH", StringType()),
            StructField("CHARGE", StringType()),
            StructField("CONTACT_NAME", StringType()),
            StructField("USERID", StringType()),
            StructField("BILLINGINST", StringType()),
            StructField("DISTCHANNEL", StringType()),
            StructField("REDINV", StringType()),
            StructField("SERIAL", StringType()),
            StructField("POTEXT", StringType()),
            StructField("EXPDATE", StringType()),
            StructField("RET_SO", StringType()),
            StructField("VAT_CODE", StringType()),
            StructField("SODOC_DATE", StringType()),
            StructField("ITEMNOTES", StringType()),
            StructField("MG1", StringType()),
            StructField("YEAR", StringType()),
            StructField("MONTH", StringType()),
            StructField("CHANNEL", StringType())
        ])
        df = session.read.schema(df_schema)\\
            .option("skip_header", 1)\\
            .option("field_delimiter", ",")\\
            .option("null_if", "") \\
            .option("encoding", "UTF-8")\\
            .option("field_optionally_enclosed_by", "\\"") \\
            .csv("@" + stage_name + "/" + temp_stage_path + "/" + file_name)

        df=df.na.drop("all")
        if df.count()==0 :
            return "No Data in file"
            
        # Perform transformations
        df = df.with_column("RUN_ID", lit(None).cast(DecimalType(14,0)))\\
               .with_column("FILE_NAME", lit(file_name))
        snowdf = df.select(
            "PLANT",
            "PRINCIPALCODE",
            "PRINCIPAL",
            "PRODUCT",
            "PRODUCTNAME",
            "KUNNR",
            "NAME1",
            "NAME2",
            "ADDRESS",
            "PROVINCE",
            "ZTERM",
            "KDGRP",
            "CUSTGROUP",
            "REGION",
            "DISTRICT",
            "VBELN",
            "BILLINGDATE",
            "REASON",
            "Qty",
            "DGLE",
            "PERNR",
            "VAT",
            "SUOM",
            "CUSTPAYTO",
            "TT",
            "NGUYENGIA",
            "TTV",
            "DISCOUNT",
            "DEVICE_CODE",
            "DEVICE",
            "ORDER_NO",
            "ORGINV",
            "BATCH",
            "CHARGE",
            "CONTACT_NAME",
            "USERID",
            "BILLINGINST",
            "DISTCHANNEL",
            "REDINV",
            "SERIAL",
            "POTEXT",
            "EXPDATE",
            "RET_SO",
            "VAT_CODE",
            "SODOC_DATE",
            "ITEMNOTES",
            "MG1",
            "YEAR",
            "MONTH",
            "CHANNEL",
            "FILE_NAME",
            "RUN_ID"
        )
        
        # Archive successful loads for auditability
        file_name=file_name.split(".")[0]+"_"+datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        
        snowdf.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)
        # Write operation to append data to the target table
        snowdf.write.mode("append").save_as_table(stage_name.split(".")[0]+"."+target_table)
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

-----------------------------------------------------------------------


CREATE OR REPLACE PROCEDURE VNMSDL_RAW.VN_DMS_FORECAST_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS CALLER
AS 'from snowflake.snowpark.functions import col, lit, date_format, current_timestamp, to_date, year, month, concat, format_number, regexp_replace, to_timestamp, when, trim, upper
from snowflake.snowpark.types import StringType, StructType, StructField
import snowflake.snowpark
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz

def main(session: snowpark.Session,Param):
    #Param=["OUT_CON_Forecast_VN_20191118.csv","VNMSDL_RAW.DEV_LOAD_STAGE_ADLS","dev/dms/transaction/manual_file","sdl_vn_dms_forecast"]
    try:
        file_name = Param[0]
        stage_name = Param[1]
        temp_stage_path = Param[2]
        target_table = Param[3]

        df_schema = StructType([
            StructField("cycle", StringType(), True),
            StructField("channel", StringType(), True),
            StructField("territory_dist", StringType(), True),
            StructField("warehouse", StringType(), True),
            StructField("franchise", StringType(), True),
            StructField("brand", StringType(), True),
            StructField("variant", StringType(), True),
            StructField("forecastso_mil", StringType(), True),
        ])

        df = session.read \\
            .schema(df_schema) \\
            .option("skip_header", 1) \\
            .option("field_delimiter", "|") \\
            .option("field_optionally_enclosed_by", "\\"") \\
            .csv("@" + stage_name + "/" + temp_stage_path + "/" + file_name)

        df = df.na.drop("all")

        if df.count() == 0:
            return "No Data in file"

        df = df.withColumn("run_id", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        df = df.withColumn("file_name", lit(file_name))
        df = df.withColumn("crtd_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))

        snowdf = df.select(
            "cycle",
            "channel",
            "territory_dist",
            "warehouse",
            "franchise",
            "brand",
            "variant",
            "forecastso_mil",
            "crtd_dttm",
            "run_id",
            "file_name",
            
        )

        file_name = file_name.split(".")[0] + "_" + datetime.now(pytz.timezone("Asia/Singapore")).strftime(
            "%Y%m%d%H%M%S")

        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")

        # move to success
        snowdf.write.copy_into_location(
            "@" + stage_name + "/" + temp_stage_path + "/processed/success/" + formatted_year + "/" + formatted_month + "/" + file_name,
            header=True, OVERWRITE=True)

        snowdf.write.mode("append").saveAsTable(stage_name.split(".")[0] + "." + target_table)

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

-------------------------------------------------------------------------


CREATE OR REPLACE PROCEDURE VNMSDL_RAW.VN_DKSH_STOCK_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS CALLER
AS '# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them.

import snowflake.snowpark as snowpark
from snowflake import snowpark
from snowflake.snowpark.functions import col,lit
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField
import pandas as pd
from datetime import datetime
import pytz

def main(session: snowpark.Session,Param): 

    try:

        #Param=[''DKSH_stock_20240319.csv'',''VNMSDL_RAW.DEV_LOAD_STAGE_ADLS'',''dev/dksh/transaction/dksh_stock/'',''SDL_VN_DKSH_DAILY_SALES'']

        # Extracting parameters from the input
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]


        # Define the schema for the DataFrame
        df_schema=StructType([
            StructField("GROUP_DS",StringType()),
            StructField("CATEGORY",StringType()),
            StructField("MATERIAL",StringType()),
            StructField("MATERIALDESCRIPTION",StringType()),
            StructField("SYSLOT",StringType()),
            StructField("BATCHNO",StringType()),
            StructField("EXP_DATE",StringType()),
            StructField("TOTAL",IntegerType()),
            StructField("HCM",StringType()),
            StructField("VSIP",StringType()),
            StructField("LANGHA",StringType()),
            StructField("THANHTRI",StringType()),
            StructField("DANANG",StringType()),
            StructField("VALUES_LC",IntegerType()),
            StructField("REASON",StringType())
            ])


        # Read the CSV file into a DataFrame
    
        dataframe = session.read\\
            .schema(df_schema)\\
            .option("skip_header",2)\\
            .option("field_delimiter", "\\u0001")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .option("skip_blank_lines", True)\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        #---------------------------Transformation logic ------------------------------#

        # Handle null values or empty rows
        dataframe=dataframe.na.drop("all")

        # Check for empty Dataframe
        if dataframe.count()==0:
            return "No Data in file"

        # Add file_date, RUN_ID , filename columns
        
        filedate= file_name.split("_")[2].split(".")[0]

        dataframe = dataframe.withColumn("FILE_DATE",lit(filedate).cast("string"))
        dataframe = dataframe.with_column("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")).cast("integer"))
        dataframe = dataframe.with_column("FILE_NAME",lit(file_name))

        # Creating Final Dataframe
        final_df = dataframe.alias("final_df")

        # Delete existing Data for the current file
        
        del_sql = "DELETE FROM " + target_table + " WHERE FILE_NAME ="+"''" + (file_name)+"''"
        session.sql(del_sql).collect()


        # Load Data to the target table
        final_df.write.mode("append").saveAsTable(target_table)

        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")

        # write to success folder
    
        file_name=file_name.split(".")[0]+''_''+datetime.now().strftime("%Y%m%d%H%M%S")
        final_df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)
        
        return "Success"

    

    except KeyError as key_error:
        # Handle KeyError (missing columns) here
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message
        
    except Exception as e:
        # Handle exceptions here
        error_message = f"Error: {str(e)}"
        return error_message';


---------------------------------------------------------------------

CREATE OR REPLACE PROCEDURE VNMSDL_RAW.VN_DMS_SELLOUT_DETAILS_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('regex==2023.10.3','snowflake-snowpark-python==*')
HANDLER = 'main'
EXECUTE AS CALLER
AS '# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them.

import snowflake.snowpark as snowpark
from snowflake import snowpark
from snowflake.snowpark.functions import col,lit,trim, substring
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField
import pandas as pd
from datetime import datetime
import pytz

def main(session: snowpark.Session,Param): 

    try:

        #Param=[''OUT_CON_D_VN_20240320000543.csv'',''VNMSDL_RAW.DEV_LOAD_STAGE_ADLS'',''dev/dms/transaction/dms_source/sellout_details'',''SDL_VN_DMS_D_SELLOUT_SALES_FACT'']

        # Extracting parameters from the input
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]


        # Define the schema for the DataFrame
        df_schema=StructType([
            StructField("DSTRBTR_ID",StringType()),
            StructField("CNTRY_CODE",StringType()),
            StructField("OUTLET_ID",StringType()),
            StructField("ORDER_DATE",StringType()),
            StructField("INVOICE_DATE",StringType()),
            StructField("ORDER_NO",StringType()),
            StructField("INVOICE_NO",StringType()),
            StructField("SALES_ROUTE_ID",StringType()),
            StructField("SALE_ROUTE_NAME",StringType()),
            StructField("SALES_GROUP",StringType()),
            StructField("SALESREP_ID",StringType()),
            StructField("SALESREP_NAME",StringType()),
            StructField("MATERIAL_CODE",StringType()),
            StructField("UOM",StringType()),
            StructField("GROSS_PRICE",StringType()),
            StructField("ORDERQTY",StringType()),
            StructField("QUANTITY",StringType()),
            StructField("TOTAL_SELLOUT_AFVAT_BFDISC",StringType()),
            StructField("DISCOUNT",StringType()),
            StructField("TOTAL_SELLOUT_AFVAT_AFDISC",StringType()),
            StructField("LINE_NUMBER",StringType()),
            StructField("PROMOTION_ID",StringType()),
            StructField("STATUS",StringType())
            ])


        # Read the CSV file into a DataFrame
    
        dataframe = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "|")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .option("skip_blank_lines", True)\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        #---------------------------Transformation logic ------------------------------#

        # Handle null values or empty rows
        dataframe=dataframe.na.drop("all")

        # Check for empty Dataframe
        if dataframe.count()==0:
            return "No Data in file"

        # Add curr_date, RUN_ID and source_file_name in Dataframe

        column_to_trim=["PROMOTION_ID","STATUS"]
        for col_name in column_to_trim:
            dataframe = dataframe.withColumn(col_name, substring(dataframe[col_name],1,50))


        
        dataframe = dataframe.with_column("CURR_DATE",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        dataframe = dataframe.with_column("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")).cast("integer"))
        dataframe = dataframe.with_column("SOURCE_FILE_NAME",lit(file_name))

        # Creating Final Dataframe
        final_df = dataframe.alias("final_df")

        # Load Data to the target table
        final_df.write.mode("append").saveAsTable(target_table)
        

        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")

        # write to success folder
    
        file_name=file_name.split(".")[0]+''_''+datetime.now().strftime("%Y%m%d%H%M%S")
        final_df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)
        
        return "Success"

    

    except KeyError as key_error:
        # Handle KeyError (missing columns) here
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message
        
    except Exception as e:
        # Handle exceptions here
        error_message = f"Error: {str(e)}"
        return error_message';

----------------------------------------------------------------------

CREATE OR REPLACE PROCEDURE VNMSDL_RAW.VN_DMS_SELLOUT_HEADER_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS CALLER
AS '# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them.

import snowflake.snowpark as snowpark
from snowflake import snowpark
from snowflake.snowpark.functions import col,lit,trim, substring
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField
import pandas as pd
from datetime import datetime
import pytz

def main(session: snowpark.Session,Param): 

    try:

        #Param=[''OUT_CON_H_VN_20240321000913.csv'',''VNMSDL_RAW.DEV_LOAD_STAGE_ADLS'',''dev/dms/transaction/dms_source/sellout_header/'',''SDL_VN_DMS_H_SELLOUT_SALES_FACT'']

        # Extracting parameters from the input
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]


        # Define the schema for the DataFrame
        df_schema=StructType([
            StructField("DSTRBTR_ID",StringType()),
            StructField("CNTRY_CODE",StringType()),
            StructField("OUTLET_ID",StringType()),
            StructField("ORDER_DATE",StringType()),
            StructField("INVOICE_DATE",StringType()),
            StructField("ORDER_NO",StringType()),
            StructField("INVOICE_NO",StringType()),
            StructField("SELLOUT_AFVAT_BFDISC",StringType()),
            StructField("TOTAL_DISCOUNT",StringType()),
            StructField("INVOICE_DISCOUNT",StringType()),
            StructField("SELLOUT_AFVAT_AFDISC",StringType()),
            StructField("STATUS",StringType())
            ])


        # Read the CSV file into a DataFrame
    
        dataframe = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "|")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .option("skip_blank_lines", True)\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)


        #---------------------------Transformation logic ------------------------------#

        # Handle null values or empty rows
        dataframe=dataframe.na.drop("all")

        # Check for empty Dataframe
        if dataframe.count()==0:
            return "No Data in file"

        # Add curr_date, RUN_ID and source_file_name in Dataframe
        
        dataframe = dataframe.with_column("CURR_DATE",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        dataframe = dataframe.with_column("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")).cast("integer"))
        dataframe = dataframe.with_column("SOURCE_FILE_NAME",lit(file_name))

        # Creating Final Dataframe
        final_df = dataframe.alias("final_df")

        # Load Data to the target table
        final_df.write.mode("append").saveAsTable(target_table)

        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")

        # write to success folder
    
        file_name=file_name.split(".")[0]+''_''+datetime.now().strftime("%Y%m%d%H%M%S")
        final_df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)
        
        return "Success"

    

    except KeyError as key_error:
        # Handle KeyError (missing columns) here
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message
        
    except Exception as e:
        # Handle exceptions here
        error_message = f"Error: {str(e)}"
        return error_message';

----------------------------------------------------------------


