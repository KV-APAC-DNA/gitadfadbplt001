USE SCHEMA VNMSDL_RAW;

CREATE OR REPLACE PROCEDURE VNMSDL_RAW.SDL_VN_DMS_CALL_DETAILS("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
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
        .option("encoding", "UTF-8") \\
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


CREATE OR REPLACE PROCEDURE VNMSDL_RAW.SDL_VN_DMS_ORDER_PROMOTION("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
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
        .option("encoding", "UTF-8") \\
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
EXECUTE AS OWNER
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
        .option("encoding", "UTF-8") \\
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
EXECUTE AS OWNER
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
        .option("encoding", "UTF-8") \\
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


CREATE OR REPLACE PROCEDURE VNMSDL_RAW.SDL_VN_DMS_KPI("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
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
        .option("encoding", "UTF-8") \\
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

CREATE OR REPLACE PROCEDURE VNMSDL_RAW.SDL_VN_DMS_KPI_SELLIN_SELLTHRGH("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
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
        .option("encoding", "UTF-8") \\
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

CREATE OR REPLACE PROCEDURE VNMSDL_RAW.SDL_VN_DMS_MSL("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '
from snowflake.snowpark.functions import col, lit, current_timestamp, trim, upper
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DecimalType,DateType
import pandas as pd
from datetime import datetime
import pytz
import snowflake.snowpark as snowpark
def main(session:snowpark.Session, Param):
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
        .option("encoding", "UTF-8") \\
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


CREATE OR REPLACE PROCEDURE VNMSDL_RAW.SDL_VN_DMS_PRODUCT_DIM("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
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
        .option("encoding", "UTF-8") \\
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


CREATE OR REPLACE PROCEDURE VNMSDL_RAW.SDL_VN_DMS_PROMOTION_LIST("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
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
        .option("encoding", "UTF-8") \\
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


CREATE OR REPLACE PROCEDURE VNMSDL_RAW.SDL_VN_DMS_SALES_ORG_DIM("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
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
        .option("encoding", "UTF-8") \\
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
