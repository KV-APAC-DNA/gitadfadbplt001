USE SCHEMA THASDL_RAW;

CREATE OR REPLACE PROCEDURE SDL_TH_MT_BIGC("PARAM" ARRAY)
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
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        df_schema = StructType([
            StructField("REPORT_CODE", StringType()),
            StructField("SUPPLIER", StringType()),
            StructField("BUSINESS_FORMAT", StringType()),
            StructField("COMPARE", StringType()),
            StructField("STORE", StringType()),
            StructField("TRANSACTION_DATE", StringType()),
            StructField("LY_COMPARE_DATE", StringType()),
            StructField("REPORT_DATE", StringType()),
            StructField("DIVISION", StringType()),
            StructField("DEPARTMENT", StringType()),
            StructField("SUBDEPARTMENT", StringType()),
            StructField("CLASS", StringType()),
            StructField("SUBCLASS", StringType()),
            StructField("BARCODE", StringType()),
            StructField("ARTICLE", StringType()),
            StructField("ARTICLE_NAME", StringType()),
            StructField("BRAND", StringType()),
            StructField("MODEL", StringType()),
            StructField("SALE_AMT_TY_BAHT", StringType()),
            StructField("SALE_AMT_LY_BAHT", StringType()),
            StructField("SALE_AMT_VAR", StringType()),
            StructField("SALE_QTY_TY", StringType()),
            StructField("SALE_QTY_LY", StringType()),
            StructField("SALE_QTY_VAR", StringType()),
            StructField("STOCK_TY_BAHT", StringType()),
            StructField("STOCK_LY_BAHT", StringType()),
            StructField("STOCK_VAR", StringType()),
            StructField("STOCK_QTY_TY", StringType()),
            StructField("STOCK_QTY_LY", StringType()),
            StructField("STOCK_QTY_VAR", StringType()),
            StructField("DAY_ON_HAND_TY", StringType()),
            StructField("DAY_ON_HAND_LY", StringType()),
            StructField("DAY_ON_HAND_DIFF", StringType())
        ])
        
        df = session.read\\
        .schema(df_schema)\\
        .option("skip_header",1)\\
        .option("field_delimiter", ",")\\
        .option("encoding", "UTF-8") \\
        .option("REPLACE_INVALID_CHARACTERS", True) \\
        .option("field_optionally_enclosed_by", "\\"") \\
        .option("null_if", "") \\
        .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        df = df.with_column("CRT_DTTM", lit(datetime.now().strftime("%Y%m%d%H%M%S"))) \\
               .with_column("FILE_NAME", lit(file_name))

        
        snowdf = df.select(
            "REPORT_CODE",
            "SUPPLIER",
            "BUSINESS_FORMAT",
            "COMPARE",
            "STORE",
            "TRANSACTION_DATE",
            "LY_COMPARE_DATE",
            "REPORT_DATE",
            "DIVISION",
            "DEPARTMENT",
            "SUBDEPARTMENT",
            "CLASS",
            "SUBCLASS",
            "BARCODE",
            "ARTICLE",
            "ARTICLE_NAME",
            "BRAND",
            "MODEL",
            "SALE_AMT_TY_BAHT",
            "SALE_AMT_LY_BAHT",
            "SALE_AMT_VAR",
            "SALE_QTY_TY",
            "SALE_QTY_LY",
            "SALE_QTY_VAR",
            "STOCK_TY_BAHT",
            "STOCK_LY_BAHT",
            "STOCK_VAR",
            "STOCK_QTY_TY",
            "STOCK_QTY_LY",
            "STOCK_QTY_VAR",
            "DAY_ON_HAND_TY",
            "DAY_ON_HAND_LY",
            "DAY_ON_HAND_DIFF",
            "FILE_NAME",
            "CRT_DTTM"
        )
        
        # Check if DataFrame is empty
		snowdf=snowdf.na.drop("all")
        if snowdf.count() == 0:
            return "No Data in table"
        
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")

        # write to success folder    
        # Archive
        file_name1=("_").join(file_name.split("_")[0:5])+"_"+datetime.now().strftime("%Y%m%d%H%M%S")
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
CREATE OR REPLACE PROCEDURE SDL_TH_MT_MAKRO("PARAM" ARRAY)
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
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]

        df_schema = StructType([
            StructField("SUPPLIER_NUMBER", StringType()),
            StructField("LOCATION_NUMBER", StringType()),
            StructField("LOCATION_NAME", StringType()),
            StructField("CLASS_NUMBER", StringType()),
            StructField("SUBCLASS_NUMBER", StringType()),
            StructField("ITEM_NUMBER", StringType()),
            StructField("BARCODE", StringType()),
            StructField("ITEM_DESC", StringType()),
            StructField("EOH_QTY", StringType()),
            StructField("ORDER_IN_TRANSIT_QTY", StringType()),
            StructField("PACK_TYPE", StringType()),
            StructField("MAKRO_UNIT", StringType()),
            StructField("AVG_NET_SALES_QTY", StringType()),
            StructField("NET_SALES_QTY_YTD", StringType()),
            StructField("LAST_RECV_DT", StringType()),
            StructField("LAST_SOLD_DT", StringType()),
            StructField("STOCK_COVER_DAYS", StringType()),
            StructField("NET_SALES_QTY_MTD", StringType()),
            StructField("DAY_1", StringType()),
            StructField("DAY_2", StringType()),
            StructField("DAY_3", StringType()),
            StructField("DAY_4", StringType()),
            StructField("DAY_5", StringType()),
            StructField("DAY_6", StringType()),
            StructField("DAY_7", StringType()),
            StructField("DAY_8", StringType()),
            StructField("DAY_9", StringType()),
            StructField("DAY_10", StringType()),
            StructField("DAY_11", StringType()),
            StructField("DAY_12", StringType()),
            StructField("DAY_13", StringType()),
            StructField("DAY_14", StringType()),
            StructField("DAY_15", StringType()),
            StructField("DAY_16", StringType()),
            StructField("DAY_17", StringType()),
            StructField("DAY_18", StringType()),
            StructField("DAY_19", StringType()),
            StructField("DAY_20", StringType()),
            StructField("DAY_21", StringType()),
            StructField("DAY_22", StringType()),
            StructField("DAY_23", StringType()),
            StructField("DAY_24", StringType()),
            StructField("DAY_25", StringType()),
            StructField("DAY_26", StringType()),
            StructField("DAY_27", StringType()),
            StructField("DAY_28", StringType()),
            StructField("DAY_29", StringType()),
            StructField("DAY_30", StringType()),
            StructField("DAY_31", StringType())
        ])


        df = session.read\\
        .schema(df_schema)\\
        .option("skip_header",1)\\
        .option("field_delimiter", ''\\x01'')\\
        .option("encoding", "UTF-8")\\
        .option("field_optionally_enclosed_by", "\\"")\\
        .option("null_if", "")\\
        .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        # Calculate FILE_NAME from file_name
        # Assuming "Makro_202108_20211117200459.xls" is the file format,
        # and we want to extract "202108" for the CRTD_DTM
        file_date_part = file_name[6:12]  # This will extract "202108"
        df = df.with_column("TRANSACTION_DATE", lit(datetime.strptime(file_date_part, "%Y%m"))) \\
               .with_column("FILE_NAME", lit(file_name)) \\
               .with_column("CRTD_DTM", lit(datetime.now().strftime("%Y%m%d%H%M%S")))


        snowdf = df.select(
            "TRANSACTION_DATE",
            "SUPPLIER_NUMBER",
            "LOCATION_NUMBER",
            "LOCATION_NAME",
            "CLASS_NUMBER",
            "SUBCLASS_NUMBER",
            "ITEM_NUMBER",
            "BARCODE",
            "ITEM_DESC",
            "EOH_QTY",
            "ORDER_IN_TRANSIT_QTY",
            "PACK_TYPE",
            "MAKRO_UNIT",
            "AVG_NET_SALES_QTY",
            "NET_SALES_QTY_YTD",
            "LAST_RECV_DT",
            "LAST_SOLD_DT",
            "STOCK_COVER_DAYS",
            "NET_SALES_QTY_MTD",
            "DAY_1",
            "DAY_2",
            "DAY_3",
            "DAY_4",
            "DAY_5",
            "DAY_6",
            "DAY_7",
            "DAY_8",
            "DAY_9",
            "DAY_10",
            "DAY_11",
            "DAY_12",
            "DAY_13",
            "DAY_14",
            "DAY_15",
            "DAY_16",
            "DAY_17",
            "DAY_18",
            "DAY_19",
            "DAY_20",
            "DAY_21",
            "DAY_22",
            "DAY_23",
            "DAY_24",
            "DAY_25",
            "DAY_26",
            "DAY_27",
            "DAY_28",
            "DAY_29",
            "DAY_30",
            "DAY_31",
            "FILE_NAME",
            "CRTD_DTM"
        )

        # Check if DataFrame is empty
		snowdf=snowdf.na.drop("all")
        if snowdf.count() == 0:
            return "No Data in table"

        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")

        
        # Archive
        file_name1=("_").join(file_name.split("_")[0:5])+"_"+datetime.now().strftime("%Y%m%d%H%M%S")
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
CREATE OR REPLACE PROCEDURE SDL_TH_MT_WATSONS("PARAM" ARRAY)
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
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        df_schema = StructType([
            StructField("DIV", StringType()),
            StructField("DEPT", StringType()),
            StructField("CLASS", StringType()),
            StructField("SUBCLASS", StringType()),
            StructField("ITEM", StringType()),
            StructField("ITEM_DESC", StringType()),
            StructField("NON_SLOW", StringType()),
            StructField("NON_SLOW2", StringType()),
            StructField("FINANCE_STATUS", StringType()),
            StructField("CREATE_DATETIME", StringType()),
            StructField("PRIM_SUPPLIER", StringType()),
            StructField("OLD_SUPP_NO", StringType()),
            StructField("SUPP_DESC", StringType()),
            StructField("LEAD_TIME", StringType()),
            StructField("UNIT_COST", StringType()),
            StructField("UNIT_RETAIL_ZONE5", StringType()),
            StructField("ITEM_STATUS", StringType()),
            StructField("STATUS_WH", StringType()),
            StructField("STATUS_WH_UPDATE_DATE", StringType()),
            StructField("STATUS_STORE", StringType()),
            StructField("STATUS_STORE_UPDATE_DATE", StringType()),
            StructField("STATUS_XDOCK", StringType()),
            StructField("STATUS_XDOCK_UPDATE_DATE", StringType()),
            StructField("SOURCE_METHOD", StringType()),
            StructField("SOURCE_WH", StringType()),
            StructField("POG", StringType()),
            StructField("PRODUCT_TYPE", StringType()),
            StructField("LABEL_UDA", StringType()),
            StructField("BRAND", StringType()),
            StructField("ITEM_TYPE", StringType()),
            StructField("RETURN_POLICY", StringType()),
            StructField("RETURN_TYPE", StringType()),
            StructField("WH_WAC", StringType()),
            StructField("IN_TAX", StringType()),
            StructField("TAX_RATE", StringType()),
            StructField("STOCK_CAT", StringType()),
            StructField("ORDER_FLAG", StringType()),
            StructField("NEW_ITEM_13WEEK", StringType()),
            StructField("DEACTIVATE_DATE", StringType()),
            StructField("WH_ON_ORDER", StringType()),
            StructField("FIRST_RCV", StringType()),
            StructField("PROMO_MONTH", StringType()),
            StructField("SALES_TW", StringType()),
            StructField("NET_AMT", StringType()),
            StructField("NET_COST", StringType()),
            StructField("SALE_AVG_QTY_13WEEKS", StringType()),
            StructField("SALE_AVG_AMT_13WEEKS", StringType()),
            StructField("SALE_AVG_COST13WEEKS", StringType()),
            StructField("NET_QTY_YTD", StringType()),
            StructField("NET_AMT_YTD", StringType()),
            StructField("NET_COST_YTD", StringType()),
            StructField("TURN_WK", StringType()),
            StructField("WH_SOH", StringType()),
            StructField("STORE_TOTAL_STOCK", StringType()),
            StructField("TOTAL_STOCK_QTY", StringType()),
            StructField("WH_STOCK_AMT", StringType()),
            StructField("STORE_TOTAL_STOCK_AMT", StringType()),
            StructField("TOTAL_STOCK_XVAT", StringType()),
            StructField("PRO2", StringType()),
            StructField("DISC", StringType()),
            StructField("PRO22", StringType()),
            StructField("PRO2_PERT_DISC", StringType()),
            StructField("FIRST_DATE_SMS", StringType()),
            StructField("AGING_SMS", StringType()),
            StructField("GROUP_W", StringType()),
            StructField("WIN", StringType()),
            StructField("POG_2", StringType()),
            StructField("FILE_NAME", StringType()),
            StructField("DATE", StringType())
        ])
        df = session.read\\
        .schema(df_schema)\\
        .option("skip_header",1)\\
        .option("field_delimiter", ''\\x01'')\\
        .option("encoding", "UTF-8")\\
        .option("field_optionally_enclosed_by", "\\"")\\
        .option("null_if", "")\\
        .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
        # Adding transformations for specific columns if needed
        df = df.with_column("DATE", lit(datetime.now().strftime("%Y%m%d")))\\
               .with_column("FILE_NAME", lit(file_name))
        snowdf = df.select(
            "DIV",
            "DEPT",
            "CLASS",
            "SUBCLASS",
            "ITEM",
            "ITEM_DESC",
            "NON_SLOW",
            "NON_SLOW2",
            "FINANCE_STATUS",
            "CREATE_DATETIME",
            "PRIM_SUPPLIER",
            "OLD_SUPP_NO",
            "SUPP_DESC",
            "LEAD_TIME",
            "UNIT_COST",
            "UNIT_RETAIL_ZONE5",
            "ITEM_STATUS",
            "STATUS_WH",
            "STATUS_WH_UPDATE_DATE",
            "STATUS_STORE",
            "STATUS_STORE_UPDATE_DATE",
            "STATUS_XDOCK",
            "STATUS_XDOCK_UPDATE_DATE",
            "SOURCE_METHOD",
            "SOURCE_WH",
            "POG",
            "PRODUCT_TYPE",
            "LABEL_UDA",
            "BRAND",
            "ITEM_TYPE",
            "RETURN_POLICY",
            "RETURN_TYPE",
            "WH_WAC",
            "IN_TAX",
            "TAX_RATE",
            "STOCK_CAT",
            "ORDER_FLAG",
            "NEW_ITEM_13WEEK",
            "DEACTIVATE_DATE",
            "WH_ON_ORDER",
            "FIRST_RCV",
            "PROMO_MONTH",
            "SALES_TW",
            "NET_AMT",
            "NET_COST",
            "SALE_AVG_QTY_13WEEKS",
            "SALE_AVG_AMT_13WEEKS",
            "SALE_AVG_COST13WEEKS",
            "NET_QTY_YTD",
            "NET_AMT_YTD",
            "NET_COST_YTD",
            "TURN_WK",
            "WH_SOH",
            "STORE_TOTAL_STOCK",
            "TOTAL_STOCK_QTY",
            "WH_STOCK_AMT",
            "STORE_TOTAL_STOCK_AMT",
            "TOTAL_STOCK_XVAT",
            "PRO2",
            "DISC",
            "PRO22",
            "PRO2_PERT_DISC",
            "FIRST_DATE_SMS",
            "AGING_SMS",
            "GROUP_W",
            "WIN",
            "POG_2",
            "FILE_NAME",
            "DATE"
        )
        
        # Check if DataFrame is empty
		snowdf=snowdf.na.drop("all")
        if snowdf.count() == 0:
            return "No Data in table"
        
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
            
        # Archive
        file_name1=("_").join(file_name.split("_")[0:5])+"_"+datetime.now().strftime("%Y%m%d%H%M%S")
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

CREATE OR REPLACE PROCEDURE TH_ACTION_BOUNCE_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them.

import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col,lit
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz

def main(session: snowpark.Session,Param): 
    try :
        #Param=["TH_Action_Bounce_20230621_20230621170609.csv","THASDL_RAW.DEV_LOAD_STAGE_ADLS","dev/cert_data_lake/SFMC/TH_Action_Bounce","SDL_TH_SFMC_BOUNCE_DATA"]

        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        # Define the schema for the DataFrame
        df_schema =  StructType([
                    StructField("oyb_account_id", StringType()),
                    StructField("job_id", StringType()),
                    StructField("list_id", StringType()),
                    StructField("batch_id", StringType()),
                    StructField("subscriber_id", StringType()),
                    StructField("subscriber_key", StringType()),
                    StructField("event_date", StringType()),
                    StructField("is_unique", StringType()),
                    StructField("domain", StringType()),
                    StructField("bounce_category_id", StringType()),
                    StructField("bounce_category", StringType()),
                    StructField("bounce_subcategory_id", StringType()),
                    StructField("bounce_subcategory", StringType()),
                    StructField("bounce_type_id", StringType()),
                    StructField("bounce_type", StringType()),
                    StructField("smtp_bounce_reason", StringType()),
                    StructField("smtp_message", StringType()),
                    StructField("smtp_code", StringType()),
                    StructField("triggerer_send_definition_object_id", StringType()),
                    StructField("triggered_send_customer_key", StringType()),
                    StructField("email_subject", StringType()),
                    StructField("bcc_email", StringType()),
                    StructField("email_name", StringType()),
                    StructField("email_id", StringType()),
                    StructField("email_address", StringType())
                ])
        
        dataframe = session.read\\
        .schema(df_schema)\\
        .option("skip_header",1)\\
        .option("field_delimiter", "|")\\
        .option("field_optionally_enclosed_by", "\\"") \\
        .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        dataframe=dataframe.na.drop("all")
        if dataframe.count()==0:
            return "No Data in file"
        #---------------------------Transformation logic ------------------------------#

        # Add FILE_NAME and CRTD_DTTM to the dataframe
        new_file_name=file_name[0:25] +".csv"
        dataframe = dataframe.with_column("FILE_NAME",lit(new_file_name))
        dataframe = dataframe.with_column("CRTD_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))


        # Creating copy of the Dataframe
        final_df = dataframe.alias("final_df")

       
        # Load Data to the target table
        final_df.write.mode("append").saveAsTable(target_table)

        # write to success folder
        file_name=file_name[0:25]+''_''+datetime.now().strftime("%Y%m%d%H%M%S")
		
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
		
        final_df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)
        
        return ''Success''
        
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
CREATE OR REPLACE PROCEDURE TH_ACTION_CLICK_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them.

import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col,lit
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DecimalType,DateType
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz

def main(session: snowpark.Session,Param): 
    try :
        

        #Param=["TH_Action_Click_20230621_20230621170615.csv","THASDL_RAW.DEV_LOAD_STAGE_ADLS","dev/cert_data_lake/SFMC/TH_Action_Click","SDL_TH_SFMC_CLICK_DATA"]

        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        # Define the schema for the DataFrame
        df_schema = StructType([
                StructField("oyb_account_id", StringType()),
                StructField("job_id", StringType()),
                StructField("list_id", StringType()),
                StructField("batch_id", StringType()),
                StructField("subscriber_id", StringType()),
                StructField("subscriber_key", StringType()),
                StructField("event_date", StringType()),
                StructField("domain", StringType()),
                StructField("url", StringType()),
                StructField("link_name", StringType()),
                StructField("link_content", StringType()),
                StructField("is_unique", StringType()),
                StructField("email_name", StringType()),
                StructField("email_subject", StringType())
            ])
        
        dataframe = session.read\\
        .schema(df_schema)\\
        .option("skip_header",1)\\
        .option("field_delimiter", "|")\\
        .option("field_optionally_enclosed_by", "\\"") \\
        .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        dataframe=dataframe.na.drop("all")
        if dataframe.count()==0:
            return "No Data in file"
        #---------------------------Transformation logic ------------------------------#

        # Add FILE_NAME and CRTD_DTTM to the dataframe
        new_file_name=file_name[0:24] +".csv"
        dataframe = dataframe.with_column("FILE_NAME",lit(new_file_name))
        dataframe = dataframe.with_column("CRTD_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))


        # Creating copy of the Dataframe
        final_df = dataframe.alias("final_df")

        
        # Load Data to the target table
        final_df.write.mode("append").saveAsTable(target_table)

        # write to success folder
        file_name=file_name[0:24]+''_''+datetime.now().strftime("%Y%m%d%H%M%S")
		
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        final_df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)
        
        return ''Success''
        
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
CREATE OR REPLACE PROCEDURE TH_ACTION_OPEN_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them.

import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col,lit
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DecimalType,DateType
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz



def main(session: snowpark.Session,Param): 
    
    try :

        #Param=["TH_Action_Open_20231205_20231212083337.csv","THASDL_RAW.DEV_LOAD_STAGE_ADLS","dev/cert_data_lake/SFMC/TH_Action_Open/","sdl_TH_sfmc_open_data"]

        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        # Define the schema for the DataFrame
        df_schema = StructType([
            StructField("oyb_account_id", StringType()),
            StructField("job_id", StringType()),
            StructField("list_id", StringType()),
            StructField("batch_id", StringType()),
            StructField("subscriber_id", StringType()),
            StructField("subscriber_key", StringType()),
            StructField("email_name", StringType()),
            StructField("email_subject", StringType()),
            StructField("bcc_email", StringType()),
            StructField("email_id", StringType()),
            StructField("event_date", StringType()),
            StructField("domain", StringType()),
            StructField("is_unique", StringType())
        ])
        
        dataframe = session.read.schema(df_schema) \\
        .option("skip_header",1) \\
        .option("field_delimiter", "|") \\
        .option("field_optionally_enclosed_by", "\\"")  \\
        .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        dataframe=dataframe.na.drop("all")
        if dataframe.count()==0:
            return "No Data in file"
        #---------------------------Transformation logic ------------------------------#

        # Add FILE_NAME and CRTD_DTTM to the dataframe
        new_file_name=file_name[0:23] +".csv"
        dataframe = dataframe.with_column("FILE_NAME",lit(new_file_name))
        dataframe = dataframe.with_column("CRTD_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))


        # Creating copy of the Dataframe
        final_df = dataframe.alias("final_df")

        
        # Load Data to the target table
        final_df.write.mode("append").saveAsTable(target_table)

        # write to success folder
        file_name=file_name[0:23]+''_''+datetime.now().strftime("%Y%m%d%H%M%S")
		
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")

        final_df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)
        
        return ''Success''
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
CREATE OR REPLACE PROCEDURE TH_ACTION_SENT_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them.

import snowflake.snowpark as snowpark
from snowflake import snowpark
from snowflake.snowpark.functions import col,lit,trim
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField,DecimalType,DateType,TimestampType
from datetime import datetime
import pytz

def main(session: snowpark.Session,Param): 

    try:

        #Param=[''TH_Action_Sent_20230621_20230621170614.csv'',''THASDL_RAW.DEV_LOAD_STAGE_ADLS'',''dev/cert_data_lake/SFMC/TH_Action_Sent/'',''SDL_TH_SFMC_SENT_DATA'']

        # Extracting parameters from the input
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        

        # Define the schema for the DataFrame
        df_schema=StructType([
            StructField("OYB_ACCOUNT_ID",StringType()),
            StructField("JOB_ID",StringType()),
            StructField("LIST_ID",StringType()),
            StructField("BATCH_ID",StringType()),
            StructField("SUBSCRIBER_ID",StringType()),
            StructField("SUBSCRIBER_KEY",StringType()),
            StructField("EVENT_DATE",TimestampType()),
            StructField("DOMAIN",StringType()),
            StructField("TRIGGERER_SEND_DEFINITION_OBJECT_ID",StringType()),
            StructField("TRIGGERED_SEND_CUSTOMER_KEY",StringType()),
            StructField("EMAIL_NAME",StringType()),
            StructField("EMAIL_SUBJECT",StringType()),
            StructField("EMAIL_ID",StringType())
            ])


        # Read the CSV file into a DataFrame
    
        dataframe = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "|")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)


        #---------------------------Transformation logic ------------------------------#
        
        # Handle null values or empty rows
        dataframe=dataframe.na.drop("all")
		if dataframe.count()==0:
            return "No Data in file"

        # Trim Spaces in Email Subject column
        dataframe = dataframe.withColumn("EMAIL_SUBJECT", trim(dataframe["EMAIL_SUBJECT"]))

         # Add FILE_NAME and CRTD_DTTM to the dataframe
        new_file_name=file_name[0:23] +".csv"

        dataframe = dataframe.with_column("FILE_NAME",lit(new_file_name))
        dataframe = dataframe.with_column("CRTD_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))

        dataframe=dataframe.select("OYB_ACCOUNT_ID","JOB_ID","LIST_ID","BATCH_ID","SUBSCRIBER_ID","SUBSCRIBER_KEY","EVENT_DATE","DOMAIN","TRIGGERER_SEND_DEFINITION_OBJECT_ID","TRIGGERED_SEND_CUSTOMER_KEY","EMAIL_NAME","EMAIL_SUBJECT","EMAIL_ID","FILE_NAME","CRTD_DTTM")

        # Creating copy of the Dataframe
        final_df = dataframe.alias("final_df")

        


        # Load Data to the target table
        final_df.write.mode("append").saveAsTable(target_table)
        
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")

        # write to success folder
    
        file_name=file_name.split(".")[0]
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
CREATE OR REPLACE PROCEDURE TH_ACTION_UNSUBSCRIBE_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them.

import snowflake.snowpark as snowpark
from snowflake import snowpark
from snowflake.snowpark.functions import col,lit,trim
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField,DecimalType,DateType,TimestampType
from datetime import datetime
import pytz

def main(session: snowpark.Session,Param): 

    try:

        #Param=[''TH_Action_Unsubscribe_20230627_202306271706'',''THASDL_RAW.DEV_LOAD_STAGE_ADLS'',''dev/cert_data_lake/SFMC/TH_Action_Unsubscribe/'',''SDL_TH_SFMC_UNSUBSCRIBE_DATA'']

        # Extracting parameters from the input
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        

        # Define the schema for the DataFrame
        df_schema=StructType([
            StructField("OYB_ACCOUNT_ID",StringType()),
            StructField("JOB_ID",StringType()),
            StructField("LIST_ID",StringType()),
            StructField("BATCH_ID",StringType()),
            StructField("SUBSCRIBER_ID",StringType()),
            StructField("SUBSCRIBER_KEY",StringType()),
            StructField("EVENT_DATE",TimestampType()),
            StructField("DOMAIN",StringType()),
            StructField("EMAIL_NAME",StringType()),
            StructField("EMAIL_SUBJECT",StringType()),
            StructField("EMAIL_ID",StringType()),
            StructField("IS_UNIQUE",StringType())
            ])


        # Read the CSV file into a DataFrame
    
        dataframe = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "|")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        #---------------------------Transformation logic ------------------------------#
        
        # Handle null values or empty rows
        dataframe=dataframe.na.drop("all")
		if dataframe.count()==0:
            return "No Data in file"

         # Add FILE_NAME and CRTD_DTTM to the dataframe
        new_file_name=file_name[0:30] +".csv"

        dataframe = dataframe.with_column("FILE_NAME",lit(new_file_name))
        dataframe = dataframe.with_column("CRTD_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))


        # Creating copy of the Dataframe
        final_df = dataframe.alias("final_df")

        

       

        # Load Data to the target table
        final_df.write.mode("append").saveAsTable(target_table)

        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")

        # write to success folder
    
        file_name=file_name.split(".")[0]
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
CREATE OR REPLACE PROCEDURE TH_CRM_CHILDREN_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them.

import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col,lit,trim,when
from snowflake.snowpark.types import  StringType, StructType, StructField
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz



def main(session: snowpark.Session,Param): 
       
    try :

        #Param=["TH_CRM_Children_20240311_20240311171401.csv","THASDL_RAW.DEV_LOAD_STAGE_ADLS","dev/cert_data_lake/SFMC/TH_CRM_Children/","SDL_TH_SFMC_CHILDREN_DATA"]

        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        # Define the schema for the DataFrame
        
        df_schema = StructType([
                StructField("parent_key", StringType()),
                StructField("child_nm", StringType()),
                StructField("child_birth_mnth", StringType()),
                StructField("child_birth_year", StringType()),
                StructField("child_gender", StringType()),
                StructField("child_number", StringType())
            ])
        
        dataframe = session.read.schema(df_schema) \\
        .option("skip_header",1) \\
        .option("field_delimiter", "|") \\
        .option("field_optionally_enclosed_by", "\\"")  \\
        .option("encoding","UTF-16")\\
        .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        dataframe=dataframe.na.drop("all")
        if dataframe.count()==0:
            return "No Data in file"
       
        #---------------------------Transformation logic ------------------------------#

        # Add FILE_NAME and CRTD_DTTM to the dataframe
        new_file_name=file_name[0:24] +".csv"
        dataframe = dataframe.with_column("FILE_NAME",lit(new_file_name))
        dataframe = dataframe.with_column("CRTD_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))


       

        # Apply the case statement
        dataframe = dataframe.withColumn("CHILD_BIRTH_MNTH",  (
             when(trim(col("child_birth_mnth")) == "มกราคม", "01")
            .when(trim(col("child_birth_mnth")) == "กุมภาพันธ์", "02")
            .when(trim(col("child_birth_mnth")) == "มีนาคม", "03")
            .when(trim(col("child_birth_mnth")) == "เมษายน", "04")
            .when(trim(col("child_birth_mnth")) == "พฤษภาคม", "05")
            .when(trim(col("child_birth_mnth")) == "มิถุนายน", "06")
            .when(trim(col("child_birth_mnth")) == "กรกฎาคม", "07")
            .when(trim(col("child_birth_mnth")) == "สิงหาคม", "08")
            .when(trim(col("child_birth_mnth")) == "กันยายน", "09")
            .when(trim(col("child_birth_mnth")) == "ตุลาคม", "10")
            .when(trim(col("child_birth_mnth")) == "พฤศจิกายน", "11")
            .when(trim(col("child_birth_mnth")) == "ธันวาคม", "12")
            .otherwise("UNDEFINED")

        ))

        # Creating copy of the Dataframe
        final_df = dataframe.select("PARENT_KEY","CHILD_NM","CHILD_BIRTH_MNTH","CHILD_BIRTH_YEAR","CHILD_GENDER","CHILD_NUMBER","FILE_NAME","CRTD_DTTM")

        
        # Load Data to the target table
        final_df.write.mode("append").saveAsTable(target_table)

        # write to success folder
        file_name=file_name[0:24]+''_''+datetime.now().strftime("%Y%m%d%H%M%S")
        # final_df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/success/"+file_name,file_format_type="csv",header=True,OVERWRITE=True)

        
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        #move to success
        final_df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)

        
        return ''Success''
        
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
