CREATE OR REPLACE PROCEDURE PCFSDL_RAW.PA_PHARMA_INV_SYMBION_PREPROCESSING("PARAM" ARRAY)
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
from snowflake.snowpark.functions import col,lit
from snowflake.snowpark.types import StringType, StructType, StructField
import pandas as pd
from datetime import datetime
import pytz
 
def main(session: snowpark.Session,Param):
 
    try:
 
        #Param=[''Stock_Status_by_DC_-_13_Months_Sales.csv'',''PCFSDL_RAW.DEV_LOAD_STAGE_ADLS'',''dev/Pacific_reg_inv/transaction/Symbion'',''SDL_symbion_DSTR,SDL_AU_DSTR_symbion_HEADER'']
 
        # Extracting parameters from the input
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3].split(",")[0]
        Header_table    = Param[3].split(",")[1]
 
 
        # Define the schema for the DataFrame
        df_schema=StructType([
            StructField("STATE", StringType()),
            StructField("WAREHOUSE", StringType()),
            StructField("WAREHOUSE_DESC", StringType()),
            StructField("SYMBION_PRODUCT_NO", StringType()),
            StructField("SYMBION_PRODUCT_DESC", StringType()),
            StructField("SUPPLIER_PART_NO", StringType()),
            StructField("EAN", StringType()),
            StructField("GLOBAL_STD_COST", StringType()),
            StructField("DATE_INTRODUCED", StringType()),
            StructField("DATE_LAST_RECIVED", StringType()),
            StructField("OOSR", StringType()),
            StructField("SOH_AMT", StringType()),
            StructField("SOH_QTY", StringType()),
            StructField("ON_ORDER", StringType()),
            StructField("BACK_ORDER", StringType()),
            StructField("RESERVED_AMT_FOR_ORDER", StringType()),
            StructField("RESERVED_QTY_FOR_ORDER", StringType()),
            StructField("RESERVED_AMT_FOR_QA", StringType()),
            StructField("RESERVED_QTY_FOR_QA", StringType()),
            StructField("AVAILABLE_AMT", StringType()),
            StructField("AVAILABLE_QTY", StringType()),
            StructField("MTD", StringType()),
            StructField("MONTH_01", StringType()),
            StructField("MONTH_02", StringType()),
            StructField("MONTH_03", StringType()),
            StructField("MONTH_04", StringType()),
            StructField("MONTH_05", StringType()),
            StructField("MONTH_06", StringType()),
            StructField("MONTH_07", StringType()),
            StructField("MONTH_08", StringType()),
            StructField("MONTH_09", StringType()),
            StructField("MONTH_10", StringType()),
            StructField("MONTH_11", StringType()),
            StructField("MONTH_12", StringType()),
            StructField("MONTH_13", StringType())
            
            ])
 
 
        # Read the CSV file into a DataFrame
        df_header = session.read\\
            .schema(df_schema)\\
            .option("skip_header",3)\\
            .option("field_delimiter", "\\u0001")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .option("skip_blank_lines", True)\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)\\
            .limit(3)

        dataframe = session.read\\
            .schema(df_schema)\\
            .option("skip_header",7)\\
            .option("field_delimiter", "\\u0001")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .option("skip_blank_lines", True)\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)\\


    
 
        #---------------------------Transformation logic ------------------------------#
 
        #Handle null values or empty rows
        dataframe=dataframe.na.drop("all")
 
        # Check for empty Dataframe
        if dataframe.count()==0:
            return "No Data in file"

        # Extract the date from the header
        first_row = df_header.first()
        date = first_row[0]
        date = date.split(",")
        datePart = "".join(date[-2:]) 
        
        header= df_header.limit(3).subtract(df_header.limit(2))

        source_file=file_name.split(".")[0].replace("_"," ") +".xlsx"

        # ADD filename column to header dataframe
        header = header.with_column("FILE_NAME", lit(source_file))

        # Add INV_DT column with the concatenated date to the data DataFrame and filename column
        dataframe = dataframe.withColumn("INV_DT", lit(datePart).cast("String"))
        dataframe = dataframe.with_column("FILE_NAME", lit(source_file))
  
    
        # Return the DataFrame with INV_DT column
        #Creating copy of the Dataframe
        final_df = dataframe.select("INV_DT","STATE","WAREHOUSE","WAREHOUSE_DESC","SYMBION_PRODUCT_NO","SYMBION_PRODUCT_DESC","SUPPLIER_PART_NO","EAN","GLOBAL_STD_COST","DATE_INTRODUCED","DATE_LAST_RECIVED","OOSR","SOH_AMT",\\
                                    "SOH_QTY","ON_ORDER","BACK_ORDER","RESERVED_AMT_FOR_ORDER","RESERVED_QTY_FOR_ORDER","RESERVED_AMT_FOR_QA","RESERVED_QTY_FOR_QA","AVAILABLE_AMT","AVAILABLE_QTY","MTD","MONTH_01","MONTH_02","MONTH_03",\\
                                    "MONTH_04","MONTH_05","MONTH_06","MONTH_07","MONTH_08","MONTH_09","MONTH_10","MONTH_11","MONTH_12","MONTH_13","FILE_NAME")

        # Load Data to the target table
        final_df.write.mode("overwrite").saveAsTable(target_table)

        # Load the header to header target table "SDL_AU_DSTR_CHS_HEADER"

        header.write.mode("overwrite").saveAsTable(Header_table)

        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        header_file="Symbion_out_hdr"

        # write to success folder
    
        file_name=file_name.replace("_"," ").split(".")[0]+''_''+datetime.now().strftime("%Y%m%d%H%M%S")
        final_df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)

        # Write the header dataframe to temp folder under processed
        header.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/temp/"+formatted_year+"/"+formatted_month+"/"+header_file,file_format_type=''csv'',header=True,OVERWRITE=True)

        

        return "Success"
 
    
 
    except KeyError as key_error:
        # Handle KeyError (missing columns) here
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message
    except Exception as e:
        # Handle exceptions here
        error_message = f"Error: {str(e)}"
        return error_message';


CREATE OR REPLACE PROCEDURE PCFSDL_RAW.PACIFIC_PERENSO_CALL_COVERAGE_TARGETS("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS 'from snowflake.snowpark.functions import col, lit, date_format, current_timestamp, to_date, year, month, concat, format_number, regexp_replace,to_timestamp,when,trim,upper
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DecimalType
import snowflake.snowpark
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz
import pandas as pd

def main(session: snowpark.Session,Param):
    #Param=["Pacific_Perenso_Call_Coverage_targets.csv","PCFSDL_RAW.DEV_LOAD_STAGE_ADLS","dev/ap_perenso/transaction/user_files","sdl_pacific_perenso_call_coverage_targets"]
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        
        df_schema = StructType([
                StructField("account_type_description", StringType(), True),
                StructField("account_grade character", StringType(), True),
                StructField("weekly_targets", StringType(), True),
                StructField("monthly_targets", StringType(), True),
                StructField("yearly_targets",StringType(), True)
                
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

        df = df.with_column("call_per_week", lit(None))
        df = df.with_column("FILE_NAME",lit(file_name))
        df = df.with_column("run_id", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        df = df.with_column("CREATE_dt", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        
        snowdf= df.select(
                    "account_type_description",
                    "account_grade character",
                    "weekly_targets",
                    "monthly_targets",
                    "yearly_targets",
                    "call_per_week",
                    "run_id",
                    "CREATE_dt",
                    "FILE_NAME"
                    )

        snowdf=snowdf.replace("",None)

        snowdf.write.mode("append").saveAsTable(stage_name.split(".")[0] + "." + target_table)
        
        
        file_name=file_name.split(".")[0]+"_"+datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")

        snowdf.write.mode("append").saveAsTable(stage_name.split(".")[0] + "." + target_table)
        
        #move to success
        snowdf.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)
        
        
        
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
CREATE OR REPLACE PROCEDURE PCFSDL_RAW.ACCTCUSTOMLIST_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS 'from snowflake.snowpark.functions import col, lit, date_format, current_timestamp, to_date, year, month, concat, format_number, regexp_replace,to_timestamp,when,trim,upper
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DecimalType
#import snowflake.snowpark
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz
import pandas as pd

def main(session: snowpark.Session,Param):
    #Param=["AcctCustomList_20240326223026.csv","PCFSDL_RAW.DEV_LOAD_STAGE_ADLS","dev/ap_perenso/master/account/","sdl_perenso_account_custom_list"]
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        
        df_schema = StructType([
                StructField("acct_key",IntegerType(), True),
                StructField("field_key", IntegerType(), True),
                StructField("option_desc", StringType(), True)
            ])
        
        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", ",")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

       
        
        df=df.na.drop("all")
        if df.count()==0 :
            return "No Data in file"

       
        df = df.with_column("run_id", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        df = df.with_column("file_name", lit(file_name))
        df = df.with_column("CREATE_dt", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))

        snowdf= df.select(
                    "acct_key",
                    "field_key",
                    "option_desc",
                    "run_id",
                    "CREATE_dt",
                    "file_name"
                    )

        snowdf.write.mode("append").saveAsTable(stage_name.split(".")[0] + "." + target_table)


        file_name=file_name.split(".")[0]+"_"+datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")

        
        
        #move to success
        snowdf.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)
        
        
        
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
CREATE OR REPLACE PROCEDURE PCFSDL_RAW.PERENSO_ACCOUNT_FIELDS_PREPROCESSED("PARAM" ARRAY)
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
    # Param=["AcctFields_20240326223026.csv","PCFSDL_RAW.DEV_LOAD_STAGE_ADLS","dev/ap_perenso/master/account","SDL_PERENSO_ACCOUNT_FIELDS"]
    try:
        file_name = Param[0]
        stage_name = Param[1]
        temp_stage_path = Param[2]
        target_table = Param[3]
        
        df_schema = StructType([
            StructField("field_key", StringType(), True),
            StructField("field_desc", StringType(), True),   
            StructField("field_type", StringType(), True),
            StructField("acct_type_key", StringType(), True),
            StructField("active", StringType(), True),
        ])

        
        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", ",")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
        
        df = df.na.drop("all")
        
        if df.count() == 0:
            return "No Data in file"


        df = df.with_column("file_name", lit(file_name))    
        df = df.withColumn("run_id", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        df = df.withColumn("CREATE_dt", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))

        snowdf = df.select(
            "field_key",
            "field_desc",
            "field_type",
            "acct_type_key",
            "active",
            "run_id",
            "CREATE_dt",
            "file_name"

        )
        

        snowdf.write.mode("append").saveAsTable(stage_name.split(".")[0] + "." + target_table)


        file_name=file_name.split(".")[0]+"_"+ datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        
        #move to success
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
CREATE OR REPLACE PROCEDURE PCFSDL_RAW.PCF_IG_INV_METCASH_PREPROCESSING("PARAM" ARRAY)
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
from snowflake.snowpark.functions import col,lit
from snowflake.snowpark.types import StringType, StructType, StructField
import pandas as pd
from datetime import datetime
import pytz
 
def main(session: snowpark.Session,Param):
 
    try:
 
        #Param=[''MT01P39R_20240127.csv'',''PCFSDL_RAW.DEV_LOAD_STAGE_ADLS'',''dev/Pacific_reg_inv/transaction/Metcash/Source'',''sdl_ig_inventory_data'']
 
        # Extracting parameters from the input
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
 
 
        # Define the schema for the DataFrame
        df_schema=StructType([
            StructField("state", StringType()),
            StructField("ware_house", StringType()),
            StructField("pe_item_no", StringType()),
            StructField("item_description", StringType()),
            StructField("stock_details_promo_ind", StringType()),
            StructField("stock_details_vendor", StringType()),
            StructField("stock_details_vendor_description", StringType()),
            StructField("stock_details_sub_vendor", StringType()),
            StructField("stock_details_lead_time", StringType()),
            StructField("stock_details_buyer_number", StringType()),
            StructField("stock_details_buyer_name", StringType()),
            StructField("stock_details_stock_control_email", StringType()),
            StructField("stock_details_soh", StringType()),
            StructField("stock_details_soo", StringType()),
            StructField("stock_details_awd", StringType()),
            StructField("stock_details_weeks_cover", StringType()),
            StructField("inbound_ordered_cases", StringType()),
            StructField("inbound_received_cases", StringType()),
            StructField("inbound_po_number", StringType()),
            StructField("item_details_item_sub_range_code", StringType()),
            StructField("item_details_item_sub_range_description", StringType()),
            StructField("item_details_pack_size", StringType()),
            StructField("item_details_buying_master_pack", StringType()),
            StructField("item_details_retail_unit", StringType()),
            StructField("item_details_buy_in_pallet", StringType()),
            StructField("item_details_ti", StringType()),
            StructField("item_details_hi", StringType()),
            StructField("item_details_pallet", StringType()),
            StructField("item_details_item_status", StringType()),
            StructField("item_details_delete_code", StringType()),
            StructField("item_details_deletion_date", StringType()),
            StructField("item_details_metcash_item_type", StringType()),
            StructField("item_details_ord_split_cat_code", StringType()),
            StructField("item_details_ndc_item", StringType()),
            StructField("item_details_imported_goods", StringType()),
            StructField("item_details_code_date", StringType()),
            StructField("item_details_packed_on_date", StringType()),
            StructField("item_details_incremental_days", StringType()),
            StructField("item_details_max_shelf_days", StringType()),
            StructField("item_details_receiving_limit", StringType()),
            StructField("item_details_dispatch_limit", StringType()),
            StructField("item_details_current_dd", StringType()),
            StructField("item_details_date_added_to_og", StringType()),
            StructField("item_details_consumer_gtin", StringType()),
            StructField("item_details_inner_gtin", StringType()),
            StructField("item_details_outer_gtin", StringType()),
            StructField("sales_week_6", StringType()),
            StructField("sales_week_5", StringType()),
            StructField("sales_week_4", StringType()),
            StructField("sales_week_3", StringType()),
            StructField("sales_week_2", StringType()),
            StructField("sales_week_1", StringType()),
            StructField("sales_this_week", StringType())
            
            ])
 
 
        # Read the CSV file into a DataFrame
        dataframe = session.read\\
            .schema(df_schema)\\
            .option("skip_header",6)\\
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

        fileuploadeddate = file_name.split("_")[-1].split(".")[0]
        dataframe = dataframe.withColumn("INV_DT", (lit(fileuploadeddate)))
        dataframe = dataframe.with_column("FILE_NAME", lit(file_name.split(".")[0]+".xlsx"))
        dataframe = dataframe.with_column("CREATE_DT",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))


        #Creating copy of the Dataframe
        final_df = dataframe.select("INV_DT","state", "ware_house", "pe_item_no", "item_description", "stock_details_promo_ind", "stock_details_vendor", "stock_details_vendor_description", "stock_details_sub_vendor", "stock_details_lead_time", "stock_details_buyer_number", "stock_details_buyer_name", "stock_details_stock_control_email", "stock_details_soh", "stock_details_soo", "stock_details_awd", "stock_details_weeks_cover",\\
                                    "inbound_ordered_cases", "inbound_received_cases", "inbound_po_number", "item_details_item_sub_range_code", "item_details_item_sub_range_description", "item_details_pack_size", "item_details_buying_master_pack", "item_details_retail_unit", "item_details_buy_in_pallet", "item_details_ti", "item_details_hi", "item_details_pallet", "item_details_item_status", "item_details_delete_code",\\
                                    "item_details_deletion_date", "item_details_metcash_item_type", "item_details_ord_split_cat_code", "item_details_ndc_item", "item_details_imported_goods", "item_details_code_date", "item_details_packed_on_date", "item_details_incremental_days", "item_details_max_shelf_days", "item_details_receiving_limit", "item_details_dispatch_limit", "item_details_current_dd", "item_details_date_added_to_og",\\
                                    "item_details_consumer_gtin", "item_details_inner_gtin", "item_details_outer_gtin", "sales_week_6", "sales_week_5", "sales_week_4", "sales_week_3", "sales_week_2", "sales_week_1", "sales_this_week","CREATE_DT","FILE_NAME")

        # Load Data to the target table
        final_df.write.mode("overwrite").saveAsTable(target_table)
 
         
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
 
        # write to success folder
        file_name=file_name.split(".")[0]
        final_df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)
        return "Success"
        #return final_df
 
    
 
    except KeyError as key_error:
        # Handle KeyError (missing columns) here
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message
    except Exception as e:
        # Handle exceptions here
        error_message = f"Error: {str(e)}"
        return error_message';

CREATE OR REPLACE PROCEDURE PCFSDL_RAW.PA_GROCERY_INV_COLES_PREPROCESSING("PARAM" ARRAY)
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
from snowflake.snowpark.functions import col,lit,trim, substring,current_timestamp,to_timestamp,date_format
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField,FloatType
import pandas as pd
from datetime import datetime
import pytz

def main(session: snowpark.Session,Param): 

    try:

        #Param=[''Coles_09a__SOH_Trend_Detail_Report.csv'',''PCFSDL_RAW.DEV_LOAD_STAGE_ADLS'',''dev/Pacific_reg_inv/transaction/Coles/'',''sdl_dstr_coles_inv'']

        # Extracting parameters from the input
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]


        # Define the schema for the DataFrame
        df_schema=StructType([
            StructField("VENDORVENDOR_NAME",StringType()),
            StructField("VENDOR_NAME",StringType()),
            StructField("DC_STATE_SHRT_DESC",StringType()),
            StructField("DC_STATE_DESC",StringType()),
            StructField("DC",StringType()),
            StructField("DC_DESC",StringType()),
            StructField("CATEGORY",StringType()),
            StructField("CATEGORY_DESC",StringType()),
            StructField("ORDER_ITEM",StringType()),
            StructField("ORDER_ITEM_DESC",StringType()),
            StructField("EAN",StringType()),
            StructField("INV_DATE",StringType()),
            StructField("CLOSING_SOH_NIC",StringType()),
            StructField("CLOSING_SOH_QTY_CTNS",StringType()),
            StructField("CLOSING_SOH_QTY_OCTNS",StringType()),
            StructField("CLOSING_SOH_QTY_UNIT",StringType()),
            StructField("DC_DAYS_ON_HAND",StringType())
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
        dataframe= dataframe.na.drop("all")


        # Check for empty Dataframe
        if dataframe.count()==0:
            return "No Data in file"

        file=file_name.replace("_"," ").split(".")[0]
        source_file=file.replace(" ","_",2)+".xlsx"

        dataframe=dataframe.withColumn(''INV_DATE'', date_format(col(''INV_DATE''), ''DD-MM-YYYY''))
        dataframe = dataframe.with_column("FILE_NAME",lit(source_file))

        # Creating Final Dataframe
        final_df = dataframe.select("VENDORVENDOR_NAME","VENDOR_NAME","DC_STATE_SHRT_DESC","DC_STATE_DESC","DC","DC_DESC","CATEGORY","CATEGORY_DESC","ORDER_ITEM","ORDER_ITEM_DESC","EAN"\\
                                   ,"INV_DATE","CLOSING_SOH_NIC","CLOSING_SOH_QTY_CTNS","CLOSING_SOH_QTY_OCTNS","CLOSING_SOH_QTY_UNIT","DC_DAYS_ON_HAND","FILE_NAME")


        # Load Data to the target table
        final_df.write.mode("append").saveAsTable(target_table)

        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")

        # write to success folder
    
        file_name=file_name.replace("_"," ").split(".")[0]+''_''+datetime.now().strftime("%Y%m%d%H%M%S")
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


CREATE OR REPLACE PROCEDURE PCFSDL_RAW.PA_GROCERY_INV_WOOLWORTHS_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS 'from snowflake.snowpark.functions import col, lit, date_format, current_timestamp, to_date, year, month, concat, format_number, to_timestamp
from snowflake.snowpark.types import StringType, StructType, StructField
import snowflake.snowpark
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz

def main(session: snowpark.Session,Param):
    try:

        #Param=[''WW_AV303_Stock_Status_Weekly_-_25.02.2024.csv'',''PCFSDL_RAW.DEV_LOAD_STAGE_ADLS'',''dev/Pacific_reg_inv/transaction/Woolworths/'',''SDL_DSTR_WOOLWORTH_INV'']
        
        file_name = Param[0]
        stage_name = Param[1]
        temp_stage_path = Param[2]
        target_table = Param[3]


        df_schema = StructType([
                StructField("INV_DATE", StringType(), True),
                StructField("Rank", StringType(), True),
                StructField("Article_Code", StringType(), True),
                StructField("ARTICLEDESC", StringType(), True),
                StructField("MM_Code", StringType(), True),
                StructField("MM_Name", StringType(), True),
                StructField("CM_Code", StringType(), True),
                StructField("CM_Name", StringType(), True),
                StructField("RM_Code", StringType(), True),
                StructField("RM_Name", StringType(), True),
                StructField("Rep_Code", StringType(), True),
                StructField("Replenisher", StringType(), True),
                StructField("Goods_Supplier_Code", StringType(), True),
                StructField("Goods_Supplier_Name", StringType(), True),
                StructField("LT", StringType(), True),
                StructField("DC_Code", StringType(), True),
                StructField("ACD", StringType(), True),
                StructField("RP_Type", StringType(), True),
                StructField("ALC_Status", StringType(), True),
                StructField("OM", StringType(), True),
                StructField("VP", StringType(), True),
                StructField("Ti", StringType(), True),
                StructField("Hi", StringType(), True),
                StructField("WW_Stores", StringType(), True),
                StructField("SL_PERC", StringType(), True),
                StructField("SL_Missed_Value", StringType(), True),
                StructField("SOH_OMs", StringType(), True),
                StructField("SOO_OMs", StringType(), True),
                StructField("SOH_PRICE", StringType(), True),
                StructField("Demand_OMs", StringType(), True),
                StructField("Issues_OMs", StringType(), True),
                StructField("Not_Supplied_OMs", StringType(), True),
                StructField("Fairshare_OMs", StringType(), True),
                StructField("Overlay_OMs", StringType(), True),
                StructField("AWD_OMs", StringType(), True),
                StructField("AVG_ISSUES", StringType(), True),
                StructField("DOS_OMs", StringType(), True),
                StructField("Due_Date", StringType(), True),
                StructField("Reason", StringType(), True),
                StructField("OOS_Comments", StringType(), True),
                StructField("OOS_28_DAYS", StringType(), True),
                StructField("Cons_Days_OOS", StringType(), True),
                StructField("Total_Wholesale_Demand_OM", StringType(), True),
                StructField("Total_Wholesale_Issue_OM", StringType(), True),
                StructField("Wholesale_Flag", StringType(), True)
            ])

        sheet_name="Current_Week"

        new_file_name=file_name

        file_name=sheet_name+".csv"


        df = session.read\\
        .schema(df_schema)\\
        .option("skip_header",1)\\
        .option("field_delimiter", "\\u0001")\\
        .option("field_optionally_enclosed_by", "\\"") \\
        .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        
        df = df.na.drop("all")

        
        if df.count() == 0:
            return "No Data in file"

        df=df.withColumn(''INV_DATE'', date_format(col(''INV_DATE''), ''DD-MM-YYYY''))
        df = df.with_column("FILE_NAME", lit(new_file_name))

        snowdf=df.select("INV_DATE", "Rank", "Article_Code", "ARTICLEDESC", 
                        "MM_Code", "MM_Name", "CM_Code", "CM_Name", "RM_Code", "RM_Name", 
                        "Rep_Code", "Replenisher", "Goods_Supplier_Code", "Goods_Supplier_Name", 
                        "LT", "DC_Code", "ACD", "RP_Type", "ALC_Status", "OM", "VP", "Ti", 
                        "Hi", "WW_Stores", "SL_PERC", "SL_Missed_Value", "SOH_OMs", "SOO_OMs", 
                        "SOH_PRICE", "Demand_OMs", "Issues_OMs", "Not_Supplied_OMs", "Fairshare_OMs", 
                        "Overlay_OMs", "AWD_OMs", "AVG_ISSUES", "DOS_OMs", "Due_Date", 
                        "Reason", "OOS_Comments", "OOS_28_DAYS", "Cons_Days_OOS", 
                        "Total_Wholesale_Demand_OM", "Total_Wholesale_Issue_OM", "Wholesale_Flag","FILE_NAME")

        # Remove Total from last row
        row_count = snowdf.count()
        final_df = snowdf.limit(row_count - 1)

        

        final_df.write.mode("append").saveAsTable(stage_name.split(".")[0] + "." + target_table)

        file_name=file_name.split(".")[0]+"_"+ datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        
        #move to success
        final_df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)
        

        
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


CREATE OR REPLACE PROCEDURE PCFSDL_RAW.PERENSO_ACCT_DIST_ACCT_PREPROCESSING("PARAM" ARRAY)
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
    # Param=["AcctDistAcct_20240326223026.csv","PCFSDL_RAW.DEV_LOAD_STAGE_ADLS","dev/ap_perenso/master/account","SDL_PERENSO_ACCT_DIST_ACCT"]
    try:
        file_name = Param[0]
        stage_name = Param[1]
        temp_stage_path = Param[2]
        target_table = Param[3]
        
        df_schema = StructType([
            StructField("acct_key", StringType(), True),
            StructField("branch_key", StringType(), True),   
            StructField("id", StringType(), True),
            StructField("system_primary", StringType(), True),
            StructField("active", StringType(), True),
        ])

        
        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", ",")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
        
        df = df.na.drop("all")
        
        if df.count() == 0:
            return "No Data in file"

        

        df = df.with_column("FILE_NAME", lit(file_name))    
        df = df.withColumn("run_id", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        df = df.withColumn("CREATE_dt", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))

        snowdf = df.select(
            "acct_key",
            "branch_key",
            "id",
            "system_primary",
            "active",
            "run_id",
            "CREATE_dt",
            "FILE_NAME"
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

CREATE OR REPLACE PROCEDURE PCFSDL_RAW.ANZ_PERENSO_OVER_AND_ABOVE_STATE_PREPROCESSING("PARAM" ARRAY)
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
from snowflake.snowpark.types import  StringType, StructType, StructField,IntegerType
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz



def main(session: snowpark.Session,Param): 
       
    try :
            
        # Param=[''OverandAboveState_20240320223707.csv'',''PCFSDL_RAW.DEV_LOAD_STAGE_ADLS'',''dev/ap_perenso/transaction/overandabove'',''sdl_perenso_over_and_above_state'']

        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        # Define the schema for the DataFrame
        
        df_schema = StructType([
                StructField("over_and_above_key", IntegerType()),
                StructField("start_date", StringType()),
                StructField("end_date", StringType()),
                StructField("batch_count", IntegerType()),
                StructField("store_chk_hdr_key", IntegerType())
            ])

  
        
        dataframe = session.read.schema(df_schema) \\
        .option("skip_header",1) \\
        .option("field_delimiter", ",") \\
        .option("field_optionally_enclosed_by", "\\"")  \\
        .option("encoding","UTF-8")\\
        .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        
        dataframe=dataframe.na.drop("all")
        if dataframe.count()==0:
            return "No Data in file"


        dataframe = dataframe.with_column("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        dataframe = dataframe.with_column("CRTD_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        dataframe = dataframe.with_column("FILE_NAME", lit(file_name))

        # Load Data to the target table
        dataframe.write.mode("append").saveAsTable(target_table)
       
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        #move to success
        dataframe.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)

        
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

CREATE OR REPLACE PROCEDURE PCFSDL_RAW.PCF_PERENSO_ORDER_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
PACKAGES = ('openpyxl==3.0.10','snowflake-snowpark-python==*')
HANDLER = 'main'
EXECUTE AS OWNER
AS 'import snowflake.snowpark as snowpark
from snowflake.snowpark.types import StringType, StructType, StructField ,DecimalType
from snowflake.snowpark.functions import lit
from datetime import datetime
import pytz


def get_schema(file_name):
    df_schema=""
    select_col=""
    if file_name.startswith("OrderHdr"):
        df_schema=StructType([
        StructField("ORDER_KEY" , StringType()),
        StructField("ORDER_TYPE_KEY" , StringType()),
        StructField("ACCT_KEY" , StringType()),
        StructField("ORDER_DATE" , StringType()),
        StructField("STATUS" , StringType()),
        StructField("CHARGE" , StringType()),
        StructField("CONFIRMATION" , StringType()),
        StructField("DIARY_ITEM_KEY" , StringType()),
        StructField("WORK_ITEM_KEY" , StringType()),
        StructField("ACCOUNT_ORDER_NO" , StringType()),
        StructField("DELVRY_INSTNS" , StringType()),
            ])
        select_col=["ORDER_KEY","ORDER_TYPE_KEY","ACCT_KEY","ORDER_DATE","STATUS","CHARGE","CONFIRMATION","DIARY_ITEM_KEY","WORK_ITEM_KEY","ACCOUNT_ORDER_NO","DELVRY_INSTNS","RUN_ID","CREATE_DT","FILE_NAME"]
    if file_name.startswith("OrderType"):
        df_schema=StructType([
        StructField("ORDER_TYPE_KEY" , StringType()),
        StructField("ORDER_TYPE_DESC" , StringType()),
        StructField("SOURCE" , StringType())
            ])
        select_col=["ORDER_TYPE_KEY","ORDER_TYPE_DESC","SOURCE","run_id","CREATE_dt","FILE_NAME"]
    if file_name.startswith("OrderBatch"):
        df_schema=StructType([
        StructField("ORDER_KEY" , StringType()),
        StructField("BATCH_KEY" , StringType()),
        StructField("BRANCH_KEY" , StringType()),
        StructField("DIST_ACCT" , StringType()),
        StructField("DELVRY_DT" , StringType()),
        StructField("STATUS" , StringType()),
        StructField("SUFFIX" , StringType()),
        StructField("SENT_DT" , StringType()),
            ])
        select_col=["ORDER_KEY","BATCH_KEY","BRANCH_KEY","DIST_ACCT","DELVRY_DT","STATUS","SUFFIX","SENT_DT","RUN_ID","CREATE_DT","FILE_NAME"]
    if file_name.startswith("OrderDetail"):
        df_schema=StructType([
         StructField("ORDER_KEY", StringType()),
        StructField("BATCH_KEY", StringType()),
        StructField("LINE_KEY", StringType()),
        StructField("PROD_KEY", StringType()),
        StructField("UNIT_QTY", StringType()),
        StructField("ENTERED_QTY", StringType()),
        StructField("ENTERED_UNIT_KEY", StringType()),
        StructField("LIST_PRICE", StringType()),
        StructField("NIS", StringType()),
        StructField("RRP", StringType()),
        StructField("DISC_KEY", StringType()),
        StructField("CREDIT_LINE_KEY", StringType()),
        StructField("CREDITED", StringType())
            ])
        select_col=["ORDER_KEY","BATCH_KEY","LINE_KEY","PROD_KEY","UNIT_QTY","ENTERED_QTY","ENTERED_UNIT_KEY","LIST_PRICE","NIS","RRP","DISC_KEY","CREDIT_LINE_KEY","CREDITED","RUN_ID", "CREATE_DT","FILE_NAME"]
    return df_schema,select_col
    

def main(session: snowpark.Session,Param):
    try:
        # Extracting parameters from the input

        file_name       = Param[0]     
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        db_name         = stage_name.split(".")[0]
        target_table    = db_name+"."+Param[3]
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")


        # Define the schema for the DataFrame
        df_schema,select_col=get_schema(file_name)
        

        df = session.read\\
        .schema(df_schema)\\
        .option("skip_header",1)\\
        .option("field_delimiter", ",")\\
        .option("field_optionally_enclosed_by", "\\"")\\
        .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
        
        df=df.na.drop("all")
        if df.count()==0:
            return "No Data in file"

        df = df.with_column("FILE_NAME",lit(file_name))
        df = df.withColumn("run_id",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        df = df.withColumn("CREATE_dt",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
            
        
                    
        final_df=df.select(select_col)
                        


        
        # write to target table
        final_df.write.mode("append").saveAsTable(target_table)

        file_name=file_name+"_"+datetime.now().strftime("%Y%m%d%H%M%S")
        
        #move to success
        final_df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True,file_format_name="PCFSDL_RAW.MY_CSV_FORMAT")


        return "Success"

    except KeyError as key_error:
        # Handle KeyError (missing columns) here
        error_message = f"KeyError: {str(key_error)}"
        return error_message

    
    except Exception as e:
        # Handle exceptions here
        error_message = f"Error: {str(e)}"
        return error_message';
CREATE OR REPLACE PROCEDURE PCFSDL_RAW.PERENSO_ACCOUNT_RELN_ID_PREPROCESSING("PARAM" ARRAY)
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
   # Param=["AcctRelid_20240326223026.csv","PCFSDL_RAW.DEV_LOAD_STAGE_ADLS","dev/ap_perenso/master/account","SDL_PERENSO_ACCOUNT_RELN_ID"]
    try:
        file_name = Param[0]
        stage_name = Param[1]
        temp_stage_path = Param[2]
        target_table = Param[3]
        
        df_schema = StructType([
            StructField("acct_key", StringType(), True),
            StructField("field_key", StringType(), True),   
            StructField("id", StringType(), True),
        ])

        
        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", ",")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
        
        df = df.na.drop("all")
        
        if df.count() == 0:
            return "No Data in file"
            
        df = df.with_column("FILE_NAME", lit(file_name))
        df = df.withColumn("run_id", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        df = df.withColumn("CREATE_dt", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))

        snowdf = df.select(
            "acct_key",
            "field_key",
            "id",
            "run_id",
            "CREATE_dt",
            "FILE_NAME"
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
CREATE OR REPLACE PROCEDURE PCFSDL_RAW.PCF_PERENSO_DIARYITEM_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
PACKAGES = ('openpyxl==3.0.10','snowflake-snowpark-python==*')
HANDLER = 'main'
EXECUTE AS CALLER
AS 'import snowflake.snowpark as snowpark
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField ,DecimalType
from snowflake.snowpark.functions import lit
from datetime import datetime
import pytz

def main(session: snowpark.Session,Param): 
    
	# SP call and parameters to pass.
	# CALL PCFSDL_RAW.PCF_PERENSO_DIARYITEM_PREPROCESSING([''DiaryItem.csv'',''PCFSDL_RAW.PROD_LOAD_STAGE_ADLS'',''prd/ap_perenso/master/user'',''sdl_perenso_diary_item''])
	
    try:
        # Extracting parameters from the input

        file_name       = Param[0]     
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        db_name         = stage_name.split(''.'')[0]
        target_table    = db_name+"."+Param[3]
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")


        # Define the schema for the DataFrame
        df_schema=StructType([
        StructField(''diary_item_key'' , DecimalType()),
        StructField(''diary_item_type_key'' , DecimalType()),
        StructField(''start_time'' , StringType()),
        StructField(''end_time'' , StringType()),
        StructField(''acct_key'' , DecimalType()),
        StructField(''acct_key_1'' , DecimalType()),
        StructField(''create_user_key'' , DecimalType()),
        StructField(''complete'' , StringType())
            ])
        # Set the current session schema
        
        session.use_schema(stage_name.split(''.'')[0])

             
        stage_path="@{0}/{1}/{2}".format(stage_name,temp_stage_path,file_name)
        print(stage_path)
            
        temp_df = session.read\\
        .schema(df_schema)\\
        .option("skip_header",1)\\
        .option("field_delimiter", ",")\\
        .option("field_optionally_enclosed_by", "\\"") \\
        .csv(stage_path)
            
        temp_df=temp_df.na.drop("all")
        if temp_df.count()==0:
            return "No Data in file"

        temp_df = temp_df.with_column("FILE_NAME", lit(file_name))    
        temp_df = temp_df.withColumn("run_id",lit(lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S"))))
        temp_df = temp_df.withColumn("CREATE_dt",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
            
        
                    
        final_df=temp_df.select("diary_item_key","diary_item_type_key","start_time","end_time","acct_key","acct_key_1","CREATE_user_key","complete","run_id","CREATE_dt","FILE_NAME")
                        


        file_name=file_name+''_''+datetime.now().strftime("%Y%m%d%H%M%S")
        
        # write to target table
        final_df.write.mode("append").saveAsTable(target_table)
        
        #move to success
        final_df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)


        return ''Success''

    except KeyError as key_error:
        # Handle KeyError (missing columns) here
        error_message = f"KeyError: {str(key_error)}"
        return error_message

    
    except Exception as e:
        # Handle exceptions here
        error_message = f"Error: {str(e)}"
        return error_message
';
CREATE OR REPLACE PROCEDURE PCFSDL_RAW.PCF_PERENSO_USER_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
PACKAGES = ('openpyxl==3.0.10','snowflake-snowpark-python==*')
HANDLER = 'main'
EXECUTE AS CALLER
AS 'import snowflake.snowpark as snowpark
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField ,DecimalType
from snowflake.snowpark.functions import lit
from datetime import datetime
import pytz

def main(session: snowpark.Session,Param): 
    
	# SP call and parameters to pass.
	# CALL PCFSDL_RAW.PCF_PERENSO_USER_PREPROCESSING([''Users.csv'',''PCFSDL_RAW.PROD_LOAD_STAGE_ADLS'',''prd/ap_perenso/master/user'',''sdl_perenso_users''])
	
    try:
        # Extracting parameters from the input

        file_name       = Param[0]     
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        db_name         = stage_name.split(''.'')[0]
        target_table    = db_name+"."+Param[3]
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")


        # Define the schema for the DataFrame
        df_schema=StructType([
        StructField(''user_key'' , DecimalType()),
        StructField(''display_name'' , StringType()),
        StructField(''user_type_key'' , DecimalType()),
        StructField(''user_type_desc'' , StringType()),
        StructField(''active'' , StringType()),
        StructField(''email_address'' , StringType())
            ])
        # Set the current session schema
        
        session.use_schema(stage_name.split(''.'')[0])

        
        stage_path="@{0}/{1}/{2}".format(stage_name,temp_stage_path,file_name)
        print(stage_path)
            
        temp_df = session.read\\
        .schema(df_schema)\\
        .option("skip_header",1)\\
        .option("field_delimiter", ",")\\
        .option("field_optionally_enclosed_by", "\\"") \\
        .csv(stage_path)
            
        temp_df=temp_df.na.drop("all")
        if temp_df.count()==0:
            return "No Data in file"

        temp_df = temp_df.with_column("FILE_NAME", lit(file_name))
        temp_df = temp_df.withColumn("run_id",lit(lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S"))))
        temp_df = temp_df.withColumn("CREATE_dt",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
            
        
                    
        final_df=temp_df.select("user_key","display_name","user_type_key","user_type_desc","active","email_address","run_id","CREATE_dt","FILE_NAME")
                        


        file_name=file_name+''_''+datetime.now().strftime("%Y%m%d%H%M%S")
        
        # write to target table
        final_df.write.mode("append").saveAsTable(target_table)
        
        #move to success
        final_df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)


        return ''Success''

    except KeyError as key_error:
        # Handle KeyError (missing columns) here
        error_message = f"KeyError: {str(key_error)}"
        return error_message

    
    except Exception as e:
        # Handle exceptions here
        error_message = f"Error: {str(e)}"
        return error_message
';
CREATE OR REPLACE PROCEDURE PCFSDL_RAW.PERENSO_ACCOUNT_GROUP_LVL_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS 'from snowflake.snowpark.functions import col, lit, date_format, current_timestamp, to_date, year, month, concat, format_number, regexp_replace, to_timestamp, when, trim, upper
from snowflake.snowpark.types import StringType, StructType, StructField, IntegerType
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz

def main(session: snowpark.Session,Param):
    #Param=["AcctGrpLevel_20240326223026.csv","PCFSDL_RAW.DEV_LOAD_STAGE_ADLS","dev/ap_perenso/master/account","SDL_PERENSO_ACCOUNT_GROUP_LVL"]
    try:
        file_name = Param[0]
        stage_name = Param[1]
        temp_stage_path = Param[2]
        target_table = Param[3]
        
        df_schema = StructType([
            StructField("acct_grp_lev_key", IntegerType(), True),
            StructField("acct_lev_desc", StringType(), True),     
            StructField("acct_lev_index", IntegerType(), True),
            StructField("field_key", IntegerType(), True),
        ])

        
        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", ",")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
        
        df = df.na.drop("all")
        
        if df.count() == 0:
            return "No Data in file"
            
        df = df.with_column("FILE_NAME", lit(file_name))
        df = df.withColumn("run_id", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        df = df.withColumn("CREATE_dt", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))

        snowdf = df.select(
            "acct_grp_lev_key",
            "acct_lev_desc",
            "acct_lev_index",
            "field_key",
            "run_id",
            "CREATE_dt",
            "FILE_NAME"
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
CREATE OR REPLACE PROCEDURE PCFSDL_RAW.PCF_PERENSO_HEAD_OFFICE_REQ_STATE_PREPROCESSING("PARAM" ARRAY)
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
from snowflake.snowpark.functions import col,lit
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField
import pandas as pd
from datetime import datetime
import pytz
 
def main(session: snowpark.Session,Param):
 
    try:
 
        #Param=[''HeadOfficeReqState.csv'',''PCFSDL_RAW.DEV_LOAD_STAGE_ADLS'',''dev/ap_perenso/transaction/headoffice/'',''sdl_perenso_head_office_req_state'']
 
        # Extracting parameters from the input
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
 
 
        # Define the schema for the DataFrame
        df_schema=StructType([
            StructField("ACCT_KEY",StringType()),
            StructField("TODO_KEY",StringType()),
            StructField("PROD_GRP_KEY",StringType()),
            StructField("START_DATE",StringType()),
            StructField("END_DATE",StringType()),
            StructField("REQ_STATE",StringType()),
            StructField("STORE_CHK_HDR_KEY",StringType())
            
            ])
 
        # Read the CSV file into a DataFrame
        dataframe = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", ",")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .option("skip_blank_lines", True)\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
 
        #---------------------------Transformation logic ------------------------------#
 
        # Handle null values or empty rows
        dataframe=dataframe.na.drop("all")
 
        # Check for empty Dataframe
        if dataframe.count()==0:
            return "No Data in file"
    
        dataframe = dataframe.with_column("FILE_NAME", lit(file_name))
        dataframe = dataframe.with_column("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        dataframe = dataframe.with_column("CREATE_DT",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        # Creating copy of the Dataframe
        final_df = dataframe.select("ACCT_KEY", "TODO_KEY", "PROD_GRP_KEY", "START_DATE", "END_DATE","REQ_STATE",\\
                                    "STORE_CHK_HDR_KEY","RUN_ID","CREATE_DT","FILE_NAME")
 
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
CREATE OR REPLACE PROCEDURE PCFSDL_RAW.PA_PHARMA_INV_API_PREPROCESSING("PARAM" ARRAY)
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
from snowflake.snowpark.functions import col,lit,trim, substring,current_timestamp,to_timestamp,date_format
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField,FloatType,DecimalType,DoubleType
import pandas as pd
from datetime import datetime
import pytz

def main(session: snowpark.Session,Param): 

    try:

        #Param=[''RE_2.0_API_Inventory_and_Sales_Report_2024-03-31.csv'',''PCFSDL_RAW.DEV_LOAD_STAGE_ADLS'',''dev/Pacific_reg_inv/transaction/API/'',''SDL_API_DSTR,SDL_au_dstr_api_header'']

        # Extracting parameters from the input
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3].split(",")[0]
        Header_table    = Param[3].split(",")[1]


        # Define the schema for the DataFrame
        df_schema=StructType([
            StructField("ARTICLE_ID",StringType()),
            StructField("ARTICLE_DESC",StringType()),
            StructField("PRODUCT_EAN",StringType()),
            StructField("SITE_ID",StringType()),
            StructField("SITE_DESC",StringType()),
            StructField("VENDOR",StringType()),
            StructField("VENDOR_DESC",StringType()),
            StructField("PRODUCT_SAP_ID",StringType()),
            StructField("COST_PRICE",StringType()),
            StructField("CROSS_SITE_STATUS",StringType()),
            StructField("MONTH_13",StringType()),
            StructField("MONTH_12",StringType()),
            StructField("MONTH_11",StringType()),
            StructField("MONTH_10",StringType()),
            StructField("MONTH_09",StringType()),
            StructField("MONTH_08",StringType()),
            StructField("MONTH_07",StringType()),
            StructField("MONTH_06",StringType()),
            StructField("MONTH_05",StringType()),
            StructField("MONTH_04",StringType()),
            StructField("MONTH_03",StringType()),
            StructField("MONTH_02",StringType()),
            StructField("MONTH_01",StringType()),
            StructField("NULL_COLOUMN",StringType()),
            StructField("MTH_TOTAL_INVOICED_QTY",StringType()),
            StructField("SOH_QTY",StringType()),
            StructField("DC_SOO_QTY",StringType()),
            StructField("SO_BACKORDER_QTY",StringType())
            ])


        # Read Date and Header for column names
    
        df_header = session.read\\
            .schema(df_schema)\\
            .option("skip_header",0)\\
            .option("field_delimiter", "\\u0001")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .option("skip_blank_lines", True)\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)\\
            .limit(2)

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
        dataframe= dataframe.na.drop("all")


        # Check for empty Dataframe
        if dataframe.count()==0:
            return "No Data in file"

        # Fetch value for inv_date column
        date=file_name.split("_")[-1].split(".")[0]

        inv_date=date

        columns = dataframe.columns

        source_file=file_name.rsplit(".",1)[0] + ".xlsx"

        # Add inv_date and filename column to the dataframe
        dataframe=dataframe.with_column("INVT_DT",lit(inv_date).cast("String"))
        dataframe = dataframe.with_column("FILE_NAME", lit(source_file))

        new_columns = ["INVT_DT"] + [col for col in columns if col != "INVT_DT"] + ["FILE_NAME"]

        final_df=dataframe.select(new_columns)

        # Fetch Header from files
        df_pandas=df_header.to_pandas()
        
        df_header_1=df_pandas.iloc[0,:9]
        df_header_2=df_pandas.iloc[1,9:]

        # Concatenate the DataFrames
        data1 = df_header_1.values.flatten().tolist()
        data2 = df_header_2.values.flatten().tolist()

        combined_data = data1 + data2
        header_columns=df_header.columns

        result_df = pd.DataFrame([combined_data],columns=header_columns)
        df_trimmed = result_df.applymap(lambda x: x.strip() if isinstance(x, str) else x)

        header=session.create_dataframe(df_trimmed)

        header = header.with_column("FILE_NAME", lit(source_file))
        

        # Load Data to the target table
        final_df.write.mode("overwrite").saveAsTable(target_table)

        # Load the header to header target table "SDL_AU_DSTR_sigma_HEADER"

        header.write.mode("overwrite").saveAsTable(Header_table)

        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        header_file="API_out_hdr"

        # write to success folder
    
        file_name_1=file_name.replace("_"," ")[::-1].split(".",1)
        file_name=file_name_1[1][::-1]+''_''+datetime.now().strftime("%Y%m%d%H%M%S")
        final_df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)

        # Write the header dataframe to temp folder under processed
        header.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/temp/"+formatted_year+"/"+formatted_month+"/"+header_file,file_format_type=''csv'',header=True,OVERWRITE=True)


        return "Success"

    
    except KeyError as key_error:
        # Handle KeyError (missing columns) here
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message
        
    except Exception as e:
        # Handle exceptions here
        error_message = f"Error: {str(e)}"
        return error_message
        ';
CREATE OR REPLACE PROCEDURE PCFSDL_RAW.PCF_PERENSO_ORDER_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
PACKAGES = ('openpyxl==3.0.10','snowflake-snowpark-python==*')
HANDLER = 'main'
EXECUTE AS OWNER
AS 'import snowflake.snowpark as snowpark
from snowflake.snowpark.types import StringType, StructType, StructField ,DecimalType
from snowflake.snowpark.functions import lit
from datetime import datetime
import pytz


def get_schema(file_name):
    df_schema=""
    select_col=""
    if file_name.startswith("OrderHdr"):
        df_schema=StructType([
        StructField("ORDER_KEY" , StringType()),
        StructField("ORDER_TYPE_KEY" , StringType()),
        StructField("ACCT_KEY" , StringType()),
        StructField("ORDER_DATE" , StringType()),
        StructField("STATUS" , StringType()),
        StructField("CHARGE" , StringType()),
        StructField("CONFIRMATION" , StringType()),
        StructField("DIARY_ITEM_KEY" , StringType()),
        StructField("WORK_ITEM_KEY" , StringType()),
        StructField("ACCOUNT_ORDER_NO" , StringType()),
        StructField("DELVRY_INSTNS" , StringType()),
            ])
        select_col=["ORDER_KEY","ORDER_TYPE_KEY","ACCT_KEY","ORDER_DATE","STATUS","CHARGE","CONFIRMATION","DIARY_ITEM_KEY","WORK_ITEM_KEY","ACCOUNT_ORDER_NO","DELVRY_INSTNS","RUN_ID","CREATE_DT","FILE_NAME"]
    if file_name.startswith("OrderType"):
        df_schema=StructType([
        StructField("ORDER_TYPE_KEY" , StringType()),
        StructField("ORDER_TYPE_DESC" , StringType()),
        StructField("SOURCE" , StringType())
            ])
        select_col=["ORDER_TYPE_KEY","ORDER_TYPE_DESC","SOURCE","run_id","CREATE_dt","FILE_NAME"]
    if file_name.startswith("OrderBatch"):
        df_schema=StructType([
        StructField("ORDER_KEY" , StringType()),
        StructField("BATCH_KEY" , StringType()),
        StructField("BRANCH_KEY" , StringType()),
        StructField("DIST_ACCT" , StringType()),
        StructField("DELVRY_DT" , StringType()),
        StructField("STATUS" , StringType()),
        StructField("SUFFIX" , StringType()),
        StructField("SENT_DT" , StringType()),
            ])
        select_col=["ORDER_KEY","BATCH_KEY","BRANCH_KEY","DIST_ACCT","DELVRY_DT","STATUS","SUFFIX","SENT_DT","RUN_ID","CREATE_DT","FILE_NAME"]
    if file_name.startswith("OrderDetail"):
        df_schema=StructType([
         StructField("ORDER_KEY", StringType()),
        StructField("BATCH_KEY", StringType()),
        StructField("LINE_KEY", StringType()),
        StructField("PROD_KEY", StringType()),
        StructField("UNIT_QTY", StringType()),
        StructField("ENTERED_QTY", StringType()),
        StructField("ENTERED_UNIT_KEY", StringType()),
        StructField("LIST_PRICE", StringType()),
        StructField("NIS", StringType()),
        StructField("RRP", StringType()),
        StructField("DISC_KEY", StringType()),
        StructField("CREDIT_LINE_KEY", StringType()),
        StructField("CREDITED", StringType())
            ])
        select_col=["ORDER_KEY","BATCH_KEY","LINE_KEY","PROD_KEY","UNIT_QTY","ENTERED_QTY","ENTERED_UNIT_KEY","LIST_PRICE","NIS","RRP","DISC_KEY","CREDIT_LINE_KEY","CREDITED","RUN_ID", "CREATE_DT","FILE_NAME"]
    return df_schema,select_col
    

def main(session: snowpark.Session,Param):
    try:
        # Extracting parameters from the input

        file_name       = Param[0]     
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        db_name         = stage_name.split(".")[0]
        target_table    = db_name+"."+Param[3]
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")


        # Define the schema for the DataFrame
        df_schema,select_col=get_schema(file_name)
        

        df = session.read\\
        .schema(df_schema)\\
        .option("skip_header",1)\\
        .option("field_delimiter", ",")\\
        .option("field_optionally_enclosed_by", "\\"")\\
        .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
        
        df=df.na.drop("all")
        if df.count()==0:
            return "No Data in file"

        df = df.with_column("FILE_NAME",lit(file_name))
        df = df.withColumn("run_id",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        df = df.withColumn("CREATE_dt",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
            
        
                    
        final_df=df.select(select_col)
                        


        
        # write to target table
        final_df.write.mode("append").saveAsTable(target_table)

        file_name=file_name+"_"+datetime.now().strftime("%Y%m%d%H%M%S")
        
        #move to success
        final_df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True,file_format_name="PCFSDL_RAW.MY_CSV_FORMAT")


        return "Success"

    except KeyError as key_error:
        # Handle KeyError (missing columns) here
        error_message = f"KeyError: {str(key_error)}"
        return error_message

    
    except Exception as e:
        # Handle exceptions here
        error_message = f"Error: {str(e)}"
        return error_message';
CREATE OR REPLACE PROCEDURE PCFSDL_RAW.PA_PHARMA_INV_CHS_PREPROCESSING("PARAM" ARRAY)
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
from snowflake.snowpark.functions import col,lit,trim, substring,current_timestamp,to_timestamp
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField,FloatType,DecimalType,DoubleType
import pandas as pd
from datetime import datetime
import pytz

def main(session: snowpark.Session,Param): 

    try:

        #Param=[''ManufacturersReport.csv'',''PCFSDL_RAW.DEV_LOAD_STAGE_ADLS'',''dev/Pacific_reg_inv/transaction/CHS/'',''SDL_CHS_DSTR,SDL_AU_DSTR_CHS_HEADER'']

        # Extracting parameters from the input
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3].split(",")[0]
        Header_table    = Param[3].split(",")[1]


        # Define the schema for the DataFrame
        df_schema=StructType([
            StructField("WAREHOUSE",StringType()),
            StructField("PRODUCT_CODE",StringType()),
            StructField("PRODUCT_DESC",StringType()),
            StructField("SUPPLIER_PRODUCT_CODE",StringType()),
            StructField("PRIMARY_GTIN",StringType()),
            StructField("ABC_CODE",StringType()),
            StructField("STATUS",StringType()),
            StructField("LAST_COST",StringType()),
            StructField("SOH_QTY",StringType()),
            StructField("SOH_AMT",StringType()),
            StructField("SOO_QTY",StringType()),
            StructField("SOO_AMT",StringType()),
            StructField("BACK_ORDER_QTY",StringType()),
            StructField("MONTH_01",StringType()),
            StructField("MONTH_02",StringType()),
            StructField("MONTH_03",StringType()),
            StructField("MONTH_04",StringType()),
            StructField("MONTH_05",StringType()),
            StructField("MONTH_06",StringType()),
            StructField("MONTH_07",StringType()),
            StructField("MONTH_08",StringType()),
            StructField("MONTH_09",StringType()),
            StructField("MONTH_10",StringType()),
            StructField("MONTH_11",StringType()),
            StructField("MONTH_12",StringType())
            ])


        # Read Date and Header for column names
    
        df_header = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "\\u0001")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .option("skip_blank_lines", True)\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)\\
            .limit(2)

        # Read the CSV file into a DataFrame

        dataframe = session.read\\
            .schema(df_schema)\\
            .option("skip_header",3)\\
            .option("field_delimiter", "\\u0001")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .option("skip_blank_lines", True)\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        #---------------------------Transformation logic ------------------------------#

        # Handle null values or empty rows
        dataframe= dataframe.na.drop("all")


        # Check for empty Dataframe
        if dataframe.count()==0:
            return "No Data in file"

        # Extract Header and Date from df_header dataframe

        first_row = df_header.first()
        inv_date=first_row[0]
        
        header= df_header.limit(2).subtract(df_header.limit(1))

        header = header.with_column("FILE_NAME", lit(file_name))

        # Add inv_date to the Dataframe

        dataframe=dataframe.with_column("INV_DT",lit(inv_date).cast("String"))
        dataframe = dataframe.with_column("FILE_NAME", lit(file_name))
        final_df=dataframe.select("INV_DT","WAREHOUSE","PRODUCT_CODE","PRODUCT_DESC","SUPPLIER_PRODUCT_CODE","PRIMARY_GTIN",\\
                                 "ABC_CODE","STATUS","LAST_COST","SOH_QTY","SOH_AMT","SOO_QTY","SOO_AMT","BACK_ORDER_QTY","MONTH_01",\\
                                 "MONTH_02","MONTH_03","MONTH_04","MONTH_05","MONTH_06","MONTH_07","MONTH_08","MONTH_09","MONTH_10","MONTH_11","MONTH_12","FILE_NAME")

        
        # Load Data to the target table
        final_df.write.mode("overwrite").saveAsTable(target_table)

        # Load the header to header target table "SDL_AU_DSTR_CHS_HEADER"

        header.write.mode("overwrite").saveAsTable(Header_table)

        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        header_file="CHS_out_hdr"

        # write to success folder
    
        file_name=file_name.replace("_"," ").split(".")[0]+''_''+datetime.now().strftime("%Y%m%d%H%M%S")
        final_df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)

        # Write the header dataframe to temp folder under processed
        header.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/temp/"+formatted_year+"/"+formatted_month+"/"+header_file,file_format_type=''csv'',header=True,OVERWRITE=True)

        

        return "Success"

    

    except KeyError as key_error:
        # Handle KeyError (missing columns) here
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message
        
    except Exception as e:
        # Handle exceptions here
        error_message = f"Error: {str(e)}"
        return error_message';
CREATE OR REPLACE PROCEDURE PCFSDL_RAW.PCF_PERENSO_ORDER_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
PACKAGES = ('openpyxl==3.0.10','snowflake-snowpark-python==*')
HANDLER = 'main'
EXECUTE AS OWNER
AS 'import snowflake.snowpark as snowpark
from snowflake.snowpark.types import StringType, StructType, StructField ,DecimalType
from snowflake.snowpark.functions import lit
from datetime import datetime
import pytz


def get_schema(file_name):
    df_schema=""
    select_col=""
    if file_name.startswith("OrderHdr"):
        df_schema=StructType([
        StructField("ORDER_KEY" , StringType()),
        StructField("ORDER_TYPE_KEY" , StringType()),
        StructField("ACCT_KEY" , StringType()),
        StructField("ORDER_DATE" , StringType()),
        StructField("STATUS" , StringType()),
        StructField("CHARGE" , StringType()),
        StructField("CONFIRMATION" , StringType()),
        StructField("DIARY_ITEM_KEY" , StringType()),
        StructField("WORK_ITEM_KEY" , StringType()),
        StructField("ACCOUNT_ORDER_NO" , StringType()),
        StructField("DELVRY_INSTNS" , StringType()),
            ])
        select_col=["ORDER_KEY","ORDER_TYPE_KEY","ACCT_KEY","ORDER_DATE","STATUS","CHARGE","CONFIRMATION","DIARY_ITEM_KEY","WORK_ITEM_KEY","ACCOUNT_ORDER_NO","DELVRY_INSTNS","RUN_ID","CREATE_DT","FILE_NAME"]
    if file_name.startswith("OrderType"):
        df_schema=StructType([
        StructField("ORDER_TYPE_KEY" , StringType()),
        StructField("ORDER_TYPE_DESC" , StringType()),
        StructField("SOURCE" , StringType())
            ])
        select_col=["ORDER_TYPE_KEY","ORDER_TYPE_DESC","SOURCE","run_id","create_dt","FILE_NAME"]
    if file_name.startswith("OrderBatch"):
        df_schema=StructType([
        StructField("ORDER_KEY" , StringType()),
        StructField("BATCH_KEY" , StringType()),
        StructField("BRANCH_KEY" , StringType()),
        StructField("DIST_ACCT" , StringType()),
        StructField("DELVRY_DT" , StringType()),
        StructField("STATUS" , StringType()),
        StructField("SUFFIX" , StringType()),
        StructField("SENT_DT" , StringType()),
            ])
        select_col=["ORDER_KEY","BATCH_KEY","BRANCH_KEY","DIST_ACCT","DELVRY_DT","STATUS","SUFFIX","SENT_DT","RUN_ID","CREATE_DT","FILE_NAME"]
    if file_name.startswith("OrderDetail"):
        df_schema=StructType([
         StructField("ORDER_KEY", StringType()),
        StructField("BATCH_KEY", StringType()),
        StructField("LINE_KEY", StringType()),
        StructField("PROD_KEY", StringType()),
        StructField("UNIT_QTY", StringType()),
        StructField("ENTERED_QTY", StringType()),
        StructField("ENTERED_UNIT_KEY", StringType()),
        StructField("LIST_PRICE", StringType()),
        StructField("NIS", StringType()),
        StructField("RRP", StringType()),
        StructField("DISC_KEY", StringType()),
        StructField("CREDIT_LINE_KEY", StringType()),
        StructField("CREDITED", StringType())
            ])
        select_col=["ORDER_KEY","BATCH_KEY","LINE_KEY","PROD_KEY","UNIT_QTY","ENTERED_QTY","ENTERED_UNIT_KEY","LIST_PRICE","NIS","RRP","DISC_KEY","CREDIT_LINE_KEY","CREDITED","RUN_ID", "CREATE_DT","FILE_NAME"]
    return df_schema,select_col
    

def main(session: snowpark.Session,Param):
    try:
        # Extracting parameters from the input

        file_name       = Param[0]     
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        db_name         = stage_name.split(".")[0]
        target_table    = db_name+"."+Param[3]
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")


        # Define the schema for the DataFrame
        df_schema,select_col=get_schema(file_name)
        

        df = session.read\\
        .schema(df_schema)\\
        .option("skip_header",1)\\
        .option("field_delimiter", ",")\\
        .option("field_optionally_enclosed_by", "\\"")\\
        .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
        
        df=df.na.drop("all")
        if df.count()==0:
            return "No Data in file"

        df = df.with_column("FILE_NAME",lit(file_name))
        df = df.withColumn("run_id",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        df = df.withColumn("create_dt",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
            
        
                    
        final_df=df.select(select_col)
                        


        
        # write to target table
        final_df.write.mode("append").saveAsTable(target_table)

        file_name=file_name+"_"+datetime.now().strftime("%Y%m%d%H%M%S")
        
        #move to success
        final_df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True,file_format_name="PCFSDL_RAW.MY_CSV_FORMAT")


        return "Success"

    except KeyError as key_error:
        # Handle KeyError (missing columns) here
        error_message = f"KeyError: {str(key_error)}"
        return error_message

    
    except Exception as e:
        # Handle exceptions here
        error_message = f"Error: {str(e)}"
        return error_message';

CREATE OR REPLACE PROCEDURE PCFSDL_RAW.OVER_AND_ABOVE_POINTS("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS 'from snowflake.snowpark.functions import col, lit, date_format, current_timestamp, to_date, year, month, concat, format_number, regexp_replace,to_timestamp,when,trim,upper
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DecimalType
import snowflake.snowpark
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz
import pandas as pd

def main(session: snowpark.Session,Param):
    #Param=["Over_and_Above_Points.csv","PCFSDL_RAW.DEV_LOAD_STAGE_ADLS","dev/ap_perenso/user_files","sdl_over_and_above_points"]
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        
        df_schema = StructType([
                StructField("display_type", StringType(), True),
                StructField("points", IntegerType(), True)
            ])
        
        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", ",")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

       
        
        df=df.na.drop("all")
        if df.count()==0 :
            return "No Data in file"

       


       
        df = df.with_column("run_id", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        #file_name1=file_name.split(".")[0]+"_"+ datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")+".csv"
        df = df.with_column("file_name", lit(file_name))
        df = df.with_column("create_dt", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))

        snowdf= df.select(
                    "display_type",
                    "points",
                    "run_id",
                    "create_dt",
                    "file_name"
                    )

        snowdf.write.mode("append").saveAsTable(stage_name.split(".")[0] + "." + target_table)


        file_name=file_name.split(".")[0]+"_"+datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        
        #move to success
        snowdf.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)
        
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


CREATE OR REPLACE PROCEDURE PCFSDL_RAW.PERENSO_ACCOUNT_GROUP_RELN_PREPROCESSING("PARAM" ARRAY)
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
    # Param=["AcctGrpRel_20240326223026.csv","PCFSDL_RAW.DEV_LOAD_STAGE_ADLS","dev/ap_perenso/master/account","SDL_PERENSO_ACCOUNT_GROUP_RELN"]
    try:
        file_name = Param[0]
        stage_name = Param[1]
        temp_stage_path = Param[2]
        target_table = Param[3]
        
        df_schema = StructType([
            StructField("acct_key", StringType(), True),
            StructField("acct_grp_key", StringType(), True),   
        ])

        
        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", ",")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
        
        df = df.na.drop("all")
        
        if df.count() == 0:
            return "No Data in file"
            
        df = df.withColumn("run_id", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        df = df.withColumn("create_dt", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        df = df.with_column("FILE_NAME", lit(file_name))

        snowdf = df.select(
            "acct_key",
            "acct_grp_key",
            "run_id",
            "create_dt",
            "FILE_NAME"
        )
        

        
        snowdf.write.mode("append").saveAsTable(stage_name.split(".")[0] + "." + target_table)
        
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        
        #move to success
        file_name=file_name.split(".")[0]+"_"+ datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        
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

CREATE OR REPLACE PROCEDURE PCFSDL_RAW.PERENSO_ACCOUNT_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS 'from snowflake.snowpark.functions import col, lit, date_format, current_timestamp, to_date, year, month, concat, format_number, regexp_replace, to_timestamp, when, trim, upper
from snowflake.snowpark.types import StringType, StructType, StructField
from snowflake.snowpark.files import SnowflakeFile
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz
import csv

def main(session: snowpark.Session,Param):
    # Param=["Account_20240415223028.csv","PCFSDL_RAW.DEV_LOAD_STAGE_ADLS","dev/ap_perenso/master/account","SDL_PERENSO_ACCOUNT"]
    try:
        file_name = Param[0]
        stage_name = Param[1]
        temp_stage_path = Param[2]
        target_table = Param[3] 

        full_path = "@"+stage_name+"/"+temp_stage_path+"/"+file_name
        with SnowflakeFile.open(full_path, ''rb'', require_scoped_url = False) as f:
            df1 = pd.read_csv(f)

        # print(df1)

        snowpark_df = session.create_dataframe(df1)

        # return snowpark_df

        df = snowpark_df.na.drop("all")

        if df.count() == 0:
            return "No Data in file"
            
        df = snowpark_df.withColumn("run_id", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        snowdf = df.withColumn("create_dt", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        snowdf = snowdf.with_column("FILE_NAME", lit(file_name))

        snowdf.write.mode("append").saveAsTable(stage_name.split(".")[0] + "." + target_table)

        
        file_name = file_name.split(".")[0] + "_" + datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        
        # Move to success
        snowdf.write.mode("append").saveAsTable(stage_name.split(".")[0] + "." + target_table)
        snowdf.write.copy_into_location("@" + stage_name + "/" + temp_stage_path + "/processed/success/" + formatted_year + "/" + formatted_month + "/" + file_name, header=True, OVERWRITE=True)
        
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
CREATE OR REPLACE PROCEDURE PCFSDL_RAW.PCF_PERENSO_STORE_CHK_HDR_PREPROCESSING("PARAM" ARRAY)
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
from snowflake.snowpark.functions import col,lit
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField
import pandas as pd
from datetime import datetime
import pytz
 
def main(session: snowpark.Session,Param):
 
    try:
 
        #Param=[''StoreChkHdr_20240320223707.csv'',''PCFSDL_RAW.DEV_LOAD_STAGE_ADLS'',''dev/ap_perenso/transation/headoffice/,''sdl_perenso_store_chk_hdr'']
 
        # Extracting parameters from the input
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
 
 
        # Define the schema for the DataFrame
        df_schema=StructType([
            StructField("STORE_CHK_HDR_KEY",IntegerType()),
            StructField("DIARY_ITEM_KEY",IntegerType()),
            StructField("ACCT_KEY",IntegerType()),
            StructField("WORK_ITEM_KEY",IntegerType()),
            StructField("STORE_CHK_DATE",StringType())
            
            ])
 
        # Read the CSV file into a DataFrame
        dataframe = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", ",")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .option("skip_blank_lines", True)\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
 
        #---------------------------Transformation logic ------------------------------#
 
        # Handle null values or empty rows
        dataframe=dataframe.na.drop("all")
 
        # Check for empty Dataframe
        if dataframe.count()==0:
            return "No Data in file"
    
        dataframe = dataframe.with_column("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        dataframe = dataframe.with_column("CREATE_DT",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        dataframe = dataframe.with_column("FILE_NAME", lit(file_name))
        # Creating copy of the Dataframe
        final_df = dataframe.select("STORE_CHK_HDR_KEY","DIARY_ITEM_KEY","ACCT_KEY","WORK_ITEM_KEY","STORE_CHK_DATE","RUN_ID","CREATE_DT","FILE_NAME")
 
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

CREATE OR REPLACE PROCEDURE PCFSDL_RAW.PCF_PERENSO_PRODUCT_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
PACKAGES = ('openpyxl==3.0.10','snowflake-snowpark-python==*')
HANDLER = 'main'
COMMENT='One SP for 7 Tables'
EXECUTE AS CALLER
AS 'import snowflake.snowpark as snowpark
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField ,DecimalType
from snowflake.snowpark.functions import lit
from datetime import datetime
import pytz


def get_schema(file_name):
    df_schema=""
    select_col=""
    if file_name.startswith("ProdBranchIdentifier"):
        df_schema=StructType([
        StructField(''prod_key'' , IntegerType()),
        StructField(''branch_key'' , IntegerType()),
        StructField(''identifier'' , StringType())
            ])
        select_col=["prod_key","branch_key","identifier","run_id","create_dt"]
    if file_name.startswith("ProdField"):
        df_schema=StructType([
        StructField(''field_key'' , IntegerType()),
        StructField(''field_desc'' , StringType()),
        StructField(''field_type'' , IntegerType()),
        StructField(''active'' , StringType())
            ])
        select_col=["field_key","field_desc","field_type","active","run_id","create_dt"]
    if file_name.startswith("ProdGrp"):
        df_schema=StructType([
        StructField(''prod_grp_key'' , IntegerType()),
        StructField(''Prod_grp_lev_key'' , IntegerType()),
        StructField(''grp_desc'' , StringType()),
        StructField(''dsp_order'' , IntegerType()),
        StructField(''parent_key'' , IntegerType())
            ])
        select_col=["prod_grp_key","Prod_grp_lev_key","grp_desc","dsp_order","parent_key","run_id","create_dt","FILE_NAME"]
    if file_name.startswith("ProdGrpLevel"):
        df_schema=StructType([
        StructField(''Prod_grp_lev_key'' , IntegerType()),
        StructField(''prod_Lev_desc'' , StringType()),
        StructField(''prod_Lev_index'' , IntegerType()),
        StructField(''field_key'' , IntegerType())
            ])
        select_col=["Prod_grp_lev_key","prod_Lev_desc","prod_Lev_index","field_key","run_id","create_dt","FILE_NAME"]
        
    if file_name.startswith("ProdGrpRel"):
        df_schema=StructType([
        StructField(''prod_key'' , IntegerType()),
        StructField(''prod_grp_key'' , IntegerType())
            ])
        select_col=["prod_key","prod_grp_key","run_id","create_dt"]
        
    if file_name.startswith("ProdRelId"):
        df_schema=StructType([
        StructField(''prod_key'' , IntegerType()),
        StructField(''field_key'' , IntegerType()),
        StructField(''id'' , StringType())    
            ])
        select_col=["prod_key","field_key","id","run_id","create_dt"]
        
    if file_name.startswith("Product"):
        df_schema=StructType([
        StructField(''prod_key'' , IntegerType()),
        StructField(''Prod_id'' , StringType()),
        StructField(''Prod_desc'' , StringType()), 
        StructField(''Ean'' , StringType()),
        StructField(''Active'' , StringType()), 
            ])
        select_col=["prod_key","Prod_id","Prod_desc","Ean","Active","run_id","create_dt"]
    return df_schema,select_col
        



def main(session: snowpark.Session,Param): 
    
	# SP call and parameters to pass.
	# CALL PCFSDL_RAW.PCF_PERENSO_PRODUCT_PREPROCESSING([''Product_20240325223028.csv'',''PCFSDL_RAW.DEV_LOAD_STAGE_ADLS'',''dev/ap_perenso/master/product'',''sdl_perenso_product''])
	
    try:
        # Extracting parameters from the input

        file_name       = Param[0]     
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        db_name         = stage_name.split(''.'')[0]
        target_table    = db_name+"."+Param[3]
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")


        # Define the schema for the DataFrame
        df_schema,select_col=get_schema(file_name)
        print(df_schema)
        print(select_col)
        # Set the current session schema
        
        session.use_schema(stage_name.split(''.'')[0])

        
        
        stage_path="@{0}/{1}/{2}".format(stage_name,temp_stage_path,file_name)
        print(stage_path)
        
        temp_df = session.read\\
        .schema(df_schema)\\
        .option("skip_header",1)\\
        .option("field_delimiter", ",")\\
        .option("field_optionally_enclosed_by", "\\"")\\
        .csv(stage_path)
        
        temp_df=temp_df.na.drop("all")
        if temp_df.count()==0:
            return "No Data in file"

            
        temp_df = temp_df.withColumn("run_id",lit(lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S"))))
        temp_df = temp_df.withColumn("create_dt",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        temp_df = temp_df.with_column("FILE_NAME", lit(file_name))    
        
                    
        final_df=temp_df.select(select_col)
                        


        file_name=file_name+''_''+datetime.now().strftime("%Y%m%d%H%M%S")
        
        # write to target table
        final_df.write.mode("append").saveAsTable(target_table)
        
        #move to success
        final_df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True,file_format_name="PCFSDL_RAW.MY_CSV_FORMAT")


        return ''Success''

    except KeyError as key_error:
        # Handle KeyError (missing columns) here
        error_message = f"KeyError: {str(key_error)}"
        return error_message

    
    except Exception as e:
        # Handle exceptions here
        error_message = f"Error: {str(e)}"
        return error_message';

CREATE OR REPLACE PROCEDURE PCFSDL_RAW.PCF_PERENSO_PRODUCT_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
PACKAGES = ('openpyxl==3.0.10','snowflake-snowpark-python==*')
HANDLER = 'main'
COMMENT='One SP for 7 Tables'
EXECUTE AS CALLER
AS 'import snowflake.snowpark as snowpark
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField ,DecimalType
from snowflake.snowpark.functions import lit
from datetime import datetime
import pytz


def get_schema(file_name):
    df_schema=""
    select_col=""
    if file_name.startswith("ProdBranchIdentifier"):
        df_schema=StructType([
        StructField(''prod_key'' , IntegerType()),
        StructField(''branch_key'' , IntegerType()),
        StructField(''identifier'' , StringType())
            ])
        select_col=["prod_key","branch_key","identifier","run_id","create_dt","FILE_NAME"]
    if file_name.startswith("ProdField"):
        df_schema=StructType([
        StructField(''field_key'' , IntegerType()),
        StructField(''field_desc'' , StringType()),
        StructField(''field_type'' , IntegerType()),
        StructField(''active'' , StringType())
            ])
        select_col=["field_key","field_desc","field_type","active","run_id","create_dt","FILE_NAME"]
    if file_name.startswith("ProdGrp"):
        df_schema=StructType([
        StructField(''prod_grp_key'' , IntegerType()),
        StructField(''Prod_grp_lev_key'' , IntegerType()),
        StructField(''grp_desc'' , StringType()),
        StructField(''dsp_order'' , IntegerType()),
        StructField(''parent_key'' , IntegerType())
            ])
        select_col=["prod_grp_key","Prod_grp_lev_key","grp_desc","dsp_order","parent_key","run_id","create_dt","FILE_NAME"]
    if file_name.startswith("ProdGrpLevel"):
        df_schema=StructType([
        StructField(''Prod_grp_lev_key'' , IntegerType()),
        StructField(''prod_Lev_desc'' , StringType()),
        StructField(''prod_Lev_index'' , IntegerType()),
        StructField(''field_key'' , IntegerType())
            ])
        select_col=["Prod_grp_lev_key","prod_Lev_desc","prod_Lev_index","field_key","run_id","create_dt","FILE_NAME"]
        
    if file_name.startswith("ProdGrpRel"):
        df_schema=StructType([
        StructField(''prod_key'' , IntegerType()),
        StructField(''prod_grp_key'' , IntegerType())
            ])
        select_col=["prod_key","prod_grp_key","run_id","create_dt","FILE_NAME"]
        
    if file_name.startswith("ProdRelId"):
        df_schema=StructType([
        StructField(''prod_key'' , IntegerType()),
        StructField(''field_key'' , IntegerType()),
        StructField(''id'' , StringType())    
            ])
        select_col=["prod_key","field_key","id","run_id","create_dt","FILE_NAME"]
        
    if file_name.startswith("Product"):
        df_schema=StructType([
        StructField(''prod_key'' , IntegerType()),
        StructField(''Prod_id'' , StringType()),
        StructField(''Prod_desc'' , StringType()), 
        StructField(''Ean'' , StringType()),
        StructField(''Active'' , StringType()), 
            ])
        select_col=["prod_key","Prod_id","Prod_desc","Ean","Active","run_id","create_dt","FILE_NAME"]
    return df_schema,select_col
        



def main(session: snowpark.Session,Param): 
    
	# SP call and parameters to pass.
	# CALL PCFSDL_RAW.PCF_PERENSO_PRODUCT_PREPROCESSING([''Product_20240325223028.csv'',''PCFSDL_RAW.DEV_LOAD_STAGE_ADLS'',''dev/ap_perenso/master/product'',''sdl_perenso_product''])
	
    try:
        # Extracting parameters from the input

        file_name       = Param[0]     
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        db_name         = stage_name.split(''.'')[0]
        target_table    = db_name+"."+Param[3]
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")


        # Define the schema for the DataFrame
        df_schema,select_col=get_schema(file_name)
        print(df_schema)
        print(select_col)
        # Set the current session schema
        
        session.use_schema(stage_name.split(''.'')[0])

        
        
        stage_path="@{0}/{1}/{2}".format(stage_name,temp_stage_path,file_name)
        print(stage_path)
        
        temp_df = session.read\\
        .schema(df_schema)\\
        .option("skip_header",1)\\
        .option("field_delimiter", ",")\\
        .option("field_optionally_enclosed_by", "\\"")\\
        .csv(stage_path)
        
        temp_df=temp_df.na.drop("all")
        if temp_df.count()==0:
            return "No Data in file"

            
        temp_df = temp_df.withColumn("run_id",lit(lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S"))))
        temp_df = temp_df.withColumn("create_dt",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        temp_df = temp_df.with_column("FILE_NAME", lit(file_name))    
        
                    
        final_df=temp_df.select(select_col)
                        


        file_name=file_name+''_''+datetime.now().strftime("%Y%m%d%H%M%S")
        
        # write to target table
        final_df.write.mode("append").saveAsTable(target_table)
        
        #move to success
        final_df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True,file_format_name="PCFSDL_RAW.MY_CSV_FORMAT")


        return ''Success''

    except KeyError as key_error:
        # Handle KeyError (missing columns) here
        error_message = f"KeyError: {str(key_error)}"
        return error_message

    
    except Exception as e:
        # Handle exceptions here
        error_message = f"Error: {str(e)}"
        return error_message';

CREATE OR REPLACE PROCEDURE PCFSDL_RAW.PCF_PERENSO_ORDER_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
PACKAGES = ('openpyxl==3.0.10','snowflake-snowpark-python==*')
HANDLER = 'main'
EXECUTE AS OWNER
AS 'import snowflake.snowpark as snowpark
from snowflake.snowpark.types import StringType, StructType, StructField ,DecimalType
from snowflake.snowpark.functions import lit
from datetime import datetime
import pytz


def get_schema(file_name):
    df_schema=""
    select_col=""
    if file_name.startswith("OrderHdr"):
        df_schema=StructType([
        StructField("ORDER_KEY" , StringType()),
        StructField("ORDER_TYPE_KEY" , StringType()),
        StructField("ACCT_KEY" , StringType()),
        StructField("ORDER_DATE" , StringType()),
        StructField("STATUS" , StringType()),
        StructField("CHARGE" , StringType()),
        StructField("CONFIRMATION" , StringType()),
        StructField("DIARY_ITEM_KEY" , StringType()),
        StructField("WORK_ITEM_KEY" , StringType()),
        StructField("ACCOUNT_ORDER_NO" , StringType()),
        StructField("DELVRY_INSTNS" , StringType()),
            ])
        select_col=["ORDER_KEY","ORDER_TYPE_KEY","ACCT_KEY","ORDER_DATE","STATUS","CHARGE","CONFIRMATION","DIARY_ITEM_KEY","WORK_ITEM_KEY","ACCOUNT_ORDER_NO","DELVRY_INSTNS","RUN_ID","CREATE_DT","FILE_NAME"]
    if file_name.startswith("OrderType"):
        df_schema=StructType([
        StructField("ORDER_TYPE_KEY" , StringType()),
        StructField("ORDER_TYPE_DESC" , StringType()),
        StructField("SOURCE" , StringType())
            ])
        select_col=["ORDER_TYPE_KEY","ORDER_TYPE_DESC","SOURCE","run_id","create_dt","FILE_NAME"]
    if file_name.startswith("OrderBatch"):
        df_schema=StructType([
        StructField("ORDER_KEY" , StringType()),
        StructField("BATCH_KEY" , StringType()),
        StructField("BRANCH_KEY" , StringType()),
        StructField("DIST_ACCT" , StringType()),
        StructField("DELVRY_DT" , StringType()),
        StructField("STATUS" , StringType()),
        StructField("SUFFIX" , StringType()),
        StructField("SENT_DT" , StringType()),
            ])
        select_col=["ORDER_KEY","BATCH_KEY","BRANCH_KEY","DIST_ACCT","DELVRY_DT","STATUS","SUFFIX","SENT_DT","RUN_ID","CREATE_DT","FILE_NAME"]
    if file_name.startswith("OrderDetail"):
        df_schema=StructType([
         StructField("ORDER_KEY", StringType()),
        StructField("BATCH_KEY", StringType()),
        StructField("LINE_KEY", StringType()),
        StructField("PROD_KEY", StringType()),
        StructField("UNIT_QTY", StringType()),
        StructField("ENTERED_QTY", StringType()),
        StructField("ENTERED_UNIT_KEY", StringType()),
        StructField("LIST_PRICE", StringType()),
        StructField("NIS", StringType()),
        StructField("RRP", StringType()),
        StructField("DISC_KEY", StringType()),
        StructField("CREDIT_LINE_KEY", StringType()),
        StructField("CREDITED", StringType())
            ])
        select_col=["ORDER_KEY","BATCH_KEY","LINE_KEY","PROD_KEY","UNIT_QTY","ENTERED_QTY","ENTERED_UNIT_KEY","LIST_PRICE","NIS","RRP","DISC_KEY","CREDIT_LINE_KEY","CREDITED","RUN_ID", "CREATE_DT","FILE_NAME"]
    return df_schema,select_col
    

def main(session: snowpark.Session,Param):
    try:
        # Extracting parameters from the input

        file_name       = Param[0]     
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        db_name         = stage_name.split(".")[0]
        target_table    = db_name+"."+Param[3]
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")


        # Define the schema for the DataFrame
        df_schema,select_col=get_schema(file_name)
        

        df = session.read\\
        .schema(df_schema)\\
        .option("skip_header",1)\\
        .option("field_delimiter", ",")\\
        .option("field_optionally_enclosed_by", "\\"")\\
        .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
        
        df=df.na.drop("all")
        if df.count()==0:
            return "No Data in file"

        df = df.with_column("FILE_NAME",lit(file_name))
        df = df.withColumn("run_id",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        df = df.withColumn("create_dt",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
            
        
                    
        final_df=df.select(select_col)
                        


        
        # write to target table
        final_df.write.mode("append").saveAsTable(target_table)

        file_name=file_name+"_"+datetime.now().strftime("%Y%m%d%H%M%S")
        
        #move to success
        final_df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True,file_format_name="PCFSDL_RAW.MY_CSV_FORMAT")


        return "Success"

    except KeyError as key_error:
        # Handle KeyError (missing columns) here
        error_message = f"KeyError: {str(key_error)}"
        return error_message

    
    except Exception as e:
        # Handle exceptions here
        error_message = f"Error: {str(e)}"
        return error_message';

CREATE OR REPLACE PROCEDURE PCFSDL_RAW.PA_PHARMA_INV_SIGMA_PREPROCESSING("PARAM" ARRAY)
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
from snowflake.snowpark.functions import col,lit,trim, substring,current_timestamp,to_timestamp,date_format
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField,FloatType,DecimalType,DoubleType
import pandas as pd
from datetime import datetime
import pytz

def main(session: snowpark.Session,Param): 

    try:

        #Param=[''Replenishment_E3_Buyers_Report.csv'',''PCFSDL_RAW.DEV_LOAD_STAGE_ADLS'',''dev/Pacific_reg_inv/transaction/Sigma/'',''SDL_sigma_DSTR,SDL_AU_DSTR_sigma_HEADER'']

        # Extracting parameters from the input
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3].split(",")[0]
        Header_table    = Param[3].split(",")[1]


        # Define the schema for the DataFrame
        df_schema=StructType([
            StructField("WAREHOUSE_DESC",StringType()),
            StructField("PRODUCT_CODE",StringType()),
            StructField("PRODUCT_DESC",StringType()),
            StructField("TEMP",StringType()),
            StructField("SUPPLIER_PRODUCT_CODE",StringType()),
            StructField("EAN",StringType()),
            StructField("VOLUME_CLAS_CODE",StringType()),
            StructField("HANDLING_STATUS",StringType()),
            StructField("COST_PRICE",StringType()),
            StructField("SOH_QTY",StringType()),
            StructField("SOH_AMT",StringType()),
            StructField("STOCK_IN_TRANSIT_QTY",StringType()),
            StructField("STOCK_IN_TRANSIT_AMT",StringType()),
            StructField("RESTRICTED_STOCK_QTY",StringType()),
            StructField("RESTRICTED_STOCK_AMT",StringType()),
            StructField("SOO_QTY",StringType()),
            StructField("SOO_AMT",StringType()),
            StructField("BACK_ORDER_QTY",StringType()),
            StructField("BACK_ORDER_AMT",StringType()),
            StructField("MONTH_01",StringType()),
            StructField("MONTH_02",StringType()),
            StructField("MONTH_03",StringType()),
            StructField("MONTH_04",StringType()),
            StructField("MONTH_05",StringType()),
            StructField("MONTH_06",StringType()),
            StructField("MONTH_07",StringType()),
            StructField("MONTH_08",StringType()),
            StructField("MONTH_09",StringType()),
            StructField("MONTH_10",StringType()),
            StructField("MONTH_11",StringType()),
            StructField("MONTH_12",StringType()),
            StructField("MONTH_13",StringType()),
            StructField("MONTH_14",StringType()),
            StructField("MONTH_15",StringType()),
            StructField("MONTH_16",StringType())
            ])


        # Read Date and Header for column names
    
        df_header = session.read\\
            .schema(df_schema)\\
            .option("skip_header",3)\\
            .option("field_delimiter", "\\u0001")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .option("skip_blank_lines", True)\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)\\
            .limit(1)

        # Read the CSV file into a DataFrame

        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",20)\\
            .option("field_delimiter", "\\u0001")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .option("skip_blank_lines", True)\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        #---------------------------Transformation logic ------------------------------#

        # Handle null values or empty rows
        df= df.na.drop("all")


        # Check for empty Dataframe
        if df.count()==0:
            return "No Data in file"

        # Fetch value for inv_date column

        first_row = df_header.first()
        date=first_row[0]
        inv_date=date.split(" ")[-1]


        # Fetch header columns required for Header Table
        header_columns=df.limit(1)
      

        header = header_columns.withColumn(''MONTH_01'', date_format(col(''MONTH_01''), ''YYYY - MMMM''))\\
        .withColumn(''MONTH_02'', date_format(col(''MONTH_02''), ''YYYY - MMMM''))\\
        .withColumn(''MONTH_03'', date_format(col(''MONTH_03''), ''YYYY - MMMM''))\\
        .withColumn(''MONTH_04'', date_format(col(''MONTH_04''), ''YYYY - MMMM''))\\
        .withColumn(''MONTH_05'', date_format(col(''MONTH_05''), ''YYYY - MMMM''))\\
        .withColumn(''MONTH_06'', date_format(col(''MONTH_06''), ''YYYY - MMMM''))\\
        .withColumn(''MONTH_07'', date_format(col(''MONTH_07''), ''YYYY - MMMM''))\\
        .withColumn(''MONTH_08'', date_format(col(''MONTH_08''), ''YYYY - MMMM''))\\
        .withColumn(''MONTH_09'', date_format(col(''MONTH_09''), ''YYYY - MMMM''))\\
        .withColumn(''MONTH_10'', date_format(col(''MONTH_10''), ''YYYY - MMMM''))\\
        .withColumn(''MONTH_11'', date_format(col(''MONTH_11''), ''YYYY - MMMM''))\\
        .withColumn(''MONTH_12'', date_format(col(''MONTH_12''), ''YYYY - MMMM''))\\
        .withColumn(''MONTH_13'', date_format(col(''MONTH_13''), ''YYYY - MMMM''))\\
        .withColumn(''MONTH_14'', date_format(col(''MONTH_14''), ''YYYY - MMMM''))\\
        .withColumn(''MONTH_15'', date_format(col(''MONTH_15''), ''YYYY - MMMM''))\\
        .withColumn(''MONTH_16'', date_format(col(''MONTH_16''), ''YYYY - MMMM''))

        source_file=file_name.split(".")[0]+".xlsx"

        header = header.with_column("FILE_NAME", lit(source_file))
        


        # Filter Rows with values "Warehouse Desc" and  "product code" and "Product Desc"
        dataframe= df.filter((df["WAREHOUSE_DESC"] != "Warehouse Desc") & (df["PRODUCT_CODE"] != "product code") & (df["PRODUCT_DESC"] != "Product Desc"))

        # Add inv_date to the dataframe
        dataframe=dataframe.with_column("INV_DATE",lit(inv_date).cast("String"))
        dataframe = dataframe.with_column("FILE_NAME", lit(source_file))

        columns = df.columns
        new_columns = ["INV_DATE"] + [col for col in columns if col != "INV_DATE"] + ["FILE_NAME"]

        final_df=dataframe.select(new_columns)

        # Load Data to the target table
        final_df.write.mode("overwrite").saveAsTable(target_table)

        # Load the header to header target table "SDL_AU_DSTR_sigma_HEADER"

        header.write.mode("overwrite").saveAsTable(Header_table)

        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        header_file="Sigma_out_hdr"

        # write to success folder
    
        file_name=file_name.replace("_"," ").split(".")[0]+''_''+datetime.now().strftime("%Y%m%d%H%M%S")
        final_df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)

        # Write the header dataframe to temp folder under processed
        header.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/temp/"+formatted_year+"/"+formatted_month+"/"+header_file,file_format_type=''csv'',header=True,OVERWRITE=True)


        return "Success"

    
    except KeyError as key_error:
        # Handle KeyError (missing columns) here
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message
        
    except Exception as e:
        # Handle exceptions here
        error_message = f"Error: {str(e)}"
        return error_message
        ';

CREATE OR REPLACE PROCEDURE PCFSDL_RAW.PERENSO_ACCOUNT_TYPE_PREPROCESSING("PARAM" ARRAY)
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
    #Param=["AccountType_2024032622302.csv","PCFSDL_RAW.DEV_LOAD_STAGE_ADLS","dev/ap_perenso/master/account","SDL_PERENSO_ACCOUNT_TYPE"]
    try:
        file_name = Param[0]
        stage_name = Param[1]
        temp_stage_path = Param[2]
        target_table = Param[3]
        
        df_schema = StructType([
            StructField("acct_type_key", StringType(), True),
            StructField("acct_type_desc", StringType(), True),   
            StructField("dsp_order", StringType(), True),
            StructField("active", StringType(), True),
        ])

     

        
        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", ",")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
        
        df = df.na.drop("all")
        
        if df.count() == 0:
            return "No Data in file"
            
        df = df.withColumn("run_id", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        df = df.withColumn("create_dt", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        df = df.with_column("FILE_NAME", lit(file_name))

        snowdf = df.select(
            "acct_type_key",
            "acct_type_desc",
            "dsp_order",
            "active",
            "run_id",
            "create_dt",
            "FILE_NAME"
        )
        

        
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

CREATE OR REPLACE PROCEDURE PCFSDL_RAW.PCF_PERENSO_RANGING_PREPROCESSING("PARAM" ARRAY)
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
from snowflake.snowpark.functions import col,lit
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField
import pandas as pd
from datetime import datetime
import pytz
 
def main(session: snowpark.Session,Param):
 
    try:
 
        #Param=[''Ranging_20240320223707.csv'',''PCFSDL_RAW.DEV_LOAD_STAGE_ADLS'',''dev/ap_perenso/transation/ranging/'',,''sdl_perenso_ranging'']
 
        # Extracting parameters from the input
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
 
 
        # Define the schema for the DataFrame
        df_schema=StructType([
            StructField("RANGING_KEY",IntegerType()),
            StructField("RANGING_DESC",StringType()),
            StructField("ACCT_GRP_LEV_KEY",IntegerType()),
            StructField("ACTIVE",StringType()),
            StructField("ALL_ACCOUNTS",StringType())
            
            ])
 
        # Read the CSV file into a DataFrame
        dataframe = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", ",")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .option("skip_blank_lines", True)\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
 
        #---------------------------Transformation logic ------------------------------#
 
        # Handle null values or empty rows
        dataframe=dataframe.na.drop("all")
 
        # Check for empty Dataframe
        if dataframe.count()==0:
            return "No Data in file"
    
        dataframe = dataframe.with_column("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        dataframe = dataframe.with_column("CREATE_DT",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        dataframe = dataframe.with_column("FILE_NAME", lit(file_name))
        # Creating copy of the Dataframe
        final_df = dataframe.select("RANGING_KEY", "RANGING_DESC", "ACCT_GRP_LEV_KEY", "ACTIVE", "ALL_ACCOUNTS","RUN_ID","CREATE_DT","FILE_NAME")
 
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
CREATE OR REPLACE PROCEDURE PCFSDL_RAW.PERENSO_ACCOUNT_GROUP_PREPROCESSING("PARAM" ARRAY)
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
   # Param=["AcctGrp_20240326223026.csv","PCFSDL_RAW.DEV_LOAD_STAGE_ADLS","dev/ap_perenso/master/account","SDL_PERENSO_ACCOUNT_GROUP"]
    try:
        file_name = Param[0]
        stage_name = Param[1]
        temp_stage_path = Param[2]
        target_table = Param[3]
        
        df_schema = StructType([
            StructField("acct_grp_key", StringType(), True),
            StructField("acct_grp_lev_key", StringType(), True),   
            StructField("grp_desc", StringType(), True),
            StructField("dsp_order", StringType(), True),
            StructField("parent_key", StringType(), True),
        ])

        
        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", ",")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
        
        df = df.na.drop("all")
        
        if df.count() == 0:
            return "No Data in file"
            
        df = df.withColumn("run_id", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        df = df.withColumn("CREATE_dt", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        df = df.with_column("FILE_NAME", lit(file_name))

        snowdf = df.select(
            "acct_grp_key",
            "acct_grp_lev_key",
            "grp_desc",
            "dsp_order",
            "parent_key",
            "run_id",
            "CREATE_dt",
            "FILE_NAME"
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
CREATE OR REPLACE PROCEDURE PCFSDL_RAW.PCF_PERENSO_DIARY_ITEM_TYPE_PREPROCESSING("PARAM" ARRAY)
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
from snowflake.snowpark.functions import col,lit
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField
import pandas as pd
from datetime import datetime
import pytz
 
def main(session: snowpark.Session,Param):
 
    try:
 
        #Param=[''DiaryItemType_20240320223707.csv'',''PCFSDL_RAW.DEV_LOAD_STAGE_ADLS'',''dev/ap_perenso/transation/call/'',''sdl_perenso_diary_item_type'']
 
        # Extracting parameters from the input
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
 
 
        # Define the schema for the DataFrame
        df_schema=StructType([
            StructField("DIARY_ITEM_TYPE_KEY",StringType()),
            StructField("DIARY_ITEM_TYPE_DESC",StringType()),
            StructField("DSP_ORDER",IntegerType()),
            StructField("ACTIVE",StringType()),
            StructField("CATEGORY",IntegerType()),
            StructField("COUNT_IN_CALL_RATE",StringType())
            
            ])
 
 
        # Read the CSV file into a DataFrame
        dataframe = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", ",")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .option("skip_blank_lines", True)\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
 
        #---------------------------Transformation logic ------------------------------#
 
        # Handle null values or empty rows
        dataframe=dataframe.na.drop("all")
 
        # Check for empty Dataframe
        if dataframe.count()==0:
            return "No Data in file"

        
        dataframe = dataframe.with_column("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        dataframe = dataframe.with_column("CREATE_DT",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        dataframe = dataframe.with_column("FILE_NAME", lit(file_name))
        
        # Creating copy of the Dataframe
        final_df = dataframe.select("DIARY_ITEM_TYPE_KEY", "DIARY_ITEM_TYPE_DESC", "DSP_ORDER", "ACTIVE","CATEGORY",\\
                                    "COUNT_IN_CALL_RATE","RUN_ID","CREATE_DT","FILE_NAME")
 
        # Load Data to the target table
        final_df.write.mode("append").saveAsTable(target_table)
 
         
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
 
        # write to success folder
        file_name=file_name.split(".")[0]
        final_df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)
        return "Success"
        #return final_df
 
    
 
    except KeyError as key_error:
        # Handle KeyError (missing columns) here
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message
    except Exception as e:
        # Handle exceptions here
        error_message = f"Error: {str(e)}"
        return error_message';
CREATE OR REPLACE PROCEDURE PCFSDL_RAW.PERENSO_DISTRIBUTOR_DETAIL_PREPROCESSING("PARAM" ARRAY)
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
    # Param=["DistributorDetail_20240326223026.csv","PCFSDL_RAW.DEV_LOAD_STAGE_ADLS","dev/ap_perenso/master/account","SDL_PERENSO_DISTRIBUTOR_DETAIL"]
    try:
        file_name = Param[0]
        stage_name = Param[1]
        temp_stage_path = Param[2]
        target_table = Param[3]
        
        df_schema = StructType([
            StructField("dist_key", StringType(), True),
            StructField("distributor", StringType(), True),   
            StructField("dist_id", StringType(), True),
            StructField("branch_key", StringType(), True),
            StructField("display_name", StringType(), True),
            StructField("short_name", StringType(), True),
            StructField("active", StringType(), True),
        ])


        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", ",")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
        
        df = df.na.drop("all")
        
        if df.count() == 0:
            return "No Data in file"
            
        df = df.withColumn("run_id", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        df = df.withColumn("CREATE_dt", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        df = df.with_column("FILE_NAME", lit(file_name))

        snowdf = df.select(
            "dist_key",
            "distributor",
            "dist_id",
            "branch_key",
            "display_name",
            "short_name",
            "active",
            "run_id",
            "CREATE_dt",
            "FILE_NAME"
        )
        

        
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
CREATE OR REPLACE PROCEDURE PCFSDL_RAW.PA_PHARMA_INV_SYMBION_PREPROCESSING("PARAM" ARRAY)
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
from snowflake.snowpark.functions import col,lit
from snowflake.snowpark.types import StringType, StructType, StructField
import pandas as pd
from datetime import datetime
import pytz
 
def main(session: snowpark.Session,Param):
 
    try:
 
        #Param=[''Stock_Status_by_DC_-_13_Months_Sales.csv'',''PCFSDL_RAW.DEV_LOAD_STAGE_ADLS'',''dev/Pacific_reg_inv/transaction/Symbion'',''SDL_symbion_DSTR,SDL_AU_DSTR_symbion_HEADER'']
 
        # Extracting parameters from the input
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3].split(",")[0]
        Header_table    = Param[3].split(",")[1]
 
 
        # Define the schema for the DataFrame
        df_schema=StructType([
            StructField("STATE", StringType()),
            StructField("WAREHOUSE", StringType()),
            StructField("WAREHOUSE_DESC", StringType()),
            StructField("SYMBION_PRODUCT_NO", StringType()),
            StructField("SYMBION_PRODUCT_DESC", StringType()),
            StructField("SUPPLIER_PART_NO", StringType()),
            StructField("EAN", StringType()),
            StructField("GLOBAL_STD_COST", StringType()),
            StructField("DATE_INTRODUCED", StringType()),
            StructField("DATE_LAST_RECIVED", StringType()),
            StructField("OOSR", StringType()),
            StructField("SOH_AMT", StringType()),
            StructField("SOH_QTY", StringType()),
            StructField("ON_ORDER", StringType()),
            StructField("BACK_ORDER", StringType()),
            StructField("RESERVED_AMT_FOR_ORDER", StringType()),
            StructField("RESERVED_QTY_FOR_ORDER", StringType()),
            StructField("RESERVED_AMT_FOR_QA", StringType()),
            StructField("RESERVED_QTY_FOR_QA", StringType()),
            StructField("AVAILABLE_AMT", StringType()),
            StructField("AVAILABLE_QTY", StringType()),
            StructField("MTD", StringType()),
            StructField("MONTH_01", StringType()),
            StructField("MONTH_02", StringType()),
            StructField("MONTH_03", StringType()),
            StructField("MONTH_04", StringType()),
            StructField("MONTH_05", StringType()),
            StructField("MONTH_06", StringType()),
            StructField("MONTH_07", StringType()),
            StructField("MONTH_08", StringType()),
            StructField("MONTH_09", StringType()),
            StructField("MONTH_10", StringType()),
            StructField("MONTH_11", StringType()),
            StructField("MONTH_12", StringType()),
            StructField("MONTH_13", StringType())
            
            ])
 
 
        # Read the CSV file into a DataFrame
        df_header = session.read\\
            .schema(df_schema)\\
            .option("skip_header",3)\\
            .option("field_delimiter", "\\u0001")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .option("skip_blank_lines", True)\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)\\
            .limit(3)

        dataframe = session.read\\
            .schema(df_schema)\\
            .option("skip_header",7)\\
            .option("field_delimiter", "\\u0001")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .option("skip_blank_lines", True)\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)\\


    
 
        #---------------------------Transformation logic ------------------------------#
 
        #Handle null values or empty rows
        dataframe=dataframe.na.drop("all")
 
        # Check for empty Dataframe
        if dataframe.count()==0:
            return "No Data in file"

        # Extract the date from the header
        first_row = df_header.first()
        date = first_row[0]
        date = date.split(",")
        datePart = "".join(date[-2:]) 
        
        header= df_header.limit(3).subtract(df_header.limit(2))

        source_file=file_name.split(".")[0].replace("_"," ") +".xlsx"

        # ADD filename column to header dataframe
        header = header.with_column("FILE_NAME", lit(source_file))

        # Add INV_DT column with the concatenated date to the data DataFrame and filename column
        dataframe = dataframe.withColumn("INV_DT", lit(datePart).cast("String"))
        dataframe = dataframe.with_column("FILE_NAME", lit(source_file))
  
    
        # Return the DataFrame with INV_DT column
        #Creating copy of the Dataframe
        final_df = dataframe.select("INV_DT","STATE","WAREHOUSE","WAREHOUSE_DESC","SYMBION_PRODUCT_NO","SYMBION_PRODUCT_DESC","SUPPLIER_PART_NO","EAN","GLOBAL_STD_COST","DATE_INTRODUCED","DATE_LAST_RECIVED","OOSR","SOH_AMT",\\
                                    "SOH_QTY","ON_ORDER","BACK_ORDER","RESERVED_AMT_FOR_ORDER","RESERVED_QTY_FOR_ORDER","RESERVED_AMT_FOR_QA","RESERVED_QTY_FOR_QA","AVAILABLE_AMT","AVAILABLE_QTY","MTD","MONTH_01","MONTH_02","MONTH_03",\\
                                    "MONTH_04","MONTH_05","MONTH_06","MONTH_07","MONTH_08","MONTH_09","MONTH_10","MONTH_11","MONTH_12","MONTH_13","FILE_NAME")

        # Load Data to the target table
        final_df.write.mode("overwrite").saveAsTable(target_table)

        # Load the header to header target table "SDL_AU_DSTR_CHS_HEADER"

        header.write.mode("overwrite").saveAsTable(Header_table)

        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        header_file="Symbion_out_hdr"

        # write to success folder
    
        file_name=file_name.replace("_"," ").split(".")[0]+''_''+datetime.now().strftime("%Y%m%d%H%M%S")
        final_df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)

        # Write the header dataframe to temp folder under processed
        header.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/temp/"+formatted_year+"/"+formatted_month+"/"+header_file,file_format_type=''csv'',header=True,OVERWRITE=True)

        

        return "Success"
 
    
 
    except KeyError as key_error:
        # Handle KeyError (missing columns) here
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message
    except Exception as e:
        # Handle exceptions here
        error_message = f"Error: {str(e)}"
        return error_message';
CREATE OR REPLACE PROCEDURE PCFSDL_RAW.ANZ_PERENSO_OVER_AND_ABOVE_PREPROCESSING("PARAM" ARRAY)
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
from snowflake.snowpark.types import  StringType, StructType, StructField,IntegerType
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz



def main(session: snowpark.Session,Param): 
       
    try :

        #Param=["OverandAbove_20240320223707.csv","PCFSDL_RAW.DEV_LOAD_STAGE_ADLS","dev/ap_perenso/transaction/overandabove","sdl_perenso_Over_and_Above"]

        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        # Define the schema for the DataFrame
        
        df_schema = StructType([
                StructField("over_and_above_key", IntegerType()),
                StructField("acct_key", IntegerType()),
                StructField("todo_option_key", IntegerType()),
                StructField("prod_grp_key", IntegerType()),
                StructField("activated", StringType()),
                StructField("notes", StringType())
            ])

   
        
        dataframe = session.read.schema(df_schema) \\
        .option("skip_header",1) \\
        .option("field_delimiter", ",") \\
        .option("field_optionally_enclosed_by", "\\"")  \\
        .option("encoding","UTF-8")\\
        .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        
        dataframe=dataframe.na.drop("all")
        if dataframe.count()==0:
            return "No Data in file"

        dataframe = dataframe.with_column("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        dataframe = dataframe.with_column("CRTD_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        dataframe = dataframe.with_column("FILE_NAME", lit(file_name))

        # Load Data to the target table
        dataframe.write.mode("append").saveAsTable(target_table)
       
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        #move to success
        dataframe.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)

        
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
CREATE OR REPLACE PROCEDURE PCFSDL_RAW.PCF_PERENSO_CALL_OBJECTIVES_PREPROCESSING("PARAM" ARRAY)
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
from snowflake.snowpark.functions import col,lit
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField
import pandas as pd
from datetime import datetime
import pytz
 
def main(session: snowpark.Session,Param):
 
    try:
 
        #Param=[''CallObjectives_20240320223707.csv'',''PCFSDL_RAW.DEV_LOAD_STAGE_ADLS'',''dev/ap_perenso/transation/call/'',''sdl_perenso_call_objectives'']
 
        # Extracting parameters from the input
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
 
 
        # Define the schema for the DataFrame
        df_schema=StructType([
            StructField("OBJECTIVE_KEY",StringType()),
            StructField("CALL_CREATED_DT",StringType()),
            StructField("DESCRIPTION",StringType()),
            StructField("ACCT_KEY",StringType()),
            StructField("ASSIGNED_USER_KEY",StringType()),
            StructField("CREATED_USER_KEY",StringType()),
            StructField("DUE_DT",StringType()),
            StructField("COMPLETED_DT",StringType()),
            StructField("STATUS",StringType())
            
            ])
 
        # Read the CSV file into a DataFrame
        dataframe = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", ",")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .option("skip_blank_lines", True)\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
 
        #---------------------------Transformation logic ------------------------------#
 
        # Handle null values or empty rows
        dataframe=dataframe.na.drop("all")
 
        # Check for empty Dataframe
        if dataframe.count()==0:
            return "No Data in file"
    
        dataframe = dataframe.with_column("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        dataframe = dataframe.with_column("CREATE_DT",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        dataframe = dataframe.with_column("FILE_NAME", lit(file_name))
        # Creating copy of the Dataframe
        final_df = dataframe.select("OBJECTIVE_KEY", "CALL_CREATED_DT", "DESCRIPTION", "ACCT_KEY", "ASSIGNED_USER_KEY","CREATED_USER_KEY",\\
                                    "DUE_DT","COMPLETED_DT","STATUS","RUN_ID","CREATE_DT","FILE_NAME")
 
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
CREATE OR REPLACE PROCEDURE PCFSDL_RAW.PCF_PERENSO_PRODUCT_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
PACKAGES = ('openpyxl==3.0.10','snowflake-snowpark-python==*')
HANDLER = 'main'
COMMENT='One SP for 7 Tables'
EXECUTE AS CALLER
AS 'import snowflake.snowpark as snowpark
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField ,DecimalType
from snowflake.snowpark.functions import lit
from datetime import datetime
import pytz


def get_schema(file_name):
    df_schema=""
    select_col=""
    if file_name.startswith("ProdBranchIdentifier"):
        df_schema=StructType([
        StructField(''prod_key'' , IntegerType()),
        StructField(''branch_key'' , IntegerType()),
        StructField(''identifier'' , StringType())
            ])
        select_col=["prod_key","branch_key","identifier","run_id","CREATE_dt"]
    if file_name.startswith("ProdField"):
        df_schema=StructType([
        StructField(''field_key'' , IntegerType()),
        StructField(''field_desc'' , StringType()),
        StructField(''field_type'' , IntegerType()),
        StructField(''active'' , StringType())
            ])
        select_col=["field_key","field_desc","field_type","active","run_id","CREATE_dt"]
    if file_name.startswith("ProdGrp"):
        df_schema=StructType([
        StructField(''prod_grp_key'' , IntegerType()),
        StructField(''Prod_grp_lev_key'' , IntegerType()),
        StructField(''grp_desc'' , StringType()),
        StructField(''dsp_order'' , IntegerType()),
        StructField(''parent_key'' , IntegerType())
            ])
        select_col=["prod_grp_key","Prod_grp_lev_key","grp_desc","dsp_order","parent_key","run_id","CREATE_dt","FILE_NAME"]
    if file_name.startswith("ProdGrpLevel"):
        df_schema=StructType([
        StructField(''Prod_grp_lev_key'' , IntegerType()),
        StructField(''prod_Lev_desc'' , StringType()),
        StructField(''prod_Lev_index'' , IntegerType()),
        StructField(''field_key'' , IntegerType())
            ])
        select_col=["Prod_grp_lev_key","prod_Lev_desc","prod_Lev_index","field_key","run_id","CREATE_dt","FILE_NAME"]
        
    if file_name.startswith("ProdGrpRel"):
        df_schema=StructType([
        StructField(''prod_key'' , IntegerType()),
        StructField(''prod_grp_key'' , IntegerType())
            ])
        select_col=["prod_key","prod_grp_key","run_id","CREATE_dt"]
        
    if file_name.startswith("ProdRelId"):
        df_schema=StructType([
        StructField(''prod_key'' , IntegerType()),
        StructField(''field_key'' , IntegerType()),
        StructField(''id'' , StringType())    
            ])
        select_col=["prod_key","field_key","id","run_id","CREATE_dt"]
        
    if file_name.startswith("Product"):
        df_schema=StructType([
        StructField(''prod_key'' , IntegerType()),
        StructField(''Prod_id'' , StringType()),
        StructField(''Prod_desc'' , StringType()), 
        StructField(''Ean'' , StringType()),
        StructField(''Active'' , StringType()), 
            ])
        select_col=["prod_key","Prod_id","Prod_desc","Ean","Active","run_id","CREATE_dt","FILE_NAME"]
    return df_schema,select_col
        



def main(session: snowpark.Session,Param): 
    
	# SP call and parameters to pass.
	# CALL PCFSDL_RAW.PCF_PERENSO_PRODUCT_PREPROCESSING([''Product_20240325223028.csv'',''PCFSDL_RAW.DEV_LOAD_STAGE_ADLS'',''dev/ap_perenso/master/product'',''sdl_perenso_product''])
	
    try:
        # Extracting parameters from the input

        file_name       = Param[0]     
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        db_name         = stage_name.split(''.'')[0]
        target_table    = db_name+"."+Param[3]
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")


        # Define the schema for the DataFrame
        df_schema,select_col=get_schema(file_name)
        print(df_schema)
        print(select_col)
        # Set the current session schema
        
        session.use_schema(stage_name.split(''.'')[0])

        
        
        stage_path="@{0}/{1}/{2}".format(stage_name,temp_stage_path,file_name)
        print(stage_path)
        
        temp_df = session.read\\
        .schema(df_schema)\\
        .option("skip_header",1)\\
        .option("field_delimiter", ",")\\
        .option("field_optionally_enclosed_by", "\\"")\\
        .csv(stage_path)
        
        temp_df=temp_df.na.drop("all")
        if temp_df.count()==0:
            return "No Data in file"

            
        temp_df = temp_df.withColumn("run_id",lit(lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S"))))
        temp_df = temp_df.withColumn("CREATE_dt",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        temp_df = temp_df.with_column("FILE_NAME", lit(file_name))    
        
                    
        final_df=temp_df.select(select_col)
                        


        file_name=file_name+''_''+datetime.now().strftime("%Y%m%d%H%M%S")
        
        # write to target table
        final_df.write.mode("append").saveAsTable(target_table)
        
        #move to success
        final_df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True,file_format_name="PCFSDL_RAW.MY_CSV_FORMAT")


        return ''Success''

    except KeyError as key_error:
        # Handle KeyError (missing columns) here
        error_message = f"KeyError: {str(key_error)}"
        return error_message

    
    except Exception as e:
        # Handle exceptions here
        error_message = f"Error: {str(e)}"
        return error_message';



CREATE OR REPLACE PROCEDURE PCFSDL_RAW.SURVEY_PRODUCT_GRP_TO_CATEGORY_MAP("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS 'from snowflake.snowpark.functions import col, lit, date_format, current_timestamp, to_date, year, month, concat, format_number, regexp_replace,to_timestamp,when,trim,upper
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DecimalType
import snowflake.snowpark
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz
import pandas as pd

def main(session: snowpark.Session,Param):
    #Param=["survey_product_grp_to_category_map.csv","PCFSDL_RAW.DEV_LOAD_STAGE_ADLS","dev/ap_perenso/transaction/user_files","sdl_survey_product_grp_to_category_map"]
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        
        df_schema = StructType([
                StructField("category", StringType(), True),
                StructField("question_product_group", StringType(), True)
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

        
        df = df.with_column("run_id", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        df = df.with_column("FILE_NAME", lit(file_name))
        df = df.with_column("CREATE_dt", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))

        snowdf= df.select(
                    "category",
                    "question_product_group",
                    "run_id",
                    "CREATE_dt",
                    "FILE_NAME"
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
        return error_message';
CREATE OR REPLACE PROCEDURE PCFSDL_RAW.PCF_PERENSO_PRODUCT_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
PACKAGES = ('openpyxl==3.0.10','snowflake-snowpark-python==*')
HANDLER = 'main'
COMMENT='One SP for 7 Tables'
EXECUTE AS CALLER
AS 'import snowflake.snowpark as snowpark
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField ,DecimalType
from snowflake.snowpark.functions import lit
from datetime import datetime
import pytz


def get_schema(file_name):
    df_schema=""
    select_col=""
    if file_name.startswith("ProdBranchIdentifier"):
        df_schema=StructType([
        StructField(''prod_key'' , IntegerType()),
        StructField(''branch_key'' , IntegerType()),
        StructField(''identifier'' , StringType())
            ])
        select_col=["prod_key","branch_key","identifier","run_id","CREATE_dt"]
    if file_name.startswith("ProdField"):
        df_schema=StructType([
        StructField(''field_key'' , IntegerType()),
        StructField(''field_desc'' , StringType()),
        StructField(''field_type'' , IntegerType()),
        StructField(''active'' , StringType())
            ])
        select_col=["field_key","field_desc","field_type","active","run_id","CREATE_dt"]
    if file_name.startswith("ProdGrp"):
        df_schema=StructType([
        StructField(''prod_grp_key'' , IntegerType()),
        StructField(''Prod_grp_lev_key'' , IntegerType()),
        StructField(''grp_desc'' , StringType()),
        StructField(''dsp_order'' , IntegerType()),
        StructField(''parent_key'' , IntegerType())
            ])
        select_col=["prod_grp_key","Prod_grp_lev_key","grp_desc","dsp_order","parent_key","run_id","CREATE_dt","FILE_NAME"]
    if file_name.startswith("ProdGrpLevel"):
        df_schema=StructType([
        StructField(''Prod_grp_lev_key'' , IntegerType()),
        StructField(''prod_Lev_desc'' , StringType()),
        StructField(''prod_Lev_index'' , IntegerType()),
        StructField(''field_key'' , IntegerType())
            ])
        select_col=["Prod_grp_lev_key","prod_Lev_desc","prod_Lev_index","field_key","run_id","CREATE_dt","FILE_NAME"]
        
    if file_name.startswith("ProdGrpRel"):
        df_schema=StructType([
        StructField(''prod_key'' , IntegerType()),
        StructField(''prod_grp_key'' , IntegerType())
            ])
        select_col=["prod_key","prod_grp_key","run_id","CREATE_dt","FILE_NAME"]
        
    if file_name.startswith("ProdRelId"):
        df_schema=StructType([
        StructField(''prod_key'' , IntegerType()),
        StructField(''field_key'' , IntegerType()),
        StructField(''id'' , StringType())    
            ])
        select_col=["prod_key","field_key","id","run_id","CREATE_dt"]
        
    if file_name.startswith("Product"):
        df_schema=StructType([
        StructField(''prod_key'' , IntegerType()),
        StructField(''Prod_id'' , StringType()),
        StructField(''Prod_desc'' , StringType()), 
        StructField(''Ean'' , StringType()),
        StructField(''Active'' , StringType()), 
            ])
        select_col=["prod_key","Prod_id","Prod_desc","Ean","Active","run_id","CREATE_dt","FILE_NAME"]
    return df_schema,select_col
        



def main(session: snowpark.Session,Param): 
    
	# SP call and parameters to pass.
	# CALL PCFSDL_RAW.PCF_PERENSO_PRODUCT_PREPROCESSING([''Product_20240325223028.csv'',''PCFSDL_RAW.DEV_LOAD_STAGE_ADLS'',''dev/ap_perenso/master/product'',''sdl_perenso_product''])
	
    try:
        # Extracting parameters from the input

        file_name       = Param[0]     
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        db_name         = stage_name.split(''.'')[0]
        target_table    = db_name+"."+Param[3]
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")


        # Define the schema for the DataFrame
        df_schema,select_col=get_schema(file_name)
        print(df_schema)
        print(select_col)
        # Set the current session schema
        
        session.use_schema(stage_name.split(''.'')[0])

        
        
        stage_path="@{0}/{1}/{2}".format(stage_name,temp_stage_path,file_name)
        print(stage_path)
        
        temp_df = session.read\\
        .schema(df_schema)\\
        .option("skip_header",1)\\
        .option("field_delimiter", ",")\\
        .option("field_optionally_enclosed_by", "\\"")\\
        .csv(stage_path)
        
        temp_df=temp_df.na.drop("all")
        if temp_df.count()==0:
            return "No Data in file"

            
        temp_df = temp_df.withColumn("run_id",lit(lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S"))))
        temp_df = temp_df.withColumn("CREATE_dt",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        temp_df = temp_df.with_column("FILE_NAME", lit(file_name))    
        
                    
        final_df=temp_df.select(select_col)
                        


        file_name=file_name+''_''+datetime.now().strftime("%Y%m%d%H%M%S")
        
        # write to target table
        final_df.write.mode("append").saveAsTable(target_table)
        
        #move to success
        final_df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True,file_format_name="PCFSDL_RAW.MY_CSV_FORMAT")


        return ''Success''

    except KeyError as key_error:
        # Handle KeyError (missing columns) here
        error_message = f"KeyError: {str(key_error)}"
        return error_message

    
    except Exception as e:
        # Handle exceptions here
        error_message = f"Error: {str(e)}"
        return error_message';

CREATE OR REPLACE PROCEDURE PCFSDL_RAW.PCF_PERENSO_ORDER_CONSTANTS_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them.
 
import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col, lit, date_format, current_timestamp, to_date, year, month, concat, format_number, regexp_replace, to_timestamp, when, trim, upper

from snowflake.snowpark.files import SnowflakeFile
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz
import csv
def main(session: snowpark.Session,Param):

    try:

        #Param = [''Constants.csv'', ''PCFSDL_RAW.DEV_LOAD_STAGE_ADLS'', ''dev/ap_perenso/transaction/order'', ''sdl_perenso_constants'']

        # Extracting parameters from the input
        file_name = Param[0]
        stage_name = Param[1]
        temp_stage_path = Param[2]
        target_table = Param[3]

        # Read the CSV file into a DataFrame
        full_path = "@" + stage_name + "/" + temp_stage_path + "/" + file_name
        with SnowflakeFile.open(full_path, ''rb'', require_scoped_url=False) as f:
            df1 = pd.read_csv(f)
        
        df1[''RUN_ID''] = datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        df1[''CREATE_DT''] = datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")
        df1["FILE_NAME"] = file_name


        df1 = df1[["constkey", "constdesc","consttype","dsporder","RUN_ID","CREATE_DT","FILE_NAME"]]
        dataframe = session.create_dataframe(df1)

        #---------------------------Transformation logic ------------------------------#
 
        #Handle null values or empty rows
        dataframe=dataframe.na.drop("all")
 
        # Check for empty Dataframe
        if dataframe.count()==0:
            return "No Data in file"
        # Creating copy of the Datafram
        
                     
 
        # Load Data to the target table
        dataframe.write.mode("append").saveAsTable(target_table)
 
         
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
 
        # write to success folder
        file_name=file_name.split(".")[0]
        dataframe.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)
        return "Success"
 
    
 
    except KeyError as key_error:
        # Handle KeyError (missing columns) here
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message
    except Exception as e:
        # Handle exceptions here
        error_message = f"Error: {str(e)}"
        return error_message';


CREATE OR REPLACE PROCEDURE PCFSDL_RAW.PCF_CBG_DATA_PREPROCESSING("PARAM" ARRAY)
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
from snowflake.snowpark.functions import col,lit
from snowflake.snowpark.types import StringType, StructType, StructField
import pandas as pd
from datetime import datetime
import pytz
 
def main(session: snowpark.Session,Param):
 
    try:
 
        #Param=[''JJP_NEC_Markets_202403222024-03-26.csv'',''PCFSDL_RAW.DEV_LOAD_STAGE_ADLS'',''dev/CBG/transaction/Source'',''sdl_competitive_banner_group'']
 
        # Extracting parameters from the input
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
 
 
        # Define the schema for the DataFrame
        df_schema=StructType([
            StructField("market", StringType()),
            StructField("banner", StringType()),
            StructField("banner_classification", StringType()),
            StructField("manufacturer", StringType()),
            StructField("brand", StringType()),
            StructField("sku_name", StringType()),
            StructField("apn", StringType()),
            StructField("time_period", StringType()),
            StructField("unit", StringType()),
            StructField("dollar", StringType())
            
            ])
 
 
        # Read the CSV file into a DataFrame
        dataframe = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "\\t")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .option("skip_blank_lines", True)\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
 
        #---------------------------Transformation logic ------------------------------#
 
        # Handle null values or empty rows
        dataframe=dataframe.na.drop("all")
 
        # Check for empty Dataframe
        if dataframe.count()==0:
            return "No Data in file"

        dataframe = dataframe.with_column("FILE_NAME", lit(file_name))

        #Creating copy of the Dataframe
        final_df = dataframe.select("market", "banner", "banner_classification", "manufacturer", "brand", "sku_name", "apn", "time_period", "unit", "dollar","FILE_NAME")

        # Load Data to the target table
        final_df.write.mode("overwrite").saveAsTable(target_table)
 
         
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
 
        # write to success folder
        file_name=file_name.split(".")[0]
        final_df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True,file_format_name="PCFSDL_RAW.MY_CSV_FORMAT")
        return "Success"
        #return final_df
 
    
 
    except KeyError as key_error:
        # Handle KeyError (missing columns) here
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message
    except Exception as e:
        # Handle exceptions here
        error_message = f"Error: {str(e)}"
        return error_message';


CREATE OR REPLACE PROCEDURE PCFSDL_RAW.PCF_PERENSO_TODO_OPTION_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
PACKAGES = ('openpyxl==3.0.10','snowflake-snowpark-python==*')
HANDLER = 'main'
EXECUTE AS CALLER
AS 'import snowflake.snowpark as snowpark
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField ,DecimalType
from snowflake.snowpark.functions import lit
from datetime import datetime
import pytz

def main(session: snowpark.Session,Param): 
    
	# SP call and parameters to pass.
	# CALL PCFSDL_RAW.PCF_PERENSO_TODO_OPTION_PREPROCESSING([''TodoOption.csv'',''PCFSDL_RAW.PROD_LOAD_STAGE_ADLS'',''prd/ap_perenso/transaction/survey'',''sdl_perenso_todo_option''])
	
    try:
        # Extracting parameters from the input

        file_name       = Param[0]     
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        db_name         = stage_name.split(''.'')[0]
        target_table    = db_name+"."+Param[3]
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")


        # Define the schema for the DataFrame
        df_schema=StructType([
        StructField(''option_key'' , DecimalType()),
        StructField(''todo_key'' , DecimalType()),
        StructField(''option_desc'' , StringType()),
        StructField(''dsp_order'' , DecimalType()),
        StructField(''active'' , StringType()),
        StructField(''cascade_next_todo_key'' , DecimalType()),
        StructField(''cascadeon_answermode'' , DecimalType())
            ])
        # Set the current session schema
        
        session.use_schema(stage_name.split(''.'')[0])

        
        
        stage_path="@{0}/{1}/{2}".format(stage_name,temp_stage_path,file_name)
        print(stage_path)
        
        temp_df = session.read\\
        .schema(df_schema)\\
        .option("skip_header",1)\\
        .option("field_delimiter", ",")\\
        .option("field_optionally_enclosed_by", "\\"") \\
        .csv(stage_path)
        
        temp_df=temp_df.na.drop("all")
        if temp_df.count()==0:
            return "No Data in file"

            
        temp_df = temp_df.withColumn("run_id",lit(lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S"))))
        temp_df = temp_df.withColumn("CREATE_dt",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        temp_df = temp_df.with_column("FILE_NAME", lit(file_name))
            
        
                    
        final_df=temp_df.select("option_key","todo_key","option_desc","dsp_order","active","run_id","CREATE_dt","cascade_next_todo_key","cascadeon_answermode","FILE_NAME")
                        


        file_name=file_name+''_''+datetime.now().strftime("%Y%m%d%H%M%S")
        
        # write to target table
        final_df.write.mode("append").saveAsTable(target_table)
        
        #move to success
        final_df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)


        return ''Success''

    except KeyError as key_error:
        # Handle KeyError (missing columns) here
        error_message = f"KeyError: {str(key_error)}"
        return error_message

    
    except Exception as e:
        # Handle exceptions here
        error_message = f"Error: {str(e)}"
        return error_message
';
CREATE OR REPLACE PROCEDURE PCFSDL_RAW.PCF_PERENSO_PRODUCT_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
PACKAGES = ('openpyxl==3.0.10','snowflake-snowpark-python==*')
HANDLER = 'main'
COMMENT='One SP for 7 Tables'
EXECUTE AS CALLER
AS 'import snowflake.snowpark as snowpark
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField ,DecimalType
from snowflake.snowpark.functions import lit
from datetime import datetime
import pytz


def get_schema(file_name):
    df_schema=""
    select_col=""
    if file_name.startswith("ProdBranchIdentifier"):
        df_schema=StructType([
        StructField(''prod_key'' , IntegerType()),
        StructField(''branch_key'' , IntegerType()),
        StructField(''identifier'' , StringType())
            ])
        select_col=["prod_key","branch_key","identifier","run_id","CREATE_dt"]
    if file_name.startswith("ProdField"):
        df_schema=StructType([
        StructField(''field_key'' , IntegerType()),
        StructField(''field_desc'' , StringType()),
        StructField(''field_type'' , IntegerType()),
        StructField(''active'' , StringType())
            ])
        select_col=["field_key","field_desc","field_type","active","run_id","CREATE_dt"]
    if file_name.startswith("ProdGrp"):
        df_schema=StructType([
        StructField(''prod_grp_key'' , IntegerType()),
        StructField(''Prod_grp_lev_key'' , IntegerType()),
        StructField(''grp_desc'' , StringType()),
        StructField(''dsp_order'' , IntegerType()),
        StructField(''parent_key'' , IntegerType())
            ])
        select_col=["prod_grp_key","Prod_grp_lev_key","grp_desc","dsp_order","parent_key","run_id","CREATE_dt","FILE_NAME"]
    if file_name.startswith("ProdGrpLevel"):
        df_schema=StructType([
        StructField(''Prod_grp_lev_key'' , IntegerType()),
        StructField(''prod_Lev_desc'' , StringType()),
        StructField(''prod_Lev_index'' , IntegerType()),
        StructField(''field_key'' , IntegerType())
            ])
        select_col=["Prod_grp_lev_key","prod_Lev_desc","prod_Lev_index","field_key","run_id","CREATE_dt","FILE_NAME"]
        
    if file_name.startswith("ProdGrpRel"):
        df_schema=StructType([
        StructField(''prod_key'' , IntegerType()),
        StructField(''prod_grp_key'' , IntegerType())
            ])
        select_col=["prod_key","prod_grp_key","run_id","CREATE_dt","FILE_NAME"]
        
    if file_name.startswith("ProdRelId"):
        df_schema=StructType([
        StructField(''prod_key'' , IntegerType()),
        StructField(''field_key'' , IntegerType()),
        StructField(''id'' , StringType())    
            ])
        select_col=["prod_key","field_key","id","run_id","CREATE_dt"]
        
    if file_name.startswith("Product"):
        df_schema=StructType([
        StructField(''prod_key'' , IntegerType()),
        StructField(''Prod_id'' , StringType()),
        StructField(''Prod_desc'' , StringType()), 
        StructField(''Ean'' , StringType()),
        StructField(''Active'' , StringType()), 
            ])
        select_col=["prod_key","Prod_id","Prod_desc","Ean","Active","run_id","CREATE_dt","FILE_NAME"]
    return df_schema,select_col
        



def main(session: snowpark.Session,Param): 
    
	# SP call and parameters to pass.
	# CALL PCFSDL_RAW.PCF_PERENSO_PRODUCT_PREPROCESSING([''Product_20240325223028.csv'',''PCFSDL_RAW.DEV_LOAD_STAGE_ADLS'',''dev/ap_perenso/master/product'',''sdl_perenso_product''])
	
    try:
        # Extracting parameters from the input

        file_name       = Param[0]     
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        db_name         = stage_name.split(''.'')[0]
        target_table    = db_name+"."+Param[3]
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")


        # Define the schema for the DataFrame
        df_schema,select_col=get_schema(file_name)
        print(df_schema)
        print(select_col)
        # Set the current session schema
        
        session.use_schema(stage_name.split(''.'')[0])

        
        
        stage_path="@{0}/{1}/{2}".format(stage_name,temp_stage_path,file_name)
        print(stage_path)
        
        temp_df = session.read\\
        .schema(df_schema)\\
        .option("skip_header",1)\\
        .option("field_delimiter", ",")\\
        .option("field_optionally_enclosed_by", "\\"")\\
        .csv(stage_path)
        
        temp_df=temp_df.na.drop("all")
        if temp_df.count()==0:
            return "No Data in file"

            
        temp_df = temp_df.withColumn("run_id",lit(lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S"))))
        temp_df = temp_df.withColumn("CREATE_dt",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        temp_df = temp_df.with_column("FILE_NAME", lit(file_name))    
        
                    
        final_df=temp_df.select(select_col)
                        


        file_name=file_name+''_''+datetime.now().strftime("%Y%m%d%H%M%S")
        
        # write to target table
        final_df.write.mode("append").saveAsTable(target_table)
        
        #move to success
        final_df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True,file_format_name="PCFSDL_RAW.MY_CSV_FORMAT")


        return ''Success''

    except KeyError as key_error:
        # Handle KeyError (missing columns) here
        error_message = f"KeyError: {str(key_error)}"
        return error_message

    
    except Exception as e:
        # Handle exceptions here
        error_message = f"Error: {str(e)}"
        return error_message';
CREATE OR REPLACE PROCEDURE PCFSDL_RAW.PA_PHARMA_INV_API_PREPROCESSING("PARAM" ARRAY)
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
from snowflake.snowpark.functions import col,lit,trim, substring,current_timestamp,to_timestamp,date_format
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField,FloatType,DecimalType,DoubleType
import pandas as pd
from datetime import datetime
import pytz

def main(session: snowpark.Session,Param): 

    try:

        #Param=[''RE_2.0_API_Inventory_and_Sales_Report_2024-03-31.csv'',''PCFSDL_RAW.DEV_LOAD_STAGE_ADLS'',''dev/Pacific_reg_inv/transaction/API/'',''SDL_API_DSTR,SDL_au_dstr_api_header'']

        # Extracting parameters from the input
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3].split(",")[0]
        Header_table    = Param[3].split(",")[1]


        # Define the schema for the DataFrame
        df_schema=StructType([
            StructField("ARTICLE_ID",StringType()),
            StructField("ARTICLE_DESC",StringType()),
            StructField("PRODUCT_EAN",StringType()),
            StructField("SITE_ID",StringType()),
            StructField("SITE_DESC",StringType()),
            StructField("VENDOR",StringType()),
            StructField("VENDOR_DESC",StringType()),
            StructField("PRODUCT_SAP_ID",StringType()),
            StructField("COST_PRICE",StringType()),
            StructField("CROSS_SITE_STATUS",StringType()),
            StructField("MONTH_13",StringType()),
            StructField("MONTH_12",StringType()),
            StructField("MONTH_11",StringType()),
            StructField("MONTH_10",StringType()),
            StructField("MONTH_09",StringType()),
            StructField("MONTH_08",StringType()),
            StructField("MONTH_07",StringType()),
            StructField("MONTH_06",StringType()),
            StructField("MONTH_05",StringType()),
            StructField("MONTH_04",StringType()),
            StructField("MONTH_03",StringType()),
            StructField("MONTH_02",StringType()),
            StructField("MONTH_01",StringType()),
            StructField("NULL_COLOUMN",StringType()),
            StructField("MTH_TOTAL_INVOICED_QTY",StringType()),
            StructField("SOH_QTY",StringType()),
            StructField("DC_SOO_QTY",StringType()),
            StructField("SO_BACKORDER_QTY",StringType())
            ])


        # Read Date and Header for column names
    
        df_header = session.read\\
            .schema(df_schema)\\
            .option("skip_header",0)\\
            .option("field_delimiter", "\\u0001")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .option("skip_blank_lines", True)\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)\\
            .limit(2)

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
        dataframe= dataframe.na.drop("all")


        # Check for empty Dataframe
        if dataframe.count()==0:
            return "No Data in file"

        # Fetch value for inv_date column
        date=file_name.split("_")[-1].split(".")[0]

        inv_date=date

        columns = dataframe.columns

        source_file=file_name.rsplit(".",1)[0] + ".xlsx"

        # Add inv_date and filename column to the dataframe
        dataframe=dataframe.with_column("INVT_DT",lit(inv_date).cast("String"))
        dataframe = dataframe.with_column("FILE_NAME", lit(source_file))

        new_columns = ["INVT_DT"] + [col for col in columns if col != "INVT_DT"] + ["FILE_NAME"]

        final_df=dataframe.select(new_columns)

        # Fetch Header from files
        df_pandas=df_header.to_pandas()
        
        df_header_1=df_pandas.iloc[0,:9]
        df_header_2=df_pandas.iloc[1,9:]

        # Concatenate the DataFrames
        data1 = df_header_1.values.flatten().tolist()
        data2 = df_header_2.values.flatten().tolist()

        combined_data = data1 + data2
        header_columns=df_header.columns

        result_df = pd.DataFrame([combined_data],columns=header_columns)
        df_trimmed = result_df.applymap(lambda x: x.strip() if isinstance(x, str) else x)

        header=session.create_dataframe(df_trimmed)

        header = header.with_column("FILE_NAME", lit(source_file))
        

        # Load Data to the target table
        final_df.write.mode("overwrite").saveAsTable(target_table)

        # Load the header to header target table "SDL_AU_DSTR_sigma_HEADER"

        header.write.mode("overwrite").saveAsTable(Header_table)

        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        header_file="API_out_hdr"

        # write to success folder
    
        file_name_1=file_name.replace("_"," ")[::-1].split(".",1)
        file_name=file_name_1[1][::-1]+''_''+datetime.now().strftime("%Y%m%d%H%M%S")
        final_df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)

        # Write the header dataframe to temp folder under processed
        header.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/temp/"+formatted_year+"/"+formatted_month+"/"+header_file,file_format_type=''csv'',header=True,OVERWRITE=True)


        return "Success"

    
    except KeyError as key_error:
        # Handle KeyError (missing columns) here
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message
        
    except Exception as e:
        # Handle exceptions here
        error_message = f"Error: {str(e)}"
        return error_message
        ';
CREATE OR REPLACE PROCEDURE PCFSDL_RAW.PCF_PERENSO_DIARY_ITEM_TIME_PREPROCESSING("PARAM" ARRAY)
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
from snowflake.snowpark.functions import col,lit
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField
import pandas as pd
from datetime import datetime
import pytz
 
def main(session: snowpark.Session,Param):
 
    try:
 
        #Param=[''DiaryItemTime_20240320223707.csv'',''PCFSDL_RAW.DEV_LOAD_STAGE_ADLS'',''dev/ap_perenso/transation/call/'',''sdl_perenso_diary_item_time'']
 
        # Extracting parameters from the input
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
 
 
        # Define the schema for the DataFrame
        df_schema=StructType([
            StructField("DIARY_ITEM_KEY",StringType()),
            StructField("DATE_TIME",StringType()),
            StructField("START_TIME",StringType()),
            StructField("END_TIME",StringType()),
            StructField("DURATION",StringType()),
            StructField("LATITUDE",StringType()),
            StructField("LONGITUDE",StringType()),
            StructField("GOOGLE_MAPS_URL",StringType())
            
            ])
 
 
        # Read the CSV file into a DataFrame
        dataframe = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", ",")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .option("skip_blank_lines", True)\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
 
        #---------------------------Transformation logic ------------------------------#
 
        # Handle null values or empty rows
        dataframe=dataframe.na.drop("all")
 
        # Check for empty Dataframe
        if dataframe.count()==0:
            return "No Data in file"

        
        dataframe = dataframe.with_column("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        dataframe = dataframe.with_column("CREATE_DT",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        dataframe = dataframe.with_column("FILE_NAME", lit(file_name))
        # Creating copy of the Dataframe
        final_df = dataframe.select("DIARY_ITEM_KEY", "DATE_TIME", "START_TIME", "END_TIME", "DURATION","LATITUDE",\\
                                    "LONGITUDE","GOOGLE_MAPS_URL","RUN_ID","CREATE_DT","FILE_NAME")
 
        # Load Data to the target table
        final_df.write.mode("append").saveAsTable(target_table)
 
         
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
 
        # write to success folder
        file_name=file_name.split(".")[0]
        final_df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)
        return "Success"
        #return final_df
 
    
 
    except KeyError as key_error:
        # Handle KeyError (missing columns) here
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message
    except Exception as e:
        # Handle exceptions here
        error_message = f"Error: {str(e)}"
        return error_message';
CREATE OR REPLACE PROCEDURE PCFSDL_RAW.PCF_PERENSO_RANGING_ACCT_GRP_REL_PREPROCESSING("PARAM" ARRAY)
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
from snowflake.snowpark.functions import col,lit
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField
import pandas as pd
from datetime import datetime
import pytz
 
def main(session: snowpark.Session,Param):
 
    try:
 
        #Param=[''RangingProduct_20240320223707.csv'',''PCFSDL_RAW.DEV_LOAD_STAGE_ADLS'',''dev/ap_perenso/transation/ranging/'',''sdl_perenso_ranging_acct_grp_rel'']
 
        # Extracting parameters from the input
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
 
 
        # Define the schema for the DataFrame
        df_schema=StructType([
            StructField("RANGING_KEY",IntegerType()),
            StructField("ACCT_GRP_KEY",IntegerType())        
            ])
 
        # Read the CSV file into a DataFrame
        dataframe = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", ",")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .option("skip_blank_lines", True)\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
 
        #---------------------------Transformation logic ------------------------------#
 
        # Handle null values or empty rows
        dataframe=dataframe.na.drop("all")
 
        # Check for empty Dataframe
        if dataframe.count()==0:
            return "No Data in file"
    
        dataframe = dataframe.with_column("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        dataframe = dataframe.with_column("CREATE_DT",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        dataframe = dataframe.with_column("FILE_NAME", lit(file_name))
        # Creating copy of the Dataframe
        final_df = dataframe.select("RANGING_KEY","ACCT_GRP_KEY","RUN_ID","CREATE_DT","FILE_NAME")
 
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
CREATE OR REPLACE PROCEDURE PCFSDL_RAW.PCF_PERENSO_TODO_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them.
 
import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col, lit, date_format, current_timestamp, to_date, year, month, concat, format_number, regexp_replace, to_timestamp, when, trim, upper

from snowflake.snowpark.files import SnowflakeFile
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz
import csv
def main(session: snowpark.Session,Param):
 
    try:
 
        
        #Param=[''Todo.csv'',''PCFSDL_RAW.DEV_LOAD_STAGE_ADLS'',''dev/ap_perenso/transaction/headoffice'',''sdl_perenso_todo'']
 
        # Extracting parameters from the input
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
 
        # Read the CSV file into a DataFrame
        full_path = "@"+stage_name+"/"+temp_stage_path+"/"+file_name
        with SnowflakeFile.open(full_path, ''rb'', require_scoped_url = False) as f:
            df1 = pd.read_csv(f)
        df1[''RUN_ID''] = datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        df1[''CREATE_DT''] = datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")
        df1["FILE_NAME"]=file_name

        df1 =df1[["todokey", "todotype", "tododesc", "workitemkey", "startdate",
                       "enddate", "RUN_ID", "CREATE_DT", "dsporder", "anstype",
                       "cascadeonanswermode", "cascadetodokey", "cascadenexttodokey","FILE_NAME"]]
        dataframe = session.create_dataframe(df1)


 
        #---------------------------Transformation logic ------------------------------#
 
        #Handle null values or empty rows
        dataframe=dataframe.na.drop("all")
 
        # Check for empty Dataframe
        if dataframe.count()==0:
            return "No Data in file"
        # Creating copy of the Datafram
        
                     
 
        # Load Data to the target table
        dataframe.write.mode("append").saveAsTable(target_table)
 
         
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
 
        # write to success folder
        file_name=file_name.split(".")[0]
        dataframe.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)
        return "Success"
 
    
 
    except KeyError as key_error:
        # Handle KeyError (missing columns) here
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message
    except Exception as e:
        # Handle exceptions here
        error_message = f"Error: {str(e)}"
        return error_message';
CREATE OR REPLACE PROCEDURE PCFSDL_RAW.PCF_PERENSO_HEAD_OFFICE_REQ_CHECK_PREPROCESSING("PARAM" ARRAY)
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
from snowflake.snowpark.functions import col,lit
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField
import pandas as pd
from datetime import datetime
import pytz
 
def main(session: snowpark.Session,Param):
 
    try:
 
        #Param=[HeadOfficeReqCheck_20240320223707.csv'',''PCFSDL_RAW.DEV_LOAD_STAGE_ADLS'',''dev/ap_perenso/transation/headoffice/,''sdl_perenso_head_office_req_check'']
 
        # Extracting parameters from the input
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
 
 
        # Define the schema for the DataFrame
        df_schema=StructType([
            StructField("STORE_CHK_HDR_KEY",IntegerType()),
            StructField("TODO_KEY",IntegerType()),
            StructField("PROD_GRP_KEY",IntegerType()),
            StructField("NOTES",StringType()),
            StructField("FAIL_REASON_KEY",StringType()),
            StructField("FAIL_REASON_DESC",StringType())
            ])
 
        # Read the CSV file into a DataFrame
        dataframe = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", ",")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .option("skip_blank_lines", True)\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
 
        #---------------------------Transformation logic ------------------------------#
 
        # Handle null values or empty rows
        dataframe=dataframe.na.drop("all")
 
        # Check for empty Dataframe
        if dataframe.count()==0:
            return "No Data in file"
    
        dataframe = dataframe.with_column("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        dataframe = dataframe.with_column("CREATE_DT",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        dataframe = dataframe.with_column("FILE_NAME", lit(file_name))
        
        # Creating copy of the Dataframe
        final_df = dataframe.select("STORE_CHK_HDR_KEY", "TODO_KEY", "PROD_GRP_KEY", "NOTES", "FAIL_REASON_KEY","FAIL_REASON_DESC",\\
                                    "RUN_ID","CREATE_DT","FILE_NAME")
 
        # Load Data to the target table
        final_df.write.mode("append").saveAsTable(target_table)
 
         
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
 
        # write to success folder
        file_name=file_name.split(".")[0]
        final_df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True,file_format_name="PCFSDL_RAW.MY_CSV_FORMAT")
        return "Success"
 
    
 
    except KeyError as key_error:
        # Handle KeyError (missing columns) here
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message
    except Exception as e:
        # Handle exceptions here
        error_message = f"Error: {str(e)}"
        return error_message';


CREATE OR REPLACE PROCEDURE PCFSDL_RAW.PCF_PERENSO_WORK_ITEM_PREPROCESSING("PARAM" ARRAY)
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
from snowflake.snowpark.functions import col,lit
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField
import pandas as pd
from datetime import datetime
import pytz
 
def main(session: snowpark.Session,Param):
 
    try:
 
        #Param=[''Workitem_20240320223707.csv'',''PCFSDL_RAW.DEV_LOAD_STAGE_ADLS'',''dev/ap_perenso/transation/call/'',''sdl_perenso_work_item'']
 
        # Extracting parameters from the input
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
 
 
        # Define the schema for the DataFrame
        df_schema=StructType([
            StructField("WORK_ITEM_KEY",IntegerType()),
            StructField("WORK_ITEM_DESC",StringType()),
            StructField("WORK_ITEM_TYPE",IntegerType()),
            StructField("DIARY_ITEM_TYPE_KEY",IntegerType()),
            StructField("START_DATE",StringType()),
            StructField("END_DATE",StringType())
            
            ])
 
        # Read the CSV file into a DataFrame
        dataframe = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", ",")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .option("skip_blank_lines", True)\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
 
        #---------------------------Transformation logic ------------------------------#
 
        # Handle null values or empty rows
        dataframe=dataframe.na.drop("all")
 
        # Check for empty Dataframe
        if dataframe.count()==0:
            return "No Data in file"
    
        dataframe = dataframe.with_column("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        dataframe = dataframe.with_column("CREATE_DT",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        dataframe = dataframe.with_column("FILE_NAME", lit(file_name))
        # Creating copy of the Dataframe"
        final_df = dataframe.select("WORK_ITEM_KEY", "WORK_ITEM_DESC", "WORK_ITEM_TYPE", "DIARY_ITEM_TYPE_KEY", "START_DATE","END_DATE",\\
                                    "RUN_ID","CREATE_DT","FILE_NAME")
 
        # Load Data to the target table
        final_df.write.mode("append").saveAsTable(target_table)
 
         
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
 
        # write to success folder
        file_name=file_name.split(".")[0]
        final_df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)
        return "Success"
        #return final_df
 
    
 
    except KeyError as key_error:
        # Handle KeyError (missing columns) here
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message
    except Exception as e:
        # Handle exceptions here
        error_message = f"Error: {str(e)}"
        return error_message';



CREATE OR REPLACE PROCEDURE PCFSDL_RAW.SURVEY_TARGETS("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS 'from snowflake.snowpark.functions import col, lit, date_format, current_timestamp, to_date, year, month, concat, format_number, regexp_replace,to_timestamp,when,trim,upper
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DecimalType
import snowflake.snowpark
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz
import pandas as pd

def main(session: snowpark.Session,Param):
    #Param=["survey_targets.csv","PCFSDL_RAW.DEV_LOAD_STAGE_ADLS","dev/ap_perenso/user_files","sdl_survey_targets"]
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        
        df_schema = StructType([
                StructField("year", StringType(), True),
                StructField("cycle_start_date", StringType(), True),
                StructField("cycle_end_date", StringType(), True),
                StructField("account_type_description", StringType(), True),
                StructField("target_type", StringType(), True),
                StructField("category", StringType(), True),
                StructField("territory_region", StringType(), True),
                StructField("territory", StringType(), True),
                StructField("target", StringType(), True),
                
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

       
        df = df.with_column("run_id", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        #file_name1=file_name.split(".")[0]+"_"+ datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")+".csv"
        
        df = df.with_column("CREATE_dt", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        df = df.with_column("FILE_NAME", lit(file_name))

        snowdf= df.select(
                    "year",
                    "cycle_start_date",
                    "cycle_end_date",
                    "account_type_description",
                    "target_type",
                    "category",
                    "territory_region",
                    "territory",
                    "target",
                    "run_id",
                    "CREATE_dt",
                    "FILE_NAME"
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
        return error_message';
CREATE OR REPLACE PROCEDURE PCFSDL_RAW.ANZ_PERENSO_PRODCHKDISTRIBUTION_PREPROCESSING("PARAM" ARRAY)
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
from snowflake.snowpark.types import  StringType, StructType, StructField,IntegerType
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz



def main(session: snowpark.Session,Param): 
       
    try :
            
        #Param=[''ProdChkDistribution_20240320223707.csv'',''PCFSDL_RAW.DEV_LOAD_STAGE_ADLS'',''dev/ap_perenso/transaction/overandabove'',''sdl_perenso_Prod_Chk_Distribution'']

        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        # Define the schema for the DataFrame
        
        df_schema = StructType([
                StructField("acct_key", IntegerType()),
                StructField("prod_key", IntegerType()),
                StructField("start_date", StringType()),
                StructField("end_date", StringType()),
                StructField("in_distribution", StringType())
            ])


        
        dataframe = session.read.schema(df_schema) \\
        .option("skip_header",1) \\
        .option("field_delimiter", ",") \\
        .option("field_optionally_enclosed_by", "\\"")  \\
        .option("encoding","UTF-8")\\
        .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        
        dataframe=dataframe.na.drop("all")
        if dataframe.count()==0:
            return "No Data in file"

        dataframe = dataframe.with_column("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        dataframe = dataframe.with_column("CRTD_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        dataframe = dataframe.with_column("FILE_NAME", lit(file_name))

        # Load Data to the target table
        dataframe.write.mode("append").saveAsTable(target_table)
       
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        #move to success
        dataframe.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)

        
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

CREATE OR REPLACE PROCEDURE PCFSDL_RAW.PCF_PERENSO_PRODUCT_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
PACKAGES = ('openpyxl==3.0.10','snowflake-snowpark-python==*')
HANDLER = 'main'
COMMENT='One SP for 7 Tables'
EXECUTE AS CALLER
AS 'import snowflake.snowpark as snowpark
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField ,DecimalType
from snowflake.snowpark.functions import lit
from datetime import datetime
import pytz


def get_schema(file_name):
    df_schema=""
    select_col=""
    if file_name.startswith("ProdBranchIdentifier"):
        df_schema=StructType([
        StructField(''prod_key'' , IntegerType()),
        StructField(''branch_key'' , IntegerType()),
        StructField(''identifier'' , StringType())
            ])
        select_col=["prod_key","branch_key","identifier","run_id","CREATE_dt"]
    if file_name.startswith("ProdField"):
        df_schema=StructType([
        StructField(''field_key'' , IntegerType()),
        StructField(''field_desc'' , StringType()),
        StructField(''field_type'' , IntegerType()),
        StructField(''active'' , StringType())
            ])
        select_col=["field_key","field_desc","field_type","active","run_id","CREATE_dt","FILE_NAME"]
    if file_name.startswith("ProdGrp"):
        df_schema=StructType([
        StructField(''prod_grp_key'' , IntegerType()),
        StructField(''Prod_grp_lev_key'' , IntegerType()),
        StructField(''grp_desc'' , StringType()),
        StructField(''dsp_order'' , IntegerType()),
        StructField(''parent_key'' , IntegerType())
            ])
        select_col=["prod_grp_key","Prod_grp_lev_key","grp_desc","dsp_order","parent_key","run_id","CREATE_dt","FILE_NAME"]
    if file_name.startswith("ProdGrpLevel"):
        df_schema=StructType([
        StructField(''Prod_grp_lev_key'' , IntegerType()),
        StructField(''prod_Lev_desc'' , StringType()),
        StructField(''prod_Lev_index'' , IntegerType()),
        StructField(''field_key'' , IntegerType())
            ])
        select_col=["Prod_grp_lev_key","prod_Lev_desc","prod_Lev_index","field_key","run_id","CREATE_dt","FILE_NAME"]
        
    if file_name.startswith("ProdGrpRel"):
        df_schema=StructType([
        StructField(''prod_key'' , IntegerType()),
        StructField(''prod_grp_key'' , IntegerType())
            ])
        select_col=["prod_key","prod_grp_key","run_id","CREATE_dt","FILE_NAME"]
        
    if file_name.startswith("ProdRelId"):
        df_schema=StructType([
        StructField(''prod_key'' , IntegerType()),
        StructField(''field_key'' , IntegerType()),
        StructField(''id'' , StringType())    
            ])
        select_col=["prod_key","field_key","id","run_id","CREATE_dt"]
        
    if file_name.startswith("Product"):
        df_schema=StructType([
        StructField(''prod_key'' , IntegerType()),
        StructField(''Prod_id'' , StringType()),
        StructField(''Prod_desc'' , StringType()), 
        StructField(''Ean'' , StringType()),
        StructField(''Active'' , StringType()), 
            ])
        select_col=["prod_key","Prod_id","Prod_desc","Ean","Active","run_id","CREATE_dt","FILE_NAME"]
    return df_schema,select_col
        



def main(session: snowpark.Session,Param): 
    
	# SP call and parameters to pass.
	# CALL PCFSDL_RAW.PCF_PERENSO_PRODUCT_PREPROCESSING([''Product_20240325223028.csv'',''PCFSDL_RAW.DEV_LOAD_STAGE_ADLS'',''dev/ap_perenso/master/product'',''sdl_perenso_product''])
	
    try:
        # Extracting parameters from the input

        file_name       = Param[0]     
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        db_name         = stage_name.split(''.'')[0]
        target_table    = db_name+"."+Param[3]
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")


        # Define the schema for the DataFrame
        df_schema,select_col=get_schema(file_name)
        print(df_schema)
        print(select_col)
        # Set the current session schema
        
        session.use_schema(stage_name.split(''.'')[0])

        
        
        stage_path="@{0}/{1}/{2}".format(stage_name,temp_stage_path,file_name)
        print(stage_path)
        
        temp_df = session.read\\
        .schema(df_schema)\\
        .option("skip_header",1)\\
        .option("field_delimiter", ",")\\
        .option("field_optionally_enclosed_by", "\\"")\\
        .csv(stage_path)
        
        temp_df=temp_df.na.drop("all")
        if temp_df.count()==0:
            return "No Data in file"

            
        temp_df = temp_df.withColumn("run_id",lit(lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S"))))
        temp_df = temp_df.withColumn("CREATE_dt",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        temp_df = temp_df.with_column("FILE_NAME", lit(file_name))    
        
                    
        final_df=temp_df.select(select_col)
                        


        file_name=file_name+''_''+datetime.now().strftime("%Y%m%d%H%M%S")
        
        # write to target table
        final_df.write.mode("append").saveAsTable(target_table)
        
        #move to success
        final_df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True,file_format_name="PCFSDL_RAW.MY_CSV_FORMAT")


        return ''Success''

    except KeyError as key_error:
        # Handle KeyError (missing columns) here
        error_message = f"KeyError: {str(key_error)}"
        return error_message

    
    except Exception as e:
        # Handle exceptions here
        error_message = f"Error: {str(e)}"
        return error_message';
CREATE OR REPLACE PROCEDURE PCFSDL_RAW.PA_PHARMA_INV_SIGMA_PREPROCESSING("PARAM" ARRAY)
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
from snowflake.snowpark.functions import col,lit,trim, substring,current_timestamp,to_timestamp,date_format
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField,FloatType,DecimalType,DoubleType
import pandas as pd
from datetime import datetime
import pytz

def main(session: snowpark.Session,Param): 

    try:

        #Param=[''Replenishment_E3_Buyers_Report.csv'',''PCFSDL_RAW.DEV_LOAD_STAGE_ADLS'',''dev/Pacific_reg_inv/transaction/Sigma/'',''SDL_sigma_DSTR,SDL_AU_DSTR_sigma_HEADER'']

        # Extracting parameters from the input
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3].split(",")[0]
        Header_table    = Param[3].split(",")[1]


        # Define the schema for the DataFrame
        df_schema=StructType([
            StructField("WAREHOUSE_DESC",StringType()),
            StructField("PRODUCT_CODE",StringType()),
            StructField("PRODUCT_DESC",StringType()),
            StructField("TEMP",StringType()),
            StructField("SUPPLIER_PRODUCT_CODE",StringType()),
            StructField("EAN",StringType()),
            StructField("VOLUME_CLAS_CODE",StringType()),
            StructField("HANDLING_STATUS",StringType()),
            StructField("COST_PRICE",StringType()),
            StructField("SOH_QTY",StringType()),
            StructField("SOH_AMT",StringType()),
            StructField("STOCK_IN_TRANSIT_QTY",StringType()),
            StructField("STOCK_IN_TRANSIT_AMT",StringType()),
            StructField("RESTRICTED_STOCK_QTY",StringType()),
            StructField("RESTRICTED_STOCK_AMT",StringType()),
            StructField("SOO_QTY",StringType()),
            StructField("SOO_AMT",StringType()),
            StructField("BACK_ORDER_QTY",StringType()),
            StructField("BACK_ORDER_AMT",StringType()),
            StructField("MONTH_01",StringType()),
            StructField("MONTH_02",StringType()),
            StructField("MONTH_03",StringType()),
            StructField("MONTH_04",StringType()),
            StructField("MONTH_05",StringType()),
            StructField("MONTH_06",StringType()),
            StructField("MONTH_07",StringType()),
            StructField("MONTH_08",StringType()),
            StructField("MONTH_09",StringType()),
            StructField("MONTH_10",StringType()),
            StructField("MONTH_11",StringType()),
            StructField("MONTH_12",StringType()),
            StructField("MONTH_13",StringType()),
            StructField("MONTH_14",StringType()),
            StructField("MONTH_15",StringType()),
            StructField("MONTH_16",StringType())
            ])


        # Read Date and Header for column names
    
        df_header = session.read\\
            .schema(df_schema)\\
            .option("skip_header",3)\\
            .option("field_delimiter", "\\u0001")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .option("skip_blank_lines", True)\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)\\
            .limit(1)

        # Read the CSV file into a DataFrame

        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",20)\\
            .option("field_delimiter", "\\u0001")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .option("skip_blank_lines", True)\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        #---------------------------Transformation logic ------------------------------#

        # Handle null values or empty rows
        df= df.na.drop("all")


        # Check for empty Dataframe
        if df.count()==0:
            return "No Data in file"

        # Fetch value for inv_date column

        first_row = df_header.first()
        date=first_row[0]
        inv_date=date.split(" ")[-1]


        # Fetch header columns required for Header Table
        header_columns=df.limit(1)
      

        header = header_columns.withColumn(''MONTH_01'', date_format(col(''MONTH_01''), ''YYYY - MMMM''))\\
        .withColumn(''MONTH_02'', date_format(col(''MONTH_02''), ''YYYY - MMMM''))\\
        .withColumn(''MONTH_03'', date_format(col(''MONTH_03''), ''YYYY - MMMM''))\\
        .withColumn(''MONTH_04'', date_format(col(''MONTH_04''), ''YYYY - MMMM''))\\
        .withColumn(''MONTH_05'', date_format(col(''MONTH_05''), ''YYYY - MMMM''))\\
        .withColumn(''MONTH_06'', date_format(col(''MONTH_06''), ''YYYY - MMMM''))\\
        .withColumn(''MONTH_07'', date_format(col(''MONTH_07''), ''YYYY - MMMM''))\\
        .withColumn(''MONTH_08'', date_format(col(''MONTH_08''), ''YYYY - MMMM''))\\
        .withColumn(''MONTH_09'', date_format(col(''MONTH_09''), ''YYYY - MMMM''))\\
        .withColumn(''MONTH_10'', date_format(col(''MONTH_10''), ''YYYY - MMMM''))\\
        .withColumn(''MONTH_11'', date_format(col(''MONTH_11''), ''YYYY - MMMM''))\\
        .withColumn(''MONTH_12'', date_format(col(''MONTH_12''), ''YYYY - MMMM''))\\
        .withColumn(''MONTH_13'', date_format(col(''MONTH_13''), ''YYYY - MMMM''))\\
        .withColumn(''MONTH_14'', date_format(col(''MONTH_14''), ''YYYY - MMMM''))\\
        .withColumn(''MONTH_15'', date_format(col(''MONTH_15''), ''YYYY - MMMM''))\\
        .withColumn(''MONTH_16'', date_format(col(''MONTH_16''), ''YYYY - MMMM''))

        source_file=file_name.split(".")[0]+".xlsx"

        header = header.with_column("FILE_NAME", lit(source_file))
        


        # Filter Rows with values "Warehouse Desc" and  "product code" and "Product Desc"
        dataframe= df.filter((df["WAREHOUSE_DESC"] != "Warehouse Desc") & (df["PRODUCT_CODE"] != "product code") & (df["PRODUCT_DESC"] != "Product Desc"))

        # Add inv_date to the dataframe
        dataframe=dataframe.with_column("INV_DATE",lit(inv_date).cast("String"))
        dataframe = dataframe.with_column("FILE_NAME", lit(source_file))

        columns = df.columns
        new_columns = ["INV_DATE"] + [col for col in columns if col != "INV_DATE"] + ["FILE_NAME"]

        final_df=dataframe.select(new_columns)

        # Load Data to the target table
        final_df.write.mode("overwrite").saveAsTable(target_table)

        # Load the header to header target table "SDL_AU_DSTR_sigma_HEADER"

        header.write.mode("overwrite").saveAsTable(Header_table)

        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        header_file="Sigma_out_hdr"

        # write to success folder
    
        file_name=file_name.replace("_"," ").split(".")[0]+''_''+datetime.now().strftime("%Y%m%d%H%M%S")
        final_df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)

        # Write the header dataframe to temp folder under processed
        header.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/temp/"+formatted_year+"/"+formatted_month+"/"+header_file,file_format_type=''csv'',header=True,OVERWRITE=True)


        return "Success"

    
    except KeyError as key_error:
        # Handle KeyError (missing columns) here
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message
        
    except Exception as e:
        # Handle exceptions here
        error_message = f"Error: {str(e)}"
        return error_message
        ';
CREATE OR REPLACE PROCEDURE PCFSDL_RAW.PCF_PERENSO_ORDER_DEALDISCOUNT_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them.
 
import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col, lit, date_format, current_timestamp, to_date, year, month, concat, format_number, regexp_replace, to_timestamp, when, trim, upper

from snowflake.snowpark.files import SnowflakeFile
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz
import csv
def main(session: snowpark.Session,Param):

    try:

        #Param = [''DealDiscount.csv'', ''PCFSDL_RAW.DEV_LOAD_STAGE_ADLS'', ''dev/ap_perenso/transaction/order'', ''sdl_perenso_deal_discount'']

        # Extracting parameters from the input
        file_name = Param[0]
        stage_name = Param[1]
        temp_stage_path = Param[2]
        target_table = Param[3]

        # Read the CSV file into a DataFrame
        full_path = "@" + stage_name + "/" + temp_stage_path + "/" + file_name
        with SnowflakeFile.open(full_path, ''rb'', require_scoped_url=False) as f:
            df1 = pd.read_csv(f)
        
        df1[''RUN_ID''] = datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        df1[''CREATE_DT''] = datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")
        df1["FILE_NAME"]=file_name


        dataframe = session.create_dataframe(df1)


        #---------------------------Transformation logic ------------------------------#
 
        #Handle null values or empty rows
        dataframe=dataframe.na.drop("all")
 
        # Check for empty Dataframe
        if dataframe.count()==0:
            return "No Data in file"
        # Creating copy of the Datafram
        
                     
 
        # Load Data to the target table
        dataframe.write.mode("append").saveAsTable(target_table)
 
         
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
 
        # write to success folder
        file_name=file_name.split(".")[0]
        dataframe.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)
        return "Success"
 
    
 
    except KeyError as key_error:
        # Handle KeyError (missing columns) here
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message
    except Exception as e:
        # Handle exceptions here
        error_message = f"Error: {str(e)}"
        return error_message';

CREATE OR REPLACE PROCEDURE PCFSDL_RAW.SDL_SURVEY_TYPE_TO_QUESTION_MAP("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS 'from snowflake.snowpark.functions import col, lit, date_format, current_timestamp, to_date, year, month, concat, format_number, regexp_replace,to_timestamp,when,trim,upper
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DecimalType
import snowflake.snowpark
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz
import pandas as pd

def main(session: snowpark.Session,Param):
    #Param=["survey_type_to_question_map.csv","PCFSDL_RAW.DEV_LOAD_STAGE_ADLS","dev/ap_perenso/user_files","sdl_survey_type_to_question_map"]
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        
        df_schema = StructType([
                StructField("account_type_description", StringType(), True),
                StructField("target_type", StringType(), True),
                StructField("perenso_questions", StringType(), True)
                
            ])
        
        df = session.read\\
            .schema(df_schema)\\
            .option("field_delimiter", "|")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

       
        
        df=df.na.drop("all")
        if df.count()==0 :
            return "No Data in file"

        
        df = df.with_column("run_id", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        #file_name1=file_name.split(".")[0]+"_"+ datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")+".csv"
        df = df.with_column("create_dt", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        df = df.with_column("FILE_NAME", lit(file_name))

        snowdf= df.select(
                    "account_type_description",
                    "target_type",
                    "perenso_questions",
                    "run_id",
                    "create_dt",
                    "FILE_NAME"
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
        return error_message';


CREATE OR REPLACE PROCEDURE PCFSDL_RAW.PCF_PERENSO_SURVEYRESULT_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
PACKAGES = ('openpyxl==3.0.10','snowflake-snowpark-python==*')
HANDLER = 'main'
EXECUTE AS CALLER
AS 'import snowflake.snowpark as snowpark
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField ,DecimalType
from snowflake.snowpark.functions import lit
from datetime import datetime
import pytz

def main(session: snowpark.Session,Param): 
    
	# SP call and parameters to pass.
	# CALL PCFSDL_RAW.PCF_PERENSO_SURVEYRESULT_PREPROCESSING([''SurveyResult.csv'',''PCFSDL_RAW.PROD_LOAD_STAGE_ADLS'',''prd/ap_perenso/transaction/survey'',''sdl_perenso_survey_result''])
	
    try:
        # Extracting parameters from the input

        file_name       = Param[0]     
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        db_name         = stage_name.split(''.'')[0]
        target_table    = db_name+"."+Param[3]
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")


        # Define the schema for the DataFrame
        df_schema=StructType([
        StructField(''store_chk_hdr_key'' , DecimalType()),
        StructField(''line_key'' , DecimalType()),
        StructField(''todo_key'' , DecimalType()),
        StructField(''prod_grp_key'' , DecimalType()),
        StructField(''optionans'' , DecimalType()),
        StructField(''notesans'' , StringType())
            ])
        # Set the current session schema
        
        session.use_schema(stage_name.split(''.'')[0])


        stage_path="@{0}/{1}/{2}".format(stage_name,temp_stage_path,file_name)
        print(stage_path)
        
        temp_df = session.read\\
        .schema(df_schema)\\
        .option("skip_header",1)\\
        .option("field_delimiter", ",")\\
        .csv(stage_path)
        
        temp_df=temp_df.na.drop("all")
        if temp_df.count()==0:
            return "No Data in file"
        print(temp_df.show(10))
        temp_df = temp_df.withColumn("run_id",lit(lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S"))))
        temp_df = temp_df.withColumn("CREATE_dt",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        temp_df = temp_df.with_column("FILE_NAME", lit(file_name))

        
        
                    
        final_df=temp_df.select("store_chk_hdr_key","line_key","todo_key","prod_grp_key","optionans","notesans","run_id","CREATE_dt","FILE_NAME")
                        

        
        # write to target table
        final_df.write.mode("append").saveAsTable(target_table)

        file_name=file_name+''_''+datetime.now().strftime("%Y%m%d%H%M%S")
        
        #move to success
        final_df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)



        return ''Success''

    except KeyError as key_error:
        # Handle KeyError (missing columns) here
        error_message = f"KeyError: {str(key_error)}"
        return error_message

    
    except Exception as e:
        # Handle exceptions here
        error_message = f"Error: {str(e)}"
        return error_message
';
CREATE OR REPLACE PROCEDURE PCFSDL_RAW.PROMOTIONWEEKLYSELL_PREPROCESSING("PARAM" ARRAY)
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
from snowflake.snowpark.types import  StringType, StructType, StructField,IntegerType
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz



def main(session: snowpark.Session,Param): 
       
    try :
            
        #Param=[''PromotionWeeklySell.csv'',''PCFSDL_RAW.DEV_LOAD_STAGE_ADLS'',''dev/file_system/PromotionWeeklySell'',''sdl_px_weekly_sell'']

        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        # Define the schema for the DataFrame
        
        df_schema= StructType([
            StructField("promotionrowid", StringType()),
            StructField("p_promonumber", StringType()),
            StructField("transactionlongname", StringType()),
            StructField("gltt_rowid", StringType()),
            StructField("ac_longname", StringType()),
            StructField("ac_code", StringType()),
            StructField("activity", StringType()),
            StructField("sku_stockcode", StringType()),
            StructField("sku_longname", StringType()),
            StructField("sku_profitcentre", StringType()),
            StructField("promotionforecastweek", StringType()),
            StructField("committed_amount", StringType()),
            StructField("planspend_total", StringType()),
            StructField("paid_total", StringType()),
            StructField("writeback_tot", StringType()),
            StructField("balance_tot", StringType()),
            StructField("case_quantity", StringType()),
            StructField("promotionitemstatus", StringType())
        ])
                
        dataframe = session.read.schema(df_schema) \\
        .option("skip_header",1) \\
        .option("field_delimiter", "|") \\
        .option("field_optionally_enclosed_by", "\\"")  \\
        .option("encoding","UTF-8")\\
        .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        
        dataframe=dataframe.na.drop("all")
        if dataframe.count()==0:
            return "No Data in file"

        dataframe = dataframe.with_column("FILE_NAME", lit(file_name))
       
        # Load Data to the target table
        dataframe.write.mode("append").saveAsTable(target_table)
       
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        #move to success
        file_name=file_name[0:23]+''_''+datetime.now().strftime("%Y%m%d%H%M%S")
        dataframe.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)

        
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
CREATE OR REPLACE PROCEDURE PCFSDL_RAW.PCF_PERENSO_RANGING_PRODUCT_PREPROCESSING("PARAM" ARRAY)
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
from snowflake.snowpark.functions import col,lit
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField
import pandas as pd
from datetime import datetime
import pytz
 
def main(session: snowpark.Session,Param):
 
    try:
 
        #Param=[''RangingProduct_20240320223707.csv'',''PCFSDL_RAW.DEV_LOAD_STAGE_ADLS'',''dev/ap_perenso/transation/ranging/'',''sdl_perenso_ranging_product'']
 
        # Extracting parameters from the input
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
 
 
        # Define the schema for the DataFrame
        df_schema=StructType([
            StructField("RANGING_KEY",IntegerType()),
            StructField("PROD_KEY",IntegerType()),
            StructField("ACCT_GRP_KEY",IntegerType()),
            StructField("CORE",StringType()),
            StructField("RANGE_RANK",StringType())
            
            ])
 
        # Read the CSV file into a DataFrame
        dataframe = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", ",")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .option("skip_blank_lines", True)\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
 
        #---------------------------Transformation logic ------------------------------#
 
        # Handle null values or empty rows
        dataframe=dataframe.na.drop("all")
 
        # Check for empty Dataframe
        if dataframe.count()==0:
            return "No Data in file"
    
        dataframe = dataframe.with_column("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        dataframe = dataframe.with_column("CREATE_DT",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        dataframe = dataframe.with_column("FILE_NAME", lit(file_name))
        # Creating copy of the Dataframe
        final_df = dataframe.select("RANGING_KEY", "PROD_KEY", "ACCT_GRP_KEY", "CORE", "RANGE_RANK","RUN_ID","CREATE_DT","FILE_NAME")
 
        # Load Data to the target table
        final_df.write.mode("append").saveAsTable(target_table)
 
         
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
 
        # write to success folder
        file_name=file_name.split(".")[0]
        final_df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True,file_format_name="PCFSDL_RAW.MY_CSV_FORMAT")
        return "Success"
 
    
 
    except KeyError as key_error:
        # Handle KeyError (missing columns) here
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message
    except Exception as e:
        # Handle exceptions here
        error_message = f"Error: {str(e)}"
        return error_message';
