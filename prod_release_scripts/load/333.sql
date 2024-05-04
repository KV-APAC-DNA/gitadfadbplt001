create or replace TABLE PHLSDL_RAW.SDL_PH_LE_TRGT (
	JJ_MNTH_ID VARCHAR(10),
	CUST_ID VARCHAR(20),
	TRGT_TYPE VARCHAR(10),
	BRND_CD VARCHAR(100),
	TP_TRGT_AMT VARCHAR(100),
	FILENAME VARCHAR(20),
	CDL_DTTM VARCHAR(50),
	CURR_DATE TIMESTAMP_NTZ(9)
);

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
        dataframe = dataframe.with_column("CREATE_DT",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))


        #Creating copy of the Dataframe
        final_df = dataframe.select("INV_DT","state", "ware_house", "pe_item_no", "item_description", "stock_details_promo_ind", "stock_details_vendor", "stock_details_vendor_description", "stock_details_sub_vendor", "stock_details_lead_time", "stock_details_buyer_number", "stock_details_buyer_name", "stock_details_stock_control_email", "stock_details_soh", "stock_details_soo", "stock_details_awd", "stock_details_weeks_cover",\\
                                    "inbound_ordered_cases", "inbound_received_cases", "inbound_po_number", "item_details_item_sub_range_code", "item_details_item_sub_range_description", "item_details_pack_size", "item_details_buying_master_pack", "item_details_retail_unit", "item_details_buy_in_pallet", "item_details_ti", "item_details_hi", "item_details_pallet", "item_details_item_status", "item_details_delete_code",\\
                                    "item_details_deletion_date", "item_details_metcash_item_type", "item_details_ord_split_cat_code", "item_details_ndc_item", "item_details_imported_goods", "item_details_code_date", "item_details_packed_on_date", "item_details_incremental_days", "item_details_max_shelf_days", "item_details_receiving_limit", "item_details_dispatch_limit", "item_details_current_dd", "item_details_date_added_to_og",\\
                                    "item_details_consumer_gtin", "item_details_inner_gtin", "item_details_outer_gtin", "sales_week_6", "sales_week_5", "sales_week_4", "sales_week_3", "sales_week_2", "sales_week_1", "sales_this_week","CREATE_DT")

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
