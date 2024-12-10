create or replace TABLE PROD_DNA_LOAD.THASDL_RAW.SDL_TH_MT_WATSONS_1209_UAT (
	DIV VARCHAR(50),
	DEPT VARCHAR(50),
	CLASS VARCHAR(50),
	SUBCLASS VARCHAR(100),
	ITEM VARCHAR(100),
	ITEM_DESC VARCHAR(200),
	NON_SLOW VARCHAR(100),
	NON_SLOW2 VARCHAR(100),
	FINANCE_STATUS VARCHAR(200),
	CREATE_DATETIME VARCHAR(50),
	PRIM_SUPPLIER VARCHAR(100),
	OLD_SUPP_NO VARCHAR(100),
	SUPP_DESC VARCHAR(200),
	LEAD_TIME VARCHAR(50),
	UNIT_COST VARCHAR(100),
	UNIT_RETAIL_ZONE5 VARCHAR(100),
	ITEM_STATUS VARCHAR(100),
	STATUS_WH VARCHAR(100),
	STATUS_WH_UPDATE_DATE VARCHAR(50),
	STATUS_STORE VARCHAR(100),
	STATUS_STORE_UPDATE_DATE VARCHAR(50),
	STATUS_XDOCK VARCHAR(50),
	STATUS_XDOCK_UPDATE_DATE VARCHAR(50),
	SOURCE_METHOD VARCHAR(100),
	SOURCE_WH VARCHAR(100),
	POG VARCHAR(100),
	PRODUCT_TYPE VARCHAR(200),
	LABEL_UDA VARCHAR(100),
	BRAND VARCHAR(100),
	ITEM_TYPE VARCHAR(100),
	RETURN_POLICY VARCHAR(100),
	RETURN_TYPE VARCHAR(100),
	WH_WAC VARCHAR(100),
	IN_TAX VARCHAR(50),
	TAX_RATE VARCHAR(50),
	STOCK_CAT VARCHAR(50),
	ORDER_FLAG VARCHAR(50),
	NEW_ITEM_13WEEK VARCHAR(50),
	DEACTIVATE_DATE VARCHAR(50),
	WH_ON_ORDER VARCHAR(50),
	FIRST_RCV VARCHAR(50),
	PROMO_MONTH VARCHAR(50),
	SALES_TW VARCHAR(50),
	NET_AMT VARCHAR(50),
	NET_COST VARCHAR(50),
	SALE_AVG_QTY_13WEEKS NUMBER(16,4),
	SALE_AVG_AMT_13WEEKS NUMBER(16,4),
	SALE_AVG_COST13WEEKS NUMBER(16,4),
	NET_QTY_YTD NUMBER(16,4),
	NET_AMT_YTD NUMBER(16,4),
	NET_COST_YTD NUMBER(16,4),
	TURN_WK VARCHAR(100),
	WH_SOH NUMBER(16,4),
	STORE_TOTAL_STOCK NUMBER(16,4),
	TOTAL_STOCK_QTY NUMBER(16,4),
	WH_STOCK_AMT NUMBER(16,4),
	STORE_TOTAL_STOCK_AMT NUMBER(16,4),
	TOTAL_STOCK_XVAT NUMBER(16,4),
	PRO2 VARCHAR(50),
	DISC VARCHAR(50),
	PRO22 VARCHAR(50),
	PRO2_PERT_DISC VARCHAR(50),
	FIRST_DATE_SMS VARCHAR(50),
	AGING_SMS VARCHAR(50),
	GROUP_W VARCHAR(50),
	WIN VARCHAR(50),
	POG_2 VARCHAR(50),
	FILE_NAME VARCHAR(100),
	DATE VARCHAR(10)
);

CREATE OR REPLACE PROCEDURE DEV_DNA_LOAD.THASDL_RAW.SDL_TH_MT_WATSONS_1206("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python','xlrd','openpyxl')
HANDLER = 'main'
EXECUTE AS OWNER
AS '
from snowflake.snowpark.functions import col, lit, date_format, current_timestamp, to_date, year, month, concat, format_number, regexp_replace, to_timestamp, when, trim, upper, concat, split, coalesce,get, regexp_count,seq1,row_number,to_date,is_null
import pandas as pd
from datetime import datetime
from snowflake.snowpark.window import Window
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField,DecimalType,DateType
from snowflake.snowpark.files import SnowflakeFile
import snowflake.snowpark as snowpark
from re import sub
import pytz
import csv


def main(session: snowpark.Session, Param):
    try:
        # Parameters
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]

        full_path = "@"+stage_name+"/"+temp_stage_path+"/"+file_name
        with SnowflakeFile.open(full_path, "rb", require_scoped_url = False) as f:
            excel_data = pd.read_excel(f, sheet_name=4)
            
        schema = StructType([
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
    StructField("POG_2", StringType())])
        
        # Create an empty DataFrame with the defined schema
        snowpark_df = session.create_dataframe([], schema)

        # Convert the pandas DataFrame to a list of tuples
        data = [tuple(row) for row in excel_data.to_numpy()]

        # Insert the data into the Snowpark DataFrame
        snowpark_df = session.create_dataframe(data, schema)
        
        snowpark_df = snowpark_df.withColumn("SALE_AVG_QTY_13WEEKS",regexp_replace(col("SALE_AVG_QTY_13WEEKS"), r''[^0-9.-]'', ''''))
        snowpark_df = snowpark_df.withColumn("SALE_AVG_QTY_13WEEKS",when(col("SALE_AVG_QTY_13WEEKS")=='''',lit(None)).otherwise(col("SALE_AVG_QTY_13WEEKS")))
        snowpark_df = snowpark_df.withColumn("SALE_AVG_AMT_13WEEKS",regexp_replace(col("SALE_AVG_AMT_13WEEKS"), r''[^0-9.-]'', ''''))
        snowpark_df = snowpark_df.withColumn("SALE_AVG_AMT_13WEEKS",when(col("SALE_AVG_AMT_13WEEKS")=='''',lit(None)).otherwise(col("SALE_AVG_AMT_13WEEKS")))
        snowpark_df = snowpark_df.withColumn("SALE_AVG_COST13WEEKS",regexp_replace(col("SALE_AVG_COST13WEEKS"), r''[^0-9.-]'', ''''))
        snowpark_df = snowpark_df.withColumn("SALE_AVG_COST13WEEKS",when(col("SALE_AVG_COST13WEEKS")=='''',lit(None)).otherwise(col("SALE_AVG_COST13WEEKS")))
        snowpark_df = snowpark_df.withColumn("NET_QTY_YTD",regexp_replace(col("NET_QTY_YTD"), r''[^0-9.-]'', ''''))
        snowpark_df = snowpark_df.withColumn("NET_QTY_YTD",when(col("NET_QTY_YTD")=='''',lit(None)).otherwise(col("NET_QTY_YTD")))
        snowpark_df = snowpark_df.withColumn("NET_AMT_YTD",regexp_replace(col("NET_AMT_YTD"), r''[^0-9.-]'', ''''))
        snowpark_df = snowpark_df.withColumn("NET_AMT_YTD",when(col("NET_AMT_YTD")=='''',lit(None)).otherwise(col("NET_AMT_YTD")))
        snowpark_df = snowpark_df.withColumn("NET_COST_YTD",regexp_replace(col("NET_COST_YTD"), r''[^0-9.-]'', ''''))
        snowpark_df = snowpark_df.withColumn("NET_COST_YTD",when(col("NET_COST_YTD")=='''',lit(None)).otherwise(col("NET_COST_YTD")))
        snowpark_df = snowpark_df.withColumn("WH_SOH",regexp_replace(col("WH_SOH"), r''[^0-9.-]'', ''''))
        snowpark_df = snowpark_df.withColumn("WH_SOH",when(col("WH_SOH")=='''',lit(None)).otherwise(col("WH_SOH")))
        snowpark_df = snowpark_df.withColumn("STORE_TOTAL_STOCK",regexp_replace(col("STORE_TOTAL_STOCK"), r''[^0-9.-]'', ''''))
        snowpark_df = snowpark_df.withColumn("STORE_TOTAL_STOCK",when(col("STORE_TOTAL_STOCK")=='''',lit(None)).otherwise(col("STORE_TOTAL_STOCK")))
        snowpark_df = snowpark_df.withColumn("TOTAL_STOCK_QTY",regexp_replace(col("TOTAL_STOCK_QTY"), r''[^0-9.-]'', ''''))
        snowpark_df = snowpark_df.withColumn("TOTAL_STOCK_QTY",when(col("TOTAL_STOCK_QTY")=='''',lit(None)).otherwise(col("TOTAL_STOCK_QTY")))
        snowpark_df = snowpark_df.withColumn("WH_STOCK_AMT",regexp_replace(col("WH_STOCK_AMT"), r''[^0-9.-]'', ''''))
        snowpark_df = snowpark_df.withColumn("WH_STOCK_AMT",when(col("WH_STOCK_AMT")=='''',lit(None)).otherwise(col("WH_STOCK_AMT")))
        snowpark_df = snowpark_df.withColumn("STORE_TOTAL_STOCK_AMT",regexp_replace(col("STORE_TOTAL_STOCK_AMT"), r''[^0-9.-]'', ''''))
        snowpark_df = snowpark_df.withColumn("STORE_TOTAL_STOCK_AMT",when(col("STORE_TOTAL_STOCK_AMT")=='''',lit(None)).otherwise(col("STORE_TOTAL_STOCK_AMT")))
        snowpark_df = snowpark_df.withColumn("TOTAL_STOCK_XVAT",regexp_replace(col("TOTAL_STOCK_XVAT"), r''[^0-9.-]'', ''''))
        snowpark_df = snowpark_df.withColumn("TOTAL_STOCK_XVAT",when(col("TOTAL_STOCK_XVAT")=='''',lit(None)).otherwise(col("TOTAL_STOCK_XVAT")))
        snowpark_df = snowpark_df.withColumn("file_name", lit(file_name))        
        snowpark_df = snowpark_df.withColumn("date", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S"))) 
        snowdf = snowpark_df.select(
           "div",
            "dept",
            "class",
            "subclass",
            "item",
            "item_desc",
            "non_slow",
            "non_slow2",
            "finance_status",
            "create_datetime",
            "prim_supplier",
            "old_supp_no",
            "supp_desc",
            "lead_time",
            "unit_cost",
            "unit_retail_zone5",
            "item_status",
            "status_wh",
            "status_wh_update_date",
            "status_store",
            "status_store_update_date",
            "status_xdock",
            "status_xdock_update_date",
            "source_method",
            "source_wh",
            "pog",
            "product_type",
            "label_uda",
            "brand",
            "item_type",
            "return_policy",
            "return_type",
            "wh_wac",
            "in_tax",
            "tax_rate",
            "stock_cat",
            "order_flag",
            "new_item_13week",
            "deactivate_date",
            "wh_on_order",
            "first_rcv",
            "promo_month",
            "sales_tw",
            "net_amt",
            "net_cost",
            "sale_avg_qty_13weeks",
            "sale_avg_amt_13weeks",
            "sale_avg_cost13weeks",
            "net_qty_ytd",
            "net_amt_ytd",
            "net_cost_ytd",
            "turn_wk",
            "wh_soh",
            "store_total_stock",
            "total_stock_qty",
            "wh_stock_amt",
            "store_total_stock_amt",
            "total_stock_xvat",
            "pro2",
            "disc",
            "pro22",
            "pro2_pert_disc",
            "first_date_sms",
            "aging_sms",
            "group_w",
            "win",
            "pog_2",
            "file_name",
            "date"
            )
        snowdf.write.mode("append").saveAsTable(stage_name.split(".")[0] + "." + target_table)   
        file_name=file_name.split(".")[0]+"_"+ datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        #move to success
        snowdf.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)  
        
        return ''Success''       
        
        
        #return dffiltered
    
    except KeyError as key_error:
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message
    
    except Exception as e:
        error_message = f"Error: {str(e)}"
        return error_message
            
    except Exception as e:
        error_message = f"Error: {str(e)}"
        return error_message 
    
';


CALL PROD_DNA_LOAD.THASDL_RAW.SDL_TH_MT_WATSONS_1206(['2024_JJ_Jun_FB_20240703.xlsx','THASDL_RAW.PROD_LOAD_STAGE_ADLS','prd/TH_MT/transaction/AS_WATSONS/source','SDL_TH_MT_WATSON_1209_UAT']);
