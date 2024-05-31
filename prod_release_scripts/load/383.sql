update parameters
set PARAMETER_NAME = 'sheet_names' and PARAMETER_VALUE = 'Current Week'
where PARAMETER_ID = 5533;

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

        snowdf=df.select("INV_DATE", "Rank", "Article_Code", "ARTICLEDESC", 
                        "MM_Code", "MM_Name", "CM_Code", "CM_Name", "RM_Code", "RM_Name", 
                        "Rep_Code", "Replenisher", "Goods_Supplier_Code", "Goods_Supplier_Name", 
                        "LT", "DC_Code", "ACD", "RP_Type", "ALC_Status", "OM", "VP", "Ti", 
                        "Hi", "WW_Stores", "SL_PERC", "SL_Missed_Value", "SOH_OMs", "SOO_OMs", 
                        "SOH_PRICE", "Demand_OMs", "Issues_OMs", "Not_Supplied_OMs", "Fairshare_OMs", 
                        "Overlay_OMs", "AWD_OMs", "AVG_ISSUES", "DOS_OMs", "Due_Date", 
                        "Reason", "OOS_Comments", "OOS_28_DAYS", "Cons_Days_OOS", 
                        "Total_Wholesale_Demand_OM", "Total_Wholesale_Issue_OM", "Wholesale_Flag")

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


CREATE OR REPLACE PROCEDURE THASDL_RAW.SDL_TH_MT_WATSONS("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python','xlrd','openpyxl')
HANDLER = 'main'
EXECUTE AS OWNER
AS '
from snowflake.snowpark.functions import col, lit, current_timestamp, trim, upper, regexp_replace
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DecimalType, DateType
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
from snowflake.snowpark.files import SnowflakeFile

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

        # Process column names
        excel_data.columns = [col.strip().upper().replace(" ", "_").replace("\\n", "_") for col in excel_data.columns]
        
        # Rename specific columns
        columns_to_rename = {
            "NON/SLOW": "NON_SLOW",
            "NON/SLOW2": "NON_SLOW2",
            "DEACTI_VATE_DATE": "DEACTIVATE_DATE",
            "PRO8": "PRO2",
            "%DISC": "DISC",
            "PRO82": "PRO22",
            "PRO8_%DISC": "PRO2_PERT_DISC",
            "GROUP": "GROUP_W",
            "WIN_SUP": "WIN",
            "POG": "POG_2",
            "P_O_G": "POG"
        }
        excel_data.rename(columns=columns_to_rename, inplace=True)
        excel_data["SALES_TW"] = excel_data["SALES_TW"].str.strip().replace(["-", "                -", ""], None) 
        excel_data["NET_AMT"] = excel_data["SALES_TW"].str.strip().replace(["-", "                -", ""], None) 
        excel_data["NET_COST"] = excel_data["SALES_TW"].str.strip().replace(["-", "                -", ""], None) 
        excel_data["TURN_WK"] = excel_data["SALES_TW"].str.strip().replace(["-", "                -", ""], None) 

        columns_to_clean = [
            "SALE_AVG_QTY_13WEEKS",
            "SALE_AVG_AMT_13WEEKS",
            "SALE_AVG_COST13WEEKS",
            "NET_QTY_YTD",
            "NET_AMT_YTD",
            "NET_COST_YTD",
            "WH_SOH",
            "STORE_TOTAL_STOCK",
            "TOTAL_STOCK_QTY",
            "WH_STOCK_AMT",
            "STORE_TOTAL_STOCK_AMT",
            "TOTAL_STOCK_XVAT"
        ]
        
        for col_name in columns_to_clean:
            excel_data[col_name] = excel_data[col_name].astype(str).str.strip().replace(["-", "                -", ""], None)
            excel_data[col_name] = pd.to_numeric(excel_data[col_name])
    
        # Create Snowflake DataFrame
        df = session.create_dataframe(excel_data)
        
        # Add additional transformations if needed
        df = df.with_column("DATE", lit(datetime.now().strftime("%Y%m%d"))) \\
               .with_column("FILE_NAME", lit(file_name))

        # Select required columns
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



        # Drop rows where all columns are null
        snowdf = snowdf.na.drop("all")
        if snowdf.count() == 0:
            return "No Data in table"

        # Archive processed file
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")

        snowdf.write.copy_into_location(
            "@"+stage_name+
            "/"+temp_stage_path+
            "/"+"processed/success/"+formatted_year+
            "/"+formatted_month+
            "/"+file_name,
            header=True,
            OVERWRITE=True
        )
        
        # Write DataFrame to target table
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
