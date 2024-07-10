CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.THASDL_RAW.SDL_TH_MT_WATSONS("PARAM" ARRAY)
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
        excel_data["SALES_TW"] = excel_data["SALES_TW"].astype("str")
        excel_data["NET_AMT"] = excel_data["NET_AMT"].astype("str")
        excel_data["NET_COST"] = excel_data["NET_COST"].astype("str")
        excel_data["TURN_WK"] = excel_data["TURN_WK"].astype("str")
        
        excel_data.rename(columns=columns_to_rename, inplace=True)
        excel_data["SALES_TW"] = excel_data["SALES_TW"].str.strip().replace(["-", "                -", ""], None) 
        excel_data["NET_AMT"] = excel_data["NET_AMT"].str.strip().replace(["-", "                -", ""], None) 
        excel_data["NET_COST"] = excel_data["NET_COST"].str.strip().replace(["-", "                -", ""], None) 
        excel_data["TURN_WK"] = excel_data["TURN_WK"].str.strip().replace(["-", "                -", ""], None) 

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
        df = df.with_column("DATE", lit(datetime.now().strftime("%Y-%m-%d"))) \\
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
