CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.INDSDL_RAW.SDL_LIDAR_FF_OTH_PAID_MEDIA("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '
from snowflake.snowpark.functions import col, lit, date_format, current_timestamp, to_date, year, month, concat, format_number, regexp_replace, to_timestamp, when, trim, upper
from snowflake.snowpark.types import StringType, StructType, StructField,DecimalType
import snowflake.snowpark
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
            StructField("CLIENT", StringType()),
			StructField("PRODUCT_NAME", StringType()),
            StructField("CAMPAIGN_NAME", StringType()),
            StructField("OBJECTIVE", StringType()),
            StructField("YEAR", StringType()),
            StructField("QUARTER", StringType()),
            StructField("MONTH", StringType()),
            StructField("START_DATE", StringType()),
            StructField("END_DATE", StringType()),
            StructField("PUBLISHER", StringType()),
            StructField("DOMAIN_SITE", StringType()),
            StructField("CATEGORY_PUBLISHER_TYPE", StringType()),
            StructField("SECTION", StringType()),
            StructField("TARGETING", StringType()),
            StructField("GEOGRAPHY", StringType()),
            StructField("DEMO", StringType()),
            StructField("AD_UNIT", StringType()),
            StructField("NORMALIZED_AD_UNIT", StringType()),
            StructField("AD_UNIT_TYPE", StringType()),
            StructField("CREATIVE_SIZE", StringType()),
            StructField("NO_OF_DAYS", StringType()),
            StructField("PROGRAMMATIC", StringType()),
            StructField("DSP", StringType()),
            StructField("MEDIA_PROPERTY_TYPE", StringType()),
            StructField("MEDIUM", StringType()),
            StructField("DEAL_TYPE", StringType()),
            StructField("REACH", StringType()),
            StructField("DELIVERED_IMPRESSIONS", StringType()),
            StructField("DELIVERED_TOTAL_VIEWS_VIDEO", StringType()),
            StructField("VIDEO_FIRST_QUARTILE_VIEWS", StringType()),
            StructField("VIDEO_SECOND_QUARTILE_VIEWS", StringType()),
            StructField("VIDEO_THIRD_QUARTILE_VIEWS", StringType()),
            StructField("VIDEO_TOTAL_VIEWS", StringType()),
			StructField("DELIVERED_CLICKS", StringType()),
			StructField("DELIVERED_LINK_CLICKS", StringType()),
			StructField("DELIVERED_ENGAGEMENTS", StringType()),
			StructField("DELIVERED_NET_RATE", StringType()),
			StructField("NET_COST", StringType())
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

        df = df.na.drop("all")
        if df.count() == 0:
            return "No Data in table"

        file_name1 = file_name.split(".")[0] + "_" + datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S") + ".csv"

        df = df.with_column("LOAD_DATE", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S"))) \\
               .with_column("FILE_NAME", lit(file_name1))

        
        snowdf = df.select(
            "CLIENT",
            "PRODUCT_NAME",
            "CAMPAIGN_NAME",
            "OBJECTIVE",
            "YEAR",
            "QUARTER",
            "MONTH",
            "START_DATE",
            "END_DATE",
            "PUBLISHER",
            "DOMAIN_SITE",
            "CATEGORY_PUBLISHER_TYPE",
            "SECTION",
            "TARGETING",
            "GEOGRAPHY",
            "DEMO",
            "AD_UNIT",
            "NORMALIZED_AD_UNIT",
            "AD_UNIT_TYPE",
            "CREATIVE_SIZE",
            "NO_OF_DAYS",
            "PROGRAMMATIC",
            "DSP",
            "MEDIA_PROPERTY_TYPE",
            "MEDIUM",
            "DEAL_TYPE",
            "REACH",
            "DELIVERED_IMPRESSIONS",
            "DELIVERED_TOTAL_VIEWS_VIDEO",
            "VIDEO_FIRST_QUARTILE_VIEWS",
            "VIDEO_SECOND_QUARTILE_VIEWS",
            "VIDEO_THIRD_QUARTILE_VIEWS",
            "VIDEO_TOTAL_VIEWS",
            "DELIVERED_CLICKS",
			"DELIVERED_LINK_CLICKS",
			"DELIVERED_ENGAGEMENTS",
			"DELIVERED_NET_RATE",
			"NET_COST",
            "FILE_NAME",
            "LOAD_DATE"
        )
        
        # Check if DataFrame is empty
        

        # Archive to success
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        file_name=file_name.split(".")[0]+"_"+ datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")

        snowdf.write.copy_into_location(
            "@"+stage_name+
            "/"+temp_stage_path+
            "/"+"processed/success/"+formatted_year+
            "/"+formatted_month+
            "/"+file_name,
            header=True,
            OVERWRITE=True
        )
        
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

CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.INDSDL_RAW.SDL_LIDAR_FF_TV_SPENDS("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '
from snowflake.snowpark.functions import col, lit, date_format, current_timestamp, to_date, year, month, concat, format_number, regexp_replace, to_timestamp, when, trim, upper
from snowflake.snowpark.types import StringType, StructType, StructField,DecimalType
import snowflake.snowpark
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
            StructField("CLIENT", StringType()),
			StructField("CL_CODE", StringType()),
            StructField("CL_GSTIN", StringType()),
            StructField("PRODUCT", StringType()),
            StructField("EST_NO", StringType()),
            StructField("EST_DT", StringType()),
            StructField("EST_AMOUNT", StringType()),
            StructField("EST_RT10SC", StringType()),
            StructField("SUPP_NO", StringType()),
            StructField("SUPP_DT", StringType()),
            StructField("CAM_CODE", StringType()),
            StructField("CAM_DES", StringType()),
            StructField("CAM_REM", StringType()),
            StructField("CHANNEL_CD", StringType()),
            StructField("CHANNEL", StringType()),
            StructField("PROGRAM", StringType()),
            StructField("DAYS", StringType()),
            StructField("SPOT_DATE", StringType()),
            StructField("SPOT_DAY", StringType()),
            StructField("ORI_DATE", StringType()),
            StructField("TELE_TIME", StringType()),
            StructField("DURATION", StringType()),
            StructField("REV_DATE", StringType()),
            StructField("STATUS", StringType()),
            StructField("RO_NUMBER", StringType()),
            StructField("RO_DATE", StringType()),
            StructField("RO_AMOUNT", StringType()),
            StructField("RO_RT_10SC", StringType()),
            StructField("CAN_NO", StringType()),
            StructField("CAN_DATE", StringType()),
            StructField("SAVEEST_NO", StringType()),
            StructField("CURRENCY", StringType()),
            StructField("MONI_COMP", StringType()),
			StructField("MONI_NO", StringType()),
			StructField("MONI_DATE", StringType()),
			StructField("AD_SPOTORD", StringType()),
			StructField("TOT_SPOT", StringType()),
			StructField("AD_TIME", StringType()),
			StructField("AD_LANG", StringType()),
			StructField("OB_NUMBER", StringType()),
			StructField("OB_DATE", StringType()),
			StructField("OB_AMOUNT", StringType()),
			StructField("OB_RT_10SC", StringType()),
			StructField("VENDOR_CD", StringType()),
			StructField("IB_NUMBER", StringType()),
			StructField("IB_DATE", StringType()),
			StructField("ENTRC", StringType()),
			StructField("AD_NO", StringType()),
			StructField("CAPTION", StringType()),
			StructField("ROTOCOMP", StringType()),
			StructField("VN_GSTIN", StringType()),
			StructField("PONO", StringType()),
			StructField("PO_DT", StringType()),
			StructField("SAPNO", StringType()),
			StructField("FA_IB_NO", StringType()),
			StructField("AAC_AMT", StringType()),
			StructField("AAC_PER", StringType()),
			StructField("SAC_PER", StringType()),
			StructField("NET_RO", StringType()),
			StructField("NET_OB", StringType()),
			StructField("NET_IB", StringType()),
			StructField("RO_CLIENT", StringType()),
			StructField("CAT1_CODE", StringType()),
			StructField("CAT2_CODE", StringType()),
			StructField("CAT1_NAME", StringType()),
			StructField("CAT2_NAME", StringType()),
			StructField("STD_RATE", StringType()),
			StructField("STD_COST", StringType()),
			StructField("PLAN_RATE", StringType()),
			StructField("PLAN_COST", StringType()),
			StructField("BILL_NO", StringType()),
			StructField("SCHMONTH", StringType()),
			StructField("SCHYEAR", StringType()),
			StructField("TAPE_NO", StringType()),
			StructField("ADTYPDESC", StringType()),
			StructField("SUPBILL_NO", StringType()),
			StructField("SUPBILL_DT", StringType()),
			StructField("SUPBILLAMT", StringType()),
			StructField("SUPBILLNET", StringType()),
			StructField("OB_STATE", StringType()),
			StructField("OB_GST_ON", StringType()),
			StructField("AGNGSTBASE", StringType()),
			StructField("AGNCGSTPER", StringType()),
			StructField("AGNCOMCGST", StringType()),
			StructField("AGNSGSTPER", StringType()),
			StructField("AGNCOMSGST", StringType()),
			StructField("AGNIGSTPER", StringType()),
			StructField("AGNCOMIGST", StringType()),
			StructField("RECOVERTAX", StringType()),
			StructField("OB_GSTBASE", StringType()),
			StructField("OB_CGSTPER", StringType()),
			StructField("OB_CGSTAMT", StringType()),
			StructField("OB_SGSTPER", StringType()),
			StructField("OB_SGSTAMT", StringType()),
			StructField("OB_IGSTPER", StringType()),
			StructField("OB_IGSTAMT", StringType()),
			StructField("IRN_NO", StringType()),
			StructField("ACK_NO", StringType()),
			StructField("ACK_DT", StringType()),
			StructField("GROUP", StringType()),
			StructField("TEL_STN", StringType()),
			StructField("KEY", StringType()),
			StructField("CL_TOTAL", StringType()),
			StructField("ROCL_CD", StringType()),
			StructField("STVENTER", StringType()),
			StructField("EST_SPOT", StringType()),
			StructField("MEDIAM", StringType()),
			StructField("PR_CODE", StringType()),
			StructField("OB_COST", StringType()),
			StructField("UIDKEY", StringType()),
			StructField("COMP_GRPCD", StringType()),
			StructField("COMP_GRPNM", StringType()),
			StructField("HOUSE_NAME", StringType()),
			StructField("PROV_ON", StringType())
			
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

        df = df.na.drop("all")
        if df.count() == 0:
            return "No Data in table"

        file_name1 = file_name.split(".")[0] + "_" + datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S") + ".csv"

        df = df.with_column("LOAD_DATE", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S"))) \\
               .with_column("FILE_NAME", lit(file_name1))

        
        snowdf = df.select(
            "CLIENT",
            "CL_CODE",
            "CL_GSTIN",
            "PRODUCT",
            "EST_NO",
            "EST_DT",
            "EST_AMOUNT",
            "EST_RT10SC",
            "SUPP_NO",
            "SUPP_DT",
            "CAM_CODE",
            "CAM_DES",
            "CAM_REM",
            "CHANNEL_CD",
            "CHANNEL",
            "PROGRAM",
            "DAYS",
            "SPOT_DATE",
            "SPOT_DAY",
            "ORI_DATE",
            "TELE_TIME",
            "DURATION",
            "REV_DATE",
            "STATUS",
            "RO_NUMBER",
            "RO_DATE",
            "RO_AMOUNT",
            "RO_RT_10SC",
            "CAN_NO",
            "CAN_DATE",
            "SAVEEST_NO",
            "CURRENCY",
            "MONI_COMP",
			"MONI_NO",
			"MONI_DATE",
			"AD_SPOTORD",
			"TOT_SPOT",
            "AD_TIME",
            "AD_LANG",
			"OB_NUMBER",
			"OB_DATE",
			"OB_AMOUNT",
			"OB_RT_10SC",
			"VENDOR_CD",
			"IB_NUMBER",
			"IB_DATE",
			"ENTRC",
			"AD_NO",
			"CAPTION",
			"ROTOCOMP",
			"VN_GSTIN",
			"PONO",
			"PO_DT",
			"SAPNO",
			"FA_IB_NO",
			"AAC_AMT",
			"AAC_PER",
			"SAC_PER",
			"NET_RO",
			"NET_OB",
			"NET_IB",
			"RO_CLIENT",
			"CAT1_CODE",
			"CAT2_CODE",
			"CAT1_NAME",
			"CAT2_NAME",
			"STD_RATE",
			"STD_COST",
			"PLAN_RATE",
			"PLAN_COST",
			"BILL_NO",
			"SCHMONTH",
			"SCHYEAR",
			"TAPE_NO",
			"ADTYPDESC",
			"SUPBILL_NO",
			"SUPBILL_DT",
			"SUPBILLAMT",
			"SUPBILLNET",
			"OB_STATE",
			"OB_GST_ON",
			"AGNGSTBASE",
			"AGNCGSTPER",
			"AGNCOMCGST",
			"AGNSGSTPER",
			"AGNCOMSGST",
			"AGNIGSTPER",
			"AGNCOMIGST",
			"RECOVERTAX",
			"OB_GSTBASE",
			"OB_CGSTPER",
			"OB_CGSTAMT",
			"OB_SGSTPER",
			"OB_SGSTAMT",
			"OB_IGSTPER",
			"OB_IGSTAMT",
			"IRN_NO",
			"ACK_NO",
			"ACK_DT",
			"GROUP",
			"TEL_STN",
			"KEY",
			"CL_TOTAL",
			"ROCL_CD",
			"STVENTER",
			"EST_SPOT",
			"MEDIAM",
			"PR_CODE",
			"OB_COST",
			"UIDKEY",
			"COMP_GRPCD",
			"COMP_GRPNM",
			"HOUSE_NAME",
			"PROV_ON",
			"FILE_NAME",
			"LOAD_DATE"
			
        )
        
        # Check if DataFrame is empty
        

        # Archive to success
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        file_name=file_name.split(".")[0]+"_"+ datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")

        snowdf.write.copy_into_location(
            "@"+stage_name+
            "/"+temp_stage_path+
            "/"+"processed/success/"+formatted_year+
            "/"+formatted_month+
            "/"+file_name,
            header=True,
            OVERWRITE=True
        )
        
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


CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.INDSDL_RAW.SDL_LIDAR_FF_TV_GRP("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '
from snowflake.snowpark.functions import col, lit, date_format, current_timestamp, to_date, year, month, concat, format_number, regexp_replace, to_timestamp, when, trim, upper
from snowflake.snowpark.types import StringType, StructType, StructField
import pandas as pd
from datetime import datetime
from snowflake.snowpark.window import Window
from snowflake.snowpark.functions import col,lit,concat,regexp_replace,trim,split,rtrim,upper,coalesce,row_number,when,to_date,is_null
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField,DecimalType,DateType
from snowflake.snowpark.files import SnowflakeFile
import snowflake.snowpark as snowpark
from re import sub
import pytz
import csv
def main(session:snowpark.Session,Param):
    try:
        file_name    = Param[0]
        stage_name   = Param[1]
        file_path    = Param[2]
        target_table = Param[3]   
        full_path = "@"+stage_name+"/"+file_path+file_name   
        with SnowflakeFile.open(full_path, "rb", require_scoped_url=False) as f: 
            if ".csv" in file_name:
                temp = pd.read_csv(f, delimiter=",", engine="python",dtype={"Start Time":str})
            elif ".xlsx" in file_name:
                temp = pd.read_excel(f,engine="python",dtype={"Start Time":str})
                
            #Printing Shape before transform
            # print("Shape before transformation : ",temp.shape)
        
        
            # Do NOT UNCOMMENT THIS snippet
            # ##Incorrect timestamp treatment
        temp["Start Time"]=temp["Start Time"].apply(lambda x: x[-8:])
            # print("Time treatment completed")
        
            ##NA treatment
        cols_na=[col for col in temp.columns[25:] if "n.a" in temp[col].unique()]
        for c in cols_na:
            temp.loc[temp[c]=="n.a",c]=0.0
        
        
       #      print("Total KPI Value before transformation :",temp.iloc[:,25:].sum(axis=1).sum())
    
        id_vars = list(temp.columns[0:25])
        # print(id_vars)
        measure_vars = list(temp.columns[25:])
        # print(measure_vars)
        
        temp2 = pd.melt(temp,id_vars=id_vars, value_vars=measure_vars, var_name="kpi", value_name="kpi_1")
        # print("Shape after transformation : ",temp2.shape)
        temp2["kpi_1"] = pd.to_numeric(temp2["kpi_1"], errors="coerce")
        
       
             
        file_name1 = file_name.split(".")[0] + "_" + datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S") + ".csv"
        temp2=session.create_dataframe(temp2)
        temp2 = temp2.with_column("FILE_NAME", lit(file_name1))
        temp2 = temp2.with_column("LOAD_DATE", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        temp2 = temp2.with_column("GCPH_BRAND", lit(None))
        temp2 = temp2.with_column("BRAND_HARMONIZED_BY", lit(None))
        snowdf = temp2.select("*")
            
       #      # Check if DataFrame is empty
            
    
       #      # Archive to success
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        file_name=file_name.split(".")[0]+"_"+ datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
    
        snowdf.write.copy_into_location(
            "@"+stage_name+
            "/"+file_path+
            "/"+"processed/success/"+formatted_year+
            "/"+formatted_month+
            "/"+file_name,
            header=True,
            OVERWRITE=True
        )
            
       #      # Write operation
        temp2.write.mode("append").save_as_table(stage_name.split(".")[0]+"."+target_table)
            
       #      # Success message
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
