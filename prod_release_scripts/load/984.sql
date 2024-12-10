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
        full_path = "@"+stage_name+"/"+file_path+"/"+file_name   
        with SnowflakeFile.open(full_path, "rb", require_scoped_url=False) as f: 
            if ".csv" in file_name:
                temp = pd.read_csv(f, delimiter=",", engine="python",dtype={"Start Time":str})
            elif ".xlsx" in file_name:
                temp = pd.read_excel(f,engine="python",dtype={"Start Time":str})
                
            #Printing Shape before transform
            # print("Shape before transformation : ",temp.shape)
        
        
            # Do NOT UNCOMMENT THIS snippet
            # ##Incorrect timestamp treatment
        #temp["Start Time"]=temp["Start Time"].apply(lambda x: x[-8:])
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
        r = len(temp)//2
        temp2 = pd.melt(temp.loc[:r],id_vars=id_vars, value_vars=measure_vars, var_name="kpi", value_name="kpi_1")
        temp3 = pd.melt(temp.loc[r+1:],id_vars=id_vars, value_vars=measure_vars, var_name="kpi", value_name="kpi_1")
        df_final=pd.concat([temp2,temp3],ignore_index=True)
        df_final["kpi_1"] = pd.to_numeric(df_final["kpi_1"], errors="coerce")
              
        file_name1 = file_name.split(".")[0] + "_" + datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S") + ".csv"
        df_final=session.create_dataframe(df_final)
        df_final = df_final.with_column("FILE_NAME", lit(file_name1))
        df_final = df_final.with_column("LOAD_DATE", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        df_final = df_final.with_column("GCPH_BRAND", lit(None))
        df_final = df_final.with_column("BRAND_HARMONIZED_BY", lit(None))
        snowdf = df_final.select("*")
            
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
        df_final.write.mode("append").save_as_table(stage_name.split(".")[0]+"."+target_table)
            
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





CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.INDSDL_RAW.SDL_LIDAR_FF_PRINT_COMP_SPEND("PARAM" ARRAY)
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
            StructField("MEDIUM", StringType()),
			StructField("CATEGORY", StringType()),
            StructField("COMP", StringType()),
            StructField("COMPTTN", StringType()),
            StructField("MONTH", StringType()),
            StructField("WEEK", StringType()),
            StructField("YR_MTH", StringType()),
            StructField("CONCATE", StringType()),
            StructField("SUPER_CATEGORY", StringType()),
            StructField("PRODUCT_GROUP", StringType()),
            StructField("ADVERTISER", StringType()),
            StructField("PRODUCT", StringType()),
            StructField("AD_MAIN_TYPE", StringType()),
            StructField("AD_SUB_TYPE", StringType()),
            StructField("PARENT_PUBLICATION", StringType()),
            StructField("PUBLICATION", StringType()),
            StructField("SUPPLEMENTARY", StringType()),
            StructField("DATE", StringType()),
            StructField("PAGENO", StringType()),
            StructField("PAGE_TITLE", StringType()),
            StructField("POSITION", StringType()),
            StructField("AD_TYPE", StringType()),
            StructField("AD_LANGUAGE", StringType()),
            StructField("LOCATION", StringType()),
            StructField("PAGE_SIDE", StringType()),
            StructField("PUB_NATURE", StringType()),
            StructField("PUB_GROUP", StringType()),
            StructField("PUB_LANGUAGE", StringType()),
            StructField("PUB_PERIODICITY", StringType()),
            StructField("PUB_GENRE", StringType()),
            StructField("ZONE", StringType()),
            StructField("STATE", StringType()),
            StructField("EDITION", StringType()),
			StructField("SALES_PROMO", StringType()),
			StructField("INNOVATION", StringType()),
			StructField("FESTIVAL", StringType()),
			StructField("AGENCY", StringType()),
			StructField("COL", StringType()),
			StructField("CM", StringType()),
			StructField("AD_CM", StringType()),
			StructField("VOL_CC", StringType()),
			StructField("VOL_SQCM", StringType()),
			StructField("TAM_COST", StringType()),
			StructField("DD", StringType()),
			StructField("MN", StringType()),
			StructField("YR", StringType()),
			StructField("DAY", StringType()),
			StructField("HOUSE_ADS", StringType()),
			StructField("ADS", StringType()),
			StructField("PAGETAG", StringType()),
			StructField("DISCOUNTING_FACTOR", StringType()),
			StructField("ADJUSTED_COST_IN_CR", StringType())
			
			
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
            "MEDIUM",
            "CATEGORY",
            "COMP",
            "COMPTTN",
            "MONTH",
            "WEEK",
            "YR_MTH",
            "CONCATE",
            "SUPER_CATEGORY",
            "PRODUCT_GROUP",
            "ADVERTISER",
            "PRODUCT",
            "AD_MAIN_TYPE",
            "AD_SUB_TYPE",
            "PARENT_PUBLICATION",
            "PUBLICATION",
            "SUPPLEMENTARY",
            "DATE",
            "PAGENO",
            "PAGE_TITLE",
            "POSITION",
            "AD_TYPE",
            "AD_LANGUAGE",
            "LOCATION",
            "PAGE_SIDE",
            "PUB_NATURE",
            "PUB_GROUP",
            "PUB_LANGUAGE",
            "PUB_PERIODICITY",
            "PUB_GENRE",
            "ZONE",
            "STATE",
            "EDITION",
            "SALES_PROMO",
			"INNOVATION",
			"FESTIVAL",
			"AGENCY",
			"COL",
            "CM",
            "AD_CM",
			"VOL_CC",
			"VOL_SQCM",
			"TAM_COST",
			"DD",
			"MN",
			"YR",
			"DAY",
			"HOUSE_ADS",
			"ADS",
			"PAGETAG",
			"DISCOUNTING_FACTOR",
			"ADJUSTED_COST_IN_CR",
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





CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.INDSDL_RAW.SDL_LIDAR_FF_MICROLEVEL_REACH("PARAM" ARRAY)
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
            StructField("REPORT_NAME", StringType()),
			StructField("DATA_SOURCE", StringType()),
            StructField("MERGEID", StringType()),
            StructField("COUNTRY", StringType()),
            StructField("PLATFORM", StringType()),
            StructField("BRAND_NAME", StringType()),
            StructField("SUBBRAND", StringType()),
            StructField("CAMPAIGN_PHASE", StringType()),
            StructField("MARKET_NAME", StringType()),
            StructField("MARKET_CLUSTER", StringType()),
            StructField("CAMPAIGN_NAME_NEW", StringType()),
            StructField("HVA_TG_NAME", StringType()),
            StructField("AD_TYPE", StringType()),
            StructField("BUY_TYPE", StringType()),
            StructField("CAMPAIGN_MONTH", StringType()),
            StructField("MONTH_FILTER", StringType()),
            StructField("REACH_SOURCE_FILE", StringType()),
            StructField("REPORTING_STARTS", StringType()),
            StructField("REPORTING_ENDS", StringType()),
            StructField("CAMPAIGN_START", StringType()),
            StructField("CAMPAIGN_END", StringType()),
            StructField("CURRENCY", StringType()),
            StructField("ACCOUNT_ID", StringType()),
            StructField("ACCOUNT_NAME", StringType()),
            StructField("PARTNER_ID", StringType()),
            StructField("PARTNER", StringType()),
            StructField("ADVERTISER_ID", StringType()),
            StructField("ADVERTISER", StringType()),
            StructField("CAMPAIGN_ID", StringType()),
            StructField("CAMPAIGN_TAXONOMY", StringType()),
            StructField("INSERTION_ORDER_ID", StringType()),
            StructField("AD_SET_ID", StringType()),
            StructField("IO_AD_SET_NAME_TAXONOMY", StringType()),
			StructField("LINE_ITEM_TYPE", StringType()),
			StructField("MEDIA_TYPE", StringType()),
			StructField("BUYING_TYPE", StringType()),
			StructField("OBJECTIVE", StringType()),
			StructField("LATEST_UPDATE", StringType()),
			StructField("REACH", StringType()),
			StructField("ESTM_REACH", StringType()),
			StructField("IMPRESSIONS", StringType()),
			StructField("ESTM_IMPRESSIONS", StringType()),
			StructField("SPENDS", StringType()),
			StructField("ESTM_MEDIA_SPENDS", StringType()),
			StructField("CLICKS", StringType()),
			StructField("ESTM_CLICKS", StringType()),
			StructField("VIDEO_VIEWS_THRUPLAYS", StringType()),
			StructField("ESTM_VIDEO_VIEWS_THRUPLAYS", StringType()),
			StructField("QUARTILE_VIDEO_VIEWS_1ST", StringType()),
			StructField("QUARTILE_VIDEO_VIEWS_2ND", StringType()),
			StructField("QUARTILE_VIDEO_VIEWS_3RD", StringType()),
			StructField("QUARTILE_VIDEO_VIEWS_4TH", StringType()),
			StructField("TRUEVIEW_VIEWS", StringType()),
			StructField("THRUPLAYS", StringType()),
			StructField("SECOND_VIDEO_PLAYS_3", StringType()),
			StructField("COMPANION_VIEWS_VIDEO", StringType()),
			StructField("SKIPS_VIDEO", StringType()),
			StructField("BILLABLE_IMPRESSIONS", StringType()),
			StructField("MEDIA_COST_ADVERTISER_CURRENCY", StringType()),
			StructField("CLICKS_ALL", StringType()),
			StructField("PAGE_ENGAGEMENT", StringType()),
			StructField("POST_ENGAGEMENTS", StringType()),
			StructField("FOLLOWS_OR_LIKES", StringType())
						
	
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
            "REPORT_NAME",
            "DATA_SOURCE",
            "MERGEID",
            "COUNTRY",
            "PLATFORM",
            "BRAND_NAME",
            "SUBBRAND",
            "CAMPAIGN_PHASE",
            "MARKET_NAME",
            "MARKET_CLUSTER",
            "CAMPAIGN_NAME_NEW",
            "HVA_TG_NAME",
            "AD_TYPE",
            "BUY_TYPE",
            "CAMPAIGN_MONTH",
            "MONTH_FILTER",
            "REACH_SOURCE_FILE",
            "REPORTING_STARTS",
            "REPORTING_ENDS",
            "CAMPAIGN_START",
            "CAMPAIGN_END",
            "CURRENCY",
            "ACCOUNT_ID",
            "ACCOUNT_NAME",
            "PARTNER_ID",
            "PARTNER",
            "ADVERTISER_ID",
            "ADVERTISER",
            "CAMPAIGN_ID",
            "CAMPAIGN_TAXONOMY",
            "INSERTION_ORDER_ID",
            "AD_SET_ID",
            "IO_AD_SET_NAME_TAXONOMY",
            "LINE_ITEM_TYPE",
			"MEDIA_TYPE",
			"BUYING_TYPE",
			"OBJECTIVE",
			"LATEST_UPDATE",
            "REACH",
            "ESTM_REACH",
			"IMPRESSIONS",
			"ESTM_IMPRESSIONS",
			"SPENDS",
			"ESTM_MEDIA_SPENDS",
			"CLICKS",
			"ESTM_CLICKS",
			"VIDEO_VIEWS_THRUPLAYS",
			"ESTM_VIDEO_VIEWS_THRUPLAYS",
			"QUARTILE_VIDEO_VIEWS_1ST",
			"QUARTILE_VIDEO_VIEWS_2ND",
			"QUARTILE_VIDEO_VIEWS_3RD",
			"QUARTILE_VIDEO_VIEWS_4TH",
			"TRUEVIEW_VIEWS",
			"THRUPLAYS",
			"SECOND_VIDEO_PLAYS_3",
			"COMPANION_VIEWS_VIDEO",
			"SKIPS_VIDEO",
			"BILLABLE_IMPRESSIONS",
			"MEDIA_COST_ADVERTISER_CURRENCY",
			"CLICKS_ALL",
			"PAGE_ENGAGEMENT",
			"POST_ENGAGEMENTS",
			"FOLLOWS_OR_LIKES",
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





CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.INDSDL_RAW.SDL_LIDAR_FF_PLATFORMLEVEL_REACH("PARAM" ARRAY)
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
            StructField("REPORT_NAME", StringType()),
			StructField("DATA_SOURCE", StringType()),
            StructField("MERGEID", StringType()),
            StructField("COUNTRY", StringType()),
            StructField("PLATFORM", StringType()),
            StructField("BRAND_NAME", StringType()),
            StructField("SUBBRAND", StringType()),
            StructField("CAMPAIGN_PHASE", StringType()),
            StructField("MARKET_NAME", StringType()),
            StructField("MARKET_CLUSTER", StringType()),
            StructField("CAMPAIGN_NAME_NEW", StringType()),
            StructField("HVA_TG_NAME", StringType()),
            StructField("AD_TYPE", StringType()),
            StructField("BUY_TYPE", StringType()),
            StructField("CAMPAIGN_MONTH", StringType()),
            StructField("MONTH_FILTER", StringType()),
            StructField("REACH_SOURCE_FILE", StringType()),
            StructField("REPORTING_STARTS", StringType()),
            StructField("REPORTING_ENDS", StringType()),
            StructField("CAMPAIGN_START", StringType()),
            StructField("CAMPAIGN_END", StringType()),
            StructField("CURRENCY", StringType()),
            StructField("ACCOUNT_ID", StringType()),
            StructField("ACCOUNT_NAME", StringType()),
            StructField("PARTNER_ID", StringType()),
            StructField("PARTNER", StringType()),
            StructField("ADVERTISER_ID", StringType()),
            StructField("ADVERTISER", StringType()),
            StructField("CAMPAIGN_ID", StringType()),
            StructField("CAMPAIGN_TAXONOMY", StringType()),
            StructField("INSERTION_ORDER_ID", StringType()),
            StructField("AD_SET_ID", StringType()),
            StructField("IO_AD_SET_NAME_TAXONOMY", StringType()),
			StructField("LINE_ITEM_TYPE", StringType()),
			StructField("MEDIA_TYPE", StringType()),
			StructField("BUYING_TYPE", StringType()),
			StructField("OBJECTIVE", StringType()),
			StructField("LATEST_UPDATE", StringType()),
			StructField("REACH", StringType()),
			StructField("ESTM_REACH", StringType()),
			StructField("IMPRESSIONS", StringType()),
			StructField("ESTM_IMPRESSIONS", StringType()),
			StructField("SPENDS", StringType()),
			StructField("ESTM_MEDIA_SPENDS", StringType()),
			StructField("CLICKS", StringType()),
			StructField("ESTM_CLICKS", StringType()),
			StructField("VIDEO_VIEWS_THRUPLAYS", StringType()),
			StructField("ESTM_VIDEO_VIEWS_THRUPLAYS", StringType()),
			StructField("QUARTILE_VIDEO_VIEWS_1ST", StringType()),
			StructField("QUARTILE_VIDEO_VIEWS_2ND", StringType()),
			StructField("QUARTILE_VIDEO_VIEWS_3RD", StringType()),
			StructField("QUARTILE_VIDEO_VIEWS_4TH", StringType()),
			StructField("TRUEVIEW_VIEWS", StringType()),
			StructField("THRUPLAYS", StringType()),
			StructField("SECOND_VIDEO_PLAYS_3", StringType()),
			StructField("COMPANION_VIEWS_VIDEO", StringType()),
			StructField("SKIPS_VIDEO", StringType()),
			StructField("BILLABLE_IMPRESSIONS", StringType()),
			StructField("MEDIA_COST_ADVERTISER_CURRENCY", StringType()),
			StructField("CLICKS_ALL", StringType()),
			StructField("PAGE_ENGAGEMENT", StringType()),
			StructField("POST_ENGAGEMENTS", StringType()),
			StructField("FOLLOWS_OR_LIKES", StringType())
						
	
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
            "REPORT_NAME",
            "DATA_SOURCE",
            "MERGEID",
            "COUNTRY",
            "PLATFORM",
            "BRAND_NAME",
            "SUBBRAND",
            "CAMPAIGN_PHASE",
            "MARKET_NAME",
            "MARKET_CLUSTER",
            "CAMPAIGN_NAME_NEW",
            "HVA_TG_NAME",
            "AD_TYPE",
            "BUY_TYPE",
            "CAMPAIGN_MONTH",
            "MONTH_FILTER",
            "REACH_SOURCE_FILE",
            "REPORTING_STARTS",
            "REPORTING_ENDS",
            "CAMPAIGN_START",
            "CAMPAIGN_END",
            "CURRENCY",
            "ACCOUNT_ID",
            "ACCOUNT_NAME",
            "PARTNER_ID",
            "PARTNER",
            "ADVERTISER_ID",
            "ADVERTISER",
            "CAMPAIGN_ID",
            "CAMPAIGN_TAXONOMY",
            "INSERTION_ORDER_ID",
            "AD_SET_ID",
            "IO_AD_SET_NAME_TAXONOMY",
            "LINE_ITEM_TYPE",
			"MEDIA_TYPE",
			"BUYING_TYPE",
			"OBJECTIVE",
			"LATEST_UPDATE",
            "REACH",
            "ESTM_REACH",
			"IMPRESSIONS",
			"ESTM_IMPRESSIONS",
			"SPENDS",
			"ESTM_MEDIA_SPENDS",
			"CLICKS",
			"ESTM_CLICKS",
			"VIDEO_VIEWS_THRUPLAYS",
			"ESTM_VIDEO_VIEWS_THRUPLAYS",
			"QUARTILE_VIDEO_VIEWS_1ST",
			"QUARTILE_VIDEO_VIEWS_2ND",
			"QUARTILE_VIDEO_VIEWS_3RD",
			"QUARTILE_VIDEO_VIEWS_4TH",
			"TRUEVIEW_VIEWS",
			"THRUPLAYS",
			"SECOND_VIDEO_PLAYS_3",
			"COMPANION_VIEWS_VIDEO",
			"SKIPS_VIDEO",
			"BILLABLE_IMPRESSIONS",
			"MEDIA_COST_ADVERTISER_CURRENCY",
			"CLICKS_ALL",
			"PAGE_ENGAGEMENT",
			"POST_ENGAGEMENTS",
			"FOLLOWS_OR_LIKES",
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
