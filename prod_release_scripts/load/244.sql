CREATE OR REPLACE PROCEDURE PACIFIC_PERENSO_CALL_COVERAGE_TARGETS("PARAM" ARRAY)
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
            .option("skip_header",0)\\
            .option("field_delimiter", "|")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

       
        
        df=df.na.drop("all")
        if df.count()==0 :
            return "No Data in file"

        df = df.with_column("call_per_week", lit(None))
        df = df.with_column("run_id", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        df = df.with_column("create_dt", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        
        snowdf= df.select(
                    "account_type_description",
                    "account_grade character",
                    "weekly_targets",
                    "monthly_targets ",
                    "yearly_targets",
                    "call_per_week",
                    "run_id",
                    "create_dt"
                    )
        
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
CREATE OR REPLACE PROCEDURE PA_GROCERY_INV_COLES_PREPROCESSING("PARAM" ARRAY)
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

        # Creating Final Dataframe
        final_df = dataframe.alias("final_df")


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
CREATE OR REPLACE PROCEDURE PA_GROCERY_INV_WOOLWORTHS_PREPROCESSING("PARAM" ARRAY)
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

        #Param=[''WW_AV303_Stock_Status_Weekly_-_25.02.2024.csv'',''PCFSDL_RAW.DEV_LOAD_STAGE_ADLS'',''dev/Pacific_reg_inv/transaction/'',''SDL_DSTR_WOOLWORTH_INV'']
        
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

        df = session.read\\
        .schema(df_schema)\\
        .option("skip_header",1)\\
        .option("field_delimiter", "\\u0001")\\
        .option("field_optionally_enclosed_by", "\\"") \\
        .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        
        df = df.na.drop("all")

        
        if df.count() == 0:
            return "No Data in file"

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
CREATE OR REPLACE PROCEDURE PCF_PERENSO_CALL_OBJECTIVES_PREPROCESSING("PARAM" ARRAY)
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
        # Creating copy of the Dataframe
        final_df = dataframe.select("OBJECTIVE_KEY", "CALL_CREATED_DT", "DESCRIPTION", "ACCT_KEY", "ASSIGNED_USER_KEY","CREATED_USER_KEY",\\
                                    "DUE_DT","COMPLETED_DT","STATUS","RUN_ID","CREATE_DT")
 
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
CREATE OR REPLACE PROCEDURE PCF_PERENSO_DIARYITEM_PREPROCESSING("PARAM" ARRAY)
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

            
        temp_df = temp_df.withColumn("run_id",lit(lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S"))))
        temp_df = temp_df.withColumn("create_dt",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
            
        
                    
        final_df=temp_df.select("diary_item_key","diary_item_type_key","start_time","end_time","acct_key","acct_key_1","create_user_key","complete","run_id","create_dt")
                        


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
CREATE OR REPLACE PROCEDURE PCF_PERENSO_DIARY_ITEM_TIME_PREPROCESSING("PARAM" ARRAY)
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
        # Creating copy of the Dataframe
        final_df = dataframe.select("DIARY_ITEM_KEY", "DATE_TIME", "START_TIME", "END_TIME", "DURATION","LATITUDE",\\
                                    "LONGITUDE","GOOGLE_MAPS_URL","RUN_ID","CREATE_DT")
 
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
CREATE OR REPLACE PROCEDURE PCF_PERENSO_DIARY_ITEM_TYPE_PREPROCESSING("PARAM" ARRAY)
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
        
        # Creating copy of the Dataframe
        final_df = dataframe.select("DIARY_ITEM_TYPE_KEY", "DIARY_ITEM_TYPE_DESC", "DSP_ORDER", "ACTIVE","CATEGORY",\\
                                    "COUNT_IN_CALL_RATE","RUN_ID","CREATE_DT")
 
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
CREATE OR REPLACE PROCEDURE PCF_PERENSO_HEAD_OFFICE_REQ_CHECK_PREPROCESSING("PARAM" ARRAY)
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
        
        # Creating copy of the Dataframe
        final_df = dataframe.select("STORE_CHK_HDR_KEY", "TODO_KEY", "PROD_GRP_KEY", "NOTES", "FAIL_REASON_KEY","FAIL_REASON_DESC",\\
                                    "RUN_ID","CREATE_DT")
 
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
CREATE OR REPLACE PROCEDURE PCF_PERENSO_HEAD_OFFICE_REQ_STATE_PREPROCESSING("PARAM" ARRAY)
RETURNS TABLE ()
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
 
        #Param=[''HeadOfficeReqState_20240320223707.csv'',''PCFSDL_RAW.DEV_LOAD_STAGE_ADLS'',''dev/ap_perenso/transation/headoffice/,''sdl_perenso_head_office_req_state'']
 
        # Extracting parameters from the input
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
 
 
        # Define the schema for the DataFrame
        df_schema=StructType([
            StructField("ACCT_KEY",IntegerType()),
            StructField("TODO_KEY",IntegerType()),
            StructField("PROD_GRP_KEY",IntegerType()),
            StructField("START_DATE",StringType()),
            StructField("END_DATE",StringType()),
            StructField("REQ_STATE",IntegerType()),
            StructField("STORE_CHK_HDR_KEY",IntegerType())
            
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
        # Creating copy of the Dataframe
        final_df = dataframe.select("ACCT_KEY", "TODO_KEY", "PROD_GRP_KEY", "START_DATE", "END_DATE","REQ_STATE",\\
                                    "STORE_CHK_HDR_KEY","RUN_ID","CREATE_DT")
 
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
CREATE OR REPLACE PROCEDURE PCF_PERENSO_METCASH_PREPROCESSING("PARAM" ARRAY)
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
	# CALL PCFSDL_RAW.PCF_PERENSO_METCASH_PREPROCESSING([''Monthly_Sales_Report,_Supplier_filter_only_J_J 202402.csv'',''PCFSDL_RAW.DEV_LOAD_STAGE_ADLS'',''dev/ap_perenso/transaction/metcash/grocery'',''sdl_metcash_ind_grocery''])
	
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
        StructField(''supp_id'' , StringType()),
        StructField(''supp_name'' , StringType()),
        StructField(''state'' , StringType()),
        StructField(''banner_id'' , StringType()),
        StructField(''banner'' , StringType()),
        StructField(''customer_id'' , StringType()),
        StructField(''customer'' , StringType()),
        StructField(''product_id'' , StringType()),
        StructField(''product'' , StringType()),
        StructField(''gross_sales_wk1'' , StringType()),
        StructField(''gross_sales_wk2'' , StringType()),
        StructField(''gross_sales_wk3'' , StringType()),
        StructField(''gross_sales_wk4'' , StringType()),
        StructField(''gross_sales_wk5'' , StringType()),
        StructField(''gross_cases_wk1'' , StringType()),
        StructField(''gross_cases_wk2'' , StringType()),
        StructField(''gross_cases_wk3'' , StringType()),
        StructField(''gross_cases_wk4'' , StringType()),
        StructField(''gross_cases_wk5'' , StringType())
            ])
        # Set the current session schema
        
        session.use_schema(stage_name.split(''.'')[0])

        
        
        stage_path="@{0}/{1}/{2}".format(stage_name,temp_stage_path,file_name)
        print(stage_path)
        
        temp_df = session.read\\
        .schema(df_schema)\\
        .option("skip_header",1)\\
        .option("field_delimiter", "\\u0001")\\
        .option("field_optionally_enclosed_by", "\\"") \\
        .csv(stage_path)
        
        temp_df=temp_df.na.drop("all")
        if temp_df.count()==0:
            return "No Data in file"

            
        temp_df = temp_df.withColumn("run_id",lit(lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S"))))
        temp_df = temp_df.withColumn("create_dt",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        temp_df = temp_df.withColumn("file_name",lit(file_name.replace("_"," ").replace(".csv",".xlsx")))
                    
        final_df=temp_df.select("supp_id","supp_name","state","banner_id","banner","customer_id","customer","product_id","product","gross_sales_wk1","gross_sales_wk2","gross_sales_wk3","gross_sales_wk4","gross_sales_wk5","gross_cases_wk1","gross_cases_wk2","gross_cases_wk3","gross_cases_wk4","gross_cases_wk5","run_id","file_name","create_dt")
                        


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
        return error_message';
CREATE OR REPLACE PROCEDURE PCF_PERENSO_ORDER_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
PACKAGES = ('openpyxl==3.0.10','snowflake-snowpark-python==*')
HANDLER = 'main'
COMMENT='One SP for 7 Tables'
EXECUTE AS OWNER
AS 'import snowflake.snowpark as snowpark
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField ,DecimalType
from snowflake.snowpark.functions import lit
from datetime import datetime
import pytz


def get_schema(file_name):
    df_schema=""
    select_col=""
    if file_name.startswith("OrderHdr"):
        df_schema=StructType([
        StructField(''ORDER_KEY'' , IntegerType()),
        StructField(''ORDER_TYPE_KEY'' , IntegerType()),
        StructField(''ACCT_KEY'' , IntegerType()),
        StructField(''ORDER_DATE'' , StringType()),
        StructField(''STATUS'' , IntegerType()),
        StructField(''CHARGE'' , StringType()),
        StructField(''CONFIRMATION'' , StringType()),
        StructField(''DIARY_ITEM_KEY'' , IntegerType()),
        StructField(''WORK_ITEM_KEY'' , IntegerType()),
        StructField(''ACCOUNT_ORDER_NO'' , StringType()),
        StructField(''DELVRY_INSTNS'' , StringType()),
            ])
        select_col=["ORDER_KEY","ORDER_TYPE_KEY","ACCT_KEY","ORDER_DATE","STATUS","CHARGE","CONFIRMATION","DIARY_ITEM_KEY","WORK_ITEM_KEY","ACCOUNT_ORDER_NO","DELVRY_INSTNS","RUN_ID","CREATE_DT"]
    if file_name.startswith("OrderType"):
        df_schema=StructType([
        StructField(''ORDER_TYPE_KEY'' , IntegerType()),
        StructField(''ORDER_TYPE_DESC'' , StringType()),
        StructField(''SOURCE'' , IntegerType())
            ])
        select_col=["ORDER_TYPE_KEY","ORDER_TYPE_DESC","SOURCE","run_id","create_dt"]
    if file_name.startswith("OrderBatch"):
        df_schema=StructType([
        StructField(''ORDER_KEY'' , IntegerType()),
        StructField(''BATCH_KEY'' , IntegerType()),
        StructField(''BRANCH_KEY'' , IntegerType()),
        StructField(''DIST_ACCT'' , StringType()),
        StructField(''DELVRY_DT'' , StringType()),
        StructField(''STATUS'' , IntegerType()),
        StructField(''SUFFIX'' , StringType()),
        StructField(''SENT_DT'' , IntegerType()),
            ])
        select_col=["ORD_KEY","BATCH_KEY","BRANCH_KEY","DIST_ACCT","DELVRY_DT","STATUS","SUFFIX","SENT_DT","RUN_ID","CREATE_DT"]
    if file_name.startswith("OrderDetail"):
        df_schema=StructType([
         StructField("ORDER_KEY", IntegerType()),
        StructField("BATCH_KEY", IntegerType()),
        StructField("LINE_KEY", IntegerType()),
        StructField("PROD_KEY", IntegerType()),
        StructField("UNIT_QTY", IntegerType()),
        StructField("ENTERED_QTY", IntegerType()),
        StructField("ENTERED_UNIT_KEY", IntegerType()),
        StructField("LIST_PRICE", IntegerType()),
        StructField("NIS", IntegerType()),
        StructField("RRP", IntegerType()),
        StructField("DISC_KEY", IntegerType()),
        StructField("CREDIT_LINE_KEY", IntegerType()),
        StructField("CREDITED", StringType())
            ])
        select_col=["ORDER_KEY","BATCH_KEY","LINE_KEY","PROD_KEY","UNIT_QTY","ENTERED_QTY","ENTERED_UNIT_KEY","LIST_PRICE","NIS","RRP","DISC_KEY","CREDIT_LINE_KEY","CREDITED"]
        
    if file_name.startswith("Constants"):
        df_schema=StructType([
            StructField("CONST_KEY", StringType()),
            StructField("CONST_DESC", StringType()),
            StructField("CONST_TYPE", StringType()),
            StructField("DSPORDER", StringType())
            ])
    
        select_col=["CONST_KEY", "CONST_DESC", "CONST_TYPE", "DSPORDER", "RUN_ID", "CREATE_DT"]
        
    if file_name.startswith("DealDiscount"):
        df_schema=StructType([
        StructField("DEAL_KEY", IntegerType()),
        StructField("DEAL_DESC", StringType()),
        StructField("START_DATE", StringType()),
        StructField("END_DATE", StringType()),
        StructField("SHORT_DESC", StringType()),
        StructField("DISC_KEY", IntegerType()),
        StructField("DISCOUNT_DESC", StringType())   
            ])
        select_col=["DEAL_KEY","DEAL_DESC","START_DATE","END_DATE","SHORT_DESC","DISC_KEY", "DISCOUNT_DESC", "RUN_ID", "CREATE_DT"]

def main(session: snowpark.Session,Param):
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
        
        df = session.read\\
        .schema(df_schema)\\
        .option("skip_header",1)\\
        .option("field_delimiter", ",")\\
        .csv(stage_path)
        
        df=df.na.drop("all")
        if temp_df.count()==0:
            return "No Data in file"

            
        df = df.withColumn("run_id",lit(lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S"))))
        df = df.withColumn("create_dt",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
            
        
                    
        final_df=df.select(select_col)
                        


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
        return error_message';
CREATE OR REPLACE PROCEDURE PCF_PERENSO_PRODUCT_PREPROCESSING("PARAM" ARRAY)
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
        select_col=["prod_grp_key","Prod_grp_lev_key","grp_desc","dsp_order","parent_key","run_id","create_dt"]
    if file_name.startswith("ProdGrpLevel"):
        df_schema=StructType([
        StructField(''Prod_grp_lev_key'' , IntegerType()),
        StructField(''prod_Lev_desc'' , StringType()),
        StructField(''prod_Lev_index'' , IntegerType()),
        StructField(''field_key'' , IntegerType())
            ])
        select_col=["Prod_grp_lev_key","prod_Lev_desc","prod_Lev_index","field_key","run_id","create_dt"]
        
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
        .csv(stage_path)
        
        temp_df=temp_df.na.drop("all")
        if temp_df.count()==0:
            return "No Data in file"

            
        temp_df = temp_df.withColumn("run_id",lit(lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S"))))
        temp_df = temp_df.withColumn("create_dt",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
            
        
                    
        final_df=temp_df.select(select_col)
                        


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
        return error_message';
CREATE OR REPLACE PROCEDURE PCF_PERENSO_RANGING_ACCT_GRP_REL_PREPROCESSING("PARAM" ARRAY)
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
        # Creating copy of the Dataframe
        final_df = dataframe.select("RANGING_KEY","ACCT_GRP_KEY","RUN_ID","CREATE_DT")
 
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
CREATE OR REPLACE PROCEDURE PCF_PERENSO_RANGING_PREPROCESSING("PARAM" ARRAY)
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
        # Creating copy of the Dataframe
        final_df = dataframe.select("RANGING_KEY", "RANGING_DESC", "ACCT_GRP_LEV_KEY", "ACTIVE", "ALL_ACCOUNTS","RUN_ID","CREATE_DT")
 
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
CREATE OR REPLACE PROCEDURE PCF_PERENSO_RANGING_PRODUCT_PREPROCESSING("PARAM" ARRAY)
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
        # Creating copy of the Dataframe
        final_df = dataframe.select("RANGING_KEY", "PROD_KEY", "ACCT_GRP_KEY", "CORE", "RANGE_RANK","RUN_ID","CREATE_DT")
 
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
CREATE OR REPLACE PROCEDURE PCF_PERENSO_STORE_CHK_HDR_PREPROCESSING("PARAM" ARRAY)
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
        # Creating copy of the Dataframe
        final_df = dataframe.select("STORE_CHK_HDR_KEY","DIARY_ITEM_KEY","ACCT_KEY","WORK_ITEM_KEY","STORE_CHK_DATE","RUN_ID","CREATE_DT")
 
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
CREATE OR REPLACE PROCEDURE PCF_PERENSO_SURVEYRESULT_PREPROCESSING("PARAM" ARRAY)
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
        temp_df = temp_df.withColumn("create_dt",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        
        
                    
        final_df=temp_df.select("store_chk_hdr_key","line_key","todo_key","prod_grp_key","optionans","notesans","run_id","create_dt")
                        


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
CREATE OR REPLACE PROCEDURE PCF_PERENSO_TODO_OPTION_PREPROCESSING("PARAM" ARRAY)
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
        temp_df = temp_df.withColumn("create_dt",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
            
        
                    
        final_df=temp_df.select("option_key","todo_key","option_desc","dsp_order","active","run_id","create_dt","cascade_next_todo_key","cascadeon_answermode")
                        


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
CREATE OR REPLACE PROCEDURE PCF_PERENSO_TODO_PREPROCESSING("PARAM" ARRAY)
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
 
        #Param=[Todo_20240320223707.csv'',''PCFSDL_RAW.DEV_LOAD_STAGE_ADLS'',''dev/ap_perenso/transation/headoffice/'',''sdl_perenso_todo'']
 
        # Extracting parameters from the input
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
 
 
        # Define the schema for the DataFrame
        df_schema=StructType([
            StructField("TODO_KEY",IntegerType()),
            StructField("TODO_TYPE",IntegerType()),
            StructField("TODO_DESC",IntegerType()),
            StructField("WORK_ITEM_KEY",StringType()),
            StructField("START_DATE",StringType()),
            StructField("END_DATE",IntegerType()),
            StructField("DSP_ORDER",IntegerType()),
            StructField("ANS_TYPE",IntegerType()),
            StructField("CASCADEON_ANSWERMODE",StringType()),
            StructField("CASCADE_TODO_KEY",StringType()),
            StructField("CASCADE_NEXT_TODO_KEY",IntegerType())  
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
        # Creating copy of the Dataframe

        dataframe = dataframe.with_column("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        dataframe = dataframe.with_column("CREATE_DT",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        
        final_df = dataframe.select("TODO_KEY", "TODO_TYPE", "TODO_DESC", "WORK_ITEM_KEY", "START_DATE","END_DATE",\\
                                    "RUN_ID","CREATE_DT","DSP_ORDER","ANS_TYPE","CASCADEON_ANSWERMODE","CASCADE_TODO_KEY","CASCADE_NEXT_TODO_KEY")                
 
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
CREATE OR REPLACE PROCEDURE PCF_PERENSO_USER_PREPROCESSING("PARAM" ARRAY)
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

            
        temp_df = temp_df.withColumn("run_id",lit(lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S"))))
        temp_df = temp_df.withColumn("create_dt",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
            
        
                    
        final_df=temp_df.select("user_key","display_name","user_type_key","user_type_desc","active","email_address","run_id","create_dt")
                        


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
CREATE OR REPLACE PROCEDURE PCF_PERENSO_WORK_ITEM_PREPROCESSING("PARAM" ARRAY)
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
        # Creating copy of the Dataframe"
        final_df = dataframe.select("WORK_ITEM_KEY", "WORK_ITEM_DESC", "WORK_ITEM_TYPE", "DIARY_ITEM_TYPE_KEY", "START_DATE","END_DATE",\\
                                    "RUN_ID","CREATE_DT")
 
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
CREATE OR REPLACE PROCEDURE PERENSO_ACCOUNT_FIELDS_PREPROCESSED("PARAM" ARRAY)
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
            
        df = df.withColumn("run_id", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        df = df.withColumn("create_dt", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))

        snowdf = df.select(
            "field_key",
            "field_desc",
            "field_type",
            "acct_type_key",
            "active",
            "run_id",
            "create_dt"
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
CREATE OR REPLACE PROCEDURE PERENSO_ACCOUNT_GROUP_LVL_PREPROCESSING("PARAM" ARRAY)
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
            
        df = df.withColumn("run_id", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        df = df.withColumn("create_dt", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))

        snowdf = df.select(
            "acct_grp_lev_key",
            "acct_lev_desc",
            "acct_lev_index",
            "field_key",
            "run_id",
            "create_dt"
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
CREATE OR REPLACE PROCEDURE PERENSO_ACCOUNT_GROUP_PREPROCESSING("PARAM" ARRAY)
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
        df = df.withColumn("create_dt", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))

        snowdf = df.select(
            "acct_grp_key",
            "acct_grp_lev_key",
            "grp_desc",
            "dsp_order",
            "parent_key",
            "run_id",
            "create_dt"
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
CREATE OR REPLACE PROCEDURE PERENSO_ACCOUNT_GROUP_RELN_PREPROCESSING("PARAM" ARRAY)
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

        snowdf = df.select(
            "acct_key",
            "acct_grp_key",
            "run_id",
            "create_dt"
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
CREATE OR REPLACE PROCEDURE PERENSO_ACCOUNT_RELN_ID_PREPROCESSING("PARAM" ARRAY)
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
            
        df = df.withColumn("run_id", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        df = df.withColumn("create_dt", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))

        snowdf = df.select(
            "acct_key",
            "field_key",
            "id",
            "run_id",
            "create_dt"
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
CREATE OR REPLACE PROCEDURE PERENSO_ACCOUNT_TYPE_PREPROCESSING("PARAM" ARRAY)
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

        snowdf = df.select(
            "acct_type_key",
            "acct_type_desc",
            "dsp_order",
            "active",
            "run_id",
            "create_dt"
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
CREATE OR REPLACE PROCEDURE PERENSO_ACCT_DIST_ACCT_PREPROCESSING("PARAM" ARRAY)
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
            
        df = df.withColumn("run_id", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        df = df.withColumn("create_dt", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))

        snowdf = df.select(
            "acct_key",
            "branch_key",
            "id",
            "system_primary",
            "active",
            "run_id",
            "create_dt"
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
CREATE OR REPLACE PROCEDURE PERENSO_DISTRIBUTOR_DETAIL_PREPROCESSING("PARAM" ARRAY)
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
        df = df.withColumn("create_dt", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))

        snowdf = df.select(
            "dist_key",
            "distributor",
            "dist_id",
            "branch_key",
            "display_name",
            "short_name",
            "active",
            "run_id",
            "create_dt"
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
CREATE OR REPLACE PROCEDURE SDL_SURVEY_PRODUCT_GRP_TO_CATEGORY_MAP("PARAM" ARRAY)
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
                StructField("perenso_questions", StringType(), True),
                
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
        #df = df.with_column("file_name", lit(file_name1))
        df = df.with_column("create_dt", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))

        snowdf= df.select(
                    "account_type_description",
                    "target_type",
                    "perenso_questions"
                    "run_id",
                    "create_id"
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
CREATE OR REPLACE PROCEDURE SDL_SURVEY_TARGETS("PARAM" ARRAY)
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
            .option("field_delimiter", "|")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

       
        
        df=df.na.drop("all")
        if df.count()==0 :
            return "No Data in file"

       
        df = df.with_column("run_id", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        #file_name1=file_name.split(".")[0]+"_"+ datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")+".csv"
        #df = df.with_column("file_name", lit(file_name1))
        df = df.with_column("create_dt", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))

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
                    "create_dt"
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
CREATE OR REPLACE PROCEDURE SDL_SURVEY_TYPE_TO_QUESTION_MAP("PARAM" ARRAY)
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
        #df = df.with_column("file_name", lit(file_name1))
        df = df.with_column("create_dt", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))

        snowdf= df.select(
                    "account_type_description",
                    "target_type",
                    "perenso_questions",
                    "run_id",
                    "create_dt"
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
CREATE OR REPLACE PROCEDURE SURVEY_PRODUCT_GRP_TO_CATEGORY_MAP("PARAM" ARRAY)
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
        #file_name1=file_name.split(".")[0]+"_"+ datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")+".csv"
        #df = df.with_column("file_name", lit(file_name1))
        df = df.with_column("create_dt", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))

        snowdf= df.select(
                    "category",
                    "question_product_group",
                    "run_id",
                    "create_dt"
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
CREATE OR REPLACE PROCEDURE SURVEY_TARGETS("PARAM" ARRAY)
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

def main(session: snowpark.Session, Param):
    #Param=["survey_targets.csv","PCFSDL_RAW.DEV_LOAD_STAGE_ADLS","dev/ap_perenso/transaction/user_files","sdl_survey_targets"]
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
            .option("field_delimiter", "|")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
            
            

       
        
        df=df.na.drop("all")
        if df.count()==0 :
            return "No Data in file"

       
        df = df.with_column("run_id", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        #file_name1=file_name.split(".")[0]+"_"+ datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")+".csv"
        #df = df.with_column("file_name", lit(file_name1))
        df = df.with_column("create_dt", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        

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
                    "create_dt"
                )
        
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
