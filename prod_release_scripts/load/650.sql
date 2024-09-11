USE ScHEMA META_RAW;

INSERT INTO PROCESS VALUES (5060,5060,'my_perfectstore_sku_mst',5060,1,1,FALSE,TRUE,341,1,1,NULL,'','','','MYSSDL_RAW.PROD_LOAD_STAGE_ADLS','','','');
INSERT INTO PROCESS VALUES (5061,5061,'my_perfectstore_outlet_mst',5061,1,1,FALSE,TRUE,341,1,1,NULL,'','','','MYSSDL_RAW.PROD_LOAD_STAGE_ADLS','','','');
INSERT INTO PROCESS VALUES (5062,5062,'my_perfectstore_osa',5062,1,1,FALSE,TRUE,341,1,1,NULL,'','','','MYSSDL_RAW.PROD_LOAD_STAGE_ADLS','','','');
INSERT INTO PROCESS VALUES (5063,5063,'my_perfectstore_oos',5063,1,1,FALSE,TRUE,341,1,1,NULL,'','','','MYSSDL_RAW.PROD_LOAD_STAGE_ADLS','','','');
INSERT INTO PROCESS VALUES (5064,5064,'my_perfectstore_sos',5064,1,1,FALSE,TRUE,341,1,1,NULL,'','','','MYSSDL_RAW.PROD_LOAD_STAGE_ADLS','','','');
INSERT INTO PROCESS VALUES (5065,5065,'my_perfectstore_promocomp',5065,1,1,FALSE,TRUE,341,1,1,NULL,'','','','MYSSDL_RAW.PROD_LOAD_STAGE_ADLS','','','');

INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (22322,5062,'my_perfectstore_osa_group','source_extn','csv',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (22323,5062,'my_perfectstore_osa_group','file_header_row_num','0',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (22324,5062,'my_perfectstore_osa_group','trigger_mail','N',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (22325,5062,'my_perfectstore_osa_group','business_mail_trigger','N',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (22326,5062,'my_perfectstore_osa_group','val_file_header','Date,Channel,Chain,Region,Outlet,Outlet_No,Category,Brand,Sub_Category,Sub_Brand,Packsize,Barcode,SKU_Description,Answer',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (22327,5062,'my_perfectstore_osa_group','file_spec','OSA',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (22328,5062,'my_perfectstore_osa_group','val_file_name','OSA',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (22329,5062,'my_perfectstore_osa_group','val_file_extn','csv',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (22330,5062,'my_perfectstore_osa_group','load_method','sp',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (22331,5062,'my_perfectstore_osa_group','sp_name','MYSSDL_RAW.MY_PERFECTSTORE_OOS_OSA_PREPROCESSING',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (22332,5062,'my_perfectstore_osa_group','is_direct_load','N',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (22333,5062,'my_perfectstore_osa_group','folder_path','inStoreExecution/transaction',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (22334,5062,'my_perfectstore_osa_group','target_table','sdl_my_perfectstore_osa',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (22335,5062,'my_perfectstore_osa_group','container','mys',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (22336,5062,'my_perfectstore_osa_group','target_schema','MYSSDL_RAW',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (22337,5062,'my_perfectstore_osa_group','validation','1-1-1',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (22338,5062,'my_perfectstore_osa_group','index','full',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (22339,5062,'my_perfectstore_osa_group','is_truncate','Y',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (22340,5063,'my_perfectstore_oos_group','source_extn','csv',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (22341,5063,'my_perfectstore_oos_group','file_header_row_num','0',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (22342,5063,'my_perfectstore_oos_group','trigger_mail','N',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (22343,5063,'my_perfectstore_oos_group','business_mail_trigger','N',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (22344,5063,'my_perfectstore_oos_group','val_file_header','Date,Channel,Chain,Region,Outlet,Outlet_No,Category,Brand,Sub_Category,Sub_Brand,Packsize,Product_Barcode,SKU_Description,Answer',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (22345,5063,'my_perfectstore_oos_group','file_spec','OOS',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (22346,5063,'my_perfectstore_oos_group','val_file_name','OOS',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (22347,5063,'my_perfectstore_oos_group','val_file_extn','csv',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (22348,5063,'my_perfectstore_oos_group','load_method','sp',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (22349,5063,'my_perfectstore_oos_group','sp_name','MYSSDL_RAW.MY_PERFECTSTORE_OOS_OSA_PREPROCESSING',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (22350,5063,'my_perfectstore_oos_group','is_direct_load','N',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (22351,5063,'my_perfectstore_oos_group','folder_path','inStoreExecution/transaction',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (22352,5063,'my_perfectstore_oos_group','target_table','sdl_my_perfectstore_oos',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (22353,5063,'my_perfectstore_oos_group','container','mys',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (22354,5063,'my_perfectstore_oos_group','target_schema','MYSSDL_RAW',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (22355,5063,'my_perfectstore_oos_group','validation','1-1-1',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (22356,5063,'my_perfectstore_oos_group','index','full',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (22357,5063,'my_perfectstore_oos_group','is_truncate','Y',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (22358,5064,'my_perfectstore_sos_group','source_extn','csv',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (22359,5064,'my_perfectstore_sos_group','file_header_row_num','0',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (22360,5064,'my_perfectstore_sos_group','trigger_mail','N',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (22361,5064,'my_perfectstore_sos_group','business_mail_trigger','N',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (22362,5064,'my_perfectstore_sos_group','val_file_header','Date,Channel,Chain,Region,Outlet,Outlet_No,Category,Brand,Sub_Category,Sub_Brand,Packsize,SKU_Description,Answer',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (22363,5064,'my_perfectstore_sos_group','file_spec','SOS',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (22364,5064,'my_perfectstore_sos_group','val_file_name','SOS',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (22365,5064,'my_perfectstore_sos_group','val_file_extn','csv',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (22366,5064,'my_perfectstore_sos_group','load_method','sp',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (22367,5064,'my_perfectstore_sos_group','sp_name','MYSSDL_RAW.MY_PERFECTSTORE_SOS_PREPROCESSING',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (22368,5064,'my_perfectstore_sos_group','is_direct_load','N',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (22369,5064,'my_perfectstore_sos_group','folder_path','inStoreExecution/transaction',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (22370,5064,'my_perfectstore_sos_group','target_table','sdl_my_perfectstore_sos',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (22371,5064,'my_perfectstore_sos_group','container','mys',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (22372,5064,'my_perfectstore_sos_group','target_schema','MYSSDL_RAW',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (22373,5064,'my_perfectstore_sos_group','validation','1-1-1',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (22374,5064,'my_perfectstore_sos_group','index','full',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (22375,5064,'my_perfectstore_sos_group','is_truncate','Y',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (22376,5065,'my_perfectstore_promocomp_group','source_extn','csv',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (22377,5065,'my_perfectstore_promocomp_group','file_header_row_num','0',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (22378,5065,'my_perfectstore_promocomp_group','trigger_mail','N',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (22379,5065,'my_perfectstore_promocomp_group','business_mail_trigger','N',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (22380,5065,'my_perfectstore_promocomp_group','val_file_header','Date,Channel,Chain,Region,Outlet,Outlet_No,Category,Brand,Activation,Promo_Comp_On_Time,Promo_Comp_On_Time_In_Full,Promo_Comp_Successfully_Set_Up,Non_Compliance_Reason',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (22381,5065,'my_perfectstore_promocomp_group','file_spec','PROMOCOMP',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (22382,5065,'my_perfectstore_promocomp_group','val_file_name','PROMOCOMP',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (22383,5065,'my_perfectstore_promocomp_group','val_file_extn','csv',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (22384,5065,'my_perfectstore_promocomp_group','load_method','sp',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (22385,5065,'my_perfectstore_promocomp_group','sp_name','MYSSDL_RAW.MY_PERFECTSTORE_PROMOCOMP_PREPROCESSING',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (22386,5065,'my_perfectstore_promocomp_group','is_direct_load','N',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (22387,5065,'my_perfectstore_promocomp_group','folder_path','inStoreExecution/transaction',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (22388,5065,'my_perfectstore_promocomp_group','target_table','sdl_my_perfectstore_promocomp',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (22389,5065,'my_perfectstore_promocomp_group','container','mys',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (22390,5065,'my_perfectstore_promocomp_group','target_schema','MYSSDL_RAW',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (22391,5065,'my_perfectstore_promocomp_group','validation','1-1-1',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (22392,5065,'my_perfectstore_promocomp_group','index','full',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (22393,5065,'my_perfectstore_promocomp_group','is_truncate','Y',FALSE,TRUE);


CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.MYSSDL_RAW.MY_PERFECTSTORE_OOS_OSA_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '
# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them.
 
import snowflake.snowpark as snowpark
from snowflake import snowpark
from snowflake.snowpark.functions import col,lit,when,to_date,regexp_replace,cast,right
from snowflake.snowpark.types import StringType, StructType, StructField
import pandas as pd
from datetime import datetime
import pytz
 
def main(session: snowpark.Session,Param):
 
    try:
 
      #  Param=[''OSA_072024.csv'',''MYSSDL_RAW.DEV_LOAD_STAGE_ADLS'',''dev/inStoreExecution/transaction'',''sdl_my_perfectstore_osa'']
 
        # Extracting parameters from the input
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
 
 
        # Define the schema for the DataFrame
        df_schema=StructType([
            StructField("DATE", StringType()),
            StructField("CHANNEL", StringType()), 
            StructField("CHAIN", StringType()), 
            StructField("REGION", StringType()), 
            StructField("OUTLET", StringType()), 
            StructField("OUTLET_NO", StringType()), 
            StructField("CATEGORY", StringType()), 
            StructField("BRAND", StringType()), 
            StructField("SUB_CATEGORY", StringType()), 
            StructField("SUB_BRAND", StringType()), 
            StructField("PACKSIZE", StringType()), 
            StructField("PRODUCT_BARCODE", StringType()), 
            StructField("SKU_DESCRIPTION", StringType()), 
            StructField("ANSWER", StringType())
            
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
        #return dataframe
 
        # Handle null values or empty rows
        dataframe=dataframe.na.drop("all")
 
         # Check for empty Dataframe
        if dataframe.count()==0:
            return "No Data in file"


        dataframe = dataframe.with_column("DATE",
            when(
                cast(regexp_replace(right(col("Date"), 4), "/", "."), "numeric") > 1999,
                to_date(col("Date"), ''DD/MM/YYYY'')
            ).otherwise(
                to_date(col("Date"), ''DD/MM/YY'')
            )
        )
            
        dataframe = dataframe.with_column("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        dataframe = dataframe.with_column("FILE_NAME",lit(file_name))
        yearmo = file_name.split(".")[0].split("_")[-1]
        dataframe = dataframe.withColumn("YEARMO",lit(yearmo).cast("string"))
        # Creating copy of the Dataframe
        final_df = dataframe.select("DATE","CHANNEL", "CHAIN", "REGION", "OUTLET", "OUTLET_NO", "CATEGORY", "BRAND", "SUB_CATEGORY", "SUB_BRAND", "PACKSIZE", "PRODUCT_BARCODE", "SKU_DESCRIPTION", "ANSWER", "RUN_ID","FILE_NAME", "YEARMO")
 
         # Load Data to the target table
        final_df.write.mode("append").saveAsTable(target_table)
 
         
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
 
         # write to success folder
        file_name=file_name.split(".")[0]+"_"+ datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        final_df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)
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

CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.MYSSDL_RAW.MY_PERFECTSTORE_OUTLET_MST_PREPROCESSING("PARAM" ARRAY)
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
import snowflake.snowpark as snowpark
import pytz

def main(session: snowpark.Session,Param):
    # Param=[''OUTLET_MST.csv'',''MYSSDL_RAW.DEV_LOAD_STAGE_ADLS'',''dev/inStoreExecution/transaction'',''sdl_my_perfectstore_outlet_mst'']
    try:
        file_name = Param[0]
        stage_name = Param[1]
        temp_stage_path = Param[2]
        target_table = Param[3]
        
        df_schema = StructType([
            StructField("outlet_no", StringType(), True),
            StructField("name", StringType(), True),     
            StructField("zone_no", StringType(), True),
            StructField("chain_no", StringType(), True),
            StructField("channel_no", StringType(), True),
            StructField("address", StringType(), True),
            StructField("postcode", StringType(), True),
            StructField("latitude", StringType(), True),
            StructField("longitude", StringType(), True),     
        ])
        
        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
        
        df = df.na.drop("all")
        
        if df.count() == 0:
            return "No Data in file"

        df = df.withColumn("run_id", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        df = df.withColumn("filename", lit(file_name))


        snowdf = df.select(
           "outlet_no", 
           "name",      
           "zone_no", 
           "chain_no", 
           "channel_no", 
           "address", 
           "postcode", 
           "latitude", 
           "longitude",      
            "run_id", 
            "filename"
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

CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.MYSSDL_RAW.MY_PERFECTSTORE_PROMOCOMP_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '
import snowflake.snowpark as snowpark
from snowflake import snowpark
from snowflake.snowpark.functions import col,lit,when,to_date,regexp_replace,cast,right
from snowflake.snowpark.types import StringType, StructType, StructField
import pandas as pd
from datetime import datetime
import pytz
 
def main(session: snowpark.Session,Param):
 
    try:
 
        # Param=[''PROMOCOMP_072024.csv'',''MYSSDL_RAW.DEV_LOAD_STAGE_ADLS'',''dev/inStoreExecution/transaction'',''sdl_my_perfectstore_promocomp'']
 
        # Extracting parameters from the input
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
 
 
        # Define the schema for the DataFrame
        df_schema=StructType([
            StructField("DATE", StringType()),
            StructField("CHANNEL", StringType()), 
            StructField("CHAIN", StringType()), 
            StructField("REGION", StringType()), 
            StructField("OUTLET", StringType()), 
            StructField("OUTLET_NO", StringType()), 
            StructField("CATEGORY", StringType()), 
            StructField("BRAND", StringType()), 
            StructField("ACTIVATION", StringType()), 
            StructField("PROMO_COMP_ON_TIME", StringType()), 
            StructField("PROMO_COMP_ON_TIME_IN_FULL", StringType()), 
            StructField("PROMO_COMP_SUCCESSFULLY_SET_UP", StringType()), 
            StructField("NON_COMPLIANCE_REASON", StringType())
            
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
        #return dataframe
 
        # Handle null values or empty rows
        dataframe=dataframe.na.drop("all")
 
         # Check for empty Dataframe
        if dataframe.count()==0:
            return "No Data in file"


        dataframe = dataframe.with_column("DATE",
            when(
                cast(regexp_replace(right(col("Date"), 4), "/", "."), "numeric") > 1999,
                to_date(col("Date"), ''DD/MM/YYYY'')
            ).otherwise(
                to_date(col("Date"), ''DD/MM/YY'')
            )
        )
            
        dataframe = dataframe.with_column("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        dataframe = dataframe.with_column("FILE_NAME",lit(file_name))
        yearmo = file_name.split(".")[0].split("_")[-1]
        dataframe = dataframe.withColumn("YEARMO",lit(yearmo).cast("string"))
        # Creating copy of the Dataframe
        final_df = dataframe.select("DATE","CHANNEL", "CHAIN", "REGION", "OUTLET", "OUTLET_NO", "CATEGORY", "BRAND", "ACTIVATION", "PROMO_COMP_ON_TIME", "PROMO_COMP_ON_TIME_IN_FULL", "PROMO_COMP_SUCCESSFULLY_SET_UP", "NON_COMPLIANCE_REASON", "RUN_ID","FILE_NAME", "YEARMO")
 
        # # Load Data to the target table
        final_df.write.mode("append").saveAsTable(target_table)
 
         
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
 
         # write to success folder
        file_name=file_name.split(".")[0]+"_"+ datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        final_df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)
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

CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.MYSSDL_RAW.MY_PERFECTSTORE_SKU_MST_PREPROCESSING("PARAM" ARRAY)
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
import snowflake.snowpark as snowpark
import pytz

def main(session: snowpark.Session,Param):
    # Param=[''SKU_MST.csv'',''MYSSDL_RAW.DEV_LOAD_STAGE_ADLS'',''dev/inStoreExecution/transaction'',''sdl_my_perfectstore_sku_mst'']
    try:
        file_name = Param[0]
        stage_name = Param[1]
        temp_stage_path = Param[2]
        target_table = Param[3]
        
        df_schema = StructType([
            StructField("sku_no", StringType(), True),
            StructField("description", StringType(), True), 
            StructField("client", StringType(), True),
            StructField("manufacture", StringType(), True),
            StructField("category", StringType(), True),
            StructField("brand", StringType(), True),
            StructField("sub_catgory", StringType(), True),
            StructField("sub_brand", StringType(), True),
            StructField("packsize", StringType(), True),     
            StructField("other", StringType(), True),
            StructField("product_barcode", StringType(), True),
            StructField("list_price_fib", StringType(), True),
            StructField("list_price_unit", StringType(), True),
            StructField("rcp", StringType(), True),
            StructField("packing_config", StringType(), True),
        ])
        
        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
        
        df = df.na.drop("all")
        
        if df.count() == 0:
            return "No Data in file"

        df = df.withColumn("run_id", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        df = df.withColumn("filename", lit(file_name))


        snowdf = df.select(
            "sku_no",
           "description", 
           "client",
           "manufacture",
           "category",
           "brand",
           "sub_catgory",
           "sub_brand",
           "packsize",     
           "other",
           "product_barcode",
           "list_price_fib",
           "list_price_unit",
           "rcp",
           "packing_config",
            "run_id", 
            "filename"            
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

CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.MYSSDL_RAW.MY_PERFECTSTORE_SOS_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '
# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them.
 
import snowflake.snowpark as snowpark
from snowflake import snowpark
from snowflake.snowpark.functions import col,lit,when,to_date,regexp_replace,cast,right
from snowflake.snowpark.types import StringType, StructType, StructField
import pandas as pd
from datetime import datetime
import pytz
 
def main(session: snowpark.Session,Param):
 
    try:
 
       # Param=[''SOS_072024.csv'',''MYSSDL_RAW.DEV_LOAD_STAGE_ADLS'',''dev/inStoreExecution/transaction'',''sdl_my_perfectstore_sos'']
 
        # Extracting parameters from the input
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
 
 
        # Define the schema for the DataFrame
        df_schema=StructType([
            StructField("DATE", StringType()),
            StructField("CHANNEL", StringType()), 
            StructField("CHAIN", StringType()), 
            StructField("REGION", StringType()), 
            StructField("OUTLET", StringType()), 
            StructField("OUTLET_NO", StringType()), 
            StructField("CATEGORY", StringType()), 
            StructField("BRAND", StringType()), 
            StructField("SUB_CATEGORY", StringType()), 
            StructField("SUB_BRAND", StringType()), 
            StructField("PACKSIZE", StringType()), 
            StructField("SKU_DESCRIPTION", StringType()), 
            StructField("ANSWER", StringType())
            
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
        #return dataframe
 
        # Handle null values or empty rows
        dataframe=dataframe.na.drop("all")
 
         # Check for empty Dataframe
        if dataframe.count()==0:
            return "No Data in file"


        dataframe = dataframe.with_column("DATE",
            when(
                cast(regexp_replace(right(col("Date"), 4), "/", "."), "numeric") > 1999,
                to_date(col("Date"), ''DD/MM/YYYY'')
            ).otherwise(
                to_date(col("Date"), ''DD/MM/YY'')
            )
        )
            
        dataframe = dataframe.with_column("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        dataframe = dataframe.with_column("FILE_NAME",lit(file_name))
        yearmo = file_name.split(".")[0].split("_")[-1]
        dataframe = dataframe.withColumn("YEARMO",lit(yearmo).cast("string"))
        # Creating copy of the Dataframe
        final_df = dataframe.select("DATE","CHANNEL", "CHAIN", "REGION", "OUTLET", "OUTLET_NO", "CATEGORY", "BRAND", "SUB_CATEGORY", "SUB_BRAND", "PACKSIZE",  "SKU_DESCRIPTION", "ANSWER", "RUN_ID","FILE_NAME", "YEARMO")
 
        # # Load Data to the target table
        final_df.write.mode("append").saveAsTable(target_table)
 
         
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
 
         # write to success folder
        file_name=file_name.split(".")[0]+"_"+ datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        final_df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)
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
