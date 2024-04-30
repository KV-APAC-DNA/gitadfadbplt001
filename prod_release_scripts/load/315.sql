delete from meta_raw.process
where parameter_group_id = 519;

delete from meta_raw.parameters
where parameter_group_id = 519;

INSERT INTO meta_raw.PROCESS VALUES (519,519,'J_Pac_PX_Master_weekly_sellin',519,1,2,FALSE,TRUE,142,5,1,NULL,'','','','PCFSDL_RAW.PROD_LOAD_STAGE_ADLS','','');
INSERT INTO META_RAW.PROCESS VALUES (586,586,'J_Pac_ProMAX_weekly_sellin_file_system',586,1,1,FALSE,TRUE,142,7,1,null,'','','','','','');

INSERT INTO meta_raw.PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (7069,586,'J_Pac_ProMAX_weekly_sellin_file_system','filename','PromotionWeeklySell.csv',FALSE,TRUE);
INSERT INTO meta_raw.PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (7070,586,'J_Pac_ProMAX_weekly_sellin_file_system','adls_path','file_system/PromotionWeeklySell',FALSE,TRUE);
INSERT INTO meta_raw.PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (7071,586,'J_Pac_ProMAX_weekly_sellin_file_system','container','pac',FALSE,TRUE);
INSERT INTO meta_raw.PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (7072,586,'J_Pac_ProMAX_weekly_sellin_file_system','filesystemName','sawswfgsgpw0002',FALSE,TRUE);
INSERT INTO meta_raw.PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (6757,519,'J_Pac_ProMAX_weekly_sellin','trigger_mail','Y',FALSE,TRUE);
INSERT INTO meta_raw.PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (6758,519,'J_Pac_ProMAX_weekly_sellin','business_mail_trigger','Y',FALSE,TRUE);
INSERT INTO meta_raw.PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (6759,519,'J_Pac_ProMAX_weekly_sellin','val_file_header','PromotionRowId|PromotionNumber|TransactionLongName|TransactionRowId|Account|AccountNumber|Activity|Material|MaterialLongName|MaterialProfitCentre|PromotionForecastWeek|CommittedAmount|plannedAmount|PaidAmount|WritebackAmount|BalanceAmount|Quantity|PromotionStatus',FALSE,TRUE);
INSERT INTO meta_raw.PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (6760,519,'J_Pac_ProMAX_weekly_sellin','file_spec','PromotionWeeklySell',FALSE,TRUE);
INSERT INTO meta_raw.PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (6761,519,'J_Pac_ProMAX_weekly_sellin','val_file_name','PromotionWeeklySell',FALSE,TRUE);
INSERT INTO meta_raw.PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (6762,519,'J_Pac_ProMAX_weekly_sellin','val_file_extn','csv',FALSE,TRUE);
INSERT INTO meta_raw.PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (6763,519,'J_Pac_ProMAX_weekly_sellin','load_method','sp',FALSE,TRUE);
INSERT INTO meta_raw.PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (6764,519,'J_Pac_ProMAX_weekly_sellin','sp_name','PCFSDL_RAW.PromotionWeeklySell_PREPROCESSING',FALSE,TRUE);
INSERT INTO meta_raw.PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (6765,519,'J_Pac_ProMAX_weekly_sellin','sheet_index','0',FALSE,TRUE);
INSERT INTO meta_raw.PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (6766,519,'J_Pac_ProMAX_weekly_sellin','folder_path','file_system/PromotionWeeklySell',FALSE,TRUE);
INSERT INTO meta_raw.PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (6767,519,'J_Pac_ProMAX_weekly_sellin','target_table','sdl_px_weekly_sell',FALSE,TRUE);
INSERT INTO meta_raw.PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (6768,519,'J_Pac_ProMAX_weekly_sellin','container','pac',FALSE,TRUE);
INSERT INTO meta_raw.PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (6769,519,'J_Pac_ProMAX_weekly_sellin','target_schema','PCFSDL_RAW',FALSE,TRUE);
INSERT INTO meta_raw.PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (6770,519,'J_Pac_ProMAX_weekly_sellin','validation','1-1-0',FALSE,TRUE);
INSERT INTO meta_raw.PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (6771,519,'J_Pac_ProMAX_weekly_sellin','index','last',FALSE,TRUE);
INSERT INTO meta_raw.PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (6772,519,'J_Pac_ProMAX_weekly_sellin','source_extn','csv',FALSE,TRUE);
INSERT INTO meta_raw.PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (6773,519,'J_Pac_ProMAX_weekly_sellin','file_header_row_num','0',FALSE,TRUE);
INSERT INTO meta_raw.PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (6774,519,'J_Pac_ProMAX_weekly_sellin','is_truncate','Y',FALSE,TRUE);

INSERT INTO META_RAW.SOURCE (SOURCE_ID, SOURCE_NAME,SOURCE_TYPE,FREQUENCY) VALUES (7,'File_System','File_System','');


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
