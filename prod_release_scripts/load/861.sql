 create or replace TABLE PROD_DNA_LOAD.PHLSDL_RAW.SDL_PH_SFMC_CLICK_DATA (
 OYB_ACCOUNT_ID VARCHAR(50),
 JOB_ID VARCHAR(50),
 LIST_ID VARCHAR(50),
 BATCH_ID VARCHAR(50),
 SUBSCRIBER_ID VARCHAR(50),
 SUBSCRIBER_KEY VARCHAR(100),
 EVENT_DATE TIMESTAMP_NTZ(9),
 DOMAIN VARCHAR(50),
 URL VARCHAR(1000),
 LINK_NAME VARCHAR(1000),
 LINK_CONTENT VARCHAR(1000),
 IS_UNIQUE VARCHAR(10),
 EMAIL_NAME VARCHAR(100),
 EMAIL_SUBJECT VARCHAR(200),
 FILE_NAME VARCHAR(255),
 CRTD_DTTM TIMESTAMP_NTZ(9) DEFAULT CAST('CURRENT_TIMESTAMP()' AS TIMESTAMP_NTZ(9))
);

CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.PHLSDL_RAW.PH_POS_DYNA_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS 'from snowflake.snowpark.functions import col, lit, date_format, current_timestamp, to_date, year, month, concat, format_number, regexp_replace,to_timestamp,when,trim,upper, substring
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DecimalType
import snowflake.snowpark
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz
import pandas as pd
def main(session: snowpark.Session,Param):
    # Param=["DYNA_DYO_202311.csv","PHLSDL_RAW.DEV_LOAD_STAGE_ADLS","dev/pos/dyna/transaction_data","SDL_PH_POS_DYNA_SALES"]
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        
        month_id = file_name.split(".")[0].split("_")[2]
        month_str = datetime.strptime(month_id,"%Y%m").strftime("%b").upper() + month_id[:4]
        
        month_qty_column = f"q{month_str}"
        month_sls_column = f"s{month_str}"
    
        df_0 = session.read.option("field_delimiter", "\\u0001").option("parse_header",True).csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
        
        df_1 = df_0.to_pandas()
        
        df_1.rename(columns = {
                df_1.columns[0]: ''sls_area'',
                df_1.columns[1]: ''plant'',
                df_1.columns[2]: ''customer_id'',
                df_1.columns[3]: ''old_cust_id'',
                df_1.columns[4]: ''cust_nm'',
                df_1.columns[5]: ''chnl'',
                df_1.columns[6]: ''sls_off'',
                df_1.columns[7]: ''sls_grp'',
                df_1.columns[8]: ''address'',
                df_1.columns[9]: ''city'',
                df_1.columns[10]: ''postal_cd'',
                df_1.columns[11]: ''dsm'',
                df_1.columns[12]: ''sman'',
                df_1.columns[13]: ''prin'',
                df_1.columns[14]: ''principal'',
                df_1.columns[15]: ''matl_grp'',
                df_1.columns[16]: ''matl_sub_grp'',
                df_1.columns[17]: ''brand'',
                df_1.columns[18]: ''uom_conv'',
                df_1.columns[19]: ''matl_num'',
                df_1.columns[20]: ''old_matl_num'',
                df_1.columns[21]: ''matl_desc'',
                month_qty_column: ''qty'',
                month_sls_column: ''sls_amt''   
            },inplace=True)
    # selected_columns = df_1.iloc[:,:21]
        result_df=df_1[[''sls_area'',
                    ''plant'',
                    ''customer_id'',
                    ''old_cust_id'',
                    ''cust_nm'',
                    ''chnl'',
                    ''sls_off'',
                    ''sls_grp'',
                    ''address'',
                    ''city'',
                    ''postal_cd'',
                    ''dsm'',
                    ''sman'',
                    ''prin'',
                    ''principal'',
                    ''matl_grp'',
                    ''matl_sub_grp'',
                    ''brand'',
                    ''uom_conv'',
                    ''matl_num'',
                    ''old_matl_num'',
                    ''matl_desc'',
                    ''qty'',
                    ''sls_amt'']]
    
    
        df = session.create_dataframe(result_df)
        
        #df = df.with_column("cdl_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        
        
        
        df = df.with_column("crtd_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        extrctd_month_id=month_id
        df = df.with_column("mnth_id", lit(extrctd_month_id))
        df = df.with_column("FILE_NAME", lit(file_name.split(".")[0]+".xlsx"))
        
        
        
        snowdf= df.select(
                    ''MNTH_ID'',                 
                    ''"sls_area"'',
                    ''"plant"'',
                    ''"customer_id"'',
                    ''"old_cust_id"'',
                    ''"cust_nm"'',
                    ''"chnl"'',
                    ''"sls_off"'',
                    ''"sls_grp"'',
                    ''"address"'',
                    ''"city"'',
                    ''"postal_cd"'',
                    ''"dsm"'',
                    ''"sman"'',
                    ''"prin"'',
                    ''"principal"'',
                    ''"matl_grp"'',
                    ''"matl_sub_grp"'',
                    ''"brand"'',
                    ''"uom_conv"'',
                    ''"matl_num"'',
                    ''"old_matl_num"'',
                    ''"matl_desc"'',
                    ''"qty"'',
                    ''"sls_amt"'',
                    ''FILE_NAME''
                    )
                            
        file_name=file_name.split(".")[0]+"_"+datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        
        
        #move to success
        
        snowdf.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=False,OVERWRITE=True)
        
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
 
 
 
