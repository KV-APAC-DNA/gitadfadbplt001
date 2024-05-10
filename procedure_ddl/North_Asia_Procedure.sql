CREATE OR REPLACE PROCEDURE DEV_DNA_LOAD.NTASDL_RAW.KR_IL_DONG_HU_DI_S_DEOK_SEONG_SANG_SA_PREPROCESSING("PARAM" ARRAY)
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
            
        #Param=[''IL DONG HU DI S DEOK SEONG SANG SA.xls'',''NTASDL_RAW.DEV_LOAD_STAGE_ADLS_KOR'',''dev/coupang_premium/transaction'',''sdl_kr_il_dong_hu_di_s_deok_seong_sang_sa_gt_sellout'']

        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        # Define the schema for the DataFrame
        
        df_schema=StructType([
                        StructField("dstr_nm", StringType()),
                        StructField("ccode", StringType()),
                        StructField("sub_customer_name", StringType()),
                        StructField("gcode", StringType()),
                        StructField("on_site_name", StringType()),
                        StructField("year", StringType()),
                        StructField("ims_txn_dt", StringType()),
                        StructField("transaction_number", StringType()),
                        StructField("product_classification", StringType()),
                        StructField("product_code", StringType()),
                        StructField("management_code", StringType()),
                        StructField("ean", StringType()),
                        StructField("prize_name", StringType()),
                        StructField("classification", StringType()),
                        StructField("rules", StringType()),
                        StructField("color", StringType()),
                        StructField("delivery_date", StringType()),
                        StructField("deliver", StringType()),
                        StructField("factory_status", StringType()),
                        StructField("number_of_goods", StringType()),
                        StructField("BOX", StringType()),
                        StructField("one_piece", StringType()),
                        StructField("qty", StringType()),
                        StructField("weight", StringType()),
                        StructField("list", StringType()),
                        StructField("unit_price_rate", StringType()),
                        StructField("box_danga", StringType()),
                        StructField("unit_price", StringType()),
                        StructField("cust_cd", StringType())
                        ])
                
        dataframe = session.read.schema(df_schema) \\
        .option("skip_header",1) \\
        .option("field_delimiter","\\u0001") \\
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
        file_name=file_name.split(".")[0]+''_''+datetime.now().strftime("%Y%m%d%H%M%S")
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
