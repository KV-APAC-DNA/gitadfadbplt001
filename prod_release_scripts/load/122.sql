ALTER TABLE THASDL_RAW.SDL_TH_GT_VISIT
ALTER COLUMN OBJECT SET DATA TYPE VARCHAR(255);

ALTER TABLE THASDL_RAW.SDL_TH_SFMC_CONSUMER_MASTER_ADDITIONAL
ALTER COLUMN ATTRIBUTE_VALUE  SET DATA TYPE VARCHAR(500);

CREATE OR REPLACE PROCEDURE THASDL_RAW.TH_MBOX_CUSTOMER_PREPROCESSING("PARAM" ARRAY)
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
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField,DecimalType,DateType
import pandas as pd
from datetime import datetime
import pytz

def main(session: snowpark.Session,Param):

    
    try:

        # Parameters for consumerreach_cvs
        #Param=[''A1_SPC2403042250_20240305023320.txt'',''THASDL_RAW.DEV_LOAD_STAGE_ADLS'',''dev/GT_Intervention/DnA_VMR/cert-data-lake/CustomerDim/TH_GT_CUSTOMER/'',''sdl_th_dms_chana_customer_dim'']
        
        # Extracting parameters from the input
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]

        # Define the schema for the DataFrame
        df_schema = StructType([
            StructField("distributorid", StringType()),
            StructField("arcode", StringType()),
            StructField("arname", StringType()),
            StructField("araddress", StringType()),
            StructField("telephone", StringType()),
            StructField("fax", StringType()),
            StructField("city", StringType()),
            StructField("region", StringType()),
            StructField("saledistrict", StringType()),
            StructField("saleoffice", StringType()),
            StructField("salegroup", StringType()),
            StructField("artypecode", StringType()),
            StructField("saleemployee", StringType()),
            StructField("salename", StringType()),
            StructField("billno", StringType()),
            StructField("billmoo", StringType()),
            StructField("billsoi", StringType()),
            StructField("billroad", StringType()),
            StructField("billsubdist", StringType()),
            StructField("billdistrict", StringType()),
            StructField("billprovince", StringType()),
            StructField("billzipcode", StringType()),
            StructField("activestatus", StringType()),
            StructField("routestep1", StringType()),
            StructField("routestep2", StringType()),
            StructField("routestep3", StringType()),
            StructField("routestep4", StringType()),
            StructField("routestep5", StringType()),
            StructField("routestep6", StringType()),
            StructField("routestep7", StringType()),
            StructField("routestep8", StringType()),
            StructField("routestep9", StringType()),
            StructField("routestep10", StringType()),
            StructField("store", StringType()),
            StructField("pricelevel", StringType()),
            StructField("salesarename", StringType()),
            StructField("branchcode", StringType()),
            StructField("branchname", StringType()),
            StructField("frequencyofvisit", StringType())
            ])

        
        # Read the CSV file into a DataFrame
    
        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",0)\\
            .option("field_delimiter", "\\t")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)


        #---------------------------Transformation logic ------------------------------#

        # Check if the Dataframe is having Data
        df = df.na.drop("all")
        
        if df.count()==0:
            return "No Data in file"
		
		
        # Add RUN_ID, FILE NAME and YEARMO columns  
        df= df.with_column("run_id", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S"))) 
        df=df.with_column("filename",lit(file_name))
        df=df.with_column("crt_dttm",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))

        

        
        # Creating Final Dataframe
        final_df = df.select("distributorid","arcode","arname","araddress","telephone","fax","city","region","saledistrict","saleoffice","salegroup","artypecode","saleemployee","salename","billno","billmoo","billsoi","billroad","billsubdist","billdistrict","billprovince","billzipcode","activestatus","routestep1","routestep2","routestep3","routestep4","routestep5","routestep6","routestep7","routestep8","routestep9","routestep10","store","pricelevel","salesarename","branchcode","branchname","frequencyofvisit","filename","run_id","crt_dttm")


        #Delete existing Data for the current file
        
        del_sql = "DELETE FROM " + target_table + " WHERE filename ="+"''" + (file_name)+"''"
        session.sql(del_sql).collect()
        
        # Load Data to the target table
        final_df.write.mode("append").saveAsTable(stage_name.split(".")[0]+"."+target_table)

        # write to success folder
    
        file_name=file_name.split(".")[0]+''_''+datetime.now().strftime("%Y%m%d%H%M%S")
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        
        #move to success
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
