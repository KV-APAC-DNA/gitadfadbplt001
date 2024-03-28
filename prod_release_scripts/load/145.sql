CREATE OR REPLACE PROCEDURE THASDL_RAW.TH_GT_VISIT_PREPROCESSING("PARAM" ARRAY)
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
from snowflake.snowpark.functions import col,lit,to_date,trim,date_format
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField,DecimalType,DateType
import pandas as pd
from datetime import datetime
import pytz

def main(session: snowpark.Session,Param):

    
    try:

        # Parameters for consumerreach_cvs
        #Param=[''Visit_20240306223006_20240307013015.txt'',''THASDL_RAW.DEV_LOAD_STAGE_ADLS'',''dev/GT_Intervention/DnA_VMR/cert-data-lake/Visit/TH_GT_VISIT'',''SDL_TH_GT_VISIT'']
        
        # Extracting parameters from the input
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]

        # Define the schema for the DataFrame
        df_schema = StructType([
            StructField("id_sale", StringType()),
            StructField("sale_name", StringType()),
            StructField("id_customer", StringType()),
            StructField("customer_name", StringType()),
            StructField("date_plan", StringType()),
            StructField("time_plan", StringType()),
            StructField("date_visi", StringType()),
            StructField("time_visi", StringType()),
            StructField("object", StringType()),
            StructField("visit_end", StringType()),
            StructField("visit_time", StringType()),
            StructField("regioncode", StringType()),
            StructField("areacode", StringType()),
            StructField("branchcode", StringType()),
            StructField("saleunit", StringType()),
            StructField("time_survey_in", StringType()),
            StructField("time_survey_out", StringType()),
            StructField("count_survey", StringType())
            ])

        
        # Read the CSV file into a DataFrame
    
        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "|")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)


        #---------------------------Transformation logic ------------------------------#
        
        #Check if the Dataframe is having Data
        

        if df.count()==0:
            return "No Data in file"
        

        df= df.filter(df["date_plan"].isNotNull())
        

        #transform columns

        df = df.withColumn("CNTRY_CD", lit("TH"))
        df = df.withColumn("CRNCY_CD", lit("THB"))
        df = df.with_column("id_sale",trim(col("id_sale")))
        df = df.with_column("sale_name",trim(col("sale_name")))
        df = df.with_column("id_customer",trim(col("id_customer")))
        df = df.with_column("customer_name",trim(col("customer_name")))
        df = df.with_column("date_plan",trim(col("date_plan")))
        df = df.with_column("time_plan",trim(col("time_plan")))
        df = df.with_column("date_visi",trim(col("date_visi")))
        df = df.with_column("time_visi",trim(col("time_visi")))
        df = df.with_column("object",trim(col("object")))
        df = df.with_column("visit_end",trim(col("visit_end")))
        df = df.with_column("visit_time",trim(col("visit_time")))
        df = df.with_column("regioncode",trim(col("regioncode")))
        df = df.with_column("areacode",trim(col("areacode")))
        df = df.with_column("branchcode",trim(col("branchcode")))
        df = df.with_column("saleunit",trim(col("saleunit")))
        df = df.with_column("time_survey_in",trim(col("time_survey_in")))
        df = df.with_column("time_survey_out",trim(col("time_survey_out")))
        df = df.with_column("count_survey",trim(col("count_survey")))
        run_id=datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        new_file_name=file_name.split(".")[0]+''_''+run_id+''.txt''
        df= df.with_column("run_id", lit(run_id)) 
        df=df.with_column("filename",lit(new_file_name))
        df=df.with_column("crt_dttm",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))

        

        final_df = df.select("CNTRY_CD","CRNCY_CD","id_sale","sale_name","id_customer","customer_name",to_date("date_plan", lit("YYYYMMDD")).as_("date_plan"),"time_plan",to_date("date_visi", lit("YYYYMMDD")).as_("date_visi"),"time_visi","object",to_date("visit_end", lit("YYYYMMDD")).as_("visit_end"),"visit_time","regioncode","areacode","branchcode","saleunit","time_survey_in","time_survey_out","count_survey","filename","run_id","crt_dttm")

       
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
        run_id=datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        new_file_name=file_name.split(".")[0]+''_''+run_id+''.txt''
        df= df.with_column("run_id", lit(run_id)) 
        df=df.with_column("filename",lit(new_file_name))
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
