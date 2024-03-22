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
        final_df = df.select("distributorid","arcode","arname","araddress","telephone","fax","city","region","saledistrict","saleoffice","salegroup","artypecode","saleemployee","salename","billno","billmoo","billsoi","billroad","billsubdist","billdistrict","billprovince","billzipcode","activestatus","routestep1","routestep2","routestep3","routestep4","routestep5","routestep6","routestep7","routestep8","routestep9","routestep10","store","pricelevel","salesarename","branchcode","frequencyofvisit","filename","run_id","crt_dttm")


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

CREATE OR REPLACE PROCEDURE THASDL_RAW.TH_MBOX_INVENTORY_PREPROCESSING("PARAM" ARRAY)
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
    #Param=["I_KCS2402282300.txt","THASDL_RAW.DEV_LOAD_STAGE_ADLS","dev/GT_Intervention/DnA_VMR/cert-data-lake/dms_source/processed_file","SDL_TH_DMS_INVENTORY_FACT"]
    try:
       file_name       = Param[0]
       stage_name      = Param[1]
       temp_stage_path = Param[2]
       target_table    = Param[3]
       
       df_schema = StructType([
        StructField("recdate", StringType(), nullable=True),
        StructField("distributorid", StringType(), nullable=True),
        StructField("whcode", StringType(), nullable=True),
        StructField("productcode", StringType(), nullable=True),
        StructField("qty", StringType(), nullable=True),
        StructField("amount", StringType(), nullable=True),
        StructField("batchno", StringType(), nullable=True),
        StructField("expirydate", StringType(), nullable=True)
        ])
       df = session.read\\
                    .schema(df_schema)\\
                    .option("skip_header",0)\\
                    .option("field_delimiter", "\\t")\\
                    .option("field_optionally_enclosed_by", "\\"") \\
                    .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
        
       df=df.na.drop("all")
       if df.count()==0 :
           return "No Data in file"
       
       df= df.with_column("run_id", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S"))) 
       df=df.with_column("file_name",lit(file_name))
       df=df.with_column("crt_dttm",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))

       snowdf=df.select(    "recdate",
                            "distributorid",
                            "whcode",
                            "productcode",
                            "qty",
                            "amount",
                            "batchno",
                            "expirydate",
                            "crt_dttm",
                            "run_id",
                            "file_name")
            
       #snowdf= snowdf.filter(snowdf["distributorid"].isNotNull())
    
       #if snowdf.count()==0 :
                #return "No Data in table"
                
            
            #move file into success folder
       file_name=file_name.split(".")[0]+"_"+datetime.now().strftime("%Y%m%d%H%M%S")
       
       current_date = datetime.now()
       formatted_year = current_date.strftime("%Y")
       formatted_month = current_date.strftime("%m")
        
        #move to success
       snowdf.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)
    
            #write on sdl layer
        
       snowdf.write.mode("append").saveAsTable(stage_name.split(".")[0]+"."+target_table)
            
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
            return error_message
            
            
       ';

CREATE OR REPLACE PROCEDURE THASDL_RAW.TH_MBOX_SALETOOL_CUSTOMER_PREPROCESSING("PARAM" ARRAY)
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
    #Param=["A1_MPP2403052250_20240306023142.txt","THASDL_RAW.DEV_LOAD_STAGE_ADLS","dev/GT_Intervention/DnA_VMR/cert-data-lake/dms_source/processed_file","SDL_TH_DMS_CUSTOMER_DIM"]
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        df_schema = StructType([
            StructField("distributorid", StringType(), nullable=True),
            StructField("arcode", StringType(), nullable=True),
            StructField("arname", StringType(), nullable=True),
            StructField("araddress", StringType(), nullable=True),
            StructField("telephone", StringType(), nullable=True),
            StructField("fax", StringType(), nullable=True),
            StructField("city", StringType(), nullable=True),
            StructField("region", StringType(), nullable=True),
            StructField("saledistrict", StringType(), nullable=True),
            StructField("saleoffice", StringType(), nullable=True),
            StructField("salegroup", StringType(), nullable=True),
            StructField("artypecode", StringType(), nullable=True),
            StructField("saleemployee", StringType(), nullable=True),
            StructField("salename", StringType(), nullable=True),
            StructField("billno", StringType(), nullable=True),
            StructField("billmoo", StringType(), nullable=True),
            StructField("billsoi", StringType(), nullable=True),
            StructField("billroad", StringType(), nullable=True),
            StructField("billsubdist", StringType(), nullable=True),
            StructField("billdistrict", StringType(), nullable=True),
            StructField("billprovince", StringType(), nullable=True),
            StructField("billzipcode", StringType(), nullable=True),
            StructField("activestatus", StringType(), nullable=True),
            StructField("routestep1", StringType(), nullable=True),
            StructField("routestep2", StringType(), nullable=True),
            StructField("routestep3", StringType(), nullable=True),
            StructField("routestep4", StringType(), nullable=True),
            StructField("routestep5", StringType(), nullable=True),
            StructField("routestep6", StringType(), nullable=True),
            StructField("routestep7", StringType(), nullable=True),
            StructField("routestep8", StringType(), nullable=True),
            StructField("routestep9", StringType(), nullable=True),
            StructField("routestep10", StringType(), nullable=True),
            StructField("store", StringType(), nullable=True),
            StructField("sourcefile", StringType(), nullable=True),
            StructField("old_custid", StringType(), nullable=True),
            StructField("modifydate", StringType(), nullable=True)
             ])
        df = session.read\\
                    .schema(df_schema)\\
                    .option("skip_header",0)\\
                    .option("field_delimiter", "\\t")\\
                    .option("field_optionally_enclosed_by", "\\"") \\
                    .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
        
        df=df.na.drop("all")
        if df.count()==0 :
            return "No Data in file"


        df= df.with_column("run_id", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S"))) 
        df=df.with_column("file_name",lit(file_name))
        df=df.with_column("curr_date",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))

        snowdf=df.select(   "distributorid", "arcode", "arname", "araddress", "telephone", "fax", "city", "region",
                            "saledistrict", "saleoffice", "salegroup", "artypecode", "saleemployee", "salename",
                            "billno", "billmoo", "billsoi", "billroad", "billsubdist", "billdistrict", "billprovince",
                            "billzipcode", "activestatus", "routestep1", "routestep2", "routestep3", "routestep4",
                            "routestep5", "routestep6", "routestep7", "routestep8", "routestep9", "routestep10",
                            "store", "sourcefile", "old_custid", "modifydate", "curr_date", "run_id", "file_name"
                        )
                                    
        #snowdf= snowdf.filter(snowdf["distributorid"].isNotNull())
    
        #if snowdf.count()==0 :
                #return "No Data in table"
                
            
            #move file into success folder
        file_name=file_name.split(".")[0]+"_"+datetime.now().strftime("%Y%m%d%H%M%S")
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        
        #move to success
        snowdf.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)
    
            #write on sdl layer
        
        snowdf.write.mode("append").saveAsTable(stage_name.split(".")[0]+"."+target_table)
            
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
            return error_message




        
        ';

CREATE OR REPLACE PROCEDURE THASDL_RAW.TH_MBOX_SELLOUT_PREPROCESSING("PARAM" ARRAY)
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
    #Param=[''S_rev2208312255_20220901015227_rev.csv'',''THASDL_RAW.DEV_LOAD_STAGE_ADLS'',''dev/GT_Intervention/DnA_VMR/cert-data-lake/dms_source/processed_file'',''SDL_TH_DMS_sellout_FACT'']
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        
        df_schema = StructType([
            StructField("distributorid", StringType(), nullable=True),
            StructField("orderno", StringType(), nullable=True),
            StructField("orderdate", StringType(), nullable=True),
            StructField("arcode", StringType(), nullable=True),
            StructField("arname", StringType(), nullable=True),
            StructField("city", StringType(), nullable=True),
            StructField("region", StringType(), nullable=True),
            StructField("saledistrict", StringType(), nullable=True),
            StructField("saleoffice", StringType(), nullable=True),
            StructField("salegroup", StringType(), nullable=True),
            StructField("artypecode", StringType(), nullable=True),
            StructField("saleemployee", StringType(), nullable=True),
            StructField("salename", StringType(), nullable=True),
            StructField("productcode", StringType(), nullable=True),
            StructField("productdesc", StringType(), nullable=True),
            StructField("megabrand", StringType(), nullable=True),
            StructField("brand", StringType(), nullable=True),
            StructField("baseproduct", StringType(), nullable=True),
            StructField("variant", StringType(), nullable=True),
            StructField("putup", StringType(), nullable=True),
            StructField("grossprice", StringType(), nullable=True),
            StructField("qty", StringType(), nullable=True),
            StructField("subamt1", StringType(), nullable=True),
            StructField("discount", StringType(), nullable=True),
            StructField("subamt2", StringType(), nullable=True),
            StructField("discountbtline", StringType(), nullable=True),
            StructField("totalbeforevat", StringType(), nullable=True),
            StructField("total", StringType(), nullable=True),
            StructField("linenumber", StringType(), nullable=True),
            StructField("iscancel", StringType(), nullable=True),
            StructField("cndocno", StringType(), nullable=True),
            StructField("cnreasoncode", StringType(), nullable=True),
            StructField("promotionheader1", StringType(), nullable=True),
            StructField("promotionheader2", StringType(), nullable=True),
            StructField("promotionheader3", StringType(), nullable=True),
            StructField("promodesc1", StringType(), nullable=True),
            StructField("promodesc2", StringType(), nullable=True),
            StructField("promodesc3", StringType(), nullable=True),
            StructField("promocode1", StringType(), nullable=True),
            StructField("promocode2", StringType(), nullable=True),
            StructField("promocode3", StringType(), nullable=True),
            StructField("avgdiscount", StringType(), nullable=True)
        ])

        df = session.read\\
                    .schema(df_schema)\\
                    .option("skip_header",0)\\
                    .option("field_delimiter", "\\t")\\
                    .option("field_optionally_enclosed_by", "\\"") \\
                    .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
        
        df=df.na.drop("all")
        if df.count()==0 :
            return "No Data in file"
        


        df= df.with_column("run_id", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S"))) 
        df=df.with_column("file_name",lit(file_name.split(".")[0]+".csv"))
        df=df.with_column("curr_date",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))

        snowdf=df.select(    "distributorid", "orderno", "orderdate", "arcode", "arname", "city", "region", 
                            "saledistrict", "saleoffice", "salegroup", "artypecode", "saleemployee", "salename", 
                            "productcode", "productdesc", "megabrand", "brand", "baseproduct", "variant", "putup", 
                            "grossprice", "qty", "subamt1", "discount", "subamt2", "discountbtline", "totalbeforevat", 
                            "total", "linenumber", "iscancel", "cndocno", "cnreasoncode", "promotionheader1", 
                            "promotionheader2", "promotionheader3", "promodesc1", "promodesc2", "promodesc3", 
                            "promocode1", "promocode2", "promocode3", "avgdiscount", "curr_date", "run_id", "file_name")
        
            
        
            
            #move file into success folder
        file_name=file_name.split(".")[0]+"_"+datetime.now().strftime("%Y%m%d%H%M%S")
        
    
            #write on sdl layer
        
        snowdf.write.mode("append").saveAsTable(stage_name.split(".")[0]+"."+target_table)
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        
        #move to success
        snowdf.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)
            
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
            return error_message




        ';

