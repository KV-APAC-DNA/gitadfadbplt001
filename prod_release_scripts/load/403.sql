CREATE OR REPLACE PROCEDURE IDNSDL_RAW.ID_PS_PROMO_COMPETITOR_PREPROCESSING("PARAM" ARRAY)
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
    # Param=["PROMO COMPETITOR - Y2024 M5 - CREATE AT 2024.05.14 03.10.15.txt","IDNSDL_RAW.DEV_LOAD_STAGE_ADLS_IDN","dev/5Ps/transaction/promo_competitor/","SDL_ID_PS_PROMOTION_COMPETITOR"]
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]

        file_name_new= file_name.split("_")[0]
        # print(file_name_new)
        
        df_schema = StructType([
                # StructField("mnth_id",StringType(), True),
                StructField("outlet_id",StringType(), True),
                StructField("outlet_custom_code",StringType(), True),
                StructField("outlet_name",StringType(), True),
                StructField("province",StringType(), True),
                StructField("city",StringType(), True),
                StructField("channel",StringType(), True),
                StructField("merchandiser_id",StringType(), True),
                StructField("merchandiser_name",StringType(), True),
                StructField("cust_group",StringType(), True),
                StructField("address",StringType(), True),
                StructField("jnj_year",StringType(), True),
                StructField("jnj_month",StringType(), True),
                StructField("jnj_week",StringType(), True),
                StructField("day_name",StringType(), True),
                StructField("input_date",StringType(), True),
                StructField("franchise",StringType(), True),
                StructField("photo_link",StringType(), True),
                StructField("description",StringType(), True)
            ])
        
        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter","\\t")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name_new)

        
        
        df=df.na.drop("all")
        if df.count()==0 :
            return "No Data in file"


        df = df.with_column("file_name", lit(file_name))



        
        final_df= df.select(
                    # "mnth_id",
                    "outlet_id",
                    "outlet_custom_code",
                    "outlet_name",
                    "province",
                    "city",
                    "channel",
                    "merchandiser_id",
                    "merchandiser_name",
                    "cust_group",
                    "address",
                    "jnj_year",
                    "jnj_month",
                    "jnj_week",
                    "day_name",
                    "input_date",
                    "franchise",
                    "photo_link",
                    "description",
                    "file_name"              
                    )
        file_name=file_name.split(".")[0]+"_"+datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")

        file_name=file_name.split(".")[0]
        final_df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)
        return "Success"
        # #return final_df
 
    
 
    except KeyError as key_error:
        # Handle KeyError (missing columns) here
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message
    except Exception as e:
        # Handle exceptions here
        error_message = f"Error: {str(e)}"
        return error_message';

CREATE OR REPLACE PROCEDURE IDNSDL_RAW.IVY_OUTLETMASTER_PREPROCESSING("PARAM" ARRAY)
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
from snowflake.snowpark.functions import col,lit,trim, substring
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField,DecimalType
import pandas as pd
import numpy as np
from datetime import datetime
import pytz

def main(session: snowpark.Session,Param): 

    try:

        #Param=[''OutletMaster_20240606_0130.csv'',''IDNSDL_RAW.DEV_LOAD_STAGE_ADLS_IDN'',''dev/IVY/'',''SDL_DISTRIBUTOR_IVY_OUTLET_MASTER'']

        # Extracting parameters from the input
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        sch_name        = stage_name.split(''.'')[0]
        target_table    = sch_name+"."+Param[3]


        # Define the schema for the DataFrame
        df_schema=StructType([
            StructField("DISTRIBUTORCODE",StringType()),
            StructField("USERCODE",StringType()),
            StructField("LOCATIONCODE",StringType()),
            StructField("OUTLETCODE",StringType()),
            StructField("OUTLETNAME",StringType()),
            StructField("OUTLETADDRESS",StringType()),
            StructField("PINCODE",StringType()),
            StructField("CHANNELCODE",StringType()),
            StructField("CHANNELNAME",StringType()),
            StructField("SUBCHANNELCODE",StringType()),
            StructField("SUBCHANNELNAME",StringType()),
            StructField("TIERINGCODE",StringType()),
            StructField("TIERINGNAME",StringType()),
            StructField("CLASSCODE",StringType()),
            StructField("ROUTECODE",StringType()),
            StructField("VISIT_FREQUENCY",StringType()),
            StructField("VISITDAY",StringType()),
            StructField("JNJ_ID",StringType()),
            StructField("CONTACTPERSON",StringType()),
            StructField("CREDIT_LIMIT",StringType()),
            StructField("INVOICE_LIMIT",StringType()),
            StructField("CREDIT_PERIOD",StringType()),
            StructField("TIN",StringType()),
            StructField("IS_DIAMOND_STORE",StringType()),
            StructField("ID_NO",StringType()),
            StructField("MASTER_CODE",StringType()),
            StructField("STORE_CLUSTER",StringType()),
            StructField("LATTITUDE",StringType()),
            StructField("LONGITUDE",StringType())
            ])


        # Read the CSV file into a DataFrame
    
        dataframe = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "|")\\
            .option("encoding","UTF-8")\\
            .option("skip_blank_lines", True)\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        #---------------------------Transformation logic ------------------------------#
        

        # Handle null values or empty rows
        dataframe=dataframe.na.drop("all")

        # Check for empty Dataframe
        if dataframe.count()==0:
            return "No Data in file"

        df_pandas=dataframe.to_pandas()

        columns_to_check = ["CREDIT_LIMIT", "INVOICE_LIMIT", "CREDIT_PERIOD"]
        df_pandas[columns_to_check] = df_pandas[columns_to_check].apply(lambda x: x.str.strip()).replace('''', np.nan)

        
        df_pandas[''JNJ_ID''] = df_pandas[''JNJ_ID''].fillna('''')


        dataframe=session.create_dataframe(df_pandas)

        df=dataframe.select("DISTRIBUTORCODE","USERCODE","LOCATIONCODE","OUTLETCODE","OUTLETNAME","OUTLETADDRESS","PINCODE","CHANNELCODE","CHANNELNAME","SUBCHANNELCODE",\\
                           "SUBCHANNELNAME","TIERINGCODE","TIERINGNAME","CLASSCODE","ROUTECODE","VISIT_FREQUENCY","VISITDAY","JNJ_ID","CONTACTPERSON","CREDIT_LIMIT",\\
                           "INVOICE_LIMIT","CREDIT_PERIOD","TIN","IS_DIAMOND_STORE","ID_NO","MASTER_CODE","STORE_CLUSTER","LATTITUDE","LONGITUDE")

        # Add CDL_DTTM, RUNID and filename
        
        
        df = df.with_column("CDL_DTTM",lit(datetime.now(pytz.timezone("GMT")).strftime("%Y-%m-%d %H:%M:%S")))
        df = df.with_column("RUN_ID",lit(datetime.now(pytz.timezone("GMT")).strftime("%Y%m%d%H%M%S")).cast("integer"))
        df = df.with_column("SOURCE_FILE_NAME",lit(file_name))

        # Creating Final Dataframe
        final_df = df.alias("final_df")
        
         # Load Data to the target table
        
        final_df.write.mode("append").saveAsTable(target_table)


        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")

         # write to success folder
    
        file_name=file_name.split(".")[0]+''_''+datetime.now().strftime("%Y%m%d%H%M%S")
        final_df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True,file_format=''FILE_FORMAT_COMMA'')
        
        return "Success"


    

    except KeyError as key_error:
        # Handle KeyError (missing columns) here
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message
        
    except Exception as e:
        # Handle exceptions here
        error_message = f"Error: {str(e)}"
        return error_message';
