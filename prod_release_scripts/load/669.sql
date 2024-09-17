CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.IDNSDL_RAW.IVY_INVENTORY_PREPROCESSING("PARAM" ARRAY)
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

        #Param=[''Inventory_20240521_0130.csv'',''IDNSDL_RAW.DEV_LOAD_STAGE_ADLS_IDN'',''dev/IVY/transaction/Inventory/'',''SDL_DISTRIBUTOR_IVY_INVENTORY'']

        # Extracting parameters from the input
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        sch_name        = stage_name.split(''.'')[0]
        target_table    = sch_name+"."+Param[3]


        # Define the schema for the DataFrame
        df_schema=StructType([
            StructField("DISTRIBUTOR_CODE",StringType()),
            StructField("WAREHOUSE_CODE",StringType()),
            StructField("PRODUCT_CODE",StringType()),
            StructField("BATCH_CODE",StringType()),
            StructField("BATCH_EXPIRY_DATE",StringType()),
            StructField("UOM",StringType()),
            StructField("QTY",StringType())
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

        # Add CDL_DTTM, RUNID and filename
        
        
        dataframe = dataframe.with_column("CDL_DTTM",lit(datetime.now(pytz.timezone("GMT")).strftime("%Y-%m-%d %H:%M:%S")))
        dataframe = dataframe.with_column("RUN_ID",lit(datetime.now(pytz.timezone("GMT")).strftime("%Y%m%d%H%M%S")).cast("integer"))
        dataframe = dataframe.with_column("FILE_NAME", lit(Param[0].split(".")[0]))

        # Creating Final Dataframe
        final_df = dataframe.alias("final_df")

        
        # Load Data to the target table
        
        final_df.write.mode("append").saveAsTable(target_table)


        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")

         # write to success folder
    
        file_name=file_name.split(".")[0]+''_''+datetime.now().strftime("%Y%m%d%H%M%S")
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
		
CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.IDNSDL_RAW.IVY_USERMASTER_PREPROCESSING("PARAM" ARRAY)
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
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField
import pandas as pd
from datetime import datetime
import pytz

def main(session: snowpark.Session,Param): 

    try:

        #Param=[''UserMaster_20240423_0130.csv'',''IDNSDL_RAW.DEV_LOAD_STAGE_ADLS_IDN'',''dev/IVY/transaction/UserMaster/'',''SDL_DISTRIBUTOR_IVY_USER_MASTER'']

        # Extracting parameters from the input
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        sch_name        = stage_name.split(''.'')[0]
        target_table    = sch_name+"."+Param[3]


        # Define the schema for the DataFrame
        df_schema=StructType([
            StructField("MD_LOCATION",StringType()),
            StructField("MD_CODE",StringType()),
            StructField("MD_NAME",StringType()),
            StructField("SD_LOCATION",StringType()),
            StructField("SD_CODE",StringType()),
            StructField("SD_NAME",StringType()),
            StructField("RBDM_LOCATION",StringType()),
            StructField("RBDM_CODE",StringType()),
            StructField("RBDM_NAME",StringType()),
            StructField("BDM_LOCATION",StringType()),
            StructField("BDM_CODE",StringType()),
            StructField("BDM_NAME",StringType()),
            StructField("BDR_LOCATION",StringType()),
            StructField("BDR_CODE",StringType()),
            StructField("BDR_NAME",StringType()),
            StructField("DIS_LOCATION",StringType()),
            StructField("DIS_CODE",StringType()),
            StructField("DIS_NAME",StringType()),
            StructField("RSM_CODE",StringType()),
            StructField("RSM_NAME",StringType()),
            StructField("SUP_CODE",StringType()),
            StructField("SUP_NAME",StringType()),
            StructField("SR_CODE",StringType()),
            StructField("SR_NAME",StringType())
            ])


        # Read the CSV file into a DataFrame
    
        dataframe = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "|")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .option("encoding","UTF-8")\\
            .option("skip_blank_lines", True)\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        #---------------------------Transformation logic ------------------------------#
        

        # Handle null values or empty rows
        dataframe=dataframe.na.drop("all")

        # Check for empty Dataframe
        if dataframe.count()==0:
            return "No Data in file"

        # Replace null values with an empty string for multiple columns
        columns_to_fill = ["DIS_LOCATION", "DIS_CODE", "DIS_NAME","RSM_CODE","RSM_NAME","SUP_CODE","SUP_NAME","SR_CODE","SR_NAME"]
        dataframe = dataframe.na.fill('''', subset=columns_to_fill)

        # Add CDL_DTTM, RUNID and filename
        
        
        dataframe = dataframe.with_column("CDL_DTTM",lit(datetime.now(pytz.timezone("GMT")).strftime("%Y-%m-%d %H:%M:%S")))
        dataframe = dataframe.with_column("RUN_ID",lit(datetime.now(pytz.timezone("GMT")).strftime("%Y%m%d%H%M%S")).cast("integer"))
        dataframe = dataframe.with_column("FILE_NAME", lit(Param[0].split(".")[0]))

        # Creating Final Dataframe
        final_df = dataframe.alias("final_df")


        # Load Data to the target table
        
        final_df.write.mode("append").saveAsTable(target_table)


        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")

        # write to success folder
    
        file_name=file_name.split(".")[0]+''_''+datetime.now().strftime("%Y%m%d%H%M%S")
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
		
CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.IDNSDL_RAW.ID_SERVER_DATA_CUSTOMERMASTER_PREPROCESSING("PARAM" ARRAY)
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
from snowflake.snowpark.functions import col,lit,trim,regexp_replace
from snowflake.snowpark.types import StringType, StructType, StructField
import pandas as pd
from datetime import datetime
import pytz
 
def main(session: snowpark.Session,Param):
 
    try:
 
        #Param=[''Customer_1.csv'',''IDNSDL_RAW.DEV_LOAD_STAGE_ADLS_IDN'',''dev/alteryx/transaction/customermaster'',''SDL_DISTRIBUTOR_CUSTOMER_DIM'']
 
        # Extracting parameters from the input
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
 
 
        # Define the schema for the DataFrame
        df_schema=StructType([
            
            StructField("KEY_OUTLET", StringType()),
            StructField("JJ_SAP_DSTRBTR_ID", StringType()),
            StructField("JJ_SAP_DSTRBTR_NM", StringType()),
            StructField("CUST_ID", StringType()),
            StructField("CUST_NM", StringType()),
            StructField("ADDRESS", StringType()),
            StructField("CITY", StringType()),
            StructField("CUST_GRP", StringType()),
            StructField("CHNL", StringType()),
            StructField("OUTLET_TYPE", StringType()),
            StructField("CHNL_GRP", StringType()),
            StructField("JJID", StringType()),
            StructField("PST_CD", StringType()),
            StructField("CUST_ID_MAP", StringType()),
            StructField("CUST_NM_MAP", StringType()),
            StructField("CHNL_GRP2", StringType()),
            StructField("CUST_CRTD_DT", StringType()),
            StructField("CUST_GRP2", StringType()),
            StructField("CRTD_DTTM", StringType()),
            StructField("UPDT_DTTM", StringType())
            ])
 
 
        # Read the CSV file into a DataFrame
        dataframe = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "|")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .option("skip_blank_lines", True)\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
 
        #---------------------------Transformation logic ------------------------------#
 
        # Handle null values or empty rows
        dataframe=dataframe.na.drop("all")
 
        # Check for empty Dataframe
        if dataframe.count()==0:
            return "No Data in file"

        dataframe = dataframe.with_column("FILE_NAME", lit(file_name.split(".")[0]))
        
        #Creating copy of the Dataframe
        final_df = final_df = dataframe.select("KEY_OUTLET","JJ_SAP_DSTRBTR_ID","JJ_SAP_DSTRBTR_NM","CUST_ID","CUST_NM","ADDRESS","CITY","CUST_GRP","CHNL","OUTLET_TYPE","CHNL_GRP","JJID","PST_CD","CUST_ID_MAP","CUST_NM_MAP",\\
        "CHNL_GRP2","CUST_CRTD_DT","CUST_GRP2","CRTD_DTTM","UPDT_DTTM","FILE_NAME"   )

        #return final_df
        
       # Load Data to the target table
        final_df.write.mode("append").saveAsTable(target_table)
 
         
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
 
        # write to success folder
        file_name = file_name.split(''.'')[0]+"_"+datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        final_df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True,file_format = "idnsdl_raw.CSV_FORMAT_PIPE")
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
		
CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.IDNSDL_RAW.IVY_MERCHANDISING_PREPROCESSING("PARAM" ARRAY)
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

        #Param=[''Merchandising_20240522_0130.csv'',''IDNSDL_RAW.DEV_LOAD_STAGE_ADLS_IDN'',''dev/IVY/transaction/Merchandising/'',''SDL_DISTRIBUTOR_IVY_MERCHANDISING'']

        # Extracting parameters from the input
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        sch_name        = stage_name.split(''.'')[0]
        target_table    = sch_name+"."+Param[3]


        # Define the schema for the DataFrame
        df_schema=StructType([
            StructField("DISTRIBUTOR_CODE",StringType()),
            StructField("DISTRIBUTOR_NAME",StringType()),
            StructField("SALES_REPCODE",StringType()),
            StructField("SALES_REPNAME",StringType()),
            StructField("CHANNEL_NAME",StringType()),
            StructField("SUB_CHANNEL_NAME",StringType()),
            StructField("RETAILER_CODE",StringType()),
            StructField("RETAILER_NAME",StringType()),
            StructField("MONTH",StringType()),
            StructField("SURVEYDATE",StringType()),
            StructField("AQ_NAME",StringType()),
            StructField("SRD_ANSWER",StringType()),
            StructField("LINK",StringType())
            ])


        # Read the CSV file into a DataFrame
    
        dataframe = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "|")\\
            .option("encoding","UTF-8")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .option("skip_blank_lines", True)\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        df_pandas=dataframe.to_pandas()

        # Remove spaces in the values using strip

        #df_pandas["DISTRIBUTOR_CODE"] = df_pandas["DISTRIBUTOR_CODE"].apply(lambda x: x.strip()[:6] if len(x.strip()) >= 7 else x.strip())
        df_pandas[''DISTRIBUTOR_CODE''] = df_pandas[''DISTRIBUTOR_CODE''].str.strip()
        df_pandas[''DISTRIBUTOR_CODE''] = np.where(df_pandas[''DISTRIBUTOR_CODE''].str.len() >= 7, df_pandas[''DISTRIBUTOR_CODE''].str[0:6], df_pandas[''DISTRIBUTOR_CODE''])
        df_pandas[''DISTRIBUTOR_NAME''] = df_pandas[''DISTRIBUTOR_NAME''].str.strip()
        df_pandas[''SALES_REPCODE''] = df_pandas[''SALES_REPCODE''].str.strip()
        df_pandas[''SALES_REPNAME''] = df_pandas[''SALES_REPNAME''].str.strip()
        df_pandas[''CHANNEL_NAME''] = df_pandas[''CHANNEL_NAME''].str.strip()
        df_pandas[''SUB_CHANNEL_NAME''] = df_pandas[''SUB_CHANNEL_NAME''].str.strip()
        df_pandas[''RETAILER_CODE''] = np.where(df_pandas[''RETAILER_CODE''].str.strip().str.upper().str.endswith(''\\\\D''), df_pandas[''RETAILER_CODE''].str.split(''_'').str[0], df_pandas[''RETAILER_CODE''])
        df_pandas[''RETAILER_NAME''] = df_pandas[''RETAILER_NAME''].str.strip().str.replace("''''","")
        df_pandas[''MONTH''] = df_pandas[''MONTH''].str.strip()
        df_pandas[''SURVEYDATE''] = df_pandas[''SURVEYDATE''].str.strip()
        df_pandas[''AQ_NAME''] = df_pandas[''AQ_NAME''].str.strip()
        df_pandas[''SRD_ANSWER''] = df_pandas[''SRD_ANSWER''].str.strip()
        df_pandas[''LINK''] = df_pandas[''LINK''].str.strip()

        # Apply groupby / row number logic to filter out duplicates based on specific columns

        df_pandas[''row_number''] = df_pandas.groupby([''DISTRIBUTOR_CODE'', ''SALES_REPCODE'', ''RETAILER_CODE'', ''SURVEYDATE'', ''AQ_NAME'', df_pandas[''LINK''].fillna(''NA'')])[''SRD_ANSWER''].rank(method=''first'', ascending=False)
        print(df_pandas[''row_number''].value_counts())
        result_df = df_pandas[df_pandas[''row_number''] == 1].drop(columns=[''row_number''])
        

        #df_pandas.drop_duplicates(subset=["DISTRIBUTOR_CODE","SALES_REPCODE","RETAILER_CODE","SURVEYDATE","AQ_NAME","LINK"],keep=''first'')

        dataframe=session.create_dataframe(result_df)

        # Add CDL_DTTM, RUNID and filename
        
        
        dataframe = dataframe.with_column("CDL_DTTM",lit(datetime.now(pytz.timezone("GMT")).strftime("%Y-%m-%d %H:%M:%S")))
        dataframe = dataframe.with_column("RUN_ID",lit(datetime.now(pytz.timezone("GMT")).strftime("%Y%m%d%H%M%S")).cast("integer"))
        dataframe = dataframe.with_column("FILE_NAME", lit(Param[0].split(".")[0]))
        
        # Creating Final Dataframe
        final_df = dataframe.alias("final_df")


        # Load Data to the target table
        
        final_df.write.mode("append").saveAsTable(target_table)


        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")

         # write to success folder
    
        file_name=file_name.split(".")[0]+''_''+datetime.now().strftime("%Y%m%d%H%M%S")
        final_df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True,file_format_name=''FILE_FORMAT_COMMA'')
        
        return "Success"



    except KeyError as key_error:
        # Handle KeyError (missing columns) here
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message
        
    except Exception as e:
        # Handle exceptions here
        error_message = f"Error: {str(e)}"
        return error_message';
		
CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.IDNSDL_RAW.IVY_INVOICE_PREPROCESSING("PARAM" ARRAY)
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
import re

def main(session: snowpark.Session,Param): 

    try:

        #Param=[''Invoice_20240523_0130.csv'',''IDNSDL_RAW.DEV_LOAD_STAGE_ADLS_IDN'',''dev/IVY/transaction/Invoice/'',''SDL_DISTRIBUTOR_IVY_INVOICE'']

        # Extracting parameters from the input
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        sch_name        = stage_name.split(''.'')[0]
        target_table    = sch_name+"."+Param[3]


        # Define the schema for the DataFrame
        df_schema=StructType([
            StructField("DISTRIBUTOR_CODE",StringType()),
            StructField("USER_CODE",StringType()),
            StructField("RETAILER_CODE",StringType()),
            StructField("INVOICE_DATE",StringType()),
            StructField("ORDER_ID",StringType()),
            StructField("INVOICE_NO",StringType()),
            StructField("PRODUCT_CODE",StringType()),
            StructField("UOM",StringType()),
            StructField("UOM_COUNT",StringType()),
            StructField("QTY",StringType()),
            StructField("PIECE_PRICE",StringType()),
            StructField("LINE_VALUE",StringType()),
            StructField("INVOICE_AMOUNT",StringType()),
            StructField("LINES_PER_CALL",StringType()),
            StructField("SCHEME_CODE",StringType()),
            StructField("SCHEME_DESCRIPTION",StringType()),
            StructField("SCHEME_DISCOUNT",StringType()),
            StructField("SCHEME_PRECENTAGE",StringType()),
            StructField("BILLDISCOUNT",StringType()),
            StructField("BILLDISC_PERCENTAGE",StringType()),
            StructField("PO_NUMBER",StringType()),
            StructField("PAYMENT_TYPE",StringType()),
            StructField("EXP_DELIVERY_DATE",StringType()),
            StructField("INVOICE_ADDRESS",StringType()),
            StructField("SHIPPING_ADDRESS",StringType()),
            StructField("INVOICE_STATUS",StringType()),
            StructField("EFAKTUR_NO",StringType()),
            StructField("TAX_VALUE",StringType()),
            StructField("BATCH_NO",StringType())
            ])


        # Read the CSV file into a DataFrame
    
        dataframe = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "|")\\
            .option("encoding","UTF-8")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .option("skip_blank_lines", True)\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        df_pandas=dataframe.to_pandas()

        #df_pandas[''ORDER_ID''] = df_pandas[''ORDER_ID''].apply(lambda x: "" if x == np.nan else x)
        df_pandas[''ORDER_ID''] = df_pandas[''ORDER_ID''].fillna("")
        df_pandas[''INVOICE_ADDRESS''] = df_pandas[''INVOICE_ADDRESS''].fillna("")
        df_pandas[''SHIPPING_ADDRESS''] = df_pandas[''SHIPPING_ADDRESS''].fillna("")
        df_pandas[''INVOICE_STATUS''] = df_pandas[''INVOICE_STATUS''].fillna("")

        df_pandas[''EXP_DELIVERY_DATE''] = df_pandas[''EXP_DELIVERY_DATE''].apply(lambda x: pd.to_datetime(x, format=''%Y-%m-%d'', errors=''coerce'') 
              if re.match(r''\\d{1,4}-\\d{1,2}-\\d{1,2}'', str(x)) 
              else pd.NaT)

        dataframe=session.create_dataframe(df_pandas)

        dataframe = dataframe.with_column("CDL_DTTM",lit(datetime.now(pytz.timezone("GMT")).strftime("%Y-%m-%d %H:%M:%S")))
        dataframe = dataframe.with_column("RUN_ID",lit(datetime.now(pytz.timezone("GMT")).strftime("%Y%m%d%H%M%S")).cast("integer"))
        dataframe = dataframe.with_column("FILE_NAME", lit(Param[0].split(".")[0]))

        # Creating Final Dataframe
        final_df = dataframe.alias("final_df")

        # Load Data to the target table
        
        final_df.write.mode("append").saveAsTable(target_table)
        

        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")

         # write to success folder
    
        file_name=file_name.split(".")[0]+''_''+datetime.now().strftime("%Y%m%d%H%M%S")
        final_df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True,file_format_name=''FILE_FORMAT_COMMA'')
        
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
		
CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.IDNSDL_RAW.IVY_ORDERS_PREPROCESSING("PARAM" ARRAY)
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

        #Param=[''OrderDetail_20240523_0130.csv'',''IDNSDL_RAW.DEV_LOAD_STAGE_ADLS_IDN'',''dev/IVY/transaction/Order/'',''SDL_DISTRIBUTOR_IVY_ORDER'']

        # Extracting parameters from the input
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        sch_name        = stage_name.split(''.'')[0]
        target_table    = sch_name+"."+Param[3]


        # Define the schema for the DataFrame
        df_schema=StructType([
            StructField("DISTRIBUTOR_CODE",StringType()),
            StructField("USER_CODE",StringType()),
            StructField("RETAILER_CODE",StringType()),
            StructField("ROUTE_CODE",StringType()),
            StructField("ORDER_DATE",StringType()),
            StructField("ORDER_ID",StringType()),
            StructField("PRODUCT_CODE",StringType()),
            StructField("UOM",StringType()),
            StructField("UOM_COUNT",StringType()),
            StructField("QTY",StringType()),
            StructField("LINE_VALUE",StringType()),
            StructField("PIECE_PRICE",StringType()),
            StructField("ORDER_VALUE",StringType()),
            StructField("LINES_PER_CALL",StringType()),
            StructField("SCHEME_CODE",StringType()),
            StructField("SCHEME_DESCRIPTION",StringType()),
            StructField("SCHEME_DISCOUNT",StringType()),
            StructField("SCHEME_PRECENTAGE",StringType()),
            StructField("BILLDISCOUNT",StringType()),
            StructField("BILLDISC_PERCENTAGE",StringType()),
            StructField("PO_NUMBER",StringType()),
            StructField("PAYMENT_TYPE",StringType()),
            StructField("DELIVERY_DATE",StringType()),
            StructField("INVOICE_ADDRESS",StringType()),
            StructField("SHIPPING_ADDRESS",StringType()),
            StructField("ORDER_STATUS",StringType()),
            StructField("ORDER_LATITUDE",StringType()),
            StructField("ORDER_LONGITUDE",StringType())
            ])


        # Read the CSV file into a DataFrame
    
        dataframe = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "|")\\
            .option("encoding","UTF-8")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .option("skip_blank_lines", True)\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        df_pandas=dataframe.to_pandas()

        df_pandas[''QTY''] = df_pandas[''QTY''].apply(lambda x: np.nan if x == '''' else x)

        dataframe=session.create_dataframe(df_pandas)

        dataframe = dataframe.with_column("CDL_DTTM",lit(datetime.now(pytz.timezone("GMT")).strftime("%Y-%m-%d %H:%M:%S")))
        dataframe = dataframe.with_column("RUN_ID",lit(datetime.now(pytz.timezone("GMT")).strftime("%Y%m%d%H%M%S")).cast("integer"))
        dataframe = dataframe.with_column("FILE_NAME", lit(file_name.split(".")[0]))

        # Creating Final Dataframe
        final_df = dataframe.alias("final_df")

        # Load Data to the target table
        
        final_df.write.mode("append").saveAsTable(target_table)

        
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")

         # write to success folder
    
        file_name=file_name.split(".")[0]+''_''+datetime.now().strftime("%Y%m%d%H%M%S")
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

CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.IDNSDL_RAW.IVY_OUTLETMASTER_PREPROCESSING("PARAM" ARRAY)
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
        df = df.with_column("FILE_NAME", lit(file_name.split(".")[0]))

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


CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.HCPSDL_RAW.HCP360_IN_SFMC_LOAD_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '
from snowflake.snowpark.functions import col, lit, date_format, current_timestamp, to_date, year, month, concat, format_number, regexp_replace,to_timestamp,when,trim,upper
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DecimalType,DateType
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz


def main(session: snowpark.Session,Param):
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        sch_name        = stage_name.split('.')[0]
        target_table    = sch_name+"."+Param[3]
       
        df = session.read\\
            .option("INFER_SCHEMA", True)\\
            .option("skip_header",0)\\
            .option("field_delimiter", "|")\\
            .option("field_optionally_enclosed_by", "\\"") \\
            .option("encoding","UTF-16LE") \\
            .option("trim_space", True) \\
            .with_metadata("METADATA$FILE_ROW_NUMBER") \\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        df = df.filter(col("METADATA$FILE_ROW_NUMBER") > lit(1))
        
        df = df.drop("METADATA$FILE_ROW_NUMBER")

        df = df.with_column("FILE_NAME", lit(file_name))

        df = df.withColumn("CRT_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        

        
        final_df = df

        if final_df.count()==0:
            return "No Data in file"
        
        # Load Data to the target table
        final_df.write.mode("append").saveAsTable(target_table)

        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")

        # write to success folder
    
        file_name=file_name.split(".")[0] + '_' + datetime.now().strftime("%Y%m%d%H%M%S")
        final_df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,file_format_type="csv",OVERWRITE=True,header=True)
   
        return "Success"
        
    except KeyError as key_error:
        
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message
        
    except Exception as e:
        
        error_message = f"Error: {str(e)}" +str(df.columns)
        return error_message
';
CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.HCPSDL_RAW.HCP360_IN_SFMC_LOAD_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '
from snowflake.snowpark.functions import col, lit, date_format, current_timestamp, to_date, year, month, concat, format_number, regexp_replace,to_timestamp,when,trim,upper
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DecimalType,DateType
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz


def main(session: snowpark.Session,Param):
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        sch_name        = stage_name.split('.')[0]
        target_table    = sch_name+"."+Param[3]
       
        df = session.read\\
            .option("INFER_SCHEMA", True)\\
            .option("skip_header",0)\\
            .option("field_delimiter", "|")\\
            .option("field_optionally_enclosed_by", "\\"") \\
            .option("encoding","UTF-16LE") \\
            .option("trim_space", True) \\
            .with_metadata("METADATA$FILE_ROW_NUMBER") \\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        df = df.filter(col("METADATA$FILE_ROW_NUMBER") > lit(1))
        
        df = df.drop("METADATA$FILE_ROW_NUMBER")

        df = df.with_column("FILE_NAME", lit(file_name))

        df = df.withColumn("CRT_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        

        
        final_df = df

        if final_df.count()==0:
            return "No Data in file"
        
        # Load Data to the target table
        final_df.write.mode("append").saveAsTable(target_table)

        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")

        # write to success folder
    
        file_name=file_name.split(".")[0] + '_' + datetime.now().strftime("%Y%m%d%H%M%S")
        final_df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,file_format_type="csv",OVERWRITE=True,header=True)
   
        return "Success"
        
    except KeyError as key_error:
        
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message
        
    except Exception as e:
        
        error_message = f"Error: {str(e)}" +str(df.columns)
        return error_message
';
CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.HCPSDL_RAW.HCP360_IN_SFMC_LOAD_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '
from snowflake.snowpark.functions import col, lit, date_format, current_timestamp, to_date, year, month, concat, format_number, regexp_replace,to_timestamp,when,trim,upper
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DecimalType,DateType
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz


def main(session: snowpark.Session,Param):
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        sch_name        = stage_name.split('.')[0]
        target_table    = sch_name+"."+Param[3]
       
        df = session.read\\
            .option("INFER_SCHEMA", True)\\
            .option("skip_header",0)\\
            .option("field_delimiter", "|")\\
            .option("field_optionally_enclosed_by", "\\"") \\
            .option("encoding","UTF-16LE") \\
            .option("trim_space", True) \\
            .with_metadata("METADATA$FILE_ROW_NUMBER") \\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        df = df.filter(col("METADATA$FILE_ROW_NUMBER") > lit(1))
        
        df = df.drop("METADATA$FILE_ROW_NUMBER")

        df = df.with_column("FILE_NAME", lit(file_name))

        df = df.withColumn("CRT_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        

        
        final_df = df

        if final_df.count()==0:
            return "No Data in file"
        
        # Load Data to the target table
        final_df.write.mode("append").saveAsTable(target_table)

        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")

        # write to success folder
    
        file_name=file_name.split(".")[0] + '_' + datetime.now().strftime("%Y%m%d%H%M%S")
        final_df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,file_format_type="csv",OVERWRITE=True,header=True)
   
        return "Success"
        
    except KeyError as key_error:
        
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message
        
    except Exception as e:
        
        error_message = f"Error: {str(e)}" +str(df.columns)
        return error_message
';

CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.HCPSDL_RAW.HCP360_IN_SFMC_LOAD_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '
from snowflake.snowpark.functions import col, lit, date_format, current_timestamp, to_date, year, month, concat, format_number, regexp_replace,to_timestamp,when,trim,upper
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DecimalType,DateType
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz


def main(session: snowpark.Session,Param):
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        sch_name        = stage_name.split('.')[0]
        target_table    = sch_name+"."+Param[3]
       
        df = session.read\\
            .option("INFER_SCHEMA", True)\\
            .option("skip_header",0)\\
            .option("field_delimiter", "|")\\
            .option("field_optionally_enclosed_by", "\\"") \\
            .option("encoding","UTF-16LE") \\
            .option("trim_space", True) \\
            .with_metadata("METADATA$FILE_ROW_NUMBER") \\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        df = df.filter(col("METADATA$FILE_ROW_NUMBER") > lit(1))
        
        df = df.drop("METADATA$FILE_ROW_NUMBER")

        df = df.with_column("FILE_NAME", lit(file_name))

        df = df.withColumn("CRT_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        

        
        final_df = df

        if final_df.count()==0:
            return "No Data in file"
        
        # Load Data to the target table
        final_df.write.mode("append").saveAsTable(target_table)

        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")

        # write to success folder
    
        file_name=file_name.split(".")[0] + '_' + datetime.now().strftime("%Y%m%d%H%M%S")
        final_df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,file_format_type="csv",OVERWRITE=True,header=True)
   
        return "Success"
        
    except KeyError as key_error:
        
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message
        
    except Exception as e:
        
        error_message = f"Error: {str(e)}" +str(df.columns)
        return error_message
';

CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.HCPSDL_RAW.HCP360_IN_SFMC_LOAD_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '
from snowflake.snowpark.functions import col, lit, date_format, current_timestamp, to_date, year, month, concat, format_number, regexp_replace,to_timestamp,when,trim,upper
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DecimalType,DateType
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz


def main(session: snowpark.Session,Param):
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        sch_name        = stage_name.split('.')[0]
        target_table    = sch_name+"."+Param[3]
       
        df = session.read\\
            .option("INFER_SCHEMA", True)\\
            .option("skip_header",0)\\
            .option("field_delimiter", "|")\\
            .option("field_optionally_enclosed_by", "\\"") \\
            .option("encoding","UTF-16LE") \\
            .option("trim_space", True) \\
            .with_metadata("METADATA$FILE_ROW_NUMBER") \\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        df = df.filter(col("METADATA$FILE_ROW_NUMBER") > lit(1))
        
        df = df.drop("METADATA$FILE_ROW_NUMBER")

        df = df.with_column("FILE_NAME", lit(file_name))

        df = df.withColumn("CRT_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        

        
        final_df = df

        if final_df.count()==0:
            return "No Data in file"
        
        # Load Data to the target table
        final_df.write.mode("append").saveAsTable(target_table)

        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")

        # write to success folder
    
        file_name=file_name.split(".")[0] + '_' + datetime.now().strftime("%Y%m%d%H%M%S")
        final_df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,file_format_type="csv",OVERWRITE=True,header=True)
   
        return "Success"
        
    except KeyError as key_error:
        
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message
        
    except Exception as e:
        
        error_message = f"Error: {str(e)}" +str(df.columns)
        return error_message
';
CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.HCPSDL_RAW.HCP360_IN_SFMC_LOAD_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '
from snowflake.snowpark.functions import col, lit, date_format, current_timestamp, to_date, year, month, concat, format_number, regexp_replace,to_timestamp,when,trim,upper
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DecimalType,DateType
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz


def main(session: snowpark.Session,Param):
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        sch_name        = stage_name.split('.')[0]
        target_table    = sch_name+"."+Param[3]
       
        df = session.read\\
            .option("INFER_SCHEMA", True)\\
            .option("skip_header",0)\\
            .option("field_delimiter", "|")\\
            .option("field_optionally_enclosed_by", "\\"") \\
            .option("encoding","UTF-16LE") \\
            .option("trim_space", True) \\
            .with_metadata("METADATA$FILE_ROW_NUMBER") \\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        df = df.filter(col("METADATA$FILE_ROW_NUMBER") > lit(1))
        
        df = df.drop("METADATA$FILE_ROW_NUMBER")

        df = df.with_column("FILE_NAME", lit(file_name))

        df = df.withColumn("CRT_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        

        
        final_df = df

        if final_df.count()==0:
            return "No Data in file"
        
        # Load Data to the target table
        final_df.write.mode("append").saveAsTable(target_table)

        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")

        # write to success folder
    
        file_name=file_name.split(".")[0] + '_' + datetime.now().strftime("%Y%m%d%H%M%S")
        final_df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,file_format_type="csv",OVERWRITE=True,header=True)
   
        return "Success"
        
    except KeyError as key_error:
        
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message
        
    except Exception as e:
        
        error_message = f"Error: {str(e)}" +str(df.columns)
        return error_message
';
CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.HCPSDL_RAW.HCP360_IN_SFMC_LOAD_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '
from snowflake.snowpark.functions import col, lit, date_format, current_timestamp, to_date, year, month, concat, format_number, regexp_replace,to_timestamp,when,trim,upper
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DecimalType,DateType
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz


def main(session: snowpark.Session,Param):
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        sch_name        = stage_name.split('.')[0]
        target_table    = sch_name+"."+Param[3]
       
        df = session.read\\
            .option("INFER_SCHEMA", True)\\
            .option("skip_header",0)\\
            .option("field_delimiter", "|")\\
            .option("field_optionally_enclosed_by", "\\"") \\
            .option("encoding","UTF-16LE") \\
            .option("trim_space", True) \\
            .with_metadata("METADATA$FILE_ROW_NUMBER") \\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        df = df.filter(col("METADATA$FILE_ROW_NUMBER") > lit(1))
        
        df = df.drop("METADATA$FILE_ROW_NUMBER")

        df = df.with_column("FILE_NAME", lit(file_name))

        df = df.withColumn("CRT_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        

        
        final_df = df

        if final_df.count()==0:
            return "No Data in file"
        
        # Load Data to the target table
        final_df.write.mode("append").saveAsTable(target_table)

        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")

        # write to success folder
    
        file_name=file_name.split(".")[0] + '_' + datetime.now().strftime("%Y%m%d%H%M%S")
        final_df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,file_format_type="csv",OVERWRITE=True,header=True)
   
        return "Success"
        
    except KeyError as key_error:
        
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message
        
    except Exception as e:
        
        error_message = f"Error: {str(e)}" +str(df.columns)
        return error_message
';
