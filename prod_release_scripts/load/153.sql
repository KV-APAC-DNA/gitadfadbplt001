USE SCHEMA THASDL_RAW;

CREATE OR REPLACE PROCEDURE THASDL_RAW.LA_GT_CUSTOMER_PREPROCESSING("PARAM" ARRAY)
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

        #Param=[''A1_LAO2403232330.txt'',''THASDL_RAW.DEV_LOAD_STAGE_ADLS'',''dev/LCM/master/Laos_Customer_Data/'',''sdl_la_gt_customer'']

        # Extracting parameters from the input
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        

        # Define the schema for the DataFrame
        df_schema=StructType([
            StructField("DISTRIBUTORID",StringType()),
            StructField("ARCODE",StringType()),
            StructField("ARNAME",StringType()),
            StructField("ARADDRESS",StringType()),
            StructField("TELEPHONE",StringType()),
            StructField("FAX",StringType()),
            StructField("CITY",StringType()),
            StructField("REGION",StringType()),
            StructField("SALEDISTRICT",StringType()),
            StructField("SALEOFFICE",StringType()),
            StructField("SALEGROUP",StringType()),
            StructField("ARTYPECODE",StringType()),
            StructField("SALEEMPLOYEE",StringType()),
            StructField("SALENAME",StringType()),
            StructField("BILLNO",StringType()),
            StructField("BILLMOO",StringType()),
            StructField("BILLSOI",StringType()),
            StructField("BILLROAD",StringType()),
            StructField("BILLSUBDIST",StringType()),
            StructField("BILLDISTRICT",StringType()),
            StructField("BILLPROVINCE",StringType()),
            StructField("BILLZIPCODE",StringType()),
            StructField("ACTIVESTATUS",IntegerType()),
            StructField("ROUTESTEP1",StringType()),
            StructField("ROUTESTEP2",StringType()),
            StructField("ROUTESTEP3",StringType()),
            StructField("ROUTESTEP4",StringType()),
            StructField("ROUTESTEP5",StringType()),
            StructField("ROUTESTEP6",StringType()),
            StructField("ROUTESTEP7",StringType()),
            StructField("LATITUDE",StringType()),
            StructField("LONGITUDE",StringType()),
            StructField("ROUTESTEP10",StringType()),
            StructField("STORE",StringType()),
            StructField("PRICELEVEL",StringType()),
            StructField("SALESAREANAME",StringType()),
            StructField("BRANCHCODE",StringType()),
            StructField("BRANCHNAME",StringType()),
            StructField("FREQUENCYOFVISIT",StringType())
            ])


        # Read the CSV file into a DataFrame
    
        dataframe = session.read\\
            .schema(df_schema)\\
            .option("skip_header",0)\\
            .option("field_delimiter", "\\t")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
        
        #---------------------------Transformation logic ------------------------------#

        dataframe=dataframe.na.drop("all")
        
        if dataframe.count()==0:
            return "No Data in file"

        # Add File name, run_id and crt_dttm to the dataframe
        run_id=datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        new_file_name=file_name.split(".")[0]+''_''+run_id+''.txt''
        dataframe = dataframe.withColumn("FILENAME",lit(new_file_name).cast("string"))
        dataframe = dataframe.with_column("RUN_ID",lit(run_id))
        dataframe = dataframe.with_column("CRT_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))

        # Creating copy of the Dataframe
        final_df = dataframe.alias("final_df")

        # Load Data to the target table
        final_df.write.mode("append").saveAsTable(target_table)

        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")

        # write to success folder
    
        file_name=file_name.split(".")[0]+''_''+run_id
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


CREATE OR REPLACE PROCEDURE THASDL_RAW.LA_GT_INVENTORY_PREPROCESSING("PARAM" ARRAY)
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

        #Param=[''I_LAO2403122340_20240313040259.txt'',''THASDL_RAW.DEV_LOAD_STAGE_ADLS'',''dev/cert_data_lake/LCM/Laos_Inventory_Data/'',''sdl_la_gt_inventory_fact'']

        # Extracting parameters from the input
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        

        # Define the schema for the DataFrame
        df_schema=StructType([
            StructField("RECDATE",StringType()),
            StructField("DISTRIBUTORID",StringType()),
            StructField("WHCODE",StringType()),
            StructField("PRODUCTCODE",StringType()),
            StructField("QTY",StringType()),
            StructField("AMOUNT",StringType()),
            StructField("BATCHNO",StringType()),
            StructField("EXPIRYDATE",StringType())
            ])


        # Read the CSV file into a DataFrame
    
        dataframe = session.read\\
            .schema(df_schema)\\
            .option("skip_header",0)\\
            .option("field_delimiter", "\\t")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        #---------------------------Transformation logic ------------------------------#

        # Handle null values or empty rows

        dataframe=dataframe.na.drop("all")
        
        # Check for empty Dataframe
        if dataframe.count()==0:
            return "No Data in file"

        # Add File name, run_id and crt_dttm to the dataframe
        run_id=datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        new_file_name=file_name.split(".")[0]+''_''+run_id+''.txt''
        dataframe = dataframe.withColumn("FILENAME",lit(new_file_name).cast("string"))
        dataframe = dataframe.with_column("RUN_ID",lit(run_id))
        dataframe = dataframe.with_column("CRT_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))

        # Creating copy of the Dataframe
        final_df = dataframe.alias("final_df")


        # Load Data to the target table
        final_df.write.mode("append").saveAsTable(target_table)

        
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")

        # write to success folder
    
        file_name=file_name.split(".")[0]+''_''+run_id
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


CREATE OR REPLACE PROCEDURE THASDL_RAW.LA_GT_ROUTE_DTL_PREPROCESSING("PARAM" ARRAY)
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
from snowflake.snowpark.functions import col,lit,md5,trim,coalesce,upper,concat,to_timestamp,to_date
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField
import pandas as pd
from datetime import datetime
import pytz

def main(session: snowpark.Session,Param): 

    try:

        #Param=[''RouteDtl_LAO_20240313232501_20240314040335.txt'',''THASDL_RAW.DEV_LOAD_STAGE_ADLS'',''dev/LCM/master/Laos_Route_Dtl_Data/'',''SDL_LA_GT_ROUTE_DETAIL'']

        # Extracting parameters from the input
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]


        # Define the schema for the DataFrame
        df_schema=StructType([
            StructField("ROUTE_ID",StringType()),
            StructField("CUSTOMER_ID",StringType()),
            StructField("ROUTE_NO",StringType()),
            StructField("SALEUNIT",StringType()),
            StructField("SHIP_TO",StringType()),
            StructField("CONTACT_PERSON",StringType()),
            StructField("CREATED_DATE",StringType())
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

        fileuploadeddate = file_name.split("_")[2][0:8]
        run_id=datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")

         # Add HASHKEY, FILE_UPLOADED_DATE, File name, run_id and crt_dttm to the dataframe
        
        dataframe = dataframe.withColumn("HASHKEY", 
            md5(concat( \\
                    coalesce(upper(trim(col("SALEUNIT"))), lit(''N/A'')) , \\
                    coalesce(upper(trim(col("CUSTOMER_ID"))), lit(''N/A'')), \\
                    coalesce(upper(trim(col("ROUTE_ID"))), lit(''N/A'')), \\
                    coalesce(upper(trim(col("ROUTE_NO"))), lit(''N/A'')) \\
                ) \\
            ) \\
        )

        dataframe = dataframe.withColumn("FILE_UPLOADED_DATE", to_date(lit(fileuploadeddate),"YYYYMMDD"))
        new_file_name=file_name.split(".")[0]+''_''+run_id+''.txt''
        dataframe = dataframe.withColumn("FILENAME",lit(new_file_name).cast("string"))
        dataframe = dataframe.with_column("RUN_ID",lit(run_id).cast("string"))
        dataframe = dataframe.with_column("CRT_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        
        # Creating copy of the Dataframe
        final_df = dataframe.select("HASHKEY", "ROUTE_ID", "CUSTOMER_ID", "ROUTE_NO", "SALEUNIT","SHIP_TO",\\
                                    "CONTACT_PERSON","CREATED_DATE","FILE_UPLOADED_DATE",\\
                                    "FILENAME","RUN_ID","CRT_DTTM")

        # Load Data to the target table
        final_df.write.mode("append").saveAsTable(target_table)

         
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")

        # write to success folder
    
        file_name=file_name.split(".")[0]+''_''+run_id
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

CREATE OR REPLACE PROCEDURE THASDL_RAW.LA_GT_ROUTE_HDR_PREPROCESSING("PARAM" ARRAY)
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
from snowflake.snowpark.functions import col,lit,md5,trim,coalesce,upper,concat,to_timestamp,to_date
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField
import pandas as pd
from datetime import datetime
import pytz

def main(session: snowpark.Session,Param): 

    try:

        #Param=[''RouteHdr_LAO_20240327232501.txt'',''THASDL_RAW.DEV_LOAD_STAGE_ADLS'',''dev/LCM/master/Laos_Route_Hdr_Data/'',''SDL_LA_GT_ROUTE_HEADER'']

        # Extracting parameters from the input
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        

        # Define the schema for the DataFrame
        df_schema=StructType([
            StructField("ROUTE_ID",StringType()),
            StructField("ROUTE_NAME",StringType()),
            StructField("ROUTE_DESC",StringType()),
            StructField("IS_ACTIVE",StringType()),
            StructField("ROUTESALE",StringType()),
            StructField("SALEUNIT",StringType()),
            StructField("ROUTE_CODE",StringType()),
            StructField("DESCRIPTION",StringType()),
            StructField("LAST_UPDATED_DATE",StringType())
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

        fileuploadeddate = file_name.split("_")[2][0:8]
        run_id=datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")

         # Add HASHKEY, FILE_UPLOADED_DATE, File name, run_id and crt_dttm to the dataframe
        
        dataframe = dataframe.withColumn("HASHKEY", 
            md5(concat( \\
                    coalesce(upper(trim(col("SALEUNIT"))), lit(''N/A'')) , \\
                    coalesce(upper(trim(col("ROUTESALE"))), lit(''N/A'')), \\
                    coalesce(upper(trim(col("ROUTE_ID"))), lit(''N/A'')) \\
                ) \\
            ) \\
        )

        dataframe = dataframe.withColumn("FILE_UPLOADED_DATE", to_date(lit(fileuploadeddate),"YYYYMMDD"))
        new_file_name=file_name.split(".")[0]+''_''+run_id+''.txt''
        dataframe = dataframe.withColumn("FILENAME",lit(new_file_name).cast("string"))
        dataframe = dataframe.with_column("RUN_ID",lit(run_id).cast("string"))
        dataframe = dataframe.with_column("CRT_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))

        # Creating copy of the Dataframe
        final_df = dataframe.select("HASHKEY", "ROUTE_ID", "ROUTE_NAME", "ROUTE_DESC", "IS_ACTIVE","ROUTESALE",\\
                                    "SALEUNIT","ROUTE_CODE","DESCRIPTION","LAST_UPDATED_DATE","FILE_UPLOADED_DATE",\\
                                    "FILENAME","RUN_ID","CRT_DTTM")
        
        # Load Data to the target table
        final_df.write.mode("append").saveAsTable(target_table)


        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")

        # write to success folder
    
        file_name=file_name.split(".")[0]+''_''+run_id
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

CREATE OR REPLACE PROCEDURE THASDL_RAW.LA_GT_SALES_ORDER_PREPROCESSING("PARAM" ARRAY)
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
from snowflake.snowpark import dataframe
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField,DecimalType,DateType
import pandas as pd
from datetime import datetime
import pytz

def main(session: snowpark.Session,Param): 

    try:

        #Param=[''SO_LAO_20240312232537_20240313040259.txt'',''THASDL_RAW.DEV_LOAD_STAGE_ADLS'',''dev/cert_data_lake/LCM/Laos_Sales_Order_Data/'',''sdl_la_gt_sales_order_fact'']

        # Extracting parameters from the input
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]

        # Define the schema for the DataFrame
        df_schema=StructType([
            StructField("SALEUNIT",StringType()),
            StructField("ORDERID",StringType()),
            StructField("ORDERDATE",StringType()),
            StructField("CUSTOMER_ID",StringType()),
            StructField("CUSTOMER_NAME",StringType()),
            StructField("CITY",StringType()),
            StructField("REGION",StringType()),
            StructField("SALEDISTRICT",StringType()),
            StructField("SALEOFFICE",StringType()),
            StructField("SALEGROUP",StringType()),
            StructField("CUSTOMERTYPE",StringType()),
            StructField("STORETYPE",StringType()),
            StructField("SALETYPE",StringType()),
            StructField("SALESEMPLOYEE",StringType()),
            StructField("SALENAME",StringType()),
            StructField("PRODUCTID",StringType()),
            StructField("PRODUCTNAME",StringType()),
            StructField("MEGABRAND",StringType()),
            StructField("BRAND",StringType()),
            StructField("BASEPRODUCT",StringType()),
            StructField("VARIANT",StringType()),
            StructField("PUTUP",StringType()),
            StructField("PRICEREF",StringType()),
            StructField("BACKLOG",StringType()),
            StructField("QTY",StringType()),
            StructField("SUBAMT1",StringType()),
            StructField("DISCOUNT",StringType()),
            StructField("SUBAMT2",StringType()),
            StructField("DISCOUNTBTLINE",StringType()),
            StructField("TOTALBEFOREVAT",StringType()),
            StructField("TOTAL",StringType()),
            StructField("NO",StringType()),
            StructField("CANCELED",StringType()),
            StructField("DOCUMENTID",StringType()),
            StructField("RETURN_REASON",StringType()),
            StructField("PROMOTIONCODE",StringType()),
            StructField("PROMOTIONCODE1",StringType()),
            StructField("PROMOTIONCODE2",StringType()),
            StructField("PROMOTIONCODE3",StringType()),
            StructField("PROMOTIONCODE4",StringType()),
            StructField("PROMOTIONCODE5",StringType()),
            StructField("PROMOTION_CODE",StringType()),
            StructField("PROMOTION_CODE2",StringType()),
            StructField("PROMOTION_CODE3",StringType()),
            StructField("AVGDISCOUNT",IntegerType()),
            StructField("ORDERTYPE",StringType()),
            StructField("APPROVERSTATUS",StringType()),
            StructField("PRICELEVEL",StringType()),
            StructField("OPTIONAL3",StringType()),
            StructField("DELIVERYDATE",StringType()),
            StructField("ORDERTIME",StringType()),
            StructField("SHIPTO",StringType()),
            StructField("BILLTO",StringType()),
            StructField("DELIVERYROUTEID",StringType()),
            StructField("APPROVED_DATE",StringType()),
            StructField("APPROVED_TIME",StringType()),
            StructField("REF_15",StringType()),
            StructField("PAYMENTTYPE",StringType())
            ])


        # Read the CSV file into a DataFrame
    
        dataframe = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "|")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        #---------------------------Transformation logic ------------------------------#

        # Handle null values or empty rows
        dataframe=dataframe.na.drop("all")

        # Check for empty Dataframe
        if dataframe.count()==0:
            return "No Data in file"

        # Add File name, run_id and crt_dttm to the dataframe
        run_id=datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        new_file_name=file_name.split(".")[0]+''_''+run_id+''.txt''
        dataframe = dataframe.withColumn("FILENAME",lit(new_file_name).cast("string"))
        dataframe = dataframe.with_column("RUN_ID",lit(run_id).cast("string"))
        dataframe = dataframe.with_column("CRT_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))

        # Creating copy of the Dataframe
        final_df = dataframe.alias("final_df")
       
        # Load Data to the target table
        final_df.write.mode("append").saveAsTable(target_table)

        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")

        # write to success folder
    
        file_name=file_name.split(".")[0]+''_''+run_id
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


CREATE OR REPLACE PROCEDURE THASDL_RAW.LA_GT_SCHEDULE_PREPROCESSING("PARAM" ARRAY)
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
from snowflake.snowpark.functions import col,lit,md5,trim,coalesce,upper,concat,to_timestamp,to_date
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField
import pandas as pd
from datetime import datetime
import pytz

def main(session: snowpark.Session,Param): 

    try:

        #Param=[''Schedule_20240317232501_20240318040301.txt'',''THASDL_RAW.DEV_LOAD_STAGE_ADLS'',''dev/cert_data_lake/LCM/Laos_Schedule_Data/'',''SDL_LA_GT_SCHEDULE'']

        # Extracting parameters from the input
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]


        # Define the schema for the DataFrame
        df_schema=StructType([
            StructField("EMPLOYEE_ID",StringType()),
            StructField("ROUTE_ID",StringType()),
            StructField("SCHEDULE_DATE",StringType()),
            StructField("APPROVED",StringType()),
            StructField("SALEUNIT",StringType())
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


        # Add File name, run_id and crt_dttm to the dataframe
        
        run_id=datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        new_file_name=file_name.split(".")[0]+''_''+run_id+''.txt''
        
        dataframe = dataframe.withColumn("FILENAME",lit(new_file_name).cast("string"))
        dataframe = dataframe.with_column("RUN_ID",lit(run_id).cast("string"))
        dataframe = dataframe.with_column("CRT_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))

        # Creating copy of the Dataframe
        final_df = dataframe.alias("final_df")

        # Load Data to the target table
        final_df.write.mode("append").saveAsTable(target_table)

        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")

        # write to success folder
    
        file_name=file_name.split(".")[0]+''_''+run_id
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


CREATE OR REPLACE PROCEDURE THASDL_RAW.LA_GT_SELLOUT_PREPROCESSING("PARAM" ARRAY)
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

        #Param=[''S_LAO2403132335_20240314040335.txt'',''THASDL_RAW.DEV_LOAD_STAGE_ADLS'',''dev/cert_data_lake/LCM/Laos_Sellout_Data/'',''SDL_LA_GT_SELLOUT_FACT'']

        # Extracting parameters from the input
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        

        # Define the schema for the DataFrame
        df_schema=StructType([
            StructField("DISTRIBUTORID",StringType()),
            StructField("ORDERNO",StringType()),
            StructField("ORDERDATE",StringType()),
            StructField("ARCODE",StringType()),
            StructField("ARNAME",StringType()),
            StructField("CITY",StringType()),
            StructField("REGION",StringType()),
            StructField("SALEDISTRICT",StringType()),
            StructField("SALEOFFICE",StringType()),
            StructField("SALEGROUP",StringType()),
            StructField("ARTYPECODE",StringType()),
            StructField("SALEEMPLOYEE",StringType()),
            StructField("SALENAME",StringType()),
            StructField("PRODUCTCODE",StringType()),
            StructField("PRODUCTDESC",StringType()),
            StructField("MEGABRAND",StringType()),
            StructField("BRAND",StringType()),
            StructField("BASEPRODUCT",StringType()),
            StructField("VARIANT",StringType()),
            StructField("PUTUP",StringType()),
            StructField("GROSSPRICE",IntegerType()),
            StructField("QTY",IntegerType()),
            StructField("SUBAMT1",IntegerType()),
            StructField("DISCOUNT",IntegerType()),
            StructField("SUBAMT2",IntegerType()),
            StructField("DISCOUNTBTLINE",IntegerType()),
            StructField("TOTALBEFOREVAT",IntegerType()),
            StructField("TOTAL",IntegerType()),
            StructField("LINENUMBER",IntegerType()),
            StructField("ISCANCEL",IntegerType()),
            StructField("CNDOCNO",StringType()),
            StructField("CNREASONCODE",StringType()),
            StructField("PROMOTIONHEADER1",StringType()),
            StructField("PROMOTIONHEADER2",StringType()),
            StructField("PROMOTIONHEADER3",StringType()),
            StructField("PROMODESC1",StringType()),
            StructField("PROMODESC2",StringType()),
            StructField("PROMODESC3",StringType()),
            StructField("PROMOCODE1",StringType()),
            StructField("PROMOCODE2",StringType()),
            StructField("PROMOCODE3",StringType()),
            StructField("AVGDISCOUNT",IntegerType())
            ])


        # Read the CSV file into a DataFrame
    
        dataframe = session.read\\
            .schema(df_schema)\\
            .option("skip_header",0)\\
            .option("field_delimiter", "\\t")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
        

        #---------------------------Transformation logic ------------------------------#

        # Handle null values or empty rows
        dataframe=dataframe.na.drop("all")

        # Check for empty Dataframe
        if dataframe.count()==0:
            return "No Data in file"

        # Add File name, run_id and crt_dttm to the dataframe
        run_id=datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        new_file_name=file_name.split(".")[0]+''_''+run_id+''.txt''
        dataframe = dataframe.withColumn("FILENAME",lit(new_file_name).cast("string"))
        dataframe = dataframe.with_column("RUN_ID",lit(run_id).cast("string"))
        dataframe = dataframe.with_column("CRT_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))

        # Creating copy of the Dataframe
        final_df = dataframe.alias("final_df")

        # Load Data to the target table
        final_df.write.mode("append").saveAsTable(target_table)

        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")

        # write to success folder
    
        file_name=file_name.split(".")[0]+''_''+run_id
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

CREATE OR REPLACE PROCEDURE THASDL_RAW.LA_GT_VISIT_PREPROCESSING("PARAM" ARRAY)
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
from snowflake.snowpark.functions import col,lit,md5,trim,coalesce,upper,concat,to_timestamp,to_date
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField
import pandas as pd
from datetime import datetime
import pytz

def main(session: snowpark.Session,Param): 

    try:

        #Param=[''Visit_20240317232501_20240318040301.txt'',''THASDL_RAW.DEV_LOAD_STAGE_ADLS'',''dev/cert_data_lake/LCM/Laos_Visit_Data/'',''SDL_LA_GT_VISIT'']

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


        # Add File name, run_id and crt_dttm to the dataframe
        run_id=datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        new_file_name=file_name.split(".")[0]+''_''+run_id+''.txt''
        
        dataframe = dataframe.withColumn("FILENAME",lit(new_file_name).cast("string"))
        dataframe = dataframe.with_column("RUN_ID",lit(run_id).cast("string"))
        dataframe = dataframe.with_column("CRT_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))

        # Creating copy of the Dataframe
        final_df = dataframe.alias("final_df")

        
        # Load Data to the target table
        final_df.write.mode("append").saveAsTable(target_table)

        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")

        # write to success folder
    
        file_name=file_name.split(".")[0]+''_''+run_id
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
