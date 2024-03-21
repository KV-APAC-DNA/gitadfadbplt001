create or replace stage THASDL_RAW.PROD_LOAD_STAGE_ADLS
 storage_integration = PROD_DNA_LOAD_AZURE34_SI
 url = 'azure://dlsadbplt001.blob.core.windows.net/tha/';

USE SCHEMA THASDL_RAW;
CREATE OR REPLACE PROCEDURE CAMBODIA_DAILY_SALES_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS 'from snowflake.snowpark.functions import col, lit, date_format, current_timestamp, to_date, year, month, concat, format_number, regexp_replace,to_timestamp,when,trim,upper
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DecimalType,DateType
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz
import os


def main(session: snowpark.Session,Param):
    #Param = ["Daily_Sales_Report_RAW_Apr_2023_20230927040049.csv","THASDL_RAW.DEV_LOAD_STAGE_ADLS", "dev/cert_data_lake/LCM/LCM_Cambodia_Sales_Data","SDL_CBD_GT_SALES_REPORT_FACT"]
    try:
        file_name1 = Param[0]
        file_name=file_name1.replace("RAW","(RAW)")
        stage_name = Param[1]
        temp_stage_path = Param[2]
        target_table = Param[3]

        df_schema = StructType([
                        StructField("BU", StringType(), True),
                        StructField("Client", StringType(), True),
                        StructField("Sub_Client", StringType(), True),
                        StructField("Product_Code", StringType(), True),
                        StructField("Product_Name", StringType(), True),
                        StructField("Billing_No", StringType(), True),
                        StructField("Billing_Date", StringType(), True),
                        StructField("Batch_No", StringType(), True),
                        StructField("Expiry_Date", StringType(), True),
                        StructField("Customer_Code", StringType(), True),
                        StructField("Customer_Name", StringType(), True),
                        StructField("Distribution_Channel", StringType(), True),
                        StructField("Customer_Group", StringType(), True),
                        StructField("Province", StringType(), True),
                        StructField("Sales_Qty", StringType(), True),
                        StructField("FOC_Qty", StringType(), True),
                        StructField("Net_Price", StringType(), True),
                        StructField("Net_Sales", StringType(), True),
                        StructField("Sales_Rep_No", StringType(), True),
                        StructField("Order_No", StringType(), True),
                        StructField("Return_Reason", StringType(), True),
                        StructField("Payment_Term", StringType(), True)
                    ])
        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",3)\\
            .option("field_delimiter", "\\u0001")\\
            .option("field_optionally_enclosed_by", "\\"") \\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name1)
        df=df.na.drop("all")
        if df.count()==0 :
            return "No Data in file"
        
        #file_name1 = ("_").join(file_name.split("_")[0:6])+".csv"
        
        df = df.with_column("run_id", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        df = df.with_column("file_name", lit(file_name))
        df = df.with_column("crtd_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        
        snowdf = df.select(
                "Bu", "Client", "Sub_Client", "Product_Code", "Product_Name", "Billing_No", "Billing_Date", "Batch_No",
                "Expiry_Date", "Customer_Code", "Customer_Name", "Distribution_Channel", "Customer_Group", "Province",
                "Sales_Qty", "FOC_Qty", "Net_Price", "Net_Sales", "Sales_Rep_No", "Order_No", "Return_Reason",
                "Payment_Term", "file_name", "run_id", "crtd_Dttm"
                )
        
    
        
        # Move file into success folder
        file_name=file_name.split(".")[0]+"_"+datetime.now().strftime("%Y%m%d%H%M%S")
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        
        #move to success
        snowdf.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)
        
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
        return error_message
';
CREATE OR REPLACE PROCEDURE CAMBODIA_INVENTORY_REPORT_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS 'from snowflake.snowpark.functions import col, lit, date_format, current_timestamp, to_date, year, month, concat, format_number, regexp_replace,to_timestamp,when,trim,upper
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DecimalType,DateType
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz


def main(session: snowpark.Session,Param): 
    #Param = ["Stock_Aging_Report_End_Sep_2023.csv", "THASDL_RAW.DEV_LOAD_STAGE_ADLS", "dev/cert_data_lake/LCM/LCM_Cambodia_inventory_Data", "SDL_CBD_GT_INVENTORY_REPORT_FACT"]
    try:
        file_name1       = Param[0]
        split_name=file_name1.split("_")
        file_name = ("_").join(split_name[0:3])+"_"+datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")+".csv"
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        
        df_schema = StructType([
                    StructField("date", StringType()),
                    StructField("clientcd_name", StringType()),
                    StructField("PRODUCT_CODE", StringType()),
                    StructField("product_name", StringType()),
                    StructField("baseUOM", StringType()),
                    StructField("expired", StringType()),
                    StructField("1-90days", StringType()),
                    StructField("91-180days", StringType()),
                    StructField("181-365days", StringType()),
                    StructField(">365days", StringType()),
                    StructField("total_qty", StringType())
                ])

        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",3)\\
            .option("field_delimiter", "\\u0001")\\
            .option("field_optionally_enclosed_by", "\\"") \\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name1)
        df=df.na.drop("all")
        if df.count()==0 :
            return "No Data in file" 

        df = df.with_column("run_id", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S"))) 
        df=df.with_column("file_name",lit(file_name))
        df=df.with_column("crtd_dttm",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))


        
        snowdf=df.select( "date", "clientcd_name", "product_code", "product_name", "baseUOM", "expired",
                            "1-90days", "91-180days", "181-365days", ">365days", "total_qty",
                            "file_name", "run_id", "crtd_dttm")
        

            
        
        #move file into success folder
        file_name=("_").join(split_name[0:3])+"_"+datetime.now().strftime("%Y%m%d%H%M%S")
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

CREATE OR REPLACE PROCEDURE LA_GT_CUSTOMER_PREPROCESSING("PARAM" ARRAY)
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

        #Param=[''A1_LAO2310272330_20231028040039.txt'',''THASDL_RAW.DEV_LOAD_STAGE_ADLS'',''dev/cert_data_lake/LCM/Laos_Customer_Data/'',''sdl_la_gt_customer'']

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
        run_id=file_name.split(".")[0].split("_")[2]
        dataframe = dataframe.withColumn("FILENAME",lit(file_name).cast("string"))
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
    
        file_name=file_name.split(".")[0]
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
CREATE OR REPLACE PROCEDURE LA_GT_INVENTORY_PREPROCESSING("PARAM" ARRAY)
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
        run_id=file_name.split(".")[0].split("_")[2]
        dataframe = dataframe.withColumn("FILENAME",lit(file_name).cast("string"))
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
    
        file_name=file_name.split(".")[0]
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
CREATE OR REPLACE PROCEDURE LA_GT_ROUTE_DTL_PREPROCESSING("PARAM" ARRAY)
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
        run_id=file_name.split(".")[0].split("_")[3]

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
        dataframe = dataframe.withColumn("FILENAME",lit(file_name).cast("string"))
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
    
        file_name=file_name.split(".")[0]
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
CREATE OR REPLACE PROCEDURE LA_GT_ROUTE_HDR_PREPROCESSING("PARAM" ARRAY)
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

        #Param=[''RouteHdr_LAO_20240313232501_20240314040335.txt'',''THASDL_RAW.DEV_LOAD_STAGE_ADLS'',''dev/cert_data_lake/LCM/Laos_Route_Hdr_Data/'',''SDL_LA_GT_ROUTE_HEADER'']

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
        run_id=file_name.split(".")[0].split("_")[3]

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
        dataframe = dataframe.withColumn("FILENAME",lit(file_name).cast("string"))
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
    
        file_name=file_name.split(".")[0]
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
CREATE OR REPLACE PROCEDURE LA_GT_SALES_ORDER_PREPROCESSING("PARAM" ARRAY)
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
        run_id=file_name.split(".")[0].split("_")[3]
        dataframe = dataframe.withColumn("FILENAME",lit(file_name).cast("string"))
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
    
        file_name=file_name.split(".")[0]
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
CREATE OR REPLACE PROCEDURE LA_GT_SCHEDULE_PREPROCESSING("PARAM" ARRAY)
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
        run_id=file_name.split(".")[0].split("_")[2]

        dataframe = dataframe.withColumn("FILENAME",lit(file_name).cast("string"))
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
    
        file_name=file_name.split(".")[0]
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
CREATE OR REPLACE PROCEDURE LA_GT_SELLOUT_PREPROCESSING("PARAM" ARRAY)
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
        run_id=file_name.split(".")[0].split("_")[2]
        dataframe = dataframe.withColumn("FILENAME",lit(file_name).cast("string"))
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
    
        file_name=file_name.split(".")[0]
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
