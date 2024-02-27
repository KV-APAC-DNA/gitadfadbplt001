-- create or replace stage MYSSDL_RAW.PROD_LOAD_STAGE_ADLS
-- storage_integration = PROD_DNA_LOAD_AZURE28_SI
-- url = 'azure://dlsadbplt001.blob.core.windows.net/mys/';


CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.MYSSDL_RAW.JOINT_MONTHLY_INV_PREPROCESSING("PARAM" ARRAY)
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
    #Param=["MY_GT_Distributor_Integrated_Inv_202312.csv","MYSSDL_RAW.DEV_LOAD_STAGE_ADLS","dev/transaction/sellout/distributor_integrated","SDL_MY_MONTHLY_SELLOUT_STOCK_FACT"]
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]

        df_schema = StructType([
            StructField("Distributor_ID", StringType()),
            StructField("DATE", StringType()),
            StructField("Distributor_WH_ID", StringType()),
            StructField("SAP_Material_ID", StringType()),
            StructField("Product_Code", StringType()),
            StructField("Product_EAN_Code", StringType()),
            StructField("Product_Description", StringType()),
            StructField("Quantity_Available", StringType()),
            StructField("UOM_Available", StringType()),
            StructField("Quantity_ON_Order", StringType()),
            StructField("UOM_ON_Order", StringType()),
            StructField("Quantity_Committed", StringType()),
            StructField("UOM_Committed", StringType()),
            StructField("Quantity_Available_IN_Pieces", StringType()),
            StructField("Quantity_ON_Order_IN_Pieces", StringType()),
            StructField("Quantity_Committed_IN_Pieces", StringType()),
            StructField("Unit_Price", StringType()),
            StructField("Total_Value_Available", StringType()),
            StructField("Custom_Field_1", StringType()),
            StructField("Custom_Field_2", StringType())
        ])

        df = session.read\\
        .schema(df_schema)\\
        .option("skip_header",0)\\
        .option("field_delimiter", ",")\\
        .option("encoding", "ISO-8859-15") \\
        .option("field_optionally_enclosed_by", "\\"") \\
        .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)


        #df = df.with_column("run_id", lit(None).cast(DecimalType(14,0)))
        df = df.with_column("cdl_dttm", lit(datetime.now().strftime("%Y%m%d%H%M%S")))
        df=df.with_column("file_name",lit(file_name))
        df = df.withColumn("crtd_dttm", lit(datetime.now().strftime("%Y-%m-%d %H:%M:%S")))

        snowdf=df.select("Distributor_ID",
                        "DATE",
                        "Distributor_WH_ID",
                        "SAP_Material_ID",
                        "Product_Code",
                        "Product_EAN_Code",
                        "Product_Description",
                        "Quantity_Available",
                        "UOM_Available",
                        "Quantity_ON_Order",
                        "UOM_ON_Order",
                        "Quantity_Committed",
                        "UOM_Committed",
                        "Quantity_Available_IN_Pieces",
                        "Quantity_ON_Order_IN_Pieces",
                        "Quantity_Committed_IN_Pieces",
                        "Unit_Price",
                        "Total_Value_Available",
                        "Custom_Field_1",
                        "Custom_Field_2",
                        "file_name",
                        "cdl_dttm",
                        "crtd_dttm")

        snowdf= snowdf.filter(snowdf["Product_Code"].isNotNull())
        
        
        if snowdf.count()==0 :
            return "No Data in table"

        
        file_name=("_").join(file_name.split("_")[0:5])+"_"+datetime.now().strftime("%Y%m%d%H%M%S")
        snowdf.write.copy_into_location("@"+Param[1]+"/"+Param[2]+"/success/"+file_name,header=True,OVERWRITE=True)
        

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
		
CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.MYSSDL_RAW.JOINT_MONTHLY_SALES_PREPROCESSING("PARAM" ARRAY)
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
    #Param=["MY_GT_Distributor_Integrated_Sales_202312.csv","MYSSDL_RAW.DEV_LOAD_STAGE_ADLS","dev/transaction/sellout/distributor_integrated","SDL_MY_MONTHLY_SELLOUT_SALES_FACT"]
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]

        df_schema = StructType([
            StructField("Distributor_ID", StringType()),
            StructField("Sales_Order_Number", StringType()),
            StructField("Sales_Order_Date", StringType()),
            StructField("TYPE", StringType()),
            StructField("Customer_Code", StringType()),
            StructField("Distributor_WH_ID", StringType()),
            StructField("SAP_Material_ID", StringType()),
            StructField("Product_Code", StringType()),
            StructField("Product_EAN_Code", StringType()),
            StructField("Product_Description", StringType()),
            StructField("Gross_Item_Price", StringType()),
            StructField("Quantity", StringType()),
            StructField("UOM", StringType()),
            StructField("Quantity_in_Pieces", StringType()),
            StructField("Quantity_After_Conversion", StringType()),
            StructField("Sub_Total_1", StringType()),
            StructField("Discount", StringType()),
            StructField("Sub_Total_2", StringType()),
            StructField("Bottom_Line_Discount", StringType()),
            StructField("Total_Amt_After_Tax", StringType()),
            StructField("Total_Amt_Before_Tax", StringType()),
            StructField("Sales_Employee", StringType()),
            StructField("Custom_Field_1", StringType()),
            StructField("Custom_Field_2", StringType()),
            StructField("Custom_Field_3", StringType())
        ])

        df = session.read\\
        .schema(df_schema)\\
        .option("skip_header",1)\\
        .option("field_delimiter", ",")\\
        .option("encoding", "ISO-8859-15") \\
        .option("field_optionally_enclosed_by", "\\"") \\
        .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        #df = df.with_column("run_id", lit(None).cast(DecimalType(14,0)))
        df = df.with_column("cdl_dttm", lit(datetime.now().strftime("%Y%m%d%H%M%S")))
        df=df.with_column("file_name",lit(file_name))
        df = df.withColumn("crtd_dttm", current_timestamp())

        snowdf=df.select(
                            "Distributor_ID",
                            "Sales_Order_Number",
                            "Sales_Order_Date",
                            "TYPE",
                            "Customer_Code",
                            "Distributor_WH_ID",
                            "SAP_Material_ID",
                            "Product_Code",
                            "Product_EAN_Code",
                            "Product_Description",
                            "Gross_Item_Price",
                            "Quantity",
                            "UOM",
                            "Quantity_in_Pieces",
                            "Quantity_After_Conversion",
                            "Sub_Total_1",
                            "Discount",
                            "Sub_Total_2",
                            "Bottom_Line_Discount",
                            "Total_Amt_After_Tax",
                            "Total_Amt_Before_Tax",
                            "Sales_Employee",
                            "Custom_Field_1",
                            "Custom_Field_2",
                            "Custom_Field_3",
                            "file_name",
                            "cdl_dttm",
                            "crtd_dttm")
        
        snowdf= snowdf.filter(snowdf["Product_Code"].isNotNull())
        
        if snowdf.count()==0 :
            return "No Data in table"


        file_name1=("_").join(file_name.split("_")[0:5])+"_"+datetime.now().strftime("%Y%m%d%H%M%S")
        snowdf.write.copy_into_location("@"+Param[1]+"/"+Param[2]+"/success/"+file_name1,header=True,OVERWRITE=True)
        

        #write on sdl layer
    
        snowdf.write.mode("append").saveAsTable(stage_name.split(".")[0]+"."+target_table)

        #need to truncate the mds table and load again

        del_sql = "DELETE FROM " + stage_name.split(".")[0]+"."+target_table+"_mds_sync where filename=''"+ file_name +"''"
        session.sql(del_sql).collect()
        
        snowdf.write.mode("append").saveAsTable(stage_name.split(".")[0]+"."+target_table+"mds_sync")
        
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
		
CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.MYSSDL_RAW.MY_ACCRUAL_PREPROCESSING("PARAM" ARRAY)
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

        #Param=[''MY_Accrual.csv'',''MYSSDL_RAW.DEV_LOAD_STAGE_ADLS'',''dev/others/sellin/'',''SDL_MY_ACCRUALS'']

        # Extracting parameters from the input
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        

        # Define the schema for the DataFrame
        df_schema=StructType([
            StructField("CUST_ID",StringType()),
            StructField("CUST_NM",StringType()),
            StructField("JJ_YEAR",StringType()),
            StructField("FILE_TYPE",StringType()),
            StructField("JAN_VAL",StringType()),
            StructField("FEB_VAL",StringType()),
            StructField("MAR_VAL",StringType()),
            StructField("APR_VAL",StringType()),
            StructField("MAY_VAL",StringType()),
            StructField("JUN_VAL",StringType()),
            StructField("JUL_VAL",StringType()),
            StructField("AUG_VAL",StringType()),
            StructField("SEP_VAL",StringType()),
            StructField("OCT_VAL",StringType()),
            StructField("NOV_VAL",StringType()),
            StructField("DEC_VAL",StringType()),
            ])


        # Read the CSV file into a DataFrame
    
        dataframe = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "\\u0001")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
        

            #---------------------------Transformation logic ------------------------------#

         # Add CDL_DTTM and CURR_DT to the dataframe

        dataframe = dataframe.with_column("CDL_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))

        dataframe = dataframe.with_column("CURR_DT",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))


        # Creating copy of the Dataframe
        final_df = dataframe.alias("final_df")

        if final_df.count()==0:
            return "No Data in file"

        # Handle null values or empty rows
        
        final_df=final_df.filter(col("CUST_ID").is_not_null()) and final_df.filter(col("CUST_NM").is_not_null())

        # Load Data to the target table
        final_df.write.mode("append").saveAsTable(target_table)

        # write to success folder
    
        file_name=file_name.split(".")[0]+''_''+datetime.now().strftime("%Y%m%d%H%M%S")
        final_df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/success/"+file_name,file_format_type="csv",header=True,OVERWRITE=True)

        
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


CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.MYSSDL_RAW.MY_AFGR_PREPROCESSING("PARAM" ARRAY)
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
    # Your code goes here, inside the "main" handler.

    try:

        #Param=[''MY_AFGR.csv'',''MYSSDL_RAW.DEV_LOAD_STAGE_ADLS'',''dev/others/sellin/'',''SDL_MY_AFGR'']

        # Extracting parameters from the input
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        

        # Define the schema for the DataFrame
        df_schema=StructType([
            StructField("CUST_ID",StringType()),
            StructField("CUST_NM",StringType()),
            StructField("AFGR_NUM",StringType()),
            StructField("CUST_DN_NUM",StringType()),
            StructField("DN_AMT_EXC_GST_VAL",StringType()),
            StructField("AFGR_AMT",StringType()),
            StructField("DT_TO_SC",StringType()),
            StructField("SC_VALIDATION",StringType()),
            StructField("RTN_ORD_NUM",StringType()),
            StructField("RTN_ORD_DT",StringType()),
            StructField("RTN_ORD_AMT",StringType()),
            StructField("CN_EXP_ISSUE_DT",StringType()),
            StructField("BILL_NUM",StringType()),
            StructField("BILL_DT",StringType()),
            StructField("CN_AMT",StringType())
            ])


        # Read the CSV file into a DataFrame
    
        dataframe = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "\\u0001")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
        

    #---------------------------Transformation logic ------------------------------#

        # Add CDL_DTTM and CURR_DT to the dataframe

        dataframe = dataframe.with_column("CDL_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))

        dataframe = dataframe.with_column("CURR_DT",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))

        # Creating copy of the Dataframe
        final_df = dataframe.alias("final_df")

        if final_df.count()==0:
            return "No Data in file"

        # Handle null values or empty rows
        
        final_df=final_df.filter(col("CUST_ID").is_not_null()) and final_df.filter(col("AFGR_NUM").is_not_null())

        # Load Data to the target table
        final_df.write.mode("append").saveAsTable(target_table)

        # write to success folder
    
        file_name=file_name.split(".")[0]+''_''+datetime.now().strftime("%Y%m%d%H%M%S")
        final_df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/success/"+file_name,file_format_type="csv",header=True,OVERWRITE=True)

        
        return "Success"


    except KeyError as key_error:
        # Handle KeyError (missing columns) here
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message
        
    except Exception as e:
        # Handle exceptions here
        error_message = f"Error: {str(e)}"
        return error_message';
		
		
CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.MYSSDL_RAW.MY_AS_WATSONS_INV_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS CALLER
AS '

import snowflake.snowpark as snowpark
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DateType
from snowflake.snowpark.functions import col,lit,date_format,current_timestamp,to_timestamp,replace
import pandas as pd
from datetime import datetime
import pytz


def main(session: snowpark.Session, Param): 
    
    try:
        # Extracting parameters from the input

        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        db_name         = stage_name.split(''.'')[0]
        target_table    = db_name+"."+Param[3]
        
        # Define the schema for the DataFrame
        df_schema=StructType([
            StructField("CUSTOMER_CODE",StringType()),
            StructField("STORE_CODE",StringType()),
            StructField("YEAR",StringType()),
            StructField("MTH_CODE",StringType()),
            StructField("MATERIAL_CODE",StringType()),
            StructField("INV_QTY(PC)",StringType()),
            StructField("INV_VALUE(LP)",StringType())
            ])
        # Set the current session schema
       
        session.use_schema(stage_name.split(''.'')[0])
        
        # Read the CSV file into a DataFrame
        dataframe = session.read\\
        .schema(df_schema)\\
        .option("skip_header",1)\\
        .option("field_delimiter", "\\u0001")\\
        .option("field_optionally_enclosed_by", "\\"") \\
        .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
    
        # Add "FILENAME", "CRTD_DTTM" to the Dataframe

        dataframe = dataframe.withColumn("FILENAME",lit(file_name.replace(".csv",".xlsx")).cast("string"))
        
        #convertin time stamp into sg timezone
        dataframe = dataframe.with_column("CRTD_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        
        
        df = dataframe.select("CUSTOMER_CODE","STORE_CODE","YEAR",replace(col("MTH_CODE"), lit("/"), lit("")).alias("MTH_CODE"), "MATERIAL_CODE","INV_QTY(PC)","INV_VALUE(LP)", "FILENAME", "CRTD_DTTM")
        # Creating copy of the Dataframe
        final_df = df.alias("final_df")

        if final_df.count()==0:
            return "No Data in file"

        final_df=final_df.filter(col("CUSTOMER_CODE").is_not_null())

        
        # Load Data to the target table
        final_df.write.mode("append").saveAsTable(target_table)

    
        # write to success folder
    
        file_name=file_name.split(".")[0]+''_''+datetime.now().strftime("%Y%m%d%H%M%S")
        final_df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/success/"+file_name,file_format_type="csv",header=True,OVERWRITE=True)
        
        


        return "Success"

    except KeyError as key_error:
        # Handle KeyError (missing columns) here
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message
        
    except pd.errors.MergeError as merge_error:
        # Handle DataFrame merging error
        error_message = f"DataFrame merging error: {str(merge_error)}"
        return error_mess
    
    except Exception as e:
        # Handle exceptions here
        error_message = f"Error: {str(e)}"
        return error_message
';


CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.MYSSDL_RAW.MY_CIW_MAP_PREPROCESSING("PARAM" ARRAY)
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
    # Your code goes here, inside the "main" handler.

    try:

        #Param=[''MY_CIW.csv'',''MYSSDL_RAW.DEV_LOAD_STAGE_ADLS'',''dev/others/sellin/'',''SDL_MY_CIW_MAP'']

        # Extracting parameters from the input
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        

        # Define the schema for the DataFrame
        df_schema=StructType([
            StructField("CIW_CTGRY",StringType()),
            StructField("CIW_BUCKT1",StringType()),
            StructField("CIW_BUCKT2",StringType()),
            StructField("BRAVO_CD1",StringType()),
            StructField("BRAVO_DESC1",StringType()),
            StructField("BRAVO_CD2",StringType()),
            StructField("BRAVO_DESC2",StringType()),
            StructField("ACCT_TYPE",StringType()),
            StructField("ACCT_NUM",StringType()),
            StructField("ACCT_DESC",StringType()),
            StructField("ACCT_TYPE1",StringType())
            ])


        # Read the CSV file into a DataFrame
    
        dataframe = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "\\u0001")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
        

    #---------------------------Transformation logic ------------------------------#

        # Add CDL_DTTM and CURR_DT to the dataframe

        dataframe = dataframe.with_column("CDL_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))

        dataframe = dataframe.with_column("CURR_DT",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))

        # Creating copy of the Dataframe
        final_df = dataframe.alias("final_df")

        # Handle null values or empty rows
        
        final_df=final_df.filter(col("ACCT_TYPE").is_not_null()) and final_df.filter(col("ACCT_NUM").is_not_null())

        if final_df.count()==0:
            return "No Data in file"

        # Load Data to the target table
        final_df.write.mode("append").saveAsTable(target_table)

        # write to success folder
    
        file_name=file_name.split(".")[0]+''_''+datetime.now().strftime("%Y%m%d%H%M%S")
        final_df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/success/"+file_name,file_format_type="csv",header=True,OVERWRITE=True)

        
        return "Success"


    except KeyError as key_error:
        # Handle KeyError (missing columns) here
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message
        
    except Exception as e:
        # Handle exceptions here
        error_message = f"Error: {str(e)}"
        return error_message';
		

CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.MYSSDL_RAW.MY_JOINT_MONTHLY_INV_PREPROCESSING("PARAM" ARRAY)
RETURNS TABLE ()
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
    #Param=["MY_GT_Distributor_Integrated_Inv_202312.csv","MYSSDL_RAW.DEV_LOAD_STAGE_ADLS","dev/transaction/sellout/distributor_integrated","SDL_MY_MONTHLY_SELLOUT_STOCK_FACT"]
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]

        df_schema = StructType([
            StructField("Distributor_ID", StringType()),
            StructField("DATE", StringType()),
            StructField("Distributor_WH_ID", StringType()),
            StructField("SAP_Material_ID", StringType()),
            StructField("Product_Code", StringType()),
            StructField("Product_EAN_Code", StringType()),
            StructField("Product_Description", StringType()),
            StructField("Quantity_Available", StringType()),
            StructField("UOM_Available", StringType()),
            StructField("Quantity_ON_Order", StringType()),
            StructField("UOM_ON_Order", StringType()),
            StructField("Quantity_Committed", StringType()),
            StructField("UOM_Committed", StringType()),
            StructField("Quantity_Available_IN_Pieces", StringType()),
            StructField("Quantity_ON_Order_IN_Pieces", StringType()),
            StructField("Quantity_Committed_IN_Pieces", StringType()),
            StructField("Unit_Price", StringType()),
            StructField("Total_Value_Available", StringType()),
            StructField("Custom_Field_1", StringType()),
            StructField("Custom_Field_2", StringType())
        ])

        df = session.read\\
        .schema(df_schema)\\
        .option("skip_header",0)\\
        .option("field_delimiter", ",")\\
        .option("encoding", "ISO-8859-15") \\
        .option("field_optionally_enclosed_by", "\\"") \\
        .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)


        #df = df.with_column("run_id", lit(None).cast(DecimalType(14,0)))
        df = df.with_column("cdl_dttm", lit(datetime.now().strftime("%Y%m%d%H%M%S")))
        df=df.with_column("file_name",lit(file_name))
        df = df.withColumn("crtd_dttm", current_timestamp())

        snowdf=df.select("Distributor_ID",
                        "DATE",
                        "Distributor_WH_ID",
                        "SAP_Material_ID",
                        "Product_Code",
                        "Product_EAN_Code",
                        "Product_Description",
                        "Quantity_Available",
                        "UOM_Available",
                        "Quantity_ON_Order",
                        "UOM_ON_Order",
                        "Quantity_Committed",
                        "UOM_Committed",
                        "Quantity_Available_IN_Pieces",
                        "Quantity_ON_Order_IN_Pieces",
                        "Quantity_Committed_IN_Pieces",
                        "Unit_Price",
                        "Total_Value_Available",
                        "Custom_Field_1",
                        "Custom_Field_2",
                        "file_name",
                        "cdl_dttm",
                        "crtd_dttm")

        snowdf= snowdf.filter(snowdf["Product_Code"].isNotNull())
        
        
        if snowdf.count()==0 :
            return "No Data in table"

        
        file_name=("_").join(file_name.split("_")[0:5])+"_"+datetime.now().strftime("%Y%m%d%H%M%S")
        snowdf.write.copy_into_location("@"+Param[1]+"/"+Param[2]+"/success/"+file_name,header=True,OVERWRITE=True)
        

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
		

CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.MYSSDL_RAW.MY_LE_TARGET_PREPROCESSING("PARAM" ARRAY)
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

        #Param=[''MY_LE_Target.csv'',''MYSSDL_RAW.DEV_LOAD_STAGE_ADLS'',''dev/targets/sellin/'',''SDL_MY_LE_TRGT'']

        # Extracting parameters from the input
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]

        # Define the schema for the DataFrame
        df_schema=StructType([
            StructField("CUST_ID",StringType()),
            StructField("CUST_NM",StringType()),
            StructField("JJ_YEAR",StringType()),
            StructField("MNTH_NM",StringType()),
            StructField("TRGT_TYPE",StringType()),
            StructField("TRGT_VAL_TYPE",StringType()),
            StructField("WK1",StringType()),
            StructField("WK2",StringType()),
            StructField("WK3",StringType()),
            StructField("WK4",StringType()),
            StructField("WK5",StringType())
            ])


        # Read the CSV file into a DataFrame
    
        dataframe = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "\\u0001")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        #---------------------------Transformation logic ------------------------------#

         # Add CDL_DTTM and CURR_DT to the dataframe

        dataframe = dataframe.with_column("CDL_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))

        dataframe = dataframe.with_column("CURR_DT",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        

        # Creating copy of the Dataframe
        final_df = dataframe.alias("final_df")

        if final_df.count()==0:
            return "No Data in file"

        # Handle null values or empty rows
        
        final_df=final_df.filter(col("CUST_ID").is_not_null()) and final_df.filter(col("CUST_NM").is_not_null())


        # Load Data to the target table
        final_df.write.mode("append").saveAsTable(target_table)

        # write to success folder
    
        file_name=file_name.split(".")[0]+''_''+datetime.now().strftime("%Y%m%d%H%M%S")
        final_df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/success/"+file_name,file_format_type="csv",header=True,OVERWRITE=True)

        
        return "Success"
        

    
    except KeyError as key_error:
        # Handle KeyError (missing columns) here
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message
        
    except Exception as e:
        # Handle exceptions here
        error_message = f"Error: {str(e)}"
        return error_message';
		

CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.MYSSDL_RAW.MY_POS_MT_CUST_SALES_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS CALLER
AS '

import snowflake.snowpark as snowpark
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DateType
from snowflake.snowpark.functions import col,lit,date_format,current_timestamp,to_timestamp
import pandas as pd
from datetime import datetime
import pytz


def main(session: snowpark.Session, Param): 
    
    try:
        # Extracting parameters from the input

        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        db_name         = stage_name.split(''.'')[0]
        target_table    = db_name+"."+Param[3]
        
    
        # Define the schema for the DataFrame
        df_schema=StructType([
            StructField("CUSTOMER",StringType()),
            StructField("CUSTOMER_NAME",StringType()),
            StructField("STORE_CODE",StringType()),
            StructField("STORE_NAME",StringType()),
            StructField("DEPT_CODE",StringType()),
            StructField("DEPT_NAME",StringType()),
            StructField("ITEM_CODE",StringType()),
            StructField("ITEM_DESC",StringType()),
            StructField("YEAR_MTH",StringType()),
            StructField("WEEK_NO",StringType()),
            StructField("QTY",StringType()),
            StructField("SELLOUT_VALUE",StringType()),
            StructField("SAP_CODE",StringType())
            ])
            
        # Set the current session schema
        
        session.use_schema(stage_name.split(''.'')[0])
        
        # Read the CSV file into a DataFrame
        df = session.read\\
        .schema(df_schema)\\
        .option("skip_header",1)\\
        .option("field_delimiter", "\\u0001")\\
        .option("field_optionally_enclosed_by", "\\"") \\
        .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
    
         # Add  "FILE_NAME" to the Dataframe
        df = df.withColumn("FILE_NAME",lit(file_name).cast("string"))

        
# Add "CDL_DTTM", "CRTD_DTTM" to the Dataframe

        df = df.with_column("CDL_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        #convertin time stamp into sg timezone
        df = df.with_column("CRTD_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        # Creating copy of the Dataframe
        final_df = df.alias("final_df")

        if final_df.count()==0:
            return "No Data in file"

        final_df=final_df.filter(col("CUSTOMER").is_not_null())

        
        # Load Data to the target table
        final_df.write.mode("append").saveAsTable(target_table)

    
        # write to success folder
    
        file_name=file_name.split(".")[0]+''_''+datetime.now().strftime("%Y%m%d%H%M%S")
        final_df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/success/"+file_name,file_format_type="csv",header=True,OVERWRITE=True)
        
        


        return "Success"

    except KeyError as key_error:
        # Handle KeyError (missing columns) here
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message
        
    except pd.errors.MergeError as merge_error:
        # Handle DataFrame merging error
        error_message = f"DataFrame merging error: {str(merge_error)}"
        return error_mess
    
    except Exception as e:
        # Handle exceptions here
        error_message = f"Error: {str(e)}"
        return error_message
';


CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.MYSSDL_RAW.MY_POS_OUTLETMASTER_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS CALLER
AS '

import snowflake.snowpark as snowpark
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField
from snowflake.snowpark.functions import col,lit,date_format,current_timestamp,to_timestamp
import pandas as pd
from datetime import datetime
import pytz


def main(session: snowpark.Session, Param): 
    
    try:
        # Extracting parameters from the input

        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        db_name         = stage_name.split(''.'')[0]
        target_table    = db_name+"."+Param[3]
        
    
        # Define the schema for the DataFrame
        df_schema=StructType([
            StructField("CUSTOMER_CODE",StringType()),
            StructField("CUSTOMER",StringType()),
            StructField("STORE_CODE",StringType()),
            StructField("STORE_NAME",StringType()),
            StructField("DEPT_CODE",StringType()),
            StructField("DEPT_NAME",StringType()),
            StructField("REGION",StringType()),
            StructField("STORE_FORMAT",StringType()),
            StructField("STORE_TYPE",StringType())
            ])
        # Set the current session schema
        
        session.use_schema(stage_name.split(''.'')[0])
        
        # Read the CSV file into a DataFrame
        df = session.read\\
        .schema(df_schema)\\
        .option("skip_header",1)\\
        .option("field_delimiter", "\\u0001")\\
        .option("field_optionally_enclosed_by", "\\"") \\
        .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
    
        # Add "CDL_DTTM", "CRTD_DTTM" to the Dataframe

        df = df.with_column("CDL_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        #convertin time stamp into sg timezone
        df = df.with_column("CRTD_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        

        # Creating copy of the Dataframe
        final_df = df.alias("final_df")

        if final_df.count()==0:
            return "No Data in file"

        final_df=final_df.filter(col("CUSTOMER_CODE").is_not_null()) 

        
        # Load Data to the target table
        final_df.write.mode("append").saveAsTable(target_table)

    
        # write to success folder
    
        file_name=file_name.split(".")[0]+''_''+datetime.now().strftime("%Y%m%d%H%M%S")
        final_df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/success/"+file_name,file_format_type="csv",header=True,OVERWRITE=True)
        
        


        return "Success"

    except KeyError as key_error:
        # Handle KeyError (missing columns) here
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message
        
    except pd.errors.MergeError as merge_error:
        # Handle DataFrame merging error
        error_message = f"DataFrame merging error: {str(merge_error)}"
        return error_mess
    
    except Exception as e:
        # Handle exceptions here
        error_message = f"Error: {str(e)}"
        return error_message
';


CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.MYSSDL_RAW.MY_SELLIN_CUSTMASTER_PREPROCESSING("PARAM" ARRAY)
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
from snowflake.snowpark.functions import col,lit,date_format,current_timestamp,to_timestamp
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField,DecimalType,DateType
import pandas as pd
from datetime import datetime
import pytz


def main(session: snowpark.Session,Param): 

    try:

        #Param=[''MY_Sellin_CustomerMaster.csv'',''MYSSDL_RAW.DEV_LOAD_STAGE_ADLS'',''dev/Sellin/Masters'',''SDL_MY_CUSTOMER_DIM'']

        # Extracting parameters from the input
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        

        # Define the schema for the DataFrame
        df_schema=StructType([
            StructField("CUST_ID",StringType()),
            StructField("CUST_NM",StringType()),
            StructField("DSTRBTR_GRP_CD",StringType()),
            StructField("DSTRBTR_GRP_NM",StringType()),
            StructField("ULLAGE",StringType()),
            StructField("CHNL",StringType()),
            StructField("TERRITORY",StringType()),
            StructField("RETAIL_ENV",StringType()),
            StructField("TRDNG_TERM_VAL",StringType()),
            StructField("RDD_IND",StringType())
            
            ])


        # Read the CSV file into a DataFrame
    
        dataframe = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "\\u0001")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
        

            #---------------------------Transformation logic ------------------------------#

        # Add CDL_DTTM and CURR_DT to the dataframe

        dataframe = dataframe.with_column("CDL_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))

        dataframe = dataframe.with_column("CURR_DT",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))


        # Creating copy of the Dataframe
        final_df = dataframe.alias("final_df")

        if final_df.count()==0:
            return "No Data in file"


        final_df=final_df.filter(col("CUST_ID").is_not_null()) and final_df.filter(col("DSTRBTR_GRP_CD").is_not_null())

        # Load Data to the target table
        final_df.write.mode("append").saveAsTable(target_table)

        
        # write to success folder
    
        file_name=file_name.split(".")[0]+''_''+datetime.now().strftime("%Y%m%d%H%M%S")
        final_df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/success/"+file_name,file_format_type="csv",header=True,OVERWRITE=True)
        
        return "Success"



    
    except KeyError as key_error:
        # Handle KeyError (missing columns) here
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message
        
    except Exception as e:
        # Handle exceptions here
        error_message = f"Error: {str(e)}"
        return error_message';
		

CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.MYSSDL_RAW.MY_SELLOUT_DISTRIBUTOR_DOC_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them.

import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col,lit
from snowflake.snowpark.types import  StringType, StructType, StructField
import pandas as pd
from datetime import datetime
import pytz


def main(session: snowpark.Session,Param): 

    try:
        #Param=[''MY_GT_Distributor_Doc_Type.csv'',''MYSSDL_RAW.DEV_LOAD_STAGE_ADLS'',''dev/master/sellout'',''SDL_MY_DSTRBTR_DOC_TYPE'']

        # Extracting parameters from the input
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]

         # Define the schema
        df_schema = StructType([
                StructField("dist_code", StringType()),
                StructField("dist_name", StringType()),
                StructField("level1", StringType()),
                StructField("level2", StringType()),
                StructField("wh_id", StringType()),
                StructField("doc_type", StringType()),
                StructField("doc_type_des", StringType())
            ])
        
                
        dataframe = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "\\u0001")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        # Add CDL_DTTM and CURR_DT to the dataframe
        dataframe = dataframe.with_column("CDL_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        dataframe = dataframe.with_column("CURR_DT",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))

        #drop the column which has no DISTRIBUTOR_ID 
        dataframe=dataframe.filter(col("dist_code").is_not_null())

        #if file has not 
        if dataframe.count()==0:
            return "No Data in file"

        print(dataframe.show())

         # Load Data to the target table
        dataframe.write.mode("append").saveAsTable(target_table)

        # write to success folder
        file_name=file_name.split(".")[0]+''_''+datetime.now().strftime("%Y%m%d%H%M%S")
        dataframe.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/success/"+file_name,file_format_type="csv",header=True,OVERWRITE=True)

    
        return ''Success''

    except KeyError as key_error:
    # Handle KeyError (missing columns) here
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message
    
    except Exception as e:
    # Handle exceptions here
        error_message = f"Error: {str(e)}"
        return error_message


    ';
	
CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.MYSSDL_RAW.MY_SELLOUT_DISTRIBUTOR_HIERARCHY_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
COMMENT='preprocessing structural changes for file MY_GT_Distributor_Hierarchy.csv'
EXECUTE AS OWNER
AS '# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them.

import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col,lit
from snowflake.snowpark.types import  StringType, StructType, StructField
import pandas as pd
from datetime import datetime
import pytz


def main(session: snowpark.Session,Param): 

    try:
        #Param=[''MY_GT_Distributor_Hierarchy.csv'',''MYSSDL_RAW.DEV_LOAD_STAGE_ADLS'',''dev/master/sellout'',''SDL_MY_DSTRBTRR_DIM'']

        # Extracting parameters from the input
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]

         # Define the schema
        df_schema = StructType([
            StructField("distributor_id", StringType()),
            StructField("distributor_name", StringType()),
            StructField("level_1", StringType()),
            StructField("level_2", StringType()),
            StructField("level_3", StringType()),
            StructField("level_4", StringType()),
            StructField("level_5", StringType()),
            StructField("tradeterm", StringType()),
            StructField("abbreviation", StringType()),
            StructField("buyer_gln", StringType()),
            StructField("location_gln", StringType()),
            StructField("channel_manager", StringType()),
            StructField("cdm", StringType()),
            StructField("region", StringType())
        ])
        
                
        dataframe = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "\\u0001")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        # Add CDL_DTTM and CURR_DT to the dataframe
        dataframe = dataframe.with_column("CDL_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        dataframe = dataframe.with_column("CURR_DT",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))

        #drop the column which has no DISTRIBUTOR_ID 
        dataframe=dataframe.filter(col("DISTRIBUTOR_ID").is_not_null())

        #if file has not 
        if dataframe.count()==0:
            return "No Data in file"

         # Load Data to the target table
        dataframe.write.mode("append").saveAsTable(target_table)

        # write to success folder
        file_name=file_name.split(".")[0]+''_''+datetime.now().strftime("%Y%m%d%H%M%S")
        dataframe.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/success/"+file_name,file_format_type="csv",header=True,OVERWRITE=True)

    
        return ''Success''

    except KeyError as key_error:
    # Handle KeyError (missing columns) here
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message
    
    except Exception as e:
    # Handle exceptions here
        error_message = f"Error: {str(e)}"
        return error_message


    ';
	
CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.MYSSDL_RAW.MY_SELLOUT_GT_IDS_EXCHNG_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them.

import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col,lit
from snowflake.snowpark.types import  StringType, StructType, StructField
import pandas as pd
from datetime import datetime
import pytz


def main(session: snowpark.Session,Param): 

    try:
        #Param=[''MY_GT_IDS_EXCHNG_RATE.csv'',''MYSSDL_RAW.DEV_LOAD_STAGE_ADLS'',''dev/transaction/sellout/others'',''SDL_RAW_MY_IDS_RATE'']

        # Extracting parameters from the input
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]

         # Define the schema
        df_schema = StructType([
                    StructField("distributor_code", StringType()),
                    StructField("distributor_name", StringType()),
                    StructField("exchange_rate", StringType()),
                    StructField("effective_month", StringType())
                ])
        
               
        dataframe = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "\\u0001")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        # Add CDL_DTTM and CURR_DT to the dataframe
        dataframe = dataframe.with_column("CDL_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        dataframe = dataframe.with_column("CURR_DT",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))

        #drop the column which has no DISTRIBUTOR_ID 
        dataframe=dataframe.filter(col("distributor_code").is_not_null())

        #if file has not 
        if dataframe.count()==0:
            return "No Data in file"

        
         # Load Data to the target table
        dataframe.write.mode("append").saveAsTable(target_table)

        # write to success folder
        file_name=file_name.split(".")[0]+''_''+datetime.now().strftime("%Y%m%d%H%M%S")
        dataframe.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/success/"+file_name,file_format_type="csv",header=True,OVERWRITE=True)

    
        return ''Success''

    except KeyError as key_error:
    # Handle KeyError (missing columns) here
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message
    
    except Exception as e:
    # Handle exceptions here
        error_message = f"Error: {str(e)}"
        return error_message


    ';
	
CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.MYSSDL_RAW.MY_SELLOUT_GT_IN_TRANSIT_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them.

import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col,lit
from snowflake.snowpark.types import  StringType, StructType, StructField
import pandas as pd
from datetime import datetime
import pytz


def main(session: snowpark.Session,Param): 

    try:
        #Param=[''MY_GT_In_Transit_Inv.csv'',''MYSSDL_RAW.DEV_LOAD_STAGE_ADLS'',''dev/transaction/sellout/others'',''sdl_MY_IN_TRANSIT'']

        # Extracting parameters from the input
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]

         # Define the schema
        df_schema = StructType([
                StructField("billing_doc", StringType()),
                StructField("billing_date", StringType()),
                StructField("gr_date", StringType()),
                StructField("closing_date", StringType()),
                StructField("remarks", StringType())
            ])
        
               
        dataframe = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "\\u0001")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        # Add CDL_DTTM and CURR_DT to the dataframe
        dataframe = dataframe.with_column("CDL_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        dataframe = dataframe.with_column("CURR_DT",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))

        #drop the column which has no DISTRIBUTOR_ID 
        dataframe=dataframe.filter(col("billing_doc").is_not_null())

        #if file has not 
        if dataframe.count()==0:
            return "No Data in file"


         # Load Data to the target table
        dataframe.write.mode("append").saveAsTable(target_table)

        # write to success folder
        file_name=file_name.split(".")[0]+''_''+datetime.now().strftime("%Y%m%d%H%M%S")
        dataframe.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/success/"+file_name,file_format_type="csv",header=True,OVERWRITE=True)

    
        return ''Success''

    except KeyError as key_error:
    # Handle KeyError (missing columns) here
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message
    
    except Exception as e:
    # Handle exceptions here
        error_message = f"Error: {str(e)}"
        return error_message


    ';
	

CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.MYSSDL_RAW.MY_SELLOUT_SALES_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them.



from snowflake.snowpark.functions import col, lit, date_format, current_timestamp, to_date, year, month,  format_number, to_timestamp,when,trim,regexp_replace
from snowflake.snowpark.types import  StringType, StructType, StructField, DecimalType
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz
def main(session: snowpark.Session,Param):
    #Param=[''MY_GT_133986_Sales_2019123.csv'',''MYSSDL_RAW.DEV_LOAD_STAGE_ADLS'',''dev/transaction/sales'',''SDL_SO_SALES_133986'']
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        stage_path = Param[2]
        target_table    = Param[3]
        # Define the schema
        df_schema = StructType([
                StructField("DISTRIBUTOR_ID", StringType()),
                StructField("SALES_ORDER_NUMBER", StringType()),
                StructField("SALES_ORDER_DATE", StringType()),
                StructField("TYPE", StringType()),
                StructField("CUSTOMER_CODE", StringType()),
                StructField("DISTRIBUTOR_WH_ID", StringType()),
                StructField("SAP_MATERIAL_ID", StringType()),
                StructField("PRODUCT_CODE", StringType()),
                StructField("PRODUCT_EAN_CODE", StringType()),
                StructField("PRODUCT_DESCRIPTION", StringType()),
                StructField("GROSS_ITEM_PRICE", StringType()),
                StructField("QUANTITY", StringType()),
                StructField("UOM", StringType()),
                StructField("QUANTITY_IN_PIECES", StringType()),
                StructField("QUANTITY_AFTER_CONVERSION", StringType()),
                StructField("SUB_TOTAL_1", StringType()),
                StructField("DISCOUNT", StringType()),
                StructField("SUB_TOTAL_2", StringType()),
                StructField("BOTTOM_LINE_DISCOUNT", StringType()),
                StructField("TOTAL_AMT_AFTER_TAX", StringType()),
                StructField("TOTAL_AMT_BEFORE_TAX", StringType()),
                StructField("SALES_EMPLOYEE", StringType()),
                StructField("CUSTOM_FIELD_1", StringType()),
                StructField("CUSTOM_FIELD_2", StringType()),
                StructField("CUSTOM_FIELD_3", StringType())
        ])



        #read the file
        df = session.read\\
        .schema(df_schema)\\
        .option("skip_header",1)\\
        .option("field_delimiter", ",")\\
        .option("field_optionally_enclosed_by", "\\"") \\
        .csv("@"+stage_name+"/"+stage_path+"/"+file_name)
        
        #create the column which are derived from file name
        
        #df = df.withColumn("GROSS_ITEM_PRICE", regexp_replace(col("GROSS_ITEM_PRICE"), ",", ""));
        df = df.withColumn("GROSS_ITEM_PRICE", regexp_replace(col("GROSS_ITEM_PRICE"), ",", ""));
        df = df.withColumn("QUANTITY", regexp_replace(col("QUANTITY"), ",", ""));
        df = df.withColumn("QUANTITY_IN_PIECES", regexp_replace(col("QUANTITY_IN_PIECES"), ",", ""));
        df = df.withColumn("QUANTITY_AFTER_CONVERSION", regexp_replace(col("QUANTITY_AFTER_CONVERSION"), ",", ""));
        df = df.withColumn("SUB_TOTAL_1", regexp_replace(col("SUB_TOTAL_1"), ",", ""));
        df = df.withColumn("SUB_TOTAL_2", regexp_replace(col("SUB_TOTAL_2"), ",", ""));
        df = df.withColumn("BOTTOM_LINE_DISCOUNT", regexp_replace(col("BOTTOM_LINE_DISCOUNT"), ",", ""));
        df = df.withColumn("TOTAL_AMT_AFTER_TAX", regexp_replace(col("TOTAL_AMT_AFTER_TAX"), ",", ""));
        df = df.withColumn("TOTAL_AMT_BEFORE_TAX", regexp_replace(col("TOTAL_AMT_BEFORE_TAX"), ",", ""));
        df=df.with_column("file_name",lit(file_name))



        
        
        snowdf=df.select("DISTRIBUTOR_ID","SALES_ORDER_NUMBER","SALES_ORDER_DATE","TYPE","CUSTOMER_CODE","DISTRIBUTOR_WH_ID","SAP_MATERIAL_ID","PRODUCT_CODE","PRODUCT_EAN_CODE","PRODUCT_DESCRIPTION","GROSS_ITEM_PRICE","QUANTITY","UOM","QUANTITY_IN_PIECES","QUANTITY_AFTER_CONVERSION","SUB_TOTAL_1","DISCOUNT","SUB_TOTAL_2","BOTTOM_LINE_DISCOUNT","TOTAL_AMT_AFTER_TAX","TOTAL_AMT_BEFORE_TAX","SALES_EMPLOYEE","CUSTOM_FIELD_1","CUSTOM_FIELD_2","CUSTOM_FIELD_3","file_name")
        
        snowdf= snowdf.filter(snowdf["DISTRIBUTOR_ID"].isNotNull())
        
        if snowdf.count()==0 :
            return "No Data in file"



        
        #move file into success folder
        file_name=file_name.split(".")[0]+"_"+datetime.now().strftime("%Y%m%d%H%M%S")
        snowdf.write.copy_into_location("@"+Param[1]+"/"+Param[2]+"/success/"+file_name,header=True,OVERWRITE=True)



        
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
        return error_message';
		

CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.MYSSDL_RAW.MY_SELLOUT_SALES_PREPROCESSING("PARAM" VARIANT)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them.



from snowflake.snowpark.functions import col, lit, date_format, current_timestamp, to_date, year, month,  format_number, to_timestamp,when,trim,regexp_replace
from snowflake.snowpark.types import  StringType, StructType, StructField, DecimalType
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz
def main(session: snowpark.Session,Param):
    #Param=[''MY_GT_133986_Sales_2019123.csv'',''MYSSDL_RAW.DEV_LOAD_STAGE_ADLS'',''dev/transaction/sales'',''SDL_SO_SALES_133986'']
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        stage_path = Param[2]
        target_table    = Param[3]
        # Define the schema
        df_schema = StructType([
                StructField("DISTRIBUTOR_ID", StringType()),
                StructField("SALES_ORDER_NUMBER", StringType()),
                StructField("SALES_ORDER_DATE", StringType()),
                StructField("TYPE", StringType()),
                StructField("CUSTOMER_CODE", StringType()),
                StructField("DISTRIBUTOR_WH_ID", StringType()),
                StructField("SAP_MATERIAL_ID", StringType()),
                StructField("PRODUCT_CODE", StringType()),
                StructField("PRODUCT_EAN_CODE", StringType()),
                StructField("PRODUCT_DESCRIPTION", StringType()),
                StructField("GROSS_ITEM_PRICE", StringType()),
                StructField("QUANTITY", StringType()),
                StructField("UOM", StringType()),
                StructField("QUANTITY_IN_PIECES", StringType()),
                StructField("QUANTITY_AFTER_CONVERSION", StringType()),
                StructField("SUB_TOTAL_1", StringType()),
                StructField("DISCOUNT", StringType()),
                StructField("SUB_TOTAL_2", StringType()),
                StructField("BOTTOM_LINE_DISCOUNT", StringType()),
                StructField("TOTAL_AMT_AFTER_TAX", StringType()),
                StructField("TOTAL_AMT_BEFORE_TAX", StringType()),
                StructField("SALES_EMPLOYEE", StringType()),
                StructField("CUSTOM_FIELD_1", StringType()),
                StructField("CUSTOM_FIELD_2", StringType()),
                StructField("CUSTOM_FIELD_3", StringType())
        ])



        #read the file
        df = session.read\\
        .schema(df_schema)\\
        .option("skip_header",1)\\
        .option("field_delimiter", ",")\\
        .option("field_optionally_enclosed_by", "\\"") \\
        .csv("@"+stage_name+"/"+stage_path+"/"+file_name)
        
        #create the column which are derived from file name
        
        #df = df.withColumn("GROSS_ITEM_PRICE", regexp_replace(col("GROSS_ITEM_PRICE"), ",", ""));
        df = df.withColumn("GROSS_ITEM_PRICE", regexp_replace(col("GROSS_ITEM_PRICE"), ",", ""));
        df = df.withColumn("QUANTITY", regexp_replace(col("QUANTITY"), ",", ""));
        df = df.withColumn("QUANTITY_IN_PIECES", regexp_replace(col("QUANTITY_IN_PIECES"), ",", ""));
        df = df.withColumn("QUANTITY_AFTER_CONVERSION", regexp_replace(col("QUANTITY_AFTER_CONVERSION"), ",", ""));
        df = df.withColumn("SUB_TOTAL_1", regexp_replace(col("SUB_TOTAL_1"), ",", ""));
        df = df.withColumn("SUB_TOTAL_2", regexp_replace(col("SUB_TOTAL_2"), ",", ""));
        df = df.withColumn("BOTTOM_LINE_DISCOUNT", regexp_replace(col("BOTTOM_LINE_DISCOUNT"), ",", ""));
        df = df.withColumn("TOTAL_AMT_AFTER_TAX", regexp_replace(col("TOTAL_AMT_AFTER_TAX"), ",", ""));
        df = df.withColumn("TOTAL_AMT_BEFORE_TAX", regexp_replace(col("TOTAL_AMT_BEFORE_TAX"), ",", ""));
        df=df.with_column("file_name",lit(file_name))



        
        
        snowdf=df.select("DISTRIBUTOR_ID","SALES_ORDER_NUMBER","SALES_ORDER_DATE","TYPE","CUSTOMER_CODE","DISTRIBUTOR_WH_ID","SAP_MATERIAL_ID","PRODUCT_CODE","PRODUCT_EAN_CODE","PRODUCT_DESCRIPTION","GROSS_ITEM_PRICE","QUANTITY","UOM","QUANTITY_IN_PIECES","QUANTITY_AFTER_CONVERSION","SUB_TOTAL_1","DISCOUNT","SUB_TOTAL_2","BOTTOM_LINE_DISCOUNT","TOTAL_AMT_AFTER_TAX","TOTAL_AMT_BEFORE_TAX","SALES_EMPLOYEE","CUSTOM_FIELD_1","CUSTOM_FIELD_2","CUSTOM_FIELD_3","file_name")
        
        snowdf= snowdf.filter(snowdf["DISTRIBUTOR_ID"].isNotNull())
        
        if snowdf.count()==0 :
            return "No Data in file"



        
        #move file into success folder
        file_name=file_name.split(".")[0]+"_"+datetime.now().strftime("%Y%m%d%H%M%S")
        snowdf.write.copy_into_location("@"+Param[1]+"/"+Param[2]+"/success/"+file_name,header=True,OVERWRITE=True)



        
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
        return error_message';
		
		
CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.MYSSDL_RAW.MY_TARGETS_PREPROCESSING("PARAM" ARRAY)
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

        #Param=[''MY_Targets.csv'',''MYSSDL_RAW.DEV_LOAD_STAGE_ADLS'',''dev/targets/sellin/'',''SDL_MY_TRGTS'']

        # Extracting parameters from the input
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]

        # Define the schema for the DataFrame
        df_schema=StructType([
            StructField("CUST_ID",StringType()),
            StructField("CUST_NM",StringType()),
            StructField("BRND_DESC",StringType()),
            StructField("SUB_SEGMENT",StringType()),
            StructField("JJ_YEAR",StringType()),
            StructField("TRGT_TYPE",StringType()),
            StructField("TRGT_VAL_TYPE",StringType()),
            StructField("JAN_VAL",StringType()),
            StructField("FEB_VAL",StringType()),
            StructField("MAR_VAL",StringType()),
            StructField("APR_VAL",StringType()),
            StructField("MAY_VAL",StringType()),
            StructField("JUN_VAL",StringType()),
            StructField("JUL_VAL",StringType()),
            StructField("AUG_VAL",StringType()),
            StructField("SEP_VAL",StringType()),
            StructField("OCT_VAL",StringType()),
            StructField("NOV_VAL",StringType()),
            StructField("DEC_VAL",StringType())
            ])

        
        # Read the CSV file into a DataFrame
    
        dataframe = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "\\u0001")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
        

        #---------------------------Transformation logic ------------------------------#

         # Add CDL_DTTM and CURR_DT to the dataframe

        dataframe = dataframe.with_column("CDL_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))

        dataframe = dataframe.with_column("CURR_DT",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))


        # Creating copy of the Dataframe
        final_df = dataframe.alias("final_df")

        if final_df.count()==0:
            return "No Data in file"

        # Handle null values or empty rows
        
        final_df=final_df.filter(col("CUST_ID").is_not_null()) and final_df.filter(col("CUST_NM").is_not_null())


        # Load Data to the target table
        final_df.write.mode("append").saveAsTable(target_table)

        # write to success folder
    
        file_name=file_name.split(".")[0]+''_''+datetime.now().strftime("%Y%m%d%H%M%S")
        final_df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/success/"+file_name,file_format_type="csv",header=True,OVERWRITE=True)

        
        return "Success"

        


    except KeyError as key_error:
        # Handle KeyError (missing columns) here
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message
        
    except Exception as e:
        # Handle exceptions here
        error_message = f"Error: {str(e)}"
        return error_message';
