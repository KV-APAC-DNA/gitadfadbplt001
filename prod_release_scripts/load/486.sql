CREATE OR REPLACE PROCEDURE PCFSDL_RAW.PCF_PERENSO_METCASH_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('openpyxl==3.1.2','snowflake-snowpark-python==*','xlrd==2.0.1')
HANDLER = 'main'
EXECUTE AS OWNER
AS 'import snowflake.snowpark
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz
import sys
import pandas as pd
import logging
from datetime import datetime
import collections
from pathlib import Path
import openpyxl as xl
from snowflake.snowpark.files import SnowflakeFile
from snowflake.snowpark.functions import col, lit, current_timestamp, trim, upper
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DecimalType,DateType

def main(session: snowpark.Session,Param): 
    
	# SP call and parameters to pass.
	# C
    try:
        # Extracting parameters from the input

        file_name_raw  = "Monthly Sales Report, Supplier filter only_J_J "+Param[0].split("_")[-1]
        file_name= file_name_raw.split(".")[0]+".xlsx"
        stage_name = Param[1]
        temp_stage_path = Param[2]
        target_table = Param[3]

        full_path = "@"+stage_name+"/"+temp_stage_path+"/"+file_name



        # Define the schema for the DataFramee
        with SnowflakeFile.open(full_path, "rb", require_scoped_url = False) as f: 
            try:
                df1 = pd.read_excel(f,sheet_name=0,dtype=str)
                df1=df1.iloc[5:,:]
                df1 = df1[df1.iloc[:, 0] != "Total"]
            except pd.errors.EmptyDataError:
            # Handle the error when the file is empty or contains no columns
                return("No Data in file")

            if len(df1.columns)==17:
                df1.insert(loc=13,column="new_sale1",value=None)
                df1.insert(loc=18,column="new_case1",value=None)

        df=session.create_dataframe(df1)

        df=df.na.drop("all")
        if df.count()==0:
            return "No Data in file"

        
        # Set the current session schema
        
        #session.use_schema(stage_name.split(".")[0])


        
        stage_path="@{0}/{1}/{2}".format(stage_name,temp_stage_path,file_name)
        #print(stage_path)
        
       

        #temp_df = temp_df.withColumn("gross_sales_wk5",lit(None))
        #temp_df = temp_df.withColumn("gross_cases_wk5",lit(None))
        df = df.withColumn("run_id",lit(lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S"))))
        df = df.withColumn("file_name",lit(file_name))
        #df = df.withColumn("file_name",lit(file_name.replace("_"," ").replace(".csv",".xlsx")))
        df = df.withColumn("create_dt",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        
                    
        snowdf=df
        #return snowdf
        file_name = file_name.split(".")[0] + "_" + datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        

        
        
        snowdf.write.mode("append").saveAsTable(stage_name.split(".")[0] + "." + target_table)
        
        snowdf.write.copy_into_location("@" + stage_name + "/" + temp_stage_path + "/processed/success/" + formatted_year + "/" + formatted_month + "/" + file_name, header=True, OVERWRITE=True)


        return "Success"

    except KeyError as key_error:
        # Handle KeyError (missing columns) here
        error_message = f"KeyError: {str(key_error)}"
        return error_message

    
    except Exception as e:
        # Handle exceptions here
        error_message = f"Error: {str(e)}"
        return error_message';
