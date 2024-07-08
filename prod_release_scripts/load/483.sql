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
        full_path = "@"+stage_name+"/"+temp_stage_path+"/"+file_nam

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

CREATE OR REPLACE PROCEDURE NTASDL_RAW.POP6_HK_MASTER_PRODUCT_LISTS_PRODUCTS_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them.

from snowflake.snowpark.functions import col, lit, date_format, current_timestamp, to_date, year, month, concat, format_number, regexp_replace,to_timestamp,when,trim,upper,is_null,md5
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DecimalType
import snowflake.snowpark
import pandas as pd
from datetime import datetime,timedelta
import snowflake.snowpark as snowpark
import pytz
import pandas as pd

def main(session:snowpark.Session,Param):
    # Param=["20240617_product_lists_products.csv","JPNSDL_RAW.DEV_LOAD_STAGE_ADLS","dev/pop6/master/listdata/product_lists_products/","sdl_pop6_jp_product_lists_products"]
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        
        df_schema = StructType([
                StructField("Product_List", StringType(), True),
                StructField("Product_List_Code", StringType(), True),
                StructField("ProductDB_ID", StringType(), True),
                StructField("SKU", StringType(), True),
                StructField("MSL_Ranking", StringType(), True),
                StructField("Date", StringType(), True),
                
                
            ])
        
        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", ",")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .option("encoding",''UTF-8'')\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

       
        
        df=df.na.drop("all")
        if df.count()==0 :
            return "No Data in file"

        df = df.with_column(''file_name'',lit(file_name))


        def generunid():
            new_time = datetime.now(pytz.utc) + timedelta(hours=8)
            return new_time.strftime(''%Y%m%d%H%M%S'')

        def generatecrtd_dttm():
            new_time = datetime.now(pytz.utc) + timedelta(hours=8)
            return new_time.strftime(''%Y-%m-%d %H:%M:%S'')
            
            

        df = df.with_column(''hashkey'',md5(concat(when(is_null(col(''Product_List'')),lit('''')).otherwise(col(''Product_List'')),\\
                                            when(is_null(col(''Product_List_Code'')),lit('''')).otherwise(col(''Product_List_Code'')),\\
                                          when(is_null(col(''ProductDB_ID'')),lit('''')).otherwise(col(''ProductDB_ID'')),\\
                                          when(is_null(col(''SKU'')),lit('''')).otherwise(col(''SKU'')),\\
                                         when(is_null(col(''MSL_Ranking'')),lit('''')).otherwise(col(''MSL_Ranking'')),\\
                                            when(is_null(col(''Date'')),lit('''')).otherwise(col(''Date'')))))

        
        df = df.with_column(''run_id'',lit(generunid()))

        df = df.with_column(''crtd_dttm'',lit(generatecrtd_dttm()))
        

        

        
        snowdf= df.select(
                    "product_list",
                    "productdb_id",
                    "sku",
                    "msl_ranking",
                    "date",
                    "file_name",
                    "run_id",
                    "crtd_dttm",
                    "hashkey"
                    
            
                    )
        file_name=file_name.split(".")[0]+"_"+datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")

        snowdf.write.mode("append").saveAsTable(stage_name.split(".")[0] + "." + target_table)
        
        # #move to success
        snowdf.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)
        
        
        
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

CREATE OR REPLACE PROCEDURE NTASDL_RAW.POP6_HK_MASTER_PRODUCT_LISTS_ALLOCATION_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them.

from snowflake.snowpark.functions import col, lit, date_format, current_timestamp, to_date, year, month, concat, format_number, regexp_replace,to_timestamp,when,trim,upper,is_null,md5
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DecimalType
import snowflake.snowpark
import pandas as pd
from datetime import datetime,timedelta
import snowflake.snowpark as snowpark
import pytz
import pandas as pd

def main(session:snowpark.Session,Param):
    # Param=["20240617_product_lists_allocation.csv","JPNSDL_RAW.DEV_LOAD_STAGE_ADLS","dev/pop6/master/listdata/product_lists_allocation","sdl_pop6_jp_product_lists_allocation"]
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        
        df_schema = StructType([
                StructField("Product_Group_Status", StringType(), True),
                StructField("Product_Group", StringType(), True),
                StructField("Product_List_Status", StringType(), True),
                StructField("Product_List", StringType(), True),
                StructField("Product_List_Code", StringType(), True),
                StructField("POP_Attribute_ID", StringType(), True),
                StructField("POP_Attribute", StringType(), True),
                StructField("POP_Attribute_Value_ID", StringType(), True),
                StructField("POP_Attribute_Value", StringType(), True),
                StructField("Date", StringType(), True),
                
                
                
            ])
        
        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", ",")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .option("encoding",''UTF-8'')\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

       
        
        df=df.na.drop("all")
        if df.count()==0 :
            return "No Data in file"

        df = df.with_column(''file_name'',lit(file_name))


        def generunid():
            new_time = datetime.now(pytz.utc) + timedelta(hours=8)
            return new_time.strftime(''%Y%m%d%H%M%S'')

        def generatecrtd_dttm():
            new_time = datetime.now(pytz.utc) + timedelta(hours=8)
            return new_time.strftime(''%Y-%m-%d %H:%M:%S'')
            
            

        df = df.with_column(''hashkey'',md5(concat(when(is_null(col(''Product_Group_Status'')),lit('''')).otherwise(col(''Product_Group_Status'')),\\
                                            when(is_null(col(''Product_Group'')),lit('''')).otherwise(col(''Product_Group'')),\\
                                          when(is_null(col(''Product_List_Status'')),lit('''')).otherwise(col(''Product_List_Status'')),\\
                                          when(is_null(col(''Product_List'')),lit('''')).otherwise(col(''Product_List'')),\\
                                         when(is_null(col(''POP_Attribute_ID'')),lit('''')).otherwise(col(''POP_Attribute_ID'')),\\
                                         when(is_null(col(''POP_Attribute'')),lit('''')).otherwise(col(''POP_Attribute'')),\\
                                           when(is_null(col(''POP_Attribute_Value_ID'')),lit('''')).otherwise(col(''POP_Attribute_Value_ID'')),\\
                                          when(is_null(col(''POP_Attribute_Value'')),lit('''')).otherwise(col(''POP_Attribute_Value'')),\\
                                         when(is_null(col(''date'')),lit('''')).otherwise(col(''date'')))))

        
        df = df.with_column(''run_id'',lit(generunid()))

        df = df.with_column(''crtd_dttm'',lit(generatecrtd_dttm()))
        

        

        
        snowdf= df.select(
                    "Product_Group_Status",
                    "Product_Group",
                    "Product_List_Status",
                    "Product_List",
                    "POP_Attribute_ID",
                    "POP_Attribute",
                    "POP_Attribute_Value_ID",
                    "POP_Attribute_Value",
                    "date",
                    "file_name",
                    "run_id",
                    "crtd_dttm",
                    "hashkey"
            
                    )
        file_name=file_name.split(".")[0]+"_"+datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")

        snowdf.write.mode("append").saveAsTable(stage_name.split(".")[0] + "." + target_table)
        
        # #move to success
        snowdf.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)
        
        
        
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


