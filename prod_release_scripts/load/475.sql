CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.NTASDL_RAW.OTC_INV_1_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('openpyxl==3.1.2','snowflake-snowpark-python==*')
HANDLER = 'main'
EXECUTE AS OWNER
AS '# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them.

import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col,lit,trim,when
from snowflake.snowpark.types import  StringType, StructType, StructField
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz
import openpyxl
from snowflake.snowpark.files import SnowflakeFile
from openpyxl import load_workbook


def main(session: snowpark.Session,Param): 
       
    try :
            
        # Param=[''OTC_KR_INV_1_202103.csv'',''NTASDL_RAW.DEV_LOAD_STAGE_ADLS_KOR'',''dev/otc_inventory/transactional/'',''SDL_KR_OTC_INVENTORY'']

        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        # Define the schema for the DataFrame
        
        df_schema=StructType([
                        StructField("MATL_NUM", StringType()),
                        StructField("BRAND", StringType()),
                        StructField("PRODUCT_NAME", StringType()),
                        StructField("UNIT_PRICE", StringType()),
                        StructField("INV_QTY", StringType()),
                        StructField("INV_AMT", StringType())
                        
                        ])
                
        dataframe = session.read.schema(df_schema) \\
        .option("skip_header",3) \\
        .option("field_delimiter", "\\u0001") \\
        .option("encoding","UTF-8")\\
        .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
        
       
            
        
        dataframe=dataframe.na.drop("all")
        if dataframe.count()==0:
            return "No Data in file"

        full_path = "@"+stage_name+"/"+temp_stage_path+"/"+file_name.split(".")[0] + ".xlsx"
        with SnowflakeFile.open(full_path, "rb", require_scoped_url = False) as f:
            workbook = load_workbook(f)
            sn=workbook.sheetnames
        sn=sn[0]

        dataframe = dataframe.filter((col("MATL_NUM") != "TOTAL"))
        
        
        
        mnth_id = file_name.split(".")[0].split("_")[4]
        dataframe = dataframe.with_column("mnth_id",lit(mnth_id))
        dataframe = dataframe.with_column("DISTRIBUTOR_CD",lit(sn))
        dataframe = dataframe.with_column("FILE_NAME",lit(file_name.split(".")[0] + ''.xlsx''))
        dataframe = dataframe.with_column("CRTD_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        dataframe=dataframe.select(col("MNTH_ID"), col("MATL_NUM"), col("BRAND"),col("PRODUCT_NAME"), col("DISTRIBUTOR_CD"), col("UNIT_PRICE"),col("INV_QTY"), col("INV_AMT"), col("FILE_NAME"), col("CRTD_DTTM"))
        
        # Load Data to the target table
        dataframe.write.mode("append").saveAsTable(target_table)
       
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        #move to success
        file_name=file_name.split(".")[0]+''_''+datetime.now().strftime("%Y%m%d%H%M%S")
        dataframe.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)

        
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
		
CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.NTASDL_RAW.OTC_INV_2_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('openpyxl==3.1.2','snowflake-snowpark-python==*')
HANDLER = 'main'
EXECUTE AS OWNER
AS '# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them.

import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col,lit,trim,when
from snowflake.snowpark.types import  StringType, StructType, StructField
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz
import openpyxl
from snowflake.snowpark.files import SnowflakeFile
from openpyxl import load_workbook


def main(session: snowpark.Session,Param): 
       
    try :
            
        #Param=[''OTC_KR_INV_2_202405.csv'',''NTASDL_RAW.DEV_LOAD_STAGE_ADLS_KOR'',''dev/otc_inventory/transactional/'',''SDL_KR_OTC_INVENTORY'']

        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        # Define the schema for the DataFrame
        
        df_schema=StructType([
                        StructField("MATL_NUM", StringType()),
                        StructField("BRAND", StringType()),
                        StructField("PRODUCT_NAME", StringType()),
                        StructField("not_required", StringType()),
                        StructField("UNIT_PRICE", StringType()),
                        StructField("INV_QTY", StringType()),
                        StructField("INV_AMT", StringType())
                        
                        ])
                
        dataframe = session.read.schema(df_schema) \\
        .option("skip_header",3) \\
        .option("field_delimiter", "\\u0001") \\
        .option("encoding","UTF-8")\\
        .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
        
       
            
        
        dataframe=dataframe.na.drop("all")
        if dataframe.count()==0:
            return "No Data in file"

        full_path = "@"+stage_name+"/"+temp_stage_path+"/"+file_name.split(".")[0] + ".xlsx"
        with SnowflakeFile.open(full_path, "rb", require_scoped_url = False) as f:
            workbook = load_workbook(f)
            sn=workbook.sheetnames
        sn=sn[0]

        dataframe = dataframe.filter((col("MATL_NUM") != "TOTAL"))
        
        
        
        mnth_id = file_name.split(".")[0].split("_")[4]
        dataframe = dataframe.with_column("mnth_id",lit(mnth_id))
        dataframe = dataframe.with_column("DISTRIBUTOR_CD",lit(sn))
        dataframe = dataframe.with_column("FILE_NAME",lit(file_name.split(".")[0] + ''.xlsx''))
        dataframe = dataframe.with_column("CRTD_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        dataframe=dataframe.select(col("MNTH_ID"), col("MATL_NUM"), col("BRAND"),col("PRODUCT_NAME"), col("DISTRIBUTOR_CD"), col("UNIT_PRICE"),col("INV_QTY"), col("INV_AMT"), col("FILE_NAME"), col("CRTD_DTTM"))
        
        # Load Data to the target table
        dataframe.write.mode("append").saveAsTable(target_table)
       
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        #move to success
        file_name=file_name.split(".")[0]+''_''+datetime.now().strftime("%Y%m%d%H%M%S")
        dataframe.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)

        
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
		
		
CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.NTASDL_RAW.OTC_INV_3_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('openpyxl==3.1.2','snowflake-snowpark-python==*','unidecode==1.2.0')
HANDLER = 'main'
EXECUTE AS OWNER
AS 'import snowflake.snowpark as snowpark
from snowflake import snowpark
from snowflake.snowpark.window import Window
from snowflake.snowpark.functions import col,lit,concat,regexp_replace,trim,split,rtrim,upper,coalesce,row_number,when,to_date,is_null
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField,DecimalType,DateType
from snowflake.snowpark.files import SnowflakeFile
import pandas as pd
import numpy as np
import openpyxl
from datetime import datetime
import pytz
from unidecode import unidecode

from re import sub

def main(session: snowpark.Session,Param):
    

    try:

        #Param=[''OTC_KR_INV_3_201911.xlsx'',''NTASDL_RAW.DEV_LOAD_STAGE_ADLS_KOR'',''dev/otc_inventory/transactional/'',''SDL_KR_OTC_INVENTORY'']

        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        file_header_loc = 10

        #Reading data from adls
        full_path = "@"+stage_name+"/"+temp_stage_path+"/"+file_name
        
        with SnowflakeFile.open(full_path,''rb'',require_scoped_url= False) as f:
            excel_file = pd.ExcelFile(f)
            #print(excel_file)
            
            df1 = excel_file.parse(sheet_name=''133532'',skiprows=3,engine=''openpyxl'')
            df1 = df1.drop(df1.columns[0], axis=1)

            df1 = df1.applymap(lambda x: str(x))
            #df1 = df1.applymap(lambda x: unidecode(x))
            #print(df1)
            df1.rename(columns={df1.columns[0]:''MATL_NUM'',
                                df1.columns[1]:''BRAND'',
                                df1.columns[2]:''PRODUCT_NAME'',
                                df1.columns[3]:''RENAME'',
                                df1.columns[4]:''INV_QTY'',
                                df1.columns[5]:''UNIT_PRICE'',
                                df1.columns[6]:''INV_AMT''
                               },inplace = True)

            df1 = df1[df1["MATL_NUM"] != "TOTAL"]
            df1 = df1.drop(columns=[''Amount''])
            df1["DISTRIBUTOR_CD"] = ''133532''
            df1 = df1.drop(columns=[''RENAME''])
            mnth_id = file_name.split(".")[0].split("_")[4]
            df1["MNTH_ID"] = mnth_id
            dataframe=session.create_dataframe(df1)
            print(df1.columns)
            dataframe = dataframe.with_column("FILE_NAME",lit(file_name))
            dataframe = dataframe.with_column("CRTD_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
            dataframe=dataframe.select(col("MNTH_ID"), col("MATL_NUM"), col("BRAND"),col("PRODUCT_NAME"), col("DISTRIBUTOR_CD"), col("UNIT_PRICE"),col("INV_QTY"), col("INV_AMT"), col("FILE_NAME"), col("CRTD_DTTM"))


        with SnowflakeFile.open(full_path,''rb'',require_scoped_url= False) as f:
            excel_file = pd.ExcelFile(f)
            #print(excel_file)
            
            df2 = excel_file.parse(sheet_name=''120847'',skiprows=3,engine=''openpyxl'')
            df2 = df2.drop(df2.columns[0], axis=1)

            df2 = df2.applymap(lambda x: str(x))
            #df1 = df1.applymap(lambda x: unidecode(x))
            #print(df1)
            df2.rename(columns={df2.columns[0]:''MATL_NUM'',
                                df2.columns[1]:''BRAND'',
                                df2.columns[2]:''PRODUCT_NAME'',
                                df2.columns[3]:''RENAME'',
                                df2.columns[4]:''INV_QTY'',
                                df2.columns[5]:''UNIT_PRICE'',
                                df2.columns[6]:''INV_AMT''
                               },inplace = True)

            df2 = df2[df2["MATL_NUM"] != "TOTAL"]
            df2 = df2.drop(columns=[''Amount''])
            df2["DISTRIBUTOR_CD"] = ''120847''
            df2 = df2.drop(columns=[''RENAME''])
            mnth_id = file_name.split(".")[0].split("_")[4]
            df2["MNTH_ID"] = mnth_id
            dataframe2=session.create_dataframe(df2)
            dataframe2 = dataframe2.with_column("FILE_NAME",lit(file_name))
            dataframe2 = dataframe2.with_column("CRTD_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
            dataframe2=dataframe2.select(col("MNTH_ID"), col("MATL_NUM"), col("BRAND"),col("PRODUCT_NAME"), col("DISTRIBUTOR_CD"), col("UNIT_PRICE"),col("INV_QTY"), col("INV_AMT"), col("FILE_NAME"), col("CRTD_DTTM"))
            
        df_combined = dataframe.union(dataframe2)

        # Load Data to the target table
        df_combined.write.mode("append").saveAsTable(target_table)
       
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        #move to success
        file_name=file_name.split(".")[0]+''_''+datetime.now().strftime("%Y%m%d%H%M%S")
        df_combined.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)

        return ''Success''
            

    except KeyError as key_error:
        # Handle KeyError (missing columns) here
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message
        
    except Exception as e:
        # Handle exceptions here
        error_message = f"Error: {str(e)}"
        return error_message;';
		
		
CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.NTASDL_RAW.OTC_INV_4_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('openpyxl==3.1.2','snowflake-snowpark-python==*')
HANDLER = 'main'
EXECUTE AS OWNER
AS '# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them.

import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col,lit
from snowflake.snowpark.types import  StringType, StructType, StructField
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz
from snowflake.snowpark.functions import col



def main(session: snowpark.Session,Param): 
       
    try :
            
        #Param=[''OTC_KR_INV_4_202403.csv'',''NTASDL_RAW.DEV_LOAD_STAGE_ADLS_KOR'',''dev/otc_inventory/transactional/'',''SDL_KR_OTC_INVENTORY'']

        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        # Define the schema for the DataFrame
        
        df_schema=StructType([
                        StructField("MATL_NUM", StringType()),
                        StructField("BRAND", StringType()),
                        StructField("PRODUCT_NAME", StringType()),
                        StructField("UNIT_PRICE", StringType()),
                        StructField("inv_qty1", StringType()),
                        StructField("inv_amt1", StringType()),
                        StructField("inv_qty2", StringType()),
                        StructField("inv_amt2", StringType()),
                        StructField("inv_qty3", StringType()),
                        StructField("inv_amt3", StringType()),
                        StructField("inv_qty4", StringType()),
                        StructField("inv_amt4", StringType())
                       
                        
                        ])
                
        dataframe = session.read.schema(df_schema) \\
        .option("skip_header",5) \\
        .option("field_delimiter", "\\u0001") \\
        .option("encoding","UTF-8")\\
        .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
        
       
            
        
        dataframe=dataframe.na.drop("all")
        if dataframe.count()==0:
            return "No Data in file"

       

        dataframe = dataframe.filter((col("MATL_NUM") != "Total"))
        
        
        
        mnth_id = file_name.split(".")[0].split("_")[4]
        dataframe = dataframe.with_column("mnth_id",lit(mnth_id))
        dataframe = dataframe.with_column("FILE_NAME",lit(file_name.split(".")[0] + ''.xlsx''))
        dataframe = dataframe.with_column("CRTD_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))

        pd_dataframe = dataframe.to_pandas()
        dist_cd_list = ["140403", "128004", "128032", "127969"]

        
        df_qty = pd_dataframe.melt(id_vars=["MNTH_ID", "MATL_NUM", "BRAND", "PRODUCT_NAME", "UNIT_PRICE", "FILE_NAME", "CRTD_DTTM"],
                 value_vars=["INV_QTY1", "INV_QTY2", "INV_QTY3", "INV_QTY4"],
                 var_name="inv_qty_type",
                 value_name="INV_QTY")
       
        
        df_amt = pd_dataframe.melt(id_vars=["MNTH_ID", "MATL_NUM", "BRAND", "PRODUCT_NAME", "UNIT_PRICE", "FILE_NAME", "CRTD_DTTM"],
                         value_vars=["INV_AMT1", "INV_AMT2", "INV_AMT3", "INV_AMT4"],
                         var_name="inv_amt_type",
                         value_name="INV_AMT")

        
        
        # Combine the melted DataFrames
  
        df_combined = pd.concat([df_qty.drop(columns="inv_qty_type"), df_amt["INV_AMT"]], axis=1)

        df_combined = df_combined.sort_values(by=''PRODUCT_NAME'')

        df_combined["DISTRIBUTOR_CD"]=dist_cd_list * (len(df_combined)//4)
        
        #pandas df to snowpark df 
        dataframe=session.create_dataframe(df_combined)
        print(dataframe.columns)
        dataframe=dataframe.select(col("MNTH_ID"), col("MATL_NUM"), col("BRAND"),col("PRODUCT_NAME"), col("DISTRIBUTOR_CD"), col("UNIT_PRICE"),col("INV_QTY"), col("INV_AMT"), col("FILE_NAME"), col("CRTD_DTTM"))
        # # Load Data to the target table
        dataframe.write.mode("append").saveAsTable(target_table)
       
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        #move to success
        file_name=file_name.split(".")[0]+''_''+datetime.now().strftime("%Y%m%d%H%M%S")
        dataframe.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)

        
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
		
		
CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.NTASDL_RAW.OTC_INV_5_PREPROCESSING("PARAM" ARRAY)
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
import snowflake.snowpark as snowpark
import pytz
from snowflake.snowpark.functions import col



def main(session: snowpark.Session,Param): 
       
    try :
            
        #Param=[''OTC_KR_INV_5_202403.csv'',''NTASDL_RAW.DEV_LOAD_STAGE_ADLS_KOR'',''dev/otc_inventory/transactional/'',''SDL_KR_OTC_INVENTORY'']

        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        # Define the schema for the DataFrame
        
        df_schema=StructType([
                        StructField("MATL_NUM", StringType()),
                        StructField("BRAND", StringType()),
                        StructField("PRODUCT_NAME", StringType()),
                        StructField("UNIT_PRICE", StringType()),
                        StructField("INV_QTY1", StringType()),
                        StructField("INV_AMT1", StringType()),
                        StructField("INV_QTY2", StringType()),
                        StructField("INV_AMT2", StringType()),
                        StructField("INV_QTY3", StringType()),
                        StructField("INV_AMT3", StringType()),
                        StructField("INV_QTY4", StringType()),
                        StructField("INV_AMT4", StringType()),
                        StructField("INV_QTY5", StringType()),
                        StructField("INV_AMT5", StringType()),
                        StructField("INV_QTY6", StringType()),
                        StructField("INV_AMT6", StringType()),
                        StructField("INV_QTY7", StringType()),
                        StructField("INV_AMT7", StringType()),
                        StructField("INV_QTY8", StringType()),
                        StructField("INV_AMT8", StringType()),
                        StructField("INV_QTY9", StringType()),
                        StructField("INV_AMT9", StringType()),
                        StructField("INV_QTY10", StringType()),
                        StructField("INV_AMT10", StringType()),
                        StructField("INV_QTY11", StringType()),
                        StructField("INV_AMT11", StringType()),
                        StructField("INV_QTY12", StringType()),
                        StructField("INV_AMT12", StringType()),
                        StructField("INV_QTY13", StringType()),
                        StructField("INV_AMT13", StringType()),
                        StructField("INV_QTY14", StringType()),
                        StructField("INV_AMT14", StringType()),
                        StructField("INV_QTY15", StringType()),
                        StructField("INV_AMT15", StringType()),
                        StructField("INV_QTY16", StringType()),
                        StructField("INV_AMT16", StringType()),
                        StructField("INV_QTY17", StringType()),
                        StructField("INV_AMT17", StringType()),
                        StructField("INV_QTY18", StringType()),
                        StructField("INV_AMT18", StringType()),
                        StructField("INV_QTY19", StringType()),
                        StructField("INV_AMT19", StringType()),
                        StructField("INV_QTY20", StringType()),
                        StructField("INV_AMT20", StringType()),
                        StructField("INV_QTY21", StringType()),
                        StructField("INV_AMT21", StringType()),
                        StructField("INV_QTY22", StringType()),
                        StructField("INV_AMT22", StringType()),
                        StructField("INV_QTY23", StringType()),
                        StructField("INV_AMT23", StringType()),
                        StructField("INV_QTY24", StringType()),
                        StructField("INV_AMT24", StringType()),
                        StructField("INV_QTY25", StringType()),
                        StructField("INV_AMT25", StringType()),
                        StructField("INV_QTY26", StringType()),
                        StructField("INV_AMT26", StringType()),
                        StructField("INV_QTY27", StringType()),
                        StructField("INV_AMT27", StringType()),
                        StructField("INV_QTY28", StringType()),
                        StructField("INV_AMT28", StringType()),
                        StructField("INV_QTY29", StringType()),
                        StructField("INV_AMT29", StringType()),
                        StructField("INV_QTY30", StringType()),
                        StructField("INV_AMT30", StringType()),
                        StructField("INV_QTY31", StringType()),
                        StructField("INV_AMT31", StringType()),
                        StructField("INV_QTY32", StringType()),
                        StructField("INV_AMT32", StringType()),
                        StructField("INV_QTY33", StringType()),
                        StructField("INV_AMT33", StringType()),
                        StructField("INV_QTY34", StringType()),
                        StructField("INV_AMT34", StringType()),
                        StructField("INV_QTY35", StringType()),
                        StructField("INV_AMT35", StringType()),
                        StructField("INV_QTY36", StringType()),
                        StructField("INV_AMT36", StringType()),
                        StructField("INV_QTY37", StringType()),
                        StructField("INV_AMT37", StringType()),
                        StructField("INV_QTY38", StringType()),
                        StructField("INV_AMT38", StringType()),
                        StructField("INV_QTY39", StringType()),
                        StructField("INV_AMT39", StringType()),
                        StructField("INV_QTY40", StringType()),
                        StructField("INV_AMT40", StringType()),
                        StructField("INV_QTY41", StringType()),
                        StructField("INV_AMT41", StringType()),
                        StructField("INV_QTY42", StringType()),
                        StructField("INV_AMT42", StringType()),
                        StructField("INV_QTY43", StringType()),
                        StructField("INV_AMT43", StringType()),
                        StructField("INV_QTY44", StringType()),
                        StructField("INV_AMT44", StringType()),
                        StructField("INV_QTY45", StringType()),
                        StructField("INV_AMT45", StringType()),
                        StructField("INV_QTY46", StringType()),
                        StructField("INV_AMT46", StringType()),
                        StructField("INV_QTY47", StringType()),
                        StructField("INV_AMT47", StringType())

                       
                        
                        ])
                
        dataframe = session.read.schema(df_schema) \\
        .option("skip_header",15) \\
        .option("field_delimiter", "\\u0001") \\
        .option("encoding","UTF-8")\\
        .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
        
       
            
        
        dataframe=dataframe.na.drop("all")
        if dataframe.count()==0:
            return "No Data in file"

       

        dataframe = dataframe.filter((col("MATL_NUM") != "Total"))
        
        
        
        mnth_id = file_name.split(".")[0].split("_")[4]
        dataframe = dataframe.with_column("mnth_id",lit(mnth_id))
        dataframe = dataframe.with_column("FILE_NAME",lit(file_name.split(".")[0] + ''.xlsx''))
        dataframe = dataframe.with_column("CRTD_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))

        pd_dataframe = dataframe.to_pandas()
        dist_cd_list = ["121087","131201","121276","137168","137169","121094","121125","130757","121207",
                            "128675","130581","130756","134618","137167","137185","121131","121237","122832",
                            "128673","130597","131993","132777","134420","121188","121229","122140","130819",
                            "121151","121190","121277","121495","126836","121187","121230","121270","129085",
                            "121208","121238","122935","136597","121086","121088","121205","121206","124424",
                            "130818","130973"]

        
        df_qty = pd_dataframe.melt(id_vars=["MNTH_ID", "MATL_NUM", "BRAND", "PRODUCT_NAME", "UNIT_PRICE", "FILE_NAME", "CRTD_DTTM"],
                 value_vars=["INV_QTY1", "INV_QTY2", "INV_QTY3", "INV_QTY4","INV_QTY5","INV_QTY6","INV_QTY7",
                                "INV_QTY8", "INV_QTY9", "INV_QTY10", "INV_QTY11","INV_QTY12","INV_QTY13","INV_QTY14",
                                "INV_QTY15", "INV_QTY16", "INV_QTY17", "INV_QTY18","INV_QTY18","INV_QTY19","INV_QTY20",
                                "INV_QTY21", "INV_QTY22", "INV_QTY23", "INV_QTY24","INV_QTY25","INV_QTY26","INV_QTY27",
                                "INV_QTY28", "INV_QTY29", "INV_QTY30", "INV_QTY31","INV_QTY32","INV_QTY33","INV_QTY34",
                                "INV_QTY35", "INV_QTY36", "INV_QTY37", "INV_QTY38","INV_QTY39","INV_QTY40","INV_QTY41",
                                "INV_QTY42", "INV_QTY43", "INV_QTY44", "INV_QTY45","INV_QTY46","INV_QTY47"
                                ],
                 var_name="inv_qty_type",
                 value_name="INV_QTY")
       
        
        df_amt = pd_dataframe.melt(id_vars=["MNTH_ID", "MATL_NUM", "BRAND", "PRODUCT_NAME", "UNIT_PRICE", "FILE_NAME", "CRTD_DTTM"],
                         value_vars=["INV_AMT1", "INV_AMT2", "INV_AMT3", "INV_AMT4","INV_AMT5","INV_AMT6","INV_AMT7",
                                    "INV_AMT8", "INV_AMT9", "INV_AMT10", "INV_AMT11","INV_AMT12","INV_AMT13","INV_AMT14",
                                    "INV_AMT15", "INV_AMT16", "INV_AMT17", "INV_AMT18","INV_AMT18","INV_AMT19","INV_AMT20",
                                    "INV_AMT21", "INV_AMT22", "INV_AMT23", "INV_AMT24","INV_AMT25","INV_AMT26","INV_AMT27",
                                    "INV_AMT28", "INV_AMT29", "INV_AMT30", "INV_AMT31","INV_AMT32","INV_AMT33","INV_AMT34",
                                    "INV_AMT35", "INV_AMT36", "INV_AMT37", "INV_AMT38","INV_AMT39","INV_AMT40","INV_AMT41",
                                    "INV_AMT42", "INV_AMT43", "INV_AMT44", "INV_AMT45","INV_AMT46","INV_AMT47"
                                    ],
                         var_name="inv_amt_type",
                         value_name="INV_AMT")

        
        
        # Combine the melted DataFrames
  
        df_combined = pd.concat([df_qty.drop(columns="inv_qty_type"), df_amt["INV_AMT"]], axis=1)

        df_combined = df_combined.sort_values(by=''PRODUCT_NAME'')

        df_combined["DISTRIBUTOR_CD"]=dist_cd_list * (len(df_combined)//47)
        
        # #pandas df to snowpark df 
        dataframe=session.create_dataframe(df_combined)
        dataframe=dataframe.select(col("MNTH_ID"), col("MATL_NUM"), col("BRAND"),col("PRODUCT_NAME"), col("DISTRIBUTOR_CD"), col("UNIT_PRICE"),col("INV_QTY"), col("INV_AMT"), col("FILE_NAME"), col("CRTD_DTTM"))
        # # # Load Data to the target table
        dataframe.write.mode("append").saveAsTable(target_table)
       
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        #move to success
        file_name=file_name.split(".")[0]+''_''+datetime.now().strftime("%Y%m%d%H%M%S")
        dataframe.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)

        
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


CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.NTASDL_RAW.POP6_TH_POP_LISTS_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS 'from snowflake.snowpark.functions import col, lit, date_format, current_timestamp, to_date,md5, year, month, concat, format_number, regexp_replace, to_timestamp, when, trim, upper
from snowflake.snowpark.types import StringType, StructType, StructField,DecimalType
import snowflake.snowpark
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz

def main(session: snowpark.Session,Param):
    #Param=["20240422_pop_lists.csv","THASDL_RAW.DEV_LOAD_STAGE_ADLS","dev/pop6","SDL_POP6_TH_POP_LISTS"]
    try:
        file_name = Param[0]
        stage_name = Param[1]
        temp_stage_path = Param[2]
        target_table = Param[3]

        df_schema = StructType([
                    StructField("Status", StringType(), nullable=True),
                    StructField("POP_List", StringType(), nullable=True),
                    StructField("POPDB_ID", StringType(), nullable=True),
                    StructField("POP_Code", StringType(), nullable=True),
                    StructField("POP_Name", StringType(), nullable=True),
                    StructField("Date", StringType(), nullable=True)
                ])
        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "|")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name.split(".")[0])
        
              
        df = df.na.drop("all")
        if df.count() == 0:
            return "No Data in file"

        
        df = df.withColumn("filename", lit(file_name))
        df = df.withColumn("crt_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        df = df.withColumn("run_id", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))

        df1=df
        
        df1=df1.na.fill("",subset=["Status",
                                "POP_List",
                                "POPDB_ID",
                                "POP_Code",
                                "POP_Name"
                                ])

        df1 = df1.withColumn("hash_c",concat(df1.Status,df1.POP_List,df1.POPDB_ID,df1.POP_Code,df1.POP_Name))
        df1=df1.withColumn("hash",md5(col("hash_c")))
        df1=df1.na.replace({"":None},["Status",
                                    "POP_List",
                                    "POPDB_ID",
                                    "POP_Code",
                                    "POP_Name"
                                ])
        snowdf = df1.select(
                           "Status",
                            "POP_List",
                            "POPDB_ID",
                            "POP_Code",
                            "POP_Name",
                            "Date",
                            "filename",
                            "run_id",
                            "crt_dttm",
                            "hash"
                        )
        
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        
        #move to success
        
        
        snowdf.write.mode("append").saveAsTable(stage_name.split(".")[0] + "." + target_table)
        snowdf.write.copy_into_location("@" + stage_name + "/" + temp_stage_path + "/processed/success/" + formatted_year + "/" + formatted_month + "/" + file_name, header=True, OVERWRITE=True)
        
        return "Success"

    except KeyError as key_error:
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message

    except pd.errors.MergeError as merge_error:
        error_message = f"DataFrame merging error: {str(merge_error)}"
        return error_message

    except Exception as e:
        error_message = f"Error: {str(e)}"
        return error_message
        
        
        
        
        ';

==

CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.NTASDL_RAW.POP6_HK_PRODUCTS("PARAM" ARRAY)
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
    #Param=["20240609_products.csv","NTASDL_RAW.DEV_LOAD_STAGE_ADLS_HKG","dev/pop6/masterdata/products/","sdl_pop6_hk_products"]
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        
        df_schema = StructType([
                StructField("Status", StringType(), True),
                StructField("productdb_id", StringType(), True),
                StructField("barcode", StringType(), True),
                StructField("sku", StringType(), True),
                StructField("unit_price", StringType(), True),
                StructField("display_order", StringType(), True),
                StructField("launch_date", StringType(), True),
                StructField("largest_uom_quantity", StringType(), True),
                StructField("middle_uom_quantity", StringType(), True),
                StructField("smallest_uom_quantity", StringType(), True),
                StructField("sku_english", StringType(), True),
                StructField("sku_code", StringType(), True),
                StructField("ps_category", StringType(), True),
                StructField("ps_segment", StringType(), True),
                StructField("ps_category_segment", StringType(), True),
                StructField("company", StringType(), True),
                StructField("country_l1", StringType(), True),
                StructField("regional_franchise_l2", StringType(), True),
                StructField("franchise_l3", StringType(), True),
                StructField("brand_l4", StringType(), True),
                StructField("sub_category_l5", StringType(), True),
                StructField("platform_level_l6", StringType(), True),
                StructField("variance_l7", StringType(), True),
                StructField("pack_size_l8", StringType(), True)
                
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
            
            

        df = df.with_column(''hashkey'',md5(concat(when(is_null(col(''status'')),lit('''')).otherwise(col(''status'')),\\
                                            when(is_null(col(''productdb_id'')),lit('''')).otherwise(col(''productdb_id'')),\\
                                          when(is_null(col(''barcode'')),lit('''')).otherwise(col(''barcode'')),\\
                                          when(is_null(col(''sku'')),lit('''')).otherwise(col(''sku'')),\\
                                         when(is_null(col(''unit_price'')),lit('''')).otherwise(col(''unit_price'')),\\
                                         when(is_null(col(''display_order'')),lit('''')).otherwise(col(''display_order'')),\\
                                         when(is_null(col(''launch_date'')),lit('''')).otherwise(col(''launch_date'')),\\
                                          when(is_null(col(''largest_uom_quantity'')),lit('''')).otherwise(col(''largest_uom_quantity'')),\\
                                         when(is_null(col(''middle_uom_quantity'')),lit('''')).otherwise(col(''middle_uom_quantity'')),\\
                                         when(is_null(col(''smallest_uom_quantity'')),lit('''')).otherwise(col(''smallest_uom_quantity'')),\\
                                         when(is_null(col(''company'')),lit('''')).otherwise(col(''company'')),\\
                                         when(is_null(col(''sku_english'')),lit('''')).otherwise(col(''sku_english'')),\\
                                         when(is_null(col(''sku_code'')),lit('''')).otherwise(col(''sku_code'')),\\
                                        when(is_null(col(''ps_category'')),lit('''')).otherwise(col(''ps_category'')),\\
                                          when(is_null(col(''ps_segment'')),lit('''')).otherwise(col(''ps_segment'')),\\
                                         when(is_null(col(''ps_category_segment'')),lit('''')).otherwise(col(''ps_category_segment'')),\\
                                         when(is_null(col(''country_l1'')),lit('''')).otherwise(col(''country_l1'')),\\
                                         when(is_null(col(''regional_franchise_l2'')),lit('''')).otherwise(col(''regional_franchise_l2'')),\\
                                          when(is_null(col(''franchise_l3'')),lit('''')).otherwise(col(''franchise_l3'')),\\
                                         when(is_null(col(''brand_l4'')),lit('''')).otherwise(col(''brand_l4'')),\\
                                         when(is_null(col(''sub_category_l5'')),lit('''')).otherwise(col(''sub_category_l5'')),\\
                                         when(is_null(col(''platform_level_l6'')),lit('''')).otherwise(col(''platform_level_l6'')),\\
                                         when(is_null(col(''variance_l7'')),lit('''')).otherwise(col(''variance_l7'')),\\
                                         when(is_null(col(''pack_size_l8'')),lit('''')).otherwise(col(''pack_size_l8'')))))

        
        df = df.with_column(''run_id'',lit(generunid()))

        df = df.with_column(''crtd_dttm'',lit(generatecrtd_dttm()))
        

        

        
        snowdf= df.select(
                   "Status", 
                "productdb_id", 
                "barcode", 
                "sku", 
                "unit_price", 
                "display_order", 
                "launch_date", 
                "largest_uom_quantity", 
                "middle_uom_quantity", 
                "smallest_uom_quantity", 
                "company", 
                "sku_english", 
                "sku_code", 
                "ps_category", 
                "ps_segment", 
                "ps_category_segment", 
                "country_l1", 
                "regional_franchise_l2", 
                "franchise_l3", 
                "brand_l4", 
                "sub_category_l5", 
                "platform_level_l6", 
                "variance_l7", 
                "pack_size_l8",
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

==

CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.NTASDL_RAW.POP6_HK_TRANS_SERVICE_LEVELS_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them.

import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import lit
from snowflake.snowpark.types import  StringType, StructType, StructField
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz
from datetime import datetime, timedelta



def main(session: snowpark.Session,Param): 
       
    try :
            
        # Param=[''20240612_service_levels.csv'',''NTASDL_RAW.DEV_LOAD_STAGE_ADLS_HKG'',''dev/pop6/visitdata/'',''sdl_pop6_hk_service_levels'']

        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        # Define the schema for the DataFrame
        
        df_schema=StructType([
                        StructField("customer", StringType()),
                        StructField("customer_grade", StringType()),
                        StructField("team", StringType()),
                        StructField("visit_frequency", StringType()),
                        StructField("estimated_visit_duration", StringType()),
                        StructField("service_level_date", StringType())
                        
                        ])
                
        dataframe = session.read.schema(df_schema) \\
        .option("skip_header",1) \\
        .option("field_delimiter", ",") \\
        .option("encoding","UTF-8")\\
        .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
        
       
            
        
        dataframe=dataframe.na.drop("all")
        if dataframe.count()==0:
            return "No Data in file"

       
        
        
        
        dataframe = dataframe.with_column("FILE_NAME", lit(file_name) )
        dataframe = dataframe.with_column("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        dataframe = dataframe.with_column("CRTD_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
       
        
        # Load Data to the target table
        dataframe.write.mode("append").saveAsTable(target_table)
       
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        #move to success
        file_name=file_name.split(".")[0]+''_''+datetime.now().strftime("%Y%m%d%H%M%S")
        dataframe.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)

        
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



CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.NTASDL_RAW.POP6_HK_TRANS_EXECUTED_VISITS_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them.

import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import lit
from snowflake.snowpark.types import  StringType, StructType, StructField,DecimalType,IntegerType
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz
from datetime import datetime, timedelta



def main(session: snowpark.Session,Param): 
       
    try :
            
        #Param=[''executed_visits.csv'',''NTASDL_RAW.DEV_LOAD_STAGE_ADLS_HKG'',''dev/pop6/visitdata/'',''sdl_pop6_hk_executed_visits'']

        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        # Define the schema for the DataFrame
        
        df_schema=StructType([
                        StructField("visit_id", StringType()),
                        StructField("visit_date", StringType()),
                        StructField("check_in_datetime", StringType()),
                        StructField("check_out_datetime", StringType()),
                        StructField("popdb_id", StringType()),
                        StructField("pop_code", StringType()),
                        StructField("pop_name", StringType()),
                        StructField("address", StringType()),
                        StructField("check_in_longitude", DecimalType(18, 5)),
                        StructField("check_in_latitude", DecimalType(18, 5)),
                        StructField("check_out_longitude", DecimalType(18, 5)),
                        StructField("check_out_latitude", DecimalType(18, 5)),
                        StructField("check_in_photo", StringType()),
                        StructField("check_out_photo", StringType()),
                        StructField("username", StringType()),
                        StructField("user_full_name", StringType()),
                        StructField("superior_username", StringType()),
                        StructField("superior_name", StringType()),
                        StructField("planned_visit", StringType()),
                        StructField("cancelled_visit", IntegerType()),
                        StructField("cancellation_reason", IntegerType()),
                        StructField("cancellation_note", StringType())
            
                        
                        ])
                
        dataframe = session.read.schema(df_schema) \\
        .option("skip_header",1) \\
        .option("field_delimiter", ",") \\
        .option("field_optionally_enclosed_by", "\\"") \\
        .option("encoding","UTF-8")\\
        .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
        
       
            
        
        dataframe=dataframe.na.drop("all")
        if dataframe.count()==0:
            return "No Data in file"

       
        
        
        # Get the current date and time
        current_datetime = datetime.now()
        # Subtract one day
        yesterday_datetime = current_datetime - timedelta(days=1)
        dataframe = dataframe.with_column("FILE_NAME",lit(  file_name ))
        dataframe = dataframe.with_column("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        dataframe = dataframe.with_column("CRTD_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
       
        
        # Load Data to the target table
        dataframe.write.mode("append").saveAsTable(target_table)
       
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        #move to success
        file_name=file_name.split(".")[0]+''_''+datetime.now().strftime("%Y%m%d%H%M%S")
        dataframe.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)

        
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

=


CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.NTASDL_RAW.POP6_HK_TRANS_PLANNED_VISITS_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them.

import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import lit
from snowflake.snowpark.types import  StringType, StructType, StructField,DecimalType,IntegerType
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz
from datetime import datetime, timedelta



def main(session: snowpark.Session,Param): 
       
    try :
            
        #Param=[''planned_visits.csv'',''NTASDL_RAW.DEV_LOAD_STAGE_ADLS_HKG'',''dev/pop6/visitdata/'',''sdl_pop6_hk_planned_visits'']

        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        # Define the schema for the DataFrame
        
        df_schema=StructType([
                        StructField("planned_visit_date", StringType()),
                        StructField("popdb_id", StringType()),
                        StructField("pop_code", StringType()),
                        StructField("pop_name", StringType()),
                        StructField("address", StringType()),
                        StructField("username", StringType()),
                        StructField("user_full_name", StringType())
            
                        
                        ])
                
        dataframe = session.read.schema(df_schema) \\
        .option("skip_header",1) \\
        .option("field_delimiter", ",") \\
        .option("field_optionally_enclosed_by", "\\"") \\
        .option("encoding","UTF-8")\\
        .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
        
       
            
        
        dataframe=dataframe.na.drop("all")
        if dataframe.count()==0:
            return "No Data in file"

       
        
        
     
        dataframe = dataframe.with_column("FILE_NAME",lit( file_name ))
        dataframe = dataframe.with_column("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        dataframe = dataframe.with_column("CRTD_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
       
        
        # Load Data to the target table
        dataframe.write.mode("append").saveAsTable(target_table)
       
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        #move to success
        file_name=file_name.split(".")[0]+''_''+datetime.now().strftime("%Y%m%d%H%M%S")
        dataframe.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)

        
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

==

CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.NTASDL_RAW.POP6_HK_TRANS_TASKS_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them.

import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import lit
from snowflake.snowpark.types import  StringType, StructType, StructField,IntegerType
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz
from datetime import datetime, timedelta



def main(session: snowpark.Session,Param): 
       
    try :
           
        # Param=[''20231020_tasks.csv'',''NTASDL_RAW.DEV_LOAD_STAGE_ADLS_HKG'',''dev/pop6/taskdata/'',''sdl_pop6_hk_tasks'']

        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        # Define the schema for the DataFrame
        
        df_schema=StructType([
                        StructField("visit_id", StringType()),
                        StructField("task_group", StringType()),
                        StructField("task_id", IntegerType()),
                        StructField("task_name", StringType()),
                        StructField("field_id", StringType()),
                        StructField("field_code", StringType()),
                        StructField("field_label", StringType()),
                        StructField("field_type", StringType()),
                        StructField("dependent_on_field_id", StringType()),
                        StructField("response", StringType())
            
            
                        
                        ])
                
        dataframe = session.read.schema(df_schema) \\
        .option("skip_header",1) \\
        .option("field_delimiter", ",") \\
        .option("field_optionally_enclosed_by", "\\"") \\
        .option("encoding","UTF-8")\\
        .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
        
       
            
        
        dataframe=dataframe.na.drop("all")
        if dataframe.count()==0:
            return "No Data in file"

       
        
        
     
        dataframe = dataframe.with_column("FILE_NAME",lit( file_name ))
        dataframe = dataframe.with_column("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        dataframe = dataframe.with_column("CRTD_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
       
        
        # Load Data to the target table
        dataframe.write.mode("append").saveAsTable(target_table)
       
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        #move to success
        file_name=file_name.split(".")[0]+''_''+datetime.now().strftime("%Y%m%d%H%M%S")
        dataframe.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)

        
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




CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.NTASDL_RAW.POP6_HK_TRANS_TASKS_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them.

import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import lit
from snowflake.snowpark.types import  StringType, StructType, StructField,IntegerType
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz
from datetime import datetime, timedelta



def main(session: snowpark.Session,Param): 
       
    try :
           
        # Param=[''20231020_tasks.csv'',''NTASDL_RAW.DEV_LOAD_STAGE_ADLS_HKG'',''dev/pop6/taskdata/'',''sdl_pop6_hk_tasks'']

        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        # Define the schema for the DataFrame
        
        df_schema=StructType([
                        StructField("visit_id", StringType()),
                        StructField("task_group", StringType()),
                        StructField("task_id", IntegerType()),
                        StructField("task_name", StringType()),
                        StructField("field_id", StringType()),
                        StructField("field_code", StringType()),
                        StructField("field_label", StringType()),
                        StructField("field_type", StringType()),
                        StructField("dependent_on_field_id", StringType()),
                        StructField("response", StringType())
            
            
                        
                        ])
                
        dataframe = session.read.schema(df_schema) \\
        .option("skip_header",1) \\
        .option("field_delimiter", ",") \\
        .option("field_optionally_enclosed_by", "\\"") \\
        .option("encoding","UTF-8")\\
        .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
        
       
            
        
        dataframe=dataframe.na.drop("all")
        if dataframe.count()==0:
            return "No Data in file"

       
        
        
     
        dataframe = dataframe.with_column("FILE_NAME",lit( file_name ))
        dataframe = dataframe.with_column("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        dataframe = dataframe.with_column("CRTD_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
       
        
        # Load Data to the target table
        dataframe.write.mode("append").saveAsTable(target_table)
       
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        #move to success
        file_name=file_name.split(".")[0]+''_''+datetime.now().strftime("%Y%m%d%H%M%S")
        dataframe.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)

        
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


=


CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.NTASDL_RAW.POP6_KR_TRANS_PRODUCT_ATTRIBUTE_AUDITS_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them.

import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import lit
from snowflake.snowpark.types import  StringType, StructType, StructField
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz
from datetime import datetime, timedelta                



def main(session: snowpark.Session, Param): 
       
    try :
            
        # Param=[''20240612_product_attribute_audits.csv'',''NTASDL_RAW.DEV_LOAD_STAGE_ADLS_KOR'',''dev/pop6/auditdata/'',''sdl_pop6_kr_product_attribute_audits'']  

        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        # Define the schema for the DataFrame
        
        df_schema=StructType([
                        StructField("Visit_ID", StringType()),
                        StructField("Audit_Form_ID", StringType()),
                        StructField("Audit_Form", StringType()),
                        StructField("Section_ID", StringType()),
                        StructField("Section", StringType()),
                        StructField("Field_ID", StringType()),
                        StructField("Field_Code", StringType()),
                        StructField("Field_Label", StringType()),
                        StructField("Field_Type", StringType()),
                        StructField("Dependent_On_Field_ID", StringType()),
                        StructField("Product_Attribute_ID", StringType()),
                        StructField("Product_Attribute", StringType()),
                        StructField("Product_Attribute_Value_ID", StringType()),
                        StructField("Product_Attribute_Value", StringType()),
                        StructField("Response", StringType())
                        ])
                
        dataframe = session.read.schema(df_schema) \\
        .option("skip_header",1) \\
        .option("field_delimiter", ",") \\
        .option("field_optionally_enclosed_by", "\\"") \\
        .option("encoding","UTF-8")\\
        .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
        
       
            
        
        dataframe=dataframe.na.drop("all")
        if dataframe.count()==0:
            return "No Data in file"    

        dataframe = dataframe.with_column("FILE_NAME",lit(file_name))
        dataframe = dataframe.with_column("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        dataframe = dataframe.with_column("CRTD_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        
        # Load Data to the target table
        dataframe.write.mode("append").saveAsTable(target_table)
       
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        #move to success
        file_name=file_name.split(".")[0]+"_"+datetime.now().strftime("%Y%m%d%H%M%S")
        dataframe.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)
        
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



CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.NTASDL_RAW.POP6_KR_TRANS_SKU_AUDITS_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them.

import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import lit
from snowflake.snowpark.types import  StringType, StructType, StructField
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz
from datetime import datetime, timedelta                



def main(session: snowpark.Session, Param): 
       
    try :
            
        # Param=[''20240612_sku_audits.csv'',''NTASDL_RAW.DEV_LOAD_STAGE_ADLS_KOR'',''dev/pop6/auditdata/'',''sdl_pop6_kr_sku_audits'']  

        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        # Define the schema for the DataFrame
        
        df_schema=StructType([
                        StructField("Visit_ID", StringType()),
                        StructField("Audit_Form_ID", StringType()),
                        StructField("Audit_Form", StringType()),
                        StructField("Section_ID", StringType()),
                        StructField("Section", StringType()),
                        StructField("Field_ID", StringType()),
                        StructField("Field_Code", StringType()),
                        StructField("Field_Label", StringType()),
                        StructField("Field_Type", StringType()),
                        StructField("Dependent_On_Field_ID", StringType()),
                        StructField("SKU_ID", StringType()),
                        StructField("SKU", StringType()),
                        StructField("Response", StringType())
                        ])
                
        dataframe = session.read.schema(df_schema) \\
        .option("skip_header",1) \\
        .option("field_delimiter", ",") \\
        .option("field_optionally_enclosed_by", "\\"") \\
        .option("encoding","UTF-8")\\
        .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
        
       
            
        
        dataframe=dataframe.na.drop("all")
        if dataframe.count()==0:
            return "No Data in file"    

       
        
        # # Get the current date and time
        # current_datetime = datetime.now()
        # # Subtract one day
        # yesterday_datetime = current_datetime - timedelta(days=1)
        # dataframe = dataframe.with_column("FILE_NAME",lit( yesterday_datetime.strftime("%Y%m%d") +"_"+ file_name ))
        dataframe = dataframe.with_column("FILE_NAME",lit(file_name))
        dataframe = dataframe.with_column("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        dataframe = dataframe.with_column("CRTD_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        
        # Load Data to the target table
        dataframe.write.mode("append").saveAsTable(target_table)
       
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        #move to success
        file_name=file_name.split(".")[0]+"_"+datetime.now().strftime("%Y%m%d%H%M%S")
        dataframe.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)
        
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




CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.NTASDL_RAW.POP6_KR_TRANS_PLANNED_VISITS_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them.

import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import lit
from snowflake.snowpark.types import  StringType, StructType, StructField
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz
from datetime import datetime, timedelta                



def main(session: snowpark.Session, Param): 
       
    try :
            
        # Param=[''20240612_planned_visits.csv'',''NTASDL_RAW.DEV_LOAD_STAGE_ADLS_KOR'',''dev/pop6/visitdata/'',''sdl_pop6_kr_planned_visits'']  

        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        # Define the schema for the DataFrame
        df_schema=StructType([
                        StructField("Planned_Visit_Date", StringType()),
                        StructField("POPDB_ID", StringType()),
                        StructField("POP_Code", StringType()),
                        StructField("POP_Name", StringType()),
                        StructField("Address", StringType()),
                        StructField("Username", StringType()),
                        StructField("User_Full_name", StringType())
                        ])
                
        dataframe = session.read.schema(df_schema) \\
        .option("skip_header",1) \\
        .option("field_delimiter", ",") \\
        .option("field_optionally_enclosed_by", "\\"") \\
        .option("encoding","UTF-8")\\
        .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
        
       
            
        
        dataframe=dataframe.na.drop("all")
        if dataframe.count()==0:
            return "No Data in file"    

       
        
        dataframe = dataframe.with_column("FILE_NAME",lit(file_name))
        dataframe = dataframe.with_column("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        dataframe = dataframe.with_column("CRTD_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        
        # Load Data to the target table
        dataframe.write.mode("append").saveAsTable(target_table)
       
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        #move to success
        file_name=file_name.split(".")[0]+"_"+datetime.now().strftime("%Y%m%d%H%M%S")
        dataframe.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)
        
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




CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.NTASDL_RAW.POP6_KR_TRANS_SERVICE_LEVELS_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them.

import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import lit
from snowflake.snowpark.types import  StringType, StructType, StructField
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz
from datetime import datetime, timedelta                



def main(session: snowpark.Session, Param): 
       
    try :
            
        # Param=[''20240612_service_levels.csv'',''NTASDL_RAW.DEV_LOAD_STAGE_ADLS_KOR'',''dev/pop6/visitdata/'',''sdl_pop6_kr_service_levels'']  

        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        # Define the schema for the DataFrame
        df_schema=StructType([
                        StructField("Retail Environment (PS)", StringType()),
                        StructField("Customer Grade", StringType()),
                        StructField("Team", StringType()),
                        StructField("Visit_Frequency", StringType()),
                        StructField("Estimated_Visit_Duration", StringType()),
                        StructField("Date", StringType())
                        ])
                
        dataframe = session.read.schema(df_schema) \\
        .option("skip_header",1) \\
        .option("field_delimiter", ",") \\
        .option("field_optionally_enclosed_by", "\\"") \\
        .option("encoding","UTF-8")\\
        .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
        
       
            
        
        dataframe=dataframe.na.drop("all")
        if dataframe.count()==0:
            return "No Data in file"    

       
        
        dataframe = dataframe.with_column("FILE_NAME",lit(file_name))
        dataframe = dataframe.with_column("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        dataframe = dataframe.with_column("CRTD_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        
        # Load Data to the target table
        dataframe.write.mode("append").saveAsTable(target_table)
       
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        #move to success
        file_name=file_name.split(".")[0]+"_"+datetime.now().strftime("%Y%m%d%H%M%S")
        dataframe.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)
        
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




CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.NTASDL_RAW.POP6_KR_TRANS_EXECUTED_VISITS_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them.

import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import lit
from snowflake.snowpark.types import  StringType, StructType, StructField
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz
from datetime import datetime, timedelta                



def main(session: snowpark.Session, Param): 
       
    try :
            
        # Param=[''20240612_executed_visits.csv'',''NTASDL_RAW.DEV_LOAD_STAGE_ADLS_KOR'',''dev/pop6/visitdata/'',''sdl_pop6_kr_executed_visits'']  

        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        # Define the schema for the DataFrame
        
        df_schema=StructType([
                        StructField("Visit_ID", StringType()),
                        StructField("Visit_Date", StringType()),
                        StructField("Check-In_DateTime", StringType()),
                        StructField("Check-Out_DateTime", StringType()),
                        StructField("POPDB_ID", StringType()),
                        StructField("POP_Code", StringType()),
                        StructField("POP_Name", StringType()),
                        StructField("Address", StringType()),
                        StructField("Check-In_Longitude", StringType()),
                        StructField("Check-In_Latitude", StringType()),
                        StructField("Check-Out_Longitude", StringType()),
                        StructField("Check-Out_Latitude", StringType()),
                        StructField("Check-In_Photo", StringType()),
                        StructField("Check-Out_Photo", StringType()),
                        StructField("Username", StringType()),
                        StructField("User_Full_Name", StringType()),
                        StructField("Superior_Username", StringType()),
                        StructField("Superior_Name", StringType()),
                        StructField("Planned_Visit", StringType()),
                        StructField("Cancelled_Visit", StringType()),
                        StructField("Cancellation_Reason", StringType()),
                        StructField("Cancellation_Note", StringType())
                        ])
                
        dataframe = session.read.schema(df_schema) \\
        .option("skip_header",1) \\
        .option("field_delimiter", ",") \\
        .option("field_optionally_enclosed_by", "\\"") \\
        .option("encoding","UTF-8")\\
        .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
        
       
            
        
        dataframe=dataframe.na.drop("all")
        if dataframe.count()==0:
            return "No Data in file"    

       
        
        dataframe = dataframe.with_column("FILE_NAME",lit(file_name))
        dataframe = dataframe.with_column("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        dataframe = dataframe.with_column("CRTD_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
       
        
        # Load Data to the target table
        dataframe.write.mode("append").saveAsTable(target_table)
       
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        #move to success
        file_name=file_name.split(".")[0]+"_"+datetime.now().strftime("%Y%m%d%H%M%S")
        dataframe.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)

        
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

=


CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.NTASDL_RAW.POP6_KR_TRANS_GENERAL_AUDITS_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them.

import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import lit
from snowflake.snowpark.types import  StringType, StructType, StructField
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz
from datetime import datetime, timedelta                



def main(session: snowpark.Session, Param):
       
    try :
            
        # Param=[''20240612_general_audits.csv'',''NTASDL_RAW.DEV_LOAD_STAGE_ADLS_KOR'',''dev/pop6/auditdata/'',''sdl_pop6_kr_general_audits'']  

        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        # Define the schema for the DataFrame
        
        
        df_schema=StructType([
                        StructField("Visit_ID", StringType()),
                        StructField("Audit_Form_ID", StringType()),
                        StructField("Audit_Form", StringType()),
                        StructField("Section_ID", StringType()),
                        StructField("Section", StringType()),
                        StructField("Subsection_ID", StringType()),
                        StructField("Subsection", StringType()),
                        StructField("Field_ID", StringType()),
                        StructField("Field_Code", StringType()),
                        StructField("Field_Label", StringType()),
                        StructField("Field_Type", StringType()),
                        StructField("Dependent_On_Field_ID", StringType()),
                        StructField("Response", StringType())
                        ])
                
        dataframe = session.read.schema(df_schema) \\
        .option("skip_header",1) \\
        .option("field_delimiter", ",") \\
        .option("field_optionally_enclosed_by", "\\"") \\
        .option("encoding","UTF-8")\\
        .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
        
       
            
        
        dataframe=dataframe.na.drop("all")
        if dataframe.count()==0:
            return "No Data in file"    

       
        
        dataframe = dataframe.with_column("FILE_NAME",lit(file_name))
        dataframe = dataframe.with_column("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        dataframe = dataframe.with_column("CRTD_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
       
        
        # Load Data to the target table
        dataframe.write.mode("append").saveAsTable(target_table)
       
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        #move to success
        file_name=file_name.split(".")[0]+"_"+datetime.now().strftime("%Y%m%d%H%M%S")
        dataframe.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)

        
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






CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.NTASDL_RAW.POP6_HK_TRANS_GENERAL_AUDITS_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them.

import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import lit
from snowflake.snowpark.types import  StringType, StructType, StructField,IntegerType
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz
from datetime import datetime, timedelta



def main(session: snowpark.Session,Param): 
       
    try :
           
        #Param=[''20240611_general_audits.csv'',''NTASDL_RAW.DEV_LOAD_STAGE_ADLS_HKG'',''dev/pop6/auditdata/'',''sdl_pop6_hk_general_audits'']

        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        # Define the schema for the DataFrame
        
        df_schema=StructType([
                        StructField("visit_id", StringType()),
                        StructField("audit_form_id", StringType()),
                        StructField("audit_form", StringType()),
                        StructField("section_id", StringType()),
                        StructField("section", StringType()),
                        StructField("subsection_id", StringType()),
                        StructField("subsection", StringType()),
                        StructField("field_id", StringType()),
                        StructField("field_code", StringType()),
                        StructField("field_label", StringType()),
                        StructField("field_type", StringType()),
                        StructField("dependent_on_field_id", StringType()),
                        StructField("response", StringType())
            
                        
                        ])
                
        dataframe = session.read.schema(df_schema) \\
        .option("skip_header",1) \\
        .option("field_delimiter", ",") \\
        .option("field_optionally_enclosed_by", "\\"") \\
        .option("encoding","UTF-8")\\
        .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
        
       
            
        
        dataframe=dataframe.na.drop("all")
        if dataframe.count()==0:
            return "No Data in file"

       
        
        
     
        dataframe = dataframe.with_column("FILE_NAME",lit( file_name ))
        dataframe = dataframe.with_column("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        dataframe = dataframe.with_column("CRTD_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
       
        
        # Load Data to the target table
        dataframe.write.mode("append").saveAsTable(target_table)
       
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        #move to success
        file_name=file_name.split(".")[0]+''_''+datetime.now().strftime("%Y%m%d%H%M%S")
        dataframe.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)

        
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





CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.NTASDL_RAW.POP6_HK_TRANS_PRODUCT_ATTRIBUTE_AUDITS_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them.

import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import lit
from snowflake.snowpark.types import  StringType, StructType, StructField,IntegerType
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz
from datetime import datetime, timedelta



def main(session: snowpark.Session,Param): 
       
    try :
           
        #Param=[''20240611_product_attribute_audits.csv'',''NTASDL_RAW.DEV_LOAD_STAGE_ADLS_HKG'',''dev/pop6/auditdata/'',''sdl_pop6_hk_product_attribute_audits'']

        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        # Define the schema for the DataFrame
        
        df_schema=StructType([
                        StructField("visit_id", StringType()),
                        StructField("audit_form_id", StringType()),
                        StructField("audit_form", StringType()),
                        StructField("section_id", StringType()),
                        StructField("section", StringType()),
                        StructField("field_id", StringType()),
                        StructField("field_code", StringType()),
                        StructField("field_label", StringType()),
                        StructField("field_type", StringType()),
                        StructField("dependent_on_field_id", StringType()),
                        StructField("product_attribute_id", StringType()),
                        StructField("product_attribute", StringType()),
                        StructField("product_attribute_value_id", StringType()),
                        StructField("product_attribute_value", StringType()),
                        StructField("response", StringType())
            
                        
                        ])
                
        dataframe = session.read.schema(df_schema) \\
        .option("skip_header",1) \\
        .option("field_delimiter", ",") \\
        .option("field_optionally_enclosed_by", "\\"") \\
        .option("encoding","UTF-8")\\
        .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
        
       
            
        
        dataframe=dataframe.na.drop("all")
        if dataframe.count()==0:
            return "No Data in file"

       
        
        
     
        dataframe = dataframe.with_column("FILE_NAME",lit( file_name ))
        dataframe = dataframe.with_column("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        dataframe = dataframe.with_column("CRTD_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
       
        
        # Load Data to the target table
        dataframe.write.mode("append").saveAsTable(target_table)
       
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        #move to success
        file_name=file_name.split(".")[0]+''_''+datetime.now().strftime("%Y%m%d%H%M%S")
        dataframe.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)

        
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


=



CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.NTASDL_RAW.POP6_HK_TRANS_SKU_AUDITS_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them.

import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import lit
from snowflake.snowpark.types import  StringType, StructType, StructField,IntegerType
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz
from datetime import datetime, timedelta



def main(session: snowpark.Session,Param): 
       
    try :
           
        #Param=[''20240611_sku_audits.csv'',''NTASDL_RAW.DEV_LOAD_STAGE_ADLS_HKG'',''dev/pop6/auditdata/'',''sdl_pop6_hk_sku_audits'']

        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        # Define the schema for the DataFrame
        
        df_schema=StructType([
                        StructField("visit_id", StringType()),
                        StructField("audit_form_id", StringType()),
                        StructField("audit_form", StringType()),
                        StructField("section_id", StringType()),
                        StructField("section", StringType()),
                        StructField("field_id", StringType()),
                        StructField("field_code", StringType()),
                        StructField("field_label", StringType()),
                        StructField("field_type", StringType()),
                        StructField("dependent_on_field_id", StringType()),
                        StructField("sku_id", StringType()),
                        StructField("sku", StringType()),
                        StructField("response", StringType())
            
                        
                        ])
                
        dataframe = session.read.schema(df_schema) \\
        .option("skip_header",1) \\
        .option("field_delimiter", ",") \\
        .option("field_optionally_enclosed_by", "\\"") \\
        .option("encoding","UTF-8")\\
        .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
        
       
            
        
        dataframe=dataframe.na.drop("all")
        if dataframe.count()==0:
            return "No Data in file"

       
        
        
     
        dataframe = dataframe.with_column("FILE_NAME",lit( file_name ))
        dataframe = dataframe.with_column("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        dataframe = dataframe.with_column("CRTD_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
       
        
        # Load Data to the target table
        dataframe.write.mode("append").saveAsTable(target_table)
       
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        #move to success
        file_name=file_name.split(".")[0]+''_''+datetime.now().strftime("%Y%m%d%H%M%S")
        dataframe.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)

        
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





CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.NTASDL_RAW.POP6_HK_MASTER_USERS_PREPROCESSING("PARAM" ARRAY)
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
    #Param=["20240610_users.csv","NTASDL_RAW.DEV_LOAD_STAGE_ADLS_HKG","dev/pop6/masterdata/users/","sdl_pop6_hk_users"]
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        
        df_schema = StructType([
                StructField("Status", StringType(), True),
                StructField("USERDB_ID", StringType(), True),
                StructField("Username", StringType(), True),
                StructField("FIRST_NAME", StringType(), True),
                StructField("Last_Name", StringType(), True),
                StructField("Team", StringType(), True),
                StructField("Superior_Name", StringType(), True),
                StructField("Authorisation_Group", StringType(), True),
                StructField("Email_Address", StringType(), True),
                StructField("Longitude", StringType(), True),
                StructField("Latitude", StringType(), True),
                StructField("Business_Units_ID", StringType(), True),
                StructField("Business_Unit_Name", StringType(), True)
                
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
            
            

        df = df.with_column(''hashkey'',md5(concat(when(is_null(col(''status'')),lit('''')).otherwise(col(''status'')),\\
                                            when(is_null(col(''USERDB_ID'')),lit('''')).otherwise(col(''USERDB_ID'')),\\
                                          when(is_null(col(''Username'')),lit('''')).otherwise(col(''Username'')),\\
                                          when(is_null(col(''FIRST_NAME'')),lit('''')).otherwise(col(''FIRST_NAME'')),\\
                                         when(is_null(col(''Last_Name'')),lit('''')).otherwise(col(''Last_Name'')),\\
                                         when(is_null(col(''Team'')),lit('''')).otherwise(col(''Team'')),\\
                                         when(is_null(col(''Superior_Name'')),lit('''')).otherwise(col(''Superior_Name'')),\\
                                          when(is_null(col(''Authorisation_Group'')),lit('''')).otherwise(col(''Authorisation_Group'')),\\
                                         when(is_null(col(''Email_Address'')),lit('''')).otherwise(col(''Email_Address'')),\\
                                         when(is_null(col(''Longitude'')),lit('''')).otherwise(col(''Longitude'')),\\
                                         when(is_null(col(''Latitude'')),lit('''')).otherwise(col(''Latitude'')),\\
                                         when(is_null(col(''Business_Units_ID'')),lit('''')).otherwise(col(''Business_Units_ID'')),
                                         when(is_null(col(''Business_Unit_Name'')),lit('''')).otherwise(col(''Business_Unit_Name'')))))

        
        df = df.with_column(''run_id'',lit(generunid()))

        df = df.with_column(''crtd_dttm'',lit(generatecrtd_dttm()))
        

        

        
        snowdf= df.select(
                    "status",
                    "userdb_id",
                    "username",
                    "first_name",
                    "last_name",
                    "team",
                    "superior_name",
                    "authorisation_group",
                    "email_address",
                    "longitude",
                    "latitude",
                    "file_name",
                    "run_id",
                    "crtd_dttm",
                    "hashkey",
                    "business_units_id",
                    "business_unit_name"
            
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




CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.NTASDL_RAW.POP6_HK_MASTER_POPS_PREPROCESSING("PARAM" ARRAY)
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
    #Param=["20240609_pops.csv","NTASDL_RAW.DEV_LOAD_STAGE_ADLS_HKG","dev/pop6/masterdata/pops/","sdl_pop6_hk_pops"]
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        
        df_schema = StructType([
                StructField("Status", StringType(), True),
                StructField("popdb_id", StringType(), True),
                StructField("pop_code", StringType(), True),
                StructField("pop_name", StringType(), True),
                StructField("address", StringType(), True),
                StructField("longitude", StringType(), True),
                StructField("latitude", StringType(), True),
                StructField("Business_Units_ID", StringType(), True),
                StructField("Business_Unit_Name", StringType(), True),
                StructField("country", StringType(), True),
                StructField("channel", StringType(), True),
                StructField("retail_environment_ps", StringType(), True),
                StructField("customer", StringType(), True),
                StructField("sales_group_code", StringType(), True),
                StructField("sales_group_name", StringType(), True),
                StructField("Store_Type", StringType(), True),
                StructField("Customer_Grade", StringType(), True),
                StructField("External_Store_Code", StringType(), True),
                StructField("territory", StringType(), True),
               
                
                
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
            
            

        df = df.with_column(''hashkey'',md5(concat(when(is_null(col(''status'')),lit('''')).otherwise(col(''status'')),\\
                                            when(is_null(col(''popdb_id'')),lit('''')).otherwise(col(''popdb_id'')),\\
                                          when(is_null(col(''pop_code'')),lit('''')).otherwise(col(''pop_code'')),\\
                                          when(is_null(col(''pop_name'')),lit('''')).otherwise(col(''pop_name'')),\\
                                         when(is_null(col(''address'')),lit('''')).otherwise(col(''address'')),\\
                                         when(is_null(col(''longitude'')),lit('''')).otherwise(col(''longitude'')),\\
                                         when(is_null(col(''latitude'')),lit('''')).otherwise(col(''latitude'')),\\
                                          when(is_null(col(''country'')),lit('''')).otherwise(col(''country'')),\\
                                         when(is_null(col(''channel'')),lit('''')).otherwise(col(''channel'')),\\
                                         when(is_null(col(''retail_environment_ps'')),lit('''')).otherwise(col(''retail_environment_ps'')),\\
                                         when(is_null(col(''customer'')),lit('''')).otherwise(col(''customer'')),\\
                                         when(is_null(col(''sales_group_code'')),lit('''')).otherwise(col(''sales_group_code'')),\\
                                         when(is_null(col(''sales_group_name'')),lit('''')).otherwise(col(''sales_group_name'')),\\
                                        when(is_null(col(''customer_grade'')),lit('''')).otherwise(col(''customer_grade'')),\\
                                          when(is_null(col(''external_store_code'')),lit('''')).otherwise(col(''external_store_code'')),
                                                when(is_null(col(''business_units_id'')),lit('''')).otherwise(col(''business_units_id'')),\\
                                          when(is_null(col(''territory'')),lit('''')).otherwise(col(''territory'')))))
                                         
        
        df = df.with_column(''run_id'',lit(generunid()))

        df = df.with_column(''crtd_dttm'',lit(generatecrtd_dttm()))
        

        

        
        snowdf= df.select(
                    "Status",
                "popdb_id",
                "pop_code",
                "pop_name",
                "address",
                "longitude",
                "latitude",
                "country",
                "channel",
                "retail_environment_ps",
                "customer",
                "sales_group_code",
                "sales_group_name",
                "customer_grade",
                "external_store_code",
                "file_name",
                "run_id",
                "crtd_dttm",
                "hashkey",
                "business_units_id",
                "business_unit_name",
                "territory" 
                
            
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




CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.NTASDL_RAW.POP6_HK_MASTER_PRODUCTS_PREPROCESSING("PARAM" ARRAY)
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
    #Param=["20240609_products.csv","NTASDL_RAW.DEV_LOAD_STAGE_ADLS_HKG","dev/pop6/masterdata/products/","sdl_pop6_hk_products"]
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        
        df_schema = StructType([
                StructField("Status", StringType(), True),
                StructField("productdb_id", StringType(), True),
                StructField("barcode", StringType(), True),
                StructField("sku", StringType(), True),
                StructField("unit_price", StringType(), True),
                StructField("display_order", StringType(), True),
                StructField("launch_date", StringType(), True),
                StructField("largest_uom_quantity", StringType(), True),
                StructField("middle_uom_quantity", StringType(), True),
                StructField("smallest_uom_quantity", StringType(), True),
                StructField("sku_english", StringType(), True),
                StructField("sku_code", StringType(), True),
                StructField("ps_category", StringType(), True),
                StructField("ps_segment", StringType(), True),
                StructField("ps_category_segment", StringType(), True),
                StructField("company", StringType(), True),
                StructField("country_l1", StringType(), True),
                StructField("regional_franchise_l2", StringType(), True),
                StructField("franchise_l3", StringType(), True),
                StructField("brand_l4", StringType(), True),
                StructField("sub_category_l5", StringType(), True),
                StructField("platform_level_l6", StringType(), True),
                StructField("variance_l7", StringType(), True),
                StructField("pack_size_l8", StringType(), True)
                
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
            
            

        df = df.with_column(''hashkey'',md5(concat(when(is_null(col(''status'')),lit('''')).otherwise(col(''status'')),\\
                                            when(is_null(col(''productdb_id'')),lit('''')).otherwise(col(''productdb_id'')),\\
                                          when(is_null(col(''barcode'')),lit('''')).otherwise(col(''barcode'')),\\
                                          when(is_null(col(''sku'')),lit('''')).otherwise(col(''sku'')),\\
                                         when(is_null(col(''unit_price'')),lit('''')).otherwise(col(''unit_price'')),\\
                                         when(is_null(col(''display_order'')),lit('''')).otherwise(col(''display_order'')),\\
                                         when(is_null(col(''launch_date'')),lit('''')).otherwise(col(''launch_date'')),\\
                                          when(is_null(col(''largest_uom_quantity'')),lit('''')).otherwise(col(''largest_uom_quantity'')),\\
                                         when(is_null(col(''middle_uom_quantity'')),lit('''')).otherwise(col(''middle_uom_quantity'')),\\
                                         when(is_null(col(''smallest_uom_quantity'')),lit('''')).otherwise(col(''smallest_uom_quantity'')),\\
                                         when(is_null(col(''company'')),lit('''')).otherwise(col(''company'')),\\
                                         when(is_null(col(''sku_english'')),lit('''')).otherwise(col(''sku_english'')),\\
                                         when(is_null(col(''sku_code'')),lit('''')).otherwise(col(''sku_code'')),\\
                                        when(is_null(col(''ps_category'')),lit('''')).otherwise(col(''ps_category'')),\\
                                          when(is_null(col(''ps_segment'')),lit('''')).otherwise(col(''ps_segment'')),\\
                                         when(is_null(col(''ps_category_segment'')),lit('''')).otherwise(col(''ps_category_segment'')),\\
                                         when(is_null(col(''country_l1'')),lit('''')).otherwise(col(''country_l1'')),\\
                                         when(is_null(col(''regional_franchise_l2'')),lit('''')).otherwise(col(''regional_franchise_l2'')),\\
                                          when(is_null(col(''franchise_l3'')),lit('''')).otherwise(col(''franchise_l3'')),\\
                                         when(is_null(col(''brand_l4'')),lit('''')).otherwise(col(''brand_l4'')),\\
                                         when(is_null(col(''sub_category_l5'')),lit('''')).otherwise(col(''sub_category_l5'')),\\
                                         when(is_null(col(''platform_level_l6'')),lit('''')).otherwise(col(''platform_level_l6'')),\\
                                         when(is_null(col(''variance_l7'')),lit('''')).otherwise(col(''variance_l7'')),\\
                                         when(is_null(col(''pack_size_l8'')),lit('''')).otherwise(col(''pack_size_l8'')))))

        
        df = df.with_column(''run_id'',lit(generunid()))

        df = df.with_column(''crtd_dttm'',lit(generatecrtd_dttm()))
        

        

        
        snowdf= df.select(
                   "Status", 
                "productdb_id", 
                "barcode", 
                "sku", 
                "unit_price", 
                "display_order", 
                "launch_date", 
                "largest_uom_quantity", 
                "middle_uom_quantity", 
                "smallest_uom_quantity", 
                "company", 
                "sku_english", 
                "sku_code", 
                "ps_category", 
                "ps_segment", 
                "ps_category_segment", 
                "country_l1", 
                "regional_franchise_l2", 
                "franchise_l3", 
                "brand_l4", 
                "sub_category_l5", 
                "platform_level_l6", 
                "variance_l7", 
                "pack_size_l8",
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



CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.NTASDL_RAW.POP6_KR_TRANS_DISPLAY_PLANS_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them.

import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import lit
from snowflake.snowpark.types import  StringType, StructType, StructField
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz
from datetime import datetime, timedelta                



def main(session: snowpark.Session, Param): 
       
    try :
            
        # Param=[''20240612_display_plans.csv'',''NTASDL_RAW.DEV_LOAD_STAGE_ADLS_KOR'',''dev/pop6/displaydata/'',''sdl_pop6_kr_display_plans'']  

        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        # Define the schema for the DataFrame

        df_schema=StructType([
                        StructField("Display_Plan_ID", StringType()),
                        StructField("Status", StringType()),
                        StructField("Allocation_Method", StringType()),
                        StructField("POP_Code_or_POP_List_Code", StringType()),
                        StructField("Team", StringType()),
                        StructField("Display_Code", StringType()),
                        StructField("Display_Name", StringType()),
                        StructField("Required_Number_of_Displays", StringType()),
                        StructField("Start_Date", StringType()),
                        StructField("End_Date", StringType()),
                        StructField("Product_Attribute_ID", StringType()),
                        StructField("Product_Attribute", StringType()),
                        StructField("Product_Attribute_Value_ID", StringType()),
                        StructField("Product_Attribute_Value", StringType()),
                        StructField("Comments", StringType())
                        ])
                
        dataframe = session.read.schema(df_schema) \\
        .option("skip_header",1) \\
        .option("field_delimiter", ",") \\
        .option("field_optionally_enclosed_by", "\\"") \\
        .option("encoding","UTF-8")\\
        .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
        
       
            
        
        dataframe=dataframe.na.drop("all")
        if dataframe.count()==0:
            return "No Data in file"    

       
        
        dataframe = dataframe.with_column("FILE_NAME",lit(file_name))
        dataframe = dataframe.with_column("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        dataframe = dataframe.with_column("CRTD_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))

        
        # Load Data to the target table
        dataframe.write.mode("append").saveAsTable(target_table)
       
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        #move to success
        file_name=file_name.split(".")[0]+"_"+datetime.now().strftime("%Y%m%d%H%M%S")
        dataframe.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)

        
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



CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.NTASDL_RAW.POP6_HK_TRANS_DISPLAYS_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them.

import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import lit
from snowflake.snowpark.types import  StringType, StructType, StructField,IntegerType
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz
from datetime import datetime, timedelta



def main(session: snowpark.Session,Param): 
       
    try :
           
        #Param=[''20240613_displays.csv'',''NTASDL_RAW.DEV_LOAD_STAGE_ADLS_HKG'',''dev/pop6/auditdata/'',''sdl_pop6_hk_displays'']

        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        # Define the schema for the DataFrame
        
        df_schema=StructType([
                        StructField("visit_id", StringType()),
                        StructField("display_plan_id", StringType()),
                        StructField("display_type", StringType()),
                        StructField("display_code", StringType()),
                        StructField("display_name", StringType()),
                        StructField("start_date", StringType()),
                        StructField("end_date", StringType()),
                        StructField("checklist_method", StringType()),
                        StructField("display_number", IntegerType()),
                        StructField("product_attribute_id", StringType()),
                        StructField("product_attribute", StringType()),
                        StructField("product_attribute_value_id", StringType()),
                        StructField("product_attribute_value", StringType()),
                        StructField("comments", StringType()),
                        StructField("field_id", StringType()),
                        StructField("field_code", StringType()),
                        StructField("field_label", StringType()),
                        StructField("field_type", StringType()),
                        StructField("dependent_on_field_id", StringType()),
                        StructField("response", StringType())
            
                        
                        ])
                
        dataframe = session.read.schema(df_schema) \\
        .option("skip_header",1) \\
        .option("field_delimiter", ",") \\
        .option("field_optionally_enclosed_by", "\\"") \\
        .option("encoding","UTF-8")\\
        .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
        
       
            
        
        dataframe=dataframe.na.drop("all")
        if dataframe.count()==0:
            return "No Data in file"

       
        
        
     
        dataframe = dataframe.with_column("FILE_NAME",lit( file_name ))
        dataframe = dataframe.with_column("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        dataframe = dataframe.with_column("CRTD_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
       
        
        # Load Data to the target table
        dataframe.write.mode("append").saveAsTable(target_table)
       
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        #move to success
        file_name=file_name.split(".")[0]+''_''+datetime.now().strftime("%Y%m%d%H%M%S")
        dataframe.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)

        
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

=

CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.NTASDL_RAW.POP6_KR_TRANS_DISPLAYS_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them.

import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import lit
from snowflake.snowpark.types import  StringType, StructType, StructField
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz
from datetime import datetime, timedelta                



def main(session: snowpark.Session, Param): 
       
    try :
            
        # Param=[''20240612_displays.csv'',''NTASDL_RAW.DEV_LOAD_STAGE_ADLS_KOR'',''dev/pop6/displaydata/'',''sdl_pop6_kr_displays'']  

        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        # Define the schema for the DataFrame

    
        df_schema=StructType([
                        StructField("Visit_ID", StringType()),
                        StructField("Display_Plan_ID", StringType()),
                        StructField("Display_Type", StringType()),
                        StructField("Display_Code", StringType()),
                        StructField("Display_Name", StringType()),
                        StructField("Start_Date", StringType()),
                        StructField("End_Date", StringType()),
                        StructField("Checklist_Method", StringType()),
                        StructField("Display_Number", StringType()),
                        StructField("Product_Attribute_ID", StringType()),
                        StructField("Product_Attribute", StringType()),
                        StructField("Product_Attribute_Value_ID", StringType()),
                        StructField("Product_Attribute_Value", StringType()),
                        StructField("Comments", StringType()),
                        StructField("Field_ID", StringType()),
                        StructField("Field_Code", StringType()),
                        StructField("Field_Label", StringType()),
                        StructField("Field_Type", StringType()),
                        StructField("Dependent_On_Field_ID", StringType()),
                        StructField("Response", StringType())
                        ])
                
        dataframe = session.read.schema(df_schema) \\
        .option("skip_header",1) \\
        .option("field_delimiter", ",") \\
        .option("field_optionally_enclosed_by", "\\"") \\
        .option("encoding","UTF-8")\\
        .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
        
       
            
        
        dataframe=dataframe.na.drop("all")
        if dataframe.count()==0:
            return "No Data in file"    

       
        
        dataframe = dataframe.with_column("FILE_NAME",lit(file_name))
        dataframe = dataframe.with_column("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        dataframe = dataframe.with_column("CRTD_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))

        
        # Load Data to the target table
        dataframe.write.mode("append").saveAsTable(target_table)
       
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        #move to success
        file_name=file_name.split(".")[0]+"_"+datetime.now().strftime("%Y%m%d%H%M%S")
        dataframe.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)

        
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



CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.NTASDL_RAW.POP6_KR_TRANS_DATA_EXCLUSION_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them.

import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import lit
from snowflake.snowpark.types import  StringType, StructType, StructField
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz
from datetime import datetime, timedelta                



def main(session: snowpark.Session, Param): 
       
    try :
            
        # Param=[''20221103_pop6_kr_data_exclusion.csv'',''NTASDL_RAW.DEV_LOAD_STAGE_ADLS_KOR'',''dev/pop6/exclusiondata/'',''sdl_pop6_kr_exclusion'']  

        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        # Define the schema for the DataFrame
    
        df_schema=StructType([
                        StructField("exclude_kpi", StringType()),
                        StructField("visit_date", StringType()),
                        StructField("pop_code", StringType()),
                        StructField("country", StringType()),
                        StructField("merchandiser_userid", StringType()),
                        StructField("audit_form_name", StringType()),
                        StructField("section_name", StringType()),
                        StructField("operation_type", StringType())
                        ])
                
        dataframe = session.read.schema(df_schema) \\
        .option("skip_header",1) \\
        .option("field_delimiter", ",") \\
        .option("field_optionally_enclosed_by", "\\"") \\
        .option("encoding","UTF-8")\\
        .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
        
       
            
        
        dataframe=dataframe.na.drop("all")
        if dataframe.count()==0:
            return "No Data in file"    

       
        
        dataframe = dataframe.with_column("FILE_NAME",lit(file_name))
        dataframe = dataframe.with_column("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        dataframe = dataframe.with_column("CRTD_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))

        
        # Load Data to the target table
        dataframe.write.mode("append").saveAsTable(target_table)
       
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        #move to success
        file_name=file_name.split(".")[0]+"_"+datetime.now().strftime("%Y%m%d%H%M%S")
        dataframe.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)

        
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

=

CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.NTASDL_RAW.POP6_HK_TRANS_DISPLAYS_PLAN_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them.

import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import lit
from snowflake.snowpark.types import  StringType, StructType, StructField,IntegerType
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz
from datetime import datetime, timedelta



def main(session: snowpark.Session,Param): 
       
    try :
           
        # Param=[''20240613_display_plans.csv'',''NTASDL_RAW.DEV_LOAD_STAGE_ADLS_HKG'',''dev/pop6/displaydata/displayplan/'',''sdl_pop6_hk_display_plans'']

        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        # Define the schema for the DataFrame
        
        df_schema=StructType([
                        StructField("display_plan_id", StringType()),
                        StructField("status", StringType()),
                        StructField("allocation_method", StringType()),
                        StructField("pop_code_or_pop_list_code", StringType()),
                        StructField("team", StringType()),
                        StructField("display_code", StringType()),
                        StructField("display_name", StringType()),
                        StructField("required_number_of_displays", IntegerType()),
                        StructField("start_date", StringType()),
                        StructField("end_date", StringType()),
                        StructField("product_attribute_id", StringType()),
                        StructField("product_attribute", StringType()),
                        StructField("product_attribute_value_id", StringType()),
                        StructField("product_attribute_value", StringType()),
                        StructField("comments", StringType())
            
                        
                        ])
                
        dataframe = session.read.schema(df_schema) \\
        .option("skip_header",1) \\
        .option("field_delimiter", ",") \\
        .option("field_optionally_enclosed_by", "\\"") \\
        .option("encoding","UTF-8")\\
        .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
        
       
            
        
        dataframe=dataframe.na.drop("all")
        if dataframe.count()==0:
            return "No Data in file"

       
        
        
     
        dataframe = dataframe.with_column("FILE_NAME",lit( file_name ))
        dataframe = dataframe.with_column("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        dataframe = dataframe.with_column("CRTD_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
       
        
        # Load Data to the target table
        dataframe.write.mode("append").saveAsTable(target_table)
       
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        #move to success
        file_name=file_name.split(".")[0]+''_''+datetime.now().strftime("%Y%m%d%H%M%S")
        dataframe.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)

        
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



CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.NTASDL_RAW.POP6_HK_TRANS_DISPLAYS_PLAN_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them.

import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import lit
from snowflake.snowpark.types import  StringType, StructType, StructField,IntegerType
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz
from datetime import datetime, timedelta



def main(session: snowpark.Session,Param): 
       
    try :
           
        # Param=[''20240613_display_plans.csv'',''NTASDL_RAW.DEV_LOAD_STAGE_ADLS_HKG'',''dev/pop6/displaydata/displayplan/'',''sdl_pop6_hk_display_plans'']

        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        # Define the schema for the DataFrame
        
        df_schema=StructType([
                        StructField("display_plan_id", StringType()),
                        StructField("status", StringType()),
                        StructField("allocation_method", StringType()),
                        StructField("pop_code_or_pop_list_code", StringType()),
                        StructField("team", StringType()),
                        StructField("display_code", StringType()),
                        StructField("display_name", StringType()),
                        StructField("required_number_of_displays", IntegerType()),
                        StructField("start_date", StringType()),
                        StructField("end_date", StringType()),
                        StructField("product_attribute_id", StringType()),
                        StructField("product_attribute", StringType()),
                        StructField("product_attribute_value_id", StringType()),
                        StructField("product_attribute_value", StringType()),
                        StructField("comments", StringType())
            
                        
                        ])
                
        dataframe = session.read.schema(df_schema) \\
        .option("skip_header",1) \\
        .option("field_delimiter", ",") \\
        .option("field_optionally_enclosed_by", "\\"") \\
        .option("encoding","UTF-8")\\
        .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
        
       
            
        
        dataframe=dataframe.na.drop("all")
        if dataframe.count()==0:
            return "No Data in file"

       
        
        
     
        dataframe = dataframe.with_column("FILE_NAME",lit( file_name ))
        dataframe = dataframe.with_column("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        dataframe = dataframe.with_column("CRTD_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
       
        
        # Load Data to the target table
        dataframe.write.mode("append").saveAsTable(target_table)
       
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        #move to success
        file_name=file_name.split(".")[0]+''_''+datetime.now().strftime("%Y%m%d%H%M%S")
        dataframe.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)

        
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

==

CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.NTASDL_RAW.POP6_KR_TRANS_PROMOTION_PLANS_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them.

import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import lit
from snowflake.snowpark.types import  StringType, StructType, StructField
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz
from datetime import datetime, timedelta                



def main(session: snowpark.Session, Param): 
       
    try :
            
        # Param=[''20240613_promotion_plans.csv'',''NTASDL_RAW.DEV_LOAD_STAGE_ADLS_KOR'',''dev/pop6/promotiondata/'',''sdl_pop6_kr_promotion_plans''] 

        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        # Define the schema for the DataFrame
    
        df_schema=StructType([
                        StructField("Status", StringType()),
                        StructField("Promotion_Plan_ID", StringType()),
                        StructField("Allocation_Method", StringType()),
                        StructField("POP_Code_or_POP_List_Code", StringType()),
                        StructField("Team", StringType()),
                        StructField("Promotion_Code", StringType()),
                        StructField("Promotion_Name", StringType()),
                        StructField("Promotion_Mechanics", StringType()),
                        StructField("Start_Date", StringType()),
                        StructField("End_Date", StringType()),
                        StructField("Product_Attribute_ID", StringType()),
                        StructField("Product_Attribute", StringType()),
                        StructField("Product_Attribute_Value_ID", StringType()),
                        StructField("Product_Attribute_Value", StringType()),
                        StructField("Promotion_Price", StringType()),
                        StructField("Price_Field", StringType()),
                        StructField("Photo_Field", StringType()),
                        StructField("Reason_Field", StringType())
                        ])
                
        dataframe = session.read.schema(df_schema) \\
        .option("skip_header",1) \\
        .option("field_delimiter", ",") \\
        .option("field_optionally_enclosed_by", "\\"") \\
        .option("encoding","UTF-8")\\
        .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
        
       
            
        
        dataframe=dataframe.na.drop("all")
        if dataframe.count()==0:
            return "No Data in file"    

       
        
        dataframe = dataframe.with_column("FILE_NAME",lit(file_name))
        dataframe = dataframe.with_column("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        dataframe = dataframe.with_column("CRTD_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))

        
        # Load Data to the target table
        dataframe.write.mode("append").saveAsTable(target_table)
       
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        #move to success
        file_name=file_name.split(".")[0]+"_"+datetime.now().strftime("%Y%m%d%H%M%S")
        dataframe.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)

        
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

=


CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.NTASDL_RAW.POP6_KR_TRANS_PROMOTIONS_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them.

import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import lit
from snowflake.snowpark.types import  StringType, StructType, StructField
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz
from datetime import datetime, timedelta                



def main(session: snowpark.Session, Param):
       
    try :
            
        # Param=[''20240613_promotions.csv'',''NTASDL_RAW.DEV_LOAD_STAGE_ADLS_KOR'',''dev/pop6/promotiondata/'',''sdl_pop6_kr_promotions''] 

        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        # Define the schema for the DataFrame
    
        df_schema=StructType([
                        StructField("Visit_ID", StringType()),
                        StructField("Promotion_Plan_ID", StringType()),
                        StructField("Promotion_Code", StringType()),
                        StructField("Promotion_Name", StringType()),
                        StructField("Promotion_Mechanics", StringType()),
                        StructField("Promotion_Type", StringType()),
                        StructField("Start_Date", StringType()),
                        StructField("End_Date", StringType()),
                        StructField("Product_Attribute_ID", StringType()),
                        StructField("Product_Attribute", StringType()),
                        StructField("Product_Attribute_Value_ID", StringType()),
                        StructField("Product_Attribute_Value", StringType()),
                        StructField("Promotion_Price", StringType()),
                        StructField("Promotion_Compliance", StringType()),
                        StructField("Actual_Price", StringType()),
                        StructField("Non-Compliance_Reason", StringType()),
                        StructField("Photo", StringType())
                        ])
                
        dataframe = session.read.schema(df_schema) \\
        .option("skip_header",1) \\
        .option("field_delimiter", ",") \\
        .option("field_optionally_enclosed_by", "\\"") \\
        .option("encoding","UTF-8")\\
        .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
        
       
            
        
        dataframe=dataframe.na.drop("all")
        if dataframe.count()==0:
            return "No Data in file"    

       
        
        dataframe = dataframe.with_column("FILE_NAME",lit(file_name))
        dataframe = dataframe.with_column("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        dataframe = dataframe.with_column("CRTD_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))

        
        # Load Data to the target table
        dataframe.write.mode("append").saveAsTable(target_table)
       
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        #move to success
        file_name=file_name.split(".")[0]+"_"+datetime.now().strftime("%Y%m%d%H%M%S")
        dataframe.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)

        
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


CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.NTASDL_RAW.POP6_KR_TRANS_TASKS_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them.

import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import lit
from snowflake.snowpark.types import  StringType, StructType, StructField
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz
from datetime import datetime, timedelta                

def main(session: snowpark.Session, Param): 
       
    try :
            
        # Param=[''20240613_tasks.csv'',''NTASDL_RAW.DEV_LOAD_STAGE_ADLS_KOR'',''dev/pop6/taskdata/'',''sdl_pop6_kr_tasks''] 

        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        # Define the schema for the DataFrame
        df_schema=StructType([
                        StructField("Visit_ID", StringType()),
                        StructField("Task_Group", StringType()),
                        StructField("Task_ID", StringType()),
                        StructField("Task_Name", StringType()),
                        StructField("Field_ID", StringType()),
                        StructField("Field_Code", StringType()),
                        StructField("Field_Label", StringType()),
                        StructField("Field_Type", StringType()),
                        StructField("Dependent_On_Field_ID", StringType()),
                        StructField("Response", StringType())
                        ])
                
        dataframe = session.read.schema(df_schema) \\
        .option("skip_header",1) \\
        .option("field_delimiter", ",") \\
        .option("field_optionally_enclosed_by", "\\"") \\
        .option("encoding","UTF-8")\\
        .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
        
       
            
        
        dataframe=dataframe.na.drop("all")
        if dataframe.count()==0:
            return "No Data in file"    

       
        
        dataframe = dataframe.with_column("FILE_NAME",lit(file_name))
        dataframe = dataframe.with_column("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        dataframe = dataframe.with_column("CRTD_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))

        
        # Load Data to the target table
        dataframe.write.mode("append").saveAsTable(target_table)
       
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        #move to success
        file_name=file_name.split(".")[0]+"_"+datetime.now().strftime("%Y%m%d%H%M%S")
        dataframe.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)

        
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

==

CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.NTASDL_RAW.POP6_KR_MASTER_USERS_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them

import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import lit, md5, concat, when, is_null, col
from snowflake.snowpark.types import  StringType, StructType, StructField
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz
from datetime import datetime, timedelta                


def main(session: snowpark.Session, Param): 
       
    try :
            
        # Param=[''20240616_users.csv'',''NTASDL_RAW.DEV_LOAD_STAGE_ADLS_KOR'',''dev/pop6/masterdata/'',''sdl_pop6_kr_users''] 

        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        # Define the schema for the DataFrame

        
        
        df_schema=StructType([
                        StructField("Status", StringType()),
                        StructField("UserDB_ID", StringType()),
                        StructField("Username", StringType()),
                        StructField("First_Name", StringType()),
                        StructField("Last_Name", StringType()),
                        StructField("Team", StringType()),
                        StructField("Superior_Name", StringType()),
                        StructField("Authorisation_Group", StringType()),
                        StructField("Email_Address", StringType()),
                        StructField("Longitude", StringType()),
                        StructField("Latitude", StringType()),
                        StructField("Business_Units_ID", StringType()),
                        StructField("Business_Unit_Name", StringType())
                        ])
                
        dataframe = session.read.schema(df_schema) \\
        .option("skip_header",1) \\
        .option("field_delimiter", ",") \\
        .option("field_optionally_enclosed_by", "\\"") \\
        .option("encoding","UTF-8")\\
        .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
        
       
            
        
        dataframe=dataframe.na.drop("all")
        if dataframe.count()==0:
            return "No Data in file"    

       
        
        dataframe = dataframe.with_column("FILE_NAME",lit(file_name))
        dataframe = dataframe.with_column("HASHKEY",md5(concat(when(is_null(col("Status")),lit("")).otherwise(col("Status")),
                                            when(is_null(col("UserDB_ID")),lit("")).otherwise(col("UserDB_ID")),
                                            when(is_null(col("Username")),lit("")).otherwise(col("Username")),
                                            when(is_null(col("First_Name")),lit("")).otherwise(col("First_Name")),
                                            when(is_null(col("Last_Name")),lit("")).otherwise(col("Last_Name")),
                                            when(is_null(col("Team")),lit("")).otherwise(col("Team")),
                                            when(is_null(col("Superior_Name")),lit("")).otherwise(col("Superior_Name")),
                                            when(is_null(col("Authorisation_Group")),lit("")).otherwise(col("Authorisation_Group")),
                                            when(is_null(col("Email_Address")),lit("")).otherwise(col("Email_Address")),
                                            when(is_null(col("Longitude")),lit("")).otherwise(col("Longitude")),
                                            when(is_null(col("Latitude")),lit("")).otherwise(col("Latitude")),
                                            when(is_null(col("Business_Units_ID")),lit("")).otherwise(col("Business_Units_ID")),
                                            when(is_null(col("Business_Unit_Name")),lit("")).otherwise(col("Business_Unit_Name")))))
        dataframe = dataframe.with_column("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        dataframe = dataframe.with_column("CRTD_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
                
        snowdf= dataframe.select(
                    "Status",
                    "UserDB_ID",
                    "Username",
                    "First_Name",
                    "Last_Name",
                    "Team",
                    "Superior_Name",
                    "Authorisation_Group",
                    "Email_Address",
                    "Longitude",
                    "Latitude",
                    "FILE_NAME",
                    "RUN_ID",
                    "CRTD_DTTM",
                    "HASHKEY",
                    "Business_Units_Id",
                    "Business_Unit_Name"
                    )
        
        # Load Data to the target table
        snowdf.write.mode("append").saveAsTable(target_table)
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        #move to success
        file_name=file_name.split(".")[0]+"_"+datetime.now().strftime("%Y%m%d%H%M%S")
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



CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.NTASDL_RAW.POP6_HK_TRANS_DATA_EXCLUSION_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them.

import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import lit
from snowflake.snowpark.types import  StringType, StructType, StructField,IntegerType
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz
from datetime import datetime, timedelta



def main(session: snowpark.Session,Param): 
       
    try :
           
        #Param=[''20220112_pop6_hk_data_exclusion.csv'',''NTASDL_RAW.DEV_LOAD_STAGE_ADLS_HKG'',''dev/pop6/exclusionsdata/'',''sdl_pop6_hk_exclusion'']

        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        # Define the schema for the DataFrame
        
        df_schema=StructType([
                        StructField("exclude_kpi", StringType()),
                        StructField("visit_date", StringType()),
                        StructField("pop_code", StringType()),
                        StructField("country", StringType()),
                        StructField("merchandiser_userid", StringType()),
                        StructField("audit_form_name", StringType()),
                        StructField("section_name", StringType()),
                        StructField("operation_type", StringType())
            
                        
                        ])
                
        dataframe = session.read.schema(df_schema) \\
        .option("skip_header",1) \\
        .option("field_delimiter", ",") \\
        .option("field_optionally_enclosed_by", "\\"") \\
        .option("encoding","UTF-8")\\
        .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
        
       
            
        
        dataframe=dataframe.na.drop("all")
        if dataframe.count()==0:
            return "No Data in file"

       
        
        
     
        dataframe = dataframe.with_column("FILE_NAME",lit( file_name ))
        dataframe = dataframe.with_column("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        dataframe = dataframe.with_column("CRTD_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
       
        
        # Load Data to the target table
        dataframe.write.mode("append").saveAsTable(target_table)
       
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        #move to success
        file_name=file_name.split(".")[0]+''_''+datetime.now().strftime("%Y%m%d%H%M%S")
        dataframe.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)

        
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



CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.NTASDL_RAW.POP6_TW_MASTER_POPS_PREPROCESSING("PARAM" ARRAY)
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
    #Param=["20240616_pops.csv","NTASDL_RAW.DEV_LOAD_STAGE_ADLS_TWN","dev/pop6/masterdata/pops/","sdl_pop6_tw_pops"]
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        
        df_schema = StructType([
                StructField("Status", StringType(), True),
                StructField("popdb_id", StringType(), True),
                StructField("pop_code", StringType(), True),
                StructField("pop_name", StringType(), True),
                StructField("address", StringType(), True),
                StructField("longitude", StringType(), True),
                StructField("latitude", StringType(), True),
                StructField("Business_Units_ID", StringType(), True),
                StructField("Business_Unit_Name", StringType(), True),
                StructField("country", StringType(), True),
                StructField("channel", StringType(), True),
                StructField("retail_environment_ps", StringType(), True),
                StructField("customer", StringType(), True),
                StructField("sales_group_name", StringType(), True),
                StructField("Store_Type", StringType(), True),
                StructField("sales_group_code", StringType(), True),
                StructField("Customer_Grade", StringType(), True),
                StructField("External_Store_Code", StringType(), True),
                 StructField("Sales", StringType(), True),
                StructField("territory", StringType(), True),
               
                
                
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
            
            

        df = df.with_column(''hashkey'',md5(concat(when(is_null(col(''status'')),lit('''')).otherwise(col(''status'')),\\
                                            when(is_null(col(''popdb_id'')),lit('''')).otherwise(col(''popdb_id'')),\\
                                          when(is_null(col(''pop_code'')),lit('''')).otherwise(col(''pop_code'')),\\
                                          when(is_null(col(''pop_name'')),lit('''')).otherwise(col(''pop_name'')),\\
                                         when(is_null(col(''address'')),lit('''')).otherwise(col(''address'')),\\
                                         when(is_null(col(''longitude'')),lit('''')).otherwise(col(''longitude'')),\\
                                         when(is_null(col(''latitude'')),lit('''')).otherwise(col(''latitude'')),\\
                                          when(is_null(col(''country'')),lit('''')).otherwise(col(''country'')),\\
                                         when(is_null(col(''channel'')),lit('''')).otherwise(col(''channel'')),\\
                                         when(is_null(col(''retail_environment_ps'')),lit('''')).otherwise(col(''retail_environment_ps'')),\\
                                         when(is_null(col(''customer'')),lit('''')).otherwise(col(''customer'')),\\
                                         when(is_null(col(''sales_group_code'')),lit('''')).otherwise(col(''sales_group_code'')),\\
                                         when(is_null(col(''sales_group_name'')),lit('''')).otherwise(col(''sales_group_name'')),\\
                                        when(is_null(col(''customer_grade'')),lit('''')).otherwise(col(''customer_grade'')),\\
                                          when(is_null(col(''external_store_code'')),lit('''')).otherwise(col(''external_store_code'')),
                                                when(is_null(col(''business_units_id'')),lit('''')).otherwise(col(''business_units_id'')),\\
                                                 when(is_null(col(''sales'')),lit('''')).otherwise(col(''sales'')),\\
                                          when(is_null(col(''territory'')),lit('''')).otherwise(col(''territory'')))))
                                         
        
        df = df.with_column(''run_id'',lit(generunid()))

        df = df.with_column(''crtd_dttm'',lit(generatecrtd_dttm()))
        

        

        
        snowdf= df.select(
                    "Status",
                "popdb_id",
                "pop_code",
                "pop_name",
                "address",
                "longitude",
                "latitude",
                "country",
                "channel",
                "retail_environment_ps",
                "customer",
                "sales_group_code",
                "sales_group_name",
                "customer_grade",
                "external_store_code",
                "file_name",
                "Sales",
                "run_id",
                "crtd_dttm",
                "hashkey",
                "business_units_id",
                "business_unit_name",
                "territory" 
                
            
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
=

CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.NTASDL_RAW.POP6_TW_MASTER_PRODUCTS_PREPROCESSING("PARAM" ARRAY)
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
    #Param=["20240617_products.csv","NTASDL_RAW.DEV_LOAD_STAGE_ADLS_TWN","dev/pop6/masterdata/products/","sdl_pop6_tw_products"]
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        
        df_schema = StructType([
                StructField("Status", StringType(), True),
                StructField("productdb_id", StringType(), True),
                StructField("barcode", StringType(), True),
                StructField("sku", StringType(), True),
                StructField("unit_price", StringType(), True),
                StructField("display_order", StringType(), True),
                StructField("launch_date", StringType(), True),
                StructField("largest_uom_quantity", StringType(), True),
                StructField("middle_uom_quantity", StringType(), True),
                StructField("smallest_uom_quantity", StringType(), True),
                StructField("sku_english", StringType(), True),
                StructField("company", StringType(), True),
                StructField("sku_code", StringType(), True),
                StructField("ps_category", StringType(), True),
                StructField("ps_segment", StringType(), True),
                StructField("ps_category_segment", StringType(), True),
                StructField("country_l1", StringType(), True),
                StructField("regional_franchise_l2", StringType(), True),
                StructField("franchise_l3", StringType(), True),
                StructField("brand_l4", StringType(), True),
                StructField("sub_category_l5", StringType(), True),
                StructField("platform_level_l6", StringType(), True),
                StructField("variance_l7", StringType(), True),
                StructField("pack_size_l8", StringType(), True)
                
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
            
            

        df = df.with_column(''hashkey'',md5(concat(when(is_null(col(''status'')),lit('''')).otherwise(col(''status'')),\\
                                            when(is_null(col(''productdb_id'')),lit('''')).otherwise(col(''productdb_id'')),\\
                                          when(is_null(col(''barcode'')),lit('''')).otherwise(col(''barcode'')),\\
                                          when(is_null(col(''sku'')),lit('''')).otherwise(col(''sku'')),\\
                                         when(is_null(col(''unit_price'')),lit('''')).otherwise(col(''unit_price'')),\\
                                         when(is_null(col(''display_order'')),lit('''')).otherwise(col(''display_order'')),\\
                                         when(is_null(col(''launch_date'')),lit('''')).otherwise(col(''launch_date'')),\\
                                          when(is_null(col(''largest_uom_quantity'')),lit('''')).otherwise(col(''largest_uom_quantity'')),\\
                                         when(is_null(col(''middle_uom_quantity'')),lit('''')).otherwise(col(''middle_uom_quantity'')),\\
                                         when(is_null(col(''smallest_uom_quantity'')),lit('''')).otherwise(col(''smallest_uom_quantity'')),\\
                                         when(is_null(col(''company'')),lit('''')).otherwise(col(''company'')),\\
                                         when(is_null(col(''sku_english'')),lit('''')).otherwise(col(''sku_english'')),\\
                                         when(is_null(col(''sku_code'')),lit('''')).otherwise(col(''sku_code'')),\\
                                        when(is_null(col(''ps_category'')),lit('''')).otherwise(col(''ps_category'')),\\
                                          when(is_null(col(''ps_segment'')),lit('''')).otherwise(col(''ps_segment'')),\\
                                         when(is_null(col(''ps_category_segment'')),lit('''')).otherwise(col(''ps_category_segment'')),\\
                                         when(is_null(col(''country_l1'')),lit('''')).otherwise(col(''country_l1'')),\\
                                         when(is_null(col(''regional_franchise_l2'')),lit('''')).otherwise(col(''regional_franchise_l2'')),\\
                                          when(is_null(col(''franchise_l3'')),lit('''')).otherwise(col(''franchise_l3'')),\\
                                         when(is_null(col(''brand_l4'')),lit('''')).otherwise(col(''brand_l4'')),\\
                                         when(is_null(col(''sub_category_l5'')),lit('''')).otherwise(col(''sub_category_l5'')),\\
                                         when(is_null(col(''platform_level_l6'')),lit('''')).otherwise(col(''platform_level_l6'')),\\
                                         when(is_null(col(''variance_l7'')),lit('''')).otherwise(col(''variance_l7'')),\\
                                         when(is_null(col(''pack_size_l8'')),lit('''')).otherwise(col(''pack_size_l8'')))))

        
        df = df.with_column(''run_id'',lit(generunid()))

        df = df.with_column(''crtd_dttm'',lit(generatecrtd_dttm()))
        

        

        
        snowdf= df.select(
                   "Status", 
                "productdb_id", 
                "barcode", 
                "sku", 
                "unit_price", 
                "display_order", 
                "launch_date", 
                "largest_uom_quantity", 
                "middle_uom_quantity", 
                "smallest_uom_quantity", 
                "company", 
                "sku_english", 
                "sku_code", 
                "ps_category", 
                "ps_segment", 
                "ps_category_segment", 
                "country_l1", 
                "regional_franchise_l2", 
                "franchise_l3", 
                "brand_l4", 
                "sub_category_l5", 
                "platform_level_l6", 
                "variance_l7", 
                "pack_size_l8",
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

=
CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.NTASDL_RAW.POP6_TW_MASTER_USERS_PREPROCESSING("PARAM" ARRAY)
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
    # Param=["20240616_users.csv","NTASDL_RAW.DEV_LOAD_STAGE_ADLS_TWN","dev/pop6/masterdata/users/","sdl_pop6_tw_users"]
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        
        df_schema = StructType([
                StructField("Status", StringType(), True),
                StructField("USERDB_ID", StringType(), True),
                StructField("Username", StringType(), True),
                StructField("FIRST_NAME", StringType(), True),
                StructField("Last_Name", StringType(), True),
                StructField("Team", StringType(), True),
                StructField("Superior_Name", StringType(), True),
                StructField("Authorisation_Group", StringType(), True),
                StructField("Email_Address", StringType(), True),
                StructField("Longitude", StringType(), True),
                StructField("Latitude", StringType(), True),
                StructField("Business_Units_ID", StringType(), True),
                StructField("Business_Unit_Name", StringType(), True)
                
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
            
            

        df = df.with_column(''hashkey'',md5(concat(when(is_null(col(''status'')),lit('''')).otherwise(col(''status'')),\\
                                            when(is_null(col(''USERDB_ID'')),lit('''')).otherwise(col(''USERDB_ID'')),\\
                                          when(is_null(col(''Username'')),lit('''')).otherwise(col(''Username'')),\\
                                          when(is_null(col(''FIRST_NAME'')),lit('''')).otherwise(col(''FIRST_NAME'')),\\
                                         when(is_null(col(''Last_Name'')),lit('''')).otherwise(col(''Last_Name'')),\\
                                         when(is_null(col(''Team'')),lit('''')).otherwise(col(''Team'')),\\
                                         when(is_null(col(''Superior_Name'')),lit('''')).otherwise(col(''Superior_Name'')),\\
                                          when(is_null(col(''Authorisation_Group'')),lit('''')).otherwise(col(''Authorisation_Group'')),\\
                                         when(is_null(col(''Email_Address'')),lit('''')).otherwise(col(''Email_Address'')),\\
                                         when(is_null(col(''Longitude'')),lit('''')).otherwise(col(''Longitude'')),\\
                                         when(is_null(col(''Latitude'')),lit('''')).otherwise(col(''Latitude'')),\\
                                         when(is_null(col(''Business_Units_ID'')),lit('''')).otherwise(col(''Business_Units_ID'')),
                                         when(is_null(col(''Business_Unit_Name'')),lit('''')).otherwise(col(''Business_Unit_Name'')))))

        
        df = df.with_column(''run_id'',lit(generunid()))

        df = df.with_column(''crtd_dttm'',lit(generatecrtd_dttm()))
        

        

        
        snowdf= df.select(
                    "status",
                    "userdb_id",
                    "username",
                    "first_name",
                    "last_name",
                    "team",
                    "superior_name",
                    "authorisation_group",
                    "email_address",
                    "longitude",
                    "latitude",
                    "file_name",
                    "run_id",
                    "crtd_dttm",
                    "hashkey",
                    "business_units_id",
                    "business_unit_name"
            
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

==
CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.NTASDL_RAW.POP6_TW_MASTER_POP_LISTS_PREPROCESSING("PARAM" ARRAY)
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
    # Param=["20240617_pop_lists.csv","NTASDL_RAW.DEV_LOAD_STAGE_ADLS_TWN","dev/pop6/listdata/pop_lists","sdl_pop6_tw_pop_lists"]
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        
        df_schema = StructType([
                StructField("Status", StringType(), True),
                StructField("pop_list", StringType(), True),
                StructField("popdb_id", StringType(), True),
                StructField("pop_code", StringType(), True),
                StructField("pop_name", StringType(), True),
                StructField("Date", StringType(), True)
                
                
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
            
            

        df = df.with_column(''hashkey'',md5(concat(when(is_null(col(''status'')),lit('''')).otherwise(col(''status'')),\\
                                            when(is_null(col(''pop_list'')),lit('''')).otherwise(col(''pop_list'')),\\
                                          when(is_null(col(''popdb_id'')),lit('''')).otherwise(col(''popdb_id'')),\\
                                          when(is_null(col(''pop_code'')),lit('''')).otherwise(col(''pop_code'')),\\
                                         when(is_null(col(''pop_name'')),lit('''')).otherwise(col(''pop_name'')),\\
                                         when(is_null(col(''date'')),lit('''')).otherwise(col(''date'')))))

        
        df = df.with_column(''run_id'',lit(generunid()))

        df = df.with_column(''crtd_dttm'',lit(generatecrtd_dttm()))
        

        

        
        snowdf= df.select(
                    "status",
                    "pop_list",
                    "popdb_id",
                    "pop_code",
                    "pop_name",
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


CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.NTASDL_RAW.POP6_TW_MASTER_PRODUCT_LISTS_ALLOCATION_PREPROCESSING("PARAM" ARRAY)
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
    # Param=["20240617_product_lists_allocation.csv","NTASDL_RAW.DEV_LOAD_STAGE_ADLS_TWN","dev/pop6/listdata/product_lists_allocation","sdl_pop6_tw_product_lists_allocation"]
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

CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.NTASDL_RAW.POP6_KR_MASTER_POPS_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them

import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import lit, md5, concat, when, is_null, col
from snowflake.snowpark.types import  StringType, StructType, StructField
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz
from datetime import datetime, timedelta                


def main(session: snowpark.Session, Param): 
       
    try :
            
        # Param=[''20240617_pops.csv'',''NTASDL_RAW.DEV_LOAD_STAGE_ADLS_KOR'',''dev/pop6/masterdata/'',''sdl_pop6_kr_pops''] 

        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        # Define the schema for the DataFrame
        df_schema=StructType([
                        StructField("Status", StringType()),
                        StructField("POPDB_ID", StringType()),
                        StructField("POP_Code", StringType()),
                        StructField("POP_Name", StringType()),
                        StructField("Address", StringType()),
                        StructField("Longitude", StringType()),
                        StructField("Latitude", StringType()),
                        StructField("Business_Units_ID", StringType()),
                        StructField("Business_Unit_Name", StringType()),
                        StructField("Country", StringType()),
                        StructField("Channel", StringType()),
                        StructField("Retail Environment (PS)", StringType()),
                        StructField("Sales Group Name", StringType()),
                        StructField("Customer", StringType()),    
                        StructField("Store Type", StringType()),
                        StructField("Sales Group Code", StringType()),
                        StructField("Customer Grade", StringType()),
                        StructField("External Store Code", StringType()),
                        StructField("Territory/Region", StringType()),
                        StructField("Comments", StringType())
                        ])
                
        dataframe = session.read.schema(df_schema) \\
        .option("skip_header",1) \\
        .option("field_delimiter", ",") \\
        .option("field_optionally_enclosed_by", "\\"") \\
        .option("encoding","UTF-8")\\
        .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)    
        
        dataframe=dataframe.na.drop("all")
        if dataframe.count()==0:
            return "No Data in file"    
        
        dataframe = dataframe.with_column("FILE_NAME",lit(file_name))
        dataframe = dataframe.with_column("HASHKEY",md5(concat(when(is_null(col("Status")),lit("")).otherwise(col("Status")),
                                            when(is_null(col("POPDB_ID")),lit("")).otherwise(col("POPDB_ID")),
                                            when(is_null(col("POP_Code")),lit("")).otherwise(col("POP_Code")),
                                            when(is_null(col("POP_Name")),lit("")).otherwise(col("POP_Name")),
                                            when(is_null(col("Address")),lit("")).otherwise(col("Address")),
                                            when(is_null(col("Longitude")),lit("")).otherwise(col("Longitude")),
                                            when(is_null(col("Latitude")),lit("")).otherwise(col("Latitude")),
                                            when(is_null(col("Country")),lit("")).otherwise(col("Country")),
                                            when(is_null(col("Channel")),lit("")).otherwise(col("Channel")),
                                            when(is_null(col("Retail Environment (PS)")),lit("")).otherwise(col("Retail Environment (PS)")),
                                            when(is_null(col("Customer")),lit("")).otherwise(col("Customer")),
                                            when(is_null(col("Sales Group Code")),lit("")).otherwise(col("Sales Group Code")),
                                            when(is_null(col("Sales Group Name")),lit("")).otherwise(col("Sales Group Name")),
                                            when(is_null(col("Customer Grade")),lit("")).otherwise(col("Customer Grade")),
                                            when(is_null(col("External Store Code")),lit("")).otherwise(col("External Store Code")),
                                            when(is_null(col("Business_Units_ID")),lit("")).otherwise(col("Business_Units_ID")),
                                                when(is_null(col("Business_Unit_Name")),lit("")).otherwise(col("Business_Unit_Name")),
                                            when(is_null(col("Territory/Region")),lit("")).otherwise(col("Territory/Region")))))
        dataframe = dataframe.with_column("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        dataframe = dataframe.with_column("CRTD_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        
        snowdf= dataframe.select(
                    "Status",
                    "POPDB_ID",
                    "POP_Code",
                    "POP_Name",
                    "Address",
                    "Longitude",
                    "Latitude",
                    "Country",
                    "Channel",
                    "Retail Environment (PS)",
                    "Customer",
                    "Sales Group Code",
                    "Sales Group Name",
                    "Customer Grade",
                    "External Store Code",
                    "FILE_NAME",
                    "RUN_ID",
                    "CRTD_DTTM",
                    "HASHKEY",
                    "Business_Units_ID",
                    "Business_Unit_Name",
                    "Territory/Region"
                    )
        
        # Load Data to the target table
        snowdf.write.mode("append").saveAsTable(target_table)
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        #move to success
        file_name=file_name.split(".")[0]+"_"+datetime.now().strftime("%Y%m%d%H%M%S")
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
==
CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.NTASDL_RAW.POP6_TW_MASTER_PRODUCT_LISTS_POPS_PREPROCESSING("PARAM" ARRAY)
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
    # Param=["20240617_product_lists_pops.csv","NTASDL_RAW.DEV_LOAD_STAGE_ADLS_TWN","dev/pop6/listdata/product_lists_pops","sdl_pop6_tw_product_lists_pops"]
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        
        df_schema = StructType([
                StructField("Product_List", StringType(), True),
                StructField("Product_List_Code", StringType(), True),
                StructField("POPDB_ID", StringType(), True),
                StructField("POP_Code", StringType(), True),
                StructField("POP_Name", StringType(), True),
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
            
            

        df = df.with_column(''hashkey'',md5(concat(when(is_null(col(''product_list'')),lit('''')).otherwise(col(''product_list'')),\\
                                            when(is_null(col(''Product_List_Code'')),lit('''')).otherwise(col(''Product_List_Code'')),\\
                                          when(is_null(col(''POPDB_ID'')),lit('''')).otherwise(col(''POPDB_ID'')),\\
                                          when(is_null(col(''POP_Code'')),lit('''')).otherwise(col(''POP_Code'')),\\
                                         when(is_null(col(''POP_Name'')),lit('''')).otherwise(col(''POP_Name'')),\\
                                            when(is_null(col(''date'')),lit('''')).otherwise(col(''date'')))))

        
        df = df.with_column(''run_id'',lit(generunid()))

        df = df.with_column(''crtd_dttm'',lit(generatecrtd_dttm()))
        

        

        
        snowdf= df.select(
                    "product_list",
                    "popdb_id",
                    "pop_code",
                    "pop_name",
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




CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.NTASDL_RAW.POP6_TW_MASTER_PRODUCT_LISTS_PRODUCTS_PREPROCESSING("PARAM" ARRAY)
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
    # Param=["20240617_product_lists_products.csv","NTASDL_RAW.DEV_LOAD_STAGE_ADLS_TWN","dev/pop6/listdata/product_lists_products/","sdl_pop6_tw_product_lists_products"]
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



CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.NTASDL_RAW.POP6_KR_TRANS_LOAD_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS 'from snowflake.snowpark.functions import col, lit, date_format, current_timestamp, to_date, year, month, concat, format_number, regexp_replace,to_timestamp,when,trim,upper
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DecimalType,DateType
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz


def main(session: snowpark.Session, Param):
    try:
        # Param=[''20240617_tasks.csv'',''NTASDL_RAW.DEV_LOAD_STAGE_ADLS_KOR'',''dev/pop6/taskdata/'',''sdl_pop6_kr_tasks''] 
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        sch_name        = stage_name.split(''.'')[0]
        target_table    = sch_name+"."+Param[3]
        
        df = session.read\\
            .option("INFER_SCHEMA", True)\\
            .option("skip_header",0)\\
            .option("field_delimiter", ",")\\
            .option("field_optionally_enclosed_by", "\\"") \\
            .option("encoding","UTF-8") \\
            .option("trim_space", True) \\
            .with_metadata("METADATA$FILE_ROW_NUMBER") \\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        df = df.filter(col("METADATA$FILE_ROW_NUMBER") > lit(1))

        df = df.filter(col("METADATA$FILE_ROW_NUMBER") > lit(1))
        
        df = df.drop("METADATA$FILE_ROW_NUMBER")

        df = df.with_column("FILE_NAME",lit(file_name))
        df = df.with_column("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        df = df.withColumn("CRTD_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))

        if df.count()==0:
            return "No Data in file"
        
    
        # Load Data to the target table
        df.write.mode("append").saveAsTable(target_table)
       
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        #move to success
        file_name=file_name.split(".")[0]+"_"+datetime.now().strftime("%Y%m%d%H%M%S")
        df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)

        # write to success folder
    
        file_name=file_name.split(".")[0] + ''_'' + datetime.now().strftime("%Y%m%d%H%M%S")
        df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,file_format_type="csv",OVERWRITE=True,header=True)
   
        return "Success"
        
    except KeyError as key_error:
        
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message
        
    except Exception as e:
        
        error_message = f"Error: {str(e)}" +str(df.columns)
        return error_message
';

CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.NTASDL_RAW.POP6_TW_MASTER_PRODUCT_LISTS_PRODUCTS_PREPROCESSING("PARAM" ARRAY)
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
    # Param=["20240617_product_lists_products.csv","NTASDL_RAW.DEV_LOAD_STAGE_ADLS_TWN","dev/pop6/listdata/product_lists_products/","sdl_pop6_tw_product_lists_products"]
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


CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.NTASDL_RAW.POP6_TW_MASTER_PRODUCT_GROUPS_LISTS_PREPROCESSING("PARAM" ARRAY)
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
    # Param=["20200229_product_groups_lists.csv","NTASDL_RAW.DEV_LOAD_STAGE_ADLS_TWN","dev/pop6/listdata/product_group_lists/","sdl_pop6_tw_product_groups_lists"]
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        
        df_schema = StructType([
                StructField("Product_Group_status", StringType(), True),
                StructField("Product_Group", StringType(), True),
                StructField("Product_List_status", StringType(), True),
                StructField("Product_List", StringType(), True),
                StructField("ProductDB_ID", StringType(), True),
                StructField("SKU", StringType(), True),
                StructField("Date", StringType(), True),
                StructField("Custom_POP_Attribute_1", StringType(), True),
                StructField("Custom_POP_Attribute_2", StringType(), True),
                StructField("Custom_POP_Attribute_3", StringType(), True),
                
                
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
            
            

        df = df.with_column(''hashkey'',md5(concat(when(is_null(col(''product_group_status'')),lit('''')).otherwise(col(''product_group_status'')),\\
                                            when(is_null(col(''product_group'')),lit('''')).otherwise(col(''product_group'')),\\
                                          when(is_null(col(''product_list_status'')),lit('''')).otherwise(col(''product_list_status'')),\\
                                          when(is_null(col(''product_list'')),lit('''')).otherwise(col(''product_list'')),\\
                                         when(is_null(col(''productdb_id'')),lit('''')).otherwise(col(''productdb_id'')),\\
                                            when(is_null(col(''sku'')),lit('''')).otherwise(col(''sku'')),\\
                                             when(is_null(col(''date'')),lit('''')).otherwise(col(''date'')),\\
                                         when(is_null(col(''custom_pop_attribute_1'')),lit('''')).otherwise(col(''custom_pop_attribute_1'')),\\
                                            when(is_null(col(''custom_pop_attribute_2'')),lit('''')).otherwise(col(''custom_pop_attribute_2'')),
                                               when(is_null(col(''custom_pop_attribute_3'')),lit('''')).otherwise(col(''custom_pop_attribute_3'')))))

        
        df = df.with_column(''run_id'',lit(generunid()))

        df = df.with_column(''crtd_dttm'',lit(generatecrtd_dttm()))
        

        

        
        snowdf= df.select(
                    "product_group_status",
                    "product_group",
                    "product_list_status",
                    "product_list",
                    "productdb_id",
                    "sku",
                    "date",
                    "custom_pop_attribute_1",
                    "custom_pop_attribute_2",
                    "custom_pop_attribute_3",
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



CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.NTASDL_RAW.POP6_HK_TRANS_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them.

import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import lit,col
from snowflake.snowpark.types import  StringType, StructType, StructField,IntegerType
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz
from datetime import datetime, timedelta



def main(session: snowpark.Session,Param): 
       
    try :
           
        #Param=[''20220112_pop6_hk_data_exclusion.csv'',''NTASDL_RAW.DEV_LOAD_STAGE_ADLS_HKG'',''dev/pop6/exclusionsdata/'',''sdl_pop6_hk_exclusion'']

        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        sch_name        = stage_name.split(''.'')[0]
        target_table    = sch_name+"."+Param[3]
        
        df = session.read\\
            .option("INFER_SCHEMA", True)\\
            .option("skip_header",0)\\
            .option("field_delimiter", ",")\\
            .option("field_optionally_enclosed_by", "\\"") \\
            .option("encoding","UTF-8") \\
            .option("trim_space", True) \\
            .with_metadata("METADATA$FILE_ROW_NUMBER") \\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
        
       
        df = df.filter(col("METADATA$FILE_ROW_NUMBER") > lit(1))
        df = df.drop("METADATA$FILE_ROW_NUMBER")
        
        df=df.na.drop("all")
        if df.count()==0:
            return "No Data in file"

       
        
        
     
        df = df.with_column("FILE_NAME",lit( file_name ))
        df = df.with_column("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        df = df.with_column("CRTD_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
       
        
        # Load Data to the target table
        df.write.mode("append").saveAsTable(target_table)
       
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        #move to success
        file_name=file_name.split(".")[0]+''_''+datetime.now().strftime("%Y%m%d%H%M%S")
        df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)

        
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



CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.NTASDL_RAW.POP6_KR_MASTER_PRODUCT_GROUPS_LISTS_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them

import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import lit, md5, concat, when, is_null, col
from snowflake.snowpark.types import  StringType, StructType, StructField
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz
from datetime import datetime, timedelta                


def main(session: snowpark.Session, Param): 
       
    try :
            
        # Param=[''20200907_product_groups_lists.csv'',''NTASDL_RAW.DEV_LOAD_STAGE_ADLS_KOR'',''dev/pop6/master/listdata'',''sdl_pop6_kr_product_groups_lists''] 

        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        # Define the schema for the DataFrame

        df_schema=StructType([
                        StructField("Product_Group_status", StringType()),
                        StructField("Product_Group", StringType()),
                        StructField("Product_List_status", StringType()),
                        StructField("Product_List", StringType()),
                        StructField("ProductDB_ID", StringType()),
                        StructField("SKU", StringType()),
                        StructField("Date", StringType()),
                        StructField("Custom_POP_Attribute_1", StringType()),
                        StructField("Custom_POP_Attribute_2", StringType()),
                        StructField("Custom_POP_Attribute_3", StringType())
                        ])
                
        dataframe = session.read.schema(df_schema) \\
        .option("skip_header",1) \\
        .option("field_delimiter", ",") \\
        .option("field_optionally_enclosed_by", "\\"") \\
        .option("encoding","UTF-8")\\
        .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)    
        
        dataframe=dataframe.na.drop("all")
        if dataframe.count()==0:
            return "No Data in file"

        dataframe = dataframe.with_column("FILE_NAME",lit(file_name))
        dataframe = dataframe.with_column("HASHKEY",md5(concat(when(is_null(col("Product_Group_status")),lit("")).otherwise(col("Product_Group_status")),
                                            when(is_null(col("Product_Group")),lit("")).otherwise(col("Product_Group")),
                                            when(is_null(col("Product_List_status")),lit("")).otherwise(col("Product_List_status")),
                                            when(is_null(col("Product_List")),lit("")).otherwise(col("Product_List")),
                                            when(is_null(col("ProductDB_ID")),lit("")).otherwise(col("ProductDB_ID")),
                                            when(is_null(col("SKU")),lit("")).otherwise(col("SKU")),
                                            when(is_null(col("Date")),lit("")).otherwise(col("Date")),
                                            when(is_null(col("Custom_POP_Attribute_1")),lit("")).otherwise(col("Custom_POP_Attribute_1")),
                                            when(is_null(col("Custom_POP_Attribute_2")),lit("")).otherwise(col("Custom_POP_Attribute_2")),
                                            when(is_null(col("Custom_POP_Attribute_3")),lit("")).otherwise(col("Custom_POP_Attribute_3")))))
                                                              
        dataframe = dataframe.with_column("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        dataframe = dataframe.with_column("CRTD_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        
        snowdf= dataframe.select(
        	"Product_Group_status",
        	"Product_Group",
        	"Product_List_status",
        	"Product_List",
        	"ProductDB_ID",
        	"SKU",
            "Date",
            "Custom_POP_Attribute_1",
            "Custom_POP_Attribute_2",
            "Custom_POP_Attribute_3",
            "FILE_NAME",
        	"RUN_ID",
        	"CRTD_DTTM",
        	"HASHKEY"
        )
        
        # Load Data to the target table
        snowdf.write.mode("append").saveAsTable(target_table)
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        #move to success
        file_name=file_name.split(".")[0]+"_"+datetime.now().strftime("%Y%m%d%H%M%S")
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

=
CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.NTASDL_RAW.POP6_KR_MASTER_PRODUCT_LISTS_ALLOCATION_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them

import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import lit, md5, concat, when, is_null, col
from snowflake.snowpark.types import  StringType, StructType, StructField
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz
from datetime import datetime, timedelta                


def main(session: snowpark.Session, Param): 
       
    try :
            
        # Param=[''20240623_product_lists_allocation.csv'',''NTASDL_RAW.DEV_LOAD_STAGE_ADLS_KOR'',''dev/pop6/master/listdata'',''sdl_pop6_kr_product_lists_allocation''] 

        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        # Define the schema for the DataFrame

        df_schema=StructType([
                        StructField("Product_Group_Status", StringType()),
                        StructField("Product_Group", StringType()),
                        StructField("Product_List_Status", StringType()),
                        StructField("Product_List", StringType()),
                        StructField("Product_List_Code", StringType()),
                        StructField("POP_Attribute_ID", StringType()),
                        StructField("POP_Attribute", StringType()),
                        StructField("POP_Attribute_Value_ID", StringType()),
                        StructField("POP_Attribute_Value", StringType()),
                        StructField("Date", StringType())
                        ])
                
        dataframe = session.read.schema(df_schema) \\
        .option("skip_header",1) \\
        .option("field_delimiter", ",") \\
        .option("field_optionally_enclosed_by", "\\"") \\
        .option("encoding","UTF-8")\\
        .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)    
        
        dataframe=dataframe.na.drop("all")
        if dataframe.count()==0:
            return "No Data in file"

        dataframe = dataframe.with_column("FILE_NAME",lit(file_name))
        dataframe = dataframe.with_column("HASHKEY",md5(concat(when(is_null(col("Product_Group_Status")),lit("")).otherwise(col("Product_Group_Status")),
                                            when(is_null(col("Product_Group")),lit("")).otherwise(col("Product_Group")),
                                            when(is_null(col("Product_List_Status")),lit("")).otherwise(col("Product_List_Status")),
                                            when(is_null(col("Product_List")),lit("")).otherwise(col("Product_List")),
                                            when(is_null(col("Product_List_Code")),lit("")).otherwise(col("Product_List_Code")),
                                            when(is_null(col("POP_Attribute_ID")),lit("")).otherwise(col("POP_Attribute_ID")),
                                            when(is_null(col("POP_Attribute")),lit("")).otherwise(col("POP_Attribute")),
                                            when(is_null(col("POP_Attribute_Value_ID")),lit("")).otherwise(col("POP_Attribute_Value_ID")),
                                            when(is_null(col("POP_Attribute_Value")),lit("")).otherwise(col("POP_Attribute_Value")),
                                            when(is_null(col("Date")),lit("")).otherwise(col("Date")))))
                                                              
        dataframe = dataframe.with_column("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        dataframe = dataframe.with_column("CRTD_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        
        snowdf= dataframe.select(
        	"Product_Group_Status",
        	"Product_Group",
        	"Product_List_Status",
        	"Product_List",
        	"POP_Attribute_ID",
            "POP_Attribute",
            "POP_Attribute_Value_ID",
            "POP_Attribute_Value",
            "Date",
            "FILE_NAME",
        	"RUN_ID",
        	"CRTD_DTTM",
        	"HASHKEY"
        )
        
        # Load Data to the target table
        snowdf.write.mode("append").saveAsTable(target_table)
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        #move to success
        file_name=file_name.split(".")[0]+"_"+datetime.now().strftime("%Y%m%d%H%M%S")
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




CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.NTASDL_RAW.POP6_KR_MASTER_PRODUCT_LISTS_POPS_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them

import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import lit, md5, concat, when, is_null, col
from snowflake.snowpark.types import  StringType, StructType, StructField
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz
from datetime import datetime, timedelta                


def main(session: snowpark.Session, Param): 
       
    try :
            
        # Param=[''20240624_product_lists_pops.csv'',''NTASDL_RAW.DEV_LOAD_STAGE_ADLS_KOR'',''dev/pop6/master/listdata'',''sdl_pop6_kr_product_lists_pops''] 

        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        # Define the schema for the DataFrame
        
        df_schema=StructType([
                        StructField("Product_List", StringType()),
                        StructField("Product_List_Code", StringType()),
                        StructField("POPDB_ID", StringType()),
                        StructField("POP_Code", StringType()),
                        StructField("POP_Name", StringType()),
                        StructField("Date", StringType())
                        ])
                
        dataframe = session.read.schema(df_schema) \\
        .option("skip_header",1) \\
        .option("field_delimiter", ",") \\
        .option("field_optionally_enclosed_by", "\\"") \\
        .option("encoding","UTF-8")\\
        .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)    
        
        dataframe=dataframe.na.drop("all")
        if dataframe.count()==0:
            return "No Data in file"
        
        dataframe = dataframe.with_column("FILE_NAME",lit(file_name))
        
        dataframe = dataframe.with_column("HASHKEY",md5(concat(when(is_null(col("Product_List")),lit("")).otherwise(col("Product_List")),
                                            when(is_null(col("POPDB_ID")),lit("")).otherwise(col("POPDB_ID")),
                                            when(is_null(col("POP_Code")),lit("")).otherwise(col("POP_Code")),
                                            when(is_null(col("POP_Name")),lit("")).otherwise(col("POP_Name")),
                                            when(is_null(col("Date")),lit("")).otherwise(col("Date")))))
                                                              
        dataframe = dataframe.with_column("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        dataframe = dataframe.with_column("CRTD_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        
        snowdf= dataframe.select(
        	"Product_List",
        	"POPDB_ID",
        	"POP_Code",
        	"POP_Name",
        	"Date",
            "FILE_NAME",
        	"RUN_ID",
        	"CRTD_DTTM",
        	"HASHKEY"
        )
        
        # Load Data to the target table
        snowdf.write.mode("append").saveAsTable(target_table)
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        #move to success
        file_name=file_name.split(".")[0]+"_"+datetime.now().strftime("%Y%m%d%H%M%S")
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

=


CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.NTASDL_RAW.POP6_KR_MASTER_PRODUCT_LISTS_PRODUCTS_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them

import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import lit, md5, concat, when, is_null, col
from snowflake.snowpark.types import  StringType, StructType, StructField
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz
from datetime import datetime, timedelta                


def main(session: snowpark.Session, Param): 
       
    try :
            
        # Param=[''20240624_product_lists_products.csv'',''NTASDL_RAW.DEV_LOAD_STAGE_ADLS_KOR'',''dev/pop6/master/listdata'',''sdl_pop6_kr_product_lists_products''] 

        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        # Define the schema for the DataFrame

        
        df_schema=StructType([
                        StructField("Product_List", StringType()),
                        StructField("Product_List_Code", StringType()),
                        StructField("ProductDB_ID", StringType()),
                        StructField("SKU", StringType()),
                        StructField("MSL_Ranking", StringType()),
                        StructField("Date", StringType())
                        ])
                
        dataframe = session.read.schema(df_schema) \\
        .option("skip_header",1) \\
        .option("field_delimiter", ",") \\
        .option("field_optionally_enclosed_by", "\\"") \\
        .option("encoding","UTF-8")\\
        .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)    
        
        dataframe=dataframe.na.drop("all")
        if dataframe.count()==0:
            return "No Data in file"
        
        dataframe = dataframe.with_column("FILE_NAME",lit(file_name))

        
        dataframe = dataframe.with_column("HASHKEY",md5(concat(when(is_null(col("Product_List")),lit("")).otherwise(col("Product_List")),
                                            when(is_null(col("ProductDB_ID")),lit("")).otherwise(col("ProductDB_ID")),
                                            when(is_null(col("SKU")),lit("")).otherwise(col("SKU")),
                                            when(is_null(col("MSL_Ranking")),lit("")).otherwise(col("MSL_Ranking")),
                                            when(is_null(col("Date")),lit("")).otherwise(col("Date")))))
                                                              
        dataframe = dataframe.with_column("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        dataframe = dataframe.with_column("CRTD_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))

        snowdf= dataframe.select(
        	"Product_List",
        	"ProductDB_ID",
        	"SKU",
        	"MSL_Ranking",
        	"Date",
            "FILE_NAME",
        	"RUN_ID",
        	"CRTD_DTTM",
        	"HASHKEY"
        )
        
        # Load Data to the target table
        snowdf.write.mode("append").saveAsTable(target_table)
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        #move to success
        file_name=file_name.split(".")[0]+"_"+datetime.now().strftime("%Y%m%d%H%M%S")
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




CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.NTASDL_RAW.POP6_NA_TRANSACTION_SERVICE_LEVELS_PREPROCESSING("PARAM" ARRAY)
RETURNS TABLE ()
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
    # Param=["20240617_service_levels.csv","JPNSDL_RAW.DEV_LOAD_STAGE_ADLS","dev/pop6/transaction/visitdata/service_levels/","SDL_POP6_JP_SERVICE_LEVELS"]
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        
        
        df = session.read\\
            .option("INFER_SCHEMA",True)\\
            .option("field_delimiter", ",")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .option("encoding",''UTF-8'')\\
            .with_metadata("METADATA$FILE_ROW_NUMBER") \\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        df = df.filter(col("METADATA$FILE_ROW_NUMBER") > lit(1))
        
        df = df.drop("METADATA$FILE_ROW_NUMBER")

       
        
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
            
            

       
        
        df = df.with_column(''run_id'',lit(generunid()))

        df = df.with_column(''crtd_dttm'',lit(generatecrtd_dttm()))
        

        

        
        
        file_name=file_name.split(".")[0]+"_"+datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")

        df.write.mode("append").saveAsTable(stage_name.split(".")[0] + "." + target_table)
        
        # #move to success
        df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)
        
        
        
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



CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.NTASDL_RAW.POP6_NA_TRANSACTION_PLANNED_VISITS_PREPROCESSING("PARAM" ARRAY)
RETURNS TABLE ()
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
    # Param=["20240617_service_levels.csv","JPNSDL_RAW.DEV_LOAD_STAGE_ADLS","dev/pop6/transaction/visitdata/service_levels/","SDL_POP6_JP_SERVICE_LEVELS"]
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        
        
        df = session.read\\
            .option("INFER_SCHEMA",True)\\
            .option("field_delimiter", ",")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .option("encoding",''UTF-8'')\\
            .with_metadata("METADATA$FILE_ROW_NUMBER") \\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        df = df.filter(col("METADATA$FILE_ROW_NUMBER") > lit(1))
        
        df = df.drop("METADATA$FILE_ROW_NUMBER")

       
        
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
            
            

       
        
        df = df.with_column(''run_id'',lit(generunid()))

        df = df.with_column(''crtd_dttm'',lit(generatecrtd_dttm()))
        

        

        
        
        file_name=file_name.split(".")[0]+"_"+datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")

        df.write.mode("append").saveAsTable(stage_name.split(".")[0] + "." + target_table)
        
        # #move to success
        df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)
        
        
        
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



CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.NTASDL_RAW.POP6_NA_TRANSACTION_EXECUTED_VISITS_PREPROCESSING("PARAM" ARRAY)
RETURNS TABLE ()
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
    # Param=["20240617_service_levels.csv","JPNSDL_RAW.DEV_LOAD_STAGE_ADLS","dev/pop6/transaction/visitdata/service_levels/","SDL_POP6_JP_SERVICE_LEVELS"]
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        
        
        df = session.read\\
            .option("INFER_SCHEMA",True)\\
            .option("field_delimiter", ",")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .option("encoding",''UTF-8'')\\
            .with_metadata("METADATA$FILE_ROW_NUMBER") \\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        df = df.filter(col("METADATA$FILE_ROW_NUMBER") > lit(1))
        
        df = df.drop("METADATA$FILE_ROW_NUMBER")

       
        
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
            
            

       
        
        df = df.with_column(''run_id'',lit(generunid()))

        df = df.with_column(''crtd_dttm'',lit(generatecrtd_dttm()))
        

        

        
        
        file_name=file_name.split(".")[0]+"_"+datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")

        df.write.mode("append").saveAsTable(stage_name.split(".")[0] + "." + target_table)
        
        # #move to success
        df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)
        
        
        
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

==

CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.NTASDL_RAW.POP6_NA_TRANSACTION_PRODUCT_ATTRIBUTE_AUDITS_PREPROCESSING("PARAM" ARRAY)
RETURNS TABLE ()
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
    # Param=["20240617_service_levels.csv","JPNSDL_RAW.DEV_LOAD_STAGE_ADLS","dev/pop6/transaction/visitdata/service_levels/","SDL_POP6_JP_SERVICE_LEVELS"]
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        
        
        df = session.read\\
            .option("INFER_SCHEMA",True)\\
            .option("field_delimiter", ",")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .option("encoding",''UTF-8'')\\
            .with_metadata("METADATA$FILE_ROW_NUMBER") \\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        df = df.filter(col("METADATA$FILE_ROW_NUMBER") > lit(1))
        
        df = df.drop("METADATA$FILE_ROW_NUMBER")

       
        
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
            
            

       
        
        df = df.with_column(''run_id'',lit(generunid()))

        df = df.with_column(''crtd_dttm'',lit(generatecrtd_dttm()))
        

        

        
        
        file_name=file_name.split(".")[0]+"_"+datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")

        df.write.mode("append").saveAsTable(stage_name.split(".")[0] + "." + target_table)
        
        # #move to success
        df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)
        
        
        
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



CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.NTASDL_RAW.POP6_NA_TRANSACTION_GENERAL_AUDITS_PREPROCESSING("PARAM" ARRAY)
RETURNS TABLE ()
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
    # Param=["20240617_service_levels.csv","JPNSDL_RAW.DEV_LOAD_STAGE_ADLS","dev/pop6/transaction/visitdata/service_levels/","SDL_POP6_JP_SERVICE_LEVELS"]
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        
        
        df = session.read\\
            .option("INFER_SCHEMA",True)\\
            .option("field_delimiter", ",")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .option("encoding",''UTF-8'')\\
            .with_metadata("METADATA$FILE_ROW_NUMBER") \\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        df = df.filter(col("METADATA$FILE_ROW_NUMBER") > lit(1))
        
        df = df.drop("METADATA$FILE_ROW_NUMBER")

       
        
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
            
            

       
        
        df = df.with_column(''run_id'',lit(generunid()))

        df = df.with_column(''crtd_dttm'',lit(generatecrtd_dttm()))
        

        

        
        
        file_name=file_name.split(".")[0]+"_"+datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")

        df.write.mode("append").saveAsTable(stage_name.split(".")[0] + "." + target_table)
        
        # #move to success
        df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)
        
        
        
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

==

CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.NTASDL_RAW.POP6_NA_TRANSACTION_DISPLAY_PLANS_PREPROCESSING("PARAM" ARRAY)
RETURNS TABLE ()
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
    # Param=["20240617_service_levels.csv","JPNSDL_RAW.DEV_LOAD_STAGE_ADLS","dev/pop6/transaction/visitdata/service_levels/","SDL_POP6_JP_SERVICE_LEVELS"]
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        
        
        df = session.read\\
            .option("INFER_SCHEMA",True)\\
            .option("field_delimiter", ",")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .option("encoding",''UTF-8'')\\
            .with_metadata("METADATA$FILE_ROW_NUMBER") \\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        df = df.filter(col("METADATA$FILE_ROW_NUMBER") > lit(1))
        
        df = df.drop("METADATA$FILE_ROW_NUMBER")

       
        
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
            
            

       
        
        df = df.with_column(''run_id'',lit(generunid()))

        df = df.with_column(''crtd_dttm'',lit(generatecrtd_dttm()))
        

        

        
        
        file_name=file_name.split(".")[0]+"_"+datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")

        df.write.mode("append").saveAsTable(stage_name.split(".")[0] + "." + target_table)
        
        # #move to success
        df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)
        
        
        
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



CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.NTASDL_RAW.POP6_NA_TRANSACTION_PROMOTION_PLANS_PREPROCESSING("PARAM" ARRAY)
RETURNS TABLE ()
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
    # Param=["20240617_service_levels.csv","JPNSDL_RAW.DEV_LOAD_STAGE_ADLS","dev/pop6/transaction/visitdata/service_levels/","SDL_POP6_JP_SERVICE_LEVELS"]
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        
        
        df = session.read\\
            .option("INFER_SCHEMA",True)\\
            .option("field_delimiter", ",")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .option("encoding",''UTF-8'')\\
            .with_metadata("METADATA$FILE_ROW_NUMBER") \\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        df = df.filter(col("METADATA$FILE_ROW_NUMBER") > lit(1))
        
        df = df.drop("METADATA$FILE_ROW_NUMBER")

       
        
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
            
            

       
        
        df = df.with_column(''run_id'',lit(generunid()))

        df = df.with_column(''crtd_dttm'',lit(generatecrtd_dttm()))
        

        

        
        
        file_name=file_name.split(".")[0]+"_"+datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")

        df.write.mode("append").saveAsTable(stage_name.split(".")[0] + "." + target_table)
        
        # #move to success
        df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)
        
        
        
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



CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.NTASDL_RAW.POP6_NA_TRANSACTION_PROMOTIONS_TRANSACTION("PARAM" ARRAY)
RETURNS TABLE ()
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
    # Param=["20240617_service_levels.csv","JPNSDL_RAW.DEV_LOAD_STAGE_ADLS","dev/pop6/transaction/visitdata/service_levels/","SDL_POP6_JP_SERVICE_LEVELS"]
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        
        
        df = session.read\\
            .option("INFER_SCHEMA",True)\\
            .option("field_delimiter", ",")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .option("encoding",''UTF-8'')\\
            .with_metadata("METADATA$FILE_ROW_NUMBER") \\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        df = df.filter(col("METADATA$FILE_ROW_NUMBER") > lit(1))
        
        df = df.drop("METADATA$FILE_ROW_NUMBER")

       
        
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
            
            

       
        
        df = df.with_column(''run_id'',lit(generunid()))

        df = df.with_column(''crtd_dttm'',lit(generatecrtd_dttm()))
        

        

        
        
        file_name=file_name.split(".")[0]+"_"+datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")

        df.write.mode("append").saveAsTable(stage_name.split(".")[0] + "." + target_table)
        
        # #move to success
        df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)
        
        
        
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



CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.NTASDL_RAW.POP6_NA_TRANSACTION_TASKS_PREPROCESSING("PARAM" ARRAY)
RETURNS TABLE ()
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
    # Param=["20240617_service_levels.csv","JPNSDL_RAW.DEV_LOAD_STAGE_ADLS","dev/pop6/transaction/visitdata/service_levels/","SDL_POP6_JP_SERVICE_LEVELS"]
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        
        
        df = session.read\\
            .option("INFER_SCHEMA",True)\\
            .option("field_delimiter", ",")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .option("encoding",''UTF-8'')\\
            .with_metadata("METADATA$FILE_ROW_NUMBER") \\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        df = df.filter(col("METADATA$FILE_ROW_NUMBER") > lit(1))
        
        df = df.drop("METADATA$FILE_ROW_NUMBER")

       
        
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
            
            

       
        
        df = df.with_column(''run_id'',lit(generunid()))

        df = df.with_column(''crtd_dttm'',lit(generatecrtd_dttm()))
        

        

        
        
        file_name=file_name.split(".")[0]+"_"+datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")

        df.write.mode("append").saveAsTable(stage_name.split(".")[0] + "." + target_table)
        
        # #move to success
        df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)
        
        
        
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



CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.NTASDL_RAW.POP6_NA_TRANSACTION_EXCLUSION_PREPROCESSING("PARAM" ARRAY)
RETURNS TABLE ()
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
    # Param=["20240617_service_levels.csv","JPNSDL_RAW.DEV_LOAD_STAGE_ADLS","dev/pop6/transaction/visitdata/service_levels/","SDL_POP6_JP_SERVICE_LEVELS"]
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        
        
        df = session.read\\
            .option("INFER_SCHEMA",True)\\
            .option("field_delimiter", ",")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .option("encoding",''UTF-8'')\\
            .with_metadata("METADATA$FILE_ROW_NUMBER") \\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        df = df.filter(col("METADATA$FILE_ROW_NUMBER") > lit(1))
        
        df = df.drop("METADATA$FILE_ROW_NUMBER")

       
        
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
            
            

       
        
        df = df.with_column(''run_id'',lit(generunid()))

        df = df.with_column(''crtd_dttm'',lit(generatecrtd_dttm()))
        

        

        
        
        file_name=file_name.split(".")[0]+"_"+datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")

        df.write.mode("append").saveAsTable(stage_name.split(".")[0] + "." + target_table)
        
        # #move to success
        df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)
        
        
        
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



CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.NTASDL_RAW.POP6_NA_TRANSACTION_DISPLAYS_PREPROCESSING("PARAM" ARRAY)
RETURNS TABLE ()
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
    # Param=["20240617_service_levels.csv","JPNSDL_RAW.DEV_LOAD_STAGE_ADLS","dev/pop6/transaction/visitdata/service_levels/","SDL_POP6_JP_SERVICE_LEVELS"]
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        
        
        df = session.read\\
            .option("INFER_SCHEMA",True)\\
            .option("field_delimiter", ",")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .option("encoding",''UTF-8'')\\
            .with_metadata("METADATA$FILE_ROW_NUMBER") \\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        df = df.filter(col("METADATA$FILE_ROW_NUMBER") > lit(1))
        
        df = df.drop("METADATA$FILE_ROW_NUMBER")

       
        
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
            
            

       
        
        df = df.with_column(''run_id'',lit(generunid()))

        df = df.with_column(''crtd_dttm'',lit(generatecrtd_dttm()))
        

        

        
        
        file_name=file_name.split(".")[0]+"_"+datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")

        df.write.mode("append").saveAsTable(stage_name.split(".")[0] + "." + target_table)
        
        # #move to success
        df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)
        
        
        
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

==

CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.NTASDL_RAW.POP6_NA_TRANSACTION_SKU_AUDITS_PREPROCESSING("PARAM" ARRAY)
RETURNS TABLE ()
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
    # Param=["20240617_service_levels.csv","JPNSDL_RAW.DEV_LOAD_STAGE_ADLS","dev/pop6/transaction/visitdata/service_levels/","SDL_POP6_JP_SERVICE_LEVELS"]
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        
        
        df = session.read\\
            .option("INFER_SCHEMA",True)\\
            .option("field_delimiter", ",")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .option("encoding",''UTF-8'')\\
            .with_metadata("METADATA$FILE_ROW_NUMBER") \\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        df = df.filter(col("METADATA$FILE_ROW_NUMBER") > lit(1))
        
        df = df.drop("METADATA$FILE_ROW_NUMBER")

       
        
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
            
            

       
        
        df = df.with_column(''run_id'',lit(generunid()))

        df = df.with_column(''crtd_dttm'',lit(generatecrtd_dttm()))
        

        

        
        
        file_name=file_name.split(".")[0]+"_"+datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")

        df.write.mode("append").saveAsTable(stage_name.split(".")[0] + "." + target_table)
        
        # #move to success
        df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)
        
        
        
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



CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.NTASDL_RAW.SDL_POP6_TH_POP_LISTS_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS 'from snowflake.snowpark.functions import col, lit, date_format, current_timestamp, to_date,md5, year, month, concat, format_number, regexp_replace, to_timestamp, when, trim, upper
from snowflake.snowpark.types import StringType, StructType, StructField,DecimalType
import snowflake.snowpark
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz

def main(session: snowpark.Session,Param):
    #Param=["20240422_pop_lists.csv","THASDL_RAW.DEV_LOAD_STAGE_ADLS","dev/pop6","SDL_POP6_TH_POP_LISTS"]
    try:
        file_name = Param[0]
        stage_name = Param[1]
        temp_stage_path = Param[2]
        target_table = Param[3]

        df_schema = StructType([
                    StructField("Status", StringType(), nullable=True),
                    StructField("POP_List", StringType(), nullable=True),
                    StructField("POPDB_ID", StringType(), nullable=True),
                    StructField("POP_Code", StringType(), nullable=True),
                    StructField("POP_Name", StringType(), nullable=True),
                    StructField("Date", StringType(), nullable=True)
                ])
        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "|")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name.split(".")[0])
        
              
        df = df.na.drop("all")
        if df.count() == 0:
            return "No Data in file"

        
        df = df.withColumn("filename", lit(file_name))
        df = df.withColumn("crt_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        df = df.withColumn("run_id", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))

        df1=df
        
        df1=df1.na.fill("",subset=["Status",
                                "POP_List",
                                "POPDB_ID",
                                "POP_Code",
                                "POP_Name"
                                ])

        df1 = df1.withColumn("hash_c",concat(df1.Status,df1.POP_List,df1.POPDB_ID,df1.POP_Code,df1.POP_Name))
        df1=df1.withColumn("hash",md5(col("hash_c")))
        df1=df1.na.replace({"":None},["Status",
                                    "POP_List",
                                    "POPDB_ID",
                                    "POP_Code",
                                    "POP_Name"
                                ])
        snowdf = df1.select(
                           "Status",
                            "POP_List",
                            "POPDB_ID",
                            "POP_Code",
                            "POP_Name",
                            "Date",
                            "filename",
                            "run_id",
                            "crt_dttm",
                            "hash"
                        )
        
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        
        #move to success
        
        
        snowdf.write.mode("append").saveAsTable(stage_name.split(".")[0] + "." + target_table)
        file_name1 = file_name.split(".")[0] + "_" + datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S") + ".csv"
        snowdf.write.copy_into_location("@" + stage_name + "/" + temp_stage_path + "/processed/success/" + formatted_year + "/" + formatted_month + "/" + file_name1, header=True, OVERWRITE=True)
        
        return "Success"

    except KeyError as key_error:
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message

    except pd.errors.MergeError as merge_error:
        error_message = f"DataFrame merging error: {str(merge_error)}"
        return error_message

    except Exception as e:
        error_message = f"Error: {str(e)}"
        return error_message
        
        
        
        
        ';




CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.NTASDL_RAW.SDL_POP6_TH_PRODUCT_LISTS_ALLOCATION_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS 'from snowflake.snowpark.functions import col, lit, date_format, current_timestamp, to_date,md5, year, month, concat, format_number, regexp_replace, to_timestamp, when, trim, upper
from snowflake.snowpark.types import StringType, StructType, StructField,DecimalType
import snowflake.snowpark
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz

def main(session: snowpark.Session,Param):
    #Param=["20240422_product_lists_allocation.csv","THASDL_RAW.DEV_LOAD_STAGE_ADLS","dev/pop6","sdl_pop6_th_product_lists_allocation"]
    try:
        file_name = Param[0]
        stage_name = Param[1]
        temp_stage_path = Param[2]
        target_table = Param[3]

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
                    StructField("Date", StringType(), True)
                ])
        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "|")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name.split(".")[0])
        
              
        df = df.na.drop("all")
        if df.count() == 0:
            return "No Data in file"

        
        df = df.withColumn("filename", lit(file_name))
        df = df.withColumn("crt_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        df = df.withColumn("run_id", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))

        df1=df
        
        df1=df1.na.fill("",subset=["Product_Group_Status",
                                    "Product_Group",
                                    "Product_List_Status",
                                    "Product_List",
                                    "POP_Attribute_ID",
                                    "POP_Attribute",
                                    "POP_Attribute_Value_ID",
                                    "POP_Attribute_Value"
    
                                ])

        df1 = df1.withColumn("hash_c",concat(df1.Product_Group_Status,df1.Product_Group,df1.Product_List_Status,df1.POP_Attribute_ID,df1.POP_Attribute,df1.POP_Attribute_Value_ID,df1.POP_Attribute_Value,df1.Date))
        df1=df1.withColumn("hash",md5(col("hash_c")))
        df1=df1.na.replace({"":None},["Product_Group_Status",
                                    "Product_Group",
                                    "Product_List_Status",
                                    "Product_List"
                                    "POP_Attribute_ID",
                                    "POP_Attribute",
                                    "POP_Attribute_Value_ID",
                                    "POP_Attribute_Value"
                                ])
        snowdf = df1.select(
                          "Product_Group_Status",
                            "Product_Group",
                            "Product_List_Status",
                            "Product_List",
                            "Product_List_Code",
                            "POP_Attribute_ID",
                            "POP_Attribute",
                            "POP_Attribute_Value_ID",
                            "POP_Attribute_Value",
                            "Date",
                            "filename",
                            "run_id",
                            "crt_dttm",
                            "hash"
                        )
        
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        
        #move to success
        
        
        snowdf.write.mode("append").saveAsTable(stage_name.split(".")[0] + "." + target_table)
        file_name1 = file_name.split(".")[0] + "_" + datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S") + ".csv"
        snowdf.write.copy_into_location("@" + stage_name + "/" + temp_stage_path + "/processed/success/" + formatted_year + "/" + formatted_month + "/" + file_name1, header=True, OVERWRITE=True)
        
        return "Success"

    except KeyError as key_error:
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message

    except pd.errors.MergeError as merge_error:
        error_message = f"DataFrame merging error: {str(merge_error)}"
        return error_message

    except Exception as e:
        error_message = f"Error: {str(e)}"
        return error_message
        
        
        
        
        ';


CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.NTASDL_RAW.SDL_POP6_TH_PRODUCT_LISTS_POPS_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS 'from snowflake.snowpark.functions import col, lit, date_format, current_timestamp, to_date,md5, year, month, concat, format_number, regexp_replace, to_timestamp, when, trim, upper
from snowflake.snowpark.types import StringType, StructType, StructField,DecimalType
import snowflake.snowpark
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz

def main(session: snowpark.Session,Param):
    #Param=["20240422_product_lists_pops.csv","THASDL_RAW.DEV_LOAD_STAGE_ADLS","dev/pop6","SDL_POP6_TH_PRODUCT_LISTS_POPS"]
    try:
        file_name = Param[0]
        stage_name = Param[1]
        temp_stage_path = Param[2]
        target_table = Param[3]

        df_schema = StructType([
                    StructField("Product_List", StringType(), True),
                    StructField("Product_List_Code", StringType(), True),
                    StructField("POPDB_ID", StringType(), True),
                    StructField("POP_Code", StringType(), True),
                    StructField("POP_Name", StringType(), True),
                    StructField("Date", StringType(), True)
                ])
        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "|")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name.split(".")[0])
        
              
        df = df.na.drop("all")
        if df.count() == 0:
            return "No Data in file"

        
        df = df.withColumn("filename", lit(file_name))
        df = df.withColumn("crt_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        df = df.withColumn("run_id", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))

        df1=df
        
        df1=df1.na.fill("",subset=["Product_List", "POPDB_ID", "POP_Code", "POP_Name"])

        df1 = df1.withColumn("hash_c",concat(df1.Product_List,df1.POPDB_ID,df1.POP_Code,df1.POP_Name,df1.Date))
        df1=df1.withColumn("hash",md5(col("hash_c")))
        df1=df1.na.replace({"":None},["Product_List", "POPDB_ID", "POP_Code", "POP_Name"])
        snowdf = df1.select(
                          "Product_List", "POPDB_ID", "POP_Code", "POP_Name",
                            "Date",
                            "filename",
                            "run_id",
                            "crt_dttm",
                            "hash"
                        )
        
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        
        #move to success
        
        
        snowdf.write.mode("append").saveAsTable(stage_name.split(".")[0] + "." + target_table)
        file_name1 = file_name.split(".")[0] + "_" + datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S") + ".csv"
        snowdf.write.copy_into_location("@" + stage_name + "/" + temp_stage_path + "/processed/success/" + formatted_year + "/" + formatted_month + "/" + file_name1, header=True, OVERWRITE=True)
        
        return "Success"

    except KeyError as key_error:
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message

    except pd.errors.MergeError as merge_error:
        error_message = f"DataFrame merging error: {str(merge_error)}"
        return error_message

    except Exception as e:
        error_message = f"Error: {str(e)}"
        return error_message
        ';


CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.NTASDL_RAW.SDL_POP6_TH_PRODUCT_LISTS_PRODUCTS_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS 'from snowflake.snowpark.functions import col, lit, date_format, current_timestamp, to_date,md5, year, month, concat, format_number, regexp_replace, to_timestamp, when, trim, upper
from snowflake.snowpark.types import StringType, StructType, StructField,DecimalType
import snowflake.snowpark
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz

def main(session: snowpark.Session,Param):
    #Param=["20240422_product_lists_products.csv","THASDL_RAW.DEV_LOAD_STAGE_ADLS","dev/pop6","sdl_pop6_th_product_lists_products"]
    try:
        file_name = Param[0]
        stage_name = Param[1]
        temp_stage_path = Param[2]
        target_table = Param[3]

        df_schema = StructType([
                                    StructField("Product_List", StringType(), True),
                                    StructField("Product_List_Code", StringType(), True),
                                    StructField("ProductDB_ID", StringType(), True),
                                    StructField("SKU", StringType(), True),
                                    StructField("MSL_Ranking", StringType(), True),
                                    StructField("Date", StringType(), True)
                                ])
        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "|")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name.split(".")[0])
        
              
        df = df.na.drop("all")
        if df.count() == 0:
            return "No Data in file"

        
        df = df.withColumn("filename", lit(file_name))
        df = df.withColumn("crt_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        df = df.withColumn("run_id", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))

        df1=df
        
        df1=df1.na.fill("",subset=["Product_List", "ProductDB_ID", "SKU", "MSL_Ranking"])

        df1 = df1.withColumn("hash_c",concat(df1.Product_List,df1.ProductDB_ID,df1.SKU,df1.MSL_Ranking,df1.Date))
        df1=df1.withColumn("hash",md5(col("hash_c")))
        df1=df1.na.replace({"":None},["Product_List", "ProductDB_ID", "SKU", "MSL_Ranking"])
        snowdf = df1.select(
                          "Product_List", "Product_List_Code","ProductDB_ID", "SKU", "MSL_Ranking",
                            "Date",
                            "filename",
                            "run_id",
                            "crt_dttm",
                            "hash"
                        )
        
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        
        #move to success
        
        
        snowdf.write.mode("append").saveAsTable(stage_name.split(".")[0] + "." + target_table)
        file_name1 = file_name.split(".")[0] + "_" + datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S") + ".csv"
        snowdf.write.copy_into_location("@" + stage_name + "/" + temp_stage_path + "/processed/success/" + formatted_year + "/" + formatted_month + "/" + file_name1, header=True, OVERWRITE=True)
        
        return "Success"

    except KeyError as key_error:
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message

    except pd.errors.MergeError as merge_error:
        error_message = f"DataFrame merging error: {str(merge_error)}"
        return error_message

    except Exception as e:
        error_message = f"Error: {str(e)}"
        return error_message
        ';

=

CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.NTASDL_RAW.SDL_POP6_TH_POPS_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS 'from snowflake.snowpark.functions import col, lit, date_format, current_timestamp, to_date,md5, year, month, concat, format_number, regexp_replace, to_timestamp, when, trim, upper
from snowflake.snowpark.types import StringType, StructType, StructField,DecimalType
import snowflake.snowpark
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz

def main(session: snowpark.Session,Param):
    #Param=["20240423_pops.csv","THASDL_RAW.DEV_LOAD_STAGE_ADLS","dev/pop6","sdl_pop6_th_pops"]
    try:
        file_name = Param[0]
        stage_name = Param[1]
        temp_stage_path = Param[2]
        target_table = Param[3]

        df_schema = StructType([
                                        StructField("Status", StringType(), True),
                                        StructField("POPDB_ID", StringType(), True),
                                        StructField("POP_Code", StringType(), True),
                                        StructField("POP_Name", StringType(), True),
                                        StructField("Address", StringType(), True),
                                        StructField("Longitude", StringType(), True),
                                        StructField("Latitude", StringType(), True),
                                        StructField("Business_Units_ID", StringType(), True),
                                        StructField("Business_Unit_Name", StringType(), True),
                                        StructField("Country", StringType(), True),
                                        StructField("Channel", StringType(), True),
                                        StructField("Retail_Environment_(PS)", StringType(), True),
                                        StructField("Sales_Group_Name", StringType(), True),
                                        StructField("Store_Type", StringType(), True),
                                        StructField("Sales_Group_Code", StringType(), True),
                                        StructField("Customer", StringType(), True),
                                        StructField("Customer_Grade", StringType(), True),
                                        StructField("External_Store_Code", StringType(), True),
                                        StructField("Territory/Region", StringType(), True)
                                    ])
        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "|")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name.split(".")[0])
        
              
        df = df.na.drop("all")
        if df.count() == 0:
            return "No Data in file"

        
        df = df.withColumn("filename", lit(file_name))
        df = df.withColumn("crt_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        df = df.withColumn("run_id", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))

        df1=df
        concat_columns = [
                        "Status", "POPDB_ID", "POP_Code", "POP_Name", "Address", "Longitude", "Latitude",
                        "Business_Units_ID", "Business_Unit_Name", "Country", "Channel", "Retail_Environment_(PS)",
                        "Sales_Group_Name", "Sales_Group_Code", "Customer", "Customer_Grade",
                        "External_Store_Code", "Territory/Region"
                    ]
        
        df1=df1.na.fill("",subset=concat_columns)


       # Concatenate the columns
        df1 = df1.withColumn("hash_c", concat(*[df1[col] for col in concat_columns]))

        #df1 = df1.withColumn("hash_c",concat(df1.Product_List,df1.ProductDB_ID,df1.SKU,df1.MSL_Ranking,df1.Date))
        df1=df1.withColumn("hash",md5(col("hash_c")))
        df1=df1.na.replace({"":None},concat_columns)
        snowdf = df1.select(
                          "Status", "POPDB_ID", "POP_Code", "POP_Name", "Address", "Longitude", "Latitude",
                            "Business_Units_ID", "Business_Unit_Name", "Country", "Channel", "Retail_Environment_(PS)",
                            "Sales_Group_Name","Customer", "Sales_Group_Code",  "Customer_Grade",
                            "External_Store_Code", "Territory/Region",
                            "filename",
                            "run_id",
                            "crt_dttm",
                            "hash"
                        )
        
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        
        #move to success
        
        snowdf.write.mode("append").saveAsTable(stage_name.split(".")[0] + "." + target_table)
        #snowdf.write.mode("append").saveAsTable(target_table)
        file_name1 = file_name.split(".")[0] + "_" + datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S") + ".csv"
        snowdf.write.copy_into_location("@" + stage_name + "/" + temp_stage_path + "/processed/success/" + formatted_year + "/" + formatted_month + "/" + file_name1, header=True, OVERWRITE=True)
        
        return "Success"

    except KeyError as key_error:
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message

    except pd.errors.MergeError as merge_error:
        error_message = f"DataFrame merging error: {str(merge_error)}"
        return error_message

    except Exception as e:
        error_message = f"Error: {str(e)}"
        return error_message
        ';



CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.NTASDL_RAW.SDL_POP6_TH_USERS_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS 'from snowflake.snowpark.functions import col, lit, date_format, current_timestamp, to_date,md5, year, month, concat, format_number, regexp_replace, to_timestamp, when, trim, upper
from snowflake.snowpark.types import StringType, StructType, StructField,DecimalType
import snowflake.snowpark
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz

def main(session: snowpark.Session,Param):
    #Param=["20240423_users.csv","THASDL_RAW.DEV_LOAD_STAGE_ADLS","dev/pop6","sdl_pop6_th_users"]
    try:
        file_name = Param[0]
        stage_name = Param[1]
        temp_stage_path = Param[2]
        target_table = Param[3]

        df_schema = StructType([
                                StructField("Status", StringType(), True),
                                StructField("UserDB_ID", StringType(), True),
                                StructField("Username", StringType(), True),
                                StructField("First_Name", StringType(), True),
                                StructField("Last_Name", StringType(), True),
                                StructField("Team", StringType(), True),
                                StructField("Superior_Name", StringType(), True),
                                StructField("Authorisation_Group", StringType(), True),
                                StructField("Email_Address", StringType(), True),
                                StructField("Longitude", StringType(), True),
                                StructField("Latitude", StringType(), True),
                                StructField("Business_Units_ID", StringType(), True),
                                StructField("Business_Unit_Name", StringType(), True)
                            ])
        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "|")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name.split(".")[0])
        
              
        df = df.na.drop("all")
        if df.count() == 0:
            return "No Data in file"

        
        df = df.withColumn("filename", lit(file_name))
        df = df.withColumn("crt_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        df = df.withColumn("run_id", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))

        df1=df
        concat_columns =  [
                            "Status", "UserDB_ID", "Username", "First_Name", "Last_Name", "Team",
                            "Superior_Name", "Authorisation_Group", "Email_Address", "Longitude",
                            "Latitude", "Business_Units_ID", "Business_Unit_Name"
                        ]
                                
        df1=df1.na.fill("",subset=concat_columns)


       # Concatenate the columns
        df1 = df1.withColumn("hash_c", concat(*[df1[col] for col in concat_columns]))

        #df1 = df1.withColumn("hash_c",concat(df1.Product_List,df1.ProductDB_ID,df1.SKU,df1.MSL_Ranking,df1.Date))
        df1=df1.withColumn("hash",md5(col("hash_c")))
        df1=df1.na.replace({"":None},concat_columns)
        snowdf = df1.select(
                        "Status", "UserDB_ID", "Username", "First_Name", "Last_Name", "Team",
                            "Superior_Name", "Authorisation_Group", "Email_Address", "Longitude",
                            "Latitude", "Business_Units_ID", "Business_Unit_Name",
                            "filename",
                            "run_id",
                            "crt_dttm",
                            "hash"
                        )
        
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        
        #move to success
        
        snowdf.write.mode("append").saveAsTable(stage_name.split(".")[0] + "." + target_table)
        #snowdf.write.mode("append").saveAsTable(target_table)
        file_name1 = file_name.split(".")[0] + "_" + datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S") + ".csv"
        snowdf.write.copy_into_location("@" + stage_name + "/" + temp_stage_path + "/processed/success/" + formatted_year + "/" + formatted_month + "/" + file_name1, header=True, OVERWRITE=True)
        
        return "Success"

    except KeyError as key_error:
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message

    except pd.errors.MergeError as merge_error:
        error_message = f"DataFrame merging error: {str(merge_error)}"
        return error_message

    except Exception as e:
        error_message = f"Error: {str(e)}"
        return error_message
        ';




CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.NTASDL_RAW.SDL_POP6_TH_PRODUCTS_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS 'from snowflake.snowpark.functions import col, lit, date_format, current_timestamp, to_date,md5, year, month, concat, format_number, regexp_replace, to_timestamp, when, trim, upper
from snowflake.snowpark.types import StringType, StructType, StructField,DecimalType
import snowflake.snowpark
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz

def main(session: snowpark.Session,Param):
    #Param=["20240423_products.csv","THASDL_RAW.DEV_LOAD_STAGE_ADLS","dev/pop6","sdl_pop6_th_products"]
    try:
        file_name = Param[0]
        stage_name = Param[1]
        temp_stage_path = Param[2]
        target_table = Param[3]

        df_schema = StructType([
                                StructField("Status", StringType(), True),
                                StructField("ProductDB_ID", StringType(), True),
                                StructField("Barcode", StringType(), True),
                                StructField("SKU", StringType(), True),
                                StructField("Unit_Price", StringType(), True),
                                StructField("Display_order", StringType(), True),
                                StructField("Launch_Date", StringType(), True),
                                StructField("Largest_UOM_Quantity", StringType(), True),
                                StructField("Middle_UOM_Quantity", StringType(), True),
                                StructField("Smallest_UOM_Quantity", StringType(), True),
                                StructField("SKU_(English)", StringType(), True),
                                StructField("Company", StringType(), True),
                                StructField("SKU_Code", StringType(), True),
                                StructField("PS_Category", StringType(), True),
                                StructField("PS_Segment", StringType(), True),
                                StructField("PS_Category_Segment", StringType(), True),
                                StructField("Country_L1", StringType(), True),
                                StructField("Regional_Franchise_L2", StringType(), True),
                                StructField("Franchise_L3", StringType(), True),
                                StructField("Brand_L4", StringType(), True),
                                StructField("Sub_Category_L5", StringType(), True),
                                StructField("Platform_L6", StringType(), True),
                                StructField("Variance_L7", StringType(), True),
                                StructField("Pack_Size_L8", StringType(), True)
                            ])
        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "|")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name.split(".")[0])
        
              
        df = df.na.drop("all")
        if df.count() == 0:
            return "No Data in file"

        
        df = df.withColumn("filename", lit(file_name))
        df = df.withColumn("crt_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        df = df.withColumn("run_id", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))

        df1=df
        concat_columns =  [
                        "Status", "ProductDB_ID", "Barcode", "SKU", "Unit_Price", "Display_order",
                        "Launch_Date", "Largest_UOM_Quantity", "Middle_UOM_Quantity", "Smallest_UOM_Quantity",
                        "SKU_(English)", "Company", "SKU_Code", "PS_Category", "PS_Segment", "PS_Category_Segment",
                        "Country_L1", "Regional_Franchise_L2", "Franchise_L3", "Brand_L4", "Sub_Category_L5",
                        "Platform_L6", "Variance_L7", "Pack_Size_L8"
                        ]
        
        df1=df1.na.fill("",subset=concat_columns)


       # Concatenate the columns
        df1 = df1.withColumn("hash_c", concat(*[df1[col] for col in concat_columns]))

        #df1 = df1.withColumn("hash_c",concat(df1.Product_List,df1.ProductDB_ID,df1.SKU,df1.MSL_Ranking,df1.Date))
        df1=df1.withColumn("hash",md5(col("hash_c")))
        df1=df1.na.replace({"":None},concat_columns)
        snowdf = df1.select(
                        "Status", "ProductDB_ID", "Barcode", "SKU", "Unit_Price", "Display_order",
                        "Launch_Date", "Largest_UOM_Quantity", "Middle_UOM_Quantity", "Smallest_UOM_Quantity",
                        "SKU_(English)", "Company", "SKU_Code", "PS_Category", "PS_Segment", "PS_Category_Segment",
                        "Country_L1", "Regional_Franchise_L2", "Franchise_L3", "Brand_L4", "Sub_Category_L5",
                        "Platform_L6", "Variance_L7", "Pack_Size_L8",
                            "filename",
                            "run_id",
                            "crt_dttm",
                            "hash"
                        )
        
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        
        #move to success
        
        snowdf.write.mode("append").saveAsTable(stage_name.split(".")[0] + "." + target_table)
        #snowdf.write.mode("append").saveAsTable(target_table)
        file_name1 = file_name.split(".")[0] + "_" + datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S") + ".csv"
        snowdf.write.copy_into_location("@" + stage_name + "/" + temp_stage_path + "/processed/success/" + formatted_year + "/" + formatted_month + "/" + file_name1, header=True, OVERWRITE=True)
        
        return "Success"

    except KeyError as key_error:
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message

    except pd.errors.MergeError as merge_error:
        error_message = f"DataFrame merging error: {str(merge_error)}"
        return error_message

    except Exception as e:
        error_message = f"Error: {str(e)}"
        return error_message
        ';


CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.JPNSDL_RAW.POP6_JP_MASTER_USERS_PREPROCESSING("PARAM" ARRAY)
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
    # Param=["20240617_users.csv","JPNSDL_RAW.DEV_LOAD_STAGE_ADLS","dev/pop6/master/masterdata/users/","sdl_pop6_jp_users"]
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        
        df_schema = StructType([
                StructField("Status", StringType(), True),
                StructField("USERDB_ID", StringType(), True),
                StructField("Username", StringType(), True),
                StructField("FIRST_NAME", StringType(), True),
                StructField("Last_Name", StringType(), True),
                StructField("Team", StringType(), True),
                StructField("Superior_Name", StringType(), True),
                StructField("Authorisation_Group", StringType(), True),
                StructField("Email_Address", StringType(), True),
                StructField("Longitude", StringType(), True),
                StructField("Latitude", StringType(), True),
                StructField("Business_Units_ID", StringType(), True),
                StructField("Business_Unit_Name", StringType(), True)
                
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
            
            

        df = df.with_column(''hashkey'',md5(concat(when(is_null(col(''status'')),lit('''')).otherwise(col(''status'')),\\
                                            when(is_null(col(''USERDB_ID'')),lit('''')).otherwise(col(''USERDB_ID'')),\\
                                          when(is_null(col(''Username'')),lit('''')).otherwise(col(''Username'')),\\
                                          when(is_null(col(''FIRST_NAME'')),lit('''')).otherwise(col(''FIRST_NAME'')),\\
                                         when(is_null(col(''Last_Name'')),lit('''')).otherwise(col(''Last_Name'')),\\
                                         when(is_null(col(''Team'')),lit('''')).otherwise(col(''Team'')),\\
                                         when(is_null(col(''Superior_Name'')),lit('''')).otherwise(col(''Superior_Name'')),\\
                                          when(is_null(col(''Authorisation_Group'')),lit('''')).otherwise(col(''Authorisation_Group'')),\\
                                         when(is_null(col(''Email_Address'')),lit('''')).otherwise(col(''Email_Address'')),\\
                                         when(is_null(col(''Longitude'')),lit('''')).otherwise(col(''Longitude'')),\\
                                         when(is_null(col(''Latitude'')),lit('''')).otherwise(col(''Latitude'')),\\
                                         when(is_null(col(''Business_Units_ID'')),lit('''')).otherwise(col(''Business_Units_ID'')),
                                         when(is_null(col(''Business_Unit_Name'')),lit('''')).otherwise(col(''Business_Unit_Name'')))))

        
        df = df.with_column(''run_id'',lit(generunid()))

        df = df.with_column(''crtd_dttm'',lit(generatecrtd_dttm()))
        

        

        
        snowdf= df.select(
                    "status",
                    "userdb_id",
                    "username",
                    "first_name",
                    "last_name",
                    "team",
                    "superior_name",
                    "authorisation_group",
                    "email_address",
                    "longitude",
                    "latitude",
                     "business_units_id",
                    "business_unit_name",
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



CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.JPNSDL_RAW.POP6_JP_MASTER_POPS_PREPROCESSING("PARAM" ARRAY)
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
    # Param=["20240617_pops.csv","JPNSDL_RAW.DEV_LOAD_STAGE_ADLS","dev/pop6/master/masterdata/pops/","sdl_pop6_jp_pops"]
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        
        df_schema = StructType([
                StructField("Status", StringType(), True),
                StructField("popdb_id", StringType(), True),
                StructField("pop_code", StringType(), True),
                StructField("pop_name", StringType(), True),
                StructField("address", StringType(), True),
                StructField("longitude", StringType(), True),
                StructField("latitude", StringType(), True),
                StructField("Business_Units_ID", StringType(), True),
                StructField("Business_Unit_Name", StringType(), True),
                StructField("country", StringType(), True),
                StructField("channel", StringType(), True),
                StructField("retail_environment_ps", StringType(), True),
                StructField("sales_group_name", StringType(), True),
                StructField("customer", StringType(), True),
               StructField("Store_Type", StringType(), True),
                StructField("sales_group_code", StringType(), True),
                StructField("Customer_Grade", StringType(), True),
                StructField("territory", StringType(), True),
                
        
                
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
            
            

        df = df.with_column(''hashkey'',md5(concat(when(is_null(col(''status'')),lit('''')).otherwise(col(''status'')),\\
                                            when(is_null(col(''popdb_id'')),lit('''')).otherwise(col(''popdb_id'')),\\
                                          when(is_null(col(''pop_code'')),lit('''')).otherwise(col(''pop_code'')),\\
                                          when(is_null(col(''pop_name'')),lit('''')).otherwise(col(''pop_name'')),\\
                                         when(is_null(col(''address'')),lit('''')).otherwise(col(''address'')),\\
                                         when(is_null(col(''longitude'')),lit('''')).otherwise(col(''longitude'')),\\
                                         when(is_null(col(''latitude'')),lit('''')).otherwise(col(''latitude'')),\\
                                          when(is_null(col(''Business_Units_ID'')),lit('''')).otherwise(col(''Business_Units_ID'')),\\
                                         when(is_null(col(''Business_Unit_Name'')),lit('''')).otherwise(col(''Business_Unit_Name'')),\\
                                         when(is_null(col(''country'')),lit('''')).otherwise(col(''country'')),\\
                                         when(is_null(col(''channel'')),lit('''')).otherwise(col(''channel'')),\\
                                         when(is_null(col(''retail_environment_ps'')),lit('''')).otherwise(col(''retail_environment_ps'')),\\
                                         when(is_null(col(''sales_group_code'')),lit('''')).otherwise(col(''sales_group_code'')),\\
                                        when(is_null(col(''customer'')),lit('''')).otherwise(col(''customer'')),\\
                                          when(is_null(col(''sales_group_name'')),lit('''')).otherwise(col(''sales_group_name'')),
                                                when(is_null(col(''customer_grade'')),lit('''')).otherwise(col(''customer_grade'')),\\
                                                 when(is_null(col(''Territory'')),lit('''')).otherwise(col(''Territory'')))))
                                         
        
        df = df.with_column(''run_id'',lit(generunid()))

        df = df.with_column(''crtd_dttm'',lit(generatecrtd_dttm()))
        

        

        
        snowdf= df.select(
                    "Status",
                "popdb_id",
                "pop_code",
                "pop_name",
                "address",
                "longitude",
                "latitude",
                "Business_Units_ID",
                "Business_Unit_Name",
                "country",
                "channel",
                "retail_environment_ps",
                "sales_group_name",
                "customer",
                "sales_group_code",
                "customer_grade",
                "Territory",
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
=


CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.JPNSDL_RAW.POP6_JP_MASTER_PRODUCT_LISTS_PRODUCTS_PREPROCESSING("PARAM" ARRAY)
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
                    "Product_List_Code",
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

==

CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.JPNSDL_RAW.POP6_JP_MASTER_POP_LISTS_PREPROCESSING("PARAM" ARRAY)
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
    # Param=["20240617_pop_lists.csv","JPNSDL_RAW.DEV_LOAD_STAGE_ADLS","dev/pop6/master/listdata/pop_lists","sdl_pop6_jp_pop_lists"]
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        
        df_schema = StructType([
                StructField("Status", StringType(), True),
                StructField("pop_list", StringType(), True),
                StructField("popdb_id", StringType(), True),
                StructField("pop_code", StringType(), True),
                StructField("pop_name", StringType(), True),
                StructField("Date", StringType(), True)
                
                
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
            
            

        df = df.with_column(''hashkey'',md5(concat(when(is_null(col(''status'')),lit('''')).otherwise(col(''status'')),\\
                                            when(is_null(col(''pop_list'')),lit('''')).otherwise(col(''pop_list'')),\\
                                          when(is_null(col(''popdb_id'')),lit('''')).otherwise(col(''popdb_id'')),\\
                                          when(is_null(col(''pop_code'')),lit('''')).otherwise(col(''pop_code'')),\\
                                         when(is_null(col(''pop_name'')),lit('''')).otherwise(col(''pop_name'')),\\
                                         when(is_null(col(''date'')),lit('''')).otherwise(col(''date'')))))

        
        df = df.with_column(''run_id'',lit(generunid()))

        df = df.with_column(''crtd_dttm'',lit(generatecrtd_dttm()))
        

        

        
        snowdf= df.select(
                    "status",
                    "pop_list",
                    "popdb_id",
                    "pop_code",
                    "pop_name",
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




CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.JPNSDL_RAW.POP6_JP_MASTER_PRODUCT_LISTS_PRODUCTS_PREPROCESSING("PARAM" ARRAY)
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
                    "Product_List_Code",
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

==
CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.JPNSDL_RAW.POP6_JP_MASTER_PRODUCT_LISTS_POPS_PREPROCESSING("PARAM" ARRAY)
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
    # Param=["20240617_product_lists_pops.csv","JPNSDL_RAW.DEV_LOAD_STAGE_ADLS","dev/pop6/master/listdata/product_lists_pops","sdl_pop6_jp_product_lists_pops"]
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        
        df_schema = StructType([
                StructField("Product_List", StringType(), True),
                StructField("Product_List_Code", StringType(), True),
                StructField("POPDB_ID", StringType(), True),
                StructField("POP_Code", StringType(), True),
                StructField("POP_Name", StringType(), True),
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
            
            

        df = df.with_column(''hashkey'',md5(concat(when(is_null(col(''product_list'')),lit('''')).otherwise(col(''product_list'')),\\
                                            when(is_null(col(''Product_List_Code'')),lit('''')).otherwise(col(''Product_List_Code'')),\\
                                          when(is_null(col(''POPDB_ID'')),lit('''')).otherwise(col(''POPDB_ID'')),\\
                                          when(is_null(col(''POP_Code'')),lit('''')).otherwise(col(''POP_Code'')),\\
                                         when(is_null(col(''POP_Name'')),lit('''')).otherwise(col(''POP_Name'')),\\
                                            when(is_null(col(''date'')),lit('''')).otherwise(col(''date'')))))

        
        df = df.with_column(''run_id'',lit(generunid()))

        df = df.with_column(''crtd_dttm'',lit(generatecrtd_dttm()))
        

        

        
        snowdf= df.select(
                    "product_list",
                    "popdb_id",
                    "pop_code",
                    "pop_name",
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



CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.JPNSDL_RAW.POP6_JP_TRANSACTION_DISPLAY_PLAN_PREPROCESSING("PARAM" ARRAY)
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
    # Param=["20240617_display_plans.csv","JPNSDL_RAW.DEV_LOAD_STAGE_ADLS","dev/pop6/transaction/displaydata/display_plans/","SDL_POP6_JP_DISPLAY_PLANS"]
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        
        
        df = session.read\\
            .option("INFER_SCHEMA",True)\\
            .option("PARSE_HEADER",True)\\
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
            
            

       
        
        df = df.with_column(''run_id'',lit(generunid()))

        df = df.with_column(''crtd_dttm'',lit(generatecrtd_dttm()))
        

        

        
        
        file_name=file_name.split(".")[0]+"_"+datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")

        df.write.mode("append").saveAsTable(stage_name.split(".")[0] + "." + target_table)
        
        # #move to success
        df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)
        
        
        
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



CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.JPNSDL_RAW.POP6_JP_TRANSACTION_GENERAL_AUDITS_PREPROCESSING("PARAM" ARRAY)
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
    # Param=["20240617_general_audits.csv","JPNSDL_RAW.DEV_LOAD_STAGE_ADLS","dev/pop6/transaction/auditdata/general_audit","SDL_POP6_JP_GENERAL_AUDITS"]
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        
        
        df = session.read\\
            .option("INFER_SCHEMA",True)\\
            .option("field_delimiter", ",")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .option("encoding",''UTF-8'')\\
            .with_metadata("METADATA$FILE_ROW_NUMBER") \\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        df = df.filter(col("METADATA$FILE_ROW_NUMBER") > lit(1))
        
        df = df.drop("METADATA$FILE_ROW_NUMBER")

       
        
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
            
            

       
        
        df = df.with_column(''run_id'',lit(generunid()))

        df = df.with_column(''crtd_dttm'',lit(generatecrtd_dttm()))
        

        

        
        
        file_name=file_name.split(".")[0]+"_"+datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")

        df.write.mode("append").saveAsTable(stage_name.split(".")[0] + "." + target_table)
        
        # #move to success
        df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)
        
        
        
        return df

    
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



CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.JPNSDL_RAW.POP6_JP_TRANSACTION_PRODUCT_ATTRIBUTE_AUDITS_PREPROCESSING("PARAM" ARRAY)
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
    # Param=["20240617_product_attribute_audits.csv","JPNSDL_RAW.DEV_LOAD_STAGE_ADLS","dev/pop6/transaction/auditdata/product_attribute_audits","SDL_POP6_JP_PRODUCT_ATTRIBUTE_AUDITS"]
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        
        
        df = session.read\\
            .option("INFER_SCHEMA",True)\\
            .option("field_delimiter", ",")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .option("encoding",''UTF-8'')\\
            .with_metadata("METADATA$FILE_ROW_NUMBER") \\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        df = df.filter(col("METADATA$FILE_ROW_NUMBER") > lit(1))
        
        df = df.drop("METADATA$FILE_ROW_NUMBER")
       
        
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
            
            

       
        
        df = df.with_column(''run_id'',lit(generunid()))

        df = df.with_column(''crtd_dttm'',lit(generatecrtd_dttm()))
        

        

        
        
        file_name=file_name.split(".")[0]+"_"+datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")

        df.write.mode("append").saveAsTable(stage_name.split(".")[0] + "." + target_table)
        
        # #move to success
        df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)
        
        
        
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

=

CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.JPNSDL_RAW.POP6_JP_TRANSACTION_SKU_AUDITS_PREPROCESSING("PARAM" ARRAY)
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
    # Param=["20240617_display_plans.csv","JPNSDL_RAW.DEV_LOAD_STAGE_ADLS","dev/pop6/transaction/displaydata/display_plans/","SDL_POP6_JP_DISPLAY_PLANS"]
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        
        
        df = session.read\\
            .option("INFER_SCHEMA",True)\\
            .option("field_delimiter", ",")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .option("encoding",''UTF-8'')\\
            .with_metadata("METADATA$FILE_ROW_NUMBER") \\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        df = df.filter(col("METADATA$FILE_ROW_NUMBER") > lit(1))
        
        df = df.drop("METADATA$FILE_ROW_NUMBER")

       
        
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
            
            

       
        
        df = df.with_column(''run_id'',lit(generunid()))

        df = df.with_column(''crtd_dttm'',lit(generatecrtd_dttm()))
        

        

        
        
        file_name=file_name.split(".")[0]+"_"+datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")

        df.write.mode("append").saveAsTable(stage_name.split(".")[0] + "." + target_table)
        
        # #move to success
        df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)
        
        
        
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




CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.JPNSDL_RAW.POP6_JP_TRANSACTION_EXCLUSIONDATA_PREPROCESSING("PARAM" ARRAY)
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
    # Param=["20210223_pop6_jp_data_exclusion.csv","JPNSDL_RAW.DEV_LOAD_STAGE_ADLS","dev/pop6/transaction/exclusiondata/","SDL_POP6_JP_EXCLUSIONDATA"]
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        
        
        df = session.read\\
            .option("INFER_SCHEMA",True)\\
            .option("field_delimiter", ",")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .option("encoding",''UTF-8'')\\
            .with_metadata("METADATA$FILE_ROW_NUMBER") \\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        df = df.filter(col("METADATA$FILE_ROW_NUMBER") > lit(1))
        
        df = df.drop("METADATA$FILE_ROW_NUMBER")

       
        
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
            
            

       
        
        df = df.with_column(''run_id'',lit(generunid()))

        df = df.with_column(''crtd_dttm'',lit(generatecrtd_dttm()))
        

        

        
        
        file_name=file_name.split(".")[0]+"_"+datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")

        df.write.mode("append").saveAsTable(stage_name.split(".")[0] + "." + target_table)
        
        # #move to success
        df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)
        
        
        
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

==

CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.JPNSDL_RAW.POP6_JP_TRANSACTION_DISPLAY_PLANS_PREPROCESSING("PARAM" ARRAY)
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
    # Param=["20240617_display_plans.csv","JPNSDL_RAW.DEV_LOAD_STAGE_ADLS","dev/pop6/transaction/displaydata/display_plans/","SDL_POP6_JP_DISPLAY_PLANS"]
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        
        
        df = session.read\\
            .option("INFER_SCHEMA",True)\\
            .option("field_delimiter", ",")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .option("encoding",''UTF-8'')\\
            .with_metadata("METADATA$FILE_ROW_NUMBER") \\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        df = df.filter(col("METADATA$FILE_ROW_NUMBER") > lit(1))
        
        df = df.drop("METADATA$FILE_ROW_NUMBER")

       
        
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
            
            

       
        
        df = df.with_column(''run_id'',lit(generunid()))

        df = df.with_column(''crtd_dttm'',lit(generatecrtd_dttm()))
        

        

        
        
        file_name=file_name.split(".")[0]+"_"+datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")

        df.write.mode("append").saveAsTable(stage_name.split(".")[0] + "." + target_table)
        
        # #move to success
        df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)
        
        
        
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




CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.JPNSDL_RAW.POP6_JP_TRANSACTION_DISPLAY_PREPROCESSING("PARAM" ARRAY)
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
    # Param=["20240617_displays.csv","JPNSDL_RAW.DEV_LOAD_STAGE_ADLS","dev/pop6/transaction/displaydata/displays/","SDL_POP6_JP_DISPLAYS"]
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        
        
        df = session.read\\
            .option("INFER_SCHEMA",True)\\
            .option("field_delimiter", ",")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .option("encoding",''UTF-8'')\\
            .with_metadata("METADATA$FILE_ROW_NUMBER") \\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        df = df.filter(col("METADATA$FILE_ROW_NUMBER") > lit(1))
        
        df = df.drop("METADATA$FILE_ROW_NUMBER")

       
        
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
            
            

       
        
        df = df.with_column(''run_id'',lit(generunid()))

        df = df.with_column(''crtd_dttm'',lit(generatecrtd_dttm()))
        

        

        
        
        file_name=file_name.split(".")[0]+"_"+datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")

        df.write.mode("append").saveAsTable(stage_name.split(".")[0] + "." + target_table)
        
        # #move to success
        df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)
        
        
        
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





CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.JPNSDL_RAW.POP6_JP_TRANSACTION_PROMOTIONS_PREPROCESSING("PARAM" ARRAY)
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
    # Param=["20240617_promotions.csv","JPNSDL_RAW.DEV_LOAD_STAGE_ADLS","dev/pop6/transaction/promotiondata/promotion/","SDL_POP6_JP_PROMOTIONS"]
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        
        
        df = session.read\\
            .option("INFER_SCHEMA",True)\\
            .option("field_delimiter", ",")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .option("encoding",''UTF-8'')\\
            .with_metadata("METADATA$FILE_ROW_NUMBER") \\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        df = df.filter(col("METADATA$FILE_ROW_NUMBER") > lit(1))
        
        df = df.drop("METADATA$FILE_ROW_NUMBER")

       
        
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
            
            

       
        
        df = df.with_column(''run_id'',lit(generunid()))

        df = df.with_column(''crtd_dttm'',lit(generatecrtd_dttm()))
        

        

        
        
        file_name=file_name.split(".")[0]+"_"+datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")

        df.write.mode("append").saveAsTable(stage_name.split(".")[0] + "." + target_table)
        
        # #move to success
        df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)
        
        
        
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





CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.JPNSDL_RAW.POP6_JP_TRANSACTION_PROMOTIONS_PLANS_PREPROCESSING("PARAM" ARRAY)
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
    # Param=["20240617_promotion_plans.csv","JPNSDL_RAW.DEV_LOAD_STAGE_ADLS","dev/pop6/transaction/promotiondata/promotion_plans/","SDL_POP6_JP_PROMOTIONS_PLANS"]
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        
        
        df = session.read\\
            .option("INFER_SCHEMA",True)\\
            .option("field_delimiter", ",")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .option("encoding",''UTF-8'')\\
            .with_metadata("METADATA$FILE_ROW_NUMBER") \\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        df = df.filter(col("METADATA$FILE_ROW_NUMBER") > lit(1))
        
        df = df.drop("METADATA$FILE_ROW_NUMBER")

       
        
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
            
            

       
        
        df = df.with_column(''run_id'',lit(generunid()))

        df = df.with_column(''crtd_dttm'',lit(generatecrtd_dttm()))
        

        

        
        
        file_name=file_name.split(".")[0]+"_"+datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")

        df.write.mode("append").saveAsTable(stage_name.split(".")[0] + "." + target_table)
        
        # #move to success
        df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)
        
        
        
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





CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.JPNSDL_RAW.POP6_JP_TRANSACTION_TASKS_PREPROCESSING("PARAM" ARRAY)
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
    # Param=["20240617_tasks.csv","JPNSDL_RAW.DEV_LOAD_STAGE_ADLS","dev/pop6/transaction/taskdata/tasks","SDL_POP6_JP_TASKS"]
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        
        
        df = session.read\\
            .option("INFER_SCHEMA",True)\\
            .option("field_delimiter", ",")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .option("encoding",''UTF-8'')\\
            .with_metadata("METADATA$FILE_ROW_NUMBER") \\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        df = df.filter(col("METADATA$FILE_ROW_NUMBER") > lit(1))
        
        df = df.drop("METADATA$FILE_ROW_NUMBER")

       
        
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
            
            

       
        
        df = df.with_column(''run_id'',lit(generunid()))

        df = df.with_column(''crtd_dttm'',lit(generatecrtd_dttm()))
        

        

        
        
        file_name=file_name.split(".")[0]+"_"+datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")

        df.write.mode("append").saveAsTable(stage_name.split(".")[0] + "." + target_table)
        
        # #move to success
        df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)
        
        
        
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




CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.JPNSDL_RAW.POP6_JP_TRANSACTION_EXECUTE_VISITS_PREPROCESSING("PARAM" ARRAY)
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
    # Param=["20240617_executed_visits.csv","JPNSDL_RAW.DEV_LOAD_STAGE_ADLS","dev/pop6/transaction/visitdata/executed_visits/","SDL_POP6_JP_EXECUTE_VISIT"]
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        
        
        df = session.read\\
            .option("INFER_SCHEMA",True)\\
            .option("field_delimiter", ",")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .option("encoding",''UTF-8'')\\
            .with_metadata("METADATA$FILE_ROW_NUMBER") \\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        df = df.filter(col("METADATA$FILE_ROW_NUMBER") > lit(1))
        
        df = df.drop("METADATA$FILE_ROW_NUMBER")

       
        
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
            
            

       
        
        df = df.with_column(''run_id'',lit(generunid()))

        df = df.with_column(''crtd_dttm'',lit(generatecrtd_dttm()))
        

        

        
        
        file_name=file_name.split(".")[0]+"_"+datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")

        df.write.mode("append").saveAsTable(stage_name.split(".")[0] + "." + target_table)
        
        # #move to success
        df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)
        
        
        
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




CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.JPNSDL_RAW.POP6_JP_TRANSACTION_PLANNED_VISITS_PREPROCESSING("PARAM" ARRAY)
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
    # Param=["20240617_planned_visits.csv","JPNSDL_RAW.DEV_LOAD_STAGE_ADLS","dev/pop6/transaction/visitdata/planned_visits/","SDL_POP6_JP_PLANNED_VISITS"]
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        
        
        df = session.read\\
            .option("INFER_SCHEMA",True)\\
            .option("field_delimiter", ",")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .option("encoding",''UTF-8'')\\
            .with_metadata("METADATA$FILE_ROW_NUMBER") \\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        df = df.filter(col("METADATA$FILE_ROW_NUMBER") > lit(1))
        
        df = df.drop("METADATA$FILE_ROW_NUMBER")


       
        
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
            
            

       
        
        df = df.with_column(''run_id'',lit(generunid()))

        df = df.with_column(''crtd_dttm'',lit(generatecrtd_dttm()))
        

        

        
        
        file_name=file_name.split(".")[0]+"_"+datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")

        df.write.mode("append").saveAsTable(stage_name.split(".")[0] + "." + target_table)
        
        # #move to success
        df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)
        
        
        
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




CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.JPNSDL_RAW.POP6_JP_TRANSACTION_SERVICE_LEVELS_PREPROCESSING("PARAM" ARRAY)
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
    # Param=["20240617_service_levels.csv","JPNSDL_RAW.DEV_LOAD_STAGE_ADLS","dev/pop6/transaction/visitdata/service_levels/","SDL_POP6_JP_SERVICE_LEVELS"]
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        
        
        df = session.read\\
            .option("INFER_SCHEMA",True)\\
            .option("field_delimiter", ",")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .option("encoding",''UTF-8'')\\
            .with_metadata("METADATA$FILE_ROW_NUMBER") \\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        df = df.filter(col("METADATA$FILE_ROW_NUMBER") > lit(1))
        
        df = df.drop("METADATA$FILE_ROW_NUMBER")

       
        
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
            
            

       
        
        df = df.with_column(''run_id'',lit(generunid()))

        df = df.with_column(''crtd_dttm'',lit(generatecrtd_dttm()))
        

        

        
        
        file_name=file_name.split(".")[0]+"_"+datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")

        df.write.mode("append").saveAsTable(stage_name.split(".")[0] + "." + target_table)
        
        # #move to success
        df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)
        
        
        
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

-
CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.JPNSDL_RAW.POP6_JP_MASTER_PRODUCT_LISTS_ALLOCATION_PREPROCESSING("PARAM" ARRAY)
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
                    "Product_List_code",
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

==

CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.JPNSDL_RAW.POP6_JP_MASTER_PRODUCTS_PREPROCESSING("PARAM" ARRAY)
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
    # Param=["20240617_products.csv","JPNSDL_RAW.DEV_LOAD_STAGE_ADLS","dev/pop6/master/masterdata/products/","sdl_pop6_jp_products"]
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        
        df_schema = StructType([
                StructField("Status", StringType(), True),
                StructField("productdb_id", StringType(), True),
                StructField("barcode", StringType(), True),
                StructField("sku", StringType(), True),
                StructField("unit_price", StringType(), True),
                StructField("display_order", StringType(), True),
                StructField("launch_date", StringType(), True),
                StructField("largest_uom_quantity", StringType(), True),
                StructField("middle_uom_quantity", StringType(), True),
                StructField("smallest_uom_quantity", StringType(), True),
                StructField("sku_english", StringType(), True),
                StructField("company", StringType(), True),
                StructField("sku_code", StringType(), True),
                StructField("ps_category", StringType(), True),
                StructField("ps_segment", StringType(), True),
                StructField("ps_category_segment", StringType(), True),
                StructField("country_l1", StringType(), True),
                StructField("regional_franchise_l2", StringType(), True),
                StructField("franchise_l3", StringType(), True),
                StructField("brand_l4", StringType(), True),
                StructField("sub_category_l5", StringType(), True),
                StructField("platform_level_l6", StringType(), True),
                StructField("variance_l7", StringType(), True),
                StructField("pack_size_l8", StringType(), True)
                
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
            
            

        df = df.with_column(''hashkey'',md5(concat(when(is_null(col(''status'')),lit('''')).otherwise(col(''status'')),\\
                                            when(is_null(col(''productdb_id'')),lit('''')).otherwise(col(''productdb_id'')),\\
                                          when(is_null(col(''barcode'')),lit('''')).otherwise(col(''barcode'')),\\
                                          when(is_null(col(''sku'')),lit('''')).otherwise(col(''sku'')),\\
                                         when(is_null(col(''unit_price'')),lit('''')).otherwise(col(''unit_price'')),\\
                                         when(is_null(col(''display_order'')),lit('''')).otherwise(col(''display_order'')),\\
                                         when(is_null(col(''launch_date'')),lit('''')).otherwise(col(''launch_date'')),\\
                                          when(is_null(col(''largest_uom_quantity'')),lit('''')).otherwise(col(''largest_uom_quantity'')),\\
                                         when(is_null(col(''middle_uom_quantity'')),lit('''')).otherwise(col(''middle_uom_quantity'')),\\
                                         when(is_null(col(''smallest_uom_quantity'')),lit('''')).otherwise(col(''smallest_uom_quantity'')),\\
                                         when(is_null(col(''company'')),lit('''')).otherwise(col(''company'')),\\
                                         when(is_null(col(''sku_english'')),lit('''')).otherwise(col(''sku_english'')),\\
                                         when(is_null(col(''sku_code'')),lit('''')).otherwise(col(''sku_code'')),\\
                                        when(is_null(col(''ps_category'')),lit('''')).otherwise(col(''ps_category'')),\\
                                          when(is_null(col(''ps_segment'')),lit('''')).otherwise(col(''ps_segment'')),\\
                                         when(is_null(col(''ps_category_segment'')),lit('''')).otherwise(col(''ps_category_segment'')),\\
                                         when(is_null(col(''country_l1'')),lit('''')).otherwise(col(''country_l1'')),\\
                                         when(is_null(col(''regional_franchise_l2'')),lit('''')).otherwise(col(''regional_franchise_l2'')),\\
                                          when(is_null(col(''franchise_l3'')),lit('''')).otherwise(col(''franchise_l3'')),\\
                                         when(is_null(col(''brand_l4'')),lit('''')).otherwise(col(''brand_l4'')),\\
                                         when(is_null(col(''sub_category_l5'')),lit('''')).otherwise(col(''sub_category_l5'')),\\
                                         when(is_null(col(''platform_level_l6'')),lit('''')).otherwise(col(''platform_level_l6'')),\\
                                         when(is_null(col(''variance_l7'')),lit('''')).otherwise(col(''variance_l7'')),\\
                                         when(is_null(col(''pack_size_l8'')),lit('''')).otherwise(col(''pack_size_l8'')))))

        
        df = df.with_column(''run_id'',lit(generunid()))

        df = df.with_column(''crtd_dttm'',lit(generatecrtd_dttm()))
        

        

        
        snowdf= df.select(
                   "Status", 
                "productdb_id", 
                "barcode", 
                "sku", 
                "unit_price", 
                "display_order", 
                "launch_date", 
                "largest_uom_quantity", 
                "middle_uom_quantity", 
                "smallest_uom_quantity", 
                "company", 
                "sku_english", 
                "sku_code", 
                "ps_category", 
                "ps_segment", 
                "ps_category_segment", 
                "country_l1", 
                "regional_franchise_l2", 
                "franchise_l3", 
                "brand_l4", 
                "sub_category_l5", 
                "platform_level_l6", 
                "variance_l7", 
                "pack_size_l8",
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




CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.JPNSDL_RAW.POP6_JP_MASTER_PRODUCT_LISTS_POPS_PREPROCESSING("PARAM" ARRAY)
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
    # Param=["20240617_product_lists_pops.csv","JPNSDL_RAW.DEV_LOAD_STAGE_ADLS","dev/pop6/master/listdata/product_lists_pops","sdl_pop6_jp_product_lists_pops"]
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        
        df_schema = StructType([
                StructField("Product_List", StringType(), True),
                StructField("Product_List_Code", StringType(), True),
                StructField("POPDB_ID", StringType(), True),
                StructField("POP_Code", StringType(), True),
                StructField("POP_Name", StringType(), True),
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
            
            

        df = df.with_column(''hashkey'',md5(concat(when(is_null(col(''product_list'')),lit('''')).otherwise(col(''product_list'')),\\
                                            when(is_null(col(''Product_List_Code'')),lit('''')).otherwise(col(''Product_List_Code'')),\\
                                          when(is_null(col(''POPDB_ID'')),lit('''')).otherwise(col(''POPDB_ID'')),\\
                                          when(is_null(col(''POP_Code'')),lit('''')).otherwise(col(''POP_Code'')),\\
                                         when(is_null(col(''POP_Name'')),lit('''')).otherwise(col(''POP_Name'')),\\
                                            when(is_null(col(''date'')),lit('''')).otherwise(col(''date'')))))

        
        df = df.with_column(''run_id'',lit(generunid()))

        df = df.with_column(''crtd_dttm'',lit(generatecrtd_dttm()))
        

        

        
        snowdf= df.select(
                    "product_list",
                    "popdb_id",
                    "pop_code",
                    "pop_name",
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

==

CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.SGPSDL_RAW.POP6_SG_TRANS_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them.

import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import lit,col
from snowflake.snowpark.types import  StringType, StructType, StructField,IntegerType
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz
from datetime import datetime, timedelta



def main(session: snowpark.Session,Param): 
       
    try :
           
        #Param=[''20220112_pop6_hk_data_exclusion.csv'',''NTASDL_RAW.DEV_LOAD_STAGE_ADLS_HKG'',''dev/pop6/exclusionsdata/'',''sdl_pop6_hk_exclusion'']

        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        sch_name        = stage_name.split(''.'')[0]
        target_table    = sch_name+"."+Param[3]
        
        df = session.read\\
            .option("INFER_SCHEMA", True)\\
            .option("skip_header",0)\\
            .option("field_delimiter", ",")\\
            .option("field_optionally_enclosed_by", "\\"") \\
            .option("encoding","UTF-8") \\
            .option("trim_space", True) \\
            .with_metadata("METADATA$FILE_ROW_NUMBER") \\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
        
       
        df = df.filter(col("METADATA$FILE_ROW_NUMBER") > lit(1))
        df = df.drop("METADATA$FILE_ROW_NUMBER")
        
        df=df.na.drop("all")
        if df.count()==0:
            return "No Data in file"

       
        
        
     
        df = df.with_column("FILE_NAME",lit( file_name ))
        df = df.with_column("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        df = df.with_column("CRTD_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
       
        
        # Load Data to the target table
        df.write.mode("append").saveAsTable(target_table)
       
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        #move to success
        file_name=file_name.split(".")[0]+''_''+datetime.now().strftime("%Y%m%d%H%M%S")
        df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)

        
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



CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.THASDL_RAW.POP6_TH_TRANS_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them.

import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import lit,col
from snowflake.snowpark.types import  StringType, StructType, StructField,IntegerType
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz
from datetime import datetime, timedelta



def main(session: snowpark.Session,Param): 
       
    try :
           
        #Param=[''20220112_pop6_hk_data_exclusion.csv'',''NTASDL_RAW.DEV_LOAD_STAGE_ADLS_HKG'',''dev/pop6/exclusionsdata/'',''sdl_pop6_hk_exclusion'']

        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        sch_name        = stage_name.split(''.'')[0]
        target_table    = sch_name+"."+Param[3]
        
        df = session.read\\
            .option("INFER_SCHEMA", True)\\
            .option("skip_header",0)\\
            .option("field_delimiter", ",")\\
            .option("field_optionally_enclosed_by", "\\"") \\
            .option("encoding","UTF-8") \\
            .option("trim_space", True) \\
            .with_metadata("METADATA$FILE_ROW_NUMBER") \\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
        
       
        df = df.filter(col("METADATA$FILE_ROW_NUMBER") > lit(1))
        df = df.drop("METADATA$FILE_ROW_NUMBER")
        
        df=df.na.drop("all")
        if df.count()==0:
            return "No Data in file"

       
        
        
     
        df = df.with_column("FILE_NAME",lit( file_name ))
        df = df.with_column("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        df = df.with_column("CRTD_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
       
        
        # Load Data to the target table
        df.write.mode("append").saveAsTable(target_table)
       
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        #move to success
        file_name=file_name.split(".")[0]+''_''+datetime.now().strftime("%Y%m%d%H%M%S")
        df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)

        
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


CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.SGPSDL_RAW.SG_POP6_POPS_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS 'from snowflake.snowpark.functions import col, lit, date_format, current_timestamp, to_date,md5, year, month, concat, format_number, regexp_replace, to_timestamp, when, trim, upper
from snowflake.snowpark.types import StringType, StructType, StructField,DecimalType
import snowflake.snowpark
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz

def main(session: snowpark.Session,Param):
    #Param=["20240620_pops.csv","SGPSDL_RAW.DEV_LOAD_STAGE_ADLS","dev/pop6/master/masterdata","sdl_pop6_sg_pops"]    
    try:
        file_name = Param[0]
        stage_name = Param[1]
        temp_stage_path = Param[2]
        target_table = Param[3]

        df_schema = StructType([
                    StructField("status", StringType(), True),
                    StructField("popdb_id", StringType(), True),
                    StructField("pop_code", StringType(), True),
                    StructField("pop_name", StringType(), True),
                    StructField("address", StringType(), True),
                    StructField("longitude", StringType(), True),
                    StructField("latitude", StringType(), True),
                    StructField("Business_Units_ID", StringType(), True),
                    StructField("Business_Unit_Name", StringType(), True),
                    StructField("country", StringType(), True),
                    StructField("channel", StringType(), True),
                    StructField("retail_environment_ps", StringType(), True),
                    StructField("sales_group_name", StringType(), True),
                    StructField("customer", StringType(), True),
                    StructField("Store_Type", StringType(), True),
                    StructField("sales_group_code", StringType(), True),
                    StructField("customer_grade", StringType(), True),
                    StructField("Territory", StringType(), True)
                ])
        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "|")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name.split(".")[0])
        
              
        df = df.na.drop("all")
        if df.count() == 0:
            return "No Data in file"

        
        df = df.withColumn("filename", lit(file_name))
        df = df.withColumn("crt_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        df = df.withColumn("run_id", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))

        df1=df
        concat_columns = [
                          "status",
                            "popdb_id",
                            "pop_code",
                            "pop_name",
                            "address",
                            "longitude",
                            "latitude",
                            "Business_Units_ID",
                            "Business_Unit_Name",
                            "country",
                            "channel",
                            "retail_environment_ps",
                            "sales_group_code",
                            "customer",
                            "sales_group_name",
                            "customer_grade",
                            "Territory"
                    ]
        
        df1=df1.na.fill("",subset=concat_columns)


       # Concatenate the columns
        df1 = df1.withColumn("hash_c", concat(*[df1[col] for col in concat_columns]))

        #df1 = df1.withColumn("hash_c",concat(df1.Product_List,df1.ProductDB_ID,df1.SKU,df1.MSL_Ranking,df1.Date))
        df1=df1.withColumn("hash",md5(col("hash_c")))
        df1=df1.na.replace({"":None},concat_columns)
        snowdf = df1.select(
                           "status",
                            "popdb_id",
                            "pop_code",
                            "pop_name",
                            "address",
                            "longitude",
                            "latitude",
                            "Business_Units_ID",
                            "Business_Unit_Name",
                            "country",
                            "channel",
                            "retail_environment_ps",
                            "sales_group_name",
                            "customer",
                            "sales_group_code",
                            "customer_grade",
                            "Territory",
                            "filename",
                            "run_id",
                            "crt_dttm",
                            "hash"
                        )
        
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        
        #move to success
        
        snowdf.write.mode("append").saveAsTable(stage_name.split(".")[0] + "." + target_table)
        #snowdf.write.mode("append").saveAsTable(target_table)
        file_name1 = file_name.split(".")[0] + "_" + datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        snowdf.write.copy_into_location("@" + stage_name + "/" + temp_stage_path + "/processed/success/" + formatted_year + "/" + formatted_month + "/" + file_name1, header=True, OVERWRITE=True)
        
        return "Success"

    except KeyError as key_error:
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message

    except pd.errors.MergeError as merge_error:
        error_message = f"DataFrame merging error: {str(merge_error)}"
        return error_message

    except Exception as e:
        error_message = f"Error: {str(e)}"
        return error_message';



CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.SGPSDL_RAW.SG_POP6_PRODUCTS_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS 'from snowflake.snowpark.functions import col, lit, date_format, current_timestamp, to_date,md5, year, month, concat, format_number, regexp_replace, to_timestamp, when, trim, upper
from snowflake.snowpark.types import StringType, StructType, StructField,DecimalType
import snowflake.snowpark
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz

def main(session: snowpark.Session,Param):
    #Param=["20240620_products.csv","SGPSDL_RAW.DEV_LOAD_STAGE_ADLS","dev/pop6/master/masterdata","sdl_pop6_sg_pops"]    
    try:
        file_name = Param[0]
        stage_name = Param[1]
        temp_stage_path = Param[2]
        target_table = Param[3]

        df_schema = StructType([
                    StructField("Status", StringType(), True),
                    StructField("ProductDB_ID", StringType(), True),
                    StructField("Barcode", StringType(), True),
                    StructField("SKU", StringType(), True),
                    StructField("Unit_Price", StringType(), True),
                    StructField("Display_order", StringType(), True),
                    StructField("Launch_Date", StringType(), True),
                    StructField("Largest_UOM_Quantity", StringType(), True),
                    StructField("Middle_UOM_Quantity", StringType(), True),
                    StructField("Smallest_UOM_Quantity", StringType(), True),
                    StructField("SKU_English", StringType(), True),
                    StructField("Company", StringType(), True),
                    StructField("SKU_Code", StringType(), True),
                    StructField("PS_Category", StringType(), True),
                    StructField("PS_Segment", StringType(), True),
                    StructField("PS_Category_Segment", StringType(), True),
                    StructField("Country_L1", StringType(), True),
                    StructField("Regional_Franchise_L2", StringType(), True),
                    StructField("Franchise_L3", StringType(), True),
                    StructField("Brand_L4", StringType(), True),
                    StructField("Sub_Category_L5", StringType(), True),
                    StructField("Platform_L6", StringType(), True),
                    StructField("Variance_L7", StringType(), True),
                    StructField("Pack_size_L8", StringType(), True)
                ])
        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "|")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name.split(".")[0])
        
              
        df = df.na.drop("all")
        if df.count() == 0:
            return "No Data in file"

        
        df = df.withColumn("filename", lit(file_name))
        df = df.withColumn("crt_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        df = df.withColumn("run_id", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))

        df1=df
        concat_columns = [
                           "status",
                        "productdb_id",
                        "barcode",
                        "sku",
                        "unit_price",
                        "display_order",
                        "launch_date",
                        "largest_uom_quantity",
                        "middle_uom_quantity",
                        "smallest_uom_quantity",
                        "company",
                        "sku_english",
                        "sku_code",
                        "ps_category",
                        "ps_segment",
                        "ps_category_segment",
                        "country_l1",
                        "regional_franchise_l2",
                        "franchise_l3",
                        "brand_l4",
                        "sub_category_l5",
                        "platform_l6",
                        "variance_l7",
                        "pack_size_l8"
                    ]
        
        df1=df1.na.fill("",subset=concat_columns)


       # Concatenate the columns
        df1 = df1.withColumn("hash_c", concat(*[df1[col] for col in concat_columns]))

        #df1 = df1.withColumn("hash_c",concat(df1.Product_List,df1.ProductDB_ID,df1.SKU,df1.MSL_Ranking,df1.Date))
        df1=df1.withColumn("hash",md5(col("hash_c")))
        df1=df1.na.replace({"":None},concat_columns)
        snowdf = df1.select(
                            "status",
                        "productdb_id",
                        "barcode",
                        "sku",
                        "unit_price",
                        "display_order",
                        "launch_date",
                        "largest_uom_quantity",
                        "middle_uom_quantity",
                        "smallest_uom_quantity",
                        "company",
                        "sku_english",
                        "sku_code",
                        "ps_category",
                        "ps_segment",
                        "ps_category_segment",
                        "country_l1",
                        "regional_franchise_l2",
                        "franchise_l3",
                        "brand_l4",
                        "sub_category_l5",
                        "platform_l6",
                        "variance_l7",
                            "filename",
                            "run_id",
                            "crt_dttm",
                            "hash"
                        )
        
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        
        #move to success
        
        snowdf.write.mode("append").saveAsTable(stage_name.split(".")[0] + "." + target_table)
        #snowdf.write.mode("append").saveAsTable(target_table)
        file_name1 = file_name.split(".")[0] + "_" + datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        snowdf.write.copy_into_location("@" + stage_name + "/" + temp_stage_path + "/processed/success/" + formatted_year + "/" + formatted_month + "/" + file_name1, header=True, OVERWRITE=True)
        
        return "Success"

    except KeyError as key_error:
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message

    except pd.errors.MergeError as merge_error:
        error_message = f"DataFrame merging error: {str(merge_error)}"
        return error_message

    except Exception as e:
        error_message = f"Error: {str(e)}"
        return error_message';



CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.SGPSDL_RAW.SG_POP6_PRODUCT_LISTS_POPS_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS 'from snowflake.snowpark.functions import col, lit, date_format, current_timestamp, to_date,md5, year, month, concat, format_number, regexp_replace, to_timestamp, when, trim, upper
from snowflake.snowpark.types import StringType, StructType, StructField,DecimalType
import snowflake.snowpark
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz

def main(session: snowpark.Session,Param):
    #Param=["20240620_product_lists_pops.csv","SGPSDL_RAW.DEV_LOAD_STAGE_ADLS","dev/pop6/master/listdata","sdl_pop6_sg_pops"]    
    try:
        file_name = Param[0]
        stage_name = Param[1]
        temp_stage_path = Param[2]
        target_table = Param[3]

        df_schema = StructType([
                    StructField("Product_List", StringType(), True),
                    StructField("Product_List_Code", StringType(), True),
                    StructField("POPDB_ID", StringType(), True),
                    StructField("POP_Code", StringType(), True),
                    StructField("POP_Name", StringType(), True),
                    StructField("Date", StringType(), True)
                ])
        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "|")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name.split(".")[0])
        
              
        df = df.na.drop("all")
        if df.count() == 0:
            return "No Data in file"

        
        df = df.withColumn("filename", lit(file_name))
        df = df.withColumn("crt_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        df = df.withColumn("run_id", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))

        df1=df
        concat_columns = [
                            "Product_List",
                            "POPDB_ID",
                            "POP_Code",
                            "POP_Name",
                            "Date"
                    ]
        
        df1=df1.na.fill("",subset=concat_columns)


       # Concatenate the columns
        df1 = df1.withColumn("hash_c", concat(*[df1[col] for col in concat_columns]))

        #df1 = df1.withColumn("hash_c",concat(df1.Product_List,df1.ProductDB_ID,df1.SKU,df1.MSL_Ranking,df1.Date))
        df1=df1.withColumn("hash",md5(col("hash_c")))
        df1=df1.na.replace({"":None},concat_columns)
        snowdf = df1.select(
                             "Product_List",
                            "POPDB_ID",
                            "POP_Code",
                            "POP_Name",
                            "Date",
                            "filename",
                            "run_id",
                            "crt_dttm",
                            "hash"
                        )
        
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        
        #move to success
        
        snowdf.write.mode("append").saveAsTable(stage_name.split(".")[0] + "." + target_table)
        #snowdf.write.mode("append").saveAsTable(target_table)
        file_name1 = file_name.split(".")[0] + "_" + datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        snowdf.write.copy_into_location("@" + stage_name + "/" + temp_stage_path + "/processed/success/" + formatted_year + "/" + formatted_month + "/" + file_name1, header=True, OVERWRITE=True)
        
        return "Success"

    except KeyError as key_error:
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message

    except pd.errors.MergeError as merge_error:
        error_message = f"DataFrame merging error: {str(merge_error)}"
        return error_message

    except Exception as e:
        error_message = f"Error: {str(e)}"
        return error_message';


CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.SGPSDL_RAW.SG_POP6_USERS_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('openpyxl==3.1.2','snowflake-snowpark-python==*')
HANDLER = 'main'
EXECUTE AS OWNER
AS 'from snowflake.snowpark.functions import col, lit, date_format, current_timestamp, to_date,md5, year, month, concat, format_number, regexp_replace, to_timestamp, when, trim, upper
from snowflake.snowpark.types import StringType, StructType, StructField,DecimalType
import snowflake.snowpark
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz

def main(session: snowpark.Session,Param):
    #Param=["20240620_users.csv","SGPSDL_RAW.DEV_LOAD_STAGE_ADLS","dev/pop6/master/masterdata","sdl_pop6_sg_pops"]    
    try:
        file_name = Param[0]
        stage_name = Param[1]
        temp_stage_path = Param[2]
        target_table = Param[3]

        df_schema = StructType([
                    StructField("Status", StringType(), True),
                    StructField("UserDB_ID", StringType(), True),
                    StructField("Username", StringType(), True),
                    StructField("First_Name", StringType(), True),
                    StructField("Last_Name", StringType(), True),
                    StructField("Team", StringType(), True),
                    StructField("Superior_Name", StringType(), True),
                    StructField("Authorisation_Group", StringType(), True),
                    StructField("Email_Address", StringType(), True),
                    StructField("Longitude", StringType(), True),
                    StructField("Latitude", StringType(), True),
                    StructField("Business_Units_ID", StringType(), True),
                    StructField("Business_Unit_Name", StringType(), True),
                    StructField("Mobile_Number", StringType(), True)
                ])
        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "|")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name.split(".")[0])
        
              
        df = df.na.drop("all")
        if df.count() == 0:
            return "No Data in file"

        
        df = df.withColumn("filename", lit(file_name))
        df = df.withColumn("crt_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        df = df.withColumn("run_id", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))

        df1=df
        concat_columns = [
                              "status",
                                "userdb_id",
                                "username",
                                "first_name",
                                "last_name",
                                "team",
                                "superior_name",
                                "authorisation_group",
                                "email_address",
                                "longitude",
                                "latitude",
                                "business_units_id",
                                "business_unit_name",
                                "mobile_number"
                    ]
        
        df1=df1.na.fill("",subset=concat_columns)


       # Concatenate the columns
        df1 = df1.withColumn("hash_c", concat(*[df1[col] for col in concat_columns]))

        #df1 = df1.withColumn("hash_c",concat(df1.Product_List,df1.ProductDB_ID,df1.SKU,df1.MSL_Ranking,df1.Date))
        df1=df1.withColumn("hash",md5(col("hash_c")))
        df1=df1.na.replace({"":None},concat_columns)
        snowdf = df1.select(
                             "status",
                            "userdb_id",
                            "username",
                            "first_name",
                            "last_name",
                            "team",
                            "superior_name",
                            "authorisation_group",
                            "email_address",
                            "longitude",
                            "latitude",
                            "business_units_id",
                            "business_unit_name",
                            "mobile_number",
                    
                            "filename",
                            "run_id",
                            "crt_dttm",
                            "hash"
                        )
        
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        
        #move to success
        
        snowdf.write.mode("append").saveAsTable(stage_name.split(".")[0] + "." + target_table)
        #snowdf.write.mode("append").saveAsTable(target_table)
        file_name1 = file_name.split(".")[0] + "_" + datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        snowdf.write.copy_into_location("@" + stage_name + "/" + temp_stage_path + "/processed/success/" + formatted_year + "/" + formatted_month + "/" + file_name1, header=True, OVERWRITE=True)
        
        return "Success"

    except KeyError as key_error:
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message

    except pd.errors.MergeError as merge_error:
        error_message = f"DataFrame merging error: {str(merge_error)}"
        return error_message

    except Exception as e:
        error_message = f"Error: {str(e)}"
        return error_message';



CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.SGPSDL_RAW.SG_POP6_POP_LISTS_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS 'from snowflake.snowpark.functions import col, lit, date_format, current_timestamp, to_date,md5, year, month, concat, format_number, regexp_replace, to_timestamp, when, trim, upper
from snowflake.snowpark.types import StringType, StructType, StructField,DecimalType
import snowflake.snowpark
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz

def main(session: snowpark.Session,Param):
    #Param=["20240620_pop_lists.csv","SGPSDL_RAW.DEV_LOAD_STAGE_ADLS","dev/pop6/master/listdata","sdl_pop6_sg_pops"]    
    try:
        file_name = Param[0]
        stage_name = Param[1]
        temp_stage_path = Param[2]
        target_table = Param[3]

        df_schema = StructType([
                    StructField("Status", StringType(), True),
                    StructField("POP_List", StringType(), True),
                    StructField("POPDB_ID", StringType(), True),
                    StructField("POP_Code", StringType(), True),
                    StructField("POP_Name", StringType(), True),
                    StructField("Date", StringType(), True)
                ])
        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "|")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name.split(".")[0])
        
              
        df = df.na.drop("all")
        if df.count() == 0:
            return "No Data in file"

        
        df = df.withColumn("filename", lit(file_name))
        df = df.withColumn("crt_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        df = df.withColumn("run_id", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))

        df1=df
        concat_columns = [
                              "Status",
                            "POP_List",
                            "POPDB_ID",
                            "POP_Code",
                            "POP_Name",
                            "Date"
                    ]
        
        df1=df1.na.fill("",subset=concat_columns)


       # Concatenate the columns
        df1 = df1.withColumn("hash_c", concat(*[df1[col] for col in concat_columns]))

        #df1 = df1.withColumn("hash_c",concat(df1.Product_List,df1.ProductDB_ID,df1.SKU,df1.MSL_Ranking,df1.Date))
        df1=df1.withColumn("hash",md5(col("hash_c")))
        df1=df1.na.replace({"":None},concat_columns)
        snowdf = df1.select(
                              "Status",
                            "POP_List",
                            "POPDB_ID",
                            "POP_Code",
                            "POP_Name",
                            "Date",
                    
                            "filename",
                            "run_id",
                            "crt_dttm",
                            "hash"
                        )
        
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        
        #move to success
        
        snowdf.write.mode("append").saveAsTable(stage_name.split(".")[0] + "." + target_table)
        #snowdf.write.mode("append").saveAsTable(target_table)
        file_name1 = file_name.split(".")[0] + "_" + datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        snowdf.write.copy_into_location("@" + stage_name + "/" + temp_stage_path + "/processed/success/" + formatted_year + "/" + formatted_month + "/" + file_name1, header=True, OVERWRITE=True)
        
        return "Success"

    except KeyError as key_error:
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message

    except pd.errors.MergeError as merge_error:
        error_message = f"DataFrame merging error: {str(merge_error)}"
        return error_message

    except Exception as e:
        error_message = f"Error: {str(e)}"
        return error_message';




CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.SGPSDL_RAW.SG_POP6_PRODUCT_LISTS_ALLOCATION_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS 'from snowflake.snowpark.functions import col, lit, date_format, current_timestamp, to_date,md5, year, month, concat, format_number, regexp_replace, to_timestamp, when, trim, upper
from snowflake.snowpark.types import StringType, StructType, StructField,DecimalType
import snowflake.snowpark
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz

def main(session: snowpark.Session,Param):
    #Param=["20240620_product_lists_allocation.csv","SGPSDL_RAW.DEV_LOAD_STAGE_ADLS","dev/pop6/master/listdata","sdl_pop6_sg_pops"]    
    try:
        file_name = Param[0]
        stage_name = Param[1]
        temp_stage_path = Param[2]
        target_table = Param[3]

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
                    StructField("Date", StringType(), True)
                ])
        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "|")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name.split(".")[0])
        
              
        df = df.na.drop("all")
        if df.count() == 0:
            return "No Data in file"

        
        df = df.withColumn("filename", lit(file_name))
        df = df.withColumn("crt_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        df = df.withColumn("run_id", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))

        df1=df
        concat_columns = [
                            "Product_Group_Status",
                            "Product_Group",
                            "Product_List_Status",
                            "Product_List",
                            "Product_List_Code",
                            "POP_Attribute_ID",
                            "POP_Attribute",
                            "POP_Attribute_Value_ID",
                            "POP_Attribute_Value",
                            "Date"
                    ]
        
        df1=df1.na.fill("",subset=concat_columns)


       # Concatenate the columns
        df1 = df1.withColumn("hash_c", concat(*[df1[col] for col in concat_columns]))

        #df1 = df1.withColumn("hash_c",concat(df1.Product_List,df1.ProductDB_ID,df1.SKU,df1.MSL_Ranking,df1.Date))
        df1=df1.withColumn("hash",md5(col("hash_c")))
        df1=df1.na.replace({"":None},concat_columns)
        snowdf = df1.select(
                             "Product_Group_Status",
                            "Product_Group",
                            "Product_List_Status",
                            "Product_List",
                            "Product_List_Code",
                            "POP_Attribute_ID",
                            "POP_Attribute",
                            "POP_Attribute_Value_ID",
                            "POP_Attribute_Value",
                            "Date",
                            "filename",
                            "run_id",
                            "crt_dttm",
                            "hash"
                        )
        
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        
        #move to success
        
        snowdf.write.mode("append").saveAsTable(stage_name.split(".")[0] + "." + target_table)
        #snowdf.write.mode("append").saveAsTable(target_table)
        file_name1 = file_name.split(".")[0] + "_" + datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        snowdf.write.copy_into_location("@" + stage_name + "/" + temp_stage_path + "/processed/success/" + formatted_year + "/" + formatted_month + "/" + file_name1, header=True, OVERWRITE=True)
        
        return "Success"

    except KeyError as key_error:
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message

    except pd.errors.MergeError as merge_error:
        error_message = f"DataFrame merging error: {str(merge_error)}"
        return error_message

    except Exception as e:
        error_message = f"Error: {str(e)}"
        return error_message';



CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.SGPSDL_RAW.SG_POP6_PRODUCT_LISTS_PRODUCTS_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS 'from snowflake.snowpark.functions import col, lit, date_format, current_timestamp, to_date,md5, year, month, concat, format_number, regexp_replace, to_timestamp, when, trim, upper
from snowflake.snowpark.types import StringType, StructType, StructField,DecimalType
import snowflake.snowpark
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz

def main(session: snowpark.Session,Param):
    #Param=["20240620_product_lists_products.csv","SGPSDL_RAW.DEV_LOAD_STAGE_ADLS","dev/pop6/master/listdata","sdl_pop6_sg_pops"]    
    try:
        file_name = Param[0]
        stage_name = Param[1]
        temp_stage_path = Param[2]
        target_table = Param[3]

        df_schema = StructType([
                    StructField("Product_List", StringType(), True),
                    StructField("Product_List_Code", StringType(), True),
                    StructField("ProductDB_ID", StringType(), True),
                    StructField("SKU", StringType(), True),
                    StructField("MSL_Ranking", StringType(), True),
                    StructField("Date", StringType(), True)
                ])
        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "|")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name.split(".")[0])
        
              
        df = df.na.drop("all")
        if df.count() == 0:
            return "No Data in file"

        
        df = df.withColumn("filename", lit(file_name))
        df = df.withColumn("crt_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        df = df.withColumn("run_id", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))

        df1=df
        concat_columns = [
                            "Product_List",
                            "Product_List_Code",
                            "ProductDB_ID",
                            "SKU",
                            "MSL_Ranking",
                            "Date"
                    ]
        
        df1=df1.na.fill("",subset=concat_columns)


       # Concatenate the columns
        df1 = df1.withColumn("hash_c", concat(*[df1[col] for col in concat_columns]))

        #df1 = df1.withColumn("hash_c",concat(df1.Product_List,df1.ProductDB_ID,df1.SKU,df1.MSL_Ranking,df1.Date))
        df1=df1.withColumn("hash",md5(col("hash_c")))
        df1=df1.na.replace({"":None},concat_columns)
        snowdf = df1.select(
                             "Product_List",
                            "Product_List_Code",
                            "ProductDB_ID",
                            "SKU",
                            "MSL_Ranking",
                            "Date",
                            "filename",
                            "run_id",
                            "crt_dttm",
                            "hash"
                        )
        
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        
        #move to success
        
        snowdf.write.mode("append").saveAsTable(stage_name.split(".")[0] + "." + target_table)
        #snowdf.write.mode("append").saveAsTable(target_table)
        file_name1 = file_name.split(".")[0] + "_" + datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        snowdf.write.copy_into_location("@" + stage_name + "/" + temp_stage_path + "/processed/success/" + formatted_year + "/" + formatted_month + "/" + file_name1, header=True, OVERWRITE=True)
        
        return "Success"

    except KeyError as key_error:
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message

    except pd.errors.MergeError as merge_error:
        error_message = f"DataFrame merging error: {str(merge_error)}"
        return error_message

    except Exception as e:
        error_message = f"Error: {str(e)}"
        return error_message';
==
CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.NTASDL_RAW.POP6_HK_MASTER_PRODUCT_GROUPS_LISTS_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them

import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import lit, md5, concat, when, is_null, col
from snowflake.snowpark.types import  StringType, StructType, StructField
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz
from datetime import datetime, timedelta                


def main(session: snowpark.Session, Param): 
       
    try :
            
        # Param=[''20200907_product_groups_lists.csv'',''NTASDL_RAW.DEV_LOAD_STAGE_ADLS_KOR'',''dev/pop6/master/listdata'',''sdl_pop6_kr_product_groups_lists''] 

        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        # Define the schema for the DataFrame

        df_schema=StructType([
                        StructField("Product_Group_status", StringType()),
                        StructField("Product_Group", StringType()),
                        StructField("Product_List_status", StringType()),
                        StructField("Product_List", StringType()),
                        StructField("ProductDB_ID", StringType()),
                        StructField("SKU", StringType()),
                        StructField("Date", StringType()),
                        StructField("Custom_POP_Attribute_1", StringType()),
                        StructField("Custom_POP_Attribute_2", StringType()),
                        StructField("Custom_POP_Attribute_3", StringType())
                        ])
                
        dataframe = session.read.schema(df_schema) \\
        .option("skip_header",1) \\
        .option("field_delimiter", ",") \\
        .option("field_optionally_enclosed_by", "\\"") \\
        .option("encoding","UTF-8")\\
        .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)    
        
        dataframe=dataframe.na.drop("all")
        if dataframe.count()==0:
            return "No Data in file"

        dataframe = dataframe.with_column("FILE_NAME",lit(file_name))
        dataframe = dataframe.with_column("HASHKEY",md5(concat(when(is_null(col("Product_Group_status")),lit("")).otherwise(col("Product_Group_status")),
                                            when(is_null(col("Product_Group")),lit("")).otherwise(col("Product_Group")),
                                            when(is_null(col("Product_List_status")),lit("")).otherwise(col("Product_List_status")),
                                            when(is_null(col("Product_List")),lit("")).otherwise(col("Product_List")),
                                            when(is_null(col("ProductDB_ID")),lit("")).otherwise(col("ProductDB_ID")),
                                            when(is_null(col("SKU")),lit("")).otherwise(col("SKU")),
                                            when(is_null(col("Date")),lit("")).otherwise(col("Date")),
                                            when(is_null(col("Custom_POP_Attribute_1")),lit("")).otherwise(col("Custom_POP_Attribute_1")),
                                            when(is_null(col("Custom_POP_Attribute_2")),lit("")).otherwise(col("Custom_POP_Attribute_2")),
                                            when(is_null(col("Custom_POP_Attribute_3")),lit("")).otherwise(col("Custom_POP_Attribute_3")))))
                                                              
        dataframe = dataframe.with_column("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        dataframe = dataframe.with_column("CRTD_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        
        snowdf= dataframe.select(
        	"Product_Group_status",
        	"Product_Group",
        	"Product_List_status",
        	"Product_List",
        	"ProductDB_ID",
        	"SKU",
            "Date",
            "Custom_POP_Attribute_1",
            "Custom_POP_Attribute_2",
            "Custom_POP_Attribute_3",
            "FILE_NAME",
        	"RUN_ID",
        	"CRTD_DTTM",
        	"HASHKEY"
        )
        
        # Load Data to the target table
        snowdf.write.mode("append").saveAsTable(target_table)
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        #move to success
        file_name=file_name.split(".")[0]+"_"+datetime.now().strftime("%Y%m%d%H%M%S")
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
=

CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.JPNSDL_RAW.POP6_HK_MASTER_POP_LISTS_PREPROCESSING("PARAM" ARRAY)
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
    # Param=["20240617_pop_lists.csv","JPNSDL_RAW.DEV_LOAD_STAGE_ADLS","dev/pop6/master/listdata/pop_lists","sdl_pop6_jp_pop_lists"]
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        
        df_schema = StructType([
                StructField("Status", StringType(), True),
                StructField("pop_list", StringType(), True),
                StructField("popdb_id", StringType(), True),
                StructField("pop_code", StringType(), True),
                StructField("pop_name", StringType(), True),
                StructField("Date", StringType(), True)
                
                
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
            
            

        df = df.with_column(''hashkey'',md5(concat(when(is_null(col(''status'')),lit('''')).otherwise(col(''status'')),\\
                                            when(is_null(col(''pop_list'')),lit('''')).otherwise(col(''pop_list'')),\\
                                          when(is_null(col(''popdb_id'')),lit('''')).otherwise(col(''popdb_id'')),\\
                                          when(is_null(col(''pop_code'')),lit('''')).otherwise(col(''pop_code'')),\\
                                         when(is_null(col(''pop_name'')),lit('''')).otherwise(col(''pop_name'')),\\
                                         when(is_null(col(''date'')),lit('''')).otherwise(col(''date'')))))

        
        df = df.with_column(''run_id'',lit(generunid()))

        df = df.with_column(''crtd_dttm'',lit(generatecrtd_dttm()))
        

        

        
        snowdf= df.select(
                    "status",
                    "pop_list",
                    "popdb_id",
                    "pop_code",
                    "pop_name",
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


CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.JPNSDL_RAW.POP6_HK_MASTER_PRODUCT_LISTS_ALLOCATION_PREPROCESSING("PARAM" ARRAY)
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
                    "Product_List_code",
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


CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.JPNSDL_RAW.POP6_HK_MASTER_PRODUCT_LISTS_ALLOCATION_PREPROCESSING("PARAM" ARRAY)
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
                    "Product_List_code",
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

CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.JPNSDL_RAW.POP6_HK_MASTER_PRODUCT_LISTS_POPS_PREPROCESSING("PARAM" ARRAY)
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
    # Param=["20240617_product_lists_pops.csv","JPNSDL_RAW.DEV_LOAD_STAGE_ADLS","dev/pop6/master/listdata/product_lists_pops","sdl_pop6_jp_product_lists_pops"]
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        
        df_schema = StructType([
                StructField("Product_List", StringType(), True),
                StructField("Product_List_Code", StringType(), True),
                StructField("POPDB_ID", StringType(), True),
                StructField("POP_Code", StringType(), True),
                StructField("POP_Name", StringType(), True),
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
            
            

        df = df.with_column(''hashkey'',md5(concat(when(is_null(col(''product_list'')),lit('''')).otherwise(col(''product_list'')),\\
                                            when(is_null(col(''Product_List_Code'')),lit('''')).otherwise(col(''Product_List_Code'')),\\
                                          when(is_null(col(''POPDB_ID'')),lit('''')).otherwise(col(''POPDB_ID'')),\\
                                          when(is_null(col(''POP_Code'')),lit('''')).otherwise(col(''POP_Code'')),\\
                                         when(is_null(col(''POP_Name'')),lit('''')).otherwise(col(''POP_Name'')),\\
                                            when(is_null(col(''date'')),lit('''')).otherwise(col(''date'')))))

        
        df = df.with_column(''run_id'',lit(generunid()))

        df = df.with_column(''crtd_dttm'',lit(generatecrtd_dttm()))
        

        

        
        snowdf= df.select(
                    "product_list",
                    "popdb_id",
                    "pop_code",
                    "pop_name",
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



CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.JPNSDL_RAW.POP6_HK_MASTER_PRODUCT_LISTS_PRODUCTS_PREPROCESSING("PARAM" ARRAY)
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
                    "Product_List_Code",
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




















































CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.NTASDL_RAW.POP6_TH_POP_LISTS_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS 'from snowflake.snowpark.functions import col, lit, date_format, current_timestamp, to_date,md5, year, month, concat, format_number, regexp_replace, to_timestamp, when, trim, upper
from snowflake.snowpark.types import StringType, StructType, StructField,DecimalType
import snowflake.snowpark
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz

def main(session: snowpark.Session,Param):
    #Param=["20240422_pop_lists.csv","THASDL_RAW.DEV_LOAD_STAGE_ADLS","dev/pop6","SDL_POP6_TH_POP_LISTS"]
    try:
        file_name = Param[0]
        stage_name = Param[1]
        temp_stage_path = Param[2]
        target_table = Param[3]

        df_schema = StructType([
                    StructField("Status", StringType(), nullable=True),
                    StructField("POP_List", StringType(), nullable=True),
                    StructField("POPDB_ID", StringType(), nullable=True),
                    StructField("POP_Code", StringType(), nullable=True),
                    StructField("POP_Name", StringType(), nullable=True),
                    StructField("Date", StringType(), nullable=True)
                ])
        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "|")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name.split(".")[0])
        
              
        df = df.na.drop("all")
        if df.count() == 0:
            return "No Data in file"

        
        df = df.withColumn("filename", lit(file_name))
        df = df.withColumn("crt_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        df = df.withColumn("run_id", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))

        df1=df
        
        df1=df1.na.fill("",subset=["Status",
                                "POP_List",
                                "POPDB_ID",
                                "POP_Code",
                                "POP_Name"
                                ])

        df1 = df1.withColumn("hash_c",concat(df1.Status,df1.POP_List,df1.POPDB_ID,df1.POP_Code,df1.POP_Name))
        df1=df1.withColumn("hash",md5(col("hash_c")))
        df1=df1.na.replace({"":None},["Status",
                                    "POP_List",
                                    "POPDB_ID",
                                    "POP_Code",
                                    "POP_Name"
                                ])
        snowdf = df1.select(
                           "Status",
                            "POP_List",
                            "POPDB_ID",
                            "POP_Code",
                            "POP_Name",
                            "Date",
                            "filename",
                            "run_id",
                            "crt_dttm",
                            "hash"
                        )
        
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        
        #move to success
        
        
        snowdf.write.mode("append").saveAsTable(stage_name.split(".")[0] + "." + target_table)
        snowdf.write.copy_into_location("@" + stage_name + "/" + temp_stage_path + "/processed/success/" + formatted_year + "/" + formatted_month + "/" + file_name, header=True, OVERWRITE=True)
        
        return "Success"

    except KeyError as key_error:
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message

    except pd.errors.MergeError as merge_error:
        error_message = f"DataFrame merging error: {str(merge_error)}"
        return error_message

    except Exception as e:
        error_message = f"Error: {str(e)}"
        return error_message
        
        
        
        
        ';

==

CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.NTASDL_RAW.POP6_HK_PRODUCTS("PARAM" ARRAY)
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
    #Param=["20240609_products.csv","NTASDL_RAW.DEV_LOAD_STAGE_ADLS_HKG","dev/pop6/masterdata/products/","sdl_pop6_hk_products"]
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        
        df_schema = StructType([
                StructField("Status", StringType(), True),
                StructField("productdb_id", StringType(), True),
                StructField("barcode", StringType(), True),
                StructField("sku", StringType(), True),
                StructField("unit_price", StringType(), True),
                StructField("display_order", StringType(), True),
                StructField("launch_date", StringType(), True),
                StructField("largest_uom_quantity", StringType(), True),
                StructField("middle_uom_quantity", StringType(), True),
                StructField("smallest_uom_quantity", StringType(), True),
                StructField("sku_english", StringType(), True),
                StructField("sku_code", StringType(), True),
                StructField("ps_category", StringType(), True),
                StructField("ps_segment", StringType(), True),
                StructField("ps_category_segment", StringType(), True),
                StructField("company", StringType(), True),
                StructField("country_l1", StringType(), True),
                StructField("regional_franchise_l2", StringType(), True),
                StructField("franchise_l3", StringType(), True),
                StructField("brand_l4", StringType(), True),
                StructField("sub_category_l5", StringType(), True),
                StructField("platform_level_l6", StringType(), True),
                StructField("variance_l7", StringType(), True),
                StructField("pack_size_l8", StringType(), True)
                
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
            
            

        df = df.with_column(''hashkey'',md5(concat(when(is_null(col(''status'')),lit('''')).otherwise(col(''status'')),\\
                                            when(is_null(col(''productdb_id'')),lit('''')).otherwise(col(''productdb_id'')),\\
                                          when(is_null(col(''barcode'')),lit('''')).otherwise(col(''barcode'')),\\
                                          when(is_null(col(''sku'')),lit('''')).otherwise(col(''sku'')),\\
                                         when(is_null(col(''unit_price'')),lit('''')).otherwise(col(''unit_price'')),\\
                                         when(is_null(col(''display_order'')),lit('''')).otherwise(col(''display_order'')),\\
                                         when(is_null(col(''launch_date'')),lit('''')).otherwise(col(''launch_date'')),\\
                                          when(is_null(col(''largest_uom_quantity'')),lit('''')).otherwise(col(''largest_uom_quantity'')),\\
                                         when(is_null(col(''middle_uom_quantity'')),lit('''')).otherwise(col(''middle_uom_quantity'')),\\
                                         when(is_null(col(''smallest_uom_quantity'')),lit('''')).otherwise(col(''smallest_uom_quantity'')),\\
                                         when(is_null(col(''company'')),lit('''')).otherwise(col(''company'')),\\
                                         when(is_null(col(''sku_english'')),lit('''')).otherwise(col(''sku_english'')),\\
                                         when(is_null(col(''sku_code'')),lit('''')).otherwise(col(''sku_code'')),\\
                                        when(is_null(col(''ps_category'')),lit('''')).otherwise(col(''ps_category'')),\\
                                          when(is_null(col(''ps_segment'')),lit('''')).otherwise(col(''ps_segment'')),\\
                                         when(is_null(col(''ps_category_segment'')),lit('''')).otherwise(col(''ps_category_segment'')),\\
                                         when(is_null(col(''country_l1'')),lit('''')).otherwise(col(''country_l1'')),\\
                                         when(is_null(col(''regional_franchise_l2'')),lit('''')).otherwise(col(''regional_franchise_l2'')),\\
                                          when(is_null(col(''franchise_l3'')),lit('''')).otherwise(col(''franchise_l3'')),\\
                                         when(is_null(col(''brand_l4'')),lit('''')).otherwise(col(''brand_l4'')),\\
                                         when(is_null(col(''sub_category_l5'')),lit('''')).otherwise(col(''sub_category_l5'')),\\
                                         when(is_null(col(''platform_level_l6'')),lit('''')).otherwise(col(''platform_level_l6'')),\\
                                         when(is_null(col(''variance_l7'')),lit('''')).otherwise(col(''variance_l7'')),\\
                                         when(is_null(col(''pack_size_l8'')),lit('''')).otherwise(col(''pack_size_l8'')))))

        
        df = df.with_column(''run_id'',lit(generunid()))

        df = df.with_column(''crtd_dttm'',lit(generatecrtd_dttm()))
        

        

        
        snowdf= df.select(
                   "Status", 
                "productdb_id", 
                "barcode", 
                "sku", 
                "unit_price", 
                "display_order", 
                "launch_date", 
                "largest_uom_quantity", 
                "middle_uom_quantity", 
                "smallest_uom_quantity", 
                "company", 
                "sku_english", 
                "sku_code", 
                "ps_category", 
                "ps_segment", 
                "ps_category_segment", 
                "country_l1", 
                "regional_franchise_l2", 
                "franchise_l3", 
                "brand_l4", 
                "sub_category_l5", 
                "platform_level_l6", 
                "variance_l7", 
                "pack_size_l8",
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

==

CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.NTASDL_RAW.POP6_HK_TRANS_SERVICE_LEVELS_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them.

import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import lit
from snowflake.snowpark.types import  StringType, StructType, StructField
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz
from datetime import datetime, timedelta



def main(session: snowpark.Session,Param): 
       
    try :
            
        # Param=[''20240612_service_levels.csv'',''NTASDL_RAW.DEV_LOAD_STAGE_ADLS_HKG'',''dev/pop6/visitdata/'',''sdl_pop6_hk_service_levels'']

        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        # Define the schema for the DataFrame
        
        df_schema=StructType([
                        StructField("customer", StringType()),
                        StructField("customer_grade", StringType()),
                        StructField("team", StringType()),
                        StructField("visit_frequency", StringType()),
                        StructField("estimated_visit_duration", StringType()),
                        StructField("service_level_date", StringType())
                        
                        ])
                
        dataframe = session.read.schema(df_schema) \\
        .option("skip_header",1) \\
        .option("field_delimiter", ",") \\
        .option("encoding","UTF-8")\\
        .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
        
       
            
        
        dataframe=dataframe.na.drop("all")
        if dataframe.count()==0:
            return "No Data in file"

       
        
        
        
        dataframe = dataframe.with_column("FILE_NAME", lit(file_name) )
        dataframe = dataframe.with_column("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        dataframe = dataframe.with_column("CRTD_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
       
        
        # Load Data to the target table
        dataframe.write.mode("append").saveAsTable(target_table)
       
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        #move to success
        file_name=file_name.split(".")[0]+''_''+datetime.now().strftime("%Y%m%d%H%M%S")
        dataframe.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)

        
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



CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.NTASDL_RAW.POP6_HK_TRANS_EXECUTED_VISITS_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them.

import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import lit
from snowflake.snowpark.types import  StringType, StructType, StructField,DecimalType,IntegerType
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz
from datetime import datetime, timedelta



def main(session: snowpark.Session,Param): 
       
    try :
            
        #Param=[''executed_visits.csv'',''NTASDL_RAW.DEV_LOAD_STAGE_ADLS_HKG'',''dev/pop6/visitdata/'',''sdl_pop6_hk_executed_visits'']

        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        # Define the schema for the DataFrame
        
        df_schema=StructType([
                        StructField("visit_id", StringType()),
                        StructField("visit_date", StringType()),
                        StructField("check_in_datetime", StringType()),
                        StructField("check_out_datetime", StringType()),
                        StructField("popdb_id", StringType()),
                        StructField("pop_code", StringType()),
                        StructField("pop_name", StringType()),
                        StructField("address", StringType()),
                        StructField("check_in_longitude", DecimalType(18, 5)),
                        StructField("check_in_latitude", DecimalType(18, 5)),
                        StructField("check_out_longitude", DecimalType(18, 5)),
                        StructField("check_out_latitude", DecimalType(18, 5)),
                        StructField("check_in_photo", StringType()),
                        StructField("check_out_photo", StringType()),
                        StructField("username", StringType()),
                        StructField("user_full_name", StringType()),
                        StructField("superior_username", StringType()),
                        StructField("superior_name", StringType()),
                        StructField("planned_visit", StringType()),
                        StructField("cancelled_visit", IntegerType()),
                        StructField("cancellation_reason", IntegerType()),
                        StructField("cancellation_note", StringType())
            
                        
                        ])
                
        dataframe = session.read.schema(df_schema) \\
        .option("skip_header",1) \\
        .option("field_delimiter", ",") \\
        .option("field_optionally_enclosed_by", "\\"") \\
        .option("encoding","UTF-8")\\
        .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
        
       
            
        
        dataframe=dataframe.na.drop("all")
        if dataframe.count()==0:
            return "No Data in file"

       
        
        
        # Get the current date and time
        current_datetime = datetime.now()
        # Subtract one day
        yesterday_datetime = current_datetime - timedelta(days=1)
        dataframe = dataframe.with_column("FILE_NAME",lit(  file_name ))
        dataframe = dataframe.with_column("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        dataframe = dataframe.with_column("CRTD_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
       
        
        # Load Data to the target table
        dataframe.write.mode("append").saveAsTable(target_table)
       
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        #move to success
        file_name=file_name.split(".")[0]+''_''+datetime.now().strftime("%Y%m%d%H%M%S")
        dataframe.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)

        
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

=


CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.NTASDL_RAW.POP6_HK_TRANS_PLANNED_VISITS_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them.

import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import lit
from snowflake.snowpark.types import  StringType, StructType, StructField,DecimalType,IntegerType
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz
from datetime import datetime, timedelta



def main(session: snowpark.Session,Param): 
       
    try :
            
        #Param=[''planned_visits.csv'',''NTASDL_RAW.DEV_LOAD_STAGE_ADLS_HKG'',''dev/pop6/visitdata/'',''sdl_pop6_hk_planned_visits'']

        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        # Define the schema for the DataFrame
        
        df_schema=StructType([
                        StructField("planned_visit_date", StringType()),
                        StructField("popdb_id", StringType()),
                        StructField("pop_code", StringType()),
                        StructField("pop_name", StringType()),
                        StructField("address", StringType()),
                        StructField("username", StringType()),
                        StructField("user_full_name", StringType())
            
                        
                        ])
                
        dataframe = session.read.schema(df_schema) \\
        .option("skip_header",1) \\
        .option("field_delimiter", ",") \\
        .option("field_optionally_enclosed_by", "\\"") \\
        .option("encoding","UTF-8")\\
        .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
        
       
            
        
        dataframe=dataframe.na.drop("all")
        if dataframe.count()==0:
            return "No Data in file"

       
        
        
     
        dataframe = dataframe.with_column("FILE_NAME",lit( file_name ))
        dataframe = dataframe.with_column("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        dataframe = dataframe.with_column("CRTD_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
       
        
        # Load Data to the target table
        dataframe.write.mode("append").saveAsTable(target_table)
       
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        #move to success
        file_name=file_name.split(".")[0]+''_''+datetime.now().strftime("%Y%m%d%H%M%S")
        dataframe.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)

        
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

==

CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.NTASDL_RAW.POP6_HK_TRANS_TASKS_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them.

import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import lit
from snowflake.snowpark.types import  StringType, StructType, StructField,IntegerType
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz
from datetime import datetime, timedelta



def main(session: snowpark.Session,Param): 
       
    try :
           
        # Param=[''20231020_tasks.csv'',''NTASDL_RAW.DEV_LOAD_STAGE_ADLS_HKG'',''dev/pop6/taskdata/'',''sdl_pop6_hk_tasks'']

        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        # Define the schema for the DataFrame
        
        df_schema=StructType([
                        StructField("visit_id", StringType()),
                        StructField("task_group", StringType()),
                        StructField("task_id", IntegerType()),
                        StructField("task_name", StringType()),
                        StructField("field_id", StringType()),
                        StructField("field_code", StringType()),
                        StructField("field_label", StringType()),
                        StructField("field_type", StringType()),
                        StructField("dependent_on_field_id", StringType()),
                        StructField("response", StringType())
            
            
                        
                        ])
                
        dataframe = session.read.schema(df_schema) \\
        .option("skip_header",1) \\
        .option("field_delimiter", ",") \\
        .option("field_optionally_enclosed_by", "\\"") \\
        .option("encoding","UTF-8")\\
        .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
        
       
            
        
        dataframe=dataframe.na.drop("all")
        if dataframe.count()==0:
            return "No Data in file"

       
        
        
     
        dataframe = dataframe.with_column("FILE_NAME",lit( file_name ))
        dataframe = dataframe.with_column("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        dataframe = dataframe.with_column("CRTD_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
       
        
        # Load Data to the target table
        dataframe.write.mode("append").saveAsTable(target_table)
       
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        #move to success
        file_name=file_name.split(".")[0]+''_''+datetime.now().strftime("%Y%m%d%H%M%S")
        dataframe.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)

        
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




CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.NTASDL_RAW.POP6_HK_TRANS_TASKS_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them.

import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import lit
from snowflake.snowpark.types import  StringType, StructType, StructField,IntegerType
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz
from datetime import datetime, timedelta



def main(session: snowpark.Session,Param): 
       
    try :
           
        # Param=[''20231020_tasks.csv'',''NTASDL_RAW.DEV_LOAD_STAGE_ADLS_HKG'',''dev/pop6/taskdata/'',''sdl_pop6_hk_tasks'']

        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        # Define the schema for the DataFrame
        
        df_schema=StructType([
                        StructField("visit_id", StringType()),
                        StructField("task_group", StringType()),
                        StructField("task_id", IntegerType()),
                        StructField("task_name", StringType()),
                        StructField("field_id", StringType()),
                        StructField("field_code", StringType()),
                        StructField("field_label", StringType()),
                        StructField("field_type", StringType()),
                        StructField("dependent_on_field_id", StringType()),
                        StructField("response", StringType())
            
            
                        
                        ])
                
        dataframe = session.read.schema(df_schema) \\
        .option("skip_header",1) \\
        .option("field_delimiter", ",") \\
        .option("field_optionally_enclosed_by", "\\"") \\
        .option("encoding","UTF-8")\\
        .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
        
       
            
        
        dataframe=dataframe.na.drop("all")
        if dataframe.count()==0:
            return "No Data in file"

       
        
        
     
        dataframe = dataframe.with_column("FILE_NAME",lit( file_name ))
        dataframe = dataframe.with_column("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        dataframe = dataframe.with_column("CRTD_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
       
        
        # Load Data to the target table
        dataframe.write.mode("append").saveAsTable(target_table)
       
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        #move to success
        file_name=file_name.split(".")[0]+''_''+datetime.now().strftime("%Y%m%d%H%M%S")
        dataframe.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)

        
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


=


CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.NTASDL_RAW.POP6_KR_TRANS_PRODUCT_ATTRIBUTE_AUDITS_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them.

import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import lit
from snowflake.snowpark.types import  StringType, StructType, StructField
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz
from datetime import datetime, timedelta                



def main(session: snowpark.Session, Param): 
       
    try :
            
        # Param=[''20240612_product_attribute_audits.csv'',''NTASDL_RAW.DEV_LOAD_STAGE_ADLS_KOR'',''dev/pop6/auditdata/'',''sdl_pop6_kr_product_attribute_audits'']  

        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        # Define the schema for the DataFrame
        
        df_schema=StructType([
                        StructField("Visit_ID", StringType()),
                        StructField("Audit_Form_ID", StringType()),
                        StructField("Audit_Form", StringType()),
                        StructField("Section_ID", StringType()),
                        StructField("Section", StringType()),
                        StructField("Field_ID", StringType()),
                        StructField("Field_Code", StringType()),
                        StructField("Field_Label", StringType()),
                        StructField("Field_Type", StringType()),
                        StructField("Dependent_On_Field_ID", StringType()),
                        StructField("Product_Attribute_ID", StringType()),
                        StructField("Product_Attribute", StringType()),
                        StructField("Product_Attribute_Value_ID", StringType()),
                        StructField("Product_Attribute_Value", StringType()),
                        StructField("Response", StringType())
                        ])
                
        dataframe = session.read.schema(df_schema) \\
        .option("skip_header",1) \\
        .option("field_delimiter", ",") \\
        .option("field_optionally_enclosed_by", "\\"") \\
        .option("encoding","UTF-8")\\
        .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
        
       
            
        
        dataframe=dataframe.na.drop("all")
        if dataframe.count()==0:
            return "No Data in file"    

        dataframe = dataframe.with_column("FILE_NAME",lit(file_name))
        dataframe = dataframe.with_column("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        dataframe = dataframe.with_column("CRTD_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        
        # Load Data to the target table
        dataframe.write.mode("append").saveAsTable(target_table)
       
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        #move to success
        file_name=file_name.split(".")[0]+"_"+datetime.now().strftime("%Y%m%d%H%M%S")
        dataframe.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)
        
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



CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.NTASDL_RAW.POP6_KR_TRANS_SKU_AUDITS_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them.

import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import lit
from snowflake.snowpark.types import  StringType, StructType, StructField
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz
from datetime import datetime, timedelta                



def main(session: snowpark.Session, Param): 
       
    try :
            
        # Param=[''20240612_sku_audits.csv'',''NTASDL_RAW.DEV_LOAD_STAGE_ADLS_KOR'',''dev/pop6/auditdata/'',''sdl_pop6_kr_sku_audits'']  

        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        # Define the schema for the DataFrame
        
        df_schema=StructType([
                        StructField("Visit_ID", StringType()),
                        StructField("Audit_Form_ID", StringType()),
                        StructField("Audit_Form", StringType()),
                        StructField("Section_ID", StringType()),
                        StructField("Section", StringType()),
                        StructField("Field_ID", StringType()),
                        StructField("Field_Code", StringType()),
                        StructField("Field_Label", StringType()),
                        StructField("Field_Type", StringType()),
                        StructField("Dependent_On_Field_ID", StringType()),
                        StructField("SKU_ID", StringType()),
                        StructField("SKU", StringType()),
                        StructField("Response", StringType())
                        ])
                
        dataframe = session.read.schema(df_schema) \\
        .option("skip_header",1) \\
        .option("field_delimiter", ",") \\
        .option("field_optionally_enclosed_by", "\\"") \\
        .option("encoding","UTF-8")\\
        .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
        
       
            
        
        dataframe=dataframe.na.drop("all")
        if dataframe.count()==0:
            return "No Data in file"    

       
        
        # # Get the current date and time
        # current_datetime = datetime.now()
        # # Subtract one day
        # yesterday_datetime = current_datetime - timedelta(days=1)
        # dataframe = dataframe.with_column("FILE_NAME",lit( yesterday_datetime.strftime("%Y%m%d") +"_"+ file_name ))
        dataframe = dataframe.with_column("FILE_NAME",lit(file_name))
        dataframe = dataframe.with_column("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        dataframe = dataframe.with_column("CRTD_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        
        # Load Data to the target table
        dataframe.write.mode("append").saveAsTable(target_table)
       
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        #move to success
        file_name=file_name.split(".")[0]+"_"+datetime.now().strftime("%Y%m%d%H%M%S")
        dataframe.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)
        
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




CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.NTASDL_RAW.POP6_KR_TRANS_PLANNED_VISITS_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them.

import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import lit
from snowflake.snowpark.types import  StringType, StructType, StructField
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz
from datetime import datetime, timedelta                



def main(session: snowpark.Session, Param): 
       
    try :
            
        # Param=[''20240612_planned_visits.csv'',''NTASDL_RAW.DEV_LOAD_STAGE_ADLS_KOR'',''dev/pop6/visitdata/'',''sdl_pop6_kr_planned_visits'']  

        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        # Define the schema for the DataFrame
        df_schema=StructType([
                        StructField("Planned_Visit_Date", StringType()),
                        StructField("POPDB_ID", StringType()),
                        StructField("POP_Code", StringType()),
                        StructField("POP_Name", StringType()),
                        StructField("Address", StringType()),
                        StructField("Username", StringType()),
                        StructField("User_Full_name", StringType())
                        ])
                
        dataframe = session.read.schema(df_schema) \\
        .option("skip_header",1) \\
        .option("field_delimiter", ",") \\
        .option("field_optionally_enclosed_by", "\\"") \\
        .option("encoding","UTF-8")\\
        .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
        
       
            
        
        dataframe=dataframe.na.drop("all")
        if dataframe.count()==0:
            return "No Data in file"    

       
        
        dataframe = dataframe.with_column("FILE_NAME",lit(file_name))
        dataframe = dataframe.with_column("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        dataframe = dataframe.with_column("CRTD_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        
        # Load Data to the target table
        dataframe.write.mode("append").saveAsTable(target_table)
       
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        #move to success
        file_name=file_name.split(".")[0]+"_"+datetime.now().strftime("%Y%m%d%H%M%S")
        dataframe.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)
        
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




CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.NTASDL_RAW.POP6_KR_TRANS_SERVICE_LEVELS_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them.

import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import lit
from snowflake.snowpark.types import  StringType, StructType, StructField
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz
from datetime import datetime, timedelta                



def main(session: snowpark.Session, Param): 
       
    try :
            
        # Param=[''20240612_service_levels.csv'',''NTASDL_RAW.DEV_LOAD_STAGE_ADLS_KOR'',''dev/pop6/visitdata/'',''sdl_pop6_kr_service_levels'']  

        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        # Define the schema for the DataFrame
        df_schema=StructType([
                        StructField("Retail Environment (PS)", StringType()),
                        StructField("Customer Grade", StringType()),
                        StructField("Team", StringType()),
                        StructField("Visit_Frequency", StringType()),
                        StructField("Estimated_Visit_Duration", StringType()),
                        StructField("Date", StringType())
                        ])
                
        dataframe = session.read.schema(df_schema) \\
        .option("skip_header",1) \\
        .option("field_delimiter", ",") \\
        .option("field_optionally_enclosed_by", "\\"") \\
        .option("encoding","UTF-8")\\
        .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
        
       
            
        
        dataframe=dataframe.na.drop("all")
        if dataframe.count()==0:
            return "No Data in file"    

       
        
        dataframe = dataframe.with_column("FILE_NAME",lit(file_name))
        dataframe = dataframe.with_column("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        dataframe = dataframe.with_column("CRTD_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        
        # Load Data to the target table
        dataframe.write.mode("append").saveAsTable(target_table)
       
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        #move to success
        file_name=file_name.split(".")[0]+"_"+datetime.now().strftime("%Y%m%d%H%M%S")
        dataframe.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)
        
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




CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.NTASDL_RAW.POP6_KR_TRANS_EXECUTED_VISITS_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them.

import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import lit
from snowflake.snowpark.types import  StringType, StructType, StructField
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz
from datetime import datetime, timedelta                



def main(session: snowpark.Session, Param): 
       
    try :
            
        # Param=[''20240612_executed_visits.csv'',''NTASDL_RAW.DEV_LOAD_STAGE_ADLS_KOR'',''dev/pop6/visitdata/'',''sdl_pop6_kr_executed_visits'']  

        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        # Define the schema for the DataFrame
        
        df_schema=StructType([
                        StructField("Visit_ID", StringType()),
                        StructField("Visit_Date", StringType()),
                        StructField("Check-In_DateTime", StringType()),
                        StructField("Check-Out_DateTime", StringType()),
                        StructField("POPDB_ID", StringType()),
                        StructField("POP_Code", StringType()),
                        StructField("POP_Name", StringType()),
                        StructField("Address", StringType()),
                        StructField("Check-In_Longitude", StringType()),
                        StructField("Check-In_Latitude", StringType()),
                        StructField("Check-Out_Longitude", StringType()),
                        StructField("Check-Out_Latitude", StringType()),
                        StructField("Check-In_Photo", StringType()),
                        StructField("Check-Out_Photo", StringType()),
                        StructField("Username", StringType()),
                        StructField("User_Full_Name", StringType()),
                        StructField("Superior_Username", StringType()),
                        StructField("Superior_Name", StringType()),
                        StructField("Planned_Visit", StringType()),
                        StructField("Cancelled_Visit", StringType()),
                        StructField("Cancellation_Reason", StringType()),
                        StructField("Cancellation_Note", StringType())
                        ])
                
        dataframe = session.read.schema(df_schema) \\
        .option("skip_header",1) \\
        .option("field_delimiter", ",") \\
        .option("field_optionally_enclosed_by", "\\"") \\
        .option("encoding","UTF-8")\\
        .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
        
       
            
        
        dataframe=dataframe.na.drop("all")
        if dataframe.count()==0:
            return "No Data in file"    

       
        
        dataframe = dataframe.with_column("FILE_NAME",lit(file_name))
        dataframe = dataframe.with_column("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        dataframe = dataframe.with_column("CRTD_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
       
        
        # Load Data to the target table
        dataframe.write.mode("append").saveAsTable(target_table)
       
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        #move to success
        file_name=file_name.split(".")[0]+"_"+datetime.now().strftime("%Y%m%d%H%M%S")
        dataframe.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)

        
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

=


CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.NTASDL_RAW.POP6_KR_TRANS_GENERAL_AUDITS_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them.

import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import lit
from snowflake.snowpark.types import  StringType, StructType, StructField
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz
from datetime import datetime, timedelta                



def main(session: snowpark.Session, Param):
       
    try :
            
        # Param=[''20240612_general_audits.csv'',''NTASDL_RAW.DEV_LOAD_STAGE_ADLS_KOR'',''dev/pop6/auditdata/'',''sdl_pop6_kr_general_audits'']  

        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        # Define the schema for the DataFrame
        
        
        df_schema=StructType([
                        StructField("Visit_ID", StringType()),
                        StructField("Audit_Form_ID", StringType()),
                        StructField("Audit_Form", StringType()),
                        StructField("Section_ID", StringType()),
                        StructField("Section", StringType()),
                        StructField("Subsection_ID", StringType()),
                        StructField("Subsection", StringType()),
                        StructField("Field_ID", StringType()),
                        StructField("Field_Code", StringType()),
                        StructField("Field_Label", StringType()),
                        StructField("Field_Type", StringType()),
                        StructField("Dependent_On_Field_ID", StringType()),
                        StructField("Response", StringType())
                        ])
                
        dataframe = session.read.schema(df_schema) \\
        .option("skip_header",1) \\
        .option("field_delimiter", ",") \\
        .option("field_optionally_enclosed_by", "\\"") \\
        .option("encoding","UTF-8")\\
        .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
        
       
            
        
        dataframe=dataframe.na.drop("all")
        if dataframe.count()==0:
            return "No Data in file"    

       
        
        dataframe = dataframe.with_column("FILE_NAME",lit(file_name))
        dataframe = dataframe.with_column("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        dataframe = dataframe.with_column("CRTD_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
       
        
        # Load Data to the target table
        dataframe.write.mode("append").saveAsTable(target_table)
       
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        #move to success
        file_name=file_name.split(".")[0]+"_"+datetime.now().strftime("%Y%m%d%H%M%S")
        dataframe.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)

        
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






CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.NTASDL_RAW.POP6_HK_TRANS_GENERAL_AUDITS_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them.

import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import lit
from snowflake.snowpark.types import  StringType, StructType, StructField,IntegerType
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz
from datetime import datetime, timedelta



def main(session: snowpark.Session,Param): 
       
    try :
           
        #Param=[''20240611_general_audits.csv'',''NTASDL_RAW.DEV_LOAD_STAGE_ADLS_HKG'',''dev/pop6/auditdata/'',''sdl_pop6_hk_general_audits'']

        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        # Define the schema for the DataFrame
        
        df_schema=StructType([
                        StructField("visit_id", StringType()),
                        StructField("audit_form_id", StringType()),
                        StructField("audit_form", StringType()),
                        StructField("section_id", StringType()),
                        StructField("section", StringType()),
                        StructField("subsection_id", StringType()),
                        StructField("subsection", StringType()),
                        StructField("field_id", StringType()),
                        StructField("field_code", StringType()),
                        StructField("field_label", StringType()),
                        StructField("field_type", StringType()),
                        StructField("dependent_on_field_id", StringType()),
                        StructField("response", StringType())
            
                        
                        ])
                
        dataframe = session.read.schema(df_schema) \\
        .option("skip_header",1) \\
        .option("field_delimiter", ",") \\
        .option("field_optionally_enclosed_by", "\\"") \\
        .option("encoding","UTF-8")\\
        .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
        
       
            
        
        dataframe=dataframe.na.drop("all")
        if dataframe.count()==0:
            return "No Data in file"

       
        
        
     
        dataframe = dataframe.with_column("FILE_NAME",lit( file_name ))
        dataframe = dataframe.with_column("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        dataframe = dataframe.with_column("CRTD_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
       
        
        # Load Data to the target table
        dataframe.write.mode("append").saveAsTable(target_table)
       
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        #move to success
        file_name=file_name.split(".")[0]+''_''+datetime.now().strftime("%Y%m%d%H%M%S")
        dataframe.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)

        
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





CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.NTASDL_RAW.POP6_HK_TRANS_PRODUCT_ATTRIBUTE_AUDITS_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them.

import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import lit
from snowflake.snowpark.types import  StringType, StructType, StructField,IntegerType
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz
from datetime import datetime, timedelta



def main(session: snowpark.Session,Param): 
       
    try :
           
        #Param=[''20240611_product_attribute_audits.csv'',''NTASDL_RAW.DEV_LOAD_STAGE_ADLS_HKG'',''dev/pop6/auditdata/'',''sdl_pop6_hk_product_attribute_audits'']

        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        # Define the schema for the DataFrame
        
        df_schema=StructType([
                        StructField("visit_id", StringType()),
                        StructField("audit_form_id", StringType()),
                        StructField("audit_form", StringType()),
                        StructField("section_id", StringType()),
                        StructField("section", StringType()),
                        StructField("field_id", StringType()),
                        StructField("field_code", StringType()),
                        StructField("field_label", StringType()),
                        StructField("field_type", StringType()),
                        StructField("dependent_on_field_id", StringType()),
                        StructField("product_attribute_id", StringType()),
                        StructField("product_attribute", StringType()),
                        StructField("product_attribute_value_id", StringType()),
                        StructField("product_attribute_value", StringType()),
                        StructField("response", StringType())
            
                        
                        ])
                
        dataframe = session.read.schema(df_schema) \\
        .option("skip_header",1) \\
        .option("field_delimiter", ",") \\
        .option("field_optionally_enclosed_by", "\\"") \\
        .option("encoding","UTF-8")\\
        .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
        
       
            
        
        dataframe=dataframe.na.drop("all")
        if dataframe.count()==0:
            return "No Data in file"

       
        
        
     
        dataframe = dataframe.with_column("FILE_NAME",lit( file_name ))
        dataframe = dataframe.with_column("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        dataframe = dataframe.with_column("CRTD_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
       
        
        # Load Data to the target table
        dataframe.write.mode("append").saveAsTable(target_table)
       
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        #move to success
        file_name=file_name.split(".")[0]+''_''+datetime.now().strftime("%Y%m%d%H%M%S")
        dataframe.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)

        
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


=



CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.NTASDL_RAW.POP6_HK_TRANS_SKU_AUDITS_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them.

import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import lit
from snowflake.snowpark.types import  StringType, StructType, StructField,IntegerType
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz
from datetime import datetime, timedelta



def main(session: snowpark.Session,Param): 
       
    try :
           
        #Param=[''20240611_sku_audits.csv'',''NTASDL_RAW.DEV_LOAD_STAGE_ADLS_HKG'',''dev/pop6/auditdata/'',''sdl_pop6_hk_sku_audits'']

        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        # Define the schema for the DataFrame
        
        df_schema=StructType([
                        StructField("visit_id", StringType()),
                        StructField("audit_form_id", StringType()),
                        StructField("audit_form", StringType()),
                        StructField("section_id", StringType()),
                        StructField("section", StringType()),
                        StructField("field_id", StringType()),
                        StructField("field_code", StringType()),
                        StructField("field_label", StringType()),
                        StructField("field_type", StringType()),
                        StructField("dependent_on_field_id", StringType()),
                        StructField("sku_id", StringType()),
                        StructField("sku", StringType()),
                        StructField("response", StringType())
            
                        
                        ])
                
        dataframe = session.read.schema(df_schema) \\
        .option("skip_header",1) \\
        .option("field_delimiter", ",") \\
        .option("field_optionally_enclosed_by", "\\"") \\
        .option("encoding","UTF-8")\\
        .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
        
       
            
        
        dataframe=dataframe.na.drop("all")
        if dataframe.count()==0:
            return "No Data in file"

       
        
        
     
        dataframe = dataframe.with_column("FILE_NAME",lit( file_name ))
        dataframe = dataframe.with_column("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        dataframe = dataframe.with_column("CRTD_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
       
        
        # Load Data to the target table
        dataframe.write.mode("append").saveAsTable(target_table)
       
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        #move to success
        file_name=file_name.split(".")[0]+''_''+datetime.now().strftime("%Y%m%d%H%M%S")
        dataframe.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)

        
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





CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.NTASDL_RAW.POP6_HK_MASTER_USERS_PREPROCESSING("PARAM" ARRAY)
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
    #Param=["20240610_users.csv","NTASDL_RAW.DEV_LOAD_STAGE_ADLS_HKG","dev/pop6/masterdata/users/","sdl_pop6_hk_users"]
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        
        df_schema = StructType([
                StructField("Status", StringType(), True),
                StructField("USERDB_ID", StringType(), True),
                StructField("Username", StringType(), True),
                StructField("FIRST_NAME", StringType(), True),
                StructField("Last_Name", StringType(), True),
                StructField("Team", StringType(), True),
                StructField("Superior_Name", StringType(), True),
                StructField("Authorisation_Group", StringType(), True),
                StructField("Email_Address", StringType(), True),
                StructField("Longitude", StringType(), True),
                StructField("Latitude", StringType(), True),
                StructField("Business_Units_ID", StringType(), True),
                StructField("Business_Unit_Name", StringType(), True)
                
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
            
            

        df = df.with_column(''hashkey'',md5(concat(when(is_null(col(''status'')),lit('''')).otherwise(col(''status'')),\\
                                            when(is_null(col(''USERDB_ID'')),lit('''')).otherwise(col(''USERDB_ID'')),\\
                                          when(is_null(col(''Username'')),lit('''')).otherwise(col(''Username'')),\\
                                          when(is_null(col(''FIRST_NAME'')),lit('''')).otherwise(col(''FIRST_NAME'')),\\
                                         when(is_null(col(''Last_Name'')),lit('''')).otherwise(col(''Last_Name'')),\\
                                         when(is_null(col(''Team'')),lit('''')).otherwise(col(''Team'')),\\
                                         when(is_null(col(''Superior_Name'')),lit('''')).otherwise(col(''Superior_Name'')),\\
                                          when(is_null(col(''Authorisation_Group'')),lit('''')).otherwise(col(''Authorisation_Group'')),\\
                                         when(is_null(col(''Email_Address'')),lit('''')).otherwise(col(''Email_Address'')),\\
                                         when(is_null(col(''Longitude'')),lit('''')).otherwise(col(''Longitude'')),\\
                                         when(is_null(col(''Latitude'')),lit('''')).otherwise(col(''Latitude'')),\\
                                         when(is_null(col(''Business_Units_ID'')),lit('''')).otherwise(col(''Business_Units_ID'')),
                                         when(is_null(col(''Business_Unit_Name'')),lit('''')).otherwise(col(''Business_Unit_Name'')))))

        
        df = df.with_column(''run_id'',lit(generunid()))

        df = df.with_column(''crtd_dttm'',lit(generatecrtd_dttm()))
        

        

        
        snowdf= df.select(
                    "status",
                    "userdb_id",
                    "username",
                    "first_name",
                    "last_name",
                    "team",
                    "superior_name",
                    "authorisation_group",
                    "email_address",
                    "longitude",
                    "latitude",
                    "file_name",
                    "run_id",
                    "crtd_dttm",
                    "hashkey",
                    "business_units_id",
                    "business_unit_name"
            
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




CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.NTASDL_RAW.POP6_HK_MASTER_POPS_PREPROCESSING("PARAM" ARRAY)
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
    #Param=["20240609_pops.csv","NTASDL_RAW.DEV_LOAD_STAGE_ADLS_HKG","dev/pop6/masterdata/pops/","sdl_pop6_hk_pops"]
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        
        df_schema = StructType([
                StructField("Status", StringType(), True),
                StructField("popdb_id", StringType(), True),
                StructField("pop_code", StringType(), True),
                StructField("pop_name", StringType(), True),
                StructField("address", StringType(), True),
                StructField("longitude", StringType(), True),
                StructField("latitude", StringType(), True),
                StructField("Business_Units_ID", StringType(), True),
                StructField("Business_Unit_Name", StringType(), True),
                StructField("country", StringType(), True),
                StructField("channel", StringType(), True),
                StructField("retail_environment_ps", StringType(), True),
                StructField("customer", StringType(), True),
                StructField("sales_group_code", StringType(), True),
                StructField("sales_group_name", StringType(), True),
                StructField("Store_Type", StringType(), True),
                StructField("Customer_Grade", StringType(), True),
                StructField("External_Store_Code", StringType(), True),
                StructField("territory", StringType(), True),
               
                
                
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
            
            

        df = df.with_column(''hashkey'',md5(concat(when(is_null(col(''status'')),lit('''')).otherwise(col(''status'')),\\
                                            when(is_null(col(''popdb_id'')),lit('''')).otherwise(col(''popdb_id'')),\\
                                          when(is_null(col(''pop_code'')),lit('''')).otherwise(col(''pop_code'')),\\
                                          when(is_null(col(''pop_name'')),lit('''')).otherwise(col(''pop_name'')),\\
                                         when(is_null(col(''address'')),lit('''')).otherwise(col(''address'')),\\
                                         when(is_null(col(''longitude'')),lit('''')).otherwise(col(''longitude'')),\\
                                         when(is_null(col(''latitude'')),lit('''')).otherwise(col(''latitude'')),\\
                                          when(is_null(col(''country'')),lit('''')).otherwise(col(''country'')),\\
                                         when(is_null(col(''channel'')),lit('''')).otherwise(col(''channel'')),\\
                                         when(is_null(col(''retail_environment_ps'')),lit('''')).otherwise(col(''retail_environment_ps'')),\\
                                         when(is_null(col(''customer'')),lit('''')).otherwise(col(''customer'')),\\
                                         when(is_null(col(''sales_group_code'')),lit('''')).otherwise(col(''sales_group_code'')),\\
                                         when(is_null(col(''sales_group_name'')),lit('''')).otherwise(col(''sales_group_name'')),\\
                                        when(is_null(col(''customer_grade'')),lit('''')).otherwise(col(''customer_grade'')),\\
                                          when(is_null(col(''external_store_code'')),lit('''')).otherwise(col(''external_store_code'')),
                                                when(is_null(col(''business_units_id'')),lit('''')).otherwise(col(''business_units_id'')),\\
                                          when(is_null(col(''territory'')),lit('''')).otherwise(col(''territory'')))))
                                         
        
        df = df.with_column(''run_id'',lit(generunid()))

        df = df.with_column(''crtd_dttm'',lit(generatecrtd_dttm()))
        

        

        
        snowdf= df.select(
                    "Status",
                "popdb_id",
                "pop_code",
                "pop_name",
                "address",
                "longitude",
                "latitude",
                "country",
                "channel",
                "retail_environment_ps",
                "customer",
                "sales_group_code",
                "sales_group_name",
                "customer_grade",
                "external_store_code",
                "file_name",
                "run_id",
                "crtd_dttm",
                "hashkey",
                "business_units_id",
                "business_unit_name",
                "territory" 
                
            
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




CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.NTASDL_RAW.POP6_HK_MASTER_PRODUCTS_PREPROCESSING("PARAM" ARRAY)
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
    #Param=["20240609_products.csv","NTASDL_RAW.DEV_LOAD_STAGE_ADLS_HKG","dev/pop6/masterdata/products/","sdl_pop6_hk_products"]
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        
        df_schema = StructType([
                StructField("Status", StringType(), True),
                StructField("productdb_id", StringType(), True),
                StructField("barcode", StringType(), True),
                StructField("sku", StringType(), True),
                StructField("unit_price", StringType(), True),
                StructField("display_order", StringType(), True),
                StructField("launch_date", StringType(), True),
                StructField("largest_uom_quantity", StringType(), True),
                StructField("middle_uom_quantity", StringType(), True),
                StructField("smallest_uom_quantity", StringType(), True),
                StructField("sku_english", StringType(), True),
                StructField("sku_code", StringType(), True),
                StructField("ps_category", StringType(), True),
                StructField("ps_segment", StringType(), True),
                StructField("ps_category_segment", StringType(), True),
                StructField("company", StringType(), True),
                StructField("country_l1", StringType(), True),
                StructField("regional_franchise_l2", StringType(), True),
                StructField("franchise_l3", StringType(), True),
                StructField("brand_l4", StringType(), True),
                StructField("sub_category_l5", StringType(), True),
                StructField("platform_level_l6", StringType(), True),
                StructField("variance_l7", StringType(), True),
                StructField("pack_size_l8", StringType(), True)
                
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
            
            

        df = df.with_column(''hashkey'',md5(concat(when(is_null(col(''status'')),lit('''')).otherwise(col(''status'')),\\
                                            when(is_null(col(''productdb_id'')),lit('''')).otherwise(col(''productdb_id'')),\\
                                          when(is_null(col(''barcode'')),lit('''')).otherwise(col(''barcode'')),\\
                                          when(is_null(col(''sku'')),lit('''')).otherwise(col(''sku'')),\\
                                         when(is_null(col(''unit_price'')),lit('''')).otherwise(col(''unit_price'')),\\
                                         when(is_null(col(''display_order'')),lit('''')).otherwise(col(''display_order'')),\\
                                         when(is_null(col(''launch_date'')),lit('''')).otherwise(col(''launch_date'')),\\
                                          when(is_null(col(''largest_uom_quantity'')),lit('''')).otherwise(col(''largest_uom_quantity'')),\\
                                         when(is_null(col(''middle_uom_quantity'')),lit('''')).otherwise(col(''middle_uom_quantity'')),\\
                                         when(is_null(col(''smallest_uom_quantity'')),lit('''')).otherwise(col(''smallest_uom_quantity'')),\\
                                         when(is_null(col(''company'')),lit('''')).otherwise(col(''company'')),\\
                                         when(is_null(col(''sku_english'')),lit('''')).otherwise(col(''sku_english'')),\\
                                         when(is_null(col(''sku_code'')),lit('''')).otherwise(col(''sku_code'')),\\
                                        when(is_null(col(''ps_category'')),lit('''')).otherwise(col(''ps_category'')),\\
                                          when(is_null(col(''ps_segment'')),lit('''')).otherwise(col(''ps_segment'')),\\
                                         when(is_null(col(''ps_category_segment'')),lit('''')).otherwise(col(''ps_category_segment'')),\\
                                         when(is_null(col(''country_l1'')),lit('''')).otherwise(col(''country_l1'')),\\
                                         when(is_null(col(''regional_franchise_l2'')),lit('''')).otherwise(col(''regional_franchise_l2'')),\\
                                          when(is_null(col(''franchise_l3'')),lit('''')).otherwise(col(''franchise_l3'')),\\
                                         when(is_null(col(''brand_l4'')),lit('''')).otherwise(col(''brand_l4'')),\\
                                         when(is_null(col(''sub_category_l5'')),lit('''')).otherwise(col(''sub_category_l5'')),\\
                                         when(is_null(col(''platform_level_l6'')),lit('''')).otherwise(col(''platform_level_l6'')),\\
                                         when(is_null(col(''variance_l7'')),lit('''')).otherwise(col(''variance_l7'')),\\
                                         when(is_null(col(''pack_size_l8'')),lit('''')).otherwise(col(''pack_size_l8'')))))

        
        df = df.with_column(''run_id'',lit(generunid()))

        df = df.with_column(''crtd_dttm'',lit(generatecrtd_dttm()))
        

        

        
        snowdf= df.select(
                   "Status", 
                "productdb_id", 
                "barcode", 
                "sku", 
                "unit_price", 
                "display_order", 
                "launch_date", 
                "largest_uom_quantity", 
                "middle_uom_quantity", 
                "smallest_uom_quantity", 
                "company", 
                "sku_english", 
                "sku_code", 
                "ps_category", 
                "ps_segment", 
                "ps_category_segment", 
                "country_l1", 
                "regional_franchise_l2", 
                "franchise_l3", 
                "brand_l4", 
                "sub_category_l5", 
                "platform_level_l6", 
                "variance_l7", 
                "pack_size_l8",
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



CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.NTASDL_RAW.POP6_KR_TRANS_DISPLAY_PLANS_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them.

import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import lit
from snowflake.snowpark.types import  StringType, StructType, StructField
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz
from datetime import datetime, timedelta                



def main(session: snowpark.Session, Param): 
       
    try :
            
        # Param=[''20240612_display_plans.csv'',''NTASDL_RAW.DEV_LOAD_STAGE_ADLS_KOR'',''dev/pop6/displaydata/'',''sdl_pop6_kr_display_plans'']  

        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        # Define the schema for the DataFrame

        df_schema=StructType([
                        StructField("Display_Plan_ID", StringType()),
                        StructField("Status", StringType()),
                        StructField("Allocation_Method", StringType()),
                        StructField("POP_Code_or_POP_List_Code", StringType()),
                        StructField("Team", StringType()),
                        StructField("Display_Code", StringType()),
                        StructField("Display_Name", StringType()),
                        StructField("Required_Number_of_Displays", StringType()),
                        StructField("Start_Date", StringType()),
                        StructField("End_Date", StringType()),
                        StructField("Product_Attribute_ID", StringType()),
                        StructField("Product_Attribute", StringType()),
                        StructField("Product_Attribute_Value_ID", StringType()),
                        StructField("Product_Attribute_Value", StringType()),
                        StructField("Comments", StringType())
                        ])
                
        dataframe = session.read.schema(df_schema) \\
        .option("skip_header",1) \\
        .option("field_delimiter", ",") \\
        .option("field_optionally_enclosed_by", "\\"") \\
        .option("encoding","UTF-8")\\
        .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
        
       
            
        
        dataframe=dataframe.na.drop("all")
        if dataframe.count()==0:
            return "No Data in file"    

       
        
        dataframe = dataframe.with_column("FILE_NAME",lit(file_name))
        dataframe = dataframe.with_column("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        dataframe = dataframe.with_column("CRTD_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))

        
        # Load Data to the target table
        dataframe.write.mode("append").saveAsTable(target_table)
       
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        #move to success
        file_name=file_name.split(".")[0]+"_"+datetime.now().strftime("%Y%m%d%H%M%S")
        dataframe.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)

        
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



CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.NTASDL_RAW.POP6_HK_TRANS_DISPLAYS_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them.

import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import lit
from snowflake.snowpark.types import  StringType, StructType, StructField,IntegerType
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz
from datetime import datetime, timedelta



def main(session: snowpark.Session,Param): 
       
    try :
           
        #Param=[''20240613_displays.csv'',''NTASDL_RAW.DEV_LOAD_STAGE_ADLS_HKG'',''dev/pop6/auditdata/'',''sdl_pop6_hk_displays'']

        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        # Define the schema for the DataFrame
        
        df_schema=StructType([
                        StructField("visit_id", StringType()),
                        StructField("display_plan_id", StringType()),
                        StructField("display_type", StringType()),
                        StructField("display_code", StringType()),
                        StructField("display_name", StringType()),
                        StructField("start_date", StringType()),
                        StructField("end_date", StringType()),
                        StructField("checklist_method", StringType()),
                        StructField("display_number", IntegerType()),
                        StructField("product_attribute_id", StringType()),
                        StructField("product_attribute", StringType()),
                        StructField("product_attribute_value_id", StringType()),
                        StructField("product_attribute_value", StringType()),
                        StructField("comments", StringType()),
                        StructField("field_id", StringType()),
                        StructField("field_code", StringType()),
                        StructField("field_label", StringType()),
                        StructField("field_type", StringType()),
                        StructField("dependent_on_field_id", StringType()),
                        StructField("response", StringType())
            
                        
                        ])
                
        dataframe = session.read.schema(df_schema) \\
        .option("skip_header",1) \\
        .option("field_delimiter", ",") \\
        .option("field_optionally_enclosed_by", "\\"") \\
        .option("encoding","UTF-8")\\
        .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
        
       
            
        
        dataframe=dataframe.na.drop("all")
        if dataframe.count()==0:
            return "No Data in file"

       
        
        
     
        dataframe = dataframe.with_column("FILE_NAME",lit( file_name ))
        dataframe = dataframe.with_column("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        dataframe = dataframe.with_column("CRTD_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
       
        
        # Load Data to the target table
        dataframe.write.mode("append").saveAsTable(target_table)
       
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        #move to success
        file_name=file_name.split(".")[0]+''_''+datetime.now().strftime("%Y%m%d%H%M%S")
        dataframe.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)

        
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

=

CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.NTASDL_RAW.POP6_KR_TRANS_DISPLAYS_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them.

import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import lit
from snowflake.snowpark.types import  StringType, StructType, StructField
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz
from datetime import datetime, timedelta                



def main(session: snowpark.Session, Param): 
       
    try :
            
        # Param=[''20240612_displays.csv'',''NTASDL_RAW.DEV_LOAD_STAGE_ADLS_KOR'',''dev/pop6/displaydata/'',''sdl_pop6_kr_displays'']  

        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        # Define the schema for the DataFrame

    
        df_schema=StructType([
                        StructField("Visit_ID", StringType()),
                        StructField("Display_Plan_ID", StringType()),
                        StructField("Display_Type", StringType()),
                        StructField("Display_Code", StringType()),
                        StructField("Display_Name", StringType()),
                        StructField("Start_Date", StringType()),
                        StructField("End_Date", StringType()),
                        StructField("Checklist_Method", StringType()),
                        StructField("Display_Number", StringType()),
                        StructField("Product_Attribute_ID", StringType()),
                        StructField("Product_Attribute", StringType()),
                        StructField("Product_Attribute_Value_ID", StringType()),
                        StructField("Product_Attribute_Value", StringType()),
                        StructField("Comments", StringType()),
                        StructField("Field_ID", StringType()),
                        StructField("Field_Code", StringType()),
                        StructField("Field_Label", StringType()),
                        StructField("Field_Type", StringType()),
                        StructField("Dependent_On_Field_ID", StringType()),
                        StructField("Response", StringType())
                        ])
                
        dataframe = session.read.schema(df_schema) \\
        .option("skip_header",1) \\
        .option("field_delimiter", ",") \\
        .option("field_optionally_enclosed_by", "\\"") \\
        .option("encoding","UTF-8")\\
        .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
        
       
            
        
        dataframe=dataframe.na.drop("all")
        if dataframe.count()==0:
            return "No Data in file"    

       
        
        dataframe = dataframe.with_column("FILE_NAME",lit(file_name))
        dataframe = dataframe.with_column("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        dataframe = dataframe.with_column("CRTD_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))

        
        # Load Data to the target table
        dataframe.write.mode("append").saveAsTable(target_table)
       
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        #move to success
        file_name=file_name.split(".")[0]+"_"+datetime.now().strftime("%Y%m%d%H%M%S")
        dataframe.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)

        
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



CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.NTASDL_RAW.POP6_KR_TRANS_DATA_EXCLUSION_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them.

import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import lit
from snowflake.snowpark.types import  StringType, StructType, StructField
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz
from datetime import datetime, timedelta                



def main(session: snowpark.Session, Param): 
       
    try :
            
        # Param=[''20221103_pop6_kr_data_exclusion.csv'',''NTASDL_RAW.DEV_LOAD_STAGE_ADLS_KOR'',''dev/pop6/exclusiondata/'',''sdl_pop6_kr_exclusion'']  

        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        # Define the schema for the DataFrame
    
        df_schema=StructType([
                        StructField("exclude_kpi", StringType()),
                        StructField("visit_date", StringType()),
                        StructField("pop_code", StringType()),
                        StructField("country", StringType()),
                        StructField("merchandiser_userid", StringType()),
                        StructField("audit_form_name", StringType()),
                        StructField("section_name", StringType()),
                        StructField("operation_type", StringType())
                        ])
                
        dataframe = session.read.schema(df_schema) \\
        .option("skip_header",1) \\
        .option("field_delimiter", ",") \\
        .option("field_optionally_enclosed_by", "\\"") \\
        .option("encoding","UTF-8")\\
        .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
        
       
            
        
        dataframe=dataframe.na.drop("all")
        if dataframe.count()==0:
            return "No Data in file"    

       
        
        dataframe = dataframe.with_column("FILE_NAME",lit(file_name))
        dataframe = dataframe.with_column("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        dataframe = dataframe.with_column("CRTD_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))

        
        # Load Data to the target table
        dataframe.write.mode("append").saveAsTable(target_table)
       
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        #move to success
        file_name=file_name.split(".")[0]+"_"+datetime.now().strftime("%Y%m%d%H%M%S")
        dataframe.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)

        
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

=

CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.NTASDL_RAW.POP6_HK_TRANS_DISPLAYS_PLAN_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them.

import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import lit
from snowflake.snowpark.types import  StringType, StructType, StructField,IntegerType
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz
from datetime import datetime, timedelta



def main(session: snowpark.Session,Param): 
       
    try :
           
        # Param=[''20240613_display_plans.csv'',''NTASDL_RAW.DEV_LOAD_STAGE_ADLS_HKG'',''dev/pop6/displaydata/displayplan/'',''sdl_pop6_hk_display_plans'']

        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        # Define the schema for the DataFrame
        
        df_schema=StructType([
                        StructField("display_plan_id", StringType()),
                        StructField("status", StringType()),
                        StructField("allocation_method", StringType()),
                        StructField("pop_code_or_pop_list_code", StringType()),
                        StructField("team", StringType()),
                        StructField("display_code", StringType()),
                        StructField("display_name", StringType()),
                        StructField("required_number_of_displays", IntegerType()),
                        StructField("start_date", StringType()),
                        StructField("end_date", StringType()),
                        StructField("product_attribute_id", StringType()),
                        StructField("product_attribute", StringType()),
                        StructField("product_attribute_value_id", StringType()),
                        StructField("product_attribute_value", StringType()),
                        StructField("comments", StringType())
            
                        
                        ])
                
        dataframe = session.read.schema(df_schema) \\
        .option("skip_header",1) \\
        .option("field_delimiter", ",") \\
        .option("field_optionally_enclosed_by", "\\"") \\
        .option("encoding","UTF-8")\\
        .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
        
       
            
        
        dataframe=dataframe.na.drop("all")
        if dataframe.count()==0:
            return "No Data in file"

       
        
        
     
        dataframe = dataframe.with_column("FILE_NAME",lit( file_name ))
        dataframe = dataframe.with_column("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        dataframe = dataframe.with_column("CRTD_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
       
        
        # Load Data to the target table
        dataframe.write.mode("append").saveAsTable(target_table)
       
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        #move to success
        file_name=file_name.split(".")[0]+''_''+datetime.now().strftime("%Y%m%d%H%M%S")
        dataframe.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)

        
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



CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.NTASDL_RAW.POP6_HK_TRANS_DISPLAYS_PLAN_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them.

import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import lit
from snowflake.snowpark.types import  StringType, StructType, StructField,IntegerType
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz
from datetime import datetime, timedelta



def main(session: snowpark.Session,Param): 
       
    try :
           
        # Param=[''20240613_display_plans.csv'',''NTASDL_RAW.DEV_LOAD_STAGE_ADLS_HKG'',''dev/pop6/displaydata/displayplan/'',''sdl_pop6_hk_display_plans'']

        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        # Define the schema for the DataFrame
        
        df_schema=StructType([
                        StructField("display_plan_id", StringType()),
                        StructField("status", StringType()),
                        StructField("allocation_method", StringType()),
                        StructField("pop_code_or_pop_list_code", StringType()),
                        StructField("team", StringType()),
                        StructField("display_code", StringType()),
                        StructField("display_name", StringType()),
                        StructField("required_number_of_displays", IntegerType()),
                        StructField("start_date", StringType()),
                        StructField("end_date", StringType()),
                        StructField("product_attribute_id", StringType()),
                        StructField("product_attribute", StringType()),
                        StructField("product_attribute_value_id", StringType()),
                        StructField("product_attribute_value", StringType()),
                        StructField("comments", StringType())
            
                        
                        ])
                
        dataframe = session.read.schema(df_schema) \\
        .option("skip_header",1) \\
        .option("field_delimiter", ",") \\
        .option("field_optionally_enclosed_by", "\\"") \\
        .option("encoding","UTF-8")\\
        .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
        
       
            
        
        dataframe=dataframe.na.drop("all")
        if dataframe.count()==0:
            return "No Data in file"

       
        
        
     
        dataframe = dataframe.with_column("FILE_NAME",lit( file_name ))
        dataframe = dataframe.with_column("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        dataframe = dataframe.with_column("CRTD_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
       
        
        # Load Data to the target table
        dataframe.write.mode("append").saveAsTable(target_table)
       
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        #move to success
        file_name=file_name.split(".")[0]+''_''+datetime.now().strftime("%Y%m%d%H%M%S")
        dataframe.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)

        
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

==

CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.NTASDL_RAW.POP6_KR_TRANS_PROMOTION_PLANS_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them.

import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import lit
from snowflake.snowpark.types import  StringType, StructType, StructField
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz
from datetime import datetime, timedelta                



def main(session: snowpark.Session, Param): 
       
    try :
            
        # Param=[''20240613_promotion_plans.csv'',''NTASDL_RAW.DEV_LOAD_STAGE_ADLS_KOR'',''dev/pop6/promotiondata/'',''sdl_pop6_kr_promotion_plans''] 

        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        # Define the schema for the DataFrame
    
        df_schema=StructType([
                        StructField("Status", StringType()),
                        StructField("Promotion_Plan_ID", StringType()),
                        StructField("Allocation_Method", StringType()),
                        StructField("POP_Code_or_POP_List_Code", StringType()),
                        StructField("Team", StringType()),
                        StructField("Promotion_Code", StringType()),
                        StructField("Promotion_Name", StringType()),
                        StructField("Promotion_Mechanics", StringType()),
                        StructField("Start_Date", StringType()),
                        StructField("End_Date", StringType()),
                        StructField("Product_Attribute_ID", StringType()),
                        StructField("Product_Attribute", StringType()),
                        StructField("Product_Attribute_Value_ID", StringType()),
                        StructField("Product_Attribute_Value", StringType()),
                        StructField("Promotion_Price", StringType()),
                        StructField("Price_Field", StringType()),
                        StructField("Photo_Field", StringType()),
                        StructField("Reason_Field", StringType())
                        ])
                
        dataframe = session.read.schema(df_schema) \\
        .option("skip_header",1) \\
        .option("field_delimiter", ",") \\
        .option("field_optionally_enclosed_by", "\\"") \\
        .option("encoding","UTF-8")\\
        .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
        
       
            
        
        dataframe=dataframe.na.drop("all")
        if dataframe.count()==0:
            return "No Data in file"    

       
        
        dataframe = dataframe.with_column("FILE_NAME",lit(file_name))
        dataframe = dataframe.with_column("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        dataframe = dataframe.with_column("CRTD_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))

        
        # Load Data to the target table
        dataframe.write.mode("append").saveAsTable(target_table)
       
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        #move to success
        file_name=file_name.split(".")[0]+"_"+datetime.now().strftime("%Y%m%d%H%M%S")
        dataframe.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)

        
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

=


CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.NTASDL_RAW.POP6_KR_TRANS_PROMOTIONS_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them.

import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import lit
from snowflake.snowpark.types import  StringType, StructType, StructField
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz
from datetime import datetime, timedelta                



def main(session: snowpark.Session, Param):
       
    try :
            
        # Param=[''20240613_promotions.csv'',''NTASDL_RAW.DEV_LOAD_STAGE_ADLS_KOR'',''dev/pop6/promotiondata/'',''sdl_pop6_kr_promotions''] 

        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        # Define the schema for the DataFrame
    
        df_schema=StructType([
                        StructField("Visit_ID", StringType()),
                        StructField("Promotion_Plan_ID", StringType()),
                        StructField("Promotion_Code", StringType()),
                        StructField("Promotion_Name", StringType()),
                        StructField("Promotion_Mechanics", StringType()),
                        StructField("Promotion_Type", StringType()),
                        StructField("Start_Date", StringType()),
                        StructField("End_Date", StringType()),
                        StructField("Product_Attribute_ID", StringType()),
                        StructField("Product_Attribute", StringType()),
                        StructField("Product_Attribute_Value_ID", StringType()),
                        StructField("Product_Attribute_Value", StringType()),
                        StructField("Promotion_Price", StringType()),
                        StructField("Promotion_Compliance", StringType()),
                        StructField("Actual_Price", StringType()),
                        StructField("Non-Compliance_Reason", StringType()),
                        StructField("Photo", StringType())
                        ])
                
        dataframe = session.read.schema(df_schema) \\
        .option("skip_header",1) \\
        .option("field_delimiter", ",") \\
        .option("field_optionally_enclosed_by", "\\"") \\
        .option("encoding","UTF-8")\\
        .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
        
       
            
        
        dataframe=dataframe.na.drop("all")
        if dataframe.count()==0:
            return "No Data in file"    

       
        
        dataframe = dataframe.with_column("FILE_NAME",lit(file_name))
        dataframe = dataframe.with_column("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        dataframe = dataframe.with_column("CRTD_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))

        
        # Load Data to the target table
        dataframe.write.mode("append").saveAsTable(target_table)
       
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        #move to success
        file_name=file_name.split(".")[0]+"_"+datetime.now().strftime("%Y%m%d%H%M%S")
        dataframe.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)

        
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


CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.NTASDL_RAW.POP6_KR_TRANS_TASKS_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them.

import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import lit
from snowflake.snowpark.types import  StringType, StructType, StructField
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz
from datetime import datetime, timedelta                

def main(session: snowpark.Session, Param): 
       
    try :
            
        # Param=[''20240613_tasks.csv'',''NTASDL_RAW.DEV_LOAD_STAGE_ADLS_KOR'',''dev/pop6/taskdata/'',''sdl_pop6_kr_tasks''] 

        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        # Define the schema for the DataFrame
        df_schema=StructType([
                        StructField("Visit_ID", StringType()),
                        StructField("Task_Group", StringType()),
                        StructField("Task_ID", StringType()),
                        StructField("Task_Name", StringType()),
                        StructField("Field_ID", StringType()),
                        StructField("Field_Code", StringType()),
                        StructField("Field_Label", StringType()),
                        StructField("Field_Type", StringType()),
                        StructField("Dependent_On_Field_ID", StringType()),
                        StructField("Response", StringType())
                        ])
                
        dataframe = session.read.schema(df_schema) \\
        .option("skip_header",1) \\
        .option("field_delimiter", ",") \\
        .option("field_optionally_enclosed_by", "\\"") \\
        .option("encoding","UTF-8")\\
        .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
        
       
            
        
        dataframe=dataframe.na.drop("all")
        if dataframe.count()==0:
            return "No Data in file"    

       
        
        dataframe = dataframe.with_column("FILE_NAME",lit(file_name))
        dataframe = dataframe.with_column("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        dataframe = dataframe.with_column("CRTD_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))

        
        # Load Data to the target table
        dataframe.write.mode("append").saveAsTable(target_table)
       
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        #move to success
        file_name=file_name.split(".")[0]+"_"+datetime.now().strftime("%Y%m%d%H%M%S")
        dataframe.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)

        
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

==

CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.NTASDL_RAW.POP6_KR_MASTER_USERS_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them

import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import lit, md5, concat, when, is_null, col
from snowflake.snowpark.types import  StringType, StructType, StructField
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz
from datetime import datetime, timedelta                


def main(session: snowpark.Session, Param): 
       
    try :
            
        # Param=[''20240616_users.csv'',''NTASDL_RAW.DEV_LOAD_STAGE_ADLS_KOR'',''dev/pop6/masterdata/'',''sdl_pop6_kr_users''] 

        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        # Define the schema for the DataFrame

        
        
        df_schema=StructType([
                        StructField("Status", StringType()),
                        StructField("UserDB_ID", StringType()),
                        StructField("Username", StringType()),
                        StructField("First_Name", StringType()),
                        StructField("Last_Name", StringType()),
                        StructField("Team", StringType()),
                        StructField("Superior_Name", StringType()),
                        StructField("Authorisation_Group", StringType()),
                        StructField("Email_Address", StringType()),
                        StructField("Longitude", StringType()),
                        StructField("Latitude", StringType()),
                        StructField("Business_Units_ID", StringType()),
                        StructField("Business_Unit_Name", StringType())
                        ])
                
        dataframe = session.read.schema(df_schema) \\
        .option("skip_header",1) \\
        .option("field_delimiter", ",") \\
        .option("field_optionally_enclosed_by", "\\"") \\
        .option("encoding","UTF-8")\\
        .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
        
       
            
        
        dataframe=dataframe.na.drop("all")
        if dataframe.count()==0:
            return "No Data in file"    

       
        
        dataframe = dataframe.with_column("FILE_NAME",lit(file_name))
        dataframe = dataframe.with_column("HASHKEY",md5(concat(when(is_null(col("Status")),lit("")).otherwise(col("Status")),
                                            when(is_null(col("UserDB_ID")),lit("")).otherwise(col("UserDB_ID")),
                                            when(is_null(col("Username")),lit("")).otherwise(col("Username")),
                                            when(is_null(col("First_Name")),lit("")).otherwise(col("First_Name")),
                                            when(is_null(col("Last_Name")),lit("")).otherwise(col("Last_Name")),
                                            when(is_null(col("Team")),lit("")).otherwise(col("Team")),
                                            when(is_null(col("Superior_Name")),lit("")).otherwise(col("Superior_Name")),
                                            when(is_null(col("Authorisation_Group")),lit("")).otherwise(col("Authorisation_Group")),
                                            when(is_null(col("Email_Address")),lit("")).otherwise(col("Email_Address")),
                                            when(is_null(col("Longitude")),lit("")).otherwise(col("Longitude")),
                                            when(is_null(col("Latitude")),lit("")).otherwise(col("Latitude")),
                                            when(is_null(col("Business_Units_ID")),lit("")).otherwise(col("Business_Units_ID")),
                                            when(is_null(col("Business_Unit_Name")),lit("")).otherwise(col("Business_Unit_Name")))))
        dataframe = dataframe.with_column("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        dataframe = dataframe.with_column("CRTD_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
                
        snowdf= dataframe.select(
                    "Status",
                    "UserDB_ID",
                    "Username",
                    "First_Name",
                    "Last_Name",
                    "Team",
                    "Superior_Name",
                    "Authorisation_Group",
                    "Email_Address",
                    "Longitude",
                    "Latitude",
                    "FILE_NAME",
                    "RUN_ID",
                    "CRTD_DTTM",
                    "HASHKEY",
                    "Business_Units_Id",
                    "Business_Unit_Name"
                    )
        
        # Load Data to the target table
        snowdf.write.mode("append").saveAsTable(target_table)
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        #move to success
        file_name=file_name.split(".")[0]+"_"+datetime.now().strftime("%Y%m%d%H%M%S")
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



CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.NTASDL_RAW.POP6_HK_TRANS_DATA_EXCLUSION_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them.

import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import lit
from snowflake.snowpark.types import  StringType, StructType, StructField,IntegerType
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz
from datetime import datetime, timedelta



def main(session: snowpark.Session,Param): 
       
    try :
           
        #Param=[''20220112_pop6_hk_data_exclusion.csv'',''NTASDL_RAW.DEV_LOAD_STAGE_ADLS_HKG'',''dev/pop6/exclusionsdata/'',''sdl_pop6_hk_exclusion'']

        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        # Define the schema for the DataFrame
        
        df_schema=StructType([
                        StructField("exclude_kpi", StringType()),
                        StructField("visit_date", StringType()),
                        StructField("pop_code", StringType()),
                        StructField("country", StringType()),
                        StructField("merchandiser_userid", StringType()),
                        StructField("audit_form_name", StringType()),
                        StructField("section_name", StringType()),
                        StructField("operation_type", StringType())
            
                        
                        ])
                
        dataframe = session.read.schema(df_schema) \\
        .option("skip_header",1) \\
        .option("field_delimiter", ",") \\
        .option("field_optionally_enclosed_by", "\\"") \\
        .option("encoding","UTF-8")\\
        .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
        
       
            
        
        dataframe=dataframe.na.drop("all")
        if dataframe.count()==0:
            return "No Data in file"

       
        
        
     
        dataframe = dataframe.with_column("FILE_NAME",lit( file_name ))
        dataframe = dataframe.with_column("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        dataframe = dataframe.with_column("CRTD_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
       
        
        # Load Data to the target table
        dataframe.write.mode("append").saveAsTable(target_table)
       
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        #move to success
        file_name=file_name.split(".")[0]+''_''+datetime.now().strftime("%Y%m%d%H%M%S")
        dataframe.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)

        
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



CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.NTASDL_RAW.POP6_TW_MASTER_POPS_PREPROCESSING("PARAM" ARRAY)
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
    #Param=["20240616_pops.csv","NTASDL_RAW.DEV_LOAD_STAGE_ADLS_TWN","dev/pop6/masterdata/pops/","sdl_pop6_tw_pops"]
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        
        df_schema = StructType([
                StructField("Status", StringType(), True),
                StructField("popdb_id", StringType(), True),
                StructField("pop_code", StringType(), True),
                StructField("pop_name", StringType(), True),
                StructField("address", StringType(), True),
                StructField("longitude", StringType(), True),
                StructField("latitude", StringType(), True),
                StructField("Business_Units_ID", StringType(), True),
                StructField("Business_Unit_Name", StringType(), True),
                StructField("country", StringType(), True),
                StructField("channel", StringType(), True),
                StructField("retail_environment_ps", StringType(), True),
                StructField("customer", StringType(), True),
                StructField("sales_group_name", StringType(), True),
                StructField("Store_Type", StringType(), True),
                StructField("sales_group_code", StringType(), True),
                StructField("Customer_Grade", StringType(), True),
                StructField("External_Store_Code", StringType(), True),
                 StructField("Sales", StringType(), True),
                StructField("territory", StringType(), True),
               
                
                
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
            
            

        df = df.with_column(''hashkey'',md5(concat(when(is_null(col(''status'')),lit('''')).otherwise(col(''status'')),\\
                                            when(is_null(col(''popdb_id'')),lit('''')).otherwise(col(''popdb_id'')),\\
                                          when(is_null(col(''pop_code'')),lit('''')).otherwise(col(''pop_code'')),\\
                                          when(is_null(col(''pop_name'')),lit('''')).otherwise(col(''pop_name'')),\\
                                         when(is_null(col(''address'')),lit('''')).otherwise(col(''address'')),\\
                                         when(is_null(col(''longitude'')),lit('''')).otherwise(col(''longitude'')),\\
                                         when(is_null(col(''latitude'')),lit('''')).otherwise(col(''latitude'')),\\
                                          when(is_null(col(''country'')),lit('''')).otherwise(col(''country'')),\\
                                         when(is_null(col(''channel'')),lit('''')).otherwise(col(''channel'')),\\
                                         when(is_null(col(''retail_environment_ps'')),lit('''')).otherwise(col(''retail_environment_ps'')),\\
                                         when(is_null(col(''customer'')),lit('''')).otherwise(col(''customer'')),\\
                                         when(is_null(col(''sales_group_code'')),lit('''')).otherwise(col(''sales_group_code'')),\\
                                         when(is_null(col(''sales_group_name'')),lit('''')).otherwise(col(''sales_group_name'')),\\
                                        when(is_null(col(''customer_grade'')),lit('''')).otherwise(col(''customer_grade'')),\\
                                          when(is_null(col(''external_store_code'')),lit('''')).otherwise(col(''external_store_code'')),
                                                when(is_null(col(''business_units_id'')),lit('''')).otherwise(col(''business_units_id'')),\\
                                                 when(is_null(col(''sales'')),lit('''')).otherwise(col(''sales'')),\\
                                          when(is_null(col(''territory'')),lit('''')).otherwise(col(''territory'')))))
                                         
        
        df = df.with_column(''run_id'',lit(generunid()))

        df = df.with_column(''crtd_dttm'',lit(generatecrtd_dttm()))
        

        

        
        snowdf= df.select(
                    "Status",
                "popdb_id",
                "pop_code",
                "pop_name",
                "address",
                "longitude",
                "latitude",
                "country",
                "channel",
                "retail_environment_ps",
                "customer",
                "sales_group_code",
                "sales_group_name",
                "customer_grade",
                "external_store_code",
                "file_name",
                "Sales",
                "run_id",
                "crtd_dttm",
                "hashkey",
                "business_units_id",
                "business_unit_name",
                "territory" 
                
            
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
=

CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.NTASDL_RAW.POP6_TW_MASTER_PRODUCTS_PREPROCESSING("PARAM" ARRAY)
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
    #Param=["20240617_products.csv","NTASDL_RAW.DEV_LOAD_STAGE_ADLS_TWN","dev/pop6/masterdata/products/","sdl_pop6_tw_products"]
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        
        df_schema = StructType([
                StructField("Status", StringType(), True),
                StructField("productdb_id", StringType(), True),
                StructField("barcode", StringType(), True),
                StructField("sku", StringType(), True),
                StructField("unit_price", StringType(), True),
                StructField("display_order", StringType(), True),
                StructField("launch_date", StringType(), True),
                StructField("largest_uom_quantity", StringType(), True),
                StructField("middle_uom_quantity", StringType(), True),
                StructField("smallest_uom_quantity", StringType(), True),
                StructField("sku_english", StringType(), True),
                StructField("company", StringType(), True),
                StructField("sku_code", StringType(), True),
                StructField("ps_category", StringType(), True),
                StructField("ps_segment", StringType(), True),
                StructField("ps_category_segment", StringType(), True),
                StructField("country_l1", StringType(), True),
                StructField("regional_franchise_l2", StringType(), True),
                StructField("franchise_l3", StringType(), True),
                StructField("brand_l4", StringType(), True),
                StructField("sub_category_l5", StringType(), True),
                StructField("platform_level_l6", StringType(), True),
                StructField("variance_l7", StringType(), True),
                StructField("pack_size_l8", StringType(), True)
                
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
            
            

        df = df.with_column(''hashkey'',md5(concat(when(is_null(col(''status'')),lit('''')).otherwise(col(''status'')),\\
                                            when(is_null(col(''productdb_id'')),lit('''')).otherwise(col(''productdb_id'')),\\
                                          when(is_null(col(''barcode'')),lit('''')).otherwise(col(''barcode'')),\\
                                          when(is_null(col(''sku'')),lit('''')).otherwise(col(''sku'')),\\
                                         when(is_null(col(''unit_price'')),lit('''')).otherwise(col(''unit_price'')),\\
                                         when(is_null(col(''display_order'')),lit('''')).otherwise(col(''display_order'')),\\
                                         when(is_null(col(''launch_date'')),lit('''')).otherwise(col(''launch_date'')),\\
                                          when(is_null(col(''largest_uom_quantity'')),lit('''')).otherwise(col(''largest_uom_quantity'')),\\
                                         when(is_null(col(''middle_uom_quantity'')),lit('''')).otherwise(col(''middle_uom_quantity'')),\\
                                         when(is_null(col(''smallest_uom_quantity'')),lit('''')).otherwise(col(''smallest_uom_quantity'')),\\
                                         when(is_null(col(''company'')),lit('''')).otherwise(col(''company'')),\\
                                         when(is_null(col(''sku_english'')),lit('''')).otherwise(col(''sku_english'')),\\
                                         when(is_null(col(''sku_code'')),lit('''')).otherwise(col(''sku_code'')),\\
                                        when(is_null(col(''ps_category'')),lit('''')).otherwise(col(''ps_category'')),\\
                                          when(is_null(col(''ps_segment'')),lit('''')).otherwise(col(''ps_segment'')),\\
                                         when(is_null(col(''ps_category_segment'')),lit('''')).otherwise(col(''ps_category_segment'')),\\
                                         when(is_null(col(''country_l1'')),lit('''')).otherwise(col(''country_l1'')),\\
                                         when(is_null(col(''regional_franchise_l2'')),lit('''')).otherwise(col(''regional_franchise_l2'')),\\
                                          when(is_null(col(''franchise_l3'')),lit('''')).otherwise(col(''franchise_l3'')),\\
                                         when(is_null(col(''brand_l4'')),lit('''')).otherwise(col(''brand_l4'')),\\
                                         when(is_null(col(''sub_category_l5'')),lit('''')).otherwise(col(''sub_category_l5'')),\\
                                         when(is_null(col(''platform_level_l6'')),lit('''')).otherwise(col(''platform_level_l6'')),\\
                                         when(is_null(col(''variance_l7'')),lit('''')).otherwise(col(''variance_l7'')),\\
                                         when(is_null(col(''pack_size_l8'')),lit('''')).otherwise(col(''pack_size_l8'')))))

        
        df = df.with_column(''run_id'',lit(generunid()))

        df = df.with_column(''crtd_dttm'',lit(generatecrtd_dttm()))
        

        

        
        snowdf= df.select(
                   "Status", 
                "productdb_id", 
                "barcode", 
                "sku", 
                "unit_price", 
                "display_order", 
                "launch_date", 
                "largest_uom_quantity", 
                "middle_uom_quantity", 
                "smallest_uom_quantity", 
                "company", 
                "sku_english", 
                "sku_code", 
                "ps_category", 
                "ps_segment", 
                "ps_category_segment", 
                "country_l1", 
                "regional_franchise_l2", 
                "franchise_l3", 
                "brand_l4", 
                "sub_category_l5", 
                "platform_level_l6", 
                "variance_l7", 
                "pack_size_l8",
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

=
CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.NTASDL_RAW.POP6_TW_MASTER_USERS_PREPROCESSING("PARAM" ARRAY)
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
    # Param=["20240616_users.csv","NTASDL_RAW.DEV_LOAD_STAGE_ADLS_TWN","dev/pop6/masterdata/users/","sdl_pop6_tw_users"]
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        
        df_schema = StructType([
                StructField("Status", StringType(), True),
                StructField("USERDB_ID", StringType(), True),
                StructField("Username", StringType(), True),
                StructField("FIRST_NAME", StringType(), True),
                StructField("Last_Name", StringType(), True),
                StructField("Team", StringType(), True),
                StructField("Superior_Name", StringType(), True),
                StructField("Authorisation_Group", StringType(), True),
                StructField("Email_Address", StringType(), True),
                StructField("Longitude", StringType(), True),
                StructField("Latitude", StringType(), True),
                StructField("Business_Units_ID", StringType(), True),
                StructField("Business_Unit_Name", StringType(), True)
                
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
            
            

        df = df.with_column(''hashkey'',md5(concat(when(is_null(col(''status'')),lit('''')).otherwise(col(''status'')),\\
                                            when(is_null(col(''USERDB_ID'')),lit('''')).otherwise(col(''USERDB_ID'')),\\
                                          when(is_null(col(''Username'')),lit('''')).otherwise(col(''Username'')),\\
                                          when(is_null(col(''FIRST_NAME'')),lit('''')).otherwise(col(''FIRST_NAME'')),\\
                                         when(is_null(col(''Last_Name'')),lit('''')).otherwise(col(''Last_Name'')),\\
                                         when(is_null(col(''Team'')),lit('''')).otherwise(col(''Team'')),\\
                                         when(is_null(col(''Superior_Name'')),lit('''')).otherwise(col(''Superior_Name'')),\\
                                          when(is_null(col(''Authorisation_Group'')),lit('''')).otherwise(col(''Authorisation_Group'')),\\
                                         when(is_null(col(''Email_Address'')),lit('''')).otherwise(col(''Email_Address'')),\\
                                         when(is_null(col(''Longitude'')),lit('''')).otherwise(col(''Longitude'')),\\
                                         when(is_null(col(''Latitude'')),lit('''')).otherwise(col(''Latitude'')),\\
                                         when(is_null(col(''Business_Units_ID'')),lit('''')).otherwise(col(''Business_Units_ID'')),
                                         when(is_null(col(''Business_Unit_Name'')),lit('''')).otherwise(col(''Business_Unit_Name'')))))

        
        df = df.with_column(''run_id'',lit(generunid()))

        df = df.with_column(''crtd_dttm'',lit(generatecrtd_dttm()))
        

        

        
        snowdf= df.select(
                    "status",
                    "userdb_id",
                    "username",
                    "first_name",
                    "last_name",
                    "team",
                    "superior_name",
                    "authorisation_group",
                    "email_address",
                    "longitude",
                    "latitude",
                    "file_name",
                    "run_id",
                    "crtd_dttm",
                    "hashkey",
                    "business_units_id",
                    "business_unit_name"
            
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

==
CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.NTASDL_RAW.POP6_TW_MASTER_POP_LISTS_PREPROCESSING("PARAM" ARRAY)
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
    # Param=["20240617_pop_lists.csv","NTASDL_RAW.DEV_LOAD_STAGE_ADLS_TWN","dev/pop6/listdata/pop_lists","sdl_pop6_tw_pop_lists"]
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        
        df_schema = StructType([
                StructField("Status", StringType(), True),
                StructField("pop_list", StringType(), True),
                StructField("popdb_id", StringType(), True),
                StructField("pop_code", StringType(), True),
                StructField("pop_name", StringType(), True),
                StructField("Date", StringType(), True)
                
                
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
            
            

        df = df.with_column(''hashkey'',md5(concat(when(is_null(col(''status'')),lit('''')).otherwise(col(''status'')),\\
                                            when(is_null(col(''pop_list'')),lit('''')).otherwise(col(''pop_list'')),\\
                                          when(is_null(col(''popdb_id'')),lit('''')).otherwise(col(''popdb_id'')),\\
                                          when(is_null(col(''pop_code'')),lit('''')).otherwise(col(''pop_code'')),\\
                                         when(is_null(col(''pop_name'')),lit('''')).otherwise(col(''pop_name'')),\\
                                         when(is_null(col(''date'')),lit('''')).otherwise(col(''date'')))))

        
        df = df.with_column(''run_id'',lit(generunid()))

        df = df.with_column(''crtd_dttm'',lit(generatecrtd_dttm()))
        

        

        
        snowdf= df.select(
                    "status",
                    "pop_list",
                    "popdb_id",
                    "pop_code",
                    "pop_name",
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


CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.NTASDL_RAW.POP6_TW_MASTER_PRODUCT_LISTS_ALLOCATION_PREPROCESSING("PARAM" ARRAY)
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
    # Param=["20240617_product_lists_allocation.csv","NTASDL_RAW.DEV_LOAD_STAGE_ADLS_TWN","dev/pop6/listdata/product_lists_allocation","sdl_pop6_tw_product_lists_allocation"]
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

CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.NTASDL_RAW.POP6_KR_MASTER_POPS_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them

import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import lit, md5, concat, when, is_null, col
from snowflake.snowpark.types import  StringType, StructType, StructField
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz
from datetime import datetime, timedelta                


def main(session: snowpark.Session, Param): 
       
    try :
            
        # Param=[''20240617_pops.csv'',''NTASDL_RAW.DEV_LOAD_STAGE_ADLS_KOR'',''dev/pop6/masterdata/'',''sdl_pop6_kr_pops''] 

        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        # Define the schema for the DataFrame
        df_schema=StructType([
                        StructField("Status", StringType()),
                        StructField("POPDB_ID", StringType()),
                        StructField("POP_Code", StringType()),
                        StructField("POP_Name", StringType()),
                        StructField("Address", StringType()),
                        StructField("Longitude", StringType()),
                        StructField("Latitude", StringType()),
                        StructField("Business_Units_ID", StringType()),
                        StructField("Business_Unit_Name", StringType()),
                        StructField("Country", StringType()),
                        StructField("Channel", StringType()),
                        StructField("Retail Environment (PS)", StringType()),
                        StructField("Sales Group Name", StringType()),
                        StructField("Customer", StringType()),    
                        StructField("Store Type", StringType()),
                        StructField("Sales Group Code", StringType()),
                        StructField("Customer Grade", StringType()),
                        StructField("External Store Code", StringType()),
                        StructField("Territory/Region", StringType()),
                        StructField("Comments", StringType())
                        ])
                
        dataframe = session.read.schema(df_schema) \\
        .option("skip_header",1) \\
        .option("field_delimiter", ",") \\
        .option("field_optionally_enclosed_by", "\\"") \\
        .option("encoding","UTF-8")\\
        .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)    
        
        dataframe=dataframe.na.drop("all")
        if dataframe.count()==0:
            return "No Data in file"    
        
        dataframe = dataframe.with_column("FILE_NAME",lit(file_name))
        dataframe = dataframe.with_column("HASHKEY",md5(concat(when(is_null(col("Status")),lit("")).otherwise(col("Status")),
                                            when(is_null(col("POPDB_ID")),lit("")).otherwise(col("POPDB_ID")),
                                            when(is_null(col("POP_Code")),lit("")).otherwise(col("POP_Code")),
                                            when(is_null(col("POP_Name")),lit("")).otherwise(col("POP_Name")),
                                            when(is_null(col("Address")),lit("")).otherwise(col("Address")),
                                            when(is_null(col("Longitude")),lit("")).otherwise(col("Longitude")),
                                            when(is_null(col("Latitude")),lit("")).otherwise(col("Latitude")),
                                            when(is_null(col("Country")),lit("")).otherwise(col("Country")),
                                            when(is_null(col("Channel")),lit("")).otherwise(col("Channel")),
                                            when(is_null(col("Retail Environment (PS)")),lit("")).otherwise(col("Retail Environment (PS)")),
                                            when(is_null(col("Customer")),lit("")).otherwise(col("Customer")),
                                            when(is_null(col("Sales Group Code")),lit("")).otherwise(col("Sales Group Code")),
                                            when(is_null(col("Sales Group Name")),lit("")).otherwise(col("Sales Group Name")),
                                            when(is_null(col("Customer Grade")),lit("")).otherwise(col("Customer Grade")),
                                            when(is_null(col("External Store Code")),lit("")).otherwise(col("External Store Code")),
                                            when(is_null(col("Business_Units_ID")),lit("")).otherwise(col("Business_Units_ID")),
                                                when(is_null(col("Business_Unit_Name")),lit("")).otherwise(col("Business_Unit_Name")),
                                            when(is_null(col("Territory/Region")),lit("")).otherwise(col("Territory/Region")))))
        dataframe = dataframe.with_column("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        dataframe = dataframe.with_column("CRTD_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        
        snowdf= dataframe.select(
                    "Status",
                    "POPDB_ID",
                    "POP_Code",
                    "POP_Name",
                    "Address",
                    "Longitude",
                    "Latitude",
                    "Country",
                    "Channel",
                    "Retail Environment (PS)",
                    "Customer",
                    "Sales Group Code",
                    "Sales Group Name",
                    "Customer Grade",
                    "External Store Code",
                    "FILE_NAME",
                    "RUN_ID",
                    "CRTD_DTTM",
                    "HASHKEY",
                    "Business_Units_ID",
                    "Business_Unit_Name",
                    "Territory/Region"
                    )
        
        # Load Data to the target table
        snowdf.write.mode("append").saveAsTable(target_table)
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        #move to success
        file_name=file_name.split(".")[0]+"_"+datetime.now().strftime("%Y%m%d%H%M%S")
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
==
CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.NTASDL_RAW.POP6_TW_MASTER_PRODUCT_LISTS_POPS_PREPROCESSING("PARAM" ARRAY)
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
    # Param=["20240617_product_lists_pops.csv","NTASDL_RAW.DEV_LOAD_STAGE_ADLS_TWN","dev/pop6/listdata/product_lists_pops","sdl_pop6_tw_product_lists_pops"]
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        
        df_schema = StructType([
                StructField("Product_List", StringType(), True),
                StructField("Product_List_Code", StringType(), True),
                StructField("POPDB_ID", StringType(), True),
                StructField("POP_Code", StringType(), True),
                StructField("POP_Name", StringType(), True),
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
            
            

        df = df.with_column(''hashkey'',md5(concat(when(is_null(col(''product_list'')),lit('''')).otherwise(col(''product_list'')),\\
                                            when(is_null(col(''Product_List_Code'')),lit('''')).otherwise(col(''Product_List_Code'')),\\
                                          when(is_null(col(''POPDB_ID'')),lit('''')).otherwise(col(''POPDB_ID'')),\\
                                          when(is_null(col(''POP_Code'')),lit('''')).otherwise(col(''POP_Code'')),\\
                                         when(is_null(col(''POP_Name'')),lit('''')).otherwise(col(''POP_Name'')),\\
                                            when(is_null(col(''date'')),lit('''')).otherwise(col(''date'')))))

        
        df = df.with_column(''run_id'',lit(generunid()))

        df = df.with_column(''crtd_dttm'',lit(generatecrtd_dttm()))
        

        

        
        snowdf= df.select(
                    "product_list",
                    "popdb_id",
                    "pop_code",
                    "pop_name",
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




CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.NTASDL_RAW.POP6_TW_MASTER_PRODUCT_LISTS_PRODUCTS_PREPROCESSING("PARAM" ARRAY)
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
    # Param=["20240617_product_lists_products.csv","NTASDL_RAW.DEV_LOAD_STAGE_ADLS_TWN","dev/pop6/listdata/product_lists_products/","sdl_pop6_tw_product_lists_products"]
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



CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.NTASDL_RAW.POP6_KR_TRANS_LOAD_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS 'from snowflake.snowpark.functions import col, lit, date_format, current_timestamp, to_date, year, month, concat, format_number, regexp_replace,to_timestamp,when,trim,upper
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DecimalType,DateType
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz


def main(session: snowpark.Session, Param):
    try:
        # Param=[''20240617_tasks.csv'',''NTASDL_RAW.DEV_LOAD_STAGE_ADLS_KOR'',''dev/pop6/taskdata/'',''sdl_pop6_kr_tasks''] 
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        sch_name        = stage_name.split(''.'')[0]
        target_table    = sch_name+"."+Param[3]
        
        df = session.read\\
            .option("INFER_SCHEMA", True)\\
            .option("skip_header",0)\\
            .option("field_delimiter", ",")\\
            .option("field_optionally_enclosed_by", "\\"") \\
            .option("encoding","UTF-8") \\
            .option("trim_space", True) \\
            .with_metadata("METADATA$FILE_ROW_NUMBER") \\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        df = df.filter(col("METADATA$FILE_ROW_NUMBER") > lit(1))

        df = df.filter(col("METADATA$FILE_ROW_NUMBER") > lit(1))
        
        df = df.drop("METADATA$FILE_ROW_NUMBER")

        df = df.with_column("FILE_NAME",lit(file_name))
        df = df.with_column("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        df = df.withColumn("CRTD_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))

        if df.count()==0:
            return "No Data in file"
        
    
        # Load Data to the target table
        df.write.mode("append").saveAsTable(target_table)
       
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        #move to success
        file_name=file_name.split(".")[0]+"_"+datetime.now().strftime("%Y%m%d%H%M%S")
        df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)

        # write to success folder
    
        file_name=file_name.split(".")[0] + ''_'' + datetime.now().strftime("%Y%m%d%H%M%S")
        df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,file_format_type="csv",OVERWRITE=True,header=True)
   
        return "Success"
        
    except KeyError as key_error:
        
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message
        
    except Exception as e:
        
        error_message = f"Error: {str(e)}" +str(df.columns)
        return error_message
';

CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.NTASDL_RAW.POP6_TW_MASTER_PRODUCT_LISTS_PRODUCTS_PREPROCESSING("PARAM" ARRAY)
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
    # Param=["20240617_product_lists_products.csv","NTASDL_RAW.DEV_LOAD_STAGE_ADLS_TWN","dev/pop6/listdata/product_lists_products/","sdl_pop6_tw_product_lists_products"]
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


CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.NTASDL_RAW.POP6_TW_MASTER_PRODUCT_GROUPS_LISTS_PREPROCESSING("PARAM" ARRAY)
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
    # Param=["20200229_product_groups_lists.csv","NTASDL_RAW.DEV_LOAD_STAGE_ADLS_TWN","dev/pop6/listdata/product_group_lists/","sdl_pop6_tw_product_groups_lists"]
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        
        df_schema = StructType([
                StructField("Product_Group_status", StringType(), True),
                StructField("Product_Group", StringType(), True),
                StructField("Product_List_status", StringType(), True),
                StructField("Product_List", StringType(), True),
                StructField("ProductDB_ID", StringType(), True),
                StructField("SKU", StringType(), True),
                StructField("Date", StringType(), True),
                StructField("Custom_POP_Attribute_1", StringType(), True),
                StructField("Custom_POP_Attribute_2", StringType(), True),
                StructField("Custom_POP_Attribute_3", StringType(), True),
                
                
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
            
            

        df = df.with_column(''hashkey'',md5(concat(when(is_null(col(''product_group_status'')),lit('''')).otherwise(col(''product_group_status'')),\\
                                            when(is_null(col(''product_group'')),lit('''')).otherwise(col(''product_group'')),\\
                                          when(is_null(col(''product_list_status'')),lit('''')).otherwise(col(''product_list_status'')),\\
                                          when(is_null(col(''product_list'')),lit('''')).otherwise(col(''product_list'')),\\
                                         when(is_null(col(''productdb_id'')),lit('''')).otherwise(col(''productdb_id'')),\\
                                            when(is_null(col(''sku'')),lit('''')).otherwise(col(''sku'')),\\
                                             when(is_null(col(''date'')),lit('''')).otherwise(col(''date'')),\\
                                         when(is_null(col(''custom_pop_attribute_1'')),lit('''')).otherwise(col(''custom_pop_attribute_1'')),\\
                                            when(is_null(col(''custom_pop_attribute_2'')),lit('''')).otherwise(col(''custom_pop_attribute_2'')),
                                               when(is_null(col(''custom_pop_attribute_3'')),lit('''')).otherwise(col(''custom_pop_attribute_3'')))))

        
        df = df.with_column(''run_id'',lit(generunid()))

        df = df.with_column(''crtd_dttm'',lit(generatecrtd_dttm()))
        

        

        
        snowdf= df.select(
                    "product_group_status",
                    "product_group",
                    "product_list_status",
                    "product_list",
                    "productdb_id",
                    "sku",
                    "date",
                    "custom_pop_attribute_1",
                    "custom_pop_attribute_2",
                    "custom_pop_attribute_3",
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



CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.NTASDL_RAW.POP6_HK_TRANS_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them.

import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import lit,col
from snowflake.snowpark.types import  StringType, StructType, StructField,IntegerType
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz
from datetime import datetime, timedelta



def main(session: snowpark.Session,Param): 
       
    try :
           
        #Param=[''20220112_pop6_hk_data_exclusion.csv'',''NTASDL_RAW.DEV_LOAD_STAGE_ADLS_HKG'',''dev/pop6/exclusionsdata/'',''sdl_pop6_hk_exclusion'']

        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        sch_name        = stage_name.split(''.'')[0]
        target_table    = sch_name+"."+Param[3]
        
        df = session.read\\
            .option("INFER_SCHEMA", True)\\
            .option("skip_header",0)\\
            .option("field_delimiter", ",")\\
            .option("field_optionally_enclosed_by", "\\"") \\
            .option("encoding","UTF-8") \\
            .option("trim_space", True) \\
            .with_metadata("METADATA$FILE_ROW_NUMBER") \\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
        
       
        df = df.filter(col("METADATA$FILE_ROW_NUMBER") > lit(1))
        df = df.drop("METADATA$FILE_ROW_NUMBER")
        
        df=df.na.drop("all")
        if df.count()==0:
            return "No Data in file"

       
        
        
     
        df = df.with_column("FILE_NAME",lit( file_name ))
        df = df.with_column("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        df = df.with_column("CRTD_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
       
        
        # Load Data to the target table
        df.write.mode("append").saveAsTable(target_table)
       
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        #move to success
        file_name=file_name.split(".")[0]+''_''+datetime.now().strftime("%Y%m%d%H%M%S")
        df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)

        
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



CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.NTASDL_RAW.POP6_KR_MASTER_PRODUCT_GROUPS_LISTS_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them

import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import lit, md5, concat, when, is_null, col
from snowflake.snowpark.types import  StringType, StructType, StructField
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz
from datetime import datetime, timedelta                


def main(session: snowpark.Session, Param): 
       
    try :
            
        # Param=[''20200907_product_groups_lists.csv'',''NTASDL_RAW.DEV_LOAD_STAGE_ADLS_KOR'',''dev/pop6/master/listdata'',''sdl_pop6_kr_product_groups_lists''] 

        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        # Define the schema for the DataFrame

        df_schema=StructType([
                        StructField("Product_Group_status", StringType()),
                        StructField("Product_Group", StringType()),
                        StructField("Product_List_status", StringType()),
                        StructField("Product_List", StringType()),
                        StructField("ProductDB_ID", StringType()),
                        StructField("SKU", StringType()),
                        StructField("Date", StringType()),
                        StructField("Custom_POP_Attribute_1", StringType()),
                        StructField("Custom_POP_Attribute_2", StringType()),
                        StructField("Custom_POP_Attribute_3", StringType())
                        ])
                
        dataframe = session.read.schema(df_schema) \\
        .option("skip_header",1) \\
        .option("field_delimiter", ",") \\
        .option("field_optionally_enclosed_by", "\\"") \\
        .option("encoding","UTF-8")\\
        .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)    
        
        dataframe=dataframe.na.drop("all")
        if dataframe.count()==0:
            return "No Data in file"

        dataframe = dataframe.with_column("FILE_NAME",lit(file_name))
        dataframe = dataframe.with_column("HASHKEY",md5(concat(when(is_null(col("Product_Group_status")),lit("")).otherwise(col("Product_Group_status")),
                                            when(is_null(col("Product_Group")),lit("")).otherwise(col("Product_Group")),
                                            when(is_null(col("Product_List_status")),lit("")).otherwise(col("Product_List_status")),
                                            when(is_null(col("Product_List")),lit("")).otherwise(col("Product_List")),
                                            when(is_null(col("ProductDB_ID")),lit("")).otherwise(col("ProductDB_ID")),
                                            when(is_null(col("SKU")),lit("")).otherwise(col("SKU")),
                                            when(is_null(col("Date")),lit("")).otherwise(col("Date")),
                                            when(is_null(col("Custom_POP_Attribute_1")),lit("")).otherwise(col("Custom_POP_Attribute_1")),
                                            when(is_null(col("Custom_POP_Attribute_2")),lit("")).otherwise(col("Custom_POP_Attribute_2")),
                                            when(is_null(col("Custom_POP_Attribute_3")),lit("")).otherwise(col("Custom_POP_Attribute_3")))))
                                                              
        dataframe = dataframe.with_column("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        dataframe = dataframe.with_column("CRTD_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        
        snowdf= dataframe.select(
        	"Product_Group_status",
        	"Product_Group",
        	"Product_List_status",
        	"Product_List",
        	"ProductDB_ID",
        	"SKU",
            "Date",
            "Custom_POP_Attribute_1",
            "Custom_POP_Attribute_2",
            "Custom_POP_Attribute_3",
            "FILE_NAME",
        	"RUN_ID",
        	"CRTD_DTTM",
        	"HASHKEY"
        )
        
        # Load Data to the target table
        snowdf.write.mode("append").saveAsTable(target_table)
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        #move to success
        file_name=file_name.split(".")[0]+"_"+datetime.now().strftime("%Y%m%d%H%M%S")
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

=
CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.NTASDL_RAW.POP6_KR_MASTER_PRODUCT_LISTS_ALLOCATION_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them

import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import lit, md5, concat, when, is_null, col
from snowflake.snowpark.types import  StringType, StructType, StructField
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz
from datetime import datetime, timedelta                


def main(session: snowpark.Session, Param): 
       
    try :
            
        # Param=[''20240623_product_lists_allocation.csv'',''NTASDL_RAW.DEV_LOAD_STAGE_ADLS_KOR'',''dev/pop6/master/listdata'',''sdl_pop6_kr_product_lists_allocation''] 

        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        # Define the schema for the DataFrame

        df_schema=StructType([
                        StructField("Product_Group_Status", StringType()),
                        StructField("Product_Group", StringType()),
                        StructField("Product_List_Status", StringType()),
                        StructField("Product_List", StringType()),
                        StructField("Product_List_Code", StringType()),
                        StructField("POP_Attribute_ID", StringType()),
                        StructField("POP_Attribute", StringType()),
                        StructField("POP_Attribute_Value_ID", StringType()),
                        StructField("POP_Attribute_Value", StringType()),
                        StructField("Date", StringType())
                        ])
                
        dataframe = session.read.schema(df_schema) \\
        .option("skip_header",1) \\
        .option("field_delimiter", ",") \\
        .option("field_optionally_enclosed_by", "\\"") \\
        .option("encoding","UTF-8")\\
        .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)    
        
        dataframe=dataframe.na.drop("all")
        if dataframe.count()==0:
            return "No Data in file"

        dataframe = dataframe.with_column("FILE_NAME",lit(file_name))
        dataframe = dataframe.with_column("HASHKEY",md5(concat(when(is_null(col("Product_Group_Status")),lit("")).otherwise(col("Product_Group_Status")),
                                            when(is_null(col("Product_Group")),lit("")).otherwise(col("Product_Group")),
                                            when(is_null(col("Product_List_Status")),lit("")).otherwise(col("Product_List_Status")),
                                            when(is_null(col("Product_List")),lit("")).otherwise(col("Product_List")),
                                            when(is_null(col("Product_List_Code")),lit("")).otherwise(col("Product_List_Code")),
                                            when(is_null(col("POP_Attribute_ID")),lit("")).otherwise(col("POP_Attribute_ID")),
                                            when(is_null(col("POP_Attribute")),lit("")).otherwise(col("POP_Attribute")),
                                            when(is_null(col("POP_Attribute_Value_ID")),lit("")).otherwise(col("POP_Attribute_Value_ID")),
                                            when(is_null(col("POP_Attribute_Value")),lit("")).otherwise(col("POP_Attribute_Value")),
                                            when(is_null(col("Date")),lit("")).otherwise(col("Date")))))
                                                              
        dataframe = dataframe.with_column("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        dataframe = dataframe.with_column("CRTD_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        
        snowdf= dataframe.select(
        	"Product_Group_Status",
        	"Product_Group",
        	"Product_List_Status",
        	"Product_List",
        	"POP_Attribute_ID",
            "POP_Attribute",
            "POP_Attribute_Value_ID",
            "POP_Attribute_Value",
            "Date",
            "FILE_NAME",
        	"RUN_ID",
        	"CRTD_DTTM",
        	"HASHKEY"
        )
        
        # Load Data to the target table
        snowdf.write.mode("append").saveAsTable(target_table)
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        #move to success
        file_name=file_name.split(".")[0]+"_"+datetime.now().strftime("%Y%m%d%H%M%S")
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




CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.NTASDL_RAW.POP6_KR_MASTER_PRODUCT_LISTS_POPS_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them

import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import lit, md5, concat, when, is_null, col
from snowflake.snowpark.types import  StringType, StructType, StructField
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz
from datetime import datetime, timedelta                


def main(session: snowpark.Session, Param): 
       
    try :
            
        # Param=[''20240624_product_lists_pops.csv'',''NTASDL_RAW.DEV_LOAD_STAGE_ADLS_KOR'',''dev/pop6/master/listdata'',''sdl_pop6_kr_product_lists_pops''] 

        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        # Define the schema for the DataFrame
        
        df_schema=StructType([
                        StructField("Product_List", StringType()),
                        StructField("Product_List_Code", StringType()),
                        StructField("POPDB_ID", StringType()),
                        StructField("POP_Code", StringType()),
                        StructField("POP_Name", StringType()),
                        StructField("Date", StringType())
                        ])
                
        dataframe = session.read.schema(df_schema) \\
        .option("skip_header",1) \\
        .option("field_delimiter", ",") \\
        .option("field_optionally_enclosed_by", "\\"") \\
        .option("encoding","UTF-8")\\
        .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)    
        
        dataframe=dataframe.na.drop("all")
        if dataframe.count()==0:
            return "No Data in file"
        
        dataframe = dataframe.with_column("FILE_NAME",lit(file_name))
        
        dataframe = dataframe.with_column("HASHKEY",md5(concat(when(is_null(col("Product_List")),lit("")).otherwise(col("Product_List")),
                                            when(is_null(col("POPDB_ID")),lit("")).otherwise(col("POPDB_ID")),
                                            when(is_null(col("POP_Code")),lit("")).otherwise(col("POP_Code")),
                                            when(is_null(col("POP_Name")),lit("")).otherwise(col("POP_Name")),
                                            when(is_null(col("Date")),lit("")).otherwise(col("Date")))))
                                                              
        dataframe = dataframe.with_column("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        dataframe = dataframe.with_column("CRTD_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        
        snowdf= dataframe.select(
        	"Product_List",
        	"POPDB_ID",
        	"POP_Code",
        	"POP_Name",
        	"Date",
            "FILE_NAME",
        	"RUN_ID",
        	"CRTD_DTTM",
        	"HASHKEY"
        )
        
        # Load Data to the target table
        snowdf.write.mode("append").saveAsTable(target_table)
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        #move to success
        file_name=file_name.split(".")[0]+"_"+datetime.now().strftime("%Y%m%d%H%M%S")
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

=


CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.NTASDL_RAW.POP6_KR_MASTER_PRODUCT_LISTS_PRODUCTS_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them

import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import lit, md5, concat, when, is_null, col
from snowflake.snowpark.types import  StringType, StructType, StructField
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz
from datetime import datetime, timedelta                


def main(session: snowpark.Session, Param): 
       
    try :
            
        # Param=[''20240624_product_lists_products.csv'',''NTASDL_RAW.DEV_LOAD_STAGE_ADLS_KOR'',''dev/pop6/master/listdata'',''sdl_pop6_kr_product_lists_products''] 

        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        # Define the schema for the DataFrame

        
        df_schema=StructType([
                        StructField("Product_List", StringType()),
                        StructField("Product_List_Code", StringType()),
                        StructField("ProductDB_ID", StringType()),
                        StructField("SKU", StringType()),
                        StructField("MSL_Ranking", StringType()),
                        StructField("Date", StringType())
                        ])
                
        dataframe = session.read.schema(df_schema) \\
        .option("skip_header",1) \\
        .option("field_delimiter", ",") \\
        .option("field_optionally_enclosed_by", "\\"") \\
        .option("encoding","UTF-8")\\
        .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)    
        
        dataframe=dataframe.na.drop("all")
        if dataframe.count()==0:
            return "No Data in file"
        
        dataframe = dataframe.with_column("FILE_NAME",lit(file_name))

        
        dataframe = dataframe.with_column("HASHKEY",md5(concat(when(is_null(col("Product_List")),lit("")).otherwise(col("Product_List")),
                                            when(is_null(col("ProductDB_ID")),lit("")).otherwise(col("ProductDB_ID")),
                                            when(is_null(col("SKU")),lit("")).otherwise(col("SKU")),
                                            when(is_null(col("MSL_Ranking")),lit("")).otherwise(col("MSL_Ranking")),
                                            when(is_null(col("Date")),lit("")).otherwise(col("Date")))))
                                                              
        dataframe = dataframe.with_column("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        dataframe = dataframe.with_column("CRTD_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))

        snowdf= dataframe.select(
        	"Product_List",
        	"ProductDB_ID",
        	"SKU",
        	"MSL_Ranking",
        	"Date",
            "FILE_NAME",
        	"RUN_ID",
        	"CRTD_DTTM",
        	"HASHKEY"
        )
        
        # Load Data to the target table
        snowdf.write.mode("append").saveAsTable(target_table)
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        #move to success
        file_name=file_name.split(".")[0]+"_"+datetime.now().strftime("%Y%m%d%H%M%S")
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




CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.NTASDL_RAW.POP6_NA_TRANSACTION_SERVICE_LEVELS_PREPROCESSING("PARAM" ARRAY)
RETURNS TABLE ()
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
    # Param=["20240617_service_levels.csv","JPNSDL_RAW.DEV_LOAD_STAGE_ADLS","dev/pop6/transaction/visitdata/service_levels/","SDL_POP6_JP_SERVICE_LEVELS"]
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        
        
        df = session.read\\
            .option("INFER_SCHEMA",True)\\
            .option("field_delimiter", ",")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .option("encoding",''UTF-8'')\\
            .with_metadata("METADATA$FILE_ROW_NUMBER") \\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        df = df.filter(col("METADATA$FILE_ROW_NUMBER") > lit(1))
        
        df = df.drop("METADATA$FILE_ROW_NUMBER")

       
        
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
            
            

       
        
        df = df.with_column(''run_id'',lit(generunid()))

        df = df.with_column(''crtd_dttm'',lit(generatecrtd_dttm()))
        

        

        
        
        file_name=file_name.split(".")[0]+"_"+datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")

        df.write.mode("append").saveAsTable(stage_name.split(".")[0] + "." + target_table)
        
        # #move to success
        df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)
        
        
        
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



CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.NTASDL_RAW.POP6_NA_TRANSACTION_PLANNED_VISITS_PREPROCESSING("PARAM" ARRAY)
RETURNS TABLE ()
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
    # Param=["20240617_service_levels.csv","JPNSDL_RAW.DEV_LOAD_STAGE_ADLS","dev/pop6/transaction/visitdata/service_levels/","SDL_POP6_JP_SERVICE_LEVELS"]
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        
        
        df = session.read\\
            .option("INFER_SCHEMA",True)\\
            .option("field_delimiter", ",")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .option("encoding",''UTF-8'')\\
            .with_metadata("METADATA$FILE_ROW_NUMBER") \\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        df = df.filter(col("METADATA$FILE_ROW_NUMBER") > lit(1))
        
        df = df.drop("METADATA$FILE_ROW_NUMBER")

       
        
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
            
            

       
        
        df = df.with_column(''run_id'',lit(generunid()))

        df = df.with_column(''crtd_dttm'',lit(generatecrtd_dttm()))
        

        

        
        
        file_name=file_name.split(".")[0]+"_"+datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")

        df.write.mode("append").saveAsTable(stage_name.split(".")[0] + "." + target_table)
        
        # #move to success
        df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)
        
        
        
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



CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.NTASDL_RAW.POP6_NA_TRANSACTION_EXECUTED_VISITS_PREPROCESSING("PARAM" ARRAY)
RETURNS TABLE ()
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
    # Param=["20240617_service_levels.csv","JPNSDL_RAW.DEV_LOAD_STAGE_ADLS","dev/pop6/transaction/visitdata/service_levels/","SDL_POP6_JP_SERVICE_LEVELS"]
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        
        
        df = session.read\\
            .option("INFER_SCHEMA",True)\\
            .option("field_delimiter", ",")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .option("encoding",''UTF-8'')\\
            .with_metadata("METADATA$FILE_ROW_NUMBER") \\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        df = df.filter(col("METADATA$FILE_ROW_NUMBER") > lit(1))
        
        df = df.drop("METADATA$FILE_ROW_NUMBER")

       
        
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
            
            

       
        
        df = df.with_column(''run_id'',lit(generunid()))

        df = df.with_column(''crtd_dttm'',lit(generatecrtd_dttm()))
        

        

        
        
        file_name=file_name.split(".")[0]+"_"+datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")

        df.write.mode("append").saveAsTable(stage_name.split(".")[0] + "." + target_table)
        
        # #move to success
        df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)
        
        
        
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

==

CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.NTASDL_RAW.POP6_NA_TRANSACTION_PRODUCT_ATTRIBUTE_AUDITS_PREPROCESSING("PARAM" ARRAY)
RETURNS TABLE ()
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
    # Param=["20240617_service_levels.csv","JPNSDL_RAW.DEV_LOAD_STAGE_ADLS","dev/pop6/transaction/visitdata/service_levels/","SDL_POP6_JP_SERVICE_LEVELS"]
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        
        
        df = session.read\\
            .option("INFER_SCHEMA",True)\\
            .option("field_delimiter", ",")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .option("encoding",''UTF-8'')\\
            .with_metadata("METADATA$FILE_ROW_NUMBER") \\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        df = df.filter(col("METADATA$FILE_ROW_NUMBER") > lit(1))
        
        df = df.drop("METADATA$FILE_ROW_NUMBER")

       
        
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
            
            

       
        
        df = df.with_column(''run_id'',lit(generunid()))

        df = df.with_column(''crtd_dttm'',lit(generatecrtd_dttm()))
        

        

        
        
        file_name=file_name.split(".")[0]+"_"+datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")

        df.write.mode("append").saveAsTable(stage_name.split(".")[0] + "." + target_table)
        
        # #move to success
        df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)
        
        
        
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



CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.NTASDL_RAW.POP6_NA_TRANSACTION_GENERAL_AUDITS_PREPROCESSING("PARAM" ARRAY)
RETURNS TABLE ()
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
    # Param=["20240617_service_levels.csv","JPNSDL_RAW.DEV_LOAD_STAGE_ADLS","dev/pop6/transaction/visitdata/service_levels/","SDL_POP6_JP_SERVICE_LEVELS"]
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        
        
        df = session.read\\
            .option("INFER_SCHEMA",True)\\
            .option("field_delimiter", ",")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .option("encoding",''UTF-8'')\\
            .with_metadata("METADATA$FILE_ROW_NUMBER") \\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        df = df.filter(col("METADATA$FILE_ROW_NUMBER") > lit(1))
        
        df = df.drop("METADATA$FILE_ROW_NUMBER")

       
        
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
            
            

       
        
        df = df.with_column(''run_id'',lit(generunid()))

        df = df.with_column(''crtd_dttm'',lit(generatecrtd_dttm()))
        

        

        
        
        file_name=file_name.split(".")[0]+"_"+datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")

        df.write.mode("append").saveAsTable(stage_name.split(".")[0] + "." + target_table)
        
        # #move to success
        df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)
        
        
        
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

==

CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.NTASDL_RAW.POP6_NA_TRANSACTION_DISPLAY_PLANS_PREPROCESSING("PARAM" ARRAY)
RETURNS TABLE ()
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
    # Param=["20240617_service_levels.csv","JPNSDL_RAW.DEV_LOAD_STAGE_ADLS","dev/pop6/transaction/visitdata/service_levels/","SDL_POP6_JP_SERVICE_LEVELS"]
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        
        
        df = session.read\\
            .option("INFER_SCHEMA",True)\\
            .option("field_delimiter", ",")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .option("encoding",''UTF-8'')\\
            .with_metadata("METADATA$FILE_ROW_NUMBER") \\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        df = df.filter(col("METADATA$FILE_ROW_NUMBER") > lit(1))
        
        df = df.drop("METADATA$FILE_ROW_NUMBER")

       
        
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
            
            

       
        
        df = df.with_column(''run_id'',lit(generunid()))

        df = df.with_column(''crtd_dttm'',lit(generatecrtd_dttm()))
        

        

        
        
        file_name=file_name.split(".")[0]+"_"+datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")

        df.write.mode("append").saveAsTable(stage_name.split(".")[0] + "." + target_table)
        
        # #move to success
        df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)
        
        
        
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



CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.NTASDL_RAW.POP6_NA_TRANSACTION_PROMOTION_PLANS_PREPROCESSING("PARAM" ARRAY)
RETURNS TABLE ()
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
    # Param=["20240617_service_levels.csv","JPNSDL_RAW.DEV_LOAD_STAGE_ADLS","dev/pop6/transaction/visitdata/service_levels/","SDL_POP6_JP_SERVICE_LEVELS"]
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        
        
        df = session.read\\
            .option("INFER_SCHEMA",True)\\
            .option("field_delimiter", ",")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .option("encoding",''UTF-8'')\\
            .with_metadata("METADATA$FILE_ROW_NUMBER") \\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        df = df.filter(col("METADATA$FILE_ROW_NUMBER") > lit(1))
        
        df = df.drop("METADATA$FILE_ROW_NUMBER")

       
        
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
            
            

       
        
        df = df.with_column(''run_id'',lit(generunid()))

        df = df.with_column(''crtd_dttm'',lit(generatecrtd_dttm()))
        

        

        
        
        file_name=file_name.split(".")[0]+"_"+datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")

        df.write.mode("append").saveAsTable(stage_name.split(".")[0] + "." + target_table)
        
        # #move to success
        df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)
        
        
        
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



CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.NTASDL_RAW.POP6_NA_TRANSACTION_PROMOTIONS_TRANSACTION("PARAM" ARRAY)
RETURNS TABLE ()
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
    # Param=["20240617_service_levels.csv","JPNSDL_RAW.DEV_LOAD_STAGE_ADLS","dev/pop6/transaction/visitdata/service_levels/","SDL_POP6_JP_SERVICE_LEVELS"]
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        
        
        df = session.read\\
            .option("INFER_SCHEMA",True)\\
            .option("field_delimiter", ",")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .option("encoding",''UTF-8'')\\
            .with_metadata("METADATA$FILE_ROW_NUMBER") \\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        df = df.filter(col("METADATA$FILE_ROW_NUMBER") > lit(1))
        
        df = df.drop("METADATA$FILE_ROW_NUMBER")

       
        
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
            
            

       
        
        df = df.with_column(''run_id'',lit(generunid()))

        df = df.with_column(''crtd_dttm'',lit(generatecrtd_dttm()))
        

        

        
        
        file_name=file_name.split(".")[0]+"_"+datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")

        df.write.mode("append").saveAsTable(stage_name.split(".")[0] + "." + target_table)
        
        # #move to success
        df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)
        
        
        
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



CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.NTASDL_RAW.POP6_NA_TRANSACTION_TASKS_PREPROCESSING("PARAM" ARRAY)
RETURNS TABLE ()
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
    # Param=["20240617_service_levels.csv","JPNSDL_RAW.DEV_LOAD_STAGE_ADLS","dev/pop6/transaction/visitdata/service_levels/","SDL_POP6_JP_SERVICE_LEVELS"]
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        
        
        df = session.read\\
            .option("INFER_SCHEMA",True)\\
            .option("field_delimiter", ",")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .option("encoding",''UTF-8'')\\
            .with_metadata("METADATA$FILE_ROW_NUMBER") \\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        df = df.filter(col("METADATA$FILE_ROW_NUMBER") > lit(1))
        
        df = df.drop("METADATA$FILE_ROW_NUMBER")

       
        
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
            
            

       
        
        df = df.with_column(''run_id'',lit(generunid()))

        df = df.with_column(''crtd_dttm'',lit(generatecrtd_dttm()))
        

        

        
        
        file_name=file_name.split(".")[0]+"_"+datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")

        df.write.mode("append").saveAsTable(stage_name.split(".")[0] + "." + target_table)
        
        # #move to success
        df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)
        
        
        
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



CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.NTASDL_RAW.POP6_NA_TRANSACTION_EXCLUSION_PREPROCESSING("PARAM" ARRAY)
RETURNS TABLE ()
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
    # Param=["20240617_service_levels.csv","JPNSDL_RAW.DEV_LOAD_STAGE_ADLS","dev/pop6/transaction/visitdata/service_levels/","SDL_POP6_JP_SERVICE_LEVELS"]
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        
        
        df = session.read\\
            .option("INFER_SCHEMA",True)\\
            .option("field_delimiter", ",")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .option("encoding",''UTF-8'')\\
            .with_metadata("METADATA$FILE_ROW_NUMBER") \\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        df = df.filter(col("METADATA$FILE_ROW_NUMBER") > lit(1))
        
        df = df.drop("METADATA$FILE_ROW_NUMBER")

       
        
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
            
            

       
        
        df = df.with_column(''run_id'',lit(generunid()))

        df = df.with_column(''crtd_dttm'',lit(generatecrtd_dttm()))
        

        

        
        
        file_name=file_name.split(".")[0]+"_"+datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")

        df.write.mode("append").saveAsTable(stage_name.split(".")[0] + "." + target_table)
        
        # #move to success
        df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)
        
        
        
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



CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.NTASDL_RAW.POP6_NA_TRANSACTION_DISPLAYS_PREPROCESSING("PARAM" ARRAY)
RETURNS TABLE ()
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
    # Param=["20240617_service_levels.csv","JPNSDL_RAW.DEV_LOAD_STAGE_ADLS","dev/pop6/transaction/visitdata/service_levels/","SDL_POP6_JP_SERVICE_LEVELS"]
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        
        
        df = session.read\\
            .option("INFER_SCHEMA",True)\\
            .option("field_delimiter", ",")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .option("encoding",''UTF-8'')\\
            .with_metadata("METADATA$FILE_ROW_NUMBER") \\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        df = df.filter(col("METADATA$FILE_ROW_NUMBER") > lit(1))
        
        df = df.drop("METADATA$FILE_ROW_NUMBER")

       
        
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
            
            

       
        
        df = df.with_column(''run_id'',lit(generunid()))

        df = df.with_column(''crtd_dttm'',lit(generatecrtd_dttm()))
        

        

        
        
        file_name=file_name.split(".")[0]+"_"+datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")

        df.write.mode("append").saveAsTable(stage_name.split(".")[0] + "." + target_table)
        
        # #move to success
        df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)
        
        
        
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

==

CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.NTASDL_RAW.POP6_NA_TRANSACTION_SKU_AUDITS_PREPROCESSING("PARAM" ARRAY)
RETURNS TABLE ()
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
    # Param=["20240617_service_levels.csv","JPNSDL_RAW.DEV_LOAD_STAGE_ADLS","dev/pop6/transaction/visitdata/service_levels/","SDL_POP6_JP_SERVICE_LEVELS"]
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        
        
        df = session.read\\
            .option("INFER_SCHEMA",True)\\
            .option("field_delimiter", ",")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .option("encoding",''UTF-8'')\\
            .with_metadata("METADATA$FILE_ROW_NUMBER") \\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        df = df.filter(col("METADATA$FILE_ROW_NUMBER") > lit(1))
        
        df = df.drop("METADATA$FILE_ROW_NUMBER")

       
        
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
            
            

       
        
        df = df.with_column(''run_id'',lit(generunid()))

        df = df.with_column(''crtd_dttm'',lit(generatecrtd_dttm()))
        

        

        
        
        file_name=file_name.split(".")[0]+"_"+datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")

        df.write.mode("append").saveAsTable(stage_name.split(".")[0] + "." + target_table)
        
        # #move to success
        df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)
        
        
        
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



CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.NTASDL_RAW.SDL_POP6_TH_POP_LISTS_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS 'from snowflake.snowpark.functions import col, lit, date_format, current_timestamp, to_date,md5, year, month, concat, format_number, regexp_replace, to_timestamp, when, trim, upper
from snowflake.snowpark.types import StringType, StructType, StructField,DecimalType
import snowflake.snowpark
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz

def main(session: snowpark.Session,Param):
    #Param=["20240422_pop_lists.csv","THASDL_RAW.DEV_LOAD_STAGE_ADLS","dev/pop6","SDL_POP6_TH_POP_LISTS"]
    try:
        file_name = Param[0]
        stage_name = Param[1]
        temp_stage_path = Param[2]
        target_table = Param[3]

        df_schema = StructType([
                    StructField("Status", StringType(), nullable=True),
                    StructField("POP_List", StringType(), nullable=True),
                    StructField("POPDB_ID", StringType(), nullable=True),
                    StructField("POP_Code", StringType(), nullable=True),
                    StructField("POP_Name", StringType(), nullable=True),
                    StructField("Date", StringType(), nullable=True)
                ])
        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "|")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name.split(".")[0])
        
              
        df = df.na.drop("all")
        if df.count() == 0:
            return "No Data in file"

        
        df = df.withColumn("filename", lit(file_name))
        df = df.withColumn("crt_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        df = df.withColumn("run_id", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))

        df1=df
        
        df1=df1.na.fill("",subset=["Status",
                                "POP_List",
                                "POPDB_ID",
                                "POP_Code",
                                "POP_Name"
                                ])

        df1 = df1.withColumn("hash_c",concat(df1.Status,df1.POP_List,df1.POPDB_ID,df1.POP_Code,df1.POP_Name))
        df1=df1.withColumn("hash",md5(col("hash_c")))
        df1=df1.na.replace({"":None},["Status",
                                    "POP_List",
                                    "POPDB_ID",
                                    "POP_Code",
                                    "POP_Name"
                                ])
        snowdf = df1.select(
                           "Status",
                            "POP_List",
                            "POPDB_ID",
                            "POP_Code",
                            "POP_Name",
                            "Date",
                            "filename",
                            "run_id",
                            "crt_dttm",
                            "hash"
                        )
        
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        
        #move to success
        
        
        snowdf.write.mode("append").saveAsTable(stage_name.split(".")[0] + "." + target_table)
        file_name1 = file_name.split(".")[0] + "_" + datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S") + ".csv"
        snowdf.write.copy_into_location("@" + stage_name + "/" + temp_stage_path + "/processed/success/" + formatted_year + "/" + formatted_month + "/" + file_name1, header=True, OVERWRITE=True)
        
        return "Success"

    except KeyError as key_error:
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message

    except pd.errors.MergeError as merge_error:
        error_message = f"DataFrame merging error: {str(merge_error)}"
        return error_message

    except Exception as e:
        error_message = f"Error: {str(e)}"
        return error_message
        
        
        
        
        ';




CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.NTASDL_RAW.SDL_POP6_TH_PRODUCT_LISTS_ALLOCATION_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS 'from snowflake.snowpark.functions import col, lit, date_format, current_timestamp, to_date,md5, year, month, concat, format_number, regexp_replace, to_timestamp, when, trim, upper
from snowflake.snowpark.types import StringType, StructType, StructField,DecimalType
import snowflake.snowpark
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz

def main(session: snowpark.Session,Param):
    #Param=["20240422_product_lists_allocation.csv","THASDL_RAW.DEV_LOAD_STAGE_ADLS","dev/pop6","sdl_pop6_th_product_lists_allocation"]
    try:
        file_name = Param[0]
        stage_name = Param[1]
        temp_stage_path = Param[2]
        target_table = Param[3]

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
                    StructField("Date", StringType(), True)
                ])
        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "|")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name.split(".")[0])
        
              
        df = df.na.drop("all")
        if df.count() == 0:
            return "No Data in file"

        
        df = df.withColumn("filename", lit(file_name))
        df = df.withColumn("crt_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        df = df.withColumn("run_id", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))

        df1=df
        
        df1=df1.na.fill("",subset=["Product_Group_Status",
                                    "Product_Group",
                                    "Product_List_Status",
                                    "Product_List",
                                    "POP_Attribute_ID",
                                    "POP_Attribute",
                                    "POP_Attribute_Value_ID",
                                    "POP_Attribute_Value"
    
                                ])

        df1 = df1.withColumn("hash_c",concat(df1.Product_Group_Status,df1.Product_Group,df1.Product_List_Status,df1.POP_Attribute_ID,df1.POP_Attribute,df1.POP_Attribute_Value_ID,df1.POP_Attribute_Value,df1.Date))
        df1=df1.withColumn("hash",md5(col("hash_c")))
        df1=df1.na.replace({"":None},["Product_Group_Status",
                                    "Product_Group",
                                    "Product_List_Status",
                                    "Product_List"
                                    "POP_Attribute_ID",
                                    "POP_Attribute",
                                    "POP_Attribute_Value_ID",
                                    "POP_Attribute_Value"
                                ])
        snowdf = df1.select(
                          "Product_Group_Status",
                            "Product_Group",
                            "Product_List_Status",
                            "Product_List",
                            "Product_List_Code",
                            "POP_Attribute_ID",
                            "POP_Attribute",
                            "POP_Attribute_Value_ID",
                            "POP_Attribute_Value",
                            "Date",
                            "filename",
                            "run_id",
                            "crt_dttm",
                            "hash"
                        )
        
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        
        #move to success
        
        
        snowdf.write.mode("append").saveAsTable(stage_name.split(".")[0] + "." + target_table)
        file_name1 = file_name.split(".")[0] + "_" + datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S") + ".csv"
        snowdf.write.copy_into_location("@" + stage_name + "/" + temp_stage_path + "/processed/success/" + formatted_year + "/" + formatted_month + "/" + file_name1, header=True, OVERWRITE=True)
        
        return "Success"

    except KeyError as key_error:
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message

    except pd.errors.MergeError as merge_error:
        error_message = f"DataFrame merging error: {str(merge_error)}"
        return error_message

    except Exception as e:
        error_message = f"Error: {str(e)}"
        return error_message
        
        
        
        
        ';


CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.NTASDL_RAW.SDL_POP6_TH_PRODUCT_LISTS_POPS_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS 'from snowflake.snowpark.functions import col, lit, date_format, current_timestamp, to_date,md5, year, month, concat, format_number, regexp_replace, to_timestamp, when, trim, upper
from snowflake.snowpark.types import StringType, StructType, StructField,DecimalType
import snowflake.snowpark
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz

def main(session: snowpark.Session,Param):
    #Param=["20240422_product_lists_pops.csv","THASDL_RAW.DEV_LOAD_STAGE_ADLS","dev/pop6","SDL_POP6_TH_PRODUCT_LISTS_POPS"]
    try:
        file_name = Param[0]
        stage_name = Param[1]
        temp_stage_path = Param[2]
        target_table = Param[3]

        df_schema = StructType([
                    StructField("Product_List", StringType(), True),
                    StructField("Product_List_Code", StringType(), True),
                    StructField("POPDB_ID", StringType(), True),
                    StructField("POP_Code", StringType(), True),
                    StructField("POP_Name", StringType(), True),
                    StructField("Date", StringType(), True)
                ])
        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "|")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name.split(".")[0])
        
              
        df = df.na.drop("all")
        if df.count() == 0:
            return "No Data in file"

        
        df = df.withColumn("filename", lit(file_name))
        df = df.withColumn("crt_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        df = df.withColumn("run_id", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))

        df1=df
        
        df1=df1.na.fill("",subset=["Product_List", "POPDB_ID", "POP_Code", "POP_Name"])

        df1 = df1.withColumn("hash_c",concat(df1.Product_List,df1.POPDB_ID,df1.POP_Code,df1.POP_Name,df1.Date))
        df1=df1.withColumn("hash",md5(col("hash_c")))
        df1=df1.na.replace({"":None},["Product_List", "POPDB_ID", "POP_Code", "POP_Name"])
        snowdf = df1.select(
                          "Product_List", "POPDB_ID", "POP_Code", "POP_Name",
                            "Date",
                            "filename",
                            "run_id",
                            "crt_dttm",
                            "hash"
                        )
        
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        
        #move to success
        
        
        snowdf.write.mode("append").saveAsTable(stage_name.split(".")[0] + "." + target_table)
        file_name1 = file_name.split(".")[0] + "_" + datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S") + ".csv"
        snowdf.write.copy_into_location("@" + stage_name + "/" + temp_stage_path + "/processed/success/" + formatted_year + "/" + formatted_month + "/" + file_name1, header=True, OVERWRITE=True)
        
        return "Success"

    except KeyError as key_error:
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message

    except pd.errors.MergeError as merge_error:
        error_message = f"DataFrame merging error: {str(merge_error)}"
        return error_message

    except Exception as e:
        error_message = f"Error: {str(e)}"
        return error_message
        ';


CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.NTASDL_RAW.SDL_POP6_TH_PRODUCT_LISTS_PRODUCTS_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS 'from snowflake.snowpark.functions import col, lit, date_format, current_timestamp, to_date,md5, year, month, concat, format_number, regexp_replace, to_timestamp, when, trim, upper
from snowflake.snowpark.types import StringType, StructType, StructField,DecimalType
import snowflake.snowpark
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz

def main(session: snowpark.Session,Param):
    #Param=["20240422_product_lists_products.csv","THASDL_RAW.DEV_LOAD_STAGE_ADLS","dev/pop6","sdl_pop6_th_product_lists_products"]
    try:
        file_name = Param[0]
        stage_name = Param[1]
        temp_stage_path = Param[2]
        target_table = Param[3]

        df_schema = StructType([
                                    StructField("Product_List", StringType(), True),
                                    StructField("Product_List_Code", StringType(), True),
                                    StructField("ProductDB_ID", StringType(), True),
                                    StructField("SKU", StringType(), True),
                                    StructField("MSL_Ranking", StringType(), True),
                                    StructField("Date", StringType(), True)
                                ])
        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "|")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name.split(".")[0])
        
              
        df = df.na.drop("all")
        if df.count() == 0:
            return "No Data in file"

        
        df = df.withColumn("filename", lit(file_name))
        df = df.withColumn("crt_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        df = df.withColumn("run_id", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))

        df1=df
        
        df1=df1.na.fill("",subset=["Product_List", "ProductDB_ID", "SKU", "MSL_Ranking"])

        df1 = df1.withColumn("hash_c",concat(df1.Product_List,df1.ProductDB_ID,df1.SKU,df1.MSL_Ranking,df1.Date))
        df1=df1.withColumn("hash",md5(col("hash_c")))
        df1=df1.na.replace({"":None},["Product_List", "ProductDB_ID", "SKU", "MSL_Ranking"])
        snowdf = df1.select(
                          "Product_List", "Product_List_Code","ProductDB_ID", "SKU", "MSL_Ranking",
                            "Date",
                            "filename",
                            "run_id",
                            "crt_dttm",
                            "hash"
                        )
        
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        
        #move to success
        
        
        snowdf.write.mode("append").saveAsTable(stage_name.split(".")[0] + "." + target_table)
        file_name1 = file_name.split(".")[0] + "_" + datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S") + ".csv"
        snowdf.write.copy_into_location("@" + stage_name + "/" + temp_stage_path + "/processed/success/" + formatted_year + "/" + formatted_month + "/" + file_name1, header=True, OVERWRITE=True)
        
        return "Success"

    except KeyError as key_error:
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message

    except pd.errors.MergeError as merge_error:
        error_message = f"DataFrame merging error: {str(merge_error)}"
        return error_message

    except Exception as e:
        error_message = f"Error: {str(e)}"
        return error_message
        ';

=

CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.NTASDL_RAW.SDL_POP6_TH_POPS_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS 'from snowflake.snowpark.functions import col, lit, date_format, current_timestamp, to_date,md5, year, month, concat, format_number, regexp_replace, to_timestamp, when, trim, upper
from snowflake.snowpark.types import StringType, StructType, StructField,DecimalType
import snowflake.snowpark
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz

def main(session: snowpark.Session,Param):
    #Param=["20240423_pops.csv","THASDL_RAW.DEV_LOAD_STAGE_ADLS","dev/pop6","sdl_pop6_th_pops"]
    try:
        file_name = Param[0]
        stage_name = Param[1]
        temp_stage_path = Param[2]
        target_table = Param[3]

        df_schema = StructType([
                                        StructField("Status", StringType(), True),
                                        StructField("POPDB_ID", StringType(), True),
                                        StructField("POP_Code", StringType(), True),
                                        StructField("POP_Name", StringType(), True),
                                        StructField("Address", StringType(), True),
                                        StructField("Longitude", StringType(), True),
                                        StructField("Latitude", StringType(), True),
                                        StructField("Business_Units_ID", StringType(), True),
                                        StructField("Business_Unit_Name", StringType(), True),
                                        StructField("Country", StringType(), True),
                                        StructField("Channel", StringType(), True),
                                        StructField("Retail_Environment_(PS)", StringType(), True),
                                        StructField("Sales_Group_Name", StringType(), True),
                                        StructField("Store_Type", StringType(), True),
                                        StructField("Sales_Group_Code", StringType(), True),
                                        StructField("Customer", StringType(), True),
                                        StructField("Customer_Grade", StringType(), True),
                                        StructField("External_Store_Code", StringType(), True),
                                        StructField("Territory/Region", StringType(), True)
                                    ])
        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "|")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name.split(".")[0])
        
              
        df = df.na.drop("all")
        if df.count() == 0:
            return "No Data in file"

        
        df = df.withColumn("filename", lit(file_name))
        df = df.withColumn("crt_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        df = df.withColumn("run_id", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))

        df1=df
        concat_columns = [
                        "Status", "POPDB_ID", "POP_Code", "POP_Name", "Address", "Longitude", "Latitude",
                        "Business_Units_ID", "Business_Unit_Name", "Country", "Channel", "Retail_Environment_(PS)",
                        "Sales_Group_Name", "Sales_Group_Code", "Customer", "Customer_Grade",
                        "External_Store_Code", "Territory/Region"
                    ]
        
        df1=df1.na.fill("",subset=concat_columns)


       # Concatenate the columns
        df1 = df1.withColumn("hash_c", concat(*[df1[col] for col in concat_columns]))

        #df1 = df1.withColumn("hash_c",concat(df1.Product_List,df1.ProductDB_ID,df1.SKU,df1.MSL_Ranking,df1.Date))
        df1=df1.withColumn("hash",md5(col("hash_c")))
        df1=df1.na.replace({"":None},concat_columns)
        snowdf = df1.select(
                          "Status", "POPDB_ID", "POP_Code", "POP_Name", "Address", "Longitude", "Latitude",
                            "Business_Units_ID", "Business_Unit_Name", "Country", "Channel", "Retail_Environment_(PS)",
                            "Sales_Group_Name","Customer", "Sales_Group_Code",  "Customer_Grade",
                            "External_Store_Code", "Territory/Region",
                            "filename",
                            "run_id",
                            "crt_dttm",
                            "hash"
                        )
        
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        
        #move to success
        
        snowdf.write.mode("append").saveAsTable(stage_name.split(".")[0] + "." + target_table)
        #snowdf.write.mode("append").saveAsTable(target_table)
        file_name1 = file_name.split(".")[0] + "_" + datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S") + ".csv"
        snowdf.write.copy_into_location("@" + stage_name + "/" + temp_stage_path + "/processed/success/" + formatted_year + "/" + formatted_month + "/" + file_name1, header=True, OVERWRITE=True)
        
        return "Success"

    except KeyError as key_error:
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message

    except pd.errors.MergeError as merge_error:
        error_message = f"DataFrame merging error: {str(merge_error)}"
        return error_message

    except Exception as e:
        error_message = f"Error: {str(e)}"
        return error_message
        ';



CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.NTASDL_RAW.SDL_POP6_TH_USERS_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS 'from snowflake.snowpark.functions import col, lit, date_format, current_timestamp, to_date,md5, year, month, concat, format_number, regexp_replace, to_timestamp, when, trim, upper
from snowflake.snowpark.types import StringType, StructType, StructField,DecimalType
import snowflake.snowpark
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz

def main(session: snowpark.Session,Param):
    #Param=["20240423_users.csv","THASDL_RAW.DEV_LOAD_STAGE_ADLS","dev/pop6","sdl_pop6_th_users"]
    try:
        file_name = Param[0]
        stage_name = Param[1]
        temp_stage_path = Param[2]
        target_table = Param[3]

        df_schema = StructType([
                                StructField("Status", StringType(), True),
                                StructField("UserDB_ID", StringType(), True),
                                StructField("Username", StringType(), True),
                                StructField("First_Name", StringType(), True),
                                StructField("Last_Name", StringType(), True),
                                StructField("Team", StringType(), True),
                                StructField("Superior_Name", StringType(), True),
                                StructField("Authorisation_Group", StringType(), True),
                                StructField("Email_Address", StringType(), True),
                                StructField("Longitude", StringType(), True),
                                StructField("Latitude", StringType(), True),
                                StructField("Business_Units_ID", StringType(), True),
                                StructField("Business_Unit_Name", StringType(), True)
                            ])
        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "|")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name.split(".")[0])
        
              
        df = df.na.drop("all")
        if df.count() == 0:
            return "No Data in file"

        
        df = df.withColumn("filename", lit(file_name))
        df = df.withColumn("crt_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        df = df.withColumn("run_id", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))

        df1=df
        concat_columns =  [
                            "Status", "UserDB_ID", "Username", "First_Name", "Last_Name", "Team",
                            "Superior_Name", "Authorisation_Group", "Email_Address", "Longitude",
                            "Latitude", "Business_Units_ID", "Business_Unit_Name"
                        ]
                                
        df1=df1.na.fill("",subset=concat_columns)


       # Concatenate the columns
        df1 = df1.withColumn("hash_c", concat(*[df1[col] for col in concat_columns]))

        #df1 = df1.withColumn("hash_c",concat(df1.Product_List,df1.ProductDB_ID,df1.SKU,df1.MSL_Ranking,df1.Date))
        df1=df1.withColumn("hash",md5(col("hash_c")))
        df1=df1.na.replace({"":None},concat_columns)
        snowdf = df1.select(
                        "Status", "UserDB_ID", "Username", "First_Name", "Last_Name", "Team",
                            "Superior_Name", "Authorisation_Group", "Email_Address", "Longitude",
                            "Latitude", "Business_Units_ID", "Business_Unit_Name",
                            "filename",
                            "run_id",
                            "crt_dttm",
                            "hash"
                        )
        
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        
        #move to success
        
        snowdf.write.mode("append").saveAsTable(stage_name.split(".")[0] + "." + target_table)
        #snowdf.write.mode("append").saveAsTable(target_table)
        file_name1 = file_name.split(".")[0] + "_" + datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S") + ".csv"
        snowdf.write.copy_into_location("@" + stage_name + "/" + temp_stage_path + "/processed/success/" + formatted_year + "/" + formatted_month + "/" + file_name1, header=True, OVERWRITE=True)
        
        return "Success"

    except KeyError as key_error:
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message

    except pd.errors.MergeError as merge_error:
        error_message = f"DataFrame merging error: {str(merge_error)}"
        return error_message

    except Exception as e:
        error_message = f"Error: {str(e)}"
        return error_message
        ';




CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.NTASDL_RAW.SDL_POP6_TH_PRODUCTS_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS 'from snowflake.snowpark.functions import col, lit, date_format, current_timestamp, to_date,md5, year, month, concat, format_number, regexp_replace, to_timestamp, when, trim, upper
from snowflake.snowpark.types import StringType, StructType, StructField,DecimalType
import snowflake.snowpark
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz

def main(session: snowpark.Session,Param):
    #Param=["20240423_products.csv","THASDL_RAW.DEV_LOAD_STAGE_ADLS","dev/pop6","sdl_pop6_th_products"]
    try:
        file_name = Param[0]
        stage_name = Param[1]
        temp_stage_path = Param[2]
        target_table = Param[3]

        df_schema = StructType([
                                StructField("Status", StringType(), True),
                                StructField("ProductDB_ID", StringType(), True),
                                StructField("Barcode", StringType(), True),
                                StructField("SKU", StringType(), True),
                                StructField("Unit_Price", StringType(), True),
                                StructField("Display_order", StringType(), True),
                                StructField("Launch_Date", StringType(), True),
                                StructField("Largest_UOM_Quantity", StringType(), True),
                                StructField("Middle_UOM_Quantity", StringType(), True),
                                StructField("Smallest_UOM_Quantity", StringType(), True),
                                StructField("SKU_(English)", StringType(), True),
                                StructField("Company", StringType(), True),
                                StructField("SKU_Code", StringType(), True),
                                StructField("PS_Category", StringType(), True),
                                StructField("PS_Segment", StringType(), True),
                                StructField("PS_Category_Segment", StringType(), True),
                                StructField("Country_L1", StringType(), True),
                                StructField("Regional_Franchise_L2", StringType(), True),
                                StructField("Franchise_L3", StringType(), True),
                                StructField("Brand_L4", StringType(), True),
                                StructField("Sub_Category_L5", StringType(), True),
                                StructField("Platform_L6", StringType(), True),
                                StructField("Variance_L7", StringType(), True),
                                StructField("Pack_Size_L8", StringType(), True)
                            ])
        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "|")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name.split(".")[0])
        
              
        df = df.na.drop("all")
        if df.count() == 0:
            return "No Data in file"

        
        df = df.withColumn("filename", lit(file_name))
        df = df.withColumn("crt_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        df = df.withColumn("run_id", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))

        df1=df
        concat_columns =  [
                        "Status", "ProductDB_ID", "Barcode", "SKU", "Unit_Price", "Display_order",
                        "Launch_Date", "Largest_UOM_Quantity", "Middle_UOM_Quantity", "Smallest_UOM_Quantity",
                        "SKU_(English)", "Company", "SKU_Code", "PS_Category", "PS_Segment", "PS_Category_Segment",
                        "Country_L1", "Regional_Franchise_L2", "Franchise_L3", "Brand_L4", "Sub_Category_L5",
                        "Platform_L6", "Variance_L7", "Pack_Size_L8"
                        ]
        
        df1=df1.na.fill("",subset=concat_columns)


       # Concatenate the columns
        df1 = df1.withColumn("hash_c", concat(*[df1[col] for col in concat_columns]))

        #df1 = df1.withColumn("hash_c",concat(df1.Product_List,df1.ProductDB_ID,df1.SKU,df1.MSL_Ranking,df1.Date))
        df1=df1.withColumn("hash",md5(col("hash_c")))
        df1=df1.na.replace({"":None},concat_columns)
        snowdf = df1.select(
                        "Status", "ProductDB_ID", "Barcode", "SKU", "Unit_Price", "Display_order",
                        "Launch_Date", "Largest_UOM_Quantity", "Middle_UOM_Quantity", "Smallest_UOM_Quantity",
                        "SKU_(English)", "Company", "SKU_Code", "PS_Category", "PS_Segment", "PS_Category_Segment",
                        "Country_L1", "Regional_Franchise_L2", "Franchise_L3", "Brand_L4", "Sub_Category_L5",
                        "Platform_L6", "Variance_L7", "Pack_Size_L8",
                            "filename",
                            "run_id",
                            "crt_dttm",
                            "hash"
                        )
        
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        
        #move to success
        
        snowdf.write.mode("append").saveAsTable(stage_name.split(".")[0] + "." + target_table)
        #snowdf.write.mode("append").saveAsTable(target_table)
        file_name1 = file_name.split(".")[0] + "_" + datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S") + ".csv"
        snowdf.write.copy_into_location("@" + stage_name + "/" + temp_stage_path + "/processed/success/" + formatted_year + "/" + formatted_month + "/" + file_name1, header=True, OVERWRITE=True)
        
        return "Success"

    except KeyError as key_error:
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message

    except pd.errors.MergeError as merge_error:
        error_message = f"DataFrame merging error: {str(merge_error)}"
        return error_message

    except Exception as e:
        error_message = f"Error: {str(e)}"
        return error_message
        ';


CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.JPNSDL_RAW.POP6_JP_MASTER_USERS_PREPROCESSING("PARAM" ARRAY)
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
    # Param=["20240617_users.csv","JPNSDL_RAW.DEV_LOAD_STAGE_ADLS","dev/pop6/master/masterdata/users/","sdl_pop6_jp_users"]
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        
        df_schema = StructType([
                StructField("Status", StringType(), True),
                StructField("USERDB_ID", StringType(), True),
                StructField("Username", StringType(), True),
                StructField("FIRST_NAME", StringType(), True),
                StructField("Last_Name", StringType(), True),
                StructField("Team", StringType(), True),
                StructField("Superior_Name", StringType(), True),
                StructField("Authorisation_Group", StringType(), True),
                StructField("Email_Address", StringType(), True),
                StructField("Longitude", StringType(), True),
                StructField("Latitude", StringType(), True),
                StructField("Business_Units_ID", StringType(), True),
                StructField("Business_Unit_Name", StringType(), True)
                
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
            
            

        df = df.with_column(''hashkey'',md5(concat(when(is_null(col(''status'')),lit('''')).otherwise(col(''status'')),\\
                                            when(is_null(col(''USERDB_ID'')),lit('''')).otherwise(col(''USERDB_ID'')),\\
                                          when(is_null(col(''Username'')),lit('''')).otherwise(col(''Username'')),\\
                                          when(is_null(col(''FIRST_NAME'')),lit('''')).otherwise(col(''FIRST_NAME'')),\\
                                         when(is_null(col(''Last_Name'')),lit('''')).otherwise(col(''Last_Name'')),\\
                                         when(is_null(col(''Team'')),lit('''')).otherwise(col(''Team'')),\\
                                         when(is_null(col(''Superior_Name'')),lit('''')).otherwise(col(''Superior_Name'')),\\
                                          when(is_null(col(''Authorisation_Group'')),lit('''')).otherwise(col(''Authorisation_Group'')),\\
                                         when(is_null(col(''Email_Address'')),lit('''')).otherwise(col(''Email_Address'')),\\
                                         when(is_null(col(''Longitude'')),lit('''')).otherwise(col(''Longitude'')),\\
                                         when(is_null(col(''Latitude'')),lit('''')).otherwise(col(''Latitude'')),\\
                                         when(is_null(col(''Business_Units_ID'')),lit('''')).otherwise(col(''Business_Units_ID'')),
                                         when(is_null(col(''Business_Unit_Name'')),lit('''')).otherwise(col(''Business_Unit_Name'')))))

        
        df = df.with_column(''run_id'',lit(generunid()))

        df = df.with_column(''crtd_dttm'',lit(generatecrtd_dttm()))
        

        

        
        snowdf= df.select(
                    "status",
                    "userdb_id",
                    "username",
                    "first_name",
                    "last_name",
                    "team",
                    "superior_name",
                    "authorisation_group",
                    "email_address",
                    "longitude",
                    "latitude",
                     "business_units_id",
                    "business_unit_name",
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



CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.JPNSDL_RAW.POP6_JP_MASTER_POPS_PREPROCESSING("PARAM" ARRAY)
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
    # Param=["20240617_pops.csv","JPNSDL_RAW.DEV_LOAD_STAGE_ADLS","dev/pop6/master/masterdata/pops/","sdl_pop6_jp_pops"]
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        
        df_schema = StructType([
                StructField("Status", StringType(), True),
                StructField("popdb_id", StringType(), True),
                StructField("pop_code", StringType(), True),
                StructField("pop_name", StringType(), True),
                StructField("address", StringType(), True),
                StructField("longitude", StringType(), True),
                StructField("latitude", StringType(), True),
                StructField("Business_Units_ID", StringType(), True),
                StructField("Business_Unit_Name", StringType(), True),
                StructField("country", StringType(), True),
                StructField("channel", StringType(), True),
                StructField("retail_environment_ps", StringType(), True),
                StructField("sales_group_name", StringType(), True),
                StructField("customer", StringType(), True),
               StructField("Store_Type", StringType(), True),
                StructField("sales_group_code", StringType(), True),
                StructField("Customer_Grade", StringType(), True),
                StructField("territory", StringType(), True),
                
        
                
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
            
            

        df = df.with_column(''hashkey'',md5(concat(when(is_null(col(''status'')),lit('''')).otherwise(col(''status'')),\\
                                            when(is_null(col(''popdb_id'')),lit('''')).otherwise(col(''popdb_id'')),\\
                                          when(is_null(col(''pop_code'')),lit('''')).otherwise(col(''pop_code'')),\\
                                          when(is_null(col(''pop_name'')),lit('''')).otherwise(col(''pop_name'')),\\
                                         when(is_null(col(''address'')),lit('''')).otherwise(col(''address'')),\\
                                         when(is_null(col(''longitude'')),lit('''')).otherwise(col(''longitude'')),\\
                                         when(is_null(col(''latitude'')),lit('''')).otherwise(col(''latitude'')),\\
                                          when(is_null(col(''Business_Units_ID'')),lit('''')).otherwise(col(''Business_Units_ID'')),\\
                                         when(is_null(col(''Business_Unit_Name'')),lit('''')).otherwise(col(''Business_Unit_Name'')),\\
                                         when(is_null(col(''country'')),lit('''')).otherwise(col(''country'')),\\
                                         when(is_null(col(''channel'')),lit('''')).otherwise(col(''channel'')),\\
                                         when(is_null(col(''retail_environment_ps'')),lit('''')).otherwise(col(''retail_environment_ps'')),\\
                                         when(is_null(col(''sales_group_code'')),lit('''')).otherwise(col(''sales_group_code'')),\\
                                        when(is_null(col(''customer'')),lit('''')).otherwise(col(''customer'')),\\
                                          when(is_null(col(''sales_group_name'')),lit('''')).otherwise(col(''sales_group_name'')),
                                                when(is_null(col(''customer_grade'')),lit('''')).otherwise(col(''customer_grade'')),\\
                                                 when(is_null(col(''Territory'')),lit('''')).otherwise(col(''Territory'')))))
                                         
        
        df = df.with_column(''run_id'',lit(generunid()))

        df = df.with_column(''crtd_dttm'',lit(generatecrtd_dttm()))
        

        

        
        snowdf= df.select(
                    "Status",
                "popdb_id",
                "pop_code",
                "pop_name",
                "address",
                "longitude",
                "latitude",
                "Business_Units_ID",
                "Business_Unit_Name",
                "country",
                "channel",
                "retail_environment_ps",
                "sales_group_name",
                "customer",
                "sales_group_code",
                "customer_grade",
                "Territory",
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
=


CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.JPNSDL_RAW.POP6_JP_MASTER_PRODUCT_LISTS_PRODUCTS_PREPROCESSING("PARAM" ARRAY)
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
                    "Product_List_Code",
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

==

CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.JPNSDL_RAW.POP6_JP_MASTER_POP_LISTS_PREPROCESSING("PARAM" ARRAY)
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
    # Param=["20240617_pop_lists.csv","JPNSDL_RAW.DEV_LOAD_STAGE_ADLS","dev/pop6/master/listdata/pop_lists","sdl_pop6_jp_pop_lists"]
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        
        df_schema = StructType([
                StructField("Status", StringType(), True),
                StructField("pop_list", StringType(), True),
                StructField("popdb_id", StringType(), True),
                StructField("pop_code", StringType(), True),
                StructField("pop_name", StringType(), True),
                StructField("Date", StringType(), True)
                
                
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
            
            

        df = df.with_column(''hashkey'',md5(concat(when(is_null(col(''status'')),lit('''')).otherwise(col(''status'')),\\
                                            when(is_null(col(''pop_list'')),lit('''')).otherwise(col(''pop_list'')),\\
                                          when(is_null(col(''popdb_id'')),lit('''')).otherwise(col(''popdb_id'')),\\
                                          when(is_null(col(''pop_code'')),lit('''')).otherwise(col(''pop_code'')),\\
                                         when(is_null(col(''pop_name'')),lit('''')).otherwise(col(''pop_name'')),\\
                                         when(is_null(col(''date'')),lit('''')).otherwise(col(''date'')))))

        
        df = df.with_column(''run_id'',lit(generunid()))

        df = df.with_column(''crtd_dttm'',lit(generatecrtd_dttm()))
        

        

        
        snowdf= df.select(
                    "status",
                    "pop_list",
                    "popdb_id",
                    "pop_code",
                    "pop_name",
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




CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.JPNSDL_RAW.POP6_JP_MASTER_PRODUCT_LISTS_PRODUCTS_PREPROCESSING("PARAM" ARRAY)
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
                    "Product_List_Code",
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

==
CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.JPNSDL_RAW.POP6_JP_MASTER_PRODUCT_LISTS_POPS_PREPROCESSING("PARAM" ARRAY)
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
    # Param=["20240617_product_lists_pops.csv","JPNSDL_RAW.DEV_LOAD_STAGE_ADLS","dev/pop6/master/listdata/product_lists_pops","sdl_pop6_jp_product_lists_pops"]
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        
        df_schema = StructType([
                StructField("Product_List", StringType(), True),
                StructField("Product_List_Code", StringType(), True),
                StructField("POPDB_ID", StringType(), True),
                StructField("POP_Code", StringType(), True),
                StructField("POP_Name", StringType(), True),
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
            
            

        df = df.with_column(''hashkey'',md5(concat(when(is_null(col(''product_list'')),lit('''')).otherwise(col(''product_list'')),\\
                                            when(is_null(col(''Product_List_Code'')),lit('''')).otherwise(col(''Product_List_Code'')),\\
                                          when(is_null(col(''POPDB_ID'')),lit('''')).otherwise(col(''POPDB_ID'')),\\
                                          when(is_null(col(''POP_Code'')),lit('''')).otherwise(col(''POP_Code'')),\\
                                         when(is_null(col(''POP_Name'')),lit('''')).otherwise(col(''POP_Name'')),\\
                                            when(is_null(col(''date'')),lit('''')).otherwise(col(''date'')))))

        
        df = df.with_column(''run_id'',lit(generunid()))

        df = df.with_column(''crtd_dttm'',lit(generatecrtd_dttm()))
        

        

        
        snowdf= df.select(
                    "product_list",
                    "popdb_id",
                    "pop_code",
                    "pop_name",
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



CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.JPNSDL_RAW.POP6_JP_TRANSACTION_DISPLAY_PLAN_PREPROCESSING("PARAM" ARRAY)
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
    # Param=["20240617_display_plans.csv","JPNSDL_RAW.DEV_LOAD_STAGE_ADLS","dev/pop6/transaction/displaydata/display_plans/","SDL_POP6_JP_DISPLAY_PLANS"]
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        
        
        df = session.read\\
            .option("INFER_SCHEMA",True)\\
            .option("PARSE_HEADER",True)\\
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
            
            

       
        
        df = df.with_column(''run_id'',lit(generunid()))

        df = df.with_column(''crtd_dttm'',lit(generatecrtd_dttm()))
        

        

        
        
        file_name=file_name.split(".")[0]+"_"+datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")

        df.write.mode("append").saveAsTable(stage_name.split(".")[0] + "." + target_table)
        
        # #move to success
        df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)
        
        
        
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



CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.JPNSDL_RAW.POP6_JP_TRANSACTION_GENERAL_AUDITS_PREPROCESSING("PARAM" ARRAY)
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
    # Param=["20240617_general_audits.csv","JPNSDL_RAW.DEV_LOAD_STAGE_ADLS","dev/pop6/transaction/auditdata/general_audit","SDL_POP6_JP_GENERAL_AUDITS"]
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        
        
        df = session.read\\
            .option("INFER_SCHEMA",True)\\
            .option("field_delimiter", ",")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .option("encoding",''UTF-8'')\\
            .with_metadata("METADATA$FILE_ROW_NUMBER") \\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        df = df.filter(col("METADATA$FILE_ROW_NUMBER") > lit(1))
        
        df = df.drop("METADATA$FILE_ROW_NUMBER")

       
        
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
            
            

       
        
        df = df.with_column(''run_id'',lit(generunid()))

        df = df.with_column(''crtd_dttm'',lit(generatecrtd_dttm()))
        

        

        
        
        file_name=file_name.split(".")[0]+"_"+datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")

        df.write.mode("append").saveAsTable(stage_name.split(".")[0] + "." + target_table)
        
        # #move to success
        df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)
        
        
        
        return df

    
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



CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.JPNSDL_RAW.POP6_JP_TRANSACTION_PRODUCT_ATTRIBUTE_AUDITS_PREPROCESSING("PARAM" ARRAY)
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
    # Param=["20240617_product_attribute_audits.csv","JPNSDL_RAW.DEV_LOAD_STAGE_ADLS","dev/pop6/transaction/auditdata/product_attribute_audits","SDL_POP6_JP_PRODUCT_ATTRIBUTE_AUDITS"]
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        
        
        df = session.read\\
            .option("INFER_SCHEMA",True)\\
            .option("field_delimiter", ",")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .option("encoding",''UTF-8'')\\
            .with_metadata("METADATA$FILE_ROW_NUMBER") \\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        df = df.filter(col("METADATA$FILE_ROW_NUMBER") > lit(1))
        
        df = df.drop("METADATA$FILE_ROW_NUMBER")
       
        
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
            
            

       
        
        df = df.with_column(''run_id'',lit(generunid()))

        df = df.with_column(''crtd_dttm'',lit(generatecrtd_dttm()))
        

        

        
        
        file_name=file_name.split(".")[0]+"_"+datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")

        df.write.mode("append").saveAsTable(stage_name.split(".")[0] + "." + target_table)
        
        # #move to success
        df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)
        
        
        
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

=

CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.JPNSDL_RAW.POP6_JP_TRANSACTION_SKU_AUDITS_PREPROCESSING("PARAM" ARRAY)
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
    # Param=["20240617_display_plans.csv","JPNSDL_RAW.DEV_LOAD_STAGE_ADLS","dev/pop6/transaction/displaydata/display_plans/","SDL_POP6_JP_DISPLAY_PLANS"]
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        
        
        df = session.read\\
            .option("INFER_SCHEMA",True)\\
            .option("field_delimiter", ",")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .option("encoding",''UTF-8'')\\
            .with_metadata("METADATA$FILE_ROW_NUMBER") \\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        df = df.filter(col("METADATA$FILE_ROW_NUMBER") > lit(1))
        
        df = df.drop("METADATA$FILE_ROW_NUMBER")

       
        
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
            
            

       
        
        df = df.with_column(''run_id'',lit(generunid()))

        df = df.with_column(''crtd_dttm'',lit(generatecrtd_dttm()))
        

        

        
        
        file_name=file_name.split(".")[0]+"_"+datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")

        df.write.mode("append").saveAsTable(stage_name.split(".")[0] + "." + target_table)
        
        # #move to success
        df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)
        
        
        
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




CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.JPNSDL_RAW.POP6_JP_TRANSACTION_EXCLUSIONDATA_PREPROCESSING("PARAM" ARRAY)
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
    # Param=["20210223_pop6_jp_data_exclusion.csv","JPNSDL_RAW.DEV_LOAD_STAGE_ADLS","dev/pop6/transaction/exclusiondata/","SDL_POP6_JP_EXCLUSIONDATA"]
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        
        
        df = session.read\\
            .option("INFER_SCHEMA",True)\\
            .option("field_delimiter", ",")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .option("encoding",''UTF-8'')\\
            .with_metadata("METADATA$FILE_ROW_NUMBER") \\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        df = df.filter(col("METADATA$FILE_ROW_NUMBER") > lit(1))
        
        df = df.drop("METADATA$FILE_ROW_NUMBER")

       
        
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
            
            

       
        
        df = df.with_column(''run_id'',lit(generunid()))

        df = df.with_column(''crtd_dttm'',lit(generatecrtd_dttm()))
        

        

        
        
        file_name=file_name.split(".")[0]+"_"+datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")

        df.write.mode("append").saveAsTable(stage_name.split(".")[0] + "." + target_table)
        
        # #move to success
        df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)
        
        
        
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

==

CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.JPNSDL_RAW.POP6_JP_TRANSACTION_DISPLAY_PLANS_PREPROCESSING("PARAM" ARRAY)
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
    # Param=["20240617_display_plans.csv","JPNSDL_RAW.DEV_LOAD_STAGE_ADLS","dev/pop6/transaction/displaydata/display_plans/","SDL_POP6_JP_DISPLAY_PLANS"]
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        
        
        df = session.read\\
            .option("INFER_SCHEMA",True)\\
            .option("field_delimiter", ",")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .option("encoding",''UTF-8'')\\
            .with_metadata("METADATA$FILE_ROW_NUMBER") \\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        df = df.filter(col("METADATA$FILE_ROW_NUMBER") > lit(1))
        
        df = df.drop("METADATA$FILE_ROW_NUMBER")

       
        
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
            
            

       
        
        df = df.with_column(''run_id'',lit(generunid()))

        df = df.with_column(''crtd_dttm'',lit(generatecrtd_dttm()))
        

        

        
        
        file_name=file_name.split(".")[0]+"_"+datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")

        df.write.mode("append").saveAsTable(stage_name.split(".")[0] + "." + target_table)
        
        # #move to success
        df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)
        
        
        
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




CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.JPNSDL_RAW.POP6_JP_TRANSACTION_DISPLAY_PREPROCESSING("PARAM" ARRAY)
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
    # Param=["20240617_displays.csv","JPNSDL_RAW.DEV_LOAD_STAGE_ADLS","dev/pop6/transaction/displaydata/displays/","SDL_POP6_JP_DISPLAYS"]
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        
        
        df = session.read\\
            .option("INFER_SCHEMA",True)\\
            .option("field_delimiter", ",")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .option("encoding",''UTF-8'')\\
            .with_metadata("METADATA$FILE_ROW_NUMBER") \\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        df = df.filter(col("METADATA$FILE_ROW_NUMBER") > lit(1))
        
        df = df.drop("METADATA$FILE_ROW_NUMBER")

       
        
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
            
            

       
        
        df = df.with_column(''run_id'',lit(generunid()))

        df = df.with_column(''crtd_dttm'',lit(generatecrtd_dttm()))
        

        

        
        
        file_name=file_name.split(".")[0]+"_"+datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")

        df.write.mode("append").saveAsTable(stage_name.split(".")[0] + "." + target_table)
        
        # #move to success
        df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)
        
        
        
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





CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.JPNSDL_RAW.POP6_JP_TRANSACTION_PROMOTIONS_PREPROCESSING("PARAM" ARRAY)
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
    # Param=["20240617_promotions.csv","JPNSDL_RAW.DEV_LOAD_STAGE_ADLS","dev/pop6/transaction/promotiondata/promotion/","SDL_POP6_JP_PROMOTIONS"]
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        
        
        df = session.read\\
            .option("INFER_SCHEMA",True)\\
            .option("field_delimiter", ",")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .option("encoding",''UTF-8'')\\
            .with_metadata("METADATA$FILE_ROW_NUMBER") \\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        df = df.filter(col("METADATA$FILE_ROW_NUMBER") > lit(1))
        
        df = df.drop("METADATA$FILE_ROW_NUMBER")

       
        
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
            
            

       
        
        df = df.with_column(''run_id'',lit(generunid()))

        df = df.with_column(''crtd_dttm'',lit(generatecrtd_dttm()))
        

        

        
        
        file_name=file_name.split(".")[0]+"_"+datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")

        df.write.mode("append").saveAsTable(stage_name.split(".")[0] + "." + target_table)
        
        # #move to success
        df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)
        
        
        
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





CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.JPNSDL_RAW.POP6_JP_TRANSACTION_PROMOTIONS_PLANS_PREPROCESSING("PARAM" ARRAY)
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
    # Param=["20240617_promotion_plans.csv","JPNSDL_RAW.DEV_LOAD_STAGE_ADLS","dev/pop6/transaction/promotiondata/promotion_plans/","SDL_POP6_JP_PROMOTIONS_PLANS"]
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        
        
        df = session.read\\
            .option("INFER_SCHEMA",True)\\
            .option("field_delimiter", ",")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .option("encoding",''UTF-8'')\\
            .with_metadata("METADATA$FILE_ROW_NUMBER") \\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        df = df.filter(col("METADATA$FILE_ROW_NUMBER") > lit(1))
        
        df = df.drop("METADATA$FILE_ROW_NUMBER")

       
        
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
            
            

       
        
        df = df.with_column(''run_id'',lit(generunid()))

        df = df.with_column(''crtd_dttm'',lit(generatecrtd_dttm()))
        

        

        
        
        file_name=file_name.split(".")[0]+"_"+datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")

        df.write.mode("append").saveAsTable(stage_name.split(".")[0] + "." + target_table)
        
        # #move to success
        df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)
        
        
        
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





CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.JPNSDL_RAW.POP6_JP_TRANSACTION_TASKS_PREPROCESSING("PARAM" ARRAY)
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
    # Param=["20240617_tasks.csv","JPNSDL_RAW.DEV_LOAD_STAGE_ADLS","dev/pop6/transaction/taskdata/tasks","SDL_POP6_JP_TASKS"]
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        
        
        df = session.read\\
            .option("INFER_SCHEMA",True)\\
            .option("field_delimiter", ",")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .option("encoding",''UTF-8'')\\
            .with_metadata("METADATA$FILE_ROW_NUMBER") \\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        df = df.filter(col("METADATA$FILE_ROW_NUMBER") > lit(1))
        
        df = df.drop("METADATA$FILE_ROW_NUMBER")

       
        
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
            
            

       
        
        df = df.with_column(''run_id'',lit(generunid()))

        df = df.with_column(''crtd_dttm'',lit(generatecrtd_dttm()))
        

        

        
        
        file_name=file_name.split(".")[0]+"_"+datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")

        df.write.mode("append").saveAsTable(stage_name.split(".")[0] + "." + target_table)
        
        # #move to success
        df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)
        
        
        
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




CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.JPNSDL_RAW.POP6_JP_TRANSACTION_EXECUTE_VISITS_PREPROCESSING("PARAM" ARRAY)
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
    # Param=["20240617_executed_visits.csv","JPNSDL_RAW.DEV_LOAD_STAGE_ADLS","dev/pop6/transaction/visitdata/executed_visits/","SDL_POP6_JP_EXECUTE_VISIT"]
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        
        
        df = session.read\\
            .option("INFER_SCHEMA",True)\\
            .option("field_delimiter", ",")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .option("encoding",''UTF-8'')\\
            .with_metadata("METADATA$FILE_ROW_NUMBER") \\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        df = df.filter(col("METADATA$FILE_ROW_NUMBER") > lit(1))
        
        df = df.drop("METADATA$FILE_ROW_NUMBER")

       
        
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
            
            

       
        
        df = df.with_column(''run_id'',lit(generunid()))

        df = df.with_column(''crtd_dttm'',lit(generatecrtd_dttm()))
        

        

        
        
        file_name=file_name.split(".")[0]+"_"+datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")

        df.write.mode("append").saveAsTable(stage_name.split(".")[0] + "." + target_table)
        
        # #move to success
        df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)
        
        
        
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




CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.JPNSDL_RAW.POP6_JP_TRANSACTION_PLANNED_VISITS_PREPROCESSING("PARAM" ARRAY)
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
    # Param=["20240617_planned_visits.csv","JPNSDL_RAW.DEV_LOAD_STAGE_ADLS","dev/pop6/transaction/visitdata/planned_visits/","SDL_POP6_JP_PLANNED_VISITS"]
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        
        
        df = session.read\\
            .option("INFER_SCHEMA",True)\\
            .option("field_delimiter", ",")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .option("encoding",''UTF-8'')\\
            .with_metadata("METADATA$FILE_ROW_NUMBER") \\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        df = df.filter(col("METADATA$FILE_ROW_NUMBER") > lit(1))
        
        df = df.drop("METADATA$FILE_ROW_NUMBER")


       
        
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
            
            

       
        
        df = df.with_column(''run_id'',lit(generunid()))

        df = df.with_column(''crtd_dttm'',lit(generatecrtd_dttm()))
        

        

        
        
        file_name=file_name.split(".")[0]+"_"+datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")

        df.write.mode("append").saveAsTable(stage_name.split(".")[0] + "." + target_table)
        
        # #move to success
        df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)
        
        
        
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




CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.JPNSDL_RAW.POP6_JP_TRANSACTION_SERVICE_LEVELS_PREPROCESSING("PARAM" ARRAY)
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
    # Param=["20240617_service_levels.csv","JPNSDL_RAW.DEV_LOAD_STAGE_ADLS","dev/pop6/transaction/visitdata/service_levels/","SDL_POP6_JP_SERVICE_LEVELS"]
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        
        
        df = session.read\\
            .option("INFER_SCHEMA",True)\\
            .option("field_delimiter", ",")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .option("encoding",''UTF-8'')\\
            .with_metadata("METADATA$FILE_ROW_NUMBER") \\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        df = df.filter(col("METADATA$FILE_ROW_NUMBER") > lit(1))
        
        df = df.drop("METADATA$FILE_ROW_NUMBER")

       
        
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
            
            

       
        
        df = df.with_column(''run_id'',lit(generunid()))

        df = df.with_column(''crtd_dttm'',lit(generatecrtd_dttm()))
        

        

        
        
        file_name=file_name.split(".")[0]+"_"+datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")

        df.write.mode("append").saveAsTable(stage_name.split(".")[0] + "." + target_table)
        
        # #move to success
        df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)
        
        
        
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

-
CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.JPNSDL_RAW.POP6_JP_MASTER_PRODUCT_LISTS_ALLOCATION_PREPROCESSING("PARAM" ARRAY)
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
                    "Product_List_code",
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

==

CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.JPNSDL_RAW.POP6_JP_MASTER_PRODUCTS_PREPROCESSING("PARAM" ARRAY)
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
    # Param=["20240617_products.csv","JPNSDL_RAW.DEV_LOAD_STAGE_ADLS","dev/pop6/master/masterdata/products/","sdl_pop6_jp_products"]
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        
        df_schema = StructType([
                StructField("Status", StringType(), True),
                StructField("productdb_id", StringType(), True),
                StructField("barcode", StringType(), True),
                StructField("sku", StringType(), True),
                StructField("unit_price", StringType(), True),
                StructField("display_order", StringType(), True),
                StructField("launch_date", StringType(), True),
                StructField("largest_uom_quantity", StringType(), True),
                StructField("middle_uom_quantity", StringType(), True),
                StructField("smallest_uom_quantity", StringType(), True),
                StructField("sku_english", StringType(), True),
                StructField("company", StringType(), True),
                StructField("sku_code", StringType(), True),
                StructField("ps_category", StringType(), True),
                StructField("ps_segment", StringType(), True),
                StructField("ps_category_segment", StringType(), True),
                StructField("country_l1", StringType(), True),
                StructField("regional_franchise_l2", StringType(), True),
                StructField("franchise_l3", StringType(), True),
                StructField("brand_l4", StringType(), True),
                StructField("sub_category_l5", StringType(), True),
                StructField("platform_level_l6", StringType(), True),
                StructField("variance_l7", StringType(), True),
                StructField("pack_size_l8", StringType(), True)
                
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
            
            

        df = df.with_column(''hashkey'',md5(concat(when(is_null(col(''status'')),lit('''')).otherwise(col(''status'')),\\
                                            when(is_null(col(''productdb_id'')),lit('''')).otherwise(col(''productdb_id'')),\\
                                          when(is_null(col(''barcode'')),lit('''')).otherwise(col(''barcode'')),\\
                                          when(is_null(col(''sku'')),lit('''')).otherwise(col(''sku'')),\\
                                         when(is_null(col(''unit_price'')),lit('''')).otherwise(col(''unit_price'')),\\
                                         when(is_null(col(''display_order'')),lit('''')).otherwise(col(''display_order'')),\\
                                         when(is_null(col(''launch_date'')),lit('''')).otherwise(col(''launch_date'')),\\
                                          when(is_null(col(''largest_uom_quantity'')),lit('''')).otherwise(col(''largest_uom_quantity'')),\\
                                         when(is_null(col(''middle_uom_quantity'')),lit('''')).otherwise(col(''middle_uom_quantity'')),\\
                                         when(is_null(col(''smallest_uom_quantity'')),lit('''')).otherwise(col(''smallest_uom_quantity'')),\\
                                         when(is_null(col(''company'')),lit('''')).otherwise(col(''company'')),\\
                                         when(is_null(col(''sku_english'')),lit('''')).otherwise(col(''sku_english'')),\\
                                         when(is_null(col(''sku_code'')),lit('''')).otherwise(col(''sku_code'')),\\
                                        when(is_null(col(''ps_category'')),lit('''')).otherwise(col(''ps_category'')),\\
                                          when(is_null(col(''ps_segment'')),lit('''')).otherwise(col(''ps_segment'')),\\
                                         when(is_null(col(''ps_category_segment'')),lit('''')).otherwise(col(''ps_category_segment'')),\\
                                         when(is_null(col(''country_l1'')),lit('''')).otherwise(col(''country_l1'')),\\
                                         when(is_null(col(''regional_franchise_l2'')),lit('''')).otherwise(col(''regional_franchise_l2'')),\\
                                          when(is_null(col(''franchise_l3'')),lit('''')).otherwise(col(''franchise_l3'')),\\
                                         when(is_null(col(''brand_l4'')),lit('''')).otherwise(col(''brand_l4'')),\\
                                         when(is_null(col(''sub_category_l5'')),lit('''')).otherwise(col(''sub_category_l5'')),\\
                                         when(is_null(col(''platform_level_l6'')),lit('''')).otherwise(col(''platform_level_l6'')),\\
                                         when(is_null(col(''variance_l7'')),lit('''')).otherwise(col(''variance_l7'')),\\
                                         when(is_null(col(''pack_size_l8'')),lit('''')).otherwise(col(''pack_size_l8'')))))

        
        df = df.with_column(''run_id'',lit(generunid()))

        df = df.with_column(''crtd_dttm'',lit(generatecrtd_dttm()))
        

        

        
        snowdf= df.select(
                   "Status", 
                "productdb_id", 
                "barcode", 
                "sku", 
                "unit_price", 
                "display_order", 
                "launch_date", 
                "largest_uom_quantity", 
                "middle_uom_quantity", 
                "smallest_uom_quantity", 
                "company", 
                "sku_english", 
                "sku_code", 
                "ps_category", 
                "ps_segment", 
                "ps_category_segment", 
                "country_l1", 
                "regional_franchise_l2", 
                "franchise_l3", 
                "brand_l4", 
                "sub_category_l5", 
                "platform_level_l6", 
                "variance_l7", 
                "pack_size_l8",
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




CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.JPNSDL_RAW.POP6_JP_MASTER_PRODUCT_LISTS_POPS_PREPROCESSING("PARAM" ARRAY)
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
    # Param=["20240617_product_lists_pops.csv","JPNSDL_RAW.DEV_LOAD_STAGE_ADLS","dev/pop6/master/listdata/product_lists_pops","sdl_pop6_jp_product_lists_pops"]
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        
        df_schema = StructType([
                StructField("Product_List", StringType(), True),
                StructField("Product_List_Code", StringType(), True),
                StructField("POPDB_ID", StringType(), True),
                StructField("POP_Code", StringType(), True),
                StructField("POP_Name", StringType(), True),
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
            
            

        df = df.with_column(''hashkey'',md5(concat(when(is_null(col(''product_list'')),lit('''')).otherwise(col(''product_list'')),\\
                                            when(is_null(col(''Product_List_Code'')),lit('''')).otherwise(col(''Product_List_Code'')),\\
                                          when(is_null(col(''POPDB_ID'')),lit('''')).otherwise(col(''POPDB_ID'')),\\
                                          when(is_null(col(''POP_Code'')),lit('''')).otherwise(col(''POP_Code'')),\\
                                         when(is_null(col(''POP_Name'')),lit('''')).otherwise(col(''POP_Name'')),\\
                                            when(is_null(col(''date'')),lit('''')).otherwise(col(''date'')))))

        
        df = df.with_column(''run_id'',lit(generunid()))

        df = df.with_column(''crtd_dttm'',lit(generatecrtd_dttm()))
        

        

        
        snowdf= df.select(
                    "product_list",
                    "popdb_id",
                    "pop_code",
                    "pop_name",
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

==

CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.SGPSDL_RAW.POP6_SG_TRANS_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them.

import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import lit,col
from snowflake.snowpark.types import  StringType, StructType, StructField,IntegerType
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz
from datetime import datetime, timedelta



def main(session: snowpark.Session,Param): 
       
    try :
           
        #Param=[''20220112_pop6_hk_data_exclusion.csv'',''NTASDL_RAW.DEV_LOAD_STAGE_ADLS_HKG'',''dev/pop6/exclusionsdata/'',''sdl_pop6_hk_exclusion'']

        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        sch_name        = stage_name.split(''.'')[0]
        target_table    = sch_name+"."+Param[3]
        
        df = session.read\\
            .option("INFER_SCHEMA", True)\\
            .option("skip_header",0)\\
            .option("field_delimiter", ",")\\
            .option("field_optionally_enclosed_by", "\\"") \\
            .option("encoding","UTF-8") \\
            .option("trim_space", True) \\
            .with_metadata("METADATA$FILE_ROW_NUMBER") \\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
        
       
        df = df.filter(col("METADATA$FILE_ROW_NUMBER") > lit(1))
        df = df.drop("METADATA$FILE_ROW_NUMBER")
        
        df=df.na.drop("all")
        if df.count()==0:
            return "No Data in file"

       
        
        
     
        df = df.with_column("FILE_NAME",lit( file_name ))
        df = df.with_column("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        df = df.with_column("CRTD_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
       
        
        # Load Data to the target table
        df.write.mode("append").saveAsTable(target_table)
       
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        #move to success
        file_name=file_name.split(".")[0]+''_''+datetime.now().strftime("%Y%m%d%H%M%S")
        df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)

        
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



CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.THASDL_RAW.POP6_TH_TRANS_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them.

import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import lit,col
from snowflake.snowpark.types import  StringType, StructType, StructField,IntegerType
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz
from datetime import datetime, timedelta



def main(session: snowpark.Session,Param): 
       
    try :
           
        #Param=[''20220112_pop6_hk_data_exclusion.csv'',''NTASDL_RAW.DEV_LOAD_STAGE_ADLS_HKG'',''dev/pop6/exclusionsdata/'',''sdl_pop6_hk_exclusion'']

        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        sch_name        = stage_name.split(''.'')[0]
        target_table    = sch_name+"."+Param[3]
        
        df = session.read\\
            .option("INFER_SCHEMA", True)\\
            .option("skip_header",0)\\
            .option("field_delimiter", ",")\\
            .option("field_optionally_enclosed_by", "\\"") \\
            .option("encoding","UTF-8") \\
            .option("trim_space", True) \\
            .with_metadata("METADATA$FILE_ROW_NUMBER") \\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
        
       
        df = df.filter(col("METADATA$FILE_ROW_NUMBER") > lit(1))
        df = df.drop("METADATA$FILE_ROW_NUMBER")
        
        df=df.na.drop("all")
        if df.count()==0:
            return "No Data in file"

       
        
        
     
        df = df.with_column("FILE_NAME",lit( file_name ))
        df = df.with_column("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        df = df.with_column("CRTD_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
       
        
        # Load Data to the target table
        df.write.mode("append").saveAsTable(target_table)
       
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        #move to success
        file_name=file_name.split(".")[0]+''_''+datetime.now().strftime("%Y%m%d%H%M%S")
        df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)

        
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


CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.SGPSDL_RAW.SG_POP6_POPS_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS 'from snowflake.snowpark.functions import col, lit, date_format, current_timestamp, to_date,md5, year, month, concat, format_number, regexp_replace, to_timestamp, when, trim, upper
from snowflake.snowpark.types import StringType, StructType, StructField,DecimalType
import snowflake.snowpark
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz

def main(session: snowpark.Session,Param):
    #Param=["20240620_pops.csv","SGPSDL_RAW.DEV_LOAD_STAGE_ADLS","dev/pop6/master/masterdata","sdl_pop6_sg_pops"]    
    try:
        file_name = Param[0]
        stage_name = Param[1]
        temp_stage_path = Param[2]
        target_table = Param[3]

        df_schema = StructType([
                    StructField("status", StringType(), True),
                    StructField("popdb_id", StringType(), True),
                    StructField("pop_code", StringType(), True),
                    StructField("pop_name", StringType(), True),
                    StructField("address", StringType(), True),
                    StructField("longitude", StringType(), True),
                    StructField("latitude", StringType(), True),
                    StructField("Business_Units_ID", StringType(), True),
                    StructField("Business_Unit_Name", StringType(), True),
                    StructField("country", StringType(), True),
                    StructField("channel", StringType(), True),
                    StructField("retail_environment_ps", StringType(), True),
                    StructField("sales_group_name", StringType(), True),
                    StructField("customer", StringType(), True),
                    StructField("Store_Type", StringType(), True),
                    StructField("sales_group_code", StringType(), True),
                    StructField("customer_grade", StringType(), True),
                    StructField("Territory", StringType(), True)
                ])
        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "|")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name.split(".")[0])
        
              
        df = df.na.drop("all")
        if df.count() == 0:
            return "No Data in file"

        
        df = df.withColumn("filename", lit(file_name))
        df = df.withColumn("crt_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        df = df.withColumn("run_id", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))

        df1=df
        concat_columns = [
                          "status",
                            "popdb_id",
                            "pop_code",
                            "pop_name",
                            "address",
                            "longitude",
                            "latitude",
                            "Business_Units_ID",
                            "Business_Unit_Name",
                            "country",
                            "channel",
                            "retail_environment_ps",
                            "sales_group_code",
                            "customer",
                            "sales_group_name",
                            "customer_grade",
                            "Territory"
                    ]
        
        df1=df1.na.fill("",subset=concat_columns)


       # Concatenate the columns
        df1 = df1.withColumn("hash_c", concat(*[df1[col] for col in concat_columns]))

        #df1 = df1.withColumn("hash_c",concat(df1.Product_List,df1.ProductDB_ID,df1.SKU,df1.MSL_Ranking,df1.Date))
        df1=df1.withColumn("hash",md5(col("hash_c")))
        df1=df1.na.replace({"":None},concat_columns)
        snowdf = df1.select(
                           "status",
                            "popdb_id",
                            "pop_code",
                            "pop_name",
                            "address",
                            "longitude",
                            "latitude",
                            "Business_Units_ID",
                            "Business_Unit_Name",
                            "country",
                            "channel",
                            "retail_environment_ps",
                            "sales_group_name",
                            "customer",
                            "sales_group_code",
                            "customer_grade",
                            "Territory",
                            "filename",
                            "run_id",
                            "crt_dttm",
                            "hash"
                        )
        
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        
        #move to success
        
        snowdf.write.mode("append").saveAsTable(stage_name.split(".")[0] + "." + target_table)
        #snowdf.write.mode("append").saveAsTable(target_table)
        file_name1 = file_name.split(".")[0] + "_" + datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        snowdf.write.copy_into_location("@" + stage_name + "/" + temp_stage_path + "/processed/success/" + formatted_year + "/" + formatted_month + "/" + file_name1, header=True, OVERWRITE=True)
        
        return "Success"

    except KeyError as key_error:
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message

    except pd.errors.MergeError as merge_error:
        error_message = f"DataFrame merging error: {str(merge_error)}"
        return error_message

    except Exception as e:
        error_message = f"Error: {str(e)}"
        return error_message';



CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.SGPSDL_RAW.SG_POP6_PRODUCTS_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS 'from snowflake.snowpark.functions import col, lit, date_format, current_timestamp, to_date,md5, year, month, concat, format_number, regexp_replace, to_timestamp, when, trim, upper
from snowflake.snowpark.types import StringType, StructType, StructField,DecimalType
import snowflake.snowpark
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz

def main(session: snowpark.Session,Param):
    #Param=["20240620_products.csv","SGPSDL_RAW.DEV_LOAD_STAGE_ADLS","dev/pop6/master/masterdata","sdl_pop6_sg_pops"]    
    try:
        file_name = Param[0]
        stage_name = Param[1]
        temp_stage_path = Param[2]
        target_table = Param[3]

        df_schema = StructType([
                    StructField("Status", StringType(), True),
                    StructField("ProductDB_ID", StringType(), True),
                    StructField("Barcode", StringType(), True),
                    StructField("SKU", StringType(), True),
                    StructField("Unit_Price", StringType(), True),
                    StructField("Display_order", StringType(), True),
                    StructField("Launch_Date", StringType(), True),
                    StructField("Largest_UOM_Quantity", StringType(), True),
                    StructField("Middle_UOM_Quantity", StringType(), True),
                    StructField("Smallest_UOM_Quantity", StringType(), True),
                    StructField("SKU_English", StringType(), True),
                    StructField("Company", StringType(), True),
                    StructField("SKU_Code", StringType(), True),
                    StructField("PS_Category", StringType(), True),
                    StructField("PS_Segment", StringType(), True),
                    StructField("PS_Category_Segment", StringType(), True),
                    StructField("Country_L1", StringType(), True),
                    StructField("Regional_Franchise_L2", StringType(), True),
                    StructField("Franchise_L3", StringType(), True),
                    StructField("Brand_L4", StringType(), True),
                    StructField("Sub_Category_L5", StringType(), True),
                    StructField("Platform_L6", StringType(), True),
                    StructField("Variance_L7", StringType(), True),
                    StructField("Pack_size_L8", StringType(), True)
                ])
        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "|")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name.split(".")[0])
        
              
        df = df.na.drop("all")
        if df.count() == 0:
            return "No Data in file"

        
        df = df.withColumn("filename", lit(file_name))
        df = df.withColumn("crt_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        df = df.withColumn("run_id", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))

        df1=df
        concat_columns = [
                           "status",
                        "productdb_id",
                        "barcode",
                        "sku",
                        "unit_price",
                        "display_order",
                        "launch_date",
                        "largest_uom_quantity",
                        "middle_uom_quantity",
                        "smallest_uom_quantity",
                        "company",
                        "sku_english",
                        "sku_code",
                        "ps_category",
                        "ps_segment",
                        "ps_category_segment",
                        "country_l1",
                        "regional_franchise_l2",
                        "franchise_l3",
                        "brand_l4",
                        "sub_category_l5",
                        "platform_l6",
                        "variance_l7",
                        "pack_size_l8"
                    ]
        
        df1=df1.na.fill("",subset=concat_columns)


       # Concatenate the columns
        df1 = df1.withColumn("hash_c", concat(*[df1[col] for col in concat_columns]))

        #df1 = df1.withColumn("hash_c",concat(df1.Product_List,df1.ProductDB_ID,df1.SKU,df1.MSL_Ranking,df1.Date))
        df1=df1.withColumn("hash",md5(col("hash_c")))
        df1=df1.na.replace({"":None},concat_columns)
        snowdf = df1.select(
                            "status",
                        "productdb_id",
                        "barcode",
                        "sku",
                        "unit_price",
                        "display_order",
                        "launch_date",
                        "largest_uom_quantity",
                        "middle_uom_quantity",
                        "smallest_uom_quantity",
                        "company",
                        "sku_english",
                        "sku_code",
                        "ps_category",
                        "ps_segment",
                        "ps_category_segment",
                        "country_l1",
                        "regional_franchise_l2",
                        "franchise_l3",
                        "brand_l4",
                        "sub_category_l5",
                        "platform_l6",
                        "variance_l7",
                            "filename",
                            "run_id",
                            "crt_dttm",
                            "hash"
                        )
        
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        
        #move to success
        
        snowdf.write.mode("append").saveAsTable(stage_name.split(".")[0] + "." + target_table)
        #snowdf.write.mode("append").saveAsTable(target_table)
        file_name1 = file_name.split(".")[0] + "_" + datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        snowdf.write.copy_into_location("@" + stage_name + "/" + temp_stage_path + "/processed/success/" + formatted_year + "/" + formatted_month + "/" + file_name1, header=True, OVERWRITE=True)
        
        return "Success"

    except KeyError as key_error:
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message

    except pd.errors.MergeError as merge_error:
        error_message = f"DataFrame merging error: {str(merge_error)}"
        return error_message

    except Exception as e:
        error_message = f"Error: {str(e)}"
        return error_message';



CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.SGPSDL_RAW.SG_POP6_PRODUCT_LISTS_POPS_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS 'from snowflake.snowpark.functions import col, lit, date_format, current_timestamp, to_date,md5, year, month, concat, format_number, regexp_replace, to_timestamp, when, trim, upper
from snowflake.snowpark.types import StringType, StructType, StructField,DecimalType
import snowflake.snowpark
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz

def main(session: snowpark.Session,Param):
    #Param=["20240620_product_lists_pops.csv","SGPSDL_RAW.DEV_LOAD_STAGE_ADLS","dev/pop6/master/listdata","sdl_pop6_sg_pops"]    
    try:
        file_name = Param[0]
        stage_name = Param[1]
        temp_stage_path = Param[2]
        target_table = Param[3]

        df_schema = StructType([
                    StructField("Product_List", StringType(), True),
                    StructField("Product_List_Code", StringType(), True),
                    StructField("POPDB_ID", StringType(), True),
                    StructField("POP_Code", StringType(), True),
                    StructField("POP_Name", StringType(), True),
                    StructField("Date", StringType(), True)
                ])
        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "|")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name.split(".")[0])
        
              
        df = df.na.drop("all")
        if df.count() == 0:
            return "No Data in file"

        
        df = df.withColumn("filename", lit(file_name))
        df = df.withColumn("crt_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        df = df.withColumn("run_id", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))

        df1=df
        concat_columns = [
                            "Product_List",
                            "POPDB_ID",
                            "POP_Code",
                            "POP_Name",
                            "Date"
                    ]
        
        df1=df1.na.fill("",subset=concat_columns)


       # Concatenate the columns
        df1 = df1.withColumn("hash_c", concat(*[df1[col] for col in concat_columns]))

        #df1 = df1.withColumn("hash_c",concat(df1.Product_List,df1.ProductDB_ID,df1.SKU,df1.MSL_Ranking,df1.Date))
        df1=df1.withColumn("hash",md5(col("hash_c")))
        df1=df1.na.replace({"":None},concat_columns)
        snowdf = df1.select(
                             "Product_List",
                            "POPDB_ID",
                            "POP_Code",
                            "POP_Name",
                            "Date",
                            "filename",
                            "run_id",
                            "crt_dttm",
                            "hash"
                        )
        
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        
        #move to success
        
        snowdf.write.mode("append").saveAsTable(stage_name.split(".")[0] + "." + target_table)
        #snowdf.write.mode("append").saveAsTable(target_table)
        file_name1 = file_name.split(".")[0] + "_" + datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        snowdf.write.copy_into_location("@" + stage_name + "/" + temp_stage_path + "/processed/success/" + formatted_year + "/" + formatted_month + "/" + file_name1, header=True, OVERWRITE=True)
        
        return "Success"

    except KeyError as key_error:
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message

    except pd.errors.MergeError as merge_error:
        error_message = f"DataFrame merging error: {str(merge_error)}"
        return error_message

    except Exception as e:
        error_message = f"Error: {str(e)}"
        return error_message';


CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.SGPSDL_RAW.SG_POP6_USERS_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('openpyxl==3.1.2','snowflake-snowpark-python==*')
HANDLER = 'main'
EXECUTE AS OWNER
AS 'from snowflake.snowpark.functions import col, lit, date_format, current_timestamp, to_date,md5, year, month, concat, format_number, regexp_replace, to_timestamp, when, trim, upper
from snowflake.snowpark.types import StringType, StructType, StructField,DecimalType
import snowflake.snowpark
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz

def main(session: snowpark.Session,Param):
    #Param=["20240620_users.csv","SGPSDL_RAW.DEV_LOAD_STAGE_ADLS","dev/pop6/master/masterdata","sdl_pop6_sg_pops"]    
    try:
        file_name = Param[0]
        stage_name = Param[1]
        temp_stage_path = Param[2]
        target_table = Param[3]

        df_schema = StructType([
                    StructField("Status", StringType(), True),
                    StructField("UserDB_ID", StringType(), True),
                    StructField("Username", StringType(), True),
                    StructField("First_Name", StringType(), True),
                    StructField("Last_Name", StringType(), True),
                    StructField("Team", StringType(), True),
                    StructField("Superior_Name", StringType(), True),
                    StructField("Authorisation_Group", StringType(), True),
                    StructField("Email_Address", StringType(), True),
                    StructField("Longitude", StringType(), True),
                    StructField("Latitude", StringType(), True),
                    StructField("Business_Units_ID", StringType(), True),
                    StructField("Business_Unit_Name", StringType(), True),
                    StructField("Mobile_Number", StringType(), True)
                ])
        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "|")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name.split(".")[0])
        
              
        df = df.na.drop("all")
        if df.count() == 0:
            return "No Data in file"

        
        df = df.withColumn("filename", lit(file_name))
        df = df.withColumn("crt_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        df = df.withColumn("run_id", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))

        df1=df
        concat_columns = [
                              "status",
                                "userdb_id",
                                "username",
                                "first_name",
                                "last_name",
                                "team",
                                "superior_name",
                                "authorisation_group",
                                "email_address",
                                "longitude",
                                "latitude",
                                "business_units_id",
                                "business_unit_name",
                                "mobile_number"
                    ]
        
        df1=df1.na.fill("",subset=concat_columns)


       # Concatenate the columns
        df1 = df1.withColumn("hash_c", concat(*[df1[col] for col in concat_columns]))

        #df1 = df1.withColumn("hash_c",concat(df1.Product_List,df1.ProductDB_ID,df1.SKU,df1.MSL_Ranking,df1.Date))
        df1=df1.withColumn("hash",md5(col("hash_c")))
        df1=df1.na.replace({"":None},concat_columns)
        snowdf = df1.select(
                             "status",
                            "userdb_id",
                            "username",
                            "first_name",
                            "last_name",
                            "team",
                            "superior_name",
                            "authorisation_group",
                            "email_address",
                            "longitude",
                            "latitude",
                            "business_units_id",
                            "business_unit_name",
                            "mobile_number",
                    
                            "filename",
                            "run_id",
                            "crt_dttm",
                            "hash"
                        )
        
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        
        #move to success
        
        snowdf.write.mode("append").saveAsTable(stage_name.split(".")[0] + "." + target_table)
        #snowdf.write.mode("append").saveAsTable(target_table)
        file_name1 = file_name.split(".")[0] + "_" + datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        snowdf.write.copy_into_location("@" + stage_name + "/" + temp_stage_path + "/processed/success/" + formatted_year + "/" + formatted_month + "/" + file_name1, header=True, OVERWRITE=True)
        
        return "Success"

    except KeyError as key_error:
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message

    except pd.errors.MergeError as merge_error:
        error_message = f"DataFrame merging error: {str(merge_error)}"
        return error_message

    except Exception as e:
        error_message = f"Error: {str(e)}"
        return error_message';



CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.SGPSDL_RAW.SG_POP6_POP_LISTS_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS 'from snowflake.snowpark.functions import col, lit, date_format, current_timestamp, to_date,md5, year, month, concat, format_number, regexp_replace, to_timestamp, when, trim, upper
from snowflake.snowpark.types import StringType, StructType, StructField,DecimalType
import snowflake.snowpark
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz

def main(session: snowpark.Session,Param):
    #Param=["20240620_pop_lists.csv","SGPSDL_RAW.DEV_LOAD_STAGE_ADLS","dev/pop6/master/listdata","sdl_pop6_sg_pops"]    
    try:
        file_name = Param[0]
        stage_name = Param[1]
        temp_stage_path = Param[2]
        target_table = Param[3]

        df_schema = StructType([
                    StructField("Status", StringType(), True),
                    StructField("POP_List", StringType(), True),
                    StructField("POPDB_ID", StringType(), True),
                    StructField("POP_Code", StringType(), True),
                    StructField("POP_Name", StringType(), True),
                    StructField("Date", StringType(), True)
                ])
        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "|")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name.split(".")[0])
        
              
        df = df.na.drop("all")
        if df.count() == 0:
            return "No Data in file"

        
        df = df.withColumn("filename", lit(file_name))
        df = df.withColumn("crt_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        df = df.withColumn("run_id", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))

        df1=df
        concat_columns = [
                              "Status",
                            "POP_List",
                            "POPDB_ID",
                            "POP_Code",
                            "POP_Name",
                            "Date"
                    ]
        
        df1=df1.na.fill("",subset=concat_columns)


       # Concatenate the columns
        df1 = df1.withColumn("hash_c", concat(*[df1[col] for col in concat_columns]))

        #df1 = df1.withColumn("hash_c",concat(df1.Product_List,df1.ProductDB_ID,df1.SKU,df1.MSL_Ranking,df1.Date))
        df1=df1.withColumn("hash",md5(col("hash_c")))
        df1=df1.na.replace({"":None},concat_columns)
        snowdf = df1.select(
                              "Status",
                            "POP_List",
                            "POPDB_ID",
                            "POP_Code",
                            "POP_Name",
                            "Date",
                    
                            "filename",
                            "run_id",
                            "crt_dttm",
                            "hash"
                        )
        
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        
        #move to success
        
        snowdf.write.mode("append").saveAsTable(stage_name.split(".")[0] + "." + target_table)
        #snowdf.write.mode("append").saveAsTable(target_table)
        file_name1 = file_name.split(".")[0] + "_" + datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        snowdf.write.copy_into_location("@" + stage_name + "/" + temp_stage_path + "/processed/success/" + formatted_year + "/" + formatted_month + "/" + file_name1, header=True, OVERWRITE=True)
        
        return "Success"

    except KeyError as key_error:
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message

    except pd.errors.MergeError as merge_error:
        error_message = f"DataFrame merging error: {str(merge_error)}"
        return error_message

    except Exception as e:
        error_message = f"Error: {str(e)}"
        return error_message';




CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.SGPSDL_RAW.SG_POP6_PRODUCT_LISTS_ALLOCATION_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS 'from snowflake.snowpark.functions import col, lit, date_format, current_timestamp, to_date,md5, year, month, concat, format_number, regexp_replace, to_timestamp, when, trim, upper
from snowflake.snowpark.types import StringType, StructType, StructField,DecimalType
import snowflake.snowpark
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz

def main(session: snowpark.Session,Param):
    #Param=["20240620_product_lists_allocation.csv","SGPSDL_RAW.DEV_LOAD_STAGE_ADLS","dev/pop6/master/listdata","sdl_pop6_sg_pops"]    
    try:
        file_name = Param[0]
        stage_name = Param[1]
        temp_stage_path = Param[2]
        target_table = Param[3]

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
                    StructField("Date", StringType(), True)
                ])
        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "|")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name.split(".")[0])
        
              
        df = df.na.drop("all")
        if df.count() == 0:
            return "No Data in file"

        
        df = df.withColumn("filename", lit(file_name))
        df = df.withColumn("crt_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        df = df.withColumn("run_id", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))

        df1=df
        concat_columns = [
                            "Product_Group_Status",
                            "Product_Group",
                            "Product_List_Status",
                            "Product_List",
                            "Product_List_Code",
                            "POP_Attribute_ID",
                            "POP_Attribute",
                            "POP_Attribute_Value_ID",
                            "POP_Attribute_Value",
                            "Date"
                    ]
        
        df1=df1.na.fill("",subset=concat_columns)


       # Concatenate the columns
        df1 = df1.withColumn("hash_c", concat(*[df1[col] for col in concat_columns]))

        #df1 = df1.withColumn("hash_c",concat(df1.Product_List,df1.ProductDB_ID,df1.SKU,df1.MSL_Ranking,df1.Date))
        df1=df1.withColumn("hash",md5(col("hash_c")))
        df1=df1.na.replace({"":None},concat_columns)
        snowdf = df1.select(
                             "Product_Group_Status",
                            "Product_Group",
                            "Product_List_Status",
                            "Product_List",
                            "Product_List_Code",
                            "POP_Attribute_ID",
                            "POP_Attribute",
                            "POP_Attribute_Value_ID",
                            "POP_Attribute_Value",
                            "Date",
                            "filename",
                            "run_id",
                            "crt_dttm",
                            "hash"
                        )
        
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        
        #move to success
        
        snowdf.write.mode("append").saveAsTable(stage_name.split(".")[0] + "." + target_table)
        #snowdf.write.mode("append").saveAsTable(target_table)
        file_name1 = file_name.split(".")[0] + "_" + datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        snowdf.write.copy_into_location("@" + stage_name + "/" + temp_stage_path + "/processed/success/" + formatted_year + "/" + formatted_month + "/" + file_name1, header=True, OVERWRITE=True)
        
        return "Success"

    except KeyError as key_error:
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message

    except pd.errors.MergeError as merge_error:
        error_message = f"DataFrame merging error: {str(merge_error)}"
        return error_message

    except Exception as e:
        error_message = f"Error: {str(e)}"
        return error_message';



CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.SGPSDL_RAW.SG_POP6_PRODUCT_LISTS_PRODUCTS_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS 'from snowflake.snowpark.functions import col, lit, date_format, current_timestamp, to_date,md5, year, month, concat, format_number, regexp_replace, to_timestamp, when, trim, upper
from snowflake.snowpark.types import StringType, StructType, StructField,DecimalType
import snowflake.snowpark
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz

def main(session: snowpark.Session,Param):
    #Param=["20240620_product_lists_products.csv","SGPSDL_RAW.DEV_LOAD_STAGE_ADLS","dev/pop6/master/listdata","sdl_pop6_sg_pops"]    
    try:
        file_name = Param[0]
        stage_name = Param[1]
        temp_stage_path = Param[2]
        target_table = Param[3]

        df_schema = StructType([
                    StructField("Product_List", StringType(), True),
                    StructField("Product_List_Code", StringType(), True),
                    StructField("ProductDB_ID", StringType(), True),
                    StructField("SKU", StringType(), True),
                    StructField("MSL_Ranking", StringType(), True),
                    StructField("Date", StringType(), True)
                ])
        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "|")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name.split(".")[0])
        
              
        df = df.na.drop("all")
        if df.count() == 0:
            return "No Data in file"

        
        df = df.withColumn("filename", lit(file_name))
        df = df.withColumn("crt_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        df = df.withColumn("run_id", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))

        df1=df
        concat_columns = [
                            "Product_List",
                            "Product_List_Code",
                            "ProductDB_ID",
                            "SKU",
                            "MSL_Ranking",
                            "Date"
                    ]
        
        df1=df1.na.fill("",subset=concat_columns)


       # Concatenate the columns
        df1 = df1.withColumn("hash_c", concat(*[df1[col] for col in concat_columns]))

        #df1 = df1.withColumn("hash_c",concat(df1.Product_List,df1.ProductDB_ID,df1.SKU,df1.MSL_Ranking,df1.Date))
        df1=df1.withColumn("hash",md5(col("hash_c")))
        df1=df1.na.replace({"":None},concat_columns)
        snowdf = df1.select(
                             "Product_List",
                            "Product_List_Code",
                            "ProductDB_ID",
                            "SKU",
                            "MSL_Ranking",
                            "Date",
                            "filename",
                            "run_id",
                            "crt_dttm",
                            "hash"
                        )
        
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        
        #move to success
        
        snowdf.write.mode("append").saveAsTable(stage_name.split(".")[0] + "." + target_table)
        #snowdf.write.mode("append").saveAsTable(target_table)
        file_name1 = file_name.split(".")[0] + "_" + datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        snowdf.write.copy_into_location("@" + stage_name + "/" + temp_stage_path + "/processed/success/" + formatted_year + "/" + formatted_month + "/" + file_name1, header=True, OVERWRITE=True)
        
        return "Success"

    except KeyError as key_error:
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message

    except pd.errors.MergeError as merge_error:
        error_message = f"DataFrame merging error: {str(merge_error)}"
        return error_message

    except Exception as e:
        error_message = f"Error: {str(e)}"
        return error_message';
