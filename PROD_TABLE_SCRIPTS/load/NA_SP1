CREATE OR REPLACE PROCEDURE NTASDL_RAW.BRANDRANKING_DAILY_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them.

import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col,lit,trim,when
from snowflake.snowpark.types import  StringType, StructType, StructField,IntegerType
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz



def main(session: snowpark.Session,Param): 
       
    try :
            
        #Param=['''',''NTASDL_RAW.DEV_LOAD_STAGE_ADLS_KOR'',''dev/coupang_premium/transaction/brandranking/'',''sdl_kr_coupang_brand_ranking'']

        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        # Define the schema for the DataFrame
        
        df_schema=StructType([
                        StructField("category_depth1", StringType()),
                        StructField("category_depth2", StringType()),
                        StructField("category_depth3", StringType()),
                        StructField("ranking", StringType()),
                        StructField("brand", StringType()),
                        StructField("jnj_brand", StringType()),
                        StructField("rank_change", StringType())
                        
                        ])
                
        dataframe = session.read.schema(df_schema) \\
        .option("skip_header",1) \\
        .option("field_delimiter", "\\u0001") \\
        .option("encoding","UTF-8")\\
        .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
        

        
        dataframe=dataframe.na.drop("all")
        if dataframe.count()==0:
            return "No Data in file"


        yearmo = file_name.split(".")[0].split("-")[2]
        dataframe = dataframe.with_column("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        dataframe = dataframe.with_column("FILE_NAME",lit(file_name))
        dataframe = dataframe.withColumn("yearmo", lit(yearmo))
        dataframe = dataframe.withColumn("data_granularity", lit(''daily''))

        
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

CREATE OR REPLACE PROCEDURE NTASDL_RAW.BRANDRANKING_MONTHLY_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them.

import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col,lit,trim,when
from snowflake.snowpark.types import  StringType, StructType, StructField,IntegerType
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz



def main(session: snowpark.Session,Param): 
       
    try :
            
        #Param=['''',''NTASDL_RAW.DEV_LOAD_STAGE_ADLS_KOR'',''dev/coupang_premium/transaction/brandranking/'',''sdl_kr_coupang_brand_ranking'']

        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        # Define the schema for the DataFrame
        
        df_schema=StructType([
                        StructField("category_depth1", StringType()),
                        StructField("category_depth2", StringType()),
                        StructField("category_depth3", StringType()),
                        StructField("ranking", StringType()),
                        StructField("brand", StringType()),
                        StructField("jnj_brand", StringType()),
                        StructField("rank_change", StringType())
                        
                        ])
                
        dataframe = session.read.schema(df_schema) \\
        .option("skip_header",1) \\
        .option("field_delimiter", "\\u0001") \\
        .option("encoding","UTF-8")\\
        .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
        

        
        dataframe=dataframe.na.drop("all")
        if dataframe.count()==0:
            return "No Data in file"


        yearmo = file_name.split(".")[0].split("-")[2]
        dataframe = dataframe.with_column("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        dataframe = dataframe.with_column("FILE_NAME",lit(file_name))
        dataframe = dataframe.withColumn("yearmo", lit(yearmo))
        dataframe = dataframe.withColumn("data_granularity", lit(''monthly''))

        
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

CREATE OR REPLACE PROCEDURE NTASDL_RAW.BRANDRANKING_WEEKLY_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them.

import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col,lit,trim,when
from snowflake.snowpark.types import  StringType, StructType, StructField,IntegerType
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz



def main(session: snowpark.Session,Param): 
       
    try :
            
        #Param=['''',''NTASDL_RAW.DEV_LOAD_STAGE_ADLS_KOR'',''dev/coupang_premium/transaction/brandranking/'',''sdl_kr_coupang_brand_ranking'']

        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        # Define the schema for the DataFrame
        
        df_schema=StructType([
                        StructField("category_depth1", StringType()),
                        StructField("category_depth2", StringType()),
                        StructField("category_depth3", StringType()),
                        StructField("ranking", StringType()),
                        StructField("brand", StringType()),
                        StructField("jnj_brand", StringType()),
                        StructField("rank_change", StringType())
                        
                        ])
                
        dataframe = session.read.schema(df_schema) \\
        .option("skip_header",1) \\
        .option("field_delimiter", "\\u0001") \\
        .option("encoding","UTF-8")\\
        .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
        

        
        dataframe=dataframe.na.drop("all")
        if dataframe.count()==0:
            return "No Data in file"


        yearmo = file_name.split(".")[0].split("-")[2]
        dataframe = dataframe.with_column("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        dataframe = dataframe.with_column("FILE_NAME",lit(file_name))
        dataframe = dataframe.withColumn("yearmo", lit(yearmo))
        dataframe = dataframe.withColumn("data_granularity", lit(''weekly''))

        
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


CREATE OR REPLACE PROCEDURE NTASDL_RAW.COUPANG_DAILY_BRAND_REVIEWS_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them.

import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col,lit,trim,when
from snowflake.snowpark.types import  StringType, StructType, StructField,IntegerType
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz



def main(session: snowpark.Session,Param): 
       
    try :
            
        #Param=[''DailyBrandReviews-daily-20211101.csv'',''NTASDL_RAW.DEV_LOAD_STAGE_ADLS_KOR'',''dev/coupang_premium/transaction'',''sdl_kr_coupang_daily_brand_reviews'']

        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        # Define the schema for the DataFrame
        
        df_schema=StructType([
                        StructField("review_date", StringType()),
                        StructField("brand", StringType()),
                        StructField("coupang_sku_id", StringType()),
                        StructField("coupang_sku_name", StringType()),
                        StructField("review_score_star", StringType()),
                        StructField("review_contents", StringType())
                        ])
                
        dataframe = session.read.schema(df_schema) \\
        .option("skip_header",1) \\
        .option("field_delimiter", "\\u0001") \\
        .option("encoding","UTF-8")\\
        .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
        

        
        dataframe=dataframe.na.drop("all")
        if dataframe.count()==0:
            return "No Data in file"

        
       

        
        
        yearmo = file_name.split("-")[2].split(".")[0]
        dataframe = dataframe.with_column("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        dataframe = dataframe.with_column("FILE_NAME",lit(file_name))
        dataframe = dataframe.withColumn("yearmo", lit(yearmo))
        dataframe = dataframe.withColumn("data_granularity", lit(''daily''))

        
        # Load Data to the target table
        dataframe.write.mode("append").saveAsTable(target_table)
       
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        #move to success
        file_name=file_name[0:32]+''_''+datetime.now().strftime("%Y%m%d%H%M%S")
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

CREATE OR REPLACE PROCEDURE NTASDL_RAW.CUSTOMERBRANDTREND_MONTLY_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them.

import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col,lit,trim,when
from snowflake.snowpark.types import  StringType, StructType, StructField,IntegerType
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz



def main(session: snowpark.Session,Param): 
       
    try :
            
        #Param=[''CustomerBrandTrend-monthly-20211031.csv'',''NTASDL_RAW.DEV_LOAD_STAGE_ADLS_KOR'',''dev/coupang_premium/transaction/customer_brand_trend/'',''sdl_kr_coupang_customer_brand_trend'']

        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        # Define the schema for the DataFrame
        
        df_schema=StructType([
                        StructField("date_yyyymm", StringType()),
                        StructField("coupang_id", StringType()),
                        StructField("category_depth1", StringType()),
                        StructField("brand", StringType()),
                        StructField("new_user_count", StringType()),
                        StructField("curr_user_count", StringType()),
                        StructField("tot_user_count", StringType()),
                        StructField("new_user_sales_amt", StringType()),
                        StructField("curr_user_sales_amt", StringType()),
                        StructField("new_user_avg_product_sales_price", StringType()),
                        StructField("curr_user_avg_product_sales_price", StringType()),
                        StructField("tot_user_avg_product_sales_price", StringType())
                        
                        ])
                
        dataframe = session.read.schema(df_schema) \\
        .option("skip_header",1) \\
        .option("field_delimiter", "\\u0001") \\
        .option("encoding","UTF-8")\\
        .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
        

        
        dataframe=dataframe.na.drop("all")
        if dataframe.count()==0:
            return "No Data in file"


        yearmo = file_name.split(".")[0].split("-")[2]
        dataframe = dataframe.with_column("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        dataframe = dataframe.with_column("FILE_NAME",lit(file_name))
        dataframe = dataframe.withColumn("yearmo", lit(yearmo))
        dataframe = dataframe.withColumn("data_granularity", lit(''monthly''))

        
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

CREATE OR REPLACE PROCEDURE NTASDL_RAW.GT_SELLOUT_HYUNDAI_PREPROCESSING("PARAM" ARRAY)
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

        #Param=[''Hyundai_126137_202401.xlsx'',''NTASDL_RAW.DEV_LOAD_STAGE_ADLS_KOR'',''dev/gt_sellout/transaction_files/department_store/hyundai/'',''sdl_kr_hyundai_gt_sellout'']

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
            
            df1 = excel_file.parse(sheet_name=''현대압구정'',engine=''openpyxl'')


            df1 = df1.applymap(lambda x: str(x))
            #df1 = df1.applymap(lambda x: unidecode(x))
            #print(df1)
            df1.rename(columns={df1.columns[0]:''BURIAL_NAME'',
                                df1.columns[1]:''EAN'',
                                df1.columns[2]:''ARTICLE_NAME'',
                                df1.columns[3]:''NORMAL_SALES'',
                                df1.columns[4]:''QTY'',
                                df1.columns[5]:''SALES'',
                                df1.columns[6]:''UNIT_PRICE'',
                                df1.columns[7]:''DC_RATE'',
                                df1.columns[8]:''MARGIN_RATE'',
                                df1.columns[9]:''MARGIN_NORMAL'',
                                df1.columns[10]:''MARGIN_SOLD'',
                                df1.columns[11]:''UNIT_PRC_PRES''},inplace = True)

            
            k  = df1.iloc[0][''BURIAL_NAME'']
            
            df1 = df1.iloc[3:]
            
            df1 = df1[df1.columns[:12]]
            
            df1[''SUB_CUSTOMER_NAME''] = k
            
            df1[''EAN''] = df1[''EAN''].map(lambda x: None if x==''nan'' else x)
            
            df1.dropna(subset=[''EAN''],inplace=True)
#----------------------------------------------------------------------------------------------------------------------------------------------
            df2 = excel_file.parse(sheet_name=''무'',engine=''openpyxl'')
            
            df2 = df2.applymap(lambda x: str(x))
            #df2 = df2.applymap(lambda x: unidecode(x))
            #print(df2)
            df2.rename(columns={df2.columns[0]:''BURIAL_NAME'',
                                df2.columns[1]:''EAN'',
                                df2.columns[2]:''ARTICLE_NAME'',
                                df2.columns[3]:''NORMAL_SALES'',
                                df2.columns[4]:''QTY'',
                                df2.columns[5]:''SALES'',
                                df2.columns[6]:''UNIT_PRICE'',
                                df2.columns[7]:''DC_RATE'',
                                df2.columns[8]:''MARGIN_RATE'',
                                df2.columns[9]:''MARGIN_NORMAL'',
                                df2.columns[10]:''MARGIN_SOLD'',
                                df2.columns[11]:''UNIT_PRC_PRES''},inplace = True)

            
            k1  = df2.iloc[0][''BURIAL_NAME'']
            
            df2 = df2.iloc[3:]
            df2 = df2[df2.columns[:12]]
            df2[''SUB_CUSTOMER_NAME''] = k1
            df2[''EAN''] = df2[''EAN''].map(lambda x: None if x==''nan'' else x)
            df2.dropna(subset=[''EAN''],inplace=True)
            
            #snowdf = snowdf.with_columns(''k'',lit(k))
            #---------------------------------------------------------------------------------------------------------------------------------------

            df3 = excel_file.parse(sheet_name=''천'',engine=''openpyxl'')
            
            df3 = df3.applymap(lambda x: str(x))
            #df3 = df3.applymap(lambda x: unidecode(x))
            #print(df3)
            df3.rename(columns={df3.columns[0]:''BURIAL_NAME'',
                                df3.columns[1]:''EAN'',
                                df3.columns[2]:''ARTICLE_NAME'',
                                df3.columns[3]:''NORMAL_SALES'',
                                df3.columns[4]:''QTY'',
                                df3.columns[5]:''SALES'',
                                df3.columns[6]:''UNIT_PRICE'',
                                df3.columns[7]:''DC_RATE'',
                                df3.columns[8]:''MARGIN_RATE'',
                                df3.columns[9]:''MARGIN_NORMAL'',
                                df3.columns[10]:''MARGIN_SOLD'',
                                df3.columns[11]:''UNIT_PRC_PRES''},inplace = True)

            
            k  = df3.iloc[0][''BURIAL_NAME'']
            
            df3 = df3.iloc[3:]
            
            df3 = df3[df3.columns[:12]]
            
            df3[''SUB_CUSTOMER_NAME''] = k
            
            df3[''EAN''] = df3[''EAN''].map(lambda x: None if x==''nan'' else x)
            
            df3.dropna(subset=[''EAN''],inplace=True)

            #---------------------------------------------------------------------------------------------------

            df4 = excel_file.parse(sheet_name=''신'',engine=''openpyxl'')
            
            df4 = df4.applymap(lambda x: str(x))
            #df4 = df4.applymap(lambda x: unidecode(x))
            #print(df4)
            df4.rename(columns={df4.columns[0]:''BURIAL_NAME'',
                                df4.columns[1]:''EAN'',
                                df4.columns[2]:''ARTICLE_NAME'',
                                df4.columns[3]:''NORMAL_SALES'',
                                df4.columns[4]:''QTY'',
                                df4.columns[5]:''SALES'',
                                df4.columns[6]:''UNIT_PRICE'',
                                df4.columns[7]:''DC_RATE'',
                                df4.columns[8]:''MARGIN_RATE'',
                                df4.columns[9]:''MARGIN_NORMAL'',
                                df4.columns[10]:''MARGIN_SOLD'',
                                df4.columns[11]:''UNIT_PRC_PRES''},inplace = True)

            
            k  = df4.iloc[0][''BURIAL_NAME'']
            
            df4 = df4.iloc[3:]
            
            df4 = df4[df4.columns[:12]]
            
            df4[''SUB_CUSTOMER_NAME''] = k
            
            df4[''EAN''] = df4[''EAN''].map(lambda x: None if x==''nan'' else x)
            
            df4.dropna(subset=[''EAN''],inplace=True)
            
            
            
            #--------------------------------------------------------------------------------------------------

            df5 = excel_file.parse(sheet_name=''현미'',engine=''openpyxl'')
            
            df5 = df5.applymap(lambda x: str(x))
            #df5 = df5.applymap(lambda x: unidecode(x))
            #print(df5)
            df5.rename(columns={df5.columns[0]:''BURIAL_NAME'',
                                df5.columns[1]:''EAN'',
                                df5.columns[2]:''ARTICLE_NAME'',
                                df5.columns[3]:''NORMAL_SALES'',
                                df5.columns[4]:''QTY'',
                                df5.columns[5]:''SALES'',
                                df5.columns[6]:''UNIT_PRICE'',
                                df5.columns[7]:''DC_RATE'',
                                df5.columns[8]:''MARGIN_RATE'',
                                df5.columns[9]:''MARGIN_NORMAL'',
                                df5.columns[10]:''MARGIN_SOLD'',
                                df5.columns[11]:''UNIT_PRC_PRES''},inplace = True)

            
            k  = df5.iloc[0][''BURIAL_NAME'']
            
            df5 = df5.iloc[3:]
            
            df5 = df5[df5.columns[:12]]
            
            df5[''SUB_CUSTOMER_NAME''] = k
            
            df5[''EAN''] = df5[''EAN''].map(lambda x: None if x==''nan'' else x)
            
            df5.dropna(subset=[''EAN''],inplace=True)
            
            #------------------------------------------------------------------------------------------

            df6 = excel_file.parse(sheet_name=''목'',engine=''openpyxl'')
            
            df6 = df6.applymap(lambda x: str(x))
            #df6 = df6.applymap(lambda x: unidecode(x))
            #print(df6)
            df6.rename(columns={df6.columns[0]:''BURIAL_NAME'',
                                df6.columns[1]:''EAN'',
                                df6.columns[2]:''ARTICLE_NAME'',
                                df6.columns[3]:''NORMAL_SALES'',
                                df6.columns[4]:''QTY'',
                                df6.columns[5]:''SALES'',
                                df6.columns[6]:''UNIT_PRICE'',
                                df6.columns[7]:''DC_RATE'',
                                df6.columns[8]:''MARGIN_RATE'',
                                df6.columns[9]:''MARGIN_NORMAL'',
                                df6.columns[10]:''MARGIN_SOLD'',
                                df6.columns[11]:''UNIT_PRC_PRES''},inplace = True)

            
            k  = df6.iloc[0][''BURIAL_NAME'']
            
            df6 = df6.iloc[3:]
            
            df6 = df6[df6.columns[:12]]
            
            df6[''SUB_CUSTOMER_NAME''] = k
            
            df6[''EAN''] = df6[''EAN''].map(lambda x: None if x==''nan'' else x)
            
            df6.dropna(subset=[''EAN''],inplace=True)
            
            #---------------------------------------------------------------------------

            df7 = excel_file.parse(sheet_name=''현중'',engine=''openpyxl'')
            
            df7 = df7.applymap(lambda x: str(x))
            #df7 = df7.applymap(lambda x: unidecode(x))
            #print(df7)
            df7.rename(columns={df7.columns[0]:''BURIAL_NAME'',
                                df7.columns[1]:''EAN'',
                                df7.columns[2]:''ARTICLE_NAME'',
                                df7.columns[3]:''NORMAL_SALES'',
                                df7.columns[4]:''QTY'',
                                df7.columns[5]:''SALES'',
                                df7.columns[6]:''UNIT_PRICE'',
                                df7.columns[7]:''DC_RATE'',
                                df7.columns[8]:''MARGIN_RATE'',
                                df7.columns[9]:''MARGIN_NORMAL'',
                                df7.columns[10]:''MARGIN_SOLD'',
                                df7.columns[11]:''UNIT_PRC_PRES''},inplace = True)

            
            k  = df7.iloc[0][''BURIAL_NAME'']
            
            df7 = df7.iloc[3:]
            
            df7 = df7[df7.columns[:12]]
            
            df7[''SUB_CUSTOMER_NAME''] = k
            
            df7[''EAN''] = df7[''EAN''].map(lambda x: None if x==''nan'' else x)
            
            df7.dropna(subset=[''EAN''],inplace=True)
            
            #--------------------------------------------------------------------------
            df8 = excel_file.parse(sheet_name=''킨'',engine=''openpyxl'')
            
            df8 = df8.applymap(lambda x: str(x))
            #df8 = df8.applymap(lambda x: unidecode(x))
            #print(df8)
            df8.rename(columns={df8.columns[0]:''BURIAL_NAME'',
                                df8.columns[1]:''EAN'',
                                df8.columns[2]:''ARTICLE_NAME'',
                                df8.columns[3]:''NORMAL_SALES'',
                                df8.columns[4]:''QTY'',
                                df8.columns[5]:''SALES'',
                                df8.columns[6]:''UNIT_PRICE'',
                                df8.columns[7]:''DC_RATE'',
                                df8.columns[8]:''MARGIN_RATE'',
                                df8.columns[9]:''MARGIN_NORMAL'',
                                df8.columns[10]:''MARGIN_SOLD'',
                                df8.columns[11]:''UNIT_PRC_PRES''},inplace = True)

            
            k  = df8.iloc[0][''BURIAL_NAME'']
            
            df8 = df8.iloc[3:]
            
            df8 = df8[df8.columns[:12]]
            
            df8[''SUB_CUSTOMER_NAME''] = k
            
            df8[''EAN''] = df8[''EAN''].map(lambda x: None if x==''nan'' else x)
            
            df8.dropna(subset=[''EAN''],inplace=True)

            
            
            #-----------------------------------------------------------------------
            df9 = excel_file.parse(sheet_name=''디'',engine=''openpyxl'')
            
            df9 = df9.applymap(lambda x: str(x))
            #df9 = df9.applymap(lambda x: unidecode(x))
            #print(df9)
            df9.rename(columns={df9.columns[0]:''BURIAL_NAME'',
                                df9.columns[1]:''EAN'',
                                df9.columns[2]:''ARTICLE_NAME'',
                                df9.columns[3]:''NORMAL_SALES'',
                                df9.columns[4]:''QTY'',
                                df9.columns[5]:''SALES'',
                                df9.columns[6]:''UNIT_PRICE'',
                                df9.columns[7]:''DC_RATE'',
                                df9.columns[8]:''MARGIN_RATE'',
                                df9.columns[9]:''MARGIN_NORMAL'',
                                df9.columns[10]:''MARGIN_SOLD'',
                                df9.columns[11]:''UNIT_PRC_PRES''},inplace = True)

            
            k  = df9.iloc[0][''BURIAL_NAME'']
            
            df9 = df9.iloc[3:]
            
            df9 = df9[df9.columns[:12]]
            
            df9[''SUB_CUSTOMER_NAME''] = k
            
            df9[''EAN''] = df9[''EAN''].map(lambda x: None if x==''nan'' else x)
            
            df9.dropna(subset=[''EAN''],inplace=True)
            
            #------------------------------------------------------------------------------------------------------------------

            df10 = excel_file.parse(sheet_name=''판'',engine=''openpyxl'')
            
            df10 = df10.applymap(lambda x: str(x))
            #df10 = df10.applymap(lambda x: unidecode(x))
            #print(df10)
            df10.rename(columns={df10.columns[0]:''BURIAL_NAME'',
                                df10.columns[1]:''EAN'',
                                df10.columns[2]:''ARTICLE_NAME'',
                                df10.columns[3]:''NORMAL_SALES'',
                                df10.columns[4]:''QTY'',
                                df10.columns[5]:''SALES'',
                                df10.columns[6]:''UNIT_PRICE'',
                                df10.columns[7]:''DC_RATE'',
                                df10.columns[8]:''MARGIN_RATE'',
                                df10.columns[9]:''MARGIN_NORMAL'',
                                df10.columns[10]:''MARGIN_SOLD'',
                                df10.columns[11]:''UNIT_PRC_PRES''},inplace = True)

            
            k  = df10.iloc[0][''BURIAL_NAME'']
            
            df10 = df10.iloc[3:]
            
            df10 = df10[df10.columns[:12]]
            
            df10[''SUB_CUSTOMER_NAME''] = k
            
            df10[''EAN''] = df10[''EAN''].map(lambda x: None if x==''nan'' else x)
            
            df10.dropna(subset=[''EAN''],inplace=True)
            
            #------------------------------------------------------------------------------------------
            df11 = excel_file.parse(sheet_name=''서울'',engine=''openpyxl'')
            
            df11 = df11.applymap(lambda x: str(x))
            #df11 = df11.applymap(lambda x: unidecode(x))
            #print(df11)
            df11.rename(columns={df11.columns[0]:''BURIAL_NAME'',
                                df11.columns[1]:''EAN'',
                                df11.columns[2]:''ARTICLE_NAME'',
                                df11.columns[3]:''NORMAL_SALES'',
                                df11.columns[4]:''QTY'',
                                df11.columns[5]:''SALES'',
                                df11.columns[6]:''UNIT_PRICE'',
                                df11.columns[7]:''DC_RATE'',
                                df11.columns[8]:''MARGIN_RATE'',
                                df11.columns[9]:''MARGIN_NORMAL'',
                                df11.columns[10]:''MARGIN_SOLD'',
                                df11.columns[11]:''UNIT_PRC_PRES''},inplace = True)

            
            k  = df11.iloc[0][''BURIAL_NAME'']
            
            df11 = df11.iloc[3:]
            
            df11 = df11[df11.columns[:12]]
            
            df11[''SUB_CUSTOMER_NAME''] = k
            
            df11[''EAN''] = df11[''EAN''].map(lambda x: None if x==''nan'' else x)
            
            df11.dropna(subset=[''EAN''],inplace=True)
            
            #--------------------------------------------------------------------
            df12 = excel_file.parse(sheet_name=''현부산'',engine=''openpyxl'')
            
            df12 = df12.applymap(lambda x: str(x))
            #df12 = df12.applymap(lambda x: unidecode(x))
            #print(df12)
            df12.rename(columns={df12.columns[0]:''BURIAL_NAME'',
                                df12.columns[1]:''EAN'',
                                df12.columns[2]:''ARTICLE_NAME'',
                                df12.columns[3]:''NORMAL_SALES'',
                                df12.columns[4]:''QTY'',
                                df12.columns[5]:''SALES'',
                                df12.columns[6]:''UNIT_PRICE'',
                                df12.columns[7]:''DC_RATE'',
                                df12.columns[8]:''MARGIN_RATE'',
                                df12.columns[9]:''MARGIN_NORMAL'',
                                df12.columns[10]:''MARGIN_SOLD'',
                                df12.columns[11]:''UNIT_PRC_PRES''},inplace = True)

            
            k  = df12.iloc[0][''BURIAL_NAME'']
            
            df12 = df12.iloc[3:]
            
            df12 = df12[df12.columns[:12]]
            
            df12[''SUB_CUSTOMER_NAME''] = k
            
            df12[''EAN''] = df12[''EAN''].map(lambda x: None if x==''nan'' else x)
            
            df12.dropna(subset=[''EAN''],inplace=True)
            
            #-----------------------------------------------------------------------------------

            df13 = excel_file.parse(sheet_name=''현대구'',engine=''openpyxl'')
            
            df13 = df13.applymap(lambda x: str(x))
            #df13 = df13.applymap(lambda x: unidecode(x))
            #print(df13)
            df13.rename(columns={df13.columns[0]:''BURIAL_NAME'',
                                df13.columns[1]:''EAN'',
                                df13.columns[2]:''ARTICLE_NAME'',
                                df13.columns[3]:''NORMAL_SALES'',
                                df13.columns[4]:''QTY'',
                                df13.columns[5]:''SALES'',
                                df13.columns[6]:''UNIT_PRICE'',
                                df13.columns[7]:''DC_RATE'',
                                df13.columns[8]:''MARGIN_RATE'',
                                df13.columns[9]:''MARGIN_NORMAL'',
                                df13.columns[10]:''MARGIN_SOLD'',
                                df13.columns[11]:''UNIT_PRC_PRES''},inplace = True)

            
            k  = df13.iloc[0][''BURIAL_NAME'']
            
            df13 = df13.iloc[3:]
            
            df13 = df13[df13.columns[:12]]
            
            df13[''SUB_CUSTOMER_NAME''] = k
            
            df13[''EAN''] = df13[''EAN''].map(lambda x: None if x==''nan'' else x)
            
            df13.dropna(subset=[''EAN''],inplace=True)
            
            #-----------------------------------------------------------------------

            df14 = excel_file.parse(sheet_name=''현울'',engine=''openpyxl'')
            
            df14 = df14.applymap(lambda x: str(x))
            #df14 = df14.applymap(lambda x: unidecode(x))
            #print(df14)
            df14.rename(columns={df14.columns[0]:''BURIAL_NAME'',
                                df14.columns[1]:''EAN'',
                                df14.columns[2]:''ARTICLE_NAME'',
                                df14.columns[3]:''NORMAL_SALES'',
                                df14.columns[4]:''QTY'',
                                df14.columns[5]:''SALES'',
                                df14.columns[6]:''UNIT_PRICE'',
                                df14.columns[7]:''DC_RATE'',
                                df14.columns[8]:''MARGIN_RATE'',
                                df14.columns[9]:''MARGIN_NORMAL'',
                                df14.columns[10]:''MARGIN_SOLD'',
                                df14.columns[11]:''UNIT_PRC_PRES''},inplace = True)

            
            k  = df14.iloc[0][''BURIAL_NAME'']
            
            df14 = df14.iloc[3:]
            
            df14 = df14[df14.columns[:12]]
            
            df14[''SUB_CUSTOMER_NAME''] = k
            
            df14[''EAN''] = df14[''EAN''].map(lambda x: None if x==''nan'' else x)
            
            df14.dropna(subset=[''EAN''],inplace=True)
            
            #-------------------------------------------------------------------------

            df15 = excel_file.parse(sheet_name=''동구'',engine=''openpyxl'')
            
            df15 = df15.applymap(lambda x: str(x))
            #df15 = df15.applymap(lambda x: unidecode(x))
            #print(df15)
            df15.rename(columns={df15.columns[0]:''BURIAL_NAME'',
                                df15.columns[1]:''EAN'',
                                df15.columns[2]:''ARTICLE_NAME'',
                                df15.columns[3]:''NORMAL_SALES'',
                                df15.columns[4]:''QTY'',
                                df15.columns[5]:''SALES'',
                                df15.columns[6]:''UNIT_PRICE'',
                                df15.columns[7]:''DC_RATE'',
                                df15.columns[8]:''MARGIN_RATE'',
                                df15.columns[9]:''MARGIN_NORMAL'',
                                df15.columns[10]:''MARGIN_SOLD'',
                                df15.columns[11]:''UNIT_PRC_PRES''},inplace = True)

            
            k  = df15.iloc[0][''BURIAL_NAME'']
            
            df15 = df15.iloc[2:]
            
            df15 = df15[df15.columns[:12]]
            
            df15[''SUB_CUSTOMER_NAME''] = k
            
            df15[''EAN''] = df15[''EAN''].map(lambda x: None if x==''nan'' else x)
            
            df15.dropna(subset=[''EAN''],inplace=True)
            
            #-------------------------------------------------------------------

            

            df16 = excel_file.parse(sheet_name=''충'',engine=''openpyxl'')
            
            df16 = df16.applymap(lambda x: str(x))
            #df16 = df16.applymap(lambda x: unidecode(x))
            #print(df16)
            df16.rename(columns={df16.columns[0]:''BURIAL_NAME'',
                                df16.columns[1]:''EAN'',
                                df16.columns[2]:''ARTICLE_NAME'',
                                df16.columns[3]:''NORMAL_SALES'',
                                df16.columns[4]:''QTY'',
                                df16.columns[5]:''SALES'',
                                df16.columns[6]:''UNIT_PRICE'',
                                df16.columns[7]:''DC_RATE'',
                                df16.columns[8]:''MARGIN_RATE'',
                                df16.columns[9]:''MARGIN_NORMAL'',
                                df16.columns[10]:''MARGIN_SOLD'',
                                df16.columns[11]:''UNIT_PRC_PRES''},inplace = True)

            
            k  = df16.iloc[0][''BURIAL_NAME'']
            
            df16 = df16.iloc[2:]
            
            df16 = df16[df16.columns[:12]]
            
            df16[''SUB_CUSTOMER_NAME''] = k
            
            df16[''EAN''] = df16[''EAN''].map(lambda x: None if x==''nan'' else x)
            
            df16.dropna(subset=[''EAN''],inplace=True)
            
            #---------------------------------------------------------------------------------

            df17 = excel_file.parse(sheet_name=''아산마트'',engine=''openpyxl'')
            
            df17 = df17.applymap(lambda x: str(x))
            #df17 = df17.applymap(lambda x: unidecode(x))
            #print(df17)
            df17.rename(columns={df17.columns[0]:''BURIAL_NAME'',
                                df17.columns[1]:''EAN'',
                                df17.columns[2]:''ARTICLE_NAME'',
                                df17.columns[3]:''NORMAL_SALES'',
                                df17.columns[4]:''QTY'',
                                df17.columns[5]:''SALES'',
                                df17.columns[6]:''UNIT_PRICE'',
                                df17.columns[7]:''DC_RATE'',
                                df17.columns[8]:''MARGIN_RATE'',
                                df17.columns[9]:''MARGIN_NORMAL'',
                                df17.columns[10]:''MARGIN_SOLD'',
                                df17.columns[11]:''UNIT_PRC_PRES''},inplace = True)

            
            k  = df17.iloc[0][''BURIAL_NAME'']
            
            df17 = df17.iloc[2:]
            
            df17 = df17[df17.columns[:12]]
            
            df17[''SUB_CUSTOMER_NAME''] = k
            
            df17[''EAN''] = df17[''EAN''].map(lambda x: None if x==''nan'' else x)
            
            
            df17.dropna(subset=[''EAN''],inplace=True)
            
            #--------------------------------------------------------------------

            df18 = excel_file.parse(sheet_name=''현대기타'',engine=''openpyxl'')
            
            df18 = df18.applymap(lambda x: str(x))
            #df18 = df18.applymap(lambda x: unidecode(x))
            #print(df18)
            df18.rename(columns={df18.columns[0]:''BURIAL_NAME'',
                                df18.columns[1]:''EAN'',
                                df18.columns[2]:''ARTICLE_NAME'',
                                df18.columns[3]:''NORMAL_SALES'',
                                df18.columns[4]:''QTY'',
                                df18.columns[5]:''SALES'',
                                df18.columns[6]:''UNIT_PRICE'',
                                df18.columns[7]:''DC_RATE'',
                                df18.columns[8]:''MARGIN_RATE'',
                                df18.columns[9]:''MARGIN_NORMAL'',
                                df18.columns[10]:''MARGIN_SOLD'',
                                df18.columns[11]:''UNIT_PRC_PRES''},inplace = True)

            
            k  = df18.iloc[0][''BURIAL_NAME'']
            
            df18 = df18.iloc[2:]
            
            df18 = df18[df18.columns[:12]]
            
            df18[''SUB_CUSTOMER_NAME''] = k
            
            df18[''EAN''] = df18[''EAN''].map(lambda x: None if x==''nan'' else x)
            
            df18.dropna(subset=[''EAN''],inplace=True)

            

            df = pd.concat([df1,df2,df3,df4,df5,df6,df7,df8,df9,df10,df11,df12,df13,df14,df15,df16,df17,df18],axis = 0)

            df[''NORMAL_SALES''] = df[''NORMAL_SALES''].map(lambda x:str(round(float(x))))
            
            snowdf = session.create_dataframe(df)

            snowdf = snowdf.na.drop(''all'')

            #Checking if dataframe is having any data
            
            if snowdf.count()==0:
                return "No Data in file"

            def returnimx_txn_dt(file_name):
                return file_name.split(''.'')[0].split(''_'')[2]

            def returncust_cd(file_name):
                return file_name.split(''.'')[0].split(''_'')[1]
            

            snowdf =  snowdf.with_column(''dstr_nm'',lit(''HYUNDAI''))
            
            snowdf =  snowdf.with_column(''ims_txn_dt'',lit(returnimx_txn_dt(file_name)))
            
            snowdf = snowdf.with_column(''cust_cd'',lit(returncust_cd(file_name)))

            snowparkdf = snowdf.select(''dstr_nm'',
                                       ''ims_txn_dt'',
                                       ''burial_name'',
                                       ''ean'',
                                       ''article_name'',
                                       ''normal_sales'',
                                       ''qty'',
                                       ''sales'',
                                       ''unit_price'',
                                       ''dc_rate'',
                                       ''margin_rate'',
                                       ''margin_normal'',
                                       ''margin_sold'',
                                       ''unit_prc_pres'',
                                       ''sub_customer_name'',
                                       ''cust_cd'')

            snowparkdf.write.mode("append").saveAsTable(target_table)

            current_date = datetime.now()
            formatted_year = current_date.strftime("%Y")
            formatted_month = current_date.strftime("%m")

             # write to success folder
            #file_name = file_name.split(''.'')[0]+"_"+datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")

            

            snowparkdf.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)

            

            

            return ''Success''
            

    except KeyError as key_error:
        # Handle KeyError (missing columns) here
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message
        
    except Exception as e:
        # Handle exceptions here
        error_message = f"Error: {str(e)}"
        return error_message;';


CREATE OR REPLACE PROCEDURE NTASDL_RAW.GT_SELLOUT_LOTTE_AK_PROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('openpyxl==3.1.2','snowflake-snowpark-python==*')
HANDLER = 'main'
EXECUTE AS OWNER
AS '# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them.

import snowflake.snowpark as snowpark
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
#from unidecode import unidecode

from re import sub

def main(session: snowpark.Session,Param):
    

    try:

        #Param=[''Lotte_AK_126137_202312.xlsx'',''NTASDL_RAW.DEV_LOAD_STAGE_ADLS_KOR'',''dev/gt_sellout/transaction_files/department_store/lotte_ak/'',''sdl_kr_lotte_ak_gt_sellout'']

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
            
            df1 = excel_file.parse(sheet_name=''AK수원'',engine=''openpyxl'')


            df1 = df1.applymap(lambda x: str(x))
            #df1 = df1.applymap(lambda x: unidecode(x))
            #print(df1)
            df1.rename(columns={df1.columns[0]:''EAN'',
                                df1.columns[1]:''ARTICLE_NAME'',
                                df1.columns[2]:''NORMAL_SALES'',
                                df1.columns[3]:''UNIT_PRICE'',
                                df1.columns[4]:''DC_RATE'',
                                df1.columns[5]:''QTY'',
                                df1.columns[6]:''EVENT_SALES'',
                                df1.columns[7]:''MARGIN_27'',
                                df1.columns[8]:''MARGIN_22'',
                                df1.columns[9]:''RECEIPT'',
                                df1.columns[10]:''VND_MARGIN_22'',
                                df1.columns[11]:''UNIT_PRC_DIFF'',
                                df1.columns[12]:''FIN_UNIT_PRC''},inplace = True)

            #ean,article_name,normal_sales,unit_price,dc_rate,qty,event_sales,margin_27,margin_22,receipt,vnd_margin_22,unit_prc_diff,fin_unit_prc

            
            k  = df1.iloc[0][''EAN'']
            
            df1 = df1.iloc[3:]
            
            df1 = df1[df1.columns[:13]]
            
            df1[''SUB_CUSTOMER_NAME''] = k
            
            df1[''EAN''] = df1[''EAN''].map(lambda x: None if x==''nan'' else x)
            
            df1.dropna(subset=[''EAN''],inplace=True)

            
#----------------------------------------------------------------------------------------------------------------------------------------------
            df2 = excel_file.parse(sheet_name=''AK분당'',engine=''openpyxl'')


            df2 = df2.applymap(lambda x: str(x))
            #df2 = df2.applymap(lambda x: unidecode(x))
            #print(df2)
            df2.rename(columns={df2.columns[0]:''EAN'',
                                df2.columns[1]:''ARTICLE_NAME'',
                                df2.columns[2]:''NORMAL_SALES'',
                                df2.columns[3]:''UNIT_PRICE'',
                                df2.columns[4]:''DC_RATE'',
                                df2.columns[5]:''QTY'',
                                df2.columns[6]:''EVENT_SALES'',
                                df2.columns[7]:''MARGIN_27'',
                                df2.columns[8]:''MARGIN_22'',
                                df2.columns[9]:''RECEIPT'',
                                df2.columns[10]:''VND_MARGIN_22'',
                                df2.columns[11]:''UNIT_PRC_DIFF'',
                                df2.columns[12]:''FIN_UNIT_PRC''},inplace = True)

            #ean,article_name,normal_sales,unit_price,dc_rate,qty,event_sales,margin_27,margin_22,receipt,vnd_margin_22,unit_prc_diff,fin_unit_prc

            
            k  = df2.iloc[0][''EAN'']
            
            df2 = df2.iloc[3:]
            
            df2 = df2[df2.columns[:13]]
            
            df2[''SUB_CUSTOMER_NAME''] = k
            
            df2[''EAN''] = df2[''EAN''].map(lambda x: None if x==''nan'' else x)
            
            df2.dropna(subset=[''EAN''],inplace=True)

            
            # #---------------------------------------------------------------------------------------------------------------------------------------

            df3 = excel_file.parse(sheet_name=''AK평택'',engine=''openpyxl'')


            df3 = df3.applymap(lambda x: str(x))
            #df3 = df3.applymap(lambda x: unidecode(x))
            #print(df3)
            df3.rename(columns={df3.columns[0]:''EAN'',
                                df3.columns[1]:''ARTICLE_NAME'',
                                df3.columns[2]:''NORMAL_SALES'',
                                df3.columns[3]:''UNIT_PRICE'',
                                df3.columns[4]:''DC_RATE'',
                                df3.columns[5]:''QTY'',
                                df3.columns[6]:''EVENT_SALES'',
                                df3.columns[7]:''MARGIN_27'',
                                df3.columns[8]:''MARGIN_22'',
                                df3.columns[9]:''RECEIPT'',
                                df3.columns[10]:''VND_MARGIN_22'',
                                df3.columns[11]:''UNIT_PRC_DIFF'',
                                df3.columns[12]:''FIN_UNIT_PRC''},inplace = True)

            #ean,article_name,normal_sales,unit_price,dc_rate,qty,event_sales,margin_27,margin_22,receipt,vnd_margin_22,unit_prc_diff,fin_unit_prc

            
            k  = df3.iloc[0][''EAN'']
            
            df3 = df3.iloc[3:]
            
            df3 = df3[df3.columns[:13]]
            
            df3[''SUB_CUSTOMER_NAME''] = k
            
            df3[''EAN''] = df3[''EAN''].map(lambda x: None if x==''nan'' else x)
            
            df3.dropna(subset=[''EAN''],inplace=True)

            

            # #---------------------------------------------------------------------------------------------------

            df4 = excel_file.parse(sheet_name=''명동'',engine=''openpyxl'')


            df4 = df4.applymap(lambda x: str(x))
            #df4 = df4.applymap(lambda x: unidecode(x))
            #print(df4)
            df4.rename(columns={df4.columns[0]:''EAN'',
                                df4.columns[1]:''ARTICLE_NAME'',
                                df4.columns[2]:''NORMAL_SALES'',
                                df4.columns[3]:''UNIT_PRICE'',
                                df4.columns[4]:''DC_RATE'',
                                df4.columns[5]:''QTY'',
                                df4.columns[6]:''EVENT_SALES'',
                                df4.columns[7]:''MARGIN_27'',
                                df4.columns[8]:''MARGIN_22'',
                                df4.columns[9]:''RECEIPT'',
                                df4.columns[10]:''VND_MARGIN_22'',
                                df4.columns[11]:''UNIT_PRC_DIFF'',
                                df4.columns[12]:''FIN_UNIT_PRC''},inplace = True)

            #ean,article_name,normal_sales,unit_price,dc_rate,qty,event_sales,margin_27,margin_22,receipt,vnd_margin_22,unit_prc_diff,fin_unit_prc

            
            k  = df4.iloc[0][''EAN'']
            
            df4 = df4.iloc[3:]
            
            df4 = df4[df4.columns[:13]]
            
            df4[''SUB_CUSTOMER_NAME''] = k
            
            df4[''EAN''] = df4[''EAN''].map(lambda x: None if x==''nan'' else x)
            
            df4.dropna(subset=[''EAN''],inplace=True)
            
            
            
            # #--------------------------------------------------------------------------------------------------

            df5 = excel_file.parse(sheet_name=''잠실'',engine=''openpyxl'')


            df5 = df5.applymap(lambda x: str(x))
            k = df5.columns[0]
            #df5 = df5.applymap(lambda x: unidecode(x))
            #print(df5)
            df5.rename(columns={df5.columns[0]:''EAN'',
                                df5.columns[1]:''ARTICLE_NAME'',
                                df5.columns[2]:''NORMAL_SALES'',
                                df5.columns[3]:''UNIT_PRICE'',
                                df5.columns[4]:''DC_RATE'',
                                df5.columns[5]:''QTY'',
                                df5.columns[6]:''EVENT_SALES'',
                                df5.columns[7]:''MARGIN_27'',
                                df5.columns[8]:''MARGIN_22'',
                                df5.columns[9]:''RECEIPT'',
                                df5.columns[10]:''VND_MARGIN_22'',
                                df5.columns[11]:''UNIT_PRC_DIFF'',
                                df5.columns[12]:''FIN_UNIT_PRC''},inplace = True)

            #ean,article_name,normal_sales,unit_price,dc_rate,qty,event_sales,margin_27,margin_22,receipt,vnd_margin_22,unit_prc_diff,fin_unit_prc

            
            
            
            df5 = df5.iloc[3:]
            
            df5 = df5[df5.columns[:13]]
            
            df5[''SUB_CUSTOMER_NAME''] = k
            
            df5[''EAN''] = df5[''EAN''].map(lambda x: None if x==''nan'' else x)
            
            df5.dropna(subset=[''EAN''],inplace=True)

            
            # #------------------------------------------------------------------------------------------

            df6 = excel_file.parse(sheet_name=''청량리'',engine=''openpyxl'')


            df6 = df6.applymap(lambda x: str(x))
            #df6 = df6.applymap(lambda x: unidecode(x))
            #print(df6)
            k = df6.columns[0]
            
            df6.rename(columns={df6.columns[0]:''EAN'',
                                df6.columns[1]:''ARTICLE_NAME'',
                                df6.columns[2]:''NORMAL_SALES'',
                                df6.columns[3]:''UNIT_PRICE'',
                                df6.columns[4]:''DC_RATE'',
                                df6.columns[5]:''QTY'',
                                df6.columns[6]:''EVENT_SALES'',
                                df6.columns[7]:''MARGIN_27'',
                                df6.columns[8]:''MARGIN_22'',
                                df6.columns[9]:''RECEIPT'',
                                df6.columns[10]:''VND_MARGIN_22'',
                                df6.columns[11]:''UNIT_PRC_DIFF'',
                                df6.columns[12]:''FIN_UNIT_PRC''},inplace = True)

            #ean,article_name,normal_sales,unit_price,dc_rate,qty,event_sales,margin_27,margin_22,receipt,vnd_margin_22,unit_prc_diff,fin_unit_prc

            
            
            
            df6 = df6.iloc[3:]
            
            df6 = df6[df6.columns[:13]]
            
            df6[''SUB_CUSTOMER_NAME''] = k
            
            df6[''EAN''] = df6[''EAN''].map(lambda x: None if x==''nan'' else x)
            
            df6.dropna(subset=[''EAN''],inplace=True)

            
            # #---------------------------------------------------------------------------

            df7 = excel_file.parse(sheet_name=''관악'',engine=''openpyxl'')


            df7 = df7.applymap(lambda x: str(x))
            #df7 = df7.applymap(lambda x: unidecode(x))
            #print(df7)
            df7.rename(columns={df7.columns[0]:''EAN'',
                                df7.columns[1]:''ARTICLE_NAME'',
                                df7.columns[2]:''NORMAL_SALES'',
                                df7.columns[3]:''UNIT_PRICE'',
                                df7.columns[4]:''DC_RATE'',
                                df7.columns[5]:''QTY'',
                                df7.columns[6]:''EVENT_SALES'',
                                df7.columns[7]:''MARGIN_27'',
                                df7.columns[8]:''MARGIN_22'',
                                df7.columns[9]:''RECEIPT'',
                                df7.columns[10]:''VND_MARGIN_22'',
                                df7.columns[11]:''UNIT_PRC_DIFF'',
                                df7.columns[12]:''FIN_UNIT_PRC''},inplace = True)

            #ean,article_name,normal_sales,unit_price,dc_rate,qty,event_sales,margin_27,margin_22,receipt,vnd_margin_22,unit_prc_diff,fin_unit_prc

            
            k  = df7.iloc[0][''EAN'']
            
            df7 = df7.iloc[3:]
            
            df7 = df7[df7.columns[:13]]
            
            df7[''SUB_CUSTOMER_NAME''] = k
            
            df7[''EAN''] = df7[''EAN''].map(lambda x: None if x==''nan'' else x)
            
            df7.dropna(subset=[''EAN''],inplace=True)

            
            # #--------------------------------------------------------------------------
            df8 = excel_file.parse(sheet_name=''분당'',engine=''openpyxl'')


            df8 = df8.applymap(lambda x: str(x))
            #df8 = df8.applymap(lambda x: unidecode(x))
            #print(df8)
            df8.rename(columns={df8.columns[0]:''EAN'',
                                df8.columns[1]:''ARTICLE_NAME'',
                                df8.columns[2]:''NORMAL_SALES'',
                                df8.columns[3]:''UNIT_PRICE'',
                                df8.columns[4]:''DC_RATE'',
                                df8.columns[5]:''QTY'',
                                df8.columns[6]:''EVENT_SALES'',
                                df8.columns[7]:''MARGIN_27'',
                                df8.columns[8]:''MARGIN_22'',
                                df8.columns[9]:''RECEIPT'',
                                df8.columns[10]:''VND_MARGIN_22'',
                                df8.columns[11]:''UNIT_PRC_DIFF'',
                                df8.columns[12]:''FIN_UNIT_PRC''},inplace = True)

            #ean,article_name,normal_sales,unit_price,dc_rate,qty,event_sales,margin_27,margin_22,receipt,vnd_margin_22,unit_prc_diff,fin_unit_prc

            
            k  = df8.iloc[0][''EAN'']
            
            df8 = df8.iloc[3:]
            
            df8 = df8[df8.columns[:13]]
            
            df8[''SUB_CUSTOMER_NAME''] = k
            
            df8[''EAN''] = df8[''EAN''].map(lambda x: None if x==''nan'' else x)
            
            df8.dropna(subset=[''EAN''],inplace=True)

            
            
            # #-----------------------------------------------------------------------
            df9 = excel_file.parse(sheet_name=''영등포'',engine=''openpyxl'')


            df9 = df9.applymap(lambda x: str(x))
            #df9 = df9.applymap(lambda x: unidecode(x))
            #print(df9)
            df9.rename(columns={df9.columns[0]:''EAN'',
                                df9.columns[1]:''ARTICLE_NAME'',
                                df9.columns[2]:''NORMAL_SALES'',
                                df9.columns[3]:''UNIT_PRICE'',
                                df9.columns[4]:''DC_RATE'',
                                df9.columns[5]:''QTY'',
                                df9.columns[6]:''EVENT_SALES'',
                                df9.columns[7]:''MARGIN_27'',
                                df9.columns[8]:''MARGIN_22'',
                                df9.columns[9]:''RECEIPT'',
                                df9.columns[10]:''VND_MARGIN_22'',
                                df9.columns[11]:''UNIT_PRC_DIFF'',
                                df9.columns[12]:''FIN_UNIT_PRC''},inplace = True)

            #ean,article_name,normal_sales,unit_price,dc_rate,qty,event_sales,margin_27,margin_22,receipt,vnd_margin_22,unit_prc_diff,fin_unit_prc

            
            k  = df9.iloc[0][''EAN'']
            
            df9 = df9.iloc[3:]
            
            df9 = df9[df9.columns[:13]]
            
            df9[''SUB_CUSTOMER_NAME''] = k
            
            df9[''EAN''] = df9[''EAN''].map(lambda x: None if x==''nan'' else x)
            
            df9.dropna(subset=[''EAN''],inplace=True)

            snowparkdf = session.create_dataframe(df9)
            
            # #------------------------------------------------------------------------------------------------------------------

            df10 = excel_file.parse(sheet_name=''강남'',engine=''openpyxl'')


            df10 = df10.applymap(lambda x: str(x))
            #df10 = df10.applymap(lambda x: unidecode(x))
            #print(df10)
            df10.rename(columns={df10.columns[0]:''EAN'',
                                df10.columns[1]:''ARTICLE_NAME'',
                                df10.columns[2]:''NORMAL_SALES'',
                                df10.columns[3]:''UNIT_PRICE'',
                                df10.columns[4]:''DC_RATE'',
                                df10.columns[5]:''QTY'',
                                df10.columns[6]:''EVENT_SALES'',
                                df10.columns[7]:''MARGIN_27'',
                                df10.columns[8]:''MARGIN_22'',
                                df10.columns[9]:''RECEIPT'',
                                df10.columns[10]:''VND_MARGIN_22'',
                                df10.columns[11]:''UNIT_PRC_DIFF'',
                                df10.columns[12]:''FIN_UNIT_PRC''},inplace = True)

            #ean,article_name,normal_sales,unit_price,dc_rate,qty,event_sales,margin_27,margin_22,receipt,vnd_margin_22,unit_prc_diff,fin_unit_prc

            
            k  = df10.iloc[0][''EAN'']
            
            df10 = df10.iloc[3:]
            
            df10 = df10[df10.columns[:13]]
            
            df10[''SUB_CUSTOMER_NAME''] = k
            
            df10[''EAN''] = df10[''EAN''].map(lambda x: None if x==''nan'' else x)
            
            df10.dropna(subset=[''EAN''],inplace=True)
            
            # #------------------------------------------------------------------------------------------
            df11 = excel_file.parse(sheet_name=''노원'',engine=''openpyxl'')


            df11 = df11.applymap(lambda x: str(x))
            #df11 = df11.applymap(lambda x: unidecode(x))
            #print(df11)
            df11.rename(columns={df11.columns[0]:''EAN'',
                                df11.columns[1]:''ARTICLE_NAME'',
                                df11.columns[2]:''NORMAL_SALES'',
                                df11.columns[3]:''UNIT_PRICE'',
                                df11.columns[4]:''DC_RATE'',
                                df11.columns[5]:''QTY'',
                                df11.columns[6]:''EVENT_SALES'',
                                df11.columns[7]:''MARGIN_27'',
                                df11.columns[8]:''MARGIN_22'',
                                df11.columns[9]:''RECEIPT'',
                                df11.columns[10]:''VND_MARGIN_22'',
                                df11.columns[11]:''UNIT_PRC_DIFF'',
                                df11.columns[12]:''FIN_UNIT_PRC''},inplace = True)

            #ean,article_name,normal_sales,unit_price,dc_rate,qty,event_sales,margin_27,margin_22,receipt,vnd_margin_22,unit_prc_diff,fin_unit_prc

            
            k  = df11.iloc[0][''EAN'']
            
            df11 = df11.iloc[3:]
            
            df11 = df11[df11.columns[:13]]
            
            df11[''SUB_CUSTOMER_NAME''] = k
            
            df11[''EAN''] = df11[''EAN''].map(lambda x: None if x==''nan'' else x)
            
            df11.dropna(subset=[''EAN''],inplace=True)
            
            # #--------------------------------------------------------------------
            df12 = excel_file.parse(sheet_name=''미아'',engine=''openpyxl'')


            df12 = df12.applymap(lambda x: str(x))
            #df12 = df12.applymap(lambda x: unidecode(x))
            #print(df12)
            df12.rename(columns={df12.columns[0]:''EAN'',
                                df12.columns[1]:''ARTICLE_NAME'',
                                df12.columns[2]:''NORMAL_SALES'',
                                df12.columns[3]:''UNIT_PRICE'',
                                df12.columns[4]:''DC_RATE'',
                                df12.columns[5]:''QTY'',
                                df12.columns[6]:''EVENT_SALES'',
                                df12.columns[7]:''MARGIN_27'',
                                df12.columns[8]:''MARGIN_22'',
                                df12.columns[9]:''RECEIPT'',
                                df12.columns[10]:''VND_MARGIN_22'',
                                df12.columns[11]:''UNIT_PRC_DIFF'',
                                df12.columns[12]:''FIN_UNIT_PRC''},inplace = True)

            #ean,article_name,normal_sales,unit_price,dc_rate,qty,event_sales,margin_27,margin_22,receipt,vnd_margin_22,unit_prc_diff,fin_unit_prc

            
            k  = df12.iloc[0][''EAN'']
            
            df12 = df12.iloc[3:]
            
            df12 = df12[df12.columns[:13]]
            
            df12[''SUB_CUSTOMER_NAME''] = k
            
            df12[''EAN''] = df12[''EAN''].map(lambda x: None if x==''nan'' else x)
            
            df12.dropna(subset=[''EAN''],inplace=True)
            
            # #-----------------------------------------------------------------------------------

            df13 = excel_file.parse(sheet_name=''건대스타'',engine=''openpyxl'')


            df13 = df13.applymap(lambda x: str(x))
            #df13 = df13.applymap(lambda x: unidecode(x))
            #print(df13)
            df13.rename(columns={df13.columns[0]:''EAN'',
                                df13.columns[1]:''ARTICLE_NAME'',
                                df13.columns[2]:''NORMAL_SALES'',
                                df13.columns[3]:''UNIT_PRICE'',
                                df13.columns[4]:''DC_RATE'',
                                df13.columns[5]:''QTY'',
                                df13.columns[6]:''EVENT_SALES'',
                                df13.columns[7]:''MARGIN_27'',
                                df13.columns[8]:''MARGIN_22'',
                                df13.columns[9]:''RECEIPT'',
                                df13.columns[10]:''VND_MARGIN_22'',
                                df13.columns[11]:''UNIT_PRC_DIFF'',
                                df13.columns[12]:''FIN_UNIT_PRC''},inplace = True)

            #ean,article_name,normal_sales,unit_price,dc_rate,qty,event_sales,margin_27,margin_22,receipt,vnd_margin_22,unit_prc_diff,fin_unit_prc

            
            k  = df13.iloc[0][''EAN'']
            
            df13 = df13.iloc[3:]
            
            df13 = df13[df13.columns[:13]]
            
            df13[''SUB_CUSTOMER_NAME''] = k
            
            df13[''EAN''] = df13[''EAN''].map(lambda x: None if x==''nan'' else x)
            
            df13.dropna(subset=[''EAN''],inplace=True)  
            
            # #-----------------------------------------------------------------------

            df14 = excel_file.parse(sheet_name=''중동'',engine=''openpyxl'')


            df14 = df14.applymap(lambda x: str(x))
            #df14 = df14.applymap(lambda x: unidecode(x))
            #print(df14)
            df14.rename(columns={df14.columns[0]:''EAN'',
                                df14.columns[1]:''ARTICLE_NAME'',
                                df14.columns[2]:''NORMAL_SALES'',
                                df14.columns[3]:''UNIT_PRICE'',
                                df14.columns[4]:''DC_RATE'',
                                df14.columns[5]:''QTY'',
                                df14.columns[6]:''EVENT_SALES'',
                                df14.columns[7]:''MARGIN_27'',
                                df14.columns[8]:''MARGIN_22'',
                                df14.columns[9]:''RECEIPT'',
                                df14.columns[10]:''VND_MARGIN_22'',
                                df14.columns[11]:''UNIT_PRC_DIFF'',
                                df14.columns[12]:''FIN_UNIT_PRC''},inplace = True)

            #ean,article_name,normal_sales,unit_price,dc_rate,qty,event_sales,margin_27,margin_22,receipt,vnd_margin_22,unit_prc_diff,fin_unit_prc

            
            k  = df14.iloc[0][''EAN'']
            
            df14 = df14.iloc[3:]
            
            df14 = df14[df14.columns[:13]]
            
            df14[''SUB_CUSTOMER_NAME''] = k
            
            df14[''EAN''] = df14[''EAN''].map(lambda x: None if x==''nan'' else x)
            
            df14.dropna(subset=[''EAN''],inplace=True)
            
            # #-------------------------------------------------------------------------

            df15 = excel_file.parse(sheet_name=''구리'',engine=''openpyxl'')


            df15 = df15.applymap(lambda x: str(x))
            #df15 = df15.applymap(lambda x: unidecode(x))
            #print(df15)
            df15.rename(columns={df15.columns[0]:''EAN'',
                                df15.columns[1]:''ARTICLE_NAME'',
                                df15.columns[2]:''NORMAL_SALES'',
                                df15.columns[3]:''UNIT_PRICE'',
                                df15.columns[4]:''DC_RATE'',
                                df15.columns[5]:''QTY'',
                                df15.columns[6]:''EVENT_SALES'',
                                df15.columns[7]:''MARGIN_27'',
                                df15.columns[8]:''MARGIN_22'',
                                df15.columns[9]:''RECEIPT'',
                                df15.columns[10]:''VND_MARGIN_22'',
                                df15.columns[11]:''UNIT_PRC_DIFF'',
                                df15.columns[12]:''FIN_UNIT_PRC''},inplace = True)

            #ean,article_name,normal_sales,unit_price,dc_rate,qty,event_sales,margin_27,margin_22,receipt,vnd_margin_22,unit_prc_diff,fin_unit_prc

            
            k  = df15.iloc[0][''EAN'']
            
            df15 = df15.iloc[3:]
            
            df15 = df15[df15.columns[:13]]
            
            df15[''SUB_CUSTOMER_NAME''] = k
            
            df15[''EAN''] = df15[''EAN''].map(lambda x: None if x==''nan'' else x)
            
            df15.dropna(subset=[''EAN''],inplace=True)
            
            # #-------------------------------------------------------------------

            

            df16 = excel_file.parse(sheet_name=''안산'',engine=''openpyxl'')


            df16 = df16.applymap(lambda x: str(x))
            #df16 = df16.applymap(lambda x: unidecode(x))
            #print(df16)
            df16.rename(columns={df16.columns[0]:''EAN'',
                                df16.columns[1]:''ARTICLE_NAME'',
                                df16.columns[2]:''NORMAL_SALES'',
                                df16.columns[3]:''UNIT_PRICE'',
                                df16.columns[4]:''DC_RATE'',
                                df16.columns[5]:''QTY'',
                                df16.columns[6]:''EVENT_SALES'',
                                df16.columns[7]:''MARGIN_27'',
                                df16.columns[8]:''MARGIN_22'',
                                df16.columns[9]:''RECEIPT'',
                                df16.columns[10]:''VND_MARGIN_22'',
                                df16.columns[11]:''UNIT_PRC_DIFF'',
                                df16.columns[12]:''FIN_UNIT_PRC''},inplace = True)

            #ean,article_name,normal_sales,unit_price,dc_rate,qty,event_sales,margin_27,margin_22,receipt,vnd_margin_22,unit_prc_diff,fin_unit_prc

            
            k  = df16.iloc[0][''EAN'']
            
            df16 = df16.iloc[3:]
            
            df16 = df16[df16.columns[:13]]
            
            df16[''SUB_CUSTOMER_NAME''] = k
            
            df16[''EAN''] = df16[''EAN''].map(lambda x: None if x==''nan'' else x)
            
            df16.dropna(subset=[''EAN''],inplace=True)
            
            # #---------------------------------------------------------------------------------

            df17 = excel_file.parse(sheet_name=''평촌'',engine=''openpyxl'')


            df17 = df17.applymap(lambda x: str(x))
            #df17 = df17.applymap(lambda x: unidecode(x))
            #print(df17)
            df17.rename(columns={df17.columns[0]:''EAN'',
                                df17.columns[1]:''ARTICLE_NAME'',
                                df17.columns[2]:''NORMAL_SALES'',
                                df17.columns[3]:''UNIT_PRICE'',
                                df17.columns[4]:''DC_RATE'',
                                df17.columns[5]:''QTY'',
                                df17.columns[6]:''EVENT_SALES'',
                                df17.columns[7]:''MARGIN_27'',
                                df17.columns[8]:''MARGIN_22'',
                                df17.columns[9]:''RECEIPT'',
                                df17.columns[10]:''VND_MARGIN_22'',
                                df17.columns[11]:''UNIT_PRC_DIFF'',
                                df17.columns[12]:''FIN_UNIT_PRC''},inplace = True)

            #ean,article_name,normal_sales,unit_price,dc_rate,qty,event_sales,margin_27,margin_22,receipt,vnd_margin_22,unit_prc_diff,fin_unit_prc

            
            k  = df17.iloc[0][''EAN'']
            
            df17 = df17.iloc[3:]
            
            df17 = df17[df17.columns[:13]]
            
            df17[''SUB_CUSTOMER_NAME''] = k
            
            df17[''EAN''] = df17[''EAN''].map(lambda x: None if x==''nan'' else x)
            
            df17.dropna(subset=[''EAN''],inplace=True)
            
            # #--------------------------------------------------------------------

            df18 = excel_file.parse(sheet_name=''수원'',engine=''openpyxl'')


            df18 = df18.applymap(lambda x: str(x))
            #df18 = df18.applymap(lambda x: unidecode(x))
            #print(df18)
            df18.rename(columns={df18.columns[0]:''EAN'',
                                df18.columns[1]:''ARTICLE_NAME'',
                                df18.columns[2]:''NORMAL_SALES'',
                                df18.columns[3]:''UNIT_PRICE'',
                                df18.columns[4]:''DC_RATE'',
                                df18.columns[5]:''QTY'',
                                df18.columns[6]:''EVENT_SALES'',
                                df18.columns[7]:''MARGIN_27'',
                                df18.columns[8]:''MARGIN_22'',
                                df18.columns[9]:''RECEIPT'',
                                df18.columns[10]:''VND_MARGIN_22'',
                                df18.columns[11]:''UNIT_PRC_DIFF'',
                                df18.columns[12]:''FIN_UNIT_PRC''},inplace = True)

            #ean,article_name,normal_sales,unit_price,dc_rate,qty,event_sales,margin_27,margin_22,receipt,vnd_margin_22,unit_prc_diff,fin_unit_prc

            
            k  = df18.iloc[0][''EAN'']
            
            df18 = df18.iloc[3:]
            
            df18 = df18[df18.columns[:13]]
            
            df18[''SUB_CUSTOMER_NAME''] = k
            
            df18[''EAN''] = df18[''EAN''].map(lambda x: None if x==''nan'' else x)
            
            df18.dropna(subset=[''EAN''],inplace=True)
        #-----------------------------------------------------------------------------------------------------------------------

            df19 = excel_file.parse(sheet_name=''동탄'',engine=''openpyxl'')


            df19 = df19.applymap(lambda x: str(x))
            #df19 = df19.applymap(lambda x: unidecode(x))
            #print(df19)
            df19.rename(columns={df19.columns[0]:''EAN'',
                                df19.columns[1]:''ARTICLE_NAME'',
                                df19.columns[2]:''NORMAL_SALES'',
                                df19.columns[3]:''UNIT_PRICE'',
                                df19.columns[4]:''DC_RATE'',
                                df19.columns[5]:''QTY'',
                                df19.columns[6]:''EVENT_SALES'',
                                df19.columns[7]:''MARGIN_27'',
                                df19.columns[8]:''MARGIN_22'',
                                df19.columns[9]:''RECEIPT'',
                                df19.columns[10]:''VND_MARGIN_22'',
                                df19.columns[11]:''UNIT_PRC_DIFF'',
                                df19.columns[12]:''FIN_UNIT_PRC''},inplace = True)

            #ean,article_name,normal_sales,unit_price,dc_rate,qty,event_sales,margin_27,margin_22,receipt,vnd_margin_22,unit_prc_diff,fin_unit_prc

            
            k  = df19.iloc[0][''EAN'']
            
            df19 = df19.iloc[3:]
            
            df19 = df19[df19.columns[:13]]
            
            df19[''SUB_CUSTOMER_NAME''] = k
            
            df19[''EAN''] = df19[''EAN''].map(lambda x: None if x==''nan'' else x)
            
            df19.dropna(subset=[''EAN''],inplace=True)
        
            #-------------------------------------------------------------------------------------------

            df20 = excel_file.parse(sheet_name=''부산'',engine=''openpyxl'')


            df20 = df20.applymap(lambda x: str(x))
            #df20 = df20.applymap(lambda x: unidecode(x))
            #print(df20)
            df20.rename(columns={df20.columns[0]:''EAN'',
                                df20.columns[1]:''ARTICLE_NAME'',
                                df20.columns[2]:''NORMAL_SALES'',
                                df20.columns[3]:''UNIT_PRICE'',
                                df20.columns[4]:''DC_RATE'',
                                df20.columns[5]:''QTY'',
                                df20.columns[6]:''EVENT_SALES'',
                                df20.columns[7]:''MARGIN_27'',
                                df20.columns[8]:''MARGIN_22'',
                                df20.columns[9]:''RECEIPT'',
                                df20.columns[10]:''VND_MARGIN_22'',
                                df20.columns[11]:''UNIT_PRC_DIFF'',
                                df20.columns[12]:''FIN_UNIT_PRC''},inplace = True)

            #ean,article_name,normal_sales,unit_price,dc_rate,qty,event_sales,margin_27,margin_22,receipt,vnd_margin_22,unit_prc_diff,fin_unit_prc

            
            k  = df20.iloc[0][''EAN'']
            
            df20 = df20.iloc[3:]
            
            df20 = df20[df20.columns[:13]]
            
            df20[''SUB_CUSTOMER_NAME''] = k
            
            df20[''EAN''] = df20[''EAN''].map(lambda x: None if x==''nan'' else x)
            
            df20.dropna(subset=[''EAN''],inplace=True)
            
            #---------------------------------------------------------------------
            df21 = excel_file.parse(sheet_name=''대전'',engine=''openpyxl'')


            df21 = df21.applymap(lambda x: str(x))
            #df21 = df21.applymap(lambda x: unidecode(x))
            #print(df21)
            df21.rename(columns={df21.columns[0]:''EAN'',
                                df21.columns[1]:''ARTICLE_NAME'',
                                df21.columns[2]:''NORMAL_SALES'',
                                df21.columns[3]:''UNIT_PRICE'',
                                df21.columns[4]:''DC_RATE'',
                                df21.columns[5]:''QTY'',
                                df21.columns[6]:''EVENT_SALES'',
                                df21.columns[7]:''MARGIN_27'',
                                df21.columns[8]:''MARGIN_22'',
                                df21.columns[9]:''RECEIPT'',
                                df21.columns[10]:''VND_MARGIN_22'',
                                df21.columns[11]:''UNIT_PRC_DIFF'',
                                df21.columns[12]:''FIN_UNIT_PRC''},inplace = True)

            #ean,article_name,normal_sales,unit_price,dc_rate,qty,event_sales,margin_27,margin_22,receipt,vnd_margin_22,unit_prc_diff,fin_unit_prc

            
            k  = df21.iloc[0][''EAN'']
            
            df21 = df21.iloc[3:]
            
            df21 = df21[df21.columns[:13]]
            
            df21[''SUB_CUSTOMER_NAME''] = k
            
            df21[''EAN''] = df21[''EAN''].map(lambda x: None if x==''nan'' else x)
            
            df21.dropna(subset=[''EAN''],inplace=True)
        #----------------------------------------------------------------------------------------------------

            df22 = excel_file.parse(sheet_name=''광주'',engine=''openpyxl'')


            df22 = df22.applymap(lambda x: str(x))
            #df22 = df22.applymap(lambda x: unidecode(x))
            #print(df22)
            df22.rename(columns={df22.columns[0]:''EAN'',
                                df22.columns[1]:''ARTICLE_NAME'',
                                df22.columns[2]:''NORMAL_SALES'',
                                df22.columns[3]:''UNIT_PRICE'',
                                df22.columns[4]:''DC_RATE'',
                                df22.columns[5]:''QTY'',
                                df22.columns[6]:''EVENT_SALES'',
                                df22.columns[7]:''MARGIN_27'',
                                df22.columns[8]:''MARGIN_22'',
                                df22.columns[9]:''RECEIPT'',
                                df22.columns[10]:''VND_MARGIN_22'',
                                df22.columns[11]:''UNIT_PRC_DIFF'',
                                df22.columns[12]:''FIN_UNIT_PRC''},inplace = True)

            #ean,article_name,normal_sales,unit_price,dc_rate,qty,event_sales,margin_27,margin_22,receipt,vnd_margin_22,unit_prc_diff,fin_unit_prc

            
            k  = df22.iloc[0][''EAN'']
            
            df22 = df22.iloc[3:]
            
            df22 = df22[df22.columns[:13]]
            
            df22[''SUB_CUSTOMER_NAME''] = k
            
            df22[''EAN''] = df22[''EAN''].map(lambda x: None if x==''nan'' else x)
            
            df22.dropna(subset=[''EAN''],inplace=True)

        #-------------------------------------------------------------------------------------------------------

            df23 = excel_file.parse(sheet_name=''포항'',engine=''openpyxl'')


            df23 = df23.applymap(lambda x: str(x))
            #df23 = df23.applymap(lambda x: unidecode(x))
            #print(df23)
            df23.rename(columns={df23.columns[0]:''EAN'',
                                df23.columns[1]:''ARTICLE_NAME'',
                                df23.columns[2]:''NORMAL_SALES'',
                                df23.columns[3]:''UNIT_PRICE'',
                                df23.columns[4]:''DC_RATE'',
                                df23.columns[5]:''QTY'',
                                df23.columns[6]:''EVENT_SALES'',
                                df23.columns[7]:''MARGIN_27'',
                                df23.columns[8]:''MARGIN_22'',
                                df23.columns[9]:''RECEIPT'',
                                df23.columns[10]:''VND_MARGIN_22'',
                                df23.columns[11]:''UNIT_PRC_DIFF'',
                                df23.columns[12]:''FIN_UNIT_PRC''},inplace = True)

            #ean,article_name,normal_sales,unit_price,dc_rate,qty,event_sales,margin_27,margin_22,receipt,vnd_margin_22,unit_prc_diff,fin_unit_prc

            
            k  = df23.iloc[0][''EAN'']
            
            df23 = df23.iloc[3:]
            
            df23 = df23[df23.columns[:13]]
            
            df23[''SUB_CUSTOMER_NAME''] = k
            
            df23[''EAN''] = df23[''EAN''].map(lambda x: None if x==''nan'' else x)
            
            df23.dropna(subset=[''EAN''],inplace=True)

        #-----------------------------------------------------------------------------

            df24 = excel_file.parse(sheet_name=''울산'',engine=''openpyxl'')


            df24 = df24.applymap(lambda x: str(x))
            #df24 = df24.applymap(lambda x: unidecode(x))
            #print(df24)
            df24.rename(columns={df24.columns[0]:''EAN'',
                                df24.columns[1]:''ARTICLE_NAME'',
                                df24.columns[2]:''NORMAL_SALES'',
                                df24.columns[3]:''UNIT_PRICE'',
                                df24.columns[4]:''DC_RATE'',
                                df24.columns[5]:''QTY'',
                                df24.columns[6]:''EVENT_SALES'',
                                df24.columns[7]:''MARGIN_27'',
                                df24.columns[8]:''MARGIN_22'',
                                df24.columns[9]:''RECEIPT'',
                                df24.columns[10]:''VND_MARGIN_22'',
                                df24.columns[11]:''UNIT_PRC_DIFF'',
                                df24.columns[12]:''FIN_UNIT_PRC''},inplace = True)

            #ean,article_name,normal_sales,unit_price,dc_rate,qty,event_sales,margin_27,margin_22,receipt,vnd_margin_22,unit_prc_diff,fin_unit_prc

            
            k  = df24.iloc[0][''EAN'']
            
            df24 = df24.iloc[3:]
            
            df24 = df24[df24.columns[:13]]
            
            df24[''SUB_CUSTOMER_NAME''] = k
            
            df24[''EAN''] = df24[''EAN''].map(lambda x: None if x==''nan'' else x)
            
            df24.dropna(subset=[''EAN''],inplace=True)

        #------------------------------------------------------------------------------------

            df25 = excel_file.parse(sheet_name=''동래'',engine=''openpyxl'')


            df25 = df25.applymap(lambda x: str(x))
            #df25 = df25.applymap(lambda x: unidecode(x))
            #print(df25)
            df25.rename(columns={df25.columns[0]:''EAN'',
                                df25.columns[1]:''ARTICLE_NAME'',
                                df25.columns[2]:''NORMAL_SALES'',
                                df25.columns[3]:''UNIT_PRICE'',
                                df25.columns[4]:''DC_RATE'',
                                df25.columns[5]:''QTY'',
                                df25.columns[6]:''EVENT_SALES'',
                                df25.columns[7]:''MARGIN_27'',
                                df25.columns[8]:''MARGIN_22'',
                                df25.columns[9]:''RECEIPT'',
                                df25.columns[10]:''VND_MARGIN_22'',
                                df25.columns[11]:''UNIT_PRC_DIFF'',
                                df25.columns[12]:''FIN_UNIT_PRC''},inplace = True)

            #ean,article_name,normal_sales,unit_price,dc_rate,qty,event_sales,margin_27,margin_22,receipt,vnd_margin_22,unit_prc_diff,fin_unit_prc

            
            k  = df25.iloc[0][''EAN'']
            
            df25 = df25.iloc[3:]
            
            df25 = df25[df25.columns[:13]]
            
            df25[''SUB_CUSTOMER_NAME''] = k
            
            df25[''EAN''] = df25[''EAN''].map(lambda x: None if x==''nan'' else x)
            
            df25.dropna(subset=[''EAN''],inplace=True)

        #--------------------------------------------------------------------------------------------------

            df26 = excel_file.parse(sheet_name=''창원'',engine=''openpyxl'')


            df26 = df26.applymap(lambda x: str(x))
            #df26 = df26.applymap(lambda x: unidecode(x))
            #print(df26)
            df26.rename(columns={df26.columns[0]:''EAN'',
                                df26.columns[1]:''ARTICLE_NAME'',
                                df26.columns[2]:''NORMAL_SALES'',
                                df26.columns[3]:''UNIT_PRICE'',
                                df26.columns[4]:''DC_RATE'',
                                df26.columns[5]:''QTY'',
                                df26.columns[6]:''EVENT_SALES'',
                                df26.columns[7]:''MARGIN_27'',
                                df26.columns[8]:''MARGIN_22'',
                                df26.columns[9]:''RECEIPT'',
                                df26.columns[10]:''VND_MARGIN_22'',
                                df26.columns[11]:''UNIT_PRC_DIFF'',
                                df26.columns[12]:''FIN_UNIT_PRC''},inplace = True)

            #ean,article_name,normal_sales,unit_price,dc_rate,qty,event_sales,margin_27,margin_22,receipt,vnd_margin_22,unit_prc_diff,fin_unit_prc

            
            k  = df26.iloc[0][''EAN'']
            
            df26 = df26.iloc[3:]
            
            df26 = df26[df26.columns[:13]]
            
            df26[''SUB_CUSTOMER_NAME''] = k
            
            df26[''EAN''] = df26[''EAN''].map(lambda x: None if x==''nan'' else x)
            
            df26.dropna(subset=[''EAN''],inplace=True)

        #----------------------------------------------------------------------

            df27 = excel_file.parse(sheet_name=''대구'',engine=''openpyxl'')


            df27 = df27.applymap(lambda x: str(x))
            #df27 = df27.applymap(lambda x: unidecode(x))
            #print(df27)
            df27.rename(columns={df27.columns[0]:''EAN'',
                                df27.columns[1]:''ARTICLE_NAME'',
                                df27.columns[2]:''NORMAL_SALES'',
                                df27.columns[3]:''UNIT_PRICE'',
                                df27.columns[4]:''DC_RATE'',
                                df27.columns[5]:''QTY'',
                                df27.columns[6]:''EVENT_SALES'',
                                df27.columns[7]:''MARGIN_27'',
                                df27.columns[8]:''MARGIN_22'',
                                df27.columns[9]:''RECEIPT'',
                                df27.columns[10]:''VND_MARGIN_22'',
                                df27.columns[11]:''UNIT_PRC_DIFF'',
                                df27.columns[12]:''FIN_UNIT_PRC''},inplace = True)

            #ean,article_name,normal_sales,unit_price,dc_rate,qty,event_sales,margin_27,margin_22,receipt,vnd_margin_22,unit_prc_diff,fin_unit_prc

            
            k  = df27.iloc[0][''EAN'']
            
            df27 = df27.iloc[3:]
            
            df27 = df27[df27.columns[:13]]
            
            df27[''SUB_CUSTOMER_NAME''] = k
            
            df27[''EAN''] = df27[''EAN''].map(lambda x: None if x==''nan'' else x)
            
            df27.dropna(subset=[''EAN''],inplace=True)

        #----------------------------------------------------------------------

            df28 = excel_file.parse(sheet_name=''상인'',engine=''openpyxl'')


            df28 = df28.applymap(lambda x: str(x))
            #df28 = df28.applymap(lambda x: unidecode(x))
            #print(df28)
            df28.rename(columns={df28.columns[0]:''EAN'',
                                df28.columns[1]:''ARTICLE_NAME'',
                                df28.columns[2]:''NORMAL_SALES'',
                                df28.columns[3]:''UNIT_PRICE'',
                                df28.columns[4]:''DC_RATE'',
                                df28.columns[5]:''QTY'',
                                df28.columns[6]:''EVENT_SALES'',
                                df28.columns[7]:''MARGIN_27'',
                                df28.columns[8]:''MARGIN_22'',
                                df28.columns[9]:''RECEIPT'',
                                df28.columns[10]:''VND_MARGIN_22'',
                                df28.columns[11]:''UNIT_PRC_DIFF'',
                                df28.columns[12]:''FIN_UNIT_PRC''},inplace = True)

            #ean,article_name,normal_sales,unit_price,dc_rate,qty,event_sales,margin_27,margin_22,receipt,vnd_margin_22,unit_prc_diff,fin_unit_prc

            
            k  = df28.iloc[0][''EAN'']
            
            df28 = df28.iloc[3:]
            
            df28 = df28[df28.columns[:13]]
            
            df28[''SUB_CUSTOMER_NAME''] = k
            
            df28[''EAN''] = df28[''EAN''].map(lambda x: None if x==''nan'' else x)
            
            df28.dropna(subset=[''EAN''],inplace=True)

        #------------------------------------------------------------------------------------------

            df29 = excel_file.parse(sheet_name=''센텀'',engine=''openpyxl'')


            df29 = df29.applymap(lambda x: str(x))
            #df29 = df29.applymap(lambda x: unidecode(x))
            #print(df29)
            df29.rename(columns={df29.columns[0]:''EAN'',
                                df29.columns[1]:''ARTICLE_NAME'',
                                df29.columns[2]:''NORMAL_SALES'',
                                df29.columns[3]:''UNIT_PRICE'',
                                df29.columns[4]:''DC_RATE'',
                                df29.columns[5]:''QTY'',
                                df29.columns[6]:''EVENT_SALES'',
                                df29.columns[7]:''MARGIN_27'',
                                df29.columns[8]:''MARGIN_22'',
                                df29.columns[9]:''RECEIPT'',
                                df29.columns[10]:''VND_MARGIN_22'',
                                df29.columns[11]:''UNIT_PRC_DIFF'',
                                df29.columns[12]:''FIN_UNIT_PRC''},inplace = True)

            #ean,article_name,normal_sales,unit_price,dc_rate,qty,event_sales,margin_27,margin_22,receipt,vnd_margin_22,unit_prc_diff,fin_unit_prc

            
            k  = df29.iloc[0][''EAN'']
            
            df29 = df29.iloc[3:]
            
            df29 = df29[df29.columns[:13]]
            
            df29[''SUB_CUSTOMER_NAME''] = k
            
            df29[''EAN''] = df29[''EAN''].map(lambda x: None if x==''nan'' else x)
            
            df29.dropna(subset=[''EAN''],inplace=True)

        #----------------------------------------------------------------------------------------------------

            df30 = excel_file.parse(sheet_name=''광복'',engine=''openpyxl'')


            df30 = df30.applymap(lambda x: str(x))
            #df30 = df30.applymap(lambda x: unidecode(x))
            #print(df30)
            df30.rename(columns={df30.columns[0]:''EAN'',
                                df30.columns[1]:''ARTICLE_NAME'',
                                df30.columns[2]:''NORMAL_SALES'',
                                df30.columns[3]:''UNIT_PRICE'',
                                df30.columns[4]:''DC_RATE'',
                                df30.columns[5]:''QTY'',
                                df30.columns[6]:''EVENT_SALES'',
                                df30.columns[7]:''MARGIN_27'',
                                df30.columns[8]:''MARGIN_22'',
                                df30.columns[9]:''RECEIPT'',
                                df30.columns[10]:''VND_MARGIN_22'',
                                df30.columns[11]:''UNIT_PRC_DIFF'',
                                df30.columns[12]:''FIN_UNIT_PRC''},inplace = True)

            #ean,article_name,normal_sales,unit_price,dc_rate,qty,event_sales,margin_27,margin_22,receipt,vnd_margin_22,unit_prc_diff,fin_unit_prc

            
            k  = df30.iloc[0][''EAN'']
            
            df30 = df30.iloc[3:]
            
            df30 = df30[df30.columns[:13]]
            
            df30[''SUB_CUSTOMER_NAME''] = k
            
            df30[''EAN''] = df30[''EAN''].map(lambda x: None if x==''nan'' else x)
            
            df30.dropna(subset=[''EAN''],inplace=True)

        #----------------------------------------------------------------------------------------

            df31 = excel_file.parse(sheet_name=''마산'',engine=''openpyxl'')


            df31 = df31.applymap(lambda x: str(x))
            #df31 = df31.applymap(lambda x: unidecode(x))
            #print(df31)
            df31.rename(columns={df31.columns[0]:''EAN'',
                                df31.columns[1]:''ARTICLE_NAME'',
                                df31.columns[2]:''NORMAL_SALES'',
                                df31.columns[3]:''UNIT_PRICE'',
                                df31.columns[4]:''DC_RATE'',
                                df31.columns[5]:''QTY'',
                                df31.columns[6]:''EVENT_SALES'',
                                df31.columns[7]:''MARGIN_27'',
                                df31.columns[8]:''MARGIN_22'',
                                df31.columns[9]:''RECEIPT'',
                                df31.columns[10]:''VND_MARGIN_22'',
                                df31.columns[11]:''UNIT_PRC_DIFF'',
                                df31.columns[12]:''FIN_UNIT_PRC''},inplace = True)

            #ean,article_name,normal_sales,unit_price,dc_rate,qty,event_sales,margin_27,margin_22,receipt,vnd_margin_22,unit_prc_diff,fin_unit_prc

            
            k  = df31.iloc[0][''EAN'']
            
            df31 = df31.iloc[3:]
            
            df31 = df31[df31.columns[:13]]
            
            df31[''SUB_CUSTOMER_NAME''] = k
            
            df31[''EAN''] = df31[''EAN''].map(lambda x: None if x==''nan'' else x)
            
            df31.dropna(subset=[''EAN''],inplace=True)

        #-------------------------------------------------------------------------------

            df32 = excel_file.parse(sheet_name=''인천'',engine=''openpyxl'')


            df32 = df32.applymap(lambda x: str(x))
            #df32 = df32.applymap(lambda x: unidecode(x))
            #print(df32)
            df32.rename(columns={df32.columns[0]:''EAN'',
                                df32.columns[1]:''ARTICLE_NAME'',
                                df32.columns[2]:''NORMAL_SALES'',
                                df32.columns[3]:''UNIT_PRICE'',
                                df32.columns[4]:''DC_RATE'',
                                df32.columns[5]:''QTY'',
                                df32.columns[6]:''EVENT_SALES'',
                                df32.columns[7]:''MARGIN_27'',
                                df32.columns[8]:''MARGIN_22'',
                                df32.columns[9]:''RECEIPT'',
                                df32.columns[10]:''VND_MARGIN_22'',
                                df32.columns[11]:''UNIT_PRC_DIFF'',
                                df32.columns[12]:''FIN_UNIT_PRC''},inplace = True)

            #ean,article_name,normal_sales,unit_price,dc_rate,qty,event_sales,margin_27,margin_22,receipt,vnd_margin_22,unit_prc_diff,fin_unit_prc

            
            k  = df32.iloc[0][''EAN'']
            
            df32 = df32.iloc[3:]
            
            df32 = df32[df32.columns[:13]]
            
            df32[''SUB_CUSTOMER_NAME''] = k
            
            df32[''EAN''] = df32[''EAN''].map(lambda x: None if x==''nan'' else x)
            
            df32.dropna(subset=[''EAN''],inplace=True)
            
            df = pd.concat([df1,df2,df3,df4,df5,df6,df7,df8,df9,df10,df11,df12,df13,df14,df15,df16,df17,df18,df19,df20,df21,df22,df23,df24,df25,df26,df27,df28,df29,df30,df31,df32],axis = 0)

            #df[''NORMAL_SALES''] = df[''NORMAL_SALES''].map(lambda x:str(round(float(x))))
            
            snowdf = session.create_dataframe(df)

            snowdf = snowdf.na.drop(''all'')

            #Checking if dataframe is having any data
            
            if snowdf.count()==0:
                return "No Data in file"

            def returnimx_txn_dt(file_name):
                return file_name.split(''.'')[0].split(''_'')[3]

            def returncust_cd(file_name):
                return file_name.split(''.'')[0].split(''_'')[2]
            

            snowdf =  snowdf.with_column(''dstr_nm'',lit(''LOTTE_AK''))
            
            snowdf =  snowdf.with_column(''ims_txn_dt'',lit(returnimx_txn_dt(file_name)))
            
            snowdf = snowdf.with_column(''cust_cd'',lit(returncust_cd(file_name)))

            snowparkdf = snowdf.select(''dstr_nm'',
                                       ''ims_txn_dt'',
                                       ''ean'',
                                       ''article_name'',
                                       ''normal_sales'',
                                       ''unit_price'',
                                       ''dc_rate'',
                                       ''qty'',
                                       ''event_sales'',
                                       ''margin_27'',
                                       ''margin_22'',
                                       ''receipt'',
                                       ''vnd_margin_22'',
                                       ''unit_prc_diff'',
                                       ''fin_unit_prc'',
                                       ''sub_customer_name'',
                                       ''cust_cd'')

            snowparkdf.write.mode("append").saveAsTable(target_table)

            current_date = datetime.now()
            formatted_year = current_date.strftime("%Y")
            formatted_month = current_date.strftime("%m")

             # write to success folder
            file_name = file_name.split(''.'')[0]+"_"+datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")

            

            snowparkdf.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)

            

            

            return ''Success''
            

    except KeyError as key_error:
        # Handle KeyError (missing columns) here
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message
        
    except Exception as e:
        # Handle exceptions here
        error_message = f"Error: {str(e)}"
        return error_message;';



CREATE OR REPLACE PROCEDURE NTASDL_RAW.J_KR_ECOMMERCE_EBAY_ETL_FRAMEWORK_PREPROCESSING("PARAM" ARRAY)
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
    #Param=["Weekly_Summary_eBay_raw_data_20240429.csv","NTASDL_RAW.DEV_LOAD_STAGE_ADLS_KOR","dev/ecommerce_sellout/transaction/ebay/","SDL_KR_ECOMMERCE_EBAY_SELLOUT"]
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        
        df_schema = StructType([
                
                StructField("cust_name", StringType(), True),
                StructField("sub_cust_name", StringType(), True),
                StructField("ean_code", StringType(), True),
                StructField("sap_code", StringType(), True),
                StructField("sku_type", StringType(), True),
                StructField("brand", StringType(), True),
                StructField("prod_desc", StringType(), True),
                StructField("year", StringType(), True),
                StructField("month", StringType(), True),
                StructField("week", StringType(), True),
                StructField("transaction_date", StringType(), True),
                StructField("sellout_qty", StringType(), True),
                StructField("sellout_amt", StringType(), True),
                StructField("account_name", StringType(), True),
                StructField("sold_to", StringType(), True)
                
            ])
        
        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",5)\\
            .option("field_delimiter",'''')\\
            .option("encoding","UTF-8")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        #print(df.show())
        
        df=df.na.drop("all")
        if df.count()==0 :
            return "No Data in file"

        
        #Converting file extensiion from xlsx to csv    
        def filename_change(filename):
                filename1 = filename.split(''.'')
                filename1[1]=''xlsx''
                newfilename = ''.''.join(filename1)
                return newfilename
        
        # df = df.with_column("run_id", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        # #file_name1=file_name.split(".")[0]+"_"+ datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")+".csv"
        df = df.with_column("filename", lit(filename_change(file_name)))
        df = df.with_column("crtd_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        # df = df.with_column(''bu_version'', lit(extract_buversion(file_name)))
        # df = df.with_column(''forecast_on_year'',lit(extract_year(file_name)))
        # df = df.with_column(''forecast_on_month'',lit(extract_month(file_name)))
        df = df.with_column(''cust_code'',lit(''133782''))

        snowdf= df.select(
                    "cust_name",
                    "cust_code",
                    "sub_cust_name",
                    "ean_code",
                    "sap_code",
                    "sku_type",
                    "brand",
                    "prod_desc",
                    "year",
                    "month",
                    "week",
                    "transaction_date",
                    "sellout_qty",
                    "sellout_amt",
                    "account_name",
                    "sold_to",
                    "crtd_dttm",
                    "filename"
                    
                    )
        # file_name=file_name.split(".")[0]+"_"+datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
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
        return error_message';
		
CREATE OR REPLACE PROCEDURE NTASDL_RAW.J_KR_ECOMMERCE_TREXI_ETL_FRAMEWORK_PREPROCESSING("PARAM" ARRAY)
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
    #Param=["Weekly_Summary_Trexi_raw_data_20211031.csv","NTASDL_RAW.DEV_LOAD_STAGE_ADLS_KOR","dev/ecommerce_sellout/transaction/trexi/","SDL_KR_ECOMMERCE_TREXI_SELLOUT"]
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        
        df_schema = StructType([
                
                StructField("cust_nm", StringType(), True),
                StructField("sub_cust_nm", StringType(), True),
                StructField("ean_num", StringType(), True),
                StructField("sap_cd", StringType(), True),
                StructField("sku_type", StringType(), True),
                StructField("brand", StringType(), True),
                StructField("prod_desc", StringType(), True),
                StructField("year", StringType(), True),
                StructField("month", StringType(), True),
                StructField("week", StringType(), True),
                StructField("transaction_date", StringType(), True),
                StructField("sellout_qty", StringType(), True),
                StructField("sellout_amt", StringType(), True)
                
                
            ])
        
        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",5)\\
            .option("field_delimiter", "")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .option("encoding","UTF-8")\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        # print(df.show())
        
        df=df.na.drop("all")
        if df.count()==0 :
            return "No Data in file"

        
        #Converting file extensiion from xlsx to csv    
        def filename_change(filename):
                filename1 = filename.split(''.'')
                filename1[1]=''xlsx''
                newfilename = ''.''.join(filename1)
                return newfilename
        
        df = df.with_column("run_id", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        # # #file_name1=file_name.split(".")[0]+"_"+ datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")+".csv"
        df = df.with_column("filename", lit(filename_change(file_name)))
        df = df.with_column("crtd_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        # # df = df.with_column(''bu_version'', lit(extract_buversion(file_name)))
        # # df = df.with_column(''forecast_on_year'',lit(extract_year(file_name)))
        # # df = df.with_column(''forecast_on_month'',lit(extract_month(file_name)))
        df = df.with_column(''cust_code'',lit(''135856''))

        snowdf= df.select(
                    "cust_nm",
                    "cust_code",
                    "sub_cust_nm",
                    "ean_num",
                    "sap_cd",
                    "sku_type",
                    "brand",
                    "prod_desc",
                    "year",
                    "month",
                    "week",
                    "transaction_date",
                    "sellout_qty",
                    "sellout_amt",
                    "crtd_dttm",
                    "filename"
                    
                    )
        # # file_name=file_name.split(".")[0]+"_"+datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
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
        return error_message';

CREATE OR REPLACE PROCEDURE NTASDL_RAW.J_TW_SELL_IN_FORECAST_TRANSACTION_DATA_INGESTION_BP_TARGET_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
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
#import openpyxl
from datetime import datetime
import pytz
#from unidecode import unidecode
from re import sub

def main(session: snowpark.Session,Param):
    

    try:

        #Param=[''BP_BP1_20240101.csv'',''NTASDL_RAW.DEV_LOAD_STAGE_ADLS_TWN'',''dev/taiwan_forecast/transaction/bp_target/'',''sdl_tw_bp_forecast'']

        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        

        #Reading data from adls
        full_path = "@"+stage_name+"/"+temp_stage_path+"/"+file_name
        
        with SnowflakeFile.open(full_path,''rb'',require_scoped_url= False) as f:
            
            df = pd.read_csv(f,delimiter='','')

            df.rename(columns={''customer'':"REGION",
                              ''LPH_Level_6_(original)'':''PROD_NAME'',
                              ''LPH_Level_6'':''LPH_LEVEL_6'',
                              ''Representative_Customer_Number'':''REPRESENTATIVE_CUST_NO'',
                              ''Year'':''FORECAST_FOR_YEAR'',
                              ''Month'':''FORECAST_FOR_MNTH'',
                              "''reference_list''!B6":"PRE_SALES",
                              "''reference_list''!C6":"TP",
                              "''reference_list''!D6":''NTS''},
                              inplace=True
                                 )

            predf = session.createDataFrame(df)

            predf =  predf.with_column(''filename'',lit(file_name))

            predf=predf.na.drop("all")
            if predf.count()==0 :
                return "No Data in file"

            def extract_buversion(data):
                listed = data.split(''_'')
                version = listed[1]
                return version

            def extract_year(data):
                listed = data.split(''_'')
                yearmth = listed[2]
                year_name = yearmth[:4]
                return year_name

            def extract_month(data):
                listed = data.split(''_'')
                yearmth1 = listed[2]
                mth_name = yearmth1[4:6]
                return mth_name

            predf = predf.with_column("run_id", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
            predf = predf.with_column("load_date", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
            predf = predf.with_column(''bp_version'', lit(extract_buversion(file_name)))
            predf = predf.with_column(''forecast_on_year'',lit(extract_year(file_name)))
            predf = predf.with_column(''forecast_on_month'',lit(extract_month(file_name)))

            snowdf = predf.select(''bp_version'',
                                  ''forecast_on_year'',
                                  ''forecast_on_month'',
                                  ''forecast_for_year'',
                                  ''forecast_for_mnth'',
                                  ''region'',
                                  ''prod_name'',
                                  ''lph_level_6'',
                                  ''representative_cust_no'',
                                  ''pre_sales'',
                                  ''tp'',
                                  ''nts'',
                                  ''filename'',
                                  ''run_id'',
                                  ''load_date''
                
            )
            
             # file_name=file_name.split(".")[0]+"_"+datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
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
        return error_message';

CREATE OR REPLACE PROCEDURE NTASDL_RAW.J_TW_SELL_IN_FORECAST_TRANSACTION_DATA_INGESTION_BU_FORECAST_PROD_HIER_PREPROCESSING("PARAM" ARRAY)
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
    #Param=["BU_BU1_107479_202403.csv","NTASDL_RAW.DEV_LOAD_STAGE_ADLS_TWN","dev/taiwan_forecast/transaction/prod_hier/","sdl_tw_bu_forecast_prod_hier"]
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        
        df_schema = StructType([
                StructField("lph_level_6", StringType(), True),
                StructField("product", StringType(), True),
                StructField("representative_cust_no", StringType(), True),
                StructField("forecast_for_year", StringType(), True),
                StructField("forecast_for_mnth", StringType(), True),
                StructField("price_off", StringType(), True),
                StructField("display", StringType(), True),
                StructField("dm", StringType(), True),
                StructField("other_support", StringType(), True),
                StructField("sr", StringType(), True),
                StructField("pre_sales_before_returns", StringType(), True),
                StructField("pre_sales", StringType(), True)
                
            ])
        
        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", ",")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .option("encoding","UTF-8")\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        print(df.show())
        
        df=df.na.drop("all")
        if df.count()==0 :
            return "No Data in file"

        def extract_buversion(data):
            listed = data.split(''_'')
            version = listed[1]
            return version

        def extract_year(data):
            listed = data.split(''_'')
            yearmth = listed[3]
            year_name = yearmth[:4]
            return year_name
            
        
        def extract_month(data):
            listed = data.split(''_'')
            yearmth1 = listed[3]
            mth_name = yearmth1[4:-4]
            return mth_name
            
       
        
        df = df.with_column("run_id", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        #file_name1=file_name.split(".")[0]+"_"+ datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")+".csv"
        df = df.with_column("filename", lit(file_name))
        df = df.with_column("load_date", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        df = df.with_column(''bu_version'', lit(extract_buversion(file_name)))
        df = df.with_column(''forecast_on_year'',lit(extract_year(file_name)))
        df = df.with_column(''forecast_on_month'',lit(extract_month(file_name)))
       

        snowdf= df.select(
                    "bu_version",
                    "forecast_on_year",
                    "forecast_on_month",
                    "forecast_for_year",
                    "forecast_for_mnth",
                    "lph_level_6",
                    "representative_cust_no",
                    "price_off",
                    "display",
                    "dm",
                    "other_support",
                    "sr",
                    "pre_sales_before_returns",
                    "pre_sales",
                    "filename",
                    "run_id",
                    "load_date"
                    
                    )
        # file_name=file_name.split(".")[0]+"_"+datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
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
        return error_message';

CREATE OR REPLACE PROCEDURE NTASDL_RAW.J_TW_SELL_IN_FORECAST_TRANSACTION_DATA_INGESTION_BU_FORECAST_SKU_PREPROCESSING("PARAM" ARRAY)
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
    #Param=["BU_BU1_Carrefour_202403.csv","NTASDL_RAW.DEV_LOAD_STAGE_ADLS_TWN","dev/taiwan_forecast/transaction/","sdl_tw_strategic_cust_hier"]
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        
        df_schema = StructType([
                StructField("sap_code", StringType(), True),
                StructField("representative_cust_no", StringType(), True),
                StructField("forecast_for_year", StringType(), True),
                StructField("forecast_for_mnth", StringType(), True),
                StructField("system_list_price", StringType(), True),
                StructField("gross_invoice_price", StringType(), True),
                StructField("gross_invoice_price_less_terms", StringType(), True),
                StructField("rf_sell_out_qty", StringType(), True),
                StructField("rf_sell_in_qty", StringType(), True),
                StructField("price_off", StringType(), True),
                StructField("pre_sales_before_returns", StringType(), True)
                
                
            ])
        
        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", ",")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .option("encoding","UTF-8")\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        print(df.show())
        
        df=df.na.drop("all")
        if df.count()==0 :
            return "No Data in file"

        def extract_buversion(data):
            listed = data.split(''_'')
            version = listed[1]
            return version

        def extract_year(data):
            listed = data.split(''_'')
            yearmth = listed[3]
            year_name = yearmth[:4]
            return year_name
            
        
        def extract_month(data):
            listed = data.split(''_'')
            yearmth1 = listed[3]
            mth_name = yearmth1[4:-4]
            return mth_name
            
       
        
        df = df.with_column("run_id", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        #file_name1=file_name.split(".")[0]+"_"+ datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")+".csv"
        df = df.with_column("filename", lit(file_name))
        df = df.with_column("load_date", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        df = df.with_column(''bu_version'', lit(extract_buversion(file_name)))
        df = df.with_column(''forecast_on_year'',lit(extract_year(file_name)))
        df = df.with_column(''forecast_on_month'',lit(extract_month(file_name)))
       

        snowdf= df.select(
                    "bu_version",
                    ''forecast_on_year'',
                    ''forecast_on_month'',
                    "sap_code",
                    "representative_cust_no",
                    "forecast_for_year",
                    "forecast_for_mnth",
                    "system_list_price",
                    "gross_invoice_price",
                    "gross_invoice_price_less_terms",
                    "rf_sell_out_qty",
                    "rf_sell_in_qty",
                    "price_off",
                    "pre_sales_before_returns",
                    "filename",
                    "run_id",
                    "load_date"
                    
                    
                    )
        file_name=file_name.split(".")[0]+"_"+datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")

        snowdf.write.mode("append").saveAsTable(stage_name.split(".")[0] + "." + target_table)
        
        #move to success
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
        return error_message';

CREATE OR REPLACE PROCEDURE NTASDL_RAW.KR_DADS_COUPANG_BPA_ADS_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('openpyxl==3.0.10','snowflake-snowpark-python==*')
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

def main(session:snowpark.Session,Param):
    #Param=["A00165277_pa_daily_keyword_20210201_20210228_20230806005011.csv","NTASDL_RAW.DEV_LOAD_STAGE_ADLS_KOR","dev/dads/transaction/couapng_pa/","sdl_KR_coupang_bpa_report"]
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        
        df_schema = StructType([
                StructField("date", StringType(), True),
                StructField("Bidding_Type", StringType(), True),
                StructField("Sales_method", StringType(), True),
                StructField("campaign_start_date", StringType(), True),
                StructField("campaign_end_date", StringType(), True),
                StructField("ad_objectives", StringType(), True),
                StructField("campaign_name", StringType(), True),
                StructField("campaign_id", StringType(), True),
                StructField("ad_group", StringType(), True),
                StructField("ad_group_id", StringType(), True),
                StructField("ad_name", StringType(), True),
                StructField("template_type", StringType(), True),
                StructField("advertisement_id", StringType(), True),
                StructField("impression_area", StringType(), True),
                StructField("material_type", StringType(), True),
                StructField("material", StringType(), True),
                StructField("material_id", StringType(), True),
                StructField("product", StringType(), True),
                StructField("option_id", StringType(), True),
                StructField("ad_execution_product_name", StringType(), True),
                StructField("ad_execution_option_id", StringType(), True),
                StructField("landing_page_type", StringType(), True),
                StructField("landing_page_name", StringType(), True),
                StructField("landing_page_id", StringType(), True),
                StructField("impressed_keywords", StringType(), True),
                StructField("input_keywords", StringType(), True),
                StructField("keyword_extension_type", StringType(), True),
                StructField("category", StringType(), True),
                StructField("impression_count", StringType(), True),
                StructField("click_count", StringType(), True),
                StructField("ctr", StringType(), True),
                StructField("ad_cost", StringType(), True),
                StructField("total_orders_1d", StringType(), True),
                StructField("direct_orders_1d", StringType(), True),
                StructField("indirect_orders_1d", StringType(), True),
                StructField("total_sales_1d", StringType(), True),
                StructField("direct_sales_qty_1d", StringType(), True),
                StructField("indirect_sales_qty_1d", StringType(), True),
                StructField("total_conversion_sales_1d", StringType(), True),
                StructField("direct_conversion_sales_1d", StringType(), True),
                StructField("indirect_conversion_sales_1d", StringType(), True),
                StructField("total_orders_14d", StringType(), True),
                StructField("direct_orders_14d", StringType(), True),
                StructField("indirect_orders_14d", StringType(), True),
                StructField("total_sales_14d", StringType(), True),
                StructField("direct_sales_qty_14d", StringType(), True),
                StructField("indirect_sales_qty_14d", StringType(), True),
                StructField("total_conversion_sales_14d", StringType(), True),
                StructField("direct_conversion_sales_14d", StringType(), True),
                StructField("indirect_conversion_sales_14d", StringType(), True),
                StructField("total_ad_return_1d", StringType(), True),
                StructField("direct_ad_return_1d", StringType(), True),
                StructField("indirect_ad_return_1d", StringType(), True),
                StructField("total_ad_return_14d", StringType(), True),
                StructField("direct_ad_return_14d", StringType(), True),
                StructField("indirect_ad_return_14d", StringType(), True)
               
                
                
               
                
            ])
        
        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .option("encoding",''UTF-8'')\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

       
        
        df=df.na.drop("all")
        if df.count()==0 :
            return "No Data in file"

        

        df = df.na.drop(subset = [''DATE''])
        df = df.na.drop(subset = [''BIDDING_TYPE''])
        df = df.na.drop(subset = [''SALES_METHOD''])
        df = df.na.drop(subset = [''AD_OBJECTIVES''])
        df = df.na.drop(subset = [''CTR''])
        df = df.na.drop(subset = [''AD_EXECUTION_OPTION_ID''])
        # # print(df.columns)
        
        

        df = df.with_column("file_name", lit(file_name))
        # # df = df.with_column(''file_date'',lit(file_date_r(file_name)))
        df = df.with_column("crtd_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))

        snowdf = df.select("date",
                           "bidding_type",
                            "sales_method",
                            "campaign_start_date",
                            "campaign_end_date",
                            "ad_objectives",
                            "campaign_name",
                            "campaign_id",
                            "ad_group",
                            "ad_group_id",
                            "ad_name",
                            "template_type",
                                "advertisement_id",
                                "impression_area",
                                "material_type",
                                "material",
                                "material_id",
                                "product",
                                "option_id",
                                "ad_execution_product_name",
                                "ad_execution_option_id",
                                "landing_page_type",
                                "landing_page_name",
                                "landing_page_id",
                                "impressed_keywords",
                                "input_keywords",
                                "keyword_extension_type",
                                "category",
                                "impression_count",
                                "click_count",
                                "ctr",
                                "ad_cost",
                                "total_orders_1d",
                                "direct_orders_1d",
                                "indirect_orders_1d",
                                "total_sales_1d",
                                "direct_sales_qty_1d",
                                "indirect_sales_qty_1d",
                                "total_conversion_sales_1d",
                                "direct_conversion_sales_1d",
                                "indirect_conversion_sales_1d",
                                "total_orders_14d",
                                "direct_orders_14d",
                                "indirect_orders_14d",
                                "total_sales_14d",
                                "direct_sales_qty_14d",
                                "indirect_sales_qty_14d",
                                "total_conversion_sales_14d",
                                "direct_conversion_sales_14d",
                                "indirect_conversion_sales_14d",
                                "total_ad_return_1d",
                                "direct_ad_return_1d",
                                "indirect_ad_return_1d",
                                "total_ad_return_14d",
                                "direct_ad_return_14d",
                                "indirect_ad_return_14d",
                                 "file_name",
                                  "crtd_dttm")

        file_name=file_name.split(".")[0]+"_"+datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")

        snowdf.write.mode("append").saveAsTable(stage_name.split(".")[0] + "." + target_table)

        #move to success
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

CREATE OR REPLACE PROCEDURE NTASDL_RAW.KR_DADS_COUPANG_PA_ADS_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('openpyxl==3.0.10','snowflake-snowpark-python==*','unidecode==1.2.0')
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

def main(session:snowpark.Session,Param):
    #Param=["A00165277_pa_daily_keyword_20210201_20210228_20230806005011.csv","NTASDL_RAW.DEV_LOAD_STAGE_ADLS_KOR","dev/dads/transaction/couapng_pa/","sdl_KR_coupang_pa_report"]
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        
        df_schema = StructType([
                StructField("date", StringType(), True),
                StructField("Bidding_Type", StringType(), True),
                StructField("Sales_method", StringType(), True),
                StructField("Ad_types", StringType(), True),
                StructField("Campaign_ID", StringType(), True),
                StructField("Campaign_Name", StringType(), True),
                StructField("Ad_groups", StringType(), True),
                StructField("Ad_execution_product_name", StringType(), True),
                StructField("Ad_execution_option_ID", StringType(), True),
                StructField("Ad_con_revenue_gen_product_nm", StringType(), True),
                StructField("Ad_Con_Revenue_Gen_Product_Option_ID", StringType(), True),
                StructField("Ad_Impression_Area", StringType(), True),
                StructField("keyword", StringType(), True),
                StructField("Impression_Count", StringType(), True),
                StructField("Click_Count", StringType(), True),
                StructField("Ad_Cost", StringType(), True),
                StructField("Ctr", StringType(), True),
                StructField("Total_orders_1d", StringType(), True),
                StructField("Direct_orders_1d", StringType(), True),
                StructField("Indirect_orders_1d", StringType(), True),
                StructField("Total_sales_1d", StringType(), True),
                StructField("Direct_sales_quantity_1d", StringType(), True),
                StructField("Indirect_Sales_Quantity_1d", StringType(), True),
                StructField("Total_Conversion_Sales_1d", StringType(), True),
                StructField("Direct_conversion_sales_1d", StringType(), True),
                StructField("Indirect_conversion_sales_1d", StringType(), True),
                StructField("Total_orders_14d", StringType(), True),
                StructField("Direct_orders_14d", StringType(), True),
                StructField("Indirect_orders_14d", StringType(), True),
                StructField("Total_sales_14d", StringType(), True),
                StructField("Direct_Sales_Quantity_14d", StringType(), True),
                StructField("Indirect_Sales_Quantity_14d", StringType(), True),
                StructField("Total_Conversion_Sales_14d", StringType(), True),
                StructField("Direct_conversion_sales_14d", StringType(), True),
                StructField("Indirect_conversion_sales_14d", StringType(), True),
                StructField("Total_ad_return_1d", StringType(), True),
                StructField("Direct_ad_return_1d", StringType(), True),
                StructField("Indirect_ad_return_1d", StringType(), True),
                StructField("Total_ad_return_14d", StringType(), True),
                StructField("Direct_ad_return_14d", StringType(), True),
                StructField("Indirect_Ad_Return_14d", StringType(), True),
                StructField("Campaign_Start_Date", StringType(), True),
                StructField("Campaign_end_Date", StringType(), True)
                
                
                
               
                
            ])
        
        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .option("encoding",''UTF-8'')\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

       
        
        df=df.na.drop("all")
        if df.count()==0 :
            return "No Data in file"

        

        # print(df.columns)
        
        # df_pandas = df.to_pandas()
        # df_pandas[''MOBILE_RATIO_QTY_PRDT_PAID''] = df_pandas[''MOBILE_RATIO_QTY_PRDT_PAID''].apply(lambda x:str(float(x)*100) if isinstance(x,str) else x)
        # df_pandas[''MOBILE_RATIO_PAYMENT_AMOUNT''] = df_pandas[''MOBILE_RATIO_PAYMENT_AMOUNT''].apply(lambda x:str(float(x)*100) if isinstance(x,str) else x)
        # df_pandas[''REFUND_RATE''] = df_pandas[''REFUND_RATE''].apply(lambda x:str(float(x)*100) if isinstance(x,str) else x)
        # df_pandas[''REFUND_QTY_PAID_PRODUCT''] = df_pandas[''REFUND_QTY_PAID_PRODUCT''].apply(lambda x:str(float(x)*100) if isinstance(x,str) else x)

        # df = session.create_dataframe(df_pandas)
        # def file_date_r(file_name):
        #     return file_name.split(''_'')[3]

        df = df.with_column("file_name", lit(file_name))
        # df = df.with_column(''file_date'',lit(file_date_r(file_name)))
        df = df.with_column("crtd_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))

        snowdf = df.select("date",
                            "Bidding_Type" ,
                            "Sales_method",
                            "Ad_types",
                            "Campaign_ID",
                            "Campaign_Name",
                            "Ad_groups",
                            "Ad_execution_product_name",
                            "Ad_execution_option_ID",
                            "Ad_con_revenue_gen_product_nm",
                            "Ad_Con_Revenue_Gen_Product_Option_ID",
                           "Ad_Impression_Area",
                            "keyword",
                            "Impression_Count",
                            "Click_Count",
                            "Ad_Cost",
                            "Ctr",
                            "Total_orders_1d",
                            "Direct_orders_1d",
                            "Indirect_orders_1d",
                            "Total_sales_1d",
                            "Direct_sales_quantity_1d",
                            "Indirect_Sales_Quantity_1d",
                            "Total_Conversion_Sales_1d",
                            "Direct_conversion_sales_1d",
                            "Indirect_conversion_sales_1d",
                            "Total_orders_14d",
                            "Direct_orders_14d",
                            "Indirect_orders_14d",
                            "Total_sales_14d",
                            "Direct_Sales_Quantity_14d",
                            "Indirect_Sales_Quantity_14d",
                            "Total_Conversion_Sales_14d",
                            "Direct_conversion_sales_14d",
                            "Indirect_conversion_sales_14d",
                            "Total_ad_return_1d",
                            "Direct_ad_return_1d",
                            "Indirect_ad_return_1d",
                            "Total_ad_return_14d",
                            "Direct_ad_return_14d",
                            "Indirect_Ad_Return_14d",
                            "Campaign_Start_Date",
                            "Campaign_end_Date",
                            "file_name",
                             "crtd_dttm")

        file_name=file_name.split(".")[0]+"_"+datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")

        snowdf.write.mode("append").saveAsTable(stage_name.split(".")[0] + "." + target_table)

        #move to success
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

CREATE OR REPLACE PROCEDURE NTASDL_RAW.KR_DADS_COUPANG_PRICE_PREPROCESSING("PARAM" ARRAY)
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
    #Param=["Price_-_Products-220516.csv","NTASDL_RAW.DEV_LOAD_STAGE_ADLS_KOR","dev/dads/transaction/couapng_price","sdl_KR_DADS_coupang_price"]
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        
        df_schema = StructType([
                    StructField("report_date", StringType(), True),
                    StructField("trusted_upc", StringType(), True),
                    StructField("trusted_rpc", StringType(), True),
                    StructField("trusted_mpc", StringType(), True),
                    StructField("trusted_product_description", StringType(), True),
                    StructField("region", StringType(), True),
                    StructField("online_store", StringType(), True),
                    StructField("brand", StringType(), True),
                    StructField("manufacturer", StringType(), True),
                    StructField("category", StringType(), True),
                    StructField("dimension1", StringType(), True),
                    StructField("Sub_Category", StringType(), True),
                    StructField("Brand_SubCategory", StringType(), True),
                    StructField("dimension4", StringType(), True),
                    StructField("dimension5", StringType(), True),
                    StructField("dimension6", StringType(), True),
                    StructField("Seller", StringType(), True),
                    StructField("Power_SKU", StringType(), True),
                    StructField("availability_status", StringType(), True),
                    StructField("currency", StringType(), True),
                    StructField("observed_price", StringType(), True),
                    StructField("store_list_price", StringType(), True),
                    StructField("min_price", StringType(), True),
                    StructField("max_price", StringType(), True),
                    StructField("min_max_diff_pct", StringType(), True),
                    StructField("min_max_diff_price", StringType(), True),
                    StructField("msrp", StringType(), True),
                    StructField("msrp_diff_pct", StringType(), True),
                    StructField("msrp_diff_amount", StringType(), True),
                    StructField("previous_day_price", StringType(), True),
                    StructField("previous_day_diff_pct", StringType(), True),
                    StructField("previous_day_diff_amount", StringType(), True),
                    StructField("promotion_text", StringType(), True),
                    StructField("url", StringType(), True)
                        
            ])
        
        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", ",")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

       
        
        df=df.na.drop("all")
        if df.count()==0 :
            return "No Data in file"

        
        
        
        # # df = df.with_column("run_id", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        file_name1=file_name.split(".")[0]+"_"+ datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")+".csv"
        df = df.with_column("filename", lit(file_name1))
        # df = df.with_column("file_date", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d")))
        df = df.with_column("crtd_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        # # df = df.with_column("start_date",lit(start_date_extract(file_name)))
        # # df = df.with_column("end_date",lit(end_date_extract(file_name)))

        snowdf= df.select(
                    "report_date",
                    "trusted_upc",
                    "trusted_rpc",
                    "trusted_mpc",
                    "trusted_product_description",
                    "region",
                    "online_store",
                    "brand",
                    "manufacturer",
                    "category",
                    "dimension1",
                    "Sub_Category",
                    "Brand_SubCategory",
                    "dimension4",
                    "dimension5",
                    "dimension6",
                    "Seller",
                    "Power_SKU",
                    "availability_status",
                    "currency",
                    "observed_price",
                    "store_list_price",
                    "min_price",
                    "max_price",
                    "min_max_diff_pct",
                    "min_max_diff_price",
                    "msrp",
                    "msrp_diff_pct",
                    "msrp_diff_amount",
                    "previous_day_price",
                    "previous_day_diff_pct",
                    "previous_day_diff_amount",
                    "promotion_text",
                    "url",
                    "filename",
                    "crtd_dttm"
                    
                    )
        file_name=file_name.split(".")[0]+"_"+datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")

        snowdf.write.mode("append").saveAsTable(stage_name.split(".")[0] + "." + target_table)
        # # #move to success
        snowdf.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True,file_format_name=''csv_FORMAT'')
        
        
        
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
        return error_message';


CREATE OR REPLACE PROCEDURE NTASDL_RAW.KR_DADS_COUPANG_SEARCH_KEYWORD_PREPROCESSING("PARAM" ARRAY)
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
    #Param=["search-keyword-by-category-A00165277-monthly_20230501-5.xlsx_20230806015237.csv","NTASDL_RAW.DEV_LOAD_STAGE_ADLS_KOR","dev/dads/transaction/cauapng_search_keyword/","sdl_KR_DADS_coupang_search_keyword"]
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        
        df_schema = StructType([
                StructField("By_search_term_ranking", StringType(), True),
                StructField("Product_Ranking_Criteria", StringType(), True),
                StructField("Category_1", StringType(), True),
                StructField("Category_2", StringType(), True),
                StructField("Category_3", StringType(), True),
                StructField("ranking", StringType(), True),
                StructField("Query", StringType(), True),
                StructField("Product_standings", StringType(), True),
                StructField("goods", StringType(), True),
                StructField("My_Products", StringType(), True)
                
            ])
        
        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

       
        
        df=df.na.drop("all")
        if df.count()==0 :
            return "No Data in file"

        # #Extracting start date from file name 
        # def start_date_extract(file_name):
        #     name = file_name.split(''.'')[0]
        #     start_date = name.split(''_'')[-2]
        #     date_start = datetime.strptime(start_date,''%Y%m%d'').date()
        #     return date_start

        # #Extracting end date from filename 
        # def end_date_extract(file_name):
        #     name = file_name.split(''.'')[0]
        #     end_date = name.split(''_'')[-1]
        #     date_end = datetime.strptime(end_date,''%Y%m%d'').date()
        #     return date_end
            
        # # 
        
        
        # df = df.with_column("run_id", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        # #file_name1=file_name.split(".")[0]+"_"+ datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")+".csv"
        df = df.with_column("filename", lit(file_name))
        df = df.with_column("file_date", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d")))
        df = df.with_column("crtd_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        # df = df.with_column("start_date",lit(start_date_extract(file_name)))
        # df = df.with_column("end_date",lit(end_date_extract(file_name)))

        snowdf= df.select(
                    "By_search_term_ranking",
                    "Product_Ranking_Criteria",
                    "Category_1",
                    "Category_2",
                    "Category_3",
                    "ranking",
                    "Query",
                    "Product_standings",
                    "goods",
                    "My_Products",
                    "filename",
                    "file_date",
                    "crtd_dttm"
                    
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
        return error_message';

CREATE OR REPLACE PROCEDURE NTASDL_RAW.KR_DADS_NAVER_ADS_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python==*','unidecode==1.2.0')
HANDLER = 'main'
EXECUTE AS OWNER
AS 'import snowflake.snowpark as snowpark
from snowflake import snowpark
from snowflake.snowpark.window import Window
from snowflake.snowpark.functions import col,lit,concat,regexp_replace,trim,split,rtrim,upper,coalesce,row_number,when,to_date,is_null,substring
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField,DecimalType,DateType
from snowflake.snowpark.files import SnowflakeFile
import pandas as pd
import numpy as np

from datetime import datetime
import pytz

from re import sub

def main(session:snowpark.Session,Param):
    

    try:

        #Param=[''Naver_keyword_20240506_20240512_20240521103442.csv'',''NTASDL_RAW.DEV_LOAD_STAGE_ADLS_KOR'',''dev/dads/transaction/naver_keyword weekly/'',''sdl_KR_DADS_linkprice'']

        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        file_header_loc = 10

        #Reading data from adls
        full_path = "@"+stage_name+"/"+temp_stage_path+"/"+file_name
        
        with SnowflakeFile.open(full_path,''rb'',require_scoped_url= False) as f:
            df = pd.read_csv(f,delimiter="",skiprows=11,names=[''TEMP1'', ''TEMP2'', ''CAMPAIGN_NAME'', ''GROUP_NAME'', ''MATERIAL_ID'', ''PRODUCT_NUMBER'', ''PRODUCT_NAME'', ''IMPRESSISON_AREA'', ''KEYWORD'', ''IMPRESSION'', ''CLICK_COUNT'', ''CTR'', ''IMPRESSION_RANKING'', ''AVG_CLICK_RATE'', ''CONSUMED_COST'', ''CONVERSION_COUNT'', ''CONVERSION_RATE'', ''PURCHASED_AMOUNT'', ''ROAS'', ''PREVIOUS_ROAS'', ''TEMP3'', ''TEMP4'', ''TEMP5'', ''TEMP6''])

            df[''CONVERSION_RATE''] = df[''CONVERSION_RATE''].apply(lambda x: float(x) if isinstance(x,str) else x)
            df[''CONVERSION_RATE''] = df[''CONVERSION_RATE''].apply(lambda x: x*100 if isinstance(x,float) else x)
            df[''CONVERSION_RATE''] = df[''CONVERSION_RATE''].apply(lambda x: str(x)[:5]+''%'' if isinstance(x,float) else x)

            df[''ROAS''] = df[''ROAS''].apply(lambda x: float(x) if isinstance(x,str) else x)
            df[''ROAS''] = df[''ROAS''].apply(lambda x: x*1000 if isinstance(x,float) else x)
            df[''ROAS''] = df[''ROAS''].apply(lambda x: str(x)[:4]+''%'' if isinstance(x,float) else x)

            df[''PREVIOUS_ROAS''] = df[''PREVIOUS_ROAS''].apply(lambda x:float(x) if isinstance(x,str) else x)
            df[''PREVIOUS_ROAS''] = df[''PREVIOUS_ROAS''].apply(lambda x:x*100 if isinstance(x,str) else x)
            df[''PREVIOUS_ROAS''] = df[''PREVIOUS_ROAS''].apply(lambda x: str(x) if isinstance(x,float) else x)                          
            
            snowparkdf = session.create_dataframe(df)

            # print(snowparkdf.columns)


            snowparkdf = snowparkdf[[''CAMPAIGN_NAME'', ''GROUP_NAME'', ''MATERIAL_ID'', ''PRODUCT_NUMBER'', ''PRODUCT_NAME'', ''IMPRESSISON_AREA'', ''KEYWORD'', ''IMPRESSION'', ''CLICK_COUNT'', ''CTR'', ''IMPRESSION_RANKING'', ''AVG_CLICK_RATE'', ''CONSUMED_COST'', ''CONVERSION_COUNT'', ''CONVERSION_RATE'', ''PURCHASED_AMOUNT'', ''ROAS'', ''PREVIOUS_ROAS'']]

            # print(snowparkdf.columns) 

            def file_date(file_name):
                return file_name.split(''_'')[2]
                
            snowparkdf = snowparkdf.with_column(''file_name'',lit(file_name))
            snowparkdf = snowparkdf.with_column(''file_date'',lit(file_date(file_name)))
            snowparkdf = snowparkdf.with_column("crtd_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))

            file_name=file_name.split(".")[0]+"_"+datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
            current_date = datetime.now()
            formatted_year = current_date.strftime("%Y")
            formatted_month = current_date.strftime("%m")

            #move to success
            snowparkdf.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)
    
            snowparkdf.write.mode("append").saveAsTable(stage_name.split(".")[0] + "." + target_table)

            
            return ''Success''

            

    except KeyError as key_error:
        # Handle KeyError (missing columns) here
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message
        
    except Exception as e:
        # Handle exceptions here
        error_message = f"Error: {str(e)}"
        return error_message;';

CREATE OR REPLACE PROCEDURE NTASDL_RAW.KR_DADS_NAVER_GMV_PREPROCESSING("PARAM" ARRAY)
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

def main(session:snowpark.Session,Param):
    #Param=["Product_performance_weekly_20240513_20240519_20240521103448.csv","NTASDL_RAW.DEV_LOAD_STAGE_ADLS_KOR","dev/dads/transaction/sales_product_performance/","sdl_KR_DADS_Naver_GMV"]
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        
        df_schema = StructType([
                StructField("Product_Category_L", StringType(), True),
                StructField("Product_Category_M", StringType(), True),
                StructField("Product_Category_S", StringType(), True),
                StructField("Product_Category_Detail", StringType(), True),
                StructField("Product_Name", StringType(), True),
                StructField("Product_ID", StringType(), True),
                StructField("Number_of_payments", StringType(), True),
                StructField("Quantity_of_products_paid", StringType(), True),
                StructField("Mobile_ratio_qty_prdt_paid", StringType(), True),
                StructField("Payment_amount", StringType(), True),
                StructField("Mobile_Ratio_Payment_Amount", StringType(), True),
                StructField("Pymt_amt_per_prdt_allowance", StringType(), True),
                StructField("Coupon_Total", StringType(), True),
                StructField("Product_coupon", StringType(), True),
                StructField("Order_Coupon", StringType(), True),
                StructField("Refund_Number", StringType(), True),
                StructField("Refund_amount", StringType(), True),
                StructField("Refund_rate", StringType(), True),
                StructField("Refund_Qty", StringType(), True),
                StructField("Refund_Qty_Paid_Product", StringType(), True),
                
                
               
                
            ])
        
        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .option("encoding",''UTF-8'')\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

       
        
        df=df.na.drop("all")
        if df.count()==0 :
            return "No Data in file"

        

        print(df.columns)
        
        df_pandas = df.to_pandas()
        df_pandas[''MOBILE_RATIO_QTY_PRDT_PAID''] = df_pandas[''MOBILE_RATIO_QTY_PRDT_PAID''].apply(lambda x:str(float(x)*100) if isinstance(x,str) else x)
        df_pandas[''MOBILE_RATIO_PAYMENT_AMOUNT''] = df_pandas[''MOBILE_RATIO_PAYMENT_AMOUNT''].apply(lambda x:str(float(x)*100) if isinstance(x,str) else x)
        df_pandas[''REFUND_RATE''] = df_pandas[''REFUND_RATE''].apply(lambda x:str(float(x)*100) if isinstance(x,str) else x)
        df_pandas[''REFUND_QTY_PAID_PRODUCT''] = df_pandas[''REFUND_QTY_PAID_PRODUCT''].apply(lambda x:str(float(x)*100) if isinstance(x,str) else x)

        df = session.create_dataframe(df_pandas)
        def file_date_r(file_name):
            return file_name.split(''_'')[3]

        df = df.with_column("file_name", lit(file_name))
        df = df.with_column(''file_date'',lit(file_date_r(file_name)))
        df = df.with_column("crtd_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))

        snowdf = df.select(''PRODUCT_CATEGORY_L'', 
                           ''PRODUCT_CATEGORY_M'', 
                           ''PRODUCT_CATEGORY_S'', 
                           ''PRODUCT_CATEGORY_DETAIL'', 
                           ''PRODUCT_NAME'', ''PRODUCT_ID'', 
                           ''NUMBER_OF_PAYMENTS'', 
                           ''QUANTITY_OF_PRODUCTS_PAID'', 
                           ''MOBILE_RATIO_QTY_PRDT_PAID'', 
                           ''PAYMENT_AMOUNT'', 
                           ''MOBILE_RATIO_PAYMENT_AMOUNT'', 
                           ''PYMT_AMT_PER_PRDT_ALLOWANCE'', 
                           ''COUPON_TOTAL'', 
                           ''PRODUCT_COUPON'', 
                           ''ORDER_COUPON'', 
                           ''REFUND_NUMBER'', 
                           ''REFUND_AMOUNT'', 
                           ''REFUND_RATE'', 
                           ''REFUND_QTY'', 
                           ''REFUND_QTY_PAID_PRODUCT'',
                           ''FILE_NAME'',
                           ''FILE_DATE'',
                            ''CRTD_DTTM'')

        file_name=file_name.split(".")[0]+"_"+datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")

        snowdf.write.mode("append").saveAsTable(stage_name.split(".")[0] + "." + target_table)

        #move to success
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
CREATE OR REPLACE PROCEDURE NTASDL_RAW.KR_DADS_NAVER_KEYWORD_VOLUME_PREPROCESSING("PARAM" ARRAY)
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

def main(session:snowpark.Session,Param):
    #Param=["JNJ_2024_04_20240503103441.csv","NTASDL_RAW.DEV_LOAD_STAGE_ADLS_KOR","dev/dads/transaction/naver_keyword/","sdl_KR_DADS_naver_keyword_search_volume"]
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        
        df_schema = StructType([
                StructField("No", StringType(), True),
                StructField("keyword", StringType(), True),
                StructField("Total_monthly_searches", StringType(), True),
                StructField("Monthly_desktop_search_volume", StringType(), True),
                StructField("Monthl_mobile_searches", StringType(), True),
                StructField("Average_daily_search_volume", StringType(), True),
                StructField("Keyword_first_appearance_date", StringType(), True),
                StructField("Keyword_Rating", StringType(), True),
                StructField("Adult_keywords", StringType(), True),
                StructField("BlogRecent_Mnthly_Publications", StringType(), True),
                StructField("BlogTotal_Period_Pbliction_Vol", StringType(), True),
                StructField("CafeRecent_Monthly_Issue", StringType(), True),
                StructField("Cafe_Total_Period_Issue ", StringType(), True),
                StructField("VIEW_Recent_Mnthly_Publication", StringType(), True),
                StructField("VIEW_Total_Period_Issuance", StringType(), True),
                StructField("Search_volume_until_yesterday", StringType(), True),
                StructField("Search_vol_end_of_the_month", StringType(), True),
                StructField("Blog_Saturation_Index", StringType(), True),
                StructField("Cafe_Saturation_Index ", StringType(), True),
                StructField("VIEW_Saturation_Index", StringType(), True),
                StructField("Related_Keywords", StringType(), True),
                
                
               
                
            ])
        
        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .option("encoding",''UTF-8'')\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

       
        
        df=df.na.drop("all")
        if df.count()==0 :
            return "No Data in file"

       
        def file_date_r(file_name):
            return file_name.split(''_'')[1]+''01''

        df = df.with_column("file_name", lit(file_name))
        df = df.with_column(''file_date'',lit(file_date_r(file_name)))
        df = df.with_column("crtd_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))

        snowdf = df.select(''NO'', 
                           ''KEYWORD'', 
                           ''TOTAL_MONTHLY_SEARCHES'',
                           ''MONTHLY_DESKTOP_SEARCH_VOLUME'',
                           ''MONTHL_MOBILE_SEARCHES'',
                           ''AVERAGE_DAILY_SEARCH_VOLUME'',
                           ''KEYWORD_FIRST_APPEARANCE_DATE'',
                           ''KEYWORD_RATING'',
                           ''ADULT_KEYWORDS'',
                           ''BLOGRECENT_MNTHLY_PUBLICATIONS'',
                           ''BLOGTOTAL_PERIOD_PBLICTION_VOL'',
                           ''CAFERECENT_MONTHLY_ISSUE'',
                           ''"Cafe_Total_Period_Issue "'',
                           ''VIEW_RECENT_MNTHLY_PUBLICATION'',
                           ''VIEW_TOTAL_PERIOD_ISSUANCE'',
                           ''SEARCH_VOLUME_UNTIL_YESTERDAY'',
                           ''SEARCH_VOL_END_OF_THE_MONTH'',
                           ''BLOG_SATURATION_INDEX'',
                           ''"Cafe_Saturation_Index "'',
                           ''VIEW_SATURATION_INDEX'',
                           ''RELATED_KEYWORDS'',
                           ''FILE_NAME'',
                           ''FILE_DATE'',
                            ''CRTD_DTTM'')

        file_name=file_name.split(".")[0]+"_"+datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")

        snowdf.write.mode("append").saveAsTable(stage_name.split(".")[0] + "." + target_table)

        #move to success
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

CREATE OR REPLACE PROCEDURE NTASDL_RAW.KR_DADS_NAVER_SEARCH_CHANNEL_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS 'from snowflake.snowpark.functions import col, lit, date_format, current_timestamp, to_date, year, month, concat, format_number, regexp_replace,to_timestamp,when,trim,upper,cast
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DecimalType
import snowflake.snowpark
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz
import pandas as pd

def main(session:snowpark.Session,Param):
    #Param=["search_channel_20240401_20240401103507.csv","NTASDL_RAW.DEV_LOAD_STAGE_ADLS_KOR","dev/dads/transaction/naver_search_channel/","sdl_KR_DADS_naver_search_channel"]
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        
        df_schema = StructType([
                StructField("Channel_Properties", StringType(), True),
                StructField("Channel_Groups", StringType(), True),
                StructField("Channel_Name", StringType(), True),
                StructField("keyword", StringType(), True),
                StructField("Customers", StringType(), True),
                StructField("Inlet_water", StringType(), True),
                StructField("Number_of_pages", StringType(), True),
                StructField("Pages_per_inflow", StringType(), True),
                StructField("Number_of_payments", StringType(), True),
                StructField("Payment_rate_per_inflow", StringType(), True),
                StructField("Payment_amount", StringType(), True),
                StructField("Payment_amount_per_inflow", StringType(), True),
                StructField("No_of_payments_14d", StringType(), True),
                StructField("Payment_rate_per_inflow_14d", StringType(), True),
                StructField("Payment_amount_14d", StringType(), True),
                StructField("Payment_amount_per_inflow_14d", StringType(), True)                
                
                
               
                
            ])
        
        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .option("encoding",''UTF-8'')\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

       
        
        df=df.na.drop("all")
        if df.count()==0 :
            return "No Data in file"

       
        def file_date_r(file_name:str):
            return file_name.split(''_'')[2][:6]+''01''

        df = df.with_column("file_name", lit(file_name))
        df = df.with_column(''file_date'',lit(file_date_r(file_name)))
        df = df.with_column("crtd_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        df = df.with_column("Payment_rate_per_inflow",cast(col("Payment_rate_per_inflow"),DecimalType(25,18)))
        df = df.with_column("Payment_rate_per_inflow",col("Payment_rate_per_inflow")*100)
        df = df.with_column("Payment_rate_per_inflow",cast(col("Payment_rate_per_inflow"),StringType()))
        df = df.with_column("Payment_rate_per_inflow",trim(col("Payment_rate_per_inflow"),lit(''0'')))
        
        df = df.with_column("Payment_rate_per_inflow_14d",cast(col("Payment_rate_per_inflow_14d"),DecimalType(30,20)))
        df = df.with_column("Payment_rate_per_inflow_14d",col("Payment_rate_per_inflow_14d")*100)
        df = df.with_column("Payment_rate_per_inflow_14d",cast(col("Payment_rate_per_inflow_14d"),StringType()))
        df = df.with_column("Payment_rate_per_inflow_14d",trim(col("Payment_rate_per_inflow_14d"),lit(''0'')))
        
        snowdf = df.select(''Channel_Properties'',
                          ''Channel_Groups'',
                          ''Channel_Name'',
                          ''keyword'',
                          ''Customers'',
                          ''Inlet_water'',
                          ''Number_of_pages'',
                          ''Pages_per_inflow'',
                          ''Number_of_payments'',
                          ''Payment_rate_per_inflow'',
                          ''Payment_amount'',
                          ''Payment_amount_per_inflow'',
                          ''No_of_payments_14d'',
                          ''Payment_rate_per_inflow_14d'',
                          ''Payment_amount_14d'',
                          ''Payment_amount_per_inflow_14d'',
                          ''file_name'',
                          ''file_date'',
                          ''crtd_dttm'')

        file_name=file_name.split(".")[0]+"_"+datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")

        snowdf.write.mode("append").saveAsTable(stage_name.split(".")[0] + "." + target_table)

        #move to success
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

CREATE OR REPLACE PROCEDURE NTASDL_RAW.KR_ECOM_DSTR_SELLOUT_STOCK_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('openpyxl==3.0.10','snowflake-snowpark-python==*')
HANDLER = 'main'
EXECUTE AS OWNER
AS 'import snowflake.snowpark as snowpark
from snowflake import snowpark
from snowflake.snowpark.window import Window
from snowflake.snowpark.functions import col, lit, concat, regexp_replace, trim, split, rtrim, upper, coalesce, row_number, when, to_date, is_null
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DecimalType, DateType
from snowflake.snowpark.files import SnowflakeFile
import pandas as pd
import numpy as np
import openpyxl
from datetime import datetime
import pytz
from re import sub

def main(session: snowpark.Session,Param):
    try:
        # Param = [''KR_eCom_sku_level_Jun_2023_20240320180008.xlsx'', ''NTASDL_RAW.DEV_LOAD_STAGE_ADLS_KOR'', ''dev/ecom_regional_inventory/transaction/kr_sales_inv'', ''SDL_KR_ECOM_DSTR_SELLOUT_STOCK'']

        file_name = Param[0]
        stage_name = Param[1]
        temp_stage_path = Param[2]
        target_table = Param[3]

        # Reading data from adls
        full_path = f"@{stage_name}/{temp_stage_path}/{file_name}"

        with SnowflakeFile.open(full_path, ''rb'', require_scoped_url=False) as f:
            df = pd.read_excel(f)
        
        # Extracting the data
        df1 = df.iloc[4:, 7:]
        for i in range(1, len(df1.iloc[0])):
            if pd.isna(df1.iloc[0, i]):
                df1.iloc[0, i] = df1.iloc[0, i - 1]

        # Concatenating the values from two rows into a single string
        for i in range(0, len(df1.iloc[0])):
            df1.iloc[0, i] = str(df1.iloc[0, i]) + ''+'' + str(df1.iloc[1, i])

        df1.columns = df1.iloc[0]
        df1 = df1.iloc[2:]
        # print(df1)
        df2 = df.iloc[5:, 2:7]
        # print(df2)
        df2.columns = df2.iloc[0]
        df2 = df2.iloc[1:]
        # print(df2)
        
        df3 = pd.concat([df2, df1], axis=1)
        df_unpivoted = pd.melt(df3, id_vars=df3.columns[:5], value_vars=df3.columns[5:])
        df_unpivoted[[''coded'', ''status'']] = df_unpivoted[''variable''].str.split("+", expand=True)
        df3 = df_unpivoted.where(df_unpivoted[''coded''] != ''TOTAL'')
        df3.dropna(how=''all'', inplace=True)

        df4 = df3.astype(str)
        
        # print(df4.columns)
        df4[''value''] = df4[''value''].apply(lambda x:''0'' if x == ''nan'' else x)
        df4[''value''] = df4[''value''].apply(lambda x: x.strip())
        df4[''value''] = df4[''value''].apply(lambda x:''0'' if x == '''' else x)
        
        df4 = df4.applymap(lambda x: None if x==''nan'' else x)
        df = session.create_dataframe(df4)

        df = df.na.drop("all")
        
        if df.count() == 0:
            return "No Data in file"

        def filename_change(filename):
                filename1 = filename.split(''.'')
                filename1[1]=''csv''
                newfilename = ''.''.join(filename1)
                return newfilename

        def transaction_date(file_name):
            k = file_name
            p = k.split(''_'')
            date = ''-''.join(p[4:6])
            new_date = ''01-''+date
            return datetime.strptime(new_date,''%d-%b-%Y'')
        
        df = df.with_column_renamed(''"SAP"'',"sap")
        df = df.with_column_renamed(''"EAN Code"'',"ean_code")
        df = df.with_column_renamed(''"Brand"'',"brand")
        df = df.with_column_renamed(''"SKU Name"'',"SKU_NAME")
        df = df.with_column_renamed(''"Remark"'',"REMARK")
        df = df.with_column_renamed(''"coded"'',"drtc_cd")
        df = df.with_column_renamed(''"status"'',"drc_src")
        df = df.with_column_renamed(''"value"'',"qty")

        df = df.withColumn("crt_dtm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        df = df.withColumn(''filename'',lit(filename_change(file_name)))
        df = df.withColumn("run_id", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        df = df.withColumn("t_date", lit(transaction_date(file_name)))
        
        snowdf = df.select(
             "SAP", 
             "ean_code",       
            "brand", 
            "SKU_NAME", 
            "Remark", 
            "drtc_cd", 
            "drc_src", 
            "qty",
            "t_date",
            "crt_dtm",
            "filename",
            "run_id"
    
        )
        
        file_name=file_name.split(".")[0]+"_"+ datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        
        #move to success
        snowdf.write.mode("append").saveAsTable(stage_name.split(".")[0] + "." + target_table)
        snowdf.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)

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


CREATE OR REPLACE PROCEDURE NTASDL_RAW.KR_GT_SELLOUT_HYUNDAI_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them.

import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col,lit,trim,when
from snowflake.snowpark.types import  StringType, StructType, StructField,IntegerType
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz



def main(session: snowpark.Session,Param): 
       
    try :
            
        #Param=[''Hyundai_126137_202401.csv'',''NTASDL_RAW.DEV_LOAD_STAGE_ADLS_KOR'',''dev/gt_sellout/transaction_files/department_store/hyundai'',''SDL_KR_HYUNDAI_GT_SELLOUT'']

        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        
        # Define the schema for the DataFrame
        df_schema=StructType([
        StructField("BURIAL_NAME", StringType()),
        StructField("EAN", StringType()),
        StructField("ARTICLE_NAME", StringType()),
        StructField("NORMAL_SALES", StringType()),
        StructField("QTY", StringType()),
        StructField("SALES", StringType()),
        StructField("UNIT_PRICE", StringType()),
        StructField("DC_RATE", StringType()),
        StructField("MARGIN_RATE", StringType()),
        StructField("MARGIN_NORMAL", StringType()),
        StructField("MARGIN_SOLD", StringType()),
        StructField("UNIT_PRC_PRES", StringType())
        ])
                
        dataframe = session.read.schema(df_schema) \\
        .option("skip_header",1) \\
        .option("field_delimiter","\\\\u0001") \\
        .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        if dataframe.count()==0:
            return "No Data in file"

        df_pandas = dataframe.to_pandas()
        
        dstr_nm = "HYUNDAI"
        sub_customer_name = df_pandas.iloc[0,0]    
        imx_txn_dt = file_name.split(''_'')[1]
        cust_cd = file_name.split(''_'')[-1]
        cust_cd = cust_cd.split(''.'')[0]

        

        df_pandas = df_pandas[df_pandas["EAN"].notna() & (df_pandas["EAN"] != "")]
        df_pandas = df_pandas[df_pandas["MARGIN_SOLD"].notna()]
        #df_pandas = df_pandas[df_pandas["EAN"].apply(lambda x:isinstance(x,int))]
    
        df_pandas["DSTR_NM"] = dstr_nm
        df_pandas["CUST_CD"] = cust_cd
        df_pandas["IMS_TXN_DT"] = imx_txn_dt
        df_pandas["SUB_CUSTOMER_NAME"] = sub_customer_name
        
        df = session.create_dataframe(df_pandas)
        snowdf = df.select (
            "DSTR_NM",
            "IMS_TXN_DT",
            "BURIAL_NAME",
            "EAN",
            "ARTICLE_NAME",
            "NORMAL_SALES",
            "QTY",
            "SALES",
            "UNIT_PRICE",
            "DC_RATE",
            "MARGIN_RATE",
            "MARGIN_NORMAL",
            "MARGIN_SOLD",
            "UNIT_PRC_PRES",
            "SUB_CUSTOMER_NAME",
            "CUST_CD"

        )
        
        # Load Data to the target table
        snowdf.write.mode("append").saveAsTable(target_table)
        
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        #move to success
        file_name=file_name.split(".")[0]+''_''+datetime.now().strftime("%Y%m%d%H%M%S")
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
        return error_message';

CREATE OR REPLACE PROCEDURE NTASDL_RAW.KR_GT_SELLOUT_JUNGSEOK_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them.

import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col,lit,trim,when
from snowflake.snowpark.types import  StringType, StructType, StructField,IntegerType
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz



def main(session: snowpark.Session,Param): 
       
    try :
            
        # Param=[''JUNGSEOK_135475_202107.csv'',''NTASDL_RAW.DEV_LOAD_STAGE_ADLS_KOR'',''dev/gt_sellout/transaction_files/food_ws/jungseok'',''SDL_KR_JUNGSEOK_GT_SELLOUT'']

        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        # Define the schema for the DataFrame
        df_schema=StructType([
        StructField(''S_NO'', StringType()),
        StructField(''SUB_CUSTOMER_NAME '', StringType()),
        StructField(''EAN'', StringType()),
        StructField(''NECK_NAME '', StringType()),
        StructField(''THE_RULES'', StringType()),
        StructField(''MASTER_FILE_NAME '', StringType()),
        StructField(''QTY'', StringType()),
        StructField(''AMOUNT'', StringType()),
        StructField(''TAX'', StringType()),
        StructField(''TOTAL'', StringType())
        ])
                
        dataframe = session.read.schema(df_schema) \\
        .option("skip_header",1) \\
        .option("field_delimiter","\\\\u0001") \\
        .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
        

        
        dataframe=dataframe.na.drop("all")
        if dataframe.count()==0:
            return "No Data in file"

        dstr_nm = (''''.join(file_name.split(''_'')[:-1]))
        imx_txn_dt = file_name.split(''_'')[1]
        cust_cd = file_name.split(''_'')[-1]
        cust_cd = cust_cd.split(''.'')[0]

        dataframe= dataframe.withColumn("DSTR_NM", lit(dstr_nm).cast("string"))
        dataframe= dataframe.withColumn("IMS_TXN_DT", lit(imx_txn_dt).cast("string"))
        dataframe= dataframe.withColumn("CUST_CD", lit(cust_cd).cast("string"))

        dataframe = dataframe.filter(col("EAN").is_not_null())
        
        dataframe=dataframe[[
            ''DSTR_NM'',
            ''IMS_TXN_DT'',
            ''S_NO'',
            ''SUB_CUSTOMER_NAME '',
            ''EAN'',
            ''NECK_NAME '',
            ''THE_RULES'',
            ''MASTER_FILE_NAME '',
            ''QTY'',
            ''AMOUNT'',
            ''TAX'',
            ''TOTAL'',
            ''CUST_CD''
        ]]
        
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
        return error_message';


CREATE OR REPLACE PROCEDURE NTASDL_RAW.KR_IL_DONG_HU_DI_S_DEOK_SEONG_SANG_SA_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them.

import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col,lit,trim,when
from snowflake.snowpark.types import  StringType, StructType, StructField,IntegerType
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz



def main(session: snowpark.Session,Param): 
       
    try :
            
        #Param=[''IL DONG HU DI S DEOK SEONG SANG SA.xls'',''NTASDL_RAW.DEV_LOAD_STAGE_ADLS_KOR'',''dev/coupang_premium/transaction'',''sdl_kr_il_dong_hu_di_s_deok_seong_sang_sa_gt_sellout'']

        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        # Define the schema for the DataFrame
        
        df_schema=StructType([
                        StructField("dstr_nm", StringType()),
                        StructField("ccode", StringType()),
                        StructField("sub_customer_name", StringType()),
                        StructField("gcode", StringType()),
                        StructField("on_site_name", StringType()),
                        StructField("year", StringType()),
                        StructField("ims_txn_dt", StringType()),
                        StructField("transaction_number", StringType()),
                        StructField("product_classification", StringType()),
                        StructField("product_code", StringType()),
                        StructField("management_code", StringType()),
                        StructField("ean", StringType()),
                        StructField("prize_name", StringType()),
                        StructField("classification", StringType()),
                        StructField("rules", StringType()),
                        StructField("color", StringType()),
                        StructField("delivery_date", StringType()),
                        StructField("deliver", StringType()),
                        StructField("factory_status", StringType()),
                        StructField("number_of_goods", StringType()),
                        StructField("BOX", StringType()),
                        StructField("one_piece", StringType()),
                        StructField("qty", StringType()),
                        StructField("weight", StringType()),
                        StructField("list", StringType()),
                        StructField("unit_price_rate", StringType()),
                        StructField("box_danga", StringType()),
                        StructField("unit_price", StringType()),
                        StructField("cust_cd", StringType())
                        ])
                
        dataframe = session.read.schema(df_schema) \\
        .option("skip_header",1) \\
        .option("field_delimiter","\\u0001") \\
        .option("encoding","UTF-8")\\
        .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
        

        
        dataframe=dataframe.na.drop("all")
        if dataframe.count()==0:
            return "No Data in file"

        
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

CREATE OR REPLACE PROCEDURE NTASDL_RAW.KR_JU_HJ_LIFE_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them.

import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col,lit,trim,when
from snowflake.snowpark.types import  StringType, StructType, StructField,IntegerType
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz



def main(session: snowpark.Session,Param): 
       
    try :
            
        #Param=[''JU_HJ_LIFE_125235.csv'',''NTASDL_RAW.DEV_LOAD_STAGE_ADLS_KOR'',''dev/gt_sellout/transaction_files/food_ws/ju_hj_life'',''sdl_kr_ju_hj_life_gt_sellout'']

        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        # Define the schema for the DataFrame
        df_schema=StructType([
        StructField("IMS_TXN_DT", StringType()),
        StructField("SUB_CUSTOMER_NAME", StringType()),
        StructField("ITEMS", StringType()),
        StructField("EAN", StringType()),
        StructField("QTY", StringType()),
        StructField("UNIT", StringType()),
        StructField("TOTAL_SALES", StringType()),
        StructField("SEE", StringType())])
                
        dataframe = session.read.schema(df_schema) \\
        .option("skip_header",1) \\
        .option("field_delimiter","\\\\u0001") \\
        .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
        

        
        dataframe=dataframe.na.drop("all")
        if dataframe.count()==0:
            return "No Data in file"

        dstr_nm = (''''.join(file_name.split(''_'')[:-1]))
        cust_cd = file_name.split(''_'')[-1]
        cust_cd = cust_cd.split(''.'')[0]

        dataframe= dataframe.withColumn("DSTR_NM", lit(dstr_nm).cast("string"))
        dataframe= dataframe.withColumn("CUST_CD", lit(cust_cd).cast("string"))

        dataframe=dataframe[[''DSTR_NM'',
            ''IMS_TXN_DT'',
            ''SUB_CUSTOMER_NAME'',
            ''ITEMS'',
            ''EAN'',
            ''QTY'',
            ''UNIT'',
            ''TOTAL_SALES'',
            ''SEE'',
            ''CUST_CD'']]
        
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
        return error_message';

CREATE OR REPLACE PROCEDURE NTASDL_RAW.KR_NACF_GT_SELLOUT_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS 'from snowflake.snowpark.functions import col, lit, date_format, current_timestamp, to_date, year, month, concat, format_number, regexp_replace, to_timestamp, when, trim, upper
from snowflake.snowpark.types import StringType, StructType, StructField
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz

def main(session: snowpark.Session,Param):
    # Param=[''NACF.csv'',''NTASDL_RAW.DEV_LOAD_STAGE_ADLS_KOR'',''dev/gt_sellout/transaction_files/nacf'',''SDL_KR_NACF_GT_SELLOUT'']
    try:
        file_name = Param[0]
        stage_name = Param[1]
        temp_stage_path = Param[2]
        target_table = Param[3]
        
        df_schema = StructType([
            StructField("customer_code", StringType(), True),
            StructField("account_name", StringType(), True),
            StructField("ims_txn_dt", StringType(), True),
            StructField("inspection_date", StringType(), True),
            StructField("scheduled_date_of_payment", StringType(), True),
            StructField("scheduled_delivery_number", StringType(), True),
            StructField("innovation_design", StringType(), True),
            StructField("pb", StringType(), True),
            StructField("sub_customer_code", StringType(), True),
            StructField("sub_customer_name", StringType(), True),
            StructField("economic_integration", StringType(), True),
            StructField("business_name", StringType(), True),
            StructField("supply_type", StringType(), True),
            StructField("system_contract_classification", StringType(), True),
            StructField("document_serial_number", StringType(), True),
            StructField("ean", StringType(), True),
            StructField("year_of_production", StringType(), True),
            StructField("trade_name", StringType(), True),
            StructField("product_standard_name", StringType(), True),
            StructField("tax_code_name", StringType(), True),
            StructField("wearing_weight", StringType(), True),
            StructField("quantity_of_goods", StringType(), True),
            StructField("unit_price", StringType(), True),
            StructField("sales_qty", StringType(), True),
            StructField("supply_amount", StringType(), True),
            StructField("purchase_tax", StringType(), True),
            StructField("purchase_amount", StringType(), True),
            StructField("commission_accout_code", StringType(), True),
            StructField("commission_accout_name", StringType(), True),
            StructField("Commission", StringType(), True),
            StructField("Commission_tax", StringType(), True),
            StructField("Commission_code", StringType(), True),
            StructField("Commission_name", StringType(), True),
            StructField("Transaction_type_code", StringType(), True),
            StructField("Transaction_type_name", StringType(), True),
            StructField("Correction/cancel_type", StringType(), True),
            StructField("Correction/cancel_type_name", StringType(), True),
            StructField("Delivery_Schedule_Write", StringType(), True)
         
        ])

        
        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
        
        df = df.na.drop("all")
        
        if df.count() == 0:
            return "No Data in file"

        def extract_buversion(data):
                listed = data.split(''.'')
                version = listed[0]
                return version

        df = df.with_column(''dstr_nm'', lit(extract_buversion(file_name)))
        df = df.withColumn("trade_name", lit("NULL"))
        # Remove additional characters after the first 10 characters in ims_txn_dt column
        df = df.withColumn("ims_txn_dt", col("ims_txn_dt").substr(1, 10))
            
        # df = df.withColumn("crt_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        # df = df.withColumn("file_name", lit(file_name))
        # df = df.withColumn("run_id", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))

        snowdf = df.select(
             "dstr_nm", 
            "customer_code",      
            "account_name", 
            "ims_txn_dt", 
            "inspection_date", 
            "scheduled_date_of_payment", 
            "scheduled_delivery_number", 
            "innovation_design", 
            "pb", 
            "sub_customer_code", 
            "sub_customer_name", 
            "economic_integration", 
            "business_name", 
            "supply_type", 
            "system_contract_classification", 
            "document_serial_number", 
            "ean",
            "year_of_production",
            "trade_name",
            "product_standard_name",
            "tax_code_name",
            "wearing_weight",
            "quantity_of_goods",
            "unit_price",
            "sales_qty",
            "supply_amount",
            "purchase_tax",
            "purchase_amount",
        )

        
        file_name=file_name.split(".")[0]+"_"+ datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        
        #move to success
        snowdf.write.mode("append").saveAsTable(stage_name.split(".")[0] + "." + target_table)
        snowdf.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)
        
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

CREATE OR REPLACE PROCEDURE NTASDL_RAW.KR_NH_GT_SELLOUT_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS 'from snowflake.snowpark.functions import col, lit, date_format, current_timestamp, to_date, year, month, concat, format_number, regexp_replace, to_timestamp, when, upper
from snowflake.snowpark.types import StringType, StructType, StructField
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz

def main(session: snowpark.Session,Param):
    # Param=[''NH_202403.csv'',''NTASDL_RAW.DEV_LOAD_STAGE_ADLS_KOR'',''dev/gt_sellout/transaction_files/nh'',''SDL_KR_NH_GT_SELLOUT'']
    try:
        file_name = Param[0]
        stage_name = Param[1]
        temp_stage_path = Param[2]
        target_table = Param[3]
        
        df_schema = StructType([
            StructField("Customer Code", StringType(), True),
            StructField("Customer Name", StringType(), True),
            StructField("ims_txn_dt", StringType(), True),
            StructField("account_name", StringType(), True),     
            StructField("product_description", StringType(), True),
            StructField("ean", StringType(), True),
            StructField("quantity", StringType(), True),
            StructField("amount", StringType(), True),   
        ])

        
        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
        
        if df.count() == 0:
            return "No Data in file"

        def extract_buversion(data):
                listed = data.split(''_'')
                version = listed[0]
                return version

        df = df.with_column(''dstr_nm'', lit(extract_buversion(file_name)))

        df = df.withColumn("ims_txn_dt",when(col(''ims_txn_dt'')==None,lit(None)).otherwise( regexp_replace(col("ims_txn_dt"), r"(\\d{2})(\\d{2})-([0]*)([0-9]*)-([0]*)([0-9]*)", r"\\4/\\6/\\2")))

            
        # df = df.withColumn("crt_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        df = df.withColumn("filename", lit(file_name))
        # df = df.withColumn("run_id", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))

        snowdf = df.select(
             "dstr_nm", 
             "Customer Code",       
            "ims_txn_dt", 
            "account_name", 
            "product_description", 
            "ean", 
            "quantity", 
            "amount",
            "filename",
        )

        
        file_name=file_name.split(".")[0]+"_"+ datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        
        #move to success
        snowdf.write.mode("append").saveAsTable(stage_name.split(".")[0] + "." + target_table)
        snowdf.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)
        
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


CREATE OR REPLACE PROCEDURE NTASDL_RAW.KR_NU_RI_ZON_GT_SELLOUT_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS 'from snowflake.snowpark.functions import col, lit, date_format, current_timestamp, to_date, year, month, concat, format_number, regexp_replace, to_timestamp, when, upper
from snowflake.snowpark.types import StringType, StructType, StructField
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz

def main(session: snowpark.Session,Param):
    # Param=[''NU_RI_ZON_125017.csv'',''NTASDL_RAW.DEV_LOAD_STAGE_ADLS_KOR'',''dev/gt_sellout/transaction_files/food_ws/nu_ri_zon'',''SDL_KR_NU_RI_ZON_GT_SELLOUT'']
    try:
        file_name = Param[0]
        stage_name = Param[1]
        temp_stage_path = Param[2]
        target_table = Param[3]
        
        df_schema = StructType([
            StructField("ims_txn_dt", StringType(), True),
            StructField("sub_customer_Name", StringType(), True),     
            StructField("ean", StringType(), True),
            StructField("name", StringType(), True),
            StructField("standard", StringType(), True),
            StructField("qty", StringType(), True),
            StructField("unit_price", StringType(), True),
            StructField("total", StringType(), True),

        ])

        
        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
        
        if df.count() == 0:
            return "No Data in file"

        def extract_buversion(data):
                listed = data.split(''_'')
                version = listed[:3]
                new_var = '' ''.join(version)
                return new_var

        df = df.with_column(''dstr_nm'', lit(extract_buversion(file_name)))
        df = df.withColumn("cust_cd", lit("125017"))

            
        # df = df.withColumn("crt_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        # df = df.withColumn("filename", lit(file_name))
        # df = df.withColumn("run_id", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))

        snowdf = df.select(
             "dstr_nm", 
            "ims_txn_dt", 
            "sub_customer_Name",      
            "ean", 
            "name", 
            "standard", 
            "qty", 
            "unit_price", 
            "total",
            "cust_cd"
        )

        
        # file_name=file_name.split(".")[0]+"_"+ datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        
        #move to success
        snowdf.write.mode("append").saveAsTable(stage_name.split(".")[0] + "." + target_table)
        snowdf.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)
        
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

CREATE OR REPLACE PROCEDURE NTASDL_RAW.KR_OTC_SELLOUT_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS 'from snowflake.snowpark.functions import col, lit, date_format, current_timestamp, to_date, year, month, concat, format_number, regexp_replace, to_timestamp, when,sum
from snowflake.snowpark.types import StringType, StructType, StructField
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz

def main(session: snowpark.Session,Param):
    # Param=[''KR_OTC_202402_20240514110205.csv'',''NTASDL_RAW.DEV_LOAD_STAGE_ADLS_KOR'',''dev/otc_sellout/transaction/korea_otc_data_ingestion'',''SDL_KR_OTC_SELLOUT'']
    try:
        file_name = Param[0]
        stage_name = Param[1]
        temp_stage_path = Param[2]
        target_table = Param[3]
        
        df_schema = StructType([
            StructField("Customer Code", StringType(), True),
            StructField("customer_Name", StringType(), True),     
            StructField("ims_txn_dt", StringType(), True),
            StructField("account_name", StringType(), True),
            StructField("product_description", StringType(), True),
            StructField("ean", StringType(), True),
            StructField("quantity", StringType(), True),
            StructField("amount", StringType(), True),
            StructField("pcode", StringType(), True),     
        ])

        
        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
        
        df = df.na.drop("all")
        
        if df.count() == 0:
            return "No Data in file"

        def extract_buversion(data):
                listed = data.split(''_'')
                version = listed[1]
                return version

        df = df.with_column(''dstr_nm'', lit(extract_buversion(file_name)))
        
        df_pandas=df.to_pandas()
         # Truncate the values after the decimal point in the "amount" column
        df_pandas[''AMOUNT''] = df_pandas[''AMOUNT''].apply(lambda x: x.split(''.'')[0]) 
        df = session.createDataFrame(df_pandas)

        df = df.groupBy([''dstr_nm'', ''Customer Code'', ''ims_txn_dt'', ''account_name'', ''ean'', ''pcode'']).agg(sum(''quantity'').alias(''quantity''),sum(''amount'').alias(''amount''))
        df = df.withColumn("product_description", lit("NA"))
        df = df.withColumn("ims_txn_dt",when(col(''ims_txn_dt'')==None,lit(None)).otherwise( regexp_replace(col("ims_txn_dt"), r"(\\d{2})(\\d{2})-([0]*)([0-9]*)-([0]*)([0-9]*)", r"\\4/\\6/\\2")))
        
        # df = df.withColumn("crt_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        df = df.withColumn("filename", lit(file_name))
        # df = df.withColumn("run_id", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))

        snowdf = df.select(
             "dstr_nm", 
             "Customer Code",       
            "ims_txn_dt", 
            "account_name", 
            "product_description", 
            "ean", 
            "quantity", 
            "amount",
            "filename",
            "pcode"
        )

        
        file_name=file_name.split(".")[0]+"_"+ datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        
        #move to success
        snowdf.write.mode("append").saveAsTable(stage_name.split(".")[0] + "." + target_table)
        snowdf.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)
        
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

CREATE OR REPLACE PROCEDURE NTASDL_RAW.KR_POS_ELAND_PREPROCESSING("PARAM" ARRAY)
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
    #Param=["KR_POS_ELand_2024-05-18.csv","NTASDL_RAW.DEV_LOAD_STAGE_ADLS_KOR","dev/pos/transaction/e-land/","sdl_kr_pos_eland"]
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        
        df_schema = StructType([
                StructField("pos_date", StringType(), True),
                StructField("store_code", StringType(), True),
                StructField("store_name", StringType(), True),
                StructField("currency", StringType(), True),
                StructField("barcode", StringType(), True),
                StructField("product_name", StringType(), True),
                StructField("number_of_sales", StringType(), True),
                StructField("sales_rvenue_incl_vat", StringType(), True),
                StructField("sales_rvenue_excl_vat", StringType(), True)
                
            ])
        
        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .option("encoding",''UTF-8'')\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

       
        
        df=df.na.drop("all")
        if df.count()==0 :
            return "No Data in file"

        
        
        
        df = df.with_column("run_id", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        #file_name1=file_name.split(".")[0]+"_"+ datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")+".csv"
        df = df.with_column("file_name", lit(file_name))
        df = df.with_column("crtd_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        df = df.with_column("upd_dttm", lit(None))
        
        snowdf= df.select(
                    "pos_date",
                    "store_code",
                    "store_name",
                    "currency",
                    "barcode",
                    "product_name",
                    "number_of_sales",
                    "sales_rvenue_incl_vat",
                    "sales_rvenue_excl_vat",
                    "file_name",
                    "run_id",
                    "crtd_dttm",
                    "upd_dttm"
                    )
        file_name=file_name.split(".")[0]+"_"+datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")

        snowdf.write.mode("append").saveAsTable(stage_name.split(".")[0] + "." + target_table)
        
        #move to success
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

CREATE OR REPLACE PROCEDURE NTASDL_RAW.KR_POS_EMART_SSG_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('regex==2023.10.3','snowflake-snowpark-python==*')
HANDLER = 'main'
EXECUTE AS OWNER
AS 'from snowflake.snowpark.functions import col, lit,concat
from snowflake.snowpark.types import  StringType, StructType, StructField
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz

def main(session: snowpark.Session,Param): 
    try :
        

        #Param=[''KR_POS_SSG_2024-05-15.csv'',''NTASDL_RAW.DEV_LOAD_STAGE_ADLS_KOR'',''dev/pos/transaction/emart_online/'',''SDL_KR_POS_EMART_SSG'']

        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        # Define the schema for the DataFrame
        df_schema = StructType([
                StructField("STR_NM", StringType()),
                StructField("TEAM_NM", StringType()),
                StructField("LRG_CLASSIFICATION_NM", StringType()),
                StructField("MID_CLASSIFICATION_NM", StringType()),
                StructField("SUB_CLASSIFIED_NM", StringType()),
                StructField("OFFLINE_EAN", StringType()),
                StructField("EAN", StringType()),
                StructField("PROD_NM", StringType()),
                StructField("POS_DT", StringType()),
                StructField("SELLOUT_QTY", StringType()),
                StructField("SELLOUT_AMT", StringType()),
                StructField("SUPPLIERS", StringType()),
                StructField("PRODUCT_TYPE", StringType())
            ])
        
        dataframe = session.read\\
        .schema(df_schema)\\
        .option("skip_header",1)\\
        .option("field_delimiter", "\\u0001")\\
        .option("field_optionally_enclosed_by", "\\"")\\
        .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)



        #---------------------------Transformation logic ------------------------------#
 
        # Handle null values or empty rows
        dataframe=dataframe.na.drop("all")
 
        # Check for empty Dataframe
        if dataframe.count()==0:
            return "No Data in file"
        
        dataframe = dataframe.with_column("STR_CD", lit(None).cast("string"))
        dataframe = dataframe.with_column("STR_NM", concat(col("STR_NM"), lit("_online")))
        file_name1 = file_name.split(".")[0] + ".xls"
        dataframe = dataframe.with_column("FILENAME",lit(file_name1))
        dataframe = dataframe.with_column("CRTD_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        # # Creating copy of the Dataframe
        final_df = dataframe.select("STR_NM", "STR_CD", "TEAM_NM", "LRG_CLASSIFICATION_NM", "MID_CLASSIFICATION_NM","SUB_CLASSIFIED_NM","OFFLINE_EAN","EAN",\\
                                   "PROD_NM","POS_DT","SELLOUT_QTY","SELLOUT_AMT","SUPPLIERS","PRODUCT_TYPE","CRTD_DTTM","FILENAME")
 
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


CREATE OR REPLACE PROCEDURE NTASDL_RAW.KR_POS_GS_SUPER_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them.

import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col,lit,trim,when,substring,to_varchar, to_date,sql_expr,regexp_replace
from snowflake.snowpark.types import  StringType, StructType, StructField,IntegerType
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz



def main(session: snowpark.Session,Param): 
       
    try :
            
        #Param=[''KR_POS_GS_Super_20201228_20201229093149.MKJNJ02'',''NTASDL_RAW.DEV_LOAD_STAGE_ADLS_KOR'',''dev/pos/gs-super/'',''sdl_kr_pos_gs_super'']

        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        # Define the schema for the DataFrame
        
        df_schema=StructType([
                        StructField("all_col", StringType())
                        
                        ])
                
        dataframe = session.read.schema(df_schema) \\
        .option("skip_header",1) \\
        .option("field_delimiter", "|") \\
        .option("encoding","UTF-8")\\
        .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
        

        
        dataframe=dataframe.na.drop("all")
        if dataframe.count()==0:
            return "No Data in file"
        dataframe=dataframe.with_column("pos_date", substring(col("all_col"), 1, 8))
        dataframe=dataframe.with_column("pos_date",to_varchar(to_date(col("pos_date"), ''YYYYMMDD''), ''YYYY-MM-DD''))
        dataframe=dataframe.with_column("store_code", substring(col("all_col"), 9, 6))
        dataframe=dataframe.with_column("product_code", substring(col("all_col"), 15, 6))
        dataframe=dataframe.with_column("bar_code", substring(col("all_col"), 21, 22))
        dataframe=dataframe.with_column("unit_price", substring(col("all_col"), 43, 9))
        dataframe=dataframe.with_column("unit_price", regexp_replace(col("unit_price"), r''^0+'', ''''))
        dataframe=dataframe.with_column("number_of_sales", substring(col("all_col"), 52, 7))
        dataframe=dataframe.with_column("number_of_sales", regexp_replace(col("number_of_sales"), r''^0+'', ''''))
        dataframe=dataframe.with_column("sales_revenue", substring(col("all_col"), 59, 11))
        dataframe=dataframe.with_column("sales_revenue", regexp_replace(col("sales_revenue"), r''^0+'', ''''))
        dataframe=dataframe.with_column("date_of_preparation", substring(col("all_col"), 70, 8))
        dataframe=dataframe.with_column("date_of_preparation", sql_expr("TRY_TO_DATE(date_of_preparation, ''YYYYMMDD'')"))
        dataframe=dataframe.with_column("serial_num", substring(col("all_col"), 78, 7))
        dataframe=dataframe.with_column("distribution_code", substring(col("all_col"), 85, 3))
        dataframe=dataframe.with_column("customer_code", substring(col("all_col"), 88, 6))
        
        
        file_name = file_name.split(".")[0] + ".csv"
        dataframe = dataframe.with_column("CRTD_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        dataframe = dataframe.withColumn("upd_dttm", lit(None))
        dataframe = dataframe.with_column("FILE_NAME",lit(file_name))
        dataframe = dataframe.with_column("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        dataframe = dataframe.drop(col("all_col"))
        
       
        

        
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

CREATE OR REPLACE PROCEDURE NTASDL_RAW.KR_POS_HOME_PLUS_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS 'import snowflake.snowpark as snowpark
from snowflake import snowpark
from snowflake.snowpark.window import Window
from snowflake.snowpark.functions import col,lit,concat,regexp_replace,trim,split,rtrim,upper,coalesce,row_number,when,to_date,is_null,substring
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField,DecimalType,DateType
from snowflake.snowpark.files import SnowflakeFile
import pandas as pd
import numpy as np

from datetime import datetime
import pytz

from re import sub

def main(session: snowpark.Session,Param):
    

    try:

        #Param=[''KR_POS_Home_Plus_20240515.txt'',''NTASDL_RAW.DEV_LOAD_STAGE_ADLS_KOR'',''dev/pos/transaction/Home_Plus/'',''sdl_kr_pos_homeplus'']

        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        file_header_loc = 10

        #Reading data from adls
        full_path = "@"+stage_name+"/"+temp_stage_path+"/"+file_name
        
        with SnowflakeFile.open(full_path,''rb'',require_scoped_url= False) as f:
            df = pd.read_csv(f,delimiter="\\t",names=[''a''])
            df[''pos_Date''] = df[''a''].apply(lambda x:x[0:8])
            df[''pos_Date''] = df[''pos_Date''].apply(lambda x:datetime.strptime(x,''%Y%m%d''))
            df[''store_code''] = df[''a''].apply(lambda x:x[8:14])
            df[''product_code''] = df[''a''].apply(lambda x:x[14:20])
            df[''bar_code''] = df[''a''].apply(lambda x:x[20:42])
            df[''unit_price''] = df[''a''].apply(lambda x:x[42:51])
            df[''unit_price''] = df[''unit_price''].apply(lambda x: x.lstrip(''0'') if x!=''000000000'' else str(int(x)))
            df[''unit_price''] = df[''unit_price''].apply(lambda x: x+''.0'')
            df[''number_of_sales''] = df[''a''].apply(lambda x:x[51:58])
            #df[''number_of_sales''] = df[''number_of_sales''].fillna(''0'')
            df[''number_of_sales''] = df[''number_of_sales''].apply(lambda x: x.lstrip(''0'') if x!=''0000000'' else str(int(x)))
            
            df[''Sales_revenue''] = df[''a''].apply(lambda x:x[58:69])
            df[''Sales_revenue''] =  df[''Sales_revenue''].apply(lambda x:x.lstrip(''0'') if x!=''00000000000'' else str(int(x)))
            df[''Sales_revenue''] =  df[''Sales_revenue''].apply(lambda x:x+''.0'')
            df[''date_of_preperation''] = df[''a''].apply(lambda x:x[69:77])
            df[''date_of_preperation''] = df[''date_of_preperation''].apply(lambda x: datetime.strptime(x,''%Y%m%d''))
            df[''serial_number''] = df[''a''].apply(lambda x:x[77:84])
            df[''distribution_code''] = df[''a''].apply(lambda x:x[84:87])
            df[''customer_code''] = df[''a''].apply(lambda x:x[87:93])
            
            snowparkdf = session.create_dataframe(df)

            print(snowparkdf.columns)

            #[''"a"'', ''"pos_Date"'', ''"store_code"'', ''"product_code"'', ''"bar_code"'', ''"unit_price"'', ''"number_of_sales"'', ''"Sales_revenue"'', ''"date_of_preperation"'', ''"serial_number"'', ''"distribution_code"'', ''"customer_code"'']

            snowparkdf = snowparkdf.rename({col(''"pos_Date"''):"pos_date",col(''"store_code"''):''store_code'',col(''"product_code"''):''product_code'',col(''"bar_code"''):''bar_code'',
                                            col(''"unit_price"''):''unit_price'',col(''"number_of_sales"''):''number_of_sales'',col(''"Sales_revenue"''):''Sales_revenue'',
                                            col(''"date_of_preperation"''):''date_of_preperation'',col(''"serial_number"''):''serial_number'',col(''"distribution_code"''):''distribution_code'',col(''"customer_code"''):''customer_code''})

            def filenamechange(file_name):
                new_file_name = file_name.split(''.'')[0]
                changed_name = new_file_name +''.csv''
                return changed_name
                
            snowparkdf = snowparkdf.withColumn(''file_name'', lit(filenamechange(file_name)))
            snowparkdf = snowparkdf.with_column("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
            snowparkdf = snowparkdf.withColumn("crtd_dttm",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
            snowparkdf = snowparkdf.withColumn(''upd_dttm'',lit(None))


            snowdf = snowparkdf.select(''pos_date'',
                                       ''store_code'',
                                      ''product_code'',
                                       ''bar_code'',
                                      ''unit_price'',
                                       ''NUMBER_OF_SALES'',
                                       ''SALES_REVENUE'',
                                        ''DATE_OF_PREPERATION'',
                                        ''SERIAL_NUMBER'',
                                         ''DISTRIBUTION_CODE'',
                                          ''CUSTOMER_CODE'',
                                          ''FILE_NAME'',
                                          ''RUN_ID'',
                                           ''CRTD_DTTM'',
                                           ''UPD_DTTM'')

            # Load Data to the target table
            snowdf.write.mode("append").saveAsTable(target_table)

            current_date = datetime.now()
            formatted_year = current_date.strftime("%Y")
            formatted_month = current_date.strftime("%m")

             # write to success folder
            file_name = file_name.split(''.'')[0]+"_"+datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")

            snowdf.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True,file_format_name=''csv_FORMAT'')
            
            
            return ''Success''

            

    except KeyError as key_error:
        # Handle KeyError (missing columns) here
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message
        
    except Exception as e:
        # Handle exceptions here
        error_message = f"Error: {str(e)}"
        return error_message;';

CREATE OR REPLACE PROCEDURE NTASDL_RAW.KR_POS_LOHBS_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS 'from snowflake.snowpark.functions import col, lit, date_format, current_timestamp, to_date, year, month, concat, format_number, regexp_replace,to_timestamp,when,trim,upper,cast
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DecimalType
import snowflake.snowpark
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz
import pandas as pd

def main(session: snowpark.Session,Param):
    #Param=["KR_POS_LOHBS_20221218.csv","NTASDL_RAW.DEV_LOAD_STAGE_ADLS_KOR","dev/pos/transaction/lohbs/","sdl_kr_pos_lohbs"]
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        
        df_schema = StructType([
                StructField("store_name", StringType(), True),
                StructField("store_code", StringType(), True),
                StructField("customer_code", StringType(), True),
                StructField("product_code", StringType(), True),
                StructField("barcode", StringType(), True),
                StructField("product_name", StringType(), True),
                StructField("product_volume", StringType(), True),
                StructField("number_of_sales", StringType(), True),
                StructField("sales_revenue", StringType(), True)
                
            ])
        
        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",5)\\
            .option("field_delimiter", "")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .option("encoding",''UTF-8'')\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)\\
            

       
        
        df=df.na.drop("all")
        if df.count()==0 :
            return "No Data in file"

        
        
        df = df.na.drop(subset=[''CUSTOMER_CODE''])

        df = df.with_column(''SALES_REVENUE'', regexp_replace(col("SALES_REVENUE"),lit('',''),lit('''')))
        df = df.with_column(''SALES_REVENUE'', cast(cast(col(''SALES_REVENUE''),DecimalType(12,2)),StringType()))

        def return_pos_date(file_name):
            file_date = file_name.split(''.'')[0].split(''_'')[3]
            date_new = datetime.strptime(file_date,''%Y%m%d'')
            return date_new

        df = df.with_column("pos_date",lit(return_pos_date(file_name)))
            
        df = df.with_column("run_id", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        # #file_name1=file_name.split(".")[0]+"_"+ datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")+".csv"
        df = df.with_column("file_name", lit(file_name))
        df = df.with_column("crtd_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        df = df.with_column("upd_dttm", lit(None))
        
        snowdf= df.select(
                    "store_name",
                    "store_code",
                    "customer_code",
                    "product_code",
                    "barcode",
                    "product_name",
                    "product_volume",
                    "number_of_sales",
                    "sales_revenue",
                    "pos_date",
                    "file_name",
                    "run_id",
                    "crtd_dttm",
                    "upd_dttm"
                    )
        file_name=file_name.split(".")[0]+"_"+datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")

        snowdf.write.mode("append").saveAsTable(stage_name.split(".")[0] + "." + target_table)
        
        #move to success
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

CREATE OR REPLACE PROCEDURE NTASDL_RAW.KR_POS_LOTTE_MART_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS 'import snowflake.snowpark as snowpark
from snowflake import snowpark
from snowflake.snowpark.window import Window
from snowflake.snowpark.functions import col,lit,concat,regexp_replace,trim,split,rtrim,upper,coalesce,row_number,when,to_date,is_null,substring
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField,DecimalType,DateType
from snowflake.snowpark.files import SnowflakeFile
import pandas as pd
import numpy as np

from datetime import datetime
import pytz

from re import sub

def main(session: snowpark.Session,Param):
    

    try:

        #Param=[''KR_POS_Lotte_Mart_20240514.txt'',''NTASDL_RAW.DEV_LOAD_STAGE_ADLS_KOR'',''dev/pos/transaction/lotte_mart/'',''sdl_kr_pos_lotte_mart'']

        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        file_header_loc = 10

        #Reading data from adls
        full_path = "@"+stage_name+"/"+temp_stage_path+"/"+file_name
        
        with SnowflakeFile.open(full_path,''rb'',require_scoped_url= False) as f:
            df = pd.read_csv(f,delimiter="\\t",names=[''a''],skiprows=5)
            df[''a''] = df[''a''].apply(lambda x:x.split('';'')) 

            def value_return(data:list):
                k = ''''
                for i in data:
                    k = i
                return k
                
                

            df[''store_name''] = df[''a''].apply(lambda x:x[0])
            df[''store_code''] = df[''a''].apply(lambda x:value_return(x[1:2]))
            df[''product_code''] = df[''a''].apply(lambda x:value_return(x[2:3]))
            df[''product_name''] = df[''a''].apply(lambda x:value_return(x[3:4]))
            df[''barcode''] = df[''a''].apply(lambda x:value_return(x[4:5]))
            df[''num_of_sales''] = df[''a''].apply(lambda x:value_return(x[5:6]))
            df[''sales_revenue''] = df[''a''].apply(lambda x:value_return(x[6:7]))
            
            # for i in df[''store_code'']:
            #     print(i)
            
            df[''store_code''] = df[''store_code''].apply(lambda x:x.strip('' ''))
            df[''store_code''] = df[''store_code''].apply(lambda x:None if len(x)<2 else x)
            df.dropna(subset=[''store_code''],inplace=True)
            snowparkdf = session.create_dataframe(df)
            
            #print(snowparkdf[395:])

            #[''"a"'', ,, ''"product_code"'', ''"product_name"'', ''"barcode"'', ''"num_of_sales"'', ''"sales_revenue"'']
            snowparkdf = snowparkdf.rename({col(''"store_name"''):''store_name'',col(''"store_code"''):''store_code'',col(''"product_code"''):''product_code'',col(''"product_name"''):''product_name'',col(''"barcode"''):''barcode'',col(''"num_of_sales"''):''num_of_sales'',col(''"sales_revenue"''):''sales_revenue''})

            def pos_date_return(file_name):
                date_str = file_name.split(''.'')[0].split(''_'')[4]
                date_date = datetime.strptime(date_str,''%Y%m%d'')
                return date_date
                

            def filename_change(file_name):
                filename = file_name.split(''.'')[0]+''.csv''
                return filename

            
            snowparkdf = snowparkdf.with_column(''pos_date'',lit(pos_date_return(file_name)))
            snowparkdf = snowparkdf.with_column(''filename'',lit(filename_change(file_name)))
            snowparkdf = snowparkdf.with_column("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
            snowparkdf = snowparkdf.withColumn("crtd_dttm",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
            snowparkdf = snowparkdf.withColumn(''upd_dttm'',lit(None))

            snowdf = snowparkdf.select(''store_name'',
                                       ''store_code'',
                                       ''product_code'',
                                       ''product_name'',
                                       ''barcode'',
                                       ''num_of_sales'',
                                       ''sales_revenue'',
                                       ''pos_date'',
                                       ''filename'',
                                       ''run_id'',
                                       ''crtd_dttm'',
                                       ''upd_dttm'')

            # Load Data to the target table
            snowdf.write.mode("append").saveAsTable(target_table)

            current_date = datetime.now()
            formatted_year = current_date.strftime("%Y")
            formatted_month = current_date.strftime("%m")

             # write to success folder
            file_name = file_name.split(''.'')[0]+"_"+datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")

            snowdf.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True,file_format_name=''csv_FORMAT'')
            return ''Success''

            

    except KeyError as key_error:
        # Handle KeyError (missing columns) here
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message
        
    except Exception as e:
        # Handle exceptions here
        error_message = f"Error: {str(e)}"
        return error_message;';

CREATE OR REPLACE PROCEDURE NTASDL_RAW.KR_POS_LOTTE_SUPER_PREPROCESSING("PARAM" ARRAY)
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

def main(session:snowpark.Session,Param):
    #Param=["KR_POS_Lotte_Super_20240518.csv","NTASDL_RAW.DEV_LOAD_STAGE_ADLS_KOR","dev/pos/transaction/lotte_super/","sdl_kr_pos_lotte_super"]
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        
        df_schema = StructType([
                StructField("store_name", StringType(), True),
                StructField("store_code", StringType(), True),
                StructField("customer_code", StringType(), True),
                StructField("product_code", StringType(), True),
                StructField("barcode", StringType(), True),
                StructField("product_name", StringType(), True),
                StructField("product_volume", StringType(), True),
                StructField("number_of_sales", StringType(), True),
                StructField("sales_revenue", StringType(), True)
               
                
            ])
        
        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",5)\\
            .option("field_delimiter", "")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .option("encoding",''UTF-8'')\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

       
        
        df=df.na.drop("all")
        if df.count()==0 :
            return "No Data in file"

        df = df.na.drop(subset=["customer_code"])

        df_pandas = df.to_pandas()
        df_pandas[''SALES_REVENUE''] = df_pandas[''SALES_REVENUE''].apply(lambda x:x.replace('','','''').replace('' '','''')+''.00'')
        df = session.create_dataframe(df_pandas)

        
        
        def pos_date_return(file_name):
                date_str = file_name.split(''.'')[0].split(''_'')[4]
                date_date = datetime.strptime(date_str,''%Y%m%d'')
                return date_date
        

        df = df.with_column(''pos_date'',lit(pos_date_return(file_name)))
        df = df.with_column("run_id", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        #file_name1=file_name.split(".")[0]+"_"+ datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")+".csv"
        df = df.with_column("file_name", lit(file_name))
        df = df.with_column("crtd_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        df = df.with_column("upd_dttm", lit(None))
        
        snowdf= df.select(
                    "store_name",
                    "store_code",
                    "customer_code",
                    "product_code",
                    "barcode",
                    "product_name",
                     "product_volume",
                    "number_of_sales",
                    "sales_revenue",
                    "pos_date",
                    "file_name",
                    "run_id",
                    "crtd_dttm",
                    "upd_dttm"
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

CREATE OR REPLACE PROCEDURE NTASDL_RAW.KR_POS_OLIVE_YOUNG_PREPROCESSING("PARAM" ARRAY)
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

def main(session:snowpark.Session,Param):
    #Param=["KR_POS_Olive_Young_20240517.csv","NTASDL_RAW.DEV_LOAD_STAGE_ADLS_KOR","dev/pos/transaction/olive_young/","sdl_kr_pos_olive_young"]
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        
        df_schema = StructType([
                StructField("pos_date", StringType(), True),
                StructField("barcode", StringType(), True),
                StructField("product_name", StringType(), True),
                StructField("store_code", StringType(), True),
                StructField("store_name", StringType(), True),
                StructField("number_of_sales", StringType(), True),
                StructField("sales_price", StringType(), True),
                StructField("sales_revenue", StringType(), True)
               
                
            ])
        
        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .option("encoding",''UTF-8'')\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

       
        
        df=df.na.drop("all")
        if df.count()==0 :
            return "No Data in file"

        # df = df.na.drop(subset=["customer_code"])

        def pos_date_return(date_str):
                
                date_date = datetime.strptime(date_str,''%Y%m%d'')
                return date_date

        df_pandas = df.to_pandas()
        df_pandas[''SALES_REVENUE''] = df_pandas[''SALES_REVENUE''].apply(lambda x:x.replace('','','''').replace('' '','''')+''.00'')
        df_pandas[''SALES_PRICE''] = df_pandas[''SALES_PRICE''].apply(lambda x:x.replace('','','''').replace('' '','''')+''.00'')
        df_pandas[''POS_DATE''] = df_pandas[''POS_DATE''].apply(lambda x: pos_date_return(x))
        
        df = session.create_dataframe(df_pandas)

        
        
        
        

        
        df = df.with_column("run_id", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        # #file_name1=file_name.split(".")[0]+"_"+ datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")+".csv"
        df = df.with_column("file_name", lit(file_name))
        df = df.with_column("crtd_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        df = df.with_column("upd_dttm", lit(None))
        
        snowdf= df.select(
                    "pos_date",
                    "barcode",
                    "product_name",
                    "store_code",
                    "store_name",
                    "number_of_sales",
                    "sales_price",
                    "sales_revenue",
                    "file_name",
                    "run_id",
                    "crtd_dttm",
                    "upd_dttm"
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

CREATE OR REPLACE PROCEDURE NTASDL_RAW.NA_KR_BO_YOUNG_JONG_HAP_LOGISTICS_GT_SELLOUT_PREPROCESSING("PARAM" ARRAY)
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
        sch_name        = stage_name.split(''.'')[0]
        target_table    = sch_name+"."+Param[3]
        
        df_schema = StructType([
                    StructField("Date", StringType()),
                    StructField("Origin_Code", StringType()),
                    StructField("Customer_Name", StringType()),
                    StructField("EAN", StringType()),
                    StructField("Booklet_Code", StringType()),
                    StructField("Trade_Name", StringType()),
                    StructField("Standard", StringType()),
                    StructField("Unit", StringType()),
                    StructField("QTY", StringType()),
                    StructField("Unit_Price", StringType()),
                    StructField("COL11", StringType()),
                    StructField("COL12", StringType()),
                    StructField("COL13", StringType()),
                    StructField("COL14", StringType()),
                    StructField("COL15", StringType()),
                    StructField("COL16", StringType())
                ])

        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "\\u0001")\\
            .option("field_optionally_enclosed_by", "\\"") \\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        dstr_nm = ('' ''.join(file_name.split(''_'')[:-1]))
        cust_cd = file_name.split(''_'')[-1]
        cust_cd = cust_cd.split(''.'')[0]

        df= df.withColumn("DSTR_NM", lit(dstr_nm).cast("string"))
        df= df.withColumn("CUST_CD", lit(cust_cd).cast("string"))

        final_df = df.select("DSTR_NM", "Date", "Origin_Code", "Customer_Name", "EAN", "Booklet_Code", "Trade_Name", "Standard", "Unit", "QTY", "Unit_Price", "CUST_CD").alias("final_df")

        if final_df.count()==0:
            return "No Data in file"
        
        # Load Data to the target table
        final_df.write.mode("append").saveAsTable(target_table)

        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")

        # write to success folder
    
        file_name=file_name.split(".")[0] + ''_'' + datetime.now().strftime("%Y%m%d%H%M%S")
        final_df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,file_format_type="csv",OVERWRITE=True,header=True)
   
        return "Success"
        
    except KeyError as key_error:
        
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message
        
    except Exception as e:
        
        error_message = f"Error: {str(e)}" +str(df.columns)
        return error_message
';



CREATE OR REPLACE PROCEDURE NTASDL_RAW.NA_KR_DAISO_GT_SELLOUT_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python','pandas')
HANDLER = 'main'
EXECUTE AS OWNER
AS '
from snowflake.snowpark.functions import col, lit, date_format, current_timestamp, to_date, year, month, concat, format_number, regexp_replace,to_timestamp,when,trim,upper
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DecimalType,DateType
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz
import pandas


def main(session: snowpark.Session,Param):
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        sch_name        = stage_name.split(''.'')[0]
        target_table    = sch_name+"."+Param[3]
        
        df_schema = StructType([
                    StructField("COL01", StringType()),
                    StructField("COL02", StringType()),
                    StructField("COL03", StringType()),
                    StructField("EAN", StringType()),
                    StructField("NAME", StringType()),
                    StructField("01", StringType()),
                    StructField("02", StringType()),
                    StructField("03", StringType()),
                    StructField("04", StringType()),
                    StructField("05", StringType()),
                    StructField("06", StringType()),
                    StructField("07", StringType()),
                    StructField("08", StringType()),
                    StructField("09", StringType()),
                    StructField("10", StringType()),
                    StructField("11", StringType()),
                    StructField("12", StringType()),
                    StructField("COL18", StringType()),
                    StructField("COL19", StringType()),
                    StructField("COL20", StringType())
                ])

        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",0)\\
            .option("field_delimiter", "\\u0001")\\
            .option("field_optionally_enclosed_by", "\\"") \\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        pd_df = df.to_pandas()
        
        name = pd_df.iloc[0]["COL02"]
        df = df.withColumn("CUSTOMER_NAME", lit(name).cast("string"))

        filenamepart, extension = file_name.split(''.'')
        dstr_nm , cust_cd, year = filenamepart.split(''_'')

        df = df.filter(col("NAME").is_not_null()) and df.filter((upper(col("01")) != "QTY") & (upper(col("NAME")) != "TOTAL"))
        
        df= df.withColumn("DSTR_NM", upper(lit(dstr_nm).cast("string")))
        df= df.withColumn("Year", lit(year).cast("string"))
        df= df.withColumn("CUST_CD", lit(cust_cd).cast("string"))

        df2 = df.unpivot("QTY","MONTH",["01","02","03","04","05","06","07","08","09","10","11","12"])
        file_df = df2.select("DSTR_NM", "YEAR", "MONTH", "EAN", "NAME", "QTY", "Customer_Name", "CUST_CD").alias("file_df")
        
        df = df.na.fill("", ["01","02","03","04","05","06","07","08","09","10","11","12"])
        
        df = df.unpivot("QTY","MONTH",["01","02","03","04","05","06","07","08","09","10","11","12"])
        
        final_df = df.select("DSTR_NM", "YEAR", "MONTH", "EAN", "NAME", "QTY", "Customer_Name", "CUST_CD").alias("final_df")

        if final_df.count()==0:
            return "No Data in file"
        
        # Load Data to the target table
        final_df.write.mode("append").saveAsTable(target_table)

        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")

        # write to success folder
    
        file_name=file_name.split(".")[0] + ''_'' + datetime.now().strftime("%Y%m%d%H%M%S")
        file_df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,file_format_type="csv",OVERWRITE=True,header=True)   
        
        return "Success"
        
    except KeyError as key_error:
        
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message
        
    except Exception as e:
        
        error_message = f"Error: {str(e)}" +str(df.columns)
        return error_message
';

CREATE OR REPLACE PROCEDURE NTASDL_RAW.NA_KR_DA_IN_SANG_SA_GT_SELLOUT_PREPROCESSING("PARAM" ARRAY)
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
        sch_name        = stage_name.split(''.'')[0]
        target_table    = sch_name+"."+Param[3]

        df_schema = StructType([
                    StructField("DATE", StringType()),
                    StructField("KEJO", StringType()),
                    StructField("GURNAME", StringType()),
                    StructField("ITEM", StringType()),
                    StructField("GYU", StringType()),
                    StructField("BIGO", StringType()),
                    StructField("EA", StringType()),
                    StructField("PRICE", StringType()),
                    StructField("GUM", StringType()),
                    StructField("VAT", StringType()),
                    StructField("GUMVAT", StringType()),
                    StructField("CODE2", StringType())
                ])

        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "\\u0001")\\
            .option("field_optionally_enclosed_by", "\\"") \\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        dstr_nm = ('' ''.join(file_name.split(''_'')[:-1]))
        cust_cd = file_name.split(''_'')[-1]
        cust_cd = cust_cd.split(''.'')[0]

        df= df.withColumn("DSTR_NM", lit(dstr_nm).cast("string"))
        df= df.withColumn("CUST_CD", lit(cust_cd).cast("string"))

        final_df = df.select("DSTR_NM", "DATE", "KEJO", "GURNAME", "ITEM", "GYU", "BIGO", "EA", "PRICE", "GUM", "VAT", "GUMVAT", "CODE2",  "CUST_CD").alias("final_df")

        if final_df.count()==0:
            return "No Data in file"
        
        # Load Data to the target table
        final_df.write.mode("append").saveAsTable(target_table)

        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")

        # write to success folder
    
        file_name=file_name.split(".")[0] + ''_'' + datetime.now().strftime("%Y%m%d%H%M%S")
        final_df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,file_format_type="csv",OVERWRITE=True,header=True)
   
        return "Success"
        
    except KeyError as key_error:
        
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message
        
    except Exception as e:
        
        error_message = f"Error: {str(e)}" +str(df.columns)
        return error_message
';


CREATE OR REPLACE PROCEDURE NTASDL_RAW.NA_KR_DONGBU_LSD_GT_SELLOUT_PREPROCESSING("PARAM" ARRAY)
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
        sch_name        = stage_name.split(''.'')[0]
        target_table    = sch_name+"."+Param[3]
 
        df_schema = StructType([
                    StructField("Date", StringType()),
                    StructField("number", StringType()),
                    StructField("sub_customer_name", StringType()),
                    StructField("total_amount", StringType()),
                    StructField("total_room_amount", StringType()),
                    StructField("EAN", StringType()),
                    StructField("product", StringType()),
                    StructField("unit", StringType()),
                    StructField("Qty", StringType()),
                    StructField("unit_price", StringType())
                ])

        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "\\u0001")\\
            .option("field_optionally_enclosed_by", "\\"") \\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
        
        dstr_nm = ('' ''.join(file_name.split(''_'')[:-1]))
        cust_cd = file_name.split(''_'')[-1]
        cust_cd = cust_cd.split(''.'')[0]

        df= df.withColumn("DSTR_NM", lit(dstr_nm).cast("string"))
        df= df.withColumn("CUST_CD", lit(cust_cd).cast("string"))

        df = df.filter(col("Date").is_not_null()) and df.filter(col("EAN").is_not_null())
        
        final_df = df.select("DSTR_NM", "Date", "number", "sub_customer_name", "total_amount", "total_room_amount", "EAN", "product", "unit", "Qty", "unit_price", "CUST_CD").alias("final_df")

        if final_df.count()==0:
            return "No Data in file"
        
        # Load Data to the target table
        final_df.write.mode("append").saveAsTable(target_table)

        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")

        # write to success folder
    
        file_name=file_name.split(".")[0] + ''_'' + datetime.now().strftime("%Y%m%d%H%M%S")
        final_df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,file_format_type="csv",OVERWRITE=True,header=True)
   

        return "Success"
        
    except KeyError as key_error:
        
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message
        
    except Exception as e:
        
        error_message = f"Error: {str(e)}" +str(df.columns)
        return error_message
';


CREATE OR REPLACE PROCEDURE NTASDL_RAW.NA_KR_DONGBU_LSD_GT_SELLOUT_PREPROCESSING("PARAM" ARRAY)
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
        sch_name        = stage_name.split(''.'')[0]
        target_table    = sch_name+"."+Param[3]
 
        df_schema = StructType([
                    StructField("Date", StringType()),
                    StructField("number", StringType()),
                    StructField("sub_customer_name", StringType()),
                    StructField("total_amount", StringType()),
                    StructField("total_room_amount", StringType()),
                    StructField("EAN", StringType()),
                    StructField("product", StringType()),
                    StructField("unit", StringType()),
                    StructField("Qty", StringType()),
                    StructField("unit_price", StringType())
                ])

        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "\\u0001")\\
            .option("field_optionally_enclosed_by", "\\"") \\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
        
        dstr_nm = ('' ''.join(file_name.split(''_'')[:-1]))
        cust_cd = file_name.split(''_'')[-1]
        cust_cd = cust_cd.split(''.'')[0]

        df= df.withColumn("DSTR_NM", lit(dstr_nm).cast("string"))
        df= df.withColumn("CUST_CD", lit(cust_cd).cast("string"))

        df = df.filter(col("Date").is_not_null()) and df.filter(col("EAN").is_not_null())
        
        final_df = df.select("DSTR_NM", "Date", "number", "sub_customer_name", "total_amount", "total_room_amount", "EAN", "product", "unit", "Qty", "unit_price", "CUST_CD").alias("final_df")

        if final_df.count()==0:
            return "No Data in file"
        
        # Load Data to the target table
        final_df.write.mode("append").saveAsTable(target_table)

        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")

        # write to success folder
    
        file_name=file_name.split(".")[0] + ''_'' + datetime.now().strftime("%Y%m%d%H%M%S")
        final_df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,file_format_type="csv",OVERWRITE=True,header=True)
   

        return "Success"
        
    except KeyError as key_error:
        
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message
        
    except Exception as e:
        
        error_message = f"Error: {str(e)}" +str(df.columns)
        return error_message
';


CREATE OR REPLACE PROCEDURE NTASDL_RAW.NA_KR_DU_BAE_RO_YU_TONG_GT_SELLOUT_PREPROCESSING("PARAM" ARRAY)
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
        sch_name        = stage_name.split(''.'')[0]
        target_table    = sch_name+"."+Param[3]
        
        df_schema = StructType([
                    StructField("S_NO", StringType()),
                    StructField("Customer_Name", StringType()),
                    StructField("Date", StringType()),
                    StructField("EAN", StringType()),
                    StructField("Neck_Name", StringType()),
                    StructField("Standard", StringType()),
                    StructField("Unit", StringType()),
                    StructField("Qty", StringType()),
                    StructField("Unit_Price", StringType()),
                    StructField("Total", StringType())
                ])

        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",3)\\
            .option("field_delimiter", "\\u0001")\\
            .option("field_optionally_enclosed_by", "\\"") \\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
 
        valid_df=df.filter(col("Date").is_not_null()) and df.filter(col("EAN").is_not_null())

        dstr_nm = ('' ''.join(file_name.split(''_'')[:-1]))
        cust_cd = file_name.split(''_'')[-1:][0]
        cust_cd = cust_cd.split(''.'')[0]

        valid_df= valid_df.withColumn("DSTR_NM", lit(dstr_nm).cast("string"))
        valid_df= valid_df.withColumn("CUST_CD", lit(cust_cd).cast("string"))

        final_df = valid_df.select("DSTR_NM", "S_NO", "Customer_Name", "Date", "EAN", "Neck_Name", "Standard", "Unit", "Qty", "Unit_Price", "Total", "CUST_CD").alias("final_df")

        if final_df.count()==0:
            return "No Data in file"
        
        # Load Data to the target table
        final_df.write.mode("append").saveAsTable(target_table)

        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")

        # write to success folder
    
        file_name=file_name.split(".")[0] + ''_'' + datetime.now().strftime("%Y%m%d%H%M%S")
        final_df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,file_format_type="csv",OVERWRITE=True,header=True)
   
        return "Success"
        
    except KeyError as key_error:
        
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message
        
    except Exception as e:
        
        error_message = f"Error: {str(e)}" +str(df.columns)
        return error_message
';

CREATE OR REPLACE PROCEDURE NTASDL_RAW.NA_KR_GT_DAISO_PRICE_PREPROCESSING("PARAM" ARRAY)
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
        sch_name        = stage_name.split(''.'')[0]
        target_table    = sch_name+"."+Param[3]


        df_schema = StructType([
                    StructField("EAN", StringType()),
                    StructField("Unit_Price", StringType())
                ])

        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "\\u0001")\\
            .option("field_optionally_enclosed_by", "\\"") \\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        dstr_nm = file_name.split(''_'')[0]
        df= df.withColumn("DSTR_NM", upper(lit(dstr_nm).cast("string")))
        
        final_df = df.select("DSTR_NM", "EAN", "Unit_Price").alias("final_df")

        if final_df.count()==0:
            return "No Data in file"
        
        # Load Data to the target table
        final_df.write.mode("append").saveAsTable(target_table)

        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")

        # write to success folder
    
        file_name=file_name.split(".")[0] + ''_'' + datetime.now().strftime("%Y%m%d%H%M%S")
        final_df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,file_format_type="csv",OVERWRITE=True,header=True)

        return "Success"
        
    except KeyError as key_error:
        
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message
        
    except Exception as e:
        
        error_message = f"Error: {str(e)}" +str(df.columns)
        return error_message
';

CREATE OR REPLACE PROCEDURE NTASDL_RAW.NA_KR_GT_DPT_DAISO_PREPROCESSING("PARAM" ARRAY)
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
        sch_name        = stage_name.split(''.'')[0]
        target_table    = sch_name+"."+Param[3]
        
        df_schema = StructType([
                    StructField("Sub_Customer_Name", StringType()),
                    StructField("Sub_Customer_Code", StringType()),
                    StructField("Customer_Code", StringType())
                ])

        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "\\u0001")\\
            .option("field_optionally_enclosed_by", "\\"") \\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        final_df = df.select("Sub_Customer_Name", "Sub_Customer_Code", "Customer_Code").alias("final_df")

        if final_df.count()==0:
            return "No Data in file"
        
        # Load Data to the target table
        final_df.write.mode("append").saveAsTable(target_table)

        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")

        # write to success folder
    
        file_name=file_name.split(".")[0] + ''_'' + datetime.now().strftime("%Y%m%d%H%M%S")
        final_df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,file_format_type="csv",OVERWRITE=True,header=True)
   
        return "Success"
        
    except KeyError as key_error:
        
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message
        
    except Exception as e:
        
        error_message = f"Error: {str(e)}" +str(df.columns)
        return error_message
';
CREATE OR REPLACE PROCEDURE NTASDL_RAW.NA_KR_POS_COSTCO_INVRPT_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '
# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them.

import snowflake.snowpark as snowpark
from snowflake.snowpark.window import Window
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DateType
from snowflake.snowpark.functions import col,lit,date_format,current_timestamp,to_timestamp, md5, coalesce, concat, upper, trim, to_date, when, lead,lpad, replace, substring, ltrim, right, to_double
import pandas as pd
from datetime import datetime
import pytz

def main(session: snowpark.Session): 
    # Your code goes here, inside the "main" handler.
    # tableName = ''information_schema.packages''

    # Param = [
    #     # ''TH_Action_Open_20230422_20230422170714.csv'',
    #     ''UPLUS_UCOST01_INVRPT_20210328_20210329070517.dat'',
    #     ''NTASDL_RAW.DEV_LOAD_STAGE_ADLS_KOR'',
    #     ''dev/pos/costco/invrpt'',
    #     ''SDL_KR_POS_COSTCO_INVRPT''
    # ]
    file_name       = Param[0]
    stage_name      = Param[1]
    temp_stage_path = Param[2]
    sch_name        = stage_name.split(''.'')[0]
    target_table    = sch_name+"."+Param[3]
    header_table    = target_table +"_HEADER"
    line_table      = target_table +"_LINE"
    
    df_schema = StructType([
                StructField("DistCode", StringType())
            ])

    df = session.read\\
        .schema(df_schema)\\
        .option("skip_header",0)\\
        .option("field_delimiter", "\\t")\\
        .option("skip_blank_lines", True) \\
        .with_metadata("METADATA$FILE_ROW_NUMBER") \\
        .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

    df = df.with_column_renamed("METADATA$FILE_ROW_NUMBER", "Seq_No")
    # df = df.with_column_renamed("METADATA$FILENAME", "FILE_NM")
    df_header = (df.filter(df.col("DISTCODE").startswith("H")))

    df_line = (df.filter(df.col("DISTCODE").startswith("L")))
    df_header = df_header.with_column("Dummy", lit(1))
    
    df_header = df_header.select(lpad(col("Seq_No"),6,lit("0")).alias("SEQ"),col("Seq_No"),col("DistCode"), lead("Seq_No").over(Window.partition_by(col("Dummy")).order_by(col("Seq_No"))).alias("Next_Seq_No"))
    df_header = df_header.na.fill(0, "Next_Seq_No")
    
    current_date = datetime.now()
    formatted_year = current_date.strftime("%Y")
    formatted_month = current_date.strftime("%m")
    header_file_name = file_name.replace(".dat","_Header.csv")
    # print(new_file_name)
    print("@"+stage_name+"/"+temp_stage_path+"/processed/success/"+formatted_year+"/"+formatted_month+"/"+header_file_name)

    df_header.select(concat(col("SEQ"),col("DISTCODE"))).write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/processed/success/"+formatted_year+"/"+formatted_month+"/"+header_file_name,file_format_type="csv",OVERWRITE=True,header=False)

    #df_header.show()
    df_line = df_line.join(df_header,((df_line.seq_no >df_header.seq_no) & ((df_line.seq_no <=df_header.next_seq_no) | (df_header.next_seq_no == 0)))).select(df_header.seq_no.alias("seq_no"),df_line.distcode.alias("distcode"))

    line_file_name = file_name.replace(".dat","_Line.csv")
    print("@"+stage_name+"/"+temp_stage_path+"/processed/success/"+formatted_year+"/"+formatted_month+"/"+line_file_name)
    #df_line.select(concat(lpad(col("Seq_No"),6,lit("0")), col("DISTCODE"))).write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/processed/success/"+formatted_year+"/"+formatted_month+"/"+line_file_name, file_format_type="csv", OVERWRITE=True, header=False)

    df_header_split = df_header.select(  \\
        col("SEQ"), col("Seq_No"), \\
        substring(col("DISTCODE"),1,1).alias("FIELD_TYPE"), \\
        substring(col("DISTCODE"),2,3).alias("DOC_FUN"), \\
        substring(col("DISTCODE"),5,35).alias("DOC_NO"), \\
        substring(col("DISTCODE"),40,3).alias("DOC_FUN_CD"), \\
        substring(col("DISTCODE"),43,35).alias("DOC_SEND_DATE"), \\
        substring(col("DISTCODE"),78,35).alias("INV_RPT_DATE"), \\
        substring(col("DISTCODE"),113,455).alias("NOT_IN_USE1"), \\
        substring(col("DISTCODE"),568,35).alias("BUYE_LOC_CD"), \\
        substring(col("DISTCODE"),603,330).alias("NOT_IN_USE2"), \\
        substring(col("DISTCODE"),933,35).alias("VEND_LOC_CD"), \\
        substring(col("DISTCODE"),968,330).alias("NOT_IN_USE3"), \\
        substring(col("DISTCODE"),1298,35).alias("PROVIDER_LOC_CD"), \\
        substring(col("DISTCODE"),1333,330).alias("NOT_IN_USE4") \\
    )

    df_header_split.select(concat(col("SEQ"), col("FIELD_TYPE")).alias("SEQ_NO"), \\
                           "DOC_FUN","DOC_NO", "DOC_FUN_CD", "DOC_SEND_DATE", "INV_RPT_DATE", "NOT_IN_USE1", "BUYE_LOC_CD", \\
                           "NOT_IN_USE2", "VEND_LOC_CD", "NOT_IN_USE3", "PROVIDER_LOC_CD", "NOT_IN_USE4", \\
                           lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")).alias("CRT_DTTM"), \\
                           lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")).alias("UPDT_DTTM") \\
                          ).write.mode("overwrite").saveAsTable(header_table)

    df_line_split = df_line.select( \\
        lpad(col("Seq_No"),6,lit("0")).alias("SEQ"), \\
        col("Seq_No"), \\
        substring(col("DISTCODE"),1,1).alias("FIELD_TYPE"), \\
        substring(col("DISTCODE"),2,6).alias("LINE_NO"), \\
        substring(col("DISTCODE"),8,35).alias("EAN_CD"), \\
        substring(col("DISTCODE"),43,53).alias("NOT_IN_USE1"), \\
        substring(col("DISTCODE"),96,15).alias("COMP_QTY"), \\
        substring(col("DISTCODE"),111,3).alias("UNIT_OF_PKG_COMP"), \\
        substring(col("DISTCODE"),114,18).alias("NOT_IN_USE2"), \\
        substring(col("DISTCODE"),132,15).alias("INVT_QTY"), \\
        substring(col("DISTCODE"),147,3).alias("UNIT_OF_PKG_INVT"), \\
        substring(col("DISTCODE"),150,15).alias("SALES_QTY"), \\
        substring(col("DISTCODE"),165,3).alias("UNIT_OF_PKG_SALES"), \\
        substring(col("DISTCODE"),168,36).alias("NOT_IN_USE3"), \\
        substring(col("DISTCODE"),204,15).alias("ORDER_QTY"), \\
        substring(col("DISTCODE"),219,3).alias("UNIT_OF_PKG_ORDER"), \\
        substring(col("DISTCODE"),222,556).alias("NOT_IN_USE4") \\
    )

    df_line_split.select(concat(col("SEQ"), col("FIELD_TYPE")).alias("SEQ_NO"), "LINE_NO", "EAN_CD", "NOT_IN_USE1", "COMP_QTY", "UNIT_OF_PKG_COMP", \\
                         "NOT_IN_USE2", "INVT_QTY", "UNIT_OF_PKG_INVT", "SALES_QTY", "UNIT_OF_PKG_SALES", "NOT_IN_USE3", "ORDER_QTY", "UNIT_OF_PKG_ORDER", "NOT_IN_USE4", \\
                           lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")).alias("CRT_DTTM"), \\
                           lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")).alias("UPDT_DTTM") \\
                        ).write.mode("overwrite").saveAsTable(line_table)

   #     LTRIM(RIGHT(TRIM(DOC_NO),3),0) STORE_CODE, 
   # LTRIM(TRIM(EAN_CD),0) AS EAN_CD, 
   # TO_NUMBER(TRIM(SALES_QTY),''999999999999999'') AS SALES_QTY, 
   # TRIM(UNIT_OF_PKG_SALES) AS UNIT_OF_PKG_SALES,
   # TO_DATE(TRIM(DOC_SEND_DATE),''YYYYMMDD'') AS DOC_SEND_DATE,
   # TO_DATE(TRIM(INV_RPT_DATE),''YYYYMMDD'') AS POS_DT,
   # TO_NUMBER(TRIM(INVT_QTY),''999999999999999'') AS INVT_QTY,
   # TRIM(UNIT_OF_PKG_INVT) AS UNIT_OF_PKG_INVT,
   # TRIM(DOC_FUN) AS DOC_FUN,
   # TRIM(DOC_NO) AS DOC_NO,
   # TRIM(DOC_FUN_CD) AS DOC_FUN_CD,
   # TRIM(BUYE_LOC_CD) AS BUYE_LOC_CD,
   # TRIM(VEND_LOC_CD) AS VEND_LOC_CD,
   # TRIM(PROVIDER_LOC_CD) AS PROVIDER_LOC_CD,
   # TRIM(LINE_NO),
   # TO_NUMBER(TRIM(COMP_QTY),''999999999999999'') AS COMP_QTY,
   # TRIM(UNIT_OF_PKG_COMP) AS UNIT_OF_PKG_COMP,
   # TO_NUMBER(TRIM(ORDER_QTY),''999999999999999'') AS ORDER_QTY,
   # TRIM(UNIT_OF_PKG_ORDER) AS UNIT_OF_PKG_ORDER,
   #  ''"+(String)globalMap.get("File_Name")+"'',
   # ''"+context.run_id+"'' ,
   #  SYSDATE AS CRT_DTTM, 
   #  SYSDATE AS UPDT_DTTM

    final_df = df_line_split.join(df_header_split,df_line_split.seq_no == df_header_split.seq_no).select( \\
        ltrim(right(trim(df_header_split.doc_no),3), lit("0")).alias("STORE_CODE"), \\
        ltrim(df_line_split.ean_cd, lit("0")).alias("EAN_CD"), \\
        to_double(trim(df_line_split.sales_qty),"999999999999999").alias("SALES_QTY"), \\
        trim(df_line_split.unit_of_pkg_sales).alias("UNIT_OF_PKG_SALES"), \\
        to_date(df_header_split.doc_send_date, lit("YYYYMMDD")).alias("DOC_SEND_DATE"), \\
        to_date(df_header_split.inv_rpt_date, lit("YYYYMMDD")).alias("POS_DT"), \\
        to_double(trim(df_line_split.invt_qty),"999999999999999").alias("INVT_QTY"), \\
        trim(df_line_split.unit_of_pkg_invt).alias("UNIT_OF_PKG_INVT"), \\
        trim(df_header_split.doc_fun).alias("DOC_FUN"), \\
        trim(df_header_split.doc_no).alias("DOC_NO"), \\
        trim(df_header_split.doc_fun_cd).alias("DOC_FUN_CD"), \\
        trim(df_header_split.buye_loc_cd).alias("BUYE_LOC_CD"), \\
        trim(df_header_split.vend_loc_cd).alias("VEND_LOC_CD"), \\
        trim(df_header_split.provider_loc_cd).alias("PROVIDER_LOC_CD"), \\
        trim(df_line_split.line_no).alias("LINE_NO"), \\
        to_double(trim(df_line_split.invt_qty),"999999999999999").alias("COMP_QTY"), \\
        trim(df_line_split.unit_of_pkg_comp).alias("UNIT_OF_PKG_COMP"), \\
        to_double(trim(df_line_split.invt_qty),"999999999999999").alias("ORDER_QTY"), \\
        trim(df_line_split.unit_of_pkg_order).alias("UNIT_OF_PKG_ORDER"), \\
        lit(file_name).cast("string").alias("FILENAME"), \\
        lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")).alias("RUN_ID"), \\
        lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")).alias("CRT_DTTM"), \\
        lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")).alias("UPDT_DTTM") \\
    )

    final_df.write.mode("append").saveAsTable(target_table)
    target_file_name = file_name.replace(".dat", ".csv")
    final_df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/processed/success/"+formatted_year+"/"+formatted_month+"/"+target_file_name,file_format_type="csv",OVERWRITE=True,header=True)
    
    return "Success"
';

CREATE OR REPLACE PROCEDURE NTASDL_RAW.NA_KR_POS_COSTCO_VMIMST_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '

import snowflake.snowpark as snowpark
from snowflake.snowpark.window import Window
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DateType
from snowflake.snowpark.functions import col,lit,date_format,current_timestamp,to_timestamp, md5, coalesce, concat, upper, trim, to_date, when, lead,lpad, replace, substring, length, right, ltrim, try_cast, to_double
import pandas as pd
from datetime import datetime
import pytz

def main(session: snowpark.Session): 
    # Your code goes here, inside the "main" handler.
    # tableName = ''information_schema.packages''

    # Param = [
    #     # ''TH_Action_Open_20230422_20230422170714.csv'',
    #     ''UPLUS_UCOST01_VMIMST_20210328_20210329070408.dat'',
    #     ''NTASDL_RAW.DEV_LOAD_STAGE_ADLS_KOR'',
    #     ''dev/pos/costco/vmimst'',
    #     ''SDL_KR_POS_COSTCO_VMIMST''
    # ]
    file_name       = Param[0]
    stage_name      = Param[1]
    temp_stage_path = Param[2]
    sch_name        = stage_name.split(''.'')[0]
    target_table    = sch_name+"."+Param[3]
    raw_header_table = sch_name+".SDL_VMIMST_HEADER"
    raw_line_table  = sch_name+".SDL_VMIMST_LINE"
    header_table    = target_table +"_HEADER"
    line_table      = target_table +"_LINE"
    
    df_schema = StructType([
                StructField("DistCode", StringType())
            ])

    df = session.read\\
        .schema(df_schema)\\
        .option("skip_header",0)\\
        .option("field_delimiter", "|")\\
        .option("skip_blank_lines", True) \\
        .option("encoding","EUC-KR") \\
        .with_metadata("METADATA$FILE_ROW_NUMBER") \\
        .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

    df = df.with_column_renamed("METADATA$FILE_ROW_NUMBER", "Seq_No")
    df = df.with_column("DATA_LEN", length(col("DISTCODE")))
    df_header = (df.filter(df.col("DISTCODE").startswith("H")))

    df_line = (df.filter(df.col("DISTCODE").startswith("L")))
    df_header = df_header.with_column("Dummy", lit(1))
    
    df_header = df_header.select(lpad(col("Seq_No"),6,lit("0")).alias("SEQ"),col("Seq_No"),col("DistCode"), lead("Seq_No").over(Window.partition_by(col("Dummy")).order_by(col("Seq_No"))).alias("Next_Seq_No"))
    df_header = df_header.na.fill(0, "Next_Seq_No")
    
    current_date = datetime.now()
    formatted_year = current_date.strftime("%Y")
    formatted_month = current_date.strftime("%m")
    header_file_name = file_name.replace(".dat","_Header.csv")
    # print(new_file_name)
    print("@"+stage_name+"/"+temp_stage_path+"/processed/success/"+formatted_year+"/"+formatted_month+"/"+header_file_name)

    #df_header.select(concat(col("SEQ"),col("DISTCODE"))).write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/processed/success/"+formatted_year+"/"+formatted_month+"/"+header_file_name,file_format_type="csv",OVERWRITE=True,header=False)
    df_header.select(concat(col("SEQ"),col("DISTCODE"))).write.mode("overwrite").saveAsTable(raw_header_table)
    #df_header.show()
    df_line = df_line.join(df_header,((df_line.seq_no >df_header.seq_no) & ((df_line.seq_no <=df_header.next_seq_no) | (df_header.next_seq_no == 0)))).select(df_header.seq_no.alias("seq_no"),df_line.distcode.alias("distcode"))

    line_file_name = file_name.replace(".dat","_Line.csv")
    print("@"+stage_name+"/"+temp_stage_path+"/processed/success/"+formatted_year+"/"+formatted_month+"/"+line_file_name)
    #df_line.select(concat(lpad(col("Seq_No"),6,lit("0")), col("DISTCODE"))).write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/processed/success/"+formatted_year+"/"+formatted_month+"/"+line_file_name, file_format_type="csv", OVERWRITE=True, header=False)
    df_line.select(concat(lpad(col("Seq_No"),6,lit("0")),col("DISTCODE")).alias("LINE_DATA")).write.mode("overwrite").saveAsTable(raw_line_table)

    df_header_split = df_header.select(  \\
        col("SEQ"), col("Seq_No"), \\
        substring(col("DISTCODE"),1,1).alias("FIELD_TYPE"), \\
        substring(col("DISTCODE"),2,9).alias("STYLE"), \\
        substring(col("DISTCODE"),11,length(col("DISTCODE"))-56).alias("PRODUCT_NM"), \\
        right(col("DISTCODE"),46).alias("DISTCODE2"), \\
        substring(col("DISTCODE2"),1,14).alias("PRODUCT_CD"), \\
        substring(col("DISTCODE2"),15,15).alias("OCC_NO"), \\
        substring(col("DISTCODE2"),30,1).alias("ITEM_TYPE"), \\
        substring(col("DISTCODE2"),31,10).alias("UNIT_OF_PKG_ITEM"), \\
        substring(col("DISTCODE2"),41,5).alias("PACK_SIZE"), \\
        substring(col("DISTCODE2"),46,1).alias("DELIVERY_METHOD") \\
    )

    df_header_split.select(concat(col("SEQ"), col("FIELD_TYPE")).alias("SEQ_NO"), \\
                           "STYLE","PRODUCT_NM", "PRODUCT_CD", "OCC_NO", "ITEM_TYPE", "UNIT_OF_PKG_ITEM", "PACK_SIZE", "DELIVERY_METHOD", \\
                           lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")).alias("CRT_DTTM"), \\
                           lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")).alias("UPDT_DTTM") \\
                          ).write.mode("overwrite").saveAsTable(header_table)


    df_line_split = df_line.select( \\
        lpad(col("Seq_No"),6,lit("0")).alias("SEQ"), \\
        col("Seq_No"), \\
        substring(col("DISTCODE"),1,1).alias("FIELD_TYPE"), \\
        substring(col("DISTCODE"),2,2).alias("LINE_NO"), \\
        substring(col("DISTCODE"),4,4).alias("STORE_CD"), \\
        substring(col("DISTCODE"),8,length(col("DISTCODE"))-56).alias("STORE_NM"), \\
        right(col("DISTCODE"),49).alias("DISTCODE2"), \\
        substring(col("DISTCODE2"),1,6).alias("VENDOR"), \\
        substring(col("DISTCODE2"),7,2).alias("LEAD_TIME"), \\
        substring(col("DISTCODE2"),9,1).alias("HANDLE_CLASS"), \\
        substring(col("DISTCODE2"),10,8).alias("PRM_STRT_DT"), \\
        substring(col("DISTCODE2"),18,8).alias("PRM_END_DT"), \\
        substring(col("DISTCODE2"),26,8).alias("SALES_TGT"), \\
        substring(col("DISTCODE2"),34,8).alias("AMT_ORDER"), \\
        substring(col("DISTCODE2"),42,8).alias("WAREHOUSE_DT") \\
    )

    df_line_split.select(concat(col("SEQ"), col("FIELD_TYPE")).alias("SEQ_NO"), "LINE_NO", "STORE_CD", "STORE_NM", "VENDOR", \\
                         "LEAD_TIME", "HANDLE_CLASS", "PRM_STRT_DT", "PRM_END_DT", "SALES_TGT", "AMT_ORDER", "WAREHOUSE_DT", \\
                         lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")).alias("CRT_DTTM"), \\
                         lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")).alias("UPDT_DTTM") \\
                    ).write.mode("overwrite").saveAsTable(line_table)

    final_df = df_line_split.join(df_header_split, df_line_split.seq_no == df_header_split.seq_no).select( \\
        trim(df_line_split.line_no).alias("LINE_NO,"), \\
        ltrim(trim(df_header_split.product_cd), lit("0")).alias("PRODUCT_CD"), \\
        trim(df_header_split.product_nm).alias("PRODUCT_NM"), \\
        ltrim(df_line_split.store_cd, lit("0")).alias("STORE_CD"), \\
        trim(df_line_split.store_nm).alias("STORE_NM"), \\
        ltrim(df_line_split.vendor, lit("0")).alias("VENDOR"), \\
        try_cast(trim(df_line_split.prm_strt_dt, lit("00000000")), DateType()).alias("PRM_STRT_DT"), \\
        try_cast(trim(df_line_split.prm_end_dt, lit("00000000")), DateType()).alias("PRM_END_DT"), \\
        to_double(trim(df_line_split.sales_tgt), lit("99999999")).alias("SALES_TGT"), \\
        to_double(trim(df_line_split.amt_order), lit("99999999")).alias("AMT_ORDER"), \\
        to_double(trim(df_line_split.warehouse_dt), lit("99999999")).alias("WAREHOUSE_DT"), \\
        df_header_split.item_type.alias("ITEM_TYPE"), \\
        df_header_split.unit_of_pkg_item.alias("UNIT_OF_PKG_ITEM"), \\
        ltrim(df_header_split.pack_size, lit("0")).alias("PACK_SIZE"), \\
        df_header_split.delivery_method.alias("DELIVERY_METHOD"), \\
        df_header_split.style.alias("STYLE"), \\
        df_header_split.occ_no.alias("OCC_NO"), \\
        lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")).alias("CRT_DTTM"), \\
        lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")).alias("UPDT_DTTM"), \\
        lit(file_name).cast("string").alias("FILENAME"), \\
        lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")).alias("RUN_ID") \\
    )

    final_df.write.mode("append").save_as_table(target_table)

    target_file_name = file_name.replace(".dat",".csv")

    final_df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/processed/success/"+formatted_year+"/"+formatted_month+"/"+target_file_name,file_format_type="csv",OVERWRITE=True,header=True)
    
    return "Success"
';

CREATE OR REPLACE PROCEDURE NTASDL_RAW.NA_TRXN_IMS_INV_PREPROCESSING("PARAM" ARRAY)
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
    # Param=["HK_IMS_Irpt_wingkeung_20240331_20240405011609.csv","NTASDL_RAW.DEV_LOAD_STAGE_ADLS_HKG","ims/transaction/wing-keung/HK_IMS_Irpt_wingkeung/","SDL_HK_IMS_WINGKEUNG_INV"]
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        
        df_schema = StructType([
                # StructField("mnth_id",StringType(), True),
                StructField("date",StringType(), True),
                StructField("stk_code",StringType(), True),
                StructField("prod_code",StringType(), True),
                StructField("chn_desp",StringType(), True),
                StructField("chn_uom",StringType(), True),
                StructField("closing",StringType(), True),
                StructField("amount",StringType(), True)
            ])
        
        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", ",")\\
            .option("encoding","Big5")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
    
        # df.show(8)
        
        df=df.na.drop("all")
        if df.count()==0 :
            return "No Data in file"
        
        # df = df.with_column("cdl_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        file_name1=file_name.split(".")[0]+"_"+ datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")+".csv"
        # df = df.with_column("file_name", lit(file_name1))
        df = df.with_column("crt_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        df = df.with_column("updt_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        # extrctd_month_id=file_name.split("_")[1].split(".")[0]
        # df = df.with_column("mnth_id", lit(extrctd_month_id))
        
        # date=file_name.split("_")[3].split(".")[0]
        # year=date[0:4]
        # mnth_no=date[4:6]
        # inv_week=date[6:]
        # df = df.withColumn("year", lit(year))
        # df = df.with_column("mnth_no", lit(mnth_no))
        # df = df.with_column("inv_week", lit(inv_week))  
        
        snowdf= df.select(
                    "date",
                    "stk_code",
                    "prod_code",
                    "chn_desp",
                    "chn_uom",
                    regexp_replace(df.col("closing"),",","").as_("closing"),
                    regexp_replace(df.col("amount"),",","").as_("Amount"),
                    "crt_dttm",
                    "updt_dttm"
                    )
        
        file_name=file_name.split(".")[0]+"_"+datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
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
        return error_message';

CREATE OR REPLACE PROCEDURE NTASDL_RAW.OTC_INV_1_PREPROCESSING("PARAM" ARRAY)
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

CREATE OR REPLACE PROCEDURE NTASDL_RAW.OTC_INV_2_PREPROCESSING("PARAM" ARRAY)
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
		

CREATE OR REPLACE PROCEDURE NTASDL_RAW.POP6_TH_POP_LISTS_PREPROCESSING("PARAM" ARRAY)
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

CREATE OR REPLACE PROCEDURE NTASDL_RAW.PRODUCT_MASTER_DAILY_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them.

import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col,lit,trim,when
from snowflake.snowpark.types import  StringType, StructType, StructField,IntegerType
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz



def main(session: snowpark.Session,Param): 
       
    try :
            
        #Param=[''master-product-ranking-A00165277-daily-20211102_10.csv'',''NTASDL_RAW.DEV_LOAD_STAGE_ADLS_KOR'',''dev/coupang_premium/transaction'',''sdl_kr_coupang_product_master'']

        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        # Define the schema for the DataFrame
        
        df_schema=StructType([
                        StructField("category_depth1", StringType()),
                        StructField("category_depth2", StringType()),
                        StructField("category_depth3", StringType()),
                        StructField("all_brand", StringType()),
                        StructField("coupang_sku_id", StringType()),
                        StructField("coupang_sku_name", StringType()),
                        StructField("ranking", StringType()),
                        StructField("jnj_product_flag", StringType())
                        ])
                
        dataframe = session.read.schema(df_schema) \\
        .option("skip_header",1) \\
        .option("field_delimiter", "\\u0001") \\
        .option("encoding","UTF-8")\\
        .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
        

        
        dataframe=dataframe.na.drop("all")
        if dataframe.count()==0:
            return "No Data in file"


        yearmo = file_name.split(".")[0].split("-")[5].split("_")[0]
        dataframe = dataframe.with_column("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        dataframe = dataframe.with_column("FILE_NAME",lit(file_name))
        dataframe = dataframe.withColumn("yearmo", lit(yearmo))
        

        
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

CREATE OR REPLACE PROCEDURE NTASDL_RAW.PRODUCT_MASTER_MONTHLY_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them.

import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col,lit,trim,when
from snowflake.snowpark.types import  StringType, StructType, StructField,IntegerType
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz



def main(session: snowpark.Session,Param): 
       
    try :
            
        #Param=[''master-product-ranking-A00165277-monthly-20210831_10.csv'',''NTASDL_RAW.DEV_LOAD_STAGE_ADLS_KOR'',''dev/coupang_premium/transaction'',''sdl_kr_coupang_product_summary_monthly'']

        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        # Define the schema for the DataFrame
        
        df_schema=StructType([
                        StructField("category_depth1", StringType()),
                        StructField("category_depth2", StringType()),
                        StructField("category_depth3", StringType()),
                        StructField("all_brand", StringType()),
                        StructField("coupang_sku_id", StringType()),
                        StructField("coupang_sku_name", StringType()),
                        StructField("ranking", StringType()),
                        StructField("jnj_product_flag", StringType())
                        ])
                
        dataframe = session.read.schema(df_schema) \\
        .option("skip_header",1) \\
        .option("field_delimiter", "\\u0001") \\
        .option("encoding","UTF-8")\\
        .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
        

        
        dataframe=dataframe.na.drop("all")
        if dataframe.count()==0:
            return "No Data in file"


        yearmo = file_name.split(".")[0].split("-")[5].split("_")[0]
        dataframe = dataframe.with_column("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        dataframe = dataframe.with_column("FILE_NAME",lit(file_name))
        dataframe = dataframe.withColumn("yearmo", lit(yearmo))
        

        
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
CREATE OR REPLACE PROCEDURE NTASDL_RAW.PRODUCT_MASTER_WEEKLY_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them.

import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col,lit,trim,when
from snowflake.snowpark.types import  StringType, StructType, StructField,IntegerType
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz



def main(session: snowpark.Session,Param): 
       
    try :
            
        #Param=[''master-product-ranking-A00165277-weekly-20210904_10.csv'',''NTASDL_RAW.DEV_LOAD_STAGE_ADLS_KOR'',''dev/coupang_premium/transaction'',''sdl_kr_coupang_product_summary_weekly'']

        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        # Define the schema for the DataFrame
        
        df_schema=StructType([
                        StructField("category_depth1", StringType()),
                        StructField("category_depth2", StringType()),
                        StructField("category_depth3", StringType()),
                        StructField("all_brand", StringType()),
                        StructField("coupang_sku_id", StringType()),
                        StructField("coupang_sku_name", StringType()),
                        StructField("ranking", StringType()),
                        StructField("jnj_product_flag", StringType())
                        ])
                
        dataframe = session.read.schema(df_schema) \\
        .option("skip_header",1) \\
        .option("field_delimiter", "\\u0001") \\
        .option("encoding","UTF-8")\\
        .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
        

        
        dataframe=dataframe.na.drop("all")
        if dataframe.count()==0:
            return "No Data in file"


        yearmo = file_name.split(".")[0].split("-")[5].split("_")[0]
        dataframe = dataframe.with_column("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        dataframe = dataframe.with_column("FILE_NAME",lit(file_name))
        dataframe = dataframe.withColumn("yearmo", lit(yearmo))
        

        
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

CREATE OR REPLACE PROCEDURE NTASDL_RAW.PRODUCT_RANKING_DAILY_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them.

import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col,lit,trim,when
from snowflake.snowpark.types import  StringType, StructType, StructField,IntegerType
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz



def main(session: snowpark.Session,Param): 
       
    try :
            
        #Param=[''product-ranking-A00165277-daily-20211107_18.csv'',''NTASDL_RAW.DEV_LOAD_STAGE_ADLS_KOR'',''dev/coupang_premium/transaction'',''sdl_kr_coupang_product_ranking_daily'']

        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        # Define the schema for the DataFrame
        
        df_schema=StructType([
                        StructField("product_ranking_date", StringType()),
                        StructField("category_depth1", StringType()),
                        StructField("category_depth2", StringType()),
                        StructField("category_depth3", StringType()),
                        StructField("coupang_sku_id", StringType()),
                        StructField("coupang_sku_name", StringType()),
                        StructField("ranking", StringType())
                        ])
                
        dataframe = session.read.schema(df_schema) \\
        .option("skip_header",1) \\
        .option("field_delimiter", "\\u0001") \\
        .option("encoding","UTF-8")\\
        .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
        

        
        dataframe=dataframe.na.drop("all")
        if dataframe.count()==0:
            return "No Data in file"


        yearmo = file_name.split(".")[0].split("-")[4].split("_")[0]
        dataframe = dataframe.with_column("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        dataframe = dataframe.with_column("FILE_NAME",lit(file_name))
        dataframe = dataframe.withColumn("yearmo", lit(yearmo))
        dataframe = dataframe.withColumn("data_granularity", lit(''daily''))

        
        # Load Data to the target table
        dataframe.write.mode("append").saveAsTable(target_table)
       
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        #move to success
        file_name=file_name.split(".")[0]+''_''+datetime.now().strftime("%Y%m%d%H%M%S")
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

CREATE OR REPLACE PROCEDURE NTASDL_RAW.PRODUCT_RANKING_MONTHLY_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them.

import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col,lit,trim,when
from snowflake.snowpark.types import  StringType, StructType, StructField,IntegerType
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz



def main(session: snowpark.Session,Param): 
       
    try :
            
        #Param=[''product-ranking-A00165277-monthly-20211031_1.csv'',''NTASDL_RAW.DEV_LOAD_STAGE_ADLS_KOR'',''dev/coupang_premium/transaction'',''sdl_kr_coupang_product_ranking_monthly'']

        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        # Define the schema for the DataFrame
        
        df_schema=StructType([
                        StructField("product_ranking_date", StringType()),
                        StructField("category_depth1", StringType()),
                        StructField("category_depth2", StringType()),
                        StructField("category_depth3", StringType()),
                        StructField("coupang_sku_id", StringType()),
                        StructField("coupang_sku_name", StringType()),
                        StructField("ranking", StringType())
                        ])
                
        dataframe = session.read.schema(df_schema) \\
        .option("skip_header",1) \\
        .option("field_delimiter", "\\u0001") \\
        .option("encoding","UTF-8")\\
        .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
        

        
        dataframe=dataframe.na.drop("all")
        if dataframe.count()==0:
            return "No Data in file"

        
       

        
        
        yearmo = file_name.split(".")[0].split("-")[4].split("_")[0]
        dataframe = dataframe.with_column("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        dataframe = dataframe.with_column("FILE_NAME",lit(file_name))
        dataframe = dataframe.withColumn("yearmo", lit(yearmo))
        dataframe = dataframe.withColumn("data_granularity", lit(''monthly''))

        
        # Load Data to the target table
        dataframe.write.mode("append").saveAsTable(target_table)
       
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        #move to success
        file_name=file_name[0:44]+''_''+datetime.now().strftime("%Y%m%d%H%M%S")
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

CREATE OR REPLACE PROCEDURE NTASDL_RAW.PRODUCT_RANKING_WEEKLY_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them.

import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col,lit,trim,when
from snowflake.snowpark.types import  StringType, StructType, StructField,IntegerType
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz



def main(session: snowpark.Session,Param): 
       
    try :
            
        #Param=[''product-ranking-A00165277-weekly-20210904_10.csv'',''NTASDL_RAW.DEV_LOAD_STAGE_ADLS_KOR'',''dev/coupang_premium/transaction'',''sdl_kr_coupang_product_ranking_weekly'']

        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        # Define the schema for the DataFrame
        
        df_schema=StructType([
                        StructField("product_ranking_date", StringType()),
                        StructField("category_depth1", StringType()),
                        StructField("category_depth2", StringType()),
                        StructField("category_depth3", StringType()),
                        StructField("coupang_sku_id", StringType()),
                        StructField("coupang_sku_name", StringType()),
                        StructField("ranking", StringType())
                        ])
                
        dataframe = session.read.schema(df_schema) \\
        .option("skip_header",1) \\
        .option("field_delimiter", "\\u0001") \\
        .option("encoding","UTF-8")\\
        .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
        

        
        dataframe=dataframe.na.drop("all")
        if dataframe.count()==0:
            return "No Data in file"

        
       

        
        
        yearmo = file_name.split(".")[0].split("-")[4].split("_")[0]
        dataframe = dataframe.with_column("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        dataframe = dataframe.with_column("FILE_NAME",lit(file_name))
        dataframe = dataframe.withColumn("yearmo", lit(yearmo))
        dataframe = dataframe.withColumn("data_granularity", lit(''weekly''))

        
        # Load Data to the target table
        dataframe.write.mode("append").saveAsTable(target_table)
       
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        #move to success
        file_name=file_name[0:44]+''_''+datetime.now().strftime("%Y%m%d%H%M%S")
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

CREATE OR REPLACE PROCEDURE NTASDL_RAW.SDL_HK_IMS_VIVA_WING_PREPROCESSING("PARAM" ARRAY)
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
    #Param=["HK_IMS_Wing_Keung_DataCube_20240518.txt.","NTASDL_RAW.DEV_LOAD_STAGE_ADLS_hkg","dev/ims/wing-keung","SDL_HK_IMS_WINGKEUNG_SEL_OUT"]
    try:
        file_name = Param[0]
        stage_name = Param[1]
        temp_stage_path = Param[2]
        target_table = Param[3]

        df_schema = StructType([
                    StructField("calendar_sid", StringType(), True),
                    StructField("sales_office", StringType(), True),
                    StructField("sales_group", StringType(), True),
                    StructField("sales_office_name", StringType(), True),
                    StructField("sales_group_name", StringType(), True),
                    StructField("account_types", StringType(), True),
                    StructField("sales_volume", StringType(), True),
                    StructField("sales_order_quantity", StringType(), True),
                    StructField("net_trade_sales", StringType(), True),
                    StructField("customer_name", StringType(), True),
                    StructField("customer_number", StringType(), True),
                    StructField("base_product", StringType(), True),
                    StructField("variant", StringType(), True),
                    StructField("mvgr1_base", StringType(), True),
                    StructField("mvgr2_variant", StringType(), True),
                    StructField("mega_brand", StringType(), True),
                    StructField("brand", StringType(), True),
                    StructField("mvgr4_mega", StringType(), True),
                    StructField("mvgr5_brand", StringType(), True),
                    StructField("product_description", StringType(), True),
                    StructField("product_number", StringType(), True),
                    StructField("local_curr_exch_rate", StringType(), True),
                    StructField("employee", StringType(), True),
                    StructField("employee_name", StringType(), True),
                    StructField("transactiontype", StringType(), True),
                    StructField("return_reason", StringType(), True),
                    StructField("country_code", StringType(), True),
                    StructField("currency", StringType(), True)
                ])

        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",0)\\
            .option("field_delimiter", ",")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name.split(".")[0])
        
              
        df = df.na.drop("all")
        if df.count() == 0:
            return "No Data in file"


        df = df.withColumn("calendar_sid", regexp_replace(col("calendar_sid"), ''[^0-9]'', ""))
        df = df.withColumn("calendar_sid", to_date(col(''calendar_sid''),''yyyyMMdd''))
        df = df.withColumn("country_code", lit(''HK''))
        df = df.withColumn("currency", lit(''HKD''))        
         
        
        
        #df = df.withColumn("filename", lit(file_name.split(".")[0]+"_"+datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")+".csv"))
        df = df.withColumn("crt_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        df = df.withColumn("run_id", lit(None))
        
        snowdf = df.select(
                            "calendar_sid",
                            "sales_office",
                            "sales_group",
                            "sales_office_name",
                            "sales_group_name",
                            "account_types",
                            "sales_volume",
                            "sales_order_quantity",
                            "net_trade_sales",
                            "customer_name",
                            "customer_number",
                            "base_product",
                            "variant",
                            "mvgr1_base",
                            "mvgr2_variant",
                            "mega_brand",
                            "brand",
                            "mvgr4_mega",
                            "mvgr5_brand",
                            "product_description",
                            "product_number",
                            "local_curr_exch_rate",
                            "employee",
                            "employee_name",
                            "transactiontype",
                            "return_reason",
                            "country_code",
                            "currency",
                            "crt_dttm",
                            "run_id"
                        )
        
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        
        #move to success
        
        snowdf.write.mode("append").saveAsTable(stage_name.split(".")[0] + "." + target_table)
        file_name1 = file_name.split(".")[0] + "_" + datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S") + ".csv"
        snowdf.write.copy_into_location("@" + stage_name + "/" + temp_stage_path + "/processed/success/" + formatted_year + "/" + formatted_month + "/" + file_name1, header=True, OVERWRITE=True,file_format_name=''csv_format_comma'')
        
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
		
CREATE OR REPLACE PROCEDURE NTASDL_RAW.SDL_HK_POS_SCORECARD_MANNINGS_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('openpyxl==3.1.2','snowflake-snowpark-python==*','xlrd==2.0.1')
HANDLER = 'main'
EXECUTE AS OWNER
AS '#from snowflake.snowpark.types import StringType, StructType, StructField
import snowflake.snowpark
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
   #Param=["J_J_30032024.xlsx","NTASDL_RAW.DEV_LOAD_STAGE_ADLS_hkg","dev/pos/Mannings","sdl_hk_pos_scorecard_mannings"]
    try:
        file_name = Param[0].split(".")[0]+".xlsx"
        stage_name = Param[1]
        temp_stage_path = Param[2]
        target_table = Param[3]

       

        full_path = "@"+stage_name+"/"+temp_stage_path+"/"+file_name
        with SnowflakeFile.open(full_path, "rb", require_scoped_url = False) as f:
            dataset = pd.read_excel(f,sheet_name=0)
        #return session.create_dataframe(dataset)

        z=[]
        
        for i in range(0,list(dataset.shape)[0]):
            if dataset.iloc[i][0] == ''Vendor ID'':
                 z.append(i)
                    
        z=z[0]
            
        x=[]
        for i in range(0,5):
            x.append(dataset.iloc[z,i])
                
            
            
        
            
            # Check for first 4 columns that have Total
        colnames = dataset.columns
        total_pairs = list(zip(dataset[colnames[0]]==''Total'', 
                            dataset[colnames[1]] ==''Total'', 
                            dataset[colnames[2]] ==''Total'', 
                            dataset[colnames[3]] ==''Total'',
                            dataset[colnames[4]] ==''Total''))
            
        index_to_keep = [not any(x) for x in total_pairs]
        dataset = dataset[index_to_keep]
            
            # print(''before: \\n'', dataset[colnames[0:7]].head(10))
            
            # For first 6 columns fill na as ffill
        df_dims = dataset[colnames[0:5]].copy()
        df_dims.fillna(method=''ffill'', inplace = True)
            
            # For the rest columns fill na as 0
        df_data = dataset[colnames[5:]].copy()
        df_data.fillna(value = 0, inplace = True)
            
            # Combine results together
        dataset = pd.concat([df_dims, df_data], axis = 1)
            # print(dataset.head(10))
            
            
            # # Transpose date from column to row
            
            
        columnIndex = 5
        dateValues = []
        data = [[]]
            
        for j in range(0,len(dataset.columns)):
                # Get available date values in dataset after column index indicator
            if j > columnIndex:
                selectedDate = dataset.iloc[1][j]
                dateValues.append(selectedDate)
            
            # Put date values in an array & remove NULLs
        dateValues = [x for x in dateValues if x != 0 ]
        dateValues.pop()   
                
            # Loop through the dataset by rows
        for u in range(z,len(dataset.index)):
                # Only check for data after row 2
            if u > (z+1):
                    # Set up key columns in the right order
                vendorId = dataset.iloc[u][0] 
                vendorDesc = dataset.iloc[u][1] 
                brandId = dataset.iloc[u][2] 
                productId = dataset.iloc[u][3] 
                productDesc = dataset.iloc[u][4] 
                    
                    # Counter value for Sales Value Selection
                valueCounter = 6
                    
                    # For every SKU propagate date values & sales data
                for h in dateValues:
                    date = h
                    salesQty = dataset.iloc[u][valueCounter]
                    valueCounter += 1
                    salesValue = dataset.iloc[u][valueCounter]
                    valueCounter += 1
                    dataRow = [vendorId,vendorDesc,brandId,productId,productDesc,date,salesQty,salesValue]
                    data.append(dataRow)                
            
            
            # Assign columns to the formatted dataset
            #print(''Creating dataframe...'')
        formattedDataSet = pd.DataFrame(data,columns=["vendorId","vendorDesc","brandId","productId","productDesc","date",
                                                  "salesQty","salesValue"])
        formattedDataSet["date"]=pd.to_datetime(formattedDataSet.date, format=''%d/%m/%Y'')
        formattedDataSet["date"]=formattedDataSet["date"].dt.strftime("%d/%m/%Y")
        formattedDataSet = formattedDataSet.drop(labels=0, axis=0)
        #formattedDataSet=pd.DataFrame(formattedDataSet,columns=[''vendorid'', ''vendordesc'', ''brand'', ''productid'', ''productdesc'', ''date'', ''salesqty'', ''salesvalue''])
        
        df = session.create_dataframe(formattedDataSet)
        #return df

       
            
        file_name1 = "formatted_"+file_name.split(".")[0] + ".csv"
        df = df.withColumn("filename", lit(file_name1))
        df = df.withColumn("run_id", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        df = df.withColumn("crt_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))

        # snowdf = df.select(
        #     ''vendorid'', ''vendordesc'', ''brand'', ''productid'', ''productdesc'', ''date'', ''salesqty'', ''salesvalue'', ''filename'', ''run_id'', ''crt_dttm''

        # )
        snowdf=df
        
        file_name = file_name.split(".")[0] + "_" + datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        

        
        
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
        return error_message';

CREATE OR REPLACE PROCEDURE NTASDL_RAW.SDL_KR_ECOM_COUPANG_PREPROCESSING("PARAM" ARRAY)
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
    #Param=["KR_Coupang_20240527.xlsx","NTASDL_RAW.DEV_LOAD_STAGE_ADLS_kor","dev/ecommerce_sellout/Coupang3P","sdl_kr_ECOM_COUPANG"]
    try:
        file_name = Param[0].split(".")[0]+".xlsx"
        stage_name = Param[1]
        temp_stage_path = Param[2]
        target_table = Param[3]

       

        full_path = "@"+stage_name+"/"+temp_stage_path+"/"+file_name
        with SnowflakeFile.open(full_path, "rb", require_scoped_url = False) as f:
            df1 = pd.read_excel(f,sheet_name=0,names=[''EAN_Number'', ''Seller'', ''Order_date'', ''Seller_product_code'', ''Seller_product_name'', ''product_name'', ''Brand'', ''Seller_product_option'', ''Order_Qty'', ''Product_Qty'', ''Product_value'', ''Invoice_Value'', ''delivery_service_company'', ''Shipment'', ''Delivery_date'']
)
            df1["cust_nm"]=''(JU) UNITOA_COUPANG''
            df1["a"]=None
            df1["b"]=None
            df=pd.DataFrame(df1[[''cust_nm'',''a'',''b'',''EAN_Number'',''Brand'',''product_name'',''Delivery_date'',''Product_Qty'',''Invoice_Value'']])
            #df[''Delivery_date''] = df[''Delivery_date''].astype(str).str.slice(0,10).str.replace("-","/")
            df[''Delivery_date''] = pd.to_datetime(df[''Delivery_date'']).dt.strftime(''%Y/%m/%d'')
            df[''Delivery_date''] = pd.to_datetime(df[''Delivery_date'']).dt.strftime(''%d/%m/%y'')
            #df=df1["EAN_Number"]
        df=session.create_dataframe(df)
        # df = df.withColumn("cust_nm", lit(''(JU) UNITOA_COUPANG''))
        # df = df.withColumn("a", lit(None))
        # df = df.withColumn("b", lit(None))
        df = df.withColumn("crt_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        df = df.withColumn("filename", lit(file_name.split(".")[0]+"_"+datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")+".csv"))
        df = df.withColumn("run_id", lit(None))
        

        #snowdf = df.select(
         #       ''cust_nm'',''a'',''b'',''EAN_Number'',''Brand'',''product_name'',''Delivery_date'',''Product_Qty'',''Invoice_Value'',''crt_dttm'',''filename'',''run_id''
         #)
        
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
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message

    except pd.errors.MergeError as merge_error:
        error_message = f"DataFrame merging error: {str(merge_error)}"
        return error_message

    except Exception as e:
        error_message = f"Error: {str(e)}"
        return error_message';

CREATE OR REPLACE PROCEDURE NTASDL_RAW.SDL_KR_ECOM_NAVER_SELLOUT_TEMP_PREPROCESSING("PARAM" ARRAY)
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
import calendar

def main(session: snowpark.Session,Param):
    #Param=["Korea_eComm_Offtake_Naver_Mar2024.csv","NTASDL_RAW.DEV_LOAD_STAGE_ADLS_KOR","dev/ecommerce_sellout/transaction/naver/","sdl_kr_ecom_naver_sellout_temp"]
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        
        df_schema = StructType([
                StructField("order_date", StringType(), True),
                StructField("Product_code", StringType(), True),
                StructField("Product_name", StringType(), True),
                StructField("Product_Count", StringType(), True),
                StructField("Year", StringType(), True),
                StructField("Day", StringType(), True),
                StructField("Month", StringType(), True),
                StructField("Week", StringType(), True),
                StructField("Invoice", StringType(), True),
                StructField("Value", StringType(), True),
                StructField("Brand", StringType(), True)
                
                
            ])
        
        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .option("encoding","UTF-8")\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        # # print(df.show())
        
        df=df.na.drop("all")
        if df.count()==0 :
            return "No Data in file"

        
        #Converting file extensiion from xlsx to csv    
        def filename_change(filename):
                filename1 = filename.split(''.'')
                filename1[1]=''xlsx''
                newfilename = ''.''.join(filename1)
                return newfilename
        
        df = df.with_column("run_id", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        # # #file_name1=file_name.split(".")[0]+"_"+ datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")+".csv"
        df = df.with_column("filename", lit(filename_change(file_name)))
        df = df.with_column("crtd_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
       

        df_pandas = df.to_pandas()
        
        df_pandas[''MONTH''] = df_pandas[''MONTH''].apply(lambda x: list(calendar.month_abbr).index(x) if isinstance(x,str) else x)
        df_pandas[''MONTH''] = df_pandas[''MONTH''].apply(lambda x: ''0''+str(x) if len(str(x).strip())<2 else str(x))
                                                      
        #df_pandas[''MONTH''] = df_pandas[''MONTH''].apply(lambda x: x[:-2])
        
        df_pandas[''WEEK''] = df_pandas[''WEEK''].apply(lambda x:int(x[-1]) if isinstance(x,str) else x)
        
        df = session.create_dataframe(df_pandas)
        
        df = df.withColumn(''cust_code'', lit(None))
        df = df.withColumn(''cust_nm'',lit(''NAVER''))
        df = df.withColumn(''sub_cust_nm'',lit(None))

        df = df.filter(col(''order_date'') == None)
        df = df.filter(col(''order_date'') == '''')
        df = df.filter(upper(col(''brand'')) == ''GIFT'') 
        snowdf= df.select(
                    "cust_nm",
                    "cust_code",
                    "sub_cust_nm",
                    "Product_code",
                    "brand",
                    "product_name",
                    "brand",
                    "year",
                    "month",
                    "week",
                    "day",
                    "order_date",
                    "Product_Count",
                    "value",
                    "run_id",
                    "crtd_dttm",
                    "filename"
                    
                    )
        # # file_name=file_name.split(".")[0]+"_"+datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
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
        return error_message';

CREATE OR REPLACE PROCEDURE NTASDL_RAW.SDL_KR_SFMC_NAVER_DATA_ADDITIONAL_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS 'import pandas as pd
import sys
from pathlib import Path
import warnings
import os 
from snowflake.snowpark.files import SnowflakeFile
from snowflake.snowpark import Window
import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col, lit, date_format, current_timestamp, to_date,md5, year, month, concat, format_number, regexp_replace, to_timestamp, when, trim, upper
from snowflake.snowpark.types import StringType, StructType, StructField,DecimalType
import snowflake.snowpark
import pandas as pd
from datetime import datetime
import pytz

def main(session: snowpark.Session,Param):
    #Param=["PROD_Naver_KR_Lounge_Data_20240424.csv","NTASDL_RAW.DEV_LOAD_STAGE_ADLS_KOR","dev/SFMC/","sdl_kr_sfmc_naver_data_additional"]
    #data is getting loaded in 2 tables by splitting single file
    try:
        
        file_name = Param[0]
        stage_name = Param[1]
        temp_stage_path = Param[2]
        target_table = Param[3]
        
        null_value = "Null"
        lst_columns_attr = [''Naver_ID'',''Acquisition_Channel'',''Reason_for_Joining'',''Skin_type_Body'',''Baby_skin_concerns'',''Oral_health_concerns'',''Skin_concerns_Face'']
        primary_key = "Naver_ID"
        lst_attr_col = ["Naver_ID","Attribute_Name","Attribute_value"]
        attr_val = ''Attribute_value''
        lst_columns_master = [''Acquisition_Channel'',''Reason_for_Joining'',''Skin_type_Body'',''Baby_skin_concerns'',''Oral_health_concerns'',''Skin_concerns_Face'']
    
       # reading the CSV file
        #file_name="PROD_Naver_KR_Lounge_Data_20240424.csv"
        full_path = "@"+stage_name+"/"+temp_stage_path+"/"+file_name
        #full_path= "@DEV_LOAD_STAGE_ADLS_KOR/dev/SFMC/PROD_Naver_KR_Lounge_Data_20240424.csv"
        with SnowflakeFile.open(full_path,''rb'',require_scoped_url=False) as fhandle:
            
            raw_data = pd.read_csv(fhandle, encoding=''utf-16'', sep=''|'')
    
        # fill the null values with ''Null''
        raw_data = raw_data.fillna(null_value)
    
    
        # calling the required columns from main dataframe
        new_attr_df = raw_data[lst_columns_attr]
    
        # preparing data for stacking
        new_attr_df_indexed = new_attr_df.set_index(primary_key)
        new_attr_df_stacked = pd.DataFrame(new_attr_df_indexed.stack()).reset_index()
    
        # renaming columns for the new file
        new_attr_df_stacked.columns = lst_attr_col
    
        # removing the empty spaces
        new_attr_df_stacked[attr_val] = new_attr_df_stacked[attr_val].str.split('';'').map(lambda elements: [e.strip() for e in elements])
        new_attr_df_stacked[attr_val] = new_attr_df_stacked[attr_val].map(lambda elements: list(filter(None, elements)))
    
        # final dataframe
        df_transposed = new_attr_df_stacked.explode(attr_val).reset_index(drop=True)
    
        # removing null values
        df_transposed = df_transposed[df_transposed[attr_val] != null_value]
        df_transposed = df_transposed.dropna(subset=[attr_val]).reset_index(drop=True)
    
        
        file_name1= "PROD_Naver_KR_Lounge_Data_Additional_"+file_name.split("_")[5].split(".")[0]+".csv"
        df_transposed["File_name"]=file_name1
        df_transposed["crtd_dttm"]=datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")
        df_transposed.drop_duplicates(subset=["Naver_ID","Attribute_Name","Attribute_value","File_name"], keep=''last'')
        df_transposed=df_transposed.replace("Null",None)
    
        df1=session.create_dataframe(df_transposed)

        df1.write.mode("append").save_as_table(stage_name.split(".")[0]+"."+target_table)
    
        
        df2 = raw_data.drop(lst_columns_master, axis=1)
        df2=df2.replace("Null",None)
        file_name2= "PROD_Naver_KR_Lounge_Data_Primary_"+file_name.split("_")[5].split(".")[0]+".csv"
        df2["File_name"]=file_name2
        df2["crtd_dttm"]=datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")
        
        df3=session.create_dataframe(df2)

        df3.write.mode("append").save_as_table(stage_name.split(".")[0]+"."+"sdl_kr_sfmc_naver_data")

        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        
        #move to success
        
        file_name2= "PROD_Naver_KR_Lounge_Data_Primary_"+file_name.split("_")[5].split(".")[0] 
        df3.write.copy_into_location("@" + stage_name + "/" + temp_stage_path + "/processed/success/" + formatted_year + "/" + formatted_month + "/" + file_name2, header=True, OVERWRITE=True)
        file_name1= "PROD_Naver_KR_Lounge_Data_Additional_"+file_name.split("_")[5].split(".")[0]
        df3.write.copy_into_location("@" + stage_name + "/" + temp_stage_path + "/processed/success/" + formatted_year + "/" + formatted_month + "/" + file_name1, header=True, OVERWRITE=True)

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

CREATE OR REPLACE PROCEDURE NTASDL_RAW.SDL_KR_SFMC_NAVER_DATA_ADDITIONAL_PREPROCESSSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS 'import pandas as pd
import sys
from pathlib import Path
import warnings
import os 
from snowflake.snowpark.files import SnowflakeFile
from snowflake.snowpark import Window
import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col, lit, date_format, current_timestamp, to_date,md5, year, month, concat, format_number, regexp_replace, to_timestamp, when, trim, upper
from snowflake.snowpark.types import StringType, StructType, StructField,DecimalType
import snowflake.snowpark
import pandas as pd
from datetime import datetime
import pytz

def main(session: snowpark.Session,Param):
    #Param=["PROD_Naver_KR_Lounge_Data_20240501.csv","NTASDL_RAW.DEV_LOAD_STAGE_ADLS_KOR","dev/SFMC/","sdl_kr_sfmc_naver_data_additional"]
    #data is getting loaded in 2 tables by splitting single file
    try:
        
        file_name = Param[0]
        stage_name = Param[1]
        temp_stage_path = Param[2]
        target_table = Param[3]
        
        null_value = "Null"
        lst_columns_attr = [''Naver_ID'',''Acquisition_Channel'',''Reason_for_Joining'',''Skin_type_Body'',''Baby_skin_concerns'',''Oral_health_concerns'',''Skin_concerns_Face'']
        primary_key = "Naver_ID"
        lst_attr_col = ["Naver_ID","Attribute_Name","Attribute_value"]
        attr_val = ''Attribute_value''
        lst_columns_master = [''Acquisition_Channel'',''Reason_for_Joining'',''Skin_type_Body'',''Baby_skin_concerns'',''Oral_health_concerns'',''Skin_concerns_Face'']
    
       # reading the CSV file
        #file_name="PROD_Naver_KR_Lounge_Data_20240424.csv"
        full_path = "@"+stage_name+"/"+temp_stage_path+"/"+file_name
        #full_path= "@DEV_LOAD_STAGE_ADLS_KOR/dev/SFMC/PROD_Naver_KR_Lounge_Data_20240424.csv"
        with SnowflakeFile.open(full_path,''rb'',require_scoped_url=False) as fhandle:
            
            raw_data = pd.read_csv(fhandle, encoding=''utf-16'', sep=''|'')
    
        # fill the null values with ''Null''
        raw_data = raw_data.fillna(null_value)
    
    
        # calling the required columns from main dataframe
        new_attr_df = raw_data[lst_columns_attr]
    
        # preparing data for stacking
        new_attr_df_indexed = new_attr_df.set_index(primary_key)
        new_attr_df_stacked = pd.DataFrame(new_attr_df_indexed.stack()).reset_index()
    
        # renaming columns for the new file
        new_attr_df_stacked.columns = lst_attr_col
    
        # removing the empty spaces
        new_attr_df_stacked[attr_val] = new_attr_df_stacked[attr_val].str.split('';'').map(lambda elements: [e.strip() for e in elements])
        new_attr_df_stacked[attr_val] = new_attr_df_stacked[attr_val].map(lambda elements: list(filter(None, elements)))
    
        # final dataframe
        df_transposed = new_attr_df_stacked.explode(attr_val).reset_index(drop=True)
    
        # removing null values
        df_transposed = df_transposed[df_transposed[attr_val] != null_value]
        df_transposed = df_transposed.dropna(subset=[attr_val]).reset_index(drop=True)
    
        
        file_name1= "PROD_Naver_KR_Lounge_Data_Additional_"+file_name.split("_")[5].split(".")[0]+".csv"
        df_transposed["File_name"]=file_name1
        df_transposed["crtd_dttm"]=datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")
        df_transposed.drop_duplicates(subset=["Naver_ID","Attribute_Name","Attribute_value","File_name"], keep=''last'')
        df_transposed=df_transposed.replace("Null",None)
    
        df1=session.create_dataframe(df_transposed)

        df1.write.mode("append").save_as_table(stage_name.split(".")[0]+"."+target_table)
    
        
        df2 = raw_data.drop(lst_columns_master, axis=1)
        df2=df2.replace("Null",None)
        file_name2= "PROD_Naver_KR_Lounge_Data_Primary_"+file_name.split("_")[5].split(".")[0]+".csv"
        df2["File_name"]=file_name2
        df2["crtd_dttm"]=datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")
        
        df3=session.create_dataframe(df2)

        df3.write.mode("append").save_as_table(stage_name.split(".")[0]+"."+"sdl_kr_sfmc_naver_data")

        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        
        #move to success
        
        file_name2= "PROD_Naver_KR_Lounge_Data_Primary_"+file_name.split("_")[5].split(".")[0] 
        df3.write.copy_into_location("@" + stage_name + "/" + temp_stage_path + "/processed/success/" + formatted_year + "/" + formatted_month + "/" + file_name2, header=True, OVERWRITE=True)
        file_name1= "PROD_Naver_KR_Lounge_Data_Additional_"+file_name.split("_")[5].split(".")[0]
        df3.write.copy_into_location("@" + stage_name + "/" + temp_stage_path + "/processed/success/" + formatted_year + "/" + formatted_month + "/" + file_name1, header=True, OVERWRITE=True)

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

CREATE OR REPLACE PROCEDURE NTASDL_RAW.SDL_POP6_TH_POPS_PREPROCESSING("PARAM" ARRAY)
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

CREATE OR REPLACE PROCEDURE NTASDL_RAW.SDL_POP6_TH_POP_LISTS_PREPROCESSING("PARAM" ARRAY)
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
		
CREATE OR REPLACE PROCEDURE NTASDL_RAW.SDL_POP6_TH_PRODUCTS_PREPROCESSING("PARAM" ARRAY)
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

CREATE OR REPLACE PROCEDURE NTASDL_RAW.SDL_POP6_TH_PRODUCTS_PREPROCESSING("PARAM" ARRAY)
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

CREATE OR REPLACE PROCEDURE NTASDL_RAW.SDL_POP6_TH_PRODUCT_LISTS_ALLOCATION_PREPROCESSING("PARAM" ARRAY)
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
CREATE OR REPLACE PROCEDURE NTASDL_RAW.SDL_POP6_TH_PRODUCT_LISTS_POPS_PREPROCESSING("PARAM" ARRAY)
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

CREATE OR REPLACE PROCEDURE NTASDL_RAW.SDL_POP6_TH_PRODUCT_LISTS_PRODUCTS_PREPROCESSING("PARAM" ARRAY)
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


CREATE OR REPLACE PROCEDURE NTASDL_RAW.SDL_POP6_TH_USERS_PREPROCESSING("PARAM" ARRAY)
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

CREATE OR REPLACE PROCEDURE NTASDL_RAW.SDL_TW_AS_WATSONS_INVENTORY_PREPROCESSING("PARAM" ARRAY)
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
    #Param=["TW_2024_WK15_JJ_KPI_Status.csv","NTASDL_RAW.DEV_LOAD_STAGE_ADLS_TWN","dev/pos/transaction/watsons/inventory","SDL_TW_AS_WATSONS_INVENTORY"]
    try:
        file_name = Param[0]
        stage_name = Param[1]
        temp_stage_path = Param[2]
        target_table = Param[3]

        df_schema = StructType([
                    StructField("SUPPLIER", StringType(), True),
                    StructField("SKU_NO", StringType(), True),
                    StructField("BUY_CODE", StringType(), True),
                    StructField("HOME_CDESC", StringType(), True),
                    StructField("PRDT_GRP", StringType(), True),
                    StructField("GRP_DESC", StringType(), True),
                    StructField("PRDT_CAT", StringType(), True),
                    StructField("CAT_DESC", StringType(), True),
                    StructField("C_CDESC", StringType(), True),
                    StructField("TYPE", StringType(), True),
                    StructField("AVGE_SALES_COST_VALUE", StringType(), True),
                    StructField("TOTAL_STOCK_QTY", StringType(), True),
                    StructField("TOTAL_STOCK_VALUE", StringType(), True),
                    StructField("WEEKS_HOLDING_sales", StringType(), True),
                    StructField("WEEKS_HOLDING", StringType(), True),
                    StructField("FIRST_RECV_DATE", StringType(), True),
                    StructField("TURN_TYPE_SALES", StringType(), True),
                    StructField("TURN_TYPE", StringType(), True),
                    StructField("UDA73", StringType(), True),
                    StructField("DISCONTINUE_DATE", StringType(), True),
                    StructField("STOCKCLASS", StringType(), True),
                    StructField("POG", StringType(), True),
                    StructField("EAN", StringType(), True)
                    ])

        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "\\u0001")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        df = df.na.drop("all")
        if df.count() == 0:
            return "No Data in file"

        df = df.withColumn("YEAR", lit(file_name.split("_")[1]))
        df = df.withColumn("week", lit(file_name.split("_")[2][2:4]))
        
        df = df.withColumn("filename", lit(file_name.split(".")[0]+".xlsx"))
        
        df = df.withColumn("crt_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        #df = df.withColumn("run_id", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))

# Define the list of column names
        snowdf=df.select("YEAR","week",
        "SUPPLIER", "SKU_NO", "BUY_CODE", "HOME_CDESC", "PRDT_GRP", "GRP_DESC", 
        "PRDT_CAT", "CAT_DESC", "C_CDESC", "TYPE", "AVGE_SALES_COST_VALUE", 
        "TOTAL_STOCK_QTY", "TOTAL_STOCK_VALUE", "WEEKS_HOLDING_sales", 
        "WEEKS_HOLDING", "FIRST_RECV_DATE", "TURN_TYPE_SALES", "TURN_TYPE", 
        "UDA73", "DISCONTINUE_DATE", "STOCKCLASS", "POG", "EAN","filename","crt_dttm"
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
        return error_message';


CREATE OR REPLACE PROCEDURE NTASDL_RAW.SDL_TW_IMS_DSTR_STD_CUSTOMER_PREPROCESSING("PARAM" ARRAY)
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
    #Param=["TW_IMS_Distributor_136454_Customer_20240515.csv","NTASDL_RAW.DEV_LOAD_STAGE_ADLS_twn","dev/ims/distributor/136454","sdl_tw_ims_dstr_std_customer_136454_customer"]
    try:
        file_name = Param[0]
        stage_name = Param[1]
        temp_stage_path = Param[2]
        target_table = Param[3]

        df_schema = StructType([
                    StructField("distributor_code", StringType()),
                    StructField("distributor_customer_code", StringType()),
                    StructField("distributor_customer_name", StringType()),
                    StructField("distributor_address", StringType()),
                    StructField("distributor_telephone", StringType()),
                    StructField("distributor_contact", StringType()),
                    StructField("distributor_sales_area", StringType())
                ])

        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", ",")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
        
              
        df = df.na.drop("all")
        if df.count() == 0:
            return "No Data in file"

        df=df.withColumn("transaction_date",regexp_replace(col("distributor_telephone"), "\\''",""))
        
        #df = df.withColumn("filename", lit(file_name.split(".")[0]+"_"+datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")+".csv"))
        df = df.withColumn("crt_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        df = df.withColumn("upd_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))        
        snowdf = df.select(
                        "distributor_code",
                        "distributor_customer_code",
                        "distributor_customer_name",
                        "distributor_address",
                        "distributor_telephone",
                        "distributor_contact",
                        "distributor_sales_area",
                        "crt_dttm",
                        "upd_dttm"
                        )
        
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        
        #move to success
        
        snowdf.write.mode("append").saveAsTable(stage_name.split(".")[0] + "." + target_table)
        file_name1 = file_name.split(".")[0] + "_" + datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S") + ".csv"
        snowdf.write.copy_into_location("@" + stage_name + "/" + temp_stage_path + "/processed/success/" + formatted_year + "/" + formatted_month + "/" + file_name1, header=True, OVERWRITE=True,)
        
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
CREATE OR REPLACE PROCEDURE NTASDL_RAW.SDL_TW_IMS_DSTR_STD_SEL_OUT_PREPROCESSING("PARAM" ARRAY)
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
    #Param=["TW_IMS_Distributor_107479_SellOut_20240513.csv","NTASDL_RAW.DEV_LOAD_STAGE_ADLS_twn","dev/ims/distributor/107479","sdl_tw_ims_dstr_std_sel_out_107479_sellout"]
    try:
        file_name = Param[0]
        stage_name = Param[1]
        temp_stage_path = Param[2]
        target_table = Param[3]

        df_schema = StructType([
                            StructField("transaction_date", StringType()),
                            StructField("distributor_code", StringType()),
                            StructField("distributor_name", StringType()),
                            StructField("distributors_customer_code", StringType()),
                            StructField("distributors_customer_name", StringType()),
                            StructField("distributors_product_code", StringType()),
                            StructField("distributors_product_name", StringType()),
                            StructField("report_period_start_date", StringType()),
                            StructField("report_period_end_date", StringType()),
                            StructField("ean", StringType()),
                            StructField("uom", StringType()),
                            StructField("unit_price", StringType()),
                            StructField("sales_amount", StringType()),
                            StructField("sales_qty", StringType()),
                            StructField("return_qty", StringType()),
                            StructField("return_amount", StringType()),
                            StructField("sales_rep_code", StringType()),
                            StructField("sales_rep_name", StringType())
                        ])

        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", ",")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name.split(".")[0])
        
              
        df = df.na.drop("all")
        if df.count() == 0:
            return "No Data in file"

        df=df.withColumn("transaction_date",regexp_replace(col("transaction_date"), "/","-"))
        df1=df.filter(col("transaction_date").like("%-%"))
        if df1.count()>0:
            df=df.withColumn("transaction_date",date_format(col("transaction_date"),''yyyy-mm-dd''))
        else:
            df=df.withColumn("transaction_date",col("transaction_date").cast("String"))
            df=df.withColumn("transaction_date",to_date(col("transaction_date"),''yyyymmdd''))
            #df=df.withColumn("transaction_date",date_format(col("transaction_date"),''yyyy-mm-dd''))

        df = df.withColumn("unit_price", regexp_replace(col("unit_price"), ",", ""));
        df = df.withColumn("sales_amount", regexp_replace(col("sales_amount"), ",", ""));
        df = df.withColumn("sales_qty", regexp_replace(col("sales_qty"), ",", ""));
        df = df.withColumn("return_qty", regexp_replace(col("return_qty"), ",", ""));
        df = df.withColumn("return_amount", regexp_replace(col("return_amount"), ",", ""));
         
        
        
        #df = df.withColumn("filename", lit(file_name.split(".")[0]+"_"+datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")+".csv"))
        df = df.withColumn("crt_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        df = df.withColumn("run_id", lit(None))
        
        snowdf = df.select(
                        "transaction_date",
                            "distributor_code",
                            "distributor_name",
                            "distributors_customer_code",
                            "distributors_customer_name",
                            "distributors_product_code",
                            "distributors_product_name",
                            "report_period_start_date",
                            "report_period_end_date",
                            "ean",
                            "uom",
                            "unit_price",
                            "sales_amount",
                            "sales_qty",
                            "return_qty",
                            "return_amount",
                            "sales_rep_code",
                            "sales_rep_name",
                            "crt_dttm",
                            "run_id"
                        )
        
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        
        #move to success
        
        snowdf.write.mode("append").saveAsTable(stage_name.split(".")[0] + "." + target_table)
        #snowdf.write.mode("append").saveAsTable(target_table)
        file_name1 = file_name.split(".")[0] + "_" + datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S") + ".csv"
        snowdf.write.copy_into_location("@" + stage_name + "/" + temp_stage_path + "/processed/success/" + formatted_year + "/" + formatted_month + "/" + file_name1, header=True, OVERWRITE=True,)
        
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

CREATE OR REPLACE PROCEDURE NTASDL_RAW.SDL_TW_IMS_DSTR_STD_STOCK_PREPROCESSING("PARAM" ARRAY)
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
    #Param=["TW_IMS_Distributor_135561_Stock_20240506.csv","NTASDL_RAW.DEV_LOAD_STAGE_ADLS_twn","dev/ims/distributor/135561","sdl_tw_ims_dstr_std_stock_135561_stock"]
    try:
        file_name = Param[0]
        stage_name = Param[1]
        temp_stage_path = Param[2]
        target_table = Param[3]

        df_schema = StructType([
                    StructField("distributor_code", StringType(), True),
                    StructField("ean", StringType(), True),
                    StructField("distributor_product_code", StringType(), True),
                    StructField("quantity", StringType(), True),
                    StructField("total_cost", StringType(), True),
                    StructField("inventory_date", StringType(), True),
                    StructField("distributors_product_name", StringType(), True),
                    StructField("uom", StringType(), True),
                    StructField("storage_name", StringType(), True)
                ])
        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", ",")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name.split(".")[0])
        
              
        df = df.na.drop("all")
        if df.count() == 0:
            return "No Data in file"

        df=df.withColumn("inventory_date",regexp_replace(col("inventory_date"), "/","-"))
        df1=df.filter(col("inventory_date").like("%-%"))
        if df1.count()>0:
            df=df.withColumn("inventory_date",date_format(col("inventory_date"),''yyyy-mm-dd''))
        else:
            df=df.withColumn("inventory_date",col("inventory_date").cast("String"))
            df=df.withColumn("inventory_date",to_date(col("inventory_date"),''yyyymmdd''))
        
        df = df.withColumn("filename", lit(file_name.split(".")[0]+"_"+datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")+".csv"))
        df = df.withColumn("crt_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        df = df.withColumn("run_id", lit(None))
        
        snowdf = df.select(
                         "distributor_code",
                            "ean",
                            "distributor_product_code",
                            "quantity",
                            "total_cost",
                            "inventory_date",
                            "distributors_product_name",
                            "uom",
                            "storage_name",
                            "crt_dttm",
                            "run_id",
                             "filename"
                        )
        
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        
        #move to success
        
        snowdf.write.mode("append").saveAsTable(stage_name.split(".")[0] + "." + target_table)
        #snowdf.write.mode("append").saveAsTable(target_table)
        file_name1 = file_name.split(".")[0] + "_" + datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S") + ".csv"
        snowdf.write.copy_into_location("@" + stage_name + "/" + temp_stage_path + "/processed/success/" + formatted_year + "/" + formatted_month + "/" + file_name1, header=True, OVERWRITE=True,)
        
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

CREATE OR REPLACE PROCEDURE NTASDL_RAW.SEARCH_KEYWORD_BY_CATEGORY_DAILY_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them.

import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col,lit,trim,when
from snowflake.snowpark.types import  StringType, StructType, StructField,IntegerType
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz



def main(session: snowpark.Session,Param): 
       
    try :
            
        #Param=[''search-keyword-by-category-A00165277-daily-20211018_10.csv'',''NTASDL_RAW.DEV_LOAD_STAGE_ADLS_KOR'',''dev/coupang_premium/transaction/search_keyword_by_category/'',''SDL_RAW_KR_COUPANG_SEARCH_KEYWORD_BY_CATEGORY'']

        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        # Define the schema for the DataFrame
        
        df_schema=StructType([
                        StructField("by_search_keyword", StringType()),
                        StructField("product_ranking_date", StringType()),
                        StructField("category_depth1", StringType()),
                        StructField("category_depth2", StringType()),
                        StructField("category_depth3", StringType()),
                        StructField("ranking", StringType()),
                        StructField("search_keyword", StringType()),
                        StructField("product_ranking", StringType()),
                        StructField("product_name", StringType()),
                        StructField("jnj_product_flag", StringType())
                        
                        ])
                
        dataframe = session.read.schema(df_schema) \\
        .option("skip_header",1) \\
        .option("field_delimiter", "\\u0001") \\
        .option("encoding","UTF-8")\\
        .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
        

        
        dataframe=dataframe.na.drop("all")
        if dataframe.count()==0:
            return "No Data in file"


        yearmo = file_name.split(".")[0].split("_")[0].split("-")[6]
        dataframe = dataframe.with_column("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        dataframe = dataframe.with_column("FILE_NAME",lit(file_name))
        dataframe = dataframe.withColumn("yearmo", lit(yearmo))
        dataframe = dataframe.withColumn("data_granularity", lit(''daily''))

        
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

CREATE OR REPLACE PROCEDURE NTASDL_RAW.SEARCH_KEYWORD_BY_CATEGORY_MONTHLY_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them.

import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col,lit,trim,when
from snowflake.snowpark.types import  StringType, StructType, StructField,IntegerType
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz



def main(session: snowpark.Session,Param): 
       
    try :
            
        #Param=[''search-keyword-by-category-A00165277-monthly-20211031_10.csv'',''NTASDL_RAW.DEV_LOAD_STAGE_ADLS_KOR'',''dev/coupang_premium/transaction/search_keyword_by_category/'',''SDL_RAW_KR_COUPANG_SEARCH_KEYWORD_BY_CATEGORY'']

        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        # Define the schema for the DataFrame
        
        df_schema=StructType([
                        StructField("by_search_keyword", StringType()),
                        StructField("product_ranking_date", StringType()),
                        StructField("category_depth1", StringType()),
                        StructField("category_depth2", StringType()),
                        StructField("category_depth3", StringType()),
                        StructField("ranking", StringType()),
                        StructField("search_keyword", StringType()),
                        StructField("product_ranking", StringType()),
                        StructField("product_name", StringType()),
                        StructField("jnj_product_flag", StringType())
                        
                        ])
                
        dataframe = session.read.schema(df_schema) \\
        .option("skip_header",1) \\
        .option("field_delimiter", "\\u0001") \\
        .option("encoding","UTF-8")\\
        .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
        

        
        dataframe=dataframe.na.drop("all")
        if dataframe.count()==0:
            return "No Data in file"


        yearmo = file_name.split(".")[0].split("_")[0].split("-")[6]
        dataframe = dataframe.with_column("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        dataframe = dataframe.with_column("FILE_NAME",lit(file_name))
        dataframe = dataframe.withColumn("yearmo", lit(yearmo))
        dataframe = dataframe.withColumn("data_granularity", lit(''monthly''))

        
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

CREATE OR REPLACE PROCEDURE NTASDL_RAW.SEARCH_KEYWORD_BY_CATEGORY_WEEKLY_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them.

import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col,lit,trim,when
from snowflake.snowpark.types import  StringType, StructType, StructField,IntegerType
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz



def main(session: snowpark.Session,Param): 
       
    try :
            
        #Param=[''search-keyword-by-category-A00165277-weekly-20211009_10.csv'',''NTASDL_RAW.DEV_LOAD_STAGE_ADLS_KOR'',''dev/coupang_premium/transaction/search_keyword_by_category/'',''SDL_RAW_KR_COUPANG_SEARCH_KEYWORD_BY_CATEGORY'']

        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        # Define the schema for the DataFrame
        
        df_schema=StructType([
                        StructField("by_search_keyword", StringType()),
                        StructField("product_ranking_date", StringType()),
                        StructField("category_depth1", StringType()),
                        StructField("category_depth2", StringType()),
                        StructField("category_depth3", StringType()),
                        StructField("ranking", StringType()),
                        StructField("search_keyword", StringType()),
                        StructField("product_ranking", StringType()),
                        StructField("product_name", StringType()),
                        StructField("jnj_product_flag", StringType())
                        
                        ])
                
        dataframe = session.read.schema(df_schema) \\
        .option("skip_header",1) \\
        .option("field_delimiter", "\\u0001") \\
        .option("encoding","UTF-8")\\
        .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
        

        
        dataframe=dataframe.na.drop("all")
        if dataframe.count()==0:
            return "No Data in file"


        yearmo = file_name.split(".")[0].split("_")[0].split("-")[6]
        dataframe = dataframe.with_column("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        dataframe = dataframe.with_column("FILE_NAME",lit(file_name))
        dataframe = dataframe.withColumn("yearmo", lit(yearmo))
        dataframe = dataframe.withColumn("data_granularity", lit(''weekly''))

        
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

CREATE OR REPLACE PROCEDURE NTASDL_RAW.SEARCH_KEYWORD_BY_PRODUCT_DAILY_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them.

import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col,lit,trim,when
from snowflake.snowpark.types import  StringType, StructType, StructField,IntegerType
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz



def main(session: snowpark.Session,Param): 
       
    try :
            
        #Param=[''search-keyword-by-product-A00165277-daily-20211018_12_1.csv'',''NTASDL_RAW.DEV_LOAD_STAGE_ADLS_KOR'',''dev/coupang_premium/transaction/search_keyword_by_product/'',''sdl_kr_coupang_search_keyword_by_product'']

        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        # Define the schema for the DataFrame
        
        df_schema=StructType([
                        StructField("category_depth1", StringType()),
                        StructField("category_depth2", StringType()),
                        StructField("category_depth3", StringType()),
                        StructField("product_name", StringType()),
                        StructField("ranking", StringType()),
                        StructField("search_keyword", StringType()),
                        StructField("click_rate", StringType()),
                        StructField("cart_transition_rate", StringType()),
                        StructField("purchase_conversion_rate", StringType())
                        
                        ])
                
        dataframe = session.read.schema(df_schema) \\
        .option("skip_header",1) \\
        .option("field_delimiter", "\\u0001") \\
        .option("encoding","UTF-8")\\
        .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
        

        
        dataframe=dataframe.na.drop("all")
        if dataframe.count()==0:
            return "No Data in file"


        yearmo = file_name.split(".")[0].split("-")[6].split("_")[0]
        dataframe = dataframe.with_column("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        dataframe = dataframe.with_column("FILE_NAME",lit(file_name))
        dataframe = dataframe.withColumn("yearmo", lit(yearmo))
        dataframe = dataframe.withColumn("data_granularity", lit(''daily''))

        
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
CREATE OR REPLACE PROCEDURE NTASDL_RAW.SEARCH_KEYWORD_BY_PRODUCT_MONTHLY_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them.

import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col,lit,trim,when
from snowflake.snowpark.types import  StringType, StructType, StructField,IntegerType
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz



def main(session: snowpark.Session,Param): 
       
    try :
            
        #Param=['''',''NTASDL_RAW.DEV_LOAD_STAGE_ADLS_KOR'',''dev/coupang_premium/transaction/search_keyword_by_product/'',''sdl_kr_coupang_search_keyword_by_product'']

        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        # Define the schema for the DataFrame
        
        df_schema=StructType([
                        StructField("category_depth1", StringType()),
                        StructField("category_depth2", StringType()),
                        StructField("category_depth3", StringType()),
                        StructField("product_name", StringType()),
                        StructField("ranking", StringType()),
                        StructField("search_keyword", StringType()),
                        StructField("click_rate", StringType()),
                        StructField("cart_transition_rate", StringType()),
                        StructField("purchase_conversion_rate", StringType())
                        
                        ])
                
        dataframe = session.read.schema(df_schema) \\
        .option("skip_header",1) \\
        .option("field_delimiter", "\\u0001") \\
        .option("encoding","UTF-8")\\
        .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
        

        
        dataframe=dataframe.na.drop("all")
        if dataframe.count()==0:
            return "No Data in file"


        yearmo = file_name.split(".")[0].split("-")[6].split("_")[0]
        dataframe = dataframe.with_column("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        dataframe = dataframe.with_column("FILE_NAME",lit(file_name))
        dataframe = dataframe.withColumn("yearmo", lit(yearmo))
        dataframe = dataframe.withColumn("data_granularity", lit(''monthly''))

        
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
CREATE OR REPLACE PROCEDURE NTASDL_RAW.SEARCH_KEYWORD_BY_PRODUCT_WEEKLY_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them.

import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col,lit,trim,when
from snowflake.snowpark.types import  StringType, StructType, StructField,IntegerType
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz



def main(session: snowpark.Session,Param): 
       
    try :
            
        #Param=['''',''NTASDL_RAW.DEV_LOAD_STAGE_ADLS_KOR'',''dev/coupang_premium/transaction/search_keyword_by_product/'',''sdl_kr_coupang_search_keyword_by_product'']

        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        # Define the schema for the DataFrame
        
        df_schema=StructType([
                        StructField("category_depth1", StringType()),
                        StructField("category_depth2", StringType()),
                        StructField("category_depth3", StringType()),
                        StructField("product_name", StringType()),
                        StructField("ranking", StringType()),
                        StructField("search_keyword", StringType()),
                        StructField("click_rate", StringType()),
                        StructField("cart_transition_rate", StringType()),
                        StructField("purchase_conversion_rate", StringType())
                        
                        ])
                
        dataframe = session.read.schema(df_schema) \\
        .option("skip_header",1) \\
        .option("field_delimiter", "\\u0001") \\
        .option("encoding","UTF-8")\\
        .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
        

        
        dataframe=dataframe.na.drop("all")
        if dataframe.count()==0:
            return "No Data in file"


        yearmo = file_name.split(".")[0].split("-")[6].split("_")[0]
        dataframe = dataframe.with_column("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        dataframe = dataframe.with_column("FILE_NAME",lit(file_name))
        dataframe = dataframe.withColumn("yearmo", lit(yearmo))
        dataframe = dataframe.withColumn("data_granularity", lit(''weekly''))

        
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
CREATE OR REPLACE PROCEDURE NTASDL_RAW.TWSI_TARGET_INGESTION_PREPROCESING("PARAM" ARRAY)
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
    # Param=["Target_vs_PSR_202312_20231220200009.csv","NTASDL_RAW.DEV_LOAD_STAGE_ADLS_TWN","cert-data-lake/TW_Sales_Incentive","SDL_TSI_TARGET_DATA"]
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        
        df_schema = StructType([
                # StructField("mnth_id",StringType(), True),
                StructField("date",StringType(), True),
                StructField("ec",StringType(), True),
                StructField("offtake",StringType(), True),
                StructField("customer_code",StringType(), True),
                StructField("customer_name",StringType(), True),
                StructField("customer_cname",StringType(), True),
                StructField("customer_sname",StringType(), True),
                StructField("nts",StringType(), True),
                StructField("offtake(sellout)",StringType(), True),
                StructField("gts",StringType(), True),
                StructField("pre_sales",StringType(), True),
                StructField("prs_code_01",StringType(), True),
                StructField("prs_code_02",StringType(), True),
                StructField("prs_code_03",StringType(), True),
                StructField("prs_code_04",StringType(), True),
                StructField("prs_code_05",StringType(), True)   
                
            ])
        
        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "")\\
            .option("FIELD_OPTIONALLY_ENCLOSED_BY", "\\"")\\
            .option("encoding","UTF-8")\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
    
        # df.show(8)
        
        df=df.na.drop("all")
        if df.count()==0 :
            return "No Data in file"
    
        
        # df = df.with_column("cdl_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        file_name1=file_name.split(".")[0]+"_"+ datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")+".csv"
        df = df.with_column("filename", lit(file_name1))
        df = df.with_column("crt_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        # extrctd_month_id=file_name.split("_")[1].split(".")[0]
        # df = df.with_column("mnth_id", lit(extrctd_month_id))
        
        # date=file_name.split("_")[3].split(".")[0]
        # year=date[0:4]
        # mnth_no=date[4:6]
        # inv_week=date[6:]
        # df = df.withColumn("year", lit(year))
        # df = df.with_column("mnth_no", lit(mnth_no))
        # df = df.with_column("inv_week", lit(inv_week))  
        
        snowdf= df.select(
                    "date",
                    "ec",
                    "offtake",
                    "customer_code",
                    "customer_name",
                    "customer_cname",
                    "customer_sname",
                    "nts",
                    "offtake(sellout)",
                    "gts",
                    "pre_sales",
                    "prs_code_01",
                    "prs_code_02",
                    "prs_code_03",
                    "prs_code_04",
                    "prs_code_05",
                    "filename",
                    "crt_dttm"
                    )
        file_name=file_name.split(".")[0]+"_"+datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
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
        return error_message';

CREATE OR REPLACE PROCEDURE NTASDL_RAW.TW_CRM_CHILDERN_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS 'from snowflake import snowpark
from snowflake.snowpark.window import Window
from snowflake.snowpark.functions import col,lit,concat,regexp_replace,trim,split,rtrim,upper,coalesce,row_number,when,to_date,is_null
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField,DecimalType,DateType
import pandas as pd
from datetime import datetime
import pytz
import re

def main(session: snowpark.Session,Param):
    #Param = [''TW_CRM_Children_20240430.csv'',''NTASDL_RAW.DEV_LOAD_STAGE_ADLS_TWN'',''dev/SFMC/transaction/tw_crm_children/'',''SDL_TW_SFMC_CHILDREN_DATA'']
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]

        df_schema = StructType([
                StructField("parent_key", StringType(), True),
                StructField("child_nm", StringType(), True),
                StructField("child_birth_mnth", StringType(), True),
                StructField("child_birth_year", StringType(), True),
                StructField("child_gender", StringType(), True),
                StructField("child_number", StringType(), True)
                
            ])


        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "|")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .option("encoding","UTF-16")\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        df = df.na.drop("all")
        if df.count() == 0:
            return "No Data in file"

        df = df.withColumn("crtd_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        df = df.withColumn("filename", lit(file_name))

        snowdf = df.select(
            "parent_key",
            "child_nm",
            "child_birth_mnth",
            "child_birth_year",
            "child_gender",
            "child_number",
            "filename",
            "crtd_dttm"
            )

        snowdf.write.mode("append").saveAsTable(stage_name.split(".")[0] + "." + target_table)

        file_name=file_name.split(".")[0]+"_"+datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")

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



CREATE OR REPLACE PROCEDURE NTASDL_RAW.TW_CRM_CONSUMER_MASTER_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('openpyxl==3.0.10','snowflake-snowpark-python==*','unidecode==1.2.0')
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

        #Param=[''TW_CRM_Consumer_Master_20240429.csv'',''NTASDL_RAW.DEV_LOAD_STAGE_ADLS_TWN'',''dev/SFMC/transaction/tw_crm_consumer_master/'',''SDL_TW_SFMC_consumer_master'']

        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        

        #Reading data from adls
        full_path = "@"+stage_name+"/"+temp_stage_path+"/"+file_name
        
        with SnowflakeFile.open(full_path,''rb'',require_scoped_url= False) as f:
            
            # Reading the csv file 
            df = pd.read_csv(f,encoding=''UTF-16'',delimiter=''|'',header=0)
            #Converting every value into string for uniformity 
            df = df.applymap(lambda x: str(x))
            #df = df.applymap(lambda x:unidecode(x))
            #Converting string nans into proper null value 
            df = df.applymap(lambda x: None if x==''nan'' else x)
            #Transformation necessary to make data  similar to redshift data
            df[''Birthday_Month''] = df[''Birthday_Month''].apply(lambda x:sub(r''\\.[0]'','''',x) if isinstance(x,str) else x)
            df[''Birthday_Month''] = df[''Birthday_Month''].apply(lambda x:''0''+x if isinstance(x,str) and len(x) == 1 else x)
            df[''Birthday_Year''] = df[''Birthday_Year''].apply(lambda x:sub(r''\\.[0]'','''',x) if isinstance(x,str) else x)
            df[''Birthday_Year''] = df[''Birthday_Year''].apply(lambda x:sub(r''\\.[0]'','''',x) if isinstance(x,str) else x)
            df[''Address_Zipcode''] = df[''Address_Zipcode''].apply(lambda x:sub(r''\\.[0]'','''',x) if isinstance(x,str) else x)
            df[''LINE_Channel_ID''] = df[''LINE_Channel_ID''].apply(lambda x:sub(r''\\.[0]'','''',x) if isinstance(x,str) else x)
            df[''Remaining_Points''] = df[''Remaining_Points''].apply(lambda x:x+''.00'' if isinstance(x,str) else x)
            df[''Total_Points''] = df[''Total_Points''].apply(lambda x:x+''0'' if isinstance(x,str) else x)
            df[''Updated_Date''] = pd.to_datetime(df[''Updated_Date''],format="%Y-%m-%d %H:%M:%S")
            df[''filename''] = file_name
            df[''crtd_dttm''] = datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")
            

            
            
            
            
            

            snowparkdf = session.createDataFrame(df)

            
            

        

            
            
            snowparkdf = snowparkdf.na.drop("all")
            if snowparkdf.count() == 0:
                return "No Data in file"

        
        
            snowparkdf.write.mode("append").saveAsTable(stage_name.split(".")[0] + "." + target_table)

            file_name=file_name.split(".")[0]+"_"+datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
            current_date = datetime.now()
            formatted_year = current_date.strftime("%Y")
            formatted_month = current_date.strftime("%m")

            snowparkdf.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)

            return ''Success''

            

            

            

    except KeyError as key_error:
        # Handle KeyError (missing columns) here
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message
        
    except Exception as e:
        # Handle exceptions here
        error_message = f"Error: {str(e)}"
        return error_message;
';
CREATE OR REPLACE PROCEDURE NTASDL_RAW.TW_CRM_INVOICE_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS 'from snowflake.snowpark.functions import col, lit, date_format, current_timestamp, to_date, year, month, concat, format_number, regexp_replace, to_timestamp, when, trim, upper
from snowflake.snowpark.types import StringType, StructType, StructField
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark

import pytz
def main(session: snowpark.Session,Param):
    #Param=["TW_CRM_Invoice_20240430.csv","NTASDL_RAW.DEV_LOAD_STAGE_ADLS_TWN","dev/SFMC/transaction/tw_crm_invoice/","SDL_TW_SFMC_INVOICE_DATA"]
    try:
        file_name = Param[0]
        stage_name = Param[1]
        temp_stage_path = Param[2]
        target_table = Param[3]
        df_schema = StructType([
            StructField("purchase_date", StringType(), True),
            StructField("channel", StringType(), True),
            StructField("product", StringType(), True),
            StructField("status", StringType(), True),
            StructField("created_date", StringType(), True),
            StructField("completed_date", StringType(), True),
            StructField("subscriber_key", StringType(), True),
            StructField("points", StringType(), True),
            StructField("show_record", StringType(), True),
            StructField("qty", StringType(), True),
            StructField("invoice_type", StringType(), True),
            StructField("seller_nm", StringType(), True),
            StructField("product_category", StringType(), True),
            StructField("website_unique_id", StringType(), True),
            StructField("invoice_num", StringType(), True),
            StructField("epsilon_price_per_unit", StringType(), True),
            StructField("epsilon_amount", StringType(), True),
            StructField("epsilon_total_amount", StringType(), True),
        ])
        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "|")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .option("encoding","UTF-16")\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
        df = df.na.drop("all")
        if df.count() == 0:
            return "No Data in file"
        df = df.withColumn("EPSILON_PRICE_PER_UNIT",regexp_replace(lit(col("EPSILON_PRICE_PER_UNIT")),"''",''''))
        df = df.withColumn("EPSILON_AMOUNT",regexp_replace(lit(col("EPSILON_AMOUNT")),"''",''''))
        
        df = df.withColumn("crtd_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        df = df.withColumn("filename", lit(file_name))
        # df = df.withColumn("run_id", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        snowdf = df.select(
           "purchase_date",
           "channel",
           "product",
           "status",
           "created_date",
           "completed_date",
           "subscriber_key",
           "points",
           "show_record",
           "qty",
           "invoice_type",
           "seller_nm",
           "product_category",
           "website_unique_id",
           "invoice_num",
           "epsilon_price_per_unit",
           "epsilon_amount",
           "epsilon_total_amount",
            "filename",
            "crtd_dttm"
            )
        file_name=file_name.split(".")[0]+"_"+ datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        #move to success
        snowdf.write.mode("append").saveAsTable(stage_name.split(".")[0] + "." + target_table)
        snowdf.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)
        return ''Success''
    except KeyError as key_error:
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message
    except pd.errors.MergeError as merge_error:
        error_message = f"DataFrame merging error: {str(merge_error)}"
        return error_message
    except Exception as e:
        error_message = f"Error: {str(e)}"
        return error_message';



CREATE OR REPLACE PROCEDURE NTASDL_RAW.TW_CRM_REDEMPTION_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS 'from snowflake.snowpark.functions import col, lit, date_format, current_timestamp, to_date, year, month, concat, format_number, regexp_replace, to_timestamp, when, trim, upper
from snowflake.snowpark.types import StringType, StructType, StructField
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz
def main(session: snowpark.Session,Param):
    #Param=["TW_CRM_Redemption_20240430.csv","NTASDL_RAW.DEV_LOAD_STAGE_ADLS_TWN","dev/SFMC/transaction/tw_crm_redemption/","SDL_TW_SFMC_redemption_data"]
    try:
        file_name = Param[0]
        stage_name = Param[1]
        temp_stage_path = Param[2]
        target_table = Param[3]
        df_schema = StructType([
            StructField("prod_nm", StringType(), True),
            StructField("redeemed_points", StringType(), True),
            StructField("qty", StringType(), True),
            StructField("redeemed_date", StringType(), True),
            StructField("status", StringType(), True),
            StructField("completed_date", StringType(), True),
            StructField("subscriber_key", StringType(), True),
            StructField("created_date", StringType(), True),
            StructField("order_num", StringType(), True),
            StructField("website_unique_id", StringType(), True),
        ])
        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "|")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .option("encoding","UTF-16")\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
        df = df.na.drop("all")
        if df.count() == 0:
            return "No Data in file"
        df = df.withColumn("crtd_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        df = df.withColumn("filename", lit(file_name))
        # df = df.withColumn("run_id", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        snowdf = df.select(
            "prod_nm",
            "redeemed_points",
            "qty",
            "redeemed_date",
            "status",
            "completed_date",
            "subscriber_key",
            "created_date",
            "order_num",
            "website_unique_id",
            "filename",
            "crtd_dttm"
            )
        file_name=file_name.split(".")[0]+"_"+ datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        #move to success
        snowdf.write.mode("append").saveAsTable(stage_name.split(".")[0] + "." + target_table)
        snowdf.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)
        return ''Success''
    except KeyError as key_error:
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message
    except pd.errors.MergeError as merge_error:
        error_message = f"DataFrame merging error: {str(merge_error)}"
        return error_message
    except Exception as e:
        error_message = f"Error: {str(e)}"
        return error_message';
CREATE OR REPLACE PROCEDURE NTASDL_RAW.TW_POS_711_PREPROCESSING("PARAM" ARRAY)
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
from snowflake.snowpark.functions import col,lit,replace,to_date
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField
import pandas as pd
from datetime import datetime
import pytz
 
def main(session: snowpark.Session,Param):
 
    try:
 
        #Param=[''TW_POS_711_RawData_20201209.csv'',''NTASDL_RAW.DEV_LOAD_STAGE_ADLS_TWN'',''dev/pos/transaction/7-11/'',''sdl_tw_pos_7eleven'']
 
        # Extracting parameters from the input
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
 
 
        # Define the schema for the DataFrame
        df_schema=StructType([
            StructField("POS_DATE",StringType()),
            StructField("PRODUCT_CODE",StringType()),
            StructField("PRODUCT_DESCRIPTION",StringType()),
            StructField("SALES_QTY",StringType()),
            StructField("VENDOR_CODE",StringType()),
            StructField("VENDOR_DESCRIPTION",StringType()),
            ])
 
        # Read the CSV file into a DataFrame
        dataframe = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "\\u0001")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .option("skip_blank_lines", True)\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
 
 
        #---------------------------Transformation logic ------------------------------#
 
        # Handle null values or empty rows
        dataframe=dataframe.na.drop("all")
 
        # Check for empty Dataframe
        if dataframe.count()==0:
            return "No Data in file"
    
        dataframe = dataframe.with_column("UPD_DTTM", lit(None).cast("string"))
        dataframe = dataframe.with_column("CRT_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        # # Creating copy of the Dataframe
        final_df = dataframe.select("POS_DATE", "PRODUCT_CODE", "PRODUCT_DESCRIPTION", "SALES_QTY", "VENDOR_CODE","VENDOR_DESCRIPTION","CRT_DTTM","UPD_DTTM" )
 
        # # Load Data to the target table
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
