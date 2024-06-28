update meta_raw.parameters
set PARAMETER_NAME = 'is_direct_load',PARAMETER_VALUE = 'Y'
where PARAMETER_ID = 12397;


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
    # Param=["Product_performance_weekly_20240401_20240407.csv","NTASDL_RAW.DEV_LOAD_STAGE_ADLS_KOR","dev/dads/transaction/sales_product_performance/","sdl_KR_DADS_Naver_GMV"]
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

        def get_file_name(file_name):
                return file_name.split(''.'')[0]+"_"+datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")+''.csv''

        df = df.with_column("file_name", lit(get_file_name(file_name)))
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
    # Param=["JNJ_2024_05.csv","NTASDL_RAW.DEV_LOAD_STAGE_ADLS_KOR","dev/dads/transaction/naver_keyword/","sdl_KR_DADS_naver_keyword_search_volume"]
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
            filename = file_name[:8] + file_name[9:11]
            return filename.split(''_'')[1]+''01''

        def get_file_name(file_name):
            filename = file_name[:8] + file_name[9:11]
            return filename.split(''.'')[0]+"_"+datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")+''.csv''

        df = df.with_column("file_name", lit(get_file_name(file_name)))
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

        # snowdf.write.mode("append").saveAsTable(stage_name.split(".")[0] + "." + target_table)

        #move to success
        # snowdf.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)
        
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
    # Param=["search_channel_20240527.csv","NTASDL_RAW.DEV_LOAD_STAGE_ADLS_KOR","dev/dads/transaction/naver_search_channel/","sdl_KR_DADS_naver_search_channel"]
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

        

        def get_file_name(file_name):
                return file_name.split(''.'')[0]+"_"+datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")+''.csv''
            

        df = df.with_column("file_name", lit(get_file_name(file_name)))
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


CREATE OR REPLACE PROCEDURE NTASDL_RAW.KR_DADS_NAVER_ADS_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('openpyxl==3.1.2','snowflake-snowpark-python==*','unidecode==1.2.0')
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

        # Param=[''Naver_keyword_20240610_20240616.xlsx'',''NTASDL_RAW.DEV_LOAD_STAGE_ADLS_KOR'',''dev/dads/transaction/naver_keyword weekly/'',''sdl_KR_DADS_linkprice'']

        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        file_header_loc = 10

        #Reading data from adls
        full_path = "@"+stage_name+"/"+temp_stage_path+"/"+file_name
        
        with SnowflakeFile.open(full_path,''rb'',require_scoped_url= False) as f:
            df = pd.read_excel(f)

            df = df.applymap(lambda x:str(x))

            df = df[10:]

            df1 = df[df.columns[2:]] 

            df1.rename(columns = {df1.columns[0] : ''CAMPAIGN_NAME'',
                                 df1.columns[1]:''GROUP_NAME'',
                                  df1.columns[2]:''MATERIAL_ID'',
                                 df1.columns[3]:''PRODUCT_NUMBER'',
                                 df1.columns[4]:''PRODUCT_NAME'',
                                 df1.columns[5]:''IMPRESSISON_AREA'',
                                 df1.columns[6]:''KEYWORD'',
                                 df1.columns[7]:''IMPRESSION'',
                                 df1.columns[8]:''CLICK_COUNT'',
                                 df1.columns[9]:''CTR'',
                                 df1.columns[10]:''IMPRESSION_RANKING'',
                                 df1.columns[11]:''AVG_CLICK_RATE'',
                                 df1.columns[12]:''CONSUMED_COST'',
                                 df1.columns[13]:''CONVERSION_COUNT'',
                                 df1.columns[14]:''CONVERSION_RATE'',
                                 df1.columns[15]:''PURCHASED_AMOUNT'',
                                 df1.columns[16]:''ROAS'',
                                 df1.columns[17]:''PREVIOUS_ROAS''},inplace = True)

            df1[''CONVERSION_RATE''] = df1[''CONVERSION_RATE''].apply(lambda x: float(x) if isinstance(x,str) else x)
            df1[''CONVERSION_RATE''] = df1[''CONVERSION_RATE''].apply(lambda x: x*100 if isinstance(x,float) else x)
            df1[''CONVERSION_RATE''] = df1[''CONVERSION_RATE''].apply(lambda x: str(x)[:5]+''%''.replace(''.'','''') if isinstance(x,float) else x)

            df1[''ROAS''] = df1[''ROAS''].apply(lambda x: float(x) if isinstance(x,str) else x)
            df1[''ROAS''] = df1[''ROAS''].apply(lambda x: x*100 if isinstance(x,float) else x)
            df1[''ROAS''] = df1[''ROAS''].apply(lambda x: str(x)[:4]+''%'' if isinstance(x,float) else x)
            df1[''ROAS''] = df1[''ROAS''].apply(lambda x: x.replace(''.%'',''%''))

            df1[''PREVIOUS_ROAS''] = df1[''PREVIOUS_ROAS''].apply(lambda x:float(x) if isinstance(x,str) else x)
            df1[''PREVIOUS_ROAS''] = df1[''PREVIOUS_ROAS''].apply(lambda x:x*100 if isinstance(x,str) else x)
            df1[''PREVIOUS_ROAS''] = df1[''PREVIOUS_ROAS''].apply(lambda x: str(x) if isinstance(x,float) else x)  
            
            df1[''AVG_CLICK_RATE''] = df1[''AVG_CLICK_RATE''].apply(lambda x: str(int(float(x))))
            df1[''AVG_CLICK_RATE''] = df1[''AVG_CLICK_RATE''].apply(lambda x: x.replace(''0'',''-'') if len(x) == 1 else x)

            df1[''CTR''] = df1[''CTR''].apply(lambda x:str(round(float(x)*100,2))+''%'')
            

            df1 = df1.applymap(lambda x: None if x== ''nan'' else x)
            
            snowparkdf = session.create_dataframe(df1)

           

            def file_date(file_name):
                return file_name.split(''_'')[2]


            def get_file_name(file_name):
                return file_name.split(''.'')[0]+"_"+datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")+''.csv''
                
            snowparkdf = snowparkdf.with_column(''file_name'',lit(get_file_name(file_name)))
            snowparkdf = snowparkdf.with_column(''file_date'',lit(file_date(file_name)))
            snowparkdf = snowparkdf.with_column("crtd_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))

            file_name=file_name.split(".")[0]+"_"+datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
            current_date = datetime.now()
            formatted_year = current_date.strftime("%Y")
            formatted_month = current_date.strftime("%m")

            #move to success
            snowparkdf.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)
    
            snowparkdf.write.mode("append").saveAsTable(stage_name.split(".")[0] + "." + target_table )

            
            return ''Success''

            

    except KeyError as key_error:
        # Handle KeyError (missing columns) here
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message
        
    except Exception as e:
        # Handle exceptions here
        error_message = f"Error: {str(e)}"
        return error_message;';
