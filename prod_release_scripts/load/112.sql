CREATE OR REPLACE PROCEDURE THASDL_RAW.MYM_CUST_SALES_PREFORMAT("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python','pandas','openpyxl')
HANDLER = 'main'
EXECUTE AS CALLER
AS '

import snowflake.snowpark as snowpark
from snowflake.snowpark.files import SnowflakeFile
from snowflake.snowpark.functions import lit
from datetime import datetime
import os,sys
import pandas as pd
import pytz
def main(session: snowpark.Session, Param):
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        sch_name        = stage_name.split(''.'')[0]
        target_table    = sch_name+"."+Param[3]
    
        full_path = "@"+stage_name+"/"+temp_stage_path+"/"+file_name
        savefile  = "Reformatted_"+file_name
        with SnowflakeFile.open(full_path, ''rb'', require_scoped_url = False) as f:
            mym_sales = pd.read_excel(f)
            mym_period = mym_sales.iloc[0, 0]
            mym_cust_group = mym_sales.iloc[1, 0]
            mym_cols = mym_sales.iloc[2].values
            mym_sales = pd.DataFrame(data=mym_sales.iloc[3:, :].values, columns=mym_cols)
            mym_cust_list = mym_sales.loc[(~mym_sales[''Item No.''].isnull()) & (mym_sales[''Description''].isnull())][''Item No.''].values
            mym_cust_list = mym_cust_list[:-1]
            mym_cust_code = [x.split(":")[1] for x in mym_cust_list]
            mym_cust_name = [x.split(":")[2] for x in mym_cust_list]
            mym_sales = mym_sales.dropna()
            mym_sales[''old_index''] = mym_sales.index
        
            increment_value = 0
            cust_code = []
            cust_name = []
            for row_num in range(len(mym_sales)):
                cur_index = mym_sales.iloc[row_num][''old_index'']
        
                if row_num == 0:
                    running_index = cur_index
                    cust_code.append(mym_cust_code[increment_value])
                    cust_name.append(mym_cust_name[increment_value])
                else:
                    if cur_index > running_index:
                        increment_value += 1
                        running_index = cur_index
                        cust_code.append(mym_cust_code[increment_value])
                        cust_name.append(mym_cust_name[increment_value])
                    else:
                        cust_code.append(mym_cust_code[increment_value])
                        cust_name.append(mym_cust_name[increment_value])
                running_index += 1
            mym_sales[''period''] = mym_period
            mym_sales[''customer_group''] = mym_cust_group
            mym_sales[''customer_code''] = cust_code
            mym_sales[''customer_name''] = cust_name
            mym_sales = mym_sales.drop(columns=[''old_index''])
            if ''FOC'' not in mym_sales.columns:
                mym_sales.insert(loc=3, column=''FOC'', value=0)
            mym_sales = mym_sales.reset_index(drop=True)
            final_df = session.create_dataframe(mym_sales)
            final_df = final_df.withColumn("FILE_NAME",lit(file_name).cast("string"))
            final_df = final_df.withColumn("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
            final_df = final_df.withColumn("CRT_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
            final_df.write.mode("append").saveAsTable(target_table)
            current_date = datetime.now()
            formatted_year = current_date.strftime("%Y")
            formatted_month = current_date.strftime("%m")
        
            #move to success
            final_df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)
        return "SUCCESS"
        
    except Exception as e:
        # Handle exceptions here
        error_message = f"ERROR: {str(e)}"
        return error_message
';

CREATE OR REPLACE PROCEDURE THASDL_RAW.MYM_CUST_SALES_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python','pandas','openpyxl')
HANDLER = 'main'
EXECUTE AS OWNER
AS '

import snowflake.snowpark as snowpark
from snowflake.snowpark.files import SnowflakeFile
from snowflake.snowpark.functions import lit
from datetime import datetime
import os,sys
import pandas as pd
import pytz
def main(session: snowpark.Session, Param):
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        sch_name        = stage_name.split(''.'')[0]
        target_table    = sch_name+"."+Param[3]
    
        full_path = "@"+stage_name+"/"+temp_stage_path+"/"+file_name
        savefile  = "Reformatted_"+file_name
        with SnowflakeFile.open(full_path, ''rb'', require_scoped_url = False) as f:
            mym_sales = pd.read_excel(f)
            mym_period = mym_sales.iloc[0, 0]
            mym_cust_group = mym_sales.iloc[1, 0]
            mym_cols = mym_sales.iloc[2].values
            mym_sales = pd.DataFrame(data=mym_sales.iloc[3:, :].values, columns=mym_cols)
            mym_cust_list = mym_sales.loc[(~mym_sales[''Item No.''].isnull()) & (mym_sales[''Description''].isnull())][''Item No.''].values
            mym_cust_list = mym_cust_list[:-1]
            mym_cust_code = [x.split(":")[1] for x in mym_cust_list]
            mym_cust_name = [x.split(":")[2] for x in mym_cust_list]
            mym_sales = mym_sales.dropna()
            mym_sales[''old_index''] = mym_sales.index
        
            increment_value = 0
            cust_code = []
            cust_name = []
            for row_num in range(len(mym_sales)):
                cur_index = mym_sales.iloc[row_num][''old_index'']
        
                if row_num == 0:
                    running_index = cur_index
                    cust_code.append(mym_cust_code[increment_value])
                    cust_name.append(mym_cust_name[increment_value])
                else:
                    if cur_index > running_index:
                        increment_value += 1
                        running_index = cur_index
                        cust_code.append(mym_cust_code[increment_value])
                        cust_name.append(mym_cust_name[increment_value])
                    else:
                        cust_code.append(mym_cust_code[increment_value])
                        cust_name.append(mym_cust_name[increment_value])
                running_index += 1
            mym_sales[''period''] = mym_period
            mym_sales[''customer_group''] = mym_cust_group
            mym_sales[''customer_code''] = cust_code
            mym_sales[''customer_name''] = cust_name
            mym_sales = mym_sales.drop(columns=[''old_index''])
            if ''FOC'' not in mym_sales.columns:
                mym_sales.insert(loc=3, column=''FOC'', value=0)
            mym_sales = mym_sales.reset_index(drop=True)
            final_df = session.create_dataframe(mym_sales)
            final_df = final_df.withColumn("FILE_NAME",lit(file_name).cast("string"))
            final_df = final_df.withColumn("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
            final_df = final_df.withColumn("CRT_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
            final_df.write.mode("append").saveAsTable(target_table)
            current_date = datetime.now()
            formatted_year = current_date.strftime("%Y")
            formatted_month = current_date.strftime("%m")
        
            #move to success
            final_df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)
        return "Success"
        
    except Exception as e:
        # Handle exceptions here
        error_message = f"ERROR: {str(e)}"
        return error_message
';

CREATE OR REPLACE PROCEDURE THASDL_RAW.MYM_INVENTORY_REPORT_PREPROCESSING("PARAM" ARRAY)
RETURNS TABLE ()
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them.

from snowflake.snowpark.functions import col, lit, date_format, current_timestamp, to_date, year, month, concat, format_number, regexp_replace,to_timestamp,when,trim,upper
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DecimalType,DateType
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz


def main(session: snowpark.Session,Param): 
  try:
          file_name       = Param[0]
          stage_name      = Param[1]
          temp_stage_path = Param[2]
          target_table    = Param[3]
      
          df_schema = StructType([
                StructField("item_no", StringType()),
                StructField("item_description", StringType()),
                StructField("qty_sold", StringType()),
                StructField("stock", StringType())
            ])
          df = session.read\\
                    .schema(df_schema)\\
                    .option("skip_header",0)\\
                    .option("field_delimiter", "\\u0001")\\
                    .option("field_optionally_enclosed_by", "\\"") \\
                    .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
          
          df= df.with_column("run_id", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S"))) 
          df=df.with_column("file_name",lit(file_name))
          df=df.with_column("crt_dttm",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))

          snowdf=df.select(  "item_no",
                            "item_description",
                            "qty_sold",
                            "stock",
                            "file_name",
                            "run_id",
                            "crt_dttm")
            
          snowdf= snowdf.filter(snowdf["item_no"].isNotNull())
    
          if snowdf.count()==0 :
                return "No Data in table"
                
            
            #move file into success folder
          file_name=file_name.split(".")[0]+"_"+datetime.now().strftime("%Y%m%d%H%M%S")
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

CREATE OR REPLACE PROCEDURE THASDL_RAW.MYM_SALES_REPORT_PREPROCESSING("PARAM" ARRAY)
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
    #Param = ["Reformatted_Customer_Sale_Report_03.02.2024.csv", "THASDL_RAW.DEV_LOAD_STAGE_ADLS", "dev/cert_data_lake/LCM/LCM_myanmar_Sales_Data", "SDL_MYM_GT_SALES_REPORT_FACT"]
    try:
            file_name       = Param[0]
            stage_name      = Param[1]
            temp_stage_path = Param[2]
            target_table    = Param[3]
        
            df_schema = StructType([
                        StructField("item_no", StringType()),
                        StructField("description", StringType()),
                        StructField("qty_sold", StringType()),
                        StructField("foc_qty", StringType()),
                        StructField("Total", StringType()),
                        StructField("period", StringType()),
                        StructField("customer_group", StringType()),
                        StructField("customer_code", StringType()),
                        StructField("customer_name", StringType())
                    ])
            df = session.read\\
                .schema(df_schema)\\
                .option("skip_header",0)\\
                .option("field_delimiter", "\\u0001")\\
                .option("field_optionally_enclosed_by", "\\"") \\
                .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

            df = df.with_column("run_id", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S"))) 
            df=df.with_column("file_name",lit(file_name))
            df=df.with_column("crt_dttm",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        
            snowdf=df.select( "item_no", "description", "qty_sold", "foc_qty", "Total", "period",
                                "customer_group", "customer_code", "customer_name", "file_name", "run_id", "crt_dttm")
            
            snowdf= snowdf.filter(snowdf["item_no"].isNotNull())
    
            if snowdf.count()==0 :
                return "No Data in table"
                
            
            #move file into success folder
            file_name=file_name.split(".")[0]+"_"+datetime.now().strftime("%Y%m%d%H%M%S")
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

CREATE OR REPLACE PROCEDURE THASDL_RAW.MYM_SALES_STOCK_REPORT_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '
from snowflake.snowpark.functions import col, lit, date_format, current_timestamp, to_date, year, month, concat, format_number, regexp_replace,to_timestamp,when,trim,upper
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DecimalType,DateType
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz

def main(session: snowpark.Session,Param): 
    #Param = ["Reformatted_Customer_Sale_Report_03.02.2024.csv", "THASDL_RAW.DEV_LOAD_STAGE_ADLS", "dev/cert_data_lake/LCM/LCM_myanmar_Sales_Data", "SDL_MYM_GT_SALES_REPORT_FACT"]
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        sch_name        = stage_name.split(''.'')[0]
        target_table    = sch_name+"."+Param[3]
    
        df_schema = StructType([
                    StructField("item_no", StringType()),
                    StructField("description", StringType()),
                    StructField("qty_sold", StringType()),
                    StructField("stock", StringType())
                ])
        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "\\u0001")\\
            .option("field_optionally_enclosed_by", "\\"") \\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        df = df.withColumn("FILE_NAME",lit(file_name).cast("string"))
        df = df.withColumn("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        df = df.withColumn("CRT_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
                
        final_df= df.select( "item_no", "description", "qty_sold", "stock", "FILE_NAME", "RUN_ID", "CRT_DTTM")
        
        final_dffinal_df= final_df.filter(final_df["item_no"].isNotNull())

        if final_df.count()==0 :
            return "No Data in table"
            
        
        #move file into success folder
        file_name=file_name.split(".")[0]+"_"+datetime.now().strftime("%Y%m%d%H%M%S")
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        
        #move to success
        final_df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)

        #write on sdl layer
    
        final_df.write.mode("append").saveAsTable(target_table)
        
        return "Success"
    except KeyError as key_error:
        # Handle KeyError (missing columns) here
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message
        
    except Exception as e:
        # Handle exceptions here
        error_message = f"Error: {str(e)}" +str(df.columns)
        return error_message
        
';
