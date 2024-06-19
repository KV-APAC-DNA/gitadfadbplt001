CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.INDSDL_RAW.POS_VMM_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS 'from snowflake.snowpark.functions import col, lit, current_timestamp, to_date, year, month, concat, format_number, regexp_replace, to_timestamp, when, trim, upper
from snowflake.snowpark.types import StringType, StructType, StructField
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz
def main(session: snowpark.Session,Param):
    # Param=["Offtake_VMM_20240419.csv","INDSDL_RAW.DEV_LOAD_STAGE_ADLS","dev/key_account_offtake/transaction/vmm","SDL_POS_VMM"]
    try:
        file_name = Param[0]
        stage_name = Param[1]
        temp_stage_path = Param[2]
        target_table = Param[3]
        df_schema = StructType([
            StructField("Account_Name", StringType(), True),
            StructField("Level", StringType(), True),     
            StructField("Period", StringType(), True),
            StructField("Store_Code", StringType(), True),
            StructField("Subcategory", StringType(), True),
            StructField("Article_Code", StringType(), True),
            StructField("Sales_Qty", StringType(), True),
            StructField("Sales_Value_Rs", StringType(), True),
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
            
        # Replace empty values in specified columns with 0
        df = df.fillna({''Sales_Qty'': ''0'', ''Sales_Value_Rs'': ''0''})

        def filename_change(filename):
                filename1 = filename.split(''.'')
                filename1[1]=''xlsx''
                newfilename = ''.''.join(filename1)
                return newfilename

        df = df.withColumn(''filename'',lit(filename_change(file_name)))
        df = df.withColumn("crtd_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        df = df.withColumn("run_id", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        df = df.withColumn("upload_date", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d")))
 
        def replacewith15(col):
            dated = col.split(''-'')
            dated[2] = ''15''
            new_date = ''-''.join(dated)
            return new_date
 
        df_pandas = df.to_pandas()
        print(df_pandas[''PERIOD''])
        df_pandas[''PERIOD''] = df_pandas[''PERIOD''].apply(lambda x: replacewith15(x) if isinstance(x,str) else str(x))
        print(df_pandas)
        df = session.create_dataframe(df_pandas)

        snowdf = df.select(
            "Account_Name",     
            "Period", 
            "Store_Code",
            "Article_Code",
            "Subcategory", 
            "Level",
            "Sales_Qty", 
            "Sales_Value_Rs",  
            "filename",
            "run_id",
            "upload_date",
            "crtd_dttm"
        )
        file_name=file_name.split(".")[0]+"_"+ datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        
        # Move to success
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

CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.INDSDL_RAW.POS_RIL_PREPROCESSING("PARAM" ARRAY)
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
    # Param=["Offtake_RIL_20240408.csv","INDSDL_RAW.DEV_LOAD_STAGE_ADLS","dev/key_account_offtake/transaction/ril","SDL_POS_RIL"]
    try:
        file_name = Param[0]
        stage_name = Param[1]
        temp_stage_path = Param[2]
        target_table = Param[3]
        
        df_schema = StructType([
            StructField("Account_Name", StringType(), True),
            StructField("Level", StringType(), True),     
            StructField("Period", StringType(), True),
            StructField("Store_Code", StringType(), True),
            StructField("Article_Code", StringType(), True),
            StructField("Sales_Qty", StringType(), True),
            StructField("Sales_Value_Rs", StringType(), True),
             StructField("Subcategory", StringType(), True),
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
            

        # Replace empty values in specified columns with 0
        df = df.fillna({''Sales_Qty'': ''0'', ''Sales_Value_Rs'': ''0''})
        
        def filename_change(filename):
                filename1 = filename.split(''.'')
                filename1[1]=''xlsx''
                newfilename = ''.''.join(filename1)
                return newfilename
        df = df.withColumn(''filename'',lit(filename_change(file_name)))    
        df = df.withColumn("crtd_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        df = df.withColumn("run_id", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        df = df.withColumn("upload_date", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d")))

        def replacewith15(col):
            dated = col.split(''-'')
            dated[2] = ''15''
            new_date = ''-''.join(dated)
            return new_date
 
        df_pandas = df.to_pandas()
        print(df_pandas[''PERIOD''])
        df_pandas[''PERIOD''] = df_pandas[''PERIOD''].apply(lambda x: replacewith15(x) if isinstance(x,str) else str(x))
        print(df_pandas)
        df = session.create_dataframe(df_pandas)

        snowdf = df.select(
            "Account_Name",     
            "Period", 
            "Store_Code",
            "Article_Code",
            "Subcategory", 
            "Level",
            "Sales_Qty", 
            "Sales_Value_Rs",  
            "filename",
            "run_id",
            "upload_date",
            "crtd_dttm"
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


CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.INDSDL_RAW.POS_ABRL_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS 'from snowflake.snowpark.functions import col, lit, date_format, current_timestamp, to_date, year, month, concat, format_number, regexp_replace, to_timestamp, trim, upper,concat_ws
from snowflake.snowpark.types import StringType, StructType, StructField
import pandas as pd
from datetime import datetime
import calendar
import snowflake.snowpark as snowpark
import pytz

def main(session: snowpark.Session,Param):
    # Param=["Offtake_ABRL_20240202.csv","INDSDL_RAW.DEV_LOAD_STAGE_ADLS","dev/key_account_offtake/transaction/abrl","SDL_POS_ABRL"]
    try:
        file_name = Param[0]
        stage_name = Param[1]
        temp_stage_path = Param[2]
        target_table = Param[3]
        
        df_schema = StructType([
            StructField("Account_Name", StringType(), True),
            StructField("Year", StringType(), True),     
            StructField("Month", StringType(), True),
            StructField("Store_Code", StringType(), True),
            StructField("Article_Code", StringType(), True),
            StructField("Subcategory", StringType(), True),
            StructField("Sales_Qty", StringType(), True),
            StructField("Sales_Value_Rs", StringType(), True),
            StructField("Level", StringType(), True),
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
            

        # Replace empty values in specified columns with 0
        df = df.fillna({''Sales_Qty'': ''0'', ''Sales_Value_Rs'': ''0''})

        def get_month_num(date):
            dic_month = {}
            for i in range(1,13):
                dic_month[calendar.month_abbr[i]] = i
            return dic_month[date]
            
        df_pandas = df.to_pandas()
        df_pandas[''MONTH''] = df_pandas[''MONTH''].apply(lambda x: get_month_num(x))
        df = session.createDataFrame(df_pandas)

        def filename_change(filename):
                filename1 = filename.split(''.'')
                filename1[1]=''xlsx''
                newfilename = ''.''.join(filename1)
                return newfilename
        
        df = df.withColumn(''filename'',lit(filename_change(file_name)))
        df = df.withColumn("crtd_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        df = df.withColumn("run_id", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        df = df.withColumn("upload_date", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d")))
        df = df.withColumn(''Period'',concat_ws(lit(''-''),col("year"),col(''month''),lit(''15'')))

        snowdf = df.select(
            "Account_Name",     
            "Period", 
            "Store_Code",
            "Article_Code",
            "Subcategory", 
            "Level",
            "Sales_Qty", 
            "Sales_Value_Rs",  
            "filename",
            "run_id",
            "upload_date",
            "crtd_dttm"
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

CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.INDSDL_RAW.POS_APOLLO_PREPROCESSING("PARAM" ARRAY)
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
    # Param=["Offtake_APOLLO_20240415.csv","INDSDL_RAW.DEV_LOAD_STAGE_ADLS","dev/key_account_offtake/transaction/apollo","SDL_POS_APOLLO"]
    try:
        file_name = Param[0]
        stage_name = Param[1]
        temp_stage_path = Param[2]
        target_table = Param[3]
        
        df_schema = StructType([
            StructField("Account_Name", StringType(), True),
            StructField("Level", StringType(), True),
            StructField("Store_Code", StringType(), True),
            StructField("Article_Code", StringType(), True),
            StructField("Subcategory", StringType(), True),
            StructField("Period", StringType(), True),
            StructField("Sales_Qty", StringType(), True),
            StructField("Sales_Value_Rs", StringType(), True),
             
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
            

        # Replace empty values in specified columns with 0
        df = df.fillna({''Sales_Qty'': ''0'', ''Sales_Value_Rs'': ''0''})
        
        def filename_change(filename):
                filename1 = filename.split(''.'')
                filename1[1]=''xlsx''
                newfilename = ''.''.join(filename1)
                return newfilename
            
        df = df.withColumn(''filename'',lit(filename_change(file_name)))    
        df = df.withColumn("crtd_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        df = df.withColumn("run_id", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        df = df.withColumn("upload_date", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d")))

        def replacewith15(col):
            dated = col.split(''-'')
            dated[2] = ''15''
            new_date = ''-''.join(dated)
            return new_date
 
        df_pandas = df.to_pandas()
        print(df_pandas[''PERIOD''])
        df_pandas[''PERIOD''] = df_pandas[''PERIOD''].apply(lambda x: replacewith15(x) if isinstance(x,str) else str(x))
        print(df_pandas)
        df = session.create_dataframe(df_pandas)

        snowdf = df.select(
            "Account_Name",     
            "Period", 
            "Store_Code",
            "Article_Code",
            "Subcategory", 
            "Level",
            "Sales_Qty", 
            "Sales_Value_Rs",  
            "filename",
            "run_id",
            "upload_date",
            "crtd_dttm"
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


CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.INDSDL_RAW.POS_TESCO_PREPROCESSING("PARAM" ARRAY)
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
    # Param=["Offtake_TESCO_20240320.csv","INDSDL_RAW.DEV_LOAD_STAGE_ADLS","dev/key_account_offtake/transaction/tesco","SDL_POS_TESCO"]
    try:
        file_name = Param[0]
        stage_name = Param[1]
        temp_stage_path = Param[2]
        target_table = Param[3]
        
        df_schema = StructType([
            StructField("key_account_name", StringType(), True),
            StructField("pos_dt", StringType(), True),
            StructField("store_code", StringType(), True),
            StructField("article_code", StringType(), True),
            StructField("subcategory", StringType(), True),
            StructField("level", StringType(), True),
            StructField("sls_qty", StringType(), True),
            StructField("sls_val_lcy", StringType(), True),
    
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
            

        # Replace empty values in specified columns with 0
        df = df.fillna({''sls_qty'': ''0'', ''sls_val_lcy'': ''0''})

        def filename_change(filename):
                filename1 = filename.split(''.'')
                filename1[1]=''xlsx''
                newfilename = ''.''.join(filename1)
                return newfilename
        
        df = df.withColumn(''filename'',lit(filename_change(file_name)))
        df = df.withColumn("crtd_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        df = df.withColumn("run_id", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        df = df.withColumn("upload_date", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d")))

        def replacewith15(col):
            dated = col.split(''-'')
            dated[2] = ''15''
            new_date = ''-''.join(dated)
            return new_date
 
        df_pandas = df.to_pandas()
        print(df_pandas[''POS_DT''])
        df_pandas[''POS_DT''] = df_pandas[''POS_DT''].apply(lambda x: replacewith15(x) if isinstance(x,str) else str(x))
        print(df_pandas)
        df = session.create_dataframe(df_pandas)

        snowdf = df.select(
            "key_account_name", 
            "pos_dt", 
            "store_code", 
            "article_code", 
            "subcategory", 
            "level", 
            "sls_qty", 
            "sls_val_lcy",  
            "filename",
            "run_id",
            "upload_date",
            "crtd_dttm"
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

CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.INDSDL_RAW.POS_RE_MAPPING_PREPROCESSING("PARAM" ARRAY)
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
    # Param=["RE_Mapping_20231016.csv","INDSDL_RAW.DEV_LOAD_STAGE_ADLS","dev/key_account_offtake/master/re_mapping","SDL_POS_RE_MAPPING"]
    try:
        file_name = Param[0]
        stage_name = Param[1]
        temp_stage_path = Param[2]
        target_table = Param[3]
        
        df_schema = StructType([
            StructField("store_cd", StringType(), True),
            StructField("account_name", StringType(), True),     
            StructField("store_name", StringType(), True),
            StructField("region", StringType(), True),
            StructField("zone", StringType(), True),
            StructField("re", StringType(), True),
            StructField("promotor", StringType(), True),
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
            
        def filename_change(filename):
                filename1 = filename.split(''.'')
                filename1[1]=''xlsx''
                newfilename = ''.''.join(filename1)
                return newfilename

        df = df.withColumn(''filename'',lit(filename_change(file_name)))  
        df = df.withColumn("crtd_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        df = df.withColumn("run_id", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))

        snowdf = df.select(
            "store_cd",
            "account_name",   
            "store_name",
            "region",
            "zone",
            "re",
            "promotor",  
            "filename",
            "run_id",
            "crtd_dttm"
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


CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.INDSDL_RAW.POS_CATEGORY_MAPPING_PREPROCESSING("PARAM" ARRAY)
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
    # Param=["Category_Mapping_20231208.csv","INDSDL_RAW.DEV_LOAD_STAGE_ADLS","dev/key_account_offtake/master/category_mapping","SDL_POS_CATEGORY_MAPPING"]
    try:
        file_name = Param[0]
        stage_name = Param[1]
        temp_stage_path = Param[2]
        target_table = Param[3]
        
        df_schema = StructType([
            StructField("account_name", StringType(), True),
            StructField("article_cd", StringType(), True),     
            StructField("article_desc", StringType(), True),
            StructField("ean", StringType(), True),
            StructField("sap_cd", StringType(), True),
            StructField("mother_sku_name", StringType(), True),
            StructField("brand_name", StringType(), True),
            StructField("franchise_name", StringType(), True),
            StructField("product_category_name", StringType(), True),     
            StructField("variant_name", StringType(), True),
            StructField("product_name", StringType(), True),
            StructField("internal_category", StringType(), True),
            StructField("internal_sub_category", StringType(), True),
            StructField("external_category", StringType(), True),
            StructField("external_sub_category", StringType(), True),
        ])
        
        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",2)\\
            .option("field_delimiter", "")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
        
        df = df.na.drop("all")
        
        if df.count() == 0:
            return "No Data in file"
            
        def filename_change(filename):
                filename1 = filename.split(''.'')
                filename1[1]=''xlsx''
                newfilename = ''.''.join(filename1)
                return newfilename

        df = df.withColumn(''filename'',lit(filename_change(file_name)))  
        df = df.withColumn("crtd_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        df = df.withColumn("run_id", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))

        snowdf = df.select(
            "account_name",
            "article_cd",  
            "article_desc",
            "ean", 
            "sap_cd", 
            "mother_sku_name",
            "brand_name",
            "franchise_name",
            "product_category_name",     
            "variant_name",
            "product_name", 
            "internal_category", 
            "internal_sub_category", 
            "external_category",
            "external_sub_category",   
            "filename",
            "run_id",
            "crtd_dttm"
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


CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.INDSDL_RAW.POS_HISTORICAL_BTL_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS 'from snowflake.snowpark.functions import col, lit, date_format, current_timestamp, to_date, year, month, concat, format_number, regexp_replace, to_timestamp, when, trim, upper,concat_ws,month
from snowflake.snowpark.types import StringType, StructType, StructField
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz
import calendar
def main(session: snowpark.Session,Param):
    # Param=["Historical_BTLs_20210426.csv","INDSDL_RAW.DEV_LOAD_STAGE_ADLS","dev/key_account_offtake/transaction/historical_btl","SDL_POS_HISTORICAL_BTL"]
    try:
        file_name = Param[0]
        stage_name = Param[1]
        temp_stage_path = Param[2]
        target_table = Param[3]
        
        df_schema = StructType([
            StructField("mother_sku_name", StringType(), True),
            StructField("account_name", StringType(), True),     
            StructField("re", StringType(), True),
            StructField("year", StringType(), True),
            StructField("month", StringType(), True),
            StructField("promos", StringType(), True),
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
 
        def get_month_num(date):
            dic_month = {}
            for i in range(1,13):
                dic_month[calendar.month_abbr[i]] = i
            return dic_month[date]
            
        # Replace empty values in specified columns with 0
        df = df.fillna({''promos'': ''0''})
        
        def filename_change(filename):
                filename1 = filename.split(''.'')
                filename1[1]=''xlsx''
                newfilename = ''.''.join(filename1)
                return newfilename
        df_pandas = df.to_pandas()
 
        df_pandas[''MONTH''] = df_pandas[''MONTH''].apply(lambda x: get_month_num(x))
 
        df = session.createDataFrame(df_pandas)
        df = df.withColumn(''filename'',lit(filename_change(file_name)))  
        df = df.withColumn("crtd_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        df = df.withColumn("run_id", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        df = df.withColumn(''pos_dt'',concat_ws(lit(''-''),col("year"),col(''month''),lit(''15'')))       


        snowdf = df.select(
            "mother_sku_name", 
            "account_name",      
            "re", 
            "pos_dt",
            "promos",   
            "filename",
            "run_id",
            "crtd_dttm"
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

CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.INDSDL_RAW.POS_DMART_PREPROCESSING("PARAM" ARRAY)
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
    # Param=["Offtake_Dmart_20240408.csv","INDSDL_RAW.DEV_LOAD_STAGE_ADLS","dev/key_account_offtake/transaction/dmart","SDL_POS_DMART"]
    try:
        file_name = Param[0]
        stage_name = Param[1]
        temp_stage_path = Param[2]
        target_table = Param[3]
        
        df_schema = StructType([
            StructField("Account_Name", StringType(), True),
            StructField("Level", StringType(), True),
            StructField("Store_Code", StringType(), True),
            StructField("Article_Code", StringType(), True),
            StructField("Subcategory", StringType(), True),
            StructField("Period", StringType(), True),
            StructField("Sales_Qty", StringType(), True),
            StructField("Sales_Value_Rs", StringType(), True),
             
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
            

        # Replace empty values in specified columns with 0
        df = df.fillna({''Sales_Qty'': ''0'', ''Sales_Value_Rs'': ''0''})
        
        def filename_change(filename):
                filename1 = filename.split(''.'')
                filename1[1]=''xlsx''
                newfilename = ''.''.join(filename1)
                return newfilename
            
        df = df.withColumn(''filename'',lit(filename_change(file_name)))    
        df = df.withColumn("crtd_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        df = df.withColumn("run_id", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        df = df.withColumn("upload_date", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d")))

        def replacewith15(col):
            dated = col.split(''-'')
            dated[2] = ''15''
            new_date = ''-''.join(dated)
            return new_date
 
        df_pandas = df.to_pandas()
        print(df_pandas[''PERIOD''])
        df_pandas[''PERIOD''] = df_pandas[''PERIOD''].apply(lambda x: replacewith15(x) if isinstance(x,str) else str(x))
        print(df_pandas)
        df = session.create_dataframe(df_pandas)

        snowdf = df.select(
            "Account_Name",     
            "Period", 
            "Store_Code",
            "Article_Code",
            "Subcategory", 
            "Level",
            "Sales_Qty", 
            "Sales_Value_Rs",  
            "filename",
            "run_id",
            "upload_date",
            "crtd_dttm"
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

CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.INDSDL_RAW.POS_FRL_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS 'from snowflake.snowpark.functions import col, lit, date_format, current_timestamp, to_date, year, month, concat, format_number, regexp_replace, to_timestamp, trim, upper,concat_ws
from snowflake.snowpark.types import StringType, StructType, StructField
import pandas as pd
from datetime import datetime
import calendar
import snowflake.snowpark as snowpark
import pytz

def main(session: snowpark.Session,Param):
    # Param=["Offtake_FRL_20200630.csv","INDSDL_RAW.DEV_LOAD_STAGE_ADLS","dev/key_account_offtake/transaction/frl","SDL_POS_FRL"]
    try:
        file_name = Param[0]
        stage_name = Param[1]
        temp_stage_path = Param[2]
        target_table = Param[3]
        
        df_schema = StructType([
            StructField("Account_Name", StringType(), True),
            StructField("Year", StringType(), True),     
            StructField("Month", StringType(), True),
            StructField("Store_Code", StringType(), True),
            StructField("Subcategory", StringType(), True),
            StructField("Level", StringType(), True),
            StructField("Article_Code", StringType(), True),
            StructField("Sales_Qty", StringType(), True),
            StructField("Sales_Value_Rs", StringType(), True),
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
            

        # Replace empty values in specified columns with 0
        df = df.fillna({''Sales_Qty'': ''0'', ''Sales_Value_Rs'': ''0''})

        def get_month_num(date):
            dic_month = {}
            for i in range(1,13):
                dic_month[calendar.month_name[i]] = i
            return dic_month[date]
            
        df_pandas = df.to_pandas()
        df_pandas[''MONTH''] = df_pandas[''MONTH''].apply(lambda x: get_month_num(x))
        df = session.createDataFrame(df_pandas)

        def filename_change(filename):
                filename1 = filename.split(''.'')
                filename1[1]=''xlsx''
                newfilename = ''.''.join(filename1)
                return newfilename
        
        df = df.withColumn(''filename'',lit(filename_change(file_name)))
        df = df.withColumn("crtd_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        df = df.withColumn("run_id", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        df = df.withColumn("upload_date", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d")))
        df = df.withColumn(''Period'',concat_ws(lit(''-''),col("year"),col(''month''),lit(''15'')))

        snowdf = df.select(
            "Account_Name",     
            "Period", 
            "Store_Code",
            "Article_Code",
            "Subcategory", 
            "Level",
            "Sales_Qty", 
            "Sales_Value_Rs",  
            "filename",
            "run_id",
            "upload_date",
            "crtd_dttm"
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


CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.INDSDL_RAW.POS_DABUR_PREPROCESSING("PARAM" ARRAY)
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
    # Param=["Offtake_Dabur_20240424.csv","INDSDL_RAW.DEV_LOAD_STAGE_ADLS","dev/key_account_offtake/transaction/dabur","SDL_POS_DABUR"]
    try:
        file_name = Param[0]
        stage_name = Param[1]
        temp_stage_path = Param[2]
        target_table = Param[3]
        
        df_schema = StructType([
            StructField("Account_Name", StringType(), True),
            StructField("Level", StringType(), True),
            StructField("Period", StringType(), True),
            StructField("Store_Code", StringType(), True),
            StructField("Article_Code", StringType(), True),
            StructField("Subcategory", StringType(), True),
            StructField("Sales_Qty", StringType(), True),
            StructField("Sales_Value_Rs", StringType(), True),
             
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
            

        # Replace empty values in specified columns with 0
        df = df.fillna({''Sales_Qty'': ''0'', ''Sales_Value_Rs'': ''0''})
        
        def filename_change(filename):
                filename1 = filename.split(''.'')
                filename1[1]=''xlsx''
                newfilename = ''.''.join(filename1)
                return newfilename
            
        df = df.withColumn(''filename'',lit(filename_change(file_name)))    
        df = df.withColumn("crtd_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        df = df.withColumn("run_id", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        df = df.withColumn("upload_date", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d")))

        def replacewith15(col):
            dated = col.split(''-'')
            dated[2] = ''15''
            new_date = ''-''.join(dated)
            return new_date
 
        df_pandas = df.to_pandas()
        print(df_pandas[''PERIOD''])
        df_pandas[''PERIOD''] = df_pandas[''PERIOD''].apply(lambda x: replacewith15(x) if isinstance(x,str) else str(x))
        print(df_pandas)
        df = session.create_dataframe(df_pandas)

        snowdf = df.select(
            "Account_Name",     
            "Period", 
            "Store_Code",
            "Article_Code",
            "Subcategory", 
            "Level",
            "Sales_Qty", 
            "Sales_Value_Rs",  
            "filename",
            "run_id",
            "upload_date",
            "crtd_dttm"
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

CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.INDSDL_RAW.POS_SPENCER_PREPROCESSING("PARAM" ARRAY)
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
    # Param=["Offtake_SPENCER_20240314.csv","INDSDL_RAW.DEV_LOAD_STAGE_ADLS","dev/key_account_offtake/transaction/spencer","SDL_POS_SPENCER"]
    try:
        file_name = Param[0]
        stage_name = Param[1]
        temp_stage_path = Param[2]
        target_table = Param[3]
        
        df_schema = StructType([
            StructField("key_account_name", StringType(), True),
            StructField("level", StringType(), True),
            StructField("pos_dt", StringType(), True),
            StructField("store_code", StringType(), True),
            StructField("subcategory", StringType(), True),
            StructField("article_code", StringType(), True),
            StructField("sls_qty", StringType(), True),
            StructField("sls_val_lcy", StringType(), True),
    
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
            

        # Replace empty values in specified columns with 0
        df = df.fillna({''sls_qty'': ''0'', ''sls_val_lcy'': ''0''})

        def filename_change(filename):
                filename1 = filename.split(''.'')
                filename1[1]=''xlsx''
                newfilename = ''.''.join(filename1)
                return newfilename
        df = df.withColumn(''filename'',lit(filename_change(file_name)))
        df = df.withColumn("crtd_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        df = df.withColumn("run_id", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        df = df.withColumn("upload_date", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d")))

        def replacewith15(col):
            dated = col.split(''-'')
            dated[2] = ''15''
            new_date = ''-''.join(dated)
            return new_date
 
        df_pandas = df.to_pandas()
        print(df_pandas[''POS_DT''])
        df_pandas[''POS_DT''] = df_pandas[''POS_DT''].apply(lambda x: replacewith15(x) if isinstance(x,str) else str(x))
        print(df_pandas)
        df = session.create_dataframe(df_pandas)
        
        snowdf = df.select(
            "key_account_name", 
            "pos_dt", 
            "store_code", 
            "article_code", 
            "subcategory", 
            "level", 
            "sls_qty", 
            "sls_val_lcy",  
            "filename",
            "run_id",
            "upload_date",
            "crtd_dttm"
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


CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.INDSDL_RAW.CSL_DAILYSALES_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS 'from snowflake.snowpark.functions import lit
from snowflake.snowpark.types import StringType, StructType, StructField
from datetime import datetime
import pandas as pd
import snowflake.snowpark as snowpark
import pytz

def main(session: snowpark.Session,Param):
    # Param = ["dailysales20240523061554.csv","INDSDL_RAW.DEV_LOAD_STAGE_ADLS","dev/india_xdm/transaction/dailysales","SDL_CSL_DAILYSALES"]

    try:
        file_name = Param[0]
        stage_name = Param[1]
        temp_stage_path = Param[2]
        target_table = Param[3]

        # Define schema
        df_schema = StructType([
            StructField("distcode", StringType(), True),
            StructField("salinvno", StringType(), True),
            StructField("salinvdate", StringType(), True),
            StructField("saldlvdate", StringType(), True),
            StructField("salgrossamt", StringType(), True),
            StructField("salspldiscamt", StringType(), True),
            StructField("salschdiscamt", StringType(), True),
            StructField("salcashdiscamt", StringType(), True),
            StructField("saldbdiscamt", StringType(), True),
            StructField("saltaxamt", StringType(), True),
            StructField("salwdsamt", StringType(), True),
            StructField("saldbadjamt", StringType(), True),
            StructField("salcradjamt", StringType(), True),
            StructField("salonaccountamt", StringType(), True),
            StructField("salmktretamt", StringType(), True),
            StructField("salreplaceamt", StringType(), True),
            StructField("salotherchargesamt", StringType(), True),
            StructField("salinvleveldiscamt", StringType(), True),
            StructField("saltotdedn", StringType(), True),
            StructField("saltotaddn", StringType(), True),
            StructField("salroundoffamt", StringType(), True),
            StructField("salnetamt", StringType(), True),
            StructField("lcncode", StringType(), True),
            StructField("salesmancode", StringType(), True),
            StructField("salesmanname", StringType(), True),
            StructField("salesroutecode", StringType(), True),
            StructField("salesroutename", StringType(), True),
            StructField("rtrcode", StringType(), True),
            StructField("rtrname", StringType(), True),
            StructField("deliveryroutecode", StringType(), True),
            StructField("deliveryroutename", StringType(), True),
            StructField("prdcode", StringType(), True),
            StructField("prdbatcde", StringType(), True),
            StructField("prdqty", StringType(), True),
            StructField("prdselratebeforetax", StringType(), True),
            StructField("prdselrateaftertax", StringType(), True),
            StructField("prdfreeqty", StringType(), True),
            StructField("prdgrossamt", StringType(), True),
            StructField("prdspldiscamt", StringType(), True),
            StructField("prdschdiscamt", StringType(), True),
            StructField("prdcashdiscamt", StringType(), True),
            StructField("prddbdiscamt", StringType(), True),
            StructField("prdtaxamt", StringType(), True),
            StructField("prdnetamt", StringType(), True),
            StructField("createddate", StringType(), True),
            StructField("mrp", StringType(), True),
            StructField("salfreeqtyvalue", StringType(), True),
            StructField("nrvalue", StringType(), True),
            StructField("vcpschemeamount", StringType(), True),
            StructField("rtrurccode", StringType(), True),
            StructField("syncid", StringType(), True),
            StructField("creditnoteamt", StringType(), True),
            StructField("modifieddate", StringType(), True),
            StructField("salinvmode", StringType(), True),
            StructField("salinvtype", StringType(), True),
            StructField("vechname", StringType(), True),
            StructField("dlvboyname", StringType(), True),
            StructField("createduserid", StringType(), True),
            StructField("salinvlinecount", StringType(), True),
            StructField("mrpcs", StringType(), True),
            StructField("lpvalue", StringType(), True),
            StructField("createddt", StringType(), True)
        ])

        # Read the CSV file into a Snowflake DataFrame
        df = session.read \\
            .schema(df_schema) \\
            .option("skip_header", 1) \\
            .option("field_delimiter", "") \\
            .option("field_optionally_enclosed_by", "\\"") \\
            .csv(f"@{stage_name}/{temp_stage_path}/{file_name}")

        df = df.na.drop("all")

        if df.count() == 0:
            return "No Data in file"

        def removecsv(file_name):
            return file_name.split(''.'')[0] 

        df = df.withColumn("lcnid", lit(None)).withColumn("rtrid", lit(None)).withColumn("uploadflag", lit(None)).withColumn("migrationflag", lit(None))

        df = df.withColumn(''filename'',lit(removecsv(file_name)))  
        df = df.withColumn("crtd_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        df = df.withColumn("run_id", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))


        snowdf = df.select(
            "distcode",
            "salinvno",
            "salinvdate",
            "saldlvdate",
            "salinvmode",
            "salinvtype",
            "salgrossamt",
            "salspldiscamt",
            "salschdiscamt",
            "salcashdiscamt",
            "saldbdiscamt",
            "saltaxamt",
            "salwdsamt",
            "saldbadjamt",
            "salcradjamt",
            "salonaccountamt",
            "salmktretamt",
            "salreplaceamt",
            "salotherchargesamt",
            "salinvleveldiscamt",
            "saltotdedn",
            "saltotaddn",
            "salroundoffamt",
            "salnetamt",
            "lcnid",
            "lcncode",
            "salesmancode",
            "salesmanname",
            "salesroutecode",
            "salesroutename",
            "rtrid",
            "rtrcode",
            "rtrname",
            "vechname",
            "dlvboyname",
            "deliveryroutecode",
            "deliveryroutename",
            "prdcode",
            "prdbatcde",
            "prdqty",
            "prdselratebeforetax",
            "prdselrateaftertax",
            "prdfreeqty",
            "prdgrossamt",
            "prdspldiscamt",
            "prdschdiscamt",
            "prdcashdiscamt",
            "prddbdiscamt",
            "prdtaxamt",
            "prdnetamt",
            "uploadflag",
            "createduserid",
            "createddate",
            "migrationflag",
            "salinvlinecount",
            "mrp",
            "syncid",
            "creditnoteamt",
            "salfreeqtyvalue",
            "nrvalue",
            "vcpschemeamount",
            "modifieddate",
            "run_id",
            "crtd_dttm",
            "filename"  
        )

        file_name=file_name.split(".")[0]+"_"+ datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        
        #move to success
        snowdf.write.mode("append").saveAsTable(stage_name.split(".")[0] + "." + target_table)
        # snowdf.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)
        
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

CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.INDSDL_RAW.CSL_DAILYSALES_UNDELIVERED_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS 'from snowflake.snowpark.functions import lit
from snowflake.snowpark.types import StringType, StructType, StructField
from datetime import datetime
import pandas as pd
import snowflake.snowpark as snowpark
import pytz

def main(session: snowpark.Session,Param):
    # Param = ["dailysales_undelivered20240523061916.csv","INDSDL_RAW.DEV_LOAD_STAGE_ADLS","dev/india_xdm/transaction/dailysales_undelivered","SDL_CSL_DAILYSALES_UNDELIVERED"]

    try:
        file_name = Param[0]
        stage_name = Param[1]
        temp_stage_path = Param[2]
        target_table = Param[3]

        # Define schema
        df_schema = StructType([
            StructField("distcode", StringType(), True),
            StructField("salinvno", StringType(), True),
            StructField("salinvdate", StringType(), True),
            StructField("saldlvdate", StringType(), True),
            StructField("salgrossamt", StringType(), True),
            StructField("salspldiscamt", StringType(), True),
            StructField("salschdiscamt", StringType(), True),
            StructField("salcashdiscamt", StringType(), True),
            StructField("saldbdiscamt", StringType(), True),
            StructField("saltaxamt", StringType(), True),
            StructField("salwdsamt", StringType(), True),
            StructField("saldbadjamt", StringType(), True),
            StructField("salcradjamt", StringType(), True),
            StructField("salonaccountamt", StringType(), True),
            StructField("salmktretamt", StringType(), True),
            StructField("salreplaceamt", StringType(), True),
            StructField("salotherchargesamt", StringType(), True),
            StructField("salinvleveldiscamt", StringType(), True),
            StructField("saltotdedn", StringType(), True),
            StructField("saltotaddn", StringType(), True),
            StructField("salroundoffamt", StringType(), True),
            StructField("salnetamt", StringType(), True),
            StructField("lcncode", StringType(), True),
            StructField("salesmancode", StringType(), True),
            StructField("salesmanname", StringType(), True),
            StructField("salesroutecode", StringType(), True),
            StructField("salesroutename", StringType(), True),
            StructField("rtrcode", StringType(), True),
            StructField("rtrname", StringType(), True),
            StructField("vechname", StringType(), True),
            StructField("dlvboyname", StringType(), True),
            StructField("deliveryroutecode", StringType(), True),
            StructField("deliveryroutename", StringType(), True),
            StructField("prdcode", StringType(), True),
            StructField("prdbatcde", StringType(), True),
            StructField("prdqty", StringType(), True),
            StructField("prdselratebeforetax", StringType(), True),
            StructField("prdselrateaftertax", StringType(), True),
            StructField("prdfreeqty", StringType(), True),
            StructField("prdgrossamt", StringType(), True),
            StructField("prdspldiscamt", StringType(), True),
            StructField("prdschdiscamt", StringType(), True),
            StructField("prdcashdiscamt", StringType(), True),
            StructField("prddbdiscamt", StringType(), True),
            StructField("prdtaxamt", StringType(), True),
            StructField("prdnetamt", StringType(), True),
            StructField("salinvlvldiscper", StringType(), True),
            StructField("billstatus", StringType(), True),
            StructField("mrp", StringType(), True),
            StructField("nrvalue", StringType(), True),
            StructField("rtrurccode", StringType(), True),
            StructField("createddate", StringType(), True),
            StructField("salinvmode", StringType(), True),
            StructField("salinvtype", StringType(), True),
            StructField("salinvlinecount", StringType(), True),
            StructField("syncid", StringType(), True),
            StructField("modifieddate", StringType(), True),
            StructField("createddt", StringType(), True)
        ])

        # Read the CSV file into a Snowflake DataFrame
        df = session.read \\
            .schema(df_schema) \\
            .option("skip_header", 1) \\
            .option("field_delimiter", "") \\
            .option("field_optionally_enclosed_by", "\\"") \\
            .csv(f"@{stage_name}/{temp_stage_path}/{file_name}")

        df = df.na.drop("all")

        if df.count() == 0:
            return "No Data in file"

        def removecsv(file_name):
            return file_name.split(''.'')[0] 

        df = df.withColumn("lcnid", lit(None)).withColumn("rtrid", lit(None)).withColumn("uploadflag", lit(None)).withColumn("uploadeddate", lit(None))

        df = df.withColumn(''filename'',lit(removecsv(file_name)))  
        df = df.withColumn("crtd_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        df = df.withColumn("run_id", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))


        snowdf = df.select(
            "distcode",
            "salinvno",
            "salinvdate",
            "saldlvdate",
            "salinvmode",
            "salinvtype",
            "salgrossamt",
            "salspldiscamt",
            "salschdiscamt",
            "salcashdiscamt",
            "saldbdiscamt",
            "saltaxamt",
            "salwdsamt",
            "saldbadjamt",
            "salcradjamt",
            "salonaccountamt",
            "salmktretamt",
            "salreplaceamt",
            "salotherchargesamt",
            "salinvleveldiscamt",
            "saltotdedn",
            "saltotaddn",
            "salroundoffamt",
            "salnetamt",
            "lcnid",
            "lcncode",
            "salesmancode",
            "salesmanname",
            "salesroutecode",
            "salesroutename",
            "rtrid",
            "rtrcode",
            "rtrname",
            "vechname",
            "dlvboyname",
            "deliveryroutecode",
            "deliveryroutename",
            "prdcode",
            "prdbatcde",
            "prdqty",
            "prdselratebeforetax",
            "prdselrateaftertax",
            "prdfreeqty",
            "prdgrossamt",
            "prdspldiscamt",
            "prdschdiscamt",
            "prdcashdiscamt",
            "prddbdiscamt",
            "prdtaxamt",
            "prdnetamt",
            "uploadflag",
            "salinvlinecount",
            "salinvlvldiscper",
            "billstatus",
            "uploadeddate",
            "syncid",
            "createddate",
            "mrp",
            "nrvalue",
            "run_id",
            "crtd_dttm",
            "filename"  
        )

        file_name=file_name.split(".")[0]+"_"+ datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        
        #move to success
        snowdf.write.mode("append").saveAsTable(stage_name.split(".")[0] + "." + target_table)
        # snowdf.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True,file_format_name=''my_csv_format'')
        
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

CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.INDSDL_RAW.CSL_ORDERBOOKING_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS 'from snowflake.snowpark.functions import lit, col
from snowflake.snowpark.types import StringType, StructType, StructField
from datetime import datetime
import snowflake.snowpark as snowpark
import pandas as pd
import pytz

def main(session: snowpark.Session,Param):
    # Param = [''orderbooking20240527061541.csv'',''INDSDL_RAW.DEV_LOAD_STAGE_ADLS'',''dev/india_xdm/transaction/orderbooking'',''SDL_CSL_ORDERBOOKING'']

    try:
        file_name = Param[0]
        stage_name = Param[1]
        temp_stage_path = Param[2]
        target_table = Param[3]

        # Define schema
        df_schema = StructType([
            StructField("distcode", StringType(), True),
            StructField("orderno", StringType(), True),
            StructField("orderdate", StringType(), True),
            StructField("orddlvdate", StringType(), True),
            StructField("allowbackorder", StringType(), True),
            StructField("ordtype", StringType(), True),
            StructField("ordpriority", StringType(), True),
            StructField("orddocref", StringType(), True),
            StructField("remarks", StringType(), True),
            StructField("roundoffamt", StringType(), True),
            StructField("ordtotalamt", StringType(), True),
            StructField("salesmancode", StringType(), True),
            StructField("salesmanname", StringType(), True),
            StructField("salesroutecode", StringType(), True),
            StructField("salesroutename", StringType(), True),
            StructField("urccode", StringType(), True),
            StructField("rtrcode", StringType(), True),
            StructField("rtrname", StringType(), True),
            StructField("prdcode", StringType(), True),
            StructField("prdbatcde", StringType(), True),
            StructField("prdqty", StringType(), True),
            StructField("prdbilledqty", StringType(), True),
            StructField("prdselrate", StringType(), True),
            StructField("prdgrossamt", StringType(), True),
            StructField("recorddate", StringType(), True),
            StructField("createddate", StringType(), True),
            StructField("syncid", StringType(), True),
            StructField("recommendedsku", StringType(), True),
            StructField("createddt", StringType(), True),
        ])

        # Read the CSV file into a Snowflake DataFrame
        df = session.read \\
            .schema(df_schema) \\
            .option("skip_header", 1) \\
            .option("field_delimiter", "") \\
            .option("field_optionally_enclosed_by", "\\"") \\
            .csv(f"@{stage_name}/{temp_stage_path}/{file_name}")

        df = df.na.drop("all")

        if df.count() == 0:
            return "No Data in file"

        def removecsv(file_name):
            return file_name.split(''.'')[0] 

        # Rename the column urccode to RtrId
        df = df.withColumnRenamed("urccode", "RtrId")
        df = df.withColumn("uploadflag", lit(None))

        df = df.withColumn(''filename'', lit(removecsv(file_name)))  
        df = df.withColumn("crtd_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        df = df.withColumn("run_id", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))

        snowdf = df.select(
            "distcode", 
            "orderno", 
            "orderdate", 
            "orddlvdate", 
            "allowbackorder", 
            "ordtype", 
            "ordpriority", 
            "orddocref", 
            "remarks", 
            "roundoffamt", 
            "ordtotalamt", 
            "salesmancode", 
            "salesmanname", 
            "salesroutecode", 
            "salesroutename", 
            "RtrId", 
            "rtrcode", 
            "rtrname", 
            "prdcode", 
            "prdbatcde", 
            "prdqty", 
            "prdbilledqty", 
            "prdselrate", 
            "prdgrossamt",
            "uploadflag",
            "recorddate", 
            "createddate", 
            "syncid", 
            "recommendedsku", 
            "run_id",
            "crtd_dttm",
            "filename"  
        )

        file_name = file_name.split(".")[0] + "_" + datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        
        # Move to success
        snowdf.write.mode("append").saveAsTable(stage_name.split(".")[0] + "." + target_table)
        # snowdf.write.copy_into_location(f"@{stage_name}/{temp_stage_path}/processed/success/{formatted_year}/{formatted_month}/{file_name}", header=True, OVERWRITE=True)
        
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

CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.INDSDL_RAW.CSL_SALESINVOICEORDERS_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS 'from snowflake.snowpark.functions import lit, col
from snowflake.snowpark.types import StringType, StructType, StructField
from datetime import datetime
import snowflake.snowpark as snowpark
import pandas as pd
import pytz

def main(session: snowpark.Session,Param):
    # Param = [''salesinvoiceorders20240527061541.csv'',''INDSDL_RAW.DEV_LOAD_STAGE_ADLS'',''dev/india_xdm/transaction/salesinvoiceorders'',''SDL_CSL_SALESINVOICEORDERS'']

    try:
        file_name = Param[0]
        stage_name = Param[1]
        temp_stage_path = Param[2]
        target_table = Param[3]

        # Define schema
        df_schema = StructType([
            StructField("distcode", StringType(), True),
            StructField("salinvno", StringType(), True),
            StructField("orderno", StringType(), True),
            StructField("orderdate", StringType(), True),
            StructField("createddate", StringType(), True),
            StructField("syncid", StringType(), True),
            StructField("createddt", StringType(), True),
        ])

        # Read the CSV file into a Snowflake DataFrame
        df = session.read \\
            .schema(df_schema) \\
            .option("skip_header", 1) \\
            .option("field_delimiter", "") \\
            .option("field_optionally_enclosed_by", "\\"") \\
            .csv(f"@{stage_name}/{temp_stage_path}/{file_name}")

        df = df.na.drop("all")

        if df.count() == 0:
            return "No Data in file"

        def removecsv(file_name):
            return file_name.split(''.'')[0] 

        df = df.withColumn("uploadflag", lit(None))

        df = df.withColumn(''filename'', lit(removecsv(file_name)))  
        df = df.withColumn("crtd_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        df = df.withColumn("run_id", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))

        snowdf = df.select(
            "distcode", 
            "salinvno", 
            "orderno", 
            "orderdate", 
            "uploadflag",
            "createddate", 
            "syncid", 
            "run_id",
            "crtd_dttm",
            "filename"  
        )

        file_name = file_name.split(".")[0] + "_" + datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        
        # Move to success
        snowdf.write.mode("append").saveAsTable(stage_name.split(".")[0] + "." + target_table)
        # snowdf.write.copy_into_location(f"@{stage_name}/{temp_stage_path}/processed/success/{formatted_year}/{formatted_month}/{file_name}", header=True, OVERWRITE=True)
        
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


CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.INDSDL_RAW.CSL_SALESRETURN_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS 'from snowflake.snowpark.functions import lit
from snowflake.snowpark.types import StringType, StructType, StructField
from datetime import datetime
import pandas as pd
import snowflake.snowpark as snowpark
import pytz

def main(session: snowpark.Session,Param):
    # Param = ["salesreturn20240523062035.csv","INDSDL_RAW.DEV_LOAD_STAGE_ADLS","dev/india_xdm/transaction/salesreturn","SDL_CSL_SALESRETURN"]

    try:
        file_name = Param[0]
        stage_name = Param[1]
        temp_stage_path = Param[2]
        target_table = Param[3]

        # Define schema
        df_schema = StructType([
            StructField("distcode", StringType(), True),
            StructField("srnrefno", StringType(), True),
            StructField("srnreftype", StringType(), True),
            StructField("srndate", StringType(), True),
            StructField("srnmode", StringType(), True),
            StructField("srntype", StringType(), True),
            StructField("srngrossamt", StringType(), True),
            StructField("srnspldiscamt", StringType(), True),
            StructField("srnschdiscamt", StringType(), True),
            StructField("srncashdiscamt", StringType(), True),
            StructField("srndbdiscamt", StringType(), True),
            StructField("srntaxamt", StringType(), True),
            StructField("srnroundoffamt", StringType(), True),
            StructField("srnnetamt", StringType(), True),
            StructField("salesmanname", StringType(), True),
            StructField("salesroutename", StringType(), True),
            StructField("rtrcode", StringType(), True),
            StructField("rtrname", StringType(), True),
            StructField("prdsalinvno", StringType(), True),
            StructField("prdlcncode", StringType(), True),
            StructField("prdcode", StringType(), True),
            StructField("prdbatcde", StringType(), True),
            StructField("prdsalqty", StringType(), True),
            StructField("prdunsalqty", StringType(), True),
            StructField("prdofferqty", StringType(), True),
            StructField("prdselrate", StringType(), True),
            StructField("prdgrossamt", StringType(), True),
            StructField("prdspldiscamt", StringType(), True),
            StructField("prdschdiscamt", StringType(), True),
            StructField("prdcashdiscamt", StringType(), True),
            StructField("prddbdiscamt", StringType(), True),
            StructField("prdtaxamt", StringType(), True),
            StructField("prdnetamt", StringType(), True),
            StructField("mrp", StringType(), True),
            StructField("rtnfreeqtyvalue", StringType(), True),
            StructField("referencetype", StringType(), True),
            StructField("salesmancode", StringType(), True),
            StructField("salesroutecode", StringType(), True),
            StructField("nrvalue", StringType(), True),
            StructField("prdselrateaftertax", StringType(), True),
            StructField("mrpcs", StringType(), True),
            StructField("lpvalue", StringType(), True),
            StructField("rtnwindowdisplayamt", StringType(), True),
            StructField("cradjamt", StringType(), True),
            StructField("rtrurccode", StringType(), True),
            StructField("createddate", StringType(), True),
            StructField("modifieddate", StringType(), True),
            StructField("syncid", StringType(), True),
            StructField("rtnlinecount", StringType(), True),
            StructField("createddt", StringType(), True)
            
        ])

        # Read the CSV file into a Snowflake DataFrame
        df = session.read \\
            .schema(df_schema) \\
            .option("skip_header", 1) \\
            .option("field_delimiter", "") \\
            .option("field_optionally_enclosed_by", "\\"") \\
            .csv(f"@{stage_name}/{temp_stage_path}/{file_name}")

        df = df.na.drop("all")

        if df.count() == 0:
            return "No Data in file"

        def removecsv(file_name):
            return file_name.split(''.'')[0] 

        df = df.withColumn("rtrid", lit(None)).withColumn("prdlcnid", lit(None)).withColumn("uploadflag", lit(None)).withColumn("createduserid", lit(None)).withColumn("migrationflag", lit(None))

        df = df.withColumn(''filename'',lit(removecsv(file_name)))  
        df = df.withColumn("crtd_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        df = df.withColumn("run_id", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))


        snowdf = df.select(
             "distcode", 
            "srnrefno", 
            "srnreftype", 
            "srndate", 
            "srnmode", 
            "srntype", 
            "srngrossamt", 
            "srnspldiscamt", 
            "srnschdiscamt", 
            "srncashdiscamt", 
            "srndbdiscamt", 
            "srntaxamt", 
            "srnroundoffamt", 
            "srnnetamt", 
            "salesmanname", 
            "salesroutename", 
            "rtrid",
            "rtrcode", 
            "rtrname", 
            "prdsalinvno", 
            "prdlcnid",
            "prdlcncode", 
            "prdcode", 
            "prdbatcde", 
            "prdsalqty", 
            "prdunsalqty", 
            "prdofferqty", 
            "prdselrate", 
            "prdgrossamt", 
            "prdspldiscamt", 
            "prdschdiscamt", 
            "prdcashdiscamt", 
            "prddbdiscamt", 
            "prdtaxamt", 
            "prdnetamt",
            "uploadflag",
            "createduserid",
            "createddate", 
            "migrationflag",
            "mrp",
            "syncid", 
            "rtnfreeqtyvalue", 
            "rtnlinecount",
            "referencetype", 
            "salesmancode", 
            "salesroutecode", 
            "nrvalue", 
            "prdselrateaftertax", 
            "modifieddate",  
            "run_id",
            "crtd_dttm",
            "filename"  
        )

        file_name=file_name.split(".")[0]+"_"+ datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        
        #move to success
        snowdf.write.mode("append").saveAsTable(stage_name.split(".")[0] + "." + target_table)
        # snowdf.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True,file_format_name=''my_csv_format'')
        
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

CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.INDSDL_RAW.CSL_UDCDETAILS_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS 'from snowflake.snowpark.functions import col, lit, date_format, current_timestamp, to_date, year, month, concat, format_number, length, to_timestamp, when, substring, upper
from snowflake.snowpark.types import StringType, StructType, StructField
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz

def main(session: snowpark.Session,Param):
    # Param=["udcdetails20240523062403.csv","INDSDL_RAW.DEV_LOAD_STAGE_ADLS","dev/india_xdm/master/udcdetails","SDL_CSL_UDCDETAILS"]
    try:
        file_name = Param[0]
        stage_name = Param[1]
        temp_stage_path = Param[2]
        target_table = Param[3]
        
        df_schema = StructType([
            StructField("cmpcode", StringType(), True),
            StructField("distcode", StringType(), True),     
            StructField("mastername", StringType(), True),
            StructField("mastervaluecode", StringType(), True),
            StructField("columnname", StringType(), True),
            StructField("columnvalue", StringType(), True),
            StructField("isdefault", StringType(), True),
            StructField("createddt", StringType(), True)
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

        df = df.withColumn("master_id", lit(None)).withColumn("sync_id", lit(None)).withColumn("mastervaluename", lit(None)).withColumn("uploadflag", lit(None))

        def removecsv(file_name):
            return file_name.split(''.'')[0] 

        df = df.withColumn("mastervaluecode", substring("mastervaluecode", 7, length("mastervaluecode")))
        
        df = df.withColumn(''filename'',lit(removecsv(file_name)))  
        df = df.withColumn("crtd_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        df = df.withColumn("run_id", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))

        snowdf = df.select( 
                  "distcode",
                  "master_id",
                  "mastername", 
                  "mastervaluecode",
                  "mastervaluename",
                  "columnname", 
                  "columnvalue",
                  "uploadflag",
                  "createddt",
                  "sync_id",
                  "run_id",
                  "crtd_dttm",
                  "filename"
            )


        file_name=file_name.split(".")[0]+"_"+ datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        
        #move to success
        snowdf.write.mode("append").saveAsTable(stage_name.split(".")[0] + "." + target_table)
        # snowdf.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)
        
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

CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.INDSDL_RAW.ECOMMERCE_OFFTAKE_AMAZON_PREPROCESSING("PARAM" ARRAY)
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
    # Param=[''Offtake_India_Amazon_Apr24_2024-05-17.csv'',''INDSDL_RAW.DEV_LOAD_STAGE_ADLS'',''dev/ecommerce_offtake/transaction/amazon'',''SDL_ECOMMERCE_OFFTAKE_AMAZON'']
    try:
        file_name = Param[0]
        stage_name = Param[1]
        temp_stage_path = Param[2]
        target_table = Param[3]
        
        df_schema = StructType([
            StructField("platform", StringType(), True),
            StructField("brand", StringType(), True),     
            StructField("rpc", StringType(), True),
            StructField("product_title", StringType(), True),
            StructField("Quantity", StringType(), True),
            StructField("MRP", StringType(), True),
            StructField("MRP_Offtakes_Value", StringType(), True),
            StructField("month", StringType(), True),
            
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

        # Extract the relevant part of the file name
        extracted_file_name = "_".join(file_name.split("_")[:4])

        df = df.withColumn("load_date", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d")))
        df = df.withColumn("filename", lit(extracted_file_name))
        # df = df.withColumn("run_id", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))


        snowdf = df.select(
            "load_date",
            "filename",
            "platform", 
            "brand",      
            "rpc", 
            "product_title", 
            "Quantity",
            "MRP",
            "MRP_Offtakes_Value", 
            "month" 
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

CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.INDSDL_RAW.ECOMMERCE_OFFTAKE_BIGBASKET_PREPROCESSING("PARAM" ARRAY)
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
    # Param=[''Offtake_India_Bigbasket_Apr24_2024-05-13.csv'',''INDSDL_RAW.DEV_LOAD_STAGE_ADLS'',''dev/ecommerce_offtake/transaction/bigbasket'',''SDL_ECOMMERCE_OFFTAKE_BIGBASKET'']
    try:
        file_name = Param[0]
        stage_name = Param[1]
        temp_stage_path = Param[2]
        target_table = Param[3]
        
        df_schema = StructType([
            StructField("city_name", StringType(), True),
            StructField("dc_name", StringType(), True),     
            StructField("department", StringType(), True),
            StructField("top_level_category", StringType(), True),
            StructField("lowest_category", StringType(), True),
            StructField("product_id", StringType(), True),
            StructField("brand_name", StringType(), True),
            StructField("product_description", StringType(), True),
             StructField("total_cost_price", StringType(), True),
            StructField("mrp", StringType(), True),     
            StructField("qty_sold", StringType(), True),
            StructField("total_sales", StringType(), True),
            StructField("sub_category", StringType(), True),
            StructField("manufacturing_company", StringType(), True),
            StructField("cost_price", StringType(), True),
            
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

        # Extract the relevant part of the file name
        extracted_file_name = "_".join(file_name.split("_")[:4])

        df = df.withColumn("load_date", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d")))
        df = df.withColumn("filename", lit(extracted_file_name))
        # df = df.withColumn("run_id", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))


        snowdf = df.select(
            "load_date",
            "filename",
             "city_name", 
            "dc_name",      
            "department", 
            "top_level_category", 
            "lowest_category", 
            "product_id", 
            "brand_name", 
            "product_description", 
            "total_cost_price", 
            "mrp",      
            "qty_sold", 
            "total_sales", 
            "sub_category", 
            "manufacturing_company", 
            "cost_price"
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

CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.INDSDL_RAW.ECOMMERCE_OFFTAKE_FIRSTCRY_PREPROCESSING("PARAM" ARRAY)
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
    # Param=[''Offtake_India_FirstCry_Apr24_2024-05-14.csv'',''INDSDL_RAW.DEV_LOAD_STAGE_ADLS'',''dev/ecommerce_offtake/transaction/firstcry'',''SDL_ECOMMERCE_OFFTAKE_FIRSTCRY'']
    try:
        file_name = Param[0]
        stage_name = Param[1]
        temp_stage_path = Param[2]
        target_table = Param[3]
        
        df_schema = StructType([
            StructField("brand_name", StringType(), True),
            StructField("product_id", StringType(), True),     
            StructField("product_name", StringType(), True),
            StructField("sum_of_sales", StringType(), True),
            StructField("sum_of_mrpsales", StringType(), True),
            
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

        # Extract the relevant part of the file name
        extracted_file_name = "_".join(file_name.split("_")[:4])

        df = df.withColumn("load_date", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d")))
        df = df.withColumn("filename", lit(extracted_file_name))
        # df = df.withColumn("run_id", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))


        snowdf = df.select(
            "load_date",
            "filename",
            "brand_name", 
            "product_id",      
            "product_name", 
            "sum_of_sales", 
            "sum_of_mrpsales",
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

CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.INDSDL_RAW.ECOMMERCE_OFFTAKE_FLIPKART_PREPROCESSING("PARAM" ARRAY)
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
    # Param=[''Offtake_India_Flipkart_Apr24_2024-05-14.csv'',''INDSDL_RAW.DEV_LOAD_STAGE_ADLS'',''dev/ecommerce_offtake/transaction/flipkart'',''SDL_ECOMMERCE_OFFTAKE_FLIPKART'']
    try:
        file_name = Param[0]
        stage_name = Param[1]
        temp_stage_path = Param[2]
        target_table = Param[3]
        
        df_schema = StructType([
            StructField("account_name", StringType(), True),
            StructField("transaction_date", StringType(), True),     
            StructField("fsn", StringType(), True),
            StructField("product_description", StringType(), True),
            StructField("brand", StringType(), True),
            StructField("qty", StringType(), True),
            StructField("gmv", StringType(), True),
            
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

        # Extract the relevant part of the file name
        extracted_file_name = "_".join(file_name.split("_")[:4])

        df = df.withColumn("load_date", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d")))
        df = df.withColumn("filename", lit(extracted_file_name))
        # df = df.withColumn("run_id", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))


        snowdf = df.select(
            "load_date",
            "filename",
            "account_name", 
            "transaction_date",      
            "fsn", 
            "product_description", 
            "brand", 
            "qty", 
            "gmv",
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

CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.INDSDL_RAW.ECOMMERCE_OFFTAKE_GROFERS_PREPROCESSING("PARAM" ARRAY)
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
    # Param=[''Offtake_India_Grofers_Mar24_2024-05-16.csv'',''INDSDL_RAW.DEV_LOAD_STAGE_ADLS'',''dev/ecommerce_offtake/transaction/grofers'',''SDL_ECOMMERCE_OFFTAKE_GROFERS'']
    try:
        file_name = Param[0]
        stage_name = Param[1]
        temp_stage_path = Param[2]
        target_table = Param[3]
        
        df_schema = StructType([
            StructField("l_cat", StringType(), True),
            StructField("l1_cat", StringType(), True),     
            StructField("product_id", StringType(), True),
            StructField("product_name", StringType(), True),
            StructField("sum_of_mrp_gmv", StringType(), True),
            StructField("sum_of_qty_sold", StringType(), True),
        ])
        
        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",2)\\
            .option("field_delimiter", "")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
        
        df = df.na.drop("all")
        
        if df.count() == 0:
            return "No Data in file"

        # Extract the relevant part of the file name
        extracted_file_name = "_".join(file_name.split("_")[:4])

        df = df.withColumn("load_date", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d")))
        df = df.withColumn("filename", lit(extracted_file_name))
        # df = df.withColumn("run_id", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))


        snowdf = df.select(
            "load_date",
            "filename",
            "l_cat", 
            "l1_cat",      
            "product_id", 
            "product_name", 
            "sum_of_mrp_gmv", 
            "sum_of_qty_sold", 
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

CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.INDSDL_RAW.ECOMMERCE_OFFTAKE_MASTER_MAPPING_PREPROCESSING("PARAM" ARRAY)
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
    # Param=[''India_E_Commerce_Offtake_Master_Mapping_EAN_w_SKU_20231130_2024-03-12.csv'',''INDSDL_RAW.DEV_LOAD_STAGE_ADLS'',''dev/ecommerce_offtake/transaction/india_e_commerce_offtake_master_mapping_ean_w_sku'',''SDL_ECOMMERCE_OFFTAKE_MASTER_MAPPING'']
    try:
        file_name = Param[0]
        stage_name = Param[1]
        temp_stage_path = Param[2]
        target_table = Param[3]
        
        df_schema = StructType([
            StructField("account_name", StringType(), True),
            StructField("account_sku_code", StringType(), True),     
            StructField("sku_name_in_file", StringType(), True),
            StructField("brand_name", StringType(), True),
            StructField("lakshya_sku_name", StringType(), True),
            StructField("ean", StringType(), True),
            
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

        # Extract the relevant part of the file name
        extracted_file_name = "_".join(file_name.split("_")[:10])

        df = df.withColumn("load_date", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d")))
        df = df.withColumn("filename", lit(extracted_file_name))
        # df = df.withColumn("run_id", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))


        snowdf = df.select(
            "load_date",
            "filename",
            "account_name", 
            "account_sku_code",      
            "sku_name_in_file", 
            "brand_name", 
            "lakshya_sku_name", 
            "ean", 
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
CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.INDSDL_RAW.ECOMMERCE_OFFTAKE_NYKAA_PREPROCESSING("PARAM" ARRAY)
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
    # Param=[''Offtake_India_Nykaa_Mar24_2024-05-09.csv'',''INDSDL_RAW.DEV_LOAD_STAGE_ADLS'',''dev/ecommerce_offtake/transaction/nykaa'',''SDL_ECOMMERCE_OFFTAKE_NYKAA'']
    try:
        file_name = Param[0]
        stage_name = Param[1]
        temp_stage_path = Param[2]
        target_table = Param[3]
        
        df_schema = StructType([
            StructField("sku_code", StringType(), True),
            StructField("qty", StringType(), True),     
            StructField("mrp", StringType(), True),
            StructField("product_name", StringType(), True),
            
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

        # Extract the relevant part of the file name
        extracted_file_name = "_".join(file_name.split("_")[:4])

        df = df.withColumn("load_date", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d")))
        df = df.withColumn("filename", lit(extracted_file_name))
        # df = df.withColumn("run_id", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))


        snowdf = df.select(
            "load_date",
            "filename",
            "sku_code", 
            "qty",      
            "mrp", 
            "product_name", 
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

CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.INDSDL_RAW.ECOMMERCE_OFFTAKE_PAYTM_PREPROCESSING("PARAM" ARRAY)
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
    # Param=['''',''INDSDL_RAW.DEV_LOAD_STAGE_ADLS'',''dev/ecommerce_offtake/transaction/paytm'',''SDL_ECOMMERCE_OFFTAKE_PAYTM'']
    try:
        file_name = Param[0]
        stage_name = Param[1]
        temp_stage_path = Param[2]
        target_table = Param[3]
        
        df_schema = StructType([
            StructField("date", StringType(), True),
            StructField("reporting_category", StringType(), True),     
            StructField("product_id", StringType(), True),
            StructField("product_name", StringType(), True),
            StructField("brand_id", StringType(), True),
            StructField("brand_name", StringType(), True),
            StructField("merchant_id", StringType(), True),
            StructField("merchant_name", StringType(), True),
            StructField("l3", StringType(), True),
            StructField("product_category", StringType(), True),
            StructField("category_id", StringType(), True),
            StructField("quantity_ordered", StringType(), True),
            StructField("gmv_ordered", StringType(), True),
            
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

        # Extract the relevant part of the file name
        extracted_file_name = "_".join(file_name.split("_")[:4])

        df = df.withColumn("load_date", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d")))
        df = df.withColumn("filename", lit(extracted_file_name))
        # df = df.withColumn("run_id", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))


        snowdf = df.select(
            "load_date",
            "filename",
             "date", 
            "reporting_category",      
            "product_id", 
            "product_name", 
            "brand_id", 
            "brand_name", 
            "merchant_id", 
            "merchant_name", 
            "l3", 
            "product_category", 
            "category_id", 
            "quantity_ordered", 
            "gmv_ordered", 
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

CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.INDSDL_RAW.IN_BATCH_MASTER_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '
import snowflake.snowpark as snowpark
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DateType, DoubleType,TimestampType,DecimalType
from snowflake.snowpark.functions import col,lit,date_format, try_cast, trim, to_date, left,  right
import pandas as pd
from datetime import datetime
import pytz

def main(session: snowpark.Session,Param): 

    try:
    
        # Param = [
        #     ''batchmaster'',
        #     ''INDSDL_RAW.DEV_LOAD_STAGE_ADLS'',
        #     ''dev/india_dna/master/batchmaster/in_batchmaster'',
        #     ''SDL_XDM_BATCHMASTER''
        # ]
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        sch_name        = stage_name.split(''.'')[0]
        target_table    = sch_name+"."+Param[3]
    
        df_schema=StructType([
                StructField(''STATECODE'', StringType()),
                StructField(''PRODCODE'', StringType()),
                StructField(''MRP'', StringType()),
                StructField(''LSP'', StringType()),
                StructField(''SELLRATE'', StringType()),
                StructField(''PURCHRATE'', StringType()),
                StructField(''CLAIMRATE'', StringType()),
                StructField(''NETRATE'', StringType()),
                StructField(''CREATEDDT'', StringType())
                ])
    
        df = session.read \\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "\\u0001")\\
            .option("skip_blank_lines", True) \\
            .option("field_optionally_enclosed_by", "\\"") \\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
    
        df = df.withColumn("FILE_NAME",lit(file_name).cast("string"))
        df = df.withColumn("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Kolkata")).strftime("%Y%m%d%H%M%S")))
        df = df.withColumn("CRT_DTTM",lit(datetime.now(pytz.timezone("Asia/Kolkata")).strftime("%Y-%m-%d %H:%M:%S")))

                                    
        final_df = df.select(\\
            trim(col("statecode")).alias("statecode"),\\
            trim(col("prodcode")).alias("prodcode"),\\
            try_cast(trim(col("mrp")),DecimalType(18,6)).alias("mrp"),\\
            try_cast(trim(col("lsp")),DecimalType(18,6)).alias("lsp"),\\
            try_cast(trim(col("sellrate")),DecimalType(18,6)).alias("sellrate"),\\
            try_cast(trim(col("purchrate")),DecimalType(18,6)).alias("purchrate"),\\
            try_cast(trim(col("claimrate")),DecimalType(18,6)).alias("claimrate"),\\
            try_cast(trim(col("netrate")),DecimalType(18,6)).alias("netrate"),\\
            try_cast(col("createddt"), TimestampType()).alias("createddt"),\\
            "FILE_NAME", "RUN_ID", "CRT_DTTM" 
        ).alias("final_df")
        
        final_df.write.mode("append").saveAsTable(target_table)
                                    
        return "Success"
        
    except KeyError as key_error:
        # Handle KeyError (missing columns) here
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message
        
    except Exception as e:
        # Handle exceptions here
        error_message = f"Error: {str(e)}" +str(df.columns)
        return error_message';

CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.INDSDL_RAW.IN_BUSINESS_CALENDAR_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS 'import snowflake.snowpark as snowpark
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DateType, DoubleType,TimestampType,DecimalType
from snowflake.snowpark.functions import col,lit,date_format, try_cast, trim, to_date, left,  right
import pandas as pd
from datetime import datetime
import pytz

def main(session: snowpark.Session,Param): 

    try:
    
        # Param = [
        #     ''businesscalendar'',
        #     ''INDSDL_RAW.DEV_LOAD_STAGE_ADLS'',
        #     ''dev/india_dna/master/businesscalendar/in_businesscalendar'',
        #     ''sdl_xdm_businesscalendar''
        # ]
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        sch_name        = stage_name.split(''.'')[0]
        target_table    = sch_name+"."+Param[3]
    
        df_schema=StructType([
                StructField(''SALINVDATE'', StringType()),
                StructField(''MONTH'', StringType()),
                StructField(''YEAR'', StringType()),
                StructField(''WEEK'', StringType()),
                StructField(''MONTHKEY'', StringType())
                ])

        
        df = session.read \\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "\\u0001")\\
            .option("skip_blank_lines", True) \\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
    
        df = df.withColumn("FILE_NAME",lit(file_name).cast("string"))
        df = df.withColumn("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Kolkata")).strftime("%Y%m%d%H%M%S")))
        df = df.withColumn("CRT_DTTM",lit(datetime.now(pytz.timezone("Asia/Kolkata")).strftime("%Y-%m-%d %H:%M:%S")))

                                    
        final_df = df.select(\\
           
            try_cast(col("SALINVDATE"), TimestampType()).alias("SALINVDATE"),\\
            trim(col(''MONTH'')).alias(''MONTH''),\\
            try_cast(col("YEAR"), IntegerType()).alias("YEAR"),\\
            trim(col(''WEEK'')).alias(''WEEK''),\\
            try_cast(col("MONTHKEY"), IntegerType()).alias("MONTHKEY"),\\
            "FILE_NAME", "RUN_ID",
            try_cast(col("CRT_DTTM"), TimestampType()).alias("CRT_DTTM")
        ).alias("final_df")
        
        final_df.write.mode("append").saveAsTable(target_table)
                                    
        return "Success"
        
    except KeyError as key_error:
        # Handle KeyError (missing columns) here
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message
        
    except Exception as e:
        # Handle exceptions here
        error_message = f"Error: {str(e)}" +str(df.columns)
        return error_message';


CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.INDSDL_RAW.IN_CHANNEL_MASTER_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS 'import snowflake.snowpark as snowpark
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DateType, DoubleType,TimestampType,DecimalType
from snowflake.snowpark.functions import col,lit,date_format, try_cast, trim, to_date, left,  right
import pandas as pd
from datetime import datetime
import pytz

def main(session: snowpark.Session,Param): 

    try:
    
        # Param = [
        #     ''channelmaster'',
        #     ''INDSDL_RAW.DEV_LOAD_STAGE_ADLS'',
        #     ''dev/india_dna/master/channelmaster/in_channelmaster'',
        #     ''sdl_xdm_channelmaster''
        # ]
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        sch_name        = stage_name.split(''.'')[0]
        target_table    = sch_name+"."+Param[3]
    
        df_schema=StructType([
                StructField(''CMPCODE'', StringType()),
                StructField(''CHANNELCODE'', StringType()),
                StructField(''CHANNELNAME'', StringType()),
                StructField(''MODDT'', StringType()),
                StructField(''CREATEDDT'', StringType()),
                StructField(''CLICKTYPE'', StringType())
                ])

        
        df = session.read \\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "\\u0001")\\
            .option("skip_blank_lines", True) \\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
    
        df = df.withColumn("FILE_NAME",lit(file_name).cast("string"))
        df = df.withColumn("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Kolkata")).strftime("%Y%m%d%H%M%S")))
        df = df.withColumn("CRT_DTTM",lit(datetime.now(pytz.timezone("Asia/Kolkata")).strftime("%Y-%m-%d %H:%M:%S")))

                                    
        final_df = df.select(\\
            trim(col(''CMPCODE'')).alias(''CMPCODE''),\\
            trim(col(''CHANNELCODE'')).alias(''CHANNELCODE''),\\
            trim(col(''CHANNELNAME'')).alias(''CHANNELNAME''),\\
            try_cast(col("MODDT"), TimestampType()).alias("MODDT"),\\
            try_cast(col("CREATEDDT"), TimestampType()).alias("CREATEDDT"),\\
            trim(col(''CLICKTYPE'')).alias(''CLICKTYPE''),\\
            "FILE_NAME", "RUN_ID",
            try_cast(col("CRT_DTTM"), TimestampType()).alias("CRT_DTTM")
        ).alias("final_df")
        
        final_df.write.mode("append").saveAsTable(target_table)
                                    
        return "Success"
        
    except KeyError as key_error:
        # Handle KeyError (missing columns) here
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message
        
    except Exception as e:
        # Handle exceptions here
        error_message = f"Error: {str(e)}" +str(df.columns)
        return error_message';

CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.INDSDL_RAW.IN_CSL_CLASSMASTER_PREPROCESSING("PARAM" ARRAY)
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
            StructField("TableId", StringType()),
            StructField("PKey", StringType()),
            StructField("ClassId", StringType()),
            StructField("ClassCode", StringType()),
            StructField("ClassDesc", StringType()),
            StructField("TurnOver", StringType()),
            StructField("Availabilty", StringType()),
            StructField("CreatedUserId", StringType()),
            StructField("CreatedDate", StringType()),
            StructField("DistHierarchyId", StringType())
        ])
        
        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "\\u0001")\\
            .option("field_optionally_enclosed_by", None) \\
            .csv("@" + stage_name + "/" + temp_stage_path + "/" + file_name)
        
        df = df.withColumn("CRT_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))

        final_df = df

        if final_df.count()==0:
            return "No Data in file"

        final_df.write.mode("append").saveAsTable(target_table)

        return "Success"
        
    except KeyError as key_error:
        
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message
        
    except Exception as e:
        
        error_message = f"Error: {str(e)}" +str(df.columns)
        return error_message
';

CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.INDSDL_RAW.IN_CSL_DISTRIBUTORACTIVATION_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '

import snowflake.snowpark as snowpark
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DateType, DoubleType,TimestampType
from snowflake.snowpark.functions import col,lit,date_format, try_cast, trim, to_date, left,  right
import pandas as pd
from datetime import datetime
import pytz

def main(session: snowpark.Session, Param): 

    try:
    
        # Param = [
        #     ''route_20240517062034.csv'',
        #     ''INDSDL_RAW.DEV_LOAD_STAGE_ADLS'',
        #     ''dev/india_dna/master/route/in_route'',
        #     ''sdl_in_route''
        # ]
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        sch_name        = stage_name.split(''.'')[0]
        target_table    = sch_name+"."+Param[3]

        df_schema=StructType([
                StructField("CmpCode", StringType()),
                StructField("UserCode", StringType()),
                StructField("DistrName", StringType()),
                StructField("DistrBrCode", StringType()),
                StructField("DistrBrType", StringType()),
                StructField("DistrBrName", StringType()),
                StructField("DistrBrAddr1", StringType()),
                StructField("DistrBrAddr2", StringType()),
                StructField("DistrBrAddr3", StringType()),
                StructField("City", StringType()),
                StructField("DistrBrState", StringType()),
                StructField("PostalCode", StringType()),
                StructField("Country", StringType()),
                StructField("ContactPerson", StringType()),
                StructField("Phone", StringType()),
                StructField("EmailId", StringType()),
                StructField("IsActive", StringType()),
                StructField("GSTINNumber", StringType()),
                StructField("GSTDistrType", StringType()),
                StructField("GSTStateCode", StringType()),
                StructField("Others1", StringType()),
                StructField("CreatedDt", StringType())
                ])
    
        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "\\u0001")\\
            .option("skip_blank_lines", True) \\
            .option("field_optionally_enclosed_by", "\\"") \\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
    
        df = df.withColumn("FILE_NAME",lit(file_name).cast("string"))
        df = df.withColumn("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        df = df.withColumn("CRT_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        df = df.withColumn("createddate",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))

        final_df = df.select(\\
            "UserCode", lit(None).as_("activefromdate"), lit(None).as_("activatedby"), lit(None).as_("activatedon"), \\
            lit(None).as_("inactivefromdate"), lit(None).as_("inactivatedby"), lit(None).as_("inactivatedon"), \\
            when(col("IsActive") == "Y", lit(1)).when(col("IsActive") == "N", lit(0)).alias("activestatus"), \\
            "createddate", "RUN_ID", "CRT_DTTM", "FILE_NAME" \\
        ).alias("final_df")
    
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

CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.INDSDL_RAW.IN_CSL_RETAILERHIERARCHY_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '

import snowflake.snowpark as snowpark
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DateType, DoubleType,TimestampType
from snowflake.snowpark.functions import col,lit,date_format, try_cast, trim, to_date, left,  right
import pandas as pd
from datetime import datetime
import pytz

def main(session: snowpark.Session, Param): 

    try:
    
        # Param = [
        #     ''route_20240517062034.csv'',
        #     ''INDSDL_RAW.DEV_LOAD_STAGE_ADLS'',
        #     ''dev/india_dna/master/route/in_route'',
        #     ''sdl_in_route''
        # ]
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        sch_name        = stage_name.split(''.'')[0]
        target_table    = sch_name+"."+Param[3]

        df_schema=StructType([
                StructField("CmpCode", StringType()),
                StructField("ChannelCode", StringType()),
                StructField("ChannelName", StringType()),
                StructField("RetlrGroupCode", StringType()),
                StructField("RetlrGroupName", StringType()),
                StructField("ClassCode", StringType()),
                StructField("ClassName", StringType()),
                StructField("Turnover", StringType()),
                StructField("CreatedDt", StringType())
                ])
    
        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "\\u0001")\\
            .option("skip_blank_lines", True) \\
            .option("field_optionally_enclosed_by", "\\"") \\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
    
        df = df.withColumn("FILE_NAME",lit(file_name).cast("string"))
        df = df.withColumn("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        df = df.withColumn("CRT_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))

        final_df = df.select(\\
            "CmpCode", "ChannelCode", "ChannelName", "RetlrGroupCode", "RetlrGroupName", "ClassCode", "ClassName", \\
            try_cast(col("Turnover"), DoubleType()).alias("RMPopulation"), \\
            try_cast(col("createddt"), TimestampType()).alias("createddt"),\\
            "RUN_ID", "CRT_DTTM", "FILE_NAME" \\
        ).alias("final_df")
               
    
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

CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.INDSDL_RAW.IN_CSL_RETAILERMASTER_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '

import snowflake.snowpark as snowpark
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DateType, DoubleType,TimestampType
from snowflake.snowpark.functions import col,lit,date_format, try_cast, trim, to_date, left,  right, when, substring, length
import pandas as pd
from datetime import datetime
import pytz

def main(session: snowpark.Session, Param): 

    try:
    
        # Param = [
        #     ''route_20240517062034.csv'',
        #     ''INDSDL_RAW.DEV_LOAD_STAGE_ADLS'',
        #     ''dev/india_dna/master/route/in_route'',
        #     ''sdl_in_route''
        # ]
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        sch_name        = stage_name.split(''.'')[0]
        target_table    = sch_name+"."+Param[3]

        df_schema=StructType([
                StructField("CmpCode", StringType()),
				StructField("Distcode", StringType()),
				StructField("DistrBrCode", StringType()),
				StructField("RtrCode", StringType()),
				StructField("RtrType", StringType()),
				StructField("Rtrname", StringType()),
				StructField("RtrUniqueCode", StringType()),
				StructField("ChannelCode", StringType()),
				StructField("RetlrGroupCode", StringType()),
				StructField("ClassCode", StringType()),
				StructField("RtrPhoneNo", StringType()),
				StructField("RtrContactPerson", StringType()),
				StructField("EmailId", StringType()),
				StructField("RegDate", StringType()),
				StructField("RtrLicNo", StringType()),
				StructField("RtrLicExpiryDate", StringType()),
				StructField("DrugLNo", StringType()),
				StructField("RtrDrugExpiryDate", StringType()),
				StructField("RtrCrBills", StringType()),
				StructField("RtrCrDays", StringType()),
				StructField("RtrCrLimit", StringType()),
				StructField("RelationStatus", StringType()),
				StructField("ParentCode", StringType()),
				StructField("Status", StringType()),
				StructField("RtrLatitude", StringType()),
				StructField("RtrLongitude", StringType()),
				StructField("CSRtrCode", StringType()),
				StructField("KeyAccount", StringType()),
				StructField("RtrFoodLicNo", StringType()),
				StructField("PanNumber", StringType()),
				StructField("RetailerType", StringType()),
				StructField("Composite", StringType()),
				StructField("RelatedParty", StringType()),
				StructField("StateName", StringType()),
				StructField("LastModDate", StringType()),
				StructField("CreatedDt", StringType()),
				StructField("rtraddress1", StringType()),
				StructField("rtraddress2", StringType()),
				StructField("rtraddress3", StringType())
                ])
    
        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "\\u0001")\\
            .option("skip_blank_lines", True) \\
            .option("field_optionally_enclosed_by", "\\"") \\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
    
        df = df.withColumn("FILE_NAME",lit(file_name).cast("string"))
        df = df.withColumn("RUN_ID",lit(9999))
        df = df.withColumn("CRT_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))

        final_df = df.select(\\
            "distcode", lit(None).as_("rtrid"), substring(col("rtrcode"),7, length("rtrcode")).alias("rtrcode"), "rtrname", \\
            "csrtrcode", "retlrgroupcode", "ChannelCode", "classcode", "keyaccount", try_cast(col("regdate"), TimestampType()).alias("regdate"), \\
            "relationstatus", "parentcode", lit(None).as_("geolevel"), lit(None).as_("geolevelvalue"), \\
            when(col("status") == "Y", lit(1)).when(col("status") == "N", lit(0)).alias("status"), \\
            lit(None).as_("createdid"), lit(None).as_("createddate"),  "rtraddress1", "rtraddress2", "rtraddress3", \\
            lit(None).as_("rtrpincode"), lit(None).as_("villageid"), lit(None).as_("villagecode"), lit(None).as_("villagename"), \\
            lit(None).as_("MODE"), lit(None).as_("uploadflag"), lit(None).as_("approvalremarks"), lit(None).as_("syncid"), "druglno", \\
            try_cast(col("rtrcrbills"),IntegerType()).alias("rtrcrbills"), \\
            try_cast(col("rtrcrlimit"), DoubleType()).alias("rtrcrlimit"), \\
            try_cast(col("rtrcrdays"),IntegerType()).alias("rtrcrdays"), \\
            lit(None).as_("rtrdayoff"), lit(None).as_("rtrtinno"), lit(None).as_("rtrcstno"), "rtrlicno", "rtrlicexpirydate", \\
            "rtrdrugexpirydate", lit(None).as_("rtrpestlicno"), lit(None).as_("rtrpestexpirydate"), lit(None).as_("approved"), \\
            "rtrphoneno", "rtrcontactperson", lit(None).as_("rtrtaxgroup"), "rtrtype", lit(None).as_("rtrtaxable"), \\
            lit(None).as_("rtrshippadd1"), lit(None).as_("rtrshippadd2"), lit(None).as_("rtrshippadd3"), "rtrfoodlicno", \\
            lit(None).as_("rtrfoodexpirydate"), lit(None).as_("rtrfoodgracedays"), lit(None).as_("rtrdruggracedays"), \\
            lit(None).as_("rtrcosmeticlicno"), lit(None).as_("rtrcosmeticexpirydate"), lit(None).as_("rtrcosmeticgracedays"), "rtrlatitude", \\
            "rtrlongitude", "rtruniquecode", \\
            "RUN_ID", "CRT_DTTM", "FILE_NAME" \\
        ).alias("final_df")
               
    
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

CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.INDSDL_RAW.IN_CSL_RETAILERROUTE_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '

import snowflake.snowpark as snowpark
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DateType, DoubleType,TimestampType
from snowflake.snowpark.functions import col,lit,date_format, try_cast, trim, to_date, left,  right
import pandas as pd
from datetime import datetime
import pytz

def main(session: snowpark.Session, Param): 

    try:
    
        # Param = [
        #     ''route_20240517062034.csv'',
        #     ''INDSDL_RAW.DEV_LOAD_STAGE_ADLS'',
        #     ''dev/india_dna/master/route/in_route'',
        #     ''sdl_in_route''
        # ]
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        sch_name        = stage_name.split(''.'')[0]
        target_table    = sch_name+"."+Param[3]

        df_schema=StructType([
                StructField("cmpcode", StringType()),
                StructField("distcode", StringType()),
                StructField("distrbrcode", StringType()),
                StructField("RtrCode", StringType()),
                StructField("RMCode", StringType()),
                StructField("RouteType", StringType()),
                StructField("CoverageSequence", StringType()),
                StructField("CreatedDt", StringType()),
                StructField("rtrname", StringType()),
                StructField("rmname", StringType())
                ])
    
        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "\\u0001")\\
            .option("skip_blank_lines", True) \\
            .option("field_optionally_enclosed_by", None) \\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
    
        df = df.withColumn("FILE_NAME",lit(file_name).cast("string"))
        df = df.withColumn("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        df = df.withColumn("CRT_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))

        final_df = df.select(\\
            "distcode",  lit(None).as_("rtrid"),  "rtrcode",  lit(None).as_("rtrname"),  lit(None).as_("rmid"),  "rmcode",\\
            lit(None).as_("rmname"),  "routetype",  lit(None).as_("uploadflag"),  \\
            try_cast(col("createddt"), TimestampType()).alias("createddate"),  lit(None).as_("syncid"), \\
            "RUN_ID", "CRT_DTTM", "FILE_NAME" \\
        ).alias("final_df")
               
    
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

CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.INDSDL_RAW.IN_CSL_SALESMANMASTER_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '

import snowflake.snowpark as snowpark
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DateType, DoubleType,TimestampType
from snowflake.snowpark.functions import col,lit,date_format, try_cast, trim, to_date, left,  right
import pandas as pd
from datetime import datetime
import pytz

def main(session: snowpark.Session, Param): 

    try:
    
        # Param = [
        #     ''route_20240517062034.csv'',
        #     ''INDSDL_RAW.DEV_LOAD_STAGE_ADLS'',
        #     ''dev/india_dna/master/route/in_route'',
        #     ''sdl_in_route''
        # ]
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        sch_name        = stage_name.split(''.'')[0]
        target_table    = sch_name+"."+Param[3]

        df_schema=StructType([
                StructField("CmpCode", StringType()),
                StructField("Distcode", StringType()),
                StructField("DistrBrCode", StringType()),
                StructField("SMCode", StringType()),
                StructField("SMName", StringType()),
                StructField("SMPhoneNo", StringType()),
                StructField("SMEmail", StringType()),
                StructField("RDSSMType", StringType()),
                StructField("SMDailyAllowance", StringType()),
                StructField("SMMonthlySalary", StringType()),
                StructField("SMMktCredit", StringType()),
                StructField("SMCreditDays", StringType()),
                StructField("Status", StringType()),
                StructField("ModUserCode", StringType()),
                StructField("AadhaarNo", StringType()),
                StructField("CreatedDt", StringType()),
                StructField("routecode", StringType()),
                StructField("rmname", StringType()),
                StructField("createddate", StringType()),
                StructField("UniqueSalesCode", StringType())
                ])
    
        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "\\u0001")\\
            .option("skip_blank_lines", True) \\
            .option("field_optionally_enclosed_by", "\\"") \\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
    
        df = df.withColumn("FILE_NAME",lit(file_name).cast("string"))
        df = df.withColumn("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        df = df.withColumn("CRT_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))

        final_df = df.select(\\
            "Distcode", lit(None).as_("smid"), "SMCode", "SMName", "SMPhoneNo", "SMEmail", lit(None).as_("smotherdetails"), \\
            try_cast(col("SMDailyAllowance"), DoubleType()).alias("SMDailyAllowance"), \\
            try_cast(col("SMMonthlySalary"), DoubleType()).alias("SMMonthlySalary"), \\
            try_cast(col("SMMktCredit"), DoubleType()).alias("SMMktCredit"), \\
            try_cast(col("smcreditdays"), IntegerType()).alias("smcreditdays"), \\
            "Status", lit(None).as_("rmid"), "routecode", "rmname", lit(None).as_("uploadflag"), \\
            try_cast(col("createddate"), TimestampType()).alias("createddate"), \\
            lit(None).as_("syncid"), "RDSSMType", "UniqueSalesCode", \\
            "RUN_ID", "CRT_DTTM", "FILE_NAME" \\
        ).alias("final_df")
               
    
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

CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.INDSDL_RAW.IN_CSL_SCHEMEUTILIZATION_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '

import snowflake.snowpark as snowpark
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DateType, DoubleType,TimestampType
from snowflake.snowpark.functions import col,lit,date_format, try_cast, trim, to_date, left,  right
import pandas as pd
from datetime import datetime
import pytz

def main(session: snowpark.Session, Param): 

    try:
    
        # Param = [
        #     ''schemeutilization_20240517062034.csv'',
        #     ''INDSDL_RAW.DEV_LOAD_STAGE_ADLS'',
        #     ''dev/india_xdm/transaction/schemeutilization'',
        #     ''sdl_csl_schemeutilization''
        # ]
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        sch_name        = stage_name.split(''.'')[0]
        target_table    = sch_name+"."+Param[3]

        df_schema=StructType([
            StructField("DistCode", StringType()),
            StructField("SchemeCode", StringType()),
            StructField("SchemeDescription", StringType()),
            StructField("InvoiceNo", StringType()),
            StructField("RtrCode", StringType()),
            StructField("SchDate", StringType()),
            StructField("SchemeType", StringType()),
            StructField("SchemeUtilizedAmt", StringType()),
            StructField("SchemeFreeProduct", StringType()),
            StructField("SchemeUtilizedQty", StringType()),
            StructField("CompanySchemeCode", StringType()),
            StructField("SchLineCount", StringType()),
            StructField("SchValueType", StringType()),
            StructField("SlabId", StringType()),
            StructField("BilledPrdCCode", StringType()),
            StructField("BilledPrdBatCode", StringType()),
            StructField("BilledQty", StringType()),
            StructField("SchDiscPerc", StringType()),
            StructField("FreePrdBatCode", StringType()),
            StructField("BilledRate", StringType()),
            StructField("ServiceCRNRefNo", StringType()),
            StructField("RtrURCCode", StringType()),
            StructField("CreatedDate", StringType()),
            StructField("ModifiedDate", StringType()),
            StructField("SyncId", StringType()),
            StructField("CreatedDt", StringType())
        ])
        
        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "\\u0001")\\
            .option("skip_blank_lines", True) \\
            .option("truncatecolumns",True) \\
            .option("field_optionally_enclosed_by", None) \\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
    
        df = df.withColumn("FILE_NAME",lit(file_name).cast("string"))
        df = df.withColumn("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        df = df.withColumn("CRT_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))

        final_df = df.select(\\
            "distcode", \\
            "schemecode", \\
            "schemedescription", \\
            "invoiceno", \\
            "rtrcode", \\
            lit(None).as_("company"), \\
            try_cast(col("schdate"), TimestampType()).alias("schdate"), \\
            "schemetype", \\
            try_cast(col("schemeutilizedamt"), DoubleType()).alias("schemeutilizedamt"), \\
            "schemefreeproduct", \\
            try_cast(col("schemeutilizedqty"), IntegerType()).alias("schemeutilizedqty"), \\
            "companyschemecode", \\
            try_cast(col("createddate"), TimestampType()).alias("createddate"), \\
            lit(None).as_("migrationflag"), \\
            lit(None).as_("schememode"), \\
            try_cast(col("syncid"), IntegerType()).alias("syncid"), \\
            try_cast(col("schlinecount"), IntegerType()).alias("schlinecount"), \\
            "schvaluetype", \\
            try_cast(col("slabid"), IntegerType()).alias("slabid"), \\
            "billedprdccode", \\
            "billedprdbatcode", \\
            try_cast(col("billedqty"), IntegerType()).alias("billedqty"), \\
            try_cast(col("schdiscperc"), DoubleType()).alias("schdiscperc"), \\
            "freeprdbatcode", \\
            try_cast(col("billedrate"), DoubleType()).alias("billedrate"), \\
            try_cast(col("modifieddate"), TimestampType()).alias("modifieddate"), \\
            "servicecrnrefno", \\
            "rtrurccode", \\
            "RUN_ID", \\
            "CRT_DTTM", \\
            "FILE_NAME" \\
        ).alias("final_df")
    
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

CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.INDSDL_RAW.IN_CSL_UDCMASTER_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '

import snowflake.snowpark as snowpark
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DateType, DoubleType,TimestampType
from snowflake.snowpark.functions import col,lit,date_format, try_cast, trim, to_date, left,  right
import pandas as pd
from datetime import datetime
import pytz

def main(session: snowpark.Session, Param): 

    try:
    
        # Param = [
        #     ''route_20240517062034.csv'',
        #     ''INDSDL_RAW.DEV_LOAD_STAGE_ADLS'',
        #     ''dev/india_dna/master/route/in_route'',
        #     ''sdl_in_route''
        # ]
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        sch_name        = stage_name.split(''.'')[0]
        target_table    = sch_name+"."+Param[3]

        df_schema=StructType([
                StructField("CmpCode", StringType()),
                StructField("AttributeCode", StringType()),
                StructField("MasterName", StringType()),
                StructField("ControlDt", StringType()),
                StructField("IsMandatory", StringType()),
                StructField("IsAllowEdit", StringType()),
                StructField("DataType", StringType()),
                StructField("Size", StringType()),
                StructField("AttrPrecision", StringType()),
                StructField("Remarks", StringType()),
                StructField("Reset", StringType()),
                StructField("UDCStatus", StringType()),
                StructField("ModDt", StringType()),
                StructField("ModUserCode", StringType()),
                StructField("CreatedDt", StringType())
                ])
    
        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "\\u0001")\\
            .option("skip_blank_lines", True) \\
            .option("field_optionally_enclosed_by", "\\"") \\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
    
        df = df.withColumn("FILE_NAME",lit(file_name).cast("string"))
        df = df.withColumn("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        df = df.withColumn("CRT_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))

        final_df = df.select(\\
            lit(None).as_("udcmasterid"), lit(None).as_("masterid"), lit(None).as_("mastername"), col("MasterName").as_("columnname"), \\
            "DataType", try_cast(col("Size"), IntegerType()).alias("Size"), \\
            try_cast(col("AttrPrecision"), IntegerType()).alias("AttrPrecision"), \\
            when(col("IsAllowEdit") == "Y", lit(1)).when(col("IsAllowEdit") == "N", lit(0)).alias("editable"), \\
            when(col("IsMandatory") == "Y", lit(1)).when(col("IsMandatory") == "N", lit(0)).alias("columnmandatory"), \\
            lit(None).as_("pickfromdefault"), lit(None).as_("downloadstatus"), lit(None).as_("createduserid"), \\
            try_cast(col("CreatedDt"), TimestampType()).alias("CreatedDt"),  \\
            "ModUserCode", \\
            try_cast(col("ModDt"), TimestampType()).alias("ModDt"),  \\
            when(col("UDCStatus") == "Y", lit(1)).when(col("UDCStatus") == "N", lit(0)).alias("udcstatus"), \\
            "RUN_ID", "CRT_DTTM", "FILE_NAME" \\
        ).alias("final_df")
               
    
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



CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.INDSDL_RAW.IN_DAILYSALES_DEL_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS 'from snowflake.snowpark.functions import lit
from snowflake.snowpark.types import StringType, StructType, StructField
from datetime import datetime
import pandas as pd
import snowflake.snowpark as snowpark
import pytz

def main(session: snowpark.Session,Param):
    # Param = [''dailysales_del20240523061554.csv'',''INDSDL_RAW.DEV_LOAD_STAGE_ADLS'',''dev/india_xdm/transaction/dailysales_del'',''SDL_DAILYSALES_DEL'']

    try:
        file_name = Param[0]
        stage_name = Param[1]
        temp_stage_path = Param[2]
        target_table = Param[3]

        # Define schema
        df_schema = StructType([
            StructField("distcode", StringType(), True),
            StructField("salinvno", StringType(), True),
            StructField("salinvdate", StringType(), True),
            StructField("saldlvdate", StringType(), True),
            StructField("salgrossamt", StringType(), True),
            StructField("salspldiscamt", StringType(), True),
            StructField("salschdiscamt", StringType(), True),
            StructField("salcashdiscamt", StringType(), True),
            StructField("saldbdiscamt", StringType(), True),
            StructField("saltaxamt", StringType(), True),
            StructField("salwdsamt", StringType(), True),
            StructField("saldbadjamt", StringType(), True),
            StructField("salcradjamt", StringType(), True),
            StructField("salonaccountamt", StringType(), True),
            StructField("salmktretamt", StringType(), True),
            StructField("salreplaceamt", StringType(), True),
            StructField("salotherchargesamt", StringType(), True),
            StructField("salinvleveldiscamt", StringType(), True),
            StructField("saltotdedn", StringType(), True),
            StructField("saltotaddn", StringType(), True),
            StructField("salroundoffamt", StringType(), True),
            StructField("salnetamt", StringType(), True),
            StructField("lcncode", StringType(), True),
            StructField("salesmancode", StringType(), True),
            StructField("salesmanname", StringType(), True),
            StructField("salesroutecode", StringType(), True),
            StructField("salesroutename", StringType(), True),
            StructField("rtrcode", StringType(), True),
            StructField("rtrname", StringType(), True),
            StructField("deliveryroutecode", StringType(), True),
            StructField("deliveryroutename", StringType(), True),
            StructField("prdcode", StringType(), True),
            StructField("prdbatcde", StringType(), True),
            StructField("prdqty", StringType(), True),
            StructField("prdselratebeforetax", StringType(), True),
            StructField("prdselrateaftertax", StringType(), True),
            StructField("prdfreeqty", StringType(), True),
            StructField("prdgrossamt", StringType(), True),
            StructField("prdspldiscamt", StringType(), True),
            StructField("prdschdiscamt", StringType(), True),
            StructField("prdcashdiscamt", StringType(), True),
            StructField("prddbdiscamt", StringType(), True),
            StructField("prdtaxamt", StringType(), True),
            StructField("prdnetamt", StringType(), True),
            StructField("createddate", StringType(), True),
            StructField("mrp", StringType(), True),
            StructField("salfreeqtyvalue", StringType(), True),
            StructField("nrvalue", StringType(), True),
            StructField("vcpschemeamount", StringType(), True),
            StructField("rtrurccode", StringType(), True),
            StructField("syncid", StringType(), True),
            StructField("creditnoteamt", StringType(), True),
            StructField("modifieddate", StringType(), True),
            StructField("salinvmode", StringType(), True),
            StructField("salinvtype", StringType(), True),
            StructField("vechname", StringType(), True),
            StructField("dlvboyname", StringType(), True),
            StructField("createduserid", StringType(), True),
            StructField("salinvlinecount", StringType(), True),
            StructField("mrpcs", StringType(), True),
            StructField("lpvalue", StringType(), True),
            StructField("createddt", StringType(), True)
        ])

        # Read the CSV file into a Snowflake DataFrame
        df = session.read \\
            .schema(df_schema) \\
            .option("skip_header", 0) \\
            .option("field_delimiter", "") \\
            .option("field_optionally_enclosed_by", "\\"") \\
            .csv(f"@{stage_name}/{temp_stage_path}/{file_name}")

        df = df.na.drop("all")

        if df.count() == 0:
            return "No Data in file"

        def removecsv(file_name):
            return file_name.split(''.'')[0] 

        df = df.withColumn(''filename'',lit(removecsv(file_name)))  
        df = df.withColumn("crtd_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        df = df.withColumn("run_id", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))


        snowdf = df.select(
            "distcode",
            "salinvno",
            "salinvdate",
            "saldlvdate",
            "salgrossamt",
            "salspldiscamt",
            "salschdiscamt",
            "salcashdiscamt",
            "saldbdiscamt",
            "saltaxamt",
            "salwdsamt",
            "saldbadjamt",
            "salcradjamt",
            "salonaccountamt",
            "salmktretamt",
            "salreplaceamt",
            "salotherchargesamt",
            "salinvleveldiscamt",
            "saltotdedn",
            "saltotaddn",
            "salroundoffamt",
            "salnetamt",
            "lcncode",
            "salesmancode",
            "salesmanname",
            "salesroutecode",
            "salesroutename",
            "rtrcode",
            "rtrname",
            "deliveryroutecode",
            "deliveryroutename",
            "prdcode",
            "prdbatcde",
            "prdqty",
            "prdselratebeforetax",
            "prdselrateaftertax",
            "prdfreeqty",
            "prdgrossamt",
            "prdspldiscamt",
            "prdschdiscamt",
            "prdcashdiscamt",
            "prddbdiscamt",
            "prdtaxamt",
            "prdnetamt",
            "createddate",
            "mrp",
            "salfreeqtyvalue",
            "nrvalue",
            "vcpschemeamount",
            "rtrurccode", 
            "syncid", 
            "creditnoteamt", 
            "modifieddate", 
            "salinvmode", 
            "salinvtype", 
            "vechname", 
            "dlvboyname", 
            "createduserid", 
            "salinvlinecount", 
            "mrpcs", 
            "lpvalue", 
            "createddt", 
            "run_id",
            "crtd_dttm",
            "filename"  
        )

        file_name=file_name.split(".")[0]+"_"+ datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        
        #move to success
        snowdf.write.mode("append").saveAsTable(stage_name.split(".")[0] + "." + target_table)
        # snowdf.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)
        
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

CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.INDSDL_RAW.IN_DISTRIBUTOR_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '
import snowflake.snowpark as snowpark
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DateType, DoubleType,TimestampType,DecimalType
from snowflake.snowpark.functions import col,lit,date_format, try_cast, trim, to_date, left,  right
import pandas as pd
from datetime import datetime
import pytz

def main(session: snowpark.Session,Param): 

    try:
    
        # Param = [
        #     ''distributor'',
        #     ''INDSDL_RAW.DEV_LOAD_STAGE_ADLS'',
        #     ''dev/india_dna/master/distributor/in_distributor'',
        #     ''sdl_xdm_distributor''
        # ]
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        sch_name        = stage_name.split(''.'')[0]
        target_table    = sch_name+"."+Param[3]
    
        df_schema=StructType([
                StructField(''CMPCODE'', StringType()),
                StructField(''USERCODE'', StringType()),
                StructField(''DISTRNAME'', StringType()),
                StructField(''DISTRBRCODE'', StringType()),
                StructField(''DISTRBRTYPE'', StringType()),
                StructField(''DISTRBRNAME'', StringType()),
                StructField(''DISTRBRADDR1'', StringType()),
                StructField(''DISTRBRADDR2'', StringType()),
                StructField(''DISTRBRADDR3'', StringType()),
                StructField(''CITY'', StringType()),
                StructField(''DISTRBRSTATE'', StringType()),
                StructField(''POSTALCODE'', StringType()),
                StructField(''COUNTRY'', StringType()),
                StructField(''CONTACTPERSON'', StringType()),
                StructField(''PHONE'', StringType()),
                StructField(''EMAILID'', StringType()),
                StructField(''ISACTIVE'', StringType()),
                StructField(''GSTINNUMBER'', StringType()),
                StructField(''GSTDISTRTYPE'', StringType()),
                StructField(''GSTSTATECODE'', StringType()),
                StructField(''OTHERS1'', StringType()),
                StructField(''ISDIRECTACCT'', StringType()),
                StructField(''TYPECODE'', StringType()),
                StructField(''PSNONPS'', StringType()),
                StructField(''CREATEDDT'', StringType())
                ])

        
        df = session.read \\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "\\u0001")\\
            .option("skip_blank_lines", True) \\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
    
        df = df.withColumn("FILE_NAME",lit(file_name).cast("string"))
        df = df.withColumn("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Kolkata")).strftime("%Y%m%d%H%M%S")))
        df = df.withColumn("CRT_DTTM",lit(datetime.now(pytz.timezone("Asia/Kolkata")).strftime("%Y-%m-%d %H:%M:%S")))

                                    
        final_df = df.select(\\
            trim(col(''CMPCODE'')).alias(''CMPCODE''),\\
            trim(col(''USERCODE'')).alias(''USERCODE''),\\
            trim(col(''DISTRNAME'')).alias(''DISTRNAME''),\\
            trim(col(''DISTRBRCODE'')).alias(''DISTRBRCODE''),\\
            trim(col(''DISTRBRTYPE'')).alias(''DISTRBRTYPE''),\\
            trim(col(''DISTRBRNAME'')).alias(''DISTRBRNAME''),\\
            trim(col(''DISTRBRADDR1'')).alias(''DISTRBRADDR1''),\\
            trim(col(''DISTRBRADDR2'')).alias(''DISTRBRADDR2''),\\
            trim(col(''DISTRBRADDR3'')).alias(''DISTRBRADDR3''),\\
            trim(col(''CITY'')).alias(''CITY''),\\
            trim(col(''DISTRBRSTATE'')).alias(''DISTRBRSTATE''),\\
            try_cast(col("POSTALCODE"), IntegerType()).alias("POSTALCODE"),\\
            trim(col(''COUNTRY'')).alias(''COUNTRY''),\\
            trim(col(''CONTACTPERSON'')).alias(''CONTACTPERSON''),\\
            trim(col(''PHONE'')).alias(''PHONE''),\\
            trim(col(''EMAILID'')).alias(''EMAILID''),\\
            trim(col(''ISACTIVE'')).alias(''ISACTIVE''),\\
            trim(col(''GSTINNUMBER'')).alias(''GSTINNUMBER''),\\
            trim(col(''GSTDISTRTYPE'')).alias(''GSTDISTRTYPE''),\\
            trim(col(''GSTSTATECODE'')).alias(''GSTSTATECODE''),\\
            trim(col(''OTHERS1'')).alias(''OTHERS1''),\\
            trim(col(''ISDIRECTACCT'')).alias(''ISDIRECTACCT''),\\
            trim(col(''TYPECODE'')).alias(''TYPECODE''),\\
            trim(col(''PSNONPS'')).alias(''PSNONPS''),\\
            try_cast(col("CREATEDDT"), TimestampType()).alias("CREATEDDT"),\\
            "FILE_NAME", "RUN_ID",
            try_cast(col("CRT_DTTM"), TimestampType()).alias("CRT_DTTM")
        ).alias("final_df")
        
        final_df.write.mode("append").saveAsTable(target_table)
                                    
        return "Success"
        
    except KeyError as key_error:
        # Handle KeyError (missing columns) here
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message
        
    except Exception as e:
        # Handle exceptions here
        error_message = f"Error: {str(e)}" +str(df.columns)
        return error_message';

CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.INDSDL_RAW.IN_FINANCE_SIMULATOR_MISCDATA_INGESTION_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS 'from snowflake.snowpark.functions import col, lit, date_format, current_timestamp, to_date, year, month, concat, format_number, regexp_replace,to_timestamp,when,trim,upper,sql_expr
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DecimalType,DateType
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz


def main(session: snowpark.Session,Param):
   
    try:

                
        # Param = [
        #     ''MiscData_20231221011223_20240107100027.csv'',\\
        #     ''INDSDL_RAW.DEV_LOAD_STAGE_ADLS'',\\
        #     ''dev/finance_simulator/source/miscdata/'',\\
        #     ''sdl_fin_sim_MiscData''
        # ]
        
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        sch_name        = stage_name.split(''.'')[0]
        target_table    = sch_name+"."+Param[3]

        df_schema = StructType([
                    StructField(''MATL_NUM'', StringType()),
                    StructField(''SKU_DESC'', StringType()),
                    StructField(''BRAND_COMBI'', StringType()),
                    StructField(''FISC_YR'', StringType()),
                    StructField(''YEAR_MONTH'', StringType()),
                    StructField(''CHNL_DESC2'', StringType()),
                    StructField(''NATURE'', StringType()),
                    StructField(''AMT_OBJ_CRNCY'', StringType()),
                    StructField(''QTY'', StringType()),
                   
                ])

        df = session.read \\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", ",")\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        df = df.withColumn("FILENAME",lit(file_name).cast("string"))
        df = df.withColumn("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        df = df.withColumn("CRTD_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        df = df.withColumn("MONTH",sql_expr("RIGHT(YEAR_MONTH, 2)"))
        

        final_df = df.select(
                        "MATL_NUM",
                        "SKU_DESC",
                        "BRAND_COMBI",
                        "FISC_YR",
                        "MONTH",
                        "CHNL_DESC2",
                        "NATURE",
                        "AMT_OBJ_CRNCY",
                        "QTY",
                        "FILENAME",
                        "RUN_ID",
                        "CRTD_DTTM" ).alias("final_df")

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

CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.INDSDL_RAW.IN_FINANCE_SIMULATOR_PLANDATA_INGESTION_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS 'from snowflake.snowpark.functions import col, lit, date_format, current_timestamp, to_date, year, month, concat, format_number, regexp_replace,to_timestamp,when,trim,upper,sql_expr
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DecimalType,DateType
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz


def main(session: snowpark.Session,Param):
   
    try:

                
        # Param = [
        #     ''plandata_20231221011223_20240107100027.csv'',\\
        #     ''INDSDL_RAW.DEV_LOAD_STAGE_ADLS'',\\
        #     ''dev/finance_simulator/plan/finance_simulator_plan/plandata/'',\\
        #     ''sdl_fin_sim_plandata''
        # ]
        
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        sch_name        = stage_name.split(''.'')[0]
        target_table    = sch_name+"."+Param[3]

        df_schema = StructType([
                    StructField(''MATL_NUM'', StringType()),
                    StructField(''FISC_YR'', StringType()),
                    StructField(''YEAR_MONTH'', StringType()),
                    StructField(''CHNL_DESC2'', StringType()),
                    StructField(''NATURE'', StringType()),
                    StructField(''AMT_OBJ_CRNCY'', StringType()),
                    StructField(''QTY'', StringType()),
                    StructField(''PLAN'', StringType())
                   
                ])

        df = session.read \\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", ",")\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        df = df.withColumn("FILENAME",lit(file_name).cast("string"))
        df = df.withColumn("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        df = df.withColumn("CRTD_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        df = df.withColumn("MONTH",sql_expr("RIGHT(YEAR_MONTH, 2)"))
        

        final_df = df.select("MATL_NUM",
                        "FISC_YR",
                        "MONTH",
                        "CHNL_DESC2",
                        "NATURE",
                        "AMT_OBJ_CRNCY",
                        "QTY",
                        "PLAN",
                        "FILENAME",
                        "RUN_ID",
                        "CRTD_DTTM" ).alias("final_df")

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

CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.INDSDL_RAW.IN_GEO_HEIRARCHY_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '
import snowflake.snowpark as snowpark
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DateType, DoubleType,TimestampType,DecimalType
from snowflake.snowpark.functions import col,lit,date_format, try_cast, trim, to_date, left,  right
import pandas as pd
from datetime import datetime
import pytz

def main(session: snowpark.Session,Param): 

    try:
    
        # Param = [
        #     ''geohierarchy'',
        #     ''INDSDL_RAW.DEV_LOAD_STAGE_ADLS'',
        #     ''dev/india_dna/master/geoheirarchy/in_geoheirarchy'',
        #     ''sdl_xdm_geohierarchy''
        # ]
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        sch_name        = stage_name.split(''.'')[0]
        target_table    = sch_name+"."+Param[3]
    
        df_schema=StructType([
                StructField(''STATECODE'', StringType()),
                StructField(''STATENAME'', StringType()),
                StructField(''DISTRICTCODE'', StringType()),
                StructField(''DISTRICTNAME'', StringType()),
                StructField(''THESILCODE'', StringType()),
                StructField(''THESILNAME'', StringType()),
                StructField(''CITYCODE'', StringType()),
                StructField(''CITYNAME'', StringType()),
                StructField(''DISTRIBUTORCODE'', StringType()),
                StructField(''DISTRIBUTORNAME'', StringType()),
                StructField(''CREATEDDT'', StringType())
                ])
    
        df = session.read \\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "\\u0001")\\
            .option("skip_blank_lines", True) \\
            .option("field_optionally_enclosed_by", "\\"") \\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
    
        df = df.withColumn("FILE_NAME",lit(file_name).cast("string"))
        df = df.withColumn("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Kolkata")).strftime("%Y%m%d%H%M%S")))
        df = df.withColumn("CRT_DTTM",lit(datetime.now(pytz.timezone("Asia/Kolkata")).strftime("%Y-%m-%d %H:%M:%S")))

                                    
        final_df = df.select(\\
            trim(col(''STATECODE'')).alias(''STATECODE''),
            trim(col(''STATENAME'')).alias(''STATENAME''),
            trim(col(''DISTRICTCODE'')).alias(''DISTRICTCODE''),
            trim(col(''DISTRICTNAME'')).alias(''DISTRICTNAME''),
            trim(col(''THESILCODE'')).alias(''THESILCODE''),
            trim(col(''THESILNAME'')).alias(''THESILNAME''),
            trim(col(''CITYCODE'')).alias(''CITYCODE''),
            trim(col(''CITYNAME'')).alias(''CITYNAME''),
            trim(col(''DISTRIBUTORCODE'')).alias(''DISTRIBUTORCODE''),
            trim(col(''DISTRIBUTORNAME'')).alias(''DISTRIBUTORNAME''),
            try_cast(col("CREATEDDT"), TimestampType()).alias("CREATEDDT"),\\
            "FILE_NAME", "RUN_ID",
            try_cast(col("CRT_DTTM"), TimestampType()).alias("CRT_DTTM")
        ).alias("final_df")
        
        final_df.write.mode("append").saveAsTable(target_table)
                                    
        return "Success"
        
    except KeyError as key_error:
        # Handle KeyError (missing columns) here
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message
        
    except Exception as e:
        # Handle exceptions here
        error_message = f"Error: {str(e)}" +str(df.columns)
        return error_message';

CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.INDSDL_RAW.IN_MUSER_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS 'import snowflake.snowpark as snowpark
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DateType, DoubleType,TimestampType,DecimalType
from snowflake.snowpark.functions import col,lit,date_format, try_cast, trim, to_date, left,  right
import pandas as pd
from datetime import datetime
import pytz

def main(session: snowpark.Session,Param): 

    try:
    
        # Param = [
        #     ''muser'',
        #     ''INDSDL_RAW.DEV_LOAD_STAGE_ADLS'',
        #     ''dev/india_dna/master/muser/in_muser'',
        #     ''sdl_xdm_muser''
        # ]
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        sch_name        = stage_name.split(''.'')[0]
        target_table    = sch_name+"."+Param[3]
    
        df_schema=StructType([
                StructField(''USERCODE'', StringType()),
                StructField(''USERFIRSTNAME'', StringType()),
                StructField(''USERLASTNAME'', StringType()),
                StructField(''NTID'', StringType()),
                StructField(''USERPROFILE'', StringType()),
                StructField(''REGIONCODE'', StringType()),
                StructField(''ZONECODE'', StringType()),
                StructField(''TERRITORYCODE'', StringType()),
                StructField(''ISACTIVE'', StringType()),
                StructField(''EMAILID'', StringType()),
                StructField(''WWID'', StringType()),
                StructField(''CREATEDDATE'', StringType()),
                StructField(''MODIFIEDDATE'', StringType()),
                StructField(''CREATEDDT'', StringType()),
                ])

        
        df = session.read \\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "\\u0001")\\
            .option("skip_blank_lines", True) \\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
    
        df = df.withColumn("FILE_NAME",lit(file_name).cast("string"))
        df = df.withColumn("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Kolkata")).strftime("%Y%m%d%H%M%S")))
        df = df.withColumn("CRT_DTTM",lit(datetime.now(pytz.timezone("Asia/Kolkata")).strftime("%Y-%m-%d %H:%M:%S")))

                                    
        final_df = df.select(\\
            trim(col(''USERCODE'')).alias(''USERCODE''),\\
            trim(col(''USERFIRSTNAME'')).alias(''USERFIRSTNAME''),\\
            trim(col(''USERLASTNAME'')).alias(''USERLASTNAME''),\\
            trim(col(''NTID'')).alias(''NTID''),\\
            trim(col(''USERPROFILE'')).alias(''USERPROFILE''),\\
            trim(col(''REGIONCODE'')).alias(''REGIONCODE''),\\
            trim(col(''ZONECODE'')).alias(''ZONECODE''),\\
            trim(col(''TERRITORYCODE'')).alias(''TERRITORYCODE''),\\
            trim(col(''ISACTIVE'')).alias(''ISACTIVE''),\\
            trim(col(''EMAILID'')).alias(''EMAILID''),\\
            trim(col(''WWID'')).alias(''WWID''),\\
            try_cast(col("CREATEDDATE"), TimestampType()).alias("CREATEDDATE"),\\
            try_cast(col("MODIFIEDDATE"), TimestampType()).alias("MODIFIEDDATE"),\\
            try_cast(col("CREATEDDT"), TimestampType()).alias("CREATEDDT"),\\
            "FILE_NAME", "RUN_ID",
            try_cast(col("CRT_DTTM"), TimestampType()).alias("CRT_DTTM")
        ).alias("final_df")
        
        final_df.write.mode("append").saveAsTable(target_table)
                                    
        return "Success"
        
    except KeyError as key_error:
        # Handle KeyError (missing columns) here
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message
        
    except Exception as e:
        # Handle exceptions here
        error_message = f"Error: {str(e)}" +str(df.columns)
        return error_message';

CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.INDSDL_RAW.IN_PERFECTSTORE_MSL_PREPROCESSING("PARAM" ARRAY)
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
        sheet_names     = Param[4]

        df_schema = StructType([
            StructField("Visit_ID", StringType()),
            StructField("Visit_Date", StringType()),
            StructField("Region", StringType()),
            StructField("JnJRKAM", StringType()),
            StructField("JnJZM_Code", StringType()),
            StructField("JNJ_ABI_Code", StringType()),
            StructField("JnJSupervisor_Code", StringType()),
            StructField("ISP_Code", StringType()),
            StructField("ISP_Name", StringType()),
            StructField("Month", StringType()),
            StructField("Year", StringType()),
            StructField("Format", StringType()),
            StructField("Chain_Code", StringType()),
            StructField("Chain", StringType()),
            StructField("Store_Code", StringType()),
            StructField("Store_Name", StringType()),
            StructField("Product_Code", StringType()),
            StructField("Product_Name", StringType()),
            StructField("MSL", StringType()),
            StructField("Cost(INR)", StringType()),
            StructField("Quantity", StringType()),
            StructField("Amount(INR)", StringType()),
            StructField("Priority_Store", StringType())
        ])

        if sheet_names == "[]":
            files_to_read = [file_name]
        else:
            files_to_read = sheet_names[1:-1].split(",")
            files_to_read = [sh_name.strip("\\"").replace("(", "").replace(")", "").replace(" ","_") + ".csv" for sh_name in files_to_read]
        
        df = None

        for src_file in files_to_read:
            
            file_df = session.read\\
                .schema(df_schema)\\
                .option("skip_header",1)\\
                .option("field_delimiter", "\\u0001")\\
                .option("field_optionally_enclosed_by", "\\"") \\
                .csv("@" + stage_name+"/"+temp_stage_path+"/"+src_file)
            
            if not df:
                df = file_df
            else:
                df = df.unionByName(file_df)

        yearmo = file_name.split(".")[0].split("_")[-1]

        df = df.na.drop(how=''all'')

        df = df.withColumn("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        df = df.withColumn("FILE_NAME",lit(file_name).cast("string"))
        df = df.withColumn("YEARMO",lit(yearmo).cast("string"))

        final_df = df
                        
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

CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.INDSDL_RAW.IN_PERFECTSTORE_PAID_DISPLAY_PREPROCESSING("PARAM" ARRAY)
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
        sheet_names     = Param[4]
                        
        df_schema = StructType([
            StructField("Visit_ID", StringType()),
            StructField("Visit_DateTime", StringType()),
            StructField("Region", StringType()),
            StructField("JnJRKAM", StringType()),
            StructField("JnJZM_Code", StringType()),
            StructField("JNJ_ABI_Code", StringType()),
            StructField("JnJSupervisor_Code", StringType()),
            StructField("ISP_Code", StringType()),
            StructField("ISP_Name", StringType()),
            StructField("Month", StringType()),
            StructField("Year", StringType()),
            StructField("Format", StringType()),
            StructField("Chain_Code", StringType()),
            StructField("Chain", StringType()),
            StructField("Store_Code", StringType()),
            StructField("Store_Name", StringType()),
            StructField("Asset", StringType()),
            StructField("Product_Category", StringType()),
            StructField("Prooduct_Brand", StringType()),
            StructField("POSM_Brand", StringType()),
            StructField("Start_Date", StringType()),
            StructField("End_Date", StringType()),
            StructField("Audit_Status", StringType()),
            StructField("Is_Available", StringType()),
            StructField("Availability_Points", StringType()),
            StructField("Visibility_Type", StringType()),
            StructField("Visibility_Condition", StringType()),
            StructField("Is_Planogram_Availbale", StringType()),
            StructField("Select_Brand", StringType()),
            StructField("Is_Correct_Brand_Displayed", StringType()),
            StructField("BrandAvailability_Points", StringType()),
            StructField("Stock_Status", StringType()),
            StructField("Stock_Points", StringType()),
            StructField("Is_Near_Category", StringType()),
            StructField("NearCategory_Points", StringType()),
            StructField("Audit_Score", StringType()),
            StructField("Paid_Visibility_Score", StringType()),
            StructField("Reason", StringType()),
            StructField("Priority_Store", StringType())
        ])

        if sheet_names == "[]":
            files_to_read = [file_name]
        else:
            files_to_read = sheet_names[1:-1].split(",")
            files_to_read = [sh_name.strip("\\"").replace("(", "").replace(")", "").replace(" ","_") + ".csv" for sh_name in files_to_read]
        
        df = None

        for src_file in files_to_read:
            
            file_df = session.read\\
                .schema(df_schema)\\
                .option("skip_header",1)\\
                .option("field_delimiter", "\\u0001")\\
                .option("field_optionally_enclosed_by", "\\"") \\
                .csv("@" + stage_name+"/"+temp_stage_path+"/"+src_file)
            
            if not df:
                df = file_df
            else:
                df = df.unionByName(file_df)


        yearmo = file_name.split(".")[0].split("_")[-1]

        df = df.na.drop(how=''all'')

        df = df.withColumn("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        df = df.withColumn("FILE_NAME",lit(file_name).cast("string"))
        df = df.withColumn("YEARMO",lit(yearmo).cast("string"))

        final_df = df
                        
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

CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.INDSDL_RAW.IN_PERFECTSTORE_PROMO_PREPROCESSING("PARAM" ARRAY)
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
        sheet_names     = Param[4]

        df_schema = StructType([
            StructField("Visit_ID", StringType()),
            StructField("Visit_DateTime", StringType()),
            StructField("Region", StringType()),
            StructField("JnJRKAM", StringType()),
            StructField("JnJZM_Code", StringType()),
            StructField("JNJ_ABI_Code", StringType()),
            StructField("JnJSupervisor_Code", StringType()),
            StructField("ISP_Code", StringType()),
            StructField("ISP_Name", StringType()),
            StructField("Month", StringType()),
            StructField("Year", StringType()),
            StructField("Format", StringType()),
            StructField("Chain_Code", StringType()),
            StructField("Chain", StringType()),
            StructField("Store_Code", StringType()),
            StructField("Store_Name", StringType()),
            StructField("Product_Category", StringType()),
            StructField("Product_Brand", StringType()),
            StructField("Promotion_Product_Code", StringType()),
            StructField("Promotion_Product_Name", StringType()),
            StructField("IsPromotionAvailable", StringType()),
            StructField("PhotoPath", StringType()),
            StructField("CountOfFacing", StringType()),
            StructField("PromotionOfferType", StringType()),
            StructField("NotAvailableReason", StringType()),
            StructField("Price_Off", StringType()),
            StructField("Priority_Store", StringType())        
        ])

        if sheet_names == "[]":
            files_to_read = [file_name]
        else:
            files_to_read = sheet_names[1:-1].split(",")
            files_to_read = [sh_name.strip("\\"").replace("(", "").replace(")", "").replace(" ","_") + ".csv" for sh_name in files_to_read]
        
        df = None

        for src_file in files_to_read:
            
            file_df = session.read\\
                .schema(df_schema)\\
                .option("skip_header",1)\\
                .option("field_delimiter", "\\u0001")\\
                .option("field_optionally_enclosed_by", "\\"") \\
                .csv("@" + stage_name+"/"+temp_stage_path+"/"+src_file)
            
            if not df:
                df = file_df
            else:
                df = df.unionByName(file_df)

        yearmo = file_name.split(".")[0].split("_")[-1]

        df = df.na.drop(how=''all'')

        df = df.withColumn("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        df = df.withColumn("FILE_NAME",lit(file_name).cast("string"))
        df = df.withColumn("YEARMO",lit(yearmo).cast("string"))

        final_df = df
                        
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

CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.INDSDL_RAW.IN_PERFECTSTORE_SOS_PREPROCESSING("PARAM" ARRAY)
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
        sheet_names     = Param[4]
                        
        df_schema = StructType([
            StructField("Visit_DateTime", StringType()),
            StructField("Region", StringType()),
            StructField("JnJRKAM", StringType()),
            StructField("JnJZM_Code", StringType()),
            StructField("JNJ_ABI_Code", StringType()),
            StructField("JnJSupervisor_Code", StringType()),
            StructField("ISP_Code", StringType()),
            StructField("JnJISP_Name", StringType()),
            StructField("Month", StringType()),
            StructField("Year", StringType()),
            StructField("Format", StringType()),
            StructField("Chain_Code", StringType()),
            StructField("Chain", StringType()),
            StructField("Store_Code", StringType()),
            StructField("Store_Name", StringType()),
            StructField("Category", StringType()),
            StructField("Prod_Facings", StringType()),
            StructField("Total_Facings", StringType()),
            StructField("Facing_Contribution", StringType()),
            StructField("Priority_Store", StringType())
        ])

        if sheet_names == "[]":
            files_to_read = [file_name]
        else:
            files_to_read = sheet_names[1:-1].split(",")
            files_to_read = [sh_name.strip("\\"").replace("(", "").replace(")", "").replace(" ","_") + ".csv" for sh_name in files_to_read]
        
        df = None

        for src_file in files_to_read:
            
            file_df = session.read\\
                .schema(df_schema)\\
                .option("skip_header",1)\\
                .option("field_delimiter", "\\u0001")\\
                .option("field_optionally_enclosed_by", "\\"") \\
                .csv("@" + stage_name+"/"+temp_stage_path+"/"+src_file)
            
            if not df:
                df = file_df
            else:
                df = df.unionByName(file_df)

        yearmo = file_name.split(".")[0].split("_")[-1]
        
        df = df.na.drop(how=''all'')
        
        df = df.withColumn("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        df = df.withColumn("FILE_NAME",lit(file_name).cast("string"))
        df = df.withColumn("YEARMO",lit(yearmo).cast("string"))
                        
        final_df = df
                        
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

CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.INDSDL_RAW.IN_POS_HG_PREPROCESSING("PARAM" ARRAY)
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

        retval = "before schema"
        df_schema = StructType([
                    StructField("Account_Name", StringType()),
                    StructField("Year", StringType()),
                    StructField("Month", StringType()),
                    StructField("Level", StringType()),
                    StructField("Store_Code", StringType()),
                    StructField("Subcategory", StringType()),
                    StructField("Article_Code", StringType()),
                    StructField("Sales_Value_Rs_", StringType()),
                    StructField("Sales_Qty", StringType())
                ])

        retval = "before read"

        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "\\u0001")\\
            .option("field_optionally_enclosed_by", "\\"") \\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        

        df = df.withColumn("FILE_NAME",lit(file_name).cast("string"))
        df = df.withColumn("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        df = df.withColumn("FILE_UPLOADED_DATE",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d")))
        df = df.withColumn("CRT_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        
        final_df = df.select("Account_Name", to_date(concat(col("Year"), lit("-"), col("Month"), lit("-15")),lit("YYYY-mon-DD")).as_("POST_DT"), "Level",  "Store_Code",  "Subcategory", "Article_Code", \\
                            "Sales_Value_Rs_", "Sales_Qty", "FILE_NAME", "RUN_ID", "FILE_UPLOADED_DATE", "CRT_DTTM" ).alias("final_df")

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

CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.INDSDL_RAW.IN_POS_MAX_PREPROCESSING("PARAM" ARRAY)
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
                    StructField("Account_Name", StringType()),
                    StructField("Year", StringType()),
                    StructField("Month", StringType()),
                    StructField("Level", StringType()),
                    StructField("Store_Code", StringType()),
                    StructField("Subcategory", StringType()),
                    StructField("Article_Code", StringType()),
                    StructField("Sales_Value_Rs_", StringType()),
                    StructField("Sales_Qty", StringType())
                ])

        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "\\u0001")\\
            .option("field_optionally_enclosed_by", "\\"") \\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        

        df = df.withColumn("FILE_NAME",lit(file_name).cast("string"))
        df = df.withColumn("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        df = df.withColumn("FILE_UPLOADED_DATE",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d")))
        df = df.withColumn("CRT_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        
        final_df = df.select("Account_Name", to_date(concat(col("Year"), lit("-"), col("Month"), lit("-15")),lit("YYYY-mon-DD")).as_("POST_DT"), \\
                            "Level",  "Store_Code",  "Subcategory", "Article_Code", "Sales_Value_Rs_", "Sales_Qty", \\
                            "FILE_NAME", "RUN_ID", "FILE_UPLOADED_DATE", "CRT_DTTM" ).alias("final_df")

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

CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.INDSDL_RAW.IN_RETAILER_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '

import snowflake.snowpark as snowpark
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DateType, DoubleType,TimestampType
from snowflake.snowpark.functions import col,lit,date_format, try_cast, trim, to_date, left,  right
import pandas as pd
from datetime import datetime
import pytz

def main(session: snowpark.Session, Param): 

    try:
    
        # Param = [
        #     ''retailer_20240517061542.csv'',
        #     ''INDSDL_RAW.DEV_LOAD_STAGE_ADLS'',
        #     ''dev/india_dna/master/retailer/in_retailer'',
        #     ''sdl_in_retailer''
        # ]
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        sch_name        = stage_name.split(''.'')[0]
        target_table    = sch_name+"."+Param[3]
    
        df_schema=StructType([
                StructField("cmpcode", StringType()),
                StructField("distcode", StringType()),
                StructField("distrbrcode", StringType()),
                StructField("rtrcode", StringType()),
                StructField("rtrtype", StringType()),
                StructField("rtrname", StringType()),
                StructField("rtruniquecode", StringType()),
                StructField("channelcode", StringType()),
                StructField("retlrgroupcode", StringType()),
                StructField("classcode", StringType()),
                StructField("rtrphoneno", StringType()),
                StructField("rtrcontactperson", StringType()),
                StructField("emailid", StringType()),
                StructField("regdate", StringType()),
                StructField("rtrlicno", StringType()),
                StructField("rtrlicexpirydate", StringType()),
                StructField("druglno", StringType()),
                StructField("rtrdrugexpirydate", StringType()),
                StructField("rtrcrbills", StringType()),
                StructField("rtrcrdays", StringType()),
                StructField("rtrcrlimit", StringType()),
                StructField("relationstatus", StringType()),
                StructField("parentcode", StringType()),
                StructField("status", StringType()),
                StructField("rtrlatitude", StringType()),
                StructField("rtrlongitude", StringType()),
                StructField("csrtrcode", StringType()),
                StructField("keyaccount", StringType()),
                StructField("rtrfoodlicno", StringType()),
                StructField("pannumber", StringType()),
                StructField("retailertype", StringType()),
                StructField("composite", StringType()),
                StructField("relatedparty", StringType()),
                StructField("statename", StringType()),
                StructField("lastmoddate", StringType()),
                StructField("createddt", StringType())
                ])
    
        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "\\u0001")\\
            .option("skip_blank_lines", True) \\
            .option("field_optionally_enclosed_by", None) \\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
    
        df = df.withColumn("FILE_NAME",lit(file_name).cast("string"))
        df = df.withColumn("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        df = df.withColumn("CRT_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
    
        final_df = df.select(\\
            "cmpcode", "distcode", "distrbrcode", "rtrcode", "rtrtype", "rtrname", "rtruniquecode", "channelcode", \\
            "retlrgroupcode", "classcode", "rtrphoneno", "rtrcontactperson", "emailid", \\
            try_cast(col("regdate"), DateType()).alias("regdate"),\\
            "rtrlicno", try_cast(col("rtrlicexpirydate"), DateType()).alias("rtrlicexpirydate"), \\
            "druglno", try_cast(col("rtrdrugexpirydate"), DateType()).alias("rtrdrugexpirydate"), \\
            try_cast(col("rtrcrbills"),IntegerType()).alias("rtrcrbills"), \\
            try_cast(col("rtrcrdays"),IntegerType()).alias("rtrcrdays"), \\
            try_cast(col("rtrcrlimit"), DoubleType()).alias("rtrcrlimit"), \\
            "relationstatus", "parentcode", "status", "rtrlatitude", "rtrlongitude", "csrtrcode", \\
            "keyaccount", "rtrfoodlicno", "pannumber", "retailertype", "composite", "relatedparty", "statename", \\
            try_cast(col("lastmoddate"), TimestampType()).alias("lastmoddate"),\\
            try_cast(col("createddt"), TimestampType()).alias("createddt"),\\
            "FILE_NAME", "RUN_ID", "CRT_DTTM" \\
        ).alias("final_df")
        
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

CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.INDSDL_RAW.IN_RETAILER_ROUTE_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '

import snowflake.snowpark as snowpark
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DateType, DoubleType,TimestampType
from snowflake.snowpark.functions import col,lit,date_format, try_cast, trim, to_date, left,  right
import pandas as pd
from datetime import datetime
import pytz

def main(session: snowpark.Session, Param): 

    try:
    
        # Param = [
        #     ''retailer_route_20240517061753.csv'',
        #     ''INDSDL_RAW.DEV_LOAD_STAGE_ADLS'',
        #     ''dev/india_dna/master/retailer_route/in_retailer_route'',
        #     ''sdl_in_retailer_route''
        # ]
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        sch_name        = stage_name.split(''.'')[0]
        target_table    = sch_name+"."+Param[3]
    
        df_schema=StructType([
                StructField("cmpcode", StringType()),
                StructField("distcode", StringType()),
                StructField("distrbrcode", StringType()),
                StructField("rtrcode", StringType()),
                StructField("RMCode", StringType()),
                StructField("RouteType", StringType()),
                StructField("CoverageSequence", StringType()),
                StructField("createddt", StringType())
                ])
    
        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "\\u0001")\\
            .option("skip_blank_lines", True) \\
            .option("field_optionally_enclosed_by", "\\"") \\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
    
        df = df.withColumn("FILE_NAME",lit(file_name).cast("string"))
        df = df.withColumn("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        df = df.withColumn("CRT_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
    
        final_df = df.select(\\
            "cmpcode", "distcode", "distrbrcode", "rtrcode", "RMCode", "RouteType", \\
            try_cast(col("CoverageSequence"),IntegerType()).alias("CoverageSequence"), \\
            try_cast(col("createddt"), TimestampType()).alias("createddt"),\\
            "FILE_NAME", "RUN_ID", "CRT_DTTM" \\
        ).alias("final_df")
        
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

CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.INDSDL_RAW.IN_RKEYACCCUSTOMER_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '

import snowflake.snowpark as snowpark
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DateType, DoubleType,TimestampType
from snowflake.snowpark.functions import col,lit,date_format, try_cast, trim, to_date, left,  right
import pandas as pd
from datetime import datetime
import pytz

def main(session: snowpark.Session, Param): 

    try:
    
        # Param = [
        #     ''route_20240517062034.csv'',
        #     ''INDSDL_RAW.DEV_LOAD_STAGE_ADLS'',
        #     ''dev/india_dna/master/route/in_route'',
        #     ''sdl_in_route''
        # ]
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        sch_name        = stage_name.split(''.'')[0]
        target_table    = sch_name+"."+Param[3]

        df_schema=StructType([
            StructField("CustomerCode", StringType(50)),
            StructField("CustomerName", StringType(50)),
            StructField("CustomerAddress1", StringType(250)),
            StructField("CustomerAddress2", StringType(250)),
            StructField("CustomerAddress3", StringType(250)),
            StructField("SAPID", StringType(50)),
            StructField("RegionCode", StringType(50)),
            StructField("ZoneCode", StringType(50)),
            StructField("TerritoryCode", StringType(50)),
            StructField("StateCode", StringType(50)),
            StructField("TownCode", StringType(50)),
            StructField("EmailID", StringType(50)),
            StructField("MobileLL", StringType(50)),
            StructField("IsActive", StringType(1)),
            StructField("WholesalerCode", StringType(50)),
            StructField("URC", StringType(50)),
            StructField("NKACStores", StringType(1)),
            StructField("ParentCustomerCode", StringType(50)),
            StructField("IsDirectAcct", StringType(1)),
            StructField("IsParent", StringType(1)),
            StructField("ABICode", StringType(50)),
            StructField("DistributorSAPID", StringType(50)),
            StructField("IsConfirm", StringType(1)),
            StructField("CreatedDate", StringType()),
            StructField("createdUserCode", StringType(50)),
            StructField("ModDt", StringType()),
            StructField("ModUserCode", StringType(50)),
            StructField("CreatedDt", StringType())
        ])
        
        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "\\u0001")\\
            .option("skip_blank_lines", True) \\
            .option("truncatecolumns",True) \\
            .option("field_optionally_enclosed_by", None) \\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
    
        df = df.withColumn("CRT_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))

        final_df = df.select(\\
            "CustomerCode", "CustomerName", "CustomerAddress1", "CustomerAddress2", "CustomerAddress3", "SAPID", "RegionCode", \\
            "ZoneCode", "TerritoryCode", "StateCode", "TownCode", "EmailID", "MobileLL", "IsActive", "WholesalerCode", "URC", \\
            "NKACStores", "ParentCustomerCode", "IsDirectAcct", "IsParent", "ABICode", "DistributorSAPID", "IsConfirm", \\
            try_cast(col("CreatedDate"), TimestampType()).alias("CreatedDate"),  \\
            "createdUserCode", \\
            try_cast(col("ModDt"), TimestampType()).alias("ModDt"),  \\
            "ModUserCode", \\
            try_cast(col("CreatedDt"), TimestampType()).alias("CreatedDt"),  \\
            "CRT_DTTM"\\
        ).alias("final_df")
               
    
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

CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.INDSDL_RAW.IN_ROUTE_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '

import snowflake.snowpark as snowpark
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DateType, DoubleType,TimestampType
from snowflake.snowpark.functions import col,lit,date_format, try_cast, trim, to_date, left,  right
import pandas as pd
from datetime import datetime
import pytz

def main(session: snowpark.Session, Param): 

    try:
    
        # Param = [
        #     ''route_20240517062034.csv'',
        #     ''INDSDL_RAW.DEV_LOAD_STAGE_ADLS'',
        #     ''dev/india_dna/master/route/in_route'',
        #     ''sdl_in_route''
        # ]
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        sch_name        = stage_name.split(''.'')[0]
        target_table    = sch_name+"."+Param[3]

        df_schema=StructType([
                StructField("cmpcode", StringType()),
                StructField("distcode", StringType()),
                StructField("distrbrcode", StringType()),
                StructField("RMCode", StringType()),
                StructField("RouteType", StringType()),
                StructField("RMName", StringType()),
                StructField("Distance", StringType()),
                StructField("VanRoute", StringType()),
                StructField("Status", StringType()),
                StructField("RMPopulation", StringType()),
                StructField("LocalUpCountry", StringType()),
                StructField("createddt", StringType())
                ])
    
        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "\\u0001")\\
            .option("skip_blank_lines", True) \\
            .option("field_optionally_enclosed_by", "\\"") \\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
    
        df = df.withColumn("FILE_NAME",lit(file_name).cast("string"))
        df = df.withColumn("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        df = df.withColumn("CRT_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))

        final_df = df.select(\\
            "cmpcode", "distcode", "distrbrcode", "RMCode", "RouteType", "RMName", \\
            try_cast(col("Distance"),IntegerType()).alias("Distance"), \\
            "VanRoute", "Status", \\
            try_cast(col("RMPopulation"),IntegerType()).alias("RMPopulation"), \\
            "LocalUpCountry", \\
            try_cast(col("createddt"), TimestampType()).alias("createddt"),\\
            "FILE_NAME", "RUN_ID", "CRT_DTTM" \\
        ).alias("final_df")
               
    
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

CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.INDSDL_RAW.IN_RRETAILERGEOEXTENSION_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '

import snowflake.snowpark as snowpark
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DateType, DoubleType,TimestampType
from snowflake.snowpark.functions import col,lit,date_format, try_cast, trim, to_date, left,  right
import pandas as pd
from datetime import datetime
import pytz

def main(session: snowpark.Session, Param): 

    try:
    
        # Param = [
        #     ''rretailergeoextension_20240517062034.csv'',
        #     ''INDSDL_RAW.DEV_LOAD_STAGE_ADLS'',
        #     ''dev/india_dna/master/rretailergeoextension/in_rretailergeoextension'',
        #     ''SDL_IN_RRETAILERGEOEXTENSION''
        # ]
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        sch_name        = stage_name.split(''.'')[0]
        target_table    = sch_name+"."+Param[3]

        df_schema=StructType([
            StructField("Cmpcode", StringType()),
            StructField("Distrcode", StringType()),
            StructField("Customercode", StringType()),
            StructField("Cmpcutomercode", StringType()),
            StructField("Distributorcustomercode", StringType()),
            StructField("Latitude", StringType()),
            StructField("Longitude", StringType()),
            StructField("TownName", StringType()),
            StructField("StateName", StringType()),
            StructField("DistrictName", StringType()),
            StructField("SubDistrictName", StringType()),
            StructField("Type", StringType()),
            StructField("VillageName", StringType()),
            StructField("Pincode", StringType()),
            StructField("UAcheck", StringType()),
            StructField("UAName", StringType()),
            StructField("Population", StringType()),
            StructField("PopStrata", StringType()),
            StructField("FinalPopulationwithUA", StringType()),
            StructField("Modifydate", StringType()),
            StructField("Createddate", StringType()),
            StructField("isDeleted", StringType()),
            StructField("ExtraField1", StringType()),
            StructField("ExtraField2", StringType()),
            StructField("ExtraField3", StringType()),
            StructField("createddt", StringType())
        ])
        
        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "\\u0001")\\
            .option("skip_blank_lines", True) \\
            .option("field_optionally_enclosed_by", None) \\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
    
        df = df.withColumn("FILE_NAME",lit(file_name).cast("string"))
        df = df.withColumn("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        df = df.withColumn("CRT_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))

        final_df = df.select( \\
            "Cmpcode", "Distrcode", "Customercode", "Cmpcutomercode", "Distributorcustomercode", "Latitude", "Longitude", \\
            "TownName", "StateName", "DistrictName", "SubDistrictName", "Type", "VillageName", \\
            try_cast(col("Pincode"), IntegerType()).alias("Pincode"), \\
            "UAcheck", "UAName", try_cast(col("Population"), IntegerType()).alias("Population"), \\
            "PopStrata", try_cast(col("FinalPopulationwithUA"), IntegerType()).alias("FinalPopulationwithUA"), \\
            try_cast(col("Modifydate"), TimestampType()).alias("Modifydate"), \\
            try_cast(col("Createddate"), TimestampType()).alias("Createddate"), \\
            "isDeleted", "ExtraField1", "ExtraField2", "ExtraField3", \\
            try_cast(col("createddt"), TimestampType()).alias("createddt"), \\
            "FILE_NAME","RUN_ID","CRT_DTTM" \\
        ).alias("final_df")
               
    
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

CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.INDSDL_RAW.IN_RRL_UDCMAPPING_PREPROCESSING("PARAM" ARRAY)
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
                    StructField("distCode", StringType()),
                    StructField("RsdCode", StringType()),
                    StructField("OutletCode", StringType()),
                    StructField("UserCode", StringType()),
                    StructField("UdcCode", StringType()),
                    StructField("CreatedDate", StringType()),
                    StructField("IsActive", StringType()),
                    StructField("IsDelFlag", StringType()),
                    StructField("RowId", StringType())
                ])

        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "|")\\
            .option("field_optionally_enclosed_by", "\\"") \\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        df = df.withColumn("FILE_NAME",lit(file_name).cast("string"))
        df = df.withColumn("CRT_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))

        final_df = df.select("distCode", "RsdCode", "OutletCode", "UserCode", "UdcCode", to_timestamp("CreatedDate", lit("YYYY/MM/DD HH24:MI:SS")).as_("CreatedDate"), \\
            "IsActive", "IsDelFlag", "RowId", "FILE_NAME", "CRT_DTTM" ).alias("final_df")

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

CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.INDSDL_RAW.IN_RRL_USERMASTER_PREPROCESSING("PARAM" ARRAY)
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
                    StructField("UserID", StringType()),
                    StructField("UserCode", StringType()),
                    StructField("Login", StringType()),
                    StructField("Password", StringType()),
                    StructField("EUserName", StringType()),
                    StructField("UserLevel", StringType()),
                    StructField("ParentID", StringType()),
                    StructField("IsActive", StringType()),
                    StructField("TeritoryID", StringType()),
                    StructField("ABNumber", StringType()),
                    StructField("ForumCode", StringType()),
                    StructField("RegionId", StringType()),
                    StructField("EmailID", StringType()),
                    StructField("CurrentVersion", StringType()),
                    StructField("UpdateVersion", StringType()),
                    StructField("IMEI", StringType()),
                    StructField("MobileNo", StringType()),
                    StructField("LocationID", StringType()),
                    StructField("IsHHT", StringType()),
                    StructField("User_CreatedDate", StringType()),
                    StructField("DistUserId", StringType()),
                    StructField("FreezeDay", StringType())
                ])

        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "|")\\
            .option("field_optionally_enclosed_by", "\\"") \\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        df = df.withColumn("FILE_NAME",lit(file_name).cast("string"))
        df = df.withColumn("CRT_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))

        final_df = df.select("UserID", "UserCode", "Login", "Password", "EUserName", "UserLevel", "ParentID", "IsActive", \\
                            "TeritoryID", "ABNumber", "ForumCode", "RegionId", "EmailID", "CurrentVersion", "UpdateVersion", \\
                            "IMEI", "MobileNo", "LocationID", "IsHHT", "User_CreatedDate", "DistUserId", "FreezeDay", \\
                            "FILE_NAME", "CRT_DTTM" ).alias("final_df")

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
CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.INDSDL_RAW.IN_RRSRDISTRIBUTOR_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '

import snowflake.snowpark as snowpark
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DateType, DoubleType,TimestampType
from snowflake.snowpark.functions import col,lit,date_format, try_cast, trim, to_date, left,  right
import pandas as pd
from datetime import datetime
import pytz

def main(session: snowpark.Session, Param): 

    try:
    
        # Param = [
        #     ''rrsrdistributor_20240517062034.csv'',
        #     ''INDSDL_RAW.DEV_LOAD_STAGE_ADLS'',
        #     ''dev/india_dna/master/rrsrdistributor/in_rrsrdistributor'',
        #     ''SDL_IN_RRSRDISTRIBUTOR''
        # ]
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        sch_name        = stage_name.split(''.'')[0]
        target_table    = sch_name+"."+Param[3]
       
        df_schema=StructType([
            StructField("CmpCode", StringType()),
            StructField("RSRCode", StringType()),
            StructField("DistrCode", StringType()),
            StructField("DateOfJoin", StringType()),
            StructField("IsActive", StringType()),
            StructField("ModUserCode", StringType()),
            StructField("ModDt", StringType()),
            StructField("CreatedDt", StringType())
        ])
        
        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "\\u0001")\\
            .option("skip_blank_lines", True) \\
            .option("field_optionally_enclosed_by", None) \\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
    
        df = df.withColumn("FILE_NAME",lit(file_name).cast("string"))
        df = df.withColumn("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        df = df.withColumn("CRT_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))

        final_df = df.select( \\
            "CmpCode", "RSRCode", "DistrCode", \\
            try_cast(col("DateOfJoin"), DateType()).alias("DateOfJoin"),  \\
            "IsActive", "ModUserCode", try_cast(col("ModDt"), TimestampType()).alias("ModDt"),  \\
            try_cast(col("CreatedDt"), TimestampType()).alias("CreatedDt"), \\
            "FILE_NAME","RUN_ID","CRT_DTTM" \\
        ).alias("final_df")
               
    
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

CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.INDSDL_RAW.IN_RRSRHEADER_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '

import snowflake.snowpark as snowpark
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DateType, DoubleType,TimestampType
from snowflake.snowpark.functions import col,lit,date_format, try_cast, trim, to_date, left,  right
import pandas as pd
from datetime import datetime
import pytz

def main(session: snowpark.Session, Param): 

    try:
    
        # Param = [
        #     ''rrsrheader_20240517062034.csv'',
        #     ''INDSDL_RAW.DEV_LOAD_STAGE_ADLS'',
        #     ''dev/india_dna/master/rrsrheader/in_rrsrheader'',
        #     ''SDL_IN_RRSRHEADER''
        # ]
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        sch_name        = stage_name.split(''.'')[0]
        target_table    = sch_name+"."+Param[3]

        df_schema=StructType([
            StructField("CmpCode", StringType()),
            StructField("RSRCode", StringType()),
            StructField("RSRName", StringType()),
            StructField("EmailId", StringType()),
            StructField("phoneno", StringType()),
            StructField("DateOfBirth", StringType()),
            StructField("DateOfJoin", StringType()),
            StructField("IsActive", StringType()),
            StructField("ModUserCode", StringType()),
            StructField("ModDt", StringType()),
            StructField("ApprovalStatus", StringType()),
            StructField("dailyAllowance", StringType()),
            StructField("monthlySalary", StringType()),
            StructField("aadharNo", StringType()),
            StructField("ImagePath", StringType()),
            StructField("CreatedDt", StringType())
        ])
        
        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "\\u0001")\\
            .option("skip_blank_lines", True) \\
            .option("field_optionally_enclosed_by", None) \\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
    
        df = df.withColumn("FILE_NAME",lit(file_name).cast("string"))
        df = df.withColumn("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        df = df.withColumn("CRT_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))

        final_df = df.select( \\
            "CmpCode", "RSRCode", "RSRName", "EmailId", "phoneno", \\
            try_cast(col("DateOfBirth"), DateType()).alias("DateOfBirth"),  \\
            try_cast(col("DateOfJoin"), DateType()).alias("DateOfJoin"),  \\
            "IsActive", "ModUserCode", try_cast(col("ModDt"), TimestampType()).alias("ModDt"),  \\
            "ApprovalStatus", \\
            try_cast(col("dailyAllowance"), DoubleType()).alias("dailyAllowance"), \\
            try_cast(col("monthlySalary"), DoubleType()).alias("monthlySalary"), \\
            "aadharNo", "ImagePath", \\
            try_cast(col("CreatedDt"), TimestampType()).alias("CreatedDt"), \\
            "FILE_NAME","RUN_ID","CRT_DTTM" \\
        ).alias("final_df")
               
    
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

CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.INDSDL_RAW.IN_RTLDISTRIBUTOR_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '

import snowflake.snowpark as snowpark
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DateType, DoubleType,TimestampType
from snowflake.snowpark.functions import col,lit,date_format, try_cast, trim, to_date, left,  right
import pandas as pd
from datetime import datetime
import pytz

def main(session: snowpark.Session, Param): 

    try:
    
        # Param = [
        #     ''rtldistributor_20240517062034.csv'',
        #     ''INDSDL_RAW.DEV_LOAD_STAGE_ADLS'',
        #     ''dev/india_dna/master/rtldistributor/in_rtldistributor'',
        #     ''SDL_IN_RTLDISTRIBUTOR''
        # ]
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        sch_name        = stage_name.split(''.'')[0]
        target_table    = sch_name+"."+Param[3]

        df_schema=StructType([
            StructField("CmpCode", StringType()),
            StructField("TLCode", StringType()),
            StructField("DistrCode", StringType()),
            StructField("DateOfJoin", StringType()),
            StructField("IsActive", StringType()),
            StructField("ModUserCode", StringType()),
            StructField("ModDt", StringType()),
            StructField("CreatedDt", StringType())
        ])
        
        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "\\u0001")\\
            .option("skip_blank_lines", True) \\
            .option("field_optionally_enclosed_by", None) \\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
    
        df = df.withColumn("FILE_NAME",lit(file_name).cast("string"))
        df = df.withColumn("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        df = df.withColumn("CRT_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))

        final_df = df.select( \\
            "CmpCode", "TLCode", "DistrCode", \\
            try_cast(col("DateOfJoin"), DateType()).alias("DateOfJoin"),  \\
            "IsActive", "ModUserCode", try_cast(col("ModDt"), TimestampType()).alias("ModDt"),  \\
            try_cast(col("CreatedDt"), TimestampType()).alias("CreatedDt"), \\
            "FILE_NAME","RUN_ID","CRT_DTTM" \\
        ).alias("final_df")
               
    
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

CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.INDSDL_RAW.IN_RTLHEADER_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '

import snowflake.snowpark as snowpark
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DateType, DoubleType,TimestampType
from snowflake.snowpark.functions import col,lit,date_format, try_cast, trim, to_date, left,  right
import pandas as pd
from datetime import datetime
import pytz

def main(session: snowpark.Session, Param): 

    try:
    
        # Param = [
        #     ''rtlheader_20240517062034.csv'',
        #     ''INDSDL_RAW.DEV_LOAD_STAGE_ADLS'',
        #     ''dev/india_dna/master/rtlheader/in_rtlheader'',
        #     ''SDL_IN_RTLHEADER''
        # ]
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        sch_name        = stage_name.split(''.'')[0]
        target_table    = sch_name+"."+Param[3]

        df_schema=StructType([
            StructField("CmpCode", StringType()),
            StructField("TLCode", StringType()),
            StructField("TLName", StringType()),
            StructField("EmailId", StringType()),
            StructField("phoneno", StringType()),
            StructField("DateOfBirth", StringType()),
            StructField("DateOfJoin", StringType()),
            StructField("IsActive", StringType()),
            StructField("ModUserCode", StringType()),
            StructField("ModDt", StringType()),
            StructField("ApprovalStatus", StringType()),
            StructField("dailyAllowance", StringType()),
            StructField("monthlySalary", StringType()),
            StructField("aadharNo", StringType()),
            StructField("ImagePath", StringType()),
            StructField("CreatedDt", StringType())
        ])
        
        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "\\u0001")\\
            .option("skip_blank_lines", True) \\
            .option("field_optionally_enclosed_by", None) \\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
    
        df = df.withColumn("FILE_NAME",lit(file_name).cast("string"))
        df = df.withColumn("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        df = df.withColumn("CRT_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))

        final_df = df.select( \\
            "CmpCode", "TLCode", "TLName", "EmailId", "phoneno", \\
            try_cast(col("DateOfBirth"), DateType()).alias("DateOfBirth"),  \\
            try_cast(col("DateOfJoin"), DateType()).alias("DateOfJoin"),  \\
            "IsActive", "ModUserCode", try_cast(col("ModDt"), TimestampType()).alias("ModDt"),  \\
            "ApprovalStatus", \\
            try_cast(col("dailyAllowance"), DoubleType()).alias("dailyAllowance"), \\
            try_cast(col("monthlySalary"), DoubleType()).alias("monthlySalary"), \\
            "aadharNo", "ImagePath", \\
            try_cast(col("CreatedDt"), TimestampType()).alias("CreatedDt"), \\
            "FILE_NAME","RUN_ID","CRT_DTTM" \\
        ).alias("final_df")
               
    
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

CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.INDSDL_RAW.IN_RTLSALESMAN_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '

import snowflake.snowpark as snowpark
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DateType, DoubleType,TimestampType
from snowflake.snowpark.functions import col,lit,date_format, try_cast, trim, to_date, left,  right
import pandas as pd
from datetime import datetime
import pytz

def main(session: snowpark.Session, Param): 

    try:
    
        # Param = [
        #     ''rtlsalesman_20240517062034.csv'',
        #     ''INDSDL_RAW.DEV_LOAD_STAGE_ADLS'',
        #     ''dev/india_dna/master/rtlsalesman/in_rtlsalesman'',
        #     ''SDL_IN_RTLSALESMAN''
        # ]
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        sch_name        = stage_name.split(''.'')[0]
        target_table    = sch_name+"."+Param[3]

        df_schema=StructType([
            StructField("CmpCode", StringType()),
            StructField("TLCode", StringType()),
            StructField("DistrCode", StringType()),
            StructField("DistrBrCode", StringType()),
            StructField("SalesmanCode", StringType()),
            StructField("DateOfJoin", StringType()),
            StructField("IsActive", StringType()),
            StructField("ModUserCode", StringType()),
            StructField("ModDt", StringType()),
            StructField("CreatedDt", StringType())
        ])
        
        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "\\u0001")\\
            .option("skip_blank_lines", True) \\
            .option("field_optionally_enclosed_by", None) \\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
    
        df = df.withColumn("FILE_NAME",lit(file_name).cast("string"))
        df = df.withColumn("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        df = df.withColumn("CRT_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))

        final_df = df.select( \\
            "CmpCode", "TLCode", "DistrCode", "DistrBrCode", "SalesmanCode", \\
            try_cast(col("DateOfJoin"), DateType()).alias("DateOfJoin"),  \\
            "IsActive", "ModUserCode", try_cast(col("ModDt"), TimestampType()).alias("ModDt"),  \\
            try_cast(col("CreatedDt"), TimestampType()).alias("CreatedDt"), \\
            "FILE_NAME","RUN_ID","CRT_DTTM" \\
        ).alias("final_df")
               
    
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
CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.INDSDL_RAW.IN_SALESMANSKUMAPPING_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '
import snowflake.snowpark as snowpark
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DateType, DoubleType,TimestampType,DecimalType
from snowflake.snowpark.functions import col,lit,date_format, try_cast, trim, to_date, left,  right
import pandas as pd
from datetime import datetime
import pytz

def main(session: snowpark.Session,Param): 

    try:
    
        # Param = [
        #     ''salesmanskulinemapping'',
        #     ''INDSDL_RAW.DEV_LOAD_STAGE_ADLS'',
        #     ''dev/india_dna/master/salesmanskulinemapping/in_salesmanskulinemapping'',
        #     ''sdl_xdm_salesmanskulinemapping''
        # ]
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        sch_name        = stage_name.split(''.'')[0]
        target_table    = sch_name+"."+Param[3]
    
        df_schema=StructType([
                StructField(''CMPCODE'', StringType()),
                StructField(''DISTRCODE'', StringType()),
                StructField(''DISTRBRCODE'', StringType()),
                StructField(''SALESMANCODE'', StringType()),
                StructField(''SKULINE'', StringType()),
                StructField(''SKUHIERLEVELCODE'', StringType()),
                StructField(''SKUHIERVALUECODE'', StringType()),
                StructField(''MODDT'', StringType()),
                StructField(''CREATEDDT'', StringType()),
                StructField(''MOTHERSKUCODE'', StringType())
                ])
    
        df = session.read \\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "\\u0001")\\
            .option("skip_blank_lines", True) \\
            .option("field_optionally_enclosed_by", "\\"") \\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
    
        df = df.withColumn("FILE_NAME",lit(file_name).cast("string"))
        df = df.withColumn("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Kolkata")).strftime("%Y%m%d%H%M%S")))
        df = df.withColumn("CRT_DTTM",lit(datetime.now(pytz.timezone("Asia/Kolkata")).strftime("%Y-%m-%d %H:%M:%S")))

                                    
        final_df = df.select(\\
            trim(col(''CMPCODE'')).alias(''CMPCODE''),
            trim(col(''DISTRCODE'')).alias(''DISTRCODE''),
            trim(col(''DISTRBRCODE'')).alias(''DISTRBRCODE''),
            trim(col(''SALESMANCODE'')).alias(''SALESMANCODE''),
            trim(col(''SKULINE'')).alias(''SKULINE''),
            try_cast(col(''SKUHIERLEVELCODE''),IntegerType()).alias(''SKUHIERLEVELCODE''),
            trim(col(''SKUHIERVALUECODE'')).alias(''SKUHIERVALUECODE''),
            trim(col(''MODDT'')).alias(''MODDT''),
            try_cast(col("CREATEDDT"), TimestampType()).alias("CREATEDDT"),\\
            trim(col(''MOTHERSKUCODE'')).alias(''MOTHERSKUCODE''),
            "FILE_NAME", "RUN_ID",
            try_cast(col("CRT_DTTM"), TimestampType()).alias("CRT_DTTM")
        ).alias("final_df")
        
        final_df.write.mode("append").saveAsTable(target_table)
                                    
        return "Success"
        
    except KeyError as key_error:
        # Handle KeyError (missing columns) here
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message
        
    except Exception as e:
        # Handle exceptions here
        error_message = f"Error: {str(e)}" +str(df.columns)
        return error_message';

CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.INDSDL_RAW.IN_SALESMAN_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '

import snowflake.snowpark as snowpark
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DateType, DoubleType,TimestampType
from snowflake.snowpark.functions import col,lit,date_format, try_cast, trim, to_date, left,  right
import pandas as pd
from datetime import datetime
import pytz

def main(session: snowpark.Session, Param): 

    try:
    
        # Param = [
        #     ''salesman_20240517061916.csv'',
        #     ''INDSDL_RAW.DEV_LOAD_STAGE_ADLS'',
        #     ''dev/india_dna/master/salesman/in_salesman'',
        #     ''sdl_in_salesman''
        # ]
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        sch_name        = stage_name.split(''.'')[0]
        target_table    = sch_name+"."+Param[3]
    
    
        df_schema=StructType([
                StructField("cmpcode", StringType()),
                StructField("distcode", StringType()),
                StructField("distrbrcode", StringType()),
                StructField("SMCode", StringType()),
                StructField("SMName", StringType()),
                StructField("smphoneno", StringType()),
                StructField("SMEmail", StringType()),
                StructField("RDSSMType", StringType()),
                StructField("SMDailyAllowance", StringType()),
                StructField("SMMonthlySalary", StringType()),
                StructField("SMMktCredit", StringType()),
                StructField("SMCreditDays", StringType()),
                StructField("Status", StringType()),
                StructField("ModUserCode", StringType()),
                StructField("ModDt", StringType()),
                StructField("AadhaarNo", StringType()),
                StructField("UniqueSalesCode", StringType()),
                StructField("createddt", StringType())
                ])
    
        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "\\u0001")\\
            .option("skip_blank_lines", True) \\
            .option("field_optionally_enclosed_by", "\\"") \\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
    
        df = df.withColumn("FILE_NAME",lit(file_name).cast("string"))
        df = df.withColumn("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        df = df.withColumn("CRT_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
    
        final_df = df.select(\\
            "cmpcode", "distcode", "distrbrcode", "SMCode", "SMName", "smphoneno", "SMEmail", "RDSSMType", \\
            try_cast(col("SMDailyAllowance"), DoubleType()).alias("SMDailyAllowance"), \\
            try_cast(col("SMMonthlySalary"), DoubleType()).alias("SMMonthlySalary"), \\
            try_cast(col("SMMktCredit"), DoubleType()).alias("SMMktCredit"), \\
            try_cast(col("SMCreditDays"),IntegerType()).alias("SMCreditDays"), \\
            "Status", "ModUserCode", \\
            try_cast(col("ModDt"), DateType()).alias("ModDt"),\\
            "AadhaarNo", "UniqueSalesCode", \\
            try_cast(col("createddt"), TimestampType()).alias("createddt"),\\
            "FILE_NAME", "RUN_ID", "CRT_DTTM" \\
        ).alias("final_df")
        
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

CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.INDSDL_RAW.IN_SALESMAN_ROUTE_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '

import snowflake.snowpark as snowpark
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DateType, DoubleType,TimestampType
from snowflake.snowpark.functions import col,lit,date_format, try_cast, trim, to_date, left,  right
import pandas as pd
from datetime import datetime
import pytz

def main(session: snowpark.Session, Param): 

    try:
    
        # Param = [
        #     ''salesman_route_20240517061954.csv'',
        #     ''INDSDL_RAW.DEV_LOAD_STAGE_ADLS'',
        #     ''dev/india_dna/master/salesman_route/in_salesman_route'',
        #     ''sdl_in_salesman_route''
        # ]
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        sch_name        = stage_name.split(''.'')[0]
        target_table    = sch_name+"."+Param[3]
    
        df_schema=StructType([
                StructField("cmpcode", StringType()),
                StructField("DistrCode", StringType()),
                StructField("distrbrcode", StringType()),
                StructField("SalesmanCode", StringType()),
                StructField("RouteCode", StringType()),
                StructField("createddt", StringType())
                ])
    
        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "\\u0001")\\
            .option("skip_blank_lines", True) \\
            .option("field_optionally_enclosed_by", "\\"") \\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
    
        df = df.withColumn("FILE_NAME",lit(file_name).cast("string"))
        df = df.withColumn("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        df = df.withColumn("CRT_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
    
        final_df = df.select(\\
            "cmpcode", "DistrCode", "distrbrcode", "SalesmanCode", "RouteCode", \\
            try_cast(col("createddt"), TimestampType()).alias("createddt"),\\
            "FILE_NAME", "RUN_ID", "CRT_DTTM" \\
        ).alias("final_df")
        
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

CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.INDSDL_RAW.IN_SALESMAN_TARGET_PREPROCESSING("PARAM" ARRAY)
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
            StructField("DistCode", StringType()),
            StructField("SMCode", StringType()),
            StructField("SM_Target", StringType()),
            StructField("Brand_Focus", StringType()),
            StructField("Measure_Type", StringType()),
            StructField("Channel", StringType()),
            StructField("YY", StringType()),
            StructField("MM", StringType())
        ])
        
        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "\\u0001")\\
            .option("field_optionally_enclosed_by", "\\"") \\
            .csv("@" + stage_name + "/" + temp_stage_path + "/" + file_name)
        
        final_df = df

        if final_df.count()==0:
            return "No Data in file"

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
CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.INDSDL_RAW.IN_SALES_HEIRARCHY_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '
import snowflake.snowpark as snowpark
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DateType, DoubleType,TimestampType,DecimalType
from snowflake.snowpark.functions import col,lit,date_format, try_cast, trim, to_date, left,  right
import pandas as pd
from datetime import datetime
import pytz

def main(session: snowpark.Session,Param): 

    try:
    
        # Param = [
        #     ''salesheirarchy'',
        #     ''INDSDL_RAW.DEV_LOAD_STAGE_ADLS'',
        #     ''dev/india_dna/master/salesheirarchy/in_salesheirarchy'',
        #     ''sdl_xdm_salesheirarchy''
        # ]
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        sch_name        = stage_name.split(''.'')[0]
        target_table    = sch_name+"."+Param[3]
    
        df_schema=StructType([
                StructField(''RSMCODE'', StringType()),
                StructField(''RSMNAME'', StringType()),
                StructField(''RSM_FLMASMCODE'', StringType()),
                StructField(''FLMASMCODE'', StringType()),
                StructField(''FLMASMNAME'', StringType()),
                StructField(''FLMASM_ABICODE'', StringType()),
                StructField(''ABICODE'', StringType()),
                StructField(''ABINAME'', StringType()),
                StructField(''ABI_DISTRIBUTORCODE'', StringType()),
                StructField(''DISTRIBUTORCODE'', StringType()),
                StructField(''DISTRIBUTORNAME'', StringType()),
                StructField(''CREATEDDT'', StringType()),
                ])
    
        df = session.read \\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "\\u0001")\\
            .option("skip_blank_lines", True) \\
            .option("field_optionally_enclosed_by", "\\"") \\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
    
        df = df.withColumn("FILE_NAME",lit(file_name).cast("string"))
        df = df.withColumn("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Kolkata")).strftime("%Y%m%d%H%M%S")))
        df = df.withColumn("CRT_DTTM",lit(datetime.now(pytz.timezone("Asia/Kolkata")).strftime("%Y-%m-%d %H:%M:%S")))

                                    
        final_df = df.select(\\
            trim(col(''RSMCODE'')).alias(''RSMCODE''),\\
            trim(col(''RSMNAME'')).alias(''RSMNAME''),\\
            trim(col(''RSM_FLMASMCODE'')).alias(''RSM_FLMASMCODE''),\\
            trim(col(''FLMASMCODE'')).alias(''FLMASMCODE''),\\
            trim(col(''FLMASMNAME'')).alias(''FLMASMNAME''),\\
            trim(col(''FLMASM_ABICODE'')).alias(''FLMASM_ABICODE''),\\
            trim(col(''ABICODE'')).alias(''ABICODE''),\\
            trim(col(''ABINAME'')).alias(''ABINAME''),\\
            trim(col(''ABI_DISTRIBUTORCODE'')).alias(''ABI_DISTRIBUTORCODE''),\\
            trim(col(''DISTRIBUTORCODE'')).alias(''DISTRIBUTORCODE''),\\
            trim(col(''DISTRIBUTORNAME'')).alias(''DISTRIBUTORNAME''),\\
            try_cast(col("CREATEDDT"), TimestampType()).alias("CREATEDDT"),\\
            "FILE_NAME", "RUN_ID",
            try_cast(col("CRT_DTTM"), TimestampType()).alias("CRT_DTTM")
        ).alias("final_df")
        
        final_df.write.mode("append").saveAsTable(target_table)
                                    
        return "Success"
        
    except KeyError as key_error:
        # Handle KeyError (missing columns) here
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message
        
    except Exception as e:
        # Handle exceptions here
        error_message = f"Error: {str(e)}" +str(df.columns)
        return error_message';

CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.INDSDL_RAW.IN_SKU_RECOM_FLAG_PREPROCESSING("PARAM" ARRAY)
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
            StructField("PRODUCT", StringType()),
            StructField("OUTLET", StringType()),
            StructField("DISTRIBUTOR", StringType()),
            StructField("OOS_FLAG", StringType()),
            StructField("MS_FLAG", StringType()),
            StructField("CS_FLAG", StringType()),
            StructField("SOQ", StringType()),
            StructField("URCCode", StringType()),
            StructField("CsrtrCode", StringType()),
            StructField("RT_Code", StringType()),
            StructField("SM_Code", StringType())
        ])
        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "|")\\
            .option("field_optionally_enclosed_by", "\\"") \\
            .csv("@" + stage_name + "/" + temp_stage_path + "/" + file_name)
            
        yearmo = file_name.split("_")[1]
        yearmo = yearmo[2:]+yearmo[:2]

        df = df.withColumn("YEARMO",lit(yearmo).cast("string"))

        final_df = df.select( \\
            "YEARMO", "PRODUCT", "OUTLET", "DISTRIBUTOR", "OOS_FLAG", "MS_FLAG", \\
            "CS_FLAG", "SOQ", "URCCode", "CsrtrCode", "RT_Code", "SM_Code" \\
        ).alias("final_df")
                        
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
CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.INDSDL_RAW.IN_SSS_SCORECARD_DATA_PREPROCESSING("PARAM" ARRAY)
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
                    StructField("Program_Type", StringType()),
                    StructField("JNJ_ID", StringType()),
                    StructField("RS_ID", StringType()),
                    StructField("Outlet_Name", StringType()),
                    StructField("Region", StringType()),
                    StructField("Zone", StringType()),
                    StructField("Territory", StringType()),
                    StructField("City", StringType()),
                    StructField("Franchise", StringType()),
                    StructField("KPI", StringType()),
                    StructField("Quarter", StringType()),
                    StructField("Year", StringType()),
                    StructField("Target", StringType()),
                    StructField("Actual", StringType())
                ])

        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "\\u0001")\\
            .option("field_optionally_enclosed_by", "\\"") \\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        df = df.withColumn("CRT_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        df = df.withColumn("FILE_NAME",lit(file_name).cast("string"))
        df = df.withColumn("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))

        final_df = df.select("Program_Type", "JNJ_ID", "RS_ID", "Outlet_Name", "Region", "Zone", "Territory", "City", \\
                            "Franchise", "KPI", "Quarter", "Year", "Target", "Actual", \\
                            "CRT_DTTM", "FILE_NAME", "RUN_ID").alias("final_df")

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

CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.INDSDL_RAW.IN_SUPPLIER_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '
import snowflake.snowpark as snowpark
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DateType, DoubleType,TimestampType,DecimalType
from snowflake.snowpark.functions import col,lit,date_format, try_cast, trim, to_date, left,  right
import pandas as pd
from datetime import datetime
import pytz

def main(session: snowpark.Session,Param): 

    try:
    
        # Param = [
        #     ''supplier'',
        #     ''INDSDL_RAW.DEV_LOAD_STAGE_ADLS'',
        #     ''dev/india_dna/master/supplier/in_supplier'',
        #     ''sdl_xdm_supplier''
        # ]
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        sch_name        = stage_name.split(''.'')[0]
        target_table    = sch_name+"."+Param[3]
    
        df_schema=StructType([
                StructField(''CMPCODE'', StringType()),
                StructField(''SUPCODE'', StringType()),
                StructField(''SUPTYPE'', StringType()),
                StructField(''SUPNAME'', StringType()),
                StructField(''SUPADDR1'', StringType()),
                StructField(''SUPADDR2'', StringType()),
                StructField(''SUPADDR3'', StringType()),
                StructField(''CITY'', StringType()),
                StructField(''STATE'', StringType()),
                StructField(''COUNTRY'', StringType()),
                StructField(''GSTSTATECODE'', StringType()),
                StructField(''SUPPLIERGSTIN'', StringType()),
                StructField(''CREATEDDT'', StringType())
                ])
    
        df = session.read \\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "\\u0001")\\
            .option("skip_blank_lines", True) \\
            .option("field_optionally_enclosed_by", "\\"") \\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
    
        df = df.withColumn("FILE_NAME",lit(file_name).cast("string"))
        df = df.withColumn("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Kolkata")).strftime("%Y%m%d%H%M%S")))
        df = df.withColumn("CRT_DTTM",lit(datetime.now(pytz.timezone("Asia/Kolkata")).strftime("%Y-%m-%d %H:%M:%S")))

                                    
        final_df = df.select(\\
            trim(col(''CMPCODE'')).alias(''CMPCODE''),\\
            trim(col(''SUPCODE'')).alias(''SUPCODE''),\\
            trim(col(''SUPTYPE'')).alias(''SUPTYPE''),\\
            trim(col(''SUPNAME'')).alias(''SUPNAME''),\\
            trim(col(''SUPADDR1'')).alias(''SUPADDR1''),\\
            trim(col(''SUPADDR2'')).alias(''SUPADDR2''),\\
            trim(col(''SUPADDR3'')).alias(''SUPADDR3''),\\
            trim(col(''CITY'')).alias(''CITY''),\\
            trim(col(''STATE'')).alias(''STATE''),\\
            trim(col(''COUNTRY'')).alias(''COUNTRY''),\\
            trim(col(''GSTSTATECODE'')).alias(''GSTSTATECODE''),\\
            trim(col(''SUPPLIERGSTIN'')).alias(''SUPPLIERGSTIN''),\\
            try_cast(col("CREATEDDT"), TimestampType()).alias("CREATEDDT"),\\
            "FILE_NAME", "RUN_ID",
            try_cast(col("CRT_DTTM"), TimestampType()).alias("CRT_DTTM")
        ).alias("final_df")
        
        final_df.write.mode("append").saveAsTable(target_table)
                                    
        return "Success"
        
    except KeyError as key_error:
        # Handle KeyError (missing columns) here
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message
        
    except Exception as e:
        # Handle exceptions here
        error_message = f"Error: {str(e)}" +str(df.columns)
        return error_message';

CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.INDSDL_RAW.IN_WINCULUM_DAILYSALES_PREPROCESSING("PARAM" ARRAY)
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
                    StructField("DistCode", StringType()),
                    StructField("salinvdate", StringType()),
                    StructField("salinvno", StringType()),
                    StructField("RtrCode", StringType()),
                    StructField("Productcode", StringType()),
                    StructField("PrdQty", StringType()),
                    StructField("NR", StringType()),
                    StructField("Total_Price", StringType()),
                    StructField("Tax", StringType())
                ])
 
        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "\\u0001")\\
            .option("skip_blank_lines", True) \\
            .option("field_optionally_enclosed_by", "\\"") \\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        df2=df.filter(col("DistCode").is_not_null()) and df.filter(col("salinvdate").is_not_null())  and df.filter(col("salinvno").is_not_null()) \\
            and df.filter(col("RtrCode").is_not_null()) and df.filter(col("Productcode").is_not_null())
            
        df2 = df2.withColumn("FILE_NAME",lit(file_name).cast("string"))
        df2 = df2.withColumn("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        df2 = df2.withColumn("CRT_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        
        final_df = df2.select("DistCode", "salinvdate", "salinvno", "RtrCode", "Productcode", "PrdQty", "NR", "Total_Price", "Tax", \\
                            "FILE_NAME", "RUN_ID", "CRT_DTTM" ).alias("final_df")

                        
                            
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

CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.INDSDL_RAW.IN_WINCULUM_SALESRETURN_PREPROCESSING("PARAM" ARRAY)
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
                    StructField("DistCode", StringType()),
                    StructField("SRNDate", StringType()),
                    StructField("SRNRefNo", StringType()),
                    StructField("RtrCode", StringType()),
                    StructField("Productcode", StringType()),
                    StructField("PrdQty", StringType()),
                    StructField("NR", StringType()),
                    StructField("Total_Price", StringType()),
                    StructField("Tax", StringType())
                ])
 
        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "\\u0001")\\
            .option("skip_blank_lines", True) \\
            .option("field_optionally_enclosed_by", "\\"") \\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        df2=df.filter(col("DistCode").is_not_null()) and df.filter(col("SRNDate").is_not_null())  and df.filter(col("SRNRefNo").is_not_null()) \\
            and df.filter(col("RtrCode").is_not_null()) and df.filter(col("Productcode").is_not_null())
            
        df2 = df2.withColumn("FILE_NAME",lit(file_name).cast("string"))
        df2 = df2.withColumn("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        df2 = df2.withColumn("CRT_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        
        final_df = df2.select("DistCode", "SRNDate", "SRNRefNo", "RtrCode", "Productcode", "PrdQty", "NR", "Total_Price", "Tax", \\
                            "FILE_NAME", "RUN_ID", "CRT_DTTM" ).alias("final_df")

                        
                            
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

CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.INDSDL_RAW.M_SDL_ITTARGET_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS 'import snowflake.snowpark as snowpark
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DateType, DoubleType,TimestampType,DecimalType
from snowflake.snowpark.functions import col,lit,date_format, try_cast, trim, to_date, left,  right, is_integer, to_variant, not_, is_null
import pandas as pd
from datetime import datetime
import pytz

def main(session: snowpark.Session,Param): 

    try:
    
        # Param = [
        #     ''it_target'',
        #     ''INDSDL_RAW.DEV_LOAD_STAGE_ADLS'',
        #     ''dev/master_target'',
        #     ''sdl_ittarget''
        # ]
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        sch_name        = stage_name.split(''.'')[0]
        target_table    = sch_name+"."+Param[3]
    
        df_schema=StructType([
                StructField(''LAKSHYAT_TERRITORY_NAME'', StringType()),
                StructField(''TARGET_VARIANT'', StringType()),
                StructField(''JANAMOUNT'', StringType()),
                StructField(''FEBAMOUNT'', StringType()),
                StructField(''MARAMOUNT'', StringType()),
                StructField(''APRAMOUNT'', StringType()),
                StructField(''MAYAMOUNT'', StringType()),
                StructField(''JUNAMOUNT'', StringType()),
                StructField(''JULYAMOUNT'', StringType()),
                StructField(''AUGAMOUNT'', StringType()),
                StructField(''SEPAMOUNT'', StringType()),
                StructField(''OCTAMOUNT'', StringType()),
                StructField(''NOVAMOUNT'', StringType()),
                StructField(''DECAMOUNT'', StringType()),
                StructField(''YTDAMOUNT'', StringType()),
                ])

        
        df = session.read \\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "\\u0001")\\
            .option("skip_blank_lines", True) \\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        df = df.withColumn("CRT_DTTM",lit(datetime.now().strftime("%Y-%m-%d %H:%M:%S")))

                                    
        final_df = df.select(\\
            ''LAKSHYAT_TERRITORY_NAME'',\\
            ''TARGET_VARIANT'',\\
            try_cast(col(''JANAMOUNT''), IntegerType()).alias(''JANAMOUNT''),\\
            try_cast(col(''FEBAMOUNT''), IntegerType()).alias(''FEBAMOUNT''),\\
            try_cast(col(''MARAMOUNT''), IntegerType()).alias(''MARAMOUNT''),\\
            try_cast(col(''APRAMOUNT''), IntegerType()).alias(''APRAMOUNT''),\\
            try_cast(col(''MAYAMOUNT''), IntegerType()).alias(''MAYAMOUNT''),\\
            try_cast(col(''JUNAMOUNT''), IntegerType()).alias(''JUNAMOUNT''),\\
            try_cast(col(''JULYAMOUNT''), IntegerType()).alias(''JULYAMOUNT''),\\
            try_cast(col(''AUGAMOUNT''), IntegerType()).alias(''AUGAMOUNT''),\\
            try_cast(col(''SEPAMOUNT''), IntegerType()).alias(''SEPAMOUNT''),\\
            try_cast(col(''OCTAMOUNT''), IntegerType()).alias(''OCTAMOUNT''),\\
            try_cast(col(''NOVAMOUNT''), IntegerType()).alias(''NOVAMOUNT''),\\
            try_cast(col(''DECAMOUNT''), IntegerType()).alias(''DECAMOUNT''),\\
            try_cast(col(''YTDAMOUNT''), IntegerType()).alias(''YTDAMOUNT''),\\
            try_cast(col("CRT_DTTM"), TimestampType()).alias("CRT_DTTM")
            
        ).alias("final_df")

        
        err_df = final_df.filter( is_null(col(''JANAMOUNT''))\\
                                | is_null(col(''FEBAMOUNT''))\\
                                | is_null(col(''MARAMOUNT''))\\
                                | is_null(col(''APRAMOUNT''))\\
                                | is_null(col(''MAYAMOUNT''))\\
                                | is_null(col(''JUNAMOUNT''))\\
                                | is_null(col(''JULYAMOUNT''))\\
                                | is_null(col(''AUGAMOUNT''))\\
                                | is_null(col(''SEPAMOUNT''))\\
                                | is_null(col(''OCTAMOUNT''))\\
                                | is_null(col(''NOVAMOUNT''))\\
                                | is_null(col(''DECAMOUNT''))\\
                                | is_null(col(''YTDAMOUNT'')))
        
        corr_df = final_df.filter(not_(is_null(col(''JANAMOUNT''))\\
                                | is_null(col(''FEBAMOUNT''))\\
                                | is_null(col(''MARAMOUNT''))\\
                                | is_null(col(''APRAMOUNT''))\\
                                | is_null(col(''MAYAMOUNT''))\\
                                | is_null(col(''JUNAMOUNT''))\\
                                | is_null(col(''JULYAMOUNT''))\\
                                | is_null(col(''AUGAMOUNT''))\\
                                | is_null(col(''SEPAMOUNT''))\\
                                | is_null(col(''OCTAMOUNT''))\\
                                | is_null(col(''NOVAMOUNT''))\\
                                | is_null(col(''DECAMOUNT''))\\
                                | is_null(col(''YTDAMOUNT''))))

        file_name=file_name.split(".")[0]+"_"+ datetime.now().strftime("%Y%m%d%H%M%S")
        
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        
        #move to success
        corr_df.write.mode("append").saveAsTable(target_table)

        
        corr_df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)
        err_df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/error/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)
                                    
        return "Success"
        
    except KeyError as key_error:
        # Handle KeyError (missing columns) here
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message
        
    except Exception as e:
        # Handle exceptions here
        error_message = f"Error: {str(e)}" +str(df.columns)
        return error_message';

CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.INDSDL_RAW.M_SDL_RDSSIZE_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS 'import snowflake.snowpark as snowpark
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DateType, DoubleType,TimestampType,DecimalType
from snowflake.snowpark.functions import col,lit,date_format, try_cast, trim, to_date, left,  right, is_integer, to_variant, not_, is_null
import pandas as pd
from datetime import datetime
import pytz

def main(session: snowpark.Session,Param): 

    try:
    
        # Param = [
        #     ''RDS_Size'',
        #     ''INDSDL_RAW.DEV_LOAD_STAGE_ADLS'',
        #     ''dev/master_customer'',
        #     ''sdl_rdssize''
        # ]
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        sch_name        = stage_name.split(''.'')[0]
        target_table    = sch_name+"."+Param[3]
    
        df_schema=StructType([
                StructField(''RDSCODE'', StringType()),
                StructField(''RDSSIZE'', StringType())
                ])

        
        df = session.read \\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "\\u0001")\\
            .option("skip_blank_lines", True) \\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        df = df.withColumn("CRT_DTTM",lit(datetime.now().strftime("%Y-%m-%d %H:%M:%S")))

                                    
        final_df = df.select(\\
            try_cast(col("RDSCODE"), IntegerType()).alias("RDSCODE"),
            "RDSSIZE",
            try_cast(col("CRT_DTTM"), TimestampType()).alias("CRT_DTTM")
            
        ).alias("final_df")

        
        err_df = final_df.filter(is_null(col("RDSCODE")))
        corr_df = final_df.filter(not_(is_null(col("RDSCODE"))))

        file_name=file_name.split(".")[0]+"_"+ datetime.now().strftime("%Y%m%d%H%M%S")
        
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        
        #move to success
        corr_df.write.mode("append").saveAsTable(target_table)

        corr_df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)
        err_df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/error/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)
                                    
        return "Success"
        
    except KeyError as key_error:
        # Handle KeyError (missing columns) here
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message
        
    except Exception as e:
        # Handle exceptions here
        error_message = f"Error: {str(e)}" +str(df.columns)
        return error_message';

CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.INDSDL_RAW.M_WRP_IN_SDLITG_PWS_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS 'import snowflake.snowpark as snowpark
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DateType, DoubleType,TimestampType,DecimalType
from snowflake.snowpark.functions import col,lit,date_format, try_cast, trim, to_date, left,  right
import pandas as pd
from datetime import datetime
import pytz

def main(session: snowpark.Session,Param): 

    try:
    
        # Param = [
        #     ''productwisestock'',
        #     ''INDSDL_RAW.DEV_LOAD_STAGE_ADLS'',
        #     ''dev/india_xdm/transaction/productwisestock'',
        #     ''sdl_csl_productwisestock''
        # ]
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        sch_name        = stage_name.split(''.'')[0]
        target_table    = sch_name+"."+Param[3]
    
        df_schema=StructType([
                StructField(''DISTCODE'', StringType()),
                StructField(''TRANSDATE'', StringType()),
                StructField(''LCNCODE'', StringType()),
                StructField(''PRDCODE'', StringType()),
                StructField(''SALOPENSTOCK'', StringType()),
                StructField(''UNSALOPENSTOCK'', StringType()),
                StructField(''OFFEROPENSTOCK'', StringType()),
                StructField(''SALPURCHASE'', StringType()),
                StructField(''UNSALPURCHASE'', StringType()),
                StructField(''OFFERPURCHASE'', StringType()),
                StructField(''SALPURRETURN'', StringType()),
                StructField(''UNSALPURRETURN'', StringType()),
                StructField(''OFFERPURRETURN'', StringType()),
                StructField(''SALSALES'', StringType()),
                StructField(''UNSALSALES'', StringType()),
                StructField(''OFFERSALES'', StringType()),
                StructField(''SALSTOCKIN'', StringType()),
                StructField(''UNSALSTOCKIN'', StringType()),
                StructField(''OFFERSTOCKIN'', StringType()),
                StructField(''SALSTOCKOUT'', StringType()),
                StructField(''UNSALSTOCKOUT'', StringType()),
                StructField(''OFFERSTOCKOUT'', StringType()),
                StructField(''DAMAGEIN'', StringType()),
                StructField(''DAMAGEOUT'', StringType()),
                StructField(''SALSALESRETURN'', StringType()),
                StructField(''UNSALSALESRETURN'', StringType()),
                StructField(''OFFERSALESRETURN'', StringType()),
                StructField(''SALSTKJURIN'', StringType()),
                StructField(''UNSALSTKJURIN'', StringType()),
                StructField(''OFFERSTKJURIN'', StringType()),
                StructField(''SALSTKJUROUT'', StringType()),
                StructField(''UNSALSTKJUROUT'', StringType()),
                StructField(''OFFERSTKJUROUT'', StringType()),
                StructField(''SALBATTFRIN'', StringType()),
                StructField(''UNSALBATTFRIN'', StringType()),
                StructField(''OFFERBATTFRIN'', StringType()),
                StructField(''SALBATTFROUT'', StringType()),
                StructField(''UNSALBATTFROUT'', StringType()),
                StructField(''OFFERBATTFROUT'', StringType()),
                StructField(''SALLCNTFRIN'', StringType()),
                StructField(''UNSALLCNTFRIN'', StringType()),
                StructField(''OFFERLCNTFRIN'', StringType()),
                StructField(''SALLCNTFROUT'', StringType()),
                StructField(''UNSALLCNTFROUT'', StringType()),
                StructField(''OFFERLCNTFROUT'', StringType()),
                StructField(''SALREPLACEMENT'', StringType()),
                StructField(''OFFERREPLACEMENT'', StringType()),
                StructField(''SALCLSSTOCK'', StringType()),
                StructField(''UNSALCLSSTOCK'', StringType()),
                StructField(''OFFERCLSSTOCK'', StringType()),
                StructField(''UPLOADDATE'', StringType()),
                StructField(''SYNCID'', StringType()),
                StructField(''CREATEDDATE'', StringType())
                
                ])

        
        df = session.read \\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "\\u0001")\\
            .option("skip_blank_lines", True) \\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
    
        df = df.withColumn("FILE_NAME",lit(file_name).cast("string"))
        df = df.withColumn("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        df = df.withColumn("CRT_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))

                                    
        final_df = df.select(\\
            ''DISTCODE'',\\
            try_cast(col("TRANSDATE"), TimestampType()).alias("TRANSDATE"),\\
            try_cast(lit(None), IntegerType()).alias("LCNID"),\\
            ''LCNCODE'',\\
            try_cast(lit(None), IntegerType()).alias("PRDID"),\\
            ''PRDCODE'',\\
            try_cast(col(''SALOPENSTOCK''), IntegerType()).alias(''SALOPENSTOCK''),\\
            try_cast(col(''UNSALOPENSTOCK''), IntegerType()).alias(''UNSALOPENSTOCK''),\\
            try_cast(col(''OFFEROPENSTOCK''), IntegerType()).alias(''OFFEROPENSTOCK''),\\
            try_cast(col(''SALPURCHASE''), IntegerType()).alias(''SALPURCHASE''),\\
            try_cast(col(''UNSALPURCHASE''), IntegerType()).alias(''UNSALPURCHASE''),\\
            try_cast(col(''OFFERPURCHASE''), IntegerType()).alias(''OFFERPURCHASE''),\\
            try_cast(col(''SALPURRETURN''), IntegerType()).alias(''SALPURRETURN''),\\
            try_cast(col(''UNSALPURRETURN''), IntegerType()).alias(''UNSALPURRETURN''),\\
            try_cast(col(''OFFERPURRETURN''), IntegerType()).alias(''OFFERPURRETURN''),\\
            try_cast(col(''SALSALES''), IntegerType()).alias(''SALSALES''),\\
            try_cast(col(''UNSALSALES''), IntegerType()).alias(''UNSALSALES''),\\
            try_cast(col(''OFFERSALES''), IntegerType()).alias(''OFFERSALES''),\\
            try_cast(col(''SALSTOCKIN''), IntegerType()).alias(''SALSTOCKIN''),\\
            try_cast(col(''UNSALSTOCKIN''), IntegerType()).alias(''UNSALSTOCKIN''),\\
            try_cast(col(''OFFERSTOCKIN''), IntegerType()).alias(''OFFERSTOCKIN''),\\
            try_cast(col(''SALSTOCKOUT''), IntegerType()).alias(''SALSTOCKOUT''),\\
            try_cast(col(''UNSALSTOCKOUT''), IntegerType()).alias(''UNSALSTOCKOUT''),\\
            try_cast(col(''OFFERSTOCKOUT''), IntegerType()).alias(''OFFERSTOCKOUT''),\\
            try_cast(col(''DAMAGEIN''), IntegerType()).alias(''DAMAGEIN''),\\
            try_cast(col(''DAMAGEOUT''), IntegerType()).alias(''DAMAGEOUT''),\\
            try_cast(col(''SALSALESRETURN''), IntegerType()).alias(''SALSALESRETURN''),\\
            try_cast(col(''UNSALSALESRETURN''), IntegerType()).alias(''UNSALSALESRETURN''),\\
            try_cast(col(''OFFERSALESRETURN''), IntegerType()).alias(''OFFERSALESRETURN''),\\
            try_cast(col(''SALSTKJURIN''), IntegerType()).alias(''SALSTKJURIN''),\\
            try_cast(col(''UNSALSTKJURIN''), IntegerType()).alias(''UNSALSTKJURIN''),\\
            try_cast(col(''OFFERSTKJURIN''), IntegerType()).alias(''OFFERSTKJURIN''),\\
            try_cast(col(''SALSTKJUROUT''), IntegerType()).alias(''SALSTKJUROUT''),\\
            try_cast(col(''UNSALSTKJUROUT''), IntegerType()).alias(''UNSALSTKJUROUT''),\\
            try_cast(col(''OFFERSTKJUROUT''), IntegerType()).alias(''OFFERSTKJUROUT''),\\
            try_cast(col(''SALBATTFRIN''), IntegerType()).alias(''SALBATTFRIN''),\\
            try_cast(col(''UNSALBATTFRIN''), IntegerType()).alias(''UNSALBATTFRIN''),\\
            try_cast(col(''OFFERBATTFRIN''), IntegerType()).alias(''OFFERBATTFRIN''),\\
            try_cast(col(''SALBATTFROUT''), IntegerType()).alias(''SALBATTFROUT''),\\
            try_cast(col(''UNSALBATTFROUT''), IntegerType()).alias(''UNSALBATTFROUT''),\\
            try_cast(col(''OFFERBATTFROUT''), IntegerType()).alias(''OFFERBATTFROUT''),\\
            try_cast(col(''SALLCNTFRIN''), IntegerType()).alias(''SALLCNTFRIN''),\\
            try_cast(col(''UNSALLCNTFRIN''), IntegerType()).alias(''UNSALLCNTFRIN''),\\
            try_cast(col(''OFFERLCNTFRIN''), IntegerType()).alias(''OFFERLCNTFRIN''),\\
            try_cast(col(''SALLCNTFROUT''), IntegerType()).alias(''SALLCNTFROUT''),\\
            try_cast(col(''UNSALLCNTFROUT''), IntegerType()).alias(''UNSALLCNTFROUT''),\\
            try_cast(col(''OFFERLCNTFROUT''), IntegerType()).alias(''OFFERLCNTFROUT''),\\
            try_cast(col(''SALREPLACEMENT''), IntegerType()).alias(''SALREPLACEMENT''),\\
            try_cast(col(''OFFERREPLACEMENT''), IntegerType()).alias(''OFFERREPLACEMENT''),\\
            try_cast(col(''SALCLSSTOCK''), IntegerType()).alias(''SALCLSSTOCK''),\\
            try_cast(col(''UNSALCLSSTOCK''), IntegerType()).alias(''UNSALCLSSTOCK''),\\
            try_cast(col(''OFFERCLSSTOCK''), IntegerType()).alias(''OFFERCLSSTOCK''),\\
            try_cast(col("CREATEDDATE"), TimestampType()).alias("UPLOADDATE"),\\
            lit(None).alias("UPLOADFLAG"),\\
            try_cast(col("CREATEDDATE"), TimestampType()).alias("CREATEDDATE"),\\
            ''SYNCID'',\\
            "RUN_ID",
            try_cast(col("CRT_DTTM"), TimestampType()).alias("CRT_DTTM"),\\
            "FILE_NAME"
        ).alias("final_df")
        
        final_df.write.mode("append").saveAsTable(target_table)
                                    
        return "Success"
        
    except KeyError as key_error:
        # Handle KeyError (missing columns) here
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message
        
    except Exception as e:
        # Handle exceptions here
        error_message = f"Error: {str(e)}" +str(df.columns)
        return error_message';

CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.INDSDL_RAW.RRL_RETAILERCATEGORY_PREPROCESSING("PARAM" ARRAY)
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
    # Param=["rt_retailercategory_202404031901_20240404020009.csv","INDSDL_RAW.DEV_LOAD_STAGE_ADLS","dev/india_rt/rt_retailercategory","SDL_RRL_RETAILERCATEGORY"]
    try:
        file_name = Param[0]
        stage_name = Param[1]
        temp_stage_path = Param[2]
        target_table = Param[3]
        
        df_schema = StructType([
            StructField("retailercategorycode", StringType(), True),
            StructField("retailercategoryname", StringType(), True),     
            StructField("ctgcode", StringType(), True),
            StructField("ctglinkid", StringType(), True),
            StructField("ctglevelid", StringType(), True),
            StructField("isbrandshow", StringType(), True),
            StructField("isactive", StringType(), True),
            StructField("rowid", StringType(), True),
    
        ])
        
        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "|")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
        
        df = df.na.drop("all")
        
        if df.count() == 0:
            return "No Data in file"

        df = df.withColumn("crtd_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        df = df.withColumn("filename", lit(file_name))
        # df = df.withColumn("run_id", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))


        snowdf = df.select(
            "retailercategorycode", 
            "retailercategoryname",      
            "ctgcode", 
            "ctglinkid", 
            "ctglevelid", 
            "isbrandshow", 
            "isactive", 
            "rowid", 
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
CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.INDSDL_RAW.RRL_RETAILERMASTER_PREPROCESSING("PARAM" ARRAY)
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
    # Param=["rt_retailermaster_20241628_20240429020007.csv","INDSDL_RAW.DEV_LOAD_STAGE_ADLS","dev/india_rt/rt_retailermaster","SDL_RRL_RETAILERMASTER"]
    try:
        file_name = Param[0]
        stage_name = Param[1]
        temp_stage_path = Param[2]
        target_table = Param[3]
        
        df_schema = StructType([
            StructField("retailercode", StringType(), True),
            StructField("retailername", StringType(), True),     
            StructField("routecode", StringType(), True),
            StructField("retailerclasscode", StringType(), True),
            StructField("villagecode", StringType(), True),
            StructField("rsdcode", StringType(), True),
            StructField("distributorcode", StringType(), True),
            StructField("foodlicenseno", StringType(), True),
            StructField("druglicenseno", StringType(), True),
            StructField("address", StringType(), True),
            StructField("phone", StringType(), True),
            StructField("mobile", StringType(), True),
            StructField("prcontact", StringType(), True),
            StructField("seccontact", StringType(), True),
            StructField("creditlimit", StringType(), True),
            StructField("creditperiod", StringType(), True),
            StructField("invoicelimit", StringType(), True),
            StructField("isapproved", StringType(), True),
            StructField("isactive", StringType(), True),
            StructField("rsrcode", StringType(), True),
            StructField("drugvaliditydate", StringType(), True),
            StructField("fssaivaliditydate", StringType(), True),
            StructField("displaystatus", StringType(), True),
            StructField("createddate", StringType(), True),
            StructField("ownername", StringType(), True),
            StructField("druglicenseno2", StringType(), True),
            StructField("r_statecode", StringType(), True),
            StructField("r_districtcode", StringType(), True),
            StructField("r_tahsilcode", StringType(), True),
            StructField("address1", StringType(), True),
            StructField("address2", StringType(), True),
            StructField("retailerchannelcode", StringType(), True),
            StructField("retailerclassid", StringType(), True),
    
        ])
        
        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "|")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
        
        df = df.na.drop("all")
        
        if df.count() == 0:
            return "No Data in file"

        df = df.withColumn("crtd_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        df = df.withColumn("filename", lit(file_name))
        # df = df.withColumn("run_id", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))

        df_pandas=df.to_pandas()
        df_pandas[''CREATEDDATE''] = pd.to_datetime(df_pandas[''CREATEDDATE''])
        df_pandas[''CREATEDDATE''] = df_pandas[''CREATEDDATE''].dt.strftime(''%Y-%m-%d %H:%M:%S.%f'')
        df=session.create_dataframe(df_pandas)

        snowdf = df.select(
            "retailercode", 
            "retailername",      
            "routecode", 
            "retailerclasscode", 
            "villagecode", 
            "rsdcode", 
            "distributorcode", 
            "foodlicenseno", 
            "druglicenseno", 
            "address", 
            "phone", 
            "mobile", 
            "prcontact", 
            "seccontact", 
            "creditlimit", 
            "creditperiod", 
            "invoicelimit", 
            "isapproved", 
            "isactive", 
            "rsrcode", 
            "drugvaliditydate", 
            "fssaivaliditydate", 
            "displaystatus", 
            "CreatedDate", 
            "ownername", 
            "druglicenseno2", 
            "r_statecode", 
            "r_districtcode", 
            "r_tahsilcode", 
            "address1", 
            "address2", 
            "retailerchannelcode", 
            "retailerclassid", 
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

CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.INDSDL_RAW.RRL_RETAILERVALUECLASS_PREPROCESSING("PARAM" ARRAY)
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
    # Param=["rt_retailervalue_202403311901_20240401020008.csv","INDSDL_RAW.DEV_LOAD_STAGE_ADLS","dev/india_rt/rt_retailervalue","SDL_RRL_RETAILERVALUECLASS"]
    try:
        file_name = Param[0]
        stage_name = Param[1]
        temp_stage_path = Param[2]
        target_table = Param[3]
        
        df_schema = StructType([
            StructField("classid", StringType(), True),
            StructField("valueclasscode", StringType(), True),     
            StructField("valueclassname", StringType(), True),
            StructField("ctgmainid", StringType(), True),
            StructField("isactive", StringType(), True),
            StructField("distcode", StringType(), True),
            StructField("rowid", StringType(), True),
    
        ])
        
        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "|")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
        
        df = df.na.drop("all")
        
        if df.count() == 0:
            return "No Data in file"

        df = df.withColumn("crtd_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        df = df.withColumn("filename", lit(file_name))
        # df = df.withColumn("run_id", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))


        snowdf = df.select(
            "classid",
            "valueclasscode",    
            "valueclassname",
            "ctgmainid",
            "isactive",
            "distcode",
            "rowid", 
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

CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.INDSDL_RAW.RRL_RETAILER_GEOCOORDINATES_PREPROCESSING("PARAM" ARRAY)
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
    # Param=[''rt_geocoordinates_20241802_20240603020008.csv'',''INDSDL_RAW.DEV_LOAD_STAGE_ADLS'',''dev/india_rt/geocoordinates'',''SDL_RRL_RETAILER_GEOCOORDINATES'']
    try:
        file_name = Param[0]
        stage_name = Param[1]
        temp_stage_path = Param[2]
        target_table = Param[3]
        
        df_schema = StructType([
            StructField("rgc_id", StringType(), True),
            StructField("rgc_usercode", StringType(), True),     
            StructField("rgc_distcode", StringType(), True),
            StructField("rgc_code", StringType(), True),
            StructField("rgc_latitude", StringType(), True),
            StructField("rgc_longtitude", StringType(), True),
            StructField("rgc_geouniqueid", StringType(), True),
            StructField("rgc_createdby", StringType(), True),
            StructField("rgc_createddate", StringType(), True),     
            StructField("rgc_modifiedby", StringType(), True),
            StructField("rgc_modifieddate", StringType(), True),
            StructField("rgc_flag", StringType(), True),
            StructField("rgc_status_flag", StringType(), True),
            StructField("rgc_flex", StringType(), True),
    
        ])
        
        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "|")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
        
        df = df.na.drop("all")
        
        if df.count() == 0:
            return "No Data in file"

        df = df.withColumn("rgc_createddate", regexp_replace(col("rgc_createddate"), "/", "-"))
        df = df.withColumn("rgc_modifieddate", regexp_replace(col("rgc_modifieddate"), "/", "-"))

        df = df.withColumn("crtd_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        df = df.withColumn("filename", lit(file_name))
        # df = df.withColumn("run_id", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))


        snowdf = df.select(
            "rgc_id", 
            "rgc_usercode",      
            "rgc_distcode", 
            "rgc_code", 
            "rgc_latitude", 
            "rgc_longtitude", 
            "rgc_geouniqueid", 
            "rgc_createdby", 
            "rgc_createddate",      
            "rgc_modifiedby", 
            "rgc_modifieddate", 
            "rgc_flag", 
            "rgc_status_flag", 
            "rgc_flex", 
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

CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.INDSDL_RAW.RRL_ROUTEMASTER_PREPROCESSING("PARAM" ARRAY)
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
    # Param=["rt_routemaster_20241702_20240503020009.csv","INDSDL_RAW.DEV_LOAD_STAGE_ADLS","dev/india_rt/rt_routemaster","SDL_RRL_ROUTEMASTER"]
    try:
        file_name = Param[0]
        stage_name = Param[1]
        temp_stage_path = Param[2]
        target_table = Param[3]
        
        df_schema = StructType([
            StructField("routecode", StringType(), True),
            StructField("routeename", StringType(), True),     
            StructField("flag", StringType(), True),
            StructField("isactive", StringType(), True),
            StructField("distributorcode", StringType(), True),
            StructField("rowid", StringType(), True),
    
        ])
        
        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "|")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
        
        df = df.na.drop("all")
        
        if df.count() == 0:
            return "No Data in file"

        df = df.withColumn("crtd_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        df = df.withColumn("filename", lit(file_name))
        # df = df.withColumn("run_id", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))


        snowdf = df.select(
            "routecode",
            "routeename",     
            "flag",
            "isactive",
            "distributorcode",
            "rowid", 
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

CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.INDSDL_RAW.RRL_RSDMASTER_PREPROCESSING("PARAM" ARRAY)
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
    # Param=["rt_rsdmaster_20241701_20240502020007.csv","INDSDL_RAW.DEV_LOAD_STAGE_ADLS","dev/india_rt/rt_rsdmaster","SDL_RRL_RSDMASTER"]
    try:
        file_name = Param[0]
        stage_name = Param[1]
        temp_stage_path = Param[2]
        target_table = Param[3]
        
        df_schema = StructType([
            StructField("rsdcode", StringType(), True),
            StructField("rsdname", StringType(), True),     
            StructField("rsdfirm", StringType(), True),
            StructField("rsrcode", StringType(), True),
            StructField("villagecode", StringType(), True),
            StructField("distributorcode", StringType(), True),
            StructField("montlyincome", StringType(), True),
            StructField("manpower", StringType(), True),
            StructField("godownspace", StringType(), True),
            StructField("address", StringType(), True),
            StructField("contactno", StringType(), True),
            StructField("druglicense", StringType(), True),
            StructField("foodlicense", StringType(), True),
            StructField("isownhouse", StringType(), True),
            StructField("isnative", StringType(), True),
            StructField("drugvaliditydate", StringType(), True),
            StructField("fssaivaliditydate", StringType(), True),
            StructField("isapproved", StringType(), True),
            StructField("flag", StringType(), True),
            StructField("isactive", StringType(), True),
            StructField("createdby", StringType(), True),
            StructField("createddate", StringType(), True),
            StructField("modifiedby", StringType(), True),
            StructField("modifieddate", StringType(), True),
            StructField("deleteddate", StringType(), True),
            StructField("channelname", StringType(), True),
            StructField("subchannelid", StringType(), True),
            StructField("subchannelname", StringType(), True),
            StructField("categoryid", StringType(), True),
            StructField("categoryname", StringType(), True),
            StructField("outlettype", StringType(), True),
            StructField("modaloutlet", StringType(), True),
            StructField("synctimestamp", StringType(), True),
            StructField("contactperson", StringType(), True),
            StructField("rsdemailid", StringType(), True),
            StructField("druglicenseno2", StringType(), True),
            StructField("rsdemailid1", StringType(), True),
            StructField("rsdemailid2", StringType(), True),
            StructField("salesrepemailid", StringType(), True),
            StructField("routecode", StringType(), True),
            StructField("rtrclassid", StringType(), True),
    
        ])
        
        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "|")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
        
        df = df.na.drop("all")
        
        if df.count() == 0:
            return "No Data in file"  

        df = df.withColumn("crtd_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        df = df.withColumn("filename", lit(file_name))
        # df = df.withColumn("run_id", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))

        df_pandas=df.to_pandas()
        # Convert columns to datetime
        date_columns = [''CREATEDDATE'',''DRUGVALIDITYDATE'',''MODIFIEDDATE'',''DELETEDDATE'',''SYNCTIMESTAMP'']
        for column in date_columns:
            df_pandas[column] = pd.to_datetime(df_pandas[column])

        # Format columns as string with desired format
        for column in date_columns:
            df_pandas[column] = df_pandas[column].dt.strftime(''%Y-%m-%d %H:%M:%S.%f'')
        df_pandas[''FSSAIVALIDITYDATE''] = pd.to_datetime(df_pandas[''FSSAIVALIDITYDATE''],format="%Y-%m-%d %H:%M:%S")
        # Create DataFrame in Snowpark session
        df = session.create_dataframe(df_pandas)

        snowdf = df.select(
            "rsdcode", 
            "rsdname",      
            "rsdfirm", 
            "rsrcode", 
            "villagecode", 
            "distributorcode", 
            "montlyincome", 
            "manpower", 
            "godownspace", 
            "address", 
            "contactno", 
            "druglicense", 
            "foodlicense", 
            "isownhouse", 
            "isnative", 
            "drugvaliditydate", 
            "fssaivaliditydate", 
            "isapproved", 
            "flag", 
            "isactive", 
            "createdby", 
            "createddate", 
            "modifiedby", 
            "modifieddate", 
            "deleteddate", 
            "channelname", 
            "subchannelid", 
            "subchannelname", 
            "categoryid", 
            "categoryname", 
            "outlettype", 
            "modaloutlet", 
            "synctimestamp", 
            "contactperson", 
            "rsdemailid", 
            "druglicenseno2", 
            "rsdemailid1", 
            "rsdemailid2", 
            "salesrepemailid", 
            "routecode", 
            "rtrclassid", 
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

CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.INDSDL_RAW.RRL_RURALSTOREORDERDETAIL_PREPROCESSING("PARAM" ARRAY)
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
    # Param=["rt_ruralstoreorderdetail_20241628_20240429020007.csv","INDSDL_RAW.DEV_LOAD_STAGE_ADLS","dev/india_rt/rt_ruralstoreorderdetail","SDL_RRL_RURALSTOREORDERDETAIL"]
    try:
        file_name = Param[0]
        stage_name = Param[1]
        temp_stage_path = Param[2]
        target_table = Param[3]
        
        df_schema = StructType([
            StructField("orderid", StringType(), True),
            StructField("productid", StringType(), True),     
            StructField("uomid", StringType(), True),
            StructField("qty", StringType(), True),
            StructField("price", StringType(), True),
            StructField("netprice", StringType(), True),
            StructField("discountvalue", StringType(), True),
            StructField("foc", StringType(), True),
            StructField("tax", StringType(), True),
            StructField("status", StringType(), True),
            StructField("flag", StringType(), True),
            StructField("usercode", StringType(), True),
            StructField("ordd_distributorcode", StringType(), True),
            StructField("orderdate", StringType(), True),
            StructField("uom", StringType(), True),
    
        ])
        
        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "|")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
        
        df = df.na.drop("all")
        
        if df.count() == 0:
            return "No Data in file"

        df = df.withColumn("crtd_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        df = df.withColumn("filename", lit(file_name))
        # df = df.withColumn("run_id", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))

        df_pandas=df.to_pandas()
        df_pandas[''ORDERDATE''] = pd.to_datetime(df_pandas[''ORDERDATE''])
        df_pandas[''ORDERDATE''] = df_pandas[''ORDERDATE''].dt.strftime(''%Y-%m-%d %H:%M:%S.%f'')
        df=session.create_dataframe(df_pandas)

        snowdf = df.select(
            "orderid", 
            "productid",      
            "uomid", 
            "qty", 
            "price", 
            "netprice", 
            "discountvalue", 
            "foc", 
            "tax", 
            "status", 
            "flag", 
            "usercode", 
            "ordd_distributorcode", 
            "orderdate", 
            "uom", 
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

CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.INDSDL_RAW.RRL_RURALSTOREORDERHEADER_PREPROCESSING("PARAM" ARRAY)
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
    # Param=["rt_ruralstoreorderheader_20241628_20240429020046.csv","INDSDL_RAW.DEV_LOAD_STAGE_ADLS","dev/india_rt/rt_ruralstoreorderheader","SDL_RRL_RURALSTOREORDERHEADER"]
    try:
        file_name = Param[0]
        stage_name = Param[1]
        temp_stage_path = Param[2]
        target_table = Param[3]

        
        df_schema = StructType([
            StructField("orderid", StringType(), True),
            StructField("orderdate", StringType(), True),
            StructField("deliverydate", StringType(), True),
            StructField("ovid", StringType(), True),
            StructField("usercode", StringType(), True),
            StructField("ordervalue", StringType(), True),
            StructField("linespercall", StringType(), True),
            StructField("feedback", StringType(), True),
            StructField("orderstarttime", StringType(), True),
            StructField("orderendtime", StringType(), True),
            StructField("outletsignature", StringType(), True),
            StructField("islocked", StringType(), True),
            StructField("flag", StringType(), True),
            StructField("saletype", StringType(), True),
            StructField("retailerid", StringType(), True),
            StructField("invoicestatus", StringType(), True),
            StructField("billdiscount", StringType(), True),
            StructField("tax", StringType(), True),
            StructField("isjointcall", StringType(), True),
            StructField("ord_distributorcode", StringType(), True),
            StructField("weekid", StringType(), True),
            StructField("rsd_code", StringType(), True),
            StructField("row_id", StringType(), True),
            StructField("route_code", StringType(), True),
        ])
        
        
        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "|")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
        
        df = df.na.drop("all")
        
        if df.count() == 0:
            return "No Data in file"

        df_pandas=df.to_pandas()
        df_pandas[''ORDERDATE''] = pd.to_datetime(df_pandas[''ORDERDATE''])
        df_pandas[''ORDERDATE''] = df_pandas[''ORDERDATE''].dt.strftime(''%Y-%m-%d %H:%M:%S.%f'')
        df=session.create_dataframe(df_pandas)

        df = df.withColumn("crtd_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        df = df.withColumn("filename", lit(file_name))
        # df = df.withColumn("run_id", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))


        snowdf = df.select(
            "orderid", 
            "orderdate",      
            "deliverydate", 
            "ovid", 
            "usercode", 
            "ordervalue", 
            "linespercall", 
            "feedback", 
            "orderstarttime", 
            "orderendtime", 
            "outletsignature", 
            "islocked", 
            "flag", 
            "saletype", 
            "retailerid", 
            "invoicestatus", 
            "billdiscount", 
            "tax", 
            "isjointcall", 
            "ord_distributorcode", 
            "weekid", 
            "rsd_code", 
            "row_id", 
            "route_code",
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

CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.INDSDL_RAW.RRL_TOWNMASTER_PREPROCESSING("PARAM" ARRAY)
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
    # Param=[''rt_townmaster_20241801_20240602020007.csv'',''INDSDL_RAW.DEV_LOAD_STAGE_ADLS'',''dev/india_rt/townmaster'',''SDL_RRL_TOWNMASTER'']
    try:
        file_name = Param[0]
        stage_name = Param[1]
        temp_stage_path = Param[2]
        target_table = Param[3]
        
        df_schema = StructType([
            StructField("routecode", StringType(), True),
            StructField("villagecode", StringType(), True),     
            StructField("villagename", StringType(), True),
            StructField("population", StringType(), True),
            StructField("rsrcode", StringType(), True),
            StructField("rsdcode", StringType(), True),
            StructField("distributorcode", StringType(), True),
            StructField("sarpanchname", StringType(), True),
            StructField("sarpanchno", StringType(), True),     
            StructField("isactive", StringType(), True),
            StructField("createddate", StringType(), True),
            StructField("createdby", StringType(), True),
            StructField("updateddate", StringType(), True),
            StructField("updatedby", StringType(), True),
    
        ])
        
        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "|")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
        
        df = df.na.drop("all")
        
        if df.count() == 0:
            return "No Data in file"

        df = df.withColumn("createddate", regexp_replace(col("createddate"), "/", "-"))
        df = df.withColumn("updateddate", regexp_replace(col("updateddate"), "/", "-"))


        df = df.withColumn("crtd_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        df = df.withColumn("filename", lit(file_name))
        # df = df.withColumn("run_id", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))


        snowdf = df.select(
            "routecode", 
            "villagecode",      
            "villagename", 
            "population", 
            "rsrcode", 
            "rsdcode", 
            "distributorcode", 
            "sarpanchname", 
            "sarpanchno",      
            "isactive", 
            "createddate", 
            "createdby", 
            "updateddate", 
            "updatedby", 
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

CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.INDSDL_RAW.RRL_UDCMASTERPREPROCESSING("PARAM" ARRAY)
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
    # Param=[''rt_udcmaster_202105131900_20211119222603.csv'',''INDSDL_RAW.DEV_LOAD_STAGE_ADLS'',''dev/india_rt/rt_udcmaster'',''SDL_RRL_UDCMASTER'']
    try:
        file_name = Param[0]
        stage_name = Param[1]
        temp_stage_path = Param[2]
        target_table = Param[3]
        
        df_schema = StructType([
            StructField("udccode", StringType(), True),
            StructField("udcname", StringType(), True),     
            StructField("isactive", StringType(), True),
            StructField("rowid", StringType(), True),
            
        ])
        
        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "|")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
        
        df = df.na.drop("all")
        
        if df.count() == 0:
            return "No Data in file"


        df = df.withColumn("crtd_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        df = df.withColumn("filename", lit(file_name))
        # df = df.withColumn("run_id", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))


        snowdf = df.select(
            "udccode", 
            "udcname",      
            "isactive", 
            "rowid",  
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

CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.INDSDL_RAW.SALESRETURN_DEL_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS 'from snowflake.snowpark.functions import lit
from snowflake.snowpark.types import StringType, StructType, StructField
from datetime import datetime
import pandas as pd
import snowflake.snowpark as snowpark
import pytz

def main(session: snowpark.Session,Param):
    # Param = ["salesreturn_del20240523062035.csv","INDSDL_RAW.DEV_LOAD_STAGE_ADLS","dev/india_xdm/transaction/salesreturn_del","SDL_SALESRETURN_DEL"]

    try:
        file_name = Param[0]
        stage_name = Param[1]
        temp_stage_path = Param[2]
        target_table = Param[3]

        # Define schema
        df_schema = StructType([
            StructField("distcode", StringType(), True),
            StructField("srnrefno", StringType(), True),
            StructField("srnreftype", StringType(), True),
            StructField("srndate", StringType(), True),
            StructField("srnmode", StringType(), True),
            StructField("srntype", StringType(), True),
            StructField("srngrossamt", StringType(), True),
            StructField("srnspldiscamt", StringType(), True),
            StructField("srnschdiscamt", StringType(), True),
            StructField("srncashdiscamt", StringType(), True),
            StructField("srndbdiscamt", StringType(), True),
            StructField("srntaxamt", StringType(), True),
            StructField("srnroundoffamt", StringType(), True),
            StructField("srnnetamt", StringType(), True),
            StructField("salesmanname", StringType(), True),
            StructField("salesroutename", StringType(), True),
            StructField("rtrcode", StringType(), True),
            StructField("rtrname", StringType(), True),
            StructField("prdsalinvno", StringType(), True),
            StructField("prdlcncode", StringType(), True),
            StructField("prdcode", StringType(), True),
            StructField("prdbatcde", StringType(), True),
            StructField("prdsalqty", StringType(), True),
            StructField("prdunsalqty", StringType(), True),
            StructField("prdofferqty", StringType(), True),
            StructField("prdselrate", StringType(), True),
            StructField("prdgrossamt", StringType(), True),
            StructField("prdspldiscamt", StringType(), True),
            StructField("prdschdiscamt", StringType(), True),
            StructField("prdcashdiscamt", StringType(), True),
            StructField("prddbdiscamt", StringType(), True),
            StructField("prdtaxamt", StringType(), True),
            StructField("prdnetamt", StringType(), True),
            StructField("mrp", StringType(), True),
            StructField("rtnfreeqtyvalue", StringType(), True),
            StructField("referencetype", StringType(), True),
            StructField("salesmancode", StringType(), True),
            StructField("salesroutecode", StringType(), True),
            StructField("nrvalue", StringType(), True),
            StructField("prdselrateaftertax", StringType(), True),
            StructField("mrpcs", StringType(), True),
            StructField("lpvalue", StringType(), True),
            StructField("rtnwindowdisplayamt", StringType(), True),
            StructField("cradjamt", StringType(), True),
            StructField("rtrurccode", StringType(), True),
            StructField("createddate", StringType(), True),
            StructField("modifieddate", StringType(), True),
            StructField("syncid", StringType(), True),
            StructField("rtnlinecount", StringType(), True),
            StructField("createddt", StringType(), True)
            
        ])

        # Read the CSV file into a Snowflake DataFrame
        df = session.read \\
            .schema(df_schema)\\
            .option("skip_header", 1)\\
            .option("field_delimiter","\\u0001")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .csv(f"@{stage_name}/{temp_stage_path}/{file_name}")

        df = df.na.drop("all")

        if df.count() == 0:
            return "No Data in file"

        def removecsv(file_name):
            return file_name.split(''.'')[0] 

        df = df.withColumn("rtrid", lit(None)).withColumn("prdlcnid", lit(None)).withColumn("uploadflag", lit(None)).withColumn("createduserid", lit(None)).withColumn("migrationflag", lit(None))

        df = df.withColumn(''filename'',lit(removecsv(file_name)))  
        df = df.withColumn("crtd_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        df = df.withColumn("run_id", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))


        snowdf = df.select(
            "distcode", 
            "srnrefno", 
            "srnreftype", 
            "srndate", 
            "srnmode", 
            "srntype", 
            "srngrossamt", 
            "srnspldiscamt", 
            "srnschdiscamt", 
            "srncashdiscamt", 
            "srndbdiscamt", 
            "srntaxamt", 
            "srnroundoffamt", 
            "srnnetamt", 
            "salesmanname", 
            "salesroutename", 
            "rtrcode", 
            "rtrname", 
            "prdsalinvno", 
            "prdlcncode", 
            "prdcode", 
            "prdbatcde", 
            "prdsalqty", 
            "prdunsalqty", 
            "prdofferqty", 
            "prdselrate", 
            "prdgrossamt", 
            "prdspldiscamt", 
            "prdschdiscamt", 
            "prdcashdiscamt", 
            "prddbdiscamt", 
            "prdtaxamt", 
            "prdnetamt", 
            "mrp", 
            "rtnfreeqtyvalue", 
            "referencetype", 
            "salesmancode", 
            "salesroutecode", 
            "nrvalue", 
            "prdselrateaftertax", 
            "mrpcs", 
            "lpvalue", 
            "rtnwindowdisplayamt", 
            "cradjamt", 
            "rtrurccode", 
            "createddate", 
            "modifieddate", 
            "syncid", 
            "rtnlinecount", 
            "createddt",  
            "run_id",
            "crtd_dttm",
            "filename"  
        )

        file_name=file_name.split(".")[0]+"_"+ datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        
        #move to success
        snowdf.write.mode("append").saveAsTable(stage_name.split(".")[0] + "." + target_table)
        # snowdf.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)
        
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

CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.INDSDL_RAW.SDL_CSL_RDSSMWEEKLYTARGET_OUTPUT_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS 'import snowflake.snowpark as snowpark
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DateType, DoubleType,TimestampType,DecimalType
from snowflake.snowpark.functions import col,lit,date_format, try_cast, trim, to_date, left,  right
import pandas as pd
from datetime import datetime
import pytz

def main(session: snowpark.Session,Param): 

    try:
    
        # Param = [
        #     ''rdssmweeklytarget_output'',
        #     ''INDSDL_RAW.DEV_LOAD_STAGE_ADLS'',
        #     ''dev/india_xdm/master/rdssmweeklytarget_output/rdssmweeklytarget_output'',
        #     ''sdl_csl_rdssmweeklytarget_output''
        # ]
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        sch_name        = stage_name.split(''.'')[0]
        target_table    = sch_name+"."+Param[3]
    
        df_schema=StructType([
                StructField(''DISTCODE'', StringType()),
                StructField(''TARGETREFNO'', StringType()),
                StructField(''TARGETDATE'', StringType()),
                StructField(''SMCODE'', StringType()),
                StructField(''SMNAME'', StringType()),
                StructField(''RMCODE'', StringType()),
                StructField(''RMNAME'', StringType()),
                StructField(''TARGETYEAR'', StringType()),
                StructField(''TARGETMONTH'', StringType()),
                StructField(''TARGETVALUE'', StringType()),
                StructField(''TARGETNAME'', StringType()),
                StructField(''WEEK1'', StringType()),
                StructField(''WEEK2'', StringType()),
                StructField(''WEEK3'', StringType()),
                StructField(''WEEK4'', StringType()),
                StructField(''WEEK5'', StringType()),
                StructField(''TARGETSTATUS'', StringType()),
                StructField(''TARGETTYPE'', StringType()),
                StructField(''DOWNLOADSTATUS'', StringType()),
                StructField(''CREATEDDATE'', StringType())
                ])

        
        df = session.read \\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "\\u0001")\\
            .option("skip_blank_lines", True) \\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
    
        df = df.withColumn("FILE_NAME",lit(file_name).cast("string"))
        df = df.withColumn("RUN_ID",lit(datetime.now().strftime("%Y%m%d%H%M%S")))
        df = df.withColumn("CRT_DTTM",lit(datetime.now().strftime("%Y-%m-%d %H:%M:%S")))

                                    
        final_df = df.select(\\
            ''DISTCODE'',\\
            ''TARGETREFNO'',\\
            try_cast(col("TARGETDATE"), TimestampType()).alias("TARGETDATE"),\\
            ''SMCODE'',\\
            ''SMNAME'',\\
            ''RMCODE'',\\
            ''RMNAME'',\\
            try_cast(col(''TARGETYEAR''), IntegerType()).alias(''TARGETYEAR''),
            ''TARGETMONTH'',\\
            try_cast(col(''TARGETVALUE''), DecimalType()).alias(''TARGETVALUE''),
            ''TARGETNAME'',\\
            try_cast(col(''WEEK1''), DecimalType()).alias(''WEEK1''),\\
            try_cast(col(''WEEK2''), DecimalType()).alias(''WEEK2''),\\
            try_cast(col(''WEEK3''), DecimalType()).alias(''WEEK3''),\\
            try_cast(col(''WEEK4''), DecimalType()).alias(''WEEK4''),\\
            try_cast(col(''WEEK5''), DecimalType()).alias(''WEEK5''),\\
            ''TARGETSTATUS'',\\
            ''TARGETTYPE'',\\
            ''DOWNLOADSTATUS'',\\
            try_cast(col("CREATEDDATE"), TimestampType()).alias("CREATEDDATE"),\\
            try_cast(col("RUN_ID"), IntegerType()).alias("RUN_ID"),\\
            try_cast(col("CRT_DTTM"), TimestampType()).alias("CRT_DTTM"),\\
            "FILE_NAME"
        ).alias("final_df")
        
        final_df.write.mode("append").saveAsTable(target_table)
                                    
        return "Success"
        
    except KeyError as key_error:
        # Handle KeyError (missing columns) here
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message
        
    except Exception as e:
        # Handle exceptions here
        error_message = f"Error: {str(e)}" +str(df.columns)
        return error_message';

CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.INDSDL_RAW.SDL_CSL_SCHEME_HEADER_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS 'import snowflake.snowpark as snowpark
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DateType, DoubleType,TimestampType,DecimalType
from snowflake.snowpark.functions import col,lit,date_format, try_cast, trim, to_date, left,  right, when
import pandas as pd
from datetime import datetime
import pytz

def main(session: snowpark.Session,Param): 

    try:
    
        # Param = [
        #     ''scheme_header'',
        #     ''INDSDL_RAW.DEV_LOAD_STAGE_ADLS'',
        #     ''dev/india_xdm/master/scheme_header/scheme_header'',
        #     ''sdl_csl_scheme_header''
        # ]
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        sch_name        = stage_name.split(''.'')[0]
        target_table    = sch_name+"."+Param[3]
    
        df_schema=StructType([
                StructField(''CmpCode'', StringType()),
                StructField(''SchemeCode'', StringType()),
                StructField(''CmpSchemeCode'', StringType()),
                StructField(''SchemeName'', StringType()),
                StructField(''SchemeBase'', StringType()),
                StructField(''SchemeType'', StringType()),
                StructField(''SchemeStartDt'', StringType()),
                StructField(''SchemeEndDt'', StringType()),
                StructField(''PayOutCount'', StringType()),
                StructField(''PayOutType'', StringType()),
                StructField(''PayoutFrequency'', StringType()),
                StructField(''IsOpen'', StringType()),
                StructField(''IsClaimable'', StringType()),
                StructField(''ClaimableOn'', StringType()),
                StructField(''ClaimGroup'', StringType()),
                StructField(''IsCombi'', StringType()),
                StructField(''IsFlexi'', StringType()),
                StructField(''FlexiType'', StringType()),
                StructField(''UseProrata'', StringType()),
                StructField(''BudgetType'', StringType()),
                StructField(''TotalBudget'', StringType()),
                StructField(''RetailerCap'', StringType()),
                StructField(''InvoiceCap'', StringType()),
                StructField(''UnCheckScheme'', StringType()),
                StructField(''ModUserCode'', StringType()),
                StructField(''ModDt'', StringType()),
                StructField(''CreatedDt'', StringType()),
                ])

        
        df = session.read \\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "\\u0001")\\
            .option("skip_blank_lines", True) \\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
    
        

        mod_df = df.select(
                lit(None).alias("schid"),
                col("SchemeCode").alias("schcode"),
                col("SchemeName").alias("schdsc"),
                when(col("IsClaimable") == "Y", 1).when(col("IsClaimable") == "N", 0).alias("claimable"),
                when(col("IsClaimable") == "SellRate", 1).when(col("IsClaimable") == "None", 0).alias("clmamton"),
                col("CmpSchemeCode").alias("cmpschcode"),
                lit(None).alias("schlevel_id"),
                col("schemebase").alias("schtype"),
                when(col("isflexi") == "N", 1).when(col("isflexi") == "Y", 0).alias("flexisch"),
                when(col("flexitype") == "N", 1).when(col("flexitype") == "Y", 0).alias("flexischtype"),
                when(col("iscombi") == "N", 1).when(col("iscombi") == "Y", 0).alias("combisch"),
                lit(None).alias("RANGE"),
                when(col("useprorata") == "N", 1).when(col("useprorata") == "Y", 0).alias("prorata"),
                when(col("schemetype") == "Non", 0).otherwise(1).alias("qps"),
                lit(None).alias("qpsreset"),
                col("SchemeStartDt").alias("schvalidfrom"),
                col("SchemeEndDt").alias("schvalidtill"),
                when(col("isopen") == "P", 0).when(col("isopen") == "Y", 1).alias("schstatus"),
                col("TotalBudget").alias("budget"),
                lit(None).alias("adjwindisponlyonce"),
                lit(None).alias("purofevery"),
                lit(None).alias("apyqpssch"),
                lit(None).alias("setwindowdisp"),
                lit(None).alias("editscheme"),
                lit(None).alias("schlvlmode"),
                lit(None).alias("createduserid"),
                lit(None).alias("createddate"),
                col("ModUserCode").alias("modifiedby"),
                col("ModDt").alias("modifieddate"),
                lit(None).alias("versionno"),
                lit(None).alias("serialno"),
                col("ClaimGroup").alias("claimgrpcode"),
                lit(None).alias("fbm"),
                lit(None).alias("combitype"),
                lit(None).alias("allowuncheck"),
                lit(None).alias("settlementtype"),
                lit(None).alias("consumerpromo"),
                lit(None).alias("wdsbillscount"),
                lit(None).alias("wdscapamount"),
                lit(None).alias("wdsminpurqty")
            )

        mod_df = mod_df.withColumn("FILE_NAME",lit(file_name).cast("string"))
        mod_df = mod_df.withColumn("RUN_ID",lit(datetime.now().strftime("%Y%m%d%H%M%S")))
        mod_df = mod_df.withColumn("CRT_DTTM",lit(datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
                                    
        final_df = mod_df.select(\\
            try_cast(col(''SCHID''), IntegerType()).alias(''SCHID''),\\
            ''SCHCODE'',\\
            ''SCHDSC'',\\
            try_cast(col(''CLAIMABLE''), IntegerType()).alias(''CLAIMABLE''),\\
            try_cast(col(''CLMAMTON''), IntegerType()).alias(''CLMAMTON''),\\
            ''CMPSCHCODE'',\\
            try_cast(col(''SCHLEVEL_ID''), IntegerType()).alias(''SCHLEVEL_ID''),\\
            ''SCHTYPE'',\\
            try_cast(col(''FLEXISCH''), IntegerType()).alias(''FLEXISCH''),\\
            try_cast(col(''FLEXISCHTYPE''), IntegerType()).alias(''FLEXISCHTYPE''),\\
            try_cast(col(''COMBISCH''), IntegerType()).alias(''COMBISCH''),\\
            try_cast(col(''RANGE''), IntegerType()).alias(''RANGE''),\\
            try_cast(col(''PRORATA''), IntegerType()).alias(''PRORATA''),\\
            ''QPS'',\\
            try_cast(col(''QPSRESET''), IntegerType()).alias(''QPSRESET''),\\
            try_cast(col("SCHVALIDFROM"), TimestampType()).alias("SCHVALIDFROM"),\\
            try_cast(col("SCHVALIDTILL"), TimestampType()).alias("SCHVALIDTILL"),\\
            try_cast(col(''SCHSTATUS''), IntegerType()).alias(''SCHSTATUS''),\\
            try_cast(col(''BUDGET''), IntegerType()).alias(''BUDGET''),\\
            try_cast(col(''ADJWINDISPONLYONCE''), IntegerType()).alias(''ADJWINDISPONLYONCE''),\\
            try_cast(col(''PUROFEVERY''), IntegerType()).alias(''PUROFEVERY''),\\
            try_cast(col(''APYQPSSCH''), IntegerType()).alias(''APYQPSSCH''),\\
            try_cast(col(''SETWINDOWDISP''), IntegerType()).alias(''SETWINDOWDISP''),\\
            try_cast(col(''EDITSCHEME''), IntegerType()).alias(''EDITSCHEME''),\\
            try_cast(col(''SCHLVLMODE''), IntegerType()).alias(''SCHLVLMODE''),\\
            try_cast(col(''CREATEDUSERID''), IntegerType()).alias(''CREATEDUSERID''),\\
            try_cast(col("CREATEDDATE"), TimestampType()).alias("CREATEDDATE"),\\
            ''MODIFIEDBY'',\\
            try_cast(col("MODIFIEDDATE"), TimestampType()).alias("MODIFIEDDATE"),\\
            try_cast(col(''VERSIONNO''), IntegerType()).alias(''VERSIONNO''),\\
            ''SERIALNO'',\\
            ''CLAIMGRPCODE'',\\
            try_cast(col(''FBM''), IntegerType()).alias(''FBM''),\\
            try_cast(col(''COMBITYPE''), IntegerType()).alias(''COMBITYPE''),\\
            try_cast(col(''ALLOWUNCHECK''), IntegerType()).alias(''ALLOWUNCHECK''),\\
            try_cast(col(''SETTLEMENTTYPE''), IntegerType()).alias(''SETTLEMENTTYPE''),\\
            try_cast(col(''CONSUMERPROMO''), IntegerType()).alias(''CONSUMERPROMO''),\\
            try_cast(col(''WDSBILLSCOUNT''), IntegerType()).alias(''WDSBILLSCOUNT''),\\
            try_cast(col(''WDSCAPAMOUNT''), IntegerType()).alias(''WDSCAPAMOUNT''),\\
            try_cast(col(''WDSMINPURQTY''), IntegerType()).alias(''WDSMINPURQTY''),\\
            "RUN_ID",\\
            try_cast(col("CRT_DTTM"), TimestampType()).alias("CRT_DTTM"),
            "FILE_NAME"
        ).alias("final_df")
        
        final_df.write.mode("append").saveAsTable(target_table)
                                    
        return final_df
        
    except KeyError as key_error:
        # Handle KeyError (missing columns) here
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message
        
    except Exception as e:
        # Handle exceptions here
        error_message = f"Error: {str(e)}" +str(df.columns)
        return error_message';

CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.INDSDL_RAW.XDM_DISTRIBUTOR_SUPPLIER_PREPROCESSING("PARAM" ARRAY)
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
    # Param=[''distributorsupplier_20240603033304.csv'',''INDSDL_RAW.DEV_LOAD_STAGE_ADLS'',''dev/india_dna/master/distributorsupplier/in_distributor_supplier'',''SDL_XDM_DISTRIBUTOR_SUPPLIER'']
    try:
        file_name = Param[0]
        stage_name = Param[1]
        temp_stage_path = Param[2]
        target_table = Param[3]
        
        df_schema = StructType([
            StructField("cmpcode", StringType(), True),
            StructField("distrcode", StringType(), True),     
            StructField("supcode", StringType(), True),
            StructField("isdefault", StringType(), True),
            StructField("modusercode", StringType(), True),
            StructField("moddt", StringType(), True),
            StructField("createddt", StringType(), True),
            
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


        df = df.withColumn("crtd_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        df = df.withColumn("filename", lit(file_name))
        df = df.withColumn("run_id", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))


        snowdf = df.select(
            "cmpcode", 
            "distrcode",      
            "supcode", 
            "isdefault", 
            "modusercode", 
            "moddt", 
            "createddt",  
            "filename",
            "run_id",
            "crtd_dttm"
            )


        file_name=file_name.split(".")[0]+"_"+ datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        
        #move to success
        snowdf.write.mode("append").saveAsTable(stage_name.split(".")[0] + "." + target_table)
        # snowdf.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)
        
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

CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.INDSDL_RAW.XDM_PRODUCTUOM_PREPROCESSING("PARAM" ARRAY)
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
    # Param=[''productuom_20240604033400.csv'',''INDSDL_RAW.DEV_LOAD_STAGE_ADLS'',''dev/india_dna/master/productuom/in_product_uom'',''SDL_XDM_PRODUCTUOM'']
    try:
        file_name = Param[0]
        stage_name = Param[1]
        temp_stage_path = Param[2]
        target_table = Param[3]
        
        df_schema = StructType([
            StructField("cmpcode", StringType(), True),
            StructField("prodcode", StringType(), True),     
            StructField("uomcode", StringType(), True),
            StructField("uomconvfactor", StringType(), True),
            StructField("modusercode", StringType(), True),
            StructField("moddt", StringType(), True),
            StructField("createddt", StringType(), True),
        
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


        df = df.withColumn("crtd_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        df = df.withColumn("filename", lit(file_name))
        df = df.withColumn("run_id", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))


        snowdf = df.select(
            "cmpcode", 
            "prodcode",      
            "uomcode", 
            "uomconvfactor", 
            "modusercode", 
            "moddt", 
            "createddt",  
            "filename",
            "run_id",
            "crtd_dttm"
            )


        file_name=file_name.split(".")[0]+"_"+ datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        
        #move to success
        snowdf.write.mode("append").saveAsTable(stage_name.split(".")[0] + "." + target_table)
        # snowdf.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)
        
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

CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.INDSDL_RAW.XDM_PRODUCT_PREPROCESSING("PARAM" ARRAY)
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
    # Param=[''product_20240604033403.csv'',''INDSDL_RAW.DEV_LOAD_STAGE_ADLS'',''dev/india_dna/master/product/in_product'',''SDL_XDM_PRODUCT'']
    try:
        file_name = Param[0]
        stage_name = Param[1]
        temp_stage_path = Param[2]
        target_table = Param[3]
        
        df_schema = StructType([
            StructField("prodcompany", StringType(), True),
            StructField("productcode", StringType(), True),     
            StructField("productuom", StringType(), True),
            StructField("uomconvfactor", StringType(), True),
            StructField("prodhierarchylvl", StringType(), True),
            StructField("prodhierarchyval", StringType(), True),
            StructField("productname", StringType(), True),
            StructField("prodshortname", StringType(), True),
            StructField("productcmpcode", StringType(), True),     
            StructField("stockcovdays", StringType(), True),
            StructField("productweight", StringType(), True),
            StructField("productunit", StringType(), True),
            StructField("productstatus", StringType(), True),
            StructField("productdrugtype", StringType(), True),
            StructField("serialno", StringType(), True),
            StructField("shelflife", StringType(), True),
            StructField("franchisecode", StringType(), True),
            StructField("franchisename", StringType(), True),
            StructField("brandcode", StringType(), True),     
            StructField("brandname", StringType(), True),
            StructField("product_code", StringType(), True),
            StructField("product_name", StringType(), True),
            StructField("variantcode", StringType(), True),
            StructField("variantname", StringType(), True),
            StructField("motherskucode", StringType(), True),     
            StructField("motherskuname", StringType(), True),
            StructField("eanno", StringType(), True),
            StructField("createddt", StringType(), True),
    
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

       
        df = df.withColumn("crtd_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        df = df.withColumn("filename", lit(file_name))
        df = df.withColumn("run_id", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))


        snowdf = df.select(
            "prodcompany",
            "productcode",     
            "productuom",
            "uomconvfactor",
            "prodhierarchylvl",
            "prodhierarchyval",
            "productname",
            "prodshortname",
            "productcmpcode",     
            "stockcovdays",
            "productweight",
            "productunit",
            "productstatus",
            "productdrugtype",
            "serialno",
            "shelflife",
            "franchisecode",
            "franchisename",
            "brandcode",     
            "brandname",
            "product_code",
            "product_name",
            "variantcode",
            "variantname",
            "motherskucode",     
            "motherskuname",
            "eanno",
            "createddt",
            "filename",
            "run_id",
            "crtd_dttm"
            )


        file_name=file_name.split(".")[0]+"_"+ datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        
        #move to success
        snowdf.write.mode("append").saveAsTable(stage_name.split(".")[0] + "." + target_table)
        # snowdf.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)
        
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
