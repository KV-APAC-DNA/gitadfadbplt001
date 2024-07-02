SET ENV ='PROD';

SET DB = $ENV||'_DNA_LOAD';

USE DATABASE identifier($DB);

USE SCHEMA INDSDL_RAW;


CREATE OR REPLACE PROCEDURE INDSDL_RAW.IN_POS_MAX_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS
$$
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
        sch_name        = stage_name.split('.')[0]
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

        df = session.read\
            .schema(df_schema)\
            .option("skip_header",1)\
            .option("field_delimiter", "\u0001")\
            .option("field_optionally_enclosed_by", "\"") \
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        

        df = df.withColumn("FILE_NAME",lit(file_name).cast("string"))
        df = df.withColumn("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        df = df.withColumn("FILE_UPLOADED_DATE",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d")))
        df = df.withColumn("CRT_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        
        final_df = df.select("Account_Name", to_date(concat(col("Year"), lit("-"), col("Month"), lit("-15")),lit("YYYY-mon-DD")).as_("POST_DT"), \
                            "Level",  "Store_Code",  "Subcategory", "Article_Code", "Sales_Value_Rs_", "Sales_Qty", \
                            "FILE_NAME", "RUN_ID", "FILE_UPLOADED_DATE", "CRT_DTTM" ).alias("final_df")

        if final_df.count()==0:
            return "No Data in file"
        
        # Load Data to the target table
        final_df.write.mode("append").saveAsTable(target_table)

        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")

        # write to success folder
    
        file_name=file_name.split(".")[0] + '_' + datetime.now().strftime("%Y%m%d%H%M%S")
        final_df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,file_format_type="csv",OVERWRITE=True,header=True)
   
        return "Success"
        
    except KeyError as key_error:
        
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message
        
    except Exception as e:
        
        error_message = f"Error: {str(e)}" +str(df.columns)
        return error_message
$$;



CREATE OR REPLACE PROCEDURE INDSDL_RAW.IN_POS_HG_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS
$$
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
        sch_name        = stage_name.split('.')[0]
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

        df = session.read\
            .schema(df_schema)\
            .option("skip_header",1)\
            .option("field_delimiter", "\u0001")\
            .option("field_optionally_enclosed_by", "\"") \
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        

        df = df.withColumn("FILE_NAME",lit(file_name).cast("string"))
        df = df.withColumn("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        df = df.withColumn("FILE_UPLOADED_DATE",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d")))
        df = df.withColumn("CRT_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        
        final_df = df.select("Account_Name", to_date(concat(col("Year"), lit("-"), col("Month"), lit("-15")),lit("YYYY-mon-DD")).as_("POST_DT"), \
                            "Level",  "Store_Code",  "Subcategory", "Article_Code", "Sales_Value_Rs_", "Sales_Qty", \
                            "FILE_NAME", "RUN_ID", "FILE_UPLOADED_DATE", "CRT_DTTM" ).alias("final_df")

        if final_df.count()==0:
            return "No Data in file"
        
        # Load Data to the target table
        final_df.write.mode("append").saveAsTable(target_table)

        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")

        # write to success folder
    
        file_name=file_name.split(".")[0] + '_' + datetime.now().strftime("%Y%m%d%H%M%S")
        final_df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,file_format_type="csv",OVERWRITE=True,header=True)
   
        return "Success"
        
    except KeyError as key_error:
        
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message
        
    except Exception as e:
        
        error_message = f"Error: {str(e)}" +str(df.columns)
        return error_message
$$;


CREATE OR REPLACE PROCEDURE INDSDL_RAW.IN_RRL_UDCMAPPING_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS
$$
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
        sch_name        = stage_name.split('.')[0]
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

        df = session.read\
            .schema(df_schema)\
            .option("skip_header",1)\
            .option("field_delimiter", "|")\
            .option("field_optionally_enclosed_by", "\"") \
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        df = df.withColumn("FILE_NAME",lit(file_name).cast("string"))
        df = df.withColumn("CRT_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))

        final_df = df.select("distCode", "RsdCode", "OutletCode", "UserCode", "UdcCode", \
            to_timestamp("CreatedDate", lit("YYYY/MM/DD HH24:MI:SS")).as_("CreatedDate"), \
            "IsActive", "IsDelFlag", "RowId", "FILE_NAME", "CRT_DTTM" ).alias("final_df")

        if final_df.count()==0:
            return "No Data in file"
        
        # Load Data to the target table
        final_df.write.mode("append").saveAsTable(target_table)

        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")

        # write to success folder
    
        file_name=file_name.split(".")[0] + '_' + datetime.now().strftime("%Y%m%d%H%M%S")
        final_df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,file_format_type="csv",OVERWRITE=True,header=True)
   

        return "Success"
        
    except KeyError as key_error:
        
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message
        
    except Exception as e:
        
        error_message = f"Error: {str(e)}" +str(df.columns)
        return error_message
$$;


CREATE OR REPLACE PROCEDURE INDSDL_RAW.IN_RRL_USERMASTER_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS
$$
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
        sch_name        = stage_name.split('.')[0]
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

        df = session.read\
            .schema(df_schema)\
            .option("skip_header",1)\
            .option("field_delimiter", "|")\
            .option("field_optionally_enclosed_by", "\"") \
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        df = df.withColumn("FILE_NAME",lit(file_name).cast("string"))
        df = df.withColumn("CRT_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))

        final_df = df.select("UserID", "UserCode", "Login", "Password", "EUserName", "UserLevel", "ParentID", "IsActive", \
                            "TeritoryID", "ABNumber", "ForumCode", "RegionId", "EmailID", "CurrentVersion", "UpdateVersion", \
                            "IMEI", "MobileNo", "LocationID", "IsHHT", "User_CreatedDate", "DistUserId", "FreezeDay", \
                            "FILE_NAME", "CRT_DTTM" ).alias("final_df")

        if final_df.count()==0:
            return "No Data in file"
        
        # Load Data to the target table
        final_df.write.mode("append").saveAsTable(target_table)

        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")

        # write to success folder
    
        file_name=file_name.split(".")[0] + '_' + datetime.now().strftime("%Y%m%d%H%M%S")
        final_df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,file_format_type="csv",OVERWRITE=True,header=True)
   
        return "Success"
        
    except KeyError as key_error:
        
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message
        
    except Exception as e:
        
        error_message = f"Error: {str(e)}" +str(df.columns)
        return error_message
$$;


CREATE OR REPLACE PROCEDURE INDSDL_RAW.IN_SSS_SCORECARD_DATA_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS
$$
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
        sch_name        = stage_name.split('.')[0]
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

        df = session.read\
            .schema(df_schema)\
            .option("skip_header",1)\
            .option("field_delimiter", "\u0001")\
            .option("field_optionally_enclosed_by", "\"") \
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        df = df.withColumn("CRT_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        df = df.withColumn("FILE_NAME",lit(file_name).cast("string"))
        df = df.withColumn("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))

        final_df = df.select("Program_Type", "JNJ_ID", "RS_ID", "Outlet_Name", "Region", "Zone", "Territory", "City", \
                            "Franchise", "KPI", "Quarter", "Year", "Target", "Actual", \
                            "CRT_DTTM", "FILE_NAME", "RUN_ID").alias("final_df")

        if final_df.count()==0:
            return "No Data in file"
        
        # Load Data to the target table
        final_df.write.mode("append").saveAsTable(target_table)

        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")

        # write to success folder
    
        file_name=file_name.split(".")[0] + '_' + datetime.now().strftime("%Y%m%d%H%M%S")
        final_df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,file_format_type="csv",OVERWRITE=True,header=True)
   
        return "Success"
        
    except KeyError as key_error:
        
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message
        
    except Exception as e:
        
        error_message = f"Error: {str(e)}" +str(df.columns)
        return error_message
$$;


CREATE OR REPLACE PROCEDURE INDSDL_RAW.IN_WINCULUM_DAILYSALES_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS
$$
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
        sch_name        = stage_name.split('.')[0]
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
 
        df = session.read\
            .schema(df_schema)\
            .option("skip_header",1)\
            .option("field_delimiter", "\u0001")\
            .option("field_optionally_enclosed_by", "\"") \
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        df2=df.filter(col("DistCode").is_not_null()) and df.filter(col("salinvdate").is_not_null())  and df.filter(col("salinvno").is_not_null()) \
            and df.filter(col("RtrCode").is_not_null()) and df.filter(col("Productcode").is_not_null())
            
        df2 = df2.withColumn("FILE_NAME",lit(file_name).cast("string"))
        df2 = df2.withColumn("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        df2 = df2.withColumn("CRT_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        
        final_df = df2.select("DistCode", "salinvdate", "salinvno", "RtrCode", "Productcode", "PrdQty", "NR", "Total_Price", "Tax", \
                            "FILE_NAME", "RUN_ID", "CRT_DTTM" ).alias("final_df")

                        
                            
        if final_df.count()==0:
            return "No Data in file"
        
        # Load Data to the target table
        final_df.write.mode("append").saveAsTable(target_table)
        
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")

        # write to success folder
    
        file_name=file_name.split(".")[0] + '_' + datetime.now().strftime("%Y%m%d%H%M%S")
        final_df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,file_format_type="csv",OVERWRITE=True,header=True)
   
        return "Success"
        
    except KeyError as key_error:
        
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message
        
    except Exception as e:
        
        error_message = f"Error: {str(e)}" +str(df.columns)
        return error_message
$$;

CREATE OR REPLACE PROCEDURE INDSDL_RAW.IN_WINCULUM_SALESRETURN_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS
$$
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
        sch_name        = stage_name.split('.')[0]
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
 
        df = session.read\
            .schema(df_schema)\
            .option("skip_header",1)\
            .option("field_delimiter", "\u0001")\
            .option("field_optionally_enclosed_by", "\"") \
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        df2=df.filter(col("DistCode").is_not_null()) and df.filter(col("SRNDate").is_not_null())  and df.filter(col("SRNRefNo").is_not_null()) \
            and df.filter(col("RtrCode").is_not_null()) and df.filter(col("Productcode").is_not_null())
            
        df2 = df2.withColumn("FILE_NAME",lit(file_name).cast("string"))
        df2 = df2.withColumn("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        df2 = df2.withColumn("CRT_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        
        final_df = df2.select("DistCode", "SRNDate", "SRNRefNo", "RtrCode", "Productcode", "PrdQty", "NR", "Total_Price", "Tax", \
                            "FILE_NAME", "RUN_ID", "CRT_DTTM" ).alias("final_df")

                        
                            
        if final_df.count()==0:
            return "No Data in file"
        
        # Load Data to the target table
        final_df.write.mode("append").saveAsTable(target_table)
        
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")

        # write to success folder
    
        file_name=file_name.split(".")[0] + '_' + datetime.now().strftime("%Y%m%d%H%M%S")
        final_df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,file_format_type="csv",OVERWRITE=True,header=True)
   
        return "Success"
        
    except KeyError as key_error:
        
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message
        
    except Exception as e:
        
        error_message = f"Error: {str(e)}" +str(df.columns)
        return error_message
$$;

CREATE OR REPLACE PROCEDURE INDSDL_RAW.IN_BATCH_MASTER_PREPROCESSING("PARAM" ARRAY)
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
                    StructField("statecode", StringType()),
                    StructField("prodcode", StringType()),
                    StructField("mrp", StringType()),
                    StructField("lsp", StringType()),
                    StructField("sellrate", StringType()),
                    StructField("purchrate", StringType()),
                    StructField("claimrate", StringType()),
                    StructField("netrate", StringType()),
                    StructField("createddt", StringType())
                ])

        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "\\u0001")\\
            .option("field_optionally_enclosed_by", "\\"") \\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        df = df.withColumn("FILE_NAME",lit(file_name).cast("string"))
        df = df.withColumn("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Kolkata")).strftime("%Y%m%d%H%M%S")))
        df = df.withColumn("CRT_DTTM",lit(datetime.now(pytz.timezone("Asia/Kolkata")).strftime("%Y-%m-%d %H:%M:%S")))

        final_df = df.select("statecode", "prodcode", "mrp", "lsp", "sellrate", "purchrate","claimrate","netrate","createddt", \\
             "FILE_NAME","RUN_ID","CRT_DTTM" ).alias("final_df")

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


CREATE OR REPLACE PROCEDURE INDSDL_RAW.IN_RETAILER_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS 
$$

import snowflake.snowpark as snowpark
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DateType, DoubleType,TimestampType
from snowflake.snowpark.functions import col,lit,date_format, try_cast, trim, to_date, left,  right
import pandas as pd
from datetime import datetime
import pytz

def main(session: snowpark.Session, Param): 

    try:
    
        # Param = [
        #     'retailer_20240517061542.csv',
        #     'INDSDL_RAW.DEV_LOAD_STAGE_ADLS',
        #     'dev/india_dna/master/retailer/in_retailer',
        #     'sdl_in_retailer'
        # ]
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        sch_name        = stage_name.split('.')[0]
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
    
        df = session.read\
            .schema(df_schema)\
            .option("skip_header",1)\
            .option("field_delimiter", "\u0001")\
            .option("skip_blank_lines", True) \
            .option("field_optionally_enclosed_by", None) \
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
    
        df = df.withColumn("FILE_NAME",lit(file_name).cast("string"))
        df = df.withColumn("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        df = df.withColumn("CRT_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
    
        final_df = df.select(\
            "cmpcode", "distcode", "distrbrcode", "rtrcode", "rtrtype", "rtrname", "rtruniquecode", "channelcode", \
            "retlrgroupcode", "classcode", "rtrphoneno", "rtrcontactperson", "emailid", \
            try_cast(col("regdate"), DateType()).alias("regdate"),\
            "rtrlicno", try_cast(col("rtrlicexpirydate"), DateType()).alias("rtrlicexpirydate"), \
            "druglno", try_cast(col("rtrdrugexpirydate"), DateType()).alias("rtrdrugexpirydate"), \
            try_cast(col("rtrcrbills"),IntegerType()).alias("rtrcrbills"), \
            try_cast(col("rtrcrdays"),IntegerType()).alias("rtrcrdays"), \
            try_cast(col("rtrcrlimit"), DoubleType()).alias("rtrcrlimit"), \
            "relationstatus", "parentcode", "status", "rtrlatitude", "rtrlongitude", "csrtrcode", \
            "keyaccount", "rtrfoodlicno", "pannumber", "retailertype", "composite", "relatedparty", "statename", \
            try_cast(col("lastmoddate"), TimestampType()).alias("lastmoddate"),\
            try_cast(col("createddt"), TimestampType()).alias("createddt"),\
            "FILE_NAME", "RUN_ID", "CRT_DTTM" \
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
$$;


CREATE OR REPLACE PROCEDURE INDSDL_RAW.IN_RETAILER_ROUTE_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS 
$$

import snowflake.snowpark as snowpark
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DateType, DoubleType,TimestampType
from snowflake.snowpark.functions import col,lit,date_format, try_cast, trim, to_date, left,  right
import pandas as pd
from datetime import datetime
import pytz

def main(session: snowpark.Session, Param): 

    try:
    
        # Param = [
        #     'retailer_route_20240517061753.csv',
        #     'INDSDL_RAW.DEV_LOAD_STAGE_ADLS',
        #     'dev/india_dna/master/retailer_route/in_retailer_route',
        #     'sdl_in_retailer_route'
        # ]
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        sch_name        = stage_name.split('.')[0]
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
    
        df = session.read\
            .schema(df_schema)\
            .option("skip_header",1)\
            .option("field_delimiter", "\u0001")\
            .option("skip_blank_lines", True) \
            .option("field_optionally_enclosed_by", None) \
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
    
        df = df.withColumn("FILE_NAME",lit(file_name).cast("string"))
        df = df.withColumn("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        df = df.withColumn("CRT_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
    
        final_df = df.select(\
            "cmpcode", "distcode", "distrbrcode", "rtrcode", "RMCode", "RouteType", \
            try_cast(col("CoverageSequence"),IntegerType()).alias("CoverageSequence"), \
            try_cast(col("createddt"), TimestampType()).alias("createddt"),\
            "FILE_NAME", "RUN_ID", "CRT_DTTM" \
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
$$;


CREATE OR REPLACE PROCEDURE INDSDL_RAW.IN_SALESMAN_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS 
$$

import snowflake.snowpark as snowpark
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DateType, DoubleType,TimestampType
from snowflake.snowpark.functions import col,lit,date_format, try_cast, trim, to_date, left,  right
import pandas as pd
from datetime import datetime
import pytz

def main(session: snowpark.Session, Param): 

    try:
    
        # Param = [
        #     'salesman_20240517061916.csv',
        #     'INDSDL_RAW.DEV_LOAD_STAGE_ADLS',
        #     'dev/india_dna/master/salesman/in_salesman',
        #     'sdl_in_salesman'
        # ]
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        sch_name        = stage_name.split('.')[0]
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
    
        df = session.read\
            .schema(df_schema)\
            .option("skip_header",1)\
            .option("field_delimiter", "\u0001")\
            .option("skip_blank_lines", True) \
            .option("field_optionally_enclosed_by", None) \
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
    
        df = df.withColumn("FILE_NAME",lit(file_name).cast("string"))
        df = df.withColumn("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        df = df.withColumn("CRT_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
    
        final_df = df.select(\
            "cmpcode", "distcode", "distrbrcode", "SMCode", "SMName", "smphoneno", "SMEmail", "RDSSMType", \
            try_cast(col("SMDailyAllowance"), DoubleType()).alias("SMDailyAllowance"), \
            try_cast(col("SMMonthlySalary"), DoubleType()).alias("SMMonthlySalary"), \
            try_cast(col("SMMktCredit"), DoubleType()).alias("SMMktCredit"), \
            try_cast(col("SMCreditDays"),IntegerType()).alias("SMCreditDays"), \
            "Status", "ModUserCode", \
            try_cast(col("ModDt"), DateType()).alias("ModDt"),\
            "AadhaarNo", "UniqueSalesCode", \
            try_cast(col("createddt"), TimestampType()).alias("createddt"),\
            "FILE_NAME", "RUN_ID", "CRT_DTTM" \
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
$$;


CREATE OR REPLACE PROCEDURE INDSDL_RAW.IN_SALESMAN_ROUTE_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS 
$$

import snowflake.snowpark as snowpark
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DateType, DoubleType,TimestampType
from snowflake.snowpark.functions import col,lit,date_format, try_cast, trim, to_date, left,  right
import pandas as pd
from datetime import datetime
import pytz

def main(session: snowpark.Session, Param): 

    try:
    
        # Param = [
        #     'salesman_route_20240517061954.csv',
        #     'INDSDL_RAW.DEV_LOAD_STAGE_ADLS',
        #     'dev/india_dna/master/salesman_route/in_salesman_route',
        #     'sdl_in_salesman_route'
        # ]
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        sch_name        = stage_name.split('.')[0]
        target_table    = sch_name+"."+Param[3]
    
        df_schema=StructType([
                StructField("cmpcode", StringType()),
                StructField("DistrCode", StringType()),
                StructField("distrbrcode", StringType()),
                StructField("SalesmanCode", StringType()),
                StructField("RouteCode", StringType()),
                StructField("createddt", StringType())
                ])
    
        df = session.read\
            .schema(df_schema)\
            .option("skip_header",1)\
            .option("field_delimiter", "\u0001")\
            .option("skip_blank_lines", True) \
            .option("field_optionally_enclosed_by", None) \
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
    
        df = df.withColumn("FILE_NAME",lit(file_name).cast("string"))
        df = df.withColumn("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        df = df.withColumn("CRT_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
    
        final_df = df.select(\
            "cmpcode", "DistrCode", "distrbrcode", "SalesmanCode", "RouteCode", \
            try_cast(col("createddt"), TimestampType()).alias("createddt"),\
            "FILE_NAME", "RUN_ID", "CRT_DTTM" \
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
$$;



CREATE OR REPLACE PROCEDURE INDSDL_RAW.IN_ROUTE_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS 
$$

import snowflake.snowpark as snowpark
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DateType, DoubleType,TimestampType
from snowflake.snowpark.functions import col,lit,date_format, try_cast, trim, to_date, left,  right
import pandas as pd
from datetime import datetime
import pytz

def main(session: snowpark.Session, Param): 

    try:
    
        # Param = [
        #     'route_20240517062034.csv',
        #     'INDSDL_RAW.DEV_LOAD_STAGE_ADLS',
        #     'dev/india_dna/master/route/in_route',
        #     'sdl_in_route'
        # ]
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        sch_name        = stage_name.split('.')[0]
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
    
        df = session.read\
            .schema(df_schema)\
            .option("skip_header",1)\
            .option("field_delimiter", "\u0001")\
            .option("skip_blank_lines", True) \
            .option("field_optionally_enclosed_by", None) \
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
    
        df = df.withColumn("FILE_NAME",lit(file_name).cast("string"))
        df = df.withColumn("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        df = df.withColumn("CRT_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))

        final_df = df.select(\
            "cmpcode", "distcode", "distrbrcode", "RMCode", "RouteType", "RMName", \
            try_cast(col("Distance"),IntegerType()).alias("Distance"), \
            "VanRoute", "Status", \
            try_cast(col("RMPopulation"),IntegerType()).alias("RMPopulation"), \
            "LocalUpCountry", \
            try_cast(col("createddt"), TimestampType()).alias("createddt"),\
            "FILE_NAME", "RUN_ID", "CRT_DTTM" \
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
$$;


CREATE OR REPLACE PROCEDURE INDSDL_RAW.IN_CSL_RETAILERROUTE_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS 
$$

import snowflake.snowpark as snowpark
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DateType, DoubleType,TimestampType
from snowflake.snowpark.functions import col,lit,date_format, try_cast, trim, to_date, left,  right
import pandas as pd
from datetime import datetime
import pytz

def main(session: snowpark.Session, Param): 

    try:
    
        # Param = [
        #     'route_20240517062034.csv',
        #     'INDSDL_RAW.DEV_LOAD_STAGE_ADLS',
        #     'dev/india_dna/master/route/in_route',
        #     'sdl_in_route'
        # ]
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        sch_name        = stage_name.split('.')[0]
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
    
        df = session.read\
            .schema(df_schema)\
            .option("skip_header",1)\
            .option("field_delimiter", "\u0001")\
            .option("skip_blank_lines", True) \
            .option("field_optionally_enclosed_by", None) \
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
    
        df = df.withColumn("FILE_NAME",lit(file_name).cast("string"))
        df = df.withColumn("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        df = df.withColumn("CRT_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))

        final_df = df.select(\
            "distcode",  lit(None).as_("rtrid"),  "rtrcode",  lit(None).as_("rtrname"),  lit(None).as_("rmid"),  "rmcode",\
            lit(None).as_("rmname"),  "routetype",  lit(None).as_("uploadflag"),  \
            try_cast(col("createddt"), TimestampType()).alias("createddate"),  lit(None).as_("syncid"), \
            "RUN_ID", "CRT_DTTM", "FILE_NAME" \
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
$$;


CREATE OR REPLACE PROCEDURE INDSDL_RAW.IN_CSL_RETAILERHIERARCHY_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS 
$$

import snowflake.snowpark as snowpark
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DateType, DoubleType,TimestampType
from snowflake.snowpark.functions import col,lit,date_format, try_cast, trim, to_date, left,  right
import pandas as pd
from datetime import datetime
import pytz

def main(session: snowpark.Session, Param): 

    try:
    
        # Param = [
        #     'route_20240517062034.csv',
        #     'INDSDL_RAW.DEV_LOAD_STAGE_ADLS',
        #     'dev/india_dna/master/route/in_route',
        #     'sdl_in_route'
        # ]
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        sch_name        = stage_name.split('.')[0]
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
    
        df = session.read\
            .schema(df_schema)\
            .option("skip_header",1)\
            .option("field_delimiter", "\u0001")\
            .option("skip_blank_lines", True) \
            .option("field_optionally_enclosed_by", None) \
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
    
        df = df.withColumn("FILE_NAME",lit(file_name).cast("string"))
        df = df.withColumn("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        df = df.withColumn("CRT_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))

        final_df = df.select(\
            "CmpCode", "ChannelCode", "ChannelName", "RetlrGroupCode", "RetlrGroupName", "ClassCode", "ClassName", \
            try_cast(col("Turnover"), DoubleType()).alias("RMPopulation"), \
            try_cast(col("createddt"), TimestampType()).alias("createddt"),\
            "RUN_ID", "CRT_DTTM", "FILE_NAME" \
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
$$;


CREATE OR REPLACE PROCEDURE INDSDL_RAW.IN_CSL_RETAILERMASTER_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS 
$$

import snowflake.snowpark as snowpark
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DateType, DoubleType,TimestampType
from snowflake.snowpark.functions import col,lit,date_format, try_cast, trim, to_date, left,  right, when, substring, length
import pandas as pd
from datetime import datetime
import pytz

def main(session: snowpark.Session, Param): 

    try:
    
        # Param = [
        #     'route_20240517062034.csv',
        #     'INDSDL_RAW.DEV_LOAD_STAGE_ADLS',
        #     'dev/india_dna/master/route/in_route',
        #     'sdl_in_route'
        # ]
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        sch_name        = stage_name.split('.')[0]
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
    
        df = session.read\
            .schema(df_schema)\
            .option("skip_header",1)\
            .option("field_delimiter", "\u0001")\
            .option("skip_blank_lines", True) \
            .option("field_optionally_enclosed_by", None) \
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
    
        df = df.withColumn("FILE_NAME",lit(file_name).cast("string"))
        df = df.withColumn("RUN_ID",lit(9999))
        df = df.withColumn("CRT_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))

        final_df = df.select(\
            "distcode", lit(None).as_("rtrid"), substring(col("rtrcode"),7, length("rtrcode")).alias("rtrcode"), "rtrname", \
            "csrtrcode", "retlrgroupcode", "ChannelCode", "classcode", "keyaccount", try_cast(col("regdate"), TimestampType()).alias("regdate"), \
            "relationstatus", "parentcode", lit(None).as_("geolevel"), lit(None).as_("geolevelvalue"), \
            when(col("status") == "Y", lit(1)).when(col("status") == "N", lit(0)).alias("status"), \
            lit(None).as_("createdid"), lit(None).as_("createddate"),  "rtraddress1", "rtraddress2", "rtraddress3", \
            lit(None).as_("rtrpincode"), lit(None).as_("villageid"), lit(None).as_("villagecode"), lit(None).as_("villagename"), \
            lit(None).as_("MODE"), lit(None).as_("uploadflag"), lit(None).as_("approvalremarks"), lit(None).as_("syncid"), "druglno", \
            try_cast(col("rtrcrbills"),IntegerType()).alias("rtrcrbills"), \
            try_cast(col("rtrcrlimit"), DoubleType()).alias("rtrcrlimit"), \
            try_cast(col("rtrcrdays"),IntegerType()).alias("rtrcrdays"), \
            lit(None).as_("rtrdayoff"), lit(None).as_("rtrtinno"), lit(None).as_("rtrcstno"), "rtrlicno", "rtrlicexpirydate", \
            "rtrdrugexpirydate", lit(None).as_("rtrpestlicno"), lit(None).as_("rtrpestexpirydate"), lit(None).as_("approved"), \
            "rtrphoneno", "rtrcontactperson", lit(None).as_("rtrtaxgroup"), "rtrtype", lit(None).as_("rtrtaxable"), \
            lit(None).as_("rtrshippadd1"), lit(None).as_("rtrshippadd2"), lit(None).as_("rtrshippadd3"), "rtrfoodlicno", \
            lit(None).as_("rtrfoodexpirydate"), lit(None).as_("rtrfoodgracedays"), lit(None).as_("rtrdruggracedays"), \
            lit(None).as_("rtrcosmeticlicno"), lit(None).as_("rtrcosmeticexpirydate"), lit(None).as_("rtrcosmeticgracedays"), "rtrlatitude", \
            "rtrlongitude", "rtruniquecode", \
            "RUN_ID", "CRT_DTTM", "FILE_NAME" \
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
$$;


CREATE OR REPLACE PROCEDURE INDSDL_RAW.IN_CSL_SALESMANMASTER_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS 
$$

import snowflake.snowpark as snowpark
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DateType, DoubleType,TimestampType
from snowflake.snowpark.functions import col,lit,date_format, try_cast, trim, to_date, left,  right
import pandas as pd
from datetime import datetime
import pytz

def main(session: snowpark.Session, Param): 

    try:
    
        # Param = [
        #     'route_20240517062034.csv',
        #     'INDSDL_RAW.DEV_LOAD_STAGE_ADLS',
        #     'dev/india_dna/master/route/in_route',
        #     'sdl_in_route'
        # ]
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        sch_name        = stage_name.split('.')[0]
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
    
        df = session.read\
            .schema(df_schema)\
            .option("skip_header",1)\
            .option("field_delimiter", "\u0001")\
            .option("skip_blank_lines", True) \
            .option("field_optionally_enclosed_by", None) \
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
    
        df = df.withColumn("FILE_NAME",lit(file_name).cast("string"))
        df = df.withColumn("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        df = df.withColumn("CRT_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))

        final_df = df.select(\
            "Distcode", lit(None).as_("smid"), "SMCode", "SMName", "SMPhoneNo", "SMEmail", lit(None).as_("smotherdetails"), \
            try_cast(col("SMDailyAllowance"), DoubleType()).alias("SMDailyAllowance"), \
            try_cast(col("SMMonthlySalary"), DoubleType()).alias("SMMonthlySalary"), \
            try_cast(col("SMMktCredit"), DoubleType()).alias("SMMktCredit"), \
            try_cast(col("smcreditdays"), IntegerType()).alias("smcreditdays"), \
            "Status", lit(None).as_("rmid"), "routecode", "rmname", lit(None).as_("uploadflag"), \
            try_cast(col("createddate"), TimestampType()).alias("createddate"), \
            lit(None).as_("syncid"), "RDSSMType", "UniqueSalesCode", \
            "RUN_ID", "CRT_DTTM", "FILE_NAME" \
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
$$;


CREATE OR REPLACE PROCEDURE INDSDL_RAW.IN_CSL_DISTRIBUTORACTIVATION_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS 
$$

import snowflake.snowpark as snowpark
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DateType, DoubleType,TimestampType
from snowflake.snowpark.functions import col,lit,date_format, try_cast, trim, to_date, left,  right, when
import pandas as pd
from datetime import datetime
import pytz

def main(session: snowpark.Session, Param): 

    try:
    
        # Param = [
        #     'route_20240517062034.csv',
        #     'INDSDL_RAW.DEV_LOAD_STAGE_ADLS',
        #     'dev/india_dna/master/route/in_route',
        #     'sdl_in_route'
        # ]
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        sch_name        = stage_name.split('.')[0]
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
    
        df = session.read\
            .schema(df_schema)\
            .option("skip_header",1)\
            .option("field_delimiter", "\u0001")\
            .option("skip_blank_lines", True) \
            .option("field_optionally_enclosed_by", None) \
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
    
        df = df.withColumn("FILE_NAME",lit(file_name).cast("string"))
        df = df.withColumn("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        df = df.withColumn("CRT_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        df = df.withColumn("createddate",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))

        final_df = df.select(\
            "UserCode", lit(None).as_("activefromdate"), lit(None).as_("activatedby"), lit(None).as_("activatedon"), \
            lit(None).as_("inactivefromdate"), lit(None).as_("inactivatedby"), lit(None).as_("inactivatedon"), \
            when(col("IsActive") == "Y", lit(1)).when(col("IsActive") == "N", lit(0)).alias("activestatus"), \
            "createddate", "RUN_ID", "CRT_DTTM", "FILE_NAME" \
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
$$;


CREATE OR REPLACE PROCEDURE INDSDL_RAW.IN_CSL_UDCMASTER_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS 
$$

import snowflake.snowpark as snowpark
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DateType, DoubleType,TimestampType
from snowflake.snowpark.functions import col,lit,date_format, try_cast, trim, to_date, left,  right, when
import pandas as pd
from datetime import datetime
import pytz

def main(session: snowpark.Session, Param): 

    try:
    
        # Param = [
        #     'route_20240517062034.csv',
        #     'INDSDL_RAW.DEV_LOAD_STAGE_ADLS',
        #     'dev/india_dna/master/route/in_route',
        #     'sdl_in_route'
        # ]
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        sch_name        = stage_name.split('.')[0]
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
    
        df = session.read\
            .schema(df_schema)\
            .option("skip_header",1)\
            .option("field_delimiter", "\u0001")\
            .option("skip_blank_lines", True) \
            .option("field_optionally_enclosed_by", None) \
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
    
        df = df.withColumn("FILE_NAME",lit(file_name).cast("string"))
        df = df.withColumn("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        df = df.withColumn("CRT_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))

        final_df = df.select(\
            lit(None).as_("udcmasterid"), lit(None).as_("masterid"), lit(None).as_("mastername"), col("MasterName").as_("columnname"), \
            "DataType", try_cast(col("Size"), IntegerType()).alias("Size"), \
            try_cast(col("AttrPrecision"), IntegerType()).alias("AttrPrecision"), \
            when(col("IsAllowEdit") == "Y", lit(1)).when(col("IsAllowEdit") == "N", lit(0)).alias("editable"), \
            when(col("IsMandatory") == "Y", lit(1)).when(col("IsMandatory") == "N", lit(0)).alias("columnmandatory"), \
            lit(None).as_("pickfromdefault"), lit(None).as_("downloadstatus"), lit(None).as_("createduserid"), \
            try_cast(col("CreatedDt"), TimestampType()).alias("CreatedDt"),  \
            "ModUserCode", \
            try_cast(col("ModDt"), TimestampType()).alias("ModDt"),  \
            when(col("UDCStatus") == "Y", lit(1)).when(col("UDCStatus") == "N", lit(0)).alias("udcstatus"), \
            "RUN_ID", "CRT_DTTM", "FILE_NAME" \
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
$$;



CREATE OR REPLACE PROCEDURE INDSDL_RAW.IN_RKEYACCCUSTOMER_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS 
$$

import snowflake.snowpark as snowpark
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DateType, DoubleType,TimestampType
from snowflake.snowpark.functions import col,lit,date_format, try_cast, trim, to_date, left,  right
import pandas as pd
from datetime import datetime
import pytz

def main(session: snowpark.Session, Param): 

    try:
    
        # Param = [
        #     'route_20240517062034.csv',
        #     'INDSDL_RAW.DEV_LOAD_STAGE_ADLS',
        #     'dev/india_dna/master/route/in_route',
        #     'sdl_in_route'
        # ]
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        sch_name        = stage_name.split('.')[0]
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
        
        df = session.read\
            .schema(df_schema)\
            .option("skip_header",1)\
            .option("field_delimiter", "\u0001")\
            .option("skip_blank_lines", True) \
            .option("truncatecolumns",True) \
            .option("field_optionally_enclosed_by", None) \
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
    
        df = df.withColumn("CRT_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))

        final_df = df.select(\
            "CustomerCode", "CustomerName", "CustomerAddress1", "CustomerAddress2", "CustomerAddress3", "SAPID", "RegionCode", \
            "ZoneCode", "TerritoryCode", "StateCode", "TownCode", "EmailID", "MobileLL", "IsActive", "WholesalerCode", "URC", \
            "NKACStores", "ParentCustomerCode", "IsDirectAcct", "IsParent", "ABICode", "DistributorSAPID", "IsConfirm", \
            try_cast(col("CreatedDate"), TimestampType()).alias("CreatedDate"),  \
            "createdUserCode", \
            try_cast(col("ModDt"), TimestampType()).alias("ModDt"),  \
            "ModUserCode", \
            try_cast(col("CreatedDt"), TimestampType()).alias("CreatedDt"),  \
            "CRT_DTTM"\
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
$$;



CREATE OR REPLACE PROCEDURE INDSDL_RAW.IN_CSL_SCHEMEUTILIZATION_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS 
$$

import snowflake.snowpark as snowpark
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DateType, DoubleType,TimestampType
from snowflake.snowpark.functions import col,lit,date_format, try_cast, trim, to_date, left,  right
import pandas as pd
from datetime import datetime
import pytz

def main(session: snowpark.Session, Param): 

    try:
    
        # Param = [
        #     'schemeutilization_20240517062034.csv',
        #     'INDSDL_RAW.DEV_LOAD_STAGE_ADLS',
        #     'dev/india_xdm/transaction/schemeutilization',
        #     'sdl_csl_schemeutilization'
        # ]
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        sch_name        = stage_name.split('.')[0]
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
        
        df = session.read\
            .schema(df_schema)\
            .option("skip_header",1)\
            .option("field_delimiter", "\u0001")\
            .option("skip_blank_lines", True) \
            .option("truncatecolumns",True) \
            .option("field_optionally_enclosed_by", None) \
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
    
        df = df.withColumn("FILE_NAME",lit(file_name).cast("string"))
        df = df.withColumn("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        df = df.withColumn("CRT_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))

        final_df = df.select(\
            "distcode", \
            "schemecode", \
            "schemedescription", \
            "invoiceno", \
            "rtrcode", \
            lit(None).as_("company"), \
            try_cast(col("schdate"), TimestampType()).alias("schdate"), \
            "schemetype", \
            try_cast(col("schemeutilizedamt"), DoubleType()).alias("schemeutilizedamt"), \
            "schemefreeproduct", \
            try_cast(col("schemeutilizedqty"), IntegerType()).alias("schemeutilizedqty"), \
            "companyschemecode", \
            try_cast(col("createddate"), TimestampType()).alias("createddate"), \
            lit(None).as_("migrationflag"), \
            lit(None).as_("schememode"), \
            try_cast(col("syncid"), IntegerType()).alias("syncid"), \
            try_cast(col("schlinecount"), IntegerType()).alias("schlinecount"), \
            "schvaluetype", \
            try_cast(col("slabid"), IntegerType()).alias("slabid"), \
            "billedprdccode", \
            "billedprdbatcode", \
            try_cast(col("billedqty"), IntegerType()).alias("billedqty"), \
            try_cast(col("schdiscperc"), DoubleType()).alias("schdiscperc"), \
            "freeprdbatcode", \
            try_cast(col("billedrate"), DoubleType()).alias("billedrate"), \
            try_cast(col("modifieddate"), TimestampType()).alias("modifieddate"), \
            "servicecrnrefno", \
            "rtrurccode", \
            "RUN_ID", \
            "CRT_DTTM", \
            "FILE_NAME" \
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
$$;


CREATE OR REPLACE PROCEDURE INDSDL_RAW.IN_RTLHEADER_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS 
$$

import snowflake.snowpark as snowpark
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DateType, DoubleType,TimestampType
from snowflake.snowpark.functions import col,lit,date_format, try_cast, trim, to_date, left,  right
import pandas as pd
from datetime import datetime
import pytz

def main(session: snowpark.Session, Param): 

    try:
    
        # Param = [
        #     'rtlheader_20240517062034.csv',
        #     'INDSDL_RAW.DEV_LOAD_STAGE_ADLS',
        #     'dev/india_dna/master/rtlheader/in_rtlheader',
        #     'SDL_IN_RTLHEADER'
        # ]
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        sch_name        = stage_name.split('.')[0]
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
        
        df = session.read\
            .schema(df_schema)\
            .option("skip_header",1)\
            .option("field_delimiter", "\u0001")\
            .option("skip_blank_lines", True) \
            .option("field_optionally_enclosed_by", None) \
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
    
        df = df.withColumn("FILE_NAME",lit(file_name).cast("string"))
        df = df.withColumn("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        df = df.withColumn("CRT_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))

        final_df = df.select( \
            "CmpCode", "TLCode", "TLName", "EmailId", "phoneno", \
            try_cast(col("DateOfBirth"), DateType()).alias("DateOfBirth"),  \
            try_cast(col("DateOfJoin"), DateType()).alias("DateOfJoin"),  \
            "IsActive", "ModUserCode", try_cast(col("ModDt"), TimestampType()).alias("ModDt"),  \
            "ApprovalStatus", \
            try_cast(col("dailyAllowance"), DoubleType()).alias("dailyAllowance"), \
            try_cast(col("monthlySalary"), DoubleType()).alias("monthlySalary"), \
            "aadharNo", "ImagePath", \
            try_cast(col("CreatedDt"), TimestampType()).alias("CreatedDt"), \
            "FILE_NAME","RUN_ID","CRT_DTTM" \
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
$$;


CREATE OR REPLACE PROCEDURE INDSDL_RAW.IN_RRSRHEADER_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS 
$$

import snowflake.snowpark as snowpark
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DateType, DoubleType,TimestampType
from snowflake.snowpark.functions import col,lit,date_format, try_cast, trim, to_date, left,  right
import pandas as pd
from datetime import datetime
import pytz

def main(session: snowpark.Session, Param): 

    try:
    
        # Param = [
        #     'rrsrheader_20240517062034.csv',
        #     'INDSDL_RAW.DEV_LOAD_STAGE_ADLS',
        #     'dev/india_dna/master/rrsrheader/in_rrsrheader',
        #     'SDL_IN_RRSRHEADER'
        # ]
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        sch_name        = stage_name.split('.')[0]
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
        
        df = session.read\
            .schema(df_schema)\
            .option("skip_header",1)\
            .option("field_delimiter", "\u0001")\
            .option("skip_blank_lines", True) \
            .option("field_optionally_enclosed_by", None) \
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
    
        df = df.withColumn("FILE_NAME",lit(file_name).cast("string"))
        df = df.withColumn("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        df = df.withColumn("CRT_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))

        final_df = df.select( \
            "CmpCode", "RSRCode", "RSRName", "EmailId", "phoneno", \
            try_cast(col("DateOfBirth"), DateType()).alias("DateOfBirth"),  \
            try_cast(col("DateOfJoin"), DateType()).alias("DateOfJoin"),  \
            "IsActive", "ModUserCode", try_cast(col("ModDt"), TimestampType()).alias("ModDt"),  \
            "ApprovalStatus", \
            try_cast(col("dailyAllowance"), DoubleType()).alias("dailyAllowance"), \
            try_cast(col("monthlySalary"), DoubleType()).alias("monthlySalary"), \
            "aadharNo", "ImagePath", \
            try_cast(col("CreatedDt"), TimestampType()).alias("CreatedDt"), \
            "FILE_NAME","RUN_ID","CRT_DTTM" \
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
$$;


CREATE OR REPLACE PROCEDURE INDSDL_RAW.IN_RTLDISTRIBUTOR_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS 
$$

import snowflake.snowpark as snowpark
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DateType, DoubleType,TimestampType
from snowflake.snowpark.functions import col,lit,date_format, try_cast, trim, to_date, left,  right
import pandas as pd
from datetime import datetime
import pytz

def main(session: snowpark.Session, Param): 

    try:
    
        # Param = [
        #     'rtldistributor_20240517062034.csv',
        #     'INDSDL_RAW.DEV_LOAD_STAGE_ADLS',
        #     'dev/india_dna/master/rtldistributor/in_rtldistributor',
        #     'SDL_IN_RTLDISTRIBUTOR'
        # ]
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        sch_name        = stage_name.split('.')[0]
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
        
        df = session.read\
            .schema(df_schema)\
            .option("skip_header",1)\
            .option("field_delimiter", "\u0001")\
            .option("skip_blank_lines", True) \
            .option("field_optionally_enclosed_by", None) \
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
    
        df = df.withColumn("FILE_NAME",lit(file_name).cast("string"))
        df = df.withColumn("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        df = df.withColumn("CRT_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))

        final_df = df.select( \
            "CmpCode", "TLCode", "DistrCode", \
            try_cast(col("DateOfJoin"), DateType()).alias("DateOfJoin"),  \
            "IsActive", "ModUserCode", try_cast(col("ModDt"), TimestampType()).alias("ModDt"),  \
            try_cast(col("CreatedDt"), TimestampType()).alias("CreatedDt"), \
            "FILE_NAME","RUN_ID","CRT_DTTM" \
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
$$;


CREATE OR REPLACE PROCEDURE INDSDL_RAW.IN_RRSRDISTRIBUTOR_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS 
$$

import snowflake.snowpark as snowpark
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DateType, DoubleType,TimestampType
from snowflake.snowpark.functions import col,lit,date_format, try_cast, trim, to_date, left,  right
import pandas as pd
from datetime import datetime
import pytz

def main(session: snowpark.Session, Param): 

    try:
    
        # Param = [
        #     'rrsrdistributor_20240517062034.csv',
        #     'INDSDL_RAW.DEV_LOAD_STAGE_ADLS',
        #     'dev/india_dna/master/rrsrdistributor/in_rrsrdistributor',
        #     'SDL_IN_RRSRDISTRIBUTOR'
        # ]
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        sch_name        = stage_name.split('.')[0]
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
        
        df = session.read\
            .schema(df_schema)\
            .option("skip_header",1)\
            .option("field_delimiter", "\u0001")\
            .option("skip_blank_lines", True) \
            .option("field_optionally_enclosed_by", None) \
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
    
        df = df.withColumn("FILE_NAME",lit(file_name).cast("string"))
        df = df.withColumn("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        df = df.withColumn("CRT_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))

        final_df = df.select( \
            "CmpCode", "RSRCode", "DistrCode", \
            try_cast(col("DateOfJoin"), DateType()).alias("DateOfJoin"),  \
            "IsActive", "ModUserCode", try_cast(col("ModDt"), TimestampType()).alias("ModDt"),  \
            try_cast(col("CreatedDt"), TimestampType()).alias("CreatedDt"), \
            "FILE_NAME","RUN_ID","CRT_DTTM" \
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
$$;


CREATE OR REPLACE PROCEDURE INDSDL_RAW.IN_RTLSALESMAN_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS 
$$

import snowflake.snowpark as snowpark
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DateType, DoubleType,TimestampType
from snowflake.snowpark.functions import col,lit,date_format, try_cast, trim, to_date, left,  right
import pandas as pd
from datetime import datetime
import pytz

def main(session: snowpark.Session, Param): 

    try:
    
        # Param = [
        #     'rtlsalesman_20240517062034.csv',
        #     'INDSDL_RAW.DEV_LOAD_STAGE_ADLS',
        #     'dev/india_dna/master/rtlsalesman/in_rtlsalesman',
        #     'SDL_IN_RTLSALESMAN'
        # ]
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        sch_name        = stage_name.split('.')[0]
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
        
        df = session.read\
            .schema(df_schema)\
            .option("skip_header",1)\
            .option("field_delimiter", "\u0001")\
            .option("skip_blank_lines", True) \
            .option("field_optionally_enclosed_by", None) \
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
    
        df = df.withColumn("FILE_NAME",lit(file_name).cast("string"))
        df = df.withColumn("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        df = df.withColumn("CRT_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))

        final_df = df.select( \
            "CmpCode", "TLCode", "DistrCode", "DistrBrCode", "SalesmanCode", \
            try_cast(col("DateOfJoin"), DateType()).alias("DateOfJoin"),  \
            "IsActive", "ModUserCode", try_cast(col("ModDt"), TimestampType()).alias("ModDt"),  \
            try_cast(col("CreatedDt"), TimestampType()).alias("CreatedDt"), \
            "FILE_NAME","RUN_ID","CRT_DTTM" \
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
$$;


CREATE OR REPLACE PROCEDURE INDSDL_RAW.IN_RRETAILERGEOEXTENSION_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS 
$$

import snowflake.snowpark as snowpark
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DateType, DoubleType,TimestampType
from snowflake.snowpark.functions import col,lit,date_format, try_cast, trim, to_date, left,  right
import pandas as pd
from datetime import datetime
import pytz

def main(session: snowpark.Session, Param): 

    try:
    
        # Param = [
        #     'rretailergeoextension_20240517062034.csv',
        #     'INDSDL_RAW.DEV_LOAD_STAGE_ADLS',
        #     'dev/india_dna/master/rretailergeoextension/in_rretailergeoextension',
        #     'SDL_IN_RRETAILERGEOEXTENSION'
        # ]
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        sch_name        = stage_name.split('.')[0]
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
        
        df = session.read\
            .schema(df_schema)\
            .option("skip_header",1)\
            .option("field_delimiter", "\u0001")\
            .option("skip_blank_lines", True) \
            .option("field_optionally_enclosed_by", None) \
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
    
        df = df.withColumn("FILE_NAME",lit(file_name).cast("string"))
        df = df.withColumn("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        df = df.withColumn("CRT_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))

        final_df = df.select( \
            "Cmpcode", "Distrcode", "Customercode", "Cmpcutomercode", "Distributorcustomercode", "Latitude", "Longitude", \
            "TownName", "StateName", "DistrictName", "SubDistrictName", "Type", "VillageName", \
            try_cast(col("Pincode"), IntegerType()).alias("Pincode"), \
            "UAcheck", "UAName", try_cast(col("Population"), IntegerType()).alias("Population"), \
            "PopStrata", try_cast(col("FinalPopulationwithUA"), IntegerType()).alias("FinalPopulationwithUA"), \
            try_cast(col("Modifydate"), TimestampType()).alias("Modifydate"), \
            try_cast(col("Createddate"), TimestampType()).alias("Createddate"), \
            "isDeleted", "ExtraField1", "ExtraField2", "ExtraField3", \
            try_cast(col("createddt"), TimestampType()).alias("createddt"), \
            "FILE_NAME","RUN_ID","CRT_DTTM" \
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
$$;


CREATE OR REPLACE PROCEDURE INDSDL_RAW.IN_PERFECTSTORE_MSL_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS
$$
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
        sch_name        = stage_name.split('.')[0]
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
            files_to_read = [sh_name.strip("\"").replace("(", "").replace(")", "").replace(" ","_") + ".csv" for sh_name in files_to_read]
        
        df = None

        for src_file in files_to_read:
            
            file_df = session.read\
                .schema(df_schema)\
                .option("skip_header",1)\
                .option("field_delimiter", "\u0001")\
                .option("field_optionally_enclosed_by", "\"") \
                .csv("@" + stage_name+"/"+temp_stage_path+"/"+src_file)
            
            if not df:
                df = file_df
            else:
                df = df.unionByName(file_df)

        yearmo = file_name.split(".")[0].split("_")[-1]

        df = df.na.drop(how='all')

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
    
        file_name=file_name.split(".")[0] + '_' + datetime.now().strftime("%Y%m%d%H%M%S")
        final_df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,file_format_type="csv",OVERWRITE=True,header=True)
   
        return "Success"
        
    except KeyError as key_error:
        
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message
        
    except Exception as e:
        
        error_message = f"Error: {str(e)}" +str(df.columns)
        return error_message
$$;


CREATE OR REPLACE PROCEDURE INDSDL_RAW.IN_PERFECTSTORE_SOS_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS
$$
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
        sch_name        = stage_name.split('.')[0]
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
            files_to_read = [sh_name.strip("\"").replace("(", "").replace(")", "").replace(" ","_") + ".csv" for sh_name in files_to_read]
        
        df = None

        for src_file in files_to_read:
            
            file_df = session.read\
                .schema(df_schema)\
                .option("skip_header",1)\
                .option("field_delimiter", "\u0001")\
                .option("field_optionally_enclosed_by", "\"") \
                .csv("@" + stage_name+"/"+temp_stage_path+"/"+src_file)
            
            if not df:
                df = file_df
            else:
                df = df.unionByName(file_df)

        yearmo = file_name.split(".")[0].split("_")[-1]
        
        df = df.na.drop(how='all')
        
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
    
        file_name=file_name.split(".")[0] + '_' + datetime.now().strftime("%Y%m%d%H%M%S")
        final_df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,file_format_type="csv",OVERWRITE=True,header=True)
   
        return "Success"
        
    except KeyError as key_error:
        
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message
        
    except Exception as e:
        
        error_message = f"Error: {str(e)}" +str(df.columns)
        return error_message
$$;


CREATE OR REPLACE PROCEDURE INDSDL_RAW.IN_PERFECTSTORE_PROMO_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS
$$
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
        sch_name        = stage_name.split('.')[0]
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
            files_to_read = [sh_name.strip("\"").replace("(", "").replace(")", "").replace(" ","_") + ".csv" for sh_name in files_to_read]
        
        df = None

        for src_file in files_to_read:
            
            file_df = session.read\
                .schema(df_schema)\
                .option("skip_header",1)\
                .option("field_delimiter", "\u0001")\
                .option("field_optionally_enclosed_by", "\"") \
                .csv("@" + stage_name+"/"+temp_stage_path+"/"+src_file)
            
            if not df:
                df = file_df
            else:
                df = df.unionByName(file_df)

        yearmo = file_name.split(".")[0].split("_")[-1]

        df = df.na.drop(how='all')

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
    
        file_name=file_name.split(".")[0] + '_' + datetime.now().strftime("%Y%m%d%H%M%S")
        final_df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,file_format_type="csv",OVERWRITE=True,header=True)
  
        return "Success"
        
    except KeyError as key_error:
        
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message
        
    except Exception as e:
        
        error_message = f"Error: {str(e)}" +str(df.columns)
        return error_message
$$;


CREATE OR REPLACE PROCEDURE INDSDL_RAW.IN_PERFECTSTORE_PAID_DISPLAY_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS
$$
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
        sch_name        = stage_name.split('.')[0]
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
            files_to_read = [sh_name.strip("\"").replace("(", "").replace(")", "").replace(" ","_") + ".csv" for sh_name in files_to_read]
        
        df = None

        for src_file in files_to_read:
            
            file_df = session.read\
                .schema(df_schema)\
                .option("skip_header",1)\
                .option("field_delimiter", "\u0001")\
                .option("field_optionally_enclosed_by", "\"") \
                .csv("@" + stage_name+"/"+temp_stage_path+"/"+src_file)
            
            if not df:
                df = file_df
            else:
                df = df.unionByName(file_df)


        yearmo = file_name.split(".")[0].split("_")[-1]

        df = df.na.drop(how='all')

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
    
        file_name=file_name.split(".")[0] + '_' + datetime.now().strftime("%Y%m%d%H%M%S")
        final_df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,file_format_type="csv",OVERWRITE=True,header=True)
   
        return "Success"
        
    except KeyError as key_error:
        
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message
        
    except Exception as e:
        
        error_message = f"Error: {str(e)}" +str(df.columns)
        return error_message
$$;


CREATE OR REPLACE PROCEDURE INDSDL_RAW.IN_SKU_RECOM_FLAG_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS
$$
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
        sch_name        = stage_name.split('.')[0]
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
        
        df = session.read\
            .schema(df_schema)\
            .option("skip_header",1)\
            .option("field_delimiter", "|")\
            .option("field_optionally_enclosed_by", "\"") \
            .csv("@" + stage_name + "/" + temp_stage_path + "/" + file_name)

        yearmo = file_name.split("_")[1]
        yearmo = yearmo[2:]+yearmo[:2]

        df = df.withColumn("YEARMO",lit(yearmo).cast("string"))

        final_df = df.select( \
            "YEARMO", "PRODUCT", "OUTLET", "DISTRIBUTOR", "OOS_FLAG", "MS_FLAG", \
            "CS_FLAG", "SOQ", "URCCode", "CsrtrCode", "RT_Code", "SM_Code" \
        ).alias("final_df")
                        
        if final_df.count()==0:
            return "No Data in file"
        
        # Load Data to the target table
        final_df.write.mode("append").saveAsTable(target_table)

        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")

        # write to success folder
    
        file_name=file_name.split(".")[0] + '_' + datetime.now().strftime("%Y%m%d%H%M%S")
        final_df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,file_format_type="csv",OVERWRITE=True,header=True)
   
        return "Success"
        
    except KeyError as key_error:
        
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message
        
    except Exception as e:
        
        error_message = f"Error: {str(e)}" +str(df.columns)
        return error_message
$$;


CREATE OR REPLACE PROCEDURE INDSDL_RAW.IN_CSL_CLASSMASTER_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS
$$
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
        sch_name        = stage_name.split('.')[0]
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
        
        df = session.read\
            .schema(df_schema)\
            .option("skip_header",1)\
            .option("field_delimiter", "\u0001")\
            .option("field_optionally_enclosed_by", None) \
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
$$;


CREATE OR REPLACE PROCEDURE INDSDL_RAW.IN_SALESMAN_TARGET_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS
$$
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
        sch_name        = stage_name.split('.')[0]
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
        
        df = session.read\
            .schema(df_schema)\
            .option("skip_header",1)\
            .option("field_delimiter", "\u0001")\
            .option("field_optionally_enclosed_by", "\"") \
            .csv("@" + stage_name + "/" + temp_stage_path + "/" + file_name)
        
        final_df = df

        if final_df.count()==0:
            return "No Data in file"

        final_df.write.mode("append").saveAsTable(target_table)
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")

        # write to success folder
    
        file_name=file_name.split(".")[0] + '_' + datetime.now().strftime("%Y%m%d%H%M%S")
        final_df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,file_format_type="csv",OVERWRITE=True,header=True)
   
        return "Success"
        
    except KeyError as key_error:
        
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message
        
    except Exception as e:
        
        error_message = f"Error: {str(e)}" +str(df.columns)
        return error_message
$$;

CREATE OR REPLACE PROCEDURE INDSDL_RAW.CSL_DAILYSALES_PREPROCESSING("PARAM" ARRAY)
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

CREATE OR REPLACE PROCEDURE INDSDL_RAW.CSL_DAILYSALES_UNDELIVERED_PREPROCESSING("PARAM" ARRAY)
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

CREATE OR REPLACE PROCEDURE INDSDL_RAW.CSL_ORDERBOOKING_PREPROCESSING("PARAM" ARRAY)
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

CREATE OR REPLACE PROCEDURE INDSDL_RAW.CSL_SALESINVOICEORDERS_PREPROCESSING("PARAM" ARRAY)
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

CREATE OR REPLACE PROCEDURE INDSDL_RAW.CSL_SALESRETURN_PREPROCESSING("PARAM" ARRAY)
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


CREATE OR REPLACE PROCEDURE INDSDL_RAW.CSL_UDCDETAILS_PREPROCESSING("PARAM" ARRAY)
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

CREATE OR REPLACE PROCEDURE INDSDL_RAW.IN_DAILYSALES_DEL_PREPROCESSING("PARAM" ARRAY)
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
            .option("skip_header", 1) \\
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

CREATE OR REPLACE PROCEDURE INDSDL_RAW.IN_GEO_HEIRARCHY_PREPROCESSING("PARAM" ARRAY)
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
            .option("field_optionally_enclosed_by", None) \\
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


CREATE OR REPLACE PROCEDURE INDSDL_RAW.IN_SALESMANSKUMAPPING_PREPROCESSING("PARAM" ARRAY)
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
            .option("field_optionally_enclosed_by", None) \\
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
    
CREATE OR REPLACE PROCEDURE INDSDL_RAW.IN_SALES_HEIRARCHY_PREPROCESSING("PARAM" ARRAY)
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
            .option("field_optionally_enclosed_by", None) \\
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
    
CREATE OR REPLACE PROCEDURE INDSDL_RAW.IN_SUPPLIER_PREPROCESSING("PARAM" ARRAY)
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
            .option("field_optionally_enclosed_by", None) \\
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

CREATE OR REPLACE PROCEDURE INDSDL_RAW.SALESRETURN_DEL_PREPROCESSING("PARAM" ARRAY)
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

CREATE OR REPLACE PROCEDURE INDSDL_RAW.XDM_DISTRIBUTOR_SUPPLIER_PREPROCESSING("PARAM" ARRAY)
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

CREATE OR REPLACE PROCEDURE INDSDL_RAW.XDM_PRODUCTUOM_PREPROCESSING("PARAM" ARRAY)
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

CREATE OR REPLACE PROCEDURE INDSDL_RAW.XDM_PRODUCT_PREPROCESSING("PARAM" ARRAY)
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
