SET ENV ='DEV';

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
            .option("field_delimiter", "|")\\
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
            .option("field_optionally_enclosed_by", "\"") \
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
            .option("field_optionally_enclosed_by", "\"") \
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
            .option("field_optionally_enclosed_by", "\"") \
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
            .option("field_optionally_enclosed_by", "\"") \
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
            .option("field_optionally_enclosed_by", "\"") \
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