SET ENV ='DEV';

SET DB = $ENV||'_DNA_LOAD';

USE DATABASE identifier($DB);

USE SCHEMA NTASDL_RAW;

CREATE OR REPLACE PROCEDURE NTASDL_RAW.NA_KR_DA_IN_SANG_SA_GT_SELLOUT_PREPROCESSING("PARAM" ARRAY)
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

        df = session.read\
            .schema(df_schema)\
            .option("skip_header",1)\
            .option("field_delimiter", "\u0001")\
            .option("field_optionally_enclosed_by", "\"") \
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        dstr_nm = (' '.join(file_name.split('_')[:-1]))
        cust_cd = file_name.split('_')[-1]
        cust_cd = cust_cd.split('.')[0]

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

CREATE OR REPLACE PROCEDURE NTASDL_RAW.NA_KR_GT_DPT_DAISO_PREPROCESSING("PARAM" ARRAY)
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
                    StructField("Sub_Customer_Name", StringType()),
                    StructField("Sub_Customer_Code", StringType()),
                    StructField("Customer_Code", StringType())
                ])

        df = session.read\
            .schema(df_schema)\
            .option("skip_header",1)\
            .option("field_delimiter", "\u0001")\
            .option("field_optionally_enclosed_by", "\"") \
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


CREATE OR REPLACE PROCEDURE NTASDL_RAW.NA_KR_GT_DAISO_PRICE_PREPROCESSING("PARAM" ARRAY)
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
                    StructField("EAN", StringType()),
                    StructField("Unit_Price", StringType())
                ])

        df = session.read\
            .schema(df_schema)\
            .option("skip_header",1)\
            .option("field_delimiter", "\u0001")\
            .option("field_optionally_enclosed_by", "\"") \
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        dstr_nm = file_name.split('_')[0]
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

CREATE OR REPLACE PROCEDURE NTASDL_RAW.NA_KR_DAISO_GT_SELLOUT_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python', 'pandas')
HANDLER = 'main'
EXECUTE AS OWNER
AS
$$
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
        sch_name        = stage_name.split('.')[0]
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

        df = session.read\
            .schema(df_schema)\
            .option("skip_header",0)\
            .option("field_delimiter", "\u0001")\
            .option("field_optionally_enclosed_by", "\"") \
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        pd_df = df.to_pandas()
        
        name = pd_df.iloc[0]["COL02"]
        df = df.withColumn("CUSTOMER_NAME", lit(name).cast("string"))

        filenamepart, extension = file_name.split('.')
        dstr_nm , cust_cd, year = filenamepart.split('_')

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
    
        file_name=file_name.split(".")[0] + '_' + datetime.now().strftime("%Y%m%d%H%M%S")
        file_df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,file_format_type="csv",OVERWRITE=True,header=True)   
        
        return "Success"
        
    except KeyError as key_error:
        
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message
        
    except Exception as e:
        
        error_message = f"Error: {str(e)}" +str(df.columns)
        return error_message
$$;

CREATE OR REPLACE PROCEDURE NTASDL_RAW.NA_KR_BO_YOUNG_JONG_HAP_LOGISTICS_GT_SELLOUT_PREPROCESSING("PARAM" ARRAY)
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

        df = session.read\
            .schema(df_schema)\
            .option("skip_header",1)\
            .option("field_delimiter", "\u0001")\
            .option("field_optionally_enclosed_by", "\"") \
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        dstr_nm = (' '.join(file_name.split('_')[:-1]))
        cust_cd = file_name.split('_')[-1]
        cust_cd = cust_cd.split('.')[0]

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

CREATE OR REPLACE PROCEDURE NTASDL_RAW.NA_KR_DONGBU_LSD_GT_SELLOUT_PREPROCESSING("PARAM" ARRAY)
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

        df = session.read\
            .schema(df_schema)\
            .option("skip_header",1)\
            .option("field_delimiter", "\u0001")\
            .option("field_optionally_enclosed_by", "\"") \
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
        
        dstr_nm = (' '.join(file_name.split('_')[:-1]))
        cust_cd = file_name.split('_')[-1]
        cust_cd = cust_cd.split('.')[0]

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

CREATE OR REPLACE PROCEDURE NTASDL_RAW.NA_KR_DU_BAE_RO_YU_TONG_GT_SELLOUT_PREPROCESSING("PARAM" ARRAY)
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

        df = session.read\
            .schema(df_schema)\
            .option("skip_header",3)\
            .option("field_delimiter", "\u0001")\
            .option("field_optionally_enclosed_by", "\"") \
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
 
        valid_df=df.filter(col("Date").is_not_null()) and df.filter(col("EAN").is_not_null())

        dstr_nm = (' '.join(file_name.split('_')[:-1]))
        cust_cd = file_name.split('_')[-1:][0]
        cust_cd = cust_cd.split('.')[0]

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

