CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.SGPSDL_RAW.SG_ZUELLIG_SELLOUT_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS 'from snowflake.snowpark.functions import col, lit, date_format, current_timestamp, to_date, year, month, concat, format_number, regexp_replace,to_timestamp,when,trim,upper,date_format,as_date
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DecimalType,DateType
import pandas as pd
from datetime import datetime 
import snowflake.snowpark as snowpark
import pytz

def main(session: snowpark.Session,Param):
    #Param=["SG_Zuellig_Sell_Out_Sep21.csv","SGPSDL_RAW.DEV_LOAD_STAGE_ADLS","dev/transaction_sellout","SDL_SG_ZUELLIG_SELLOUT"]
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        
        
       
        df_schema = StructType([
            StructField("sales_date",StringType()),
            StructField("Warehouse_Code", StringType()),
            StructField("Customer_Code", StringType()),
            StructField("Customer_Name", StringType()),
            StructField("Invoice", StringType()),
            StructField("Item_Name", StringType()),
            StructField("Item_Code", StringType()),
            StructField("Type", StringType(), True),
            StructField("sales_value", DecimalType(17, 3)),
            StructField("sales_units", DecimalType(17, 3)),
            StructField("bonus_units", DecimalType(17, 3)),
            StructField("returns_units", DecimalType(17, 3)),
            StructField("returns_value", DecimalType(17, 3)),
            StructField("returns_bonus_units", DecimalType(17, 3)),
        ])

        df = session.read\\
        .schema(df_schema)\\
        .option("skip_header",1)\\
        .option("field_delimiter", "\\u0001")\\
        .option("field_optionally_enclosed_by", "\\"") \\
        .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        df=df.na.drop("all")
        if df.count()==0 :
            return "No Data in table"

        file_name_list=file_name.split("_")
        
        file_name="_".join(file_name_list[0:4])+"_"+file_name_list[4][:3]+"-"+file_name_list[4][3:5]+".csv"


        df = df.withColumn("month_no",lit(file_name_list[4][:3]+"-"+file_name_list[4][3:5]))
        df=df.with_column("file_name",lit(file_name))
        df=df.with_column("crtd_dttm",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        df = df.with_column("cdl_dttm", lit(None))
        df = df.with_column("run_id", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        
        pdf=df.toPandas()

        pdf=pd.DataFrame(pdf)

        pdf["SALES_DATE"] = pd.to_datetime(pdf["SALES_DATE"]).dt.strftime(''%m/%d/%Y'')
        df = session.createDataFrame(pdf)

       
        snowdf=df.select("Month_No", "sales_date", "Warehouse_Code", "Customer_Code", "Customer_Name","Invoice", "Item_Name", "Item_Code", "Type", "Sales_Value","Sales_Units", "Bonus_Units", "Returns_Units", "Returns_Value", "Returns_Bonus_Units", "cdl_dttm", "crtd_dttm", "file_name", "run_id")
    



        

        

        #deleteing the data from table 

        #del_sql = "DELETE FROM " + stage_name.split(".")[0]+"."+target_table
        #session.sql(del_sql).collect()
        
       # move file into success folder
        file_name=file_name.split(".")[0]+"_"+datetime.now().strftime("%Y%m%d%H%M%S")
        
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")        
        
        snowdf.write.copy_into_location("@"+Param[1]+"/"+Param[2]+"/processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)

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
        return error_message';

CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.SGPSDL_RAW.SDL_SG_ZUELLIG_CUSTOMER_MAPPING_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS 'from snowflake.snowpark.functions import col, lit, date_format, current_timestamp, to_date, year, month, concat, format_number, regexp_replace,to_timestamp,when,trim,upper
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DecimalType,DateType, TimestampType
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz

def main(session: snowpark.Session,Param):
    #Param=["SG_Zuellig_Customer_Mapping.csv","SGPSDL_RAW.DEV_LOAD_STAGE_ADLS","dev/master_sellout/SG_Zuellig_Customer_Mapping","SDL_SG_ZUELLIG_CUSTOMER_MAPPING"]
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        
        df_schema = StructType([
            StructField("zuellig_customer", StringType()),
            StructField("regional_banner", StringType()),
            StructField("merchandizing", StringType())
            ]
            )
        df = session.read\\
        .schema(df_schema)\\
        .option("skip_header",1)\\
        .option("field_delimiter", "\\u0001")\\
        .option("field_optionally_enclosed_by", "\\"") \\
        .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        df=df.na.drop("all")
        if df.count()==0 :
            return "No Data in table"

        df=df.with_column("file_name",lit(file_name))
        df = df.with_column("run_id", lit(None).cast(DecimalType(14,0)))
        df = df.with_column("cdl_dttm", lit(None))
        df=df.with_column("curr_date",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
            


        snowdf=df.select("zuellig_customer","regional_banner","merchandizing","cdl_dttm","curr_date","file_name","run_id")
        

        #deleteing the data from table 

        #del_sql = "DELETE FROM " + stage_name.split(".")[0]+"."+target_table
        #session.sql(del_sql).collect()

        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        
        #move file into success folder
        file_name=file_name.split(".")[0]+"_"+datetime.now().strftime("%Y%m%d%H%M%S")
        snowdf.write.copy_into_location("@"+Param[1]+"/"+Param[2]+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)

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
        return error_message';

CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.SGPSDL_RAW.SG_ZUELLIG_PRODUCT_MAPPIN_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS 'from snowflake.snowpark.functions import col, lit, date_format, current_timestamp, to_date, year, month, concat, format_number, regexp_replace,to_timestamp,when,trim,upper
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DecimalType,DateType, TimestampType
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz
def main(session: snowpark.Session,Param):
    #Param=["SG_Zuellig_Product_Mapping.csv","SGPSDL_RAW.DEV_LOAD_STAGE_ADLS","dev/master_sellout/SG_Zuellig_Product_Mapping","SDL_SG_ZUELLIG_Product_MAPPING"]
    
    try:

        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        
        df_schema = StructType([
            StructField("zp_material", StringType(), True),
            StructField("zp_item_code", StringType(), True),
            StructField("jj_code", StringType(), True),
            StructField("item_name", StringType(), True),
            StructField("brand", StringType(), True)
            ])
        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "\\u0001")\\
            .option("field_optionally_enclosed_by", "\\"") \\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        df=df.na.drop("all")
        if df.count()==0 :
            return "No Data in table"
            
        
        df = df\\
            .withColumn("cdl_dttm", lit(None))\\
            .with_column("curr_date",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))\\
            .with_column("file_name",lit(file_name))\\
            .with_column("run_id", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        
        snowdf=df.select("zp_material","zp_item_code","jj_code","item_name","brand","cdl_dttm","curr_date","file_name","run_id")
       


        
        

        #deleteing the data from table 

        #del_sql = "DELETE FROM " + stage_name.split(".")[0]+"."+target_table
        #session.sql(del_sql).collect()
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        
        #move file into success folder
        file_name=file_name.split(".")[0]+"_"+datetime.now().strftime("%Y%m%d%H%M%S")
        snowdf.write.copy_into_location("@"+Param[1]+"/"+Param[2]+"/processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)

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
CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.SGPSDL_RAW.SG_CIW_MAP_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS 'from snowflake.snowpark.functions import col, lit, date_format, current_timestamp, to_date, year, month, concat, format_number, regexp_replace,to_timestamp,when,trim,upper
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DecimalType,DateType, TimestampType
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz

def main(session: snowpark.Session,Param):
    #Param=["ciw_map.csv","SGPSDL_RAW.DEV_LOAD_STAGE_ADLS","dev/master_sellout/CIW_MAP","SDL_SG_CIW_MAPPING"]

    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]

        df_schema = StructType([
            StructField("condition_type", StringType()),
            StructField("gl", StringType()),
            StructField("gl_description", StringType()),
            StructField("posted_where", StringType()),
            StructField("purpose", StringType()),
            StructField("ciw_bucket", StringType()),
            StructField("cdl_dttm", StringType()),
            StructField("curr_date", TimestampType())
        ])
        
        df = session.read\\
        .schema(df_schema)\\
        .option("skip_header",1)\\
        .option("field_delimiter", "\\u0001")\\
        .option("field_optionally_enclosed_by", "\\"") \\
        .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
        df=df.na.drop("all")
        if df.count()==0 :
            return "No Data in table"

        df = df\\
            .withColumn("cdl_dttm", lit(None))\\
            .with_column("curr_date",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))\\
            .with_column("file_name",lit(file_name))\\
            .with_column("run_id", lit(None).cast(DecimalType(14,0)))
        

        #deleteing the data from table 
        
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")

        #del_sql = "DELETE FROM " + stage_name.split(".")[0]+"."+target_table
        #session.sql(del_sql).collect()
    

        snowdf=df.select(''condition_type'', ''gl'', ''gl_description'', ''posted_where'', ''purpose'', ''ciw_bucket'', ''cdl_dttm'', ''curr_date'', ''file_name'', ''run_id'')

        
        #move file into success folder
        file_name=file_name.split(".")[0]+"_"+datetime.now().strftime("%Y%m%d%H%M%S")
        snowdf.write.copy_into_location("@"+Param[1]+"/"+Param[2]+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)

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
            StructField("Dist_Code", StringType()),
            StructField("SM_Code", StringType()),
            StructField("SM_Target", StringType()),
            StructField("Brand_Focus", StringType()),
            StructField("Measure_Type", StringType()),
            StructField("Channel", StringType()),
            StructField("YY", StringType()),
            StructField("MM", StringType()),
        ])
        
        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "\\u0001")\\
            .option("field_optionally_enclosed_by", "\\"") \\
            .csv("@" + stage_name + "/" + temp_stage_path + "/" + file_name)
        
        df = df.with_column("FILE_NAME", lit(file_name.split(".")[0]+".xlsx"))
        #print(df)
        
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
            .option("field_delimiter", "")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
        
        df = df.na.drop("all")
        
        if df.count() == 0:
            return "No Data in file"

        # Extract the relevant part of the file name
        extracted_file_name = "_".join((file_name.split(".")[0]).split("_")[:4])

        df = df.withColumn("load_date", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d")))
        df = df.withColumn("SOURCE_FILE_NAME", lit(Param[0].split(".")[0]+".xlsx"))
        # df = df.withColumn("run_id", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))


        snowdf = df.select(
            "load_date",
            "SOURCE_FILE_NAME",
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
        df = df.with_column("FILE_NAME", lit(file_name))

        final_df = df.select(\\
            "CustomerCode", "CustomerName", "CustomerAddress1", "CustomerAddress2", "CustomerAddress3", "SAPID", "RegionCode", \\
            "ZoneCode", "TerritoryCode", "StateCode", "TownCode", "EmailID", "MobileLL", "IsActive", "WholesalerCode", "URC", \\
            "NKACStores", "ParentCustomerCode", "IsDirectAcct", "IsParent", "ABICode", "DistributorSAPID", "IsConfirm", \\
            try_cast(col("CreatedDate"), TimestampType()).alias("CreatedDate"),  \\
            "createdUserCode", \\
            try_cast(col("ModDt"), TimestampType()).alias("ModDt"),  \\
            "ModUserCode", \\
            try_cast(col("CreatedDt"), TimestampType()).alias("CreatedDt"),  \\
            "CRT_DTTM","FILE_NAME"\\
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
            .option("field_delimiter", "/u0001")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
        
        df = df.na.drop("all")
        
        if df.count() == 0:
            return "No Data in file"

        # Extract the relevant part of the file name
        extracted_file_name = "_".join((file_name.split(".")[0]).split("_")[:4])

        df = df.withColumn("load_date", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d")))
        df = df.withColumn("filename", lit(extracted_file_name))
        # df = df.withColumn("run_id", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        df = df.with_column("SOURCE_FILE_NAME", lit(Param[0].split(".")[0]+".xlsx"))

        snowdf = df.select(
            "load_date",
            "SOURCE_FILE_NAME",
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
        df = df.with_column("FILE_NAME", lit(file_name.split(".")[0]+".xlsx"))
                                    
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
            try_cast(col("CRT_DTTM"), TimestampType()).alias("CRT_DTTM"),\\
            "FILE_NAME"
            
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
            .option("field_delimiter", "")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
        
        df = df.na.drop("all")
        
        if df.count() == 0:
            return "No Data in file"

        # Extract the relevant part of the file name
        extracted_file_name = "_".join((file_name.split(".")[0]).split("_")[:4])

        df = df.withColumn("load_date", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d")))
        df = df.withColumn("filename", lit(extracted_file_name))
        # df = df.withColumn("run_id", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        df = df.with_column("SOURCE_FILE_NAME", lit(Param[0].split(".")[0]+".xlsx"))



        snowdf = df.select(
            "load_date",
            "SOURCE_FILE_NAME",
            "sku_code", 
            "qty",      
            "mrp", 
            "product_name"
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
        df = df.with_column("FILE_NAME", lit(file_name))


        final_df = df.select( \\
            "YEARMO", "PRODUCT", "OUTLET", "DISTRIBUTOR", "OOS_FLAG", "MS_FLAG", \\
            "CS_FLAG", "SOQ", "URCCode", "CsrtrCode", "RT_Code", "SM_Code","FILE_NAME" \\
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
            StructField("TABLEID", StringType()),
            StructField("PKEY", StringType()),
            StructField("CLASSID", StringType()),
            StructField("CLASSCODE", StringType()),
            StructField("CLASSDESC", StringType()),
            StructField("TURNOVER", StringType()),
            StructField("AVAILABILTY", StringType()),
            StructField("CREATEDUSERID", StringType()),
            StructField("CREATEDDATE", StringType()),
            StructField("DISTHIERARCHYID", StringType())
        ])
        
        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "\\u0001")\\
            .option("field_optionally_enclosed_by", None) \\
            .csv("@" + stage_name + "/" + temp_stage_path + "/" + file_name)
        
        df = df.withColumn("FILE_NAME", lit(file_name))
        df = df.withColumn("CRT_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))


        final_df = df.select("TABLEID", "PKEY", "CLASSID", "CLASSCODE", "CLASSDESC", "TURNOVER", "AVAILABILTY", "CREATEDUSERID", "CREATEDDATE", "DISTHIERARCHYID",  "CRT_DTTM","FILE_NAME")

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
            .option("field_delimiter", "\\u0001")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
        
        df = df.na.drop("all")
        
        if df.count() == 0:
            return "No Data in file"

        # Extract the relevant part of the file name
        extracted_file_name = "_".join((file_name.split(".")[0]).split("_")[:4])

        df = df.withColumn("load_date", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d")))
        df = df.withColumn("filename", lit(extracted_file_name))
        # df = df.withColumn("run_id", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        df = df.with_column("SOURCE_FILE_NAME", lit(Param[0]))


        snowdf = df.select(
            "load_date",
            "SOURCE_FILE_NAME"
            "l_cat", 
            "l1_cat",      
            "product_id", 
            "product_name", 
            "sum_of_mrp_gmv", 
            "sum_of_qty_sold"
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
            .option("field_delimiter", "")\\
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
        df = df.with_column("SOURCE_FILE_NAME", lit(file_name.split(".")[0]+".xlsx"))


        snowdf = df.select(
            "load_date",
            "SOURCE_FILE_NAME",
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
        df = df.with_column("FILE_NAME", lit(file_name.split(".")[0]+".xlsx"))

                                    
        final_df = df.select(\\
            try_cast(col("RDSCODE"), IntegerType()).alias("RDSCODE"),
            "RDSSIZE",
            try_cast(col("CRT_DTTM"), TimestampType()).alias("CRT_DTTM"),
            "FILE_NAME"
            
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
            .option("field_delimiter", "\\u0001")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
        
        df = df.na.drop("all")
        
        if df.count() == 0:
            return "No Data in file"

        # Extract the relevant part of the file name
        extracted_file_name = "_".join((file_name.split(".")[0]).split("_")[:4])

        df = df.withColumn("load_date", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d")))
        df = df.withColumn("filename", lit(extracted_file_name))
        # df = df.withColumn("run_id", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        df = df.with_column("SOURCE_FILE_NAME", lit(Param[0].split(".")[0]+".xlsx"))


        snowdf = df.select(
            "load_date",
            "SOURCE_FILE_NAME",
            "brand_name", 
            "product_id",      
            "product_name", 
            "sum_of_sales", 
            "sum_of_mrpsales"
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
            .option("field_delimiter", "\u0001")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
        
        df = df.na.drop("all")
        
        if df.count() == 0:
            return "No Data in file"

        # Extract the relevant part of the file name
        extracted_file_name = "_".join((file_name.split(".")[0]).split("_")[:10])

        df = df.withColumn("load_date", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d")))
        df = df.withColumn("filename", lit(extracted_file_name))
        # df = df.withColumn("run_id", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        df = df.with_column("SOURCE_FILE_NAME", lit(Param[0].split(".")[0]+".xlsx"))


        snowdf = df.select(
            "load_date",
            "SOURCE_FILE_NAME",
            "account_name", 
            "account_sku_code",      
            "sku_name_in_file", 
            "brand_name", 
            "lakshya_sku_name", 
            "ean"
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
        
        df = df.with_column("SOURCE_FILE_NAME", lit(Param[0].split(".")[0]+".xlsx"))

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
