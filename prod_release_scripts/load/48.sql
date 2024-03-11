update META_RAW.PARAMETERS 
set PARAMETER_VALUE = 'MYSSDL_RAW.MY_SELLOUT_GT_IN_TRANSIT_PREPROCESSING'
where PARAMETER_GROUP_ID = 85 and PARAMETER_NAME = 'sp_name';

CREATE OR REPLACE PROCEDURE SGPSDL_RAW.SDL_SG_ZUELLIG_CUSTOMER_MAPPING_PREPROCESSING("PARAM" ARRAY)
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
    
        
        #move file into success folder
        file_name=file_name.split(".")[0]+"_"+datetime.now().strftime("%Y%m%d%H%M%S")
        snowdf.write.copy_into_location("@"+Param[1]+"/"+Param[2]+"/success/"+file_name,header=True,OVERWRITE=True)

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


CREATE OR REPLACE PROCEDURE SGPSDL_RAW.SG_AMAZON_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS 'from snowflake.snowpark.functions import col,lit,date_format,current_timestamp,to_date,year,month,concat,format_number,to_timestamp
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField,DecimalType
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col
import pytz

def main(session: snowpark.Session,Param): 
    try:
        #Param= [''Amazon_202312.csv'',''SGPSDL_RAW.DEV_LOAD_STAGE_ADLS'',''dev'',''sdl_sg_scan_data_amazon'']
        
    # Extracting parameters from the input
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
    
        df_schema = StructType([
            StructField("rm", StringType()),
            StructField("merchant_customer_id", StringType()),
            StructField("gl", StringType()),
            StructField("category", StringType()),
            StructField("subcategory", StringType()),
            StructField("brand", StringType()),
            StructField("item_code", StringType()),
            StructField("item_desc", StringType()),
            StructField("net_sales", DecimalType(10, 4)),
            StructField("pcogs", DecimalType(10, 4)),
            StructField("SALES_QTY", DecimalType(10, 0)),
            StructField("ppmpercent", DecimalType(10, 5)),
            StructField("ppmdollar", DecimalType(10, 5)),
            StructField("month", IntegerType()),
            StructField("year", IntegerType()),
            StructField("vendor_code", StringType()),
            StructField("vendor_name", StringType())
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
            return "No Data in file" 
        

        df=df.with_column("trx_date",lit(None).cast("date"))
        df=df.with_column("store",lit("Amazon"))
        df=df.with_column("store_name",lit("Amazon"))
        df=df.with_column("cdl_dttm",lit(None))
        #convertin time stamp into sg timezone
        df=df.with_column("crtd_dttm",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        df=df.with_column("file_name",lit(file_name))
        df=df.with_column("run_id",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        #df=df.with_column("run_id",col("run_id").cast(DecimalType(14,0)))

         

        #del_sql = "DELETE FROM " + stage_name.split(".")[0]+"."+target_table
        #session.sql(del_sql).collect()
    

        snowdf=df.select("trx_date","rm","merchant_customer_id","gl","category","subcategory","brand","item_code","item_desc","net_sales","pcogs","sales_qty","ppmpercent","ppmdollar","month","year","vendor_code","vendor_name","store","store_name","cdl_dttm","crtd_dttm","file_name","run_id")
     


        
            

        #move file into success folder
        file_name=file_name.split(".")[0]+"_"+datetime.now().strftime("%Y%m%d%H%M%S")
        snowdf.write.copy_into_location("@"+Param[1]+"/"+Param[2]+"/success/"+file_name,header=True,OVERWRITE=True)

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

CREATE OR REPLACE PROCEDURE SGPSDL_RAW.SG_CIW_MAP_PREPROCESSING("PARAM" ARRAY)
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
        
        

        #del_sql = "DELETE FROM " + stage_name.split(".")[0]+"."+target_table
        #session.sql(del_sql).collect()
    

        snowdf=df.select(''condition_type'', ''gl'', ''gl_description'', ''posted_where'', ''purpose'', ''ciw_bucket'', ''cdl_dttm'', ''curr_date'', ''file_name'', ''run_id'')

        
        #move file into success folder
        file_name=file_name.split(".")[0]+"_"+datetime.now().strftime("%Y%m%d%H%M%S")
        snowdf.write.copy_into_location("@"+Param[1]+"/"+Param[2]+"/success/"+file_name,header=True,OVERWRITE=True)

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

CREATE OR REPLACE PROCEDURE SGPSDL_RAW.SG_DFI_PREPROCESSING("PARAM" ARRAY)
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
    try: 
        #Param=["DFI_202312_new.csv",''SGPSDL_RAW.DEV_LOAD_STAGE_ADLS'',''dev/scan360/dfi'',''sdl_sg_scan_data_dfi'']
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        
        df_schema = StructType([
            StructField("TRXDATE", StringType()),
            StructField("BUYERCODE", StringType()),
            StructField("VENDORCODE", StringType()),
            StructField("STORECODE", StringType()),
            StructField("STORESHORTCODE", StringType()),
            StructField("STOREDESC", StringType()),
            StructField("BRAND", StringType()),
            StructField("ITEMCODE", StringType()),
            StructField("SUPPLIERITEMCODE", StringType()),
            StructField("ITEMDESC", StringType()),
            StructField("SIZE", StringType()),
            StructField("UOM", StringType()),
            StructField("PUF", StringType()),
            StructField("BARCODE", StringType()),
            StructField("SALESAMOUNT", DecimalType(10, 2)),  
            StructField("SALESQTY", DecimalType(10, 2))
        ])
        df = session.read\\
        .schema(df_schema)\\
        .option("skip_header",1)\\
        .option("field_delimiter", ",")\\
        .option("field_optionally_enclosed_by", "\\"") \\
        .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        df=df.na.drop("all")
        if df.count()==0 :
            return "No Data in file"


        df = df.withColumn("CUST_NAME", when(col("STORECODE").like("G%"),"Giant").otherwise(
                                      when(col("STORECODE").like("Q%"), "Cold Storage").otherwise("711")))
        df = df.with_column("cdl_dttm", lit(None))
        df = df.with_column("run_id", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S"))) 
        df=df.with_column("file_name",lit(file_name))
        df=df.with_column("crtd_dttm",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        df = df.withColumn("TRXDATE", regexp_replace(col("TRXDATE"), "\\\\.", "-").cast(DateType()))
        

    

        snowdf=df.select("trxdate", "buyercode", "vendorcode", "storecode", "storeshortcode", "storedesc", "brand", "itemcode", "supplieritemcode", "itemdesc", "size", "uom", "puf", "barcode", "salesamount", "salesqty", "CUST_NAME", "cdl_dttm", "crtd_dttm", "file_name", "run_id")
        


        
        
        #move file into success folder
        file_name=file_name.split(".")[0]+"_"+datetime.now().strftime("%Y%m%d%H%M%S")
        snowdf.write.copy_into_location("@"+Param[1]+"/"+Param[2]+"/success/"+file_name,header=True,OVERWRITE=True)
        
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

CREATE OR REPLACE PROCEDURE SGPSDL_RAW.SG_NTUC_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS 'from snowflake.snowpark.functions import col, lit, date_format, current_timestamp, to_date, year, month, concat, format_number, to_timestamp,when,trim,upper
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DecimalType,DateType
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz

def main(session: snowpark.Session,Param):
    try:
        #Param=["NTUC_202112.csv", "SBX_DNA_LAB1.NBANGA01_RAW.DEV_LOAD_STAGE_ADLS_2","test5","s"]
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        
        # Define the schema
        df_schema = StructType([
                StructField("Vendor_Code", StringType()),
                StructField("Vendor_Name", StringType()),
                StructField("Dept_Code", StringType()),
                StructField("Dept_Description", StringType()),
                StructField("Class_No", StringType()),
                StructField("Class_Description", StringType()),
                StructField("Sub_Class_Description", StringType()),
                StructField("MCH", StringType()),
                StructField("ITEM_CODE", StringType()),
                StructField("ITEM_DESC", StringType()),
                StructField("Brand", StringType()),
                StructField("Sales_UOM", StringType()),
                StructField("Pack_Size", DecimalType(10,0)),
                StructField("Store_Code", StringType()),
                StructField("Store_Name", StringType()),
                StructField("Store_Format", StringType()),
                StructField("Attribute1", StringType()),
                StructField("Attribute2", StringType()),
                StructField("TRX_DATE", DateType()),
                StructField("Value", DecimalType(38, 8))  
        ])

        df = session.read\\
        .schema(df_schema)\\
        .option("skip_header",1)\\
        .option("field_delimiter", "\\u0001")\\
        .option("field_optionally_enclosed_by", "\\"") \\
        .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
        df=df.na.drop("all")
        if df.count()==0 :
            return "No Data in file"

        #pivoting using when statement 

        df = df.withColumn("net_sales", 
                   when((trim(upper(df["Attribute1"])) == "SALES"), col("value"))
                   .otherwise(None)
                   .cast(DecimalType(14, 4)))

        df = df.withColumn("SALES_QTY", 
                   when((trim(df["Attribute1"]) == "Qty (in EA)"), col("value"))
                   .cast(DecimalType(10, 0)))

        df=df.with_column("file_name",lit(file_name))
        df=df.with_column("CUST_NAME",lit("NTUC"))
        #convertin time stamp into sg timezone
        df=df.with_column("crtd_dttm",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        df = df.with_column("cdl_dttm", lit(None))
        df = df.with_column("run_id", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
       
        

        #del_sql = "DELETE FROM " + stage_name.split(".")[0]+"."+target_table
        #session.sql(del_sql).collect()
    

        snowdf=df.select("Vendor_Code", "Vendor_Name", "Dept_Code", "Dept_Description", "Class_No", "Class_Description", "Sub_Class_Description", "MCH", "ITEM_CODE", "ITEM_DESC", "Brand", "Sales_UOM", "Pack_Size", "Store_Code", "Store_Name", "Store_Format", "Attribute1", "Attribute2", "TRX_DATE", "NET_SALES", "SALES_QTY", "CUST_NAME", "cdl_dttm", "crtd_dttm", "file_name", "run_id")
    

        
            
        
        #move file into success folder
        file_name=file_name.split(".")[0]+"_"+datetime.now().strftime("%Y%m%d%H%M%S")
        snowdf.write.copy_into_location("@"+Param[1]+"/"+Param[2]+"/success/"+file_name,header=True,OVERWRITE=True)
        
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

CREATE OR REPLACE PROCEDURE SGPSDL_RAW.SG_WATSONS_PREPROCESSING("PARAM" ARRAY)
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
    
    #Param=["SG_SCAN_DATA_WATSONS_Apr16.csv", "SBX_DNA_LAB1.NBANGA01_RAW.DEV_LOAD_STAGE_ADLS_2","test5","s"]
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]

        
        df_schema = StructType([
            StructField("year", IntegerType()),
            StructField("week", IntegerType()),
            StructField("store", StringType()),
            StructField("div", StringType()),
            StructField("prdt_dept", StringType()),
            StructField("prdtcode", StringType()),
            StructField("prdtdesc", StringType()),
            StructField("brand", StringType()),
            StructField("supcode", StringType()),
            StructField("sup_name", StringType()),
            StructField("barcode", StringType()),
            StructField("sup_cat", StringType()),
            StructField("dept_name", StringType()),
            StructField("net_sales", StringType()),
            StructField("sales_qty", DecimalType(10, 0))
        ])

        df = session.read\\
        .schema(df_schema)\\
        .option("skip_header",1)\\
        .option("field_delimiter", ",")\\
        .option("field_optionally_enclosed_by", "\\"") \\
        .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        df=df.na.drop("all")
        if df.count()==0 :
            return "No Data in file"


        df = df.with_column("run_id", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S"))) 
        df=df.with_column("file_name",lit(file_name))
        df=df.with_column("crtd_dttm",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        df = df.with_column("cdl_dttm", lit(None))
        df = df.with_column("cust_name", lit("Watsons"))
    
    

        snowdf=df.select("year", "week", "store", "div", "prdt_dept", "prdtcode", "prdtdesc", "brand", "supcode", "sup_name", "barcode", "sup_cat", "dept_name", "net_sales", "sales_qty", "CUST_NAME", "cdl_dttm", "crtd_dttm", "file_name", "run_id")
        



        
        
        #move file into success folder
        file_name=file_name.split(".")[0]+"_"+datetime.now().strftime("%Y%m%d%H%M%S")
        snowdf.write.copy_into_location("@"+Param[1]+"/"+Param[2]+"/success/"+file_name,header=True,OVERWRITE=True)

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

CREATE OR REPLACE PROCEDURE SGPSDL_RAW.SG_ZUELLIG_PRODUCT_MAPPIN_PREPROCESSING("PARAM" ARRAY)
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
    
        
        #move file into success folder
        file_name=file_name.split(".")[0]+"_"+datetime.now().strftime("%Y%m%d%H%M%S")
        snowdf.write.copy_into_location("@"+Param[1]+"/"+Param[2]+"/success/"+file_name,header=True,OVERWRITE=True)

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


CREATE OR REPLACE PROCEDURE SGPSDL_RAW.SG_ZUELLIG_SELLOUT_PREPROCESSING("PARAM" ARRAY)
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
        snowdf.write.copy_into_location("@"+Param[1]+"/"+Param[2]+"/success/"+file_name,header=True,OVERWRITE=True)

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
