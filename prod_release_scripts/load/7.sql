CREATE OR REPLACE PROCEDURE MYSSDL_RAW.MY_SELLOUT_SALES_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them.



from snowflake.snowpark.functions import col, lit, date_format, current_timestamp, to_date, year, month,  format_number, to_timestamp,when,trim,regexp_replace
from snowflake.snowpark.types import  StringType, StructType, StructField, DecimalType
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz
def main(session: snowpark.Session,Param):
    #Param=[''MY_GT_108273_Sales_20240227.csv'',''MYSSDL_RAW.DEV_LOAD_STAGE_ADLS'',''dev/transaction/sales'',''SDL_SO_SALES_108273'']
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        stage_path = Param[2]
        target_table    = Param[3]
        # Define the schema
        df_schema = StructType([
                StructField("DISTRIBUTOR_ID", StringType()),
                StructField("SALES_ORDER_NUMBER", StringType()),
                StructField("SALES_ORDER_DATE", StringType()),
                StructField("TYPE", StringType()),
                StructField("CUSTOMER_CODE", StringType()),
                StructField("DISTRIBUTOR_WH_ID", StringType()),
                StructField("SAP_MATERIAL_ID", StringType()),
                StructField("PRODUCT_CODE", StringType()),
                StructField("PRODUCT_EAN_CODE", StringType()),
                StructField("PRODUCT_DESCRIPTION", StringType()),
                StructField("GROSS_ITEM_PRICE", StringType()),
                StructField("QUANTITY", StringType()),
                StructField("UOM", StringType()),
                StructField("QUANTITY_IN_PIECES", StringType()),
                StructField("QUANTITY_AFTER_CONVERSION", StringType()),
                StructField("SUB_TOTAL_1", StringType()),
                StructField("DISCOUNT", StringType()),
                StructField("SUB_TOTAL_2", StringType()),
                StructField("BOTTOM_LINE_DISCOUNT", StringType()),
                StructField("TOTAL_AMT_AFTER_TAX", StringType()),
                StructField("TOTAL_AMT_BEFORE_TAX", StringType()),
                StructField("SALES_EMPLOYEE", StringType()),
                StructField("CUSTOM_FIELD_1", StringType()),
                StructField("CUSTOM_FIELD_2", StringType()),
                StructField("CUSTOM_FIELD_3", StringType())
        ])



        #read the file
        df = session.read\\
        .schema(df_schema)\\
        .option("skip_header",0)\\
        .option("field_delimiter", ",")\\
        .option("encoding", "ISO-8859-15") \\
        .option("field_optionally_enclosed_by", "\\"") \\
        .csv("@"+stage_name+"/"+stage_path+"/"+file_name)
        
        #create the column which are derived from file name
        
        #df = df.withColumn("GROSS_ITEM_PRICE", regexp_replace(col("GROSS_ITEM_PRICE"), ",", ""));
        df = df.withColumn("GROSS_ITEM_PRICE", regexp_replace(col("GROSS_ITEM_PRICE"), ",", ""));
        df = df.withColumn("QUANTITY", regexp_replace(col("QUANTITY"), ",", ""));
        df = df.withColumn("QUANTITY_IN_PIECES", regexp_replace(col("QUANTITY_IN_PIECES"), ",", ""));
        df = df.withColumn("QUANTITY_AFTER_CONVERSION", regexp_replace(col("QUANTITY_AFTER_CONVERSION"), ",", ""));
        df = df.withColumn("SUB_TOTAL_1", regexp_replace(col("SUB_TOTAL_1"), ",", ""));
        df = df.withColumn("SUB_TOTAL_2", regexp_replace(col("SUB_TOTAL_2"), ",", ""));
        df = df.withColumn("BOTTOM_LINE_DISCOUNT", regexp_replace(col("BOTTOM_LINE_DISCOUNT"), ",", ""));
        df = df.withColumn("TOTAL_AMT_AFTER_TAX", regexp_replace(col("TOTAL_AMT_AFTER_TAX"), ",", ""));
        df = df.withColumn("TOTAL_AMT_BEFORE_TAX", regexp_replace(col("TOTAL_AMT_BEFORE_TAX"), ",", ""));
        df=df.with_column("file_name",lit(file_name))
    

        
        
        snowdf=df.select("DISTRIBUTOR_ID","SALES_ORDER_NUMBER","SALES_ORDER_DATE","TYPE","CUSTOMER_CODE","DISTRIBUTOR_WH_ID","SAP_MATERIAL_ID","PRODUCT_CODE","PRODUCT_EAN_CODE","PRODUCT_DESCRIPTION","GROSS_ITEM_PRICE","QUANTITY","UOM","QUANTITY_IN_PIECES","QUANTITY_AFTER_CONVERSION","SUB_TOTAL_1","DISCOUNT","SUB_TOTAL_2","BOTTOM_LINE_DISCOUNT","TOTAL_AMT_AFTER_TAX","TOTAL_AMT_BEFORE_TAX","SALES_EMPLOYEE","CUSTOM_FIELD_1","CUSTOM_FIELD_2","CUSTOM_FIELD_3","file_name")
        
        snowdf= snowdf.filter(snowdf["DISTRIBUTOR_ID"].isNotNull())
        
        if snowdf.count()==0 :
            return "No Data in file"

        
        #move file into success folder
        file_name=file_name.split(".")[0]+"_"+datetime.now().strftime("%Y%m%d%H%M%S")

        snowdf.write.mode("append").saveAsTable(stage_name.split(".")[0]+"."+target_table)
    
        
        # Copy data into the location using the specified file format


        #snowdf.write.copy_into_location("@"+stage_name+"/"+stage_path+"/success/"+file_name,file_format_type="csv",header=True,OVERWRITE=True)


        sql = f"COPY INTO your_table FROM @{stage_name}/{stage_path}/success/{file_name} FILE_FORMAT = (FORMAT_TYPE=''csv'' FIELD_OPTIONALLY_ENCLOSED_BY=''\\"'') OVERWRITE = TRUE"
        session.sql(sql)
        
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

CREATE OR REPLACE PROCEDURE MYSSDL_RAW.MY_SELLOUT_INV_PREPROCESSING("PARAM" ARRAY)
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

def main(session:snowpark.Session, Param):
    
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]

        df_schema = StructType([
            StructField("DISTRIBUTOR_ID", StringType()),
            StructField("DATE", StringType()),
            StructField("DISTRIBUTOR_WH_ID", StringType()),
            StructField("SAP_MATERIAL_ID", StringType()),
            StructField("PRODUCT_CODE", StringType()),
            StructField("PRODUCT_EAN_CODE", StringType()),
            StructField("PRODUCT_DESCRIPTION", StringType()),
            StructField("QUANTITY_AVAILABLE", StringType()),
            StructField("UOM_AVAILABLE", StringType()),
            StructField("QUANTITY_ON_ORDER", StringType()),
            StructField("UOM_ON_ORDER", StringType()),
            StructField("QUANTITY_COMMITTED", StringType()),
            StructField("UOM_COMMITTED", StringType()),
            StructField("QUANTITY_AVAILABLE_IN_PIECES", StringType()),
            StructField("QUANTITY_ON_ORDER_IN_PIECES", StringType()),
            StructField("QUANTITY_COMMITTED_IN_PIECES", StringType()),
            StructField("UNIT_PRICE", StringType()),
            StructField("TOTAL_VALUE_AVAILABLE", StringType()),
            StructField("CUSTOM_FIELD_1", StringType()),
            StructField("CUSTOM_FIELD_2", StringType())
        ])

        df = session.read\\
        .schema(df_schema)\\
        .option("skip_header",1)\\
        .option("field_delimiter", ",")\\
        .option("encoding", "ISO-8859-15") \\
        .option("field_optionally_enclosed_by", "\\"") \\
        .option("null_if", "") \\
        .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        #df = df.with_column("run_id", lit(None).cast(DecimalType(14,0)))
        df = df.with_column("cdl_dttm", lit(datetime.now().strftime("%Y%m%d%H%M%S")))
        df=df.with_column("file_name",lit(file_name))
        df = df.withColumn("crtd_dttm", current_timestamp())

        snowdf=df.select(
                            "DISTRIBUTOR_ID",
                            "DATE",
                            "DISTRIBUTOR_WH_ID",
                            "SAP_MATERIAL_ID",
                            "PRODUCT_CODE",
                            "PRODUCT_EAN_CODE",
                            "PRODUCT_DESCRIPTION",
                            "QUANTITY_AVAILABLE",
                            "UOM_AVAILABLE",
                            "QUANTITY_ON_ORDER",
                            "UOM_ON_ORDER",
                            "QUANTITY_COMMITTED",
                            "UOM_COMMITTED",
                            "QUANTITY_AVAILABLE_IN_PIECES",
                            "QUANTITY_ON_ORDER_IN_PIECES",
                            "QUANTITY_COMMITTED_IN_PIECES",
                            "UNIT_PRICE",
                            "TOTAL_VALUE_AVAILABLE",
                            "CUSTOM_FIELD_1",
                            "CUSTOM_FIELD_2",
                            "FILE_NAME")
        
        snowdf= snowdf.filter(snowdf["Product_Code"].isNotNull())
        
        if snowdf.count()==0 :
            return "No Data in table"


        file_name1=("_").join(file_name.split("_")[0:5])+"_"+datetime.now().strftime("%Y%m%d%H%M%S")
        snowdf.write.copy_into_location("@"+Param[1]+"/"+Param[2]+"/success/"+file_name1,header=True,OVERWRITE=True)
        

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