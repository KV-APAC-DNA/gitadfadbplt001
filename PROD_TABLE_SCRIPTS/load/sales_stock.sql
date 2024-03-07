CREATE OR REPLACE PROCEDURE "test_HAINAN_PREPROCESSING"("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS 'import snowflake.snowpark as snowpark
from snowflake import snowpark
from snowflake.snowpark.functions import col,lit,current_timestamp,to_date,year,month,to_timestamp
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField,DecimalType
import pandas as pd
from datetime import datetime

    
def main(session: snowpark.Session,Param): 
    # Your code goes here, inside the "main" handler.
    #Param=[''Hainan_Vendor_Sales_Report_for_Asian.csv'',''ASPSDL_RAW.DEV_LOAD_STAGE_ADLS'',''dev/transactional/dfs'',''sdl_rg_travel_retail_dfS_hainan'']
    try:
        # Extracting parameters from the input
        file_name       = Param[0]
        stage_name      = Param[1]
        adls_path       = Param[2]
        db_name         = stage_name.split(".")[0]
        target_table    = db_name+"."+Param[3]

        # Define the schema for the DataFrame
        df_schema=StructType([
            StructField("product_department_desc",StringType()),
            StructField("product_department_code",StringType()),
            StructField("brand",StringType()),
            StructField("product_class_desc",StringType()),
            StructField("product_class_code",StringType()),
            StructField("product_subclass_desc",StringType()),
            StructField("product_subclass_code",StringType()),
            StructField("brand_collection",StringType()),
            StructField("reatiler_product_code",StringType()),
            StructField("reatiler_product_description",StringType()),
            StructField("dcl_code",StringType()),
            StructField("ean",StringType()),
            StructField("style_type_code",StringType()),
            StructField("month",StringType()),
            StructField("door_name",StringType()),
            StructField("sls_mtd_qty",DecimalType(precision=18, scale=0)),
            StructField("sls_mtd_amt",DecimalType(precision=38, scale=18)),
            StructField("sls_ytd_qty",DecimalType(precision=18, scale=0)),
            StructField("sls_ytd_amt",DecimalType(precision=38, scale=18))
            ])
        
        # Read the CSV file into a DataFrame
        df = session.read \\
            .schema(df_schema) \\
            .option("skip_header", 2) \\
            .option("field_delimiter", "\\u0001") \\
            .option("field_optionally_enclosed_by", "\\"") \\
            .csv("@" + Param[1] + "/" + Param[2] + "/" + file_name)

        # Add retailer_name, crttd, and file_name,yearmo,year,mon
        df = df.with_column("Retailer_name",lit("DFS HAINAN"))
        df = df.with_column("filename",lit(file_name.split(".")[0].replace("_"," ")+".xlsx"))
        df = df.withColumn("crttd", lit(to_timestamp(current_timestamp())))
        df = df.withColumn("yearmo", to_date(col("month"), ''Mon yy''))
        df = df.withColumn("year", year(col("yearmo")))
        df = df.withColumn("mon", month(col("yearmo")))
        
        snowdf=df.select("year","mon","yearmo","Retailer_name","product_department_desc","product_department_code","brand","product_class_desc",
                  "product_class_code","product_subclass_desc","product_subclass_code","brand_collection","reatiler_product_code","reatiler_product_description",
                  "dcl_code","ean","style_type_code","month","door_name","sls_mtd_qty","sls_mtd_amt","sls_ytd_qty","sls_ytd_amt","filename","crttd")
        
        snowdf = snowdf[snowdf["reatiler_product_code"].isNotNull()]
        if snowdf.count()==0 :
            return "No Data in file" 
        # Create a Snowflake DataFrame and write to success folder
        
        # for DFS HAINAN, we dont have truncate and load into sdl table. we have append logic  
        
        snowdf.write.mode("append").saveAsTable(target_table)

        file_name=file_name.split(".")[0]+''_''+datetime.now().strftime("%Y%m%d%H%M%S")
        snowdf.write.copy_into_location("@"+stage_name+"/"+adls_path+"/success/"+file_name,file_format_type="csv",header=True,OVERWRITE=True)
    
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
