CREATE OR REPLACE PROCEDURE DEV_DNA_LOAD.SGPSDL_RAW.SG_POP6_PRODUCT_LISTS_PRODUCTS_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS 'from snowflake.snowpark.functions import col, lit, date_format, current_timestamp, to_date,md5, year, month, concat, format_number, regexp_replace, to_timestamp, when, trim, upper
from snowflake.snowpark.types import StringType, StructType, StructField,DecimalType
import snowflake.snowpark
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz

def main(session: snowpark.Session,Param):
    #Param=["20240620_product_lists_products.csv","SGPSDL_RAW.DEV_LOAD_STAGE_ADLS","dev/pop6/master/listdata","sdl_pop6_sg_pops"]    
    try:
        file_name = Param[0]
        stage_name = Param[1]
        temp_stage_path = Param[2]
        target_table = Param[3]

        df_schema = StructType([
                    StructField("Product_List", StringType(), True),
                    StructField("Product_List_Code", StringType(), True),
                    StructField("ProductDB_ID", StringType(), True),
                    StructField("SKU", StringType(), True),
                    StructField("MSL_Ranking", StringType(), True),
                    StructField("Date", StringType(), True)
                ])
        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "|")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name.split(".")[0])
        
              
        df = df.na.drop("all")
        if df.count() == 0:
            return "No Data in file"

        
        df = df.withColumn("filename", lit(file_name))
        df = df.withColumn("crt_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        df = df.withColumn("run_id", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))

        df1=df
        concat_columns = [
                            "Product_List",
                            "Product_List_Code",
                            "ProductDB_ID",
                            "SKU",
                            "MSL_Ranking",
                            "Date"
                    ]
        
        df1=df1.na.fill("",subset=concat_columns)


       # Concatenate the columns
        df1 = df1.withColumn("hash_c", concat(*[df1[col] for col in concat_columns]))

        #df1 = df1.withColumn("hash_c",concat(df1.Product_List,df1.ProductDB_ID,df1.SKU,df1.MSL_Ranking,df1.Date))
        df1=df1.withColumn("hash",md5(col("hash_c")))
        df1=df1.na.replace({"":None},concat_columns)
        snowdf = df1.select(
                             "Product_List",
                            "Product_List_Code",
                            "ProductDB_ID",
                            "SKU",
                            "MSL_Ranking",
                            "Date",
                            "filename",
                            "run_id",
                            "crt_dttm",
                            "hash"
                        )
        
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        
        #move to success
        
        snowdf.write.mode("append").saveAsTable(stage_name.split(".")[0] + "." + target_table)
        #snowdf.write.mode("append").saveAsTable(target_table)
        file_name1 = file_name.split(".")[0] + "_" + datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        snowdf.write.copy_into_location("@" + stage_name + "/" + temp_stage_path + "/processed/success/" + formatted_year + "/" + formatted_month + "/" + file_name1, header=True, OVERWRITE=True)
        
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
