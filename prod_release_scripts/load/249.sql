CREATE OR REPLACE PROCEDURE VNMSDL_RAW.VN_MTI_OFFTAKE_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS 'from snowflake.snowpark.functions import col, lit, date_format, current_timestamp, to_date, year, month, concat, format_number, regexp_replace, to_timestamp, when, trim, upper
from snowflake.snowpark.types import StringType, StructType, StructField
import snowflake.snowpark
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz

def main(session: snowpark.Session,Param):
    #Param=["SPIRAL_MT_OFFTAKE_202307_20231226134908.csv","VNMSDL_RAW.DEV_LOAD_STAGE_ADLS","dev/VN_MTI","sdl_SPIRAL_MTI_OFFTAKE"]
    try:
        file_name = Param[0]
        stage_name = Param[1]
        temp_stage_path = Param[2]
        target_table = Param[3]
        
        df_schema = StructType([
                StructField("STT", StringType(), True),
                StructField("Area", StringType(), True),
                StructField("ChannelName", StringType(), True),
                StructField("CustomerName", StringType(), True),
                StructField("ShopCode", StringType(), True),
                StructField("ShopName", StringType(), True),
                StructField("Address", StringType(), True),
                StructField("SupCode", StringType(), True),
                StructField("SupName", StringType(), True),
                StructField("Year", StringType(), True),
                StructField("Month", StringType(), True),
                StructField("Barcode", StringType(), True),
                StructField("ProductName", StringType(), True),
                StructField("Franchise", StringType(), True),
                StructField("Brand", StringType(), True),
                StructField("Cate", StringType(), True),
                StructField("Sub_cat", StringType(), True),
                StructField("Sub_brand", StringType(), True),
                StructField("Size", StringType(), True),
                StructField("Quantity", StringType(), True),
                StructField("Amount", StringType(), True),
                StructField("AmountUSD", StringType(), True)
            ])
        
        df = session.read\\
        .schema(df_schema)\\
        .option("skip_header",1)\\
        .option("field_delimiter", "\\u0001")\\
        .option("field_optionally_enclosed_by", "\\"") \\
        .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
        df = df.na.drop("all")
        
        if df.count() == 0:
            return "No Data in file"
        
        file_name1 = file_name.split(".")[0] + "_" + datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S") + ".csv"
        df = df.withColumn("file_name", lit(file_name1))
        df=df.filter(col("Area")!="Area")

        df=df.na.fill("",subset=''SupCode'')

        
        ex_df=session.sql("select *  from vnmsdl_raw.sdl_SPIRAL_MTI_OFFTAKE")



        ex_df=ex_df.na.fill("",subset=''SupCode'')

        
        df_j=df.join(ex_df,["CustomerName", "ShopCode", "SupCode", "Year", "Month", "BarCode"]).select(df.STT,
                                                                                                        df.Area,
                                                                                                        df.ChannelName,
                                                                                                        df.CustomerName,
                                                                                                        df.ShopCode,
                                                                                                        df.ShopName,
                                                                                                        df.Address,
                                                                                                        df.SupCode,
                                                                                                        df.SupName,
                                                                                                        df.Year,
                                                                                                        df.Month,
                                                                                                        df.Barcode,
                                                                                                        df.ProductName,
                                                                                                        df.Franchise,
                                                                                                        df.Brand,
                                                                                                        df.Cate,
                                                                                                        df.Sub_cat,
                                                                                                        df.Sub_brand,
                                                                                                        df.Size,
                                                                                                        df.Quantity,
                                                                                                        df.Amount,
                                                                                                        df.AmountUSD,
                                                                                                        df.file_name
                                                                                                      )
        df_f= ex_df.subtract(df_j)

        snowdf=df_f.union_all(df)

        
        snowdf=snowdf.na.replace({"":None},[''SupCode''])

        
        snowdf.write.mode("overwrite").saveAsTable(stage_name.split(".")[0] + "." + target_table)

        file_name=file_name.split(".")[0]+"_"+ datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        
        #move to success
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

use schema PCFSDL_RAW;
CREATE or replace FILE FORMAT file_format_zip
FIELD_OPTIONALLY_ENCLOSED_BY = '\"'
FIELD_DELIMITER = ',';

CREATE or replace FILE FORMAT MY_CSV_FORMAT
FIELD_OPTIONALLY_ENCLOSED_BY = '\"'
FIELD_DELIMITER = ',';

CREATE or replace FILE FORMAT MY_FILE_FORMAT
FIELD_DELIMITER = ',';
