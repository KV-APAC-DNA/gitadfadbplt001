USE SCHEMA VNMSDL_RAW;

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
    #Param=["SPIRAL_MT_OFFTAKE_202308.csv","VNMSDL_RAW.DEV_LOAD_STAGE_ADLS","dev/VN_MTI/transaction/VN_MTI_OFFTAKE","sdl_SPIRAL_MTI_OFFTAKE"]
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

        for i in [''CustomerName'', ''ShopCode'', ''SupCode'', ''Year'', ''Month'', ''BarCode'']:

            df=df.na.fill("",subset=f''{i}'')

        
        ex_df=session.sql("select *  from vnmsdl_raw.sdl_SPIRAL_MTI_OFFTAKE")



        for i in [''CustomerName'', ''ShopCode'', ''SupCode'', ''Year'', ''Month'', ''BarCode'']:

            ex_df=ex_df.na.fill("",subset=f''{i}'')


        df_j=df.join(ex_df,["CustomerName", "ShopCode", "SupCode", "Year", "Month", "BarCode"]).select(ex_df.STT,
                                                                                                        ex_df.Area,
                                                                                                        ex_df.ChannelName,
                                                                                                        ex_df.CustomerName,
                                                                                                        ex_df.ShopCode,
                                                                                                        ex_df.ShopName,
                                                                                                        ex_df.Address,
                                                                                                        ex_df.SupCode,
                                                                                                        ex_df.SupName,
                                                                                                        ex_df.Year,
                                                                                                        ex_df.Month,
                                                                                                        ex_df.Barcode,
                                                                                                        ex_df.ProductName,
                                                                                                        ex_df.Franchise,
                                                                                                        ex_df.Brand,
                                                                                                        ex_df.Cate,
                                                                                                        ex_df.Sub_cat,
                                                                                                        ex_df.Sub_brand,
                                                                                                        ex_df.Size,
                                                                                                        ex_df.Quantity,
                                                                                                        ex_df.Amount,
                                                                                                        ex_df.AmountUSD,
                                                                                                        ex_df.file_name
                                                                                                      )
    
        df_f= ex_df.subtract(df_j)

        snowdf=df_f.union_all(df)

        for i in [''CustomerName'', ''ShopCode'', ''SupCode'', ''Year'', ''Month'', ''BarCode'']:
            snowdf=snowdf.na.replace({"":None},[f''{i}''])

        

        
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


TRUNCATE table VNMSDL_RAW.sdl_SPIRAL_MTI_OFFTAKE;
