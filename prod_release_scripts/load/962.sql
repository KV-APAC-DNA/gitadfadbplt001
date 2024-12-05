CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.NTASDL_RAW.TW_CRM_INVOICE_PREPROCESSING_1205_BKP("PARAM" ARRAY)
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
    #Param=["TW_CRM_Invoice_20240430.csv","NTASDL_RAW.DEV_LOAD_STAGE_ADLS_TWN","dev/SFMC/transaction/tw_crm_invoice/","SDL_TW_SFMC_INVOICE_DATA"]
    try:
        file_name = Param[0]
        stage_name = Param[1]
        temp_stage_path = Param[2]
        target_table = Param[3]
        df_schema = StructType([
            StructField("purchase_date", StringType(), True),
            StructField("channel", StringType(), True),
            StructField("product", StringType(), True),
            StructField("status", StringType(), True),
            StructField("created_date", StringType(), True),
            StructField("completed_date", StringType(), True),
            StructField("subscriber_key", StringType(), True),
            StructField("points", StringType(), True),
            StructField("show_record", StringType(), True),
            StructField("qty", StringType(), True),
            StructField("invoice_type", StringType(), True),
            StructField("seller_nm", StringType(), True),
            StructField("product_category", StringType(), True),
            StructField("website_unique_id", StringType(), True),
            StructField("invoice_num", StringType(), True),
            StructField("epsilon_price_per_unit", StringType(), True),
            StructField("epsilon_amount", StringType(), True),
            StructField("epsilon_total_amount", StringType(), True),
        ])
        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "|")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .option("encoding","UTF-16")\\
            .option("record_delimiter","\\n")\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
        df = df.na.drop("all")
        if df.count() == 0:
            return "No Data in file"
        df = df.withColumn("EPSILON_PRICE_PER_UNIT",regexp_replace(lit(col("EPSILON_PRICE_PER_UNIT")),"''",''''))
        df = df.withColumn("EPSILON_AMOUNT",regexp_replace(lit(col("EPSILON_AMOUNT")),"''",''''))
        
        df = df.withColumn("crtd_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        df = df.withColumn("filename", lit(file_name))
        # df = df.withColumn("run_id", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        snowdf = df.select(
           "purchase_date",
           "channel",
           "product",
           "status",
           "created_date",
           "completed_date",
           "subscriber_key",
           "points",
           "show_record",
           "qty",
           "invoice_type",
           "seller_nm",
           "product_category",
           "website_unique_id",
           "invoice_num",
           "epsilon_price_per_unit",
           "epsilon_amount",
           "epsilon_total_amount",
            "filename",
            "crtd_dttm"
            )
        file_name=file_name.split(".")[0]+"_"+ datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        #move to success
        snowdf.write.mode("append").saveAsTable(stage_name.split(".")[0] + "." + target_table)
        snowdf.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)
        return ''Success''
    except KeyError as key_error:
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message
    except pd.errors.MergeError as merge_error:
        error_message = f"DataFrame merging error: {str(merge_error)}"
        return error_message
    except Exception as e:
        error_message = f"Error: {str(e)}"
        return error_message';

CREATE OR REPLACE PROCEDURE DEV_DNA_LOAD.NTASDL_RAW.TW_CRM_INVOICE_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('openpyxl==3.0.10','snowflake-snowpark-python==*','unidecode==1.2.0','chardet')
HANDLER = 'main'
EXECUTE AS OWNER
AS '
from snowflake.snowpark.functions import col, lit, date_format, current_timestamp, to_date, year, month, concat, format_number, regexp_replace, to_timestamp, when, trim, upper, concat, split, coalesce,get, regexp_count,seq1,row_number,to_date,is_null
import pandas as pd
from datetime import datetime
from snowflake.snowpark.window import Window
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField,DecimalType,DateType
from snowflake.snowpark.files import SnowflakeFile
import snowflake.snowpark as snowpark
from unidecode import unidecode
from re import sub
import pytz
import csv
import chardet

def main(session:snowpark.Session,Param):

    try:
        file_name = Param[0]
        stage_name = Param[1]
        temp_stage_path = Param[2]
        target_table = Param[3]

      
        full_path = "@"+stage_name+"/"+temp_stage_path + "/" +file_name


        schema = StructType([StructField("LineData", StringType())                            
                            ])
        dfmain = session.read.schema(schema).option("encoding","UTF-16LE").option("VALIDATE_UTF8",True).option("skip_header",1).option("field_delimiter","\\\\u0001").csv(full_path)
       
        dfmain = dfmain.with_column("no_of_sep",regexp_count(col("LineData"),''\\|''))
        dfmain = dfmain.with_column("MainSno",seq1())
        
        dffiltered = dfmain.filter(col("no_of_sep") < 17)
        dffiltered = dffiltered.with_column("FilteredSno",seq1())
        
        start_seq = 0
        max_sep_cnt = 17
        prev_sep_cnt = 0
        setindicator = 1
        df_count = dffiltered.count()
        temp_merge_final = session.create_dataframe([["TOBEDELETEDROW",0]], schema=["LINEDATA", "MainSno"])

        for index in range(df_count):
            curr_sep_cnt = dffiltered.filter(col("FilteredSno") == index).select(col(''no_of_sep'')).collect()[0][0]
            curr_sep_cnt = prev_sep_cnt + curr_sep_cnt
            if curr_sep_cnt == max_sep_cnt:
                end_seq = index
                print("Start_seq:",start_seq)
                print("End_seq:",end_seq)

                if(end_seq-start_seq == 1):
                    temp1 = dffiltered.filter(col("FilteredSno") == start_seq)
                    temp2 = dffiltered.filter(col("FilteredSno") == end_seq)
                
                    temp1 = temp1.with_column("setindicator",lit(setindicator))
                    temp2 = temp2.with_column("setindicator",lit(setindicator))

                    temp_merged1 = temp1.join(temp2, temp1.setindicator == temp2.setindicator).select(concat(temp1.LINEDATA,temp2.LINEDATA).alias("LINEDATA"),temp1.mainsno.alias("MainSno"))
                    
                    temp_merge_final = temp_merge_final.union_all_by_name(temp_merged1)
                                
                if(end_seq-start_seq > 1):
                    print("Inside:")
                    temp1 = dffiltered.filter(col("FilteredSno") == start_seq)
                    temp2 = dffiltered.filter(col("FilteredSno") == start_seq+1)
                
                    temp1 = temp1.with_column("setindicator",lit(setindicator))
                    temp2 = temp2.with_column("setindicator",lit(setindicator))

                    temp3 = temp1.join(temp2, temp1.setindicator == temp2.setindicator).select(concat(temp1.LINEDATA,temp2.LINEDATA).alias("LINEDATA"),temp1.mainsno.alias("MainSno"),temp1.setindicator.alias("setindicator"))

                    for i in range(start_seq+2,end_seq+1):
                        print("i:",i)
                        print("setindicator:",setindicator)
                        temp4 = dffiltered.filter(col("FilteredSno") == i)
                        temp4 = temp4.with_column("setindicator",lit(setindicator))
                        
                        temp3 = temp3.join(temp4, temp3.setindicator == temp4.setindicator).select(concat(temp3.LINEDATA,temp4.LINEDATA).alias("LINEDATA"),temp3.mainsno.alias("MainSno"),temp3.setindicator.alias("setindicator"))
                    temp_merge2 = temp3.select(col("LINEDATA").alias("LINEDATA"),col("MainSno").alias("MainSno"))
                    temp_merge_final = temp_merge_final.union_all_by_name(temp_merge2)

                
                setindicator = setindicator + 1
                prev_sep_cnt = 0
                start_seq = end_seq + 1
                
            else:
                prev_sep_cnt = curr_sep_cnt

            temp_merge_final = temp_merge_final.filter(col("LINEDATA") != "TOBEDELETEDROW")

            dfcombined = dfmain.join(temp_merge_final,dfmain.MainSno == temp_merge_final.MainSno,"left").select(coalesce(temp_merge_final.LINEDATA,dfmain.LINEDATA).alias("LINEDATA"),dfmain.MainSno.alias("MainSno")).sort(col("MainSno").asc())
            dfcombined = dfcombined.with_column("no_of_sep",regexp_count(col("LineData"),''\\|''))
            dffinal = dfcombined.filter(col("no_of_sep") == 17).select(regexp_replace(col("LINEDATA"),lit("\\t"),"").alias("LINEDATA"))

            dh = dffinal.select(split(col("LINEDATA"),lit("|")).alias("LINEDATACOLUMNS"))
            df_final_data = dh.select(get(dh.LINEDATACOLUMNS, lit(0)).alias("PURCHASE_DATE"),
                                    get(dh.LINEDATACOLUMNS, lit(1)).alias("CHANNEL"),
                                    get(dh.LINEDATACOLUMNS, lit(2)).alias("PRODUCT"),
                                    get(dh.LINEDATACOLUMNS, lit(3)).alias("STATUS"),
                                    get(dh.LINEDATACOLUMNS, lit(4)).alias("CREATED_DATE"),
                                    get(dh.LINEDATACOLUMNS, lit(5)).alias("COMPLETED_DATE"),
                                    get(dh.LINEDATACOLUMNS, lit(6)).alias("SUBSCRIBER_KEY"),
                                    get(dh.LINEDATACOLUMNS, lit(7)).alias("POINTS"),
                                    get(dh.LINEDATACOLUMNS, lit(8)).alias("SHOW_RECORD"),
                                    get(dh.LINEDATACOLUMNS, lit(9)).alias("QUANTITY"),
                                    get(dh.LINEDATACOLUMNS, lit(10)).alias("INVOICE_TYPE"),
                                    get(dh.LINEDATACOLUMNS, lit(11)).alias("SELLER_NAME"),
                                    get(dh.LINEDATACOLUMNS, lit(12)).alias("PRODUCT_CATEGORY"),
                                    get(dh.LINEDATACOLUMNS, lit(13)).alias("WEBSITE_UNIQUE_ID"),
                                    get(dh.LINEDATACOLUMNS, lit(14)).alias("INVOICE_NUMBER"),
                                    get(dh.LINEDATACOLUMNS, lit(15)).alias("EPSILON_PRICE_PER_UNIT"),
                                    get(dh.LINEDATACOLUMNS, lit(16)).alias("EPSILON_AMOUNT"),
                                    get(dh.LINEDATACOLUMNS, lit(17)).alias("EPSILON_TOTAL_AMOUNT") 
               )
        #df_final_data = df_final_data.withColumn("PURCHASE_DATE",when(col(''PURCHASE_DATE'')==None,lit(None)).otherwise( regexp_replace(col("PURCHASE_DATE"), r"(\\d{2})-(\\d{2})-(\\d{2}) (\\d{2}:\\d{2}:\\d{2})",r"20\\3-\\2-\\1 \\4")))
        df_final_data = df_final_data.withColumn("PURCHASE_DATE",when(col(''PURCHASE_DATE'')==None,lit(None)).otherwise(col("PURCHASE_DATE")))
        df_final_data = df_final_data.withColumn("PURCHASE_DATE",when(col(''PURCHASE_DATE'')=="",lit(None)).otherwise(col("PURCHASE_DATE")))
        df_final_data = df_final_data.withColumn("CREATED_DATE",when(col(''CREATED_DATE'')=="",lit(None)).otherwise(col("CREATED_DATE")))
        df_final_data = df_final_data.withColumn("COMPLETED_DATE",when(col(''COMPLETED_DATE'')=="",lit(None)).otherwise(col("COMPLETED_DATE")))
        
        df_final_data = df_final_data.withColumn("QUANTITY",regexp_replace(col("QUANTITY"), r''[^0-9]'', ''''))
        df_final_data = df_final_data.withColumn("EPSILON_PRICE_PER_UNIT",regexp_replace(col("EPSILON_PRICE_PER_UNIT"), r''[^0-9]'', ''''))
        df_final_data = df_final_data.withColumn("EPSILON_AMOUNT",regexp_replace(col("EPSILON_AMOUNT"), r''[^0-9]'', ''''))

        df_final_data = df_final_data.withColumn("EPSILON_TOTAL_AMOUNT",regexp_replace(col("EPSILON_TOTAL_AMOUNT"), r''[^0-9]'', ''''))
        df_final_data = df_final_data.withColumn("QUANTITY",when(col(''QUANTITY'')=='''',lit(None)).otherwise(col("QUANTITY")))
        df_final_data = df_final_data.withColumn("EPSILON_PRICE_PER_UNIT",when(col(''EPSILON_PRICE_PER_UNIT'')=='''',lit(None)).otherwise(col("EPSILON_PRICE_PER_UNIT")))
        df_final_data = df_final_data.withColumn("EPSILON_AMOUNT",when(col(''EPSILON_AMOUNT'')=='''',lit(None)).otherwise(col("EPSILON_AMOUNT")))
        df_final_data = df_final_data.withColumn("EPSILON_TOTAL_AMOUNT",when(col(''EPSILON_TOTAL_AMOUNT'')=='''',lit(None)).otherwise(col("EPSILON_TOTAL_AMOUNT")))

        
        df_final_data = df_final_data.withColumn("file_name", lit(file_name))        
        df_final_data = df_final_data.withColumn("crtd_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S"))) 
        df_final_data = df_final_data.rename({col(''QUANTITY''):''QTY'',col(''SELLER_NAME''):''SELLER_NM'',col(''INVOICE_NUMBER''):''INVOICE_NUM''})
        snow_df = df_final_data.select(
           "purchase_date",
           "channel",
           "product",
           "status",
           "created_date",
           "completed_date",
           "subscriber_key",
           "points",
           "show_record",
           "qty",
           "invoice_type",
           "seller_nm",
           "product_category",
           "website_unique_id",
           "invoice_num",
           "epsilon_price_per_unit",
           "epsilon_amount",
           "epsilon_total_amount",
            "file_name",
            "crtd_dttm"
            )

        snow_df.write.mode("append").saveAsTable(stage_name.split(".")[0] + "." + target_table)   
        file_name=file_name.split(".")[0]+"_"+ datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        #move to success
        snow_df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)  
        
        return ''Success''       
        
        
        #return dffiltered
    
    except KeyError as key_error:
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message
    
    except Exception as e:
        error_message = f"Error: {str(e)}"
        return error_message
            
    except Exception as e:
        error_message = f"Error: {str(e)}"
        return error_message        
';
