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
        snowdf = snowdf.filter(
                                ~(
                                    (snowdf["Month_No"].isNull()) &
                                    (snowdf["sales_date"].isNull()) &
                                    (snowdf["Warehouse_Code"].isNull()) &
                                    (snowdf["Customer_Code"].isNull()) &
                                    (snowdf["Customer_Name"].isNull()) &
                                    (snowdf["Invoice"].isNull()) &
                                    (snowdf["Item_Name"].isNull()) &
                                    (snowdf["Item_Code"].isNull()) &
                                    (snowdf["Type"].isNull()) &
                                    (snowdf["Sales_Value"].isNull()) &
                                    (snowdf["Sales_Units"].isNull()) &
                                    (snowdf["Bonus_Units"].isNull()) &
                                    (snowdf["Returns_Units"].isNull()) &
                                    (snowdf["Returns_Value"].isNull()) &
                                    (snowdf["Returns_Bonus_Units"].isNull()) &
                                    (snowdf["cdl_dttm"].isNull()) &
                                    (snowdf["crtd_dttm"].isNull()) &
                                    (snowdf["file_name"].isNull()) &
                                    (snowdf["run_id"].isNull())
                                )
                            )



        

        if snowdf.count()==0 :
            return "No Data in table"

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
