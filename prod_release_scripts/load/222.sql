CREATE OR REPLACE PROCEDURE THASDL_RAW.SDL_TH_TESCO_TRANSDATA_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS 'from snowflake.snowpark.functions import col, lit, date_format, current_timestamp, to_date, year, month, concat, format_number, regexp_replace, to_timestamp, when, trim,upper,substring, row_number
from snowflake.snowpark.types import StringType, StructType, StructField
import snowflake.snowpark
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz
import zipfile
from snowflake.snowpark.files import SnowflakeFile
from snowflake.snowpark import Window


def main(session: snowpark.Session,Param):
    try:
        file_name1 = Param[0]
        stage_name = Param[1]
        temp_stage_path = Param[2]
        target_table = Param[3]
        #dev/IQVIA/transaction/Weekly_Data/Outlet_20240323_20240328213034.zip
        #dev/IQVIA/transaction/Weekly_Data/Outlet_20240323_20240328213034.zip

        dfs=pd.DataFrame()
        full_path = "@"+stage_name+"/"+temp_stage_path+"/"+file_name1
        with SnowflakeFile.open(full_path,''rb'',require_scoped_url=False) as fhandle:
            with zipfile.ZipFile(fhandle,"r") as zip_ref:
                for file_name in zip_ref.namelist():
                    if file_name.endswith(''.xml''):
                        with zip_ref.open(file_name) as file:
                            dfh=pd.read_xml(file,xpath=''invrptth/ir_header'',parser=''etree'')
                        with zip_ref.open(file_name) as file:
                            df = pd.read_xml(file,xpath=''invrptth/ir_items/ir_item'',parser=''etree'',names =[''ITEM_NUMBER'',''EANSKU'',''ARTICLE_ID'',''SPN'',''ARTICLE_NAME'',''STOCK'',''SALES'',''SALES_AMOUNT''])
                            df=pd.DataFrame(df)
                            df[''CREATION_DATE'']=str(dfh.iloc[0,0])
                            df[''SUPPLIER_ID'']=str(dfh.iloc[0,1])
                            df[''SUPPLIER_NAME'']=str(dfh.iloc[0,2])
                            df[''WAREHOUSE'']=str(dfh.iloc[0,3])
                            df[''DELIVERY_POINT_NAME'']=str(dfh.iloc[0,4])
                            df[''IR_DATE'']=str(dfh.iloc[0,5])
                            df[''FILE_NAME'']=str(file_name).split("/")[1]
                            dfs=pd.concat([dfs,df])
        dfp=session.create_dataframe(dfs)
                            

        dfp = dfp.na.drop("all")
        if dfp.count() == 0:
            return "No Data in table"
            
        df = dfp.with_column("FOLDER_NAME", lit(file_name1.split(".")[0]))

        df_filtered = df.filter((col("IR_DATE").is_not_null()) & (col("IR_DATE").cast(StringType()) != ''''))

        windowSpec = Window.partitionBy("IR_DATE", "WAREHOUSE", "SUPPLIER_ID").orderBy(substring(col("FILE_NAME"), 8, 26).desc())
        df_with_rank = df_filtered.withColumn("RANK", row_number().over(windowSpec))

        df_rank_1_files = df_with_rank.filter(col("RANK") == 1).select("FILE_NAME").distinct()

        df_filtered = df_filtered.join(df_rank_1_files, "FILE_NAME", "inner")

        snowdf = df_filtered.select(
            "CREATION_DATE",
            "SUPPLIER_ID",
            "SUPPLIER_NAME",
            "WAREHOUSE",
            "DELIVERY_POINT_NAME",
            "IR_DATE",
            "EANSKU",
            "ARTICLE_ID", "SPN", "ARTICLE_NAME", "STOCK", "SALES", "SALES_AMOUNT",
            "FILE_NAME",
            "FOLDER_NAME"
        )
        
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")

        
        
        # Write operation
        snowdf.write.mode("append").save_as_table(stage_name.split(".")[0]+"."+target_table)
        snowdf.write.copy_into_location(
            "@"+stage_name+
            "/"+temp_stage_path+
            "/"+"processed/success/"+formatted_year+
            "/"+formatted_month+
            "/"+file_name1,
            header=True,
            OVERWRITE=True
        )
        
        # Success message
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
