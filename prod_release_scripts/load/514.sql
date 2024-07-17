update meta_raw.s3_to_adls set adls_path='coupang_premium/transaction/brandranking/'  where id in (416,417,418);
update meta_raw.s3_to_adls set s3_file='BrandRanking-weekly'  where id in (417);
update meta_raw.s3_to_adls set s3_file='BrandRanking-monthly'  where id in (418);
CREATE OR REPLACE PROCEDURE NTASDL_RAW.SDL_KR_SFMC_NAVER_DATA_ADDITIONAL_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS 'import pandas as pd
import sys
from pathlib import Path
import warnings
import os 
from snowflake.snowpark.files import SnowflakeFile
from snowflake.snowpark import Window
import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col, lit, date_format, current_timestamp, to_date,md5, year, month, concat, format_number, regexp_replace, to_timestamp, when, trim, upper
from snowflake.snowpark.types import StringType, StructType, StructField,DecimalType
import snowflake.snowpark
import pandas as pd
from datetime import datetime
import pytz

def main(session: snowpark.Session,Param):
    #Param=["PROD_Naver_KR_Lounge_Data_20240424.csv","NTASDL_RAW.DEV_LOAD_STAGE_ADLS_KOR","dev/SFMC/","sdl_kr_sfmc_naver_data_additional"]
    #data is getting loaded in 2 tables by splitting single file
    try:
        
        file_name = Param[0]
        stage_name = Param[1]
        temp_stage_path = Param[2]
        target_table = Param[3]
        
        null_value = "Null"
        lst_columns_attr = [''Naver_ID'',''Acquisition_Channel'',''Reason_for_Joining'',''Skin_type_Body'',''Baby_skin_concerns'',''Oral_health_concerns'',''Skin_concerns_Face'']
        primary_key = "Naver_ID"
        lst_attr_col = ["Naver_ID","Attribute_Name","Attribute_value"]
        attr_val = ''Attribute_value''
        lst_columns_master = [''Acquisition_Channel'',''Reason_for_Joining'',''Skin_type_Body'',''Baby_skin_concerns'',''Oral_health_concerns'',''Skin_concerns_Face'']
    
       # reading the CSV file
        #file_name="PROD_Naver_KR_Lounge_Data_20240424.csv"
        full_path = "@"+stage_name+"/"+temp_stage_path+"/"+file_name
        #full_path= "@DEV_LOAD_STAGE_ADLS_KOR/dev/SFMC/PROD_Naver_KR_Lounge_Data_20240424.csv"
        with SnowflakeFile.open(full_path,''rb'',require_scoped_url=False) as fhandle:
            
            raw_data = pd.read_csv(fhandle, encoding=''utf-16'', sep=''|'')
    
        # fill the null values with ''Null''
        raw_data = raw_data.fillna(null_value)
    
    
        # calling the required columns from main dataframe
        new_attr_df = raw_data[lst_columns_attr]
    
        # preparing data for stacking
        new_attr_df_indexed = new_attr_df.set_index(primary_key)
        new_attr_df_stacked = pd.DataFrame(new_attr_df_indexed.stack()).reset_index()
    
        # renaming columns for the new file
        new_attr_df_stacked.columns = lst_attr_col
    
        # removing the empty spaces
        new_attr_df_stacked[attr_val] = new_attr_df_stacked[attr_val].str.split('';'').map(lambda elements: [e.strip() for e in elements])
        new_attr_df_stacked[attr_val] = new_attr_df_stacked[attr_val].map(lambda elements: list(filter(None, elements)))
    
        # final dataframe
        df_transposed = new_attr_df_stacked.explode(attr_val).reset_index(drop=True)
    
        # removing null values
        df_transposed = df_transposed[df_transposed[attr_val] != null_value]
        df_transposed = df_transposed.dropna(subset=[attr_val]).reset_index(drop=True)
    
        
        file_name1= "PROD_Naver_KR_Lounge_Data_Additional_"+file_name.split("_")[5].split(".")[0]+".csv"
        df_transposed["File_name"]=file_name1
        df_transposed["crtd_dttm"]=datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")
        df_transposed.drop_duplicates(subset=["Naver_ID","Attribute_Name","Attribute_value","File_name"], keep=''last'')
        df_transposed=df_transposed.replace("Null",None)
    
        df1=session.create_dataframe(df_transposed)

        df1.write.mode("overwrite").save_as_table(stage_name.split(".")[0]+"."+target_table)
    
        
        df2 = raw_data.drop(lst_columns_master, axis=1)
        df2=df2.replace("Null",None)
        file_name2= "PROD_Naver_KR_Lounge_Data_Primary_"+file_name.split("_")[5].split(".")[0]+".csv"
        df2["File_name"]=file_name2
        df2["crtd_dttm"]=datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")
        
        df3=session.create_dataframe(df2)

        df3.write.mode("overwrite").save_as_table(stage_name.split(".")[0]+"."+"sdl_kr_sfmc_naver_data")

        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        
        #move to success
        
        file_name2= "PROD_Naver_KR_Lounge_Data_Primary_"+file_name.split("_")[5].split(".")[0] 
        df3.write.copy_into_location("@" + stage_name + "/" + temp_stage_path + "/processed/success/" + formatted_year + "/" + formatted_month + "/" + file_name2, header=True, OVERWRITE=True)
        file_name1= "PROD_Naver_KR_Lounge_Data_Additional_"+file_name.split("_")[5].split(".")[0]
        df3.write.copy_into_location("@" + stage_name + "/" + temp_stage_path + "/processed/success/" + formatted_year + "/" + formatted_month + "/" + file_name1, header=True, OVERWRITE=True)

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
