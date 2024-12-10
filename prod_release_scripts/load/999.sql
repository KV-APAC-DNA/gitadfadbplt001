CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.PHLSDL_RAW.PH_POS_PRICELIST_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python','unidecode ==1.3.8')
HANDLER = 'main'
EXECUTE AS OWNER
AS '
import pandas as pd
from datetime import datetime
from unidecode import unidecode
import snowflake.snowpark as snowpark
from snowflake.snowpark.files import SnowflakeFile
from snowflake.snowpark.functions import col,lit
import pytz


def main(session: snowpark.Session, Param):
    try:
        #Param=["POS_Pricelist_202406.txt","PHLSDL_RAW.PROD_LOAD_STAGE_ADLS","prd/MDS_POS/PriceList","SDL_PH_MDS_POS_PRICELIST"]
    
        file_name       = Param[0]
        stage_name      = Param[1]
        stage_path      = Param[2]
        target_table    = Param[3]
        
        #read the file from path
        full_path = "@"+stage_name+"/"+stage_path+"/"+file_name
        usecols = ["Product_Name", "Product_Code", "Consumers_Barcode", "Shippers_Barcode", "DzPerCase", "ListPriceCase", "ListPriceDz", 
                   "ListPriceUnit", "SRP", "Legend"]
      
        with SnowflakeFile.open(full_path, ''rb'' ,require_scoped_url= False) as f:
            df = pd.read_csv(f,names=usecols, sep="\\t", skiprows = 10, encoding=''windows-1252'')
    
        df = df.map(lambda x: str(x))
        df = df.map(lambda x: x.strip())
    
    
        #Replacing all nan with null
        df = df.map(lambda x: None if x == ''nan'' else x)
    
       
        #Handling null values and empty rows
        snowdf = session.create_dataframe(df) 
        snowdf = snowdf.na.drop(''all'')
       
        #Checking if dataframe is having any data    
        if snowdf.count()==0:
            return "No Data in file"
       
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
    
         
        snowdf = snowdf.withColumn("file_name", lit(file_name))
        snowdf = snowdf.withColumn("crt_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))   
    
        
        #move to success
        snowdf.write.mode("append").saveAsTable(stage_name.split(".")[0] + "." + target_table)

        formattypeoptions = {''FIELD_OPTIONALLY_ENCLOSED_BY'' : ''\"''}
        snowdf.write.copy_into_location("@"+stage_name+"/"+stage_path+"/processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name, header=True, OVERWRITE=True, format_type_options= formattypeoptions, SINGLE=True) 
    
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
