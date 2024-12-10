CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.PHLSDL_RAW.PH_GT_CUSTOMERS_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python','unidecode ==1.3.8')
HANDLER = 'main'
EXECUTE AS OWNER
AS '
from snowflake.snowpark.functions import lit
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
from unidecode import unidecode
import pytz
from snowflake.snowpark.files import SnowflakeFile

def main(session: snowpark.Session, Param):
    try:
        #Param = [''''OUT_CON_C_PH_20240905220000.csv'''',''''PHLSDL_RAW.PROD_LOAD_STAGE_ADLS'''',''''prd/DMS/Customer'''',''''SDL_PH_MDS_GT_Customer'''']
        file_name       = Param[0]
        stage_name      = Param[1]
        stage_path 		= Param[2]
        target_table    = Param[3]
       
        #read the file from path
        full_path = "@"+stage_name+"/"+stage_path+"/"+file_name
    
        #read the file
        with SnowflakeFile.open(full_path,''rb'',require_scoped_url= False) as f:
            df = pd.read_csv(f,
                             dtype={"LDT_SAP_ID" : "string",
                                    "DT_ID" : "string",
                                    "Country_Code" : "string",
                                    "Outlet_ID" : "string",
                                    "Outlet_Name" : "string",
                                    "Address_1" : "string",
                                    "Address_2" : "string",
                                    "Telephone" : "string",
                                    "FAX" : "string",
                                    "City" : "string",
                                    "PostCode" : "string",
                                    "Region" : "string",
                                    "Channel_Group" : "string",
                                    "Sub_Channel" : "string",
                                    "Sales_Route_ID" : "string",
                                    "Sales_Route_Name" : "string",
                                    "SaleGroup" : "string",
                                    "SalesRep_ID" : "string",
                                    "SaleRep_Name" : "string",
                                    "GPS_Lat" : "string",
                                    "GPS_Long" : "string",
                                    "Status" : "string",
                                    "District" : "string",
                                    "Province" : "string",
                                    "Sup_Code" : "string",
                                    "Sup_Name" : "string",
                                    "Store_Prioritization" : "string",
                                    })
    
        df = df.map(lambda x: str(x))
        df = df.map(lambda x : unidecode(x))
    
        #Replacing all nan with null
        df = df.map(lambda x: None if x == ''<NA>'' else x)
    
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
        snowdf.write.copy_into_location("@"+stage_name+"/"+stage_path+"/processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name, header=True,  OVERWRITE=True, format_type_options= formattypeoptions, SINGLE=True) 
        
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
