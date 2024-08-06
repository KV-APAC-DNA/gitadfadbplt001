CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.ASPSDL_RAW.ECOMMERCE_6PAI_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('openpyxl==3.0.10','snowflake-snowpark-python==*','xlrd==2.0.1')
HANDLER = 'main'
EXECUTE AS OWNER
AS '
# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them.

import snowflake.snowpark as snowpark
from snowflake import snowpark
from snowflake.snowpark.functions import col,lit,trim, substring,current_timestamp,to_timestamp,when,column
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField,FloatType
import pandas as pd
from datetime import datetime
from snowflake.snowpark.files import SnowflakeFile
import pytz

def main(session: snowpark.Session,Param): 
    # Your code goes here, inside the "main" handler.
    try:
        
        #Param=["6P_AI_Calculated_202311_20240102210038.xlsx","ASPSDL_RAW.DEV_LOAD_STAGE_ADLS","dev/Regional/6PAI_Edge/","sdl_ecommerce_6pai"]


        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        sch_name        = stage_name.split(".")[0]
        target_table    = sch_name+"."+Param[3]


        full_path = "@"+stage_name+"/"+temp_stage_path+"/"+file_name
        with SnowflakeFile.open(full_path, "rb", require_scoped_url = False) as f:
            file = pd.ExcelFile(f)
            SheetNames=file.sheet_names

        retail_header_names=["YEAR", "MONTH","CLUSTER", "MARKET", "KPI", "DETAIL", "PLAN","SCORE_WEIGHTED", "SCORE_NON_WEIGHTED", "GAP_VS_PM", "GAP_VS_P3M","GAP_VS_PLAN"]
        sos_header_names=["YEAR", "MONTH","CLUSTER", "MARKET", "KPI", "DETAIL", "PLAN", "FRANCHISE","SCORE_WEIGHTED", "SCORE_NON_WEIGHTED", "GAP_VS_PM", "GAP_VS_P3M","GAP_VS_PLAN"]


        if len(SheetNames)==1 and SheetNames[0]=="Retail Level":
            with SnowflakeFile.open(full_path, "rb", require_scoped_url = False) as f:
                retail_df=pd.read_excel(f,sheet_name="Retail Level",dtype=str,names=retail_header_names)

            dataframe_retail = session.create_dataframe(retail_df,schema=retail_header_names)

        elif len(SheetNames)>1 and (SheetNames[0]=="Retail Level" and (SheetNames[1]=="SOS Franchise" or SheetNames[1]=="SOS BMC")):
            with SnowflakeFile.open(full_path, "rb", require_scoped_url = False) as f:
                retail_df=pd.read_excel(f,sheet_name=0,dtype=str,names=retail_header_names)
                sos_df=pd.read_excel(f,sheet_name=1,dtype=str,names=sos_header_names)
                pd.set_option("display.max_columns", None)
                print(sos_df.head(20))


            dataframe_retail = session.create_dataframe(retail_df,schema=retail_header_names)
            dataframe_sos = session.create_dataframe(sos_df,schema=sos_header_names)

        
        # Fetching the current date
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        new_file_name=file_name.split(".")[0]

        # write to success folder

        # Calling the function based on Sheet

        if len(SheetNames)==1 and SheetNames[0]=="Retail Level":
            result_retail=retail_processing(dataframe_retail,SheetNames,file_name,target_table)

            result_retail.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+new_file_name,header=True,OVERWRITE=True)
            status="Retail Load Success"
            

        elif len(SheetNames)>1 and (SheetNames[0]=="Retail Level" and (SheetNames[1]=="SOS Franchise" or SheetNames[1]=="SOS BMC")):
            result_retail=retail_processing(dataframe_retail,SheetNames,file_name,target_table)
            result_sos=sos_processing(dataframe_sos,SheetNames,file_name,target_table)

            final_result=result_retail.union(result_sos)

            final_result.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+new_file_name,header=True,OVERWRITE=True)
            
            status="Retail and SOS Success"




        if len(SheetNames)==1 and status=="Retail Load Success":
            return "Success"

        elif len(SheetNames)>1 and status=="Retail and SOS Success":
            return "Success"
        


    except Exception as e:
            # Handle exceptions here
            error_message = f"FAILED: {str(e)}"
            return error_message



# Function for Retail sheet load
def retail_processing(dataframe_retail,SheetNames,file_name,target_table):
    
    # Handle null values or empty rows
                
    dataframe_retail= dataframe_retail.na.drop("all")
        
     # Check for empty Dataframe
    if dataframe_retail.count()==0:
        return "No Data in file"
        
    # Convert the values to float and handle null values
        
    dataframe_retail = dataframe_retail.with_column("Plan", when(col("Plan").isNull(), None).otherwise(col("Plan").cast(FloatType()) * 100))
    dataframe_retail = dataframe_retail.with_column("Score_Weighted", when(col("Score_Weighted").isNull(), None).otherwise(col("Score_Weighted").cast(FloatType()) * 100))
    dataframe_retail = dataframe_retail.with_column("Score_NON_Weighted", when(col("Score_NON_Weighted").isNull(), None).otherwise(col("Score_NON_Weighted").cast(FloatType()) * 100))
    dataframe_retail = dataframe_retail.with_column("Gap_vs_PM", when(col("Gap_vs_PM").isNull(), None).otherwise(col("Gap_vs_PM").cast(FloatType()) * 100))
    dataframe_retail = dataframe_retail.with_column("Gap_vs_P3M", when(col("Gap_vs_P3M").isNull(), None).otherwise(col("Gap_vs_P3M").cast(FloatType()) * 100))
    dataframe_retail = dataframe_retail.with_column("Gap_vs_Plan", when(col("Gap_vs_Plan").isNull(), None).otherwise(col("Gap_vs_Plan").cast(FloatType()) * 100))
        
    # Add Source, FILENAME and CRT_DTTM
    sheet_1=SheetNames[0]
                
    dataframe_retail = dataframe_retail.with_column("SOURCE",lit(sheet_1))
    dataframe_retail = dataframe_retail.with_column("FILENAME",lit(file_name))
    dataframe_retail = dataframe_retail.with_column("CRT_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
    dataframe_retail = dataframe_retail.with_column("FRANCHISE",lit(None))
        
    final_retail_df=dataframe_retail.select("SOURCE","YEAR", "MONTH","CLUSTER", "MARKET", "KPI", "DETAIL", "PLAN", "FRANCHISE","SCORE_WEIGHTED", "SCORE_NON_WEIGHTED", "GAP_VS_PM", "GAP_VS_P3M","GAP_VS_PLAN","FILENAME","CRT_DTTM")
        
    # Load Data to the target table
    final_retail_df.write.mode("append").saveAsTable(target_table)

    return final_retail_df


# Function for SOS sheet load
def sos_processing(dataframe_sos,SheetNames,file_name,target_table):
    
    # Handle null values or empty rows         
    dataframe_sos= dataframe_sos.na.drop("all")

    # Check for empty Dataframe
    if dataframe_sos.count()==0:
        return "No Data in file"


    # Convert the values to float and handle null values

    dataframe_sos = dataframe_sos.with_column("Plan", when(col("Plan").isNull(), None).otherwise(col("Plan").cast(FloatType()) * 100))
    dataframe_sos = dataframe_sos.with_column("Score_Weighted", when(col("Score_Weighted").isNull(), None).otherwise(col("Score_Weighted").cast(FloatType()) * 100))
    dataframe_sos = dataframe_sos.with_column("Score_NON_Weighted", when(col("Score_NON_Weighted").isNull(), None).otherwise(col("Score_NON_Weighted").cast(FloatType()) * 100))
    dataframe_sos = dataframe_sos.with_column("Gap_vs_PM", when(col("Gap_vs_PM").isNull(), None).otherwise(col("Gap_vs_PM").cast(FloatType()) * 100))
    dataframe_sos = dataframe_sos.with_column("Gap_vs_P3M", when(col("Gap_vs_P3M").isNull(), None).otherwise(col("Gap_vs_P3M").cast(FloatType()) * 100))
    dataframe_sos = dataframe_sos.with_column("Gap_vs_Plan", when(col("Gap_vs_Plan").isNull(), None).otherwise(col("Gap_vs_Plan").cast(FloatType()) * 100))


    # Add Source, FILENAME and CRT_DTTM
    sheet_2=SheetNames[1]
                
    dataframe_sos = dataframe_sos.with_column("SOURCE",lit(sheet_2))
    dataframe_sos = dataframe_sos.with_column("FILENAME",lit(file_name))
    dataframe_sos =  dataframe_sos.with_column("CRT_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))


    final_sos_df=dataframe_sos.select("SOURCE","YEAR", "MONTH","CLUSTER", "MARKET", "KPI", "DETAIL", "PLAN", "FRANCHISE","SCORE_WEIGHTED", "SCORE_NON_WEIGHTED", "GAP_VS_PM", "GAP_VS_P3M","GAP_VS_PLAN","FILENAME","CRT_DTTM")
                
        
    # Load Data to the target table
    final_sos_df.write.mode("append").saveAsTable(target_table)

    
    return final_sos_df


';



CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.ASPSDL_RAW.OKR_ALTERYX_DATA_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('openpyxl==3.1.2','snowflake-snowpark-python==*','xlrd==2.0.1')
HANDLER = 'main'
EXECUTE AS OWNER
AS '
from snowflake.snowpark.functions import col, lit, date_format, current_timestamp, to_date, year, month, concat, format_number, regexp_replace,to_timestamp,when,trim,upper
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DecimalType,DateType
from datetime import datetime
import pandas as pd
import snowflake.snowpark as snowpark
import pytz

def main(session: snowpark.Session,Param): 
    
    try:
        
        #Param=["OKR_Alteryx_20240730135416.csv","ASPSDL_RAW.DEV_LOAD_STAGE_ADLS","dev/OKR/transaction/Alteryx/","SDL_OKR_ALTERYX_AUTOMATION"]
        
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        sch_name        = stage_name.split(".")[0]
        target_table    = sch_name+"."+Param[3]

        df_schema=StructType([
                StructField("KPI",StringType()),
                StructField("DATATYPE",StringType()),
                StructField("CLUSTER",StringType()),
                StructField("MARKET",StringType()),
                StructField("SEGMENT",StringType()),
                StructField("BRAND",StringType()),
                StructField("YEARMONTH",StringType()),
                StructField("YEAR",StringType()),
                StructField("QUARTER",StringType()),
                StructField("ACTUALS",StringType()),
                StructField("TARGET",StringType()),
                StructField("TARGET_TYPE",StringType())
                ])
       
        # Read the CSV file into a DataFrame
    
        dataframe = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", ",")\\
            .option("encoding","UTF-8")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .option("skip_blank_lines", True)\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)


        #---------------------------Transformation logic ------------------------------#
        

        # Handle null values or empty rows
        dataframe=dataframe.na.drop("all")

        # Check for empty Dataframe
        if dataframe.count()==0:
             return "No Data in file"

        new_file_name=file_name.split(".")[0]+"_"+datetime.now().strftime("%Y%m%d%H%M%S") + ".csv"

        dataframe = dataframe.with_column("FILENAME",lit(new_file_name))
        dataframe =  dataframe.with_column("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        dataframe =  dataframe.with_column("CRT_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))

        # Creating Final Dataframe
        final_df = dataframe.alias(dataframe)

        # Load Data to the target table
        
        final_df.write.mode("append").saveAsTable(target_table)

        
        # write to success folder

        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")

    
        final_df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+new_file_name+".gz",header=True,OVERWRITE=True,file_format_name="CSV_FILE_FORMAT_COMMA",single=True)
        
        return "Success"

    
        
    except KeyError as key_error:
        
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message
        
    except Exception as e:
        
        error_message = f"Error: {str(e)}" +str(dataframe.columns)
        return error_message
';


CREATE OR REPLACE FILE FORMAT ASPSDL_RAW.CSV_FILE_FORMAT_SOH
	TYPE = csv
	FIELD_DELIMITER = '\u0001'
	SKIP_HEADER = 1
	FIELD_OPTIONALLY_ENCLOSED_BY = '\"'
	NULL_IF = ('')
	COMPRESSION = GZIP
	ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
	EMPTY_FIELD_AS_NULL = FALSE
	ENCODING = 'UTF-8'
;


CREATE OR REPLACE FILE FORMAT ASPSDL_RAW.CSV_FILE_FORMAT_COMMA
	TYPE = csv
	FIELD_OPTIONALLY_ENCLOSED_BY = '\"'
	NULL_IF = ('')
	COMPRESSION = GZIP
	ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
	EMPTY_FIELD_AS_NULL = FALSE
	ENCODING = 'UTF-8'
;

CREATE OR REPLACE FILE FORMAT ASPSDL_RAW.CSV_FILE_FORMAT_TAB
	TYPE = csv
	FIELD_DELIMITER = '|'
	SKIP_HEADER = 1
	FIELD_OPTIONALLY_ENCLOSED_BY = '\"'
	NULL_IF = ('')
	ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
	EMPTY_FIELD_AS_NULL = FALSE
	ENCODING = 'UTF-8'
;
