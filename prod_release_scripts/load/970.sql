CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.PHLSDL_RAW.PH_POS_RKS_ROSE_PHARMA_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '
# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them.

import snowflake.snowpark as snowpark
from snowflake import snowpark
from snowflake.snowpark.functions import col,lit,trim, substring,current_timestamp,to_timestamp,count
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField,FloatType
import pandas as pd
from datetime import datetime
import pytz

def main(session: snowpark.Session,Param): 

    try:

        #Param=[''RSPHARM_OTC_202207.xlsx'',''PHLSDL_RAW.DEV_LOAD_STAGE_ADLS'',''dev/rosepharma_pos/transaction/'',''PH_POS_RKS_ROSE_PHARMA_OFFTAKE'']

        # Extracting parameters from the input
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]


        # Define the schema for the DataFrame
        df_schema=StructType([
            StructField("BRANCH_CODE",StringType()),
            StructField("BRANCH_NAME",StringType()),
            StructField("MONTH",StringType()),
            StructField("SKU",StringType()),
            StructField("SKU_DESCRIPTION",StringType()),
            StructField("QTY",FloatType())

            ])


        # Read the CSV file into a DataFrame
    
        dataframe = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "\\u0001")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
        

        #---------------------------Transformation logic ------------------------------#

        # Handle empty rows
        dataframe= dataframe.na.drop("all")


        #dataframe=dataframe.filter(col("SKU").is_not_null()) 

        # dataframe=dataframe.filter(dataframe["BRANCH_CODE"].isNotNull() & dataframe["BRANCH_NAME"].isNotNull() & dataframe["MONTH"].isNotNull()\\
        #                           & dataframe["SKU"].isNotNull() & dataframe["SKU_DESCRIPTION"].isNotNull() & dataframe["QTY"].isNotNull())

        # Check for empty Dataframe
        if dataframe.count()==0:
            return "No Data in file"

        # Add filename and CRT_DTTM columns

        dataframe = dataframe.withColumn("CRT_DTTM",lit(to_timestamp(current_timestamp())))
        
        file_name_without_extension = file_name.split(".")[0]
        date_string= file_name_without_extension[32:36]
        #new_file_name = "RSPHARM_OTC"+" "+date_string+" "+ file_name_without_extension[-2:]+".xlsx"
        new_file_name = file_name_without_extension
        dataframe = dataframe.with_column("FILE_NAME",lit(new_file_name))
        #Print(new_file_name)
        #Print(date_string)
        


        # final_df=session.create_dataframe(result_df)

        filtered_df = dataframe.filter(dataframe[''FILE_NAME''] == new_file_name)
        result_df = filtered_df.groupBy([''BRANCH_CODE'', ''BRANCH_NAME'', ''MONTH'', ''SKU'', ''SKU_DESCRIPTION'', ''QTY'', ''CRT_DTTM'', ''FILE_NAME'']).agg(count(''*'').alias(''count''))

        final_df=result_df.select(''BRANCH_CODE'', ''BRANCH_NAME'', ''MONTH'', ''SKU'', ''SKU_DESCRIPTION'', ''QTY'', ''CRT_DTTM'', ''FILE_NAME'')

        # Load Data to the target table
        final_df.write.mode("append").saveAsTable(target_table)
        

        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")

        # write to success folder
    
        file_name=new_file_name.split(".")[0]+''_''+datetime.now().strftime("%Y%m%d%H%M%S")
        final_df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)
        
        
        return "Success"

    

    except KeyError as key_error:
        # Handle KeyError (missing columns) here
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message
        
    except Exception as e:
        # Handle exceptions here
        error_message = f"Error: {str(e)}"
        return error_message';
		
		
CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.PHLSDL_RAW.PH_POS_RKS_ROSE_PHARMA_CONSUMER_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '
# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them.

import snowflake.snowpark as snowpark
from snowflake import snowpark
from snowflake.snowpark.functions import col,lit,trim, substring,current_timestamp,to_timestamp,count
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField,FloatType
import pandas as pd
from datetime import datetime
import pytz

def main(session: snowpark.Session,Param): 

    try:

        #Param=[''RSPHARM_202207.xlsx'',''PHLSDL_RAW.DEV_LOAD_STAGE_ADLS'',''rosepharma_pos_consumer/transaction/'',''SDL_POS_RKS_ROSE_PHARMA_CONSUMER'']

        # Extracting parameters from the input
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]


        # Define the schema for the DataFrame
        df_schema=StructType([
            StructField("BRANCH_CODE",StringType()),
            StructField("BRANCH_NAME",StringType()),
            StructField("MONTH",StringType()),
            StructField("SKU",StringType()),
            StructField("SKU_DESCRIPTION",StringType()),
            StructField("QTY",FloatType())

            ])


        # Read the CSV file into a DataFrame
    
        dataframe = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "\\u0001")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
        

        #---------------------------Transformation logic ------------------------------#

        # Handle empty rows
        dataframe= dataframe.na.drop("all")


        #dataframe=dataframe.filter(col("SKU").is_not_null()) 

        # dataframe=dataframe.filter(dataframe["BRANCH_CODE"].isNotNull() & dataframe["BRANCH_NAME"].isNotNull() & dataframe["MONTH"].isNotNull()\\
        #                           & dataframe["SKU"].isNotNull() & dataframe["SKU_DESCRIPTION"].isNotNull() & dataframe["QTY"].isNotNull())

        # Check for empty Dataframe
        if dataframe.count()==0:
            return "No Data in file"

        # Add filename and CRT_DTTM columns

        dataframe = dataframe.withColumn("CRT_DTTM",lit(to_timestamp(current_timestamp())))
        
        file_name_without_extension = file_name.split(".")[0]
        date_string= file_name_without_extension[32:36]
        #new_file_name = "RSPHARM_OTC"+" "+date_string+" "+ file_name_without_extension[-2:]+".xlsx"
        new_file_name = file_name_without_extension
        dataframe = dataframe.with_column("FILE_NAME",lit(new_file_name))
        #Print(new_file_name)
        #Print(date_string)
        


        # final_df=session.create_dataframe(result_df)

        filtered_df = dataframe.filter(dataframe[''FILE_NAME''] == new_file_name)
        result_df = filtered_df.groupBy([''BRANCH_CODE'', ''BRANCH_NAME'', ''MONTH'', ''SKU'', ''SKU_DESCRIPTION'', ''QTY'', ''CRT_DTTM'', ''FILE_NAME'']).agg(count(''*'').alias(''count''))

        final_df=result_df.select(''BRANCH_CODE'', ''BRANCH_NAME'', ''MONTH'', ''SKU'', ''SKU_DESCRIPTION'', ''QTY'', ''CRT_DTTM'', ''FILE_NAME'')

        # Load Data to the target table
        final_df.write.mode("append").saveAsTable(target_table)
        

        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")

        # write to success folder
    
        file_name=new_file_name.split(".")[0]+''_''+datetime.now().strftime("%Y%m%d%H%M%S")
        final_df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)
        
        
        return "Success"

    

    except KeyError as key_error:
        # Handle KeyError (missing columns) here
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message
        
    except Exception as e:
        # Handle exceptions here
        error_message = f"Error: {str(e)}"
        return error_message';		
		
create or replace TABLE PROD_DNA_LOAD.PHLSDL_RAW.SDL_POS_RKS_ROSE_PHARMA (
	BRANCH_CODE VARCHAR(30),
	BRANCH_NAME VARCHAR(100),
	MONTH VARCHAR(10),
	SKU VARCHAR(100),
	SKU_DESCRIPTION VARCHAR(100),
	QTY NUMBER(20,0),
	CRTD_DTTM TIMESTAMP_NTZ(9),
	FILENAME VARCHAR(100)
);

create or replace TABLE PROD_DNA_LOAD.PHLSDL_RAW.SDL_POS_RKS_ROSE_PHARMA_CONSUMER (
	BRANCH_CODE VARCHAR(30),
	BRANCH_NAME VARCHAR(100),
	MONTH VARCHAR(10),
	SKU VARCHAR(100),
	SKU_DESCRIPTION VARCHAR(100),
	QTY NUMBER(20,0),
	CRTD_DTTM TIMESTAMP_NTZ(9),
	FILENAME VARCHAR(100)
);		
