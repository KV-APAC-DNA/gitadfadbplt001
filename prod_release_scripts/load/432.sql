UPDATE PROD_DNA_LOAD.META_RAW.PARAMETERS SET PARAMETER_VALUE='Market|Time|Product|EAN|Dollars_(000)|Units_(000)|numeric_distribution|weighted_distribution|store_count_where_scanned|ac_nielsencode' WHERE parameter_group_id=323 AND PARAMETER_NAME='val_file_header' and PARAMETER_ID=4303;

CREATE or REPLACE TABLE PROD_DNA_LOAD.PCFSDL_RAW.SDL_IRI_SCAN_SALES(
	IRI_MARKET VARCHAR(255),
	WK_END_DT VARCHAR(100),
	IRI_PROD_DESC VARCHAR(255),
	IRI_EAN VARCHAR(100),
	SCAN_SALES NUMBER(20,4),
	SCAN_UNITS NUMBER(20,4),
    	NUMERIC_DISTRIBUTION numeric(20,4),
    	WEIGHTED_DISTRIBUTION numeric(20,4),
   	STORE_COUNT_WHERE_SCANNED numeric(20,4),
	AC_NIELSENCODE VARCHAR(100),
	CRTD_DTTM TIMESTAMP_NTZ(9) DEFAULT CAST('CURRENT_TIMESTAMP()' AS TIMESTAMP_NTZ(9)),
	FILENAME VARCHAR(255)
);

CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.PCFSDL_RAW.IRI_SCAN_SALES_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '# The Snowpark package is required for Python Worksheets. 
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

        #Param=[''All_J_J_Items_WE100324.csv'',''PCFSDL_RAW.DEV_LOAD_STAGE_ADLS'',''dev/IRI_Scan_Data/transaction/all_jj_item_we/'',''SDL_IRI_SCAN_SALES_NEW'']

        # Extracting parameters from the input
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]


        # Define the schema for the DataFrame
        df_schema=StructType([
            StructField("IRI_MARKET",StringType()),
            StructField("WK_END_DT",StringType()),
            StructField("IRI_PROD_DESC",StringType()),
            StructField("IRI_EAN",StringType()),
            StructField("SCAN_SALES",StringType()),
            StructField("SCAN_UNITS",StringType()),
            StructField("NUMERIC_DISTRIBUTION",FloatType()),
            StructField("WEIGHTED_DISTRIBUTION",FloatType()),
            StructField("STORE_COUNT_WHERE_SCANNED",FloatType()),
            StructField("AC_NIELSENCODE",StringType())
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


        dataframe=dataframe.filter(col("IRI_MARKET").is_not_null()) or dataframe.filter(col("WK_END_DT").is_not_null()) or \\
        dataframe.filter(col("IRI_PROD_DESC").is_not_null()) or dataframe.filter(col("IRI_EAN").is_not_null()) or \\
        dataframe.filter(col("SCAN_SALES").is_not_null()) or dataframe.filter(col("SCAN_UNITS").is_not_null())

        # dataframe=dataframe.filter(dataframe["IRI_MARKET"].isNotNull() & dataframe["WK_END_DT"].isNotNull() & dataframe["IRI_PROD_DESC"].isNotNull()\\
        #                           & dataframe["IRI_EAN"].isNotNull() & dataframe["SCAN_SALES"].isNotNull() & dataframe["SCAN_UNITS"].isNotNull())

        # Check for empty Dataframe
        if dataframe.count()==0:
            return "No Data in file"

        # Add filename and CRT_DTTM columns

        dataframe = dataframe.withColumn("CRTD_DTTM",lit(to_timestamp(current_timestamp())))
        
        file_name_without_extension = file_name.split(".")[0]
        date_string= file_name_without_extension[16:]
        new_file_name = "All J_J Items WE"+date_string + ".xlsx"
        dataframe = dataframe.with_column("FILE_NAME",lit(new_file_name))


        # final_df=session.create_dataframe(result_df)

        filtered_df = dataframe.filter(dataframe[''FILE_NAME''] == new_file_name)
        result_df = filtered_df.groupBy([''iri_market'', ''wk_end_dt'', ''iri_prod_desc'', ''iri_ean'', ''scan_sales'', ''scan_units'', ''NUMERIC_DISTRIBUTION'',''WEIGHTED_DISTRIBUTION'',''STORE_COUNT_WHERE_SCANNED'',''ac_nielsencode'', ''crtd_dttm'', ''FILE_NAME'']).agg(count(''*'').alias(''count''))

        final_df=result_df.select(''iri_market'', ''wk_end_dt'', ''iri_prod_desc'', ''iri_ean'', ''scan_sales'', ''scan_units'',''NUMERIC_DISTRIBUTION'',''WEIGHTED_DISTRIBUTION'',''STORE_COUNT_WHERE_SCANNED'', ''ac_nielsencode'', ''crtd_dttm'', ''FILE_NAME'')

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
