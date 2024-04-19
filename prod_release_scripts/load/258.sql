CREATE OR REPLACE PROCEDURE PCFSDL_RAW.IRI_SCAN_SALES_PREPROCESSING("PARAM" ARRAY)
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

        #Param=[''All_J_J_Items_WE100324.csv'',''PCFSDL_RAW.DEV_LOAD_STAGE_ADLS'',''dev/IRI_Scan_Data/transaction/all_jj_item_we/'',''SDL_IRI_SCAN_SALES'']

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
            StructField("SCAN_SALES",FloatType()),
            StructField("SCAN_UNITS",FloatType()),
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
        result_df = filtered_df.groupBy([''iri_market'', ''wk_end_dt'', ''iri_prod_desc'', ''iri_ean'', ''scan_sales'', ''scan_units'', ''ac_nielsencode'', ''crtd_dttm'', ''FILE_NAME'']).agg(count(''*'').alias(''count''))

        final_df=result_df.select(''iri_market'', ''wk_end_dt'', ''iri_prod_desc'', ''iri_ean'', ''scan_sales'', ''scan_units'', ''ac_nielsencode'', ''crtd_dttm'', ''FILE_NAME'')

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


CREATE OR REPLACE PROCEDURE PCFSDL_RAW.PA_PHARMA_INV_API_PREPROCESSING("PARAM" ARRAY)
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
from snowflake.snowpark.functions import col,lit,trim, substring,current_timestamp,to_timestamp,date_format
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField,FloatType,DecimalType,DoubleType
import pandas as pd
from datetime import datetime
import pytz

def main(session: snowpark.Session,Param): 

    try:

        #Param=[''RE_2.0_API_Inventory_and_Sales_Report_2024-03-31.csv'',''PCFSDL_RAW.DEV_LOAD_STAGE_ADLS'',''dev/Pacific_reg_inv/transaction/API/'',''SDL_API_DSTR,SDL_au_dstr_api_header'']

        # Extracting parameters from the input
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3].split(",")[0]
        Header_table    = Param[3].split(",")[1]


        # Define the schema for the DataFrame
        df_schema=StructType([
            StructField("ARTICLE_ID",StringType()),
            StructField("ARTICLE_DESC",StringType()),
            StructField("PRODUCT_EAN",StringType()),
            StructField("SITE_ID",StringType()),
            StructField("SITE_DESC",StringType()),
            StructField("VENDOR",StringType()),
            StructField("VENDOR_DESC",StringType()),
            StructField("PRODUCT_SAP_ID",StringType()),
            StructField("COST_PRICE",StringType()),
            StructField("CROSS_SITE_STATUS",StringType()),
            StructField("MONTH_13",StringType()),
            StructField("MONTH_12",StringType()),
            StructField("MONTH_11",StringType()),
            StructField("MONTH_10",StringType()),
            StructField("MONTH_09",StringType()),
            StructField("MONTH_08",StringType()),
            StructField("MONTH_07",StringType()),
            StructField("MONTH_06",StringType()),
            StructField("MONTH_05",StringType()),
            StructField("MONTH_04",StringType()),
            StructField("MONTH_03",StringType()),
            StructField("MONTH_02",StringType()),
            StructField("MONTH_01",StringType()),
            StructField("NULL_COLOUMN",StringType()),
            StructField("MTH_TOTAL_INVOICED_QTY",StringType()),
            StructField("SOH_QTY",StringType()),
            StructField("DC_SOO_QTY",StringType()),
            StructField("SO_BACKORDER_QTY",StringType())
            ])


        # Read Date and Header for column names
    
        df_header = session.read\\
            .schema(df_schema)\\
            .option("skip_header",0)\\
            .option("field_delimiter", "\\u0001")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .option("skip_blank_lines", True)\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)\\
            .limit(2)

        # Read the CSV file into a DataFrame

        dataframe = session.read\\
            .schema(df_schema)\\
            .option("skip_header",2)\\
            .option("field_delimiter", "\\u0001")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .option("skip_blank_lines", True)\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

         #---------------------------Transformation logic ------------------------------#

        # Handle null values or empty rows
        dataframe= dataframe.na.drop("all")


        # Check for empty Dataframe
        if dataframe.count()==0:
            return "No Data in file"

        # Fetch value for inv_date column
        date=file_name.split("_")[-1].split(".")[0]

        inv_date=date

        columns = dataframe.columns

        # Add inv_date to the dataframe
        dataframe=dataframe.with_column("INVT_DT",lit(inv_date).cast("String"))

        new_columns = ["INVT_DT"] + [col for col in columns if col != "INVT_DT"]

        final_df=dataframe.select(new_columns)

        # Fetch Header from files
        df_pandas=df_header.to_pandas()
        
        df_header_1=df_pandas.iloc[0,:9]
        df_header_2=df_pandas.iloc[1,9:]

        # Concatenate the DataFrames
        data1 = df_header_1.values.flatten().tolist()
        data2 = df_header_2.values.flatten().tolist()

        combined_data = data1 + data2
        header_columns=df_header.columns

        result_df = pd.DataFrame([combined_data],columns=header_columns)
        df_trimmed = result_df.applymap(lambda x: x.strip() if isinstance(x, str) else x)

        header=session.create_dataframe(df_trimmed)
        

        # Load Data to the target table
        final_df.write.mode("overwrite").saveAsTable(target_table)

        # Load the header to header target table "SDL_AU_DSTR_sigma_HEADER"

        header.write.mode("overwrite").saveAsTable(Header_table)

        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        header_file="API_out_hdr"

        # write to success folder
    
        file_name_1=file_name.replace("_"," ")[::-1].split(".",1)
        file_name=file_name_1[1][::-1]+''_''+datetime.now().strftime("%Y%m%d%H%M%S")
        final_df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)

        # Write the header dataframe to temp folder under processed
        header.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/temp/"+formatted_year+"/"+formatted_month+"/"+header_file,file_format_type=''csv'',header=True,OVERWRITE=True)


        return "Success"

    
    except KeyError as key_error:
        # Handle KeyError (missing columns) here
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message
        
    except Exception as e:
        # Handle exceptions here
        error_message = f"Error: {str(e)}"
        return error_message
        ';

CREATE OR REPLACE PROCEDURE PCFSDL_RAW.PA_PHARMA_INV_CHS_PREPROCESSING("PARAM" ARRAY)
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
from snowflake.snowpark.functions import col,lit,trim, substring,current_timestamp,to_timestamp
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField,FloatType,DecimalType,DoubleType
import pandas as pd
from datetime import datetime
import pytz

def main(session: snowpark.Session,Param): 

    try:

        #Param=[''ManufacturersReport.csv'',''PCFSDL_RAW.DEV_LOAD_STAGE_ADLS'',''dev/Pacific_reg_inv/transaction/CHS/'',''SDL_CHS_DSTR,SDL_AU_DSTR_CHS_HEADER'']

        # Extracting parameters from the input
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3].split(",")[0]
        Header_table    = Param[3].split(",")[1]


        # Define the schema for the DataFrame
        df_schema=StructType([
            StructField("WAREHOUSE",StringType()),
            StructField("PRODUCT_CODE",StringType()),
            StructField("PRODUCT_DESC",StringType()),
            StructField("SUPPLIER_PRODUCT_CODE",StringType()),
            StructField("PRIMARY_GTIN",StringType()),
            StructField("ABC_CODE",StringType()),
            StructField("STATUS",StringType()),
            StructField("LAST_COST",StringType()),
            StructField("SOH_QTY",StringType()),
            StructField("SOH_AMT",StringType()),
            StructField("SOO_QTY",StringType()),
            StructField("SOO_AMT",StringType()),
            StructField("BACK_ORDER_QTY",StringType()),
            StructField("MONTH_01",StringType()),
            StructField("MONTH_02",StringType()),
            StructField("MONTH_03",StringType()),
            StructField("MONTH_04",StringType()),
            StructField("MONTH_05",StringType()),
            StructField("MONTH_06",StringType()),
            StructField("MONTH_07",StringType()),
            StructField("MONTH_08",StringType()),
            StructField("MONTH_09",StringType()),
            StructField("MONTH_10",StringType()),
            StructField("MONTH_11",StringType()),
            StructField("MONTH_12",StringType())
            ])


        # Read Date and Header for column names
    
        df_header = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "\\u0001")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .option("skip_blank_lines", True)\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)\\
            .limit(2)

        # Read the CSV file into a DataFrame

        dataframe = session.read\\
            .schema(df_schema)\\
            .option("skip_header",3)\\
            .option("field_delimiter", "\\u0001")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .option("skip_blank_lines", True)\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        #---------------------------Transformation logic ------------------------------#

        # Handle null values or empty rows
        dataframe= dataframe.na.drop("all")


        # Check for empty Dataframe
        if dataframe.count()==0:
            return "No Data in file"

        # Extract Header and Date from df_header dataframe

        first_row = df_header.first()
        inv_date=first_row[0]
        
        header= df_header.limit(2).subtract(df_header.limit(1))

        # Add inv_date to the Dataframe

        dataframe=dataframe.with_column("INV_DT",lit(inv_date).cast("String"))
        final_df=dataframe.select("INV_DT","WAREHOUSE","PRODUCT_CODE","PRODUCT_DESC","SUPPLIER_PRODUCT_CODE","PRIMARY_GTIN",\\
                                 "ABC_CODE","STATUS","LAST_COST","SOH_QTY","SOH_AMT","SOO_QTY","SOO_AMT","BACK_ORDER_QTY","MONTH_01",\\
                                 "MONTH_02","MONTH_03","MONTH_04","MONTH_05","MONTH_06","MONTH_07","MONTH_08","MONTH_09","MONTH_10","MONTH_11","MONTH_12")

        
        # Load Data to the target table
        final_df.write.mode("overwrite").saveAsTable(target_table)

        # Load the header to header target table "SDL_AU_DSTR_CHS_HEADER"

        header.write.mode("overwrite").saveAsTable(Header_table)

        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        header_file="CHS_out_hdr"

        # write to success folder
    
        file_name=file_name.replace("_"," ").split(".")[0]+''_''+datetime.now().strftime("%Y%m%d%H%M%S")
        final_df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)

        # Write the header dataframe to temp folder under processed
        header.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/temp/"+formatted_year+"/"+formatted_month+"/"+header_file,file_format_type=''csv'',header=True,OVERWRITE=True)

        

        return "Success"

    

    except KeyError as key_error:
        # Handle KeyError (missing columns) here
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message
        
    except Exception as e:
        # Handle exceptions here
        error_message = f"Error: {str(e)}"
        return error_message';


CREATE OR REPLACE PROCEDURE PCFSDL_RAW.PA_PHARMA_INV_SIGMA_PREPROCESSING("PARAM" ARRAY)
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
from snowflake.snowpark.functions import col,lit,trim, substring,current_timestamp,to_timestamp,date_format
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField,FloatType,DecimalType,DoubleType
import pandas as pd
from datetime import datetime
import pytz

def main(session: snowpark.Session,Param): 

    try:

        #Param=[''Replenishment_E3_Buyers_Report.csv'',''PCFSDL_RAW.DEV_LOAD_STAGE_ADLS'',''dev/Pacific_reg_inv/transaction/Sigma/'',''SDL_sigma_DSTR,SDL_AU_DSTR_sigma_HEADER'']

        # Extracting parameters from the input
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3].split(",")[0]
        Header_table    = Param[3].split(",")[1]


        # Define the schema for the DataFrame
        df_schema=StructType([
            StructField("WAREHOUSE_DESC",StringType()),
            StructField("PRODUCT_CODE",StringType()),
            StructField("PRODUCT_DESC",StringType()),
            StructField("TEMP",StringType()),
            StructField("SUPPLIER_PRODUCT_CODE",StringType()),
            StructField("EAN",StringType()),
            StructField("VOLUME_CLAS_CODE",StringType()),
            StructField("HANDLING_STATUS",StringType()),
            StructField("COST_PRICE",StringType()),
            StructField("SOH_QTY",StringType()),
            StructField("SOH_AMT",StringType()),
            StructField("STOCK_IN_TRANSIT_QTY",StringType()),
            StructField("STOCK_IN_TRANSIT_AMT",StringType()),
            StructField("RESTRICTED_STOCK_QTY",StringType()),
            StructField("RESTRICTED_STOCK_AMT",StringType()),
            StructField("SOO_QTY",StringType()),
            StructField("SOO_AMT",StringType()),
            StructField("BACK_ORDER_QTY",StringType()),
            StructField("BACK_ORDER_AMT",StringType()),
            StructField("MONTH_01",StringType()),
            StructField("MONTH_02",StringType()),
            StructField("MONTH_03",StringType()),
            StructField("MONTH_04",StringType()),
            StructField("MONTH_05",StringType()),
            StructField("MONTH_06",StringType()),
            StructField("MONTH_07",StringType()),
            StructField("MONTH_08",StringType()),
            StructField("MONTH_09",StringType()),
            StructField("MONTH_10",StringType()),
            StructField("MONTH_11",StringType()),
            StructField("MONTH_12",StringType()),
            StructField("MONTH_13",StringType()),
            StructField("MONTH_14",StringType()),
            StructField("MONTH_15",StringType()),
            StructField("MONTH_16",StringType())
            ])


        # Read Date and Header for column names
    
        df_header = session.read\\
            .schema(df_schema)\\
            .option("skip_header",3)\\
            .option("field_delimiter", "\\u0001")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .option("skip_blank_lines", True)\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)\\
            .limit(1)

        # Read the CSV file into a DataFrame

        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",20)\\
            .option("field_delimiter", "\\u0001")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .option("skip_blank_lines", True)\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        #---------------------------Transformation logic ------------------------------#

        # Handle null values or empty rows
        df= df.na.drop("all")


        # Check for empty Dataframe
        if df.count()==0:
            return "No Data in file"

        # Fetch value for inv_date column

        first_row = df_header.first()
        date=first_row[0]
        inv_date=date.split(" ")[-1]


        # Fetch header columns required for Header Table
        header_columns=df.limit(1)
      

        header = header_columns.withColumn(''MONTH_01'', date_format(col(''MONTH_01''), ''YYYY - MMMM''))\\
        .withColumn(''MONTH_02'', date_format(col(''MONTH_02''), ''YYYY - MMMM''))\\
        .withColumn(''MONTH_03'', date_format(col(''MONTH_03''), ''YYYY - MMMM''))\\
        .withColumn(''MONTH_04'', date_format(col(''MONTH_04''), ''YYYY - MMMM''))\\
        .withColumn(''MONTH_05'', date_format(col(''MONTH_05''), ''YYYY - MMMM''))\\
        .withColumn(''MONTH_06'', date_format(col(''MONTH_06''), ''YYYY - MMMM''))\\
        .withColumn(''MONTH_07'', date_format(col(''MONTH_07''), ''YYYY - MMMM''))\\
        .withColumn(''MONTH_08'', date_format(col(''MONTH_08''), ''YYYY - MMMM''))\\
        .withColumn(''MONTH_09'', date_format(col(''MONTH_09''), ''YYYY - MMMM''))\\
        .withColumn(''MONTH_10'', date_format(col(''MONTH_10''), ''YYYY - MMMM''))\\
        .withColumn(''MONTH_11'', date_format(col(''MONTH_11''), ''YYYY - MMMM''))\\
        .withColumn(''MONTH_12'', date_format(col(''MONTH_12''), ''YYYY - MMMM''))\\
        .withColumn(''MONTH_13'', date_format(col(''MONTH_13''), ''YYYY - MMMM''))\\
        .withColumn(''MONTH_14'', date_format(col(''MONTH_14''), ''YYYY - MMMM''))\\
        .withColumn(''MONTH_15'', date_format(col(''MONTH_15''), ''YYYY - MMMM''))\\
        .withColumn(''MONTH_16'', date_format(col(''MONTH_16''), ''YYYY - MMMM''))
        


        # Filter Rows with values "Warehouse Desc" and  "product code" and "Product Desc"
        dataframe= df.filter((df["WAREHOUSE_DESC"] != "Warehouse Desc") & (df["PRODUCT_CODE"] != "product code") & (df["PRODUCT_DESC"] != "Product Desc"))

        # Add inv_date to the dataframe
        dataframe=dataframe.with_column("INV_DATE",lit(inv_date).cast("String"))

        columns = df.columns
        new_columns = ["INV_DATE"] + [col for col in columns if col != "INV_DATE"]

        final_df=dataframe.select(new_columns)

        # Load Data to the target table
        final_df.write.mode("overwrite").saveAsTable(target_table)

        # Load the header to header target table "SDL_AU_DSTR_sigma_HEADER"

        header.write.mode("overwrite").saveAsTable(Header_table)

        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        header_file="Sigma_out_hdr"

        # write to success folder
    
        file_name=file_name.replace("_"," ").split(".")[0]+''_''+datetime.now().strftime("%Y%m%d%H%M%S")
        final_df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)

        # Write the header dataframe to temp folder under processed
        header.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/temp/"+formatted_year+"/"+formatted_month+"/"+header_file,file_format_type=''csv'',header=True,OVERWRITE=True)


        return "Success"

    
    except KeyError as key_error:
        # Handle KeyError (missing columns) here
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message
        
    except Exception as e:
        # Handle exceptions here
        error_message = f"Error: {str(e)}"
        return error_message
        ';

CREATE OR REPLACE PROCEDURE PCFSDL_RAW.PA_PHARMA_INV_SYMBION_PREPROCESSING("PARAM" ARRAY)
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
from snowflake.snowpark.functions import col,lit
from snowflake.snowpark.types import StringType, StructType, StructField
import pandas as pd
from datetime import datetime
import pytz
 
def main(session: snowpark.Session,Param):
 
    try:
 
        #Param=[''Stock_Status_by_DC_-_13_Months_Sales.csv'',''PCFSDL_RAW.DEV_LOAD_STAGE_ADLS'',''dev/Pacific_reg_inv/transaction/Symbion'',''SDL_symbion_DSTR,SDL_AU_DSTR_symbion_HEADER'']
 
        # Extracting parameters from the input
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3].split(",")[0]
        Header_table    = Param[3].split(",")[1]
 
 
        # Define the schema for the DataFrame
        df_schema=StructType([
            StructField("STATE", StringType()),
            StructField("WAREHOUSE", StringType()),
            StructField("WAREHOUSE_DESC", StringType()),
            StructField("SYMBION_PRODUCT_NO", StringType()),
            StructField("SYMBION_PRODUCT_DESC", StringType()),
            StructField("SUPPLIER_PART_NO", StringType()),
            StructField("EAN", StringType()),
            StructField("GLOBAL_STD_COST", StringType()),
            StructField("DATE_INTRODUCED", StringType()),
            StructField("DATE_LAST_RECIVED", StringType()),
            StructField("OOSR", StringType()),
            StructField("SOH_AMT", StringType()),
            StructField("SOH_QTY", StringType()),
            StructField("ON_ORDER", StringType()),
            StructField("BACK_ORDER", StringType()),
            StructField("RESERVED_AMT_FOR_ORDER", StringType()),
            StructField("RESERVED_QTY_FOR_ORDER", StringType()),
            StructField("RESERVED_AMT_FOR_QA", StringType()),
            StructField("RESERVED_QTY_FOR_QA", StringType()),
            StructField("AVAILABLE_AMT", StringType()),
            StructField("AVAILABLE_QTY", StringType()),
            StructField("MTD", StringType()),
            StructField("MONTH_01", StringType()),
            StructField("MONTH_02", StringType()),
            StructField("MONTH_03", StringType()),
            StructField("MONTH_04", StringType()),
            StructField("MONTH_05", StringType()),
            StructField("MONTH_06", StringType()),
            StructField("MONTH_07", StringType()),
            StructField("MONTH_08", StringType()),
            StructField("MONTH_09", StringType()),
            StructField("MONTH_10", StringType()),
            StructField("MONTH_11", StringType()),
            StructField("MONTH_12", StringType()),
            StructField("MONTH_13", StringType())
            
            ])
 
 
        # Read the CSV file into a DataFrame
        df_header = session.read\\
            .schema(df_schema)\\
            .option("skip_header",3)\\
            .option("field_delimiter", "\\u0001")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .option("skip_blank_lines", True)\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)\\
            .limit(3)

        dataframe = session.read\\
            .schema(df_schema)\\
            .option("skip_header",7)\\
            .option("field_delimiter", "\\u0001")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .option("skip_blank_lines", True)\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)\\


    
 
        #---------------------------Transformation logic ------------------------------#
 
        #Handle null values or empty rows
        dataframe=dataframe.na.drop("all")
 
        # Check for empty Dataframe
        if dataframe.count()==0:
            return "No Data in file"

        # Extract the date from the header
        first_row = df_header.first()
        date = first_row[0]
        date = date.split(",")
        datePart = "".join(date[-2:]) 
        
        header= df_header.limit(3).subtract(df_header.limit(2))
        # Add INV_DT column with the concatenated date to the data DataFrame
        dataframe = dataframe.withColumn("INV_DT", lit(datePart).cast("String"))
  
    
        # Return the DataFrame with INV_DT column
        #Creating copy of the Dataframe
        final_df = dataframe.select("INV_DT","STATE","WAREHOUSE","WAREHOUSE_DESC","SYMBION_PRODUCT_NO","SYMBION_PRODUCT_DESC","SUPPLIER_PART_NO","EAN","GLOBAL_STD_COST","DATE_INTRODUCED","DATE_LAST_RECIVED","OOSR","SOH_AMT",\\
                                    "SOH_QTY","ON_ORDER","BACK_ORDER","RESERVED_AMT_FOR_ORDER","RESERVED_QTY_FOR_ORDER","RESERVED_AMT_FOR_QA","RESERVED_QTY_FOR_QA","AVAILABLE_AMT","AVAILABLE_QTY","MTD","MONTH_01","MONTH_02","MONTH_03",\\
                                    "MONTH_04","MONTH_05","MONTH_06","MONTH_07","MONTH_08","MONTH_09","MONTH_10","MONTH_11","MONTH_12","MONTH_13")

        # Load Data to the target table
        final_df.write.mode("overwrite").saveAsTable(target_table)

        # Load the header to header target table "SDL_AU_DSTR_CHS_HEADER"

        header.write.mode("overwrite").saveAsTable(Header_table)

        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        header_file="Symbion_out_hdr"

        # write to success folder
    
        file_name=file_name.replace("_"," ").split(".")[0]+''_''+datetime.now().strftime("%Y%m%d%H%M%S")
        final_df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)

        # Write the header dataframe to temp folder under processed
        header.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/temp/"+formatted_year+"/"+formatted_month+"/"+header_file,file_format_type=''csv'',header=True,OVERWRITE=True)

        

        return "Success"
 
    
 
    except KeyError as key_error:
        # Handle KeyError (missing columns) here
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message
    except Exception as e:
        # Handle exceptions here
        error_message = f"Error: {str(e)}"
        return error_message';

CREATE OR REPLACE PROCEDURE PCFSDL_RAW.PCF_CBG_DATA_PREPROCESSING("PARAM" ARRAY)
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
from snowflake.snowpark.functions import col,lit
from snowflake.snowpark.types import StringType, StructType, StructField
import pandas as pd
from datetime import datetime
import pytz
 
def main(session: snowpark.Session,Param):
 
    try:
 
        #Param=[''JJP_NEC_Markets_202403222024-03-26.csv'',''PCFSDL_RAW.DEV_LOAD_STAGE_ADLS'',''dev/CBG/transaction/Source'',''sdl_competitive_banner_group'']
 
        # Extracting parameters from the input
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
 
 
        # Define the schema for the DataFrame
        df_schema=StructType([
            StructField("market", StringType()),
            StructField("banner", StringType()),
            StructField("banner_classification", StringType()),
            StructField("manufacturer", StringType()),
            StructField("brand", StringType()),
            StructField("sku_name", StringType()),
            StructField("apn", StringType()),
            StructField("time_period", StringType()),
            StructField("unit", StringType()),
            StructField("dollar", StringType())
            
            ])
 
 
        # Read the CSV file into a DataFrame
        dataframe = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "\\t")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .option("skip_blank_lines", True)\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
 
        #---------------------------Transformation logic ------------------------------#
 
        # Handle null values or empty rows
        dataframe=dataframe.na.drop("all")
 
        # Check for empty Dataframe
        if dataframe.count()==0:
            return "No Data in file"


        #Creating copy of the Dataframe
        final_df = dataframe.select("market", "banner", "banner_classification", "manufacturer", "brand", "sku_name", "apn", "time_period", "unit", "dollar")

        # Load Data to the target table
        final_df.write.mode("overwrite").saveAsTable(target_table)
 
         
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
 
        # write to success folder
        file_name=file_name.split(".")[0]
        final_df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)
        return "Success"
        #return final_df
 
    
 
    except KeyError as key_error:
        # Handle KeyError (missing columns) here
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message
    except Exception as e:
        # Handle exceptions here
        error_message = f"Error: {str(e)}"
        return error_message';

CREATE OR REPLACE PROCEDURE PCFSDL_RAW.PCF_IG_INV_METCASH_PREPROCESSING("PARAM" ARRAY)
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
from snowflake.snowpark.functions import col,lit
from snowflake.snowpark.types import StringType, StructType, StructField
import pandas as pd
from datetime import datetime
import pytz
 
def main(session: snowpark.Session,Param):
 
    try:
 
        #Param=[''MT01P39R_20240127.csv'',''PCFSDL_RAW.DEV_LOAD_STAGE_ADLS'',''dev/Pacific_reg_inv/transaction/Metcash/Source'',''sdl_ig_inventory_data'']
 
        # Extracting parameters from the input
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
 
 
        # Define the schema for the DataFrame
        df_schema=StructType([
            StructField("state", StringType()),
            StructField("ware_house", StringType()),
            StructField("pe_item_no", StringType()),
            StructField("item_description", StringType()),
            StructField("stock_details_promo_ind", StringType()),
            StructField("stock_details_vendor", StringType()),
            StructField("stock_details_vendor_description", StringType()),
            StructField("stock_details_sub_vendor", StringType()),
            StructField("stock_details_lead_time", StringType()),
            StructField("stock_details_buyer_number", StringType()),
            StructField("stock_details_buyer_name", StringType()),
            StructField("stock_details_stock_control_email", StringType()),
            StructField("stock_details_soh", StringType()),
            StructField("stock_details_soo", StringType()),
            StructField("stock_details_awd", StringType()),
            StructField("stock_details_weeks_cover", StringType()),
            StructField("inbound_ordered_cases", StringType()),
            StructField("inbound_received_cases", StringType()),
            StructField("inbound_po_number", StringType()),
            StructField("item_details_item_sub_range_code", StringType()),
            StructField("item_details_item_sub_range_description", StringType()),
            StructField("item_details_pack_size", StringType()),
            StructField("item_details_buying_master_pack", StringType()),
            StructField("item_details_retail_unit", StringType()),
            StructField("item_details_buy_in_pallet", StringType()),
            StructField("item_details_ti", StringType()),
            StructField("item_details_hi", StringType()),
            StructField("item_details_pallet", StringType()),
            StructField("item_details_item_status", StringType()),
            StructField("item_details_delete_code", StringType()),
            StructField("item_details_deletion_date", StringType()),
            StructField("item_details_metcash_item_type", StringType()),
            StructField("item_details_ord_split_cat_code", StringType()),
            StructField("item_details_ndc_item", StringType()),
            StructField("item_details_imported_goods", StringType()),
            StructField("item_details_code_date", StringType()),
            StructField("item_details_packed_on_date", StringType()),
            StructField("item_details_incremental_days", StringType()),
            StructField("item_details_max_shelf_days", StringType()),
            StructField("item_details_receiving_limit", StringType()),
            StructField("item_details_dispatch_limit", StringType()),
            StructField("item_details_current_dd", StringType()),
            StructField("item_details_date_added_to_og", StringType()),
            StructField("item_details_consumer_gtin", StringType()),
            StructField("item_details_inner_gtin", StringType()),
            StructField("item_details_outer_gtin", StringType()),
            StructField("sales_week_6", StringType()),
            StructField("sales_week_5", StringType()),
            StructField("sales_week_4", StringType()),
            StructField("sales_week_3", StringType()),
            StructField("sales_week_2", StringType()),
            StructField("sales_week_1", StringType()),
            StructField("sales_this_week", StringType())
            
            ])
 
 
        # Read the CSV file into a DataFrame
        dataframe = session.read\\
            .schema(df_schema)\\
            .option("skip_header",0)\\
            .option("field_delimiter", "|")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .option("skip_blank_lines", True)\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
 
        #---------------------------Transformation logic ------------------------------#
 
        # Handle null values or empty rows
        dataframe=dataframe.na.drop("all")
 
        # Check for empty Dataframe
        if dataframe.count()==0:
            return "No Data in file"

        fileuploadeddate = file_name.split("_")[-1].split(".")[0]
        dataframe = dataframe.withColumn("INV_DT", (lit(fileuploadeddate)))
        dataframe = dataframe.with_column("CREATE_DT",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))


        #Creating copy of the Dataframe
        final_df = dataframe.select("INV_DT","state", "ware_house", "pe_item_no", "item_description", "stock_details_promo_ind", "stock_details_vendor", "stock_details_vendor_description", "stock_details_sub_vendor", "stock_details_lead_time", "stock_details_buyer_number", "stock_details_buyer_name", "stock_details_stock_control_email", "stock_details_soh", "stock_details_soo", "stock_details_awd", "stock_details_weeks_cover",\\
                                    "inbound_ordered_cases", "inbound_received_cases", "inbound_po_number", "item_details_item_sub_range_code", "item_details_item_sub_range_description", "item_details_pack_size", "item_details_buying_master_pack", "item_details_retail_unit", "item_details_buy_in_pallet", "item_details_ti", "item_details_hi", "item_details_pallet", "item_details_item_status", "item_details_delete_code",\\
                                    "item_details_deletion_date", "item_details_metcash_item_type", "item_details_ord_split_cat_code", "item_details_ndc_item", "item_details_imported_goods", "item_details_code_date", "item_details_packed_on_date", "item_details_incremental_days", "item_details_max_shelf_days", "item_details_receiving_limit", "item_details_dispatch_limit", "item_details_current_dd", "item_details_date_added_to_og",\\
                                    "item_details_consumer_gtin", "item_details_inner_gtin", "item_details_outer_gtin", "sales_week_6", "sales_week_5", "sales_week_4", "sales_week_3", "sales_week_2", "sales_week_1", "sales_this_week","CREATE_DT")

        # Load Data to the target table
        final_df.write.mode("overwrite").saveAsTable(target_table)
 
         
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
 
        # write to success folder
        file_name=file_name.split(".")[0]
        final_df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)
        return "Success"
        #return final_df
 
    
 
    except KeyError as key_error:
        # Handle KeyError (missing columns) here
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message
    except Exception as e:
        # Handle exceptions here
        error_message = f"Error: {str(e)}"
        return error_message';

CREATE OR REPLACE PROCEDURE PCFSDL_RAW.PERENSO_ACCOUNT_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS 'from snowflake.snowpark.functions import col, lit, date_format, current_timestamp, to_date, year, month, concat, format_number, regexp_replace, to_timestamp, when, trim, upper
from snowflake.snowpark.types import StringType, StructType, StructField
from snowflake.snowpark.files import SnowflakeFile
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz
import csv

def main(session: snowpark.Session,Param):
    # Param=["Account_20240415223028.csv","PCFSDL_RAW.DEV_LOAD_STAGE_ADLS","dev/ap_perenso/master/account","SDL_PERENSO_ACCOUNT"]
    try:
        file_name = Param[0]
        stage_name = Param[1]
        temp_stage_path = Param[2]
        target_table = Param[3] 

        full_path = "@"+stage_name+"/"+temp_stage_path+"/"+file_name
        with SnowflakeFile.open(full_path, ''rb'', require_scoped_url = False) as f:
            df1 = pd.read_csv(f)

        # print(df1)

        snowpark_df = session.create_dataframe(df1)

        # return snowpark_df

        df = snowpark_df.na.drop("all")

        if df.count() == 0:
            return "No Data in file"
            
        df = snowpark_df.withColumn("run_id", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        snowdf = df.withColumn("create_dt", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))


        file_name = file_name.split(".")[0] + "_" + datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        
        # Move to success
        snowdf.write.mode("append").saveAsTable(stage_name.split(".")[0] + "." + target_table)
        snowdf.write.copy_into_location("@" + stage_name + "/" + temp_stage_path + "/processed/success/" + formatted_year + "/" + formatted_month + "/" + file_name, header=True, OVERWRITE=True)
        
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

CREATE OR REPLACE PROCEDURE PCFSDL_RAW.PCF_PERENSO_ORDER_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
PACKAGES = ('openpyxl==3.0.10','snowflake-snowpark-python==*')
HANDLER = 'main'
EXECUTE AS OWNER
AS 'import snowflake.snowpark as snowpark
from snowflake.snowpark.types import StringType, StructType, StructField ,DecimalType
from snowflake.snowpark.functions import lit
from datetime import datetime
import pytz


def get_schema(file_name):
    df_schema=""
    select_col=""
    if file_name.startswith("OrderHdr"):
        df_schema=StructType([
        StructField("ORDER_KEY" , StringType()),
        StructField("ORDER_TYPE_KEY" , StringType()),
        StructField("ACCT_KEY" , StringType()),
        StructField("ORDER_DATE" , StringType()),
        StructField("STATUS" , StringType()),
        StructField("CHARGE" , StringType()),
        StructField("CONFIRMATION" , StringType()),
        StructField("DIARY_ITEM_KEY" , StringType()),
        StructField("WORK_ITEM_KEY" , StringType()),
        StructField("ACCOUNT_ORDER_NO" , StringType()),
        StructField("DELVRY_INSTNS" , StringType()),
            ])
        select_col=["ORDER_KEY","ORDER_TYPE_KEY","ACCT_KEY","ORDER_DATE","STATUS","CHARGE","CONFIRMATION","DIARY_ITEM_KEY","WORK_ITEM_KEY","ACCOUNT_ORDER_NO","DELVRY_INSTNS","RUN_ID","CREATE_DT"]
    if file_name.startswith("OrderType"):
        df_schema=StructType([
        StructField("ORDER_TYPE_KEY" , StringType()),
        StructField("ORDER_TYPE_DESC" , StringType()),
        StructField("SOURCE" , StringType())
            ])
        select_col=["ORDER_TYPE_KEY","ORDER_TYPE_DESC","SOURCE","run_id","create_dt"]
    if file_name.startswith("OrderBatch"):
        df_schema=StructType([
        StructField("ORDER_KEY" , StringType()),
        StructField("BATCH_KEY" , StringType()),
        StructField("BRANCH_KEY" , StringType()),
        StructField("DIST_ACCT" , StringType()),
        StructField("DELVRY_DT" , StringType()),
        StructField("STATUS" , StringType()),
        StructField("SUFFIX" , StringType()),
        StructField("SENT_DT" , StringType()),
            ])
        select_col=["ORDER_KEY","BATCH_KEY","BRANCH_KEY","DIST_ACCT","DELVRY_DT","STATUS","SUFFIX","SENT_DT","RUN_ID","CREATE_DT"]
    if file_name.startswith("OrderDetail"):
        df_schema=StructType([
         StructField("ORDER_KEY", StringType()),
        StructField("BATCH_KEY", StringType()),
        StructField("LINE_KEY", StringType()),
        StructField("PROD_KEY", StringType()),
        StructField("UNIT_QTY", StringType()),
        StructField("ENTERED_QTY", StringType()),
        StructField("ENTERED_UNIT_KEY", StringType()),
        StructField("LIST_PRICE", StringType()),
        StructField("NIS", StringType()),
        StructField("RRP", StringType()),
        StructField("DISC_KEY", StringType()),
        StructField("CREDIT_LINE_KEY", StringType()),
        StructField("CREDITED", StringType())
            ])
        select_col=["ORDER_KEY","BATCH_KEY","LINE_KEY","PROD_KEY","UNIT_QTY","ENTERED_QTY","ENTERED_UNIT_KEY","LIST_PRICE","NIS","RRP","DISC_KEY","CREDIT_LINE_KEY","CREDITED", "RUN_ID", "CREATE_DT"]
        
    if file_name.startswith("Constants"):
        df_schema=StructType([
            StructField("CONST_KEY", StringType()),
            StructField("CONST_DESC", StringType()),
            StructField("CONST_TYPE", StringType()),
            StructField("DSPORDER", StringType())
            ])
    
        select_col=["CONST_KEY", "CONST_DESC", "CONST_TYPE", "DSPORDER", "RUN_ID", "CREATE_DT"]
        
    if file_name.startswith("DealDiscount"):
        df_schema=StructType([
        StructField("DEAL_KEY", StringType()),
        StructField("DEAL_DESC", StringType()),
        StructField("START_DATE", StringType()),
        StructField("END_DATE", StringType()),
        StructField("SHORT_DESC", StringType()),
        StructField("DISC_KEY", StringType()),
        StructField("DISCOUNT_DESC", StringType())   
            ])
        select_col=["DEAL_KEY","DEAL_DESC","START_DATE","END_DATE","SHORT_DESC","DISC_KEY", "DISCOUNT_DESC", "RUN_ID", "CREATE_DT"]
    return df_schema,select_col
    

def main(session: snowpark.Session,Param):
    try:
        # Extracting parameters from the input

        file_name       = Param[0]     
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        db_name         = stage_name.split(".")[0]
        target_table    = db_name+"."+Param[3]
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")


        # Define the schema for the DataFrame
        df_schema,select_col=get_schema(file_name)
        

        df = session.read\\
        .schema(df_schema)\\
        .option("skip_header",1)\\
        .option("field_delimiter", ",")\\
        .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
        
        df=df.na.drop("all")
        if df.count()==0:
            return "No Data in file"

            
        df = df.withColumn("run_id",lit(lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S"))))
        df = df.withColumn("create_dt",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
            
        
                    
        final_df=df.select(select_col)
                        


        file_name=file_name+"_"+datetime.now().strftime("%Y%m%d%H%M%S")
        
        # write to target table
        final_df.write.mode("append").saveAsTable(target_table)
        
        #move to success
        final_df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)


        return "Success"

    except KeyError as key_error:
        # Handle KeyError (missing columns) here
        error_message = f"KeyError: {str(key_error)}"
        return error_message

    
    except Exception as e:
        # Handle exceptions here
        error_message = f"Error: {str(e)}"
        return error_message';

