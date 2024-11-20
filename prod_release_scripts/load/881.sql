create or replace TABLE PROD_DNA_LOAD.PHLSDL_RAW.SDL_HCP_ACOMMERCE_LOAD (
	PREFIX VARCHAR(50),
	CLIENT VARCHAR(50),
	ORDER_DATE TIMESTAMP_NTZ(9),
	MONTH_NO NUMBER(38,0),
	MONTH VARCHAR(50),
	DAY NUMBER(38,0),
	MARKETPLACE_ORDER_DATE TIMESTAMP_NTZ(9),
	MARKETPLACE_MONTH NUMBER(38,0),
	MARKETPLACE_DAY NUMBER(38,0),
	DELIVERED_DATE TIMESTAMP_NTZ(9),
	ORDER_ID VARCHAR(50),
	PARTNER_ORDER_ID VARCHAR(50),
	TERRITORY_MANAGER VARCHAR(50),
	CUSTOMER_EMAIL VARCHAR(100),
	DELIVERY_STATUS VARCHAR(50),
	ITEM_SKU VARCHAR(50),
	ACOMMERCE_ITEM_SKU VARCHAR(50),
	SUB_SALES_CHANNEL VARCHAR(50),
	PAYMENT_TYPE VARCHAR(50),
	PRODUCT_TITLE VARCHAR(100),
	BRAND VARCHAR(100),
	MAPPING VARCHAR(100),
	LTP NUMBER(30,5),
	JJ_SRP NUMBER(30,5),
	MARGIN NUMBER(30,5),
	TYPE VARCHAR(50),
	QUANTITY NUMBER(10,0),
	GMV NUMBER(30,5),
	COUNT_ORDER VARCHAR(50),
	ADDRESSEE VARCHAR(200),
	SHIPPING_PROVINCE VARCHAR(50),
	CRT_DTTM TIMESTAMP_NTZ(9),
	FILE_NAME VARCHAR(200)
);


CREATE PROCEDURE PROD_DNA_LOAD.PHLSDL_RAW.PH_HCE_ACOMMERCE_PREPROCESSING("PARAM" ARRAY)
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

        #Param=[''JNTL_HCP_MMM_RAW_MMDDYY.xlsx'',''PHLSDL_RAW.DEV_LOAD_STAGE_ADLS'',''dev/acommerce_hcp/'',''SDL_hcp_acommerce_load'']

        # Extracting parameters from the input
        file_name       = "Raw_Data.csv" ## Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]


        # Define the schema for the DataFrame
        df_schema=StructType([
            StructField("prefix",StringType()),
            StructField("client",StringType()),
            StructField("order_date",StringType()),
            StructField("month_no",StringType()),
            StructField("month",StringType()),
            StructField("day",StringType()),
            StructField("Marketplace_Order_Date",StringType()),
            StructField("Marketplace_Month",StringType()),
            StructField("Marketplace_Day",StringType()),
            StructField("delivered_date",StringType()),
            StructField("order_id",StringType()),
            StructField("partner_order_id",StringType()),
            StructField("territory_manager",StringType()),
            StructField("customer_email",StringType()),
            StructField("delivery_status",StringType()),
            StructField("item_sku",StringType()),
            StructField("acommerce_item_sku",StringType()),
            StructField("sub_sales_channel",StringType()),
            StructField("payment_type",StringType()),
            StructField("product_title",StringType()),
            StructField("Brand",StringType()),
            StructField("Mapping",StringType()),
            StructField("LTP",StringType()),
            StructField("JJ_SRP",StringType()),
            StructField("Margin",StringType()),
            StructField("type",StringType()),
            StructField("quantity",StringType()),
            StructField("GMV",StringType()),
            StructField("COUNT_ORDER",StringType()),
            StructField("addressee",StringType()),
            StructField("shipping_province",StringType())

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

        #dataframe=dataframe.filter(dataframe["order_id"].isNotNull() & dataframe["item_sku"].isNotNull() &dataframe["MONTH"].isNotNull()
    

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
        result_df = filtered_df.groupBy([''prefix'',''client'',''order_date'',''month_no'',''month'',''day'',''marketplace_order_date'',''Marketplace_Month'',''Marketplace_Day'',''delivered_date'',''order_id'',''partner_order_id'',''territory_manager'',''customer_email'',''delivery_status'',''item_sku'',''acommerce_item_sku'',''sub_sales_channel'',''payment_type'',''product_title'',''Brand'',''Mapping'',''LTP'',''jj_srp'',''Margin'',''type'',''quantity'',''GMV'',''count_order'',''addressee'',''shipping_province'', ''CRT_DTTM'',''FILE_NAME'']).agg(count(''*'').alias(''count''))

        final_df= result_df.select(''prefix'',''client'',''order_date'',''month_no'',''month'',''day'',''marketplace_order_date'',''Marketplace_Month'',''Marketplace_Day'',''delivered_date'',''order_id'',''partner_order_id'',''territory_manager'',''customer_email'',''delivery_status'',''item_sku'',''acommerce_item_sku'',''sub_sales_channel'',''payment_type'',''product_title'',''Brand'',''Mapping'',''LTP'',''jj_srp'',''Margin'',''type'',''quantity'',''GMV'',''count_order'',''addressee'',''shipping_province'',''CRT_DTTM'',''FILE_NAME'')
        
 

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
