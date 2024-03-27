SET ENV ='DEV';

SET DB = $ENV||'_DNA_LOAD';

USE DATABASE identifier($DB);

USE SCHEMA THASDL_RAW;

CREATE OR REPLACE PROCEDURE THASDL_RAW.TH_MT_TOPS_7_11_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS CALLER
AS '

import snowflake.snowpark as snowpark
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DateType
from snowflake.snowpark.functions import col,lit,date_format,current_timestamp,to_timestamp, xmlget, flatten, get, when
import pandas as pd
from datetime import datetime
import pytz


def get_xml_element(
        column:str,
        element:str,
        datatype:str,
        with_alias:bool = True
):
    new_element = (
        get(
            xmlget(
                col(column),
                lit(element),
            ),
            lit(''$'')
        )
        .cast("string")
    )

    new_element = when(new_element=='''', None).otherwise(new_element).cast(datatype)

    # alias needs to be optional
    return (
        new_element.alias(element) if with_alias else new_element
    )
    


def main(session: snowpark.Session, Param): 
    
    try:
        # Extracting parameters from the input

        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        sch_name        = stage_name.split(''.'')[0]
        target_table    = sch_name+"."+Param[3]
        
        # Set the current session schema
        
        session.use_schema(sch_name)
        
        # Read the CSV file into a DataFrame
        df = session.read\\
        .option("STRIP_OUTER_ELEMENT",False)\\
        .xml("@"+stage_name+"/"+temp_stage_path+"/"+file_name) \\
        .select(
             xmlget(col(''''), lit(''InventoryHeader'')).alias(''InventoryHeader'') \\
            ,get_xml_element(''InventoryHeader'', ''PartnerCode'', ''string'') \\
            ,get_xml_element(''InventoryHeader'', ''PartnerName'', ''string'') \\
            ,get_xml_element(''InventoryHeader'', ''PartnerGLN'', ''string'') \\
            ,get_xml_element(''InventoryHeader'', ''PartnerInventoryLocation'', ''string'') \\
            ,get_xml_element(''InventoryHeader'', ''SupplierCodeWithCustomer'', ''string'') \\
            ,get_xml_element(''InventoryHeader'', ''SupplierName'', ''string'') \\
            ,get_xml_element(''InventoryHeader'', ''SupplierGLN'', ''string'') \\
            ,get_xml_element(''InventoryHeader'', ''MessageDate'', ''string'') \\
            ,get_xml_element(''InventoryHeader'', ''InventoryReportDate'', ''string'') \\
            ,get_xml_element(''InventoryHeader'', ''DateType'', ''string'') \\
            ,flatten(col(''''),'''') \\
        ) \\
        .select(
             "PartnerCode", "PartnerName", "PartnerGLN", "PartnerInventoryLocation" \\
            ,"SupplierCodeWithCustomer", "SupplierName", "SupplierGLN", "MessageDate" \\
            ,"InventoryReportDate", "DateType" \\
            ,xmlget(col(''value''), lit(''ItemDetail'')).alias(''ItemDetail'') \\
            ,xmlget(col(''value''), lit(''ItemQuantity'')).alias(''ItemQuantity'') \\
            ,xmlget(col(''value''), lit(''ItemPriceCondition'')).alias(''ItemPriceCondition'') \\
            
            ,get_xml_element(''ItemDetail'', ''LineItemNumber'', ''string'') \\
            ,get_xml_element(''ItemDetail'', ''MaterialNumber'', ''string'') \\
            ,get_xml_element(''ItemDetail'', ''EANItemCode'', ''string'') \\
            ,get_xml_element(''ItemDetail'', ''EANPackCode'', ''string'') \\
            ,get_xml_element(''ItemDetail'', ''CustomerItemCode'', ''string'') \\

            ,get_xml_element(''ItemQuantity'', ''InventoryLocation'', ''string'') \\
            ,get_xml_element(''ItemQuantity'', ''UnitOfMeasure'', ''string'') \\
            ,get_xml_element(''ItemQuantity'', ''QtyPerPack'', ''string'') \\
            ,get_xml_element(''ItemQuantity'', ''TotalQtyOnHand'', ''string'') \\
            ,get_xml_element(''ItemQuantity'', ''ActualOnHandStockQty'', ''string'') \\
            ,get_xml_element(''ItemQuantity'', ''QtyinTransit'', ''string'') \\
            ,get_xml_element(''ItemQuantity'', ''SalesQty'', ''string'') \\
            ,get_xml_element(''ItemQuantity'', ''ExpectedSalesQty'', ''string'') \\
            ,get_xml_element(''ItemQuantity'', ''ShortShippedQty'', ''string'') \\
            
            ,get_xml_element(''ItemPriceCondition'', ''ItemPriceType'', ''string'') \\
            ,get_xml_element(''ItemPriceCondition'', ''ItemPrice'', ''string'') \\
            ,get_xml_element(''ItemPriceCondition'', ''ItemPriceUnit'', ''string'') \\
            ,get_xml_element(''ItemPriceCondition'', ''PriceCurrency'', ''string'') \\
        ) \\
        .select(
            "PartnerCode", "PartnerName", "PartnerGLN", "PartnerInventoryLocation" \\
            , "SupplierCodeWithCustomer", "SupplierName", "SupplierGLN", "MessageDate" \\
            , "InventoryReportDate", "DateType", "LineItemNumber", "MaterialNumber", "EANItemCode" \\
            , "EANPackCode", "CustomerItemCode", "InventoryLocation", "UnitOfMeasure", "QtyPerPack" \\
            , "TotalQtyOnHand", "ActualOnHandStockQty", "QtyinTransit", "SalesQty", "ExpectedSalesQty" \\
            , "ShortShippedQty", "ItemPriceType", "ItemPrice", "ItemPriceUnit", "PriceCurrency" \\
        )
        
        # Add  "FILE_NAME", "RUN_ID", "CRT_DTTM" to the Dataframe

        
        #convertin time stamp into sg timezone
        df = df.withColumn("CRT_DTTM", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        df = df.withColumn("FILE_NAME", lit(file_name).cast("string"))
        df = df.withColumn("RUN_ID", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        
        # Creating copy of the Dataframe
        final_df = df.alias("final_df")

        if final_df.count()==0:
            return "No Data in file"

        final_df=final_df.filter(col("LineItemNumber").is_not_null())

        
        # Load Data to the target table
        final_df.write.mode("append").saveAsTable(target_table)

    
        # write to success folder
    
        file_name=file_name.split(".")[0]+''_''+datetime.now().strftime("%Y%m%d%H%M%S")
        final_df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/success/"+file_name,file_format_type="csv",OVERWRITE=True,header=True)
   
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

