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
             xmlget(col(''$1''), lit(''InventoryHeader'')).alias(''InventoryHeader'') \\
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
            ,flatten(col(''$1''),''$'') \\
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


CREATE OR REPLACE PROCEDURE THASDL_RAW.TH_GT_SCHEDULE_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS CALLER
AS '

import snowflake.snowpark as snowpark
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DateType
from snowflake.snowpark.functions import col,lit,date_format,current_timestamp,to_timestamp, to_date
import pandas as pd
from datetime import datetime
import pytz



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

        # Define the schema for the DataFrame
        df_schema=StructType([
            StructField("employeeid",StringType(50)),
            StructField("routeid",StringType(50)),
            StructField("date",StringType(20)),
            StructField("approved",StringType(10)),
            StructField("saleunit",StringType(50))
            ])
        # Set the current session schema
        
        # Read the CSV file into a DataFrame
        df = session.read\\
        .schema(df_schema)\\
        .option("skip_header",1)\\
        .option("field_delimiter", "|")\\
        .option("field_optionally_enclosed_by", "\\"") \\
        .option("truncatecolumns",True) \\
        .option("skip_blank_lines", True) \\
        .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        
        #convertin time stamp into sg timezone
        df = df.withColumn("CRT_DTTM", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        df = df.withColumn("FILE_NAME", lit(file_name).cast("string"))
        df = df.withColumn("RUN_ID", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        df = df.withColumn("CNTRY_CD", lit("TH"))
        df = df.withColumn("CRNCY_CD", lit("THB"))
        
        # Creating copy of the Dataframe
        final_df = df.select("CNTRY_CD", "CRNCY_CD", "employeeid", "routeid", to_date("date", lit("YYYYMMDD")).as_("date"), "approved", "saleunit", "FILE_NAME", "RUN_ID", "CRT_DTTM").alias("final_df")

        if final_df.count()==0:
            return "No Data in file"
        
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


CREATE OR REPLACE PROCEDURE THASDL_RAW.TH_GT_SALES_ORDER_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS CALLER
AS '

import snowflake.snowpark as snowpark
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DateType
from snowflake.snowpark.functions import col,lit,date_format,current_timestamp,to_timestamp, md5, coalesce, concat, upper, trim, to_date
import pandas as pd
from datetime import datetime
import pytz



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

        # Define the schema for the DataFrame
        df_schema=StructType([
            StructField("SaleUnit", StringType(50)),
            StructField("OrderID", StringType(50)),
            StructField("orderdate", StringType(50)),
            StructField("Customer_Id", StringType(50)),
            StructField("Customer_Name", StringType(100)),
            StructField("City", StringType(50)),
            StructField("Region", StringType(50)),
            StructField("SaleDistrict", StringType(50)),
            StructField("SaleOffice", StringType(50)),
            StructField("SaleGroup", StringType(50)),
            StructField("CustomerType", StringType(50)),
            StructField("StoreType", StringType(50)),
            StructField("SaleType", StringType(50)),
            StructField("SalesEmployee", StringType(50)),
            StructField("SaleName", StringType(100)),
            StructField("ProductID", StringType(50)),
            StructField("ProductName", StringType(100)),
            StructField("MegaBrand", StringType(50)),
            StructField("Brand", StringType(50)),
            StructField("BaseProduct", StringType(50)),
            StructField("Variant", StringType(50)),
            StructField("Putup", StringType(50)),
            StructField("PriceRef", StringType(50)),
            StructField("Backlog", StringType(50)),
            StructField("Qty", StringType(50)),
            StructField("SubAmt1", StringType(50)),
            StructField("Discount", StringType(50)),
            StructField("SubAmt2", StringType(50)),
            StructField("DiscountBTLine", StringType(50)),
            StructField("TotalBeforeVat", StringType(50)),
            StructField("Total", StringType(50)),
            StructField("No", StringType(50)),
            StructField("Canceled", StringType(50)),
            StructField("DocumentID", StringType(50)),
            StructField("RETURN_REASON", StringType(100)),
            StructField("PromotionCode", StringType(50)),
            StructField("PromotionCode1", StringType(50)),
            StructField("PromotionCode2", StringType(50)),
            StructField("PromotionCode3", StringType(50)),
            StructField("PromotionCode4", StringType(50)),
            StructField("PromotionCode5", StringType(50)),
            StructField("Promotion_Code", StringType(50)),
            StructField("Promotion_Code2", StringType(50)),
            StructField("Promotion_Code3", StringType(50)),
            StructField("AvgDiscount", StringType(50)),
            StructField("ORDERTYPE", StringType(10)),
            StructField("ApproverStatus", StringType(10)),
            StructField("PRICELEVEL", StringType(10)),
            StructField("OPTIONAL3", StringType(50)),
            StructField("DELIVERYDATE", StringType(50)),
            StructField("OrderTime", StringType(50)),
            StructField("SHIPTO", StringType(50)),
            StructField("BILLTO", StringType(50)),
            StructField("DeliveryRouteID", StringType(50)),
            StructField("APPROVED_DATE", StringType(50)),
            StructField("APPROVED_TIME", StringType(50)),
            StructField("REF_15", StringType(50)),
            StructField("PaymentType", StringType(50))
            ])
        # Set the current session schema
        
        # Read the CSV file into a DataFrame
        df = session.read\\
        .schema(df_schema)\\
        .option("skip_header",1)\\
        .option("field_delimiter", "|")\\
        .option("field_optionally_enclosed_by", "\\"") \\
        .option("truncatecolumns",True) \\
        .option("skip_blank_lines", True) \\
        .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        
        #convertin time stamp into sg timezone
        df = df.withColumn("CRT_DTTM", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        df = df.withColumn("FILE_NAME", lit(file_name).cast("string"))
        df = df.withColumn("RUN_ID", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        df = df.withColumn("CNTRY_CD", lit("TH"))
        df = df.withColumn("CRNCY_CD", lit("THB"))
        df = df.withColumn("order_date", to_date("orderdate", lit("YYYY/MM/DD")))
        df = df.withColumn("HASH_KEY", 
            md5(concat( \\
                    coalesce(upper(trim(col("SaleUnit"))), lit(''N/A'')) , \\
                    coalesce(upper(trim(col("OrderID"))), lit(''N/A'')) , \\
                    coalesce(upper(trim(col("order_date"))), lit(''9999-12-31'')), \\
                    coalesce(upper(trim(col("ProductID"))), lit(''N/A'')), \\
                    coalesce(upper(trim(col("Customer_Id"))), lit(''N/A'')), \\
                    coalesce(upper(trim(col("No"))), lit(''N/A'')) \\
                ) \\
            ) \\
        )

        # Creating copy of the Dataframe
        final_df = df.select("HASH_KEY", "CNTRY_CD", "CRNCY_CD", "SaleUnit", "OrderID", \\
            "order_date", "Customer_Id", \\
            "Customer_Name", "City", "Region", "SaleDistrict", "SaleOffice", "SaleGroup", "CustomerType", \\
            "StoreType", "SaleType", "SalesEmployee", "SaleName", "ProductID", "ProductName", "MegaBrand", \\
            "Brand", "BaseProduct", "Variant", "Putup", "PriceRef", "Backlog", "Qty", "SubAmt1", "Discount", \\
            "SubAmt2", "DiscountBTLine", "TotalBeforeVat", "Total", "No", "Canceled", "DocumentID", "RETURN_REASON", \\
            "PromotionCode", "PromotionCode1", "PromotionCode2", "PromotionCode3", "PromotionCode4", \\
            "PromotionCode5", "Promotion_Code", "Promotion_Code2", "Promotion_Code3", "AvgDiscount", "ORDERTYPE", \\
            "ApproverStatus", "PRICELEVEL", to_date("OPTIONAL3", lit("YYYYMMDD")).as_("OPTIONAL3"), \\
            to_date("DELIVERYDATE", lit("YYYYMMDD")).as_("DELIVERYDATE"), "OrderTime", "SHIPTO", "BILLTO", \\
            "DeliveryRouteID", to_date("APPROVED_DATE", lit("YYYYMMDD")).as_("APPROVED_DATE"), "APPROVED_TIME", \\
            "REF_15", "PaymentType", "FILE_NAME", "RUN_ID", "CRT_DTTM" ).alias("final_df")

        if final_df.count()==0:
            return "No Data in file"
        
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


CREATE OR REPLACE PROCEDURE THASDL_RAW.TH_GT_ROUTE_DTL_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS CALLER
AS '

import snowflake.snowpark as snowpark
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DateType
from snowflake.snowpark.functions import col,lit,date_format,current_timestamp,to_timestamp, md5, coalesce, concat, upper, trim, to_date, when
import pandas as pd
from datetime import datetime
import pytz



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

        # Define the schema for the DataFrame
        df_schema=StructType([
            StructField("routeid", StringType(50)),
            StructField("customerid", StringType(50)),
            StructField("routeNo", StringType(50)),
            StructField("saleunit", StringType(50)),
            StructField("SHIP_TO", StringType(50)),
            StructField("CONTACT_PERSON", StringType(100)),
            StructField("Created_date", StringType(50))
            ])
        # Set the current session schema
        
        # Read the CSV file into a DataFrame
        df = session.read\\
        .schema(df_schema)\\
        .option("skip_header",1)\\
        .option("field_delimiter", "|")\\
        .option("field_optionally_enclosed_by", "\\"") \\
        .option("truncatecolumns",True) \\
        .option("skip_blank_lines", True) \\
        .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        filespec,filecode,fileuploadeddate,filedate = file_name.split("_")
        
        #convertin time stamp into sg timezone
        df = df.withColumn("CRT_DTTM", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        df = df.withColumn("FILE_NAME", lit(file_name).cast("string"))
        df = df.withColumn("RUN_ID", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        df = df.withColumn("CNTRY_CD", lit("TH"))
        df = df.withColumn("CRNCY_CD", lit("THB"))
        df = df.withColumn("HASH_KEY", 
            md5(concat( \\
                    coalesce(upper(trim(col("saleunit"))), lit(''N/A'')) , \\
                    coalesce(upper(trim(col("customerid"))), lit(''N/A'')) , \\
                    coalesce(upper(trim(col("routeid"))), lit(''N/A'')), \\
                    coalesce(upper(trim(col("routeno"))), lit(''N/A'')) \\
                ) \\
            ) \\
        )
        
        df = df.withColumn("FILE_UPLOADED_DATE", to_timestamp(lit(fileuploadeddate),"YYYYMMDDHHMISS"))
              
        # Creating copy of the Dataframe
        file_df = df.select("HASH_KEY", "CNTRY_CD", "CRNCY_CD", "routeid", "customerid", \\
            "routeNo", "saleunit", "SHIP_TO", "CONTACT_PERSON", \\
             to_date("Created_date", lit("YYYYMMDD")).as_("Created_date"), \\
            "FILE_NAME", "FILE_UPLOADED_DATE", "RUN_ID", "CRT_DTTM" )
            
        final_df = file_df.select("HASH_KEY", "CNTRY_CD", "CRNCY_CD", "routeid", "customerid", \\
            "routeNo", "saleunit", trim("SHIP_TO").as_("SHIP_TO"), trim("CONTACT_PERSON").as_("CONTACT_PERSON"), \\
             "Created_date", "FILE_NAME", "FILE_UPLOADED_DATE", "RUN_ID", "CRT_DTTM" )

        if final_df.count()==0:
            return "No Data in file"
        
        # Load Data to the target table
        final_df.write.mode("append").saveAsTable(target_table)

    
        # write to success folder
    
        file_name=file_name.split(".")[0]+''_''+datetime.now().strftime("%Y%m%d%H%M%S")
        file_df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/success/"+file_name,file_format_type="csv",OVERWRITE=True,header=True)
   
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


CREATE OR REPLACE PROCEDURE THASDL_RAW.TH_GT_ROUTE_HDR_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS CALLER
AS '

import snowflake.snowpark as snowpark
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DateType
from snowflake.snowpark.functions import col,lit,date_format,current_timestamp,to_timestamp, md5, coalesce, concat, upper, trim, to_date
import pandas as pd
from datetime import datetime
import pytz



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

        # Define the schema for the DataFrame
        df_schema=StructType([
            StructField("id", StringType(50)),
            StructField("name", StringType(100)),
            StructField("desc", StringType(100)),
            StructField("is_active", StringType(10)),
            StructField("routesale", StringType(50)),
            StructField("saleunit", StringType(50)),
            StructField("route_code", StringType(50)),
            StructField("description", StringType(100)),
            StructField("Last_Updated_date", StringType(20))
            ])
        # Set the current session schema
        
        # Read the CSV file into a DataFrame
        df = session.read\\
        .schema(df_schema)\\
        .option("skip_header",1)\\
        .option("field_delimiter", "|")\\
        .option("field_optionally_enclosed_by", "\\"") \\
        .option("truncatecolumns",True) \\
        .option("skip_blank_lines", True) \\
        .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        filespec,filecode,uploadeddate,filedate = file_name.split("_")
        
        #convertin time stamp into sg timezone
        df = df.withColumn("CRT_DTTM", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        df = df.withColumn("FILE_NAME", lit(file_name).cast("string"))
        df = df.withColumn("RUN_ID", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        df = df.withColumn("CNTRY_CD", lit("TH"))
        df = df.withColumn("CRNCY_CD", lit("THB"))
        df = df.withColumn("HASH_KEY", 
            md5(concat( \\
                    coalesce(upper(trim(col("saleunit"))), lit(''N/A'')) , \\
                    coalesce(upper(trim(col("routesale"))), lit(''N/A'')) , \\
                    coalesce(upper(trim(col("id"))), lit(''N/A'')) \\
                ) \\
            ) \\
        )
 
        df = df.withColumn("FILE_UPLOADED_DATE", to_timestamp(lit(uploadeddate),"YYYYMMDDHHMISS"))
            
        
        # Creating copy of the Dataframe
        final_df = df.select("HASH_KEY", "CNTRY_CD", "CRNCY_CD", "id", "name", "desc", \\
            "is_active", "routesale", "saleunit", "route_code", "description", to_date("Last_Updated_date", lit("YYYYMMDD")).as_("Last_Updated_date"), \\
            "FILE_NAME", "FILE_UPLOADED_DATE", "RUN_ID", "CRT_DTTM" ).alias("final_df")

        if final_df.count()==0:
            return "No Data in file"
        
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


CREATE OR REPLACE PROCEDURE THASDL_RAW.TH_GT_MSLD_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS CALLER
AS '

import snowflake.snowpark as snowpark
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DateType
from snowflake.snowpark.functions import col,lit,date_format,current_timestamp,to_timestamp, split, trim, to_date
import pandas as pd
from datetime import datetime
import pytz



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

        # Define the schema for the DataFrame
        df_schema=StructType([
            StructField("DC", StringType(100)),
            StructField("RE_Name", StringType(100)),
            StructField("Store_Name", StringType(100)),
            StructField("Sales_Rep_Code", StringType(50)),
            StructField("Sales_Rep", StringType(100)),
            StructField("Category_Code", StringType(50)),
            StructField("Category", StringType(100)),
            StructField("Brand_Code", StringType(50)),
            StructField("Brand", StringType(100)),
            StructField("Barcode", StringType(50)),
            StructField("Product_Description", StringType(100)),
            StructField("Survey_Date", StringType(20)),
            StructField("NoDistribution", StringType(10)),
            StructField("OSA", StringType(10)),
            StructField("OOS", StringType(10)),
            StructField("OOSReason", StringType(10))
            ])
        # Set the current session schema
        
        # Read the CSV file into a DataFrame
        df = session.read\\
        .schema(df_schema)\\
        .option("skip_header",1)\\
        .option("field_delimiter", "|")\\
        .option("field_optionally_enclosed_by", "\\"") \\
        .option("truncatecolumns",True) \\
        .option("skip_blank_lines", True) \\
        .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        
        #convertin time stamp into sg timezone
        df = df.withColumn("CRT_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        df = df.withColumn("FILE_NAME",lit(file_name).cast("string"))
        df = df.withColumn("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        df = df.withColumn("CNTRY_CD",lit("TH"))
        df = df.withColumn("CRNCY_CD",lit("THB"))
        
        # Creating copy of the Dataframe
        final_df = df.select("CNTRY_CD", "CRNCY_CD", \\
            trim(split(col("DC"), lit("-"))[0].cast("string")).alias("DC_Code"), \\
            trim(split(col("DC"), lit("-"))[1].cast("string")).alias("DC_Name"), \\
            trim(split(col("RE_Name"), lit("-"))[0].cast("string")).alias("RE_Code"), \\
            trim(split(col("RE_Name"), lit("-"))[1].cast("string")).alias("RE_Name"), \\
            trim(split(col("Store_Name"), lit("-"))[0].cast("string")).alias("Store_Code"), \\
            trim(split(col("Store_Name"), lit("-"))[1].cast("string")).alias("Store_Name"), \\
            "Sales_Rep_Code", "Sales_Rep", "Category_Code", "Category", "Brand_Code", \\
            "Brand", "Barcode", "Product_Description", to_date("Survey_Date", lit("YYYYMMDD")).as_("Survey_Date"), \\
            "NoDistribution", "OSA", "OOS", "OOSReason", \\
            "FILE_NAME", "RUN_ID", "CRT_DTTM" ).alias("final_df")

        if final_df.count()==0:
            return "No Data in file"
        
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



CREATE OR REPLACE PROCEDURE DEV_DNA_LOAD.THASDL_RAW.MYM_SALES_STOCK_REPORT_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS 
$$
from snowflake.snowpark.functions import col, lit, date_format, current_timestamp, to_date, year, month, concat, format_number, regexp_replace,to_timestamp,when,trim,upper
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DecimalType,DateType
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz

def main(session: snowpark.Session,Param): 
    #Param = ["Reformatted_Customer_Sale_Report_03.02.2024.csv", "THASDL_RAW.DEV_LOAD_STAGE_ADLS", "dev/cert_data_lake/LCM/LCM_myanmar_Sales_Data", "SDL_MYM_GT_SALES_REPORT_FACT"]
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        sch_name        = stage_name.split('.')[0]
        target_table    = sch_name+"."+Param[3]
    
        df_schema = StructType([
                    StructField("item_no", StringType()),
                    StructField("description", StringType()),
                    StructField("qty_sold", StringType()),
                    StructField("stock", StringType())
                ])
        df = session.read\
            .schema(df_schema)\
            .option("skip_header",1)\
            .option("field_delimiter", "\u0001")\
            .option("field_optionally_enclosed_by", "\"") \
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        df = df.withColumn("FILE_NAME",lit(file_name).cast("string"))
        df = df.withColumn("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        df = df.withColumn("CRT_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
                
        final_df= df.select( "item_no", "description", "qty_sold", "stock", "FILE_NAME", "RUN_ID", "CRT_DTTM")
        
        final_dffinal_df= final_df.filter(final_df["item_no"].isNotNull())

        if final_df.count()==0 :
            return "No Data in table"
            
        
        #move file into success folder
        file_name=file_name.split(".")[0]+"_"+datetime.now().strftime("%Y%m%d%H%M%S")
        final_df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/success/"+file_name,header=True,OVERWRITE=True)

        #write on sdl layer
    
        final_df.write.mode("append").saveAsTable(target_table)
        
        return "Success"
    except KeyError as key_error:
        # Handle KeyError (missing columns) here
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message
        
    except Exception as e:
        # Handle exceptions here
        error_message = f"Error: {str(e)}" +str(df.columns)
        return error_message
        
$$;


CREATE OR REPLACE PROCEDURE TH_MT_PRICE_MULTISHEET_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python', 'pandas', 'openpyxl')
HANDLER = 'main'
EXECUTE AS OWNER
AS
$$

import snowflake.snowpark as snowpark
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DateType
from snowflake.snowpark.files import SnowflakeFile
from snowflake.snowpark.functions import lit
from datetime import datetime
import os,sys
import pandas as pd
from openpyxl import load_workbook
import pytz


def main(session: snowpark.Session, Param):

    # file_path, sheet, target_stage 
    # Param = [
    #     # 'TH_Action_Open_20230422_20230422170714.csv',
    #     'Customer_Sale_Report_28.06.2023.csv',
    #     'THASDL_RAW.DEV_LOAD_STAGE_ADLS',
    #     'dev/cert_data_lake/LCM/LCM_myanmar_Sales_Data',
    #     'Temp_Cust_sales'
    # ]
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        db_name         = stage_name.split('.')[0]
        target_table    = db_name+"."+Param[3]
    
        full_path = "@"+stage_name+"/"+temp_stage_path+"/"+file_name
        savefile  = "Reformatted_"+file_name

        df_schema=StructType([
            StructField("COMPANY", StringType()),
            StructField("DATE", StringType()),
            StructField("BRAND", StringType()),
            StructField("MANUFACTURER", StringType()),
            StructField("PRODUCT_NAME", StringType()),
            StructField("SKU_ID", StringType()),
            StructField("LIST_PRICE", StringType()),
            StructField("PRICE", StringType()),
            StructField("CATEGORY_JNJ", StringType()),
            StructField("SUB_CATEGORY_JNJ", StringType()),
            StructField("CATEGORY", StringType()),
            StructField("SUB_CATEGORY", StringType()),
            StructField("REVIEW_SCORE", StringType()),
            StructField("REVIEW_QTY", StringType()),
            StructField("DISCOUNT_DEPTH", StringType()),
            StructField("SOURCE", StringType())
            ])
        
        with SnowflakeFile.open(full_path, 'rb', require_scoped_url = False) as f:
            wb = load_workbook(f)
            sheet_names= wb.sheetnames
            sheet_names= sheet_names[1:-1]
    
            #creating empty list to append data
            stacked_data=[]
        
            #iterate over each sheet 
            for sheet_name in sheet_names:
                
                #skipping the first two rows
                df =pd.read_excel(f, engine='openpyxl' ,
                                       sheet_name=sheet_name, skiprows=2)
                
                #adding source column with respective sheet name as value
                df["Source"]= sheet_name
                #append the processed dataframe to the list
                stacked_data.append(df)
                                   
            #concatnate the stacked data into a single dataframe
            stacked_df= pd.concat(stacked_data, axis=0, ignore_index=True)
            stacked_df.rename( columns={'Unnamed: 0':'company'}, inplace=True )

            for column in stacked_df.columns:
                stacked_df[column] = stacked_df[column].apply(str)
            
            final_df = session.create_dataframe(stacked_df, df_schema)
            final_df = final_df.withColumn("FILE_NAME",lit(file_name).cast("string"))
            final_df = final_df.withColumn("CRT_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
            final_df.write.mode("append").saveAsTable(target_table)
            final_df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/success/"+file_name,file_format_type="csv",OVERWRITE=True,header=True)
            
        return "Success"
        
    except Exception as e:
        # Handle exceptions here
        error_message = f"ERROR: {str(e)}"
        return error_message
$$;

CREATE OR REPLACE PROCEDURE MYM_CUST_SALES_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python', 'pandas', 'openpyxl')
HANDLER = 'main'
EXECUTE AS OWNER
AS
$$

import snowflake.snowpark as snowpark
from snowflake.snowpark.files import SnowflakeFile
from snowflake.snowpark.functions import lit
from datetime import datetime
import os,sys
import pandas as pd
import pytz
def main(session: snowpark.Session, Param):

    # file_path, sheet, target_stage 
    # Param = [
    #     # 'TH_Action_Open_20230422_20230422170714.csv',
    #     'Customer_Sale_Report_28.06.2023.csv',
    #     'THASDL_RAW.DEV_LOAD_STAGE_ADLS',
    #     'dev/cert_data_lake/LCM/LCM_myanmar_Sales_Data',
    #     'Temp_Cust_sales'
    # ]
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        sch_name        = stage_name.split('.')[0]
        target_table    = sch_name+"."+Param[3]
    
        full_path = "@"+stage_name+"/"+temp_stage_path+"/"+file_name
        savefile  = "Reformatted_"+file_name
        with SnowflakeFile.open(full_path, 'rb', require_scoped_url = False) as f:
            mym_sales = pd.read_excel(f)
            mym_period = mym_sales.iloc[0, 0]
            mym_cust_group = mym_sales.iloc[1, 0]
            mym_cols = mym_sales.iloc[2].values
            mym_sales = pd.DataFrame(data=mym_sales.iloc[3:, :].values, columns=mym_cols)
            mym_cust_list = mym_sales.loc[(~mym_sales['Item No.'].isnull()) & (mym_sales['Description'].isnull())]['Item No.'].values
            mym_cust_list = mym_cust_list[:-1]
            mym_cust_code = [x.split(":")[1] for x in mym_cust_list]
            mym_cust_name = [x.split(":")[2] for x in mym_cust_list]
            mym_sales = mym_sales.dropna()
            mym_sales['old_index'] = mym_sales.index
        
            increment_value = 0
            cust_code = []
            cust_name = []
            for row_num in range(len(mym_sales)):
                cur_index = mym_sales.iloc[row_num]['old_index']
        
                if row_num == 0:
                    running_index = cur_index
                    cust_code.append(mym_cust_code[increment_value])
                    cust_name.append(mym_cust_name[increment_value])
                else:
                    if cur_index > running_index:
                        increment_value += 1
                        running_index = cur_index
                        cust_code.append(mym_cust_code[increment_value])
                        cust_name.append(mym_cust_name[increment_value])
                    else:
                        cust_code.append(mym_cust_code[increment_value])
                        cust_name.append(mym_cust_name[increment_value])
                running_index += 1
            mym_sales['period'] = mym_period
            mym_sales['customer_group'] = mym_cust_group
            mym_sales['customer_code'] = cust_code
            mym_sales['customer_name'] = cust_name
            mym_sales = mym_sales.drop(columns=['old_index'])
            if 'FOC' not in mym_sales.columns:
                mym_sales.insert(loc=3, column='FOC', value=0)
            mym_sales = mym_sales.reset_index(drop=True)
            
            # ----- code below is not working despite saying successful ------------------------------- hence commented-------
            #with pd.ExcelWriter("/tmp/"+savefile, engine="openpyxl") as writer:
            #   mym_sales.to_excel(writer)
            #   putresult = session.file.put("/tmp/"+savefile, "@"+stage_name+"/"+temp_stage_path,auto_compress=False)
            # ----- non working code ends here -------------------------------------------------------------------------------
            final_df = session.create_dataframe(mym_sales)
            final_df = final_df.withColumn("FILE_NAME",lit(file_name).cast("string"))
            final_df = final_df.withColumn("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
            final_df = final_df.withColumn("CRT_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
            final_df.write.mode("append").saveAsTable(target_table)
            final_df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/success/"+file_name,file_format_type="csv",OVERWRITE=True,header=True)
        return "Success"
        
    except Exception as e:
        # Handle exceptions here
        error_message = f"ERROR: {str(e)}"
        return error_message
$$;