CREATE OR REPLACE PROCEDURE MYSSDL_RAW.MY_SELLOUT_INV_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '

from snowflake.snowpark.functions import col, lit, date_format, current_timestamp, to_date, year, month, concat, format_number, regexp_replace,to_timestamp,when,trim,upper
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DecimalType,DateType
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz

def main(session:snowpark.Session, Param):
    
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]

        df_schema = StructType([
            StructField("DISTRIBUTOR_ID", StringType()),
            StructField("DATE", StringType()),
            StructField("DISTRIBUTOR_WH_ID", StringType()),
            StructField("SAP_MATERIAL_ID", StringType()),
            StructField("PRODUCT_CODE", StringType()),
            StructField("PRODUCT_EAN_CODE", StringType()),
            StructField("PRODUCT_DESCRIPTION", StringType()),
            StructField("QUANTITY_AVAILABLE", StringType()),
            StructField("UOM_AVAILABLE", StringType()),
            StructField("QUANTITY_ON_ORDER", StringType()),
            StructField("UOM_ON_ORDER", StringType()),
            StructField("QUANTITY_COMMITTED", StringType()),
            StructField("UOM_COMMITTED", StringType()),
            StructField("QUANTITY_AVAILABLE_IN_PIECES", StringType()),
            StructField("QUANTITY_ON_ORDER_IN_PIECES", StringType()),
            StructField("QUANTITY_COMMITTED_IN_PIECES", StringType()),
            StructField("UNIT_PRICE", StringType()),
            StructField("TOTAL_VALUE_AVAILABLE", StringType()),
            StructField("CUSTOM_FIELD_1", StringType()),
            StructField("CUSTOM_FIELD_2", StringType())
        ])

        df = session.read\\
        .schema(df_schema)\\
        .option("skip_header",0)\\
        .option("field_delimiter", ",")\\
        .option("encoding", "ISO-8859-15") \\
        .option("field_optionally_enclosed_by", "\\"") \\
        .option("null_if", "") \\
        .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        #df = df.with_column("run_id", lit(None).cast(DecimalType(14,0)))
        df = df.with_column("cdl_dttm", lit(datetime.now().strftime("%Y%m%d%H%M%S")))
        df=df.with_column("file_name",lit(file_name))
        df = df.withColumn("crtd_dttm", current_timestamp())

        snowdf=df.select(
                            "DISTRIBUTOR_ID",
                            "DATE",
                            "DISTRIBUTOR_WH_ID",
                            "SAP_MATERIAL_ID",
                            "PRODUCT_CODE",
                            "PRODUCT_EAN_CODE",
                            "PRODUCT_DESCRIPTION",
                            "QUANTITY_AVAILABLE",
                            "UOM_AVAILABLE",
                            "QUANTITY_ON_ORDER",
                            "UOM_ON_ORDER",
                            "QUANTITY_COMMITTED",
                            "UOM_COMMITTED",
                            "QUANTITY_AVAILABLE_IN_PIECES",
                            "QUANTITY_ON_ORDER_IN_PIECES",
                            "QUANTITY_COMMITTED_IN_PIECES",
                            "UNIT_PRICE",
                            "TOTAL_VALUE_AVAILABLE",
                            "CUSTOM_FIELD_1",
                            "CUSTOM_FIELD_2",
                            "FILE_NAME")
        
        snowdf= snowdf.filter(snowdf["DISTRIBUTOR_ID"].isNotNull())
        
        if snowdf.count()==0 :
            return "No Data in table"


        file_name1=("_").join(file_name.split("_")[0:5])+"_"+datetime.now().strftime("%Y%m%d%H%M%S")
        
        

        #write on sdl layer
    
        snowdf.write.mode("append").saveAsTable(stage_name.split(".")[0]+"."+target_table)
        snowdf.write.copy_into_location("@"+Param[1]+"/"+Param[2]+"/success/"+file_name1,header=True,OVERWRITE=True)
        
        return "Success"
    except KeyError as key_error:
        # Handle KeyError (missing columns) here
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message

    except pd.errors.MergeError as merge_error:
        # Handle DataFrame merging error
        error_message = f"DataFrame merging error: {str(merge_error)}"
        return error_message
    
    except Exception as e:
        # Handle exceptions here
        error_message = f"Error: {str(e)}"
        return error_message
';


CREATE OR REPLACE PROCEDURE MYSSDL_RAW.MY_JOINT_MONTHLY_INV_PREPROCESSING("PARAM" ARRAY)
RETURNS TABLE ()
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS 'from snowflake.snowpark.functions import col, lit, date_format, current_timestamp, to_date, year, month, concat, format_number, regexp_replace,to_timestamp,when,trim,upper
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DecimalType,DateType
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz

def main(session: snowpark.Session,Param):
    #Param=["MY_GT_Distributor_Integrated_Inv_202312.csv","MYSSDL_RAW.DEV_LOAD_STAGE_ADLS","dev/transaction/sellout/distributor_integrated","SDL_MY_MONTHLY_SELLOUT_STOCK_FACT"]
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]

        df_schema = StructType([
            StructField("Distributor_ID", StringType()),
            StructField("DATE", StringType()),
            StructField("Distributor_WH_ID", StringType()),
            StructField("SAP_Material_ID", StringType()),
            StructField("Product_Code", StringType()),
            StructField("Product_EAN_Code", StringType()),
            StructField("Product_Description", StringType()),
            StructField("Quantity_Available", StringType()),
            StructField("UOM_Available", StringType()),
            StructField("Quantity_ON_Order", StringType()),
            StructField("UOM_ON_Order", StringType()),
            StructField("Quantity_Committed", StringType()),
            StructField("UOM_Committed", StringType()),
            StructField("Quantity_Available_IN_Pieces", StringType()),
            StructField("Quantity_ON_Order_IN_Pieces", StringType()),
            StructField("Quantity_Committed_IN_Pieces", StringType()),
            StructField("Unit_Price", StringType()),
            StructField("Total_Value_Available", StringType()),
            StructField("Custom_Field_1", StringType()),
            StructField("Custom_Field_2", StringType())
        ])

        df = session.read\\
        .schema(df_schema)\\
        .option("skip_header",0)\\
        .option("field_delimiter", ",")\\
        .option("encoding", "ISO-8859-15") \\
        .option("field_optionally_enclosed_by", "\\"") \\
        .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)


        #df = df.with_column("run_id", lit(None).cast(DecimalType(14,0)))
        df = df.with_column("cdl_dttm", lit(datetime.now().strftime("%Y%m%d%H%M%S")))
        df=df.with_column("file_name",lit(file_name))
        df = df.withColumn("crtd_dttm", current_timestamp())

        snowdf=df.select("Distributor_ID",
                        "DATE",
                        "Distributor_WH_ID",
                        "SAP_Material_ID",
                        "Product_Code",
                        "Product_EAN_Code",
                        "Product_Description",
                        "Quantity_Available",
                        "UOM_Available",
                        "Quantity_ON_Order",
                        "UOM_ON_Order",
                        "Quantity_Committed",
                        "UOM_Committed",
                        "Quantity_Available_IN_Pieces",
                        "Quantity_ON_Order_IN_Pieces",
                        "Quantity_Committed_IN_Pieces",
                        "Unit_Price",
                        "Total_Value_Available",
                        "Custom_Field_1",
                        "Custom_Field_2",
                        "file_name",
                        "cdl_dttm",
                        "crtd_dttm")

        snowdf= snowdf.filter(snowdf["Product_Code"].isNotNull())
        
        
        if snowdf.count()==0 :
            return "No Data in table"

        
        file_name=("_").join(file_name.split("_")[0:5])+"_"+datetime.now().strftime("%Y%m%d%H%M%S")
        

        #write on sdl layer
    
        snowdf.write.mode("append").saveAsTable(stage_name.split(".")[0]+"."+target_table)
        snowdf.write.copy_into_location("@"+Param[1]+"/"+Param[2]+"/success/"+file_name,header=True,OVERWRITE=True)
        
        return "Success"
    except KeyError as key_error:
        # Handle KeyError (missing columns) here
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message

    except pd.errors.MergeError as merge_error:
        # Handle DataFrame merging error
        error_message = f"DataFrame merging error: {str(merge_error)}"
        return error_message
    
    except Exception as e:
        # Handle exceptions here
        error_message = f"Error: {str(e)}"
        return error_message
        ';

CREATE OR REPLACE PROCEDURE MYSSDL_RAW.JOINT_MONTHLY_SALES_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS 'from snowflake.snowpark.functions import col, lit, date_format, current_timestamp, to_date, year, month, concat, format_number, regexp_replace,to_timestamp,when,trim,upper
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DecimalType,DateType
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz

def main(session: snowpark.Session,Param):
    #Param=["MY_GT_Distributor_Integrated_Sales_202312.csv","MYSSDL_RAW.DEV_LOAD_STAGE_ADLS","dev/transaction/sellout/distributor_integrated","SDL_MY_MONTHLY_SELLOUT_SALES_FACT"]
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]

        df_schema = StructType([
            StructField("Distributor_ID", StringType()),
            StructField("Sales_Order_Number", StringType()),
            StructField("Sales_Order_Date", StringType()),
            StructField("TYPE", StringType()),
            StructField("Customer_Code", StringType()),
            StructField("Distributor_WH_ID", StringType()),
            StructField("SAP_Material_ID", StringType()),
            StructField("Product_Code", StringType()),
            StructField("Product_EAN_Code", StringType()),
            StructField("Product_Description", StringType()),
            StructField("Gross_Item_Price", StringType()),
            StructField("Quantity", StringType()),
            StructField("UOM", StringType()),
            StructField("Quantity_in_Pieces", StringType()),
            StructField("Quantity_After_Conversion", StringType()),
            StructField("Sub_Total_1", StringType()),
            StructField("Discount", StringType()),
            StructField("Sub_Total_2", StringType()),
            StructField("Bottom_Line_Discount", StringType()),
            StructField("Total_Amt_After_Tax", StringType()),
            StructField("Total_Amt_Before_Tax", StringType()),
            StructField("Sales_Employee", StringType()),
            StructField("Custom_Field_1", StringType()),
            StructField("Custom_Field_2", StringType()),
            StructField("Custom_Field_3", StringType())
        ])

        df = session.read\\
        .schema(df_schema)\\
        .option("skip_header",0)\\
        .option("field_delimiter", ",")\\
        .option("encoding", "ISO-8859-15") \\
        .option("field_optionally_enclosed_by", "\\"") \\
        .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
        
        df=df.na.drop("all")
        if df.count()==0 :
            return "No Data in table"

        #df = df.with_column("run_id", lit(None).cast(DecimalType(14,0)))
        df = df.with_column("cdl_dttm", lit(datetime.now().strftime("%Y%m%d%H%M%S")))
        df=df.with_column("file_name",lit(file_name))
        df = df.withColumn("crtd_dttm", current_timestamp())

        snowdf=df.select(
                            "Distributor_ID",
                            "Sales_Order_Number",
                            "Sales_Order_Date",
                            "TYPE",
                            "Customer_Code",
                            "Distributor_WH_ID",
                            "SAP_Material_ID",
                            "Product_Code",
                            "Product_EAN_Code",
                            "Product_Description",
                            "Gross_Item_Price",
                            "Quantity",
                            "UOM",
                            "Quantity_in_Pieces",
                            "Quantity_After_Conversion",
                            "Sub_Total_1",
                            "Discount",
                            "Sub_Total_2",
                            "Bottom_Line_Discount",
                            "Total_Amt_After_Tax",
                            "Total_Amt_Before_Tax",
                            "Sales_Employee",
                            "Custom_Field_1",
                            "Custom_Field_2",
                            "Custom_Field_3",
                            "file_name",
                            "cdl_dttm",
                            "crtd_dttm")
        
        #snowdf= snowdf.filter(snowdf["Product_Code"].isNotNull())
        
        #if snowdf.count()==0 :
            #return "No Data in table"


        file_name1=("_").join(file_name.split("_")[0:5])+"_"+datetime.now().strftime("%Y%m%d%H%M%S")
        
        

        #write on sdl layer
    
        snowdf.write.mode("append").saveAsTable(stage_name.split(".")[0]+"."+target_table)

        #need to truncate the mds table and load again

        del_sql = "DELETE FROM " + stage_name.split(".")[0]+"."+target_table+"_mds_sync where filename=''"+ file_name +"''"
        session.sql(del_sql).collect()
        
        snowdf.write.mode("append").saveAsTable(stage_name.split(".")[0]+"."+target_table+"mds_sync")
        snowdf.write.copy_into_location("@"+Param[1]+"/"+Param[2]+"/success/"+file_name1,header=True,OVERWRITE=True)
        
        return "Success"
    except KeyError as key_error:
        # Handle KeyError (missing columns) here
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message

    except pd.errors.MergeError as merge_error:
        # Handle DataFrame merging error
        error_message = f"DataFrame merging error: {str(merge_error)}"
        return error_message
    
    except Exception as e:
        # Handle exceptions here
        error_message = f"Error: {str(e)}"
        return error_message
        ';
