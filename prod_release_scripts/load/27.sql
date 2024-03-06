CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.MYSSDL_RAW.JOINT_MONTHLY_INV_PREPROCESSING("PARAM" ARRAY)
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
        df = df.withColumn("crtd_dttm", lit(datetime.now().strftime("%Y-%m-%d %H:%M:%S")))

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

        snowdf= snowdf.filter(snowdf["DATE"].isNotNull())
        
        
        if snowdf.count()==0 :
            return "No Data in table"

        
        file_name=("_").join(file_name.split("_")[0:5])+"_"+datetime.now().strftime("%Y%m%d%H%M%S")
        snowdf.write.copy_into_location("@"+Param[1]+"/"+Param[2]+"/success/"+file_name,header=True,OVERWRITE=True)
        

        #write on sdl layer
    
        snowdf.write.mode("append").saveAsTable(stage_name.split(".")[0]+"."+target_table)
        
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
