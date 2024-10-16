

CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.PHLSDL_RAW.PH_POS_PRICELIST("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python','unidecode ==1.3.8')
HANDLER = 'main'
EXECUTE AS OWNER
AS '

import pandas as pd
from datetime import datetime
from unidecode import unidecode
import snowflake.snowpark as snowpark
from snowflake.snowpark.files import SnowflakeFile
from snowflake.snowpark.functions import col,lit
import pytz


def main(session: snowpark.Session, Param):
    try:
        #Param=["POS_Pricelist_202406.txt","PHLSDL_RAW.PROD_LOAD_STAGE_ADLS","prd/MDS_POS/PriceList","SDL_PH_MDS_POS_PRICELIST"]
    
        file_name       = Param[0]
        stage_name      = Param[1]
        stage_path      = Param[2]
        target_table    = Param[3]
        
        #read the file from path
        full_path = "@"+stage_name+"/"+stage_path+"/"+file_name
        usecols = ["Product_Name", "Product_Code", "Consumers_Barcode", "Shippers_Barcode", "DzPerCase", "ListPriceCase", "ListPriceDz", 
                   "ListPriceUnit", "SRP", "Legend"]
      
        with SnowflakeFile.open(full_path, ''rb'' ,require_scoped_url= False) as f:
            df = pd.read_csv(f,names=usecols, sep="\\t", skiprows = 10, encoding=''windows-1252'')
    
        df = df.map(lambda x: str(x))
        df = df.map(lambda x: x.strip())
    
    
        #Replacing all nan with null
        df = df.map(lambda x: None if x == ''nan'' else x)
    
       
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
                     
        snowdf.write.copy_into_location("@"+stage_name+"/"+stage_path+"/processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name, header=True,OVERWRITE=True) 
    
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


	
	

CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.PHLSDL_RAW.PH_POS_SM_GR_PO_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '
import snowflake.snowpark as snowpark
from snowflake import snowpark
from snowflake.snowpark.functions import col,lit,trim,substring,current_timestamp,to_timestamp,count
from snowflake.snowpark.types import StringType, StructType, StructField
import pandas as pd
from datetime import datetime

def main(session: snowpark.Session, Param): 

    try:

        #Param = [''SM_GR_202407.xlsx'',''PHLSDL_RAW.PROD_LOAD_STAGE_ADLS'',''prd/MDS_POS/PureGold_SMGoods/'',''SDL_PH_MDS_POS_SM_PO,SDL_PH_MDS_POS_SM_GR'', ''["PO","GR"]'']

        # Extracting parameters from the input
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table_1    = Param[3].split(",")[0]
        target_table_2  = Param[3].split(",")[1] 


        # Define the schema for the DataFrame
        df_schema_PO = StructType([
            StructField("DocumentType",StringType()),
            StructField("CompanyName",StringType()),
            StructField("PostDate",StringType()),
            StructField("PostTime",StringType()),
            StructField("VendorName",StringType()),
            StructField("VendorCode",StringType()),
            StructField("TermsDiscount",StringType()),
            StructField("SiteCode",StringType()),
            StructField("SiteName",StringType()),
            StructField("SiteAddress",StringType()),
            StructField("SiteTIN",StringType()),
            StructField("DocumentNumber",StringType()),
            StructField("DateEntry",StringType()),
            StructField("DateCancel",StringType()),
            StructField("DateReceipt",StringType()),
            StructField("ArticleCount",StringType()),
            StructField("DeliveryMessage",StringType()),
            StructField("LineNumber",StringType()),
            StructField("SKUMatCode",StringType()),
            StructField("Description",StringType()),
            StructField("Discount",StringType()),
            StructField("UPC",StringType()),
            StructField("BuyQty",StringType()),
            StructField("BuyCost",StringType()),
            StructField("BuyUM",StringType()),
            StructField("Package",StringType()),
            StructField("SellUM",StringType()),
            StructField("ppsitecode",StringType()),
            StructField("ppsitedesc",StringType()),
            StructField("ppqty",StringType()),
            StructField("ppunit",StringType()),
            StructField("TotalAmount",StringType()),
            StructField("Remarks",StringType())           
            ])


        df_schema_GR = StructType([
            StructField("buisnessName",StringType()),
            StructField("title",StringType()),
            StructField("date",StringType()),
            StructField("vendorCode",StringType()),
            StructField("vendorName",StringType()),
            StructField("receiptDate",StringType()),
            StructField("TermsandDiscount",StringType()),
            StructField("SiteCode",StringType()),
            StructField("SiteName",StringType()),
            StructField("ShipTo",StringType()),
            StructField("GRNumber",StringType()),
            StructField("PONumber",StringType()),
            StructField("CancelDate",StringType()),
            StructField("Totalarticles",StringType()),
            StructField("ArticleNumber",StringType()),
            StructField("ArticleDescription",StringType()),
            StructField("UPC",StringType()),
            StructField("UOM",StringType()),
            StructField("ReceivedQty",StringType()),
            StructField("ImportantRemarks",StringType())          
            ])
        
        PO_fullpath = "@"+stage_name+"/"+temp_stage_path+"/"+"PO.csv"
        GR_fullpath = "@"+stage_name+"/"+temp_stage_path+"/"+"GR.csv"
        
        # Read the XLSX file into a DataFrame    
        dataframe_PO = session.read\\
            .schema(df_schema_PO)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "\\u0001")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .csv(PO_fullpath)
        
 
        dataframe_GR = session.read\\
            .schema(df_schema_GR)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "\\u0001")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .csv(GR_fullpath)


        #---------------------------Transformation logic ------------------------------#

        # Handle empty rows
        df_PO = dataframe_PO.na.drop("all")        
        df_GR = dataframe_GR.na.drop("all")       
      
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        
        def by_PO_load(df_PO,file_name):
            df_PO = df_PO.withColumn("CRTD_DTTM",lit(to_timestamp(current_timestamp())))            
            df_PO = df_PO.with_column("FILE_NAME",lit(file_name))
           
            final_df_PO=df_PO.select("DocumentType","CompanyName","PostDate","PostTime","VendorName","VendorCode","TermsDiscount", "SiteCode","SiteName","SiteAddress","SiteTIN","DocumentNumber","DateEntry","DateCancel","DateReceipt","ArticleCount","DeliveryMessage","LineNumber","SKUMatCode","Description","Discount","UPC","BuyQty","BuyCost","BuyUM","Package","SellUM","ppsitecode","ppsitedesc","ppqty","ppunit","TotalAmount","Remarks","crtd_dttm", "FILE_NAME")

            # Load Data to the target table
            final_df_PO.write.mode("overwrite").saveAsTable(target_table_1)
            
            # write to success folder            
            file_name_PO=file_name.split(".")[0]+''_''+''_PO''+datetime.now().strftime("%Y%m%d%H%M%S")
            final_df_PO.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name_PO,header=True,OVERWRITE=True)
        
        def by_GR_load(df_GR,file_name):
            df_GR = df_GR.withColumn("CRTD_DTTM",lit(to_timestamp(current_timestamp())))
            df_GR = df_GR.with_column("FILE_NAME",lit(file_name))
            
            final_df_GR=df_GR.select("buisnessName","title","date","vendorCode","vendorName","receiptDate","TermsandDiscount", "SiteCode", "SiteName", "ShipTo","GRNumber","PONumber","CancelDate","Totalarticles","ArticleNumber","ArticleDescription","UPC","UOM", "ReceivedQty", "ImportantRemarks", "crtd_dttm", "FILE_NAME")

            # Load Data to the target table
            final_df_GR.write.mode("overwrite").saveAsTable(target_table_2)
            
            # write to success folder            
            file_name_GR=file_name.split(".")[0]+''_''+''_GR''+datetime.now().strftime("%Y%m%d%H%M%S")
            final_df_GR.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name_GR,header=True,OVERWRITE=True)      
       
       
        # Check for empty Dataframe
        if df_PO.count()==0 and df_GR.count()==0:
            return "No Data in file"
        
        elif df_PO.count()==0 and df_GR.count()!=0: 
            by_GR_load(df_GR,file_name)
            return "No Data in PO Sheet, GR sheet load Triggered"
            
        elif df_GR.count()==0 and df_PO.count()!=0:
            by_PO_load(df_PO,file_name)
            return "No Data in GR Sheet, PO sheet load Triggered"
            
        else:
            by_PO_load(df_PO,file_name)
            by_GR_load(df_GR,file_name)
        
        
        return "Success"

    

    except KeyError as key_error:
        # Handle KeyError (missing columns) here
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message
        
    except Exception as e:
        # Handle exceptions here
        error_message = f"Error: {str(e)}"
        return error_message';
		
		
CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.PHLSDL_RAW.PH_POS_MDS_SOUTH_STAR_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS 'from snowflake.snowpark.functions import col, lit, date_format, current_timestamp, to_date, year, month, concat, format_number, regexp_replace, to_timestamp, when, trim, upper
from snowflake.snowpark.types import StringType, StructType, StructField
import snowflake.snowpark
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz

def main(session: snowpark.Session,Param):
    #Param=["SS_202402.csv","PHLSDL_RAW.DEV_LOAD_STAGE_ADLS","dev/pos/TEST_PH_POS","SDL_PH_POS_RUSTANS"]
    try:
        file_name = Param[0]
        stage_name = Param[1]
        temp_stage_path = Param[2]
        target_table = Param[3]

        df_schema = StructType([
                    StructField("sku_code", StringType(), True),
                    StructField("sku_description", StringType(), True),
                    StructField("upc", StringType(), True),
                    StructField("supplier_product_code", StringType(), True),
                    StructField("store_code", StringType(), True),
                    StructField("store_description", StringType(), True),
                    StructField("units_sold_ty", StringType(), True),
                    StructField("net_sales_ty", StringType(), True)
                ])
        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",10)\\
            .option("field_delimiter", "\\u0001")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
        
        #df=df.na.replace({"":None})      
        df = df.na.drop("all")
        
        if df.count() == 0:
            return "No Data in file"

        yearmo = file_name.split("_")[1].split(".")[0]

        df = df.withColumn("yearmo", lit(yearmo))
        df = df.withColumn("filename", lit(file_name.split(".")[0]+".xlsx"))
        df = df.withColumn("crt_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        df = df.withColumn("run_id", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))

        snowdf = df.select(
                           "yearmo",
                            "sku_code",
                            "sku_description",
                            "upc",
                            "supplier_product_code",
                            "store_code",
                            "store_description",
                            "units_sold_ty",
                            "net_sales_ty",
                            "filename",
                            "run_id",
                            "crt_dttm"
                        )
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        
        #move to success
        
        file_name=file_name.split(".")[0]+"_"+datetime.now().strftime("%Y%m%d%H%M%S")
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
