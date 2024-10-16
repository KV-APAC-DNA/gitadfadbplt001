INSERT INTO META_RAW.PROCESS VALUES (2267,2267,'ReverseSync_SDL_PH_MDS_POS_711',2267,1,2,FALSE,TRUE,537,5,1,NULL,'','','','','','','');
INSERT INTO META_RAW.PROCESS VALUES (2268,2268,'ReverseSync_SDL_PH_MDS_POS_DYNA',2268,1,2,FALSE,TRUE,537,5,1,NULL,'','','','','','','');
INSERT INTO META_RAW.PROCESS VALUES (2269,2269,'ReverseSync_SDL_PH_MDS_POS_PUREGOLD',2269,1,2,FALSE,TRUE,537,5,1,NULL,'','','','','','','');
INSERT INTO META_RAW.PROCESS VALUES (2270,2270,'ReverseSync_SDL_PH_MDS_POS_ROB',2270,1,2,FALSE,TRUE,537,5,1,NULL,'','','','','','','');
INSERT INTO META_RAW.PROCESS VALUES (2271,2271,'ReverseSync_SDL_PH_MDS_POS_SM_GR',2271,1,2,FALSE,TRUE,537,5,1,NULL,'','','','','','','');
INSERT INTO META_RAW.PROCESS VALUES (2272,2272,'ReverseSync_SDL_PH_MDS_POS_SM_PO',2272,1,2,FALSE,TRUE,537,5,1,NULL,'','','','','','','');
INSERT INTO META_RAW.PROCESS VALUES (2273,2273,'ReverseSync_SDL_PH_MDS_POS_SOUTH_STAR',2273,1,2,FALSE,TRUE,537,5,1,NULL,'','','','','','','');
INSERT INTO META_RAW.PROCESS VALUES (2274,2274,'ReverseSync_SDL_PH_MDS_POS_WATSONS',2274,1,2,FALSE,TRUE,537,5,1,NULL,'','','','','','','');
INSERT INTO META_RAW.PROCESS VALUES (2275,2275,'ReverseSync_SDL_PH_MDS_POS_WALTERMART',2275,1,2,FALSE,TRUE,537,5,1,NULL,'','','','','','','');
INSERT INTO META_RAW.PROCESS VALUES (2276,2276,'ReverseSync_SDL_PH_MDS_POS_PRICELIST',2276,1,2,FALSE,TRUE,538,5,1,NULL,'','','','','','','');
INSERT INTO META_RAW.PROCESS VALUES (2277,2277,'ReverseSync_SDL_PH_MDS_GT_CUSTOMER',2277,1,2,FALSE,TRUE,539,5,1,NULL,'','','','','','','');



create or replace TABLE PROD_DNA_LOAD.PHLSDL_RAW.SDL_PH_MDS_GT_CUSTOMER (
	LDT_SAP_ID VARCHAR(100),
	DT_ID VARCHAR(100),
	"Country_Code" VARCHAR(100),
	"Outlet_ID" VARCHAR(400),
	"Outlet_Name" VARCHAR(400),
	"Address_1" VARCHAR(400),
	"Address_2" VARCHAR(400),
	"Telephone" VARCHAR(100),
	FAX VARCHAR(100),
	"City" VARCHAR(100),
	"PostCode" VARCHAR(100),
	"Region" VARCHAR(100),
	"Channel_Group" VARCHAR(100),
	"Sub_Channel" VARCHAR(100),
	"Sales_Route_ID" VARCHAR(100),
	"Sales_Route_Name" VARCHAR(100),
	"SaleGroup" VARCHAR(100),
	"SalesRep_ID" VARCHAR(100),
	"SaleRep_Name" VARCHAR(100),
	"GPS_Lat" VARCHAR(100),
	"GPS_Long" VARCHAR(100),
	"Status" VARCHAR(100),
	"District" VARCHAR(100),
	"Province" VARCHAR(100),
	"Sup_Code" VARCHAR(100),
	"Sup_Name" VARCHAR(100),
	"Store_Prioritization" VARCHAR(100),
	FILE_NAME VARCHAR(100),
	CRT_DTTM TIMESTAMP_NTZ(9)
);


create or replace TABLE PROD_DNA_LOAD.PHLSDL_RAW.SDL_PH_MDS_POS_711 (
	MNTH_ID VARCHAR(30),
	STORE_CD VARCHAR(30),
	STORE_NM VARCHAR(255),
	ITEM_CATEGORY VARCHAR(100),
	ITEM_CD VARCHAR(30),
	ITEM_NM VARCHAR(255),
	TOT_QTY VARCHAR(255),
	TOT_AMT VARCHAR(255),
	NO_OF_SLS_DAYS VARCHAR(10),
	AVG_DAY_QTY VARCHAR(255),
	AVG_AMT_DAY VARCHAR(255),
	FILE_NAME VARCHAR(255)
);



create or replace TABLE PROD_DNA_LOAD.PHLSDL_RAW.SDL_PH_MDS_POS_DYNA (
	MNTH_ID VARCHAR(255),
	SLS_AREA VARCHAR(255),
	PLANT VARCHAR(255),
	CUSTOMER_ID VARCHAR(255),
	OLD_CUST_ID VARCHAR(255),
	CUST_NM VARCHAR(255),
	CHNL VARCHAR(255),
	SLS_OFF VARCHAR(255),
	SLS_GRP VARCHAR(255),
	ADDRESS VARCHAR(255),
	CITY VARCHAR(255),
	POSTAL_CD VARCHAR(255),
	DSM VARCHAR(255),
	SMAN VARCHAR(255),
	PRIN VARCHAR(255),
	PRINCIPAL VARCHAR(255),
	MATL_GRP VARCHAR(255),
	MATL_SUB_GRP VARCHAR(255),
	BRAND VARCHAR(255),
	UOM_CONV VARCHAR(255),
	MATL_NUM VARCHAR(255),
	OLD_MATL_NUM VARCHAR(255),
	MATL_DESC VARCHAR(255),
	QTY VARCHAR(255),
	SLS_AMT VARCHAR(255),
	FILE_NAME VARCHAR(255)
);


create or replace TABLE PROD_DNA_LOAD.PHLSDL_RAW.SDL_PH_MDS_POS_PRICELIST (
	PRODUCT_NAME VARCHAR(100),
	PRODUCT_CODE VARCHAR(16777216),
	CONSUMERS_BARCODE VARCHAR(100),
	SHIPPERS_BARCODE VARCHAR(100),
	DZPERCASE VARCHAR(100),
	LISTPRICECASE VARCHAR(100),
	LISTPRICEDZ VARCHAR(100),
	LISTPRICEUNIT VARCHAR(100),
	SRP VARCHAR(100),
	LEGEND VARCHAR(100),
	FILENAME VARCHAR(100),
	CRT_DTTM TIMESTAMP_NTZ(9)
);


create or replace TABLE PROD_DNA_LOAD.PHLSDL_RAW.SDL_PH_MDS_POS_PUREGOLD (
	PO_NUMBER VARCHAR(50),
	VENDOR_CODE VARCHAR(50),
	VENDOR_NAME VARCHAR(50),
	FROM_STORE VARCHAR(50),
	TO_STORE VARCHAR(50),
	STORE_NAME VARCHAR(50),
	SKU VARCHAR(50),
	SKU_DESC VARCHAR(100),
	QTY VARCHAR(50),
	RCR_NUMBER VARCHAR(50),
	FILE_NAME VARCHAR(50),
	CRT_DTTM TIMESTAMP_NTZ(9)
);



create or replace TABLE PROD_DNA_LOAD.PHLSDL_RAW.SDL_PH_MDS_POS_ROB (
	JJ_MNTH_ID VARCHAR(256),
	POS_PROD_CD VARCHAR(256),
	POS_PROD_NM VARCHAR(256),
	UPC VARCHAR(256),
	STORE_CD VARCHAR(256),
	STORE_NM VARCHAR(256),
	UOM VARCHAR(256),
	QTY VARCHAR(256),
	NET_AMT VARCHAR(256),
	TAX_AMT VARCHAR(256),
	AMT VARCHAR(256),
	CUST_PRICE_PC VARCHAR(256),
	FILE_NM VARCHAR(256),
	CDL_DTTM VARCHAR(256),
	CURR_DATE TIMESTAMP_NTZ(9)
);


create or replace TABLE PROD_DNA_LOAD.PHLSDL_RAW.SDL_PH_MDS_POS_SM_GR (
	BUISNESSNAME VARCHAR(16777216),
	TITLE VARCHAR(16777216),
	DATE VARCHAR(16777216),
	VENDORCODE VARCHAR(16777216),
	VENDORNAME VARCHAR(16777216),
	RECEIPTDATE VARCHAR(16777216),
	TERMSANDDISCOUNT VARCHAR(16777216),
	SITECODE VARCHAR(16777216),
	SITENAME VARCHAR(16777216),
	SHIPTO VARCHAR(16777216),
	GRNUMBER VARCHAR(16777216),
	PONUMBER VARCHAR(16777216),
	CANCELDATE VARCHAR(16777216),
	TOTALARTICLES VARCHAR(16777216),
	ARTICLENUMBER VARCHAR(16777216),
	ARTICLEDESCRIPTION VARCHAR(16777216),
	UPC VARCHAR(16777216),
	UOM VARCHAR(16777216),
	RECEIVEDQTY VARCHAR(16777216),
	IMPORTANTREMARKS VARCHAR(16777216),
	CRTD_DTTM TIMESTAMP_NTZ(9) NOT NULL,
	FILE_NAME VARCHAR(16) NOT NULL
);



create or replace TABLE PROD_DNA_LOAD.PHLSDL_RAW.SDL_PH_MDS_POS_SM_PO (
	DOCUMENTTYPE VARCHAR(16777216),
	COMPANYNAME VARCHAR(16777216),
	POSTDATE VARCHAR(16777216),
	POSTTIME VARCHAR(16777216),
	VENDORNAME VARCHAR(16777216),
	VENDORCODE VARCHAR(16777216),
	TERMSDISCOUNT VARCHAR(16777216),
	SITECODE VARCHAR(16777216),
	SITENAME VARCHAR(16777216),
	SITEADDRESS VARCHAR(16777216),
	SITETIN VARCHAR(16777216),
	DOCUMENTNUMBER VARCHAR(16777216),
	DATEENTRY VARCHAR(16777216),
	DATECANCEL VARCHAR(16777216),
	DATERECEIPT VARCHAR(16777216),
	ARTICLECOUNT VARCHAR(16777216),
	DELIVERYMESSAGE VARCHAR(16777216),
	LINENUMBER VARCHAR(16777216),
	SKUMATCODE VARCHAR(16777216),
	DESCRIPTION VARCHAR(16777216),
	DISCOUNT VARCHAR(16777216),
	UPC VARCHAR(16777216),
	BUYQTY VARCHAR(16777216),
	BUYCOST VARCHAR(16777216),
	BUYUM VARCHAR(16777216),
	PACKAGE VARCHAR(16777216),
	SELLUM VARCHAR(16777216),
	PPSITECODE VARCHAR(16777216),
	PPSITEDESC VARCHAR(16777216),
	PPQTY VARCHAR(16777216),
	PPUNIT VARCHAR(16777216),
	TOTALAMOUNT VARCHAR(16777216),
	REMARKS VARCHAR(16777216),
	CRTD_DTTM TIMESTAMP_NTZ(9) NOT NULL,
	FILE_NAME VARCHAR(16) NOT NULL
);


create or replace TABLE PROD_DNA_LOAD.PHLSDL_RAW.SDL_PH_MDS_POS_SOUTH_STAR (
	JJ_MNTH_ID VARCHAR(256),
	POS_PROD_CD VARCHAR(256),
	POS_PROD_NM VARCHAR(256),
	UPC VARCHAR(256),
	SUPPLIER_PROD_CD VARCHAR(256),
	STORE_CD VARCHAR(256),
	STORE_NM VARCHAR(256),
	QTY VARCHAR(256),
	AMT VARCHAR(256),
	FILE_NM VARCHAR(256),
	CDL_DTTM VARCHAR(256),
	CURR_DATE TIMESTAMP_NTZ(9)
);


create or replace TABLE PROD_DNA_LOAD.PHLSDL_RAW.SDL_PH_MDS_POS_WALTERMART (
	JJ_MNTH_ID VARCHAR(256),
	CODING VARCHAR(256),
	STORE_CD VARCHAR(256),
	STORE_NM VARCHAR(256),
	DEPT VARCHAR(256),
	SDEPT VARCHAR(256),
	WALTER_CLASS VARCHAR(256),
	SCLASS VARCHAR(256),
	CATEGORY_NM VARCHAR(256),
	VENDOR_CD VARCHAR(256),
	VENDOR_NM VARCHAR(256),
	POS_PROD_CD VARCHAR(256),
	POS_PROD_NM VARCHAR(256),
	QTY VARCHAR(256),
	AMT VARCHAR(256),
	CUST_PRICE_PC VARCHAR(256),
	FILE_NM VARCHAR(256),
	CDL_DTTM VARCHAR(256),
	CURR_DATE TIMESTAMP_NTZ(9)
);


create or replace TABLE PROD_DNA_LOAD.PHLSDL_RAW.SDL_PH_MDS_POS_WATSONS (
	JJ_MNTH_ID VARCHAR(256),
	STORE_CD VARCHAR(256),
	STORE_NM VARCHAR(256),
	POS_PROD_CD VARCHAR(256),
	POS_PROD_NM VARCHAR(256),
	AMT VARCHAR(256),
	QTY VARCHAR(256),
	CUST_PRICE_PC VARCHAR(256),
	FILE_NM VARCHAR(256),
	CDL_DTTM VARCHAR(256),
	CURR_DATE TIMESTAMP_NTZ(9)
);

UPDATE META_RAW.PARAMETERS set parameter_value='PHLSDL_RAW.PH_POS_MDS_SOUTH_STAR_PREPROCESSING' WHERE PARAMETER_ID='27527'

CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.PHLSDL_RAW.PH_GT_CUSTOMERS_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python','unidecode ==1.3.8')
HANDLER = 'main'
EXECUTE AS OWNER
AS '
from snowflake.snowpark.functions import lit
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
from unidecode import unidecode
import pytz
from snowflake.snowpark.files import SnowflakeFile

def main(session: snowpark.Session, Param):
    try:
        #Param = [''''OUT_CON_C_PH_20240905220000.csv'''',''''PHLSDL_RAW.PROD_LOAD_STAGE_ADLS'''',''''prd/DMS/Customer'''',''''SDL_PH_MDS_GT_Customer'''']
        file_name       = Param[0]
        stage_name      = Param[1]
        stage_path 		= Param[2]
        target_table    = Param[3]
       
        #read the file from path
        full_path = "@"+stage_name+"/"+stage_path+"/"+file_name
    
        #read the file
        with SnowflakeFile.open(full_path,''rb'',require_scoped_url= False) as f:
            df = pd.read_csv(f,
                             dtype={"LDT_SAP_ID" : "string",
                                    "DT_ID" : "string",
                                    "Country_Code" : "string",
                                    "Outlet_ID" : "string",
                                    "Outlet_Name" : "string",
                                    "Address_1" : "string",
                                    "Address_2" : "string",
                                    "Telephone" : "string",
                                    "FAX" : "string",
                                    "City" : "string",
                                    "PostCode" : "string",
                                    "Region" : "string",
                                    "Channel_Group" : "string",
                                    "Sub_Channel" : "string",
                                    "Sales_Route_ID" : "string",
                                    "Sales_Route_Name" : "string",
                                    "SaleGroup" : "string",
                                    "SalesRep_ID" : "string",
                                    "SaleRep_Name" : "string",
                                    "GPS_Lat" : "string",
                                    "GPS_Long" : "string",
                                    "Status" : "string",
                                    "District" : "string",
                                    "Province" : "string",
                                    "Sup_Code" : "string",
                                    "Sup_Name" : "string",
                                    "Store_Prioritization" : "string",
                                    })
    
        df = df.map(lambda x: str(x))
        df = df.map(lambda x : unidecode(x))
    
        #Replacing all nan with null
        df = df.map(lambda x: None if x == ''<NA>'' else x)
    
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
	
	
CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.ASPSDL_RAW.FILE_VALIDATION("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('openpyxl==3.1.2','regex==2023.10.3','snowflake-snowpark-python==*','xlrd==2.0.1','pyarrow==14.0.2')
HANDLER = 'main'
EXECUTE AS OWNER
AS '

import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col
from snowflake.snowpark.files import SnowflakeFile
import math
import regex
import pandas as pd
from datetime import datetime
import zipfile
import pyarrow.parquet as pq
import pyarrow as pa


def main(session: snowpark.Session,Param):
    try:

        # Variables passed from Parameters table
        # CURRENT_FILE , index , validation, val_file_name,val_file_extn,val_header,file_header_row_num,stage_name,temp_stage_path,header_reg,sheet_names,schema


        CURRENT_FILE        =  Param[0]
        index               =  Param[1]
        validation          =  Param[2]
        val_file_name       =  Param[3]
        val_file_extn       =  Param[4]
        val_header          =  Param[5]
        file_header_row_num	=  Param[6]
        stage_name     		=  Param[7]
        temp_stage_path		=  Param[8]
        header_reg          =  Param[9]
        sheet_names         =  Param[10]
        schema              =  Param[7].split(".")[0]

        # Split the value from validation parameter to activate the respective validation
        # 1-1-1 , first occurence of 1-FileNameValidation , second occurence of 1-FileExtnValidation, last occurence of 1-FileHeaderValidation

        FileNameValidation,FileExtnValidation,FileHeaderValidation = validation.split("-")
        counter             =  0 


        # If validation flag is 0 for all, then return the success message to ADF
        if FileNameValidation=="0" and FileExtnValidation=="0" and FileHeaderValidation=="0":
             return "SUCCESS: File validation passed"
            
        
    
        # If the File belongs to Regional, then it enters the function
    
        if stage_name.split(".")[0]=="ASPSDL_RAW":
            processed_file_name=rg_travel_validation(CURRENT_FILE)
            
        # If the File belongs to Thailand, then it enters the function

        elif stage_name.split(".")[0]=="THASDL_RAW": 
            processed_file_name=thailand_processing(CURRENT_FILE)

        # If the File belongs to Pacific, then it enters the function

        elif stage_name.split(".")[0]=="PCFSDL_RAW":
            processed_file_name=aus_processing(CURRENT_FILE)

        # If the File belongs to North Asia, then it enters the function

        elif stage_name.split(".")[0]=="NTASDL_RAW":
            processed_file_name=north_asia_processing(CURRENT_FILE)

        # If the File belongs to Indonesia, then it enters the function

        elif stage_name.split(".")[0]=="IDNSDL_RAW":
            processed_file_name=indonesia_processing(CURRENT_FILE)

		# If the File belongs to Philippines, then it enters the function
        elif stage_name.split(".")[0]=="PHLSDL_RAW":
           processed_file_name=philippines_processing(CURRENT_FILE) 

        else:
            processed_file_name=CURRENT_FILE

    
        # Extracting the filename based on index variable last, first, full, pre
        
        if index.lower() == "last":
            extracted_filename = processed_file_name.rsplit("_", 1)[0]
            print(extracted_filename)
        elif index.lower() == "first":
            extracted_filename = processed_file_name.split("_")[0]
            print(extracted_filename)
        elif index.lower() == "full":
            extracted_filename = processed_file_name.rsplit(".", 1)[0]
            print(extracted_filename)
        elif index.lower() == "pre":
            extracted_filename = processed_file_name.split("_", 1)[1].split(".")[0]
            print(extracted_filename)  
        elif index =="name_mmmyyyy.xlsx" or index =="name_yyyymmww.xlsx" or index =="name_yyyymmww.xls":
            extracted_filename=CURRENT_FILE.split(".")[0]

    
    
        # Check for File name Validation, if the flag is 1 then call the File name Validation function
    
        if FileNameValidation=="1":
            file_name_validation_status,counter=file_validation(counter,extracted_filename,val_file_name,schema)
        else :
            print("File Name Validation not required")
    
    
        # Check for File extn Validation, if the flag is 1 then call the File extn Validation function
    
        if FileExtnValidation == "1":
            file_ext_validation_status,counter=file_extn_validation(counter,CURRENT_FILE,val_file_extn)
        else:
            print("File extension Validation not required")
    
    
        # Check for File Header Validation, if the flag is 1 then proceed with File Header Validation
    
        if FileHeaderValidation == "1" and counter==0:

            # Converting the extension from xlsx to csv
            # Extracting the Header from the file
            # If- else is bought in to handle files which requires additional process to extract
            # Reading files with specific encoding, using pandas for specific need and normal way of reading files
            
            if "NTUC" in CURRENT_FILE:
                # if Core
                file="CORE"
                file_name=file +".csv"
                data_core = session.read.option("INFER_SCHEMA", True).option("field_optionally_enclosed_by", "\\"").csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
                df_core=data_core.to_pandas()
                header_core=df_core.iloc[int(file_header_row_num)].tolist()

                file="OTC"
                file_name=file +".csv"
                data_otc = session.read.option("INFER_SCHEMA", True).option("field_optionally_enclosed_by", "\\"").csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
                df_otc=data_otc.to_pandas()
                header_otc=df_otc.iloc[int(file_header_row_num)].tolist()

                if header_core!=header_otc:
                    return "File Validation Failed; Columns from both the sheets are not matching"
                else:
                    header=header_core

            elif "CRM_Children" in CURRENT_FILE or "CRM_Consumer" in CURRENT_FILE or "Action_Complaint" in CURRENT_FILE or "Action_Open" in CURRENT_FILE or "Action_Click" in CURRENT_FILE or "Action_Sent" in CURRENT_FILE or "Action_Unsubscribe" in CURRENT_FILE or "Action_Bounce" in CURRENT_FILE or "TW_CRM_Invoice" in CURRENT_FILE or "TW_CRM_Redemption" in CURRENT_FILE or "PROD_Naver_KR_Lounge_Data" in CURRENT_FILE :
                file_name= CURRENT_FILE.replace("xlsx","csv")
                file_name = file_name.replace("(", "").replace(")", "").replace(" ","_")
                utf_encoding= ''UTF-16''
                df = session.read.option("INFER_SCHEMA", True).option("field_optionally_enclosed_by", "\\"").option("encoding",utf_encoding).csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

                df_pandas=df.to_pandas()
                header=df_pandas.iloc[int(file_header_row_num)].tolist()
                
            elif "OUT_CON_Forecast_VN" in CURRENT_FILE or "OUT_CON_Yeartarget" in CURRENT_FILE or "PH_IOP" in CURRENT_FILE:
                file_name= CURRENT_FILE.replace("xlsx","csv")
                file_name = file_name.replace("(", "").replace(")", "").replace(" ","_")
                df = session.read.option("INFER_SCHEMA", True).csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

                df_pandas=df.to_pandas()
                header=df_pandas.iloc[int(file_header_row_num)].tolist()

            elif "Weekly_Sales_Report_-_Kenvue" in CURRENT_FILE:
                file_name= CURRENT_FILE.replace("xlsx","csv").replace("xls","csv")
                file_name = file_name.replace("(", "").replace(")", "").replace(" ","_")
                df = session.read.option("INFER_SCHEMA", True).option("field_delimiter", "\\u0001").option("field_optionally_enclosed_by", "\\"").csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

            
            
                df_pandas=df.to_pandas()
                header=df_pandas.iloc[int(file_header_row_num)].tolist()
                
            elif "SPS003" in CURRENT_FILE:
                file_name= CURRENT_FILE.replace("xlsx","csv").replace("xls","csv")
                file_name = file_name.replace("(", "").replace(")", "").replace(" ","_")
                df = session.read.option("INFER_SCHEMA", True).option("field_optionally_enclosed_by", "\\"").option("REPLACE_INVALID_CHARACTERS", True).option("null_if", "").csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

                df_pandas=df.to_pandas()
                header=df_pandas.iloc[int(file_header_row_num)].tolist()

            elif "Account" in CURRENT_FILE or "PH_BP" in CURRENT_FILE or "Constants" in CURRENT_FILE or "DealDiscount" in CURRENT_FILE or "Todo" in CURRENT_FILE:
                file_name= CURRENT_FILE.replace("xlsx","csv").replace("xls","csv")
                file_name = file_name.replace("(", "").replace(")", "").replace(" ","_")
                df = session.read.option("INFER_SCHEMA", True).csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

                df_pandas=df.to_pandas()
                header=df_pandas.iloc[int(file_header_row_num)].tolist()

            elif CURRENT_FILE[0:3]=="ROB" or CURRENT_FILE[0:2]=="SS" or "MT01P39R" in CURRENT_FILE:
                full_path = "@"+stage_name+"/"+temp_stage_path+"/"+CURRENT_FILE
                with SnowflakeFile.open(full_path, "rb", require_scoped_url = False) as f:
                    df_pandas=pd.read_excel(f)
                    header=df_pandas.iloc[int(file_header_row_num)].tolist()

            elif val_file_name=="FSSI Week":
                file_name=CURRENT_FILE.replace("_"," ")
                full_path = "@"+stage_name+"/"+temp_stage_path+"/"+file_name
                with SnowflakeFile.open(full_path, "rb", require_scoped_url = False) as f:
                    df_pandas=pd.read_excel(f)
                    header=df_pandas.iloc[int(file_header_row_num)].tolist()

            elif "JJ_KPI_Status" in CURRENT_FILE:
                file_name= CURRENT_FILE.replace("xlsx","csv").replace("xls","csv")
                file_name = file_name.replace("(", "").replace(")", "").replace(" ","_")
                df = session.read.option("INFER_SCHEMA", True).option("field_delimiter", "\\u0001").option("field_optionally_enclosed_by", "\\"").csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
                
                df_pandas=df.to_pandas()
                df_header=df_pandas.iloc[:,:10]
                header=df_header.iloc[int(file_header_row_num)].tolist()

            elif "TW_POS_PXCivilia" in CURRENT_FILE or ''TW_POS_RTMart_RawData'' in  CURRENT_FILE:
                file_name= CURRENT_FILE.replace("xlsx","csv").replace("xls","csv")
                file_name = file_name.replace("(", "").replace(")", "").replace(" ","_")
                df = session.read.option("INFER_SCHEMA", True).option("field_delimiter", "").option("field_optionally_enclosed_by", "\\"").csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
                
                df_pandas=df.to_pandas()
                header=df_pandas.iloc[int(file_header_row_num)].tolist()

            elif "HK_IMS_Irpt_wingkeung" in CURRENT_FILE:
                file_name= CURRENT_FILE.replace("xlsx","csv").replace("xls","csv")
                file_name = file_name.replace("(", "").replace(")", "").replace(" ","_")            
                df = session.read.option("INFER_SCHEMA", True).option("encoding","Big5").option("field_optionally_enclosed_by", "\\"").csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
                
                df_pandas=df.to_pandas()
                header=df_pandas.iloc[int(file_header_row_num)].tolist()

            elif "Weekly_Summary_Trexi_raw_data" in CURRENT_FILE:
                file_name= CURRENT_FILE.replace("xlsx","csv").replace("xls","csv")
                file_name = file_name.replace("(", "").replace(")", "").replace(" ","_")
                df = session.read.option("INFER_SCHEMA", True).option("field_delimiter", "").option("field_optionally_enclosed_by", "\\"").csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

                df_pandas=df.to_pandas()
                df_header=df_pandas.iloc[:,0:14]
                header=df_header.iloc[int(file_header_row_num)].tolist()

            elif ("BU_" in CURRENT_FILE and "bu_forecast/sku/" in temp_stage_path) or "BP_" in CURRENT_FILE :
                file_name= CURRENT_FILE.replace("xlsx","csv").replace("xls","csv")
                file_name = file_name.replace("(", "").replace(")", "").replace(" ","_")
                df = session.read.option("INFER_SCHEMA", True).option("field_optionally_enclosed_by", "\\"").csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
            
                df_pandas=df.to_pandas()
                header_extract=df_pandas.iloc[int(file_header_row_num)].tolist()
                header = [s.replace("''", ''"'') for s in header_extract]

            elif "PROMO_COMPETITOR" in CURRENT_FILE or "BRAND_BLOCKING" in CURRENT_FILE or "VISIBILITY" in CURRENT_FILE or "PLANOGRAM" in CURRENT_FILE or "PRICING" in CURRENT_FILE or "SECONDARY_DISPLAY" in CURRENT_FILE or "PROMO" in CURRENT_FILE or "PRODUCT_AVAILABILITY" in CURRENT_FILE:
                file_name=CURRENT_FILE.replace("xlsx","csv").replace("xls","csv")
                file_name=file_name.split("_")[0]
                df = session.read.option("INFER_SCHEMA", True).option("field_delimiter", "\\t").option("field_optionally_enclosed_by", "\\"").csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
            
                df_pandas=df.to_pandas()
                header=df_pandas.iloc[int(file_header_row_num)].tolist()

            elif "Naver_keyword" in CURRENT_FILE:
                file_name= CURRENT_FILE
                file_name = file_name.replace("(", "").replace(")", "").replace(" ","_")
                full_path = "@"+stage_name+"/"+temp_stage_path+"/"+file_name
                with SnowflakeFile.open(full_path, "rb", require_scoped_url = False) as f:
                    df_pandas=pd.read_excel(f)
                    header=df_pandas.iloc[int(file_header_row_num)].tolist()

            elif "WW_AV303_Stock_Status_Weekly" in CURRENT_FILE:
                file_name=''Current_Week.csv''
                df = session.read.option("INFER_SCHEMA", True).option("field_optionally_enclosed_by", "\\"").csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
            
                df_pandas=df.to_pandas()
                df_header=df_pandas.iloc[:,0:44]
                header=df_header.iloc[int(file_header_row_num)].tolist()
                

            elif "Sellout_1" in CURRENT_FILE:
                file_name= CURRENT_FILE.replace("xlsx","csv").replace("xls","csv")
                file_name = file_name.replace("(", "").replace(")", "").replace(" ","_")
                df = session.read.option("INFER_SCHEMA", True).option("field_optionally_enclosed_by", "\\"").option("field_delimiter", "|").csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
                df_pandas=df.to_pandas()
                header=df_pandas.iloc[int(file_header_row_num)].tolist()

            elif "HCPSDL_RAW" in stage_name and "SFMC/" in temp_stage_path :
                file_name= CURRENT_FILE.replace("xlsx","csv")
                file_name = file_name.replace("(", "").replace(")", "").replace(" ","_")
                utf_encoding= ''UTF-16LE''
                df = session.read.option("INFER_SCHEMA", True).option("field_optionally_enclosed_by", "\\"").option("encoding",utf_encoding).csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

                df_pandas=df.to_pandas()
                header=df_pandas.iloc[int(file_header_row_num)].tolist()

            elif sheet_names != "[]" and "iConnectUsers" in CURRENT_FILE:
                print("double sheet")
                sheets = sheet_names[1:-1].split(",")
                tmp_header= []
                for sheet in sheets:
                    file_name = sheet.strip("\\"").replace("(", "").replace(")", "").replace(" ","_")+".csv"
                    df = session.read.option("INFER_SCHEMA", True).option("field_optionally_enclosed_by", "\\"").csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
                    df_pandas=df.to_pandas()
                    tmp_header= tmp_header + df_pandas.iloc[int(file_header_row_num)].tolist()
                header = ["\\x01".join(tmp_header)]

			# Handle excel files with multi sheet, for those files where header validation is required for more than one sheet 
            # For reading header from excel having multiple sheets using sheet names parameter
            #Philippines Rose Pharmacy and SM_GR
            elif sheet_names != "[]" and ("Rose_Pharmacy" in CURRENT_FILE or "SM_GR" in CURRENT_FILE ):
                print("double sheet")
                sheets = sheet_names[1:-1].split(",")
                tmp_header = []
                for sheet in sheets:
                    file_name = sheet.strip("\\"").replace("(", "").replace(")", "").replace(" ", "_")+".csv"

                    file_header_rownum = 0
                    print("Sheetname:", sheet)
                    if "Offtake" in file_name:                        
                        file_header_rownum = file_header_row_num - 1
                    if "Inventory" in file_name:
                        file_header_rownum = file_header_row_num
                    
                    # Reading CSV into Snowpark DataFrame
                    df = session.read.option("INFER_SCHEMA", True).option("field_optionally_enclosed_by", "\\"").csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)                    
                    # Convert Snowpark DataFrame to Pandas DataFrame
                    df_pandas = df.to_pandas()                    
                    # Extract header from the specified row
                    tmp_header += df_pandas.iloc[int(file_header_rownum)].tolist()                    
                    # Combine headers into a single string with "\\x01" as delimiter

                    if "SM_GR" in CURRENT_FILE:
                        tmp_header = [i for i in tmp_header if i is not None]
                        
                new_list = []
                for i in tmp_header:
                    new_list += i.split("")
                header = ["\\x01".join(new_list)]																																   
                

            elif sheet_names != "[]":
                file_name = sheet_names[1:-1].split(",")[0]
                file_name = file_name.strip("\\"").replace("(", "").replace(")", "").replace(" ","_")+".csv"
                df = session.read.option("INFER_SCHEMA", True).option("field_optionally_enclosed_by", "\\"").csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
                df_pandas=df.to_pandas()
                header=df_pandas.iloc[int(file_header_row_num)].tolist()
                    

            

            elif "pop6/transaction" in temp_stage_path:
                full_path = "@"+stage_name+"/"+temp_stage_path+"/"+CURRENT_FILE
                with SnowflakeFile.open(full_path, "rb", require_scoped_url = False) as f:
                    df_pandas=pd.read_csv(f,sep="|")
                    header=df_pandas.columns
                

            elif "ventasys/transaction" in temp_stage_path and val_file_extn == "zip":
                full_path = "@"+stage_name+"/"+temp_stage_path+"/"+CURRENT_FILE
                with SnowflakeFile.open(full_path, "rb", require_scoped_url = False) as f:
                    with zipfile.ZipFile(f,"r") as zip_ref:
                        for i_file_name in zip_ref.namelist():
                            with zip_ref.open(i_file_name) as file:
                                df_pandas = pd.read_csv(file, delimiter="~", header =file_header_row_num, nrows = file_header_row_num+1, encoding_errors = "replace", quoting=3, na_filter=False)
                                header=list(df_pandas)


            elif "OTC_Sellout" in CURRENT_FILE and schema=="CHNSDL_RAW":
                full_path = "@"+stage_name+"/"+temp_stage_path+"/"+CURRENT_FILE
                with SnowflakeFile.open(full_path, "rb", require_scoped_url = False) as f:
                    parquet_file = pq.ParquetFile(f)
                    first_ten_rows = next(parquet_file.iter_batches(batch_size=10))
                    df_pandas = pa.Table.from_batches([first_ten_rows]).to_pandas()
                    header=df_pandas.columns
                       
            else:
                file_name= CURRENT_FILE.replace("xlsx","csv").replace("xls","csv")
                file_name = file_name.replace("(", "").replace(")", "").replace(" ","_")
                df = session.read.option("INFER_SCHEMA", True).option("field_optionally_enclosed_by", "\\"").csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
            
                df_pandas=df.to_pandas()
                header=df_pandas.iloc[int(file_header_row_num)].tolist()
                    
                

            # If the source is of xlsx type, then splitting based on \\\\\\\\x01 delimiter

            if CURRENT_FILE[0:3]=="ROB" or CURRENT_FILE[0:2]=="SS":
                header=[i[:-1] for i in header]

            elif "Naver_keyword" in CURRENT_FILE:
                header=header
                
            # If the Header from File is of comma separated or | or ~ separated then split accordingly
            else:
                header_pipe_split = header[0].split(''|'')
                header_tilda_split = header[0].split(''~'')
                header_comma_split=header[0].split(",")
                
            if (val_file_extn==''xlsx'' or val_file_extn==''xls'') and ''Weekly Sales Report'' not in val_file_name and CURRENT_FILE[0:3]!="ROB" and CURRENT_FILE[0:2]!="SS" and "FSSI_Week" not in CURRENT_FILE and "JJ_KPI_Status" not in CURRENT_FILE and "TW_POS_PXCivilia" not in CURRENT_FILE and ''TW_POS_RTMart_RawData'' not in  CURRENT_FILE and "MT01P39R" not in CURRENT_FILE and "Weekly_Summary_Trexi_raw_data" not in CURRENT_FILE and "Naver_keyword" not in CURRENT_FILE:
                result_list = header[0].split(''\\x01'')
            elif val_file_extn==''xlsx'':
                result_list = header
        
            elif len(header_pipe_split)>1:
                result_list = header_pipe_split

            elif len(header_tilda_split)>1:
                result_list = header_tilda_split

            elif len(header_comma_split)>1:
                result_list=header_comma_split
                
            else:
                result_list = header

            # Handle null values, empty strings or any other handling if required

            if stage_name.split(".")[0]=="PCFSDL_RAW" and val_file_name=="FSSI Week":
                filtered_list=list('''' if pd.isna(x) else x for x in result_list)
                filtered_list=[s.split(''\\n'')[0] for s in filtered_list]

            elif stage_name.split(".")[0]=="PCFSDL_RAW" and val_file_name=="ManufacturersReport":
                result=list(filter(None,result_list))
                result_list=result[:13]
                filtered_list = [value for value in result_list if value is not None and not (isinstance(value, float) and math.isnan(value))]

            elif stage_name.split(".")[0]=="PCFSDL_RAW" and val_file_name=="Replenishment E3 Buyers Report":
                result_list=result_list[:19]
                filtered_list = [value for value in result_list if value is not None and not (isinstance(value, float) and math.isnan(value))]

            elif stage_name.split(".")[0]=="PCFSDL_RAW" and val_file_name==''Stock Status by DC - 13 Months Sales'':
                result_list=result_list[:11]
                filtered_list = [value for value in result_list if value is not None and not (isinstance(value, float) and math.isnan(value))]

            elif stage_name.split(".")[0]=="PCFSDL_RAW" and val_file_name==''MT01P39R'':
                result=list(filter(None,result_list))
                filtered_list = [string.replace(''\\n'', '''') for string in result]

            elif stage_name.split(".")[0]=="PCFSDL_RAW" and val_file_name==''WW_AV303_Stock_Status_Weekly'':
                result_list=result_list[:45]
                filtered_list = [value for value in result_list if value is not None and not (isinstance(value, float) and math.isnan(value))] 


            elif stage_name.split(".")[0]=="NTASDL_RAW" and val_file_name==''Weekly_Summary_Trexi_raw_data'':
                result_list=result_list[:12]
                filtered_list = [value for value in result_list if value is not None and not (isinstance(value, float) and math.isnan(value))]

            elif (stage_name.split(".")[0]=="NTASDL_RAW" and val_file_name==''J_J'') or (stage_name.split(".")[0]=="HCPSDL_RAW" and val_file_name== ''IQVIA_ORSL_Brand'') or (stage_name.split(".")[0]=="HCPSDL_RAW" and val_file_name==''IQVIA_AVEENO_Specialty''):
                result_list=result_list[:5]
                filtered_list = [value for value in result_list if value is not None and not (isinstance(value, float) and math.isnan(value))]

            elif stage_name.split(".")[0]=="HCPSDL_RAW" and (val_file_name== ''IQVIA_ORSL_Indication'' or val_file_name==''IQVIA_ORSL_Specialty'' or val_file_name==''IQVIA_AVEENO_Sales_Brand'' or val_file_name==''IQVIA_AVEENO_Brand''):
                result_list=result_list[:4]
                filtered_list = [value for value in result_list if value is not None and not (isinstance(value, float) and math.isnan(value))]

            elif stage_name.split(".")[0]=="HCPSDL_RAW" and val_file_name== ''IQVIA_ORSL_Sales_Brand'':
                result_list=result_list[:6]
                filtered_list = [value for value in result_list if value is not None and not (isinstance(value, float) and math.isnan(value))]

            elif stage_name.split(".")[0]=="NTASDL_RAW" and val_file_name=="JNJ":
                result_list=result_list[:21]
                filtered_list = [value for value in result_list if value is not None and not (isinstance(value, float) and math.isnan(value))]

            else:
                result_list=list(filter(None,result_list))
                filtered_list = [value for value in result_list if value is not None and not (isinstance(value, float) and math.isnan(value))]

            # to keep a common standard, we are replacing . and space with underscore

            file_header= [item.replace(" ", "_").replace(".", "_") for item in filtered_list]
            val_header= val_header.lower()
			
			#Philippines Rose Pharmacy and SM_GR
            if "Rose_Pharmacy" in CURRENT_FILE or "SM_GR" in CURRENT_FILE:
                val_header = val_header.replace('' '',''_'')
                valid_header = val_header.split('';'')
                final_valid_header = []
                for hdr in valid_header:
                    pipe_split = hdr.split(''|'')
                    final_valid_header.append(pipe_split)
                print(final_valid_header)

                final_val_header = [x for a in final_valid_header for x in a]
                header_reg = header_reg.lower()
                regex_list = header_reg.split(''^'')
				
				
            # If the Header from Metadata is of comma separated or | separated then split accordingly
            
            comma_split = val_header.split('','')
            if len(comma_split) > 1:
                final_val_header=comma_split
    
            pipe_split = val_header.split(''|'')
            if len(pipe_split) > 1:
                final_val_header=pipe_split

            space_split=val_header.split(" ")
            if len(space_split) > 1:
                final_val_header=space_split

            tab_split=val_header.split("	")
            if len(tab_split) > 1:
                final_val_header=tab_split

            tilda_split=val_header.split("~")
            if len(tilda_split) > 1:
                final_val_header=tilda_split

            header_reg = header_reg.lower()
            regex_list = header_reg.split(''^'')

            # Call the file header validation function
            
            file_header_validation_status,counter=file_header_validation(counter,final_val_header,file_header, regex_list)
        
        else:
            print("File Header Validation not required")


        # Return error message based on the failure on specific stage
        if counter == 0 :
                validation_status = "SUCCESS: File validation passed" 
        elif counter == 1 :
                validation_status = "FAILED: {0}".format(file_name_validation_status)
        elif counter == 2 :
                validation_status = "FAILED: {0}".format(file_ext_validation_status)
        elif counter == 3 :
                validation_status = "FAILED: {0};{1}".format(file_name_validation_status,file_ext_validation_status)
        elif counter == 4 :
                validation_status = "FAILED: {0}".format(file_header_validation_status)
        elif counter == 5:
            validation_status = "FAILED: {0},{1}".format(file_name_validation_status,file_header_validation_status)
        elif counter == 6:
                validation_status = "FAILED: {0},{1}".format(file_ext_validation_status,file_header_validation_status)
        else :
                validation_status = "FAILED: {0};{1};{2}".format(file_name_validation_status,file_ext_validation_status,file_header_validation_status)
        
        return validation_status

    except Exception as e:
            # Handle exceptions here
            error_message = f"FAILED: {str(e)}"
            return error_message

# Function for Regional market
def rg_travel_validation(CURRENT_FILE):
    #assigning the value to varibale file
    
    if "CNSC" in CURRENT_FILE:
            fileA = CURRENT_FILE.replace(" ", "_")
            file = fileA.replace("_", "_",1)
            print("FileName : ", file)
    elif "Dufry" in CURRENT_FILE:
            fileA = CURRENT_FILE.replace(" ", "_")
            file = fileA.replace("_", " ",1)
            print("FileName : ", file)
    elif "Vendor" in CURRENT_FILE:
            fileA = CURRENT_FILE.replace(" ", "_")
            file = fileA.replace("_", "_",1)
            print("FileName : ", file)
    elif "LSTR" in CURRENT_FILE:
            file = CURRENT_FILE.replace(" ", "_")
            print("FileName : ", file)
    else:
            file = CURRENT_FILE
            print("FileName : ", file)

    return file

# Function for Thailand market
def thailand_processing(CURRENT_FILE):

    if "COP_1" in CURRENT_FILE:
        file= CURRENT_FILE.replace("_"," ",3)
        print("FileName : ", file)
    elif "Shelf" in CURRENT_FILE:
        file=CURRENT_FILE.replace("_"," ",5)
        print("FileName : ", file)
    elif "OSA" in CURRENT_FILE:
        file=CURRENT_FILE.replace("_"," ",4)
        print("FileName : ", file)
    else:
        file = CURRENT_FILE.replace(" ","_")
        print("FileName : ", file)

    return file

# Function for Pacific market
def aus_processing(CURRENT_FILE):
    if "Weekly_Sales_Report_-_Kenvue" in CURRENT_FILE:
        file= CURRENT_FILE.replace("_"," ",3)
        print("FileName : ", file)
    elif "FSSI_Week" in CURRENT_FILE:
        file= CURRENT_FILE.replace("_"," ",1)
        print("FileName : ", file)

    elif "All_J_J_Items_WE" in CURRENT_FILE:
        parts= CURRENT_FILE[:16].split(''_'')
        file_name = parts[0] + " " + parts[1]
        file_name += ''_'' + parts[2].replace(''_'', '' '', 1)
        file_name += '' '' + parts[3].replace(''_'', '''' '''', 1)
        file_name += '' '' + parts[4].replace(''_'', '''' '''', 1)
        file=file_name
        print("FileName : ", file)
    elif "Monthly_Sales_Report,_Supplier_filter_only_J_J" in CURRENT_FILE:
        file=CURRENT_FILE.replace(''_'', '' '', 5)   

    elif "Coles_09a__SOH_Trend_Detail_Report" in CURRENT_FILE:
        fileA=CURRENT_FILE.replace("_"," ")
        file=fileA.replace(" ","_",2)

    elif "Replenishment_E3_Buyers_Report" in CURRENT_FILE or "RE_2.0_API_Inventory_and_Sales_Report" in CURRENT_FILE or "Stock_Status_by_DC_-_13_Months_Sales" in CURRENT_FILE:
        file=CURRENT_FILE.replace("_"," ")
        
    else:
        file = CURRENT_FILE.replace(" ","_")
        print("FileName : ", file)

    return file

# Function for North Asia market
def north_asia_processing(CURRENT_FILE):

    if "NU_RI_ZON" in CURRENT_FILE:
        file=CURRENT_FILE.replace("_"," ",2)

    elif "KR_POS_GS_Super" in CURRENT_FILE:
        file=CURRENT_FILE.rsplit("_",2)[0]

    elif "LOTTE_LOGISTICS_YANG_JU" in CURRENT_FILE:
        file=CURRENT_FILE.replace("_"," ")

    else:
        file = CURRENT_FILE.replace(" ","_")
        print("FileName : ", file)
        
    return file

# Function for Indonesia market
def indonesia_processing(CURRENT_FILE):
    if "PROMO_COMPETITOR" in CURRENT_FILE or "BRAND_BLOCKING" in CURRENT_FILE or "VISIBILITY" in CURRENT_FILE or "PLANOGRAM" in CURRENT_FILE or "PRICING" in CURRENT_FILE or "SECONDARY_DISPLAY" in CURRENT_FILE or "PROMO" in CURRENT_FILE or "PRODUCT_AVAILABILITY" in CURRENT_FILE:
        file=CURRENT_FILE.replace("_"," ",1)
        print(file)

    else:
        file = CURRENT_FILE.replace(" ","_")
        print("FileName : ", file)

    return file

	# Function for Philippines market
def philippines_processing(CURRENT_FILE):

    if "Consumer" in CURRENT_FILE:
        file= CURRENT_FILE.replace(" ","_")
        print("FileName : ", file)
    elif "OTC" in CURRENT_FILE:
        file=CURRENT_FILE.replace(" ","_")
        print("FileName : ", file)   
    else:
        file = CURRENT_FILE
        print("FileName : ", file)

    return file 							  
    

# Function to Perform File name validation

def file_validation(counter,extracted_filename,val_file_name,schema):


        if "Coop_Sell_In" in extracted_filename:
            first_name = extracted_filename[:extracted_filename.rfind("_")]
            print("first_name:", first_name)
            date = extracted_filename[extracted_filename.rfind("_")+1:]
            print("extracted date:", date)
            total_filename = f"{first_name}_{date}"
            if regex.match("[a-zA-Z]{3}[0-9]{4}", date) and first_name.lower() == val_file_name.lower() and total_filename.lower() == extracted_filename.lower():
                 file_name_validation_status=""
            else:
                file_name_validation_status="Invalid File Name"
                counter=1

        elif "Target_Sell_In" in extracted_filename:
            first_name = extracted_filename[:extracted_filename.rfind("_")]
            date = extracted_filename[extracted_filename.rfind("_") + 1:]
            if len(date) == 7 and date[:3].isalpha() and date[3:].isdigit() and first_name.lower() == val_file_name.lower() :
                file_name_validation_status=""
            else :
                file_name_validation_status="Invalid File Name"
                counter=1
            
        elif "TW" in extracted_filename and "_JJ_KPI_Status" in extracted_filename:
            extracted_filename_year=extracted_filename.split("_")[1]
            extracted_filename_week=extracted_filename.split("_")[2][2:]
            extracted_filename_last="_"+("_").join(extracted_filename.split("_")[3:]).split(".")[0]
            extracted_filename_first=extracted_filename.split("_")[0]
            if extracted_filename_year.isnumeric() and extracted_filename_week.isnumeric() and extracted_filename_last=="_JJ_KPI_Status" and extracted_filename_first==val_file_name:
                file_name_validation_status=""
            else :
                file_name_validation_status="Invalid File Name"
                counter=1

        elif "Sellout_Superindo" in extracted_filename or "Stock_Superindo" in extracted_filename or "Sellout_Alfmidi" in extracted_filename or "Stock_Alfmidi" in extracted_filename or "Sellout_Alfamart" in extracted_filename or "Stock_Alfamart" in extracted_filename  or "Sellout_Carrefour" in extracted_filename or "Stock_Carrefour" in extracted_filename or "Diamond" in extracted_filename or "Stock_Guardian" in extracted_filename or "Indomaret" in extracted_filename or "Indogrosir" in extracted_filename or ("JNJ" in extracted_filename and schema=="NTASDL_RAW") or "6P_AI_Calculated" in extracted_filename:
            file_name_date_format=extracted_filename.rsplit("_",1)[1]
            if val_file_name.upper() == extracted_filename.rsplit("_",1)[0].upper():
                if len(file_name_date_format) == 6 and file_name_date_format.isdigit():
                    file_name_validation_status=""
                else:
                    file_name_validation_status="File Name Valid, Invalid Date format- "+ "Expected ''YYYYMM'' but received " + file_name_date_format
                    counter=1
            else:
                
                file_name_validation_status="Invalid File Name"
                counter=1

        elif "Matahari" in extracted_filename:
            file_name_date_format=extracted_filename.rsplit("_",1)[1]
            if val_file_name.upper() == extracted_filename.rsplit("_",2)[0].upper():
                if len(file_name_date_format) == 6 and file_name_date_format.isdigit():
                    file_name_validation_status=""

                else:
                    file_name_validation_status="File Name Valid, Invalid Date format- "+ "Expected ''YYYYMM'' but received " + file_name_date_format
                    counter=1
            else:
                
                file_name_validation_status="Invalid File Name"
                counter=1

        elif "Weekly_Summary_TCA_raw_data" in extracted_filename or "Weekly_Summary_Unitoa_raw_data" in extracted_filename or "SAP_BW_KR_LIST_PRICE" in extracted_filename:
            file_name_date_format=extracted_filename.rsplit("_",1)[1]
            if val_file_name.upper() == extracted_filename.rsplit("_",1)[0].upper():
                if len(file_name_date_format) == 8 and file_name_date_format.isdigit():
                    file_name_validation_status=""

                else:
                    file_name_validation_status="File Name Valid, Invalid Date format- "+ "Expected ''YYYYMMDD'' but received " + file_name_date_format
                    counter=1
            else:
                
                file_name_validation_status="Invalid File Name"
                counter=1


        elif "OKR_Alteryx" in extracted_filename:
            file_name_date_format=extracted_filename.rsplit("_",1)[1]
            if val_file_name.upper() == extracted_filename.rsplit("_",1)[0].upper():
                if len(file_name_date_format) == 14 and file_name_date_format.isdigit():
                    file_name_validation_status=""

                else:
                    file_name_validation_status="File Name Valid, Invalid Date format- "+ "Expected ''YYYYMMDDHHmmss'' but received " + file_name_date_format
                    counter=1
            else:
                
                file_name_validation_status="Invalid File Name"
                counter=1

        elif "KR_eCom_sku_level" in extracted_filename:
            file_length=len(extracted_filename)
            if len(extracted_filename)==26 and val_file_name.upper() == extracted_filename.rsplit("_",2)[0].upper():
                file_name_validation_status=""
                print("file_name_validation_status is successful")
            else:
                file_name_validation_status="Invalid File Name, Length of FileName did not match "+ "Expected ''26'' but received " + str(file_length)
                counter=1

        elif val_file_name.upper() == extracted_filename.upper():
            file_name_validation_status=""
            print("file_name_validation_status is successful")

        elif regex.match(val_file_name.upper(), extracted_filename.upper()):
            file_name_validation_status=""
            print("file_name_validation_status is successful")
            
        
        else:
            file_name_validation_status="Invalid File Name, received  " + extracted_filename+" while expecting " +val_file_name
            print("file_name_validation_status",file_name_validation_status)
            counter = 1
        return file_name_validation_status,counter
    

# Function to perform file extension validation

def file_extn_validation(counter,CURRENT_FILE,val_file_extn):
    
        current_file_extn = CURRENT_FILE.split(".")[-1]
        if current_file_extn.upper() == val_file_extn.upper():
            file_ext_validation_status=""
            print("file_ext_validation_status is successful")
        else:
            file_ext_validation_status="Invalid File Extension"
            print("file_ext_validation_status",file_ext_validation_status)
            counter = counter+2
        return file_ext_validation_status,counter

# Function to perform file header validation
def file_header_validation(counter,final_val_header,file_header, hreg):

        # Compare the header from file and the header from metadata
        # Get the count of both
        # Perform index matching and return output based on the checks
        # Moving the failed header to a list and displaying it as part of Error message
        
        file_header_rejected_list=[]
        val_rejected_list=[]
        index=[]
        extra_columns=[]

        file_header=[x.lower() for x in file_header]
        val_header_count=len(final_val_header)
        file_header_count=len(file_header)
    
        rindex = 0
        for i in range(max(file_header_count, val_header_count)):
            if i < file_header_count and i < val_header_count:
                if "{r}" in final_val_header[i]:
                    final_val_header[i] = final_val_header[i].replace("{r}", hreg[rindex])
                    if rindex < len(hreg)-1:
                        rindex += 1
                    if not regex.match(final_val_header[i],file_header[i]):
                        index.append(i+1)
                        file_header_rejected_list.append(file_header[i])
                elif file_header[i] != final_val_header[i]:
                    index.append(i+1)
                    file_header_rejected_list.append(file_header[i])               

            elif i < file_header_count:
                extra_columns.append(file_header[i])
            elif i < val_header_count:
                val_rejected_list.append(final_val_header[i])

            
            # Check if count matches and no value in rejected list
    
        if file_header_count==val_header_count and not file_header_rejected_list:
            file_header_validation_status="Success"
            print("file_header_validation_status is successful")

            # Return Fail message if value found in Rejected list and not in extra columns list
        elif len(file_header_rejected_list)!=0 and not extra_columns:
            file_header_validation_status="Header validation Failed; \\nunmatched columns are" + str(file_header_rejected_list) +"\\nExpected Header :"+str(final_val_header)+ "\\nReceived Header :" + str(file_header)
            #print("file_header_validation_status",file_header_validation_status)
            counter = counter+4

            # Return Fail message if values found in extra columns list
        elif len(extra_columns)!=0:
            file_header_validation_status="Header validation Failed; \\nunmatched columns are " + str(file_header_rejected_list) + "\\nextra columns found in file header! " + str(extra_columns) 
            #print("file_header_validation_status",file_header_validation_status)
            counter = counter+4

        else:
            file_header_validation_status="Header validation Failed, columns missing from file header!" + str(val_rejected_list)
            counter = counter+4

        
            
        return file_header_validation_status,counter';
