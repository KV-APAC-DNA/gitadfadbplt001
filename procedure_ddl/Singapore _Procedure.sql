------------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE "FILE_VALIDATION"("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them.

import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col
import math

def main(session: snowpark.Session,Param): 

    # Your code goes here, inside the "main" handler.
    # Return value will appear in the Results tab
    #Shilla_202201 Without2ndColumnHeaders SC8
    # ********   Variable  we need from ETL table : 
    # CURRENT_FILE , index , validation, val_file_name,val_file_extn

    CURRENT_FILE        =  Param[0]
    index               =  Param[1]
    validation          =  Param[2]
    val_file_name       =  Param[3]
    val_file_extn       =  Param[4]
    val_header          =  Param[5]
    file_header_row_num	=  Param[6]
    stage_name     		=  Param[7]
    temp_stage_path		=  Param[8]

    FileNameValidation,FileExtnValidation,FileHeaderValidation = validation.split("-")
    counter             =  0 

    # If the File belongs to Regional, then it enters the function

    if stage_name.split(".")[0]=="ASPSDL_RAW":
        CURRENT_FILE=rg_travel_validation(CURRENT_FILE)

    #Extracting the filename based on index variable
        
    if index.lower() == "last":
        extracted_filename = CURRENT_FILE.rsplit("_", 1)[0]
    elif index.lower() == "first":
        extracted_filename = CURRENT_FILE.split("_")[0]
    elif index.lower() == "full":
        extracted_filename = CURRENT_FILE.rsplit(".", 1)[0]


    # Check for File name Validation
    
    if FileNameValidation=="1":
        file_name_validation_status,counter=file_validation(counter,extracted_filename,val_file_name)
    else :
        print("File Name Validation not required")


    # Check for  File extension validation

    if FileExtnValidation == "1":
        file_ext_validation_status,counter=file_extn_validation(counter,CURRENT_FILE,val_file_extn)
    else:
        print("File extension Validation not required")


    # Check for File Header Validation
    
    if FileHeaderValidation == "1":

        # Converting the extension from xlsx to csv
        # Extracting the Header from the file
	
        file_name= CURRENT_FILE.replace("xlsx","csv")
        df = session.read.option("INFER_SCHEMA", True).option("field_optionally_enclosed_by", "\\"").csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
        
        df_pandas=df.to_pandas()
        header=df_pandas.iloc[int(file_header_row_num)].tolist()

        # If the source is of xlsx type, then splitting based on \\x01 delimiter
        
        if val_file_extn==''xlsx'':
            result_list = header[0].split(''\\x01'')
        else:
            result_list = header
        result_list=list(filter(None,result_list))
        filtered_list = [value for value in result_list if value is not None and not (isinstance(value, float) and math.isnan(value))]
        file_header= [item.replace(" ", "_").replace(".", "_") for item in filtered_list]
        val_header= val_header.lower()

        # If the Header from Metadata is of comma separated or | separated then split accordingly
        
        comma_split = val_header.split('','')
        if len(comma_split) > 1:
            final_val_header=comma_split

        pipe_split = val_header.split(''|'')
        if len(pipe_split) > 1:
            final_val_header=pipe_split
        
        file_header_validation_status,counter=file_header_validation(counter,final_val_header,file_header)
        
    else:
        print("File Header Validation not required")


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


# Function to Perform File name validation

def file_validation(counter,extracted_filename,val_file_name):

        if extracted_filename.upper() == val_file_name.upper():
            file_name_validation_status=""
            print("file_name_validation_status is successful")
        else:
            file_name_validation_status="Invalid File Name"
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

def file_header_validation(counter,final_val_header,file_header):

        # Compare the header from file and the header from metadata
        # Adding the counter flag as a double check to the comparision
        # Moving the failed header to a list and displaying it as part of Error message
    
        val_header_count=len(final_val_header)
        file_header_count=0
        l3=[]
        l2=[x.lower() for x in final_val_header]
        for i in file_header:
            if i.lower() in l2:
                file_header_count+=1
            else:
                l3.append(i)
        if file_header_count == val_header_count and not l3:
            file_header_validation_status="Success"
            print("file_header_validation_status is successful")
        else:
            file_header_validation_status="Header validation Failed"+" and the unmatched columns are "+ str(l3)
            print("file_header_validation_status",file_header_validation_status)
            counter = counter+4
        return file_header_validation_status,counter


            





    
        

    ';

		
------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE "SG_AMAZON_PREPROCESSING"("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS 'from snowflake.snowpark.functions import col,lit,date_format,current_timestamp,to_date,year,month,concat,format_number,to_timestamp
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField,DecimalType
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col
import pytz

def main(session: snowpark.Session,Param): 
    try:
        #Param= [''Amazon_202312.csv'',''SGPSDL_RAW.DEV_LOAD_STAGE_ADLS'',''dev'',''sdl_sg_scan_data_amazon'']
        
    # Extracting parameters from the input
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
    
        df_schema = StructType([
            StructField("rm", StringType()),
            StructField("merchant_customer_id", StringType()),
            StructField("gl", StringType()),
            StructField("category", StringType()),
            StructField("subcategory", StringType()),
            StructField("brand", StringType()),
            StructField("item_code", StringType()),
            StructField("item_desc", StringType()),
            StructField("net_sales", DecimalType(10, 4)),
            StructField("pcogs", DecimalType(10, 4)),
            StructField("SALES_QTY", DecimalType(10, 0)),
            StructField("ppmpercent", DecimalType(10, 5)),
            StructField("ppmdollar", DecimalType(10, 5)),
            StructField("month", IntegerType()),
            StructField("year", IntegerType()),
            StructField("vendor_code", StringType()),
            StructField("vendor_name", StringType())
        ]
        )
    
        df = session.read\\
        .schema(df_schema)\\
        .option("skip_header",1)\\
        .option("field_delimiter", "\\u0001")\\
        .option("field_optionally_enclosed_by", "\\"") \\
        .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
        

        df=df.with_column("trx_date",lit(None).cast("date"))
        df=df.with_column("store",lit("Amazon"))
        df=df.with_column("store_name",lit("Amazon"))
        df=df.with_column("cdl_dttm",lit(None))
        #convertin time stamp into sg timezone
        df=df.with_column("crtd_dttm",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        df=df.with_column("file_name",lit(file_name))
        df=df.with_column("run_id",lit(None))
        df=df.with_column("run_id",col("run_id").cast(DecimalType(14,0)))

        #deleteing the data from table 

        del_sql = "DELETE FROM " + stage_name.split(".")[0]+"."+target_table
        session.sql(del_sql).collect()
    

        snowdf=df.select("trx_date","rm","merchant_customer_id","gl","category","subcategory","brand","item_code","item_desc","net_sales","pcogs","sales_qty","ppmpercent","ppmdollar","month","year","vendor_code","vendor_name","store","store_name","cdl_dttm","crtd_dttm","file_name","run_id")
        snowdf= snowdf.filter(snowdf["item_code"].isNotNull())
        if snowdf.count()==0 :
            return "No Data in file" 

        #move file into success folder
        file_name=file_name.split(".")[0]+"_"+datetime.now().strftime("%Y%m%d%H%M%S")
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

-----------------------------------------------------
CREATE OR REPLACE PROCEDURE "SG_DFI_PREPROCESSING"("PARAM" ARRAY)
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
    try: 
        #Param=["DFI_202312_new.csv",''SGPSDL_RAW.DEV_LOAD_STAGE_ADLS'',''dev/scan360/dfi'',''sdl_sg_scan_data_dfi'']
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        
        df_schema = StructType([
            StructField("TRXDATE", StringType()),
            StructField("BUYERCODE", StringType()),
            StructField("VENDORCODE", StringType()),
            StructField("STORECODE", StringType()),
            StructField("STORESHORTCODE", StringType()),
            StructField("STOREDESC", StringType()),
            StructField("BRAND", StringType()),
            StructField("ITEMCODE", StringType()),
            StructField("SUPPLIERITEMCODE", StringType()),
            StructField("ITEMDESC", StringType()),
            StructField("SIZE", StringType()),
            StructField("UOM", StringType()),
            StructField("PUF", StringType()),
            StructField("BARCODE", StringType()),
            StructField("SALESAMOUNT", DecimalType(10, 2)),  
            StructField("SALESQTY", DecimalType(10, 2))
        ])
        df = session.read\\
        .schema(df_schema)\\
        .option("skip_header",1)\\
        .option("field_delimiter", ",")\\
        .option("field_optionally_enclosed_by", "\\"") \\
        .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)


        df = df.withColumn("CUST_NAME", when(col("STORECODE").like("G%"),"Giant").otherwise(
                                      when(col("STORECODE").like("Q%"), "Cold Storage").otherwise("711")))
        df = df.with_column("cdl_dttm", lit(None))
        df = df.with_column("run_id", lit(None).cast(DecimalType(14,0))) 
        df=df.with_column("file_name",lit(file_name))
        df=df.with_column("crtd_dttm",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        df = df.withColumn("TRXDATE", regexp_replace(col("TRXDATE"), "\\\\.", "-").cast(DateType()))
        
        
        
        

        #deleteing the data from table 

        del_sql = "DELETE FROM " + target_table 
        session.sql(del_sql).collect()
    

        snowdf=df.select("trxdate", "buyercode", "vendorcode", "storecode", "storeshortcode", "storedesc", "brand", "itemcode", "supplieritemcode", "itemdesc", "size", "uom", "puf", "barcode", "salesamount", "salesqty", "CUST_NAME", "cdl_dttm", "crtd_dttm", "file_name", "run_id")
        
        snowdf= snowdf.filter(snowdf["itemcode"].isNotNull())
        if snowdf.count()==0 :
            return "No Data in file"
        
        #move file into success folder
        file_name=file_name.split(".")[0]+"_"+datetime.now().strftime("%Y%m%d%H%M%S")
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
        return error_message';
		
--------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE "SG_GUARDIAN_PREPROCESSING"("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them.

from snowflake.snowpark.functions import col, lit, date_format, current_timestamp, to_date, year, month, concat, format_number, to_timestamp
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DecimalType,DateType
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz

def main(session: snowpark.Session, Param):
    try:
        #Param=["Guardian_202312.csv", "SBX_DNA_LAB1.NBANGA01_RAW.DEV_LOAD_STAGE_ADLS_2","test5",s]
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        
        # Define the schema
        df_schema = StructType([
				StructField("trxdate", DateType()),
				StructField("buyercode", StringType()),
				StructField("vendorcode", StringType()),
				StructField("storecode", StringType()),
				StructField("storeshortcode", StringType()),
				StructField("storepostalcode", StringType()),
				StructField("storeaddress1", StringType()),
				StructField("storeaddress2", StringType()),
				StructField("storeaddress3", StringType()),
				StructField("storecountry", StringType()),
				StructField("storedesc", StringType()),
				StructField("brand", StringType()),
				StructField("itemcode", StringType()),
				StructField("supplieritemcode", StringType()),
				StructField("itemdesc", StringType()),
				StructField("size", StringType()),
				StructField("uom", StringType()),
				StructField("puf", DecimalType(10, 0)),
				StructField("salesqty", DecimalType(10, 0)),
				StructField("salesamount", StringType()),
				StructField("inventoryonhand", DecimalType(10, 0)),
				StructField("barcode", StringType()),
				StructField("barcode2", StringType()),
				StructField("barcode3", StringType()),
				StructField("barcode4", StringType()),
				StructField("barcode5", StringType()),
				StructField("barcode6", StringType()),
				StructField("barcode7", StringType()),
				StructField("barcode8", StringType()),
				StructField("barcode9", StringType()),
				StructField("barcode10", StringType())
                ])
        
        df = session.read \\
            .schema(df_schema)\\
            .option("skip_header", 1)\\
            .option("field_delimiter", ",")\\
            .option("field_optionally_enclosed_by", "\\"") \\
            .csv("@" + stage_name + "/" + temp_stage_path + "/" + file_name)

        df = df.withColumn("CUST_NAME", lit("Guardian").cast("string"))
        df = df.with_column("cdl_dttm", lit(None).cast("string"))
        df=df.with_column("crtd_dttm",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        df=df.with_column("file_name",lit(file_name).cast("string"))
        df = df.with_column("run_id", lit(None).cast(DecimalType(14,0)))

        #deleteing the data from table 

        del_sql = "DELETE FROM " + stage_name.split(".")[0]+"."+target_table
        session.sql(del_sql).collect()


        snowdf=df.select("trxdate","buyercode","vendorcode","storecode","storeshortcode","storepostalcode","storeaddress1","storeaddress2","storeaddress3","storecountry","storedesc","brand","itemcode","supplieritemcode","itemdesc","size","uom","puf","salesqty","salesamount","inventoryonhand","barcode","CUST_NAME","cdl_dttm","crtd_dttm","file_name","run_id")
        snowdf= snowdf.filter(snowdf["storecode"].isNotNull())
        if snowdf.count()==0 :
            return "No Data in file"
        
        #move file into success folder
        file_name=file_name.split(".")[0]+"_"+datetime.now().strftime("%Y%m%d%H%M%S")
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
        return error_message';
		
---------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE "SG_MARKETPLACE_PREPROCESSING"("PARAM" ARRAY)
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
from snowflake.snowpark.functions import col,lit,date_format,current_timestamp,to_timestamp
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField,DecimalType,DateType
import pandas as pd
from datetime import datetime
import pytz

def main(session: snowpark.Session,Param):

    try:
        # Extracting parameters from the input
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]

        # Define the schema for the DataFrame
        df_schema=StructType([
            StructField("CHANNEL",StringType()),
            StructField("MONTH",StringType()),
            StructField("ORDER_CREATION_DATE",DateType()),
            StructField("INVOICE_NUMBER",StringType()),
            StructField("STATUS",StringType()),
            StructField("ITEM_CODE",StringType()),
            StructField("ITEM_NAME",StringType()),
            StructField("SALES_UNIT",IntegerType()),
            StructField("NET_INVOICED_SALES",DecimalType(precision=38, scale=4)),
            StructField("BRAND",StringType()),
            ])

        # Read the CSV file into a DataFrame
    
        dataframe = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "\\u0001")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
    

    #---------------------------Transformation logic ------------------------------
    
    
    # Add "CUST_NAME", "CDL_DTTM", "CRTD_DTTM", "FILE_NAME" to the Dataframe
    
        dataframe = dataframe.withColumn("CUST_NAME",lit("Marketplace").cast("string"))
        dataframe = dataframe.withColumn("CDL_DTTM",lit(None).cast("string"))
        
        #convertin time stamp into sg timezone
        dataframe = dataframe.with_column("CRTD_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        dataframe = dataframe.withColumn("FILE_NAME",lit(file_name).cast("string"))
        dataframe =  dataframe.withColumn("RUN_ID",lit(None).cast(DecimalType(14,0)))
    
    # Creating copy of the Dataframe
        final_df = dataframe.alias("final_df")

        if final_df.count()==0:
            return "No Data in file"

        
        final_df=final_df.filter(col("INVOICE_NUMBER").is_not_null()) and final_df.filter(col("ITEM_NAME").is_not_null())
        
    # Load Data to the target table
        final_df.write.mode("overwrite").saveAsTable(target_table)

    
    # write to success folder
    
        file_name=file_name.split(".")[0]+''_''+datetime.now().strftime("%Y%m%d%H%M%S")
        final_df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/success/"+file_name,file_format_type="csv",header=True,OVERWRITE=True)

    
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
	
---------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE "SG_NTUC_PREPROCESSING"("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS 'from snowflake.snowpark.functions import col, lit, date_format, current_timestamp, to_date, year, month, concat, format_number, to_timestamp,when,trim,upper
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DecimalType,DateType
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz

def main(session: snowpark.Session,Param):
    try:
        #Param=["NTUC_202112.csv", "SBX_DNA_LAB1.NBANGA01_RAW.DEV_LOAD_STAGE_ADLS_2","test5","s"]
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        
        # Define the schema
        df_schema = StructType([
                StructField("Vendor_Code", StringType()),
                StructField("Vendor_Name", StringType()),
                StructField("Dept_Code", StringType()),
                StructField("Dept_Description", StringType()),
                StructField("Class_No", StringType()),
                StructField("Class_Description", StringType()),
                StructField("Sub_Class_Description", StringType()),
                StructField("MCH", StringType()),
                StructField("ITEM_CODE", StringType()),
                StructField("ITEM_DESC", StringType()),
                StructField("Brand", StringType()),
                StructField("Sales_UOM", StringType()),
                StructField("Pack_Size", DecimalType(10,0)),
                StructField("Store_Code", StringType()),
                StructField("Store_Name", StringType()),
                StructField("Store_Format", StringType()),
                StructField("Attribute1", StringType()),
                StructField("Attribute2", StringType()),
                StructField("TRX_DATE", DateType()),
                StructField("Value", DecimalType(38, 8))  
        ])

        df = session.read\\
        .schema(df_schema)\\
        .option("skip_header",1)\\
        .option("field_delimiter", "\\u0001")\\
        .option("field_optionally_enclosed_by", "\\"") \\
        .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        #pivoting using when statement 

        df = df.withColumn("net_sales", 
                   when((trim(upper(df["Attribute1"])) == "SALES"), col("value"))
                   .otherwise(None)
                   .cast(DecimalType(14, 4)))

        df = df.withColumn("SALES_QTY", 
                   when((trim(df["Attribute1"]) == "Qty (in EA)"), col("value"))
                   .cast(DecimalType(10, 0)))

        df=df.with_column("file_name",lit(file_name))
        df=df.with_column("CUST_NAME",lit("NTUC"))
        #convertin time stamp into sg timezone
        df=df.with_column("crtd_dttm",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        df = df.with_column("cdl_dttm", lit(None))
        df = df.with_column("run_id", lit(None).cast(DecimalType(14,0)))
       
        #deleteing the data from table 

        del_sql = "DELETE FROM " + stage_name.split(".")[0]+"."+target_table
        session.sql(del_sql).collect()
    

        snowdf=df.select("Vendor_Code", "Vendor_Name", "Dept_Code", "Dept_Description", "Class_No", "Class_Description", "Sub_Class_Description", "MCH", "ITEM_CODE", "ITEM_DESC", "Brand", "Sales_UOM", "Pack_Size", "Store_Code", "Store_Name", "Store_Format", "Attribute1", "Attribute2", "TRX_DATE", "NET_SALES", "SALES_QTY", "CUST_NAME", "cdl_dttm", "crtd_dttm", "file_name", "run_id")
        
        snowdf= snowdf.filter(snowdf["item_code"].isNotNull())
        if snowdf.count()==0 :
            return "No Data in file"
        #move file into success folder
        file_name=file_name.split(".")[0]+"_"+datetime.now().strftime("%Y%m%d%H%M%S")
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

------------------------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE "SG_REDMART_PREPROCESSING"("PARAM" ARRAY)
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
from snowflake.snowpark.functions import col,lit,date_format,current_timestamp,to_timestamp
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField,DecimalType,DateType
import pandas as pd
from datetime import datetime
import pytz

def main(session: snowpark.Session,Param):

    try:

        # Extracting parameters from the input
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]

        # Define the schema for the DataFrame
        df_schema=StructType([
            StructField("TRX_DATE",DateType()),
            StructField("ITEM_CODE",StringType()),
            StructField("PRODUCT_CODE",StringType()),
            StructField("ITEM_DESC",StringType()),
            StructField("PACKSIZE",StringType()),
            StructField("BRAND",StringType()),
            StructField("SUPPLIER_ID",StringType()),
            StructField("SUPPLIER_NAME",StringType()),
            StructField("MANUFACTURER",StringType()),
            StructField("CATEGORY_1",StringType()),
            StructField("CATEGORY_2",StringType()),
            StructField("CATEGORY_3",StringType()),
            StructField("CATEGORY_4",StringType()),
            StructField("GMV",DecimalType(precision=38, scale=4)),
            StructField("UNITS_SOLD",DecimalType(precision=38, scale=0))
            
            ])

        # Read the CSV file into a DataFrame
    
        dataframe = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "\\u0001")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)


         #---------------------------Transformation logic ------------------------------
    
    
        # Add "STORE", "STORE_NAME","CDL_DTTM", "CRTD_DTTM", "FILE_NAME" to the Dataframe

        dataframe = dataframe.withColumn("STORE",lit("Redmart").cast("string"))
        dataframe = dataframe.withColumn("STORE_NAME",lit("Redmart").cast("string"))
        dataframe = dataframe.withColumn("CDL_DTTM",lit(None).cast("string"))

        #convertin time stamp into sg timezone
        dataframe = dataframe.with_column("CRTD_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        dataframe = dataframe.withColumn("FILE_NAME",lit(file_name).cast("string"))
        dataframe = dataframe.withColumn("RUN_ID",lit(None).cast(DecimalType(14,0)))
        

        # Creating copy of the Dataframe
        final_df = dataframe.alias("final_df")

        if final_df.count()==0:
            return "No Data in file"

        final_df=final_df.filter(col("ITEM_CODE").is_not_null()) and final_df.filter(col("PRODUCT_CODE").is_not_null())

        
        # Load Data to the target table
        final_df.write.mode("overwrite").saveAsTable(target_table)

    
        # write to success folder
    
        file_name=file_name.split(".")[0]+''_''+datetime.now().strftime("%Y%m%d%H%M%S")
        final_df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/success/"+file_name,file_format_type="csv",header=True,OVERWRITE=True)
        
        


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

---------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE "SG_SCOMMERCE_PREPROCESSING"("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS 'from snowflake.snowpark.functions import col, lit, date_format, current_timestamp, to_date, year, month, concat, format_number, to_timestamp
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DecimalType,DateType
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz

def main(session: snowpark.Session,Param): 
    try:
        #Param=[''Scommerce_20220.csv'',''SGPSDL_RAW.DEV_LOAD_STAGE_ADLS'',''dev'',''sdl_sg_scan_data_scommerce'']
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
    
        df_schema = StructType([
            StructField("date_id", DateType()),
            StructField("ordersn", StringType()),
            StructField("itemid", StringType()),
            StructField("modelid", StringType()),
            StructField("sku_id", StringType()),
            StructField("item_name", StringType()),
            StructField("model_name", StringType()),
            StructField("sales_qty", DecimalType(10,0)),
            StructField("net_sales", DecimalType(10,6)),
        ])
        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header", 1)\\
            .option("field_delimiter", "\\u0001")\\
            .option("field_optionally_enclosed_by", "\\"") \\
            .csv("@" + stage_name + "/" + temp_stage_path + "/" + file_name)

        df = df.with_column("store", lit("Scommerce"))
        df = df.with_column("store_name", lit("Scommerce"))
        df = df.with_column("cdl_dttm", lit(None))
        df = df.with_column("crtd_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        df = df.with_column("file_name", lit(file_name))
        df = df.with_column("run_id", lit(None).cast(DecimalType(10,0))) 

        #deleteing the data from table 

        del_sql = "DELETE FROM " + stage_name.split(".")[0]+"."+target_table
        session.sql(del_sql).collect()
    

        snowdf=df.select("date_id", "ordersn", "itemid", "modelid", "sku_id", "item_name", "model_name", "sales_qty", "net_sales", "store", "store_name", "cdl_dttm" , "crtd_dttm", "file_name", "run_id")
        
        snowdf= snowdf.filter(snowdf["itemid"].isNotNull())
        if snowdf.count()==0 :
            return "No Data in file"
        
        #move file into success folder
        file_name=file_name.split(".")[0]+"_"+datetime.now().strftime("%Y%m%d%H%M%S")
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


-----------------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE "SG_WATSONS_PREPROCESSING"("PARAM" ARRAY)
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
    
    #Param=["SG_SCAN_DATA_WATSONS_Apr16.csv", "SBX_DNA_LAB1.NBANGA01_RAW.DEV_LOAD_STAGE_ADLS_2","test5","s"]
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]

        
        df_schema = StructType([
            StructField("year", IntegerType()),
            StructField("week", IntegerType()),
            StructField("store", StringType()),
            StructField("div", StringType()),
            StructField("prdt_dept", StringType()),
            StructField("prdtcode", StringType()),
            StructField("prdtdesc", StringType()),
            StructField("brand", StringType()),
            StructField("supcode", StringType()),
            StructField("sup_name", StringType()),
            StructField("barcode", StringType()),
            StructField("sup_cat", StringType()),
            StructField("dept_name", StringType()),
            StructField("net_sales", StringType()),
            StructField("sales_qty", DecimalType(10, 0))
        ])

        df = session.read\\
        .schema(df_schema)\\
        .option("skip_header",1)\\
        .option("field_delimiter", ",")\\
        .option("field_optionally_enclosed_by", "\\"") \\
        .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)


        df = df.with_column("run_id", lit(None).cast(DecimalType(14,0))) 
        df=df.with_column("file_name",lit(file_name))
        df=df.with_column("crtd_dttm",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        df = df.with_column("cdl_dttm", lit(None))
        df = df.with_column("cust_name", lit("Watsons"))
    
        #deleteing the data from table 

        del_sql = "DELETE FROM " + stage_name.split(".")[0]+"."+target_table
        session.sql(del_sql).collect()
    

        snowdf=df.select("year", "week", "store", "div", "prdt_dept", "prdtcode", "prdtdesc", "brand", "supcode", "sup_name", "barcode", "sup_cat", "dept_name", "net_sales", "sales_qty", "CUST_NAME", "cdl_dttm", "crtd_dttm", "file_name", "run_id")
        
        snowdf= snowdf.filter(snowdf["year"].isNotNull())
        if snowdf.count()==0 :
            return "No Data in file"
        
        #move file into success folder
        file_name=file_name.split(".")[0]+"_"+datetime.now().strftime("%Y%m%d%H%M%S")
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
        return error_message';
