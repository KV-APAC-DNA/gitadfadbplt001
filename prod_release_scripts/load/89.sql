CREATE OR REPLACE PROCEDURE THASDL_RAW.FILE_VALIDATION("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('regex==2023.10.3','snowflake-snowpark-python==*')
HANDLER = 'main'
EXECUTE AS OWNER
AS '

import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col
import math
import regex

def main(session: snowpark.Session,Param): 
    try:


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

        FileNameValidation,FileExtnValidation,FileHeaderValidation = validation.split("-")
        counter             =  0 

    
        
    
        if stage_name.split(".")[0]=="ASPSDL_RAW":
            processed_file_name=rg_travel_validation(CURRENT_FILE)
            
        
        elif stage_name.split(".")[0]=="THASDL_RAW":
           processed_file_name=thailand_processing(CURRENT_FILE)

        else:
            processed_file_name=CURRENT_FILE

    
        
        
        if index.lower() == "last":
            extracted_filename = processed_file_name.rsplit("_", 1)[0]
        elif index.lower() == "first":
            extracted_filename = processed_file_name.split("_")[0]
        elif index.lower() == "full":
            extracted_filename = processed_file_name.rsplit(".", 1)[0]
    
    
        
    
        if FileNameValidation=="1":
            file_name_validation_status,counter=file_validation(counter,extracted_filename,val_file_name)
        else :
            print("File Name Validation not required")
    
    
        
    
        if FileExtnValidation == "1":
            file_ext_validation_status,counter=file_extn_validation(counter,CURRENT_FILE,val_file_extn)
        else:
            print("File extension Validation not required")
    
    
        # Check for File Header Validation
    
        if FileHeaderValidation == "1":

            
            if "CRM_Children" in CURRENT_FILE or "TH_CRM_Consumer" in CURRENT_FILE:
                utf_encoding= ''UTF-16''
            else:
                utf_encoding= ''UTF-8''
            
            if "NTUC" in CURRENT_FILE:
                
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
                
            else:
                file_name= CURRENT_FILE.replace("xlsx","csv")
                file_name = file_name.replace("(", "").replace(")", "").replace(" ","_")
                df = session.read.option("INFER_SCHEMA", True).option("field_optionally_enclosed_by", "\\"").option("encoding",utf_encoding).csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
            
                df_pandas=df.to_pandas()
                header=df_pandas.iloc[int(file_header_row_num)].tolist()
                    
                

            

            header_pipe_split = header[0].split(''|'')
            if val_file_extn==''xlsx'':
                result_list = header[0].split(''\\x01'')
            elif len(header_pipe_split)>1:
                result_list = header_pipe_split
            else:
                result_list = header
            
            result_list=list(filter(None,result_list))
            filtered_list = [value for value in result_list if value is not None and not (isinstance(value, float) and math.isnan(value))]
            file_header= [item.replace(" ", "_").replace(".", "_") for item in filtered_list]
            val_header= val_header.lower()

            
            
            comma_split = val_header.split('','')
            if len(comma_split) > 1:
                final_val_header=comma_split
    
            pipe_split = val_header.split(''|'')
            if len(pipe_split) > 1:
                final_val_header=pipe_split

            header_reg = header_reg.lower()
            regex_list = header_reg.split(''^'')
            
            file_header_validation_status,counter=file_header_validation(counter,final_val_header,file_header, regex_list)
        
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

    except Exception as e:
            
            error_message = f"FAILED: {str(e)}"
            return error_message


def rg_travel_validation(CURRENT_FILE):
    
    
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
    elif "TH_Action" in CURRENT_FILE:
        split_name=CURRENT_FILE.split("_")
        file= ("_").join(split_name[0:4])+"."+CURRENT_FILE.split(".")[-1]
    elif "LAO" in CURRENT_FILE or "Schedule" in CURRENT_FILE or "Visit" in CURRENT_FILE:
        file=CURRENT_FILE.rsplit("_",1)[0]
        print("FileName : ", file)
    else:
        file = CURRENT_FILE.replace(" ","_")
        print("FileName : ", file)

    return file




def file_validation(counter,extracted_filename,val_file_name):

        if val_file_name.upper() == extracted_filename.upper():
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

def file_header_validation(counter,final_val_header,file_header, hreg):

        
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
            
            
    
        if file_header_count==val_header_count and not file_header_rejected_list:
            file_header_validation_status="Success"
            print("file_header_validation_status is successful")

            
        elif len(file_header_rejected_list)!=0 and not extra_columns:
            file_header_validation_status="Header validation Failed"+" , unmatched columns found in index "+ str(index) +" and columns are" + str(file_header_rejected_list) + " expected "+str(final_val_header)+ " received " + str(file_header)
            print("file_header_validation_status",file_header_validation_status)
            counter = counter+4

            
        elif len(extra_columns)!=0:
            file_header_validation_status="Header validation Failed, unmatched columns found in index " + str(index) + " and columns are " + str(file_header_rejected_list) + " ; extra columns found in file header! " + str(extra_columns) 
            print("file_header_validation_status",file_header_validation_status)
            counter = counter+4

        else:
            file_header_validation_status="Header validation Failed, columns missing from file header!" + str(val_rejected_list)
            counter = counter+4
        
            
        return file_header_validation_status,counter

';


use schema thasdl_raw;

CREATE OR REPLACE PROCEDURE SDL_TH_MT_MAKRO("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '

from snowflake.snowpark.functions import col, lit, current_timestamp, trim, upper
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DecimalType,DateType
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark

def main(session:snowpark.Session, Param):

    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]

        df_schema = StructType([
            StructField("SUPPLIER_NUMBER", StringType()),
            StructField("LOCATION_NUMBER", StringType()),
            StructField("LOCATION_NAME", StringType()),
            StructField("CLASS_NUMBER", StringType()),
            StructField("SUBCLASS_NUMBER", StringType()),
            StructField("ITEM_NUMBER", StringType()),
            StructField("BARCODE", StringType()),
            StructField("ITEM_DESC", StringType()),
            StructField("EOH_QTY", StringType()),
            StructField("ORDER_IN_TRANSIT_QTY", StringType()),
            StructField("PACK_TYPE", StringType()),
            StructField("MAKRO_UNIT", StringType()),
            StructField("AVG_NET_SALES_QTY", StringType()),
            StructField("NET_SALES_QTY_YTD", StringType()),
            StructField("LAST_RECV_DT", StringType()),
            StructField("LAST_SOLD_DT", StringType()),
            StructField("STOCK_COVER_DAYS", StringType()),
            StructField("NET_SALES_QTY_MTD", StringType()),
            StructField("DAY_1", StringType()),
            StructField("DAY_2", StringType()),
            StructField("DAY_3", StringType()),
            StructField("DAY_4", StringType()),
            StructField("DAY_5", StringType()),
            StructField("DAY_6", StringType()),
            StructField("DAY_7", StringType()),
            StructField("DAY_8", StringType()),
            StructField("DAY_9", StringType()),
            StructField("DAY_10", StringType()),
            StructField("DAY_11", StringType()),
            StructField("DAY_12", StringType()),
            StructField("DAY_13", StringType()),
            StructField("DAY_14", StringType()),
            StructField("DAY_15", StringType()),
            StructField("DAY_16", StringType()),
            StructField("DAY_17", StringType()),
            StructField("DAY_18", StringType()),
            StructField("DAY_19", StringType()),
            StructField("DAY_20", StringType()),
            StructField("DAY_21", StringType()),
            StructField("DAY_22", StringType()),
            StructField("DAY_23", StringType()),
            StructField("DAY_24", StringType()),
            StructField("DAY_25", StringType()),
            StructField("DAY_26", StringType()),
            StructField("DAY_27", StringType()),
            StructField("DAY_28", StringType()),
            StructField("DAY_29", StringType()),
            StructField("DAY_30", StringType()),
            StructField("DAY_31", StringType())
        ])


        df = session.read\\
        .schema(df_schema)\\
        .option("skip_header",1)\\
        .option("field_delimiter", ''\\x01'')\\
        .option("encoding", "UTF-8")\\
        .option("field_optionally_enclosed_by", "\\"")\\
        .option("null_if", "")\\
        .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        file_date_part = file_name[6:12]  # This will extract "202108"
        df = df.with_column("TRANSACTION_DATE", lit(datetime.strptime(file_date_part, "%Y%m"))) \\
               .with_column("FILE_NAME", lit(file_name)) \\
               .with_column("CRTD_DTM", lit(datetime.now().strftime("%Y%m%d%H%M%S")))


        snowdf = df.select(
            "TRANSACTION_DATE",
            "SUPPLIER_NUMBER",
            "LOCATION_NUMBER",
            "LOCATION_NAME",
            "CLASS_NUMBER",
            "SUBCLASS_NUMBER",
            "ITEM_NUMBER",
            "BARCODE",
            "ITEM_DESC",
            "EOH_QTY",
            "ORDER_IN_TRANSIT_QTY",
            "PACK_TYPE",
            "MAKRO_UNIT",
            "AVG_NET_SALES_QTY",
            "NET_SALES_QTY_YTD",
            "LAST_RECV_DT",
            "LAST_SOLD_DT",
            "STOCK_COVER_DAYS",
            "NET_SALES_QTY_MTD",
            "DAY_1",
            "DAY_2",
            "DAY_3",
            "DAY_4",
            "DAY_5",
            "DAY_6",
            "DAY_7",
            "DAY_8",
            "DAY_9",
            "DAY_10",
            "DAY_11",
            "DAY_12",
            "DAY_13",
            "DAY_14",
            "DAY_15",
            "DAY_16",
            "DAY_17",
            "DAY_18",
            "DAY_19",
            "DAY_20",
            "DAY_21",
            "DAY_22",
            "DAY_23",
            "DAY_24",
            "DAY_25",
            "DAY_26",
            "DAY_27",
            "DAY_28",
            "DAY_29",
            "DAY_30",
            "DAY_31",
            "FILE_NAME",
            "CRTD_DTM"
        )

        # Check if DataFrame is empty
		snowdf=snowdf.na.drop("all")
        if snowdf.count() == 0:
            return "No Data in table"

        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")

        
        # Archive
        file_name1=("_").join(file_name.split("_")[0:5])+"_"+datetime.now().strftime("%Y%m%d%H%M%S")
        snowdf.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name1,header=True,OVERWRITE=True)
        
        # Write operation
        snowdf.write.mode("append").save_as_table(stage_name.split(".")[0]+"."+target_table)
        
        # Success message
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
CREATE OR REPLACE PROCEDURE SDL_TH_MT_WATSONS("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '
from snowflake.snowpark.functions import col, lit, current_timestamp, trim, upper
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DecimalType,DateType
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
def main(session:snowpark.Session, Param):
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        df_schema = StructType([
            StructField("DIV", StringType()),
            StructField("DEPT", StringType()),
            StructField("CLASS", StringType()),
            StructField("SUBCLASS", StringType()),
            StructField("ITEM", StringType()),
            StructField("ITEM_DESC", StringType()),
            StructField("NON_SLOW", StringType()),
            StructField("NON_SLOW2", StringType()),
            StructField("FINANCE_STATUS", StringType()),
            StructField("CREATE_DATETIME", StringType()),
            StructField("PRIM_SUPPLIER", StringType()),
            StructField("OLD_SUPP_NO", StringType()),
            StructField("SUPP_DESC", StringType()),
            StructField("LEAD_TIME", StringType()),
            StructField("UNIT_COST", StringType()),
            StructField("UNIT_RETAIL_ZONE5", StringType()),
            StructField("ITEM_STATUS", StringType()),
            StructField("STATUS_WH", StringType()),
            StructField("STATUS_WH_UPDATE_DATE", StringType()),
            StructField("STATUS_STORE", StringType()),
            StructField("STATUS_STORE_UPDATE_DATE", StringType()),
            StructField("STATUS_XDOCK", StringType()),
            StructField("STATUS_XDOCK_UPDATE_DATE", StringType()),
            StructField("SOURCE_METHOD", StringType()),
            StructField("SOURCE_WH", StringType()),
            StructField("POG", StringType()),
            StructField("PRODUCT_TYPE", StringType()),
            StructField("LABEL_UDA", StringType()),
            StructField("BRAND", StringType()),
            StructField("ITEM_TYPE", StringType()),
            StructField("RETURN_POLICY", StringType()),
            StructField("RETURN_TYPE", StringType()),
            StructField("WH_WAC", StringType()),
            StructField("IN_TAX", StringType()),
            StructField("TAX_RATE", StringType()),
            StructField("STOCK_CAT", StringType()),
            StructField("ORDER_FLAG", StringType()),
            StructField("NEW_ITEM_13WEEK", StringType()),
            StructField("DEACTIVATE_DATE", StringType()),
            StructField("WH_ON_ORDER", StringType()),
            StructField("FIRST_RCV", StringType()),
            StructField("PROMO_MONTH", StringType()),
            StructField("SALES_TW", StringType()),
            StructField("NET_AMT", StringType()),
            StructField("NET_COST", StringType()),
            StructField("SALE_AVG_QTY_13WEEKS", StringType()),
            StructField("SALE_AVG_AMT_13WEEKS", StringType()),
            StructField("SALE_AVG_COST13WEEKS", StringType()),
            StructField("NET_QTY_YTD", StringType()),
            StructField("NET_AMT_YTD", StringType()),
            StructField("NET_COST_YTD", StringType()),
            StructField("TURN_WK", StringType()),
            StructField("WH_SOH", StringType()),
            StructField("STORE_TOTAL_STOCK", StringType()),
            StructField("TOTAL_STOCK_QTY", StringType()),
            StructField("WH_STOCK_AMT", StringType()),
            StructField("STORE_TOTAL_STOCK_AMT", StringType()),
            StructField("TOTAL_STOCK_XVAT", StringType()),
            StructField("PRO2", StringType()),
            StructField("DISC", StringType()),
            StructField("PRO22", StringType()),
            StructField("PRO2_PERT_DISC", StringType()),
            StructField("FIRST_DATE_SMS", StringType()),
            StructField("AGING_SMS", StringType()),
            StructField("GROUP_W", StringType()),
            StructField("WIN", StringType()),
            StructField("POG_2", StringType()),
            StructField("FILE_NAME", StringType()),
            StructField("DATE", StringType())
        ])
        df = session.read\\
        .schema(df_schema)\\
        .option("skip_header",1)\\
        .option("field_delimiter", ''\\x01'')\\
        .option("encoding", "UTF-8")\\
        .option("field_optionally_enclosed_by", "\\"")\\
        .option("null_if", "")\\
        .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
        # Adding transformations for specific columns if needed
        df = df.with_column("DATE", lit(datetime.now().strftime("%Y%m%d")))\\
               .with_column("FILE_NAME", lit(file_name))
        snowdf = df.select(
            "DIV",
            "DEPT",
            "CLASS",
            "SUBCLASS",
            "ITEM",
            "ITEM_DESC",
            "NON_SLOW",
            "NON_SLOW2",
            "FINANCE_STATUS",
            "CREATE_DATETIME",
            "PRIM_SUPPLIER",
            "OLD_SUPP_NO",
            "SUPP_DESC",
            "LEAD_TIME",
            "UNIT_COST",
            "UNIT_RETAIL_ZONE5",
            "ITEM_STATUS",
            "STATUS_WH",
            "STATUS_WH_UPDATE_DATE",
            "STATUS_STORE",
            "STATUS_STORE_UPDATE_DATE",
            "STATUS_XDOCK",
            "STATUS_XDOCK_UPDATE_DATE",
            "SOURCE_METHOD",
            "SOURCE_WH",
            "POG",
            "PRODUCT_TYPE",
            "LABEL_UDA",
            "BRAND",
            "ITEM_TYPE",
            "RETURN_POLICY",
            "RETURN_TYPE",
            "WH_WAC",
            "IN_TAX",
            "TAX_RATE",
            "STOCK_CAT",
            "ORDER_FLAG",
            "NEW_ITEM_13WEEK",
            "DEACTIVATE_DATE",
            "WH_ON_ORDER",
            "FIRST_RCV",
            "PROMO_MONTH",
            "SALES_TW",
            "NET_AMT",
            "NET_COST",
            "SALE_AVG_QTY_13WEEKS",
            "SALE_AVG_AMT_13WEEKS",
            "SALE_AVG_COST13WEEKS",
            "NET_QTY_YTD",
            "NET_AMT_YTD",
            "NET_COST_YTD",
            "TURN_WK",
            "WH_SOH",
            "STORE_TOTAL_STOCK",
            "TOTAL_STOCK_QTY",
            "WH_STOCK_AMT",
            "STORE_TOTAL_STOCK_AMT",
            "TOTAL_STOCK_XVAT",
            "PRO2",
            "DISC",
            "PRO22",
            "PRO2_PERT_DISC",
            "FIRST_DATE_SMS",
            "AGING_SMS",
            "GROUP_W",
            "WIN",
            "POG_2",
            "FILE_NAME",
            "DATE"
        )
        
        # Check if DataFrame is empty
		snowdf=snowdf.na.drop("all")
        if snowdf.count() == 0:
            return "No Data in table"
        
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
            
        # Archive
        file_name1=("_").join(file_name.split("_")[0:5])+"_"+datetime.now().strftime("%Y%m%d%H%M%S")
        snowdf.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name1,header=True,OVERWRITE=True)
        
        # Write operation
        snowdf.write.mode("append").save_as_table(stage_name.split(".")[0]+"."+target_table)
        
        # Success message
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
CREATE OR REPLACE PROCEDURE SDL_TH_TESCO_TRANSDATA("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '

import snowflake.snowpark as snowpark
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DateType
from snowflake.snowpark.functions import col,lit,date_format,current_timestamp,to_timestamp, xmlget, flatten, get, when, substring, row_number
from snowflake.snowpark import Window
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
    



def main(session:snowpark.Session, Param):

    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]

        df = session.read\\
        .option("STRIP_OUTER_ELEMENT", False) \\
        .xml("@" + stage_name + "/" + temp_stage_path + "/" + file_name) \\
        .select(
            xmlget(col("$1"), lit("invrptth")).alias("invrptth"),
            xmlget(xmlget(col("$1"), lit("invrptth")), lit("ir_header")).alias("ir_header"),
            xmlget(xmlget(col("$1"), lit("invrptth")), lit("ir_items")).alias("ir_items"),

            get_xml_element("ir_header", "creation_date", "string"),
            get_xml_element("ir_header", "supplier_ID", "string"),
            get_xml_element("ir_header", "supplier_name", "string"),
            get_xml_element("ir_header", "warehouse", "string"),
            get_xml_element("ir_header", "delivery_point_name", "string"),
            get_xml_element("ir_header", "IR_date", "string")
        ) \\
        .select(
            "creation_date",
            "supplier_ID",
            "supplier_name",
            "warehouse",
            "delivery_point_name",
            "IR_date",
            flatten(col("ir_items"), "$")
        ) \\
        .select (
            "creation_date",
            "supplier_ID",
            "supplier_name",
            "warehouse",
            "delivery_point_name",
            "IR_date",
            get_xml_element("value", "EANSKU", "string"),
            get_xml_element("value", "article_ID", "string"),
            get_xml_element("value", "SPN", "string"),
            get_xml_element("value", "article_name", "string"),
            get_xml_element("value", "stock", "string"),
            get_xml_element("value", "sales", "string"),
            get_xml_element("value", "sales_amount", "string"),
        )

        # Adding transformations for specific columns if needed
        df = df.with_column("FOLDER_NAME", lit(temp_stage_path))\\
               .with_column("FILE_NAME", lit(file_name))

        # Remove entries with null IR_DATE
        df_filtered = df.filter((col("IR_DATE").is_not_null()) & (col("IR_DATE").cast(StringType()) != ''''))

        # Continue from the window specification and ranking
        windowSpec = Window.partitionBy("IR_DATE", "WAREHOUSE", "SUPPLIER_ID").orderBy(substring(col("FILE_NAME"), 8, 26).desc())
        df_with_rank = df_filtered.withColumn("RANK", row_number().over(windowSpec))

        # Create a DataFrame of distinct file names where rank = 1
        df_rank_1_files = df_with_rank.filter(col("RANK") == 1).select("FILE_NAME").distinct()

        # Use a join operation to filter df_filtered based on the file names that have rank = 1
        df_filtered = df_filtered.join(df_rank_1_files, "FILE_NAME", "inner")

        snowdf = df_filtered.select(
            "CREATION_DATE",
            "SUPPLIER_ID",
            "SUPPLIER_NAME",
            "WAREHOUSE",
            "DELIVERY_POINT_NAME",
            "IR_DATE",
            "EANSKU",
            "ARTICLE_ID",
            "SPN",
            "ARTICLE_NAME",
            "STOCK",
            "SALES",
            "SALES_AMOUNT",
            "FILE_NAME",
            "FOLDER_NAME"
        )

        # Check if DataFrame is empty
		snowdf=snowdf.na.drop("all")
        if snowdf.count() == 0:
            return "No Data in table"

        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")

        # Archive
        file_name1=("_").join(file_name.split("_")[0:5])+"_"+datetime.now().strftime("%Y%m%d%H%M%S")
        snowdf.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name1,header=True,OVERWRITE=True)
        
        # Write operation
        snowdf.write.mode("append").save_as_table(stage_name.split(".")[0]+"."+target_table)
        
        # Success message
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

CREATE OR REPLACE PROCEDURE TH_ACTION_BOUNCE_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them.

import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col,lit
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz

def main(session: snowpark.Session,Param): 
    try :
        #Param=["TH_Action_Bounce_20230621_20230621170609.csv","THASDL_RAW.DEV_LOAD_STAGE_ADLS","dev/cert_data_lake/SFMC/TH_Action_Bounce","SDL_TH_SFMC_BOUNCE_DATA"]

        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        # Define the schema for the DataFrame
        df_schema =  StructType([
                    StructField("oyb_account_id", StringType()),
                    StructField("job_id", StringType()),
                    StructField("list_id", StringType()),
                    StructField("batch_id", StringType()),
                    StructField("subscriber_id", StringType()),
                    StructField("subscriber_key", StringType()),
                    StructField("event_date", StringType()),
                    StructField("is_unique", StringType()),
                    StructField("domain", StringType()),
                    StructField("bounce_category_id", StringType()),
                    StructField("bounce_category", StringType()),
                    StructField("bounce_subcategory_id", StringType()),
                    StructField("bounce_subcategory", StringType()),
                    StructField("bounce_type_id", StringType()),
                    StructField("bounce_type", StringType()),
                    StructField("smtp_bounce_reason", StringType()),
                    StructField("smtp_message", StringType()),
                    StructField("smtp_code", StringType()),
                    StructField("triggerer_send_definition_object_id", StringType()),
                    StructField("triggered_send_customer_key", StringType()),
                    StructField("email_subject", StringType()),
                    StructField("bcc_email", StringType()),
                    StructField("email_name", StringType()),
                    StructField("email_id", StringType()),
                    StructField("email_address", StringType())
                ])
        
        dataframe = session.read\\
        .schema(df_schema)\\
        .option("skip_header",1)\\
        .option("field_delimiter", "|")\\
        .option("field_optionally_enclosed_by", "\\"") \\
        .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        dataframe=dataframe.na.drop("all")
        if dataframe.count()==0:
            return "No Data in file"
        #---------------------------Transformation logic ------------------------------#

        # Add FILE_NAME and CRTD_DTTM to the dataframe
        new_file_name=file_name[0:25] +".csv"
        dataframe = dataframe.with_column("FILE_NAME",lit(new_file_name))
        dataframe = dataframe.with_column("CRTD_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))


        # Creating copy of the Dataframe
        final_df = dataframe.alias("final_df")

       
        # Load Data to the target table
        final_df.write.mode("append").saveAsTable(target_table)

        # write to success folder
        file_name=file_name[0:25]+''_''+datetime.now().strftime("%Y%m%d%H%M%S")
		
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
		
        final_df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)
        
        return ''Success''
        
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
CREATE OR REPLACE PROCEDURE TH_ACTION_CLICK_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them.

import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col,lit
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DecimalType,DateType
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz

def main(session: snowpark.Session,Param): 
    try :
        

        #Param=["TH_Action_Click_20230621_20230621170615.csv","THASDL_RAW.DEV_LOAD_STAGE_ADLS","dev/cert_data_lake/SFMC/TH_Action_Click","SDL_TH_SFMC_CLICK_DATA"]

        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        # Define the schema for the DataFrame
        df_schema = StructType([
                StructField("oyb_account_id", StringType()),
                StructField("job_id", StringType()),
                StructField("list_id", StringType()),
                StructField("batch_id", StringType()),
                StructField("subscriber_id", StringType()),
                StructField("subscriber_key", StringType()),
                StructField("event_date", StringType()),
                StructField("domain", StringType()),
                StructField("url", StringType()),
                StructField("link_name", StringType()),
                StructField("link_content", StringType()),
                StructField("is_unique", StringType()),
                StructField("email_name", StringType()),
                StructField("email_subject", StringType())
            ])
        
        dataframe = session.read\\
        .schema(df_schema)\\
        .option("skip_header",1)\\
        .option("field_delimiter", "|")\\
        .option("field_optionally_enclosed_by", "\\"") \\
        .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        dataframe=dataframe.na.drop("all")
        if dataframe.count()==0:
            return "No Data in file"
        #---------------------------Transformation logic ------------------------------#

        # Add FILE_NAME and CRTD_DTTM to the dataframe
        new_file_name=file_name[0:24] +".csv"
        dataframe = dataframe.with_column("FILE_NAME",lit(new_file_name))
        dataframe = dataframe.with_column("CRTD_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))


        # Creating copy of the Dataframe
        final_df = dataframe.alias("final_df")

        
        # Load Data to the target table
        final_df.write.mode("append").saveAsTable(target_table)

        # write to success folder
        file_name=file_name[0:24]+''_''+datetime.now().strftime("%Y%m%d%H%M%S")
		
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        final_df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)
        
        return ''Success''
        
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
CREATE OR REPLACE PROCEDURE TH_ACTION_OPEN_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them.

import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col,lit
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DecimalType,DateType
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz



def main(session: snowpark.Session,Param): 
    
    try :

        #Param=["TH_Action_Open_20231205_20231212083337.csv","THASDL_RAW.DEV_LOAD_STAGE_ADLS","dev/cert_data_lake/SFMC/TH_Action_Open/","sdl_TH_sfmc_open_data"]

        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        # Define the schema for the DataFrame
        df_schema = StructType([
            StructField("oyb_account_id", StringType()),
            StructField("job_id", StringType()),
            StructField("list_id", StringType()),
            StructField("batch_id", StringType()),
            StructField("subscriber_id", StringType()),
            StructField("subscriber_key", StringType()),
            StructField("email_name", StringType()),
            StructField("email_subject", StringType()),
            StructField("bcc_email", StringType()),
            StructField("email_id", StringType()),
            StructField("event_date", StringType()),
            StructField("domain", StringType()),
            StructField("is_unique", StringType())
        ])
        
        dataframe = session.read.schema(df_schema) \\
        .option("skip_header",1) \\
        .option("field_delimiter", "|") \\
        .option("field_optionally_enclosed_by", "\\"")  \\
        .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        dataframe=dataframe.na.drop("all")
        if dataframe.count()==0:
            return "No Data in file"
        #---------------------------Transformation logic ------------------------------#

        # Add FILE_NAME and CRTD_DTTM to the dataframe
        new_file_name=file_name[0:23] +".csv"
        dataframe = dataframe.with_column("FILE_NAME",lit(new_file_name))
        dataframe = dataframe.with_column("CRTD_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))


        # Creating copy of the Dataframe
        final_df = dataframe.alias("final_df")

        
        # Load Data to the target table
        final_df.write.mode("append").saveAsTable(target_table)

        # write to success folder
        file_name=file_name[0:23]+''_''+datetime.now().strftime("%Y%m%d%H%M%S")
		
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")

        final_df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)
        
        return ''Success''
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
CREATE OR REPLACE PROCEDURE TH_ACTION_SENT_PREPROCESSING("PARAM" ARRAY)
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
from snowflake.snowpark.functions import col,lit,trim
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField,DecimalType,DateType,TimestampType
from datetime import datetime
import pytz

def main(session: snowpark.Session,Param): 

    try:

        #Param=[''TH_Action_Sent_20230621_20230621170614.csv'',''THASDL_RAW.DEV_LOAD_STAGE_ADLS'',''dev/cert_data_lake/SFMC/TH_Action_Sent/'',''SDL_TH_SFMC_SENT_DATA'']

        # Extracting parameters from the input
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        

        # Define the schema for the DataFrame
        df_schema=StructType([
            StructField("OYB_ACCOUNT_ID",StringType()),
            StructField("JOB_ID",StringType()),
            StructField("LIST_ID",StringType()),
            StructField("BATCH_ID",StringType()),
            StructField("SUBSCRIBER_ID",StringType()),
            StructField("SUBSCRIBER_KEY",StringType()),
            StructField("EVENT_DATE",TimestampType()),
            StructField("DOMAIN",StringType()),
            StructField("TRIGGERER_SEND_DEFINITION_OBJECT_ID",StringType()),
            StructField("TRIGGERED_SEND_CUSTOMER_KEY",StringType()),
            StructField("EMAIL_NAME",StringType()),
            StructField("EMAIL_SUBJECT",StringType()),
            StructField("EMAIL_ID",StringType())
            ])


        # Read the CSV file into a DataFrame
    
        dataframe = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "|")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)


        #---------------------------Transformation logic ------------------------------#
        
        # Handle null values or empty rows
        dataframe=dataframe.na.drop("all")
		if dataframe.count()==0:
            return "No Data in file"

        # Trim Spaces in Email Subject column
        dataframe = dataframe.withColumn("EMAIL_SUBJECT", trim(dataframe["EMAIL_SUBJECT"]))

         # Add FILE_NAME and CRTD_DTTM to the dataframe
        new_file_name=file_name[0:23] +".csv"

        dataframe = dataframe.with_column("FILE_NAME",lit(new_file_name))
        dataframe = dataframe.with_column("CRTD_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))

        dataframe=dataframe.select("OYB_ACCOUNT_ID","JOB_ID","LIST_ID","BATCH_ID","SUBSCRIBER_ID","SUBSCRIBER_KEY","EVENT_DATE","DOMAIN","TRIGGERER_SEND_DEFINITION_OBJECT_ID","TRIGGERED_SEND_CUSTOMER_KEY","EMAIL_NAME","EMAIL_SUBJECT","EMAIL_ID","FILE_NAME","CRTD_DTTM")

        # Creating copy of the Dataframe
        final_df = dataframe.alias("final_df")

        


        # Load Data to the target table
        final_df.write.mode("append").saveAsTable(target_table)
        
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")

        # write to success folder
    
        file_name=file_name.split(".")[0]
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
CREATE OR REPLACE PROCEDURE TH_ACTION_UNSUBSCRIBE_PREPROCESSING("PARAM" ARRAY)
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
from snowflake.snowpark.functions import col,lit,trim
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField,DecimalType,DateType,TimestampType
from datetime import datetime
import pytz

def main(session: snowpark.Session,Param): 

    try:

        #Param=[''TH_Action_Unsubscribe_20230627_202306271706'',''THASDL_RAW.DEV_LOAD_STAGE_ADLS'',''dev/cert_data_lake/SFMC/TH_Action_Unsubscribe/'',''SDL_TH_SFMC_UNSUBSCRIBE_DATA'']

        # Extracting parameters from the input
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        

        # Define the schema for the DataFrame
        df_schema=StructType([
            StructField("OYB_ACCOUNT_ID",StringType()),
            StructField("JOB_ID",StringType()),
            StructField("LIST_ID",StringType()),
            StructField("BATCH_ID",StringType()),
            StructField("SUBSCRIBER_ID",StringType()),
            StructField("SUBSCRIBER_KEY",StringType()),
            StructField("EVENT_DATE",TimestampType()),
            StructField("DOMAIN",StringType()),
            StructField("EMAIL_NAME",StringType()),
            StructField("EMAIL_SUBJECT",StringType()),
            StructField("EMAIL_ID",StringType()),
            StructField("IS_UNIQUE",StringType())
            ])


        # Read the CSV file into a DataFrame
    
        dataframe = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "|")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        #---------------------------Transformation logic ------------------------------#
        
        # Handle null values or empty rows
        dataframe=dataframe.na.drop("all")
		if dataframe.count()==0:
            return "No Data in file"

         # Add FILE_NAME and CRTD_DTTM to the dataframe
        new_file_name=file_name[0:30] +".csv"

        dataframe = dataframe.with_column("FILE_NAME",lit(new_file_name))
        dataframe = dataframe.with_column("CRTD_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))


        # Creating copy of the Dataframe
        final_df = dataframe.alias("final_df")

        

       

        # Load Data to the target table
        final_df.write.mode("append").saveAsTable(target_table)

        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")

        # write to success folder
    
        file_name=file_name.split(".")[0]
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
CREATE OR REPLACE PROCEDURE TH_CRM_CHILDREN_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them.

import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col,lit,trim,when
from snowflake.snowpark.types import  StringType, StructType, StructField
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz



def main(session: snowpark.Session,Param): 
       
    try :

        #Param=["TH_CRM_Children_20240311_20240311171401.csv","THASDL_RAW.DEV_LOAD_STAGE_ADLS","dev/cert_data_lake/SFMC/TH_CRM_Children/","SDL_TH_SFMC_CHILDREN_DATA"]

        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        # Define the schema for the DataFrame
        
        df_schema = StructType([
                StructField("parent_key", StringType()),
                StructField("child_nm", StringType()),
                StructField("child_birth_mnth", StringType()),
                StructField("child_birth_year", StringType()),
                StructField("child_gender", StringType()),
                StructField("child_number", StringType())
            ])
        
        dataframe = session.read.schema(df_schema) \\
        .option("skip_header",1) \\
        .option("field_delimiter", "|") \\
        .option("field_optionally_enclosed_by", "\\"")  \\
        .option("encoding","UTF-16")\\
        .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        dataframe=dataframe.na.drop("all")
        if dataframe.count()==0:
            return "No Data in file"
       
        #---------------------------Transformation logic ------------------------------#

        # Add FILE_NAME and CRTD_DTTM to the dataframe
        new_file_name=file_name[0:24] +".csv"
        dataframe = dataframe.with_column("FILE_NAME",lit(new_file_name))
        dataframe = dataframe.with_column("CRTD_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))


       

        # Apply the case statement
        dataframe = dataframe.withColumn("CHILD_BIRTH_MNTH",  (
             when(trim(col("child_birth_mnth")) == "มกราคม", "01")
            .when(trim(col("child_birth_mnth")) == "กุมภาพันธ์", "02")
            .when(trim(col("child_birth_mnth")) == "มีนาคม", "03")
            .when(trim(col("child_birth_mnth")) == "เมษายน", "04")
            .when(trim(col("child_birth_mnth")) == "พฤษภาคม", "05")
            .when(trim(col("child_birth_mnth")) == "มิถุนายน", "06")
            .when(trim(col("child_birth_mnth")) == "กรกฎาคม", "07")
            .when(trim(col("child_birth_mnth")) == "สิงหาคม", "08")
            .when(trim(col("child_birth_mnth")) == "กันยายน", "09")
            .when(trim(col("child_birth_mnth")) == "ตุลาคม", "10")
            .when(trim(col("child_birth_mnth")) == "พฤศจิกายน", "11")
            .when(trim(col("child_birth_mnth")) == "ธันวาคม", "12")
            .otherwise("UNDEFINED")

        ))

        # Creating copy of the Dataframe
        final_df = dataframe.select("PARENT_KEY","CHILD_NM","CHILD_BIRTH_MNTH","CHILD_BIRTH_YEAR","CHILD_GENDER","CHILD_NUMBER","FILE_NAME","CRTD_DTTM")

        
        # Load Data to the target table
        final_df.write.mode("append").saveAsTable(target_table)

        # write to success folder
        file_name=file_name[0:24]+''_''+datetime.now().strftime("%Y%m%d%H%M%S")
        # final_df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/success/"+file_name,file_format_type="csv",header=True,OVERWRITE=True)

        
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        #move to success
        final_df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)

        
        return ''Success''
        
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
CREATE OR REPLACE PROCEDURE TH_CRM_CONSUMER_MASTER_ADDITIONAL("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '

import snowflake.snowpark as snowpark
from snowflake.snowpark.window import Window
from snowflake.snowpark.functions import col,lit,trim,when,row_number,when_matched
from snowflake.snowpark.types import  StringType, StructType, StructField
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz



def main(session: snowpark.Session,Param): 
       
    try :

        #Param=["TH_CRM_Consumer_Master_Additional_20240317","THASDL_RAW.DEV_LOAD_STAGE_ADLS","dev/cert_data_lake/SFMC/TH_CRM_Consumer_Master_Additional","SDL_TH_SFMC_consumer_master_additional"]

        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        # Define the schema for the DataFrame
        
        df_schema = StructType([
                StructField("subscriber_key", StringType()),
                StructField("attribute_name", StringType()),
                StructField("attribute_value", StringType())
            ])
        
        dataframe = session.read.schema(df_schema) \\
        .option("skip_header",1) \\
        .option("field_delimiter", "|") \\
        .option("field_optionally_enclosed_by", "\\"")  \\
        .option("encoding","UTF-8")\\
        .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        dataframe=dataframe.na.drop("all")
        if dataframe.count()==0:
            return "No Data in file"
       
        # #---------------------------Transformation logic ------------------------------#

        # Add FILE_NAME and CRTD_DTTM to the dataframe
        new_file_name=file_name[0:42] +".csv"
        dataframe = dataframe.with_column("FILE_NAME",lit(new_file_name))
        dataframe = dataframe.with_column("CRTD_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))



        duplicates = dataframe.with_column("Duplicate", row_number().over(Window.partition_by(col("subscriber_key"), col("attribute_name"), col("attribute_value"), col("file_name")).order_by(col("subscriber_key"))))
        duplicates = duplicates.filter(col("Duplicate") > 1)
        dataframe=dataframe.join(duplicates,((dataframe["subscriber_key"] == duplicates["subscriber_key"]) & (dataframe["attribute_name"] == duplicates["attribute_name"]) &  (dataframe["attribute_value"] == duplicates["attribute_value"])  &  (dataframe["file_name"] == duplicates["file_name"])),"left_anti")  \\
                                 .select(dataframe["subscriber_key"],dataframe["attribute_name"],dataframe["attribute_value"],dataframe["file_name"],dataframe["CRTD_DTTM"])
        
        #dataframe.merge(duplicates, dataframe["subscriber_key"] == duplicates["subscriber_key"], [when_matched().delete()])
        
        
        
        
        
        # Load Data to the target table
        dataframe.write.mode("append").saveAsTable(target_table)

        # write to success folder
        file_name=file_name[0:42]+''_''+datetime.now().strftime("%Y%m%d%H%M%S")
    
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        #move to success
        dataframe.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)

        
        return ''Success''
        
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
CREATE OR REPLACE PROCEDURE TH_CRM_CONSUMER_MASTER_ADDITIONAL_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '

import snowflake.snowpark as snowpark
from snowflake.snowpark.window import Window
from snowflake.snowpark.functions import col,lit,trim,when,row_number,when_matched
from snowflake.snowpark.types import  StringType, StructType, StructField
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz



def main(session: snowpark.Session,Param): 
       
    try :

        #Param=["TH_CRM_Consumer_Master_Additional_20240317","THASDL_RAW.DEV_LOAD_STAGE_ADLS","dev/cert_data_lake/SFMC/TH_CRM_Consumer_Master_Additional","SDL_TH_SFMC_consumer_master_additional"]

        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        # Define the schema for the DataFrame
        
        df_schema = StructType([
                StructField("subscriber_key", StringType()),
                StructField("attribute_name", StringType()),
                StructField("attribute_value", StringType())
            ])
        
        dataframe = session.read.schema(df_schema) \\
        .option("skip_header",1) \\
        .option("field_delimiter", "|") \\
        .option("field_optionally_enclosed_by", "\\"")  \\
        .option("encoding","UTF-8")\\
        .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        dataframe=dataframe.na.drop("all")
        if dataframe.count()==0:
            return "No Data in file"
       
        # #---------------------------Transformation logic ------------------------------#

        # Add FILE_NAME and CRTD_DTTM to the dataframe
        new_file_name=file_name[0:42] +".csv"
        dataframe = dataframe.with_column("FILE_NAME",lit(new_file_name))
        dataframe = dataframe.with_column("CRTD_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))



        duplicates = dataframe.with_column("Duplicate", row_number().over(Window.partition_by(col("subscriber_key"), col("attribute_name"), col("attribute_value"), col("file_name")).order_by(col("subscriber_key"))))
        duplicates = duplicates.filter(col("Duplicate") > 1)
        dataframe=dataframe.join(duplicates,((dataframe["subscriber_key"] == duplicates["subscriber_key"]) & (dataframe["attribute_name"] == duplicates["attribute_name"]) &  (dataframe["attribute_value"] == duplicates["attribute_value"])  &  (dataframe["file_name"] == duplicates["file_name"])),"left_anti")  \\
                                 .select(dataframe["subscriber_key"],dataframe["attribute_name"],dataframe["attribute_value"],dataframe["file_name"],dataframe["CRTD_DTTM"])
        
        #dataframe.merge(duplicates, dataframe["subscriber_key"] == duplicates["subscriber_key"], [when_matched().delete()])
        
        
        
        
        
        # Load Data to the target table
        dataframe.write.mode("append").saveAsTable(target_table)

        # write to success folder
        file_name=file_name[0:42]+''_''+datetime.now().strftime("%Y%m%d%H%M%S")
    
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        #move to success
        dataframe.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)

        
        return ''Success''
        
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
CREATE OR REPLACE PROCEDURE TH_CRM_CONSUMER_MASTER_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them.

import snowflake.snowpark as snowpark
from snowflake.snowpark.window import Window
from snowflake.snowpark.functions import col,lit,trim,when,row_number,when_matched
from snowflake.snowpark.types import  StringType, StructType, StructField
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz
import sys
from pathlib import Path
import warnings
import os 



def main(session: snowpark.Session,Param): 
       
    try :
	

        
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        # Define the schema for the DataFrame
        
        df_schema =  schema = StructType([
                    StructField("First_Name", StringType()),
                    StructField("Last_Name", StringType()),
                    StructField("Mobile_Number", StringType()),
                    StructField("Mobile_Country_Code", StringType()),
                    StructField("Birthday_Month", StringType()),
                    StructField("Birthday_Year", StringType()),
                    StructField("Address_1", StringType()),
                    StructField("Address_2", StringType()),
                    StructField("Address_City", StringType()),
                    StructField("Address_Zipcode", StringType()),
                    StructField("Subscriber_Key", StringType()),
                    StructField("Website_Unique_ID", StringType()),
                    StructField("Source", StringType()),
                    StructField("Medium", StringType()),
                    StructField("Brand", StringType()),
                    StructField("Address_Country", StringType()),
                    StructField("Campaign_ID", StringType()),
                    StructField("Created_Date", StringType()),
                    StructField("Updated_Date", StringType()),
                    StructField("Unsubscribe_Date", StringType()),
                    StructField("Email", StringType()),
                    StructField("Full_Name", StringType()),
                    StructField("Last_Logon_Time", StringType()),
                    StructField("Remaining_Points", StringType()),
                    StructField("Redeemed_Points", StringType()),
                    StructField("Total_Points", StringType()),
                    StructField("Gender", StringType()),
                    StructField("Line_ID", StringType()),
                    StructField("Line_Name", StringType()),
                    StructField("Line_Email", StringType()),
                    StructField("LINE_Channel_ID", StringType()),
                    StructField("Address_Region", StringType()),
                    StructField("Tier", StringType()),
                    StructField("Opt_In_For_Communication", StringType()),
                    StructField("Smoker", StringType()),
                    StructField("Have_Kid", StringType()),
                    StructField("Expectant_Mother", StringType()),
                    StructField("Category_they_are_using", StringType()),
                    StructField("Skin_Condition", StringType()),
                    StructField("Skin_Problem", StringType()),
                    StructField("Use_Mouthwash", StringType()),
                    StructField("Mouthwash_time", StringType()),
                    StructField("Why_not_use_Mouthwash", StringType()),
                    StructField("Oral_Problem", StringType()),
                    StructField("Age", StringType())
                ])

        dataframe = session.read.schema(df_schema) \\
        .option("skip_header",1) \\
        .option("field_delimiter", "|") \\
        .option("field_optionally_enclosed_by", "\\"")  \\
        .option("encoding","UTF-16")\\
        .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        dataframe=dataframe.na.drop("all")
        if dataframe.count()==0:
            return "No Data in file"

        # ----------------  creating  TH_CRM_Consumer_Master_Additional_ file start ------------------
       
        raw_data=dataframe.to_pandas()

        
        #removing hard coded values from fn
        null_value = "Null"
        
        lst_columns_attr = [''SUBSCRIBER_KEY'',''SMOKER'',''EXPECTANT_MOTHER'',
               ''CATEGORY_THEY_ARE_USING'', ''SKIN_CONDITION'', ''SKIN_PROBLEM'',
               ''USE_MOUTHWASH'', ''MOUTHWASH_TIME'', ''WHY_NOT_USE_MOUTHWASH'',
               ''ORAL_PROBLEM'']
        primary_key = "SUBSCRIBER_KEY"
        lst_attr_col = ["SUBSCRIBER_KEY","Attribute_Name","Attribute_value"]
        attr_val = ''Attribute_value''
        lst_columns_master = [''SMOKER'',''EXPECTANT_MOTHER'',
               ''CATEGORY_THEY_ARE_USING'', ''SKIN_CONDITION'', ''SKIN_PROBLEM'',
               ''USE_MOUTHWASH'', ''MOUTHWASH_TIME'', ''WHY_NOT_USE_MOUTHWASH'',
               ''ORAL_PROBLEM'']
       
        
        raw_data = raw_data.fillna(null_value)
    
    
    
 
        new_attr_df = raw_data[lst_columns_attr]
        
    

        new_attr_df_indexed = new_attr_df.set_index(primary_key)
        new_attr_df_stacked = pd.DataFrame(new_attr_df_indexed.stack()).reset_index()
    
        # renaming columns for the new file
        new_attr_df_stacked.columns = lst_attr_col
    

        new_attr_df_stacked[attr_val] = new_attr_df_stacked[attr_val].str.split('','').map(lambda elements: [e.strip() for e in elements])
        new_attr_df_stacked[attr_val] = new_attr_df_stacked[attr_val].map(lambda elements: list(filter(None, elements)))
    
        # final dataframe
        df_transposed = new_attr_df_stacked.explode(attr_val).reset_index(drop=True)
    
        # removing null values
        df_transposed = df_transposed[df_transposed[attr_val] != null_value]
        df_transposed = df_transposed.dropna(subset=[attr_val]).reset_index(drop=True)

        dataframe=session.create_dataframe(df_transposed)
        filename="TH_CRM_Consumer_Master_Additional_" + Path(file_name).stem[23:31]
        dataframe.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"TH_CRM_Consumer_Master_Additional/"+"/"+filename,file_format_type="csv",format_type_options={''COMPRESSION'':''None'',"FIELD_DELIMITER":"|"},partition_by=None,overwrite=True,header=True)
        
        dataframe = session.read.schema(df_schema) \\
        .option("skip_header",1) \\
        .option("field_delimiter", "|") \\
        .option("field_optionally_enclosed_by", "\\"")  \\
        .option("encoding","UTF-16")\\
        .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        filename="TH_CRM_Consumer_Master_Primary_" + Path(file_name).stem[23:31]
        dataframe = dataframe.with_column("FILE_NAME",lit(filename))
        dataframe = dataframe.with_column("CRTD_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        dataframe = dataframe.withColumn("BIRTHDAY_MONTH",  (
             when(trim(col("BIRTHDAY_MONTH")) == "มกราคม", "01")
            .when(trim(col("BIRTHDAY_MONTH")) == "กุมภาพันธ์", "02")
            .when(trim(col("BIRTHDAY_MONTH")) == "มีนาคม", "03")
            .when(trim(col("BIRTHDAY_MONTH")) == "เมษายน", "04")
            .when(trim(col("BIRTHDAY_MONTH")) == "พฤษภาคม", "05")
            .when(trim(col("BIRTHDAY_MONTH")) == "มิถุนายน", "06")
            .when(trim(col("BIRTHDAY_MONTH")) == "กรกฎาคม", "07")
            .when(trim(col("BIRTHDAY_MONTH")) == "สิงหาคม", "08")
            .when(trim(col("BIRTHDAY_MONTH")) == "กันยายน", "09")
            .when(trim(col("BIRTHDAY_MONTH")) == "ตุลาคม", "10")
            .when(trim(col("BIRTHDAY_MONTH")) == "พฤศจิกายน", "11")
            .when(trim(col("BIRTHDAY_MONTH")) == "ธันวาคม", "12")
            .otherwise("UNDEFINED")

        ))
        dataframe=dataframe.select("FIRST_NAME","LAST_NAME","MOBILE_NUMBER","MOBILE_COUNTRY_CODE","BIRTHDAY_MONTH",
                            "BIRTHDAY_YEAR","ADDRESS_1","ADDRESS_2","ADDRESS_CITY","ADDRESS_ZIPCODE","SUBSCRIBER_KEY",
                            "WEBSITE_UNIQUE_ID","SOURCE","MEDIUM","BRAND","ADDRESS_COUNTRY","CAMPAIGN_ID","CREATED_DATE",
                            "UPDATED_DATE","UNSUBSCRIBE_DATE","EMAIL","FULL_NAME","LAST_LOGON_TIME","REMAINING_POINTS",
                            "REDEEMED_POINTS","TOTAL_POINTS","GENDER","LINE_ID","LINE_NAME","LINE_EMAIL","LINE_CHANNEL_ID",
                            "ADDRESS_REGION","TIER","OPT_IN_FOR_COMMUNICATION","HAVE_KID","AGE","FILE_NAME","CRTD_DTTM")

        # Load Data to the target table
        dataframe.write.mode("append").saveAsTable(target_table)

        # write to success folder
        file_name=file_name[0:24]+''_''+datetime.now().strftime("%Y%m%d%H%M%S")

        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        #move to success
        dataframe.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)

        
        return ''Success''
        
        
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
CREATE OR REPLACE PROCEDURE TH_GT_MSLD_PREPROCESSING("PARAM" ARRAY)
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
		
		df=df.na.drop("all")
		if df.count()==0:
            return "No Data in file"

        
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

        
        
        # Load Data to the target table
        final_df.write.mode("append").saveAsTable(target_table)

    
        # write to success folder
    
        file_name=file_name.split(".")[0]+''_''+datetime.now().strftime("%Y%m%d%H%M%S")
		
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
		
        final_df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)
   
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
CREATE OR REPLACE PROCEDURE TH_GT_ROUTE_DTL_PREPROCESSING("PARAM" ARRAY)
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
		
		df=df.na.drop("all")
		if df.count()==0:
            return "No Data in file"

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

        
        
        # Load Data to the target table
        final_df.write.mode("append").saveAsTable(target_table)

    
        # write to success folder
        
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
		
        file_name=file_name.split(".")[0]+''_''+datetime.now().strftime("%Y%m%d%H%M%S")
        final_df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)
   
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
CREATE OR REPLACE PROCEDURE TH_GT_ROUTE_HDR_PREPROCESSING("PARAM" ARRAY)
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
		
		df=df.na.drop("all")
		if df.count()==0:
            return "No Data in file"

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
		

        
        # Load Data to the target table
        final_df.write.mode("append").saveAsTable(target_table)

    
        # write to success folder
		
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
    
        file_name=file_name.split(".")[0]+''_''+datetime.now().strftime("%Y%m%d%H%M%S")
		
        final_df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)
   
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
CREATE OR REPLACE PROCEDURE TH_GT_SALES_ORDER_PREPROCESSING("PARAM" ARRAY)
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
		
		df=df.na.drop("all")
		if df.count()==0:
            return "No Data in file"

        
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
        
        # Load Data to the target table
        final_df.write.mode("append").saveAsTable(target_table)

    
        # write to success folder
    
        file_name=file_name.split(".")[0]+''_''+datetime.now().strftime("%Y%m%d%H%M%S")
		
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
		
        final_df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)
   
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
CREATE OR REPLACE PROCEDURE TH_GT_SCHEDULE_PREPROCESSING("PARAM" ARRAY)
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
		
		df=df.na.drop("all")
		if df.count()==0:
            return "No Data in file"

        
        #convertin time stamp into sg timezone
        df = df.withColumn("CRT_DTTM", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        df = df.withColumn("FILE_NAME", lit(file_name).cast("string"))
        df = df.withColumn("RUN_ID", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        df = df.withColumn("CNTRY_CD", lit("TH"))
        df = df.withColumn("CRNCY_CD", lit("THB"))
        
        # Creating copy of the Dataframe
        final_df = df.select("CNTRY_CD", "CRNCY_CD", "employeeid", "routeid", to_date("date", lit("YYYYMMDD")).as_("date"), "approved", "saleunit", "FILE_NAME", "RUN_ID", "CRT_DTTM").alias("final_df")
		
		
        
        # Load Data to the target table
        final_df.write.mode("append").saveAsTable(target_table)

    
        # write to success folder
    
        file_name=file_name.split(".")[0]+''_''+datetime.now().strftime("%Y%m%d%H%M%S")
		
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
		
        final_df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)
   
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
CREATE OR REPLACE PROCEDURE TH_GT_VISIT_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '# The Snowpark package is required for Python Worksheets. 

import snowflake.snowpark as snowpark
from snowflake import snowpark
from snowflake.snowpark.functions import col,lit,to_date,trim,date_format
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
        df_schema = StructType([
            StructField("id_sale", StringType()),
            StructField("sale_name", StringType()),
            StructField("id_customer", StringType()),
            StructField("customer_name", StringType()),
            StructField("date_plan", StringType()),
            StructField("time_plan", StringType()),
            StructField("date_visi", StringType()),
            StructField("time_visi", StringType()),
            StructField("object", StringType()),
            StructField("visit_end", StringType()),
            StructField("visit_time", StringType()),
            StructField("regioncode", StringType()),
            StructField("areacode", StringType()),
            StructField("branchcode", StringType()),
            StructField("saleunit", StringType()),
            StructField("time_survey_in", StringType()),
            StructField("time_survey_out", StringType()),
            StructField("count_survey", StringType())
            ])

        
        # Read the CSV file into a DataFrame
    
        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "|")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)


        #---------------------------Transformation logic ------------------------------#
        
        #Check if the Dataframe is having Data
        
        

        df=df.na.drop("all")
		if df.count()==0:
            return "No Data in file"
        

        #transform columns

        df = df.withColumn("CNTRY_CD", lit("TH"))
        df = df.withColumn("CRNCY_CD", lit("THB"))
        df = df.with_column("id_sale",trim(col("id_sale")))
        df = df.with_column("sale_name",trim(col("sale_name")))
        df = df.with_column("id_customer",trim(col("id_customer")))
        df = df.with_column("customer_name",trim(col("customer_name")))
        df = df.with_column("date_plan",trim(col("date_plan")))
        df = df.with_column("time_plan",trim(col("time_plan")))
        df = df.with_column("date_visi",trim(col("date_visi")))
        df = df.with_column("time_visi",trim(col("time_visi")))
        df = df.with_column("object",trim(col("object")))
        df = df.with_column("visit_end",trim(col("visit_end")))
        df = df.with_column("visit_time",trim(col("visit_time")))
        df = df.with_column("regioncode",trim(col("regioncode")))
        df = df.with_column("areacode",trim(col("areacode")))
        df = df.with_column("branchcode",trim(col("branchcode")))
        df = df.with_column("saleunit",trim(col("saleunit")))
        df = df.with_column("time_survey_in",trim(col("time_survey_in")))
        df = df.with_column("time_survey_out",trim(col("time_survey_out")))
        df = df.with_column("count_survey",trim(col("count_survey")))
        df= df.with_column("run_id", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S"))) 
        df=df.with_column("filename",lit(file_name))
        df=df.with_column("crt_dttm",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))

        

        final_df = df.select("CNTRY_CD","CRNCY_CD","id_sale","sale_name","id_customer","customer_name",to_date("date_plan", lit("YYYYMMDD")).as_("date_plan"),"time_plan",to_date("date_visi", lit("YYYYMMDD")).as_("date_visi"),"time_visi","object",to_date("visit_end", lit("YYYYMMDD")).as_("visit_end"),"visit_time","regioncode","areacode","branchcode","saleunit","time_survey_in","time_survey_out","count_survey","filename","run_id","crt_dttm")

       
         #Delete existing Data for the current file
        
        del_sql = "DELETE FROM " + target_table + " WHERE filename ="+"''" + (file_name)+"''"
        session.sql(del_sql).collect()
        
        # Load Data to the target table
        final_df.write.mode("append").saveAsTable(stage_name.split(".")[0]+"."+target_table)

        # write to success folder
    
        file_name=file_name.split(".")[0]+''_''+datetime.now().strftime("%Y%m%d%H%M%S")
		
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")

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
CREATE OR REPLACE PROCEDURE TH_MBOX_CUSTOMER_PREPROCESSING("PARAM" ARRAY)
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
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField,DecimalType,DateType
import pandas as pd
from datetime import datetime
import pytz

def main(session: snowpark.Session,Param):

    
    try:

        # Parameters for consumerreach_cvs
        #Param=[''A1_SPC2403042250_20240305023320.txt'',''THASDL_RAW.DEV_LOAD_STAGE_ADLS'',''dev/GT_Intervention/DnA_VMR/cert-data-lake/CustomerDim/TH_GT_CUSTOMER/'',''sdl_th_dms_chana_customer_dim'']
        
        # Extracting parameters from the input
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]

        # Define the schema for the DataFrame
        df_schema = StructType([
            StructField("distributorid", StringType()),
            StructField("arcode", StringType()),
            StructField("arname", StringType()),
            StructField("araddress", StringType()),
            StructField("telephone", StringType()),
            StructField("fax", StringType()),
            StructField("city", StringType()),
            StructField("region", StringType()),
            StructField("saledistrict", StringType()),
            StructField("saleoffice", StringType()),
            StructField("salegroup", StringType()),
            StructField("artypecode", StringType()),
            StructField("saleemployee", StringType()),
            StructField("salename", StringType()),
            StructField("billno", StringType()),
            StructField("billmoo", StringType()),
            StructField("billsoi", StringType()),
            StructField("billroad", StringType()),
            StructField("billsubdist", StringType()),
            StructField("billdistrict", StringType()),
            StructField("billprovince", StringType()),
            StructField("billzipcode", StringType()),
            StructField("activestatus", StringType()),
            StructField("routestep1", StringType()),
            StructField("routestep2", StringType()),
            StructField("routestep3", StringType()),
            StructField("routestep4", StringType()),
            StructField("routestep5", StringType()),
            StructField("routestep6", StringType()),
            StructField("routestep7", StringType()),
            StructField("routestep8", StringType()),
            StructField("routestep9", StringType()),
            StructField("routestep10", StringType()),
            StructField("store", StringType()),
            StructField("pricelevel", StringType()),
            StructField("salesarename", StringType()),
            StructField("branchcode", StringType()),
            StructField("branchname", StringType()),
            StructField("frequencyofvisit", StringType())
            ])

        
        # Read the CSV file into a DataFrame
    
        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",0)\\
            .option("field_delimiter", "\\t")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)


        #---------------------------Transformation logic ------------------------------#

        # Check if the Dataframe is having Data
        df = df.na.drop("all")
        
        if df.count()==0:
            return "No Data in file"
		
		
        # Add RUN_ID, FILE NAME and YEARMO columns  
        df= df.with_column("run_id", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S"))) 
        df=df.with_column("filename",lit(file_name))
        df=df.with_column("crt_dttm",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))

        

        
        # Creating Final Dataframe
        final_df = df.select("distributorid","arcode","arname","araddress","telephone","fax","city","region","saledistrict","saleoffice","salegroup","artypecode","saleemployee","salename","billno","billmoo","billsoi","billroad","billsubdist","billdistrict","billprovince","billzipcode","activestatus","routestep1","routestep2","routestep3","routestep4","routestep5","routestep6","routestep7","routestep8","routestep9","routestep10","store","pricelevel","salesarename","branchcode","frequencyofvisit","filename","run_id","crt_dttm")


        #Delete existing Data for the current file
        
        del_sql = "DELETE FROM " + target_table + " WHERE filename ="+"''" + (file_name)+"''"
        session.sql(del_sql).collect()
        
        # Load Data to the target table
        final_df.write.mode("append").saveAsTable(stage_name.split(".")[0]+"."+target_table)

        # write to success folder
    
        file_name=file_name.split(".")[0]+''_''+datetime.now().strftime("%Y%m%d%H%M%S")
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        
        #move to success
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
CREATE OR REPLACE PROCEDURE TH_MBOX_INVENTORY_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS 'from snowflake.snowpark.functions import col, lit, date_format, current_timestamp, to_date, year, month, concat, format_number, regexp_replace,to_timestamp,when,trim,upper
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DecimalType
import snowflake.snowpark
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz
import pandas as pd

def main(session: snowpark.Session,Param): 
    #Param=["I_KCS2402282300.txt","THASDL_RAW.DEV_LOAD_STAGE_ADLS","dev/GT_Intervention/DnA_VMR/cert-data-lake/dms_source/processed_file","SDL_TH_DMS_INVENTORY_FACT"]
    try:
       file_name       = Param[0]
       stage_name      = Param[1]
       temp_stage_path = Param[2]
       target_table    = Param[3]
       
       df_schema = StructType([
        StructField("recdate", StringType(), nullable=True),
        StructField("distributorid", StringType(), nullable=True),
        StructField("whcode", StringType(), nullable=True),
        StructField("productcode", StringType(), nullable=True),
        StructField("qty", StringType(), nullable=True),
        StructField("amount", StringType(), nullable=True),
        StructField("batchno", StringType(), nullable=True),
        StructField("expirydate", StringType(), nullable=True)
        ])
       df = session.read\\
                    .schema(df_schema)\\
                    .option("skip_header",0)\\
                    .option("field_delimiter", "\\t")\\
                    .option("field_optionally_enclosed_by", "\\"") \\
                    .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
        
       df=df.na.drop("all")
       if df.count()==0 :
           return "No Data in file"
       
       df= df.with_column("run_id", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S"))) 
       df=df.with_column("file_name",lit(file_name))
       df=df.with_column("crt_dttm",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))

       snowdf=df.select(    "recdate",
                            "distributorid",
                            "whcode",
                            "productcode",
                            "qty",
                            "amount",
                            "batchno",
                            "expirydate",
                            "crt_dttm",
                            "run_id",
                            "file_name")
            
       #snowdf= snowdf.filter(snowdf["distributorid"].isNotNull())
    
       #if snowdf.count()==0 :
                #return "No Data in table"
                
            
            #move file into success folder
       file_name=file_name.split(".")[0]+"_"+datetime.now().strftime("%Y%m%d%H%M%S")
       
       current_date = datetime.now()
       formatted_year = current_date.strftime("%Y")
       formatted_month = current_date.strftime("%m")
        
        #move to success
       snowdf.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)
    
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
CREATE OR REPLACE PROCEDURE TH_MBOX_SALETOOL_CUSTOMER_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS 'from snowflake.snowpark.functions import col, lit, date_format, current_timestamp, to_date, year, month, concat, format_number, regexp_replace,to_timestamp,when,trim,upper
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DecimalType
import snowflake.snowpark
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz
import pandas as pd

def main(session: snowpark.Session,Param):

    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        df_schema = StructType([
            StructField("distributorid", StringType(), nullable=True),
            StructField("arcode", StringType(), nullable=True),
            StructField("arname", StringType(), nullable=True),
            StructField("araddress", StringType(), nullable=True),
            StructField("telephone", StringType(), nullable=True),
            StructField("fax", StringType(), nullable=True),
            StructField("city", StringType(), nullable=True),
            StructField("region", StringType(), nullable=True),
            StructField("saledistrict", StringType(), nullable=True),
            StructField("saleoffice", StringType(), nullable=True),
            StructField("salegroup", StringType(), nullable=True),
            StructField("artypecode", StringType(), nullable=True),
            StructField("saleemployee", StringType(), nullable=True),
            StructField("salename", StringType(), nullable=True),
            StructField("billno", StringType(), nullable=True),
            StructField("billmoo", StringType(), nullable=True),
            StructField("billsoi", StringType(), nullable=True),
            StructField("billroad", StringType(), nullable=True),
            StructField("billsubdist", StringType(), nullable=True),
            StructField("billdistrict", StringType(), nullable=True),
            StructField("billprovince", StringType(), nullable=True),
            StructField("billzipcode", StringType(), nullable=True),
            StructField("activestatus", StringType(), nullable=True),
            StructField("routestep1", StringType(), nullable=True),
            StructField("routestep2", StringType(), nullable=True),
            StructField("routestep3", StringType(), nullable=True),
            StructField("routestep4", StringType(), nullable=True),
            StructField("routestep5", StringType(), nullable=True),
            StructField("routestep6", StringType(), nullable=True),
            StructField("routestep7", StringType(), nullable=True),
            StructField("routestep8", StringType(), nullable=True),
            StructField("routestep9", StringType(), nullable=True),
            StructField("routestep10", StringType(), nullable=True),
            StructField("store", StringType(), nullable=True),
            StructField("sourcefile", StringType(), nullable=True),
            StructField("old_custid", StringType(), nullable=True),
            StructField("modifydate", StringType(), nullable=True)
             ])
        df = session.read\\
                    .schema(df_schema)\\
                    .option("skip_header",0)\\
                    .option("field_delimiter", "\\t")\\
                    .option("field_optionally_enclosed_by", "\\"") \\
                    .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
        
        df=df.na.drop("all")
        if df.count()==0 :
            return "No Data in file"


        df= df.with_column("run_id", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S"))) 
        df=df.with_column("file_name",lit(file_name))
        df=df.with_column("curr_date",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))

        snowdf=df.select(   "distributorid", "arcode", "arname", "araddress", "telephone", "fax", "city", "region",
                            "saledistrict", "saleoffice", "salegroup", "artypecode", "saleemployee", "salename",
                            "billno", "billmoo", "billsoi", "billroad", "billsubdist", "billdistrict", "billprovince",
                            "billzipcode", "activestatus", "routestep1", "routestep2", "routestep3", "routestep4",
                            "routestep5", "routestep6", "routestep7", "routestep8", "routestep9", "routestep10",
                            "store", "sourcefile", "old_custid", "modifydate", "curr_date", "run_id", "file_name"
                        )
                                    
        #snowdf= snowdf.filter(snowdf["distributorid"].isNotNull())
    
        #if snowdf.count()==0 :
                #return "No Data in table"
                
            
            #move file into success folder
        file_name=file_name.split(".")[0]+"_"+datetime.now().strftime("%Y%m%d%H%M%S")
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        
        #move to success
        snowdf.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)
    
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
CREATE OR REPLACE PROCEDURE TH_MBOX_SELLOUT_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS 'from snowflake.snowpark.functions import col, lit, date_format, current_timestamp, to_date, year, month, concat, format_number, regexp_replace,to_timestamp,when,trim,upper
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DecimalType
import snowflake.snowpark
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz
import pandas as pd

def main(session: snowpark.Session,Param):

    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        
        df_schema = StructType([
            StructField("distributorid", StringType(), nullable=True),
            StructField("orderno", StringType(), nullable=True),
            StructField("orderdate", StringType(), nullable=True),
            StructField("arcode", StringType(), nullable=True),
            StructField("arname", StringType(), nullable=True),
            StructField("city", StringType(), nullable=True),
            StructField("region", StringType(), nullable=True),
            StructField("saledistrict", StringType(), nullable=True),
            StructField("saleoffice", StringType(), nullable=True),
            StructField("salegroup", StringType(), nullable=True),
            StructField("artypecode", StringType(), nullable=True),
            StructField("saleemployee", StringType(), nullable=True),
            StructField("salename", StringType(), nullable=True),
            StructField("productcode", StringType(), nullable=True),
            StructField("productdesc", StringType(), nullable=True),
            StructField("megabrand", StringType(), nullable=True),
            StructField("brand", StringType(), nullable=True),
            StructField("baseproduct", StringType(), nullable=True),
            StructField("variant", StringType(), nullable=True),
            StructField("putup", StringType(), nullable=True),
            StructField("grossprice", StringType(), nullable=True),
            StructField("qty", StringType(), nullable=True),
            StructField("subamt1", StringType(), nullable=True),
            StructField("discount", StringType(), nullable=True),
            StructField("subamt2", StringType(), nullable=True),
            StructField("discountbtline", StringType(), nullable=True),
            StructField("totalbeforevat", StringType(), nullable=True),
            StructField("total", StringType(), nullable=True),
            StructField("linenumber", StringType(), nullable=True),
            StructField("iscancel", StringType(), nullable=True),
            StructField("cndocno", StringType(), nullable=True),
            StructField("cnreasoncode", StringType(), nullable=True),
            StructField("promotionheader1", StringType(), nullable=True),
            StructField("promotionheader2", StringType(), nullable=True),
            StructField("promotionheader3", StringType(), nullable=True),
            StructField("promodesc1", StringType(), nullable=True),
            StructField("promodesc2", StringType(), nullable=True),
            StructField("promodesc3", StringType(), nullable=True),
            StructField("promocode1", StringType(), nullable=True),
            StructField("promocode2", StringType(), nullable=True),
            StructField("promocode3", StringType(), nullable=True),
            StructField("avgdiscount", StringType(), nullable=True)
        ])

        df = session.read\\
                    .schema(df_schema)\\
                    .option("skip_header",0)\\
                    .option("field_delimiter", "\\t")\\
                    .option("field_optionally_enclosed_by", "\\"") \\
                    .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
        
        df=df.na.drop("all")
        if df.count()==0 :
            return "No Data in file"
        


        df= df.with_column("run_id", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S"))) 
        df=df.with_column("file_name",lit(file_name.split(".")[0]+".csv"))
        df=df.with_column("curr_date",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))

        snowdf=df.select(    "distributorid", "orderno", "orderdate", "arcode", "arname", "city", "region", 
                            "saledistrict", "saleoffice", "salegroup", "artypecode", "saleemployee", "salename", 
                            "productcode", "productdesc", "megabrand", "brand", "baseproduct", "variant", "putup", 
                            "grossprice", "qty", "subamt1", "discount", "subamt2", "discountbtline", "totalbeforevat", 
                            "total", "linenumber", "iscancel", "cndocno", "cnreasoncode", "promotionheader1", 
                            "promotionheader2", "promotionheader3", "promodesc1", "promodesc2", "promodesc3", 
                            "promocode1", "promocode2", "promocode3", "avgdiscount", "curr_date", "run_id", "file_name")
        
            
        
            
            #move file into success folder
        file_name=file_name.split(".")[0]+"_"+datetime.now().strftime("%Y%m%d%H%M%S")
        
    
            #write on sdl layer
        
        snowdf.write.mode("append").saveAsTable(stage_name.split(".")[0]+"."+target_table)
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        
        #move to success
        snowdf.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)
            
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
CREATE OR REPLACE PROCEDURE TH_MT_PRICE_MULTISHEET_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python','pandas','openpyxl')
HANDLER = 'main'
EXECUTE AS OWNER
AS '

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

    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        db_name         = stage_name.split(''.'')[0]
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
        
        with SnowflakeFile.open(full_path, ''rb'', require_scoped_url = False) as f:
            wb = load_workbook(f)
            sheet_names= wb.sheetnames
            sheet_names= sheet_names[1:-1]
    
            #creating empty list to append data
            stacked_data=[]
        
            #iterate over each sheet 
            for sheet_name in sheet_names:
                
                #skipping the first two rows
                df =pd.read_excel(f, engine=''openpyxl'' ,
                                       sheet_name=sheet_name, skiprows=2)
                
                #adding source column with respective sheet name as value
                df["Source"]= sheet_name
                #append the processed dataframe to the list
                stacked_data.append(df)
                                   
            #concatnate the stacked data into a single dataframe
            stacked_df= pd.concat(stacked_data, axis=0, ignore_index=True)
            stacked_df.rename( columns={''Unnamed: 0'':''company''}, inplace=True )

            for column in stacked_df.columns:
                stacked_df[column] = stacked_df[column].apply(str)
            
            final_df = session.create_dataframe(stacked_df, df_schema)
            final_df = final_df.withColumn("FILE_NAME",lit(file_name).cast("string"))
            final_df = final_df.withColumn("CRT_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
            final_df.write.mode("append").saveAsTable(target_table)
            current_date = datetime.now()
            formatted_year = current_date.strftime("%Y")
            formatted_month = current_date.strftime("%m")
        
            #move to success
            final_df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)
            
        return "Success"
        
    except Exception as e:
        # Handle exceptions here
        error_message = f"ERROR: {str(e)}"
        return error_message
';
CREATE OR REPLACE PROCEDURE TH_MT_TOPS_7_11_PREPROCESSING("PARAM" ARRAY)
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
		df=df.na.drop("all")
		if df.count()==0:
            return "No Data in file"

        
        #convertin time stamp into sg timezone
        df = df.withColumn("CRT_DTTM", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        df = df.withColumn("FILE_NAME", lit(file_name).cast("string"))
        df = df.withColumn("RUN_ID", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        
        # Creating copy of the Dataframe
        final_df = df.alias("final_df")


        
        # Load Data to the target table
        final_df.write.mode("append").saveAsTable(target_table)

    
        # write to success folder
    
        file_name=file_name.split(".")[0]+''_''+datetime.now().strftime("%Y%m%d%H%M%S")
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        
        #move to success
        final_df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)
   
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
CREATE OR REPLACE PROCEDURE TH_MYM_CUST_SALES_PREFORMAT("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python','pandas','openpyxl')
HANDLER = 'main'
EXECUTE AS CALLER
AS '

import snowflake.snowpark as snowpark
from snowflake.snowpark.files import SnowflakeFile
from snowflake.snowpark.functions import lit
from datetime import datetime
import os,sys
import pandas as pd
import pytz
def main(session: snowpark.Session, Param):


    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        db_name         = stage_name.split(''.'')[0]
        target_table    = db_name+"."+Param[3]
    
        full_path = "@"+stage_name+"/"+temp_stage_path+"/"+file_name
        savefile  = "Reformatted_"+file_name
        with SnowflakeFile.open(full_path, ''rb'', require_scoped_url = False) as f:
            mym_sales = pd.read_excel(f)
            mym_period = mym_sales.iloc[0, 0]
            mym_cust_group = mym_sales.iloc[1, 0]
            mym_cols = mym_sales.iloc[2].values
            mym_sales = pd.DataFrame(data=mym_sales.iloc[3:, :].values, columns=mym_cols)
            mym_cust_list = mym_sales.loc[(~mym_sales[''Item No.''].isnull()) & (mym_sales[''Description''].isnull())][''Item No.''].values
            mym_cust_list = mym_cust_list[:-1]
            mym_cust_code = [x.split(":")[1] for x in mym_cust_list]
            mym_cust_name = [x.split(":")[2] for x in mym_cust_list]
            mym_sales = mym_sales.dropna()
            mym_sales[''old_index''] = mym_sales.index
        
            increment_value = 0
            cust_code = []
            cust_name = []
            for row_num in range(len(mym_sales)):
                cur_index = mym_sales.iloc[row_num][''old_index'']
        
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
            mym_sales[''period''] = mym_period
            mym_sales[''customer_group''] = mym_cust_group
            mym_sales[''customer_code''] = cust_code
            mym_sales[''customer_name''] = cust_name
            mym_sales = mym_sales.drop(columns=[''old_index''])
            if ''FOC'' not in mym_sales.columns:
                mym_sales.insert(loc=3, column=''FOC'', value=0)
            mym_sales = mym_sales.reset_index(drop=True)
            
        
            
            final_df = session.create_dataframe(mym_sales)
            final_df = final_df.withColumn("FILE_NAME",lit(file_name).cast("string"))
            final_df = final_df.withColumn("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
            final_df = final_df.withColumn("CRT_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
            final_df.write.mode("append").saveAsTable(target_table)
            current_date = datetime.now()
            formatted_year = current_date.strftime("%Y")
            formatted_month = current_date.strftime("%m")
        
            #move to success
            final_df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)
        return "SUCCESS"
        
    except Exception as e:
        # Handle exceptions here
        error_message = f"ERROR: {str(e)}"
        return error_message
';
CREATE OR REPLACE PROCEDURE TH_SFMC_ACTION_COMPLAINT_PREPROCESSING("PARAM" ARRAY)
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
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField,DecimalType,DateType,TimestampType
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
            StructField("OYB_ACCOUNT_ID",StringType()),
            StructField("JOB_ID",StringType()),
            StructField("LIST_ID",StringType()),
            StructField("BATCH_ID",StringType()),
            StructField("SUBSCRIBER_ID",StringType()),
            StructField("SUBSCRIBER_KEY",StringType()),
            StructField("EVENT_DATE",TimestampType()),
            StructField("IS_UNIQUE",StringType()),
            StructField("DOMAIN",StringType()),
            StructField("EMAIL_SUBJECT",StringType()),
            StructField("EMAIL_NAME",StringType()),
            StructField("EMAIL_ID",StringType())
            ])


        # Read the CSV file into a DataFrame
    
        dataframe = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "|")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)


        dataframe=dataframe.na.drop("all")
		if dataframe.count()==0:
            return "No Data in file"

        #---------------------------Transformation logic ------------------------------#

        # Handle null values or empty rows
        dataframe=dataframe.na.drop("all")

         # Add FILE_NAME and CRTD_DTTM to the dataframe
        new_file_name=file_name[0:28] +".csv"

        dataframe = dataframe.with_column("FILE_NAME",lit(new_file_name))
        dataframe = dataframe.with_column("CRTD_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))


        # Creating copy of the Dataframe
        final_df = dataframe.alias("final_df")




        # Load Data to the target table
        final_df.write.mode("append").saveAsTable(target_table)
        
        # write to success folder
    
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")

        # write to success folder
    
        file_name=file_name.split(".")[0]
        final_df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)
        
        return "Success"


    except KeyError as key_error:

        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message
        
    except Exception as e:
 
        error_message = f"Error: {str(e)}"
        return error_message

        ';
