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
