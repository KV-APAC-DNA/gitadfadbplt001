-- version History--------------------------------
-- 02/02/24 	Thanish		Initial Version
-- 01/03/24	Srihari		Added Header logic to Handle files with '|' Delimiter
-- 05/03/24	Srihari		Added Exception handling
-- 06/03/24	Thanish		Added file name logic to Handle Thailand files
-- 09/03/24 Srihari     Added Logic to handle regular expressions in name and header
-- 12/03/24 Thanish		Added Logic to Handle NTUC Multi xlsx file header
-- 22/03/24 Thanish		Added Header Logic to handle CRM files (Thailand) 
-- 29/03/24 Thanish     Header Logic to handle Aus files (PAC)
-- 4/4/24   Shantanu,Mahima,Thanish 
-- 10/4/24  Thanish     Header handling for PH market
-- 29/4/24  Latest Update
-- 3/5/24   Saurabh, Thanish
-- 10/5/24  Thanish
-- 14/5/24  Thanish
-- 22/5/24  Thanish
-- 29/5/24  Thanish
-- 06/6/24  Srihari     Header handled if sheetname is used instead of sheet_index
-- 13/6/24  Srihari     Header handled if source is SFMC


CREATE OR REPLACE PROCEDURE DEV_DNA_LOAD.ASPSDL_RAW.FILE_VALIDATION("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('openpyxl==3.1.2','regex==2023.10.3','snowflake-snowpark-python==*','xlrd==2.0.1')
HANDLER = 'main'
EXECUTE AS OWNER
AS '# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them.

import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col
from snowflake.snowpark.files import SnowflakeFile
import math
import regex
import pandas as pd
from datetime import datetime

def main(session: snowpark.Session,Param):
    try:

        # Example input
        # Your code goes here, inside the "main" handler.
        # Return value will appear in the Results tab
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
        header_reg          =  Param[9]
        sheet_names         =  Param[10]

        FileNameValidation,FileExtnValidation,FileHeaderValidation = validation.split("-")
        counter             =  0 

        if FileNameValidation=="0" and FileExtnValidation=="0" and FileHeaderValidation=="0":
             return "SUCCESS: File validation passed"
            
        
    
        # If the File belongs to Regional, then it enters the function
    
        if stage_name.split(".")[0]=="ASPSDL_RAW":
            processed_file_name=rg_travel_validation(CURRENT_FILE)
            
        # If the File belongs to Thailand, then it enters the function
        elif stage_name.split(".")[0]=="THASDL_RAW": 
            processed_file_name=thailand_processing(CURRENT_FILE)

        elif stage_name.split(".")[0]=="PCFSDL_RAW":
            processed_file_name=aus_processing(CURRENT_FILE)

        elif stage_name.split(".")[0]=="NTASDL_RAW":
            processed_file_name=north_asia_processing(CURRENT_FILE)

        elif stage_name.split(".")[0]=="IDNSDL_RAW":
            processed_file_name=indonesia_processing(CURRENT_FILE)

        else:
            processed_file_name=CURRENT_FILE

    
        #Extracting the filename based on index variable
        
        if index.lower() == "last":
            extracted_filename = processed_file_name.rsplit("_", 1)[0]
            print(extracted_filename)
        elif index.lower() == "first":
            extracted_filename = processed_file_name.split("_")[0]
            print(extracted_filename)
        elif index.lower() == "full":
            extracted_filename = processed_file_name.rsplit(".", 1)[0]
            print(extracted_filename)
        elif index =="name_mmmyyyy.xlsx" or index =="name_yyyymmww.xlsx" or index =="name_yyyymmww.xls":
            extracted_filename=CURRENT_FILE.split(".")[0]

    
    
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
    
        if FileHeaderValidation == "1" and counter==0:

            # Converting the extension from xlsx to csv
            # Extracting the Header from the file
            
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

            elif "BU_" in CURRENT_FILE or "BP_" in CURRENT_FILE :
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
            
            elif "HCPSDL_RAW" in stage_name and "/SFMC/" in temp_stage_path :
                file_name= CURRENT_FILE.replace("xlsx","csv")
                file_name = file_name.replace("(", "").replace(")", "").replace(" ","_")
                utf_encoding= ''UTF-16LE''
                df = session.read.option("INFER_SCHEMA", True).option("field_optionally_enclosed_by", "\\"").option("encoding",utf_encoding).csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

                df_pandas=df.to_pandas()
                header=df_pandas.iloc[int(file_header_row_num)].tolist()

            elif sheet_names != "[]":
                file_name = sheet_names[1:-1].split(",")[0]
                file_name = file_name.strip("\\"").replace("(", "").replace(")", "").replace(" ","_")+".csv"
                df = session.read.option("INFER_SCHEMA", True).option("field_optionally_enclosed_by", "\\"").csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
                df_pandas=df.to_pandas()
                header=df_pandas.iloc[int(file_header_row_num)].tolist()                
                       
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
                
            else:
                header_pipe_split = header[0].split(''|'')
                
            if (val_file_extn==''xlsx'' or val_file_extn==''xls'') and ''Weekly Sales Report'' not in val_file_name and CURRENT_FILE[0:3]!="ROB" and CURRENT_FILE[0:2]!="SS" and "FSSI_Week" not in CURRENT_FILE and "JJ_KPI_Status" not in CURRENT_FILE and "TW_POS_PXCivilia" not in CURRENT_FILE and ''TW_POS_RTMart_RawData'' not in  CURRENT_FILE and "MT01P39R" not in CURRENT_FILE and "Weekly_Summary_Trexi_raw_data" not in CURRENT_FILE and "Naver_keyword" not in CURRENT_FILE:
                result_list = header[0].split(''\\x01'')
            elif val_file_extn==''xlsx'':
                result_list = header
        
            elif len(header_pipe_split)>1:
                result_list = header_pipe_split
            else:
                result_list = header


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
                

            else:
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

            space_split=val_header.split(" ")
            if len(space_split) > 1:
                final_val_header=space_split

            tab_split=val_header.split("	")
            if len(tab_split) > 1:
                final_val_header=tab_split

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
            # Handle exceptions here
            error_message = f"FAILED: {str(e)}"
            return error_message


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


def north_asia_processing(CURRENT_FILE):
    var_1=CURRENT_FILE.find("_")
    new_file=CURRENT_FILE[var_1+1:].split(".")[0]
    bol_th_pos6=[i for i in ["pop_lists","product_lists_allocation","product_lists_pops","product_lists_products","pops","products","users"] if i == new_file]
    if len(bol_th_pos6) >0:
        file=("_").join(CURRENT_FILE.split("_")[1:])

    elif "NU_RI_ZON" in CURRENT_FILE:
        file=CURRENT_FILE.replace("_"," ",2)

    elif "KR_POS_GS_Super" in CURRENT_FILE:
        file=CURRENT_FILE.rsplit("_",2)[0]

    else:
        file = CURRENT_FILE.replace(" ","_")
        print("FileName : ", file)
        
    return file

def indonesia_processing(CURRENT_FILE):
    if "PROMO_COMPETITOR" in CURRENT_FILE or "BRAND_BLOCKING" in CURRENT_FILE or "VISIBILITY" in CURRENT_FILE or "PLANOGRAM" in CURRENT_FILE or "PRICING" in CURRENT_FILE or "SECONDARY_DISPLAY" in CURRENT_FILE or "PROMO" in CURRENT_FILE or "PRODUCT_AVAILABILITY" in CURRENT_FILE:
        file=CURRENT_FILE.replace("_"," ",1)
        print(file)

    else:
        file = CURRENT_FILE.replace(" ","_")
        print("FileName : ", file)

    return file

    

# Function to Perform File name validation

def file_validation(counter,extracted_filename,val_file_name):


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

        elif "Sellout_Superindo" in extracted_filename or "Stock_Superindo" in extracted_filename or "Sellout_Alfmidi" in extracted_filename or "Stock_Alfmidi" in extracted_filename or "Sellout_Alfamart" in extracted_filename or "Stock_Alfamart" in extracted_filename  or "Sellout_Carrefour" in extracted_filename or "Stock_Carrefour" in extracted_filename or "Diamond" in extracted_filename or "Stock_Guardian" in extracted_filename or "Indomaret" in extracted_filename or "Indogrosir" in extracted_filename:
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
