CREATE OR REPLACE PROCEDURE ASPSDL_RAW.FILE_VALIDATION("PARAM" ARRAY)
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

    # Example input
    # Param=[''LSTR 112022.xlsx'',''last'',''1-1-1'',''LSTR'',''xlsx'',''Brand_name|Barcode|Item_Code|English_Desc|Chinese_Desc|Category|SRP_USD|Unit|Amt|Unit|Amt|Unit|Amt|Stock'',2,''ASPSDL_RAW.DEV_LOAD_STAGE_ADLS'',''dev/transactional/Lagardere'']

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
    
        
        for i in range(max(file_header_count, val_header_count)):
            if i < file_header_count and i < val_header_count:
                if file_header[i] != final_val_header[i]:
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
            file_header_validation_status="Header validation Failed"+" , unmatched columns found in index "+ str(index) +" and columns are" + str(file_header_rejected_list)
            print("file_header_validation_status",file_header_validation_status)
            counter = counter+4

            # Return Fail message if values found in extra columns list
        elif len(extra_columns)!=0:
            file_header_validation_status="Header validation Failed, unmatched columns found in index " + str(index) + " and columns are " + str(file_header_rejected_list) + " ; extra columns found in file header! " + str(extra_columns)
            print("file_header_validation_status",file_header_validation_status)
            counter = counter+4

        else:
            file_header_validation_status="Header validation Failed, columns missing from file header!" + str(val_rejected_list)
            counter = counter+4
        
            
        return file_header_validation_status,counter


            





    
        

    ';
