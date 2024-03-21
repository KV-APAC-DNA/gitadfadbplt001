use schema thasdl_raw;
CREATE OR REPLACE PROCEDURE LA_GT_VISIT_PREPROCESSING("PARAM" ARRAY)
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
from snowflake.snowpark.functions import col,lit,md5,trim,coalesce,upper,concat,to_timestamp,to_date
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField
import pandas as pd
from datetime import datetime
import pytz

def main(session: snowpark.Session,Param): 

    try:

        #Param=[''Visit_20240317232501_20240318040301.txt'',''THASDL_RAW.DEV_LOAD_STAGE_ADLS'',''dev/cert_data_lake/LCM/Laos_Visit_Data/'',''SDL_LA_GT_VISIT'']

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
    
        dataframe = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "|")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .option("skip_blank_lines", True)\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        #---------------------------Transformation logic ------------------------------#

        # Handle null values or empty rows
        dataframe=dataframe.na.drop("all")

        # Check for empty Dataframe
        if dataframe.count()==0:
            return "No Data in file"


        # Add File name, run_id and crt_dttm to the dataframe
        run_id=file_name.split(".")[0].split("_")[2]

        dataframe = dataframe.withColumn("FILENAME",lit(file_name).cast("string"))
        dataframe = dataframe.with_column("RUN_ID",lit(run_id).cast("string"))
        dataframe = dataframe.with_column("CRT_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))

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
CREATE OR REPLACE PROCEDURE MYM_CUST_SALES_PREFORMAT("PARAM" ARRAY)
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

    # file_path, sheet, target_stage 
    # Param = [
    #     # ''TH_Action_Open_20230422_20230422170714.csv'',
    #     ''Customer_Sale_Report_28.06.2023.csv'',
    #     ''THASDL_RAW.DEV_LOAD_STAGE_ADLS'',
    #     ''dev/cert_data_lake/LCM/LCM_myanmar_Sales_Data'',
    #     ''Temp_Cust_sales''
    # ]
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        sch_name        = stage_name.split(''.'')[0]
        target_table    = sch_name+"."+Param[3]
    
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
CREATE OR REPLACE PROCEDURE MYM_CUST_SALES_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python','pandas','openpyxl')
HANDLER = 'main'
EXECUTE AS OWNER
AS '

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
    #     # ''TH_Action_Open_20230422_20230422170714.csv'',
    #     ''Customer_Sale_Report_28.06.2023.csv'',
    #     ''THASDL_RAW.DEV_LOAD_STAGE_ADLS'',
    #     ''dev/cert_data_lake/LCM/LCM_myanmar_Sales_Data'',
    #     ''Temp_Cust_sales''
    # ]
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        sch_name        = stage_name.split(''.'')[0]
        target_table    = sch_name+"."+Param[3]
    
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


CREATE OR REPLACE PROCEDURE MYM_SALES_STOCK_REPORT_PREPROCESSING("PARAM" ARRAY)
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

def main(session: snowpark.Session,Param): 
    #Param = ["Reformatted_Customer_Sale_Report_03.02.2024.csv", "THASDL_RAW.DEV_LOAD_STAGE_ADLS", "dev/cert_data_lake/LCM/LCM_myanmar_Sales_Data", "SDL_MYM_GT_SALES_REPORT_FACT"]
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        sch_name        = stage_name.split(''.'')[0]
        target_table    = sch_name+"."+Param[3]
    
        df_schema = StructType([
                    StructField("item_no", StringType()),
                    StructField("description", StringType()),
                    StructField("qty_sold", StringType()),
                    StructField("stock", StringType())
                ])
        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "\\u0001")\\
            .option("field_optionally_enclosed_by", "\\"") \\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
		
		df=df.na.drop("all")

        # Check if the Dataframe is having Data

        if df.count()==0:
            return "No Data in file"
			

        df = df.withColumn("FILE_NAME",lit(file_name).cast("string"))
        df = df.withColumn("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        df = df.withColumn("CRT_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
                
        final_df= df.select( "item_no", "description", "qty_sold", "stock", "FILE_NAME", "RUN_ID", "CRT_DTTM")
        
            
        
        #move file into success folder
        file_name=file_name.split(".")[0]+"_"+datetime.now().strftime("%Y%m%d%H%M%S")
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        
        #move to success
        final_df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)

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
        
';
CREATE OR REPLACE PROCEDURE PERFECT_STORE_CONSUMERREACH_CVS_711_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '# This Preprocessing Code can be used for both Perfect store- 
# consumerreach_cvs and consumerreach_711

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
        #Param=[''jnj_consumerreach_cvs_202401.csv'',''THASDL_RAW.DEV_LOAD_STAGE_ADLS'',''dev/perfect-store/transaction/'',''SDL_JNJ_CONSUMERREACH_CVS'']

        # Parameters for consumerreach_711
        #Param=[''jnj_consumerreach_711_202401.csv'',''THASDL_RAW.DEV_LOAD_STAGE_ADLS'',''dev/perfect-store/transaction/'',''SDL_JNJ_CONSUMERREACH_711'']
        
        # Extracting parameters from the input
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]

        # Define the schema for the DataFrame
        df_schema=StructType([
            StructField("ID",StringType()),
            StructField("CDATE",StringType()),
            StructField("RETAIL",StringType()),
            StructField("RETAILNAME",StringType()),
            StructField("RETAILBRANCH",StringType()),
            StructField("RETAILPROVINCE",StringType()),
            StructField("JJSKUBARCODE",StringType()),
            StructField("JJSKUNAME",StringType()),
            StructField("JJCORE",StringType()),
            StructField("DISTRIBUTION",StringType()),
            StructField("STATUS",StringType())
            ])

        
        # Read the CSV file into a DataFrame
    
        dataframe = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "\\u0001")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)


        #---------------------------Transformation logic ------------------------------#

        # Handle null values or empty rows
        dataframe=dataframe.na.drop("all")

        # Check if the Dataframe is having Data

        if dataframe.count()==0:
            return "No Data in file"

        # Add RUN_ID, FILE NAME and YEARMO columns

        dataframe = dataframe.with_column("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        dataframe = dataframe.with_column("FILE_NAME",lit(file_name))

        # Extract year month from the file
        yearmo= file_name.split("_")[-1].split(".")[0]

        dataframe = dataframe.with_column("YEARMO",lit(yearmo))


        # Creating Final Dataframe
        final_df = dataframe.alias("final_df")


        # Delete existing Data for the current file
        
        del_sql = "DELETE FROM " + target_table + " WHERE FILE_NAME ="+"''" + (file_name)+"''"
        session.sql(del_sql).collect()
        
        # Load Data to the target table
        final_df.write.mode("append").saveAsTable(target_table)

        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")

        # write to success folder
    
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
        return error_message';
CREATE OR REPLACE PROCEDURE PERFECT_STORE_COP_PREPROCESSING("PARAM" ARRAY)
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
from snowflake.snowpark.functions import col,lit,concat
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField,DecimalType,DateType
import pandas as pd
from datetime import datetime
import pytz

def main(session: snowpark.Session,Param):

    try:

        #Param=[''JNJ_Mer_-_COP_1_202401.csv'',''THASDL_RAW.DEV_LOAD_STAGE_ADLS'',''dev/perfect-store/transaction/'',''SDL_JNJ_MER_COP'']

        # Extracting parameters from the input
        file_name       = Param[0]
        escaped_file_name = file_name.replace("(", "").replace(")", "").replace(" ",''_'')
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]

        # Define the schema for the DataFrame
        df_schema=StructType([
            StructField("COP_DATE",StringType()),
            StructField("EMP_ADDRESS_PC",StringType()),
            StructField("PC_NAME",StringType()),
            StructField("SURVEY_NAME",StringType()),
            StructField("EMP_ADDRESS_SUPERVISOR",StringType()),
            StructField("SUPERVISOR_NAME",StringType()),
            StructField("ACTIVITY",StringType()),
            StructField("CATEGORY",StringType()),
            StructField("BRAND",StringType()),
            StructField("START_DATE",StringType()),
            StructField("END_DATE",StringType()),
            StructField("AREA",StringType()),
            StructField("CHANNEL",StringType()),
            StructField("ACCOUNT",StringType()),
            StructField("STORE_ID",StringType()),
            StructField("STORE_NAME",StringType()),
            StructField("COMPLIANCE",StringType()),
            StructField("QUESTION",StringType()),
            StructField("ANSWER",StringType())
            ])


        # Read the CSV file into a DataFrame
        # escaped_file_name = file_name.replace("(", "").replace(")", "")
        # print(escaped_file_name)
        
        dataframe = session.read\\
            .schema(df_schema)\\
            .option("skip_header",2)\\
            .option("field_delimiter", "\\u0001")\\
            .option("encoding", "UTF-8")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+escaped_file_name)
         

        #---------------------------Transformation logic ------------------------------#

        # Handle null values or empty rows
        dataframe=dataframe.na.drop("all")
        
        # Check if the Dataframe is having Data

        if dataframe.count()==0:
            return "No Data in file"

        # Add COP_PRIORITY,RUN_ID, FILE NAME and YEARMO columns
        
        dataframe = dataframe.withColumn("COP_PRIORITY", lit(dataframe["ACTIVITY"]))
        dataframe = dataframe.with_column("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))

        # Extract year month from the file
        yearmo= file_name.split("_")[-1].split(".")[0]

        dataframe = dataframe.with_column("YEARMO",lit(yearmo))
        
        # Preparing the File_name column
        new_file_name="(JNJ Mer) - COP_1_"+yearmo+".csv"
        dataframe = dataframe.with_column("FILE_NAME",lit(new_file_name))


        # Creating Final Dataframe
        
        final_df = dataframe.select("COP_DATE","EMP_ADDRESS_PC","PC_NAME",
                                    "SURVEY_NAME","EMP_ADDRESS_SUPERVISOR","SUPERVISOR_NAME","COP_PRIORITY","START_DATE",
                                    "END_DATE","AREA","CHANNEL","ACCOUNT","STORE_ID","STORE_NAME","QUESTION","ANSWER",
                                    "RUN_ID","FILE_NAME","YEARMO","ACTIVITY","CATEGORY","BRAND","COMPLIANCE")
         
        # Delete existing Data for the current file
        
        del_sql = "DELETE FROM " + target_table + " WHERE FILE_NAME ="+"''" + (new_file_name)+"''"
        session.sql(del_sql).collect()
        
        # Load Data to the target table
        final_df.write.mode("append").saveAsTable(target_table)

        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")

        # write to success folder
    
        file_name=new_file_name.split(".")[0]+''_''+datetime.now().strftime("%Y%m%d%H%M%S")
        
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
CREATE OR REPLACE PROCEDURE PERFECT_STORE_OSA_OSS_REPORT_PREPROCESSING("PARAM" ARRAY)
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
from snowflake.snowpark.functions import col,lit,concat
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField,DecimalType,DateType
import pandas as pd
from datetime import datetime
import pytz

def main(session: snowpark.Session,Param):

    try:

        #Param=[''JNJ_OSA_and_OOS_Report_202401.csv'',''THASDL_RAW.DEV_LOAD_STAGE_ADLS'',''dev/perfect-store/transaction/'',''SDL_JNJ_OSA_OOS_REPORT'']

        # Extracting parameters from the input
        file_name       = Param[0]
        escaped_file_name = file_name.replace("(", "").replace(")", "").replace(" ",''_'')
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]

        # Define the schema for the DataFrame
        df_schema=StructType([
            StructField("OSA_OOS_DATE",StringType()),
            StructField("WEEK",StringType()),
            StructField("EMP_ADDRESS_PC",StringType()),
            StructField("PC_NAME",StringType()),
            StructField("EMP_ADDRESS_SUPERVISOR",StringType()),
            StructField("SUPERVISOR_NAME",StringType()),
            StructField("AREA",StringType()),
            StructField("CHANNEL",StringType()),
            StructField("ACCOUNT",StringType()),
            StructField("STORE_ID",StringType()),
            StructField("STORE_NAME",StringType()),
            StructField("SHOP_TYPE",StringType()),
            StructField("BRAND",StringType()),
            StructField("CATEGORY",StringType()),
            StructField("BARCODE",StringType()),
            StructField("SKU",StringType()),
            StructField("MSL_PRICE_TAG",StringType()),
            StructField("OOS",StringType()),
            StructField("OOS_REASON",StringType())
            ])

        # Read the CSV file into a DataFrame
        
        dataframe = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "\\u0001")\\
            .option("encoding", "UTF-8")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+escaped_file_name)
        

        #---------------------------Transformation logic ------------------------------#

        # Handle null values or empty rows
        dataframe=dataframe.na.drop("all")
        
        # Check if the Dataframe is having Data

        if dataframe.count()==0:
            return "No Data in file"

        # Add RUN_ID, FILE NAME and YEARMO columns
        
        dataframe = dataframe.with_column("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))

        # Extract year month from the file
        yearmo= file_name.split("_")[-1].split(".")[0]

        # Preparing the File_name column
        new_file_name="(JNJ) OSA and OOS Report_"+yearmo+".csv"
        dataframe = dataframe.with_column("FILE_NAME",lit(new_file_name))

        dataframe = dataframe.with_column("YEARMO",lit(yearmo))


        # Creating Final Dataframe
        final_df = dataframe.alias("final_df")

        # Delete existing Data for the current file
        
        del_sql = "DELETE FROM " + target_table + " WHERE FILE_NAME ="+"''" + (new_file_name)+"''"
        session.sql(del_sql).collect()
        
        # Load Data to the target table
        final_df.write.mode("append").saveAsTable(target_table)

        
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")

        # write to success folder
    
        file_name=new_file_name.split(".")[0]+''_''+datetime.now().strftime("%Y%m%d%H%M%S")
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
CREATE OR REPLACE PROCEDURE PERFECT_STORE_SHARE_OF_SHELF_PREPROCESSING("PARAM" ARRAY)
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
from snowflake.snowpark.functions import col,lit,concat
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField,DecimalType,DateType
import pandas as pd
from datetime import datetime
import pytz

def main(session: snowpark.Session,Param):

    try:

        #Param=[''(JNJ Mer) - Share of Shelf_202401.csv'',''THASDL_RAW.DEV_LOAD_STAGE_ADLS'',''dev/perfect-store/transaction/'',''SDL_JNJ_MER_SHARE_OF_SHELF'']

        # Extracting parameters from the input
        file_name       = Param[0]
        escaped_file_name = file_name.replace("(", "").replace(")", "").replace(" ",''_'')
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]

        # Define the schema for the DataFrame
        df_schema=StructType([
            StructField("SOS_DATE",StringType()),
            StructField("MERCHANDISER_NAME",StringType()),
            StructField("SUPERVISOR_NAME",StringType()),
            StructField("AREA",StringType()),
            StructField("CHANNEL",StringType()),
            StructField("ACCOUNT",StringType()),
            StructField("STORE_ID",StringType()),
            StructField("STORE_NAME",StringType()),
            StructField("CATEGORY",StringType()),
            StructField("AGENCY",StringType()),
            StructField("BRAND",StringType()),
            StructField("SIZE",StringType())
            ])

        # Read the CSV file into a DataFrame
        
        dataframe = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "\\u0001")\\
            .option("encoding", "UTF-8")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+escaped_file_name)
        

        #---------------------------Transformation logic ------------------------------#

        # Handle null values or empty rows
        dataframe=dataframe.na.drop("all")
        
        # Check if the Dataframe is having Data

        if dataframe.count()==0:
            return "No Data in file"

        # Add RUN_ID, FILE NAME and YEARMO columns
        
        dataframe = dataframe.with_column("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))

        # Extract year month from the file
        yearmo= file_name.split("_")[-1].split(".")[0]

        # Preparing the File_name column
        new_file_name="(JNJ Mer) - Share of Shelf_"+yearmo+".csv"
        dataframe = dataframe.with_column("FILE_NAME",lit(new_file_name))

        dataframe = dataframe.with_column("YEARMO",lit(yearmo))


        # Creating Final Dataframe
        final_df = dataframe.alias("final_df")


        # Delete existing Data for the current file
        
        del_sql = "DELETE FROM " + target_table + " WHERE FILE_NAME ="+"''" + (new_file_name)+"''"
        session.sql(del_sql).collect()
        
        # Load Data to the target table
        final_df.write.mode("append").saveAsTable(target_table)
        
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")

        # write to success folder
    
        file_name=new_file_name.split(".")[0]+''_''+datetime.now().strftime("%Y%m%d%H%M%S")
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
CREATE OR REPLACE PROCEDURE SDL_TH_MT_BIGC("PARAM" ARRAY)
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
            StructField("REPORT_CODE", StringType()),
            StructField("SUPPLIER", StringType()),
            StructField("BUSINESS_FORMAT", StringType()),
            StructField("COMPARE", StringType()),
            StructField("STORE", StringType()),
            StructField("TRANSACTION_DATE", StringType()),
            StructField("LY_COMPARE_DATE", StringType()),
            StructField("REPORT_DATE", StringType()),
            StructField("DIVISION", StringType()),
            StructField("DEPARTMENT", StringType()),
            StructField("SUBDEPARTMENT", StringType()),
            StructField("CLASS", StringType()),
            StructField("SUBCLASS", StringType()),
            StructField("BARCODE", StringType()),
            StructField("ARTICLE", StringType()),
            StructField("ARTICLE_NAME", StringType()),
            StructField("BRAND", StringType()),
            StructField("MODEL", StringType()),
            StructField("SALE_AMT_TY_BAHT", StringType()),
            StructField("SALE_AMT_LY_BAHT", StringType()),
            StructField("SALE_AMT_VAR", StringType()),
            StructField("SALE_QTY_TY", StringType()),
            StructField("SALE_QTY_LY", StringType()),
            StructField("SALE_QTY_VAR", StringType()),
            StructField("STOCK_TY_BAHT", StringType()),
            StructField("STOCK_LY_BAHT", StringType()),
            StructField("STOCK_VAR", StringType()),
            StructField("STOCK_QTY_TY", StringType()),
            StructField("STOCK_QTY_LY", StringType()),
            StructField("STOCK_QTY_VAR", StringType()),
            StructField("DAY_ON_HAND_TY", StringType()),
            StructField("DAY_ON_HAND_LY", StringType()),
            StructField("DAY_ON_HAND_DIFF", StringType())
        ])
        
        df = session.read\\
        .schema(df_schema)\\
        .option("skip_header",1)\\
        .option("field_delimiter", ",")\\
        .option("encoding", "UTF-8") \\
        .option("REPLACE_INVALID_CHARACTERS", True) \\
        .option("field_optionally_enclosed_by", "\\"") \\
        .option("null_if", "") \\
        .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        df = df.with_column("CRT_DTTM", lit(datetime.now().strftime("%Y%m%d%H%M%S"))) \\
               .with_column("FILE_NAME", lit(file_name))

        
        snowdf = df.select(
            "REPORT_CODE",
            "SUPPLIER",
            "BUSINESS_FORMAT",
            "COMPARE",
            "STORE",
            "TRANSACTION_DATE",
            "LY_COMPARE_DATE",
            "REPORT_DATE",
            "DIVISION",
            "DEPARTMENT",
            "SUBDEPARTMENT",
            "CLASS",
            "SUBCLASS",
            "BARCODE",
            "ARTICLE",
            "ARTICLE_NAME",
            "BRAND",
            "MODEL",
            "SALE_AMT_TY_BAHT",
            "SALE_AMT_LY_BAHT",
            "SALE_AMT_VAR",
            "SALE_QTY_TY",
            "SALE_QTY_LY",
            "SALE_QTY_VAR",
            "STOCK_TY_BAHT",
            "STOCK_LY_BAHT",
            "STOCK_VAR",
            "STOCK_QTY_TY",
            "STOCK_QTY_LY",
            "STOCK_QTY_VAR",
            "DAY_ON_HAND_TY",
            "DAY_ON_HAND_LY",
            "DAY_ON_HAND_DIFF",
            "FILE_NAME",
            "CRT_DTTM"
        )
        
        # Check if DataFrame is empty
		snowdf=snowdf.na.drop("all")
        if snowdf.count() == 0:
            return "No Data in table"
        
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")

        # write to success folder    
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
