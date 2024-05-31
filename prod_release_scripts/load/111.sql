CREATE OR REPLACE PROCEDURE THASDL_RAW.TH_MT_PRICE_MULTISHEET_PREPROCESSING("PARAM" ARRAY)
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
            final_df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)
            
        return "Success"
        
    except Exception as e:
        # Handle exceptions here
        error_message = f"ERROR: {str(e)}"
        return error_message
';
CREATE OR REPLACE PROCEDURE THASDL_RAW.TH_MYM_CUST_SALES_PREFORMAT("PARAM" ARRAY)
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