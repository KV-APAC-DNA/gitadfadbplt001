CREATE OR REPLACE PROCEDURE PHLSDL_RAW.PH_NON_POS_LANDMARK_DS_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python==*','unidecode==1.2.0')
HANDLER = 'main'
EXECUTE AS OWNER
AS 'import snowflake.snowpark as snowpark
from snowflake import snowpark
from snowflake.snowpark.window import Window
from snowflake.snowpark.functions import col,lit,concat,regexp_replace,trim,split,rtrim,upper,coalesce,row_number,when,to_date,is_null
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField,DecimalType,DateType
from snowflake.snowpark.files import SnowflakeFile
import pandas as pd
import numpy as np

from datetime import datetime
import pytz
from unidecode import unidecode
from re import sub

def main(session: snowpark.Session,Param):

    try:

        #Param=[''LANDMARK_DS_20230417_20230505150011.csv'',''PHLSDL_RAW.DEV_LOAD_STAGE_ADLS'',''dev/non_pos/transaction/landmark_ds/ph_non_pos_landmark_ds/'',''sdl_ph_non_ise_waltermart'']

        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        file_header_loc = 10

        #Reading data from adls
        full_path = "@"+stage_name+"/"+temp_stage_path+"/"+file_name
        
        with SnowflakeFile.open(full_path,''rb'',require_scoped_url= False) as f:
            
            df = pd.read_csv(f,delimiter='''')
            #print(df)

             # Converting the dataframe into  string  using applymap so that it can be decoded it using unidecode as some values are not ascii
            
            df = df.applymap(lambda x: str(x))

            df = df.applymap(lambda x : unidecode(x))

            #Replacing all nan with null
            df = df.applymap(lambda x: None if x == ''nan'' else x)
           

            #setting mode as index so values below it can be converted into columns after transpose

            df.columns = df.iloc[0]
            df1 = df.set_index(''WEEKLY STOCK AVAILABILITY MONITORING REPORT'')
            df1 = df1.T
            df1 = df1[df1.columns[0:11]]
            df1.columns = [i.strip() for i in df1.columns]
           
                    
            
                      
            
            
           

            #extracting required columns after transposing 
            
            verticle_df = df1[5:]
            

           
                

            #As data for the horizontal columns which contain sku code branch barcode are after 10th row
            df2 = df[file_header_loc:]
            df2 = df2.reset_index()
            df2.columns = df2.iloc[0]
            df2 = df2[1:]

            

            #seperating valvars and idvars so that we can unpivote it
            
            valvars = []
            
            for i in df2.columns[5:]:
                valvars.append(i)

            df2.rename(columns={df2.columns[4]:''MSL DEPT''})
                
            idvars = []
            idvars = [''SKU CODE'',''BRAND'',''BARCODE'',''ITEM DESCRIPTION'',''MSL DEPT'']

            

            
            

            #Converting columns for branch name and osa flag
            unpivoted_df = df2.melt(id_vars=idvars,value_vars=valvars,var_name=''branch_name'',value_name=''osa_flag'')

            #unpivoted_df.rename(columns={''MODE'':''SKU CODE''},inplace=True)

            #Changing sku code to branch name so sa to join two tables9virticle df and 
            
            verticle_df.rename(columns={''SKU CODE'':''branch_name''},inplace=True)

            joined_df = unpivoted_df.merge(verticle_df,on=''branch_name'',how=''inner'')
            

            #dropping row where column value of ''SKU CODE is string or null''
            joined_df.dropna(subset=[''SKU CODE''],inplace=True)
            
            #joined_df = joined_df[joined_df[''SKU CODE''].apply(lambda x:isinstance(x,int))]
                    
    
            #Deleting values where month is null
            joined_df.dropna(subset=[''MONTH:''],inplace=True)

            joined_df[''Actual Delivery Date:'']=joined_df[''Actual Delivery Date:''].apply(lambda x :sub(r''([0-9]{2})([0-9]{2})-([0-9]{2})-([0-9]{2})'',r"\\3/\\4/\\2",x) if isinstance(x,str) else x)

            #for debugging
            print(joined_df[''Actual Delivery Date:''])

            #Converting the dataframe into snowpark-dataframe
            snowparkdf = session.create_dataframe(joined_df)
            

            #return snowparkdf

            #Converting columns similar to table column names
            snowparkdf = snowparkdf.rename({col(''SKU CODE''):''sku_code'',col(''BRAND''):''brand'',col(''BARCODE''):''barcode'',\\
                                        col(''ITEM DESCRIPTION''):''item_description'',col(''MSL DEPT''):''msl_dept'',\\
                                        col(''MONTH:''):''month'',col(''WEEK:''):''week'',col(''ACCOUNT: LANDMARK DS''):''reason'',\\
                                        col(''Actual Delivery Date:''):''acct_deliv_date'',col(''OSA Check Date''):''osa_check_date'',\\
                                        col(''Encoded Report''):''encoded_report'',col(''TEAM LEADER:''):''team_leader'',\\
                                        col(''BRANCH CODE:''):''branch_code'',\\
                                        col(''Branch Size and Classification''):''branch_classification'',\\
                                        col(''"branch_name"''):''branch_name'',\\
                                         col(''"osa_flag"''):''osa_flag''})
            
            

            #Handling null values and empty rows

            snowparkdf = snowparkdf.na.drop(''all'')

            #Checking if dataframe is having any data
            
            if snowparkdf.count()==0:
                return "No Data in file"

           ##FUNCTIONS FOR TRANSFORMATION##

            
            #writing function for ret_nm_prefi
            def get_ret_nm_prefix(branch_code,branch_name):
                if (branch_code is not None or branch_code != ''''):
                    return regexp_replace(split(branch_code,lit(''-''))[0],''[a-z]'',''O'')
                else:
                    return regexp_replace(split(branch_code,lit('' ''))[0],'''','''') 

            #logic to extract branch_code
            def branchcode(branch_code):
                if branch_code  is not None or branch_code != '''':
                    return regexp_replace(lit(split(branch_code,lit("-"))[1]),'''','''')

            #retailer name extraction logic 
            def retailer_name(file_name):
                return rtrim(regexp_replace(split(lit(file_name),lit(''.''))[0],''[0-9]'',''''),lit(''_''))


            def filename_change(filename):
                filename1 = filename.split(''.'')
                filename1[1]=''xlsx''
                newfilename = ''.''.join(filename1)
                return newfilename
                
                    

            #----------------------------------------------------------------------------#
            #Adding columns that are needed for final table 
            
            snowparkdf = snowparkdf.withColumn("ret_nm_prefix", lit(''LMD''))

            snowparkdf = snowparkdf.with_column("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))

            snowparkdf = snowparkdf.withColumn("crtd_dttm",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))

            snowparkdf = snowparkdf.withColumn(''retailer_name'',retailer_name(file_name))
            
            
            snowparkdf = snowparkdf.withColumn(''filename'',lit(filename_change(file_name)))
            
            snowparkdf = snowparkdf.withColumn(''branch_code_original'', lit(snowparkdf[''branch_code'']))
            
            snowparkdf = snowparkdf.withColumn(''branch_code'',lit(branchcode(snowparkdf["branch_code"])))
            
            snowparkdf = snowparkdf.withColumn(''rn'', row_number().over(Window.partition_by(snowparkdf[''sku_code''],snowparkdf[''osa_check_date''],coalesce(snowparkdf[''branch_code''],snowparkdf[''branch_code''])).order_by(snowparkdf[''week''],snowparkdf[''item_description''])))

            snowparkdf = snowparkdf.withColumn("acct_deliv_date",when(col(''acct_deliv_date'')==None,lit(None)).otherwise( regexp_replace(col("acct_deliv_date"), r"(\\d{2})(\\d{2})-(\\d{2})-(\\d{2}) (\\d{2}:\\d{2}:\\d{2})", r"\\3/\\4/\\2")))

            snowparkdf = snowparkdf.withColumn("OSA_CHECK_DATE",to_date(col("OSA_CHECK_DATE")))

            snowparkdf = snowparkdf.filter(col(''rn'') == 1)

            # extracting important columns
            snowdf = snowparkdf.select(
                    ''ret_nm_prefix'',
                     ''sku_code'',
                     ''brand'',
                     ''barcode'',
                     ''item_description'',
                     ''msl_dept'',
                     ''MONTH'',
                     ''week'',
                     ''reason'',
                     ''acct_deliv_date'',
                     ''osa_check_date'',
                     ''encoded_report'',
                     ''team_leader'',
                     ''branch_code'',
                     ''branch_code_original'',
                     ''branch_classification'',
                     ''branch_name'',
                     ''osa_flag'',
                     ''retailer_name'',
                     ''filename'',
                     ''run_id'',
                     ''crtd_dttm''
                    
        )

            # Load Data to the target table
            #snowdf.write.mode("append").saveAsTable(target_table)

            current_date = datetime.now()
            formatted_year = current_date.strftime("%Y")
            formatted_month = current_date.strftime("%m")

             # write to success folder
            file_name = file_name.split(''.'')[0]+"_"+datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")

            

            #snowdf.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True,file_format_name=''ph_non_pos_svi_smc_FORMAT'')

           
            return ''Success''
            
    except KeyError as key_error:
        # Handle KeyError (missing columns) here
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message
        
    except Exception as e:
        # Handle exceptions here
        error_message = f"Error: {str(e)}"
        return error_message;

           
            
   '----------------------------------------------------------------------------------------------------------

CREATE OR REPLACE PROCEDURE PHLSDL_RAW.PH_NON_POS_SVI_SMC_PRECROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('openpyxl==3.0.10','snowflake-snowpark-python==*','unidecode==1.2.0')
HANDLER = 'main'
EXECUTE AS OWNER
AS 'import snowflake.snowpark as snowpark
from snowflake import snowpark
from snowflake.snowpark.window import Window
from snowflake.snowpark.functions import col,lit,concat,regexp_replace,trim,split,rtrim,upper,coalesce,row_number,when,to_date,is_null
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField,DecimalType,DateType
from snowflake.snowpark.files import SnowflakeFile
import pandas as pd
import numpy as np
import openpyxl
from datetime import datetime
import pytz
from unidecode import unidecode
from re import sub

def main(session: snowpark.Session,Param):
    

    try:

        #Param=[''SVI_SMC_20230521_20230601150011.csv'',''PHLSDL_RAW.DEV_LOAD_STAGE_ADLS'',''dev/non_pos/transaction/svi_smc/ph_non_pos_svi_smc'',''sdl_ph_non_ise_svi_smc'']

        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        file_header_loc = 10

        #Reading data from adls
        full_path = "@"+stage_name+"/"+temp_stage_path+"/"+file_name
        
        with SnowflakeFile.open(full_path,''rb'',require_scoped_url= False) as f:
            df = pd.read_csv(f,delimiter='''')

            # Converting the dataframe into  string  using applymap so that it can be decoded it using unidecode as some values are not ascii
            
            df = df.applymap(lambda x: str(x))

            df = df.applymap(lambda x : unidecode(x))

            #Replacing all nan with null
            df = df.applymap(lambda x: None if x == ''nan'' else x)

            
            #setting mode as index so values below it can be converted into columns after transpose

            df.columns = df.iloc[0]
            df1 = df.set_index(''WEEKLY STOCK AVAILABILITY MONITORING REPORT'')

            #extracting required columns after transposing 
            
            verticle_df = df1.T[7:][[''MONTH:'',''WEEK:'',''ACCOUNT: SVI-SMC'',''Actual Delivery Date:'',''OSA Check Date'',''Encoded Report'',''TEAM LEADER:'',''BRANCH CODE:'',''Branch Size and Classification'',''SKU CODE'']]

            #As data for the horizontal columns which contain sku code branch barcode are after 10th row
            df2 = df1[file_header_loc:]
            #print(df2)
            #replacing columns names with original and not taking 1st row as it contains col names
            df2.reset_index(inplace=True)
            df2.columns = df2.iloc[0]
            df2 = df2[1:]
            #print(df2)

            #seperating valvars and idvars so that we can unpivote it

            #seperating valvars and idvars so that we can unpivote it
            
            valvars = []
            
            for i in df2.columns[9:]:
                valvars.append(i)
                
            
            idvars = [''SKU CODE'',''BRAND'',''BARCODE'',''ITEM DESCRIPTION'',''SPMKT LARGE'',''SPMKT SMALL'',''SPMKT PREM'']

            #Converting columns for branch name and osa flag
            unpivoted_df = df2.melt(id_vars=idvars,value_vars=valvars,var_name=''branch_name'',value_name=''osa_flag'')

            #unpivoted_df.rename(columns={''MODE'':''SKU CODE''},inplace=True)

            #Changing sku code to branch name so sa to join two tables9virticle df and 
            
            verticle_df.rename(columns={''SKU CODE'':''branch_name''},inplace=True)

            joined_df = unpivoted_df.merge(verticle_df,on=''branch_name'',how=''inner'')
            #print(joined_df)
            #dropping row where column value of ''SKU CODE is string or null''
            joined_df.dropna(subset=[''SKU CODE''],inplace=True)
            
            #joined_df = joined_df[joined_df[''SKU CODE''].apply(lambda x:isinstance(x,int))]
            
            #getting actual delivery date in proper date format
            joined_df[''Actual Delivery Date:'']=joined_df[''Actual Delivery Date:''].apply(lambda x :sub(r''([0-9]{2})([0-9]{2})-([0-9]{2})-([0-9]{2})'',r"\\3/\\4/\\2",x) if isinstance(x,str) else x)

            #Deleting values where month is null
            joined_df.dropna(subset=[''MONTH:''],inplace=True)
            
            ##CREATING SNOWPARK DATAFRAME##
            snowparkdf = session.create_dataframe(joined_df)

            #Converting columns similar to table column names
            snowparkdf = snowparkdf.rename({col(''SKU CODE''):''sku_code'',col(''BRAND''):''brand'',col(''BARCODE''):''barcode'',\\
                                        col(''ITEM DESCRIPTION''):''item_description'',col(''SPMKT LARGE''):''msl_large'',\\
                                        col(''SPMKT SMALL''):''msl_small'',col(''SPMKT PREM''):''msl_premium'',\\
                                        col(''MONTH:''):''month'',col(''WEEK:''):''week'',col(''ACCOUNT: SVI-SMC''):''reason'',\\
                                        col(''Actual Delivery Date:''):''acct_deliv_date'',col(''OSA Check Date''):''osa_check_date'',\\
                                        col(''Encoded Report''):''encoded_report'',col(''TEAM LEADER:''):''team_leader''\\
                                        ,col(''BRANCH CODE:''):''branch_code'',\\
                                        col(''Branch Size and Classification''):''branch_classification'',\\
                                        col(''"branch_name"''):''branch_name'',\\
                                         col(''"osa_flag"''):''osa_flag''})
            
            

            #Handling null values and empty rows

            snowparkdf = snowparkdf.na.drop(''all'')

            #Checking if dataframe is having any data
            
            if snowparkdf.count()==0:
                return "No Data in file"

           ##FUNCTIONS FOR TRANSFORMATION##

            
            #writing function for ret_nm_prefi
            def get_ret_nm_prefix(branch_code,branch_name):
                if (branch_code is not None or branch_code != ''''):
                    return regexp_replace(split(branch_code,lit(''-''))[0],''[a-z]'',''O'')
                else:
                    return regexp_replace(split(branch_code,lit('' ''))[0],'''','''') 

            #logic to extract branch_code
            def branchcode(branch_code):
                if branch_code  is not None or branch_code != '''':
                    return regexp_replace(lit(split(branch_code,lit("-"))[1]),'''','''')

            #retailer name extraction logic 
            def retailer_name(file_name):
                return rtrim(regexp_replace(split(lit(file_name),lit(''.''))[0],''[0-9]'',''''),lit(''_''))

            
            def filename_change(filename):
                filename1 = filename.split(''.'')
                filename1[1]=''xlsx''
                newfilename = ''.''.join(filename1)
                return newfilename
                    

            #----------------------------------------------------------------------------#
            #Adding columns that are needed for final table 
            
            snowparkdf = snowparkdf.withColumn("ret_nm_prefix", lit(get_ret_nm_prefix(snowparkdf["branch_code"],snowparkdf[''branch_name''])))

            snowparkdf = snowparkdf.with_column("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))

            snowparkdf = snowparkdf.withColumn("crtd_dttm",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))

            snowparkdf = snowparkdf.withColumn(''retailer_name'',retailer_name(file_name))
            
            snowparkdf = snowparkdf.withColumn(''filename'',lit(filename_change(file_name)))
            
            snowparkdf = snowparkdf.withColumn(''branch_code_original'', lit(snowparkdf[''branch_code'']))
            
            snowparkdf = snowparkdf.withColumn(''branch_code'',lit(branchcode(snowparkdf["branch_code"])))
            
            snowparkdf = snowparkdf.withColumn(''rn'', row_number().over(Window.partition_by(snowparkdf[''sku_code''],snowparkdf[''osa_check_date''],coalesce(snowparkdf[''branch_code''],snowparkdf[''branch_code''])).order_by(snowparkdf[''week''],snowparkdf[''item_description''])))

            snowparkdf = snowparkdf.withColumn("acct_deliv_date",when(col(''acct_deliv_date'')==None,lit(None)).otherwise( regexp_replace(col("acct_deliv_date"), r"(\\d{2})(\\d{2})-(\\d{2})-(\\d{2}) (\\d{2}:\\d{2}:\\d{2})", r"\\3/\\4/\\2")))

            snowparkdf = snowparkdf.withColumn("OSA_CHECK_DATE",to_date(col("OSA_CHECK_DATE")))

            snowparkdf = snowparkdf.filter(col(''rn'') == 1)

            # extracting important columns
            snowdf = snowparkdf.select(
                    ''ret_nm_prefix'',
                     ''sku_code'',
                     ''brand'',
                     ''barcode'',
                     ''item_description'',
                     ''msl_large'',
                     ''msl_small'',
                     ''msl_premium'',
                     ''MONTH'',
                     ''week'',
                     ''reason'',
                     ''acct_deliv_date'',
                     ''osa_check_date'',
                     ''encoded_report'',
                     ''team_leader'',
                     ''branch_code'',
                     ''branch_code_original'',
                     ''branch_classification'',
                     ''branch_name'',
                     ''osa_flag'',
                     ''retailer_name'',
                     ''filename'',
                     ''run_id'',
                     ''crtd_dttm''
                    
        )

            # Load Data to the target table
            snowdf.write.mode("append").saveAsTable(target_table)

            current_date = datetime.now()
            formatted_year = current_date.strftime("%Y")
            formatted_month = current_date.strftime("%m")

             # write to success folder
            file_name = file_name.split(''.'')[0]+"_"+datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")

            

            snowdf.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True,file_format_name=''ph_non_pos_svi_smc_FORMAT'')

            

            

            return ''Success''

    except KeyError as key_error:
        # Handle KeyError (missing columns) here
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message
        
    except Exception as e:
        # Handle exceptions here
        error_message = f"Error: {str(e)}"
        return error_message;'
--------------------------------------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE PHLSDL_RAW.PH_NON_POS_WALTERMART_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('openpyxl==3.0.10','snowflake-snowpark-python==*','unidecode==1.2.0')
HANDLER = 'main'
EXECUTE AS OWNER
AS 'import snowflake.snowpark as snowpark
from snowflake import snowpark
from snowflake.snowpark.window import Window
from snowflake.snowpark.functions import col,lit,concat,regexp_replace,trim,split,rtrim,upper,coalesce,row_number,when,to_date,is_null
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField,DecimalType,DateType
from snowflake.snowpark.files import SnowflakeFile
import pandas as pd
import numpy as np
import openpyxl
from datetime import datetime
import pytz
from unidecode import unidecode
from re import sub

def main(session: snowpark.Session,Param):

    try:

        #Param=[''WALTERMART_20220519_20220520150013.csv'',''PHLSDL_RAW.DEV_LOAD_STAGE_ADLS'',''dev/non_pos/transaction/waltermart/ph_non_pos_waltermart/'',''sdl_ph_non_ise_waltermart'']

        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        file_header_loc = 10

        #Reading data from adls
        full_path = "@"+stage_name+"/"+temp_stage_path+"/"+file_name
        
        with SnowflakeFile.open(full_path,''rb'',require_scoped_url= False) as f:
            
            df = pd.read_csv(f,delimiter='''')
            #print(df)

             # Converting the dataframe into  string  using applymap so that it can be decoded it using unidecode as some values are not ascii
            
            df = df.applymap(lambda x: str(x))

            df = df.applymap(lambda x : unidecode(x))

            #Replacing all nan with null
            df = df.applymap(lambda x: None if x == ''nan'' else x)

           
            df.columns = df.iloc[0]
            df1 = df.set_index(''WEEKLY STOCK AVAILABILITY MONITORING REPORT'')

            #extracting required columns after transposing 
            
            verticle_df = df1.T[7:][[''MONTH:'',''WEEK:'',''ACCOUNT: WALTERMART'',''Actual Delivery Date:'',''OSA Check Date'',''Encoded Report'',''TEAM LEADER:'',''BRANCH CODE:'',''Branch Size and Classification'',''SKU CODE'']]

             #As data for the horizontal columns which contain sku code branch barcode are after 10th row
            df2 = df1[file_header_loc:]
            df2.reset_index(inplace=True)
            df2.columns = df2.iloc[0]
            df2 = df2[1:]
            #print(df2.columns)

             #seperating valvars and idvars so that we can unpivote it

            #seperating valvars and idvars so that we can unpivote it
            
            valvars = []
            
            for i in df2.columns[9:]:
                valvars.append(i)
                
            
            idvars = [''SKU CODE'',''BRAND'',''BARCODE'',''ITEM DESCRIPTION'',''MSL LARGE'',''MSL SMALL'',''MSL PREMIUM'']

            #Converting columns for branch name and osa flag
            unpivoted_df = df2.melt(id_vars=idvars,value_vars=valvars,var_name=''branch_name'',value_name=''osa_flag'')

            #Changing sku code to branch name so sa to join two tables9virticle df and 
            
            verticle_df.rename(columns={''SKU CODE'':''branch_name''},inplace=True)

            print(verticle_df)

            joined_df = unpivoted_df.merge(verticle_df,on=''branch_name'',how=''inner'')
            print(joined_df[[''osa_flag'']])
            #dropping row where column value of ''''SKU CODE is string or null''''
            joined_df.dropna(subset=[''SKU CODE''],inplace=True)

            #for debugging
            #print(joined_df)

            #getting actual delivery date in proper date format
            joined_df[''Actual Delivery Date:'']=joined_df[''Actual Delivery Date:''].apply(lambda x :sub(r''([0-9]{2})([0-9]{2})-([0-9]{2})-([0-9]{2})'',r''\\3/\\4/\\1'',x) if isinstance(x,str) else x)
            joined_df[''Actual Delivery Date:'']=joined_df[''Actual Delivery Date:''].apply(lambda x :x[:8] if isinstance(x,str) else x)

            #Deleting values where month is null
            joined_df.dropna(subset=[''MONTH:''],inplace=True)

            snowparkdf = session.create_dataframe(joined_df)

            #Converting columns similar to table column names
            snowparkdf = snowparkdf.rename({col(''SKU CODE''):''sku_code'',col(''BRAND''):''brand'',col(''BARCODE''):''barcode'',\\
                                        col(''ITEM DESCRIPTION''):''item_description'',col(''MSL LARGE''):''msl_large'',\\
                                        col(''MSL SMALL''):''msl_small'',col(''MSL PREMIUM''):''msl_premium'',\\
                                        col(''MONTH:''):''month'',col(''WEEK:''):''week'',col(''ACCOUNT: WALTERMART''):''reason'',\\
                                        col(''Actual Delivery Date:''):''acct_deliv_date'',col(''OSA Check Date''):''osa_check_date'',\\
                                        col(''Encoded Report''):''encoded_report'',col(''TEAM LEADER:''):''team_leader''\\
                                        ,col(''BRANCH CODE:''):''branch_code'',\\
                                        col(''Branch Size and Classification''):''branch_classification'',\\
                                        col(''"branch_name"''):''branch_name'',\\
                                         col(''"osa_flag"''):''osa_flag''})

            #Handling null values and empty rows

            snowparkdf = snowparkdf.na.drop(''all'')

            #Checking if dataframe is having any data
            
            if snowparkdf.count()==0:
                return "No Data in file"


            ##FUNCTIONS FOR TRANSFORMATION##

            
            #writing function for ret_nm_prefi
            def get_ret_nm_prefix(branch_code,branch_name):
                if (branch_code is not None or branch_code != ''''):
                    return regexp_replace(split(branch_code,lit(''-''))[0],''[a-z]'',''O'')
                else:
                    return regexp_replace(split(branch_code,lit('' ''))[0],'''','''') 

            #logic to extract branch_code
            def branchcode(branch_code):
                if branch_code  is not None or branch_code != '''':
                    return regexp_replace(lit(split(branch_code,lit("-"))[1]),'''','''')

            #retailer name extraction logic 
            def retailer_name(file_name):
                return rtrim(regexp_replace(split(lit(file_name),lit(''.''))[0],''[0-9]'',''''),lit(''_''))


            def filename_change(filename):
                filename1 = filename.split(''.'')
                filename1[1]=''xlsx''
                newfilename = ''.''.join(filename1)
                return newfilename

            #----------------------------------------------------------------------------#
            #Adding columns that are needed for final table 
            
            snowparkdf = snowparkdf.withColumn("ret_nm_prefix", lit(''WM''))

            snowparkdf = snowparkdf.with_column("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))

            snowparkdf = snowparkdf.withColumn("crtd_dttm",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))

            snowparkdf = snowparkdf.withColumn(''retailer_name'',retailer_name(file_name))
            
            
            snowparkdf = snowparkdf.withColumn(''filename'',lit(filename_change(file_name)))
            
            snowparkdf = snowparkdf.withColumn(''branch_code_original'', lit(snowparkdf[''branch_code'']))
            
            snowparkdf = snowparkdf.withColumn(''branch_code'',lit(branchcode(snowparkdf["branch_code"])))
            
            snowparkdf = snowparkdf.withColumn(''rn'', row_number().over(Window.partition_by(snowparkdf[''sku_code''],snowparkdf[''osa_check_date''],coalesce(snowparkdf[''branch_code''],snowparkdf[''branch_code''])).order_by(snowparkdf[''week''],snowparkdf[''item_description''])))

            snowparkdf = snowparkdf.withColumn("acct_deliv_date",when(col(''acct_deliv_date'')==None,lit(None)).otherwise( regexp_replace(col("acct_deliv_date"), r"(\\d{2})(\\d{2})-(\\d{2})-(\\d{2}) (\\d{2}:\\d{2}:\\d{2})", r"\\3/\\4/\\2")))

            snowparkdf = snowparkdf.withColumn("OSA_CHECK_DATE",to_date(col("OSA_CHECK_DATE")))

            snowparkdf = snowparkdf.filter(col(''rn'') == 1)

            # extracting important columns
            snowdf = snowparkdf.select(
                    ''ret_nm_prefix'',
                     ''sku_code'',
                     ''brand'',
                     ''barcode'',
                     ''item_description'',
                     ''msl_large'',
                     ''msl_small'',
                     ''msl_premium'',
                     ''MONTH'',
                     ''week'',
                     ''reason'',
                     ''acct_deliv_date'',
                     ''osa_check_date'',
                     ''encoded_report'',
                     ''team_leader'',
                     ''branch_code'',
                     ''branch_code_original'',
                     ''branch_classification'',
                     ''branch_name'',
                     ''osa_flag'',
                     ''retailer_name'',
                     ''filename'',
                     ''run_id'',
                     ''crtd_dttm''
                    
        )

            # Load Data to the target table
            snowdf.write.mode("append").saveAsTable(target_table)

            current_date = datetime.now()
            formatted_year = current_date.strftime("%Y")
            formatted_month = current_date.strftime("%m")

             # write to success folder
            file_name = file_name.split(''.'')[0]+"_"+datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")

            

            snowdf.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True,file_format_name=''ph_non_pos_svi_smc_FORMAT'')
            
            
            return ''Success''

            
           
    except KeyError as key_error:
        # Handle KeyError (missing columns) here
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message
        
    except Exception as e:
        # Handle exceptions here
        error_message = f"Error: {str(e)}"
        return error_message;

           
            
   '-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE PROCEDURE PHLSDL_RAW.PH_NON_POS_LANDMARK_SM_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('openpyxl==3.0.10','snowflake-snowpark-python==*','unidecode==1.2.0')
HANDLER = 'main'
EXECUTE AS OWNER
AS 'import snowflake.snowpark as snowpark
from snowflake import snowpark
from snowflake.snowpark.window import Window
from snowflake.snowpark.functions import col,lit,concat,regexp_replace,trim,split,rtrim,upper,coalesce,row_number,when,to_date,is_null
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField,DecimalType,DateType
from snowflake.snowpark.files import SnowflakeFile
import pandas as pd
import numpy as np
import openpyxl
from datetime import datetime
import pytz
from unidecode import unidecode
from re import sub

def main(session: snowpark.Session,Param):

    try:

        #Param=[''LANDMARK_SM_20231147.csv'',''PHLSDL_RAW.DEV_LOAD_STAGE_ADLS'',''dev/non_pos/transaction/landmark_sm/ph_non_pos_landmark_sm/'',''sdl_ph_non_ise_svi_smc'']

        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        file_header_loc = 10

        #Reading data from adls
        full_path = "@"+stage_name+"/"+temp_stage_path+"/"+file_name
        
        with SnowflakeFile.open(full_path,''rb'',require_scoped_url= False) as f:
            
            df = pd.read_csv(f,delimiter='''')

            # Converting the dataframe into  string  using applymap so that it can be decoded it using unidecode as some values are not ascii
            
            df = df.applymap(lambda x: str(x))

            df = df.applymap(lambda x : unidecode(x))

            #Replacing all nan with null
            df = df.applymap(lambda x: None if x == ''nan'' else x)
            print(df)

            #setting mode as index so values below it can be converted into columns after transpose

            df.columns = df.iloc[0]
            df1 = df.set_index(''WEEKLY STOCK AVAILABILITY MONITORING REPORT'')
            df2 = df1[:11].T
            df2.columns = [i.strip() for i in df2.columns]

            #extracting required columns after transposing 
            
            verticle_df = df2[[''MONTH:'',''WEEK:'',''ACCOUNT: LANDMARK SM'',''Actual Delivery Date:'',''OSA Check Date'',''Encoded Report'',''TEAM LEADER:'',''BRANCH CODE:'',''Branch Size and Classification'',''SKU CODE'']]

             #As data for the horizontal columns which contain sku code branch barcode are after 10th row
            df2 = df1[file_header_loc:]
            df2.reset_index(inplace=True)
            df2.columns = df2.iloc[0]
            df2 = df2[1:]
            print(df2)

             #seperating valvars and idvars so that we can unpivote it

            #seperating valvars and idvars so that we can unpivote it
            
            valvars = []
            
            for i in df2.columns[7:]:
                valvars.append(i)
                
            df2.rename(columns= {df2.columns[4]:''MSL LARGE'',df2.columns[5]:''MSL SMALL'',df2.columns[6]:''MSL PREMIUM''}, inplace=True)
            idvars = df2.columns[0:7] 
            #Converting columns for branch name and osa flag
            unpivoted_df = df2.melt(id_vars=idvars,value_vars=valvars,var_name=''branch_name'',value_name=''osa_flag'')
            #print(unpivoted_df)

            #Changing sku code to branch name so sa to join two tables9virticle df and 
            
            verticle_df.rename(columns={''SKU CODE'':''branch_name''},inplace=True)

            #print(verticle_df)

            joined_df = unpivoted_df.merge(verticle_df,on=''branch_name'',how=''inner'')

            #print(joined_df)

            #getting actual delivery date in proper date format
            joined_df[''Actual Delivery Date:'']=joined_df[''Actual Delivery Date:''].apply(lambda x :sub(r''([0-9]{2})([0-9]{2})-([0-9]{2})-([0-9]{2})'',r''\\3/\\4/\\1'',x) if isinstance(x,str) else x)
            joined_df[''Actual Delivery Date:'']=joined_df[''Actual Delivery Date:''].apply(lambda x :x[:8] if isinstance(x,str) else x)

             #Deleting values where month is null
            joined_df.dropna(subset=[''MONTH:''],inplace=True)

            

            snowparkdf = session.create_dataframe(joined_df)

            #Converting columns similar to table column names
            snowparkdf = snowparkdf.rename({col(''SKU CODE''):''sku_code'',col(''BRAND''):''brand'',col(''BARCODE''):''barcode'',\\
                                        col(''ITEM DESCRIPTION''):''item_description'',col(''MSL LARGE''):''msl_large'',\\
                                        col(''MSL SMALL''):''msl_small'',col(''MSL PREMIUM''):''msl_premium'',\\
                                        col(''MONTH:''):''month'',col(''WEEK:''):''week'',col(''ACCOUNT: LANDMARK SM''):''reason'',\\
                                        col(''Actual Delivery Date:''):''acct_deliv_date'',col(''OSA Check Date''):''osa_check_date'',\\
                                        col(''Encoded Report''):''encoded_report'',col(''TEAM LEADER:''):''team_leader''\\
                                        ,col(''BRANCH CODE:''):''branch_code'',\\
                                        col(''Branch Size and Classification''):''branch_classification'',\\
                                        col(''"branch_name"''):''branch_name'',\\
                                         col(''"osa_flag"''):''osa_flag''})

            #Handling null values and empty rows

            snowparkdf = snowparkdf.na.drop(''all'')

            #Checking if dataframe is having any data
            
            if snowparkdf.count()==0:
                return "No Data in file"


            ##FUNCTIONS FOR TRANSFORMATION##

            
            #writing function for ret_nm_prefi
            def get_ret_nm_prefix(branch_code,branch_name):
                if (branch_code is not None or branch_code != ''''):
                    return regexp_replace(split(branch_code,lit(''-''))[0],''[a-z]'',''O'')
                else:
                    return regexp_replace(split(branch_code,lit('' ''))[0],'''','''') 

            #logic to extract branch_code
            def branchcode(branch_code):
                if branch_code  is not None or branch_code != '''':
                    return regexp_replace(lit(split(branch_code,lit("-"))[1]),'''','''')

            #retailer name extraction logic 
            def retailer_name(file_name):
                return rtrim(regexp_replace(split(lit(file_name),lit(''.''))[0],''[0-9]'',''''),lit(''_''))


            def filename_change(filename):
                filename1 = filename.split(''.'')
                filename1[1]=''xlsx''
                newfilename = ''.''.join(filename1)
                return newfilename

            #----------------------------------------------------------------------------#

            snowparkdf = snowparkdf.withColumn("ret_nm_prefix", lit(''LMS''))

            snowparkdf = snowparkdf.with_column("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))

            snowparkdf = snowparkdf.withColumn("crtd_dttm",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))

            snowparkdf = snowparkdf.withColumn(''retailer_name'',retailer_name(file_name))
            
            snowparkdf = snowparkdf.withColumn(''filename'',lit(filename_change(file_name)))
            
            snowparkdf = snowparkdf.withColumn(''branch_code_original'', lit(snowparkdf[''branch_code'']))

            snowparkdf = snowparkdf.withColumn(''branch_code'',lit(branchcode(snowparkdf["branch_code"])))

            snowparkdf = snowparkdf.withColumn(''rn'', row_number().over(Window.partition_by(snowparkdf[''sku_code''],snowparkdf[''osa_check_date''],coalesce(snowparkdf[''branch_code''],snowparkdf[''branch_code''])).order_by(snowparkdf[''week''],snowparkdf[''item_description''])))

            snowparkdf = snowparkdf.withColumn("acct_deliv_date",when(col(''acct_deliv_date'')==None,lit(None)).otherwise( regexp_replace(col("acct_deliv_date"), r"(\\d{2})(\\d{2})-(\\d{2})-(\\d{2}) (\\d{2}:\\d{2}:\\d{2})", r"\\3/\\4/\\2")))

            snowparkdf = snowparkdf.withColumn("OSA_CHECK_DATE",to_date(col("OSA_CHECK_DATE")))

            snowparkdf = snowparkdf.filter(col(''rn'') == 1)

             # extracting important columns
            snowdf = snowparkdf.select(
                    ''ret_nm_prefix'',
                     ''sku_code'',
                     ''brand'',
                     ''barcode'',
                     ''item_description'',
                     ''msl_large'',
                     ''msl_small'',
                     ''msl_premium'',
                     ''MONTH'',
                     ''week'',
                     ''reason'',
                     ''acct_deliv_date'',
                     ''osa_check_date'',
                     ''encoded_report'',
                     ''team_leader'',
                     ''branch_code'',
                     ''branch_code_original'',
                     ''branch_classification'',
                     ''branch_name'',
                     ''osa_flag'',
                     ''retailer_name'',
                     ''filename'',
                     ''run_id'',
                     ''crtd_dttm''
                    
        )

            # Load Data to the target table
            snowdf.write.mode("append").saveAsTable(target_table)

            current_date = datetime.now()
            formatted_year = current_date.strftime("%Y")
            formatted_month = current_date.strftime("%m")

             # write to success folder
            file_name = file_name.split(''.'')[0]+"_"+datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")

            

            snowdf.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True,file_format_name=''ph_non_pos_svi_smc_FORMAT'')
            
            
            return ''Success''
            
            
           
            

            

            
           

            

            
            
           

            
            
    except KeyError as key_error:
        # Handle KeyError (missing columns) here
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message
        
    except Exception as e:
        # Handle exceptions here
        error_message = f"Error: {str(e)}"
        return error_message;

           
            
   --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE PROCEDURE PHLSDL_RAW.PH_NON_POS_RUSTANS_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('openpyxl==3.0.10','snowflake-snowpark-python==*','unidecode==1.2.0')
HANDLER = 'main'
EXECUTE AS OWNER
AS 'import snowflake.snowpark as snowpark
from snowflake import snowpark
from snowflake.snowpark.window import Window
from snowflake.snowpark.functions import col,lit,concat,regexp_replace,trim,split,rtrim,upper,coalesce,row_number,when,to_date,is_null
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField,DecimalType,DateType
from snowflake.snowpark.files import SnowflakeFile
import pandas as pd
import numpy as np
import openpyxl
from datetime import datetime
import pytz
from unidecode import unidecode
from re import sub

def main(session: snowpark.Session,Param):
    

    try:

        #Param=[''RUSTANS_20210131_20210330225126.csv'',''PHLSDL_RAW.DEV_LOAD_STAGE_ADLS'',''dev/non_pos/transaction/rustans/ph_non_pos_rustans'',''sdl_ph_non_ise_svi_smc'']

        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        file_header_loc = 10

        #Reading data from adls
        full_path = "@"+stage_name+"/"+temp_stage_path+"/"+file_name
        
        with SnowflakeFile.open(full_path,''rb'',require_scoped_url= False) as f:
            df = pd.read_csv(f,delimiter='''')

            # Converting the dataframe into  string  using applymap so that it can be decoded it using unidecode as some values are not ascii
            
            df = df.applymap(lambda x: str(x))

            df = df.applymap(lambda x : unidecode(x))

            #Replacing all nan with null
            df = df.applymap(lambda x: None if x == ''nan'' else x)

            df.columns = df.iloc[0]
            df1 = df.set_index(''WEEKLY STOCK AVAILABILITY MONITORING REPORT'')
            
           

            #extracting required columns after transposing 
            
            verticle_df = df1.T[7:][[''MONTH:'',''WEEK:'',''ACCOUNT: RUSTANS'',''Actual Delivery Date:'',''OSA Check Date'',''Encoded Report'',''TEAM LEADER:'',''BRANCH CODE:'',''Branch Size and Classification'',''SKU CODE'']]

            #As data for the horizontal columns which contain sku code branch barcode are after 10th row
            df2 = df1[file_header_loc:]
            df2.reset_index(inplace=True)
            df2.columns = df2.iloc[0]
            df2 = df2[1:]
            print(df2)

             #seperating valvars and idvars so that we can unpivote it

            #seperating valvars and idvars so that we can unpivote it
            
            valvars = []
            
            for i in df2.columns[9:]:
                valvars.append(i)
                
            
            idvars = [''SKU CODE'',''BRAND'',''BARCODE'',''ITEM DESCRIPTION'',''MSL LARGE'',''MSL SMALL'',''MSL PREMIUM'']

            #Converting columns for branch name and osa flag
            unpivoted_df = df2.melt(id_vars=idvars,value_vars=valvars,var_name=''branch_name'',value_name=''osa_flag'')

            #Changing sku code to branch name so sa to join two tables9virticle df and 
            
            verticle_df.rename(columns={''SKU CODE'':''branch_name''},inplace=True)

            joined_df = unpivoted_df.merge(verticle_df,on=''branch_name'',how=''inner'')
            print(joined_df[[''osa_flag'']])
            #dropping row where column value of ''''SKU CODE is string or null''''
            joined_df.dropna(subset=[''SKU CODE''],inplace=True)

            #for debugging
            #print(joined_df)

            #getting actual delivery date in proper date format
            joined_df[''Actual Delivery Date:'']=joined_df[''Actual Delivery Date:''].apply(lambda x :sub(r''([0-9]{2})([0-9]{2})-([0-9]{2})-([0-9]{2})'',r''\\3/\\4/\\1'',x) if isinstance(x,str) else x)
            joined_df[''Actual Delivery Date:'']=joined_df[''Actual Delivery Date:''].apply(lambda x :x[:8] if isinstance(x,str) else x)

            #Deleting values where month is null
            joined_df.dropna(subset=[''MONTH:''],inplace=True)
            joined_df = joined_df[joined_df[''SKU CODE''].apply(lambda x: str(x).isdigit())]

            def osa_flag_correction_alpha(x):
                if isinstance(x,str):
                    if len(x) >= 2:
                        return x.strip().replace('','','''').replace('' '','''').replace(''-'','''')
                    else:
                        return x
                

            #joined_df[''osa_flag'']= joined_df[''osa_flag''].apply(lambda x: osa_flag_correction_float(x))
            joined_df[''osa_flag''] = joined_df[''osa_flag''].apply(lambda x: osa_flag_correction_alpha(x) )

            snowparkdf = session.create_dataframe(joined_df)

            #Converting columns similar to table column names
            snowparkdf = snowparkdf.rename({col(''SKU CODE''):''sku_code'',col(''BRAND''):''brand'',col(''BARCODE''):''barcode'',\\
                                        col(''ITEM DESCRIPTION''):''item_description'',col(''MSL LARGE''):''msl_large'',\\
                                        col(''MSL SMALL''):''msl_small'',col(''MSL PREMIUM''):''msl_premium'',\\
                                        col(''MONTH:''):''month'',col(''WEEK:''):''week'',col(''ACCOUNT: RUSTANS''):''reason'',\\
                                        col(''Actual Delivery Date:''):''acct_deliv_date'',col(''OSA Check Date''):''osa_check_date'',\\
                                        col(''Encoded Report''):''encoded_report'',col(''TEAM LEADER:''):''team_leader''\\
                                        ,col(''BRANCH CODE:''):''branch_code'',\\
                                        col(''Branch Size and Classification''):''branch_classification'',\\
                                        col(''"branch_name"''):''branch_name'',\\
                                         col(''"osa_flag"''):''osa_flag''})


            #Handling null values and empty rows

            snowparkdf = snowparkdf.na.drop(''all'')

            #Checking if dataframe is having any data
            
            if snowparkdf.count()==0:
                return "No Data in file"


            ##FUNCTIONS FOR TRANSFORMATION##

            
            #writing function for ret_nm_prefi
            def get_ret_nm_prefix(branch_code,branch_name):
                if (branch_code is not None or branch_code != ''''):
                    return regexp_replace(split(branch_code,lit(''-''))[0],''[a-z]'',''O'')
                else:
                    return regexp_replace(split(branch_code,lit('' ''))[0],'''','''') 

            #logic to extract branch_code
            def branchcode(branch_code):
                if branch_code  is not None or branch_code != '''':
                    return regexp_replace(lit(split(branch_code,lit("-"))[1]),'''','''')

            #retailer name extraction logic 
            def retailer_name(file_name):
                return rtrim(regexp_replace(split(lit(file_name),lit(''.''))[0],''[0-9]'',''''),lit(''_''))


            def filename_change(filename):
                filename1 = filename.split(''.'')
                filename1[1]=''xlsx''
                newfilename = ''.''.join(filename1)
                return newfilename

            #----------------------------------------------------------------------------#
            #Adding columns that are needed for final table 
            
            snowparkdf = snowparkdf.withColumn("ret_nm_prefix", lit(''RS''))

            snowparkdf = snowparkdf.with_column("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))

            snowparkdf = snowparkdf.withColumn("crtd_dttm",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))

            snowparkdf = snowparkdf.withColumn(''retailer_name'',retailer_name(file_name))
            
            snowparkdf = snowparkdf.withColumn(''filename'',lit(filename_change(file_name)))

            
            snowparkdf = snowparkdf.withColumn(''branch_code_original'', lit(snowparkdf[''branch_code'']))
            
            snowparkdf = snowparkdf.withColumn(''branch_code'',lit(branchcode(snowparkdf["branch_code"])))
            
            snowparkdf = snowparkdf.withColumn(''rn'', row_number().over(Window.partition_by(snowparkdf[''sku_code''],snowparkdf[''osa_check_date''],coalesce(snowparkdf[''branch_code''],snowparkdf[''branch_code''])).order_by(snowparkdf[''week''],snowparkdf[''item_description''])))

            snowparkdf = snowparkdf.withColumn("acct_deliv_date",when(col(''acct_deliv_date'')==None,lit(None)).otherwise( regexp_replace(col("acct_deliv_date"), r"(\\d{2})(\\d{2})-(\\d{2})-(\\d{2}) (\\d{2}:\\d{2}:\\d{2})", r"\\3/\\4/\\2")))

            snowparkdf = snowparkdf.withColumn("OSA_CHECK_DATE",to_date(col("OSA_CHECK_DATE")))

            snowparkdf = snowparkdf.filter(col(''rn'') == 1)

            # extracting important columns
            snowdf = snowparkdf.select(
                    ''ret_nm_prefix'',
                     ''sku_code'',
                     ''brand'',
                     ''barcode'',
                     ''item_description'',
                     ''msl_large'',
                     ''msl_small'',
                     ''msl_premium'',
                     ''MONTH'',
                     ''week'',
                     ''reason'',
                     ''acct_deliv_date'',
                     ''osa_check_date'',
                     ''encoded_report'',
                     ''team_leader'',
                     ''branch_code'',
                     ''branch_code_original'',
                     ''branch_classification'',
                     ''branch_name'',
                     ''osa_flag'',
                     ''retailer_name'',
                     ''filename'',
                     ''run_id'',
                     ''crtd_dttm''
                    
        )

            # Load Data to the target table
            snowdf.write.mode("append").saveAsTable(target_table)

            current_date = datetime.now()
            formatted_year = current_date.strftime("%Y")
            formatted_month = current_date.strftime("%m")

             # write to success folder
            file_name = file_name.split(''.'')[0]+"_"+datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")

            

            snowdf.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True,file_format_name=''ph_non_pos_svi_smc_FORMAT'')
            
            
            return ''Success''
            

            
            

    except KeyError as key_error:
        # Handle KeyError (missing columns) here
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message
        
    except Exception as e:
        # Handle exceptions here
        error_message = f"Error: {str(e)}"
        return error_message;';

------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE PROCEDURE PHLSDL_RAW.PH_NON_POS_PUREGOLD_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('openpyxl==3.0.10','snowflake-snowpark-python==*','unidecode==1.2.0')
HANDLER = 'main'
EXECUTE AS OWNER
AS 'import snowflake.snowpark as snowpark
from snowflake import snowpark
from snowflake.snowpark.window import Window
from snowflake.snowpark.functions import col,lit,concat,regexp_replace,trim,split,rtrim,upper,coalesce,row_number,when,to_date,is_null
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField,DecimalType,DateType
from snowflake.snowpark.files import SnowflakeFile
import pandas as pd
import numpy as np
import openpyxl
from datetime import datetime
import pytz
from unidecode import unidecode
from re import sub  

def main(session: snowpark.Session,Param):

    try:

        #Param=[''PUREGOLD_20230102_20230123150018.csv'',''PHLSDL_RAW.DEV_LOAD_STAGE_ADLS'',''dev/non_pos/transaction/puregold/ph_non_pos_puregold'',''sdl_ph_non_ise_puregold'']

        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        file_header_loc = 10

        #Reading data from adls
        full_path = "@"+stage_name+"/"+temp_stage_path+"/"+file_name
        
        with SnowflakeFile.open(full_path,''rb'',require_scoped_url= False) as f:
            
            
            df = pd.read_csv(f,delimiter='''')
            #print(df)

             # Converting the dataframe into  string  using applymap so that it can be decoded it using unidecode as some values are not ascii
            
            df = df.applymap(lambda x: str(x))
            
            df = df.applymap(lambda x : unidecode(x))

            #Replacing all nan with null
            df = df.applymap(lambda x: None if x == ''nan'' else x)

            #setting mode as index so values below it can be converted into columns after transpose

            df.columns = df.iloc[0]
            df1 = df.set_index(''WEEKLY STOCK AVAILABILITY MONITORING REPORT'')

             #extracting required columns after transposing 
            
            verticle_df = df1.T[9:][[''MONTH:'',''WEEK:'',''ACCOUNT: PUREGOLD'',''Actual Delivery Date:'',''OSA Check Date'',''Encoded Report'',''TEAM LEADER:'',''BRANCH CODE:'',''Branch Size and Classification'',''SKU CODE'']]

            #As data for the horizontal columns which contain sku code branch barcode are after 10th row
            df2 = df1[file_header_loc:]
            df2.reset_index(inplace=True)
            df2.columns = df2.iloc[0]
            df2 = df2[1:]

   

             #--Extracting Month,Week,Account,Actual delivery date, osa check date,encoded report, team leader,branch code, Branch size,and branch name as SKU code 
            #-- will be joining them afterwards with 

            transpose_df = df1[:11].T
            transpose_df.columns = [i.strip() for i in transpose_df.columns]

            transpose_df.dropna(subset=[''MONTH:''],inplace=True)

            #extracting required columns after transposing 
            
            verticle_df = transpose_df[10:][[''MONTH:'',''WEEK:'',''ACCOUNT: PUREGOLD'',''Actual Delivery Date:'',''OSA Check Date'',''Encoded Report'',''TEAM LEADER:'',''BRANCH CODE:'',''Branch Size and Classification'',''SKU CODE'']]
            #print(verticle_df)

            #Converting ''Actual Delivery Date:'' into string and then converting string nan into null
            
            

            #As data for the horizontal columns which contain sku code branch barcode are after 10th row
            horizontal_df = df1[file_header_loc:]

            #replacing columns names with original and not taking 1st row as it contains col names
            
            

            horizontal_df.reset_index(inplace=True)
            horizontal_df.columns = horizontal_df.iloc[0]
            
            horizontal_df = horizontal_df[1:]
            
           

            print(horizontal_df)
            

             #seperating valvars and idvars so that we can unpivote it

            valvars = []

            for i in horizontal_df.columns[11:]:
                valvars.append(i)

            idvars = [''SKU CODE'',''BRAND'',''BARCODE'',''ITEM DESCRIPTION'',''SPMKT LARGE'',''SPMKT SMALL'',''GROCERY'']


             #Converting columns for branch name and osa flag
            unpivoted_df = horizontal_df.melt(id_vars=idvars,value_vars=valvars,var_name=''BRANCH_NAME'',value_name=''OSA_FLAG'')

            

            #Changing sku code to branch name so sa to join two tabless virticle df and 
            
            verticle_df.rename(columns={''SKU CODE'':''BRANCH_NAME''},inplace=True)

            joined_df = unpivoted_df.merge(verticle_df,on=''BRANCH_NAME'',how=''inner'')

            #Deleting values where month is null
            joined_df.dropna(subset=[''MONTH:''],inplace=True)

            #getting actual delivery date in proper date format
            joined_df[''Actual Delivery Date:'']=joined_df[''Actual Delivery Date:''].apply(lambda x :sub(r''([0-9]{2})([0-9]{2})-([0-9]{2})-([0-9]{2})'',r"\\3/\\4/\\2",x) if isinstance(x,str) else x)
            #joined_df[''OSA Check Date''] = joined_df[''OSA Check Date''].apply(lambda x: sub(r''NO DEL'',None,x) )
            
            
            #Creating snowpark dataframe
            snowparkdf = session.create_dataframe(joined_df)

            #Converting columns similar to table column names
            snowparkdf = snowparkdf.rename({col(''SKU CODE''):''sku_code'',col(''BRAND''):''brand'',col(''BARCODE''):''barcode'',\\
                                        col(''ITEM DESCRIPTION''):''item_description'',col(''SPMKT LARGE''):''msl_large'',\\
                                        col(''SPMKT SMALL''):''msl_small'',col(''GROCERY''):''msl_grocery'',\\
                                        col(''MONTH:''):''month'',col(''WEEK:''):''week'',col(''ACCOUNT: PUREGOLD''):''account'',\\
                                        col(''Actual Delivery Date:''):''acct_deliv_date'',col(''OSA Check Date''):''osa_check_date'',\\
                                        col(''Encoded Report''):''encoded_report'',col(''TEAM LEADER:''):''team_leader''\\
                                        ,col(''BRANCH CODE:''):''branch_code'',\\
                                        col(''Branch Size and Classification''):''branch_classification'',\\
                                        col(''"BRANCH_NAME"''):''branch_name'',\\
                                         col(''"OSA_FLAG"''):''osa_flag''})

             #Handling null values and empty rows

            snowparkdf = snowparkdf.na.drop(''all'')

            #Checking if dataframe is having any data
            
            if snowparkdf.count()==0:
                return "No Data in file"

           ##FUNCTIONS FOR TRANSFORMATION##

            
            #writing function for ret_nm_prefi
            def get_ret_nm_prefix(branch_code,branch_name):
                if (branch_code is not None or branch_code != ''''):
                    return regexp_replace(split(branch_code,lit(''-''))[0],''[a-z]'',''O'')
                else:
                    return regexp_replace(split(branch_code,lit('' ''))[0],'''','''') 

            #logic to extract branch_code
            def branchcode(branch_code):
                if branch_code  is not None or branch_code != '''':
                    return regexp_replace(lit(split(branch_code,lit("-"))[1]),'''','''')

            #retailer name extraction logic 
            def retailer_name(file_name):
                return rtrim(regexp_replace(split(lit(file_name),lit(''.''))[0],''[0-9]'',''''),lit(''_''))


            def filename_change(filename):
                filename1 = filename.split(''.'')
                filename1[1]=''xlsx''
                newfilename = ''.''.join(filename1)
                return newfilename



             #----------------------------------------------------------------------------#
            #Adding columns that are needed for final table 
            
            snowparkdf = snowparkdf.withColumn("ret_nm_prefix", lit(''PG''))

            snowparkdf = snowparkdf.with_column("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))

            snowparkdf = snowparkdf.withColumn("crtd_dttm",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))

            snowparkdf = snowparkdf.withColumn(''retailer_name'',retailer_name(file_name))
            
            snowparkdf = snowparkdf.withColumn(''filename'',lit(file_name))
            
            snowparkdf = snowparkdf.withColumn(''branch_code_original'', lit(snowparkdf[''branch_code'']))
            
            snowparkdf = snowparkdf.withColumn(''branch_code'',lit(snowparkdf["branch_code"]))
            
            snowparkdf = snowparkdf.withColumn(''rn'', row_number().over(Window.partition_by(snowparkdf[''sku_code''],snowparkdf[''osa_check_date''],coalesce(snowparkdf[''branch_code''],snowparkdf[''branch_code''])).order_by(snowparkdf[''week''],snowparkdf[''item_description''])))

            snowparkdf = snowparkdf.withColumn("acct_deliv_date",when(col(''acct_deliv_date'')==None,lit(None)).otherwise( regexp_replace(col("acct_deliv_date"), r"(\\d{2})(\\d{2})-(\\d{2})-(\\d{2}) (\\d{2}:\\d{2}:\\d{2})", r"\\3/\\4/\\2")))

            snowparkdf = snowparkdf.withColumn("OSA_CHECK_DATE",to_date(col("OSA_CHECK_DATE")))

            snowparkdf = snowparkdf.filter(col(''rn'') == 1)

             # extracting important columns
            snowdf = snowparkdf.select(
                    ''ret_nm_prefix'',
                     ''sku_code'',
                     ''brand'',
                     ''barcode'',
                     ''item_description'',
                     ''msl_large'',
                     ''msl_small'',
                     ''msl_grocery'',
                     ''MONTH'',
                     ''week'',
                     ''account'',
                     ''acct_deliv_date'',
                     ''osa_check_date'',
                     ''encoded_report'',
                     ''team_leader'',
                     ''branch_code'',
                     ''branch_code_original'',
                     ''branch_classification'',
                     ''branch_name'',
                     ''osa_flag'',
                     ''retailer_name'',
                     ''filename'',
                     ''run_id'',
                     ''crtd_dttm''
                    
        )

             # Load Data to the target table
            snowdf.write.mode("append").saveAsTable(target_table)

            current_date = datetime.now()
            formatted_year = current_date.strftime("%Y")
            formatted_month = current_date.strftime("%m")

             # write to success folder
            file_name = file_name.split(''.'')[0]+"_"+datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")

            

            snowdf.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True,file_format_name=''ph_non_pos_svi_smc_FORMAT'')

           
            
            

            return ''Success''

        
            
    except KeyError as key_error:
        # Handle KeyError (missing columns) here
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message
        
    except Exception as e:
        # Handle exceptions here
        error_message = f"Error: {str(e)}"
        return error_message;

           
            
   ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE PHLSDL_RAW.PH_NON_POS_ROBINSONS_DS_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('openpyxl==3.0.10','snowflake-snowpark-python==*','unidecode==1.2.0')
HANDLER = 'main'
EXECUTE AS OWNER
AS 'import snowflake.snowpark as snowpark
from snowflake import snowpark
from snowflake.snowpark.window import Window
from snowflake.snowpark.functions import col,lit,concat,regexp_replace,trim,split,rtrim,upper,coalesce,row_number,when,to_date,is_null
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField,DecimalType,DateType
from snowflake.snowpark.files import SnowflakeFile
import pandas as pd
import numpy as np
import openpyxl
from datetime import datetime
import pytz
from unidecode import unidecode
from re import sub

def main(session: snowpark.Session,Param):
    

    try:

        #Param=[''ROBINSONS_DS_20230521_20230601150011.csv'',''PHLSDL_RAW.DEV_LOAD_STAGE_ADLS'',''dev/non_pos/transaction/robinsons_ds/ph_non_pos_robinsons_ds/'',''sdl_ph_non_ise_robinsons_ds'']

        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        file_header_loc = 10

        #Reading data from adls
        full_path = "@"+stage_name+"/"+temp_stage_path+"/"+file_name
        
        with SnowflakeFile.open(full_path,''rb'',require_scoped_url= False) as f:
            df = pd.read_csv(f,delimiter='''',engine=''python'')
            print(df)

            # Converting the dataframe into  string  using applymap so that it can be decoded it using unidecode as some values are not ascii
            
            df = df.applymap(lambda x: str(x))

            df = df.applymap(lambda x : unidecode(x))

            #Replacing all nan with null
            df = df.applymap(lambda x: None if x == ''nan'' else x)

            
            #setting mode as index so values below it can be converted into columns after transpose

            df.columns = df.iloc[0]
            df1 = df.set_index(''WEEKLY STOCK AVAILABILITY MONITORING REPORT'')

            #extracting required columns after transposing 
            
            verticle_df = df1.T[7:][[''MONTH:'',''WEEK:'',''ACCOUNT: WALTERMART'',''Actual Delivery Date:'',''OSA Check Date'',''Encoded Report'',''TEAM LEADER:'',''BRANCH CODE:'',''Branch Size and Classification'',''SKU CODE'']]

            #As data for the horizontal columns which contain sku code branch barcode are after 10th row
            df2 = df1[file_header_loc:]

            #replacing columns names with original and not taking 1st row as it contains col names
            
            df2.reset_index(inplace=True)
            df2.columns = df2.iloc[0]
            df2 = df2[1:]
            #seperating valvars and idvars so that we can unpivote it

            #seperating valvars and idvars so that we can unpivote it
            
            valvars = []
            
            for i in df2.columns[5:]:
                valvars.append(i)
                
            df2.rename(columns={df2.columns[4]:''DEPT MASS''},inplace=True)
            idvars = [''SKU CODE'',''BRAND'',''BARCODE'',''ITEM DESCRIPTION'',''DEPT MASS'']
            print(valvars)

            #Converting columns for branch name and osa flag
            unpivoted_df = df2.melt(id_vars=idvars,value_vars=valvars,var_name=''branch_name'',value_name=''osa_flag'')

            unpivoted_df.rename(columns={''MODE'':''SKU CODE''},inplace=True)

            #Changing sku code to branch name so sa to join two tables9virticle df and 
            
            verticle_df.rename(columns={''SKU CODE'':''branch_name''},inplace=True)

            joined_df = unpivoted_df.merge(verticle_df,on=''branch_name'',how=''inner'')
            #print(joined_df)
            #dropping row where column value of ''SKU CODE is string or null''
            joined_df.dropna(subset=[''SKU CODE''],inplace=True)
            
            #joined_df = joined_df[joined_df[''SKU CODE''].apply(lambda x:isinstance(x,int))]
            
            #getting actual delivery date in proper date format
            joined_df[''Actual Delivery Date:'']=joined_df[''Actual Delivery Date:''].apply(lambda x :sub(r''([0-9]{2})([0-9]{2})-([0-9]{2})-([0-9]{2})'',r"\\3/\\4/\\2",x) if isinstance(x,str) else x)
            joined_df[''Actual Delivery Date:'']=joined_df[''Actual Delivery Date:''].apply(lambda x :x[:8] if isinstance(x,str) else x)

            #Deleting values where month is null
            joined_df.dropna(subset=[''MONTH:''],inplace=True)

            snowparkdf = session.create_dataframe(joined_df)

            #Converting columns similar to table column names
            snowparkdf = snowparkdf.rename({col(''SKU CODE''):''sku_code'',col(''BRAND''):''brand'',col(''BARCODE''):''barcode'',\\
                                        col(''ITEM DESCRIPTION''):''item_description'',col(''DEPT MASS''):''msl_dept'',\\
                                        col(''MONTH:''):''month'',col(''WEEK:''):''week'',col(''ACCOUNT: WALTERMART''):''reason'',\\
                                        col(''Actual Delivery Date:''):''acct_deliv_date'',col(''OSA Check Date''):''osa_check_date'',\\
                                        col(''Encoded Report''):''encoded_report'',col(''TEAM LEADER:''):''team_leader''\\
                                        ,col(''BRANCH CODE:''):''branch_code'',\\
                                        col(''Branch Size and Classification''):''branch_classification'',\\
                                        col(''"branch_name"''):''branch_name'',\\
                                         col(''"osa_flag"''):''osa_flag''})
            
            

            #Handling null values and empty rows

            snowparkdf = snowparkdf.na.drop(''all'')

            #Checking if dataframe is having any data
            
            if snowparkdf.count()==0:
                return "No Data in file"

           ##FUNCTIONS FOR TRANSFORMATION##

            
            #writing function for ret_nm_prefi
            def get_ret_nm_prefix(branch_code,branch_name):
                if (branch_code is not None or branch_code != ''''):
                    return regexp_replace(split(branch_code,lit(''-''))[0],''[a-z]'',''O'')
                else:
                    return regexp_replace(split(branch_code,lit('' ''))[0],'''','''') 

            #logic to extract branch_code
            def branchcode(branch_code):
                if branch_code  is not None or branch_code != '''':
                    return regexp_replace(lit(split(branch_code,lit("-"))[1]),'''','''')

            #retailer name extraction logic 
            def retailer_name(file_name):
                return rtrim(regexp_replace(split(lit(file_name),lit(''.''))[0],''[0-9]'',''''),lit(''_''))

            def filename_change(filename):
                filename1 = filename.split(''.'')
                filename1[1]=''xlsx''
                newfilename = ''.''.join(filename1)
                return newfilename
                            
                    

            #----------------------------------------------------------------------------#
            #Adding columns that are needed for final table 
            
            snowparkdf = snowparkdf.withColumn("ret_nm_prefix", lit(''RDS''))

            snowparkdf = snowparkdf.with_column("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))

            snowparkdf = snowparkdf.withColumn("crtd_dttm",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))

            snowparkdf = snowparkdf.withColumn(''retailer_name'',retailer_name(file_name))
            
            
            snowparkdf = snowparkdf.withColumn(''filename'',lit(filename_change(file_name)))
            
            snowparkdf = snowparkdf.withColumn(''branch_code_original'', lit(snowparkdf[''branch_code'']))
            
            snowparkdf = snowparkdf.withColumn(''branch_code'',lit(branchcode(snowparkdf["branch_code"])))
            
            snowparkdf = snowparkdf.withColumn(''rn'', row_number().over(Window.partition_by(snowparkdf[''sku_code''],snowparkdf[''osa_check_date''],coalesce(snowparkdf[''branch_code''],snowparkdf[''branch_code''])).order_by(snowparkdf[''week''],snowparkdf[''item_description''])))

            snowparkdf = snowparkdf.withColumn("acct_deliv_date",when(col(''acct_deliv_date'')==None,lit(None)).otherwise( regexp_replace(col("acct_deliv_date"), r"(\\d{2})(\\d{2})-(\\d{2})-(\\d{2}) (\\d{2}:\\d{2}:\\d{2})", r"\\3/\\4/\\2")))

            snowparkdf = snowparkdf.withColumn("OSA_CHECK_DATE",to_date(col("OSA_CHECK_DATE")))

            snowparkdf = snowparkdf.filter(col(''rn'') == 1)

            # extracting important columns
            snowdf = snowparkdf.select(
                    ''ret_nm_prefix'',
                     ''sku_code'',
                     ''brand'',
                     ''barcode'',
                     ''item_description'',
                     ''msl_dept'',
                     ''MONTH'',
                     ''week'',
                     ''reason'',
                     ''acct_deliv_date'',
                     ''osa_check_date'',
                     ''encoded_report'',
                     ''team_leader'',
                     ''branch_code'',
                     ''branch_code_original'',
                     ''branch_classification'',
                     ''branch_name'',
                     ''osa_flag'',
                     ''retailer_name'',
                     ''filename'',
                     ''run_id'',
                     ''crtd_dttm''
                    
        )

            # Load Data to the target table
            snowdf.write.mode("append").saveAsTable(target_table)

            current_date = datetime.now()
            formatted_year = current_date.strftime("%Y")
            formatted_month = current_date.strftime("%m")

             # write to success folder
            file_name = file_name.split(''.'')[0]+"_"+datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")

            

            snowdf.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True,file_format_name=''ph_non_pos_svi_smc_FORMAT'')

            

            

            return ''Success''

    except KeyError as key_error:
        # Handle KeyError (missing columns) here
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message
        
    except Exception as e:
        # Handle exceptions here
        error_message = f"Error: {str(e)}"
        return error_message;';

-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE PHLSDL_RAW.PH_NON_POS_ROBINSONS_SM_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('openpyxl==3.0.10','snowflake-snowpark-python==*','unidecode==1.2.0')
HANDLER = 'main'
EXECUTE AS OWNER
AS 'import snowflake.snowpark as snowpark
from snowflake import snowpark
from snowflake.snowpark.window import Window
from snowflake.snowpark.functions import col,lit,concat,regexp_replace,trim,split,rtrim,upper,coalesce,row_number,when,to_date,is_null
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField,DecimalType,DateType
from snowflake.snowpark.files import SnowflakeFile
import pandas as pd
import numpy as np
import openpyxl
from datetime import datetime
import pytz
from unidecode import unidecode
from re import sub

def main(session: snowpark.Session,Param):
    

    try:

        #Param=[''ROBINSONS_SM_20210131_20210806154234.xlsx'',''PHLSDL_RAW.DEV_LOAD_STAGE_ADLS'',''dev/non_pos/transaction/robinsons_sm/ph_non_pos_robinsons_sm'',''sdl_ph_non_ise_svi_smc'']

        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        file_header_loc = 10

        #Reading data from adls
        full_path = "@"+stage_name+"/"+temp_stage_path+"/"+file_name
        
        with SnowflakeFile.open(full_path,''rb'',require_scoped_url= False) as f:
            df = pd.read_excel(f)

            # Converting the dataframe into  string  using applymap so that it can be decoded it using unidecode as some values are not ascii
            
            df = df.applymap(lambda x: str(x))

            df = df.applymap(lambda x : unidecode(x))

            #Replacing all nan with null
            df = df.applymap(lambda x: None if x == ''nan'' else x)
            df.columns = df.iloc[0]
            df1 = df.set_index(''WEEKLY STOCK AVAILABILITY MONITORING REPORT'')

            #extracting required columns after transposing 
            
            verticle_df = df1.T[7:][[''MONTH:'',''WEEK:'',''ACCOUNT: ROBINSONS SM'',''Actual Delivery Date:'',''OSA Check Date'',''Encoded Report'',''TEAM LEADER:'',''BRANCH CODE:'',''Branch Size and Classification'',''SKU CODE'']]

             #As data for the horizontal columns which contain sku code branch barcode are after 10th row
            df2 = df1[file_header_loc:]
            df2.reset_index(inplace=True)
            df2.columns = df2.iloc[0]
            df2 = df2[1:]
            #print(df2.columns)
            
           
            
          

            #seperating valvars and idvars so that we can unpivote it

            #seperating valvars and idvars so that we can unpivote it
            
            valvars = []
            
            for i in df2.columns[9:]:
                valvars.append(i)
                
            
            idvars = [''SKU CODE'',''BRAND'',''BARCODE'',''ITEM DESCRIPTION'',''MSL LARGE'',''MSL SMALL'',''MSL PREMIUM'']

            #Converting columns for branch name and osa flag
            unpivoted_df = df2.melt(id_vars=idvars,value_vars=valvars,var_name=''branch_name'',value_name=''osa_flag'')

            #Changing sku code to branch name so sa to join two tables9virticle df and 
            
            verticle_df.rename(columns={''SKU CODE'':''branch_name''},inplace=True)

            #For debugging
            #print(unpivoted_df)

            joined_df = unpivoted_df.merge(verticle_df,on=''branch_name'',how=''inner'')
            print(joined_df[[''osa_flag'']])
            #dropping row where column value of ''''SKU CODE is string or null''''
            joined_df.dropna(subset=[''SKU CODE''],inplace=True)

            #for debugging
            #print(joined_df)

            #getting actual delivery date in proper date format
            joined_df[''Actual Delivery Date:'']=joined_df[''Actual Delivery Date:''].apply(lambda x :sub(r''([0-9]{2})([0-9]{2})-([0-9]{2})-([0-9]{2})'',r"\\3/\\4/\\2",x) if isinstance(x,str) else x)
            joined_df[''Actual Delivery Date:'']=joined_df[''Actual Delivery Date:''].apply(lambda x :x[:8] if isinstance(x,str) else x)

            #Deleting values where month is null
            joined_df.dropna(subset=[''MONTH:''],inplace=True)
            print(joined_df)

            
                
            
                

            #joined_df[''osa_flag'']= joined_df[''osa_flag''].apply(lambda x: osa_flag_correction_float(x))
            #joined_df[''osa_flag''] = joined_df[''osa_flag''].apply(lambda x: osa_flag_correction_alpha(x) )

            snowparkdf = session.create_dataframe(joined_df)

            #Converting columns similar to table column names
            snowparkdf = snowparkdf.rename({col(''SKU CODE''):''sku_code'',col(''BRAND''):''brand'',col(''BARCODE''):''barcode'',\\
                                        col(''ITEM DESCRIPTION''):''item_description'',col(''MSL LARGE''):''msl_large'',\\
                                        col(''MSL SMALL''):''msl_small'',col(''MSL PREMIUM''):''msl_premium'',\\
                                        col(''MONTH:''):''month'',col(''WEEK:''):''week'',col(''ACCOUNT: ROBINSONS SM''):''reason'',\\
                                        col(''Actual Delivery Date:''):''acct_deliv_date'',col(''OSA Check Date''):''osa_check_date'',\\
                                        col(''Encoded Report''):''encoded_report'',col(''TEAM LEADER:''):''team_leader''\\
                                        ,col(''BRANCH CODE:''):''branch_code'',\\
                                        col(''Branch Size and Classification''):''branch_classification'',\\
                                        col(''"branch_name"''):''branch_name'',\\
                                         col(''"osa_flag"''):''osa_flag''})

            #Handling null values and empty rows

            snowparkdf = snowparkdf.na.drop(''all'')

            #Checking if dataframe is having any data
            
            if snowparkdf.count()==0:
                return "No Data in file"


            ##FUNCTIONS FOR TRANSFORMATION##

            
            #writing function for ret_nm_prefi
            def get_ret_nm_prefix(branch_code,branch_name):
                if (branch_code is not None or branch_code != ''''):
                    return regexp_replace(split(branch_code,lit(''-''))[0],''[a-z]'',''O'')
                else:
                    return regexp_replace(split(branch_code,lit('' ''))[0],'''','''') 

            #logic to extract branch_code
            def branchcode(branch_code):
                if branch_code  is not None or branch_code != '''':
                    return regexp_replace(lit(branch_code),''[A-Z]*'','''')

            #retailer name extraction logic 
            def retailer_name(file_name):
                return rtrim(regexp_replace(split(lit(file_name),lit(''.''))[0],''[0-9]'',''''),lit(''_''))

            def filename_change(filename):
                filename1 = filename.split(''.'')
                filename1[1]=''xlsx''
                newfilename = ''.''.join(filename1)
                return newfilename

            #----------------------------------------------------------------------------#
            #Adding columns that are needed for final table 
            
            snowparkdf = snowparkdf.withColumn("ret_nm_prefix", lit(''ROB''))

            snowparkdf = snowparkdf.with_column("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))

            snowparkdf = snowparkdf.withColumn("crtd_dttm",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))

            snowparkdf = snowparkdf.withColumn(''retailer_name'',retailer_name(file_name))
            
            
            snowparkdf = snowparkdf.withColumn(''filename'',lit(filename_change(file_name)))
            
            snowparkdf = snowparkdf.withColumn(''branch_code_original'', lit(snowparkdf[''branch_code'']))
            
            snowparkdf = snowparkdf.withColumn(''branch_code'',lit(branchcode(snowparkdf["branch_code"])))
            
            snowparkdf = snowparkdf.withColumn(''rn'', row_number().over(Window.partition_by(snowparkdf[''sku_code''],snowparkdf[''osa_check_date''],coalesce(snowparkdf[''branch_code''],snowparkdf[''branch_code''])).order_by(snowparkdf[''week''],snowparkdf[''item_description''])))

            snowparkdf = snowparkdf.withColumn("acct_deliv_date",when(col(''acct_deliv_date'')==None,lit(None)).otherwise( regexp_replace(col("acct_deliv_date"), r"(\\d{2})(\\d{2})-(\\d{2})-(\\d{2}) (\\d{2}:\\d{2}:\\d{2})", r"\\3/\\4/\\2")))

            snowparkdf = snowparkdf.withColumn("OSA_CHECK_DATE",to_date(col("OSA_CHECK_DATE")))

            snowparkdf = snowparkdf.filter(col(''rn'') == 1)

            # extracting important columns
            snowdf = snowparkdf.select(
                    ''ret_nm_prefix'',
                     ''sku_code'',
                     ''brand'',
                     ''barcode'',
                     ''item_description'',
                     ''msl_large'',
                     ''msl_small'',
                     ''msl_premium'',
                     ''MONTH'',
                     ''week'',
                     ''reason'',
                     ''acct_deliv_date'',
                     ''osa_check_date'',
                     ''encoded_report'',
                     ''team_leader'',
                     ''branch_code'',
                     ''branch_code_original'',
                     ''branch_classification'',
                     ''branch_name'',
                     ''osa_flag'',
                     ''retailer_name'',
                     ''filename'',
                     ''run_id'',
                     ''crtd_dttm''
                    
        )

            # Load Data to the target table
            snowdf.write.mode("append").saveAsTable(target_table)

            current_date = datetime.now()
            formatted_year = current_date.strftime("%Y")
            formatted_month = current_date.strftime("%m")

             # write to success folder
            file_name = file_name.split(''.'')[0]+"_"+datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")

            

            snowdf.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True,file_format_name=''ph_non_pos_svi_smc_FORMAT'')
            
            
            return ''Success''

            

    except KeyError as key_error:
        # Handle KeyError (missing columns) here
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message
        
    except Exception as e:
        # Handle exceptions here
        error_message = f"Error: {str(e)}"
        return error_message;
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE PHLSDL_RAW.PH_NON_POS_SHM_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('openpyxl==3.0.10','snowflake-snowpark-python==*','unidecode==1.2.0')
HANDLER = 'main'
EXECUTE AS OWNER
AS 'import snowflake.snowpark as snowpark
from snowflake import snowpark
from snowflake.snowpark.window import Window
from snowflake.snowpark.functions import col,lit,concat,regexp_replace,trim,split,rtrim,upper,coalesce,row_number,when,to_date,is_null,try_cast
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField,DecimalType,DateType
from snowflake.snowpark.files import SnowflakeFile
import pandas as pd
import numpy as np
import openpyxl
from datetime import datetime
import pytz
from unidecode import unidecode
from re import sub

def main(session: snowpark.Session,Param):

    try:

        #Param=[''SHM_20230417_20230505150011.csv'',''PHLSDL_RAW.DEV_LOAD_STAGE_ADLS'',''dev/non_pos/transaction/shm/ph_non_pos_shm/'',''sdl_ph_non_ise_shm'']

        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        file_header_loc = 10

        #Reading data from adls
        full_path = "@"+stage_name+"/"+temp_stage_path+"/"+file_name
        
        with SnowflakeFile.open(full_path,''rb'',require_scoped_url= False) as f:
            
            df = pd.read_csv(f,delimiter='''')
            #print(df)

             # Converting the dataframe into  string  using applymap so that it can be decoded it using unidecode as some values are not ascii
            
            df = df.applymap(lambda x: str(x))

            df = df.applymap(lambda x : unidecode(x))

            #Replacing all nan with null
            df = df.applymap(lambda x: None if x == ''nan'' else x)
           

            #setting mode as index so values below it can be converted into columns after transpose

            df.columns = df.iloc[0]
            df1 = df.set_index(''WEEKLY STOCK AVAILABILITY MONITORING REPORT'')
            
            df2 = df1[:11].T
            df2.columns = [i.strip() for i in df2.columns]
            
           

            #extracting required columns after transposing 
            
            verticle_df = df2[8:][[''MONTH:'',''WEEK:'',''ACCOUNT: SHM'',''Actual Delivery Date:'',''OSA Check Date'',''Encoded Report'',''TEAM LEADER:'',''BRANCH CODE:'',''Branch Size and Classification'',''SKU CODE'']]

           
                

            #As data for the horizontal columns which contain sku code branch barcode are after 10th row
            df2 = df1[file_header_loc:]
            print(df2)

            #replacing columns names with original and not taking 1st row as it contains col names
            df2.reset_index(inplace=True)
            df2.columns = df2.iloc[0]
            df2 = df2[1:]

            #seperating valvars and idvars so that we can unpivote it
            
            valvars = []
            print(df2.columns[4])
            df2.rename(columns={df2.columns[4]:''SPMKT LARGE'',df2.columns[5]:''SPMKT SMALL'',df2.columns[6]:''SPMKT PREM''},inplace=True)
            print(df2.columns)
            
            for i in df2.columns[9:]:
                valvars.append(i)
                
            idvars = []
            idvars = [''SKU CODE'',''BRAND'',''BARCODE'',''ITEM DESCRIPTION'',''SPMKT LARGE'',''SPMKT SMALL'',''SPMKT PREM'']

            

            
            

            #Converting columns for branch name and osa flag
            unpivoted_df = df2.melt(id_vars=idvars,value_vars=valvars,var_name=''branch_name'',value_name=''osa_flag'')

            #unpivoted_df.rename(columns={''MODE'':''SKU CODE''},inplace=True)

            #Changing sku code to branch name so sa to join two tables9virticle df and 
            
            verticle_df.rename(columns={''SKU CODE'':''branch_name''},inplace=True)

            joined_df = unpivoted_df.merge(verticle_df,on=''branch_name'',how=''inner'')
            

            #dropping row where column value of ''SKU CODE is string or null''
            joined_df.dropna(subset=[''SKU CODE''],inplace=True)
            
            #joined_df = joined_df[joined_df[''SKU CODE''].apply(lambda x:isinstance(x,int))]
                    
    
            #Deleting values where month is null
            joined_df.dropna(subset=[''MONTH:''],inplace=True)

            joined_df[''Actual Delivery Date:'']=joined_df[''Actual Delivery Date:''].apply(lambda x :sub(r''([0-9]{2})([0-9]{2})-([0-9]{2})-([0-9]{2})'',r"\\3/\\4/\\2",x) if isinstance(x,str) else x)
            joined_df[''Actual Delivery Date:'']=joined_df[''Actual Delivery Date:''].apply(lambda x :x[:8] if isinstance(x,str) else x)

            #for debugging
            print(joined_df[''Actual Delivery Date:''])

            #Converting the dataframe into snowpark-dataframe
            snowparkdf = session.create_dataframe(joined_df)
            

            #return snowparkdf

            #Converting columns similar to table column names
            snowparkdf = snowparkdf.rename({col(''SKU CODE''):''sku_code'',col(''BRAND''):''brand'',col(''BARCODE''):''barcode'',\\
                                        col(''ITEM DESCRIPTION''):''item_description'',col(''SPMKT LARGE''):''msl_large'',\\
                                        col(''SPMKT SMALL''):''msl_small'',col(''SPMKT PREM''):''msl_premium'',\\
                                        col(''MONTH:''):''month'',col(''WEEK:''):''week'',col(''ACCOUNT: SHM''):''reason'',\\
                                        col(''Actual Delivery Date:''):''acct_deliv_date'',col(''OSA Check Date''):''osa_check_date'',\\
                                        col(''Encoded Report''):''encoded_report'',col(''TEAM LEADER:''):''team_leader''\\
                                        ,col(''BRANCH CODE:''):''branch_code'',\\
                                        col(''Branch Size and Classification''):''branch_classification'',\\
                                        col(''"branch_name"''):''branch_name'',\\
                                         col(''"osa_flag"''):''osa_flag''})
            
            

            #Handling null values and empty rows

            snowparkdf = snowparkdf.na.drop(''all'')

            #Checking if dataframe is having any data
            
            if snowparkdf.count()==0:
                return "No Data in file"

           ##FUNCTIONS FOR TRANSFORMATION##

            
            #writing function for ret_nm_prefi
            def get_ret_nm_prefix(branch_code,branch_name):
                if (branch_code is not None or branch_code != ''''):
                    return regexp_replace(split(branch_code,lit(''-''))[0],''[a-z]'',''O'')
                else:
                    return regexp_replace(split(branch_code,lit('' ''))[0],'''','''') 

            #logic to extract branch_code
            def branchcode(branch_code):
                if branch_code  is not None or branch_code != '''':
                    return regexp_replace(lit(branch_code),''[A-Z-]*'','''')

            #retailer name extraction logic 
            def retailer_name(file_name):
                return rtrim(regexp_replace(split(lit(file_name),lit(''.''))[0],''[0-9]'',''''),lit(''_''))

            #logic to handle null values in date
            def handle_nulls_date(date_col):
                return to_date(when(date_col.isNotNull() & (date_col !=''''),lit(date_col)).otherwise(None),lit("MM/dd/yyyy"))

            
            def filename_change(filename):
                filename1 = filename.split(''.'')
                filename1[1]=''xlsx''
                newfilename = ''.''.join(filename1)
                return newfilename    
                    

            #----------------------------------------------------------------------------#
            #Adding columns that are needed for final table 
            
            snowparkdf = snowparkdf.withColumn("ret_nm_prefix", lit(''SSM''))

            snowparkdf = snowparkdf.with_column("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))

            snowparkdf = snowparkdf.withColumn("crtd_dttm",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))

            snowparkdf = snowparkdf.withColumn(''retailer_name'',retailer_name(file_name))
            
            snowparkdf = snowparkdf.withColumn(''filename'',lit(filename_change(file_name)))
            
            snowparkdf = snowparkdf.withColumn(''branch_code_original'', lit(snowparkdf[''branch_code'']))
            
            snowparkdf = snowparkdf.withColumn(''branch_code'',lit(branchcode(snowparkdf["branch_code"])))
            
            snowparkdf = snowparkdf.withColumn(''rn'', row_number().over(Window.partition_by(snowparkdf[''sku_code''],snowparkdf[''osa_check_date''],coalesce(snowparkdf[''branch_code''],snowparkdf[''branch_code''])).order_by(snowparkdf[''week''],snowparkdf[''item_description''])))

            snowparkdf = snowparkdf.withColumn("acct_deliv_date",when(col(''acct_deliv_date'')==None,lit(None)).otherwise( regexp_replace(col("acct_deliv_date"), r"(\\d{2})(\\d{2})-(\\d{2})-(\\d{2}) (\\d{2}:\\d{2}:\\d{2})", r"\\3/\\4/\\2")))

            snowparkdf = snowparkdf.withColumn("OSA_CHECK_DATE",to_date(col("OSA_CHECK_DATE")))

            snowparkdf = snowparkdf.filter(col(''rn'') == 1)

            # extracting important columns
            snowdf = snowparkdf.select(
                    ''ret_nm_prefix'',
                     ''sku_code'',
                     ''brand'',
                     ''barcode'',
                     ''item_description'',
                     ''msl_large'',
                     ''msl_small'',
                     ''msl_premium'',
                     ''MONTH'',
                     ''week'',
                     ''reason'',
                     ''acct_deliv_date'',
                     ''osa_check_date'',
                     ''encoded_report'',
                     ''team_leader'',
                     ''branch_code'',
                     ''branch_code_original'',
                     ''branch_classification'',
                     ''branch_name'',
                     ''osa_flag'',
                     ''retailer_name'',
                     ''filename'',
                     ''run_id'',
                     ''crtd_dttm''
                    
        )

            # Load Data to the target table
            snowdf.write.mode("append").saveAsTable(target_table)

            current_date = datetime.now()
            formatted_year = current_date.strftime("%Y")
            formatted_month = current_date.strftime("%m")

             # write to success folder
            file_name = file_name.split(''.'')[0]+"_"+datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")

            

            snowdf.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True,file_format_name=''ph_non_pos_svi_smc_FORMAT'')

           
            return ''Success''
            
    except KeyError as key_error:
        # Handle KeyError (missing columns) here
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message
        
    except Exception as e:
        # Handle exceptions here
        error_message = f"Error: {str(e)}"
        return error_message;

           
            
   ------------------------------------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE PHLSDL_RAW.PH_NON_POS_SUPER_8_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('openpyxl==3.0.10','snowflake-snowpark-python==*','unidecode==1.2.0')
HANDLER = 'main'
EXECUTE AS OWNER
AS 'import snowflake.snowpark as snowpark
from snowflake import snowpark
from snowflake.snowpark.window import Window
from snowflake.snowpark.functions import col,lit,concat,regexp_replace,trim,split,rtrim,upper,coalesce,row_number,when,to_date,is_null
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField,DecimalType,DateType
from snowflake.snowpark.files import SnowflakeFile
import pandas as pd
import numpy as np
import openpyxl
from datetime import datetime
import pytz
from unidecode import unidecode
from re import sub

def main(session: snowpark.Session,Param):

    try:

        #Param=[''SUPER_8_20220624_20220624150012.xlsx'',''PHLSDL_RAW.DEV_LOAD_STAGE_ADLS'',''dev/non_pos/transaction/super_8/ph_non_pos_super_8/'',''sdl_ph_non_ise_super_8'']

        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        file_header_loc = 10

        #Reading data from adls
        full_path = "@"+stage_name+"/"+temp_stage_path+"/"+file_name
        
        with SnowflakeFile.open(full_path,''rb'',require_scoped_url= False) as f:
            
            df = pd.read_csv(f,delimiter='''')
            #print(df)

             # Converting the dataframe into  string  using applymap so that it can be decoded it using unidecode as some values are not ascii
            
            df = df.applymap(lambda x: str(x))

            df = df.applymap(lambda x : unidecode(x))

            #Replacing all nan with null
            df = df.applymap(lambda x: None if x == ''nan'' else x)
           

            #setting mode as index so values below it can be converted into columns after transpose

            df.columns = df.iloc[0]
            df1 = df.set_index(''WEEKLY STOCK AVAILABILITY MONITORING REPORT'')
            df2 = df1[:11].T
            df2.columns = [i.strip() for i in df2.columns]
            
           

            #extracting required columns after transposing 
            
            verticle_df = df2[8:][[''MONTH:'',''WEEK:'',''ACCOUNT: SUPER 8'',''Actual Delivery Date:'',''OSA Check Date'',''Encoded Report'',''TEAM LEADER:'',''BRANCH CODE:'',''Branch Size and Classification'',''SKU CODE'']]

           
                

            #As data for the horizontal columns which contain sku code branch barcode are after 10th row
            df2 = df1[file_header_loc:]
            print(df2)

            #replacing columns names with original and not taking 1st row as it contains col names
            df2.columns = df2.iloc[0]
            
            df2 = df2[1:]
            
            df2.reset_index(inplace=True)

            #seperating valvars and idvars so that we can unpivote it
            
            valvars = []
            
            for i in df2.columns[7:]:
                valvars.append(i)
                
            idvars = []
            idvars = [''MODE'',''BRAND'',''BARCODE'',''ITEM DESCRIPTION'',''MSL SUP HYBRID'']

            #print(valvars)

            
            

            #Converting columns for branch name and osa flag
            unpivoted_df = df2.melt(id_vars=idvars,value_vars=valvars,var_name=''branch_name'',value_name=''osa_flag'')

            unpivoted_df.rename(columns={''MODE'':''SKU CODE''},inplace=True)

            #Changing sku code to branch name so sa to join two tables9virticle df and 
            
            verticle_df.rename(columns={''SKU CODE'':''branch_name''},inplace=True)

            joined_df = unpivoted_df.merge(verticle_df,on=''branch_name'',how=''inner'')
            

            #dropping row where column value of ''SKU CODE is string or null''
            joined_df.dropna(subset=[''SKU CODE''],inplace=True)
            
            #joined_df = joined_df[joined_df[''SKU CODE''].apply(lambda x:isinstance(x,int))]
                    
    
            #Deleting values where month is null
            joined_df.dropna(subset=[''MONTH:''],inplace=True)

            joined_df[''Actual Delivery Date:'']=joined_df[''Actual Delivery Date:''].apply(lambda x :sub(r''([0-9]{2})([0-9]{2})-([0-9]{2})-([0-9]{2})'',r"\\3/\\4/\\2",x) if isinstance(x,str) else x)
            joined_df[''Actual Delivery Date:'']=joined_df[''Actual Delivery Date:''].apply(lambda x :x[:8] if isinstance(x,str) else x)

            #for debugging
            print(joined_df[''Actual Delivery Date:''])

            #Converting the dataframe into snowpark-dataframe
            snowparkdf = session.create_dataframe(joined_df)
            

            #return snowparkdf

            #Converting columns similar to table column names
            snowparkdf = snowparkdf.rename({col(''SKU CODE''):''sku_code'',col(''BRAND''):''brand'',col(''BARCODE''):''barcode'',\\
                                        col(''ITEM DESCRIPTION''):''item_description'',col(''MSL SUP HYBRID''):''msl_sup_hybrid'',\\
                                        col(''MONTH:''):''month'',col(''WEEK:''):''week'',col(''ACCOUNT: SUPER 8''):''reason'',\\
                                        col(''Actual Delivery Date:''):''acct_deliv_date'',col(''OSA Check Date''):''osa_check_date'',\\
                                        col(''Encoded Report''):''encoded_report'',col(''TEAM LEADER:''):''team_leader''\\
                                        ,col(''BRANCH CODE:''):''branch_code'',\\
                                        col(''Branch Size and Classification''):''branch_classification'',\\
                                        col(''"branch_name"''):''branch_name'',\\
                                         col(''"osa_flag"''):''osa_flag''})
            
            

            #Handling null values and empty rows

            snowparkdf = snowparkdf.na.drop(''all'')

            #Checking if dataframe is having any data
            
            if snowparkdf.count()==0:
                return "No Data in file"

           ##FUNCTIONS FOR TRANSFORMATION##

            
            #writing function for ret_nm_prefi
            def get_ret_nm_prefix(branch_code,branch_name):
                if (branch_code is not None or branch_code != ''''):
                    return regexp_replace(split(branch_code,lit(''-''))[0],''[a-z]'',''O'')
                else:
                    return regexp_replace(split(branch_code,lit('' ''))[0],'''','''') 

            #logic to extract branch_code
            def branchcode(branch_code):
                if branch_code  is not None or branch_code != '''':
                    return regexp_replace(lit(branch_code),''[A-Z-]*'','''')

            #retailer name extraction logic 
            def retailer_name(file_name):
                return rtrim(regexp_replace(split(lit(file_name),lit(''.''))[0],''[0-9]'',''''),lit(''_''))

            def filename_change(filename):
                filename1 = filename.split(''.'')
                filename1[1]=''xlsx''
                newfilename = ''.''.join(filename1)
                return newfilename
                
                    

            #----------------------------------------------------------------------------#
            #Adding columns that are needed for final table 
            
            snowparkdf = snowparkdf.withColumn("ret_nm_prefix", lit(''S8''))

            snowparkdf = snowparkdf.with_column("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))

            snowparkdf = snowparkdf.withColumn("crtd_dttm",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))

            snowparkdf = snowparkdf.withColumn(''retailer_name'',retailer_name(file_name))
            
            snowparkdf = snowparkdf.withColumn(''filename'',lit(filename_change(file_name)))
            
            snowparkdf = snowparkdf.withColumn(''branch_code_original'', lit(snowparkdf[''branch_code'']))
            
            snowparkdf = snowparkdf.withColumn(''branch_code'',lit(branchcode(snowparkdf["branch_code"])))
            
            snowparkdf = snowparkdf.withColumn(''rn'', row_number().over(Window.partition_by(snowparkdf[''sku_code''],snowparkdf[''osa_check_date''],coalesce(snowparkdf[''branch_code''],snowparkdf[''branch_code''])).order_by(snowparkdf[''week''],snowparkdf[''item_description''])))

            snowparkdf = snowparkdf.withColumn("acct_deliv_date",when(col(''acct_deliv_date'')==None,lit(None)).otherwise( regexp_replace(col("acct_deliv_date"), r"(\\d{2})(\\d{2})-(\\d{2})-(\\d{2}) (\\d{2}:\\d{2}:\\d{2})", r"\\3/\\4/\\2")))

            snowparkdf = snowparkdf.withColumn("OSA_CHECK_DATE",to_date(col("OSA_CHECK_DATE")))

            snowparkdf = snowparkdf.filter(col(''rn'') == 1)

            # extracting important columns
            snowdf = snowparkdf.select(
                    ''ret_nm_prefix'',
                     ''sku_code'',
                     ''brand'',
                     ''barcode'',
                     ''item_description'',
                     ''msl_sup_hybrid'',
                     ''MONTH'',
                     ''week'',
                     ''reason'',
                     ''acct_deliv_date'',
                     ''osa_check_date'',
                     ''encoded_report'',
                     ''team_leader'',
                     ''branch_code'',
                     ''branch_code_original'',
                     ''branch_classification'',
                     ''branch_name'',
                     ''osa_flag'',
                     ''retailer_name'',
                     ''filename'',
                     ''run_id'',
                     ''crtd_dttm''
                    
        )

            # Load Data to the target table
            snowdf.write.mode("append").saveAsTable(target_table)

            current_date = datetime.now()
            formatted_year = current_date.strftime("%Y")
            formatted_month = current_date.strftime("%m")

             # write to success folder
            file_name = file_name.split(''.'')[0]+"_"+datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")

            

            snowdf.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True,file_format_name=''ph_non_pos_svi_smc_FORMAT'')

           
            return ''Success''
            
    except KeyError as key_error:
        # Handle KeyError (missing columns) here
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message
        
    except Exception as e:
        # Handle exceptions here
        error_message = f"Error: {str(e)}"
        return error_message;

           
            
   ';
