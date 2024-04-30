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

            #----------------------------------------------------------------------------#
            #Adding columns that are needed for final table 
            
            snowparkdf = snowparkdf.withColumn("ret_nm_prefix", lit(''RS''))

            snowparkdf = snowparkdf.with_column("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))

            snowparkdf = snowparkdf.withColumn("crtd_dttm",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))

            snowparkdf = snowparkdf.withColumn(''retailer_name'',retailer_name(file_name))
            
            snowparkdf = snowparkdf.withColumn(''filename'',lit(file_name))
            
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
