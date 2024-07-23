use schema hcpsdl_raw;

create or replace stage HCPSDL_RAW.PROD_LOAD_STAGE_ADLS
 storage_integration = PROD_DNA_LOAD_AZURE39_SI
 url = 'azure://dlsadbplt001.blob.core.windows.net/hcp/';


CREATE OR REPLACE PROCEDURE HCP360_IN_IQVIA_AVEENO_BRAND_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('openpyxl==3.1.2','snowflake-snowpark-python==*')
HANDLER = 'main'
EXECUTE AS OWNER
AS 'import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col, lit
from snowflake.snowpark.files import SnowflakeFile
import pandas as pd
import openpyxl
from datetime import datetime
import pytz

def return_true_df(df1):
     # Sperating constant columns which we are not going melt afterward
            df_1 = df1[df1.columns[:4]]
            
            # Seperating 
            col_df_1 = [i for i in df_1]

            df_2 = df1[df1.columns[4:]]

            df_2_col = []

            for i in df_2.columns:
                if i.startswith("QTR"):
                    df_2_col.append(i)
                else:
                    pass
            df_2 = df_2[df_2_col]

            for i in range(len(df_2.columns)):
                df_2.iloc[0][df_2.columns[i]] = df_2.iloc[0][df_2.columns[i]]+df_2.columns[i][:11].replace(''.'','''')


            df_2.columns = df_2.iloc[0]


            df_2 = df_2[1:]

            rxn_col = []

            for i in df_2.columns:
                if i.startswith(''Rxn''):
                    rxn_col.append(i)


            df_Rxn = df_1
            df_Rxn.columns = df_Rxn.iloc[0]
            df_Rxn[rxn_col] = df_2[rxn_col]
            df_Rxn = df_Rxn[1:]
            df_Rxn = df_Rxn.map(lambda x: None if x==''nan'' else x)
            df_Rxn.dropna(how=''all'',inplace=True)

            df_Rxn_unpivoted  = pd.melt(df_Rxn,id_vars=df_Rxn.columns[:4], value_vars=df_Rxn.columns[4:],var_name = ''Date'',value_name = ''no_of_prescriptions'')
                
            df_Rxn_unpivoted.columns = [str(i) for i in df_Rxn_unpivoted.columns]

            df_Rxn_unpivoted[''Date''] = df_Rxn_unpivoted[''Date''].map(lambda x: str(x)[6:].strip('''') )


            rxer_col = []

            for i in df_2.columns:
                if i.startswith(''Rxer''):
                    rxer_col.append(i)
            

            df_1 = df1[df1.columns[:4]]
            df_Rxer = df_1
            df_Rxer.columns = df_Rxer.iloc[0]
            df_Rxer[rxer_col] = df_2[rxer_col]
            df_Rxer = df_Rxer[1:]
            df_Rxer = df_Rxer.map(lambda x: None if x==''nan'' else x)
            df_Rxer.dropna(how=''all'',inplace=True)


            df_Rxer_unpivoted  = pd.melt(df_Rxer,id_vars=df_Rxer.columns[:4], value_vars=df_Rxer.columns[4:],var_name = ''Date'',value_name = ''no_of_precribers'')
                
            df_Rxer_unpivoted.columns = [str(i) for i in df_Rxer_unpivoted.columns]

            df_Rxer_unpivoted[''Date''] = df_Rxer_unpivoted[''Date''].map(lambda x: str(x)[7:].strip('''') )


            df_final = df_Rxn_unpivoted.merge(df_Rxer_unpivoted, on =[''Zone'',''Product Description'', ''Pack Description'', ''Pack Volume'',''Date''],how=''inner'')

            # print(df_Rxn_unpivoted.columns)

            def get_date(dateted):
                new_date_1 = ''01/''+ dateted
                return ''-''.join(new_date_1.split(''/'')[::-1])

            df_final[''Date''] = df_final[''Date''].map(lambda x: get_date(x.replace('' '','''')))
            df_final.columns = [i.upper() for i in df_final.columns]
            df_final = df_final.applymap(lambda x : None if x== ''nan'' else x)

            return df_final
                
            

def main(session: snowpark.Session,Param):
    try:
        # Param = [''IQVIA_AVEENO_Brand_MAR2021.xlsx'', ''HCPSDL_RAW.DEV_LOAD_STAGE_ADLS'', ''dev/iqvia/transaction/aveeno_brand/'', ''SDL_HCP360_IN_IQVIA_AVEENO_BRAND'']

        file_name = Param[0]
        stage_name = Param[1]
        temp_stage_path = Param[2]
        target_table = Param[3]

        # Reading data from ADLS
        full_path = f"@{stage_name}/{temp_stage_path}/{file_name}"
        
        with SnowflakeFile.open(full_path, ''rb'', require_scoped_url=False) as f:
            excel_file = pd.ExcelFile(f)

            # Reading the sheets
            df1 = excel_file.parse(sheet_name=''Working_Baby'', engine=''openpyxl'')
            df2 = excel_file.parse(sheet_name=''Working_Body'', engine=''openpyxl'')

            # Converting all data to string
            df1 = df1.applymap(lambda x: str(x))
            df2 = df2.applymap(lambda x: str(x))

            # Ensuring the columns are the same in both DataFrames
            df1.columns = [str(i) for i in df1.columns]
            df2.columns = [str(i) for i in df2.columns]


            df_1 = return_true_df(df1)
            df_2 = return_true_df(df2)

            # Concatenating the DataFrames
            df3 = pd.concat([df_1, df_2], axis=0).drop_duplicates()

            df = session.create_dataframe(df3)
            
            df = df.na.drop("all")
        
        if df.count() == 0:
            return "No Data in file"

        df = df.filter(col(''ZONE'') != ''All India'')
        df = df.filter((col(''ZONE'').isNotNull()) & (col(''PACK DESCRIPTION'').isNotNull()) & (col(''PACK VOLUME'').isNotNull()))
        
        def filename_change(filename):
                filename1 = filename.split(''.'')
                filename1[1]=''csv''
                newfilename = ''.''.join(filename1)
                return newfilename

        
        df = df.with_column_renamed(''PRODUCT DESCRIPTION'',"Product_description")
        df = df.with_column_renamed(''PACK DESCRIPTION'',"Pack_description")
        df = df.with_column_renamed(''PACK VOLUME'',"Pack_value")

        df = df.withColumn("crt_dtm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        df = df.withColumn(''filename'',lit(filename_change(file_name)))

        snowdf = df.select(
            "Zone",     
            "Product_description", 
            "Pack_description", 
            "Pack_value", 
            "Date",
            "no_of_prescriptions",
            "no_of_precribers",
            "crt_dtm",
            "filename" 
        )
        
        file_name=file_name.split(".")[0]+"_"+ datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        
        # move to success
        snowdf.write.mode("append").saveAsTable(stage_name.split(".")[0] + "." + target_table)
        snowdf.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)

        return "Success" 

    except KeyError as key_error:
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message

    except pd.errors.MergeError as merge_error:
        error_message = f"DataFrame merging error: {str(merge_error)}"
        return error_message

    except Exception as e:
        error_message = f"Error: {str(e)}"
        return error_message';
CREATE OR REPLACE PROCEDURE HCP360_IN_IQVIA_AVEENO_SALES_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('openpyxl==3.1.2','snowflake-snowpark-python==*')
HANDLER = 'main'
EXECUTE AS OWNER
AS 'import snowflake.snowpark as snowpark
from snowflake import snowpark
from snowflake.snowpark.functions import col, lit, try_cast, to_date
from snowflake.snowpark.types import StringType, DateType, TimestampType,DecimalType
from snowflake.snowpark.files import SnowflakeFile
import pandas as pd
from datetime import datetime
import re


# Function to convert ''Total Units_Mar 2017'' to ''01/03/2017''
def convert_to_date(value):
    match = re.search(r''_(\\w{3}) (\\d{4})'', value)
    if match:
        # Handle format like ''Total Units_Mar 2017''
        month_str = match.group(1)
        year_str = match.group(2)
        month = pd.to_datetime(month_str, format=''%b'').month
        return f''{year_str}-{month:02d}-01''
    else:
        pass


def main(session: snowpark.Session, Param):
    try:
        # Param = [''IQVIA_AVEENO_Sales_Brand_FEB2022.xlsx'', ''HCPSDL_RAW.DEV_LOAD_STAGE_ADLS'', ''dev/iqvia/transaction/aveeno_sales'', ''sdl_hcp360_in_iqvia_aveeno_zone'',''[Aveeno Baby Base,Aveeno Body Base]'']
    
        file_name = Param[0]
        stage_name = Param[1]
        temp_stage_path = Param[2]
        target_table = Param[3]
        sheet_names = Param[4]
        
        # Reading data from adls
        full_path = f"@{stage_name}/{temp_stage_path}/{file_name}"

        with SnowflakeFile.open(full_path, ''rb'', require_scoped_url=False) as f:
    
            # reading pd.Excel so that we can parse and read 2nd sheet 
            excel_file = pd.ExcelFile(f)
    
            consol_df = pd.DataFrame()
            
            if sheet_names == "[]":
                files_to_read = [file_name]
            else:
                files_to_read = sheet_names[1:-1].split(",")
                files_to_read = [sh_name.strip("\\"").replace("(", "").replace(")", "") for sh_name in files_to_read]
    
            for sheet in files_to_read:
                df1 = excel_file.parse(sheet_name=sheet, dtype=str, engine=''openpyxl'')
                # concat first two rows to create a new row for colunmn header
                new_column_names = list(df1.iloc[0].fillna('''') + ''_'' + df1.columns)
                
                # Dropping the first row since it''s now used as column headers
                df1 = df1.drop(0).reset_index(drop=True)
                valid_cols = [''STATE'', ''ZONE'', ''PRODUCT'', ''PACK'']
                str_to_replace = [''Total Units'',''Values'']
                j = 0
                for i in valid_cols:
                    if j<4:
                        new_column_names[j] = i
                        j+=1
                
                df1.columns = new_column_names
                column1_name = new_column_names[0]
                # Drop rows where any column has NaN or blank values
                df_cleaned = df1.dropna(subset=[column1_name])
                in_valid_cols = [col for col in df_cleaned.columns if col not in valid_cols and not any(sub in col for sub in str_to_replace)]
                
                df_cleaned.drop(columns=in_valid_cols,inplace = True)
                unpivoted_df = pd.melt(df_cleaned, id_vars=valid_cols, var_name=''qtr'', value_name=''value'')

                source_filtered_df = unpivoted_df[
                    (unpivoted_df[''PACK''].notnull()) &
                    (~unpivoted_df[''qtr''].str.contains(''MAT'', na=False)) &
                    ((unpivoted_df[''qtr''].str.contains(''Total Units'')) | (unpivoted_df[''qtr''].str.contains(''Values''))) &
                    (unpivoted_df[''qtr''].notnull()) &
                    (unpivoted_df[''ZONE''] != ''All India'')
                ]
            
                source_filtered_df[''YEAR_MONTH''] = source_filtered_df[''qtr''].apply(convert_to_date)
    
                for substring in str_to_replace:
                    if substring in (''Total Units''):
                        # print(''here'')
                        source_filtered_df.loc[source_filtered_df[''qtr''].str.contains(substring), ''qtr''] = ''TOTAL_UNITS''
                    elif substring in (''Values''):
                        source_filtered_df.loc[source_filtered_df[''qtr''].str.contains(substring), ''qtr''] = ''VALUE''
    
                g_df = source_filtered_df.copy()
                
                g_df[''value''] = pd.to_numeric(g_df[''value''])
                gg_df = g_df.groupby([''STATE'', ''ZONE'', ''PRODUCT'', ''PACK'', ''YEAR_MONTH'',''qtr''],as_index=False).agg(''sum'')

                pivot_df = gg_df.pivot_table(index=[''STATE'', ''ZONE'', ''PRODUCT'', ''PACK'', ''YEAR_MONTH''], columns= ''qtr'', values=''value'',aggfunc=''sum'').reset_index()
                
                pivot_df[''DATA_SOURCE''] = sheet
                consol_df = pd.concat([consol_df,pivot_df])

    
            pre_final_df = session.create_dataframe(consol_df)
          
            pre_final_df = pre_final_df.withColumn("CRT_DTTM",lit(datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
            pre_final_df = pre_final_df.withColumn("FILE_NAME",lit(file_name).cast("string"))
           
            
            # return pre_final_df
            final_df = pre_final_df.select(\\
                try_cast(col(''STATE''), StringType()).alias(''STATE''),\\
                try_cast(col(''ZONE''), StringType()).alias(''ZONE''),\\
                try_cast(col(''PRODUCT''), StringType()).alias(''PRODUCT''),\\
                try_cast(col(''PACK''), StringType()).alias(''PACK''),\\
                try_cast(col(''YEAR_MONTH''), DateType()).alias(''YEAR_MONTH''),\\
                ''TOTAL_UNITS'',\\
                ''VALUE'',\\
                try_cast(col("CRT_DTTM"), TimestampType()).alias("CRT_DTTM"),\\
                "FILE_NAME",\\
                "DATA_SOURCE"\\
            )
            
            final_df.write.mode("append").saveAsTable(target_table)
        
                    
            return "Success"
    
    except KeyError as key_error:
        # Handle KeyError (missing columns) here
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message
        
    except Exception as e:
        # Handle exceptions here
        error_message = f"Error: {str(e)}" +str(df.columns)
        return error_message';
CREATE OR REPLACE PROCEDURE HCP360_IN_IQVIA_AVEENO_SPECIALITY_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('openpyxl==3.1.2','snowflake-snowpark-python==*')
HANDLER = 'main'
EXECUTE AS OWNER
AS 'import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col, lit
from snowflake.snowpark.files import SnowflakeFile
import pandas as pd
import openpyxl
from datetime import datetime
import pytz

def return_true_df(df1):
     # Sperating constant columns which we are not going melt afterward
            df_1 = df1[df1.columns[:5]]
            
            # Seperating 
            col_df_1 = [i for i in df_1]

            df_2 = df1[df1.columns[5:]]

            df_2_col = []

            for i in df_2.columns:
                if i.startswith("QTR"):
                    df_2_col.append(i)
                else:
                    pass
            df_2 = df_2[df_2_col]

            for i in range(len(df_2.columns)):
                df_2.iloc[0][df_2.columns[i]] = df_2.iloc[0][df_2.columns[i]]+df_2.columns[i][:11].replace(''.'','''')


            df_2.columns = df_2.iloc[0]


            df_2 = df_2[1:]

            rxn_col = []

            for i in df_2.columns:
                if i.startswith(''Rxn''):
                    rxn_col.append(i)


            df_Rxn = df_1
            df_Rxn.columns = df_Rxn.iloc[0]
            df_Rxn[rxn_col] = df_2[rxn_col]
            df_Rxn = df_Rxn[1:]
            df_Rxn = df_Rxn.map(lambda x: None if x==''nan'' else x)
            df_Rxn.dropna(how=''all'',inplace=True)

            df_Rxn_unpivoted  = pd.melt(df_Rxn,id_vars=df_Rxn.columns[:5], value_vars=df_Rxn.columns[5:],var_name = ''Date'',value_name = ''no_of_prescriptions'')
                
            df_Rxn_unpivoted.columns = [str(i) for i in df_Rxn_unpivoted.columns]

            df_Rxn_unpivoted[''Date''] = df_Rxn_unpivoted[''Date''].map(lambda x: str(x)[6:].strip('''') )


            rxer_col = []

            for i in df_2.columns:
                if i.startswith(''Rxer''):
                    rxer_col.append(i)
            

            df_1 = df1[df1.columns[:5]]
            df_Rxer = df_1
            df_Rxer.columns = df_Rxer.iloc[0]
            df_Rxer[rxer_col] = df_2[rxer_col]
            df_Rxer = df_Rxer[1:]
            df_Rxer = df_Rxer.map(lambda x: None if x==''nan'' else x)
            df_Rxer.dropna(how=''all'',inplace=True)


            df_Rxer_unpivoted  = pd.melt(df_Rxer,id_vars=df_Rxer.columns[:5], value_vars=df_Rxer.columns[5:],var_name = ''Date'',value_name = ''no_of_precribers'')
                
            df_Rxer_unpivoted.columns = [str(i) for i in df_Rxer_unpivoted.columns]

            df_Rxer_unpivoted[''Date''] = df_Rxer_unpivoted[''Date''].map(lambda x: str(x)[7:].strip('''') )


            df_final = df_Rxn_unpivoted.merge(df_Rxer_unpivoted, on =[''Zone'',''Product Description'', ''Pack Description'', ''Pack Volume'', ''Specialty'',''Date''],how=''inner'')

            # print(df_Rxn_unpivoted.columns)

            def get_date(dateted):
                new_date_1 = ''01/''+ dateted
                
                return ''-''.join(new_date_1.split(''/'')[::-1])

            df_final[''Date''] = df_final[''Date''].map(lambda x: get_date(x.replace('' '','''')))
            df_final.columns = [i.upper() for i in df_final.columns]
            df_final = df_final.applymap(lambda x : None if x== ''nan'' else x)

            return df_final
                
            

def main(session: snowpark.Session,Param):
    try:
        # Param = [''IQVIA_AVEENO_Specialty_SEPT2021.xlsx'', ''HCPSDL_RAW.DEV_LOAD_STAGE_ADLS'', ''dev/iqvia/transaction/aveeno_speciality/'', ''SDL_HCP360_IN_IQVIA_AVEENO_SPECIALITY'']

        file_name = Param[0]
        stage_name = Param[1]
        temp_stage_path = Param[2]
        target_table = Param[3]

        # Reading data from ADLS
        full_path = f"@{stage_name}/{temp_stage_path}/{file_name}"
        
        with SnowflakeFile.open(full_path, ''rb'', require_scoped_url=False) as f:
            excel_file = pd.ExcelFile(f)

            # Reading the sheets
            df1 = excel_file.parse(sheet_name=''Aveeno Baby Base'', engine=''openpyxl'')
            df2 = excel_file.parse(sheet_name=''Aveeno Body Base'', engine=''openpyxl'')

            # Converting all data to string
            df1 = df1.applymap(lambda x: str(x))
            df2 = df2.applymap(lambda x: str(x))

            # Ensuring the columns are the same in both DataFrames
            df1.columns = [str(i) for i in df1.columns]
            df2.columns = [str(i) for i in df2.columns]


            df_1 = return_true_df(df1)
            df_2 = return_true_df(df2)

            # Concatenating the DataFrames
            df3 = pd.concat([df_1, df_2], axis=0).drop_duplicates()

           
            df = session.create_dataframe(df3)
            
            df = df.na.drop("all")
        
        if df.count() == 0:
            return "No Data in file"

        df = df.filter(col(''ZONE'') != ''All India'')
        df = df.filter(col(''SPECIALTY'') != ''TOTAL'')
        df = df.filter((col(''ZONE'').isNotNull()) & (col(''PACK VOLUME'').isNotNull()))
        
        def filename_change(filename):
                filename1 = filename.split(''.'')
                filename1[1]=''csv''
                newfilename = ''.''.join(filename1)
                return newfilename

        
        df = df.with_column_renamed(''PRODUCT DESCRIPTION'',"Product_description")
        df = df.with_column_renamed(''PACK DESCRIPTION'',"Pack_description")
        df = df.with_column_renamed(''PACK VOLUME'',"Pack_value")

        df = df.withColumn("crt_dtm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        df = df.withColumn(''filename'',lit(filename_change(file_name)))

        snowdf = df.select(
            "Zone",     
            "Product_description", 
            "Pack_description", 
            "Pack_value",
            "Specialty",
            "Date",
            "no_of_prescriptions",
            "no_of_precribers",
            "crt_dtm",
            "filename" 
        )
        
        file_name=file_name.split(".")[0]+"_"+ datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        
        # move to success
        snowdf.write.mode("append").saveAsTable(stage_name.split(".")[0] + "." + target_table)
        snowdf.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)

        return "Success"

    except KeyError as key_error:
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message

    except pd.errors.MergeError as merge_error:
        error_message = f"DataFrame merging error: {str(merge_error)}"
        return error_message

    except Exception as e:
        error_message = f"Error: {str(e)}"
        return error_message';
CREATE OR REPLACE PROCEDURE HCP360_IN_IQVIA_BRAND_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('openpyxl==3.1.2','snowflake-snowpark-python==*')
HANDLER = 'main'
EXECUTE AS OWNER
AS 'import snowflake.snowpark as snowpark
from snowflake import snowpark
from snowflake.snowpark.window import Window
from snowflake.snowpark.functions import col, lit, concat, regexp_replace, trim, split, rtrim, upper, coalesce, row_number, when, to_date, is_null
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DecimalType, DateType
from snowflake.snowpark.files import SnowflakeFile
import pandas as pd
import numpy as np
import openpyxl
from datetime import datetime
import pytz
from re import sub

def main(session: snowpark.Session,Param):
    try:
        # Param = [''IQVIA_ORSL_Brand_SEPT2023.xlsx'', ''HCPSDL_RAW.DEV_LOAD_STAGE_ADLS'', ''dev/iqvia/transaction/brand'', ''SDL_HCP360_IN_IQVIA_BRAND'']

        file_name = Param[0]
        stage_name = Param[1]
        temp_stage_path = Param[2]
        target_table = Param[3]
        
# Reading data from adls
        full_path = f"@{stage_name}/{temp_stage_path}/{file_name}"
        

        with SnowflakeFile.open(full_path, ''rb'', require_scoped_url=False) as f:

            # reading pd.Excel so that we can parse and read 2nd sheet 
            excel_file = pd.ExcelFile(f)
            
            # Reading 2nd sheet ''Working'' from the file 
            df1 = excel_file.parse(sheet_name=''Working'',engine=''openpyxl'')

            #Converting everything into string for uniformity of datatype
            df1.columns = [str(i) for i in df1.columns]
            df1 = df1.applymap(lambda x: str(x))
            
            # Sperating constant columns which we are not going melt afterward
            df_1 = df1[df1.columns[:5]]
            
            # Seperating 
            col_df_1 = [i for i in df_1]

            df_2 = df1[df1.columns[5:]]

            df_2_col = []

            for i in df_2.columns:
                if i.startswith("QTR"):
                    df_2_col.append(i)
                else:
                    pass
            df_2 = df_2[df_2_col]

            for i in range(len(df_2.columns)):
                df_2.iloc[0][df_2.columns[i]] = df_2.iloc[0][df_2.columns[i]]+df_2.columns[i][:11].replace(''.'','''')


            df_2.columns = df_2.iloc[0]


            df_2 = df_2[1:]

            rxn_col = []

            for i in df_2.columns:
                if i.startswith(''Rxn''):
                    rxn_col.append(i)


            df_Rxn = df_1
            df_Rxn.columns = df_Rxn.iloc[0]
            df_Rxn[rxn_col] = df_2[rxn_col]
            df_Rxn = df_Rxn[1:]
            df_Rxn = df_Rxn.map(lambda x: None if x==''nan'' else x)
            df_Rxn.dropna(how=''all'',inplace=True)

            df_Rxn_unpivoted  = pd.melt(df_Rxn,id_vars=df_Rxn.columns[:5], value_vars=df_Rxn.columns[5:],var_name = ''Date'',value_name = ''no_of_prescribtions'')
                
            df_Rxn_unpivoted.columns = [str(i) for i in df_Rxn_unpivoted.columns]

            df_Rxn_unpivoted[''Date''] = df_Rxn_unpivoted[''Date''].map(lambda x: str(x)[6:].strip('''') )


            rxer_col = []

            for i in df_2.columns:
                if i.startswith(''Rxer''):
                    rxer_col.append(i)
            

            df_1 = df1[df1.columns[:5]]
            df_Rxer = df_1
            df_Rxer.columns = df_Rxer.iloc[0]
            df_Rxer[rxer_col] = df_2[rxer_col]
            df_Rxer = df_Rxer[1:]
            df_Rxer = df_Rxer.map(lambda x: None if x==''nan'' else x)
            df_Rxer.dropna(how=''all'',inplace=True)


            df_Rxer_unpivoted  = pd.melt(df_Rxer,id_vars=df_Rxer.columns[:5], value_vars=df_Rxer.columns[5:],var_name = ''Date'',value_name = ''no_of_precribers'')
                
            df_Rxer_unpivoted.columns = [str(i) for i in df_Rxer_unpivoted.columns]

            df_Rxer_unpivoted[''Date''] = df_Rxer_unpivoted[''Date''].map(lambda x: str(x)[7:].strip('''') )


            df_final = df_Rxn_unpivoted.merge(df_Rxer_unpivoted, on =[''Product Description'', ''Brand'', ''Pack Description'', ''Category'', ''Zone'',''Date''],how=''inner'')

            # print(df_Rxn_unpivoted.columns)

            def get_date(dateted):
                new_date_1 = ''01/''+dateted
                return ''-''.join(new_date_1.split(''/'')[::-1])

            df_final[''Date''] = df_final[''Date''].map(lambda x: get_date(x.replace('' '','''')))
            df_final.columns = [i.upper() for i in df_final.columns]
            df_final = df_final.applymap(lambda x : None if x== ''nan'' else x)
                
            df = session.create_dataframe(df_final)
        

        df = df.na.drop("all")
        
        if df.count() == 0:
            return "No Data in file"

        df = df.filter((col(''ZONE'') != ''All India'') | col(''ZONE'').isNull())       
        df = df.filter((col(''Category'') != ''TOTAL'') | col(''Category'').isNull())
        def filename_change(filename):
                filename1 = filename.split(''.'')
                filename1[1]=''csv''
                newfilename = ''.''.join(filename1)
                return newfilename

        
        df = df.with_column_renamed(''PRODUCT DESCRIPTION'',"Product_description")
        df = df.with_column_renamed(''PACK DESCRIPTION'',"Pack_description")

        df = df.withColumn("crt_dtm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        df = df.withColumn(''filename'',lit(filename_change(file_name)))

        snowdf = df.select(
            "Product_description",     
            "brand", 
            "Pack_description", 
            "Category", 
            "Zone", 
            "Date",
            "no_of_prescribtions",
            "no_of_precribers",
            "crt_dtm",
            "filename" 
    
        )
        
        file_name=file_name.split(".")[0]+"_"+ datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        
        # # #move to success
        snowdf.write.mode("append").saveAsTable(stage_name.split(".")[0] + "." + target_table)
        snowdf.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)

        return "Success"  

    except KeyError as key_error:
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message

    except pd.errors.MergeError as merge_error:
        error_message = f"DataFrame merging error: {str(merge_error)}"
        return error_message

    except Exception as e:
        error_message = f"Error: {str(e)}"
        return error_message';
CREATE OR REPLACE PROCEDURE HCP360_IN_IQVIA_INDICATION_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('openpyxl==3.1.2','snowflake-snowpark-python==*')
HANDLER = 'main'
EXECUTE AS OWNER
AS 'import snowflake.snowpark as snowpark
from snowflake import snowpark
from snowflake.snowpark.window import Window
from snowflake.snowpark.functions import col, lit, concat, regexp_replace, trim, split, rtrim, upper, coalesce, row_number, when, to_date, is_null
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DecimalType, DateType
from snowflake.snowpark.files import SnowflakeFile
import pandas as pd
import numpy as np
import openpyxl
from datetime import datetime
import pytz
from re import sub

def main(session: snowpark.Session,Param):
    try:
        # Param = [''IQVIA_ORSL_Indication_SEPT2023.xlsx'', ''HCPSDL_RAW.DEV_LOAD_STAGE_ADLS'', ''dev/iqvia/transaction/indication'', ''SDL_HCP360_IN_IQVIA_INDICATION'']

        file_name = Param[0]
        stage_name = Param[1]
        temp_stage_path = Param[2]
        target_table = Param[3]
        
        # Reading data from adls
        full_path = f"@{stage_name}/{temp_stage_path}/{file_name}"
        

        with SnowflakeFile.open(full_path, ''rb'', require_scoped_url=False) as f:

            # reading pd.Excel so that we can parse and read 2nd sheet 
            excel_file = pd.ExcelFile(f)
            
            # Reading 2nd sheet from the file 
            df1 = excel_file.parse(sheet_name=''Indication Zone'',engine=''openpyxl'')

            #Converting everything into string for uniformity of datatype
            df1.columns = [str(i) for i in df1.columns]
            df1 = df1.applymap(lambda x: str(x))
            
            # Sperating constant columns which we are not going melt afterward
            df_1 = df1[df1.columns[:4]]
            
            # Seperating 
            col_df_1 = [i for i in df_1]

            df_2 = df1[df1.columns[4:]]

            df_2_col = []

            for i in df_2.columns:
                if i.startswith("QTR"):
                    df_2_col.append(i)
                else:
                    pass
            df_2 = df_2[df_2_col]

            for i in range(len(df_2.columns)):
                df_2.iloc[0][df_2.columns[i]] = df_2.iloc[0][df_2.columns[i]]+df_2.columns[i][:11].replace(''.'','''')


            df_2.columns = df_2.iloc[0]

            df_2 = df_2[1:]

            rxn_col = []

            for i in df_2.columns:
                if i.startswith(''Rxn''):
                    rxn_col.append(i)


            df_Rxn = df_1
            df_Rxn.columns = df_Rxn.iloc[0]
            df_Rxn[rxn_col] = df_2[rxn_col]
            df_Rxn = df_Rxn[1:]
            df_Rxn = df_Rxn.map(lambda x: None if x==''nan'' else x)
            df_Rxn.dropna(how=''all'',inplace=True)
            
            df_Rxn_unpivoted  = pd.melt(df_Rxn,id_vars=df_Rxn.columns[:4], value_vars=df_Rxn.columns[4:],var_name = ''Date'',value_name = ''no_of_prescribtions'')
                

            df_Rxn_unpivoted.columns = [str(i) for i in df_Rxn_unpivoted.columns]

            df_Rxn_unpivoted[''Date''] = df_Rxn_unpivoted[''Date''].map(lambda x: str(x)[6:].strip('''') )

            rxer_col = []

            for i in df_2.columns:
                if i.startswith(''Rxer''):
                    rxer_col.append(i)
            

            df_1 = df1[df1.columns[:4]]
            df_Rxer = df_1
            df_Rxer.columns = df_Rxer.iloc[0]
            df_Rxer[rxer_col] = df_2[rxer_col]
            df_Rxer = df_Rxer[1:]
            df_Rxer = df_Rxer.map(lambda x: None if x==''nan'' else x)
            df_Rxer.dropna(how=''all'',inplace=True)


            df_Rxer_unpivoted  = pd.melt(df_Rxer,id_vars=df_Rxer.columns[:4], value_vars=df_Rxer.columns[4:],var_name = ''Date'',value_name = ''no_of_precribers'')
                
            df_Rxer_unpivoted.columns = [str(i) for i in df_Rxer_unpivoted.columns]

            df_Rxer_unpivoted[''Date''] = df_Rxer_unpivoted[''Date''].map(lambda x: str(x)[7:].strip('''') )
            
            df_final = df_Rxn_unpivoted.merge(df_Rxer_unpivoted, on =[''Zone'',''Product Description'',''Brand'',''Diagnosis_Level_2'',''Date''],how=''inner'')
            df_final.columns = [i.upper() for i in df_final.columns]

            def get_date(dateted):
                new_date_1 = ''01/''+dateted
                return ''-''.join(new_date_1.split(''/'')[::-1])

            df_final[''DATE''] = df_final[''DATE''].map(lambda x: get_date(x.replace('' '','''')))
            df_final.dropna(subset=[''PRODUCT DESCRIPTION''],inplace = True)
            
            df = session.create_dataframe(df_final)
        

        df = df.na.drop("all")
        
        if df.count() == 0:
            return "No Data in file"

        def filename_change(filename):
                filename1 = filename.split(''.'')
                filename1[1]=''csv''
                newfilename = ''.''.join(filename1)
                return newfilename

        df = df.filter((col(''ZONE'') != ''All India'') | col(''ZONE'').isNull())        
        df = df.filter((col(''Diagnosis_Level_2'') != ''TOTAL'') | col(''Diagnosis_Level_2'').isNull())


        df = df.with_column_renamed(''PRODUCT DESCRIPTION'',"Product_description")

        df = df.withColumn("crt_dtm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        df = df.withColumn(''filename'',lit(filename_change(file_name)))
        
        snowdf = df.select(
            "Zone",
            "Product_description",     
            "brand", 
            "Diagnosis_Level_2", 
            "Date",
            "no_of_prescribtions",
            "no_of_precribers",
            "crt_dtm",
            "filename" 
        )
        
        file_name=file_name.split(".")[0]+"_"+ datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        
        # #move to success
        snowdf.write.mode("append").saveAsTable(stage_name.split(".")[0] + "." + target_table)
        snowdf.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)

        return "Success"

    except KeyError as key_error:
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message

    except pd.errors.MergeError as merge_error:
        error_message = f"DataFrame merging error: {str(merge_error)}"
        return error_message

    except Exception as e:
        error_message = f"Error: {str(e)}"
        return error_message';
CREATE OR REPLACE PROCEDURE HCP360_IN_IQVIA_SALES_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('openpyxl==3.1.2','snowflake-snowpark-python==*')
HANDLER = 'main'
EXECUTE AS OWNER
AS 'import snowflake.snowpark as snowpark
from snowflake import snowpark
from snowflake.snowpark.window import Window
from snowflake.snowpark.functions import col, lit, concat, regexp_replace, trim, split, rtrim, upper, coalesce, row_number, when, to_date, is_null
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DecimalType, DateType
from snowflake.snowpark.files import SnowflakeFile
import calendar
import pandas as pd
import numpy as np
import openpyxl
from datetime import datetime
import pytz
from re import sub

def main(session: snowpark.Session,Param):
    try:
        # Param = [''IQVIA_ORSL_Sales_Brand_OCT2023.xlsx'', ''HCPSDL_RAW.DEV_LOAD_STAGE_ADLS'', ''dev/iqvia/transaction/sales'', ''SDL_HCP360_IN_IQVIA_SALES'']

        file_name = Param[0]
        stage_name = Param[1]
        temp_stage_path = Param[2]
        target_table = Param[3]
        
        # Reading data from adls
        full_path = f"@{stage_name}/{temp_stage_path}/{file_name}"
        

        with SnowflakeFile.open(full_path, ''rb'', require_scoped_url=False) as f:

            # reading pd.Excel so that we can parse and read 2nd sheet 
            excel_file = pd.ExcelFile(f)
            
            # Reading 2nd sheet from the file 
            df1 = excel_file.parse(sheet_name=''Working'',engine=''openpyxl'')

            #Converting everything into string for uniformity of datatype
            df1.columns = [str(i) for i in df1.columns]
            df1 = df1.applymap(lambda x: str(x))
            
            # Sperating constant columns which we are not going melt afterward
            df_1 = df1[df1.columns[:6]]
            
            # Seperating 
            col_df_1 = [i for i in df_1]

            df_2 = df1[df1.columns[6:]]

            df_2_col = []

            for i in df_2.columns:
                if not i.startswith(" MAT"):
                    df_2_col.append(i)
                else:
                    pass
            df_2 = df_2[df_2_col]

            for i in range(len(df_2.columns)):
                df_2.iloc[0][df_2.columns[i]] = df_2.iloc[0][df_2.columns[i]]+df_2.columns[i][:11].replace(''.'','''')

            df_3_col = []

            df_2.columns = df_2.iloc[0]
            for i in df_2.columns:
                if not i.startswith("nan"):
                    df_3_col.append(i)

            df_2 = df_2[df_3_col]
            df_2 = df_2[1:]

            rxn_col = []

            for i in df_2.columns:
                if i.startswith(''Total Units''):
                    rxn_col.append(i)

            df_Rxn = df_1
            df_Rxn.columns = df_Rxn.iloc[0]
            df_Rxn[rxn_col] = df_2[rxn_col]
            df_Rxn = df_Rxn[1:]
            df_Rxn = df_Rxn.map(lambda x:x.strip('' ''))
            df_Rxn = df_Rxn.map(lambda x: None if x==''nan'' else x)
            df_Rxn.dropna(how=''all'',inplace=True)
            
            
            df_Rxn_unpivoted  = pd.melt(df_Rxn,id_vars=df_Rxn.columns[:6], value_vars=df_Rxn.columns[6:],var_name = ''Date'',value_name = ''Total_Units'')
                

            df_Rxn_unpivoted.columns = [str(i) for i in df_Rxn_unpivoted.columns]

            # df_Rxn_unpivoted[''Date''] = df_Rxn_unpivoted[''Date''].map(lambda x: str(x)[6:].strip('''') )
            df_Rxn_unpivoted.dropna(subset=[''Product''],inplace = True)
            def date_filter(date_name):
                return date_name[11:]
            df_Rxn_unpivoted[''Date''] = df_Rxn_unpivoted[''Date''].map(lambda x:date_filter(x))
            df_Rxn_unpivoted[''Date''] = df_Rxn_unpivoted[''Date''].map(lambda x:x.strip('' ''))
            df_Rxn_unpivoted[''nos''] =  [i for i in range(len(df_Rxn_unpivoted[''Date'']))]

            rxer_col = []

            for i in df_2.columns:
                if i.startswith(''Values''):
                    rxer_col.append(i)
            

            df_1 = df1[df1.columns[:6]]
            df_Rxer = df_1
            df_Rxer.columns = df_Rxer.iloc[0]
            df_Rxer[rxer_col] = df_2[rxer_col]
            df_Rxer = df_Rxer[1:]
            df_Rxer = df_Rxer.map(lambda x:x.strip('' ''))
            df_Rxer = df_Rxer.map(lambda x: None if x==''nan'' else x)
            df_Rxer.dropna(how=''all'',inplace=True)
            

            df_Rxer_unpivoted  = pd.melt(df_Rxer,id_vars=df_Rxer.columns[:6], value_vars=df_Rxer.columns[6:],var_name = ''Date'',value_name = ''Values'')

            def date_filter1(date_name):
                return date_name[6:-1]

            df_Rxer_unpivoted[''Date''] = df_Rxer_unpivoted[''Date''].map(lambda x:date_filter1(x))
            df_Rxer_unpivoted[''Date''] = df_Rxer_unpivoted[''Date''].map(lambda x:x.strip('' ''))

            df_Rxer_unpivoted.columns = [str(i) for i in df_Rxer_unpivoted.columns]

            df_Rxer_unpivoted.dropna(subset=[''Product''],inplace = True)

            df_Rxer_unpivoted[''nos''] =  [i for i in range(len(df_Rxer_unpivoted[''Date'']))]

            
            df_final = df_Rxn_unpivoted.merge(df_Rxer_unpivoted, on =[''State'',''Region'',''Product'',''Product Grp'',''Pack'',''Cateogry'',''Date'',''nos''],how=''inner'')
            df_final.columns = [i.upper() for i in df_final.columns]

            def get_date(new_date):
                k = new_date.split('' '')
                month = list(calendar.month_abbr).index(k[0])
                month_str = f''{month:02d}''  # Zero-padding the month
                return f''{k[1]}-{month_str}-01''
                
            df_final[''DATE''] = df_final[''DATE''].apply(lambda x: get_date(x))
        
            
            df = session.create_dataframe(df_final)
            

        df = df.na.drop("all")
        
        if df.count() == 0:
            return "No Data in file"

        def filename_change(filename):
                filename1 = filename.split(''.'')
                filename1[1]=''csv''
                newfilename = ''.''.join(filename1)
                return newfilename

        df = df.filter(col(''State'') != ''All India'')


        df = df.with_column_renamed(''PRODUCT GRP'',"Product_Grp")

        df = df.withColumn("crt_dtm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        df = df.withColumn(''filename'',lit(filename_change(file_name)))
        
        snowdf = df.select(
            "State",
            "Region",     
            "Product", 
            "Product_Grp",
            "Pack",
            "Cateogry",
            "Date",
            "Total_Units",
            "Values",
            "crt_dtm",
            "filename" 
        )
        
        file_name=file_name.split(".")[0]+"_"+ datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        
        # #move to success
        snowdf.write.mode("append").saveAsTable(stage_name.split(".")[0] + "." + target_table)
        snowdf.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)

        return "Success"

    except KeyError as key_error:
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message

    except pd.errors.MergeError as merge_error:
        error_message = f"DataFrame merging error: {str(merge_error)}"
        return error_message

    except Exception as e:
        error_message = f"Error: {str(e)}"
        return error_message';
CREATE OR REPLACE PROCEDURE HCP360_IN_IQVIA_SPECIALITY_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('openpyxl==3.1.2','snowflake-snowpark-python==*')
HANDLER = 'main'
EXECUTE AS OWNER
AS 'import snowflake.snowpark as snowpark
from snowflake import snowpark
from snowflake.snowpark.window import Window
from snowflake.snowpark.functions import col, lit, concat, regexp_replace, trim, split, rtrim, upper, coalesce, row_number, when, to_date, is_null
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DecimalType, DateType
from snowflake.snowpark.files import SnowflakeFile
import pandas as pd
import numpy as np
import openpyxl
from datetime import datetime
import pytz
from re import sub

def main(session: snowpark.Session,Param):
    try:
        # Param = [''IQVIA_ORSL_Specialty_SEPT2023.xlsx'', ''HCPSDL_RAW.DEV_LOAD_STAGE_ADLS'', ''dev/iqvia/transaction/speciality'', ''SDL_HCP360_IN_IQVIA_SPECIALITY'']

        file_name = Param[0]
        stage_name = Param[1]
        temp_stage_path = Param[2]
        target_table = Param[3]
        
# Reading data from adls
        full_path = f"@{stage_name}/{temp_stage_path}/{file_name}"
        

        with SnowflakeFile.open(full_path, ''rb'', require_scoped_url=False) as f:

            # reading pd.Excel so that we can parse and read 2nd sheet 
            excel_file = pd.ExcelFile(f)
            
            # Reading 2nd sheet from the file 
            df1 = excel_file.parse(sheet_name=''Specialty Base'',engine=''openpyxl'')

            #Converting everything into string for uniformity of datatype
            df1.columns = [str(i) for i in df1.columns]
            df1 = df1.applymap(lambda x: str(x))
            
            # Sperating constant columns which we are not going melt afterward
            df_1 = df1[df1.columns[:4]]
            
            # Seperating 
            col_df_1 = [i for i in df_1]

            df_2 = df1[df1.columns[4:]]

            df_2_col = []

            for i in df_2.columns:
                if i.startswith("QTR"):
                    df_2_col.append(i)
                else:
                    pass
            df_2 = df_2[df_2_col]

            for i in range(len(df_2.columns)):
                df_2.iloc[0][df_2.columns[i]] = df_2.iloc[0][df_2.columns[i]]+df_2.columns[i][:11].replace(''.'','''')


            df_2.columns = df_2.iloc[0]


            df_2 = df_2[1:]

            rxn_col = []

            for i in df_2.columns:
                if i.startswith(''Rxn''):
                    rxn_col.append(i)


            df_Rxn = df_1
            df_Rxn.columns = df_Rxn.iloc[0]
            df_Rxn[rxn_col] = df_2[rxn_col]
            df_Rxn = df_Rxn[1:]
            df_Rxn = df_Rxn.map(lambda x: None if x==''nan'' else x)
            df_Rxn.dropna(how=''all'',inplace=True)
            
            df_Rxn_unpivoted  = pd.melt(df_Rxn,id_vars=df_Rxn.columns[:4], value_vars=df_Rxn.columns[4:],var_name = ''Date'',value_name = ''no_of_prescribtions'')
                
            df_Rxn_unpivoted.columns = [str(i) for i in df_Rxn_unpivoted.columns]

            df_Rxn_unpivoted[''Date''] = df_Rxn_unpivoted[''Date''].map(lambda x: str(x)[6:].strip('''') )

            rxer_col = []

            for i in df_2.columns:
                if i.startswith(''Rxer''):
                    rxer_col.append(i)
            

            df_1 = df1[df1.columns[:4]]
            df_Rxer = df_1
            df_Rxer.columns = df_Rxer.iloc[0]
            df_Rxer[rxer_col] = df_2[rxer_col]
            df_Rxer = df_Rxer[1:]
            df_Rxer = df_Rxer.map(lambda x: None if x==''nan'' else x)
            df_Rxer.dropna(how=''all'',inplace=True)


            df_Rxer_unpivoted  = pd.melt(df_Rxer,id_vars=df_Rxer.columns[:4], value_vars=df_Rxer.columns[4:],var_name = ''Date'',value_name = ''no_of_precribers'')
                
            df_Rxer_unpivoted.columns = [str(i) for i in df_Rxer_unpivoted.columns]

            df_Rxer_unpivoted[''Date''] = df_Rxer_unpivoted[''Date''].map(lambda x: str(x)[7:].strip('''') )
            
            df_final = df_Rxn_unpivoted.merge(df_Rxer_unpivoted, on =[''Zone'',''Product Description'',''Brand'',''Specialty'',''Date''],how=''inner'')
            df_final.columns = [i.upper() for i in df_final.columns]

            print(df_final.columns)

            def get_date(dateted):
                new_date_1 = ''01/''+dateted
                return ''-''.join(new_date_1.split(''/'')[::-1])

            df_final[''DATE''] = df_final[''DATE''].map(lambda x: get_date(x.replace('' '','''')))
            df_final.dropna(subset=[''PRODUCT DESCRIPTION''],inplace = True)
            
            df = session.create_dataframe(df_final)
        

        df = df.na.drop("all")
        
        if df.count() == 0:
            return "No Data in file"

        def filename_change(filename):
                filename1 = filename.split(''.'')
                filename1[1]=''csv''
                newfilename = ''.''.join(filename1)
                return newfilename

        df = df.filter((col(''ZONE'') != ''All India'') | col(''ZONE'').isNull())        
        df = df.filter((col(''Specialty'') != ''TOTAL'') | col(''Specialty'').isNull()) 

        df = df.with_column_renamed(''PRODUCT DESCRIPTION'',"Product_description")

        df = df.withColumn("crt_dtm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        df = df.withColumn(''filename'',lit(filename_change(file_name)))
        
        snowdf = df.select(
            "Zone",
            "Product_description",     
            "brand", 
            "Specialty", 
            "Date",
            "no_of_prescribtions",
            "no_of_precribers",
            "crt_dtm",
            "filename" 
        )
        
        file_name=file_name.split(".")[0]+"_"+ datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        
        # #move to success
        snowdf.write.mode("append").saveAsTable(stage_name.split(".")[0] + "." + target_table)
        snowdf.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)

        return "Success"

    except KeyError as key_error:
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message

    except pd.errors.MergeError as merge_error:
        error_message = f"DataFrame merging error: {str(merge_error)}"
        return error_message

    except Exception as e:
        error_message = f"Error: {str(e)}"
        return error_message';
CREATE OR REPLACE PROCEDURE HCP360_IN_SFMC_LOAD_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '
from snowflake.snowpark.functions import col, lit, date_format, current_timestamp, to_date, year, month, concat, format_number, regexp_replace,to_timestamp,when,trim,upper
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DecimalType,DateType
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz


def main(session: snowpark.Session,Param):
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        sch_name        = stage_name.split(''.'')[0]
        target_table    = sch_name+"."+Param[3]
       
        df = session.read\\
            .option("INFER_SCHEMA", True)\\
            .option("skip_header",0)\\
            .option("field_delimiter", "|")\\
            .option("field_optionally_enclosed_by", "\\"") \\
            .option("encoding","UTF-16LE") \\
            .option("trim_space", True) \\
            .with_metadata("METADATA$FILE_ROW_NUMBER") \\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        df = df.filter(col("METADATA$FILE_ROW_NUMBER") > lit(1))
        
        df = df.drop("METADATA$FILE_ROW_NUMBER")

        df = df.withColumn("CRT_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        
        final_df = df

        if final_df.count()==0:
            return "No Data in file"
        
        # Load Data to the target table
        final_df.write.mode("append").saveAsTable(target_table)

        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")

        # write to success folder
    
        file_name=file_name.split(".")[0] + ''_'' + datetime.now().strftime("%Y%m%d%H%M%S")
        final_df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,file_format_type="csv",OVERWRITE=True,header=True)
   
        return "Success"
        
    except KeyError as key_error:
        
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message
        
    except Exception as e:
        
        error_message = f"Error: {str(e)}" +str(df.columns)
        return error_message
';
CREATE OR REPLACE PROCEDURE HCP360_IN_VENTASYS_HCP_MASTER_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '
from snowflake.snowpark.functions import col, lit, date_format, current_timestamp, to_date, year, month, concat, format_number, regexp_replace,to_timestamp,when,trim,upper
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DecimalType,DateType
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz
import pandas as pd
import zipfile
from snowflake.snowpark.files import SnowflakeFile


def main(session: snowpark.Session,Param):
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        sch_name        = stage_name.split(''.'')[0]
        target_table    = sch_name+"."+Param[3]

        full_path = "@"+stage_name+"/"+temp_stage_path+"/"+file_name
        pdf = pd.DataFrame()
        with SnowflakeFile.open(full_path,''rb'',require_scoped_url=False) as fhandle:
            with zipfile.ZipFile(fhandle,"r") as zip_ref:
            # Iterate over each file in the zip archive
                for i_file_name in zip_ref.namelist():
                    with zip_ref.open(i_file_name) as file:
                        source_df = pd.read_csv(file, delimiter="~", header =0, encoding_errors = "replace", quoting=3, na_filter=False)
                        for column in source_df.columns:
                            source_df[column] = source_df[column].apply(str)
                        pdf = pd.concat([pdf, source_df])
                       
        df= session.create_dataframe(pdf)

        df = df.withColumn("CRT_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        df = df.withColumn("FILE_NAME",lit(file_name).cast("string"))

        df = df.withColumn("CONSENTFLAG", col(''"Consent_Flag"''))
        df = df.withColumn("CONSENTUPDATETIME", when(col(''"Consent_Update_Datetime"'') == lit(""), lit(None)).otherwise(col(''"Consent_Update_Datetime"'')))
        df = df.withColumn("FIRSTRXDATE", when(col("FIRST_RX_DATE") == lit(""), lit(None)).otherwise(col("FIRST_RX_DATE")))
        df = df.withColumn("CUSTENDTEREDDATE", when(col("CUST_ENDTERED_DATE") == lit(""), lit(None)).otherwise(col("CUST_ENDTERED_DATE")))
        
        final_df = df.select(
            "TEAM_NAME", "V_CUSTID", "V_TERRID", "CUST_NAME", "CUST_TYPE", "CUST_QUAL", "CUST_SPEC", "CORE_NONCORE", "CLASSIFICATION",\\
            "IS_FBM_ADOPTED", "VISITS_PER_MONTH", "CELL_PHONE", "PHONE", "EMAIL", "CITY", "STATE", "IS_ACTIVE", "FIRSTRXDATE", \\
            "CRT_DTTM", "FILE_NAME", "CUSTENDTEREDDATE", "CONSENTFLAG", \\
            to_timestamp("CONSENTUPDATETIME", lit("MM/DD/YYYY HH12:MI:SS AM")).as_("CONSENTUPDATETIME"), \\

        ).alias("final_df")

        if final_df.count()==0:
            return "No Data in file"

        
        file_df = df.select(
            "TEAM_NAME", "V_CUSTID", "V_TERRID", "CUST_NAME", "CUST_TYPE", "CUST_QUAL", "CUST_SPEC", "CORE_NONCORE", "CLASSIFICATION",\\
            "IS_FBM_ADOPTED", "VISITS_PER_MONTH", "CELL_PHONE", "PHONE", "EMAIL", "CITY", "STATE", "IS_ACTIVE", "FIRST_RX_DATE", \\
            "CRT_DTTM", "FILE_NAME", "CUST_ENDTERED_DATE", ''"Consent_Flag"'', \\
            ''"Consent_Update_Datetime"'', \\
        ).alias("file_df")
        
        # Load Data to the target table
        final_df.write.mode("append").saveAsTable(target_table)

        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")

        # write to success folder
    
        file_name=file_name.split(".")[0] + ''_'' + datetime.now().strftime("%Y%m%d%H%M%S")
        file_df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,file_format_type="csv",OVERWRITE=True,header=True,format_type_options=dict(FIELD_OPTIONALLY_ENCLOSED_BY="\\""))
   
        return "Success"
        
        
    except KeyError as key_error:
        
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message
        
    except Exception as e:
        
        error_message = f"Error: {str(e)}" +str(df.columns)
        return error_message
';
CREATE OR REPLACE PROCEDURE HCP360_IN_VENTASYS_LOAD_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '
from snowflake.snowpark.functions import col, lit, date_format, current_timestamp, to_date, year, month, concat, format_number, regexp_replace,to_timestamp,when,trim,upper
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DecimalType,DateType
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz
import pandas as pd
import zipfile
from snowflake.snowpark.files import SnowflakeFile

def main(session: snowpark.Session,Param):
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        sch_name        = stage_name.split(''.'')[0]
        target_table    = sch_name+"."+Param[3]

        full_path = "@"+stage_name+"/"+temp_stage_path+"/"+file_name
        pdf = pd.DataFrame()
        with SnowflakeFile.open(full_path,''rb'',require_scoped_url=False) as fhandle:
            with zipfile.ZipFile(fhandle,"r") as zip_ref:
            # Iterate over each file in the zip archive
                for i_file_name in zip_ref.namelist():
                    with zip_ref.open(i_file_name) as file:
                        source_df = pd.read_csv(file, delimiter="~", header =0, encoding_errors = "replace", quoting=3, na_filter=False)
                        for column in source_df.columns:
                            source_df[column] = source_df[column].apply(str)
                        source_df = source_df.replace('''',None)
                        pdf = pd.concat([pdf, source_df])
                       
        df= session.create_dataframe(pdf)

        df = df.withColumn("CRT_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        df = df.withColumn("FILE_NAME",lit(file_name).cast("string"))
        
        final_df = df
        if final_df.count()==0:
            return "No Data in file"
        
        # Load Data to the target table
        final_df.write.mode("append").saveAsTable(target_table)

        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")

        # write to success folder
    
        file_name=file_name.split(".")[0] + ''_'' + datetime.now().strftime("%Y%m%d%H%M%S")
        final_df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,file_format_type="csv",OVERWRITE=True,header=True,format_type_options=dict(FIELD_OPTIONALLY_ENCLOSED_BY="\\""))
   
        return "Success"

    except KeyError as key_error:
        
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message
        
    except Exception as e:
        
        error_message = f"Error: {str(e)}" +str(df.columns)
        return error_message
';
CREATE OR REPLACE PROCEDURE HCP360_IN_VENTASYS_SAMPLEDATA_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '
from snowflake.snowpark.functions import col, lit, date_format, current_timestamp, to_date, year, month, concat, format_number, regexp_replace,to_timestamp,when,trim,upper
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DecimalType,DateType
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz
import pandas as pd
import zipfile
from snowflake.snowpark.files import SnowflakeFile


def main(session: snowpark.Session,Param):
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        sch_name        = stage_name.split(''.'')[0]
        target_table    = sch_name+"."+Param[3]

        full_path = "@"+stage_name+"/"+temp_stage_path+"/"+file_name
        pdf = pd.DataFrame()
        with SnowflakeFile.open(full_path,''rb'',require_scoped_url=False) as fhandle:
            with zipfile.ZipFile(fhandle,"r") as zip_ref:
            # Iterate over each file in the zip archive
                for i_file_name in zip_ref.namelist():
                    with zip_ref.open(i_file_name) as file:
                        source_df = pd.read_csv(file, delimiter="~", header =0, encoding_errors = "replace", quoting=3, na_filter=False)
                        pdf = pd.concat([pdf, source_df])
                       
        df= session.create_dataframe(pdf)

        df = df.withColumn("CRT_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        df = df.withColumn("FILE_NAME",lit(file_name).cast("string"))
        
        final_df = df.select("TEAM_NAME", "V_SAMPLEID", "V_EMPID", "V_CUSTID", "DCR_DT", \\
            "SAMPLE_PRODUCT", "SAMPLE_UNITS", "CRT_DTTM", "FILE_NAME", "CATEGORY" ).alias("final_df")

        if final_df.count()==0:
            return "No Data in file"
        
        # Load Data to the target table
        final_df.write.mode("append").saveAsTable(target_table)

        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")

        # write to success folder
    
        file_name=file_name.split(".")[0] + ''_'' + datetime.now().strftime("%Y%m%d%H%M%S")
        final_df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,file_format_type="csv",OVERWRITE=True,header=True)
   
        return "Success"
        
    except KeyError as key_error:
        
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message
        
    except Exception as e:
        
        error_message = f"Error: {str(e)}" +str(df.columns)
        return error_message
';
CREATE OR REPLACE PROCEDURE HCP_HCP360_IN_SFMC_HCP_DETAIL_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '
from snowflake.snowpark.functions import col, lit, date_format, current_timestamp, to_date, year, month, concat, format_number, regexp_replace,to_timestamp,when,trim,upper
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DecimalType,DateType
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz


def main(session: snowpark.Session,Param):
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        sch_name        = stage_name.split(''.'')[0]
        target_table    = sch_name+"."+Param[3]
       
        df_schema = StructType([
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
            StructField("FA_Response_ID", StringType()),
            StructField("Source", StringType()),
            StructField("Medium", StringType()),
            StructField("Brand", StringType()),
            StructField("Country", StringType()),
            StructField("Campaign_ID", StringType()),
            StructField("Opt_In_For_Campaign", StringType()),
            StructField("Opt_In_For_Communication", StringType()),
            StructField("Opt_In_For_JNJ_Communication", StringType()),
            StructField("Created_Date", StringType()),
            StructField("Updated_Date", StringType()),
            StructField("Unsubscribe_Date", StringType()),
            StructField("Are_You_Board_Certified_Professional", StringType()),
            StructField("Status", StringType()),
            StructField("Profession", StringType()),
            StructField("Specialty", StringType()),
            StructField("License_Number", StringType()),
            StructField("Licensing_City", StringType()),
            StructField("Graduation_Year", StringType()),
            StructField("Practice_Name", StringType()),
            StructField("Hospital_Affiliation", StringType()),
            StructField("Promo_Code", StringType()),
            StructField("Reason_Code", StringType()),
            StructField("Secondary_Mobile_Number", StringType()),
            StructField("IP_Address", StringType()),
            StructField("Is_Confirmation_Email_Sent", StringType()),
            StructField("Salutation", StringType()),
            StructField("Territory_Name", StringType()),
            StructField("Position", StringType()),
            StructField("Primary_Parent", StringType()),
            StructField("Date_of_Sample_Order", StringType()),
            StructField("Email", StringType()),
            StructField("Device_ID", StringType()),
            StructField("How_Did_You_Hear_About_Us", StringType())
        ])

        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "|")\\
            .option("field_optionally_enclosed_by", "\\"") \\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        
        df = df.withColumn("CRT_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        
        final_df = df.select("Account_Name", to_date(concat(col("Year"), lit("-"), col("Month"), lit("-15")),lit("YYYY-mon-DD")).as_("POST_DT"), \\
                            "Level",  "Store_Code",  "Subcategory", "Article_Code", "Sales_Value_Rs_", "Sales_Qty", \\
                            "FILE_NAME", "RUN_ID", "FILE_UPLOADED_DATE", "CRT_DTTM" ).alias("final_df")

        if final_df.count()==0:
            return "No Data in file"
        
        # Load Data to the target table
        final_df.write.mode("append").saveAsTable(target_table)

        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")

        # write to success folder
    
        file_name=file_name.split(".")[0] + ''_'' + datetime.now().strftime("%Y%m%d%H%M%S")
        final_df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,file_format_type="csv",OVERWRITE=True,header=True)
   
        return "Success"
        
    except KeyError as key_error:
        
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message
        
    except Exception as e:
        
        error_message = f"Error: {str(e)}" +str(df.columns)
        return error_message
';
