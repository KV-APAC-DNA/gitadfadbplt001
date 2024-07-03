CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.NTASDL_RAW.GT_SELLOUT_HYUNDAI_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('openpyxl==3.1.2','snowflake-snowpark-python==*','unidecode==1.2.0')
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

        #Param=[''Hyundai_126137_202401.xlsx'',''NTASDL_RAW.DEV_LOAD_STAGE_ADLS_KOR'',''dev/gt_sellout/transaction_files/department_store/hyundai/'',''sdl_kr_hyundai_gt_sellout'']

        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        file_header_loc = 10

        #Reading data from adls
        full_path = "@"+stage_name+"/"+temp_stage_path+"/"+file_name
        
        with SnowflakeFile.open(full_path,''rb'',require_scoped_url= False) as f:
            excel_file = pd.ExcelFile(f)
            #print(excel_file)
            
            df1 = excel_file.parse(sheet_name=''현대압구정'',engine=''openpyxl'')


            df1 = df1.applymap(lambda x: str(x))
            #df1 = df1.applymap(lambda x: unidecode(x))
            #print(df1)
            df1.rename(columns={df1.columns[0]:''BURIAL_NAME'',
                                df1.columns[1]:''EAN'',
                                df1.columns[2]:''ARTICLE_NAME'',
                                df1.columns[3]:''NORMAL_SALES'',
                                df1.columns[4]:''QTY'',
                                df1.columns[5]:''SALES'',
                                df1.columns[6]:''UNIT_PRICE'',
                                df1.columns[7]:''DC_RATE'',
                                df1.columns[8]:''MARGIN_RATE'',
                                df1.columns[9]:''MARGIN_NORMAL'',
                                df1.columns[10]:''MARGIN_SOLD'',
                                df1.columns[11]:''UNIT_PRC_PRES''},inplace = True)

            
            k  = df1.iloc[0][''BURIAL_NAME'']
            
            df1 = df1.iloc[3:]
            
            df1 = df1[df1.columns[:12]]
            
            df1[''SUB_CUSTOMER_NAME''] = k
            
            df1[''EAN''] = df1[''EAN''].map(lambda x: None if x==''nan'' else x)
            
            df1.dropna(subset=[''EAN''],inplace=True)
#----------------------------------------------------------------------------------------------------------------------------------------------
            df2 = excel_file.parse(sheet_name=''무'',engine=''openpyxl'')
            
            df2 = df2.applymap(lambda x: str(x))
            #df2 = df2.applymap(lambda x: unidecode(x))
            #print(df2)
            df2.rename(columns={df2.columns[0]:''BURIAL_NAME'',
                                df2.columns[1]:''EAN'',
                                df2.columns[2]:''ARTICLE_NAME'',
                                df2.columns[3]:''NORMAL_SALES'',
                                df2.columns[4]:''QTY'',
                                df2.columns[5]:''SALES'',
                                df2.columns[6]:''UNIT_PRICE'',
                                df2.columns[7]:''DC_RATE'',
                                df2.columns[8]:''MARGIN_RATE'',
                                df2.columns[9]:''MARGIN_NORMAL'',
                                df2.columns[10]:''MARGIN_SOLD'',
                                df2.columns[11]:''UNIT_PRC_PRES''},inplace = True)

            
            k1  = df2.iloc[0][''BURIAL_NAME'']
            
            df2 = df2.iloc[3:]
            df2 = df2[df2.columns[:12]]
            df2[''SUB_CUSTOMER_NAME''] = k1
            df2[''EAN''] = df2[''EAN''].map(lambda x: None if x==''nan'' else x)
            df2.dropna(subset=[''EAN''],inplace=True)
            
            #snowdf = snowdf.with_columns(''k'',lit(k))
            #---------------------------------------------------------------------------------------------------------------------------------------

            df3 = excel_file.parse(sheet_name=''천'',engine=''openpyxl'')
            
            df3 = df3.applymap(lambda x: str(x))
            #df3 = df3.applymap(lambda x: unidecode(x))
            #print(df3)
            df3.rename(columns={df3.columns[0]:''BURIAL_NAME'',
                                df3.columns[1]:''EAN'',
                                df3.columns[2]:''ARTICLE_NAME'',
                                df3.columns[3]:''NORMAL_SALES'',
                                df3.columns[4]:''QTY'',
                                df3.columns[5]:''SALES'',
                                df3.columns[6]:''UNIT_PRICE'',
                                df3.columns[7]:''DC_RATE'',
                                df3.columns[8]:''MARGIN_RATE'',
                                df3.columns[9]:''MARGIN_NORMAL'',
                                df3.columns[10]:''MARGIN_SOLD'',
                                df3.columns[11]:''UNIT_PRC_PRES''},inplace = True)

            
            k  = df3.iloc[0][''BURIAL_NAME'']
            
            df3 = df3.iloc[3:]
            
            df3 = df3[df3.columns[:12]]
            
            df3[''SUB_CUSTOMER_NAME''] = k
            
            df3[''EAN''] = df3[''EAN''].map(lambda x: None if x==''nan'' else x)
            
            df3.dropna(subset=[''EAN''],inplace=True)

            #---------------------------------------------------------------------------------------------------

            df4 = excel_file.parse(sheet_name=''신'',engine=''openpyxl'')
            
            df4 = df4.applymap(lambda x: str(x))
            #df4 = df4.applymap(lambda x: unidecode(x))
            #print(df4)
            df4.rename(columns={df4.columns[0]:''BURIAL_NAME'',
                                df4.columns[1]:''EAN'',
                                df4.columns[2]:''ARTICLE_NAME'',
                                df4.columns[3]:''NORMAL_SALES'',
                                df4.columns[4]:''QTY'',
                                df4.columns[5]:''SALES'',
                                df4.columns[6]:''UNIT_PRICE'',
                                df4.columns[7]:''DC_RATE'',
                                df4.columns[8]:''MARGIN_RATE'',
                                df4.columns[9]:''MARGIN_NORMAL'',
                                df4.columns[10]:''MARGIN_SOLD'',
                                df4.columns[11]:''UNIT_PRC_PRES''},inplace = True)

            
            k  = df4.iloc[0][''BURIAL_NAME'']
            
            df4 = df4.iloc[3:]
            
            df4 = df4[df4.columns[:12]]
            
            df4[''SUB_CUSTOMER_NAME''] = k
            
            df4[''EAN''] = df4[''EAN''].map(lambda x: None if x==''nan'' else x)
            
            df4.dropna(subset=[''EAN''],inplace=True)
            
            
            
            #--------------------------------------------------------------------------------------------------

            df5 = excel_file.parse(sheet_name=''현미'',engine=''openpyxl'')
            
            df5 = df5.applymap(lambda x: str(x))
            #df5 = df5.applymap(lambda x: unidecode(x))
            #print(df5)
            df5.rename(columns={df5.columns[0]:''BURIAL_NAME'',
                                df5.columns[1]:''EAN'',
                                df5.columns[2]:''ARTICLE_NAME'',
                                df5.columns[3]:''NORMAL_SALES'',
                                df5.columns[4]:''QTY'',
                                df5.columns[5]:''SALES'',
                                df5.columns[6]:''UNIT_PRICE'',
                                df5.columns[7]:''DC_RATE'',
                                df5.columns[8]:''MARGIN_RATE'',
                                df5.columns[9]:''MARGIN_NORMAL'',
                                df5.columns[10]:''MARGIN_SOLD'',
                                df5.columns[11]:''UNIT_PRC_PRES''},inplace = True)

            
            k  = df5.iloc[0][''BURIAL_NAME'']
            
            df5 = df5.iloc[3:]
            
            df5 = df5[df5.columns[:12]]
            
            df5[''SUB_CUSTOMER_NAME''] = k
            
            df5[''EAN''] = df5[''EAN''].map(lambda x: None if x==''nan'' else x)
            
            df5.dropna(subset=[''EAN''],inplace=True)
            
            #------------------------------------------------------------------------------------------

            df6 = excel_file.parse(sheet_name=''목'',engine=''openpyxl'')
            
            df6 = df6.applymap(lambda x: str(x))
            #df6 = df6.applymap(lambda x: unidecode(x))
            #print(df6)
            df6.rename(columns={df6.columns[0]:''BURIAL_NAME'',
                                df6.columns[1]:''EAN'',
                                df6.columns[2]:''ARTICLE_NAME'',
                                df6.columns[3]:''NORMAL_SALES'',
                                df6.columns[4]:''QTY'',
                                df6.columns[5]:''SALES'',
                                df6.columns[6]:''UNIT_PRICE'',
                                df6.columns[7]:''DC_RATE'',
                                df6.columns[8]:''MARGIN_RATE'',
                                df6.columns[9]:''MARGIN_NORMAL'',
                                df6.columns[10]:''MARGIN_SOLD'',
                                df6.columns[11]:''UNIT_PRC_PRES''},inplace = True)

            
            k  = df6.iloc[0][''BURIAL_NAME'']
            
            df6 = df6.iloc[3:]
            
            df6 = df6[df6.columns[:12]]
            
            df6[''SUB_CUSTOMER_NAME''] = k
            
            df6[''EAN''] = df6[''EAN''].map(lambda x: None if x==''nan'' else x)
            
            df6.dropna(subset=[''EAN''],inplace=True)
            
            #---------------------------------------------------------------------------

            df7 = excel_file.parse(sheet_name=''현중'',engine=''openpyxl'')
            
            df7 = df7.applymap(lambda x: str(x))
            #df7 = df7.applymap(lambda x: unidecode(x))
            #print(df7)
            df7.rename(columns={df7.columns[0]:''BURIAL_NAME'',
                                df7.columns[1]:''EAN'',
                                df7.columns[2]:''ARTICLE_NAME'',
                                df7.columns[3]:''NORMAL_SALES'',
                                df7.columns[4]:''QTY'',
                                df7.columns[5]:''SALES'',
                                df7.columns[6]:''UNIT_PRICE'',
                                df7.columns[7]:''DC_RATE'',
                                df7.columns[8]:''MARGIN_RATE'',
                                df7.columns[9]:''MARGIN_NORMAL'',
                                df7.columns[10]:''MARGIN_SOLD'',
                                df7.columns[11]:''UNIT_PRC_PRES''},inplace = True)

            
            k  = df7.iloc[0][''BURIAL_NAME'']
            
            df7 = df7.iloc[3:]
            
            df7 = df7[df7.columns[:12]]
            
            df7[''SUB_CUSTOMER_NAME''] = k
            
            df7[''EAN''] = df7[''EAN''].map(lambda x: None if x==''nan'' else x)
            
            df7.dropna(subset=[''EAN''],inplace=True)
            
            #--------------------------------------------------------------------------
            df8 = excel_file.parse(sheet_name=''킨'',engine=''openpyxl'')
            
            df8 = df8.applymap(lambda x: str(x))
            #df8 = df8.applymap(lambda x: unidecode(x))
            #print(df8)
            df8.rename(columns={df8.columns[0]:''BURIAL_NAME'',
                                df8.columns[1]:''EAN'',
                                df8.columns[2]:''ARTICLE_NAME'',
                                df8.columns[3]:''NORMAL_SALES'',
                                df8.columns[4]:''QTY'',
                                df8.columns[5]:''SALES'',
                                df8.columns[6]:''UNIT_PRICE'',
                                df8.columns[7]:''DC_RATE'',
                                df8.columns[8]:''MARGIN_RATE'',
                                df8.columns[9]:''MARGIN_NORMAL'',
                                df8.columns[10]:''MARGIN_SOLD'',
                                df8.columns[11]:''UNIT_PRC_PRES''},inplace = True)

            
            k  = df8.iloc[0][''BURIAL_NAME'']
            
            df8 = df8.iloc[3:]
            
            df8 = df8[df8.columns[:12]]
            
            df8[''SUB_CUSTOMER_NAME''] = k
            
            df8[''EAN''] = df8[''EAN''].map(lambda x: None if x==''nan'' else x)
            
            df8.dropna(subset=[''EAN''],inplace=True)

            
            
            #-----------------------------------------------------------------------
            df9 = excel_file.parse(sheet_name=''디'',engine=''openpyxl'')
            
            df9 = df9.applymap(lambda x: str(x))
            #df9 = df9.applymap(lambda x: unidecode(x))
            #print(df9)
            df9.rename(columns={df9.columns[0]:''BURIAL_NAME'',
                                df9.columns[1]:''EAN'',
                                df9.columns[2]:''ARTICLE_NAME'',
                                df9.columns[3]:''NORMAL_SALES'',
                                df9.columns[4]:''QTY'',
                                df9.columns[5]:''SALES'',
                                df9.columns[6]:''UNIT_PRICE'',
                                df9.columns[7]:''DC_RATE'',
                                df9.columns[8]:''MARGIN_RATE'',
                                df9.columns[9]:''MARGIN_NORMAL'',
                                df9.columns[10]:''MARGIN_SOLD'',
                                df9.columns[11]:''UNIT_PRC_PRES''},inplace = True)

            
            k  = df9.iloc[0][''BURIAL_NAME'']
            
            df9 = df9.iloc[3:]
            
            df9 = df9[df9.columns[:12]]
            
            df9[''SUB_CUSTOMER_NAME''] = k
            
            df9[''EAN''] = df9[''EAN''].map(lambda x: None if x==''nan'' else x)
            
            df9.dropna(subset=[''EAN''],inplace=True)
            
            #------------------------------------------------------------------------------------------------------------------

            df10 = excel_file.parse(sheet_name=''판'',engine=''openpyxl'')
            
            df10 = df10.applymap(lambda x: str(x))
            #df10 = df10.applymap(lambda x: unidecode(x))
            #print(df10)
            df10.rename(columns={df10.columns[0]:''BURIAL_NAME'',
                                df10.columns[1]:''EAN'',
                                df10.columns[2]:''ARTICLE_NAME'',
                                df10.columns[3]:''NORMAL_SALES'',
                                df10.columns[4]:''QTY'',
                                df10.columns[5]:''SALES'',
                                df10.columns[6]:''UNIT_PRICE'',
                                df10.columns[7]:''DC_RATE'',
                                df10.columns[8]:''MARGIN_RATE'',
                                df10.columns[9]:''MARGIN_NORMAL'',
                                df10.columns[10]:''MARGIN_SOLD'',
                                df10.columns[11]:''UNIT_PRC_PRES''},inplace = True)

            
            k  = df10.iloc[0][''BURIAL_NAME'']
            
            df10 = df10.iloc[3:]
            
            df10 = df10[df10.columns[:12]]
            
            df10[''SUB_CUSTOMER_NAME''] = k
            
            df10[''EAN''] = df10[''EAN''].map(lambda x: None if x==''nan'' else x)
            
            df10.dropna(subset=[''EAN''],inplace=True)
            
            #------------------------------------------------------------------------------------------
            df11 = excel_file.parse(sheet_name=''서울'',engine=''openpyxl'')
            
            df11 = df11.applymap(lambda x: str(x))
            #df11 = df11.applymap(lambda x: unidecode(x))
            #print(df11)
            df11.rename(columns={df11.columns[0]:''BURIAL_NAME'',
                                df11.columns[1]:''EAN'',
                                df11.columns[2]:''ARTICLE_NAME'',
                                df11.columns[3]:''NORMAL_SALES'',
                                df11.columns[4]:''QTY'',
                                df11.columns[5]:''SALES'',
                                df11.columns[6]:''UNIT_PRICE'',
                                df11.columns[7]:''DC_RATE'',
                                df11.columns[8]:''MARGIN_RATE'',
                                df11.columns[9]:''MARGIN_NORMAL'',
                                df11.columns[10]:''MARGIN_SOLD'',
                                df11.columns[11]:''UNIT_PRC_PRES''},inplace = True)

            
            k  = df11.iloc[0][''BURIAL_NAME'']
            
            df11 = df11.iloc[3:]
            
            df11 = df11[df11.columns[:12]]
            
            df11[''SUB_CUSTOMER_NAME''] = k
            
            df11[''EAN''] = df11[''EAN''].map(lambda x: None if x==''nan'' else x)
            
            df11.dropna(subset=[''EAN''],inplace=True)
            
            #--------------------------------------------------------------------
            df12 = excel_file.parse(sheet_name=''현부산'',engine=''openpyxl'')
            
            df12 = df12.applymap(lambda x: str(x))
            #df12 = df12.applymap(lambda x: unidecode(x))
            #print(df12)
            df12.rename(columns={df12.columns[0]:''BURIAL_NAME'',
                                df12.columns[1]:''EAN'',
                                df12.columns[2]:''ARTICLE_NAME'',
                                df12.columns[3]:''NORMAL_SALES'',
                                df12.columns[4]:''QTY'',
                                df12.columns[5]:''SALES'',
                                df12.columns[6]:''UNIT_PRICE'',
                                df12.columns[7]:''DC_RATE'',
                                df12.columns[8]:''MARGIN_RATE'',
                                df12.columns[9]:''MARGIN_NORMAL'',
                                df12.columns[10]:''MARGIN_SOLD'',
                                df12.columns[11]:''UNIT_PRC_PRES''},inplace = True)

            
            k  = df12.iloc[0][''BURIAL_NAME'']
            
            df12 = df12.iloc[3:]
            
            df12 = df12[df12.columns[:12]]
            
            df12[''SUB_CUSTOMER_NAME''] = k
            
            df12[''EAN''] = df12[''EAN''].map(lambda x: None if x==''nan'' else x)
            
            df12.dropna(subset=[''EAN''],inplace=True)
            
            #-----------------------------------------------------------------------------------

            df13 = excel_file.parse(sheet_name=''현대구'',engine=''openpyxl'')
            
            df13 = df13.applymap(lambda x: str(x))
            #df13 = df13.applymap(lambda x: unidecode(x))
            #print(df13)
            df13.rename(columns={df13.columns[0]:''BURIAL_NAME'',
                                df13.columns[1]:''EAN'',
                                df13.columns[2]:''ARTICLE_NAME'',
                                df13.columns[3]:''NORMAL_SALES'',
                                df13.columns[4]:''QTY'',
                                df13.columns[5]:''SALES'',
                                df13.columns[6]:''UNIT_PRICE'',
                                df13.columns[7]:''DC_RATE'',
                                df13.columns[8]:''MARGIN_RATE'',
                                df13.columns[9]:''MARGIN_NORMAL'',
                                df13.columns[10]:''MARGIN_SOLD'',
                                df13.columns[11]:''UNIT_PRC_PRES''},inplace = True)

            
            k  = df13.iloc[0][''BURIAL_NAME'']
            
            df13 = df13.iloc[3:]
            
            df13 = df13[df13.columns[:12]]
            
            df13[''SUB_CUSTOMER_NAME''] = k
            
            df13[''EAN''] = df13[''EAN''].map(lambda x: None if x==''nan'' else x)
            
            df13.dropna(subset=[''EAN''],inplace=True)
            
            #-----------------------------------------------------------------------

            df14 = excel_file.parse(sheet_name=''현울'',engine=''openpyxl'')
            
            df14 = df14.applymap(lambda x: str(x))
            #df14 = df14.applymap(lambda x: unidecode(x))
            #print(df14)
            df14.rename(columns={df14.columns[0]:''BURIAL_NAME'',
                                df14.columns[1]:''EAN'',
                                df14.columns[2]:''ARTICLE_NAME'',
                                df14.columns[3]:''NORMAL_SALES'',
                                df14.columns[4]:''QTY'',
                                df14.columns[5]:''SALES'',
                                df14.columns[6]:''UNIT_PRICE'',
                                df14.columns[7]:''DC_RATE'',
                                df14.columns[8]:''MARGIN_RATE'',
                                df14.columns[9]:''MARGIN_NORMAL'',
                                df14.columns[10]:''MARGIN_SOLD'',
                                df14.columns[11]:''UNIT_PRC_PRES''},inplace = True)

            
            k  = df14.iloc[0][''BURIAL_NAME'']
            
            df14 = df14.iloc[3:]
            
            df14 = df14[df14.columns[:12]]
            
            df14[''SUB_CUSTOMER_NAME''] = k
            
            df14[''EAN''] = df14[''EAN''].map(lambda x: None if x==''nan'' else x)
            
            df14.dropna(subset=[''EAN''],inplace=True)
            
            #-------------------------------------------------------------------------

            df15 = excel_file.parse(sheet_name=''동구'',engine=''openpyxl'')
            
            df15 = df15.applymap(lambda x: str(x))
            #df15 = df15.applymap(lambda x: unidecode(x))
            #print(df15)
            df15.rename(columns={df15.columns[0]:''BURIAL_NAME'',
                                df15.columns[1]:''EAN'',
                                df15.columns[2]:''ARTICLE_NAME'',
                                df15.columns[3]:''NORMAL_SALES'',
                                df15.columns[4]:''QTY'',
                                df15.columns[5]:''SALES'',
                                df15.columns[6]:''UNIT_PRICE'',
                                df15.columns[7]:''DC_RATE'',
                                df15.columns[8]:''MARGIN_RATE'',
                                df15.columns[9]:''MARGIN_NORMAL'',
                                df15.columns[10]:''MARGIN_SOLD'',
                                df15.columns[11]:''UNIT_PRC_PRES''},inplace = True)

            
            k  = df15.iloc[0][''BURIAL_NAME'']
            
            df15 = df15.iloc[2:]
            
            df15 = df15[df15.columns[:12]]
            
            df15[''SUB_CUSTOMER_NAME''] = k
            
            df15[''EAN''] = df15[''EAN''].map(lambda x: None if x==''nan'' else x)
            
            df15.dropna(subset=[''EAN''],inplace=True)
            
            #-------------------------------------------------------------------

            

            df16 = excel_file.parse(sheet_name=''충'',engine=''openpyxl'')
            
            df16 = df16.applymap(lambda x: str(x))
            #df16 = df16.applymap(lambda x: unidecode(x))
            #print(df16)
            df16.rename(columns={df16.columns[0]:''BURIAL_NAME'',
                                df16.columns[1]:''EAN'',
                                df16.columns[2]:''ARTICLE_NAME'',
                                df16.columns[3]:''NORMAL_SALES'',
                                df16.columns[4]:''QTY'',
                                df16.columns[5]:''SALES'',
                                df16.columns[6]:''UNIT_PRICE'',
                                df16.columns[7]:''DC_RATE'',
                                df16.columns[8]:''MARGIN_RATE'',
                                df16.columns[9]:''MARGIN_NORMAL'',
                                df16.columns[10]:''MARGIN_SOLD'',
                                df16.columns[11]:''UNIT_PRC_PRES''},inplace = True)

            
            k  = df16.iloc[0][''BURIAL_NAME'']
            
            df16 = df16.iloc[2:]
            
            df16 = df16[df16.columns[:12]]
            
            df16[''SUB_CUSTOMER_NAME''] = k
            
            df16[''EAN''] = df16[''EAN''].map(lambda x: None if x==''nan'' else x)
            
            df16.dropna(subset=[''EAN''],inplace=True)
            
            #---------------------------------------------------------------------------------

            df17 = excel_file.parse(sheet_name=''아산마트'',engine=''openpyxl'')
            
            df17 = df17.applymap(lambda x: str(x))
            #df17 = df17.applymap(lambda x: unidecode(x))
            #print(df17)
            df17.rename(columns={df17.columns[0]:''BURIAL_NAME'',
                                df17.columns[1]:''EAN'',
                                df17.columns[2]:''ARTICLE_NAME'',
                                df17.columns[3]:''NORMAL_SALES'',
                                df17.columns[4]:''QTY'',
                                df17.columns[5]:''SALES'',
                                df17.columns[6]:''UNIT_PRICE'',
                                df17.columns[7]:''DC_RATE'',
                                df17.columns[8]:''MARGIN_RATE'',
                                df17.columns[9]:''MARGIN_NORMAL'',
                                df17.columns[10]:''MARGIN_SOLD'',
                                df17.columns[11]:''UNIT_PRC_PRES''},inplace = True)

            
            k  = df17.iloc[0][''BURIAL_NAME'']
            
            df17 = df17.iloc[2:]
            
            df17 = df17[df17.columns[:12]]
            
            df17[''SUB_CUSTOMER_NAME''] = k
            
            df17[''EAN''] = df17[''EAN''].map(lambda x: None if x==''nan'' else x)
            
            
            df17.dropna(subset=[''EAN''],inplace=True)
            
            #--------------------------------------------------------------------

            df18 = excel_file.parse(sheet_name=''현대기타'',engine=''openpyxl'')
            
            df18 = df18.applymap(lambda x: str(x))
            #df18 = df18.applymap(lambda x: unidecode(x))
            #print(df18)
            df18.rename(columns={df18.columns[0]:''BURIAL_NAME'',
                                df18.columns[1]:''EAN'',
                                df18.columns[2]:''ARTICLE_NAME'',
                                df18.columns[3]:''NORMAL_SALES'',
                                df18.columns[4]:''QTY'',
                                df18.columns[5]:''SALES'',
                                df18.columns[6]:''UNIT_PRICE'',
                                df18.columns[7]:''DC_RATE'',
                                df18.columns[8]:''MARGIN_RATE'',
                                df18.columns[9]:''MARGIN_NORMAL'',
                                df18.columns[10]:''MARGIN_SOLD'',
                                df18.columns[11]:''UNIT_PRC_PRES''},inplace = True)

            
            k  = df18.iloc[0][''BURIAL_NAME'']
            
            df18 = df18.iloc[2:]
            
            df18 = df18[df18.columns[:12]]
            
            df18[''SUB_CUSTOMER_NAME''] = k
            
            df18[''EAN''] = df18[''EAN''].map(lambda x: None if x==''nan'' else x)
            
            df18.dropna(subset=[''EAN''],inplace=True)

            

            df = pd.concat([df1,df2,df3,df4,df5,df6,df7,df8,df9,df10,df11,df12,df13,df14,df15,df16,df17,df18],axis = 0)

            df[''NORMAL_SALES''] = df[''NORMAL_SALES''].map(lambda x:str(round(float(x))))
            
            snowdf = session.create_dataframe(df)

            snowdf = snowdf.na.drop(''all'')

            #Checking if dataframe is having any data
            
            if snowdf.count()==0:
                return "No Data in file"

            def returnimx_txn_dt(file_name):
                return file_name.split(''.'')[0].split(''_'')[2]

            def returncust_cd(file_name):
                return file_name.split(''.'')[0].split(''_'')[1]
            

            snowdf =  snowdf.with_column(''dstr_nm'',lit(''HYUNDAI''))
            
            snowdf =  snowdf.with_column(''ims_txn_dt'',lit(returnimx_txn_dt(file_name)))
            
            snowdf = snowdf.with_column(''cust_cd'',lit(returncust_cd(file_name)))

            snowparkdf = snowdf.select(''dstr_nm'',
                                       ''ims_txn_dt'',
                                       ''burial_name'',
                                       ''ean'',
                                       ''article_name'',
                                       ''normal_sales'',
                                       ''qty'',
                                       ''sales'',
                                       ''unit_price'',
                                       ''dc_rate'',
                                       ''margin_rate'',
                                       ''margin_normal'',
                                       ''margin_sold'',
                                       ''unit_prc_pres'',
                                       ''sub_customer_name'',
                                       ''cust_cd'')

            snowparkdf.write.mode("append").saveAsTable(target_table)

            current_date = datetime.now()
            formatted_year = current_date.strftime("%Y")
            formatted_month = current_date.strftime("%m")

             # write to success folder
            #file_name = file_name.split(''.'')[0]+"_"+datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")

            

            snowparkdf.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)

            

            

            return ''Success''
            

    except KeyError as key_error:
        # Handle KeyError (missing columns) here
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message
        
    except Exception as e:
        # Handle exceptions here
        error_message = f"Error: {str(e)}"
        return error_message;';
		
		
		
CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.NTASDL_RAW.GT_SELLOUT_LOTTE_AK_PROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('openpyxl==3.1.2','snowflake-snowpark-python==*')
HANDLER = 'main'
EXECUTE AS OWNER
AS '# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them.

import snowflake.snowpark as snowpark
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
#from unidecode import unidecode

from re import sub

def main(session: snowpark.Session,Param):
    

    try:

        #Param=[''Lotte_AK_126137_202312.xlsx'',''NTASDL_RAW.DEV_LOAD_STAGE_ADLS_KOR'',''dev/gt_sellout/transaction_files/department_store/lotte_ak/'',''sdl_kr_lotte_ak_gt_sellout'']

        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        file_header_loc = 10

        #Reading data from adls
        full_path = "@"+stage_name+"/"+temp_stage_path+"/"+file_name
        
        with SnowflakeFile.open(full_path,''rb'',require_scoped_url= False) as f:
            excel_file = pd.ExcelFile(f)
            #print(excel_file)
            
            df1 = excel_file.parse(sheet_name=''AK수원'',engine=''openpyxl'')


            df1 = df1.applymap(lambda x: str(x))
            #df1 = df1.applymap(lambda x: unidecode(x))
            #print(df1)
            df1.rename(columns={df1.columns[0]:''EAN'',
                                df1.columns[1]:''ARTICLE_NAME'',
                                df1.columns[2]:''NORMAL_SALES'',
                                df1.columns[3]:''UNIT_PRICE'',
                                df1.columns[4]:''DC_RATE'',
                                df1.columns[5]:''QTY'',
                                df1.columns[6]:''EVENT_SALES'',
                                df1.columns[7]:''MARGIN_27'',
                                df1.columns[8]:''MARGIN_22'',
                                df1.columns[9]:''RECEIPT'',
                                df1.columns[10]:''VND_MARGIN_22'',
                                df1.columns[11]:''UNIT_PRC_DIFF'',
                                df1.columns[12]:''FIN_UNIT_PRC''},inplace = True)

            #ean,article_name,normal_sales,unit_price,dc_rate,qty,event_sales,margin_27,margin_22,receipt,vnd_margin_22,unit_prc_diff,fin_unit_prc

            
            k  = df1.iloc[0][''EAN'']
            
            df1 = df1.iloc[3:]
            
            df1 = df1[df1.columns[:13]]
            
            df1[''SUB_CUSTOMER_NAME''] = k
            
            df1[''EAN''] = df1[''EAN''].map(lambda x: None if x==''nan'' else x)
            
            df1.dropna(subset=[''EAN''],inplace=True)

            
#----------------------------------------------------------------------------------------------------------------------------------------------
            df2 = excel_file.parse(sheet_name=''AK분당'',engine=''openpyxl'')


            df2 = df2.applymap(lambda x: str(x))
            #df2 = df2.applymap(lambda x: unidecode(x))
            #print(df2)
            df2.rename(columns={df2.columns[0]:''EAN'',
                                df2.columns[1]:''ARTICLE_NAME'',
                                df2.columns[2]:''NORMAL_SALES'',
                                df2.columns[3]:''UNIT_PRICE'',
                                df2.columns[4]:''DC_RATE'',
                                df2.columns[5]:''QTY'',
                                df2.columns[6]:''EVENT_SALES'',
                                df2.columns[7]:''MARGIN_27'',
                                df2.columns[8]:''MARGIN_22'',
                                df2.columns[9]:''RECEIPT'',
                                df2.columns[10]:''VND_MARGIN_22'',
                                df2.columns[11]:''UNIT_PRC_DIFF'',
                                df2.columns[12]:''FIN_UNIT_PRC''},inplace = True)

            #ean,article_name,normal_sales,unit_price,dc_rate,qty,event_sales,margin_27,margin_22,receipt,vnd_margin_22,unit_prc_diff,fin_unit_prc

            
            k  = df2.iloc[0][''EAN'']
            
            df2 = df2.iloc[3:]
            
            df2 = df2[df2.columns[:13]]
            
            df2[''SUB_CUSTOMER_NAME''] = k
            
            df2[''EAN''] = df2[''EAN''].map(lambda x: None if x==''nan'' else x)
            
            df2.dropna(subset=[''EAN''],inplace=True)

            
            # #---------------------------------------------------------------------------------------------------------------------------------------

            df3 = excel_file.parse(sheet_name=''AK평택'',engine=''openpyxl'')


            df3 = df3.applymap(lambda x: str(x))
            #df3 = df3.applymap(lambda x: unidecode(x))
            #print(df3)
            df3.rename(columns={df3.columns[0]:''EAN'',
                                df3.columns[1]:''ARTICLE_NAME'',
                                df3.columns[2]:''NORMAL_SALES'',
                                df3.columns[3]:''UNIT_PRICE'',
                                df3.columns[4]:''DC_RATE'',
                                df3.columns[5]:''QTY'',
                                df3.columns[6]:''EVENT_SALES'',
                                df3.columns[7]:''MARGIN_27'',
                                df3.columns[8]:''MARGIN_22'',
                                df3.columns[9]:''RECEIPT'',
                                df3.columns[10]:''VND_MARGIN_22'',
                                df3.columns[11]:''UNIT_PRC_DIFF'',
                                df3.columns[12]:''FIN_UNIT_PRC''},inplace = True)

            #ean,article_name,normal_sales,unit_price,dc_rate,qty,event_sales,margin_27,margin_22,receipt,vnd_margin_22,unit_prc_diff,fin_unit_prc

            
            k  = df3.iloc[0][''EAN'']
            
            df3 = df3.iloc[3:]
            
            df3 = df3[df3.columns[:13]]
            
            df3[''SUB_CUSTOMER_NAME''] = k
            
            df3[''EAN''] = df3[''EAN''].map(lambda x: None if x==''nan'' else x)
            
            df3.dropna(subset=[''EAN''],inplace=True)

            

            # #---------------------------------------------------------------------------------------------------

            df4 = excel_file.parse(sheet_name=''명동'',engine=''openpyxl'')


            df4 = df4.applymap(lambda x: str(x))
            #df4 = df4.applymap(lambda x: unidecode(x))
            #print(df4)
            df4.rename(columns={df4.columns[0]:''EAN'',
                                df4.columns[1]:''ARTICLE_NAME'',
                                df4.columns[2]:''NORMAL_SALES'',
                                df4.columns[3]:''UNIT_PRICE'',
                                df4.columns[4]:''DC_RATE'',
                                df4.columns[5]:''QTY'',
                                df4.columns[6]:''EVENT_SALES'',
                                df4.columns[7]:''MARGIN_27'',
                                df4.columns[8]:''MARGIN_22'',
                                df4.columns[9]:''RECEIPT'',
                                df4.columns[10]:''VND_MARGIN_22'',
                                df4.columns[11]:''UNIT_PRC_DIFF'',
                                df4.columns[12]:''FIN_UNIT_PRC''},inplace = True)

            #ean,article_name,normal_sales,unit_price,dc_rate,qty,event_sales,margin_27,margin_22,receipt,vnd_margin_22,unit_prc_diff,fin_unit_prc

            
            k  = df4.iloc[0][''EAN'']
            
            df4 = df4.iloc[3:]
            
            df4 = df4[df4.columns[:13]]
            
            df4[''SUB_CUSTOMER_NAME''] = k
            
            df4[''EAN''] = df4[''EAN''].map(lambda x: None if x==''nan'' else x)
            
            df4.dropna(subset=[''EAN''],inplace=True)
            
            
            
            # #--------------------------------------------------------------------------------------------------

            df5 = excel_file.parse(sheet_name=''잠실'',engine=''openpyxl'')


            df5 = df5.applymap(lambda x: str(x))
            k = df5.columns[0]
            #df5 = df5.applymap(lambda x: unidecode(x))
            #print(df5)
            df5.rename(columns={df5.columns[0]:''EAN'',
                                df5.columns[1]:''ARTICLE_NAME'',
                                df5.columns[2]:''NORMAL_SALES'',
                                df5.columns[3]:''UNIT_PRICE'',
                                df5.columns[4]:''DC_RATE'',
                                df5.columns[5]:''QTY'',
                                df5.columns[6]:''EVENT_SALES'',
                                df5.columns[7]:''MARGIN_27'',
                                df5.columns[8]:''MARGIN_22'',
                                df5.columns[9]:''RECEIPT'',
                                df5.columns[10]:''VND_MARGIN_22'',
                                df5.columns[11]:''UNIT_PRC_DIFF'',
                                df5.columns[12]:''FIN_UNIT_PRC''},inplace = True)

            #ean,article_name,normal_sales,unit_price,dc_rate,qty,event_sales,margin_27,margin_22,receipt,vnd_margin_22,unit_prc_diff,fin_unit_prc

            
            
            
            df5 = df5.iloc[3:]
            
            df5 = df5[df5.columns[:13]]
            
            df5[''SUB_CUSTOMER_NAME''] = k
            
            df5[''EAN''] = df5[''EAN''].map(lambda x: None if x==''nan'' else x)
            
            df5.dropna(subset=[''EAN''],inplace=True)

            
            # #------------------------------------------------------------------------------------------

            df6 = excel_file.parse(sheet_name=''청량리'',engine=''openpyxl'')


            df6 = df6.applymap(lambda x: str(x))
            #df6 = df6.applymap(lambda x: unidecode(x))
            #print(df6)
            k = df6.columns[0]
            
            df6.rename(columns={df6.columns[0]:''EAN'',
                                df6.columns[1]:''ARTICLE_NAME'',
                                df6.columns[2]:''NORMAL_SALES'',
                                df6.columns[3]:''UNIT_PRICE'',
                                df6.columns[4]:''DC_RATE'',
                                df6.columns[5]:''QTY'',
                                df6.columns[6]:''EVENT_SALES'',
                                df6.columns[7]:''MARGIN_27'',
                                df6.columns[8]:''MARGIN_22'',
                                df6.columns[9]:''RECEIPT'',
                                df6.columns[10]:''VND_MARGIN_22'',
                                df6.columns[11]:''UNIT_PRC_DIFF'',
                                df6.columns[12]:''FIN_UNIT_PRC''},inplace = True)

            #ean,article_name,normal_sales,unit_price,dc_rate,qty,event_sales,margin_27,margin_22,receipt,vnd_margin_22,unit_prc_diff,fin_unit_prc

            
            
            
            df6 = df6.iloc[3:]
            
            df6 = df6[df6.columns[:13]]
            
            df6[''SUB_CUSTOMER_NAME''] = k
            
            df6[''EAN''] = df6[''EAN''].map(lambda x: None if x==''nan'' else x)
            
            df6.dropna(subset=[''EAN''],inplace=True)

            
            # #---------------------------------------------------------------------------

            df7 = excel_file.parse(sheet_name=''관악'',engine=''openpyxl'')


            df7 = df7.applymap(lambda x: str(x))
            #df7 = df7.applymap(lambda x: unidecode(x))
            #print(df7)
            df7.rename(columns={df7.columns[0]:''EAN'',
                                df7.columns[1]:''ARTICLE_NAME'',
                                df7.columns[2]:''NORMAL_SALES'',
                                df7.columns[3]:''UNIT_PRICE'',
                                df7.columns[4]:''DC_RATE'',
                                df7.columns[5]:''QTY'',
                                df7.columns[6]:''EVENT_SALES'',
                                df7.columns[7]:''MARGIN_27'',
                                df7.columns[8]:''MARGIN_22'',
                                df7.columns[9]:''RECEIPT'',
                                df7.columns[10]:''VND_MARGIN_22'',
                                df7.columns[11]:''UNIT_PRC_DIFF'',
                                df7.columns[12]:''FIN_UNIT_PRC''},inplace = True)

            #ean,article_name,normal_sales,unit_price,dc_rate,qty,event_sales,margin_27,margin_22,receipt,vnd_margin_22,unit_prc_diff,fin_unit_prc

            
            k  = df7.iloc[0][''EAN'']
            
            df7 = df7.iloc[3:]
            
            df7 = df7[df7.columns[:13]]
            
            df7[''SUB_CUSTOMER_NAME''] = k
            
            df7[''EAN''] = df7[''EAN''].map(lambda x: None if x==''nan'' else x)
            
            df7.dropna(subset=[''EAN''],inplace=True)

            
            # #--------------------------------------------------------------------------
            df8 = excel_file.parse(sheet_name=''분당'',engine=''openpyxl'')


            df8 = df8.applymap(lambda x: str(x))
            #df8 = df8.applymap(lambda x: unidecode(x))
            #print(df8)
            df8.rename(columns={df8.columns[0]:''EAN'',
                                df8.columns[1]:''ARTICLE_NAME'',
                                df8.columns[2]:''NORMAL_SALES'',
                                df8.columns[3]:''UNIT_PRICE'',
                                df8.columns[4]:''DC_RATE'',
                                df8.columns[5]:''QTY'',
                                df8.columns[6]:''EVENT_SALES'',
                                df8.columns[7]:''MARGIN_27'',
                                df8.columns[8]:''MARGIN_22'',
                                df8.columns[9]:''RECEIPT'',
                                df8.columns[10]:''VND_MARGIN_22'',
                                df8.columns[11]:''UNIT_PRC_DIFF'',
                                df8.columns[12]:''FIN_UNIT_PRC''},inplace = True)

            #ean,article_name,normal_sales,unit_price,dc_rate,qty,event_sales,margin_27,margin_22,receipt,vnd_margin_22,unit_prc_diff,fin_unit_prc

            
            k  = df8.iloc[0][''EAN'']
            
            df8 = df8.iloc[3:]
            
            df8 = df8[df8.columns[:13]]
            
            df8[''SUB_CUSTOMER_NAME''] = k
            
            df8[''EAN''] = df8[''EAN''].map(lambda x: None if x==''nan'' else x)
            
            df8.dropna(subset=[''EAN''],inplace=True)

            
            
            # #-----------------------------------------------------------------------
            df9 = excel_file.parse(sheet_name=''영등포'',engine=''openpyxl'')


            df9 = df9.applymap(lambda x: str(x))
            #df9 = df9.applymap(lambda x: unidecode(x))
            #print(df9)
            df9.rename(columns={df9.columns[0]:''EAN'',
                                df9.columns[1]:''ARTICLE_NAME'',
                                df9.columns[2]:''NORMAL_SALES'',
                                df9.columns[3]:''UNIT_PRICE'',
                                df9.columns[4]:''DC_RATE'',
                                df9.columns[5]:''QTY'',
                                df9.columns[6]:''EVENT_SALES'',
                                df9.columns[7]:''MARGIN_27'',
                                df9.columns[8]:''MARGIN_22'',
                                df9.columns[9]:''RECEIPT'',
                                df9.columns[10]:''VND_MARGIN_22'',
                                df9.columns[11]:''UNIT_PRC_DIFF'',
                                df9.columns[12]:''FIN_UNIT_PRC''},inplace = True)

            #ean,article_name,normal_sales,unit_price,dc_rate,qty,event_sales,margin_27,margin_22,receipt,vnd_margin_22,unit_prc_diff,fin_unit_prc

            
            k  = df9.iloc[0][''EAN'']
            
            df9 = df9.iloc[3:]
            
            df9 = df9[df9.columns[:13]]
            
            df9[''SUB_CUSTOMER_NAME''] = k
            
            df9[''EAN''] = df9[''EAN''].map(lambda x: None if x==''nan'' else x)
            
            df9.dropna(subset=[''EAN''],inplace=True)

            snowparkdf = session.create_dataframe(df9)
            
            # #------------------------------------------------------------------------------------------------------------------

            df10 = excel_file.parse(sheet_name=''강남'',engine=''openpyxl'')


            df10 = df10.applymap(lambda x: str(x))
            #df10 = df10.applymap(lambda x: unidecode(x))
            #print(df10)
            df10.rename(columns={df10.columns[0]:''EAN'',
                                df10.columns[1]:''ARTICLE_NAME'',
                                df10.columns[2]:''NORMAL_SALES'',
                                df10.columns[3]:''UNIT_PRICE'',
                                df10.columns[4]:''DC_RATE'',
                                df10.columns[5]:''QTY'',
                                df10.columns[6]:''EVENT_SALES'',
                                df10.columns[7]:''MARGIN_27'',
                                df10.columns[8]:''MARGIN_22'',
                                df10.columns[9]:''RECEIPT'',
                                df10.columns[10]:''VND_MARGIN_22'',
                                df10.columns[11]:''UNIT_PRC_DIFF'',
                                df10.columns[12]:''FIN_UNIT_PRC''},inplace = True)

            #ean,article_name,normal_sales,unit_price,dc_rate,qty,event_sales,margin_27,margin_22,receipt,vnd_margin_22,unit_prc_diff,fin_unit_prc

            
            k  = df10.iloc[0][''EAN'']
            
            df10 = df10.iloc[3:]
            
            df10 = df10[df10.columns[:13]]
            
            df10[''SUB_CUSTOMER_NAME''] = k
            
            df10[''EAN''] = df10[''EAN''].map(lambda x: None if x==''nan'' else x)
            
            df10.dropna(subset=[''EAN''],inplace=True)
            
            # #------------------------------------------------------------------------------------------
            df11 = excel_file.parse(sheet_name=''노원'',engine=''openpyxl'')


            df11 = df11.applymap(lambda x: str(x))
            #df11 = df11.applymap(lambda x: unidecode(x))
            #print(df11)
            df11.rename(columns={df11.columns[0]:''EAN'',
                                df11.columns[1]:''ARTICLE_NAME'',
                                df11.columns[2]:''NORMAL_SALES'',
                                df11.columns[3]:''UNIT_PRICE'',
                                df11.columns[4]:''DC_RATE'',
                                df11.columns[5]:''QTY'',
                                df11.columns[6]:''EVENT_SALES'',
                                df11.columns[7]:''MARGIN_27'',
                                df11.columns[8]:''MARGIN_22'',
                                df11.columns[9]:''RECEIPT'',
                                df11.columns[10]:''VND_MARGIN_22'',
                                df11.columns[11]:''UNIT_PRC_DIFF'',
                                df11.columns[12]:''FIN_UNIT_PRC''},inplace = True)

            #ean,article_name,normal_sales,unit_price,dc_rate,qty,event_sales,margin_27,margin_22,receipt,vnd_margin_22,unit_prc_diff,fin_unit_prc

            
            k  = df11.iloc[0][''EAN'']
            
            df11 = df11.iloc[3:]
            
            df11 = df11[df11.columns[:13]]
            
            df11[''SUB_CUSTOMER_NAME''] = k
            
            df11[''EAN''] = df11[''EAN''].map(lambda x: None if x==''nan'' else x)
            
            df11.dropna(subset=[''EAN''],inplace=True)
            
            # #--------------------------------------------------------------------
            df12 = excel_file.parse(sheet_name=''미아'',engine=''openpyxl'')


            df12 = df12.applymap(lambda x: str(x))
            #df12 = df12.applymap(lambda x: unidecode(x))
            #print(df12)
            df12.rename(columns={df12.columns[0]:''EAN'',
                                df12.columns[1]:''ARTICLE_NAME'',
                                df12.columns[2]:''NORMAL_SALES'',
                                df12.columns[3]:''UNIT_PRICE'',
                                df12.columns[4]:''DC_RATE'',
                                df12.columns[5]:''QTY'',
                                df12.columns[6]:''EVENT_SALES'',
                                df12.columns[7]:''MARGIN_27'',
                                df12.columns[8]:''MARGIN_22'',
                                df12.columns[9]:''RECEIPT'',
                                df12.columns[10]:''VND_MARGIN_22'',
                                df12.columns[11]:''UNIT_PRC_DIFF'',
                                df12.columns[12]:''FIN_UNIT_PRC''},inplace = True)

            #ean,article_name,normal_sales,unit_price,dc_rate,qty,event_sales,margin_27,margin_22,receipt,vnd_margin_22,unit_prc_diff,fin_unit_prc

            
            k  = df12.iloc[0][''EAN'']
            
            df12 = df12.iloc[3:]
            
            df12 = df12[df12.columns[:13]]
            
            df12[''SUB_CUSTOMER_NAME''] = k
            
            df12[''EAN''] = df12[''EAN''].map(lambda x: None if x==''nan'' else x)
            
            df12.dropna(subset=[''EAN''],inplace=True)
            
            # #-----------------------------------------------------------------------------------

            df13 = excel_file.parse(sheet_name=''건대스타'',engine=''openpyxl'')


            df13 = df13.applymap(lambda x: str(x))
            #df13 = df13.applymap(lambda x: unidecode(x))
            #print(df13)
            df13.rename(columns={df13.columns[0]:''EAN'',
                                df13.columns[1]:''ARTICLE_NAME'',
                                df13.columns[2]:''NORMAL_SALES'',
                                df13.columns[3]:''UNIT_PRICE'',
                                df13.columns[4]:''DC_RATE'',
                                df13.columns[5]:''QTY'',
                                df13.columns[6]:''EVENT_SALES'',
                                df13.columns[7]:''MARGIN_27'',
                                df13.columns[8]:''MARGIN_22'',
                                df13.columns[9]:''RECEIPT'',
                                df13.columns[10]:''VND_MARGIN_22'',
                                df13.columns[11]:''UNIT_PRC_DIFF'',
                                df13.columns[12]:''FIN_UNIT_PRC''},inplace = True)

            #ean,article_name,normal_sales,unit_price,dc_rate,qty,event_sales,margin_27,margin_22,receipt,vnd_margin_22,unit_prc_diff,fin_unit_prc

            
            k  = df13.iloc[0][''EAN'']
            
            df13 = df13.iloc[3:]
            
            df13 = df13[df13.columns[:13]]
            
            df13[''SUB_CUSTOMER_NAME''] = k
            
            df13[''EAN''] = df13[''EAN''].map(lambda x: None if x==''nan'' else x)
            
            df13.dropna(subset=[''EAN''],inplace=True)  
            
            # #-----------------------------------------------------------------------

            df14 = excel_file.parse(sheet_name=''중동'',engine=''openpyxl'')


            df14 = df14.applymap(lambda x: str(x))
            #df14 = df14.applymap(lambda x: unidecode(x))
            #print(df14)
            df14.rename(columns={df14.columns[0]:''EAN'',
                                df14.columns[1]:''ARTICLE_NAME'',
                                df14.columns[2]:''NORMAL_SALES'',
                                df14.columns[3]:''UNIT_PRICE'',
                                df14.columns[4]:''DC_RATE'',
                                df14.columns[5]:''QTY'',
                                df14.columns[6]:''EVENT_SALES'',
                                df14.columns[7]:''MARGIN_27'',
                                df14.columns[8]:''MARGIN_22'',
                                df14.columns[9]:''RECEIPT'',
                                df14.columns[10]:''VND_MARGIN_22'',
                                df14.columns[11]:''UNIT_PRC_DIFF'',
                                df14.columns[12]:''FIN_UNIT_PRC''},inplace = True)

            #ean,article_name,normal_sales,unit_price,dc_rate,qty,event_sales,margin_27,margin_22,receipt,vnd_margin_22,unit_prc_diff,fin_unit_prc

            
            k  = df14.iloc[0][''EAN'']
            
            df14 = df14.iloc[3:]
            
            df14 = df14[df14.columns[:13]]
            
            df14[''SUB_CUSTOMER_NAME''] = k
            
            df14[''EAN''] = df14[''EAN''].map(lambda x: None if x==''nan'' else x)
            
            df14.dropna(subset=[''EAN''],inplace=True)
            
            # #-------------------------------------------------------------------------

            df15 = excel_file.parse(sheet_name=''구리'',engine=''openpyxl'')


            df15 = df15.applymap(lambda x: str(x))
            #df15 = df15.applymap(lambda x: unidecode(x))
            #print(df15)
            df15.rename(columns={df15.columns[0]:''EAN'',
                                df15.columns[1]:''ARTICLE_NAME'',
                                df15.columns[2]:''NORMAL_SALES'',
                                df15.columns[3]:''UNIT_PRICE'',
                                df15.columns[4]:''DC_RATE'',
                                df15.columns[5]:''QTY'',
                                df15.columns[6]:''EVENT_SALES'',
                                df15.columns[7]:''MARGIN_27'',
                                df15.columns[8]:''MARGIN_22'',
                                df15.columns[9]:''RECEIPT'',
                                df15.columns[10]:''VND_MARGIN_22'',
                                df15.columns[11]:''UNIT_PRC_DIFF'',
                                df15.columns[12]:''FIN_UNIT_PRC''},inplace = True)

            #ean,article_name,normal_sales,unit_price,dc_rate,qty,event_sales,margin_27,margin_22,receipt,vnd_margin_22,unit_prc_diff,fin_unit_prc

            
            k  = df15.iloc[0][''EAN'']
            
            df15 = df15.iloc[3:]
            
            df15 = df15[df15.columns[:13]]
            
            df15[''SUB_CUSTOMER_NAME''] = k
            
            df15[''EAN''] = df15[''EAN''].map(lambda x: None if x==''nan'' else x)
            
            df15.dropna(subset=[''EAN''],inplace=True)
            
            # #-------------------------------------------------------------------

            

            df16 = excel_file.parse(sheet_name=''안산'',engine=''openpyxl'')


            df16 = df16.applymap(lambda x: str(x))
            #df16 = df16.applymap(lambda x: unidecode(x))
            #print(df16)
            df16.rename(columns={df16.columns[0]:''EAN'',
                                df16.columns[1]:''ARTICLE_NAME'',
                                df16.columns[2]:''NORMAL_SALES'',
                                df16.columns[3]:''UNIT_PRICE'',
                                df16.columns[4]:''DC_RATE'',
                                df16.columns[5]:''QTY'',
                                df16.columns[6]:''EVENT_SALES'',
                                df16.columns[7]:''MARGIN_27'',
                                df16.columns[8]:''MARGIN_22'',
                                df16.columns[9]:''RECEIPT'',
                                df16.columns[10]:''VND_MARGIN_22'',
                                df16.columns[11]:''UNIT_PRC_DIFF'',
                                df16.columns[12]:''FIN_UNIT_PRC''},inplace = True)

            #ean,article_name,normal_sales,unit_price,dc_rate,qty,event_sales,margin_27,margin_22,receipt,vnd_margin_22,unit_prc_diff,fin_unit_prc

            
            k  = df16.iloc[0][''EAN'']
            
            df16 = df16.iloc[3:]
            
            df16 = df16[df16.columns[:13]]
            
            df16[''SUB_CUSTOMER_NAME''] = k
            
            df16[''EAN''] = df16[''EAN''].map(lambda x: None if x==''nan'' else x)
            
            df16.dropna(subset=[''EAN''],inplace=True)
            
            # #---------------------------------------------------------------------------------

            df17 = excel_file.parse(sheet_name=''평촌'',engine=''openpyxl'')


            df17 = df17.applymap(lambda x: str(x))
            #df17 = df17.applymap(lambda x: unidecode(x))
            #print(df17)
            df17.rename(columns={df17.columns[0]:''EAN'',
                                df17.columns[1]:''ARTICLE_NAME'',
                                df17.columns[2]:''NORMAL_SALES'',
                                df17.columns[3]:''UNIT_PRICE'',
                                df17.columns[4]:''DC_RATE'',
                                df17.columns[5]:''QTY'',
                                df17.columns[6]:''EVENT_SALES'',
                                df17.columns[7]:''MARGIN_27'',
                                df17.columns[8]:''MARGIN_22'',
                                df17.columns[9]:''RECEIPT'',
                                df17.columns[10]:''VND_MARGIN_22'',
                                df17.columns[11]:''UNIT_PRC_DIFF'',
                                df17.columns[12]:''FIN_UNIT_PRC''},inplace = True)

            #ean,article_name,normal_sales,unit_price,dc_rate,qty,event_sales,margin_27,margin_22,receipt,vnd_margin_22,unit_prc_diff,fin_unit_prc

            
            k  = df17.iloc[0][''EAN'']
            
            df17 = df17.iloc[3:]
            
            df17 = df17[df17.columns[:13]]
            
            df17[''SUB_CUSTOMER_NAME''] = k
            
            df17[''EAN''] = df17[''EAN''].map(lambda x: None if x==''nan'' else x)
            
            df17.dropna(subset=[''EAN''],inplace=True)
            
            # #--------------------------------------------------------------------

            df18 = excel_file.parse(sheet_name=''수원'',engine=''openpyxl'')


            df18 = df18.applymap(lambda x: str(x))
            #df18 = df18.applymap(lambda x: unidecode(x))
            #print(df18)
            df18.rename(columns={df18.columns[0]:''EAN'',
                                df18.columns[1]:''ARTICLE_NAME'',
                                df18.columns[2]:''NORMAL_SALES'',
                                df18.columns[3]:''UNIT_PRICE'',
                                df18.columns[4]:''DC_RATE'',
                                df18.columns[5]:''QTY'',
                                df18.columns[6]:''EVENT_SALES'',
                                df18.columns[7]:''MARGIN_27'',
                                df18.columns[8]:''MARGIN_22'',
                                df18.columns[9]:''RECEIPT'',
                                df18.columns[10]:''VND_MARGIN_22'',
                                df18.columns[11]:''UNIT_PRC_DIFF'',
                                df18.columns[12]:''FIN_UNIT_PRC''},inplace = True)

            #ean,article_name,normal_sales,unit_price,dc_rate,qty,event_sales,margin_27,margin_22,receipt,vnd_margin_22,unit_prc_diff,fin_unit_prc

            
            k  = df18.iloc[0][''EAN'']
            
            df18 = df18.iloc[3:]
            
            df18 = df18[df18.columns[:13]]
            
            df18[''SUB_CUSTOMER_NAME''] = k
            
            df18[''EAN''] = df18[''EAN''].map(lambda x: None if x==''nan'' else x)
            
            df18.dropna(subset=[''EAN''],inplace=True)
        #-----------------------------------------------------------------------------------------------------------------------

            df19 = excel_file.parse(sheet_name=''동탄'',engine=''openpyxl'')


            df19 = df19.applymap(lambda x: str(x))
            #df19 = df19.applymap(lambda x: unidecode(x))
            #print(df19)
            df19.rename(columns={df19.columns[0]:''EAN'',
                                df19.columns[1]:''ARTICLE_NAME'',
                                df19.columns[2]:''NORMAL_SALES'',
                                df19.columns[3]:''UNIT_PRICE'',
                                df19.columns[4]:''DC_RATE'',
                                df19.columns[5]:''QTY'',
                                df19.columns[6]:''EVENT_SALES'',
                                df19.columns[7]:''MARGIN_27'',
                                df19.columns[8]:''MARGIN_22'',
                                df19.columns[9]:''RECEIPT'',
                                df19.columns[10]:''VND_MARGIN_22'',
                                df19.columns[11]:''UNIT_PRC_DIFF'',
                                df19.columns[12]:''FIN_UNIT_PRC''},inplace = True)

            #ean,article_name,normal_sales,unit_price,dc_rate,qty,event_sales,margin_27,margin_22,receipt,vnd_margin_22,unit_prc_diff,fin_unit_prc

            
            k  = df19.iloc[0][''EAN'']
            
            df19 = df19.iloc[3:]
            
            df19 = df19[df19.columns[:13]]
            
            df19[''SUB_CUSTOMER_NAME''] = k
            
            df19[''EAN''] = df19[''EAN''].map(lambda x: None if x==''nan'' else x)
            
            df19.dropna(subset=[''EAN''],inplace=True)
        
            #-------------------------------------------------------------------------------------------

            df20 = excel_file.parse(sheet_name=''부산'',engine=''openpyxl'')


            df20 = df20.applymap(lambda x: str(x))
            #df20 = df20.applymap(lambda x: unidecode(x))
            #print(df20)
            df20.rename(columns={df20.columns[0]:''EAN'',
                                df20.columns[1]:''ARTICLE_NAME'',
                                df20.columns[2]:''NORMAL_SALES'',
                                df20.columns[3]:''UNIT_PRICE'',
                                df20.columns[4]:''DC_RATE'',
                                df20.columns[5]:''QTY'',
                                df20.columns[6]:''EVENT_SALES'',
                                df20.columns[7]:''MARGIN_27'',
                                df20.columns[8]:''MARGIN_22'',
                                df20.columns[9]:''RECEIPT'',
                                df20.columns[10]:''VND_MARGIN_22'',
                                df20.columns[11]:''UNIT_PRC_DIFF'',
                                df20.columns[12]:''FIN_UNIT_PRC''},inplace = True)

            #ean,article_name,normal_sales,unit_price,dc_rate,qty,event_sales,margin_27,margin_22,receipt,vnd_margin_22,unit_prc_diff,fin_unit_prc

            
            k  = df20.iloc[0][''EAN'']
            
            df20 = df20.iloc[3:]
            
            df20 = df20[df20.columns[:13]]
            
            df20[''SUB_CUSTOMER_NAME''] = k
            
            df20[''EAN''] = df20[''EAN''].map(lambda x: None if x==''nan'' else x)
            
            df20.dropna(subset=[''EAN''],inplace=True)
            
            #---------------------------------------------------------------------
            df21 = excel_file.parse(sheet_name=''대전'',engine=''openpyxl'')


            df21 = df21.applymap(lambda x: str(x))
            #df21 = df21.applymap(lambda x: unidecode(x))
            #print(df21)
            df21.rename(columns={df21.columns[0]:''EAN'',
                                df21.columns[1]:''ARTICLE_NAME'',
                                df21.columns[2]:''NORMAL_SALES'',
                                df21.columns[3]:''UNIT_PRICE'',
                                df21.columns[4]:''DC_RATE'',
                                df21.columns[5]:''QTY'',
                                df21.columns[6]:''EVENT_SALES'',
                                df21.columns[7]:''MARGIN_27'',
                                df21.columns[8]:''MARGIN_22'',
                                df21.columns[9]:''RECEIPT'',
                                df21.columns[10]:''VND_MARGIN_22'',
                                df21.columns[11]:''UNIT_PRC_DIFF'',
                                df21.columns[12]:''FIN_UNIT_PRC''},inplace = True)

            #ean,article_name,normal_sales,unit_price,dc_rate,qty,event_sales,margin_27,margin_22,receipt,vnd_margin_22,unit_prc_diff,fin_unit_prc

            
            k  = df21.iloc[0][''EAN'']
            
            df21 = df21.iloc[3:]
            
            df21 = df21[df21.columns[:13]]
            
            df21[''SUB_CUSTOMER_NAME''] = k
            
            df21[''EAN''] = df21[''EAN''].map(lambda x: None if x==''nan'' else x)
            
            df21.dropna(subset=[''EAN''],inplace=True)
        #----------------------------------------------------------------------------------------------------

            df22 = excel_file.parse(sheet_name=''광주'',engine=''openpyxl'')


            df22 = df22.applymap(lambda x: str(x))
            #df22 = df22.applymap(lambda x: unidecode(x))
            #print(df22)
            df22.rename(columns={df22.columns[0]:''EAN'',
                                df22.columns[1]:''ARTICLE_NAME'',
                                df22.columns[2]:''NORMAL_SALES'',
                                df22.columns[3]:''UNIT_PRICE'',
                                df22.columns[4]:''DC_RATE'',
                                df22.columns[5]:''QTY'',
                                df22.columns[6]:''EVENT_SALES'',
                                df22.columns[7]:''MARGIN_27'',
                                df22.columns[8]:''MARGIN_22'',
                                df22.columns[9]:''RECEIPT'',
                                df22.columns[10]:''VND_MARGIN_22'',
                                df22.columns[11]:''UNIT_PRC_DIFF'',
                                df22.columns[12]:''FIN_UNIT_PRC''},inplace = True)

            #ean,article_name,normal_sales,unit_price,dc_rate,qty,event_sales,margin_27,margin_22,receipt,vnd_margin_22,unit_prc_diff,fin_unit_prc

            
            k  = df22.iloc[0][''EAN'']
            
            df22 = df22.iloc[3:]
            
            df22 = df22[df22.columns[:13]]
            
            df22[''SUB_CUSTOMER_NAME''] = k
            
            df22[''EAN''] = df22[''EAN''].map(lambda x: None if x==''nan'' else x)
            
            df22.dropna(subset=[''EAN''],inplace=True)

        #-------------------------------------------------------------------------------------------------------

            df23 = excel_file.parse(sheet_name=''포항'',engine=''openpyxl'')


            df23 = df23.applymap(lambda x: str(x))
            #df23 = df23.applymap(lambda x: unidecode(x))
            #print(df23)
            df23.rename(columns={df23.columns[0]:''EAN'',
                                df23.columns[1]:''ARTICLE_NAME'',
                                df23.columns[2]:''NORMAL_SALES'',
                                df23.columns[3]:''UNIT_PRICE'',
                                df23.columns[4]:''DC_RATE'',
                                df23.columns[5]:''QTY'',
                                df23.columns[6]:''EVENT_SALES'',
                                df23.columns[7]:''MARGIN_27'',
                                df23.columns[8]:''MARGIN_22'',
                                df23.columns[9]:''RECEIPT'',
                                df23.columns[10]:''VND_MARGIN_22'',
                                df23.columns[11]:''UNIT_PRC_DIFF'',
                                df23.columns[12]:''FIN_UNIT_PRC''},inplace = True)

            #ean,article_name,normal_sales,unit_price,dc_rate,qty,event_sales,margin_27,margin_22,receipt,vnd_margin_22,unit_prc_diff,fin_unit_prc

            
            k  = df23.iloc[0][''EAN'']
            
            df23 = df23.iloc[3:]
            
            df23 = df23[df23.columns[:13]]
            
            df23[''SUB_CUSTOMER_NAME''] = k
            
            df23[''EAN''] = df23[''EAN''].map(lambda x: None if x==''nan'' else x)
            
            df23.dropna(subset=[''EAN''],inplace=True)

        #-----------------------------------------------------------------------------

            df24 = excel_file.parse(sheet_name=''울산'',engine=''openpyxl'')


            df24 = df24.applymap(lambda x: str(x))
            #df24 = df24.applymap(lambda x: unidecode(x))
            #print(df24)
            df24.rename(columns={df24.columns[0]:''EAN'',
                                df24.columns[1]:''ARTICLE_NAME'',
                                df24.columns[2]:''NORMAL_SALES'',
                                df24.columns[3]:''UNIT_PRICE'',
                                df24.columns[4]:''DC_RATE'',
                                df24.columns[5]:''QTY'',
                                df24.columns[6]:''EVENT_SALES'',
                                df24.columns[7]:''MARGIN_27'',
                                df24.columns[8]:''MARGIN_22'',
                                df24.columns[9]:''RECEIPT'',
                                df24.columns[10]:''VND_MARGIN_22'',
                                df24.columns[11]:''UNIT_PRC_DIFF'',
                                df24.columns[12]:''FIN_UNIT_PRC''},inplace = True)

            #ean,article_name,normal_sales,unit_price,dc_rate,qty,event_sales,margin_27,margin_22,receipt,vnd_margin_22,unit_prc_diff,fin_unit_prc

            
            k  = df24.iloc[0][''EAN'']
            
            df24 = df24.iloc[3:]
            
            df24 = df24[df24.columns[:13]]
            
            df24[''SUB_CUSTOMER_NAME''] = k
            
            df24[''EAN''] = df24[''EAN''].map(lambda x: None if x==''nan'' else x)
            
            df24.dropna(subset=[''EAN''],inplace=True)

        #------------------------------------------------------------------------------------

            df25 = excel_file.parse(sheet_name=''동래'',engine=''openpyxl'')


            df25 = df25.applymap(lambda x: str(x))
            #df25 = df25.applymap(lambda x: unidecode(x))
            #print(df25)
            df25.rename(columns={df25.columns[0]:''EAN'',
                                df25.columns[1]:''ARTICLE_NAME'',
                                df25.columns[2]:''NORMAL_SALES'',
                                df25.columns[3]:''UNIT_PRICE'',
                                df25.columns[4]:''DC_RATE'',
                                df25.columns[5]:''QTY'',
                                df25.columns[6]:''EVENT_SALES'',
                                df25.columns[7]:''MARGIN_27'',
                                df25.columns[8]:''MARGIN_22'',
                                df25.columns[9]:''RECEIPT'',
                                df25.columns[10]:''VND_MARGIN_22'',
                                df25.columns[11]:''UNIT_PRC_DIFF'',
                                df25.columns[12]:''FIN_UNIT_PRC''},inplace = True)

            #ean,article_name,normal_sales,unit_price,dc_rate,qty,event_sales,margin_27,margin_22,receipt,vnd_margin_22,unit_prc_diff,fin_unit_prc

            
            k  = df25.iloc[0][''EAN'']
            
            df25 = df25.iloc[3:]
            
            df25 = df25[df25.columns[:13]]
            
            df25[''SUB_CUSTOMER_NAME''] = k
            
            df25[''EAN''] = df25[''EAN''].map(lambda x: None if x==''nan'' else x)
            
            df25.dropna(subset=[''EAN''],inplace=True)

        #--------------------------------------------------------------------------------------------------

            df26 = excel_file.parse(sheet_name=''창원'',engine=''openpyxl'')


            df26 = df26.applymap(lambda x: str(x))
            #df26 = df26.applymap(lambda x: unidecode(x))
            #print(df26)
            df26.rename(columns={df26.columns[0]:''EAN'',
                                df26.columns[1]:''ARTICLE_NAME'',
                                df26.columns[2]:''NORMAL_SALES'',
                                df26.columns[3]:''UNIT_PRICE'',
                                df26.columns[4]:''DC_RATE'',
                                df26.columns[5]:''QTY'',
                                df26.columns[6]:''EVENT_SALES'',
                                df26.columns[7]:''MARGIN_27'',
                                df26.columns[8]:''MARGIN_22'',
                                df26.columns[9]:''RECEIPT'',
                                df26.columns[10]:''VND_MARGIN_22'',
                                df26.columns[11]:''UNIT_PRC_DIFF'',
                                df26.columns[12]:''FIN_UNIT_PRC''},inplace = True)

            #ean,article_name,normal_sales,unit_price,dc_rate,qty,event_sales,margin_27,margin_22,receipt,vnd_margin_22,unit_prc_diff,fin_unit_prc

            
            k  = df26.iloc[0][''EAN'']
            
            df26 = df26.iloc[3:]
            
            df26 = df26[df26.columns[:13]]
            
            df26[''SUB_CUSTOMER_NAME''] = k
            
            df26[''EAN''] = df26[''EAN''].map(lambda x: None if x==''nan'' else x)
            
            df26.dropna(subset=[''EAN''],inplace=True)

        #----------------------------------------------------------------------

            df27 = excel_file.parse(sheet_name=''대구'',engine=''openpyxl'')


            df27 = df27.applymap(lambda x: str(x))
            #df27 = df27.applymap(lambda x: unidecode(x))
            #print(df27)
            df27.rename(columns={df27.columns[0]:''EAN'',
                                df27.columns[1]:''ARTICLE_NAME'',
                                df27.columns[2]:''NORMAL_SALES'',
                                df27.columns[3]:''UNIT_PRICE'',
                                df27.columns[4]:''DC_RATE'',
                                df27.columns[5]:''QTY'',
                                df27.columns[6]:''EVENT_SALES'',
                                df27.columns[7]:''MARGIN_27'',
                                df27.columns[8]:''MARGIN_22'',
                                df27.columns[9]:''RECEIPT'',
                                df27.columns[10]:''VND_MARGIN_22'',
                                df27.columns[11]:''UNIT_PRC_DIFF'',
                                df27.columns[12]:''FIN_UNIT_PRC''},inplace = True)

            #ean,article_name,normal_sales,unit_price,dc_rate,qty,event_sales,margin_27,margin_22,receipt,vnd_margin_22,unit_prc_diff,fin_unit_prc

            
            k  = df27.iloc[0][''EAN'']
            
            df27 = df27.iloc[3:]
            
            df27 = df27[df27.columns[:13]]
            
            df27[''SUB_CUSTOMER_NAME''] = k
            
            df27[''EAN''] = df27[''EAN''].map(lambda x: None if x==''nan'' else x)
            
            df27.dropna(subset=[''EAN''],inplace=True)

        #----------------------------------------------------------------------

            df28 = excel_file.parse(sheet_name=''상인'',engine=''openpyxl'')


            df28 = df28.applymap(lambda x: str(x))
            #df28 = df28.applymap(lambda x: unidecode(x))
            #print(df28)
            df28.rename(columns={df28.columns[0]:''EAN'',
                                df28.columns[1]:''ARTICLE_NAME'',
                                df28.columns[2]:''NORMAL_SALES'',
                                df28.columns[3]:''UNIT_PRICE'',
                                df28.columns[4]:''DC_RATE'',
                                df28.columns[5]:''QTY'',
                                df28.columns[6]:''EVENT_SALES'',
                                df28.columns[7]:''MARGIN_27'',
                                df28.columns[8]:''MARGIN_22'',
                                df28.columns[9]:''RECEIPT'',
                                df28.columns[10]:''VND_MARGIN_22'',
                                df28.columns[11]:''UNIT_PRC_DIFF'',
                                df28.columns[12]:''FIN_UNIT_PRC''},inplace = True)

            #ean,article_name,normal_sales,unit_price,dc_rate,qty,event_sales,margin_27,margin_22,receipt,vnd_margin_22,unit_prc_diff,fin_unit_prc

            
            k  = df28.iloc[0][''EAN'']
            
            df28 = df28.iloc[3:]
            
            df28 = df28[df28.columns[:13]]
            
            df28[''SUB_CUSTOMER_NAME''] = k
            
            df28[''EAN''] = df28[''EAN''].map(lambda x: None if x==''nan'' else x)
            
            df28.dropna(subset=[''EAN''],inplace=True)

        #------------------------------------------------------------------------------------------

            df29 = excel_file.parse(sheet_name=''센텀'',engine=''openpyxl'')


            df29 = df29.applymap(lambda x: str(x))
            #df29 = df29.applymap(lambda x: unidecode(x))
            #print(df29)
            df29.rename(columns={df29.columns[0]:''EAN'',
                                df29.columns[1]:''ARTICLE_NAME'',
                                df29.columns[2]:''NORMAL_SALES'',
                                df29.columns[3]:''UNIT_PRICE'',
                                df29.columns[4]:''DC_RATE'',
                                df29.columns[5]:''QTY'',
                                df29.columns[6]:''EVENT_SALES'',
                                df29.columns[7]:''MARGIN_27'',
                                df29.columns[8]:''MARGIN_22'',
                                df29.columns[9]:''RECEIPT'',
                                df29.columns[10]:''VND_MARGIN_22'',
                                df29.columns[11]:''UNIT_PRC_DIFF'',
                                df29.columns[12]:''FIN_UNIT_PRC''},inplace = True)

            #ean,article_name,normal_sales,unit_price,dc_rate,qty,event_sales,margin_27,margin_22,receipt,vnd_margin_22,unit_prc_diff,fin_unit_prc

            
            k  = df29.iloc[0][''EAN'']
            
            df29 = df29.iloc[3:]
            
            df29 = df29[df29.columns[:13]]
            
            df29[''SUB_CUSTOMER_NAME''] = k
            
            df29[''EAN''] = df29[''EAN''].map(lambda x: None if x==''nan'' else x)
            
            df29.dropna(subset=[''EAN''],inplace=True)

        #----------------------------------------------------------------------------------------------------

            df30 = excel_file.parse(sheet_name=''광복'',engine=''openpyxl'')


            df30 = df30.applymap(lambda x: str(x))
            #df30 = df30.applymap(lambda x: unidecode(x))
            #print(df30)
            df30.rename(columns={df30.columns[0]:''EAN'',
                                df30.columns[1]:''ARTICLE_NAME'',
                                df30.columns[2]:''NORMAL_SALES'',
                                df30.columns[3]:''UNIT_PRICE'',
                                df30.columns[4]:''DC_RATE'',
                                df30.columns[5]:''QTY'',
                                df30.columns[6]:''EVENT_SALES'',
                                df30.columns[7]:''MARGIN_27'',
                                df30.columns[8]:''MARGIN_22'',
                                df30.columns[9]:''RECEIPT'',
                                df30.columns[10]:''VND_MARGIN_22'',
                                df30.columns[11]:''UNIT_PRC_DIFF'',
                                df30.columns[12]:''FIN_UNIT_PRC''},inplace = True)

            #ean,article_name,normal_sales,unit_price,dc_rate,qty,event_sales,margin_27,margin_22,receipt,vnd_margin_22,unit_prc_diff,fin_unit_prc

            
            k  = df30.iloc[0][''EAN'']
            
            df30 = df30.iloc[3:]
            
            df30 = df30[df30.columns[:13]]
            
            df30[''SUB_CUSTOMER_NAME''] = k
            
            df30[''EAN''] = df30[''EAN''].map(lambda x: None if x==''nan'' else x)
            
            df30.dropna(subset=[''EAN''],inplace=True)

        #----------------------------------------------------------------------------------------

            df31 = excel_file.parse(sheet_name=''마산'',engine=''openpyxl'')


            df31 = df31.applymap(lambda x: str(x))
            #df31 = df31.applymap(lambda x: unidecode(x))
            #print(df31)
            df31.rename(columns={df31.columns[0]:''EAN'',
                                df31.columns[1]:''ARTICLE_NAME'',
                                df31.columns[2]:''NORMAL_SALES'',
                                df31.columns[3]:''UNIT_PRICE'',
                                df31.columns[4]:''DC_RATE'',
                                df31.columns[5]:''QTY'',
                                df31.columns[6]:''EVENT_SALES'',
                                df31.columns[7]:''MARGIN_27'',
                                df31.columns[8]:''MARGIN_22'',
                                df31.columns[9]:''RECEIPT'',
                                df31.columns[10]:''VND_MARGIN_22'',
                                df31.columns[11]:''UNIT_PRC_DIFF'',
                                df31.columns[12]:''FIN_UNIT_PRC''},inplace = True)

            #ean,article_name,normal_sales,unit_price,dc_rate,qty,event_sales,margin_27,margin_22,receipt,vnd_margin_22,unit_prc_diff,fin_unit_prc

            
            k  = df31.iloc[0][''EAN'']
            
            df31 = df31.iloc[3:]
            
            df31 = df31[df31.columns[:13]]
            
            df31[''SUB_CUSTOMER_NAME''] = k
            
            df31[''EAN''] = df31[''EAN''].map(lambda x: None if x==''nan'' else x)
            
            df31.dropna(subset=[''EAN''],inplace=True)

        #-------------------------------------------------------------------------------

            df32 = excel_file.parse(sheet_name=''인천'',engine=''openpyxl'')


            df32 = df32.applymap(lambda x: str(x))
            #df32 = df32.applymap(lambda x: unidecode(x))
            #print(df32)
            df32.rename(columns={df32.columns[0]:''EAN'',
                                df32.columns[1]:''ARTICLE_NAME'',
                                df32.columns[2]:''NORMAL_SALES'',
                                df32.columns[3]:''UNIT_PRICE'',
                                df32.columns[4]:''DC_RATE'',
                                df32.columns[5]:''QTY'',
                                df32.columns[6]:''EVENT_SALES'',
                                df32.columns[7]:''MARGIN_27'',
                                df32.columns[8]:''MARGIN_22'',
                                df32.columns[9]:''RECEIPT'',
                                df32.columns[10]:''VND_MARGIN_22'',
                                df32.columns[11]:''UNIT_PRC_DIFF'',
                                df32.columns[12]:''FIN_UNIT_PRC''},inplace = True)

            #ean,article_name,normal_sales,unit_price,dc_rate,qty,event_sales,margin_27,margin_22,receipt,vnd_margin_22,unit_prc_diff,fin_unit_prc

            
            k  = df32.iloc[0][''EAN'']
            
            df32 = df32.iloc[3:]
            
            df32 = df32[df32.columns[:13]]
            
            df32[''SUB_CUSTOMER_NAME''] = k
            
            df32[''EAN''] = df32[''EAN''].map(lambda x: None if x==''nan'' else x)
            
            df32.dropna(subset=[''EAN''],inplace=True)
            
            df = pd.concat([df1,df2,df3,df4,df5,df6,df7,df8,df9,df10,df11,df12,df13,df14,df15,df16,df17,df18,df19,df20,df21,df22,df23,df24,df25,df26,df27,df28,df29,df30,df31,df32],axis = 0)

            #df[''NORMAL_SALES''] = df[''NORMAL_SALES''].map(lambda x:str(round(float(x))))
            
            snowdf = session.create_dataframe(df)

            snowdf = snowdf.na.drop(''all'')

            #Checking if dataframe is having any data
            
            if snowdf.count()==0:
                return "No Data in file"

            def returnimx_txn_dt(file_name):
                return file_name.split(''.'')[0].split(''_'')[3]

            def returncust_cd(file_name):
                return file_name.split(''.'')[0].split(''_'')[2]
            

            snowdf =  snowdf.with_column(''dstr_nm'',lit(''LOTTE_AK''))
            
            snowdf =  snowdf.with_column(''ims_txn_dt'',lit(returnimx_txn_dt(file_name)))
            
            snowdf = snowdf.with_column(''cust_cd'',lit(returncust_cd(file_name)))

            snowparkdf = snowdf.select(''dstr_nm'',
                                       ''ims_txn_dt'',
                                       ''ean'',
                                       ''article_name'',
                                       ''normal_sales'',
                                       ''unit_price'',
                                       ''dc_rate'',
                                       ''qty'',
                                       ''event_sales'',
                                       ''margin_27'',
                                       ''margin_22'',
                                       ''receipt'',
                                       ''vnd_margin_22'',
                                       ''unit_prc_diff'',
                                       ''fin_unit_prc'',
                                       ''sub_customer_name'',
                                       ''cust_cd'')

            snowparkdf.write.mode("append").saveAsTable(target_table)

            current_date = datetime.now()
            formatted_year = current_date.strftime("%Y")
            formatted_month = current_date.strftime("%m")

             # write to success folder
            file_name = file_name.split(''.'')[0]+"_"+datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")

            

            snowparkdf.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)

            

            

            return ''Success''
            

    except KeyError as key_error:
        # Handle KeyError (missing columns) here
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message
        
    except Exception as e:
        # Handle exceptions here
        error_message = f"Error: {str(e)}"
        return error_message;';
		
		

CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.NTASDL_RAW.GT_SELLOUT_LOTTE_LOGISTICS_YANG_JU_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('openpyxl==3.1.2','snowflake-snowpark-python==*')
HANDLER = 'main'
EXECUTE AS OWNER
AS '# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them.

import snowflake.snowpark as snowpark
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
#from unidecode import unidecode

from re import sub

def main(session: snowpark.Session,Param):
    

    try:

        #Param=[''LOTTE LOGISTICS YANG JU_122565_202402.xlsx'',''NTASDL_RAW.DEV_LOAD_STAGE_ADLS_KOR'',''dev/gt_sellout/transaction_files/lotte_logistics_yang_ju/'',''sdl_kr_lotte_logistics_yang_ju_gt_sellout'']

        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        file_header_loc = 10

        #Reading data from adls
        full_path = "@"+stage_name+"/"+temp_stage_path+"/"+file_name
        
        with SnowflakeFile.open(full_path,''rb'',require_scoped_url= False) as f:
            excel_file = pd.ExcelFile(f)
            #print(excel_file)
            
            df1 = excel_file.parse(sheet_name=''H&B'',engine=''openpyxl'')


            df1 = df1.applymap(lambda x: str(x))

            
            df1 = df1[2:]

            

            # snowdf = session.create_dataframe(df1)
            #df1 = df1.applymap(lambda x: unidecode(x))
            #print(df1)
            df1.rename(columns={df1.columns[0]:''HEAVY_CLASSIFICATION'',
                                df1.columns[1]:''SUB_CLASSIFICATION'',
                                df1.columns[2]:''CODE'',
                                df1.columns[3]:''EAN'',
                                df1.columns[4]:''PRODUCT_NAME'',
                                df1.columns[5]:''ACCOUNT_NAME'',
                                df1.columns[6]:''LDU'',
                                df1.columns[7]:''SUPPLY_DIVISION'',
                                df1.columns[8]:''SLS_QTY'',
                                df1.columns[9]:''SLS_AMT'',
                                df1.columns[10]:''SALES_PRIORITY'',
                                df1.columns[11]:''SALES_STORES'',
                                df1.columns[12]:''FIN_UNIT_PRC''
                                },inplace = True)

            df1 = df1.applymap(lambda x: None if x==''nan'' else x)

            
            
            

            
#----------------------------------------------------------------------------------------------------------------------------------------------
            df2 = excel_file.parse(sheet_name=''미용,화장품'',engine=''openpyxl'')


            df2 = df2.applymap(lambda x: str(x))

            
            df2 = df2[2:]

            

            # snowdf = session.create_dataframe(df2)
            #df2 = df2.applymap(lambda x: unidecode(x))
            #print(df2)
            df2.rename(columns={df2.columns[0]:''HEAVY_CLASSIFICATION'',
                                df2.columns[1]:''SUB_CLASSIFICATION'',
                                df2.columns[2]:''CODE'',
                                df2.columns[3]:''EAN'',
                                df2.columns[4]:''PRODUCT_NAME'',
                                df2.columns[5]:''ACCOUNT_NAME'',
                                df2.columns[6]:''LDU'',
                                df2.columns[7]:''SUPPLY_DIVISION'',
                                df2.columns[8]:''SLS_QTY'',
                                df2.columns[9]:''SLS_AMT'',
                                df2.columns[10]:''SALES_PRIORITY'',
                                df2.columns[11]:''SALES_STORES'',
                                df2.columns[12]:''SALES_RATE''
                                },inplace = True)

            df2 = df2.applymap(lambda x: None if x==''nan'' else x)

           
            # # #---------------------------------------------------------------------------------------------------------------------------------------

           
            
            df = pd.concat([df1,df2],axis = 0)

            #df[''NORMAL_SALES''] = df[''NORMAL_SALES''].map(lambda x:str(round(float(x))))
            
            snowdf = session.create_dataframe(df)

            snowdf = snowdf.na.drop(''all'')

            #Checking if dataframe is having any data
            
            if snowdf.count()==0:
                return "No Data in file"

            def returnimx_txn_dt(file_name):
                return file_name.split(''.'')[0].split(''_'')[2]

            def returncust_cd(file_name):
                return file_name.split(''.'')[0].split(''_'')[1]
            

            snowdf =  snowdf.with_column(''dstr_nm'',lit(''LOTTE LOGISTICS YANG JU''))
            
            snowdf =  snowdf.with_column(''ims_txn_dt'',lit(returnimx_txn_dt(file_name)))
            
            snowdf = snowdf.with_column(''cust_cd'',lit(returncust_cd(file_name)))

            snowparkdf = snowdf.select(''dstr_nm'',
                                       ''ims_txn_dt'',
                                       ''heavy_classification'',
                                       ''sub_classification'',
                                       ''code'',
                                       ''ean'',
                                       ''product_name'',
                                       ''account_name'',
                                       ''ldu'',
                                       ''supply_division'',
                                       ''sls_qty'',
                                       ''sls_amt'',
                                       ''sales_priority'',
                                       ''sales_stores'',
                                       ''sales_rate'',
                                       ''cust_cd'')

            snowparkdf.write.mode("append").saveAsTable(target_table)

            current_date = datetime.now()
            formatted_year = current_date.strftime("%Y")
            formatted_month = current_date.strftime("%m")

             # write to success folder
            file_name = file_name.split(''.'')[0]+"_"+datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")

            

            snowparkdf.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)

            

            

            return ''Success''
            

    except KeyError as key_error:
        # Handle KeyError (missing columns) here
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message
        
    except Exception as e:
        # Handle exceptions here
        error_message = f"Error: {str(e)}"
        return error_message;';
		
CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.NTASDL_RAW.SDL_KR_ECOM_COUPANG_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('openpyxl==3.1.2','snowflake-snowpark-python==*','xlrd==2.0.1')
HANDLER = 'main'
EXECUTE AS OWNER
AS 'import snowflake.snowpark
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz
import sys
import pandas as pd
import logging
from datetime import datetime
import collections
from pathlib import Path
import openpyxl as xl
from snowflake.snowpark.files import SnowflakeFile
from snowflake.snowpark.functions import col, lit, current_timestamp, trim, upper
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DecimalType,DateType

def main(session: snowpark.Session,Param):
    #Param=["KR_Coupang_20240527.xlsx","NTASDL_RAW.DEV_LOAD_STAGE_ADLS_kor","dev/ecommerce_sellout/Coupang3P","sdl_kr_ECOM_COUPANG"]
    try:
        file_name = Param[0].split(".")[0]+".xlsx"
        stage_name = Param[1]
        temp_stage_path = Param[2]
        target_table = Param[3]

       

        full_path = "@"+stage_name+"/"+temp_stage_path+"/"+file_name
        with SnowflakeFile.open(full_path, "rb", require_scoped_url = False) as f:
            df1 = pd.read_excel(f,sheet_name=0,names=[''EAN_Number'', ''Seller'', ''Order_date'', ''Seller_product_code'', ''Seller_product_name'', ''product_name'', ''Brand'', ''Seller_product_option'', ''Order_Qty'', ''Product_Qty'', ''Product_value'', ''Invoice_Value'', ''delivery_service_company'', ''Shipment'', ''Delivery_date'']
)
            df1["cust_nm"]=''(JU) UNITOA_COUPANG''
            df1["a"]=None
            df1["b"]=None
            df=pd.DataFrame(df1[[''cust_nm'',''a'',''b'',''EAN_Number'',''Brand'',''product_name'',''Delivery_date'',''Product_Qty'',''Invoice_Value'']])
            #df[''Delivery_date''] = df[''Delivery_date''].astype(str).str.slice(0,10).str.replace("-","/")
            df[''Delivery_date''] = pd.to_datetime(df[''Delivery_date'']).dt.strftime(''%Y/%m/%d'')
            df[''Delivery_date''] = pd.to_datetime(df[''Delivery_date'']).dt.strftime(''%d/%m/%y'')
            #df=df1["EAN_Number"]
        df=session.create_dataframe(df)
        # df = df.withColumn("cust_nm", lit(''(JU) UNITOA_COUPANG''))
        # df = df.withColumn("a", lit(None))
        # df = df.withColumn("b", lit(None))
        df = df.withColumn("crt_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        df = df.withColumn("filename", lit(file_name.split(".")[0]+"_"+datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")+".csv"))
        df = df.withColumn("run_id", lit(None))
        

        #snowdf = df.select(
         #       ''cust_nm'',''a'',''b'',''EAN_Number'',''Brand'',''product_name'',''Delivery_date'',''Product_Qty'',''Invoice_Value'',''crt_dttm'',''filename'',''run_id''
         #)
        
        snowdf=df
        #return snowdf
        file_name = file_name.split(".")[0] + "_" + datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        

        
        
        snowdf.write.mode("append").saveAsTable(stage_name.split(".")[0] + "." + target_table)
        
        snowdf.write.copy_into_location("@" + stage_name + "/" + temp_stage_path + "/processed/success/" + formatted_year + "/" + formatted_month + "/" + file_name, header=True, OVERWRITE=True)
        
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
		
		

CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.NTASDL_RAW.J_KR_ECOMMERCE_SELLOUT_TCA_INGESTION_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('openpyxl==3.1.2','snowflake-snowpark-python==*')
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
#from unidecode import unidecode

from re import sub

def main(session: snowpark.Session,Param):
    

    try:

        #Param=[''Weekly_Summary_TCA_raw_data_20240507.xlsx'',''NTASDL_RAW.DEV_LOAD_STAGE_ADLS_KOR'',''dev/ecommerce_sellout/tca/'',''sdl_kr_ecommerce_tca_sellout'']

        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        file_header_loc = 10

        #Reading data from adls
        full_path = "@"+stage_name+"/"+temp_stage_path+"/"+file_name
        
        with SnowflakeFile.open(full_path,''rb'',require_scoped_url= False) as f:
            df = pd.read_excel(f)

            df = df.applymap(lambda x: str(x))

            df = df[4:][df.columns[1:]]

            df.rename(columns = {df.columns[0]:''CUSTOMER_NAME'',
                                 df.columns[1]:''SUB_CUSTOMER_NAME'',
                                 df.columns[2]:''EAN_NUMBER'',
                                 df.columns[3]:''SAP_CODE'',
                                 df.columns[4]:''SKU_TYPE'',
                                 df.columns[5]: ''BRAND_CATEGORIES'',
                                 df.columns[6]: ''PRODUCT_NAME'',
                                 df.columns[7]: ''YEAR'',
                                 df.columns[8]: ''MONTH'',
                                 df.columns[9]: ''WEEK'',
                                 df.columns[10]:''TRANSACTION_DATE'',
                                 df.columns[11]:''SELLOUT_QTY'',
                                 df.columns[12]:''SELLOUT_AMOUNT'',
                                 df.columns[13]:''SOLD_TO''},inplace=True)

            def tranaction_date_return(strdate):
                return datetime.strptime(strdate,''%Y%m%d'')

            df[''TRANSACTION_DATE''] = df[''TRANSACTION_DATE''].map(lambda x:tranaction_date_return(x))
            df[''SELLOUT_AMOUNT''] = df[''SELLOUT_AMOUNT''].map(lambda x: str(round(float(x))))

            df = df.applymap(lambda x:None if x==''nan'' else x)
            snowparkdf = session.create_dataframe(df)

            snowparkdf = snowparkdf.with_column(''CUSTOMER_CODE'',lit(''129057''))

            snowparkdf = snowparkdf.with_column(''SOURCE_FILE_NAME'',lit(file_name))
            
         

            def creatdatetime():
                return datetime.now()

            snowparkdf = snowparkdf.with_column(''CRT_DTTM'',lit(creatdatetime()))

            snowdf = snowparkdf.select(''CUSTOMER_NAME'',
                                      ''CUSTOMER_CODE'',
                                      ''SUB_CUSTOMER_NAME'',
                                      ''EAN_NUMBER'',
                                      ''SAP_CODE'',
                                      ''SKU_TYPE'',
                                      ''BRAND_CATEGORIES'',
                                      ''PRODUCT_NAME'',
                                      ''YEAR'',
                                      ''MONTH'',
                                      ''WEEK'',
                                      ''TRANSACTION_DATE'',
                                      ''SELLOUT_QTY'',
                                      ''SELLOUT_AMOUNT'',
                                      ''SOLD_TO'',
                                      ''CRT_DTTM'',
                                      ''SOURCE_FILE_NAME'')

            snowdf.write.mode("append").saveAsTable(target_table)

            current_date = datetime.now()
            formatted_year = current_date.strftime("%Y")
            formatted_month = current_date.strftime("%m")

             # write to success folder
            file_name = file_name.split(''.'')[0]+"_"+datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")

            

            snowdf.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)

            

            

            return ''Success''
            

    except KeyError as key_error:
        # Handle KeyError (missing columns) here
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message
        
    except Exception as e:
        # Handle exceptions here
        error_message = f"Error: {str(e)}"
        return error_message;';
		
		

CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.NTASDL_RAW.J_KR_ECOMMERCE_SELLOUT_UNITOA_INGESTION_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('openpyxl==3.1.2','snowflake-snowpark-python==*')
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
#from unidecode import unidecode

from re import sub

def main(session: snowpark.Session,Param):
    

    try:

        #Param=[''Weekly_Summary_Unitoa_raw_data_20240429.xlsx'',''NTASDL_RAW.DEV_LOAD_STAGE_ADLS_KOR'',''dev/ecommerce_sellout/unitoa/'',''sdl_kr_ecommerce_unitoa_sellout'']

        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        file_header_loc = 10

        #Reading data from adls
        full_path = "@"+stage_name+"/"+temp_stage_path+"/"+file_name
        
        with SnowflakeFile.open(full_path,''rb'',require_scoped_url= False) as f:
            df = pd.read_excel(f)

            df = df.applymap(lambda x: str(x))

            df = df[5:][df.columns[1:]]

            df.rename(columns = {df.columns[0]:''CUSTOMER_NAME'',
                                 df.columns[1]:''SUB_CUSTOMER_NAME'',
                                 df.columns[2]:''EAN_NUMBER'',
                                 df.columns[3]:''SAP_CODE'',
                                 df.columns[4]:''SKU_TYPE'',
                                 df.columns[5]: ''BRAND_CATEGORIES'',
                                 df.columns[6]: ''PRODUCT_NAME'',
                                 df.columns[7]: ''YEAR'',
                                 df.columns[8]: ''MONTH'',
                                 df.columns[9]: ''WEEK'',
                                 df.columns[10]:''TRANSACTION_DATE'',
                                 df.columns[11]:''SELLOUT_QTY'',
                                 df.columns[12]:''SELLOUT_AMOUNT'',
                                 df.columns[13]:''SOLD_TO''},inplace=True)

            def tranaction_date_return(strdate):
                return datetime.strptime(strdate,''%Y%m%d'')

            df[''TRANSACTION_DATE''] = df[''TRANSACTION_DATE''].map(lambda x:tranaction_date_return(x))
            df[''SELLOUT_AMOUNT''] = df[''SELLOUT_AMOUNT''].map(lambda x: str(round(float(x))))

            df = df.applymap(lambda x:None if x==''nan'' else x)
            snowparkdf = session.create_dataframe(df)

            snowparkdf = snowparkdf.with_column(''CUSTOMER_CODE'',lit(''135139''))

            snowparkdf = snowparkdf.with_column(''SOURCE_FILE_NAME'',lit(file_name))
            
         

            def creatdatetime():
                return datetime.now()

            snowparkdf = snowparkdf.with_column(''CRT_DTTM'',lit(creatdatetime()))

            snowdf = snowparkdf.select(''CUSTOMER_NAME'',
                                      ''CUSTOMER_CODE'',
                                      ''SUB_CUSTOMER_NAME'',
                                      ''EAN_NUMBER'',
                                      ''SAP_CODE'',
                                      ''SKU_TYPE'',
                                      ''BRAND_CATEGORIES'',
                                      ''PRODUCT_NAME'',
                                      ''YEAR'',
                                      ''MONTH'',
                                      ''WEEK'',
                                      ''TRANSACTION_DATE'',
                                      ''SELLOUT_QTY'',
                                      ''SELLOUT_AMOUNT'',
                                      ''SOLD_TO'',
                                      ''CRT_DTTM'',
                                      ''SOURCE_FILE_NAME'')

            snowdf.write.mode("append").saveAsTable(target_table)

            current_date = datetime.now()
            formatted_year = current_date.strftime("%Y")
            formatted_month = current_date.strftime("%m")

             # write to success folder
            file_name = file_name.split(''.'')[0]+"_"+datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")

            

            snowdf.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)

            

            

            return ''Success''
            

    except KeyError as key_error:
        # Handle KeyError (missing columns) here
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message
        
    except Exception as e:
        # Handle exceptions here
        error_message = f"Error: {str(e)}"
        return error_message;';
		
		
CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.NTASDL_RAW.SDL_HK_POS_SCORECARD_MANNINGS_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('openpyxl==3.1.2','snowflake-snowpark-python==*','xlrd==2.0.1')
HANDLER = 'main'
EXECUTE AS OWNER
AS '#from snowflake.snowpark.types import StringType, StructType, StructField
import snowflake.snowpark
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz
import sys
import pandas as pd
import logging
from datetime import datetime
import collections
from pathlib import Path
import openpyxl as xl
from snowflake.snowpark.files import SnowflakeFile
from snowflake.snowpark.functions import col, lit, current_timestamp, trim, upper
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DecimalType,DateType

def main(session: snowpark.Session,Param):
   #Param=["J_J_30032024.xlsx","NTASDL_RAW.DEV_LOAD_STAGE_ADLS_hkg","dev/pos/Mannings","sdl_hk_pos_scorecard_mannings"]
    try:
        file_name = Param[0].split(".")[0]+".xlsx"
        stage_name = Param[1]
        temp_stage_path = Param[2]
        target_table = Param[3]

       

        full_path = "@"+stage_name+"/"+temp_stage_path+"/"+file_name
        with SnowflakeFile.open(full_path, "rb", require_scoped_url = False) as f:
            dataset = pd.read_excel(f,sheet_name=0)
        #return session.create_dataframe(dataset)

        z=[]
        
        for i in range(0,list(dataset.shape)[0]):
            if dataset.iloc[i][0] == ''Vendor ID'':
                 z.append(i)
                    
        z=z[0]
            
        x=[]
        for i in range(0,5):
            x.append(dataset.iloc[z,i])
                
            
            
        
            
            # Check for first 4 columns that have Total
        colnames = dataset.columns
        total_pairs = list(zip(dataset[colnames[0]]==''Total'', 
                            dataset[colnames[1]] ==''Total'', 
                            dataset[colnames[2]] ==''Total'', 
                            dataset[colnames[3]] ==''Total'',
                            dataset[colnames[4]] ==''Total''))
            
        index_to_keep = [not any(x) for x in total_pairs]
        dataset = dataset[index_to_keep]
            
            # print(''before: \\n'', dataset[colnames[0:7]].head(10))
            
            # For first 6 columns fill na as ffill
        df_dims = dataset[colnames[0:5]].copy()
        df_dims.fillna(method=''ffill'', inplace = True)
            
            # For the rest columns fill na as 0
        df_data = dataset[colnames[5:]].copy()
        df_data.fillna(value = 0, inplace = True)
            
            # Combine results together
        dataset = pd.concat([df_dims, df_data], axis = 1)
            # print(dataset.head(10))
            
            
            # # Transpose date from column to row
            
            
        columnIndex = 5
        dateValues = []
        data = [[]]
            
        for j in range(0,len(dataset.columns)):
                # Get available date values in dataset after column index indicator
            if j > columnIndex:
                selectedDate = dataset.iloc[1][j]
                dateValues.append(selectedDate)
            
            # Put date values in an array & remove NULLs
        dateValues = [x for x in dateValues if x != 0 ]
        dateValues.pop()   
                
            # Loop through the dataset by rows
        for u in range(z,len(dataset.index)):
                # Only check for data after row 2
            if u > (z+1):
                    # Set up key columns in the right order
                vendorId = dataset.iloc[u][0] 
                vendorDesc = dataset.iloc[u][1] 
                brandId = dataset.iloc[u][2] 
                productId = dataset.iloc[u][3] 
                productDesc = dataset.iloc[u][4] 
                    
                    # Counter value for Sales Value Selection
                valueCounter = 6
                    
                    # For every SKU propagate date values & sales data
                for h in dateValues:
                    date = h
                    salesQty = dataset.iloc[u][valueCounter]
                    valueCounter += 1
                    salesValue = dataset.iloc[u][valueCounter]
                    valueCounter += 1
                    dataRow = [vendorId,vendorDesc,brandId,productId,productDesc,date,salesQty,salesValue]
                    data.append(dataRow)                
            
            
            # Assign columns to the formatted dataset
            #print(''Creating dataframe...'')
        formattedDataSet = pd.DataFrame(data,columns=["vendorId","vendorDesc","brandId","productId","productDesc","date",
                                                  "salesQty","salesValue"])
        formattedDataSet["date"]=pd.to_datetime(formattedDataSet.date, format=''%d/%m/%Y'')
        formattedDataSet["date"]=formattedDataSet["date"].dt.strftime("%d/%m/%Y")
        formattedDataSet = formattedDataSet.drop(labels=0, axis=0)
        #formattedDataSet=pd.DataFrame(formattedDataSet,columns=[''vendorid'', ''vendordesc'', ''brand'', ''productid'', ''productdesc'', ''date'', ''salesqty'', ''salesvalue''])
        
        df = session.create_dataframe(formattedDataSet)
        #return df

       
            
        file_name1 = "formatted_"+file_name.split(".")[0] + ".csv"
        df = df.withColumn("filename", lit(file_name1))
        df = df.withColumn("run_id", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        df = df.withColumn("crt_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))

        # snowdf = df.select(
        #     ''vendorid'', ''vendordesc'', ''brand'', ''productid'', ''productdesc'', ''date'', ''salesqty'', ''salesvalue'', ''filename'', ''run_id'', ''crt_dttm''

        # )
        snowdf=df
        
        file_name = file_name.split(".")[0] + "_" + datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        

        
        
        snowdf.write.mode("append").saveAsTable(stage_name.split(".")[0] + "." + target_table)
        
        snowdf.write.copy_into_location("@" + stage_name + "/" + temp_stage_path + "/processed/success/" + formatted_year + "/" + formatted_month + "/" + file_name, header=True, OVERWRITE=True)
        
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
		
		

CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.NTASDL_RAW.COUPANG_PPM_FILE_PREPROCESSING("PARAM" ARRAY)
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

def main(session:snowpark.Session,Param):
    #Param=["daily_performance_20230607.csv","NTASDL_RAW.DEV_LOAD_STAGE_ADLS_KOR","dev/coupang_ppm/transaction/","sdl_kr_coupang_productsalereport"]
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]

        #session.sql("""TRUNCATE TABLE PROD_DNA_LOAD.NTASDL_RAW.SDL_KR_ECOMMERCE_OFFTAKE_COUPANG_TRANSACTION""")
        
        df_schema = StructType([
                StructField("transaction_date", StringType(), True),
                StructField("product_id", StringType(), True),
                StructField("barcode", StringType(), True),
                StructField("sku_id", StringType(), True),
                StructField("sku_name", StringType(), True),
                StructField("vendor_item_id", StringType(), True),
                StructField("vendor_item_name", StringType(), True),
                StructField("regular_delivery", StringType(), True),
                StructField("product_category_high", StringType(), True),
                StructField("product_category_mid", StringType(), True),
                StructField("product_category_low", StringType(), True),
                StructField("brand", StringType(), True),
                StructField("gmv", StringType(), True),
                StructField("units_sold", StringType(), True),
                StructField("return_units", StringType(), True),
                StructField("cogs", StringType(), True),
                StructField("amv", StringType(), True),
                StructField("coupon_discount", StringType(), True),
                StructField("instant_discount_price", StringType(), True),
                StructField("asp", StringType(), True),
                StructField("order_count", StringType(), True),
                StructField("ordered_customer_count", StringType(), True),
                StructField("unit_price", StringType(), True),
                StructField("conversion_rate", StringType(), True),
                StructField("pv", StringType(), True),
                StructField("reg_dlvry_gmv", StringType(), True),
                StructField("reg_dlvry_cogs", StringType(), True),
                StructField("reg_dlvry_rate", StringType(), True),
                StructField("reg_dlvry_units_sold", StringType(), True),
                StructField("reg_dlvry_return_units", StringType(), True),
                StructField("review_count", StringType(), True),
                StructField("avg_product_review_rate", StringType(), True),
                
                
               
                
            ])
        
        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", ",")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .option("encoding",''UTF-8'')\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

       
        
        df=df.na.drop("all")
        if df.count()==0 :
            return "No Data in file"

        def change_date_time(date_time):
            year_dt = date_time[:4]
            month_dt = date_time[4:6]
            day_dt = date_time[6:]
            return day_dt+"-"+month_dt+"-"+year_dt


        def get_datetime():
            return datetime.now().strftime("%Y%m%d%H%M%S")


        df= df.with_column("run_id", lit(get_datetime()))
        df = df.with_column("file_name", lit(file_name))

        def get_yearmo(file_name):
            return file_name.split(''.'')[0].split(''_'')[2][:7]

        df = df.with_column("yearmo", lit(get_yearmo(file_name)))

        df_pandas = df.to_pandas()
        df_pandas[''TRANSACTION_DATE''] = df_pandas[''TRANSACTION_DATE''].map(lambda x: datetime.strptime(change_date_time(x),"%d-%m-%Y"))

        df = session.create_dataframe(df_pandas)


       
        session.sql(''truncate table ''+ stage_name.split(".")[0] + "." + ''sdl_kr_ecommerce_offtake_coupang_transaction;'').collect()

           

        snowdf_coupang_transaction = df.select(''transaction_date'',
                              ''product_id'',
                              ''barcode'',
                              ''sku_id'',
                              ''sku_name'',
                              ''vendor_item_id'',
                              ''vendor_item_name'',
                              ''regular_delivery'',
                              ''product_category_high'',
                              ''product_category_mid'',
                              ''product_category_low'',
                              ''brand'',
                              ''gmv'',
                              ''units_sold'',
                              ''return_units'',
                              ''cogs'',
                              ''amv'',
                              ''coupon_discount'',
                              ''instant_discount_price'',
                              ''asp'',
                              ''order_count'',
                              ''ordered_customer_count'',
                              ''unit_price'',
                              ''conversion_rate'',
                              ''pv'',
                              ''reg_dlvry_gmv'',
                              ''reg_dlvry_cogs'',
                              ''reg_dlvry_rate'',
                              ''reg_dlvry_units_sold'',
                              ''reg_dlvry_return_units'',
                              ''review_count'',
                              ''avg_product_review_rate'')

        snowdf_coupang_transaction =  snowdf_coupang_transaction.with_column("load_date", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        
        snowdf_coupang_transaction = snowdf_coupang_transaction.with_column(''source_file_name'',lit(file_name))

        df_pandas = snowdf_coupang_transaction.to_pandas()
        df_pandas[''TRANSACTION_DATE''] = df_pandas[''TRANSACTION_DATE''].map(lambda x: datetime.strftime(x,"%Y-%m-%d"))
        snowdf_coupang_transaction = session.create_dataframe(df_pandas)
        

        snowdf_productsalereport = df.select(''transaction_date'',
                                           ''sku_id'',
                                           ''sku_name'',
                                           ''vendor_item_id'',
                                           ''vendor_item_name'',
                                           ''product_id'',
                                           ''barcode'',
                                    	   ''product_category_high'',
                                    		''product_category_mid'',
                                    		''product_category_low'',
                                           ''brand'',
                                           ''gmv'',
                                           ''cogs'',
                                    	   ''units_sold'',
                                           ''reg_dlvry_gmv'',
                                           ''Reg_dlvry_rate'',
                                           ''reg_dlvry_cogs'',
                                           ''units_sold'',
                                           ''review_count'',
                                           ''avg_product_review_rate'',
                                            ''run_id'',
                                            ''file_name'',
                                            ''yearmo'')


       
        
            
       
       



        file_name=file_name.split(".")[0]+"_"+datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")

        snowdf_productsalereport.write.mode("append").saveAsTable(stage_name.split(".")[0] + "." + target_table)
        snowdf_coupang_transaction.write.mode("append").saveAsTable(stage_name.split(".")[0] + "." + ''sdl_kr_ecommerce_offtake_coupang_transaction'')

        #move to success
        snowdf_productsalereport.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)
        snowdf_coupang_transaction.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)

        
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



CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.NTASDL_RAW.SDL_KR_POS_LALAVLA_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS 'from snowflake.snowpark.functions import col, lit, date_format,ltrim, current_timestamp, to_date,md5, year, month, concat, format_number, regexp_replace, to_timestamp, when, trim, upper
from snowflake.snowpark.types import StringType, StructType, StructField,DecimalType
import snowflake.snowpark
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz
from snowflake.snowpark.files import SnowflakeFile
from snowflake.snowpark import Window

def main(session: snowpark.Session,Param):
    #Param=["KR_POS_Lalavla_20221110.MJNJS01","NTASDL_RAW.DEV_LOAD_STAGE_ADLS_kor","dev/pos/lalavla","sdl_kr_pos_lalavla"]
    try:
        file_name = Param[0]
        stage_name = Param[1]
        temp_stage_path = Param[2]
        target_table = Param[3]

        df_schema = StructType([
                StructField("a", StringType(), True),
            ])
        
              
        

        df = session.read\\
            .schema(df_schema)\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
        
        
        df = df.na.drop("all")
        if df.count() == 0:
            return "No Data in file"
        #return df
        
        #df=df.withColumn("1_raw",col("a").substring(0,8))
        df=df.withColumn("1",to_date(col("a").substring(0,8),"yyyymmdd"))
        df=df.withColumn("2",col("a").substring(9,5))
        df=df.withColumn("3",col("a").substring(14,7))
        df=df.withColumn("4",col("a").substring(21,22))
        df=df.withColumn("5",ltrim(col("a").substring(43,9),trim_string=lit("0")))
        #df=df.withColumn("5",ltrim(col("5"),trim_string=lit("0")))
        df=df.withColumn("6",ltrim(col("a").substring(52,7),trim_string=lit("0")))
        df=df.withColumn("7",ltrim(col("a").substring(59,11),trim_string=lit("0")))
        df=df.withColumn("8",to_date(col("a").substring(70,8),"yyyymmdd"))
        df=df.withColumn("9",col("a").substring(78,7))
        df=df.withColumn("10",col("a").substring(85,3))
        df=df.withColumn("11",col("a").substring(88,6))
        
        #return df
        df = df.withColumn("filename", lit(file_name.split(".")[0]+"_"+datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")+".csv"))
        df = df.withColumn("crt_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        df = df.withColumn("run_id", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        df = df.withColumn("upd_dttm",lit(None))
        
        snowdf = df.select(
                                "1", "2", "3", 
                                "4", "5", 
                                "6", "7", 
                                "8", "9", 
                                "10","11",
                            "crt_dttm","upd_dttm","filename",
                            "run_id"
                        )
        
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        
        #move to success
        
        snowdf.write.mode("append").saveAsTable(stage_name.split(".")[0] + "." + target_table)
        file_name1 = file_name.split(".")[0] + "_" + datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S") + ".csv"
        snowdf.write.copy_into_location("@" + stage_name + "/" + temp_stage_path + "/processed/success/" + formatted_year + "/" + formatted_month + "/" + file_name1, header=True, OVERWRITE=True)
        
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
		
		
CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.NTASDL_RAW.POP6_HK_MASTER_POPS_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them.

from snowflake.snowpark.functions import col, lit, date_format, current_timestamp, to_date, year, month, concat, format_number, regexp_replace,to_timestamp,when,trim,upper,is_null,md5
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DecimalType
import snowflake.snowpark
import pandas as pd
from datetime import datetime,timedelta
import snowflake.snowpark as snowpark
import pytz
import pandas as pd

def main(session:snowpark.Session,Param):
    #Param=["20240609_pops.csv","NTASDL_RAW.DEV_LOAD_STAGE_ADLS_HKG","dev/pop6/masterdata/pops/","sdl_pop6_hk_pops"]
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        
        df_schema = StructType([
                StructField("Status", StringType(), True),
                StructField("popdb_id", StringType(), True),
                StructField("pop_code", StringType(), True),
                StructField("pop_name", StringType(), True),
                StructField("address", StringType(), True),
                StructField("longitude", StringType(), True),
                StructField("latitude", StringType(), True),
                StructField("Business_Units_ID", StringType(), True),
                StructField("Business_Unit_Name", StringType(), True),
                StructField("country", StringType(), True),
                StructField("channel", StringType(), True),
                StructField("retail_environment_ps", StringType(), True),
                StructField("customer", StringType(), True),
                StructField("sales_group_code", StringType(), True),
                StructField("sales_group_name", StringType(), True),
                StructField("Store_Type", StringType(), True),
                StructField("Customer_Grade", StringType(), True),
                StructField("External_Store_Code", StringType(), True),
                StructField("territory", StringType(), True),
               
                
                
            ])
        
        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", ",")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .option("encoding",''UTF-8'')\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

       
        
        df=df.na.drop("all")
        if df.count()==0 :
            return "No Data in file"

        df = df.with_column(''file_name'',lit(file_name))


        def generunid():
            new_time = datetime.now(pytz.utc) + timedelta(hours=8)
            return new_time.strftime(''%Y%m%d%H%M%S'')

        def generatecrtd_dttm():
            new_time = datetime.now(pytz.utc) + timedelta(hours=8)
            return new_time.strftime(''%Y-%m-%d %H:%M:%S'')
            
            

        df = df.with_column(''hashkey'',md5(concat(when(is_null(col(''status'')),lit('''')).otherwise(col(''status'')),\\
                                            when(is_null(col(''popdb_id'')),lit('''')).otherwise(col(''popdb_id'')),\\
                                          when(is_null(col(''pop_code'')),lit('''')).otherwise(col(''pop_code'')),\\
                                          when(is_null(col(''pop_name'')),lit('''')).otherwise(col(''pop_name'')),\\
                                         when(is_null(col(''address'')),lit('''')).otherwise(col(''address'')),\\
                                         when(is_null(col(''longitude'')),lit('''')).otherwise(col(''longitude'')),\\
                                         when(is_null(col(''latitude'')),lit('''')).otherwise(col(''latitude'')),\\
                                          when(is_null(col(''country'')),lit('''')).otherwise(col(''country'')),\\
                                         when(is_null(col(''channel'')),lit('''')).otherwise(col(''channel'')),\\
                                         when(is_null(col(''retail_environment_ps'')),lit('''')).otherwise(col(''retail_environment_ps'')),\\
                                         when(is_null(col(''customer'')),lit('''')).otherwise(col(''customer'')),\\
                                         when(is_null(col(''sales_group_code'')),lit('''')).otherwise(col(''sales_group_code'')),\\
                                         when(is_null(col(''sales_group_name'')),lit('''')).otherwise(col(''sales_group_name'')),\\
                                        when(is_null(col(''customer_grade'')),lit('''')).otherwise(col(''customer_grade'')),\\
                                          when(is_null(col(''external_store_code'')),lit('''')).otherwise(col(''external_store_code'')),
                                                when(is_null(col(''business_units_id'')),lit('''')).otherwise(col(''business_units_id'')),\\
                                          when(is_null(col(''territory'')),lit('''')).otherwise(col(''territory'')))))
                                         
        
        df = df.with_column(''run_id'',lit(generunid()))

        df = df.with_column(''crtd_dttm'',lit(generatecrtd_dttm()))
        

        

        
        snowdf= df.select(
                    "Status",
                "popdb_id",
                "pop_code",
                "pop_name",
                "address",
                "longitude",
                "latitude",
                "country",
                "channel",
                "retail_environment_ps",
                "customer",
                "sales_group_code",
                "sales_group_name",
                "customer_grade",
                "external_store_code",
                "file_name",
                "run_id",
                "crtd_dttm",
                "hashkey",
                "business_units_id",
                "business_unit_name",
                "territory" 
                
            
                    )
        file_name=file_name.split(".")[0]+"_"+datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")

        snowdf.write.mode("append").saveAsTable(stage_name.split(".")[0] + "." + target_table)
        
        # #move to success
        snowdf.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)
        
        
        
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


CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.NTASDL_RAW.POP6_HK_MASTER_POPS_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them.

from snowflake.snowpark.functions import col, lit, date_format, current_timestamp, to_date, year, month, concat, format_number, regexp_replace,to_timestamp,when,trim,upper,is_null,md5
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DecimalType
import snowflake.snowpark
import pandas as pd
from datetime import datetime,timedelta
import snowflake.snowpark as snowpark
import pytz
import pandas as pd

def main(session:snowpark.Session,Param):
    #Param=["20240609_pops.csv","NTASDL_RAW.DEV_LOAD_STAGE_ADLS_HKG","dev/pop6/masterdata/pops/","sdl_pop6_hk_pops"]
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        
        df_schema = StructType([
                StructField("Status", StringType(), True),
                StructField("popdb_id", StringType(), True),
                StructField("pop_code", StringType(), True),
                StructField("pop_name", StringType(), True),
                StructField("address", StringType(), True),
                StructField("longitude", StringType(), True),
                StructField("latitude", StringType(), True),
                StructField("Business_Units_ID", StringType(), True),
                StructField("Business_Unit_Name", StringType(), True),
                StructField("country", StringType(), True),
                StructField("channel", StringType(), True),
                StructField("retail_environment_ps", StringType(), True),
                StructField("customer", StringType(), True),
                StructField("sales_group_code", StringType(), True),
                StructField("sales_group_name", StringType(), True),
                StructField("Store_Type", StringType(), True),
                StructField("Customer_Grade", StringType(), True),
                StructField("External_Store_Code", StringType(), True),
                StructField("territory", StringType(), True),
               
                
                
            ])
        
        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", ",")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .option("encoding",''UTF-8'')\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

       
        
        df=df.na.drop("all")
        if df.count()==0 :
            return "No Data in file"

        df = df.with_column(''file_name'',lit(file_name))


        def generunid():
            new_time = datetime.now(pytz.utc) + timedelta(hours=8)
            return new_time.strftime(''%Y%m%d%H%M%S'')

        def generatecrtd_dttm():
            new_time = datetime.now(pytz.utc) + timedelta(hours=8)
            return new_time.strftime(''%Y-%m-%d %H:%M:%S'')
            
            

        df = df.with_column(''hashkey'',md5(concat(when(is_null(col(''status'')),lit('''')).otherwise(col(''status'')),\\
                                            when(is_null(col(''popdb_id'')),lit('''')).otherwise(col(''popdb_id'')),\\
                                          when(is_null(col(''pop_code'')),lit('''')).otherwise(col(''pop_code'')),\\
                                          when(is_null(col(''pop_name'')),lit('''')).otherwise(col(''pop_name'')),\\
                                         when(is_null(col(''address'')),lit('''')).otherwise(col(''address'')),\\
                                         when(is_null(col(''longitude'')),lit('''')).otherwise(col(''longitude'')),\\
                                         when(is_null(col(''latitude'')),lit('''')).otherwise(col(''latitude'')),\\
                                          when(is_null(col(''country'')),lit('''')).otherwise(col(''country'')),\\
                                         when(is_null(col(''channel'')),lit('''')).otherwise(col(''channel'')),\\
                                         when(is_null(col(''retail_environment_ps'')),lit('''')).otherwise(col(''retail_environment_ps'')),\\
                                         when(is_null(col(''customer'')),lit('''')).otherwise(col(''customer'')),\\
                                         when(is_null(col(''sales_group_code'')),lit('''')).otherwise(col(''sales_group_code'')),\\
                                         when(is_null(col(''sales_group_name'')),lit('''')).otherwise(col(''sales_group_name'')),\\
                                        when(is_null(col(''customer_grade'')),lit('''')).otherwise(col(''customer_grade'')),\\
                                          when(is_null(col(''external_store_code'')),lit('''')).otherwise(col(''external_store_code'')),
                                                when(is_null(col(''business_units_id'')),lit('''')).otherwise(col(''business_units_id'')),\\
                                          when(is_null(col(''territory'')),lit('''')).otherwise(col(''territory'')))))
                                         
        
        df = df.with_column(''run_id'',lit(generunid()))

        df = df.with_column(''crtd_dttm'',lit(generatecrtd_dttm()))
        

        

        
        snowdf= df.select(
                    "Status",
                "popdb_id",
                "pop_code",
                "pop_name",
                "address",
                "longitude",
                "latitude",
                "country",
                "channel",
                "retail_environment_ps",
                "customer",
                "sales_group_code",
                "sales_group_name",
                "customer_grade",
                "external_store_code",
                "file_name",
                "run_id",
                "crtd_dttm",
                "hashkey",
                "business_units_id",
                "business_unit_name",
                "territory" 
                
            
                    )
        file_name=file_name.split(".")[0]+"_"+datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")

        snowdf.write.mode("append").saveAsTable(stage_name.split(".")[0] + "." + target_table)
        
        # #move to success
        snowdf.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)
        
        
        
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



CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.NTASDL_RAW.POP6_HK_MASTER_USERS_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them.

from snowflake.snowpark.functions import col, lit, date_format, current_timestamp, to_date, year, month, concat, format_number, regexp_replace,to_timestamp,when,trim,upper,is_null,md5
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DecimalType
import snowflake.snowpark
import pandas as pd
from datetime import datetime,timedelta
import snowflake.snowpark as snowpark
import pytz
import pandas as pd

def main(session:snowpark.Session,Param):
    #Param=["20240610_users.csv","NTASDL_RAW.DEV_LOAD_STAGE_ADLS_HKG","dev/pop6/masterdata/users/","sdl_pop6_hk_users"]
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        
        df_schema = StructType([
                StructField("Status", StringType(), True),
                StructField("USERDB_ID", StringType(), True),
                StructField("Username", StringType(), True),
                StructField("FIRST_NAME", StringType(), True),
                StructField("Last_Name", StringType(), True),
                StructField("Team", StringType(), True),
                StructField("Superior_Name", StringType(), True),
                StructField("Authorisation_Group", StringType(), True),
                StructField("Email_Address", StringType(), True),
                StructField("Longitude", StringType(), True),
                StructField("Latitude", StringType(), True),
                StructField("Business_Units_ID", StringType(), True),
                StructField("Business_Unit_Name", StringType(), True)
                
            ])
        
        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", ",")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .option("encoding",''UTF-8'')\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

       
        
        df=df.na.drop("all")
        if df.count()==0 :
            return "No Data in file"

        df = df.with_column(''file_name'',lit(file_name))


        def generunid():
            new_time = datetime.now(pytz.utc) + timedelta(hours=8)
            return new_time.strftime(''%Y%m%d%H%M%S'')

        def generatecrtd_dttm():
            new_time = datetime.now(pytz.utc) + timedelta(hours=8)
            return new_time.strftime(''%Y-%m-%d %H:%M:%S'')
            
            

        df = df.with_column(''hashkey'',md5(concat(when(is_null(col(''status'')),lit('''')).otherwise(col(''status'')),\\
                                            when(is_null(col(''USERDB_ID'')),lit('''')).otherwise(col(''USERDB_ID'')),\\
                                          when(is_null(col(''Username'')),lit('''')).otherwise(col(''Username'')),\\
                                          when(is_null(col(''FIRST_NAME'')),lit('''')).otherwise(col(''FIRST_NAME'')),\\
                                         when(is_null(col(''Last_Name'')),lit('''')).otherwise(col(''Last_Name'')),\\
                                         when(is_null(col(''Team'')),lit('''')).otherwise(col(''Team'')),\\
                                         when(is_null(col(''Superior_Name'')),lit('''')).otherwise(col(''Superior_Name'')),\\
                                          when(is_null(col(''Authorisation_Group'')),lit('''')).otherwise(col(''Authorisation_Group'')),\\
                                         when(is_null(col(''Email_Address'')),lit('''')).otherwise(col(''Email_Address'')),\\
                                         when(is_null(col(''Longitude'')),lit('''')).otherwise(col(''Longitude'')),\\
                                         when(is_null(col(''Latitude'')),lit('''')).otherwise(col(''Latitude'')),\\
                                         when(is_null(col(''Business_Units_ID'')),lit('''')).otherwise(col(''Business_Units_ID'')),
                                         when(is_null(col(''Business_Unit_Name'')),lit('''')).otherwise(col(''Business_Unit_Name'')))))

        
        df = df.with_column(''run_id'',lit(generunid()))

        df = df.with_column(''crtd_dttm'',lit(generatecrtd_dttm()))
        

        

        
        snowdf= df.select(
                    "status",
                    "userdb_id",
                    "username",
                    "first_name",
                    "last_name",
                    "team",
                    "superior_name",
                    "authorisation_group",
                    "email_address",
                    "longitude",
                    "latitude",
                    "file_name",
                    "run_id",
                    "crtd_dttm",
                    "hashkey",
                    "business_units_id",
                    "business_unit_name"
            
                    )
        file_name=file_name.split(".")[0]+"_"+datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")

        snowdf.write.mode("append").saveAsTable(stage_name.split(".")[0] + "." + target_table)
        
        # #move to success
        snowdf.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)
        
        
        
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

