CREATE OR REPLACE PROCEDURE prod_dna_load.NTASDL_RAW.SDL_TW_IMS_DSTR_STD_SEL_OUT_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS 'from snowflake.snowpark.functions import col, lit, date_format, current_timestamp, to_date,md5, year, month, concat, format_number, regexp_replace, to_timestamp, when, trim, upper
from snowflake.snowpark.types import StringType, StructType, StructField,DecimalType
import snowflake.snowpark
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz

def main(session: snowpark.Session,Param):
    #Param=["TW_IMS_Distributor_123291_SellOut_20240625.csv","NTASDL_RAW.DEV_LOAD_STAGE_ADLS_twn","dev/ims/distributor/123291","sdl_tw_ims_dstr_std_sel_out_123291_sellout"]
    try:
        file_name = Param[0]
        stage_name = Param[1]
        temp_stage_path = Param[2]
        target_table = Param[3]

        df_schema = StructType([
                            StructField("transaction_date", StringType()),
                            StructField("distributor_code", StringType()),
                            StructField("distributor_name", StringType()),
                            StructField("distributors_customer_code", StringType()),
                            StructField("distributors_customer_name", StringType()),
                            StructField("distributors_product_code", StringType()),
                            StructField("distributors_product_name", StringType()),
                            StructField("report_period_start_date", StringType()),
                            StructField("report_period_end_date", StringType()),
                            StructField("ean", StringType()),
                            StructField("uom", StringType()),
                            StructField("unit_price", StringType()),
                            StructField("sales_amount", StringType()),
                            StructField("sales_qty", StringType()),
                            StructField("return_qty", StringType()),
                            StructField("return_amount", StringType()),
                            StructField("sales_rep_code", StringType()),
                            StructField("sales_rep_name", StringType())
                        ])

        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", ",")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name.split(".")[0])
        
              
        df = df.na.drop("all")
        if df.count() == 0:
            return "No Data in file"
        for i in df.columns:
            df=df.withColumn(i,trim(col(i)))

        df=df.withColumn("transaction_date",regexp_replace(col("transaction_date"), "/","-"))
        df1=df.filter(col("transaction_date").like("%-%"))
        if df1.count()>0:
            df=df.withColumn("transaction_date",date_format(col("transaction_date"),''yyyy-mm-dd''))
        else:
            df=df.withColumn("transaction_date",col("transaction_date").cast("String"))
            df=df.withColumn("transaction_date",to_date(col("transaction_date"),''yyyymmdd''))
            #df=df.withColumn("transaction_date",date_format(col("transaction_date"),''yyyy-mm-dd''))

        df = df.withColumn("unit_price", regexp_replace(col("unit_price"), ",", ""));
        df = df.withColumn("sales_amount", regexp_replace(col("sales_amount"), ",", ""));
        df = df.withColumn("sales_qty", regexp_replace(col("sales_qty"), ",", ""));
        df = df.withColumn("return_qty", regexp_replace(col("return_qty"), ",", ""));
        df = df.withColumn("return_amount", regexp_replace(col("return_amount"), ",", ""));
         
        
        
        #df = df.withColumn("filename", lit(file_name.split(".")[0]+"_"+datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")+".csv"))
        df = df.withColumn("crt_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        df = df.withColumn("run_id", lit(None))
        df = df.withColumn("file_name", lit(file_name))
        
        snowdf = df.select(
                        "transaction_date",
                            "distributor_code",
                            "distributor_name",
                            "distributors_customer_code",
                            "distributors_customer_name",
                            "distributors_product_code",
                            "distributors_product_name",
                            "report_period_start_date",
                            "report_period_end_date",
                            "ean",
                            "uom",
                            "unit_price",
                            "sales_amount",
                            "sales_qty",
                            "return_qty",
                            "return_amount",
                            "sales_rep_code",
                            "sales_rep_name",
                            "crt_dttm",
                            "run_id","file_name"
                        )
        
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        
        #move to success
        
        snowdf.write.mode("append").saveAsTable(stage_name.split(".")[0] + "." + target_table)
        file_name1 = file_name.split(".")[0] + "_" + datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        snowdf.write.copy_into_location("@" + stage_name + "/" + temp_stage_path + "/processed/success/" + formatted_year + "/" + formatted_month + "/" + file_name1, header=True, OVERWRITE=True,)
        
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
create OR REPLACE PROCEDURE prod_dna_load.NTASDL_RAW.TW_POS_RT_MART_PREPROCESING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS 'from snowflake.snowpark.functions import col, lit, date_format, current_timestamp, to_date, year, month, concat, format_number, regexp_replace,to_timestamp,when,trim,upper, substring
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DecimalType
import snowflake.snowpark
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz
import pandas as pd

def main(session: snowpark.Session,Param):
    # Param=["TW_POS_RTMart_RawData_20240419_20240503002807.csv","NTASDL_RAW.DEV_LOAD_STAGE_ADLS_TWN","cert-data-lake/pos/rtmart","SDL_TW_POS_RT_MART"]
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        
        df_schema = StructType([
                # StructField("mnth_id",StringType(), True),
                StructField("vendor_code",StringType(), True),
                StructField("vendor_name",StringType(), True),
                StructField("product_code",StringType(), True),
                StructField("product_name",StringType(), True),
                StructField("ean_code",StringType(), True),
                StructField("store_no",StringType(), True),
                StructField("store_name",StringType(), True),
                StructField("department",StringType(), True),
                StructField("department_name",StringType(), True),
                StructField("pos_date",StringType(), True),
                StructField("stock_receive_qty",StringType(), True),
                StructField("selling_qty",StringType(), True),
                StructField("inventory_qty",StringType(), True),
                StructField("average_selling_qty",StringType(), True)
                # StructField("crt_dttm",StringType(), True),
                # StructField("upd_dttm",StringType(), True)
            ])
        
        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "\\u0001")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .option("encoding","UTF-8")\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

    
        # df.show(8)
        
        df=df.na.drop("all")
        if df.count()==0 :
            return "No Data in file"
    
        
        # df = df.with_column("cdl_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        # file_name1=file_name.split(".")[0]+"_"+ datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")+".csv"
        df = df.with_column("file_name", lit(file_name.split(".")[0]+".xls"))
        df = df.with_column("crt_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        df = df.with_column("upd_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        # extrctd_month_id=file_name.split("_")[1].split(".")[0]
        # df = df.with_column("mnth_id", lit(extrctd_month_id))
        
        # date=file_name.split("_")[3].split(".")[0]
        # year=date[0:4]
        # mnth_no=date[4:6]
        # inv_week=date[6:]
        # df = df.withColumn("year", lit(year))
        # df = df.with_column("mnth_no", lit(mnth_no))
        # df = df.with_column("inv_week", lit(inv_week))  
        
        snowdf= df.select(
                    "vendor_code",
                    "vendor_name",
                    "product_code",
                    "product_name",
                    "ean_code",
                    "store_no",
                    "store_name",
                    "department",
                    "department_name",
                    "pos_date",
                    "stock_receive_qty",
                    "selling_qty",
                    "inventory_qty",
                    "average_selling_qty",
                    "crt_dttm",
                    "upd_dttm","file_name"
                    )
        file_name=file_name.split(".")[0]+"_"+datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        
        #move to success
        snowdf.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)
        
        snowdf.write.mode("append").saveAsTable(stage_name.split(".")[0] + "." + target_table)
        
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
        return error_message';
create OR REPLACE PROCEDURE prod_dna_load.NTASDL_RAW.SDL_TW_IMS_DSTR_STD_CUSTOMER_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS 'from snowflake.snowpark.functions import col, lit, date_format, current_timestamp, to_date,md5, year, month, concat, format_number, regexp_replace, to_timestamp, when, trim, upper
from snowflake.snowpark.types import StringType, StructType, StructField,DecimalType
import snowflake.snowpark
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz

def main(session: snowpark.Session,Param):
    #Param=["TW_IMS_Distributor_136454_Customer_20240515.csv","NTASDL_RAW.DEV_LOAD_STAGE_ADLS_twn","dev/ims/distributor/136454","sdl_tw_ims_dstr_std_customer_136454_customer"]
    try:
        file_name = Param[0]
        stage_name = Param[1]
        temp_stage_path = Param[2]
        target_table = Param[3]

        df_schema = StructType([
                    StructField("distributor_code", StringType()),
                    StructField("distributor_cusotmer_code", StringType()),
                    StructField("distributor_customer_name", StringType()),
                    StructField("distributor_address", StringType()),
                    StructField("distributor_telephone", StringType()),
                    StructField("distributor_contact", StringType()),
                    StructField("distributor_sales_area", StringType())
                ])

        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", ",")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
        
              
        df = df.na.drop("all")
        if df.count() == 0:
            return "No Data in file"

        for i in df.columns:
            df=df.withColumn(i,trim(col(i)))

        df=df.withColumn("transaction_date",regexp_replace(col("distributor_telephone"), "\\''",""))
        
        df = df.withColumn("file_name", lit(file_name))
        df = df.withColumn("crt_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        df = df.withColumn("upd_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))        
        snowdf = df.select(
                        "distributor_code",
                        "distributor_cusotmer_code",
                        "distributor_customer_name",
                        "distributor_address",
                        "distributor_telephone",
                        "distributor_contact",
                        "distributor_sales_area",
                        "crt_dttm",
                        "upd_dttm","file_name"
                        )
        
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        
        #move to success
        
        snowdf.write.mode("append").saveAsTable(stage_name.split(".")[0] + "." + target_table)
        file_name1 = file_name.split(".")[0] + "_" + datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        snowdf.write.copy_into_location("@" + stage_name + "/" + temp_stage_path + "/processed/success/" + formatted_year + "/" + formatted_month + "/" + file_name1, header=True, OVERWRITE=True,)
        
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

create OR REPLACE PROCEDURE prod_dna_load.NTASDL_RAW.KR_JU_HJ_LIFE_PREPROCESSING("PARAM" ARRAY)
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
from snowflake.snowpark.types import  StringType, StructType, StructField,IntegerType
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz



def main(session: snowpark.Session,Param): 
       
    try :
            
        #Param=[''JU_HJ_LIFE_125235.csv'',''NTASDL_RAW.DEV_LOAD_STAGE_ADLS_KOR'',''dev/gt_sellout/transaction_files/food_ws/ju_hj_life'',''sdl_kr_ju_hj_life_gt_sellout'']

        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        # Define the schema for the DataFrame
        df_schema=StructType([
        StructField("IMS_TXN_DT", StringType()),
        StructField("SUB_CUSTOMER_NAME", StringType()),
        StructField("ITEMS", StringType()),
        StructField("EAN", StringType()),
        StructField("QTY", StringType()),
        StructField("UNIT", StringType()),
        StructField("TOTAL_SALES", StringType()),
        StructField("SEE", StringType())])
                
        dataframe = session.read.schema(df_schema) \\
        .option("skip_header",1) \\
        .option("field_delimiter","\\\\u0001") \\
        .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
        

        
        dataframe=dataframe.na.drop("all")
        if dataframe.count()==0:
            return "No Data in file"

        dstr_nm = (''''.join(file_name.split(''_'')[:-1]))
        cust_cd = file_name.split(''_'')[-1]
        cust_cd = cust_cd.split(''.'')[0]

        dataframe= dataframe.withColumn("DSTR_NM", lit(dstr_nm).cast("string"))
        dataframe= dataframe.withColumn("CUST_CD", lit(cust_cd).cast("string"))
        dataframe = dataframe.withColumn("file_name", lit(file_name.split(".")[0]+".xls"))


        dataframe=dataframe[[''DSTR_NM'',
            ''IMS_TXN_DT'',
            ''SUB_CUSTOMER_NAME'',
            ''ITEMS'',
            ''EAN'',
            ''QTY'',
            ''UNIT'',
            ''TOTAL_SALES'',
            ''SEE'',
            ''CUST_CD'',
            "file_name"]]
        
        # Load Data to the target table
        dataframe.write.mode("append").saveAsTable(target_table)
        
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        #move to success
        file_name=file_name.split(".")[0]+''_''+datetime.now().strftime("%Y%m%d%H%M%S")
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
        return error_message';

create OR REPLACE PROCEDURE prod_dna_load.NTASDL_RAW.KR_NU_RI_ZON_GT_SELLOUT_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS 'from snowflake.snowpark.functions import col, lit, date_format, current_timestamp, to_date, year, month, concat, format_number, regexp_replace, to_timestamp, when, upper
from snowflake.snowpark.types import StringType, StructType, StructField
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz

def main(session: snowpark.Session,Param):
    # Param=[''NU_RI_ZON_125017.csv'',''NTASDL_RAW.DEV_LOAD_STAGE_ADLS_KOR'',''dev/gt_sellout/transaction_files/food_ws/nu_ri_zon'',''SDL_KR_NU_RI_ZON_GT_SELLOUT'']
    try:
        file_name = Param[0]
        stage_name = Param[1]
        temp_stage_path = Param[2]
        target_table = Param[3]
        
        df_schema = StructType([
            StructField("ims_txn_dt", StringType(), True),
            StructField("sub_customer_Name", StringType(), True),     
            StructField("ean", StringType(), True),
            StructField("name", StringType(), True),
            StructField("standard", StringType(), True),
            StructField("qty", StringType(), True),
            StructField("unit_price", StringType(), True),
            StructField("total", StringType(), True),

        ])

        
        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "\\u0001")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
        
        if df.count() == 0:
            return "No Data in file"

        def extract_buversion(data):
                listed = data.split(''_'')
                version = listed[:3]
                new_var = '' ''.join(version)
                return new_var

        df = df.with_column(''dstr_nm'', lit(extract_buversion(file_name)))
        df = df.withColumn("cust_cd", lit("125017"))

            
        # df = df.withColumn("crt_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        df = df.withColumn("file_name", lit(file_name.split(".")[0]+".xlsx"))
        # df = df.withColumn("run_id", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))

        snowdf = df.select(
             "dstr_nm", 
            "ims_txn_dt", 
            "sub_customer_Name",      
            "ean", 
            "name", 
            "standard", 
            "qty", 
            "unit_price", 
            "total",
            "cust_cd",
            "file_name"
        )

        
        # file_name=file_name.split(".")[0]+"_"+ datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        
        #move to success
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
        return error_message
';



create OR REPLACE PROCEDURE prod_dna_load.NTASDL_RAW.TW_POS_EC_PREPROCESSING("PARAM" ARRAY)
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
from snowflake.snowpark.functions import col,lit,replace,to_date
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField
import pandas as pd
from datetime import datetime
import pytz
 
def main(session: snowpark.Session,Param):
 
    try:
 
        #Param=[''TW_POS_EC_RawData_20240201.csv'',''NTASDL_RAW.DEV_LOAD_STAGE_ADLS_TWN'',''dev/pos/transaction/ec/'',''sdl_tw_pos_ec'']
 
        # Extracting parameters from the input
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
 
 
        # Define the schema for the DataFrame
        df_schema=StructType([
            StructField("CUSTOMER_EC_PLATFOM",StringType()),
            StructField("POS_DATE",StringType()),
            StructField("PRODUCT_CODE",StringType()),
            StructField("BRAND",StringType()),
            StructField("PRODUCT_NAME",StringType()),
            StructField("QTY",StringType()),
            StructField("SELLING_AMT_BEFORE_TAX",StringType())
            ])
 
        # Read the CSV file into a DataFrame
        dataframe = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "\\u0001")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .option("skip_blank_lines", True)\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
 
 
        #---------------------------Transformation logic ------------------------------#
 
        # Handle null values or empty rows
        dataframe=dataframe.na.drop("all")
 
        # Check for empty Dataframe
        if dataframe.count()==0:
            return "No Data in file"
    
        dataframe = dataframe.with_column("UPD_DTTM", lit(None).cast("string"))
        dataframe = dataframe.withColumn("POS_DATE", to_date(col("POS_DATE"), "yyyy/MM/dd"))
        dataframe = dataframe.with_column("CRT_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        dataframe = dataframe.withColumn("PRODUCT_NAME", replace(col("PRODUCT_NAME"), "\\n", ""))
        dataframe = dataframe.with_column("file_name", lit(file_name.split(".")[0]+".xlsx"))

        # # Creating copy of the Dataframe
        final_df = dataframe.select("CUSTOMER_EC_PLATFOM", "POS_DATE", "PRODUCT_CODE", "BRAND", "PRODUCT_NAME","QTY","SELLING_AMT_BEFORE_TAX","CRT_DTTM","UPD_DTTM","file_name" )
 
        # # Load Data to the target table
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
create OR REPLACE PROCEDURE prod_dna_load.NTASDL_RAW.KR_NACF_GT_SELLOUT_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS 'from snowflake.snowpark.functions import col, lit, date_format, current_timestamp, to_date, year, month, concat, format_number, regexp_replace, to_timestamp, when, trim, upper
from snowflake.snowpark.types import StringType, StructType, StructField
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz

def main(session: snowpark.Session,Param):
    # Param=[''NACF.csv'',''NTASDL_RAW.DEV_LOAD_STAGE_ADLS_KOR'',''dev/gt_sellout/transaction_files/nacf'',''SDL_KR_NACF_GT_SELLOUT'']
    try:
        file_name = Param[0]
        stage_name = Param[1]
        temp_stage_path = Param[2]
        target_table = Param[3]
        
        df_schema = StructType([
            StructField("customer_code", StringType(), True),
            StructField("account_name", StringType(), True),
            StructField("ims_txn_dt", StringType(), True),
            StructField("inspection_date", StringType(), True),
            StructField("scheduled_date_of_payment", StringType(), True),
            StructField("scheduled_delivery_number", StringType(), True),
            StructField("innovation_design", StringType(), True),
            StructField("pb", StringType(), True),
            StructField("sub_customer_code", StringType(), True),
            StructField("sub_customer_name", StringType(), True),
            StructField("economic_integration", StringType(), True),
            StructField("business_name", StringType(), True),
            StructField("supply_type", StringType(), True),
            StructField("system_contract_classification", StringType(), True),
            StructField("document_serial_number", StringType(), True),
            StructField("ean", StringType(), True),
            StructField("year_of_production", StringType(), True),
            StructField("trade_name", StringType(), True),
            StructField("product_standard_name", StringType(), True),
            StructField("tax_code_name", StringType(), True),
            StructField("wearing_weight", StringType(), True),
            StructField("quantity_of_goods", StringType(), True),
            StructField("unit_price", StringType(), True),
            StructField("sales_qty", StringType(), True),
            StructField("supply_amount", StringType(), True),
            StructField("purchase_tax", StringType(), True),
            StructField("purchase_amount", StringType(), True),
            StructField("commission_accout_code", StringType(), True),
            StructField("commission_accout_name", StringType(), True),
            StructField("Commission", StringType(), True),
            StructField("Commission_tax", StringType(), True),
            StructField("Commission_code", StringType(), True),
            StructField("Commission_name", StringType(), True),
            StructField("Transaction_type_code", StringType(), True),
            StructField("Transaction_type_name", StringType(), True),
            StructField("Correction/cancel_type", StringType(), True),
            StructField("Correction/cancel_type_name", StringType(), True),
            StructField("Delivery_Schedule_Write", StringType(), True)
         
        ])

        
        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "\\u0001")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
        
        df = df.na.drop("all")
        
        if df.count() == 0:
            return "No Data in file"

        def extract_buversion(data):
                listed = data.split(''.'')
                version = listed[0]
                return version

        df = df.with_column(''dstr_nm'', lit(extract_buversion(file_name)))
        df = df.withColumn("trade_name", lit("NULL"))
        # Remove additional characters after the first 10 characters in ims_txn_dt column
        df = df.withColumn("ims_txn_dt", col("ims_txn_dt").substr(1, 10))
            
        # df = df.withColumn("crt_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        df = df.withColumn("file_name", lit(file_name.split(".")[0]+".xlsx"))
        # df = df.withColumn("run_id", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))

        snowdf = df.select(
             "dstr_nm", 
            "customer_code",      
            "account_name", 
            "ims_txn_dt", 
            "inspection_date", 
            "scheduled_date_of_payment", 
            "scheduled_delivery_number", 
            "innovation_design", 
            "pb", 
            "sub_customer_code", 
            "sub_customer_name", 
            "economic_integration", 
            "business_name", 
            "supply_type", 
            "system_contract_classification", 
            "document_serial_number", 
            "ean",
            "year_of_production",
            "trade_name",
            "product_standard_name",
            "tax_code_name",
            "wearing_weight",
            "quantity_of_goods",
            "unit_price",
            "sales_qty",
            "supply_amount",
            "purchase_tax",
            "purchase_amount","file_name"
        )

        
        file_name=file_name.split(".")[0]+"_"+ datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        
        #move to success
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
        return error_message
';
create OR REPLACE PROCEDURE prod_dna_load.NTASDL_RAW.TW_POS_CARREFOUR_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them.
import snowflake.snowpark as snowpark
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DateType
from snowflake.snowpark.functions import col,lit,to_date
import pandas as pd
from datetime import datetime
import pytz
 
def main(session: snowpark.Session,Param):
 
    try:
 
        #Param=[''TW_POS_Carrefour_20240301_20240426002743.csv'',''NTASDL_RAW.DEV_LOAD_STAGE_ADLS_TWN'',''dev/pos/transaction/carrefour/'',''SDL_TW_POS_CARREFOUR'']
 
        # Extracting parameters from the input
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
 
 
        # Define the schema for the DataFrame
        df_schema=StructType([
                StructField("pos_date",StringType()),
                StructField("gln",StringType()),
                StructField("vendor",StringType()),
                StructField("store_no",StringType()),
                StructField("store_name",StringType()),
                StructField("product_code",StringType()),
                StructField("product_name_english",StringType()),
                StructField("product_name_chinese",StringType()),
                StructField("ean_code",StringType()),
                StructField("amount",StringType()),
                StructField("qty",StringType())
        ])
 
        # Read the CSV file into a DataFrame
        dataframe = session.read\\
            .schema(df_schema)\\
            .option("skip_header",0)\\
            .option("field_delimiter", "|")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .option("skip_blank_lines", True)\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)


        df = dataframe.toPandas()

        df[''POS_DATE''] = pd.to_datetime(df[''POS_DATE''], format=''%b %Y'')
        df[''POS_DATE''] = df[''POS_DATE''].dt.to_period(''M'').dt.to_timestamp()

        dataframe = session.create_dataframe(df)
 
        #---------------------------Transformation logic ------------------------------#
 
        
        #Handle null values or empty rows
       
        dataframe=dataframe.na.drop("all")
 
        # Check for empty Dataframe
        if dataframe.count()==0:
            return "No Data in file"
    
        dataframe = dataframe.with_column("upd_dttm", lit(None).cast("string"))
        dataframe = dataframe.withColumn("pos_date", to_date(col("pos_date")).cast("date"));
        dataframe = dataframe.with_column("crt_dttm",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        dataframe = dataframe.with_column("file_name", lit(file_name))
        # Creating copy of the Dataframe
        final_df = dataframe.select("pos_date","gln","vendor","store_no","store_name","product_code","product_name_english","product_name_chinese","ean_code","amount","qty","crt_dttm","upd_dttm","file_name")



        #Load Data to the target table
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




CREATE OR REPLACE PROCEDURE prod_dna_load.NTASDL_RAW.TW_POS_COSMED_PREPROCESSING("PARAM" ARRAY)
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
from snowflake.snowpark.functions import col,lit,replace,to_date
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField
import pandas as pd
from snowflake.snowpark.files import SnowflakeFile
from datetime import datetime
import pytz
 
def main(session: snowpark.Session,Param):
 
    try:
 
        #Param=[''TW_POS_Cosmed_RawData_20240415.csv'',''NTASDL_RAW.DEV_LOAD_STAGE_ADLS_TWN'',''dev/pos/transaction/cosmed'',''sdl_tw_pos_cosmed'']
 
        # Extracting parameters from the input
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
 
 
        # Read the file header separately to extract column names and date ranges
        df_columns = StructType([
            StructField(''PRODUCT_CODE'', StringType()),
            StructField(''PRODUCT_NAME'', StringType()),
            StructField(''WK1_QTY'', StringType()),
            StructField(''WK2_QTY'', StringType()),
            StructField(''WK3_QTY'', StringType()),
            StructField(''WK4_QTY'', StringType())
            ])
        df_schema = StructType([
            StructField(''PRODUCT_CO'', StringType()),
            StructField(''PRODUCT_NM'', StringType()),
            StructField(''WK1_QUANTITY'', StringType()),
            StructField(''WK2_QUANTITY'', StringType()),
            StructField(''WK3_QUANTITY'', StringType()),
            StructField(''WK4_QUANTITY'', StringType())
            ])
        
        df_header = session.read\\
            .schema(df_columns)\\
            .option("skip_header",0)\\
            .option("field_delimiter", "\\u0001")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)\\
            .limit(1)

        # Read the CSV file into a DataFrame

        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "\\u0001")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
        #---------------------------Transformation logic ------------------------------#

        df_pandas=df_header.to_pandas()

        for i in range(1, 5):  # Loop from 1 to 4 for WK1 to WK4
            col_name = f"WK{i}_QTY"  # Generate column name dynamically
            
            # Split values and extract start and end dates
            split_values = df_pandas[col_name].str.split("~", expand=True)
            start_dates = split_values[0].values
            end_dates = split_values[1].values
            
            # Assign start and end dates to respective columns
            df_pandas[f"WK{i}_STRT_DT"] = start_dates
            df_pandas[f"WK{i}_END_DT"] = end_dates
        df1 = df_pandas[["PRODUCT_CODE", "PRODUCT_NAME","WK1_STRT_DT","WK1_END_DT","WK1_QTY","WK2_STRT_DT","WK2_END_DT","WK2_QTY","WK3_STRT_DT","WK3_END_DT","WK3_QTY","WK4_STRT_DT","WK4_END_DT","WK4_QTY"]]
        dataframe = session.create_dataframe(df1)

        # Handle null values or empty rows
        dataframe=dataframe.na.drop("all")
 
        # Check for empty Dataframe
        if dataframe.count()==0:
            return "No Data in file"

        #return dataframe
        
        df = df.with_column("upd_dttm", lit(None).cast("string"))
        df = df.with_column("crt_dttm",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        df = df.select("PRODUCT_CO","PRODUCT_NM","WK1_QUANTITY","WK2_QUANTITY","WK3_QUANTITY","WK4_QUANTITY","upd_dttm","crt_dttm")
        dataframe = dataframe.withColumn("WK1_STRT_DT", to_date(col("WK1_STRT_DT"), "YYYYMMDD"))
        dataframe = dataframe.withColumn("WK1_END_DT", to_date(col("WK1_END_DT"), "YYYYMMDD"))
        dataframe = dataframe.withColumn("WK2_STRT_DT", to_date(col("WK2_STRT_DT"), "YYYYMMDD"))
        dataframe = dataframe.withColumn("WK2_END_DT", to_date(col("WK2_END_DT"), "YYYYMMDD"))
        dataframe = dataframe.withColumn("WK3_STRT_DT", to_date(col("WK3_STRT_DT"), "YYYYMMDD"))
        dataframe = dataframe.withColumn("WK3_END_DT", to_date(col("WK3_END_DT"), "YYYYMMDD"))
        dataframe = dataframe.withColumn("WK4_STRT_DT", to_date(col("WK4_STRT_DT"), "YYYYMMDD"))
        dataframe = dataframe.withColumn("WK4_END_DT", to_date(col("WK4_END_DT"), "YYYYMMDD"))
        df = df.with_column("file_name", lit(file_name.split(".")[0]+".xlsx"))
        
        final_df = df.join(dataframe)
        final_df = final_df.select("PRODUCT_CO","PRODUCT_NM","WK1_STRT_DT","WK1_END_DT","WK1_QUANTITY","WK2_STRT_DT","WK2_END_DT","WK2_QUANTITY","WK3_STRT_DT","WK3_END_DT","WK3_QUANTITY","WK4_STRT_DT","WK4_END_DT","WK4_QUANTITY","crt_dttm","upd_dttm","file_name")
 
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

CREATE OR REPLACE PROCEDURE prod_dna_load.NTASDL_RAW.GT_SELLOUT_LOTTE_LOGISTICS_YANG_JU_PREPROCESSING("PARAM" ARRAY)
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
            snowdf = snowdf.with_column(''file_name'',lit(file_name))

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
                                       ''cust_cd'',"file_name")

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

CREATE OR REPLACE PROCEDURE prod_dna_load.NTASDL_RAW.NA_KR_DU_BAE_RO_YU_TONG_GT_SELLOUT_PREPROCESSING("PARAM" ARRAY)
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
                    StructField("S_NO", StringType()),
                    StructField("Customer_Name", StringType()),
                    StructField("Date", StringType()),
                    StructField("EAN", StringType()),
                    StructField("Neck_Name", StringType()),
                    StructField("Standard", StringType()),
                    StructField("Unit", StringType()),
                    StructField("Qty", StringType()),
                    StructField("Unit_Price", StringType()),
                    StructField("Total", StringType())
                ])

        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",3)\\
            .option("field_delimiter", "\\u0001")\\
            .option("field_optionally_enclosed_by", "\\"") \\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
 
        valid_df=df.filter(col("Date").is_not_null()) and df.filter(col("EAN").is_not_null())

        dstr_nm = ('' ''.join(file_name.split(''_'')[:-1]))
        cust_cd = file_name.split(''_'')[-1:][0]
        cust_cd = cust_cd.split(''.'')[0]

        valid_df= valid_df.withColumn("DSTR_NM", lit(dstr_nm).cast("string"))
        valid_df= valid_df.withColumn("CUST_CD", lit(cust_cd).cast("string"))
        valid_df= valid_df.withColumn("file_name", lit(file_name.split(".")[0]+".xlsx"))

        final_df = valid_df.select("DSTR_NM", "S_NO", "Customer_Name", "Date", "EAN", "Neck_Name", "Standard", "Unit", "Qty", "Unit_Price", "Total", "CUST_CD","file_name").alias("final_df")

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


CREATE OR REPLACE PROCEDURE prod_dna_load.NTASDL_RAW.KR_IL_DONG_HU_DI_S_DEOK_SEONG_SANG_SA_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS 'import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col,lit,trim,when
from snowflake.snowpark.types import  StringType, StructType, StructField,IntegerType
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz



def main(session: snowpark.Session,Param): 
       
    try :
            
        #Param=["IL_DONG_HU_DI_S_DEOK_SEONG_SANG_SA_120718.csv","NTASDL_RAW.DEV_LOAD_STAGE_ADLS_KOR","prd/gt_sellout/transaction_files/food_ws/il_dong_hu_di_s_deok_seong_sang_sa","sdl_kr_il_dong_hu_di_s_deok_seong_sang_sa_gt_sellout"]

        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        # Define the schema for the DataFrame
        
        df_schema=StructType([
                        StructField("ccode", StringType()),
                        StructField("sub_customer_name", StringType()),
                        StructField("gcode", StringType()),
                        StructField("on_site_name", StringType()),
                        StructField("year", StringType()),
                        StructField("ims_txn_dt", StringType()),
                        StructField("transaction_number", StringType()),
                        StructField("product_classification", StringType()),
                        StructField("product_code", StringType()),
                        StructField("management_code", StringType()),
                        StructField("ean", StringType()),
                        StructField("prize_name", StringType()),
                        StructField("classification", StringType()),
                        StructField("rules", StringType()),
                        StructField("color", StringType()),
                        StructField("delivery_date", StringType()),
                        StructField("deliver", StringType()),
                        StructField("factory_status", StringType()),
                        StructField("number_of_goods", StringType()),
                        StructField("BOX", StringType()),
                        StructField("one_piece", StringType()),
                        StructField("qty", StringType()),
                        StructField("weight", StringType()),
                        StructField("list", StringType()),
                        StructField("unit_price_rate", StringType()),
                        StructField("box_danga", StringType()),
                        StructField("unit_price", StringType())
                        ])
                
        df = session.read.schema(df_schema) \\
        .option("skip_header",1) \\
        .option("field_delimiter","\\u0001") \\
        .option("encoding","UTF-8")\\
        .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
        

        
        df=df.na.drop("all")
        if df.count()==0:
            return "No Data in file"
        df=df.na.drop(subset=["ean"])

        df=df.with_column("dstr_nm",lit((" ").join(file_name.split("_")[0:-1])))
        df=df.with_column("cust_code",lit(file_name.split("_")[-1].split(".")[0]))
        df=df.with_column("file_name",lit(file_name.split(".")[0]+".xls"))

        
        # Load Data to the target table
        
        snowdf=df.select("dstr_nm",
                        "ccode",
                        "sub_customer_name",
                        "gcode",
                        "on_site_name",
                        "year",
                        "ims_txn_dt",
                        "transaction_number",
                        "product_classification",
                        "product_code",
                        "management_code",
                        "ean",
                        "prize_name",
                        "classification",
                        "rules",
                        "color",
                        "delivery_date",
                        "deliver",
                        "factory_status",
                        "number_of_goods",
                        "BOX",
                        "one_piece",
                        "qty",
                        "weight",
                        "list",
                        "unit_price_rate",
                        "box_danga",
                        "unit_price",
                         "cust_code","file_name"
                        )
        snowdf.write.mode("append").saveAsTable(target_table)
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        #move to success
        file_name=file_name.split(".")[0]+"_"+datetime.now().strftime("%Y%m%d%H%M%S")
        df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)

        
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


CREATE OR REPLACE PROCEDURE prod_dna_load.NTASDL_RAW.SDL_HK_IMS_VIVA_WING_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS 'from snowflake.snowpark.functions import col, lit, date_format, current_timestamp, to_date,md5, year, month, concat, format_number, regexp_replace, to_timestamp, when, trim, upper
from snowflake.snowpark.types import StringType, StructType, StructField,DecimalType
import snowflake.snowpark
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz

def main(session: snowpark.Session,Param):
    #Param=["HK_IMS_Wing_Keung_DataCube_20240518.txt","NTASDL_RAW.DEV_LOAD_STAGE_ADLS_hkg","dev/ims/wing-keung","SDL_HK_IMS_WINGKEUNG_SEL_OUT"]
    try:
        file_name = Param[0]
        stage_name = Param[1]
        temp_stage_path = Param[2]
        target_table = Param[3]

        df_schema = StructType([
                    StructField("calendar_sid", StringType(), True),
                    StructField("sales_office", StringType(), True),
                    StructField("sales_group", StringType(), True),
                    StructField("sales_office_name", StringType(), True),
                    StructField("sales_group_name", StringType(), True),
                    StructField("account_types", StringType(), True),
                    StructField("sales_volume", StringType(), True),
                    StructField("sales_order_quantity", StringType(), True),
                    StructField("net_trade_sales", StringType(), True),
                    StructField("customer_name", StringType(), True),
                    StructField("customer_number", StringType(), True),
                    StructField("base_product", StringType(), True),
                    StructField("variant", StringType(), True),
                    StructField("mvgr1_base", StringType(), True),
                    StructField("mvgr2_variant", StringType(), True),
                    StructField("mega_brand", StringType(), True),
                    StructField("brand", StringType(), True),
                    StructField("mvgr4_mega", StringType(), True),
                    StructField("mvgr5_brand", StringType(), True),
                    StructField("product_description", StringType(), True),
                    StructField("product_number", StringType(), True),
                    StructField("local_curr_exch_rate", StringType(), True),
                    StructField("employee", StringType(), True),
                    StructField("employee_name", StringType(), True),
                    StructField("transactiontype", StringType(), True),
                    StructField("return_reason", StringType(), True),
                    StructField("country_code", StringType(), True),
                    StructField("currency", StringType(), True)
                ])

        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",0)\\
            .option("field_delimiter", ",")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name.split(".")[0])
        
              
        df = df.na.drop("all")
        if df.count() == 0:
            return "No Data in file"


        df = df.withColumn("calendar_sid", regexp_replace(col("calendar_sid"), ''[^0-9]'', ""))
        df = df.withColumn("calendar_sid", to_date(col(''calendar_sid''),''yyyyMMdd''))
        df = df.withColumn("country_code", lit(''HK''))
        df = df.withColumn("currency", lit(''HKD''))        
         
        
        
        #df = df.withColumn("filename", lit(file_name.split(".")[0]+"_"+datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")+".csv"))
        df = df.withColumn("crt_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        df = df.withColumn("run_id", lit(None))
        df = df.withColumn("file_name", lit(file_name))
        
        snowdf = df.select(
                            "calendar_sid",
                            "sales_office",
                            "sales_group",
                            "sales_office_name",
                            "sales_group_name",
                            "account_types",
                            "sales_volume",
                            "sales_order_quantity",
                            "net_trade_sales",
                            "customer_name",
                            "customer_number",
                            "base_product",
                            "variant",
                            "mvgr1_base",
                            "mvgr2_variant",
                            "mega_brand",
                            "brand",
                            "mvgr4_mega",
                            "mvgr5_brand",
                            "product_description",
                            "product_number",
                            "local_curr_exch_rate",
                            "employee",
                            "employee_name",
                            "transactiontype",
                            "return_reason",
                            "country_code",
                            "currency",
                            "crt_dttm",
                            "run_id","file_name"
                        )
        
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        
        #move to success
        
        snowdf.write.mode("append").saveAsTable(stage_name.split(".")[0] + "." + target_table)
        file_name1 = file_name.split(".")[0] + "_" + datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S") + ".csv"
        snowdf.write.copy_into_location("@" + stage_name + "/" + temp_stage_path + "/processed/success/" + formatted_year + "/" + formatted_month + "/" + file_name1, header=True, OVERWRITE=True,file_format_name=''csv_format_comma'')
        
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

CREATE OR REPLACE PROCEDURE prod_dna_load.NTASDL_RAW.TW_POS_POYE_PREPROCESSING("PARAM" ARRAY)
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
from snowflake.snowpark.functions import col,lit,replace,to_date,to_timestamp
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField
import pandas as pd
from datetime import datetime
import pytz
 
def main(session: snowpark.Session,Param):
 
    try:
 
        #Param=[''TW_POS_Poya_RawData_20240402.csv'',''NTASDL_RAW.DEV_LOAD_STAGE_ADLS_TWN'',''dev/pos/transaction/poya/'',''sdl_tw_pos_poya'']
 
        # Extracting parameters from the input
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
 
 
        # Define the schema for the DataFrame
        df_schema=StructType([
            StructField("VENDOR_CODE",StringType()),
            StructField("DEPARTMENT",StringType()),
            StructField("CATEGORY_SMALL",StringType()),
            StructField("CUSTOMER_PRODUCT_CODE",StringType()),
            StructField("EAN_CODE",StringType()),
            StructField("PRODUCT_DESCRIPTION",StringType()),
            StructField("SELLING_QTY",StringType()),
            StructField("SELLING_AMT",StringType()),
            StructField("INVENTORY",StringType()),
            StructField("CHANGE_CODE",StringType()),
            StructField("CHANGE_DATE",StringType())
            ])
 
        # Read the CSV file into a DataFrame
        dataframe = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "\\u0001")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .option("skip_blank_lines", True)\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
 
 
        #---------------------------Transformation logic ------------------------------#
 
        # Handle null values or empty rows
        dataframe=dataframe.na.drop("all")
        uploadeddate = file_name.split("_")[-1].split(".")[0]
 
        # Check for empty Dataframe
        if dataframe.count()==0:
            return "No Data in file"
    
        dataframe = dataframe.with_column("UPD_DTTM", lit(None).cast("string"))
        dataframe = dataframe.withColumn("END_DATE", to_date(lit(uploadeddate),"YYYYMMDD"))
        dataframe = dataframe.withColumn("START_DATE", to_date(lit(uploadeddate),"YYYYMMDD"))
        dataframe = dataframe.with_column("CRT_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))

        dataframe = dataframe.with_column("file_name", lit(file_name.split(".")[0]+".xls"))
        # Creating copy of the Dataframe
        final_df = dataframe.select("VENDOR_CODE", "DEPARTMENT", "CATEGORY_SMALL", "CUSTOMER_PRODUCT_CODE", "EAN_CODE","PRODUCT_DESCRIPTION","SELLING_QTY","SELLING_AMT","INVENTORY","CHANGE_CODE",\\
                                    "CHANGE_DATE","START_DATE","END_DATE","CRT_DTTM","UPD_DTTM","file_name" )
 
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


CREATE OR REPLACE PROCEDURE prod_dna_load.NTASDL_RAW.NA_KR_DONGBU_LSD_GT_SELLOUT_PREPROCESSING("PARAM" ARRAY)
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
                    StructField("Date", StringType()),
                    StructField("number", StringType()),
                    StructField("sub_customer_name", StringType()),
                    StructField("total_amount", StringType()),
                    StructField("total_room_amount", StringType()),
                    StructField("EAN", StringType()),
                    StructField("product", StringType()),
                    StructField("unit", StringType()),
                    StructField("Qty", StringType()),
                    StructField("unit_price", StringType())
                ])

        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "\\u0001")\\
            .option("field_optionally_enclosed_by", "\\"") \\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
        
        dstr_nm = ('' ''.join(file_name.split(''_'')[:-1]))
        cust_cd = file_name.split(''_'')[-1]
        cust_cd = cust_cd.split(''.'')[0]

        df= df.withColumn("DSTR_NM", lit(dstr_nm).cast("string"))
        df= df.withColumn("CUST_CD", lit(cust_cd).cast("string"))

        df = df.filter(col("Date").is_not_null()) and df.filter(col("EAN").is_not_null())
        df= df.withColumn("file_name", lit(file_name.split(".")[0]+".xls"))
        
        final_df = df.select("DSTR_NM", "Date", "number", "sub_customer_name", "total_amount", "total_room_amount", "EAN", "product", "unit", "Qty", "unit_price", "CUST_CD","file_name").alias("final_df")

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



CREATE OR REPLACE PROCEDURE prod_dna_load.NTASDL_RAW.TW_POS_711_PREPROCESSING("PARAM" ARRAY)
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
from snowflake.snowpark.functions import col,lit,replace,to_date
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField
import pandas as pd
from datetime import datetime
import pytz
 
def main(session: snowpark.Session,Param):
 
    try:
 
        #Param=[''TW_POS_711_RawData_20201209.csv'',''NTASDL_RAW.DEV_LOAD_STAGE_ADLS_TWN'',''dev/pos/transaction/7-11/'',''sdl_tw_pos_7eleven'']
 
        # Extracting parameters from the input
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
 
 
        # Define the schema for the DataFrame
        df_schema=StructType([
            StructField("POS_DATE",StringType()),
            StructField("PRODUCT_CODE",StringType()),
            StructField("PRODUCT_DESCRIPTION",StringType()),
            StructField("SALES_QTY",StringType()),
            StructField("VENDOR_CODE",StringType()),
            StructField("VENDOR_DESCRIPTION",StringType()),
            ])
 
        # Read the CSV file into a DataFrame
        dataframe = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "\\u0001")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .option("skip_blank_lines", True)\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
 
 
        #---------------------------Transformation logic ------------------------------#
 
        # Handle null values or empty rows
        dataframe=dataframe.na.drop("all")
 
        # Check for empty Dataframe
        if dataframe.count()==0:
            return "No Data in file"
    
        dataframe = dataframe.with_column("UPD_DTTM", lit(None).cast("string"))
        dataframe = dataframe.with_column("CRT_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        dataframe = dataframe.with_column("file_name", lit(file_name.split(".")[0]+".xlsx"))
        # # Creating copy of the Dataframe
        final_df = dataframe.select("POS_DATE", "PRODUCT_CODE", "PRODUCT_DESCRIPTION", "SALES_QTY", "VENDOR_CODE","VENDOR_DESCRIPTION","CRT_DTTM","UPD_DTTM","file_name" )
 
        # # Load Data to the target table
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


CREATE OR REPLACE PROCEDURE prod_dna_load.NTASDL_RAW.NA_KR_GT_DAISO_PRICE_PREPROCESSING("PARAM" ARRAY)
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
                    StructField("EAN", StringType()),
                    StructField("Unit_Price", StringType())
                ])

        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "\\u0001")\\
            .option("field_optionally_enclosed_by", "\\"") \\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        dstr_nm = file_name.split(''_'')[0]
        df= df.withColumn("DSTR_NM", upper(lit(dstr_nm).cast("string")))
        df= df.withColumn("file_name", lit(file_name.split(".")[0]+".xlsx"))
        
        final_df = df.select("DSTR_NM", "EAN", "Unit_Price","file_name").alias("final_df")

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
CREATE OR REPLACE PROCEDURE prod_dna_load.NTASDL_RAW.J_KR_ONPACK_TARGET_WRAPPER_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('openpyxl==3.1.2','snowflake-snowpark-python==*','xlrd==2.0.1')
HANDLER = 'main'
EXECUTE AS OWNER
AS '
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
from snowflake.snowpark.files import SnowflakeFile
from snowflake.snowpark.functions import col, lit, current_timestamp, trim, upper
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DecimalType,DateType


from datetime import datetime
import pytz

from re import sub

def main(session:snowpark.Session, Param):
    try:
        # Param=[''KR_onpack_target_20240701.xlsx'',''NTASDL_RAW.DEV_LOAD_STAGE_ADLS_KOR'',''dev/onpack/transaction/'',''sdl_na_onpack_target'']

        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        file_header_loc = 10

        #Reading data from adls
        full_path = "@"+stage_name+"/"+temp_stage_path+"/"+file_name
        
        with SnowflakeFile.open(full_path,''rb'',require_scoped_url= False) as f:
            df = pd.read_excel(f)
            df = df.applymap(lambda x:str(x))
            # df = df[10:]
            # df1 = df[df.columns[2:]] 
            df1 = df[df.columns[:]] 
            df1.rename(columns = {df1.columns[0] : ''MATERIAL_NUMBER'',
                                  df1.columns[1] : ''DESCRIPTION'',
                                  df1.columns[2] : ''ACCOUNT_GROUP'',
                                  df1.columns[3] : ''MONTH_YEAR'',
                                  df1.columns[4] : ''TARGET_QUANTITY''}, inplace = True)
            # df1[''MONTH''], df1[''YEAR''] = df1[''MONTH_YEAR''].str.split(''-'', 1).str
            df1[[''MONTH'', ''YEAR'']] = df1[''MONTH_YEAR''].str.split(''-'', expand=True)
            df1[''COUNTRY_CODE''] = ''KR''
            df1[''CRT_DTTM''] = datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")
            df1[''UPDT_DTTM''] = pd.NaT
            df1 = df1[[''MATERIAL_NUMBER'', ''DESCRIPTION'', ''ACCOUNT_GROUP'', ''MONTH'', ''YEAR'', ''TARGET_QUANTITY'', ''COUNTRY_CODE'', ''CRT_DTTM'',''UPDT_DTTM'']]

            df1 = df1.applymap(lambda x: None if x== ''nan'' else x)
            
            snowparkdf = session.create_dataframe(df1)
            snowparkdf= snowparkdf.withColumn("file_name", lit(file_name))

            file_name=file_name.split(".")[0]+"_"+datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
            current_date = datetime.now()
            formatted_year = current_date.strftime("%Y")
            formatted_month = current_date.strftime("%m")

            #move to success
            snowparkdf.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)
    
            snowparkdf.write.mode("append").saveAsTable(stage_name.split(".")[0] + "." + target_table )

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



CREATE OR REPLACE PROCEDURE prod_dna_load.NTASDL_RAW.NA_KR_BO_YOUNG_JONG_HAP_LOGISTICS_GT_SELLOUT_PREPROCESSING("PARAM" ARRAY)
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
                    StructField("Date", StringType()),
                    StructField("Origin_Code", StringType()),
                    StructField("Customer_Name", StringType()),
                    StructField("EAN", StringType()),
                    StructField("Booklet_Code", StringType()),
                    StructField("Trade_Name", StringType()),
                    StructField("Standard", StringType()),
                    StructField("Unit", StringType()),
                    StructField("QTY", StringType()),
                    StructField("Unit_Price", StringType()),
                    StructField("COL11", StringType()),
                    StructField("COL12", StringType()),
                    StructField("COL13", StringType()),
                    StructField("COL14", StringType()),
                    StructField("COL15", StringType()),
                    StructField("COL16", StringType())
                ])

        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "\\u0001")\\
            .option("field_optionally_enclosed_by", "\\"") \\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        dstr_nm = ('' ''.join(file_name.split(''_'')[:-1]))
        cust_cd = file_name.split(''_'')[-1]
        cust_cd = cust_cd.split(''.'')[0]

        df= df.withColumn("DSTR_NM", lit(dstr_nm).cast("string"))
        df= df.withColumn("CUST_CD", lit(cust_cd).cast("string"))
        df= df.withColumn("file_name", lit(file_name.split(".")[0]+".xlsx"))

        final_df = df.select("DSTR_NM", "Date", "Origin_Code", "Customer_Name", "EAN", "Booklet_Code", "Trade_Name", "Standard", "Unit", "QTY", "Unit_Price", "CUST_CD","file_name").alias("final_df")

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


CREATE OR REPLACE PROCEDURE prod_dna_load.NTASDL_RAW.TW_POS_PXCIVILIA_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS 'from snowflake.snowpark.functions import col, lit, date_format, current_timestamp, to_date, year, month, concat, format_number, regexp_replace,to_timestamp,when,trim,upper, substring
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DecimalType
import snowflake.snowpark
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz
import pandas as pd

def main(session: snowpark.Session,Param):
    # Param=["TW_POS_PXCivilia_20240428.csv","NTASDL_RAW.DEV_LOAD_STAGE_ADLS_TWN","cert-data-lake/pos/px-civilia/","SDL_TW_POS_PX_CIVILA"]
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        
        df_schema = StructType([
                # StructField("mnth_id",StringType(), True),
                StructField("store_code",StringType(), True),
                StructField("store_name_chinese",StringType(), True),
                StructField("pos_date",StringType(), True),
                StructField("ean_code",StringType(), True),
                StructField("civilian_product_code",StringType(), True),
                StructField("brand",StringType(), True),
                StructField("product_name",StringType(), True),
                StructField("dc",StringType(), True),
                StructField("unit_price",StringType(), True),
                StructField("stock_receive_qty_by_store",StringType(), True),
                StructField("stock_selling_qty_by_store",StringType(), True),
                StructField("stock_inventory_qty",StringType(), True),
                StructField("stock_return_qty_by_store",StringType(), True),
                StructField("stock_receive_amt_by_store",StringType(), True),
                StructField("stock_selling_amt_by_store",StringType(), True),
                StructField("stock_inventory_amt",StringType(), True),
                StructField("stock_return_amt_by_store",StringType(), True)
                # StructField("crt_dttm",StringType(), True),
                # StructField("upd_dttm",StringType(), True),
    
                
            ])
        
        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "\\u0001")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .option("encoding","UTF-8")\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
    
        # df.print()
        
        df=df.na.drop("all")
        if df.count()==0 :
            return "No Data in file"
    
        
        # df = df.with_column("cdl_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        file_name1=file_name.split(".")[0]+"_"+ datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")+".csv"
        df = df.with_column("file_name", lit(file_name.split(".")[0]+".xlsx"))
        df = df.with_column("crt_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        df = df.with_column("upd_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        # extrctd_month_id=file_name.split("_")[1].split(".")[0]
        # df = df.with_column("mnth_id", lit(extrctd_month_id))
        
        # date=file_name.split("_")[3].split(".")[0]
        # year=date[0:4]
        # mnth_no=date[4:6]
        # inv_week=date[6:]
        # df = df.withColumn("year", lit(year))
        # df = df.with_column("mnth_no", lit(mnth_no))
        # df = df.with_column("inv_week", lit(inv_week))  
        
        snowdf= df.select(
                    "store_code",
                    "store_name_chinese",
                    "pos_date",
                    "ean_code",
                    "civilian_product_code",
                    "brand",
                    "product_name",
                    "dc",
                    "unit_price",
                    "stock_receive_qty_by_store",
                    "stock_selling_qty_by_store",
                    "stock_inventory_qty",
                    "stock_return_qty_by_store",
                    "stock_receive_amt_by_store",
                    "stock_selling_amt_by_store",
                    "stock_inventory_amt",
                    "stock_return_amt_by_store",
                    "crt_dttm",
                    "upd_dttm","file_name"
                    )
        file_name=file_name.split(".")[0]+"_"+datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        
        #move to success
        snowdf.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)
        
        snowdf.write.mode("append").saveAsTable(stage_name.split(".")[0] + "." + target_table)
        
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
        return error_message';


CREATE OR REPLACE PROCEDURE prod_dna_load.NTASDL_RAW.GT_SELLOUT_HYUNDAI_PREPROCESSING("PARAM" ARRAY)
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
            
            snowdf = snowdf.with_column(''file_name'',lit(file_name))


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
                                       ''cust_cd'',"file_name")

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

CREATE OR REPLACE PROCEDURE prod_dna_load.NTASDL_RAW.NA_KR_DA_IN_SANG_SA_GT_SELLOUT_PREPROCESSING("PARAM" ARRAY)
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
                    StructField("DATE", StringType()),
                    StructField("KEJO", StringType()),
                    StructField("GURNAME", StringType()),
                    StructField("ITEM", StringType()),
                    StructField("GYU", StringType()),
                    StructField("BIGO", StringType()),
                    StructField("EA", StringType()),
                    StructField("PRICE", StringType()),
                    StructField("GUM", StringType()),
                    StructField("VAT", StringType()),
                    StructField("GUMVAT", StringType()),
                    StructField("CODE2", StringType())
                ])

        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "\\u0001")\\
            .option("field_optionally_enclosed_by", "\\"") \\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        dstr_nm = ('' ''.join(file_name.split(''_'')[:-1]))
        cust_cd = file_name.split(''_'')[-1]
        cust_cd = cust_cd.split(''.'')[0]

        df= df.withColumn("DSTR_NM", lit(dstr_nm).cast("string"))
        df= df.withColumn("CUST_CD", lit(cust_cd).cast("string"))
        df= df.withColumn("file_name", lit(file_name.split(".")[0]+".xlsx"))

        final_df = df.select("DSTR_NM", "DATE", "KEJO", "GURNAME", "ITEM", "GYU", "BIGO", "EA", "PRICE", "GUM", "VAT", "GUMVAT", "CODE2",  "CUST_CD","file_name").alias("final_df")

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
CREATE OR REPLACE PROCEDURE prod_dna_load.NTASDL_RAW.NA_TRXN_IMS_INV_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS 'from snowflake.snowpark.functions import col, lit, date_format, current_timestamp, to_date, year, month, concat, format_number, regexp_replace,to_timestamp,when,trim,upper, substring
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DecimalType
import snowflake.snowpark
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz
import pandas as pd

def main(session: snowpark.Session,Param):
    # Param=["HK_IMS_Irpt_wingkeung_20240331_20240405011609.csv","NTASDL_RAW.DEV_LOAD_STAGE_ADLS_HKG","ims/transaction/wing-keung/HK_IMS_Irpt_wingkeung/","SDL_HK_IMS_WINGKEUNG_INV"]
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        
        df_schema = StructType([
                # StructField("mnth_id",StringType(), True),
                StructField("date",StringType(), True),
                StructField("stk_code",StringType(), True),
                StructField("prod_code",StringType(), True),
                StructField("chn_desp",StringType(), True),
                StructField("chn_uom",StringType(), True),
                StructField("closing",StringType(), True),
                StructField("amount",StringType(), True)
            ])
        
        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", ",")\\
            .option("encoding","Big5")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
    
        # df.show(8)
        
        df=df.na.drop("all")
        if df.count()==0 :
            return "No Data in file"
        
        # df = df.with_column("cdl_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        file_name1=file_name.split(".")[0]+"_"+ datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")+".csv"
        df = df.with_column("file_name", lit(file_name))
        df = df.with_column("crt_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        df = df.with_column("updt_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        # extrctd_month_id=file_name.split("_")[1].split(".")[0]
        # df = df.with_column("mnth_id", lit(extrctd_month_id))
        
        # date=file_name.split("_")[3].split(".")[0]
        # year=date[0:4]
        # mnth_no=date[4:6]
        # inv_week=date[6:]
        # df = df.withColumn("year", lit(year))
        # df = df.with_column("mnth_no", lit(mnth_no))
        # df = df.with_column("inv_week", lit(inv_week))  
        
        snowdf= df.select(
                    "date",
                    "stk_code",
                    "prod_code",
                    "chn_desp",
                    "chn_uom",
                    regexp_replace(df.col("closing"),",","").as_("closing"),
                    regexp_replace(df.col("amount"),",","").as_("Amount"),
                    "crt_dttm",
                    "updt_dttm","file_name"
                    )
        
        file_name=file_name.split(".")[0]+"_"+datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        
        #move to success
        snowdf.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)
        
        snowdf.write.mode("append").saveAsTable(stage_name.split(".")[0] + "." + target_table)
        
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
        return error_message';
CREATE OR REPLACE PROCEDURE prod_dna_load.NTASDL_RAW.KR_GT_SELLOUT_JUNGSEOK_PREPROCESSING("PARAM" ARRAY)
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
from snowflake.snowpark.types import  StringType, StructType, StructField,IntegerType
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz



def main(session: snowpark.Session,Param): 
       
    try :
            
        # Param=[''JUNGSEOK_135475_202107.csv'',''NTASDL_RAW.DEV_LOAD_STAGE_ADLS_KOR'',''dev/gt_sellout/transaction_files/food_ws/jungseok'',''SDL_KR_JUNGSEOK_GT_SELLOUT'']

        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        # Define the schema for the DataFrame
        df_schema=StructType([
        StructField(''S_NO'', StringType()),
        StructField(''SUB_CUSTOMER_NAME '', StringType()),
        StructField(''EAN'', StringType()),
        StructField(''NECK_NAME '', StringType()),
        StructField(''THE_RULES'', StringType()),
        StructField(''MASTER_FILE_NAME '', StringType()),
        StructField(''QTY'', StringType()),
        StructField(''AMOUNT'', StringType()),
        StructField(''TAX'', StringType()),
        StructField(''TOTAL'', StringType())
        ])
                
        dataframe = session.read.schema(df_schema) \\
        .option("skip_header",1) \\
        .option("field_delimiter","\\\\u0001") \\
        .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
        

        
        dataframe=dataframe.na.drop("all")
        if dataframe.count()==0:
            return "No Data in file"

        dstr_nm = (''''.join(file_name.split(''_'')[:-1]))
        imx_txn_dt = file_name.split(''_'')[1]
        cust_cd = file_name.split(''_'')[-1]
        cust_cd = cust_cd.split(''.'')[0]

        dataframe= dataframe.withColumn("DSTR_NM", lit(dstr_nm).cast("string"))
        dataframe= dataframe.withColumn("IMS_TXN_DT", lit(imx_txn_dt).cast("string"))
        dataframe= dataframe.withColumn("CUST_CD", lit(cust_cd).cast("string"))

        dataframe = dataframe.filter(col("EAN").is_not_null())
        dataframe= dataframe.withColumn("file_name", lit(file_name.split(".")[0]+".xlsx"))

        dataframe=dataframe[[
            ''DSTR_NM'',
            ''IMS_TXN_DT'',
            ''S_NO'',
            ''SUB_CUSTOMER_NAME '',
            ''EAN'',
            ''NECK_NAME '',
            ''THE_RULES'',
            ''MASTER_FILE_NAME '',
            ''QTY'',
            ''AMOUNT'',
            ''TAX'',
            ''TOTAL'',
            ''CUST_CD'',"file_name"
        ]]
        
        # Load Data to the target table
        dataframe.write.mode("append").saveAsTable(target_table)
        
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        #move to success
        file_name=file_name.split(".")[0]+''_''+datetime.now().strftime("%Y%m%d%H%M%S")
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
        return error_message';

CREATE OR REPLACE PROCEDURE prod_dna_load.NTASDL_RAW.NA_KR_GT_DPT_DAISO_PREPROCESSING("PARAM" ARRAY)
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
                    StructField("Sub_Customer_Name", StringType()),
                    StructField("Sub_Customer_Code", StringType()),
                    StructField("Customer_Code", StringType())
                ])

        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "\\u0001")\\
            .option("field_optionally_enclosed_by", "\\"") \\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        df= df.withColumn("file_name", lit(file_name.split(".")[0]+".xlsx"))


        final_df = df.select("Sub_Customer_Name", "Sub_Customer_Code", "Customer_Code","file_name").alias("final_df")

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

CREATE OR REPLACE PROCEDURE prod_dna_load.NTASDL_RAW.GT_SELLOUT_LOTTE_AK_PROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('openpyxl==3.1.2','snowflake-snowpark-python==*')
HANDLER = 'main'
EXECUTE AS OWNER
AS ' # The Snowpark package is required for Python Worksheets. 
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

        # Param=[''Lotte_AK_126137_202311.xlsx'',''NTASDL_RAW.DEV_LOAD_STAGE_ADLS_KOR'',''dev/gt_sellout/transaction_files/department_store/lotte_ak/'',''sdl_kr_lotte_ak_gt_sellout'']

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
            dataframe_list = []

            try:
            
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
    
                dataframe_list.append(df1)

            except Exception as e:
                pass

            
#----------------------------------------------------------------------------------------------------------------------------------------------

            try:
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
    
                dataframe_list.append(df2)

            except Exception as e:
                pass
            # #---------------------------------------------------------------------------------------------------------------------------------------
            try:
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
    
                dataframe_list.append(df3)

            except Exception as e:
                pass

            # #---------------------------------------------------------------------------------------------------
            try:
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

                dataframe_list.append(df4)

            except Exception as e:
                pass
            
            
            
            # #--------------------------------------------------------------------------------------------------
            try:
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

                dataframe_list.append(df5)

            except Exception as e:
                pass

            
            # #------------------------------------------------------------------------------------------

            try:
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

                dataframe_list.append(df6)

            except Exception as e:
                pass

            
            # #---------------------------------------------------------------------------

            try:
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

                dataframe_list.append(df7)

            except Exception as e:
                pass

            
            # #--------------------------------------------------------------------------
            try:
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

                dataframe_list.append(df8)

            except Exception as e:
                pass

            
            
            # #-----------------------------------------------------------------------
            try:
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
    
                dataframe_list.append(df9)

            except Exception as e:
                pass

                
            
            # #------------------------------------------------------------------------------------------------------------------
            try:
            
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

                dataframe_list.append(df10)

            except Exception as e:
                pass
                
                # #------------------------------------------------------------------------------------------

            try:    
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

                dataframe_list.append(df11)

            except Exception as e:
                pass
            
            # #--------------------------------------------------------------------

            try:
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

                dataframe_list.append(df12)

            except Exception as e:
                pass
            
            # #-----------------------------------------------------------------------------------
            try:
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

                dataframe_list.append(df13)

            except Exception as e:
                pass
                
                
                # #-----------------------------------------------------------------------
            try:
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

                dataframe_list.append(df14)

            except Exception as e:
                pass
                
            # #-------------------------------------------------------------------------
            try:
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

                dataframe_list.append(df15)

            except Exception as e:
                pass
                
            # #-------------------------------------------------------------------

            try:

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

                dataframe_list.append(df16)

            except Exception as e:
                pass
            
            # #---------------------------------------------------------------------------------
            try:
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

                dataframe_list.append(df17)

            except Exception as e:
                pass
            
            # #--------------------------------------------------------------------
            try:
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

                dataframe_list.append(df18)

            except Exception as e:
                pass
        #-----------------------------------------------------------------------------------------------------------------------
            try:
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

                dataframe_list.append(df19)

            except Exception as e:
                pass
        
            #-------------------------------------------------------------------------------------------
            try:
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

                dataframe_list.append(df20)

            except Exception as e:
                pass
            #---------------------------------------------------------------------
            try:
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

                dataframe_list.append(df21)

            except Exception as e:
                pass
        #----------------------------------------------------------------------------------------------------
            try:
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

                dataframe_list.append(df22)

            except Exception as e:
                pass

        #-------------------------------------------------------------------------------------------------------
            try:
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

                dataframe_list.append(df23)

            except Exception as e:
                pass

        #-----------------------------------------------------------------------------
            try:
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

                dataframe_list.append(df24)

            except Exception as e:
                pass

        #------------------------------------------------------------------------------------
            try:
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

                dataframe_list.append(df25)

            except Exception as e:
                pass

        #--------------------------------------------------------------------------------------------------
            try:
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

                dataframe_list.append(df26)

            except Exception as e:
                pass

        #----------------------------------------------------------------------
            try:
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

                dataframe_list.append(df27)

            except Exception as e:
                pass

        #----------------------------------------------------------------------
            try:
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

                dataframe_list.append(df28)

            except Exception as e:
                pass

        #------------------------------------------------------------------------------------------
            try:
                
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

                dataframe_list.append(df29)

            except Exception as e:
                pass

        #----------------------------------------------------------------------------------------------------
            try:
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

                dataframe_list.append(df30)

            except Exception as e:
                pass

        #----------------------------------------------------------------------------------------

            try:
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

                dataframe_list.append(df31)

            except Exception as e:
                pass

        #-------------------------------------------------------------------------------
            try:

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
                dataframe_list.append(df32)
                
            except Exception as e:
                pass
                
            # df = pd.concat([df1,df2,df3,df4,df5,df6,df7,df8,df9,df10,df11,df12,df13,df14,df15,df16,df17,df18,df19,df20,df21,df22,df23,df24,df25,df26,df27,df28,df29,df30,df31,df32],axis = 0)
            df = pd.concat(dataframe_list)
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
            snowdf= snowdf.withColumn("file_name", lit(file_name.split(".")[0]+".xlsx"))


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
                                       ''cust_cd'',"file_name")

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
        return error_message;

        ';



CREATE OR REPLACE PROCEDURE prod_dna_load.NTASDL_RAW.NA_KR_DAISO_GT_SELLOUT_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python','pandas')
HANDLER = 'main'
EXECUTE AS OWNER
AS '
from snowflake.snowpark.functions import col, lit, date_format, current_timestamp, to_date, year, month, concat, format_number, regexp_replace,to_timestamp,when,trim,upper
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DecimalType,DateType
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz
import pandas


def main(session: snowpark.Session,Param):
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        sch_name        = stage_name.split(''.'')[0]
        target_table    = sch_name+"."+Param[3]
        
        df_schema = StructType([
                    StructField("COL01", StringType()),
                    StructField("COL02", StringType()),
                    StructField("COL03", StringType()),
                    StructField("EAN", StringType()),
                    StructField("NAME", StringType()),
                    StructField("01", StringType()),
                    StructField("02", StringType()),
                    StructField("03", StringType()),
                    StructField("04", StringType()),
                    StructField("05", StringType()),
                    StructField("06", StringType()),
                    StructField("07", StringType()),
                    StructField("08", StringType()),
                    StructField("09", StringType()),
                    StructField("10", StringType()),
                    StructField("11", StringType()),
                    StructField("12", StringType()),
                    StructField("COL18", StringType()),
                    StructField("COL19", StringType()),
                    StructField("COL20", StringType())
                ])

        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",0)\\
            .option("field_delimiter", "\\u0001")\\
            .option("field_optionally_enclosed_by", "\\"") \\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        pd_df = df.to_pandas()
        
        name = pd_df.iloc[0]["COL02"]
        df = df.withColumn("CUSTOMER_NAME", lit(name).cast("string"))

        filenamepart, extension = file_name.split(''.'')
        dstr_nm , cust_cd, year = filenamepart.split(''_'')

        df = df.filter(col("NAME").is_not_null()) and df.filter((upper(col("01")) != "QTY") & (upper(col("NAME")) != "TOTAL"))
        
        df= df.withColumn("DSTR_NM", upper(lit(dstr_nm).cast("string")))
        df= df.withColumn("Year", lit(year).cast("string"))
        df= df.withColumn("CUST_CD", lit(cust_cd).cast("string"))

        df2 = df.unpivot("QTY","MONTH",["01","02","03","04","05","06","07","08","09","10","11","12"])
        file_df = df2.select("DSTR_NM", "YEAR", "MONTH", "EAN", "NAME", "QTY", "Customer_Name", "CUST_CD").alias("file_df")
        
        df = df.na.fill("", ["01","02","03","04","05","06","07","08","09","10","11","12"])
        
        df = df.unpivot("QTY","MONTH",["01","02","03","04","05","06","07","08","09","10","11","12"])
        df= df.withColumn("file_name", lit(file_name.split(".")[0]+".xlsx"))
        
        final_df = df.select("DSTR_NM", "YEAR", "MONTH", "EAN", "NAME", "QTY", "Customer_Name", "CUST_CD","file_name").alias("final_df")

        if final_df.count()==0:
            return "No Data in file"
        
        # Load Data to the target table
        final_df.write.mode("append").saveAsTable(target_table)

        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")

        # write to success folder
    
        file_name=file_name.split(".")[0] + ''_'' + datetime.now().strftime("%Y%m%d%H%M%S")
        file_df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,file_format_type="csv",OVERWRITE=True,header=True)   
        
        return "Success"
        
    except KeyError as key_error:
        
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message
        
    except Exception as e:
        
        error_message = f"Error: {str(e)}" +str(df.columns)
        return error_message
';
