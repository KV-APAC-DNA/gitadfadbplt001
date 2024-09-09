CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.PHLSDL_RAW.PH_DMS_SELLOUT_STOCK_FACT_PREPROCESSING(""PARAM"" ARRAY)
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

def main(session: snowpark.Session,Param):
    # Param=[""OUT_CON_I_PH_20240331224500_20240401014431.csv"",""PHLSDL_RAW.DEV_LOAD_STAGE_ADLS"",""dev/ap_ph_metadata/dms/inventory"",""sdl_ph_dms_sellout_stock_fact""]
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        
        df_schema = StructType([
                StructField(""dstrbtr_grp_cd"",StringType(), True),
                StructField(""cntry_cd"",StringType(), True),
                StructField(""wh_cd"",StringType(), True),
                StructField(""invoice_dt"",StringType(), True),
                StructField(""dstrbtr_prod_id"",StringType(), True),
                StructField(""qty"",StringType(), True),
                StructField(""uom"",StringType(), True),
                StructField(""amt"",StringType(), True)
            ])
        
        df = session.read\\
            .schema(df_schema)\\
            .option(""skip_header"",1)\\
            .option(""field_delimiter"", "","")\\
            .option(""field_optionally_enclosed_by"", ""\\"""")\\
            .csv(""@""+stage_name+""/""+temp_stage_path+""/""+file_name)

       
        
        df=df.na.drop(""all"")
        if df.count()==0 :
            return ""No Data in file""

        
        df = df.with_column(""cdl_dttm"", lit(file_name.split(""_"")[-1].split(""."")[0]))
        file_name1=file_name.split(""."")[0]+""_""+ datetime.now(pytz.timezone(""Asia/Singapore"")).strftime(""%Y%m%d%H%M%S"")+"".csv""
        # df = df.with_column(""file_name"", lit(file_name1))
        df = df.with_column(""curr_date"", lit(datetime.now(pytz.timezone(""Asia/Singapore"")).strftime(""%Y-%m-%d %H:%M:%S"")))
        df = df.withColumn(""amt"", regexp_replace(col(""amt""),  "","", """"))
        df = df.withColumn(""qty"", regexp_replace(col(""qty""),  "","", """"))
        df = df.with_column(""FILE_NAME"", lit(file_name))


        snowdf= df.select(
                    ""dstrbtr_grp_cd"",
                    ""cntry_cd"",
                    ""wh_cd"",
                    ""invoice_dt"",
                    ""dstrbtr_prod_id"",
                    ""qty"",
                    ""uom"",
                    ""amt"",
                    ""cdl_dttm"",
                    ""curr_date"",""FILE_NAME""
                    )
        file_name=file_name.split(""."")[0]+""_""+datetime.now(pytz.timezone(""Asia/Singapore"")).strftime(""%Y%m%d%H%M%S"")
        current_date = datetime.now()
        formatted_year = current_date.strftime(""%Y"")
        formatted_month = current_date.strftime(""%m"")
        
        #move to success
        snowdf.write.copy_into_location(""@""+stage_name+""/""+temp_stage_path+""/""+""processed/success/""+formatted_year+""/""+formatted_month+""/""+file_name,header=True,OVERWRITE=True)
        
        snowdf.write.mode(""append"").saveAsTable(stage_name.split(""."")[0] + ""."" + target_table)
        
        return ""Success""

    
    except KeyError as key_error:
        # Handle KeyError (missing columns) here
        error_message = f""KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame.""
        return error_message

    except pd.errors.MergeError as merge_error:
        # Handle DataFrame merging error
        error_message = f""DataFrame merging error: {str(merge_error)}""
        return error_message

    except Exception as e:
        # Handle exceptions here
        error_message = f""Error: {str(e)}""
        return error_message
        
        
        
        
';

CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.PHLSDL_RAW.PH_POS_711_PREPROCESSING(""PARAM"" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '# CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.PHLSDL_RAW.PH_POS_711_PREPROCESSING(""PARAM"" ARRAY)
# RETURNS VARCHAR(16777216)
# LANGUAGE PYTHON
# RUNTIME_VERSION = ''3.11''
# PACKAGES = (''snowflake-snowpark-python'')
# HANDLER = ''main''
# EXECUTE AS OWNER
# AS ''
from snowflake.snowpark.functions import col, lit, date_format, current_timestamp, to_date, year, month, concat, format_number, regexp_replace,to_timestamp,when,trim,upper, substring
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DecimalType
import snowflake.snowpark
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz
import pandas as pd

def main(session: snowpark.Session,Param):
    # Param=[""Sales711_202401.csv"",""PHLSDL_RAW.DEV_LOAD_STAGE_ADLS"",""dev/pos/711/transaction_data"",""SDL_PH_POS_711""]
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        
        df_schema = StructType([
                # StructField(""mnth_id"",StringType(), True),
                StructField(""store_cd"",StringType(), True),
                StructField(""store_nm"",StringType(), True),
                StructField(""Dummy_1"",StringType(), True),
                StructField(""item_category"",StringType(), True),
                StructField(""Dummy_2"",StringType(), True),
                StructField(""Dummy_3"",StringType(), True),
                StructField(""item_cd"",StringType(), True),
                StructField(""Dummy_4"",StringType(), True),
                StructField(""item_nm"",StringType(255), True),
                StructField(""tot_qty"",StringType(), True),
                StructField(""tot_amt"",StringType(), True),
                StructField(""no_of_sls_days"",StringType(), True),
                StructField(""avg_day_qty"",StringType(), True),
                StructField(""avg_amt_day"",StringType(), True)
            ])
        
        df = session.read\\
            .schema(df_schema)\\
            .option(""skip_header"",1)\\
            .option(""field_delimiter"", ""\\u0001"")\\
            .option(""field_optionally_enclosed_by"", ""\\"""")\\
            .csv(""@""+stage_name+""/""+temp_stage_path+""/""+file_name)

        
        
        df=df.na.drop(""all"")
        if df.count()==0 :
            return ""No Data in file""
        df=df.na.drop(subset=[
                    ""store_nm"",
                    ""item_category"",
                    ""item_cd"",
                    ""item_nm"",
                    ""tot_qty"",
                    ""tot_amt"",
                    ""no_of_sls_days"",
                    ""avg_day_qty"",
                    ""avg_amt_day""])
        
        df = df.with_column(""cdl_dttm"", lit(datetime.now(pytz.timezone(""Asia/Singapore"")).strftime(""%Y%m%d%H%M%S"")))
        #file_name1=file_name.split(""."")[0]+""_""+ datetime.now(pytz.timezone(""Asia/Singapore"")).strftime(""%Y%m%d%H%M%S"")+"".csv""
        #df = df.with_column(""file_name"", lit(file_name1))
        df = df.with_column(""crtd_dttm"", lit(datetime.now(pytz.timezone(""Asia/Singapore"")).strftime(""%Y-%m-%d %H:%M:%S"")))
        extrctd_month_id=file_name.split(""_"")[1].split(""."")[0]
        df = df.with_column(""mnth_id"", lit(extrctd_month_id))
        df = df.with_column(""FILE_NAME"", lit(file_name.split(""."")[0]+"".xlsx""))
        
        # date=file_name.split(""_"")[3].split(""."")[0]
        # year=date[0:4]
        # mnth_no=date[4:6]
        # inv_week=date[6:]
        # df = df.withColumn(""year"", lit(year))
        # df = df.with_column(""mnth_no"", lit(mnth_no))
        # df = df.with_column(""inv_week"", lit(inv_week))  
        
        snowdf= df.select(
                    ""mnth_id"",
                    ""store_cd"",
                    ""store_nm"",
                    ""item_category"",
                    ""item_cd"",
                    ""item_nm"",
                    ""tot_qty"",
                    ""tot_amt"",
                    ""no_of_sls_days"",
                    ""avg_day_qty"",
                    ""avg_amt_day"",
                    ""FILE_NAME""
                    )
        file_name=file_name.split(""."")[0]+""_""+datetime.now(pytz.timezone(""Asia/Singapore"")).strftime(""%Y%m%d%H%M%S"")
        current_date = datetime.now()
        formatted_year = current_date.strftime(""%Y"")
        formatted_month = current_date.strftime(""%m"")
        
        #move to success
        snowdf.write.copy_into_location(""@""+stage_name+""/""+temp_stage_path+""/""+""processed/success/""+formatted_year+""/""+formatted_month+""/""+file_name,header=True,OVERWRITE=True)
        
        snowdf.write.mode(""append"").saveAsTable(stage_name.split(""."")[0] + ""."" + target_table)
        
        return ""Success""

    
    except KeyError as key_error:
        # Handle KeyError (missing columns) here
        error_message = f""KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame.""
        return error_message

    except pd.errors.MergeError as merge_error:
        # Handle DataFrame merging error
        error_message = f""DataFrame merging error: {str(merge_error)}""
        return error_message

    except Exception as e:
        # Handle exceptions here
        error_message = f""Error: {str(e)}""
        return error_message';

CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.PHLSDL_RAW.PH_POS_DYNA_PREPROCESSING(""PARAM"" ARRAY)
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
    # Param=[""DYNA_DYO_202311.csv"",""PHLSDL_RAW.DEV_LOAD_STAGE_ADLS"",""dev/pos/dyna/transaction_data"",""SDL_PH_POS_DYNA_SALES""]
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        
        df_schema = StructType([
                # StructField(""mnth_id"",StringType(), True),
                StructField(""sls_area"",StringType(), True),
                StructField(""plant"",StringType(), True),
                StructField(""customer_id"",StringType(), True),
                StructField(""old_cust_id"",StringType(), True),
                StructField(""cust_nm"",StringType(), True),
                StructField(""chnl"",StringType(), True),
                StructField(""sls_off"",StringType(), True),
                StructField(""sls_grp"",StringType(), True),
                StructField(""address"",StringType(), True),
                StructField(""city"",StringType(), True),
                StructField(""postal_cd"",StringType(), True),
                StructField(""dsm"",StringType(), True),
                StructField(""sman"",StringType(), True),
                StructField(""prin"",StringType(), True),
                StructField(""principal"",StringType(), True),
                StructField(""matl_grp"",StringType(), True),
                StructField(""matl_sub_grp"",StringType(), True),
                StructField(""brand"",StringType(), True),
                StructField(""uom_conv"",StringType(), True),
                StructField(""matl_num"",StringType(), True),
                StructField(""old_matl_num"",StringType(), True),
                StructField(""matl_desc"",StringType(), True),
                StructField(""qty"",StringType(), True),
                StructField(""sls_amt"",StringType(), True)
            ])
        
        df = session.read\\
            .schema(df_schema)\\
            .option(""skip_header"",1)\\
            .option(""field_delimiter"", ""\\u0001"")\\
            .option(""field_optionally_enclosed_by"", ""\\"""")\\
            .csv(""@""+stage_name+""/""+temp_stage_path+""/""+file_name)

        
        
        df=df.na.drop(""all"")
        if df.count()==0 :
            return ""No Data in file""
        # df=df.na.drop(subset=[
        #             ""store_nm"",
        #             ""item_category"",
        #             ""item_cd"",
        #             ""item_nm"",
        #             ""tot_qty"",
        #             ""tot_amt"",
        #             ""no_of_sls_days"",
        #             ""avg_day_qty"",
        #             ""avg_amt_day""])
        
        df = df.with_column(""cdl_dttm"", lit(datetime.now(pytz.timezone(""Asia/Singapore"")).strftime(""%Y%m%d%H%M%S"")))
        #file_name1=file_name.split(""."")[0]+""_""+ datetime.now(pytz.timezone(""Asia/Singapore"")).strftime(""%Y%m%d%H%M%S"")+"".csv""
        #df = df.with_column(""file_name"", lit(file_name1))
        df = df.with_column(""crtd_dttm"", lit(datetime.now(pytz.timezone(""Asia/Singapore"")).strftime(""%Y-%m-%d %H:%M:%S"")))
        extrctd_month_id=file_name.split(""_"")[2].split(""."")[0]
        df = df.with_column(""mnth_id"", lit(extrctd_month_id))
        df = df.with_column(""FILE_NAME"", lit(file_name.split(""."")[0]+"".xlsx""))
        
        # date=file_name.split(""_"")[3].split(""."")[0]
        # year=date[0:4]
        # mnth_no=date[4:6]
        # inv_week=date[6:]
        # df = df.withColumn(""year"", lit(year))
        # df = df.with_column(""mnth_no"", lit(mnth_no))
        # df = df.with_column(""inv_week"", lit(inv_week))  
        
        snowdf= df.select(
                    ""mnth_id"",
                    ""sls_area"",
                    ""plant"",
                    ""customer_id"",
                    ""old_cust_id"",
                    ""cust_nm"",
                    ""chnl"",
                    ""sls_off"",
                    ""sls_grp"",
                    ""address"",
                    ""city"",
                    ""postal_cd"",
                    ""dsm"",
                    ""sman"",
                    ""prin"",
                    ""principal"",
                    ""matl_grp"",
                    ""matl_sub_grp"",
                    ""brand"",
                    ""uom_conv"",
                    ""matl_num"",
                    ""old_matl_num"",
                    ""matl_desc"",
                    ""qty"",
                    ""sls_amt"",
                    ""FILE_NAME""
                    )
        file_name=file_name.split(""."")[0]+""_""+datetime.now(pytz.timezone(""Asia/Singapore"")).strftime(""%Y%m%d%H%M%S"")
        current_date = datetime.now()
        formatted_year = current_date.strftime(""%Y"")
        formatted_month = current_date.strftime(""%m"")
        
        #move to success
        snowdf.write.copy_into_location(""@""+stage_name+""/""+temp_stage_path+""/""+""processed/success/""+formatted_year+""/""+formatted_month+""/""+file_name,header=True,OVERWRITE=True)
        
        snowdf.write.mode(""append"").saveAsTable(stage_name.split(""."")[0] + ""."" + target_table)
        
        return ""Success""

    
    except KeyError as key_error:
        # Handle KeyError (missing columns) here
        error_message = f""KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame.""
        return error_message

    except pd.errors.MergeError as merge_error:
        # Handle DataFrame merging error
        error_message = f""DataFrame merging error: {str(merge_error)}""
        return error_message

    except Exception as e:
        # Handle exceptions here
        error_message = f""Error: {str(e)}""
        return error_message';


create OR REPLACE PROCEDURE PROD_DNA_LOAD.PHLSDL_RAW.PH_DMS_SELLOUT_SALES_FACT_PREPROCESSING(""PARAM"" ARRAY)
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

def main(session: snowpark.Session,Param):
    # Param=[""OUT_CON_S_PH_20240327224506_20240328014201.csv"",""PHLSDL_RAW.DEV_LOAD_STAGE_ADLS"",""dev/ap_ph_metadata/dms/sales"",""sdl_ph_dms_sellout_sales_fact""]
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        
        df_schema = StructType([
                StructField(""dstrbtr_grp_cd"",StringType(), True),
                StructField(""cntry_cd"",StringType(), True),
                StructField(""dstrbtr_cust_id"",StringType(), True),
                StructField(""order_dt"",StringType(), True),
                StructField(""invoice_dt"",StringType(), True),
                StructField(""order_no"",StringType(), True),
                StructField(""invoice_no"",StringType(), True),
                StructField(""sls_route_id"",StringType(), True),
                StructField(""sls_route_nm"",StringType(), True),
                StructField(""sls_grp"",StringType(), True),
                StructField(""sls_rep_id"",StringType(), True),
                StructField(""sls_rep_nm"",StringType(), True),
                StructField(""dstrbtr_prod_id"",StringType(), True),
                StructField(""uom"",StringType(), True),
                StructField(""gross_price"",StringType(), True),
                StructField(""qty"",StringType(), True),
                StructField(""gts_val"",StringType(), True),
                StructField(""dscnt"",StringType(), True),
                StructField(""nts_val"",StringType(), True),
                StructField(""line_num"",StringType(), True),
                StructField(""prom_id"",StringType(), True),
                StructField(""wh_id"",StringType(), True),
                StructField(""sls_rep_type"",StringType(), True),
                StructField(""order_qty"",StringType(), True),
                StructField(""order_delivery_dt"",StringType(), True),
                StructField(""order_status"",StringType(), True)
            ])
        
        df = session.read\\
            .schema(df_schema)\\
            .option(""skip_header"",1)\\
            .option(""field_delimiter"", "","")\\
            .option(""field_optionally_enclosed_by"", ""\\"""")\\
            .csv(""@""+stage_name+""/""+temp_stage_path+""/""+file_name)

       
        
        df=df.na.drop(""all"")
        if df.count()==0 :
            return ""No Data in file""

        
        df = df.with_column(""cdl_dttm"", lit(file_name.split(""_"")[-1].split(""."")[0]))
        file_name1=file_name.split(""."")[0]+""_""+ datetime.now(pytz.timezone(""Asia/Singapore"")).strftime(""%Y%m%d%H%M%S"")+"".csv""
        # df = df.with_column(""file_name"", lit(file_name1))
        df = df.with_column(""curr_date"", lit(datetime.now(pytz.timezone(""Asia/Singapore"")).strftime(""%Y-%m-%d %H:%M:%S"")))
        df = df.with_column(""sls_route_nm"", lit(None))
        df = df.with_column(""FILE_NAME"", lit(file_name))
        

        snowdf= df.select(
                    ""dstrbtr_grp_cd"",
                    ""cntry_cd"",
                    ""dstrbtr_cust_id"",
                    ""order_dt"",
                    ""invoice_dt"",
                    ""order_no"",
                    ""invoice_no"",
                    ""sls_route_id"",
                    ""sls_route_nm"",
                    ""sls_grp"",
                    ""sls_rep_id"",
                    ""sls_rep_nm"",
                    ""dstrbtr_prod_id"",
                    ""uom"",
                    ""gross_price"",
                    ""qty"",
                    ""gts_val"",
                    ""dscnt"",
                    ""nts_val"",
                    ""line_num"",
                    ""prom_id"",
                    ""wh_id"",
                    ""cdl_dttm"",
                    ""curr_date"",
                    ""sls_rep_type"",
                    ""order_qty"",
                    ""order_delivery_dt"",
                    ""order_status"",
                    ""FILE_NAME""
                    )
        file_name=file_name.split(""."")[0]+""_""+datetime.now(pytz.timezone(""Asia/Singapore"")).strftime(""%Y%m%d%H%M%S"")
        current_date = datetime.now()
        formatted_year = current_date.strftime(""%Y"")
        formatted_month = current_date.strftime(""%m"")
        
        #move to success
        snowdf.write.copy_into_location(""@""+stage_name+""/""+temp_stage_path+""/""+""processed/success/""+formatted_year+""/""+formatted_month+""/""+file_name1,header=True,OVERWRITE=True)
        
        snowdf.write.mode(""append"").saveAsTable(stage_name.split(""."")[0] + ""."" + target_table)
        
        return ""Success""

    
    except KeyError as key_error:
        # Handle KeyError (missing columns) here
        error_message = f""KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame.""
        return error_message

    except pd.errors.MergeError as merge_error:
        # Handle DataFrame merging error
        error_message = f""DataFrame merging error: {str(merge_error)}""
        return error_message

    except Exception as e:
        # Handle exceptions here
        error_message = f""Error: {str(e)}""
        return error_message
        
        
        
        
';
