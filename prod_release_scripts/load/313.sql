CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.PHLSDL_RAW.ACOMMERCE_GMV("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS 'from snowflake.snowpark.functions import col, lit, date_format, current_timestamp, to_date, year, month, concat, format_number, regexp_replace,to_timestamp,when,trim,upper
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DecimalType,Timestamp
import snowflake.snowpark
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz
import pandas as pd

def main(session: snowpark.Session,Param):
    #Param=["aCommerce_GMV_202402_20240319.csv","PHLSDL_RAW.DEV_LOAD_STAGE_ADLS","dev/certified_data_lake/ACOMMERCE_GMV/archive","sdl_ph_ecommerce_offtake_acommerce"]
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        
        df_schema = StructType([
                StructField("partner_id", StringType(), True),
                StructField("prefix", StringType(), True),
                StructField("client", StringType(), True),
                StructField("order_date",StringType(), True),
                StructField("month_no", StringType(), True),
                StructField("month", StringType(), True),
                StructField("day", StringType(), True),
                StructField("marketplace_order_date", StringType(), True),
                StructField("marketplace_month", StringType(), True),
                StructField("marketplace_day", IntegerType(), True),
                StructField("delivered_date", StringType(), True),
                StructField("order_id", StringType(), True),
                StructField("partner_order_id", StringType(), True),
                StructField("delivery_status", StringType(), True),
                StructField("item_sku", StringType(), True),
                StructField("acommerce_item_sku", StringType(), True),
                StructField("sub_sales_channel", StringType(), True),
                StructField("payment_type", StringType(), True),
                StructField("product_title", StringType(), True),
                StructField("brand", StringType(), True),
                StructField("mapping", StringType(), True),
                StructField("ltp", DecimalType(), True),
                StructField("jj_srp", DecimalType(), True),
                StructField("margin", StringType(30), True),
                StructField("type", StringType(), True),
                StructField("quantity", IntegerType(), True),
                StructField("gmv", DecimalType(), True),
                StructField("count_order", StringType(), True),
                StructField("addressee", StringType(), True),
                StructField("shipping_province", StringType(), True),
                StructField("blank_col_1", StringType(), True),
                StructField("AMP_month", StringType(), True),
                StructField("MO_month", StringType(), True),
                StructField("blank_col_2", StringType(), True),
                StructField("blank_col_3", StringType(), True),
                StructField("lost_by_3pl", StringType(), True)
            ])

    
   
    
        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", ",")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

       
        
        df=df.na.drop("all")
        if df.count()==0 :
           
            return "No Data in file"

       
        #df = df.with_column("run_id", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        file_name1=file_name.split(".")[0]+"_"+ datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")+".csv"
        df = df.with_column("filename", lit(file_name1))
        df = df.with_column("crtd_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))

        snowdf= df.select(
                    "partner_id",
                    "prefix",
                    "client",
                    "order_date",
                    "month_no",
                    "month",
                    "day",
                    "marketplace_order_date",
                    "marketplace_month",
                    "marketplace_day",
                    "delivered_date",
                    "order_id",
                    "partner_order_id",
                    "delivery_status",
                    "item_sku",
                    "acommerce_item_sku",
                    "sub_sales_channel",
                    "payment_type",
                    "product_title",
                    "brand",
                    "mapping",
                    "ltp",
                    "jj_srp",
                    "margin",
                    "type",
                    "quantity",
                    "gmv",
                    "count_order",
                    "addressee",
                    "shipping_province",
                    "filename",
                    "crtd_dttm"
                    )
        file_name="_".join(file_name.split(".")[0].split("_")[0:2])+"_"+datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m_%d%H%M%S")
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")

        snowdf.write.mode("append").saveAsTable(stage_name.split(".")[0] + "." + target_table)
        
        #move to success
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
        return error_message';
		
		
		
CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.PHLSDL_RAW.PH_AS_WATSONS_INVENTORY_PREPROCESSING("PARAM" ARRAY)
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

def main(session: snowpark.Session, Param):
    # Param=["PH_Watson_Invty_20240208.csv","PHLSDL_RAW.DEV_LOAD_STAGE_ADLS","dev/pos/watsons","sdl_ph_as_watsons_inventory"]
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        
        df_schema = StructType([
                StructField("item_cd",StringType(), True),
                StructField("item_desc",StringType(), True),
                StructField("total_units",StringType(), True),
                StructField("total_cost",StringType(), True),
                StructField("avg_sales_cost",StringType(), True),
                StructField("wks_sup",StringType(), True),
                StructField("remarks",StringType(), True),
                StructField("br_ol_pl_ex",StringType(), True),
                StructField("group_name",StringType(), True),
                StructField("dept_name",StringType(), True),
                StructField("class_name",StringType(), True),
                StructField("sub_class_name",StringType(), True),
                StructField("br_ol_pl_ex_subclass",StringType(), True),
                StructField("subclass",StringType(), True),
                StructField("catman",StringType(), True),
                StructField("item_status",StringType(), True),
                StructField("item_class",StringType(), True),
                StructField("hold_reason_code",StringType(), True),
                StructField("site_code",StringType(), True),
                StructField("site_name",StringType(), True),
                StructField("gwp",StringType(), True),
                StructField("ret_non_ret",StringType(), True),
                StructField("good_bad_13_wks",StringType(), True)
            ])
        
        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "\\u0001")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

       
        
        df=df.na.drop("all")
        if df.count()==0 :
            return "No Data in file"

        
        df = df.with_column("cdl_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        file_name1=file_name.split(".")[0]+"_"+ datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")+".csv"
        df = df.with_column("file_name", lit(file_name1))
        df = df.with_column("crtd_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        date=file_name.split("_")[3].split(".")[0]
        year=date[0:4]
        mnth_no=date[4:6]
        inv_week=date[6:]
        df = df.withColumn("year", lit(year))
        df = df.with_column("mnth_no", lit(mnth_no))
        df = df.with_column("inv_week", lit(inv_week))  
        
        snowdf= df.select(
                    "year",
                    "mnth_no",
                    "inv_week",
                    "item_cd",
                    "item_desc",
                    "total_units",
                    "total_cost",
                    "avg_sales_cost",
                    "wks_sup",
                    "remarks",
                    "br_ol_pl_ex",
                    "group_name",
                    "dept_name",
                    "class_name",
                    "sub_class_name",
                    "br_ol_pl_ex_subclass",
                    "subclass",
                    "catman",
                    "item_status",
                    "item_class",
                    "hold_reason_code",
                    "site_code",
                    "site_name",
                    "gwp",
                    "ret_non_ret",
                    "good_bad_13_wks",
                    "file_name",
                    "crtd_dttm"
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
        return error_message
        
        
        
        
';



CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.PHLSDL_RAW.PH_BP_TRGT_PREPROCESSING("PARAM" ARRAY)
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
    # Param=["PH_BP_2021_20221114161755.csv","PHLSDL_RAW.DEV_LOAD_STAGE_ADLS","dev/ph_bp/ph_bp_target","SDL_PH_BP_TRGT"]
    try:
        file_name = Param[0]
        stage_name = Param[1]
        temp_stage_path = Param[2]
        target_table = Param[3]
        
        df_schema = StructType([
            StructField("yearmo", StringType(), True),
            StructField("soldto_code", StringType(), True),     
            StructField("target_type", StringType(), True),
            StructField("brand_code", StringType(), True),
            StructField("amount", StringType(), True),      
        ])
        
        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "|")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
        
        df = df.na.drop("all")
        
        if df.count() == 0:
            return "No Data in file"

        df = df.withColumn("crtd_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        df = df.withColumn("filename", lit(file_name))
        df = df.withColumn("run_id", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))

        snowdf = df.select(
            "yearmo",
            "soldto_code",
            "target_type",
            "brand_code",
            "amount",
            "filename",
            "run_id",
            "crtd_dttm"
            )


        file_name=file_name.split(".")[0]+"_"+ datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        
        #move to success
        snowdf.write.mode("append").saveAsTable(stage_name.split(".")[0] + "." + target_table)
        snowdf.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True,file_format_name="my_csv_format")
        
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


CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.PHLSDL_RAW.PH_CLOBOTICS_STORE_RAW_DATA_PREPROCESSING("PARAM" ARRAY)
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
    # Param=["store-raw-data-27032024_20240329014219.csv","PHLSDL_RAW.DEV_LOAD_STAGE_ADLS","dev/clobotics/store_raw_data_ingestion","SDL_PH_CLOBOTICS_STORE_RAW_DATA"]
    try:
        file_name = Param[0]
        stage_name = Param[1]
        temp_stage_path = Param[2]
        target_table = Param[3]
        
        df_schema = StructType([
            StructField("project", StringType(), True),
            StructField("period", StringType(), True),     
            StructField("planid", StringType(), True),
            StructField("planstatus", StringType(), True),
            StructField("planfinishtime", StringType(), True),
            StructField("username", StringType(), True),
            StructField("displayusername", StringType(), True),
            StructField("storecode", StringType(), True),
            StructField("storename", StringType(), True),
            StructField("city", StringType(), True),
            StructField("shopfrontimages", StringType(), True),
            StructField("channel", StringType(), True),
            StructField("planstarttime", StringType(), True),
            StructField("planuploadtime", StringType(), True),
            StructField("skuid", StringType(), True),
             StructField("skuname", StringType(), True),
            StructField("category", StringType(), True),
            StructField("subcategory", StringType(), True),
            StructField("brand", StringType(), True),
            StructField("manufacturer", StringType(), True),
            StructField("kpi", StringType(), True),
            StructField("value", StringType(), True),    
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
            
        df = df.withColumn("crt_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        df = df.withColumn("file_name", lit(file_name))
        df = df.withColumn("run_id", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))

        snowdf = df.select(
            "project",
            "period",
            "planid",
            "planstatus",
            "planfinishtime",
            "username",
            "displayusername",
            "storecode",
            "storename",
            "city",
            "shopfrontimages",
            "channel",
            "planstarttime",
            "planuploadtime",
            "skuid",
            "skuname",
            "category",
            "subcategory",
            "brand",
            "manufacturer",
            "kpi",
            "value",
            "file_name",
            "run_id",
            "crt_dttm",
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


CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.PHLSDL_RAW.PH_CLOBOTICS_SURVEY_DATA_PREPROCESSING("PARAM" ARRAY)
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
    # Param=["survey-data-27032024_20240329014219.csv","PHLSDL_RAW.DEV_LOAD_STAGE_ADLS","dev/clobotics/survery_data_ingestion","SDL_PH_CLOBOTICS_SURVEY_DATA"]
    try:
        file_name = Param[0]
        stage_name = Param[1]
        temp_stage_path = Param[2]
        target_table = Param[3]
        
        df_schema = StructType([
            StructField("project", StringType(), True),
            StructField("period", StringType(), True),     
            StructField("taskid", StringType(), True),
            StructField("planid", StringType(), True),
            StructField("planstatus", StringType(), True),
            StructField("planfinishtime", StringType(), True),
            StructField("username", StringType(), True),
            StructField("userdisplayname", StringType(), True),
            StructField("storecode", StringType(), True),
            StructField("storename", StringType(), True),
            StructField("city", StringType(), True),
            StructField("channel", StringType(), True),
            StructField("taskname", StringType(), True),
            StructField("tasktype", StringType(), True),
            StructField("taskstatus", StringType(), True),
            StructField("createtime", StringType(), True),
            StructField("taskactiontime", StringType(), True),
            StructField("qncode", StringType(), True),
            StructField("qnname", StringType(), True),
            StructField("questioncode", StringType(), True),
             StructField("questioncontent", StringType(), True),
            StructField("questionanswercode", StringType(), True),
            StructField("questionanswername", StringType(), True),
            StructField("questionanswervalue", StringType(), True),
            StructField("questionanswerphotos", StringType(), True),
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
            
        df = df.withColumn("crt_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        df = df.withColumn("file_name", lit(file_name))
        df = df.withColumn("run_id", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))

        snowdf = df.select(
            "project",
            "period",
            "taskid",
            "planid",
            "planstatus",
            "planfinishtime",
            "username",
            "userdisplayname",
            "storecode",
            "storename",
            "city",
            "channel",
            "taskname",
            "tasktype",
            "taskstatus",
            "createtime",
            "taskactiontime",
            "qncode",
            "qnname",
            "questioncode",
            "questioncontent",
            "questionanswercode",
            "questionanswername",
            "questionanswervalue",
            "questionanswerphotos",
            "file_name",
            "run_id",
            "crt_dttm",
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


CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.PHLSDL_RAW.PH_CLOBOTICS_TASK_RAW_DATA_PREPROCESSING("PARAM" ARRAY)
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
    #Param=["task-raw-data-27032024_20240329014219.csv","PHLSDL_RAW.DEV_LOAD_STAGE_ADLS","dev/clobotics/task_raw_data_ingestion","SDL_RAW_PH_CLOBOTICS_TASK_RAW_DATA"]
    try:
        file_name = Param[0]
        stage_name = Param[1]
        temp_stage_path = Param[2]
        target_table = Param[3]
        
        df_schema = StructType([
            StructField("project", StringType(), True),
            StructField("period", StringType(), True),     
            StructField("taskid", StringType(), True),
            StructField("planid", StringType(), True),
            StructField("planstatus", StringType(), True),
            StructField("planfinishtime", StringType(), True),
            StructField("username", StringType(), True),
            StructField("displayusername", StringType(), True),
            StructField("storecode", StringType(), True),
            StructField("storename", StringType(), True),
            StructField("city", StringType(), True),
            StructField("shopfrontimages", StringType(), True),
            StructField("channel", StringType(), True),
            StructField("taskname", StringType(), True),
            StructField("taskstatus", StringType(), True),
            StructField("createtime", StringType(), True),
            StructField("taskactiontime", StringType(), True),
            StructField("stitchedimageurl", StringType(), True),
            StructField("imagequality", StringType(), True),
            StructField("skuid", StringType(), True),
             StructField("skuname", StringType(), True),
            StructField("category", StringType(), True),
            StructField("subcategory", StringType(), True),
            StructField("brand", StringType(), True),
            StructField("manufacturer", StringType(), True),
            StructField("kpi", StringType(), True),
            StructField("value", StringType(), True),    
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
            
        df = df.withColumn("crt_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        df = df.withColumn("file_name", lit(file_name))
        df = df.withColumn("run_id", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))

        snowdf = df.select(
            "project",
            "period",
            "taskid",
            "planid",
            "planstatus",
            "planfinishtime",
            "username",
            "displayusername",
            "storecode",
            "storename",
            "city",
            "shopfrontimages",
            "channel",
            "taskname",
            "taskstatus",
            "createtime",
            "taskactiontime",
            "stitchedimageurl",
            "imagequality",
            "skuid",
            "skuname",
            "category",
            "subcategory",
            "brand",
            "manufacturer",
            "kpi",
            "value",
            "crt_dttm",
            "file_name",
            "run_id",
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


CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.PHLSDL_RAW.PH_CPG_CALLS_PREPROCESSING("PARAM" ARRAY)
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

def main(session: snowpark.Session, Param):
    # Param=["OUT_CON_CALLS_PH_20240331220130_20240401014629.csv","PHLSDL_RAW.DEV_LOAD_STAGE_ADLS","dev/ap_ph_metadata/dms/calls","sdl_ph_cpg_calls"]
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        
        df_schema = StructType([
                StructField("dstrbtr_grp_cd",StringType(), True),
                StructField("cntry_cd",StringType(), True),
                StructField("planned_visit",StringType(), True),
                StructField("actual_visit",StringType(), True),
                StructField("sls_rep_id",StringType(), True),
                StructField("cust_id",StringType(), True),
                StructField("order_no",StringType(), True),
                StructField("reason_no_order",StringType(), True),
                StructField("in_cpg_flag",StringType(), True),
                StructField("approved_flag",StringType(), True)
            ])
        
        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", ",")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

       
        
        df=df.na.drop("all")
        if df.count()==0 :
            return "No Data in file"

        
        df = df.with_column("cdl_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        file_name1=file_name.split(".")[0]+"_"+ datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")+".csv"
        df = df.with_column("file_name", lit(file_name1))
        df = df.with_column("crtd_date", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        df = df.with_column("order_no", lit(None))
        
        snowdf= df.select(
                    "dstrbtr_grp_cd",
                    "cntry_cd",
                    "planned_visit",
                    "actual_visit",
                    "sls_rep_id",
                    "cust_id",
                    "order_no",
                    "reason_no_order",
                    "in_cpg_flag",
                    "approved_flag",
                    "file_name",
                    "cdl_dttm",
                    "crtd_date"
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
        return error_message
        
        
        
        
';



CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.PHLSDL_RAW.PH_CRM_CHILDERN_PREPROCESSING("PARAM" ARRAY)
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
            
        #Param=[''PH_CRM_Children_20240402.csv'',''PHLSDL_RAW.DEV_LOAD_STAGE_ADLS'',''dev/SFMC/PH_CRM_Children'',''SDL_PH_SFMC_CHILDREN_DATA'']

        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        # Define the schema for the DataFrame
        
        df_schema=StructType([
                        StructField("child_nm", StringType()),
                        StructField("child_birth_mnth", StringType()),
                        StructField("child_birth_year", StringType()),
                        StructField("child_gender", StringType()),
                        StructField("parent_key", StringType()),
                        StructField("child_number", StringType()),
                        
                        ])
                
        dataframe = session.read.schema(df_schema) \\
        .option("skip_header",1) \\
        .option("field_delimiter", "|") \\
        .option("encoding","UTF-16")\\
        .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
        

        
        dataframe=dataframe.na.drop("all")
        if dataframe.count()==0:
            return "No Data in file"

        

        new_file_name=file_name[0:24] +".csv"
        dataframe = dataframe.with_column("FILE_NAME",lit(new_file_name))
        dataframe = dataframe.with_column("CRTD_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))

        # Load Data to the target table
        dataframe.write.mode("append").saveAsTable(target_table)
       
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        #move to success
        file_name=file_name[0:24]+''_''+datetime.now().strftime("%Y%m%d%H%M%S")
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
        return error_message
        ';
		
		
CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.PHLSDL_RAW.PH_CRM_CONSUMER_MASTER_PREPROCESSING("PARAM" ARRAY)
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
            
        #Param=[''PH_CRM_Consumer_Master_20240327_20240327173049.csv'',''PHLSDL_RAW.DEV_LOAD_STAGE_ADLS'',''dev/SFMC/PH_CRM_Consumer_Master_Primary'',''SDL_PH_SFMC_consumer_master'']

        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        # Define the schema for the DataFrame
        
        df_schema=StructType([
                        StructField("first_name", StringType()),
                        StructField("last_name", StringType()),
                        StructField("mobile_num", StringType()),
                        StructField("mobile_cntry_cd", StringType()),
                        StructField("birthday_mnth", StringType()),
                        StructField("birthday_year", StringType()),
                        StructField("address_1", StringType()),
                        StructField("address_2", StringType()),
                        StructField("address_city", StringType()),
                        StructField("address_zipcode", StringType()),
                        StructField("subscriber_key", StringType()),
                        StructField("website_unique_id", StringType()),
                        StructField("source", StringType()),
                        StructField("medium", StringType()),
                        StructField("brand", StringType()),
                        StructField("address_cntry", StringType()),
                        StructField("campaign_id", StringType()),
                        StructField("created_date", StringType()),
                        StructField("updated_date", StringType()),
                        StructField("unsubscribe_date", StringType()),
                        StructField("email", StringType()),
                        StructField("full_name", StringType()),
                        StructField("last_logon_time", StringType()),
                        StructField("remaining_points", StringType()),
                        StructField("redeemed_points", StringType()),
                        StructField("total_points", StringType()),
                        StructField("gender", StringType()),
                        StructField("line_id", StringType()),
                        StructField("line_name", StringType()),
                        StructField("line_email", StringType()),
                        StructField("line_channel_id", StringType()),
                        StructField("address_region", StringType()),
                        StructField("tier", StringType()),
                        StructField("Opt_In_For_Communication", StringType()),
                        StructField("Subscriber_Status", StringType()),
                        StructField("Viber_Engaged", StringType()),
                        StructField("Opt_In_For_JNJ_Communication", StringType()),
                        StructField("Opt_In_For_Campaign", StringType())
                        ])
                
        dataframe = session.read.schema(df_schema) \\
        .option("skip_header",1) \\
        .option("field_delimiter", "|") \\
        .option("encoding","UTF-8")\\
        .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
        

        
        dataframe=dataframe.na.drop("all")
        if dataframe.count()==0:
            return "No Data in file"

        #logic as per sql script 
        dataframe= dataframe.filter((dataframe["first_name"].isNotNull()) & (dataframe["last_name"].isNotNull()) & (dataframe["mobile_num"].isNotNull()) & (dataframe["subscriber_key"].isNotNull()) & (dataframe["brand"].isNotNull())  )

        new_file_name=file_name[0:31] +".csv"
        dataframe = dataframe.with_column("FILE_NAME",lit(new_file_name))
        dataframe = dataframe.with_column("CRTD_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))

        # Load Data to the target table
        dataframe.write.mode("append").saveAsTable(target_table)
       
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        #move to success
        file_name=file_name[0:31]+''_''+datetime.now().strftime("%Y%m%d%H%M%S")
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
        return error_message
        ';
		
		
CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.PHLSDL_RAW.PH_DMS_SELLOUT_SALES_FACT_PREPROCESSING("PARAM" ARRAY)
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
    # Param=["OUT_CON_S_PH_20240327224506_20240328014201.csv","PHLSDL_RAW.DEV_LOAD_STAGE_ADLS","dev/ap_ph_metadata/dms/sales","sdl_ph_dms_sellout_sales_fact"]
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        
        df_schema = StructType([
                StructField("dstrbtr_grp_cd",StringType(), True),
                StructField("cntry_cd",StringType(), True),
                StructField("dstrbtr_cust_id",StringType(), True),
                StructField("order_dt",StringType(), True),
                StructField("invoice_dt",StringType(), True),
                StructField("order_no",StringType(), True),
                StructField("invoice_no",StringType(), True),
                StructField("sls_route_id",StringType(), True),
                StructField("sls_route_nm",StringType(), True),
                StructField("sls_grp",StringType(), True),
                StructField("sls_rep_id",StringType(), True),
                StructField("sls_rep_nm",StringType(), True),
                StructField("dstrbtr_prod_id",StringType(), True),
                StructField("uom",StringType(), True),
                StructField("gross_price",StringType(), True),
                StructField("qty",StringType(), True),
                StructField("gts_val",StringType(), True),
                StructField("dscnt",StringType(), True),
                StructField("nts_val",StringType(), True),
                StructField("line_num",StringType(), True),
                StructField("prom_id",StringType(), True),
                StructField("wh_id",StringType(), True),
                StructField("sls_rep_type",StringType(), True),
                StructField("order_qty",StringType(), True),
                StructField("order_delivery_dt",StringType(), True),
                StructField("order_status",StringType(), True)
            ])
        
        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", ",")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

       
        
        df=df.na.drop("all")
        if df.count()==0 :
            return "No Data in file"

        
        df = df.with_column("cdl_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        file_name1=file_name.split(".")[0]+"_"+ datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")+".csv"
        # df = df.with_column("file_name", lit(file_name1))
        df = df.with_column("curr_date", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        df = df.with_column("sls_route_nm", lit(None))
        

        snowdf= df.select(
                    "dstrbtr_grp_cd",
                    "cntry_cd",
                    "dstrbtr_cust_id",
                    "order_dt",
                    "invoice_dt",
                    "order_no",
                    "invoice_no",
                    "sls_route_id",
                    "sls_route_nm",
                    "sls_grp",
                    "sls_rep_id",
                    "sls_rep_nm",
                    "dstrbtr_prod_id",
                    "uom",
                    "gross_price",
                    "qty",
                    "gts_val",
                    "dscnt",
                    "nts_val",
                    "line_num",
                    "prom_id",
                    "wh_id",
                    "cdl_dttm",
                    "curr_date",
                    "sls_rep_type",
                    "order_qty",
                    "order_delivery_dt",
                    "order_status"
                    )
        file_name=file_name.split(".")[0]+"_"+datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        
        #move to success
        snowdf.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name1,header=True,OVERWRITE=True)
        
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
        return error_message
        
        
        
        
';




CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.PHLSDL_RAW.PH_DMS_SELLOUT_STOCK_FACT_PREPROCESSING("PARAM" ARRAY)
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
    # Param=["OUT_CON_I_PH_20240331224500_20240401014431.csv","PHLSDL_RAW.DEV_LOAD_STAGE_ADLS","dev/ap_ph_metadata/dms/inventory","sdl_ph_dms_sellout_stock_fact"]
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        
        df_schema = StructType([
                StructField("dstrbtr_grp_cd",StringType(), True),
                StructField("cntry_cd",StringType(), True),
                StructField("wh_cd",StringType(), True),
                StructField("invoice_dt",StringType(), True),
                StructField("dstrbtr_prod_id",StringType(), True),
                StructField("qty",StringType(), True),
                StructField("uom",StringType(), True),
                StructField("amt",StringType(), True)
            ])
        
        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", ",")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

       
        
        df=df.na.drop("all")
        if df.count()==0 :
            return "No Data in file"

        
        df = df.with_column("cdl_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        file_name1=file_name.split(".")[0]+"_"+ datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")+".csv"
        # df = df.with_column("file_name", lit(file_name1))
        df = df.with_column("curr_date", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))

        snowdf= df.select(
                    "dstrbtr_grp_cd",
                    "cntry_cd",
                    "wh_cd",
                    "invoice_dt",
                    "dstrbtr_prod_id",
                    "qty",
                    "uom",
                    "amt",
                    "cdl_dttm",
                    "curr_date"
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
        return error_message
        
        
        
        
';




CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.PHLSDL_RAW.PH_ECOMMERCE_OFFTAKE_ACOMMERCE_PREPROCESSING("PARAM" ARRAY)
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
    # Param=["aCommerce_GMV_202402_20240319.csv","PHLSDL_RAW.DEV_LOAD_STAGE_ADLS","dev/acommerce_gmv","SDL_PH_ECOMMERCE_OFFTAKE_ACOMMERCE"]
    try:
        file_name = Param[0]
        stage_name = Param[1]
        temp_stage_path = Param[2]
        target_table = Param[3]
        
        df_schema = StructType([
            StructField("partner_id", StringType(), True),
            StructField("prefix", StringType(), True),     
            StructField("client", StringType(), True),
            StructField("order_date", StringType(), True),
            StructField("month_no", StringType(), True),
            StructField("month", StringType(), True),
            StructField("day", StringType(), True),
            StructField("marketplace_order_date", StringType(), True),
            StructField("marketplace_month", StringType(), True),
            StructField("marketplace_day", StringType(), True),
            StructField("delivered_date", StringType(), True),
            StructField("order_id", StringType(), True),
            StructField("partner_order_id", StringType(), True),
            StructField("delivery_status", StringType(), True),
            StructField("item_sku", StringType(), True),
            StructField("acommerce_item_sku", StringType(), True),
            StructField("sub_sales_channel", StringType(), True),
            StructField("payment_type", StringType(), True),
            StructField("product_title", StringType(), True),
            StructField("brand", StringType(), True),
            StructField("mapping", StringType(), True),
            StructField("ltp", StringType(), True),
            StructField("jj_srp", StringType(), True),
            StructField("margin", StringType(), True),
            StructField("type", StringType(), True),
            StructField("quantity", StringType(), True),
            StructField("gmv", StringType(), True),
            StructField("count_order", StringType(), True),
            StructField("addressee", StringType(), True),
            StructField("shipping_province", StringType(), True),
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
            
        df = df.withColumn("crt_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        df = df.withColumn("file_name", lit(file_name))
        # df = df.withColumn("run_id", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        # Truncate the margin column to 10 characters
        df = df.withColumn("margin", df["margin"].substr(1, 10))

        snowdf = df.select(
                "partner_id",
                "prefix",
                "client",
                "order_date",
                "month_no",
                "month",
                "day",
                "marketplace_order_date",
                "marketplace_month",
                "marketplace_day",
                "delivered_date",
                "order_id",
                "partner_order_id",
                "delivery_status",
                "item_sku",
                "acommerce_item_sku",
                "sub_sales_channel",
                "payment_type",
                "product_title",
                "brand",
                "mapping",
                "ltp",
                "jj_srp",
                "margin",
                "type",
                "quantity",
                "gmv",
                "count_order",
                "addressee",
                "shipping_province",
                "file_name",
                "crt_dttm",
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



CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.PHLSDL_RAW.PH_IOP_TRGT_PREPROCESSING("PARAM" ARRAY)
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
    # Param=["PH_IOP_2021_20220425230911.csv","PHLSDL_RAW.DEV_LOAD_STAGE_ADLS","dev/ph_iop/iop/ph_iop_target","SDL_PH_IOP_TRGT"]
    try:
        file_name = Param[0]
        stage_name = Param[1]
        temp_stage_path = Param[2]
        target_table = Param[3]
        
        df_schema = StructType([
            StructField("measure", StringType(), True),
            StructField("target_type", StringType(), True),     
            StructField("year", StringType(), True),
            StructField("brand", StringType(), True),
            StructField("segment", StringType(), True),
            StructField("customer_code", StringType(), True),
            StructField("account", StringType(), True),
            StructField("jan", StringType(), True),
            StructField("feb", StringType(), True),
            StructField("mar", StringType(), True),
            StructField("apr", StringType(), True),
            StructField("may", StringType(), True),
            StructField("jun", StringType(), True),
            StructField("jul", StringType(), True),
            StructField("aug", StringType(), True),
             StructField("sep", StringType(), True),
            StructField("oct", StringType(), True),
            StructField("nov", StringType(), True),
            StructField("dec", StringType(), True),    
        ])
        
        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "|")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
        
        df = df.na.drop("all")
        
        if df.count() == 0:
            return "No Data in file"
            
        df = df.withColumn("crt_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        df = df.withColumn("file_name", lit(file_name))
        df = df.withColumn("run_id", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))

        snowdf = df.select(
            "measure",
            "target_type",
            "year",
            "brand",
            "segment",
            "customer_code",
            "account",
            "jan",
            "feb",
            "mar",
            "apr",
            "may",
            "jun",
            "jul",
            "aug",
            "sep",
            "oct",
            "nov",
            "dec",
            "run_id",
            "crt_dttm",
            "file_name",
        )

        
        file_name=file_name.split(".")[0]+"_"+ datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        
        #move to success
        snowdf.write.mode("append").saveAsTable(stage_name.split(".")[0] + "." + target_table)
        snowdf.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True,file_format_name="my_csv_format")
        
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

            df1 = df.set_index(''MODE'')
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

            
                
                    

            #----------------------------------------------------------------------------#
            #Adding columns that are needed for final table 
            
            snowparkdf = snowparkdf.withColumn("ret_nm_prefix", lit(''S8''))

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
   
   
CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.PHLSDL_RAW.PH_PERFECTSTORE_BRANCHMASTER_PREPROCESSING("PARAM" ARRAY)
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
from snowflake.snowpark.functions import col,lit,trim, substring,current_timestamp,to_timestamp,date_format
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField
import pandas as pd
from datetime import datetime
import pytz

def main(session: snowpark.Session,Param): 

    try:

        #Param=[''ISE_tbl_Branch_PH_20240422020000.csv'',''PHLSDL_RAW.DEV_LOAD_STAGE_ADLS'',''dev/PH_Perfect_Store/master/isebranchmaster/'',''sdl_ph_tbl_ISEBranchMaster'']

        # Extracting parameters from the input
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]


        # Define the schema for the DataFrame
        df_schema=StructType([
            StructField("PARENTCODE",StringType()),
            StructField("PARENTNAME",StringType()),
            StructField("BRANCHCODE",StringType()),
            StructField("BRANCHNAME",StringType()),
            StructField("CHANNELCODE",StringType()),
            StructField("CHANNEL",StringType()),
            StructField("AREA",StringType()),
            StructField("REGION",StringType()),
            StructField("STATE",StringType()),
            StructField("CITY",StringType()),
            StructField("DISTRICT",StringType()),
            StructField("TRADETYPE",StringType()),
            StructField("STOREPRIORITIZATION",StringType()),
            StructField("LATITUDE",StringType()),
            StructField("LONGITUDE",StringType())
            ])


        # Read Date and Header for column names
    
        dataframe = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", ",")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .option("skip_blank_lines", True)\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        #---------------------------Transformation logic ------------------------------#

        # Handle null values or empty rows
        dataframe= dataframe.na.drop("all")


        # Check for empty Dataframe
        if dataframe.count()==0:
            return "No Data in file"

        dataframe=dataframe.withColumn("FILENAME",lit(file_name).cast("String"))
        dataframe = dataframe.with_column("RUNID",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")).cast("integer"))
        dataframe = dataframe.with_column("CRT_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))

        final_df=dataframe.select("BRANCHCODE","BRANCHNAME","AREA","CHANNEL","REGION","STATE","CITY","DISTRICT","PARENTCODE","PARENTNAME","TRADETYPE",\\
                                  "STOREPRIORITIZATION","CHANNELCODE","LATITUDE","LONGITUDE","FILENAME","RUNID","CRT_DTTM")

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
		
		
CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.PHLSDL_RAW.PH_PERFECTSTORE_SURVEYANSWERS_PREPROCESSING("PARAM" ARRAY)
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
from snowflake.snowpark.functions import col,lit,trim, substring,current_timestamp,to_timestamp,date_format
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField
import pandas as pd
from datetime import datetime
import pytz

def main(session: snowpark.Session,Param): 

    try:

        #Param=[''ISE_tbl_SurveyAnswers_PH_20240424020004.csv'',''PHLSDL_RAW.DEV_LOAD_STAGE_ADLS'',''dev/PH_Perfect_Store/transaction/surveyanswers/'',''SDL_PH_TBL_SURVEYANSWERS'']

        # Extracting parameters from the input
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]


        # Define the schema for the DataFrame
        df_schema=StructType([
            StructField("CUSTCODE",StringType()),
            StructField("SLSPERID",StringType()),
            StructField("BRANCHCODE",StringType()),
            StructField("ISEID",StringType()),
            StructField("QUESNO",StringType()),
            StructField("ANSWERSEQ",StringType()),
            StructField("ANSWERVALUE",StringType()),
            StructField("ANSWERSCORE",StringType()),
            StructField("OOS",StringType()),
            StructField("CREATEDDATE",StringType())
            ])

        # Read Date and Header for column names
    
        dataframe = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", ",")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .option("skip_blank_lines", True)\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        #---------------------------Transformation logic ------------------------------#

        # Handle null values or empty rows
        dataframe= dataframe.na.drop("all")


        # Check for empty Dataframe
        if dataframe.count()==0:
            return "No Data in file"
        

        file_dt=file_name.split("_")[4].split(".")[0]

        df_pandas=dataframe.to_pandas()
        df_pandas[''CREATEDDATE''] = pd.to_datetime(df_pandas[''CREATEDDATE''])
        df_pandas[''CREATEDDATE''] = df_pandas[''CREATEDDATE''].dt.strftime(''%Y-%m-%d %H:%M:%S.%f'')

        new_df=session.create_dataframe(df_pandas)

        new_df=new_df.withColumn("FILENAME",lit(file_name).cast("String"))
        new_df=new_df.withColumn("FILENAME_DT",lit(file_dt).cast("String"))
        new_df = new_df.with_column("RUNID",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")).cast("integer"))
        new_df = new_df.with_column("CRT_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))

        # Creating Final Dataframe
        final_df = new_df.alias("final_df")

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
		

CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.PHLSDL_RAW.PH_PERFECTSTORE_SURVEYCHOICES_PREPROCESSING("PARAM" ARRAY)
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
from snowflake.snowpark.functions import col,lit,trim, substring,current_timestamp,to_timestamp,date_format
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField
import pandas as pd
from datetime import datetime
import pytz

def main(session: snowpark.Session,Param): 

    try:

        #Param=[''ISE_tbl_SurveyChoices_PH_20240422020715.csv'',''PHLSDL_RAW.DEV_LOAD_STAGE_ADLS'',''dev/PH_Perfect_Store/master/surveychoices/'',''SDL_PH_TBL_SURVEYCHOICES'']

        # Extracting parameters from the input
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]


        # Define the schema for the DataFrame
        df_schema=StructType([
            StructField("ISEID",StringType()),
            StructField("QUESNO",StringType()),
            StructField("ANSWERSEQ",StringType()),
            StructField("SKUGROUP",StringType()),
            StructField("REPPARAM",StringType()),
            StructField("PUTUPID",StringType()),
            StructField("PUTUPDESC",StringType()),
            StructField("SCORE",StringType()),
            StructField("SFA",StringType()),
            StructField("BRANDID",StringType()),
            StructField("BRAND",StringType())
            ])


        # Read Date and Header for column names
    
        dataframe = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", ",")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .option("skip_blank_lines", True)\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        #---------------------------Transformation logic ------------------------------#

        # Handle null values or empty rows
        dataframe= dataframe.na.drop("all")


        # Check for empty Dataframe
        if dataframe.count()==0:
            return "No Data in file"

        file_dt=file_name.split("_")[4].split(".")[0]

        dataframe=dataframe.withColumn("FILENAME",lit(file_name).cast("String"))
        dataframe=dataframe.withColumn("FILENAME_DT",lit(file_dt).cast("String"))
        dataframe = dataframe.with_column("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")).cast("integer"))
        dataframe = dataframe.with_column("CRT_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))

        # Creating Final Dataframe
        final_df = dataframe.alias("final_df")

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
		


CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.PHLSDL_RAW.PH_PERFECTSTORE_SURVEYCPG_PREPROCESSING("PARAM" ARRAY)
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
from snowflake.snowpark.functions import col,lit,trim, substring,current_timestamp,to_timestamp,date_format
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField
import pandas as pd
from datetime import datetime
import pytz

def main(session: snowpark.Session,Param): 

    try:

        #Param=[''ISE_tbl_SurveyCPG_PH_20240421020750.csv'',''PHLSDL_RAW.DEV_LOAD_STAGE_ADLS'',''dev/PH_Perfect_Store/master/surveycpg/'',''SDL_PH_TBL_SURVEYCPG'']

        # Extracting parameters from the input
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]


        # Define the schema for the DataFrame
        df_schema=StructType([
            StructField("CUSTCODE",StringType()),
            StructField("SLSPERID",StringType()),
            StructField("BRANCHCODE",StringType()),
            StructField("CREATEDDATE",StringType()),
            StructField("VISITDATE",StringType())
            ])


        # Read Date and Header for column names
    
        dataframe = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", ",")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .option("skip_blank_lines", True)\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        #---------------------------Transformation logic ------------------------------#

        # Handle null values or empty rows
        dataframe= dataframe.na.drop("all")


        # Check for empty Dataframe
        if dataframe.count()==0:
            return "No Data in file"
        

        file_dt=file_name.split("_")[4].split(".")[0]

        df_pandas=dataframe.to_pandas()
        df_pandas[''CREATEDDATE''] = pd.to_datetime(df_pandas[''CREATEDDATE''])
        df_pandas[''VISITDATE''] = pd.to_datetime(df_pandas[''VISITDATE''])
        df_pandas[''CREATEDDATE''] = df_pandas[''CREATEDDATE''].dt.strftime(''%Y-%m-%d %H:%M:%S.%f'')
        df_pandas[''VISITDATE''] = df_pandas[''VISITDATE''].dt.strftime(''%Y-%m-%d %H:%M:%S.%f'')

        new_df=session.create_dataframe(df_pandas)

        new_df=new_df.withColumn("FILENAME",lit(file_name).cast("String"))
        new_df=new_df.withColumn("FILENAME_DT",lit(file_dt).cast("String"))
        new_df = new_df.with_column("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")).cast("integer"))
        new_df = new_df.with_column("CRT_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))

        # Creating Final Dataframe
        final_df = new_df.alias("final_df")

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
		
		

CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.PHLSDL_RAW.PH_PERFECTSTORE_SURVEYCUSTOMERS_PREPROCESSING("PARAM" ARRAY)
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
from snowflake.snowpark.functions import col,lit,trim, substring,current_timestamp,to_timestamp,date_format
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField
import pandas as pd
from datetime import datetime
import pytz

def main(session: snowpark.Session,Param): 

    try:

        #Param=[''ISE_tbl_SurveyCustomers_PH_20240424020753.csv'',''PHLSDL_RAW.DEV_LOAD_STAGE_ADLS'',''dev/PH_Perfect_Store/transaction/surveycustomers/'',''SDL_PH_TBL_SURVEYCUSTOMERS'']

        # Extracting parameters from the input
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]


        # Define the schema for the DataFrame
        df_schema=StructType([
            StructField("CUSTCODE",StringType()),
            StructField("SLSPERID",StringType()),
            StructField("BRANCHCODE",StringType()),
            StructField("ISEID",StringType()),
            StructField("VISITDATE",StringType()),
            StructField("CREATEDDATE",StringType()),
            StructField("STOREPRIORITIZATION",StringType())
            ])

        # Read Date and Header for column names
    
        dataframe = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", ",")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .option("skip_blank_lines", True)\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        #---------------------------Transformation logic ------------------------------#

        # Handle null values or empty rows
        dataframe= dataframe.na.drop("all")


        # Check for empty Dataframe
        if dataframe.count()==0:
            return "No Data in file"
        

        file_dt=file_name.split("_")[4].split(".")[0]

        df_pandas=dataframe.to_pandas()
        df_pandas[''VISITDATE''] = pd.to_datetime(df_pandas[''VISITDATE''])
        df_pandas[''CREATEDDATE''] = pd.to_datetime(df_pandas[''CREATEDDATE''])
        df_pandas[''VISITDATE''] = df_pandas[''VISITDATE''].dt.strftime(''%Y-%m-%d %H:%M:%S.%f'')
        df_pandas[''CREATEDDATE''] = df_pandas[''CREATEDDATE''].dt.strftime(''%Y-%m-%d %H:%M:%S.%f'')

        new_df=session.create_dataframe(df_pandas)

        new_df=new_df.withColumn("FILENAME",lit(file_name).cast("String"))
        new_df=new_df.withColumn("FILENAME_DT",lit(file_dt).cast("String"))
        new_df = new_df.with_column("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")).cast("integer"))
        new_df = new_df.with_column("CRT_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))

        # Creating Final Dataframe
        final_df = new_df.alias("final_df")

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
		
		

CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.PHLSDL_RAW.PH_PERFECTSTORE_SURVEYISEHDR_PREPROCESSING("PARAM" ARRAY)
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
from snowflake.snowpark.functions import col,lit,trim, substring,current_timestamp,to_timestamp,date_format
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField
import pandas as pd
from datetime import datetime
import pytz

def main(session: snowpark.Session,Param): 

    try:

        #Param=[''ISE_tbl_SurveyISEHdr_PH_20240330020713.csv'',''PHLSDL_RAW.DEV_LOAD_STAGE_ADLS'',''dev/PH_Perfect_Store/master/surveyisehdr/'',''SDL_PH_TBL_SURVEYISEHDR'']

        # Extracting parameters from the input
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]


        # Define the schema for the DataFrame
        df_schema=StructType([
            StructField("ISEID",StringType()),
            StructField("ISEDESC",StringType()),
            StructField("CHANNELCODE",StringType()),
            StructField("CHANNELDESC",StringType()),
            StructField("STARTDATE",StringType()),
            StructField("ENDDATE",StringType())
            ])


        # Read Date and Header for column names
    
        dataframe = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", ",")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .option("skip_blank_lines", True)\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        #---------------------------Transformation logic ------------------------------#

        # Handle null values or empty rows
        dataframe= dataframe.na.drop("all")


        # Check for empty Dataframe
        if dataframe.count()==0:
            return "No Data in file"
        

        file_dt=file_name.split("_")[4].split(".")[0]

        df_pandas=dataframe.to_pandas()
        df_pandas[''STARTDATE''] = pd.to_datetime(df_pandas[''STARTDATE''])
        df_pandas[''ENDDATE''] = pd.to_datetime(df_pandas[''ENDDATE''])
        df_pandas[''STARTDATE''] = df_pandas[''STARTDATE''].dt.strftime(''%Y-%m-%d %H:%M:%S.%f'')
        df_pandas[''ENDDATE''] = df_pandas[''ENDDATE''].dt.strftime(''%Y-%m-%d %H:%M:%S.%f'')

        new_df=session.create_dataframe(df_pandas)

        new_df=new_df.withColumn("FILENAME",lit(file_name).cast("String"))
        new_df=new_df.withColumn("FILENAME_DT",lit(file_dt).cast("String"))
        new_df = new_df.with_column("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")).cast("integer"))
        new_df = new_df.with_column("CRT_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))

        # Creating Final Dataframe
        final_df = new_df.alias("final_df")

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
		
		

CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.PHLSDL_RAW.PH_PERFECTSTORE_SURVEYISEQUESTION_PREPROCESSING("PARAM" ARRAY)
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
from snowflake.snowpark.functions import col,lit,trim, substring,current_timestamp,to_timestamp,date_format
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField
import pandas as pd
from datetime import datetime
import pytz

def main(session: snowpark.Session,Param): 

    try:

        #Param=[''ISE_tbl_SurveyISEQuestion_PH_20240330020713.csv'',''PHLSDL_RAW.DEV_LOAD_STAGE_ADLS'',''dev/PH_Perfect_Store/master/surveyisequestion/'',''SDL_PH_TBL_SURVEYISEQUESTION'']

        # Extracting parameters from the input
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]


        # Define the schema for the DataFrame
        df_schema=StructType([
            StructField("ISEID",StringType()),
            StructField("QUESNO",StringType()),
            StructField("QUESCODE",StringType()),
            StructField("QUESDESC",StringType()),
            StructField("STANDARDQUES_FLAG",StringType()),
            StructField("QUESCLASSCODE",StringType()),
            StructField("QUESCLASSDESC",StringType()),
            StructField("WEIGH",StringType()),
            StructField("TOTALSCORE",StringType()),
            StructField("ANSWERCODE",StringType()),
            StructField("ANSWERDESC",StringType()),
            StructField("FRANCHISECODE",StringType()),
            StructField("FRANCHISEDESC",StringType()),
            StructField("PRODUCT_FLAG",StringType()),
            StructField("PICTURE_FLAG",StringType()),
            StructField("NOPICTURE_FLAG",StringType()),
            StructField("NOTE_FLAG",StringType())
            ])

        # Read Date and Header for column names
    
        dataframe = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", ",")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .option("skip_blank_lines", True)\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
        

        #---------------------------Transformation logic ------------------------------#

        # Handle null values or empty rows
        dataframe= dataframe.na.drop("all")


        # Check for empty Dataframe
        if dataframe.count()==0:
            return "No Data in file"
            

        file_dt=file_name.split("_")[4].split(".")[0]

        dataframe=dataframe.withColumn("FILENAME",lit(file_name).cast("String"))
        dataframe=dataframe.withColumn("FILENAME_DT",lit(file_dt).cast("integer"))
        dataframe = dataframe.with_column("RUNID",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")).cast("integer"))
        dataframe = dataframe.with_column("CRT_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))

        # Creating Final Dataframe
        final_df = dataframe.alias("final_df")

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
		
		
		
CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.PHLSDL_RAW.PH_PERFECTSTORE_SURVEYMASTER_ACCTEXEC_PREPROCESSING("PARAM" ARRAY)
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
from snowflake.snowpark.functions import col,lit,trim, substring,current_timestamp,to_timestamp,date_format
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField
import pandas as pd
from datetime import datetime
import pytz

def main(session: snowpark.Session,Param): 

    try:

        #Param=[''ISE_tbl_AcctExec_PH_20240423020001.csv'',''PHLSDL_RAW.DEV_LOAD_STAGE_ADLS'',''dev/PH_Perfect_Store/master/acctexec'',''SDL_PH_TBL_ACCTEXEC'']

        # Extracting parameters from the input
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]


        # Define the schema for the DataFrame
        df_schema=StructType([
            StructField("CUSTCODE",StringType()),
            StructField("SLSPERID",StringType()),
            StructField("SUPCUSTCODE",StringType()),
            StructField("SUPID",StringType()),
            StructField("NAME",StringType()),
            StructField("POSITIONCODE",StringType()),
            StructField("POSITIONDESC",StringType())
            ])


        # Read Date and Header for column names
    
        dataframe = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", ",")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .option("skip_blank_lines", True)\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        #---------------------------Transformation logic ------------------------------#

        # Handle null values or empty rows
        dataframe= dataframe.na.drop("all")


        # Check for empty Dataframe
        if dataframe.count()==0:
            return "No Data in file"

        dataframe=dataframe.withColumn("FILENAME",lit(file_name).cast("String"))
        dataframe = dataframe.with_column("RUNID",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")).cast("integer"))
        dataframe = dataframe.with_column("CRT_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))

        # Creating Final Dataframe
        final_df = dataframe.alias("final_df")

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
        return error_message
';


CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.PHLSDL_RAW.PH_PERFECTSTORE_SURVEYNOTES_PREPROCESSING("PARAM" ARRAY)
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
from snowflake.snowpark.functions import col,lit,trim, substring,current_timestamp,to_timestamp,date_format
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField
import pandas as pd
from datetime import datetime
import pytz

def main(session: snowpark.Session,Param): 

    try:

        #Param=[''ISE_tbl_SurveyNotes_PH_20240424020818.csv'',''PHLSDL_RAW.DEV_LOAD_STAGE_ADLS'',''dev/PH_Perfect_Store/transaction/surveynotes/'',''SDL_PH_TBL_SURVEYNOTES'']

        # Extracting parameters from the input
        file_name       = Param[0]
        stage_name      = Param[1]
        schema          = Param[1].split(".")[0]
        temp_stage_path = Param[2]
        target_table    = Param[3]


        # Define the schema for the DataFrame
        df_schema=StructType([
            StructField("CUSTCODE",StringType()),
            StructField("SLSPERID",StringType()),
            StructField("BRANCHCODE",StringType()),
            StructField("ISEID",StringType()),
            StructField("QUESTIONNO",StringType()),
            StructField("ANSWERSEQ",StringType()),
            StructField("ANSWERNOTES",StringType()),
            StructField("CREATEDDATE",StringType())
            
            ])

        # Read Date and Header for column names
    
        dataframe = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", ",")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .option("skip_blank_lines", True)\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        #---------------------------Transformation logic ------------------------------#

        # Handle null values or empty rows
        dataframe= dataframe.na.drop("all")


        # Check for empty Dataframe
        if dataframe.count()==0:
            return "No Data in file"
        

        file_dt=file_name.split("_")[4].split(".")[0]

        df_pandas=dataframe.to_pandas()
        df_pandas[''CREATEDDATE''] = pd.to_datetime(df_pandas[''CREATEDDATE''])
        df_pandas[''CREATEDDATE''] = df_pandas[''CREATEDDATE''].dt.strftime(''%Y-%m-%d %H:%M:%S.%f'')

        new_df=session.create_dataframe(df_pandas)

        new_df=new_df.withColumn("FILENAME",lit(file_name).cast("String"))
        new_df=new_df.withColumn("FILENAME_DT",lit(file_dt).cast("String"))
        new_df = new_df.with_column("RUNID",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")).cast("integer"))
        new_df = new_df.with_column("CRT_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))

        # Creating Final Dataframe
        final_df = new_df.alias("final_df")

        # Load Data to the target table
        
        final_df.write.mode("append").saveAsTable(schema+"."+target_table)


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
		
		
CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.PHLSDL_RAW.PH_POS_711_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '# CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.PHLSDL_RAW.PH_POS_711_PREPROCESSING("PARAM" ARRAY)
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
    # Param=["Sales711_202401.csv","PHLSDL_RAW.DEV_LOAD_STAGE_ADLS","dev/pos/711/transaction_data","SDL_PH_POS_711"]
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        
        df_schema = StructType([
                # StructField("mnth_id",StringType(), True),
                StructField("store_cd",StringType(), True),
                StructField("store_nm",StringType(), True),
                StructField("Dummy_1",StringType(), True),
                StructField("item_category",StringType(), True),
                StructField("Dummy_2",StringType(), True),
                StructField("Dummy_3",StringType(), True),
                StructField("item_cd",StringType(), True),
                StructField("Dummy_4",StringType(), True),
                StructField("item_nm",StringType(255), True),
                StructField("tot_qty",StringType(), True),
                StructField("tot_amt",StringType(), True),
                StructField("no_of_sls_days",StringType(), True),
                StructField("avg_day_qty",StringType(), True),
                StructField("avg_amt_day",StringType(), True)
            ])
        
        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        
        
        df=df.na.drop("all")
        if df.count()==0 :
            return "No Data in file"
        df=df.na.drop(subset=[
                    "store_nm",
                    "item_category",
                    "item_cd",
                    "item_nm",
                    "tot_qty",
                    "tot_amt",
                    "no_of_sls_days",
                    "avg_day_qty",
                    "avg_amt_day"])
        
        df = df.with_column("cdl_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        file_name1=file_name.split(".")[0]+"_"+ datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")+".csv"
        df = df.with_column("file_name", lit(file_name1))
        df = df.with_column("crtd_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        extrctd_month_id=file_name.split("_")[1].split(".")[0]
        df = df.with_column("mnth_id", lit(extrctd_month_id))
        
        # date=file_name.split("_")[3].split(".")[0]
        # year=date[0:4]
        # mnth_no=date[4:6]
        # inv_week=date[6:]
        # df = df.withColumn("year", lit(year))
        # df = df.with_column("mnth_no", lit(mnth_no))
        # df = df.with_column("inv_week", lit(inv_week))  
        
        snowdf= df.select(
                    "mnth_id",
                    "store_cd",
                    "store_nm",
                    "item_category",
                    "item_cd",
                    "item_nm",
                    "tot_qty",
                    "tot_amt",
                    "no_of_sls_days",
                    "avg_day_qty",
                    "avg_amt_day"
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
		

CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.PHLSDL_RAW.PH_POS_DYNA_PREPROCESSING("PARAM" ARRAY)
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
    # Param=["DYNA_DYO_202311.csv","PHLSDL_RAW.DEV_LOAD_STAGE_ADLS","dev/pos/dyna/transaction_data","SDL_PH_POS_DYNA_SALES"]
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        
        df_schema = StructType([
                # StructField("mnth_id",StringType(), True),
                StructField("sls_area",StringType(), True),
                StructField("plant",StringType(), True),
                StructField("customer_id",StringType(), True),
                StructField("old_cust_id",StringType(), True),
                StructField("cust_nm",StringType(), True),
                StructField("chnl",StringType(), True),
                StructField("sls_off",StringType(), True),
                StructField("sls_grp",StringType(), True),
                StructField("address",StringType(), True),
                StructField("city",StringType(), True),
                StructField("postal_cd",StringType(), True),
                StructField("dsm",StringType(), True),
                StructField("sman",StringType(), True),
                StructField("prin",StringType(), True),
                StructField("principal",StringType(), True),
                StructField("matl_grp",StringType(), True),
                StructField("matl_sub_grp",StringType(), True),
                StructField("brand",StringType(), True),
                StructField("uom_conv",StringType(), True),
                StructField("matl_num",StringType(), True),
                StructField("old_matl_num",StringType(), True),
                StructField("matl_desc",StringType(), True),
                StructField("qty",StringType(), True),
                StructField("sls_amt",StringType(), True)
            ])
        
        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        
        
        df=df.na.drop("all")
        if df.count()==0 :
            return "No Data in file"
        # df=df.na.drop(subset=[
        #             "store_nm",
        #             "item_category",
        #             "item_cd",
        #             "item_nm",
        #             "tot_qty",
        #             "tot_amt",
        #             "no_of_sls_days",
        #             "avg_day_qty",
        #             "avg_amt_day"])
        
        df = df.with_column("cdl_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        file_name1=file_name.split(".")[0]+"_"+ datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")+".csv"
        df = df.with_column("file_name", lit(file_name1))
        df = df.with_column("crtd_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        extrctd_month_id=file_name.split("_")[2].split(".")[0]
        df = df.with_column("mnth_id", lit(extrctd_month_id))
        
        # date=file_name.split("_")[3].split(".")[0]
        # year=date[0:4]
        # mnth_no=date[4:6]
        # inv_week=date[6:]
        # df = df.withColumn("year", lit(year))
        # df = df.with_column("mnth_no", lit(mnth_no))
        # df = df.with_column("inv_week", lit(inv_week))  
        
        snowdf= df.select(
                    "mnth_id",
                    "sls_area",
                    "plant",
                    "customer_id",
                    "old_cust_id",
                    "cust_nm",
                    "chnl",
                    "sls_off",
                    "sls_grp",
                    "address",
                    "city",
                    "postal_cd",
                    "dsm",
                    "sman",
                    "prin",
                    "principal",
                    "matl_grp",
                    "matl_sub_grp",
                    "brand",
                    "uom_conv",
                    "matl_num",
                    "old_matl_num",
                    "matl_desc",
                    "qty",
                    "sls_amt"
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
		

CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.PHLSDL_RAW.PH_POS_MERCURY_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS 'from snowflake.snowpark.functions import col, lit, date_format, current_timestamp, to_date, year, month, concat, format_number, regexp_replace, to_timestamp, when, trim, upper
from snowflake.snowpark.types import StringType, StructType, StructField,DecimalType
import snowflake.snowpark
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz

def main(session: snowpark.Session,Param):
    #Param=["MDC_4351_202403.SP","PHLSDL_RAW.DEV_LOAD_STAGE_ADLS","dev/pos/TEST_PH_POS","SDL_PH_POS_mercury"]
    try:
        file_name = Param[0]
        stage_name = Param[1]
        temp_stage_path = Param[2]
        target_table = Param[3]

        df_schema = StructType([
                    StructField("masked", StringType(), True),
              
                ])
        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name.split(".")[0])
        #header= ["Branch","Code","Product","Code","Quantity"]
        
        #df=df.na.replace({"":None})      
        df = df.na.drop("all")
        if df.count() == 0:
            return "No Data in file"

        df=df.filter(col("masked").startswith("2"))

        yearmo = file_name.split("_")[2].split(".")[0]

        df = df.withColumn("yearmo", lit(yearmo))
        df = df.withColumn("branch_cd", col("masked").substring(2,4))
        
        df = df.withColumn("qty", col("masked").substring(12,16))

        df = df.withColumn("pc", col("masked").substring(6,6))
        
        df = df.withColumn("filename", lit(file_name))
        df = df.withColumn("crt_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        df = df.withColumn("run_id", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        snowdf = df.select(
                           "yearmo",
                           "branch_cd",
                            "pc",
                            "qty",
                            "filename",
                            "run_id",
                            "crt_dttm"
                        )
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        
        #move to success
        
        
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
        return error_message
        
        
        
        
        ';
		
		
CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.PHLSDL_RAW.PH_POS_PUREGOLD_PREPROCESSING("PARAM" ARRAY)
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
    # Param=["PG_202402.csv","PHLSDL_RAW.DEV_LOAD_STAGE_ADLS","dev/pos/puregold","sdl_ph_pos_puregold"]
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        
        df_schema = StructType([
                StructField("po_number",StringType(), True),
                StructField("vendor_code",StringType(), True),
                StructField("vendor_name",StringType(), True),
                StructField("from_store",StringType(), True),
                StructField("to_store",StringType(), True),
                StructField("store_name",StringType(), True),
                StructField("sku",StringType(), True),
                StructField("sku_desc",StringType(), True),
                StructField("qty",StringType(), True),
                StructField("rcr_number",StringType(), True)
            ])
        
        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "\\u0001")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

       
        
        df=df.na.drop("all")
        if df.count()==0 :
            return "No Data in file"

        
        # df = df.with_column("cdl_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        file_name1=file_name.split(".")[0]+"_"+ datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")+".csv"
        df = df.with_column("file_name", lit(file_name1))
        df = df.with_column("curr_date", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))

        snowdf= df.select(
                    "po_number",
                    "vendor_code",
                    "vendor_name",
                    "from_store",
                    "to_store",
                    "store_name",
                    "sku",
                    "sku_desc",
                    "qty",
                    "rcr_number",
                    "file_name",
                    "curr_date"
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
        return error_message
        
        
        
        
';


CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.PHLSDL_RAW.PH_POS_ROB_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS 'from snowflake.snowpark.functions import col, lit, date_format, current_timestamp, to_date, year, month, concat, format_number, regexp_replace, to_timestamp, when, trim, upper
from snowflake.snowpark.types import StringType, StructType, StructField,DecimalType
import snowflake.snowpark
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz

def main(session: snowpark.Session,Param):
    #Param=["ROB_202402.csv","PHLSDL_RAW.DEV_LOAD_STAGE_ADLS","dev/pos/TEST_PH_POS","SDL_PH_POS_ROBINSONS"]
    try:
        file_name = Param[0]
        stage_name = Param[1]
        temp_stage_path = Param[2]
        target_table = Param[3]

        df_schema = StructType([
                    StructField("sku_code", StringType()),
                    StructField("product_description", StringType()),
                    StructField("upc", StringType()),
                    StructField("store_code", StringType()),
                    StructField("store_description", StringType()),
                    StructField("uom", StringType()),
                    StructField("units_sold_ty", StringType()),
                    StructField("nat_sales_ty", StringType()),
                    StructField("tax_ty", StringType()),
                    StructField("gross_sales_ty", StringType())
                ])
        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",2)\\
            .option("field_delimiter", "\\u0001")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
        
        #df=df.na.replace({"":None})      
        df = df.na.drop("all")
        
        if df.count() == 0:
            return "No Data in file"

        yearmo = file_name.split("_")[1].split(".")[0]

        df = df.withColumn("yearmo", lit(yearmo))
        df = df.withColumn("filename", lit(file_name.split(".")[0]+".xlsx"))
        df = df.withColumn("crt_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        df = df.withColumn("run_id", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        df=df.na.replace(["0",None],subset="units_sold_ty")
        df = df.withColumn("cust_price", ((col("gross_sales_ty").cast(DecimalType(20, 10)) / col("units_sold_ty")).cast(DecimalType(20, 10))).cast(StringType()))
        df=df.na.fill("0",subset="cust_price")
        df=df.na.fill("0",subset="units_sold_ty")
        snowdf = df.select(
                            "yearmo",
                            "sku_code",
                            "product_description",
                            "upc",
                            "store_code",
                            "store_description",
                            "uom",
                            "units_sold_ty",
                            "nat_sales_ty",
                            "tax_ty",
                            "gross_sales_ty",
                            "cust_price",
                            "filename",
                            "run_id",
                            "crt_dttm"
                        )
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        
        #move to success
        
        file_name=file_name.split(".")[0]+"_"+datetime.now().strftime("%Y%m%d%H%M%S")
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
        return error_message
        
        
        
        
        ';
		
		
CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.PHLSDL_RAW.PH_POS_RUSTANS_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS 'from snowflake.snowpark.functions import col, lit, date_format, current_timestamp, to_date, year, month, concat, format_number, regexp_replace, to_timestamp, when, trim, upper
from snowflake.snowpark.types import StringType, StructType, StructField
import snowflake.snowpark
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz

def main(session: snowpark.Session,Param):
    #Param=["RS_202011.csv","PHLSDL_RAW.DEV_LOAD_STAGE_ADLS","dev/pos/TEST_PH_POS","SDL_PH_POS_RUSTANS"]
    try:
        file_name = Param[0]
        stage_name = Param[1]
        temp_stage_path = Param[2]
        target_table = Param[3]

        df_schema = StructType([
                    StructField("posloc", StringType()),
                    StructField("store", StringType()),
                    StructField("posdat", StringType()),
                    StructField("inumbr", StringType()),
                    StructField("idescr", StringType()),
                    StructField("posqty", StringType()),
                    StructField("posamt", StringType()),
                    StructField("asname", StringType())
                ])
        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "\\u0001")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
        
        #df=df.na.replace({"":None})      
        df = df.na.drop("all")
        
        if df.count() == 0:
            return "No Data in file"

        yearmo = file_name.split("_")[1].split(".")[0]

        df = df.withColumn("yearmo", lit(yearmo))
        df = df.withColumn("filename", lit(file_name.split(".")[0]+".xlsx"))
        df = df.withColumn("crt_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        df = df.withColumn("run_id", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))

        snowdf = df.select(
                           "yearmo",
                            "posloc",
                            "store",
                            "posdat",
                            "inumbr",
                            "idescr",
                            "posqty",
                            "posamt",
                            "asname",
                            "filename",
                            "run_id",
                            "crt_dttm"
                        )
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        
        #move to success
        file_name=file_name.split(".")[0]+"_"+datetime.now().strftime("%Y%m%d%H%M%S")
        
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
        return error_message
        
        
        
        
        ';
		
		
		
CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.PHLSDL_RAW.PH_POS_SM_GOODS_PREPROCESSING("PARAM" ARRAY)
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
    # Param=["SM_GR_202309.csv","PHLSDL_RAW.DEV_LOAD_STAGE_ADLS","dev/pos/sm_goods/pos_sm_goods","SDL_PH_POS_SM_GOODS"]
    try:
        file_name = Param[0]
        stage_name = Param[1]
        temp_stage_path = Param[2]
        target_table = Param[3]
        
        df_schema = StructType([
            StructField("business_name", StringType(), True),
            StructField("title", StringType(), True),     
            StructField("date", StringType(), True),
            StructField("vendor_code", StringType(), True),
            StructField("vendor_name", StringType(), True),
            StructField("receipt_date", StringType(), True),
            StructField("terms_and_discount", StringType(), True),
            StructField("site_code", StringType(), True),
            StructField("site_name", StringType(), True),
            StructField("ship_to", StringType(), True),
            StructField("gr_number", StringType(), True),
            StructField("po_number", StringType(), True),
            StructField("cancel_date", StringType(), True),
            StructField("total_articles", StringType(), True),
            StructField("article_number", StringType(), True),
             StructField("article_description", StringType(), True),
            StructField("upc", StringType(), True),
            StructField("uom", StringType(), True),
            StructField("received_qty", StringType(), True),
            StructField("remarks", StringType(), True),
                
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
            
        df = df.withColumn("crt_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        df = df.withColumn("file_name", lit(file_name))
        df = df.withColumn("run_id", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))

        snowdf = df.select(
            "business_name",
            "title",
            "date",
            "vendor_code",
            "vendor_name",
            "receipt_date",
            "terms_and_discount",
            "site_code",
            "site_name",
            "ship_to",
            "gr_number",
            "po_number",
            "cancel_date",
            "total_articles",
            "article_number",
            "article_description",
            "upc",
            "uom",
            "received_qty",
            "remarks",
            "file_name",
            "crt_dttm",
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


CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.PHLSDL_RAW.PH_POS_SOUTHSTAR_PREPROCESSING("PARAM" ARRAY)
RETURNS TABLE ()
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS 'from snowflake.snowpark.functions import col, lit, date_format, current_timestamp, to_date, year, month, concat, format_number, regexp_replace, to_timestamp, when, trim, upper
from snowflake.snowpark.types import StringType, StructType, StructField
import snowflake.snowpark
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz

def main(session: snowpark.Session,Param):
    #Param=["SS_202402.csv","PHLSDL_RAW.DEV_LOAD_STAGE_ADLS","dev/pos/TEST_PH_POS","SDL_PH_POS_RUSTANS"]
    try:
        file_name = Param[0]
        stage_name = Param[1]
        temp_stage_path = Param[2]
        target_table = Param[3]

        df_schema = StructType([
                    StructField("sku_code", StringType(), True),
                    StructField("sku_description", StringType(), True),
                    StructField("upc", StringType(), True),
                    StructField("supplier_product_code", StringType(), True),
                    StructField("store_code", StringType(), True),
                    StructField("store_description", StringType(), True),
                    StructField("units_sold_ty", StringType(), True),
                    StructField("net_sales_ty", StringType(), True)
                ])
        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "\\u0001")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
        
        #df=df.na.replace({"":None})      
        df = df.na.drop("all")
        
        if df.count() == 0:
            return "No Data in file"

        yearmo = file_name.split("_")[1].split(".")[0]

        df = df.withColumn("yearmo", lit(yearmo))
        df = df.withColumn("filename", lit(file_name.split(".")[0]+".xlsx"))
        df = df.withColumn("crt_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        df = df.withColumn("run_id", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))

        snowdf = df.select(
                           "yearmo",
                            "sku_code",
                            "sku_description",
                            "upc",
                            "supplier_product_code",
                            "store_code",
                            "store_description",
                            "units_sold_ty",
                            "net_sales_ty",
                            "filename",
                            "run_id",
                            "crt_dttm"
                        )
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        
        #move to success
        
        file_name=file_name.split(".")[0]+"_"+datetime.now().strftime("%Y%m%d%H%M%S")
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
        return error_message
        
        
        
        
        
    ';
	
	
CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.PHLSDL_RAW.PH_POS_WALTERMART_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS 'from snowflake.snowpark.functions import col, lit, date_format, current_timestamp, to_date, year, month, concat, format_number, regexp_replace, to_timestamp, when, trim, upper
from snowflake.snowpark.types import StringType, StructType, StructField,DecimalType
import snowflake.snowpark
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz

def main(session: snowpark.Session,Param):
    #Param=["WM_202402.csv","PHLSDL_RAW.DEV_LOAD_STAGE_ADLS","dev/pos/TEST_PH_POS","SDL_PH_POS_WALTERMART"]
    try:
        file_name = Param[0]
        stage_name = Param[1]
        temp_stage_path = Param[2]
        target_table = Param[3]

        df_schema = StructType([
                    StructField("month", StringType(), True),
                    StructField("coding", StringType(), True),
                    StructField("loccod", StringType(), True),
                    StructField("locnam", StringType(), True),
                    StructField("dept", StringType(), True),
                    StructField("sdept", StringType(), True),
                    StructField("class", StringType(), True),
                    StructField("sclass", StringType(), True),
                    StructField("catnam", StringType(), True),
                    StructField("vencod", StringType(), True),
                    StructField("vennam", StringType(), True),
                    StructField("sku", StringType(), True),
                    StructField("itmdes", StringType(), True),
                    StructField("sldqty", StringType(), True),
                    StructField("extprc", StringType(), True)
                ])
        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "\\u0001")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
        
        #df=df.na.replace({"":None})      
        df = df.na.drop("all")
        
        if df.count() == 0:
            return "No Data in file"

        yearmo = file_name.split("_")[1].split(".")[0]

        df = df.withColumn("yearmo", lit(yearmo))

        df = df.withColumn("filename", lit(file_name.split(".")[0]+".xlsx"))
        df = df.withColumn("crt_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        df = df.withColumn("run_id", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        df=df.na.replace(["0",None],subset="sldqty")
        df = df.withColumn("cust_price", ((col("extprc").cast(DecimalType(20, 10)) / col("sldqty")).cast(DecimalType(20, 10))).cast(StringType()))
        df=df.na.fill("0",subset="cust_price")
        df=df.na.fill("0",subset="sldqty")
        snowdf = df.select(
                           "yearmo",
                           "coding", "loccod", "locnam", "dept", "sdept",
                            "class", "sclass", "catnam", "vencod", "vennam",
                            "sku", "itmdes", "sldqty", "extprc",
                            "cust_price",
                            "filename",
                            "run_id",
                            "crt_dttm"
                        )
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        
        #move to success
        
        
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
        return error_message
        
        
        
        
        ';
		
		
CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.PHLSDL_RAW.PH_POS_WATSON_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS 'from snowflake.snowpark.functions import col, lit, date_format, current_timestamp, to_date, year, month, concat, format_number, regexp_replace, to_timestamp, when, trim, upper
from snowflake.snowpark.types import StringType, StructType, StructField,DecimalType
import snowflake.snowpark
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz

def main(session: snowpark.Session,Param):
    #Param=["WAT_BEAUTY_202402.csv","PHLSDL_RAW.DEV_LOAD_STAGE_ADLS","dev/pos/TEST_PH_POS","SDL_PH_POS_WATSONS"]
    try:
        file_name = Param[0]
        stage_name = Param[1]
        temp_stage_path = Param[2]
        target_table = Param[3]

        df_schema = StructType([
                                StructField("store", StringType()),
                                StructField("store_name", StringType()),
                                StructField("sku_code", StringType()),
                                StructField("sku_name", StringType()),
                                StructField("gross_sales_retail_ty", StringType()),
                                StructField("sales_units_ty", StringType())
                            ])
        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "\\u0001")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
        
        #df=df.na.replace({"":None})      
        df = df.na.drop("all")
        
        if df.count() == 0:
            return "No Data in file"

        yearmo = file_name.split("_")[2].split(".")[0]

        df = df.withColumn("yearmo", lit(yearmo))

        df = df.withColumn("filename", lit(file_name.split(".")[0]+".xlsx"))
        df = df.withColumn("crt_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        df = df.withColumn("run_id", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        df=df.na.replace(["0",None],subset="sales_units_ty")
        df = df.withColumn("cust_price", ((col("gross_sales_retail_ty").cast(DecimalType(20, 10)) / col("sales_units_ty")).cast(DecimalType(20, 10))).cast(StringType()))
        df=df.na.fill("0",subset="cust_price")
        df=df.na.fill("0",subset="sales_units_ty")
        snowdf = df.select(
                           "yearmo",
                            "store",
                            "store_name",
                            "sku_code",
                            "sku_name",
                            "gross_sales_retail_ty",
                            "sales_units_ty",
                            "cust_price",
                            "filename",
                            "run_id",
                            "crt_dttm"
                        )
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        
        #move to success
        
        file_name=file_name.split(".")[0]+"_"+datetime.now().strftime("%Y%m%d%H%M%S")
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
        return error_message
        
        
        
        
        ';
		
CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.PHLSDL_RAW.PH_SFMC_BOUNCE_PREPROCESSING("PARAM" ARRAY)
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
            
        #Param=[''PH_Action_Bounce_20240310_20240310174648.csv'',''PHLSDL_RAW.DEV_LOAD_STAGE_ADLS'',''dev/SFMC/PH_Action_Bounce'',''SDL_PH_SFMC_BOUNCE_DATA'']

        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        # Define the schema for the DataFrame
        
        df_schema = StructType([
            StructField("oyb_account_id", StringType()),
            StructField("job_id", StringType()),
            StructField("list_id", StringType()),
            StructField("batch_id", StringType()),
            StructField("subscriber_id", StringType()),
            StructField("subscriber_key", StringType()),
            StructField("event_date", StringType()),
            StructField("is_unique", StringType()),
            StructField("domain", StringType()),
            StructField("bounce_category_id", StringType()),
            StructField("bounce_category", StringType()),
            StructField("bounce_subcategory_id", StringType()),
            StructField("bounce_subcategory", StringType()),
            StructField("bounce_type_id", StringType()),
            StructField("bounce_type", StringType()),
            StructField("smtp_bounce_reason", StringType()),
            StructField("smtp_message", StringType()),
            StructField("smtp_code", StringType()),
            StructField("triggerer_send_definition_object_id", StringType()),
            StructField("triggered_send_customer_key", StringType()),
            StructField("email_subject", StringType()),
            StructField("bcc_email", StringType()),
            StructField("email_name", StringType()),
            StructField("email_id", StringType()),
            StructField("email_address", StringType())
        ])


        
        dataframe = session.read.schema(df_schema) \\
        .option("skip_header",1) \\
        .option("field_delimiter", "|") \\
        .option("field_optionally_enclosed_by", "\\"")  \\
        .option("encoding","UTF-8")\\
        .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        
        dataframe=dataframe.na.drop("all")
        if dataframe.count()==0:
            return "No Data in file"

        #logic as per sql script 
        dataframe= dataframe.filter((dataframe["oyb_account_id"].isNotNull()) & (dataframe["job_id"].isNotNull()) & (dataframe["list_id"].isNotNull()) & (dataframe["batch_id"].isNotNull()) & (dataframe["batch_id"].isNotNull())  & (dataframe["batch_id"].isNotNull()))

        new_file_name=file_name[0:25] +".csv"
        dataframe = dataframe.with_column("FILE_NAME",lit(new_file_name))
        dataframe = dataframe.with_column("CRTD_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))

        # Load Data to the target table
        dataframe.write.mode("append").saveAsTable(target_table)
       
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        #move to success
        file_name=file_name[0:25]+''_''+datetime.now().strftime("%Y%m%d%H%M%S")
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
        return error_message
        ';
		
		
CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.PHLSDL_RAW.PH_SFMC_CLICK_PREPROCESSING("PARAM" ARRAY)
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
            
        #Param=[''PH_Action_Click_20240315_20240315174839.csv'',''PHLSDL_RAW.DEV_LOAD_STAGE_ADLS'',''dev/SFMC/PH_Action_Click'',''SDL_PH_SFMC_CLICK_DATA'']

        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        # Define the schema for the DataFrame
        
        df_schema=StructType([
            StructField("oyb_account_id", StringType()),
            StructField("job_id", StringType()),
            StructField("list_id", StringType()),
            StructField("batch_id", StringType()),
            StructField("subscriber_id", StringType()),
            StructField("subscriber_key", StringType()),
            StructField("event_date", StringType()),
            StructField("domain", StringType()),
            StructField("url", StringType()),
            StructField("link_name", StringType()),
            StructField("link_content", StringType()),
            StructField("is_unique", StringType()),
            StructField("email_name", StringType()),
            StructField("email_subject", StringType())
        ])
                
        dataframe = session.read.schema(df_schema) \\
        .option("skip_header",1) \\
        .option("field_delimiter", "|") \\
        .option("field_optionally_enclosed_by", "\\"")  \\
        .option("encoding","UTF-8")\\
        .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        
        dataframe=dataframe.na.drop("all")
        if dataframe.count()==0:
            return "No Data in file"

        #logic as per sql script 
        dataframe= dataframe.filter((dataframe["oyb_account_id"].isNotNull()) & (dataframe["job_id"].isNotNull()) & (dataframe["list_id"].isNotNull()) & (dataframe["batch_id"].isNotNull()) & (dataframe["batch_id"].isNotNull())  & (dataframe["batch_id"].isNotNull()))

        new_file_name=file_name[0:24] +".csv"
        dataframe = dataframe.with_column("FILE_NAME",lit(new_file_name))
        dataframe = dataframe.with_column("CRTD_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))

        # Load Data to the target table
        dataframe.write.mode("append").saveAsTable(target_table)
       
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        #move to success
        file_name=file_name[0:24]+''_''+datetime.now().strftime("%Y%m%d%H%M%S")
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
        return error_message
        ';
		
		
CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.PHLSDL_RAW.PH_SFMC_COMPLAINT_PREPROCESSING("PARAM" ARRAY)
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
            
        #Param=[''PH_Action_Complaint_20231204_20231204174130.csv'',''PHLSDL_RAW.DEV_LOAD_STAGE_ADLS'',''dev/SFMC/PH_Action_Complaint'',''SDL_PH_SFMC_COMPLAINT_DATA'']

        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        # Define the schema for the DataFrame
        
        df_schema=StructType([
                    StructField("oyb_account_id", StringType()),
                    StructField("job_id", StringType()),
                    StructField("list_id", StringType()),
                    StructField("batch_id", StringType()),
                    StructField("subscriber_id", StringType()),
                    StructField("subscriber_key", StringType()),
                    StructField("event_date", StringType()),
                    StructField("is_unique", StringType()),
                    StructField("domain", StringType()),
                    StructField("email_subject", StringType()),
                    StructField("email_name", StringType()),
                    StructField("email_id", StringType())
                ])
                
        dataframe = session.read.schema(df_schema) \\
        .option("skip_header",1) \\
        .option("field_delimiter", "|") \\
        .option("field_optionally_enclosed_by", "\\"")  \\
        .option("encoding","UTF-8")\\
        .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        
        dataframe=dataframe.na.drop("all")
        if dataframe.count()==0:
            return "No Data in file"

        #logic as per sql script 
        dataframe= dataframe.filter((dataframe["oyb_account_id"].isNotNull()) & (dataframe["job_id"].isNotNull()) & (dataframe["list_id"].isNotNull()) & (dataframe["batch_id"].isNotNull()) & (dataframe["batch_id"].isNotNull())  & (dataframe["batch_id"].isNotNull()))

        new_file_name=file_name[0:28] +".csv"
        dataframe = dataframe.with_column("FILE_NAME",lit(new_file_name))
        dataframe = dataframe.with_column("CRTD_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))

        # Load Data to the target table
        dataframe.write.mode("append").saveAsTable(target_table)
       
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        #move to success
        file_name=file_name[0:28]+''_''+datetime.now().strftime("%Y%m%d%H%M%S")
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
        return error_message
        ';
		
CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.PHLSDL_RAW.PH_SFMC_OPEN_PREPROCESSING("PARAM" ARRAY)
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
            
        #Param=[''PH_Action_Open_20240327_20240327174619.csv'',''PHLSDL_RAW.DEV_LOAD_STAGE_ADLS'',''dev/SFMC/PH_Action_Open'',''SDL_PH_SFMC_OPEN_DATA'']

        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        # Define the schema for the DataFrame
        
        df_schema=StructType([
                StructField("oyb_account_id", StringType()),
                StructField("job_id", StringType()),
                StructField("list_id", StringType()),
                StructField("batch_id", StringType()),
                StructField("subscriber_id", StringType()),
                StructField("subscriber_key", StringType()),
                StructField("email_name", StringType()),
                StructField("email_subject", StringType()),
                StructField("bcc_email", StringType()),
                StructField("email_id", StringType()),
                StructField("event_date", StringType()),
                StructField("domain", StringType()),
                StructField("is_unique", StringType())
            ])
                
        dataframe = session.read.schema(df_schema) \\
        .option("skip_header",1) \\
        .option("field_delimiter", "|") \\
        .option("field_optionally_enclosed_by", "\\"")  \\
        .option("encoding","UTF-8")\\
        .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        
        dataframe=dataframe.na.drop("all")
        if dataframe.count()==0:
            return "No Data in file"

        #logic as per sql script 
        dataframe= dataframe.filter((dataframe["oyb_account_id"].isNotNull()) & (dataframe["job_id"].isNotNull()) & (dataframe["list_id"].isNotNull()) & (dataframe["batch_id"].isNotNull()) & (dataframe["batch_id"].isNotNull())  & (dataframe["batch_id"].isNotNull()))

        new_file_name=file_name[0:23] +".csv"
        dataframe = dataframe.with_column("FILE_NAME",lit(new_file_name))
        dataframe = dataframe.with_column("CRTD_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))

        # Load Data to the target table
        dataframe.write.mode("append").saveAsTable(target_table)
       
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        #move to success
        file_name=file_name[0:23]+''_''+datetime.now().strftime("%Y%m%d%H%M%S")
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
        return error_message
        ';
		
		
CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.PHLSDL_RAW.PH_SFMC_SENT_PREPROCESSING("PARAM" ARRAY)
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
            
        #Param=[''PH_Action_Sent_20240314_20240314175016.csv'',''PHLSDL_RAW.DEV_LOAD_STAGE_ADLS'',''dev/SFMC/PH_Action_Sent'',''SDL_PH_SFMC_SENT_DATA'']

        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        # Define the schema for the DataFrame
        
        df_schema=StructType([
                StructField("oyb_account_id", StringType()),
                StructField("job_id", StringType()),
                StructField("list_id", StringType()),
                StructField("batch_id", StringType()),
                StructField("subscriber_id", StringType()),
                StructField("subscriber_key", StringType()),
                StructField("email_name", StringType()),
                StructField("email_subject", StringType()),
                StructField("bcc_email", StringType()),
                StructField("email_id", StringType()),
                StructField("event_date", StringType()),
                StructField("domain", StringType()),
                StructField("is_unique", StringType())
            ])
                
        dataframe = session.read.schema(df_schema) \\
        .option("skip_header",1) \\
        .option("field_delimiter", "|") \\
        .option("field_optionally_enclosed_by", "\\"")  \\
        .option("encoding","UTF-8")\\
        .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        
        dataframe=dataframe.na.drop("all")
        if dataframe.count()==0:
            return "No Data in file"

        #logic as per sql script 
        dataframe= dataframe.filter((dataframe["oyb_account_id"].isNotNull()) & (dataframe["job_id"].isNotNull()) & (dataframe["list_id"].isNotNull()) & (dataframe["batch_id"].isNotNull()) & (dataframe["batch_id"].isNotNull())  & (dataframe["batch_id"].isNotNull()))

        new_file_name=file_name[0:23] +".csv"
        dataframe = dataframe.with_column("FILE_NAME",lit(new_file_name))
        dataframe = dataframe.with_column("CRTD_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))

        # Load Data to the target table
        dataframe.write.mode("append").saveAsTable(target_table)
       
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        #move to success
        file_name=file_name[0:23]+''_''+datetime.now().strftime("%Y%m%d%H%M%S")
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
        return error_message
        ';
		
CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.PHLSDL_RAW.PH_SFMC_SENT_PREPROCESSING("PARAM" ARRAY)
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
            
        #Param=[''PH_Action_Sent_20240314_20240314175016.csv'',''PHLSDL_RAW.DEV_LOAD_STAGE_ADLS'',''dev/SFMC/PH_Action_Sent'',''SDL_PH_SFMC_SENT_DATA'']

        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        # Define the schema for the DataFrame
        
        df_schema=StructType([
                StructField("oyb_account_id", StringType()),
                StructField("job_id", StringType()),
                StructField("list_id", StringType()),
                StructField("batch_id", StringType()),
                StructField("subscriber_id", StringType()),
                StructField("subscriber_key", StringType()),
                StructField("email_name", StringType()),
                StructField("email_subject", StringType()),
                StructField("bcc_email", StringType()),
                StructField("email_id", StringType()),
                StructField("event_date", StringType()),
                StructField("domain", StringType()),
                StructField("is_unique", StringType())
            ])
                
        dataframe = session.read.schema(df_schema) \\
        .option("skip_header",1) \\
        .option("field_delimiter", "|") \\
        .option("field_optionally_enclosed_by", "\\"")  \\
        .option("encoding","UTF-8")\\
        .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        
        dataframe=dataframe.na.drop("all")
        if dataframe.count()==0:
            return "No Data in file"

        #logic as per sql script 
        dataframe= dataframe.filter((dataframe["oyb_account_id"].isNotNull()) & (dataframe["job_id"].isNotNull()) & (dataframe["list_id"].isNotNull()) & (dataframe["batch_id"].isNotNull()) & (dataframe["batch_id"].isNotNull())  & (dataframe["batch_id"].isNotNull()))

        new_file_name=file_name[0:23] +".csv"
        dataframe = dataframe.with_column("FILE_NAME",lit(new_file_name))
        dataframe = dataframe.with_column("CRTD_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))

        # Load Data to the target table
        dataframe.write.mode("append").saveAsTable(target_table)
       
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        #move to success
        file_name=file_name[0:23]+''_''+datetime.now().strftime("%Y%m%d%H%M%S")
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
        return error_message
        ';
