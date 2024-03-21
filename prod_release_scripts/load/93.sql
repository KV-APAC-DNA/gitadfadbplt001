USE SCHEMA THASDL_RAW;

CREATE OR REPLACE PROCEDURE SDL_TH_TESCO_TRANSDATA("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '

import snowflake.snowpark as snowpark
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DateType
from snowflake.snowpark.functions import col,lit,date_format,current_timestamp,to_timestamp, xmlget, flatten, get, when, substring, row_number
from snowflake.snowpark import Window
import pandas as pd
from datetime import datetime
import pytz

def get_xml_element(
        column:str,
        element:str,
        datatype:str,
        with_alias:bool = True
):
    new_element = (
        get(
            xmlget(
                col(column),
                lit(element),
            ),
            lit(''$'')
        )
        .cast("string")
    )

    new_element = when(new_element=='''', None).otherwise(new_element).cast(datatype)

    
    return (
        new_element.alias(element) if with_alias else new_element
    )
    



def main(session:snowpark.Session, Param):

    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]

        df = session.read\\
        .option("STRIP_OUTER_ELEMENT", False) \\
        .xml("@" + stage_name + "/" + temp_stage_path + "/" + file_name) \\
        .select(
            xmlget(col("$1"), lit("invrptth")).alias("invrptth"),
            xmlget(xmlget(col("$1"), lit("invrptth")), lit("ir_header")).alias("ir_header"),
            xmlget(xmlget(col("$1"), lit("invrptth")), lit("ir_items")).alias("ir_items"),

            get_xml_element("ir_header", "creation_date", "string"),
            get_xml_element("ir_header", "supplier_ID", "string"),
            get_xml_element("ir_header", "supplier_name", "string"),
            get_xml_element("ir_header", "warehouse", "string"),
            get_xml_element("ir_header", "delivery_point_name", "string"),
            get_xml_element("ir_header", "IR_date", "string")
        ) \\
        .select(
            "creation_date",
            "supplier_ID",
            "supplier_name",
            "warehouse",
            "delivery_point_name",
            "IR_date",
            flatten(col("ir_items"), "$")
        ) \\
        .select (
            "creation_date",
            "supplier_ID",
            "supplier_name",
            "warehouse",
            "delivery_point_name",
            "IR_date",
            get_xml_element("value", "EANSKU", "string"),
            get_xml_element("value", "article_ID", "string"),
            get_xml_element("value", "SPN", "string"),
            get_xml_element("value", "article_name", "string"),
            get_xml_element("value", "stock", "string"),
            get_xml_element("value", "sales", "string"),
            get_xml_element("value", "sales_amount", "string"),
        )

        
        df = df.with_column("FOLDER_NAME", lit(temp_stage_path))\\
               .with_column("FILE_NAME", lit(file_name))

        
        df_filtered = df.filter((col("IR_DATE").is_not_null()) & (col("IR_DATE").cast(StringType()) != ''''))

        
        windowSpec = Window.partitionBy("IR_DATE", "WAREHOUSE", "SUPPLIER_ID").orderBy(substring(col("FILE_NAME"), 8, 26).desc())
        df_with_rank = df_filtered.withColumn("RANK", row_number().over(windowSpec))

        
        df_rank_1_files = df_with_rank.filter(col("RANK") == 1).select("FILE_NAME").distinct()

        
        df_filtered = df_filtered.join(df_rank_1_files, "FILE_NAME", "inner")

        snowdf = df_filtered.select(
            "CREATION_DATE",
            "SUPPLIER_ID",
            "SUPPLIER_NAME",
            "WAREHOUSE",
            "DELIVERY_POINT_NAME",
            "IR_DATE",
            "EANSKU",
            "ARTICLE_ID",
            "SPN",
            "ARTICLE_NAME",
            "STOCK",
            "SALES",
            "SALES_AMOUNT",
            "FILE_NAME",
            "FOLDER_NAME"
        )

        
		snowdf=snowdf.na.drop("all")
        if snowdf.count() == 0:
            return "No Data in table"

        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")

        
        file_name1=("_").join(file_name.split("_")[0:5])+"_"+datetime.now().strftime("%Y%m%d%H%M%S")
        snowdf.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name1,header=True,OVERWRITE=True)
        
        
        snowdf.write.mode("append").save_as_table(stage_name.split(".")[0]+"."+target_table)
        
        
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

CREATE OR REPLACE PROCEDURE TH_ACTION_BOUNCE_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '

import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col,lit
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz

def main(session: snowpark.Session,Param): 
    try :
        

        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
       
        df_schema =  StructType([
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
        
        dataframe = session.read\\
        .schema(df_schema)\\
        .option("skip_header",1)\\
        .option("field_delimiter", "|")\\
        .option("field_optionally_enclosed_by", "\\"") \\
        .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        dataframe=dataframe.na.drop("all")
        if dataframe.count()==0:
            return "No Data in file"
        

        
        new_file_name=file_name[0:25] +".csv"
        dataframe = dataframe.with_column("FILE_NAME",lit(new_file_name))
        dataframe = dataframe.with_column("CRTD_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))


       
        final_df = dataframe.alias("final_df")

       
        
        final_df.write.mode("append").saveAsTable(target_table)

        
        file_name=file_name[0:25]+''_''+datetime.now().strftime("%Y%m%d%H%M%S")
		
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
		
        final_df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)
        
        return ''Success''
        
    except KeyError as key_error:
        
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message

    except pd.errors.MergeError as merge_error:
        
        error_message = f"DataFrame merging error: {str(merge_error)}"
        return error_message
    
    except Exception as e:
        
        error_message = f"Error: {str(e)}"
        return error_message';
CREATE OR REPLACE PROCEDURE TH_ACTION_CLICK_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '

import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col,lit
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DecimalType,DateType
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz

def main(session: snowpark.Session,Param): 
    try :
        

        

        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        
        df_schema = StructType([
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
        
        dataframe = session.read\\
        .schema(df_schema)\\
        .option("skip_header",1)\\
        .option("field_delimiter", "|")\\
        .option("field_optionally_enclosed_by", "\\"") \\
        .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        dataframe=dataframe.na.drop("all")
        if dataframe.count()==0:
            return "No Data in file"
        

        
        new_file_name=file_name[0:24] +".csv"
        dataframe = dataframe.with_column("FILE_NAME",lit(new_file_name))
        dataframe = dataframe.with_column("CRTD_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))


       
        final_df = dataframe.alias("final_df")

        
        
        final_df.write.mode("append").saveAsTable(target_table)

        
        file_name=file_name[0:24]+''_''+datetime.now().strftime("%Y%m%d%H%M%S")
		
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        final_df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)
        
        return ''Success''
        
    except KeyError as key_error:
        
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message

    except pd.errors.MergeError as merge_error:
        
        error_message = f"DataFrame merging error: {str(merge_error)}"
        return error_message
    
    except Exception as e:
        
        error_message = f"Error: {str(e)}"
        return error_message';
CREATE OR REPLACE PROCEDURE TH_ACTION_OPEN_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '

import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col,lit
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DecimalType,DateType
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz



def main(session: snowpark.Session,Param): 
    
    try :

        

        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
       
        df_schema = StructType([
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
        .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        dataframe=dataframe.na.drop("all")
        if dataframe.count()==0:
            return "No Data in file"
        

        new_file_name=file_name[0:23] +".csv"
        dataframe = dataframe.with_column("FILE_NAME",lit(new_file_name))
        dataframe = dataframe.with_column("CRTD_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))


       
        final_df = dataframe.alias("final_df")

        
        
        final_df.write.mode("append").saveAsTable(target_table)

        
        file_name=file_name[0:23]+''_''+datetime.now().strftime("%Y%m%d%H%M%S")
		
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")

        final_df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)
        
        return ''Success''
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
CREATE OR REPLACE PROCEDURE TH_ACTION_SENT_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '

import snowflake.snowpark as snowpark
from snowflake import snowpark
from snowflake.snowpark.functions import col,lit,trim
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField,DecimalType,DateType,TimestampType
from datetime import datetime
import pytz

def main(session: snowpark.Session,Param): 

    try:

        
        
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        


        df_schema=StructType([
            StructField("OYB_ACCOUNT_ID",StringType()),
            StructField("JOB_ID",StringType()),
            StructField("LIST_ID",StringType()),
            StructField("BATCH_ID",StringType()),
            StructField("SUBSCRIBER_ID",StringType()),
            StructField("SUBSCRIBER_KEY",StringType()),
            StructField("EVENT_DATE",TimestampType()),
            StructField("DOMAIN",StringType()),
            StructField("TRIGGERER_SEND_DEFINITION_OBJECT_ID",StringType()),
            StructField("TRIGGERED_SEND_CUSTOMER_KEY",StringType()),
            StructField("EMAIL_NAME",StringType()),
            StructField("EMAIL_SUBJECT",StringType()),
            StructField("EMAIL_ID",StringType())
            ])


        
    
        dataframe = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "|")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)


        
        

        dataframe=dataframe.na.drop("all")
		if dataframe.count()==0:
            return "No Data in file"


        dataframe = dataframe.withColumn("EMAIL_SUBJECT", trim(dataframe["EMAIL_SUBJECT"]))

   
        new_file_name=file_name[0:23] +".csv"

        dataframe = dataframe.with_column("FILE_NAME",lit(new_file_name))
        dataframe = dataframe.with_column("CRTD_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))

        dataframe=dataframe.select("OYB_ACCOUNT_ID","JOB_ID","LIST_ID","BATCH_ID","SUBSCRIBER_ID","SUBSCRIBER_KEY","EVENT_DATE","DOMAIN","TRIGGERER_SEND_DEFINITION_OBJECT_ID","TRIGGERED_SEND_CUSTOMER_KEY","EMAIL_NAME","EMAIL_SUBJECT","EMAIL_ID","FILE_NAME","CRTD_DTTM")


        final_df = dataframe.alias("final_df")

        



        final_df.write.mode("append").saveAsTable(target_table)
        
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")

   
    
        file_name=file_name.split(".")[0]
        final_df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)
        
        return "Success"


    except KeyError as key_error:
 
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message
        
    except Exception as e:

        error_message = f"Error: {str(e)}"
        return error_message';
CREATE OR REPLACE PROCEDURE TH_ACTION_UNSUBSCRIBE_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '

import snowflake.snowpark as snowpark
from snowflake import snowpark
from snowflake.snowpark.functions import col,lit,trim
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField,DecimalType,DateType,TimestampType
from datetime import datetime
import pytz

def main(session: snowpark.Session,Param): 

    try:

        

       
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        

        
        df_schema=StructType([
            StructField("OYB_ACCOUNT_ID",StringType()),
            StructField("JOB_ID",StringType()),
            StructField("LIST_ID",StringType()),
            StructField("BATCH_ID",StringType()),
            StructField("SUBSCRIBER_ID",StringType()),
            StructField("SUBSCRIBER_KEY",StringType()),
            StructField("EVENT_DATE",TimestampType()),
            StructField("DOMAIN",StringType()),
            StructField("EMAIL_NAME",StringType()),
            StructField("EMAIL_SUBJECT",StringType()),
            StructField("EMAIL_ID",StringType()),
            StructField("IS_UNIQUE",StringType())
            ])



    
        dataframe = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "|")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)


        

        dataframe=dataframe.na.drop("all")
		if dataframe.count()==0:
            return "No Data in file"


        new_file_name=file_name[0:30] +".csv"

        dataframe = dataframe.with_column("FILE_NAME",lit(new_file_name))
        dataframe = dataframe.with_column("CRTD_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))



        final_df = dataframe.alias("final_df")

        

       

 
        final_df.write.mode("append").saveAsTable(target_table)

        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")

     
    
        file_name=file_name.split(".")[0]
        final_df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)
        
        return "Success"


    except KeyError as key_error:

        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message
        
    except Exception as e:

        error_message = f"Error: {str(e)}"
        return error_message';
CREATE OR REPLACE PROCEDURE TH_CRM_CHILDREN_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '

import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col,lit,trim,when
from snowflake.snowpark.types import  StringType, StructType, StructField
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz



def main(session: snowpark.Session,Param): 
       
    try :

        

        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]

        
        df_schema = StructType([
                StructField("parent_key", StringType()),
                StructField("child_nm", StringType()),
                StructField("child_birth_mnth", StringType()),
                StructField("child_birth_year", StringType()),
                StructField("child_gender", StringType()),
                StructField("child_number", StringType())
            ])
        
        dataframe = session.read.schema(df_schema) \\
        .option("skip_header",1) \\
        .option("field_delimiter", "|") \\
        .option("field_optionally_enclosed_by", "\\"")  \\
        .option("encoding","UTF-16")\\
        .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        dataframe=dataframe.na.drop("all")
        if dataframe.count()==0:
            return "No Data in file"


  
        new_file_name=file_name[0:24] +".csv"
        dataframe = dataframe.with_column("FILE_NAME",lit(new_file_name))
        dataframe = dataframe.with_column("CRTD_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))


       


        dataframe = dataframe.withColumn("CHILD_BIRTH_MNTH",  (
             when(trim(col("child_birth_mnth")) == "มกราคม", "01")
            .when(trim(col("child_birth_mnth")) == "กุมภาพันธ์", "02")
            .when(trim(col("child_birth_mnth")) == "มีนาคม", "03")
            .when(trim(col("child_birth_mnth")) == "เมษายน", "04")
            .when(trim(col("child_birth_mnth")) == "พฤษภาคม", "05")
            .when(trim(col("child_birth_mnth")) == "มิถุนายน", "06")
            .when(trim(col("child_birth_mnth")) == "กรกฎาคม", "07")
            .when(trim(col("child_birth_mnth")) == "สิงหาคม", "08")
            .when(trim(col("child_birth_mnth")) == "กันยายน", "09")
            .when(trim(col("child_birth_mnth")) == "ตุลาคม", "10")
            .when(trim(col("child_birth_mnth")) == "พฤศจิกายน", "11")
            .when(trim(col("child_birth_mnth")) == "ธันวาคม", "12")
            .otherwise("UNDEFINED")

        ))

 
        final_df = dataframe.select("PARENT_KEY","CHILD_NM","CHILD_BIRTH_MNTH","CHILD_BIRTH_YEAR","CHILD_GENDER","CHILD_NUMBER","FILE_NAME","CRTD_DTTM")

        
 
        final_df.write.mode("append").saveAsTable(target_table)

        file_name=file_name[0:24]+''_''+datetime.now().strftime("%Y%m%d%H%M%S")
  

        
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")

        final_df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)

        
        return ''Success''
        
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


