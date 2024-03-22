use schema THASDL_RAW;
CREATE OR REPLACE PROCEDURE THASDL_RAW.TH_CRM_CONSUMER_MASTER_ADDITIONAL_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them.

import snowflake.snowpark as snowpark
from snowflake.snowpark.window import Window
from snowflake.snowpark.functions import col,lit,trim,when,row_number,when_matched
from snowflake.snowpark.types import  StringType, StructType, StructField
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz



def main(session: snowpark.Session,Param): 
       
    try :

        #Param=["TH_CRM_Consumer_Master_Additional_20240317","THASDL_RAW.DEV_LOAD_STAGE_ADLS","dev/cert_data_lake/SFMC/TH_CRM_Consumer_Master_Additional","SDL_TH_SFMC_consumer_master_additional"]

        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        # Define the schema for the DataFrame
        
        df_schema = StructType([
                StructField("subscriber_key", StringType()),
                StructField("attribute_name", StringType()),
                StructField("attribute_value", StringType())
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
       
        # #---------------------------Transformation logic ------------------------------#

        # Add FILE_NAME and CRTD_DTTM to the dataframe
        new_file_name=file_name[0:42] +".csv"
        dataframe = dataframe.with_column("FILE_NAME",lit(new_file_name))
        dataframe = dataframe.with_column("CRTD_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))



        duplicates = dataframe.with_column("Duplicate", row_number().over(Window.partition_by(col("subscriber_key"), col("attribute_name"), col("attribute_value"), col("file_name")).order_by(col("subscriber_key"))))
        duplicates = duplicates.filter(col("Duplicate") > 1)
        dataframe=dataframe.join(duplicates,((dataframe["subscriber_key"] == duplicates["subscriber_key"]) & (dataframe["attribute_name"] == duplicates["attribute_name"]) &  (dataframe["attribute_value"] == duplicates["attribute_value"])  &  (dataframe["file_name"] == duplicates["file_name"])),"left_anti")  \\
                                 .select(dataframe["subscriber_key"],dataframe["attribute_name"],dataframe["attribute_value"],dataframe["file_name"],dataframe["CRTD_DTTM"])
        
        #dataframe.merge(duplicates, dataframe["subscriber_key"] == duplicates["subscriber_key"], [when_matched().delete()])
        
        
        
        
        
        # Load Data to the target table
        dataframe.write.mode("append").saveAsTable(target_table)

        # write to success folder
        file_name=file_name[0:42]+''_''+datetime.now().strftime("%Y%m%d%H%M%S")
    
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        #move to success
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

CREATE OR REPLACE PROCEDURE TH_CRM_CONSUMER_MASTER_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them.

import snowflake.snowpark as snowpark
from snowflake.snowpark.window import Window
from snowflake.snowpark.functions import col,lit,trim,when,row_number,when_matched
from snowflake.snowpark.types import  StringType, StructType, StructField
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz
import sys
from pathlib import Path
import warnings
import os 



def main(session: snowpark.Session,Param): 
       
    try :
        #Param =[''TH_CRM_Consumer_Master_20240317.csv'',''THASDL_RAW.DEV_LOAD_STAGE_ADLS'',''dev/cert_data_lake/SFMC/'',''SDL_TH_SFMC_consumer_master'']
	

        
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        # Define the schema for the DataFrame
        
        df_schema =  schema = StructType([
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
                    StructField("Source", StringType()),
                    StructField("Medium", StringType()),
                    StructField("Brand", StringType()),
                    StructField("Address_Country", StringType()),
                    StructField("Campaign_ID", StringType()),
                    StructField("Created_Date", StringType()),
                    StructField("Updated_Date", StringType()),
                    StructField("Unsubscribe_Date", StringType()),
                    StructField("Email", StringType()),
                    StructField("Full_Name", StringType()),
                    StructField("Last_Logon_Time", StringType()),
                    StructField("Remaining_Points", StringType()),
                    StructField("Redeemed_Points", StringType()),
                    StructField("Total_Points", StringType()),
                    StructField("Gender", StringType()),
                    StructField("Line_ID", StringType()),
                    StructField("Line_Name", StringType()),
                    StructField("Line_Email", StringType()),
                    StructField("LINE_Channel_ID", StringType()),
                    StructField("Address_Region", StringType()),
                    StructField("Tier", StringType()),
                    StructField("Opt_In_For_Communication", StringType()),
                    StructField("Smoker", StringType()),
                    StructField("Have_Kid", StringType()),
                    StructField("Expectant_Mother", StringType()),
                    StructField("Category_they_are_using", StringType()),
                    StructField("Skin_Condition", StringType()),
                    StructField("Skin_Problem", StringType()),
                    StructField("Use_Mouthwash", StringType()),
                    StructField("Mouthwash_time", StringType()),
                    StructField("Why_not_use_Mouthwash", StringType()),
                    StructField("Oral_Problem", StringType()),
                    StructField("Age", StringType())
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

        # ----------------  creating  TH_CRM_Consumer_Master_Additional_ file start ------------------
       
        raw_data=dataframe.to_pandas()

        
        #removing hard coded values from fn
        null_value = "Null"
        
        lst_columns_attr = [''SUBSCRIBER_KEY'',''SMOKER'',''EXPECTANT_MOTHER'',
               ''CATEGORY_THEY_ARE_USING'', ''SKIN_CONDITION'', ''SKIN_PROBLEM'',
               ''USE_MOUTHWASH'', ''MOUTHWASH_TIME'', ''WHY_NOT_USE_MOUTHWASH'',
               ''ORAL_PROBLEM'']
        primary_key = "SUBSCRIBER_KEY"
        lst_attr_col = ["SUBSCRIBER_KEY","Attribute_Name","Attribute_value"]
        attr_val = ''Attribute_value''
        lst_columns_master = [''SMOKER'',''EXPECTANT_MOTHER'',
               ''CATEGORY_THEY_ARE_USING'', ''SKIN_CONDITION'', ''SKIN_PROBLEM'',
               ''USE_MOUTHWASH'', ''MOUTHWASH_TIME'', ''WHY_NOT_USE_MOUTHWASH'',
               ''ORAL_PROBLEM'']
        # fill the null values with ''Null''
       
        
        raw_data = raw_data.fillna(null_value)
    
    
    
        # # calling the required columns from main dataframe
        new_attr_df = raw_data[lst_columns_attr]
        
    
        # preparing data for stacking
        new_attr_df_indexed = new_attr_df.set_index(primary_key)
        new_attr_df_stacked = pd.DataFrame(new_attr_df_indexed.stack()).reset_index()
    
        # renaming columns for the new file
        new_attr_df_stacked.columns = lst_attr_col
    
        # removing the empty spaces
        new_attr_df_stacked[attr_val] = new_attr_df_stacked[attr_val].str.split('','').map(lambda elements: [e.strip() for e in elements])
        new_attr_df_stacked[attr_val] = new_attr_df_stacked[attr_val].map(lambda elements: list(filter(None, elements)))
    
        # final dataframe
        df_transposed = new_attr_df_stacked.explode(attr_val).reset_index(drop=True)
    
        # removing null values
        df_transposed = df_transposed[df_transposed[attr_val] != null_value]
        df_transposed = df_transposed.dropna(subset=[attr_val]).reset_index(drop=True)

        dataframe=session.create_dataframe(df_transposed)
        filename="TH_CRM_Consumer_Master_Additional_" + Path(file_name).stem[23:31]
        dataframe.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"TH_CRM_Consumer_Master_Additional/"+"/"+filename,file_format_type="csv",format_type_options={''COMPRESSION'':''None'',"FIELD_DELIMITER":"|"},partition_by=None,overwrite=True,header=True)

        # ---------------- TH_CRM_Consumer_Master_Additional_ FILE HAS BEEN Created   ------------------

        # ---------------- TH_CRM_Consumer_Master_Primary_ processing freshly    ------------------
        
        dataframe = session.read.schema(df_schema) \\
        .option("skip_header",1) \\
        .option("field_delimiter", "|") \\
        .option("field_optionally_enclosed_by", "\\"")  \\
        .option("encoding","UTF-16")\\
        .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        filename="TH_CRM_Consumer_Master_Primary_" + Path(file_name).stem[23:31]
        dataframe = dataframe.with_column("FILE_NAME",lit(filename))
        dataframe = dataframe.with_column("CRTD_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        dataframe = dataframe.withColumn("BIRTHDAY_MONTH",  (
             when(trim(col("BIRTHDAY_MONTH")) == "มกราคม", "01")
            .when(trim(col("BIRTHDAY_MONTH")) == "กุมภาพันธ์", "02")
            .when(trim(col("BIRTHDAY_MONTH")) == "มีนาคม", "03")
            .when(trim(col("BIRTHDAY_MONTH")) == "เมษายน", "04")
            .when(trim(col("BIRTHDAY_MONTH")) == "พฤษภาคม", "05")
            .when(trim(col("BIRTHDAY_MONTH")) == "มิถุนายน", "06")
            .when(trim(col("BIRTHDAY_MONTH")) == "กรกฎาคม", "07")
            .when(trim(col("BIRTHDAY_MONTH")) == "สิงหาคม", "08")
            .when(trim(col("BIRTHDAY_MONTH")) == "กันยายน", "09")
            .when(trim(col("BIRTHDAY_MONTH")) == "ตุลาคม", "10")
            .when(trim(col("BIRTHDAY_MONTH")) == "พฤศจิกายน", "11")
            .when(trim(col("BIRTHDAY_MONTH")) == "ธันวาคม", "12")
            .otherwise("UNDEFINED")

        ))
        dataframe=dataframe.select("FIRST_NAME","LAST_NAME","MOBILE_NUMBER","MOBILE_COUNTRY_CODE","BIRTHDAY_MONTH",
                            "BIRTHDAY_YEAR","ADDRESS_1","ADDRESS_2","ADDRESS_CITY","ADDRESS_ZIPCODE","SUBSCRIBER_KEY",
                            "WEBSITE_UNIQUE_ID","SOURCE","MEDIUM","BRAND","ADDRESS_COUNTRY","CAMPAIGN_ID","CREATED_DATE",
                            "UPDATED_DATE","UNSUBSCRIBE_DATE","EMAIL","FULL_NAME","LAST_LOGON_TIME","REMAINING_POINTS",
                            "REDEEMED_POINTS","TOTAL_POINTS","GENDER","LINE_ID","LINE_NAME","LINE_EMAIL","LINE_CHANNEL_ID",
                            "ADDRESS_REGION","TIER","OPT_IN_FOR_COMMUNICATION","HAVE_KID","AGE","FILE_NAME","CRTD_DTTM")

        # Load Data to the target table
        dataframe.write.mode("append").saveAsTable(target_table)

        # write to success folder
        file_name=file_name[0:24]+''_''+datetime.now().strftime("%Y%m%d%H%M%S")

        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        #move to success
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

CREATE OR REPLACE PROCEDURE THASDL_RAW.TH_DMS_INVENTORY_FACT_PREPROCESSING("PARAM" ARRAY)
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
    #Param=["I_KCS2402282300.txt","THASDL_RAW.DEV_LOAD_STAGE_ADLS","dev/GT_Intervention/DnA_VMR","SDL_TH_DMS_INVENTORY_FACT"]
    try:
       file_name       = Param[0]
       stage_name      = Param[1]
       temp_stage_path = Param[2]
       target_table    = Param[3]
       
       df_schema = StructType([
        StructField("recdate", StringType(), nullable=True),
        StructField("distributorid", StringType(), nullable=True),
        StructField("whcode", StringType(), nullable=True),
        StructField("productcode", StringType(), nullable=True),
        StructField("qty", StringType(), nullable=True),
        StructField("amount", StringType(), nullable=True),
        StructField("batchno", StringType(), nullable=True),
        StructField("expirydate", StringType(), nullable=True)
        ])
       df = session.read\\
                    .schema(df_schema)\\
                    .option("skip_header",0)\\
                    .option("field_delimiter", "\\t")\\
                    .option("field_optionally_enclosed_by", "\\"") \\
                    .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
      
       
       df= df.with_column("run_id", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S"))) 
       df=df.with_column("file_name",lit(file_name))
       df=df.with_column("crt_dttm",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))

       snowdf=df.select(     "recdate",
                            "distributorid",
                            "whcode",
                            "productcode",
                            "qty",
                            "amount",
                            "batchno",
                            "expirydate",
                            "crt_dttm",
                            "run_id",
                            "file_name")
            
       snowdf= snowdf.filter(snowdf["distributorid"].isNotNull())
    
       if snowdf.count()==0 :
                return "No Data in table"
                
            
            #move file into success folder
       file_name=file_name.split(".")[0]+"_"+datetime.now().strftime("%Y%m%d%H%M%S")
       snowdf.write.copy_into_location("@"+Param[1]+"/"+Param[2]+"/success/"+file_name,header=True,OVERWRITE=True)
    
            #write on sdl layer
        
       snowdf.write.mode("append").saveAsTable(stage_name.split(".")[0]+"."+target_table)
            
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
CREATE OR REPLACE PROCEDURE THASDL_RAW.TH_GT_MSLD_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS CALLER
AS '

import snowflake.snowpark as snowpark
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DateType
from snowflake.snowpark.functions import col,lit,date_format,current_timestamp,to_timestamp, split, trim, to_date
import pandas as pd
from datetime import datetime
import pytz



def main(session: snowpark.Session, Param): 
    
    try:
        # Extracting parameters from the input

        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        sch_name        = stage_name.split(''.'')[0]
        target_table    = sch_name+"."+Param[3]
        
        # Set the current session schema
        
        session.use_schema(sch_name)

        # Define the schema for the DataFrame
        df_schema=StructType([
            StructField("DC", StringType(100)),
            StructField("RE_Name", StringType(100)),
            StructField("Store_Name", StringType(100)),
            StructField("Sales_Rep_Code", StringType(50)),
            StructField("Sales_Rep", StringType(100)),
            StructField("Category_Code", StringType(50)),
            StructField("Category", StringType(100)),
            StructField("Brand_Code", StringType(50)),
            StructField("Brand", StringType(100)),
            StructField("Barcode", StringType(50)),
            StructField("Product_Description", StringType(100)),
            StructField("Survey_Date", StringType(20)),
            StructField("NoDistribution", StringType(10)),
            StructField("OSA", StringType(10)),
            StructField("OOS", StringType(10)),
            StructField("OOSReason", StringType(10))
            ])
        # Set the current session schema
        
        # Read the CSV file into a DataFrame
        df = session.read\\
        .schema(df_schema)\\
        .option("skip_header",1)\\
        .option("field_delimiter", "|")\\
        .option("field_optionally_enclosed_by", "\\"") \\
        .option("truncatecolumns",True) \\
        .option("skip_blank_lines", True) \\
        .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        
        #convertin time stamp into sg timezone
        df = df.withColumn("CRT_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        df = df.withColumn("FILE_NAME",lit(file_name).cast("string"))
        df = df.withColumn("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        df = df.withColumn("CNTRY_CD",lit("TH"))
        df = df.withColumn("CRNCY_CD",lit("THB"))
        
        # Creating copy of the Dataframe
        final_df = df.select("CNTRY_CD", "CRNCY_CD", \\
            trim(split(col("DC"), lit("-"))[0].cast("string")).alias("DC_Code"), \\
            trim(split(col("DC"), lit("-"))[1].cast("string")).alias("DC_Name"), \\
            trim(split(col("RE_Name"), lit("-"))[0].cast("string")).alias("RE_Code"), \\
            trim(split(col("RE_Name"), lit("-"))[1].cast("string")).alias("RE_Name"), \\
            trim(split(col("Store_Name"), lit("-"))[0].cast("string")).alias("Store_Code"), \\
            trim(split(col("Store_Name"), lit("-"))[1].cast("string")).alias("Store_Name"), \\
            "Sales_Rep_Code", "Sales_Rep", "Category_Code", "Category", "Brand_Code", \\
            "Brand", "Barcode", "Product_Description", to_date("Survey_Date", lit("YYYYMMDD")).as_("Survey_Date"), \\
            "NoDistribution", "OSA", "OOS", "OOSReason", \\
            "FILE_NAME", "RUN_ID", "CRT_DTTM" ).alias("final_df")

        if final_df.count()==0:
            return "No Data in file"
        
        # Load Data to the target table
        final_df.write.mode("append").saveAsTable(target_table)

    
        # write to success folder
    
        file_name=file_name.split(".")[0]+''_''+datetime.now().strftime("%Y%m%d%H%M%S")
		
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
		
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

CREATE OR REPLACE PROCEDURE THASDL_RAW.TH_GT_ROUTE_DTL_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS CALLER
AS '

import snowflake.snowpark as snowpark
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DateType
from snowflake.snowpark.functions import col,lit,date_format,current_timestamp,to_timestamp, md5, coalesce, concat, upper, trim, to_date, when
import pandas as pd
from datetime import datetime
import pytz



def main(session: snowpark.Session, Param): 
    
    try:
        # Extracting parameters from the input

        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        sch_name        = stage_name.split(''.'')[0]
        target_table    = sch_name+"."+Param[3]
        
        # Set the current session schema
        
        session.use_schema(sch_name)

        # Define the schema for the DataFrame
        df_schema=StructType([
            StructField("routeid", StringType(50)),
            StructField("customerid", StringType(50)),
            StructField("routeNo", StringType(50)),
            StructField("saleunit", StringType(50)),
            StructField("SHIP_TO", StringType(50)),
            StructField("CONTACT_PERSON", StringType(100)),
            StructField("Created_date", StringType(50))
            ])
        # Set the current session schema
        
        # Read the CSV file into a DataFrame
        df = session.read\\
        .schema(df_schema)\\
        .option("skip_header",1)\\
        .option("field_delimiter", "|")\\
        .option("field_optionally_enclosed_by", "\\"") \\
        .option("truncatecolumns",True) \\
        .option("skip_blank_lines", True) \\
        .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        filespec,filecode,fileuploadeddate,filedate = file_name.split("_")
        
        #convertin time stamp into sg timezone
        df = df.withColumn("CRT_DTTM", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        df = df.withColumn("FILE_NAME", lit(file_name).cast("string"))
        df = df.withColumn("RUN_ID", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        df = df.withColumn("CNTRY_CD", lit("TH"))
        df = df.withColumn("CRNCY_CD", lit("THB"))
        df = df.withColumn("HASH_KEY", 
            md5(concat( \\
                    coalesce(upper(trim(col("saleunit"))), lit(''N/A'')) , \\
                    coalesce(upper(trim(col("customerid"))), lit(''N/A'')) , \\
                    coalesce(upper(trim(col("routeid"))), lit(''N/A'')), \\
                    coalesce(upper(trim(col("routeno"))), lit(''N/A'')) \\
                ) \\
            ) \\
        )
        
        df = df.withColumn("FILE_UPLOADED_DATE", to_timestamp(lit(fileuploadeddate),"YYYYMMDDHHMISS"))
              
        # Creating copy of the Dataframe
        file_df = df.select("HASH_KEY", "CNTRY_CD", "CRNCY_CD", "routeid", "customerid", \\
            "routeNo", "saleunit", "SHIP_TO", "CONTACT_PERSON", \\
             to_date("Created_date", lit("YYYYMMDD")).as_("Created_date"), \\
            "FILE_NAME", "FILE_UPLOADED_DATE", "RUN_ID", "CRT_DTTM" )
            
        final_df = file_df.select("HASH_KEY", "CNTRY_CD", "CRNCY_CD", "routeid", "customerid", \\
            "routeNo", "saleunit", trim("SHIP_TO").as_("SHIP_TO"), trim("CONTACT_PERSON").as_("CONTACT_PERSON"), \\
             "Created_date", "FILE_NAME", "FILE_UPLOADED_DATE", "RUN_ID", "CRT_DTTM" )

        if final_df.count()==0:
            return "No Data in file"
        
        # Load Data to the target table
        final_df.write.mode("append").saveAsTable(target_table)

    
        # write to success folder
        
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
		
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

CREATE OR REPLACE PROCEDURE THASDL_RAW.TH_GT_ROUTE_HDR_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS CALLER
AS '

import snowflake.snowpark as snowpark
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DateType
from snowflake.snowpark.functions import col,lit,date_format,current_timestamp,to_timestamp, md5, coalesce, concat, upper, trim, to_date
import pandas as pd
from datetime import datetime
import pytz



def main(session: snowpark.Session, Param): 
    
    try:
        # Extracting parameters from the input

        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        sch_name        = stage_name.split(''.'')[0]
        target_table    = sch_name+"."+Param[3]
        
        # Set the current session schema
        
        session.use_schema(sch_name)

        # Define the schema for the DataFrame
        df_schema=StructType([
            StructField("id", StringType(50)),
            StructField("name", StringType(100)),
            StructField("desc", StringType(100)),
            StructField("is_active", StringType(10)),
            StructField("routesale", StringType(50)),
            StructField("saleunit", StringType(50)),
            StructField("route_code", StringType(50)),
            StructField("description", StringType(100)),
            StructField("Last_Updated_date", StringType(20))
            ])
        # Set the current session schema
        
        # Read the CSV file into a DataFrame
        df = session.read\\
        .schema(df_schema)\\
        .option("skip_header",1)\\
        .option("field_delimiter", "|")\\
        .option("field_optionally_enclosed_by", "\\"") \\
        .option("truncatecolumns",True) \\
        .option("skip_blank_lines", True) \\
        .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        filespec,filecode,uploadeddate,filedate = file_name.split("_")
        
        #convertin time stamp into sg timezone
        df = df.withColumn("CRT_DTTM", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        df = df.withColumn("FILE_NAME", lit(file_name).cast("string"))
        df = df.withColumn("RUN_ID", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        df = df.withColumn("CNTRY_CD", lit("TH"))
        df = df.withColumn("CRNCY_CD", lit("THB"))
        df = df.withColumn("HASH_KEY", 
            md5(concat( \\
                    coalesce(upper(trim(col("saleunit"))), lit(''N/A'')) , \\
                    coalesce(upper(trim(col("routesale"))), lit(''N/A'')) , \\
                    coalesce(upper(trim(col("id"))), lit(''N/A'')) \\
                ) \\
            ) \\
        )
 
        df = df.withColumn("FILE_UPLOADED_DATE", to_timestamp(lit(uploadeddate),"YYYYMMDDHHMISS"))
            
        
        # Creating copy of the Dataframe
        final_df = df.select("HASH_KEY", "CNTRY_CD", "CRNCY_CD", "id", "name", "desc", \\
            "is_active", "routesale", "saleunit", "route_code", "description", to_date("Last_Updated_date", lit("YYYYMMDD")).as_("Last_Updated_date"), \\
            "FILE_NAME", "FILE_UPLOADED_DATE", "RUN_ID", "CRT_DTTM" ).alias("final_df")

        if final_df.count()==0:
            return "No Data in file"
        
        # Load Data to the target table
        final_df.write.mode("append").saveAsTable(target_table)

    
        # write to success folder
		
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
    
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

CREATE OR REPLACE PROCEDURE THASDL_RAW.TH_GT_SALES_ORDER_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS CALLER
AS '

import snowflake.snowpark as snowpark
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DateType
from snowflake.snowpark.functions import col,lit,date_format,current_timestamp,to_timestamp, md5, coalesce, concat, upper, trim, to_date
import pandas as pd
from datetime import datetime
import pytz



def main(session: snowpark.Session, Param): 
    
    try:
        # Extracting parameters from the input

        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        sch_name        = stage_name.split(''.'')[0]
        target_table    = sch_name+"."+Param[3]
        
        # Set the current session schema
        
        session.use_schema(sch_name)

        # Define the schema for the DataFrame
        df_schema=StructType([
            StructField("SaleUnit", StringType(50)),
            StructField("OrderID", StringType(50)),
            StructField("orderdate", StringType(50)),
            StructField("Customer_Id", StringType(50)),
            StructField("Customer_Name", StringType(100)),
            StructField("City", StringType(50)),
            StructField("Region", StringType(50)),
            StructField("SaleDistrict", StringType(50)),
            StructField("SaleOffice", StringType(50)),
            StructField("SaleGroup", StringType(50)),
            StructField("CustomerType", StringType(50)),
            StructField("StoreType", StringType(50)),
            StructField("SaleType", StringType(50)),
            StructField("SalesEmployee", StringType(50)),
            StructField("SaleName", StringType(100)),
            StructField("ProductID", StringType(50)),
            StructField("ProductName", StringType(100)),
            StructField("MegaBrand", StringType(50)),
            StructField("Brand", StringType(50)),
            StructField("BaseProduct", StringType(50)),
            StructField("Variant", StringType(50)),
            StructField("Putup", StringType(50)),
            StructField("PriceRef", StringType(50)),
            StructField("Backlog", StringType(50)),
            StructField("Qty", StringType(50)),
            StructField("SubAmt1", StringType(50)),
            StructField("Discount", StringType(50)),
            StructField("SubAmt2", StringType(50)),
            StructField("DiscountBTLine", StringType(50)),
            StructField("TotalBeforeVat", StringType(50)),
            StructField("Total", StringType(50)),
            StructField("No", StringType(50)),
            StructField("Canceled", StringType(50)),
            StructField("DocumentID", StringType(50)),
            StructField("RETURN_REASON", StringType(100)),
            StructField("PromotionCode", StringType(50)),
            StructField("PromotionCode1", StringType(50)),
            StructField("PromotionCode2", StringType(50)),
            StructField("PromotionCode3", StringType(50)),
            StructField("PromotionCode4", StringType(50)),
            StructField("PromotionCode5", StringType(50)),
            StructField("Promotion_Code", StringType(50)),
            StructField("Promotion_Code2", StringType(50)),
            StructField("Promotion_Code3", StringType(50)),
            StructField("AvgDiscount", StringType(50)),
            StructField("ORDERTYPE", StringType(10)),
            StructField("ApproverStatus", StringType(10)),
            StructField("PRICELEVEL", StringType(10)),
            StructField("OPTIONAL3", StringType(50)),
            StructField("DELIVERYDATE", StringType(50)),
            StructField("OrderTime", StringType(50)),
            StructField("SHIPTO", StringType(50)),
            StructField("BILLTO", StringType(50)),
            StructField("DeliveryRouteID", StringType(50)),
            StructField("APPROVED_DATE", StringType(50)),
            StructField("APPROVED_TIME", StringType(50)),
            StructField("REF_15", StringType(50)),
            StructField("PaymentType", StringType(50))
            ])
        # Set the current session schema
        
        # Read the CSV file into a DataFrame
        df = session.read\\
        .schema(df_schema)\\
        .option("skip_header",1)\\
        .option("field_delimiter", "|")\\
        .option("field_optionally_enclosed_by", "\\"") \\
        .option("truncatecolumns",True) \\
        .option("skip_blank_lines", True) \\
        .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        
        #convertin time stamp into sg timezone
        df = df.withColumn("CRT_DTTM", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        df = df.withColumn("FILE_NAME", lit(file_name).cast("string"))
        df = df.withColumn("RUN_ID", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        df = df.withColumn("CNTRY_CD", lit("TH"))
        df = df.withColumn("CRNCY_CD", lit("THB"))
        df = df.withColumn("order_date", to_date("orderdate", lit("YYYY/MM/DD")))
        df = df.withColumn("HASH_KEY", 
            md5(concat( \\
                    coalesce(upper(trim(col("SaleUnit"))), lit(''N/A'')) , \\
                    coalesce(upper(trim(col("OrderID"))), lit(''N/A'')) , \\
                    coalesce(upper(trim(col("order_date"))), lit(''9999-12-31'')), \\
                    coalesce(upper(trim(col("ProductID"))), lit(''N/A'')), \\
                    coalesce(upper(trim(col("Customer_Id"))), lit(''N/A'')), \\
                    coalesce(upper(trim(col("No"))), lit(''N/A'')) \\
                ) \\
            ) \\
        )

        # Creating copy of the Dataframe
        final_df = df.select("HASH_KEY", "CNTRY_CD", "CRNCY_CD", "SaleUnit", "OrderID", \\
            "order_date", "Customer_Id", \\
            "Customer_Name", "City", "Region", "SaleDistrict", "SaleOffice", "SaleGroup", "CustomerType", \\
            "StoreType", "SaleType", "SalesEmployee", "SaleName", "ProductID", "ProductName", "MegaBrand", \\
            "Brand", "BaseProduct", "Variant", "Putup", "PriceRef", "Backlog", "Qty", "SubAmt1", "Discount", \\
            "SubAmt2", "DiscountBTLine", "TotalBeforeVat", "Total", "No", "Canceled", "DocumentID", "RETURN_REASON", \\
            "PromotionCode", "PromotionCode1", "PromotionCode2", "PromotionCode3", "PromotionCode4", \\
            "PromotionCode5", "Promotion_Code", "Promotion_Code2", "Promotion_Code3", "AvgDiscount", "ORDERTYPE", \\
            "ApproverStatus", "PRICELEVEL", to_date("OPTIONAL3", lit("YYYYMMDD")).as_("OPTIONAL3"), \\
            to_date("DELIVERYDATE", lit("YYYYMMDD")).as_("DELIVERYDATE"), "OrderTime", "SHIPTO", "BILLTO", \\
            "DeliveryRouteID", to_date("APPROVED_DATE", lit("YYYYMMDD")).as_("APPROVED_DATE"), "APPROVED_TIME", \\
            "REF_15", "PaymentType", "FILE_NAME", "RUN_ID", "CRT_DTTM" ).alias("final_df")

        if final_df.count()==0:
            return "No Data in file"
        
        # Load Data to the target table
        final_df.write.mode("append").saveAsTable(target_table)

    
        # write to success folder
    
        file_name=file_name.split(".")[0]+''_''+datetime.now().strftime("%Y%m%d%H%M%S")
		
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
		
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

CREATE OR REPLACE PROCEDURE THASDL_RAW.TH_GT_SCHEDULE_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS CALLER
AS '

import snowflake.snowpark as snowpark
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DateType
from snowflake.snowpark.functions import col,lit,date_format,current_timestamp,to_timestamp, to_date
import pandas as pd
from datetime import datetime
import pytz



def main(session: snowpark.Session, Param): 
    
    try:
        # Extracting parameters from the input

        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        sch_name        = stage_name.split(''.'')[0]
        target_table    = sch_name+"."+Param[3]
        
        # Set the current session schema
        
        session.use_schema(sch_name)

        # Define the schema for the DataFrame
        df_schema=StructType([
            StructField("employeeid",StringType(50)),
            StructField("routeid",StringType(50)),
            StructField("date",StringType(20)),
            StructField("approved",StringType(10)),
            StructField("saleunit",StringType(50))
            ])
        # Set the current session schema
        
        # Read the CSV file into a DataFrame
        df = session.read\\
        .schema(df_schema)\\
        .option("skip_header",1)\\
        .option("field_delimiter", "|")\\
        .option("field_optionally_enclosed_by", "\\"") \\
        .option("truncatecolumns",True) \\
        .option("skip_blank_lines", True) \\
        .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        
        #convertin time stamp into sg timezone
        df = df.withColumn("CRT_DTTM", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        df = df.withColumn("FILE_NAME", lit(file_name).cast("string"))
        df = df.withColumn("RUN_ID", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        df = df.withColumn("CNTRY_CD", lit("TH"))
        df = df.withColumn("CRNCY_CD", lit("THB"))
        
        # Creating copy of the Dataframe
        final_df = df.select("CNTRY_CD", "CRNCY_CD", "employeeid", "routeid", to_date("date", lit("YYYYMMDD")).as_("date"), "approved", "saleunit", "FILE_NAME", "RUN_ID", "CRT_DTTM").alias("final_df")

        if final_df.count()==0:
            return "No Data in file"
        
        # Load Data to the target table
        final_df.write.mode("append").saveAsTable(target_table)

    
        # write to success folder
    
        file_name=file_name.split(".")[0]+''_''+datetime.now().strftime("%Y%m%d%H%M%S")
		
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
		
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

CREATE OR REPLACE PROCEDURE THASDL_RAW.TH_GT_VISIT_PREPROCESSING("PARAM" ARRAY)
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
from snowflake.snowpark.functions import col,lit,to_date,trim,date_format
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField,DecimalType,DateType
import pandas as pd
from datetime import datetime
import pytz

def main(session: snowpark.Session,Param):

    
    try:

        # Parameters for consumerreach_cvs
        #Param=[''Visit_20240306223006_20240307013015.txt'',''THASDL_RAW.DEV_LOAD_STAGE_ADLS'',''dev/GT_Intervention/DnA_VMR/cert-data-lake/Visit/TH_GT_VISIT'',''SDL_TH_GT_VISIT'']
        
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
    
        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "|")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)


        #---------------------------Transformation logic ------------------------------#
        
        #Check if the Dataframe is having Data
        

        if df.count()==0:
            return "No Data in file"
        

        df= df.filter(df["date_plan"].isNotNull())
        

        #transform columns

        df = df.withColumn("CNTRY_CD", lit("TH"))
        df = df.withColumn("CRNCY_CD", lit("THB"))
        df = df.with_column("id_sale",trim(col("id_sale")))
        df = df.with_column("sale_name",trim(col("sale_name")))
        df = df.with_column("id_customer",trim(col("id_customer")))
        df = df.with_column("customer_name",trim(col("customer_name")))
        df = df.with_column("date_plan",trim(col("date_plan")))
        df = df.with_column("time_plan",trim(col("time_plan")))
        df = df.with_column("date_visi",trim(col("date_visi")))
        df = df.with_column("time_visi",trim(col("time_visi")))
        df = df.with_column("object",trim(col("object")))
        df = df.with_column("visit_end",trim(col("visit_end")))
        df = df.with_column("visit_time",trim(col("visit_time")))
        df = df.with_column("regioncode",trim(col("regioncode")))
        df = df.with_column("areacode",trim(col("areacode")))
        df = df.with_column("branchcode",trim(col("branchcode")))
        df = df.with_column("saleunit",trim(col("saleunit")))
        df = df.with_column("time_survey_in",trim(col("time_survey_in")))
        df = df.with_column("time_survey_out",trim(col("time_survey_out")))
        df = df.with_column("count_survey",trim(col("count_survey")))
        df= df.with_column("run_id", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S"))) 
        df=df.with_column("filename",lit(file_name))
        df=df.with_column("crt_dttm",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))

        

        final_df = df.select("CNTRY_CD","CRNCY_CD","id_sale","sale_name","id_customer","customer_name",to_date("date_plan", lit("YYYYMMDD")).as_("date_plan"),"time_plan",to_date("date_visi", lit("YYYYMMDD")).as_("date_visi"),"time_visi","object",to_date("visit_end", lit("YYYYMMDD")).as_("visit_end"),"visit_time","regioncode","areacode","branchcode","saleunit","time_survey_in","time_survey_out","count_survey","filename","run_id","crt_dttm")

       
         #Delete existing Data for the current file
        
        del_sql = "DELETE FROM " + target_table + " WHERE filename ="+"''" + (file_name)+"''"
        session.sql(del_sql).collect()
        
        # Load Data to the target table
        final_df.write.mode("append").saveAsTable(stage_name.split(".")[0]+"."+target_table)

        # write to success folder
    
        file_name=file_name.split(".")[0]+''_''+datetime.now().strftime("%Y%m%d%H%M%S")
		
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")

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

CREATE OR REPLACE PROCEDURE TH_MBOX_CUSTOMER_PREPROCESSING("PARAM" ARRAY)
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
from snowflake.snowpark.functions import col,lit
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField,DecimalType,DateType
import pandas as pd
from datetime import datetime
import pytz

def main(session: snowpark.Session,Param):

    
    try:

        # Parameters for consumerreach_cvs
        #Param=[''A1_SPC2403042250_20240305023320.txt'',''THASDL_RAW.DEV_LOAD_STAGE_ADLS'',''dev/GT_Intervention/DnA_VMR/cert-data-lake/CustomerDim/TH_GT_CUSTOMER/'',''sdl_th_dms_chana_customer_dim'']
        
        # Extracting parameters from the input
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]

        # Define the schema for the DataFrame
        df_schema = StructType([
            StructField("distributorid", StringType()),
            StructField("arcode", StringType()),
            StructField("arname", StringType()),
            StructField("araddress", StringType()),
            StructField("telephone", StringType()),
            StructField("fax", StringType()),
            StructField("city", StringType()),
            StructField("region", StringType()),
            StructField("saledistrict", StringType()),
            StructField("saleoffice", StringType()),
            StructField("salegroup", StringType()),
            StructField("artypecode", StringType()),
            StructField("saleemployee", StringType()),
            StructField("salename", StringType()),
            StructField("billno", StringType()),
            StructField("billmoo", StringType()),
            StructField("billsoi", StringType()),
            StructField("billroad", StringType()),
            StructField("billsubdist", StringType()),
            StructField("billdistrict", StringType()),
            StructField("billprovince", StringType()),
            StructField("billzipcode", StringType()),
            StructField("activestatus", StringType()),
            StructField("routestep1", StringType()),
            StructField("routestep2", StringType()),
            StructField("routestep3", StringType()),
            StructField("routestep4", StringType()),
            StructField("routestep5", StringType()),
            StructField("routestep6", StringType()),
            StructField("routestep7", StringType()),
            StructField("routestep8", StringType()),
            StructField("routestep9", StringType()),
            StructField("routestep10", StringType()),
            StructField("store", StringType()),
            StructField("pricelevel", StringType()),
            StructField("salesarename", StringType()),
            StructField("branchcode", StringType()),
            StructField("branchname", StringType()),
            StructField("frequencyofvisit", StringType())
            ])

        
        # Read the CSV file into a DataFrame
    
        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",0)\\
            .option("field_delimiter", "\\t")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)


        #---------------------------Transformation logic ------------------------------#

        # Check if the Dataframe is having Data
        df = df.na.drop("all")
        
        if df.count()==0:
            return "No Data in file"
		
		
        # Add RUN_ID, FILE NAME and YEARMO columns  
        df= df.with_column("run_id", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S"))) 
        df=df.with_column("filename",lit(file_name))
        df=df.with_column("crt_dttm",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))

        

        
        # Creating Final Dataframe
        final_df = df.select("distributorid","arcode","arname","araddress","telephone","fax","city","region","saledistrict","saleoffice","salegroup","artypecode","saleemployee","salename","billno","billmoo","billsoi","billroad","billsubdist","billdistrict","billprovince","billzipcode","activestatus","routestep1","routestep2","routestep3","routestep4","routestep5","routestep6","routestep7","routestep8","routestep9","routestep10","store","pricelevel","salesarename","branchcode","frequencyofvisit","filename","run_id","crt_dttm")


        #Delete existing Data for the current file
        
        del_sql = "DELETE FROM " + target_table + " WHERE filename ="+"''" + (file_name)+"''"
        session.sql(del_sql).collect()
        
        # Load Data to the target table
        final_df.write.mode("append").saveAsTable(stage_name.split(".")[0]+"."+target_table)

        # write to success folder
    
        file_name=file_name.split(".")[0]+''_''+datetime.now().strftime("%Y%m%d%H%M%S")
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        
        #move to success
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
CREATE OR REPLACE PROCEDURE TH_MBOX_INVENTORY_PREPROCESSING("PARAM" ARRAY)
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
    #Param=["I_KCS2402282300.txt","THASDL_RAW.DEV_LOAD_STAGE_ADLS","dev/GT_Intervention/DnA_VMR/cert-data-lake/dms_source/processed_file","SDL_TH_DMS_INVENTORY_FACT"]
    try:
       file_name       = Param[0]
       stage_name      = Param[1]
       temp_stage_path = Param[2]
       target_table    = Param[3]
       
       df_schema = StructType([
        StructField("recdate", StringType(), nullable=True),
        StructField("distributorid", StringType(), nullable=True),
        StructField("whcode", StringType(), nullable=True),
        StructField("productcode", StringType(), nullable=True),
        StructField("qty", StringType(), nullable=True),
        StructField("amount", StringType(), nullable=True),
        StructField("batchno", StringType(), nullable=True),
        StructField("expirydate", StringType(), nullable=True)
        ])
       df = session.read\\
                    .schema(df_schema)\\
                    .option("skip_header",0)\\
                    .option("field_delimiter", "\\t")\\
                    .option("field_optionally_enclosed_by", "\\"") \\
                    .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
        
       df=df.na.drop("all")
       if df.count()==0 :
           return "No Data in file"
       
       df= df.with_column("run_id", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S"))) 
       df=df.with_column("file_name",lit(file_name))
       df=df.with_column("crt_dttm",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))

       snowdf=df.select(    "recdate",
                            "distributorid",
                            "whcode",
                            "productcode",
                            "qty",
                            "amount",
                            "batchno",
                            "expirydate",
                            "crt_dttm",
                            "run_id",
                            "file_name")
            
       #snowdf= snowdf.filter(snowdf["distributorid"].isNotNull())
    
       #if snowdf.count()==0 :
                #return "No Data in table"
                
            
            #move file into success folder
       file_name=file_name.split(".")[0]+"_"+datetime.now().strftime("%Y%m%d%H%M%S")
       
       current_date = datetime.now()
       formatted_year = current_date.strftime("%Y")
       formatted_month = current_date.strftime("%m")
        
        #move to success
       snowdf.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)
    
            #write on sdl layer
        
       snowdf.write.mode("append").saveAsTable(stage_name.split(".")[0]+"."+target_table)
            
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
CREATE OR REPLACE PROCEDURE TH_MBOX_SALETOOL_CUSTOMER_PREPROCESSING("PARAM" ARRAY)
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
    #Param=["A1_MPP2403052250_20240306023142.txt","THASDL_RAW.DEV_LOAD_STAGE_ADLS","dev/GT_Intervention/DnA_VMR/cert-data-lake/dms_source/processed_file","SDL_TH_DMS_CUSTOMER_DIM"]
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        df_schema = StructType([
            StructField("distributorid", StringType(), nullable=True),
            StructField("arcode", StringType(), nullable=True),
            StructField("arname", StringType(), nullable=True),
            StructField("araddress", StringType(), nullable=True),
            StructField("telephone", StringType(), nullable=True),
            StructField("fax", StringType(), nullable=True),
            StructField("city", StringType(), nullable=True),
            StructField("region", StringType(), nullable=True),
            StructField("saledistrict", StringType(), nullable=True),
            StructField("saleoffice", StringType(), nullable=True),
            StructField("salegroup", StringType(), nullable=True),
            StructField("artypecode", StringType(), nullable=True),
            StructField("saleemployee", StringType(), nullable=True),
            StructField("salename", StringType(), nullable=True),
            StructField("billno", StringType(), nullable=True),
            StructField("billmoo", StringType(), nullable=True),
            StructField("billsoi", StringType(), nullable=True),
            StructField("billroad", StringType(), nullable=True),
            StructField("billsubdist", StringType(), nullable=True),
            StructField("billdistrict", StringType(), nullable=True),
            StructField("billprovince", StringType(), nullable=True),
            StructField("billzipcode", StringType(), nullable=True),
            StructField("activestatus", StringType(), nullable=True),
            StructField("routestep1", StringType(), nullable=True),
            StructField("routestep2", StringType(), nullable=True),
            StructField("routestep3", StringType(), nullable=True),
            StructField("routestep4", StringType(), nullable=True),
            StructField("routestep5", StringType(), nullable=True),
            StructField("routestep6", StringType(), nullable=True),
            StructField("routestep7", StringType(), nullable=True),
            StructField("routestep8", StringType(), nullable=True),
            StructField("routestep9", StringType(), nullable=True),
            StructField("routestep10", StringType(), nullable=True),
            StructField("store", StringType(), nullable=True),
            StructField("sourcefile", StringType(), nullable=True),
            StructField("old_custid", StringType(), nullable=True),
            StructField("modifydate", StringType(), nullable=True)
             ])
        df = session.read\\
                    .schema(df_schema)\\
                    .option("skip_header",0)\\
                    .option("field_delimiter", "\\t")\\
                    .option("field_optionally_enclosed_by", "\\"") \\
                    .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
        
        df=df.na.drop("all")
        if df.count()==0 :
            return "No Data in file"


        df= df.with_column("run_id", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S"))) 
        df=df.with_column("file_name",lit(file_name))
        df=df.with_column("curr_date",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))

        snowdf=df.select(   "distributorid", "arcode", "arname", "araddress", "telephone", "fax", "city", "region",
                            "saledistrict", "saleoffice", "salegroup", "artypecode", "saleemployee", "salename",
                            "billno", "billmoo", "billsoi", "billroad", "billsubdist", "billdistrict", "billprovince",
                            "billzipcode", "activestatus", "routestep1", "routestep2", "routestep3", "routestep4",
                            "routestep5", "routestep6", "routestep7", "routestep8", "routestep9", "routestep10",
                            "store", "sourcefile", "old_custid", "modifydate", "curr_date", "run_id", "file_name"
                        )
                                    
        #snowdf= snowdf.filter(snowdf["distributorid"].isNotNull())
    
        #if snowdf.count()==0 :
                #return "No Data in table"
                
            
            #move file into success folder
        file_name=file_name.split(".")[0]+"_"+datetime.now().strftime("%Y%m%d%H%M%S")
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        
        #move to success
        snowdf.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)
    
            #write on sdl layer
        
        snowdf.write.mode("append").saveAsTable(stage_name.split(".")[0]+"."+target_table)
            
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
CREATE OR REPLACE PROCEDURE TH_MBOX_SELLOUT_PREPROCESSING("PARAM" ARRAY)
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
    #Param=[''S_rev2208312255_20220901015227_rev.csv'',''THASDL_RAW.DEV_LOAD_STAGE_ADLS'',''dev/GT_Intervention/DnA_VMR/cert-data-lake/dms_source/processed_file'',''SDL_TH_DMS_sellout_FACT'']
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        
        df_schema = StructType([
            StructField("distributorid", StringType(), nullable=True),
            StructField("orderno", StringType(), nullable=True),
            StructField("orderdate", StringType(), nullable=True),
            StructField("arcode", StringType(), nullable=True),
            StructField("arname", StringType(), nullable=True),
            StructField("city", StringType(), nullable=True),
            StructField("region", StringType(), nullable=True),
            StructField("saledistrict", StringType(), nullable=True),
            StructField("saleoffice", StringType(), nullable=True),
            StructField("salegroup", StringType(), nullable=True),
            StructField("artypecode", StringType(), nullable=True),
            StructField("saleemployee", StringType(), nullable=True),
            StructField("salename", StringType(), nullable=True),
            StructField("productcode", StringType(), nullable=True),
            StructField("productdesc", StringType(), nullable=True),
            StructField("megabrand", StringType(), nullable=True),
            StructField("brand", StringType(), nullable=True),
            StructField("baseproduct", StringType(), nullable=True),
            StructField("variant", StringType(), nullable=True),
            StructField("putup", StringType(), nullable=True),
            StructField("grossprice", StringType(), nullable=True),
            StructField("qty", StringType(), nullable=True),
            StructField("subamt1", StringType(), nullable=True),
            StructField("discount", StringType(), nullable=True),
            StructField("subamt2", StringType(), nullable=True),
            StructField("discountbtline", StringType(), nullable=True),
            StructField("totalbeforevat", StringType(), nullable=True),
            StructField("total", StringType(), nullable=True),
            StructField("linenumber", StringType(), nullable=True),
            StructField("iscancel", StringType(), nullable=True),
            StructField("cndocno", StringType(), nullable=True),
            StructField("cnreasoncode", StringType(), nullable=True),
            StructField("promotionheader1", StringType(), nullable=True),
            StructField("promotionheader2", StringType(), nullable=True),
            StructField("promotionheader3", StringType(), nullable=True),
            StructField("promodesc1", StringType(), nullable=True),
            StructField("promodesc2", StringType(), nullable=True),
            StructField("promodesc3", StringType(), nullable=True),
            StructField("promocode1", StringType(), nullable=True),
            StructField("promocode2", StringType(), nullable=True),
            StructField("promocode3", StringType(), nullable=True),
            StructField("avgdiscount", StringType(), nullable=True)
        ])

        df = session.read\\
                    .schema(df_schema)\\
                    .option("skip_header",0)\\
                    .option("field_delimiter", "\\t")\\
                    .option("field_optionally_enclosed_by", "\\"") \\
                    .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
        
        df=df.na.drop("all")
        if df.count()==0 :
            return "No Data in file"
        


        df= df.with_column("run_id", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S"))) 
        df=df.with_column("file_name",lit(file_name.split(".")[0]+".csv"))
        df=df.with_column("curr_date",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))

        snowdf=df.select(    "distributorid", "orderno", "orderdate", "arcode", "arname", "city", "region", 
                            "saledistrict", "saleoffice", "salegroup", "artypecode", "saleemployee", "salename", 
                            "productcode", "productdesc", "megabrand", "brand", "baseproduct", "variant", "putup", 
                            "grossprice", "qty", "subamt1", "discount", "subamt2", "discountbtline", "totalbeforevat", 
                            "total", "linenumber", "iscancel", "cndocno", "cnreasoncode", "promotionheader1", 
                            "promotionheader2", "promotionheader3", "promodesc1", "promodesc2", "promodesc3", 
                            "promocode1", "promocode2", "promocode3", "avgdiscount", "curr_date", "run_id", "file_name")
        
            
        
            
            #move file into success folder
        file_name=file_name.split(".")[0]+"_"+datetime.now().strftime("%Y%m%d%H%M%S")
        
    
            #write on sdl layer
        
        snowdf.write.mode("append").saveAsTable(stage_name.split(".")[0]+"."+target_table)
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        
        #move to success
        snowdf.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)
            
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
