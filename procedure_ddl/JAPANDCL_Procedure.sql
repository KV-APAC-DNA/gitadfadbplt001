CREATE OR REPLACE FILE FORMAT FILE_FORMAT_COMMA
	TYPE = csv
	SKIP_HEADER = 1
	NULL_IF = ('')
	ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
	EMPTY_FIELD_AS_NULL = FALSE
	ENCODING = 'UTF-8'
;

CREATE OR REPLACE FILE FORMAT FILE_FORMAT_QUOTE
	TYPE = csv
	SKIP_HEADER = 1
	FIELD_OPTIONALLY_ENCLOSED_BY = '\"'
	NULL_IF = ('')
	ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
	EMPTY_FIELD_AS_NULL = FALSE
	ENCODING = 'UTF-8'
;

CREATE OR REPLACE FILE FORMAT JPDCLSDL_RAW.FILE_FORMAT_SOH
	TYPE = csv
	FIELD_DELIMITER = '\u0001'
	SKIP_HEADER = 1
	NULL_IF = ('')
	ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
	EMPTY_FIELD_AS_NULL = FALSE
	ENCODING = 'UTF-8'
;


CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.JPDCLSDL_RAW.CINEXT_C_TBDMSNDHIST_PREPROCESSING("PARAM" ARRAY)
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
from snowflake.snowpark.functions import col,lit,trim, substring,to_timestamp,when
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField,DecimalType
import pandas as pd
from datetime import datetime
import pytz

def main(session: snowpark.Session,Param): 

    
    try:

        #Param=[''c_tbDMSndHist_20240610.csv'',''JPDCLSDL_RAW.DEV_LOAD_STAGE_ADLS_JPDCL'',''dev/DCL/jjrnki/c_tbdmsndhist/'',''C_TBDMSNDHIST'']

        # Extracting parameters from the input
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        sch_name        = stage_name.split(''.'')[0]
        target_table    = sch_name+"."+Param[3]


        # Define the schema for the DataFrame
        df_schema=StructType([
            StructField("C_DISENDID",StringType()),
            StructField("C_DIUSRID",StringType()),
            StructField("C_DSDMNUMBER",StringType()),
            StructField("C_DSDMSENDKUBUN",StringType()),
            StructField("C_DSDMSENDDATE",StringType()),
            StructField("C_DSDMNAME",StringType()),
            StructField("C_DSEXTENSION1",StringType()),
            StructField("C_DSEXTENSION2",StringType()),
            StructField("C_DSEXTENSION3",StringType()),
            StructField("C_DSEXTENSION4",StringType()),
            StructField("C_DSEXTENSION5",StringType()),
            StructField("C_DSDMIMPORTID",StringType()),
            StructField("DSPREP",StringType()),
            StructField("DSREN",StringType()),
            StructField("DIPREPUSR",StringType()),
            StructField("DIRENUSR",StringType()),
            StructField("DIELIMFLG",StringType()),
            StructField("DSELIM",StringType()),
            StructField("DIELIMUSR",StringType()),
            StructField("C_DIUSRCHANELID",StringType())
            ])


        # Read the CSV file into a DataFrame
    
        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "\\u0001")\\
            .option("encoding","UTF-8")\\
            .option("skip_blank_lines", True)\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        #---------------------------Transformation logic ------------------------------#
        

        # Handle null values or empty rows
        df=df.na.drop("all")

        # Check for empty Dataframe
        if df.count()==0:
            return "No Data in file"

        columns = [''DSELIM'', ''DIELIMUSR'']
        
        df = df.withColumn(''C_DSDMIMPORTID'', when(col(''C_DSDMIMPORTID'') == '''', None).otherwise(col(''C_DSDMIMPORTID'')))
        df = df.withColumn(''DSELIM'', when(col(''DSELIM'') == '''', None).otherwise(col(''DSELIM'')))
        df = df.withColumn(''DIELIMUSR'', when(col(''DIELIMUSR'') == '''', None).otherwise(col(''DIELIMUSR'')))

        column_names = [field.name for field in df_schema.fields]

        dataframe=df.select(column_names)

        # ADD columns 

        date=file_name.split("_")[-1].split(".")[0]
        file_date=date[0:8]
        type="ETL_Batch"
        dataframe = dataframe.with_column("source_file_date",lit(file_date))
        dataframe = dataframe.with_column("inserted_date",lit(datetime.now(pytz.timezone("GMT")).strftime("%Y-%m-%d %H:%M:%S")))
        dataframe = dataframe.with_column("inserted_by",lit(type))
        dataframe = dataframe.with_column("updated_date",lit(datetime.now(pytz.timezone("GMT")).strftime("%Y-%m-%d %H:%M:%S")))
        dataframe = dataframe.with_column("UPDATED_BY",lit(''''))
        dataframe = dataframe.with_column("ROWID",lit(''''))

        # Creating Final Dataframe
        final_df = dataframe.alias("final_df")

        # Load Data to the target table
        
        final_df.write.mode("append").saveAsTable(target_table)
        

        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")

         # write to success folder
    
        file_name=file_name.split(".")[0]+''_''+datetime.now().strftime("%Y%m%d%H%M%S")
        final_df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True,file_format_name=''FILE_FORMAT_COMMA'')
        
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



CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.JPDCLSDL_RAW.EXTRACTION_TABLE_PREPROCESSING("PARAM" ARRAY)
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
    # Param=["extraction_table_ADD_20240605.csv","JPDCLSDL_RAW.DEV_LOAD_STAGE_ADLS_JPDCL","dev/DCL/jjrnki/extraction_table/","EXTRACTION_TABLE"]
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        
        df_schema = StructType([
                # StructField("mnth_id",StringType(), True),
                StructField("FROM_DATE",StringType(), True),
                StructField("TO_DATE",StringType(), True),
                # StructField("PROCESSED_FLAG",StringType(), True),
                StructField("ITEMID",StringType(), True)

                    
                                                       
            ])
        
        df = session.read\\
            .schema(df_schema)\\
            .option("encoding", "UTF-8")\\
            .option("skip_header",1)\\
            .option("field_delimiter", ",")\\
            .option("skip_blank_lines", True)\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
            # .option("field_optionally_enclosed_by", "\\"")\\
            # .option("field_delimiter",",")\\

        
        
        df=df.na.drop("all")
        if df.count()==0 :
            return "No Data in file"

        
        # df = df.with_column("cdl_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        file_name1=file_name.split(".")[0]+"_"+ datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")+".csv"
        df = df.with_column("file_name",lit(file_name1))
        df = df.with_column("source_file_date",lit(file_name.split("_")[3].split(".")[0]))
        df = df.with_column("crtd_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        # df = df.with_column("mnth_id", file_name.split("_")[1])
        df = df.with_column("inserted_date", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        df = df.with_column("updated_date", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        df = df.with_column("inserted_by", lit(""))
        df = df.with_column("yearmonth", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        df = df.with_column("updated_by", lit(""))
        df = df.with_column("PROCESSED_FLAG",lit("N"))

        
        # date=file_name.split("_")[3].split(".")[0]
        # year=date[0:4]
        # mnth_no=date[4:6]
        # inv_week=date[6:]
        # df = df.withColumn("year", lit(year))
        # df = df.with_column("mnth_no", lit(mnth_no))
        # df = df.with_column("inv_week", lit(inv_week))  
        
        snowdf= df.select(
                    # "mnth_id",
                    ''FROM_DATE'',
                    ''TO_DATE'',
                    ''PROCESSED_FLAG'',
                    ''ITEMID'',
                    ''SOURCE_FILE_DATE'',
                    ''INSERTED_DATE'',
                    ''INSERTED_BY'',
                    ''UPDATED_DATE'',
                    ''UPDATED_BY''        
                    )
        file_name=file_name.split(".")[0]+"_"+datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")

        # print(snowdf)
        
        #move to success
        
        snowdf.write.mode("append").saveAsTable(stage_name.split(".")[0] + "." + target_table)
        snowdf.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True,file_format_name=''FILE_FORMAT_COMMA_1'')
        
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


CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.JPDCLSDL_RAW.KR_SPECIAL_DISCOUNT_PREPROCESSING("PARAM" ARRAY)
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
    # Param=["kr_special_discount_file_ADD_20240610.csv","JPDCLSDL_RAW.DEV_LOAD_STAGE_ADLS_JPDCL","dev/DCL/jjrnki/kr_special_discount_file/","KR_SPECIAL_DISCOUNT_FILE"]
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        
        df_schema = StructType([
                # StructField("mnth_id",StringType(), True),
                StructField("PATTERNID",StringType(), True),
                StructField("CTL_FLG",StringType(), True),
                StructField("C_DICHANELID",StringType(), True),
                StructField("dummy1",StringType(), True),
                # StructField("C_DSDMNUMBER",StringType(), True),
                StructField("C_DSDMSENDKUBUN",StringType(), True),
                StructField("C_DSDMSENDDATE",StringType(), True),
                StructField("C_DSDMNAME",StringType(), True),
                StructField("C_DSEXTENSION1",StringType(), True),
                StructField("C_DSEXTENSION2",StringType(), True),
                StructField("C_DSEXTENSION3",StringType(), True),
                StructField("C_DSEXTENSION4",StringType(), True),
                StructField("C_DSEXTENSION5",StringType(), True),
                StructField("DSITEMID",StringType(), True),
                StructField("DSITEMNAME",StringType(), True),
                StructField("EXTRACTION_DATE",StringType(), True),
                StructField("SOURCE_FILE_DATE",StringType(), True),
                StructField("INSERTED_DATE",StringType(), True),
                StructField("INSERTED_BY",StringType(), True),
                StructField("UPDATED_DATE",StringType(), True),
                StructField("UPDATED_BY",StringType(), True),
                    
                                                       
            ])
        
        df = session.read\\
            .schema(df_schema)\\
            .option("encoding", "UTF-8")\\
            .option("skip_header",1)\\
            .option("field_delimiter", ",")\\
            .option("skip_blank_lines", True)\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
            # .option("field_optionally_enclosed_by", "\\"")\\
            # .option("field_delimiter",",")\\

        
        
        df=df.na.drop("all")
        if df.count()==0 :
            return "No Data in file"

        
        # df = df.with_column("cdl_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        file_name1=file_name.split(".")[0]+"_"+ datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")+".csv"
        df = df.with_column("file_name",lit(file_name1))
        df = df.with_column("source_file_date",lit(file_name.split("_")[5].split(".")[0]))
        df = df.with_column("crtd_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        # df = df.with_column("mnth_id", file_name.split("_")[1])
        df = df.with_column("inserted_date", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        df = df.with_column("updated_date", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        df = df.with_column("inserted_by", lit(""))
        df = df.with_column("yearmonth", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        df = df.with_column("updated_by", lit(""))
        df = df.with_column("C_DSDMNUMBER",lit("C9924061001"))

        
        # date=file_name.split("_")[3].split(".")[0]
        # year=date[0:4]
        # mnth_no=date[4:6]
        # inv_week=date[6:]
        # df = df.withColumn("year", lit(year))
        # df = df.with_column("mnth_no", lit(mnth_no))
        # df = df.with_column("inv_week", lit(inv_week))  
        
        snowdf= df.select(
                    # "mnth_id",
                    ''PATTERNID'',
                    ''CTL_FLG'',
                    ''C_DICHANELID'',
                    ''C_DSDMNUMBER'',
                    ''C_DSDMSENDKUBUN'',
                    ''C_DSDMSENDDATE'',
                    ''C_DSDMNAME'',
                    ''C_DSEXTENSION1'',
                    ''C_DSEXTENSION2'',
                    ''C_DSEXTENSION3'',
                    ''C_DSEXTENSION4'',
                    ''C_DSEXTENSION5'',
                    ''DSITEMID'',
                    ''DSITEMNAME'',
                    ''EXTRACTION_DATE'',
                    ''SOURCE_FILE_DATE'',
                    ''INSERTED_DATE'',
                    ''INSERTED_BY'',
                    ''UPDATED_DATE'',
                    ''UPDATED_BY''        
                    )
        file_name=file_name.split(".")[0]+"_"+datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")

        # print(snowdf)
        
        #move to success
        
        snowdf.write.mode("append").saveAsTable(stage_name.split(".")[0] + "." + target_table)
        snowdf.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True,file_format_name=''FILE_FORMAT_COMMA_1'')
        
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


CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.JPDCLSDL_RAW.JPDCL_SFMC_BOUNCES_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them.

from snowflake.snowpark.functions import col, lit, date_format, current_timestamp, to_date, year, month, concat, format_number, regexp_replace,to_timestamp,when,trim,upper
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DecimalType,DateType
from datetime import datetime
import pandas as pd
import snowflake.snowpark as snowpark
import pytz

def main(session: snowpark.Session,Param): 
    
    try:
        
        #Param=[''Bounces.csv'',''JPDCLSDL_RAW.DEV_LOAD_STAGE_ADLS_JPDCL'',''dev/DCL/SFMC/'',''sfmc_Bounces'']
        
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        sch_name        = stage_name.split(''.'')[0]
        target_table    = sch_name+"."+Param[3]

        df_schema=StructType([
                StructField("CLIENTID",StringType()),
                StructField("SENDID",StringType()),
                StructField("SUBSCRIBERKEY",StringType()),
                StructField("EMAILADDRESS",StringType()),
                StructField("SUBSCRIBERID",StringType()),
                StructField("LISTID",StringType()),
                StructField("EVENTDATE",StringType()),
                StructField("EVENTTYPE",StringType()),
                StructField("BOUNCECATEGORY",StringType()),
                StructField("SMTPCODE",StringType()),
                StructField("BOUNCEREASON",StringType()),
                StructField("BATCHID",StringType()),
                StructField("TRIGGEREDSENDEXTERNALKEY",StringType())
                ])
       
        # Read the CSV file into a DataFrame
    
        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", ",")\\
            .option("encoding","UTF-16")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .option("skip_blank_lines", True)\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)


        #---------------------------Transformation logic ------------------------------#
        

        # Handle null values or empty rows
        df=df.na.drop("all")

        # Check for empty Dataframe
        if df.count()==0:
             return "No Data in file"


        pandas_df=df.to_pandas()

        # Convert date string to datetime
        pandas_df[''EVENTDATE''] = pd.to_datetime(pandas_df[''EVENTDATE''], format=''%m/%d/%Y %I:%M:%S %p'')

        # Format datetime as yyyy-MM-dd HH:mm and store as string
        pandas_df[''EVENTDATE''] = pandas_df[''EVENTDATE''].dt.strftime(''%Y-%m-%d %H:%M:%S'')


        final_df=session.createDataFrame(pandas_df)

        # Creating Final Dataframe
        dataframe = final_df.select(df.columns)


        # Load Data to the target table
        
        dataframe.write.mode("append").saveAsTable(target_table)

        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")

         # write to success folder
    
        file_name=file_name.split(".")[0]+''_''+datetime.now().strftime("%Y%m%d%H%M%S")
        dataframe.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True,file_format_name=''FILE_FORMAT_COMMA'')
        
        return "Success"

    
        
    except KeyError as key_error:
        
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message
        
    except Exception as e:
        
        error_message = f"Error: {str(e)}" +str(df.columns)
        return error_message';


CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.JPDCLSDL_RAW.JPDCL_SFMC_OPENS_PREPROCESSING("PARAM" ARRAY)
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
from snowflake.snowpark.types import StringType, StructType, StructField
import pandas as pd
from datetime import datetime
import pytz
 
def main(session: snowpark.Session,Param):
 
    try:
 
        #Param=[''Opens.csv'',''JPDCLSDL_RAW.DEV_LOAD_STAGE_ADLS_JPDCL'',''dev/DCL/SFMC'',''SFMC_OPENS'']
 
        # Extracting parameters from the input
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
 
 
        # Define the schema for the DataFrame
        df_schema=StructType([
            StructField("CLIENTID", StringType()),
            StructField("SENDID", StringType()),
            StructField("SUBSCRIBERKEY", StringType()),
            StructField("EMAILADDRESS", StringType()),
            StructField("SUBSCRIBERID", StringType()),
            StructField("LISTID", StringType()),
            StructField("EVENTDATE", StringType()),
            StructField("EVENTTYPE", StringType()),
            StructField("BATCHID", StringType()),
            StructField("TRIGGEREDSENDEXTERNALKEY", StringType())
            ])
 
 
        # Read the CSV file into a DataFrame
        dataframe = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("encoding", "UTF-16")\\
            .option("field_delimiter", ",")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .option("skip_blank_lines", True)\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
 
        #---------------------------Transformation logic ------------------------------#
 
        # Handle null values or empty rows
        dataframe=dataframe.na.drop("all")
 
        # Check for empty Dataframe
        if dataframe.count()==0:
            return "No Data in file"

        pandas_df=dataframe.to_pandas()

        # Convert date string to datetime
        pandas_df[''EVENTDATE''] = pd.to_datetime(pandas_df[''EVENTDATE''], format=''%m/%d/%Y %I:%M:%S %p'')
        # Format datetime as yyyy-MM-dd HH:mm and store as string
        pandas_df[''EVENTDATE''] = pandas_df[''EVENTDATE''].dt.strftime(''%Y-%m-%d %H:%M:%S'')
        dataframe = session.create_dataframe(pandas_df)
        
       # Load Data to the target table
        dataframe.write.mode("append").saveAsTable(target_table)
 
         
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
 
        # write to success folder
        file_name=file_name.split(".")[0]
        dataframe.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True,file_format = "idnsdl_raw.CSV_FORMAT_PIPE")
        return "Success"
        # #return final_df
 
    
 
    except KeyError as key_error:
        # Handle KeyError (missing columns) here
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message
    except Exception as e:
        # Handle exceptions here
        error_message = f"Error: {str(e)}"
        return error_message';


CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.JPDCLSDL_RAW.JPDCL_SFMC_SENDJOBS_PREPROCESSING("PARAM" ARRAY)
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
from snowflake.snowpark.types import StringType, StructType, StructField
import pandas as pd
from datetime import datetime
import pytz
 
def main(session: snowpark.Session,Param):
 
    try:
 
        #Param=[''SendJobs.csv'',''JPDCLSDL_RAW.DEV_LOAD_STAGE_ADLS_JPDCL'',''dev/DCL/SFMC'',''sfmc_sendjobs'']
 
        # Extracting parameters from the input
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
 
 
        # Define the schema for the DataFrame
        df_schema=StructType([
            StructField("CLIENTID" , StringType()), 
            StructField("SENDID" , StringType()), 
            StructField("FROMNAME" , StringType()), 
            StructField("FROMEMAIL" , StringType()), 
            StructField("SCHEDTIME" , StringType()), 
            StructField("SENTTIME" , StringType()), 
            StructField("SUBJECT" , StringType()), 
            StructField("EMAILNAME" , StringType()), 
            StructField("TRIGGEREDSENDEXTERNALKEY" , StringType()), 
            StructField("SENDDEFINITIONEXTERNALKEY" , StringType()), 
            StructField("JOBSTATUS" , StringType()), 
            StructField("PREVIEWURL" , StringType()), 
            StructField("ISMULTIPART" , StringType()), 
            StructField("ADDITIONAL" , StringType())
            ])
 
 
        # Read the CSV file into a DataFrame
        dataframe = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("encoding", "UTF-16")\\
            .option("field_delimiter", ",")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .option("skip_blank_lines", True)\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
 
        #---------------------------Transformation logic ------------------------------#
 
        # Handle null values or empty rows
        dataframe=dataframe.na.drop("all")
 
        # Check for empty Dataframe
        if dataframe.count()==0:
            return "No Data in file"

        pandas_df=dataframe.to_pandas()

        # Convert date string to datetime
        pandas_df[''SCHEDTIME''] = pd.to_datetime(pandas_df[''SCHEDTIME''], format=''%m/%d/%Y %I:%M:%S %p'')
        pandas_df[''SENTTIME''] = pd.to_datetime(pandas_df[''SENTTIME''], format=''%m/%d/%Y %I:%M:%S %p'')
        
        # Format datetime as yyyy-MM-dd HH:mm and store as string
        pandas_df[''SCHEDTIME''] = pandas_df[''SCHEDTIME''].dt.strftime(''%Y-%m-%d %H:%M:%S'')
        pandas_df[''SENTTIME''] = pandas_df[''SENTTIME''].dt.strftime(''%Y-%m-%d %H:%M:%S'')

        dataframe = session.create_dataframe(pandas_df)
        
       # Load Data to the target table
        dataframe.write.mode("append").saveAsTable(target_table)
 
         
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
 
        # write to success folder
        file_name=file_name.split(".")[0]
        dataframe.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True,file_format = "idnsdl_raw.CSV_FORMAT_PIPE")
        return "Success"
        # #return final_df
 
    
 
    except KeyError as key_error:
        # Handle KeyError (missing columns) here
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message
    except Exception as e:
        # Handle exceptions here
        error_message = f"Error: {str(e)}"
        return error_message';


CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.JPDCLSDL_RAW.JPDCL_SFMC_SENT_PREPROCESSING("PARAM" ARRAY)
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
from snowflake.snowpark.types import StringType, StructType, StructField
import pandas as pd
from datetime import datetime
import pytz
 
def main(session: snowpark.Session,Param):
 
    try:
 
        #Param=[''Sent.csv'',''JPDCLSDL_RAW.DEV_LOAD_STAGE_ADLS_JPDCL'',''dev/DCL/SFMC'',''sfmc_sent'']
 
        # Extracting parameters from the input
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
 
 
        # Define the schema for the DataFrame
        df_schema=StructType([
            StructField("CLIENTID", StringType()),
            StructField("SENDID", StringType()),
            StructField("SUBSCRIBERKEY", StringType()),
            StructField("EMAILADDRESS", StringType()),
            StructField("SUBSCRIBERID", StringType()),
            StructField("LISTID", StringType()),
            StructField("EVENTDATE", StringType()),
            StructField("EVENTTYPE", StringType()),
            StructField("BATCHID", StringType()),
            StructField("TRIGGEREDSENDEXTERNALKEY", StringType())
            ])
 
 
        # Read the CSV file into a DataFrame
        dataframe = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("encoding", "UTF-16")\\
            .option("field_delimiter", ",")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .option("skip_blank_lines", True)\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
 
        #---------------------------Transformation logic ------------------------------#
 
        # Handle null values or empty rows
        dataframe=dataframe.na.drop("all")
 
        # Check for empty Dataframe
        if dataframe.count()==0:
            return "No Data in file"

        pandas_df=dataframe.to_pandas()

        # Convert date string to datetime
        pandas_df[''EVENTDATE''] = pd.to_datetime(pandas_df[''EVENTDATE''], format=''%m/%d/%Y %I:%M:%S %p'')
        
        # Format datetime as yyyy-MM-dd HH:mm and store as string
        pandas_df[''EVENTDATE''] = pandas_df[''EVENTDATE''].dt.strftime(''%Y-%m-%d %H:%M:%S'')

        dataframe = session.create_dataframe(pandas_df)
        
       # Load Data to the target table
        dataframe.write.mode("append").saveAsTable(target_table)
 
         
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
 
        # write to success folder
        file_name=file_name.split(".")[0]
        dataframe.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True,file_format = "idnsdl_raw.CSV_FORMAT_PIPE")
        return "Success"
        # #return final_df
 
    
 
    except KeyError as key_error:
        # Handle KeyError (missing columns) here
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message
    except Exception as e:
        # Handle exceptions here
        error_message = f"Error: {str(e)}"
        return error_message';


CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.JPDCLSDL_RAW.JPDCL_SFMC_UNSUBS_PREPROCESSING("PARAM" ARRAY)
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
from snowflake.snowpark.types import StringType, StructType, StructField
import pandas as pd
from datetime import datetime
import pytz
 
def main(session: snowpark.Session,Param):
 
    try:
 
        #Param=[''Unsubs.csv'',''JPDCLSDL_RAW.DEV_LOAD_STAGE_ADLS_JPDCL'',''dev/DCL/SFMC'',''sfmc_unsubs'']
 
        # Extracting parameters from the input
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
 
 
        # Define the schema for the DataFrame
        df_schema=StructType([
            StructField("CLIENTID", StringType()),
            StructField("SENDID", StringType()),
            StructField("SUBSCRIBERKEY", StringType()),
            StructField("EMAILADDRESS", StringType()),
            StructField("SUBSCRIBERID", StringType()),
            StructField("LISTID", StringType()),
            StructField("EVENTDATE", StringType()),
            StructField("EVENTTYPE", StringType()),
            StructField("BATCHID", StringType()),
            StructField("TRIGGEREDSENDEXTERNALKEY", StringType())
            ])
 
 
        # Read the CSV file into a DataFrame
        dataframe = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("encoding", "UTF-16")\\
            .option("field_delimiter", ",")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .option("skip_blank_lines", True)\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
 
        #---------------------------Transformation logic ------------------------------#
 
        # Handle null values or empty rows
        dataframe=dataframe.na.drop("all")
 
        # Check for empty Dataframe
        if dataframe.count()==0:
            return "No Data in file"

        pandas_df=dataframe.to_pandas()

        # Convert date string to datetime
        pandas_df[''EVENTDATE''] = pd.to_datetime(pandas_df[''EVENTDATE''], format=''%m/%d/%Y %I:%M:%S %p'')
        
        # Format datetime as yyyy-MM-dd HH:mm and store as string
        pandas_df[''EVENTDATE''] = pandas_df[''EVENTDATE''].dt.strftime(''%Y-%m-%d %H:%M:%S'')

        dataframe = session.create_dataframe(pandas_df)
        
       # Load Data to the target table
        dataframe.write.mode("append").saveAsTable(target_table)
 
         
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
 
        # write to success folder
        file_name=file_name.split(".")[0]
        dataframe.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True,file_format = "idnsdl_raw.CSV_FORMAT_PIPE")
        return "Success"
        # #return final_df
 
    
 
    except KeyError as key_error:
        # Handle KeyError (missing columns) here
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message
    except Exception as e:
        # Handle exceptions here
        error_message = f"Error: {str(e)}"
        return error_message';

CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.JPDCLSDL_RAW.JPNDCL_SFMC_CLICKS_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them.

from snowflake.snowpark.functions import col, lit, date_format, current_timestamp, to_date, year, month, concat, format_number, regexp_replace,to_timestamp,when,trim,upper
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DecimalType,DateType
from datetime import datetime
import pandas as pd
import snowflake.snowpark as snowpark
import pytz

def main(session: snowpark.Session,Param): 
    
    try:
        
        #Param=[''Clicks.csv'',''JPDCLSDL_RAW.DEV_LOAD_STAGE_ADLS_JPDCL'',''dev/DCL/SFMC/'',''SFMC_CLICKS'']
        
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        sch_name        = stage_name.split(''.'')[0]
        target_table    = sch_name+"."+Param[3]

        df_schema=StructType([
                StructField("CLIENTID",StringType()),
                StructField("SENDID",StringType()),
                StructField("SUBSCRIBERKEY",StringType()),
                StructField("EMAILADDRESS",StringType()),
                StructField("SUBSCRIBERID",StringType()),
                StructField("LISTID",StringType()),
                StructField("EVENTDATE",StringType()),
                StructField("EVENTTYPE",StringType()),
                StructField("SENDURLID",StringType()),
                StructField("URLID",StringType()),
                StructField("URL",StringType()),
                StructField("ALIAS",StringType()),
                StructField("BATCHID",StringType()),
                StructField("TRIGGEREDSENDEXTERNALKEY",StringType())
                ])
       
        # Read the CSV file into a DataFrame
    
        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", ",")\\
            .option("encoding","UTF-16")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .option("skip_blank_lines", True)\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)


        #---------------------------Transformation logic ------------------------------#
        

        # Handle null values or empty rows
        df=df.na.drop("all")

        # Check for empty Dataframe
        if df.count()==0:
             return "No Data in file"

        pandas_df=df.to_pandas()

        # Convert date string to datetime
        pandas_df[''EVENTDATE''] = pd.to_datetime(pandas_df[''EVENTDATE''], format=''%m/%d/%Y %I:%M:%S %p'')

        # Format datetime as yyyy-MM-dd HH:mm and store as string
        pandas_df[''EVENTDATE''] = pandas_df[''EVENTDATE''].dt.strftime(''%Y-%m-%d %H:%M:%S'')


        final_df=session.createDataFrame(pandas_df)

        # Creating Final Dataframe
        dataframe = final_df.select(df.columns)


        # Load Data to the target table
        
        dataframe.write.mode("append").saveAsTable(target_table)

        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")

         # write to success folder
    
        file_name=file_name.split(".")[0]+''_''+datetime.now().strftime("%Y%m%d%H%M%S")
        dataframe.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True,file_format_name=''FILE_FORMAT_COMMA'')
        
        return "Success"

    
        
    except KeyError as key_error:
        
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message
        
    except Exception as e:
        
        error_message = f"Error: {str(e)}" +str(df.columns)
        return error_message';

CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.JPDCLSDL_RAW.JPNDCL_SFMC_KOKYA_SUBKEY_MAPPING_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them.

from snowflake.snowpark.functions import col, lit, date_format, current_timestamp, to_date, year, month, concat, format_number, regexp_replace,to_timestamp,when,trim,upper
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DecimalType,DateType
from datetime import datetime
import pandas as pd
import snowflake.snowpark as snowpark
import pytz

def main(session: snowpark.Session,Param): 
    
    try:
        
        #Param=[''Account_Customer_IDLink_20240617.csv'',''JPDCLSDL_RAW.DEV_LOAD_STAGE_ADLS_JPDCL'',''dev/DCL/'',''sfmc_kokya_subskey_mapping'']
        
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        sch_name        = stage_name.split(''.'')[0]
        target_table    = sch_name+"."+Param[3]

        df_schema=StructType([
                StructField("ACCOUNTID",StringType()),
                StructField("CUSTOMERID",StringType())
                ])
       
        # Read the CSV file into a DataFrame
    
        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", ",")\\
            .option("encoding","UTF-16")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .option("skip_blank_lines", True)\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)


        #---------------------------Transformation logic ------------------------------#
        

        # Handle null values or empty rows
        df=df.na.drop("all")

        # Check for empty Dataframe
        if df.count()==0:
             return "No Data in file"

        dataframe=df.fillna('''')

        SOURCE_FILE_DATE=file_name.split("_")[-1].split(".")[0]
        INSERTED_DATE=datetime.now()
        INSERTED_BY=''''
        UPDATED_DATE=datetime.now()
        UPDATED_BY=''''

        dataframe = dataframe.withColumn("SOURCE_FILE_DATE",lit(SOURCE_FILE_DATE))
        dataframe = dataframe.withColumn("INSERTED_DATE",lit(INSERTED_DATE))
        dataframe = dataframe.withColumn("INSERTED_BY",lit(INSERTED_BY))
        dataframe = dataframe.withColumn("UPDATED_DATE",lit(UPDATED_DATE))
        dataframe = dataframe.withColumn("UPDATED_BY",lit(UPDATED_BY))
        

        
         # Creating Final Dataframe
        final_df = dataframe.alias("final_df")


        # Load Data to the target table
        
        final_df.write.mode("append").saveAsTable(target_table)

        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")

         # write to success folder
    
        file_name=file_name.split(".")[0]+''_''+datetime.now().strftime("%Y%m%d%H%M%S")
        final_df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True,file_format_name=''FILE_FORMAT_COMMA'')
        
        return "Success"

    
        
    except KeyError as key_error:
        
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message
        
    except Exception as e:
        
        error_message = f"Error: {str(e)}" +str(df.columns)
        return error_message';

CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.JPDCLSDL_RAW.JPNDCL_SFMC_NOTSENT_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them.

from snowflake.snowpark.functions import col, lit, date_format, current_timestamp, to_date, year, month, concat, format_number, regexp_replace,to_timestamp,when,trim,upper
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DecimalType,DateType
from datetime import datetime
import pandas as pd
import snowflake.snowpark as snowpark
import pytz

def main(session: snowpark.Session,Param): 
    
    try:
        
        #Param=[''NotSent.csv'',''JPDCLSDL_RAW.DEV_LOAD_STAGE_ADLS_JPDCL'',''dev/DCL/SFMC/'',''SFMC_NOTSENT'']
        
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        sch_name        = stage_name.split(''.'')[0]
        target_table    = sch_name+"."+Param[3]

        df_schema=StructType([
                StructField("CLIENTID",StringType()),
                StructField("SENDID",StringType()),
                StructField("SUBSCRIBERKEY",StringType()),
                StructField("EMAILADDRESS",StringType()),
                StructField("SUBSCRIBERID",StringType()),
                StructField("LISTID",StringType()),
                StructField("EVENTDATE",StringType()),
                StructField("EVENTTYPE",StringType()),
                StructField("BATCHID",StringType()),
                StructField("TRIGGEREDSENDEXTERNALKEY",StringType()),
                StructField("REASON",StringType())
                ])
       
        # Read the CSV file into a DataFrame
    
        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", ",")\\
            .option("encoding","UTF-16")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .option("skip_blank_lines", True)\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)


        #---------------------------Transformation logic ------------------------------#
        

        # Handle null values or empty rows
        df=df.na.drop("all")

        # Check for empty Dataframe
        if df.count()==0:
             return "No Data in file"

        pandas_df=df.to_pandas()

        # Convert date string to datetime
        pandas_df[''EVENTDATE''] = pd.to_datetime(pandas_df[''EVENTDATE''], format=''%m/%d/%Y %I:%M:%S %p'')

        # Format datetime as yyyy-MM-dd HH:mm and store as string
        pandas_df[''EVENTDATE''] = pandas_df[''EVENTDATE''].dt.strftime(''%Y-%m-%d %H:%M:%S'')


        final_df=session.createDataFrame(pandas_df)

        # Creating Final Dataframe
        dataframe = final_df.select(df.columns)


        # Load Data to the target table
        
        dataframe.write.mode("append").saveAsTable(target_table)

        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")

         # write to success folder
    
        file_name=file_name.split(".")[0]+''_''+datetime.now().strftime("%Y%m%d%H%M%S")
        dataframe.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True,file_format_name=''FILE_FORMAT_COMMA'')
        
        return "Success"

    
        
    except KeyError as key_error:
        
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message
        
    except Exception as e:
        
        error_message = f"Error: {str(e)}" +str(df.columns)
        return error_message';
