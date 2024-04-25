CREATE OR REPLACE PROCEDURE PCFSDL_RAW.PCF_PERENSO_SURVEYRESULT_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
PACKAGES = ('openpyxl==3.0.10','snowflake-snowpark-python==*')
HANDLER = 'main'
EXECUTE AS CALLER
AS 'import snowflake.snowpark as snowpark
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField ,DecimalType
from snowflake.snowpark.functions import lit
from datetime import datetime
import pytz

def main(session: snowpark.Session,Param): 
    
	# SP call and parameters to pass.
	# CALL PCFSDL_RAW.PCF_PERENSO_SURVEYRESULT_PREPROCESSING([''SurveyResult.csv'',''PCFSDL_RAW.PROD_LOAD_STAGE_ADLS'',''prd/ap_perenso/transaction/survey'',''sdl_perenso_survey_result''])
	
    try:
        # Extracting parameters from the input

        file_name       = Param[0]     
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        db_name         = stage_name.split(''.'')[0]
        target_table    = db_name+"."+Param[3]
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")


        # Define the schema for the DataFrame
        df_schema=StructType([
        StructField(''store_chk_hdr_key'' , StringType()),
        StructField(''line_key'' , StringType()),
        StructField(''todo_key'' , StringType()),
        StructField(''prod_grp_key'' , StringType()),
        StructField(''optionans'' , StringType()),
        StructField(''notesans'' , StringType())
            ])
        # Set the current session schema
        
        session.use_schema(stage_name.split(''.'')[0])


        stage_path="@{0}/{1}/{2}".format(stage_name,temp_stage_path,file_name)
        print(stage_path)
        
        temp_df = session.read\\
        .schema(df_schema)\\
        .option("skip_header",1)\\
        .option("field_delimiter", ",")\\
        .option("field_optionally_enclosed_by", "\\"")\\
        .csv(stage_path)
        
        temp_df=temp_df.na.drop("all")
        if temp_df.count()==0:
            return "No Data in file"
        print(temp_df.show(10))

        
        temp_df = temp_df.withColumn("run_id",lit(lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S"))))
        temp_df = temp_df.withColumn("create_dt",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        
        
                    
        final_df=temp_df.select("store_chk_hdr_key","line_key","todo_key","prod_grp_key","optionans","notesans","run_id","create_dt")
                        


        file_name=file_name+''_''+datetime.now().strftime("%Y%m%d%H%M%S")
        
        # write to target table
        final_df.write.mode("append").saveAsTable(target_table)
        
        #move to success
        final_df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True,file_format_name="PCFSDL_RAW.MY_CSV_FORMAT")



        return ''Success''

    except KeyError as key_error:
        # Handle KeyError (missing columns) here
        error_message = f"KeyError: {str(key_error)}"
        return error_message

    
    except Exception as e:
        # Handle exceptions here
        error_message = f"Error: {str(e)}"
        return error_message
';
