CREATE OR REPLACE PROCEDURE PCFSDL_RAW.PCF_PERENSO_METCASH_PREPROCESSING("PARAM" ARRAY)
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
	# CALL PCFSDL_RAW.PCF_PERENSO_METCASH_PREPROCESSING(["Monthly_Sales_Report,_Supplier_filter_only_J_J202402.csv","PCFSDL_RAW.DEV_LOAD_STAGE_ADLS","dev/ap_perenso/transaction/metcash/grocery","sdl_metcash_ind_grocery"])
	
    try:
        # Extracting parameters from the input

        file_name       = Param[0]     
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        db_name         = stage_name.split(".")[0]
        target_table    = db_name+"."+Param[3]
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")


        # Define the schema for the DataFrame
        df_schema=StructType([
        StructField("supp_id" , StringType()),
        StructField("supp_name" , StringType()),
        StructField("state" , StringType()),
        StructField("banner_id" , StringType()),
        StructField("banner" , StringType()),
        StructField("customer_id" , StringType()),
        StructField("customer" , StringType()),
        StructField("product_id" , StringType()),
        StructField("product" , StringType()),
        StructField("gross_sales_wk1" , StringType()),
        StructField("gross_sales_wk2" , StringType()),
        StructField("gross_sales_wk3" , StringType()),
        StructField("gross_sales_wk4" , StringType()),
        StructField("gross_cases_wk1" , StringType()),
        StructField("gross_cases_wk2" , StringType()),
        StructField("gross_cases_wk3" , StringType()),
        StructField("gross_cases_wk4" , StringType()),
            ])
        # Set the current session schema
        
        session.use_schema(stage_name.split(".")[0])

        
        
        stage_path="@{0}/{1}/{2}".format(stage_name,temp_stage_path,file_name)
        print(stage_path)
        
        temp_df = session.read\\
        .schema(df_schema)\\
        .option("skip_header",0)\\
        .option("field_delimiter", "\\u0001")\\
        .option("field_optionally_enclosed_by", "\\"") \\
        .csv(stage_path)
        
        temp_df=temp_df.na.drop("all")
        if temp_df.count()==0:
            return "No Data in file"

            
        temp_df = temp_df.withColumn("run_id",lit(lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S"))))
		temp_df = temp_df.withColumn("gross_sales_wk5",None)
		temp_df = temp_df.withColumn("gross_cases_wk5",None)
        temp_df = temp_df.withColumn("create_dt",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        temp_df = temp_df.withColumn("file_name",lit(file_name.replace("_"," ").replace(".csv",".xlsx")))
                    
        final_df=temp_df.select("supp_id","supp_name","state","banner_id","banner","customer_id","customer","product_id","product","gross_sales_wk1","gross_sales_wk2","gross_sales_wk3","gross_sales_wk4","gross_sales_wk5","gross_cases_wk1","gross_cases_wk2","gross_cases_wk3","gross_cases_wk4","gross_cases_wk5","run_id","file_name","create_dt")
                        


        file_name=file_name+"_"+datetime.now().strftime("%Y%m%d%H%M%S")
        
        # write to target table
        final_df.write.mode("append").saveAsTable(target_table)
        
        #move to success
        final_df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)


        return "Success"

    except KeyError as key_error:
        # Handle KeyError (missing columns) here
        error_message = f"KeyError: {str(key_error)}"
        return error_message

    
    except Exception as e:
        # Handle exceptions here
        error_message = f"Error: {str(e)}"
        return error_message';
