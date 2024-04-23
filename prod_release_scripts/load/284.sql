CREATE OR REPLACE PROCEDURE PCF_PERENSO_PRODUCT_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
PACKAGES = ('openpyxl==3.0.10','snowflake-snowpark-python==*')
HANDLER = 'main'
COMMENT='One SP for 7 Tables'
EXECUTE AS CALLER
AS 'import snowflake.snowpark as snowpark
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField ,DecimalType
from snowflake.snowpark.functions import lit
from datetime import datetime
import pytz


def get_schema(file_name):
    df_schema=""
    select_col=""
    if file_name.startswith("ProdBranchIdentifier"):
        df_schema=StructType([
        StructField(''prod_key'' , IntegerType()),
        StructField(''branch_key'' , IntegerType()),
        StructField(''identifier'' , StringType())
            ])
        select_col=["prod_key","branch_key","identifier","run_id","create_dt"]
    if file_name.startswith("ProdField"):
        df_schema=StructType([
        StructField(''field_key'' , IntegerType()),
        StructField(''field_desc'' , StringType()),
        StructField(''field_type'' , IntegerType()),
        StructField(''active'' , StringType())
            ])
        select_col=["field_key","field_desc","field_type","active","run_id","create_dt"]
    if file_name.startswith("ProdGrp"):
        df_schema=StructType([
        StructField(''prod_grp_key'' , IntegerType()),
        StructField(''Prod_grp_lev_key'' , IntegerType()),
        StructField(''grp_desc'' , StringType()),
        StructField(''dsp_order'' , IntegerType()),
        StructField(''parent_key'' , IntegerType())
            ])
        select_col=["prod_grp_key","Prod_grp_lev_key","grp_desc","dsp_order","parent_key","run_id","create_dt"]
    if file_name.startswith("ProdGrpLevel"):
        df_schema=StructType([
        StructField(''Prod_grp_lev_key'' , IntegerType()),
        StructField(''prod_Lev_desc'' , StringType()),
        StructField(''prod_Lev_index'' , IntegerType()),
        StructField(''field_key'' , IntegerType())
            ])
        select_col=["Prod_grp_lev_key","prod_Lev_desc","prod_Lev_index","field_key","run_id","create_dt"]
        
    if file_name.startswith("ProdGrpRel"):
        df_schema=StructType([
        StructField(''prod_key'' , IntegerType()),
        StructField(''prod_grp_key'' , IntegerType())
            ])
        select_col=["prod_key","prod_grp_key","run_id","create_dt"]
        
    if file_name.startswith("ProdRelId"):
        df_schema=StructType([
        StructField(''prod_key'' , IntegerType()),
        StructField(''field_key'' , IntegerType()),
        StructField(''id'' , StringType())    
            ])
        select_col=["prod_key","field_key","id","run_id","create_dt"]
        
    if file_name.startswith("Product"):
        df_schema=StructType([
        StructField(''prod_key'' , IntegerType()),
        StructField(''Prod_id'' , StringType()),
        StructField(''Prod_desc'' , StringType()), 
        StructField(''Ean'' , StringType()),
        StructField(''Active'' , StringType()), 
            ])
        select_col=["prod_key","Prod_id","Prod_desc","Ean","Active","run_id","create_dt"]
    return df_schema,select_col
        



def main(session: snowpark.Session,Param): 
    
	# SP call and parameters to pass.
	# CALL PCFSDL_RAW.PCF_PERENSO_PRODUCT_PREPROCESSING([''Product_20240325223028.csv'',''PCFSDL_RAW.DEV_LOAD_STAGE_ADLS'',''dev/ap_perenso/master/product'',''sdl_perenso_product''])
	
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
        df_schema,select_col=get_schema(file_name)
        print(df_schema)
        print(select_col)
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

            
        temp_df = temp_df.withColumn("run_id",lit(lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S"))))
        temp_df = temp_df.withColumn("create_dt",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
            
        
                    
        final_df=temp_df.select(select_col)
                        


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
        return error_message';
