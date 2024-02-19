CREATE OR REPLACE PROCEDURE ASPSDL_RAW.CDFG_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '
import snowflake.snowpark as snowpark
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField,DecimalType
from snowflake.snowpark.functions import col,lit,current_timestamp
from datetime import datetime


def main(session: snowpark.Session,Param): 
    # Your code goes here, inside the "main" handler
    #Param=[''CDFG_202301.xlsx'',''ASPSDL_RAW.PROD_LOAD_STAGE_ADLS'',''prd/transactional'',''sdl_rg_travel_retail_cdfg'']

    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        db_name         = stage_name.split(''.'')[0]
        target_table    = db_name+"."+Param[3]
        year_month    = file_name.split(''.'')[0][-6:]  ## Folder expected is in YYYYMM format.
        target_raw_table= target_table+"_raw"
        final_df = None
        #session.use_schema(db_name)
    
        cdfg_df_schema=StructType([
            StructField("dcl_code",StringType()),
            StructField("barcode",StringType()),
            StructField("description",StringType()),
            StructField("sls_qty",StringType()),
            StructField("stock_qty",StringType())
            ])
        
        
        
            #---------------------------Transformation logic ------------------------------
        # Create a list of location names
        sheetlist = ["MEMBERS", "HAIKOU_BUGOU", "HTB", "Cambodia_Downtown", "XHG", "BEIJING_AIRPORT"]
        
        for sheet in sheetlist:
            stage_path="@{0}/{1}/{2}.csv".format(stage_name,temp_stage_path,sheet)
        
            
            cdfg_df = session.read\\
            .schema(cdfg_df_schema)\\
            .option("skip_header",3)\\
            .option("field_delimiter", "\\u0001")\\
            .option("field_optionally_enclosed_by", "\\"") \\
            .csv(stage_path)
        
        
        
        
            cdfg_df=cdfg_df.withColumn(''location_name'',lit(sheet))\\
                           .withColumn(''retailer_name'',lit(file_name.split(''_'')[0]))\\
                           .withColumn(''year_month'',lit(year_month))\\
                           .withColumn(''filename'',lit(file_name))
        
            if not final_df:
                final_df = cdfg_df
            else:
                final_df = final_df.unionByName(cdfg_df)
          
        final_df=final_df.filter(col("barcode") != "")
        
        if final_df.count()==0 :
            return "No Data in file"
                    
        final_df=final_df[["location_name","retailer_name","year_month","dcl_code","barcode"\\
                         ,"description","sls_qty","stock_qty","filename"]]
                        

        #writing on target

        file_name=file_name+''_''+datetime.now().strftime("%Y%m%d%H%M%S")
        
        # write to target table
        final_df.write.mode("append").saveAsTable(target_table)
        
        final_df.write.mode("append").saveAsTable(target_raw_table)
        
        #move to success
        final_df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/success/"+file_name,file_format_type="csv",header=True,OVERWRITE=True)
                     
        return ''Success''
    except Exception as e:
        # Handle exceptions here
        error_message = f"Error: {str(e)}"
        return error_message



#call ASPSDL_RAW.CDFG_PREPROCESSING([''CDFG_202301.xlsx'',''ASPSDL_RAW.PROD_LOAD_STAGE_ADLS'',
#''prd/transactional'',''sdl_rg_travel_retail_cdfg'']);';
