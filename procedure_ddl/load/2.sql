USE DATABASE PROD_DNA_LOAD;
use schema ASPSDL_RAW;

CREATE OR REPLACE PROCEDURE ASPSDL_RAW.CNSC_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS 'import snowflake.snowpark as snowpark
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField,DecimalType
from snowflake.snowpark.functions import col,lit,current_timestamp,to_timestamp
from datetime import datetime


def main(session: snowpark.Session,Param): 

    try:
        # Extracting parameters from the input
        #Param= [''CNSC DR.CILABO SALES REPORT 202206.xlsx'',''ASPSDL_RAW.PROD_LOAD_STAGE_ADLS'',''prd/transactional/CNSC'',''sdl_rg_travel_retail_cnsc_test2'']
        
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        db_name         = stage_name.split(''.'')[0]
        target_table    = db_name+"."+Param[3]
        year_month      = file_name[28:34]
        main_df         = None
        
        #session.use_schema(db_name)
        
        # Define the schema for the DataFrame
        
        sy_dt_df_schema=StructType([
            StructField("serial_number",StringType()),
            StructField("ean",StringType()),
            StructField("brand",StringType()),
            StructField("Product_description",StringType()),
            StructField("pack_size",StringType()),
            StructField("Inventory_Qty",IntegerType()),
            StructField("Sales_Qty",IntegerType()),
            StructField("Sales_amount",DecimalType(38,8)),
            StructField("store_sales",IntegerType()),
            StructField("total_store_sales",DecimalType(38,18)),
            StructField("no_of_ecommerce_sales",IntegerType()),
            StructField("total_ecommerce_sales",DecimalType(38,18)),
            StructField("membership_sls_qty",IntegerType()),
            StructField("membership_sls_amt",DecimalType(38,18)),
            StructField("DCL_CODE",StringType())
            ])
        
        
        non_sy_dt_df_schema=StructType([
            StructField("Supplier_product_code",StringType()),
            StructField("Product_description",StringType()),
            StructField("brand",StringType()),
            StructField("pack_size",StringType()),
            StructField("serial_number",StringType()),
            StructField("Inventory_Qty",IntegerType()),
            StructField("Sales_Qty",IntegerType()),
            StructField("Sales_amount",DecimalType(38,8)),
            StructField("Material_Code",StringType()),
            StructField("EAN",StringType()),
            StructField("DCL_CODE",StringType())
            ])
        
	    
            #---------------------------Transformation logic ------------------------------
        # Create a list of location names
        sheetlist = ["SY_DT", "ZZ_DT", "HZ_DT", "BJ_DT", "CQ_DT", "DL_DT"]
        
        for sheet in sheetlist:
            stage_path="@{0}/{1}/{2}.csv".format(stage_name,temp_stage_path,sheet)
            print(stage_path)
	    	
            filter_col=["TOTAL","TOTAL AMOUNT"]
	    	
            # Read the CSV file into a DataFrame
            
            if sheet == "SY_DT":
                sy_dt_df = session.read\\
                .schema(sy_dt_df_schema)\\
                .option("skip_header",2)\\
                .option("field_delimiter", "\\u0001")\\
                .option("field_optionally_enclosed_by", "\\"") \\
                .csv(stage_path)
                
                # Filter records that are not required 
                main_df=sy_dt_df.filter(~col("serial_number").isin(filter_col))
                main_df=main_df.dropna(subset=["ean", "brand","Product_description"])
                main_df=main_df.withColumn("door_name",lit(sheet.replace("_","")))\\
                               .withColumn("yearmo",lit(year_month))\\
                               .withColumn("Supplier_product_code",lit(None))\\
                               .withColumn("Material_Code",lit(None))\\
                               .withColumn("filename",lit(file_name))
            else :
                # Read the CSV file into a DataFrame
                non_sy_dt_df = session.read\\
                .schema(non_sy_dt_df_schema)\\
                .option("skip_header",1)\\
                .option("field_delimiter", "\\u0001")\\
                .option("field_optionally_enclosed_by", "\\"") \\
                .csv(stage_path)
                
	    		# Filter records that are not required 
                filtered_df=non_sy_dt_df.filter(~col("Supplier_product_code").isin(filter_col))
                filtered_df=filtered_df.dropna(subset=["serial_number", "brand","Product_description"])
                filtered_df=filtered_df.withColumn("door_name",lit(sheet.replace("_","")))\\
                                       .withColumn("yearmo",lit(year_month))\\
                                       .withColumn("store_sales",lit(None))\\
                                       .withColumn("total_store_sales",lit(None))\\
                                       .withColumn("no_of_ecommerce_sales",lit(None))\\
                                       .withColumn("total_ecommerce_sales",lit(None))\\
	    							   .withColumn("membership_sls_qty",lit(None))\\
	    							   .withColumn("membership_sls_amt",lit(None))\\
                                       .withColumn("filename",lit(file_name.replace("_"," ")))
                
                if main_df is not None:
                    main_df = main_df.unionByName(filtered_df)
                else:
                    main_df = filtered_df
        
        # Adding new columns to dataframe                
        main_df = main_df.withColumn("retailer_name",lit("CHINA NATIONAL SERVICE CORPORATION"))\\
                         .withColumn("crttd",lit(to_timestamp(current_timestamp())))
                    
        main_df=main_df[["door_name","yearmo","retailer_name","supplier_product_code"\\
                         ,"product_description","brand","pack_size","serial_number"\\
                         ,"inventory_qty","sales_qty","sales_amount","material_code"\\
                         ,"ean","dcl_code","filename","store_sales","total_store_sales"\\
                      ,"no_of_ecommerce_sales","total_ecommerce_sales","membership_sls_qty"\\
                         ,"membership_sls_amt","crttd"]]
        
        main_df= main_df.filter(main_df["ean"].isNotNull())
        
        if main_df.count()==0 :
            return "No Data in file" 
        
        
        
        print("snowpark dataframe created")
        
        # Load the dataframe in target table in append mode
        main_df.write.mode("append").saveAsTable(target_table)
        
        #Move the file to Success folder after execution.
        file_name=file_name+''_''+datetime.now().strftime("%Y%m%d%H%M%S")
        main_df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/success/"+file_name,file_format_type="csv",header=True,OVERWRITE=True)
                     
        return "Success"
        
    except Exception as e:
        # Handle exceptions here
        error_message = f"Error: {str(e)}"
        return error_message
        
#call ASPSDL_RAW.CNSC_PREPROCESSING([''CNSC DR.CILABO SALES REPORT 202206.xlsx'',''ASPSDL_RAW.PROD_LOAD_STAGE_ADLS'',
#''prd/transactional/CNSC'',''sdl_rg_travel_retail_cnsc'']);';
