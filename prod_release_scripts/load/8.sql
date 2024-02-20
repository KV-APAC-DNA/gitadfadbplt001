USE DATABASE PROD_DNA_LOAD;
use schema ASPSDL_RAW;


CREATE OR REPLACE PROCEDURE CDFG_PREPROCESSING("PARAM" ARRAY)
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
        
        
        #move to success
        final_df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/success/"+file_name,file_format_type="csv",header=True,OVERWRITE=True)
                     
        return ''Success''
    except Exception as e:
        # Handle exceptions here
        error_message = f"Error: {str(e)}"
        return error_message



#call ASPSDL_RAW.CDFG_PREPROCESSING([''CDFG_202301.xlsx'',''ASPSDL_RAW.PROD_LOAD_STAGE_ADLS'',
#''prd/transactional'',''sdl_rg_travel_retail_cdfg'']);';

CREATE OR REPLACE PROCEDURE CNSC_PREPROCESSING("PARAM" ARRAY)
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


CREATE OR REPLACE PROCEDURE SALESSTOCK_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS CALLER
AS '
import snowflake.snowpark as snowpark
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField
from snowflake.snowpark.functions import col,lit,current_timestamp
import pandas as pd
from datetime import datetime
from snowflake.snowpark import Row 

def main(session: snowpark.Session,Param): 
    
	# SP call and parameters to pass.
	# CALL ASPSDL_RAW.SALESSTOCK_PREPROCESSING([''SalesStock_2022.xlsx'',''ASPSDL_RAW.PROD_LOAD_STAGE_ADLS'',''prd/transactional'',''sdl_rg_travel_retail_sales_stock''])
	
    try:
        # Extracting parameters from the input

        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        db_name         = stage_name.split(''.'')[0]
        target_table    = db_name+"."+Param[3]
        all_months      = ["JAN","FEB","MAR","APR","MAY","JUN","JUL","AUG","SEP","OCT","NOV","DEC"] 
        sls_stk_mth     = {"01":["JAN_SLS","JAN_STOCK"],
                           "02":["FEB_SLS","FEB_STOCK"],
                           "03":["MAR_SLS","MAR_STOCK"],
                           "04":["APR_SLS","APR_STOCK"],
                           "05":["MAY_SLS","MAY_STOCK"],
                           "06":["JUN_SLS","JUN_STOCK"],
                           "07":["JUL_SLS","JUL_STOCK"],
                           "08":["AUG_SLS","AUG_STOCK"],
                           "09":["SEP_SLS","SEP_STOCK"],
                           "10":["OCT_SLS","OCT_STOCK"],
                           "11":["NOV_SLS","NOV_STOCK"],
                           "12":["DEC_SLS","DEC_STOCK"]}
        header_dict     = {}
        transformed_rows = []
        final_df = None
        

        # Define the schema for the DataFrame
        df_schema=StructType([
            StructField("temp_col",StringType()),
            StructField("dcl_code",StringType()),
            StructField("sap_code",StringType()),
            StructField("reference",StringType()),
            StructField("product_desc",StringType()),
            StructField("size",StringType()),
            StructField("rsp",StringType()),
            StructField("c_sls_qty",StringType()),
            StructField("c_sls_amt",StringType()),
            StructField("c_stock_qty",StringType()),
            StructField("c_stock_amt",StringType()),
            StructField("buffer",StringType()),
            StructField("mix",StringType()),
            StructField("r_3m",StringType()),
            StructField("comparison",StringType()),
            StructField("jan_sls",StringType()),
            StructField("jan_stock",StringType()),
            StructField("feb_sls",StringType()),
            StructField("feb_stock",StringType()),
            StructField("mar_sls",StringType()),
            StructField("mar_stock",StringType()),
            StructField("apr_sls",StringType()),
            StructField("apr_stock",StringType()),
            StructField("may_sls",StringType()),
            StructField("may_stock",StringType()),
            StructField("jun_sls",StringType()),
            StructField("jun_stock",StringType()),
            StructField("jul_sls",StringType()),
            StructField("jul_stock",StringType()),
            StructField("aug_sls",StringType()),
            StructField("aug_stock",StringType()),
            StructField("sep_sls",StringType()),
            StructField("sep_stock",StringType()),
            StructField("oct_sls",StringType()),
            StructField("oct_stock",StringType()),
            StructField("nov_sls",StringType()),
            StructField("nov_stock",StringType()),
            StructField("dec_sls",StringType()),
            StructField("dec_stock",StringType())
            ])
        # Set the current session schema
        
        session.use_schema(stage_name.split(''.'')[0])

        
        sheet_dict={''LTM'': ''LOTTE MAIN'',''LTJ'': ''LOTTE JEJU'', ''SLM'': ''SHILLA MAIN'', ''SLJ'': ''SHILLA JEJU'',\\
            ''HDC'': ''HDC'', ''SGM'': ''SHINSEGAE MAIN'',''SGB'':''SHINSEGAE BUSAN'' ,''HYUNDAI_DDM'': ''HYUNDAI DDM'',\\
            ''HYUNDAI_COEX'': ''HYUNDAI COEX'', ''DONGWHA'': ''DONGWHA''}
                    
        # Looping thorugh each sheet 
        
        for retailer_name,location_name in sheet_dict.items():
            stage_path="@{0}/{1}/{2}.csv".format(stage_name,temp_stage_path,retailer_name)
            print(''stage_path printing....'',stage_path)

            try :
                df = session.read\\
                .schema(df_schema)\\
                .option("skip_header",5)\\
                .option("field_delimiter", "\\\\u0001")\\
                .csv(stage_path)
            except Exception as e:
                error_message = f"Error: Sheet {retailer_name} is missing in excel OR {str(e)}"
                
            df_filter=df.filter(col("rsp") != "")

            # Looping to get header to check if all months data is present
            for i in range(1,4):
                header_df = session.read.option("INFER_SCHEMA", True).option("field_delimiter", "\\\\u0001").csv(stage_path)
                header_pandas=header_df.to_pandas()
                h_key="header_"+str(i)
                h_val=header_pandas.iloc[int(i)].tolist()
                header_dict[h_key]=h_val
                

            # Checking if all months data is present 
            all_present = all(element in header_dict[''header_2''] for element in all_months)
            
            #Start processing only when all month data is present.
            
            if all_present:
                for  row in df_filter.collect():
                    for month,value in sls_stk_mth.items():
                        #print(month,''........'',value)
                        
                        new_row = row.as_dict()
                        new_row["month"] = month
                        new_row["sls_qty"] = row[value[0]]
                        new_row["stock_qty"] = row[value[1]]
                        
                        # Add retailer_name, year_month, and file_name
                        new_row["location_name"] = location_name
                        new_row["retailer_name"] = retailer_name
                        new_row["year"] = file_name.split(''_'')[1].split(''.'')[0]
                        new_row["file_name"] = file_name

                        final_row=Row(**new_row)
                         
                        # Append the transformed row to the list
                        transformed_rows.append(final_row)
                        
                
                transformed_df = session.createDataFrame(transformed_rows)
                print(''Transformed df created count is : '',transformed_df.count())
                if transformed_df.count() == 0:
                    raise Exception("The excel data file is empty for sheet : ",retailer_name,". Please place a valid file!")
                else :
                     pass


                main_df=transformed_df.select(''location_name'',''retailer_name'',''year'',''month'',''dcl_code'',''sap_code'',\\
                                                  ''reference'',''product_desc'',''size'',''rsp'',''c_sls_qty'',''c_sls_amt'',\\
                                                  ''c_stock_qty'',''c_stock_amt'',''buffer'',''mix'',''r_3m'',''comparison'',\\
                                                  ''sls_qty'',''stock_qty'',''file_name'')


                if not final_df:
                    final_df = main_df
                else:
                    final_df = final_df.unionByName(main_df)  


                print(''Cummulative count of final df in each iteration '',final_df.count())

                final_df= final_df.filter(final_df["dcl_code"].isNotNull())


                if final_df.count() == 0 :
                    raise Exception("The excel data file is empty ! . Please place a valid file!")
                else :
                    # Truncate and append in main table (table is truncated in ADF pipeline)
                    final_df.write.mode("append").saveAsTable(target_table)  
                    file_name=file_name+''_''+datetime.now().strftime("%Y%m%d%H%M%S")

                    final_df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/success/"+file_name,file_format_type="csv",header=True,OVERWRITE=True)
            else:
                raise Exception("Please check if we have all months data in excel")                 

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
