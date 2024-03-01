CREATE OR REPLACE PROCEDURE "CDFG_PREPROCESSING"("PARAM" ARRAY)
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
    #Param=[''CDFG_202301.xlsx'',''ASPSDL_RAW.DEV_LOAD_STAGE_ADLS'',''dev/transactional'',''sdl_rg_travel_retail_cdfg'']

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
        #truncate the table and write 
        final_df.write.mode("overwrite").saveAsTable(target_table)
        
        final_df.write.mode("append").saveAsTable(target_raw_table)
        
        #mobe to success
        final_df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/success/"+file_name,file_format_type="csv",header=True,OVERWRITE=True)
                     
        return ''Success''
    except Exception as e:
        # Handle exceptions here
        error_message = f"Error: {str(e)}"
        return error_message



#call ASPSDL_RAW.CDFG_PREPROCESSING([''CDFG_202301.xlsx'',''ASPSDL_RAW.DEV_LOAD_STAGE_ADLS'',
#''dev/transactional'',''sdl_rg_travel_retail_cdfg'']);';

-----------------------------------------------------------
CREATE OR REPLACE PROCEDURE "CNSC_PREPROCESSING"("PARAM" ARRAY)
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
        #Param= [''CNSC DR.CILABO SALES REPORT 202206.xlsx'',''ASPSDL_RAW.DEV_LOAD_STAGE_ADLS'',''dev/transactional/CNSC'',''sdl_rg_travel_retail_cnsc_test2'']
        
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
        
#call ASPSDL_RAW.CNSC_PREPROCESSING([''CNSC DR.CILABO SALES REPORT 202206.xlsx'',''ASPSDL_RAW.DEV_LOAD_STAGE_ADLS'',
#''dev/transactional/CNSC'',''sdl_rg_travel_retail_cnsc_test2'']);';

-----------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE "DFS_PREPROCESSING"("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS 'from snowflake.snowpark.functions import col, lit, date_format, current_timestamp, to_date, year, month, concat, format_number, regexp_replace,to_timestamp,when,trim,upper
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DecimalType,DateType
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz

def main(session: snowpark.Session,Param):
    #Param=["DFS_202009.csv","ASPSDL_RAW.DEV_LOAD_STAGE_ADLS","dev/transactional","sdl_rg_travel_retail_dfS"]
    try:
        # Extracting parameters from the input
        file_name = Param[0]
        stage_name = Param[1]
        temp_stage_path = Param[2]
        db_name = stage_name.split(''.'')[0]
        target_table = db_name + "." + Param[3]

        # Define the schema for the DataFrame
        df_schema = StructType([
            StructField("PRODUCT_DEPARTMENT", StringType()),
            StructField("BRAND", StringType()),
            StructField("PRODUCT_CLASS", StringType()),
            StructField("COMMON_SKU", StringType()),
            StructField("COMMON_SKU_STATUS", StringType()),
            StructField("COMMON_SKU_TYPE", StringType()),
            StructField("STYLE", StringType()),
            StructField("REGION_SKU", StringType()),
            StructField("VENDOR_STYLE", StringType()),
            StructField("METRICS", StringType()),
            StructField("LOCATION_NAME", StringType()),
            StructField("Sold_Qty1", StringType()),
            StructField("Gross_Sales1", StringType()),
            StructField("Sold_Qty2", StringType()),
            StructField("Gross_Sales2", StringType()),
            StructField("Sold_Qty3", StringType()),
            StructField("Gross_Sales3", StringType()),
            StructField("Sold_Qty4", StringType()),
            StructField("Gross_Sales4", StringType()),
            StructField("SOH_QTY", StringType()),
            StructField("T", StringType()),
        ])

        # Read the header for location names
        df_header = session.read \\
            .schema(df_schema) \\
            .option("skip_header", 5) \\
            .option("field_delimiter", "\\\\u0001") \\
            .option("field_optionally_enclosed_by", "\\"") \\
            .csv("@" + Param[1] + "/" + Param[2] + "/" + file_name)

        t = df_header.limit(1)
        result = t.select("LOCATION_NAME", "GROSS_SALES1", "GROSS_SALES2", "GROSS_SALES3").collect()
        locations = []
        for i in result:
            locations.append(i[0])
            locations.append(i[1])
            locations.append(i[2])
            locations.append(i[3])

        # Read the CSV file into a DataFrame
        df = session.read \\
            .schema(df_schema) \\
            .option("skip_header", 9) \\
            .option("field_delimiter", "\\u0001") \\
            .option("field_optionally_enclosed_by", "\\"") \\
            .csv("@" + Param[1] + "/" + Param[2] + "/" + file_name)

        # convert DataFrame to Pandas DF
        df_pandas = df.to_pandas()
        pd.set_option(''display.max_columns'', None)
        pd.set_option(''display.max_rows'', None)

        # Create an empty list to store the transformed rows
        # Iterate over each row and location to create transformed rows
        transformed_rows = []
        for index, row in df_pandas.iterrows():
            for location in locations:
                new_row = row.copy()
                new_row["location_name"] = location
                new_row["sls_qty"] = row[f"SOLD_QTY{locations.index(location) + 1}"]
                new_row["sls_amt"] = row[f"GROSS_SALES{locations.index(location) + 1}"]

                # Add retailer_name, year_month, and file_name
                new_row["retailer_name"] = file_name.split(''_'')[0]
                new_row["year_month"] = file_name.split(''_'')[1].split(''.'')[0]
                new_row["file_name"] = file_name.split(''.'')[0] + ".xlsx"

                transformed_rows.append(new_row)
                
        
        
        # Create a new DataFrame from the transformed rows
        transformed_df = pd.DataFrame(transformed_rows)
        transformed_df = transformed_df.reset_index(drop=True)
        final_df = transformed_df[["retailer_name", "year_month", "PRODUCT_DEPARTMENT", "BRAND", "PRODUCT_CLASS",
                                    "COMMON_SKU", "COMMON_SKU_STATUS", "COMMON_SKU_TYPE", "STYLE", "REGION_SKU",
                                    "VENDOR_STYLE", "METRICS", "location_name", "sls_qty", "sls_amt", "SOH_QTY", "file_name"]]
        # Create a Snowflake DataFrame
        snowdf = session.create_dataframe(final_df)
        snowdf= snowdf.filter(snowdf["COMMON_SKU"].isNotNull())
        
        if snowdf.count()==0 :
            return "No Data in file" 
        
        # Truncate the target table and append data to it
        trun_sql="Truncate table "+Param[1].split(".")[0]+"."+Param[3]
        session.sql(trun_sql).collect()
        snowdf.write.mode("append").saveAsTable(Param[1].split(".")[0]+"."+Param[3])
        #write to success folder
        file_name=file_name.split(".")[0]+"_"+datetime.now().strftime("%Y%m%d%H%M%S")
        snowdf.write.copy_into_location("@"+Param[1]+"/"+Param[2]+"/success/"+file_name,header=True,OVERWRITE=True)
       
    
        # Return value will appear in the Results tab.
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
		
--------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE "DUFRY_PREPROCESSING"("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS 'import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col,lit,date_format,current_timestamp,to_date,year,month,concat,to_timestamp,replace
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField,DecimalType
import pandas as pd
from datetime import datetime

def main(session: snowpark.Session,Param): 
    # Your code goes here, inside the "main" handler.

    try:
        
        #Param=[''Dufry_Hainan_122022.csv'',''DEV_DNA_LOAD.ASPSDL_RAW.DEV_LOAD_STAGE_ADLS'',''dev/transactional/Dufry'',''sdl_rg_travel_retail_dufry_hainan'']
    
    
    
        # Extracting parameters from the input
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        
        del_sql = "DELETE FROM " + target_table + " WHERE filename = ''" + (file_name.replace("_"," ").split(".")[0]+".xlsx")+ "''"
        session.sql(del_sql).collect()

        
        # Define the schema for the DataFrame
        df_schema=StructType([
            StructField("BRAND",StringType()),
            StructField("ean",StringType()),
            StructField("dcl_code",StringType()),
            StructField("product_desc",StringType()),
            StructField("online_qty",DecimalType(precision=38, scale=8)),
            StructField("online_gmv",DecimalType(precision=38, scale=8)),
            StructField("online_sales_split_per",StringType()),
            StructField("offline_qty",DecimalType(precision=38, scale=8)),
            StructField("offline_gmv",DecimalType(precision=38, scale=8)),
            StructField("offline_sales_split_per",StringType()),
            StructField("total_qty",DecimalType(precision=18, scale=0)),
            StructField("total_gmv",DecimalType(precision=38, scale=8))
            ])						
    
    
        # Read the CSV file into a DataFrame
        
        df = session.read\\
        .schema(df_schema)\\
        .option("skip_header",2)\\
        .option("field_delimiter", "\\u0001")\\
        .option("field_optionally_enclosed_by", "\\"") \\
        .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
    
        
        
        #create the column which are derived from file name
    
        df= df.with_column("year",lit(file_name[15:19]))
        df=df.with_column("month", lit(file_name[13:15]))
        df=df.with_column("yearmo", concat(lit(file_name[15:19]), lit(Param[0][13:15])))
        df = df.with_column("Retailer_name",lit("DUFRY"))
        df=df.with_column("filename",lit(file_name.replace("_"," ").split(".")[0]+".xlsx"))
        df=df.with_column("BRAND",col("brand"))
        df=df.with_column("stock",lit(None))
        df = df.withColumn("stock", col("stock").cast(DecimalType(precision=18, scale=0)))
        df=df.with_column("crtddt",to_timestamp(current_timestamp()))
     
    
        #correct the order of clumns
        snowdf=df.select("year","month","yearmo","retailer_name","ean","dcl_code","product_desc","online_qty","online_gmv","online_sales_split_per","offline_qty","offline_gmv","offline_sales_split_per","total_qty","total_gmv","filename","brand","crtddt","stock")
        snowdf= snowdf.filter(snowdf["ean"].isNotNull())
        if snowdf.count()==0 :
            return "No Data in file" 
        
        #move file into success folder
        snowdf.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/success/"+file_name,file_format_type="csv",header=True,OVERWRITE=True)
    
        #write on sdl layer
        snowdf.write.mode("append").saveAsTable(target_table)
        
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
			
-------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE "FILE_VALIDATION"("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them.

import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col
import math

def main(session: snowpark.Session,Param): 

    # Your code goes here, inside the "main" handler.
    # Return value will appear in the Results tab
    #Shilla_202201 Without2ndColumnHeaders SC8
    # ********   Variable  we need from ETL table : 
    # CURRENT_FILE , index , validation, val_file_name,val_file_extn

    CURRENT_FILE        =  Param[0]
    index               =  Param[1]
    validation          =  Param[2]
    val_file_name       =  Param[3]
    val_file_extn       =  Param[4]
    val_header          =  Param[5]
    file_header_row_num	=  Param[6]
    stage_name     		=  Param[7]
    temp_stage_path		=  Param[8]

    FileNameValidation,FileExtnValidation,FileHeaderValidation = validation.split("-")
    counter             =  0 

    # If the File belongs to Regional, then it enters the function

    if stage_name.split(".")[0]=="ASPSDL_RAW":
        CURRENT_FILE=rg_travel_validation(CURRENT_FILE)

    #Extracting the filename based on index variable
        
    if index.lower() == "last":
        extracted_filename = CURRENT_FILE.rsplit("_", 1)[0]
    elif index.lower() == "first":
        extracted_filename = CURRENT_FILE.split("_")[0]
    elif index.lower() == "full":
        extracted_filename = CURRENT_FILE.rsplit(".", 1)[0]


    # Check for File name Validation
    
    if FileNameValidation=="1":
        file_name_validation_status,counter=file_validation(counter,extracted_filename,val_file_name)
    else :
        print("File Name Validation not required")


    # Check for  File extension validation

    if FileExtnValidation == "1":
        file_ext_validation_status,counter=file_extn_validation(counter,CURRENT_FILE,val_file_extn)
    else:
        print("File extension Validation not required")


    # Check for File Header Validation
    
    if FileHeaderValidation == "1":

        # Converting the extension from xlsx to csv
        # Extracting the Header from the file
	
        file_name= CURRENT_FILE.replace("xlsx","csv")
        df = session.read.option("INFER_SCHEMA", True).option("field_optionally_enclosed_by", "\\"").csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
        
        df_pandas=df.to_pandas()
        header=df_pandas.iloc[int(file_header_row_num)].tolist()

        # If the source is of xlsx type, then splitting based on \\x01 delimiter
        
        header_pipe_split = header[0].split(''|'')
        if val_file_extn==''xlsx'':
            result_list = header[0].split(''\\x01'')
        elif len(header_pipe_split)>1:
            result_list = header_pipe_split
        else:
            result_list = header
        result_list=list(filter(None,result_list))
        filtered_list = [value for value in result_list if value is not None and not (isinstance(value, float) and math.isnan(value))]
        file_header= [item.replace(" ", "_").replace(".", "_") for item in filtered_list]
        val_header= val_header.lower()

        # If the Header from Metadata is of comma separated or | separated then split accordingly
        
        comma_split = val_header.split('','')
        if len(comma_split) > 1:
            final_val_header=comma_split

        pipe_split = val_header.split(''|'')
        if len(pipe_split) > 1:
            final_val_header=pipe_split
        
        file_header_validation_status,counter=file_header_validation(counter,final_val_header,file_header)
        
    else:
        print("File Header Validation not required")


    if counter == 0 :
            validation_status = "SUCCESS: File validation passed" 
    elif counter == 1 :
            validation_status = "FAILED: {0}".format(file_name_validation_status)
    elif counter == 2 :
            validation_status = "FAILED: {0}".format(file_ext_validation_status)
    elif counter == 3 :
            validation_status = "FAILED: {0};{1}".format(file_name_validation_status,file_ext_validation_status)
    elif counter == 4 :
            validation_status = "FAILED: {0}".format(file_header_validation_status)
    elif counter == 5:
            validation_status = "FAILED: {0},{1}".format(file_name_validation_status,file_header_validation_status)
    elif counter == 6:
            validation_status = "FAILED: {0},{1}".format(file_ext_validation_status,file_header_validation_status)
    else :
            validation_status = "FAILED: {0};{1};{2}".format(file_name_validation_status,file_ext_validation_status,file_header_validation_status)
    
    return validation_status


def rg_travel_validation(CURRENT_FILE):
    #assigning the value to varibale file
    
    if "CNSC" in CURRENT_FILE:
            fileA = CURRENT_FILE.replace(" ", "_")
            file = fileA.replace("_", "_",1)
            print("FileName : ", file)
    elif "Dufry" in CURRENT_FILE:
            fileA = CURRENT_FILE.replace(" ", "_")
            file = fileA.replace("_", " ",1)
            print("FileName : ", file)
    elif "Vendor" in CURRENT_FILE:
            fileA = CURRENT_FILE.replace(" ", "_")
            file = fileA.replace("_", "_",1)
            print("FileName : ", file)
    elif "LSTR" in CURRENT_FILE:
            file = CURRENT_FILE.replace(" ", "_")
            print("FileName : ", file)
    else:
            file = CURRENT_FILE
            print("FileName : ", file)

    return file


# Function to Perform File name validation

def file_validation(counter,extracted_filename,val_file_name):

        if extracted_filename.upper() == val_file_name.upper():
            file_name_validation_status=""
            print("file_name_validation_status is successful")
        else:
            file_name_validation_status="Invalid File Name"
            print("file_name_validation_status",file_name_validation_status)
            counter = 1
        return file_name_validation_status,counter
    

# Function to perform file extension validation

def file_extn_validation(counter,CURRENT_FILE,val_file_extn):
    
        current_file_extn = CURRENT_FILE.split(".")[-1]
        if current_file_extn.upper() == val_file_extn.upper():
            file_ext_validation_status=""
            print("file_ext_validation_status is successful")
        else:
            file_ext_validation_status="Invalid File Extension"
            print("file_ext_validation_status",file_ext_validation_status)
            counter = counter+2
        return file_ext_validation_status,counter

def file_header_validation(counter,final_val_header,file_header):

        # Compare the header from file and the header from metadata
        # Adding the counter flag as a double check to the comparision
        # Moving the failed header to a list and displaying it as part of Error message
    
        val_header_count=len(final_val_header)
        file_header_count=0
        l3=[]
        l2=[x.lower() for x in final_val_header]
        for i in file_header:
            if i.lower() in l2:
                file_header_count+=1
            else:
                l3.append(i)
        if file_header_count == val_header_count and not l3:
            file_header_validation_status="Success"
            print("file_header_validation_status is successful")
        else:
            file_header_validation_status="Header validation Failed"+" and the unmatched columns are "+ str(l3)
            print("file_header_validation_status",file_header_validation_status)
            counter = counter+4
        return file_header_validation_status,counter


            





    
        

    ';


-------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE "HAINAN_PREPROCESSING"("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS 'import snowflake.snowpark as snowpark
from snowflake import snowpark
from snowflake.snowpark.functions import col,lit,current_timestamp,to_date,year,month,to_timestamp
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField,DecimalType
import pandas as pd
from datetime import datetime

    
def main(session: snowpark.Session,Param): 
    # Your code goes here, inside the "main" handler.
    #Param=[''Hainan_Vendor_Sales_Report_for_Asian.csv'',''ASPSDL_RAW.DEV_LOAD_STAGE_ADLS'',''dev/transactional/dfs'',''sdl_rg_travel_retail_dfS_hainan'']
    try:
        # Extracting parameters from the input
        file_name       = Param[0]
        stage_name      = Param[1]
        adls_path       = Param[2]
        db_name         = stage_name.split(".")[0]
        target_table    = db_name+"."+Param[3]

        # Define the schema for the DataFrame
        df_schema=StructType([
            StructField("product_department_desc",StringType()),
            StructField("product_department_code",StringType()),
            StructField("brand",StringType()),
            StructField("product_class_desc",StringType()),
            StructField("product_class_code",StringType()),
            StructField("product_subclass_desc",StringType()),
            StructField("product_subclass_code",StringType()),
            StructField("brand_collection",StringType()),
            StructField("reatiler_product_code",StringType()),
            StructField("reatiler_product_description",StringType()),
            StructField("dcl_code",StringType()),
            StructField("ean",StringType()),
            StructField("style_type_code",StringType()),
            StructField("month",StringType()),
            StructField("door_name",StringType()),
            StructField("sls_mtd_qty",DecimalType(precision=18, scale=0)),
            StructField("sls_mtd_amt",DecimalType(precision=38, scale=18)),
            StructField("sls_ytd_qty",DecimalType(precision=18, scale=0)),
            StructField("sls_ytd_amt",DecimalType(precision=38, scale=18))
            ])
        
        # Read the CSV file into a DataFrame
        df = session.read \\
            .schema(df_schema) \\
            .option("skip_header", 2) \\
            .option("field_delimiter", "\\u0001") \\
            .option("field_optionally_enclosed_by", "\\"") \\
            .csv("@" + Param[1] + "/" + Param[2] + "/" + file_name)

        # Add retailer_name, crttd, and file_name,yearmo,year,mon
        df = df.with_column("Retailer_name",lit("DFS HAINAN"))
        df = df.with_column("filename",lit(file_name.split(".")[0].replace("_"," ")+".xlsx"))
        df = df.withColumn("crttd", lit(to_timestamp(current_timestamp())))
        df = df.withColumn("yearmo", to_date(col("month"), ''Mon yy''))
        df = df.withColumn("year", year(col("yearmo")))
        df = df.withColumn("mon", month(col("yearmo")))
        
        snowdf=df.select("year","mon","yearmo","Retailer_name","product_department_desc","product_department_code","brand","product_class_desc",
                  "product_class_code","product_subclass_desc","product_subclass_code","brand_collection","reatiler_product_code","reatiler_product_description",
                  "dcl_code","ean","style_type_code","month","door_name","sls_mtd_qty","sls_mtd_amt","sls_ytd_qty","sls_ytd_amt","filename","crttd")
        
        snowdf = snowdf[snowdf["reatiler_product_code"].isNotNull()]
        if snowdf.count()==0 :
            return "No Data in file" 
        # Create a Snowflake DataFrame and write to success folder
        
        # for DFS HAINAN, we dont have truncate and load into sdl table. we have append logic  
        
        snowdf.write.mode("append").saveAsTable(target_table)

        file_name=file_name.split(".")[0]+''_''+datetime.now().strftime("%Y%m%d%H%M%S")
        snowdf.write.copy_into_location("@"+stage_name+"/"+adls_path+"/success/"+file_name,file_format_type="csv",header=True,OVERWRITE=True)
    
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
			
---------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE "LSTR_PREPROCESSING"("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS 'import snowflake.snowpark as snowpark
from snowflake import snowpark
from snowflake.snowpark.functions import col,lit,date_format,current_timestamp,to_timestamp
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField,DecimalType
import pandas as pd
from datetime import datetime

def main(session: snowpark.Session,Param):
    
    try:

    # Extracting parameters from the input
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]

    
    # Define the schema for the DataFrame
        df_schema=StructType([
            StructField("BRAND_NAME",StringType()),
            StructField("EAN",StringType()),
            StructField("DCL_CODE",StringType()),
            StructField("ENGLISH_DESC",StringType()),
            StructField("CHINESE_DESC",StringType()),
            StructField("CATEGORY",StringType()),
            StructField("SRP_USD",DecimalType(precision=18, scale=0)),
            StructField("SLS_QTY_TOTAL",DecimalType(precision=18, scale=0)),
            StructField("SLS_AMT_TOTAL",DecimalType(precision=38, scale=2)),
            StructField("OFFLINE_QTY",DecimalType(precision=18, scale=0)),
            StructField("OFFLINE_AMT",DecimalType(precision=38, scale=2)),
            StructField("ONLINE_QTY",DecimalType(precision=18, scale=0)),
            StructField("ONLINE_AMT",DecimalType(precision=38, scale=2)),
            StructField("STOCK",DecimalType(precision=18, scale=0))
            ])
        
    # Set the current session schema
        #session.use_schema(stage_name.split(''.'')[0])

    # Read the CSV file into a DataFrame
    
        df = session.read\\
        .schema(df_schema)\\
        .option("skip_header",3)\\
        .option("field_delimiter", "\\u0001")\\
        .option("field_optionally_enclosed_by", "\\"")\\
        .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)


     # Filter null values if any
        
        df=df.filter(col("DCL_CODE").is_not_null()) and df.filter(col("ENGLISH_DESC").is_not_null())

    # Convert the Dataframe into pandas Dataframe
        df_pandas=df.to_pandas()
        #pd.set_option(''display.max_columns'', None)

    #---------------------------Transformation logic ------------------------------

    # Add RETAILER_NAME, FILENAME, YEAR, MONTH, YEARMO, CRTTD
        a=file_name.split(''_'')[1].split(''.'')[0]
        YEARMO=str(a[2:6])+str(a[:2])
        df_pandas[''RETAILER_NAME'']="LAGADERE"
        df_pandas[''FILENAME'']=file_name.split(''.'')[0].replace("_"," ")+".xlsx"
        df_pandas[''YEARMO'']=YEARMO
        df_pandas[''YEAR'']=file_name.split(''_'')[1].split(''.'')[0][2:6]
        df_pandas[''MONTH'']=file_name.split(''_'')[1].split(''.'')[0][:2]

        

    # Create a new DataFrame from the transformed rows
        final_df=df_pandas[["YEAR","MONTH","YEARMO","RETAILER_NAME","BRAND_NAME","EAN","DCL_CODE","ENGLISH_DESC","CHINESE_DESC","CATEGORY","SRP_USD","SLS_QTY_TOTAL","SLS_AMT_TOTAL","OFFLINE_QTY","OFFLINE_AMT","ONLINE_QTY","ONLINE_AMT","STOCK","FILENAME"]]
        

        # Create a Snowflake DataFrame 
        snowdf=session.create_dataframe(final_df)
        snowdf= snowdf.filter(snowdf["EAN"].isNotNull())
        if snowdf.count()==0 :
            return "No Data in file" 
        snowdf = snowdf.withColumn("CRTTD",lit(to_timestamp(current_timestamp())))
        
        # Load Data to the target table
        snowdf.write.mode("append").saveAsTable(target_table)

        # write to success folder
        file_name=file_name.split(".")[0]+''_''+datetime.now().strftime("%Y%m%d%H%M%S")
        snowdf.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/success/"+file_name,file_format_type="csv",header=True,OVERWRITE=True)
        
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
	
------------------------------------------------------------
CREATE OR REPLACE PROCEDURE "SALESSTOCK_PREPROCESSING"("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS CALLER
AS '
import snowflake.snowpark as snowpark
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField
from snowflake.snowpark.functions import col
import pandas as pd
from datetime import datetime
from snowflake.snowpark import Row

def main(session: snowpark.Session,Param): 
    
    try:
        # Extracting parameters from the input

        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        db_name         = stage_name.split(''.'')[0]
        target_table    = db_name+"."+Param[3]
        target_raw_table= target_table+"_raw"
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

        
        sheet_dict={''LTM'': ''LOTTE MAIN'', ''LTJ'': ''LOTTE JEJU'', ''SLM'': ''SHILLA MAIN'', ''SLJ'': ''SHILLA JEJU'',\\
                    ''HDC'': ''HDC'', ''SGM'': ''SHINSEGAE MAIN'', ''SGB'': ''SHINSEGAE BUSAN'', ''HYUNDAI_DDM'': ''HYUNDAI DDM'',\\
                    ''HYUNDAI_COEX'': ''HYUNDAI COEX'', ''DONGWHA'': ''DONGWHA''}
        
        for retailer_name,location_name in sheet_dict.items():
            stage_path="@{0}/{1}/{2}.csv".format(stage_name,temp_stage_path,retailer_name)

            df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",5)\\
            .option("field_delimiter", "\\\\u0001")\\
            .csv(stage_path)

            df_filter=df.filter(col("rsp") != "")

            for i in range(1,4):
                header_df = session.read.option("INFER_SCHEMA", True).option("field_delimiter", "\\\\u0001").csv(stage_path)
                header_pandas=header_df.to_pandas()
                h_key="header_"+str(i)
                h_val=header_pandas.iloc[int(i)].tolist()
                header_dict[h_key]=h_val
                print(''Printing Header df'')
                print(header_dict)

            #df_pandas=df_filter.toPandas()
            df_pandas=pd.DataFrame(df_filter.collect())
            pd.set_option(''display.max_columns'', None)
                
            #print(header_2)
            print(''show df below'')
            df_filter.show(5)
            print(''collect is printed below'',df_filter.collect())
            print(''show pdf below'')
            df_pandas.head(5)
            print(''pandas count'')
            df_pandas.count()

            all_present = all(element in header_dict[''header_2''] for element in all_months)
            print(all_present,''......... result'')
            
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
                        
                print(''Transformed df..........'')
                transformed_df = session.createDataFrame(transformed_rows)
                #transformed_df = transformed_df.reset_index(drop=True)
                
                print(''Transformed df done..........'')


                filtered_df=transformed_df.select(''location_name'',''retailer_name'',''year'',''month'',''dcl_code'',''sap_code'',\\
                                                  ''reference'',''product_desc'',''size'',''rsp'',''c_sls_qty'',''c_sls_amt'',\\
                                                  ''c_stock_qty'',''c_stock_amt'',''buffer'',''mix'',''r_3m'',''comparison'',\\
                                                  ''sls_qty'',''stock_qty'',''file_name'') 

                if not final_df:
                    final_df = filtered_df
                else:
                    final_df = final_df.unionByName(filtered_df)  
                    
                                                  
        final_df.write.mode("overwrite").saveAsTable(target_table)

        final_df.write.mode("append").saveAsTable(target_raw_table)

        return ''Success''

    except KeyError as key_error:
        # Handle KeyError (missing columns) here
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message

    
    except Exception as e:
        # Handle exceptions here
        error_message = f"Error: {str(e)}"
        return error_message
';
---------------------------------------------------------------
CREATE OR REPLACE PROCEDURE "SHILLA_PREPROCESSING"("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS CALLER
AS '

import snowflake.snowpark as snowpark
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField
from snowflake.snowpark.functions import col
import pandas as pd
from datetime import datetime



def main(session: snowpark.Session, Param): 
    
    try:
        # Extracting parameters from the input

        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        db_name         = stage_name.split(''.'')[0]
        target_table    = db_name+"."+Param[3]
        
    
        # Define the schema for the DataFrame
        df_schema=StructType([
            StructField("Brand",StringType()),
            StructField("SKU",StringType()),
            StructField("Desc",StringType()),
            StructField("Ref_No",StringType()),
            StructField("EAN_UPC",StringType()),
            StructField("Color",StringType()),
            StructField("Sold_Qty1",StringType()),
            StructField("Gross_Sales1",StringType()),
            StructField("Sold_Qty2",StringType()),
            StructField("Gross_Sales2",StringType()),
            StructField("Sold_Qty3",StringType()),
            StructField("Gross_Sales3",StringType()),
            StructField("Sold_Qty4",StringType()),
            StructField("Gross_Sales4",StringType()),
            StructField("Sold_Qty5",StringType()),
            StructField("Gross_Sales5",StringType())
            ])
        # Set the current session schema
        
        session.use_schema(stage_name.split(''.'')[0])
        
        # Read the CSV file into a DataFrame
        df = session.read\\
        .schema(df_schema)\\
        .option("skip_header",2)\\
        .option("field_delimiter", "\\u0001")\\
        .option("field_optionally_enclosed_by", "\\"") \\
        .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
    
        # Read the header for location names
        df_header = session.read\\
        .schema(df_schema)\\
        .option("skip_header",0)\\
        .option("field_delimiter", "\\u0001")\\
        .option("field_optionally_enclosed_by", "\\"") \\
        .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
        
        # Extract location names from the header
        t=df_header.limit(1)
        result=t.select("Sold_Qty1","Sold_Qty2","Sold_Qty3","Sold_Qty4","Sold_Qty5").collect()
        # Create a list of location names
        locations=[]
        for i in result:
            locations.append(i[0])
            locations.append(i[1])
            locations.append(i[2])
            locations.append(i[3])
            locations.append(i[4])
       
        
        # Filter out rows with Brand ''Total'' and convert DataFrame to Pandas
        
        df_filter=df.filter(col("Brand") != ''Total'')
        df=df.filter(col("Brand").is_not_null()) and df.filter(col("SKU").is_not_null())
        df_pandas=df_filter.to_pandas()
        pd.set_option(''display.max_columns'', None)
        
            #---------------------------Transformation logic ------------------------------
        
      
        
        # Create an empty list to store the transformed rows
        # Iterate over each row and location to create transformed rows
        transformed_rows = []
    
        for index, row in df_pandas.iterrows():
        # Iterate over each location
            for location in locations:
                # Create a copy of the original row
                new_row = row.copy()
                
                # Add location-specific information
                new_row["location_name"] = location
                new_row["sls_qty"] = row[f"SOLD_QTY{locations.index(location) + 1}"]
                new_row["sls_amt"] = row[f"GROSS_SALES{locations.index(location) + 1}"]
                
                # Add retailer_name, year_month, and file_name
                new_row["retailer_name"] = file_name.split(''_'')[0]
                new_row["year_month"] = file_name.split(''_'')[1].split(''.'')[0]
                new_row["file_name"] = file_name.split(''.'')[0]+".xlsx"
                
                # Append the transformed row to the list
                transformed_rows.append(new_row)
    
        # Create a new DataFrame from the transformed rows
        transformed_df = pd.DataFrame(transformed_rows)
        transformed_df = transformed_df.reset_index(drop=True)
        final_df=transformed_df[["retailer_name","year_month","BRAND","SKU","DESC","REF_NO","EAN_UPC","COLOR","location_name","sls_qty","sls_amt","file_name"]]

    
      
        # Create a Snowflake DataFrame and write to success folder
        snowdf=session.create_dataframe(final_df)
        
        # Truncate the target table and append data to it
        trun_sql="Truncate table "+target_table
        session.sql(trun_sql).collect()
        snowdf.write.mode("append").saveAsTable(target_table)
        
        file_name=file_name.split(".")[0]+''_''+datetime.now().strftime("%Y%m%d%H%M%S")
        snowdf.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/success/"+file_name,file_format_type="csv",header=True,OVERWRITE=True)
        
                     
        return ''Success''

    except KeyError as key_error:
        # Handle KeyError (missing columns) here
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message
        
    except pd.errors.MergeError as merge_error:
        # Handle DataFrame merging error
        error_message = f"DataFrame merging error: {str(merge_error)}"
        return error_mess
    
    except Exception as e:
        # Handle exceptions here
        error_message = f"Error: {str(e)}"
        return error_message
';