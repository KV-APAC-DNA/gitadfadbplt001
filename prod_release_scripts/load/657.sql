Use schema THASDL_RAW;
CREATE OR REPLACE PROCEDURE SDL_POP6_TH_PRODUCTS_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS 'from snowflake.snowpark.functions import col, lit, date_format, current_timestamp, to_date,md5, year, month, concat, format_number, regexp_replace, to_timestamp, when, trim, upper
from snowflake.snowpark.types import StringType, StructType, StructField,DecimalType
import snowflake.snowpark
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz

def main(session: snowpark.Session,Param):
    #Param=["20240707_products.csv","THASDL_RAW.DEV_LOAD_STAGE_ADLS","prd/pop6/master/masterdata","sdl_pop6_th_products"]
    try:
        file_name = Param[0]
        stage_name = Param[1]
        temp_stage_path = Param[2]
        target_table = Param[3]

        # df_schema = StructType([
        #                         StructField("Status", StringType(), True),
        #                         StructField("ProductDB_ID", StringType(), True),
        #                         StructField("Barcode", StringType(), True),
        #                         StructField("SKU", StringType(), True),
        #                         StructField("Unit_Price", StringType(), True),
        #                         StructField("Display_order", StringType(), True),
        #                         StructField("Launch_Date", StringType(), True),
        #                         StructField("Largest_UOM_Quantity", StringType(), True),
        #                         StructField("Middle_UOM_Quantity", StringType(), True),
        #                         StructField("Smallest_UOM_Quantity", StringType(), True),
        #                         StructField("SKU_(English)", StringType(), True),
        #                         StructField("Company", StringType(), True),
        #                         StructField("SKU_Code", StringType(), True),
        #                         StructField("PS_Category", StringType(), True),
        #                         StructField("PS_Segment", StringType(), True),
        #                         StructField("PS_Category_Segment", StringType(), True),
        #                         StructField("Country_L1", StringType(), True),
        #                         StructField("Regional_Franchise_L2", StringType(), True),
        #                         StructField("Franchise_L3", StringType(), True),
        #                         StructField("Brand_L4", StringType(), True),
        #                         StructField("Sub_Category_L5", StringType(), True),
        #                         StructField("Platform_L6", StringType(), True),
        #                         StructField("Variance_L7", StringType(), True),
        #                         StructField("Pack_Size_L8", StringType(), True)
        #                     ])
        df = session.read.option("INFER_SCHEMA", True)\\
            .option("field_delimiter", ",")\\
            .option("field_optionally_enclosed_by", "\\"") \\
            .option("trim_space", True) \\
            .with_metadata("METADATA$FILE_ROW_NUMBER") \\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        df_columns=df.first()[1:]
        
        df = df.filter(col("METADATA$FILE_ROW_NUMBER") > lit(1))
        
        
        df = df.drop("METADATA$FILE_ROW_NUMBER")
        #return df.schema.names
        o=0

        for i in df_columns:
            

            df=df.withColumnRenamed(df.schema.names[o],i.replace(" ", "_").replace("(","").replace(")","").replace("]","").replace("[",""))
            o+=1
        
              
        df = df.na.drop("all")
        if df.count() == 0:
            return "No Data in file"

        
        df = df.withColumn("filename", lit(file_name))
        df = df.withColumn("crt_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        df = df.withColumn("run_id", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))

        df1=df
    
        concat_columns =  [
                            "Status",
                            "ProductDB_ID",
                            "Barcode",
                            "SKU",
                            "Unit_Price",
                            "Display_order",
                            "Launch_Date",
                            "Largest_UOM_Quantity",
                            "Middle_UOM_Quantity",
                            "Smallest_UOM_Quantity",
                            "SKU_English",  # Parentheses removed
                            "Company",
                            "SKU_Code",
                            "PS_Category",
                            "PS_Segment",
                            "PS_Category_Segment",
                            "Country_-_L1",
                            "Regional_Franchise-L2",
                            "Franchise_-_L3",
                            "Brand_-_L4",
                            "Sub_Category_-_L5",
                            "Platform_-_L6",
                            "Variance_-_L7",
                            "Pack_Size_-_L8"
                        ]
        
        df1=df1.na.fill("",subset=concat_columns)


       # Concatenate the columns
        df1 = df1.withColumn("hash_c", concat(*[df1[col] for col in concat_columns]))

        #df1 = df1.withColumn("hash_c",concat(df1.Product_List,df1.ProductDB_ID,df1.SKU,df1.MSL_Ranking,df1.Date))
        df1=df1.withColumn("hash",md5(col("hash_c")))
        df1=df1.na.replace({"":None},concat_columns)
        snowdf = df1.select(
                        [
                            "Status",
                            "ProductDB_ID",
                            "Barcode",
                            "SKU",
                            "Unit_Price",
                            "Display_order",
                            "Launch_Date",
                            "Largest_UOM_Quantity",
                            "Middle_UOM_Quantity",
                            "Smallest_UOM_Quantity",
                            "SKU_English",  # Parentheses removed
                            "Company",
                            "SKU_Code",
                            "PS_Category",
                            "PS_Segment",
                            "PS_Category_Segment",
                            "Country_-_L1",
                            "Regional_Franchise-L2",
                            "Franchise_-_L3",
                            "Brand_-_L4",
                            "Sub_Category_-_L5",
                            "Platform_-_L6",
                            "Variance_-_L7",
                            "Pack_Size_-_L8",
                            "filename",
                            "run_id",
                            "crt_dttm",
                            "hash"]
                        )
        
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        
        #move to success
        
        snowdf.write.mode("append").saveAsTable(stage_name.split(".")[0] + "." + target_table)
        #snowdf.write.mode("append").saveAsTable(target_table)
        file_name1 = file_name.split(".")[0] + "_" + datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S") + ".csv"
        snowdf.write.copy_into_location("@" + stage_name + "/" + temp_stage_path + "/processed/success/" + formatted_year + "/" + formatted_month + "/" + file_name1, header=True, OVERWRITE=True)
        
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
CREATE OR REPLACE PROCEDURE SDL_POP6_TH_USERS_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS 'from snowflake.snowpark.functions import col, lit, date_format, current_timestamp, to_date,md5, year, month, concat, format_number, regexp_replace, to_timestamp, when, trim, upper
from snowflake.snowpark.types import StringType, StructType, StructField,DecimalType
import snowflake.snowpark
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz

def main(session: snowpark.Session,Param):
    #Param=["20240423_users.csv","THASDL_RAW.DEV_LOAD_STAGE_ADLS","dev/pop6","sdl_pop6_th_users"]
    try:
        file_name = Param[0]
        stage_name = Param[1]
        temp_stage_path = Param[2]
        target_table = Param[3]

        
        df = session.read.option("INFER_SCHEMA", True)\\
            .option("field_delimiter", ",")\\
            .option("field_optionally_enclosed_by", "\\"") \\
            .option("trim_space", True) \\
            .with_metadata("METADATA$FILE_ROW_NUMBER") \\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        df_columns=df.first()[1:]
        
        df = df.filter(col("METADATA$FILE_ROW_NUMBER") > lit(1))
        
        
        df = df.drop("METADATA$FILE_ROW_NUMBER")
        #return df.schema.names
        o=0

        for i in df_columns:
            

            df=df.withColumnRenamed(df.schema.names[o],i.replace(" ", "_").replace("(","").replace(")","").replace("]","").replace("[",""))
            o+=1
        
              
        df = df.na.drop("all")
        if df.count() == 0:
            return "No Data in file"

        
        df = df.withColumn("filename", lit(file_name))
        df = df.withColumn("crt_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        df = df.withColumn("run_id", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))

        df1=df
        
        concat_columns =  [
                            "Status", "UserDB_ID", "Username", "First_Name", "Last_Name", "Team",
                            "Superior_Name", "Authorisation_Group", "Email_Address", "Longitude",
                            "Latitude", "Business_Units_ID", "Business_Unit_Name"
                        ]
                                
        df1=df1.na.fill("",subset=concat_columns)


       # Concatenate the columns
        df1 = df1.withColumn("hash_c", concat(*[df1[col] for col in concat_columns]))

        #df1 = df1.withColumn("hash_c",concat(df1.Product_List,df1.ProductDB_ID,df1.SKU,df1.MSL_Ranking,df1.Date))
        df1=df1.withColumn("hash",md5(col("hash_c")))
        df1=df1.na.replace({"":None},concat_columns)
        snowdf = df1.select(
                        "Status", "UserDB_ID", "Username", "First_Name", "Last_Name", "Team",
                            "Superior_Name", "Authorisation_Group", "Email_Address", "Longitude",
                            "Latitude", "Business_Units_ID", "Business_Unit_Name",
                            "filename",
                            "run_id",
                            "crt_dttm",
                            "hash"
                        )
        
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        
        #move to success
        
        snowdf.write.mode("append").saveAsTable(stage_name.split(".")[0] + "." + target_table)
        #snowdf.write.mode("append").saveAsTable(target_table)
        file_name1 = file_name.split(".")[0] + "_" + datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S") + ".csv"
        snowdf.write.copy_into_location("@" + stage_name + "/" + temp_stage_path + "/processed/success/" + formatted_year + "/" + formatted_month + "/" + file_name1, header=True, OVERWRITE=True)
        
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
CREATE OR REPLACE PROCEDURE SDL_POP6_TH_POPS_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS 'from snowflake.snowpark.functions import col, lit, date_format, current_timestamp, to_date,md5, year, month, concat, format_number, regexp_replace, to_timestamp, when, trim, upper
from snowflake.snowpark.types import StringType, StructType, StructField,DecimalType
import snowflake.snowpark
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz

def main(session: snowpark.Session,Param):
    #Param=["20240423_pops.csv","THASDL_RAW.DEV_LOAD_STAGE_ADLS","dev/pop6","sdl_pop6_th_pops"]
    try:
        file_name = Param[0]
        stage_name = Param[1]
        temp_stage_path = Param[2]
        target_table = Param[3]

        
        df = session.read.option("INFER_SCHEMA", True)\\
            .option("field_delimiter", ",")\\
            .option("field_optionally_enclosed_by", "\\"") \\
            .option("trim_space", True) \\
            .with_metadata("METADATA$FILE_ROW_NUMBER") \\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        df_columns=df.first()[1:]
        
        df = df.filter(col("METADATA$FILE_ROW_NUMBER") > lit(1))
        
        
        df = df.drop("METADATA$FILE_ROW_NUMBER")
        #return df.schema.names
        o=0

        for i in df_columns:
            

            df=df.withColumnRenamed(df.schema.names[o],i.replace(" ", "_").replace("(","").replace(")","").replace("]","").replace("[",""))
            o+=1
        
              
        df = df.na.drop("all")
        if df.count() == 0:
            return "No Data in file"

        
        df = df.withColumn("filename", lit(file_name))
        df = df.withColumn("crt_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        df = df.withColumn("run_id", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))

        df1=df
        
        concat_columns = [
                        "Status", "POPDB_ID", "POP_Code", "POP_Name", "Address", "Longitude", "Latitude",
                        "Business_Units_ID", "Business_Unit_Name", "Country", "Channel", "Retail_Environment_PS",
                        "Sales_Group_Name", "Sales_Group_Code", "Customer", "Customer_Grade",
                        "External_Store_Code", "Territory/Region"
                    ]
        
        df1=df1.na.fill("",subset=concat_columns)


       # Concatenate the columns
        df1 = df1.withColumn("hash_c", concat(*[df1[col] for col in concat_columns]))

        #df1 = df1.withColumn("hash_c",concat(df1.Product_List,df1.ProductDB_ID,df1.SKU,df1.MSL_Ranking,df1.Date))
        df1=df1.withColumn("hash",md5(col("hash_c")))
        df1=df1.na.replace({"":None},concat_columns)
        snowdf = df1.select(
                          "Status", "POPDB_ID", "POP_Code", "POP_Name", "Address", "Longitude", "Latitude",
                            "Business_Units_ID", "Business_Unit_Name", "Country", "Channel", "Retail_Environment_PS",
                            "Sales_Group_Name","Customer", "Sales_Group_Code",  "Customer_Grade",
                            "External_Store_Code", "Territory/Region",
                            "filename",
                            "run_id",
                            "crt_dttm",
                            "hash"
                        )
        
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        
        #move to success
        
        snowdf.write.mode("append").saveAsTable(stage_name.split(".")[0] + "." + target_table)
        #snowdf.write.mode("append").saveAsTable(target_table)
        file_name1 = file_name.split(".")[0] + "_" + datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S") + ".csv"
        snowdf.write.copy_into_location("@" + stage_name + "/" + temp_stage_path + "/processed/success/" + formatted_year + "/" + formatted_month + "/" + file_name1, header=True, OVERWRITE=True)
        
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
CREATE OR REPLACE PROCEDURE SDL_POP6_TH_PRODUCT_LISTS_PRODUCTS_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS 'from snowflake.snowpark.functions import col, lit, date_format, current_timestamp, to_date,md5, year, month, concat, format_number, regexp_replace, to_timestamp, when, trim, upper
from snowflake.snowpark.types import StringType, StructType, StructField,DecimalType
import snowflake.snowpark
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz

def main(session: snowpark.Session,Param):
    #Param=["20240422_product_lists_products.csv","THASDL_RAW.DEV_LOAD_STAGE_ADLS","dev/pop6","sdl_pop6_th_product_lists_products"]
    try:
        file_name = Param[0]
        stage_name = Param[1]
        temp_stage_path = Param[2]
        target_table = Param[3]

        df_schema = StructType([
                                    StructField("Product_List", StringType(), True),
                                    StructField("Product_List_Code", StringType(), True),
                                    StructField("ProductDB_ID", StringType(), True),
                                    StructField("SKU", StringType(), True),
                                    StructField("MSL_Ranking", StringType(), True),
                                    StructField("Date", StringType(), True)
                                ])
        df = session.read.option("INFER_SCHEMA", True)\\
            .option("field_delimiter", ",")\\
            .option("field_optionally_enclosed_by", "\\"") \\
            .option("trim_space", True) \\
            .with_metadata("METADATA$FILE_ROW_NUMBER") \\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        df_columns=df.first()[1:]
        
        df = df.filter(col("METADATA$FILE_ROW_NUMBER") > lit(1))
        
        
        df = df.drop("METADATA$FILE_ROW_NUMBER")
        #return df.schema.names
        o=0

        for i in df_columns:
            

            df=df.withColumnRenamed(df.schema.names[o],i.replace(" ", "_").replace("(","").replace(")","").replace("]","").replace("[",""))
            o+=1
        
              
        df = df.na.drop("all")
        if df.count() == 0:
            return "No Data in file"

        
        df = df.withColumn("filename", lit(file_name))
        df = df.withColumn("crt_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        df = df.withColumn("run_id", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))

        df1=df
        
        df1=df1.na.fill("",subset=["Product_List", "ProductDB_ID", "SKU", "MSL_Ranking"])

        df1 = df1.withColumn("hash_c",concat(df1.Product_List,df1.ProductDB_ID,df1.SKU,df1.MSL_Ranking,df1.Date))
        df1=df1.withColumn("hash",md5(col("hash_c")))
        df1=df1.na.replace({"":None},["Product_List", "ProductDB_ID", "SKU", "MSL_Ranking"])
        snowdf = df1.select(
                          "Product_List", "Product_List_Code","ProductDB_ID", "SKU", "MSL_Ranking",
                            "Date",
                            "filename",
                            "run_id",
                            "crt_dttm",
                            "hash"
                        )
        
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        
        #move to success
        
        
        snowdf.write.mode("append").saveAsTable(stage_name.split(".")[0] + "." + target_table)
        file_name1 = file_name.split(".")[0] + "_" + datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S") + ".csv"
        snowdf.write.copy_into_location("@" + stage_name + "/" + temp_stage_path + "/processed/success/" + formatted_year + "/" + formatted_month + "/" + file_name1, header=True, OVERWRITE=True)
        
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
CREATE OR REPLACE PROCEDURE SDL_POP6_TH_PRODUCT_LISTS_POPS_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS 'from snowflake.snowpark.functions import col, lit, date_format, current_timestamp, to_date,md5, year, month, concat, format_number, regexp_replace, to_timestamp, when, trim, upper
from snowflake.snowpark.types import StringType, StructType, StructField,DecimalType
import snowflake.snowpark
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz

def main(session: snowpark.Session,Param):
    #Param=["20240422_product_lists_pops.csv","THASDL_RAW.DEV_LOAD_STAGE_ADLS","dev/pop6","SDL_POP6_TH_PRODUCT_LISTS_POPS"]
    try:
        file_name = Param[0]
        stage_name = Param[1]
        temp_stage_path = Param[2]
        target_table = Param[3]

        df_schema = StructType([
                    StructField("Product_List", StringType(), True),
                    StructField("Product_List_Code", StringType(), True),
                    StructField("POPDB_ID", StringType(), True),
                    StructField("POP_Code", StringType(), True),
                    StructField("POP_Name", StringType(), True),
                    StructField("Date", StringType(), True)
                ])
        df = session.read.option("INFER_SCHEMA", True)\\
            .option("field_delimiter", ",")\\
            .option("field_optionally_enclosed_by", "\\"") \\
            .option("trim_space", True) \\
            .with_metadata("METADATA$FILE_ROW_NUMBER") \\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        df_columns=df.first()[1:]
        
        df = df.filter(col("METADATA$FILE_ROW_NUMBER") > lit(1))
        
        
        df = df.drop("METADATA$FILE_ROW_NUMBER")
        #return df.schema.names
        o=0

        for i in df_columns:
            

            df=df.withColumnRenamed(df.schema.names[o],i.replace(" ", "_").replace("(","").replace(")","").replace("]","").replace("[",""))
            o+=1
        
              
        df = df.na.drop("all")
        if df.count() == 0:
            return "No Data in file"

        
        df = df.withColumn("filename", lit(file_name))
        df = df.withColumn("crt_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        df = df.withColumn("run_id", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))

        df1=df
        
        df1=df1.na.fill("",subset=["Product_List", "POPDB_ID", "POP_Code", "POP_Name"])

        df1 = df1.withColumn("hash_c",concat(df1.Product_List,df1.POPDB_ID,df1.POP_Code,df1.POP_Name,df1.Date))
        df1=df1.withColumn("hash",md5(col("hash_c")))
        df1=df1.na.replace({"":None},["Product_List", "POPDB_ID", "POP_Code", "POP_Name"])
        snowdf = df1.select(
                          "Product_List", "POPDB_ID", "POP_Code", "POP_Name",
                            "Date",
                            "filename",
                            "run_id",
                            "crt_dttm",
                            "hash"
                        )
        
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        
        #move to success
        
        
        snowdf.write.mode("append").saveAsTable(stage_name.split(".")[0] + "." + target_table)
        file_name1 = file_name.split(".")[0] + "_" + datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S") + ".csv"
        snowdf.write.copy_into_location("@" + stage_name + "/" + temp_stage_path + "/processed/success/" + formatted_year + "/" + formatted_month + "/" + file_name1, header=True, OVERWRITE=True)
        
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
CREATE OR REPLACE PROCEDURE SDL_POP6_TH_POP_LISTS_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS 'from snowflake.snowpark.functions import col, lit, date_format, current_timestamp, to_date,md5, year, month, concat, format_number, regexp_replace, to_timestamp, when, trim, upper
from snowflake.snowpark.types import StringType, StructType, StructField,DecimalType
import snowflake.snowpark
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz

def main(session: snowpark.Session,Param):
    #Param=["20240422_pop_lists.csv","THASDL_RAW.DEV_LOAD_STAGE_ADLS","dev/pop6","SDL_POP6_TH_POP_LISTS"]
    try:
        file_name = Param[0]
        stage_name = Param[1]
        temp_stage_path = Param[2]
        target_table = Param[3]

        df_schema = StructType([
                    StructField("Status", StringType(), nullable=True),
                    StructField("POP_List", StringType(), nullable=True),
                    StructField("POPDB_ID", StringType(), nullable=True),
                    StructField("POP_Code", StringType(), nullable=True),
                    StructField("POP_Name", StringType(), nullable=True),
                    StructField("Date", StringType(), nullable=True)
                ])
        df = session.read.option("INFER_SCHEMA", True)\\
            .option("field_delimiter", ",")\\
            .option("field_optionally_enclosed_by", "\\"") \\
            .option("trim_space", True) \\
            .with_metadata("METADATA$FILE_ROW_NUMBER") \\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        df_columns=df.first()[1:]
        
        df = df.filter(col("METADATA$FILE_ROW_NUMBER") > lit(1))
        
        
        df = df.drop("METADATA$FILE_ROW_NUMBER")
        #return df.schema.names
        o=0

        for i in df_columns:
            

            df=df.withColumnRenamed(df.schema.names[o],i.replace(" ", "_").replace("(","").replace(")","").replace("]","").replace("[",""))
            o+=1
        
              
        df = df.na.drop("all")
        if df.count() == 0:
            return "No Data in file"

        
        df = df.withColumn("filename", lit(file_name))
        df = df.withColumn("crt_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        df = df.withColumn("run_id", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))

        df1=df
        
        df1=df1.na.fill("",subset=["Status",
                                "POP_List",
                                "POPDB_ID",
                                "POP_Code",
                                "POP_Name"
                                ])

        df1 = df1.withColumn("hash_c",concat(df1.Status,df1.POP_List,df1.POPDB_ID,df1.POP_Code,df1.POP_Name))
        df1=df1.withColumn("hash",md5(col("hash_c")))
        df1=df1.na.replace({"":None},["Status",
                                    "POP_List",
                                    "POPDB_ID",
                                    "POP_Code",
                                    "POP_Name"
                                ])
        snowdf = df1.select(
                           "Status",
                            "POP_List",
                            "POPDB_ID",
                            "POP_Code",
                            "POP_Name",
                            "Date",
                            "filename",
                            "run_id",
                            "crt_dttm",
                            "hash"
                        )
        
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        
        #move to success
        
        
        snowdf.write.mode("append").saveAsTable(stage_name.split(".")[0] + "." + target_table)
        file_name1 = file_name.split(".")[0] + "_" + datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S") + ".csv"
        snowdf.write.copy_into_location("@" + stage_name + "/" + temp_stage_path + "/processed/success/" + formatted_year + "/" + formatted_month + "/" + file_name1, header=True, OVERWRITE=True)
        
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

 
		
Use schema sgpsdl_raw;

CREATE OR REPLACE PROCEDURE SG_POP6_POPS_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS 'from snowflake.snowpark.functions import col, lit, date_format, current_timestamp, to_date,md5, year, month, concat, format_number, regexp_replace, to_timestamp, when, trim, upper
from snowflake.snowpark.types import StringType, StructType, StructField,DecimalType
import snowflake.snowpark
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz

def main(session: snowpark.Session,Param):
    #Param=["20240620_pops.csv","SGPSDL_RAW.DEV_LOAD_STAGE_ADLS","dev/pop6/master/masterdata","sdl_pop6_sg_pops"]    
    try:
        file_name = Param[0]
        stage_name = Param[1]
        temp_stage_path = Param[2]
        target_table = Param[3]

        df_schema = StructType([
                    StructField("status", StringType(), True),
                    StructField("popdb_id", StringType(), True),
                    StructField("pop_code", StringType(), True),
                    StructField("pop_name", StringType(), True),
                    StructField("address", StringType(), True),
                    StructField("longitude", StringType(), True),
                    StructField("latitude", StringType(), True),
                    StructField("Business_Units_ID", StringType(), True),
                    StructField("Business_Unit_Name", StringType(), True),
                    StructField("country", StringType(), True),
                    StructField("channel", StringType(), True),
                    StructField("retail_environment_ps", StringType(), True),
                    StructField("sales_group_name", StringType(), True),
                    StructField("customer", StringType(), True),
                    StructField("Store_Type", StringType(), True),
                    StructField("sales_group_code", StringType(), True),
                    StructField("customer_grade", StringType(), True),
                    StructField("Territory", StringType(), True)
                ])
        df = session.read.option("INFER_SCHEMA", True)\\
            .option("field_delimiter", ",")\\
            .option("field_optionally_enclosed_by", "\\"") \\
            .option("trim_space", True) \\
            .with_metadata("METADATA$FILE_ROW_NUMBER") \\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        df_columns=df.first()[1:]
        
        df = df.filter(col("METADATA$FILE_ROW_NUMBER") > lit(1))
        
        
        df = df.drop("METADATA$FILE_ROW_NUMBER")
        #return df.schema.names
        o=0

        for i in df_columns:
            

            df=df.withColumnRenamed(df.schema.names[o],i.replace(" ", "_").replace("(","").replace(")","").replace("]","").replace("[",""))
            o+=1
        
              
        df = df.na.drop("all")
        if df.count() == 0:
            return "No Data in file"

        
        df = df.withColumn("filename", lit(file_name))
        df = df.withColumn("crt_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        df = df.withColumn("run_id", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))

        df1=df
        
        concat_columns = [
                          "status",
                            "popdb_id",
                            "pop_code",
                            "pop_name",
                            "address",
                            "longitude",
                            "latitude",
                            "Business_Units_ID",
                            "Business_Unit_Name",
                            "country",
                            "channel",
                            "retail_environment_ps",
                            "sales_group_code",
                            "customer",
                            "sales_group_name",
                            "customer_grade",
                            "Territory/Region"
                    ]
        
        df1=df1.na.fill("",subset=concat_columns)


       # Concatenate the columns
        df1 = df1.withColumn("hash_c", concat(*[df1[col] for col in concat_columns]))

        #df1 = df1.withColumn("hash_c",concat(df1.Product_List,df1.ProductDB_ID,df1.SKU,df1.MSL_Ranking,df1.Date))
        df1=df1.withColumn("hash",md5(col("hash_c")))
        df1=df1.na.replace({"":None},concat_columns)
        snowdf = df1.select(
                           "status",
                            "popdb_id",
                            "pop_code",
                            "pop_name",
                            "address",
                            "longitude",
                            "latitude",
                            "Business_Units_ID",
                            "Business_Unit_Name",
                            "country",
                            "channel",
                            "retail_environment_ps",
                            "sales_group_name",
                            "customer",
                            "sales_group_code",
                            "customer_grade",
                            "Territory/Region",
                            "filename",
                            "run_id",
                            "crt_dttm",
                            "hash"
                        )
        
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        
        #move to success
        
        snowdf.write.mode("append").saveAsTable(stage_name.split(".")[0] + "." + target_table)
        #snowdf.write.mode("append").saveAsTable(target_table)
        file_name1 = file_name.split(".")[0] + "_" + datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        snowdf.write.copy_into_location("@" + stage_name + "/" + temp_stage_path + "/processed/success/" + formatted_year + "/" + formatted_month + "/" + file_name1, header=True, OVERWRITE=True)
        
        return "Success"

    except KeyError as key_error:
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message

    except pd.errors.MergeError as merge_error:
        error_message = f"DataFrame merging error: {str(merge_error)}"
        return error_message

    except Exception as e:
        error_message = f"Error: {str(e)}"
        return error_message';
CREATE OR REPLACE PROCEDURE SG_POP6_PRODUCTS_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS 'from snowflake.snowpark.functions import col, lit, date_format, current_timestamp, to_date,md5, year, month, concat, format_number, regexp_replace, to_timestamp, when, trim, upper
from snowflake.snowpark.types import StringType, StructType, StructField,DecimalType
import snowflake.snowpark
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz

def main(session: snowpark.Session,Param):
    #Param=["20240620_products.csv","SGPSDL_RAW.DEV_LOAD_STAGE_ADLS","dev/pop6/master/masterdata","sdl_pop6_sg_pops"]    
    try:
        file_name = Param[0]
        stage_name = Param[1]
        temp_stage_path = Param[2]
        target_table = Param[3]

        df_schema = StructType([
                    StructField("Status", StringType(), True),
                    StructField("ProductDB_ID", StringType(), True),
                    StructField("Barcode", StringType(), True),
                    StructField("SKU", StringType(), True),
                    StructField("Unit_Price", StringType(), True),
                    StructField("Display_order", StringType(), True),
                    StructField("Launch_Date", StringType(), True),
                    StructField("Largest_UOM_Quantity", StringType(), True),
                    StructField("Middle_UOM_Quantity", StringType(), True),
                    StructField("Smallest_UOM_Quantity", StringType(), True),
                    StructField("SKU_English", StringType(), True),
                    StructField("Company", StringType(), True),
                    StructField("SKU_Code", StringType(), True),
                    StructField("PS_Category", StringType(), True),
                    StructField("PS_Segment", StringType(), True),
                    StructField("PS_Category_Segment", StringType(), True),
                    StructField("Country_L1", StringType(), True),
                    StructField("Regional_Franchise_L2", StringType(), True),
                    StructField("Franchise_L3", StringType(), True),
                    StructField("Brand_L4", StringType(), True),
                    StructField("Sub_Category_L5", StringType(), True),
                    StructField("Platform_L6", StringType(), True),
                    StructField("Variance_L7", StringType(), True),
                    StructField("Pack_size_L8", StringType(), True)
                ])
        df = session.read.option("INFER_SCHEMA", True)\\
            .option("field_delimiter", ",")\\
            .option("field_optionally_enclosed_by", "\\"") \\
            .option("trim_space", True) \\
            .with_metadata("METADATA$FILE_ROW_NUMBER") \\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        df_columns=df.first()[1:]
        
        df = df.filter(col("METADATA$FILE_ROW_NUMBER") > lit(1))
        
        
        df = df.drop("METADATA$FILE_ROW_NUMBER")
        #return df.schema.names
        o=0

        for i in df_columns:
            

            df=df.withColumnRenamed(df.schema.names[o],i.replace(" ", "_").replace("(","").replace(")","").replace("]","").replace("[",""))
            o+=1
        
              
        df = df.na.drop("all")
        if df.count() == 0:
            return "No Data in file"

        
        df = df.withColumn("filename", lit(file_name))
        df = df.withColumn("crt_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        df = df.withColumn("run_id", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))

        df1=df
        
        concat_columns = [
                        "Status",
                        "ProductDB_ID",
                        "Barcode",
                        "SKU",
                        "Unit_Price",
                        "Display_order",
                        "Launch_Date",
                        "Largest_UOM_Quantity",
                        "Middle_UOM_Quantity",
                        "Smallest_UOM_Quantity",
                        "SKU_English",  # Parentheses removed
                        "Company",
                        "SKU_Code",
                        "PS_Category",
                        "PS_Segment",
                        "PS_Category_Segment",
                        "Country-L1",
                        "Regional_Franchise-L2",
                        "Franchise-L3",
                        "Brand-L4",
                        "Sub_Category-L5",
                        "Platform-L6",
                        "Variance-L7",
                        "Pack_size-L8"
                    ]
        
        df1=df1.na.fill("",subset=concat_columns)


       # Concatenate the columns
        df1 = df1.withColumn("hash_c", concat(*[df1[col] for col in concat_columns]))

        #df1 = df1.withColumn("hash_c",concat(df1.Product_List,df1.ProductDB_ID,df1.SKU,df1.MSL_Ranking,df1.Date))
        df1=df1.withColumn("hash",md5(col("hash_c")))
        df1=df1.na.replace({"":None},concat_columns)
        snowdf = df1.select(
                            
                            "Status",
                            "ProductDB_ID",
                            "Barcode",
                            "SKU",
                            "Unit_Price",
                            "Display_order",
                            "Launch_Date",
                            "Largest_UOM_Quantity",
                            "Middle_UOM_Quantity",
                            "Smallest_UOM_Quantity",
                            "SKU_English",  # Parentheses removed
                            "Company",
                            "SKU_Code",
                            "PS_Category",
                            "PS_Segment",
                            "PS_Category_Segment",
                            "Country-L1",
                            "Regional_Franchise-L2",
                            "Franchise-L3",
                            "Brand-L4",
                            "Sub_Category-L5",
                            "Platform-L6",
                            "Variance-L7",
                            "Pack_size-L8",
                            "filename",
                            "run_id",
                            "crt_dttm",
                            "hash"
                        )
        
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        
        #move to success
        
        snowdf.write.mode("append").saveAsTable(stage_name.split(".")[0] + "." + target_table)
        #snowdf.write.mode("append").saveAsTable(target_table)
        file_name1 = file_name.split(".")[0] + "_" + datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        snowdf.write.copy_into_location("@" + stage_name + "/" + temp_stage_path + "/processed/success/" + formatted_year + "/" + formatted_month + "/" + file_name1, header=True, OVERWRITE=True)
        
        return "Success"

    except KeyError as key_error:
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message

    except pd.errors.MergeError as merge_error:
        error_message = f"DataFrame merging error: {str(merge_error)}"
        return error_message

    except Exception as e:
        error_message = f"Error: {str(e)}"
        return error_message';
CREATE OR REPLACE PROCEDURE SG_POP6_USERS_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('openpyxl==3.1.2','snowflake-snowpark-python==*')
HANDLER = 'main'
EXECUTE AS OWNER
AS 'from snowflake.snowpark.functions import col, lit, date_format, current_timestamp, to_date,md5, year, month, concat, format_number, regexp_replace, to_timestamp, when, trim, upper
from snowflake.snowpark.types import StringType, StructType, StructField,DecimalType
import snowflake.snowpark
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz

def main(session: snowpark.Session,Param):
    #Param=["20240620_users.csv","SGPSDL_RAW.DEV_LOAD_STAGE_ADLS","dev/pop6/master/masterdata","sdl_pop6_sg_pops"]    
    try:
        file_name = Param[0]
        stage_name = Param[1]
        temp_stage_path = Param[2]
        target_table = Param[3]

        df_schema = StructType([
                    StructField("Status", StringType(), True),
                    StructField("UserDB_ID", StringType(), True),
                    StructField("Username", StringType(), True),
                    StructField("First_Name", StringType(), True),
                    StructField("Last_Name", StringType(), True),
                    StructField("Team", StringType(), True),
                    StructField("Superior_Name", StringType(), True),
                    StructField("Authorisation_Group", StringType(), True),
                    StructField("Email_Address", StringType(), True),
                    StructField("Longitude", StringType(), True),
                    StructField("Latitude", StringType(), True),
                    StructField("Business_Units_ID", StringType(), True),
                    StructField("Business_Unit_Name", StringType(), True),
                    StructField("Mobile_Number", StringType(), True)
                ])
        df = session.read.option("INFER_SCHEMA", True)\\
            .option("field_delimiter", ",")\\
            .option("field_optionally_enclosed_by", "\\"") \\
            .option("trim_space", True) \\
            .with_metadata("METADATA$FILE_ROW_NUMBER") \\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        df_columns=df.first()[1:]
        
        df = df.filter(col("METADATA$FILE_ROW_NUMBER") > lit(1))
        
        
        df = df.drop("METADATA$FILE_ROW_NUMBER")
        #return df.schema.names
        o=0

        for i in df_columns:
            

            df=df.withColumnRenamed(df.schema.names[o],i.replace(" ", "_").replace("(","").replace(")","").replace("]","").replace("[",""))
            o+=1
        
              
        df = df.na.drop("all")
        if df.count() == 0:
            return "No Data in file"

        
        df = df.withColumn("filename", lit(file_name))
        df = df.withColumn("crt_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        df = df.withColumn("run_id", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))

        df1=df
       
        concat_columns = [
                              "status",
                                "userdb_id",
                                "username",
                                "first_name",
                                "last_name",
                                "team",
                                "superior_name",
                                "authorisation_group",
                                "email_address",
                                "longitude",
                                "latitude",
                                "business_units_id",
                                "business_unit_name",
                                "mobile_number"
                    ]
        
        df1=df1.na.fill("",subset=concat_columns)


       # Concatenate the columns
        df1 = df1.withColumn("hash_c", concat(*[df1[col] for col in concat_columns]))

        #df1 = df1.withColumn("hash_c",concat(df1.Product_List,df1.ProductDB_ID,df1.SKU,df1.MSL_Ranking,df1.Date))
        df1=df1.withColumn("hash",md5(col("hash_c")))
        df1=df1.na.replace({"":None},concat_columns)
        snowdf = df1.select(
                             "status",
                            "userdb_id",
                            "username",
                            "first_name",
                            "last_name",
                            "team",
                            "superior_name",
                            "authorisation_group",
                            "email_address",
                            "longitude",
                            "latitude",
                            "business_units_id",
                            "business_unit_name",
                            "mobile_number",
                    
                            "filename",
                            "run_id",
                            "crt_dttm",
                            "hash"
                        )
        
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        
        #move to success
        
        snowdf.write.mode("append").saveAsTable(stage_name.split(".")[0] + "." + target_table)
        #snowdf.write.mode("append").saveAsTable(target_table)
        file_name1 = file_name.split(".")[0] + "_" + datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        snowdf.write.copy_into_location("@" + stage_name + "/" + temp_stage_path + "/processed/success/" + formatted_year + "/" + formatted_month + "/" + file_name1, header=True, OVERWRITE=True)
        
        return "Success"

    except KeyError as key_error:
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message

    except pd.errors.MergeError as merge_error:
        error_message = f"DataFrame merging error: {str(merge_error)}"
        return error_message

    except Exception as e:
        error_message = f"Error: {str(e)}"
        return error_message';
CREATE OR REPLACE PROCEDURE SG_POP6_PRODUCT_LISTS_PRODUCTS_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS 'from snowflake.snowpark.functions import col, lit, date_format, current_timestamp, to_date,md5, year, month, concat, format_number, regexp_replace, to_timestamp, when, trim, upper
from snowflake.snowpark.types import StringType, StructType, StructField,DecimalType
import snowflake.snowpark
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz

def main(session: snowpark.Session,Param):
    #Param=["20240620_product_lists_products.csv","SGPSDL_RAW.DEV_LOAD_STAGE_ADLS","dev/pop6/master/listdata","sdl_pop6_sg_pops"]    
    try:
        file_name = Param[0]
        stage_name = Param[1]
        temp_stage_path = Param[2]
        target_table = Param[3]

        
        df = session.read.option("INFER_SCHEMA", True)\\
            .option("field_delimiter", ",")\\
            .option("field_optionally_enclosed_by", "\\"") \\
            .option("trim_space", True) \\
            .with_metadata("METADATA$FILE_ROW_NUMBER") \\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        df_columns=df.first()[1:]
        
        df = df.filter(col("METADATA$FILE_ROW_NUMBER") > lit(1))
        
        
        df = df.drop("METADATA$FILE_ROW_NUMBER")
        #return df.schema.names
        o=0

        for i in df_columns:
            

            df=df.withColumnRenamed(df.schema.names[o],i.replace(" ", "_").replace("(","").replace(")","").replace("]","").replace("[",""))
            o+=1
        
              
        df = df.na.drop("all")
        if df.count() == 0:
            return "No Data in file"

        
        df = df.withColumn("filename", lit(file_name))
        df = df.withColumn("crt_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        df = df.withColumn("run_id", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))

        df1=df
        
        concat_columns = [
                            "Product_List",
                            "Product_List_Code",
                            "ProductDB_ID",
                            "SKU",
                            "MSL_Ranking",
                            "Date"
                    ]
        
        df1=df1.na.fill("",subset=concat_columns)


       # Concatenate the columns
        df1 = df1.withColumn("hash_c", concat(*[df1[col] for col in concat_columns]))

        #df1 = df1.withColumn("hash_c",concat(df1.Product_List,df1.ProductDB_ID,df1.SKU,df1.MSL_Ranking,df1.Date))
        df1=df1.withColumn("hash",md5(col("hash_c")))
        df1=df1.na.replace({"":None},concat_columns)
        snowdf = df1.select(
                             "Product_List",
                            "Product_List_Code",
                            "ProductDB_ID",
                            "SKU",
                            "MSL_Ranking",
                            "Date",
                            "filename",
                            "run_id",
                            "crt_dttm",
                            "hash"
                        )
        
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        
        #move to success
        
        snowdf.write.mode("append").saveAsTable(stage_name.split(".")[0] + "." + target_table)
        #snowdf.write.mode("append").saveAsTable(target_table)
        file_name1 = file_name.split(".")[0] + "_" + datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        snowdf.write.copy_into_location("@" + stage_name + "/" + temp_stage_path + "/processed/success/" + formatted_year + "/" + formatted_month + "/" + file_name1, header=True, OVERWRITE=True)
        
        return "Success"

    except KeyError as key_error:
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message

    except pd.errors.MergeError as merge_error:
        error_message = f"DataFrame merging error: {str(merge_error)}"
        return error_message

    except Exception as e:
        error_message = f"Error: {str(e)}"
        return error_message';
    
CREATE OR REPLACE PROCEDURE SG_POP6_PRODUCT_LISTS_ALLOCATION_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS 'from snowflake.snowpark.functions import col, lit, date_format, current_timestamp, to_date,md5, year, month, concat, format_number, regexp_replace, to_timestamp, when, trim, upper
from snowflake.snowpark.types import StringType, StructType, StructField,DecimalType
import snowflake.snowpark
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz

def main(session: snowpark.Session,Param):
    #Param=["20240620_product_lists_allocation.csv","SGPSDL_RAW.DEV_LOAD_STAGE_ADLS","dev/pop6/master/listdata","sdl_pop6_sg_pops"]    
    try:
        file_name = Param[0]
        stage_name = Param[1]
        temp_stage_path = Param[2]
        target_table = Param[3]

        
        df = session.read.option("INFER_SCHEMA", True)\\
            .option("field_delimiter", ",")\\
            .option("field_optionally_enclosed_by", "\\"") \\
            .option("trim_space", True) \\
            .with_metadata("METADATA$FILE_ROW_NUMBER") \\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        df_columns=df.first()[1:]
        
        df = df.filter(col("METADATA$FILE_ROW_NUMBER") > lit(1))
        
        
        df = df.drop("METADATA$FILE_ROW_NUMBER")
        #return df.schema.names
        o=0

        for i in df_columns:
            

            df=df.withColumnRenamed(df.schema.names[o],i.replace(" ", "_").replace("(","").replace(")","").replace("]","").replace("[",""))
            o+=1
        
              
        df = df.na.drop("all")
        if df.count() == 0:
            return "No Data in file"

        
        df = df.withColumn("filename", lit(file_name))
        df = df.withColumn("crt_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        df = df.withColumn("run_id", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))

        df1=df

        concat_columns = [
                            "Product_Group_Status",
                            "Product_Group",
                            "Product_List_Status",
                            "Product_List",
                            "Product_List_Code",
                            "POP_Attribute_ID",
                            "POP_Attribute",
                            "POP_Attribute_Value_ID",
                            "POP_Attribute_Value",
                            "Date"
                    ]
        
        df1=df1.na.fill("",subset=concat_columns)


       # Concatenate the columns
        df1 = df1.withColumn("hash_c", concat(*[df1[col] for col in concat_columns]))

        #df1 = df1.withColumn("hash_c",concat(df1.Product_List,df1.ProductDB_ID,df1.SKU,df1.MSL_Ranking,df1.Date))
        df1=df1.withColumn("hash",md5(col("hash_c")))
        df1=df1.na.replace({"":None},concat_columns)
        snowdf = df1.select(
                             "Product_Group_Status",
                            "Product_Group",
                            "Product_List_Status",
                            "Product_List",
                            "Product_List_Code",
                            "POP_Attribute_ID",
                            "POP_Attribute",
                            "POP_Attribute_Value_ID",
                            "POP_Attribute_Value",
                            "Date",
                            "filename",
                            "run_id",
                            "crt_dttm",
                            "hash"
                        )
        
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        
        #move to success
        
        snowdf.write.mode("append").saveAsTable(stage_name.split(".")[0] + "." + target_table)
        #snowdf.write.mode("append").saveAsTable(target_table)
        file_name1 = file_name.split(".")[0] + "_" + datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        snowdf.write.copy_into_location("@" + stage_name + "/" + temp_stage_path + "/processed/success/" + formatted_year + "/" + formatted_month + "/" + file_name1, header=True, OVERWRITE=True)
        
        return "Success"

    except KeyError as key_error:
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message

    except pd.errors.MergeError as merge_error:
        error_message = f"DataFrame merging error: {str(merge_error)}"
        return error_message

    except Exception as e:
        error_message = f"Error: {str(e)}"
        return error_message';
CREATE OR REPLACE PROCEDURE SG_POP6_POP_LISTS_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS 'from snowflake.snowpark.functions import col, lit, date_format, current_timestamp, to_date,md5, year, month, concat, format_number, regexp_replace, to_timestamp, when, trim, upper
from snowflake.snowpark.types import StringType, StructType, StructField,DecimalType
import snowflake.snowpark
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz

def main(session: snowpark.Session,Param):
    #Param=["20240620_pop_lists.csv","SGPSDL_RAW.DEV_LOAD_STAGE_ADLS","dev/pop6/master/listdata","sdl_pop6_sg_pops"]    
    try:
        file_name = Param[0]
        stage_name = Param[1]
        temp_stage_path = Param[2]
        target_table = Param[3]

        
        df = session.read.option("INFER_SCHEMA", True)\\
            .option("field_delimiter", ",")\\
            .option("field_optionally_enclosed_by", "\\"") \\
            .option("trim_space", True) \\
            .with_metadata("METADATA$FILE_ROW_NUMBER") \\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        df_columns=df.first()[1:]
        
        df = df.filter(col("METADATA$FILE_ROW_NUMBER") > lit(1))
        
        
        df = df.drop("METADATA$FILE_ROW_NUMBER")
        #return df.schema.names
        o=0

        for i in df_columns:
            

            df=df.withColumnRenamed(df.schema.names[o],i.replace(" ", "_").replace("(","").replace(")","").replace("]","").replace("[",""))
            o+=1
        
              
        df = df.na.drop("all")
        if df.count() == 0:
            return "No Data in file"

        
        df = df.withColumn("filename", lit(file_name))
        df = df.withColumn("crt_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        df = df.withColumn("run_id", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        df1=df
        concat_columns = [
                              "Status",
                            "POP_List",
                            "POPDB_ID",
                            "POP_Code",
                            "POP_Name",
                            "Date"
                    ]
        
        df1=df1.na.fill("",subset=concat_columns)


       # Concatenate the columns
        df1 = df1.withColumn("hash_c", concat(*[df1[col] for col in concat_columns]))

        #df1 = df1.withColumn("hash_c",concat(df1.Product_List,df1.ProductDB_ID,df1.SKU,df1.MSL_Ranking,df1.Date))
        df1=df1.withColumn("hash",md5(col("hash_c")))
        df1=df1.na.replace({"":None},concat_columns)
        snowdf = df1.select(
                              "Status",
                            "POP_List",
                            "POPDB_ID",
                            "POP_Code",
                            "POP_Name",
                            "Date",
                    
                            "filename",
                            "run_id",
                            "crt_dttm",
                            "hash"
                        )
        
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        
        #move to success
        
        snowdf.write.mode("append").saveAsTable(stage_name.split(".")[0] + "." + target_table)
        #snowdf.write.mode("append").saveAsTable(target_table)
        file_name1 = file_name.split(".")[0] + "_" + datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        snowdf.write.copy_into_location("@" + stage_name + "/" + temp_stage_path + "/processed/success/" + formatted_year + "/" + formatted_month + "/" + file_name1, header=True, OVERWRITE=True)
        
        return "Success"

    except KeyError as key_error:
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message

    except pd.errors.MergeError as merge_error:
        error_message = f"DataFrame merging error: {str(merge_error)}"
        return error_message

    except Exception as e:
        error_message = f"Error: {str(e)}"
        return error_message';
CREATE OR REPLACE PROCEDURE SG_POP6_PRODUCT_LISTS_POPS_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS 'from snowflake.snowpark.functions import col, lit, date_format, current_timestamp, to_date,md5, year, month, concat, format_number, regexp_replace, to_timestamp, when, trim, upper
from snowflake.snowpark.types import StringType, StructType, StructField,DecimalType
import snowflake.snowpark
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz

def main(session: snowpark.Session,Param):
    #Param=["20240620_product_lists_pops.csv","SGPSDL_RAW.DEV_LOAD_STAGE_ADLS","dev/pop6/master/listdata","sdl_pop6_sg_pops"]    
    try:
        file_name = Param[0]
        stage_name = Param[1]
        temp_stage_path = Param[2]
        target_table = Param[3]

        df = session.read.option("INFER_SCHEMA", True)\\
            .option("field_delimiter", ",")\\
            .option("field_optionally_enclosed_by", "\\"") \\
            .option("trim_space", True) \\
            .with_metadata("METADATA$FILE_ROW_NUMBER") \\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        df_columns=df.first()[1:]
        
        df = df.filter(col("METADATA$FILE_ROW_NUMBER") > lit(1))
        
        
        df = df.drop("METADATA$FILE_ROW_NUMBER")
        #return df.schema.names
        o=0

        for i in df_columns:
            

            df=df.withColumnRenamed(df.schema.names[o],i.replace(" ", "_").replace("(","").replace(")","").replace("]","").replace("[",""))
            o+=1
        
              
        df = df.na.drop("all")
        if df.count() == 0:
            return "No Data in file"

        
        df = df.withColumn("filename", lit(file_name))
        df = df.withColumn("crt_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        df = df.withColumn("run_id", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))

        df1=df
        concat_columns = [
                            "Product_List",
                            "POPDB_ID",
                            "POP_Code",
                            "POP_Name",
                            "Date"
                    ]
        
        df1=df1.na.fill("",subset=concat_columns)


       # Concatenate the columns
        df1 = df1.withColumn("hash_c", concat(*[df1[col] for col in concat_columns]))

        #df1 = df1.withColumn("hash_c",concat(df1.Product_List,df1.ProductDB_ID,df1.SKU,df1.MSL_Ranking,df1.Date))
        df1=df1.withColumn("hash",md5(col("hash_c")))
        df1=df1.na.replace({"":None},concat_columns)
        snowdf = df1.select(
                             "Product_List",
                            "POPDB_ID",
                            "POP_Code",
                            "POP_Name",
                            "Date",
                            "filename",
                            "run_id",
                            "crt_dttm",
                            "hash"
                        )
        
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        
        #move to success
        
        snowdf.write.mode("append").saveAsTable(stage_name.split(".")[0] + "." + target_table)
        #snowdf.write.mode("append").saveAsTable(target_table)
        file_name1 = file_name.split(".")[0] + "_" + datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        snowdf.write.copy_into_location("@" + stage_name + "/" + temp_stage_path + "/processed/success/" + formatted_year + "/" + formatted_month + "/" + file_name1, header=True, OVERWRITE=True)
        
        return "Success"

    except KeyError as key_error:
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message

    except pd.errors.MergeError as merge_error:
        error_message = f"DataFrame merging error: {str(merge_error)}"
        return error_message

    except Exception as e:
        error_message = f"Error: {str(e)}"
        return error_message';
		
use schema NTASDL_RAW;
CREATE OR REPLACE PROCEDURE POP6_TW_MASTER_POPS_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them.

from snowflake.snowpark.functions import col, lit, date_format, current_timestamp, to_date, year, month, concat, format_number, regexp_replace,to_timestamp,when,trim,upper,is_null,md5
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DecimalType
import snowflake.snowpark
import pandas as pd
from datetime import datetime,timedelta
import snowflake.snowpark as snowpark
import pytz
import pandas as pd
import re

def main(session:snowpark.Session,Param):
    #Param=["20240616_pops.csv","NTASDL_RAW.DEV_LOAD_STAGE_ADLS_TWN","dev/pop6/masterdata/pops/","sdl_pop6_tw_pops"]
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        
        df_schema = StructType([
                StructField("Status", StringType(), True),
                StructField("popdb_id", StringType(), True),
                StructField("pop_code", StringType(), True),
                StructField("pop_name", StringType(), True),
                StructField("address", StringType(), True),
                StructField("longitude", StringType(), True),
                StructField("latitude", StringType(), True),
                StructField("Business_Units_ID", StringType(), True),
                StructField("Business_Unit_Name", StringType(), True),
                StructField("country", StringType(), True),
                StructField("channel", StringType(), True),
                StructField("retail_environment_ps", StringType(), True),
                StructField("customer", StringType(), True),
                StructField("sales_group_name", StringType(), True),
                StructField("Store_Type", StringType(), True),
                StructField("sales_group_code", StringType(), True),
                StructField("Customer_Grade", StringType(), True),
                StructField("External_Store_Code", StringType(), True),
                 StructField("Sales", StringType(), True),
                StructField("territory", StringType(), True),  
            ])

           
        
        df = session.read.option("INFER_SCHEMA", True)\\
            .option("field_delimiter", ",")\\
            .option("field_optionally_enclosed_by", "\\"") \\
            .option("trim_space", True) \\
            .with_metadata("METADATA$FILE_ROW_NUMBER") \\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        df_columns=df.first()[1:]
        
        df = df.filter(col("METADATA$FILE_ROW_NUMBER") > lit(1))
        
        
        df = df.drop("METADATA$FILE_ROW_NUMBER")
        #return df.schema.names
        o=0

        for i in df_columns:
            

            df=df.withColumnRenamed(df.schema.names[o],(re.sub(r"[^a-zA-Z0-9_]","",i.replace(" ", "_").replace("(","").replace(")","").replace("]","").replace("[",""))).strip("_"))
            
            o+=1

       
        
        df=df.na.drop("all")
        if df.count()==0 :
            return "No Data in file"

        df = df.with_column(''file_name'',lit(file_name))


        def generunid():
            new_time = datetime.now(pytz.utc) + timedelta(hours=8)
            return new_time.strftime(''%Y%m%d%H%M%S'')

        def generatecrtd_dttm():
            new_time = datetime.now(pytz.utc) + timedelta(hours=8)
            return new_time.strftime(''%Y-%m-%d %H:%M:%S'')

            

            
             

        df = df.with_column(''hashkey'',md5(concat(when(is_null(col(''status'')),lit('''')).otherwise(col(''status'')),\\
                                            when(is_null(col(''popdb_id'')),lit('''')).otherwise(col(''popdb_id'')),\\
                                          when(is_null(col(''pop_code'')),lit('''')).otherwise(col(''pop_code'')),\\
                                          when(is_null(col(''pop_name'')),lit('''')).otherwise(col(''pop_name'')),\\
                                         when(is_null(col(''address'')),lit('''')).otherwise(col(''address'')),\\
                                         when(is_null(col(''longitude'')),lit('''')).otherwise(col(''longitude'')),\\
                                         when(is_null(col(''latitude'')),lit('''')).otherwise(col(''latitude'')),\\
                                          when(is_null(col(''country'')),lit('''')).otherwise(col(''country'')),\\
                                         when(is_null(col(''channel'')),lit('''')).otherwise(col(''channel'')),\\
                                         when(is_null(col(''retail_environment_ps'')),lit('''')).otherwise(col(''retail_environment_ps'')),\\
                                         when(is_null(col(''customer'')),lit('''')).otherwise(col(''customer'')),\\
                                         when(is_null(col(''Sales_Group_Code'')),lit('''')).otherwise(col(''Sales_Group_Code'')),\\
                                         when(is_null(col(''sales_group_name'')),lit('''')).otherwise(col(''sales_group_name'')),\\
                                        when(is_null(col(''customer_grade'')),lit('''')).otherwise(col(''customer_grade'')),\\
                                          when(is_null(col(''External_Store_Code'')),lit('''')).otherwise(col(''External_Store_Code'')),
                                                when(is_null(col(''business_units_id'')),lit('''')).otherwise(col(''business_units_id'')),\\
                                                 when(is_null(col(''sales'')),lit('''')).otherwise(col(''sales'')),\\
                                          when(is_null(col(''TerritoryRegion'')),lit('''')).otherwise(col(''TerritoryRegion'')))))
                                         
        
        df = df.with_column(''run_id'',lit(generunid()))

        df = df.with_column(''crtd_dttm'',lit(generatecrtd_dttm()))
        

       

        
        snowdf= df.select(
                    "Status",
                "popdb_id",
                "pop_code",
                "pop_name",
                "address",
                "longitude",
                "latitude",
                "country",
                "channel",
                "retail_environment_ps",
                "customer",
                "Sales_Group_Code",
                "sales_group_name",
                "customer_grade",
                "External_Store_Code",
                "file_name",
                "Sales",
                "run_id",
                "crtd_dttm",
                "hashkey",
                "business_units_id",
                "business_unit_name",
                "TerritoryRegion" 
                
            
                    )
        file_name=file_name.split(".")[0]+"_"+datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")

        snowdf.write.mode("append").saveAsTable(stage_name.split(".")[0] + "." + target_table)
        
        # #move to success
        snowdf.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)
        
        
        
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
CREATE OR REPLACE PROCEDURE POP6_TW_MASTER_PRODUCTS_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them.

from snowflake.snowpark.functions import col, lit, date_format, current_timestamp, to_date, year, month, concat, format_number, regexp_replace,to_timestamp,when,trim,upper,is_null,md5
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DecimalType
import snowflake.snowpark
import pandas as pd
from datetime import datetime,timedelta
import snowflake.snowpark as snowpark
import pytz
import pandas as pd

def main(session:snowpark.Session,Param):
    #Param=["20240617_products.csv","NTASDL_RAW.DEV_LOAD_STAGE_ADLS_TWN","dev/pop6/masterdata/products/","sdl_pop6_tw_products"]
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        
        df_schema = StructType([
                StructField("Status", StringType(), True),
                StructField("productdb_id", StringType(), True),
                StructField("barcode", StringType(), True),
                StructField("sku", StringType(), True),
                StructField("unit_price", StringType(), True),
                StructField("display_order", StringType(), True),
                StructField("launch_date", StringType(), True),
                StructField("largest_uom_quantity", StringType(), True),
                StructField("middle_uom_quantity", StringType(), True),
                StructField("smallest_uom_quantity", StringType(), True),
                StructField("sku_english", StringType(), True),
                StructField("company", StringType(), True),
                StructField("sku_code", StringType(), True),
                StructField("ps_category", StringType(), True),
                StructField("ps_segment", StringType(), True),
                StructField("ps_category_segment", StringType(), True),
                StructField("country_l1", StringType(), True),
                StructField("regional_franchise_l2", StringType(), True),
                StructField("franchise_l3", StringType(), True),
                StructField("brand_l4", StringType(), True),
                StructField("sub_category_l5", StringType(), True),
                StructField("platform_level_l6", StringType(), True),
                StructField("variance_l7", StringType(), True),
                StructField("pack_size_l8", StringType(), True)
                
            ])
        
        df = session.read.option("INFER_SCHEMA", True)\\
            .option("field_delimiter", ",")\\
            .option("field_optionally_enclosed_by", "\\"") \\
            .option("trim_space", True) \\
            .with_metadata("METADATA$FILE_ROW_NUMBER") \\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        df_columns=df.first()[1:]
        
        df = df.filter(col("METADATA$FILE_ROW_NUMBER") > lit(1))
        
        
        df = df.drop("METADATA$FILE_ROW_NUMBER")
        #return df.schema.names
        o=0

        for i in df_columns:
            

            df=df.withColumnRenamed(df.schema.names[o],i.replace(" ", "_").replace("(","").replace(")","").replace("]","").replace("[",""))
            #df=df.withColumnRenamed(df.schema.names[o],(re.sub(r"[^a-zA-Z0-9_]","",i.replace(" ", "_").replace("(","").replace(")","").replace("]","").replace("[",""))).strip("_"))
            o+=1

       
        
        df=df.na.drop("all")
        if df.count()==0 :
            return "No Data in file"

        df = df.with_column(''file_name'',lit(file_name))


        def generunid():
            new_time = datetime.now(pytz.utc) + timedelta(hours=8)
            return new_time.strftime(''%Y%m%d%H%M%S'')

        def generatecrtd_dttm():
            new_time = datetime.now(pytz.utc) + timedelta(hours=8)
            return new_time.strftime(''%Y-%m-%d %H:%M:%S'')
            
         
   

        df = df.with_column(''hashkey'',md5(concat(when(is_null(col(''status'')),lit('''')).otherwise(col(''status'')),\\
                                            when(is_null(col(''productdb_id'')),lit('''')).otherwise(col(''productdb_id'')),\\
                                          when(is_null(col(''barcode'')),lit('''')).otherwise(col(''barcode'')),\\
                                          when(is_null(col(''sku'')),lit('''')).otherwise(col(''sku'')),\\
                                         when(is_null(col(''unit_price'')),lit('''')).otherwise(col(''unit_price'')),\\
                                         when(is_null(col(''display_order'')),lit('''')).otherwise(col(''display_order'')),\\
                                         when(is_null(col(''launch_date'')),lit('''')).otherwise(col(''launch_date'')),\\
                                          when(is_null(col(''largest_uom_quantity'')),lit('''')).otherwise(col(''largest_uom_quantity'')),\\
                                         when(is_null(col(''middle_uom_quantity'')),lit('''')).otherwise(col(''middle_uom_quantity'')),\\
                                         when(is_null(col(''smallest_uom_quantity'')),lit('''')).otherwise(col(''smallest_uom_quantity'')),\\
                                         when(is_null(col(''company'')),lit('''')).otherwise(col(''company'')),\\
                                         when(is_null(col(''sku_english'')),lit('''')).otherwise(col(''sku_english'')),\\
                                         when(is_null(col(''sku_code'')),lit('''')).otherwise(col(''sku_code'')),\\
                                        when(is_null(col(''ps_category'')),lit('''')).otherwise(col(''ps_category'')),\\
                                          when(is_null(col(''ps_segment'')),lit('''')).otherwise(col(''ps_segment'')),\\
                                         when(is_null(col(''ps_category_segment'')),lit('''')).otherwise(col(''ps_category_segment'')),\\
                                         when(is_null(col(''country_l1'')),lit('''')).otherwise(col(''country_l1'')),\\
                                         when(is_null(col(''regional_franchise_l2'')),lit('''')).otherwise(col(''regional_franchise_l2'')),\\
                                          when(is_null(col(''franchise_l3'')),lit('''')).otherwise(col(''franchise_l3'')),\\
                                         when(is_null(col(''Brand__L4'')),lit('''')).otherwise(col(''Brand__L4'')),\\
                                         when(is_null(col(''Sub_Category__L5'')),lit('''')).otherwise(col(''Sub_Category__L5'')),\\
                                         when(is_null(col(''Platform_Level__L6'')),lit('''')).otherwise(col(''Platform_Level__L6'')),\\
                                         when(is_null(col(''Variance__L7'')),lit('''')).otherwise(col(''Variance__L7'')),\\
                                         when(is_null(col(''Pack_Size__L8'')),lit('''')).otherwise(col(''Pack_Size__L8'')))))

        
        df = df.with_column(''run_id'',lit(generunid()))

        df = df.with_column(''crtd_dttm'',lit(generatecrtd_dttm()))
        

    
        snowdf= df.select(
                   "Status", 
                "productdb_id", 
                "barcode", 
                "sku", 
                "unit_price", 
                "display_order", 
                "launch_date", 
                "largest_uom_quantity", 
                "middle_uom_quantity", 
                "smallest_uom_quantity", 
                "company", 
                "sku_english", 
                "sku_code", 
                "ps_category", 
                "ps_segment", 
                "ps_category_segment", 
                "country_l1", 
                "regional_franchise_l2", 
                "franchise_l3", 
                "Brand__L4", 
                "Sub_Category__L5", 
                "Platform_Level__L6", 
                "Variance__L7", 
                "Pack_Size__L8",
                "file_name",
                "run_id",
                "crtd_dttm",
                "hashkey"
            
                    )
        file_name=file_name.split(".")[0]+"_"+datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")

        snowdf.write.mode("append").saveAsTable(stage_name.split(".")[0] + "." + target_table)
        
        # #move to success
        snowdf.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)
        
        
        
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
CREATE OR REPLACE PROCEDURE POP6_TW_MASTER_USERS_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them.

from snowflake.snowpark.functions import col, lit, date_format, current_timestamp, to_date, year, month, concat, format_number, regexp_replace,to_timestamp,when,trim,upper,is_null,md5
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DecimalType
import snowflake.snowpark
import pandas as pd
from datetime import datetime,timedelta
import snowflake.snowpark as snowpark
import pytz
import pandas as pd

def main(session:snowpark.Session,Param):
    # Param=["20240616_users.csv","NTASDL_RAW.DEV_LOAD_STAGE_ADLS_TWN","dev/pop6/masterdata/users/","sdl_pop6_tw_users"]
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        
        df_schema = StructType([
                StructField("Status", StringType(), True),
                StructField("USERDB_ID", StringType(), True),
                StructField("Username", StringType(), True),
                StructField("FIRST_NAME", StringType(), True),
                StructField("Last_Name", StringType(), True),
                StructField("Team", StringType(), True),
                StructField("Superior_Name", StringType(), True),
                StructField("Authorisation_Group", StringType(), True),
                StructField("Email_Address", StringType(), True),
                StructField("Longitude", StringType(), True),
                StructField("Latitude", StringType(), True),
                StructField("Business_Units_ID", StringType(), True),
                StructField("Business_Unit_Name", StringType(), True)
                
            ])
        
        df = session.read.option("INFER_SCHEMA", True)\\
            .option("field_delimiter", ",")\\
            .option("field_optionally_enclosed_by", "\\"") \\
            .option("trim_space", True) \\
            .with_metadata("METADATA$FILE_ROW_NUMBER") \\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        df_columns=df.first()[1:]
        
        df = df.filter(col("METADATA$FILE_ROW_NUMBER") > lit(1))
        
        
        df = df.drop("METADATA$FILE_ROW_NUMBER")
        #return df.schema.names
        o=0

        for i in df_columns:
            

            df=df.withColumnRenamed(df.schema.names[o],i.replace(" ", "_").replace("(","").replace(")","").replace("]","").replace("[",""))
            #df=df.withColumnRenamed(df.schema.names[o],(re.sub(r"[^a-zA-Z0-9_]","",i.replace(" ", "_").replace("(","").replace(")","").replace("]","").replace("[",""))).strip("_"))
            o+=1

       
        
        df=df.na.drop("all")
        if df.count()==0 :
            return "No Data in file"

        df = df.with_column(''file_name'',lit(file_name))


        def generunid():
            new_time = datetime.now(pytz.utc) + timedelta(hours=8)
            return new_time.strftime(''%Y%m%d%H%M%S'')

        def generatecrtd_dttm():
            new_time = datetime.now(pytz.utc) + timedelta(hours=8)
            return new_time.strftime(''%Y-%m-%d %H:%M:%S'')

           


            
            

        df = df.with_column(''hashkey'',md5(concat(when(is_null(col(''status'')),lit('''')).otherwise(col(''status'')),\\
                                            when(is_null(col(''USERDB_ID'')),lit('''')).otherwise(col(''USERDB_ID'')),\\
                                          when(is_null(col(''Username'')),lit('''')).otherwise(col(''Username'')),\\
                                          when(is_null(col(''FIRST_NAME'')),lit('''')).otherwise(col(''FIRST_NAME'')),\\
                                         when(is_null(col(''Last_Name'')),lit('''')).otherwise(col(''Last_Name'')),\\
                                         when(is_null(col(''Team'')),lit('''')).otherwise(col(''Team'')),\\
                                         when(is_null(col(''Superior_Name'')),lit('''')).otherwise(col(''Superior_Name'')),\\
                                          when(is_null(col(''Authorisation_Group'')),lit('''')).otherwise(col(''Authorisation_Group'')),\\
                                         when(is_null(col(''Email_Address'')),lit('''')).otherwise(col(''Email_Address'')),\\
                                         when(is_null(col(''Longitude'')),lit('''')).otherwise(col(''Longitude'')),\\
                                         when(is_null(col(''Latitude'')),lit('''')).otherwise(col(''Latitude'')),\\
                                         when(is_null(col(''Business_Units_ID'')),lit('''')).otherwise(col(''Business_Units_ID'')),
                                         when(is_null(col(''Business_Unit_Name'')),lit('''')).otherwise(col(''Business_Unit_Name'')))))

        
        df = df.with_column(''run_id'',lit(generunid()))

        df = df.with_column(''crtd_dttm'',lit(generatecrtd_dttm()))
        

        

        
        snowdf= df.select(
                    "status",
                    "userdb_id",
                    "username",
                    "first_name",
                    "last_name",
                    "team",
                    "superior_name",
                    "authorisation_group",
                    "email_address",
                    "longitude",
                    "latitude",
                    "file_name",
                    "run_id",
                    "crtd_dttm",
                    "hashkey",
                    "business_units_id",
                    "business_unit_name"
            
                    )
        file_name=file_name.split(".")[0]+"_"+datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")

        snowdf.write.mode("append").saveAsTable(stage_name.split(".")[0] + "." + target_table)
        
        # #move to success
        snowdf.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)
        
        
        
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
CREATE OR REPLACE PROCEDURE POP6_TW_MASTER_POP_LISTS_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them.

from snowflake.snowpark.functions import col, lit, date_format, current_timestamp, to_date, year, month, concat, format_number, regexp_replace,to_timestamp,when,trim,upper,is_null,md5
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DecimalType
import snowflake.snowpark
import pandas as pd
from datetime import datetime,timedelta
import snowflake.snowpark as snowpark
import pytz
import pandas as pd

def main(session:snowpark.Session,Param):
    # Param=["20240617_pop_lists.csv","NTASDL_RAW.DEV_LOAD_STAGE_ADLS_TWN","dev/pop6/listdata/pop_lists","sdl_pop6_tw_pop_lists"]
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        
       
            
        
        df = session.read.option("INFER_SCHEMA", True)\\
            .option("field_delimiter", ",")\\
            .option("field_optionally_enclosed_by", "\\"") \\
            .option("trim_space", True) \\
            .with_metadata("METADATA$FILE_ROW_NUMBER") \\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        df_columns=df.first()[1:]
        
        df = df.filter(col("METADATA$FILE_ROW_NUMBER") > lit(1))
        
        
        df = df.drop("METADATA$FILE_ROW_NUMBER")
        #return df.schema.names
        o=0

        for i in df_columns:
            

            df=df.withColumnRenamed(df.schema.names[o],i.replace(" ", "_").replace("(","").replace(")","").replace("]","").replace("[",""))
            #df=df.withColumnRenamed(df.schema.names[o],(re.sub(r"[^a-zA-Z0-9_]","",i.replace(" ", "_").replace("(","").replace(")","").replace("]","").replace("[",""))).strip("_"))
            o+=1

       
        
        df=df.na.drop("all")
        if df.count()==0 :
            return "No Data in file"

        df = df.with_column(''file_name'',lit(file_name))


        def generunid():
            new_time = datetime.now(pytz.utc) + timedelta(hours=8)
            return new_time.strftime(''%Y%m%d%H%M%S'')

        def generatecrtd_dttm():
            new_time = datetime.now(pytz.utc) + timedelta(hours=8)
            return new_time.strftime(''%Y-%m-%d %H:%M:%S'')
            
            

        df = df.with_column(''hashkey'',md5(concat(when(is_null(col(''status'')),lit('''')).otherwise(col(''status'')),\\
                                            when(is_null(col(''pop_list'')),lit('''')).otherwise(col(''pop_list'')),\\
                                          when(is_null(col(''popdb_id'')),lit('''')).otherwise(col(''popdb_id'')),\\
                                          when(is_null(col(''pop_code'')),lit('''')).otherwise(col(''pop_code'')),\\
                                         when(is_null(col(''pop_name'')),lit('''')).otherwise(col(''pop_name'')),\\
                                         when(is_null(col(''date'')),lit('''')).otherwise(col(''date'')))))

        
        df = df.with_column(''run_id'',lit(generunid()))

        df = df.with_column(''crtd_dttm'',lit(generatecrtd_dttm()))
        

        

        
        snowdf= df.select(
                    "status",
                    "pop_list",
                    "popdb_id",
                    "pop_code",
                    "pop_name",
                    "date",
                    "file_name",
                    "run_id",
                    "crtd_dttm",
                    "hashkey"
            
                    )
        file_name=file_name.split(".")[0]+"_"+datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")

        snowdf.write.mode("append").saveAsTable(stage_name.split(".")[0] + "." + target_table)
        
        # #move to success
        snowdf.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)
        
        
        
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
CREATE OR REPLACE PROCEDURE POP6_TW_MASTER_PRODUCT_LISTS_ALLOCATION_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them.

from snowflake.snowpark.functions import col, lit, date_format, current_timestamp, to_date, year, month, concat, format_number, regexp_replace,to_timestamp,when,trim,upper,is_null,md5
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DecimalType
import snowflake.snowpark
import pandas as pd
from datetime import datetime,timedelta
import snowflake.snowpark as snowpark
import pytz
import pandas as pd

def main(session:snowpark.Session,Param):
    # Param=["20240617_product_lists_allocation.csv","NTASDL_RAW.DEV_LOAD_STAGE_ADLS_TWN","dev/pop6/listdata/product_lists_allocation","sdl_pop6_tw_product_lists_allocation"]
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        
        
        
        df = session.read.option("INFER_SCHEMA", True)\\
            .option("field_delimiter", ",")\\
            .option("field_optionally_enclosed_by", "\\"") \\
            .option("trim_space", True) \\
            .with_metadata("METADATA$FILE_ROW_NUMBER") \\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        df_columns=df.first()[1:]
        
        df = df.filter(col("METADATA$FILE_ROW_NUMBER") > lit(1))
        
        
        df = df.drop("METADATA$FILE_ROW_NUMBER")
        #return df.schema.names
        o=0

        for i in df_columns:
            

            df=df.withColumnRenamed(df.schema.names[o],i.replace(" ", "_").replace("(","").replace(")","").replace("]","").replace("[",""))
            #df=df.withColumnRenamed(df.schema.names[o],(re.sub(r"[^a-zA-Z0-9_]","",i.replace(" ", "_").replace("(","").replace(")","").replace("]","").replace("[",""))).strip("_"))
            o+=1

       
        
        df=df.na.drop("all")
        if df.count()==0 :
            return "No Data in file"

        df = df.with_column(''file_name'',lit(file_name))


        def generunid():
            new_time = datetime.now(pytz.utc) + timedelta(hours=8)
            return new_time.strftime(''%Y%m%d%H%M%S'')

        def generatecrtd_dttm():
            new_time = datetime.now(pytz.utc) + timedelta(hours=8)
            return new_time.strftime(''%Y-%m-%d %H:%M:%S'')
            
        
    

        df = df.with_column(''hashkey'',md5(concat(when(is_null(col(''Product_Group_Status'')),lit('''')).otherwise(col(''Product_Group_Status'')),\\
                                            when(is_null(col(''Product_Group'')),lit('''')).otherwise(col(''Product_Group'')),\\
                                          when(is_null(col(''Product_List_Status'')),lit('''')).otherwise(col(''Product_List_Status'')),\\
                                          when(is_null(col(''Product_List'')),lit('''')).otherwise(col(''Product_List'')),\\
                                         when(is_null(col(''POP_Attribute_ID'')),lit('''')).otherwise(col(''POP_Attribute_ID'')),\\
                                         when(is_null(col(''POP_Attribute'')),lit('''')).otherwise(col(''POP_Attribute'')),\\
                                           when(is_null(col(''POP_Attribute_Value_ID'')),lit('''')).otherwise(col(''POP_Attribute_Value_ID'')),\\
                                          when(is_null(col(''POP_Attribute_Value'')),lit('''')).otherwise(col(''POP_Attribute_Value'')),\\
                                         when(is_null(col(''date'')),lit('''')).otherwise(col(''date'')))))

        
        df = df.with_column(''run_id'',lit(generunid()))

        df = df.with_column(''crtd_dttm'',lit(generatecrtd_dttm()))
        

        

        
        snowdf= df.select(
                    "Product_Group_Status",
                    "Product_Group",
                    "Product_List_Status",
                    "Product_List",
                    "POP_Attribute_ID",
                    "POP_Attribute",
                    "POP_Attribute_Value_ID",
                    "POP_Attribute_Value",
                    "date",
                    "file_name",
                    "run_id",
                    "crtd_dttm",
                    "hashkey"
            
                    )
        file_name=file_name.split(".")[0]+"_"+datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")

        snowdf.write.mode("append").saveAsTable(stage_name.split(".")[0] + "." + target_table)
        
        # #move to success
        snowdf.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)
        
        
        
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
CREATE OR REPLACE PROCEDURE POP6_TW_MASTER_PRODUCT_LISTS_POPS_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them.

from snowflake.snowpark.functions import col, lit, date_format, current_timestamp, to_date, year, month, concat, format_number, regexp_replace,to_timestamp,when,trim,upper,is_null,md5
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DecimalType
import snowflake.snowpark
import pandas as pd
from datetime import datetime,timedelta
import snowflake.snowpark as snowpark
import pytz
import pandas as pd

def main(session:snowpark.Session,Param):
    # Param=["20240617_product_lists_pops.csv","NTASDL_RAW.DEV_LOAD_STAGE_ADLS_TWN","dev/pop6/listdata/product_lists_pops","sdl_pop6_tw_product_lists_pops"]
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        
       
        
        df = session.read.option("INFER_SCHEMA", True)\\
            .option("field_delimiter", ",")\\
            .option("field_optionally_enclosed_by", "\\"") \\
            .option("trim_space", True) \\
            .with_metadata("METADATA$FILE_ROW_NUMBER") \\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        df_columns=df.first()[1:]
        
        df = df.filter(col("METADATA$FILE_ROW_NUMBER") > lit(1))
        
        
        df = df.drop("METADATA$FILE_ROW_NUMBER")
        #return df.schema.names
        o=0

        for i in df_columns:
            

            df=df.withColumnRenamed(df.schema.names[o],i.replace(" ", "_").replace("(","").replace(")","").replace("]","").replace("[",""))
            #df=df.withColumnRenamed(df.schema.names[o],(re.sub(r"[^a-zA-Z0-9_]","",i.replace(" ", "_").replace("(","").replace(")","").replace("]","").replace("[",""))).strip("_"))
            o+=1

       
        
        df=df.na.drop("all")
        if df.count()==0 :
            return "No Data in file"

        df = df.with_column(''file_name'',lit(file_name))


        def generunid():
            new_time = datetime.now(pytz.utc) + timedelta(hours=8)
            return new_time.strftime(''%Y%m%d%H%M%S'')

        def generatecrtd_dttm():
            new_time = datetime.now(pytz.utc) + timedelta(hours=8)
            return new_time.strftime(''%Y-%m-%d %H:%M:%S'')
            
            

        df = df.with_column(''hashkey'',md5(concat(when(is_null(col(''product_list'')),lit('''')).otherwise(col(''product_list'')),\\
                                            when(is_null(col(''Product_List_Code'')),lit('''')).otherwise(col(''Product_List_Code'')),\\
                                          when(is_null(col(''POPDB_ID'')),lit('''')).otherwise(col(''POPDB_ID'')),\\
                                          when(is_null(col(''POP_Code'')),lit('''')).otherwise(col(''POP_Code'')),\\
                                         when(is_null(col(''POP_Name'')),lit('''')).otherwise(col(''POP_Name'')),\\
                                            when(is_null(col(''date'')),lit('''')).otherwise(col(''date'')))))

        
        df = df.with_column(''run_id'',lit(generunid()))

        df = df.with_column(''crtd_dttm'',lit(generatecrtd_dttm()))
        

        

        
        snowdf= df.select(
                    "product_list",
                    "popdb_id",
                    "pop_code",
                    "pop_name",
                    "date",
                    "file_name",
                    "run_id",
                    "crtd_dttm",
                    "hashkey"
                    
            
                    )
        file_name=file_name.split(".")[0]+"_"+datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")

        snowdf.write.mode("append").saveAsTable(stage_name.split(".")[0] + "." + target_table)
        
        # #move to success
        snowdf.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)
        
        
        
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
CREATE OR REPLACE PROCEDURE POP6_TW_MASTER_PRODUCT_LISTS_PRODUCTS_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them.

from snowflake.snowpark.functions import col, lit, date_format, current_timestamp, to_date, year, month, concat, format_number, regexp_replace,to_timestamp,when,trim,upper,is_null,md5
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DecimalType
import snowflake.snowpark
import pandas as pd
from datetime import datetime,timedelta
import snowflake.snowpark as snowpark
import pytz
import pandas as pd

def main(session:snowpark.Session,Param):
    # Param=["20240617_product_lists_products.csv","NTASDL_RAW.DEV_LOAD_STAGE_ADLS_TWN","dev/pop6/listdata/product_lists_products/","sdl_pop6_tw_product_lists_products"]
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        
        
        
        df = session.read.option("INFER_SCHEMA", True)\\
            .option("field_delimiter", ",")\\
            .option("field_optionally_enclosed_by", "\\"") \\
            .option("trim_space", True) \\
            .with_metadata("METADATA$FILE_ROW_NUMBER") \\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        df_columns=df.first()[1:]
        
        df = df.filter(col("METADATA$FILE_ROW_NUMBER") > lit(1))
        
        
        df = df.drop("METADATA$FILE_ROW_NUMBER")
        #return df.schema.names
        o=0

        for i in df_columns:
            

            df=df.withColumnRenamed(df.schema.names[o],i.replace(" ", "_").replace("(","").replace(")","").replace("]","").replace("[",""))
            #df=df.withColumnRenamed(df.schema.names[o],(re.sub(r"[^a-zA-Z0-9_]","",i.replace(" ", "_").replace("(","").replace(")","").replace("]","").replace("[",""))).strip("_"))
            o+=1

       
        
        df=df.na.drop("all")
        if df.count()==0 :
            return "No Data in file"

        df = df.with_column(''file_name'',lit(file_name))


        def generunid():
            new_time = datetime.now(pytz.utc) + timedelta(hours=8)
            return new_time.strftime(''%Y%m%d%H%M%S'')

        def generatecrtd_dttm():
            new_time = datetime.now(pytz.utc) + timedelta(hours=8)
            return new_time.strftime(''%Y-%m-%d %H:%M:%S'')
            
            

        df = df.with_column(''hashkey'',md5(concat(when(is_null(col(''Product_List'')),lit('''')).otherwise(col(''Product_List'')),\\
                                            when(is_null(col(''Product_List_Code'')),lit('''')).otherwise(col(''Product_List_Code'')),\\
                                          when(is_null(col(''ProductDB_ID'')),lit('''')).otherwise(col(''ProductDB_ID'')),\\
                                          when(is_null(col(''SKU'')),lit('''')).otherwise(col(''SKU'')),\\
                                         when(is_null(col(''MSL_Ranking'')),lit('''')).otherwise(col(''MSL_Ranking'')),\\
                                            when(is_null(col(''Date'')),lit('''')).otherwise(col(''Date'')))))

        
        df = df.with_column(''run_id'',lit(generunid()))

        df = df.with_column(''crtd_dttm'',lit(generatecrtd_dttm()))
        

        

        
        snowdf= df.select(
                    "product_list",
                    "productdb_id",
                    "sku",
                    "msl_ranking",
                    "date",
                    "file_name",
                    "run_id",
                    "crtd_dttm",
                    "hashkey"
                    
            
                    )
        file_name=file_name.split(".")[0]+"_"+datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")

        snowdf.write.mode("append").saveAsTable(stage_name.split(".")[0] + "." + target_table)
        
        # #move to success
        snowdf.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)
        
        
        
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
CREATE OR REPLACE PROCEDURE POP6_HK_MASTER_USERS_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them.

from snowflake.snowpark.functions import col, lit, date_format, current_timestamp, to_date, year, month, concat, format_number, regexp_replace,to_timestamp,when,trim,upper,is_null,md5
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DecimalType
import snowflake.snowpark
import pandas as pd
from datetime import datetime,timedelta
import snowflake.snowpark as snowpark
import pytz
import pandas as pd

def main(session:snowpark.Session,Param):
    #Param=["20240610_users.csv","NTASDL_RAW.DEV_LOAD_STAGE_ADLS_HKG","dev/pop6/masterdata/users/","sdl_pop6_hk_users"]
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        
        df_schema = StructType([
                StructField("Status", StringType(), True),
                StructField("USERDB_ID", StringType(), True),
                StructField("Username", StringType(), True),
                StructField("FIRST_NAME", StringType(), True),
                StructField("Last_Name", StringType(), True),
                StructField("Team", StringType(), True),
                StructField("Superior_Name", StringType(), True),
                StructField("Authorisation_Group", StringType(), True),
                StructField("Email_Address", StringType(), True),
                StructField("Longitude", StringType(), True),
                StructField("Latitude", StringType(), True),
                StructField("Business_Units_ID", StringType(), True),
                StructField("Business_Unit_Name", StringType(), True)
                
            ])
        
        df = session.read.option("INFER_SCHEMA", True)\\
            .option("field_delimiter", ",")\\
            .option("field_optionally_enclosed_by", "\\"") \\
            .option("trim_space", True) \\
            .with_metadata("METADATA$FILE_ROW_NUMBER") \\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        df_columns=df.first()[1:]
        
        df = df.filter(col("METADATA$FILE_ROW_NUMBER") > lit(1))
        
        
        df = df.drop("METADATA$FILE_ROW_NUMBER")
        #return df.schema.names
        o=0

        for i in df_columns:
            

            df=df.withColumnRenamed(df.schema.names[o],i.replace(" ", "_").replace("(","").replace(")","").replace("]","").replace("[",""))
            #df=df.withColumnRenamed(df.schema.names[o],(re.sub(r"[^a-zA-Z0-9_]","",i.replace(" ", "_").replace("(","").replace(")","").replace("]","").replace("[",""))).strip("_"))
            o+=1

       
        
        df=df.na.drop("all")
        if df.count()==0 :
            return "No Data in file"

        df = df.with_column(''file_name'',lit(file_name))


        def generunid():
            new_time = datetime.now(pytz.utc) + timedelta(hours=8)
            return new_time.strftime(''%Y%m%d%H%M%S'')

        def generatecrtd_dttm():
            new_time = datetime.now(pytz.utc) + timedelta(hours=8)
            return new_time.strftime(''%Y-%m-%d %H:%M:%S'')
            


        df = df.with_column(''hashkey'',md5(concat(when(is_null(col(''status'')),lit('''')).otherwise(col(''status'')),\\
                                            when(is_null(col(''USERDB_ID'')),lit('''')).otherwise(col(''USERDB_ID'')),\\
                                          when(is_null(col(''Username'')),lit('''')).otherwise(col(''Username'')),\\
                                          when(is_null(col(''FIRST_NAME'')),lit('''')).otherwise(col(''FIRST_NAME'')),\\
                                         when(is_null(col(''Last_Name'')),lit('''')).otherwise(col(''Last_Name'')),\\
                                         when(is_null(col(''Team'')),lit('''')).otherwise(col(''Team'')),\\
                                         when(is_null(col(''Superior_Name'')),lit('''')).otherwise(col(''Superior_Name'')),\\
                                          when(is_null(col(''Authorisation_Group'')),lit('''')).otherwise(col(''Authorisation_Group'')),\\
                                         when(is_null(col(''Email_Address'')),lit('''')).otherwise(col(''Email_Address'')),\\
                                         when(is_null(col(''Longitude'')),lit('''')).otherwise(col(''Longitude'')),\\
                                         when(is_null(col(''Latitude'')),lit('''')).otherwise(col(''Latitude'')),\\
                                         when(is_null(col(''Business_Units_ID'')),lit('''')).otherwise(col(''Business_Units_ID'')),
                                         when(is_null(col(''Business_Unit_Name'')),lit('''')).otherwise(col(''Business_Unit_Name'')))))

        
        df = df.with_column(''run_id'',lit(generunid()))

        df = df.with_column(''crtd_dttm'',lit(generatecrtd_dttm()))
        

        

        
        snowdf= df.select(
                    "status",
                    "userdb_id",
                    "username",
                    "first_name",
                    "last_name",
                    "team",
                    "superior_name",
                    "authorisation_group",
                    "email_address",
                    "longitude",
                    "latitude",
                    "file_name",
                    "run_id",
                    "crtd_dttm",
                    "hashkey",
                    "business_units_id",
                    "business_unit_name"
            
                    )
        file_name=file_name.split(".")[0]+"_"+datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")

        snowdf.write.mode("append").saveAsTable(stage_name.split(".")[0] + "." + target_table)
        
        # #move to success
        snowdf.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)
        
        
        
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
CREATE OR REPLACE PROCEDURE POP6_HK_MASTER_POPS_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them.

from snowflake.snowpark.functions import col, lit, date_format, current_timestamp, to_date, year, month, concat, format_number, regexp_replace,to_timestamp,when,trim,upper,is_null,md5
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DecimalType
import snowflake.snowpark
import pandas as pd
from datetime import datetime,timedelta
import snowflake.snowpark as snowpark
import pytz
import pandas as pd

def main(session:snowpark.Session,Param):
    #Param=["20240609_pops.csv","NTASDL_RAW.DEV_LOAD_STAGE_ADLS_HKG","dev/pop6/masterdata/pops/","sdl_pop6_hk_pops"]
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        
        df_schema = StructType([
                StructField("Status", StringType(), True),
                StructField("popdb_id", StringType(), True),
                StructField("pop_code", StringType(), True),
                StructField("pop_name", StringType(), True),
                StructField("address", StringType(), True),
                StructField("longitude", StringType(), True),
                StructField("latitude", StringType(), True),
                StructField("Business_Units_ID", StringType(), True),
                StructField("Business_Unit_Name", StringType(), True),
                StructField("country", StringType(), True),
                StructField("channel", StringType(), True),
                StructField("retail_environment_ps", StringType(), True),
                StructField("customer", StringType(), True),
                StructField("sales_group_code", StringType(), True),
                StructField("sales_group_name", StringType(), True),
                StructField("Store_Type", StringType(), True),
                StructField("Customer_Grade", StringType(), True),
                StructField("External_Store_Code", StringType(), True),
                StructField("territory", StringType(), True),
               
                
                
            ])
        
        df = session.read.option("INFER_SCHEMA", True)\\
            .option("field_delimiter", ",")\\
            .option("field_optionally_enclosed_by", "\\"") \\
            .option("trim_space", True) \\
            .with_metadata("METADATA$FILE_ROW_NUMBER") \\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        df_columns=df.first()[1:]
        
        df = df.filter(col("METADATA$FILE_ROW_NUMBER") > lit(1))
        
        
        df = df.drop("METADATA$FILE_ROW_NUMBER")
        #return df.schema.names
        o=0

        for i in df_columns:
            

            df=df.withColumnRenamed(df.schema.names[o],i.replace(" ", "_").replace("(","").replace(")","").replace("]","").replace("[",""))
            #df=df.withColumnRenamed(df.schema.names[o],(re.sub(r"[^a-zA-Z0-9_]","",i.replace(" ", "_").replace("(","").replace(")","").replace("]","").replace("[",""))).strip("_"))
            o+=1

       
        
        df=df.na.drop("all")
        if df.count()==0 :
            return "No Data in file"

        df = df.with_column(''file_name'',lit(file_name))


        def generunid():
            new_time = datetime.now(pytz.utc) + timedelta(hours=8)
            return new_time.strftime(''%Y%m%d%H%M%S'')

        def generatecrtd_dttm():
            new_time = datetime.now(pytz.utc) + timedelta(hours=8)
            return new_time.strftime(''%Y-%m-%d %H:%M:%S'')
            
            

        df = df.with_column(''hashkey'',md5(concat(when(is_null(col(''status'')),lit('''')).otherwise(col(''status'')),\\
                                            when(is_null(col(''popdb_id'')),lit('''')).otherwise(col(''popdb_id'')),\\
                                          when(is_null(col(''pop_code'')),lit('''')).otherwise(col(''pop_code'')),\\
                                          when(is_null(col(''pop_name'')),lit('''')).otherwise(col(''pop_name'')),\\
                                         when(is_null(col(''address'')),lit('''')).otherwise(col(''address'')),\\
                                         when(is_null(col(''longitude'')),lit('''')).otherwise(col(''longitude'')),\\
                                         when(is_null(col(''latitude'')),lit('''')).otherwise(col(''latitude'')),\\
                                          when(is_null(col(''country'')),lit('''')).otherwise(col(''country'')),\\
                                         when(is_null(col(''channel'')),lit('''')).otherwise(col(''channel'')),\\
                                         when(is_null(col(''retail_environment_ps'')),lit('''')).otherwise(col(''retail_environment_ps'')),\\
                                         when(is_null(col(''customer'')),lit('''')).otherwise(col(''customer'')),\\
                                         when(is_null(col(''sales_group_code'')),lit('''')).otherwise(col(''sales_group_code'')),\\
                                         when(is_null(col(''sales_group_name'')),lit('''')).otherwise(col(''sales_group_name'')),\\
                                        when(is_null(col(''customer_grade'')),lit('''')).otherwise(col(''customer_grade'')),\\
                                          when(is_null(col(''external_store_code'')),lit('''')).otherwise(col(''external_store_code'')),
                                                when(is_null(col(''business_units_id'')),lit('''')).otherwise(col(''business_units_id'')),\\
                                          when(is_null(col(''Territory/Region'')),lit('''')).otherwise(col(''Territory/Region'')))))
                                         
        
        df = df.with_column(''run_id'',lit(generunid()))

        df = df.with_column(''crtd_dttm'',lit(generatecrtd_dttm()))
        

        

        
        snowdf= df.select(
                    "Status",
                "popdb_id",
                "pop_code",
                "pop_name",
                "address",
                "longitude",
                "latitude",
                "country",
                "channel",
                "retail_environment_ps",
                "customer",
                "sales_group_code",
                "sales_group_name",
                "customer_grade",
                "external_store_code",
                "file_name",
                "run_id",
                "crtd_dttm",
                "hashkey",
                "business_units_id",
                "business_unit_name",
                "Territory/Region" 
                
            
                    )
        file_name=file_name.split(".")[0]+"_"+datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")

        snowdf.write.mode("append").saveAsTable(stage_name.split(".")[0] + "." + target_table)
        
        # #move to success
        snowdf.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)
        
        
        
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
CREATE OR REPLACE PROCEDURE POP6_HK_MASTER_PRODUCTS_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them.

from snowflake.snowpark.functions import col, lit, date_format, current_timestamp, to_date, year, month, concat, format_number, regexp_replace,to_timestamp,when,trim,upper,is_null,md5
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DecimalType
import snowflake.snowpark
import pandas as pd
from datetime import datetime,timedelta
import snowflake.snowpark as snowpark
import pytz
import pandas as pd

def main(session:snowpark.Session,Param):
    #Param=["20240609_products.csv","NTASDL_RAW.DEV_LOAD_STAGE_ADLS_HKG","dev/pop6/masterdata/products/","sdl_pop6_hk_products"]
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        
        df_schema = StructType([
                StructField("Status", StringType(), True),
                StructField("productdb_id", StringType(), True),
                StructField("barcode", StringType(), True),
                StructField("sku", StringType(), True),
                StructField("unit_price", StringType(), True),
                StructField("display_order", StringType(), True),
                StructField("launch_date", StringType(), True),
                StructField("largest_uom_quantity", StringType(), True),
                StructField("middle_uom_quantity", StringType(), True),
                StructField("smallest_uom_quantity", StringType(), True),
                StructField("sku_english", StringType(), True),
                StructField("sku_code", StringType(), True),
                StructField("ps_category", StringType(), True),
                StructField("ps_segment", StringType(), True),
                StructField("ps_category_segment", StringType(), True),
                StructField("company", StringType(), True),
                StructField("country_l1", StringType(), True),
                StructField("regional_franchise_l2", StringType(), True),
                StructField("franchise_l3", StringType(), True),
                StructField("brand_l4", StringType(), True),
                StructField("sub_category_l5", StringType(), True),
                StructField("platform_level_l6", StringType(), True),
                StructField("variance_l7", StringType(), True),
                StructField("pack_size_l8", StringType(), True)
                
            ])
        
        df = session.read.option("INFER_SCHEMA", True)\\
            .option("field_delimiter", ",")\\
            .option("field_optionally_enclosed_by", "\\"") \\
            .option("trim_space", True) \\
            .with_metadata("METADATA$FILE_ROW_NUMBER") \\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        df_columns=df.first()[1:]
        
        df = df.filter(col("METADATA$FILE_ROW_NUMBER") > lit(1))
        
        
        df = df.drop("METADATA$FILE_ROW_NUMBER")
        #return df.schema.names
        o=0

        for i in df_columns:
            

            df=df.withColumnRenamed(df.schema.names[o],i.replace(" ", "_").replace("(","").replace(")","").replace("]","").replace("[",""))
            #df=df.withColumnRenamed(df.schema.names[o],(re.sub(r"[^a-zA-Z0-9_]","",i.replace(" ", "_").replace("(","").replace(")","").replace("]","").replace("[",""))).strip("_"))
            o+=1

       
        
        df=df.na.drop("all")
        if df.count()==0 :
            return "No Data in file"

        df = df.with_column(''file_name'',lit(file_name))


        def generunid():
            new_time = datetime.now(pytz.utc) + timedelta(hours=8)
            return new_time.strftime(''%Y%m%d%H%M%S'')

        def generatecrtd_dttm():
            new_time = datetime.now(pytz.utc) + timedelta(hours=8)
            return new_time.strftime(''%Y-%m-%d %H:%M:%S'')
            


        df = df.with_column(''hashkey'',md5(concat(when(is_null(col(''status'')),lit('''')).otherwise(col(''status'')),\\
                                            when(is_null(col(''productdb_id'')),lit('''')).otherwise(col(''productdb_id'')),\\
                                          when(is_null(col(''barcode'')),lit('''')).otherwise(col(''barcode'')),\\
                                          when(is_null(col(''sku'')),lit('''')).otherwise(col(''sku'')),\\
                                         when(is_null(col(''unit_price'')),lit('''')).otherwise(col(''unit_price'')),\\
                                         when(is_null(col(''display_order'')),lit('''')).otherwise(col(''display_order'')),\\
                                         when(is_null(col(''launch_date'')),lit('''')).otherwise(col(''launch_date'')),\\
                                          when(is_null(col(''largest_uom_quantity'')),lit('''')).otherwise(col(''largest_uom_quantity'')),\\
                                         when(is_null(col(''middle_uom_quantity'')),lit('''')).otherwise(col(''middle_uom_quantity'')),\\
                                         when(is_null(col(''smallest_uom_quantity'')),lit('''')).otherwise(col(''smallest_uom_quantity'')),\\
                                         when(is_null(col(''company'')),lit('''')).otherwise(col(''company'')),\\
                                         when(is_null(col(''sku_english'')),lit('''')).otherwise(col(''sku_english'')),\\
                                         when(is_null(col(''sku_code'')),lit('''')).otherwise(col(''sku_code'')),\\
                                        when(is_null(col(''ps_category'')),lit('''')).otherwise(col(''ps_category'')),\\
                                          when(is_null(col(''ps_segment'')),lit('''')).otherwise(col(''ps_segment'')),\\
                                         when(is_null(col(''ps_category_segment'')),lit('''')).otherwise(col(''ps_category_segment'')),\\
                                         when(is_null(col(''Country-L1'')),lit('''')).otherwise(col(''Country-L1'')),\\
                                         when(is_null(col(''Regional_Franchise-L2'')),lit('''')).otherwise(col(''Regional_Franchise-L2'')),\\
                                          when(is_null(col(''Franchise-L3'')),lit('''')).otherwise(col(''Franchise-L3'')),\\
                                         when(is_null(col(''Brand_-L4'')),lit('''')).otherwise(col(''Brand_-L4'')),\\
                                         when(is_null(col(''Sub_Category-_L5'')),lit('''')).otherwise(col(''Sub_Category-_L5'')),\\
                                         when(is_null(col(''Platform_Level-L6'')),lit('''')).otherwise(col(''Platform_Level-L6'')),\\
                                         when(is_null(col(''Variance-L7'')),lit('''')).otherwise(col(''Variance-L7'')),\\
                                         when(is_null(col(''Pack_Size-L8'')),lit('''')).otherwise(col(''Pack_Size-L8'')))))

        
        df = df.with_column(''run_id'',lit(generunid()))

        df = df.with_column(''crtd_dttm'',lit(generatecrtd_dttm()))
        

        

  
        snowdf= df.select(
                   "Status", 
                "productdb_id", 
                "barcode", 
                "sku", 
                "unit_price", 
                "display_order", 
                "launch_date", 
                "largest_uom_quantity", 
                "middle_uom_quantity", 
                "smallest_uom_quantity", 
                "company", 
                "sku_english", 
                "sku_code", 
                "ps_category", 
                "ps_segment", 
                "ps_category_segment", 
                "Country-L1",
                "Regional_Franchise-L2",
                "Franchise-L3",
                "Brand_-L4",
                "Sub_Category-_L5",
                "Platform_Level-L6", 
                "Variance-L7",
                "Pack_Size-L8",
                "file_name",
                "run_id",
                "crtd_dttm",
                "hashkey"
            
                    )
        file_name=file_name.split(".")[0]+"_"+datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")

        snowdf.write.mode("append").saveAsTable(stage_name.split(".")[0] + "." + target_table)
        
        # #move to success
        snowdf.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)
        
        
        
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
CREATE OR REPLACE PROCEDURE POP6_HK_MASTER_POP_LISTS_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them.

from snowflake.snowpark.functions import col, lit, date_format, current_timestamp, to_date, year, month, concat, format_number, regexp_replace,to_timestamp,when,trim,upper,is_null,md5
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DecimalType
import snowflake.snowpark
import pandas as pd
from datetime import datetime,timedelta
import snowflake.snowpark as snowpark
import pytz
import pandas as pd

def main(session:snowpark.Session,Param):
    # Param=["20240617_pop_lists.csv","JPNSDL_RAW.DEV_LOAD_STAGE_ADLS","dev/pop6/master/listdata/pop_lists","sdl_pop6_jp_pop_lists"]
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        
        df_schema = StructType([
                StructField("Status", StringType(), True),
                StructField("pop_list", StringType(), True),
                StructField("popdb_id", StringType(), True),
                StructField("pop_code", StringType(), True),
                StructField("pop_name", StringType(), True),
                StructField("Date", StringType(), True)
                
                
            ])
        
        df = session.read.option("INFER_SCHEMA", True)\\
            .option("field_delimiter", ",")\\
            .option("field_optionally_enclosed_by", "\\"") \\
            .option("trim_space", True) \\
            .with_metadata("METADATA$FILE_ROW_NUMBER") \\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        df_columns=df.first()[1:]
        
        df = df.filter(col("METADATA$FILE_ROW_NUMBER") > lit(1))
        
        
        df = df.drop("METADATA$FILE_ROW_NUMBER")
        #return df.schema.names
        o=0

        for i in df_columns:
            

            df=df.withColumnRenamed(df.schema.names[o],i.replace(" ", "_").replace("(","").replace(")","").replace("]","").replace("[",""))
            #df=df.withColumnRenamed(df.schema.names[o],(re.sub(r"[^a-zA-Z0-9_]","",i.replace(" ", "_").replace("(","").replace(")","").replace("]","").replace("[",""))).strip("_"))
            o+=1

       
        
        df=df.na.drop("all")
        if df.count()==0 :
            return "No Data in file"

        df = df.with_column(''file_name'',lit(file_name))


        def generunid():
            new_time = datetime.now(pytz.utc) + timedelta(hours=8)
            return new_time.strftime(''%Y%m%d%H%M%S'')

        def generatecrtd_dttm():
            new_time = datetime.now(pytz.utc) + timedelta(hours=8)
            return new_time.strftime(''%Y-%m-%d %H:%M:%S'')
            
            

        df = df.with_column(''hashkey'',md5(concat(when(is_null(col(''status'')),lit('''')).otherwise(col(''status'')),\\
                                            when(is_null(col(''pop_list'')),lit('''')).otherwise(col(''pop_list'')),\\
                                          when(is_null(col(''popdb_id'')),lit('''')).otherwise(col(''popdb_id'')),\\
                                          when(is_null(col(''pop_code'')),lit('''')).otherwise(col(''pop_code'')),\\
                                         when(is_null(col(''pop_name'')),lit('''')).otherwise(col(''pop_name'')),\\
                                         when(is_null(col(''date'')),lit('''')).otherwise(col(''date'')))))

        
        df = df.with_column(''run_id'',lit(generunid()))

        df = df.with_column(''crtd_dttm'',lit(generatecrtd_dttm()))
        

        

        
        snowdf= df.select(
                    "status",
                    "pop_list",
                    "popdb_id",
                    "pop_code",
                    "pop_name",
                    "date",
                    "file_name",
                    "run_id",
                    "crtd_dttm",
                    "hashkey"
            
                    )
        file_name=file_name.split(".")[0]+"_"+datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")

        snowdf.write.mode("append").saveAsTable(stage_name.split(".")[0] + "." + target_table)
        
        # #move to success
        snowdf.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)
        
        
        
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
CREATE OR REPLACE PROCEDURE POP6_HK_MASTER_PRODUCT_LISTS_PRODUCTS_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them.

from snowflake.snowpark.functions import col, lit, date_format, current_timestamp, to_date, year, month, concat, format_number, regexp_replace,to_timestamp,when,trim,upper,is_null,md5
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DecimalType
import snowflake.snowpark
import pandas as pd
from datetime import datetime,timedelta
import snowflake.snowpark as snowpark
import pytz
import pandas as pd

def main(session:snowpark.Session,Param):
    # Param=["20240617_product_lists_products.csv","JPNSDL_RAW.DEV_LOAD_STAGE_ADLS","dev/pop6/master/listdata/product_lists_products/","sdl_pop6_jp_product_lists_products"]
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        
        df_schema = StructType([
                StructField("Product_List", StringType(), True),
                StructField("Product_List_Code", StringType(), True),
                StructField("ProductDB_ID", StringType(), True),
                StructField("SKU", StringType(), True),
                StructField("MSL_Ranking", StringType(), True),
                StructField("Date", StringType(), True),
                
                
            ])
        
        df = session.read.option("INFER_SCHEMA", True)\\
            .option("field_delimiter", ",")\\
            .option("field_optionally_enclosed_by", "\\"") \\
            .option("trim_space", True) \\
            .with_metadata("METADATA$FILE_ROW_NUMBER") \\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        df_columns=df.first()[1:]
        
        df = df.filter(col("METADATA$FILE_ROW_NUMBER") > lit(1))
        
        
        df = df.drop("METADATA$FILE_ROW_NUMBER")
        #return df.schema.names
        o=0

        for i in df_columns:
            

            df=df.withColumnRenamed(df.schema.names[o],i.replace(" ", "_").replace("(","").replace(")","").replace("]","").replace("[",""))
            #df=df.withColumnRenamed(df.schema.names[o],(re.sub(r"[^a-zA-Z0-9_]","",i.replace(" ", "_").replace("(","").replace(")","").replace("]","").replace("[",""))).strip("_"))
            o+=1

       
        
        df=df.na.drop("all")
        if df.count()==0 :
            return "No Data in file"

        df = df.with_column(''file_name'',lit(file_name))


        def generunid():
            new_time = datetime.now(pytz.utc) + timedelta(hours=8)
            return new_time.strftime(''%Y%m%d%H%M%S'')

        def generatecrtd_dttm():
            new_time = datetime.now(pytz.utc) + timedelta(hours=8)
            return new_time.strftime(''%Y-%m-%d %H:%M:%S'')
            
            

        df = df.with_column(''hashkey'',md5(concat(when(is_null(col(''Product_List'')),lit('''')).otherwise(col(''Product_List'')),\\
                                            when(is_null(col(''Product_List_Code'')),lit('''')).otherwise(col(''Product_List_Code'')),\\
                                          when(is_null(col(''ProductDB_ID'')),lit('''')).otherwise(col(''ProductDB_ID'')),\\
                                          when(is_null(col(''SKU'')),lit('''')).otherwise(col(''SKU'')),\\
                                         when(is_null(col(''MSL_Ranking'')),lit('''')).otherwise(col(''MSL_Ranking'')),\\
                                            when(is_null(col(''Date'')),lit('''')).otherwise(col(''Date'')))))

        
        df = df.with_column(''run_id'',lit(generunid()))

        df = df.with_column(''crtd_dttm'',lit(generatecrtd_dttm()))
        

        

        
        snowdf= df.select(
                    "product_list",
                    "productdb_id",
                    "sku",
                    "msl_ranking",
                    "date",
                    "file_name",
                    "run_id",
                    "crtd_dttm",
                    "hashkey"
                    
            
                    )
        file_name=file_name.split(".")[0]+"_"+datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")

        snowdf.write.mode("append").saveAsTable(stage_name.split(".")[0] + "." + target_table)
        
        # #move to success
        snowdf.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)
        
        
        
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
CREATE OR REPLACE PROCEDURE POP6_HK_MASTER_PRODUCT_LISTS_ALLOCATION_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them.

from snowflake.snowpark.functions import col, lit, date_format, current_timestamp, to_date, year, month, concat, format_number, regexp_replace,to_timestamp,when,trim,upper,is_null,md5
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DecimalType
import snowflake.snowpark
import pandas as pd
from datetime import datetime,timedelta
import snowflake.snowpark as snowpark
import pytz
import pandas as pd

def main(session:snowpark.Session,Param):
    # Param=["20240617_product_lists_allocation.csv","JPNSDL_RAW.DEV_LOAD_STAGE_ADLS","dev/pop6/master/listdata/product_lists_allocation","sdl_pop6_jp_product_lists_allocation"]
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        
        df_schema = StructType([
                StructField("Product_Group_Status", StringType(), True),
                StructField("Product_Group", StringType(), True),
                StructField("Product_List_Status", StringType(), True),
                StructField("Product_List", StringType(), True),
                StructField("Product_List_Code", StringType(), True),
                StructField("POP_Attribute_ID", StringType(), True),
                StructField("POP_Attribute", StringType(), True),
                StructField("POP_Attribute_Value_ID", StringType(), True),
                StructField("POP_Attribute_Value", StringType(), True),
                StructField("Date", StringType(), True),
                
                
                
            ])
        
        df = session.read.option("INFER_SCHEMA", True)\\
            .option("field_delimiter", ",")\\
            .option("field_optionally_enclosed_by", "\\"") \\
            .option("trim_space", True) \\
            .with_metadata("METADATA$FILE_ROW_NUMBER") \\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        df_columns=df.first()[1:]
        
        df = df.filter(col("METADATA$FILE_ROW_NUMBER") > lit(1))
        
        
        df = df.drop("METADATA$FILE_ROW_NUMBER")
        #return df.schema.names
        o=0

        for i in df_columns:
            

            df=df.withColumnRenamed(df.schema.names[o],i.replace(" ", "_").replace("(","").replace(")","").replace("]","").replace("[",""))
            #df=df.withColumnRenamed(df.schema.names[o],(re.sub(r"[^a-zA-Z0-9_]","",i.replace(" ", "_").replace("(","").replace(")","").replace("]","").replace("[",""))).strip("_"))
            o+=1

       
        
        df=df.na.drop("all")
        if df.count()==0 :
            return "No Data in file"

        df = df.with_column(''file_name'',lit(file_name))


        def generunid():
            new_time = datetime.now(pytz.utc) + timedelta(hours=8)
            return new_time.strftime(''%Y%m%d%H%M%S'')

        def generatecrtd_dttm():
            new_time = datetime.now(pytz.utc) + timedelta(hours=8)
            return new_time.strftime(''%Y-%m-%d %H:%M:%S'')
            
            

        df = df.with_column(''hashkey'',md5(concat(when(is_null(col(''Product_Group_Status'')),lit('''')).otherwise(col(''Product_Group_Status'')),\\
                                            when(is_null(col(''Product_Group'')),lit('''')).otherwise(col(''Product_Group'')),\\
                                          when(is_null(col(''Product_List_Status'')),lit('''')).otherwise(col(''Product_List_Status'')),\\
                                          when(is_null(col(''Product_List'')),lit('''')).otherwise(col(''Product_List'')),\\
                                         when(is_null(col(''POP_Attribute_ID'')),lit('''')).otherwise(col(''POP_Attribute_ID'')),\\
                                         when(is_null(col(''POP_Attribute'')),lit('''')).otherwise(col(''POP_Attribute'')),\\
                                           when(is_null(col(''POP_Attribute_Value_ID'')),lit('''')).otherwise(col(''POP_Attribute_Value_ID'')),\\
                                          when(is_null(col(''POP_Attribute_Value'')),lit('''')).otherwise(col(''POP_Attribute_Value'')),\\
                                         when(is_null(col(''date'')),lit('''')).otherwise(col(''date'')))))

        
        df = df.with_column(''run_id'',lit(generunid()))

        df = df.with_column(''crtd_dttm'',lit(generatecrtd_dttm()))
        

        

        
        snowdf= df.select(
                    "Product_Group_Status",
                    "Product_Group",
                    "Product_List_Status",
                    "Product_List",
                    "POP_Attribute_ID",
                    "POP_Attribute",
                    "POP_Attribute_Value_ID",
                    "POP_Attribute_Value",
                    "date",
                    "file_name",
                    "run_id",
                    "crtd_dttm",
                    "hashkey"
            
                    )
        file_name=file_name.split(".")[0]+"_"+datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")

        snowdf.write.mode("append").saveAsTable(stage_name.split(".")[0] + "." + target_table)
        
        # #move to success
        snowdf.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)
        
        
        
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
CREATE OR REPLACE PROCEDURE POP6_KR_MASTER_POPS_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them

import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import lit, md5, concat, when, is_null, col
from snowflake.snowpark.types import  StringType, StructType, StructField
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz
from datetime import datetime, timedelta                


def main(session: snowpark.Session, Param): 
       
    try :
            
        # Param=[''20240617_pops.csv'',''NTASDL_RAW.DEV_LOAD_STAGE_ADLS_KOR'',''dev/pop6/masterdata/'',''sdl_pop6_kr_pops''] 

        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        # Define the schema for the DataFrame



        df_schema=StructType([
                        StructField("Status", StringType()),
                        StructField("POPDB_ID", StringType()),
                        StructField("POP_Code", StringType()),
                        StructField("POP_Name", StringType()),
                        StructField("Address", StringType()),
                        StructField("Longitude", StringType()),
                        StructField("Latitude", StringType()),
                        StructField("Business_Units_ID", StringType()),
                        StructField("Business_Unit_Name", StringType()),
                        StructField("Country", StringType()),
                        StructField("Channel", StringType()),
                        StructField("Retail Environment (PS)", StringType()),
                        StructField("Sales Group Name", StringType()),
                        StructField("Customer", StringType()),    
                        StructField("Store Type", StringType()),
                        StructField("Sales Group Code", StringType()),
                        StructField("Customer Grade", StringType()),
                        StructField("External Store Code", StringType()),
                        StructField("Territory/Region", StringType()),
                        StructField("Comments", StringType())
                        ])
                
        dataframe = session.read.option("INFER_SCHEMA", True)\\
            .option("field_delimiter", ",")\\
            .option("field_optionally_enclosed_by", "\\"") \\
            .option("trim_space", True) \\
            .with_metadata("METADATA$FILE_ROW_NUMBER") \\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        df_columns=dataframe.first()[1:]
        
        dataframe = dataframe.filter(col("METADATA$FILE_ROW_NUMBER") > lit(1))
        
        
        dataframe = dataframe.drop("METADATA$FILE_ROW_NUMBER")
        #return df.schema.names
        o=0

        for i in df_columns:
            

            dataframe=dataframe.withColumnRenamed(dataframe.schema.names[o],i.replace(" ", "_").replace("(","").replace(")","").replace("]","").replace("[",""))
            #df=df.withColumnRenamed(df.schema.names[o],(re.sub(r"[^a-zA-Z0-9_]","",i.replace(" ", "_").replace("(","").replace(")","").replace("]","").replace("[",""))).strip("_"))
            o+=1    
        
        dataframe=dataframe.na.drop("all")
        if dataframe.count()==0:
            return "No Data in file"    
        
        
        dataframe = dataframe.with_column("FILE_NAME",lit(file_name))
        dataframe = dataframe.with_column("HASHKEY",md5(concat(when(is_null(col("Status")),lit("")).otherwise(col("Status")),
                                            when(is_null(col("POPDB_ID")),lit("")).otherwise(col("POPDB_ID")),
                                            when(is_null(col("POP_Code")),lit("")).otherwise(col("POP_Code")),
                                            when(is_null(col("POP_Name")),lit("")).otherwise(col("POP_Name")),
                                            when(is_null(col("Address")),lit("")).otherwise(col("Address")),
                                            when(is_null(col("Longitude")),lit("")).otherwise(col("Longitude")),
                                            when(is_null(col("Latitude")),lit("")).otherwise(col("Latitude")),
                                            when(is_null(col("Country")),lit("")).otherwise(col("Country")),
                                            when(is_null(col("Channel")),lit("")).otherwise(col("Channel")),
                                            when(is_null(col("Retail_Environment_PS")),lit("")).otherwise(col("Retail_Environment_PS")),
                                            when(is_null(col("Customer")),lit("")).otherwise(col("Customer")),
                                            when(is_null(col("Sales_Group_Code")),lit("")).otherwise(col("Sales_Group_Code")),
                                            when(is_null(col("Sales_Group_Name")),lit("")).otherwise(col("Sales_Group_Name")),
                                            when(is_null(col("Customer_Grade")),lit("")).otherwise(col("Customer_Grade")),
                                            when(is_null(col("External_Store_Code")),lit("")).otherwise(col("External_Store_Code")),
                                            when(is_null(col("Business_Units_ID")),lit("")).otherwise(col("Business_Units_ID")),
                                                when(is_null(col("Business_Unit_Name")),lit("")).otherwise(col("Business_Unit_Name")),
                                            when(is_null(col("Territory/Region")),lit("")).otherwise(col("Territory/Region")))))
        dataframe = dataframe.with_column("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        dataframe = dataframe.with_column("CRTD_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))

               
        snowdf= dataframe.select(
                    "Status",
                    "POPDB_ID",
                    "POP_Code",
                    "POP_Name",
                    "Address",
                    "Longitude",
                    "Latitude",
                    "Country",
                    "Channel",
                    "Retail_Environment_PS",
                    "Customer",
                    "Sales_Group_Code",
                    "Sales_Group_Name",
                    "Customer_Grade",
                    "External_Store_Code",
                    "FILE_NAME",
                    "RUN_ID",
                    "CRTD_DTTM",
                    "HASHKEY",
                    "Business_Units_ID",
                    "Business_Unit_Name",
                    "Territory/Region"
                    )
        
        # Load Data to the target table
        snowdf.write.mode("append").saveAsTable(target_table)
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        #move to success
        file_name=file_name.split(".")[0]+"_"+datetime.now().strftime("%Y%m%d%H%M%S")
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
CREATE OR REPLACE PROCEDURE POP6_KR_MASTER_USERS_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them

import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import lit, md5, concat, when, is_null, col
from snowflake.snowpark.types import  StringType, StructType, StructField
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz
from datetime import datetime, timedelta                


def main(session: snowpark.Session, Param): 
       
    try :
            
        # Param=[''20240616_users.csv'',''NTASDL_RAW.DEV_LOAD_STAGE_ADLS_KOR'',''dev/pop6/masterdata/'',''sdl_pop6_kr_users''] 

        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        # Define the schema for the DataFrame


        df_schema=StructType([
                        StructField("Status", StringType()),
                        StructField("UserDB_ID", StringType()),
                        StructField("Username", StringType()),
                        StructField("First_Name", StringType()),
                        StructField("Last_Name", StringType()),
                        StructField("Team", StringType()),
                        StructField("Superior_Name", StringType()),
                        StructField("Authorisation_Group", StringType()),
                        StructField("Email_Address", StringType()),
                        StructField("Longitude", StringType()),
                        StructField("Latitude", StringType()),
                        StructField("Business_Units_ID", StringType()),
                        StructField("Business_Unit_Name", StringType())
                        ])
                
        dataframe = session.read.option("INFER_SCHEMA", True)\\
            .option("field_delimiter", ",")\\
            .option("field_optionally_enclosed_by", "\\"") \\
            .option("trim_space", True) \\
            .with_metadata("METADATA$FILE_ROW_NUMBER") \\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        df_columns=dataframe.first()[1:]
        
        dataframe = dataframe.filter(col("METADATA$FILE_ROW_NUMBER") > lit(1))
        
        
        dataframe = dataframe.drop("METADATA$FILE_ROW_NUMBER")
        #return df.schema.names
        o=0

        for i in df_columns:
            

            dataframe=dataframe.withColumnRenamed(dataframe.schema.names[o],i.replace(" ", "_").replace("(","").replace(")","").replace("]","").replace("[",""))
            #df=df.withColumnRenamed(df.schema.names[o],(re.sub(r"[^a-zA-Z0-9_]","",i.replace(" ", "_").replace("(","").replace(")","").replace("]","").replace("[",""))).strip("_"))
            o+=1
        
       
            
        
        dataframe=dataframe.na.drop("all")
        if dataframe.count()==0:
            return "No Data in file"    

       
        
        dataframe = dataframe.with_column("FILE_NAME",lit(file_name))
        dataframe = dataframe.with_column("HASHKEY",md5(concat(when(is_null(col("Status")),lit("")).otherwise(col("Status")),
                                            when(is_null(col("UserDB_ID")),lit("")).otherwise(col("UserDB_ID")),
                                            when(is_null(col("Username")),lit("")).otherwise(col("Username")),
                                            when(is_null(col("First_Name")),lit("")).otherwise(col("First_Name")),
                                            when(is_null(col("Last_Name")),lit("")).otherwise(col("Last_Name")),
                                            when(is_null(col("Team")),lit("")).otherwise(col("Team")),
                                            when(is_null(col("Superior_Name")),lit("")).otherwise(col("Superior_Name")),
                                            when(is_null(col("Authorisation_Group")),lit("")).otherwise(col("Authorisation_Group")),
                                            when(is_null(col("Email_Address")),lit("")).otherwise(col("Email_Address")),
                                            when(is_null(col("Longitude")),lit("")).otherwise(col("Longitude")),
                                            when(is_null(col("Latitude")),lit("")).otherwise(col("Latitude")),
                                            when(is_null(col("Business_Units_ID")),lit("")).otherwise(col("Business_Units_ID")),
                                            when(is_null(col("Business_Unit_Name")),lit("")).otherwise(col("Business_Unit_Name")))))
        dataframe = dataframe.with_column("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        dataframe = dataframe.with_column("CRTD_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
                
        snowdf= dataframe.select(
                    "Status",
                    "UserDB_ID",
                    "Username",
                    "First_Name",
                    "Last_Name",
                    "Team",
                    "Superior_Name",
                    "Authorisation_Group",
                    "Email_Address",
                    "Longitude",
                    "Latitude",
                    "FILE_NAME",
                    "RUN_ID",
                    "CRTD_DTTM",
                    "HASHKEY",
                    "Business_Units_Id",
                    "Business_Unit_Name"
                    )
        
        # Load Data to the target table
        snowdf.write.mode("append").saveAsTable(target_table)
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        #move to success
        file_name=file_name.split(".")[0]+"_"+datetime.now().strftime("%Y%m%d%H%M%S")
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
CREATE OR REPLACE PROCEDURE POP6_KR_MASTER_PRODUCTS_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them

import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import lit, md5, concat, when, is_null, col
from snowflake.snowpark.types import  StringType, StructType, StructField
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz
from datetime import datetime, timedelta                


def main(session: snowpark.Session, Param): 
       
    try :
            
        # Param=[''20240617_products.csv'',''NTASDL_RAW.DEV_LOAD_STAGE_ADLS_KOR'',''dev/pop6/masterdata/'',''sdl_pop6_kr_products''] 

        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        # Define the schema for the DataFrame



        df_schema=StructType([
                        StructField("Status", StringType()),
                        StructField("ProductDB_ID", StringType()),
                        StructField("Barcode", StringType()),
                        StructField("SKU", StringType()),
                        StructField("Unit_Price", StringType()),
                        StructField("Display_order", StringType()),
                        StructField("Launch_Date", StringType()),
                        StructField("Largest_UOM_Quantity", StringType()),
                        StructField("Middle_UOM_Quantity", StringType()),
                        StructField("Smallest_UOM_Quantity", StringType()),
                        StructField("SKU (English)", StringType()),
                        StructField("Company", StringType()),
                        StructField("SKU_Code", StringType()),
                        StructField("PS Category", StringType()),
                        StructField("PS Segment", StringType()),
                        StructField("PS Category Segment", StringType()),
                        StructField("Country - L1", StringType()),
                        StructField("Regional Franchise-L2", StringType()),
                        StructField("Franchise - L3", StringType()),
                        StructField("Brand - L4", StringType()),
                        StructField("Sub Category - L5", StringType()),
                        StructField("Platform - L6", StringType()),
                        StructField("Variance - L7", StringType()),
                        StructField("Pack Size - L8", StringType()),
                        StructField("L9-Promo SKU", StringType())
                        ])
                
        dataframe = session.read.option("INFER_SCHEMA", True)\\
            .option("field_delimiter", ",")\\
            .option("field_optionally_enclosed_by", "\\"") \\
            .option("trim_space", True) \\
            .with_metadata("METADATA$FILE_ROW_NUMBER") \\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        df_columns=dataframe.first()[1:]
        
        dataframe = dataframe.filter(col("METADATA$FILE_ROW_NUMBER") > lit(1))
        
        
        dataframe = dataframe.drop("METADATA$FILE_ROW_NUMBER")
        #return df.schema.names
        o=0

        for i in df_columns:
            

            dataframe=dataframe.withColumnRenamed(dataframe.schema.names[o],i.replace(" ", "_").replace("(","").replace(")","").replace("]","").replace("[",""))
            #df=df.withColumnRenamed(df.schema.names[o],(re.sub(r"[^a-zA-Z0-9_]","",i.replace(" ", "_").replace("(","").replace(")","").replace("]","").replace("[",""))).strip("_"))
            o+=1     
        
        dataframe=dataframe.na.drop("all")
        if dataframe.count()==0:
            return "No Data in file"

            
        
        dataframe = dataframe.with_column("FILE_NAME",lit(file_name))
        dataframe = dataframe.with_column("HASHKEY",md5(concat(when(is_null(col("Status")),lit("")).otherwise(col("Status")),
                                            when(is_null(col("ProductDB_ID")),lit("")).otherwise(col("ProductDB_ID")),
                                            when(is_null(col("Barcode")),lit("")).otherwise(col("Barcode")),
                                            when(is_null(col("SKU")),lit("")).otherwise(col("SKU")),
                                            when(is_null(col("Unit_Price")),lit("")).otherwise(col("Unit_Price")),
                                            when(is_null(col("Display_order")),lit("")).otherwise(col("Display_order")),
                                            when(is_null(col("Launch_Date")),lit("")).otherwise(col("Launch_Date")),
                                            when(is_null(col("Largest_UOM_Quantity")),lit("")).otherwise(col("Largest_UOM_Quantity")),
                                            when(is_null(col("Middle_UOM_Quantity")),lit("")).otherwise(col("Middle_UOM_Quantity")),
                                            when(is_null(col("Smallest_UOM_Quantity")),lit("")).otherwise(col("Smallest_UOM_Quantity")),
                                            when(is_null(col("Company")),lit("")).otherwise(col("Company")),
                                            when(is_null(col("SKU_English")),lit("")).otherwise(col("SKU_English")),
                                            when(is_null(col("SKU_Code")),lit("")).otherwise(col("SKU_Code")),
                                            when(is_null(col("PS_Category")),lit("")).otherwise(col("PS_Category")),
                                            when(is_null(col("PS_Segment")),lit("")).otherwise(col("PS_Segment")),
                                            when(is_null(col("PS_Category_Segment")),lit("")).otherwise(col("PS_Category_Segment")),
                                            when(is_null(col("Country_-_L1")),lit("")).otherwise(col("Country_-_L1")),
                                            when(is_null(col("Regional_Franchise-L2")),lit("")).otherwise(col("Regional_Franchise-L2")),
                                            when(is_null(col("Franchise_-_L3")),lit("")).otherwise(col("Franchise_-_L3")),
                                            when(is_null(col("Brand_-_L4")),lit("")).otherwise(col("Brand_-_L4")),
                                            when(is_null(col("Sub_Category_-_L5")),lit("")).otherwise(col("Sub_Category_-_L5")),
                                            when(is_null(col("Platform_-_L6")),lit("")).otherwise(col("Platform_-_L6")),
                                            when(is_null(col("Variance_-_L7")),lit("")).otherwise(col("Variance_-_L7")),
                                            when(is_null(col("Pack_Size_-_L8")),lit("")).otherwise(col("Pack_Size_-_L8")),
                                            when(is_null(col("L9-Promo_SKU")),lit("")).otherwise(col("L9-Promo_SKU")))))
                                                              
        dataframe = dataframe.with_column("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        dataframe = dataframe.with_column("CRTD_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))

        
        
        snowdf= dataframe.select(
        	"Status",
        	"ProductDB_ID",
        	"Barcode",
        	"SKU",
        	"Unit_Price",
        	"Display_order",
        	"Launch_Date",
        	"Largest_UOM_Quantity",
        	"Middle_UOM_Quantity",
        	"Smallest_UOM_Quantity",
        	"Company",
        	"SKU_English",
        	"SKU_Code",
        	"PS_Category",
        	"PS_Segment",
        	"PS_Category_Segment",
        	"Country_-_L1",
        	"Regional_Franchise-L2",
        	"Franchise_-_L3",
        	"Brand_-_L4",
        	"Sub_Category_-_L5",
        	"Platform_-_L6",
        	"Variance_-_L7",
        	"Pack_Size_-_L8",
        	"L9-Promo_SKU",
        	"FILE_NAME",
        	"RUN_ID",
        	"CRTD_DTTM",
        	"HASHKEY"
        )
        
        # Load Data to the target table
        snowdf.write.mode("append").saveAsTable(target_table)
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        #move to success
        file_name=file_name.split(".")[0]+"_"+datetime.now().strftime("%Y%m%d%H%M%S")
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
CREATE OR REPLACE PROCEDURE POP6_KR_MASTER_POP_LISTS_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them

import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import lit, md5, concat, when, is_null, col
from snowflake.snowpark.types import  StringType, StructType, StructField
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz
from datetime import datetime, timedelta                


def main(session: snowpark.Session, Param): 
       
    try :
            
        # Param=[''20240620_pop_lists.csv'',''NTASDL_RAW.DEV_LOAD_STAGE_ADLS_KOR'',''dev/pop6/master/listdata'',''sdl_pop6_kr_pop_lists''] 

        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        # Define the schema for the DataFrame

        df_schema=StructType([
                        StructField("Status", StringType()),
                        StructField("POP_List", StringType()),
                        StructField("POPDB_ID", StringType()),
                        StructField("POP_Code", StringType()),
                        StructField("POP_Name", StringType()),
                        StructField("Date", StringType())
                        ])
                
        dataframe = session.read.option("INFER_SCHEMA", True)\\
            .option("field_delimiter", ",")\\
            .option("field_optionally_enclosed_by", "\\"") \\
            .option("trim_space", True) \\
            .with_metadata("METADATA$FILE_ROW_NUMBER") \\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        df_columns=dataframe.first()[1:]
        
        dataframe = dataframe.filter(col("METADATA$FILE_ROW_NUMBER") > lit(1))
        
        
        dataframe = dataframe.drop("METADATA$FILE_ROW_NUMBER")
        #return df.schema.names
        o=0

        for i in df_columns:
            

            dataframe=dataframe.withColumnRenamed(dataframe.schema.names[o],i.replace(" ", "_").replace("(","").replace(")","").replace("]","").replace("[",""))
            #df=df.withColumnRenamed(df.schema.names[o],(re.sub(r"[^a-zA-Z0-9_]","",i.replace(" ", "_").replace("(","").replace(")","").replace("]","").replace("[",""))).strip("_"))
            o+=1    
        
        dataframe=dataframe.na.drop("all")
        if dataframe.count()==0:
            return "No Data in file"
        
        dataframe = dataframe.with_column("FILE_NAME",lit(file_name))
        dataframe = dataframe.with_column("HASHKEY",md5(concat(when(is_null(col("Status")),lit("")).otherwise(col("Status")),
                                            when(is_null(col("POP_List")),lit("")).otherwise(col("POP_List")),
                                            when(is_null(col("POPDB_ID")),lit("")).otherwise(col("POPDB_ID")),
                                            when(is_null(col("POP_Code")),lit("")).otherwise(col("POP_Code")),
                                            when(is_null(col("POP_Name")),lit("")).otherwise(col("POP_Name")),
                                            when(is_null(col("Date")),lit("")).otherwise(col("Date")))))
                                                              
        dataframe = dataframe.with_column("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        dataframe = dataframe.with_column("CRTD_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        
        snowdf= dataframe.select(
        	"Status",
        	"POP_List",
        	"POPDB_ID",
        	"POP_Code",
        	"POP_Name",
        	"Date",
            "FILE_NAME",
        	"RUN_ID",
        	"CRTD_DTTM",
        	"HASHKEY"
        )
        
        # Load Data to the target table
        snowdf.write.mode("append").saveAsTable(target_table)
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        #move to success
        file_name=file_name.split(".")[0]+"_"+datetime.now().strftime("%Y%m%d%H%M%S")
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
CREATE OR REPLACE PROCEDURE POP6_KR_MASTER_PRODUCT_LISTS_ALLOCATION_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them

import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import lit, md5, concat, when, is_null, col
from snowflake.snowpark.types import  StringType, StructType, StructField
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz
from datetime import datetime, timedelta                


def main(session: snowpark.Session, Param): 
       
    try :
            
        # Param=[''20240623_product_lists_allocation.csv'',''NTASDL_RAW.DEV_LOAD_STAGE_ADLS_KOR'',''dev/pop6/master/listdata'',''sdl_pop6_kr_product_lists_allocation''] 

        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        # Define the schema for the DataFrame

        df_schema=StructType([
                        StructField("Product_Group_Status", StringType()),
                        StructField("Product_Group", StringType()),
                        StructField("Product_List_Status", StringType()),
                        StructField("Product_List", StringType()),
                        StructField("Product_List_Code", StringType()),
                        StructField("POP_Attribute_ID", StringType()),
                        StructField("POP_Attribute", StringType()),
                        StructField("POP_Attribute_Value_ID", StringType()),
                        StructField("POP_Attribute_Value", StringType()),
                        StructField("Date", StringType())
                        ])
                
        dataframe = session.read.option("INFER_SCHEMA", True)\\
            .option("field_delimiter", ",")\\
            .option("field_optionally_enclosed_by", "\\"") \\
            .option("trim_space", True) \\
            .with_metadata("METADATA$FILE_ROW_NUMBER") \\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        df_columns=dataframe.first()[1:]
        
        dataframe = dataframe.filter(col("METADATA$FILE_ROW_NUMBER") > lit(1))
        
        
        dataframe = dataframe.drop("METADATA$FILE_ROW_NUMBER")
        #return df.schema.names
        o=0

        for i in df_columns:
            

            dataframe=dataframe.withColumnRenamed(dataframe.schema.names[o],i.replace(" ", "_").replace("(","").replace(")","").replace("]","").replace("[",""))
            #df=df.withColumnRenamed(df.schema.names[o],(re.sub(r"[^a-zA-Z0-9_]","",i.replace(" ", "_").replace("(","").replace(")","").replace("]","").replace("[",""))).strip("_"))
            o+=1     
        
        dataframe=dataframe.na.drop("all")
        if dataframe.count()==0:
            return "No Data in file"

        dataframe = dataframe.with_column("FILE_NAME",lit(file_name))
        dataframe = dataframe.with_column("HASHKEY",md5(concat(when(is_null(col("Product_Group_Status")),lit("")).otherwise(col("Product_Group_Status")),
                                            when(is_null(col("Product_Group")),lit("")).otherwise(col("Product_Group")),
                                            when(is_null(col("Product_List_Status")),lit("")).otherwise(col("Product_List_Status")),
                                            when(is_null(col("Product_List")),lit("")).otherwise(col("Product_List")),
                                            when(is_null(col("Product_List_Code")),lit("")).otherwise(col("Product_List_Code")),
                                            when(is_null(col("POP_Attribute_ID")),lit("")).otherwise(col("POP_Attribute_ID")),
                                            when(is_null(col("POP_Attribute")),lit("")).otherwise(col("POP_Attribute")),
                                            when(is_null(col("POP_Attribute_Value_ID")),lit("")).otherwise(col("POP_Attribute_Value_ID")),
                                            when(is_null(col("POP_Attribute_Value")),lit("")).otherwise(col("POP_Attribute_Value")),
                                            when(is_null(col("Date")),lit("")).otherwise(col("Date")))))
                                                              
        dataframe = dataframe.with_column("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        dataframe = dataframe.with_column("CRTD_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        
        snowdf= dataframe.select(
        	"Product_Group_Status",
        	"Product_Group",
        	"Product_List_Status",
        	"Product_List",
        	"POP_Attribute_ID",
            "POP_Attribute",
            "POP_Attribute_Value_ID",
            "POP_Attribute_Value",
            "Date",
            "FILE_NAME",
        	"RUN_ID",
        	"CRTD_DTTM",
        	"HASHKEY"
        )
        
        # Load Data to the target table
        snowdf.write.mode("append").saveAsTable(target_table)
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        #move to success
        file_name=file_name.split(".")[0]+"_"+datetime.now().strftime("%Y%m%d%H%M%S")
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
CREATE OR REPLACE PROCEDURE POP6_KR_MASTER_PRODUCT_LISTS_POPS_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them

import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import lit, md5, concat, when, is_null, col
from snowflake.snowpark.types import  StringType, StructType, StructField
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz
from datetime import datetime, timedelta                


def main(session: snowpark.Session, Param): 
       
    try :
            
        # Param=[''20240624_product_lists_pops.csv'',''NTASDL_RAW.DEV_LOAD_STAGE_ADLS_KOR'',''dev/pop6/master/listdata'',''sdl_pop6_kr_product_lists_pops''] 

        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        # Define the schema for the DataFrame
        
        df_schema=StructType([
                        StructField("Product_List", StringType()),
                        StructField("Product_List_Code", StringType()),
                        StructField("POPDB_ID", StringType()),
                        StructField("POP_Code", StringType()),
                        StructField("POP_Name", StringType()),
                        StructField("Date", StringType())
                        ])
                
        dataframe = session.read.option("INFER_SCHEMA", True)\\
            .option("field_delimiter", ",")\\
            .option("field_optionally_enclosed_by", "\\"") \\
            .option("trim_space", True) \\
            .with_metadata("METADATA$FILE_ROW_NUMBER") \\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        df_columns=dataframe.first()[1:]
        
        dataframe = dataframe.filter(col("METADATA$FILE_ROW_NUMBER") > lit(1))
        
        
        dataframe = dataframe.drop("METADATA$FILE_ROW_NUMBER")
        #return df.schema.names
        o=0

        for i in df_columns:
            

            dataframe=dataframe.withColumnRenamed(dataframe.schema.names[o],i.replace(" ", "_").replace("(","").replace(")","").replace("]","").replace("[",""))
            #df=df.withColumnRenamed(df.schema.names[o],(re.sub(r"[^a-zA-Z0-9_]","",i.replace(" ", "_").replace("(","").replace(")","").replace("]","").replace("[",""))).strip("_"))
            o+=1    
        
        dataframe=dataframe.na.drop("all")
        if dataframe.count()==0:
            return "No Data in file"
        
        dataframe = dataframe.with_column("FILE_NAME",lit(file_name))
        
        dataframe = dataframe.with_column("HASHKEY",md5(concat(when(is_null(col("Product_List")),lit("")).otherwise(col("Product_List")),
                                            when(is_null(col("POPDB_ID")),lit("")).otherwise(col("POPDB_ID")),
                                            when(is_null(col("POP_Code")),lit("")).otherwise(col("POP_Code")),
                                            when(is_null(col("POP_Name")),lit("")).otherwise(col("POP_Name")),
                                            when(is_null(col("Date")),lit("")).otherwise(col("Date")))))
                                                              
        dataframe = dataframe.with_column("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        dataframe = dataframe.with_column("CRTD_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        
        snowdf= dataframe.select(
        	"Product_List",
        	"POPDB_ID",
        	"POP_Code",
        	"POP_Name",
        	"Date",
            "FILE_NAME",
        	"RUN_ID",
        	"CRTD_DTTM",
        	"HASHKEY"
        )
        
        # Load Data to the target table
        snowdf.write.mode("append").saveAsTable(target_table)
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        #move to success
        file_name=file_name.split(".")[0]+"_"+datetime.now().strftime("%Y%m%d%H%M%S")
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
CREATE OR REPLACE PROCEDURE POP6_KR_MASTER_PRODUCT_LISTS_PRODUCTS_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them

import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import lit, md5, concat, when, is_null, col
from snowflake.snowpark.types import  StringType, StructType, StructField
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz
from datetime import datetime, timedelta                


def main(session: snowpark.Session, Param): 
       
    try :
            
        # Param=[''20240624_product_lists_products.csv'',''NTASDL_RAW.DEV_LOAD_STAGE_ADLS_KOR'',''dev/pop6/master/listdata'',''sdl_pop6_kr_product_lists_products''] 

        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        # Define the schema for the DataFrame

        
        df_schema=StructType([
                        StructField("Product_List", StringType()),
                        StructField("Product_List_Code", StringType()),
                        StructField("ProductDB_ID", StringType()),
                        StructField("SKU", StringType()),
                        StructField("MSL_Ranking", StringType()),
                        StructField("Date", StringType())
                        ])
                
        dataframe = session.read.option("INFER_SCHEMA", True)\\
            .option("field_delimiter", ",")\\
            .option("field_optionally_enclosed_by", "\\"") \\
            .option("trim_space", True) \\
            .with_metadata("METADATA$FILE_ROW_NUMBER") \\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        df_columns=dataframe.first()[1:]
        
        dataframe = dataframe.filter(col("METADATA$FILE_ROW_NUMBER") > lit(1))
        
        
        dataframe = dataframe.drop("METADATA$FILE_ROW_NUMBER")
        #return df.schema.names
        o=0

        for i in df_columns:
            

            dataframe=dataframe.withColumnRenamed(dataframe.schema.names[o],i.replace(" ", "_").replace("(","").replace(")","").replace("]","").replace("[",""))
            #df=df.withColumnRenamed(df.schema.names[o],(re.sub(r"[^a-zA-Z0-9_]","",i.replace(" ", "_").replace("(","").replace(")","").replace("]","").replace("[",""))).strip("_"))
            o+=1    
        
        dataframe=dataframe.na.drop("all")
        if dataframe.count()==0:
            return "No Data in file"
        
        dataframe = dataframe.with_column("FILE_NAME",lit(file_name))

        
        dataframe = dataframe.with_column("HASHKEY",md5(concat(when(is_null(col("Product_List")),lit("")).otherwise(col("Product_List")),
                                            when(is_null(col("ProductDB_ID")),lit("")).otherwise(col("ProductDB_ID")),
                                            when(is_null(col("SKU")),lit("")).otherwise(col("SKU")),
                                            when(is_null(col("MSL_Ranking")),lit("")).otherwise(col("MSL_Ranking")),
                                            when(is_null(col("Date")),lit("")).otherwise(col("Date")))))
                                                              
        dataframe = dataframe.with_column("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        dataframe = dataframe.with_column("CRTD_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))

        snowdf= dataframe.select(
        	"Product_List",
        	"ProductDB_ID",
        	"SKU",
        	"MSL_Ranking",
        	"Date",
            "FILE_NAME",
        	"RUN_ID",
        	"CRTD_DTTM",
        	"HASHKEY"
        )
        
        # Load Data to the target table
        snowdf.write.mode("append").saveAsTable(target_table)
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        #move to success
        file_name=file_name.split(".")[0]+"_"+datetime.now().strftime("%Y%m%d%H%M%S")
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
		
use schema JPNSDL_RAW;
CREATE OR REPLACE PROCEDURE POP6_JP_MASTER_USERS_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them.

from snowflake.snowpark.functions import col, lit, date_format, current_timestamp, to_date, year, month, concat, format_number, regexp_replace,to_timestamp,when,trim,upper,is_null,md5
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DecimalType
import snowflake.snowpark
import pandas as pd
from datetime import datetime,timedelta
import snowflake.snowpark as snowpark
import pytz
import pandas as pd

def main(session:snowpark.Session,Param):
    # Param=["20240617_users.csv","JPNSDL_RAW.DEV_LOAD_STAGE_ADLS","dev/pop6/master/masterdata/users/","sdl_pop6_jp_users"]
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]

        
        df_schema = StructType([
                StructField("Status", StringType(), True),
                StructField("USERDB_ID", StringType(), True),
                StructField("Username", StringType(), True),
                StructField("FIRST_NAME", StringType(), True),
                StructField("Last_Name", StringType(), True),
                StructField("Team", StringType(), True),
                StructField("Superior_Name", StringType(), True),
                StructField("Authorisation_Group", StringType(), True),
                StructField("Email_Address", StringType(), True),
                StructField("Longitude", StringType(), True),
                StructField("Latitude", StringType(), True),
                StructField("Business_Units_ID", StringType(), True),
                StructField("Business_Unit_Name", StringType(), True)
                
            ])
        
        df = session.read.option("INFER_SCHEMA", True)\\
            .option("field_delimiter", ",")\\
            .option("field_optionally_enclosed_by", "\\"") \\
            .option("trim_space", True) \\
            .with_metadata("METADATA$FILE_ROW_NUMBER") \\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        df_columns=df.first()[1:]
        
        df = df.filter(col("METADATA$FILE_ROW_NUMBER") > lit(1))
        
        
        df = df.drop("METADATA$FILE_ROW_NUMBER")
        #return df.schema.names
        o=0

        for i in df_columns:
            

            df=df.withColumnRenamed(df.schema.names[o],i.replace(" ", "_").replace("(","").replace(")","").replace("]","").replace("[",""))
            #df=df.withColumnRenamed(df.schema.names[o],(re.sub(r"[^a-zA-Z0-9_]","",i.replace(" ", "_").replace("(","").replace(")","").replace("]","").replace("[",""))).strip("_"))
            o+=1

       
        
        df=df.na.drop("all")
        if df.count()==0 :
            return "No Data in file"

        df = df.with_column(''file_name'',lit(file_name))


        def generunid():
            new_time = datetime.now(pytz.utc) + timedelta(hours=8)
            return new_time.strftime(''%Y%m%d%H%M%S'')

        def generatecrtd_dttm():
            new_time = datetime.now(pytz.utc) + timedelta(hours=8)
            return new_time.strftime(''%Y-%m-%d %H:%M:%S'')
            
            

        df = df.with_column(''hashkey'',md5(concat(when(is_null(col(''status'')),lit('''')).otherwise(col(''status'')),\\
                                            when(is_null(col(''USERDB_ID'')),lit('''')).otherwise(col(''USERDB_ID'')),\\
                                          when(is_null(col(''Username'')),lit('''')).otherwise(col(''Username'')),\\
                                          when(is_null(col(''FIRST_NAME'')),lit('''')).otherwise(col(''FIRST_NAME'')),\\
                                         when(is_null(col(''Last_Name'')),lit('''')).otherwise(col(''Last_Name'')),\\
                                         when(is_null(col(''Team'')),lit('''')).otherwise(col(''Team'')),\\
                                         when(is_null(col(''Superior_Name'')),lit('''')).otherwise(col(''Superior_Name'')),\\
                                          when(is_null(col(''Authorisation_Group'')),lit('''')).otherwise(col(''Authorisation_Group'')),\\
                                         when(is_null(col(''Email_Address'')),lit('''')).otherwise(col(''Email_Address'')),\\
                                         when(is_null(col(''Longitude'')),lit('''')).otherwise(col(''Longitude'')),\\
                                         when(is_null(col(''Latitude'')),lit('''')).otherwise(col(''Latitude'')),\\
                                         when(is_null(col(''Business_Units_ID'')),lit('''')).otherwise(col(''Business_Units_ID'')),
                                         when(is_null(col(''Business_Unit_Name'')),lit('''')).otherwise(col(''Business_Unit_Name'')))))

        
        df = df.with_column(''run_id'',lit(generunid()))

        df = df.with_column(''crtd_dttm'',lit(generatecrtd_dttm()))
        

        

        
        snowdf= df.select(
                    "status",
                    "userdb_id",
                    "username",
                    "first_name",
                    "last_name",
                    "team",
                    "superior_name",
                    "authorisation_group",
                    "email_address",
                    "longitude",
                    "latitude",
                     "business_units_id",
                    "business_unit_name",
                    "file_name",
                    "run_id",
                    "crtd_dttm",
                    "hashkey"
                   
            
                    )
        file_name=file_name.split(".")[0]+"_"+datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")

        snowdf.write.mode("append").saveAsTable(stage_name.split(".")[0] + "." + target_table)
        
        # #move to success
        snowdf.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)
        
        
        
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
CREATE OR REPLACE PROCEDURE POP6_JP_MASTER_POPS_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them.

from snowflake.snowpark.functions import col, lit, date_format, current_timestamp, to_date, year, month, concat, format_number, regexp_replace,to_timestamp,when,trim,upper,is_null,md5
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DecimalType
import snowflake.snowpark
import pandas as pd
from datetime import datetime,timedelta
import snowflake.snowpark as snowpark
import pytz
import pandas as pd

def main(session:snowpark.Session,Param):
    # Param=["20240617_pops.csv","JPNSDL_RAW.DEV_LOAD_STAGE_ADLS","dev/pop6/master/masterdata/pops/","sdl_pop6_jp_pops"]
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        
        df_schema = StructType([
                StructField("Status", StringType(), True),
                StructField("popdb_id", StringType(), True),
                StructField("pop_code", StringType(), True),
                StructField("pop_name", StringType(), True),
                StructField("address", StringType(), True),
                StructField("longitude", StringType(), True),
                StructField("latitude", StringType(), True),
                StructField("Business_Units_ID", StringType(), True),
                StructField("Business_Unit_Name", StringType(), True),
                StructField("country", StringType(), True),
                StructField("channel", StringType(), True),
                StructField("retail_environment_ps", StringType(), True),
                StructField("sales_group_name", StringType(), True),
                StructField("customer", StringType(), True),
               StructField("Store_Type", StringType(), True),
                StructField("sales_group_code", StringType(), True),
                StructField("Customer_Grade", StringType(), True),
                StructField("territory", StringType(), True),
                
        
                
            ])
        
        df = session.read.option("INFER_SCHEMA", True)\\
            .option("field_delimiter", ",")\\
            .option("field_optionally_enclosed_by", "\\"") \\
            .option("trim_space", True) \\
            .with_metadata("METADATA$FILE_ROW_NUMBER") \\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        df_columns=df.first()[1:]
        
        df = df.filter(col("METADATA$FILE_ROW_NUMBER") > lit(1))
        
        
        df = df.drop("METADATA$FILE_ROW_NUMBER")
        #return df.schema.names
        o=0

        for i in df_columns:
            

            df=df.withColumnRenamed(df.schema.names[o],i.replace(" ", "_").replace("(","").replace(")","").replace("]","").replace("[",""))
            #df=df.withColumnRenamed(df.schema.names[o],(re.sub(r"[^a-zA-Z0-9_]","",i.replace(" ", "_").replace("(","").replace(")","").replace("]","").replace("[",""))).strip("_"))
            o+=1

       
        
        df=df.na.drop("all")
        if df.count()==0 :
            return "No Data in file"

        df = df.with_column(''file_name'',lit(file_name))


        def generunid():
            new_time = datetime.now(pytz.utc) + timedelta(hours=8)
            return new_time.strftime(''%Y%m%d%H%M%S'')

        def generatecrtd_dttm():
            new_time = datetime.now(pytz.utc) + timedelta(hours=8)
            return new_time.strftime(''%Y-%m-%d %H:%M:%S'')
            
            

        df = df.with_column(''hashkey'',md5(concat(when(is_null(col(''status'')),lit('''')).otherwise(col(''status'')),\\
                                            when(is_null(col(''popdb_id'')),lit('''')).otherwise(col(''popdb_id'')),\\
                                          when(is_null(col(''pop_code'')),lit('''')).otherwise(col(''pop_code'')),\\
                                          when(is_null(col(''pop_name'')),lit('''')).otherwise(col(''pop_name'')),\\
                                         when(is_null(col(''address'')),lit('''')).otherwise(col(''address'')),\\
                                         when(is_null(col(''longitude'')),lit('''')).otherwise(col(''longitude'')),\\
                                         when(is_null(col(''latitude'')),lit('''')).otherwise(col(''latitude'')),\\
                                          when(is_null(col(''Business_Units_ID'')),lit('''')).otherwise(col(''Business_Units_ID'')),\\
                                         when(is_null(col(''Business_Unit_Name'')),lit('''')).otherwise(col(''Business_Unit_Name'')),\\
                                         when(is_null(col(''country'')),lit('''')).otherwise(col(''country'')),\\
                                         when(is_null(col(''channel'')),lit('''')).otherwise(col(''channel'')),\\
                                         when(is_null(col(''retail_environment_ps'')),lit('''')).otherwise(col(''retail_environment_ps'')),\\
                                         when(is_null(col(''sales_group_code'')),lit('''')).otherwise(col(''sales_group_code'')),\\
                                        when(is_null(col(''customer'')),lit('''')).otherwise(col(''customer'')),\\
                                          when(is_null(col(''sales_group_name'')),lit('''')).otherwise(col(''sales_group_name'')),
                                                when(is_null(col(''customer_grade'')),lit('''')).otherwise(col(''customer_grade'')),\\
                                                 when(is_null(col(''Territory/Region'')),lit('''')).otherwise(col(''Territory/Region'')))))
                                         
        
        df = df.with_column(''run_id'',lit(generunid()))

        df = df.with_column(''crtd_dttm'',lit(generatecrtd_dttm()))
        

        

        
        snowdf= df.select(
                    "Status",
                "popdb_id",
                "pop_code",
                "pop_name",
                "address",
                "longitude",
                "latitude",
                "Business_Units_ID",
                "Business_Unit_Name",
                "country",
                "channel",
                "retail_environment_ps",
                "sales_group_name",
                "customer",
                "sales_group_code",
                "customer_grade",
                "Territory/Region",
                "file_name",
                "run_id",
                "crtd_dttm",
                "hashkey"
                
                
            
                    )
        file_name=file_name.split(".")[0]+"_"+datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")

        snowdf.write.mode("append").saveAsTable(stage_name.split(".")[0] + "." + target_table)
        
        # #move to success
        snowdf.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)
        
        
        
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
CREATE OR REPLACE PROCEDURE POP6_JP_MASTER_PRODUCTS_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them.

from snowflake.snowpark.functions import col, lit, date_format, current_timestamp, to_date, year, month, concat, format_number, regexp_replace,to_timestamp,when,trim,upper,is_null,md5
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DecimalType
import snowflake.snowpark
import pandas as pd
from datetime import datetime,timedelta
import snowflake.snowpark as snowpark
import pytz
import pandas as pd

def main(session:snowpark.Session,Param):
    # Param=["20240617_products.csv","JPNSDL_RAW.DEV_LOAD_STAGE_ADLS","dev/pop6/master/masterdata/products/","sdl_pop6_jp_products"]
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]

        
        
        df_schema = StructType([
                StructField("Status", StringType(), True),
                StructField("productdb_id", StringType(), True),
                StructField("barcode", StringType(), True),
                StructField("sku", StringType(), True),
                StructField("unit_price", StringType(), True),
                StructField("display_order", StringType(), True),
                StructField("launch_date", StringType(), True),
                StructField("largest_uom_quantity", StringType(), True),
                StructField("middle_uom_quantity", StringType(), True),
                StructField("smallest_uom_quantity", StringType(), True),
                StructField("sku_english", StringType(), True),
                StructField("company", StringType(), True),
                StructField("sku_code", StringType(), True),
                StructField("ps_category", StringType(), True),
                StructField("ps_segment", StringType(), True),
                StructField("ps_category_segment", StringType(), True),
                StructField("country_l1", StringType(), True),
                StructField("regional_franchise_l2", StringType(), True),
                StructField("franchise_l3", StringType(), True),
                StructField("brand_l4", StringType(), True),
                StructField("sub_category_l5", StringType(), True),
                StructField("platform_level_l6", StringType(), True),
                StructField("variance_l7", StringType(), True),
                StructField("pack_size_l8", StringType(), True)
                
            ])
        
        df = session.read.option("INFER_SCHEMA", True)\\
            .option("field_delimiter", ",")\\
            .option("field_optionally_enclosed_by", "\\"") \\
            .option("trim_space", True) \\
            .with_metadata("METADATA$FILE_ROW_NUMBER") \\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        df_columns=df.first()[1:]
        
        df = df.filter(col("METADATA$FILE_ROW_NUMBER") > lit(1))
        
        
        df = df.drop("METADATA$FILE_ROW_NUMBER")
        #return df.schema.names
        o=0

        for i in df_columns:
            

            df=df.withColumnRenamed(df.schema.names[o],i.replace(" ", "_").replace("(","").replace(")","").replace("]","").replace("[",""))
            #df=df.withColumnRenamed(df.schema.names[o],(re.sub(r"[^a-zA-Z0-9_]","",i.replace(" ", "_").replace("(","").replace(")","").replace("]","").replace("[",""))).strip("_"))
            o+=1

       
        
        df=df.na.drop("all")
        if df.count()==0 :
            return "No Data in file"

        df = df.with_column(''file_name'',lit(file_name))


        def generunid():
            new_time = datetime.now(pytz.utc) + timedelta(hours=8)
            return new_time.strftime(''%Y%m%d%H%M%S'')

        def generatecrtd_dttm():
            new_time = datetime.now(pytz.utc) + timedelta(hours=8)
            return new_time.strftime(''%Y-%m-%d %H:%M:%S'')
            

       

        df = df.with_column(''hashkey'',md5(concat(when(is_null(col(''status'')),lit('''')).otherwise(col(''status'')),\\
                                            when(is_null(col(''productdb_id'')),lit('''')).otherwise(col(''productdb_id'')),\\
                                          when(is_null(col(''barcode'')),lit('''')).otherwise(col(''barcode'')),\\
                                          when(is_null(col(''sku'')),lit('''')).otherwise(col(''sku'')),\\
                                         when(is_null(col(''unit_price'')),lit('''')).otherwise(col(''unit_price'')),\\
                                         when(is_null(col(''display_order'')),lit('''')).otherwise(col(''display_order'')),\\
                                         when(is_null(col(''launch_date'')),lit('''')).otherwise(col(''launch_date'')),\\
                                          when(is_null(col(''largest_uom_quantity'')),lit('''')).otherwise(col(''largest_uom_quantity'')),\\
                                         when(is_null(col(''middle_uom_quantity'')),lit('''')).otherwise(col(''middle_uom_quantity'')),\\
                                         when(is_null(col(''smallest_uom_quantity'')),lit('''')).otherwise(col(''smallest_uom_quantity'')),\\
                                         when(is_null(col(''company'')),lit('''')).otherwise(col(''company'')),\\
                                         when(is_null(col(''sku_english'')),lit('''')).otherwise(col(''sku_english'')),\\
                                         when(is_null(col(''sku_code'')),lit('''')).otherwise(col(''sku_code'')),\\
                                        when(is_null(col(''ps_category'')),lit('''')).otherwise(col(''ps_category'')),\\
                                          when(is_null(col(''ps_segment'')),lit('''')).otherwise(col(''ps_segment'')),\\
                                         when(is_null(col(''ps_category_segment'')),lit('''')).otherwise(col(''ps_category_segment'')),\\
                                         when(is_null(col(''Country-L1'')),lit('''')).otherwise(col(''Country-L1'')),\\
                                         when(is_null(col(''Regional_Franchise-L2'')),lit('''')).otherwise(col(''Regional_Franchise-L2'')),\\
                                          when(is_null(col(''Franchise-L3'')),lit('''')).otherwise(col(''Franchise-L3'')),\\
                                         when(is_null(col(''Brand-L4'')),lit('''')).otherwise(col(''Brand-L4'')),\\
                                         when(is_null(col(''Sub_Category-L5'')),lit('''')).otherwise(col(''Sub_Category-L5'')),\\
                                         when(is_null(col(''Platform-L6'')),lit('''')).otherwise(col(''Platform-L6'')),\\
                                         when(is_null(col(''Variance-L7'')),lit('''')).otherwise(col(''Variance-L7'')),\\
                                         when(is_null(col(''Pack_size-L8'')),lit('''')).otherwise(col(''Pack_size-L8'')))))

        
        df = df.with_column(''run_id'',lit(generunid()))

        df = df.with_column(''crtd_dttm'',lit(generatecrtd_dttm()))
        
        columns = [
    "Status",
    "ProductDB_ID",
    "Barcode",
    "SKU",
    "Unit_Price",
    "Display_order",
    "Launch_Date",
    "Largest_UOM_Quantity",
    "Middle_UOM_Quantity",
    "Smallest_UOM_Quantity",
    "SKU_English",  # Parentheses removed
    "Company",
    "SKU_Code",
    "PS_Category",
    "PS_Segment",
    "PS_Category_Segment",
    "Country-L1",
    "Regional_Franchise-L2",
    "Franchise-L3",
    "Brand-L4",
    "Sub_Category-L5",
    "Platform-L6",
    "Variance-L7",
    "Pack_size-L8"
]
          
        snowdf= df.select(
                   "Status", 
                "productdb_id", 
                "barcode", 
                "sku", 
                "unit_price", 
                "display_order", 
                "launch_date", 
                "largest_uom_quantity", 
                "middle_uom_quantity", 
                "smallest_uom_quantity", 
                "company", 
                "sku_english", 
                "sku_code", 
                "ps_category", 
                "ps_segment", 
                "ps_category_segment", 
                "Country-L1", 
                "Regional_Franchise-L2", 
                "Franchise-L3", 
                "Brand-L4", 
                "Sub_Category-L5", 
                "Platform-L6", 
                "Variance-L7", 
                "Pack_size-L8",
                "file_name",
                "run_id",
                "crtd_dttm",
                "hashkey"
            
                    )
        file_name=file_name.split(".")[0]+"_"+datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")

        snowdf.write.mode("append").saveAsTable(stage_name.split(".")[0] + "." + target_table)
        
        # #move to success
        snowdf.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)
        
        
        
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
CREATE OR REPLACE PROCEDURE POP6_JP_MASTER_PRODUCT_LISTs_ALLOCATION_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them.

from snowflake.snowpark.functions import col, lit, date_format, current_timestamp, to_date, year, month, concat, format_number, regexp_replace,to_timestamp,when,trim,upper,is_null,md5
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DecimalType
import snowflake.snowpark
import pandas as pd
from datetime import datetime,timedelta
import snowflake.snowpark as snowpark
import pytz
import pandas as pd

def main(session:snowpark.Session,Param):
    # Param=["20240617_product_lists_allocation.csv","JPNSDL_RAW.DEV_LOAD_STAGE_ADLS","dev/pop6/master/listdata/product_lists_allocation","sdl_pop6_jp_product_lists_allocation"]
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]


        
        df_schema = StructType([
                StructField("Product_Group_Status", StringType(), True),
                StructField("Product_Group", StringType(), True),
                StructField("Product_List_Status", StringType(), True),
                StructField("Product_List", StringType(), True),
                StructField("Product_List_Code", StringType(), True),
                StructField("POP_Attribute_ID", StringType(), True),
                StructField("POP_Attribute", StringType(), True),
                StructField("POP_Attribute_Value_ID", StringType(), True),
                StructField("POP_Attribute_Value", StringType(), True),
                StructField("Date", StringType(), True),
                
                
                
            ])
        
        df = session.read.option("INFER_SCHEMA", True)\\
            .option("field_delimiter", ",")\\
            .option("field_optionally_enclosed_by", "\\"") \\
            .option("trim_space", True) \\
            .with_metadata("METADATA$FILE_ROW_NUMBER") \\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        df_columns=df.first()[1:]
        
        df = df.filter(col("METADATA$FILE_ROW_NUMBER") > lit(1))
        
        
        df = df.drop("METADATA$FILE_ROW_NUMBER")
        #return df.schema.names
        o=0

        for i in df_columns:
            

            df=df.withColumnRenamed(df.schema.names[o],i.replace(" ", "_").replace("(","").replace(")","").replace("]","").replace("[",""))
            #df=df.withColumnRenamed(df.schema.names[o],(re.sub(r"[^a-zA-Z0-9_]","",i.replace(" ", "_").replace("(","").replace(")","").replace("]","").replace("[",""))).strip("_"))
            o+=1

       
        
        df=df.na.drop("all")
        if df.count()==0 :
            return "No Data in file"

        df = df.with_column(''file_name'',lit(file_name))


        def generunid():
            new_time = datetime.now(pytz.utc) + timedelta(hours=8)
            return new_time.strftime(''%Y%m%d%H%M%S'')

        def generatecrtd_dttm():
            new_time = datetime.now(pytz.utc) + timedelta(hours=8)
            return new_time.strftime(''%Y-%m-%d %H:%M:%S'')
            
            

        df = df.with_column(''hashkey'',md5(concat(when(is_null(col(''Product_Group_Status'')),lit('''')).otherwise(col(''Product_Group_Status'')),\\
                                            when(is_null(col(''Product_Group'')),lit('''')).otherwise(col(''Product_Group'')),\\
                                          when(is_null(col(''Product_List_Status'')),lit('''')).otherwise(col(''Product_List_Status'')),\\
                                          when(is_null(col(''Product_List'')),lit('''')).otherwise(col(''Product_List'')),\\
                                         when(is_null(col(''POP_Attribute_ID'')),lit('''')).otherwise(col(''POP_Attribute_ID'')),\\
                                         when(is_null(col(''POP_Attribute'')),lit('''')).otherwise(col(''POP_Attribute'')),\\
                                           when(is_null(col(''POP_Attribute_Value_ID'')),lit('''')).otherwise(col(''POP_Attribute_Value_ID'')),\\
                                          when(is_null(col(''POP_Attribute_Value'')),lit('''')).otherwise(col(''POP_Attribute_Value'')),\\
                                         when(is_null(col(''date'')),lit('''')).otherwise(col(''date'')))))

        
        df = df.with_column(''run_id'',lit(generunid()))

        df = df.with_column(''crtd_dttm'',lit(generatecrtd_dttm()))
        

        

        
        snowdf= df.select(
                    "Product_Group_Status",
                    "Product_Group",
                    "Product_List_Status",
                    "Product_List",
                    "Product_List_code",
                    "POP_Attribute_ID",
                    "POP_Attribute",
                    "POP_Attribute_Value_ID",
                    "POP_Attribute_Value",
                    "date",
                    "file_name",
                    "run_id",
                    "crtd_dttm",
                    "hashkey"
            
                    )
        file_name=file_name.split(".")[0]+"_"+datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")

        snowdf.write.mode("append").saveAsTable(stage_name.split(".")[0] + "." + target_table)
        
        # #move to success
        snowdf.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)
        
        
        
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
CREATE OR REPLACE PROCEDURE POP6_JP_MASTER_POP_LISTS_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them.

from snowflake.snowpark.functions import col, lit, date_format, current_timestamp, to_date, year, month, concat, format_number, regexp_replace,to_timestamp,when,trim,upper,is_null,md5
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DecimalType
import snowflake.snowpark
import pandas as pd
from datetime import datetime,timedelta
import snowflake.snowpark as snowpark
import pytz
import pandas as pd

def main(session:snowpark.Session,Param):
    # Param=["20240617_pop_lists.csv","JPNSDL_RAW.DEV_LOAD_STAGE_ADLS","dev/pop6/master/listdata/pop_lists","sdl_pop6_jp_pop_lists"]
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        
        df_schema = StructType([
                StructField("Status", StringType(), True),
                StructField("pop_list", StringType(), True),
                StructField("popdb_id", StringType(), True),
                StructField("pop_code", StringType(), True),
                StructField("pop_name", StringType(), True),
                StructField("Date", StringType(), True)
                
                
            ])
        
        df = session.read.option("INFER_SCHEMA", True)\\
            .option("field_delimiter", ",")\\
            .option("field_optionally_enclosed_by", "\\"") \\
            .option("trim_space", True) \\
            .with_metadata("METADATA$FILE_ROW_NUMBER") \\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        df_columns=df.first()[1:]
        
        df = df.filter(col("METADATA$FILE_ROW_NUMBER") > lit(1))
        
        
        df = df.drop("METADATA$FILE_ROW_NUMBER")
        #return df.schema.names
        o=0

        for i in df_columns:
            

            df=df.withColumnRenamed(df.schema.names[o],i.replace(" ", "_").replace("(","").replace(")","").replace("]","").replace("[",""))
            #df=df.withColumnRenamed(df.schema.names[o],(re.sub(r"[^a-zA-Z0-9_]","",i.replace(" ", "_").replace("(","").replace(")","").replace("]","").replace("[",""))).strip("_"))
            o+=1

       
        
        df=df.na.drop("all")
        if df.count()==0 :
            return "No Data in file"

        df = df.with_column(''file_name'',lit(file_name))


        def generunid():
            new_time = datetime.now(pytz.utc) + timedelta(hours=8)
            return new_time.strftime(''%Y%m%d%H%M%S'')

        def generatecrtd_dttm():
            new_time = datetime.now(pytz.utc) + timedelta(hours=8)
            return new_time.strftime(''%Y-%m-%d %H:%M:%S'')
            
            

        df = df.with_column(''hashkey'',md5(concat(when(is_null(col(''status'')),lit('''')).otherwise(col(''status'')),\\
                                            when(is_null(col(''pop_list'')),lit('''')).otherwise(col(''pop_list'')),\\
                                          when(is_null(col(''popdb_id'')),lit('''')).otherwise(col(''popdb_id'')),\\
                                          when(is_null(col(''pop_code'')),lit('''')).otherwise(col(''pop_code'')),\\
                                         when(is_null(col(''pop_name'')),lit('''')).otherwise(col(''pop_name'')),\\
                                         when(is_null(col(''date'')),lit('''')).otherwise(col(''date'')))))

        
        df = df.with_column(''run_id'',lit(generunid()))

        df = df.with_column(''crtd_dttm'',lit(generatecrtd_dttm()))
        

        

        
        snowdf= df.select(
                    "status",
                    "pop_list",
                    "popdb_id",
                    "pop_code",
                    "pop_name",
                    "date",
                    "file_name",
                    "run_id",
                    "crtd_dttm",
                    "hashkey"
            
                    )
        file_name=file_name.split(".")[0]+"_"+datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")

        snowdf.write.mode("append").saveAsTable(stage_name.split(".")[0] + "." + target_table)
        
        # #move to success
        snowdf.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)
        
        
        
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
CREATE OR REPLACE PROCEDURE POP6_JP_MASTER_PRODUCT_LISTS_PRODUCTS_preprocessing("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them.

from snowflake.snowpark.functions import col, lit, date_format, current_timestamp, to_date, year, month, concat, format_number, regexp_replace,to_timestamp,when,trim,upper,is_null,md5
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DecimalType
import snowflake.snowpark
import pandas as pd
from datetime import datetime,timedelta
import snowflake.snowpark as snowpark
import pytz
import pandas as pd

def main(session:snowpark.Session,Param):
    # Param=["20240617_product_lists_products.csv","JPNSDL_RAW.DEV_LOAD_STAGE_ADLS","dev/pop6/master/listdata/product_lists_products/","sdl_pop6_jp_product_lists_products"]
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        
        df_schema = StructType([
                StructField("Product_List", StringType(), True),
                StructField("Product_List_Code", StringType(), True),
                StructField("ProductDB_ID", StringType(), True),
                StructField("SKU", StringType(), True),
                StructField("MSL_Ranking", StringType(), True),
                StructField("Date", StringType(), True),
                
                
            ])
        
        df = session.read.option("INFER_SCHEMA", True)\\
            .option("field_delimiter", ",")\\
            .option("field_optionally_enclosed_by", "\\"") \\
            .option("trim_space", True) \\
            .with_metadata("METADATA$FILE_ROW_NUMBER") \\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        df_columns=df.first()[1:]
        
        df = df.filter(col("METADATA$FILE_ROW_NUMBER") > lit(1))
        
        
        df = df.drop("METADATA$FILE_ROW_NUMBER")
        #return df.schema.names
        o=0

        for i in df_columns:
            

            df=df.withColumnRenamed(df.schema.names[o],i.replace(" ", "_").replace("(","").replace(")","").replace("]","").replace("[",""))
            #df=df.withColumnRenamed(df.schema.names[o],(re.sub(r"[^a-zA-Z0-9_]","",i.replace(" ", "_").replace("(","").replace(")","").replace("]","").replace("[",""))).strip("_"))
            o+=1

       
        
        df=df.na.drop("all")
        if df.count()==0 :
            return "No Data in file"

        df = df.with_column(''file_name'',lit(file_name))


        def generunid():
            new_time = datetime.now(pytz.utc) + timedelta(hours=8)
            return new_time.strftime(''%Y%m%d%H%M%S'')

        def generatecrtd_dttm():
            new_time = datetime.now(pytz.utc) + timedelta(hours=8)
            return new_time.strftime(''%Y-%m-%d %H:%M:%S'')
            
            

        df = df.with_column(''hashkey'',md5(concat(when(is_null(col(''Product_List'')),lit('''')).otherwise(col(''Product_List'')),\\
                                            when(is_null(col(''Product_List_Code'')),lit('''')).otherwise(col(''Product_List_Code'')),\\
                                          when(is_null(col(''ProductDB_ID'')),lit('''')).otherwise(col(''ProductDB_ID'')),\\
                                          when(is_null(col(''SKU'')),lit('''')).otherwise(col(''SKU'')),\\
                                         when(is_null(col(''MSL_Ranking'')),lit('''')).otherwise(col(''MSL_Ranking'')),\\
                                            when(is_null(col(''Date'')),lit('''')).otherwise(col(''Date'')))))

        
        df = df.with_column(''run_id'',lit(generunid()))

        df = df.with_column(''crtd_dttm'',lit(generatecrtd_dttm()))
        

        

        
        snowdf= df.select(
                    "product_list",
                    "Product_List_Code",
                    "productdb_id",
                    "sku",
                    "msl_ranking",
                    "date",
                    "file_name",
                    "run_id",
                    "crtd_dttm",
                    "hashkey"
                    
            
                    )
        file_name=file_name.split(".")[0]+"_"+datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")

        snowdf.write.mode("append").saveAsTable(stage_name.split(".")[0] + "." + target_table)
        
        # #move to success
        snowdf.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)
        
        
        
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
CREATE OR REPLACE PROCEDURE POP6_JP_MASTER_PRODUCT_LISTS_POPS_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them.

from snowflake.snowpark.functions import col, lit, date_format, current_timestamp, to_date, year, month, concat, format_number, regexp_replace,to_timestamp,when,trim,upper,is_null,md5
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DecimalType
import snowflake.snowpark
import pandas as pd
from datetime import datetime,timedelta
import snowflake.snowpark as snowpark
import pytz
import pandas as pd

def main(session:snowpark.Session,Param):
    # Param=["20240617_product_lists_pops.csv","JPNSDL_RAW.DEV_LOAD_STAGE_ADLS","dev/pop6/master/listdata/product_lists_pops","sdl_pop6_jp_product_lists_pops"]
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        
        df_schema = StructType([
                StructField("Product_List", StringType(), True),
                StructField("Product_List_Code", StringType(), True),
                StructField("POPDB_ID", StringType(), True),
                StructField("POP_Code", StringType(), True),
                StructField("POP_Name", StringType(), True),
                StructField("Date", StringType(), True),
                
                
            ])
        
        df = session.read.option("INFER_SCHEMA", True)\\
            .option("field_delimiter", ",")\\
            .option("field_optionally_enclosed_by", "\\"") \\
            .option("trim_space", True) \\
            .with_metadata("METADATA$FILE_ROW_NUMBER") \\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        df_columns=df.first()[1:]
        
        df = df.filter(col("METADATA$FILE_ROW_NUMBER") > lit(1))
        
        
        df = df.drop("METADATA$FILE_ROW_NUMBER")
        #return df.schema.names
        o=0

        for i in df_columns:
            

            df=df.withColumnRenamed(df.schema.names[o],i.replace(" ", "_").replace("(","").replace(")","").replace("]","").replace("[",""))
            #df=df.withColumnRenamed(df.schema.names[o],(re.sub(r"[^a-zA-Z0-9_]","",i.replace(" ", "_").replace("(","").replace(")","").replace("]","").replace("[",""))).strip("_"))
            o+=1

       
        
        df=df.na.drop("all")
        if df.count()==0 :
            return "No Data in file"

        df = df.with_column(''file_name'',lit(file_name))


        def generunid():
            new_time = datetime.now(pytz.utc) + timedelta(hours=8)
            return new_time.strftime(''%Y%m%d%H%M%S'')

        def generatecrtd_dttm():
            new_time = datetime.now(pytz.utc) + timedelta(hours=8)
            return new_time.strftime(''%Y-%m-%d %H:%M:%S'')
            
            

        df = df.with_column(''hashkey'',md5(concat(when(is_null(col(''product_list'')),lit('''')).otherwise(col(''product_list'')),\\
                                            when(is_null(col(''Product_List_Code'')),lit('''')).otherwise(col(''Product_List_Code'')),\\
                                          when(is_null(col(''POPDB_ID'')),lit('''')).otherwise(col(''POPDB_ID'')),\\
                                          when(is_null(col(''POP_Code'')),lit('''')).otherwise(col(''POP_Code'')),\\
                                         when(is_null(col(''POP_Name'')),lit('''')).otherwise(col(''POP_Name'')),\\
                                            when(is_null(col(''date'')),lit('''')).otherwise(col(''date'')))))

        
        df = df.with_column(''run_id'',lit(generunid()))

        df = df.with_column(''crtd_dttm'',lit(generatecrtd_dttm()))
        

        

        
        snowdf= df.select(
                    "product_list",
                    "popdb_id",
                    "pop_code",
                    "pop_name",
                    "date",
                    "file_name",
                    "run_id",
                    "crtd_dttm",
                    "hashkey"
                    
            
                    )
        file_name=file_name.split(".")[0]+"_"+datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")

        snowdf.write.mode("append").saveAsTable(stage_name.split(".")[0] + "." + target_table)
        
        # #move to success
        snowdf.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)
        
        
        
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
