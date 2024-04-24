CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.VNMSDL_RAW.VN_MT_SELLIN_COOP_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
PACKAGES = ('openpyxl==3.0.10','snowflake-snowpark-python==*')
HANDLER = 'main'
EXECUTE AS CALLER
AS '
from snowflake.snowpark.functions import col, lit, current_timestamp, trunc
from snowflake.snowpark.types import StringType, StructType, StructField,DecimalType
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz
import pandas as pd
from snowflake.snowpark.files import SnowflakeFile
from snowflake.snowpark import Row 

def main(session:snowpark.Session,Param):

    try:
        file_name       = Param[0]
        stage_name      = Param[1] # Stage name where files are located
        temp_stage_path = Param[2]  # Path in the stage
        target_table    = Param[3] # Target table for the data
        stage_path      = "@{0}/{1}/{2}".format(stage_name,temp_stage_path,file_name)
        current_date    = datetime.now()
        formatted_year  = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        transformed_rows= []
        final_df        = None
        store_list      =[''112'',''114'',''118'',''119'',''120'',''121'',''122'',''123'',''124'',''127'',''128'',''130'',''131'',''132'',''133'',''134'',''135'',''136'',''137'',''138'',''140'',''141'',''142'',''144'',''145'',''147'',''148'',''150'',''151'',''152'',''153'',''154'',''155'',''157'',''158'',''159'',''160'',''161'',''162'',''164'',''167'',''170'',''171'',''173'',''174'',''175'',''176'',''178'',''179'',''180'',''183'',''184'',''185'',''186'',''187'',''189'',''196'',''197'',''199'',''299'',''301'',''303'',''304'',''305'',''306'',''4200'',''501'',''502'',''503'',''504'',''505'',''506'',''507'',''508'',''509'',''510'',''511'',''512'',''513'',''514'',''515'',''516'',''517'',''518'',''519'',''520'',''521'',''522'',''523'',''524'',''525'',''526'',''527'',''528'',''529'',''530'',''531'',''532'',''533'',''534'',''535'',''536'',''537'',''538'',''539'',''540'',''541'',''542'',''543'',''545'',''546'',''547'',''548'',''549'',''552'',''553'',''554'',''555'',''556'',''557'',''559'',''560'',''561'',''562'',''563'',''564'',''565'',''566'',''567'',''569'',''570''

]
        
        session.use_schema(stage_name.split(''.'')[0])
        
        df_schema = StructType([
            StructField("SKU", StringType()),
            StructField("idescr", StringType()),
            StructField("vendor", StringType()),
            StructField("asname", StringType()),
            StructField("imfgr", StringType()),
            StructField("112",DecimalType()),
            StructField("114",DecimalType()),
            StructField("118",DecimalType()),
            StructField("119",DecimalType()),
            StructField("120",DecimalType()),
            StructField("121",DecimalType()),
            StructField("122",DecimalType()),
            StructField("123",DecimalType()),
            StructField("124",DecimalType()),
            StructField("127",DecimalType()),
            StructField("128",DecimalType()),
            StructField("130",DecimalType()),
            StructField("131",DecimalType()),
            StructField("132",DecimalType()),
            StructField("133",DecimalType()),
            StructField("134",DecimalType()),
            StructField("135",DecimalType()),
            StructField("136",DecimalType()),
            StructField("137",DecimalType()),
            StructField("138",DecimalType()),
            StructField("140",DecimalType()),
            StructField("141",DecimalType()),
            StructField("142",DecimalType()),
            StructField("144",DecimalType()),
            StructField("145",DecimalType()),
            StructField("147",DecimalType()),
            StructField("148",DecimalType()),
            StructField("150",DecimalType()),
            StructField("151",DecimalType()),
            StructField("152",DecimalType()),
            StructField("153",DecimalType()),
            StructField("154",DecimalType()),
            StructField("155",DecimalType()),
            StructField("157",DecimalType()),
            StructField("158",DecimalType()),
            StructField("159",DecimalType()),
            StructField("160",DecimalType()),
            StructField("161",DecimalType()),
            StructField("162",DecimalType()),
            StructField("164",DecimalType()),
            StructField("167",DecimalType()),
            StructField("170",DecimalType()),
            StructField("171",DecimalType()),
            StructField("173",DecimalType()),
            StructField("174",DecimalType()),
            StructField("175",DecimalType()),
            StructField("176",DecimalType()),
            StructField("178",DecimalType()),
            StructField("179",DecimalType()),
            StructField("180",DecimalType()),
            StructField("183",DecimalType()),
            StructField("184",DecimalType()),
            StructField("185",DecimalType()),
            StructField("186",DecimalType()),
            StructField("187",DecimalType()),
            StructField("189",DecimalType()),
            StructField("196",DecimalType()),
            StructField("197",DecimalType()),
            StructField("199",DecimalType()),
            StructField("299",DecimalType()),
            StructField("301",DecimalType()),
            StructField("303",DecimalType()),
            StructField("304",DecimalType()),
            StructField("305",DecimalType()),
            StructField("306",DecimalType()),
            StructField("4200",DecimalType()),
            StructField("501",DecimalType()),
            StructField("502",DecimalType()),
            StructField("503",DecimalType()),
            StructField("504",DecimalType()),
            StructField("505",DecimalType()),
            StructField("506",DecimalType()),
            StructField("507",DecimalType()),
            StructField("508",DecimalType()),
            StructField("509",DecimalType()),
            StructField("510",DecimalType()),
            StructField("511",DecimalType()),
            StructField("512",DecimalType()),
            StructField("513",DecimalType()),
            StructField("514",DecimalType()),
            StructField("515",DecimalType()),
            StructField("516",DecimalType()),
            StructField("517",DecimalType()),
            StructField("518",DecimalType()),
            StructField("519",DecimalType()),
            StructField("520",DecimalType()),
            StructField("521",DecimalType()),
            StructField("522",DecimalType()),
            StructField("523",DecimalType()),
            StructField("524",DecimalType()),
            StructField("525",DecimalType()),
            StructField("526",DecimalType()),
            StructField("527",DecimalType()),
            StructField("528",DecimalType()),
            StructField("529",DecimalType()),
            StructField("530",DecimalType()),
            StructField("531",DecimalType()),
            StructField("532",DecimalType()),
            StructField("533",DecimalType()),
            StructField("534",DecimalType()),
            StructField("535",DecimalType()),
            StructField("536",DecimalType()),
            StructField("537",DecimalType()),
            StructField("538",DecimalType()),
            StructField("539",DecimalType()),
            StructField("540",DecimalType()),
            StructField("541",DecimalType()),
            StructField("542",DecimalType()),
            StructField("543",DecimalType()),
            StructField("545",DecimalType()),
            StructField("546",DecimalType()),
            StructField("547",DecimalType()),
            StructField("548",DecimalType()),
            StructField("549",DecimalType()),
            StructField("552",DecimalType()),
            StructField("553",DecimalType()),
            StructField("554",DecimalType()),
            StructField("555",DecimalType()),
            StructField("556",DecimalType()),
            StructField("557",DecimalType()),
            StructField("559",DecimalType()),
            StructField("560",DecimalType()),
            StructField("561",DecimalType()),
            StructField("562",DecimalType()),
            StructField("563",DecimalType()),
            StructField("564",DecimalType()),
            StructField("565",DecimalType()),
            StructField("566",DecimalType()),
            StructField("567",DecimalType()),
            StructField("569",DecimalType()),
            StructField("570",DecimalType())
            
        ])

        df = session.read \\
            .schema(df_schema) \\
            .option("skip_header",1) \\
            .option("field_delimiter", "\\u0001") \\
            .option("encoding", "UTF-8") \\
            .option("field_optionally_enclosed_by", "\\"") \\
            .csv(stage_path)
        

        # Check if DataFrame is empty
        df = df.na.drop("all")
        if df.count() == 0:
            return "No Data in file"

        condition = (col("sku").isNotNull()) | (col("idescr").isNotNull()) | (col("vendor").isNotNull()) | (col("asname").isNotNull())

        df_filter=df.filter(condition)
        run_id=datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        crtd_dttm=datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")


        for row in df_filter.collect():
            for store in store_list:
                new_row = row.as_dict()
                new_row["store"]=store
                new_row["sales_quantity"]=row[store]
                new_row["run_id"] = run_id
                new_row["crtd_dttm"] = crtd_dttm
                new_row["filename"]=file_name
                final_row=Row(**new_row)
                transformed_rows.append(final_row)
        transformed_df = session.createDataFrame(transformed_rows)

        
        snowdf=transformed_df.select("SKU","IDESCR","VENDOR","ASNAME","IMFGR","STORE","SALES_QUANTITY","FILENAME","RUN_ID","CRTD_DTTM")

        snowdf.write.mode("append").save_as_table(stage_name.split(".")[0] + "." + target_table)

        snowdf.write.copy_into_location("@" + stage_name + "/" + temp_stage_path + "/processed/success/" + formatted_year + "/" + formatted_month + "/" + file_name, header=True, OVERWRITE=True)

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
		
		
		
		
		
CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.VNMSDL_RAW.VNMT_SELLIN_DKSH_SELL_PREPROCESSING("PARAM" ARRAY)
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
	# CALL VNMSDL_RAW.VNMT_Sellin_DKSH_Sell_PREPROCESSING([''DKSH_Sell_In_20231113.xlsx'',''VNMSDL_RAW.PROD_LOAD_STAGE_ADLS'',''MT/transaction/Sellin/Transaction'',''sdl_vn_mt_sellin_dksh''])
	
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
            StructField("supplier_code" , StringType()),
            StructField("supplier_name" , StringType()),
            StructField("plant" , StringType()),
            StructField("productid" , StringType()),
            StructField("product" , StringType()),
            StructField("brand" , StringType()),
            StructField("sellin_category" , StringType()),
            StructField("product_group" , StringType()),
            StructField("product_sub_group" , StringType()),
            StructField("unit_of_measurement" , StringType()),
            StructField("custcode" , StringType()),
            StructField("customer" , StringType()),
            StructField("address" , StringType()),
            StructField("district" , StringType()),
            StructField("province" , StringType()),
            StructField("region" , StringType()),
            StructField("zone" , StringType()),
            StructField("channel" , StringType()),
            StructField("sellin_sub_channel" , StringType()),
            StructField("cust_group" , StringType()),
            StructField("billing_no" , StringType()),
            StructField("invoice_date" , StringType()),
            StructField("qty_include_foc" , IntegerType()),
            StructField("qty_exclude_foc" , IntegerType()),
            StructField("foc" , StringType()),
            StructField("net_price_wo_vat" , DecimalType()),
            StructField("tax" , DecimalType()),
            StructField("net_amount_wo_vat" , DecimalType()),
            StructField("net_amount_w_vat" , DecimalType()),
            StructField("gross_amount_wo_vat" , DecimalType()),
            StructField("gross_amount_w_vat" , DecimalType()),
            StructField("list_price_wo_vat" , DecimalType()),
            StructField("vendor_lot" , StringType()),
            StructField("order_type" , StringType()),
            StructField("red_invoice_no" , StringType()),
            StructField("expiry_date" , StringType()),
            StructField("order_no" , StringType()),
            StructField("order_date" , StringType()),
            StructField("period" , StringType()),
            StructField("sellout_sub_channel" , StringType()),
            StructField("group_account" , StringType()),
            StructField("account" , StringType()),
            StructField("name_st_or_ddp" , StringType()),
            StructField("zone_or_area" , StringType()),
            StructField("franchise" , StringType()),
            StructField("sellout_category" , StringType()),
            StructField("sub_cat" , StringType()),
            StructField("sub_brand" , StringType()),
            StructField("barcode" , StringType()),
            StructField("base_or_bundle" , StringType()),
            StructField("size" , StringType()),
            StructField("key_chain" , StringType()),
            StructField("status" , StringType())
            ])
        # Set the current session schema
        
        session.use_schema(stage_name.split(''.'')[0])

        
        sheetlist=[''DKSH_Sell_In_20231115''] # HERE need to change to sheetname
        
        for sheet in sheetlist:
            stage_path="@{0}/{1}/{2}.csv".format(stage_name,temp_stage_path,sheet)
            print(stage_path)
            
            temp_df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", "\\u0001")\\
            .option("encoding","UTF-8")\\
            .option("field_optionally_enclosed_by", "\\"") \\
            .csv(stage_path)
            #print(temp_df.show(10))
            
            temp_df=temp_df.na.drop("all")
            if temp_df.count()==0:
                return "No Data in file"

            
            temp_df=temp_df.withColumn("filename",lit(file_name))
            temp_df = temp_df.withColumn("run_id",lit(lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S"))))
            temp_df = temp_df.withColumn("crtd_dttm",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
            #print(temp_df.show(10))
            
        
                    
        final_df=temp_df.select("supplier_code","supplier_name","plant","productid","product","brand",\\
                          "sellin_category","product_group","product_sub_group","unit_of_measurement",\\
                          "custcode","customer","address","district","province","region","zone",\\
                          "channel","sellin_sub_channel","cust_group","billing_no","invoice_date",\\
                          "qty_include_foc","qty_exclude_foc","foc","net_price_wo_vat","tax",\\
                          "net_amount_wo_vat","net_amount_w_vat","gross_amount_wo_vat","gross_amount_w_vat",\\
                          "list_price_wo_vat","vendor_lot","order_type","red_invoice_no","expiry_date",\\
                          "order_no","order_date","period","sellout_sub_channel","group_account","account",\\
                          "name_st_or_ddp","zone_or_area","franchise","sellout_category","sub_cat",\\
                          "sub_brand","barcode","base_or_bundle","size","key_chain","status","filename","run_id","crtd_dttm")
                        


        file_name=file_name+''_''+datetime.now().strftime("%Y%m%d%H%M%S")
        
        # write to target table
        final_df.write.mode("append").saveAsTable(target_table)
        
        #move to success
  
        final_df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)


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
