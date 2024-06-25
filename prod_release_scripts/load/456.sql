use schema meta_raw;
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (14809,1281,'coupang_ppm_file _group','trigger_mail','Y',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (14810,1281,'coupang_ppm_file _group','business_mail_trigger','Y',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (14811,1281,'coupang_ppm_file _group','val_file_header','날짜|Product ID|바코드|SKU ID|SKU 명|벤더아이템 ID|벤더아이템명|로켓프레시|상품카테고리|하위카테고리|세부카테고리|브랜드|매출액(GMV)|판매수량(Units Sold)|반품수량(Return Units)|매입원가(COGS)|AMV|쿠폰 할인가|즉시 할인가|평균판매금액(ASP)|주문건수|주문 고객 수|객단가|구매전환율|PV|정기배송 매출액(SnS GMV)|정기배송 매입원가(SnS COGS)|정기배송 비중(SnS %)|정기배송 판매수량(SnS Units Sold)|정기배송 반품수량(Return Units)|상품평 수|평균 상품 평점',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (14812,1281,'coupang_ppm_file _group','file_spec','daily_performance',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (14813,1281,'coupang_ppm_file _group','val_file_name','daily_performance',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (14814,1281,'coupang_ppm_file _group','val_file_extn','csv',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (14815,1281,'coupang_ppm_file _group','load_method','sp',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (14816,1281,'coupang_ppm_file _group','sp_name','NTASDL_RAW.coupang_ppm_file_preprocessing',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (14817,1281,'coupang_ppm_file _group','sheet_index','0',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (14818,1281,'coupang_ppm_file _group','folder_path','coupang_ppm/transaction/',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (14819,1281,'coupang_ppm_file _group','target_table','sdl_kr_coupang_productsalereport',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (14820,1281,'coupang_ppm_file _group','container','kor',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (14821,1281,'coupang_ppm_file _group','target_schema','NTASDL_RAW',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (14822,1281,'coupang_ppm_file _group','validation','1-1-0',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (14823,1281,'coupang_ppm_file _group','index','last',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (14824,1281,'coupang_ppm_file _group','source_extn','csv',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (14825,1281,'coupang_ppm_file _group','file_header_row_num','0',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (14826,1281,'coupang_ppm_file _group','is_truncate','Y',FALSE,TRUE);

INSERT INTO PROCESS VALUES (1281,1281,'coupang_ppm_file',1281,1,1,FALSE,TRUE,283,1,1,NULL,'','','','NTASDL_RAW.PROD_LOAD_STAGE_ADLS_KOR','','','');
INSERT INTO USECASE (USECASE_ID, USECASE_NAME,CATEGORY,USECASE_DESCRIPTION,IS_ACTIVE,SEQUENCE_ID) VALUES (283,'coupang_ppm_file','coupang_ppm_file','coupang_ppm_file_desc','TRUE',1);
INSERT INTO USECASE (USECASE_ID, USECASE_NAME,CATEGORY,USECASE_DESCRIPTION,IS_ACTIVE,SEQUENCE_ID) VALUES (292,'KR_Trade_Promotion','KR_TRADE_PROMOTION','KR_Trade_Promotion_DESC','TRUE',1);


CREATE OR REPLACE PROCEDURE PHLSDL_RAW.PH_DMS_SELLOUT_STOCK_FACT_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS 'from snowflake.snowpark.functions import col, lit, date_format, current_timestamp, to_date, year, month, concat, format_number, regexp_replace,to_timestamp,when,trim,upper
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DecimalType
import snowflake.snowpark
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz
import pandas as pd

def main(session: snowpark.Session,Param):
    # Param=["OUT_CON_I_PH_20240331224500_20240401014431.csv","PHLSDL_RAW.DEV_LOAD_STAGE_ADLS","dev/ap_ph_metadata/dms/inventory","sdl_ph_dms_sellout_stock_fact"]
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        target_table    = Param[3]
        
        df_schema = StructType([
                StructField("dstrbtr_grp_cd",StringType(), True),
                StructField("cntry_cd",StringType(), True),
                StructField("wh_cd",StringType(), True),
                StructField("invoice_dt",StringType(), True),
                StructField("dstrbtr_prod_id",StringType(), True),
                StructField("qty",StringType(), True),
                StructField("uom",StringType(), True),
                StructField("amt",StringType(), True)
            ])
        
        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", ",")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

       
        
        df=df.na.drop("all")
        if df.count()==0 :
            return "No Data in file"

        
        df = df.with_column("cdl_dttm", lit(file_name.split("_")[-1].split(".")[0]))
        file_name1=file_name.split(".")[0]+"_"+ datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")+".csv"
        # df = df.with_column("file_name", lit(file_name1))
        df = df.with_column("curr_date", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        df = df.withColumn("amt", regexp_replace(col("amt"), "[^0-9]", ""))

        snowdf= df.select(
                    "dstrbtr_grp_cd",
                    "cntry_cd",
                    "wh_cd",
                    "invoice_dt",
                    "dstrbtr_prod_id",
                    "qty",
                    "uom",
                    "amt",
                    "cdl_dttm",
                    "curr_date"
                    )
        file_name=file_name.split(".")[0]+"_"+datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        
        #move to success
        snowdf.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/"+"processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,header=True,OVERWRITE=True)
        
        snowdf.write.mode("append").saveAsTable(stage_name.split(".")[0] + "." + target_table)
        
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
