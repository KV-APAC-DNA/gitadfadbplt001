update meta_raw.parameters
set PARAMETER_VALUE = 'csv'
where PARAMETER_GROUP_ID in (373,374,375) and  parameter_name like '%extn%';

CREATE OR REPLACE PROCEDURE PCFSDL_RAW.J_PAC_PHARM_OUTLET_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS 'from snowflake.snowpark.functions import col, lit, date_format, current_timestamp, to_date, year, month, concat, format_number, regexp_replace, to_timestamp, when, trim, upper
from snowflake.snowpark.types import StringType, StructType, StructField
import snowflake.snowpark
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz

def main(session: snowpark.Session,Param):
    #Param=["Outlet_20240323_20240328213034.csv.gz","PCFSDL_RAW.DEV_LOAD_STAGE_ADLS","dev/IQVIA/transaction/Weekly_Data","sdl_pharm_sellout_outlet"]
    try:
        file_name = Param[0]
        stage_name = Param[1]
        temp_stage_path = Param[2]
        target_table = Param[3]
        
        df_schema = StructType([
                StructField("WeekEndingDate", StringType(), True),
                StructField("OutletNumber", StringType(), True),
                StructField("Name", StringType(), True),
                StructField("Address1", StringType(), True),
                StructField("Address2", StringType(), True),
                StructField("Suburb", StringType(), True),
                StructField("State", StringType(), True),
                StructField("Postcode", StringType(), True),
                StructField("BannerGroupCode", StringType(), True),
                StructField("BannerGroupDescription", StringType(), True),
                StructField("EntityTypeCode", StringType(), True),
                StructField("EntityTypeDescription", StringType(), True),
                StructField("OutletTypeDescription", StringType(), True),
                StructField("Status", StringType(), True),
                StructField("BRICK", StringType(), True),
                StructField("ActualRetailBRICK", StringType(), True),
                StructField("ActualOutlet", StringType(), True),
            ])
        
        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", ",")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
        
        df = df.na.drop("all")
        
        if df.count() == 0:
            return "No Data in file"

        df_dup1 = df.select(''WeekEndingDate'', ''OutletNumber'', ''ActualOutlet'')
            
        df_dup2 = df_dup1.dropDuplicates()
        
        if df_dup1.count() != df_dup2.count():
            return "Duplicate values" 
            
        file_name1 = file_name.split(".")[0] + ".csv"
        df = df.withColumn("filename", lit(file_name1))
        df = df.withColumn("crt_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))

        snowdf = df.select(
            ''WeekEndingDate'', ''OutletNumber'', ''Name'', ''Address1'', ''Address2'', ''Suburb'',
            ''State'', ''Postcode'', ''BannerGroupCode'', ''BannerGroupDescription'', ''EntityTypeCode'',
            ''EntityTypeDescription'', ''OutletTypeDescription'', ''Status'', ''BRICK'', ''ActualRetailBRICK'',
            ''ActualOutlet'', ''filename'', ''crt_dttm''
        )
        
        file_name = file_name.split(".")[0] + "_" + datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        

        
        
        snowdf.write.mode("append").saveAsTable(stage_name.split(".")[0] + "." + target_table)
        
        snowdf.write.copy_into_location("@" + stage_name + "/" + temp_stage_path + "/processed/success/" + formatted_year + "/" + formatted_month + "/" + file_name, header=True, OVERWRITE=True,file_format_name=''file_format_zip'')
        
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

CREATE OR REPLACE PROCEDURE PCFSDL_RAW.J_PAC_PHARM_PROBE_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS 'from snowflake.snowpark.functions import col, lit, date_format, current_timestamp, to_date, year, month, concat, format_number, regexp_replace, to_timestamp, when, trim, upper
from snowflake.snowpark.types import StringType, StructType, StructField
import snowflake.snowpark
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz

def main(session: snowpark.Session,Param):
    #Param=["PROBE_20240323.csv","PCFSDL_RAW.DEV_LOAD_STAGE_ADLS","dev/IQVIA/transaction/Weekly_Data","sdl_pharm_sellout_probe"]
    try:
        file_name = Param[0]
        stage_name = Param[1]
        temp_stage_path = Param[2]
        target_table = Param[3]
        
        df_schema = StructType([
                StructField("WeekEndingDate", StringType(), True),
                StructField("OutletNumber", StringType(), True),
                StructField("PFC", StringType(), True),
                StructField("Units", StringType(), True),
                StructField("Value", StringType(), True)
            ])
        
        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", ",")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)
        
        df = df.na.drop("all")
        
        if df.count() == 0:
            return "No Data in file"

        df_dup1 = df.select(''WeekEndingDate'', ''OutletNumber'', ''PFC'')
            
        df_dup2 = df_dup1.dropDuplicates()
        
        if df_dup1.count() != df_dup2.count():
            return "Duplicate values" 
            
        #df = df.withColumn("run_id", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        file_name1=file_name.split(".")[0]+".csv"
        df = df.withColumn("filename", lit(file_name1))
        df = df.withColumn("crt_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))

        snowdf = df.select(
           ''WeekEndingDate'', ''OutletNumber'', ''PFC'', ''Units'', ''Value'', ''filename'', ''crt_dttm''
        )
        
        file_name=file_name.split(".")[0]+"_"+ datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        
        #move to success
        
        
        snowdf.write.mode("append").saveAsTable(stage_name.split(".")[0] + "." + target_table)
        snowdf.write.copy_into_location("@" + stage_name + "/" + temp_stage_path + "/processed/success/" + formatted_year + "/" + formatted_month + "/" + file_name, header=True, OVERWRITE=True,file_format_name=''file_format_zip'')
        
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


CREATE OR REPLACE PROCEDURE PCFSDL_RAW.J_PAC_PHARM_PRODUCT_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS 'from snowflake.snowpark.functions import col, lit, date_format, current_timestamp, to_date, year, month, concat, format_number, regexp_replace, to_timestamp, when, trim, upper
from snowflake.snowpark.types import StringType, StructType, StructField
import snowflake.snowpark
import pandas as pd
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz

def main(session: snowpark.Session,Param):
    #Param=["Product_20240323_20240328213034.csv.gz","PCFSDL_RAW.DEV_LOAD_STAGE_ADLS","dev/IQVIA/transaction","sdl_pharm_sellout_product"]
    try:
        file_name = Param[0]
        stage_name = Param[1]
        temp_stage_path = Param[2]
        target_table = Param[3]
        
        df_schema = StructType([
                StructField("WeekEndingDate", StringType(), True),
                StructField("PFC", StringType(), True),
                StructField("ProductCode", StringType(), True),
                StructField("PackCode", StringType(), True),
                StructField("Product_Long_Desc", StringType(), True),
                StructField("Pack_Long_Desc", StringType(), True),
                StructField("Org_Abbr", StringType(), True),
                StructField("Org_Long_Name", StringType(), True),
                StructField("APN", StringType(), True),
                StructField("PackSize", StringType(), True),
                StructField("PackStrengthUnits", StringType(), True),
                StructField("PackStrengthMeasure", StringType(), True),
                StructField("PackStrengthAdditional", StringType(), True),
                StructField("PackVolumeUnits", StringType(), True),
                StructField("PackVolumeMeasure", StringType(), True),
                StructField("PackForm", StringType(), True),
                StructField("ProdLaunchDate", StringType(), True),
                StructField("PackLaunchDate", StringType(), True),
                StructField("ATC1Code", StringType(), True),
                StructField("ATC1Desc", StringType(), True),
                StructField("ATC2Code", StringType(), True),
                StructField("ATC2Desc", StringType(), True),
                StructField("ATC3Code", StringType(), True),
                StructField("ATC3Desc", StringType(), True),
                StructField("ATC4Code", StringType(), True),
                StructField("ATC4Desc", StringType(), True),
                StructField("CHSegmentCode", StringType(), True),
                StructField("CHSegmentDesc", StringType(), True),
                StructField("NEC1Code", StringType(), True),
                StructField("NEC1Desc", StringType(), True),
                StructField("NEC2Code", StringType(), True),
                StructField("NEC2Desc", StringType(), True),
                StructField("NEC3Code", StringType(), True),
                StructField("NEC3Desc", StringType(), True),
                StructField("NEC4Code", StringType(), True),
                StructField("NEC4Desc", StringType(), True),
                StructField("Form1Code", StringType(), True),
                StructField("Form1Desc", StringType(), True),
                StructField("Form2Code", StringType(), True),
                StructField("Form2Desc", StringType(), True),
                StructField("Form3Code", StringType(), True),
                StructField("Form3Desc", StringType(), True),
                StructField("ConcentrationMeasure", StringType(), True),
                StructField("ConcentrationUnit", StringType(), True),
                StructField("PrescriptionStatusCode", StringType(), True),
                StructField("PrescriptionStatusDesc", StringType(), True),
                StructField("PoisonScheduleCode", StringType(), True),
                StructField("PoisonScheduleDesc", StringType(), True),
                StructField("EthicalStatus", StringType(), True),
                StructField("PBSFormulary", StringType(), True),
                StructField("PBSStatus", StringType(), True),
                StructField("BrandPricePremium", StringType(), True)
            ])
        
        df = session.read\\
            .schema(df_schema)\\
            .option("skip_header",1)\\
            .option("field_delimiter", ",")\\
            .option("field_optionally_enclosed_by", "\\"")\\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        df=df.na.replace({"":None})      
        df = df.na.drop("all")
        
        if df.count() == 0:
            return "No Data in file"

        df_dup1 = df.select(''WeekEndingDate'', ''PFC'')
            
        df_dup2 = df_dup1.dropDuplicates()
        
        if df_dup1.count() != df_dup2.count():
            return "Duplicate values" 
            
        file_name1 = file_name.split(".")[0] + "_" + datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S") + ".csv"
        df = df.withColumn("filename", lit(file_name1))
        df = df.withColumn("crt_dttm", lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))

        snowdf = df.select(
            ''WeekEndingDate'', ''PFC'', ''ProductCode'', ''PackCode'', ''Product_Long_Desc'', ''Pack_Long_Desc'', ''Org_Abbr'',
            ''Org_Long_Name'', ''APN'', ''PackSize'', ''PackStrengthUnits'', ''PackStrengthMeasure'', ''PackStrengthAdditional'',
            ''PackVolumeUnits'', ''PackVolumeMeasure'', ''PackForm'', ''ProdLaunchDate'', ''PackLaunchDate'', ''ATC1Code'',
            ''ATC1Desc'', ''ATC2Code'', ''ATC2Desc'', ''ATC3Code'', ''ATC3Desc'', ''ATC4Code'', ''ATC4Desc'', ''CHSegmentCode'',
            ''CHSegmentDesc'', ''NEC1Code'', ''NEC1Desc'', ''NEC2Code'', ''NEC2Desc'', ''NEC3Code'', ''NEC3Desc'', ''NEC4Code'',
            ''NEC4Desc'', ''Form1Code'', ''Form1Desc'', ''Form2Code'', ''Form2Desc'', ''Form3Code'', ''Form3Desc'',
            ''ConcentrationMeasure'', ''ConcentrationUnit'', ''PrescriptionStatusCode'', ''PrescriptionStatusDesc'',
            ''PoisonScheduleCode'', ''PoisonScheduleDesc'', ''EthicalStatus'', ''PBSFormulary'', ''PBSStatus'',
            ''BrandPricePremium'', ''filename'', ''crt_dttm''
        )
        
        file_name = file_name.split(".")[0] + "_" + datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")
        
        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")
        
        #move to success
        
        
        snowdf.write.mode("append").saveAsTable(stage_name.split(".")[0] + "." + target_table)
        snowdf.write.copy_into_location("@" + stage_name + "/" + temp_stage_path + "/processed/success/" + formatted_year + "/" + formatted_month + "/" + file_name, header=True, OVERWRITE=True,file_format_name=''file_format_zip'')
        
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
