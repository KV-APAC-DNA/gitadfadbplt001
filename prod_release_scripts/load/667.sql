update meta_raw.parameters set PARAMETER_VALUE ='SELECT 
	Id,
	Username,
	Name,
	CompanyName,
	Division,
	Department,
	TITLE,
	Country,
	Email,
	PHONE,
	MobilePhone,
	ALIAS,
	CommunityNickname,
	IsActive,
	TIMEZONESIDKEY,
	USERROLEID,
	RECEIVESINFOEMAILS,
	ProfileId,
	EmployeeNumber,
	ManagerId,
	LASTLOGINDATE,
	CREATEDDATE,
	CREATEDBYID,
	LastModifiedDate,
	LastModifiedById,
	FEDERATIONIDENTIFIER,
	LAST_IPAD_SYNC_VOD__C,
	JJ_Core_Country_Code__c,
	JJ_CORE_WWID__C,
	JJ_SIMP_REGION__C,
	JJ_SIMP_PROFILE_GROUP_AP__C,
	JJ_Core_User_License__c,
	JJ_MSL_Primary_Responsible_TA__c,
	JJ_MSL_Secondary_Responsible_TA__c,
	Lastname,
	Firstname,
	City,
	State,
	Postalcode,
	Usertype,
	Languagelocalekey,
	Last_Mobile_Sync_vod__c,
	User_Type_vod__c,
	Country_Code_vod__c,
	JJ_SHC_User_Franchise__c
FROM
    User
WHERE 
	LastModifiedDate >= 1900-01-01T00:00:00Z 
	and JJ_CORE_COUNTRY_CODE__C in (''ID'',''TH'',''PH'') and CompanyName not in (''J&J India'')' where PARAMETER_ID in(18349);
	
	
CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.HCPOSESDL_RAW.HCP_OSEA_SALESFORCE_LOAD_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '
from snowflake.snowpark.functions import col, lit, date_format, current_timestamp, to_date, year, month, concat, format_number, regexp_replace,to_timestamp,when,trim,upper
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DecimalType,DateType
from datetime import datetime
import snowflake.snowpark as snowpark
import pytz


def main(session: snowpark.Session,Param):
    try:
        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        sch_name        = stage_name.split(''.'')[0]
        target_table    = sch_name + "." + Param[3]
        df = session.read\\
            .option("INFER_SCHEMA", True)\\
            .option("skip_header",0)\\
            .option("field_delimiter", "\\u0001")\\
            .option("record_delimiter", "\\r\\n")\\
            .option("field_optionally_enclosed_by", None) \\
            .option("trim_space", True) \\
            .with_metadata("METADATA$FILE_ROW_NUMBER") \\
            .csv("@"+stage_name+"/"+temp_stage_path+"/"+file_name)

        df = df.filter(col("METADATA$FILE_ROW_NUMBER") > lit(1))
        
        df = df.drop("METADATA$FILE_ROW_NUMBER")
        
        if df.count()==0:
            return "No Data in file"

        df = df.withColumn("CRT_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
        df = df.withColumn("FILE_NAME",lit(file_name).cast("string"))
        df = df.withColumn("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))

        if target_table == "HCPOSESDL_RAW.SDL_HCP_OSEA_USER":
            df = df.withColumn("ADDRESS", lit(None).cast("string"))
            cols = df.columns[:8]
            cols += ["ADDRESS"]
            cols += df.columns[8:-1]
            df = df.select(*cols)
    
        final_df = df

        # Load Data to the target table
        final_df.write.mode("append").saveAsTable(target_table)

        current_date = datetime.now()
        formatted_year = current_date.strftime("%Y")
        formatted_month = current_date.strftime("%m")

        # write to success folder
    
        file_name=file_name.split(".")[0] + ''_'' + datetime.now().strftime("%Y%m%d%H%M%S")
        final_df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/processed/success/"+formatted_year+"/"+formatted_month+"/"+file_name,file_format_type="csv",OVERWRITE=True,header=True)
        
        return "Success"
        
    except KeyError as key_error:
        
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message
        
    except Exception as e:
        
        error_message = f"Error: {str(e)}" +str(df.columns)
        return error_message

';
