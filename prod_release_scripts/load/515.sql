create or replace stage HCPOSESDL_RAW.PROD_LOAD_STAGE_ADLS
 storage_integration = PROD_DNA_LOAD_AZURE30_SI
 url = 'azure://dlsadbplt001.blob.core.windows.net/ose/';


CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.HCPOSESDL_RAW.HCP_OSEA_HOLIDAY_LIST_PREPROCESSING("PARAM" ARRAY)
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
            .option("field_optionally_enclosed_by", "\\"") \\
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




CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.HCPOSESDL_RAW.HCP_OSEA_ICONNECTUSERS_PREPROCESSING("PARAM" ARRAY)
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
        target_tables   = Param[3].split('':'')
        sheet_names     = Param[4]

        if sheet_names == "[]":
            files_to_read = [file_name]
        else:
            sheet_names = sheet_names[1:-1].split(",")
            files_to_read = [sh_name.strip("\\"").replace("(", "").replace(")", "").replace(" ","_") + ".csv" for sh_name in sheet_names]

        table_pos = 0
        sheet_pos = 0
        return_str = ""
        for src_file in files_to_read:
            df = session.read\\
                .option("INFER_SCHEMA", True)\\
                .option("skip_header",0)\\
                .option("field_delimiter", "\\u0001")\\
                .option("field_optionally_enclosed_by", "\\"") \\
                .option("trim_space", True) \\
                .with_metadata("METADATA$FILE_ROW_NUMBER") \\
                .csv("@"+stage_name+"/"+temp_stage_path+"/"+src_file)

            df = df.filter(col("METADATA$FILE_ROW_NUMBER") > lit(1))
            
            df = df.drop("METADATA$FILE_ROW_NUMBER")

            df = df.withColumn("CRT_DTTM",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S")))
            df = df.withColumn("FILE_NAME",lit(file_name).cast("string"))
            df = df.withColumn("RUN_ID",lit(datetime.now(pytz.timezone("Asia/Singapore")).strftime("%Y%m%d%H%M%S")))
        
            final_df = df

            if final_df.count()==0:
                return_str = return_str + "No Data in sheet: "+sheet_names[sheet_pos]+"; "
            sheet_pos = sheet_pos + 1
            target_table = sch_name + "." + target_tables[table_pos]
        
            # Load Data to the target table
            final_df.write.mode("append").saveAsTable(target_table)

            if table_pos < len(target_tables) -1:
                table_pos = table_pos + 1

            current_date = datetime.now()
            formatted_year = current_date.strftime("%Y")
            formatted_month = current_date.strftime("%m")

            # write to success folder
        
            src_file=src_file.split(".")[0] + ''_'' + datetime.now().strftime("%Y%m%d%H%M%S")
            final_df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/processed/success/"+formatted_year+"/"+formatted_month+"/"+src_file,file_format_type="csv",OVERWRITE=True,header=True)
   
        if return_str == "":
            return "Success"
        else:
            return return_str
        
    except KeyError as key_error:
        
        error_message = f"KeyError: {str(key_error)}. Ensure all required columns are present in the DataFrame."
        return error_message
        
    except Exception as e:
        
        error_message = f"Error: {str(e)}" +str(df.columns)
        return error_message
';




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




use schema meta_raw;

INSERT INTO USECASE (USECASE_ID, USECASE_NAME,CATEGORY,USECASE_DESCRIPTION,IS_ACTIVE,SEQUENCE_ID) VALUES (306,'HCP_OneSea','HCP_OneSea','One Sea Ingestions','TRUE',1);

INSERT INTO PROCESS VALUES (1465,1465,'Master_File_Data_Holidays_Ingestion',1465,1,1,FALSE,TRUE,306,1,1,NULL,'','','','HCPOSESDL_RAW.PROD_LOAD_STAGE_ADLS','','');
INSERT INTO PROCESS VALUES (1466,1466,'Master_File_Data_iConnectusers_Ingestion',1466,1,1,FALSE,TRUE,306,1,1,NULL,'','','','HCPOSESDL_RAW.PROD_LOAD_STAGE_ADLS','','');
INSERT INTO PROCESS VALUES (1557,1557,'HCP_OSEA_Ingestion_Account_HCO',1557,1,1,TRUE,TRUE,306,5,1,NULL,'','','','HCPOSESDL_RAW.PROD_LOAD_STAGE_ADLS','','');
INSERT INTO PROCESS VALUES (1558,1558,'HCP_OSEA_Ingestion_Account_HCP',1558,1,1,TRUE,TRUE,306,5,1,NULL,'','','','HCPOSESDL_RAW.PROD_LOAD_STAGE_ADLS','','');
INSERT INTO PROCESS VALUES (1559,1559,'HCP_OSEA_Ingestion_Address',1559,1,1,TRUE,TRUE,306,5,1,NULL,'','','','HCPOSESDL_RAW.PROD_LOAD_STAGE_ADLS','','');
INSERT INTO PROCESS VALUES (1560,1560,'HCP_OSEA_Ingestion_RecordType_RG',1560,1,1,FALSE,TRUE,306,5,1,NULL,'','','','HCPOSESDL_RAW.PROD_LOAD_STAGE_ADLS','','');
INSERT INTO PROCESS VALUES (1561,1561,'HCP_OSEA_Ingestion_RecordType',1561,1,1,FALSE,TRUE,306,5,1,NULL,'','','','HCPOSESDL_RAW.PROD_LOAD_STAGE_ADLS','','');
INSERT INTO PROCESS VALUES (1562,1562,'HCP_OSEA_Ingestion_Profile_RG',1562,1,1,FALSE,TRUE,306,5,1,NULL,'','','','HCPOSESDL_RAW.PROD_LOAD_STAGE_ADLS','','');
INSERT INTO PROCESS VALUES (1563,1563,'HCP_OSEA_Ingestion_Profile',1563,1,1,FALSE,TRUE,306,5,1,NULL,'','','','HCPOSESDL_RAW.PROD_LOAD_STAGE_ADLS','','');
INSERT INTO PROCESS VALUES (1564,1564,'HCP_OSEA_Ingestion_Territory_Model',1564,1,1,FALSE,TRUE,306,5,1,NULL,'','','','HCPOSESDL_RAW.PROD_LOAD_STAGE_ADLS','','');
INSERT INTO PROCESS VALUES (1565,1565,'HCP_OSEA_Ingestion_Territory',1565,1,1,FALSE,TRUE,306,5,1,NULL,'','','','HCPOSESDL_RAW.PROD_LOAD_STAGE_ADLS','','');
INSERT INTO PROCESS VALUES (1566,1566,'HCP_OSEA_Ingestion_UserTerritory',1566,1,1,FALSE,TRUE,306,5,1,NULL,'','','','HCPOSESDL_RAW.PROD_LOAD_STAGE_ADLS','','');
INSERT INTO PROCESS VALUES (1567,1567,'HCP_OSEA_Ingestion_User_RG',1567,1,1,FALSE,TRUE,306,5,1,NULL,'','','','HCPOSESDL_RAW.PROD_LOAD_STAGE_ADLS','','');
INSERT INTO PROCESS VALUES (1568,1568,'HCP_OSEA_Ingestion_User',1568,1,1,FALSE,TRUE,306,5,1,NULL,'','','','HCPOSESDL_RAW.PROD_LOAD_STAGE_ADLS','','');
INSERT INTO PROCESS VALUES (1569,1569,'HCP_OSEA_Ingestion_Product',1569,1,1,FALSE,TRUE,306,5,1,NULL,'','','','HCPOSESDL_RAW.PROD_LOAD_STAGE_ADLS','','');
INSERT INTO PROCESS VALUES (1570,1570,'HCP_OSEA_Ingestion_Key_Message',1570,1,1,FALSE,TRUE,306,5,1,NULL,'','','','HCPOSESDL_RAW.PROD_LOAD_STAGE_ADLS','','');
INSERT INTO PROCESS VALUES (1571,1571,'HCP_OSEA_Ingestion_Call_Key_Message',1571,1,1,TRUE,TRUE,306,5,1,NULL,'','','','HCPOSESDL_RAW.PROD_LOAD_STAGE_ADLS','','');
INSERT INTO PROCESS VALUES (1572,1572,'HCP_OSEA_Ingestion_Call',1572,1,1,TRUE,TRUE,306,5,1,NULL,'','','','HCPOSESDL_RAW.PROD_LOAD_STAGE_ADLS','','');
INSERT INTO PROCESS VALUES (1573,1573,'HCP_OSEA_Ingestion_Call_Detail',1573,1,1,TRUE,TRUE,306,5,1,NULL,'','','','HCPOSESDL_RAW.PROD_LOAD_STAGE_ADLS','','');
INSERT INTO PROCESS VALUES (1574,1574,'HCP_OSEA_Ingestion_Call_Discussion',1574,1,1,TRUE,TRUE,306,5,1,NULL,'','','','HCPOSESDL_RAW.PROD_LOAD_STAGE_ADLS','','');
INSERT INTO PROCESS VALUES (1575,1575,'HCP_OSEA_Ingestion_Cycle_Plan',1575,1,1,TRUE,TRUE,306,5,1,NULL,'','','','HCPOSESDL_RAW.PROD_LOAD_STAGE_ADLS','','');
INSERT INTO PROCESS VALUES (1576,1576,'HCP_OSEA_Ingestion_Cycle_Plan_Target',1576,1,1,TRUE,TRUE,306,5,1,NULL,'','','','HCPOSESDL_RAW.PROD_LOAD_STAGE_ADLS','','');
INSERT INTO PROCESS VALUES (1577,1577,'HCP_OSEA_Ingestion_Cycle_Plan_Detail',1577,1,1,TRUE,TRUE,306,5,1,NULL,'','','','HCPOSESDL_RAW.PROD_LOAD_STAGE_ADLS','','');
INSERT INTO PROCESS VALUES (1578,1578,'HCP_OSEA_Ingestion_Time_Off_Territory',1578,1,1,TRUE,TRUE,306,5,1,NULL,'','','','HCPOSESDL_RAW.PROD_LOAD_STAGE_ADLS','','');
INSERT INTO PROCESS VALUES (1579,1579,'HCP_OSEA_Ingestion_Product_Metrics',1579,1,1,TRUE,TRUE,306,5,1,NULL,'','','','HCPOSESDL_RAW.PROD_LOAD_STAGE_ADLS','','');
INSERT INTO PROCESS VALUES (1580,1580,'HCP_OSEA_Ingestion_Coaching_Report',1580,1,1,TRUE,TRUE,306,5,1,NULL,'','','','HCPOSESDL_RAW.PROD_LOAD_STAGE_ADLS','','');
INSERT INTO PROCESS VALUES (1581,1581,'HCP_OSEA_Ingestion_Remote_Meeting',1581,1,1,TRUE,TRUE,306,5,1,NULL,'','','','HCPOSESDL_RAW.PROD_LOAD_STAGE_ADLS','','');


INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18228,1557,'HCP_OSEA_Ingestion_Account_HCO_group','container','ose',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18229,1557,'HCP_OSEA_Ingestion_Account_HCO_group','landing_file_path','salesforce/',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18230,1557,'HCP_OSEA_Ingestion_Account_HCO_group','landing_file_name','OSEA_HCO_ACCOUNT',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18231,1557,'HCP_OSEA_Ingestion_Account_HCO_group','target_table','SDL_HCP_OSEA_ACCOUNT_HCO',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18232,1557,'HCP_OSEA_Ingestion_Account_HCO_group','target_schema','HCPOSESDL_RAW',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18233,1557,'HCP_OSEA_Ingestion_Account_HCO_group','ms_query','SELECT 
	Id,
	IsDeleted,
	Name,
	RecordTypeId,
	Phone,
	Fax,
	Website,
	OwnerId,
	CreatedDate,
	CreatedById,
	LastModifiedDate,
	LastModifiedById,
	ISEXCLUDEDFROMREALIGN,
	IsPersonAccount,
	External_ID_vod__c,
	Territory_vod__c,
	Formatted_Name_vod__c,
	BEDS__C,
	Primary_Parent_vod__c,
	TOTAL_MDS_DOS__C,
	DEPARTMENTS__C,
	JJ_Core_Account_Type__c,
	JJ_Core_Sector__c,
	JJ_CORE_REMARKS__C,
	JJ_SIMP_SFE_APPROVED__C,
	JJ_CORE_Country_Code__c,
	BUSINESS_DESCRIPTION__C,
	JJ_SIMP_HCC_ACCOUNT_APPROVED__C,
	JJ_TW_Customer_Code_2__c,
	JJ_VN_INACTIVE__C,
	TOTAL_PHYSICIANS_ENROLLED__C,
	JJ_AP_KAM_Clinical_Differentiations__c,
	JJ_AP_KAM_General_Differnentiations__c,
	TOTAL_PHARMACISTS__C,
	JJ_AP_KAM_Total_AestheticSurgeons__c,
	JJ_AP_KAM_Total_CardioSurgeons__c,
	JJ_AP_KAM_Total_CardiologyPhysicians__c,
	JJ_AP_KAM_Total_DermaPhysicians__c,
	JJ_AP_KAM_Total_EndoPhysicians__c,
	JJ_AP_KAM_Total_GastroPhysicians__c,
	JJ_AP_KAM_Total_GeneralSurgeons__c,
	JJ_AP_KAM_Total_HaemaPhysicians__c,
	JJ_AP_KAM_Total_InfectiousPhysicians__c,
	JJ_AP_KAM_Total_MedOncoPhysicians__c,
	JJ_AP_KAM_Total_NeurologyPhysicians__c,
	JJ_AP_KAM_Total_OpthalSurgeons__c,
	JJ_AP_KAM_Total_OrthoSurgeons__c,
	JJ_AP_KAM_Total_PsychiatryPhysicians__c,
	JJ_AP_KAM_Total_RheumPhysicians__c,
	JJ_AP_KAM_Total_Surgeons__c,
	JJ_AP_KAM_Total_UrologySurgeons__c,
	JJ_KAM_Minimally_Invasive__c,
	JJ_KAM_ObGyn__c,
	JJ_KAM_Paediatric__c,
	OT_s__c,
	JJ_SIMP_IS_EXTERNAL_ID_NUMBER__C,
	JJ_External_ID__c,
	Specialty_2_vod__c,
	Sub_Specialty__c,
	JJ_SEA_Account_Classification__c ,
	JJ_Email_1__c,
	JJ_Email_2__c
FROM
    Account
WHERE
    IsPersonAccount=false and JJ_CORE_COUNTRY_CODE__C in (''ID'',''TH'')',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18234,1557,'HCP_OSEA_Ingestion_Account_HCO_group','truncate_and_load','Y',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18235,1557,'HCP_OSEA_Ingestion_Account_HCO_group','decide_source','salesforce_db',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18236,1557,'HCP_OSEA_Ingestion_Account_HCO_group','incremental_filter',' AND LastModifiedDate>={WATERMARK_VALUE}',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18237,1557,'HCP_OSEA_Ingestion_Account_HCO_group','load_method','sp',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18238,1557,'HCP_OSEA_Ingestion_Account_HCO_group','sp_name','HCPOSESDL_RAW.HCP_OSEA_SALESFORCE_LOAD_PREPROCESSING',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18239,1557,'HCP_OSEA_Ingestion_Account_HCO_group','next_incremental_value','query:SELECT TO_VARCHAR(DATEADD(DAY, -2, MAX(LAST_MODIFIED_DATE)),''YYYY-MM-DD"T"HH24:MI:SSZ'') AS NEXT_VALUE FROM HCPOSESDL_RAW.SDL_HCP_OSEA_ACCOUNT_HCO;',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18240,1558,'HCP_OSEA_Ingestion_Account_HCP_group','container','ose',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18241,1558,'HCP_OSEA_Ingestion_Account_HCP_group','landing_file_path','salesforce/',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18242,1558,'HCP_OSEA_Ingestion_Account_HCP_group','landing_file_name','OSEA_HCP_ACCOUNT',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18243,1558,'HCP_OSEA_Ingestion_Account_HCP_group','target_table','SDL_HCP_OSEA_ACCOUNT_HCP',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18244,1558,'HCP_OSEA_Ingestion_Account_HCP_group','target_schema','HCPOSESDL_RAW',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18245,1558,'HCP_OSEA_Ingestion_Account_HCP_group','ms_query','SELECT 
	Id,
	IsDeleted,
	Name,
	LastName,
	FirstName,
	SALUTATION,
	RecordTypeId,
	OwnerId,
	CreatedDate,
	CreatedById,
	LastModifiedDate,
	LastModifiedById,
	ISEXCLUDEDFROMREALIGN,
	IsPersonAccount,
	PersonMobilePhone,
	PersonEmail,
	PersonBirthdate,
	PersonHasOptedOutOfEmail,
	PersonDoNotCall,
	External_ID_vod__c,
	Territory_vod__c,
	Specialty_1_vod__c,
	Gender_vod__c,
	PREFERRED_NAME_VOD__C,
	Primary_Parent_vod__c,
	JJ_Core_Account_Type__c,
	JJ_Core_Professional_Type__c,
	JJ_Core_English_Name__c,
	JJ_Core_Remarks__c,
	JJ_Core_Position__c,
	JJ_CORE_DIRECT_LINE__C,
	JJ_CORE_DIRECT_FAX__C,
	JJ_SIMP_SFE_APPROVED__C,
	JJ_CORE_Country_Code__c,
	JJ_SIMP_GO_CLASSIFICATION__C,
	JJ_SIMP_HCC_ACCOUNT_APPROVED__C,
	JJ_IN_Account_Classification__c,
	JJ_TW_Customer_Code_2__c,
	KOL_vod__c,
	JJ_VN_INACTIVE__C,
	JJ_MY_PHYSICIAN_PRESCRIBING_BEHAVIOR__C,
	JJ_MY_PHYSICIAN_BEHAVIORAL_STYLE__C,
	JJ_SIMP_IS_EXTERNAL_ID_NUMBER__C,
	JJ_MY_CUSTOMER_VALUE_SEGMENTATION__C,
	JJ_MY_ABILITY_TO_INFLUENCE_PEERS__C,
	JJ_MY_PRACTICE_SIZE__C,
	JJ_MY_PATIENT_TYPE__C,
	JJ_MY_PATIENT_MEDICAL_CONDITION__C,
	JJ_MY_CUSTOMER_SEGMENTATION__C,
	JJ_MY_YEARS_OF_EXPERIENCE__C,
	JJ_MY_INNOVATION__C,
	JJ_MY_NUMBER_OF_PROCEDURES__C,
	JJ_MY_TYPES_OF_PROCEDURE__C,
	JJ_MY_TOTAL_HIP_REPLACEMENT__C,
	JJ_MY_TOTAL_KNEE_REPLACEMENT__C,
	JJ_MY_SPINE__C,
	JJ_MY_TRAUMA__C,
	JJ_MY_COLLORECTAL__C,
	JJ_MY_HEPATOBILIARY__C,
	JJ_MY_CHOLECYSTECTOMY__C,
	JJ_MY_TOTAL_HYSTERECTOMY__C,
	JJ_MY_MYOMECTOMY__C,
	JJ_MY_C_SECTION__C,
	JJ_MY_NORMAL_DELIVERY__C,
	JJ_MY_CABG__C,
	JJ_MY_VALVE_REPAIR__C,
	JJ_MY_ABDOMINAL__C,
	JJ_MY_BREAST_RECONSTRUCTION__C,
	JJ_MY_ORAL_CRANIAL_MAXILOFACIAL__C,
	JJ_Primary_TA__c,
	JJ_Secondary_TA__c,
	JJ_External_ID__c,
	Specialty_2_vod__c,
	Sub_Specialty__c,
	JJ_SEA_Account_Classification__c ,
	JJ_Email_1__c,
	JJ_Email_2__c
FROM
    Account
WHERE
    IsPersonAccount=true and JJ_CORE_COUNTRY_CODE__C in (''ID'',''TH'')',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18246,1558,'HCP_OSEA_Ingestion_Account_HCP_group','truncate_and_load','Y',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18247,1558,'HCP_OSEA_Ingestion_Account_HCP_group','decide_source','salesforce_db',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18248,1558,'HCP_OSEA_Ingestion_Account_HCP_group','incremental_filter',' AND LastModifiedDate>={WATERMARK_VALUE}',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18249,1558,'HCP_OSEA_Ingestion_Account_HCP_group','load_method','sp',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18250,1558,'HCP_OSEA_Ingestion_Account_HCP_group','sp_name','HCPOSESDL_RAW.HCP_OSEA_SALESFORCE_LOAD_PREPROCESSING',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18256,1559,'HCP_OSEA_Ingestion_Address_group','target_schema','HCPOSESDL_RAW',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18257,1559,'HCP_OSEA_Ingestion_Address_group','ms_query','SELECT 
	Id,
	IsDeleted,
	Name,
	RECORDTYPEID,
	CreatedDate,
	CreatedById,
	LastModifiedDate,
	LastModifiedById,
	Account_vod__c,
	Address_line_2_vod__c,
	City_vod__c,
	External_ID_vod__c,
	PHONE_VOD__C,
	FAX_VOD__C,
	Map_vod__c,
	Primary_vod__c,
	APPT_REQUIRED_VOD__C,
	INACTIVE_VOD__C,
	Country_vod__c,
	Latitude_vod__c,
	Zip_vod__c,
	Brick_vod__c,
	State_vod__c,
	Longitude_vod__c,
	CONTROLLING_ADDRESS_VOD__C,
	JJ_Core_Suburb__c,
	JJ_SIMP_VEEVA_AUTOGEN_ID__C,
	Islocked
FROM
    Address_vod__c
WHERE
    COUNTRY_VOD__C in (''ID'',''TH'')',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18258,1559,'HCP_OSEA_Ingestion_Address_group','truncate_and_load','Y',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18259,1559,'HCP_OSEA_Ingestion_Address_group','decide_source','salesforce_db',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18260,1559,'HCP_OSEA_Ingestion_Address_group','incremental_filter',' AND LastModifiedDate>={WATERMARK_VALUE}',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18261,1559,'HCP_OSEA_Ingestion_Address_group','load_method','sp',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18262,1559,'HCP_OSEA_Ingestion_Address_group','sp_name','HCPOSESDL_RAW.HCP_OSEA_SALESFORCE_LOAD_PREPROCESSING',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18263,1559,'HCP_OSEA_Ingestion_Address_group','next_incremental_value','query:SELECT TO_VARCHAR(DATEADD(DAY, -2, MAX(LAST_MODIFIED_DATE)),''YYYY-MM-DD"T"HH24:MI:SSZ'') AS NEXT_VALUE FROM HCPOSESDL_RAW.SDL_HCP_OSEA_ADDRESS;',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18398,1573,'HCP_OSEA_Ingestion_Call_Detail_group','container','ose',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18399,1573,'HCP_OSEA_Ingestion_Call_Detail_group','landing_file_path','salesforce/',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18400,1573,'HCP_OSEA_Ingestion_Call_Detail_group','landing_file_name','OSEA_CALL_DETAIL',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18401,1573,'HCP_OSEA_Ingestion_Call_Detail_group','target_table','SDL_HCP_OSEA_CALL_DETAIL',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18402,1573,'HCP_OSEA_Ingestion_Call_Detail_group','target_schema','HCPOSESDL_RAW',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18403,1573,'HCP_OSEA_Ingestion_Call_Detail_group','ms_query','SELECT 
	Id,
	ISDELETED,
	NAME,
	CREATEDDATE,
	CREATEDBYID,
	LastModifiedDate,
	LastModifiedById,
	IsLocked,
	Is_Parent_Call_vod__c,
	Call2_vod__c,
	Product_vod__c,
	Detail_Priority_vod__c,
	Type_vod__c,
	Detail_Group_vod__c,
	JJ_Product_ID18__c,
	JJ_Classification__c,
	JJ_MY_Adoption_ladder__c,
	JJ_SIMP_Adoption_Style__c,
	JJ_SIMP_Behavioral_Style__c,
	JJ_SIMP_Market_Share__c,
	JJ_SIMP_Pts_Qtr__c,
	JJ_SIMP_Pts_mth__c,
	JJ_SIMP_Pts_week__c,
	JJ_TW_Adoption_Ladder__c,
	JJ_TW_Adoption_Style__c,
	JJ_TW_Behavioral_Style__c,
	JJ_TW_Market_Share__c
FROM
    Call2_Detail_vod__c
WHERE 
	Call2_vod__r.JJ_SIMP_COUNTRY__C in (''ID'',''TH'')',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18404,1573,'HCP_OSEA_Ingestion_Call_Detail_group','truncate_and_load','Y',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18405,1573,'HCP_OSEA_Ingestion_Call_Detail_group','decide_source','salesforce_db',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18406,1573,'HCP_OSEA_Ingestion_Call_Detail_group','incremental_filter',' AND LastModifiedDate>={WATERMARK_VALUE}',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18407,1573,'HCP_OSEA_Ingestion_Call_Detail_group','load_method','sp',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18408,1573,'HCP_OSEA_Ingestion_Call_Detail_group','sp_name','HCPOSESDL_RAW.HCP_OSEA_SALESFORCE_LOAD_PREPROCESSING',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18409,1573,'HCP_OSEA_Ingestion_Call_Detail_group','next_incremental_value','query:SELECT TO_VARCHAR(DATEADD(DAY, -2, MAX(LAST_MODIFIED_DATE)),''YYYY-MM-DD"T"HH24:MI:SSZ'') AS NEXT_VALUE FROM HCPOSESDL_RAW.SDL_HCP_OSEA_CALL_DETAIL;',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18410,1574,'HCP_OSEA_Ingestion_Call_Discussion_group','container','ose',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18411,1574,'HCP_OSEA_Ingestion_Call_Discussion_group','landing_file_path','salesforce/',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18412,1574,'HCP_OSEA_Ingestion_Call_Discussion_group','landing_file_name','OSEA_CALL_DISCUSSION',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18413,1574,'HCP_OSEA_Ingestion_Call_Discussion_group','target_table','SDL_HCP_OSEA_CALL_DISCUSSION',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18414,1574,'HCP_OSEA_Ingestion_Call_Discussion_group','target_schema','HCPOSESDL_RAW',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18415,1574,'HCP_OSEA_Ingestion_Call_Discussion_group','ms_query','SELECT 
	ID,
	ISDELETED,
	NAME,
	RECORDTYPEID,
	CREATEDDATE,
	CREATEDBYID,
	LASTMODIFIEDDATE,
	LASTMODIFIEDBYID,
	ACCOUNT_VOD__C,
	CALL2_VOD__C,
	COMMENTS__C,
	PRODUCT_VOD__C,
	DISCUSSION_TOPICS__C,
	MEDICAL_EVENT_VOD__C,
	IS_PARENT_CALL_VOD__C,
	JJ_CORE_DISCUSSION_TYPE__C,
	JJ_MY_CALL_TYPE__C,
	JJ_MY_EFFECTIVENESS__C,
	JJ_MY_FOLLOW_UP_ACTIVITY__C,
	JJ_MY_OUTCOMES__C,
	JJ_MY_FOLLOW_UP_ADDITIONAL_INFO__C,
	JJ_MY_FOLLOW_UP_DATE__C,
	JJ_MY_MATERIALS_USED__C,
	Call_date_vod__c,
	Detail_group_vod__c,
	User_vod__c
FROM
    Call2_Discussion_vod__c
WHERE 
	Call2_vod__r.JJ_SIMP_COUNTRY__C in (''ID'',''TH'')',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18416,1574,'HCP_OSEA_Ingestion_Call_Discussion_group','truncate_and_load','Y',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18417,1574,'HCP_OSEA_Ingestion_Call_Discussion_group','decide_source','salesforce_db',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18418,1574,'HCP_OSEA_Ingestion_Call_Discussion_group','incremental_filter',' AND LastModifiedDate>={WATERMARK_VALUE}',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18419,1574,'HCP_OSEA_Ingestion_Call_Discussion_group','load_method','sp',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18420,1574,'HCP_OSEA_Ingestion_Call_Discussion_group','sp_name','HCPOSESDL_RAW.HCP_OSEA_SALESFORCE_LOAD_PREPROCESSING',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18421,1574,'HCP_OSEA_Ingestion_Call_Discussion_group','next_incremental_value','query:SELECT TO_VARCHAR(DATEADD(DAY, -2, MAX(LAST_MODIFIED_DATE)),''YYYY-MM-DD"T"HH24:MI:SSZ'') AS NEXT_VALUE FROM HCPOSESDL_RAW.SDL_HCP_OSEA_CALL_DISCUSSION;',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18386,1572,'HCP_OSEA_Ingestion_Call_group','container','ose',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18387,1572,'HCP_OSEA_Ingestion_Call_group','landing_file_path','salesforce/',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18388,1572,'HCP_OSEA_Ingestion_Call_group','landing_file_name','OSEA_CALL',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18389,1572,'HCP_OSEA_Ingestion_Call_group','target_table','SDL_HCP_OSEA_CALL',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18390,1572,'HCP_OSEA_Ingestion_Call_group','target_schema','HCPOSESDL_RAW',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18391,1572,'HCP_OSEA_Ingestion_Call_group','ms_query','SELECT 
	ID,
	OWNERID,
	ISDELETED,
	NAME,
	RECORDTYPEID,
	CREATEDDATE,
	CREATEDBYID,
	LASTMODIFIEDDATE,
	LASTMODIFIEDBYID,
	LASTACTIVITYDATE,
	IsLocked,
	CALL_COMMENTS_VOD__C,
	SAMPLE_CARD_VOD__C,
	ACCOUNT_VOD__C,
	STATUS_VOD__C,
	Parent_Address_vod__c,
	ACCOUNT_PLAN_VOD__C,
	NEXT_CALL_NOTES_VOD__C,
	PRE_CALL_NOTES_VOD__C,
	MOBILE_ID_VOD__C,
	SIGNIFICANT_EVENT__C,
	LOCATION_VOD__C,
	Subject_vod__c,
	CALL_DATETIME_VOD__C,
	Disbursed_To_vod__c,
	SIGNATURE_DATE_VOD__C,
	SIGNATURE_VOD__C,
	TERRITORY_VOD__C,
	SUBMITTED_BY_MOBILE_VOD__C,
	CALL_TYPE_VOD__C,
	Address_vod__c,
	ATTENDEES_VOD__C,
	ATTENDEE_TYPE_VOD__C,
	CALL_DATE_VOD__C,
	DETAILED_PRODUCTS_VOD__C,
	No_Disbursement_vod__c,
	PARENT_CALL_VOD__C,
	USER_VOD__C,
	MEDICAL_EVENT_VOD__C,
	Mobile_Created_Datetime_vod__c,
	MOBILE_LAST_MODIFIED_DATETIME_VOD__C,
	IS_PARENT_CALL_VOD__C,
	Last_PRODice_vod__c,
	CLM_VOD__C,
	IS_SAMPLED_CALL_VOD__C,
	PRESENTATIONS_VOD__C,
	Duration_vod__c,
	Allowed_Products_vod__c,
	Address_Line_1_vod__c,
	Address_Line_2_vod__c,
	City_vod__c,
	State_vod__c,
	PRODUCT_PRIORITY_1_VOD__C,
	PRODUCT_PRIORITY_2_VOD__C,
	PRODUCT_PRIORITY_3_VOD__C,
	PRODUCT_PRIORITY_4_VOD__C,
	PRODUCT_PRIORITY_5_VOD__C,
	ATTENDEE_LIST_VOD__C,
	Signature_Timestamp_vod__c,
	Total_Expense_Attendees_Count_vod__c,
	Location_Services_Status_vod__c,
	Signature_Location_Latitude_vod__c,
	Signature_Location_Longitude_vod__c,
	JJ_MSL_Interaction_Notes__c,
	JJ_SIMP_Call_Type__c,
	Signature_on_Sync_vod__c,
	JJ_Call_Duration__c,
	JJ_Interaction_Mode__c,
	JJ_HCP_KOL_Initiated__c,
	JJ_MSL_Interaction_Type__c,
	CLM_Location_Latitude_vod__c,
	CLM_Location_Longitude_vod__c,
	CLM_Location_Services_Status_vod__c,
	Parent_Call_Mobile_ID_vod__c,
	Submit_Location_Latitude_vod__c ,
	Submit_Location_Longitude_vod__c,
	Submit_Location_Services_Status_vod__c,
	Submit_Timestamp_vod__c,
	JJ_IN_Manager_Insights__c,
	JJ_VN_Hours__c,
	JJ_VN_In_OR_OT__c,
	JJ_VN_MD_D_Call_Type__c,
	JJ_VN_Minutes__c,
	EM_Event_vod__c,
	Medical_Inquiry_vod__c,
	Parent_Address_Id_vod__c,
	Suggestion_vod__c,
	JJ_MY_CALL_OBJECTIVE__C,
	zvod_ACCOUNT_JJ_KAM_Account__c,
	JJ_MY_SUBMISSION_DELAY__C,
	Number_of_days_to_close_calls_KPI__c,
	JJ_SIMP_COUNTRY__C,
	JJ_SIMP_REGION__C,
	Child_Account_Id_vod__c,
	Child_Account_vod__c,
	Location_Id_vod__c,
	Location_Name_vod__c,
	HSP_ADMIN__C,
	JJ_HSP_MINUTES__C,
	JJ_ORTHO_ON_CALL_CASE__C,
	JJ_ORTHO_VOLUNTEER_CASE__C,
	JJ_SIMP_CALC1__C,
	JJ_SIMP_CALCULATE_NON_CASE_TIME__C,
	JJ_SIMP_CALCULATED_HOURS_FIELD__C,
	JJ_SIMP_CASEDEPLOYMENT__C,
	JJ_SIMP_CASE_COVERAGE_12_HOURS__C,
	JJ_SIMP_CASE_PRODUCT_DISCUSSION__C,
	JJ_SIMP_CONCURRENT_CALL__C,
	JJ_SIMP_COURTESY_CALL__C,
	JJ_SIMP_IN_SERVICE__C,
	JJ_SIMP_KOL_COURSE_DISCUSSION__C,
	JJ_SIMP_KOL_MINUTES__C,
	JJ_SIMP_OTHER_ACTIVITIES_TIME_12_HOURS__C,
	JJ_SIMP_OTHER_IN_FIELD_ACTIVITIES__C,
	JJ_SIMP_OVERSEAS_WORKSHOP_VISIT__C,
	JJ_SIMP_RA_ACTIVITIES2__C,
	JJ_SIMP_SALES_ACTIVITY__C,
	JJ_SIMP_SALES_TIME_12_HOURS__C,
	JJ_SIMP_TIME_SPENT__C,
	JJ_SIMP_TIME_SPENT_ON_OTHER_ACTIVITIES__C,
	JJ_SIMP_TIME_SPENT_ON_SALES_ACTIVITY__C,
	JJ_SIMP_TIME_SPENT_ON_A_CALL__C,
	JJ_SIMP_CASE_TYPE__C,
	JJ_SETS_ACTIVITIES__C,
	JJ_TIME_SPENT_ON_CASE__C,
	JJ_TIME_SPENT_ON_OTHER_ACTIVITIES__C,
	JJ_TIME_SPENT_PER_CALL__C,
	JJ_SIMP_CASE_CONDUCTED_IN_HOSPITAL__C,
	JJ_SIMPCALCULATED_FIELD_2_DO_NOT_DISPLAY__C,
	JJ_SIMP_CALCULATED_HOURS_3__C,
	JJ_SIMP_CALL_PLANNED__C,
	JJ_SIMP_CALL_SUBMISSION_DAY__C,
	Check_In_Latitude_vod__c,
	Check_In_Location_Services_Status_vod__c,
	Check_In_Longitude_vod__c,
	Check_In_Status_vod__c,
	Check_In_Timestamp_vod__c,
	Medical_Discussions_vod__c,
	JJ_SIMP_CASE_COVERAGE__C,
	JJ_TW_Call_Duration_Mins_in_Number__c,
	JJ_SIMP_DAY_OF_WEEK__C,
	Veeva_Remote_Meeting_Id_vod__c,
	Signature_Page_Display_Name_vod__c,
	Disbursement_Order_Created_vod__c,
	JJ_Number_of_Detailing__c,
	JJ_Joined_by_Manager__c,
	JJ_Pre_Engagement_Coaching__c,
	JJ_Preparation_Time__c,
	JJ_Travel_Time__c,
	JJ_Rep_Manager__c,
	JJ_Therapeutic_Area__c,
	JJ_NumberOf_Key_Message__c,
	JJ_Account_Specialty__c,
	Owner_Company_Name__c,
	JJ_Call_SubmittedBy__c,
	JJ_Call_Submitted_Date_Time__c,
	JJ_Rep_Division__c,
	JJ_Rep_Department__c,
	JJ_VN_Productivity_Call__c,
	JJ_Account_External_ID__c,
	JJ_SG_Call_Objectives__c,
	JJ_SG_Cost_of_Procedures__c,
	JJ_SG_Others__c,
	JJ_SG_Speciality__c,
	Location_Text_vod__c,
	JJ_IN_Signed_By__c,
	Any_AEPQC_SS_I_have_reported_within_24h__c,
	JJ_SG_End_Datetime__c,
	Remote_Meeting_vod__c,
	Call_channel_vod__c,
	Jj_fml_simp_case_product_discussion_c__c,
	Jj_fml_simp_in_service__c,
	Jj_fml_simp_overseas_workshop_visit__c,
	Signature_captured_remotely_vod__c,
	JJ_Virtual_Channel_Option__c,
	JJ_THMD_Call_Objectives__c,
	JJ_THMD_Case_Type__c,
	JJ_PHMD_Call_Objective__c,
	JJ_IDMD_Call_Objectives__c,
	JJ_GSG_Case_Type__c,
	JJ_SIS_Case_Type__c
FROM
    Call2_vod__c
WHERE 
	JJ_SIMP_COUNTRY__C in (''ID'',''TH'')',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18392,1572,'HCP_OSEA_Ingestion_Call_group','truncate_and_load','Y',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18393,1572,'HCP_OSEA_Ingestion_Call_group','decide_source','salesforce_db',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18394,1572,'HCP_OSEA_Ingestion_Call_group','incremental_filter',' AND LastModifiedDate>={WATERMARK_VALUE}',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18395,1572,'HCP_OSEA_Ingestion_Call_group','load_method','sp',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18396,1572,'HCP_OSEA_Ingestion_Call_group','sp_name','HCPOSESDL_RAW.HCP_OSEA_SALESFORCE_LOAD_PREPROCESSING',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18397,1572,'HCP_OSEA_Ingestion_Call_group','next_incremental_value','query:SELECT TO_VARCHAR(DATEADD(DAY, -2, MAX(LAST_MODIFIED_DATE)),''YYYY-MM-DD"T"HH24:MI:SSZ'') AS NEXT_VALUE FROM HCPOSESDL_RAW.SDL_HCP_OSEA_CALL;',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18374,1571,'HCP_OSEA_Ingestion_Call_Key_Message_group','container','ose',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18375,1571,'HCP_OSEA_Ingestion_Call_Key_Message_group','landing_file_path','salesforce/',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18376,1571,'HCP_OSEA_Ingestion_Call_Key_Message_group','landing_file_name','OSEA_CALL_KEY_MESSAGE',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18377,1571,'HCP_OSEA_Ingestion_Call_Key_Message_group','target_table','SDL_HCP_OSEA_CALL_KEY_MESSAGE',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18378,1571,'HCP_OSEA_Ingestion_Call_Key_Message_group','target_schema','HCPOSESDL_RAW',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18379,1571,'HCP_OSEA_Ingestion_Call_Key_Message_group','ms_query','SELECT 
	Id,
	IsDeleted,
	Name,
	CreatedDate,
	CreatedById,
	LastModifiedDate,
	LastModifiedById,
	SystemModstamp,
	Account_vod__c,
	Call2_vod__c,
	Reaction_vod__c,
	Product_vod__c,
	Key_Message_vod__c,
	Contact_vod__c,
	Call_Date_vod__c,
	User_vod__c,
	Category_vod__c,
	Vehicle_vod__c,
	Is_Parent_Call_vod__c,
	CLM_ID_vod__c,
	Slide_Version_vod__c,
	Duration_vod__c,
	Presentation_ID_vod__c,
	Start_Time_vod__c,
	Attendee_Type_vod__c,
	Entity_Reference_Id_vod__c,
	Segment_vod__c,
	Display_Order_vod__c,
	Detail_Group_vod__c,
	Clm_Presentation_Name_vod__c,
	Clm_Presentation_Version_vod__c,
	Clm_Presentation_vod__c,
	Key_Message_Name_vod__c,
	Key_Message_Description__c
FROM
    Call2_Key_Message_vod__c
WHERE 
	Key_Message_vod__r.JJ_SIMP_Country_c__c in (''ID'',''PH'',''TH'')',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18380,1571,'HCP_OSEA_Ingestion_Call_Key_Message_group','truncate_and_load','Y',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18381,1571,'HCP_OSEA_Ingestion_Call_Key_Message_group','decide_source','salesforce_db',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18382,1571,'HCP_OSEA_Ingestion_Call_Key_Message_group','incremental_filter',' AND LastModifiedDate>={WATERMARK_VALUE}',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18383,1571,'HCP_OSEA_Ingestion_Call_Key_Message_group','load_method','sp',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18384,1571,'HCP_OSEA_Ingestion_Call_Key_Message_group','sp_name','HCPOSESDL_RAW.HCP_OSEA_SALESFORCE_LOAD_PREPROCESSING',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18385,1571,'HCP_OSEA_Ingestion_Call_Key_Message_group','next_incremental_value','query:SELECT TO_VARCHAR(DATEADD(DAY, -2, MAX(LAST_MODIFIED_DATE)),''YYYY-MM-DD"T"HH24:MI:SSZ'') AS NEXT_VALUE FROM HCPOSESDL_RAW.SDL_HCP_OSEA_CALL_KEY_MESSAGE;',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18482,1580,'HCP_OSEA_Ingestion_Coaching_Report','container','ose',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18483,1580,'HCP_OSEA_Ingestion_Coaching_Report','landing_file_path','salesforce/',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18484,1580,'HCP_OSEA_Ingestion_Coaching_Report','landing_file_name','OSEA_COACHING_REPORT',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18485,1580,'HCP_OSEA_Ingestion_Coaching_Report','target_table','SDL_HCP_OSEA_COACHING_REPORT',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18486,1580,'HCP_OSEA_Ingestion_Coaching_Report','target_schema','HCPOSESDL_RAW',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18487,1580,'HCP_OSEA_Ingestion_Coaching_Report','ms_query','SELECT 
	ID,
	ownerid,
	isdeleted,
	name,
	RECORDTYPEID ,
	CreatedDate,
	createdbyid,
	LastModifiedDate,
	LASTMODIFIEDBYID ,
	systemmodstamp,
	lastactivitydate,
	mayedit,
	IsLocked,
	LastViewedDate,
	LastReferencedDate,
	Mobile_ID_vod__c,
	Manager_vod__c,
	Employee_vod__c,
	Review_Date__c,
	review_period__c,
	Status__c,
	overall_rating__c,
	jj_core_country_code__c,
	jj_core_lock__c,
	jj_core_no_of_visits__c,
	JJ_Employee_Review_and_Acknowledged__c,
	JJ_Employee_Comments__c,
	JJ_SIMP_Manager_Comments_Long__c,
	JJ_SIMP_Objectives__c,
	JJ_SIMP_Rep_Comments_Long__c,
	JJ_SIMP_SG_Overall_Rating__c,
	jj_simp_long_comments__c,
	Knowledge_Strategy_Overall_Rating__c,
	Selling_Skills_Overall_Rating__c,
	JJ_MY_Call_type__c,
	JJ_MY_Location__c,
	JJ_ID_Overall_Rating__c,
	JJ_VN_MD_Overall_Rating_Med__c,
	JJ_Agreed_Next_Steps__c,
	JJ_Coaching_for_Field_Visits__c,
	JJ_Customer_Interactions__c,
	JJ_Date_of_Review_Concluded__c,
	JJ_General_Observations_and_Comments__c,
	JJ_Manager_Feedback_Completed__c,
	JJ_Number_of_Coaching_Days__c,
	JJ_Second_Line_Manager__c,
	JJ_Submission_to_Date__c,
	RelatedCoachingReport__c
FROM
    Coaching_Report_vod__c
WHERE 
	JJ_CORE_COUNTRY_CODE__C in (''ID'',''TH'')',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18488,1580,'HCP_OSEA_Ingestion_Coaching_Report','truncate_and_load','Y',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18489,1580,'HCP_OSEA_Ingestion_Coaching_Report','decide_source','salesforce_db',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18490,1580,'HCP_OSEA_Ingestion_Coaching_Report','incremental_filter',' AND LastModifiedDate>={WATERMARK_VALUE}',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18491,1580,'HCP_OSEA_Ingestion_Coaching_Report','load_method','sp',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18492,1580,'HCP_OSEA_Ingestion_Coaching_Report','sp_name','HCPOSESDL_RAW.HCP_OSEA_SALESFORCE_LOAD_PREPROCESSING',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18493,1580,'HCP_OSEA_Ingestion_Coaching_Report','next_incremental_value','query:SELECT TO_VARCHAR(DATEADD(DAY, -2, MAX(LASTMODIFIEDDATE)),''YYYY-MM-DD"T"HH24:MI:SSZ'') AS NEXT_VALUE FROM HCPOSESDL_RAW.SDL_HCP_OSEA_COACHING_REPORT;',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18446,1577,'HCP_OSEA_Ingestion_Cycle_Plan_Detail_group','container','ose',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18447,1577,'HCP_OSEA_Ingestion_Cycle_Plan_Detail_group','landing_file_path','salesforce/',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18448,1577,'HCP_OSEA_Ingestion_Cycle_Plan_Detail_group','landing_file_name','OSEA_CYCLE_PLAN_DETAIL',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18449,1577,'HCP_OSEA_Ingestion_Cycle_Plan_Detail_group','target_table','SDL_HCP_OSEA_CYCLE_PLAN_DETAIL',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18450,1577,'HCP_OSEA_Ingestion_Cycle_Plan_Detail_group','target_schema','HCPOSESDL_RAW',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18451,1577,'HCP_OSEA_Ingestion_Cycle_Plan_Detail_group','ms_query','SELECT 
	ID,
	ISDELETED,
	NAME,
	CREATEDDATE,
	CREATEDBYID,
	LASTMODIFIEDDATE,
	LASTMODIFIEDBYID,
	ISLOCKED,
	CYCLE_PLAN_TARGET_VOD__C,
	ACTUAL_DETAILS_VOD__C,
	ATTAINMENT_VOD__C,
	PLANNED_DETAILS_VOD__C,
	PRODUCT_VOD__C,
	SCHEDULED_DETAILS_VOD__C,
	TOTAL_ACTUAL_DETAILS_VOD__C,
	TOTAL_ATTAINMENT_VOD__C,
	TOTAL_PLANNED_DETAILS_VOD__C,
	TOTAL_SCHEDULED_DETAILS_VOD__C,
	REMAINING_VOD__C,
	TOTAL_REMAINING_VOD__C,
	ZVOD_PM_JJ_CLASSIFICATION__C,
	JJ_SIMP_CFA_100__C,
	JJ_SIMP_CFA_33__C,
	JJ_SIMP_CFA_66__C,
	zvod_PM_JJ_SIMP_Adoption_Style__c
FROM
    Cycle_Plan_Detail_vod__c',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18452,1577,'HCP_OSEA_Ingestion_Cycle_Plan_Detail_group','truncate_and_load','Y',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18453,1577,'HCP_OSEA_Ingestion_Cycle_Plan_Detail_group','decide_source','salesforce_db',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18423,1575,'HCP_OSEA_Ingestion_Cycle_Plan_group','landing_file_path','salesforce/',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18424,1575,'HCP_OSEA_Ingestion_Cycle_Plan_group','landing_file_name','OSEA_CYCLE_PLAN',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18425,1575,'HCP_OSEA_Ingestion_Cycle_Plan_group','target_table','SDL_HCP_OSEA_CYCLE_PLAN',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18426,1575,'HCP_OSEA_Ingestion_Cycle_Plan_group','target_schema','HCPOSESDL_RAW',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18427,1575,'HCP_OSEA_Ingestion_Cycle_Plan_group','ms_query','SELECT 
	ID,
	OWNERID,
	ISDELETED,
	NAME,
	CREATEDDATE,
	CREATEDBYID,
	LASTMODIFIEDDATE,
	LASTMODIFIEDBYID,
	ISLOCKED,
	ACTIVE_VOD__C,
	ATTAINMENT_VOD__C,
	END_DATE_VOD__C,
	EXTERNAL_ID_VOD__C,
	START_DATE_VOD__C,
	TERRITORY_VOD__C,
	ACTUAL_CALLS_VOD__C,
	PLANNED_CALLS_VOD__C,
	EXPECTED_ATTAINMENT_VOD__C,
	EXPECTED_CALLS_VOD__C,
	ATTAINMENT_DIFFERENCE_VOD__C,
	REMAINING_VOD__C,
	JJ_SIMP_STATUS__C,
	JJ_CORE_MGR_S_EMAIL__C,
	JJ_SIMP_MANAGER__C,
	JJ_SIMP_READY_FOR_APPROVAL__C,
	JJ_SIMP_CLOSE_OUT__C,
	JJ_SIMP_MANAGERNAME_C__C,
	JJ_SIMP_COUNTRY_CODE__C,
	JJ_SIMP_CP_APPROVAL_TIME__C,
	JJ_SIMP_APPROVERNAME__C,
	JJ_SIMP_NUMBER_OF_TARGETS__C,
	JJ_SIMP_NUMBER_OF_CFA_100_TARGETS__C,
	JJ_SIMP_CYCLE_PLAN_ATTAINMENT_CPTARGET__C,
	JJ_SIMP_MID_DATE__C,
	JJ_SIMP_HCP_PRODUCT_ACHIEVED_100__C,
	JJ_SIMP_HCP_PRODUCTS_PLANNED__C,
	JJ_SIMP_CPA_100__C,
	JJ_TOTAL_TARGET_REACHED__C,
	JJ_SIMP_Submitted_By__c,
	JJ_SIMP_Submitted_Date_Time__c
FROM
    Cycle_Plan_vod__c
WHERE 
	JJ_SIMP_COUNTRY_CODE__C in (''ID'',''TH'')',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18428,1575,'HCP_OSEA_Ingestion_Cycle_Plan_group','truncate_and_load','Y',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18429,1575,'HCP_OSEA_Ingestion_Cycle_Plan_group','decide_source','salesforce_db',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18430,1575,'HCP_OSEA_Ingestion_Cycle_Plan_group','incremental_filter',' AND LastModifiedDate>={WATERMARK_VALUE}',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18431,1575,'HCP_OSEA_Ingestion_Cycle_Plan_group','load_method','sp',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18432,1575,'HCP_OSEA_Ingestion_Cycle_Plan_group','sp_name','HCPOSESDL_RAW.HCP_OSEA_SALESFORCE_LOAD_PREPROCESSING',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18433,1575,'HCP_OSEA_Ingestion_Cycle_Plan_group','next_incremental_value','query:SELECT TO_VARCHAR(DATEADD(DAY, -2, MAX(LAST_MODIFIED_DATE)),''YYYY-MM-DD"T"HH24:MI:SSZ'') AS NEXT_VALUE FROM HCPOSESDL_RAW.SDL_HCP_OSEA_CYCLE_PLAN;',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18434,1576,'HCP_OSEA_Ingestion_Cycle_Plan_Target_group','container','ose',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18435,1576,'HCP_OSEA_Ingestion_Cycle_Plan_Target_group','landing_file_path','salesforce/',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18436,1576,'HCP_OSEA_Ingestion_Cycle_Plan_Target_group','landing_file_name','OSEA_CYCLE_PLAN_TARGET',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18437,1576,'HCP_OSEA_Ingestion_Cycle_Plan_Target_group','target_table','SDL_HCP_OSEA_CYCLE_PLAN_TARGET',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18438,1576,'HCP_OSEA_Ingestion_Cycle_Plan_Target_group','target_schema','HCPOSESDL_RAW',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18439,1576,'HCP_OSEA_Ingestion_Cycle_Plan_Target_group','ms_query','SELECT 
	ID,
	ISDELETED,
	NAME,
	CREATEDDATE,
	CREATEDBYID,
	LASTMODIFIEDDATE,
	LASTMODIFIEDBYID,
	ISLOCKED,
	CYCLE_PLAN_VOD__C,
	ACTUAL_CALLS_VOD__C,
	ATTAINMENT_VOD__C,
	CYCLE_PLAN_ACCOUNT_VOD__C,
	ORIGINAL_PLANNED_CALLS_VOD__C,
	PLANNED_CALLS_VOD__C,
	TOTAL_ACTUAL_CALLS_VOD__C,
	TOTAL_ATTAINMENT_VOD__C,
	TOTAL_PLANNED_CALLS_VOD__C,
	EXTERNAL_ID_VOD__C,
	SCHEDULED_CALLS_VOD__C,
	TOTAL_SCHEDULED_CALLS_VOD__C,
	REMAINING_VOD__C,
	TOTAL_REMAINING_VOD__C,
	REMAINING_SCHEDULE_VOD__C,
	TOTAL_REMAINING_SCHEDULE_VOD__C,
	JJ_CORE_PRIMARY_PARENT__C,
	ZVOD_ACCOUNTS_SPECIALTY_1_VOD__C,
	TARGET_ACCOUNT_ID__C,
	JJ_SIMP_CPT_CFA_100__C,
	JJ_SIMP_CPT_CFA_66__C,
	JJ_SIMP_CPT_CFA_33__C,
	JJ_SIMP_NUMBER_OF_CFA_100_DETAILS__C,
	JJ_SIMP_NUMBER_OF_PRODUCT_DETAILS__C,
	JJ_TARGET_REACHED_FLAG__C ,
	JJ_AC_Classification__c
FROM
    Cycle_Plan_Target_vod__c
WHERE 
	Cycle_Plan_vod__r.JJ_SIMP_COUNTRY_CODE__C in (''ID'',''TH'')',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18440,1576,'HCP_OSEA_Ingestion_Cycle_Plan_Target_group','truncate_and_load','Y',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18441,1576,'HCP_OSEA_Ingestion_Cycle_Plan_Target_group','decide_source','salesforce_db',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18442,1576,'HCP_OSEA_Ingestion_Cycle_Plan_Target_group','incremental_filter',' AND LastModifiedDate>={WATERMARK_VALUE}',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18443,1576,'HCP_OSEA_Ingestion_Cycle_Plan_Target_group','load_method','sp',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18444,1576,'HCP_OSEA_Ingestion_Cycle_Plan_Target_group','sp_name','HCPOSESDL_RAW.HCP_OSEA_SALESFORCE_LOAD_PREPROCESSING',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18445,1576,'HCP_OSEA_Ingestion_Cycle_Plan_Target_group','next_incremental_value','query:SELECT TO_VARCHAR(DATEADD(DAY, -2, MAX(LAST_MODIFIED_DATE)),''YYYY-MM-DD"T"HH24:MI:SSZ'') AS NEXT_VALUE FROM HCPOSESDL_RAW.SDL_HCP_OSEA_CYCLE_PLAN_TARGET;',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18364,1570,'HCP_OSEA_Ingestion_Key_Message_group','container','ose',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18365,1570,'HCP_OSEA_Ingestion_Key_Message_group','landing_file_path','salesforce/',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18366,1570,'HCP_OSEA_Ingestion_Key_Message_group','landing_file_name','OSEA_KEY_MESSAGE',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18367,1570,'HCP_OSEA_Ingestion_Key_Message_group','target_table','SDL_HCP_OSEA_KEY_MESSAGE',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18368,1570,'HCP_OSEA_Ingestion_Key_Message_group','target_schema','HCPOSESDL_RAW',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18369,1570,'HCP_OSEA_Ingestion_Key_Message_group','ms_query','SELECT 
	Id,
	OwnerId,
	Name,
	RecordTypeId,
	CreatedDate,
	CreatedById,
	LastModifiedDate,
	LastModifiedById,
	SystemModstamp,
	LastViewedDate,
	LastReferencedDate,
	Description_vod__c,
	Product_vod__c,
	Product_Strategy_vod__c,
	Display_Order_vod__c,
	Active_vod__c,
	Category_vod__c,
	Vehicle_vod__c,
	CLM_ID_vod__c,
	Custom_Reaction_vod__c,
	Slide_Version_vod__c,
	Language_vod__c,
	Media_File_CRC_vod__c,
	Media_File_Name_vod__c,
	Media_File_Size_vod__c,
	Segment_vod__c,
	Detail_Group_vod__c,
	JJ_Core_Content_Approval_ID__c,
	JJ_Core_Content_Expiration_Date__c,
	JJ_SIMP_Country_c__c,
	VExternal_Id_vod__c,
	CDN_Path_vod__c,
	Status_vod__c,
	JJ_AP_CLM_Country__c,
	Is_Shared_Resource_vod__c,
	Shared_Resource_vod__c,
	JJ_AP_Country__c,
	JJ_Functional_Team__c,
	JJ_Janssen_Code__c,
	Vault_Doc_Id_vod__c,
	JJ_Key_Message_Group__c,
	JJ_Key_Message_Sub_Group__c,
	JJ_Purpose__c,
	JJ_Content_Topic__c,
	JJ_Content_Sub_Topic__c
FROM
    Key_Message_vod__c	
WHERE 
	LastModifiedDate>1900-01-01T00:00:00.000Z
	and JJ_SIMP_Country_c__c in (''ID'',''TH'')',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18370,1570,'HCP_OSEA_Ingestion_Key_Message_group','truncate_and_load','Y',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18371,1570,'HCP_OSEA_Ingestion_Key_Message_group','decide_source','salesforce_db',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18372,1570,'HCP_OSEA_Ingestion_Key_Message_group','load_method','sp',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18373,1570,'HCP_OSEA_Ingestion_Key_Message_group','sp_name','HCPOSESDL_RAW.HCP_OSEA_SALESFORCE_LOAD_PREPROCESSING',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18354,1569,'HCP_OSEA_Ingestion_Product_group','container','ose',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18355,1569,'HCP_OSEA_Ingestion_Product_group','landing_file_path','salesforce/',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18356,1569,'HCP_OSEA_Ingestion_Product_group','landing_file_name','OSEA_PRODUCT',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18357,1569,'HCP_OSEA_Ingestion_Product_group','target_table','SDL_HCP_OSEA_PRODUCT',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18358,1569,'HCP_OSEA_Ingestion_Product_group','target_schema','HCPOSESDL_RAW',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18359,1569,'HCP_OSEA_Ingestion_Product_group','ms_query','SELECT 
	ID,
	OWNERID,
	ISDELETED,
	NAME,
	RECORDTYPEID,
	CREATEDDATE,
	CREATEDBYID,
	LASTMODIFIEDDATE,
	LASTMODIFIEDBYID,
	CONSUMER_SITE__C,
	PRODUCT_INFO__C,
	THERAPEUTIC_CLASS_VOD__C,
	PARENT_PRODUCT_VOD__C,
	THERAPEUTIC_AREA_VOD__C,
	PRODUCT_TYPE_VOD__C,
	REQUIRE_KEY_MESSAGE_VOD__C,
	Cost_vod__c,
	EXTERNAL_ID_VOD__C,
	MANUFACTURER_VOD__C,
	COMPANY_PRODUCT_VOD__C,
	CONTROLLED_SUBSTANCE_VOD__C,
	DESCRIPTION_VOD__C,
	SAMPLE_QUANTITY_PICKLIST_VOD__C,
	DISPLAY_ORDER_VOD__C,
	NO_METRICS_VOD__C,
	DISTRIBUTOR_VOD__C,
	SAMPLE_QUANTITY_BOUND_VOD__C,
	SAMPLE_U_M_VOD__C,
	NO_DETAILS_VOD__C,
	Quantity_Per_Case_vod__c,
	RESTRICTED_VOD__C,
	User_Aligned_vod__c,
	Restricted_States_vod__c,
	Sort_Code_vod__c,
	NO_CYCLE_PLANS_VOD__C,
	JJ_CORE_SKU_ID__C,
	JJ_CORE_BUSINESS_UNIT__C,
	JJ_CORE_FRANCHISE__C,
	JJ_SIMP_COUNTRY__C,
	VExternal_Id_vod__c,
	Product_Identifier_vod__c,
	JJ_VN_BIZ_SUB_UNIT__C,
	JJ_VN_BIZ_UNIT__C,
	JJ_SIMP_PRODUCT_SECTOR__C,
	JJ_iMR__c,
	JJ_Detail_Sub_Type__c,
	JJ_SHC_Sector__c,
	JJ_SHC_Strategic_Group__c,
	JJ_SHC_Franchise__c,
	JJ_SHC_Brand__c
FROM
    Product_vod__c	
WHERE 
	LastModifiedDate>1900-01-01T00:00:00.000Z
	and JJ_SIMP_COUNTRY__C in (''ID'',''TH'') and JJ_SIMP_Product_Sector__c not in (''Pharm'')',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18360,1569,'HCP_OSEA_Ingestion_Product_group','truncate_and_load','Y',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18361,1569,'HCP_OSEA_Ingestion_Product_group','decide_source','salesforce_db',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18362,1569,'HCP_OSEA_Ingestion_Product_group','load_method','sp',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18363,1569,'HCP_OSEA_Ingestion_Product_group','sp_name','HCPOSESDL_RAW.HCP_OSEA_SALESFORCE_LOAD_PREPROCESSING',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18470,1579,'HCP_OSEA_Ingestion_Product_Metrics_group','container','ose',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18471,1579,'HCP_OSEA_Ingestion_Product_Metrics_group','landing_file_path','salesforce/',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18472,1579,'HCP_OSEA_Ingestion_Product_Metrics_group','landing_file_name','OSEA_PRODUCT_METRICS',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18473,1579,'HCP_OSEA_Ingestion_Product_Metrics_group','target_table','SDL_HCP_OSEA_PRODUCT_METRICS',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18474,1579,'HCP_OSEA_Ingestion_Product_Metrics_group','target_schema','HCPOSESDL_RAW',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18475,1579,'HCP_OSEA_Ingestion_Product_Metrics_group','ms_query','SELECT 
	ID,
	ISDELETED,
	NAME,
	CREATEDDATE,
	CREATEDBYID,
	LASTMODIFIEDDATE,
	LASTMODIFIEDBYID,
	ACCOUNT_VOD__C,
	AWARENESS__C,
	MOVEMENT__C,
	PRODUCTS_VOD__C,
	SEGMENT__C,
	X12_MO_TRX_CHG__C,
	SPEAKER_SKILLS__C,
	INVESTIGATOR_READINESS__C,
	ENGAGEMENTS__C,
	EXTERNAL_ID_VOD__C,
	DECILE__C,
	ADOPTION_LEVEL__C,
	DETAIL_GROUP_VOD__C,
	JJ_CLASSIFICATION__C,
	JJ_PRESCRIPTIONS__C,
	JJ_SIMP_BELIEVER_OF_ADHERENCE__C,
	JJ_SIMP_INFLUENCE_LEVEL__C,
	JJ_SIMP_INTENTION_FOR_FUTURE_SUST__C,
	JJ_SIMP_LIKELY_TO_INI_SUSTENNA__C,
	JJ_SIMP_LIKELY_TO_SWITCH__C,
	JJ_SIMP_PENETRATION__C,
	JJ_SIMP_POTENTIAL__C,
	JJ_SIMP_PRESCRIBER__C,
	JJ_SIMP_PTS_MTH__C,
	JJ_SIMP_SCHIZOPHRENIA_PTS__C,
	JJ_SIMP_SCIENTIFIC_DATA__C,
	JJ_SIMP_TYPE_OF_SETTING__C,
	JJ_SIMP_USAGOF_SUSTENNA_UPON_DISCHARGING__C,
	JJ_SIMP_USAGE_OF_BRANDED_ATYP__C,
	JJ_SIMP_SG_VELCADE_KOL_EXPERTS__C,
	JJ_SIMP_SG_VELCADE_NBROFMMPTSYR__C,
	JJ_SIMP_SG_VELCADE_PRACTICE_TYPE__C,
	JJ_SIMP_SG_VELCADE_STDGUIDELINE__C,
	JJ_SIMP_PERCEPTION__C,
	JJ_SIMP_PRESCRIPTION_BEHAVIOR__C,
	JJ_SIMP_SCIENTIFICALLY_DRIVEN__C,
	JJ_SIMP_ID_UL_TREATMENT_PATTERN__C,
	JJ_SIMP_PHYSICIAN_BEHAVIOUR__C,
	JJ_SIMP_PRODUCT_PREFERENCE__C,
	JJ_SIMP_SPECIALTY__C,
	JJ_SIMP_COMPANY_LOYALTY__C,
	JJ_SIMP_BIOLOGICS_USER__C,
	JJ_SIMP_PRICE_SENSITIVITY__C,
	JJ_SIMP_USAGE_OF_GENERIC__C,
	JJ_SIMP_PREVIOUS_TRAMADOL_EXPERIENCE__C,
	JJ_SIMP_INTEREST_TO_TREAT_DISEASE_AREA__C,
	JJ_SIMP_SATISFACTION_WITH_ALTERNATIVE_TR__C,
	JJ_SIMP_PATIENT_SHARE__C,
	JJ_CORE_COUNTRY_CODE__C,
	JJ_MY_PHYSICIAN_PRODUCT_PREFERENCE__C,
	JJ_MY_PHYSICIAN_PRESCRIPTION__C,
	JJ_MY_ADOPTION_LADDER__C,
	JJ_MY_RATINGS_POINTS__C,
	JJ_MY_PEER_INFLUENCE__C,
	JJ_MY_INNOVATIONS__C,
	JJ_MY_CASES_LOADS_PER_YEAR__C,
	JJ_MY_SALES_VALUE_PER_YEAR__C,
	JJ_MY_SUPPORT__C,
	JJ_MY_CMD__C,
	JJ_MY_ASP_CLASSIFICATION__C,
	JJ_MY_NO_OF_PRODUCTS_USED__C,
	JJ_UPTRAVI_ORIENTATION__C,
	JJ_UPTRAVI_USAGE__C
FROM
    Product_Metrics_vod__c
WHERE 
	JJ_CORE_COUNTRY_CODE__C in (''ID'',''TH'')',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18476,1579,'HCP_OSEA_Ingestion_Product_Metrics_group','truncate_and_load','Y',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18477,1579,'HCP_OSEA_Ingestion_Product_Metrics_group','decide_source','salesforce_db',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18478,1579,'HCP_OSEA_Ingestion_Product_Metrics_group','incremental_filter',' AND LastModifiedDate>={WATERMARK_VALUE}',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18479,1579,'HCP_OSEA_Ingestion_Product_Metrics_group','load_method','sp',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18480,1579,'HCP_OSEA_Ingestion_Product_Metrics_group','sp_name','HCPOSESDL_RAW.HCP_OSEA_SALESFORCE_LOAD_PREPROCESSING',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18481,1579,'HCP_OSEA_Ingestion_Product_Metrics_group','next_incremental_value','query:SELECT TO_VARCHAR(DATEADD(DAY, -2, MAX(LAST_MODIFIED_DATE)),''YYYY-MM-DD"T"HH24:MI:SSZ'') AS NEXT_VALUE FROM HCPOSESDL_RAW.SDL_HCP_OSEA_PRODUCT_METRICS;',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18294,1563,'HCP_OSEA_Ingestion_Profile_group','container','ose',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18295,1563,'HCP_OSEA_Ingestion_Profile_group','landing_file_path','salesforce/',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18296,1563,'HCP_OSEA_Ingestion_Profile_group','landing_file_name','OSEA_PROFILE',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18297,1563,'HCP_OSEA_Ingestion_Profile_group','target_table','SDL_HCP_OSEA_PROFILE',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18298,1563,'HCP_OSEA_Ingestion_Profile_group','target_schema','HCPOSESDL_RAW',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18299,1563,'HCP_OSEA_Ingestion_Profile_group','ms_query','SELECT 
	ID,
	NAME,
	TYPE,
	USERLICENSEID,
	USERTYPE,
	CREATEDDATE,
	CREATEDBYID,
	LASTMODIFIEDDATE,
	LASTMODIFIEDBYID,
	DESCRIPTION
FROM
    Profile
WHERE 
	LastModifiedDate>1900-01-01T00:00:00.000Z',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18300,1563,'HCP_OSEA_Ingestion_Profile_group','truncate_and_load','Y',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18301,1563,'HCP_OSEA_Ingestion_Profile_group','decide_source','salesforce_db',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18302,1563,'HCP_OSEA_Ingestion_Profile_group','load_method','sp',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18303,1563,'HCP_OSEA_Ingestion_Profile_group','sp_name','HCPOSESDL_RAW.HCP_OSEA_SALESFORCE_LOAD_PREPROCESSING',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18284,1562,'HCP_OSEA_Ingestion_Profile_RG_group','container','ose',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18285,1562,'HCP_OSEA_Ingestion_Profile_RG_group','landing_file_path','salesforce/',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18286,1562,'HCP_OSEA_Ingestion_Profile_RG_group','landing_file_name','OSEA_PROFILE_RG',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18287,1562,'HCP_OSEA_Ingestion_Profile_RG_group','target_table','SDL_HCP_OSEA_PROFILE_RG',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18288,1562,'HCP_OSEA_Ingestion_Profile_RG_group','target_schema','HCPOSESDL_RAW',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18289,1562,'HCP_OSEA_Ingestion_Profile_RG_group','ms_query','SELECT 
	ID,
	NAME,
	USERLICENSEID,
	USERTYPE,
	CREATEDDATE,
	CREATEDBYID,
	LASTMODIFIEDDATE,
	LASTMODIFIEDBYID,
	DESCRIPTION
FROM
    Profile
WHERE 
	LastModifiedDate>=2019-01-01T00:00:00.000Z',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18290,1562,'HCP_OSEA_Ingestion_Profile_RG_group','truncate_and_load','Y',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18291,1562,'HCP_OSEA_Ingestion_Profile_RG_group','decide_source','salesforce_db',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18292,1562,'HCP_OSEA_Ingestion_Profile_RG_group','load_method','sp',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18293,1562,'HCP_OSEA_Ingestion_Profile_RG_group','sp_name','HCPOSESDL_RAW.HCP_OSEA_SALESFORCE_LOAD_PREPROCESSING',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18274,1561,'HCP_OSEA_Ingestion_RecordType_group','container','ose',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18275,1561,'HCP_OSEA_Ingestion_RecordType_group','landing_file_path','salesforce/',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18276,1561,'HCP_OSEA_Ingestion_RecordType_group','landing_file_name','OSEA_RECORDTYPE',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18277,1561,'HCP_OSEA_Ingestion_RecordType_group','target_table','SDL_HCP_OSEA_RECORDTYPE',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18278,1561,'HCP_OSEA_Ingestion_RecordType_group','target_schema','HCPOSESDL_RAW',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18279,1561,'HCP_OSEA_Ingestion_RecordType_group','ms_query','SELECT 
	ID,
	NAME,
	PRODELOPERNAME,
	NAMESPACEPREFIX,
	DESCRIPTION,
	BUSINESSPROCESSID,
	SOBJECTTYPE,
	ISACTIVE,
	ISPERSONTYPE,
	CREATEDBYID,
	CREATEDDATE,
	LASTMODIFIEDBYID,
	LASTMODIFIEDDATE
FROM
    RECORDTYPE
WHERE 
	LastModifiedDate>1900-01-01T00:00:00.000Z',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18280,1561,'HCP_OSEA_Ingestion_RecordType_group','truncate_and_load','Y',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18281,1561,'HCP_OSEA_Ingestion_RecordType_group','decide_source','salesforce_db',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18282,1561,'HCP_OSEA_Ingestion_RecordType_group','load_method','sp',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18283,1561,'HCP_OSEA_Ingestion_RecordType_group','sp_name','HCPOSESDL_RAW.HCP_OSEA_SALESFORCE_LOAD_PREPROCESSING',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18264,1560,'HCP_OSEA_Ingestion_RecordType_RG_group','container','ose',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18265,1560,'HCP_OSEA_Ingestion_RecordType_RG_group','landing_file_path','salesforce/',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18266,1560,'HCP_OSEA_Ingestion_RecordType_RG_group','landing_file_name','OSEA_RECORDTYPE_RG',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18267,1560,'HCP_OSEA_Ingestion_RecordType_RG_group','target_table','SDL_HCP_OSEA_RECORDTYPE_RG',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18268,1560,'HCP_OSEA_Ingestion_RecordType_RG_group','target_schema','HCPOSESDL_RAW',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18269,1560,'HCP_OSEA_Ingestion_RecordType_RG_group','ms_query','SELECT 
	ID,
	NAME,
	PRODELOPERNAME,
	NAMESPACEPREFIX,
	DESCRIPTION,
	BUSINESSPROCESSID,
	SOBJECTTYPE,
	ISACTIVE,
	CREATEDBYID,
	CREATEDDATE,
	LASTMODIFIEDBYID,
	LASTMODIFIEDDATE
FROM
    RECORDTYPE
WHERE
    LastModifiedDate>=2019-01-01T00:00:00.000Z',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18270,1560,'HCP_OSEA_Ingestion_RecordType_RG_group','truncate_and_load','Y',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18271,1560,'HCP_OSEA_Ingestion_RecordType_RG_group','decide_source','salesforce_db',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18272,1560,'HCP_OSEA_Ingestion_RecordType_RG_group','load_method','sp',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18273,1560,'HCP_OSEA_Ingestion_RecordType_RG_group','sp_name','HCPOSESDL_RAW.HCP_OSEA_SALESFORCE_LOAD_PREPROCESSING',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18494,1581,'HCP_OSEA_Ingestion_Remote_Meeting','container','ose',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18495,1581,'HCP_OSEA_Ingestion_Remote_Meeting','landing_file_path','salesforce/',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18496,1581,'HCP_OSEA_Ingestion_Remote_Meeting','landing_file_name','OSEA_REMOTE_MEETING',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18497,1581,'HCP_OSEA_Ingestion_Remote_Meeting','target_table','SDL_HCP_OSEA_REMOTE_MEETING',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18498,1581,'HCP_OSEA_Ingestion_Remote_Meeting','target_schema','HCPOSESDL_RAW',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18499,1581,'HCP_OSEA_Ingestion_Remote_Meeting','ms_query','SELECT 
	Id,
	OwnerId,
	IsDeleted,
	Name,
	RecordTypeId,
	CreatedDate,
	CreatedById,
	LastModifiedDate,
	LastModifiedById,
	SystemModstamp,
	MayEdit,
	IsLocked,
	Meeting_Id_vod__c,
	Meeting_Name_vod__c,
	Mobile_ID_vod__c,
	Scheduled_DateTime_vod__c,
	Scheduled_vod__c,
	Meeting_Password_vod__c,
	Meeting_Outcome_Status_vod__c,
	JJ_Host_Country__c,
	JJ_NumOfAttendee__c,
	JJ_Owner_Country__c,
	Assigned_Host_vod__c,
	Attendance_Report_Process_Status_vod__c,
	Description_vod__c,
	Latest_Meeting_Start_Datetime_vod__c,
	Webinar_Alternative_Host_1_vod__c,
	Webinar_Alternative_Host_2_vod__c,
	Webinar_Alternative_Host_3_vod__c,
	Rating_Submitted_vod__c
FROM
    Remote_Meeting_vod__c
WHERE 
	JJ_Owner_Country__c in (''ID'',''TH'')',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18500,1581,'HCP_OSEA_Ingestion_Remote_Meeting','truncate_and_load','Y',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18501,1581,'HCP_OSEA_Ingestion_Remote_Meeting','decide_source','salesforce_db',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18502,1581,'HCP_OSEA_Ingestion_Remote_Meeting','incremental_filter',' AND LastModifiedDate>={WATERMARK_VALUE}',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18503,1581,'HCP_OSEA_Ingestion_Remote_Meeting','load_method','sp',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18504,1581,'HCP_OSEA_Ingestion_Remote_Meeting','sp_name','HCPOSESDL_RAW.HCP_OSEA_SALESFORCE_LOAD_PREPROCESSING',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18505,1581,'HCP_OSEA_Ingestion_Remote_Meeting','next_incremental_value','query:SELECT TO_VARCHAR(DATEADD(DAY, -2, MAX(LAST_MODIFIED_DATE)),''YYYY-MM-DD"T"HH24:MI:SSZ'') AS NEXT_VALUE FROM HCPOSESDL_RAW.SDL_HCP_OSEA_REMOTE_MEETING;',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18314,1565,'HCP_OSEA_Ingestion_Territory_group','container','ose',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18315,1565,'HCP_OSEA_Ingestion_Territory_group','landing_file_path','salesforce/',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18316,1565,'HCP_OSEA_Ingestion_Territory_group','landing_file_name','OSEA_TERRITORY',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18317,1565,'HCP_OSEA_Ingestion_Territory_group','target_table','SDL_HCP_OSEA_TERRITORY',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18318,1565,'HCP_OSEA_Ingestion_Territory_group','target_schema','HCPOSESDL_RAW',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18319,1565,'HCP_OSEA_Ingestion_Territory_group','ms_query','SELECT 
	Id,
	Name,
	Territory2TypeId,
	Territory2ModelId,
	ParentTerritory2Id,
	Description,
	AccountAccessLevel,
	ContactAccessLevel,
	LastModifiedDate,
	LastModifiedById,
	SystemModstamp,
	PRODeloperName,
	ParentTerritory1Id__c,
	Territory1Id__c
FROM
    Territory2
WHERE 
	LastModifiedDate>1900-01-01T00:00:00.000Z',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18320,1565,'HCP_OSEA_Ingestion_Territory_group','truncate_and_load','Y',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18321,1565,'HCP_OSEA_Ingestion_Territory_group','decide_source','salesforce_db',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18322,1565,'HCP_OSEA_Ingestion_Territory_group','load_method','sp',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18323,1565,'HCP_OSEA_Ingestion_Territory_group','sp_name','HCPOSESDL_RAW.HCP_OSEA_SALESFORCE_LOAD_PREPROCESSING',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18304,1564,'HCP_OSEA_Ingestion_Territory_Model_group','container','ose',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18305,1564,'HCP_OSEA_Ingestion_Territory_Model_group','landing_file_path','salesforce/',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18306,1564,'HCP_OSEA_Ingestion_Territory_Model_group','landing_file_name','OSEA_TERRITORY2MODEL',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18307,1564,'HCP_OSEA_Ingestion_Territory_Model_group','target_table','SDL_HCP_OSEA_TERRITORY_MODEL',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18308,1564,'HCP_OSEA_Ingestion_Territory_Model_group','target_schema','HCPOSESDL_RAW',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18309,1564,'HCP_OSEA_Ingestion_Territory_Model_group','ms_query','SELECT 
	Id,
	IsDeleted,
	Name,
	CreatedDate,
	CreatedById,
	LastModifiedDate,
	LastModifiedById,
	SystemModstamp,
	MayEdit,
	IsLocked,
	Description,
	ActivatedDate,
	DeactivatedDate,
	State,
	PRODeloperName,
	LastRunRulesEndDate,
	IsCloneSource,
	LastOppTerrAssignEndDate
FROM
    Territory2Model
WHERE 
	LastModifiedDate>1900-01-01T00:00:00.000Z
	and State=''Active''',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18310,1564,'HCP_OSEA_Ingestion_Territory_Model_group','truncate_and_load','Y',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18311,1564,'HCP_OSEA_Ingestion_Territory_Model_group','decide_source','salesforce_db',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18312,1564,'HCP_OSEA_Ingestion_Territory_Model_group','load_method','sp',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18313,1564,'HCP_OSEA_Ingestion_Territory_Model_group','sp_name','HCPOSESDL_RAW.HCP_OSEA_SALESFORCE_LOAD_PREPROCESSING',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18458,1578,'HCP_OSEA_Ingestion_Time_Off_Territory_group','container','ose',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18459,1578,'HCP_OSEA_Ingestion_Time_Off_Territory_group','landing_file_path','salesforce/',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18460,1578,'HCP_OSEA_Ingestion_Time_Off_Territory_group','landing_file_name','OSEA_TIME_OFF_TERRITORY',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18461,1578,'HCP_OSEA_Ingestion_Time_Off_Territory_group','target_table','SDL_HCP_OSEA_TIME_OFF_TERRITORY',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18462,1578,'HCP_OSEA_Ingestion_Time_Off_Territory_group','target_schema','HCPOSESDL_RAW',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18463,1578,'HCP_OSEA_Ingestion_Time_Off_Territory_group','ms_query','SELECT 
	Id,
	OWNERID,
	IsDeleted,
	Name,
	RECORDTYPEID,
	CreatedDate,
	CreatedById,
	LastModifiedDate,
	LastModifiedById,
	Reason_vod__c,
	Territory_vod__c,
	Date_vod__c,
	Status_vod__c,
	Time_vod__c,
	Hours_vod__c,
	MOBILE_ID_VOD__C,
	Hours_off_vod__c,
	Start_Time_vod__c,
	JJ_SIMP_TIME_ON_TIME_OFF__C,
	JJ_SIMP_FRML_HOURS_ON__C,
	JJ_SIMP_FRML_TOTAL_WORK_DAYS__C,
	JJ_SIMP_FRML_NON_WORKING_HOURS_OFF__C,
	JJ_SIMP_FRML_PLANNED_WORK_DAYS__C,
	JJ_SIMP_DESCRIPTION__C,
	JJ_SIMP_TIME_ON__C,
	JJ_SIMP_USER__C,
	JJ_SIMP_USER_PROFILE__C,
	JJ_SM_REASON__C,
	JJ_SIMP_CALCULATEDHOURS_OFF__C,
	JJ_SIMP_TOTAL_TIME_OFF__C,
	Jj_approval_status__c,
	Owner_s_Manager_Email_ID__c,
	Country_code__c
FROM
    Time_Off_Territory_vod__c
WHERE 
	Country_Code__c in (''TH'',''ID'')',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18464,1578,'HCP_OSEA_Ingestion_Time_Off_Territory_group','truncate_and_load','Y',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18465,1578,'HCP_OSEA_Ingestion_Time_Off_Territory_group','decide_source','salesforce_db',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18466,1578,'HCP_OSEA_Ingestion_Time_Off_Territory_group','incremental_filter',' AND LastModifiedDate>={WATERMARK_VALUE}',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18467,1578,'HCP_OSEA_Ingestion_Time_Off_Territory_group','load_method','sp',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18468,1578,'HCP_OSEA_Ingestion_Time_Off_Territory_group','sp_name','HCPOSESDL_RAW.HCP_OSEA_SALESFORCE_LOAD_PREPROCESSING',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18469,1578,'HCP_OSEA_Ingestion_Time_Off_Territory_group','next_incremental_value','query:SELECT TO_VARCHAR(DATEADD(DAY, -2, MAX(LAST_MODIFIED_DATE)),''YYYY-MM-DD"T"HH24:MI:SSZ'') AS NEXT_VALUE FROM HCPOSESDL_RAW.SDL_HCP_OSEA_TIME_OFF_TERRITORY;',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18344,1568,'HCP_OSEA_Ingestion_User_group','container','ose',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18345,1568,'HCP_OSEA_Ingestion_User_group','landing_file_path','salesforce/',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18346,1568,'HCP_OSEA_Ingestion_User_group','landing_file_name','OSEA_USER',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18347,1568,'HCP_OSEA_Ingestion_User_group','target_table','SDL_HCP_OSEA_USER',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18348,1568,'HCP_OSEA_Ingestion_User_group','target_schema','HCPOSESDL_RAW',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18349,1568,'HCP_OSEA_Ingestion_User_group','ms_query','SELECT 
	Id,
	Username,
	Name,
	CompanyName,
	Division,
	Department,
	TITLE,
	Country,
	'''' AS ADDRESS,
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
	LastModifiedDate>= {ts''1900-01-01 00:00:00''}
	and JJ_CORE_COUNTRY_CODE__C in (''ID'',''TH'') and CompanyName not in (''J&J India'')',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18350,1568,'HCP_OSEA_Ingestion_User_group','truncate_and_load','Y',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18351,1568,'HCP_OSEA_Ingestion_User_group','decide_source','salesforce_db',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18352,1568,'HCP_OSEA_Ingestion_User_group','load_method','sp',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18353,1568,'HCP_OSEA_Ingestion_User_group','sp_name','HCPOSESDL_RAW.HCP_OSEA_SALESFORCE_LOAD_PREPROCESSING',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18334,1567,'HCP_OSEA_Ingestion_User_RG_group','container','ose',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18335,1567,'HCP_OSEA_Ingestion_User_RG_group','landing_file_path','salesforce/',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18336,1567,'HCP_OSEA_Ingestion_User_RG_group','landing_file_name','OSEA_USER_RG',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18337,1567,'HCP_OSEA_Ingestion_User_RG_group','target_table','SDL_HCP_OSEA_USER_RG',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18338,1567,'HCP_OSEA_Ingestion_User_RG_group','target_schema','HCPOSESDL_RAW',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18339,1567,'HCP_OSEA_Ingestion_User_RG_group','ms_query','SELECT 
	Id,
	Username,
	Lastname,
	Firstname,
	Name,
	CompanyName,
	Division,
	Department,
	TITLE,
	City,
	State,
	PostalCode,
	Country,
	'''' AS ADDRESS,
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
	UserType,
	LanguageLocaleKey,
	EmployeeNumber,
	ManagerId,
	LASTLOGINDATE,
	CREATEDDATE,
	CREATEDBYID,
	LastModifiedDate,
	LastModifiedById,
	FEDERATIONIDENTIFIER
FROM
    User
WHERE 
	LastModifiedDate>= {ts''2019-01-01 00:00:00''}
	and Country in (''ID'',''TH'') and CompanyName not in (''J&J India'')',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18340,1567,'HCP_OSEA_Ingestion_User_RG_group','truncate_and_load','Y',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18341,1567,'HCP_OSEA_Ingestion_User_RG_group','decide_source','salesforce_db',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18342,1567,'HCP_OSEA_Ingestion_User_RG_group','load_method','sp',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18343,1567,'HCP_OSEA_Ingestion_User_RG_group','sp_name','HCPOSESDL_RAW.HCP_OSEA_SALESFORCE_LOAD_PREPROCESSING',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18324,1566,'HCP_OSEA_Ingestion_UserTerritory_group','container','ose',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18325,1566,'HCP_OSEA_Ingestion_UserTerritory_group','landing_file_path','salesforce/',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18326,1566,'HCP_OSEA_Ingestion_UserTerritory_group','landing_file_name','OSEA_USERTERRITORY',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18327,1566,'HCP_OSEA_Ingestion_UserTerritory_group','target_table','SDL_HCP_OSEA_USERTERRITORY',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18328,1566,'HCP_OSEA_Ingestion_UserTerritory_group','target_schema','HCPOSESDL_RAW',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18329,1566,'HCP_OSEA_Ingestion_UserTerritory_group','ms_query','SELECT 
	ID,
	USERID,
	TERRITORY2ID,
	ISACTIVE,
	ROLEINTERRITORY2,
	LASTMODIFIEDDATE,
	LASTMODIFIEDBYID
FROM
    UserTerritory2Association
WHERE 
	LastModifiedDate>1900-01-01T00:00:00.000Z',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18330,1566,'HCP_OSEA_Ingestion_UserTerritory_group','truncate_and_load','Y',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18331,1566,'HCP_OSEA_Ingestion_UserTerritory_group','decide_source','salesforce_db',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18332,1566,'HCP_OSEA_Ingestion_UserTerritory_group','load_method','sp',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18333,1566,'HCP_OSEA_Ingestion_UserTerritory_group','sp_name','HCPOSESDL_RAW.HCP_OSEA_SALESFORCE_LOAD_PREPROCESSING',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18251,1558,'HCP_OSEA_Ingestion_Account_HCP_group','next_incremental_value','query:SELECT TO_VARCHAR(DATEADD(DAY, -2, MAX(LAST_MODIFIED_DATE)),''YYYY-MM-DD"T"HH24:MI:SSZ'') AS NEXT_VALUE FROM HCPOSESDL_RAW.SDL_HCP_OSEA_ACCOUNT_HCP;',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18252,1559,'HCP_OSEA_Ingestion_Address_group','container','ose',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18253,1559,'HCP_OSEA_Ingestion_Address_group','landing_file_path','salesforce/',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18254,1559,'HCP_OSEA_Ingestion_Address_group','landing_file_name','OSEA_ADDRESS',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18255,1559,'HCP_OSEA_Ingestion_Address_group','target_table','SDL_HCP_OSEA_ADDRESS',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (17076,1465,'Master_File_Data_Holidays_Ingestion_group','trigger_mail','Y',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (17077,1465,'Master_File_Data_Holidays_Ingestion_group','business_mail_trigger','Y',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (17078,1465,'Master_File_Data_Holidays_Ingestion_group','val_file_header','Country_Code|Holiday_in_YYYYMMDD',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (17079,1465,'Master_File_Data_Holidays_Ingestion_group','file_spec','holidays',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (17080,1465,'Master_File_Data_Holidays_Ingestion_group','val_file_name','holidays',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (17081,1465,'Master_File_Data_Holidays_Ingestion_group','val_file_extn','xlsx',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (17082,1465,'Master_File_Data_Holidays_Ingestion_group','load_method','sp',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (17083,1465,'Master_File_Data_Holidays_Ingestion_group','sp_name','HCPOSESDL_RAW.HCP_OSEA_HOLIDAY_LIST_PREPROCESSING',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (17084,1465,'Master_File_Data_Holidays_Ingestion_group','folder_path','hcp_osea/master/holidays',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (17085,1465,'Master_File_Data_Holidays_Ingestion_group','target_table','SDL_HCP_OSEA_HOLIDAY_LIST',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (17086,1465,'Master_File_Data_Holidays_Ingestion_group','container','ose',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (17087,1465,'Master_File_Data_Holidays_Ingestion_group','target_schema','HCPOSESDL_RAW',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (17088,1465,'Master_File_Data_Holidays_Ingestion_group','validation','1-1-1',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (17089,1465,'Master_File_Data_Holidays_Ingestion_group','index','first',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (17090,1465,'Master_File_Data_Holidays_Ingestion_group','source_extn','xlsx',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (17091,1465,'Master_File_Data_Holidays_Ingestion_group','file_header_row_num','0',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (17092,1465,'Master_File_Data_Holidays_Ingestion_group','is_truncate','Y',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (17093,1465,'Master_File_Data_Holidays_Ingestion_group','start_Range','A1',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (17094,1465,'Master_File_Data_Holidays_Ingestion_group','sheet_index','0',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (17095,1466,'Master_File_Data_iConnectusers_Ingestion_group','trigger_mail','Y',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (17096,1466,'Master_File_Data_iConnectusers_Ingestion_group','business_mail_trigger','Y',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (17097,1466,'Master_File_Data_iConnectusers_Ingestion_group','val_file_header','Year|Country|Sector|Qty|LicenseType|Country|Company|Division/BU|Sector',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (17098,1466,'Master_File_Data_iConnectusers_Ingestion_group','file_spec','iConnectUsers',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (17099,1466,'Master_File_Data_iConnectusers_Ingestion_group','val_file_name','iConnectUsers',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (17100,1466,'Master_File_Data_iConnectusers_Ingestion_group','val_file_extn','xlsx',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (17101,1466,'Master_File_Data_iConnectusers_Ingestion_group','load_method','sp',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (17102,1466,'Master_File_Data_iConnectusers_Ingestion_group','sp_name','HCPOSESDL_RAW.HCP_OSEA_ICONNECTUSERS_PREPROCESSING',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (17103,1466,'Master_File_Data_iConnectusers_Ingestion_group','folder_path','hcp_osea/master/iconnectusers',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (17104,1466,'Master_File_Data_iConnectusers_Ingestion_group','target_table','SDL_HCP_OSEA_ISIGHT_LICENSES:SDL_HCP_OSEA_ISIGHT_SECTOR_MAPPING',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (17105,1466,'Master_File_Data_iConnectusers_Ingestion_group','container','ose',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (17106,1466,'Master_File_Data_iConnectusers_Ingestion_group','target_schema','HCPOSESDL_RAW',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (17107,1466,'Master_File_Data_iConnectusers_Ingestion_group','validation','1-1-1',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (17108,1466,'Master_File_Data_iConnectusers_Ingestion_group','index','first',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (17109,1466,'Master_File_Data_iConnectusers_Ingestion_group','source_extn','xlsx',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (17110,1466,'Master_File_Data_iConnectusers_Ingestion_group','file_header_row_num','0',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (17111,1466,'Master_File_Data_iConnectusers_Ingestion_group','is_truncate','Y',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (17112,1466,'Master_File_Data_iConnectusers_Ingestion_group','start_Range','A1',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (17113,1466,'Master_File_Data_iConnectusers_Ingestion_group','sheet_names','Licenses,SectorMapping',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18454,1577,'HCP_OSEA_Ingestion_Cycle_Plan_Detail_group','incremental_filter',' WHERE LastModifiedDate>={WATERMARK_VALUE}',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18455,1577,'HCP_OSEA_Ingestion_Cycle_Plan_Detail_group','load_method','sp',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18456,1577,'HCP_OSEA_Ingestion_Cycle_Plan_Detail_group','sp_name','HCPOSESDL_RAW.HCP_OSEA_SALESFORCE_LOAD_PREPROCESSING',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18457,1577,'HCP_OSEA_Ingestion_Cycle_Plan_Detail_group','next_incremental_value','query:SELECT TO_VARCHAR(DATEADD(DAY, -2, MAX(LAST_MODIFIED_DATE)),''YYYY-MM-DD"T"HH24:MI:SSZ'') AS NEXT_VALUE FROM HCPOSESDL_RAW.SDL_HCP_OSEA_CYCLE_PLAN_DETAIL;',FALSE,TRUE);
INSERT INTO PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (18422,1575,'HCP_OSEA_Ingestion_Cycle_Plan_group','container','ose',FALSE,TRUE);



