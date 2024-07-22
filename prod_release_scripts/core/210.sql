--DROP TABLE HCPOSEEDW_INTEGRATION.dim_country;
CREATE OR REPLACE TABLE HCPOSEEDW_INTEGRATION.DIM_COUNTRY		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE HCPOSEEDW_INTEGRATION.dim_country
(
	country_key VARCHAR(8)  		--//  ENCODE zstd
	,country_code VARCHAR(8)  		--//  ENCODE zstd
	,country_name VARCHAR(50)  		--//  ENCODE zstd
	,country_display_order NUMERIC(2,0)  		--//  ENCODE delta
	,country_group_code VARCHAR(8)  		--//  ENCODE zstd
	,country_group_name VARCHAR(50)  		--//  ENCODE zstd
	,country_group_display_order NUMERIC(2,0)  		--//  ENCODE delta
	,description VARCHAR(50)  		--//  ENCODE zstd
	,attr1 VARCHAR(50)  		--//  ENCODE zstd
	,attr2 VARCHAR(50)  		--//  ENCODE zstd
	,attr3 VARCHAR(50)  		--//  ENCODE zstd
	,attr4 VARCHAR(50)  		--//  ENCODE zstd
	,attr5 VARCHAR(50)  		--//  ENCODE zstd
	,manual_update_date VARCHAR(50)  		--//  ENCODE zstd
	,manual_update_user VARCHAR(50)  		--//  ENCODE zstd
	,inserted_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,updated_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
)

;
--DROP TABLE HCPOSEEDW_INTEGRATION.dim_date;
CREATE OR REPLACE TABLE HCPOSEEDW_INTEGRATION.DIM_DATE		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE HCPOSEEDW_INTEGRATION.dim_date
(
	date_key numeric(18,0) NOT NULL 		--//  ENCODE az64 // INTEGER 
	,date_value TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,date_year numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,date_quarter VARCHAR(2)  		--//  ENCODE lzo
	,date_quarterlong VARCHAR(5)  		--//  ENCODE lzo
	,date_month VARCHAR(3)  		--//  ENCODE lzo
	,date_monthlong VARCHAR(10)  		--//  ENCODE lzo
	,date_monthnum numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,date_yearmonth VARCHAR(10)  		--//  ENCODE lzo
	,date_yearmonthnum numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,date_dayofweek VARCHAR(10)  		--//  ENCODE lzo
	,date_dayofmonth numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,date_weekofmonth numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,date_weekofquarter numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,date_dayofyear numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,date_weekofyear numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,jnj_date_year numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,jnj_date_quarter VARCHAR(5)  		--//  ENCODE lzo
	,jnj_date_quarterlong VARCHAR(10)  		--//  ENCODE lzo
	,jnj_date_month VARCHAR(5)  		--//  ENCODE lzo
	,jnj_date_monthlong VARCHAR(10)  		--//  ENCODE lzo
	,jnj_date_dayofmonth numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,jnj_date_weekofmonth numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,jnj_date_weekofquarter numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,jnj_date_dayofyear numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,jnj_date_weekofyear numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,manual_update_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,manual_update_user VARCHAR(100)  		--//  ENCODE lzo
	,inserted_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,updated_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,rpt_date_year numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,rpt_date_quarter CHAR(2)  		--//  ENCODE lzo
	,rpt_date_quarterlong CHAR(5)  		--//  ENCODE lzo
	,rpt_date_month CHAR(3)  		--//  ENCODE lzo
	,rpt_date_monthlong CHAR(10)  		--//  ENCODE lzo
	,rpt_date_dayofmonth numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,rpt_date_weekofmonth numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,rpt_date_weekofquarter numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,rpt_date_dayofyear numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,rpt_date_weekofyear numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,jnj_date_monthofyear CHAR(2)  		--//  ENCODE lzo
	,my_date_year numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,my_date_quarter CHAR(2)  		--//  ENCODE lzo
	,my_date_quarterlong CHAR(5)  		--//  ENCODE lzo
	,my_date_month CHAR(3)  		--//  ENCODE lzo
	,my_date_monthlong CHAR(10)  		--//  ENCODE lzo
	,my_date_dayofmonth numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,my_date_weekofmonth numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,my_date_weekofquarter numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,my_date_dayofyear numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,my_date_weekofyear numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,my_date_monthofyear CHAR(2)  		--//  ENCODE lzo
	,PRIMARY KEY (date_key)
)

cluster by (date_key)
;
--DROP TABLE HCPOSEEDW_INTEGRATION.dim_employee;
CREATE OR REPLACE TABLE HCPOSEEDW_INTEGRATION.DIM_EMPLOYEE		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE HCPOSEEDW_INTEGRATION.dim_employee
(
	employee_key VARCHAR(32) NOT NULL 		--//  ENCODE zstd
	,country_code VARCHAR(8)  		--//  ENCODE zstd
	,employee_source_id VARCHAR(18)  		--//  ENCODE zstd
	,modified_dt TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,modified_id VARCHAR(18)  		--//  ENCODE zstd
	,employee_name VARCHAR(121)  		--//  ENCODE zstd
	,employee_wwid VARCHAR(20)  		--//  ENCODE zstd
	,mobile_phone VARCHAR(40)  		--//  ENCODE zstd
	,email_id VARCHAR(128)  		--//  ENCODE zstd
	,username VARCHAR(80)  		--//  ENCODE zstd
	,nickname VARCHAR(40)  		--//  ENCODE zstd
	,local_employee_number VARCHAR(20)  		--//  ENCODE zstd
	,profile_id VARCHAR(18)  		--//  ENCODE zstd
	,profile_name VARCHAR(255)  		--//  ENCODE zstd
	,function_name VARCHAR(255)  		--//  ENCODE zstd
	,employee_profile_id VARCHAR(18)  		--//  ENCODE zstd
	,company_name VARCHAR(80)  		--//  ENCODE zstd
	,division_name VARCHAR(80)  		--//  ENCODE zstd
	,department_name VARCHAR(80)  		--//  ENCODE zstd
	,country_name VARCHAR(80)  		--//  ENCODE zstd
	,address VARCHAR(255)  		--//  ENCODE zstd
	,alias VARCHAR(18)  		--//  ENCODE zstd
	,timezonesidkey VARCHAR(40)  		--//  ENCODE zstd
	,user_role_source_id VARCHAR(18)  		--//  ENCODE zstd
	,receives_info_emails numeric(18,0)		--//  ENCODE delta // INTEGER  
	,federation_identifier VARCHAR(512)  		--//  ENCODE zstd
	,last_ipad_sync TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,user_license VARCHAR(1300)  		--//  ENCODE zstd
	,title VARCHAR(1300)  		--//  ENCODE zstd
	,phone VARCHAR(43)  		--//  ENCODE zstd
	,last_login_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,region VARCHAR(1300)  		--//  ENCODE zstd
	,profile_group_ap VARCHAR(1030)  		--//  ENCODE zstd
	,manager_name VARCHAR(80)  		--//  ENCODE zstd
	,manager_wwid VARCHAR(18)  		--//  ENCODE zstd
	,active_flag NUMERIC(2,0)  		--//  ENCODE delta
	,my_organization_code VARCHAR(18)  		--//  ENCODE zstd
	,my_organization_name VARCHAR(80)  		--//  ENCODE zstd
	,organization_l1_code VARCHAR(18)  		--//  ENCODE zstd
	,organization_l1_name VARCHAR(80)  		--//  ENCODE zstd
	,organization_l2_code VARCHAR(18)  		--//  ENCODE zstd
	,organization_l2_name VARCHAR(80)  		--//  ENCODE zstd
	,organization_l3_code VARCHAR(18)  		--//  ENCODE zstd
	,organization_l3_name VARCHAR(80)  		--//  ENCODE zstd
	,organization_l4_code VARCHAR(18)  		--//  ENCODE zstd
	,organization_l4_name VARCHAR(80)  		--//  ENCODE zstd
	,organization_l5_code VARCHAR(18)  		--//  ENCODE zstd
	,organization_l5_name VARCHAR(80)  		--//  ENCODE zstd
	,organization_l6_code VARCHAR(18)  		--//  ENCODE zstd
	,organization_l6_name VARCHAR(80)  		--//  ENCODE zstd
	,organization_l7_code VARCHAR(18)  		--//  ENCODE zstd
	,organization_l7_name VARCHAR(80)  		--//  ENCODE zstd
	,organization_l8_code VARCHAR(18)  		--//  ENCODE zstd
	,organization_l8_name VARCHAR(80)  		--//  ENCODE zstd
	,organization_l9_code VARCHAR(18)  		--//  ENCODE zstd
	,organization_l9_name VARCHAR(80)  		--//  ENCODE zstd
	,common_organization_l1_code VARCHAR(18)  		--//  ENCODE zstd
	,common_organization_l1_name VARCHAR(80)  		--//  ENCODE zstd
	,common_organization_l2_code VARCHAR(18)  		--//  ENCODE zstd
	,common_organization_l2_name VARCHAR(80)  		--//  ENCODE zstd
	,common_organization_l3_code VARCHAR(18)  		--//  ENCODE zstd
	,common_organization_l3_name VARCHAR(80)  		--//  ENCODE zstd
	,inserted_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,updated_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,msl_primary_responsible_ta VARCHAR(255)  		--//  ENCODE zstd
	,msl_secondary_responsible_ta VARCHAR(255)  		--//  ENCODE zstd
	,city VARCHAR(40)  		--//  ENCODE zstd
	,first_name VARCHAR(40)  		--//  ENCODE zstd
	,language_local_key VARCHAR(40)  		--//  ENCODE zstd
	,last_name VARCHAR(80)  		--//  ENCODE zstd
	,manager_source_id VARCHAR(18)  		--//  ENCODE zstd
	,postal_code VARCHAR(20)  		--//  ENCODE zstd
	,state VARCHAR(80)  		--//  ENCODE zstd
	,user_type VARCHAR(40)  		--//  ENCODE zstd
	,veeva_user_type VARCHAR(255)  		--//  ENCODE zstd
	,veeva_country_code VARCHAR(255)  		--//  ENCODE zstd
	,shc_user_franchise VARCHAR(255)  		--//  ENCODE zstd
	,PRIMARY KEY (employee_key)
)
		--// DISTSTYLE KEY
cluster by (employee_key)
;
--DROP TABLE HCPOSEEDW_INTEGRATION.dim_employee_iconnect;
CREATE OR REPLACE TABLE HCPOSEEDW_INTEGRATION.DIM_EMPLOYEE_ICONNECT		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE HCPOSEEDW_INTEGRATION.dim_employee_iconnect
(
	employee_key VARCHAR(32)  		--//  ENCODE lzo
	,country_code VARCHAR(8)  		--//  ENCODE lzo
	,employee_source_id VARCHAR(18)  		--//  ENCODE lzo
	,modified_dt TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,modified_id VARCHAR(18)  		--//  ENCODE lzo
	,employee_name VARCHAR(121)  		--//  ENCODE lzo
	,employee_wwid VARCHAR(20)  		--//  ENCODE lzo
	,mobile_phone VARCHAR(40)  		--//  ENCODE lzo
	,email_id VARCHAR(128)  		--//  ENCODE lzo
	,username VARCHAR(80)  		--//  ENCODE lzo
	,nickname VARCHAR(40)  		--//  ENCODE lzo
	,local_employee_number VARCHAR(20)  		--//  ENCODE lzo
	,profile_id VARCHAR(18)  		--//  ENCODE lzo
	,profile_name VARCHAR(255)  		--//  ENCODE lzo
	,function_name VARCHAR(255)  		--//  ENCODE lzo
	,employee_profile_id VARCHAR(18)  		--//  ENCODE lzo
	,company_name VARCHAR(80)  		--//  ENCODE lzo
	,division_name VARCHAR(80)  		--//  ENCODE lzo
	,department_name VARCHAR(80)  		--//  ENCODE lzo
	,country_name VARCHAR(80)  		--//  ENCODE lzo
	,address VARCHAR(255)  		--//  ENCODE lzo
	,alias VARCHAR(18)  		--//  ENCODE lzo
	,timezonesidkey VARCHAR(40)  		--//  ENCODE lzo
	,user_role_source_id VARCHAR(18)  		--//  ENCODE lzo
	,receives_info_emails numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,federation_identifier VARCHAR(512)  		--//  ENCODE lzo
	,last_ipad_sync TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,user_license VARCHAR(1300)  		--//  ENCODE lzo
	,title VARCHAR(1300)  		--//  ENCODE lzo
	,phone VARCHAR(43)  		--//  ENCODE lzo
	,last_login_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,region VARCHAR(1300)  		--//  ENCODE lzo
	,profile_group_ap VARCHAR(1030)  		--//  ENCODE lzo
	,manager_name VARCHAR(80)  		--//  ENCODE lzo
	,manager_wwid VARCHAR(18)  		--//  ENCODE lzo
	,active_flag NUMERIC(2,0)  		--//  ENCODE az64
	,my_organization_code VARCHAR(18)  		--//  ENCODE lzo
	,my_organization_name VARCHAR(80)  		--//  ENCODE lzo
	,organization_l1_code VARCHAR(18)  		--//  ENCODE lzo
	,organization_l1_name VARCHAR(80)  		--//  ENCODE lzo
	,organization_l2_code VARCHAR(18)  		--//  ENCODE lzo
	,organization_l2_name VARCHAR(80)  		--//  ENCODE lzo
	,organization_l3_code VARCHAR(18)  		--//  ENCODE lzo
	,organization_l3_name VARCHAR(80)  		--//  ENCODE lzo
	,organization_l4_code VARCHAR(18)  		--//  ENCODE lzo
	,organization_l4_name VARCHAR(80)  		--//  ENCODE lzo
	,organization_l5_code VARCHAR(18)  		--//  ENCODE lzo
	,organization_l5_name VARCHAR(80)  		--//  ENCODE lzo
	,organization_l6_code VARCHAR(18)  		--//  ENCODE lzo
	,organization_l6_name VARCHAR(80)  		--//  ENCODE lzo
	,organization_l7_code VARCHAR(18)  		--//  ENCODE lzo
	,organization_l7_name VARCHAR(80)  		--//  ENCODE lzo
	,organization_l8_code VARCHAR(18)  		--//  ENCODE lzo
	,organization_l8_name VARCHAR(80)  		--//  ENCODE lzo
	,organization_l9_code VARCHAR(18)  		--//  ENCODE lzo
	,organization_l9_name VARCHAR(80)  		--//  ENCODE lzo
	,common_organization_l1_code VARCHAR(18)  		--//  ENCODE lzo
	,common_organization_l1_name VARCHAR(80)  		--//  ENCODE lzo
	,common_organization_l2_code VARCHAR(18)  		--//  ENCODE lzo
	,common_organization_l2_name VARCHAR(80)  		--//  ENCODE lzo
	,common_organization_l3_code VARCHAR(18)  		--//  ENCODE lzo
	,common_organization_l3_name VARCHAR(80)  		--//  ENCODE lzo
	,inserted_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,updated_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,msl_primary_responsible_ta VARCHAR(255)  		--//  ENCODE lzo
	,msl_secondary_responsible_ta VARCHAR(255)  		--//  ENCODE lzo
	,city VARCHAR(40)  		--//  ENCODE zstd
	,first_name VARCHAR(40)  		--//  ENCODE zstd
	,language_local_key VARCHAR(40)  		--//  ENCODE zstd
	,last_name VARCHAR(80)  		--//  ENCODE zstd
	,manager_source_id VARCHAR(18)  		--//  ENCODE zstd
	,postal_code VARCHAR(20)  		--//  ENCODE zstd
	,state VARCHAR(80)  		--//  ENCODE zstd
	,user_type VARCHAR(40)  		--//  ENCODE zstd
	,veeva_user_type VARCHAR(255)  		--//  ENCODE zstd
	,veeva_country_code VARCHAR(255)  		--//  ENCODE zstd
	,shc_user_franchise VARCHAR(255)  		--//  ENCODE zstd
)
		--// DISTSTYLE KEY
cluster by (employee_key)
;
--DROP TABLE HCPOSEEDW_INTEGRATION.dim_hco;
CREATE OR REPLACE TABLE HCPOSEEDW_INTEGRATION.DIM_HCO		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE HCPOSEEDW_INTEGRATION.dim_hco
(
	hco_key VARCHAR(32) NOT NULL 		--//  ENCODE zstd
	,country_code VARCHAR(8)  		--//  ENCODE zstd
	,hco_source_id VARCHAR(18)  		--//  ENCODE zstd
	,modify_id VARCHAR(18)  		--//  ENCODE zstd
	,modify_dt TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,hco_business_id VARCHAR(123)  		--//  ENCODE zstd
	,inactive_flag numeric(18,0)		--//  ENCODE delta // INTEGER  
	,inactive_reason VARCHAR(255)  		--//  ENCODE zstd
	,hco_name VARCHAR(1300)  		--//  ENCODE zstd
	,hco_type VARCHAR(255)  		--//  ENCODE zstd
	,hco_type_english_name VARCHAR(255)  		--//  ENCODE zstd
	,sector VARCHAR(255)  		--//  ENCODE zstd
	,phone_number VARCHAR(40)  		--//  ENCODE zstd
	,fax_number VARCHAR(40)  		--//  ENCODE zstd
	,website VARCHAR(255)  		--//  ENCODE zstd
	,territory_name VARCHAR(255)  		--//  ENCODE zstd
	,formatted_name VARCHAR(1300)  		--//  ENCODE zstd
	,remarks VARCHAR(255)  		--//  ENCODE zstd
	,exclude_from_territory_assignment_rules numeric(18,0)		--//  ENCODE delta // INTEGER  
	,beds NUMERIC(8,0)  		--//  ENCODE delta
	,total_mds_dos NUMERIC(18,0)  		--//  ENCODE delta
	,departments NUMERIC(18,0)  		--//  ENCODE delta
	,sfe_approved_flag numeric(18,0)		--//  ENCODE delta // INTEGER  
	,business_description VARCHAR(32330)  		--//  ENCODE zstd
	,hcc_account_approved numeric(18,0)		--//  ENCODE delta // INTEGER  
	,total_physicians_enrolled NUMERIC(18,0)  		--//  ENCODE delta
	,total_pharmacists NUMERIC(3,0)  		--//  ENCODE delta
	,is_external_id_number numeric(18,0)		--//  ENCODE delta // INTEGER  
	,ot NUMERIC(18,0)  		--//  ENCODE delta
	,kam_paediatric NUMERIC(18,0)  		--//  ENCODE delta
	,kam_obgyn NUMERIC(18,0)  		--//  ENCODE delta
	,kam_minimally_invasive NUMERIC(18,0)  		--//  ENCODE delta
	,kam_total_urologysurgeons NUMERIC(18,0)  		--//  ENCODE delta
	,kam_total_surgeons NUMERIC(18,0)  		--//  ENCODE delta
	,kam_total_rheumphysicians NUMERIC(18,0)  		--//  ENCODE delta
	,kam_total_psychiatryphysicians NUMERIC(18,0)  		--//  ENCODE delta
	,kam_total_orthosurgeons NUMERIC(18,0)  		--//  ENCODE delta
	,kam_total_opthalsurgeons NUMERIC(18,0)  		--//  ENCODE delta
	,kam_total_neurologyphysicians NUMERIC(18,0)  		--//  ENCODE delta
	,kam_total_medoncophysicians NUMERIC(18,0)  		--//  ENCODE delta
	,kam_total_infectiousphysicians NUMERIC(18,0)  		--//  ENCODE delta
	,kam_total_haemaphysicians NUMERIC(18,0)  		--//  ENCODE delta
	,kam_total_generalsurgeons NUMERIC(18,0)  		--//  ENCODE delta
	,kam_total_gastrophysicians NUMERIC(18,0)  		--//  ENCODE delta
	,kam_total_endophysicians NUMERIC(18,0)  		--//  ENCODE delta
	,kam_total_dermaphysicians NUMERIC(18,0)  		--//  ENCODE delta
	,kam_total_cardiologyphysicians NUMERIC(18,0)  		--//  ENCODE delta
	,kam_total_cardiosurgeons NUMERIC(18,0)  		--//  ENCODE delta
	,kam_total_aestheticsurgeons NUMERIC(18,0)  		--//  ENCODE delta
	,kam_general_differnentiations VARCHAR(32768)  		--//  ENCODE zstd
	,kam_clinical_differentiations VARCHAR(32768)  		--//  ENCODE zstd
	,record_type_name VARCHAR(255)  		--//  ENCODE zstd
	,hco_external_id VARCHAR(120)  		--//  ENCODE zstd
	,parent_hco_key VARCHAR(32)  		--//  ENCODE zstd
	,parent_hco_name VARCHAR(255)  		--//  ENCODE zstd
	,deleted_flag numeric(18,0)		--//  ENCODE delta // INTEGER  
	,address_line_1 VARCHAR(255)  		--//  ENCODE zstd
	,address_line_2 VARCHAR(300)  		--//  ENCODE zstd
	,suburb_town VARCHAR(50)  		--//  ENCODE zstd
	,city VARCHAR(40)  		--//  ENCODE zstd
	,state VARCHAR(255)  		--//  ENCODE zstd
	,postcode VARCHAR(20)  		--//  ENCODE zstd
	,brick VARCHAR(250)  		--//  ENCODE zstd
	,map VARCHAR(1300)  		--//  ENCODE zstd
	,hco_address_source_id VARCHAR(18)  		--//  ENCODE zstd
	,appt_required_flag numeric(18,0)		--//  ENCODE delta // INTEGER  
	,address_external_id VARCHAR(123)  		--//  ENCODE zstd
	,address_phone VARCHAR(43)  		--//  ENCODE zstd
	,address_fax VARCHAR(43)  		--//  ENCODE zstd
	,primary_flag numeric(18,0)		--//  ENCODE delta // INTEGER  
	,address_inactive_flag numeric(18,0)		--//  ENCODE delta // INTEGER  
	,controlling_address VARCHAR(18)  		--//  ENCODE zstd
	,veeva_autogen_id VARCHAR(33)  		--//  ENCODE zstd
	,customer_code VARCHAR(60)  		--//  ENCODE zstd
	,inserted_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,updated_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,PRIMARY KEY (hco_key)
)

cluster by (hco_key)
		--// SORTKEY ( 
		--// 	hco_key
		--// 	)
;		--// ;
--DROP TABLE HCPOSEEDW_INTEGRATION.dim_hcp;
CREATE OR REPLACE TABLE HCPOSEEDW_INTEGRATION.DIM_HCP		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE HCPOSEEDW_INTEGRATION.dim_hcp
(
	hcp_key VARCHAR(32) NOT NULL 		--//  ENCODE zstd
	,country_code VARCHAR(8)  		--//  ENCODE zstd
	,hcp_source_id VARCHAR(18)  		--//  ENCODE zstd
	,modify_dt TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,modify_id VARCHAR(18)  		--//  ENCODE zstd
	,hcp_business_id VARCHAR(30)  		--//  ENCODE zstd
	,hcp_type VARCHAR(255)  		--//  ENCODE zstd
	,hcp_type_english VARCHAR(255)  		--//  ENCODE zstd
	,inactive_flag numeric(18,0)		--//  ENCODE delta // INTEGER  
	,inactive_date DATE  		--//  ENCODE delta
	,inactive_reason VARCHAR(255)  		--//  ENCODE zstd
	,hcp_name VARCHAR(255)  		--//  ENCODE zstd
	,hcp_english_name VARCHAR(800)  		--//  ENCODE zstd
	,hcp_display_name VARCHAR(255)  		--//  ENCODE zstd
	,gender VARCHAR(255)  		--//  ENCODE zstd
	,birth_day DATE  		--//  ENCODE delta
	,speciality_1_type VARCHAR(255)  		--//  ENCODE zstd
	,speciality_1_type_english VARCHAR(255)  		--//  ENCODE zstd
	,position_name VARCHAR(255)  		--//  ENCODE zstd
	,position_name_english VARCHAR(255)  		--//  ENCODE zstd
	,territory_name VARCHAR(255)  		--//  ENCODE zstd
	,kol_flag numeric(18,0)		--//  ENCODE delta // INTEGER  
	,license_cd VARCHAR(255)  		--//  ENCODE zstd
	,direct_line_number VARCHAR(40)  		--//  ENCODE zstd
	,direct_fax_number VARCHAR(40)  		--//  ENCODE zstd
	,email_id VARCHAR(255)  		--//  ENCODE zstd
	,mobile_nbr VARCHAR(40)  		--//  ENCODE zstd
	,do_not_call_flag numeric(18,0)		--//  ENCODE delta // INTEGER  
	,email_opt_out_flag numeric(18,0)		--//  ENCODE delta // INTEGER  
	,parent_hco_key VARCHAR(32)  		--//  ENCODE zstd
	,parent_hco_name VARCHAR(1300)  		--//  ENCODE zstd
	,record_type_name VARCHAR(255)  		--//  ENCODE zstd
	,hcp_external_id VARCHAR(120)  		--//  ENCODE zstd
	,remarks VARCHAR(255)  		--//  ENCODE zstd
	,deleted_flag numeric(18,0)		--//  ENCODE delta // INTEGER  
	,preferred_name VARCHAR(255)  		--//  ENCODE zstd
	,professional_type VARCHAR(255)  		--//  ENCODE zstd
	,go_classification VARCHAR(255)  		--//  ENCODE zstd
	,hcc_account_approved_flag numeric(18,0)		--//  ENCODE delta // INTEGER  
	,salutation VARCHAR(120)  		--//  ENCODE zstd
	,exclude_from_territory_assignment_rules_flag numeric(18,0)		--//  ENCODE delta // INTEGER  
	,sfe_approved_flag numeric(18,0)		--//  ENCODE delta // INTEGER  
	,physician_prescribing_behavior VARCHAR(255)  		--//  ENCODE zstd
	,physician_behavioral_style VARCHAR(255)  		--//  ENCODE zstd
	,is_external_id_number VARCHAR(5)  		--//  ENCODE zstd
	,customer_value_segmentation VARCHAR(255)  		--//  ENCODE zstd
	,ability_to_influence_peers VARCHAR(255)  		--//  ENCODE zstd
	,practice_size VARCHAR(255)  		--//  ENCODE zstd
	,patient_type VARCHAR(255)  		--//  ENCODE zstd
	,patient_medical_condition VARCHAR(255)  		--//  ENCODE zstd
	,md_customer_segmentation VARCHAR(255)  		--//  ENCODE zstd
	,years_of_experience VARCHAR(255)  		--//  ENCODE zstd
	,md_innovation VARCHAR(255)  		--//  ENCODE zstd
	,md_number_of_procedures VARCHAR(255)  		--//  ENCODE zstd
	,md_types_of_procedure VARCHAR(255)  		--//  ENCODE zstd
	,md_total_hip_replacement NUMERIC(18,0)  		--//  ENCODE delta
	,md_total_knee_replacement NUMERIC(18,0)  		--//  ENCODE delta
	,md_spine NUMERIC(18,0)  		--//  ENCODE delta
	,md_trauma NUMERIC(18,0)  		--//  ENCODE delta
	,md_collorectal NUMERIC(18,0)  		--//  ENCODE delta
	,md_hepatobiliary NUMERIC(18,0)  		--//  ENCODE delta
	,md_cholecystectomy NUMERIC(18,0)  		--//  ENCODE delta
	,md_total_hysterectomy NUMERIC(18,0)  		--//  ENCODE delta
	,md_myomectomy NUMERIC(18,0)  		--//  ENCODE delta
	,md_c_section NUMERIC(18,0)  		--//  ENCODE delta
	,md_normal_delivery NUMERIC(18,0)  		--//  ENCODE delta
	,md_cabg NUMERIC(18,0)  		--//  ENCODE delta
	,md_valve_repair NUMERIC(18,0)  		--//  ENCODE delta
	,md_abdominal NUMERIC(18,0)  		--//  ENCODE delta
	,md_breast_reconstruction NUMERIC(18,0)  		--//  ENCODE delta
	,md_oral_cranial_maxilofacial NUMERIC(18,0)  		--//  ENCODE delta
	,inserted_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,updated_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,primary_ta VARCHAR(255)  		--//  ENCODE zstd
	,secondary_ta VARCHAR(255)  		--//  ENCODE zstd
	,PRIMARY KEY (hcp_key)
)
		--// DISTSTYLE KEY
cluster by (hcp_key)
;
--DROP TABLE HCPOSEEDW_INTEGRATION.dim_key_message;
CREATE OR REPLACE TABLE HCPOSEEDW_INTEGRATION.DIM_KEY_MESSAGE		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE HCPOSEEDW_INTEGRATION.dim_key_message
(
	key_message_key VARCHAR(256) NOT NULL 		--//  ENCODE zstd
	,country_code VARCHAR(256)  		--//  ENCODE zstd
	,key_message_name VARCHAR(250)  		--//  ENCODE zstd
	,product_name VARCHAR(80)  		--//  ENCODE zstd
	,key_message_source_id VARCHAR(18)  		--//  ENCODE zstd
	,key_message_createddate TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,key_message_createdbyid VARCHAR(18)  		--//  ENCODE zstd
	,key_message_lastmodifieddate TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,key_message_lastmodifiedbyid VARCHAR(18)  		--//  ENCODE zstd
	,key_message_systemmodstamp TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,key_message_lastvieweddate TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,key_message_lastreferenceddate TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,key_message_description VARCHAR(800)  		--//  ENCODE zstd
	,key_message_active_flag VARCHAR(5)  		--//  ENCODE zstd
	,key_message_category VARCHAR(255)  		--//  ENCODE zstd
	,key_message_vehicle VARCHAR(255)  		--//  ENCODE zstd
	,key_message_clm_id VARCHAR(100)  		--//  ENCODE zstd
	,key_message_custom_reaction VARCHAR(255)  		--//  ENCODE zstd
	,key_message_slide_version VARCHAR(100)  		--//  ENCODE zstd
	,key_message_language VARCHAR(255)  		--//  ENCODE zstd
	,key_message_media_file_crc VARCHAR(255)  		--//  ENCODE zstd
	,key_message_media_file_name VARCHAR(255)  		--//  ENCODE zstd
	,key_message_media_file_size NUMERIC(18,0)  		--//  ENCODE delta
	,key_message_segment VARCHAR(80)  		--//  ENCODE zstd
	,key_message_detail_group VARCHAR(18)  		--//  ENCODE zstd
	,key_message_core_content_approval_id VARCHAR(100)  		--//  ENCODE zstd
	,key_message_core_content_expiration_date DATE  		--//  ENCODE delta
	,key_message_simp_country VARCHAR(1300)  		--//  ENCODE zstd
	,key_message_ap_clm_country VARCHAR(4)  		--//  ENCODE zstd
	,key_message_shared_resource_flag numeric(18,0)		--//  ENCODE delta // INTEGER  
	,key_message_shared_resource VARCHAR(18)  		--//  ENCODE zstd
	,inserted_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,updated_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,PRIMARY KEY (key_message_key)
)

cluster by (key_message_key)
		--// SORTKEY ( 
		--// 	key_message_custom_reaction
		--// 	)
;		--// ;
--DROP TABLE HCPOSEEDW_INTEGRATION.dim_organization;
CREATE OR REPLACE TABLE HCPOSEEDW_INTEGRATION.DIM_ORGANIZATION		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE HCPOSEEDW_INTEGRATION.dim_organization
(
	organization_key VARCHAR(32) NOT NULL 		--//  ENCODE zstd
	,territory_source_id VARCHAR(18)  		--//  ENCODE zstd
	,country_code VARCHAR(8)  		--//  ENCODE zstd
	,my_organization_code VARCHAR(18)  		--//  ENCODE zstd
	,my_organization_name VARCHAR(80)  		--//  ENCODE zstd
	,organization_l1_code VARCHAR(18)  		--//  ENCODE zstd
	,organization_l1_name VARCHAR(80)  		--//  ENCODE zstd
	,organization_l2_code VARCHAR(18)  		--//  ENCODE zstd
	,organization_l2_name VARCHAR(80)  		--//  ENCODE zstd
	,organization_l3_code VARCHAR(18)  		--//  ENCODE zstd
	,organization_l3_name VARCHAR(80)  		--//  ENCODE zstd
	,organization_l4_code VARCHAR(18)  		--//  ENCODE zstd
	,organization_l4_name VARCHAR(80)  		--//  ENCODE zstd
	,organization_l5_code VARCHAR(18)  		--//  ENCODE zstd
	,organization_l5_name VARCHAR(80)  		--//  ENCODE zstd
	,organization_l6_code VARCHAR(18)  		--//  ENCODE zstd
	,organization_l6_name VARCHAR(80)  		--//  ENCODE zstd
	,organization_l7_code VARCHAR(18)  		--//  ENCODE zstd
	,organization_l7_name VARCHAR(80)  		--//  ENCODE zstd
	,organization_l8_code VARCHAR(18)  		--//  ENCODE zstd
	,organization_l8_name VARCHAR(80)  		--//  ENCODE zstd
	,organization_l9_code VARCHAR(18)  		--//  ENCODE zstd
	,organization_l9_name VARCHAR(80)  		--//  ENCODE zstd
	,effective_from_date DATE  		--//  ENCODE delta
	,effective_to_date DATE  		--//  ENCODE delta
	,flag VARCHAR(18)  		--//  ENCODE zstd
	,inserted_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,updated_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,PRIMARY KEY (organization_key)
)
		--// DISTSTYLE KEY
cluster by (organization_key)
;
--DROP TABLE HCPOSEEDW_INTEGRATION.dim_product_indication;
CREATE OR REPLACE TABLE HCPOSEEDW_INTEGRATION.DIM_PRODUCT_INDICATION		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE HCPOSEEDW_INTEGRATION.dim_product_indication
(
	product_indication_key VARCHAR(32) NOT NULL 		--//  ENCODE zstd
	,country_code VARCHAR(8)  		--//  ENCODE zstd
	,product_source_id VARCHAR(18)  		--//  ENCODE zstd
	,modify_dt TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,modify_id VARCHAR(18)  		--//  ENCODE zstd
	,product_name VARCHAR(80)  		--//  ENCODE zstd
	,product_type VARCHAR(255)  		--//  ENCODE zstd
	,therapeutic_area_name VARCHAR(255)  		--//  ENCODE zstd
	,parent_product_key VARCHAR(80)  		--//  ENCODE zstd
	,company_product_flag numeric(18,0)		--//  ENCODE delta // INTEGER  
	,business_unit VARCHAR(4099)  		--//  ENCODE zstd
	,consumer_site VARCHAR(255)  		--//  ENCODE zstd
	,product_info VARCHAR(255)  		--//  ENCODE zstd
	,therapeutic_class VARCHAR(255)  		--//  ENCODE zstd
	,manufacturer VARCHAR(255)  		--//  ENCODE zstd
	,description VARCHAR(255)  		--//  ENCODE zstd
	,franchise VARCHAR(4099)  		--//  ENCODE zstd
	,require_key_message numeric(18,0)		--//  ENCODE delta // INTEGER  
	,controlled_substance numeric(18,0)		--//  ENCODE delta // INTEGER  
	,sample_quantity_picklist VARCHAR(1030)  		--//  ENCODE zstd
	,display_order NUMERIC(5,0)  		--//  ENCODE delta
	,no_metrics numeric(18,0)		--//  ENCODE delta // INTEGER  
	,distributor VARCHAR(255)  		--//  ENCODE zstd
	,sample_quantity_bound numeric(18,0)		--//  ENCODE delta // INTEGER  
	,sample_u_m VARCHAR(255)  		--//  ENCODE zstd
	,no_details numeric(18,0)		--//  ENCODE delta // INTEGER  
	,restricted numeric(18,0)		--//  ENCODE delta // INTEGER  
	,no_cycle_plans numeric(18,0)		--//  ENCODE delta // INTEGER  
	,sku_id VARCHAR(25)  		--//  ENCODE zstd
	,biz_sub_unit VARCHAR(255)  		--//  ENCODE zstd
	,biz_unit VARCHAR(255)  		--//  ENCODE zstd
	,product_sector VARCHAR(1030)  		--//  ENCODE zstd
	,external_id VARCHAR(25)  		--//  ENCODE zstd
	,record_type_name VARCHAR(80)  		--//  ENCODE zstd
	,deleted_flag numeric(18,0)		--//  ENCODE delta // INTEGER  
	,common_brand_name VARCHAR(255)  		--//  ENCODE zstd
	,common_disease_area_name VARCHAR(255)  		--//  ENCODE zstd
	,common_therapeutic_area VARCHAR(255)  		--//  ENCODE zstd
	,shc_sector VARCHAR(255)  		--//  ENCODE zstd
	,shc_strategic_group VARCHAR(255)  		--//  ENCODE zstd
	,shc_franchise VARCHAR(255)  		--//  ENCODE zstd
	,inserted_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,updated_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,shc_brand VARCHAR(255)  		--//  ENCODE zstd
	,PRIMARY KEY (product_indication_key)
)

cluster by (product_indication_key)
		--// SORTKEY ( 
		--// 	product_indication_key
		--// 	)
;		--// ;
--DROP TABLE HCPOSEEDW_INTEGRATION.dim_profile;
CREATE OR REPLACE TABLE HCPOSEEDW_INTEGRATION.DIM_PROFILE		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE HCPOSEEDW_INTEGRATION.dim_profile
(
	profile_key VARCHAR(32) NOT NULL 		--//  ENCODE zstd
	,country_code VARCHAR(2)  		--//  ENCODE zstd
	,profile_source_id VARCHAR(18)  		--//  ENCODE zstd
	,modified_dt TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,modified_id VARCHAR(18)  		--//  ENCODE zstd
	,profile_name VARCHAR(255)  		--//  ENCODE zstd
	,function_name VARCHAR(255)  		--//  ENCODE zstd
	,created_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,created_by_id VARCHAR(30)  		--//  ENCODE zstd
	,type VARCHAR(43)  		--//  ENCODE zstd
	,userlicense_source_id VARCHAR(18)  		--//  ENCODE zstd
	,usertype VARCHAR(43)  		--//  ENCODE zstd
	,description VARCHAR(255)  		--//  ENCODE zstd
	,inserted_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,updated_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,PRIMARY KEY (profile_key)
)
		--// DISTSTYLE KEY
cluster by (profile_key)
;
--DROP TABLE HCPOSEEDW_INTEGRATION.dim_profile_iconnect;
CREATE OR REPLACE TABLE HCPOSEEDW_INTEGRATION.DIM_PROFILE_ICONNECT		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE HCPOSEEDW_INTEGRATION.dim_profile_iconnect
(
	profile_key VARCHAR(32)  		--//  ENCODE lzo
	,country_code VARCHAR(2)  		--//  ENCODE lzo
	,profile_source_id VARCHAR(18)  		--//  ENCODE lzo
	,modified_dt TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,modified_id VARCHAR(18)  		--//  ENCODE lzo
	,profile_name VARCHAR(255)  		--//  ENCODE lzo
	,function_name VARCHAR(255)  		--//  ENCODE lzo
	,created_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,created_by_id VARCHAR(30)  		--//  ENCODE lzo
	,type VARCHAR(43)  		--//  ENCODE lzo
	,userlicense_source_id VARCHAR(18)  		--//  ENCODE lzo
	,usertype VARCHAR(43)  		--//  ENCODE lzo
	,description VARCHAR(255)  		--//  ENCODE lzo
	,inserted_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,updated_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)
		--// DISTSTYLE KEY
cluster by (profile_key)
;
--DROP TABLE HCPOSEEDW_INTEGRATION.dim_remote_meeting;
CREATE OR REPLACE TABLE HCPOSEEDW_INTEGRATION.DIM_REMOTE_MEETING		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE HCPOSEEDW_INTEGRATION.dim_remote_meeting
(
	remote_meeting_key VARCHAR(200)  		--//  ENCODE lzo
	,country_code VARCHAR(10)  		--//  ENCODE lzo
	,meeting_id VARCHAR(18)  		--//  ENCODE lzo
	,meeting_name VARCHAR(255)  		--//  ENCODE lzo
	,scheduled_flag VARCHAR(10)  		--//  ENCODE lzo
	,scheduled_datetime TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,latest_start_datetime TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,attendance_report_process_status VARCHAR(200)  		--//  ENCODE lzo
	,meeting_outcome_status VARCHAR(255)  		--//  ENCODE lzo
	,remote_meeting_source_id VARCHAR(18)  		--//  ENCODE lzo
	,owner_source_id VARCHAR(18)  		--//  ENCODE lzo
	,description VARCHAR(1000)  		--//  ENCODE lzo
	,num_of_attendee NUMERIC(18,1)  		--//  ENCODE az64
	,inserted_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)

;
--DROP TABLE HCPOSEEDW_INTEGRATION.edw_isight_dim_employee_snapshot_xref;
CREATE OR REPLACE TABLE HCPOSEEDW_INTEGRATION.EDW_ISIGHT_DIM_EMPLOYEE_SNAPSHOT_XREF		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE HCPOSEEDW_INTEGRATION.edw_isight_dim_employee_snapshot_xref
(
	year numeric(18,0)		--//  ENCODE delta // INTEGER  
	,month numeric(18,0)		--//  ENCODE delta // INTEGER  
	,employee_key VARCHAR(32)  		--//  ENCODE zstd
	,country_code VARCHAR(8)  		--//  ENCODE zstd
	,employee_source_id VARCHAR(18)  		--//  ENCODE zstd
	,modified_dt TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,modified_id VARCHAR(18)  		--//  ENCODE zstd
	,employee_name VARCHAR(121)  		--//  ENCODE zstd
	,employee_wwid VARCHAR(20)  		--//  ENCODE zstd
	,mobile_phone VARCHAR(40)  		--//  ENCODE zstd
	,email_id VARCHAR(128)  		--//  ENCODE zstd
	,username VARCHAR(80)  		--//  ENCODE zstd
	,nickname VARCHAR(40)  		--//  ENCODE zstd
	,local_employee_number VARCHAR(20)  		--//  ENCODE zstd
	,profile_id VARCHAR(18)  		--//  ENCODE zstd
	,profile_name VARCHAR(255)  		--//  ENCODE zstd
	,function_name VARCHAR(255)  		--//  ENCODE zstd
	,employee_profile_id VARCHAR(18)  		--//  ENCODE zstd
	,company_name VARCHAR(80)  		--//  ENCODE zstd
	,division_name VARCHAR(80)  		--//  ENCODE zstd
	,department_name VARCHAR(80)  		--//  ENCODE zstd
	,country_name VARCHAR(80)  		--//  ENCODE zstd
	,address VARCHAR(255)  		--//  ENCODE zstd
	,alias VARCHAR(18)  		--//  ENCODE zstd
	,timezonesidkey VARCHAR(40)  		--//  ENCODE zstd
	,user_role_source_id VARCHAR(18)  		--//  ENCODE zstd
	,receives_info_emails numeric(18,0)		--//  ENCODE delta // INTEGER  
	,federation_identifier VARCHAR(512)  		--//  ENCODE zstd
	,last_ipad_sync TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,user_license VARCHAR(1300)  		--//  ENCODE zstd
	,title VARCHAR(1300)  		--//  ENCODE zstd
	,phone VARCHAR(43)  		--//  ENCODE zstd
	,last_login_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,region VARCHAR(1300)  		--//  ENCODE zstd
	,profile_group_ap VARCHAR(1030)  		--//  ENCODE zstd
	,manager_name VARCHAR(80)  		--//  ENCODE zstd
	,manager_wwid VARCHAR(18)  		--//  ENCODE zstd
	,active_flag NUMERIC(2,0)  		--//  ENCODE delta
	,my_organization_code VARCHAR(18)  		--//  ENCODE zstd
	,my_organization_name VARCHAR(80)  		--//  ENCODE zstd
	,organization_l1_code VARCHAR(18)  		--//  ENCODE zstd
	,organization_l1_name VARCHAR(80)  		--//  ENCODE zstd
	,organization_l2_code VARCHAR(18)  		--//  ENCODE zstd
	,organization_l2_name VARCHAR(80)  		--//  ENCODE zstd
	,organization_l3_code VARCHAR(18)  		--//  ENCODE zstd
	,organization_l3_name VARCHAR(80)  		--//  ENCODE zstd
	,organization_l4_code VARCHAR(18)  		--//  ENCODE zstd
	,organization_l4_name VARCHAR(80)  		--//  ENCODE zstd
	,organization_l5_code VARCHAR(18)  		--//  ENCODE zstd
	,organization_l5_name VARCHAR(80)  		--//  ENCODE zstd
	,organization_l6_code VARCHAR(18)  		--//  ENCODE zstd
	,organization_l6_name VARCHAR(80)  		--//  ENCODE zstd
	,organization_l7_code VARCHAR(18)  		--//  ENCODE zstd
	,organization_l7_name VARCHAR(80)  		--//  ENCODE zstd
	,organization_l8_code VARCHAR(18)  		--//  ENCODE zstd
	,organization_l8_name VARCHAR(80)  		--//  ENCODE zstd
	,organization_l9_code VARCHAR(18)  		--//  ENCODE zstd
	,organization_l9_name VARCHAR(80)  		--//  ENCODE zstd
	,common_organization_l1_code VARCHAR(18)  		--//  ENCODE zstd
	,common_organization_l1_name VARCHAR(80)  		--//  ENCODE zstd
	,common_organization_l2_code VARCHAR(18)  		--//  ENCODE zstd
	,common_organization_l2_name VARCHAR(80)  		--//  ENCODE zstd
	,common_organization_l3_code VARCHAR(18)  		--//  ENCODE zstd
	,common_organization_l3_name VARCHAR(80)  		--//  ENCODE zstd
	,inserted_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,updated_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
)

;
--DROP TABLE HCPOSEEDW_INTEGRATION.edw_isight_licenses;
CREATE OR REPLACE TABLE HCPOSEEDW_INTEGRATION.EDW_ISIGHT_LICENSES		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE HCPOSEEDW_INTEGRATION.edw_isight_licenses
(
	year NUMERIC(18,0)  		--//  ENCODE delta
	,country VARCHAR(255)  		--//  ENCODE zstd
	,sector VARCHAR(255)  		--//  ENCODE zstd
	,qty NUMERIC(18,0)  		--//  ENCODE delta
	,licensetype VARCHAR(255)  		--//  ENCODE zstd
	,inserted_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,updated_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
)

;
--DROP TABLE HCPOSEEDW_INTEGRATION.edw_isight_sector_mapping;
CREATE OR REPLACE TABLE HCPOSEEDW_INTEGRATION.EDW_ISIGHT_SECTOR_MAPPING		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE HCPOSEEDW_INTEGRATION.edw_isight_sector_mapping
(
	country VARCHAR(256)  		--//  ENCODE zstd
	,company VARCHAR(256)  		--//  ENCODE zstd
	,division VARCHAR(256)  		--//  ENCODE zstd
	,sector VARCHAR(256)  		--//  ENCODE zstd
	,inserted_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,updated_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
)

;
--DROP TABLE HCPOSEEDW_INTEGRATION.fact_call_detail;
CREATE OR REPLACE TABLE HCPOSEEDW_INTEGRATION.FACT_CALL_DETAIL		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE HCPOSEEDW_INTEGRATION.fact_call_detail
(
	country_key VARCHAR(18) NOT NULL 		--//  ENCODE zstd
	,hcp_key VARCHAR(132) NOT NULL 		--//  ENCODE zstd
	,hco_key VARCHAR(132) NOT NULL 		--//  ENCODE zstd
	,employee_key VARCHAR(132) NOT NULL 		--//  ENCODE zstd
	,profile_key VARCHAR(132) NOT NULL 		--//  ENCODE zstd
	,organization_key VARCHAR(132) NOT NULL 		--//  ENCODE zstd
	,call_date_key numeric(18,0) NOT NULL 		--//  ENCODE delta // INTEGER 
	,call_date_time TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,call_entry_datetime TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,call_mobile_last_modified_date_time TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,utc_call_date_time TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,utc_call_entry_time TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,call_status_type VARCHAR(1255)  		--//  ENCODE zstd
	,call_channel_type VARCHAR(15)  		--//  ENCODE zstd
	,call_type VARCHAR(1255)  		--//  ENCODE zstd
	,call_duration NUMERIC(18,0)  		--//  ENCODE delta
	,call_clm_flag numeric(18,0)		--//  ENCODE delta // INTEGER  
	,parent_call_flag numeric(18,0)		--//  ENCODE delta // INTEGER  
	,attendee_type VARCHAR(1255)  		--//  ENCODE zstd
	,call_comments VARCHAR(32000)  		--//  ENCODE zstd
	,next_call_notes VARCHAR(32000)  		--//  ENCODE zstd
	,pre_call_notes VARCHAR(32000)  		--//  ENCODE zstd
	,manager_call_comment VARCHAR(32768)  		--//  ENCODE zstd
	,last_activity_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,sample_card VARCHAR(115)  		--//  ENCODE zstd
	,account_plan VARCHAR(118)  		--//  ENCODE zstd
	,mobile_id VARCHAR(1103)  		--//  ENCODE zstd
	,significant_event numeric(18,0)		--//  ENCODE delta // INTEGER  
	,location VARCHAR(1128)  		--//  ENCODE zstd
	,signature_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,signature VARCHAR(32330)  		--//  ENCODE zstd
	,territory VARCHAR(1103)  		--//  ENCODE zstd
	,attendees NUMERIC(3,0)  		--//  ENCODE delta
	,detailed_products VARCHAR(1255)  		--//  ENCODE zstd
	,parent_call_source_id VARCHAR(118)  		--//  ENCODE zstd
	,user_name VARCHAR(118)  		--//  ENCODE zstd
	,medical_event VARCHAR(118)  		--//  ENCODE zstd
	,is_sampled_call numeric(18,0)		--//  ENCODE delta // INTEGER  
	,presentations VARCHAR(1530)  		--//  ENCODE zstd
	,product_priority_1 VARCHAR(118)  		--//  ENCODE zstd
	,product_priority_2 VARCHAR(118)  		--//  ENCODE zstd
	,product_priority_3 VARCHAR(118)  		--//  ENCODE zstd
	,product_priority_4 VARCHAR(118)  		--//  ENCODE zstd
	,product_priority_5 VARCHAR(118)  		--//  ENCODE zstd
	,attendee_list VARCHAR(32768)  		--//  ENCODE zstd
	,msl_interaction_notes VARCHAR(32768)  		--//  ENCODE zstd
	,sea_call_type VARCHAR(11300)  		--//  ENCODE zstd
	,interaction_mode VARCHAR(1255)  		--//  ENCODE zstd
	,hcp_kol_initiated numeric(18,0)		--//  ENCODE delta // INTEGER  
	,msl_interaction_type VARCHAR(1255)  		--//  ENCODE zstd
	,call_objective VARCHAR(1255)  		--//  ENCODE zstd
	,submission_delay NUMERIC(18,0)  		--//  ENCODE delta
	,region VARCHAR(11300)  		--//  ENCODE zstd
	,md_hsp_admin VARCHAR(1255)  		--//  ENCODE zstd
	,hsp_minutes VARCHAR(1255)  		--//  ENCODE zstd
	,ortho_on_call_case numeric(18,0)		--//  ENCODE delta // INTEGER  
	,ortho_volunteer_case numeric(18,0)		--//  ENCODE delta // INTEGER  
	,md_calc1 NUMERIC(18,2)  		--//  ENCODE delta
	,md_calculate_non_case_time NUMERIC(18,2)  		--//  ENCODE delta
	,md_calculated_hours_field NUMERIC(18,2)  		--//  ENCODE delta
	,md_casedeployment VARCHAR(1255)  		--//  ENCODE zstd
	,md_case_coverage_12_hours VARCHAR(1255)  		--//  ENCODE zstd
	,md_product_discussion VARCHAR(1255)  		--//  ENCODE zstd
	,md_concurrent_call numeric(18,0)		--//  ENCODE delta // INTEGER  
	,courtesy_call VARCHAR(1255)  		--//  ENCODE zstd
	,md_in_service VARCHAR(1255)  		--//  ENCODE zstd
	,md_kol_course_discussion VARCHAR(1255)  		--//  ENCODE zstd
	,kol_minutes VARCHAR(1255)  		--//  ENCODE zstd
	,other_activities_time_12_hours NUMERIC(18,0)  		--//  ENCODE delta
	,other_in_field_activities NUMERIC(4,0)  		--//  ENCODE delta
	,md_overseas_workshop_visit VARCHAR(1255)  		--//  ENCODE zstd
	,md_ra_activities2 VARCHAR(1255)  		--//  ENCODE zstd
	,sales_activity VARCHAR(14399)  		--//  ENCODE zstd
	,sales_time_12_hours NUMERIC(18,0)  		--//  ENCODE delta
	,time_spent VARCHAR(1255)  		--//  ENCODE zstd
	,time_spent_on_other_activities_simp VARCHAR(1255)  		--//  ENCODE zstd
	,time_spent_on_sales_activity VARCHAR(1255)  		--//  ENCODE zstd
	,md_time_spent_on_a_call NUMERIC(18,2)  		--//  ENCODE delta
	,md_case_type VARCHAR(1255)  		--//  ENCODE zstd
	,md_sets_activities VARCHAR(1255)  		--//  ENCODE zstd
	,md_time_spent_on_case VARCHAR(1255)  		--//  ENCODE zstd
	,time_spent_on_other_activities NUMERIC(18,2)  		--//  ENCODE delta
	,time_spent_per_call NUMERIC(18,2)  		--//  ENCODE delta
	,md_case_conducted_in_hospital VARCHAR(1255)  		--//  ENCODE zstd
	,calculated_field_2 NUMERIC(18,2)  		--//  ENCODE delta
	,calculated_hours_3 NUMERIC(18,2)  		--//  ENCODE delta
	,call_planned numeric(18,0)		--//  ENCODE delta // INTEGER  
	,call_submission_day NUMERIC(18,1)  		--//  ENCODE delta
	,case_coverage NUMERIC(18,2)  		--//  ENCODE delta
	,day_of_week VARCHAR(11300)  		--//  ENCODE zstd
	,md_minutes VARCHAR(1255)  		--//  ENCODE zstd
	,md_in_or_ot numeric(18,0)		--//  ENCODE delta // INTEGER  
	,md_d_call_type VARCHAR(1255)  		--//  ENCODE zstd
	,md_hours VARCHAR(1255)  		--//  ENCODE zstd
	,call_record_type_name VARCHAR(180)  		--//  ENCODE zstd
	,call_name VARCHAR(180)  		--//  ENCODE zstd
	,parent_call_name VARCHAR(180)  		--//  ENCODE zstd
	,submitted_by_mobile numeric(18,0)		--//  ENCODE delta // INTEGER  
	,call_source_id VARCHAR(118)  		--//  ENCODE zstd
	,product_indication_key VARCHAR(132)  		--//  ENCODE zstd
	,call_detail_priority NUMERIC(2,0)  		--//  ENCODE delta
	,call_detail_type VARCHAR(1255)  		--//  ENCODE zstd
	,call_discussion_record_type_source_id VARCHAR(118)  		--//  ENCODE zstd
	,comments VARCHAR(1255)  		--//  ENCODE zstd
	,discussion_topics VARCHAR(1255)  		--//  ENCODE zstd
	,discussion_type VARCHAR(1255)  		--//  ENCODE zstd
	,call_discussion_type VARCHAR(1255)  		--//  ENCODE zstd
	,effectiveness VARCHAR(1255)  		--//  ENCODE zstd
	,follow_up_activity VARCHAR(1255)  		--//  ENCODE zstd
	,outcomes VARCHAR(1255)  		--//  ENCODE zstd
	,follow_up_additional_info VARCHAR(1255)  		--//  ENCODE zstd
	,follow_up_date DATE  		--//  ENCODE delta
	,materials_used VARCHAR(1255)  		--//  ENCODE zstd
	,call_detail_source_id VARCHAR(118)  		--//  ENCODE zstd
	,call_detail_name VARCHAR(183)  		--//  ENCODE zstd
	,call_discussion_source_id VARCHAR(118)  		--//  ENCODE zstd
	,call_discussion_name VARCHAR(183)  		--//  ENCODE zstd
	,call_modify_dt TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,call_modify_id VARCHAR(118)  		--//  ENCODE zstd
	,call_detail_modify_dt TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,call_detail_modify_id VARCHAR(118)  		--//  ENCODE zstd
	,call_discussion_modify_dt TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,call_discussion_modify_id VARCHAR(118)  		--//  ENCODE zstd
	,inserted_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,updated_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,remote_meeting_key VARCHAR(32)  		--//  ENCODE zstd
	,attendee_remote_meeting_id VARCHAR(20)  		--//  ENCODE zstd
	,PRIMARY KEY (country_key, hcp_key, hco_key, employee_key, profile_key, organization_key, call_date_key)
)
		--// DISTSTYLE KEY
cluster by (hcp_key)
;
--DROP TABLE HCPOSEEDW_INTEGRATION.fact_call_key_message;
CREATE OR REPLACE TABLE HCPOSEEDW_INTEGRATION.FACT_CALL_KEY_MESSAGE		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE HCPOSEEDW_INTEGRATION.fact_call_key_message
(
	country_key VARCHAR(15) NOT NULL 		--//  ENCODE zstd
	,hcp_key VARCHAR(32) NOT NULL 		--//  ENCODE zstd
	,hco_key VARCHAR(80) NOT NULL 		--//  ENCODE zstd
	,employee_key VARCHAR(32) NOT NULL 		--//  ENCODE zstd
	,organization_key VARCHAR(32) NOT NULL 		--//  ENCODE zstd
	,profile_key VARCHAR(32) NOT NULL 		--//  ENCODE zstd
	,product_indication_key VARCHAR(32) NOT NULL 		--//  ENCODE zstd
	,call_date_key numeric(18,0) NOT NULL 		--//  ENCODE delta // INTEGER 
	,key_message_key VARCHAR(256)  		--//  ENCODE zstd
	,hcp_source_id VARCHAR(18)  		--//  ENCODE zstd
	,call_attendance_type VARCHAR(255)  		--//  ENCODE zstd
	,call_source_id VARCHAR(18)  		--//  ENCODE zstd
	,call_date DATE  		--//  ENCODE delta
	,category VARCHAR(255)  		--//  ENCODE zstd
	,clm_id VARCHAR(100)  		--//  ENCODE zstd
	,contact_source_id VARCHAR(18)  		--//  ENCODE zstd
	,call_key_message_created_by VARCHAR(18)  		--//  ENCODE zstd
	,call_key_message_created_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,call_key_message_detail_group VARCHAR(18)  		--//  ENCODE zstd
	,call_key_message_clm_display_order NUMERIC(18,0)  		--//  ENCODE delta
	,call_key_message_duration NUMERIC(18,2)  		--//  ENCODE delta
	,call_key_message_entity_reference_id VARCHAR(100)  		--//  ENCODE zstd
	,call_key_message_source_id VARCHAR(18)  		--//  ENCODE zstd
	,call_key_message_parent_call_flag NUMERIC(18,0)  		--//  ENCODE delta
	,call_key_message_clm_presentation_name VARCHAR(400)  		--//  ENCODE zstd
	,key_message_source_id VARCHAR(18)  		--//  ENCODE zstd
	,call_key_message_last_modified_by VARCHAR(18)  		--//  ENCODE zstd
	,call_key_message_last_modified_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,call_key_message_name VARCHAR(80)  		--//  ENCODE zstd
	,call_key_message_presentation_source_id VARCHAR(100)  		--//  ENCODE zstd
	,product_source_id VARCHAR(18)  		--//  ENCODE zstd
	,call_key_message_reaction VARCHAR(255)  		--//  ENCODE zstd
	,call_key_message_segment VARCHAR(80)  		--//  ENCODE zstd
	,call_key_message_slide_version VARCHAR(100)  		--//  ENCODE zstd
	,call_key_message_start_time TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,call_key_message_modify_timestamp TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,call_key_message_source_user VARCHAR(18)  		--//  ENCODE zstd
	,call_key_message_vehicle VARCHAR(255)  		--//  ENCODE zstd
	,call_detail_source_id VARCHAR(18)  		--//  ENCODE zstd
	,call_discussion_source_id VARCHAR(18)  		--//  ENCODE zstd
	,call_key_message_data_inserted_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,call_key_message_data_updated_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,PRIMARY KEY (country_key, hcp_key, hco_key, employee_key, organization_key, profile_key)
)

;
--DROP TABLE HCPOSEEDW_INTEGRATION.fact_coaching_report;
CREATE OR REPLACE TABLE HCPOSEEDW_INTEGRATION.FACT_COACHING_REPORT		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE HCPOSEEDW_INTEGRATION.fact_coaching_report
(
	country_key VARCHAR(8)  		--//  ENCODE zstd
	,employee_key VARCHAR(32)  		--//  ENCODE zstd
	,manager_employee_key VARCHAR(32)  		--//  ENCODE zstd
	,coaching_report_date_key numeric(18,0)		--//  ENCODE delta // INTEGER  
	,coaching_report_source_id VARCHAR(100)  		--//  ENCODE zstd
	,ownerid VARCHAR(18)  		--//  ENCODE zstd
	,isdeleted VARCHAR(5)  		--//  ENCODE zstd
	,name VARCHAR(80)  		--//  ENCODE zstd
	,recordtypeid VARCHAR(18)  		--//  ENCODE zstd
	,createddate TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,createdbyid VARCHAR(18)  		--//  ENCODE zstd
	,lastmodifieddate TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,lastmodifiedbyid VARCHAR(18)  		--//  ENCODE zstd
	,systemmodstamp TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,lastactivitydate TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,mayedit VARCHAR(20)  		--//  ENCODE zstd
	,mobile_id VARCHAR(1000)  		--//  ENCODE zstd
	,manager VARCHAR(18)  		--//  ENCODE zstd
	,employee VARCHAR(18)  		--//  ENCODE zstd
	,review_date DATE  		--//  ENCODE delta
	,review_period VARCHAR(20)  		--//  ENCODE zstd
	,status VARCHAR(255)  		--//  ENCODE zstd
	,overall_rating VARCHAR(100)  		--//  ENCODE zstd
	,jj_core_country_code VARCHAR(10)  		--//  ENCODE zstd
	,jj_core_lock VARCHAR(20)  		--//  ENCODE zstd
	,jj_core_no_of_visits VARCHAR(10)  		--//  ENCODE zstd
	,employee_review_and_ackCURRENT_TIMESTAMP()ledged VARCHAR(5)  		--// 	,employee_review_and_acknowledged VARCHAR(5)   ENCODE zstd //  ENCODE zstd
	,employee_comments VARCHAR(2000)  		--//  ENCODE zstd
	,simp_manager_comments VARCHAR(65535)  		--//  ENCODE zstd
	,simp_objectives VARCHAR(65535)  		--//  ENCODE zstd
	,simp_rep_comments_long VARCHAR(65535)  		--//  ENCODE zstd
	,simp_sg_overall_rating NUMERIC(18,1)  		--//  ENCODE delta
	,simp_long_comments VARCHAR(5000)  		--//  ENCODE zstd
	,kCURRENT_TIMESTAMP()ledge_strategy_overall_rating NUMERIC(18,1)  		--// 	,knowledge_strategy_overall_rating NUMERIC(18,1)   ENCODE delta //  ENCODE delta
	,selling_skills_overall_rating NUMERIC(18,1)  		--//  ENCODE delta
	,my_call_type VARCHAR(255)  		--//  ENCODE zstd
	,my_location VARCHAR(255)  		--//  ENCODE zstd
	,id_overall_rating NUMERIC(18,1)  		--//  ENCODE delta
	,vn_md_overall_rating_med NUMERIC(18,1)  		--//  ENCODE delta
	,agreed_next_steps VARCHAR(2000)  		--//  ENCODE zstd
	,coaching_for_field_visits VARCHAR(5)  		--//  ENCODE zstd
	,customer_interactions VARCHAR(2000)  		--//  ENCODE zstd
	,date_of_review_concluded DATE  		--//  ENCODE delta
	,general_observations_and_comments VARCHAR(2000)  		--//  ENCODE zstd
	,manager_feedback_completed VARCHAR(5)  		--//  ENCODE zstd
	,number_of_coaching_days VARCHAR(255)  		--//  ENCODE zstd
	,second_line_manager VARCHAR(18)  		--//  ENCODE zstd
	,submission_to_date NUMERIC(18,1)  		--//  ENCODE delta
	,relatedcoachingreport VARCHAR(255)  		--//  ENCODE zstd
	,updated_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,inserted_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
)

;
--DROP TABLE HCPOSEEDW_INTEGRATION.fact_cycle_plan;
CREATE OR REPLACE TABLE HCPOSEEDW_INTEGRATION.FACT_CYCLE_PLAN		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE HCPOSEEDW_INTEGRATION.fact_cycle_plan
(
	country_key VARCHAR(8) NOT NULL 		--//  ENCODE zstd
	,employee_key VARCHAR(255) NOT NULL 		--//  ENCODE zstd
	,organization_key VARCHAR(255) NOT NULL 		--//  ENCODE zstd
	,territory_name VARCHAR(255)  		--//  ENCODE zstd
	,start_date_key NUMERIC(38,0) NOT NULL 		--//  ENCODE delta
	,end_date_key NUMERIC(38,0) NOT NULL 		--//  ENCODE delta
	,active_flag numeric(18,0)		--//  ENCODE delta // INTEGER  
	,manager_name VARCHAR(255)  		--//  ENCODE zstd
	,approver_name VARCHAR(255)  		--//  ENCODE zstd
	,close_out_flag numeric(18,0)		--//  ENCODE delta // INTEGER  
	,ready_for_approval_flag numeric(18,0)		--//  ENCODE delta // INTEGER  
	,status_type VARCHAR(255)  		--//  ENCODE zstd
	,cycle_plan_name VARCHAR(255)  		--//  ENCODE zstd
	,channel_type VARCHAR(255)  		--//  ENCODE zstd
	,actual_call_cnt_cp NUMERIC(38,0)  		--//  ENCODE delta
	,planned_call_cnt_cp NUMERIC(38,0)  		--//  ENCODE delta
	,cycle_plan_external_id VARCHAR(255)  		--//  ENCODE zstd
	,manager VARCHAR(255)  		--//  ENCODE zstd
	,cp_approval_time TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,number_of_targets NUMERIC(38,0)  		--//  ENCODE delta
	,number_of_cfa_100_targets NUMERIC(38,0)  		--//  ENCODE delta
	,cycle_plan_attainment_cptarget NUMERIC(38,1)  		--//  ENCODE delta
	,mid_date DATE  		--//  ENCODE delta
	,hcp_product_achieved_100 NUMERIC(38,0)  		--//  ENCODE delta
	,hcp_products_planned NUMERIC(38,0)  		--//  ENCODE delta
	,cpa_100 NUMERIC(38,1)  		--//  ENCODE zstd
	,hcp_key VARCHAR(255) NOT NULL 		--//  ENCODE zstd
	,hco_key VARCHAR(255) NOT NULL 		--//  ENCODE zstd
	,scheduled_call_count NUMERIC(38,0)  		--//  ENCODE delta
	,actual_call_count NUMERIC(38,0)  		--//  ENCODE delta
	,planned_call_count NUMERIC(38,0)  		--//  ENCODE delta
	,remaining_call_count NUMERIC(38,0)  		--//  ENCODE delta
	,cycle_plan_target_attainment NUMERIC(38,0)  		--//  ENCODE delta
	,original_planned_calls NUMERIC(38,0)  		--//  ENCODE delta
	,total_actual_calls NUMERIC(38,0)  		--//  ENCODE delta
	,total_attainment NUMERIC(38,0)  		--//  ENCODE delta
	,total_planned_calls NUMERIC(38,0)  		--//  ENCODE delta
	,cycle_plan_target_external_id VARCHAR(255)  		--//  ENCODE zstd
	,total_scheduled_calls NUMERIC(38,0)  		--//  ENCODE delta
	,remaining NUMERIC(38,0)  		--//  ENCODE delta
	,total_remaining NUMERIC(38,0)  		--//  ENCODE delta
	,total_remaining_schedule NUMERIC(38,0)  		--//  ENCODE delta
	,primary_parent VARCHAR(3000)  		--//  ENCODE zstd
	,accounts_specialty_1 VARCHAR(3000)  		--//  ENCODE zstd
	,account_source_id VARCHAR(3000)  		--//  ENCODE zstd
	,cpt_cfa_100 NUMERIC(38,0)  		--//  ENCODE delta
	,cpt_cfa_66 NUMERIC(38,0)  		--//  ENCODE delta
	,cpt_cfa_33 NUMERIC(38,0)  		--//  ENCODE delta
	,number_of_cfa_100_details NUMERIC(38,0)  		--//  ENCODE delta
	,number_of_product_details NUMERIC(38,0)  		--//  ENCODE delta
	,product_indication_key VARCHAR(255) NOT NULL 		--//  ENCODE zstd
	,classification_type VARCHAR(255)  		--//  ENCODE zstd
	,planned_call_detail_count NUMERIC(38,0)  		--//  ENCODE delta
	,scheduled_call_detail_count NUMERIC(38,0)  		--//  ENCODE delta
	,actual_call_detail_count NUMERIC(38,0)  		--//  ENCODE delta
	,remaining_call_detail_count NUMERIC(38,0)  		--//  ENCODE delta
	,cycle_plan_detail_attainment NUMERIC(38,0)  		--//  ENCODE delta
	,total_actual_details NUMERIC(38,0)  		--//  ENCODE delta
	,total_attainment_details NUMERIC(38,0)  		--//  ENCODE delta
	,total_planned_details NUMERIC(38,0)  		--//  ENCODE delta
	,total_scheduled_details NUMERIC(38,0)  		--//  ENCODE delta
	,total_remaining_details NUMERIC(38,0)  		--//  ENCODE delta
	,cfa_100 NUMERIC(38,0)  		--//  ENCODE delta
	,cfa_33 NUMERIC(38,0)  		--//  ENCODE delta
	,cfa_66 NUMERIC(38,0)  		--//  ENCODE delta
	,cycle_plan_type VARCHAR(255) NOT NULL 		--//  ENCODE zstd
	,cycle_plan_source_id VARCHAR(255)  		--//  ENCODE zstd
	,cycle_plan_target_source_id VARCHAR(255)  		--//  ENCODE zstd
	,cycle_plan_detail_source_id VARCHAR(255)  		--//  ENCODE zstd
	,cycle_plan_target_name VARCHAR(255)  		--//  ENCODE zstd
	,cycle_plan_detail_name VARCHAR(255)  		--//  ENCODE zstd
	,cycle_plan_modify_dt TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,cycle_plan_modify_id VARCHAR(255)  		--//  ENCODE zstd
	,cycle_plan_target_modify_dt TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,cycle_plan_target_modify_id VARCHAR(255)  		--//  ENCODE zstd
	,cycle_plan_detail_modify_dt TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,cycle_plan_detail_modify_id VARCHAR(255)  		--//  ENCODE zstd
	,cycle_plan_indicator VARCHAR(255)  		--//  ENCODE zstd
	,classification VARCHAR(1300)  		--//  ENCODE zstd
	,inserted_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,updated_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,PRIMARY KEY (country_key, employee_key, organization_key, start_date_key, end_date_key, hcp_key, hco_key, cycle_plan_type, product_indication_key)
)

		--// SORTKEY ( 
		--// 	cycle_plan_type
		--// 	)
;		--// ;
--DROP TABLE HCPOSEEDW_INTEGRATION.fact_timeoff_territory;
CREATE OR REPLACE TABLE HCPOSEEDW_INTEGRATION.FACT_TIMEOFF_TERRITORY		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE HCPOSEEDW_INTEGRATION.fact_timeoff_territory
(
	country_key VARCHAR(8) NOT NULL 		--//  ENCODE zstd
	,employee_key VARCHAR(32) NOT NULL 		--//  ENCODE zstd
	,start_date_key VARCHAR(32) NOT NULL 		--//  ENCODE zstd
	,end_date_key VARCHAR(32) NOT NULL 		--//  ENCODE zstd
	,territory_name VARCHAR(255)  		--//  ENCODE zstd
	,reason_type VARCHAR(255)  		--//  ENCODE zstd
	,hours_off VARCHAR(255)  		--//  ENCODE zstd
	,time_type VARCHAR(255)  		--//  ENCODE zstd
	,start_time VARCHAR(255)  		--//  ENCODE zstd
	,hours NUMERIC(18,0)  		--//  ENCODE delta
	,comments VARCHAR(800)  		--//  ENCODE zstd
	,sea_time_on_time_off VARCHAR(255)  		--//  ENCODE zstd
	,sea_frml_hours_on NUMERIC(18,0)  		--//  ENCODE delta
	,sea_frml_non_working_hours_off NUMERIC(18,0)  		--//  ENCODE delta
	,mobile_id VARCHAR(103)  		--//  ENCODE zstd
	,sea_frml_total_work_days NUMERIC(18,0)  		--//  ENCODE delta
	,sea_frml_planned_work_days NUMERIC(18,0)  		--//  ENCODE delta
	,time_on VARCHAR(255)  		--//  ENCODE zstd
	,user_name VARCHAR(255)  		--//  ENCODE zstd
	,user_profile VARCHAR(1030)  		--//  ENCODE zstd
	,calculatedhours_off NUMERIC(18,0)  		--//  ENCODE delta
	,total_time_off NUMERIC(18,0)  		--//  ENCODE delta
	,sm_reason VARCHAR(255)  		--//  ENCODE zstd
	,tot_name VARCHAR(80)  		--//  ENCODE zstd
	,timeoff_territory_source_id VARCHAR(18) NOT NULL 		--//  ENCODE zstd
	,modify_dt TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,modify_id VARCHAR(18)  		--//  ENCODE zstd
	,inserted_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,updated_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,PRIMARY KEY (timeoff_territory_source_id, country_key, employee_key)
)
		--// DISTSTYLE KEY
cluster by (employee_key)
		--// SORTKEY ( 
		--// 	sea_time_on_time_off
		--// 	)
;		--// ;
--DROP TABLE HCPOSEEDW_INTEGRATION.holiday_list;
CREATE OR REPLACE TABLE HCPOSEEDW_INTEGRATION.HOLIDAY_LIST		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE HCPOSEEDW_INTEGRATION.holiday_list
(
	country VARCHAR(5)  		--//  ENCODE lzo
	,holiday_key VARCHAR(20)  		--//  ENCODE lzo
)

;
--DROP TABLE HCPOSEEDW_INTEGRATION.isight_dim_employee_snapshot_xref;
CREATE OR REPLACE TABLE HCPOSEEDW_INTEGRATION.ISIGHT_DIM_EMPLOYEE_SNAPSHOT_XREF		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE HCPOSEEDW_INTEGRATION.isight_dim_employee_snapshot_xref
(
	year numeric(18,0)		--//  ENCODE delta // INTEGER  
	,month numeric(18,0)		--//  ENCODE delta // INTEGER  
	,employee_key VARCHAR(32)  		--//  ENCODE zstd
	,country_code VARCHAR(8)  		--//  ENCODE zstd
	,employee_source_id VARCHAR(18)  		--//  ENCODE zstd
	,modified_dt TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,modified_id VARCHAR(18)  		--//  ENCODE zstd
	,employee_name VARCHAR(121)  		--//  ENCODE zstd
	,employee_wwid VARCHAR(20)  		--//  ENCODE zstd
	,mobile_phone VARCHAR(40)  		--//  ENCODE zstd
	,email_id VARCHAR(128)  		--//  ENCODE zstd
	,username VARCHAR(80)  		--//  ENCODE zstd
	,nickname VARCHAR(40)  		--//  ENCODE zstd
	,local_employee_number VARCHAR(20)  		--//  ENCODE zstd
	,profile_id VARCHAR(18)  		--//  ENCODE zstd
	,profile_name VARCHAR(255)  		--//  ENCODE zstd
	,function_name VARCHAR(255)  		--//  ENCODE zstd
	,employee_profile_id VARCHAR(18)  		--//  ENCODE zstd
	,company_name VARCHAR(80)  		--//  ENCODE zstd
	,division_name VARCHAR(80)  		--//  ENCODE zstd
	,department_name VARCHAR(80)  		--//  ENCODE zstd
	,country_name VARCHAR(80)  		--//  ENCODE zstd
	,address VARCHAR(255)  		--//  ENCODE zstd
	,alias VARCHAR(18)  		--//  ENCODE zstd
	,timezonesidkey VARCHAR(40)  		--//  ENCODE zstd
	,user_role_source_id VARCHAR(18)  		--//  ENCODE zstd
	,receives_info_emails numeric(18,0)		--//  ENCODE delta // INTEGER  
	,federation_identifier VARCHAR(512)  		--//  ENCODE zstd
	,last_ipad_sync TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,user_license VARCHAR(1300)  		--//  ENCODE zstd
	,title VARCHAR(1300)  		--//  ENCODE zstd
	,phone VARCHAR(43)  		--//  ENCODE zstd
	,last_login_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,region VARCHAR(1300)  		--//  ENCODE zstd
	,profile_group_ap VARCHAR(1030)  		--//  ENCODE zstd
	,manager_name VARCHAR(80)  		--//  ENCODE zstd
	,manager_wwid VARCHAR(18)  		--//  ENCODE zstd
	,active_flag NUMERIC(2,0)  		--//  ENCODE delta
	,my_organization_code VARCHAR(18)  		--//  ENCODE zstd
	,my_organization_name VARCHAR(80)  		--//  ENCODE zstd
	,organization_l1_code VARCHAR(18)  		--//  ENCODE zstd
	,organization_l1_name VARCHAR(80)  		--//  ENCODE zstd
	,organization_l2_code VARCHAR(18)  		--//  ENCODE zstd
	,organization_l2_name VARCHAR(80)  		--//  ENCODE zstd
	,organization_l3_code VARCHAR(18)  		--//  ENCODE zstd
	,organization_l3_name VARCHAR(80)  		--//  ENCODE zstd
	,organization_l4_code VARCHAR(18)  		--//  ENCODE zstd
	,organization_l4_name VARCHAR(80)  		--//  ENCODE zstd
	,organization_l5_code VARCHAR(18)  		--//  ENCODE zstd
	,organization_l5_name VARCHAR(80)  		--//  ENCODE zstd
	,organization_l6_code VARCHAR(18)  		--//  ENCODE zstd
	,organization_l6_name VARCHAR(80)  		--//  ENCODE zstd
	,organization_l7_code VARCHAR(18)  		--//  ENCODE zstd
	,organization_l7_name VARCHAR(80)  		--//  ENCODE zstd
	,organization_l8_code VARCHAR(18)  		--//  ENCODE zstd
	,organization_l8_name VARCHAR(80)  		--//  ENCODE zstd
	,organization_l9_code VARCHAR(18)  		--//  ENCODE zstd
	,organization_l9_name VARCHAR(80)  		--//  ENCODE zstd
	,common_organization_l1_code VARCHAR(18)  		--//  ENCODE zstd
	,common_organization_l1_name VARCHAR(80)  		--//  ENCODE zstd
	,common_organization_l2_code VARCHAR(18)  		--//  ENCODE zstd
	,common_organization_l2_name VARCHAR(80)  		--//  ENCODE zstd
	,common_organization_l3_code VARCHAR(18)  		--//  ENCODE zstd
	,common_organization_l3_name VARCHAR(80)  		--//  ENCODE zstd
	,inserted_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,updated_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
)

;
--DROP TABLE HCPOSEEDW_INTEGRATION.rep_level_kpi;
CREATE OR REPLACE TABLE HCPOSEEDW_INTEGRATION.REP_LEVEL_KPI		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE HCPOSEEDW_INTEGRATION.rep_level_kpi
(
	country VARCHAR(20)  		--//  ENCODE lzo
	,jnj_date_year VARCHAR(11)  		--//  ENCODE lzo
	,jnj_date_month VARCHAR(5)  		--//  ENCODE lzo
	,jnj_date_quarter VARCHAR(5)  		--//  ENCODE lzo
	,date_year VARCHAR(11)  		--//  ENCODE lzo
	,date_month VARCHAR(3)  		--//  ENCODE lzo
	,date_quarter VARCHAR(2)  		--//  ENCODE lzo
	,my_date_year VARCHAR(11)  		--//  ENCODE lzo
	,my_date_month VARCHAR(3)  		--//  ENCODE lzo
	,my_date_quarter VARCHAR(2)  		--//  ENCODE lzo
	,sector VARCHAR(256)  		--//  ENCODE lzo
	,l3_username VARCHAR(80)  		--//  ENCODE lzo
	,l3_manager_name VARCHAR(121)  		--//  ENCODE lzo
	,l2_wwid VARCHAR(20)  		--//  ENCODE lzo
	,l2_username VARCHAR(80)  		--//  ENCODE lzo
	,l2_manager_name VARCHAR(121)  		--//  ENCODE lzo
	,l1_wwid VARCHAR(20)  		--//  ENCODE lzo
	,l1_username VARCHAR(80)  		--//  ENCODE lzo
	,l1_manager_name VARCHAR(121)  		--//  ENCODE lzo
	,sales_rep_ntid VARCHAR(80)  		--//  ENCODE lzo
	,organization_l1_name VARCHAR(80)  		--//  ENCODE lzo
	,organization_l2_name VARCHAR(80)  		--//  ENCODE lzo
	,organization_l3_name VARCHAR(80)  		--//  ENCODE lzo
	,organization_l4_name VARCHAR(80)  		--//  ENCODE lzo
	,organization_l5_name VARCHAR(80)  		--//  ENCODE lzo
	,sales_rep VARCHAR(121)  		--//  ENCODE lzo
	,working_days NUMERIC(18,0)  		--//  ENCODE lzo
	,total_calls numeric(38,0)		--//  ENCODE lzo // BIGINT  
	,total_cnt_call_delay NUMERIC(38,1)  		--//  ENCODE lzo
	,total_call_edetailing numeric(38,0)		--//  ENCODE lzo // BIGINT  
	,call_total_active_user numeric(38,0)		--//  ENCODE lzo // BIGINT  
	,total_calls_with_product numeric(38,0)		--//  ENCODE lzo // BIGINT  
	,total_sbmtd_calls_key_message numeric(38,0)		--//  ENCODE lzo // BIGINT  
	,total_key_message numeric(38,0)		--//  ENCODE lzo // BIGINT  
	,total_call_classification_a numeric(38,0)		--//  ENCODE lzo // BIGINT  
	,total_call_classification_b numeric(38,0)		--//  ENCODE lzo // BIGINT  
	,total_call_classification_c numeric(38,0)		--//  ENCODE lzo // BIGINT  
	,total_call_classification_d numeric(38,0)		--//  ENCODE lzo // BIGINT  
	,total_call_classification_u numeric(38,0)		--//  ENCODE lzo // BIGINT  
	,total_call_classification_z numeric(38,0)		--//  ENCODE lzo // BIGINT  
	,total_call_classification_no_product numeric(38,0)		--//  ENCODE lzo // BIGINT  
	,total_detailing numeric(38,0)		--//  ENCODE lzo // BIGINT  
	,coaching_team VARCHAR(80)  		--//  ENCODE lzo
	,coaching_status VARCHAR(255)  		--//  ENCODE lzo
	,total_coaching_report numeric(38,0)		--//  ENCODE lzo // BIGINT  
	,total_coaching_visit numeric(38,0)		--//  ENCODE lzo // BIGINT  
	,coaching_manager VARCHAR(255)  		--//  ENCODE lzo
	,coaching_sales_rep VARCHAR(255)  		--//  ENCODE lzo
	,planned_calls NUMERIC(38,0)  		--//  ENCODE lzo
	,attainment NUMERIC(38,0)  		--//  ENCODE lzo
	,actual_calls NUMERIC(38,0)  		--//  ENCODE lzo
	,cpa_100 NUMERIC(38,1)  		--//  ENCODE lzo
	,target_cpa_status VARCHAR(18)  		--//  ENCODE lzo
	,product_cpa_status VARCHAR(18)  		--//  ENCODE lzo
	,planned_call_detail_count NUMERIC(38,0)  		--//  ENCODE lzo
	,cycle_plan_detail_attainment NUMERIC(38,0)  		--//  ENCODE lzo
	,actual_call_detail_count NUMERIC(38,0)  		--//  ENCODE lzo
	,cfa_100 NUMERIC(38,0)  		--//  ENCODE lzo
	,cfa_33 NUMERIC(38,0)  		--//  ENCODE lzo
	,cfa_66 NUMERIC(38,0)  		--//  ENCODE lzo
)

;
--DROP TABLE HCPOSEEDW_INTEGRATION.wrk_call_detail;
CREATE OR REPLACE TABLE HCPOSEEDW_INTEGRATION.WRK_CALL_DETAIL		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE HCPOSEEDW_INTEGRATION.wrk_call_detail
(
	jnj_date_year VARCHAR(11)  		--//  ENCODE lzo
	,jnj_date_month VARCHAR(5)  		--//  ENCODE lzo
	,jnj_date_quarter VARCHAR(5)  		--//  ENCODE lzo
	,date_year VARCHAR(11)  		--//  ENCODE lzo
	,date_month VARCHAR(3)  		--//  ENCODE lzo
	,date_quarter VARCHAR(2)  		--//  ENCODE lzo
	,my_date_year VARCHAR(11)  		--//  ENCODE lzo
	,my_date_month VARCHAR(3)  		--//  ENCODE lzo
	,my_date_quarter VARCHAR(2)  		--//  ENCODE lzo
	,country VARCHAR(18)  		--//  ENCODE lzo
	,sector VARCHAR(256)  		--//  ENCODE lzo
	,l3_wwid VARCHAR(20)  		--//  ENCODE lzo
	,l3_username VARCHAR(80)  		--//  ENCODE lzo
	,l3_manager_name VARCHAR(121)  		--//  ENCODE lzo
	,l2_wwid VARCHAR(20)  		--//  ENCODE lzo
	,l2_username VARCHAR(80)  		--//  ENCODE lzo
	,l2_manager_name VARCHAR(121)  		--//  ENCODE lzo
	,l1_wwid VARCHAR(20)  		--//  ENCODE lzo
	,l1_username VARCHAR(80)  		--//  ENCODE lzo
	,l1_manager_name VARCHAR(121)  		--//  ENCODE lzo
	,organization_l1_name VARCHAR(80)  		--//  ENCODE lzo
	,organization_l2_name VARCHAR(80)  		--//  ENCODE lzo
	,organization_l3_name VARCHAR(80)  		--//  ENCODE lzo
	,organization_l4_name VARCHAR(80)  		--//  ENCODE lzo
	,organization_l5_name VARCHAR(80)  		--//  ENCODE lzo
	,sales_rep_ntid VARCHAR(80)  		--//  ENCODE lzo
	,sales_rep VARCHAR(121)  		--//  ENCODE lzo
	,email_id VARCHAR(128)  		--//  ENCODE lzo
	,working_days NUMERIC(18,0)  		--//  ENCODE lzo
	,hcp_name VARCHAR(255)  		--//  ENCODE lzo
	,hcp_source_id VARCHAR(18)  		--//  ENCODE lzo
	,hcp_speciality VARCHAR(255)  		--//  ENCODE lzo
	,business_account_id VARCHAR(18)  		--//  ENCODE lzo
	,business_account VARCHAR(1300)  		--//  ENCODE lzo
	,total_calls numeric(38,0)		--//  ENCODE lzo // BIGINT  
	,total_cnt_call_delay NUMERIC(38,1)  		--//  ENCODE lzo
	,total_call_edetailing numeric(38,0)		--//  ENCODE lzo // BIGINT  
	,total_active numeric(38,0)		--//  ENCODE lzo // BIGINT  
	,detailed_products numeric(38,0)		--//  ENCODE lzo // BIGINT  
	,total_sbmtd_calls_key_message numeric(38,0)		--//  ENCODE lzo // BIGINT  
	,total_key_message numeric(38,0)		--//  ENCODE lzo // BIGINT  
	,total_call_classification numeric(38,0)		--//  ENCODE lzo // BIGINT  
	,classification_type VARCHAR(255)  		--//  ENCODE lzo
	,total_call_classification_a numeric(38,0)		--//  ENCODE lzo // BIGINT  
	,total_call_classification_b numeric(38,0)		--//  ENCODE lzo // BIGINT  
	,total_call_classification_c numeric(38,0)		--//  ENCODE lzo // BIGINT  
	,total_call_classification_d numeric(38,0)		--//  ENCODE lzo // BIGINT  
	,total_call_classification_u numeric(38,0)		--//  ENCODE lzo // BIGINT  
	,total_call_classification_z numeric(38,0)		--//  ENCODE lzo // BIGINT  
	,total_call_classification_no_product numeric(38,0)		--//  ENCODE lzo // BIGINT  
	,total_detailing numeric(38,0)		--//  ENCODE lzo // BIGINT  
)

;
--DROP TABLE HCPOSEEDW_INTEGRATION.wrk_coaching_detail;
CREATE OR REPLACE TABLE HCPOSEEDW_INTEGRATION.WRK_COACHING_DETAIL		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE HCPOSEEDW_INTEGRATION.wrk_coaching_detail
(
	jnj_date_year VARCHAR(11)  		--//  ENCODE lzo
	,jnj_date_month VARCHAR(11)  		--//  ENCODE lzo
	,jnj_date_quarter VARCHAR(11)  		--//  ENCODE lzo
	,date_year VARCHAR(11)  		--//  ENCODE lzo
	,date_month VARCHAR(3)  		--//  ENCODE lzo
	,date_quarter VARCHAR(2)  		--//  ENCODE lzo
	,my_date_year VARCHAR(11)  		--//  ENCODE lzo
	,my_date_month VARCHAR(3)  		--//  ENCODE lzo
	,my_date_quarter VARCHAR(2)  		--//  ENCODE lzo
	,country VARCHAR(8)  		--//  ENCODE lzo
	,sector VARCHAR(256)  		--//  ENCODE lzo
	,l3_wwid VARCHAR(20)  		--//  ENCODE lzo
	,l3_username VARCHAR(80)  		--//  ENCODE lzo
	,l3_manager_name VARCHAR(121)  		--//  ENCODE lzo
	,l2_wwid VARCHAR(20)  		--//  ENCODE lzo
	,l2_username VARCHAR(80)  		--//  ENCODE lzo
	,l2_manager_name VARCHAR(121)  		--//  ENCODE lzo
	,l1_wwid VARCHAR(20)  		--//  ENCODE lzo
	,l1_username VARCHAR(80)  		--//  ENCODE lzo
	,l1_manager_name VARCHAR(121)  		--//  ENCODE lzo
	,organization_l1_name VARCHAR(80)  		--//  ENCODE lzo
	,organization_l2_name VARCHAR(80)  		--//  ENCODE lzo
	,organization_l3_name VARCHAR(80)  		--//  ENCODE lzo
	,organization_l4_name VARCHAR(80)  		--//  ENCODE lzo
	,organization_l5_name VARCHAR(80)  		--//  ENCODE lzo
	,sales_rep_ntid VARCHAR(80)  		--//  ENCODE lzo
	,sales_rep VARCHAR(121)  		--//  ENCODE lzo
	,coaching_status VARCHAR(255)  		--//  ENCODE lzo
	,coaching_manager VARCHAR(121)  		--//  ENCODE lzo
	,total_coaching_report numeric(38,0)		--//  ENCODE lzo // BIGINT  
	,total_coaching_visit NUMERIC(38,0)  		--//  ENCODE lzo
)

;
--DROP TABLE HCPOSEEDW_INTEGRATION.wrk_cycle_plan;
CREATE OR REPLACE TABLE HCPOSEEDW_INTEGRATION.WRK_CYCLE_PLAN		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE HCPOSEEDW_INTEGRATION.wrk_cycle_plan
(
	jnj_date_year VARCHAR(11)  		--//  ENCODE lzo
	,jnj_date_month VARCHAR(5)  		--//  ENCODE lzo
	,jnj_date_quarter VARCHAR(5)  		--//  ENCODE lzo
	,date_year VARCHAR(11)  		--//  ENCODE lzo
	,date_month VARCHAR(3)  		--//  ENCODE lzo
	,date_quarter VARCHAR(2)  		--//  ENCODE lzo
	,my_date_year VARCHAR(11)  		--//  ENCODE lzo
	,my_date_month VARCHAR(3)  		--//  ENCODE lzo
	,my_date_quarter VARCHAR(2)  		--//  ENCODE lzo
	,country VARCHAR(8)  		--//  ENCODE lzo
	,sector VARCHAR(256)  		--//  ENCODE lzo
	,l3_wwid VARCHAR(20)  		--//  ENCODE lzo
	,l3_username VARCHAR(80)  		--//  ENCODE lzo
	,l3_manager_name VARCHAR(121)  		--//  ENCODE lzo
	,l2_wwid VARCHAR(20)  		--//  ENCODE lzo
	,l2_username VARCHAR(80)  		--//  ENCODE lzo
	,l2_manager_name VARCHAR(121)  		--//  ENCODE lzo
	,l1_wwid VARCHAR(20)  		--//  ENCODE lzo
	,l1_username VARCHAR(80)  		--//  ENCODE lzo
	,l1_manager_name VARCHAR(121)  		--//  ENCODE lzo
	,sales_rep_ntid VARCHAR(80)  		--//  ENCODE lzo
	,sales_rep VARCHAR(121)  		--//  ENCODE lzo
	,email_id VARCHAR(128)  		--//  ENCODE lzo
	,organization_l1_name VARCHAR(80)  		--//  ENCODE lzo
	,organization_l2_name VARCHAR(80)  		--//  ENCODE lzo
	,organization_l3_name VARCHAR(80)  		--//  ENCODE lzo
	,organization_l4_name VARCHAR(80)  		--//  ENCODE lzo
	,organization_l5_name VARCHAR(80)  		--//  ENCODE lzo
	,total_active numeric(38,0)		--//  ENCODE lzo // BIGINT  
	,hcp_name VARCHAR(255)  		--//  ENCODE lzo
	,hcp_source_id VARCHAR(18)  		--//  ENCODE lzo
	,hcp_speciality VARCHAR(255)  		--//  ENCODE lzo
	,hco_customer_code_2 VARCHAR(60)  		--//  ENCODE lzo
	,cycle_speciality VARCHAR(3000)  		--//  ENCODE lzo
	,business_account_id VARCHAR(18)  		--//  ENCODE lzo
	,business_account VARCHAR(1300)  		--//  ENCODE lzo
	,account_segmentation VARCHAR(1300)  		--//  ENCODE lzo
	,cpa_status VARCHAR(18)  		--//  ENCODE lzo
	,cycle_plan_type VARCHAR(7)  		--//  ENCODE lzo
	,planned_calls NUMERIC(38,0)  		--//  ENCODE lzo
	,attainment NUMERIC(38,0)  		--//  ENCODE lzo
	,actual_calls NUMERIC(38,0)  		--//  ENCODE lzo
	,cpa_100 NUMERIC(38,1)  		--//  ENCODE lzo
	,product VARCHAR(80)  		--//  ENCODE lzo
	,planned_call_detail_count NUMERIC(38,0)  		--//  ENCODE lzo
	,cycle_plan_detail_attainment NUMERIC(38,0)  		--//  ENCODE lzo
	,actual_call_detail_count NUMERIC(38,0)  		--//  ENCODE lzo
	,cfa_100 NUMERIC(38,0)  		--//  ENCODE lzo
	,cfa_33 NUMERIC(38,0)  		--//  ENCODE lzo
	,cfa_66 NUMERIC(38,0)  		--//  ENCODE lzo
)

;
--DROP TABLE HCPOSEEDW_INTEGRATION.wrk_dim_organization;
CREATE OR REPLACE TABLE HCPOSEEDW_INTEGRATION.WRK_DIM_ORGANIZATION		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE HCPOSEEDW_INTEGRATION.wrk_dim_organization
(
	territory_source_id VARCHAR(18)  		--//  ENCODE zstd
	,country_code VARCHAR(8)  		--//  ENCODE zstd
	,my_organization_code VARCHAR(18)  		--//  ENCODE zstd
	,my_organization_name VARCHAR(80)  		--//  ENCODE zstd
	,organization_l1_code VARCHAR(18)  		--//  ENCODE zstd
	,organization_l1_name VARCHAR(80)  		--//  ENCODE zstd
	,organization_l2_code VARCHAR(18)  		--//  ENCODE zstd
	,organization_l2_name VARCHAR(80)  		--//  ENCODE zstd
	,organization_l3_code VARCHAR(18)  		--//  ENCODE zstd
	,organization_l3_name VARCHAR(80)  		--//  ENCODE zstd
	,organization_l4_code VARCHAR(18)  		--//  ENCODE zstd
	,organization_l4_name VARCHAR(80)  		--//  ENCODE zstd
	,organization_l5_code VARCHAR(18)  		--//  ENCODE zstd
	,organization_l5_name VARCHAR(80)  		--//  ENCODE zstd
	,organization_l6_code VARCHAR(18)  		--//  ENCODE zstd
	,organization_l6_name VARCHAR(80)  		--//  ENCODE zstd
	,organization_l7_code VARCHAR(18)  		--//  ENCODE zstd
	,organization_l7_name VARCHAR(80)  		--//  ENCODE zstd
	,organization_l8_code VARCHAR(18)  		--//  ENCODE zstd
	,organization_l8_name VARCHAR(80)  		--//  ENCODE zstd
	,organization_l9_code VARCHAR(18)  		--//  ENCODE zstd
	,organization_l9_name VARCHAR(80)  		--//  ENCODE zstd
	,inserted_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,updated_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
)

;


--DROP TABLE HCPOSEITG_INTEGRATION.itg_account_hco;
CREATE OR REPLACE TABLE HCPOSEITG_INTEGRATION.ITG_ACCOUNT_HCO		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE HCPOSEITG_INTEGRATION.itg_account_hco
(
	hco_key VARCHAR(32)  		--//  ENCODE zstd
	,parent_hco_key VARCHAR(32)  		--//  ENCODE zstd
	,account_source_id VARCHAR(18) NOT NULL 		--//  ENCODE zstd
	,is_deleted numeric(18,0)		--//  ENCODE delta // INTEGER  
	,account_name VARCHAR(255)  		--//  ENCODE zstd
	,record_type_source_id VARCHAR(18)  		--//  ENCODE zstd
	,phone_number VARCHAR(40)  		--//  ENCODE zstd
	,fax_number VARCHAR(40)  		--//  ENCODE zstd
	,website VARCHAR(255)  		--//  ENCODE zstd
	,owner_id VARCHAR(18)  		--//  ENCODE zstd
	,created_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,created_by_id VARCHAR(18)  		--//  ENCODE zstd
	,last_modified_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,last_modified_by_id VARCHAR(18)  		--//  ENCODE zstd
	,is_excluded_from_realign numeric(18,0)		--//  ENCODE delta // INTEGER  
	,is_person_account VARCHAR(5)  		--//  ENCODE zstd
	,external_id VARCHAR(120)  		--//  ENCODE zstd
	,territory_name VARCHAR(255)  		--//  ENCODE zstd
	,beds NUMERIC(4,0)  		--//  ENCODE delta
	,primary_parent_source_id VARCHAR(18)  		--//  ENCODE zstd
	,total_mds_dos NUMERIC(18,0)  		--//  ENCODE delta
	,departments NUMERIC(18,0)  		--//  ENCODE delta
	,hco_type VARCHAR(255)  		--//  ENCODE zstd
	,hco_sector VARCHAR(255)  		--//  ENCODE zstd
	,sfe_approved numeric(18,0)		--//  ENCODE delta // INTEGER  
	,country_code VARCHAR(8) NOT NULL 		--//  ENCODE zstd
	,business_description VARCHAR(32000)  		--//  ENCODE zstd
	,hcc_account_approved numeric(18,0)		--//  ENCODE delta // INTEGER  
	,inactive numeric(18,0)		--//  ENCODE delta // INTEGER  
	,total_physicians_enrolled NUMERIC(18,0)  		--//  ENCODE delta
	,total_pharmacists NUMERIC(3,0)  		--//  ENCODE delta
	,is_external_id_number numeric(18,0)		--//  ENCODE delta // INTEGER  
	,ot NUMERIC(8,0)  		--//  ENCODE delta
	,kam_paediatric NUMERIC(18,0)  		--//  ENCODE delta
	,kam_obgyn NUMERIC(18,0)  		--//  ENCODE delta
	,kam_minimally_invasive NUMERIC(18,0)  		--//  ENCODE delta
	,kam_total_urologysurgeons NUMERIC(18,0)  		--//  ENCODE delta
	,kam_total_surgeons NUMERIC(18,0)  		--//  ENCODE delta
	,kam_total_rheumphysicians NUMERIC(18,0)  		--//  ENCODE delta
	,kam_total_psychiatryphysicians NUMERIC(18,0)  		--//  ENCODE delta
	,kam_total_orthosurgeons NUMERIC(18,0)  		--//  ENCODE delta
	,kam_total_opthalsurgeons NUMERIC(18,0)  		--//  ENCODE delta
	,kam_total_neurologyphysicians NUMERIC(18,0)  		--//  ENCODE delta
	,kam_total_medoncophysicians NUMERIC(18,0)  		--//  ENCODE delta
	,kam_total_infectiousphysicians NUMERIC(18,0)  		--//  ENCODE delta
	,kam_total_haemaphysicians NUMERIC(18,0)  		--//  ENCODE delta
	,kam_total_generalsurgeons NUMERIC(18,0)  		--//  ENCODE delta
	,kam_total_gastrophysicians NUMERIC(18,0)  		--//  ENCODE delta
	,kam_total_endophysicians NUMERIC(18,0)  		--//  ENCODE delta
	,kam_total_dermaphysicians NUMERIC(18,0)  		--//  ENCODE delta
	,kam_total_cardiologyphysicians NUMERIC(18,0)  		--//  ENCODE delta
	,kam_total_cardiosurgeons NUMERIC(18,0)  		--//  ENCODE delta
	,kam_total_aestheticsurgeons NUMERIC(18,0)  		--//  ENCODE delta
	,kam_general_differnentiations VARCHAR(32768)  		--//  ENCODE zstd
	,kam_clinical_differentiations VARCHAR(32768)  		--//  ENCODE zstd
	,hco_name VARCHAR(1300)  		--//  ENCODE zstd
	,remarks VARCHAR(255)  		--//  ENCODE zstd
	,parent_hco_name VARCHAR(255)  		--//  ENCODE zstd
	,customer_code VARCHAR(60)  		--//  ENCODE zstd
	,inserted_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,updated_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,jj_external_id VARCHAR(90)  		--//  ENCODE zstd
	,specialty_2 VARCHAR(255)  		--//  ENCODE zstd
	,sub_specialty VARCHAR(255)  		--//  ENCODE zstd
	,sea_account_classification VARCHAR(255)  		--//  ENCODE zstd
	,jj_email_1 VARCHAR(80)  		--//  ENCODE zstd
	,jj_email_2 VARCHAR(80)  		--//  ENCODE zstd
	,PRIMARY KEY (account_source_id, country_code)
)

;
--DROP TABLE HCPOSEITG_INTEGRATION.itg_account_hcp;
CREATE OR REPLACE TABLE HCPOSEITG_INTEGRATION.ITG_ACCOUNT_HCP		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE HCPOSEITG_INTEGRATION.itg_account_hcp
(
	hcp_key VARCHAR(32)  		--//  ENCODE zstd
	,account_source_id VARCHAR(18) NOT NULL 		--//  ENCODE zstd
	,is_deleted numeric(18,0)		--//  ENCODE delta // INTEGER  
	,hcp_name VARCHAR(255)  		--//  ENCODE zstd
	,last_name VARCHAR(80)  		--//  ENCODE zstd
	,first_name VARCHAR(40)  		--//  ENCODE zstd
	,salutation VARCHAR(120)  		--//  ENCODE zstd
	,record_type_source_id VARCHAR(18)  		--//  ENCODE zstd
	,owner_id VARCHAR(18)  		--//  ENCODE zstd
	,created_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,created_by_id VARCHAR(18)  		--//  ENCODE zstd
	,last_modified_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,last_modified_by_id VARCHAR(18)  		--//  ENCODE zstd
	,is_excluded_from_realign numeric(18,0)		--//  ENCODE delta // INTEGER  
	,is_person_account numeric(18,0)		--//  ENCODE delta // INTEGER  
	,mobile_nbr VARCHAR(40)  		--//  ENCODE zstd
	,person_email VARCHAR(80)  		--//  ENCODE zstd
	,birth_day TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,email_opt_out_ind numeric(18,0)		--//  ENCODE delta // INTEGER  
	,do_not_call_ind numeric(18,0)		--//  ENCODE delta // INTEGER  
	,external_id VARCHAR(120)  		--//  ENCODE zstd
	,territory_name VARCHAR(255)  		--//  ENCODE zstd
	,specialty_1 VARCHAR(255)  		--//  ENCODE zstd
	,gender VARCHAR(255)  		--//  ENCODE zstd
	,preferred_name VARCHAR(12)  		--//  ENCODE zstd
	,primary_parent_source_id VARCHAR(18)  		--//  ENCODE zstd
	,professional_type VARCHAR(255)  		--//  ENCODE zstd
	,hcp_english_name VARCHAR(800)  		--//  ENCODE zstd
	,position VARCHAR(255)  		--//  ENCODE zstd
	,direct_line VARCHAR(40)  		--//  ENCODE zstd
	,direct_fax VARCHAR(40)  		--//  ENCODE zstd
	,sfe_approved numeric(18,0)		--//  ENCODE delta // INTEGER  
	,country_code VARCHAR(8) NOT NULL 		--//  ENCODE zstd
	,go_classification VARCHAR(255)  		--//  ENCODE zstd
	,hcc_account_approved numeric(18,0)		--//  ENCODE delta // INTEGER  
	,is_kol numeric(18,0)		--//  ENCODE delta // INTEGER  
	,inactive numeric(18,0)		--//  ENCODE delta // INTEGER  
	,physician_prescribing_behavior VARCHAR(255)  		--//  ENCODE zstd
	,physician_behavioral_style VARCHAR(255)  		--//  ENCODE zstd
	,is_external_id_number VARCHAR(5)  		--//  ENCODE zstd
	,customer_value_segmentation VARCHAR(255)  		--//  ENCODE zstd
	,ability_to_influence_peers VARCHAR(255)  		--//  ENCODE zstd
	,practice_size VARCHAR(255)  		--//  ENCODE zstd
	,patient_type VARCHAR(255)  		--//  ENCODE zstd
	,patient_medical_condition VARCHAR(255)  		--//  ENCODE zstd
	,md_customer_segmentation VARCHAR(255)  		--//  ENCODE zstd
	,years_of_experience VARCHAR(255)  		--//  ENCODE zstd
	,md_innovation VARCHAR(255)  		--//  ENCODE zstd
	,md_number_of_procedures VARCHAR(255)  		--//  ENCODE zstd
	,md_types_of_procedure VARCHAR(255)  		--//  ENCODE zstd
	,md_total_hip_replacement NUMERIC(18,0)  		--//  ENCODE delta
	,md_total_knee_replacement NUMERIC(18,0)  		--//  ENCODE delta
	,md_spine NUMERIC(18,0)  		--//  ENCODE delta
	,md_trauma NUMERIC(18,0)  		--//  ENCODE delta
	,md_collorectal NUMERIC(18,0)  		--//  ENCODE delta
	,md_hepatobiliary NUMERIC(18,0)  		--//  ENCODE delta
	,md_cholecystectomy NUMERIC(18,0)  		--//  ENCODE delta
	,md_total_hysterectomy NUMERIC(18,0)  		--//  ENCODE delta
	,md_myomectomy NUMERIC(18,0)  		--//  ENCODE delta
	,md_c_section NUMERIC(18,0)  		--//  ENCODE delta
	,md_normal_delivery NUMERIC(18,0)  		--//  ENCODE delta
	,md_cabg NUMERIC(18,0)  		--//  ENCODE delta
	,md_valve_repair NUMERIC(18,0)  		--//  ENCODE delta
	,md_abdominal NUMERIC(18,0)  		--//  ENCODE delta
	,md_breast_reconstruction NUMERIC(18,0)  		--//  ENCODE delta
	,md_oral_cranial_maxilofacial NUMERIC(18,0)  		--//  ENCODE delta
	,hcp_type VARCHAR(255)  		--//  ENCODE zstd
	,remarks VARCHAR(255)  		--//  ENCODE zstd
	,account_classification VARCHAR(255)  		--//  ENCODE zstd
	,customer_code_2 VARCHAR(20)  		--//  ENCODE zstd
	,inserted_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,updated_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,primary_ta VARCHAR(255)  		--//  ENCODE zstd
	,secondary_ta VARCHAR(255)  		--//  ENCODE zstd
	,jj_external_id VARCHAR(90)  		--//  ENCODE zstd
	,specialty_2 VARCHAR(255)  		--//  ENCODE zstd
	,sub_specialty VARCHAR(255)  		--//  ENCODE zstd
	,sea_account_classification VARCHAR(255)  		--//  ENCODE zstd
	,jj_email_1 VARCHAR(80)  		--//  ENCODE zstd
	,jj_email_2 VARCHAR(80)  		--//  ENCODE zstd
	,PRIMARY KEY (account_source_id, country_code)
)

;
--DROP TABLE HCPOSEITG_INTEGRATION.itg_address;
CREATE OR REPLACE TABLE HCPOSEITG_INTEGRATION.ITG_ADDRESS		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE HCPOSEITG_INTEGRATION.itg_address
(
	address_source_id VARCHAR(18) NOT NULL 		--//  ENCODE zstd
	,is_deleted numeric(18,0)		--//  ENCODE delta // INTEGER  
	,address_name VARCHAR(85)  		--//  ENCODE zstd
	,record_type_source_id VARCHAR(18)  		--//  ENCODE zstd
	,created_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,created_by_id VARCHAR(18)  		--//  ENCODE zstd
	,last_modified_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,last_modified_by_id VARCHAR(18)  		--//  ENCODE zstd
	,account_source_id VARCHAR(300)  		--//  ENCODE zstd
	,address_line2_name VARCHAR(300)  		--//  ENCODE zstd
	,city VARCHAR(40)  		--//  ENCODE zstd
	,external_id VARCHAR(120)  		--//  ENCODE zstd
	,phone VARCHAR(40)  		--//  ENCODE zstd
	,fax VARCHAR(40)  		--//  ENCODE zstd
	,map VARCHAR(1300)  		--//  ENCODE zstd
	,is_primary numeric(18,0)		--//  ENCODE delta // INTEGER  
	,appt_required numeric(18,0)		--//  ENCODE delta // INTEGER  
	,inactive numeric(18,0)		--//  ENCODE delta // INTEGER  
	,country_code VARCHAR(255) NOT NULL 		--//  ENCODE zstd
	,latitude NUMERIC(15,8)  		--//  ENCODE delta
	,zip VARCHAR(20)  		--//  ENCODE zstd
	,brick VARCHAR(80)  		--//  ENCODE zstd
	,state VARCHAR(255)  		--//  ENCODE zstd
	,longitude NUMERIC(15,8)  		--//  ENCODE delta
	,controlling_address VARCHAR(18)  		--//  ENCODE zstd
	,suburb_town VARCHAR(50)  		--//  ENCODE zstd
	,veeva_autogen_id VARCHAR(30)  		--//  ENCODE zstd
	,inserted_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,updated_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,is_locked VARCHAR(5)  		--//  ENCODE zstd
	,PRIMARY KEY (address_source_id, country_code)
)

;
--DROP TABLE HCPOSEITG_INTEGRATION.itg_call;
CREATE OR REPLACE TABLE HCPOSEITG_INTEGRATION.ITG_CALL		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE HCPOSEITG_INTEGRATION.itg_call
(
	call_key VARCHAR(32) NOT NULL 		--//  ENCODE zstd
	,call_source_id VARCHAR(18) NOT NULL 		--//  ENCODE zstd
	,owner_source_id VARCHAR(18)  		--//  ENCODE zstd
	,is_deleted numeric(18,0)		--//  ENCODE delta // INTEGER  
	,call_name VARCHAR(80)  		--//  ENCODE zstd
	,record_type_source_id VARCHAR(18)  		--//  ENCODE zstd
	,created_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,created_by_id VARCHAR(18)  		--//  ENCODE zstd
	,last_modified_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,last_modified_by_id VARCHAR(18)  		--//  ENCODE zstd
	,last_activity_date DATE  		--//  ENCODE delta
	,call_comments VARCHAR(32000)  		--//  ENCODE zstd
	,sample_card VARCHAR(15)  		--//  ENCODE zstd
	,account_source_id VARCHAR(18)  		--//  ENCODE zstd
	,call_status_type VARCHAR(255)  		--//  ENCODE zstd
	,account_plan VARCHAR(18)  		--//  ENCODE zstd
	,next_call_notes VARCHAR(32000)  		--//  ENCODE zstd
	,pre_call_notes VARCHAR(32000)  		--//  ENCODE zstd
	,mobile_id VARCHAR(100)  		--//  ENCODE zstd
	,significant_event numeric(18,0)		--//  ENCODE delta // INTEGER  
	,location VARCHAR(128)  		--//  ENCODE zstd
	,call_date_time TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,signature_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,signature VARCHAR(32000)  		--//  ENCODE zstd
	,territory VARCHAR(100)  		--//  ENCODE zstd
	,submitted_by_mobile numeric(18,0)		--//  ENCODE delta // INTEGER  
	,call_type VARCHAR(255)  		--//  ENCODE zstd
	,attendees NUMERIC(3,0)  		--//  ENCODE delta
	,attendee_type VARCHAR(255)  		--//  ENCODE zstd
	,call_date DATE  		--//  ENCODE delta
	,detailed_products VARCHAR(255)  		--//  ENCODE zstd
	,parent_call_source_id VARCHAR(18)  		--//  ENCODE zstd
	,user_name VARCHAR(18)  		--//  ENCODE zstd
	,medical_event VARCHAR(18)  		--//  ENCODE zstd
	,mobile_last_modified_date_time TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,is_parent_call NUMERIC(18,0)  		--//  ENCODE delta
	,call_clm numeric(18,0)		--//  ENCODE delta // INTEGER  
	,is_sampled_call numeric(18,0)		--//  ENCODE delta // INTEGER  
	,presentations VARCHAR(500)  		--//  ENCODE zstd
	,product_priority_1 VARCHAR(18)  		--//  ENCODE zstd
	,product_priority_2 VARCHAR(18)  		--//  ENCODE zstd
	,product_priority_3 VARCHAR(18)  		--//  ENCODE zstd
	,product_priority_4 VARCHAR(18)  		--//  ENCODE zstd
	,product_priority_5 VARCHAR(18)  		--//  ENCODE zstd
	,attendee_list VARCHAR(32768)  		--//  ENCODE zstd
	,msl_interaction_notes VARCHAR(32768)  		--//  ENCODE zstd
	,sea_call_type VARCHAR(1300)  		--//  ENCODE zstd
	,call_duration VARCHAR(255)  		--//  ENCODE zstd
	,interaction_mode VARCHAR(255)  		--//  ENCODE zstd
	,hcp_kol_initiated numeric(18,0)		--//  ENCODE delta // INTEGER  
	,msl_interaction_type VARCHAR(255)  		--//  ENCODE zstd
	,manager_call_comment VARCHAR(32768)  		--//  ENCODE zstd
	,md_hours VARCHAR(255)  		--//  ENCODE zstd
	,md_in_or_ot numeric(18,0)		--//  ENCODE delta // INTEGER  
	,md_d_call_type VARCHAR(255)  		--//  ENCODE zstd
	,md_minutes VARCHAR(255)  		--//  ENCODE zstd
	,call_objective VARCHAR(255)  		--//  ENCODE zstd
	,submission_delay NUMERIC(18,0)  		--//  ENCODE delta
	,country_code VARCHAR(1300) NOT NULL 		--//  ENCODE zstd
	,region VARCHAR(1300)  		--//  ENCODE zstd
	,md_hsp_admin VARCHAR(255)  		--//  ENCODE zstd
	,hsp_minutes VARCHAR(255)  		--//  ENCODE zstd
	,ortho_on_call_case numeric(18,0)		--//  ENCODE delta // INTEGER  
	,ortho_volunteer_case numeric(18,0)		--//  ENCODE delta // INTEGER  
	,md_calc1 NUMERIC(18,2)  		--//  ENCODE delta
	,md_calculate_non_case_time NUMERIC(18,2)  		--//  ENCODE delta
	,md_calculated_hours_field NUMERIC(18,2)  		--//  ENCODE delta
	,md_casedeployment VARCHAR(255)  		--//  ENCODE zstd
	,md_case_coverage_12_hours VARCHAR(255)  		--//  ENCODE zstd
	,md_product_discussion VARCHAR(255)  		--//  ENCODE zstd
	,md_concurrent_call numeric(18,0)		--//  ENCODE delta // INTEGER  
	,courtesy_call VARCHAR(255)  		--//  ENCODE zstd
	,md_in_service VARCHAR(255)  		--//  ENCODE zstd
	,md_kol_course_discussion VARCHAR(255)  		--//  ENCODE zstd
	,kol_minutes VARCHAR(255)  		--//  ENCODE zstd
	,other_activities_time_12_hours NUMERIC(18,0)  		--//  ENCODE delta
	,other_in_field_activities VARCHAR(4099)  		--//  ENCODE zstd
	,md_overseas_workshop_visit VARCHAR(255)  		--//  ENCODE zstd
	,md_ra_activities2 VARCHAR(255)  		--//  ENCODE zstd
	,sales_activity VARCHAR(4099)  		--//  ENCODE zstd
	,sales_time_12_hours NUMERIC(18,0)  		--//  ENCODE delta
	,time_spent VARCHAR(255)  		--//  ENCODE zstd
	,time_spent_on_other_activities_simp VARCHAR(255)  		--//  ENCODE zstd
	,time_spent_on_sales_activity VARCHAR(255)  		--//  ENCODE zstd
	,md_time_spent_on_a_call NUMERIC(18,2)  		--//  ENCODE delta
	,md_case_type VARCHAR(255)  		--//  ENCODE zstd
	,md_sets_activities VARCHAR(255)  		--//  ENCODE zstd
	,md_time_spent_on_case VARCHAR(255)  		--//  ENCODE zstd
	,time_spent_on_other_activities NUMERIC(18,2)  		--//  ENCODE delta
	,time_spent_per_call NUMERIC(18,2)  		--//  ENCODE delta
	,md_case_conducted_in_hospital VARCHAR(255)  		--//  ENCODE zstd
	,calculated_field_2 NUMERIC(18,2)  		--//  ENCODE delta
	,calculated_hours_3 NUMERIC(18,2)  		--//  ENCODE delta
	,call_planned numeric(18,0)		--//  ENCODE delta // INTEGER  
	,call_submission_day NUMERIC(18,1)  		--//  ENCODE delta
	,case_coverage NUMERIC(18,2)  		--//  ENCODE delta
	,day_of_week VARCHAR(1300)  		--//  ENCODE zstd
	,pre_engagement_coaching numeric(18,0)		--//  ENCODE delta // INTEGER  
	,joined_by_manager numeric(18,0)		--//  ENCODE delta // INTEGER  
	,preparation_time NUMERIC(18,0)  		--//  ENCODE delta
	,travel_time NUMERIC(18,0)  		--//  ENCODE delta
	,load_flag VARCHAR(2)  		--//  ENCODE zstd
	,number_of_days_to_close_calls_kpi NUMERIC(18,2)  		--//  ENCODE delta
	,jj_therapeutic_area VARCHAR(255)  		--//  ENCODE zstd
	,address_line_1 VARCHAR(80)  		--//  ENCODE zstd
	,address_line_2 VARCHAR(100)  		--//  ENCODE zstd
	,address VARCHAR(500)  		--//  ENCODE zstd
	,allowed_products VARCHAR(1000)  		--//  ENCODE zstd
	,city VARCHAR(40)  		--//  ENCODE zstd
	,disbursed_to VARCHAR(255)  		--//  ENCODE zstd
	,duration NUMERIC(18,1)  		--//  ENCODE delta
	,is_locked numeric(18,0)		--//  ENCODE delta // INTEGER  
	,account_external_id VARCHAR(1300)  		--//  ENCODE zstd
	,account_specialty VARCHAR(1300)  		--//  ENCODE zstd
	,call_submitted_date_time TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,call_submittedby VARCHAR(30)  		--//  ENCODE zstd
	,signed_by VARCHAR(255)  		--//  ENCODE zstd
	,number_of_detailing NUMERIC(18,1)  		--//  ENCODE delta
	,numberof_key_message NUMERIC(18,1)  		--//  ENCODE delta
	,rep_department VARCHAR(1300)  		--//  ENCODE zstd
	,rep_division VARCHAR(1300)  		--//  ENCODE zstd
	,rep_manager VARCHAR(1300)  		--//  ENCODE zstd
	,call_objectives VARCHAR(4099)  		--//  ENCODE zstd
	,cost_of_procedures NUMERIC(18,1)  		--//  ENCODE delta
	,other_objective VARCHAR(255)  		--//  ENCODE zstd
	,hcp_speciality VARCHAR(1300)  		--//  ENCODE zstd
	,call_duration_mins_in_number NUMERIC(18,2)  		--//  ENCODE delta
	,productivity_call VARCHAR(255)  		--//  ENCODE zstd
	,last_device VARCHAR(255)  		--//  ENCODE zstd
	,location_services_status VARCHAR(255)  		--//  ENCODE zstd
	,mobile_created_date_time TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,no_disbursement numeric(18,0)		--//  ENCODE delta // INTEGER  
	,owner_company_name VARCHAR(1300)  		--//  ENCODE zstd
	,parent_address VARCHAR(18)  		--//  ENCODE zstd
	,signature_location_latitude NUMERIC(15,6)  		--//  ENCODE delta
	,signature_location_longitude NUMERIC(15,6)  		--//  ENCODE delta
	,signature_timestamp NUMERIC(15,1)  		--//  ENCODE delta
	,state VARCHAR(10)  		--//  ENCODE zstd
	,subject VARCHAR(128)  		--//  ENCODE zstd
	,submit_timestamp NUMERIC(15,1)  		--//  ENCODE delta
	,total_expense_attendees_count NUMERIC(15,1)  		--//  ENCODE delta
	,any_aepqc_ss_i_have_reported_within_24h VARCHAR(255)  		--//  ENCODE zstd
	,check_in_latitude NUMERIC(15,6)  		--//  ENCODE delta
	,check_in_location_services_status VARCHAR(255)  		--//  ENCODE zstd
	,check_in_longitude NUMERIC(15,6)  		--//  ENCODE delta
	,check_in_status VARCHAR(255)  		--//  ENCODE zstd
	,check_in_timestamp NUMERIC(15,1)  		--//  ENCODE delta
	,child_account_id VARCHAR(40)  		--//  ENCODE zstd
	,child_account VARCHAR(18)  		--//  ENCODE zstd
	,call_clm_location_latitude NUMERIC(15,6)  		--//  ENCODE delta
	,call_clm_location_longitude NUMERIC(15,6)  		--//  ENCODE delta
	,call_clm_location_services_status VARCHAR(255)  		--//  ENCODE zstd
	,disbursement_created numeric(18,0)		--//  ENCODE delta // INTEGER  
	,em_event VARCHAR(18)  		--//  ENCODE zstd
	,location_id VARCHAR(40)  		--//  ENCODE zstd
	,location_name VARCHAR(18)  		--//  ENCODE zstd
	,location_text VARCHAR(255)  		--//  ENCODE zstd
	,medical_discussions VARCHAR(255)  		--//  ENCODE zstd
	,medical_inquiry VARCHAR(18)  		--//  ENCODE zstd
	,parent_address_id VARCHAR(255)  		--//  ENCODE zstd
	,parent_call_mobile_id VARCHAR(100)  		--//  ENCODE zstd
	,signature_on_sync VARCHAR(1300)  		--//  ENCODE zstd
	,signature_page_display_name VARCHAR(255)  		--//  ENCODE zstd
	,submit_location_latitude NUMERIC(15,6)  		--//  ENCODE delta
	,submit_location_longitude NUMERIC(15,6)  		--//  ENCODE delta
	,submit_location_services_status VARCHAR(255)  		--//  ENCODE zstd
	,suggestion VARCHAR(18)  		--//  ENCODE zstd
	,veeva_remote_meeting_id VARCHAR(20)  		--//  ENCODE zstd
	,account_classification VARCHAR(255)  		--//  ENCODE zstd
	,end_datetime TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,inserted_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,updated_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,remote_meeting_source_id VARCHAR(18)  		--//  ENCODE zstd
	,call_channel VARCHAR(255)  		--//  ENCODE zstd
	,fml_simp_case_product_discussion NUMERIC(18,2)  		--//  ENCODE delta
	,fml_simp_in_service NUMERIC(18,2)  		--//  ENCODE delta
	,fml_simp_overseas_workshop_visit NUMERIC(18,2)  		--//  ENCODE delta
	,signature_captured_remotely VARCHAR(5)  		--//  ENCODE zstd
	,virtual_channel_option VARCHAR(255)  		--//  ENCODE zstd
	,thmd_call_objectives VARCHAR(4099)  		--//  ENCODE zstd
	,thmd_case_type VARCHAR(255)  		--//  ENCODE zstd
	,phmd_call_objective VARCHAR(4099)  		--//  ENCODE zstd
	,idmd_call_objectives VARCHAR(4099)  		--//  ENCODE zstd
	,gsg_case_type VARCHAR(255)  		--//  ENCODE zstd
	,sis_case_type VARCHAR(255)  		--//  ENCODE zstd
	,PRIMARY KEY (call_key, call_source_id, country_code)
)

;
--DROP TABLE HCPOSEITG_INTEGRATION.itg_call_detail;
CREATE OR REPLACE TABLE HCPOSEITG_INTEGRATION.ITG_CALL_DETAIL		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE HCPOSEITG_INTEGRATION.itg_call_detail
(
	call_detail_source_id VARCHAR(18) NOT NULL 		--//  ENCODE zstd
	,is_deleted VARCHAR(5)  		--//  ENCODE zstd
	,call_detail_name VARCHAR(80)  		--//  ENCODE zstd
	,created_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,created_by_id VARCHAR(18)  		--//  ENCODE zstd
	,last_modified_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,last_modified_by_id VARCHAR(18)  		--//  ENCODE zstd
	,is_parent_call NUMERIC(18,0)  		--//  ENCODE delta
	,call_source_id VARCHAR(18)  		--//  ENCODE zstd
	,product_source_id VARCHAR(18)  		--//  ENCODE zstd
	,call_detail_priority NUMERIC(2,0)  		--//  ENCODE delta
	,country_code VARCHAR(8) NOT NULL 		--//  ENCODE zstd
	,is_locked numeric(18,0)		--//  ENCODE delta // INTEGER  
	,detail_call_type VARCHAR(255)  		--//  ENCODE zstd
	,call_detail_group VARCHAR(18)  		--//  ENCODE zstd
	,product_id18 VARCHAR(1300)  		--//  ENCODE zstd
	,classification VARCHAR(40)  		--//  ENCODE zstd
	,my_adoption_ladder VARCHAR(255)  		--//  ENCODE zstd
	,simp_adoption_style VARCHAR(255)  		--//  ENCODE zstd
	,simp_behavioral_style VARCHAR(255)  		--//  ENCODE zstd
	,simp_market_share NUMERIC(18,1)  		--//  ENCODE delta
	,number_of_patients_per_quarter NUMERIC(18,1)  		--//  ENCODE delta
	,number_of_patients_per_month NUMERIC(18,1)  		--//  ENCODE delta
	,number_of_patients_per_week NUMERIC(18,1)  		--//  ENCODE delta
	,tw_adoption_ladder VARCHAR(255)  		--//  ENCODE zstd
	,tw_adoption_style VARCHAR(255)  		--//  ENCODE zstd
	,tw_behavioral_style VARCHAR(255)  		--//  ENCODE zstd
	,tw_market_share NUMERIC(18,1)  		--//  ENCODE delta
	,inserted_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,updated_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,PRIMARY KEY (call_detail_source_id, country_code)
)

;
--DROP TABLE HCPOSEITG_INTEGRATION.itg_call_discussion;
CREATE OR REPLACE TABLE HCPOSEITG_INTEGRATION.ITG_CALL_DISCUSSION		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE HCPOSEITG_INTEGRATION.itg_call_discussion
(
	call_discussion_source_id VARCHAR(18) NOT NULL 		--//  ENCODE zstd
	,is_deleted VARCHAR(5)  		--//  ENCODE zstd
	,call_discussion_name VARCHAR(80)  		--//  ENCODE zstd
	,record_type_source_id VARCHAR(18)  		--//  ENCODE zstd
	,created_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,created_by_id VARCHAR(18)  		--//  ENCODE zstd
	,last_modified_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,last_modified_by_id VARCHAR(18)  		--//  ENCODE zstd
	,account_source_id VARCHAR(18)  		--//  ENCODE zstd
	,call_source_id VARCHAR(18)  		--//  ENCODE zstd
	,comments VARCHAR(800)  		--//  ENCODE zstd
	,product_source_id VARCHAR(18)  		--//  ENCODE zstd
	,discussion_topics VARCHAR(255)  		--//  ENCODE zstd
	,medical_event VARCHAR(18)  		--//  ENCODE zstd
	,is_parent_call NUMERIC(18,0)  		--//  ENCODE delta
	,discussion_type VARCHAR(255)  		--//  ENCODE zstd
	,call_discussion_type VARCHAR(255)  		--//  ENCODE zstd
	,effectiveness VARCHAR(255)  		--//  ENCODE zstd
	,follow_up_activity VARCHAR(255)  		--//  ENCODE zstd
	,outcomes VARCHAR(255)  		--//  ENCODE zstd
	,follow_up_additional_info VARCHAR(800)  		--//  ENCODE zstd
	,follow_up_date DATE  		--//  ENCODE delta
	,materials_used VARCHAR(255)  		--//  ENCODE zstd
	,country_code VARCHAR(8) NOT NULL 		--//  ENCODE zstd
	,inserted_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,updated_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,call_date DATE  		--//  ENCODE delta
	,detail_group_source_id VARCHAR(18)  		--//  ENCODE zstd
	,user_source_id VARCHAR(18)  		--//  ENCODE zstd
	,PRIMARY KEY (call_discussion_source_id, country_code)
)

;
--DROP TABLE HCPOSEITG_INTEGRATION.itg_call_key_message;
CREATE OR REPLACE TABLE HCPOSEITG_INTEGRATION.ITG_CALL_KEY_MESSAGE		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE HCPOSEITG_INTEGRATION.itg_call_key_message
(
	call_key_message_id VARCHAR(18)  		--//  ENCODE zstd
	,call_key_message_name VARCHAR(300)  		--//  ENCODE zstd
	,call_key_message_createddate TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,call_key_message_createdbyid VARCHAR(18)  		--//  ENCODE zstd
	,last_modified_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,call_key_message_lastmodifiedbyid VARCHAR(18)  		--//  ENCODE zstd
	,call_key_message_systemmodstamp TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,call_key_message_account VARCHAR(18)  		--//  ENCODE zstd
	,call_key_message_call2 VARCHAR(18)  		--//  ENCODE zstd
	,call_key_message_reaction VARCHAR(800)  		--//  ENCODE zstd
	,call_key_message_product VARCHAR(18)  		--//  ENCODE zstd
	,call_key_message_key_message VARCHAR(18)  		--//  ENCODE zstd
	,call_key_message_contact VARCHAR(18)  		--//  ENCODE zstd
	,call_key_message_call_date DATE  		--//  ENCODE delta
	,call_key_message_user VARCHAR(18)  		--//  ENCODE zstd
	,call_key_message_category VARCHAR(800)  		--//  ENCODE zstd
	,call_key_message_vehicle VARCHAR(800)  		--//  ENCODE zstd
	,call_key_message_is_parent_call NUMERIC(18,0)  		--//  ENCODE delta
	,call_key_message_clm_id VARCHAR(500)  		--//  ENCODE zstd
	,call_key_message_slide_version VARCHAR(300)  		--//  ENCODE zstd
	,call_key_message_duration NUMERIC(18,2)  		--//  ENCODE delta
	,call_key_message_presentation_id VARCHAR(300)  		--//  ENCODE zstd
	,call_key_message_start_time TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,call_key_message_attendee_type VARCHAR(800)  		--//  ENCODE zstd
	,call_key_message_entity_reference_id VARCHAR(300)  		--//  ENCODE zstd
	,call_key_message_segment VARCHAR(300)  		--//  ENCODE zstd
	,call_key_message_display_order NUMERIC(18,0)  		--//  ENCODE zstd
	,call_key_message_detail_group VARCHAR(18)  		--//  ENCODE zstd
	,call_key_message_clm_presentation_name VARCHAR(300)  		--//  ENCODE zstd
	,country_code VARCHAR(8)  		--//  ENCODE zstd
	,call_key_message_isdeleted numeric(18,0)		--//  ENCODE delta // INTEGER  
	,call_key_message_clm_presentation_version VARCHAR(100)  		--//  ENCODE zstd
	,call_key_message_clm_presentation VARCHAR(18)  		--//  ENCODE zstd
	,key_message_name VARCHAR(80)  		--//  ENCODE zstd
	,key_message_description VARCHAR(1300)  		--//  ENCODE zstd
	,inserted_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,updated_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
)

;
--DROP TABLE HCPOSEITG_INTEGRATION.itg_coaching_report;
CREATE OR REPLACE TABLE HCPOSEITG_INTEGRATION.ITG_COACHING_REPORT		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE HCPOSEITG_INTEGRATION.itg_coaching_report
(
	country_key VARCHAR(18)  		--//  ENCODE zstd
	,id VARCHAR(18)  		--//  ENCODE zstd
	,ownerid VARCHAR(18)  		--//  ENCODE zstd
	,isdeleted VARCHAR(5)  		--//  ENCODE zstd
	,name VARCHAR(80)  		--//  ENCODE zstd
	,recordtypeid VARCHAR(18)  		--//  ENCODE zstd
	,createddate TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,createdbyid VARCHAR(18)  		--//  ENCODE zstd
	,lastmodifieddate TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,lastmodifiedbyid VARCHAR(18)  		--//  ENCODE zstd
	,systemmodstamp TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,lastactivitydate TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,mayedit VARCHAR(20)  		--//  ENCODE zstd
	,mobile_id VARCHAR(1000)  		--//  ENCODE zstd
	,manager VARCHAR(18)  		--//  ENCODE zstd
	,employee VARCHAR(18)  		--//  ENCODE zstd
	,review_date DATE  		--//  ENCODE delta
	,review_period VARCHAR(20)  		--//  ENCODE zstd
	,status VARCHAR(255)  		--//  ENCODE zstd
	,overall_rating VARCHAR(100)  		--//  ENCODE zstd
	,jj_core_country_code VARCHAR(10)  		--//  ENCODE zstd
	,jj_core_lock VARCHAR(20)  		--//  ENCODE zstd
	,jj_core_no_of_visits VARCHAR(10)  		--//  ENCODE zstd
	,jj_employee_review_and_ackCURRENT_TIMESTAMP()ledged VARCHAR(5)  		--// 	,jj_employee_review_and_acknowledged VARCHAR(5)   ENCODE zstd //  ENCODE zstd
	,jj_employee_comments VARCHAR(2000)  		--//  ENCODE zstd
	,jj_simp_manager_comments VARCHAR(65535)  		--//  ENCODE zstd
	,jj_simp_objectives VARCHAR(65535)  		--//  ENCODE zstd
	,jj_simp_rep_comments_long VARCHAR(65535)  		--//  ENCODE zstd
	,jj_simp_sg_overall_rating NUMERIC(18,1)  		--//  ENCODE delta
	,jj_simp_long_comments VARCHAR(5000)  		--//  ENCODE zstd
	,kCURRENT_TIMESTAMP()ledge_strategy_overall_rating NUMERIC(18,1)  		--// 	,knowledge_strategy_overall_rating NUMERIC(18,1)   ENCODE delta //  ENCODE delta
	,selling_skills_overall_rating NUMERIC(18,1)  		--//  ENCODE delta
	,jj_my_call_type VARCHAR(255)  		--//  ENCODE zstd
	,jj_my_location VARCHAR(255)  		--//  ENCODE zstd
	,jj_id_overall_rating NUMERIC(18,1)  		--//  ENCODE delta
	,jj_vn_md_overall_rating_med NUMERIC(18,1)  		--//  ENCODE delta
	,jj_agreed_next_steps VARCHAR(2000)  		--//  ENCODE zstd
	,jj_coaching_for_field_visits VARCHAR(5)  		--//  ENCODE zstd
	,jj_customer_interactions VARCHAR(2000)  		--//  ENCODE zstd
	,jj_date_of_review_concluded DATE  		--//  ENCODE delta
	,jj_general_observations_and_comments VARCHAR(2000)  		--//  ENCODE zstd
	,jj_manager_feedback_completed VARCHAR(5)  		--//  ENCODE zstd
	,jj_number_of_coaching_days VARCHAR(255)  		--//  ENCODE zstd
	,jj_second_line_manager VARCHAR(18)  		--//  ENCODE zstd
	,jj_submission_to_date NUMERIC(18,1)  		--//  ENCODE delta
	,relatedcoachingreport VARCHAR(255)  		--//  ENCODE zstd
	,updated_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,inserted_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
)

;
--DROP TABLE HCPOSEITG_INTEGRATION.itg_common_prod_hier;
CREATE OR REPLACE TABLE HCPOSEITG_INTEGRATION.ITG_COMMON_PROD_HIER		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE HCPOSEITG_INTEGRATION.itg_common_prod_hier
(
	country_code VARCHAR(8) NOT NULL 		--//  ENCODE zstd
	,product_sfid VARCHAR(255) NOT NULL 		--//  ENCODE zstd
	,product_name VARCHAR(255)  		--//  ENCODE zstd
	,product_type VARCHAR(255)  		--//  ENCODE zstd
	,product_hierarchy_code VARCHAR(255)  		--//  ENCODE zstd
	,brand_code VARCHAR(255)  		--//  ENCODE zstd
	,brand_name VARCHAR(255)  		--//  ENCODE zstd
	,da_code VARCHAR(255)  		--//  ENCODE zstd
	,da_name VARCHAR(255)  		--//  ENCODE zstd
	,ta_code VARCHAR(255)  		--//  ENCODE zstd
	,ta_name VARCHAR(255)  		--//  ENCODE zstd
	,description VARCHAR(255)  		--//  ENCODE zstd
	,attr1 VARCHAR(255)  		--//  ENCODE zstd
	,attr2 VARCHAR(255)  		--//  ENCODE zstd
	,attr3 VARCHAR(255)  		--//  ENCODE zstd
	,attr4 VARCHAR(255)  		--//  ENCODE zstd
	,attr5 VARCHAR(255)  		--//  ENCODE zstd
	,display_order VARCHAR(255)  		--//  ENCODE zstd
	,update_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,update_user VARCHAR(255)  		--//  ENCODE zstd
	,PRIMARY KEY (country_code, product_sfid)
)

;
--DROP TABLE HCPOSEITG_INTEGRATION.itg_country;
CREATE OR REPLACE TABLE HCPOSEITG_INTEGRATION.ITG_COUNTRY		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE HCPOSEITG_INTEGRATION.itg_country
(
	country_key VARCHAR(8)  		--//  ENCODE zstd
	,country_code VARCHAR(8)  		--//  ENCODE zstd
	,country_name VARCHAR(50)  		--//  ENCODE zstd
	,country_display_order NUMERIC(2,0)  		--//  ENCODE delta
	,country_group_code VARCHAR(8)  		--//  ENCODE zstd
	,country_group_name VARCHAR(50)  		--//  ENCODE zstd
	,country_group_display_order NUMERIC(2,0)  		--//  ENCODE delta
	,descript VARCHAR(50)  		--//  ENCODE zstd
	,attr1 VARCHAR(50)  		--//  ENCODE zstd
	,attr2 VARCHAR(50)  		--//  ENCODE zstd
	,attr3 VARCHAR(50)  		--//  ENCODE zstd
	,attr4 VARCHAR(50)  		--//  ENCODE zstd
	,attr5 VARCHAR(50)  		--//  ENCODE zstd
	,manual_update_date VARCHAR(50)  		--//  ENCODE zstd
	,manual_update_user VARCHAR(50)  		--//  ENCODE zstd
	,inserted_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,updated_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
)

;
--DROP TABLE HCPOSEITG_INTEGRATION.itg_cycle_plan;
CREATE OR REPLACE TABLE HCPOSEITG_INTEGRATION.ITG_CYCLE_PLAN		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE HCPOSEITG_INTEGRATION.itg_cycle_plan
(
	cycle_plan_source_id VARCHAR(18) NOT NULL 		--//  ENCODE zstd
	,owner_source_id VARCHAR(18)  		--//  ENCODE zstd
	,is_deleted numeric(18,0)		--//  ENCODE delta // INTEGER  
	,cycle_plan_name VARCHAR(80)  		--//  ENCODE zstd
	,created_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,created_by_id VARCHAR(18)  		--//  ENCODE zstd
	,last_modified_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,last_modified_by_id VARCHAR(18)  		--//  ENCODE zstd
	,is_active numeric(18,0)		--//  ENCODE delta // INTEGER  
	,attainment NUMERIC(18,0)  		--//  ENCODE delta
	,end_date DATE  		--//  ENCODE delta
	,external_id VARCHAR(100)  		--//  ENCODE zstd
	,start_date DATE  		--//  ENCODE delta
	,territory_name VARCHAR(100)  		--//  ENCODE zstd
	,actual_calls NUMERIC(6,0)  		--//  ENCODE delta
	,planned_calls NUMERIC(3,0)  		--//  ENCODE delta
	,status_type VARCHAR(255)  		--//  ENCODE zstd
	,mgr_s_email VARCHAR(80)  		--//  ENCODE zstd
	,manager VARCHAR(18)  		--//  ENCODE zstd
	,manager_name VARCHAR(50)  		--//  ENCODE zstd
	,country_code VARCHAR(2) NOT NULL 		--//  ENCODE zstd
	,cp_approval_time TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,approver_name VARCHAR(50)  		--//  ENCODE zstd
	,number_of_targets NUMERIC(18,0)  		--//  ENCODE delta
	,number_of_cfa_100_targets NUMERIC(18,0)  		--//  ENCODE delta
	,cycle_plan_attainment_cptarget NUMERIC(18,1)  		--//  ENCODE delta
	,mid_date DATE  		--//  ENCODE delta
	,hcp_product_achieved_100 NUMERIC(18,0)  		--//  ENCODE delta
	,hcp_products_planned NUMERIC(18,0)  		--//  ENCODE delta
	,cpa_100 NUMERIC(18,1)  		--//  ENCODE delta
	,close_out numeric(18,0)		--//  ENCODE delta // INTEGER  
	,ready_for_approval_flag numeric(18,0)		--//  ENCODE delta // INTEGER  
	,attainment_difference NUMERIC(18,1)  		--//  ENCODE delta
	,expected_attainment NUMERIC(18,1)  		--//  ENCODE delta
	,expected_calls NUMERIC(18,1)  		--//  ENCODE delta
	,is_locked numeric(18,0)		--//  ENCODE delta // INTEGER  
	,submitted_by VARCHAR(100)  		--//  ENCODE zstd
	,submitted_date_time TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,total_target_reached NUMERIC(18,1)  		--//  ENCODE delta
	,remaining NUMERIC(18,1)  		--//  ENCODE delta
	,inserted_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,updated_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,PRIMARY KEY (cycle_plan_source_id, country_code)
)

cluster by (cycle_plan_source_id)
;
--DROP TABLE HCPOSEITG_INTEGRATION.itg_cycle_plan_detail;
CREATE OR REPLACE TABLE HCPOSEITG_INTEGRATION.ITG_CYCLE_PLAN_DETAIL		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE HCPOSEITG_INTEGRATION.itg_cycle_plan_detail
(
	cycle_plan_detail_source_id VARCHAR(18) NOT NULL 		--//  ENCODE zstd
	,is_deleted VARCHAR(5)  		--//  ENCODE zstd
	,cycle_plan_detail_name VARCHAR(80)  		--//  ENCODE zstd
	,created_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,created_by_id VARCHAR(18)  		--//  ENCODE zstd
	,last_modified_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,last_modified_by_id VARCHAR(18)  		--//  ENCODE zstd
	,cycle_plan_target_source_id VARCHAR(18)  		--//  ENCODE zstd
	,actual_details NUMERIC(3,0)  		--//  ENCODE delta
	,attainment NUMERIC(18,0)  		--//  ENCODE delta
	,planned_details NUMERIC(3,0)  		--//  ENCODE delta
	,product_source_id VARCHAR(18)  		--//  ENCODE zstd
	,scheduled_details NUMERIC(3,0)  		--//  ENCODE delta
	,total_actual_details NUMERIC(5,0)  		--//  ENCODE delta
	,total_attainment NUMERIC(18,0)  		--//  ENCODE delta
	,total_planned_details NUMERIC(5,0)  		--//  ENCODE delta
	,total_scheduled_details NUMERIC(5,0)  		--//  ENCODE delta
	,remaining NUMERIC(18,0)  		--//  ENCODE delta
	,total_remaining NUMERIC(18,0)  		--//  ENCODE delta
	,classification_type VARCHAR(255)  		--//  ENCODE zstd
	,cfa_100 NUMERIC(18,0)  		--//  ENCODE delta
	,cfa_33 NUMERIC(18,0)  		--//  ENCODE delta
	,cfa_66 NUMERIC(18,0)  		--//  ENCODE delta
	,country_code VARCHAR(2) NOT NULL 		--//  ENCODE zstd
	,is_locked numeric(18,0)		--//  ENCODE delta // INTEGER  
	,inserted_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,updated_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,adoption_style VARCHAR(255)  		--//  ENCODE zstd
	,PRIMARY KEY (cycle_plan_detail_source_id, country_code)
)

;
--DROP TABLE HCPOSEITG_INTEGRATION.itg_cycle_plan_target;
CREATE OR REPLACE TABLE HCPOSEITG_INTEGRATION.ITG_CYCLE_PLAN_TARGET		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE HCPOSEITG_INTEGRATION.itg_cycle_plan_target
(
	cycle_plan_target_source_id VARCHAR(18) NOT NULL 		--//  ENCODE zstd
	,is_deleted VARCHAR(5)  		--//  ENCODE zstd
	,cycle_plan_target_name VARCHAR(80)  		--//  ENCODE zstd
	,created_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,created_by_id VARCHAR(18)  		--//  ENCODE zstd
	,last_modified_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,last_modified_by_id VARCHAR(18)  		--//  ENCODE zstd
	,cycle_plan_vod_source_id VARCHAR(18)  		--//  ENCODE zstd
	,actual_calls NUMERIC(3,0)  		--//  ENCODE delta
	,attainment NUMERIC(18,0)  		--//  ENCODE delta
	,cycle_plan_account_source_id VARCHAR(18)  		--//  ENCODE zstd
	,original_planned_calls NUMERIC(3,0)  		--//  ENCODE delta
	,planned_calls NUMERIC(3,0)  		--//  ENCODE delta
	,total_actual_calls NUMERIC(5,0)  		--//  ENCODE delta
	,total_attainment NUMERIC(18,0)  		--//  ENCODE delta
	,total_planned_calls NUMERIC(5,0)  		--//  ENCODE delta
	,external_id VARCHAR(100)  		--//  ENCODE zstd
	,scheduled_calls NUMERIC(18,0)  		--//  ENCODE zstd
	,total_scheduled_calls NUMERIC(5,0)  		--//  ENCODE delta
	,remaining NUMERIC(18,0)  		--//  ENCODE delta
	,total_remaining NUMERIC(18,0)  		--//  ENCODE delta
	,remaining_schedule NUMERIC(18,0)  		--//  ENCODE delta
	,total_remaining_schedule NUMERIC(18,0)  		--//  ENCODE delta
	,primary_parent_name VARCHAR(1300)  		--//  ENCODE zstd
	,specialty_1 VARCHAR(1300)  		--//  ENCODE zstd
	,account_source_id VARCHAR(1300)  		--//  ENCODE zstd
	,cpt_cfa_100 NUMERIC(18,0)  		--//  ENCODE delta
	,cpt_cfa_66 NUMERIC(18,0)  		--//  ENCODE delta
	,cpt_cfa_33 NUMERIC(18,0)  		--//  ENCODE delta
	,number_of_cfa_100_details NUMERIC(18,0)  		--//  ENCODE delta
	,number_of_product_details NUMERIC(18,0)  		--//  ENCODE delta
	,country_code VARCHAR(2) NOT NULL 		--//  ENCODE zstd
	,classification VARCHAR(1300)  		--//  ENCODE zstd
	,is_locked numeric(18,0)		--//  ENCODE delta // INTEGER  
	,target_reached_flag NUMERIC(18,0)  		--//  ENCODE delta
	,inserted_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,updated_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,PRIMARY KEY (cycle_plan_target_source_id, country_code)
)

;
--DROP TABLE HCPOSEITG_INTEGRATION.itg_date_time;
CREATE OR REPLACE TABLE HCPOSEITG_INTEGRATION.ITG_DATE_TIME		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE HCPOSEITG_INTEGRATION.itg_date_time
(
	date_key numeric(18,0) NOT NULL 		--//  ENCODE delta // INTEGER 
	,date_value TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,date_year numeric(18,0)		--//  ENCODE delta // INTEGER  
	,date_quarter VARCHAR(2)  		--//  ENCODE zstd
	,date_quarterlong VARCHAR(5)  		--//  ENCODE zstd
	,date_month VARCHAR(3)  		--//  ENCODE zstd
	,date_monthlong VARCHAR(10)  		--//  ENCODE zstd
	,date_monthnum numeric(18,0)		--//  ENCODE delta // INTEGER  
	,date_yearmonth VARCHAR(10)  		--//  ENCODE zstd
	,date_yearmonthnum numeric(18,0)		--//  ENCODE delta // INTEGER  
	,date_dayofweek VARCHAR(10)  		--//  ENCODE zstd
	,date_dayofmonth numeric(18,0)		--//  ENCODE delta // INTEGER  
	,date_weekofmonth numeric(18,0)		--//  ENCODE delta // INTEGER  
	,date_weekofquarter numeric(18,0)		--//  ENCODE delta // INTEGER  
	,date_dayofyear numeric(18,0)		--//  ENCODE delta // INTEGER  
	,date_weekofyear numeric(18,0)		--//  ENCODE delta // INTEGER  
	,jnj_date_year numeric(18,0)		--//  ENCODE delta // INTEGER  
	,jnj_date_quarter VARCHAR(5)  		--//  ENCODE zstd
	,jnj_date_quarterlong VARCHAR(10)  		--//  ENCODE zstd
	,jnj_date_month VARCHAR(5)  		--//  ENCODE zstd
	,jnj_date_monthlong VARCHAR(10)  		--//  ENCODE zstd
	,jnj_date_dayofmonth numeric(18,0)		--//  ENCODE delta // INTEGER  
	,jnj_date_weekofmonth numeric(18,0)		--//  ENCODE delta // INTEGER  
	,jnj_date_weekofquarter numeric(18,0)		--//  ENCODE delta // INTEGER  
	,jnj_date_dayofyear numeric(18,0)		--//  ENCODE delta // INTEGER  
	,jnj_date_weekofyear numeric(18,0)		--//  ENCODE delta // INTEGER  
	,manual_update_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,manual_update_user VARCHAR(100)  		--//  ENCODE zstd
	,inserted_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,updated_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,rpt_date_year numeric(18,0)		--//  ENCODE delta // INTEGER  
	,rpt_date_quarter CHAR(2)  		--//  ENCODE zstd
	,rpt_date_quarterlong CHAR(5)  		--//  ENCODE zstd
	,rpt_date_month CHAR(3)  		--//  ENCODE zstd
	,rpt_date_monthlong CHAR(10)  		--//  ENCODE zstd
	,rpt_date_dayofmonth numeric(18,0)		--//  ENCODE delta // INTEGER  
	,rpt_date_weekofmonth numeric(18,0)		--//  ENCODE delta // INTEGER  
	,rpt_date_weekofquarter numeric(18,0)		--//  ENCODE delta // INTEGER  
	,rpt_date_dayofyear numeric(18,0)		--//  ENCODE delta // INTEGER  
	,rpt_date_weekofyear numeric(18,0)		--//  ENCODE delta // INTEGER  
	,jnj_date_monthofyear CHAR(2)  		--//  ENCODE zstd
	,my_date_year numeric(18,0)		--//  ENCODE delta // INTEGER  
	,my_date_quarter CHAR(2)  		--//  ENCODE zstd
	,my_date_quarterlong CHAR(5)  		--//  ENCODE zstd
	,my_date_month CHAR(3)  		--//  ENCODE zstd
	,my_date_monthlong CHAR(10)  		--//  ENCODE zstd
	,my_date_dayofmonth numeric(18,0)		--//  ENCODE delta // INTEGER  
	,my_date_weekofmonth numeric(18,0)		--//  ENCODE delta // INTEGER  
	,my_date_weekofquarter numeric(18,0)		--//  ENCODE delta // INTEGER  
	,my_date_dayofyear numeric(18,0)		--//  ENCODE delta // INTEGER  
	,my_date_weekofyear numeric(18,0)		--//  ENCODE delta // INTEGER  
	,my_date_monthofyear CHAR(2)  		--//  ENCODE zstd
	,PRIMARY KEY (date_key)
)

cluster by (date_key)
		--// SORTKEY ( 
		--// 	rpt_date_month
		--// 	)
;		--// ;
--DROP TABLE HCPOSEITG_INTEGRATION.itg_holiday_list;
CREATE OR REPLACE TABLE HCPOSEITG_INTEGRATION.ITG_HOLIDAY_LIST		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE HCPOSEITG_INTEGRATION.itg_holiday_list
(
	country VARCHAR(5)  		--//  ENCODE lzo
	,holiday_key VARCHAR(20)  		--//  ENCODE lzo
	,inserted_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE lzo
	,updated_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE lzo
)

;
--DROP TABLE HCPOSEITG_INTEGRATION.itg_isight_licenses;
CREATE OR REPLACE TABLE HCPOSEITG_INTEGRATION.ITG_ISIGHT_LICENSES		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE HCPOSEITG_INTEGRATION.itg_isight_licenses
(
	year NUMERIC(18,0)  		--//  ENCODE delta
	,country VARCHAR(255)  		--//  ENCODE zstd
	,sector VARCHAR(255)  		--//  ENCODE zstd
	,qty NUMERIC(18,0)  		--//  ENCODE delta
	,licensetype VARCHAR(255)  		--//  ENCODE zstd
	,inserted_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,updated_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
)

;
--DROP TABLE HCPOSEITG_INTEGRATION.itg_isight_sector_mapping;
CREATE OR REPLACE TABLE HCPOSEITG_INTEGRATION.ITG_ISIGHT_SECTOR_MAPPING		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE HCPOSEITG_INTEGRATION.itg_isight_sector_mapping
(
	country VARCHAR(256)  		--//  ENCODE zstd
	,company VARCHAR(256)  		--//  ENCODE zstd
	,division VARCHAR(256)  		--//  ENCODE zstd
	,sector VARCHAR(256)  		--//  ENCODE zstd
	,inserted_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,updated_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
)

;
--DROP TABLE HCPOSEITG_INTEGRATION.itg_key_message;
CREATE OR REPLACE TABLE HCPOSEITG_INTEGRATION.ITG_KEY_MESSAGE		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE HCPOSEITG_INTEGRATION.itg_key_message
(
	key_message_id VARCHAR(18)  		--//  ENCODE zstd
	,key_message_ownerid VARCHAR(18)  		--//  ENCODE zstd
	,key_message_name VARCHAR(250)  		--//  ENCODE zstd
	,key_message_createddate TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,key_message_createdbyid VARCHAR(18)  		--//  ENCODE zstd
	,last_modified_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,key_message_lastmodifiedbyid VARCHAR(18)  		--//  ENCODE zstd
	,key_message_systemmodstamp TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,key_message_lastvieweddate TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,key_message_lastreferenceddate TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,key_message_description VARCHAR(800)  		--//  ENCODE zstd
	,key_message_product VARCHAR(18)  		--//  ENCODE zstd
	,key_message_product_strategy VARCHAR(18)  		--//  ENCODE zstd
	,key_message_active numeric(18,0)		--//  ENCODE delta // INTEGER  
	,key_message_category VARCHAR(255)  		--//  ENCODE zstd
	,key_message_vehicle VARCHAR(255)  		--//  ENCODE zstd
	,key_message_clm_id VARCHAR(100)  		--//  ENCODE zstd
	,key_message_custom_reaction VARCHAR(255)  		--//  ENCODE zstd
	,key_message_slide_version VARCHAR(100)  		--//  ENCODE zstd
	,key_message_language VARCHAR(255)  		--//  ENCODE zstd
	,key_message_media_file_crc VARCHAR(255)  		--//  ENCODE zstd
	,key_message_media_file_name VARCHAR(255)  		--//  ENCODE zstd
	,key_message_media_file_size NUMERIC(18,0)  		--//  ENCODE delta
	,key_message_segment VARCHAR(80)  		--//  ENCODE zstd
	,key_message_detail_group VARCHAR(18)  		--//  ENCODE zstd
	,key_message_core_content_approval_id VARCHAR(100)  		--//  ENCODE zstd
	,key_message_core_content_expiration_date DATE  		--//  ENCODE delta
	,country_code VARCHAR(1300)  		--//  ENCODE zstd
	,ap_clm_country VARCHAR(4)  		--//  ENCODE zstd
	,key_message_is_shared_resource numeric(18,0)		--//  ENCODE delta // INTEGER  
	,key_message_shared_resource VARCHAR(18)  		--//  ENCODE zstd
	,key_message_recordtypeid VARCHAR(18)  		--//  ENCODE zstd
	,key_message_display_order NUMERIC(3,0)  		--//  ENCODE delta
	,key_message_vexternal_id VARCHAR(255)  		--//  ENCODE zstd
	,key_message_cdn_path VARCHAR(255)  		--//  ENCODE zstd
	,key_message_status VARCHAR(255)  		--//  ENCODE zstd
	,ap_country VARCHAR(4)  		--//  ENCODE zstd
	,functional_team VARCHAR(255)  		--//  ENCODE zstd
	,janssen_code VARCHAR(150)  		--//  ENCODE zstd
	,inserted_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,updated_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,vault_document_id VARCHAR(100)  		--//  ENCODE zstd
	,key_message_group VARCHAR(4099)  		--//  ENCODE zstd
	,key_message_sub_group VARCHAR(4099)  		--//  ENCODE zstd
	,key_message_purpose VARCHAR(255)  		--//  ENCODE zstd
	,key_message_content_topic VARCHAR(255)  		--//  ENCODE zstd
	,key_message_content_sub_topic VARCHAR(255)  		--//  ENCODE zstd
)

;
--DROP TABLE HCPOSEITG_INTEGRATION.itg_lookup_eng_data;
CREATE OR REPLACE TABLE HCPOSEITG_INTEGRATION.ITG_LOOKUP_ENG_DATA		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE HCPOSEITG_INTEGRATION.itg_lookup_eng_data
(
	country_code VARCHAR(8)  		--//  ENCODE zstd
	,table_name VARCHAR(255)  		--//  ENCODE zstd
	,column_name VARCHAR(255)  		--//  ENCODE zstd
	,key_value VARCHAR(255)  		--//  ENCODE zstd
	,target_column_name VARCHAR(255)  		--//  ENCODE zstd
	,target_value VARCHAR(255)  		--//  ENCODE zstd
	,updated_user VARCHAR(255)  		--//  ENCODE zstd
	,updated_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
)

;
--DROP TABLE HCPOSEITG_INTEGRATION.itg_lookup_retention_period;
CREATE OR REPLACE TABLE HCPOSEITG_INTEGRATION.ITG_LOOKUP_RETENTION_PERIOD		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE HCPOSEITG_INTEGRATION.itg_lookup_retention_period
(
	table_name VARCHAR(255)  		--//  ENCODE zstd
	,retention_years numeric(18,0)		--//  ENCODE delta // INTEGER  
	,update_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,update_user VARCHAR(18)  		--//  ENCODE zstd
)

;
--DROP TABLE HCPOSEITG_INTEGRATION.itg_product;
CREATE OR REPLACE TABLE HCPOSEITG_INTEGRATION.ITG_PRODUCT		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE HCPOSEITG_INTEGRATION.itg_product
(
	product_indication_key VARCHAR(32) NOT NULL 		--//  ENCODE zstd
	,parent_product_key VARCHAR(32)  		--//  ENCODE zstd
	,product_source_id VARCHAR(18) NOT NULL 		--//  ENCODE zstd
	,owner_source_id VARCHAR(100)  		--//  ENCODE zstd
	,is_deleted numeric(18,0)		--//  ENCODE delta // INTEGER  
	,product_name VARCHAR(80)  		--//  ENCODE zstd
	,record_type_source_id VARCHAR(18)  		--//  ENCODE zstd
	,created_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,created_by_id VARCHAR(18)  		--//  ENCODE zstd
	,last_modified_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,last_modified_by_id VARCHAR(18)  		--//  ENCODE zstd
	,consumer_site VARCHAR(255)  		--//  ENCODE zstd
	,product_info VARCHAR(255)  		--//  ENCODE zstd
	,therapeutic_class VARCHAR(255)  		--//  ENCODE zstd
	,parent_product_source_id VARCHAR(18)  		--//  ENCODE zstd
	,therapeutic_area VARCHAR(255)  		--//  ENCODE zstd
	,product_type VARCHAR(255)  		--//  ENCODE zstd
	,require_key_message numeric(18,0)		--//  ENCODE delta // INTEGER  
	,external_id VARCHAR(25)  		--//  ENCODE zstd
	,manufacturer VARCHAR(255)  		--//  ENCODE zstd
	,company_product numeric(18,0)		--//  ENCODE delta // INTEGER  
	,controlled_substance numeric(18,0)		--//  ENCODE delta // INTEGER  
	,description VARCHAR(255)  		--//  ENCODE zstd
	,sample_quantity_picklist VARCHAR(1000)  		--//  ENCODE zstd
	,display_order NUMERIC(5,0)  		--//  ENCODE delta
	,no_metrics numeric(18,0)		--//  ENCODE delta // INTEGER  
	,distributor VARCHAR(255)  		--//  ENCODE zstd
	,sample_quantity_bound numeric(18,0)		--//  ENCODE delta // INTEGER  
	,sample_u_m VARCHAR(255)  		--//  ENCODE zstd
	,no_details numeric(18,0)		--//  ENCODE delta // INTEGER  
	,restricted numeric(18,0)		--//  ENCODE delta // INTEGER  
	,no_cycle_plans numeric(18,0)		--//  ENCODE delta // INTEGER  
	,sku_id VARCHAR(25)  		--//  ENCODE zstd
	,business_unit VARCHAR(4099)  		--//  ENCODE zstd
	,franchise VARCHAR(4099)  		--//  ENCODE zstd
	,country_code VARCHAR(255) NOT NULL 		--//  ENCODE zstd
	,biz_sub_unit VARCHAR(255)  		--//  ENCODE zstd
	,biz_unit VARCHAR(255)  		--//  ENCODE zstd
	,product_sector VARCHAR(1300)  		--//  ENCODE zstd
	,cost NUMERIC(14,2)  		--//  ENCODE delta
	,quantity_per_case NUMERIC(10,1)  		--//  ENCODE delta
	,user_aligned numeric(18,0)		--//  ENCODE delta // INTEGER  
	,restricted_states VARCHAR(100)  		--//  ENCODE zstd
	,sort_code VARCHAR(20)  		--//  ENCODE zstd
	,vexternal_id VARCHAR(120)  		--//  ENCODE zstd
	,product_identifier VARCHAR(80)  		--//  ENCODE zstd
	,imr numeric(18,0)		--//  ENCODE delta // INTEGER  
	,detail_sub_type VARCHAR(255)  		--//  ENCODE zstd
	,shc_sector VARCHAR(255)  		--//  ENCODE zstd
	,shc_strategic_group VARCHAR(255)  		--//  ENCODE zstd
	,shc_franchise VARCHAR(255)  		--//  ENCODE zstd
	,shc_brand VARCHAR(255)  		--//  ENCODE zstd
	,inserted_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,updated_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,PRIMARY KEY (product_source_id, country_code)
)

;
--DROP TABLE HCPOSEITG_INTEGRATION.itg_product_metrics;
CREATE OR REPLACE TABLE HCPOSEITG_INTEGRATION.ITG_PRODUCT_METRICS		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE HCPOSEITG_INTEGRATION.itg_product_metrics
(
	product_metrics_source_id VARCHAR(18) NOT NULL 		--//  ENCODE zstd
	,is_deleted numeric(18,0)		--//  ENCODE delta // INTEGER  
	,pm_name VARCHAR(80)  		--//  ENCODE zstd
	,created_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,created_by_id VARCHAR(18)  		--//  ENCODE zstd
	,last_modified_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,last_modified_by_id VARCHAR(18)  		--//  ENCODE zstd
	,account_source_id VARCHAR(18)  		--//  ENCODE zstd
	,awareness VARCHAR(255)  		--//  ENCODE zstd
	,movement NUMERIC(5,2)  		--//  ENCODE delta
	,product_source_id VARCHAR(18)  		--//  ENCODE zstd
	,segment VARCHAR(255)  		--//  ENCODE zstd
	,x12_mo_trx_chg NUMERIC(5,2)  		--//  ENCODE delta
	,speaker_skills VARCHAR(255)  		--//  ENCODE zstd
	,investigator_readiness VARCHAR(255)  		--//  ENCODE zstd
	,engagements NUMERIC(4,0)  		--//  ENCODE delta
	,external_id VARCHAR(255)  		--//  ENCODE zstd
	,decile NUMERIC(18,0)  		--//  ENCODE delta
	,adoption_level VARCHAR(255)  		--//  ENCODE zstd
	,detail_group VARCHAR(18)  		--//  ENCODE zstd
	,classification_type VARCHAR(255)  		--//  ENCODE zstd
	,company NUMERIC(18,0)  		--//  ENCODE delta
	,believer_of_adherence VARCHAR(255)  		--//  ENCODE zstd
	,influence_level VARCHAR(255)  		--//  ENCODE zstd
	,intention_for_future_sust VARCHAR(255)  		--//  ENCODE zstd
	,likely_to_ini_sustenna VARCHAR(255)  		--//  ENCODE zstd
	,likely_to_switch VARCHAR(255)  		--//  ENCODE zstd
	,penetration NUMERIC(3,0)  		--//  ENCODE delta
	,potential NUMERIC(3,0)  		--//  ENCODE delta
	,prescriber VARCHAR(255)  		--//  ENCODE zstd
	,number_of_patients_month NUMERIC(3,0)  		--//  ENCODE delta
	,schizophrenia_pts NUMERIC(6,0)  		--//  ENCODE delta
	,scientific_data VARCHAR(255)  		--//  ENCODE zstd
	,type_of_setting VARCHAR(255)  		--//  ENCODE zstd
	,usagof_sustenna_upon_discharging VARCHAR(255)  		--//  ENCODE zstd
	,usage_of_branded_atyp VARCHAR(255)  		--//  ENCODE zstd
	,kol_experts numeric(18,0)		--//  ENCODE delta // INTEGER  
	,nbrofmmptsyr NUMERIC(10,0)  		--//  ENCODE delta
	,practice_type VARCHAR(255)  		--//  ENCODE zstd
	,stdguideline VARCHAR(255)  		--//  ENCODE zstd
	,perception VARCHAR(255)  		--//  ENCODE zstd
	,prescription_behavior VARCHAR(255)  		--//  ENCODE zstd
	,scientifically_driven VARCHAR(255)  		--//  ENCODE zstd
	,treatment_pattern VARCHAR(255)  		--//  ENCODE zstd
	,physician_behaviour VARCHAR(255)  		--//  ENCODE zstd
	,product_preference VARCHAR(255)  		--//  ENCODE zstd
	,specialty VARCHAR(255)  		--//  ENCODE zstd
	,company_loyalty VARCHAR(255)  		--//  ENCODE zstd
	,biologics_user VARCHAR(255)  		--//  ENCODE zstd
	,price_sensitivity VARCHAR(255)  		--//  ENCODE zstd
	,usage_of_generic VARCHAR(255)  		--//  ENCODE zstd
	,previous_tramadol_experience VARCHAR(255)  		--//  ENCODE zstd
	,interest_to_treat_disease_area VARCHAR(255)  		--//  ENCODE zstd
	,satisfaction_with_alternative_tr VARCHAR(255)  		--//  ENCODE zstd
	,patient_share VARCHAR(255)  		--//  ENCODE zstd
	,country_code VARCHAR(1300) NOT NULL 		--//  ENCODE zstd
	,physician_product_preference VARCHAR(255)  		--//  ENCODE zstd
	,physician_prescription VARCHAR(255)  		--//  ENCODE zstd
	,adoption_ladder VARCHAR(255)  		--//  ENCODE zstd
	,ratings_points VARCHAR(255)  		--//  ENCODE zstd
	,peer_influence VARCHAR(255)  		--//  ENCODE zstd
	,innovations VARCHAR(255)  		--//  ENCODE zstd
	,cases_loads_per_year VARCHAR(255)  		--//  ENCODE zstd
	,sales_value_per_year VARCHAR(255)  		--//  ENCODE zstd
	,support VARCHAR(255)  		--//  ENCODE zstd
	,cmd VARCHAR(255)  		--//  ENCODE zstd
	,md_asp_classification VARCHAR(255)  		--//  ENCODE zstd
	,no_of_products_used VARCHAR(255)  		--//  ENCODE zstd
	,orientation_field VARCHAR(255)  		--//  ENCODE zstd
	,uptravi_usage VARCHAR(255)  		--//  ENCODE zstd
	,inserted_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,updated_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,PRIMARY KEY (product_metrics_source_id, country_code)
)

;
--DROP TABLE HCPOSEITG_INTEGRATION.itg_profile;
CREATE OR REPLACE TABLE HCPOSEITG_INTEGRATION.ITG_PROFILE		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE HCPOSEITG_INTEGRATION.itg_profile
(
	profile_key VARCHAR(32)  		--//  ENCODE zstd
	,profile_source_id VARCHAR(18) NOT NULL 		--//  ENCODE zstd
	,profile_name VARCHAR(255)  		--//  ENCODE zstd
	,type VARCHAR(40)  		--//  ENCODE zstd
	,userlicense_source_id VARCHAR(18)  		--//  ENCODE zstd
	,usertype VARCHAR(40)  		--//  ENCODE zstd
	,created_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,created_by_id VARCHAR(30)  		--//  ENCODE zstd
	,last_modified_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,last_modified_by_id VARCHAR(18)  		--//  ENCODE zstd
	,description VARCHAR(255)  		--//  ENCODE zstd
	,country_code VARCHAR(255) NOT NULL 		--//  ENCODE zstd
	,inserted_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,updated_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,PRIMARY KEY (profile_source_id, country_code)
)

;
--DROP TABLE HCPOSEITG_INTEGRATION.itg_profile_iconnect;
CREATE OR REPLACE TABLE HCPOSEITG_INTEGRATION.ITG_PROFILE_ICONNECT		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE HCPOSEITG_INTEGRATION.itg_profile_iconnect
(
	profile_key VARCHAR(32)  		--//  ENCODE lzo
	,profile_source_id VARCHAR(18)  		--//  ENCODE lzo
	,profile_name VARCHAR(255)  		--//  ENCODE lzo
	,type VARCHAR(40)  		--//  ENCODE lzo
	,userlicense_source_id VARCHAR(18)  		--//  ENCODE lzo
	,usertype VARCHAR(40)  		--//  ENCODE lzo
	,created_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,created_by_id VARCHAR(30)  		--//  ENCODE lzo
	,last_modified_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,last_modified_by_id VARCHAR(18)  		--//  ENCODE lzo
	,description VARCHAR(255)  		--//  ENCODE lzo
	,country_code VARCHAR(255)  		--//  ENCODE lzo
	,inserted_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,updated_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)

;
--DROP TABLE HCPOSEITG_INTEGRATION.itg_recordtype;
CREATE OR REPLACE TABLE HCPOSEITG_INTEGRATION.ITG_RECORDTYPE		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE HCPOSEITG_INTEGRATION.itg_recordtype
(
	record_type_source_id VARCHAR(18) NOT NULL 		--//  ENCODE zstd
	,record_type_name VARCHAR(255)  		--//  ENCODE zstd
	,developer_name VARCHAR(80)  		--//  ENCODE zstd
	,name_space_prefix VARCHAR(15)  		--//  ENCODE zstd
	,description VARCHAR(255)  		--//  ENCODE zstd
	,business_process_id VARCHAR(18)  		--//  ENCODE zstd
	,sobjecttype VARCHAR(40)  		--//  ENCODE zstd
	,is_active VARCHAR(5)  		--//  ENCODE zstd
	,is_person_type VARCHAR(5)  		--//  ENCODE zstd
	,created_by_id VARCHAR(30)  		--//  ENCODE zstd
	,created_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,last_modified_by_id VARCHAR(18)  		--//  ENCODE zstd
	,last_modified_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,country_code VARCHAR(255) NOT NULL 		--//  ENCODE zstd
	,inserted_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,updated_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,PRIMARY KEY (record_type_source_id, country_code)
)

;
--DROP TABLE HCPOSEITG_INTEGRATION.itg_recordtype_iconnect;
CREATE OR REPLACE TABLE HCPOSEITG_INTEGRATION.ITG_RECORDTYPE_ICONNECT		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE HCPOSEITG_INTEGRATION.itg_recordtype_iconnect
(
	record_type_source_id VARCHAR(18)  		--//  ENCODE lzo
	,record_type_name VARCHAR(255)  		--//  ENCODE lzo
	,developer_name VARCHAR(80)  		--//  ENCODE lzo
	,name_space_prefix VARCHAR(15)  		--//  ENCODE lzo
	,description VARCHAR(255)  		--//  ENCODE lzo
	,business_process_id VARCHAR(18)  		--//  ENCODE lzo
	,sobjecttype VARCHAR(40)  		--//  ENCODE lzo
	,is_active VARCHAR(5)  		--//  ENCODE lzo
	,is_person_type VARCHAR(5)  		--//  ENCODE lzo
	,created_by_id VARCHAR(30)  		--//  ENCODE lzo
	,created_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,last_modified_by_id VARCHAR(18)  		--//  ENCODE lzo
	,last_modified_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,country_code VARCHAR(255)  		--//  ENCODE lzo
	,inserted_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,updated_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)

;
--DROP TABLE HCPOSEITG_INTEGRATION.itg_remote_meeting;
CREATE OR REPLACE TABLE HCPOSEITG_INTEGRATION.ITG_REMOTE_MEETING		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE HCPOSEITG_INTEGRATION.itg_remote_meeting
(
	remote_meeting_source_id VARCHAR(18)  		--//  ENCODE lzo
	,country_code VARCHAR(10)  		--//  ENCODE lzo
	,owner_source_id VARCHAR(18)  		--//  ENCODE lzo
	,is_deleted VARCHAR(10)  		--//  ENCODE lzo
	,name VARCHAR(255)  		--//  ENCODE lzo
	,record_type_source_id VARCHAR(18)  		--//  ENCODE lzo
	,created_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,createdbyid VARCHAR(18)  		--//  ENCODE lzo
	,last_modified_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,lastmodifiedbyid VARCHAR(18)  		--//  ENCODE lzo
	,systemmodstamp TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,mayedit VARCHAR(10)  		--//  ENCODE lzo
	,islocked VARCHAR(10)  		--//  ENCODE lzo
	,meeting_id VARCHAR(18)  		--//  ENCODE lzo
	,meeting_name VARCHAR(255)  		--//  ENCODE lzo
	,mobile_source_id VARCHAR(100)  		--//  ENCODE lzo
	,scheduled_datetime TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,scheduled_flag VARCHAR(10)  		--//  ENCODE lzo
	,meeting_password VARCHAR(20)  		--//  ENCODE lzo
	,meeting_outcome_status VARCHAR(255)  		--//  ENCODE lzo
	,jj_host_country VARCHAR(20)  		--//  ENCODE lzo
	,jj_numofattendee NUMERIC(18,1)  		--//  ENCODE az64
	,assigned_host VARCHAR(200)  		--//  ENCODE lzo
	,attendance_report_process_status VARCHAR(200)  		--//  ENCODE lzo
	,description VARCHAR(1000)  		--//  ENCODE lzo
	,latest_meeting_start_datetime TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,webinar_alternative_host_1 VARCHAR(200)  		--//  ENCODE lzo
	,webinar_alternative_host_2 VARCHAR(200)  		--//  ENCODE lzo
	,webinar_alternative_host_3 VARCHAR(200)  		--//  ENCODE lzo
	,inserted_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,updated_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,rating_submitted VARCHAR(5)  		--//  ENCODE zstd
)

;
--DROP TABLE HCPOSEITG_INTEGRATION.itg_territory;
CREATE OR REPLACE TABLE HCPOSEITG_INTEGRATION.ITG_TERRITORY		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE HCPOSEITG_INTEGRATION.itg_territory
(
	territory_source_id VARCHAR(18) NOT NULL 		--//  ENCODE zstd
	,territory_name VARCHAR(80)  		--//  ENCODE zstd
	,territory_type_source_id VARCHAR(18)  		--//  ENCODE zstd
	,territory_model_source_id VARCHAR(18)  		--//  ENCODE zstd
	,parent_territory_source_id VARCHAR(18)  		--//  ENCODE zstd
	,description VARCHAR(1000)  		--//  ENCODE zstd
	,account_access_level VARCHAR(40)  		--//  ENCODE zstd
	,contact_access_level VARCHAR(40)  		--//  ENCODE zstd
	,last_modified_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,last_modified_by_id VARCHAR(18)  		--//  ENCODE zstd
	,system_mod_stamp TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,developer_name VARCHAR(80)  		--//  ENCODE zstd
	,parent_territory1_source_id VARCHAR(18)  		--//  ENCODE zstd
	,territory1_source_id VARCHAR(18)  		--//  ENCODE zstd
	,country_code VARCHAR(255)  		--//  ENCODE zstd
	,inserted_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,updated_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
)

;
--DROP TABLE HCPOSEITG_INTEGRATION.itg_territory_model;
CREATE OR REPLACE TABLE HCPOSEITG_INTEGRATION.ITG_TERRITORY_MODEL		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE HCPOSEITG_INTEGRATION.itg_territory_model
(
	territory_model_source_id VARCHAR(18) NOT NULL 		--//  ENCODE zstd
	,is_deleted VARCHAR(5)  		--//  ENCODE zstd
	,name VARCHAR(80)  		--//  ENCODE zstd
	,created_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,created_by_id VARCHAR(18)  		--//  ENCODE zstd
	,last_modified_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,last_modified_by_id VARCHAR(18)  		--//  ENCODE zstd
	,system_mod_stamp TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,may_edit VARCHAR(5)  		--//  ENCODE zstd
	,is_locked VARCHAR(5)  		--//  ENCODE zstd
	,description VARCHAR(255)  		--//  ENCODE zstd
	,activated_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,deactivated_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,state VARCHAR(255)  		--//  ENCODE zstd
	,developer_name VARCHAR(80)  		--//  ENCODE zstd
	,last_run_rules_end_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,is_clone_source VARCHAR(5)  		--//  ENCODE zstd
	,last_opp_terr_assign_end_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,country_code VARCHAR(10)  		--//  ENCODE zstd
	,inserted_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,updated_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
)

;
--DROP TABLE HCPOSEITG_INTEGRATION.itg_time_off_territory;
CREATE OR REPLACE TABLE HCPOSEITG_INTEGRATION.ITG_TIME_OFF_TERRITORY		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE HCPOSEITG_INTEGRATION.itg_time_off_territory
(
	tot_source_id VARCHAR(18) NOT NULL 		--//  ENCODE zstd
	,owner_source_id VARCHAR(18)  		--//  ENCODE zstd
	,is_deleted numeric(18,0)		--//  ENCODE delta // INTEGER  
	,tot_name VARCHAR(80)  		--//  ENCODE zstd
	,record_type_source_id VARCHAR(18)  		--//  ENCODE zstd
	,created_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,created_by_id VARCHAR(18)  		--//  ENCODE zstd
	,last_modified_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,last_modified_by_id VARCHAR(18)  		--//  ENCODE zstd
	,reason VARCHAR(255)  		--//  ENCODE zstd
	,territory_name VARCHAR(255)  		--//  ENCODE zstd
	,tot_date DATE  		--//  ENCODE delta
	,status_cd VARCHAR(255)  		--//  ENCODE zstd
	,time_type VARCHAR(255)  		--//  ENCODE zstd
	,working_hours_on NUMERIC(18,0)  		--//  ENCODE delta
	,mobile_id VARCHAR(100)  		--//  ENCODE zstd
	,working_hours_off VARCHAR(255)  		--//  ENCODE zstd
	,start_time VARCHAR(255)  		--//  ENCODE zstd
	,simp_time_on_time_off VARCHAR(255)  		--//  ENCODE zstd
	,simp_frml_hours_on NUMERIC(18,0)  		--//  ENCODE delta
	,frml_total_work_days NUMERIC(18,0)  		--//  ENCODE delta
	,simp_frml_non_working_hours_off NUMERIC(18,0)  		--//  ENCODE delta
	,frml_planned_work_days NUMERIC(18,0)  		--//  ENCODE delta
	,simp_description VARCHAR(800)  		--//  ENCODE zstd
	,time_on VARCHAR(255)  		--//  ENCODE zstd
	,user_name VARCHAR(18)  		--//  ENCODE zstd
	,user_profile VARCHAR(1300)  		--//  ENCODE zstd
	,sm_reason VARCHAR(255)  		--//  ENCODE zstd
	,calculatedhours_off NUMERIC(18,0)  		--//  ENCODE delta
	,total_time_off NUMERIC(18,0)  		--//  ENCODE delta
	,country_code VARCHAR(10) NOT NULL 		--//  ENCODE zstd
	,inserted_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,updated_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,approval_status VARCHAR(255)  		--//  ENCODE zstd
	,owners_manager_email_id VARCHAR(80)  		--//  ENCODE zstd
	,PRIMARY KEY (tot_source_id, country_code)
)
		--// DISTSTYLE KEY
cluster by (owner_source_id)
;
--DROP TABLE HCPOSEITG_INTEGRATION.itg_user;
CREATE OR REPLACE TABLE HCPOSEITG_INTEGRATION.ITG_USER		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE HCPOSEITG_INTEGRATION.itg_user
(
	employee_key VARCHAR(32) NOT NULL 		--//  ENCODE zstd
	,employee_source_id VARCHAR(18) NOT NULL 		--//  ENCODE zstd
	,user_name VARCHAR(80)  		--//  ENCODE zstd
	,employee_name VARCHAR(121)  		--//  ENCODE zstd
	,company_name VARCHAR(80)  		--//  ENCODE zstd
	,division VARCHAR(80)  		--//  ENCODE zstd
	,department VARCHAR(80)  		--//  ENCODE zstd
	,title VARCHAR(80)  		--//  ENCODE zstd
	,country VARCHAR(80)  		--//  ENCODE zstd
	,address VARCHAR(255)  		--//  ENCODE zstd
	,email VARCHAR(128)  		--//  ENCODE zstd
	,phone VARCHAR(40)  		--//  ENCODE zstd
	,mobile_phone VARCHAR(40)  		--//  ENCODE zstd
	,alias VARCHAR(8)  		--//  ENCODE zstd
	,nickname VARCHAR(40)  		--//  ENCODE zstd
	,is_active numeric(18,0)		--//  ENCODE delta // INTEGER  
	,timezonesidkey VARCHAR(40)  		--//  ENCODE zstd
	,user_role_source_id VARCHAR(18)  		--//  ENCODE zstd
	,receives_info_emails numeric(18,0)		--//  ENCODE delta // INTEGER  
	,employee_profile_id VARCHAR(18)  		--//  ENCODE zstd
	,local_employee_number VARCHAR(20)  		--//  ENCODE zstd
	,manager_source_id VARCHAR(18)  		--//  ENCODE zstd
	,last_login_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,created_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,created_by_id VARCHAR(18)  		--//  ENCODE zstd
	,last_modified_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,last_modified_by_id VARCHAR(18)  		--//  ENCODE zstd
	,federation_identifier VARCHAR(512)  		--//  ENCODE zstd
	,last_ipad_sync TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,country_code VARCHAR(8) NOT NULL 		--//  ENCODE zstd
	,wwid VARCHAR(9)  		--//  ENCODE zstd
	,region VARCHAR(80)  		--//  ENCODE zstd
	,profile_group_ap VARCHAR(1300)  		--//  ENCODE zstd
	,user_license VARCHAR(1300)  		--//  ENCODE zstd
	,manager_name VARCHAR(80)  		--//  ENCODE zstd
	,manager_wwid VARCHAR(9)  		--//  ENCODE zstd
	,inserted_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,updated_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,msl_primary_responsible_ta VARCHAR(255)  		--//  ENCODE zstd
	,msl_secondary_responsible_ta VARCHAR(255)  		--//  ENCODE zstd
	,last_name VARCHAR(80)  		--//  ENCODE zstd
	,first_name VARCHAR(40)  		--//  ENCODE zstd
	,city VARCHAR(40)  		--//  ENCODE zstd
	,state VARCHAR(80)  		--//  ENCODE zstd
	,postal_code VARCHAR(20)  		--//  ENCODE zstd
	,user_type VARCHAR(40)  		--//  ENCODE zstd
	,language_local_key VARCHAR(40)  		--//  ENCODE zstd
	,last_mobile_sync TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,veeva_user_type VARCHAR(255)  		--//  ENCODE zstd
	,veeva_country_code VARCHAR(255)  		--//  ENCODE zstd
	,shc_user_franchise VARCHAR(255)  		--//  ENCODE zstd
	,PRIMARY KEY (employee_key, employee_source_id)
)
		--// DISTSTYLE KEY
cluster by (employee_source_id)
;
--DROP TABLE HCPOSEITG_INTEGRATION.itg_user_iconnect;
CREATE OR REPLACE TABLE HCPOSEITG_INTEGRATION.ITG_USER_ICONNECT		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE HCPOSEITG_INTEGRATION.itg_user_iconnect
(
	employee_key VARCHAR(32)  		--//  ENCODE lzo
	,employee_source_id VARCHAR(18)  		--//  ENCODE lzo
	,user_name VARCHAR(80)  		--//  ENCODE lzo
	,employee_name VARCHAR(121)  		--//  ENCODE lzo
	,company_name VARCHAR(80)  		--//  ENCODE lzo
	,division VARCHAR(80)  		--//  ENCODE lzo
	,department VARCHAR(80)  		--//  ENCODE lzo
	,title VARCHAR(80)  		--//  ENCODE lzo
	,country VARCHAR(80)  		--//  ENCODE lzo
	,address VARCHAR(255)  		--//  ENCODE lzo
	,email VARCHAR(128)  		--//  ENCODE lzo
	,phone VARCHAR(40)  		--//  ENCODE lzo
	,mobile_phone VARCHAR(40)  		--//  ENCODE lzo
	,alias VARCHAR(8)  		--//  ENCODE lzo
	,nickname VARCHAR(40)  		--//  ENCODE lzo
	,is_active numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,timezonesidkey VARCHAR(40)  		--//  ENCODE lzo
	,user_role_source_id VARCHAR(18)  		--//  ENCODE lzo
	,receives_info_emails numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,employee_profile_id VARCHAR(18)  		--//  ENCODE lzo
	,local_employee_number VARCHAR(20)  		--//  ENCODE lzo
	,manager_source_id VARCHAR(18)  		--//  ENCODE lzo
	,last_login_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,created_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,created_by_id VARCHAR(18)  		--//  ENCODE lzo
	,last_modified_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,last_modified_by_id VARCHAR(18)  		--//  ENCODE lzo
	,federation_identifier VARCHAR(512)  		--//  ENCODE lzo
	,last_ipad_sync TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,country_code VARCHAR(8)  		--//  ENCODE lzo
	,wwid VARCHAR(9)  		--//  ENCODE lzo
	,region VARCHAR(80)  		--//  ENCODE lzo
	,profile_group_ap VARCHAR(1300)  		--//  ENCODE lzo
	,user_license VARCHAR(1300)  		--//  ENCODE lzo
	,manager_name VARCHAR(80)  		--//  ENCODE lzo
	,manager_wwid VARCHAR(9)  		--//  ENCODE lzo
	,inserted_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,updated_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,msl_primary_responsible_ta VARCHAR(255)  		--//  ENCODE lzo
	,msl_secondary_responsible_ta VARCHAR(255)  		--//  ENCODE lzo
	,last_name VARCHAR(80)  		--//  ENCODE zstd
	,first_name VARCHAR(40)  		--//  ENCODE zstd
	,city VARCHAR(40)  		--//  ENCODE zstd
	,state VARCHAR(80)  		--//  ENCODE zstd
	,postal_code VARCHAR(20)  		--//  ENCODE zstd
	,user_type VARCHAR(40)  		--//  ENCODE zstd
	,language_local_key VARCHAR(40)  		--//  ENCODE zstd
	,last_mobile_sync TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,veeva_user_type VARCHAR(255)  		--//  ENCODE zstd
	,veeva_country_code VARCHAR(255)  		--//  ENCODE zstd
	,shc_user_franchise VARCHAR(255)  		--//  ENCODE zstd
)

;
--DROP TABLE HCPOSEITG_INTEGRATION.itg_userterritory;
CREATE OR REPLACE TABLE HCPOSEITG_INTEGRATION.ITG_USERTERRITORY		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE HCPOSEITG_INTEGRATION.itg_userterritory
(
	user_territory_source_id VARCHAR(18)  		--//  ENCODE zstd
	,user_territory_user_source_id VARCHAR(18)  		--//  ENCODE zstd
	,territory_source_id VARCHAR(18)  		--//  ENCODE zstd
	,is_active VARCHAR(5)  		--//  ENCODE zstd
	,role_in_territory VARCHAR(255)  		--//  ENCODE zstd
	,last_modified_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,last_modified_by_id VARCHAR(18)  		--//  ENCODE zstd
	,country_code VARCHAR(10)  		--//  ENCODE zstd
	,inserted_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,updated_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
)

;



--DROP TABLE HCPOSEWKS_INTEGRATION.wks_hcp_osea_account_hco;
CREATE OR REPLACE TABLE HCPOSEWKS_INTEGRATION.WKS_HCP_OSEA_ACCOUNT_HCO		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE HCPOSEWKS_INTEGRATION.wks_hcp_osea_account_hco
(
	account_source_id VARCHAR(18)  		--//  ENCODE zstd
	,is_deleted VARCHAR(5)  		--//  ENCODE zstd
	,account_name VARCHAR(255)  		--//  ENCODE zstd
	,record_type_source_id VARCHAR(18)  		--//  ENCODE zstd
	,phone_number VARCHAR(40)  		--//  ENCODE zstd
	,fax_number VARCHAR(40)  		--//  ENCODE zstd
	,website VARCHAR(255)  		--//  ENCODE zstd
	,owner_id VARCHAR(18)  		--//  ENCODE zstd
	,created_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,created_by_id VARCHAR(18)  		--//  ENCODE zstd
	,last_modified_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,last_modified_by_id VARCHAR(18)  		--//  ENCODE zstd
	,is_excluded_from_realign VARCHAR(5)  		--//  ENCODE zstd
	,is_person_account VARCHAR(5)  		--//  ENCODE zstd
	,external_id VARCHAR(120)  		--//  ENCODE zstd
	,territory_name VARCHAR(255)  		--//  ENCODE zstd
	,hco_name VARCHAR(1300)  		--//  ENCODE zstd
	,beds NUMERIC(4,0)  		--//  ENCODE delta
	,primary_parent_source_id VARCHAR(18)  		--//  ENCODE zstd
	,total_mds_dos NUMERIC(18,0)  		--//  ENCODE delta
	,departments NUMERIC(18,0)  		--//  ENCODE delta
	,hco_type VARCHAR(255)  		--//  ENCODE zstd
	,hco_sector VARCHAR(255)  		--//  ENCODE zstd
	,remarks VARCHAR(255)  		--//  ENCODE zstd
	,sfe_approved VARCHAR(5)  		--//  ENCODE zstd
	,country_code VARCHAR(2)  		--//  ENCODE zstd
	,business_description VARCHAR(32000)  		--//  ENCODE zstd
	,hcc_account_approved VARCHAR(5)  		--//  ENCODE zstd
	,tw_customer_code VARCHAR(60)  		--//  ENCODE zstd
	,inactive VARCHAR(5)  		--//  ENCODE zstd
	,total_physicians_enrolled NUMERIC(18,0)  		--//  ENCODE delta
	,kam_clinical_differentiations VARCHAR(32768)  		--//  ENCODE zstd
	,kam_general_differnentiations VARCHAR(32768)  		--//  ENCODE zstd
	,total_pharmacists NUMERIC(3,0)  		--//  ENCODE delta
	,kam_total_aestheticsurgeons NUMERIC(18,0)  		--//  ENCODE delta
	,kam_total_cardiosurgeons NUMERIC(18,0)  		--//  ENCODE delta
	,kam_total_cardiologyphysicians NUMERIC(18,0)  		--//  ENCODE delta
	,kam_total_dermaphysicians NUMERIC(18,0)  		--//  ENCODE delta
	,kam_total_endophysicians NUMERIC(18,0)  		--//  ENCODE delta
	,kam_total_gastrophysicians NUMERIC(18,0)  		--//  ENCODE delta
	,kam_total_generalsurgeons NUMERIC(18,0)  		--//  ENCODE delta
	,kam_total_haemaphysicians NUMERIC(18,0)  		--//  ENCODE delta
	,kam_total_infectiousphysicians NUMERIC(18,0)  		--//  ENCODE delta
	,kam_total_medoncophysicians NUMERIC(18,0)  		--//  ENCODE delta
	,kam_total_neurologyphysicians NUMERIC(18,0)  		--//  ENCODE delta
	,kam_total_opthalsurgeons NUMERIC(18,0)  		--//  ENCODE delta
	,kam_total_orthosurgeons NUMERIC(18,0)  		--//  ENCODE delta
	,kam_total_psychiatryphysicians NUMERIC(18,0)  		--//  ENCODE delta
	,kam_total_rheumphysicians NUMERIC(18,0)  		--//  ENCODE delta
	,kam_total_surgeons NUMERIC(18,0)  		--//  ENCODE delta
	,kam_total_urologysurgeons NUMERIC(18,0)  		--//  ENCODE delta
	,kam_minimally_invasive NUMERIC(18,0)  		--//  ENCODE delta
	,kam_obgyn NUMERIC(18,0)  		--//  ENCODE delta
	,kam_paediatric NUMERIC(18,0)  		--//  ENCODE delta
	,ot NUMERIC(18,0)  		--//  ENCODE delta
	,is_external_id_number VARCHAR(5)  		--//  ENCODE zstd
	,jj_external_id VARCHAR(90)  		--//  ENCODE zstd
	,specialty_2 VARCHAR(255)  		--//  ENCODE zstd
	,sub_specialty VARCHAR(255)  		--//  ENCODE zstd
	,sea_account_classification VARCHAR(255)  		--//  ENCODE zstd
	,jj_email_1 VARCHAR(80)  		--//  ENCODE zstd
	,jj_email_2 VARCHAR(80)  		--//  ENCODE zstd
)

;
--DROP TABLE HCPOSEWKS_INTEGRATION.wks_hcp_osea_account_hcp;
CREATE OR REPLACE TABLE HCPOSEWKS_INTEGRATION.WKS_HCP_OSEA_ACCOUNT_HCP		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE HCPOSEWKS_INTEGRATION.wks_hcp_osea_account_hcp
(
	account_source_id VARCHAR(18)  		--//  ENCODE zstd
	,is_deleted VARCHAR(5)  		--//  ENCODE zstd
	,hcp_name VARCHAR(255)  		--//  ENCODE zstd
	,last_name VARCHAR(80)  		--//  ENCODE zstd
	,first_name VARCHAR(40)  		--//  ENCODE zstd
	,salutation VARCHAR(120)  		--//  ENCODE zstd
	,record_type_source_id VARCHAR(18)  		--//  ENCODE zstd
	,owner_id VARCHAR(18)  		--//  ENCODE zstd
	,created_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,created_by_id VARCHAR(18)  		--//  ENCODE zstd
	,last_modified_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,last_modified_by_id VARCHAR(18)  		--//  ENCODE zstd
	,is_excluded_from_realign VARCHAR(5)  		--//  ENCODE zstd
	,is_person_account VARCHAR(5)  		--//  ENCODE zstd
	,person_mobile_phone VARCHAR(40)  		--//  ENCODE zstd
	,person_email VARCHAR(80)  		--//  ENCODE zstd
	,birth_day DATE  		--//  ENCODE delta
	,person_has_opted_out_of_email VARCHAR(5)  		--//  ENCODE zstd
	,person_do_not_call VARCHAR(5)  		--//  ENCODE zstd
	,external_id VARCHAR(120)  		--//  ENCODE zstd
	,territory_name VARCHAR(255)  		--//  ENCODE zstd
	,specialty_1 VARCHAR(255)  		--//  ENCODE zstd
	,gender VARCHAR(255)  		--//  ENCODE zstd
	,preferred_name VARCHAR(12)  		--//  ENCODE zstd
	,primary_parent_source_id VARCHAR(18)  		--//  ENCODE zstd
	,hcp_type VARCHAR(255)  		--//  ENCODE zstd
	,professional_type VARCHAR(255)  		--//  ENCODE zstd
	,hcp_english_name VARCHAR(800)  		--//  ENCODE zstd
	,remarks VARCHAR(255)  		--//  ENCODE zstd
	,position VARCHAR(255)  		--//  ENCODE zstd
	,direct_line VARCHAR(40)  		--//  ENCODE zstd
	,direct_fax VARCHAR(40)  		--//  ENCODE zstd
	,sfe_approved VARCHAR(5)  		--//  ENCODE zstd
	,country_code VARCHAR(2)  		--//  ENCODE zstd
	,go_classification VARCHAR(255)  		--//  ENCODE zstd
	,hcc_account_approved VARCHAR(5)  		--//  ENCODE zstd
	,account_classification VARCHAR(255)  		--//  ENCODE zstd
	,customer_code_2 VARCHAR(20)  		--//  ENCODE zstd
	,is_kol VARCHAR(5)  		--//  ENCODE zstd
	,inactive VARCHAR(5)  		--//  ENCODE zstd
	,physician_prescribing_behavior VARCHAR(255)  		--//  ENCODE zstd
	,physician_behavioral_style VARCHAR(255)  		--//  ENCODE zstd
	,is_external_id_number VARCHAR(5)  		--//  ENCODE zstd
	,customer_value_segmentation VARCHAR(255)  		--//  ENCODE zstd
	,ability_to_influence_peers VARCHAR(255)  		--//  ENCODE zstd
	,practice_size VARCHAR(255)  		--//  ENCODE zstd
	,patient_type VARCHAR(255)  		--//  ENCODE zstd
	,patient_medical_condition VARCHAR(255)  		--//  ENCODE zstd
	,md_customer_segmentation VARCHAR(255)  		--//  ENCODE zstd
	,years_of_experience VARCHAR(255)  		--//  ENCODE zstd
	,md_innovation VARCHAR(255)  		--//  ENCODE zstd
	,md_number_of_procedures VARCHAR(255)  		--//  ENCODE zstd
	,md_types_of_procedure VARCHAR(255)  		--//  ENCODE zstd
	,md_total_hip_replacement NUMERIC(18,0)  		--//  ENCODE delta
	,md_total_knee_replacement NUMERIC(18,0)  		--//  ENCODE delta
	,md_spine NUMERIC(18,0)  		--//  ENCODE delta
	,md_trauma NUMERIC(18,0)  		--//  ENCODE delta
	,md_collorectal NUMERIC(18,0)  		--//  ENCODE delta
	,md_hepatobiliary NUMERIC(18,0)  		--//  ENCODE delta
	,md_cholecystectomy NUMERIC(18,0)  		--//  ENCODE delta
	,md_total_hysterectomy NUMERIC(18,0)  		--//  ENCODE delta
	,md_myomectomy NUMERIC(18,0)  		--//  ENCODE delta
	,md_c_section NUMERIC(18,0)  		--//  ENCODE delta
	,md_normal_delivery NUMERIC(18,0)  		--//  ENCODE delta
	,md_cabg NUMERIC(18,0)  		--//  ENCODE delta
	,md_valve_repair NUMERIC(18,0)  		--//  ENCODE delta
	,md_abdominal NUMERIC(18,0)  		--//  ENCODE delta
	,md_breast_reconstruction NUMERIC(18,0)  		--//  ENCODE delta
	,md_oral_cranial_maxilofacial NUMERIC(18,0)  		--//  ENCODE delta
	,primary_ta VARCHAR(255)  		--//  ENCODE zstd
	,secondary_ta VARCHAR(255)  		--//  ENCODE zstd
	,jj_external_id VARCHAR(90)  		--//  ENCODE zstd
	,specialty_2 VARCHAR(255)  		--//  ENCODE zstd
	,sub_specialty VARCHAR(255)  		--//  ENCODE zstd
	,sea_account_classification VARCHAR(255)  		--//  ENCODE zstd
	,jj_email_1 VARCHAR(80)  		--//  ENCODE zstd
	,jj_email_2 VARCHAR(80)  		--//  ENCODE zstd
)

;
--DROP TABLE HCPOSEWKS_INTEGRATION.wks_hcp_osea_address;
CREATE OR REPLACE TABLE HCPOSEWKS_INTEGRATION.WKS_HCP_OSEA_ADDRESS		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE HCPOSEWKS_INTEGRATION.wks_hcp_osea_address
(
	address_source_id VARCHAR(18)  		--//  ENCODE zstd
	,is_deleted VARCHAR(5)  		--//  ENCODE zstd
	,address_name VARCHAR(83)  		--//  ENCODE zstd
	,record_type_source_id VARCHAR(18)  		--//  ENCODE zstd
	,created_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,created_by_id VARCHAR(18)  		--//  ENCODE zstd
	,last_modified_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,last_modified_by_id VARCHAR(18)  		--//  ENCODE zstd
	,account_source_id VARCHAR(100)  		--//  ENCODE zstd
	,address_line2_name VARCHAR(300)  		--//  ENCODE zstd
	,city VARCHAR(40)  		--//  ENCODE zstd
	,external_id VARCHAR(120)  		--//  ENCODE zstd
	,phone VARCHAR(40)  		--//  ENCODE zstd
	,fax VARCHAR(40)  		--//  ENCODE zstd
	,map VARCHAR(1300)  		--//  ENCODE zstd
	,is_primary VARCHAR(5)  		--//  ENCODE zstd
	,appt_required VARCHAR(5)  		--//  ENCODE zstd
	,inactive VARCHAR(5)  		--//  ENCODE zstd
	,country_code VARCHAR(255)  		--//  ENCODE zstd
	,latitude NUMERIC(15,8)  		--//  ENCODE delta
	,zip VARCHAR(20)  		--//  ENCODE zstd
	,brick VARCHAR(250)  		--//  ENCODE zstd
	,state VARCHAR(255)  		--//  ENCODE zstd
	,longitude NUMERIC(15,8)  		--//  ENCODE delta
	,controlling_address VARCHAR(18)  		--//  ENCODE zstd
	,suburb_town VARCHAR(50)  		--//  ENCODE zstd
	,veeva_autogen_id VARCHAR(30)  		--//  ENCODE zstd
	,is_locked VARCHAR(5)  		--//  ENCODE zstd
)

;
--DROP TABLE HCPOSEWKS_INTEGRATION.wks_hcp_osea_call;
CREATE OR REPLACE TABLE HCPOSEWKS_INTEGRATION.WKS_HCP_OSEA_CALL		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE HCPOSEWKS_INTEGRATION.wks_hcp_osea_call
(
	call_source_id VARCHAR(18)  		--//  ENCODE zstd
	,owner_source_id VARCHAR(18)  		--//  ENCODE zstd
	,is_deleted VARCHAR(5)  		--//  ENCODE zstd
	,call_name VARCHAR(80)  		--//  ENCODE zstd
	,record_type_source_id VARCHAR(18)  		--//  ENCODE zstd
	,created_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,created_by_id VARCHAR(18)  		--//  ENCODE zstd
	,last_modified_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,last_modified_by_id VARCHAR(18)  		--//  ENCODE zstd
	,last_activity_date DATE  		--//  ENCODE delta
	,is_locked VARCHAR(5)  		--//  ENCODE zstd
	,call_comments VARCHAR(32000)  		--//  ENCODE zstd
	,sample_card VARCHAR(15)  		--//  ENCODE zstd
	,account_source_id VARCHAR(18)  		--//  ENCODE zstd
	,call_status_type VARCHAR(255)  		--//  ENCODE zstd
	,parent_address VARCHAR(18)  		--//  ENCODE zstd
	,account_plan VARCHAR(18)  		--//  ENCODE zstd
	,next_call_notes VARCHAR(32000)  		--//  ENCODE zstd
	,pre_call_notes VARCHAR(32000)  		--//  ENCODE zstd
	,mobile_id VARCHAR(100)  		--//  ENCODE zstd
	,significant_event VARCHAR(5)  		--//  ENCODE zstd
	,location VARCHAR(128)  		--//  ENCODE zstd
	,subject VARCHAR(128)  		--//  ENCODE zstd
	,call_date_time TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,disbursed_to VARCHAR(255)  		--//  ENCODE zstd
	,signature_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,signature VARCHAR(32000)  		--//  ENCODE zstd
	,territory VARCHAR(100)  		--//  ENCODE zstd
	,submitted_by_mobile VARCHAR(5)  		--//  ENCODE zstd
	,call_type VARCHAR(255)  		--//  ENCODE zstd
	,address VARCHAR(500)  		--//  ENCODE zstd
	,attendees NUMERIC(3,0)  		--//  ENCODE delta
	,attendee_type VARCHAR(255)  		--//  ENCODE zstd
	,call_date DATE  		--//  ENCODE delta
	,detailed_products VARCHAR(255)  		--//  ENCODE zstd
	,no_disbursement VARCHAR(5)  		--//  ENCODE zstd
	,parent_call_source_id VARCHAR(18)  		--//  ENCODE zstd
	,user_name VARCHAR(18)  		--//  ENCODE zstd
	,medical_event VARCHAR(18)  		--//  ENCODE zstd
	,mobile_created_date_time TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,mobile_last_modified_date_time TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,is_parent_call NUMERIC(18,0)  		--//  ENCODE delta
	,last_device VARCHAR(255)  		--//  ENCODE zstd
	,call_clm VARCHAR(5)  		--//  ENCODE zstd
	,is_sampled_call VARCHAR(5)  		--//  ENCODE zstd
	,presentations VARCHAR(500)  		--//  ENCODE zstd
	,duration NUMERIC(18,1)  		--//  ENCODE delta
	,allowed_products VARCHAR(1000)  		--//  ENCODE zstd
	,address_line_1 VARCHAR(80)  		--//  ENCODE zstd
	,address_line_2 VARCHAR(100)  		--//  ENCODE zstd
	,city VARCHAR(40)  		--//  ENCODE zstd
	,state VARCHAR(10)  		--//  ENCODE zstd
	,product_priority_1 VARCHAR(18)  		--//  ENCODE zstd
	,product_priority_2 VARCHAR(18)  		--//  ENCODE zstd
	,product_priority_3 VARCHAR(18)  		--//  ENCODE zstd
	,product_priority_4 VARCHAR(18)  		--//  ENCODE zstd
	,product_priority_5 VARCHAR(18)  		--//  ENCODE zstd
	,attendee_list VARCHAR(32768)  		--//  ENCODE zstd
	,signature_timestamp NUMERIC(15,1)  		--//  ENCODE delta
	,total_expense_attendees_count NUMERIC(15,1)  		--//  ENCODE delta
	,location_services_status VARCHAR(255)  		--//  ENCODE zstd
	,signature_location_latitude NUMERIC(15,6)  		--//  ENCODE delta
	,signature_location_longitude NUMERIC(15,6)  		--//  ENCODE delta
	,msl_interaction_notes VARCHAR(32768)  		--//  ENCODE zstd
	,sea_call_type VARCHAR(1300)  		--//  ENCODE zstd
	,signature_on_sync VARCHAR(1300)  		--//  ENCODE zstd
	,call_duration VARCHAR(255)  		--//  ENCODE zstd
	,interaction_mode VARCHAR(255)  		--//  ENCODE zstd
	,hcp_kol_initiated VARCHAR(5)  		--//  ENCODE zstd
	,msl_interaction_type VARCHAR(255)  		--//  ENCODE zstd
	,call_clm_location_latitude NUMERIC(15,6)  		--//  ENCODE delta
	,call_clm_location_longitude NUMERIC(15,6)  		--//  ENCODE delta
	,call_clm_location_services_status VARCHAR(255)  		--//  ENCODE zstd
	,parent_call_mobile_id VARCHAR(100)  		--//  ENCODE zstd
	,submit_location_latitude NUMERIC(15,6)  		--//  ENCODE delta
	,submit_location_longitude NUMERIC(15,6)  		--//  ENCODE delta
	,submit_location_services_status VARCHAR(255)  		--//  ENCODE zstd
	,submit_timestamp NUMERIC(15,1)  		--//  ENCODE delta
	,in_manager_insights VARCHAR(32768)  		--//  ENCODE zstd
	,md_hours VARCHAR(255)  		--//  ENCODE zstd
	,md_in_or_ot VARCHAR(5)  		--//  ENCODE zstd
	,md_d_call_type VARCHAR(255)  		--//  ENCODE zstd
	,md_minutes VARCHAR(255)  		--//  ENCODE zstd
	,em_event VARCHAR(18)  		--//  ENCODE zstd
	,medical_inquiry VARCHAR(18)  		--//  ENCODE zstd
	,parent_address_id VARCHAR(255)  		--//  ENCODE zstd
	,suggestion VARCHAR(18)  		--//  ENCODE zstd
	,call_objective VARCHAR(255)  		--//  ENCODE zstd
	,account_classification VARCHAR(255)  		--//  ENCODE zstd
	,submission_delay NUMERIC(18,0)  		--//  ENCODE delta
	,number_of_days_to_close_calls_kpi__c NUMERIC(18,2)  		--//  ENCODE delta
	,country VARCHAR(1300)  		--//  ENCODE zstd
	,region VARCHAR(1300)  		--//  ENCODE zstd
	,child_account_id VARCHAR(40)  		--//  ENCODE zstd
	,child_account VARCHAR(18)  		--//  ENCODE zstd
	,location_id VARCHAR(40)  		--//  ENCODE zstd
	,location_name VARCHAR(18)  		--//  ENCODE zstd
	,md_hsp_admin VARCHAR(255)  		--//  ENCODE zstd
	,hsp_minutes VARCHAR(255)  		--//  ENCODE zstd
	,ortho_on_call_case VARCHAR(5)  		--//  ENCODE zstd
	,ortho_volunteer_case VARCHAR(5)  		--//  ENCODE zstd
	,md_calc1 NUMERIC(18,2)  		--//  ENCODE delta
	,md_calculate_non_case_time NUMERIC(18,2)  		--//  ENCODE delta
	,md_calculated_hours_field NUMERIC(18,2)  		--//  ENCODE delta
	,md_casedeployment VARCHAR(255)  		--//  ENCODE zstd
	,md_case_coverage_12_hours VARCHAR(255)  		--//  ENCODE zstd
	,md_product_discussion VARCHAR(255)  		--//  ENCODE zstd
	,md_concurrent_call VARCHAR(5)  		--//  ENCODE zstd
	,courtesy_call VARCHAR(255)  		--//  ENCODE zstd
	,md_in_service VARCHAR(255)  		--//  ENCODE zstd
	,md_kol_course_discussion VARCHAR(255)  		--//  ENCODE zstd
	,kol_minutes VARCHAR(255)  		--//  ENCODE zstd
	,other_activities_time_12_hours NUMERIC(18,0)  		--//  ENCODE delta
	,other_in_field_activities VARCHAR(4099)  		--//  ENCODE zstd
	,md_overseas_workshop_visit VARCHAR(255)  		--//  ENCODE zstd
	,md_ra_activities2 VARCHAR(255)  		--//  ENCODE zstd
	,sales_activity VARCHAR(4099)  		--//  ENCODE zstd
	,sales_time_12_hours NUMERIC(18,0)  		--//  ENCODE delta
	,time_spent VARCHAR(255)  		--//  ENCODE zstd
	,time_spent_on_other_activities_simp VARCHAR(255)  		--//  ENCODE zstd
	,time_spent_on_sales_activity VARCHAR(255)  		--//  ENCODE zstd
	,md_time_spent_on_a_call NUMERIC(18,2)  		--//  ENCODE delta
	,md_case_type VARCHAR(255)  		--//  ENCODE zstd
	,md_sets_activities VARCHAR(255)  		--//  ENCODE zstd
	,md_time_spent_on_case VARCHAR(255)  		--//  ENCODE zstd
	,time_spent_on_other_activities NUMERIC(18,2)  		--//  ENCODE delta
	,time_spent_per_call NUMERIC(18,2)  		--//  ENCODE delta
	,md_case_conducted_in_hospital VARCHAR(255)  		--//  ENCODE zstd
	,calculated_field_2 NUMERIC(18,2)  		--//  ENCODE delta
	,calculated_hours_3 NUMERIC(18,2)  		--//  ENCODE delta
	,call_planned VARCHAR(5)  		--//  ENCODE zstd
	,call_submission_day NUMERIC(18,1)  		--//  ENCODE delta
	,check_in_latitude NUMERIC(15,6)  		--//  ENCODE delta
	,check_in_location_services_status VARCHAR(255)  		--//  ENCODE zstd
	,check_in_longitude NUMERIC(15,6)  		--//  ENCODE delta
	,check_in_status VARCHAR(255)  		--//  ENCODE zstd
	,check_in_timestamp NUMERIC(15,1)  		--//  ENCODE delta
	,medical_discussions VARCHAR(255)  		--//  ENCODE zstd
	,case_coverage NUMERIC(18,2)  		--//  ENCODE delta
	,call_duration_mins_in_number NUMERIC(18,2)  		--//  ENCODE delta
	,day_of_week VARCHAR(1300)  		--//  ENCODE zstd
	,veeva_remote_meeting_id VARCHAR(20)  		--//  ENCODE zstd
	,signature_page_display_name VARCHAR(255)  		--//  ENCODE zstd
	,disbursement_created VARCHAR(5)  		--//  ENCODE zstd
	,number_of_detailing NUMERIC(18,1)  		--//  ENCODE delta
	,joined_by_manager VARCHAR(5)  		--//  ENCODE zstd
	,pre_engagement_coaching VARCHAR(5)  		--//  ENCODE zstd
	,preparation_time NUMERIC(18,0)  		--//  ENCODE delta
	,travel_time NUMERIC(18,0)  		--//  ENCODE delta
	,rep_manager VARCHAR(1300)  		--//  ENCODE zstd
	,jj_therapeutic_area__c VARCHAR(255)  		--//  ENCODE zstd
	,numberof_key_message NUMERIC(18,1)  		--//  ENCODE delta
	,account_specialty VARCHAR(1300)  		--//  ENCODE zstd
	,owner_company_name VARCHAR(1300)  		--//  ENCODE zstd
	,call_submittedby VARCHAR(30)  		--//  ENCODE zstd
	,call_submitted_date_time TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,rep_division VARCHAR(1300)  		--//  ENCODE zstd
	,rep_department VARCHAR(1300)  		--//  ENCODE zstd
	,productivity_call VARCHAR(255)  		--//  ENCODE zstd
	,account_external_id VARCHAR(1300)  		--//  ENCODE zstd
	,call_objectives VARCHAR(4099)  		--//  ENCODE zstd
	,cost_of_procedures NUMERIC(18,1)  		--//  ENCODE delta
	,other_objective VARCHAR(255)  		--//  ENCODE zstd
	,hcp_speciality VARCHAR(1300)  		--//  ENCODE zstd
	,location_text VARCHAR(255)  		--//  ENCODE zstd
	,signed_by VARCHAR(255)  		--//  ENCODE zstd
	,any_aepqc_ss_i_have_reported_within_24h VARCHAR(255)  		--//  ENCODE zstd
	,end_datetime TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,remote_meeting_source_id VARCHAR(18)  		--//  ENCODE zstd
	,call_channel VARCHAR(255)  		--//  ENCODE zstd
	,fml_simp_case_product_discussion NUMERIC(18,2)  		--//  ENCODE delta
	,fml_simp_in_service NUMERIC(18,2)  		--//  ENCODE delta
	,fml_simp_overseas_workshop_visit NUMERIC(18,2)  		--//  ENCODE delta
	,signature_captured_remotely VARCHAR(5)  		--//  ENCODE zstd
	,virtual_channel_option VARCHAR(255)  		--//  ENCODE zstd
	,thmd_call_objectives VARCHAR(4099)  		--//  ENCODE zstd
	,thmd_case_type VARCHAR(255)  		--//  ENCODE zstd
	,phmd_call_objective VARCHAR(4099)  		--//  ENCODE zstd
	,idmd_call_objectives VARCHAR(4099)  		--//  ENCODE zstd
	,gsg_case_type VARCHAR(255)  		--//  ENCODE zstd
	,sis_case_type VARCHAR(255)  		--//  ENCODE zstd
)

;
--DROP TABLE HCPOSEWKS_INTEGRATION.wks_hcp_osea_call_detail;
CREATE OR REPLACE TABLE HCPOSEWKS_INTEGRATION.WKS_HCP_OSEA_CALL_DETAIL		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE HCPOSEWKS_INTEGRATION.wks_hcp_osea_call_detail
(
	call_detail_source_id VARCHAR(18)  		--//  ENCODE zstd
	,is_deleted VARCHAR(5)  		--//  ENCODE zstd
	,call_detail_name VARCHAR(80)  		--//  ENCODE zstd
	,created_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,created_by_id VARCHAR(18)  		--//  ENCODE zstd
	,last_modified_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,last_modified_by_id VARCHAR(18)  		--//  ENCODE zstd
	,is_locked VARCHAR(5)  		--//  ENCODE zstd
	,is_parent_call NUMERIC(18,0)  		--//  ENCODE delta
	,call_source_id VARCHAR(18)  		--//  ENCODE zstd
	,product_source_id VARCHAR(18)  		--//  ENCODE zstd
	,call_detail_priority NUMERIC(2,0)  		--//  ENCODE delta
	,detail_call_type VARCHAR(255)  		--//  ENCODE zstd
	,call_detail_group VARCHAR(18)  		--//  ENCODE zstd
	,product_id18 VARCHAR(1300)  		--//  ENCODE zstd
	,classification VARCHAR(40)  		--//  ENCODE zstd
	,my_adoption_ladder VARCHAR(255)  		--//  ENCODE zstd
	,simp_adoption_style VARCHAR(255)  		--//  ENCODE zstd
	,simp_behavioral_style VARCHAR(255)  		--//  ENCODE zstd
	,simp_market_share NUMERIC(18,1)  		--//  ENCODE delta
	,number_of_patients_per_quarter NUMERIC(18,1)  		--//  ENCODE delta
	,number_of_patients_per_month NUMERIC(18,1)  		--//  ENCODE delta
	,number_of_patients_per_week NUMERIC(18,1)  		--//  ENCODE delta
	,tw_adoption_ladder VARCHAR(255)  		--//  ENCODE zstd
	,tw_adoption_style VARCHAR(255)  		--//  ENCODE zstd
	,tw_behavioral_style VARCHAR(255)  		--//  ENCODE zstd
	,tw_market_share NUMERIC(18,1)  		--//  ENCODE delta
)

;
--DROP TABLE HCPOSEWKS_INTEGRATION.wks_hcp_osea_call_discussion;
CREATE OR REPLACE TABLE HCPOSEWKS_INTEGRATION.WKS_HCP_OSEA_CALL_DISCUSSION		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE HCPOSEWKS_INTEGRATION.wks_hcp_osea_call_discussion
(
	call_discussion_source_id VARCHAR(18)  		--//  ENCODE zstd
	,is_deleted VARCHAR(5)  		--//  ENCODE zstd
	,call_discussion_name VARCHAR(80)  		--//  ENCODE zstd
	,record_type_source_id VARCHAR(18)  		--//  ENCODE zstd
	,created_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,created_by_id VARCHAR(18)  		--//  ENCODE zstd
	,last_modified_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,last_modified_by_id VARCHAR(18)  		--//  ENCODE zstd
	,account_source_id VARCHAR(18)  		--//  ENCODE zstd
	,call_source_id VARCHAR(18)  		--//  ENCODE zstd
	,comments VARCHAR(800)  		--//  ENCODE zstd
	,product_source_id VARCHAR(18)  		--//  ENCODE zstd
	,discussion_topics VARCHAR(255)  		--//  ENCODE zstd
	,medical_event VARCHAR(18)  		--//  ENCODE zstd
	,is_parent_call NUMERIC(18,0)  		--//  ENCODE zstd
	,discussion_type VARCHAR(255)  		--//  ENCODE zstd
	,call_discussion_type VARCHAR(255)  		--//  ENCODE zstd
	,effectiveness VARCHAR(255)  		--//  ENCODE zstd
	,follow_up_activity VARCHAR(255)  		--//  ENCODE zstd
	,outcomes VARCHAR(255)  		--//  ENCODE zstd
	,follow_up_additional_info VARCHAR(800)  		--//  ENCODE zstd
	,follow_up_date DATE  		--//  ENCODE zstd
	,materials_used VARCHAR(255)  		--//  ENCODE zstd
	,call_date DATE  		--//  ENCODE delta
	,detail_group_source_id VARCHAR(18)  		--//  ENCODE zstd
	,user_source_id VARCHAR(18)  		--//  ENCODE zstd
)

;
--DROP TABLE HCPOSEWKS_INTEGRATION.wks_hcp_osea_call_key_message;
CREATE OR REPLACE TABLE HCPOSEWKS_INTEGRATION.WKS_HCP_OSEA_CALL_KEY_MESSAGE		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE HCPOSEWKS_INTEGRATION.wks_hcp_osea_call_key_message
(
	call_key_message_id VARCHAR(18)  		--//  ENCODE zstd
	,call_key_message_isdeleted VARCHAR(5)  		--//  ENCODE zstd
	,call_key_message_name VARCHAR(300)  		--//  ENCODE zstd
	,call_key_message_createddate TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,call_key_message_createdbyid VARCHAR(18)  		--//  ENCODE zstd
	,last_modified_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,call_key_message_lastmodifiedbyid VARCHAR(18)  		--//  ENCODE zstd
	,call_key_message_systemmodstamp TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,call_key_message_account VARCHAR(18)  		--//  ENCODE zstd
	,call_key_message_call2 VARCHAR(18)  		--//  ENCODE zstd
	,call_key_message_reaction VARCHAR(800)  		--//  ENCODE zstd
	,call_key_message_product VARCHAR(18)  		--//  ENCODE zstd
	,call_key_message_key_message VARCHAR(18)  		--//  ENCODE zstd
	,call_key_message_contact VARCHAR(18)  		--//  ENCODE zstd
	,call_key_message_call_date DATE  		--//  ENCODE delta
	,call_key_message_user VARCHAR(18)  		--//  ENCODE zstd
	,call_key_message_category VARCHAR(800)  		--//  ENCODE zstd
	,call_key_message_vehicle VARCHAR(800)  		--//  ENCODE zstd
	,call_key_message_is_parent_call NUMERIC(18,0)  		--//  ENCODE delta
	,call_key_message_clm_id VARCHAR(500)  		--//  ENCODE zstd
	,call_key_message_slide_version VARCHAR(300)  		--//  ENCODE zstd
	,call_key_message_duration NUMERIC(18,2)  		--//  ENCODE delta
	,call_key_message_presentation_id VARCHAR(300)  		--//  ENCODE zstd
	,call_key_message_start_time TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,call_key_message_attendee_type VARCHAR(800)  		--//  ENCODE zstd
	,call_key_message_entity_reference_id VARCHAR(300)  		--//  ENCODE zstd
	,call_key_message_segment VARCHAR(300)  		--//  ENCODE zstd
	,call_key_message_display_order NUMERIC(18,0)  		--//  ENCODE delta
	,call_key_message_detail_group VARCHAR(18)  		--//  ENCODE zstd
	,call_key_message_clm_presentation_name VARCHAR(300)  		--//  ENCODE zstd
	,call_key_message_clm_presentation_version VARCHAR(100)  		--//  ENCODE zstd
	,call_key_message_clm_presentation VARCHAR(18)  		--//  ENCODE zstd
	,key_message_name VARCHAR(80)  		--//  ENCODE zstd
	,key_message_description VARCHAR(1300)  		--//  ENCODE zstd
)

;
--DROP TABLE HCPOSEWKS_INTEGRATION.wks_hcp_osea_coaching_report;
CREATE OR REPLACE TABLE HCPOSEWKS_INTEGRATION.WKS_HCP_OSEA_COACHING_REPORT		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE HCPOSEWKS_INTEGRATION.wks_hcp_osea_coaching_report
(
	id VARCHAR(18)  		--//  ENCODE zstd
	,ownerid VARCHAR(18)  		--//  ENCODE zstd
	,isdeleted VARCHAR(5)  		--//  ENCODE zstd
	,name VARCHAR(80)  		--//  ENCODE zstd
	,recordtypeid VARCHAR(18)  		--//  ENCODE zstd
	,createddate TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,createdbyid VARCHAR(18)  		--//  ENCODE zstd
	,lastmodifieddate TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,lastmodifiedbyid VARCHAR(18)  		--//  ENCODE zstd
	,systemmodstamp TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,lastactivitydate TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,mayedit VARCHAR(20)  		--//  ENCODE zstd
	,islocked VARCHAR(5)  		--//  ENCODE zstd
	,lastvieweddate TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,lastreferenceddate TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,mobile_id_vod__c VARCHAR(1000)  		--//  ENCODE zstd
	,manager_vod__c VARCHAR(18)  		--//  ENCODE zstd
	,employee_vod__c VARCHAR(18)  		--//  ENCODE zstd
	,review_date__c DATE  		--//  ENCODE delta
	,review_period__c VARCHAR(20)  		--//  ENCODE zstd
	,status__c VARCHAR(255)  		--//  ENCODE zstd
	,overall_rating__c VARCHAR(100)  		--//  ENCODE zstd
	,jj_core_country_code__c VARCHAR(10)  		--//  ENCODE zstd
	,jj_core_lock__c VARCHAR(20)  		--//  ENCODE zstd
	,jj_core_no_of_visits__c VARCHAR(10)  		--//  ENCODE zstd
	,jj_employee_review_and_ackCURRENT_TIMESTAMP()ledged__c VARCHAR(5)  		--// 	,jj_employee_review_and_acknowledged__c VARCHAR(5)   ENCODE zstd //  ENCODE zstd
	,jj_employee_comments__c VARCHAR(2000)  		--//  ENCODE zstd
	,jj_simp_manager_comments_long__c VARCHAR(65535)  		--//  ENCODE zstd
	,jj_simp_objectives__c VARCHAR(65535)  		--//  ENCODE zstd
	,jj_simp_rep_comments_long__c VARCHAR(65535)  		--//  ENCODE zstd
	,jj_simp_sg_overall_rating__c NUMERIC(18,1)  		--//  ENCODE delta
	,jj_simp_long_comments__c VARCHAR(5000)  		--//  ENCODE zstd
	,kCURRENT_TIMESTAMP()ledge_strategy_overall_rating__c NUMERIC(18,1)  		--// 	,knowledge_strategy_overall_rating__c NUMERIC(18,1)   ENCODE delta //  ENCODE delta
	,selling_skills_overall_rating__c NUMERIC(18,1)  		--//  ENCODE delta
	,jj_my_call_type__c VARCHAR(255)  		--//  ENCODE zstd
	,jj_my_location__c VARCHAR(255)  		--//  ENCODE zstd
	,jj_id_overall_rating__c NUMERIC(18,1)  		--//  ENCODE delta
	,jj_vn_md_overall_rating_med__c NUMERIC(18,1)  		--//  ENCODE delta
	,jj_agreed_next_steps__c VARCHAR(2000)  		--//  ENCODE zstd
	,jj_coaching_for_field_visits__c VARCHAR(5)  		--//  ENCODE zstd
	,jj_customer_interactions__c VARCHAR(2000)  		--//  ENCODE zstd
	,jj_date_of_review_concluded__c DATE  		--//  ENCODE delta
	,jj_general_observations_and_comments__c VARCHAR(2000)  		--//  ENCODE zstd
	,jj_manager_feedback_completed__c VARCHAR(5)  		--//  ENCODE zstd
	,jj_number_of_coaching_days__c VARCHAR(255)  		--//  ENCODE zstd
	,jj_second_line_manager__c VARCHAR(18)  		--//  ENCODE zstd
	,jj_submission_to_date__c NUMERIC(18,1)  		--//  ENCODE lzo
	,relatedcoachingreport__c VARCHAR(255)  		--//  ENCODE zstd
)

;
--DROP TABLE HCPOSEWKS_INTEGRATION.wks_hcp_osea_cycle_plan;
CREATE OR REPLACE TABLE HCPOSEWKS_INTEGRATION.WKS_HCP_OSEA_CYCLE_PLAN		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE HCPOSEWKS_INTEGRATION.wks_hcp_osea_cycle_plan
(
	cycle_plan_source_id VARCHAR(18)  		--//  ENCODE zstd
	,owner_source_id VARCHAR(18)  		--//  ENCODE zstd
	,is_deleted VARCHAR(5)  		--//  ENCODE zstd
	,cycle_plan_name VARCHAR(80)  		--//  ENCODE zstd
	,created_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,created_by_id VARCHAR(18)  		--//  ENCODE zstd
	,last_modified_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,last_modified_by_id VARCHAR(18)  		--//  ENCODE zstd
	,is_locked VARCHAR(5)  		--//  ENCODE zstd
	,is_active VARCHAR(5)  		--//  ENCODE zstd
	,attainment NUMERIC(18,0)  		--//  ENCODE delta
	,end_date DATE  		--//  ENCODE delta
	,external_id VARCHAR(100)  		--//  ENCODE zstd
	,start_date DATE  		--//  ENCODE delta
	,territory_name VARCHAR(100)  		--//  ENCODE zstd
	,actual_calls NUMERIC(6,0)  		--//  ENCODE delta
	,planned_calls NUMERIC(3,0)  		--//  ENCODE delta
	,expected_attainment NUMERIC(18,1)  		--//  ENCODE delta
	,expected_calls NUMERIC(18,1)  		--//  ENCODE delta
	,attainment_difference NUMERIC(18,1)  		--//  ENCODE delta
	,remaining NUMERIC(18,1)  		--//  ENCODE delta
	,status_type VARCHAR(255)  		--//  ENCODE zstd
	,mgr_s_email VARCHAR(80)  		--//  ENCODE zstd
	,manager VARCHAR(18)  		--//  ENCODE zstd
	,ready_for_approval_flag VARCHAR(35)  		--//  ENCODE zstd
	,close_out VARCHAR(5)  		--//  ENCODE zstd
	,manager_name VARCHAR(50)  		--//  ENCODE zstd
	,country_code VARCHAR(2)  		--//  ENCODE zstd
	,cp_approval_time TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,approver_name VARCHAR(50)  		--//  ENCODE zstd
	,number_of_targets NUMERIC(18,0)  		--//  ENCODE delta
	,number_of_cfa_100_targets NUMERIC(18,0)  		--//  ENCODE delta
	,cycle_plan_attainment_cptarget NUMERIC(18,1)  		--//  ENCODE delta
	,mid_date DATE  		--//  ENCODE delta
	,hcp_product_achieved_100 NUMERIC(18,0)  		--//  ENCODE delta
	,hcp_products_planned NUMERIC(18,0)  		--//  ENCODE delta
	,cpa_100 NUMERIC(18,1)  		--//  ENCODE delta
	,total_target_reached NUMERIC(18,1)  		--//  ENCODE delta
	,submitted_by VARCHAR(100)  		--//  ENCODE zstd
	,submitted_date_time TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
)

;
--DROP TABLE HCPOSEWKS_INTEGRATION.wks_hcp_osea_cycle_plan_detail;
CREATE OR REPLACE TABLE HCPOSEWKS_INTEGRATION.WKS_HCP_OSEA_CYCLE_PLAN_DETAIL		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE HCPOSEWKS_INTEGRATION.wks_hcp_osea_cycle_plan_detail
(
	cycle_plan_detail_source_id VARCHAR(18)  		--//  ENCODE zstd
	,is_deleted VARCHAR(5)  		--//  ENCODE zstd
	,cycle_plan_detail_name VARCHAR(80)  		--//  ENCODE zstd
	,created_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,created_by_id VARCHAR(18)  		--//  ENCODE zstd
	,last_modified_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,last_modified_by_id VARCHAR(18)  		--//  ENCODE zstd
	,is_locked VARCHAR(5)  		--//  ENCODE zstd
	,cycle_plan_target_source_id VARCHAR(18)  		--//  ENCODE zstd
	,actual_details NUMERIC(3,0)  		--//  ENCODE delta
	,attainment NUMERIC(18,0)  		--//  ENCODE delta
	,planned_details NUMERIC(3,0)  		--//  ENCODE delta
	,product_source_id VARCHAR(18)  		--//  ENCODE zstd
	,scheduled_details NUMERIC(3,0)  		--//  ENCODE delta
	,total_actual_details NUMERIC(5,0)  		--//  ENCODE delta
	,total_attainment NUMERIC(18,0)  		--//  ENCODE delta
	,total_planned_details NUMERIC(5,0)  		--//  ENCODE delta
	,total_scheduled_details NUMERIC(5,0)  		--//  ENCODE delta
	,remaining NUMERIC(18,0)  		--//  ENCODE delta
	,total_remaining NUMERIC(18,0)  		--//  ENCODE delta
	,classification_type VARCHAR(255)  		--//  ENCODE zstd
	,cfa_100 NUMERIC(18,0)  		--//  ENCODE delta
	,cfa_33 NUMERIC(18,0)  		--//  ENCODE delta
	,cfa_66 NUMERIC(18,0)  		--//  ENCODE delta
	,adoption_style VARCHAR(255)  		--//  ENCODE zstd
)

;
--DROP TABLE HCPOSEWKS_INTEGRATION.wks_hcp_osea_cycle_plan_target;
CREATE OR REPLACE TABLE HCPOSEWKS_INTEGRATION.WKS_HCP_OSEA_CYCLE_PLAN_TARGET		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE HCPOSEWKS_INTEGRATION.wks_hcp_osea_cycle_plan_target
(
	cycle_plan_target_source_id VARCHAR(18)  		--//  ENCODE zstd
	,is_deleted VARCHAR(5)  		--//  ENCODE zstd
	,cycle_plan_target_name VARCHAR(80)  		--//  ENCODE zstd
	,created_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,created_by_id VARCHAR(18)  		--//  ENCODE zstd
	,last_modified_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,last_modified_by_id VARCHAR(18)  		--//  ENCODE zstd
	,is_locked VARCHAR(5)  		--//  ENCODE zstd
	,cycle_plan_vod_source_id VARCHAR(18)  		--//  ENCODE zstd
	,actual_calls NUMERIC(3,0)  		--//  ENCODE delta
	,attainment NUMERIC(18,0)  		--//  ENCODE delta
	,cycle_plan_account_source_id VARCHAR(18)  		--//  ENCODE zstd
	,original_planned_calls NUMERIC(3,0)  		--//  ENCODE delta
	,planned_calls NUMERIC(3,0)  		--//  ENCODE delta
	,total_actual_calls NUMERIC(5,0)  		--//  ENCODE delta
	,total_attainment NUMERIC(18,0)  		--//  ENCODE delta
	,total_planned_calls NUMERIC(5,0)  		--//  ENCODE delta
	,external_id VARCHAR(100)  		--//  ENCODE zstd
	,scheduled_calls NUMERIC(18,0)  		--//  ENCODE delta
	,total_scheduled_calls NUMERIC(5,0)  		--//  ENCODE delta
	,remaining NUMERIC(18,0)  		--//  ENCODE delta
	,total_remaining NUMERIC(18,0)  		--//  ENCODE delta
	,remaining_schedule NUMERIC(18,0)  		--//  ENCODE delta
	,total_remaining_schedule NUMERIC(18,0)  		--//  ENCODE delta
	,primary_parent_name VARCHAR(1300)  		--//  ENCODE zstd
	,specialty_1 VARCHAR(1300)  		--//  ENCODE zstd
	,account_source_id VARCHAR(1300)  		--//  ENCODE zstd
	,cpt_cfa_100 NUMERIC(18,0)  		--//  ENCODE delta
	,cpt_cfa_66 NUMERIC(18,0)  		--//  ENCODE delta
	,cpt_cfa_33 NUMERIC(18,0)  		--//  ENCODE delta
	,number_of_cfa_100_details NUMERIC(18,0)  		--//  ENCODE delta
	,number_of_product_details NUMERIC(18,0)  		--//  ENCODE delta
	,target_reached_flag NUMERIC(18,0)  		--//  ENCODE delta
	,jj_ac_classification__c VARCHAR(3900)  		--//  ENCODE zstd
)

;
--DROP TABLE HCPOSEWKS_INTEGRATION.wks_hcp_osea_holiday_list;
CREATE OR REPLACE TABLE HCPOSEWKS_INTEGRATION.WKS_HCP_OSEA_HOLIDAY_LIST		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE HCPOSEWKS_INTEGRATION.wks_hcp_osea_holiday_list
(
	country VARCHAR(5)  		--//  ENCODE lzo
	,holiday_key VARCHAR(20)  		--//  ENCODE lzo
)

;
--DROP TABLE HCPOSEWKS_INTEGRATION.wks_hcp_osea_isight_licenses;
CREATE OR REPLACE TABLE HCPOSEWKS_INTEGRATION.WKS_HCP_OSEA_ISIGHT_LICENSES		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE HCPOSEWKS_INTEGRATION.wks_hcp_osea_isight_licenses
(
	year NUMERIC(18,0)  		--//  ENCODE delta
	,country VARCHAR(255)  		--//  ENCODE zstd
	,sector VARCHAR(255)  		--//  ENCODE zstd
	,qty NUMERIC(18,0)  		--//  ENCODE delta
	,licensetype VARCHAR(255)  		--//  ENCODE zstd
)
		--// DISTSTYLE EVEN
;
--DROP TABLE HCPOSEWKS_INTEGRATION.wks_hcp_osea_isight_sector_mapping;
CREATE OR REPLACE TABLE HCPOSEWKS_INTEGRATION.WKS_HCP_OSEA_ISIGHT_SECTOR_MAPPING		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE HCPOSEWKS_INTEGRATION.wks_hcp_osea_isight_sector_mapping
(
	country VARCHAR(256)  		--//  ENCODE zstd
	,company VARCHAR(256)  		--//  ENCODE zstd
	,division VARCHAR(256)  		--//  ENCODE zstd
	,sector VARCHAR(256)  		--//  ENCODE zstd
)
		--// DISTSTYLE EVEN
;
--DROP TABLE HCPOSEWKS_INTEGRATION.wks_hcp_osea_key_message;
CREATE OR REPLACE TABLE HCPOSEWKS_INTEGRATION.WKS_HCP_OSEA_KEY_MESSAGE		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE HCPOSEWKS_INTEGRATION.wks_hcp_osea_key_message
(
	key_message_id VARCHAR(18)  		--//  ENCODE zstd
	,key_message_ownerid VARCHAR(18)  		--//  ENCODE zstd
	,key_message_name VARCHAR(250)  		--//  ENCODE zstd
	,key_message_recordtypeid VARCHAR(18)  		--//  ENCODE zstd
	,key_message_createddate TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,key_message_createdbyid VARCHAR(18)  		--//  ENCODE zstd
	,last_modified_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,key_message_lastmodifiedbyid VARCHAR(18)  		--//  ENCODE zstd
	,key_message_systemmodstamp TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,key_message_lastvieweddate TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,key_message_lastreferenceddate TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,key_message_description VARCHAR(800)  		--//  ENCODE zstd
	,key_message_product VARCHAR(18)  		--//  ENCODE zstd
	,key_message_product_strategy VARCHAR(18)  		--//  ENCODE zstd
	,key_message_display_order NUMERIC(3,0)  		--//  ENCODE delta
	,key_message_active VARCHAR(5)  		--//  ENCODE zstd
	,key_message_category VARCHAR(255)  		--//  ENCODE zstd
	,key_message_vehicle VARCHAR(255)  		--//  ENCODE zstd
	,key_message_clm_id VARCHAR(100)  		--//  ENCODE zstd
	,key_message_custom_reaction VARCHAR(255)  		--//  ENCODE zstd
	,key_message_slide_version VARCHAR(100)  		--//  ENCODE zstd
	,key_message_language VARCHAR(255)  		--//  ENCODE zstd
	,key_message_media_file_crc VARCHAR(255)  		--//  ENCODE zstd
	,key_message_media_file_name VARCHAR(255)  		--//  ENCODE zstd
	,key_message_media_file_size NUMERIC(18,0)  		--//  ENCODE delta
	,key_message_segment VARCHAR(80)  		--//  ENCODE zstd
	,key_message_detail_group VARCHAR(18)  		--//  ENCODE zstd
	,key_message_core_content_approval_id VARCHAR(100)  		--//  ENCODE zstd
	,key_message_core_content_expiration_date DATE  		--//  ENCODE delta
	,simp_country VARCHAR(1300)  		--//  ENCODE zstd
	,key_message_vexternal_id VARCHAR(255)  		--//  ENCODE zstd
	,key_message_cdn_path VARCHAR(255)  		--//  ENCODE zstd
	,key_message_status VARCHAR(255)  		--//  ENCODE zstd
	,ap_clm_country VARCHAR(4)  		--//  ENCODE zstd
	,key_message_is_shared_resource VARCHAR(5)  		--//  ENCODE zstd
	,key_message_shared_resource VARCHAR(18)  		--//  ENCODE zstd
	,ap_country VARCHAR(4)  		--//  ENCODE zstd
	,functional_team VARCHAR(255)  		--//  ENCODE zstd
	,janssen_code VARCHAR(150)  		--//  ENCODE zstd
	,vault_document_id VARCHAR(100)  		--//  ENCODE zstd
	,key_message_group VARCHAR(4099)  		--//  ENCODE zstd
	,key_message_sub_group VARCHAR(4099)  		--//  ENCODE zstd
	,key_message_purpose VARCHAR(255)  		--//  ENCODE zstd
	,key_message_content_topic VARCHAR(255)  		--//  ENCODE zstd
	,key_message_content_sub_topic VARCHAR(255)  		--//  ENCODE zstd
)

;
--DROP TABLE HCPOSEWKS_INTEGRATION.wks_hcp_osea_product;
CREATE OR REPLACE TABLE HCPOSEWKS_INTEGRATION.WKS_HCP_OSEA_PRODUCT		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE HCPOSEWKS_INTEGRATION.wks_hcp_osea_product
(
	product_source_id VARCHAR(18)  		--//  ENCODE zstd
	,owner_source_id VARCHAR(18)  		--//  ENCODE zstd
	,is_deleted VARCHAR(5)  		--//  ENCODE zstd
	,product_name VARCHAR(80)  		--//  ENCODE zstd
	,record_type_source_id VARCHAR(18)  		--//  ENCODE zstd
	,created_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,created_by_id VARCHAR(18)  		--//  ENCODE zstd
	,last_modified_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,last_modified_by_id VARCHAR(18)  		--//  ENCODE zstd
	,consumer_site VARCHAR(255)  		--//  ENCODE zstd
	,product_info VARCHAR(255)  		--//  ENCODE zstd
	,therapeutic_class VARCHAR(255)  		--//  ENCODE zstd
	,parent_product_source_id VARCHAR(18)  		--//  ENCODE zstd
	,therapeutic_area VARCHAR(255)  		--//  ENCODE zstd
	,product_type VARCHAR(255)  		--//  ENCODE zstd
	,require_key_message VARCHAR(5)  		--//  ENCODE zstd
	,cost NUMERIC(14,2)  		--//  ENCODE delta
	,external_id VARCHAR(25)  		--//  ENCODE zstd
	,manufacturer VARCHAR(255)  		--//  ENCODE zstd
	,company_product VARCHAR(5)  		--//  ENCODE zstd
	,controlled_substance VARCHAR(5)  		--//  ENCODE zstd
	,description VARCHAR(255)  		--//  ENCODE zstd
	,sample_quantity_picklist VARCHAR(1000)  		--//  ENCODE zstd
	,display_order NUMERIC(5,0)  		--//  ENCODE delta
	,no_metrics VARCHAR(5)  		--//  ENCODE zstd
	,distributor VARCHAR(255)  		--//  ENCODE zstd
	,sample_quantity_bound VARCHAR(5)  		--//  ENCODE zstd
	,sample_u_m VARCHAR(255)  		--//  ENCODE zstd
	,no_details VARCHAR(5)  		--//  ENCODE zstd
	,quantity_per_case NUMERIC(10,1)  		--//  ENCODE delta
	,restricted VARCHAR(5)  		--//  ENCODE zstd
	,user_aligned VARCHAR(5)  		--//  ENCODE zstd
	,restricted_states VARCHAR(100)  		--//  ENCODE zstd
	,sort_code VARCHAR(20)  		--//  ENCODE zstd
	,no_cycle_plans VARCHAR(5)  		--//  ENCODE zstd
	,sku_id VARCHAR(25)  		--//  ENCODE zstd
	,business_unit VARCHAR(1000)  		--//  ENCODE zstd
	,franchise VARCHAR(1000)  		--//  ENCODE zstd
	,country VARCHAR(255)  		--//  ENCODE zstd
	,vexternal_id VARCHAR(120)  		--//  ENCODE zstd
	,product_identifier VARCHAR(80)  		--//  ENCODE zstd
	,biz_sub_unit VARCHAR(255)  		--//  ENCODE zstd
	,biz_unit VARCHAR(255)  		--//  ENCODE zstd
	,product_sector VARCHAR(1300)  		--//  ENCODE zstd
	,imr VARCHAR(5)  		--//  ENCODE zstd
	,detail_sub_type VARCHAR(255)  		--//  ENCODE zstd
	,shc_sector VARCHAR(255)  		--//  ENCODE zstd
	,shc_strategic_group VARCHAR(255)  		--//  ENCODE zstd
	,shc_franchise VARCHAR(255)  		--//  ENCODE zstd
	,shc_brand VARCHAR(255)  		--//  ENCODE zstd
)

;
--DROP TABLE HCPOSEWKS_INTEGRATION.wks_hcp_osea_product_metrics;
CREATE OR REPLACE TABLE HCPOSEWKS_INTEGRATION.WKS_HCP_OSEA_PRODUCT_METRICS		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE HCPOSEWKS_INTEGRATION.wks_hcp_osea_product_metrics
(
	product_metrics_source_id VARCHAR(18)  		--//  ENCODE zstd
	,is_deleted VARCHAR(5)  		--//  ENCODE zstd
	,pm_name VARCHAR(80)  		--//  ENCODE zstd
	,created_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,created_by_id VARCHAR(18)  		--//  ENCODE zstd
	,last_modified_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,last_modified_by_id VARCHAR(18)  		--//  ENCODE zstd
	,account_source_id VARCHAR(18)  		--//  ENCODE zstd
	,awareness VARCHAR(255)  		--//  ENCODE zstd
	,movement NUMERIC(5,2)  		--//  ENCODE delta
	,product_source_id VARCHAR(18)  		--//  ENCODE zstd
	,segment VARCHAR(255)  		--//  ENCODE zstd
	,x12_mo_trx_chg NUMERIC(5,2)  		--//  ENCODE delta
	,speaker_skills VARCHAR(255)  		--//  ENCODE zstd
	,investigator_readiness VARCHAR(255)  		--//  ENCODE zstd
	,engagements NUMERIC(4,0)  		--//  ENCODE delta
	,external_id VARCHAR(255)  		--//  ENCODE zstd
	,decile NUMERIC(18,0)  		--//  ENCODE delta
	,adoption_level VARCHAR(255)  		--//  ENCODE zstd
	,detail_group VARCHAR(18)  		--//  ENCODE zstd
	,classification_type VARCHAR(255)  		--//  ENCODE zstd
	,company NUMERIC(18,0)  		--//  ENCODE delta
	,believer_of_adherence VARCHAR(255)  		--//  ENCODE zstd
	,influence_level VARCHAR(255)  		--//  ENCODE zstd
	,intention_for_future_sust VARCHAR(255)  		--//  ENCODE zstd
	,likely_to_ini_sustenna VARCHAR(255)  		--//  ENCODE zstd
	,likely_to_switch VARCHAR(255)  		--//  ENCODE zstd
	,penetration NUMERIC(3,0)  		--//  ENCODE delta
	,potential NUMERIC(3,0)  		--//  ENCODE delta
	,prescriber VARCHAR(255)  		--//  ENCODE zstd
	,number_of_patients_month NUMERIC(3,0)  		--//  ENCODE delta
	,schizophrenia_pts NUMERIC(6,0)  		--//  ENCODE delta
	,scientific_data VARCHAR(255)  		--//  ENCODE zstd
	,type_of_setting VARCHAR(255)  		--//  ENCODE zstd
	,usagof_sustenna_upon_discharging VARCHAR(255)  		--//  ENCODE zstd
	,usage_of_branded_atyp VARCHAR(255)  		--//  ENCODE zstd
	,kol_experts VARCHAR(5)  		--//  ENCODE zstd
	,nbrofmmptsyr NUMERIC(10,0)  		--//  ENCODE delta
	,practice_type VARCHAR(255)  		--//  ENCODE zstd
	,stdguideline VARCHAR(255)  		--//  ENCODE zstd
	,perception VARCHAR(255)  		--//  ENCODE zstd
	,prescription_behavior VARCHAR(255)  		--//  ENCODE zstd
	,scientifically_driven VARCHAR(255)  		--//  ENCODE zstd
	,treatment_pattern VARCHAR(255)  		--//  ENCODE zstd
	,physician_behaviour VARCHAR(255)  		--//  ENCODE zstd
	,product_preference VARCHAR(255)  		--//  ENCODE zstd
	,specialty VARCHAR(255)  		--//  ENCODE zstd
	,company_loyalty VARCHAR(255)  		--//  ENCODE zstd
	,biologics_user VARCHAR(255)  		--//  ENCODE zstd
	,price_sensitivity VARCHAR(255)  		--//  ENCODE zstd
	,usage_of_generic VARCHAR(255)  		--//  ENCODE zstd
	,previous_tramadol_experience VARCHAR(255)  		--//  ENCODE zstd
	,interest_to_treat_disease_area VARCHAR(255)  		--//  ENCODE zstd
	,satisfaction_with_alternative_tr VARCHAR(255)  		--//  ENCODE zstd
	,patient_share VARCHAR(255)  		--//  ENCODE zstd
	,country_code VARCHAR(1300)  		--//  ENCODE zstd
	,physician_product_preference VARCHAR(255)  		--//  ENCODE zstd
	,physician_prescription VARCHAR(255)  		--//  ENCODE zstd
	,adoption_ladder VARCHAR(255)  		--//  ENCODE zstd
	,ratings_points VARCHAR(255)  		--//  ENCODE zstd
	,peer_influence VARCHAR(255)  		--//  ENCODE zstd
	,innovations VARCHAR(255)  		--//  ENCODE zstd
	,cases_loads_per_year VARCHAR(255)  		--//  ENCODE zstd
	,sales_value_per_year VARCHAR(255)  		--//  ENCODE zstd
	,support VARCHAR(255)  		--//  ENCODE zstd
	,cmd VARCHAR(255)  		--//  ENCODE zstd
	,md_asp_classification VARCHAR(255)  		--//  ENCODE zstd
	,no_of_products_used VARCHAR(255)  		--//  ENCODE zstd
	,orientation_field VARCHAR(255)  		--//  ENCODE zstd
	,uptravi_usage VARCHAR(255)  		--//  ENCODE zstd
)

;
--DROP TABLE HCPOSEWKS_INTEGRATION.wks_hcp_osea_profile;
CREATE OR REPLACE TABLE HCPOSEWKS_INTEGRATION.WKS_HCP_OSEA_PROFILE		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE HCPOSEWKS_INTEGRATION.wks_hcp_osea_profile
(
	profile_source_id VARCHAR(18)  		--//  ENCODE zstd
	,profile_name VARCHAR(255)  		--//  ENCODE zstd
	,type VARCHAR(40)  		--//  ENCODE zstd
	,userlicense_source_id VARCHAR(18)  		--//  ENCODE zstd
	,usertype VARCHAR(40)  		--//  ENCODE zstd
	,created_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,created_by_id VARCHAR(18)  		--//  ENCODE zstd
	,last_modified_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,last_modified_by_id VARCHAR(18)  		--//  ENCODE zstd
	,description VARCHAR(255)  		--//  ENCODE zstd
)

;
--DROP TABLE HCPOSEWKS_INTEGRATION.wks_hcp_osea_profile_rg;
CREATE OR REPLACE TABLE HCPOSEWKS_INTEGRATION.WKS_HCP_OSEA_PROFILE_RG		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE HCPOSEWKS_INTEGRATION.wks_hcp_osea_profile_rg
(
	profile_source_id VARCHAR(18)  		--//  ENCODE zstd
	,profile_name VARCHAR(255)  		--//  ENCODE zstd
	,userlicense_source_id VARCHAR(18)  		--//  ENCODE zstd
	,usertype VARCHAR(40)  		--//  ENCODE zstd
	,created_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,created_by_id VARCHAR(18)  		--//  ENCODE zstd
	,last_modified_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,last_modified_by_id VARCHAR(18)  		--//  ENCODE zstd
	,description VARCHAR(255)  		--//  ENCODE zstd
)

;
--DROP TABLE HCPOSEWKS_INTEGRATION.wks_hcp_osea_recordtype;
CREATE OR REPLACE TABLE HCPOSEWKS_INTEGRATION.WKS_HCP_OSEA_RECORDTYPE		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE HCPOSEWKS_INTEGRATION.wks_hcp_osea_recordtype
(
	record_type_source_id VARCHAR(18)  		--//  ENCODE zstd
	,record_type_name VARCHAR(255)  		--//  ENCODE zstd
	,developer_name VARCHAR(80)  		--//  ENCODE zstd
	,name_space_prefix VARCHAR(15)  		--//  ENCODE zstd
	,description VARCHAR(255)  		--//  ENCODE zstd
	,business_process_id VARCHAR(18)  		--//  ENCODE zstd
	,sobjecttype VARCHAR(40)  		--//  ENCODE zstd
	,is_active VARCHAR(5)  		--//  ENCODE zstd
	,is_person_type VARCHAR(5)  		--//  ENCODE zstd
	,created_by_id VARCHAR(18)  		--//  ENCODE zstd
	,created_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,last_modified_by_id VARCHAR(18)  		--//  ENCODE zstd
	,last_modified_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
)

;
--DROP TABLE HCPOSEWKS_INTEGRATION.wks_hcp_osea_recordtype_rg;
CREATE OR REPLACE TABLE HCPOSEWKS_INTEGRATION.WKS_HCP_OSEA_RECORDTYPE_RG		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE HCPOSEWKS_INTEGRATION.wks_hcp_osea_recordtype_rg
(
	record_type_source_id VARCHAR(18)  		--//  ENCODE zstd
	,record_type_name VARCHAR(255)  		--//  ENCODE zstd
	,developer_name VARCHAR(80)  		--//  ENCODE zstd
	,name_space_prefix VARCHAR(15)  		--//  ENCODE zstd
	,description VARCHAR(255)  		--//  ENCODE zstd
	,business_process_id VARCHAR(18)  		--//  ENCODE zstd
	,sobjecttype VARCHAR(40)  		--//  ENCODE zstd
	,is_active VARCHAR(5)  		--//  ENCODE zstd
	,created_by_id VARCHAR(18)  		--//  ENCODE zstd
	,created_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,last_modified_by_id VARCHAR(18)  		--//  ENCODE zstd
	,last_modified_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
)

;
--DROP TABLE HCPOSEWKS_INTEGRATION.wks_hcp_osea_remote_meeting;
CREATE OR REPLACE TABLE HCPOSEWKS_INTEGRATION.WKS_HCP_OSEA_REMOTE_MEETING		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE HCPOSEWKS_INTEGRATION.wks_hcp_osea_remote_meeting
(
	remote_meeting_source_id VARCHAR(18)  		--//  ENCODE lzo
	,owner_source_id VARCHAR(18)  		--//  ENCODE lzo
	,is_deleted VARCHAR(10)  		--//  ENCODE lzo
	,name VARCHAR(255)  		--//  ENCODE lzo
	,record_type_source_id VARCHAR(18)  		--//  ENCODE lzo
	,created_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,createdbyid VARCHAR(18)  		--//  ENCODE lzo
	,last_modified_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,lastmodifiedbyid VARCHAR(18)  		--//  ENCODE lzo
	,systemmodstamp TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,mayedit VARCHAR(10)  		--//  ENCODE lzo
	,islocked VARCHAR(10)  		--//  ENCODE lzo
	,meeting_id VARCHAR(18)  		--//  ENCODE lzo
	,meeting_name VARCHAR(255)  		--//  ENCODE lzo
	,mobile_source_id VARCHAR(100)  		--//  ENCODE lzo
	,scheduled_datetime TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,scheduled VARCHAR(10)  		--//  ENCODE lzo
	,meeting_password VARCHAR(20)  		--//  ENCODE lzo
	,meeting_outcome_status VARCHAR(255)  		--//  ENCODE lzo
	,jj_host_country VARCHAR(20)  		--//  ENCODE lzo
	,jj_numofattendee NUMERIC(18,1)  		--//  ENCODE az64
	,jj_owner_country VARCHAR(20)  		--//  ENCODE lzo
	,assigned_host VARCHAR(200)  		--//  ENCODE lzo
	,attendance_report_process_status VARCHAR(200)  		--//  ENCODE lzo
	,description VARCHAR(1000)  		--//  ENCODE lzo
	,latest_meeting_start_datetime TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,webinar_alternative_host_1 VARCHAR(200)  		--//  ENCODE lzo
	,webinar_alternative_host_2 VARCHAR(200)  		--//  ENCODE lzo
	,webinar_alternative_host_3 VARCHAR(200)  		--//  ENCODE lzo
	,rating_submitted VARCHAR(5)  		--//  ENCODE zstd
)

;
--DROP TABLE HCPOSEWKS_INTEGRATION.wks_hcp_osea_territory;
CREATE OR REPLACE TABLE HCPOSEWKS_INTEGRATION.WKS_HCP_OSEA_TERRITORY		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE HCPOSEWKS_INTEGRATION.wks_hcp_osea_territory
(
	territory_source_id VARCHAR(18)  		--//  ENCODE zstd
	,territory_name VARCHAR(80)  		--//  ENCODE zstd
	,territory_type_source_id VARCHAR(18)  		--//  ENCODE zstd
	,territory_model_source_id VARCHAR(18)  		--//  ENCODE zstd
	,parent_territory_source_id VARCHAR(18)  		--//  ENCODE zstd
	,description VARCHAR(1000)  		--//  ENCODE zstd
	,account_access_level VARCHAR(40)  		--//  ENCODE zstd
	,contact_access_level VARCHAR(40)  		--//  ENCODE zstd
	,last_modified_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,last_modified_by_id VARCHAR(18)  		--//  ENCODE zstd
	,system_mod_stamp TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,developer_name VARCHAR(80)  		--//  ENCODE zstd
	,parent_territory1_source_id VARCHAR(18)  		--//  ENCODE zstd
	,territory1_source_id VARCHAR(18)  		--//  ENCODE zstd
)

;
--DROP TABLE HCPOSEWKS_INTEGRATION.wks_hcp_osea_territory_model;
CREATE OR REPLACE TABLE HCPOSEWKS_INTEGRATION.WKS_HCP_OSEA_TERRITORY_MODEL		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE HCPOSEWKS_INTEGRATION.wks_hcp_osea_territory_model
(
	territory_model_source_id VARCHAR(18)  		--//  ENCODE zstd
	,is_deleted VARCHAR(5)  		--//  ENCODE zstd
	,name VARCHAR(80)  		--//  ENCODE zstd
	,created_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,created_by_id VARCHAR(18)  		--//  ENCODE zstd
	,last_modified_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,last_modified_by_id VARCHAR(18)  		--//  ENCODE zstd
	,system_mod_stamp TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,may_edit VARCHAR(5)  		--//  ENCODE zstd
	,is_locked VARCHAR(5)  		--//  ENCODE zstd
	,description VARCHAR(255)  		--//  ENCODE zstd
	,activated_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,deactivated_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,state VARCHAR(255)  		--//  ENCODE zstd
	,developer_name VARCHAR(80)  		--//  ENCODE zstd
	,last_run_rules_end_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,is_clone_source VARCHAR(5)  		--//  ENCODE zstd
	,last_opp_terr_assign_end_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
)

;
--DROP TABLE HCPOSEWKS_INTEGRATION.wks_hcp_osea_time_off_territory;
CREATE OR REPLACE TABLE HCPOSEWKS_INTEGRATION.WKS_HCP_OSEA_TIME_OFF_TERRITORY		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE HCPOSEWKS_INTEGRATION.wks_hcp_osea_time_off_territory
(
	tot_source_id VARCHAR(18)  		--//  ENCODE zstd
	,owner_source_id VARCHAR(18)  		--//  ENCODE zstd
	,is_deleted VARCHAR(5)  		--//  ENCODE zstd
	,tot_name VARCHAR(80)  		--//  ENCODE zstd
	,record_type_source_id VARCHAR(18)  		--//  ENCODE zstd
	,created_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,created_by_id VARCHAR(18)  		--//  ENCODE zstd
	,last_modified_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,last_modified_by_id VARCHAR(18)  		--//  ENCODE zstd
	,reason VARCHAR(255)  		--//  ENCODE zstd
	,territory_name VARCHAR(255)  		--//  ENCODE zstd
	,tot_date DATE  		--//  ENCODE delta
	,status_cd VARCHAR(255)  		--//  ENCODE zstd
	,time_type VARCHAR(255)  		--//  ENCODE zstd
	,working_hours_on NUMERIC(18,0)  		--//  ENCODE zstd
	,mobile_id VARCHAR(100)  		--//  ENCODE zstd
	,working_hours_off VARCHAR(255)  		--//  ENCODE zstd
	,start_time VARCHAR(255)  		--//  ENCODE zstd
	,simp_time_on_time_off VARCHAR(255)  		--//  ENCODE zstd
	,simp_frml_hours_on NUMERIC(18,0)  		--//  ENCODE zstd
	,frml_total_work_days NUMERIC(18,0)  		--//  ENCODE zstd
	,simp_frml_non_working_hours_off NUMERIC(18,0)  		--//  ENCODE zstd
	,frml_planned_work_days NUMERIC(18,0)  		--//  ENCODE zstd
	,simp_description VARCHAR(800)  		--//  ENCODE zstd
	,time_on VARCHAR(255)  		--//  ENCODE zstd
	,user_name VARCHAR(18)  		--//  ENCODE zstd
	,user_profile VARCHAR(1300)  		--//  ENCODE zstd
	,sm_reason VARCHAR(255)  		--//  ENCODE zstd
	,calculatedhours_off NUMERIC(18,0)  		--//  ENCODE zstd
	,total_time_off NUMERIC(18,0)  		--//  ENCODE zstd
	,approval_status VARCHAR(255)  		--//  ENCODE zstd
	,owners_manager_email_id VARCHAR(80)  		--//  ENCODE zstd
	,country_code VARCHAR(1300)  		--//  ENCODE zstd
)

;
--DROP TABLE HCPOSEWKS_INTEGRATION.wks_hcp_osea_user;
CREATE OR REPLACE TABLE HCPOSEWKS_INTEGRATION.WKS_HCP_OSEA_USER		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE HCPOSEWKS_INTEGRATION.wks_hcp_osea_user
(
	employee_source_id VARCHAR(18)  		--//  ENCODE lzo
	,user_name VARCHAR(80)  		--//  ENCODE lzo
	,employee_name VARCHAR(121)  		--//  ENCODE lzo
	,company_name VARCHAR(80)  		--//  ENCODE lzo
	,division VARCHAR(80)  		--//  ENCODE lzo
	,department VARCHAR(80)  		--//  ENCODE lzo
	,title VARCHAR(80)  		--//  ENCODE lzo
	,country VARCHAR(80)  		--//  ENCODE lzo
	,address VARCHAR(255)  		--//  ENCODE lzo
	,email VARCHAR(128)  		--//  ENCODE lzo
	,phone VARCHAR(40)  		--//  ENCODE lzo
	,mobile_phone VARCHAR(40)  		--//  ENCODE lzo
	,alias VARCHAR(8)  		--//  ENCODE lzo
	,nickname VARCHAR(40)  		--//  ENCODE lzo
	,is_active VARCHAR(5)  		--//  ENCODE lzo
	,timezonesidkey VARCHAR(40)  		--//  ENCODE lzo
	,user_role_source_id VARCHAR(18)  		--//  ENCODE lzo
	,receives_info_emails VARCHAR(5)  		--//  ENCODE lzo
	,employee_profile_id VARCHAR(18)  		--//  ENCODE lzo
	,local_employee_number VARCHAR(20)  		--//  ENCODE lzo
	,manager_source_id VARCHAR(18)  		--//  ENCODE lzo
	,last_login_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,created_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,created_by_id VARCHAR(18)  		--//  ENCODE lzo
	,last_modified_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,last_modified_by_id VARCHAR(18)  		--//  ENCODE lzo
	,federation_identifier VARCHAR(512)  		--//  ENCODE lzo
	,last_ipad_sync TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,country_code VARCHAR(255)  		--//  ENCODE lzo
	,wwid VARCHAR(9)  		--//  ENCODE lzo
	,region VARCHAR(80)  		--//  ENCODE lzo
	,profile_group_ap VARCHAR(1300)  		--//  ENCODE lzo
	,user_license VARCHAR(1300)  		--//  ENCODE lzo
	,msl_primary_responsible_ta VARCHAR(255)  		--//  ENCODE lzo
	,msl_secondary_responsible_ta VARCHAR(255)  		--//  ENCODE lzo
	,last_name VARCHAR(80)  		--//  ENCODE lzo
	,first_name VARCHAR(40)  		--//  ENCODE lzo
	,city VARCHAR(40)  		--//  ENCODE lzo
	,state VARCHAR(80)  		--//  ENCODE lzo
	,postal_code VARCHAR(20)  		--//  ENCODE lzo
	,user_type VARCHAR(40)  		--//  ENCODE lzo
	,language_local_key VARCHAR(40)  		--//  ENCODE lzo
	,last_mobile_sync TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,veeva_user_type VARCHAR(255)  		--//  ENCODE zstd
	,veeva_country_code VARCHAR(255)  		--//  ENCODE zstd
	,shc_user_franchise VARCHAR(255)  		--//  ENCODE zstd
)

;
--DROP TABLE HCPOSEWKS_INTEGRATION.wks_hcp_osea_user_rg;
CREATE OR REPLACE TABLE HCPOSEWKS_INTEGRATION.WKS_HCP_OSEA_USER_RG		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE HCPOSEWKS_INTEGRATION.wks_hcp_osea_user_rg
(
	employee_source_id VARCHAR(18)  		--//  ENCODE zstd
	,user_name VARCHAR(80)  		--//  ENCODE zstd
	,last_name VARCHAR(80)  		--//  ENCODE zstd
	,first_name VARCHAR(40)  		--//  ENCODE zstd
	,employee_name VARCHAR(121)  		--//  ENCODE zstd
	,company_name VARCHAR(80)  		--//  ENCODE zstd
	,division VARCHAR(80)  		--//  ENCODE zstd
	,department VARCHAR(80)  		--//  ENCODE zstd
	,title VARCHAR(80)  		--//  ENCODE zstd
	,city VARCHAR(40)  		--//  ENCODE zstd
	,state VARCHAR(80)  		--//  ENCODE zstd
	,postal_code VARCHAR(20)  		--//  ENCODE zstd
	,country VARCHAR(80)  		--//  ENCODE zstd
	,address VARCHAR(255)  		--//  ENCODE zstd
	,email VARCHAR(128)  		--//  ENCODE zstd
	,phone VARCHAR(40)  		--//  ENCODE zstd
	,mobile_phone VARCHAR(40)  		--//  ENCODE zstd
	,alias VARCHAR(8)  		--//  ENCODE zstd
	,nickname VARCHAR(40)  		--//  ENCODE zstd
	,is_active VARCHAR(5)  		--//  ENCODE zstd
	,timezonesidkey VARCHAR(40)  		--//  ENCODE zstd
	,user_role_source_id VARCHAR(18)  		--//  ENCODE zstd
	,receives_info_emails VARCHAR(5)  		--//  ENCODE zstd
	,employee_profile_id VARCHAR(18)  		--//  ENCODE zstd
	,user_type VARCHAR(40)  		--//  ENCODE zstd
	,language_local_key VARCHAR(40)  		--//  ENCODE zstd
	,local_employee_number VARCHAR(20)  		--//  ENCODE zstd
	,manager_source_id VARCHAR(18)  		--//  ENCODE zstd
	,last_login_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,created_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,created_by_id VARCHAR(18)  		--//  ENCODE zstd
	,last_modified_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,last_modified_by_id VARCHAR(18)  		--//  ENCODE zstd
	,federation_identifier VARCHAR(512)  		--//  ENCODE zstd
)

;
--DROP TABLE HCPOSEWKS_INTEGRATION.wks_hcp_osea_userterritory;
CREATE OR REPLACE TABLE HCPOSEWKS_INTEGRATION.WKS_HCP_OSEA_USERTERRITORY		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE HCPOSEWKS_INTEGRATION.wks_hcp_osea_userterritory
(
	user_territory_source_id VARCHAR(18)  		--//  ENCODE zstd
	,user_territory_user_source_id VARCHAR(18)  		--//  ENCODE zstd
	,territory_source_id VARCHAR(18)  		--//  ENCODE zstd
	,is_active VARCHAR(5)  		--//  ENCODE zstd
	,role_in_territory VARCHAR(255)  		--//  ENCODE zstd
	,last_modified_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,last_modified_by_id VARCHAR(18)  		--//  ENCODE zstd
)

;

--DROP TABLE HCPOSEITG_INTEGRATION.sdl_raw_hcp_osea_account_hco;
CREATE OR REPLACE TABLE HCPOSEITG_INTEGRATION.SDL_RAW_HCP_OSEA_ACCOUNT_HCO		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE HCPOSEITG_INTEGRATION.sdl_raw_hcp_osea_account_hco
(
	account_source_id VARCHAR(18)  		--//  ENCODE zstd
	,is_deleted VARCHAR(5)  		--//  ENCODE zstd
	,account_name VARCHAR(255)  		--//  ENCODE zstd
	,record_type_source_id VARCHAR(18)  		--//  ENCODE zstd
	,phone_number VARCHAR(40)  		--//  ENCODE zstd
	,fax_number VARCHAR(40)  		--//  ENCODE zstd
	,website VARCHAR(255)  		--//  ENCODE zstd
	,owner_id VARCHAR(18)  		--//  ENCODE zstd
	,created_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,created_by_id VARCHAR(18)  		--//  ENCODE zstd
	,last_modified_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,last_modified_by_id VARCHAR(18)  		--//  ENCODE zstd
	,is_excluded_from_realign VARCHAR(5)  		--//  ENCODE zstd
	,is_person_account VARCHAR(5)  		--//  ENCODE zstd
	,external_id VARCHAR(120)  		--//  ENCODE zstd
	,territory_name VARCHAR(255)  		--//  ENCODE zstd
	,hco_name VARCHAR(1300)  		--//  ENCODE zstd
	,beds NUMERIC(4,0)  		--//  ENCODE delta
	,primary_parent_source_id VARCHAR(18)  		--//  ENCODE zstd
	,total_mds_dos NUMERIC(18,0)  		--//  ENCODE delta
	,departments NUMERIC(18,0)  		--//  ENCODE delta
	,hco_type VARCHAR(255)  		--//  ENCODE zstd
	,hco_sector VARCHAR(255)  		--//  ENCODE zstd
	,remarks VARCHAR(255)  		--//  ENCODE zstd
	,sfe_approved VARCHAR(5)  		--//  ENCODE zstd
	,country_code VARCHAR(2)  		--//  ENCODE zstd
	,business_description VARCHAR(32000)  		--//  ENCODE zstd
	,hcc_account_approved VARCHAR(5)  		--//  ENCODE zstd
	,tw_customer_code VARCHAR(60)  		--//  ENCODE zstd
	,inactive VARCHAR(5)  		--//  ENCODE zstd
	,total_physicians_enrolled NUMERIC(18,0)  		--//  ENCODE delta
	,kam_clinical_differentiations VARCHAR(32768)  		--//  ENCODE zstd
	,kam_general_differnentiations VARCHAR(32768)  		--//  ENCODE zstd
	,total_pharmacists NUMERIC(3,0)  		--//  ENCODE delta
	,kam_total_aestheticsurgeons NUMERIC(18,0)  		--//  ENCODE delta
	,kam_total_cardiosurgeons NUMERIC(18,0)  		--//  ENCODE delta
	,kam_total_cardiologyphysicians NUMERIC(18,0)  		--//  ENCODE delta
	,kam_total_dermaphysicians NUMERIC(18,0)  		--//  ENCODE delta
	,kam_total_endophysicians NUMERIC(18,0)  		--//  ENCODE delta
	,kam_total_gastrophysicians NUMERIC(18,0)  		--//  ENCODE delta
	,kam_total_generalsurgeons NUMERIC(18,0)  		--//  ENCODE delta
	,kam_total_haemaphysicians NUMERIC(18,0)  		--//  ENCODE delta
	,kam_total_infectiousphysicians NUMERIC(18,0)  		--//  ENCODE delta
	,kam_total_medoncophysicians NUMERIC(18,0)  		--//  ENCODE delta
	,kam_total_neurologyphysicians NUMERIC(18,0)  		--//  ENCODE delta
	,kam_total_opthalsurgeons NUMERIC(18,0)  		--//  ENCODE delta
	,kam_total_orthosurgeons NUMERIC(18,0)  		--//  ENCODE delta
	,kam_total_psychiatryphysicians NUMERIC(18,0)  		--//  ENCODE delta
	,kam_total_rheumphysicians NUMERIC(18,0)  		--//  ENCODE delta
	,kam_total_surgeons NUMERIC(18,0)  		--//  ENCODE delta
	,kam_total_urologysurgeons NUMERIC(18,0)  		--//  ENCODE delta
	,kam_minimally_invasive NUMERIC(18,0)  		--//  ENCODE delta
	,kam_obgyn NUMERIC(18,0)  		--//  ENCODE delta
	,kam_paediatric NUMERIC(18,0)  		--//  ENCODE delta
	,ot NUMERIC(18,0)  		--//  ENCODE delta
	,is_external_id_number VARCHAR(5)  		--//  ENCODE zstd
	,jj_external_id VARCHAR(90)  		--//  ENCODE zstd
	,specialty_2 VARCHAR(255)  		--//  ENCODE zstd
	,sub_specialty VARCHAR(255)  		--//  ENCODE zstd
	,sea_account_classification VARCHAR(255)  		--//  ENCODE zstd
	,jj_email_1 VARCHAR(80)  		--//  ENCODE zstd
	,jj_email_2 VARCHAR(80)  		--//  ENCODE zstd
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
	,filename VARCHAR(100)  		--//  ENCODE zstd
	,run_id VARCHAR(14)  		--//  ENCODE zstd
)

;
--DROP TABLE HCPOSEITG_INTEGRATION.sdl_raw_hcp_osea_account_hcp;
CREATE OR REPLACE TABLE HCPOSEITG_INTEGRATION.SDL_RAW_HCP_OSEA_ACCOUNT_HCP		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE HCPOSEITG_INTEGRATION.sdl_raw_hcp_osea_account_hcp
(
	account_source_id VARCHAR(18)  		--//  ENCODE zstd
	,is_deleted VARCHAR(5)  		--//  ENCODE zstd
	,hcp_name VARCHAR(255)  		--//  ENCODE zstd
	,last_name VARCHAR(80)  		--//  ENCODE zstd
	,first_name VARCHAR(40)  		--//  ENCODE zstd
	,salutation VARCHAR(120)  		--//  ENCODE zstd
	,record_type_source_id VARCHAR(18)  		--//  ENCODE zstd
	,owner_id VARCHAR(18)  		--//  ENCODE zstd
	,created_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,created_by_id VARCHAR(18)  		--//  ENCODE zstd
	,last_modified_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,last_modified_by_id VARCHAR(18)  		--//  ENCODE zstd
	,is_excluded_from_realign VARCHAR(5)  		--//  ENCODE zstd
	,is_person_account VARCHAR(5)  		--//  ENCODE zstd
	,person_mobile_phone VARCHAR(40)  		--//  ENCODE zstd
	,person_email VARCHAR(80)  		--//  ENCODE zstd
	,birth_day DATE  		--//  ENCODE delta
	,person_has_opted_out_of_email VARCHAR(5)  		--//  ENCODE zstd
	,person_do_not_call VARCHAR(5)  		--//  ENCODE zstd
	,external_id VARCHAR(120)  		--//  ENCODE zstd
	,territory_name VARCHAR(255)  		--//  ENCODE zstd
	,specialty_1 VARCHAR(255)  		--//  ENCODE zstd
	,gender VARCHAR(255)  		--//  ENCODE zstd
	,preferred_name VARCHAR(12)  		--//  ENCODE zstd
	,primary_parent_source_id VARCHAR(18)  		--//  ENCODE zstd
	,hcp_type VARCHAR(255)  		--//  ENCODE zstd
	,professional_type VARCHAR(255)  		--//  ENCODE zstd
	,hcp_english_name VARCHAR(800)  		--//  ENCODE zstd
	,remarks VARCHAR(255)  		--//  ENCODE zstd
	,position VARCHAR(255)  		--//  ENCODE zstd
	,direct_line VARCHAR(40)  		--//  ENCODE zstd
	,direct_fax VARCHAR(40)  		--//  ENCODE zstd
	,sfe_approved VARCHAR(5)  		--//  ENCODE zstd
	,country_code VARCHAR(2)  		--//  ENCODE zstd
	,go_classification VARCHAR(255)  		--//  ENCODE zstd
	,hcc_account_approved VARCHAR(5)  		--//  ENCODE zstd
	,account_classification VARCHAR(255)  		--//  ENCODE zstd
	,customer_code_2 VARCHAR(20)  		--//  ENCODE zstd
	,is_kol VARCHAR(5)  		--//  ENCODE zstd
	,inactive VARCHAR(5)  		--//  ENCODE zstd
	,physician_prescribing_behavior VARCHAR(255)  		--//  ENCODE zstd
	,physician_behavioral_style VARCHAR(255)  		--//  ENCODE zstd
	,is_external_id_number VARCHAR(5)  		--//  ENCODE zstd
	,customer_value_segmentation VARCHAR(255)  		--//  ENCODE zstd
	,ability_to_influence_peers VARCHAR(255)  		--//  ENCODE zstd
	,practice_size VARCHAR(255)  		--//  ENCODE zstd
	,patient_type VARCHAR(255)  		--//  ENCODE zstd
	,patient_medical_condition VARCHAR(255)  		--//  ENCODE zstd
	,md_customer_segmentation VARCHAR(255)  		--//  ENCODE zstd
	,years_of_experience VARCHAR(255)  		--//  ENCODE zstd
	,md_innovation VARCHAR(255)  		--//  ENCODE zstd
	,md_number_of_procedures VARCHAR(255)  		--//  ENCODE zstd
	,md_types_of_procedure VARCHAR(255)  		--//  ENCODE zstd
	,md_total_hip_replacement NUMERIC(18,0)  		--//  ENCODE delta
	,md_total_knee_replacement NUMERIC(18,0)  		--//  ENCODE delta
	,md_spine NUMERIC(18,0)  		--//  ENCODE delta
	,md_trauma NUMERIC(18,0)  		--//  ENCODE delta
	,md_collorectal NUMERIC(18,0)  		--//  ENCODE delta
	,md_hepatobiliary NUMERIC(18,0)  		--//  ENCODE delta
	,md_cholecystectomy NUMERIC(18,0)  		--//  ENCODE delta
	,md_total_hysterectomy NUMERIC(18,0)  		--//  ENCODE delta
	,md_myomectomy NUMERIC(18,0)  		--//  ENCODE delta
	,md_c_section NUMERIC(18,0)  		--//  ENCODE delta
	,md_normal_delivery NUMERIC(18,0)  		--//  ENCODE delta
	,md_cabg NUMERIC(18,0)  		--//  ENCODE delta
	,md_valve_repair NUMERIC(18,0)  		--//  ENCODE delta
	,md_abdominal NUMERIC(18,0)  		--//  ENCODE delta
	,md_breast_reconstruction NUMERIC(18,0)  		--//  ENCODE delta
	,md_oral_cranial_maxilofacial NUMERIC(18,0)  		--//  ENCODE delta
	,primary_ta VARCHAR(255)  		--//  ENCODE zstd
	,secondary_ta VARCHAR(255)  		--//  ENCODE zstd
	,jj_external_id VARCHAR(90)  		--//  ENCODE zstd
	,specialty_2 VARCHAR(255)  		--//  ENCODE zstd
	,sub_specialty VARCHAR(255)  		--//  ENCODE zstd
	,sea_account_classification VARCHAR(255)  		--//  ENCODE zstd
	,jj_email_1 VARCHAR(80)  		--//  ENCODE zstd
	,jj_email_2 VARCHAR(80)  		--//  ENCODE zstd
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
	,filename VARCHAR(100)  		--//  ENCODE zstd
	,run_id VARCHAR(14)  		--//  ENCODE zstd
)

;
--DROP TABLE HCPOSEITG_INTEGRATION.sdl_raw_hcp_osea_address;
CREATE OR REPLACE TABLE HCPOSEITG_INTEGRATION.SDL_RAW_HCP_OSEA_ADDRESS		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE HCPOSEITG_INTEGRATION.sdl_raw_hcp_osea_address
(
	address_source_id VARCHAR(18)  		--//  ENCODE zstd
	,is_deleted VARCHAR(5)  		--//  ENCODE zstd
	,address_name VARCHAR(83)  		--//  ENCODE zstd
	,record_type_source_id VARCHAR(18)  		--//  ENCODE zstd
	,created_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,created_by_id VARCHAR(18)  		--//  ENCODE zstd
	,last_modified_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,last_modified_by_id VARCHAR(18)  		--//  ENCODE zstd
	,account_source_id VARCHAR(100)  		--//  ENCODE zstd
	,address_line2_name VARCHAR(300)  		--//  ENCODE zstd
	,city VARCHAR(40)  		--//  ENCODE zstd
	,external_id VARCHAR(120)  		--//  ENCODE zstd
	,phone VARCHAR(40)  		--//  ENCODE zstd
	,fax VARCHAR(40)  		--//  ENCODE zstd
	,map VARCHAR(1300)  		--//  ENCODE zstd
	,is_primary VARCHAR(5)  		--//  ENCODE zstd
	,appt_required VARCHAR(5)  		--//  ENCODE zstd
	,inactive VARCHAR(5)  		--//  ENCODE zstd
	,country_code VARCHAR(255)  		--//  ENCODE zstd
	,latitude NUMERIC(15,8)  		--//  ENCODE delta
	,zip VARCHAR(20)  		--//  ENCODE zstd
	,brick VARCHAR(250)  		--//  ENCODE zstd
	,state VARCHAR(255)  		--//  ENCODE zstd
	,longitude NUMERIC(15,8)  		--//  ENCODE delta
	,controlling_address VARCHAR(18)  		--//  ENCODE zstd
	,suburb_town VARCHAR(50)  		--//  ENCODE zstd
	,veeva_autogen_id VARCHAR(30)  		--//  ENCODE zstd
	,is_locked VARCHAR(5)  		--//  ENCODE zstd
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
	,filename VARCHAR(100)  		--//  ENCODE zstd
	,run_id VARCHAR(14)  		--//  ENCODE zstd
)

;
--DROP TABLE HCPOSEITG_INTEGRATION.sdl_raw_hcp_osea_call;
CREATE OR REPLACE TABLE HCPOSEITG_INTEGRATION.SDL_RAW_HCP_OSEA_CALL		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE HCPOSEITG_INTEGRATION.sdl_raw_hcp_osea_call
(
	call_source_id VARCHAR(18)  		--//  ENCODE zstd
	,owner_source_id VARCHAR(18)  		--//  ENCODE zstd
	,is_deleted VARCHAR(5)  		--//  ENCODE zstd
	,call_name VARCHAR(80)  		--//  ENCODE zstd
	,record_type_source_id VARCHAR(18)  		--//  ENCODE zstd
	,created_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,created_by_id VARCHAR(18)  		--//  ENCODE zstd
	,last_modified_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,last_modified_by_id VARCHAR(18)  		--//  ENCODE zstd
	,last_activity_date DATE  		--//  ENCODE delta
	,is_locked VARCHAR(5)  		--//  ENCODE zstd
	,call_comments VARCHAR(32000)  		--//  ENCODE zstd
	,sample_card VARCHAR(15)  		--//  ENCODE zstd
	,account_source_id VARCHAR(18)  		--//  ENCODE zstd
	,call_status_type VARCHAR(255)  		--//  ENCODE zstd
	,parent_address VARCHAR(18)  		--//  ENCODE zstd
	,account_plan VARCHAR(18)  		--//  ENCODE zstd
	,next_call_notes VARCHAR(32000)  		--//  ENCODE zstd
	,pre_call_notes VARCHAR(32000)  		--//  ENCODE zstd
	,mobile_id VARCHAR(100)  		--//  ENCODE zstd
	,significant_event VARCHAR(5)  		--//  ENCODE zstd
	,location VARCHAR(128)  		--//  ENCODE zstd
	,subject VARCHAR(128)  		--//  ENCODE zstd
	,call_date_time TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,disbursed_to VARCHAR(255)  		--//  ENCODE zstd
	,signature_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,signature VARCHAR(32000)  		--//  ENCODE zstd
	,territory VARCHAR(100)  		--//  ENCODE zstd
	,submitted_by_mobile VARCHAR(5)  		--//  ENCODE zstd
	,call_type VARCHAR(255)  		--//  ENCODE zstd
	,address VARCHAR(500)  		--//  ENCODE zstd
	,attendees NUMERIC(3,0)  		--//  ENCODE delta
	,attendee_type VARCHAR(255)  		--//  ENCODE zstd
	,call_date DATE  		--//  ENCODE delta
	,detailed_products VARCHAR(255)  		--//  ENCODE zstd
	,no_disbursement VARCHAR(5)  		--//  ENCODE zstd
	,parent_call_source_id VARCHAR(18)  		--//  ENCODE zstd
	,user_name VARCHAR(18)  		--//  ENCODE zstd
	,medical_event VARCHAR(18)  		--//  ENCODE zstd
	,mobile_created_date_time TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,mobile_last_modified_date_time TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,is_parent_call NUMERIC(18,0)  		--//  ENCODE delta
	,last_device VARCHAR(255)  		--//  ENCODE zstd
	,call_clm VARCHAR(5)  		--//  ENCODE zstd
	,is_sampled_call VARCHAR(5)  		--//  ENCODE zstd
	,presentations VARCHAR(500)  		--//  ENCODE zstd
	,duration NUMERIC(18,1)  		--//  ENCODE delta
	,allowed_products VARCHAR(1000)  		--//  ENCODE zstd
	,address_line_1 VARCHAR(80)  		--//  ENCODE zstd
	,address_line_2 VARCHAR(100)  		--//  ENCODE zstd
	,city VARCHAR(40)  		--//  ENCODE zstd
	,state VARCHAR(10)  		--//  ENCODE zstd
	,product_priority_1 VARCHAR(18)  		--//  ENCODE zstd
	,product_priority_2 VARCHAR(18)  		--//  ENCODE zstd
	,product_priority_3 VARCHAR(18)  		--//  ENCODE zstd
	,product_priority_4 VARCHAR(18)  		--//  ENCODE zstd
	,product_priority_5 VARCHAR(18)  		--//  ENCODE zstd
	,attendee_list VARCHAR(32768)  		--//  ENCODE zstd
	,signature_timestamp NUMERIC(15,1)  		--//  ENCODE delta
	,total_expense_attendees_count NUMERIC(15,1)  		--//  ENCODE delta
	,location_services_status VARCHAR(255)  		--//  ENCODE zstd
	,signature_location_latitude NUMERIC(15,6)  		--//  ENCODE delta
	,signature_location_longitude NUMERIC(15,6)  		--//  ENCODE delta
	,msl_interaction_notes VARCHAR(32768)  		--//  ENCODE zstd
	,sea_call_type VARCHAR(1300)  		--//  ENCODE zstd
	,signature_on_sync VARCHAR(1300)  		--//  ENCODE zstd
	,call_duration VARCHAR(255)  		--//  ENCODE zstd
	,interaction_mode VARCHAR(255)  		--//  ENCODE zstd
	,hcp_kol_initiated VARCHAR(5)  		--//  ENCODE zstd
	,msl_interaction_type VARCHAR(255)  		--//  ENCODE zstd
	,call_clm_location_latitude NUMERIC(15,6)  		--//  ENCODE delta
	,call_clm_location_longitude NUMERIC(15,6)  		--//  ENCODE delta
	,call_clm_location_services_status VARCHAR(255)  		--//  ENCODE zstd
	,parent_call_mobile_id VARCHAR(100)  		--//  ENCODE zstd
	,submit_location_latitude NUMERIC(15,6)  		--//  ENCODE delta
	,submit_location_longitude NUMERIC(15,6)  		--//  ENCODE delta
	,submit_location_services_status VARCHAR(255)  		--//  ENCODE zstd
	,submit_timestamp NUMERIC(15,1)  		--//  ENCODE delta
	,in_manager_insights VARCHAR(32768)  		--//  ENCODE zstd
	,md_hours VARCHAR(255)  		--//  ENCODE zstd
	,md_in_or_ot VARCHAR(5)  		--//  ENCODE zstd
	,md_d_call_type VARCHAR(255)  		--//  ENCODE zstd
	,md_minutes VARCHAR(255)  		--//  ENCODE zstd
	,em_event VARCHAR(18)  		--//  ENCODE zstd
	,medical_inquiry VARCHAR(18)  		--//  ENCODE zstd
	,parent_address_id VARCHAR(255)  		--//  ENCODE zstd
	,suggestion VARCHAR(18)  		--//  ENCODE zstd
	,call_objective VARCHAR(255)  		--//  ENCODE zstd
	,account_classification VARCHAR(255)  		--//  ENCODE zstd
	,submission_delay NUMERIC(18,0)  		--//  ENCODE delta
	,number_of_days_to_close_calls_kpi__c NUMERIC(18,2)  		--//  ENCODE delta
	,country VARCHAR(1300)  		--//  ENCODE zstd
	,region VARCHAR(1300)  		--//  ENCODE zstd
	,child_account_id VARCHAR(40)  		--//  ENCODE zstd
	,child_account VARCHAR(18)  		--//  ENCODE zstd
	,location_id VARCHAR(40)  		--//  ENCODE zstd
	,location_name VARCHAR(18)  		--//  ENCODE zstd
	,md_hsp_admin VARCHAR(255)  		--//  ENCODE zstd
	,hsp_minutes VARCHAR(255)  		--//  ENCODE zstd
	,ortho_on_call_case VARCHAR(5)  		--//  ENCODE zstd
	,ortho_volunteer_case VARCHAR(5)  		--//  ENCODE zstd
	,md_calc1 NUMERIC(18,2)  		--//  ENCODE delta
	,md_calculate_non_case_time NUMERIC(18,2)  		--//  ENCODE delta
	,md_calculated_hours_field NUMERIC(18,2)  		--//  ENCODE delta
	,md_casedeployment VARCHAR(255)  		--//  ENCODE zstd
	,md_case_coverage_12_hours VARCHAR(255)  		--//  ENCODE zstd
	,md_product_discussion VARCHAR(255)  		--//  ENCODE zstd
	,md_concurrent_call VARCHAR(5)  		--//  ENCODE zstd
	,courtesy_call VARCHAR(255)  		--//  ENCODE zstd
	,md_in_service VARCHAR(255)  		--//  ENCODE zstd
	,md_kol_course_discussion VARCHAR(255)  		--//  ENCODE zstd
	,kol_minutes VARCHAR(255)  		--//  ENCODE zstd
	,other_activities_time_12_hours NUMERIC(18,0)  		--//  ENCODE delta
	,other_in_field_activities VARCHAR(4099)  		--//  ENCODE zstd
	,md_overseas_workshop_visit VARCHAR(255)  		--//  ENCODE zstd
	,md_ra_activities2 VARCHAR(255)  		--//  ENCODE zstd
	,sales_activity VARCHAR(4099)  		--//  ENCODE zstd
	,sales_time_12_hours NUMERIC(18,0)  		--//  ENCODE delta
	,time_spent VARCHAR(255)  		--//  ENCODE zstd
	,time_spent_on_other_activities_simp VARCHAR(255)  		--//  ENCODE zstd
	,time_spent_on_sales_activity VARCHAR(255)  		--//  ENCODE zstd
	,md_time_spent_on_a_call NUMERIC(18,2)  		--//  ENCODE delta
	,md_case_type VARCHAR(255)  		--//  ENCODE zstd
	,md_sets_activities VARCHAR(255)  		--//  ENCODE zstd
	,md_time_spent_on_case VARCHAR(255)  		--//  ENCODE zstd
	,time_spent_on_other_activities NUMERIC(18,2)  		--//  ENCODE delta
	,time_spent_per_call NUMERIC(18,2)  		--//  ENCODE delta
	,md_case_conducted_in_hospital VARCHAR(255)  		--//  ENCODE zstd
	,calculated_field_2 NUMERIC(18,2)  		--//  ENCODE delta
	,calculated_hours_3 NUMERIC(18,2)  		--//  ENCODE delta
	,call_planned VARCHAR(5)  		--//  ENCODE zstd
	,call_submission_day NUMERIC(18,1)  		--//  ENCODE delta
	,check_in_latitude NUMERIC(15,6)  		--//  ENCODE delta
	,check_in_location_services_status VARCHAR(255)  		--//  ENCODE zstd
	,check_in_longitude NUMERIC(15,6)  		--//  ENCODE delta
	,check_in_status VARCHAR(255)  		--//  ENCODE zstd
	,check_in_timestamp NUMERIC(15,1)  		--//  ENCODE delta
	,medical_discussions VARCHAR(255)  		--//  ENCODE zstd
	,case_coverage NUMERIC(18,2)  		--//  ENCODE delta
	,call_duration_mins_in_number NUMERIC(18,2)  		--//  ENCODE delta
	,day_of_week VARCHAR(1300)  		--//  ENCODE zstd
	,veeva_remote_meeting_id VARCHAR(20)  		--//  ENCODE zstd
	,signature_page_display_name VARCHAR(255)  		--//  ENCODE zstd
	,disbursement_created VARCHAR(5)  		--//  ENCODE zstd
	,number_of_detailing NUMERIC(18,1)  		--//  ENCODE delta
	,joined_by_manager VARCHAR(5)  		--//  ENCODE zstd
	,pre_engagement_coaching VARCHAR(5)  		--//  ENCODE zstd
	,preparation_time NUMERIC(18,0)  		--//  ENCODE delta
	,travel_time NUMERIC(18,0)  		--//  ENCODE delta
	,rep_manager VARCHAR(1300)  		--//  ENCODE zstd
	,jj_therapeutic_area__c VARCHAR(255)  		--//  ENCODE zstd
	,numberof_key_message NUMERIC(18,1)  		--//  ENCODE delta
	,account_specialty VARCHAR(1300)  		--//  ENCODE zstd
	,owner_company_name VARCHAR(1300)  		--//  ENCODE zstd
	,call_submittedby VARCHAR(30)  		--//  ENCODE zstd
	,call_submitted_date_time TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,rep_division VARCHAR(1300)  		--//  ENCODE zstd
	,rep_department VARCHAR(1300)  		--//  ENCODE zstd
	,productivity_call VARCHAR(255)  		--//  ENCODE zstd
	,account_external_id VARCHAR(1300)  		--//  ENCODE zstd
	,call_objectives VARCHAR(4099)  		--//  ENCODE zstd
	,cost_of_procedures NUMERIC(18,1)  		--//  ENCODE delta
	,other_objective VARCHAR(255)  		--//  ENCODE zstd
	,hcp_speciality VARCHAR(1300)  		--//  ENCODE zstd
	,location_text VARCHAR(255)  		--//  ENCODE zstd
	,signed_by VARCHAR(255)  		--//  ENCODE zstd
	,any_aepqc_ss_i_have_reported_within_24h VARCHAR(255)  		--//  ENCODE zstd
	,end_datetime TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,remote_meeting_source_id VARCHAR(18)  		--//  ENCODE zstd
	,call_channel VARCHAR(255)  		--//  ENCODE zstd
	,fml_simp_case_product_discussion NUMERIC(18,2)  		--//  ENCODE delta
	,fml_simp_in_service NUMERIC(18,2)  		--//  ENCODE delta
	,fml_simp_overseas_workshop_visit NUMERIC(18,2)  		--//  ENCODE delta
	,signature_captured_remotely VARCHAR(5)  		--//  ENCODE zstd
	,virtual_channel_option VARCHAR(255)  		--//  ENCODE zstd
	,thmd_call_objectives VARCHAR(4099)  		--//  ENCODE zstd
	,thmd_case_type VARCHAR(255)  		--//  ENCODE zstd
	,phmd_call_objective VARCHAR(4099)  		--//  ENCODE zstd
	,idmd_call_objectives VARCHAR(4099)  		--//  ENCODE zstd
	,gsg_case_type VARCHAR(255)  		--//  ENCODE zstd
	,sis_case_type VARCHAR(255)  		--//  ENCODE zstd
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
	,filename VARCHAR(100)  		--//  ENCODE zstd
	,run_id VARCHAR(14)  		--//  ENCODE zstd
)

;
--DROP TABLE HCPOSEITG_INTEGRATION.sdl_raw_hcp_osea_call_detail;
CREATE OR REPLACE TABLE HCPOSEITG_INTEGRATION.SDL_RAW_HCP_OSEA_CALL_DETAIL		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE HCPOSEITG_INTEGRATION.sdl_raw_hcp_osea_call_detail
(
	call_detail_source_id VARCHAR(18)  		--//  ENCODE zstd
	,is_deleted VARCHAR(5)  		--//  ENCODE zstd
	,call_detail_name VARCHAR(80)  		--//  ENCODE zstd
	,created_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,created_by_id VARCHAR(18)  		--//  ENCODE zstd
	,last_modified_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,last_modified_by_id VARCHAR(18)  		--//  ENCODE zstd
	,is_locked VARCHAR(5)  		--//  ENCODE zstd
	,is_parent_call NUMERIC(18,0)  		--//  ENCODE delta
	,call_source_id VARCHAR(18)  		--//  ENCODE zstd
	,product_source_id VARCHAR(18)  		--//  ENCODE zstd
	,call_detail_priority NUMERIC(2,0)  		--//  ENCODE delta
	,detail_call_type VARCHAR(255)  		--//  ENCODE zstd
	,call_detail_group VARCHAR(18)  		--//  ENCODE zstd
	,product_id18 VARCHAR(1300)  		--//  ENCODE zstd
	,classification VARCHAR(40)  		--//  ENCODE zstd
	,my_adoption_ladder VARCHAR(255)  		--//  ENCODE zstd
	,simp_adoption_style VARCHAR(255)  		--//  ENCODE zstd
	,simp_behavioral_style VARCHAR(255)  		--//  ENCODE zstd
	,simp_market_share NUMERIC(18,1)  		--//  ENCODE delta
	,number_of_patients_per_quarter NUMERIC(18,1)  		--//  ENCODE delta
	,number_of_patients_per_month NUMERIC(18,1)  		--//  ENCODE delta
	,number_of_patients_per_week NUMERIC(18,1)  		--//  ENCODE delta
	,tw_adoption_ladder VARCHAR(255)  		--//  ENCODE zstd
	,tw_adoption_style VARCHAR(255)  		--//  ENCODE zstd
	,tw_behavioral_style VARCHAR(255)  		--//  ENCODE zstd
	,tw_market_share NUMERIC(18,1)  		--//  ENCODE delta
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
	,filename VARCHAR(100)  		--//  ENCODE zstd
	,run_id VARCHAR(14)  		--//  ENCODE zstd
)

;
--DROP TABLE HCPOSEITG_INTEGRATION.sdl_raw_hcp_osea_call_discussion;
CREATE OR REPLACE TABLE HCPOSEITG_INTEGRATION.SDL_RAW_HCP_OSEA_CALL_DISCUSSION		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE HCPOSEITG_INTEGRATION.sdl_raw_hcp_osea_call_discussion
(
	call_discussion_source_id VARCHAR(18)  		--//  ENCODE zstd
	,is_deleted VARCHAR(5)  		--//  ENCODE zstd
	,call_discussion_name VARCHAR(80)  		--//  ENCODE zstd
	,record_type_source_id VARCHAR(18)  		--//  ENCODE zstd
	,created_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,created_by_id VARCHAR(18)  		--//  ENCODE zstd
	,last_modified_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,last_modified_by_id VARCHAR(18)  		--//  ENCODE zstd
	,account_source_id VARCHAR(18)  		--//  ENCODE zstd
	,call_source_id VARCHAR(18)  		--//  ENCODE zstd
	,comments VARCHAR(800)  		--//  ENCODE zstd
	,product_source_id VARCHAR(18)  		--//  ENCODE zstd
	,discussion_topics VARCHAR(255)  		--//  ENCODE zstd
	,medical_event VARCHAR(18)  		--//  ENCODE zstd
	,is_parent_call NUMERIC(18,0)  		--//  ENCODE zstd
	,discussion_type VARCHAR(255)  		--//  ENCODE zstd
	,call_discussion_type VARCHAR(255)  		--//  ENCODE zstd
	,effectiveness VARCHAR(255)  		--//  ENCODE zstd
	,follow_up_activity VARCHAR(255)  		--//  ENCODE zstd
	,outcomes VARCHAR(255)  		--//  ENCODE zstd
	,follow_up_additional_info VARCHAR(800)  		--//  ENCODE zstd
	,follow_up_date DATE  		--//  ENCODE zstd
	,materials_used VARCHAR(255)  		--//  ENCODE zstd
	,call_date DATE  		--//  ENCODE delta
	,detail_group_source_id VARCHAR(18)  		--//  ENCODE zstd
	,user_source_id VARCHAR(18)  		--//  ENCODE zstd
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
	,filename VARCHAR(100)  		--//  ENCODE zstd
	,run_id VARCHAR(14)  		--//  ENCODE zstd
)

;
--DROP TABLE HCPOSEITG_INTEGRATION.sdl_raw_hcp_osea_call_key_message;
CREATE OR REPLACE TABLE HCPOSEITG_INTEGRATION.SDL_RAW_HCP_OSEA_CALL_KEY_MESSAGE		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE HCPOSEITG_INTEGRATION.sdl_raw_hcp_osea_call_key_message
(
	call_key_message_id VARCHAR(18)  		--//  ENCODE zstd
	,call_key_message_isdeleted VARCHAR(5)  		--//  ENCODE zstd
	,call_key_message_name VARCHAR(300)  		--//  ENCODE zstd
	,call_key_message_createddate TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,call_key_message_createdbyid VARCHAR(18)  		--//  ENCODE zstd
	,last_modified_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,call_key_message_lastmodifiedbyid VARCHAR(18)  		--//  ENCODE zstd
	,call_key_message_systemmodstamp TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,call_key_message_account VARCHAR(18)  		--//  ENCODE zstd
	,call_key_message_call2 VARCHAR(18)  		--//  ENCODE zstd
	,call_key_message_reaction VARCHAR(800)  		--//  ENCODE zstd
	,call_key_message_product VARCHAR(18)  		--//  ENCODE zstd
	,call_key_message_key_message VARCHAR(18)  		--//  ENCODE zstd
	,call_key_message_contact VARCHAR(18)  		--//  ENCODE zstd
	,call_key_message_call_date DATE  		--//  ENCODE delta
	,call_key_message_user VARCHAR(18)  		--//  ENCODE zstd
	,call_key_message_category VARCHAR(800)  		--//  ENCODE zstd
	,call_key_message_vehicle VARCHAR(800)  		--//  ENCODE zstd
	,call_key_message_is_parent_call NUMERIC(18,0)  		--//  ENCODE delta
	,call_key_message_clm_id VARCHAR(500)  		--//  ENCODE zstd
	,call_key_message_slide_version VARCHAR(300)  		--//  ENCODE zstd
	,call_key_message_duration NUMERIC(18,2)  		--//  ENCODE delta
	,call_key_message_presentation_id VARCHAR(300)  		--//  ENCODE zstd
	,call_key_message_start_time TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,call_key_message_attendee_type VARCHAR(800)  		--//  ENCODE zstd
	,call_key_message_entity_reference_id VARCHAR(300)  		--//  ENCODE zstd
	,call_key_message_segment VARCHAR(300)  		--//  ENCODE zstd
	,call_key_message_display_order NUMERIC(18,0)  		--//  ENCODE delta
	,call_key_message_detail_group VARCHAR(18)  		--//  ENCODE zstd
	,call_key_message_clm_presentation_name VARCHAR(300)  		--//  ENCODE zstd
	,call_key_message_clm_presentation_version VARCHAR(100)  		--//  ENCODE zstd
	,call_key_message_clm_presentation VARCHAR(18)  		--//  ENCODE zstd
	,key_message_name VARCHAR(80)  		--//  ENCODE zstd
	,key_message_description VARCHAR(1300)  		--//  ENCODE zstd
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
	,filename VARCHAR(100)  		--//  ENCODE zstd
	,run_id VARCHAR(14)  		--//  ENCODE zstd
)

;
--DROP TABLE HCPOSEITG_INTEGRATION.sdl_raw_hcp_osea_coaching_report;
CREATE OR REPLACE TABLE HCPOSEITG_INTEGRATION.SDL_RAW_HCP_OSEA_COACHING_REPORT		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE HCPOSEITG_INTEGRATION.sdl_raw_hcp_osea_coaching_report
(
	id VARCHAR(18)  		--//  ENCODE zstd
	,ownerid VARCHAR(18)  		--//  ENCODE zstd
	,isdeleted VARCHAR(5)  		--//  ENCODE zstd
	,name VARCHAR(80)  		--//  ENCODE zstd
	,recordtypeid VARCHAR(18)  		--//  ENCODE zstd
	,createddate TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,createdbyid VARCHAR(18)  		--//  ENCODE zstd
	,lastmodifieddate TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,lastmodifiedbyid VARCHAR(18)  		--//  ENCODE zstd
	,systemmodstamp TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,lastactivitydate TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,mayedit VARCHAR(20)  		--//  ENCODE zstd
	,islocked VARCHAR(5)  		--//  ENCODE zstd
	,lastvieweddate TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,lastreferenceddate TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,mobile_id_vod__c VARCHAR(1000)  		--//  ENCODE zstd
	,manager_vod__c VARCHAR(18)  		--//  ENCODE zstd
	,employee_vod__c VARCHAR(18)  		--//  ENCODE zstd
	,review_date__c DATE  		--//  ENCODE delta
	,review_period__c VARCHAR(20)  		--//  ENCODE zstd
	,status__c VARCHAR(255)  		--//  ENCODE zstd
	,overall_rating__c VARCHAR(100)  		--//  ENCODE zstd
	,jj_core_country_code__c VARCHAR(10)  		--//  ENCODE zstd
	,jj_core_lock__c VARCHAR(20)  		--//  ENCODE zstd
	,jj_core_no_of_visits__c VARCHAR(10)  		--//  ENCODE zstd
	,jj_employee_review_and_ackCURRENT_TIMESTAMP()ledged__c VARCHAR(5)  		--// 	,jj_employee_review_and_acknowledged__c VARCHAR(5)   ENCODE zstd //  ENCODE zstd
	,jj_employee_comments__c VARCHAR(2000)  		--//  ENCODE zstd
	,jj_simp_manager_comments_long__c VARCHAR(65535)  		--//  ENCODE zstd
	,jj_simp_objectives__c VARCHAR(65535)  		--//  ENCODE zstd
	,jj_simp_rep_comments_long__c VARCHAR(65535)  		--//  ENCODE zstd
	,jj_simp_sg_overall_rating__c NUMERIC(18,1)  		--//  ENCODE delta
	,jj_simp_long_comments__c VARCHAR(5000)  		--//  ENCODE zstd
	,kCURRENT_TIMESTAMP()ledge_strategy_overall_rating__c NUMERIC(18,1)  		--// 	,knowledge_strategy_overall_rating__c NUMERIC(18,1)   ENCODE delta //  ENCODE delta
	,selling_skills_overall_rating__c NUMERIC(18,1)  		--//  ENCODE delta
	,jj_my_call_type__c VARCHAR(255)  		--//  ENCODE zstd
	,jj_my_location__c VARCHAR(255)  		--//  ENCODE zstd
	,jj_id_overall_rating__c NUMERIC(18,1)  		--//  ENCODE delta
	,jj_vn_md_overall_rating_med__c NUMERIC(18,1)  		--//  ENCODE delta
	,jj_agreed_next_steps__c VARCHAR(2000)  		--//  ENCODE zstd
	,jj_coaching_for_field_visits__c VARCHAR(5)  		--//  ENCODE zstd
	,jj_customer_interactions__c VARCHAR(2000)  		--//  ENCODE zstd
	,jj_date_of_review_concluded__c DATE  		--//  ENCODE delta
	,jj_general_observations_and_comments__c VARCHAR(2000)  		--//  ENCODE zstd
	,jj_manager_feedback_completed__c VARCHAR(5)  		--//  ENCODE zstd
	,jj_number_of_coaching_days__c VARCHAR(255)  		--//  ENCODE zstd
	,jj_second_line_manager__c VARCHAR(18)  		--//  ENCODE zstd
	,jj_submission_to_date__c NUMERIC(18,1)  		--//  ENCODE lzo
	,relatedcoachingreport__c VARCHAR(255)  		--//  ENCODE zstd
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
	,filename VARCHAR(100)  		--//  ENCODE zstd
	,run_id VARCHAR(14)  		--//  ENCODE zstd
)

;
--DROP TABLE HCPOSEITG_INTEGRATION.sdl_raw_hcp_osea_cycle_plan_detail;
CREATE OR REPLACE TABLE HCPOSEITG_INTEGRATION.SDL_RAW_HCP_OSEA_CYCLE_PLAN_DETAIL		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE HCPOSEITG_INTEGRATION.sdl_raw_hcp_osea_cycle_plan_detail
(
	cycle_plan_detail_source_id VARCHAR(18)  		--//  ENCODE zstd
	,is_deleted VARCHAR(5)  		--//  ENCODE zstd
	,cycle_plan_detail_name VARCHAR(80)  		--//  ENCODE zstd
	,created_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,created_by_id VARCHAR(18)  		--//  ENCODE zstd
	,last_modified_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,last_modified_by_id VARCHAR(18)  		--//  ENCODE zstd
	,is_locked VARCHAR(5)  		--//  ENCODE zstd
	,cycle_plan_target_source_id VARCHAR(18)  		--//  ENCODE zstd
	,actual_details NUMERIC(3,0)  		--//  ENCODE delta
	,attainment NUMERIC(18,0)  		--//  ENCODE delta
	,planned_details NUMERIC(3,0)  		--//  ENCODE delta
	,product_source_id VARCHAR(18)  		--//  ENCODE zstd
	,scheduled_details NUMERIC(3,0)  		--//  ENCODE delta
	,total_actual_details NUMERIC(5,0)  		--//  ENCODE delta
	,total_attainment NUMERIC(18,0)  		--//  ENCODE delta
	,total_planned_details NUMERIC(5,0)  		--//  ENCODE delta
	,total_scheduled_details NUMERIC(5,0)  		--//  ENCODE delta
	,remaining NUMERIC(18,0)  		--//  ENCODE delta
	,total_remaining NUMERIC(18,0)  		--//  ENCODE delta
	,classification_type VARCHAR(255)  		--//  ENCODE zstd
	,cfa_100 NUMERIC(18,0)  		--//  ENCODE delta
	,cfa_33 NUMERIC(18,0)  		--//  ENCODE delta
	,cfa_66 NUMERIC(18,0)  		--//  ENCODE delta
	,adoption_style VARCHAR(255)  		--//  ENCODE zstd
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
	,filename VARCHAR(100)  		--//  ENCODE zstd
	,run_id VARCHAR(14)  		--//  ENCODE zstd
)

;
--DROP TABLE HCPOSEITG_INTEGRATION.sdl_raw_hcp_osea_cycle_plan_target;
CREATE OR REPLACE TABLE HCPOSEITG_INTEGRATION.SDL_RAW_HCP_OSEA_CYCLE_PLAN_TARGET		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE HCPOSEITG_INTEGRATION.sdl_raw_hcp_osea_cycle_plan_target
(
	cycle_plan_target_source_id VARCHAR(18)  		--//  ENCODE zstd
	,is_deleted VARCHAR(5)  		--//  ENCODE zstd
	,cycle_plan_target_name VARCHAR(80)  		--//  ENCODE zstd
	,created_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,created_by_id VARCHAR(18)  		--//  ENCODE zstd
	,last_modified_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,last_modified_by_id VARCHAR(18)  		--//  ENCODE zstd
	,is_locked VARCHAR(5)  		--//  ENCODE zstd
	,cycle_plan_vod_source_id VARCHAR(18)  		--//  ENCODE zstd
	,actual_calls NUMERIC(3,0)  		--//  ENCODE delta
	,attainment NUMERIC(18,0)  		--//  ENCODE delta
	,cycle_plan_account_source_id VARCHAR(18)  		--//  ENCODE zstd
	,original_planned_calls NUMERIC(3,0)  		--//  ENCODE delta
	,planned_calls NUMERIC(3,0)  		--//  ENCODE delta
	,total_actual_calls NUMERIC(5,0)  		--//  ENCODE delta
	,total_attainment NUMERIC(18,0)  		--//  ENCODE delta
	,total_planned_calls NUMERIC(5,0)  		--//  ENCODE delta
	,external_id VARCHAR(100)  		--//  ENCODE zstd
	,scheduled_calls NUMERIC(18,0)  		--//  ENCODE delta
	,total_scheduled_calls NUMERIC(5,0)  		--//  ENCODE delta
	,remaining NUMERIC(18,0)  		--//  ENCODE delta
	,total_remaining NUMERIC(18,0)  		--//  ENCODE delta
	,remaining_schedule NUMERIC(18,0)  		--//  ENCODE delta
	,total_remaining_schedule NUMERIC(18,0)  		--//  ENCODE delta
	,primary_parent_name VARCHAR(1300)  		--//  ENCODE zstd
	,specialty_1 VARCHAR(1300)  		--//  ENCODE zstd
	,account_source_id VARCHAR(1300)  		--//  ENCODE zstd
	,cpt_cfa_100 NUMERIC(18,0)  		--//  ENCODE delta
	,cpt_cfa_66 NUMERIC(18,0)  		--//  ENCODE delta
	,cpt_cfa_33 NUMERIC(18,0)  		--//  ENCODE delta
	,number_of_cfa_100_details NUMERIC(18,0)  		--//  ENCODE delta
	,number_of_product_details NUMERIC(18,0)  		--//  ENCODE delta
	,target_reached_flag NUMERIC(18,0)  		--//  ENCODE delta
	,jj_ac_classification__c VARCHAR(3900)  		--//  ENCODE zstd
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
	,filename VARCHAR(100)  		--//  ENCODE zstd
	,run_id VARCHAR(14)  		--//  ENCODE zstd
)

;
--DROP TABLE HCPOSEITG_INTEGRATION.sdl_raw_hcp_osea_holiday_list;
CREATE OR REPLACE TABLE HCPOSEITG_INTEGRATION.SDL_RAW_HCP_OSEA_HOLIDAY_LIST		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE HCPOSEITG_INTEGRATION.sdl_raw_hcp_osea_holiday_list
(
	country VARCHAR(5)  		--//  ENCODE lzo
	,holiday_key VARCHAR(20)  		--//  ENCODE lzo
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
	,filename VARCHAR(100)  		--//  ENCODE zstd
	,run_id VARCHAR(14)  		--//  ENCODE zstd
)

;
--DROP TABLE HCPOSEITG_INTEGRATION.sdl_raw_hcp_osea_isight_licenses;
CREATE OR REPLACE TABLE HCPOSEITG_INTEGRATION.SDL_RAW_HCP_OSEA_ISIGHT_LICENSES		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE HCPOSEITG_INTEGRATION.sdl_raw_hcp_osea_isight_licenses
(
	year NUMERIC(18,0)  		--//  ENCODE delta
	,country VARCHAR(255)  		--//  ENCODE zstd
	,sector VARCHAR(255)  		--//  ENCODE zstd
	,qty NUMERIC(18,0)  		--//  ENCODE delta
	,licensetype VARCHAR(255)  		--//  ENCODE zstd
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
	,filename VARCHAR(100)  		--//  ENCODE zstd
	,run_id VARCHAR(14)  		--//  ENCODE zstd
)
		--// DISTSTYLE EVEN
;
--DROP TABLE HCPOSEITG_INTEGRATION.sdl_raw_hcp_osea_isight_sector_mapping;
CREATE OR REPLACE TABLE HCPOSEITG_INTEGRATION.SDL_RAW_HCP_OSEA_ISIGHT_SECTOR_MAPPING		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE HCPOSEITG_INTEGRATION.sdl_raw_hcp_osea_isight_sector_mapping
(
	country VARCHAR(256)  		--//  ENCODE zstd
	,company VARCHAR(256)  		--//  ENCODE zstd
	,division VARCHAR(256)  		--//  ENCODE zstd
	,sector VARCHAR(256)  		--//  ENCODE zstd
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
	,filename VARCHAR(100)  		--//  ENCODE zstd
	,run_id VARCHAR(14)  		--//  ENCODE zstd
)
		--// DISTSTYLE EVEN
;
--DROP TABLE HCPOSEITG_INTEGRATION.sdl_raw_hcp_osea_key_message;
CREATE OR REPLACE TABLE HCPOSEITG_INTEGRATION.SDL_RAW_HCP_OSEA_KEY_MESSAGE		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE HCPOSEITG_INTEGRATION.sdl_raw_hcp_osea_key_message
(
	key_message_id VARCHAR(18)  		--//  ENCODE zstd
	,key_message_ownerid VARCHAR(18)  		--//  ENCODE zstd
	,key_message_name VARCHAR(250)  		--//  ENCODE zstd
	,key_message_recordtypeid VARCHAR(18)  		--//  ENCODE zstd
	,key_message_createddate TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,key_message_createdbyid VARCHAR(18)  		--//  ENCODE zstd
	,last_modified_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,key_message_lastmodifiedbyid VARCHAR(18)  		--//  ENCODE zstd
	,key_message_systemmodstamp TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,key_message_lastvieweddate TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,key_message_lastreferenceddate TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,key_message_description VARCHAR(800)  		--//  ENCODE zstd
	,key_message_product VARCHAR(18)  		--//  ENCODE zstd
	,key_message_product_strategy VARCHAR(18)  		--//  ENCODE zstd
	,key_message_display_order NUMERIC(3,0)  		--//  ENCODE delta
	,key_message_active VARCHAR(5)  		--//  ENCODE zstd
	,key_message_category VARCHAR(255)  		--//  ENCODE zstd
	,key_message_vehicle VARCHAR(255)  		--//  ENCODE zstd
	,key_message_clm_id VARCHAR(100)  		--//  ENCODE zstd
	,key_message_custom_reaction VARCHAR(255)  		--//  ENCODE zstd
	,key_message_slide_version VARCHAR(100)  		--//  ENCODE zstd
	,key_message_language VARCHAR(255)  		--//  ENCODE zstd
	,key_message_media_file_crc VARCHAR(255)  		--//  ENCODE zstd
	,key_message_media_file_name VARCHAR(255)  		--//  ENCODE zstd
	,key_message_media_file_size NUMERIC(18,0)  		--//  ENCODE delta
	,key_message_segment VARCHAR(80)  		--//  ENCODE zstd
	,key_message_detail_group VARCHAR(18)  		--//  ENCODE zstd
	,key_message_core_content_approval_id VARCHAR(100)  		--//  ENCODE zstd
	,key_message_core_content_expiration_date DATE  		--//  ENCODE delta
	,simp_country VARCHAR(1300)  		--//  ENCODE zstd
	,key_message_vexternal_id VARCHAR(255)  		--//  ENCODE zstd
	,key_message_cdn_path VARCHAR(255)  		--//  ENCODE zstd
	,key_message_status VARCHAR(255)  		--//  ENCODE zstd
	,ap_clm_country VARCHAR(4)  		--//  ENCODE zstd
	,key_message_is_shared_resource VARCHAR(5)  		--//  ENCODE zstd
	,key_message_shared_resource VARCHAR(18)  		--//  ENCODE zstd
	,ap_country VARCHAR(4)  		--//  ENCODE zstd
	,functional_team VARCHAR(255)  		--//  ENCODE zstd
	,janssen_code VARCHAR(150)  		--//  ENCODE zstd
	,vault_document_id VARCHAR(100)  		--//  ENCODE zstd
	,key_message_group VARCHAR(4099)  		--//  ENCODE zstd
	,key_message_sub_group VARCHAR(4099)  		--//  ENCODE zstd
	,key_message_purpose VARCHAR(255)  		--//  ENCODE zstd
	,key_message_content_topic VARCHAR(255)  		--//  ENCODE zstd
	,key_message_content_sub_topic VARCHAR(255)  		--//  ENCODE zstd
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
	,filename VARCHAR(100)  		--//  ENCODE zstd
	,run_id VARCHAR(14)  		--//  ENCODE zstd
)

;
--DROP TABLE HCPOSEITG_INTEGRATION.sdl_raw_hcp_osea_product;
CREATE OR REPLACE TABLE HCPOSEITG_INTEGRATION.SDL_RAW_HCP_OSEA_PRODUCT		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE HCPOSEITG_INTEGRATION.sdl_raw_hcp_osea_product
(
	product_source_id VARCHAR(18)  		--//  ENCODE zstd
	,owner_source_id VARCHAR(18)  		--//  ENCODE zstd
	,is_deleted VARCHAR(5)  		--//  ENCODE zstd
	,product_name VARCHAR(80)  		--//  ENCODE zstd
	,record_type_source_id VARCHAR(18)  		--//  ENCODE zstd
	,created_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,created_by_id VARCHAR(18)  		--//  ENCODE zstd
	,last_modified_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,last_modified_by_id VARCHAR(18)  		--//  ENCODE zstd
	,consumer_site VARCHAR(255)  		--//  ENCODE zstd
	,product_info VARCHAR(255)  		--//  ENCODE zstd
	,therapeutic_class VARCHAR(255)  		--//  ENCODE zstd
	,parent_product_source_id VARCHAR(18)  		--//  ENCODE zstd
	,therapeutic_area VARCHAR(255)  		--//  ENCODE zstd
	,product_type VARCHAR(255)  		--//  ENCODE zstd
	,require_key_message VARCHAR(5)  		--//  ENCODE zstd
	,cost NUMERIC(14,2)  		--//  ENCODE delta
	,external_id VARCHAR(25)  		--//  ENCODE zstd
	,manufacturer VARCHAR(255)  		--//  ENCODE zstd
	,company_product VARCHAR(5)  		--//  ENCODE zstd
	,controlled_substance VARCHAR(5)  		--//  ENCODE zstd
	,description VARCHAR(255)  		--//  ENCODE zstd
	,sample_quantity_picklist VARCHAR(1000)  		--//  ENCODE zstd
	,display_order NUMERIC(5,0)  		--//  ENCODE delta
	,no_metrics VARCHAR(5)  		--//  ENCODE zstd
	,distributor VARCHAR(255)  		--//  ENCODE zstd
	,sample_quantity_bound VARCHAR(5)  		--//  ENCODE zstd
	,sample_u_m VARCHAR(255)  		--//  ENCODE zstd
	,no_details VARCHAR(5)  		--//  ENCODE zstd
	,quantity_per_case NUMERIC(10,1)  		--//  ENCODE delta
	,restricted VARCHAR(5)  		--//  ENCODE zstd
	,user_aligned VARCHAR(5)  		--//  ENCODE zstd
	,restricted_states VARCHAR(100)  		--//  ENCODE zstd
	,sort_code VARCHAR(20)  		--//  ENCODE zstd
	,no_cycle_plans VARCHAR(5)  		--//  ENCODE zstd
	,sku_id VARCHAR(25)  		--//  ENCODE zstd
	,business_unit VARCHAR(1000)  		--//  ENCODE zstd
	,franchise VARCHAR(1000)  		--//  ENCODE zstd
	,country VARCHAR(255)  		--//  ENCODE zstd
	,vexternal_id VARCHAR(120)  		--//  ENCODE zstd
	,product_identifier VARCHAR(80)  		--//  ENCODE zstd
	,biz_sub_unit VARCHAR(255)  		--//  ENCODE zstd
	,biz_unit VARCHAR(255)  		--//  ENCODE zstd
	,product_sector VARCHAR(1300)  		--//  ENCODE zstd
	,imr VARCHAR(5)  		--//  ENCODE zstd
	,detail_sub_type VARCHAR(255)  		--//  ENCODE zstd
	,shc_sector VARCHAR(255)  		--//  ENCODE zstd
	,shc_strategic_group VARCHAR(255)  		--//  ENCODE zstd
	,shc_franchise VARCHAR(255)  		--//  ENCODE zstd
	,shc_brand VARCHAR(255)  		--//  ENCODE zstd
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
	,filename VARCHAR(100)  		--//  ENCODE zstd
	,run_id VARCHAR(14)  		--//  ENCODE zstd
)

;
--DROP TABLE HCPOSEITG_INTEGRATION.sdl_raw_hcp_osea_product_metrics;
CREATE OR REPLACE TABLE HCPOSEITG_INTEGRATION.SDL_RAW_HCP_OSEA_PRODUCT_METRICS		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE HCPOSEITG_INTEGRATION.sdl_raw_hcp_osea_product_metrics
(
	product_metrics_source_id VARCHAR(18)  		--//  ENCODE zstd
	,is_deleted VARCHAR(5)  		--//  ENCODE zstd
	,pm_name VARCHAR(80)  		--//  ENCODE zstd
	,created_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,created_by_id VARCHAR(18)  		--//  ENCODE zstd
	,last_modified_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,last_modified_by_id VARCHAR(18)  		--//  ENCODE zstd
	,account_source_id VARCHAR(18)  		--//  ENCODE zstd
	,awareness VARCHAR(255)  		--//  ENCODE zstd
	,movement NUMERIC(5,2)  		--//  ENCODE delta
	,product_source_id VARCHAR(18)  		--//  ENCODE zstd
	,segment VARCHAR(255)  		--//  ENCODE zstd
	,x12_mo_trx_chg NUMERIC(5,2)  		--//  ENCODE delta
	,speaker_skills VARCHAR(255)  		--//  ENCODE zstd
	,investigator_readiness VARCHAR(255)  		--//  ENCODE zstd
	,engagements NUMERIC(4,0)  		--//  ENCODE delta
	,external_id VARCHAR(255)  		--//  ENCODE zstd
	,decile NUMERIC(18,0)  		--//  ENCODE delta
	,adoption_level VARCHAR(255)  		--//  ENCODE zstd
	,detail_group VARCHAR(18)  		--//  ENCODE zstd
	,classification_type VARCHAR(255)  		--//  ENCODE zstd
	,company NUMERIC(18,0)  		--//  ENCODE delta
	,believer_of_adherence VARCHAR(255)  		--//  ENCODE zstd
	,influence_level VARCHAR(255)  		--//  ENCODE zstd
	,intention_for_future_sust VARCHAR(255)  		--//  ENCODE zstd
	,likely_to_ini_sustenna VARCHAR(255)  		--//  ENCODE zstd
	,likely_to_switch VARCHAR(255)  		--//  ENCODE zstd
	,penetration NUMERIC(3,0)  		--//  ENCODE delta
	,potential NUMERIC(3,0)  		--//  ENCODE delta
	,prescriber VARCHAR(255)  		--//  ENCODE zstd
	,number_of_patients_month NUMERIC(3,0)  		--//  ENCODE delta
	,schizophrenia_pts NUMERIC(6,0)  		--//  ENCODE delta
	,scientific_data VARCHAR(255)  		--//  ENCODE zstd
	,type_of_setting VARCHAR(255)  		--//  ENCODE zstd
	,usagof_sustenna_upon_discharging VARCHAR(255)  		--//  ENCODE zstd
	,usage_of_branded_atyp VARCHAR(255)  		--//  ENCODE zstd
	,kol_experts VARCHAR(5)  		--//  ENCODE zstd
	,nbrofmmptsyr NUMERIC(10,0)  		--//  ENCODE delta
	,practice_type VARCHAR(255)  		--//  ENCODE zstd
	,stdguideline VARCHAR(255)  		--//  ENCODE zstd
	,perception VARCHAR(255)  		--//  ENCODE zstd
	,prescription_behavior VARCHAR(255)  		--//  ENCODE zstd
	,scientifically_driven VARCHAR(255)  		--//  ENCODE zstd
	,treatment_pattern VARCHAR(255)  		--//  ENCODE zstd
	,physician_behaviour VARCHAR(255)  		--//  ENCODE zstd
	,product_preference VARCHAR(255)  		--//  ENCODE zstd
	,specialty VARCHAR(255)  		--//  ENCODE zstd
	,company_loyalty VARCHAR(255)  		--//  ENCODE zstd
	,biologics_user VARCHAR(255)  		--//  ENCODE zstd
	,price_sensitivity VARCHAR(255)  		--//  ENCODE zstd
	,usage_of_generic VARCHAR(255)  		--//  ENCODE zstd
	,previous_tramadol_experience VARCHAR(255)  		--//  ENCODE zstd
	,interest_to_treat_disease_area VARCHAR(255)  		--//  ENCODE zstd
	,satisfaction_with_alternative_tr VARCHAR(255)  		--//  ENCODE zstd
	,patient_share VARCHAR(255)  		--//  ENCODE zstd
	,country_code VARCHAR(1300)  		--//  ENCODE zstd
	,physician_product_preference VARCHAR(255)  		--//  ENCODE zstd
	,physician_prescription VARCHAR(255)  		--//  ENCODE zstd
	,adoption_ladder VARCHAR(255)  		--//  ENCODE zstd
	,ratings_points VARCHAR(255)  		--//  ENCODE zstd
	,peer_influence VARCHAR(255)  		--//  ENCODE zstd
	,innovations VARCHAR(255)  		--//  ENCODE zstd
	,cases_loads_per_year VARCHAR(255)  		--//  ENCODE zstd
	,sales_value_per_year VARCHAR(255)  		--//  ENCODE zstd
	,support VARCHAR(255)  		--//  ENCODE zstd
	,cmd VARCHAR(255)  		--//  ENCODE zstd
	,md_asp_classification VARCHAR(255)  		--//  ENCODE zstd
	,no_of_products_used VARCHAR(255)  		--//  ENCODE zstd
	,orientation_field VARCHAR(255)  		--//  ENCODE zstd
	,uptravi_usage VARCHAR(255)  		--//  ENCODE zstd
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
	,filename VARCHAR(100)  		--//  ENCODE zstd
	,run_id VARCHAR(14)  		--//  ENCODE zstd
)

;
--DROP TABLE HCPOSEITG_INTEGRATION.sdl_raw_hcp_osea_profile;
CREATE OR REPLACE TABLE HCPOSEITG_INTEGRATION.SDL_RAW_HCP_OSEA_PROFILE		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE HCPOSEITG_INTEGRATION.sdl_raw_hcp_osea_profile
(
	profile_source_id VARCHAR(18)  		--//  ENCODE zstd
	,profile_name VARCHAR(255)  		--//  ENCODE zstd
	,type VARCHAR(40)  		--//  ENCODE zstd
	,userlicense_source_id VARCHAR(18)  		--//  ENCODE zstd
	,usertype VARCHAR(40)  		--//  ENCODE zstd
	,created_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,created_by_id VARCHAR(18)  		--//  ENCODE zstd
	,last_modified_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,last_modified_by_id VARCHAR(18)  		--//  ENCODE zstd
	,description VARCHAR(255)  		--//  ENCODE zstd
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
	,filename VARCHAR(100)  		--//  ENCODE zstd
	,run_id VARCHAR(14)  		--//  ENCODE zstd
)

;
--DROP TABLE HCPOSEITG_INTEGRATION.sdl_raw_hcp_osea_profile_rg;
CREATE OR REPLACE TABLE HCPOSEITG_INTEGRATION.SDL_RAW_HCP_OSEA_PROFILE_RG		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE HCPOSEITG_INTEGRATION.sdl_raw_hcp_osea_profile_rg
(
	profile_source_id VARCHAR(18)  		--//  ENCODE zstd
	,profile_name VARCHAR(255)  		--//  ENCODE zstd
	,userlicense_source_id VARCHAR(18)  		--//  ENCODE zstd
	,usertype VARCHAR(40)  		--//  ENCODE zstd
	,created_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,created_by_id VARCHAR(18)  		--//  ENCODE zstd
	,last_modified_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,last_modified_by_id VARCHAR(18)  		--//  ENCODE zstd
	,description VARCHAR(255)  		--//  ENCODE zstd
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
	,filename VARCHAR(100)  		--//  ENCODE zstd
	,run_id VARCHAR(14)  		--//  ENCODE zstd
)

;
--DROP TABLE HCPOSEITG_INTEGRATION.sdl_raw_hcp_osea_recordtype;
CREATE OR REPLACE TABLE HCPOSEITG_INTEGRATION.SDL_RAW_HCP_OSEA_RECORDTYPE		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE HCPOSEITG_INTEGRATION.sdl_raw_hcp_osea_recordtype
(
	record_type_source_id VARCHAR(18)  		--//  ENCODE zstd
	,record_type_name VARCHAR(255)  		--//  ENCODE zstd
	,developer_name VARCHAR(80)  		--//  ENCODE zstd
	,name_space_prefix VARCHAR(15)  		--//  ENCODE zstd
	,description VARCHAR(255)  		--//  ENCODE zstd
	,business_process_id VARCHAR(18)  		--//  ENCODE zstd
	,sobjecttype VARCHAR(40)  		--//  ENCODE zstd
	,is_active VARCHAR(5)  		--//  ENCODE zstd
	,is_person_type VARCHAR(5)  		--//  ENCODE zstd
	,created_by_id VARCHAR(18)  		--//  ENCODE zstd
	,created_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,last_modified_by_id VARCHAR(18)  		--//  ENCODE zstd
	,last_modified_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
	,filename VARCHAR(100)  		--//  ENCODE zstd
	,run_id VARCHAR(14)  		--//  ENCODE zstd
)

;
--DROP TABLE HCPOSEITG_INTEGRATION.sdl_raw_hcp_osea_recordtype_rg;
CREATE OR REPLACE TABLE HCPOSEITG_INTEGRATION.SDL_RAW_HCP_OSEA_RECORDTYPE_RG		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE HCPOSEITG_INTEGRATION.sdl_raw_hcp_osea_recordtype_rg
(
	record_type_source_id VARCHAR(18)  		--//  ENCODE zstd
	,record_type_name VARCHAR(255)  		--//  ENCODE zstd
	,developer_name VARCHAR(80)  		--//  ENCODE zstd
	,name_space_prefix VARCHAR(15)  		--//  ENCODE zstd
	,description VARCHAR(255)  		--//  ENCODE zstd
	,business_process_id VARCHAR(18)  		--//  ENCODE zstd
	,sobjecttype VARCHAR(40)  		--//  ENCODE zstd
	,is_active VARCHAR(5)  		--//  ENCODE zstd
	,created_by_id VARCHAR(18)  		--//  ENCODE zstd
	,created_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,last_modified_by_id VARCHAR(18)  		--//  ENCODE zstd
	,last_modified_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
	,filename VARCHAR(100)  		--//  ENCODE zstd
	,run_id VARCHAR(14)  		--//  ENCODE zstd
)

;
--DROP TABLE HCPOSEITG_INTEGRATION.sdl_raw_hcp_osea_remote_meeting;
CREATE OR REPLACE TABLE HCPOSEITG_INTEGRATION.SDL_RAW_HCP_OSEA_REMOTE_MEETING		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE HCPOSEITG_INTEGRATION.sdl_raw_hcp_osea_remote_meeting
(
	remote_meeting_source_id VARCHAR(18)  		--//  ENCODE lzo
	,owner_source_id VARCHAR(18)  		--//  ENCODE lzo
	,is_deleted VARCHAR(10)  		--//  ENCODE lzo
	,name VARCHAR(255)  		--//  ENCODE lzo
	,record_type_source_id VARCHAR(18)  		--//  ENCODE lzo
	,created_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,createdbyid VARCHAR(18)  		--//  ENCODE lzo
	,last_modified_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,lastmodifiedbyid VARCHAR(18)  		--//  ENCODE lzo
	,systemmodstamp TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,mayedit VARCHAR(10)  		--//  ENCODE lzo
	,islocked VARCHAR(10)  		--//  ENCODE lzo
	,meeting_id VARCHAR(18)  		--//  ENCODE lzo
	,meeting_name VARCHAR(255)  		--//  ENCODE lzo
	,mobile_source_id VARCHAR(100)  		--//  ENCODE lzo
	,scheduled_datetime TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,scheduled VARCHAR(10)  		--//  ENCODE lzo
	,meeting_password VARCHAR(20)  		--//  ENCODE lzo
	,meeting_outcome_status VARCHAR(255)  		--//  ENCODE lzo
	,jj_host_country VARCHAR(20)  		--//  ENCODE lzo
	,jj_numofattendee NUMERIC(18,1)  		--//  ENCODE az64
	,jj_owner_country VARCHAR(20)  		--//  ENCODE lzo
	,assigned_host VARCHAR(200)  		--//  ENCODE lzo
	,attendance_report_process_status VARCHAR(200)  		--//  ENCODE lzo
	,description VARCHAR(1000)  		--//  ENCODE lzo
	,latest_meeting_start_datetime TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,webinar_alternative_host_1 VARCHAR(200)  		--//  ENCODE lzo
	,webinar_alternative_host_2 VARCHAR(200)  		--//  ENCODE lzo
	,webinar_alternative_host_3 VARCHAR(200)  		--//  ENCODE lzo
	,rating_submitted VARCHAR(5)  		--//  ENCODE zstd
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
	,filename VARCHAR(100)  		--//  ENCODE zstd
	,run_id VARCHAR(14)  		--//  ENCODE zstd
)

;
--DROP TABLE HCPOSEITG_INTEGRATION.sdl_raw_hcp_osea_territory;
CREATE OR REPLACE TABLE HCPOSEITG_INTEGRATION.SDL_RAW_HCP_OSEA_TERRITORY		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE HCPOSEITG_INTEGRATION.sdl_raw_hcp_osea_territory
(
	territory_source_id VARCHAR(18)  		--//  ENCODE zstd
	,territory_name VARCHAR(80)  		--//  ENCODE zstd
	,territory_type_source_id VARCHAR(18)  		--//  ENCODE zstd
	,territory_model_source_id VARCHAR(18)  		--//  ENCODE zstd
	,parent_territory_source_id VARCHAR(18)  		--//  ENCODE zstd
	,description VARCHAR(1000)  		--//  ENCODE zstd
	,account_access_level VARCHAR(40)  		--//  ENCODE zstd
	,contact_access_level VARCHAR(40)  		--//  ENCODE zstd
	,last_modified_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,last_modified_by_id VARCHAR(18)  		--//  ENCODE zstd
	,system_mod_stamp TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,developer_name VARCHAR(80)  		--//  ENCODE zstd
	,parent_territory1_source_id VARCHAR(18)  		--//  ENCODE zstd
	,territory1_source_id VARCHAR(18)  		--//  ENCODE zstd
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
	,filename VARCHAR(100)  		--//  ENCODE zstd
	,run_id VARCHAR(14)  		--//  ENCODE zstd
)

;
--DROP TABLE HCPOSEITG_INTEGRATION.sdl_raw_hcp_osea_territory_model;
CREATE OR REPLACE TABLE HCPOSEITG_INTEGRATION.SDL_RAW_HCP_OSEA_TERRITORY_MODEL		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE HCPOSEITG_INTEGRATION.sdl_raw_hcp_osea_territory_model
(
	territory_model_source_id VARCHAR(18)  		--//  ENCODE zstd
	,is_deleted VARCHAR(5)  		--//  ENCODE zstd
	,name VARCHAR(80)  		--//  ENCODE zstd
	,created_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,created_by_id VARCHAR(18)  		--//  ENCODE zstd
	,last_modified_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,last_modified_by_id VARCHAR(18)  		--//  ENCODE zstd
	,system_mod_stamp TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,may_edit VARCHAR(5)  		--//  ENCODE zstd
	,is_locked VARCHAR(5)  		--//  ENCODE zstd
	,description VARCHAR(255)  		--//  ENCODE zstd
	,activated_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,deactivated_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,state VARCHAR(255)  		--//  ENCODE zstd
	,developer_name VARCHAR(80)  		--//  ENCODE zstd
	,last_run_rules_end_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,is_clone_source VARCHAR(5)  		--//  ENCODE zstd
	,last_opp_terr_assign_end_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
	,filename VARCHAR(100)  		--//  ENCODE zstd
	,run_id VARCHAR(14)  		--//  ENCODE zstd
)

;
--DROP TABLE HCPOSEITG_INTEGRATION.sdl_raw_hcp_osea_time_off_territory;
CREATE OR REPLACE TABLE HCPOSEITG_INTEGRATION.SDL_RAW_HCP_OSEA_TIME_OFF_TERRITORY		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE HCPOSEITG_INTEGRATION.sdl_raw_hcp_osea_time_off_territory
(
	tot_source_id VARCHAR(18)  		--//  ENCODE zstd
	,owner_source_id VARCHAR(18)  		--//  ENCODE zstd
	,is_deleted VARCHAR(5)  		--//  ENCODE zstd
	,tot_name VARCHAR(80)  		--//  ENCODE zstd
	,record_type_source_id VARCHAR(18)  		--//  ENCODE zstd
	,created_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,created_by_id VARCHAR(18)  		--//  ENCODE zstd
	,last_modified_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,last_modified_by_id VARCHAR(18)  		--//  ENCODE zstd
	,reason VARCHAR(255)  		--//  ENCODE zstd
	,territory_name VARCHAR(255)  		--//  ENCODE zstd
	,tot_date DATE  		--//  ENCODE delta
	,status_cd VARCHAR(255)  		--//  ENCODE zstd
	,time_type VARCHAR(255)  		--//  ENCODE zstd
	,working_hours_on NUMERIC(18,0)  		--//  ENCODE zstd
	,mobile_id VARCHAR(100)  		--//  ENCODE zstd
	,working_hours_off VARCHAR(255)  		--//  ENCODE zstd
	,start_time VARCHAR(255)  		--//  ENCODE zstd
	,simp_time_on_time_off VARCHAR(255)  		--//  ENCODE zstd
	,simp_frml_hours_on NUMERIC(18,0)  		--//  ENCODE zstd
	,frml_total_work_days NUMERIC(18,0)  		--//  ENCODE zstd
	,simp_frml_non_working_hours_off NUMERIC(18,0)  		--//  ENCODE zstd
	,frml_planned_work_days NUMERIC(18,0)  		--//  ENCODE zstd
	,simp_description VARCHAR(800)  		--//  ENCODE zstd
	,time_on VARCHAR(255)  		--//  ENCODE zstd
	,user_name VARCHAR(18)  		--//  ENCODE zstd
	,user_profile VARCHAR(1300)  		--//  ENCODE zstd
	,sm_reason VARCHAR(255)  		--//  ENCODE zstd
	,calculatedhours_off NUMERIC(18,0)  		--//  ENCODE zstd
	,total_time_off NUMERIC(18,0)  		--//  ENCODE zstd
	,approval_status VARCHAR(255)  		--//  ENCODE zstd
	,owners_manager_email_id VARCHAR(80)  		--//  ENCODE zstd
	,country_code VARCHAR(1300)  		--//  ENCODE zstd
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
	,filename VARCHAR(100)  		--//  ENCODE zstd
	,run_id VARCHAR(14)  		--//  ENCODE zstd
)

;
--DROP TABLE HCPOSEITG_INTEGRATION.sdl_raw_hcp_osea_user;
CREATE OR REPLACE TABLE HCPOSEITG_INTEGRATION.SDL_RAW_HCP_OSEA_USER		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE HCPOSEITG_INTEGRATION.sdl_raw_hcp_osea_user
(
	employee_source_id VARCHAR(18)  		--//  ENCODE lzo
	,user_name VARCHAR(80)  		--//  ENCODE lzo
	,employee_name VARCHAR(121)  		--//  ENCODE lzo
	,company_name VARCHAR(80)  		--//  ENCODE lzo
	,division VARCHAR(80)  		--//  ENCODE lzo
	,department VARCHAR(80)  		--//  ENCODE lzo
	,title VARCHAR(80)  		--//  ENCODE lzo
	,country VARCHAR(80)  		--//  ENCODE lzo
	,address VARCHAR(255)  		--//  ENCODE lzo
	,email VARCHAR(128)  		--//  ENCODE lzo
	,phone VARCHAR(40)  		--//  ENCODE lzo
	,mobile_phone VARCHAR(40)  		--//  ENCODE lzo
	,alias VARCHAR(8)  		--//  ENCODE lzo
	,nickname VARCHAR(40)  		--//  ENCODE lzo
	,is_active VARCHAR(5)  		--//  ENCODE lzo
	,timezonesidkey VARCHAR(40)  		--//  ENCODE lzo
	,user_role_source_id VARCHAR(18)  		--//  ENCODE lzo
	,receives_info_emails VARCHAR(5)  		--//  ENCODE lzo
	,employee_profile_id VARCHAR(18)  		--//  ENCODE lzo
	,local_employee_number VARCHAR(20)  		--//  ENCODE lzo
	,manager_source_id VARCHAR(18)  		--//  ENCODE lzo
	,last_login_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,created_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,created_by_id VARCHAR(18)  		--//  ENCODE lzo
	,last_modified_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,last_modified_by_id VARCHAR(18)  		--//  ENCODE lzo
	,federation_identifier VARCHAR(512)  		--//  ENCODE lzo
	,last_ipad_sync TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,country_code VARCHAR(255)  		--//  ENCODE lzo
	,wwid VARCHAR(9)  		--//  ENCODE lzo
	,region VARCHAR(80)  		--//  ENCODE lzo
	,profile_group_ap VARCHAR(1300)  		--//  ENCODE lzo
	,user_license VARCHAR(1300)  		--//  ENCODE lzo
	,msl_primary_responsible_ta VARCHAR(255)  		--//  ENCODE lzo
	,msl_secondary_responsible_ta VARCHAR(255)  		--//  ENCODE lzo
	,last_name VARCHAR(80)  		--//  ENCODE lzo
	,first_name VARCHAR(40)  		--//  ENCODE lzo
	,city VARCHAR(40)  		--//  ENCODE lzo
	,state VARCHAR(80)  		--//  ENCODE lzo
	,postal_code VARCHAR(20)  		--//  ENCODE lzo
	,user_type VARCHAR(40)  		--//  ENCODE lzo
	,language_local_key VARCHAR(40)  		--//  ENCODE lzo
	,last_mobile_sync TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,veeva_user_type VARCHAR(255)  		--//  ENCODE zstd
	,veeva_country_code VARCHAR(255)  		--//  ENCODE zstd
	,shc_user_franchise VARCHAR(255)  		--//  ENCODE zstd
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
	,filename VARCHAR(100)  		--//  ENCODE zstd
	,run_id VARCHAR(14)  		--//  ENCODE zstd
)

;
--DROP TABLE HCPOSEITG_INTEGRATION.sdl_raw_hcp_osea_user_rg;
CREATE OR REPLACE TABLE HCPOSEITG_INTEGRATION.SDL_RAW_HCP_OSEA_USER_RG		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE HCPOSEITG_INTEGRATION.sdl_raw_hcp_osea_user_rg
(
	employee_source_id VARCHAR(18)  		--//  ENCODE zstd
	,user_name VARCHAR(80)  		--//  ENCODE zstd
	,last_name VARCHAR(80)  		--//  ENCODE zstd
	,first_name VARCHAR(40)  		--//  ENCODE zstd
	,employee_name VARCHAR(121)  		--//  ENCODE zstd
	,company_name VARCHAR(80)  		--//  ENCODE zstd
	,division VARCHAR(80)  		--//  ENCODE zstd
	,department VARCHAR(80)  		--//  ENCODE zstd
	,title VARCHAR(80)  		--//  ENCODE zstd
	,city VARCHAR(40)  		--//  ENCODE zstd
	,state VARCHAR(80)  		--//  ENCODE zstd
	,postal_code VARCHAR(20)  		--//  ENCODE zstd
	,country VARCHAR(80)  		--//  ENCODE zstd
	,address VARCHAR(255)  		--//  ENCODE zstd
	,email VARCHAR(128)  		--//  ENCODE zstd
	,phone VARCHAR(40)  		--//  ENCODE zstd
	,mobile_phone VARCHAR(40)  		--//  ENCODE zstd
	,alias VARCHAR(8)  		--//  ENCODE zstd
	,nickname VARCHAR(40)  		--//  ENCODE zstd
	,is_active VARCHAR(5)  		--//  ENCODE zstd
	,timezonesidkey VARCHAR(40)  		--//  ENCODE zstd
	,user_role_source_id VARCHAR(18)  		--//  ENCODE zstd
	,receives_info_emails VARCHAR(5)  		--//  ENCODE zstd
	,employee_profile_id VARCHAR(18)  		--//  ENCODE zstd
	,user_type VARCHAR(40)  		--//  ENCODE zstd
	,language_local_key VARCHAR(40)  		--//  ENCODE zstd
	,local_employee_number VARCHAR(20)  		--//  ENCODE zstd
	,manager_source_id VARCHAR(18)  		--//  ENCODE zstd
	,last_login_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,created_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,created_by_id VARCHAR(18)  		--//  ENCODE zstd
	,last_modified_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,last_modified_by_id VARCHAR(18)  		--//  ENCODE zstd
	,federation_identifier VARCHAR(512)  		--//  ENCODE zstd
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
	,filename VARCHAR(100)  		--//  ENCODE zstd
	,run_id VARCHAR(14)  		--//  ENCODE zstd
)

;
--DROP TABLE HCPOSEITG_INTEGRATION.sdl_raw_hcp_osea_userterritory;
CREATE OR REPLACE TABLE HCPOSEITG_INTEGRATION.SDL_RAW_HCP_OSEA_USERTERRITORY		--// CREATE TABLE IF NOT EXISTS  // CREATE OR REPLACE TABLE HCPOSEITG_INTEGRATION.sdl_raw_hcp_osea_userterritory
(
	user_territory_source_id VARCHAR(18)  		--//  ENCODE zstd
	,user_territory_user_source_id VARCHAR(18)  		--//  ENCODE zstd
	,territory_source_id VARCHAR(18)  		--//  ENCODE zstd
	,is_active VARCHAR(5)  		--//  ENCODE zstd
	,role_in_territory VARCHAR(255)  		--//  ENCODE zstd
	,last_modified_date TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE delta
	,last_modified_by_id VARCHAR(18)  		--//  ENCODE zstd
	,crt_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE zstd
	,filename VARCHAR(100)  		--//  ENCODE zstd
	,run_id VARCHAR(14)  		--//  ENCODE zstd
)

;

