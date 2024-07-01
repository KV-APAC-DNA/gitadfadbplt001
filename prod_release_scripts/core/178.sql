--DROP TABLE ASPEDW_INTEGRATION.rpt_sfa_pm_ranked;
CREATE TABLE IF NOT EXISTS ASPEDW_INTEGRATION.RPT_SFA_PM_RANKED		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS ASPEDW_INTEGRATION.rpt_sfa_pm_ranked
(
	id numeric(38,0)		--//  ENCODE az64 // BIGINT  
	,data_type VARCHAR(20)  		--//  ENCODE lzo
	,taskid VARCHAR(255)  		--//  ENCODE lzo
	,filename VARCHAR(255)
	,path VARCHAR(255)  		--//  ENCODE lzo
	,brand VARCHAR(255)  		--//  ENCODE lzo
	,mrchr_visitdate VARCHAR(255)  		--//  ENCODE lzo
	,customername VARCHAR(255)  		--//  ENCODE lzo
	,salesgroup VARCHAR(255)  		--//  ENCODE lzo
	,storetype VARCHAR(255)  		--//  ENCODE lzo
	,dist_chnl VARCHAR(255)  		--//  ENCODE lzo
	,country VARCHAR(255)  		--//  ENCODE lzo
	,salescyclename VARCHAR(255)  		--//  ENCODE lzo
	,salescampaignname VARCHAR(255)  		--//  ENCODE lzo
	,field_label VARCHAR(255)  		--//  ENCODE lzo
	,field_code VARCHAR(255)  		--//  ENCODE lzo
	,salesperson_firstname VARCHAR(128)  		--//  ENCODE lzo
	,salesperson_lastname VARCHAR(128)  		--//  ENCODE lzo
	,customerid VARCHAR(64)  		--//  ENCODE lzo
	,remotekey VARCHAR(64)  		--//  ENCODE lzo
	,secondarytradecode VARCHAR(64)  		--//  ENCODE lzo
	,secondarytradename VARCHAR(256)  		--//  ENCODE lzo
	,rank numeric(38,0)		--//  ENCODE az64 // BIGINT  
)

cluster by (filename)
		--// SORTKEY ( 
		--// 	filename
		--// 	)
;		--// ;
--DROP TABLE ASPEDW_INTEGRATION.rpt_sfa_pm_selector_channel;
CREATE TABLE IF NOT EXISTS ASPEDW_INTEGRATION.RPT_SFA_PM_SELECTOR_CHANNEL		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS ASPEDW_INTEGRATION.rpt_sfa_pm_selector_channel
(
	country VARCHAR(255)  		--//  ENCODE lzo
	,dist_chnl VARCHAR(255)  		--//  ENCODE lzo
)

;
--DROP TABLE ASPEDW_INTEGRATION.rpt_sfa_pm_selector_country;
CREATE TABLE IF NOT EXISTS ASPEDW_INTEGRATION.RPT_SFA_PM_SELECTOR_COUNTRY		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS ASPEDW_INTEGRATION.rpt_sfa_pm_selector_country
(
	country VARCHAR(255)  		--//  ENCODE lzo
)

cluster by (country)
;
--DROP TABLE ASPEDW_INTEGRATION.rpt_sfa_pm_selector_customername;
CREATE TABLE IF NOT EXISTS ASPEDW_INTEGRATION.RPT_SFA_PM_SELECTOR_CUSTOMERNAME		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS ASPEDW_INTEGRATION.rpt_sfa_pm_selector_customername
(
	country VARCHAR(255)  		--//  ENCODE lzo
	,salescyclename VARCHAR(255)  		--//  ENCODE lzo
	,salescampaignname VARCHAR(255)  		--//  ENCODE lzo
	,customername VARCHAR(255)  		--//  ENCODE lzo
)

;
--DROP TABLE ASPEDW_INTEGRATION.rpt_sfa_pm_selector_mastertask;
CREATE TABLE IF NOT EXISTS ASPEDW_INTEGRATION.RPT_SFA_PM_SELECTOR_MASTERTASK		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS ASPEDW_INTEGRATION.rpt_sfa_pm_selector_mastertask
(
	country VARCHAR(255)  		--//  ENCODE lzo
	,salescampaignname VARCHAR(255)  		--//  ENCODE lzo
)

;
--DROP TABLE ASPEDW_INTEGRATION.rpt_sfa_pm_selector_salescampaign;
CREATE TABLE IF NOT EXISTS ASPEDW_INTEGRATION.RPT_SFA_PM_SELECTOR_SALESCAMPAIGN		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS ASPEDW_INTEGRATION.rpt_sfa_pm_selector_salescampaign
(
	country VARCHAR(255)  		--//  ENCODE lzo
	,salescyclename VARCHAR(255)  		--//  ENCODE lzo
	,salescampaignname VARCHAR(255)  		--//  ENCODE lzo
)

;
--DROP TABLE ASPEDW_INTEGRATION.rpt_sfa_pm_selector_salescycle;
CREATE TABLE IF NOT EXISTS ASPEDW_INTEGRATION.RPT_SFA_PM_SELECTOR_SALESCYCLE		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS ASPEDW_INTEGRATION.rpt_sfa_pm_selector_salescycle
(
	country VARCHAR(255)  		--//  ENCODE lzo
	,salescyclename VARCHAR(255)  		--//  ENCODE lzo
)

;

--DROP TABLE jpnitg_integration.sdl_raw_pop6_jp_display_plans;
CREATE TABLE IF NOT EXISTS JPNITG_INTEGRATION.SDL_RAW_POP6_JP_DISPLAY_PLANS		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS jpnitg_integration.sdl_raw_pop6_jp_display_plans
(
	display_plan_id VARCHAR(255)  		--//  ENCODE lzo
	,status numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,allocation_method VARCHAR(255)  		--//  ENCODE lzo
	,pop_code_or_pop_list_code VARCHAR(50)  		--//  ENCODE lzo
	,team VARCHAR(50)  		--//  ENCODE lzo
	,display_code VARCHAR(255)  		--//  ENCODE lzo
	,display_name VARCHAR(255)  		--//  ENCODE lzo
	,required_number_of_displays numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,start_date DATE  		--//  ENCODE az64
	,end_date DATE  		--//  ENCODE az64
	,product_attribute_id VARCHAR(255)  		--//  ENCODE lzo
	,product_attribute VARCHAR(200)  		--//  ENCODE lzo
	,product_attribute_value_id VARCHAR(255)  		--//  ENCODE lzo
	,product_attribute_value VARCHAR(255)  		--//  ENCODE lzo
	,comments VARCHAR(255)  		--//  ENCODE lzo
	,file_name VARCHAR(100)  		--//  ENCODE lzo
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)

;
--DROP TABLE jpnitg_integration.sdl_raw_pop6_jp_displays;
CREATE TABLE IF NOT EXISTS JPNITG_INTEGRATION.SDL_RAW_POP6_JP_DISPLAYS		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS jpnitg_integration.sdl_raw_pop6_jp_displays
(
	visit_id VARCHAR(255)  		--//  ENCODE lzo
	,display_plan_id VARCHAR(255)  		--//  ENCODE lzo
	,display_type VARCHAR(255)  		--//  ENCODE lzo
	,display_code VARCHAR(255)  		--//  ENCODE lzo
	,display_name VARCHAR(255)  		--//  ENCODE lzo
	,start_date DATE  		--//  ENCODE az64
	,end_date DATE  		--//  ENCODE az64
	,checklist_method VARCHAR(50)  		--//  ENCODE lzo
	,display_number numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,product_attribute_id VARCHAR(255)  		--//  ENCODE lzo
	,product_attribute VARCHAR(200)  		--//  ENCODE lzo
	,product_attribute_value_id VARCHAR(255)  		--//  ENCODE lzo
	,product_attribute_value VARCHAR(255)  		--//  ENCODE lzo
	,comments VARCHAR(255)  		--//  ENCODE lzo
	,field_id VARCHAR(255)  		--//  ENCODE lzo
	,field_code VARCHAR(255)  		--//  ENCODE lzo
	,field_label VARCHAR(255)  		--//  ENCODE lzo
	,field_type VARCHAR(50)  		--//  ENCODE lzo
	,dependent_on_field_id VARCHAR(255)  		--//  ENCODE lzo
	,response VARCHAR(65535)  		--//  ENCODE lzo
	,file_name VARCHAR(100)  		--//  ENCODE lzo
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)

;
--DROP TABLE jpnitg_integration.sdl_raw_pop6_jp_exclusion;
CREATE TABLE IF NOT EXISTS JPNITG_INTEGRATION.SDL_RAW_POP6_JP_EXCLUSION		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS jpnitg_integration.sdl_raw_pop6_jp_exclusion
(
	exclude_kpi VARCHAR(10)  		--//  ENCODE lzo
	,visit_date DATE  		--//  ENCODE az64
	,pop_code VARCHAR(40)  		--//  ENCODE lzo
	,country VARCHAR(10)  		--//  ENCODE lzo
	,merchandiser_userid VARCHAR(100)  		--//  ENCODE lzo
	,audit_form_name VARCHAR(500)  		--//  ENCODE lzo
	,section_name VARCHAR(500)  		--//  ENCODE lzo
	,operation_type VARCHAR(10)  		--//  ENCODE lzo
	,file_name VARCHAR(40)  		--//  ENCODE lzo
	,run_id VARCHAR(40)  		--//  ENCODE lzo
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)

;
--DROP TABLE jpnitg_integration.sdl_raw_pop6_jp_executed_visits;
CREATE TABLE IF NOT EXISTS JPNITG_INTEGRATION.SDL_RAW_POP6_JP_EXECUTED_VISITS		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS jpnitg_integration.sdl_raw_pop6_jp_executed_visits
(
	visit_id VARCHAR(255)  		--//  ENCODE lzo
	,visit_date DATE  		--//  ENCODE az64
	,check_in_datetime TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,check_out_datetime TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,popdb_id VARCHAR(255)  		--//  ENCODE lzo
	,pop_code VARCHAR(50)  		--//  ENCODE lzo
	,pop_name VARCHAR(100)  		--//  ENCODE lzo
	,address VARCHAR(150)  		--//  ENCODE lzo
	,check_in_longitude NUMERIC(18,5)  		--//  ENCODE az64
	,check_in_latitude NUMERIC(18,5)  		--//  ENCODE az64
	,check_out_longitude NUMERIC(18,5)  		--//  ENCODE az64
	,check_out_latitude NUMERIC(18,5)  		--//  ENCODE az64
	,check_in_photo VARCHAR(200)  		--//  ENCODE lzo
	,check_out_photo VARCHAR(200)  		--//  ENCODE lzo
	,username VARCHAR(50)  		--//  ENCODE lzo
	,user_full_name VARCHAR(50)  		--//  ENCODE lzo
	,superior_username VARCHAR(50)  		--//  ENCODE lzo
	,superior_name VARCHAR(50)  		--//  ENCODE lzo
	,planned_visit numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,cancelled_visit numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,cancellation_reason VARCHAR(255)  		--//  ENCODE lzo
	,cancellation_note VARCHAR(255)  		--//  ENCODE lzo
	,file_name VARCHAR(100)  		--//  ENCODE lzo
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)

;
--DROP TABLE jpnitg_integration.sdl_raw_pop6_jp_general_audits;
CREATE TABLE IF NOT EXISTS JPNITG_INTEGRATION.SDL_RAW_POP6_JP_GENERAL_AUDITS		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS jpnitg_integration.sdl_raw_pop6_jp_general_audits
(
	visit_id VARCHAR(255)  		--//  ENCODE lzo
	,audit_form_id VARCHAR(255)  		--//  ENCODE lzo
	,audit_form VARCHAR(255)  		--//  ENCODE lzo
	,section_id VARCHAR(255)  		--//  ENCODE lzo
	,section VARCHAR(255)  		--//  ENCODE lzo
	,subsection_id VARCHAR(255)  		--//  ENCODE lzo
	,subsection VARCHAR(255)  		--//  ENCODE lzo
	,field_id VARCHAR(255)  		--//  ENCODE lzo
	,field_code VARCHAR(255)  		--//  ENCODE lzo
	,field_label VARCHAR(255)  		--//  ENCODE lzo
	,field_type VARCHAR(50)  		--//  ENCODE lzo
	,dependent_on_field_id VARCHAR(255)  		--//  ENCODE lzo
	,response VARCHAR(65535)  		--//  ENCODE lzo
	,file_name VARCHAR(100)  		--//  ENCODE lzo
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)

;
--DROP TABLE jpnitg_integration.sdl_raw_pop6_jp_planned_visits;
CREATE TABLE IF NOT EXISTS JPNITG_INTEGRATION.SDL_RAW_POP6_JP_PLANNED_VISITS		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS jpnitg_integration.sdl_raw_pop6_jp_planned_visits
(
	planned_visit_date DATE  		--//  ENCODE az64
	,popdb_id VARCHAR(255)  		--//  ENCODE lzo
	,pop_code VARCHAR(50)  		--//  ENCODE lzo
	,pop_name VARCHAR(100)  		--//  ENCODE lzo
	,address VARCHAR(150)  		--//  ENCODE lzo
	,username VARCHAR(50)  		--//  ENCODE lzo
	,user_full_name VARCHAR(50)  		--//  ENCODE lzo
	,file_name VARCHAR(100)  		--//  ENCODE lzo
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)

;
--DROP TABLE jpnitg_integration.sdl_raw_pop6_jp_product_attribute_audits;
CREATE TABLE IF NOT EXISTS JPNITG_INTEGRATION.SDL_RAW_POP6_JP_PRODUCT_ATTRIBUTE_AUDITS		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS jpnitg_integration.sdl_raw_pop6_jp_product_attribute_audits
(
	visit_id VARCHAR(255)  		--//  ENCODE lzo
	,audit_form_id VARCHAR(255)  		--//  ENCODE lzo
	,audit_form VARCHAR(255)  		--//  ENCODE lzo
	,section_id VARCHAR(255)  		--//  ENCODE lzo
	,section VARCHAR(255)  		--//  ENCODE lzo
	,field_id VARCHAR(255)  		--//  ENCODE lzo
	,field_code VARCHAR(255)  		--//  ENCODE lzo
	,field_label VARCHAR(255)  		--//  ENCODE lzo
	,field_type VARCHAR(50)  		--//  ENCODE lzo
	,dependent_on_field_id VARCHAR(255)  		--//  ENCODE lzo
	,product_attribute_id VARCHAR(255)  		--//  ENCODE lzo
	,product_attribute VARCHAR(200)  		--//  ENCODE lzo
	,product_attribute_value_id VARCHAR(255)  		--//  ENCODE lzo
	,product_attribute_value VARCHAR(200)  		--//  ENCODE lzo
	,response VARCHAR(65535)  		--//  ENCODE lzo
	,file_name VARCHAR(100)  		--//  ENCODE lzo
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)

;
--DROP TABLE jpnitg_integration.sdl_raw_pop6_jp_promotions;
CREATE TABLE IF NOT EXISTS JPNITG_INTEGRATION.SDL_RAW_POP6_JP_PROMOTIONS		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS jpnitg_integration.sdl_raw_pop6_jp_promotions
(
	visit_id VARCHAR(255)  		--//  ENCODE lzo
	,promotion_plan_id VARCHAR(255)  		--//  ENCODE lzo
	,promotion_code VARCHAR(255)  		--//  ENCODE lzo
	,promotion_name VARCHAR(255)  		--//  ENCODE lzo
	,promotion_mechanics VARCHAR(255)  		--//  ENCODE lzo
	,promotion_type VARCHAR(255)  		--//  ENCODE lzo
	,start_date DATE  		--//  ENCODE az64
	,end_date DATE  		--//  ENCODE az64
	,product_attribute_id VARCHAR(255)  		--//  ENCODE lzo
	,product_attribute VARCHAR(200)  		--//  ENCODE lzo
	,product_attribute_value_id VARCHAR(255)  		--//  ENCODE lzo
	,product_attribute_value VARCHAR(65535)  		--//  ENCODE lzo
	,promotion_price NUMERIC(18,2)  		--//  ENCODE az64
	,promotion_compliance VARCHAR(10)  		--//  ENCODE lzo
	,actual_price NUMERIC(18,2)  		--//  ENCODE az64
	,non_compliance_reason VARCHAR(255)  		--//  ENCODE lzo
	,photo VARCHAR(65535)  		--//  ENCODE lzo
	,file_name VARCHAR(100)  		--//  ENCODE lzo
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)

;
--DROP TABLE jpnitg_integration.sdl_raw_pop6_jp_service_levels;
CREATE TABLE IF NOT EXISTS JPNITG_INTEGRATION.SDL_RAW_POP6_JP_SERVICE_LEVELS		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS jpnitg_integration.sdl_raw_pop6_jp_service_levels
(
	sales_grp_nm VARCHAR(200)  		--//  ENCODE lzo
	,customer_grade VARCHAR(200)  		--//  ENCODE lzo
	,team VARCHAR(200)  		--//  ENCODE lzo
	,visit_frequency VARCHAR(10)  		--//  ENCODE lzo
	,estimated_visit_duration VARCHAR(150)  		--//  ENCODE lzo
	,service_level_date DATE  		--//  ENCODE az64
	,file_name VARCHAR(100)  		--//  ENCODE lzo
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)

;
--DROP TABLE jpnitg_integration.sdl_raw_pop6_jp_sku_audits;
CREATE TABLE IF NOT EXISTS JPNITG_INTEGRATION.SDL_RAW_POP6_JP_SKU_AUDITS		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS jpnitg_integration.sdl_raw_pop6_jp_sku_audits
(
	visit_id VARCHAR(255)  		--//  ENCODE lzo
	,audit_form_id VARCHAR(255)  		--//  ENCODE lzo
	,audit_form VARCHAR(255)  		--//  ENCODE lzo
	,section_id VARCHAR(255)  		--//  ENCODE lzo
	,section VARCHAR(255)  		--//  ENCODE lzo
	,field_id VARCHAR(255)  		--//  ENCODE lzo
	,field_code VARCHAR(255)  		--//  ENCODE lzo
	,field_label VARCHAR(255)  		--//  ENCODE lzo
	,field_type VARCHAR(50)  		--//  ENCODE lzo
	,dependent_on_field_id VARCHAR(255)  		--//  ENCODE lzo
	,sku_id VARCHAR(255)  		--//  ENCODE lzo
	,sku VARCHAR(255)  		--//  ENCODE lzo
	,response VARCHAR(65535)  		--//  ENCODE lzo
	,file_name VARCHAR(100)  		--//  ENCODE lzo
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)

;
--DROP TABLE jpnitg_integration.sdl_raw_pop6_jp_tasks;
CREATE TABLE IF NOT EXISTS JPNITG_INTEGRATION.SDL_RAW_POP6_JP_TASKS		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS jpnitg_integration.sdl_raw_pop6_jp_tasks
(
	visit_id VARCHAR(255)  		--//  ENCODE lzo
	,task_group VARCHAR(50)  		--//  ENCODE lzo
	,task_id numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,task_name VARCHAR(50)  		--//  ENCODE lzo
	,field_id VARCHAR(255)  		--//  ENCODE lzo
	,field_code VARCHAR(255)  		--//  ENCODE lzo
	,field_label VARCHAR(255)  		--//  ENCODE lzo
	,field_type VARCHAR(50)  		--//  ENCODE lzo
	,dependent_on_field_id VARCHAR(255)  		--//  ENCODE lzo
	,response VARCHAR(65535)  		--//  ENCODE lzo
	,file_name VARCHAR(100)  		--//  ENCODE lzo
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)

;

--DROP TABLE sgpitg_integration.sdl_raw_pop6_sg_display_plans;
CREATE TABLE IF NOT EXISTS SGPITG_INTEGRATION.SDL_RAW_POP6_SG_DISPLAY_PLANS		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS sgpitg_integration.sdl_raw_pop6_sg_display_plans
(
	display_plan_id VARCHAR(255)  		--//  ENCODE lzo
	,status numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,allocation_method VARCHAR(255)  		--//  ENCODE lzo
	,pop_code_or_pop_list_code VARCHAR(50)  		--//  ENCODE lzo
	,team VARCHAR(50)  		--//  ENCODE lzo
	,display_code VARCHAR(255)  		--//  ENCODE lzo
	,display_name VARCHAR(255)  		--//  ENCODE lzo
	,required_number_of_displays numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,start_date DATE  		--//  ENCODE az64
	,end_date DATE  		--//  ENCODE az64
	,product_attribute_id VARCHAR(255)  		--//  ENCODE lzo
	,product_attribute VARCHAR(200)  		--//  ENCODE lzo
	,product_attribute_value_id VARCHAR(255)  		--//  ENCODE lzo
	,product_attribute_value VARCHAR(255)  		--//  ENCODE lzo
	,comments VARCHAR(255)  		--//  ENCODE lzo
	,file_name VARCHAR(100)  		--//  ENCODE lzo
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)
		--// DISTSTYLE EVEN
;
--DROP TABLE sgpitg_integration.sdl_raw_pop6_sg_displays;
CREATE TABLE IF NOT EXISTS SGPITG_INTEGRATION.SDL_RAW_POP6_SG_DISPLAYS		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS sgpitg_integration.sdl_raw_pop6_sg_displays
(
	visit_id VARCHAR(255)  		--//  ENCODE lzo
	,display_plan_id VARCHAR(255)  		--//  ENCODE lzo
	,display_type VARCHAR(255)  		--//  ENCODE lzo
	,display_code VARCHAR(255)  		--//  ENCODE lzo
	,display_name VARCHAR(255)  		--//  ENCODE lzo
	,start_date DATE  		--//  ENCODE az64
	,end_date DATE  		--//  ENCODE az64
	,checklist_method VARCHAR(50)  		--//  ENCODE lzo
	,display_number numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,product_attribute_id VARCHAR(255)  		--//  ENCODE lzo
	,product_attribute VARCHAR(200)  		--//  ENCODE lzo
	,product_attribute_value_id VARCHAR(255)  		--//  ENCODE lzo
	,product_attribute_value VARCHAR(255)  		--//  ENCODE lzo
	,comments VARCHAR(255)  		--//  ENCODE lzo
	,field_id VARCHAR(255)  		--//  ENCODE lzo
	,field_code VARCHAR(255)  		--//  ENCODE lzo
	,field_label VARCHAR(255)  		--//  ENCODE lzo
	,field_type VARCHAR(50)  		--//  ENCODE lzo
	,dependent_on_field_id VARCHAR(255)  		--//  ENCODE lzo
	,response VARCHAR(65535)  		--//  ENCODE lzo
	,file_name VARCHAR(100)  		--//  ENCODE lzo
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)
		--// DISTSTYLE EVEN
;
--DROP TABLE sgpitg_integration.sdl_raw_pop6_sg_executed_visits;
CREATE TABLE IF NOT EXISTS SGPITG_INTEGRATION.SDL_RAW_POP6_SG_EXECUTED_VISITS		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS sgpitg_integration.sdl_raw_pop6_sg_executed_visits
(
	visit_id VARCHAR(255)  		--//  ENCODE lzo
	,visit_date DATE  		--//  ENCODE az64
	,check_in_datetime TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,check_out_datetime TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,popdb_id VARCHAR(255)  		--//  ENCODE lzo
	,pop_code VARCHAR(50)  		--//  ENCODE lzo
	,pop_name VARCHAR(100)  		--//  ENCODE lzo
	,address VARCHAR(150)  		--//  ENCODE lzo
	,check_in_longitude NUMERIC(18,5)  		--//  ENCODE az64
	,check_in_latitude NUMERIC(18,5)  		--//  ENCODE az64
	,check_out_longitude NUMERIC(18,5)  		--//  ENCODE az64
	,check_out_latitude NUMERIC(18,5)  		--//  ENCODE az64
	,check_in_photo VARCHAR(200)  		--//  ENCODE lzo
	,check_out_photo VARCHAR(200)  		--//  ENCODE lzo
	,username VARCHAR(50)  		--//  ENCODE lzo
	,user_full_name VARCHAR(50)  		--//  ENCODE lzo
	,superior_username VARCHAR(50)  		--//  ENCODE lzo
	,superior_name VARCHAR(50)  		--//  ENCODE lzo
	,planned_visit numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,cancelled_visit numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,cancellation_reason VARCHAR(255)  		--//  ENCODE lzo
	,cancellation_note VARCHAR(255)  		--//  ENCODE lzo
	,file_name VARCHAR(100)  		--//  ENCODE lzo
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)
		--// DISTSTYLE EVEN
;
--DROP TABLE sgpitg_integration.sdl_raw_pop6_sg_general_audits;
CREATE TABLE IF NOT EXISTS SGPITG_INTEGRATION.SDL_RAW_POP6_SG_GENERAL_AUDITS		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS sgpitg_integration.sdl_raw_pop6_sg_general_audits
(
	visit_id VARCHAR(255)  		--//  ENCODE lzo
	,audit_form_id VARCHAR(255)  		--//  ENCODE lzo
	,audit_form VARCHAR(255)  		--//  ENCODE lzo
	,section_id VARCHAR(255)  		--//  ENCODE lzo
	,section VARCHAR(255)  		--//  ENCODE lzo
	,subsection_id VARCHAR(255)  		--//  ENCODE lzo
	,subsection VARCHAR(255)  		--//  ENCODE lzo
	,field_id VARCHAR(255)  		--//  ENCODE lzo
	,field_code VARCHAR(255)  		--//  ENCODE lzo
	,field_label VARCHAR(255)  		--//  ENCODE lzo
	,field_type VARCHAR(50)  		--//  ENCODE lzo
	,dependent_on_field_id VARCHAR(255)  		--//  ENCODE lzo
	,response VARCHAR(65535)  		--//  ENCODE lzo
	,file_name VARCHAR(100)  		--//  ENCODE lzo
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)
		--// DISTSTYLE EVEN
;
--DROP TABLE sgpitg_integration.sdl_raw_pop6_sg_planned_visits;
CREATE TABLE IF NOT EXISTS SGPITG_INTEGRATION.SDL_RAW_POP6_SG_PLANNED_VISITS		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS sgpitg_integration.sdl_raw_pop6_sg_planned_visits
(
	planned_visit_date DATE  		--//  ENCODE az64
	,popdb_id VARCHAR(255)  		--//  ENCODE lzo
	,pop_code VARCHAR(50)  		--//  ENCODE lzo
	,pop_name VARCHAR(100)  		--//  ENCODE lzo
	,address VARCHAR(150)  		--//  ENCODE lzo
	,username VARCHAR(50)  		--//  ENCODE lzo
	,user_full_name VARCHAR(50)  		--//  ENCODE lzo
	,file_name VARCHAR(100)  		--//  ENCODE lzo
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)
		--// DISTSTYLE EVEN
;
--DROP TABLE sgpitg_integration.sdl_raw_pop6_sg_product_attribute_audits;
CREATE TABLE IF NOT EXISTS SGPITG_INTEGRATION.SDL_RAW_POP6_SG_PRODUCT_ATTRIBUTE_AUDITS		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS sgpitg_integration.sdl_raw_pop6_sg_product_attribute_audits
(
	visit_id VARCHAR(255)  		--//  ENCODE lzo
	,audit_form_id VARCHAR(255)  		--//  ENCODE lzo
	,audit_form VARCHAR(255)  		--//  ENCODE lzo
	,section_id VARCHAR(255)  		--//  ENCODE lzo
	,section VARCHAR(255)  		--//  ENCODE lzo
	,field_id VARCHAR(255)  		--//  ENCODE lzo
	,field_code VARCHAR(255)  		--//  ENCODE lzo
	,field_label VARCHAR(255)  		--//  ENCODE lzo
	,field_type VARCHAR(50)  		--//  ENCODE lzo
	,dependent_on_field_id VARCHAR(255)  		--//  ENCODE lzo
	,product_attribute_id VARCHAR(255)  		--//  ENCODE lzo
	,product_attribute VARCHAR(200)  		--//  ENCODE lzo
	,product_attribute_value_id VARCHAR(255)  		--//  ENCODE lzo
	,product_attribute_value VARCHAR(200)  		--//  ENCODE lzo
	,response VARCHAR(65535)  		--//  ENCODE lzo
	,file_name VARCHAR(100)  		--//  ENCODE lzo
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)
		--// DISTSTYLE EVEN
;
--DROP TABLE sgpitg_integration.sdl_raw_pop6_sg_promotion_plans;
CREATE TABLE IF NOT EXISTS SGPITG_INTEGRATION.SDL_RAW_POP6_SG_PROMOTION_PLANS		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS sgpitg_integration.sdl_raw_pop6_sg_promotion_plans
(
	status numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,promotion_plan_id VARCHAR(255)  		--//  ENCODE lzo
	,allocation_method VARCHAR(255)  		--//  ENCODE lzo
	,pop_code_or_pop_list_code VARCHAR(50)  		--//  ENCODE lzo
	,team VARCHAR(50)  		--//  ENCODE lzo
	,promotion_code VARCHAR(255)  		--//  ENCODE lzo
	,promotion_name VARCHAR(255)  		--//  ENCODE lzo
	,promotion_mechanics VARCHAR(255)  		--//  ENCODE lzo
	,start_date DATE  		--//  ENCODE az64
	,end_date DATE  		--//  ENCODE az64
	,product_attribute_id VARCHAR(255)  		--//  ENCODE lzo
	,product_attribute VARCHAR(200)  		--//  ENCODE lzo
	,product_attribute_value_id VARCHAR(255)  		--//  ENCODE lzo
	,product_attribute_value VARCHAR(65535)  		--//  ENCODE lzo
	,promotion_price NUMERIC(18,2)  		--//  ENCODE az64
	,price_field VARCHAR(255)  		--//  ENCODE lzo
	,photo_field VARCHAR(255)  		--//  ENCODE lzo
	,reason_field VARCHAR(255)  		--//  ENCODE lzo
	,file_name VARCHAR(100)  		--//  ENCODE lzo
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)
		--// DISTSTYLE EVEN
;
--DROP TABLE sgpitg_integration.sdl_raw_pop6_sg_promotions;
CREATE TABLE IF NOT EXISTS SGPITG_INTEGRATION.SDL_RAW_POP6_SG_PROMOTIONS		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS sgpitg_integration.sdl_raw_pop6_sg_promotions
(
	visit_id VARCHAR(255)  		--//  ENCODE lzo
	,promotion_plan_id VARCHAR(255)  		--//  ENCODE lzo
	,promotion_code VARCHAR(255)  		--//  ENCODE lzo
	,promotion_name VARCHAR(255)  		--//  ENCODE lzo
	,promotion_mechanics VARCHAR(255)  		--//  ENCODE lzo
	,promotion_type VARCHAR(255)  		--//  ENCODE lzo
	,start_date DATE  		--//  ENCODE az64
	,end_date DATE  		--//  ENCODE az64
	,product_attribute_id VARCHAR(255)  		--//  ENCODE lzo
	,product_attribute VARCHAR(200)  		--//  ENCODE lzo
	,product_attribute_value_id VARCHAR(255)  		--//  ENCODE lzo
	,product_attribute_value VARCHAR(65535)  		--//  ENCODE lzo
	,promotion_price NUMERIC(18,2)  		--//  ENCODE az64
	,promotion_compliance VARCHAR(10)  		--//  ENCODE lzo
	,actual_price NUMERIC(18,2)  		--//  ENCODE az64
	,non_compliance_reason VARCHAR(255)  		--//  ENCODE lzo
	,photo VARCHAR(65535)  		--//  ENCODE lzo
	,file_name VARCHAR(100)  		--//  ENCODE lzo
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)
		--// DISTSTYLE EVEN
;
--DROP TABLE sgpitg_integration.sdl_raw_pop6_sg_service_levels;
CREATE TABLE IF NOT EXISTS SGPITG_INTEGRATION.SDL_RAW_POP6_SG_SERVICE_LEVELS		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS sgpitg_integration.sdl_raw_pop6_sg_service_levels
(
	sales_grp_nm VARCHAR(200)  		--//  ENCODE lzo
	,customer_grade VARCHAR(200)  		--//  ENCODE lzo
	,team VARCHAR(200)  		--//  ENCODE lzo
	,visit_frequency VARCHAR(10)  		--//  ENCODE lzo
	,estimated_visit_duration VARCHAR(150)  		--//  ENCODE lzo
	,service_level_date DATE  		--//  ENCODE az64
	,file_name VARCHAR(100)  		--//  ENCODE lzo
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)
		--// DISTSTYLE EVEN
;
--DROP TABLE sgpitg_integration.sdl_raw_pop6_sg_sku_audits;
CREATE TABLE IF NOT EXISTS SGPITG_INTEGRATION.SDL_RAW_POP6_SG_SKU_AUDITS		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS sgpitg_integration.sdl_raw_pop6_sg_sku_audits
(
	visit_id VARCHAR(255)  		--//  ENCODE lzo
	,audit_form_id VARCHAR(255)  		--//  ENCODE lzo
	,audit_form VARCHAR(255)  		--//  ENCODE lzo
	,section_id VARCHAR(255)  		--//  ENCODE lzo
	,section VARCHAR(255)  		--//  ENCODE lzo
	,field_id VARCHAR(255)  		--//  ENCODE lzo
	,field_code VARCHAR(255)  		--//  ENCODE lzo
	,field_label VARCHAR(255)  		--//  ENCODE lzo
	,field_type VARCHAR(50)  		--//  ENCODE lzo
	,dependent_on_field_id VARCHAR(255)  		--//  ENCODE lzo
	,sku_id VARCHAR(255)  		--//  ENCODE lzo
	,sku VARCHAR(255)  		--//  ENCODE lzo
	,response VARCHAR(65535)  		--//  ENCODE lzo
	,file_name VARCHAR(100)  		--//  ENCODE lzo
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)
		--// DISTSTYLE EVEN
;
--DROP TABLE sgpitg_integration.sdl_raw_pop6_sg_tasks;
CREATE TABLE IF NOT EXISTS SGPITG_INTEGRATION.SDL_RAW_POP6_SG_TASKS		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS sgpitg_integration.sdl_raw_pop6_sg_tasks
(
	visit_id VARCHAR(255)  		--//  ENCODE lzo
	,task_group VARCHAR(50)  		--//  ENCODE lzo
	,task_id numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,task_name VARCHAR(50)  		--//  ENCODE lzo
	,field_id VARCHAR(255)  		--//  ENCODE lzo
	,field_code VARCHAR(255)  		--//  ENCODE lzo
	,field_label VARCHAR(255)  		--//  ENCODE lzo
	,field_type VARCHAR(50)  		--//  ENCODE lzo
	,dependent_on_field_id VARCHAR(255)  		--//  ENCODE lzo
	,response VARCHAR(65535)  		--//  ENCODE lzo
	,file_name VARCHAR(100)  		--//  ENCODE lzo
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)
		--// DISTSTYLE EVEN
;

--DROP TABLE thaitg_integration.sdl_raw_pop6_th_display_plans;
CREATE TABLE IF NOT EXISTS THAITG_INTEGRATION.SDL_RAW_POP6_TH_DISPLAY_PLANS		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS thaitg_integration.sdl_raw_pop6_th_display_plans
(
	display_plan_id VARCHAR(255)  		--//  ENCODE zstd
	,status numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,allocation_method VARCHAR(255)  		--//  ENCODE zstd
	,pop_code_or_pop_list_code VARCHAR(50)  		--//  ENCODE zstd
	,team VARCHAR(50)  		--//  ENCODE zstd
	,display_code VARCHAR(255)  		--//  ENCODE zstd
	,display_name VARCHAR(255)  		--//  ENCODE zstd
	,required_number_of_displays numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,start_date DATE  		--//  ENCODE az64
	,end_date DATE  		--//  ENCODE az64
	,product_attribute_id VARCHAR(255)  		--//  ENCODE zstd
	,product_attribute VARCHAR(200)  		--//  ENCODE zstd
	,product_attribute_value_id VARCHAR(255)  		--//  ENCODE zstd
	,product_attribute_value VARCHAR(255)  		--//  ENCODE zstd
	,comments VARCHAR(255)  		--//  ENCODE zstd
	,file_name VARCHAR(100)  		--//  ENCODE zstd
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)
		--// DISTSTYLE EVEN
;
--DROP TABLE thaitg_integration.sdl_raw_pop6_th_displays;
CREATE TABLE IF NOT EXISTS THAITG_INTEGRATION.SDL_RAW_POP6_TH_DISPLAYS		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS thaitg_integration.sdl_raw_pop6_th_displays
(
	visit_id VARCHAR(255)  		--//  ENCODE zstd
	,display_plan_id VARCHAR(255)  		--//  ENCODE zstd
	,display_type VARCHAR(255)  		--//  ENCODE zstd
	,display_code VARCHAR(255)  		--//  ENCODE zstd
	,display_name VARCHAR(255)  		--//  ENCODE zstd
	,start_date DATE  		--//  ENCODE az64
	,end_date DATE  		--//  ENCODE az64
	,checklist_method VARCHAR(50)  		--//  ENCODE zstd
	,display_number numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,product_attribute_id VARCHAR(255)  		--//  ENCODE zstd
	,product_attribute VARCHAR(200)  		--//  ENCODE zstd
	,product_attribute_value_id VARCHAR(255)  		--//  ENCODE zstd
	,product_attribute_value VARCHAR(255)  		--//  ENCODE zstd
	,comments VARCHAR(255)  		--//  ENCODE zstd
	,field_id VARCHAR(255)  		--//  ENCODE zstd
	,field_code VARCHAR(255)  		--//  ENCODE zstd
	,field_label VARCHAR(255)  		--//  ENCODE zstd
	,field_type VARCHAR(50)  		--//  ENCODE zstd
	,dependent_on_field_id VARCHAR(255)  		--//  ENCODE zstd
	,response VARCHAR(65535)  		--//  ENCODE zstd
	,file_name VARCHAR(100)  		--//  ENCODE zstd
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)
		--// DISTSTYLE EVEN
;
--DROP TABLE thaitg_integration.sdl_raw_pop6_th_executed_visits;
CREATE TABLE IF NOT EXISTS THAITG_INTEGRATION.SDL_RAW_POP6_TH_EXECUTED_VISITS		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS thaitg_integration.sdl_raw_pop6_th_executed_visits
(
	visit_id VARCHAR(255)  		--//  ENCODE zstd
	,visit_date DATE  		--//  ENCODE az64
	,check_in_datetime TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,check_out_datetime TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,popdb_id VARCHAR(255)  		--//  ENCODE zstd
	,pop_code VARCHAR(50)  		--//  ENCODE zstd
	,pop_name VARCHAR(100)  		--//  ENCODE zstd
	,address VARCHAR(150)  		--//  ENCODE zstd
	,check_in_longitude NUMERIC(18,5)  		--//  ENCODE az64
	,check_in_latitude NUMERIC(18,5)  		--//  ENCODE az64
	,check_out_longitude NUMERIC(18,5)  		--//  ENCODE az64
	,check_out_latitude NUMERIC(18,5)  		--//  ENCODE az64
	,check_in_photo VARCHAR(200)  		--//  ENCODE zstd
	,check_out_photo VARCHAR(200)  		--//  ENCODE zstd
	,username VARCHAR(50)  		--//  ENCODE zstd
	,user_full_name VARCHAR(50)  		--//  ENCODE zstd
	,superior_username VARCHAR(50)  		--//  ENCODE zstd
	,superior_name VARCHAR(50)  		--//  ENCODE zstd
	,planned_visit numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,cancelled_visit numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,cancellation_reason VARCHAR(255)  		--//  ENCODE zstd
	,cancellation_note VARCHAR(255)  		--//  ENCODE zstd
	,file_name VARCHAR(100)  		--//  ENCODE zstd
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)
		--// DISTSTYLE EVEN
;
--DROP TABLE thaitg_integration.sdl_raw_pop6_th_general_audits;
CREATE TABLE IF NOT EXISTS THAITG_INTEGRATION.SDL_RAW_POP6_TH_GENERAL_AUDITS		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS thaitg_integration.sdl_raw_pop6_th_general_audits
(
	visit_id VARCHAR(255)  		--//  ENCODE zstd
	,audit_form_id VARCHAR(255)  		--//  ENCODE zstd
	,audit_form VARCHAR(255)  		--//  ENCODE zstd
	,section_id VARCHAR(255)  		--//  ENCODE zstd
	,section VARCHAR(255)  		--//  ENCODE zstd
	,subsection_id VARCHAR(255)  		--//  ENCODE zstd
	,subsection VARCHAR(255)  		--//  ENCODE zstd
	,field_id VARCHAR(255)  		--//  ENCODE zstd
	,field_code VARCHAR(255)  		--//  ENCODE zstd
	,field_label VARCHAR(255)  		--//  ENCODE zstd
	,field_type VARCHAR(50)  		--//  ENCODE zstd
	,dependent_on_field_id VARCHAR(255)  		--//  ENCODE zstd
	,response VARCHAR(65535)  		--//  ENCODE zstd
	,file_name VARCHAR(100)  		--//  ENCODE zstd
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)
		--// DISTSTYLE EVEN
;
--DROP TABLE thaitg_integration.sdl_raw_pop6_th_planned_visits;
CREATE TABLE IF NOT EXISTS THAITG_INTEGRATION.SDL_RAW_POP6_TH_PLANNED_VISITS		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS thaitg_integration.sdl_raw_pop6_th_planned_visits
(
	planned_visit_date DATE  		--//  ENCODE az64
	,popdb_id VARCHAR(255)  		--//  ENCODE zstd
	,pop_code VARCHAR(50)  		--//  ENCODE zstd
	,pop_name VARCHAR(100)  		--//  ENCODE zstd
	,address VARCHAR(150)  		--//  ENCODE zstd
	,username VARCHAR(50)  		--//  ENCODE zstd
	,user_full_name VARCHAR(50)  		--//  ENCODE zstd
	,file_name VARCHAR(100)  		--//  ENCODE zstd
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)
		--// DISTSTYLE EVEN
;
--DROP TABLE thaitg_integration.sdl_raw_pop6_th_product_attribute_audits;
CREATE TABLE IF NOT EXISTS THAITG_INTEGRATION.SDL_RAW_POP6_TH_PRODUCT_ATTRIBUTE_AUDITS		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS thaitg_integration.sdl_raw_pop6_th_product_attribute_audits
(
	visit_id VARCHAR(255)  		--//  ENCODE zstd
	,audit_form_id VARCHAR(255)  		--//  ENCODE zstd
	,audit_form VARCHAR(255)  		--//  ENCODE zstd
	,section_id VARCHAR(255)  		--//  ENCODE zstd
	,section VARCHAR(255)  		--//  ENCODE zstd
	,field_id VARCHAR(255)  		--//  ENCODE zstd
	,field_code VARCHAR(255)  		--//  ENCODE zstd
	,field_label VARCHAR(255)  		--//  ENCODE zstd
	,field_type VARCHAR(50)  		--//  ENCODE zstd
	,dependent_on_field_id VARCHAR(255)  		--//  ENCODE zstd
	,product_attribute_id VARCHAR(255)  		--//  ENCODE zstd
	,product_attribute VARCHAR(200)  		--//  ENCODE zstd
	,product_attribute_value_id VARCHAR(255)  		--//  ENCODE zstd
	,product_attribute_value VARCHAR(200)  		--//  ENCODE zstd
	,response VARCHAR(65535)  		--//  ENCODE zstd
	,file_name VARCHAR(100)  		--//  ENCODE zstd
	,run_id NUMERIC(14,0)  		--//  ENCODE zstd
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)
		--// DISTSTYLE EVEN
;
--DROP TABLE thaitg_integration.sdl_raw_pop6_th_promotion_plans;
CREATE TABLE IF NOT EXISTS THAITG_INTEGRATION.SDL_RAW_POP6_TH_PROMOTION_PLANS		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS thaitg_integration.sdl_raw_pop6_th_promotion_plans
(
	status numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,promotion_plan_id VARCHAR(255)  		--//  ENCODE zstd
	,allocation_method VARCHAR(255)  		--//  ENCODE zstd
	,pop_code_or_pop_list_code VARCHAR(50)  		--//  ENCODE zstd
	,team VARCHAR(50)  		--//  ENCODE zstd
	,promotion_code VARCHAR(255)  		--//  ENCODE zstd
	,promotion_name VARCHAR(255)  		--//  ENCODE zstd
	,promotion_mechanics VARCHAR(255)  		--//  ENCODE zstd
	,start_date DATE  		--//  ENCODE az64
	,end_date DATE  		--//  ENCODE az64
	,product_attribute_id VARCHAR(255)  		--//  ENCODE zstd
	,product_attribute VARCHAR(200)  		--//  ENCODE zstd
	,product_attribute_value_id VARCHAR(255)  		--//  ENCODE zstd
	,product_attribute_value VARCHAR(65535)  		--//  ENCODE zstd
	,promotion_price NUMERIC(18,2)  		--//  ENCODE az64
	,price_field VARCHAR(255)  		--//  ENCODE zstd
	,photo_field VARCHAR(255)  		--//  ENCODE zstd
	,reason_field VARCHAR(255)  		--//  ENCODE zstd
	,file_name VARCHAR(100)  		--//  ENCODE zstd
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)
		--// DISTSTYLE EVEN
;
--DROP TABLE thaitg_integration.sdl_raw_pop6_th_promotions;
CREATE TABLE IF NOT EXISTS THAITG_INTEGRATION.SDL_RAW_POP6_TH_PROMOTIONS		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS thaitg_integration.sdl_raw_pop6_th_promotions
(
	visit_id VARCHAR(255)  		--//  ENCODE zstd
	,promotion_plan_id VARCHAR(255)  		--//  ENCODE zstd
	,promotion_code VARCHAR(255)  		--//  ENCODE zstd
	,promotion_name VARCHAR(255)  		--//  ENCODE zstd
	,promotion_mechanics VARCHAR(255)  		--//  ENCODE zstd
	,promotion_type VARCHAR(255)  		--//  ENCODE zstd
	,start_date DATE  		--//  ENCODE az64
	,end_date DATE  		--//  ENCODE az64
	,product_attribute_id VARCHAR(255)  		--//  ENCODE zstd
	,product_attribute VARCHAR(200)  		--//  ENCODE zstd
	,product_attribute_value_id VARCHAR(255)  		--//  ENCODE zstd
	,product_attribute_value VARCHAR(65535)  		--//  ENCODE zstd
	,promotion_price NUMERIC(18,2)  		--//  ENCODE az64
	,promotion_compliance VARCHAR(10)  		--//  ENCODE zstd
	,actual_price NUMERIC(18,2)  		--//  ENCODE az64
	,non_compliance_reason VARCHAR(255)  		--//  ENCODE zstd
	,photo VARCHAR(65535)  		--//  ENCODE zstd
	,file_name VARCHAR(100)  		--//  ENCODE zstd
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)
		--// DISTSTYLE EVEN
;
--DROP TABLE thaitg_integration.sdl_raw_pop6_th_rir_data;
CREATE TABLE IF NOT EXISTS THAITG_INTEGRATION.SDL_RAW_POP6_TH_RIR_DATA		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS thaitg_integration.sdl_raw_pop6_th_rir_data
(
	visit_id VARCHAR(255)  		--//  ENCODE zstd
	,sku_id VARCHAR(255)  		--//  ENCODE zstd
	,sku VARCHAR(255)  		--//  ENCODE zstd
	,facing numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,is_eyelevel numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,file_name VARCHAR(100)  		--//  ENCODE zstd
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)
		--// DISTSTYLE EVEN
;
--DROP TABLE thaitg_integration.sdl_raw_pop6_th_service_levels;
CREATE TABLE IF NOT EXISTS THAITG_INTEGRATION.SDL_RAW_POP6_TH_SERVICE_LEVELS		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS thaitg_integration.sdl_raw_pop6_th_service_levels
(
	sales_grp_nm VARCHAR(200)  		--//  ENCODE zstd
	,customer_grade VARCHAR(200)  		--//  ENCODE zstd
	,team VARCHAR(200)  		--//  ENCODE zstd
	,visit_frequency VARCHAR(10)  		--//  ENCODE zstd
	,estimated_visit_duration VARCHAR(150)  		--//  ENCODE zstd
	,service_level_date DATE  		--//  ENCODE az64
	,file_name VARCHAR(100)  		--//  ENCODE zstd
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)
		--// DISTSTYLE EVEN
;
--DROP TABLE thaitg_integration.sdl_raw_pop6_th_sku_audits;
CREATE TABLE IF NOT EXISTS THAITG_INTEGRATION.SDL_RAW_POP6_TH_SKU_AUDITS		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS thaitg_integration.sdl_raw_pop6_th_sku_audits
(
	visit_id VARCHAR(255)  		--//  ENCODE zstd
	,audit_form_id VARCHAR(255)  		--//  ENCODE zstd
	,audit_form VARCHAR(255)  		--//  ENCODE zstd
	,section_id VARCHAR(255)  		--//  ENCODE zstd
	,section VARCHAR(255)  		--//  ENCODE zstd
	,field_id VARCHAR(255)  		--//  ENCODE zstd
	,field_code VARCHAR(255)  		--//  ENCODE zstd
	,field_label VARCHAR(255)  		--//  ENCODE zstd
	,field_type VARCHAR(50)  		--//  ENCODE zstd
	,dependent_on_field_id VARCHAR(255)  		--//  ENCODE zstd
	,sku_id VARCHAR(255)  		--//  ENCODE zstd
	,sku VARCHAR(255)  		--//  ENCODE zstd
	,response VARCHAR(65535)  		--//  ENCODE zstd
	,file_name VARCHAR(100)  		--//  ENCODE zstd
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)
		--// DISTSTYLE EVEN
;
--DROP TABLE thaitg_integration.sdl_raw_pop6_th_tasks;
CREATE TABLE IF NOT EXISTS THAITG_INTEGRATION.SDL_RAW_POP6_TH_TASKS		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS thaitg_integration.sdl_raw_pop6_th_tasks
(
	visit_id VARCHAR(255)  		--//  ENCODE zstd
	,task_group VARCHAR(50)  		--//  ENCODE zstd
	,task_id numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,task_name VARCHAR(50)  		--//  ENCODE zstd
	,field_id VARCHAR(255)  		--//  ENCODE zstd
	,field_code VARCHAR(255)  		--//  ENCODE zstd
	,field_label VARCHAR(255)  		--//  ENCODE zstd
	,field_type VARCHAR(50)  		--//  ENCODE zstd
	,dependent_on_field_id VARCHAR(255)  		--//  ENCODE zstd
	,response VARCHAR(65535)  		--//  ENCODE zstd
	,file_name VARCHAR(100)  		--//  ENCODE zstd
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)
		--// DISTSTYLE EVEN
;

--DROP TABLE JPNWKS_INTEGRATION.wks_pop6_jp_display_plans;
CREATE TABLE IF NOT EXISTS JPNWKS_INTEGRATION.WKS_POP6_JP_DISPLAY_PLANS		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS JPNWKS_INTEGRATION.wks_pop6_jp_display_plans
(
	display_plan_id VARCHAR(255)  		--//  ENCODE lzo
	,status numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,allocation_method VARCHAR(255)  		--//  ENCODE lzo
	,pop_code_or_pop_list_code VARCHAR(50)  		--//  ENCODE lzo
	,team VARCHAR(50)  		--//  ENCODE lzo
	,display_code VARCHAR(255)  		--//  ENCODE lzo
	,display_name VARCHAR(255)  		--//  ENCODE lzo
	,required_number_of_displays numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,start_date DATE  		--//  ENCODE az64
	,end_date DATE  		--//  ENCODE az64
	,product_attribute_id VARCHAR(255)  		--//  ENCODE lzo
	,product_attribute VARCHAR(200)  		--//  ENCODE lzo
	,product_attribute_value_id VARCHAR(255)  		--//  ENCODE lzo
	,product_attribute_value VARCHAR(255)  		--//  ENCODE lzo
	,comments VARCHAR(255)  		--//  ENCODE lzo
	,file_name VARCHAR(100)  		--//  ENCODE lzo
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
)

;
--DROP TABLE JPNWKS_INTEGRATION.wks_pop6_jp_displays;
CREATE TABLE IF NOT EXISTS JPNWKS_INTEGRATION.WKS_POP6_JP_DISPLAYS		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS JPNWKS_INTEGRATION.wks_pop6_jp_displays
(
	visit_id VARCHAR(255)  		--//  ENCODE lzo
	,display_plan_id VARCHAR(255)  		--//  ENCODE lzo
	,display_type VARCHAR(255)  		--//  ENCODE lzo
	,display_code VARCHAR(255)  		--//  ENCODE lzo
	,display_name VARCHAR(255)  		--//  ENCODE lzo
	,start_date DATE  		--//  ENCODE az64
	,end_date DATE  		--//  ENCODE az64
	,checklist_method VARCHAR(50)  		--//  ENCODE lzo
	,display_number numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,product_attribute_id VARCHAR(255)  		--//  ENCODE lzo
	,product_attribute VARCHAR(200)  		--//  ENCODE lzo
	,product_attribute_value_id VARCHAR(255)  		--//  ENCODE lzo
	,product_attribute_value VARCHAR(255)  		--//  ENCODE lzo
	,comments VARCHAR(255)  		--//  ENCODE lzo
	,field_id VARCHAR(255)  		--//  ENCODE lzo
	,field_code VARCHAR(255)  		--//  ENCODE lzo
	,field_label VARCHAR(255)  		--//  ENCODE lzo
	,field_type VARCHAR(50)  		--//  ENCODE lzo
	,dependent_on_field_id VARCHAR(255)  		--//  ENCODE lzo
	,response VARCHAR(65535)  		--//  ENCODE lzo
	,file_name VARCHAR(100)  		--//  ENCODE lzo
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
)

;
--DROP TABLE JPNWKS_INTEGRATION.wks_pop6_jp_exclusion;
CREATE TABLE IF NOT EXISTS JPNWKS_INTEGRATION.WKS_POP6_JP_EXCLUSION		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS JPNWKS_INTEGRATION.wks_pop6_jp_exclusion
(
	exclude_kpi VARCHAR(10)  		--//  ENCODE lzo
	,visit_date DATE  		--//  ENCODE az64
	,pop_code VARCHAR(40)  		--//  ENCODE lzo
	,country VARCHAR(10)  		--//  ENCODE lzo
	,merchandiser_userid VARCHAR(255)  		--//  ENCODE lzo
	,audit_form_name VARCHAR(500)  		--//  ENCODE lzo
	,section_name VARCHAR(500)  		--//  ENCODE lzo
	,operation_type VARCHAR(10)  		--//  ENCODE lzo
	,file_name VARCHAR(40)  		--//  ENCODE lzo
	,run_id VARCHAR(40)  		--//  ENCODE lzo
)

;
--DROP TABLE JPNWKS_INTEGRATION.wks_pop6_jp_executed_visits;
CREATE TABLE IF NOT EXISTS JPNWKS_INTEGRATION.WKS_POP6_JP_EXECUTED_VISITS		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS JPNWKS_INTEGRATION.wks_pop6_jp_executed_visits
(
	visit_id VARCHAR(255)  		--//  ENCODE lzo
	,visit_date DATE  		--//  ENCODE az64
	,check_in_datetime TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,check_out_datetime TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,popdb_id VARCHAR(255)  		--//  ENCODE lzo
	,pop_code VARCHAR(50)  		--//  ENCODE lzo
	,pop_name VARCHAR(100)  		--//  ENCODE lzo
	,address VARCHAR(150)  		--//  ENCODE lzo
	,check_in_longitude NUMERIC(18,5)  		--//  ENCODE az64
	,check_in_latitude NUMERIC(18,5)  		--//  ENCODE az64
	,check_out_longitude NUMERIC(18,5)  		--//  ENCODE az64
	,check_out_latitude NUMERIC(18,5)  		--//  ENCODE az64
	,check_in_photo VARCHAR(200)  		--//  ENCODE lzo
	,check_out_photo VARCHAR(200)  		--//  ENCODE lzo
	,username VARCHAR(50)  		--//  ENCODE lzo
	,user_full_name VARCHAR(50)  		--//  ENCODE lzo
	,superior_username VARCHAR(50)  		--//  ENCODE lzo
	,superior_name VARCHAR(50)  		--//  ENCODE lzo
	,planned_visit numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,cancelled_visit numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,cancellation_reason VARCHAR(255)  		--//  ENCODE lzo
	,cancellation_note VARCHAR(255)  		--//  ENCODE lzo
	,file_name VARCHAR(100)  		--//  ENCODE lzo
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
)

;
--DROP TABLE JPNWKS_INTEGRATION.wks_pop6_jp_general_audits;
CREATE TABLE IF NOT EXISTS JPNWKS_INTEGRATION.WKS_POP6_JP_GENERAL_AUDITS		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS JPNWKS_INTEGRATION.wks_pop6_jp_general_audits
(
	visit_id VARCHAR(255)  		--//  ENCODE lzo
	,audit_form_id VARCHAR(255)  		--//  ENCODE lzo
	,audit_form VARCHAR(255)  		--//  ENCODE lzo
	,section_id VARCHAR(255)  		--//  ENCODE lzo
	,section VARCHAR(255)  		--//  ENCODE lzo
	,subsection_id VARCHAR(255)  		--//  ENCODE lzo
	,subsection VARCHAR(255)  		--//  ENCODE lzo
	,field_id VARCHAR(255)  		--//  ENCODE lzo
	,field_code VARCHAR(255)  		--//  ENCODE lzo
	,field_label VARCHAR(255)  		--//  ENCODE lzo
	,field_type VARCHAR(50)  		--//  ENCODE lzo
	,dependent_on_field_id VARCHAR(255)  		--//  ENCODE lzo
	,response VARCHAR(65535)  		--//  ENCODE lzo
	,file_name VARCHAR(100)  		--//  ENCODE lzo
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
)

;
--DROP TABLE JPNWKS_INTEGRATION.wks_pop6_jp_planned_visits;
CREATE TABLE IF NOT EXISTS JPNWKS_INTEGRATION.WKS_POP6_JP_PLANNED_VISITS		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS JPNWKS_INTEGRATION.wks_pop6_jp_planned_visits
(
	planned_visit_date DATE  		--//  ENCODE az64
	,popdb_id VARCHAR(255)  		--//  ENCODE lzo
	,pop_code VARCHAR(50)  		--//  ENCODE lzo
	,pop_name VARCHAR(100)  		--//  ENCODE lzo
	,address VARCHAR(150)  		--//  ENCODE lzo
	,username VARCHAR(50)  		--//  ENCODE lzo
	,user_full_name VARCHAR(50)  		--//  ENCODE lzo
	,file_name VARCHAR(100)  		--//  ENCODE lzo
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
)

;
--DROP TABLE JPNWKS_INTEGRATION.wks_pop6_jp_pop_lists;
CREATE TABLE IF NOT EXISTS JPNWKS_INTEGRATION.WKS_POP6_JP_POP_LISTS		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS JPNWKS_INTEGRATION.wks_pop6_jp_pop_lists
(
	status VARCHAR(255)  		--//  ENCODE lzo
	,pop_list VARCHAR(255)  		--//  ENCODE lzo
	,popdb_id VARCHAR(255)  		--//  ENCODE lzo
	,pop_code VARCHAR(255)  		--//  ENCODE lzo
	,pop_name VARCHAR(255)  		--//  ENCODE lzo
	,date VARCHAR(255)  		--//  ENCODE lzo
	,file_name VARCHAR(255)  		--//  ENCODE lzo
	,run_id VARCHAR(255)  		--//  ENCODE lzo
)

;
--DROP TABLE JPNWKS_INTEGRATION.wks_pop6_jp_pops;
CREATE TABLE IF NOT EXISTS JPNWKS_INTEGRATION.WKS_POP6_JP_POPS		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS JPNWKS_INTEGRATION.wks_pop6_jp_pops
(
	status VARCHAR(255)  		--//  ENCODE lzo
	,popdb_id VARCHAR(255)  		--//  ENCODE lzo
	,pop_code VARCHAR(255)  		--//  ENCODE lzo
	,pop_name VARCHAR(255)  		--//  ENCODE lzo
	,address VARCHAR(255)  		--//  ENCODE lzo
	,longitude VARCHAR(255)  		--//  ENCODE lzo
	,latitude VARCHAR(255)  		--//  ENCODE lzo
	,business_units_id VARCHAR(255)  		--//  ENCODE lzo
	,business_unit_name VARCHAR(255)  		--//  ENCODE lzo
	,country VARCHAR(255)  		--//  ENCODE lzo
	,channel VARCHAR(255)  		--//  ENCODE lzo
	,retail_environment_ps VARCHAR(255)  		--//  ENCODE lzo
	,sales_group_name VARCHAR(255)  		--//  ENCODE lzo
	,customer VARCHAR(255)  		--//  ENCODE lzo
	,store_type VARCHAR(255)  		--//  ENCODE lzo
	,sales_group_code VARCHAR(255)  		--//  ENCODE lzo
	,customer_grade VARCHAR(255)  		--//  ENCODE lzo
	,territory VARCHAR(255)  		--//  ENCODE lzo
	,file_name VARCHAR(255)  		--//  ENCODE lzo
	,run_id VARCHAR(255)  		--//  ENCODE lzo
)

;
--DROP TABLE JPNWKS_INTEGRATION.wks_pop6_jp_product_attribute_audits;
CREATE TABLE IF NOT EXISTS JPNWKS_INTEGRATION.WKS_POP6_JP_PRODUCT_ATTRIBUTE_AUDITS		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS JPNWKS_INTEGRATION.wks_pop6_jp_product_attribute_audits
(
	visit_id VARCHAR(255)  		--//  ENCODE lzo
	,audit_form_id VARCHAR(255)  		--//  ENCODE lzo
	,audit_form VARCHAR(255)  		--//  ENCODE lzo
	,section_id VARCHAR(255)  		--//  ENCODE lzo
	,section VARCHAR(255)  		--//  ENCODE lzo
	,field_id VARCHAR(255)  		--//  ENCODE lzo
	,field_code VARCHAR(255)  		--//  ENCODE lzo
	,field_label VARCHAR(255)  		--//  ENCODE lzo
	,field_type VARCHAR(50)  		--//  ENCODE lzo
	,dependent_on_field_id VARCHAR(255)  		--//  ENCODE lzo
	,product_attribute_id VARCHAR(255)  		--//  ENCODE lzo
	,product_attribute VARCHAR(200)  		--//  ENCODE lzo
	,product_attribute_value_id VARCHAR(255)  		--//  ENCODE lzo
	,product_attribute_value VARCHAR(200)  		--//  ENCODE lzo
	,response VARCHAR(65535)  		--//  ENCODE lzo
	,file_name VARCHAR(100)  		--//  ENCODE lzo
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
)

;
--DROP TABLE JPNWKS_INTEGRATION.wks_pop6_jp_product_lists_allocation;
CREATE TABLE IF NOT EXISTS JPNWKS_INTEGRATION.WKS_POP6_JP_PRODUCT_LISTS_ALLOCATION		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS JPNWKS_INTEGRATION.wks_pop6_jp_product_lists_allocation
(
	product_group_status VARCHAR(255)  		--//  ENCODE lzo
	,product_group VARCHAR(255)  		--//  ENCODE lzo
	,product_list_status VARCHAR(255)  		--//  ENCODE lzo
	,product_list VARCHAR(255)  		--//  ENCODE lzo
	,product_list_code VARCHAR(255)  		--//  ENCODE lzo
	,pop_attribute_id VARCHAR(255)  		--//  ENCODE lzo
	,pop_attribute VARCHAR(255)  		--//  ENCODE lzo
	,pop_attribute_value_id VARCHAR(255)  		--//  ENCODE lzo
	,pop_attribute_value VARCHAR(255)  		--//  ENCODE lzo
	,date VARCHAR(255)  		--//  ENCODE lzo
	,file_name VARCHAR(255)  		--//  ENCODE lzo
	,run_id VARCHAR(255)  		--//  ENCODE lzo
)

;
--DROP TABLE JPNWKS_INTEGRATION.wks_pop6_jp_product_lists_pops;
CREATE TABLE IF NOT EXISTS JPNWKS_INTEGRATION.WKS_POP6_JP_PRODUCT_LISTS_POPS		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS JPNWKS_INTEGRATION.wks_pop6_jp_product_lists_pops
(
	product_list VARCHAR(255)  		--//  ENCODE lzo
	,product_list_code VARCHAR(255)  		--//  ENCODE lzo
	,popdb_id VARCHAR(255)  		--//  ENCODE lzo
	,pop_code VARCHAR(255)  		--//  ENCODE lzo
	,pop_name VARCHAR(255)  		--//  ENCODE lzo
	,date VARCHAR(255)  		--//  ENCODE lzo
	,file_name VARCHAR(255)  		--//  ENCODE lzo
	,run_id VARCHAR(255)  		--//  ENCODE lzo
)

;
--DROP TABLE JPNWKS_INTEGRATION.wks_pop6_jp_product_lists_products;
CREATE TABLE IF NOT EXISTS JPNWKS_INTEGRATION.WKS_POP6_JP_PRODUCT_LISTS_PRODUCTS		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS JPNWKS_INTEGRATION.wks_pop6_jp_product_lists_products
(
	product_list VARCHAR(255)  		--//  ENCODE lzo
	,product_list_code VARCHAR(255)  		--//  ENCODE lzo
	,productdb_id VARCHAR(255)  		--//  ENCODE lzo
	,sku VARCHAR(255)  		--//  ENCODE lzo
	,msl_ranking VARCHAR(255)  		--//  ENCODE lzo
	,date VARCHAR(255)  		--//  ENCODE lzo
	,file_name VARCHAR(255)  		--//  ENCODE lzo
	,run_id VARCHAR(255)  		--//  ENCODE lzo
)

;
--DROP TABLE JPNWKS_INTEGRATION.wks_pop6_jp_products;
CREATE TABLE IF NOT EXISTS JPNWKS_INTEGRATION.WKS_POP6_JP_PRODUCTS		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS JPNWKS_INTEGRATION.wks_pop6_jp_products
(
	status VARCHAR(255)  		--//  ENCODE lzo
	,productdb_id VARCHAR(255)  		--//  ENCODE lzo
	,barcode VARCHAR(255)  		--//  ENCODE lzo
	,sku VARCHAR(255)  		--//  ENCODE lzo
	,unit_price VARCHAR(255)  		--//  ENCODE lzo
	,display_order VARCHAR(255)  		--//  ENCODE lzo
	,launch_date VARCHAR(255)  		--//  ENCODE lzo
	,largest_uom_quantity VARCHAR(255)  		--//  ENCODE lzo
	,middle_uom_quantity VARCHAR(255)  		--//  ENCODE lzo
	,smallest_uom_quantity VARCHAR(255)  		--//  ENCODE lzo
	,sku_english VARCHAR(255)  		--//  ENCODE lzo
	,company VARCHAR(255)  		--//  ENCODE lzo
	,sku_code VARCHAR(255)  		--//  ENCODE lzo
	,ps_category VARCHAR(255)  		--//  ENCODE lzo
	,ps_segment VARCHAR(255)  		--//  ENCODE lzo
	,ps_category_segment VARCHAR(255)  		--//  ENCODE lzo
	,country_l1 VARCHAR(255)  		--//  ENCODE lzo
	,regional_franchise_l2 VARCHAR(255)  		--//  ENCODE lzo
	,franchise_l3 VARCHAR(255)  		--//  ENCODE lzo
	,brand_l4 VARCHAR(255)  		--//  ENCODE lzo
	,sub_category_l5 VARCHAR(255)  		--//  ENCODE lzo
	,platform_l6 VARCHAR(255)  		--//  ENCODE lzo
	,variance_l7 VARCHAR(255)  		--//  ENCODE lzo
	,pack_size_l8 VARCHAR(255)  		--//  ENCODE lzo
	,file_name VARCHAR(255)  		--//  ENCODE lzo
	,run_id VARCHAR(255)  		--//  ENCODE lzo
)

;
--DROP TABLE JPNWKS_INTEGRATION.wks_pop6_jp_promotion_plans;
CREATE TABLE IF NOT EXISTS JPNWKS_INTEGRATION.WKS_POP6_JP_PROMOTION_PLANS		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS JPNWKS_INTEGRATION.wks_pop6_jp_promotion_plans
(
	status numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,promotion_plan_id VARCHAR(255)  		--//  ENCODE lzo
	,allocation_method VARCHAR(255)  		--//  ENCODE lzo
	,pop_code_or_pop_list_code VARCHAR(50)  		--//  ENCODE lzo
	,team VARCHAR(50)  		--//  ENCODE lzo
	,promotion_code VARCHAR(255)  		--//  ENCODE lzo
	,promotion_name VARCHAR(255)  		--//  ENCODE lzo
	,promotion_mechanics VARCHAR(255)  		--//  ENCODE lzo
	,start_date DATE  		--//  ENCODE az64
	,end_date DATE  		--//  ENCODE az64
	,product_attribute_id VARCHAR(255)  		--//  ENCODE lzo
	,product_attribute VARCHAR(200)  		--//  ENCODE lzo
	,product_attribute_value_id VARCHAR(255)  		--//  ENCODE lzo
	,product_attribute_value VARCHAR(65535)  		--//  ENCODE lzo
	,promotion_price NUMERIC(18,2)  		--//  ENCODE az64
	,price_field VARCHAR(255)  		--//  ENCODE lzo
	,photo_field VARCHAR(255)  		--//  ENCODE lzo
	,reason_field VARCHAR(255)  		--//  ENCODE lzo
	,file_name VARCHAR(100)  		--//  ENCODE lzo
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
)

;
--DROP TABLE JPNWKS_INTEGRATION.wks_pop6_jp_promotions;
CREATE TABLE IF NOT EXISTS JPNWKS_INTEGRATION.WKS_POP6_JP_PROMOTIONS		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS JPNWKS_INTEGRATION.wks_pop6_jp_promotions
(
	visit_id VARCHAR(255)  		--//  ENCODE lzo
	,promotion_plan_id VARCHAR(255)  		--//  ENCODE lzo
	,promotion_code VARCHAR(255)  		--//  ENCODE lzo
	,promotion_name VARCHAR(255)  		--//  ENCODE lzo
	,promotion_mechanics VARCHAR(255)  		--//  ENCODE lzo
	,promotion_type VARCHAR(255)  		--//  ENCODE lzo
	,start_date DATE  		--//  ENCODE az64
	,end_date DATE  		--//  ENCODE az64
	,product_attribute_id VARCHAR(255)  		--//  ENCODE lzo
	,product_attribute VARCHAR(200)  		--//  ENCODE lzo
	,product_attribute_value_id VARCHAR(255)  		--//  ENCODE lzo
	,product_attribute_value VARCHAR(65535)  		--//  ENCODE lzo
	,promotion_price NUMERIC(18,2)  		--//  ENCODE az64
	,promotion_compliance VARCHAR(10)  		--//  ENCODE lzo
	,actual_price NUMERIC(18,2)  		--//  ENCODE az64
	,non_compliance_reason VARCHAR(255)  		--//  ENCODE lzo
	,photo VARCHAR(65535)  		--//  ENCODE lzo
	,file_name VARCHAR(100)  		--//  ENCODE lzo
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
)

;
--DROP TABLE JPNWKS_INTEGRATION.wks_pop6_jp_service_levels;
CREATE TABLE IF NOT EXISTS JPNWKS_INTEGRATION.WKS_POP6_JP_SERVICE_LEVELS		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS JPNWKS_INTEGRATION.wks_pop6_jp_service_levels
(
	sales_grp_nm VARCHAR(200)  		--//  ENCODE lzo
	,customer_grade VARCHAR(200)  		--//  ENCODE lzo
	,team VARCHAR(200)  		--//  ENCODE lzo
	,visit_frequency VARCHAR(10)  		--//  ENCODE lzo
	,estimated_visit_duration VARCHAR(150)  		--//  ENCODE lzo
	,service_level_date DATE  		--//  ENCODE az64
	,file_name VARCHAR(100)  		--//  ENCODE lzo
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
)

;
--DROP TABLE JPNWKS_INTEGRATION.wks_pop6_jp_sku_audits;
CREATE TABLE IF NOT EXISTS JPNWKS_INTEGRATION.WKS_POP6_JP_SKU_AUDITS		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS JPNWKS_INTEGRATION.wks_pop6_jp_sku_audits
(
	visit_id VARCHAR(255)  		--//  ENCODE lzo
	,audit_form_id VARCHAR(255)  		--//  ENCODE lzo
	,audit_form VARCHAR(255)  		--//  ENCODE lzo
	,section_id VARCHAR(255)  		--//  ENCODE lzo
	,section VARCHAR(255)  		--//  ENCODE lzo
	,field_id VARCHAR(255)  		--//  ENCODE lzo
	,field_code VARCHAR(255)  		--//  ENCODE lzo
	,field_label VARCHAR(255)  		--//  ENCODE lzo
	,field_type VARCHAR(50)  		--//  ENCODE lzo
	,dependent_on_field_id VARCHAR(255)  		--//  ENCODE lzo
	,sku_id VARCHAR(255)  		--//  ENCODE lzo
	,sku VARCHAR(255)  		--//  ENCODE lzo
	,response VARCHAR(65535)  		--//  ENCODE lzo
	,file_name VARCHAR(100)  		--//  ENCODE lzo
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
)

;
--DROP TABLE JPNWKS_INTEGRATION.wks_pop6_jp_tasks;
CREATE TABLE IF NOT EXISTS JPNWKS_INTEGRATION.WKS_POP6_JP_TASKS		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS JPNWKS_INTEGRATION.wks_pop6_jp_tasks
(
	visit_id VARCHAR(255)  		--//  ENCODE lzo
	,task_group VARCHAR(50)  		--//  ENCODE lzo
	,task_id numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,task_name VARCHAR(50)  		--//  ENCODE lzo
	,field_id VARCHAR(255)  		--//  ENCODE lzo
	,field_code VARCHAR(255)  		--//  ENCODE lzo
	,field_label VARCHAR(255)  		--//  ENCODE lzo
	,field_type VARCHAR(50)  		--//  ENCODE lzo
	,dependent_on_field_id VARCHAR(255)  		--//  ENCODE lzo
	,response VARCHAR(65535)  		--//  ENCODE lzo
	,file_name VARCHAR(100)  		--//  ENCODE lzo
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
)

;
--DROP TABLE JPNWKS_INTEGRATION.wks_pop6_jp_users;
CREATE TABLE IF NOT EXISTS JPNWKS_INTEGRATION.WKS_POP6_JP_USERS		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS JPNWKS_INTEGRATION.wks_pop6_jp_users
(
	status VARCHAR(255)  		--//  ENCODE lzo
	,userdb_id VARCHAR(255)  		--//  ENCODE lzo
	,username VARCHAR(255)  		--//  ENCODE lzo
	,first_name VARCHAR(255)  		--//  ENCODE lzo
	,last_name VARCHAR(255)  		--//  ENCODE lzo
	,team VARCHAR(255)  		--//  ENCODE lzo
	,superior_name VARCHAR(255)  		--//  ENCODE lzo
	,authorisation_group VARCHAR(255)  		--//  ENCODE lzo
	,email_address VARCHAR(255)  		--//  ENCODE lzo
	,longitude VARCHAR(255)  		--//  ENCODE lzo
	,latitude VARCHAR(255)  		--//  ENCODE lzo
	,business_units_id VARCHAR(255)  		--//  ENCODE lzo
	,business_unit_name VARCHAR(255)  		--//  ENCODE lzo
	,file_name VARCHAR(255)  		--//  ENCODE lzo
	,run_id VARCHAR(255)  		--//  ENCODE lzo
)

;

--DROP TABLE sgpwks_integration.wks_pop6_sg_display_plans;
CREATE TABLE IF NOT EXISTS SGPWKS_INTEGRATION.WKS_POP6_SG_DISPLAY_PLANS		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS sgpwks_integration.wks_pop6_sg_display_plans
(
	display_plan_id VARCHAR(255)  		--//  ENCODE lzo
	,status numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,allocation_method VARCHAR(255)  		--//  ENCODE lzo
	,pop_code_or_pop_list_code VARCHAR(50)  		--//  ENCODE lzo
	,team VARCHAR(50)  		--//  ENCODE lzo
	,display_code VARCHAR(255)  		--//  ENCODE lzo
	,display_name VARCHAR(255)  		--//  ENCODE lzo
	,required_number_of_displays numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,start_date DATE  		--//  ENCODE az64
	,end_date DATE  		--//  ENCODE az64
	,product_attribute_id VARCHAR(255)  		--//  ENCODE lzo
	,product_attribute VARCHAR(200)  		--//  ENCODE lzo
	,product_attribute_value_id VARCHAR(255)  		--//  ENCODE lzo
	,product_attribute_value VARCHAR(255)  		--//  ENCODE lzo
	,comments VARCHAR(255)  		--//  ENCODE lzo
	,file_name VARCHAR(100)  		--//  ENCODE lzo
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
)
		--// DISTSTYLE EVEN
;
--DROP TABLE sgpwks_integration.wks_pop6_sg_displays;
CREATE TABLE IF NOT EXISTS SGPWKS_INTEGRATION.WKS_POP6_SG_DISPLAYS		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS sgpwks_integration.wks_pop6_sg_displays
(
	visit_id VARCHAR(255)  		--//  ENCODE lzo
	,display_plan_id VARCHAR(255)  		--//  ENCODE lzo
	,display_type VARCHAR(255)  		--//  ENCODE lzo
	,display_code VARCHAR(255)  		--//  ENCODE lzo
	,display_name VARCHAR(255)  		--//  ENCODE lzo
	,start_date DATE  		--//  ENCODE az64
	,end_date DATE  		--//  ENCODE az64
	,checklist_method VARCHAR(50)  		--//  ENCODE lzo
	,display_number numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,product_attribute_id VARCHAR(255)  		--//  ENCODE lzo
	,product_attribute VARCHAR(200)  		--//  ENCODE lzo
	,product_attribute_value_id VARCHAR(255)  		--//  ENCODE lzo
	,product_attribute_value VARCHAR(255)  		--//  ENCODE lzo
	,comments VARCHAR(255)  		--//  ENCODE lzo
	,field_id VARCHAR(255)  		--//  ENCODE lzo
	,field_code VARCHAR(255)  		--//  ENCODE lzo
	,field_label VARCHAR(255)  		--//  ENCODE lzo
	,field_type VARCHAR(50)  		--//  ENCODE lzo
	,dependent_on_field_id VARCHAR(255)  		--//  ENCODE lzo
	,response VARCHAR(65535)  		--//  ENCODE lzo
	,file_name VARCHAR(100)  		--//  ENCODE lzo
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
)
		--// DISTSTYLE EVEN
;
--DROP TABLE sgpwks_integration.wks_pop6_sg_executed_visits;
CREATE TABLE IF NOT EXISTS SGPWKS_INTEGRATION.WKS_POP6_SG_EXECUTED_VISITS		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS sgpwks_integration.wks_pop6_sg_executed_visits
(
	visit_id VARCHAR(255)  		--//  ENCODE lzo
	,visit_date DATE  		--//  ENCODE az64
	,check_in_datetime TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,check_out_datetime TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,popdb_id VARCHAR(255)  		--//  ENCODE lzo
	,pop_code VARCHAR(50)  		--//  ENCODE lzo
	,pop_name VARCHAR(100)  		--//  ENCODE lzo
	,address VARCHAR(150)  		--//  ENCODE lzo
	,check_in_longitude NUMERIC(18,5)  		--//  ENCODE az64
	,check_in_latitude NUMERIC(18,5)  		--//  ENCODE az64
	,check_out_longitude NUMERIC(18,5)  		--//  ENCODE az64
	,check_out_latitude NUMERIC(18,5)  		--//  ENCODE az64
	,check_in_photo VARCHAR(200)  		--//  ENCODE lzo
	,check_out_photo VARCHAR(200)  		--//  ENCODE lzo
	,username VARCHAR(50)  		--//  ENCODE lzo
	,user_full_name VARCHAR(50)  		--//  ENCODE lzo
	,superior_username VARCHAR(50)  		--//  ENCODE lzo
	,superior_name VARCHAR(50)  		--//  ENCODE lzo
	,planned_visit numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,cancelled_visit numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,cancellation_reason VARCHAR(255)  		--//  ENCODE lzo
	,cancellation_note VARCHAR(255)  		--//  ENCODE lzo
	,file_name VARCHAR(100)  		--//  ENCODE lzo
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
)
		--// DISTSTYLE EVEN
;
--DROP TABLE sgpwks_integration.wks_pop6_sg_general_audits;
CREATE TABLE IF NOT EXISTS SGPWKS_INTEGRATION.WKS_POP6_SG_GENERAL_AUDITS		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS sgpwks_integration.wks_pop6_sg_general_audits
(
	visit_id VARCHAR(255)  		--//  ENCODE lzo
	,audit_form_id VARCHAR(255)  		--//  ENCODE lzo
	,audit_form VARCHAR(255)  		--//  ENCODE lzo
	,section_id VARCHAR(255)  		--//  ENCODE lzo
	,section VARCHAR(255)  		--//  ENCODE lzo
	,subsection_id VARCHAR(255)  		--//  ENCODE lzo
	,subsection VARCHAR(255)  		--//  ENCODE lzo
	,field_id VARCHAR(255)  		--//  ENCODE lzo
	,field_code VARCHAR(255)  		--//  ENCODE lzo
	,field_label VARCHAR(255)  		--//  ENCODE lzo
	,field_type VARCHAR(50)  		--//  ENCODE lzo
	,dependent_on_field_id VARCHAR(255)  		--//  ENCODE lzo
	,response VARCHAR(65535)  		--//  ENCODE lzo
	,file_name VARCHAR(100)  		--//  ENCODE lzo
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
)
		--// DISTSTYLE EVEN
;
--DROP TABLE sgpwks_integration.wks_pop6_sg_planned_visits;
CREATE TABLE IF NOT EXISTS SGPWKS_INTEGRATION.WKS_POP6_SG_PLANNED_VISITS		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS sgpwks_integration.wks_pop6_sg_planned_visits
(
	planned_visit_date DATE  		--//  ENCODE az64
	,popdb_id VARCHAR(255)  		--//  ENCODE lzo
	,pop_code VARCHAR(50)  		--//  ENCODE lzo
	,pop_name VARCHAR(100)  		--//  ENCODE lzo
	,address VARCHAR(150)  		--//  ENCODE lzo
	,username VARCHAR(50)  		--//  ENCODE lzo
	,user_full_name VARCHAR(50)  		--//  ENCODE lzo
	,file_name VARCHAR(100)  		--//  ENCODE lzo
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
)
		--// DISTSTYLE EVEN
;
--DROP TABLE sgpwks_integration.wks_pop6_sg_pop_lists;
CREATE TABLE IF NOT EXISTS SGPWKS_INTEGRATION.WKS_POP6_SG_POP_LISTS		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS sgpwks_integration.wks_pop6_sg_pop_lists
(
	status VARCHAR(255)  		--//  ENCODE lzo
	,pop_list VARCHAR(255)  		--//  ENCODE lzo
	,popdb_id VARCHAR(255)  		--//  ENCODE lzo
	,pop_code VARCHAR(255)  		--//  ENCODE lzo
	,pop_name VARCHAR(255)  		--//  ENCODE lzo
	,date VARCHAR(255)  		--//  ENCODE lzo
	,file_name VARCHAR(255)  		--//  ENCODE lzo
	,run_id VARCHAR(255)  		--//  ENCODE lzo
)

;
--DROP TABLE sgpwks_integration.wks_pop6_sg_pops;
CREATE TABLE IF NOT EXISTS SGPWKS_INTEGRATION.WKS_POP6_SG_POPS		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS sgpwks_integration.wks_pop6_sg_pops
(
	status VARCHAR(255)  		--//  ENCODE lzo
	,popdb_id VARCHAR(255)  		--//  ENCODE lzo
	,pop_code VARCHAR(255)  		--//  ENCODE lzo
	,pop_name VARCHAR(255)  		--//  ENCODE lzo
	,address VARCHAR(255)  		--//  ENCODE lzo
	,longitude VARCHAR(255)  		--//  ENCODE lzo
	,latitude VARCHAR(255)  		--//  ENCODE lzo
	,business_units_id VARCHAR(255)  		--//  ENCODE lzo
	,business_unit_name VARCHAR(255)  		--//  ENCODE lzo
	,country VARCHAR(255)  		--//  ENCODE lzo
	,channel VARCHAR(255)  		--//  ENCODE lzo
	,retail_environment_ps VARCHAR(255)  		--//  ENCODE lzo
	,sales_group_name VARCHAR(255)  		--//  ENCODE lzo
	,customer VARCHAR(255)  		--//  ENCODE lzo
	,store_type VARCHAR(255)  		--//  ENCODE lzo
	,sales_group_code VARCHAR(255)  		--//  ENCODE lzo
	,customer_grade VARCHAR(255)  		--//  ENCODE lzo
	,territory VARCHAR(255)  		--//  ENCODE lzo
	,file_name VARCHAR(255)  		--//  ENCODE lzo
	,run_id VARCHAR(255)  		--//  ENCODE lzo
)

;
--DROP TABLE sgpwks_integration.wks_pop6_sg_product_attribute_audits;
CREATE TABLE IF NOT EXISTS SGPWKS_INTEGRATION.WKS_POP6_SG_PRODUCT_ATTRIBUTE_AUDITS		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS sgpwks_integration.wks_pop6_sg_product_attribute_audits
(
	visit_id VARCHAR(255)  		--//  ENCODE lzo
	,audit_form_id VARCHAR(255)  		--//  ENCODE lzo
	,audit_form VARCHAR(255)  		--//  ENCODE lzo
	,section_id VARCHAR(255)  		--//  ENCODE lzo
	,section VARCHAR(255)  		--//  ENCODE lzo
	,field_id VARCHAR(255)  		--//  ENCODE lzo
	,field_code VARCHAR(255)  		--//  ENCODE lzo
	,field_label VARCHAR(255)  		--//  ENCODE lzo
	,field_type VARCHAR(50)  		--//  ENCODE lzo
	,dependent_on_field_id VARCHAR(255)  		--//  ENCODE lzo
	,product_attribute_id VARCHAR(255)  		--//  ENCODE lzo
	,product_attribute VARCHAR(200)  		--//  ENCODE lzo
	,product_attribute_value_id VARCHAR(255)  		--//  ENCODE lzo
	,product_attribute_value VARCHAR(200)  		--//  ENCODE lzo
	,response VARCHAR(65535)  		--//  ENCODE lzo
	,file_name VARCHAR(100)  		--//  ENCODE lzo
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
)
		--// DISTSTYLE EVEN
;
--DROP TABLE sgpwks_integration.wks_pop6_sg_product_lists_allocation;
CREATE TABLE IF NOT EXISTS SGPWKS_INTEGRATION.WKS_POP6_SG_PRODUCT_LISTS_ALLOCATION		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS sgpwks_integration.wks_pop6_sg_product_lists_allocation
(
	product_group_status VARCHAR(255)  		--//  ENCODE lzo
	,product_group VARCHAR(255)  		--//  ENCODE lzo
	,product_list_status VARCHAR(255)  		--//  ENCODE lzo
	,product_list VARCHAR(255)  		--//  ENCODE lzo
	,product_list_code VARCHAR(255)  		--//  ENCODE lzo
	,pop_attribute_id VARCHAR(255)  		--//  ENCODE lzo
	,pop_attribute VARCHAR(255)  		--//  ENCODE lzo
	,pop_attribute_value_id VARCHAR(255)  		--//  ENCODE lzo
	,pop_attribute_value VARCHAR(255)  		--//  ENCODE lzo
	,date VARCHAR(255)  		--//  ENCODE lzo
	,file_name VARCHAR(255)  		--//  ENCODE lzo
	,run_id VARCHAR(255)  		--//  ENCODE lzo
)

;
--DROP TABLE sgpwks_integration.wks_pop6_sg_product_lists_pops;
CREATE TABLE IF NOT EXISTS SGPWKS_INTEGRATION.WKS_POP6_SG_PRODUCT_LISTS_POPS		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS sgpwks_integration.wks_pop6_sg_product_lists_pops
(
	product_list VARCHAR(255)  		--//  ENCODE lzo
	,product_list_code VARCHAR(255)  		--//  ENCODE lzo
	,popdb_id VARCHAR(255)  		--//  ENCODE lzo
	,pop_code VARCHAR(255)  		--//  ENCODE lzo
	,pop_name VARCHAR(255)  		--//  ENCODE lzo
	,date VARCHAR(255)  		--//  ENCODE lzo
	,file_name VARCHAR(255)  		--//  ENCODE lzo
	,run_id VARCHAR(255)  		--//  ENCODE lzo
)

;
--DROP TABLE sgpwks_integration.wks_pop6_sg_product_lists_products;
CREATE TABLE IF NOT EXISTS SGPWKS_INTEGRATION.WKS_POP6_SG_PRODUCT_LISTS_PRODUCTS		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS sgpwks_integration.wks_pop6_sg_product_lists_products
(
	product_list VARCHAR(255)  		--//  ENCODE lzo
	,product_list_code VARCHAR(255)  		--//  ENCODE lzo
	,productdb_id VARCHAR(255)  		--//  ENCODE lzo
	,sku VARCHAR(255)  		--//  ENCODE lzo
	,msl_ranking VARCHAR(255)  		--//  ENCODE lzo
	,date VARCHAR(255)  		--//  ENCODE lzo
	,file_name VARCHAR(255)  		--//  ENCODE lzo
	,run_id VARCHAR(255)  		--//  ENCODE lzo
)

;
--DROP TABLE sgpwks_integration.wks_pop6_sg_products;
CREATE TABLE IF NOT EXISTS SGPWKS_INTEGRATION.WKS_POP6_SG_PRODUCTS		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS sgpwks_integration.wks_pop6_sg_products
(
	status VARCHAR(255)  		--//  ENCODE lzo
	,productdb_id VARCHAR(255)  		--//  ENCODE lzo
	,barcode VARCHAR(255)  		--//  ENCODE lzo
	,sku VARCHAR(255)  		--//  ENCODE lzo
	,unit_price VARCHAR(255)  		--//  ENCODE lzo
	,display_order VARCHAR(255)  		--//  ENCODE lzo
	,launch_date VARCHAR(255)  		--//  ENCODE lzo
	,largest_uom_quantity VARCHAR(255)  		--//  ENCODE lzo
	,middle_uom_quantity VARCHAR(255)  		--//  ENCODE lzo
	,smallest_uom_quantity VARCHAR(255)  		--//  ENCODE lzo
	,sku_english VARCHAR(255)  		--//  ENCODE lzo
	,company VARCHAR(255)  		--//  ENCODE lzo
	,sku_code VARCHAR(255)  		--//  ENCODE lzo
	,ps_category VARCHAR(255)  		--//  ENCODE lzo
	,ps_segment VARCHAR(255)  		--//  ENCODE lzo
	,ps_category_segment VARCHAR(255)  		--//  ENCODE lzo
	,country_l1 VARCHAR(255)  		--//  ENCODE lzo
	,regional_franchise_l2 VARCHAR(255)  		--//  ENCODE lzo
	,franchise_l3 VARCHAR(255)  		--//  ENCODE lzo
	,brand_l4 VARCHAR(255)  		--//  ENCODE lzo
	,sub_category_l5 VARCHAR(255)  		--//  ENCODE lzo
	,platform_l6 VARCHAR(255)  		--//  ENCODE lzo
	,variance_l7 VARCHAR(255)  		--//  ENCODE lzo
	,pack_size_l8 VARCHAR(255)  		--//  ENCODE lzo
	,file_name VARCHAR(255)  		--//  ENCODE lzo
	,run_id VARCHAR(255)  		--//  ENCODE lzo
)

;
--DROP TABLE sgpwks_integration.wks_pop6_sg_promotion_plans;
CREATE TABLE IF NOT EXISTS SGPWKS_INTEGRATION.WKS_POP6_SG_PROMOTION_PLANS		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS sgpwks_integration.wks_pop6_sg_promotion_plans
(
	status numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,promotion_plan_id VARCHAR(255)  		--//  ENCODE lzo
	,allocation_method VARCHAR(255)  		--//  ENCODE lzo
	,pop_code_or_pop_list_code VARCHAR(50)  		--//  ENCODE lzo
	,team VARCHAR(50)  		--//  ENCODE lzo
	,promotion_code VARCHAR(255)  		--//  ENCODE lzo
	,promotion_name VARCHAR(255)  		--//  ENCODE lzo
	,promotion_mechanics VARCHAR(255)  		--//  ENCODE lzo
	,start_date DATE  		--//  ENCODE az64
	,end_date DATE  		--//  ENCODE az64
	,product_attribute_id VARCHAR(255)  		--//  ENCODE lzo
	,product_attribute VARCHAR(200)  		--//  ENCODE lzo
	,product_attribute_value_id VARCHAR(255)  		--//  ENCODE lzo
	,product_attribute_value VARCHAR(65535)  		--//  ENCODE lzo
	,promotion_price NUMERIC(18,2)  		--//  ENCODE az64
	,price_field VARCHAR(255)  		--//  ENCODE lzo
	,photo_field VARCHAR(255)  		--//  ENCODE lzo
	,reason_field VARCHAR(255)  		--//  ENCODE lzo
	,file_name VARCHAR(100)  		--//  ENCODE lzo
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
)
		--// DISTSTYLE EVEN
;
--DROP TABLE sgpwks_integration.wks_pop6_sg_promotions;
CREATE TABLE IF NOT EXISTS SGPWKS_INTEGRATION.WKS_POP6_SG_PROMOTIONS		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS sgpwks_integration.wks_pop6_sg_promotions
(
	visit_id VARCHAR(255)  		--//  ENCODE lzo
	,promotion_plan_id VARCHAR(255)  		--//  ENCODE lzo
	,promotion_code VARCHAR(255)  		--//  ENCODE lzo
	,promotion_name VARCHAR(255)  		--//  ENCODE lzo
	,promotion_mechanics VARCHAR(255)  		--//  ENCODE lzo
	,promotion_type VARCHAR(255)  		--//  ENCODE lzo
	,start_date DATE  		--//  ENCODE az64
	,end_date DATE  		--//  ENCODE az64
	,product_attribute_id VARCHAR(255)  		--//  ENCODE lzo
	,product_attribute VARCHAR(200)  		--//  ENCODE lzo
	,product_attribute_value_id VARCHAR(255)  		--//  ENCODE lzo
	,product_attribute_value VARCHAR(65535)  		--//  ENCODE lzo
	,promotion_price NUMERIC(18,2)  		--//  ENCODE az64
	,promotion_compliance VARCHAR(10)  		--//  ENCODE lzo
	,actual_price NUMERIC(18,2)  		--//  ENCODE az64
	,non_compliance_reason VARCHAR(255)  		--//  ENCODE lzo
	,photo VARCHAR(65535)  		--//  ENCODE lzo
	,file_name VARCHAR(100)  		--//  ENCODE lzo
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
)
		--// DISTSTYLE EVEN
;
--DROP TABLE sgpwks_integration.wks_pop6_sg_service_levels;
CREATE TABLE IF NOT EXISTS SGPWKS_INTEGRATION.WKS_POP6_SG_SERVICE_LEVELS		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS sgpwks_integration.wks_pop6_sg_service_levels
(
	sales_grp_nm VARCHAR(200)  		--//  ENCODE lzo
	,customer_grade VARCHAR(200)  		--//  ENCODE lzo
	,team VARCHAR(200)  		--//  ENCODE lzo
	,visit_frequency VARCHAR(10)  		--//  ENCODE lzo
	,estimated_visit_duration VARCHAR(150)  		--//  ENCODE lzo
	,service_level_date DATE  		--//  ENCODE az64
	,file_name VARCHAR(100)  		--//  ENCODE lzo
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
)
		--// DISTSTYLE EVEN
;
--DROP TABLE sgpwks_integration.wks_pop6_sg_sku_audits;
CREATE TABLE IF NOT EXISTS SGPWKS_INTEGRATION.WKS_POP6_SG_SKU_AUDITS		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS sgpwks_integration.wks_pop6_sg_sku_audits
(
	visit_id VARCHAR(255)  		--//  ENCODE lzo
	,audit_form_id VARCHAR(255)  		--//  ENCODE lzo
	,audit_form VARCHAR(255)  		--//  ENCODE lzo
	,section_id VARCHAR(255)  		--//  ENCODE lzo
	,section VARCHAR(255)  		--//  ENCODE lzo
	,field_id VARCHAR(255)  		--//  ENCODE lzo
	,field_code VARCHAR(255)  		--//  ENCODE lzo
	,field_label VARCHAR(255)  		--//  ENCODE lzo
	,field_type VARCHAR(50)  		--//  ENCODE lzo
	,dependent_on_field_id VARCHAR(255)  		--//  ENCODE lzo
	,sku_id VARCHAR(255)  		--//  ENCODE lzo
	,sku VARCHAR(255)  		--//  ENCODE lzo
	,response VARCHAR(65535)  		--//  ENCODE lzo
	,file_name VARCHAR(100)  		--//  ENCODE lzo
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
)
		--// DISTSTYLE EVEN
;
--DROP TABLE sgpwks_integration.wks_pop6_sg_tasks;
CREATE TABLE IF NOT EXISTS SGPWKS_INTEGRATION.WKS_POP6_SG_TASKS		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS sgpwks_integration.wks_pop6_sg_tasks
(
	visit_id VARCHAR(255)  		--//  ENCODE lzo
	,task_group VARCHAR(50)  		--//  ENCODE lzo
	,task_id numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,task_name VARCHAR(50)  		--//  ENCODE lzo
	,field_id VARCHAR(255)  		--//  ENCODE lzo
	,field_code VARCHAR(255)  		--//  ENCODE lzo
	,field_label VARCHAR(255)  		--//  ENCODE lzo
	,field_type VARCHAR(50)  		--//  ENCODE lzo
	,dependent_on_field_id VARCHAR(255)  		--//  ENCODE lzo
	,response VARCHAR(65535)  		--//  ENCODE lzo
	,file_name VARCHAR(100)  		--//  ENCODE lzo
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
)
		--// DISTSTYLE EVEN
;
--DROP TABLE sgpwks_integration.wks_pop6_sg_users;
CREATE TABLE IF NOT EXISTS SGPWKS_INTEGRATION.WKS_POP6_SG_USERS		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS sgpwks_integration.wks_pop6_sg_users
(
	status VARCHAR(255)  		--//  ENCODE lzo
	,userdb_id VARCHAR(255)  		--//  ENCODE lzo
	,username VARCHAR(255)  		--//  ENCODE lzo
	,first_name VARCHAR(255)  		--//  ENCODE lzo
	,last_name VARCHAR(255)  		--//  ENCODE lzo
	,team VARCHAR(255)  		--//  ENCODE lzo
	,superior_name VARCHAR(255)  		--//  ENCODE lzo
	,authorisation_group VARCHAR(255)  		--//  ENCODE lzo
	,email_address VARCHAR(255)  		--//  ENCODE lzo
	,longitude VARCHAR(255)  		--//  ENCODE lzo
	,latitude VARCHAR(255)  		--//  ENCODE lzo
	,business_units_id VARCHAR(255)  		--//  ENCODE lzo
	,business_unit_name VARCHAR(255)  		--//  ENCODE lzo
	,mobile_number VARCHAR(255)  		--//  ENCODE lzo
	,file_name VARCHAR(255)  		--//  ENCODE lzo
	,run_id VARCHAR(255)  		--//  ENCODE lzo
)

;

--DROP TABLE thawks_integration.wks_pop6_th_display_plans;
CREATE TABLE IF NOT EXISTS THAWKS_INTEGRATION.WKS_POP6_TH_DISPLAY_PLANS		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS thawks_integration.wks_pop6_th_display_plans
(
	display_plan_id VARCHAR(255)  		--//  ENCODE zstd
	,status numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,allocation_method VARCHAR(255)  		--//  ENCODE zstd
	,pop_code_or_pop_list_code VARCHAR(50)  		--//  ENCODE zstd
	,team VARCHAR(50)  		--//  ENCODE zstd
	,display_code VARCHAR(255)  		--//  ENCODE zstd
	,display_name VARCHAR(255)  		--//  ENCODE zstd
	,required_number_of_displays numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,start_date DATE  		--//  ENCODE az64
	,end_date DATE  		--//  ENCODE az64
	,product_attribute_id VARCHAR(255)  		--//  ENCODE zstd
	,product_attribute VARCHAR(200)  		--//  ENCODE zstd
	,product_attribute_value_id VARCHAR(255)  		--//  ENCODE zstd
	,product_attribute_value VARCHAR(255)  		--//  ENCODE zstd
	,comments VARCHAR(255)  		--//  ENCODE zstd
	,file_name VARCHAR(100)  		--//  ENCODE zstd
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
)
		--// DISTSTYLE EVEN
;
--DROP TABLE thawks_integration.wks_pop6_th_displays;
CREATE TABLE IF NOT EXISTS THAWKS_INTEGRATION.WKS_POP6_TH_DISPLAYS		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS thawks_integration.wks_pop6_th_displays
(
	visit_id VARCHAR(255)  		--//  ENCODE zstd
	,display_plan_id VARCHAR(255)  		--//  ENCODE zstd
	,display_type VARCHAR(255)  		--//  ENCODE zstd
	,display_code VARCHAR(255)  		--//  ENCODE zstd
	,display_name VARCHAR(255)  		--//  ENCODE zstd
	,start_date DATE  		--//  ENCODE az64
	,end_date DATE  		--//  ENCODE az64
	,checklist_method VARCHAR(50)  		--//  ENCODE zstd
	,display_number numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,product_attribute_id VARCHAR(255)  		--//  ENCODE zstd
	,product_attribute VARCHAR(200)  		--//  ENCODE zstd
	,product_attribute_value_id VARCHAR(255)  		--//  ENCODE zstd
	,product_attribute_value VARCHAR(255)  		--//  ENCODE zstd
	,comments VARCHAR(255)  		--//  ENCODE zstd
	,field_id VARCHAR(255)  		--//  ENCODE zstd
	,field_code VARCHAR(255)  		--//  ENCODE zstd
	,field_label VARCHAR(255)  		--//  ENCODE zstd
	,field_type VARCHAR(50)  		--//  ENCODE zstd
	,dependent_on_field_id VARCHAR(255)  		--//  ENCODE zstd
	,response VARCHAR(65535)  		--//  ENCODE zstd
	,file_name VARCHAR(100)  		--//  ENCODE zstd
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
)
		--// DISTSTYLE EVEN
;
--DROP TABLE thawks_integration.wks_pop6_th_executed_visits;
CREATE TABLE IF NOT EXISTS THAWKS_INTEGRATION.WKS_POP6_TH_EXECUTED_VISITS		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS thawks_integration.wks_pop6_th_executed_visits
(
	visit_id VARCHAR(255)  		--//  ENCODE zstd
	,visit_date DATE  		--//  ENCODE az64
	,check_in_datetime TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,check_out_datetime TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
	,popdb_id VARCHAR(255)  		--//  ENCODE zstd
	,pop_code VARCHAR(50)  		--//  ENCODE zstd
	,pop_name VARCHAR(100)  		--//  ENCODE zstd
	,address VARCHAR(150)  		--//  ENCODE zstd
	,check_in_longitude NUMERIC(18,5)  		--//  ENCODE az64
	,check_in_latitude NUMERIC(18,5)  		--//  ENCODE az64
	,check_out_longitude NUMERIC(18,5)  		--//  ENCODE az64
	,check_out_latitude NUMERIC(18,5)  		--//  ENCODE az64
	,check_in_photo VARCHAR(200)  		--//  ENCODE zstd
	,check_out_photo VARCHAR(200)  		--//  ENCODE zstd
	,username VARCHAR(50)  		--//  ENCODE zstd
	,user_full_name VARCHAR(50)  		--//  ENCODE zstd
	,superior_username VARCHAR(50)  		--//  ENCODE zstd
	,superior_name VARCHAR(50)  		--//  ENCODE zstd
	,planned_visit numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,cancelled_visit numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,cancellation_reason VARCHAR(255)  		--//  ENCODE zstd
	,cancellation_note VARCHAR(255)  		--//  ENCODE zstd
	,file_name VARCHAR(100)  		--//  ENCODE zstd
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
)
		--// DISTSTYLE EVEN
;
--DROP TABLE thawks_integration.wks_pop6_th_general_audits;
CREATE TABLE IF NOT EXISTS THAWKS_INTEGRATION.WKS_POP6_TH_GENERAL_AUDITS		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS thawks_integration.wks_pop6_th_general_audits
(
	visit_id VARCHAR(255)  		--//  ENCODE zstd
	,audit_form_id VARCHAR(255)  		--//  ENCODE zstd
	,audit_form VARCHAR(255)  		--//  ENCODE zstd
	,section_id VARCHAR(255)  		--//  ENCODE zstd
	,section VARCHAR(255)  		--//  ENCODE zstd
	,subsection_id VARCHAR(255)  		--//  ENCODE zstd
	,subsection VARCHAR(255)  		--//  ENCODE zstd
	,field_id VARCHAR(255)  		--//  ENCODE zstd
	,field_code VARCHAR(255)  		--//  ENCODE zstd
	,field_label VARCHAR(255)  		--//  ENCODE zstd
	,field_type VARCHAR(50)  		--//  ENCODE zstd
	,dependent_on_field_id VARCHAR(255)  		--//  ENCODE zstd
	,response VARCHAR(65535)  		--//  ENCODE zstd
	,file_name VARCHAR(100)  		--//  ENCODE zstd
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
)
		--// DISTSTYLE EVEN
;
--DROP TABLE thawks_integration.wks_pop6_th_planned_visits;
CREATE TABLE IF NOT EXISTS THAWKS_INTEGRATION.WKS_POP6_TH_PLANNED_VISITS		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS thawks_integration.wks_pop6_th_planned_visits
(
	planned_visit_date DATE  		--//  ENCODE az64
	,popdb_id VARCHAR(255)  		--//  ENCODE zstd
	,pop_code VARCHAR(50)  		--//  ENCODE zstd
	,pop_name VARCHAR(100)  		--//  ENCODE zstd
	,address VARCHAR(150)  		--//  ENCODE zstd
	,username VARCHAR(50)  		--//  ENCODE zstd
	,user_full_name VARCHAR(50)  		--//  ENCODE zstd
	,file_name VARCHAR(100)  		--//  ENCODE zstd
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
)
		--// DISTSTYLE EVEN
;
--DROP TABLE thawks_integration.wks_pop6_th_pop_lists;
CREATE TABLE IF NOT EXISTS THAWKS_INTEGRATION.WKS_POP6_TH_POP_LISTS		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS thawks_integration.wks_pop6_th_pop_lists
(
	status VARCHAR(255)  		--//  ENCODE lzo
	,pop_list VARCHAR(255)  		--//  ENCODE lzo
	,popdb_id VARCHAR(255)  		--//  ENCODE lzo
	,pop_code VARCHAR(255)  		--//  ENCODE lzo
	,pop_name VARCHAR(255)  		--//  ENCODE lzo
	,date VARCHAR(255)  		--//  ENCODE lzo
	,file_name VARCHAR(255)  		--//  ENCODE lzo
	,run_id VARCHAR(255)  		--//  ENCODE lzo
)

;
--DROP TABLE thawks_integration.wks_pop6_th_pops;
CREATE TABLE IF NOT EXISTS THAWKS_INTEGRATION.WKS_POP6_TH_POPS		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS thawks_integration.wks_pop6_th_pops
(
	status VARCHAR(255)  		--//  ENCODE lzo
	,popdb_id VARCHAR(255)  		--//  ENCODE lzo
	,pop_code VARCHAR(255)  		--//  ENCODE lzo
	,pop_name VARCHAR(255)  		--//  ENCODE lzo
	,address VARCHAR(255)  		--//  ENCODE lzo
	,longitude VARCHAR(255)  		--//  ENCODE lzo
	,latitude VARCHAR(255)  		--//  ENCODE lzo
	,business_units_id VARCHAR(255)  		--//  ENCODE lzo
	,business_unit_name VARCHAR(255)  		--//  ENCODE lzo
	,country VARCHAR(255)  		--//  ENCODE lzo
	,channel VARCHAR(255)  		--//  ENCODE lzo
	,retail_environment_ps VARCHAR(255)  		--//  ENCODE lzo
	,sales_group_name VARCHAR(255)  		--//  ENCODE lzo
	,store_type VARCHAR(255)  		--//  ENCODE lzo
	,sales_group_code VARCHAR(255)  		--//  ENCODE lzo
	,customer VARCHAR(255)  		--//  ENCODE lzo
	,customer_grade VARCHAR(255)  		--//  ENCODE lzo
	,external_store_code VARCHAR(255)  		--//  ENCODE lzo
	,territory VARCHAR(255)  		--//  ENCODE lzo
	,file_name VARCHAR(255)  		--//  ENCODE lzo
	,run_id VARCHAR(255)  		--//  ENCODE lzo
)

;
--DROP TABLE thawks_integration.wks_pop6_th_product_attribute_audits;
CREATE TABLE IF NOT EXISTS THAWKS_INTEGRATION.WKS_POP6_TH_PRODUCT_ATTRIBUTE_AUDITS		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS thawks_integration.wks_pop6_th_product_attribute_audits
(
	visit_id VARCHAR(255)  		--//  ENCODE zstd
	,audit_form_id VARCHAR(255)  		--//  ENCODE zstd
	,audit_form VARCHAR(255)  		--//  ENCODE zstd
	,section_id VARCHAR(255)  		--//  ENCODE zstd
	,section VARCHAR(255)  		--//  ENCODE zstd
	,field_id VARCHAR(255)  		--//  ENCODE zstd
	,field_code VARCHAR(255)  		--//  ENCODE zstd
	,field_label VARCHAR(255)  		--//  ENCODE zstd
	,field_type VARCHAR(50)  		--//  ENCODE zstd
	,dependent_on_field_id VARCHAR(255)  		--//  ENCODE zstd
	,product_attribute_id VARCHAR(255)  		--//  ENCODE zstd
	,product_attribute VARCHAR(200)  		--//  ENCODE zstd
	,product_attribute_value_id VARCHAR(255)  		--//  ENCODE zstd
	,product_attribute_value VARCHAR(200)  		--//  ENCODE zstd
	,response VARCHAR(65535)  		--//  ENCODE zstd
	,file_name VARCHAR(100)  		--//  ENCODE zstd
	,run_id NUMERIC(14,0)  		--//  ENCODE zstd
)
		--// DISTSTYLE EVEN
;
--DROP TABLE thawks_integration.wks_pop6_th_product_lists_allocation;
CREATE TABLE IF NOT EXISTS THAWKS_INTEGRATION.WKS_POP6_TH_PRODUCT_LISTS_ALLOCATION		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS thawks_integration.wks_pop6_th_product_lists_allocation
(
	product_group_status VARCHAR(255)  		--//  ENCODE lzo
	,product_group VARCHAR(255)  		--//  ENCODE lzo
	,product_list_status VARCHAR(255)  		--//  ENCODE lzo
	,product_list VARCHAR(255)  		--//  ENCODE lzo
	,product_list_code VARCHAR(255)  		--//  ENCODE lzo
	,pop_attribute_id VARCHAR(255)  		--//  ENCODE lzo
	,pop_attribute VARCHAR(255)  		--//  ENCODE lzo
	,pop_attribute_value_id VARCHAR(255)  		--//  ENCODE lzo
	,pop_attribute_value VARCHAR(255)  		--//  ENCODE lzo
	,date VARCHAR(255)  		--//  ENCODE lzo
	,file_name VARCHAR(255)  		--//  ENCODE lzo
	,run_id VARCHAR(255)  		--//  ENCODE lzo
)

;
--DROP TABLE thawks_integration.wks_pop6_th_product_lists_pops;
CREATE TABLE IF NOT EXISTS THAWKS_INTEGRATION.WKS_POP6_TH_PRODUCT_LISTS_POPS		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS thawks_integration.wks_pop6_th_product_lists_pops
(
	product_list VARCHAR(255)  		--//  ENCODE lzo
	,product_list_code VARCHAR(255)  		--//  ENCODE lzo
	,popdb_id VARCHAR(255)  		--//  ENCODE lzo
	,pop_code VARCHAR(255)  		--//  ENCODE lzo
	,pop_name VARCHAR(255)  		--//  ENCODE lzo
	,date VARCHAR(255)  		--//  ENCODE lzo
	,file_name VARCHAR(255)  		--//  ENCODE lzo
	,run_id VARCHAR(255)  		--//  ENCODE lzo
)

;
--DROP TABLE thawks_integration.wks_pop6_th_product_lists_products;
CREATE TABLE IF NOT EXISTS THAWKS_INTEGRATION.WKS_POP6_TH_PRODUCT_LISTS_PRODUCTS		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS thawks_integration.wks_pop6_th_product_lists_products
(
	product_list VARCHAR(255)  		--//  ENCODE lzo
	,product_list_code VARCHAR(255)  		--//  ENCODE lzo
	,productdb_id VARCHAR(255)  		--//  ENCODE lzo
	,sku VARCHAR(255)  		--//  ENCODE lzo
	,msl_ranking VARCHAR(255)  		--//  ENCODE lzo
	,date VARCHAR(255)  		--//  ENCODE lzo
	,file_name VARCHAR(255)  		--//  ENCODE lzo
	,run_id VARCHAR(255)  		--//  ENCODE lzo
)

;
--DROP TABLE thawks_integration.wks_pop6_th_products;
CREATE TABLE IF NOT EXISTS THAWKS_INTEGRATION.WKS_POP6_TH_PRODUCTS		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS thawks_integration.wks_pop6_th_products
(
	status VARCHAR(255)  		--//  ENCODE lzo
	,productdb_id VARCHAR(255)  		--//  ENCODE lzo
	,barcode VARCHAR(255)  		--//  ENCODE lzo
	,sku VARCHAR(255)  		--//  ENCODE lzo
	,unit_price VARCHAR(255)  		--//  ENCODE lzo
	,display_order VARCHAR(255)  		--//  ENCODE lzo
	,launch_date VARCHAR(255)  		--//  ENCODE lzo
	,largest_uom_quantity VARCHAR(255)  		--//  ENCODE lzo
	,middle_uom_quantity VARCHAR(255)  		--//  ENCODE lzo
	,smallest_uom_quantity VARCHAR(255)  		--//  ENCODE lzo
	,sku_english VARCHAR(255)  		--//  ENCODE lzo
	,company VARCHAR(255)  		--//  ENCODE lzo
	,sku_code VARCHAR(255)  		--//  ENCODE lzo
	,ps_category VARCHAR(255)  		--//  ENCODE lzo
	,ps_segment VARCHAR(255)  		--//  ENCODE lzo
	,ps_category_segment VARCHAR(255)  		--//  ENCODE lzo
	,country_l1 VARCHAR(255)  		--//  ENCODE lzo
	,regional_franchise_l2 VARCHAR(255)  		--//  ENCODE lzo
	,franchise_l3 VARCHAR(255)  		--//  ENCODE lzo
	,brand_l4 VARCHAR(255)  		--//  ENCODE lzo
	,sub_category_l5 VARCHAR(255)  		--//  ENCODE lzo
	,platform_l6 VARCHAR(255)  		--//  ENCODE lzo
	,variance_l7 VARCHAR(255)  		--//  ENCODE lzo
	,pack_size_l8 VARCHAR(255)  		--//  ENCODE lzo
	,file_name VARCHAR(255)  		--//  ENCODE lzo
	,run_id VARCHAR(255)  		--//  ENCODE lzo
)

;
--DROP TABLE thawks_integration.wks_pop6_th_promotion_plans;
CREATE TABLE IF NOT EXISTS THAWKS_INTEGRATION.WKS_POP6_TH_PROMOTION_PLANS		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS thawks_integration.wks_pop6_th_promotion_plans
(
	status numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,promotion_plan_id VARCHAR(255)  		--//  ENCODE zstd
	,allocation_method VARCHAR(255)  		--//  ENCODE zstd
	,pop_code_or_pop_list_code VARCHAR(50)  		--//  ENCODE zstd
	,team VARCHAR(50)  		--//  ENCODE zstd
	,promotion_code VARCHAR(255)  		--//  ENCODE zstd
	,promotion_name VARCHAR(255)  		--//  ENCODE zstd
	,promotion_mechanics VARCHAR(255)  		--//  ENCODE zstd
	,start_date DATE  		--//  ENCODE az64
	,end_date DATE  		--//  ENCODE az64
	,product_attribute_id VARCHAR(255)  		--//  ENCODE zstd
	,product_attribute VARCHAR(200)  		--//  ENCODE zstd
	,product_attribute_value_id VARCHAR(255)  		--//  ENCODE zstd
	,product_attribute_value VARCHAR(65535)  		--//  ENCODE zstd
	,promotion_price NUMERIC(18,2)  		--//  ENCODE az64
	,price_field VARCHAR(255)  		--//  ENCODE zstd
	,photo_field VARCHAR(255)  		--//  ENCODE zstd
	,reason_field VARCHAR(255)  		--//  ENCODE zstd
	,file_name VARCHAR(100)  		--//  ENCODE zstd
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
)
		--// DISTSTYLE EVEN
;
--DROP TABLE thawks_integration.wks_pop6_th_promotions;
CREATE TABLE IF NOT EXISTS THAWKS_INTEGRATION.WKS_POP6_TH_PROMOTIONS		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS thawks_integration.wks_pop6_th_promotions
(
	visit_id VARCHAR(255)  		--//  ENCODE zstd
	,promotion_plan_id VARCHAR(255)  		--//  ENCODE zstd
	,promotion_code VARCHAR(255)  		--//  ENCODE zstd
	,promotion_name VARCHAR(255)  		--//  ENCODE zstd
	,promotion_mechanics VARCHAR(255)  		--//  ENCODE zstd
	,promotion_type VARCHAR(255)  		--//  ENCODE zstd
	,start_date DATE  		--//  ENCODE az64
	,end_date DATE  		--//  ENCODE az64
	,product_attribute_id VARCHAR(255)  		--//  ENCODE zstd
	,product_attribute VARCHAR(200)  		--//  ENCODE zstd
	,product_attribute_value_id VARCHAR(255)  		--//  ENCODE zstd
	,product_attribute_value VARCHAR(65535)  		--//  ENCODE zstd
	,promotion_price NUMERIC(18,2)  		--//  ENCODE az64
	,promotion_compliance VARCHAR(10)  		--//  ENCODE zstd
	,actual_price NUMERIC(18,2)  		--//  ENCODE az64
	,non_compliance_reason VARCHAR(255)  		--//  ENCODE zstd
	,photo VARCHAR(65535)  		--//  ENCODE zstd
	,file_name VARCHAR(100)  		--//  ENCODE zstd
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
)
		--// DISTSTYLE EVEN
;
--DROP TABLE thawks_integration.wks_pop6_th_rir_data;
CREATE TABLE IF NOT EXISTS THAWKS_INTEGRATION.WKS_POP6_TH_RIR_DATA		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS thawks_integration.wks_pop6_th_rir_data
(
	visit_id VARCHAR(255)  		--//  ENCODE zstd
	,sku_id VARCHAR(255)  		--//  ENCODE zstd
	,sku VARCHAR(255)  		--//  ENCODE zstd
	,facing numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,is_eyelevel numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,file_name VARCHAR(100)  		--//  ENCODE zstd
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
)
		--// DISTSTYLE EVEN
;
--DROP TABLE thawks_integration.wks_pop6_th_service_levels;
CREATE TABLE IF NOT EXISTS THAWKS_INTEGRATION.WKS_POP6_TH_SERVICE_LEVELS		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS thawks_integration.wks_pop6_th_service_levels
(
	sales_grp_nm VARCHAR(200)  		--//  ENCODE zstd
	,customer_grade VARCHAR(200)  		--//  ENCODE zstd
	,team VARCHAR(200)  		--//  ENCODE zstd
	,visit_frequency VARCHAR(10)  		--//  ENCODE zstd
	,estimated_visit_duration VARCHAR(150)  		--//  ENCODE zstd
	,service_level_date DATE  		--//  ENCODE az64
	,file_name VARCHAR(100)  		--//  ENCODE zstd
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)
		--// DISTSTYLE EVEN
;
--DROP TABLE thawks_integration.wks_pop6_th_sku_audits;
CREATE TABLE IF NOT EXISTS THAWKS_INTEGRATION.WKS_POP6_TH_SKU_AUDITS		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS thawks_integration.wks_pop6_th_sku_audits
(
	visit_id VARCHAR(255)  		--//  ENCODE zstd
	,audit_form_id VARCHAR(255)  		--//  ENCODE zstd
	,audit_form VARCHAR(255)  		--//  ENCODE zstd
	,section_id VARCHAR(255)  		--//  ENCODE zstd
	,section VARCHAR(255)  		--//  ENCODE zstd
	,field_id VARCHAR(255)  		--//  ENCODE zstd
	,field_code VARCHAR(255)  		--//  ENCODE zstd
	,field_label VARCHAR(255)  		--//  ENCODE zstd
	,field_type VARCHAR(50)  		--//  ENCODE zstd
	,dependent_on_field_id VARCHAR(255)  		--//  ENCODE zstd
	,sku_id VARCHAR(255)  		--//  ENCODE zstd
	,sku VARCHAR(255)  		--//  ENCODE zstd
	,response VARCHAR(65535)  		--//  ENCODE zstd
	,file_name VARCHAR(100)  		--//  ENCODE zstd
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
)
		--// DISTSTYLE EVEN
;
--DROP TABLE thawks_integration.wks_pop6_th_tasks;
CREATE TABLE IF NOT EXISTS THAWKS_INTEGRATION.WKS_POP6_TH_TASKS		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS thawks_integration.wks_pop6_th_tasks
(
	visit_id VARCHAR(255)  		--//  ENCODE zstd
	,task_group VARCHAR(50)  		--//  ENCODE zstd
	,task_id numeric(18,0)		--//  ENCODE az64 // INTEGER  
	,task_name VARCHAR(50)  		--//  ENCODE zstd
	,field_id VARCHAR(255)  		--//  ENCODE zstd
	,field_code VARCHAR(255)  		--//  ENCODE zstd
	,field_label VARCHAR(255)  		--//  ENCODE zstd
	,field_type VARCHAR(50)  		--//  ENCODE zstd
	,dependent_on_field_id VARCHAR(255)  		--//  ENCODE zstd
	,response VARCHAR(65535)  		--//  ENCODE zstd
	,file_name VARCHAR(100)  		--//  ENCODE zstd
	,run_id NUMERIC(14,0)  		--//  ENCODE az64
	,crtd_dttm TIMESTAMP WITHOUT TIME ZONE  		--//  ENCODE az64
)
		--// DISTSTYLE EVEN
;
--DROP TABLE thawks_integration.wks_pop6_th_users;
CREATE TABLE IF NOT EXISTS THAWKS_INTEGRATION.WKS_POP6_TH_USERS		--// CREATE TABLE IF NOT EXISTS  // CREATE TABLE IF NOT EXISTS thawks_integration.wks_pop6_th_users
(
	status VARCHAR(255)  		--//  ENCODE lzo
	,userdb_id VARCHAR(255)  		--//  ENCODE lzo
	,username VARCHAR(255)  		--//  ENCODE lzo
	,first_name VARCHAR(255)  		--//  ENCODE lzo
	,last_name VARCHAR(255)  		--//  ENCODE lzo
	,team VARCHAR(255)  		--//  ENCODE lzo
	,superior_name VARCHAR(255)  		--//  ENCODE lzo
	,authorisation_group VARCHAR(255)  		--//  ENCODE lzo
	,email_address VARCHAR(255)  		--//  ENCODE lzo
	,longitude VARCHAR(255)  		--//  ENCODE lzo
	,latitude VARCHAR(255)  		--//  ENCODE lzo
	,business_units_id VARCHAR(255)  		--//  ENCODE lzo
	,business_unit_name VARCHAR(255)  		--//  ENCODE lzo
	,file_name VARCHAR(255)  		--//  ENCODE lzo
	,run_id VARCHAR(255)  		--//  ENCODE lzo
)

;

