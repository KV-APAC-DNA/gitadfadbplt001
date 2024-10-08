update meta_raw.parameters set parameter_value = 'Visit_ID,Photo,Related_Attribute,SKU_id,SKU,Layer,Total_Layer,Facing_of_this_layer' where parameter_group_id = 1640
and parameter_name = 'val_file_header';


drop table THASDL_RAW.SDL_POP6_TH_RIR_DATA;

create or replace TABLE THASDL_RAW.SDL_POP6_TH_RIR_DATA (
	VISIT_ID VARCHAR(255),
    photo varchar(500),
    related_attribute varchar(255),
	SKU_ID VARCHAR(255),
	SKU VARCHAR(255),
	layer NUMBER(18,0),
	total_layer NUMBER(18,0),
    facing_of_this_layer NUMBER(18,0),
	FILE_NAME VARCHAR(100),
	RUN_ID NUMBER(14,0),
	CRTD_DTTM TIMESTAMP_NTZ(9)
);
