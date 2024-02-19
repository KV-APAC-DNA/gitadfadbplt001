USE DATABASE PROD_DNA_LOAD;
USE SCHEMA META_RAW;

create or replace TABLE META_RAW.PROD_RUN_METADATA (
	PROCESS_ID NUMBER(38,0) autoincrement start 1 increment 1 order,
	DB VARCHAR(50),
	VERSION NUMBER(38,0),
	FILE_NAME VARCHAR(50),
	STATUS VARCHAR(50),
	TIMESTAMP TIMESTAMP_NTZ(9)
);

INSERT INTO META_RAW.PROD_RUN_METADATA(db,version,file_name,status,timestamp) values
('load',0,null,null,current_timestamp),
('core',0,null,null,current_timestamp);
