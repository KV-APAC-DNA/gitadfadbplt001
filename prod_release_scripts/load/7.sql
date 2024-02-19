update meta_raw.usecase
set USECASE_NAME='POP6_refresh' ,CATEGORY='POP6_refresh', USECASE_DESCRIPTION='Tableau Refresh Adhoc FOR POP6'
where usecase_id=13;
INSERT INTO meta_raw.USECASE (USECASE_ID, USECASE_NAME,CATEGORY,USECASE_DESCRIPTION,IS_ACTIVE,SEQUENCE_ID) VALUES (39,'CUSTOMER360_refresh','CUSTOMER360_refresh','Tableau Refresh Adhoc FOR CUSTOMER360','TRUE',1);

update meta_raw.process
set USECASE_ID=39
where process_id=31;
