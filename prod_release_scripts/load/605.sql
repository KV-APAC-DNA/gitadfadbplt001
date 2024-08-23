--MDS JP Reverse Sync 

UPDATE META_RAW.PROCESS 
SET PROCESS_NAME = 'dw_pos_daily'
where process_id = 1740;

UPDATE META_RAW.PARAMETERS 
SET PARAMETER_GROUP_NAME = 'dw_pos_daily_group'
where parameter_group_id = 1740;

UPDATE META_RAW.PARAMETERS 
SET PARAMETER_VALUE = REPLACE(PARAMETER_VALUE,'_Temp','')
where parameter_group_id = 1740 and PARAMETER_ID in (20138, 20139);

UPDATE META_RAW.PARAMETERS 
SET PARAMETER_VALUE = 'sql_server/MDS_Reverse_Sync/'
where parameter_group_id = 1740 and PARAMETER_ID in (20137);

--MDS MY GT Reverse Sync 
update parameters
set parameter_value = 'sql_server/MDS_Reverse_Sync/'
where parameter_group_id in (2211,2212)
and parameter_name = 'landing_file_path'


