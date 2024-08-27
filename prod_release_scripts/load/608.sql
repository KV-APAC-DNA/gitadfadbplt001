--MY GT

UPDATE META_RAW.PROCESS 
SET Is_Active = false
where process_id = 116;

UPDATE META_RAW.PARAMETERS 
SET PARAMETER_GROUP_NAME = Replace (PARAMETER_GROUP_NAME, '_GROUP','_group')
where parameter_group_id in (2211, 2212);

UPDATE META_RAW.PARAMETERS 
SET parameter_value = Replace (parameter_value, '_temp_mds','_temp')
where parameter_group_id in (2211, 2212)
and parameter_name in ('target_schema_table');
