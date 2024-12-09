update META_RAW.PARAMETERS 
set parameter_value = LEFT(parameter_value, length(parameter_value)-1)
where parameter_group_id in (2242, 2243, 2244, 2245, 2246, 2248, 2249, 2250, 2251, 2252, 2253)
and PARAMETER_NAME = 'folder_path'
