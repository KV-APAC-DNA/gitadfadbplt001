--Update Korea Metadata to standardize MDS process convention

UPDATE META_RAW.PARAMETERS
SET parameter_group_name = Replace(parameter_group_name, '_GROUP', '_group') 
where parameter_group_name like 'SDL_MDS_KR_%'
and parameter_group_name not like '%_group'
and parameter_id = '26663';

UPDATE META_RAW.PARAMETERS
SET parameter_group_name = Concat(parameter_group_name,'_group')
where parameter_group_name like 'SDL_MDS_KR_%'
and parameter_group_name not like '%_group'
and parameter_id = '26662';

--Dropping reduntant table
DROP TABLE chnsdl_raw.MDS_Mother_Code;
