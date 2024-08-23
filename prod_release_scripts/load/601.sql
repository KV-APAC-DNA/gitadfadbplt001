--CN Market

-- Delete existing process and Parameters and utilising for another process as its reduntant

--Update Active flag to false to stop CData Ingestion

--Insert New process (table in the MDS CN Job(s))


--Updates in existing Process to follow on the MDS standard

update META_RAW.process p
set process_name = Replace(process_name,'ch_ecomm_mds', 'China_MDS')
where usecase_id in (384);


--Insert New parameters 


--Updates in existing parameters to follow on the MDS standard

update META_RAW.parameters
set parameter_group_name = REPLACE(parameter_group_name, 'ch_ecomm_mds_','')
where parameter_group_name like '%SDL_MDS_CN%';

update META_RAW.parameters
set parameter_value = REPLACE(parameter_value, 'ch_ecomm_mds','MDS')
where parameter_group_name like 'SDL_MDS_CN%'
and parameter_name = 'landing_file_path';

update META_RAW.parameters
set parameter_value = REPLACE(parameter_group_name, '_group','')
where parameter_group_name like 'SDL_MDS_CN%'
and parameter_name = 'landing_file_name';
