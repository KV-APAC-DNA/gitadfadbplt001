--JP Sellout Market

-- Delete existing process and Parameters and re-insert to follow on the MDS standard

--Update Active flag to false to stop CData Ingestion

--Insert New process (table in the MDS JP Sellout Job(s))

--Updates in existing Process to follow on the MDS standard

UPDATE META_RAW.PROCESS
SET PROCESS_NAME = REPLACE(PROCESS_NAME,'jp_mds_refresh_sellout','JP_Sellout_MDS_Refresh')
WHERE USECASE_ID = 320 and phase_id = 1;

--Insert New parameters 

--Updates in existing parameters to follow on the MDS standard

UPDATE META_RAW.PARAMETERS
SET PARAMETER_GROUP_NAME = REPLACE(PARAMETER_GROUP_NAME,'jp_mds_refresh_sellout','SDL_MDS')
WHERE PARAMETER_GROUP_ID IN (1644, 1645, 1646);

UPDATE META_RAW.PARAMETERS
SET  PARAMETER_VALUE = CONCAT('sql_server/MDS/',REPLACE(PARAMETER_GROUP_NAME, '_group',''),'/')
WHERE PARAMETER_GROUP_ID IN (1644, 1645, 1646)
AND PARAMETER_NAME = 'landing_file_path';

UPDATE META_RAW.PARAMETERS
SET  PARAMETER_VALUE = REPLACE(PARAMETER_GROUP_NAME, '_group','')
WHERE PARAMETER_GROUP_ID IN (1644, 1645, 1646)
AND PARAMETER_NAME = 'landing_file_name';
