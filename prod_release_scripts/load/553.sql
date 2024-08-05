--TW Updated to standarize the MDS Config

UPDATE META_RAW.PARAMETERS 
SET PARAMETER_VALUE='sql_server/MDS/SDL_MDS_TW_Sales_Representative/' 
WHERE PARAMETER_ID='12819';

UPDATE META_RAW.PARAMETERS
SET PARAMETER_VALUE = Concat('SELECT * FROM MDS.mdm.', Replace(Replace(parameter_group_name,'_group',''), 'SDL_MDS_',''), ' where ValidationStatus <>''Validation failed''')
WHERE PARAMETER_GROUP_NAME like 'SDL_MDS_TW_%' 
and PARAMETER_NAME = 'ms_query'
and PARAMETER_ID in (12743, 12751);
