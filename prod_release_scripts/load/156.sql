update meta_raw.PARAMETERS
set PARAMETER_VALUE = 'GT_Intervention/reference/DnA_VMR/Msl'
where parameter_id = 2672;

update meta_raw.PARAMETERS
set PARAMETER_VALUE = 'csv'
where parameter_id = 3197;

update meta_raw.parameters
set parameter_value = 'dms/master/dms_source'
where parameter_name = 'folder_path' and PARAMETER_GROUP_ID in(
211
,212
,213
,214
,215
,216
,217
,218
);

update meta_raw.parameters
set parameter_value = 'dms/transation/dms_source'
where parameter_name = 'folder_path' and PARAMETER_GROUP_ID in(
219
,220
,221
,222
,223
);

update meta_raw.parameters
set parameter_value = 'GT/Reference/Topdoor'
where parameter_name = 'folder_path' and PARAMETER_GROUP_ID = 224;

update meta_raw.parameters
set parameter_value = 'otc_sellout/transation'
where parameter_name = 'folder_path' and PARAMETER_GROUP_ID = 225;
