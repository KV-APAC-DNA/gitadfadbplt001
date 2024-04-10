USE SCHEMA META_RAW;

update parameters set parameter_value='MT/transaction/MT_source/Inventory/' where parameter_name='folder_path' and parameter_group_id=297;

update s3_to_adls set ADLS_PATH='MT/transaction/MT_source/Inventory/' where GROUP_ID=15 and id=137;

