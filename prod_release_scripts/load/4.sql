update set  META_RAW.PARAMETERS 
PARAMETER_VALUE = 'prd/transaction/sellout/sales'
where parameter_name = 'folder_path' and PARAMETER_GROUP_ID in (90,91,92,93,94,95,96,97,98,99,100,101,102,103,104,105,106,107,108,109,110,111,112,113,114,115);

update META_RAW.s3_to_adls set
ADLS_PATH  = 'prd/transaction/sellout/sales'
where s3_path = 'ap_my_metadata/transaction/sellout/sales'
