update META_RAW.PARAMETERS set 
PARAMETER_VALUE = '6900'
where parameter_name = 'jobId' and PARAMETER_GROUP_ID =157;

update META_RAW.PARAMETERS set 
PARAMETER_VALUE = 'MY_gt_sales_SnowflakePROD_to_MDSPROD'
where parameter_name = 'jobname' and PARAMETER_GROUP_ID =116;
