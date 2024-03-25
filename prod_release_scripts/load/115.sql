update META_RAW.s3_to_adls set 
s3_path = 'raw-data-lake/travel_retail/transaction_files/'
where id = 1;

update META_RAW.PARAMETERS set 
PARAMETER_VALUE = 'GT_Intervention/transaction/DnA_VMR/CustomerDim'
where PARAMETER_ID = 2451;

update META_RAW.PARAMETERS set 
PARAMETER_VALUE = 'GT_Intervention/transaction/DnA_VMR/Visit'
where PARAMETER_ID = 2469;
