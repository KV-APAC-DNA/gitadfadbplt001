use schema meta_raw;
update parameters set parameter_value='sales_tool_v_120/transaction' where parameter_id=2510;
update s3_to_adls set s3_path = 'GT_Intervention/DnA_VMR/' where id in(111,112,114);
update s3_to_adls set s3_path = 'sales_tool_v_120/' where id in(113);
update s3_to_adls set adls_path = 'sales_tool_v_120/transaction' where id in(113);
