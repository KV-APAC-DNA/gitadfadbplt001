update meta_raw.parameters set parameter_value = 'POS/transaction/daily_sat_sellout/' where parameter_id = 10321;
update meta_raw.s3_to_adls set adls_path = 'POS/transaction/daily_sat_sellout/' where id = 317;
