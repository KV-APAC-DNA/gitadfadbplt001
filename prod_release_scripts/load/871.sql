update  prod_dna_load.meta_raw.PARAMETERS 
set 
parameter_value='business_unit,date,stock_type,product_code,product_name,batch,expirydate,base_uom,shelf_life_(mths),cogs,region,quantity,value,plant_code,plant_name,syslot'
where parameter_group_id=3007 and parameter_name='val_file_header';
