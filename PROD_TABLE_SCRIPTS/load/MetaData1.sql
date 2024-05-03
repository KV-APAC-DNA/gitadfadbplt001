update meta_raw.parmeters
set PARAMETER_VALUE = 'xlsx'
where PARAMETER_ID in (5702,5692);
update meta_raw.parmeters
set PARAMETER_VALUE = 'A7'
where PARAMETER_ID = 5704;

update meta_raw.category_market_mapping
SET CATEGORY = 'REGIONAL REFRESH'
WHERE CATEGORY = 'REGIONAL_REFRESH';
