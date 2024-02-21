alter table aspitg_integration.itg_rg_travel_retail_sellout
add column hash_key varchar(32);

update aspitg_integration.itg_rg_travel_retail_sellout
set hash_key=md5(concat(coalesce(year_month::varchar,''),'_',coalesce(upper(retailer_name),''),'_',coalesce(upper(ctry_cd),''))) where ctry_cd in ('CN','SG','CM','HK');

update aspitg_integration.itg_rg_travel_retail_sellout
set hash_key=md5(concat(coalesce(year_month::varchar,''),'_',coalesce(upper(door_name),''),'_',coalesce(upper(ctry_cd),''))) where ctry_cd in('KR');

--=============================================Transactional tables column addition====================================================================

alter table aspitg_integration.itg_copa_trans add column file_name varchar(255);
alter table aspitg_integration.itg_invnt add column file_name varchar(255);
alter table aspitg_integration.ITG_COPA17_TRANS add column file_name varchar(255);
alter table aspitg_integration.itg_invc_sls add column file_name varchar(255);

update aspitg_integration.itg_copa_trans set file_name='No file name in Legacy System' where file_name is null;
update aspitg_integration.itg_invnt set file_name='No file name in Legacy System' where file_name is null;
update aspitg_integration.ITG_COPA17_TRANS set file_name='No file name in Legacy System' where file_name is null;
update aspitg_integration.itg_invc_sls set file_name='No file name in Legacy System' where file_name is null;
