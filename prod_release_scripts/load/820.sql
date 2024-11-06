Alter table SDL_MDS_PH_POS_PRODUCT_TEMP_ADFTEMP add column ROWNUM varchar(100);

insert into SDL_MDS_PH_POS_PRODUCT_TEMP 
select * from SDL_MDS_PH_POS_PRODUCT_TEMP_ADFTEMP  where Item_cd in
(
'78752',
'78756',
'78757',
'78753',
'78754',
'78755'
);
