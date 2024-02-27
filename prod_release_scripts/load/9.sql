USE DATABASE PROD_DNA_LOAD;
-- Update PROD_DNA_LOAD.META_RAW.usecase set is_active= 'TRUE' where usecase_id in (3,37);

Update PROD_DNA_LOAD.META_RAW.usecase set SEQUENCE_ID = 2  where usecase_id = 3;
Update PROD_DNA_LOAD.META_RAW.usecase set SEQUENCE_ID = 3  where usecase_id = 37;


