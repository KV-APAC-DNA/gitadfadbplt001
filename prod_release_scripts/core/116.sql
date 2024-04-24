delete from pcfitg_integration.itg_perenso_diary_item_time where run_id='20240421223703' and to_date(create_dt) ='2024-04-22' and diary_item_key='855855';

UPDATE core_integration.dbtjobs_test_cdc_metadata
  SET tempid = '14'
  WHERE JOB_NAME = 'my_sellin';

UPDATE core_integration.dbtjobs_test_cdc_metadata
  SET tempid = '15'
  WHERE JOB_NAME = 'my_sellout';
  
UPDATE core_integration.dbtjobs_test_cdc_metadata
  SET JOB_NAME = 'my_sellout_sales'
  WHERE tempid = '16';

UPDATE core_integration.dbtjobs_test_cdc_metadata
  SET JOB_NAME = 'th_mt_daily_price_load'
  WHERE tempid = '29';
