update PROD_DNA_LOAD.META_RAW.PARAMETERS SET PARAMETER_VALUE = 'sdl_ph_non_ise_svi_smc_test'
where parameter_name = 'target_table' and parameter_group_id = '5080';
update PROD_DNA_LOAD.META_RAW.PARAMETERS SET PARAMETER_VALUE = 'sdl_ph_non_ise_waltermart_test'
where parameter_name = 'target_table' and parameter_group_id = '5081';
update PROD_DNA_LOAD.META_RAW.PARAMETERS SET PARAMETER_VALUE = 'sdl_ph_non_ise_landmark_sm_test'
where parameter_name = 'target_table' and parameter_group_id = '5082';
update PROD_DNA_LOAD.META_RAW.PARAMETERS SET PARAMETER_VALUE = 'sdl_ph_non_ise_rustans_test'
where parameter_name = 'target_table' and parameter_group_id = '5083';
update PROD_DNA_LOAD.META_RAW.PARAMETERS SET PARAMETER_VALUE = 'sdl_ph_non_ise_puregold_test'
where parameter_name = 'target_table' and parameter_group_id = '5084';
update PROD_DNA_LOAD.META_RAW.PARAMETERS SET PARAMETER_VALUE = 'sdl_ph_non_ise_landmark_ds_test'
where parameter_name = 'target_table' and parameter_group_id = '5085';
update PROD_DNA_LOAD.META_RAW.PARAMETERS SET PARAMETER_VALUE = 'sdl_ph_non_ise_shm_test'
where parameter_name = 'target_table' and parameter_group_id = '5086';
update PROD_DNA_LOAD.META_RAW.PARAMETERS SET PARAMETER_VALUE = 'sdl_ph_non_ise_super_8_test'
where parameter_name = 'target_table' and parameter_group_id = '5087';
update PROD_DNA_LOAD.META_RAW.PARAMETERS SET PARAMETER_VALUE = 'sdl_ph_non_ise_robinsons_ds_test'
where parameter_name = 'target_table' and parameter_group_id = '5088';
update PROD_DNA_LOAD.META_RAW.PARAMETERS SET PARAMETER_VALUE = 'sdl_ph_non_ise_robinsons_sm_test'
where parameter_name = 'target_table' and parameter_group_id = '5089';

update PROD_DNA_LOAD.META_RAW.PROCESS set IS_ACTIVE = FALSE where usecase_id = 546 and process_id in (5078,5079);
