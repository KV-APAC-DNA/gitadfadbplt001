Update META_RAW.PROCESS Set PROCESS_NAME=replace(PROCESS_NAME,'NA_MDS','na_mds_tab_refresh')  where  USECASE_ID='241'
and SOURCE_ID in (2,5) and PHASE_ID='1';
