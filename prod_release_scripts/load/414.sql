delete from meta_raw.process
where parameter_id in(1062
,1063
,1064
,1065
,1066
,1067
,1068
,1069
,1070
,1071
,1072
,1073
,1074
,1075
,1076
,1077
,1078);
use schema meta_raw;
INSERT INTO PROCESS VALUES (1232,1062,'id_ivy_dbt_ing',1062,1,1,FALSE,TRUE,205,NULL,2,NULL,'','','','','','');
INSERT INTO PROCESS VALUES (1233,1063,'id_ivy_dbt_tran',1063,1,2,FALSE,TRUE,205,NULL,2,NULL,'','','','','','');
INSERT INTO PROCESS VALUES (1234,1064,'id_ivy_salesman_master_update_dbt_tran',1064,1,3,FALSE,TRUE,205,NULL,2,NULL,'','','','','','');
INSERT INTO PROCESS VALUES (1235,1065,'id_cd_server_data_dbt_ing',1065,1,3,FALSE,TRUE,205,NULL,2,NULL,'','','','','','');
INSERT INTO PROCESS VALUES (1236,1066,'id_cd_server_data_dbt_tran',1066,1,4,FALSE,TRUE,205,NULL,2,NULL,'','','','','','');
INSERT INTO PROCESS VALUES (1237,1067,'id_ivy_datamart_load_dbt_tran',1067,1,5,FALSE,TRUE,205,NULL,2,NULL,'','','','','','');
INSERT INTO PROCESS VALUES (1238,1068,'id_mds_itg_load_dbt_ing',1068,1,1,FALSE,TRUE,213,NULL,2,NULL,'','','','','','');
INSERT INTO PROCESS VALUES (1239,1069,'id_mds_itg_load_dbt_tran',1069,1,2,FALSE,TRUE,213,NULL,2,NULL,'','','','','','');
INSERT INTO PROCESS VALUES (1240,1070,'id_mds_sdl_itg_load_dist_cust_dim_dbt_ing',1070,1,1,FALSE,TRUE,214,NULL,2,NULL,'','','','','','');
INSERT INTO PROCESS VALUES (1241,1071,'id_stock_load_dbt_ing',1071,1,1,FALSE,TRUE,245,NULL,2,NULL,'','','','','','');
INSERT INTO PROCESS VALUES (1242,1072,'id_dist_cust_dim_itg_edw_load_dbt_tran',1072,1,2,FALSE,TRUE,214,NULL,2,NULL,'','','','','','');
INSERT INTO PROCESS VALUES (1243,1073,'id_sellin_analysis_dbt_tran',1073,1,1,FALSE,TRUE,257,NULL,2,NULL,'','','','','','');
INSERT INTO PROCESS VALUES (1244,1074,'id_npi_dataload_dbt_tran',1074,1,1,FALSE,TRUE,257,NULL,2,NULL,'','','','','','');
INSERT INTO PROCESS VALUES (1245,1075,'id_order_sellin_analysis_dbt_tran',1075,1,2,FALSE,TRUE,257,NULL,2,NULL,'','','','','','');
INSERT INTO PROCESS VALUES (1246,1076,'id_noo_analysis_dbt_tran',1076,1,2,FALSE,TRUE,257,NULL,2,NULL,'','','','','','');
INSERT INTO PROCESS VALUES (1247,1077,'id_lppb_analysis_dbt_tran',1077,1,3,FALSE,TRUE,257,NULL,2,NULL,'','','','','','');
INSERT INTO PROCESS VALUES (1248,1078,'j_ap_id_reg_data_refresh_dbt_tran',1078,1,4,FALSE,TRUE,257,NULL,2,NULL,'','','','','','');
