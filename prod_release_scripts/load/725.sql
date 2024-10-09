update META_RAW.PARAMETERS
SET PARAMETER_VALUE = 'sdl_distributor_ivy_outlet_master_mds_sync'
WHERE PARAMETER_ID = 27009;

update META_RAW.PARAMETERS
SET PARAMETER_VALUE = 'sql_server/MDS_Reverse_Sync/MDS_Adhoc_sdl_distributor_ivy_outlet_master_mds_sync'
WHERE PARAMETER_ID = 27008;

update META_RAW.PARAMETERS
SET PARAMETER_VALUE = 'dbt_cloud_pr_5458_1401.idnitg_integration__sdl_distributor_ivy_outlet_master_mds_sync'
WHERE PARAMETER_ID = 27011;

update META_RAW.PARAMETERS
SET PARAMETER_VALUE = 'SELECT key_outlet  AS "key_outlet",jj_sap_dstrbtr_id  AS "jj_sap_dstrbtr_id",jj_sap_dstrbtr_nm  AS "jj_sap_dstrbtr_nm",cust_id  AS "cust_id",cust_nm  AS "cust_nm",address  AS "address",city  AS "city",cust_grp  AS "cust_grp",chnl  AS "chnl",outlet_type  AS "outlet_type",chnl_grp  AS "chnl_grp",jjid  AS "jjid",pst_cd  AS "pst_cd",cust_id_map  AS "cust_id_map",cust_nm_map  AS "cust_nm_map",chnl_grp2  AS "chnl_grp2",cust_crtd_dt  AS "cust_crtd_dt",cust_grp2  AS "cust_grp2",crtd_dttm  AS "crtd_dttm",updt_dttm  AS "updt_dttm" FROM IDNITG_INTEGRATION.SDL_DISTRIBUTOR_IVY_OUTLET_MASTER_MDS_SYNC'
WHERE PARAMETER_ID = 27012;
