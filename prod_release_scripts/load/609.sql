--Generic Reverse Sync
--Updates in existing process to follow on the MDS standard

UPDATE META_RAW.PROCESS 
SET process_name = 'edw_customer_base_dim' 
where parameter_group_id in (1703);

UPDATE META_RAW.PROCESS 
SET process_name = 'edw_customer_sales_dim' 
where parameter_group_id in (1704);

UPDATE META_RAW.PROCESS 
SET process_name = 'edw_company_dim' 
where parameter_group_id in (1705);

UPDATE META_RAW.PROCESS 
SET process_name = 'edw_copa_trans_fact' 
where parameter_group_id in (1706);

UPDATE META_RAW.PROCESS 
SET process_name = 'itg_jnj_osa_oos_report' 
where parameter_group_id in (1707);

UPDATE META_RAW.PROCESS 
SET process_name = 'edw_material_sales_dim' 
where parameter_group_id in (1708);

UPDATE META_RAW.PROCESS 
SET process_name = 'edw_gch_producthierarchy' 
where parameter_group_id in (1709);

UPDATE META_RAW.PROCESS 
SET process_name = 'edw_material_plant_dim' 
where parameter_group_id in (1710);

UPDATE META_RAW.PROCESS 
SET process_name = 'edw_material_uom' 
where parameter_group_id in (1711);

UPDATE META_RAW.PROCESS 
SET process_name = 'edw_list_price' 
where parameter_group_id in (1712);

UPDATE META_RAW.PROCESS 
SET process_name = 'edw_material_dim' 
where parameter_group_id in (1713);

UPDATE META_RAW.PROCESS 
SET process_name = 'edw_sales_org_dim' 
where parameter_group_id in (1714);

UPDATE META_RAW.PROCESS 
SET process_name = 'edw_profit_center_dim' 
where parameter_group_id in (1715);

UPDATE META_RAW.PROCESS 
SET process_name = 'edw_customer_attr_hier_dim' 
where parameter_group_id in (1716);

UPDATE META_RAW.PROCESS 
SET process_name = 'edi_item_m' 
where parameter_group_id in (1717);

UPDATE META_RAW.PROCESS 
SET process_name = 'edi_jedpar' 
where parameter_group_id in (1718);

UPDATE META_RAW.PROCESS 
SET process_name = 'edi_excl_rtlr' 
where parameter_group_id in (1719);

UPDATE META_RAW.PROCESS 
SET process_name = 'edi_rtlr_cd_chng' 
where parameter_group_id in (1720);

UPDATE META_RAW.PROCESS 
SET process_name = 'edi_store_m' 
where parameter_group_id in (1721);

UPDATE META_RAW.PROCESS 
SET process_name = 'wk_so_planet_no_dup' 
where parameter_group_id in (1722);

UPDATE META_RAW.PROCESS 
SET process_name = 'edw_ecc_marm' 
where parameter_group_id in (1723);

--Updates in existing parameters to follow on the MDS standard

UPDATE META_RAW.PARAMETERS 
SET parameter_value = 'asp'
where parameter_group_id in (1703, 1704, 1705, 1706, 1707, 1708, 1709, 1710, 1711, 1712, 1713, 1714, 1715, 1716, 1723 )
and parameter_name = 'container';

UPDATE META_RAW.PARAMETERS 
SET parameter_value = 'jpn'
where parameter_group_id in (1717, 1718, 1719, 1720, 1721, 1722 )
and parameter_name = 'container';

UPDATE META_RAW.PARAMETERS 
SET parameter_value = 'sql_server/MDS_Reverse_Sync/'
where parameter_group_id in (1703, 1704, 1705, 1706, 1707, 1708, 1709, 1710, 1711, 1712, 1713, 1714, 1715, 1716, 1717,
1718, 1719, 1720, 1721, 1722, 1723 )
and parameter_name = 'landing_file_path';

UPDATE META_RAW.PARAMETERS 
SET parameter_group_name = 'edw_customer_base_dim_group' 
where parameter_group_id in (1703);

UPDATE META_RAW.PARAMETERS 
SET parameter_group_name = 'edw_customer_sales_dim_group' 
where parameter_group_id in (1704);

UPDATE META_RAW.PARAMETERS 
SET parameter_group_name = 'edw_company_dim_group' 
where parameter_group_id in (1705);

UPDATE META_RAW.PARAMETERS 
SET parameter_group_name = 'edw_copa_trans_fact_group' 
where parameter_group_id in (1706);

UPDATE META_RAW.PARAMETERS 
SET parameter_group_name = 'itg_jnj_osa_oos_report_group' 
where parameter_group_id in (1707);

UPDATE META_RAW.PARAMETERS 
SET parameter_group_name = 'edw_material_sales_dim_group' 
where parameter_group_id in (1708);

UPDATE META_RAW.PARAMETERS 
SET parameter_group_name = 'edw_gch_producthierarchy_group' 
where parameter_group_id in (1709);

UPDATE META_RAW.PARAMETERS 
SET parameter_group_name = 'edw_material_plant_dim_group' 
where parameter_group_id in (1710);

UPDATE META_RAW.PARAMETERS 
SET parameter_group_name = 'edw_material_uom_group' 
where parameter_group_id in (1711);

UPDATE META_RAW.PARAMETERS 
SET parameter_group_name = 'edw_list_price_group' 
where parameter_group_id in (1712);

UPDATE META_RAW.PARAMETERS 
SET parameter_group_name = 'edw_material_dim_group' 
where parameter_group_id in (1713);

UPDATE META_RAW.PARAMETERS 
SET parameter_group_name = 'edw_sales_org_dim_group' 
where parameter_group_id in (1714);
UPDATE META_RAW.PARAMETERS 
SET parameter_group_name = 'edw_profit_center_dim_group' 
where parameter_group_id in (1715);

UPDATE META_RAW.PARAMETERS 
SET parameter_group_name = 'edw_customer_attr_hier_dim_group' 
where parameter_group_id in (1716);

UPDATE META_RAW.PARAMETERS 
SET parameter_group_name = 'edi_item_m_group' 
where parameter_group_id in (1717);

UPDATE META_RAW.PARAMETERS 
SET parameter_group_name = 'edi_jedpar_group' 
where parameter_group_id in (1718);

UPDATE META_RAW.PARAMETERS 
SET parameter_group_name = 'edi_excl_rtlr_group' 
where parameter_group_id in (1719);

UPDATE META_RAW.PARAMETERS 
SET parameter_group_name = 'edi_rtlr_cd_chng_group' 
where parameter_group_id in (1720);

UPDATE META_RAW.PARAMETERS 
SET parameter_group_name = 'edi_store_m_group' 
where parameter_group_id in (1721);

UPDATE META_RAW.PARAMETERS 
SET parameter_group_name = 'wk_so_planet_no_dup_group' 
where parameter_group_id in (1722);

UPDATE META_RAW.PARAMETERS 
SET parameter_group_name = 'edw_ecc_marm_group' 
where parameter_group_id in (1723);

UPDATE META_RAW.PARAMETERS 
SET parameter_value = Replace(parameter_group_name, '_group', '')
where parameter_group_id in (1703, 1704, 1705, 1706, 1707, 1708, 1709, 1710, 1711, 1712, 1713, 1714, 1715, 1716, 1717,
1718, 1719, 1720, 1721, 1722, 1723 )
and parameter_name = 'landing_file_name';

--ID Ivy and Alteryx Sync

UPDATE META_RAW.PROCESS 
SET PROCESS_NAME = 'SDL_DISTRIBUTOR_IVY_OUTLET_MASTER_MDS_SYNC'
where process_id = 976;

UPDATE META_RAW.PROCESS 
SET PROCESS_NAME = 'SDL_DISTRIBUTOR_ALTRYX_CUST_DIM_MDS_SYNC'
where process_id = 977;

--Updates in existing Process to follow on the MDS standard

UPDATE META_RAW.PARAMETERS 
SET PARAMETER_GROUP_NAME = 'SDL_DISTRIBUTOR_IVY_OUTLET_MASTER_MDS_SYNC_group'
where parameter_group_id = 976;

UPDATE META_RAW.PARAMETERS 
SET PARAMETER_GROUP_NAME = 'SDL_DISTRIBUTOR_ALTRYX_CUST_DIM_MDS_SYNC_group'
where parameter_group_id = 977;

UPDATE META_RAW.PARAMETERS 
SET PARAMETER_VALUE = 'sql_server/MDS_Reverse_Sync/'
where parameter_group_id IN (976, 977) and PARAMETER_ID in (12399, 12407);

UPDATE META_RAW.PARAMETERS 
SET PARAMETER_VALUE = Replace(PARAMETER_GROUP_NAME, '_group','')
where parameter_group_id IN (976, 977) and PARAMETER_ID in (12400, 12408);

