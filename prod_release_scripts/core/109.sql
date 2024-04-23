create or replace view prod_DNA_CORE.phledw_integration.EDW_VW_PH_POS_CUSTOMER_DIM(
	CNTRY_CD,
	CNTRY_NM,
	CUST_CD,
	CUST_NM,
	SOLD_TO,
	BRNCH_CD,
	BRNCH_NM,
	BRNCH_FRMT,
	BRNCH_TYP,
	DEPT_CD,
	DEPT_NM,
	ADDRESS1,
	ADDRESS2,
	REGION_CD,
	REGION_NM,
	PROV_CD,
	PROV_NM,
	CITY_CD,
	CITY_NM,
	MNCPLTY_CD,
	MNCPLTY_NM
) as
(
  (
    SELECT
      'PH' AS cntry_cd,
      'Philippines' AS cntry_nm,
      CAST((
        itg_mds_ph_pos_customers.cust_cd
      ) AS VARCHAR(255)) AS cust_cd,
      NULL AS cust_nm,
      NULL AS sold_to,
      CAST((
        itg_mds_ph_pos_customers.brnch_cd
      ) AS VARCHAR(255)) AS brnch_cd,
      itg_mds_ph_pos_customers.brnch_nm,
      NULL AS brnch_frmt,
      NULL AS brnch_typ,
      NULL AS dept_cd,
      NULL AS dept_nm,
      itg_mds_ph_pos_customers.address1,
      itg_mds_ph_pos_customers.address2,
      CAST((
        itg_mds_ph_pos_customers.region_cd
      ) AS VARCHAR(255)) AS region_cd,
      itg_mds_ph_pos_customers.region_nm,
      CAST((
        itg_mds_ph_pos_customers.prov_cd
      ) AS VARCHAR(255)) AS prov_cd,
      itg_mds_ph_pos_customers.prov_nm,
      CAST((
        itg_mds_ph_pos_customers.city_cd
      ) AS VARCHAR(255)) AS city_cd,
      itg_mds_ph_pos_customers.city_nm,
      CAST((
        itg_mds_ph_pos_customers.mncplty_cd
      ) AS VARCHAR(255)) AS mncplty_cd,
      itg_mds_ph_pos_customers.mncplty_nm
    FROM  prod_DNA_CORE.PHLITG_INTEGRATION.itg_mds_ph_pos_customers
    WHERE
      (
        CAST((
          itg_mds_ph_pos_customers.active
        ) AS TEXT) = CAST((
          CAST('Y' AS VARCHAR)
        ) AS TEXT)
      )
  )
);

create or replace view PROD_DNA_CORE.PHLEDW_INTEGRATION.EDW_VW_PH_POS_MATERIAL_DIM(
	CNTRY_CD,
	CNTRY_NM,
	JJ_MNTH_ID,
	CUST_CD,
	ITEM_CD,
	ITEM_NM,
	SAP_ITEM_CD,
	BAR_CD,
	CUST_SKU_GRP,
	CUST_CONV_FACTOR,
	CUST_ITEM_PRC,
	LST_PERIOD,
	EARLY_BK_PERIOD,
	EFF_STR_DATE,
	EFF_END_DATE
) as(
    SELECT 'PH' AS cntry_cd,
        'Philippines' AS cntry_nm,
        itg_mds_ph_pos_product.mnth_id AS jj_mnth_id,
        itg_mds_ph_pos_product.cust_cd,
        itg_mds_ph_pos_product.item_cd,
        itg_mds_ph_pos_product.item_nm,
        itg_mds_ph_pos_product.sap_item_cd,
        itg_mds_ph_pos_product.bar_cd,
        itg_mds_ph_pos_product.cust_sku_grp,
        CASE
            WHEN (
                (itg_mds_ph_pos_product.cust_cd)::text = ('MDC'::character varying)::text
            ) THEN itg_mds_ph_pos_product.jnj_pc_per_cust_unit
            WHEN (
                (itg_mds_ph_pos_product.cust_cd)::text = ('PG'::character varying)::text
            ) THEN itg_mds_ph_pos_product.jnj_pc_per_cust_unit
            WHEN (
                (itg_mds_ph_pos_product.cust_cd)::text = ('WM'::character varying)::text
            ) THEN itg_mds_ph_pos_product.cust_conv_factor
            WHEN (
                (itg_mds_ph_pos_product.cust_cd)::text = ('ROB'::character varying)::text
            ) THEN itg_mds_ph_pos_product.cust_conv_factor
            WHEN (
                (itg_mds_ph_pos_product.cust_cd)::text = ('WAT'::character varying)::text
            ) THEN itg_mds_ph_pos_product.cust_conv_factor
            WHEN (
                (itg_mds_ph_pos_product.cust_cd)::text = ('SS'::character varying)::text
            ) THEN itg_mds_ph_pos_product.jnj_pc_per_cust_unit
            WHEN (
                (
                    (
                        (itg_mds_ph_pos_product.cust_cd)::text = ('RS'::character varying)::text
                    )
                    OR (
                        (itg_mds_ph_pos_product.cust_cd)::text = ('WC'::character varying)::text
                    )
                )
                OR (
                    (itg_mds_ph_pos_product.cust_cd)::text = ('SW'::character varying)::text
                )
            ) THEN itg_mds_ph_pos_product.jnj_pc_per_cust_unit
            WHEN (
                (itg_mds_ph_pos_product.cust_cd)::text = ('DYNA'::character varying)::text
            ) THEN itg_mds_ph_pos_product.cust_conv_factor
            ELSE (NULL::numeric)::numeric(18, 0)
        END AS cust_conv_factor,
        itg_mds_ph_pos_product.cust_item_prc,
        itg_mds_ph_pos_product.lst_period,
        itg_mds_ph_pos_product.early_bk_period,
        NULL::date AS eff_str_date,
        NULL::date AS eff_end_date
    FROM  PHLITG_INTEGRATION.itg_mds_ph_pos_product
    WHERE (
            (itg_mds_ph_pos_product.active)::text = ('Y'::character varying)::text
        )
    
);

create or replace view PROD_DNA_CORE.PHLEDW_INTEGRATION.EDW_VW_PH_CUSTOMER_DIM(
        sap_cust_id,
        sap_cust_nm,
        sap_sls_org,
        sap_cmp_id,
        sap_cntry_cd,
        sap_cntry_nm,
        sap_addr,
        sap_region,
        sap_state_cd,
        sap_city,
        sap_post_cd,
        sap_chnl_cd,
        sap_chnl_desc,
        sap_sls_office_cd,
        sap_sls_office_desc,
        sap_sls_grp_cd,
        sap_sls_grp_desc,
        sap_curr_cd,
        sap_prnt_cust_key,
        sap_prnt_cust_desc,
        sap_cust_chnl_key,
        sap_cust_chnl_desc,
        sap_cust_sub_chnl_key,
        sap_sub_chnl_desc,
        sap_go_to_mdl_key,
        sap_go_to_mdl_desc,
        sap_bnr_key,
        sap_bnr_desc,
        sap_bnr_frmt_key,
        sap_bnr_frmt_desc,
        retail_env,
        gch_region,
        gch_cluster,
        gch_subcluster,
        gch_market,
        gch_retail_banner
    ) as
select * from 
(SELECT sap_cust_id as sap_cust_id,
    sap_cust_nm as sap_cust_nm,
    sap_sls_org as sap_sls_org,
    sap_cmp_id as sap_cmp_id,
    sap_cntry_cd as sap_cntry_cd,
    sap_cntry_nm as sap_cntry_nm,
    sap_addr as sap_addr,
    sap_region as sap_region,
    sap_state_cd as sap_state_cd,
    sap_city as sap_city,
    sap_post_cd as sap_post_cd,
    sap_chnl_cd as sap_chnl_cd,
    sap_chnl_desc as sap_chnl_desc,
    sap_sls_office_cd as sap_sls_office_cd,
    sap_sls_office_desc as sap_sls_office_desc,
    sap_sls_grp_cd as sap_sls_grp_cd,
    sap_sls_grp_desc as sap_sls_grp_desc,
    sap_curr_cd as sap_curr_cd,
    sap_prnt_cust_key as sap_prnt_cust_key,
    sap_prnt_cust_desc as sap_prnt_cust_desc,
    sap_cust_chnl_key as sap_cust_chnl_key,
    sap_cust_chnl_desc as sap_cust_chnl_desc,
    sap_cust_sub_chnl_key as sap_cust_sub_chnl_key,
    sap_sub_chnl_desc as sap_sub_chnl_desc,
    sap_go_to_mdl_key as sap_go_to_mdl_key,
    sap_go_to_mdl_desc as sap_go_to_mdl_desc,
    sap_bnr_key as sap_bnr_key,
    sap_bnr_desc as sap_bnr_desc,
    sap_bnr_frmt_key as sap_bnr_frmt_key,
    sap_bnr_frmt_desc as sap_bnr_frmt_desc,
    retail_env as retail_env,
    gch_region as gch_region,
    gch_cluster as gch_cluster,
    gch_subcluster as gch_subcluster,
    gch_market as gch_market,
    gch_retail_banner as gch_retail_banner
FROM (
        (
            (
                (
                    SELECT ecbd.cust_num AS sap_cust_id,
                        ecbd.cust_nm AS sap_cust_nm,
                        ecsd.sls_org AS sap_sls_org,
                        ecd.company AS sap_cmp_id,
                        ecd.ctry_key AS sap_cntry_cd,
                        ecd.ctry_nm AS sap_cntry_nm,
                        ecbd.addr AS sap_addr,
                        ecbd.rgn AS sap_region,
                        ecbd.dstrc AS sap_state_cd,
                        ecbd.city AS sap_city,
                        ecbd.pstl_cd AS sap_post_cd,
                        ecsd.dstr_chnl AS sap_chnl_cd,
                        edc.txtsh AS sap_chnl_desc,
                        ecsd.sls_ofc AS sap_sls_office_cd,
                        ecsd.sls_ofc_desc AS sap_sls_office_desc,
                        ecsd.sls_grp AS sap_sls_grp_cd,
                        ecsd.sls_grp_desc AS sap_sls_grp_desc,
                        ecsd.crncy_key AS sap_curr_cd,
                        ecsd.prnt_cust_key AS sap_prnt_cust_key,
                        cddes_pck.code_desc AS sap_prnt_cust_desc,
                        ecsd.chnl_key AS sap_cust_chnl_key,
                        cddes_chnl.code_desc AS sap_cust_chnl_desc,
                        ecsd.sub_chnl_key AS sap_cust_sub_chnl_key,
                        cddes_subchnl.code_desc AS sap_sub_chnl_desc,
                        ecsd.go_to_mdl_key AS sap_go_to_mdl_key,
                        cddes_gtm.code_desc AS sap_go_to_mdl_desc,
                        ecsd.bnr_key AS sap_bnr_key,
                        cddes_bnrkey.code_desc AS sap_bnr_desc,
                        ecsd.bnr_frmt_key AS sap_bnr_frmt_key,
                        cddes_bnrfmt.code_desc AS sap_bnr_frmt_desc,
                        subchnl_retail_env.retail_env,
                        egch.gcgh_region AS gch_region,
                        egch.gcgh_cluster AS gch_cluster,
                        egch.gcgh_subcluster AS gch_subcluster,
                        egch.gcgh_market AS gch_market,
                        egch.gcch_retail_banner AS gch_retail_banner
                    FROM ASPEDW_INTEGRATION.edw_gch_customerhierarchy egch,
                        ASPEDW_INTEGRATION.edw_customer_base_dim ecbd,
                        ASPEDW_INTEGRATION.edw_company_dim ecd,
                        ASPEDW_INTEGRATION.edw_dstrbtn_chnl edc,
                        ASPEDW_INTEGRATION.edw_sales_org_dim esod,
                        (
                            SELECT edw_customer_sales_dim.cust_num,
                                min(
                                    (
                                        CASE
                                            WHEN (
                                                (
                                                    (edw_customer_sales_dim.cust_del_flag)::text = NULL::text
                                                )
                                                OR (
                                                    (edw_customer_sales_dim.cust_del_flag IS NULL)
                                                    AND (NULL IS NULL)
                                                )
                                            ) THEN 'O'::character varying
                                            WHEN (
                                                (
                                                    (edw_customer_sales_dim.cust_del_flag)::text = ''::text
                                                )
                                                OR (
                                                    (edw_customer_sales_dim.cust_del_flag IS NULL)
                                                    AND ('' IS NULL)
                                                )
                                            ) THEN 'O'::character varying
                                            ELSE edw_customer_sales_dim.cust_del_flag
                                        END
                                    )::text
                                ) AS cust_del_flag
                            FROM ASPEDW_INTEGRATION.edw_customer_sales_dim
                            WHERE (
                                    (edw_customer_sales_dim.sls_org)::text = '2300'::text
                                )
                            GROUP BY edw_customer_sales_dim.cust_num
                        ) a,
                        (
                            (
                                (
                                    (
                                        (
                                            (
                                                ASPEDW_INTEGRATION.edw_customer_sales_dim ecsd
                                                LEFT JOIN ASPEDW_INTEGRATION.edw_code_descriptions cddes_pck ON (
                                                    (
                                                        (
                                                            (cddes_pck.code)::text = (ecsd.prnt_cust_key)::text
                                                        )
                                                        AND (
                                                            (cddes_pck.code_type)::text = 'Parent Customer Key'::text
                                                        )
                                                    )
                                                )
                                            )
                                            LEFT JOIN ASPEDW_INTEGRATION.edw_code_descriptions cddes_bnrkey ON (
                                                (
                                                    ((cddes_bnrkey.code)::text = (ecsd.bnr_key)::text)
                                                    AND (
                                                        (cddes_bnrkey.code_type)::text = 'Banner Key'::text
                                                    )
                                                )
                                            )
                                        )
                                        LEFT JOIN ASPEDW_INTEGRATION.edw_code_descriptions cddes_bnrfmt ON (
                                            (
                                                (
                                                    (cddes_bnrfmt.code)::text = (ecsd.bnr_frmt_key)::text
                                                )
                                                AND (
                                                    (cddes_bnrfmt.code_type)::text = 'Banner Format Key'::text
                                                )
                                            )
                                        )
                                    )
                                    LEFT JOIN ASPEDW_INTEGRATION.edw_code_descriptions cddes_chnl ON (
                                        (
                                            ((cddes_chnl.code)::text = (ecsd.chnl_key)::text)
                                            AND (
                                                (cddes_chnl.code_type)::text = 'Channel Key'::text
                                            )
                                        )
                                    )
                                )
                                LEFT JOIN ASPEDW_INTEGRATION.edw_code_descriptions cddes_gtm ON (
                                    (
                                        (
                                            (cddes_gtm.code)::text = (ecsd.go_to_mdl_key)::text
                                        )
                                        AND (
                                            (cddes_gtm.code_type)::text = 'Go To Model Key'::text
                                        )
                                    )
                                )
                            )
                            LEFT JOIN (
                                ASPEDW_INTEGRATION.edw_code_descriptions cddes_subchnl
                                LEFT JOIN ASPEDW_INTEGRATION.edw_subchnl_retail_env_mapping subchnl_retail_env ON (
                                    (
                                        (
                                            upper((subchnl_retail_env.sub_channel)::text) = upper((cddes_subchnl.code_desc)::text)
                                        )
                                    )
                                )
                            ) ON (
                                (
                                    (
                                        (cddes_subchnl.code)::text = (ecsd.sub_chnl_key)::text
                                    )
                                    AND (
                                        (cddes_subchnl.code_type)::text = 'Sub Channel Key'::text
                                    )
                                )
                            )
                        )
                    WHERE (
                            (
                                (
                                    (
                                        (
                                            (
                                                (
                                                    ((egch.customer)::text = (ecbd.cust_num)::text)
                                                    AND ((ecsd.cust_num)::text = (ecbd.cust_num)::text)
                                                )
                                                AND (
                                                    (
                                                        CASE
                                                            WHEN (
                                                                ((ecsd.cust_del_flag)::text = NULL::text)
                                                                OR (
                                                                    (ecsd.cust_del_flag IS NULL)
                                                                    AND (NULL IS NULL)
                                                                )
                                                            ) THEN 'O'::character varying
                                                            WHEN (
                                                                ((ecsd.cust_del_flag)::text = ''::text)
                                                                OR (
                                                                    (ecsd.cust_del_flag IS NULL)
                                                                    AND ('' IS NULL)
                                                                )
                                                            ) THEN 'O'::character varying
                                                            ELSE ecsd.cust_del_flag
                                                        END
                                                    )::text = a.cust_del_flag
                                                )
                                            )
                                            AND ((a.cust_num)::text = (ecsd.cust_num)::text)
                                        )
                                        AND ((ecsd.dstr_chnl)::text = (edc.distr_chan)::text)
                                    )
                                    AND ((ecsd.sls_org)::text = (esod.sls_org)::text)
                                )
                                AND ((esod.sls_org_co_cd)::text = (ecd.co_cd)::text)
                            )
                            AND ((ecsd.sls_org)::text = '2300'::text)
                        )
                    UNION ALL
                    SELECT (t.sap_cust_id)::character varying AS sap_cust_id,
                        t.sap_cust_nm,
                        t.sap_sls_org,
                        (t.sap_cmp_id)::character varying AS sap_cmp_id,
                        t.sap_cntry_cd,
                        t.sap_cntry_nm,
                        t.sap_addr,
                        t.sap_region,
                        t.sap_state_cd,
                        t.sap_city,
                        t.sap_post_cd,
                        t.sap_chnl_cd,
                        t.sap_chnl_desc,
                        t.sap_sls_office_cd,
                        t.sap_sls_office_desc,
                        t.sap_sls_grp_cd,
                        t.sap_sls_grp_desc,
                        t.sap_curr_cd,
                        t.sap_prnt_cust_key,
                        t.sap_prnt_cust_desc,
                        t.sap_cust_chnl_key,
                        t.sap_cust_chnl_desc,
                        t.sap_cust_sub_chnl_key,
                        t.sap_sub_chnl_desc,
                        t.sap_go_to_mdl_key,
                        t.sap_go_to_mdl_desc,
                        t.sap_bnr_key,
                        t.sap_bnr_desc,
                        t.sap_bnr_frmt_key,
                        t.sap_bnr_frmt_desc,
                        t.retail_env,
                        t.gch_region,
                        t.gch_cluster,
                        t.gch_subcluster,
                        t.gch_market,
                        t.gch_retail_banner
                    FROM (
                            SELECT ltrim((cbd.cust_num)::text, (0)::text) AS sap_cust_id,
                                cbd.cust_nm AS sap_cust_nm,
                                csd.sls_org AS sap_sls_org,
                                ltrim((cd.company)::text, (0)::text) AS sap_cmp_id,
                                cd.ctry_key AS sap_cntry_cd,
                                cd.ctry_nm AS sap_cntry_nm,
                                cbd.addr AS sap_addr,
                                cbd.rgn AS sap_region,
                                cbd.dstrc AS sap_state_cd,
                                cbd.city AS sap_city,
                                cbd.pstl_cd AS sap_post_cd,
                                csd.dstr_chnl AS sap_chnl_cd,
                                dc.txtsh AS sap_chnl_desc,
                                csd.sls_ofc AS sap_sls_office_cd,
                                csd.sls_ofc_desc AS sap_sls_office_desc,
                                csd.sls_grp AS sap_sls_grp_cd,
                                csd.sls_grp_desc AS sap_sls_grp_desc,
                                csd.crncy_key AS sap_curr_cd,
                                csd.prnt_cust_key AS sap_prnt_cust_key,
                                cddes_pck.code_desc AS sap_prnt_cust_desc,
                                csd.chnl_key AS sap_cust_chnl_key,
                                cddes_chnl.code_desc AS sap_cust_chnl_desc,
                                csd.sub_chnl_key AS sap_cust_sub_chnl_key,
                                cddes_subchnl.code_desc AS sap_sub_chnl_desc,
                                csd.go_to_mdl_key AS sap_go_to_mdl_key,
                                cddes_gtm.code_desc AS sap_go_to_mdl_desc,
                                csd.bnr_key AS sap_bnr_key,
                                cddes_bnrkey.code_desc AS sap_bnr_desc,
                                csd.bnr_frmt_key AS sap_bnr_frmt_key,
                                cddes_bnrfmt.code_desc AS sap_bnr_frmt_desc,
                                subchnl_retail_env.retail_env,
                                gch.gcgh_region AS gch_region,
                                gch.gcgh_cluster AS gch_cluster,
                                gch.gcgh_subcluster AS gch_subcluster,
                                gch.gcgh_market AS gch_market,
                                gch.gcch_retail_banner AS gch_retail_banner,
                                row_number() OVER(
                                    PARTITION BY ltrim((csd.cust_num)::text, (0)::text)
                                    ORDER BY CASE
                                            WHEN (
                                                ((csd.cust_del_flag)::text = NULL::text)
                                                OR (
                                                    (csd.cust_del_flag IS NULL)
                                                    AND (NULL IS NULL)
                                                )
                                            ) THEN 'O'::character varying
                                            WHEN (
                                                ((csd.cust_del_flag)::text = ''::text)
                                                OR (
                                                    (csd.cust_del_flag IS NULL)
                                                    AND ('' IS NULL)
                                                )
                                            ) THEN 'O'::character varying
                                            ELSE csd.cust_del_flag
                                        END,
                                        csd.sls_org,
                                        csd.dstr_chnl
                                ) AS rnk
                            FROM (
                                    (
                                        (
                                            (
                                                (
                                                    (
                                                        (
                                                            (
                                                                (
                                                                    (
                                                                        (
                                                                            (
                                                                                ASPEDW_INTEGRATION.edw_customer_sales_dim csd
                                                                                JOIN ASPEDW_INTEGRATION.edw_customer_base_dim cbd ON (
                                                                                    (
                                                                                        ltrim((csd.cust_num)::text, (0)::text) = ltrim((cbd.cust_num)::text, (0)::text)
                                                                                    )
                                                                                )
                                                                            )
                                                                            LEFT JOIN ASPEDW_INTEGRATION.edw_gch_customerhierarchy gch ON (
                                                                                (
                                                                                    ltrim((gch.customer)::text, (0)::text) = ltrim((cbd.cust_num)::text, (0)::text)
                                                                                )
                                                                            )
                                                                        )
                                                                        JOIN ASPEDW_INTEGRATION.edw_dstrbtn_chnl dc ON (
                                                                            (
                                                                                ltrim((csd.dstr_chnl)::text, (0)::text) = ltrim((dc.distr_chan)::text, (0)::text)
                                                                            )
                                                                        )
                                                                    )
                                                                    JOIN ASPEDW_INTEGRATION.edw_sales_org_dim sod ON (
                                                                        (
                                                                            (
                                                                                (
                                                                                    trim((csd.sls_org)::text) = trim((sod.sls_org)::text)
                                                                                )
                                                                                AND ((sod.ctry_key)::text = 'TH'::text)
                                                                            )
                                                                            AND (
                                                                                ((sod.sls_org)::text = '2400'::text)
                                                                                OR ((sod.sls_org)::text = '2500'::text)
                                                                            )
                                                                        )
                                                                    )
                                                                )
                                                                JOIN ASPEDW_INTEGRATION.edw_company_dim cd ON (((sod.sls_org_co_cd)::text = (cd.co_cd)::text))
                                                            )
                                                            LEFT JOIN ASPEDW_INTEGRATION.edw_code_descriptions cddes_pck ON (
                                                                (
                                                                    (
                                                                        (cddes_pck.code_type)::text = 'Parent Customer Key'::text
                                                                    )
                                                                    AND (
                                                                        (cddes_pck.code)::text = (csd.prnt_cust_key)::text
                                                                    )
                                                                )
                                                            )
                                                        )
                                                        LEFT JOIN ASPEDW_INTEGRATION.edw_code_descriptions cddes_bnrkey ON (
                                                            (
                                                                (
                                                                    (cddes_bnrkey.code_type)::text = 'Banner Key'::text
                                                                )
                                                                AND ((cddes_bnrkey.code)::text = (csd.bnr_key)::text)
                                                            )
                                                        )
                                                    )
                                                    LEFT JOIN ASPEDW_INTEGRATION.edw_code_descriptions cddes_bnrfmt ON (
                                                        (
                                                            (
                                                                (cddes_bnrfmt.code_type)::text = 'Banner Format Key'::text
                                                            )
                                                            AND (
                                                                (cddes_bnrfmt.code)::text = (csd.bnr_frmt_key)::text
                                                            )
                                                        )
                                                    )
                                                )
                                                LEFT JOIN ASPEDW_INTEGRATION.edw_code_descriptions cddes_chnl ON (
                                                    (
                                                        (
                                                            (cddes_chnl.code_type)::text = 'Channel Key'::text
                                                        )
                                                        AND ((cddes_chnl.code)::text = (csd.chnl_key)::text)
                                                    )
                                                )
                                            )
                                            LEFT JOIN ASPEDW_INTEGRATION.edw_code_descriptions cddes_gtm ON (
                                                (
                                                    (
                                                        (cddes_gtm.code_type)::text = 'Go To Model Key'::text
                                                    )
                                                    AND (
                                                        (cddes_gtm.code)::text = (csd.go_to_mdl_key)::text
                                                    )
                                                )
                                            )
                                        )
                                        LEFT JOIN ASPEDW_INTEGRATION.edw_code_descriptions cddes_subchnl ON (
                                            (
                                                (
                                                    (cddes_subchnl.code_type)::text = 'Sub Channel Key'::text
                                                )
                                                AND (
                                                    (cddes_subchnl.code)::text = (csd.sub_chnl_key)::text
                                                )
                                            )
                                        )
                                    )
                                    LEFT JOIN ASPEDW_INTEGRATION.edw_subchnl_retail_env_mapping subchnl_retail_env ON (
                                        (
                                            upper((subchnl_retail_env.sub_channel)::text) = upper((cddes_subchnl.code_desc)::text)
                                        )
                                    )
                                )
                            WHERE (
                                    ((csd.sls_org)::text = '2400'::text)
                                    OR ((csd.sls_org)::text = '2500'::text)
                                )
                        ) t
                    WHERE (t.rnk = 1)
                )
                UNION ALL
                SELECT (ltrim((recbd.cust_num)::text, '0'::text))::character varying AS sap_cust_id,
                    recbd.cust_nm AS sap_cust_nm,
                    recsd.sls_org AS sap_sls_org,
                    recd.company AS sap_cmp_id,
                    recd.ctry_key AS sap_cntry_cd,
                    recd.ctry_nm AS sap_cntry_nm,
                    recbd.addr AS sap_addr,
                    recbd.rgn AS sap_region,
                    recbd.dstrc AS sap_state_cd,
                    recbd.city AS sap_city,
                    recbd.pstl_cd AS sap_post_cd,
                    recsd.dstr_chnl AS sap_chnl_cd,
                    redc.txtsh AS sap_chnl_desc,
                    recsd.sls_ofc AS sap_sls_office_cd,
                    recsd.sls_ofc_desc AS sap_sls_office_desc,
                    recsd.sls_grp AS sap_sls_grp_cd,
                    recsd.sls_grp_desc AS sap_sls_grp_desc,
                    recsd.crncy_key AS sap_curr_cd,
                    recsd.prnt_cust_key AS sap_prnt_cust_key,
                    cddes_pck.code_desc AS sap_prnt_cust_desc,
                    recsd.chnl_key AS sap_cust_chnl_key,
                    cddes_chnl.code_desc AS sap_cust_chnl_desc,
                    recsd.sub_chnl_key AS sap_cust_sub_chnl_key,
                    cddes_subchnl.code_desc AS sap_sub_chnl_desc,
                    recsd.go_to_mdl_key AS sap_go_to_mdl_key,
                    cddes_gtm.code_desc AS sap_go_to_mdl_desc,
                    recsd.bnr_key AS sap_bnr_key,
                    cddes_bnrkey.code_desc AS sap_bnr_desc,
                    recsd.bnr_frmt_key AS sap_bnr_frmt_key,
                    cddes_bnrfmt.code_desc AS sap_bnr_frmt_desc,
                    subchnl_retail_env.retail_env,
                    regch.gcgh_region AS gch_region,
                    regch.gcgh_cluster AS gch_cluster,
                    regch.gcgh_subcluster AS gch_subcluster,
                    regch.gcgh_market AS gch_market,
                    regch.gcch_retail_banner AS gch_retail_banner
                FROM ASPEDW_INTEGRATION.edw_gch_customerhierarchy regch,
                    ASPEDW_INTEGRATION.edw_customer_base_dim recbd,
                    ASPEDW_INTEGRATION.edw_company_dim recd,
                    ASPEDW_INTEGRATION.edw_dstrbtn_chnl redc,
                    ASPEDW_INTEGRATION.edw_sales_org_dim resod,
                    (
                        SELECT edw_customer_sales_dim.cust_num,
                            min(
                                (
                                    CASE
                                        WHEN (
                                            (
                                                (edw_customer_sales_dim.cust_del_flag)::text = NULL::text
                                            )
                                            OR (
                                                (edw_customer_sales_dim.cust_del_flag IS NULL)
                                                AND (NULL IS NULL)
                                            )
                                        ) THEN 'O'::character varying
                                        WHEN (
                                            (
                                                (edw_customer_sales_dim.cust_del_flag)::text = ''::text
                                            )
                                            OR (
                                                (edw_customer_sales_dim.cust_del_flag IS NULL)
                                                AND ('' IS NULL)
                                            )
                                        ) THEN 'O'::character varying
                                        ELSE edw_customer_sales_dim.cust_del_flag
                                    END
                                )::text
                            ) AS cust_del_flag
                        FROM ASPEDW_INTEGRATION.edw_customer_sales_dim
                        WHERE (
                                (edw_customer_sales_dim.sls_org)::text = '2100'::text
                            )
                        GROUP BY edw_customer_sales_dim.cust_num
                    ) a,
                    (
                        SELECT DISTINCT edw_customer_sales_dim.cust_num,
                            "max"((edw_customer_sales_dim.dstr_chnl)::text) AS dstr_chnl
                        FROM ASPEDW_INTEGRATION.edw_customer_sales_dim
                        WHERE (
                                (edw_customer_sales_dim.sls_org)::text = '2100'::text
                            )
                        GROUP BY edw_customer_sales_dim.cust_num
                    ) b,
                    (
                        (
                            (
                                (
                                    (
                                        (
                                            ASPEDW_INTEGRATION.edw_customer_sales_dim recsd
                                            LEFT JOIN ASPEDW_INTEGRATION.edw_code_descriptions cddes_pck ON (
                                                (
                                                    (
                                                        (cddes_pck.code)::text = (recsd.prnt_cust_key)::text
                                                    )
                                                    AND (
                                                        (cddes_pck.code_type)::text = 'Parent Customer Key'::text
                                                    )
                                                )
                                            )
                                        )
                                        LEFT JOIN ASPEDW_INTEGRATION.edw_code_descriptions cddes_bnrkey ON (
                                            (
                                                (
                                                    (cddes_bnrkey.code)::text = (recsd.bnr_key)::text
                                                )
                                                AND (
                                                    (cddes_bnrkey.code_type)::text = 'Banner Key'::text
                                                )
                                            )
                                        )
                                    )
                                    LEFT JOIN ASPEDW_INTEGRATION.edw_code_descriptions cddes_bnrfmt ON (
                                        (
                                            (
                                                (cddes_bnrfmt.code)::text = (recsd.bnr_frmt_key)::text
                                            )
                                            AND (
                                                (cddes_bnrfmt.code_type)::text = 'Banner Format Key'::text
                                            )
                                        )
                                    )
                                )
                                LEFT JOIN ASPEDW_INTEGRATION.edw_code_descriptions cddes_chnl ON (
                                    (
                                        ((cddes_chnl.code)::text = (recsd.chnl_key)::text)
                                        AND (
                                            (cddes_chnl.code_type)::text = 'Channel Key'::text
                                        )
                                    )
                                )
                            )
                            LEFT JOIN ASPEDW_INTEGRATION.edw_code_descriptions cddes_gtm ON (
                                (
                                    (
                                        (cddes_gtm.code)::text = (recsd.go_to_mdl_key)::text
                                    )
                                    AND (
                                        (cddes_gtm.code_type)::text = 'Go To Model Key'::text
                                    )
                                )
                            )
                        )
                        LEFT JOIN (
                            ASPEDW_INTEGRATION.edw_code_descriptions cddes_subchnl
                            LEFT JOIN ASPEDW_INTEGRATION.edw_subchnl_retail_env_mapping subchnl_retail_env ON (
                                (
                                    (
                                        upper((subchnl_retail_env.sub_channel)::text) = upper((cddes_subchnl.code_desc)::text)
                                    )
                                )
                            )
                        ) ON (
                            (
                                (
                                    (cddes_subchnl.code)::text = (recsd.sub_chnl_key)::text
                                )
                                AND (
                                    (cddes_subchnl.code_type)::text = 'Sub Channel Key'::text
                                )
                            )
                        )
                    )
                WHERE (
                        (
                            (
                                (
                                    (
                                        (
                                            (
                                                (
                                                    (
                                                        ((regch.customer)::text = (recbd.cust_num)::text)
                                                        AND ((recsd.cust_num)::text = (recbd.cust_num)::text)
                                                    )
                                                    AND ((recsd.cust_num)::text = (b.cust_num)::text)
                                                )
                                                AND (
                                                    (recsd.dstr_chnl)::text = (redc.distr_chan)::text
                                                )
                                            )
                                            AND ((recsd.dstr_chnl)::text = b.dstr_chnl)
                                        )
                                        AND (
                                            (
                                                CASE
                                                    WHEN (
                                                        ((recsd.cust_del_flag)::text = NULL::text)
                                                        OR (
                                                            (recsd.cust_del_flag IS NULL)
                                                            AND (NULL IS NULL)
                                                        )
                                                    ) THEN 'O'::character varying
                                                    WHEN (
                                                        ((recsd.cust_del_flag)::text = ''::text)
                                                        OR (
                                                            (recsd.cust_del_flag IS NULL)
                                                            AND ('' IS NULL)
                                                        )
                                                    ) THEN 'O'::character varying
                                                    ELSE recsd.cust_del_flag
                                                END
                                            )::text = a.cust_del_flag
                                        )
                                    )
                                    AND ((a.cust_num)::text = (recsd.cust_num)::text)
                                )
                                AND ((recsd.sls_org)::text = (resod.sls_org)::text)
                            )
                            AND ((resod.sls_org_co_cd)::text = (recd.co_cd)::text)
                        )
                        AND ((recsd.sls_org)::text = '2100'::text)
                    )
            )
            UNION ALL
            SELECT (ltrim((recbd.cust_num)::text, '0'::text))::character varying AS sap_cust_id,
                recbd.cust_nm AS sap_cust_nm,
                recsd.sls_org AS sap_sls_org,
                recd.company AS sap_cmp_id,
                recd.ctry_key AS sap_cntry_cd,
                recd.ctry_nm AS sap_cntry_nm,
                recbd.addr AS sap_addr,
                recbd.rgn AS sap_region,
                recbd.dstrc AS sap_state_cd,
                recbd.city AS sap_city,
                recbd.pstl_cd AS sap_post_cd,
                recsd.dstr_chnl AS sap_chnl_cd,
                redc.txtsh AS sap_chnl_desc,
                recsd.sls_ofc AS sap_sls_office_cd,
                recsd.sls_ofc_desc AS sap_sls_office_desc,
                recsd.sls_grp AS sap_sls_grp_cd,
                recsd.sls_grp_desc AS sap_sls_grp_desc,
                recsd.crncy_key AS sap_curr_cd,
                recsd.prnt_cust_key AS sap_prnt_cust_key,
                cddes_pck.code_desc AS sap_prnt_cust_desc,
                recsd.chnl_key AS sap_cust_chnl_key,
                cddes_chnl.code_desc AS sap_cust_chnl_desc,
                recsd.sub_chnl_key AS sap_cust_sub_chnl_key,
                cddes_subchnl.code_desc AS sap_sub_chnl_desc,
                recsd.go_to_mdl_key AS sap_go_to_mdl_key,
                cddes_gtm.code_desc AS sap_go_to_mdl_desc,
                recsd.bnr_key AS sap_bnr_key,
                cddes_bnrkey.code_desc AS sap_bnr_desc,
                recsd.bnr_frmt_key AS sap_bnr_frmt_key,
                cddes_bnrfmt.code_desc AS sap_bnr_frmt_desc,
                subchnl_retail_env.retail_env,
                regch.gcgh_region AS gch_region,
                regch.gcgh_cluster AS gch_cluster,
                regch.gcgh_subcluster AS gch_subcluster,
                regch.gcgh_market AS gch_market,
                regch.gcch_retail_banner AS gch_retail_banner
            FROM ASPEDW_INTEGRATION.edw_gch_customerhierarchy regch,
                ASPEDW_INTEGRATION.edw_customer_base_dim recbd,
                ASPEDW_INTEGRATION.edw_company_dim recd,
                ASPEDW_INTEGRATION.edw_dstrbtn_chnl redc,
                ASPEDW_INTEGRATION.edw_sales_org_dim resod,
                (
                    SELECT edw_customer_sales_dim.cust_num,
                        min(
                            (
                                CASE
                                    WHEN (
                                        (
                                            (edw_customer_sales_dim.cust_del_flag)::text = NULL::text
                                        )
                                        OR (
                                            (edw_customer_sales_dim.cust_del_flag IS NULL)
                                            AND (NULL IS NULL)
                                        )
                                    ) THEN 'O'::character varying
                                    WHEN (
                                        (
                                            (edw_customer_sales_dim.cust_del_flag)::text = ''::text
                                        )
                                        OR (
                                            (edw_customer_sales_dim.cust_del_flag IS NULL)
                                            AND ('' IS NULL)
                                        )
                                    ) THEN 'O'::character varying
                                    ELSE edw_customer_sales_dim.cust_del_flag
                                END
                            )::text
                        ) AS cust_del_flag
                    FROM ASPEDW_INTEGRATION.edw_customer_sales_dim
                    WHERE (
                            (edw_customer_sales_dim.sls_org)::text = '2210'::text
                        )
                    GROUP BY edw_customer_sales_dim.cust_num
                ) a,
                (
                    SELECT DISTINCT edw_customer_sales_dim.cust_num,
                        "max"((edw_customer_sales_dim.dstr_chnl)::text) AS dstr_chnl
                    FROM ASPEDW_INTEGRATION.edw_customer_sales_dim
                    WHERE (
                            (edw_customer_sales_dim.sls_org)::text = '2210'::text
                        )
                    GROUP BY edw_customer_sales_dim.cust_num
                ) b,
                (
                    (
                        (
                            (
                                (
                                    (
                                        ASPEDW_INTEGRATION.edw_customer_sales_dim recsd
                                        LEFT JOIN ASPEDW_INTEGRATION.edw_code_descriptions cddes_pck ON (
                                            (
                                                (
                                                    (cddes_pck.code)::text = (recsd.prnt_cust_key)::text
                                                )
                                                AND (
                                                    (cddes_pck.code_type)::text = 'Parent Customer Key'::text
                                                )
                                            )
                                        )
                                    )
                                    LEFT JOIN ASPEDW_INTEGRATION.edw_code_descriptions cddes_bnrkey ON (
                                        (
                                            (
                                                (cddes_bnrkey.code)::text = (recsd.bnr_key)::text
                                            )
                                            AND (
                                                (cddes_bnrkey.code_type)::text = 'Banner Key'::text
                                            )
                                        )
                                    )
                                )
                                LEFT JOIN ASPEDW_INTEGRATION.edw_code_descriptions cddes_bnrfmt ON (
                                    (
                                        (
                                            (cddes_bnrfmt.code)::text = (recsd.bnr_frmt_key)::text
                                        )
                                        AND (
                                            (cddes_bnrfmt.code_type)::text = 'Banner Format Key'::text
                                        )
                                    )
                                )
                            )
                            LEFT JOIN ASPEDW_INTEGRATION.edw_code_descriptions cddes_chnl ON (
                                (
                                    ((cddes_chnl.code)::text = (recsd.chnl_key)::text)
                                    AND (
                                        (cddes_chnl.code_type)::text = 'Channel Key'::text
                                    )
                                )
                            )
                        )
                        LEFT JOIN ASPEDW_INTEGRATION.edw_code_descriptions cddes_gtm ON (
                            (
                                (
                                    (cddes_gtm.code)::text = (recsd.go_to_mdl_key)::text
                                )
                                AND (
                                    (cddes_gtm.code_type)::text = 'Go To Model Key'::text
                                )
                            )
                        )
                    )
                    LEFT JOIN (
                        ASPEDW_INTEGRATION.edw_code_descriptions cddes_subchnl
                        LEFT JOIN ASPEDW_INTEGRATION.edw_subchnl_retail_env_mapping subchnl_retail_env ON (
                            (
                                (
                                    upper((subchnl_retail_env.sub_channel)::text) = upper((cddes_subchnl.code_desc)::text)
                                )
                            )
                        )
                    ) ON (
                        (
                            (
                                (cddes_subchnl.code)::text = (recsd.sub_chnl_key)::text
                            )
                            AND (
                                (cddes_subchnl.code_type)::text = 'Sub Channel Key'::text
                            )
                        )
                    )
                )
            WHERE (
                    (
                        (
                            (
                                (
                                    (
                                        (
                                            (
                                                (
                                                    ((regch.customer)::text = (recbd.cust_num)::text)
                                                    AND ((recsd.cust_num)::text = (recbd.cust_num)::text)
                                                )
                                                AND ((recsd.cust_num)::text = (b.cust_num)::text)
                                            )
                                            AND (
                                                (recsd.dstr_chnl)::text = (redc.distr_chan)::text
                                            )
                                        )
                                        AND ((recsd.dstr_chnl)::text = b.dstr_chnl)
                                    )
                                    AND (
                                        (
                                            CASE
                                                WHEN (
                                                    ((recsd.cust_del_flag)::text = NULL::text)
                                                    OR (
                                                        (recsd.cust_del_flag IS NULL)
                                                        AND (NULL IS NULL)
                                                    )
                                                ) THEN 'O'::character varying
                                                WHEN (
                                                    ((recsd.cust_del_flag)::text = ''::text)
                                                    OR (
                                                        (recsd.cust_del_flag IS NULL)
                                                        AND ('' IS NULL)
                                                    )
                                                ) THEN 'O'::character varying
                                                ELSE recsd.cust_del_flag
                                            END
                                        )::text = a.cust_del_flag
                                    )
                                )
                                AND ((a.cust_num)::text = (recsd.cust_num)::text)
                            )
                            AND ((recsd.sls_org)::text = (resod.sls_org)::text)
                        )
                        AND ((resod.sls_org_co_cd)::text = (recd.co_cd)::text)
                    )
                    AND ((recsd.sls_org)::text = '2210'::text)
                )
        )
        UNION ALL
        SELECT (t.sap_cust_id)::character varying AS sap_cust_id,
            t.sap_cust_nm,
            t.sap_sls_org,
            (t.sap_cmp_id)::character varying AS sap_cmp_id,
            t.sap_cntry_cd,
            t.sap_cntry_nm,
            t.sap_addr,
            t.sap_region,
            t.sap_state_cd,
            t.sap_city,
            t.sap_post_cd,
            t.sap_chnl_cd,
            t.sap_chnl_desc,
            t.sap_sls_office_cd,
            t.sap_sls_office_desc,
            t.sap_sls_grp_cd,
            t.sap_sls_grp_desc,
            t.sap_curr_cd,
            t.sap_prnt_cust_key,
            t.sap_prnt_cust_desc,
            t.sap_cust_chnl_key,
            t.sap_cust_chnl_desc,
            t.sap_cust_sub_chnl_key,
            t.sap_sub_chnl_desc,
            t.sap_go_to_mdl_key,
            t.sap_go_to_mdl_desc,
            t.sap_bnr_key,
            t.sap_bnr_desc,
            t.sap_bnr_frmt_key,
            t.sap_bnr_frmt_desc,
            t.retail_env,
            t.gch_region,
            t.gch_cluster,
            t.gch_subcluster,
            t.gch_market,
            t.gch_retail_banner
        FROM (
                SELECT ltrim((cbd.cust_num)::text, (0)::text) AS sap_cust_id,
                    cbd.cust_nm AS sap_cust_nm,
                    csd.sls_org AS sap_sls_org,
                    ltrim((cd.company)::text, (0)::text) AS sap_cmp_id,
                    cd.ctry_key AS sap_cntry_cd,
                    cd.ctry_nm AS sap_cntry_nm,
                    cbd.addr AS sap_addr,
                    cbd.rgn AS sap_region,
                    cbd.dstrc AS sap_state_cd,
                    cbd.city AS sap_city,
                    cbd.pstl_cd AS sap_post_cd,
                    csd.dstr_chnl AS sap_chnl_cd,
                    dc.txtsh AS sap_chnl_desc,
                    csd.sls_ofc AS sap_sls_office_cd,
                    csd.sls_ofc_desc AS sap_sls_office_desc,
                    csd.sls_grp AS sap_sls_grp_cd,
                    csd.sls_grp_desc AS sap_sls_grp_desc,
                    csd.crncy_key AS sap_curr_cd,
                    csd.prnt_cust_key AS sap_prnt_cust_key,
                    cddes_pck.code_desc AS sap_prnt_cust_desc,
                    csd.chnl_key AS sap_cust_chnl_key,
                    cddes_chnl.code_desc AS sap_cust_chnl_desc,
                    csd.sub_chnl_key AS sap_cust_sub_chnl_key,
                    cddes_subchnl.code_desc AS sap_sub_chnl_desc,
                    csd.go_to_mdl_key AS sap_go_to_mdl_key,
                    cddes_gtm.code_desc AS sap_go_to_mdl_desc,
                    csd.bnr_key AS sap_bnr_key,
                    cddes_bnrkey.code_desc AS sap_bnr_desc,
                    csd.bnr_frmt_key AS sap_bnr_frmt_key,
                    cddes_bnrfmt.code_desc AS sap_bnr_frmt_desc,
                    subchnl_retail_env.retail_env,
                    gch.gcgh_region AS gch_region,
                    gch.gcgh_cluster AS gch_cluster,
                    gch.gcgh_subcluster AS gch_subcluster,
                    gch.gcgh_market AS gch_market,
                    gch.gcch_retail_banner AS gch_retail_banner,
                    row_number() OVER(
                        PARTITION BY ltrim((csd.cust_num)::text, (0)::text)
                        ORDER BY CASE
                                WHEN (
                                    ((csd.cust_del_flag)::text = NULL::text)
                                    OR (
                                        (csd.cust_del_flag IS NULL)
                                        AND (NULL IS NULL)
                                    )
                                ) THEN 'O'::character varying
                                WHEN (
                                    ((csd.cust_del_flag)::text = ''::text)
                                    OR (
                                        (csd.cust_del_flag IS NULL)
                                        AND ('' IS NULL)
                                    )
                                ) THEN 'O'::character varying
                                ELSE csd.cust_del_flag
                            END,
                            csd.sls_org,
                            csd.dstr_chnl
                    ) AS rnk
                FROM (
                        (
                            (
                                (
                                    (
                                        (
                                            (
                                                (
                                                    (
                                                        (
                                                            (
                                                                (
                                                                    ASPEDW_INTEGRATION.edw_customer_sales_dim csd
                                                                    JOIN ASPEDW_INTEGRATION.edw_customer_base_dim cbd ON (
                                                                        (
                                                                            ltrim((csd.cust_num)::text, (0)::text) = ltrim((cbd.cust_num)::text, (0)::text)
                                                                        )
                                                                    )
                                                                )
                                                                LEFT JOIN ASPEDW_INTEGRATION.edw_gch_customerhierarchy gch ON (
                                                                    (
                                                                        ltrim((gch.customer)::text, (0)::text) = ltrim((cbd.cust_num)::text, (0)::text)
                                                                    )
                                                                )
                                                            )
                                                            JOIN ASPEDW_INTEGRATION.edw_dstrbtn_chnl dc ON (
                                                                (
                                                                    ltrim((csd.dstr_chnl)::text, (0)::text) = ltrim((dc.distr_chan)::text, (0)::text)
                                                                )
                                                            )
                                                        )
                                                        JOIN ASPEDW_INTEGRATION.edw_sales_org_dim sod ON (
                                                            (
                                                                (
                                                                    (
                                                                        trim((csd.sls_org)::text) = trim((sod.sls_org)::text)
                                                                    )
                                                                    AND ((sod.ctry_key)::text = 'ID'::text)
                                                                )
                                                                AND (
                                                                    ((sod.sls_org)::text = '2000'::text)
                                                                    OR ((sod.sls_org)::text = '2050'::text)
                                                                )
                                                            )
                                                        )
                                                    )
                                                    JOIN ASPEDW_INTEGRATION.edw_company_dim cd ON (((sod.sls_org_co_cd)::text = (cd.co_cd)::text))
                                                )
                                                LEFT JOIN ASPEDW_INTEGRATION.edw_code_descriptions cddes_pck ON (
                                                    (
                                                        (
                                                            upper((cddes_pck.code_type)::text) = 'PARENT CUSTOMER KEY'::text
                                                        )
                                                        AND (
                                                            (cddes_pck.code)::text = (csd.prnt_cust_key)::text
                                                        )
                                                    )
                                                )
                                            )
                                            LEFT JOIN ASPEDW_INTEGRATION.edw_code_descriptions cddes_bnrkey ON (
                                                (
                                                    (
                                                        upper((cddes_bnrkey.code_type)::text) = 'BANNER KEY'::text
                                                    )
                                                    AND ((cddes_bnrkey.code)::text = (csd.bnr_key)::text)
                                                )
                                            )
                                        )
                                        LEFT JOIN ASPEDW_INTEGRATION.edw_code_descriptions cddes_bnrfmt ON (
                                            (
                                                (
                                                    upper((cddes_bnrfmt.code_type)::text) = 'BANNER FORMAT KEY'::text
                                                )
                                                AND (
                                                    (cddes_bnrfmt.code)::text = (csd.bnr_frmt_key)::text
                                                )
                                            )
                                        )
                                    )
                                    LEFT JOIN ASPEDW_INTEGRATION.edw_code_descriptions cddes_chnl ON (
                                        (
                                            (
                                                upper((cddes_chnl.code_type)::text) = 'CHANNEL KEY'::text
                                            )
                                            AND ((cddes_chnl.code)::text = (csd.chnl_key)::text)
                                        )
                                    )
                                )
                                LEFT JOIN ASPEDW_INTEGRATION.edw_code_descriptions cddes_gtm ON (
                                    (
                                        (
                                            upper((cddes_gtm.code_type)::text) = 'GO TO MODEL KEY'::text
                                        )
                                        AND (
                                            (cddes_gtm.code)::text = (csd.go_to_mdl_key)::text
                                        )
                                    )
                                )
                            )
                            LEFT JOIN ASPEDW_INTEGRATION.edw_code_descriptions cddes_subchnl ON (
                                (
                                    (
                                        upper((cddes_subchnl.code_type)::text) = 'SUB CHANNEL KEY'::text
                                    )
                                    AND (
                                        (cddes_subchnl.code)::text = (csd.sub_chnl_key)::text
                                    )
                                )
                            )
                        )
                        LEFT JOIN ASPEDW_INTEGRATION.edw_subchnl_retail_env_mapping subchnl_retail_env ON (
                            (
                                upper((subchnl_retail_env.sub_channel)::text) = upper((cddes_subchnl.code_desc)::text)
                            )
                        )
                    )
                WHERE (
                        ((csd.sls_org)::text = '2000'::text)
                        OR ((csd.sls_org)::text = '2050'::text)
                    )
            ) t
        WHERE (t.rnk = 1)
    )
)
where sap_cntry_cd='PH';
