CREATE OR REPLACE TABLE PCFITG_INTEGRATION.ITG_CHW_ECOMM_DATA (		--// CREATE OR REPLACE TABLE PCFITG_INTEGRATION.itg_chw_ecomm_data (
    product_probe_id varchar(20),		--//  ENCODE zstd // character varying
    product_name varchar(100),		--//  ENCODE zstd // character varying
    nec1_desc varchar(100),		--//  ENCODE zstd // character varying
    nec2_desc varchar(100),		--//  ENCODE zstd // character varying
    nec3_desc varchar(100),		--//  ENCODE zstd // character varying
    brand varchar(50),		--//  ENCODE zstd // character varying
    category varchar(50),		--//  ENCODE zstd // character varying
		owner varchar(20),
    manufacturer varchar(50),		--//  ENCODE zstd // character varying
    mat_year varchar(10),		--//  ENCODE zstd // character varying
    time_period varchar(10),		--//  ENCODE zstd // character varying
    week_end_dt date,		--//  ENCODE az64
    sales_value numeric(10,2),		--//  ENCODE az64
    sales_qty numeric(10,2),		--//  ENCODE az64
    crncy varchar(3),		--//  ENCODE zstd // character varying
    file_name varchar(50),		--//  ENCODE zstd // character varying
    crt_dttm timestamp without time zone,		--//  ENCODE az64
    updt_dttm timestamp without time zone		--//  ENCODE az64
)
		--// DISTSTYLE EVEN
;		--// SORTKEY ( week_end_dt, product_probe_id );

CREATE OR REPLACE TABLE PCFEDW_INTEGRATION.EDW_PHARMACY_ECOMMERCE_ANALYSIS (		--// CREATE OR REPLACE TABLE PCFEDW_INTEGRATION.edw_pharmacy_ecommerce_analysis (
    week_end_dt date,		--//  ENCODE az64
    jj_year numeric(18,0),		--//  ENCODE az64
    jj_qrtr numeric(18,0),		--//  ENCODE az64
    jj_mnth numeric(18,0),		--//  ENCODE az64
    jj_wk numeric(18,0),		--//  ENCODE az64
    jj_mnth_id numeric(18,0),		--//  ENCODE az64
    jj_mnth_tot numeric(18,0),		--//  ENCODE az64
    jj_mnth_day numeric(18,0),		--//  ENCODE az64
    jj_mnth_shrt varchar(3),		--//  ENCODE lzo // character varying
    jj_mnth_long varchar(10),		--//  ENCODE lzo // character varying
    cal_year numeric(18,0),		--//  ENCODE az64
    cal_qrtr numeric(18,0),		--//  ENCODE az64
    cal_mnth numeric(18,0),		--//  ENCODE az64
    cal_mnth_id numeric(18,0),		--//  ENCODE az64
    cal_mnth_nm varchar(10),		--//  ENCODE lzo // character varying
    prod_key numeric(10,0),		--//  ENCODE az64
    product_probe_id varchar(20),		--//  ENCODE lzo // character varying
    prod_desc varchar(100),		--//  ENCODE lzo // character varying
    prod_sapbw_code varchar(50),		--//  ENCODE lzo // character varying
    prod_ean varchar(50),		--//  ENCODE lzo // character varying
    prod_jj_franchise varchar(100),		--//  ENCODE lzo // character varying
    prod_jj_category varchar(100),		--//  ENCODE lzo // character varying
    iqvia_prod_category varchar(50),		--//  ENCODE lzo // character varying
    prod_jj_brand varchar(100),		--//  ENCODE lzo // character varying
    prod_sap_franchise varchar(100),		--//  ENCODE lzo // character varying
    prod_sap_profit_centre varchar(100),		--//  ENCODE lzo // character varying
    prod_sap_product_major varchar(100),		--//  ENCODE lzo // character varying
    prod_grocery_franchise varchar(100),		--//  ENCODE lzo // character varying
    prod_grocery_category varchar(100),		--//  ENCODE lzo // character varying
    prod_grocery_brand varchar(100),		--//  ENCODE lzo // character varying
    prod_pbs varchar(100),		--//  ENCODE lzo // character varying
    prod_ims_brand varchar(100),		--//  ENCODE lzo // character varying
    prod_nz_code varchar(100),		--//  ENCODE lzo // character varying
    gcph_franchise varchar(30),		--//  ENCODE lzo // character varying
    gcph_brand varchar(30),		--//  ENCODE lzo // character varying
    gcph_subbrand varchar(100),		--//  ENCODE lzo // character varying
    gcph_variant varchar(100),		--//  ENCODE lzo // character varying
    gcph_needstate varchar(50),		--//  ENCODE lzo // character varying
    gcph_category varchar(50),		--//  ENCODE lzo // character varying
    gcph_subcategory varchar(50),		--//  ENCODE lzo // character varying
    gcph_segment varchar(50),		--//  ENCODE lzo // character varying
    gcph_subsegment varchar(100),		--//  ENCODE lzo // character varying
    cust_group varchar(10),		--//  ENCODE lzo // character varying
    ecomm_cust varchar(17),		--//  ENCODE lzo // character varying
		owner varchar(20),
    manufacturer varchar(50),		--//  ENCODE lzo // character varying
    unit_online numeric(10,2),		--//  ENCODE az64
    aud_sales_online numeric(10,2),		--//  ENCODE az64
    exch_rate_to_usd numeric(15,5),		--//  ENCODE az64
    usd_sales_online numeric(26,7)		--//  ENCODE az64
)
;		--// DISTSTYLE AUTO;

CREATE OR REPLACE TABLE PCFEDW_INTEGRATION.EDW_AU_PHARM_ECOMM_FACT (		--// CREATE OR REPLACE TABLE PCFEDW_INTEGRATION.edw_au_pharm_ecomm_fact (
    cust_group varchar(10),		--//  ENCODE zstd // character varying
    product_probe_id varchar(20),		--//  ENCODE zstd // character varying
    product_name varchar(100),		--//  ENCODE zstd // character varying
    nec1_desc varchar(100),		--//  ENCODE zstd // character varying
    nec2_desc varchar(100),		--//  ENCODE zstd // character varying
    nec3_desc varchar(100),		--//  ENCODE zstd // character varying
    brand varchar(50),		--//  ENCODE zstd // character varying
    category varchar(50),		--//  ENCODE zstd // character varying
		owner varchar(20),
    manufacturer varchar(50),		--//  ENCODE zstd // character varying
    mat_year varchar(10),		--//  ENCODE zstd // character varying
    time_period varchar(10),		--//  ENCODE zstd // character varying
    week_end_dt date,		--//  ENCODE az64
    sales_value numeric(10,2),		--//  ENCODE az64
    sales_qty numeric(10,2),		--//  ENCODE az64
    crncy varchar(3),		--//  ENCODE zstd // character varying
    updt_dttm timestamp without time zone		--//  ENCODE az64
)
		--// DISTSTYLE EVEN
;		--// SORTKEY ( week_end_dt, product_probe_id );

CREATE OR REPLACE TABLE PCFWKS_INTEGRATION.WKS_CHW_ECOMM_DATA (		--// CREATE OR REPLACE TABLE PCFWKS_INTEGRATION.wks_chw_ecomm_data (
    pfc varchar(20),		--//  ENCODE zstd // character varying
    skuname varchar(100),		--//  ENCODE zstd // character varying
    nec1_desc varchar(100),		--//  ENCODE zstd // character varying
    nec2_desc varchar(100),		--//  ENCODE zstd // character varying
    nec3_desc varchar(100),		--//  ENCODE zstd // character varying
    brand varchar(50),		--//  ENCODE zstd // character varying
	  owner varchar(20),
    manufacturer varchar(50),		--//  ENCODE zstd // character varying
    category varchar(50),		--//  ENCODE zstd // character varying
    mat_year varchar(10),		--//  ENCODE zstd // character varying
    periodid varchar(10),		--//  ENCODE zstd // character varying
    sales_online varchar(10),		--//  ENCODE zstd // character varying
    unit_online varchar(10),		--//  ENCODE zstd // character varying
    week_end varchar(20),		--//  ENCODE zstd // character varying
    file_name varchar(50)		--//  ENCODE zstd // character varying
)
;		--// DISTSTYLE AUTO;

CREATE OR REPLACE TABLE PCFITG_INTEGRATION.ITG_NATIONAL_ECOMM_DATA (		--// CREATE OR REPLACE TABLE PCFITG_INTEGRATION.itg_national_ecomm_data (
    product_probe_id varchar(20),		--//  ENCODE zstd // character varying
    product_name varchar(100),		--//  ENCODE zstd // character varying
    nec1_desc varchar(100),		--//  ENCODE zstd // character varying
    nec2_desc varchar(100),		--//  ENCODE zstd // character varying
    nec3_desc varchar(100),		--//  ENCODE zstd // character varying
    brand varchar(50),		--//  ENCODE zstd // character varying
    category varchar(50),		--//  ENCODE zstd // character varying
		owner varchar(20),
    manufacturer varchar(50),		--//  ENCODE zstd // character varying
    mat_year varchar(10),		--//  ENCODE zstd // character varying
    time_period varchar(10),		--//  ENCODE zstd // character varying
    week_end_dt date,		--//  ENCODE az64
    sales_value numeric(10,2),		--//  ENCODE az64
    sales_qty numeric(10,2),		--//  ENCODE az64
    crncy varchar(3),		--//  ENCODE zstd // character varying
    file_name varchar(50),		--//  ENCODE zstd // character varying
    crt_dttm timestamp without time zone,		--//  ENCODE az64
    updt_dttm timestamp without time zone		--//  ENCODE az64
)
		--// DISTSTYLE EVEN
;		--// SORTKEY ( week_end_dt, product_probe_id );



create or replace view PCFEDW_INTEGRATION.VW_CUSTOMER_DIM(
	CUST_NO,
	CMP_ID,
	CHANNEL_CD,
	CHANNEL_DESC,
	CTRY_KEY,
	COUNTRY,
	STATE_CD,
	POST_CD,
	CUST_SUBURB,
	CUST_NM,
	SLS_ORG,
	CUST_DEL_FLAG,
	SALES_OFFICE_CD,
	SALES_OFFICE_DESC,
	SALES_GRP_CD,
	SALES_GRP_DESC,
	MERCIA_REF,
	CURR_CD
) as

SELECT cust.cust_no,
    pac_lkup.cmp_id,
    pac_lkup.chnl_cd AS channel_cd,
    pac_lkup.chnl_desc AS channel_desc,
    cust.ctry_key,
    pac_lkup.country,
    cust.state_cd,
    cust.post_cd,
    cust.cust_suburb,
    cust.cust_nm,
    pac_lkup.sls_org,
    cust.cust_del_flag,
    pac_lkup.sls_ofc AS sales_office_cd,
    pac_lkup.sls_ofc_desc AS sales_office_desc,
    pac_lkup.sls_grp AS sales_grp_cd,
    pac_lkup.sls_grp_desc AS sales_grp_desc,
    cust.mercia_ref,
    cust.curr_cd
FROM (
        (
            SELECT customer.cust_no,
                min((customer.cmp_id)::text) AS cmp_id,
                min((customer.channel_cd)::text) AS channel_cd,
                min((customer.channel_desc)::text) AS channel_desc,
                min((customer.ctry_key)::text) AS ctry_key,
                min((customer.country)::text) AS country,
                min((customer.state_cd)::text) AS state_cd,
                min((customer.post_cd)::text) AS post_cd,
                min((customer.cust_suburb)::text) AS cust_suburb,
                min((customer.cust_nm)::text) AS cust_nm,
                min((customer.sls_org)::text) AS sls_org,
                min((customer.cust_del_flag)::text) AS cust_del_flag,
                min((customer.sales_office_cd)::text) AS sales_office_cd,
                min((customer.sales_office_desc)::text) AS sales_office_desc,
                min((customer.sales_grp_cd)::text) AS sales_grp_cd,
                min((customer.sales_grp_desc)::text) AS sales_grp_desc,
                min((customer.mercia_ref)::text) AS mercia_ref,
                min((customer.curr_cd)::text) AS curr_cd
            FROM (
                    SELECT DISTINCT cust.cust_num AS cust_no,
                        pac_lkup.cmp_id,
                        pac_lkup.chnl_cd AS channel_cd,
                        pac_lkup.chnl_desc AS channel_desc,
                        cust.ctry_key,
                        pac_lkup.country,
                        cust.rgn AS state_cd,
                        cust.pstl_cd AS post_cd,
                        cust.city AS cust_suburb,
                        cust.cust_nm,
                        pac_lkup.sls_org,
                        cust_sales.cust_del_flag,
                        pac_lkup.sls_ofc AS sales_office_cd,
                        pac_lkup.sls_ofc_desc AS sales_office_desc,
                        CASE
                            WHEN (
                                ltrim((cust.cust_num)::text, '0'::text) = ltrim((control.cust_no)::text, '0'::text)
                            ) THEN control.sls_grp
                            ELSE pac_lkup.sls_grp
                        END AS sales_grp_cd,
                        pac_lkup.sls_grp_desc AS sales_grp_desc,
                        cust.fcst_chnl AS mercia_ref,
                        cust_sales.crncy_key AS curr_cd
                    FROM pcfedw_integration.customer_control_tp_accrual_reversal_ac control,
                        aspedw_integration.edw_customer_base_dim cust,
                        (
                            SELECT a.cust_num,
                                min(
                                    (
                                        CASE
                                            WHEN (
                                                (
                                                    (a.cust_del_flag)::text = (NULL::character varying)::text
                                                )
                                                OR (
                                                    (a.cust_del_flag IS NULL)
                                                    AND (NULL IS NULL)
                                                )
                                            ) THEN 'O'::character varying
                                            WHEN (
                                                (
                                                    (a.cust_del_flag)::text = (''::character varying)::text
                                                )
                                                OR (
                                                    (a.cust_del_flag IS NULL)
                                                    AND ('' IS NULL)
                                                )
                                            ) THEN 'O'::character varying
                                            ELSE a.cust_del_flag
                                        END
                                    )::text
                                ) AS cust_del_flag
                            FROM aspedw_integration.edw_customer_sales_dim a,
                                (
                                    SELECT DISTINCT dly_sls_cust_attrb_lkp.sls_org
                                    FROM pcfedw_integration.dly_sls_cust_attrb_lkp
                                ) b
                            WHERE ((a.sls_org)::text = (b.sls_org)::text)
                            GROUP BY a.cust_num
                        ) req_cust_rec,
                        (
                            aspedw_integration.edw_customer_sales_dim cust_sales
                            LEFT JOIN pcfedw_integration.dly_sls_cust_attrb_lkp pac_lkup ON (
                                (
                                    (cust_sales.sls_grp)::text = (pac_lkup.sls_grp)::text
                                )
                            )
                        )
                    WHERE (
                            (
                                (
                                    (cust_sales.cust_num)::text = (cust.cust_num)::text
                                )
                                AND (
                                    (cust_sales.cust_num)::text = (req_cust_rec.cust_num)::text
                                )
                            )
                            AND (
                                (
                                    CASE
                                        WHEN (
                                            (
                                                (cust_sales.cust_del_flag)::text = (NULL::character varying)::text
                                            )
                                            OR (
                                                (cust_sales.cust_del_flag IS NULL)
                                                AND (NULL IS NULL)
                                            )
                                        ) THEN 'O'::character varying
                                        WHEN (
                                            (
                                                (cust_sales.cust_del_flag)::text = (''::character varying)::text
                                            )
                                            OR (
                                                (cust_sales.cust_del_flag IS NULL)
                                                AND ('' IS NULL)
                                            )
                                        ) THEN 'O'::character varying
                                        ELSE cust_sales.cust_del_flag
                                    END
                                )::text = req_cust_rec.cust_del_flag
                            )
                        )
                ) customer
            GROUP BY customer.cust_no
        ) cust
        LEFT JOIN pcfedw_integration.dly_sls_cust_attrb_lkp pac_lkup ON (
            (
                ((cust.sales_grp_cd)::character varying)::text = (pac_lkup.sls_grp)::text
            )
        )
    )
WHERE (
        (
            ltrim(
                (cust.cust_no)::text,
                ('0'::character varying)::text
            ) !LIKE ('7%'::character varying)::text
        )
        OR (
            cust.cust_no IN (
                SELECT DISTINCT customer_control_tp_accrual_reversal_ac.cust_no
                FROM pcfedw_integration.customer_control_tp_accrual_reversal_ac
            )
        )
    );

create or replace view PCFEDW_INTEGRATION.VW_MATERIAL_DIM(
	MATL_ID,
	MATL_DESC,
	MEGA_BRND_CD,
	MEGA_BRND_DESC,
	BRND_CD,
	BRND_DESC,
	BASE_PROD_CD,
	BASE_PROD_DESC,
	VARIANT_CD,
	VARIANT_DESC,
	FRAN_CD,
	FRAN_DESC,
	GRP_FRAN_CD,
	GRP_FRAN_DESC,
	MATL_TYPE_CD,
	MATL_TYPE_DESC,
	PROD_FRAN_CD,
	PROD_FRAN_DESC,
	PROD_HIER_CD,
	PROD_HIER_DESC,
	PROD_MJR_CD,
	PROD_MJR_DESC,
	PROD_MNR_CD,
	PROD_MNR_DESC,
	MERCIA_PLAN,
	PUTUP_CD,
	PUTUP_DESC,
	BAR_CD,
	PRFT_CTR,
	UPDT_DT
) as SELECT DISTINCT 
    emd.matl_num AS matl_id,
    emd.matl_desc,
    emd.mega_brnd_cd,
    emd.mega_brnd_desc,
    emd.brnd_cd,
    emd.brnd_desc,
    emd.prod_base AS base_prod_cd,
    emd.base_prod_desc,
    emd.vrnt AS variant_cd,
    emd.varnt_desc AS variant_desc,
    emd.prodh3 AS fran_cd,
    emd.prodh3_txtmd AS fran_desc,
    emd.prodh2 AS grp_fran_cd,
    emd.prodh2_txtmd AS grp_fran_desc,
    emd.matl_type_cd,
    emd.matl_type_desc,
    emd.prodh4 AS prod_fran_cd,
    emd.prodh4_txtmd AS prod_fran_desc,
    emd.prod_hier_cd,
    emd.prodh6_txtmd AS prod_hier_desc,
    emd.prodh5 AS prod_mjr_cd,
    emd.prodh5_txtmd AS prod_mjr_desc,
    emd.prodh5 AS prod_mnr_cd,
    emd.prodh5_txtmd AS prod_mnr_desc,
    emd.mercia_plan,
    emd.put_up AS putup_cd,
    emd.put_up_desc AS putup_desc,
    emd.prmry_upc_cd AS bar_cd,
    NULL::text AS prft_ctr,
    current_timestamp()::timestamp_ntz(9) AS updt_dt
FROM aspedw_integration.edw_material_dim emd,
    aspedw_integration.edw_material_plant_dim empd
WHERE (
        (
            (
                (
                    (emd.matl_num)::text = (empd.matl_plnt_view)::text
                )
                AND (
                    (
                        (
                            ((empd.plnt)::text = '3300'::text)
                            OR ((empd.plnt)::text = '3410'::text)
                        )
                        OR ((empd.plnt)::text = '330A'::text)
                    )
                    OR ((empd.plnt)::text = '341A'::text)
                )
            )
            AND ((emd.prod_hier_cd)::text <> ''::text)
        )
        AND (
            (
                (
                    (
                        ((emd.matl_type_cd)::text = 'FERT'::text)
                        OR ((emd.matl_type_cd)::text = 'HALB'::text)
                    )
                    OR ((emd.matl_type_cd)::text = 'PROM'::text)
                )
                OR ((emd.matl_type_cd)::text = 'SAPR'::text)
            )
            OR ((emd.matl_type_cd)::text = 'ROH'::text)
        )
    );
