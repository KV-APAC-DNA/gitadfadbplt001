create or replace view PCFEDW_INTEGRATION.VW_DEMAND_FORECAST_ANALYSIS(
	PAC_SOURCE_TYPE,
	PAC_SUBSOURCE_TYPE,
	JJ_PERIOD,
	JJ_WEEK_NO,
	JJ_WK,
	JJ_MNTH,
	JJ_MNTH_SHRT,
	JJ_MNTH_LONG,
	JJ_QRTR,
	JJ_YEAR,
	JJ_MNTH_TOT,
	WEEK_DATE,
	MATL_NO,
	MATL_DESC,
	MASTER_CODE,
	PARENT_ID,
	PARENT_MATL_DESC,
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
	CUST_NO,
	CMP_ID,
	CTRY_KEY,
	COUNTRY,
	STATE_CD,
	POST_CD,
	CUST_SUBURB,
	CUST_NM,
	FCST_CHNL,
	FCST_CHNL_DESC,
	SALES_OFFICE_CD,
	SALES_OFFICE_DESC,
	SALES_GRP_CD,
	SALES_GRP_DESC,
	CURR_CD,
	ACTUAL_SALES_QTY,
	APO_TOT_FRCST,
	APO_BASE_FRCST,
	APO_PROMO_FRCST,
	PX_TOT_FRCST,
	PX_BASE_FRCST,
	PX_PROMO_FRCST,
	PROJECTED_APO_TOT_FRCST,
	PROJECTED_PX_TOT_FRCST
) as
(
    SELECT 'SAPBW' AS pac_source_type,
        'SAPBW_APO_FORECAST' AS pac_subsource_type,
        awf.period AS jj_period,
        awf.week_no AS jj_week_no,
        vdt.jj_wk,
        vdt.jj_mnth,
        vdt.jj_mnth_shrt,
        vdt.jj_mnth_long,
        vdt.jj_qrtr,
        vdt.jj_year,
        vdt.jj_mnth_tot,
        awf.week_date,
        (
            ltrim(
                (awf.material)::text,
                ((0)::character varying)::text
            )
        )::character varying AS matl_no,
        vmd.matl_desc,
        mstrcd.master_code,
        (
            ltrim(
                (vapcd.parent_id)::text,
                ((0)::character varying)::text
            )
        )::character varying AS parent_id,
        mstrcd.parent_matl_desc,
        vmd.mega_brnd_cd,
        vmd.mega_brnd_desc,
        vmd.brnd_cd,
        vmd.brnd_desc,
        vmd.base_prod_cd,
        vmd.base_prod_desc,
        vmd.variant_cd,
        vmd.variant_desc,
        vmd.fran_cd,
        vmd.fran_desc,
        vmd.grp_fran_cd,
        vmd.grp_fran_desc,
        vmd.matl_type_cd,
        vmd.matl_type_desc,
        vmd.prod_fran_cd,
        vmd.prod_fran_desc,
        vmd.prod_hier_cd,
        vmd.prod_hier_desc,
        vmd.prod_mjr_cd,
        vmd.prod_mjr_desc,
        vmd.prod_mnr_cd,
        vmd.prod_mnr_desc,
        vmd.mercia_plan,
        vmd.putup_cd,
        vmd.putup_desc,
        vmd.bar_cd,
        NULL AS cust_no,
        vdfcd.cmp_id,
        NULL AS ctry_key,
        vdfcd.country,
        NULL AS state_cd,
        NULL AS post_cd,
        NULL AS cust_suburb,
        NULL AS cust_nm,
        awf.channel AS fcst_chnl,
        vdfcd.fcst_chnl_desc,
        NULL AS sales_office_cd,
        NULL AS sales_office_desc,
        NULL AS sales_grp_cd,
        NULL AS sales_grp_desc,
        NULL AS curr_cd,
        0 AS actual_sales_qty,
        awf.total_fcst AS apo_tot_frcst,
        awf.tot_bas_fct AS apo_base_frcst,
        awf.promo_fcst AS apo_promo_frcst,
        0 AS px_tot_frcst,
        0 AS px_base_frcst,
        0 AS px_promo_frcst,
        CASE
            WHEN (
                awf.period > ((projprd.prev_jj_period)::numeric)::numeric(18, 0)
            ) THEN awf.total_fcst
            ELSE ((0)::numeric)::numeric(18, 0)
        END AS projected_apo_tot_frcst,
        0 AS projected_px_tot_frcst
    FROM PCFEDW_INTEGRATION.edw_time_dim vdt,
        (
            SELECT (
                    to_char(
                        add_months(
                            (
                                to_date(
                                    ((t1.jj_mnth_id)::character varying)::text,
                                    ('YYYYMM'::character varying)::text
                                )
                            )::timestamp without time zone,
                            (- (1)::bigint)
                        ),
                        ('YYYYMM'::character varying)::text
                    )
                )::integer AS prev_jj_period
            FROM PCFEDW_INTEGRATION.edw_time_dim t1
            WHERE (
                    to_date(t1.cal_date) = dateadd(
                        
                           day,1, to_date(current_timestamp())
                        
                    )
                )
        ) projprd,
        (
            SELECT DISTINCT vw_dmnd_frcst_customer_dim.cmp_id,
                vw_dmnd_frcst_customer_dim.country,
                vw_dmnd_frcst_customer_dim.sls_org,
                vw_dmnd_frcst_customer_dim.fcst_chnl,
                vw_dmnd_frcst_customer_dim.fcst_chnl_desc
            FROM PCFEDW_INTEGRATION.vw_dmnd_frcst_customer_dim
        ) vdfcd,
        (
            (
                PCFEDW_INTEGRATION.edw_apo_weekly_forecast_fact awf
                LEFT JOIN PCFEDW_INTEGRATION.edw_material_dim vmd ON (((awf.material)::text = (vmd.matl_id)::text))
            )
            LEFT JOIN (
                PCFEDW_INTEGRATION.vw_apo_parent_child_dim vapcd
                LEFT JOIN (
                    SELECT DISTINCT vw_apo_parent_child_dim.master_code,
                        vw_apo_parent_child_dim.parent_matl_desc
                    FROM PCFEDW_INTEGRATION.vw_apo_parent_child_dim
                    WHERE (
                            (vw_apo_parent_child_dim.cmp_id)::text = ((7470)::character varying)::text
                        )
                    UNION ALL
                    SELECT DISTINCT vw_apo_parent_child_dim.master_code,
                        vw_apo_parent_child_dim.parent_matl_desc
                    FROM PCFEDW_INTEGRATION.vw_apo_parent_child_dim
                    WHERE (
                            NOT (
                                vw_apo_parent_child_dim.master_code IN (
                                    SELECT DISTINCT vw_apo_parent_child_dim.master_code
                                    FROM PCFEDW_INTEGRATION.vw_apo_parent_child_dim
                                    WHERE (
                                            (vw_apo_parent_child_dim.cmp_id)::text = ((7470)::character varying)::text
                                        )
                                )
                            )
                        )
                ) mstrcd ON (
                    (
                        (vapcd.master_code)::text = (mstrcd.master_code)::text
                    )
                )
            ) ON (
                (
                    ((awf.sales_org)::text = (vapcd.sales_org)::text)
                    AND ((awf.material)::text = (vapcd.matl_id)::text)
                )
            )
        )
    WHERE (
            (
                ((awf.week_date)::numeric)::numeric(18, 0) = vdt.time_id
            )
            AND ((awf.channel)::text = (vdfcd.fcst_chnl)::text)
        )
    UNION ALL
    SELECT 'PX' AS pac_source_type,
        'PX_FORECAST' AS pac_subsource_type,
        vdt.jj_mnth_id AS jj_period,
        (
            (
                (
                    ((vdt.jj_year)::character varying)::text || lpad(
                        ((vdt.jj_wk)::character varying)::text,
                        2,
                        ((0)::character varying)::text
                    )
                )
            )::numeric
        )::numeric(18, 0) AS jj_week_no,
        vdt.jj_wk,
        vdt.jj_mnth,
        vdt.jj_mnth_shrt,
        vdt.jj_mnth_long,
        vdt.jj_qrtr,
        vdt.jj_year,
        vdt.jj_mnth_tot,
        (
            to_char(
                (epf.est_date)::timestamp without time zone,
                ('YYYYMMDD'::character varying)::text
            )
        )::character varying AS week_date,
        (
            ltrim(
                (epf.sku_stockcode)::text,
                ((0)::character varying)::text
            )
        )::character varying AS matl_no,
        vmd.matl_desc,
        mstrcd.master_code,
        (
            ltrim(
                (vapcd.parent_id)::text,
                ((0)::character varying)::text
            )
        )::character varying AS parent_id,
        mstrcd.parent_matl_desc,
        vmd.mega_brnd_cd,
        vmd.mega_brnd_desc,
        vmd.brnd_cd,
        vmd.brnd_desc,
        vmd.base_prod_cd,
        vmd.base_prod_desc,
        vmd.variant_cd,
        vmd.variant_desc,
        vmd.fran_cd,
        vmd.fran_desc,
        vmd.grp_fran_cd,
        vmd.grp_fran_desc,
        vmd.matl_type_cd,
        vmd.matl_type_desc,
        vmd.prod_fran_cd,
        vmd.prod_fran_desc,
        vmd.prod_hier_cd,
        vmd.prod_hier_desc,
        vmd.prod_mjr_cd,
        vmd.prod_mjr_desc,
        vmd.prod_mnr_cd,
        vmd.prod_mnr_desc,
        vmd.mercia_plan,
        vmd.putup_cd,
        vmd.putup_desc,
        vmd.bar_cd,
        (
            ltrim(
                (vdfcd.cust_no)::text,
                ((0)::character varying)::text
            )
        )::character varying AS cust_no,
        vdfcd.cmp_id,
        (vdfcd.ctry_key)::character varying AS ctry_key,
        vdfcd.country,
        (vdfcd.state_cd)::character varying AS state_cd,
        (vdfcd.post_cd)::character varying AS post_cd,
        (vdfcd.cust_suburb)::character varying AS cust_suburb,
        (vdfcd.cust_nm)::character varying AS cust_nm,
        vdfcd.fcst_chnl,
        vdfcd.fcst_chnl_desc,
        vdfcd.sales_office_cd,
        vdfcd.sales_office_desc,
        vdfcd.sales_grp_cd,
        vdfcd.sales_grp_desc,
        (vdfcd.curr_cd)::character varying AS curr_cd,
        0 AS actual_sales_qty,
        0 AS apo_tot_frcst,
        0 AS apo_base_frcst,
        0 AS apo_promo_frcst,
        epf.est_estimate AS px_tot_frcst,
        epf.est_normal AS px_base_frcst,
        epf.est_promotional AS px_promo_frcst,
        0 AS projected_apo_tot_frcst,
        CASE
            WHEN (
                vdt.jj_mnth_id > ((projprd.prev_jj_period)::numeric)::numeric(18, 0)
            ) THEN epf.est_estimate
            ELSE 0
        END AS projected_px_tot_frcst
    FROM PCFEDW_INTEGRATION.edw_time_dim vdt,
        (
            SELECT (
                    to_char(
                        add_months(
                            (
                                to_date(
                                    ((t1.jj_mnth_id)::character varying)::text,
                                    ('YYYYMM'::character varying)::text
                                )
                            )::timestamp without time zone,
                            (- (1)::bigint)
                        ),
                        ('YYYYMM'::character varying)::text
                    )
                )::integer AS prev_jj_period
            FROM PCFEDW_INTEGRATION.edw_time_dim t1
            WHERE (
                    to_date(t1.cal_date) = dateadd(
                        
                           day,1, to_date(current_timestamp())
                        
                    )
                )
        ) projprd,
        (
            (
                PCFEDW_INTEGRATION.edw_px_forecast_fact epf
                LEFT JOIN PCFEDW_INTEGRATION.edw_material_dim vmd ON (
                    (
                        lpad(
                            (epf.sku_stockcode)::text,
                            18,
                            ((0)::character varying)::text
                        ) = (vmd.matl_id)::text
                    )
                )
            )
            LEFT JOIN (
                (
                    SELECT DISTINCT epff.ac_attribute,
                        epff.sku_stockcode,
                        vdfcd.cust_no,
                        vdfcd.cmp_id,
                        vdfcd.ctry_key,
                        vdfcd.country,
                        vdfcd.state_cd,
                        vdfcd.post_cd,
                        vdfcd.cust_suburb,
                        vdfcd.cust_nm,
                        vdfcd.fcst_chnl,
                        vdfcd.fcst_chnl_desc,
                        vdfcd.sales_office_cd,
                        vdfcd.sales_office_desc,
                        vdfcd.sales_grp_cd,
                        vdfcd.sales_grp_desc,
                        vdfcd.curr_cd
                    FROM PCFEDW_INTEGRATION.edw_px_forecast_fact epff,
                        PCFEDW_INTEGRATION.vw_dmnd_frcst_customer_dim vdfcd
                    WHERE (
                            lpad(
                                (epff.ac_attribute)::text,
                                10,
                                ((0)::character varying)::text
                            ) = (vdfcd.cust_no)::text
                        )
                ) vdfcd
                LEFT JOIN (
                    PCFEDW_INTEGRATION.vw_apo_parent_child_dim vapcd
                    LEFT JOIN (
                        SELECT DISTINCT vw_apo_parent_child_dim.master_code,
                            vw_apo_parent_child_dim.parent_matl_desc
                        FROM PCFEDW_INTEGRATION.vw_apo_parent_child_dim
                        WHERE (
                                (vw_apo_parent_child_dim.cmp_id)::text = ((7470)::character varying)::text
                            )
                        UNION ALL
                        SELECT DISTINCT vw_apo_parent_child_dim.master_code,
                            vw_apo_parent_child_dim.parent_matl_desc
                        FROM PCFEDW_INTEGRATION.vw_apo_parent_child_dim
                        WHERE (
                                NOT (
                                    vw_apo_parent_child_dim.master_code IN (
                                        SELECT DISTINCT vw_apo_parent_child_dim.master_code
                                        FROM PCFEDW_INTEGRATION.vw_apo_parent_child_dim
                                        WHERE (
                                                (vw_apo_parent_child_dim.cmp_id)::text = ((7470)::character varying)::text
                                            )
                                    )
                                )
                            )
                    ) mstrcd ON (
                        (
                            (vapcd.master_code)::text = (mstrcd.master_code)::text
                        )
                    )
                ) ON (
                    (
                        (
                            lpad(
                                (vdfcd.sku_stockcode)::text,
                                18,
                                ((0)::character varying)::text
                            ) = (vapcd.matl_id)::text
                        )
                        AND ((vdfcd.cmp_id)::text = (vapcd.cmp_id)::text)
                    )
                )
            ) ON (
                (
                    (
                        lpad(
                            (epf.ac_attribute)::text,
                            10,
                            ((0)::character varying)::text
                        ) = (vdfcd.cust_no)::text
                    )
                    AND (
                        (epf.sku_stockcode)::text = (vdfcd.sku_stockcode)::text
                    )
                )
            )
        )
    WHERE (
            (
                (
                    to_char(
                        (epf.est_date)::timestamp without time zone,
                        ('YYYYMMDD'::character varying)::text
                    )
                )::numeric
            )::numeric(18, 0) = vdt.time_id
        )
)
UNION ALL
SELECT 'SAPBW' AS pac_source_type,
    'SAPBW_ACTUAL' AS pac_subsource_type,
    vdt.jj_mnth_id AS jj_period,
    0 AS jj_week_no,
    0 AS jj_wk,
    vdt.jj_mnth,
    vdt.jj_mnth_shrt,
    vdt.jj_mnth_long,
    vdt.jj_qrtr,
    vdt.jj_year,
    vdt.jj_mnth_tot,
    NULL AS week_date,
    (
        ltrim(
            (vsf.matl_id)::text,
            ((0)::character varying)::text
        )
    )::character varying AS matl_no,
    vmd.matl_desc,
    mstrcd.master_code,
    (
        ltrim(
            (vapcd.parent_id)::text,
            ((0)::character varying)::text
        )
    )::character varying AS parent_id,
    mstrcd.parent_matl_desc,
    vmd.mega_brnd_cd,
    vmd.mega_brnd_desc,
    vmd.brnd_cd,
    vmd.brnd_desc,
    vmd.base_prod_cd,
    vmd.base_prod_desc,
    vmd.variant_cd,
    vmd.variant_desc,
    vmd.fran_cd,
    vmd.fran_desc,
    vmd.grp_fran_cd,
    vmd.grp_fran_desc,
    vmd.matl_type_cd,
    vmd.matl_type_desc,
    vmd.prod_fran_cd,
    vmd.prod_fran_desc,
    vmd.prod_hier_cd,
    vmd.prod_hier_desc,
    vmd.prod_mjr_cd,
    vmd.prod_mjr_desc,
    vmd.prod_mnr_cd,
    vmd.prod_mnr_desc,
    vmd.mercia_plan,
    vmd.putup_cd,
    vmd.putup_desc,
    vmd.bar_cd,
    (
        ltrim(
            (vdfcd.cust_no)::text,
            ((0)::character varying)::text
        )
    )::character varying AS cust_no,
    vdfcd.cmp_id,
    (vdfcd.ctry_key)::character varying AS ctry_key,
    vdfcd.country,
    (vdfcd.state_cd)::character varying AS state_cd,
    (vdfcd.post_cd)::character varying AS post_cd,
    (vdfcd.cust_suburb)::character varying AS cust_suburb,
    (vdfcd.cust_nm)::character varying AS cust_nm,
    vdfcd.fcst_chnl,
    vdfcd.fcst_chnl_desc,
    vdfcd.sales_office_cd,
    vdfcd.sales_office_desc,
    vdfcd.sales_grp_cd,
    vdfcd.sales_grp_desc,
    (vdfcd.curr_cd)::character varying AS curr_cd,
    vsf.sales_qty AS actual_sales_qty,
    0 AS apo_tot_frcst,
    0 AS apo_base_frcst,
    0 AS apo_promo_frcst,
    0 AS px_tot_frcst,
    0 AS px_base_frcst,
    0 AS px_promo_frcst,
    CASE
        WHEN (
            vdt.jj_mnth_id <= ((projprd.prev_jj_period)::numeric)::numeric(18, 0)
        ) THEN vsf.sales_qty
        ELSE ((0)::numeric)::numeric(18, 0)
    END AS projected_apo_tot_frcst,
    CASE
        WHEN (
            vdt.jj_mnth_id <= ((projprd.prev_jj_period)::numeric)::numeric(18, 0)
        ) THEN vsf.sales_qty
        ELSE ((0)::numeric)::numeric(18, 0)
    END AS projected_px_tot_frcst
FROM (
        SELECT DISTINCT edw_time_dim.jj_mnth_id,
            edw_time_dim.jj_mnth,
            edw_time_dim.jj_mnth_shrt,
            edw_time_dim.jj_mnth_long,
            edw_time_dim.jj_qrtr,
            edw_time_dim.jj_year,
            edw_time_dim.jj_mnth_tot
        FROM PCFEDW_INTEGRATION.edw_time_dim
    ) vdt,
    (
        SELECT (
                to_char(
                    add_months(
                        (
                            to_date(
                                ((t1.jj_mnth_id)::character varying)::text,
                                ('YYYYMM'::character varying)::text
                            )
                        )::timestamp without time zone,
                        (- (1)::bigint)
                    ),
                    ('YYYYMM'::character varying)::text
                )
            )::integer AS prev_jj_period
        FROM PCFEDW_INTEGRATION.edw_time_dim t1
        WHERE (
                to_date(t1.cal_date) = dateadd(
                        
                           day,1, to_date(current_timestamp())
                        
                    )
            )
    ) projprd,
    (
        (
            (
                PCFEDW_INTEGRATION.vw_sapbw_fact vsf
                LEFT JOIN PCFEDW_INTEGRATION.edw_material_dim vmd ON (((vsf.matl_id)::text = (vmd.matl_id)::text))
            )
            LEFT JOIN PCFEDW_INTEGRATION.vw_dmnd_frcst_customer_dim vdfcd ON (
                (
                    ((vsf.cust_no)::text = (vdfcd.cust_no)::text)
                    AND ((vsf.cmp_id)::text = (vdfcd.cmp_id)::text)
                )
            )
        )
        LEFT JOIN (
            PCFEDW_INTEGRATION.vw_apo_parent_child_dim vapcd
            LEFT JOIN (
                SELECT DISTINCT vw_apo_parent_child_dim.master_code,
                    vw_apo_parent_child_dim.parent_matl_desc
                FROM PCFEDW_INTEGRATION.vw_apo_parent_child_dim
                WHERE (
                        (vw_apo_parent_child_dim.cmp_id)::text = ((7470)::character varying)::text
                    )
                UNION ALL
                SELECT DISTINCT vw_apo_parent_child_dim.master_code,
                    vw_apo_parent_child_dim.parent_matl_desc
                FROM PCFEDW_INTEGRATION.vw_apo_parent_child_dim
                WHERE (
                        NOT (
                            vw_apo_parent_child_dim.master_code IN (
                                SELECT DISTINCT vw_apo_parent_child_dim.master_code
                                FROM PCFEDW_INTEGRATION.vw_apo_parent_child_dim
                                WHERE (
                                        (vw_apo_parent_child_dim.cmp_id)::text = ((7470)::character varying)::text
                                    )
                            )
                        )
                    )
            ) mstrcd ON (
                (
                    (vapcd.master_code)::text = (mstrcd.master_code)::text
                )
            )
        ) ON (
            (
                ((vsf.matl_id)::text = (vapcd.matl_id)::text)
                AND ((vsf.cmp_id)::text = (vapcd.cmp_id)::text)
            )
        )
    )
WHERE (
        ((vsf.jj_month_id)::numeric)::numeric(18, 0) = vdt.jj_mnth_id
    );
