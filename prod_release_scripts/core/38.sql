USE SCHEMA PCFEDW_INTEGRATION;

create or replace view EDW_VW_MDS_COGS_RATE_DIM(
	JJ_YEAR,
	JJ_MNTH_ID,
	MATL_ID,
	CRNCY,
	SGD_COGS_PER_UNIT,
	EX_RT,
	COGS_PER_UNIT
) as

SELECT extd.jj_year,
    extd.jj_mnth_id,
    (ltrim((cogs.sku)::text, (0)::text))::character varying AS matl_id,
    extd.crncy,
    (cogs.nz_cogs_per_unit)::numeric(38, 5) AS sgd_cogs_per_unit,
    extd.ex_rt,
    ((cogs.nz_cogs_per_unit * extd.ex_rt))::numeric(38, 5) AS cogs_per_unit
FROM (
        (
            SELECT itg.jj_year,
                etd.jj_mnth_id,
                itg.sku,
                itg.nz_cogs_per_unit
            FROM (
                    (
                        SELECT itg_mds_cogs_master_dim.jj_year,
                            itg_mds_cogs_master_dim.sku,
                            itg_mds_cogs_master_dim.crncy,
                            itg_mds_cogs_master_dim.au_cogs_per_unit,
                            itg_mds_cogs_master_dim.nz_cogs_per_unit,
                            itg_mds_cogs_master_dim.crt_dttm
                        FROM PCFITG_INTEGRATION.itg_mds_cogs_master_dim
                        WHERE (
                                itg_mds_cogs_master_dim.nz_cogs_per_unit <> (0)::numeric(31, 2)
                            )
                    ) itg
                    LEFT JOIN (
                        SELECT DISTINCT edw_time_dim.jj_mnth_id,
                            (edw_time_dim.jj_year)::character varying AS jj_year
                        FROM PCFEDW_INTEGRATION.edw_time_dim
                    ) etd ON (((itg.jj_year)::text = (etd.jj_year)::text))
                )
        ) cogs
        JOIN (
            SELECT etd.jj_year,
                etd.jj_mnth_id,
                exch.to_crncy AS crncy,
                exch.ex_rt
            FROM (
                    (
                        SELECT edw_crncy_exch.ex_rt_typ,
                            edw_crncy_exch.from_crncy,
                            edw_crncy_exch.to_crncy,
                            (
                                (
                                    (99999999)::numeric - (edw_crncy_exch.vld_from)::numeric
                                )
                            )::numeric(18, 0) AS valid_date,
                            edw_crncy_exch.ex_rt
                        FROM ASPEDW_INTEGRATION.edw_crncy_exch
                        WHERE (
                                (
                                    ((edw_crncy_exch.from_crncy)::text = 'SGD'::text)
                                    AND ((edw_crncy_exch.to_crncy)::text = 'NZD'::text)
                                )
                                AND ((edw_crncy_exch.ex_rt_typ)::text = 'BWAR'::text)
                            )
                    ) exch
                    JOIN (
                        SELECT edw_time_dim.jj_mnth_id,
                            (edw_time_dim.jj_year)::character varying AS jj_year,
                            edw_time_dim.time_id
                        FROM PCFEDW_INTEGRATION.edw_time_dim
                    ) etd ON ((exch.valid_date = etd.time_id))
                )
        ) extd ON (
            (
                (extd.jj_mnth_id = cogs.jj_mnth_id)
                AND ((extd.jj_year)::text = (cogs.jj_year)::text)
            )
        )
    )
UNION ALL
SELECT extd.jj_year,
    extd.jj_mnth_id,
    (ltrim((cogs.sku)::text, (0)::text))::character varying AS matl_id,
    extd.crncy,
    (cogs.au_cogs_per_unit)::numeric(38, 5) AS sgd_cogs_per_unit,
    extd.ex_rt,
    ((cogs.au_cogs_per_unit * extd.ex_rt))::numeric(38, 5) AS cogs_per_unit
FROM (
        (
            SELECT itg.jj_year,
                etd.jj_mnth_id,
                itg.sku,
                itg.au_cogs_per_unit
            FROM (
                    (
                        SELECT itg_mds_cogs_master_dim.jj_year,
                            itg_mds_cogs_master_dim.sku,
                            itg_mds_cogs_master_dim.crncy,
                            itg_mds_cogs_master_dim.au_cogs_per_unit,
                            itg_mds_cogs_master_dim.nz_cogs_per_unit,
                            itg_mds_cogs_master_dim.crt_dttm
                        FROM PCFITG_INTEGRATION.itg_mds_cogs_master_dim
                        WHERE (
                                itg_mds_cogs_master_dim.au_cogs_per_unit <> (0)::numeric(31, 2)
                            )
                    ) itg
                    LEFT JOIN (
                        SELECT DISTINCT edw_time_dim.jj_mnth_id,
                            (edw_time_dim.jj_year)::character varying AS jj_year
                        FROM PCFEDW_INTEGRATION.edw_time_dim
                    ) etd ON (((itg.jj_year)::text = (etd.jj_year)::text))
                )
        ) cogs
        JOIN (
            SELECT a.jj_year,
                mnth.jj_mnth_id,
                a.crncy,
                a.ex_rt
            FROM (
                    (
                        SELECT etd.jj_year,
                            etd.jj_mnth_id,
                            exch.to_crncy AS crncy,
                            exch.ex_rt
                        FROM (
                                (
                                    SELECT edw_crncy_exch.ex_rt_typ,
                                        edw_crncy_exch.from_crncy,
                                        edw_crncy_exch.to_crncy,
                                        (
                                            (
                                                (99999999)::numeric - (edw_crncy_exch.vld_from)::numeric
                                            )
                                        )::numeric(18, 0) AS valid_date,
                                        edw_crncy_exch.ex_rt
                                    FROM ASPEDW_INTEGRATION.edw_crncy_exch
                                    WHERE (
                                            (
                                                ((edw_crncy_exch.from_crncy)::text = 'SGD'::text)
                                                AND ((edw_crncy_exch.to_crncy)::text = 'AUD'::text)
                                            )
                                            AND ((edw_crncy_exch.ex_rt_typ)::text = 'BWAR'::text)
                                        )
                                ) exch
                                JOIN (
                                    SELECT edw_time_dim.jj_mnth_id,
                                        (edw_time_dim.jj_year)::character varying AS jj_year,
                                        edw_time_dim.time_id
                                    FROM PCFEDW_INTEGRATION.edw_time_dim
                                ) etd ON ((exch.valid_date = etd.time_id))
                            )
                    ) a
                    JOIN (
                        SELECT DISTINCT edw_time_dim.jj_mnth_id,
                            (edw_time_dim.jj_year)::character varying AS jj_year
                        FROM PCFEDW_INTEGRATION.edw_time_dim
                    ) mnth ON (((mnth.jj_year)::text = (a.jj_year)::text))
                )
        ) extd ON (
            (
                (extd.jj_mnth_id = cogs.jj_mnth_id)
                AND ((extd.jj_year)::text = (cogs.jj_year)::text)
            )
        )
    );
create or replace view EDW_VW_TERMS_MASTER(
	COUNTRY,
	SLS_GRP_CD,
	SLS_GRP_NM,
	CUST_ID,
	CUST_NM,
	TERMS_PERCENTAGE
) as
SELECT terms.country,
    terms.sls_grp_cd,
    terms.sls_grp_nm,
    (terms.cust_id)::character varying AS cust_id,
    (terms.cust_nm)::character varying AS cust_nm,
    terms.terms_percentage
FROM (
        SELECT imptm.country,
            imptm.sls_grp_cd,
            imptm.sls_grp_nm,
            ltrim(
                (imptm.sls_grp_cd)::text,
                ('0'::character varying)::text
            ) AS cust_id,
            vcd.cust_nm,
            imptm.terms_percentage
        FROM PCFITG_INTEGRATION.itg_mds_pacific_terms_master imptm,
            PCFEDW_integration.vw_customer_dim vcd
        WHERE (
                (imptm.sls_grp_cd)::text = ltrim(
                    (vcd.cust_no)::text,
                    ('0'::character varying)::text
                )
            )
    ) terms
UNION ALL
SELECT imptm.country,
    imptm.sls_grp_cd,
    imptm.sls_grp_nm,
    (
        ltrim(
            (vcd.cust_no)::text,
            ('0'::character varying)::text
        )
    )::character varying AS cust_id,
    (vcd.cust_nm)::character varying AS cust_nm,
    imptm.terms_percentage
FROM (
        (
            SELECT itg_mds_pacific_terms_master.country,
                itg_mds_pacific_terms_master.sls_grp_cd,
                itg_mds_pacific_terms_master.sls_grp_nm,
                itg_mds_pacific_terms_master.terms_percentage,
                itg_mds_pacific_terms_master.crtd_dttm
            FROM PCFITG_INTEGRATION.itg_mds_pacific_terms_master
            WHERE (
                    length((itg_mds_pacific_terms_master.sls_grp_cd)::text) <= 5
                )
        ) imptm
        LEFT JOIN pcfedw_integration.vw_customer_dim vcd ON (
            (
                (imptm.sls_grp_cd)::text = (vcd.sales_grp_cd)::text
            )
        )
    )
WHERE (
        NOT (
            COALESCE(
                ltrim(
                    (vcd.cust_no)::text,
                    ('0'::character varying)::text
                ),
                ('NA'::character varying)::text
            ) IN (
                SELECT DISTINCT COALESCE(terms.cust_id, ('NA'::character varying)::text) AS "coalesce"
                FROM (
                        SELECT imptm.country,
                            imptm.sls_grp_cd,
                            imptm.sls_grp_nm,
                            ltrim(
                                (imptm.sls_grp_cd)::text,
                                ('0'::character varying)::text
                            ) AS cust_id,
                            vcd.cust_nm,
                            imptm.terms_percentage
                        FROM PCFITG_INTEGRATION.itg_mds_pacific_terms_master imptm,
                            pcfedw_integration.vw_customer_dim vcd
                        WHERE (
                                (imptm.sls_grp_cd)::text = ltrim(
                                    (vcd.cust_no)::text,
                                    ('0'::character varying)::text
                                )
                            )
                    ) terms
            )
        )
    );
create or replace view VW_ACTUAL_COGS_RATE_DIM(
	CMP_ID,
	FROM_CCY,
	TO_CCY,
	JJ_MNTH_ID,
	EXCH_RATE
) as

SELECT CASE
        WHEN (
            (b.from_ccy)::text = ('AUD'::character varying)::text
        ) THEN '7470'::character varying
        WHEN (
            (b.from_ccy)::text = ('NZD'::character varying)::text
        ) THEN '8361'::character varying
        ELSE '7471'::character varying
    END AS cmp_id,
    a.from_ccy,
    b.from_ccy AS to_ccy,
    b.jj_mnth_id,
    (
        a.exch_rate * (((1)::numeric)::numeric(18, 0) / b.exch_rate)
    ) AS exch_rate
FROM (
        SELECT DISTINCT COALESCE(a.from_ccy, 'SGD'::character varying) AS from_ccy,
            COALESCE(a.to_ccy, 'AUD'::character varying) AS to_ccy,
            b.jj_year,
            COALESCE(a.exch_rate, c.exch_rate) AS exch_rate
        FROM (
                SELECT "max"(vw_curr_exch_dim.exch_rate) AS exch_rate
                FROM PCFEDW_INTEGRATION.vw_curr_exch_dim
                WHERE (
                        (
                            (
                                (
                                    (vw_curr_exch_dim.rate_type)::text = ('DWBP'::character varying)::text
                                )
                                AND (
                                    (vw_curr_exch_dim.from_ccy)::text = ('SGD'::character varying)::text
                                )
                            )
                            AND (
                                (vw_curr_exch_dim.to_ccy)::text = ('AUD'::character varying)::text
                            )
                        )
                        AND (
                            vw_curr_exch_dim.valid_date = (
                                SELECT "max"(vw_curr_exch_dim.valid_date) AS "max"
                                FROM PCFEDW_INTEGRATION.vw_curr_exch_dim
                                WHERE (
                                        (
                                            (
                                                (vw_curr_exch_dim.rate_type)::text = ('DWBP'::character varying)::text
                                            )
                                            AND (
                                                (vw_curr_exch_dim.from_ccy)::text = ('SGD'::character varying)::text
                                            )
                                        )
                                        AND (
                                            (vw_curr_exch_dim.to_ccy)::text = ('AUD'::character varying)::text
                                        )
                                    )
                            )
                        )
                    )
            ) c,
            (
                (
                    SELECT DISTINCT edw_time_dim.jj_year
                    FROM PCFEDW_INTEGRATION.edw_time_dim
                ) b
                LEFT JOIN (
                    SELECT t1.rate_type,
                        t1.from_ccy,
                        t1.to_ccy,
                        t2.jj_year,
                        "max"(t1.exch_rate) AS exch_rate
                    FROM (
                            SELECT vw_curr_exch_dim.rate_type,
                                vw_curr_exch_dim.from_ccy,
                                vw_curr_exch_dim.to_ccy,
                                vw_curr_exch_dim.valid_date,
                                vw_curr_exch_dim.exch_rate
                            FROM PCFEDW_INTEGRATION.vw_curr_exch_dim
                            WHERE (
                                    (
                                        (
                                            (vw_curr_exch_dim.rate_type)::text = ('DWBP'::character varying)::text
                                        )
                                        AND (
                                            (vw_curr_exch_dim.from_ccy)::text = ('SGD'::character varying)::text
                                        )
                                    )
                                    AND (
                                        (vw_curr_exch_dim.to_ccy)::text = ('AUD'::character varying)::text
                                    )
                                )
                        ) t1,
                        PCFEDW_INTEGRATION.edw_time_dim t2
                    WHERE (t2.time_id = t1.valid_date)
                    GROUP BY t1.rate_type,
                        t1.from_ccy,
                        t1.to_ccy,
                        t2.jj_year
                ) a ON ((a.jj_year = b.jj_year))
            )
    ) a,
    PCFEDW_INTEGRATION.vw_jjbr_curr_exch_dim b,
    (
        SELECT DISTINCT edw_time_dim.jj_year,
            edw_time_dim.jj_mnth_id
        FROM PCFEDW_INTEGRATION.edw_time_dim
    ) c
WHERE (
        (
            (b.jj_mnth_id = c.jj_mnth_id)
            AND (c.jj_year = a.jj_year)
        )
        AND ((b.to_ccy)::text = (a.to_ccy)::text)
    );
create or replace view VW_APO_PARENT_CHILD_DIM(
	SALES_ORG,
	CMP_ID,
	MATL_ID,
	MATL_DESC,
	MASTER_CODE,
	LAUNCH_DATE,
	PREDESSOR_ID,
	PARENT_ID,
	PARENT_MATL_DESC
) as

SELECT t1.sales_org,
    t1.cmp_id,
    t1.matl_id,
    t1.matl_desc,
    t1.master_code,
    t1.launch_date,
    t1.predessor_id,
    t2.matl_id AS parent_id,
    t2.matl_desc AS parent_matl_desc
FROM (
        SELECT DISTINCT a.sls_org AS sales_org,
            c.cmp_id,
            a.matl_num AS matl_id,
            a.mstr_cd AS master_code,
            CASE
                WHEN (
                    (a.launch_dt = '1111-01-01'::date)
                    OR (
                        (a.launch_dt IS NULL)
                        AND ('1111-01-01' IS NULL)
                    )
                ) THEN '2011-01-01'::date
                WHEN (
                    (a.launch_dt = '6009-10-10'::date)
                    OR (
                        (a.launch_dt IS NULL)
                        AND ('6009-10-10' IS NULL)
                    )
                ) THEN '2011-01-01'::date
                WHEN (
                    (a.launch_dt = '9999-01-01'::date)
                    OR (
                        (a.launch_dt IS NULL)
                        AND ('9999-01-01' IS NULL)
                    )
                ) THEN '2011-01-01'::date
                WHEN (
                    (a.launch_dt = '9999-09-09'::date)
                    OR (
                        (a.launch_dt IS NULL)
                        AND ('9999-09-09' IS NULL)
                    )
                ) THEN '2011-01-01'::date
                WHEN (
                    (a.launch_dt = '9999-10-01'::date)
                    OR (
                        (a.launch_dt IS NULL)
                        AND ('9999-10-01' IS NULL)
                    )
                ) THEN '2011-01-01'::date
                WHEN (
                    (a.launch_dt = '2201-07-01'::date)
                    OR (
                        (a.launch_dt IS NULL)
                        AND ('2201-07-01' IS NULL)
                    )
                ) THEN '2011-01-01'::date
                WHEN (
                    (a.launch_dt = NULL::date)
                    OR (
                        (a.launch_dt IS NULL)
                        AND (NULL IS NULL)
                    )
                ) THEN '2011-01-01'::date
                ELSE a.launch_dt
            END AS launch_date,
            a.predecessor AS predessor_id,
            b.matl_desc
        FROM ASPEDW_INTEGRATION.edw_material_sales_dim a,
            (
                SELECT DISTINCT edw_material_dim.matl_num,
                    edw_material_dim.matl_desc
                FROM ASPEDW_INTEGRATION.edw_material_dim
            ) b,
            (
                SELECT DISTINCT dly_sls_cust_attrb_lkp.sls_org,
                    dly_sls_cust_attrb_lkp.cmp_id
                FROM PCFEDW_INTEGRATION.dly_sls_cust_attrb_lkp
            ) c
        WHERE (
                (
                    (
                        (
                            ((a.matl_num)::text = (b.matl_num)::text)
                            AND (
                                (a.dstr_chnl)::text = ('19'::character varying)::text
                            )
                        )
                        AND (
                            (a.mstr_cd)::text <> (''::character varying)::text
                        )
                    )
                    AND (a.mstr_cd IS NOT NULL)
                )
                AND ((a.sls_org)::text = (c.sls_org)::text)
            )
        ORDER BY a.sls_org,
            c.cmp_id,
            a.matl_num
    ) t1,
    (
        SELECT derived_table2.sales_org,
            derived_table2.matl_id,
            derived_table2.master_code,
            derived_table2.launch_date,
            derived_table2.matl_desc
        FROM (
                SELECT derived_table1.sales_org,
                    derived_table1.matl_id,
                    derived_table1.master_code,
                    derived_table1.launch_date,
                    derived_table1.matl_desc,
                    row_number() OVER(
                        PARTITION BY derived_table1.sales_org,
                        derived_table1.master_code
                        ORDER BY derived_table1.launch_date DESC
                    ) AS rowno
                FROM (
                        SELECT DISTINCT a.sls_org AS sales_org,
                            a.matl_num AS matl_id,
                            a.mstr_cd AS master_code,
                            CASE
                                WHEN (
                                    (a.launch_dt = '1111-01-01'::date)
                                    OR (
                                        (a.launch_dt IS NULL)
                                        AND ('1111-01-01' IS NULL)
                                    )
                                ) THEN '2011-01-01'::date
                                WHEN (
                                    (a.launch_dt = '6009-10-10'::date)
                                    OR (
                                        (a.launch_dt IS NULL)
                                        AND ('6009-10-10' IS NULL)
                                    )
                                ) THEN '2011-01-01'::date
                                WHEN (
                                    (a.launch_dt = '9999-01-01'::date)
                                    OR (
                                        (a.launch_dt IS NULL)
                                        AND ('9999-01-01' IS NULL)
                                    )
                                ) THEN '2011-01-01'::date
                                WHEN (
                                    (a.launch_dt = '9999-09-09'::date)
                                    OR (
                                        (a.launch_dt IS NULL)
                                        AND ('9999-09-09' IS NULL)
                                    )
                                ) THEN '2011-01-01'::date
                                WHEN (
                                    (a.launch_dt = '9999-10-01'::date)
                                    OR (
                                        (a.launch_dt IS NULL)
                                        AND ('9999-10-01' IS NULL)
                                    )
                                ) THEN '2011-01-01'::date
                                WHEN (
                                    (a.launch_dt = '2201-07-01'::date)
                                    OR (
                                        (a.launch_dt IS NULL)
                                        AND ('2201-07-01' IS NULL)
                                    )
                                ) THEN '2011-01-01'::date
                                WHEN (
                                    (a.launch_dt = NULL::date)
                                    OR (
                                        (a.launch_dt IS NULL)
                                        AND (NULL IS NULL)
                                    )
                                ) THEN '2011-01-01'::date
                                ELSE a.launch_dt
                            END AS launch_date,
                            b.matl_desc
                        FROM ASPEDW_INTEGRATION.edw_material_sales_dim a,
                            (
                                SELECT DISTINCT edw_material_dim.matl_num,
                                    edw_material_dim.matl_desc
                                FROM ASPEDW_INTEGRATION.edw_material_dim
                            ) b,
                            (
                                SELECT DISTINCT dly_sls_cust_attrb_lkp.sls_org,
                                    dly_sls_cust_attrb_lkp.cmp_id
                                FROM PCFEDW_INTEGRATION.dly_sls_cust_attrb_lkp
                            ) c
                        WHERE (
                                (
                                    (
                                        ((a.matl_num)::text = (b.matl_num)::text)
                                        AND ((a.sls_org)::text = (c.sls_org)::text)
                                    )
                                    AND (
                                        (a.mstr_cd)::text <> (''::character varying)::text
                                    )
                                )
                                AND (a.mstr_cd IS NOT NULL)
                            )
                        ORDER BY a.mstr_cd,
                            CASE
                                WHEN (
                                    (a.launch_dt = '1111-01-01'::date)
                                    OR (
                                        (a.launch_dt IS NULL)
                                        AND ('1111-01-01' IS NULL)
                                    )
                                ) THEN '2011-01-01'::date
                                WHEN (
                                    (a.launch_dt = '6009-10-10'::date)
                                    OR (
                                        (a.launch_dt IS NULL)
                                        AND ('6009-10-10' IS NULL)
                                    )
                                ) THEN '2011-01-01'::date
                                WHEN (
                                    (a.launch_dt = '9999-01-01'::date)
                                    OR (
                                        (a.launch_dt IS NULL)
                                        AND ('9999-01-01' IS NULL)
                                    )
                                ) THEN '2011-01-01'::date
                                WHEN (
                                    (a.launch_dt = '9999-09-09'::date)
                                    OR (
                                        (a.launch_dt IS NULL)
                                        AND ('9999-09-09' IS NULL)
                                    )
                                ) THEN '2011-01-01'::date
                                WHEN (
                                    (a.launch_dt = '9999-10-01'::date)
                                    OR (
                                        (a.launch_dt IS NULL)
                                        AND ('9999-10-01' IS NULL)
                                    )
                                ) THEN '2011-01-01'::date
                                WHEN (
                                    (a.launch_dt = '2201-07-01'::date)
                                    OR (
                                        (a.launch_dt IS NULL)
                                        AND ('2201-07-01' IS NULL)
                                    )
                                ) THEN '2011-01-01'::date
                                WHEN (
                                    (a.launch_dt = NULL::date)
                                    OR (
                                        (a.launch_dt IS NULL)
                                        AND (NULL IS NULL)
                                    )
                                ) THEN '2011-01-01'::date
                                ELSE a.launch_dt
                            END DESC
                    ) derived_table1
            ) derived_table2
        WHERE (derived_table2.rowno = 1)
    ) t2
WHERE (
        ((t1.master_code)::text = (t2.master_code)::text)
        AND ((t1.sales_org)::text = (t2.sales_org)::text)
    );
create or replace view VW_BWAR_CURR_EXCH_DIM(
	RATE_TYPE,
	FROM_CCY,
	TO_CCY,
	JJ_MNTH_ID,
	EXCH_RATE
) as

(
    SELECT bwar_curr_exchng_nzd_usd.rate_type,
        bwar_curr_exchng_nzd_usd.from_ccy,
        bwar_curr_exchng_nzd_usd.to_ccy,
        bwar_curr_exchng_nzd_usd.jj_mnth_id,
        CASE
            WHEN (
                bwar_curr_exchng_nzd_usd.jj_mnth_id = dt_control_curr.first_day_cur_yr
            ) THEN exch_control_prev.exch_rate
            ELSE bwar_curr_exchng_nzd_usd.exch_rate
        END AS exch_rate
    FROM (
            SELECT DISTINCT COALESCE(a.rate_type, 'BWAR'::character varying) AS rate_type,
                COALESCE(a.from_ccy, 'NZD'::character varying) AS from_ccy,
                COALESCE(a.to_ccy, 'USD'::character varying) AS to_ccy,
                b.jj_mnth_id,
                COALESCE(a.exch_rate, c.exch_rate) AS exch_rate
            FROM (
                    SELECT "max"(vw_curr_exch_dim.exch_rate) AS exch_rate
                    FROM PCFEDW_INTEGRATION.vw_curr_exch_dim vw_curr_exch_dim
                    WHERE (
                            (
                                (
                                    (
                                        (vw_curr_exch_dim.rate_type)::text = ('BWAR'::character varying)::text
                                    )
                                    AND (
                                        (vw_curr_exch_dim.from_ccy)::text = ('NZD'::character varying)::text
                                    )
                                )
                                AND (
                                    (vw_curr_exch_dim.to_ccy)::text = ('USD'::character varying)::text
                                )
                            )
                            AND (
                                vw_curr_exch_dim.valid_date = ((20181231)::numeric)::numeric(18, 0)
                            )
                        )
                ) c,
                (
                    (
                        SELECT DISTINCT edw_time_dim.jj_mnth_id
                        FROM PCFEDW_INTEGRATION.edw_time_dim
                    ) b
                    LEFT JOIN (
                        SELECT t1.rate_type,
                            t1.from_ccy,
                            t1.to_ccy,
                            t2.jj_mnth_id,
                            "max"(t1.exch_rate) AS exch_rate
                        FROM (
                                SELECT vw_curr_exch_dim.rate_type,
                                    vw_curr_exch_dim.from_ccy,
                                    vw_curr_exch_dim.to_ccy,
                                    vw_curr_exch_dim.valid_date,
                                    vw_curr_exch_dim.exch_rate
                                FROM PCFEDW_INTEGRATION.vw_curr_exch_dim vw_curr_exch_dim
                                WHERE (
                                        (
                                            (
                                                (vw_curr_exch_dim.rate_type)::text = ('BWAR'::character varying)::text
                                            )
                                            AND (
                                                (vw_curr_exch_dim.from_ccy)::text = ('NZD'::character varying)::text
                                            )
                                        )
                                        AND (
                                            (vw_curr_exch_dim.to_ccy)::text = ('USD'::character varying)::text
                                        )
                                    )
                            ) t1,
                            PCFEDW_INTEGRATION.edw_time_dim t2
                        WHERE (t2.time_id = t1.valid_date)
                        GROUP BY t1.rate_type,
                            t1.from_ccy,
                            t1.to_ccy,
                            t2.jj_mnth_id
                    ) a ON ((a.jj_mnth_id = b.jj_mnth_id))
                )
        ) bwar_curr_exchng_nzd_usd,
        (
            SELECT min(edw_time_dim.jj_mnth_id) AS first_day_cur_yr
            FROM PCFEDW_INTEGRATION.edw_time_dim
            WHERE (
                    edw_time_dim.time_id IN (
                        SELECT "max"(vw_curr_exch_dim.valid_date) AS "max"
                        FROM PCFEDW_INTEGRATION.vw_curr_exch_dim vw_curr_exch_dim
                        WHERE (
                                (
                                    (
                                        (
                                            (vw_curr_exch_dim.rate_type)::text = ('BWAR'::character varying)::text
                                        )
                                        AND (
                                            (vw_curr_exch_dim.from_ccy)::text = ('NZD'::character varying)::text
                                        )
                                    )
                                    AND (
                                        (vw_curr_exch_dim.to_ccy)::text = ('USD'::character varying)::text
                                    )
                                )
                                AND (
                                    "substring"(
                                        ((vw_curr_exch_dim.valid_date)::character varying)::text,
                                        5,
                                        2
                                    ) = ('01'::character varying)::text
                                )
                            )
                    )
                )
        ) dt_control_curr,
        (
            SELECT "max"(vw_curr_exch_dim.exch_rate) AS exch_rate
            FROM PCFEDW_INTEGRATION.vw_curr_exch_dim vw_curr_exch_dim
            WHERE (
                    (
                        (
                            (
                                (vw_curr_exch_dim.rate_type)::text = ('BWAR'::character varying)::text
                            )
                            AND (
                                (vw_curr_exch_dim.from_ccy)::text = ('NZD'::character varying)::text
                            )
                        )
                        AND (
                            (vw_curr_exch_dim.to_ccy)::text = ('USD'::character varying)::text
                        )
                    )
                    AND (
                        vw_curr_exch_dim.valid_date = ((20181231)::numeric)::numeric(18, 0)
                    )
                )
        ) exch_control_prev
    UNION ALL
    SELECT bwar_curr_exchng_aud_usd.rate_type,
        bwar_curr_exchng_aud_usd.from_ccy,
        bwar_curr_exchng_aud_usd.to_ccy,
        bwar_curr_exchng_aud_usd.jj_mnth_id,
        bwar_curr_exchng_aud_usd.exch_rate
    FROM (
            SELECT DISTINCT COALESCE(a.rate_type, 'BWAR'::character varying) AS rate_type,
                COALESCE(a.from_ccy, 'AUD'::character varying) AS from_ccy,
                COALESCE(a.to_ccy, 'USD'::character varying) AS to_ccy,
                b.jj_mnth_id,
                c.exch_rate
            FROM (
                    SELECT "max"(vw_curr_exch_dim.exch_rate) AS exch_rate
                    FROM PCFEDW_INTEGRATION.vw_curr_exch_dim vw_curr_exch_dim
                    WHERE (
                            (
                                (
                                    (
                                        (vw_curr_exch_dim.rate_type)::text = ('BWAR'::character varying)::text
                                    )
                                    AND (
                                        (vw_curr_exch_dim.from_ccy)::text = ('AUD'::character varying)::text
                                    )
                                )
                                AND (
                                    (vw_curr_exch_dim.to_ccy)::text = ('USD'::character varying)::text
                                )
                            )
                            AND (
                                vw_curr_exch_dim.valid_date = ((20181231)::numeric)::numeric(18, 0)
                            )
                        )
                ) c,
                (
                    (
                        SELECT DISTINCT edw_time_dim.jj_mnth_id
                        FROM PCFEDW_INTEGRATION.edw_time_dim
                    ) b
                    LEFT JOIN (
                        SELECT t1.rate_type,
                            t1.from_ccy,
                            t1.to_ccy,
                            t2.jj_mnth_id,
                            "max"(t1.exch_rate) AS exch_rate
                        FROM (
                                SELECT vw_curr_exch_dim.rate_type,
                                    vw_curr_exch_dim.from_ccy,
                                    vw_curr_exch_dim.to_ccy,
                                    vw_curr_exch_dim.valid_date,
                                    vw_curr_exch_dim.exch_rate
                                FROM PCFEDW_INTEGRATION.vw_curr_exch_dim vw_curr_exch_dim
                                WHERE (
                                        (
                                            (
                                                (vw_curr_exch_dim.rate_type)::text = ('BWAR'::character varying)::text
                                            )
                                            AND (
                                                (vw_curr_exch_dim.from_ccy)::text = ('AUD'::character varying)::text
                                            )
                                        )
                                        AND (
                                            (vw_curr_exch_dim.to_ccy)::text = ('USD'::character varying)::text
                                        )
                                    )
                            ) t1,
                            PCFEDW_INTEGRATION.edw_time_dim t2
                        WHERE (t2.time_id = t1.valid_date)
                        GROUP BY t1.rate_type,
                            t1.from_ccy,
                            t1.to_ccy,
                            t2.jj_mnth_id
                    ) a ON ((a.jj_mnth_id = b.jj_mnth_id))
                )
        ) bwar_curr_exchng_aud_usd
)
UNION ALL
SELECT bwar_curr_exchng_nzd_nzd.rate_type,
    bwar_curr_exchng_nzd_nzd.from_ccy,
    bwar_curr_exchng_nzd_nzd.to_ccy,
    bwar_curr_exchng_nzd_nzd.jj_mnth_id,
    bwar_curr_exchng_nzd_nzd.exch_rate
FROM (
        SELECT bwar_curr_exchng_nzd_usd.rate_type,
            bwar_curr_exchng_nzd_usd.from_ccy,
            bwar_curr_exchng_nzd_usd.from_ccy AS to_ccy,
            bwar_curr_exchng_nzd_usd.jj_mnth_id,
            1 AS exch_rate
        FROM (
                SELECT DISTINCT COALESCE(a.rate_type, 'BWAR'::character varying) AS rate_type,
                    COALESCE(a.from_ccy, 'NZD'::character varying) AS from_ccy,
                    COALESCE(a.to_ccy, 'USD'::character varying) AS to_ccy,
                    b.jj_mnth_id,
                    COALESCE(a.exch_rate, c.exch_rate) AS exch_rate
                FROM (
                        SELECT "max"(vw_curr_exch_dim.exch_rate) AS exch_rate
                        FROM PCFEDW_INTEGRATION.vw_curr_exch_dim vw_curr_exch_dim
                        WHERE (
                                (
                                    (
                                        (
                                            (vw_curr_exch_dim.rate_type)::text = ('BWAR'::character varying)::text
                                        )
                                        AND (
                                            (vw_curr_exch_dim.from_ccy)::text = ('NZD'::character varying)::text
                                        )
                                    )
                                    AND (
                                        (vw_curr_exch_dim.to_ccy)::text = ('USD'::character varying)::text
                                    )
                                )
                                AND (
                                    vw_curr_exch_dim.valid_date = ((20181231)::numeric)::numeric(18, 0)
                                )
                            )
                    ) c,
                    (
                        (
                            SELECT DISTINCT edw_time_dim.jj_mnth_id
                            FROM PCFEDW_INTEGRATION.edw_time_dim
                        ) b
                        LEFT JOIN (
                            SELECT t1.rate_type,
                                t1.from_ccy,
                                t1.to_ccy,
                                t2.jj_mnth_id,
                                "max"(t1.exch_rate) AS exch_rate
                            FROM (
                                    SELECT vw_curr_exch_dim.rate_type,
                                        vw_curr_exch_dim.from_ccy,
                                        vw_curr_exch_dim.to_ccy,
                                        vw_curr_exch_dim.valid_date,
                                        vw_curr_exch_dim.exch_rate
                                    FROM PCFEDW_INTEGRATION.vw_curr_exch_dim vw_curr_exch_dim
                                    WHERE (
                                            (
                                                (
                                                    (vw_curr_exch_dim.rate_type)::text = ('BWAR'::character varying)::text
                                                )
                                                AND (
                                                    (vw_curr_exch_dim.from_ccy)::text = ('NZD'::character varying)::text
                                                )
                                            )
                                            AND (
                                                (vw_curr_exch_dim.to_ccy)::text = ('USD'::character varying)::text
                                            )
                                        )
                                ) t1,
                                PCFEDW_INTEGRATION.edw_time_dim t2
                            WHERE (t2.time_id = t1.valid_date)
                            GROUP BY t1.rate_type,
                                t1.from_ccy,
                                t1.to_ccy,
                                t2.jj_mnth_id
                        ) a ON ((a.jj_mnth_id = b.jj_mnth_id))
                    )
            ) bwar_curr_exchng_nzd_usd
    ) bwar_curr_exchng_nzd_nzd;
create or replace view VW_COMPETITIVE_BANNER_GROUP(
	MARKET,
	BANNER,
	BANNER_CLASSIFICATION,
	MANUFACTURER,
	BRAND,
	SKU_NAME,
	EAN_NUMBER,
	UNIT,
	DOLLAR,
	YEAR,
	MONTH,
	QUARTER,
	JJ_MNTH,
	JJ_QRTR,
	JJ_YEAR,
	COUNTRY,
	CURRENCY,
	CRT_DTTM
) as
select coalesce(itg_cbg.market, '#'::varchar) as market,
    coalesce(itg_cbg.banner, '#'::varchar) as banner,
    coalesce(itg_cbg.banner_classification, '#'::varchar) as banner_classification,
    coalesce(itg_cbg.manufacturer, '#'::varchar) as manufacturer,
    coalesce(itg_cbg.brand, '#'::varchar) as brand,
    coalesce(itg_cbg.sku_name, '#'::varchar) as sku_name,
    coalesce(itg_cbg.ean_number, '#'::varchar) as ean_number,
    itg_cbg.unit as unit,
    itg_cbg.dollar as dollar,
    date_part(year,itg_cbg.transaction_date) as year,
    date_part(month,itg_cbg.transaction_date) as month,
    date_part(quarter,itg_cbg.transaction_date) as quarter,
    time_dim.jj_mnth as jj_mnth,
    time_dim.jj_qrtr as jj_qrtr,
    time_dim.jj_year as jj_year,
    itg_cbg.country as country,
    itg_cbg.currency as currency,
    current_timestamp()::timestamp_ntz(9) as crt_dttm
from (
        pcfitg_integration.itg_competitive_banner_group itg_cbg
        left join pcfedw_integration.edw_time_dim time_dim on ((itg_cbg.transaction_date = time_dim.cal_date))
    );
create or replace view VW_CURR_EXCH_DIM(
	RATE_TYPE,
	FROM_CCY,
	TO_CCY,
	VALID_DATE,
	EXCH_RATE
) as

SELECT edw_crncy_exch.ex_rt_typ AS rate_type,
    edw_crncy_exch.from_crncy AS from_ccy,
    edw_crncy_exch.to_crncy AS to_ccy,
    (
        ((99999999)::numeric)::numeric(18, 0) - ((edw_crncy_exch.vld_from)::numeric)::numeric(18, 0)
    ) AS valid_date,
    edw_crncy_exch.ex_rt AS exch_rate
FROM aspedw_integration.edw_crncy_exch;
create or replace view VW_CUSTOMER_DIM(
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
create or replace view VW_DEMAND_FORECAST_ANALYSIS(
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
create or replace view VW_DMND_FRCST_CUSTOMER_DIM(
	CUST_NO,
	SLS_ORG,
	CMP_ID,
	CTRY_KEY,
	COUNTRY,
	STATE_CD,
	POST_CD,
	CUST_SUBURB,
	CUST_NM,
	CUST_DEL_FLAG,
	FCST_CHNL,
	FCST_CHNL_DESC,
	SALES_OFFICE_CD,
	SALES_OFFICE_DESC,
	SALES_GRP_CD,
	SALES_GRP_DESC,
	CURR_CD
) as

SELECT cust.cust_no,
    pac_lkup.sls_org,
    pac_lkup.cmp_id,
    cust.ctry_key,
    pac_lkup.country,
    cust.state_cd,
    cust.post_cd,
    cust.cust_suburb,
    cust.cust_nm,
    cust.cust_del_flag,
    pac_lkup.fcst_chnl,
    pac_lkup.fcst_chnl_desc,
    pac_lkup.sls_office AS sales_office_cd,
    pac_lkup.sls_office_desc AS sales_office_desc,
    pac_lkup.sls_grp AS sales_grp_cd,
    pac_lkup.sls_grp_desc AS sales_grp_desc,
    cust.curr_cd
FROM (
        SELECT customer.cust_no,
            min((customer.cmp_id)::text) AS cmp_id,
            min((customer.ctry_key)::text) AS ctry_key,
            min((customer.country)::text) AS country,
            min((customer.state_cd)::text) AS state_cd,
            min((customer.post_cd)::text) AS post_cd,
            min((customer.cust_suburb)::text) AS cust_suburb,
            min((customer.cust_nm)::text) AS cust_nm,
            min(customer.sls_org) AS sls_org,
            min((customer.cust_del_flag)::text) AS cust_del_flag,
            min((customer.sales_grp_cd)::text) AS sales_grp_cd,
            min((customer.sales_grp_desc)::text) AS sales_grp_desc,
            min((customer.fcst_chnl)::text) AS fcst_chnl,
            min((customer.fcst_chnl_desc)::text) AS fcst_chnl_desc,
            min((customer.curr_cd)::text) AS curr_cd
        FROM (
                SELECT DISTINCT cust.cust_num AS cust_no,
                    pac_lkup.sls_org,
                    pac_lkup.cmp_id,
                    cust.ctry_key,
                    pac_lkup.country,
                    cust.rgn AS state_cd,
                    cust.pstl_cd AS post_cd,
                    cust.city AS cust_suburb,
                    cust.cust_nm,
                    cust_sales.cust_del_flag,
                    pac_lkup.fcst_chnl,
                    pac_lkup.fcst_chnl_desc,
                    pac_lkup.sls_office,
                    pac_lkup.sls_office_desc,
                    pac_lkup.sls_grp AS sales_grp_cd,
                    pac_lkup.sls_grp_desc AS sales_grp_desc,
                    cust_sales.crncy_key AS curr_cd
                FROM ASPEDW_INTEGRATION.edw_customer_base_dim cust,
                    ASPEDW_INTEGRATION.edw_customer_sales_dim cust_sales,
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
                                        WHEN ((
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
                        FROM ASPEDW_INTEGRATION.edw_customer_sales_dim a,
                            (
                                SELECT DISTINCT dmnd_frcst_cust_attrb_lkp.sls_org
                                FROM PCFEDW_INTEGRATION.dmnd_frcst_cust_attrb_lkp
                            ) b
                        WHERE (
                                TRIM((a.sls_org)::text) = TRIM(((b.sls_org)::character varying)::text)
                            )
                        GROUP BY a.cust_num
                    ) req_cust_rec,
                    PCFEDW_INTEGRATION.dmnd_frcst_cust_attrb_lkp pac_lkup
                WHERE (
                        (
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
                        AND (
                            (cust_sales.sls_grp)::text = (pac_lkup.sls_grp)::text
                        )
                    )
            ) customer
        GROUP BY customer.cust_no
    ) cust,
    PCFEDW_INTEGRATION.dmnd_frcst_cust_attrb_lkp pac_lkup
WHERE (
        (
            ltrim(
                (cust.cust_no)::text,
                ('0'::character varying)::text
            ) !LIKE ('7%'::character varying)::text
        )
        AND (
            CASE
                WHEN (
                    (
                        (cust.cust_no)::text = ('0000758531'::character varying)::text
                    )
                    OR (
                        (cust.cust_no IS NULL)
                        AND ('0000758531' IS NULL)
                    )
                ) THEN ('AUA'::character varying)::text
                WHEN (
                    (
                        (cust.cust_no)::text = ('0000758532'::character varying)::text
                    )
                    OR (
                        (cust.cust_no IS NULL)
                        AND ('0000758532' IS NULL)
                    )
                ) THEN ('AUB'::character varying)::text
                WHEN (
                    (
                        (cust.cust_no)::text = ('0000758636'::character varying)::text
                    )
                    OR (
                        (cust.cust_no IS NULL)
                        AND ('0000758636' IS NULL)
                    )
                ) THEN ('NZA'::character varying)::text
                ELSE cust.sales_grp_cd
            END = (pac_lkup.sls_grp)::text
        )
    );
create or replace view VW_IRI_SCAN_SALES(
	IRI_MARKET,
	WK_END_DT,
	IRI_PROD_DESC,
	IRI_EAN,
	AC_NIELSENCODE,
	AC_CODE,
	AC_LONGNAME,
	SALES_GRP_CD,
	SALES_GRP_DESC,
	SCAN_SALES,
	SCAN_UNITS
) as
SELECT iisc.iri_market,
    iisc.wk_end_dt,
    iisc.iri_prod_desc,
    iisc.iri_ean,
    iisc.ac_nielsencode,
    ianm.ac_code,
    ianm.ac_longname,
    vcd.sales_grp_cd,
    vcd.sales_grp_desc,
    iisc.scan_sales,
    iisc.scan_units
FROM (
        pcfitg_integration.itg_iri_scan_sales iisc
        LEFT JOIN (
            (
                SELECT DISTINCT sdl_mds_pacific_acct_nielsencode_mapping.ac_nielsencode,
                    sdl_mds_pacific_acct_nielsencode_mapping.ac_code,
                    sdl_mds_pacific_acct_nielsencode_mapping.ac_longname
                FROM PROD_dna_load.pcfsdl_raw.sdl_mds_pacific_acct_nielsencode_mapping
                WHERE (
                        (
                            sdl_mds_pacific_acct_nielsencode_mapping.ac_nielsencode IS NOT NULL
                        )
                        OR (
                            (
                                sdl_mds_pacific_acct_nielsencode_mapping.ac_nielsencode
                            )::text <> (''::character varying)::text
                        )
                    )
            ) ianm
            LEFT JOIN pcfedw_integration.vw_customer_dim vcd ON (
                (
                    ltrim(
                        (vcd.cust_no)::text,
                        ('0'::character varying)::text
                    ) = ltrim(
                        (ianm.ac_code)::text,
                        ('0'::character varying)::text
                    )
                )
            )
        ) ON (
            (
                upper((ianm.ac_nielsencode)::text) = upper((iisc.ac_nielsencode)::text)
            )
        )
    );
create or replace view VW_IRI_SCAN_SALES_ANALYSIS(
	TIME_ID,
	JJ_YEAR,
	JJ_QRTR,
	JJ_MNTH,
	JJ_WK,
	JJ_MNTH_WK,
	JJ_MNTH_ID,
	JJ_MNTH_TOT,
	JJ_MNTH_DAY,
	JJ_MNTH_SHRT,
	JJ_MNTH_LONG,
	CAL_YEAR,
	CAL_QRTR,
	CAL_MNTH,
	CAL_WK,
	CAL_MNTH_WK,
	CAL_MNTH_ID,
	CAL_MNTH_NM,
	WK_END_DT,
	REPRESENTATIVE_CUST_CD,
	REPRESENTATIVE_CUST_NM,
	CHANNEL_CD,
	CHANNEL_DESC,
	COUNTRY,
	SALES_GRP_CD,
	SALES_GRP_NM,
	IRI_MARKET,
	AC_NIELSENCODE,
	AC_LONGNAME,
	IRI_EAN,
	IRI_PROD_DESC,
	MATL_ID,
	MATL_DESC,
	MASTER_CODE,
	PARENT_MATL_ID,
	PARENT_MATL_DESC,
	BRND_CD,
	BRND_DESC,
	FRAN_CD,
	FRAN_DESC,
	GRP_FRAN_CD,
	GRP_FRAN_DESC,
	PROD_FRAN_CD,
	PROD_FRAN_DESC,
	PROD_MJR_CD,
	PROD_MJR_DESC,
	PROD_MNR_CD,
	PROD_MNR_DESC,
	SCAN_UNITS,
	SCAN_SALES,
	PKA_PRODUCTKEY,
	PKA_PRODUCTDESC,
	LST_SKU,
	GPH_REGION,
	GPH_REG_FRNCHSE,
	GPH_REG_FRNCHSE_GRP,
	GPH_PROD_FRNCHSE,
	GPH_PROD_BRND,
	GPH_PROD_SUB_BRND,
	GPH_PROD_VRNT,
	GPH_PROD_NEEDSTATE,
	GPH_PROD_CTGRY,
	GPH_PROD_SUBCTGRY,
	GPH_PROD_SGMNT,
	GPH_PROD_SUBSGMNT,
	GPH_PROD_PUT_UP_CD,
	GPH_PROD_PUT_UP_DESC
) as
SELECT issa.time_id,
    issa.jj_year,
    issa.jj_qrtr,
    issa.jj_mnth,
    issa.jj_wk,
    issa.jj_mnth_wk,
    issa.jj_mnth_id,
    issa.jj_mnth_tot,
    issa.jj_mnth_day,
    issa.jj_mnth_shrt,
    issa.jj_mnth_long,
    issa.cal_year,
    issa.cal_qrtr,
    issa.cal_mnth,
    issa.cal_wk,
    issa.cal_mnth_wk,
    issa.cal_mnth_id,
    issa.cal_mnth_nm,
    issa.wk_end_dt,
    issa.representative_cust_cd,
    issa.representative_cust_nm,
    issa.channel_cd,
    issa.channel_desc,
    issa.country,
    issa.sales_grp_cd,
    issa.sales_grp_nm,
    issa.iri_market,
    issa.ac_nielsencode,
    issa.ac_longname,
    issa.iri_ean,
    issa.iri_prod_desc,
    issa.matl_id,
    issa.matl_desc,
    COALESCE(issa.master_code, ph_mstr.master_code) AS master_code,
    COALESCE(issa.parent_matl_id, ph_mstr.parent_id) AS parent_matl_id,
    COALESCE(issa.parent_matl_desc, ph_mstr.parent_matl_desc) AS parent_matl_desc,
    COALESCE(issa.brnd_cd, ph_emd.brnd_cd) AS brnd_cd,
    COALESCE(issa.brnd_desc, ph_emd.brnd_desc) AS brnd_desc,
    COALESCE(issa.fran_cd, ph_emd.fran_cd) AS fran_cd,
    COALESCE(issa.fran_desc, ph_emd.fran_desc) AS fran_desc,
    COALESCE(issa.grp_fran_cd, ph_emd.grp_fran_cd) AS grp_fran_cd,
    COALESCE(issa.grp_fran_desc, ph_emd.grp_fran_desc) AS grp_fran_desc,
    COALESCE(issa.prod_fran_cd, ph_emd.prod_fran_cd) AS prod_fran_cd,
    COALESCE(issa.prod_fran_desc, ph_emd.prod_fran_desc) AS prod_fran_desc,
    COALESCE(issa.prod_mjr_cd, ph_emd.prod_mjr_cd) AS prod_mjr_cd,
    COALESCE(issa.prod_mjr_desc, ph_emd.prod_mjr_desc) AS prod_mjr_desc,
    COALESCE(issa.prod_mnr_cd, ph_emd.prod_mnr_cd) AS prod_mnr_cd,
    COALESCE(issa.prod_mnr_desc, ph_emd.prod_mnr_desc) AS prod_mnr_desc,
    issa.scan_units,
    issa.scan_sales,
    issa.pka_productkey,
    issa.pka_productdesc,
    issa.lst_sku,
    COALESCE(issa.gph_region, ph_egph.gph_region) AS gph_region,
    COALESCE(issa.gph_reg_frnchse, ph_egph.gph_reg_frnchse) AS gph_reg_frnchse,
    COALESCE(
        issa.gph_reg_frnchse_grp,
        ph_egph.gph_reg_frnchse_grp
    ) AS gph_reg_frnchse_grp,
    COALESCE(issa.gph_prod_frnchse, ph_egph.gph_prod_frnchse) AS gph_prod_frnchse,
    COALESCE(issa.gph_prod_brnd, ph_egph.gph_prod_brnd) AS gph_prod_brnd,
    COALESCE(
        issa.gph_prod_sub_brnd,
        ph_egph.gph_prod_sub_brnd
    ) AS gph_prod_sub_brnd,
    COALESCE(issa.gph_prod_vrnt, ph_egph.gph_prod_vrnt) AS gph_prod_vrnt,
    COALESCE(
        issa.gph_prod_needstate,
        ph_egph.gph_prod_needstate
    ) AS gph_prod_needstate,
    COALESCE(issa.gph_prod_ctgry, ph_egph.gph_prod_ctgry) AS gph_prod_ctgry,
    COALESCE(
        issa.gph_prod_subctgry,
        ph_egph.gph_prod_subctgry
    ) AS gph_prod_subctgry,
    COALESCE(issa.gph_prod_sgmnt, ph_egph.gph_prod_sgmnt) AS gph_prod_sgmnt,
    COALESCE(
        issa.gph_prod_subsgmnt,
        ph_egph.gph_prod_subsgmnt
    ) AS gph_prod_subsgmnt,
    COALESCE(
        issa.gph_prod_put_up_cd,
        ph_egph.gph_prod_put_up_cd
    ) AS gph_prod_put_up_cd,
    COALESCE(
        issa.gph_prod_put_up_desc,
        ph_egph.gph_prod_put_up_desc
    ) AS gph_prod_put_up_desc
FROM (
        (
            (
                (
                    SELECT sales_cte.time_id,
                        sales_cte.jj_year,
                        sales_cte.jj_qrtr,
                        sales_cte.jj_mnth,
                        sales_cte.jj_wk,
                        sales_cte.jj_mnth_wk,
                        sales_cte.jj_mnth_id,
                        sales_cte.jj_mnth_tot,
                        sales_cte.jj_mnth_day,
                        sales_cte.jj_mnth_shrt,
                        sales_cte.jj_mnth_long,
                        sales_cte.cal_year,
                        sales_cte.cal_qrtr,
                        sales_cte.cal_mnth,
                        sales_cte.cal_wk,
                        sales_cte.cal_mnth_wk,
                        sales_cte.cal_mnth_id,
                        sales_cte.cal_mnth_nm,
                        sales_cte.wk_end_dt,
                        sales_cte.representative_cust_cd,
                        sales_cte.representative_cust_nm,
                        sales_cte.channel_cd,
                        sales_cte.channel_desc,
                        sales_cte.country,
                        sales_cte.sales_grp_cd,
                        sales_cte.sales_grp_nm,
                        sales_cte.cmp_id,
                        sales_cte.iri_market,
                        sales_cte.ac_nielsencode,
                        sales_cte.ac_longname,
                        sales_cte.iri_ean,
                        sales_cte.iri_prod_desc,
                        sales_cte.matl_id,
                        sales_cte.matl_desc,
                        sales_cte.master_code,
                        sales_cte.parent_matl_id,
                        sales_cte.parent_matl_desc,
                        sales_cte.brnd_cd,
                        sales_cte.brnd_desc,
                        sales_cte.fran_cd,
                        sales_cte.fran_desc,
                        sales_cte.grp_fran_cd,
                        sales_cte.grp_fran_desc,
                        sales_cte.prod_fran_cd,
                        sales_cte.prod_fran_desc,
                        sales_cte.prod_mjr_cd,
                        sales_cte.prod_mjr_desc,
                        sales_cte.prod_mnr_cd,
                        sales_cte.prod_mnr_desc,
                        sales_cte.scan_units,
                        sales_cte.scan_sales,
                        COALESCE(
                            CASE
                                WHEN (
                                    (mat_dim.pka_product_key)::text = (''::character varying)::text
                                ) THEN NULL::character varying
                                ELSE mat_dim.pka_product_key
                            END,
                            CASE
                                WHEN (
                                    (prod_key.pka_productkey)::text = (''::character varying)::text
                                ) THEN NULL::character varying
                                ELSE prod_key.pka_productkey
                            END
                        ) AS pka_productkey,
                        COALESCE(
                            CASE
                                WHEN (
                                    (mat_dim.pka_product_key_description)::text = (''::character varying)::text
                                ) THEN NULL::character varying
                                ELSE mat_dim.pka_product_key_description
                            END,
                            CASE
                                WHEN (
                                    (prod_key.pka_productdesc)::text = (''::character varying)::text
                                ) THEN NULL::character varying
                                ELSE prod_key.pka_productdesc
                            END
                        ) AS pka_productdesc,
                        COALESCE(
                            CASE
                                WHEN (prod_key.sku = (''::character varying)::text) THEN (NULL::character varying)::text
                                ELSE ltrim(prod_key.sku, ('0'::character varying)::text)
                            END,
                            (
                                (
                                    CASE
                                        WHEN (
                                            (mat_dim.matl_num)::text = (''::character varying)::text
                                        ) THEN (NULL::character varying)::text
                                        ELSE ltrim(
                                            (mat_dim.matl_num)::text,
                                            ('0'::character varying)::text
                                        )
                                    END
                                )::character varying
                            )::text
                        ) AS lst_sku,
                        mat_dim.gph_region,
                        mat_dim.gph_reg_frnchse,
                        mat_dim.gph_reg_frnchse_grp,
                        mat_dim.gph_prod_frnchse,
                        mat_dim.gph_prod_brnd,
                        mat_dim.gph_prod_sub_brnd,
                        mat_dim.gph_prod_vrnt,
                        mat_dim.gph_prod_needstate,
                        mat_dim.gph_prod_ctgry,
                        mat_dim.gph_prod_subctgry,
                        mat_dim.gph_prod_sgmnt,
                        mat_dim.gph_prod_subsgmnt,
                        mat_dim.gph_prod_put_up_cd,
                        mat_dim.gph_prod_put_up_desc
                    FROM (
                            (
                                (
                                    SELECT sales.time_id,
                                        sales.jj_year,
                                        sales.jj_qrtr,
                                        sales.jj_mnth,
                                        sales.jj_wk,
                                        sales.jj_mnth_wk,
                                        sales.jj_mnth_id,
                                        sales.jj_mnth_tot,
                                        sales.jj_mnth_day,
                                        sales.jj_mnth_shrt,
                                        sales.jj_mnth_long,
                                        sales.cal_year,
                                        sales.cal_qrtr,
                                        sales.cal_mnth,
                                        sales.cal_wk,
                                        sales.cal_mnth_wk,
                                        sales.cal_mnth_id,
                                        sales.cal_mnth_nm,
                                        sales.wk_end_dt,
                                        sales.ac_attribute AS representative_cust_cd,
                                        sales.cust_nm AS representative_cust_nm,
                                        sales.channel_cd,
                                        sales.channel_desc,
                                        sales.country,
                                        sales.sales_grp_cd,
                                        sales.sales_grp_desc AS sales_grp_nm,
                                        sales.cmp_id,
                                        sales.iri_market,
                                        sales.ac_nielsencode,
                                        sales.ac_longname,
                                        sales.iri_ean,
                                        sales.iri_prod_desc,
                                        ltrim(
                                            (sales.matl_id)::text,
                                            ('0'::character varying)::text
                                        ) AS matl_id,
                                        sales.matl_desc,
                                        matl.master_code,
                                        ltrim(
                                            (matl.parent_id)::text,
                                            ('0'::character varying)::text
                                        ) AS parent_matl_id,
                                        matl.parent_matl_desc,
                                        sales.brnd_cd,
                                        sales.brnd_desc,
                                        sales.fran_cd,
                                        sales.fran_desc,
                                        sales.grp_fran_cd,
                                        sales.grp_fran_desc,
                                        sales.prod_fran_cd,
                                        sales.prod_fran_desc,
                                        sales.prod_mjr_cd,
                                        sales.prod_mjr_desc,
                                        sales.prod_mnr_cd,
                                        sales.prod_mnr_desc,
                                        sales.scan_units,
                                        sales.scan_sales
                                    FROM (
                                            (
                                                SELECT bar_cd_map.time_id,
                                                    bar_cd_map.jj_year,
                                                    bar_cd_map.jj_qrtr,
                                                    bar_cd_map.jj_mnth,
                                                    bar_cd_map.jj_wk,
                                                    bar_cd_map.jj_mnth_wk,
                                                    bar_cd_map.jj_mnth_id,
                                                    bar_cd_map.jj_mnth_tot,
                                                    bar_cd_map.jj_mnth_day,
                                                    bar_cd_map.jj_mnth_shrt,
                                                    bar_cd_map.jj_mnth_long,
                                                    bar_cd_map.cal_year,
                                                    bar_cd_map.cal_qrtr,
                                                    bar_cd_map.cal_mnth,
                                                    bar_cd_map.cal_wk,
                                                    bar_cd_map.cal_mnth_wk,
                                                    bar_cd_map.cal_mnth_id,
                                                    bar_cd_map.cal_mnth_nm,
                                                    bar_cd_map.wk_end_dt,
                                                    bar_cd_map.ac_attribute,
                                                    (bar_cd_map.cust_nm)::character varying AS cust_nm,
                                                    bar_cd_map.channel_cd,
                                                    bar_cd_map.channel_desc,
                                                    bar_cd_map.sales_grp_cd,
                                                    bar_cd_map.sales_grp_desc,
                                                    bar_cd_map.cmp_id,
                                                    bar_cd_map.country,
                                                    bar_cd_map.iri_market,
                                                    bar_cd_map.ac_nielsencode,
                                                    bar_cd_map.ac_longname,
                                                    bar_cd_map.iri_ean,
                                                    bar_cd_map.matl_bar_cd,
                                                    bar_cd_map.iri_prod_desc,
                                                    (bar_cd_map.matl_id)::character varying AS matl_id,
                                                    bar_cd_map.matl_desc,
                                                    bar_cd_map.brnd_cd,
                                                    bar_cd_map.brnd_desc,
                                                    bar_cd_map.fran_cd,
                                                    bar_cd_map.fran_desc,
                                                    bar_cd_map.grp_fran_cd,
                                                    bar_cd_map.grp_fran_desc,
                                                    bar_cd_map.prod_fran_cd,
                                                    bar_cd_map.prod_fran_desc,
                                                    bar_cd_map.prod_mjr_cd,
                                                    bar_cd_map.prod_mjr_desc,
                                                    bar_cd_map.prod_mnr_cd,
                                                    bar_cd_map.prod_mnr_desc,
                                                    bar_cd_map.scan_units,
                                                    bar_cd_map.scan_sales
                                                FROM (
                                                        SELECT sales_derived.time_id,
                                                            sales_derived.jj_year,
                                                            sales_derived.jj_qrtr,
                                                            sales_derived.jj_mnth,
                                                            sales_derived.jj_wk,
                                                            sales_derived.jj_mnth_wk,
                                                            sales_derived.jj_mnth_id,
                                                            sales_derived.jj_mnth_tot,
                                                            sales_derived.jj_mnth_day,
                                                            sales_derived.jj_mnth_shrt,
                                                            sales_derived.jj_mnth_long,
                                                            sales_derived.cal_year,
                                                            sales_derived.cal_qrtr,
                                                            sales_derived.cal_mnth,
                                                            sales_derived.cal_wk,
                                                            sales_derived.cal_mnth_wk,
                                                            sales_derived.cal_mnth_id,
                                                            sales_derived.cal_mnth_nm,
                                                            sales_derived.wk_end_dt,
                                                            sales_derived.ac_code AS ac_attribute,
                                                            vcd.cust_nm,
                                                            vcd.channel_cd,
                                                            vcd.channel_desc,
                                                            sales_derived.sales_grp_cd,
                                                            sales_derived.sales_grp_desc,
                                                            vcd.cmp_id,
                                                            vcd.country,
                                                            sales_derived.iri_market,
                                                            sales_derived.ac_nielsencode,
                                                            sales_derived.ac_longname,
                                                            sales_derived.iri_ean,
                                                            sales_derived.matl_bar_cd,
                                                            sales_derived.iri_prod_desc,
                                                            ltrim(
                                                                (vmd.matl_id)::text,
                                                                ('0'::character varying)::text
                                                            ) AS matl_id,
                                                            vmd.matl_desc,
                                                            vmd.brnd_cd,
                                                            vmd.brnd_desc,
                                                            vmd.fran_cd,
                                                            vmd.fran_desc,
                                                            vmd.grp_fran_cd,
                                                            vmd.grp_fran_desc,
                                                            vmd.prod_fran_cd,
                                                            vmd.prod_fran_desc,
                                                            vmd.prod_mjr_cd,
                                                            vmd.prod_mjr_desc,
                                                            vmd.prod_mnr_cd,
                                                            vmd.prod_mnr_desc,
                                                            sales_derived.scan_units,
                                                            sales_derived.scan_sales
                                                        FROM (
                                                                (
                                                                    (
                                                                        SELECT iss.iri_market,
                                                                            iss.wk_end_dt,
                                                                            iss.iri_prod_desc,
                                                                            iss.iri_ean,
                                                                            iss.ac_nielsencode,
                                                                            iss.ac_code,
                                                                            iss.ac_longname,
                                                                            iss.sales_grp_cd,
                                                                            iss.sales_grp_desc,
                                                                            iss.scan_sales,
                                                                            iss.scan_units,
                                                                            etd.cal_date,
                                                                            etd.time_id,
                                                                            etd.jj_wk,
                                                                            etd.jj_mnth,
                                                                            etd.jj_mnth_shrt,
                                                                            etd.jj_mnth_long,
                                                                            etd.jj_qrtr,
                                                                            etd.jj_year,
                                                                            etd.cal_mnth_id,
                                                                            etd.jj_mnth_id,
                                                                            etd.cal_mnth,
                                                                            etd.cal_qrtr,
                                                                            etd.cal_year,
                                                                            etd.jj_mnth_tot,
                                                                            etd.jj_mnth_day,
                                                                            etd.cal_mnth_nm,
                                                                            etd.jj_mnth_wk,
                                                                            etd.cal_wk,
                                                                            etd.cal_mnth_wk,
                                                                            ean.matl_id,
                                                                            ean.bar_cd AS matl_bar_cd
                                                                        FROM (
                                                                                SELECT etd.cal_date,
                                                                                    etd.time_id,
                                                                                    etd.jj_wk,
                                                                                    etd.jj_mnth,
                                                                                    etd.jj_mnth_shrt,
                                                                                    etd.jj_mnth_long,
                                                                                    etd.jj_qrtr,
                                                                                    etd.jj_year,
                                                                                    etd.cal_mnth_id,
                                                                                    etd.jj_mnth_id,
                                                                                    etd.cal_mnth,
                                                                                    etd.cal_qrtr,
                                                                                    etd.cal_year,
                                                                                    etd.jj_mnth_tot,
                                                                                    etd.jj_mnth_day,
                                                                                    etd.cal_mnth_nm,
                                                                                    etdw.jj_mnth_wk,
                                                                                    etdc.cal_wk,
                                                                                    etdcm.cal_mnth_wk
                                                                                FROM PCFEDW_INTEGRATION.edw_time_dim etd,
                                                                                    (
                                                                                        SELECT etd.jj_year,
                                                                                            etd.jj_mnth_id,
                                                                                            etd.jj_wk,
                                                                                            row_number() OVER(
                                                                                                PARTITION BY etd.jj_year,
                                                                                                etd.jj_mnth_id
                                                                                                ORDER BY etd.jj_year,
                                                                                                    etd.jj_mnth_id,
                                                                                                    etd.jj_wk
                                                                                            ) AS jj_mnth_wk
                                                                                        FROM (
                                                                                                SELECT DISTINCT etd.jj_year,
                                                                                                    etd.jj_mnth_id,
                                                                                                    etd.jj_wk
                                                                                                FROM PCFEDW_INTEGRATION.edw_time_dim etd
                                                                                            ) etd
                                                                                    ) etdw,
                                                                                    (
                                                                                        SELECT etd.cal_date,
                                                                                            etd.time_id,
                                                                                            etd.jj_wk,
                                                                                            etd.jj_mnth,
                                                                                            etd.jj_mnth_shrt,
                                                                                            etd.jj_mnth_long,
                                                                                            etd.jj_qrtr,
                                                                                            etd.jj_year,
                                                                                            etd.cal_mnth_id,
                                                                                            etd.jj_mnth_id,
                                                                                            etd.cal_mnth,
                                                                                            etd.cal_qrtr,
                                                                                            etd.cal_year,
                                                                                            etd.jj_mnth_tot,
                                                                                            etd.jj_mnth_day,
                                                                                            etd.cal_mnth_nm,
                                                                                            CASE
                                                                                                WHEN (
                                                                                                    (
                                                                                                        row_number() OVER(
                                                                                                            PARTITION BY etd.cal_year
                                                                                                            ORDER BY TO_DATE(etd.cal_date)
                                                                                                        ) % (7)::bigint
                                                                                                    ) = 0
                                                                                                ) THEN (
                                                                                                    row_number() OVER(
                                                                                                        PARTITION BY etd.cal_year
                                                                                                        ORDER BY TO_DATE(etd.cal_date)
                                                                                                    ) / 7
                                                                                                )
                                                                                                ELSE (
                                                                                                    (
                                                                                                        row_number() OVER(
                                                                                                            PARTITION BY etd.cal_year
                                                                                                            ORDER BY TO_DATE(etd.cal_date)
                                                                                                        ) / 7
                                                                                                    ) + 1
                                                                                                )
                                                                                            END AS cal_wk
                                                                                        FROM PCFEDW_INTEGRATION.edw_time_dim etd
                                                                                    ) etdc,
                                                                                    (
                                                                                        SELECT etdcw.cal_year,
                                                                                            etdcw.cal_mnth_id,
                                                                                            etdcw.cal_wk,
                                                                                            row_number() OVER(
                                                                                                PARTITION BY etdcw.cal_year,
                                                                                                etdcw.cal_mnth_id
                                                                                                ORDER BY etdcw.cal_year,
                                                                                                    etdcw.cal_mnth_id,
                                                                                                    etdcw.cal_wk
                                                                                            ) AS cal_mnth_wk
                                                                                        FROM (
                                                                                                SELECT DISTINCT etdc.cal_year,
                                                                                                    etdc.cal_mnth_id,
                                                                                                    etdc.cal_wk
                                                                                                FROM (
                                                                                                        SELECT etd.cal_year,
                                                                                                            etd.cal_mnth_id,
                                                                                                            CASE
                                                                                                                WHEN (
                                                                                                                    (
                                                                                                                        row_number() OVER(
                                                                                                                            PARTITION BY etd.cal_year
                                                                                                                            ORDER BY TO_DATE(etd.cal_date)
                                                                                                                        ) % (7)::bigint
                                                                                                                    ) = 0
                                                                                                                ) THEN (
                                                                                                                    row_number() OVER(
                                                                                                                        PARTITION BY etd.cal_year
                                                                                                                        ORDER BY TO_DATE(etd.cal_date)
                                                                                                                    ) / 7
                                                                                                                )
                                                                                                                ELSE (
                                                                                                                    (
                                                                                                                        row_number() OVER(
                                                                                                                            PARTITION BY etd.cal_year
                                                                                                                            ORDER BY TO_DATE(etd.cal_date)
                                                                                                                        ) / 7
                                                                                                                    ) + 1
                                                                                                                )
                                                                                                            END AS cal_wk
                                                                                                        FROM PCFEDW_INTEGRATION.edw_time_dim etd
                                                                                                    ) etdc
                                                                                            ) etdcw
                                                                                    ) etdcm
                                                                                WHERE (
                                                                                        (
                                                                                            (
                                                                                                (
                                                                                                    (
                                                                                                        (
                                                                                                            (etd.jj_year = etdw.jj_year)
                                                                                                            AND (etd.jj_mnth_id = etdw.jj_mnth_id)
                                                                                                        )
                                                                                                        AND (etd.jj_wk = etdw.jj_wk)
                                                                                                    )
                                                                                                    AND (TO_DATE(etd.cal_date) = TO_DATE(etdc.cal_date))
                                                                                                )
                                                                                                AND (etdc.cal_wk = etdcm.cal_wk)
                                                                                            )
                                                                                            AND (etdc.cal_year = etdcm.cal_year)
                                                                                        )
                                                                                        AND (etdc.cal_mnth_id = etdcm.cal_mnth_id)
                                                                                    )
                                                                            ) etd,
                                                                            (
                                                                                PCFEDW_INTEGRATION.vw_iri_scan_sales iss
                                                                                LEFT JOIN (
                                                                                    SELECT edw_material_dim.matl_id,
                                                                                        edw_material_dim.matl_desc,
                                                                                        edw_material_dim.mega_brnd_cd,
                                                                                        edw_material_dim.mega_brnd_desc,
                                                                                        edw_material_dim.brnd_cd,
                                                                                        edw_material_dim.brnd_desc,
                                                                                        edw_material_dim.base_prod_cd,
                                                                                        edw_material_dim.base_prod_desc,
                                                                                        edw_material_dim.variant_cd,
                                                                                        edw_material_dim.variant_desc,
                                                                                        edw_material_dim.fran_cd,
                                                                                        edw_material_dim.fran_desc,
                                                                                        edw_material_dim.grp_fran_cd,
                                                                                        edw_material_dim.grp_fran_desc,
                                                                                        edw_material_dim.matl_type_cd,
                                                                                        edw_material_dim.matl_type_desc,
                                                                                        edw_material_dim.prod_fran_cd,
                                                                                        edw_material_dim.prod_fran_desc,
                                                                                        edw_material_dim.prod_hier_cd,
                                                                                        edw_material_dim.prod_hier_desc,
                                                                                        edw_material_dim.prod_mjr_cd,
                                                                                        edw_material_dim.prod_mjr_desc,
                                                                                        edw_material_dim.prod_mnr_cd,
                                                                                        edw_material_dim.prod_mnr_desc,
                                                                                        edw_material_dim.mercia_plan,
                                                                                        edw_material_dim.putup_cd,
                                                                                        edw_material_dim.putup_desc,
                                                                                        edw_material_dim.bar_cd,
                                                                                        edw_material_dim.updt_dt,
                                                                                        edw_material_dim.prft_ctr
                                                                                    FROM PCFEDW_INTEGRATION.edw_material_dim
                                                                                    WHERE (
                                                                                            edw_material_dim.bar_cd IN (
                                                                                                SELECT DISTINCT derived_table1.bar_cd
                                                                                                FROM (
                                                                                                        SELECT count(*) AS count,
                                                                                                            edw_material_dim.bar_cd
                                                                                                        FROM PCFEDW_INTEGRATION.edw_material_dim
                                                                                                        GROUP BY edw_material_dim.bar_cd
                                                                                                        HAVING (count(*) = 1)
                                                                                                    ) derived_table1
                                                                                            )
                                                                                        )
                                                                                ) ean ON (
                                                                                    (
                                                                                        ltrim(
                                                                                            (ean.bar_cd)::text,
                                                                                            ('0'::character varying)::text
                                                                                        ) = ltrim(
                                                                                            (COALESCE(iss.iri_ean, '0'::character varying))::text,
                                                                                            ('0'::character varying)::text
                                                                                        )
                                                                                    )
                                                                                )
                                                                            )
                                                                        WHERE (
                                                                                TO_DATE((iss.wk_end_dt)::timestamp without time zone) = TO_DATE(etd.cal_date)
                                                                            )
                                                                    ) sales_derived
                                                                    LEFT JOIN PCFEDW_INTEGRATION.edw_material_dim vmd ON (
                                                                        (
                                                                            ltrim(
                                                                                (vmd.matl_id)::text,
                                                                                ('0'::character varying)::text
                                                                            ) = ltrim(
                                                                                (
                                                                                    COALESCE(sales_derived.matl_id, '0'::character varying)
                                                                                )::text,
                                                                                ('0'::character varying)::text
                                                                            )
                                                                        )
                                                                    )
                                                                )
                                                                LEFT JOIN PCFEDW_INTEGRATION.vw_customer_dim vcd ON (
                                                                    (
                                                                        ltrim(
                                                                            (vcd.cust_no)::text,
                                                                            ('0'::character varying)::text
                                                                        ) = ltrim(
                                                                            (sales_derived.ac_code)::text,
                                                                            ('0'::character varying)::text
                                                                        )
                                                                    )
                                                                )
                                                            )
                                                        WHERE (
                                                                sales_derived.matl_bar_cd IN (
                                                                    SELECT DISTINCT derived_table2.bar_cd
                                                                    FROM (
                                                                            SELECT count(*) AS count,
                                                                                edw_material_dim.bar_cd
                                                                            FROM PCFEDW_INTEGRATION.edw_material_dim
                                                                            GROUP BY edw_material_dim.bar_cd
                                                                            HAVING (count(*) = 1)
                                                                        ) derived_table2
                                                                )
                                                            )
                                                    ) bar_cd_map
                                                UNION ALL
                                                SELECT sales_derived.time_id,
                                                    sales_derived.jj_year,
                                                    sales_derived.jj_qrtr,
                                                    sales_derived.jj_mnth,
                                                    sales_derived.jj_wk,
                                                    sales_derived.jj_mnth_wk,
                                                    sales_derived.jj_mnth_id,
                                                    sales_derived.jj_mnth_tot,
                                                    sales_derived.jj_mnth_day,
                                                    sales_derived.jj_mnth_shrt,
                                                    sales_derived.jj_mnth_long,
                                                    sales_derived.cal_year,
                                                    sales_derived.cal_qrtr,
                                                    sales_derived.cal_mnth,
                                                    sales_derived.cal_wk,
                                                    sales_derived.cal_mnth_wk,
                                                    sales_derived.cal_mnth_id,
                                                    sales_derived.cal_mnth_nm,
                                                    sales_derived.wk_end_dt,
                                                    sales_derived.ac_code AS ac_attribute,
                                                    (vcd.cust_nm)::character varying AS cust_nm,
                                                    vcd.channel_cd,
                                                    vcd.channel_desc,
                                                    sales_derived.sales_grp_cd,
                                                    sales_derived.sales_grp_desc,
                                                    vcd.cmp_id,
                                                    vcd.country,
                                                    sales_derived.iri_market,
                                                    sales_derived.ac_nielsencode,
                                                    sales_derived.ac_longname,
                                                    sales_derived.iri_ean,
                                                    NULL::character varying AS matl_bar_cd,
                                                    sales_derived.iri_prod_desc,
                                                    (
                                                        ltrim(
                                                            (vmd.matl_id)::text,
                                                            ('0'::character varying)::text
                                                        )
                                                    )::character varying AS matl_id,
                                                    vmd.matl_desc,
                                                    vmd.brnd_cd,
                                                    vmd.brnd_desc,
                                                    vmd.fran_cd,
                                                    vmd.fran_desc,
                                                    vmd.grp_fran_cd,
                                                    vmd.grp_fran_desc,
                                                    vmd.prod_fran_cd,
                                                    vmd.prod_fran_desc,
                                                    vmd.prod_mjr_cd,
                                                    vmd.prod_mjr_desc,
                                                    vmd.prod_mnr_cd,
                                                    vmd.prod_mnr_desc,
                                                    sales_derived.scan_units,
                                                    sales_derived.scan_sales
                                                FROM (
                                                        (
                                                            (
                                                                SELECT iss.iri_market,
                                                                    iss.wk_end_dt,
                                                                    iss.iri_prod_desc,
                                                                    iss.iri_ean,
                                                                    iss.ac_nielsencode,
                                                                    iss.ac_code,
                                                                    iss.ac_longname,
                                                                    iss.sales_grp_cd,
                                                                    iss.sales_grp_desc,
                                                                    iss.scan_sales,
                                                                    iss.scan_units,
                                                                    etd.cal_date,
                                                                    etd.time_id,
                                                                    etd.jj_wk,
                                                                    etd.jj_mnth,
                                                                    etd.jj_mnth_shrt,
                                                                    etd.jj_mnth_long,
                                                                    etd.jj_qrtr,
                                                                    etd.jj_year,
                                                                    etd.cal_mnth_id,
                                                                    etd.jj_mnth_id,
                                                                    etd.cal_mnth,
                                                                    etd.cal_qrtr,
                                                                    etd.cal_year,
                                                                    etd.jj_mnth_tot,
                                                                    etd.jj_mnth_day,
                                                                    etd.cal_mnth_nm,
                                                                    etd.jj_mnth_wk,
                                                                    etd.cal_wk,
                                                                    etd.cal_mnth_wk
                                                                FROM (
                                                                        SELECT etd.cal_date,
                                                                            etd.time_id,
                                                                            etd.jj_wk,
                                                                            etd.jj_mnth,
                                                                            etd.jj_mnth_shrt,
                                                                            etd.jj_mnth_long,
                                                                            etd.jj_qrtr,
                                                                            etd.jj_year,
                                                                            etd.cal_mnth_id,
                                                                            etd.jj_mnth_id,
                                                                            etd.cal_mnth,
                                                                            etd.cal_qrtr,
                                                                            etd.cal_year,
                                                                            etd.jj_mnth_tot,
                                                                            etd.jj_mnth_day,
                                                                            etd.cal_mnth_nm,
                                                                            etdw.jj_mnth_wk,
                                                                            etdc.cal_wk,
                                                                            etdcm.cal_mnth_wk
                                                                        FROM PCFEDW_INTEGRATION.edw_time_dim etd,
                                                                            (
                                                                                SELECT etd.jj_year,
                                                                                    etd.jj_mnth_id,
                                                                                    etd.jj_wk,
                                                                                    row_number() OVER(
                                                                                        PARTITION BY etd.jj_year,
                                                                                        etd.jj_mnth_id
                                                                                        ORDER BY etd.jj_year,
                                                                                            etd.jj_mnth_id,
                                                                                            etd.jj_wk
                                                                                    ) AS jj_mnth_wk
                                                                                FROM (
                                                                                        SELECT DISTINCT etd.jj_year,
                                                                                            etd.jj_mnth_id,
                                                                                            etd.jj_wk
                                                                                        FROM PCFEDW_INTEGRATION.edw_time_dim etd
                                                                                    ) etd
                                                                            ) etdw,
                                                                            (
                                                                                SELECT etd.cal_date,
                                                                                    etd.time_id,
                                                                                    etd.jj_wk,
                                                                                    etd.jj_mnth,
                                                                                    etd.jj_mnth_shrt,
                                                                                    etd.jj_mnth_long,
                                                                                    etd.jj_qrtr,
                                                                                    etd.jj_year,
                                                                                    etd.cal_mnth_id,
                                                                                    etd.jj_mnth_id,
                                                                                    etd.cal_mnth,
                                                                                    etd.cal_qrtr,
                                                                                    etd.cal_year,
                                                                                    etd.jj_mnth_tot,
                                                                                    etd.jj_mnth_day,
                                                                                    etd.cal_mnth_nm,
                                                                                    CASE
                                                                                        WHEN (
                                                                                            (
                                                                                                row_number() OVER(
                                                                                                    PARTITION BY etd.cal_year
                                                                                                    ORDER BY TO_DATE(etd.cal_date)
                                                                                                ) % (7)::bigint
                                                                                            ) = 0
                                                                                        ) THEN (
                                                                                            row_number() OVER(
                                                                                                PARTITION BY etd.cal_year
                                                                                                ORDER BY TO_DATE(etd.cal_date)
                                                                                            ) / 7
                                                                                        )
                                                                                        ELSE (
                                                                                            (
                                                                                                row_number() OVER(
                                                                                                    PARTITION BY etd.cal_year
                                                                                                    ORDER BY TO_DATE(etd.cal_date)
                                                                                                ) / 7
                                                                                            ) + 1
                                                                                        )
                                                                                    END AS cal_wk
                                                                                FROM PCFEDW_INTEGRATION.edw_time_dim etd
                                                                            ) etdc,
                                                                            (
                                                                                SELECT etdcw.cal_year,
                                                                                    etdcw.cal_mnth_id,
                                                                                    etdcw.cal_wk,
                                                                                    row_number() OVER(
                                                                                        PARTITION BY etdcw.cal_year,
                                                                                        etdcw.cal_mnth_id
                                                                                        ORDER BY etdcw.cal_year,
                                                                                            etdcw.cal_mnth_id,
                                                                                            etdcw.cal_wk
                                                                                    ) AS cal_mnth_wk
                                                                                FROM (
                                                                                        SELECT DISTINCT etdc.cal_year,
                                                                                            etdc.cal_mnth_id,
                                                                                            etdc.cal_wk
                                                                                        FROM (
                                                                                                SELECT etd.cal_year,
                                                                                                    etd.cal_mnth_id,
                                                                                                    CASE
                                                                                                        WHEN (
                                                                                                            (
                                                                                                                row_number() OVER(
                                                                                                                    PARTITION BY etd.cal_year
                                                                                                                    ORDER BY TO_DATE(etd.cal_date)
                                                                                                                ) % (7)::bigint
                                                                                                            ) = 0
                                                                                                        ) THEN (
                                                                                                            row_number() OVER(
                                                                                                                PARTITION BY etd.cal_year
                                                                                                                ORDER BY TO_DATE(etd.cal_date)
                                                                                                            ) / 7
                                                                                                        )
                                                                                                        ELSE (
                                                                                                            (
                                                                                                                row_number() OVER(
                                                                                                                    PARTITION BY etd.cal_year
                                                                                                                    ORDER BY TO_DATE(etd.cal_date)
                                                                                                                ) / 7
                                                                                                            ) + 1
                                                                                                        )
                                                                                                    END AS cal_wk
                                                                                                FROM PCFEDW_INTEGRATION.edw_time_dim etd
                                                                                            ) etdc
                                                                                    ) etdcw
                                                                            ) etdcm
                                                                        WHERE (
                                                                                (
                                                                                    (
                                                                                        (
                                                                                            (
                                                                                                (
                                                                                                    (etd.jj_year = etdw.jj_year)
                                                                                                    AND (etd.jj_mnth_id = etdw.jj_mnth_id)
                                                                                                )
                                                                                                AND (etd.jj_wk = etdw.jj_wk)
                                                                                            )
                                                                                            AND (TO_DATE(etd.cal_date) = TO_DATE(etdc.cal_date))
                                                                                        )
                                                                                        AND (etdc.cal_wk = etdcm.cal_wk)
                                                                                    )
                                                                                    AND (etdc.cal_year = etdcm.cal_year)
                                                                                )
                                                                                AND (etdc.cal_mnth_id = etdcm.cal_mnth_id)
                                                                            )
                                                                    ) etd,
                                                                    PCFEDW_INTEGRATION.vw_iri_scan_sales iss
                                                                WHERE (
                                                                        TO_DATE((iss.wk_end_dt)::timestamp without time zone) = TO_DATE(etd.cal_date)
                                                                    )
                                                            ) sales_derived
                                                            LEFT JOIN (
                                                                (
                                                                    SELECT derived_table6.jj_month_id,
                                                                        derived_table6.bar_cd,
                                                                        derived_table6.cust_no,
                                                                        derived_table6.material_id
                                                                    FROM (
                                                                            SELECT DISTINCT derived_table5.jj_month_id,
                                                                                derived_table5.bar_cd,
                                                                                derived_table5.cust_no,
                                                                                derived_table5.material_id
                                                                            FROM (
                                                                                    SELECT DISTINCT derived_table4.jj_month_id,
                                                                                        derived_table4.master_code,
                                                                                        derived_table4.bar_cd,
                                                                                        derived_table4.cust_no,
                                                                                        derived_table4.material_count,
                                                                                        derived_table4.gts_val,
                                                                                        count(DISTINCT derived_table4.master_code) AS count,
                                                                                        CASE
                                                                                            WHEN (count(DISTINCT derived_table4.master_code) > 1) THEN CASE
                                                                                                WHEN (
                                                                                                    (
                                                                                                        (derived_table4.master_code IS NOT NULL)
                                                                                                        AND (
                                                                                                            derived_table4.gts_val >= ((0)::numeric)::numeric(18, 0)
                                                                                                        )
                                                                                                    )
                                                                                                    AND (
                                                                                                        upper((derived_table4.channel_desc)::text) <> ('AU - EXPORTS'::character varying)::text
                                                                                                    )
                                                                                                ) THEN (derived_table4.max_material_id)::character varying
                                                                                                WHEN (
                                                                                                    (
                                                                                                        (derived_table4.master_code IS NULL)
                                                                                                        AND (
                                                                                                            derived_table4.gts_val < ((0)::numeric)::numeric(18, 0)
                                                                                                        )
                                                                                                    )
                                                                                                    AND (
                                                                                                        upper((derived_table4.channel_desc)::text) = ('AU - EXPORTS'::character varying)::text
                                                                                                    )
                                                                                                ) THEN 'NULL'::character varying
                                                                                                ELSE NULL::character varying
                                                                                            END
                                                                                            WHEN (count(DISTINCT derived_table4.master_code) = 1) THEN CASE
                                                                                                WHEN (
                                                                                                    (derived_table4.material_count > 1)
                                                                                                    AND (
                                                                                                        derived_table4.gts_val >= ((0)::numeric)::numeric(18, 0)
                                                                                                    )
                                                                                                ) THEN (derived_table4.max_material_id)::character varying
                                                                                                WHEN (derived_table4.material_count = 1) THEN (derived_table4.max_material_id)::character varying
                                                                                                ELSE NULL::character varying
                                                                                            END
                                                                                            ELSE derived_table4.material_id
                                                                                        END AS material_id
                                                                                    FROM (
                                                                                            SELECT DISTINCT a.jj_month_id,
                                                                                                a.master_code,
                                                                                                a.bar_cd,
                                                                                                a.matl_id AS material_id,
                                                                                                a.cust_no,
                                                                                                count(a.matl_id) OVER(
                                                                                                    PARTITION BY a.jj_month_id,
                                                                                                    a.bar_cd,
                                                                                                    a.cust_no ORDER BY NULL ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                                                                                                ) AS material_count,
                                                                                                "max"((a.matl_id)::text) OVER(
                                                                                                    PARTITION BY a.jj_month_id,
                                                                                                    a.master_code,
                                                                                                    a.bar_cd,
                                                                                                    a.cust_no,
                                                                                                    sum(a.gts_val) ORDER BY NULL ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                                                                                                ) AS max_material_id,
                                                                                                row_number() OVER(
                                                                                                    PARTITION BY a.jj_month_id,
                                                                                                    a.bar_cd,
                                                                                                    a.cust_no
                                                                                                    ORDER BY sum(a.gts_val) DESC
                                                                                                ) AS sales_rank,
                                                                                                count(COALESCE(a.master_code, 'NA'::character varying)) OVER(
                                                                                                    PARTITION BY a.jj_month_id,
                                                                                                    a.bar_cd,
                                                                                                    a.cust_no,
                                                                                                    a.matl_id ORDER BY NULL ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                                                                                                ) AS master_code_count,
                                                                                                sum(a.gts_val) AS gts_val,
                                                                                                a.channel_desc,
                                                                                                b.matl_bar_count
                                                                                            FROM (
                                                                                                    (
                                                                                                        SELECT vsf.jj_month_id,
                                                                                                            vsf.gts_val,
                                                                                                            vmd.matl_id,
                                                                                                            vmd.bar_cd,
                                                                                                            mstrcd.master_code,
                                                                                                            vcd.cust_no,
                                                                                                            vcd.channel_desc
                                                                                                        FROM (
                                                                                                                (
                                                                                                                    (
                                                                                                                        PCFEDW_INTEGRATION.vw_sapbw_ciw_fact vsf
                                                                                                                        LEFT JOIN PCFEDW_INTEGRATION.vw_customer_dim vcd ON (((vsf.cust_no)::text = (vcd.cust_no)::text))
                                                                                                                    )
                                                                                                                    LEFT JOIN PCFEDW_INTEGRATION.edw_material_dim vmd ON (((vsf.matl_id)::text = (vmd.matl_id)::text))
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
                                                                                                                        ((vsf.cmp_id)::text = (vapcd.cmp_id)::text)
                                                                                                                        AND ((vsf.matl_id)::text = (vapcd.matl_id)::text)
                                                                                                                    )
                                                                                                                )
                                                                                                            )
                                                                                                    ) a
                                                                                                    JOIN (
                                                                                                        SELECT DISTINCT edw_material_dim.bar_cd,
                                                                                                            count(DISTINCT edw_material_dim.matl_id) AS matl_bar_count
                                                                                                        FROM PCFEDW_INTEGRATION.edw_material_dim
                                                                                                        WHERE (
                                                                                                                COALESCE(edw_material_dim.bar_cd, 'NA'::character varying) IN (
                                                                                                                    SELECT DISTINCT COALESCE(derived_table3.bar_cd, 'NA'::character varying) AS "coalesce"
                                                                                                                    FROM (
                                                                                                                            SELECT count(*) AS count,
                                                                                                                                edw_material_dim.bar_cd
                                                                                                                            FROM PCFEDW_INTEGRATION.edw_material_dim
                                                                                                                            GROUP BY edw_material_dim.bar_cd
                                                                                                                            HAVING (count(*) > 1)
                                                                                                                        ) derived_table3
                                                                                                                )
                                                                                                            )
                                                                                                        GROUP BY edw_material_dim.bar_cd
                                                                                                    ) b ON (((a.bar_cd)::text = (b.bar_cd)::text))
                                                                                                )
                                                                                            GROUP BY a.jj_month_id,
                                                                                                a.master_code,
                                                                                                a.bar_cd,
                                                                                                a.matl_id,
                                                                                                a.cust_no,
                                                                                                a.channel_desc,
                                                                                                b.matl_bar_count
                                                                                        ) derived_table4
                                                                                    WHERE (derived_table4.sales_rank = 1)
                                                                                    GROUP BY derived_table4.jj_month_id,
                                                                                        derived_table4.master_code,
                                                                                        derived_table4.bar_cd,
                                                                                        derived_table4.cust_no,
                                                                                        derived_table4.material_id,
                                                                                        derived_table4.material_count,
                                                                                        derived_table4.max_material_id,
                                                                                        derived_table4.master_code_count,
                                                                                        derived_table4.channel_desc,
                                                                                        derived_table4.gts_val,
                                                                                        derived_table4.matl_bar_count
                                                                                ) derived_table5
                                                                        ) derived_table6
                                                                ) ean
                                                                LEFT JOIN PCFEDW_INTEGRATION.edw_material_dim vmd ON (
                                                                    (
                                                                        ltrim(
                                                                            (vmd.matl_id)::text,
                                                                            ('0'::character varying)::text
                                                                        ) = ltrim(
                                                                            (
                                                                                COALESCE(ean.material_id, '0'::character varying)
                                                                            )::text,
                                                                            ('0'::character varying)::text
                                                                        )
                                                                    )
                                                                )
                                                            ) ON (
                                                                (
                                                                    (
                                                                        (
                                                                            ((ean.jj_month_id)::numeric)::numeric(18, 0) = sales_derived.jj_mnth_id
                                                                        )
                                                                        AND (
                                                                            ltrim(
                                                                                (ean.cust_no)::text,
                                                                                ('0'::character varying)::text
                                                                            ) = (sales_derived.ac_code)::text
                                                                        )
                                                                    )
                                                                    AND (
                                                                        (COALESCE(ean.bar_cd, '0'::character varying))::text = (
                                                                            COALESCE(sales_derived.iri_ean, '0'::character varying)
                                                                        )::text
                                                                    )
                                                                )
                                                            )
                                                        )
                                                        LEFT JOIN PCFEDW_INTEGRATION.vw_customer_dim vcd ON (
                                                            (
                                                                ltrim(
                                                                    (vcd.cust_no)::text,
                                                                    ('0'::character varying)::text
                                                                ) = ltrim(
                                                                    (sales_derived.ac_code)::text,
                                                                    ('0'::character varying)::text
                                                                )
                                                            )
                                                        )
                                                    )
                                                WHERE (
                                                        NOT (
                                                            COALESCE(sales_derived.iri_ean, 'NA'::character varying) IN (
                                                                SELECT DISTINCT COALESCE(bar_cd_map.iri_ean, '0'::character varying) AS "coalesce"
                                                                FROM (
                                                                        SELECT sales_derived.time_id,
                                                                            sales_derived.jj_year,
                                                                            sales_derived.jj_qrtr,
                                                                            sales_derived.jj_mnth,
                                                                            sales_derived.jj_wk,
                                                                            sales_derived.jj_mnth_wk,
                                                                            sales_derived.jj_mnth_id,
                                                                            sales_derived.jj_mnth_tot,
                                                                            sales_derived.jj_mnth_day,
                                                                            sales_derived.jj_mnth_shrt,
                                                                            sales_derived.jj_mnth_long,
                                                                            sales_derived.cal_year,
                                                                            sales_derived.cal_qrtr,
                                                                            sales_derived.cal_mnth,
                                                                            sales_derived.cal_wk,
                                                                            sales_derived.cal_mnth_wk,
                                                                            sales_derived.cal_mnth_id,
                                                                            sales_derived.cal_mnth_nm,
                                                                            sales_derived.wk_end_dt,
                                                                            sales_derived.ac_code AS ac_attribute,
                                                                            vcd.cust_nm,
                                                                            vcd.channel_cd,
                                                                            vcd.channel_desc,
                                                                            sales_derived.sales_grp_cd,
                                                                            sales_derived.sales_grp_desc,
                                                                            vcd.cmp_id,
                                                                            vcd.country,
                                                                            sales_derived.iri_market,
                                                                            sales_derived.ac_nielsencode,
                                                                            sales_derived.ac_longname,
                                                                            sales_derived.iri_ean,
                                                                            sales_derived.matl_bar_cd,
                                                                            sales_derived.iri_prod_desc,
                                                                            ltrim(
                                                                                (vmd.matl_id)::text,
                                                                                ('0'::character varying)::text
                                                                            ) AS matl_id,
                                                                            vmd.matl_desc,
                                                                            vmd.brnd_cd,
                                                                            vmd.brnd_desc,
                                                                            vmd.fran_cd,
                                                                            vmd.fran_desc,
                                                                            vmd.grp_fran_cd,
                                                                            vmd.grp_fran_desc,
                                                                            vmd.prod_fran_cd,
                                                                            vmd.prod_fran_desc,
                                                                            vmd.prod_mjr_cd,
                                                                            vmd.prod_mjr_desc,
                                                                            vmd.prod_mnr_cd,
                                                                            vmd.prod_mnr_desc,
                                                                            sales_derived.scan_units,
                                                                            sales_derived.scan_sales
                                                                        FROM (
                                                                                (
                                                                                    (
                                                                                        SELECT iss.iri_market,
                                                                                            iss.wk_end_dt,
                                                                                            iss.iri_prod_desc,
                                                                                            iss.iri_ean,
                                                                                            iss.ac_nielsencode,
                                                                                            iss.ac_code,
                                                                                            iss.ac_longname,
                                                                                            iss.sales_grp_cd,
                                                                                            iss.sales_grp_desc,
                                                                                            iss.scan_sales,
                                                                                            iss.scan_units,
                                                                                            etd.cal_date,
                                                                                            etd.time_id,
                                                                                            etd.jj_wk,
                                                                                            etd.jj_mnth,
                                                                                            etd.jj_mnth_shrt,
                                                                                            etd.jj_mnth_long,
                                                                                            etd.jj_qrtr,
                                                                                            etd.jj_year,
                                                                                            etd.cal_mnth_id,
                                                                                            etd.jj_mnth_id,
                                                                                            etd.cal_mnth,
                                                                                            etd.cal_qrtr,
                                                                                            etd.cal_year,
                                                                                            etd.jj_mnth_tot,
                                                                                            etd.jj_mnth_day,
                                                                                            etd.cal_mnth_nm,
                                                                                            etd.jj_mnth_wk,
                                                                                            etd.cal_wk,
                                                                                            etd.cal_mnth_wk,
                                                                                            ean.matl_id,
                                                                                            ean.bar_cd AS matl_bar_cd
                                                                                        FROM (
                                                                                                SELECT etd.cal_date,
                                                                                                    etd.time_id,
                                                                                                    etd.jj_wk,
                                                                                                    etd.jj_mnth,
                                                                                                    etd.jj_mnth_shrt,
                                                                                                    etd.jj_mnth_long,
                                                                                                    etd.jj_qrtr,
                                                                                                    etd.jj_year,
                                                                                                    etd.cal_mnth_id,
                                                                                                    etd.jj_mnth_id,
                                                                                                    etd.cal_mnth,
                                                                                                    etd.cal_qrtr,
                                                                                                    etd.cal_year,
                                                                                                    etd.jj_mnth_tot,
                                                                                                    etd.jj_mnth_day,
                                                                                                    etd.cal_mnth_nm,
                                                                                                    etdw.jj_mnth_wk,
                                                                                                    etdc.cal_wk,
                                                                                                    etdcm.cal_mnth_wk
                                                                                                FROM PCFEDW_INTEGRATION.edw_time_dim etd,
                                                                                                    (
                                                                                                        SELECT etd.jj_year,
                                                                                                            etd.jj_mnth_id,
                                                                                                            etd.jj_wk,
                                                                                                            row_number() OVER(
                                                                                                                PARTITION BY etd.jj_year,
                                                                                                                etd.jj_mnth_id
                                                                                                                ORDER BY etd.jj_year,
                                                                                                                    etd.jj_mnth_id,
                                                                                                                    etd.jj_wk
                                                                                                            ) AS jj_mnth_wk
                                                                                                        FROM (
                                                                                                                SELECT DISTINCT etd.jj_year,
                                                                                                                    etd.jj_mnth_id,
                                                                                                                    etd.jj_wk
                                                                                                                FROM PCFEDW_INTEGRATION.edw_time_dim etd
                                                                                                            ) etd
                                                                                                    ) etdw,
                                                                                                    (
                                                                                                        SELECT etd.cal_date,
                                                                                                            etd.time_id,
                                                                                                            etd.jj_wk,
                                                                                                            etd.jj_mnth,
                                                                                                            etd.jj_mnth_shrt,
                                                                                                            etd.jj_mnth_long,
                                                                                                            etd.jj_qrtr,
                                                                                                            etd.jj_year,
                                                                                                            etd.cal_mnth_id,
                                                                                                            etd.jj_mnth_id,
                                                                                                            etd.cal_mnth,
                                                                                                            etd.cal_qrtr,
                                                                                                            etd.cal_year,
                                                                                                            etd.jj_mnth_tot,
                                                                                                            etd.jj_mnth_day,
                                                                                                            etd.cal_mnth_nm,
                                                                                                            CASE
                                                                                                                WHEN (
                                                                                                                    (
                                                                                                                        row_number() OVER(
                                                                                                                            PARTITION BY etd.cal_year
                                                                                                                            ORDER BY TO_DATE(etd.cal_date)
                                                                                                                        ) % (7)::bigint
                                                                                                                    ) = 0
                                                                                                                ) THEN (
                                                                                                                    row_number() OVER(
                                                                                                                        PARTITION BY etd.cal_year
                                                                                                                        ORDER BY TO_DATE(etd.cal_date)
                                                                                                                    ) / 7
                                                                                                                )
                                                                                                                ELSE (
                                                                                                                    (
                                                                                                                        row_number() OVER(
                                                                                                                            PARTITION BY etd.cal_year
                                                                                                                            ORDER BY TO_DATE(etd.cal_date)
                                                                                                                        ) / 7
                                                                                                                    ) + 1
                                                                                                                )
                                                                                                            END AS cal_wk
                                                                                                        FROM PCFEDW_INTEGRATION.edw_time_dim etd
                                                                                                    ) etdc,
                                                                                                    (
                                                                                                        SELECT etdcw.cal_year,
                                                                                                            etdcw.cal_mnth_id,
                                                                                                            etdcw.cal_wk,
                                                                                                            row_number() OVER(
                                                                                                                PARTITION BY etdcw.cal_year,
                                                                                                                etdcw.cal_mnth_id
                                                                                                                ORDER BY etdcw.cal_year,
                                                                                                                    etdcw.cal_mnth_id,
                                                                                                                    etdcw.cal_wk
                                                                                                            ) AS cal_mnth_wk
                                                                                                        FROM (
                                                                                                                SELECT DISTINCT etdc.cal_year,
                                                                                                                    etdc.cal_mnth_id,
                                                                                                                    etdc.cal_wk
                                                                                                                FROM (
                                                                                                                        SELECT etd.cal_year,
                                                                                                                            etd.cal_mnth_id,
                                                                                                                            CASE
                                                                                                                                WHEN (
                                                                                                                                    (
                                                                                                                                        row_number() OVER(
                                                                                                                                            PARTITION BY etd.cal_year
                                                                                                                                            ORDER BY TO_DATE(etd.cal_date)
                                                                                                                                        ) % (7)::bigint
                                                                                                                                    ) = 0
                                                                                                                                ) THEN (
                                                                                                                                    row_number() OVER(
                                                                                                                                        PARTITION BY etd.cal_year
                                                                                                                                        ORDER BY TO_DATE(etd.cal_date)
                                                                                                                                    ) / 7
                                                                                                                                )
                                                                                                                                ELSE (
                                                                                                                                    (
                                                                                                                                        row_number() OVER(
                                                                                                                                            PARTITION BY etd.cal_year
                                                                                                                                            ORDER BY TO_DATE(etd.cal_date)
                                                                                                                                        ) / 7
                                                                                                                                    ) + 1
                                                                                                                                )
                                                                                                                            END AS cal_wk
                                                                                                                        FROM PCFEDW_INTEGRATION.edw_time_dim etd
                                                                                                                    ) etdc
                                                                                                            ) etdcw
                                                                                                    ) etdcm
                                                                                                WHERE (
                                                                                                        (
                                                                                                            (
                                                                                                                (
                                                                                                                    (
                                                                                                                        (
                                                                                                                            (etd.jj_year = etdw.jj_year)
                                                                                                                            AND (etd.jj_mnth_id = etdw.jj_mnth_id)
                                                                                                                        )
                                                                                                                        AND (etd.jj_wk = etdw.jj_wk)
                                                                                                                    )
                                                                                                                    AND (TO_DATE(etd.cal_date) = TO_DATE(etdc.cal_date))
                                                                                                                )
                                                                                                                AND (etdc.cal_wk = etdcm.cal_wk)
                                                                                                            )
                                                                                                            AND (etdc.cal_year = etdcm.cal_year)
                                                                                                        )
                                                                                                        AND (etdc.cal_mnth_id = etdcm.cal_mnth_id)
                                                                                                    )
                                                                                            ) etd,
                                                                                            (
                                                                                                PCFEDW_INTEGRATION.vw_iri_scan_sales iss
                                                                                                LEFT JOIN (
                                                                                                    SELECT edw_material_dim.matl_id,
                                                                                                        edw_material_dim.matl_desc,
                                                                                                        edw_material_dim.mega_brnd_cd,
                                                                                                        edw_material_dim.mega_brnd_desc,
                                                                                                        edw_material_dim.brnd_cd,
                                                                                                        edw_material_dim.brnd_desc,
                                                                                                        edw_material_dim.base_prod_cd,
                                                                                                        edw_material_dim.base_prod_desc,
                                                                                                        edw_material_dim.variant_cd,
                                                                                                        edw_material_dim.variant_desc,
                                                                                                        edw_material_dim.fran_cd,
                                                                                                        edw_material_dim.fran_desc,
                                                                                                        edw_material_dim.grp_fran_cd,
                                                                                                        edw_material_dim.grp_fran_desc,
                                                                                                        edw_material_dim.matl_type_cd,
                                                                                                        edw_material_dim.matl_type_desc,
                                                                                                        edw_material_dim.prod_fran_cd,
                                                                                                        edw_material_dim.prod_fran_desc,
                                                                                                        edw_material_dim.prod_hier_cd,
                                                                                                        edw_material_dim.prod_hier_desc,
                                                                                                        edw_material_dim.prod_mjr_cd,
                                                                                                        edw_material_dim.prod_mjr_desc,
                                                                                                        edw_material_dim.prod_mnr_cd,
                                                                                                        edw_material_dim.prod_mnr_desc,
                                                                                                        edw_material_dim.mercia_plan,
                                                                                                        edw_material_dim.putup_cd,
                                                                                                        edw_material_dim.putup_desc,
                                                                                                        edw_material_dim.bar_cd,
                                                                                                        edw_material_dim.updt_dt,
                                                                                                        edw_material_dim.prft_ctr
                                                                                                    FROM PCFEDW_INTEGRATION.edw_material_dim
                                                                                                    WHERE (
                                                                                                            edw_material_dim.bar_cd IN (
                                                                                                                SELECT DISTINCT derived_table1.bar_cd
                                                                                                                FROM (
                                                                                                                        SELECT count(*) AS count,
                                                                                                                            edw_material_dim.bar_cd
                                                                                                                        FROM PCFEDW_INTEGRATION.edw_material_dim
                                                                                                                        GROUP BY edw_material_dim.bar_cd
                                                                                                                        HAVING (count(*) = 1)
                                                                                                                    ) derived_table1
                                                                                                            )
                                                                                                        )
                                                                                                ) ean ON (
                                                                                                    (
                                                                                                        ltrim(
                                                                                                            (ean.bar_cd)::text,
                                                                                                            ('0'::character varying)::text
                                                                                                        ) = ltrim(
                                                                                                            (COALESCE(iss.iri_ean, '0'::character varying))::text,
                                                                                                            ('0'::character varying)::text
                                                                                                        )
                                                                                                    )
                                                                                                )
                                                                                            )
                                                                                        WHERE (
                                                                                                TO_DATE((iss.wk_end_dt)::timestamp without time zone) = TO_DATE(etd.cal_date)
                                                                                            )
                                                                                    ) sales_derived
                                                                                    LEFT JOIN PCFEDW_INTEGRATION.edw_material_dim vmd ON (
                                                                                        (
                                                                                            ltrim(
                                                                                                (vmd.matl_id)::text,
                                                                                                ('0'::character varying)::text
                                                                                            ) = ltrim(
                                                                                                (
                                                                                                    COALESCE(sales_derived.matl_id, '0'::character varying)
                                                                                                )::text,
                                                                                                ('0'::character varying)::text
                                                                                            )
                                                                                        )
                                                                                    )
                                                                                )
                                                                                LEFT JOIN PCFEDW_INTEGRATION.vw_customer_dim vcd ON (
                                                                                    (
                                                                                        ltrim(
                                                                                            (vcd.cust_no)::text,
                                                                                            ('0'::character varying)::text
                                                                                        ) = ltrim(
                                                                                            (sales_derived.ac_code)::text,
                                                                                            ('0'::character varying)::text
                                                                                        )
                                                                                    )
                                                                                )
                                                                            )
                                                                        WHERE (
                                                                                sales_derived.matl_bar_cd IN (
                                                                                    SELECT DISTINCT derived_table2.bar_cd
                                                                                    FROM (
                                                                                            SELECT count(*) AS count,
                                                                                                edw_material_dim.bar_cd
                                                                                            FROM PCFEDW_INTEGRATION.edw_material_dim
                                                                                            GROUP BY edw_material_dim.bar_cd
                                                                                            HAVING (count(*) = 1)
                                                                                        ) derived_table2
                                                                                )
                                                                            )
                                                                    ) bar_cd_map
                                                            )
                                                        )
                                                    )
                                            ) sales
                                            LEFT JOIN (
                                                SELECT vapcd.sales_org,
                                                    vapcd.cmp_id,
                                                    vapcd.matl_id,
                                                    vapcd.matl_desc,
                                                    vapcd.master_code,
                                                    vapcd.launch_date,
                                                    vapcd.predessor_id,
                                                    vapcd.parent_id,
                                                    vapcd.parent_matl_desc
                                                FROM (
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
                                                        ) mstr ON (
                                                            (
                                                                (vapcd.master_code)::text = (mstr.master_code)::text
                                                            )
                                                        )
                                                    )
                                            ) matl ON (
                                                (
                                                    ((matl.cmp_id)::text = (sales.cmp_id)::text)
                                                    AND (
                                                        ltrim(
                                                            (matl.matl_id)::text,
                                                            ('0'::character varying)::text
                                                        ) = ltrim(
                                                            (sales.matl_id)::text,
                                                            ('0'::character varying)::text
                                                        )
                                                    )
                                                )
                                            )
                                        )
                                ) sales_cte
                                LEFT JOIN (
                                    SELECT DISTINCT a.ean_upc,
                                        a.sku,
                                        a.pka_productkey,
                                        a.pka_productdesc
                                    FROM (
                                            (
                                                SELECT ltrim(
                                                        (edw_product_key_attributes.ean_upc)::text,
                                                        ('0'::character varying)::text
                                                    ) AS ean_upc,
                                                    ltrim(
                                                        (edw_product_key_attributes.matl_num)::text,
                                                        ('0'::character varying)::text
                                                    ) AS sku,
                                                    edw_product_key_attributes.pka_productkey,
                                                    edw_product_key_attributes.pka_productdesc,
                                                    edw_product_key_attributes.lst_nts AS nts_date
                                                FROM ASPEDW_INTEGRATION.edw_product_key_attributes
                                                WHERE (
                                                        (
                                                            (
                                                                (
                                                                    (
                                                                        (edw_product_key_attributes.matl_type_cd)::text = ('FERT'::character varying)::text
                                                                    )
                                                                    OR (
                                                                        (edw_product_key_attributes.matl_type_cd)::text = ('HALB'::character varying)::text
                                                                    )
                                                                )
                                                                OR (
                                                                    (edw_product_key_attributes.matl_type_cd)::text = ('SAPR'::character varying)::text
                                                                )
                                                            )
                                                            AND (edw_product_key_attributes.lst_nts IS NOT NULL)
                                                        )
                                                        AND (
                                                            (
                                                                (edw_product_key_attributes.ctry_nm)::text = ('Australia'::character varying)::text
                                                            )
                                                            OR (
                                                                (edw_product_key_attributes.ctry_nm)::text = ('New Zealand'::character varying)::text
                                                            )
                                                        )
                                                    )
                                            ) a
                                            JOIN (
                                                SELECT ltrim(
                                                        (edw_product_key_attributes.ean_upc)::text,
                                                        ('0'::character varying)::text
                                                    ) AS ean_upc,
                                                    ltrim(
                                                        (edw_product_key_attributes.matl_num)::text,
                                                        ('0'::character varying)::text
                                                    ) AS sku,
                                                    edw_product_key_attributes.lst_nts AS latest_nts_date,
                                                    row_number() OVER(
                                                        PARTITION BY edw_product_key_attributes.ean_upc
                                                        ORDER BY edw_product_key_attributes.lst_nts DESC
                                                    ) AS row_number
                                                FROM ASPEDW_INTEGRATION.edw_product_key_attributes
                                                WHERE (
                                                        (
                                                            (
                                                                (
                                                                    (
                                                                        (edw_product_key_attributes.matl_type_cd)::text = ('FERT'::character varying)::text
                                                                    )
                                                                    OR (
                                                                        (edw_product_key_attributes.matl_type_cd)::text = ('HALB'::character varying)::text
                                                                    )
                                                                )
                                                                OR (
                                                                    (edw_product_key_attributes.matl_type_cd)::text = ('SAPR'::character varying)::text
                                                                )
                                                            )
                                                            AND (edw_product_key_attributes.lst_nts IS NOT NULL)
                                                        )
                                                        AND (
                                                            (
                                                                (edw_product_key_attributes.ctry_nm)::text = ('Australia'::character varying)::text
                                                            )
                                                            OR (
                                                                (edw_product_key_attributes.ctry_nm)::text = ('New Zealand'::character varying)::text
                                                            )
                                                        )
                                                    )
                                            ) b ON (
                                                (
                                                    (
                                                        (
                                                            (a.ean_upc = b.ean_upc)
                                                            AND (a.sku = b.sku)
                                                        )
                                                        AND (b.latest_nts_date = a.nts_date)
                                                    )
                                                    AND (b.row_number = 1)
                                                )
                                            )
                                        )
                                ) prod_key ON (
                                    (
                                        ltrim(
                                            (sales_cte.iri_ean)::text,
                                            ('0'::character varying)::text
                                        ) = ltrim(prod_key.ean_upc, ('0'::character varying)::text)
                                    )
                                )
                            )
                            LEFT JOIN (
                                SELECT derived_table2.iri_ean,
                                    derived_table2.matl_id,
                                    derived_table2.matl_num,
                                    derived_table2.crt_on,
                                    derived_table2.pka_product_key,
                                    derived_table2.pka_product_key_description,
                                    derived_table2.gph_region,
                                    derived_table2.gph_reg_frnchse,
                                    derived_table2.gph_reg_frnchse_grp,
                                    derived_table2.gph_prod_frnchse,
                                    derived_table2.gph_prod_brnd,
                                    derived_table2.gph_prod_sub_brnd,
                                    derived_table2.gph_prod_vrnt,
                                    derived_table2.gph_prod_needstate,
                                    derived_table2.gph_prod_ctgry,
                                    derived_table2.gph_prod_subctgry,
                                    derived_table2.gph_prod_sgmnt,
                                    derived_table2.gph_prod_subsgmnt,
                                    derived_table2.gph_prod_put_up_cd,
                                    derived_table2.gph_prod_put_up_desc,
                                    derived_table2.rnk
                                FROM (
                                        SELECT a.iri_ean,
                                            a.matl_id,
                                            b.matl_num,
                                            b.crt_on,
                                            b.pka_product_key,
                                            b.pka_product_key_description,
                                            b.gph_region,
                                            b.gph_reg_frnchse,
                                            b.gph_reg_frnchse_grp,
                                            b.gph_prod_frnchse,
                                            b.gph_prod_brnd,
                                            b.gph_prod_sub_brnd,
                                            b.gph_prod_vrnt,
                                            b.gph_prod_needstate,
                                            b.gph_prod_ctgry,
                                            b.gph_prod_subctgry,
                                            b.gph_prod_sgmnt,
                                            b.gph_prod_subsgmnt,
                                            b.gph_prod_put_up_cd,
                                            b.gph_prod_put_up_desc,
                                            row_number() OVER(
                                                PARTITION BY a.iri_ean
                                                ORDER BY b.crt_on DESC NULLS LAST
                                            ) AS rnk
                                        FROM (
                                                (
                                                    SELECT DISTINCT iri_scan_sales_analysis_cte.iri_ean,
                                                        iri_scan_sales_analysis_cte.matl_id
                                                    FROM (
                                                            SELECT sales.time_id,
                                                                sales.jj_year,
                                                                sales.jj_qrtr,
                                                                sales.jj_mnth,
                                                                sales.jj_wk,
                                                                sales.jj_mnth_wk,
                                                                sales.jj_mnth_id,
                                                                sales.jj_mnth_tot,
                                                                sales.jj_mnth_day,
                                                                sales.jj_mnth_shrt,
                                                                sales.jj_mnth_long,
                                                                sales.cal_year,
                                                                sales.cal_qrtr,
                                                                sales.cal_mnth,
                                                                sales.cal_wk,
                                                                sales.cal_mnth_wk,
                                                                sales.cal_mnth_id,
                                                                sales.cal_mnth_nm,
                                                                sales.wk_end_dt,
                                                                sales.ac_attribute AS representative_cust_cd,
                                                                sales.cust_nm AS representative_cust_nm,
                                                                sales.channel_cd,
                                                                sales.channel_desc,
                                                                sales.country,
                                                                sales.sales_grp_cd,
                                                                sales.sales_grp_desc AS sales_grp_nm,
                                                                sales.iri_market,
                                                                sales.ac_nielsencode,
                                                                sales.ac_longname,
                                                                sales.iri_ean,
                                                                sales.iri_prod_desc,
                                                                ltrim(
                                                                    (sales.matl_id)::text,
                                                                    ('0'::character varying)::text
                                                                ) AS matl_id,
                                                                sales.matl_desc,
                                                                matl.master_code,
                                                                ltrim(
                                                                    (matl.parent_id)::text,
                                                                    ('0'::character varying)::text
                                                                ) AS parent_matl_id,
                                                                matl.parent_matl_desc,
                                                                sales.brnd_cd,
                                                                sales.brnd_desc,
                                                                sales.fran_cd,
                                                                sales.fran_desc,
                                                                sales.grp_fran_cd,
                                                                sales.grp_fran_desc,
                                                                sales.prod_fran_cd,
                                                                sales.prod_fran_desc,
                                                                sales.prod_mjr_cd,
                                                                sales.prod_mjr_desc,
                                                                sales.prod_mnr_cd,
                                                                sales.prod_mnr_desc,
                                                                sales.scan_units,
                                                                sales.scan_sales
                                                            FROM (
                                                                    (
                                                                        SELECT bar_cd_map.time_id,
                                                                            bar_cd_map.jj_year,
                                                                            bar_cd_map.jj_qrtr,
                                                                            bar_cd_map.jj_mnth,
                                                                            bar_cd_map.jj_wk,
                                                                            bar_cd_map.jj_mnth_wk,
                                                                            bar_cd_map.jj_mnth_id,
                                                                            bar_cd_map.jj_mnth_tot,
                                                                            bar_cd_map.jj_mnth_day,
                                                                            bar_cd_map.jj_mnth_shrt,
                                                                            bar_cd_map.jj_mnth_long,
                                                                            bar_cd_map.cal_year,
                                                                            bar_cd_map.cal_qrtr,
                                                                            bar_cd_map.cal_mnth,
                                                                            bar_cd_map.cal_wk,
                                                                            bar_cd_map.cal_mnth_wk,
                                                                            bar_cd_map.cal_mnth_id,
                                                                            bar_cd_map.cal_mnth_nm,
                                                                            bar_cd_map.wk_end_dt,
                                                                            bar_cd_map.ac_attribute,
                                                                            (bar_cd_map.cust_nm)::character varying AS cust_nm,
                                                                            bar_cd_map.channel_cd,
                                                                            bar_cd_map.channel_desc,
                                                                            bar_cd_map.sales_grp_cd,
                                                                            bar_cd_map.sales_grp_desc,
                                                                            bar_cd_map.cmp_id,
                                                                            bar_cd_map.country,
                                                                            bar_cd_map.iri_market,
                                                                            bar_cd_map.ac_nielsencode,
                                                                            bar_cd_map.ac_longname,
                                                                            bar_cd_map.iri_ean,
                                                                            bar_cd_map.matl_bar_cd,
                                                                            bar_cd_map.iri_prod_desc,
                                                                            (bar_cd_map.matl_id)::character varying AS matl_id,
                                                                            bar_cd_map.matl_desc,
                                                                            bar_cd_map.brnd_cd,
                                                                            bar_cd_map.brnd_desc,
                                                                            bar_cd_map.fran_cd,
                                                                            bar_cd_map.fran_desc,
                                                                            bar_cd_map.grp_fran_cd,
                                                                            bar_cd_map.grp_fran_desc,
                                                                            bar_cd_map.prod_fran_cd,
                                                                            bar_cd_map.prod_fran_desc,
                                                                            bar_cd_map.prod_mjr_cd,
                                                                            bar_cd_map.prod_mjr_desc,
                                                                            bar_cd_map.prod_mnr_cd,
                                                                            bar_cd_map.prod_mnr_desc,
                                                                            bar_cd_map.scan_units,
                                                                            bar_cd_map.scan_sales
                                                                        FROM (
                                                                                SELECT sales_derived.time_id,
                                                                                    sales_derived.jj_year,
                                                                                    sales_derived.jj_qrtr,
                                                                                    sales_derived.jj_mnth,
                                                                                    sales_derived.jj_wk,
                                                                                    sales_derived.jj_mnth_wk,
                                                                                    sales_derived.jj_mnth_id,
                                                                                    sales_derived.jj_mnth_tot,
                                                                                    sales_derived.jj_mnth_day,
                                                                                    sales_derived.jj_mnth_shrt,
                                                                                    sales_derived.jj_mnth_long,
                                                                                    sales_derived.cal_year,
                                                                                    sales_derived.cal_qrtr,
                                                                                    sales_derived.cal_mnth,
                                                                                    sales_derived.cal_wk,
                                                                                    sales_derived.cal_mnth_wk,
                                                                                    sales_derived.cal_mnth_id,
                                                                                    sales_derived.cal_mnth_nm,
                                                                                    sales_derived.wk_end_dt,
                                                                                    sales_derived.ac_code AS ac_attribute,
                                                                                    vcd.cust_nm,
                                                                                    vcd.channel_cd,
                                                                                    vcd.channel_desc,
                                                                                    sales_derived.sales_grp_cd,
                                                                                    sales_derived.sales_grp_desc,
                                                                                    vcd.cmp_id,
                                                                                    vcd.country,
                                                                                    sales_derived.iri_market,
                                                                                    sales_derived.ac_nielsencode,
                                                                                    sales_derived.ac_longname,
                                                                                    sales_derived.iri_ean,
                                                                                    sales_derived.matl_bar_cd,
                                                                                    sales_derived.iri_prod_desc,
                                                                                    ltrim(
                                                                                        (vmd.matl_id)::text,
                                                                                        ('0'::character varying)::text
                                                                                    ) AS matl_id,
                                                                                    vmd.matl_desc,
                                                                                    vmd.brnd_cd,
                                                                                    vmd.brnd_desc,
                                                                                    vmd.fran_cd,
                                                                                    vmd.fran_desc,
                                                                                    vmd.grp_fran_cd,
                                                                                    vmd.grp_fran_desc,
                                                                                    vmd.prod_fran_cd,
                                                                                    vmd.prod_fran_desc,
                                                                                    vmd.prod_mjr_cd,
                                                                                    vmd.prod_mjr_desc,
                                                                                    vmd.prod_mnr_cd,
                                                                                    vmd.prod_mnr_desc,
                                                                                    sales_derived.scan_units,
                                                                                    sales_derived.scan_sales
                                                                                FROM (
                                                                                        (
                                                                                            (
                                                                                                SELECT iss.iri_market,
                                                                                                    iss.wk_end_dt,
                                                                                                    iss.iri_prod_desc,
                                                                                                    iss.iri_ean,
                                                                                                    iss.ac_nielsencode,
                                                                                                    iss.ac_code,
                                                                                                    iss.ac_longname,
                                                                                                    iss.sales_grp_cd,
                                                                                                    iss.sales_grp_desc,
                                                                                                    iss.scan_sales,
                                                                                                    iss.scan_units,
                                                                                                    etd.cal_date,
                                                                                                    etd.time_id,
                                                                                                    etd.jj_wk,
                                                                                                    etd.jj_mnth,
                                                                                                    etd.jj_mnth_shrt,
                                                                                                    etd.jj_mnth_long,
                                                                                                    etd.jj_qrtr,
                                                                                                    etd.jj_year,
                                                                                                    etd.cal_mnth_id,
                                                                                                    etd.jj_mnth_id,
                                                                                                    etd.cal_mnth,
                                                                                                    etd.cal_qrtr,
                                                                                                    etd.cal_year,
                                                                                                    etd.jj_mnth_tot,
                                                                                                    etd.jj_mnth_day,
                                                                                                    etd.cal_mnth_nm,
                                                                                                    etd.jj_mnth_wk,
                                                                                                    etd.cal_wk,
                                                                                                    etd.cal_mnth_wk,
                                                                                                    ean.matl_id,
                                                                                                    ean.bar_cd AS matl_bar_cd
                                                                                                FROM (
                                                                                                        SELECT etd.cal_date,
                                                                                                            etd.time_id,
                                                                                                            etd.jj_wk,
                                                                                                            etd.jj_mnth,
                                                                                                            etd.jj_mnth_shrt,
                                                                                                            etd.jj_mnth_long,
                                                                                                            etd.jj_qrtr,
                                                                                                            etd.jj_year,
                                                                                                            etd.cal_mnth_id,
                                                                                                            etd.jj_mnth_id,
                                                                                                            etd.cal_mnth,
                                                                                                            etd.cal_qrtr,
                                                                                                            etd.cal_year,
                                                                                                            etd.jj_mnth_tot,
                                                                                                            etd.jj_mnth_day,
                                                                                                            etd.cal_mnth_nm,
                                                                                                            etdw.jj_mnth_wk,
                                                                                                            etdc.cal_wk,
                                                                                                            etdcm.cal_mnth_wk
                                                                                                        FROM PCFEDW_INTEGRATION.edw_time_dim etd,
                                                                                                            (
                                                                                                                SELECT etd.jj_year,
                                                                                                                    etd.jj_mnth_id,
                                                                                                                    etd.jj_wk,
                                                                                                                    row_number() OVER(
                                                                                                                        PARTITION BY etd.jj_year,
                                                                                                                        etd.jj_mnth_id
                                                                                                                        ORDER BY etd.jj_year,
                                                                                                                            etd.jj_mnth_id,
                                                                                                                            etd.jj_wk
                                                                                                                    ) AS jj_mnth_wk
                                                                                                                FROM (
                                                                                                                        SELECT DISTINCT etd.jj_year,
                                                                                                                            etd.jj_mnth_id,
                                                                                                                            etd.jj_wk
                                                                                                                        FROM PCFEDW_INTEGRATION.edw_time_dim etd
                                                                                                                    ) etd
                                                                                                            ) etdw,
                                                                                                            (
                                                                                                                SELECT etd.cal_date,
                                                                                                                    etd.time_id,
                                                                                                                    etd.jj_wk,
                                                                                                                    etd.jj_mnth,
                                                                                                                    etd.jj_mnth_shrt,
                                                                                                                    etd.jj_mnth_long,
                                                                                                                    etd.jj_qrtr,
                                                                                                                    etd.jj_year,
                                                                                                                    etd.cal_mnth_id,
                                                                                                                    etd.jj_mnth_id,
                                                                                                                    etd.cal_mnth,
                                                                                                                    etd.cal_qrtr,
                                                                                                                    etd.cal_year,
                                                                                                                    etd.jj_mnth_tot,
                                                                                                                    etd.jj_mnth_day,
                                                                                                                    etd.cal_mnth_nm,
                                                                                                                    CASE
                                                                                                                        WHEN (
                                                                                                                            (
                                                                                                                                row_number() OVER(
                                                                                                                                    PARTITION BY etd.cal_year
                                                                                                                                    ORDER BY TO_DATE(etd.cal_date)
                                                                                                                                ) % (7)::bigint
                                                                                                                            ) = 0
                                                                                                                        ) THEN (
                                                                                                                            row_number() OVER(
                                                                                                                                PARTITION BY etd.cal_year
                                                                                                                                ORDER BY TO_DATE(etd.cal_date)
                                                                                                                            ) / 7
                                                                                                                        )
                                                                                                                        ELSE (
                                                                                                                            (
                                                                                                                                row_number() OVER(
                                                                                                                                    PARTITION BY etd.cal_year
                                                                                                                                    ORDER BY TO_DATE(etd.cal_date)
                                                                                                                                ) / 7
                                                                                                                            ) + 1
                                                                                                                        )
                                                                                                                    END AS cal_wk
                                                                                                                FROM PCFEDW_INTEGRATION.edw_time_dim etd
                                                                                                            ) etdc,
                                                                                                            (
                                                                                                                SELECT etdcw.cal_year,
                                                                                                                    etdcw.cal_mnth_id,
                                                                                                                    etdcw.cal_wk,
                                                                                                                    row_number() OVER(
                                                                                                                        PARTITION BY etdcw.cal_year,
                                                                                                                        etdcw.cal_mnth_id
                                                                                                                        ORDER BY etdcw.cal_year,
                                                                                                                            etdcw.cal_mnth_id,
                                                                                                                            etdcw.cal_wk
                                                                                                                    ) AS cal_mnth_wk
                                                                                                                FROM (
                                                                                                                        SELECT DISTINCT etdc.cal_year,
                                                                                                                            etdc.cal_mnth_id,
                                                                                                                            etdc.cal_wk
                                                                                                                        FROM (
                                                                                                                                SELECT etd.cal_year,
                                                                                                                                    etd.cal_mnth_id,
                                                                                                                                    CASE
                                                                                                                                        WHEN (
                                                                                                                                            (
                                                                                                                                                row_number() OVER(
                                                                                                                                                    PARTITION BY etd.cal_year
                                                                                                                                                    ORDER BY TO_DATE(etd.cal_date)
                                                                                                                                                ) % (7)::bigint
                                                                                                                                            ) = 0
                                                                                                                                        ) THEN (
                                                                                                                                            row_number() OVER(
                                                                                                                                                PARTITION BY etd.cal_year
                                                                                                                                                ORDER BY TO_DATE(etd.cal_date)
                                                                                                                                            ) / 7
                                                                                                                                        )
                                                                                                                                        ELSE (
                                                                                                                                            (
                                                                                                                                                row_number() OVER(
                                                                                                                                                    PARTITION BY etd.cal_year
                                                                                                                                                    ORDER BY TO_DATE(etd.cal_date)
                                                                                                                                                ) / 7
                                                                                                                                            ) + 1
                                                                                                                                        )
                                                                                                                                    END AS cal_wk
                                                                                                                                FROM PCFEDW_INTEGRATION.edw_time_dim etd
                                                                                                                            ) etdc
                                                                                                                    ) etdcw
                                                                                                            ) etdcm
                                                                                                        WHERE (
                                                                                                                (
                                                                                                                    (
                                                                                                                        (
                                                                                                                            (
                                                                                                                                (
                                                                                                                                    (etd.jj_year = etdw.jj_year)
                                                                                                                                    AND (etd.jj_mnth_id = etdw.jj_mnth_id)
                                                                                                                                )
                                                                                                                                AND (etd.jj_wk = etdw.jj_wk)
                                                                                                                            )
                                                                                                                            AND (TO_DATE(etd.cal_date) = TO_DATE(etdc.cal_date))
                                                                                                                        )
                                                                                                                        AND (etdc.cal_wk = etdcm.cal_wk)
                                                                                                                    )
                                                                                                                    AND (etdc.cal_year = etdcm.cal_year)
                                                                                                                )
                                                                                                                AND (etdc.cal_mnth_id = etdcm.cal_mnth_id)
                                                                                                            )
                                                                                                    ) etd,
                                                                                                    (
                                                                                                        PCFEDW_INTEGRATION.vw_iri_scan_sales iss
                                                                                                        LEFT JOIN (
                                                                                                            SELECT edw_material_dim.matl_id,
                                                                                                                edw_material_dim.matl_desc,
                                                                                                                edw_material_dim.mega_brnd_cd,
                                                                                                                edw_material_dim.mega_brnd_desc,
                                                                                                                edw_material_dim.brnd_cd,
                                                                                                                edw_material_dim.brnd_desc,
                                                                                                                edw_material_dim.base_prod_cd,
                                                                                                                edw_material_dim.base_prod_desc,
                                                                                                                edw_material_dim.variant_cd,
                                                                                                                edw_material_dim.variant_desc,
                                                                                                                edw_material_dim.fran_cd,
                                                                                                                edw_material_dim.fran_desc,
                                                                                                                edw_material_dim.grp_fran_cd,
                                                                                                                edw_material_dim.grp_fran_desc,
                                                                                                                edw_material_dim.matl_type_cd,
                                                                                                                edw_material_dim.matl_type_desc,
                                                                                                                edw_material_dim.prod_fran_cd,
                                                                                                                edw_material_dim.prod_fran_desc,
                                                                                                                edw_material_dim.prod_hier_cd,
                                                                                                                edw_material_dim.prod_hier_desc,
                                                                                                                edw_material_dim.prod_mjr_cd,
                                                                                                                edw_material_dim.prod_mjr_desc,
                                                                                                                edw_material_dim.prod_mnr_cd,
                                                                                                                edw_material_dim.prod_mnr_desc,
                                                                                                                edw_material_dim.mercia_plan,
                                                                                                                edw_material_dim.putup_cd,
                                                                                                                edw_material_dim.putup_desc,
                                                                                                                edw_material_dim.bar_cd,
                                                                                                                edw_material_dim.updt_dt,
                                                                                                                edw_material_dim.prft_ctr
                                                                                                            FROM PCFEDW_INTEGRATION.edw_material_dim
                                                                                                            WHERE (
                                                                                                                    edw_material_dim.bar_cd IN (
                                                                                                                        SELECT DISTINCT derived_table1.bar_cd
                                                                                                                        FROM (
                                                                                                                                SELECT count(*) AS count,
                                                                                                                                    edw_material_dim.bar_cd
                                                                                                                                FROM PCFEDW_INTEGRATION.edw_material_dim
                                                                                                                                GROUP BY edw_material_dim.bar_cd
                                                                                                                                HAVING (count(*) = 1)
                                                                                                                            ) derived_table1
                                                                                                                    )
                                                                                                                )
                                                                                                        ) ean ON (
                                                                                                            (
                                                                                                                ltrim(
                                                                                                                    (ean.bar_cd)::text,
                                                                                                                    ('0'::character varying)::text
                                                                                                                ) = ltrim(
                                                                                                                    (COALESCE(iss.iri_ean, '0'::character varying))::text,
                                                                                                                    ('0'::character varying)::text
                                                                                                                )
                                                                                                            )
                                                                                                        )
                                                                                                    )
                                                                                                WHERE (
                                                                                                        TO_DATE((iss.wk_end_dt)::timestamp without time zone) = TO_DATE(etd.cal_date)
                                                                                                    )
                                                                                            ) sales_derived
                                                                                            LEFT JOIN PCFEDW_INTEGRATION.edw_material_dim vmd ON (
                                                                                                (
                                                                                                    ltrim(
                                                                                                        (vmd.matl_id)::text,
                                                                                                        ('0'::character varying)::text
                                                                                                    ) = ltrim(
                                                                                                        (
                                                                                                            COALESCE(sales_derived.matl_id, '0'::character varying)
                                                                                                        )::text,
                                                                                                        ('0'::character varying)::text
                                                                                                    )
                                                                                                )
                                                                                            )
                                                                                        )
                                                                                        LEFT JOIN PCFEDW_INTEGRATION.vw_customer_dim vcd ON (
                                                                                            (
                                                                                                ltrim(
                                                                                                    (vcd.cust_no)::text,
                                                                                                    ('0'::character varying)::text
                                                                                                ) = ltrim(
                                                                                                    (sales_derived.ac_code)::text,
                                                                                                    ('0'::character varying)::text
                                                                                                )
                                                                                            )
                                                                                        )
                                                                                    )
                                                                                WHERE (
                                                                                        sales_derived.matl_bar_cd IN (
                                                                                            SELECT DISTINCT derived_table2.bar_cd
                                                                                            FROM (
                                                                                                    SELECT count(*) AS count,
                                                                                                        edw_material_dim.bar_cd
                                                                                                    FROM PCFEDW_INTEGRATION.edw_material_dim
                                                                                                    GROUP BY edw_material_dim.bar_cd
                                                                                                    HAVING (count(*) = 1)
                                                                                                ) derived_table2
                                                                                        )
                                                                                    )
                                                                            ) bar_cd_map
                                                                        UNION ALL
                                                                        SELECT sales_derived.time_id,
                                                                            sales_derived.jj_year,
                                                                            sales_derived.jj_qrtr,
                                                                            sales_derived.jj_mnth,
                                                                            sales_derived.jj_wk,
                                                                            sales_derived.jj_mnth_wk,
                                                                            sales_derived.jj_mnth_id,
                                                                            sales_derived.jj_mnth_tot,
                                                                            sales_derived.jj_mnth_day,
                                                                            sales_derived.jj_mnth_shrt,
                                                                            sales_derived.jj_mnth_long,
                                                                            sales_derived.cal_year,
                                                                            sales_derived.cal_qrtr,
                                                                            sales_derived.cal_mnth,
                                                                            sales_derived.cal_wk,
                                                                            sales_derived.cal_mnth_wk,
                                                                            sales_derived.cal_mnth_id,
                                                                            sales_derived.cal_mnth_nm,
                                                                            sales_derived.wk_end_dt,
                                                                            sales_derived.ac_code AS ac_attribute,
                                                                            (vcd.cust_nm)::character varying AS cust_nm,
                                                                            vcd.channel_cd,
                                                                            vcd.channel_desc,
                                                                            sales_derived.sales_grp_cd,
                                                                            sales_derived.sales_grp_desc,
                                                                            vcd.cmp_id,
                                                                            vcd.country,
                                                                            sales_derived.iri_market,
                                                                            sales_derived.ac_nielsencode,
                                                                            sales_derived.ac_longname,
                                                                            sales_derived.iri_ean,
                                                                            NULL::character varying AS matl_bar_cd,
                                                                            sales_derived.iri_prod_desc,
                                                                            (
                                                                                ltrim(
                                                                                    (vmd.matl_id)::text,
                                                                                    ('0'::character varying)::text
                                                                                )
                                                                            )::character varying AS matl_id,
                                                                            vmd.matl_desc,
                                                                            vmd.brnd_cd,
                                                                            vmd.brnd_desc,
                                                                            vmd.fran_cd,
                                                                            vmd.fran_desc,
                                                                            vmd.grp_fran_cd,
                                                                            vmd.grp_fran_desc,
                                                                            vmd.prod_fran_cd,
                                                                            vmd.prod_fran_desc,
                                                                            vmd.prod_mjr_cd,
                                                                            vmd.prod_mjr_desc,
                                                                            vmd.prod_mnr_cd,
                                                                            vmd.prod_mnr_desc,
                                                                            sales_derived.scan_units,
                                                                            sales_derived.scan_sales
                                                                        FROM (
                                                                                (
                                                                                    (
                                                                                        SELECT iss.iri_market,
                                                                                            iss.wk_end_dt,
                                                                                            iss.iri_prod_desc,
                                                                                            iss.iri_ean,
                                                                                            iss.ac_nielsencode,
                                                                                            iss.ac_code,
                                                                                            iss.ac_longname,
                                                                                            iss.sales_grp_cd,
                                                                                            iss.sales_grp_desc,
                                                                                            iss.scan_sales,
                                                                                            iss.scan_units,
                                                                                            etd.cal_date,
                                                                                            etd.time_id,
                                                                                            etd.jj_wk,
                                                                                            etd.jj_mnth,
                                                                                            etd.jj_mnth_shrt,
                                                                                            etd.jj_mnth_long,
                                                                                            etd.jj_qrtr,
                                                                                            etd.jj_year,
                                                                                            etd.cal_mnth_id,
                                                                                            etd.jj_mnth_id,
                                                                                            etd.cal_mnth,
                                                                                            etd.cal_qrtr,
                                                                                            etd.cal_year,
                                                                                            etd.jj_mnth_tot,
                                                                                            etd.jj_mnth_day,
                                                                                            etd.cal_mnth_nm,
                                                                                            etd.jj_mnth_wk,
                                                                                            etd.cal_wk,
                                                                                            etd.cal_mnth_wk
                                                                                        FROM (
                                                                                                SELECT etd.cal_date,
                                                                                                    etd.time_id,
                                                                                                    etd.jj_wk,
                                                                                                    etd.jj_mnth,
                                                                                                    etd.jj_mnth_shrt,
                                                                                                    etd.jj_mnth_long,
                                                                                                    etd.jj_qrtr,
                                                                                                    etd.jj_year,
                                                                                                    etd.cal_mnth_id,
                                                                                                    etd.jj_mnth_id,
                                                                                                    etd.cal_mnth,
                                                                                                    etd.cal_qrtr,
                                                                                                    etd.cal_year,
                                                                                                    etd.jj_mnth_tot,
                                                                                                    etd.jj_mnth_day,
                                                                                                    etd.cal_mnth_nm,
                                                                                                    etdw.jj_mnth_wk,
                                                                                                    etdc.cal_wk,
                                                                                                    etdcm.cal_mnth_wk
                                                                                                FROM PCFEDW_INTEGRATION.edw_time_dim etd,
                                                                                                    (
                                                                                                        SELECT etd.jj_year,
                                                                                                            etd.jj_mnth_id,
                                                                                                            etd.jj_wk,
                                                                                                            row_number() OVER(
                                                                                                                PARTITION BY etd.jj_year,
                                                                                                                etd.jj_mnth_id
                                                                                                                ORDER BY etd.jj_year,
                                                                                                                    etd.jj_mnth_id,
                                                                                                                    etd.jj_wk
                                                                                                            ) AS jj_mnth_wk
                                                                                                        FROM (
                                                                                                                SELECT DISTINCT etd.jj_year,
                                                                                                                    etd.jj_mnth_id,
                                                                                                                    etd.jj_wk
                                                                                                                FROM PCFEDW_INTEGRATION.edw_time_dim etd
                                                                                                            ) etd
                                                                                                    ) etdw,
                                                                                                    (
                                                                                                        SELECT etd.cal_date,
                                                                                                            etd.time_id,
                                                                                                            etd.jj_wk,
                                                                                                            etd.jj_mnth,
                                                                                                            etd.jj_mnth_shrt,
                                                                                                            etd.jj_mnth_long,
                                                                                                            etd.jj_qrtr,
                                                                                                            etd.jj_year,
                                                                                                            etd.cal_mnth_id,
                                                                                                            etd.jj_mnth_id,
                                                                                                            etd.cal_mnth,
                                                                                                            etd.cal_qrtr,
                                                                                                            etd.cal_year,
                                                                                                            etd.jj_mnth_tot,
                                                                                                            etd.jj_mnth_day,
                                                                                                            etd.cal_mnth_nm,
                                                                                                            CASE
                                                                                                                WHEN (
                                                                                                                    (
                                                                                                                        row_number() OVER(
                                                                                                                            PARTITION BY etd.cal_year
                                                                                                                            ORDER BY TO_DATE(etd.cal_date)
                                                                                                                        ) % (7)::bigint
                                                                                                                    ) = 0
                                                                                                                ) THEN (
                                                                                                                    row_number() OVER(
                                                                                                                        PARTITION BY etd.cal_year
                                                                                                                        ORDER BY TO_DATE(etd.cal_date)
                                                                                                                    ) / 7
                                                                                                                )
                                                                                                                ELSE (
                                                                                                                    (
                                                                                                                        row_number() OVER(
                                                                                                                            PARTITION BY etd.cal_year
                                                                                                                            ORDER BY TO_DATE(etd.cal_date)
                                                                                                                        ) / 7
                                                                                                                    ) + 1
                                                                                                                )
                                                                                                            END AS cal_wk
                                                                                                        FROM PCFEDW_INTEGRATION.edw_time_dim etd
                                                                                                    ) etdc,
                                                                                                    (
                                                                                                        SELECT etdcw.cal_year,
                                                                                                            etdcw.cal_mnth_id,
                                                                                                            etdcw.cal_wk,
                                                                                                            row_number() OVER(
                                                                                                                PARTITION BY etdcw.cal_year,
                                                                                                                etdcw.cal_mnth_id
                                                                                                                ORDER BY etdcw.cal_year,
                                                                                                                    etdcw.cal_mnth_id,
                                                                                                                    etdcw.cal_wk
                                                                                                            ) AS cal_mnth_wk
                                                                                                        FROM (
                                                                                                                SELECT DISTINCT etdc.cal_year,
                                                                                                                    etdc.cal_mnth_id,
                                                                                                                    etdc.cal_wk
                                                                                                                FROM (
                                                                                                                        SELECT etd.cal_year,
                                                                                                                            etd.cal_mnth_id,
                                                                                                                            CASE
                                                                                                                                WHEN (
                                                                                                                                    (
                                                                                                                                        row_number() OVER(
                                                                                                                                            PARTITION BY etd.cal_year
                                                                                                                                            ORDER BY TO_DATE(etd.cal_date)
                                                                                                                                        ) % (7)::bigint
                                                                                                                                    ) = 0
                                                                                                                                ) THEN (
                                                                                                                                    row_number() OVER(
                                                                                                                                        PARTITION BY etd.cal_year
                                                                                                                                        ORDER BY TO_DATE(etd.cal_date)
                                                                                                                                    ) / 7
                                                                                                                                )
                                                                                                                                ELSE (
                                                                                                                                    (
                                                                                                                                        row_number() OVER(
                                                                                                                                            PARTITION BY etd.cal_year
                                                                                                                                            ORDER BY TO_DATE(etd.cal_date)
                                                                                                                                        ) / 7
                                                                                                                                    ) + 1
                                                                                                                                )
                                                                                                                            END AS cal_wk
                                                                                                                        FROM PCFEDW_INTEGRATION.edw_time_dim etd
                                                                                                                    ) etdc
                                                                                                            ) etdcw
                                                                                                    ) etdcm
                                                                                                WHERE (
                                                                                                        (
                                                                                                            (
                                                                                                                (
                                                                                                                    (
                                                                                                                        (
                                                                                                                            (etd.jj_year = etdw.jj_year)
                                                                                                                            AND (etd.jj_mnth_id = etdw.jj_mnth_id)
                                                                                                                        )
                                                                                                                        AND (etd.jj_wk = etdw.jj_wk)
                                                                                                                    )
                                                                                                                    AND (TO_DATE(etd.cal_date) = TO_DATE(etdc.cal_date))
                                                                                                                )
                                                                                                                AND (etdc.cal_wk = etdcm.cal_wk)
                                                                                                            )
                                                                                                            AND (etdc.cal_year = etdcm.cal_year)
                                                                                                        )
                                                                                                        AND (etdc.cal_mnth_id = etdcm.cal_mnth_id)
                                                                                                    )
                                                                                            ) etd,
                                                                                            PCFEDW_INTEGRATION.vw_iri_scan_sales iss
                                                                                        WHERE (
                                                                                                TO_DATE((iss.wk_end_dt)::timestamp without time zone) = TO_DATE(etd.cal_date)
                                                                                            )
                                                                                    ) sales_derived
                                                                                    LEFT JOIN (
                                                                                        (
                                                                                            SELECT derived_table6.jj_month_id,
                                                                                                derived_table6.bar_cd,
                                                                                                derived_table6.cust_no,
                                                                                                derived_table6.material_id
                                                                                            FROM (
                                                                                                    SELECT DISTINCT derived_table5.jj_month_id,
                                                                                                        derived_table5.bar_cd,
                                                                                                        derived_table5.cust_no,
                                                                                                        derived_table5.material_id
                                                                                                    FROM (
                                                                                                            SELECT DISTINCT derived_table4.jj_month_id,
                                                                                                                derived_table4.master_code,
                                                                                                                derived_table4.bar_cd,
                                                                                                                derived_table4.cust_no,
                                                                                                                derived_table4.material_count,
                                                                                                                derived_table4.gts_val,
                                                                                                                count(DISTINCT derived_table4.master_code) AS count,
                                                                                                                CASE
                                                                                                                    WHEN (count(DISTINCT derived_table4.master_code) > 1) THEN CASE
                                                                                                                        WHEN (
                                                                                                                            (
                                                                                                                                (derived_table4.master_code IS NOT NULL)
                                                                                                                                AND (
                                                                                                                                    derived_table4.gts_val >= ((0)::numeric)::numeric(18, 0)
                                                                                                                                )
                                                                                                                            )
                                                                                                                            AND (
                                                                                                                                upper((derived_table4.channel_desc)::text) <> ('AU - EXPORTS'::character varying)::text
                                                                                                                            )
                                                                                                                        ) THEN (derived_table4.max_material_id)::character varying
                                                                                                                        WHEN (
                                                                                                                            (
                                                                                                                                (derived_table4.master_code IS NULL)
                                                                                                                                AND (
                                                                                                                                    derived_table4.gts_val < ((0)::numeric)::numeric(18, 0)
                                                                                                                                )
                                                                                                                            )
                                                                                                                            AND (
                                                                                                                                upper((derived_table4.channel_desc)::text) = ('AU - EXPORTS'::character varying)::text
                                                                                                                            )
                                                                                                                        ) THEN 'NULL'::character varying
                                                                                                                        ELSE NULL::character varying
                                                                                                                    END
                                                                                                                    WHEN (count(DISTINCT derived_table4.master_code) = 1) THEN CASE
                                                                                                                        WHEN (
                                                                                                                            (derived_table4.material_count > 1)
                                                                                                                            AND (
                                                                                                                                derived_table4.gts_val >= ((0)::numeric)::numeric(18, 0)
                                                                                                                            )
                                                                                                                        ) THEN (derived_table4.max_material_id)::character varying
                                                                                                                        WHEN (derived_table4.material_count = 1) THEN (derived_table4.max_material_id)::character varying
                                                                                                                        ELSE NULL::character varying
                                                                                                                    END
                                                                                                                    ELSE derived_table4.material_id
                                                                                                                END AS material_id
                                                                                                            FROM (
                                                                                                                    SELECT DISTINCT a.jj_month_id,
                                                                                                                        a.master_code,
                                                                                                                        a.bar_cd,
                                                                                                                        a.matl_id AS material_id,
                                                                                                                        a.cust_no,
                                                                                                                        count(a.matl_id) OVER(
                                                                                                                            PARTITION BY a.jj_month_id,
                                                                                                                            a.bar_cd,
                                                                                                                            a.cust_no ORDER BY NULL ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                                                                                                                        ) AS material_count,
                                                                                                                        "max"((a.matl_id)::text) OVER(
                                                                                                                            PARTITION BY a.jj_month_id,
                                                                                                                            a.master_code,
                                                                                                                            a.bar_cd,
                                                                                                                            a.cust_no,
                                                                                                                            sum(a.gts_val) ORDER BY NULL ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                                                                                                                        ) AS max_material_id,
                                                                                                                        row_number() OVER(
                                                                                                                            PARTITION BY a.jj_month_id,
                                                                                                                            a.bar_cd,
                                                                                                                            a.cust_no
                                                                                                                            ORDER BY sum(a.gts_val) DESC
                                                                                                                        ) AS sales_rank,
                                                                                                                        count(COALESCE(a.master_code, 'NA'::character varying)) OVER(
                                                                                                                            PARTITION BY a.jj_month_id,
                                                                                                                            a.bar_cd,
                                                                                                                            a.cust_no,
                                                                                                                            a.matl_id ORDER BY NULL ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                                                                                                                        ) AS master_code_count,
                                                                                                                        sum(a.gts_val) AS gts_val,
                                                                                                                        a.channel_desc,
                                                                                                                        b.matl_bar_count
                                                                                                                    FROM (
                                                                                                                            (
                                                                                                                                SELECT vsf.jj_month_id,
                                                                                                                                    vsf.gts_val,
                                                                                                                                    vmd.matl_id,
                                                                                                                                    vmd.bar_cd,
                                                                                                                                    mstrcd.master_code,
                                                                                                                                    vcd.cust_no,
                                                                                                                                    vcd.channel_desc
                                                                                                                                FROM (
                                                                                                                                        (
                                                                                                                                            (
                                                                                                                                                PCFEDW_INTEGRATION.vw_sapbw_ciw_fact vsf
                                                                                                                                                LEFT JOIN PCFEDW_INTEGRATION.vw_customer_dim vcd ON (((vsf.cust_no)::text = (vcd.cust_no)::text))
                                                                                                                                            )
                                                                                                                                            LEFT JOIN PCFEDW_INTEGRATION.edw_material_dim vmd ON (((vsf.matl_id)::text = (vmd.matl_id)::text))
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
                                                                                                                                                ((vsf.cmp_id)::text = (vapcd.cmp_id)::text)
                                                                                                                                                AND ((vsf.matl_id)::text = (vapcd.matl_id)::text)
                                                                                                                                            )
                                                                                                                                        )
                                                                                                                                    )
                                                                                                                            ) a
                                                                                                                            JOIN (
                                                                                                                                SELECT DISTINCT edw_material_dim.bar_cd,
                                                                                                                                    count(DISTINCT edw_material_dim.matl_id) AS matl_bar_count
                                                                                                                                FROM PCFEDW_INTEGRATION.edw_material_dim
                                                                                                                                WHERE (
                                                                                                                                        COALESCE(edw_material_dim.bar_cd, 'NA'::character varying) IN (
                                                                                                                                            SELECT DISTINCT COALESCE(derived_table3.bar_cd, 'NA'::character varying) AS "coalesce"
                                                                                                                                            FROM (
                                                                                                                                                    SELECT count(*) AS count,
                                                                                                                                                        edw_material_dim.bar_cd
                                                                                                                                                    FROM PCFEDW_INTEGRATION.edw_material_dim
                                                                                                                                                    GROUP BY edw_material_dim.bar_cd
                                                                                                                                                    HAVING (count(*) > 1)
                                                                                                                                                ) derived_table3
                                                                                                                                        )
                                                                                                                                    )
                                                                                                                                GROUP BY edw_material_dim.bar_cd
                                                                                                                            ) b ON (((a.bar_cd)::text = (b.bar_cd)::text))
                                                                                                                        )
                                                                                                                    GROUP BY a.jj_month_id,
                                                                                                                        a.master_code,
                                                                                                                        a.bar_cd,
                                                                                                                        a.matl_id,
                                                                                                                        a.cust_no,
                                                                                                                        a.channel_desc,
                                                                                                                        b.matl_bar_count
                                                                                                                ) derived_table4
                                                                                                            WHERE (derived_table4.sales_rank = 1)
                                                                                                            GROUP BY derived_table4.jj_month_id,
                                                                                                                derived_table4.master_code,
                                                                                                                derived_table4.bar_cd,
                                                                                                                derived_table4.cust_no,
                                                                                                                derived_table4.material_id,
                                                                                                                derived_table4.material_count,
                                                                                                                derived_table4.max_material_id,
                                                                                                                derived_table4.master_code_count,
                                                                                                                derived_table4.channel_desc,
                                                                                                                derived_table4.gts_val,
                                                                                                                derived_table4.matl_bar_count
                                                                                                        ) derived_table5
                                                                                                ) derived_table6
                                                                                        ) ean
                                                                                        LEFT JOIN PCFEDW_INTEGRATION.edw_material_dim vmd ON (
                                                                                            (
                                                                                                ltrim(
                                                                                                    (vmd.matl_id)::text,
                                                                                                    ('0'::character varying)::text
                                                                                                ) = ltrim(
                                                                                                    (
                                                                                                        COALESCE(ean.material_id, '0'::character varying)
                                                                                                    )::text,
                                                                                                    ('0'::character varying)::text
                                                                                                )
                                                                                            )
                                                                                        )
                                                                                    ) ON (
                                                                                        (
                                                                                            (
                                                                                                (
                                                                                                    ((ean.jj_month_id)::numeric)::numeric(18, 0) = sales_derived.jj_mnth_id
                                                                                                )
                                                                                                AND (
                                                                                                    ltrim(
                                                                                                        (ean.cust_no)::text,
                                                                                                        ('0'::character varying)::text
                                                                                                    ) = (sales_derived.ac_code)::text
                                                                                                )
                                                                                            )
                                                                                            AND (
                                                                                                (COALESCE(ean.bar_cd, '0'::character varying))::text = (
                                                                                                    COALESCE(sales_derived.iri_ean, '0'::character varying)
                                                                                                )::text
                                                                                            )
                                                                                        )
                                                                                    )
                                                                                )
                                                                                LEFT JOIN PCFEDW_INTEGRATION.vw_customer_dim vcd ON (
                                                                                    (
                                                                                        ltrim(
                                                                                            (vcd.cust_no)::text,
                                                                                            ('0'::character varying)::text
                                                                                        ) = ltrim(
                                                                                            (sales_derived.ac_code)::text,
                                                                                            ('0'::character varying)::text
                                                                                        )
                                                                                    )
                                                                                )
                                                                            )
                                                                        WHERE (
                                                                                NOT (
                                                                                    COALESCE(sales_derived.iri_ean, 'NA'::character varying) IN (
                                                                                        SELECT DISTINCT COALESCE(bar_cd_map.iri_ean, '0'::character varying) AS "coalesce"
                                                                                        FROM (
                                                                                                SELECT sales_derived.time_id,
                                                                                                    sales_derived.jj_year,
                                                                                                    sales_derived.jj_qrtr,
                                                                                                    sales_derived.jj_mnth,
                                                                                                    sales_derived.jj_wk,
                                                                                                    sales_derived.jj_mnth_wk,
                                                                                                    sales_derived.jj_mnth_id,
                                                                                                    sales_derived.jj_mnth_tot,
                                                                                                    sales_derived.jj_mnth_day,
                                                                                                    sales_derived.jj_mnth_shrt,
                                                                                                    sales_derived.jj_mnth_long,
                                                                                                    sales_derived.cal_year,
                                                                                                    sales_derived.cal_qrtr,
                                                                                                    sales_derived.cal_mnth,
                                                                                                    sales_derived.cal_wk,
                                                                                                    sales_derived.cal_mnth_wk,
                                                                                                    sales_derived.cal_mnth_id,
                                                                                                    sales_derived.cal_mnth_nm,
                                                                                                    sales_derived.wk_end_dt,
                                                                                                    sales_derived.ac_code AS ac_attribute,
                                                                                                    vcd.cust_nm,
                                                                                                    vcd.channel_cd,
                                                                                                    vcd.channel_desc,
                                                                                                    sales_derived.sales_grp_cd,
                                                                                                    sales_derived.sales_grp_desc,
                                                                                                    vcd.cmp_id,
                                                                                                    vcd.country,
                                                                                                    sales_derived.iri_market,
                                                                                                    sales_derived.ac_nielsencode,
                                                                                                    sales_derived.ac_longname,
                                                                                                    sales_derived.iri_ean,
                                                                                                    sales_derived.matl_bar_cd,
                                                                                                    sales_derived.iri_prod_desc,
                                                                                                    ltrim(
                                                                                                        (vmd.matl_id)::text,
                                                                                                        ('0'::character varying)::text
                                                                                                    ) AS matl_id,
                                                                                                    vmd.matl_desc,
                                                                                                    vmd.brnd_cd,
                                                                                                    vmd.brnd_desc,
                                                                                                    vmd.fran_cd,
                                                                                                    vmd.fran_desc,
                                                                                                    vmd.grp_fran_cd,
                                                                                                    vmd.grp_fran_desc,
                                                                                                    vmd.prod_fran_cd,
                                                                                                    vmd.prod_fran_desc,
                                                                                                    vmd.prod_mjr_cd,
                                                                                                    vmd.prod_mjr_desc,
                                                                                                    vmd.prod_mnr_cd,
                                                                                                    vmd.prod_mnr_desc,
                                                                                                    sales_derived.scan_units,
                                                                                                    sales_derived.scan_sales
                                                                                                FROM (
                                                                                                        (
                                                                                                            (
                                                                                                                SELECT iss.iri_market,
                                                                                                                    iss.wk_end_dt,
                                                                                                                    iss.iri_prod_desc,
                                                                                                                    iss.iri_ean,
                                                                                                                    iss.ac_nielsencode,
                                                                                                                    iss.ac_code,
                                                                                                                    iss.ac_longname,
                                                                                                                    iss.sales_grp_cd,
                                                                                                                    iss.sales_grp_desc,
                                                                                                                    iss.scan_sales,
                                                                                                                    iss.scan_units,
                                                                                                                    etd.cal_date,
                                                                                                                    etd.time_id,
                                                                                                                    etd.jj_wk,
                                                                                                                    etd.jj_mnth,
                                                                                                                    etd.jj_mnth_shrt,
                                                                                                                    etd.jj_mnth_long,
                                                                                                                    etd.jj_qrtr,
                                                                                                                    etd.jj_year,
                                                                                                                    etd.cal_mnth_id,
                                                                                                                    etd.jj_mnth_id,
                                                                                                                    etd.cal_mnth,
                                                                                                                    etd.cal_qrtr,
                                                                                                                    etd.cal_year,
                                                                                                                    etd.jj_mnth_tot,
                                                                                                                    etd.jj_mnth_day,
                                                                                                                    etd.cal_mnth_nm,
                                                                                                                    etd.jj_mnth_wk,
                                                                                                                    etd.cal_wk,
                                                                                                                    etd.cal_mnth_wk,
                                                                                                                    ean.matl_id,
                                                                                                                    ean.bar_cd AS matl_bar_cd
                                                                                                                FROM (
                                                                                                                        SELECT etd.cal_date,
                                                                                                                            etd.time_id,
                                                                                                                            etd.jj_wk,
                                                                                                                            etd.jj_mnth,
                                                                                                                            etd.jj_mnth_shrt,
                                                                                                                            etd.jj_mnth_long,
                                                                                                                            etd.jj_qrtr,
                                                                                                                            etd.jj_year,
                                                                                                                            etd.cal_mnth_id,
                                                                                                                            etd.jj_mnth_id,
                                                                                                                            etd.cal_mnth,
                                                                                                                            etd.cal_qrtr,
                                                                                                                            etd.cal_year,
                                                                                                                            etd.jj_mnth_tot,
                                                                                                                            etd.jj_mnth_day,
                                                                                                                            etd.cal_mnth_nm,
                                                                                                                            etdw.jj_mnth_wk,
                                                                                                                            etdc.cal_wk,
                                                                                                                            etdcm.cal_mnth_wk
                                                                                                                        FROM PCFEDW_INTEGRATION.edw_time_dim etd,
                                                                                                                            (
                                                                                                                                SELECT etd.jj_year,
                                                                                                                                    etd.jj_mnth_id,
                                                                                                                                    etd.jj_wk,
                                                                                                                                    row_number() OVER(
                                                                                                                                        PARTITION BY etd.jj_year,
                                                                                                                                        etd.jj_mnth_id
                                                                                                                                        ORDER BY etd.jj_year,
                                                                                                                                            etd.jj_mnth_id,
                                                                                                                                            etd.jj_wk
                                                                                                                                    ) AS jj_mnth_wk
                                                                                                                                FROM (
                                                                                                                                        SELECT DISTINCT etd.jj_year,
                                                                                                                                            etd.jj_mnth_id,
                                                                                                                                            etd.jj_wk
                                                                                                                                        FROM PCFEDW_INTEGRATION.edw_time_dim etd
                                                                                                                                    ) etd
                                                                                                                            ) etdw,
                                                                                                                            (
                                                                                                                                SELECT etd.cal_date,
                                                                                                                                    etd.time_id,
                                                                                                                                    etd.jj_wk,
                                                                                                                                    etd.jj_mnth,
                                                                                                                                    etd.jj_mnth_shrt,
                                                                                                                                    etd.jj_mnth_long,
                                                                                                                                    etd.jj_qrtr,
                                                                                                                                    etd.jj_year,
                                                                                                                                    etd.cal_mnth_id,
                                                                                                                                    etd.jj_mnth_id,
                                                                                                                                    etd.cal_mnth,
                                                                                                                                    etd.cal_qrtr,
                                                                                                                                    etd.cal_year,
                                                                                                                                    etd.jj_mnth_tot,
                                                                                                                                    etd.jj_mnth_day,
                                                                                                                                    etd.cal_mnth_nm,
                                                                                                                                    CASE
                                                                                                                                        WHEN (
                                                                                                                                            (
                                                                                                                                                row_number() OVER(
                                                                                                                                                    PARTITION BY etd.cal_year
                                                                                                                                                    ORDER BY TO_DATE(etd.cal_date)
                                                                                                                                                ) % (7)::bigint
                                                                                                                                            ) = 0
                                                                                                                                        ) THEN (
                                                                                                                                            row_number() OVER(
                                                                                                                                                PARTITION BY etd.cal_year
                                                                                                                                                ORDER BY TO_DATE(etd.cal_date)
                                                                                                                                            ) / 7
                                                                                                                                        )
                                                                                                                                        ELSE (
                                                                                                                                            (
                                                                                                                                                row_number() OVER(
                                                                                                                                                    PARTITION BY etd.cal_year
                                                                                                                                                    ORDER BY TO_DATE(etd.cal_date)
                                                                                                                                                ) / 7
                                                                                                                                            ) + 1
                                                                                                                                        )
                                                                                                                                    END AS cal_wk
                                                                                                                                FROM PCFEDW_INTEGRATION.edw_time_dim etd
                                                                                                                            ) etdc,
                                                                                                                            (
                                                                                                                                SELECT etdcw.cal_year,
                                                                                                                                    etdcw.cal_mnth_id,
                                                                                                                                    etdcw.cal_wk,
                                                                                                                                    row_number() OVER(
                                                                                                                                        PARTITION BY etdcw.cal_year,
                                                                                                                                        etdcw.cal_mnth_id
                                                                                                                                        ORDER BY etdcw.cal_year,
                                                                                                                                            etdcw.cal_mnth_id,
                                                                                                                                            etdcw.cal_wk
                                                                                                                                    ) AS cal_mnth_wk
                                                                                                                                FROM (
                                                                                                                                        SELECT DISTINCT etdc.cal_year,
                                                                                                                                            etdc.cal_mnth_id,
                                                                                                                                            etdc.cal_wk
                                                                                                                                        FROM (
                                                                                                                                                SELECT etd.cal_year,
                                                                                                                                                    etd.cal_mnth_id,
                                                                                                                                                    CASE
                                                                                                                                                        WHEN (
                                                                                                                                                            (
                                                                                                                                                                row_number() OVER(
                                                                                                                                                                    PARTITION BY etd.cal_year
                                                                                                                                                                    ORDER BY TO_DATE(etd.cal_date)
                                                                                                                                                                ) % (7)::bigint
                                                                                                                                                            ) = 0
                                                                                                                                                        ) THEN (
                                                                                                                                                            row_number() OVER(
                                                                                                                                                                PARTITION BY etd.cal_year
                                                                                                                                                                ORDER BY TO_DATE(etd.cal_date)
                                                                                                                                                            ) / 7
                                                                                                                                                        )
                                                                                                                                                        ELSE (
                                                                                                                                                            (
                                                                                                                                                                row_number() OVER(
                                                                                                                                                                    PARTITION BY etd.cal_year
                                                                                                                                                                    ORDER BY TO_DATE(etd.cal_date)
                                                                                                                                                                ) / 7
                                                                                                                                                            ) + 1
                                                                                                                                                        )
                                                                                                                                                    END AS cal_wk
                                                                                                                                                FROM PCFEDW_INTEGRATION.edw_time_dim etd
                                                                                                                                            ) etdc
                                                                                                                                    ) etdcw
                                                                                                                            ) etdcm
                                                                                                                        WHERE (
                                                                                                                                (
                                                                                                                                    (
                                                                                                                                        (
                                                                                                                                            (
                                                                                                                                                (
                                                                                                                                                    (etd.jj_year = etdw.jj_year)
                                                                                                                                                    AND (etd.jj_mnth_id = etdw.jj_mnth_id)
                                                                                                                                                )
                                                                                                                                                AND (etd.jj_wk = etdw.jj_wk)
                                                                                                                                            )
                                                                                                                                            AND (TO_DATE(etd.cal_date) = TO_DATE(etdc.cal_date))
                                                                                                                                        )
                                                                                                                                        AND (etdc.cal_wk = etdcm.cal_wk)
                                                                                                                                    )
                                                                                                                                    AND (etdc.cal_year = etdcm.cal_year)
                                                                                                                                )
                                                                                                                                AND (etdc.cal_mnth_id = etdcm.cal_mnth_id)
                                                                                                                            )
                                                                                                                    ) etd,
                                                                                                                    (
                                                                                                                        PCFEDW_INTEGRATION.vw_iri_scan_sales iss
                                                                                                                        LEFT JOIN (
                                                                                                                            SELECT edw_material_dim.matl_id,
                                                                                                                                edw_material_dim.matl_desc,
                                                                                                                                edw_material_dim.mega_brnd_cd,
                                                                                                                                edw_material_dim.mega_brnd_desc,
                                                                                                                                edw_material_dim.brnd_cd,
                                                                                                                                edw_material_dim.brnd_desc,
                                                                                                                                edw_material_dim.base_prod_cd,
                                                                                                                                edw_material_dim.base_prod_desc,
                                                                                                                                edw_material_dim.variant_cd,
                                                                                                                                edw_material_dim.variant_desc,
                                                                                                                                edw_material_dim.fran_cd,
                                                                                                                                edw_material_dim.fran_desc,
                                                                                                                                edw_material_dim.grp_fran_cd,
                                                                                                                                edw_material_dim.grp_fran_desc,
                                                                                                                                edw_material_dim.matl_type_cd,
                                                                                                                                edw_material_dim.matl_type_desc,
                                                                                                                                edw_material_dim.prod_fran_cd,
                                                                                                                                edw_material_dim.prod_fran_desc,
                                                                                                                                edw_material_dim.prod_hier_cd,
                                                                                                                                edw_material_dim.prod_hier_desc,
                                                                                                                                edw_material_dim.prod_mjr_cd,
                                                                                                                                edw_material_dim.prod_mjr_desc,
                                                                                                                                edw_material_dim.prod_mnr_cd,
                                                                                                                                edw_material_dim.prod_mnr_desc,
                                                                                                                                edw_material_dim.mercia_plan,
                                                                                                                                edw_material_dim.putup_cd,
                                                                                                                                edw_material_dim.putup_desc,
                                                                                                                                edw_material_dim.bar_cd,
                                                                                                                                edw_material_dim.updt_dt,
                                                                                                                                edw_material_dim.prft_ctr
                                                                                                                            FROM PCFEDW_INTEGRATION.edw_material_dim
                                                                                                                            WHERE (
                                                                                                                                    edw_material_dim.bar_cd IN (
                                                                                                                                        SELECT DISTINCT derived_table1.bar_cd
                                                                                                                                        FROM (
                                                                                                                                                SELECT count(*) AS count,
                                                                                                                                                    edw_material_dim.bar_cd
                                                                                                                                                FROM PCFEDW_INTEGRATION.edw_material_dim
                                                                                                                                                GROUP BY edw_material_dim.bar_cd
                                                                                                                                                HAVING (count(*) = 1)
                                                                                                                                            ) derived_table1
                                                                                                                                    )
                                                                                                                                )
                                                                                                                        ) ean ON (
                                                                                                                            (
                                                                                                                                ltrim(
                                                                                                                                    (ean.bar_cd)::text,
                                                                                                                                    ('0'::character varying)::text
                                                                                                                                ) = ltrim(
                                                                                                                                    (COALESCE(iss.iri_ean, '0'::character varying))::text,
                                                                                                                                    ('0'::character varying)::text
                                                                                                                                )
                                                                                                                            )
                                                                                                                        )
                                                                                                                    )
                                                                                                                WHERE (
                                                                                                                        TO_DATE((iss.wk_end_dt)::timestamp without time zone) = TO_DATE(etd.cal_date)
                                                                                                                    )
                                                                                                            ) sales_derived
                                                                                                            LEFT JOIN PCFEDW_INTEGRATION.edw_material_dim vmd ON (
                                                                                                                (
                                                                                                                    ltrim(
                                                                                                                        (vmd.matl_id)::text,
                                                                                                                        ('0'::character varying)::text
                                                                                                                    ) = ltrim(
                                                                                                                        (
                                                                                                                            COALESCE(sales_derived.matl_id, '0'::character varying)
                                                                                                                        )::text,
                                                                                                                        ('0'::character varying)::text
                                                                                                                    )
                                                                                                                )
                                                                                                            )
                                                                                                        )
                                                                                                        LEFT JOIN PCFEDW_INTEGRATION.vw_customer_dim vcd ON (
                                                                                                            (
                                                                                                                ltrim(
                                                                                                                    (vcd.cust_no)::text,
                                                                                                                    ('0'::character varying)::text
                                                                                                                ) = ltrim(
                                                                                                                    (sales_derived.ac_code)::text,
                                                                                                                    ('0'::character varying)::text
                                                                                                                )
                                                                                                            )
                                                                                                        )
                                                                                                    )
                                                                                                WHERE (
                                                                                                        sales_derived.matl_bar_cd IN (
                                                                                                            SELECT DISTINCT derived_table2.bar_cd
                                                                                                            FROM (
                                                                                                                    SELECT count(*) AS count,
                                                                                                                        edw_material_dim.bar_cd
                                                                                                                    FROM PCFEDW_INTEGRATION.edw_material_dim
                                                                                                                    GROUP BY edw_material_dim.bar_cd
                                                                                                                    HAVING (count(*) = 1)
                                                                                                                ) derived_table2
                                                                                                        )
                                                                                                    )
                                                                                            ) bar_cd_map
                                                                                    )
                                                                                )
                                                                            )
                                                                    ) sales
                                                                    LEFT JOIN (
                                                                        SELECT vapcd.sales_org,
                                                                            vapcd.cmp_id,
                                                                            vapcd.matl_id,
                                                                            vapcd.matl_desc,
                                                                            vapcd.master_code,
                                                                            vapcd.launch_date,
                                                                            vapcd.predessor_id,
                                                                            vapcd.parent_id,
                                                                            vapcd.parent_matl_desc
                                                                        FROM (
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
                                                                                ) mstr ON (
                                                                                    (
                                                                                        (vapcd.master_code)::text = (mstr.master_code)::text
                                                                                    )
                                                                                )
                                                                            )
                                                                    ) matl ON (
                                                                        (
                                                                            ((matl.cmp_id)::text = (sales.cmp_id)::text)
                                                                            AND (
                                                                                ltrim(
                                                                                    (matl.matl_id)::text,
                                                                                    ('0'::character varying)::text
                                                                                ) = ltrim(
                                                                                    (sales.matl_id)::text,
                                                                                    ('0'::character varying)::text
                                                                                )
                                                                            )
                                                                        )
                                                                    )
                                                                )
                                                        ) iri_scan_sales_analysis_cte
                                                ) a
                                                LEFT JOIN (
                                                    SELECT derived_table1.matl_num,
                                                        derived_table1.crt_on,
                                                        derived_table1.pka_product_key,
                                                        derived_table1.pka_product_key_description,
                                                        derived_table1.gph_region,
                                                        derived_table1.gph_reg_frnchse,
                                                        derived_table1.gph_reg_frnchse_grp,
                                                        derived_table1.gph_prod_frnchse,
                                                        derived_table1.gph_prod_brnd,
                                                        derived_table1.gph_prod_sub_brnd,
                                                        derived_table1.gph_prod_vrnt,
                                                        derived_table1.gph_prod_needstate,
                                                        derived_table1.gph_prod_ctgry,
                                                        derived_table1.gph_prod_subctgry,
                                                        derived_table1.gph_prod_sgmnt,
                                                        derived_table1.gph_prod_subsgmnt,
                                                        derived_table1.gph_prod_put_up_cd,
                                                        derived_table1.gph_prod_put_up_desc,
                                                        derived_table1.in_rnk
                                                    FROM (
                                                            SELECT DISTINCT emd.matl_num,
                                                                emd.crt_on,
                                                                emd.pka_product_key,
                                                                emd.pka_product_key_description,
                                                                egph."region" AS gph_region,
                                                                egph.regional_franchise AS gph_reg_frnchse,
                                                                egph.regional_franchise_group AS gph_reg_frnchse_grp,
                                                                egph.gcph_franchise AS gph_prod_frnchse,
                                                                egph.gcph_brand AS gph_prod_brnd,
                                                                egph.gcph_subbrand AS gph_prod_sub_brnd,
                                                                egph.gcph_variant AS gph_prod_vrnt,
                                                                egph.gcph_needstate AS gph_prod_needstate,
                                                                egph.gcph_category AS gph_prod_ctgry,
                                                                egph.gcph_subcategory AS gph_prod_subctgry,
                                                                egph.gcph_segment AS gph_prod_sgmnt,
                                                                egph.gcph_subsegment AS gph_prod_subsgmnt,
                                                                egph.put_up_code AS gph_prod_put_up_cd,
                                                                egph.put_up_description AS gph_prod_put_up_desc,
                                                                row_number() OVER(
                                                                    PARTITION BY emd.matl_num
                                                                    ORDER BY emd.matl_num
                                                                ) AS in_rnk
                                                            FROM (
                                                                    ASPEDW_INTEGRATION.edw_material_dim emd
                                                                    LEFT JOIN ASPEDW_INTEGRATION.edw_gch_producthierarchy egph ON (
                                                                        (
                                                                            ltrim(
                                                                                (emd.matl_num)::text,
                                                                                ((0)::character varying)::text
                                                                            ) = ltrim(
                                                                                (egph.materialnumber)::text,
                                                                                ((0)::character varying)::text
                                                                            )
                                                                        )
                                                                    )
                                                                )
                                                            WHERE (
                                                                    (emd.prod_hier_cd)::text <> (''::character varying)::text
                                                                )
                                                        ) derived_table1
                                                    WHERE (derived_table1.in_rnk = 1)
                                                ) b ON (
                                                    (
                                                        ltrim(a.matl_id, ('0'::character varying)::text) = ltrim(
                                                            (b.matl_num)::text,
                                                            ('0'::character varying)::text
                                                        )
                                                    )
                                                )
                                            )
                                    ) derived_table2
                                WHERE (derived_table2.rnk = 1)
                            ) mat_dim ON (
                                (
                                    ltrim(
                                        (sales_cte.iri_ean)::text,
                                        ('0'::character varying)::text
                                    ) = ltrim(
                                        (mat_dim.iri_ean)::text,
                                        ('0'::character varying)::text
                                    )
                                )
                            )
                        )
                ) issa
                LEFT JOIN (
                    SELECT derived_table1.matl_id,
                        derived_table1.brnd_cd,
                        derived_table1.brnd_desc,
                        derived_table1.fran_cd,
                        derived_table1.fran_desc,
                        derived_table1.grp_fran_cd,
                        derived_table1.grp_fran_desc,
                        derived_table1.prod_fran_cd,
                        derived_table1.prod_fran_desc,
                        derived_table1.prod_mjr_cd,
                        derived_table1.prod_mjr_desc,
                        derived_table1.prod_mnr_cd,
                        derived_table1.prod_mnr_desc,
                        derived_table1.rnk
                    FROM (
                            SELECT edw_material_dim.matl_id,
                                edw_material_dim.brnd_cd,
                                edw_material_dim.brnd_desc,
                                edw_material_dim.fran_cd,
                                edw_material_dim.fran_desc,
                                edw_material_dim.grp_fran_cd,
                                edw_material_dim.grp_fran_desc,
                                edw_material_dim.prod_fran_cd,
                                edw_material_dim.prod_fran_desc,
                                edw_material_dim.prod_mjr_cd,
                                edw_material_dim.prod_mjr_desc,
                                edw_material_dim.prod_mnr_cd,
                                edw_material_dim.prod_mnr_desc,
                                row_number() OVER(
                                    PARTITION BY ltrim(
                                        (edw_material_dim.matl_id)::text,
                                        ('0'::character varying)::text
                                    )
                                    ORDER BY edw_material_dim.brnd_cd
                                ) AS rnk
                            FROM PCFEDW_INTEGRATION.edw_material_dim
                        ) derived_table1
                    WHERE (derived_table1.rnk = 1)
                ) ph_emd ON (
                    (
                        ltrim(
                            (ph_emd.matl_id)::text,
                            ('0'::character varying)::text
                        ) = ltrim(
                            COALESCE(issa.matl_id, issa.lst_sku),
                            ('0'::character varying)::text
                        )
                    )
                )
            )
            LEFT JOIN (
                SELECT derived_table2.materialnumber,
                    derived_table2.gph_region,
                    derived_table2.gph_reg_frnchse,
                    derived_table2.gph_reg_frnchse_grp,
                    derived_table2.gph_prod_frnchse,
                    derived_table2.gph_prod_brnd,
                    derived_table2.gph_prod_sub_brnd,
                    derived_table2.gph_prod_vrnt,
                    derived_table2.gph_prod_needstate,
                    derived_table2.gph_prod_ctgry,
                    derived_table2.gph_prod_subctgry,
                    derived_table2.gph_prod_sgmnt,
                    derived_table2.gph_prod_subsgmnt,
                    derived_table2.gph_prod_put_up_cd,
                    derived_table2.gph_prod_put_up_desc,
                    derived_table2.rnk
                FROM (
                        SELECT egph.materialnumber,
                            egph."region" AS gph_region,
                            egph.regional_franchise AS gph_reg_frnchse,
                            egph.regional_franchise_group AS gph_reg_frnchse_grp,
                            egph.gcph_franchise AS gph_prod_frnchse,
                            egph.gcph_brand AS gph_prod_brnd,
                            egph.gcph_subbrand AS gph_prod_sub_brnd,
                            egph.gcph_variant AS gph_prod_vrnt,
                            egph.gcph_needstate AS gph_prod_needstate,
                            egph.gcph_category AS gph_prod_ctgry,
                            egph.gcph_subcategory AS gph_prod_subctgry,
                            egph.gcph_segment AS gph_prod_sgmnt,
                            egph.gcph_subsegment AS gph_prod_subsgmnt,
                            egph.put_up_code AS gph_prod_put_up_cd,
                            egph.put_up_description AS gph_prod_put_up_desc,
                            row_number() OVER(
                                PARTITION BY ltrim(
                                    (egph.materialnumber)::text,
                                    ((0)::character varying)::text
                                )
                                ORDER BY egph.gcph_brand
                            ) AS rnk
                        FROM ASPEDW_INTEGRATION.edw_gch_producthierarchy egph
                    ) derived_table2
                WHERE (derived_table2.rnk = 1)
            ) ph_egph ON (
                (
                    ltrim(
                        (ph_egph.materialnumber)::text,
                        ((0)::character varying)::text
                    ) = ltrim(
                        COALESCE(issa.matl_id, issa.lst_sku),
                        ('0'::character varying)::text
                    )
                )
            )
        )
        LEFT JOIN (
            SELECT derived_table3.sales_org,
                derived_table3.cmp_id,
                derived_table3.matl_id,
                derived_table3.matl_desc,
                derived_table3.master_code,
                derived_table3.launch_date,
                derived_table3.predessor_id,
                derived_table3.parent_id,
                derived_table3.parent_matl_desc,
                derived_table3.rnk
            FROM (
                    SELECT vapcd.sales_org,
                        vapcd.cmp_id,
                        vapcd.matl_id,
                        vapcd.matl_desc,
                        vapcd.master_code,
                        vapcd.launch_date,
                        vapcd.predessor_id,
                        ltrim(
                            (vapcd.parent_id)::text,
                            ('0'::character varying)::text
                        ) AS parent_id,
                        vapcd.parent_matl_desc,
                        row_number() OVER(
                            PARTITION BY ltrim(
                                (vapcd.matl_id)::text,
                                ('0'::character varying)::text
                            )
                            ORDER BY ltrim(
                                    (vapcd.parent_id)::text,
                                    ('0'::character varying)::text
                                ),
                                vapcd.master_code
                        ) AS rnk
                    FROM (
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
                            ) mstr ON (
                                (
                                    (vapcd.master_code)::text = (mstr.master_code)::text
                                )
                            )
                        )
                ) derived_table3
            WHERE (derived_table3.rnk = 1)
        ) ph_mstr ON (
            (
                ltrim(
                    (ph_mstr.matl_id)::text,
                    ((0)::character varying)::text
                ) = ltrim(
                    COALESCE(issa.matl_id, issa.lst_sku),
                    ('0'::character varying)::text
                )
            )
        )
    );
create or replace view VW_JJBR_CURR_EXCH_DIM(
	RATE_TYPE,
	FROM_CCY,
	TO_CCY,
	JJ_MNTH_ID,
	EXCH_RATE
) as

(
    SELECT jjbr_curr_exchng_nzd_aud.rate_type,
        jjbr_curr_exchng_nzd_aud.from_ccy,
        jjbr_curr_exchng_nzd_aud.to_ccy,
        jjbr_curr_exchng_nzd_aud.jj_mnth_id,
        jjbr_curr_exchng_nzd_aud.exch_rate
    FROM (
            SELECT DISTINCT COALESCE(a.rate_type, 'JJBR'::character varying) AS rate_type,
                COALESCE(a.from_ccy, 'NZD'::character varying) AS from_ccy,
                COALESCE(a.to_ccy, 'AUD'::character varying) AS to_ccy,
                b.jj_mnth_id,
                COALESCE(a.exch_rate, c.exch_rate) AS exch_rate
            FROM (
                    SELECT "max"(vw_curr_exch_dim.exch_rate) AS exch_rate
                    FROM PCFEDW_INTEGRATION.vw_curr_exch_dim
                    WHERE (
                            (
                                (
                                    (
                                        (vw_curr_exch_dim.rate_type)::text = 'JJBR'::text
                                    )
                                    AND ((vw_curr_exch_dim.from_ccy)::text = 'NZD'::text)
                                )
                                AND ((vw_curr_exch_dim.to_ccy)::text = 'AUD'::text)
                            )
                            AND (
                                vw_curr_exch_dim.valid_date = (
                                    SELECT "max"(vw_curr_exch_dim.valid_date) AS "max"
                                    FROM PCFEDW_INTEGRATION.vw_curr_exch_dim
                                    WHERE (
                                            (
                                                (
                                                    (vw_curr_exch_dim.rate_type)::text = 'JJBR'::text
                                                )
                                                AND ((vw_curr_exch_dim.from_ccy)::text = 'NZD'::text)
                                            )
                                            AND ((vw_curr_exch_dim.to_ccy)::text = 'AUD'::text)
                                        )
                                )
                            )
                        )
                ) c,
                (
                    (
                        SELECT DISTINCT edw_time_dim.jj_mnth_id
                        FROM PCFEDW_INTEGRATION.edw_time_dim
                    ) b
                    LEFT JOIN (
                        SELECT t1.rate_type,
                            t1.from_ccy,
                            t1.to_ccy,
                            t2.jj_mnth_id,
                            "max"(t1.exch_rate) AS exch_rate
                        FROM (
                                SELECT vw_curr_exch_dim.rate_type,
                                    vw_curr_exch_dim.from_ccy,
                                    vw_curr_exch_dim.to_ccy,
                                    vw_curr_exch_dim.valid_date,
                                    vw_curr_exch_dim.exch_rate
                                FROM PCFEDW_INTEGRATION.vw_curr_exch_dim
                                WHERE (
                                        (
                                            (
                                                (vw_curr_exch_dim.rate_type)::text = 'JJBR'::text
                                            )
                                            AND ((vw_curr_exch_dim.from_ccy)::text = 'NZD'::text)
                                        )
                                        AND ((vw_curr_exch_dim.to_ccy)::text = 'AUD'::text)
                                    )
                            ) t1,
                            PCFEDW_INTEGRATION.edw_time_dim t2
                        WHERE (t2.time_id = t1.valid_date)
                        GROUP BY t1.rate_type,
                            t1.from_ccy,
                            t1.to_ccy,
                            t2.jj_mnth_id
                    ) a ON (((a.jj_mnth_id = b.jj_mnth_id)))
                )
        ) jjbr_curr_exchng_nzd_aud
    UNION ALL
    SELECT jjbr_curr_exchng_aud_aud.rate_type,
        jjbr_curr_exchng_aud_aud.from_ccy,
        jjbr_curr_exchng_aud_aud.to_ccy,
        jjbr_curr_exchng_aud_aud.jj_mnth_id,
        jjbr_curr_exchng_aud_aud.exch_rate
    FROM (
            SELECT jjbr_curr_exchng_nzd_aud.rate_type,
                jjbr_curr_exchng_nzd_aud.to_ccy AS from_ccy,
                jjbr_curr_exchng_nzd_aud.to_ccy,
                jjbr_curr_exchng_nzd_aud.jj_mnth_id,
                1 AS exch_rate
            FROM (
                    SELECT DISTINCT COALESCE(a.rate_type, 'JJBR'::character varying) AS rate_type,
                        COALESCE(a.from_ccy, 'NZD'::character varying) AS from_ccy,
                        COALESCE(a.to_ccy, 'AUD'::character varying) AS to_ccy,
                        b.jj_mnth_id,
                        COALESCE(a.exch_rate, c.exch_rate) AS exch_rate
                    FROM (
                            SELECT "max"(vw_curr_exch_dim.exch_rate) AS exch_rate
                            FROM PCFEDW_INTEGRATION.vw_curr_exch_dim
                            WHERE (
                                    (
                                        (
                                            (
                                                (vw_curr_exch_dim.rate_type)::text = 'JJBR'::text
                                            )
                                            AND ((vw_curr_exch_dim.from_ccy)::text = 'NZD'::text)
                                        )
                                        AND ((vw_curr_exch_dim.to_ccy)::text = 'AUD'::text)
                                    )
                                    AND (
                                        vw_curr_exch_dim.valid_date = (
                                            SELECT "max"(vw_curr_exch_dim.valid_date) AS "max"
                                            FROM PCFEDW_INTEGRATION.vw_curr_exch_dim
                                            WHERE (
                                                    (
                                                        (
                                                            (vw_curr_exch_dim.rate_type)::text = 'JJBR'::text
                                                        )
                                                        AND ((vw_curr_exch_dim.from_ccy)::text = 'NZD'::text)
                                                    )
                                                    AND ((vw_curr_exch_dim.to_ccy)::text = 'AUD'::text)
                                                )
                                        )
                                    )
                                )
                        ) c,
                        (
                            (
                                SELECT DISTINCT edw_time_dim.jj_mnth_id
                                FROM PCFEDW_INTEGRATION.edw_time_dim
                            ) b
                            LEFT JOIN (
                                SELECT t1.rate_type,
                                    t1.from_ccy,
                                    t1.to_ccy,
                                    t2.jj_mnth_id,
                                    "max"(t1.exch_rate) AS exch_rate
                                FROM (
                                        SELECT vw_curr_exch_dim.rate_type,
                                            vw_curr_exch_dim.from_ccy,
                                            vw_curr_exch_dim.to_ccy,
                                            vw_curr_exch_dim.valid_date,
                                            vw_curr_exch_dim.exch_rate
                                        FROM PCFEDW_INTEGRATION.vw_curr_exch_dim
                                        WHERE (
                                                (
                                                    (
                                                        (vw_curr_exch_dim.rate_type)::text = 'JJBR'::text
                                                    )
                                                    AND ((vw_curr_exch_dim.from_ccy)::text = 'NZD'::text)
                                                )
                                                AND ((vw_curr_exch_dim.to_ccy)::text = 'AUD'::text)
                                            )
                                    ) t1,
                                    PCFEDW_INTEGRATION.edw_time_dim t2
                                WHERE (t2.time_id = t1.valid_date)
                                GROUP BY t1.rate_type,
                                    t1.from_ccy,
                                    t1.to_ccy,
                                    t2.jj_mnth_id
                            ) a ON (((a.jj_mnth_id = b.jj_mnth_id)))
                        )
                ) jjbr_curr_exchng_nzd_aud
        ) jjbr_curr_exchng_aud_aud
)
UNION ALL
SELECT jjbr_curr_exchng_nzd_nzd.rate_type,
    jjbr_curr_exchng_nzd_nzd.from_ccy,
    jjbr_curr_exchng_nzd_nzd.to_ccy,
    jjbr_curr_exchng_nzd_nzd.jj_mnth_id,
    jjbr_curr_exchng_nzd_nzd.exch_rate
FROM (
        SELECT jjbr_curr_exchng_nzd_aud.rate_type,
            jjbr_curr_exchng_nzd_aud.from_ccy,
            jjbr_curr_exchng_nzd_aud.from_ccy AS to_ccy,
            jjbr_curr_exchng_nzd_aud.jj_mnth_id,
            1 AS exch_rate
        FROM (
                SELECT DISTINCT COALESCE(a.rate_type, 'JJBR'::character varying) AS rate_type,
                    COALESCE(a.from_ccy, 'NZD'::character varying) AS from_ccy,
                    COALESCE(a.to_ccy, 'AUD'::character varying) AS to_ccy,
                    b.jj_mnth_id,
                    COALESCE(a.exch_rate, c.exch_rate) AS exch_rate
                FROM (
                        SELECT "max"(vw_curr_exch_dim.exch_rate) AS exch_rate
                        FROM PCFEDW_INTEGRATION.vw_curr_exch_dim
                        WHERE (
                                (
                                    (
                                        (
                                            (vw_curr_exch_dim.rate_type)::text = 'JJBR'::text
                                        )
                                        AND ((vw_curr_exch_dim.from_ccy)::text = 'NZD'::text)
                                    )
                                    AND ((vw_curr_exch_dim.to_ccy)::text = 'AUD'::text)
                                )
                                AND (
                                    vw_curr_exch_dim.valid_date = (
                                        SELECT "max"(vw_curr_exch_dim.valid_date) AS "max"
                                        FROM PCFEDW_INTEGRATION.vw_curr_exch_dim
                                        WHERE (
                                                (
                                                    (
                                                        (vw_curr_exch_dim.rate_type)::text = 'JJBR'::text
                                                    )
                                                    AND ((vw_curr_exch_dim.from_ccy)::text = 'NZD'::text)
                                                )
                                                AND ((vw_curr_exch_dim.to_ccy)::text = 'AUD'::text)
                                            )
                                    )
                                )
                            )
                    ) c,
                    (
                        (
                            SELECT DISTINCT edw_time_dim.jj_mnth_id
                            FROM PCFEDW_INTEGRATION.edw_time_dim
                        ) b
                        LEFT JOIN (
                            SELECT t1.rate_type,
                                t1.from_ccy,
                                t1.to_ccy,
                                t2.jj_mnth_id,
                                "max"(t1.exch_rate) AS exch_rate
                            FROM (
                                    SELECT vw_curr_exch_dim.rate_type,
                                        vw_curr_exch_dim.from_ccy,
                                        vw_curr_exch_dim.to_ccy,
                                        vw_curr_exch_dim.valid_date,
                                        vw_curr_exch_dim.exch_rate
                                    FROM PCFEDW_INTEGRATION.vw_curr_exch_dim
                                    WHERE (
                                            (
                                                (
                                                    (vw_curr_exch_dim.rate_type)::text = 'JJBR'::text
                                                )
                                                AND ((vw_curr_exch_dim.from_ccy)::text = 'NZD'::text)
                                            )
                                            AND ((vw_curr_exch_dim.to_ccy)::text = 'AUD'::text)
                                        )
                                ) t1,
                                PCFEDW_INTEGRATION.edw_time_dim t2
                            WHERE (t2.time_id = t1.valid_date)
                            GROUP BY t1.rate_type,
                                t1.from_ccy,
                                t1.to_ccy,
                                t2.jj_mnth_id
                        ) a ON (((a.jj_mnth_id = b.jj_mnth_id)))
                    )
            ) jjbr_curr_exchng_nzd_aud
    ) jjbr_curr_exchng_nzd_nzd;
create or replace view VW_MATERIAL_DIM(
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
create or replace view VW_PX_MASTER_ANALYSIS(
	CAL_DATE,
	TIME_ID,
	JJ_WK,
	JJ_MNTH,
	JJ_MNTH_SHRT,
	JJ_MNTH_LONG,
	JJ_QRTR,
	JJ_YEAR,
	CAL_MNTH_ID,
	JJ_MNTH_ID,
	CAL_MNTH,
	CAL_QRTR,
	CAL_YEAR,
	JJ_MNTH_TOT,
	JJ_MNTH_DAY,
	CAL_MNTH_NM,
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
	CURR_CD,
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
	UPDT_DT,
	AC_CODE,
	AC_LONGNAME,
	P_PROMONUMBER,
	P_STARTDATE,
	P_STOPDATE,
	PROMO_LENGTH,
	PROMOTIONFORECASTWEEK,
	P_BUYSTARTDATEDEF,
	P_BUYSTOPDATEDEF,
	BUYPERIOD_LENGTH,
	HIERARCHY_ROWID,
	HIERARCHY_LONGNAME,
	ACTIVITY_LONGNAME,
	CONFIRMED_SWITCH,
	CLOSED_SWITCH,
	SKU_LONGNAME,
	SKU_PROFITCENTRE,
	SKU_ATTRIBUTE,
	GLTT_ROWID,
	TRANSACTION_LONGNAME,
	CASE_DEAL,
	CASE_QUANTITY,
	PLANSPEND_TOTAL,
	PAID_TOTAL,
	OPEN_TOTAL,
	COMMITTED_SPEND,
	P_DELETED,
	LOCAL_CCY,
	AUD_RATE,
	SGD_RATE,
	SAP_ACCOUNT,
	SAP_ACCNT_NM,
	PROMAX_MEASURE,
	PROMAX_BUCKET,
	PROMOTIONROWID
) as
SELECT etd.cal_date,
    etd.time_id,
    etd.jj_wk,
    etd.jj_mnth,
    etd.jj_mnth_shrt,
    etd.jj_mnth_long,
    etd.jj_qrtr,
    etd.jj_year,
    etd.cal_mnth_id,
    etd.jj_mnth_id,
    etd.cal_mnth,
    etd.cal_qrtr,
    etd.cal_year,
    etd.jj_mnth_tot,
    etd.jj_mnth_day,
    etd.cal_mnth_nm,
    vcd.cust_no,
    vcd.cmp_id,
    vcd.channel_cd,
    vcd.channel_desc,
    vcd.ctry_key,
    vcd.country,
    vcd.state_cd,
    vcd.post_cd,
    vcd.cust_suburb,
    vcd.cust_nm,
    vcd.sls_org,
    vcd.cust_del_flag,
    vcd.sales_office_cd,
    vcd.sales_office_desc,
    vcd.sales_grp_cd,
    vcd.sales_grp_desc,
    vcd.mercia_ref,
    vcd.curr_cd,
    vmd.matl_id,
    vmd.matl_desc,
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
    vmd.updt_dt,
    epmf.ac_code,
    epmf.ac_longname,
    epmf.p_promonumber,
    epmf.p_startdate,
    epmf.p_stopdate,
    epmf.promo_length,
    epmf.promotionforecastweek,
    epmf.p_buystartdatedef,
    epmf.p_buystopdatedef,
    epmf.buyperiod_length,
    epmf.hierarchy_rowid,
    epmf.hierarchy_longname,
    epmf.activity_longname,
    CASE
        WHEN (epmf.confirmed_switch = 1) THEN 'Confirmed'::character varying
        WHEN (epmf.confirmed_switch = 0) THEN 'Unconfirmed'::character varying
        ELSE 'Pending'::character varying
    END AS confirmed_switch,
    CASE
        WHEN (epmf.closed_switch = 1) THEN 'Closed'::character varying
        WHEN (epmf.closed_switch = 0) THEN 'Open'::character varying
        ELSE NULL::character varying
    END AS closed_switch,
    epmf.sku_longname,
    epmf.sku_profitcentre,
    epmf.sku_attribute,
    epmf.gltt_rowid,
    epmf.transaction_longname,
    epmf.case_deal,
    epmf.case_quantity,
    epmf.planspend_total,
    epmf.paid_total,
    (
        CASE
            WHEN (epmf.closed_switch = 1) THEN epmf.paid_total
            ELSE CASE
                WHEN (epmf.planspend_total > epmf.paid_total) THEN epmf.planspend_total
                ELSE epmf.paid_total
            END
        END - epmf.paid_total
    ) AS open_total,
    CASE
        WHEN (epmf.closed_switch = 1) THEN epmf.paid_total
        ELSE CASE
            WHEN (epmf.planspend_total > epmf.paid_total) THEN epmf.planspend_total
            ELSE epmf.paid_total
        END
    END AS committed_spend,
    CASE
        WHEN (epmf.p_deleted = 1) THEN 'Yes'::character varying
        WHEN (epmf.p_deleted = 0) THEN 'No'::character varying
        ELSE NULL::character varying
    END AS p_deleted,
    epmf.local_ccy,
    epmf.aud_rate,
    epmf.sgd_rate,
    epgm.sap_account,
    cpf.sap_accnt_nm,
    epgm.promax_measure,
    epgm.promax_bucket,
    epmf.promotionrowid
FROM (
        SELECT DISTINCT px_combined_ciw_fact.sap_accnt,
            px_combined_ciw_fact.sap_accnt_nm
        FROM PCFEDW_INTEGRATION.px_combined_ciw_fact
    ) cpf,
    PCFEDW_INTEGRATION.edw_px_gl_trans_lkp epgm,
    (
        (
            (
                PCFEDW_INTEGRATION.edw_px_master_fact epmf
                LEFT JOIN PCFEDW_INTEGRATION.vw_customer_dim vcd ON (
                    (
                        (epmf.cust_id)::text = ltrim(
                            (vcd.cust_no)::text,
                            ('0'::character varying)::text
                        )
                    )
                )
            )
            LEFT JOIN PCFEDW_INTEGRATION.vw_material_dim vmd ON (
                (
                    (epmf.matl_id)::text = ltrim(
                        (vmd.matl_id)::text,
                        ('0'::character varying)::text
                    )
                )
            )
        )
        LEFT JOIN PCFEDW_INTEGRATION.edw_time_dim etd ON (
            (
                to_date(epmf.promotionforecastweek) = to_date(etd.cal_date)
            )
        )
    )
WHERE (
        (epmf.gltt_rowid = epgm.row_id)
        AND ((epgm.sap_account)::text = (cpf.sap_accnt)::text)
    );
create or replace view VW_SAPBW_CIW_FACT(
	CMP_ID,
	CAL_MONTH_ID,
	JJ_MONTH_ID,
	CUST_NO,
	MATL_ID,
	KEY_MEASURE,
	CIW_CTGRY,
	CIW_ACCNT_GRP,
	SAP_ACCNT,
	SAP_ACCNT_NM,
	LOCAL_CCY,
	AUD_RATE,
	BASE_MEASURE,
	SALES_QTY,
	GTS_VAL,
	RETRN,
	GTS_LESS_RTRN_VAL,
	EFF_VAL,
	JGF_SI_VAL,
	PMT_TERMS_VAL,
	DATAINS_VAL,
	EXP_ADJ_VAL,
	JGF_SD_VAL,
	NTS_VAL,
	TOT_CIW_VAL,
	COGS_VAL,
	CON_FREE_GOODS_VAL,
	GP_VAL,
	PC_STD_COST_LC,
	STD_COST_LC,
	STD_COST_GP_VAL
) as

SELECT a.cmp_id,
    a.cal_month_id,
    a.jj_month_id,
    a.cust_no,
    a.matl_id,
    a.key_measure,
    a.ciw_category AS ciw_ctgry,
    a.ciw_account_group AS ciw_accnt_grp,
    a.sap_account AS sap_accnt,
    a.sap_account_nm AS sap_accnt_nm,
    a.local_ccy,
    b.exch_rate AS aud_rate,
    CASE
        WHEN (
            (
                (a.key_measure)::text = ('Cost of Goods Sold'::character varying)::text
            )
            OR (
                (a.key_measure IS NULL)
                AND ('Cost of Goods Sold' IS NULL)
            )
        ) THEN (- a.base_measure)
        WHEN (
            (
                (a.key_measure)::text = ('Consumer Free Goods'::character varying)::text
            )
            OR (
                (a.key_measure IS NULL)
                AND ('Consumer Free Goods' IS NULL)
            )
        ) THEN (- a.base_measure)
        ELSE a.base_measure
    END AS base_measure,
    a.sales_qty,
    a.gts_val,
    a.retrn_val AS retrn,
    a.gts_val AS gts_less_rtrn_val,
    a.eff_val,
    a.jgf_si_val,
    a.pmt_terms_val,
    a.datains_val,
    a.exp_adj_val,
    a.jgf_sd_val,
    (
        a.gts_val - (
            (
                (
                    ((a.eff_val + a.jgf_si_val) + a.pmt_terms_val) + a.datains_val
                ) + a.exp_adj_val
            ) + a.jgf_sd_val
        )
    ) AS nts_val,
    (
        (
            (
                ((a.eff_val + a.jgf_si_val) + a.pmt_terms_val) + a.datains_val
            ) + a.exp_adj_val
        ) + a.jgf_sd_val
    ) AS tot_ciw_val,
    (- a.cogs_val) AS cogs_val,
    (- a.con_free_goods_val) AS con_free_goods_val,
    (
        (
            (
                a.gts_val - (
                    (
                        (
                            ((a.eff_val + a.jgf_si_val) + a.pmt_terms_val) + a.datains_val
                        ) + a.exp_adj_val
                    ) + a.jgf_sd_val
                )
            ) + a.cogs_val
        ) + a.con_free_goods_val
    ) AS gp_val,
    (
        COALESCE(c.std_cost_aud, ((0)::numeric)::numeric(18, 0)) * (((1)::numeric)::numeric(18, 0) / b.exch_rate)
    ) AS pc_std_cost_lc,
    (
        a.sales_qty * (
            COALESCE(c.std_cost_aud, ((0)::numeric)::numeric(18, 0)) * (((1)::numeric)::numeric(18, 0) / b.exch_rate)
        )
    ) AS std_cost_lc,
    (
        (
            a.gts_val - (
                (
                    (
                        ((a.eff_val + a.jgf_si_val) + a.pmt_terms_val) + a.datains_val
                    ) + a.exp_adj_val
                ) + a.jgf_sd_val
            )
        ) - (
            a.sales_qty * (
                COALESCE(c.std_cost_aud, ((0)::numeric)::numeric(18, 0)) * (((1)::numeric)::numeric(18, 0) / b.exch_rate)
            )
        )
    ) AS std_cost_gp_val
FROM PCFEDW_INTEGRATION.vw_jjbr_curr_exch_dim b,
    (
        (
            SELECT derived_table2.cmp_id,
                derived_table2.cal_month_id,
                derived_table2.jj_month_id,
                derived_table2.cust_no,
                derived_table2.matl_id,
                derived_table2.key_measure,
                derived_table2.ciw_category,
                derived_table2.ciw_account_group,
                derived_table2.sap_account,
                derived_table2.sap_account_nm,
                d.to_ccy AS local_ccy,
                (derived_table2.base_measure * d.exch_rate) AS base_measure,
                derived_table2.sales_qty,
                (derived_table2.gts_val * d.exch_rate) AS gts_val,
                (derived_table2.retrn_val * d.exch_rate) AS retrn_val,
                (derived_table2.eff_val * d.exch_rate) AS eff_val,
                (derived_table2.jgf_si_val * d.exch_rate) AS jgf_si_val,
                (derived_table2.pmt_terms_val * d.exch_rate) AS pmt_terms_val,
                (derived_table2.datains_val * d.exch_rate) AS datains_val,
                (derived_table2.exp_adj_val * d.exch_rate) AS exp_adj_val,
                (derived_table2.jgf_sd_val * d.exch_rate) AS jgf_sd_val,
                (derived_table2.cogs_val * d.exch_rate) AS cogs_val,
                (derived_table2.con_free_goods_val * d.exch_rate) AS con_free_goods_val
            FROM (
                    (
                        (
                            (
                                (
                                    (
                                        (
                                            SELECT vw_actual_cogs_rate_dim.cmp_id,
                                                vw_actual_cogs_rate_dim.from_ccy,
                                                vw_actual_cogs_rate_dim.to_ccy,
                                                vw_actual_cogs_rate_dim.jj_mnth_id,
                                                vw_actual_cogs_rate_dim.exch_rate
                                            FROM PCFEDW_INTEGRATION.vw_actual_cogs_rate_dim
                                            WHERE (
                                                    vw_actual_cogs_rate_dim.jj_mnth_id IN (
                                                        SELECT "max"(vw_actual_cogs_rate_dim.jj_mnth_id) AS "max"
                                                        FROM PCFEDW_INTEGRATION.vw_actual_cogs_rate_dim
                                                        WHERE (
                                                                "substring"(
                                                                    (
                                                                        (vw_actual_cogs_rate_dim.jj_mnth_id)::character varying
                                                                    )::text,
                                                                    1,
                                                                    4
                                                                ) <= (
                                                                    (
                                                                        date_part(YEAR,
                                                                            TO_DATE(
                                                                                CURRENT_TIMESTAMP()
                                                                            )
                                                                        )
                                                                    )::character varying
                                                                )::text
                                                            )
                                                        GROUP BY vw_actual_cogs_rate_dim.cmp_id,
                                                            vw_actual_cogs_rate_dim.from_ccy,
                                                            vw_actual_cogs_rate_dim.to_ccy
                                                    )
                                                )
                                            UNION
                                            SELECT '7470'::character varying AS cmp_id,
                                                vw_jjbr_curr_exch_dim.from_ccy,
                                                vw_jjbr_curr_exch_dim.to_ccy,
                                                vw_jjbr_curr_exch_dim.jj_mnth_id,
                                                vw_jjbr_curr_exch_dim.exch_rate
                                            FROM PCFEDW_INTEGRATION.vw_jjbr_curr_exch_dim
                                            WHERE (
                                                    (
                                                        (
                                                            (vw_jjbr_curr_exch_dim.from_ccy)::text = ('AUD'::character varying)::text
                                                        )
                                                        AND (
                                                            (vw_jjbr_curr_exch_dim.to_ccy)::text = ('AUD'::character varying)::text
                                                        )
                                                    )
                                                    AND (
                                                        vw_jjbr_curr_exch_dim.jj_mnth_id IN (
                                                            SELECT "max"(vw_jjbr_curr_exch_dim.jj_mnth_id) AS "max"
                                                            FROM PCFEDW_INTEGRATION.vw_jjbr_curr_exch_dim
                                                            WHERE (
                                                                    (
                                                                        (
                                                                            (vw_jjbr_curr_exch_dim.from_ccy)::text = ('AUD'::character varying)::text
                                                                        )
                                                                        AND (
                                                                            (vw_jjbr_curr_exch_dim.to_ccy)::text = ('AUD'::character varying)::text
                                                                        )
                                                                    )
                                                                    AND (
                                                                        "substring"(
                                                                            (
                                                                                (vw_jjbr_curr_exch_dim.jj_mnth_id)::character varying
                                                                            )::text,
                                                                            1,
                                                                            4
                                                                        ) <= (
                                                                            (
                                                                                date_part(YEAR,
                                                                                    TO_DATE(
                                                                                        CURRENT_TIMESTAMP()
                                                                                    )
                                                                                )
                                                                            )::character varying
                                                                        )::text
                                                                    )
                                                                )
                                                            GROUP BY vw_jjbr_curr_exch_dim.from_ccy,
                                                                vw_jjbr_curr_exch_dim.to_ccy
                                                        )
                                                    )
                                                )
                                        )
                                        UNION
                                        SELECT '8361'::character varying AS cmp_id,
                                            vw_jjbr_curr_exch_dim.from_ccy,
                                            vw_jjbr_curr_exch_dim.to_ccy,
                                            vw_jjbr_curr_exch_dim.jj_mnth_id,
                                            vw_jjbr_curr_exch_dim.exch_rate
                                        FROM PCFEDW_INTEGRATION.vw_jjbr_curr_exch_dim
                                        WHERE (
                                                (
                                                    (
                                                        (vw_jjbr_curr_exch_dim.from_ccy)::text = ('NZD'::character varying)::text
                                                    )
                                                    AND (
                                                        (vw_jjbr_curr_exch_dim.to_ccy)::text = ('NZD'::character varying)::text
                                                    )
                                                )
                                                AND (
                                                    vw_jjbr_curr_exch_dim.jj_mnth_id IN (
                                                        SELECT "max"(vw_jjbr_curr_exch_dim.jj_mnth_id) AS "max"
                                                        FROM PCFEDW_INTEGRATION.vw_jjbr_curr_exch_dim
                                                        WHERE (
                                                                (
                                                                    (
                                                                        (vw_jjbr_curr_exch_dim.from_ccy)::text = ('NZD'::character varying)::text
                                                                    )
                                                                    AND (
                                                                        (vw_jjbr_curr_exch_dim.to_ccy)::text = ('NZD'::character varying)::text
                                                                    )
                                                                )
                                                                AND (
                                                                    "substring"(
                                                                        (
                                                                            (vw_jjbr_curr_exch_dim.jj_mnth_id)::character varying
                                                                        )::text,
                                                                        1,
                                                                        4
                                                                    ) <= (
                                                                        (
                                                                            date_part(YEAR,
                                                                                TO_DATE(
                                                                                    CURRENT_TIMESTAMP()
                                                                                )
                                                                            )
                                                                        )::character varying
                                                                    )::text
                                                                )
                                                            )
                                                        GROUP BY vw_jjbr_curr_exch_dim.from_ccy,
                                                            vw_jjbr_curr_exch_dim.to_ccy
                                                    )
                                                )
                                            )
                                    )
                                    UNION
                                    SELECT '7470'::character varying AS cmp_id,
                                        vw_jjbr_curr_exch_dim.from_ccy,
                                        vw_jjbr_curr_exch_dim.to_ccy,
                                        vw_jjbr_curr_exch_dim.jj_mnth_id,
                                        vw_jjbr_curr_exch_dim.exch_rate
                                    FROM PCFEDW_INTEGRATION.vw_jjbr_curr_exch_dim
                                    WHERE (
                                            (
                                                (
                                                    (vw_jjbr_curr_exch_dim.from_ccy)::text = ('NZD'::character varying)::text
                                                )
                                                AND (
                                                    (vw_jjbr_curr_exch_dim.to_ccy)::text = ('AUD'::character varying)::text
                                                )
                                            )
                                            AND (
                                                vw_jjbr_curr_exch_dim.jj_mnth_id IN (
                                                    SELECT "max"(vw_jjbr_curr_exch_dim.jj_mnth_id) AS "max"
                                                    FROM PCFEDW_INTEGRATION.vw_jjbr_curr_exch_dim
                                                    WHERE (
                                                            (
                                                                (
                                                                    (vw_jjbr_curr_exch_dim.from_ccy)::text = ('NZD'::character varying)::text
                                                                )
                                                                AND (
                                                                    (vw_jjbr_curr_exch_dim.to_ccy)::text = ('AUD'::character varying)::text
                                                                )
                                                            )
                                                            AND (
                                                                "substring"(
                                                                    (
                                                                        (vw_jjbr_curr_exch_dim.jj_mnth_id)::character varying
                                                                    )::text,
                                                                    1,
                                                                    4
                                                                ) <= (
                                                                    (
                                                                        date_part(YEAR,
                                                                            TO_DATE(
                                                                                CURRENT_TIMESTAMP()
                                                                            )
                                                                        )
                                                                    )::character varying
                                                                )::text
                                                            )
                                                        )
                                                    GROUP BY vw_jjbr_curr_exch_dim.from_ccy,
                                                        vw_jjbr_curr_exch_dim.to_ccy
                                                )
                                            )
                                        )
                                )
                                UNION
                                SELECT '8361'::character varying AS cmp_id,
                                    vw_jjbr_curr_exch_dim.to_ccy AS from_ccy,
                                    vw_jjbr_curr_exch_dim.from_ccy AS to_ccy,
                                    vw_jjbr_curr_exch_dim.jj_mnth_id,
                                    (
                                        ((1)::numeric)::numeric(18, 0) / vw_jjbr_curr_exch_dim.exch_rate
                                    ) AS exch_rate
                                FROM PCFEDW_INTEGRATION.vw_jjbr_curr_exch_dim
                                WHERE (
                                        (
                                            (
                                                (vw_jjbr_curr_exch_dim.from_ccy)::text = ('NZD'::character varying)::text
                                            )
                                            AND (
                                                (vw_jjbr_curr_exch_dim.to_ccy)::text = ('AUD'::character varying)::text
                                            )
                                        )
                                        AND (
                                            vw_jjbr_curr_exch_dim.jj_mnth_id IN (
                                                SELECT "max"(vw_jjbr_curr_exch_dim.jj_mnth_id) AS "max"
                                                FROM PCFEDW_INTEGRATION.vw_jjbr_curr_exch_dim
                                                WHERE (
                                                        (
                                                            (
                                                                (vw_jjbr_curr_exch_dim.from_ccy)::text = ('NZD'::character varying)::text
                                                            )
                                                            AND (
                                                                (vw_jjbr_curr_exch_dim.to_ccy)::text = ('AUD'::character varying)::text
                                                            )
                                                        )
                                                        AND (
                                                            "substring"(
                                                                (
                                                                    (vw_jjbr_curr_exch_dim.jj_mnth_id)::character varying
                                                                )::text,
                                                                1,
                                                                4
                                                            ) <= (
                                                                (
                                                                    date_part(YEAR,
                                                                        TO_DATE(
                                                                            CURRENT_TIMESTAMP()
                                                                        )
                                                                    )
                                                                )::character varying
                                                            )::text
                                                        )
                                                    )
                                                GROUP BY vw_jjbr_curr_exch_dim.from_ccy,
                                                    vw_jjbr_curr_exch_dim.to_ccy
                                            )
                                        )
                                    )
                            )
                            UNION
                            SELECT CASE
                                    WHEN (
                                        (vw_bwar_curr_exch_dim.from_ccy)::text = ('AUD'::character varying)::text
                                    ) THEN '7470'::character varying
                                    WHEN (
                                        (vw_bwar_curr_exch_dim.from_ccy)::text = ('NZD'::character varying)::text
                                    ) THEN '8361'::character varying
                                    ELSE NULL::character varying
                                END AS cmp_id,
                                vw_bwar_curr_exch_dim.to_ccy AS from_ccy,
                                vw_bwar_curr_exch_dim.from_ccy AS to_ccy,
                                vw_bwar_curr_exch_dim.jj_mnth_id,
                                (
                                    ((1)::numeric)::numeric(18, 0) / vw_bwar_curr_exch_dim.exch_rate
                                ) AS exch_rate
                            FROM PCFEDW_INTEGRATION.vw_bwar_curr_exch_dim
                            WHERE (
                                    (
                                        (vw_bwar_curr_exch_dim.to_ccy)::text = ('USD'::character varying)::text
                                    )
                                    AND (
                                        vw_bwar_curr_exch_dim.jj_mnth_id IN (
                                            SELECT "max"(vw_bwar_curr_exch_dim.jj_mnth_id) AS "max"
                                            FROM PCFEDW_INTEGRATION.vw_bwar_curr_exch_dim
                                            WHERE (
                                                    (
                                                        (vw_bwar_curr_exch_dim.to_ccy)::text = ('USD'::character varying)::text
                                                    )
                                                    AND (
                                                        "substring"(
                                                            (
                                                                (vw_bwar_curr_exch_dim.jj_mnth_id)::character varying
                                                            )::text,
                                                            1,
                                                            4
                                                        ) <= (
                                                            (
                                                                date_part(year,
                                                                    TO_DATE(
                                                                        CURRENT_TIMESTAMP()
                                                                    )
                                                                )
                                                            )::character varying
                                                        )::text
                                                    )
                                                )
                                            GROUP BY vw_bwar_curr_exch_dim.from_ccy,
                                                vw_bwar_curr_exch_dim.to_ccy
                                        )
                                    )
                                )
                        )
                        UNION
                        SELECT '747A'::character varying AS cmp_id,
                            vw_curr_exch_dim.from_ccy,
                            vw_curr_exch_dim.to_ccy,
                            (
                                "substring"(
                                    ((vw_curr_exch_dim.valid_date)::character varying)::text,
                                    1,
                                    6
                                )
                            )::numeric(18, 0) AS jj_mnth_id,
                            vw_curr_exch_dim.exch_rate
                        FROM PCFEDW_INTEGRATION.vw_curr_exch_dim
                        WHERE (
                                (
                                    (
                                        (
                                            (vw_curr_exch_dim.rate_type)::text = ('BWAR'::character varying)::text
                                        )
                                        AND (
                                            (vw_curr_exch_dim.from_ccy)::text = ('SGD'::character varying)::text
                                        )
                                    )
                                    AND (
                                        (vw_curr_exch_dim.to_ccy)::text = ('AUD'::character varying)::text
                                    )
                                )
                                AND (
                                    vw_curr_exch_dim.valid_date IN (
                                        SELECT "max"(vw_curr_exch_dim.valid_date) AS "max"
                                        FROM PCFEDW_INTEGRATION.vw_curr_exch_dim
                                        WHERE (
                                                (
                                                    (
                                                        (
                                                            (vw_curr_exch_dim.rate_type)::text = ('BWAR'::character varying)::text
                                                        )
                                                        AND (
                                                            (vw_curr_exch_dim.from_ccy)::text = ('SGD'::character varying)::text
                                                        )
                                                    )
                                                    AND (
                                                        (vw_curr_exch_dim.to_ccy)::text = ('AUD'::character varying)::text
                                                    )
                                                )
                                                AND (
                                                    "substring"(
                                                        ((vw_curr_exch_dim.valid_date)::character varying)::text,
                                                        1,
                                                        4
                                                    ) <= (
                                                        (
                                                            date_part(YEAR,
                                                                TO_DATE(
                                                                    CURRENT_TIMESTAMP()
                                                                )
                                                            )
                                                        )::character varying
                                                    )::text
                                                )
                                            )
                                        GROUP BY vw_curr_exch_dim.rate_type,
                                            vw_curr_exch_dim.from_ccy,
                                            vw_curr_exch_dim.to_ccy
                                    )
                                )
                            )
                    )
                    UNION
                    SELECT '836A'::character varying AS cmp_id,
                        vw_curr_exch_dim.from_ccy,
                        vw_curr_exch_dim.to_ccy,
                        (
                            "substring"(
                                ((vw_curr_exch_dim.valid_date)::character varying)::text,
                                1,
                                6
                            )
                        )::numeric(18, 0) AS jj_mnth_id,
                        vw_curr_exch_dim.exch_rate
                    FROM PCFEDW_INTEGRATION.vw_curr_exch_dim
                    WHERE (
                            (
                                (
                                    (
                                        (vw_curr_exch_dim.rate_type)::text = ('BWAR'::character varying)::text
                                    )
                                    AND (
                                        (vw_curr_exch_dim.from_ccy)::text = ('SGD'::character varying)::text
                                    )
                                )
                                AND (
                                    (vw_curr_exch_dim.to_ccy)::text = ('NZD'::character varying)::text
                                )
                            )
                            AND (
                                vw_curr_exch_dim.valid_date IN (
                                    SELECT "max"(vw_curr_exch_dim.valid_date) AS "max"
                                    FROM PCFEDW_INTEGRATION.vw_curr_exch_dim
                                    WHERE (
                                            (
                                                (
                                                    (
                                                        (vw_curr_exch_dim.rate_type)::text = ('BWAR'::character varying)::text
                                                    )
                                                    AND (
                                                        (vw_curr_exch_dim.from_ccy)::text = ('SGD'::character varying)::text
                                                    )
                                                )
                                                AND (
                                                    (vw_curr_exch_dim.to_ccy)::text = ('NZD'::character varying)::text
                                                )
                                            )
                                            AND (
                                                "substring"(
                                                    ((vw_curr_exch_dim.valid_date)::character varying)::text,
                                                    1,
                                                    4
                                                ) <= (
                                                    (
                                                        date_part(YEAR,
                                                            TO_DATE(
                                                                CURRENT_TIMESTAMP()
                                                            )
                                                        )
                                                    )::character varying
                                                )::text
                                            )
                                        )
                                    GROUP BY vw_curr_exch_dim.rate_type,
                                        vw_curr_exch_dim.from_ccy,
                                        vw_curr_exch_dim.to_ccy
                                )
                            )
                        )
                ) d,
                (
                    SELECT derived_table1.cmp_id,
                        derived_table1.cal_month_id,
                        derived_table1.jj_month_id,
                        derived_table1.cust_no,
                        derived_table1.matl_id,
                        derived_table1.key_measure,
                        derived_table1.ciw_category,
                        derived_table1.ciw_account_group,
                        derived_table1.sap_account,
                        derived_table1.sap_account_nm,
                        derived_table1.local_ccy,
                        sum(derived_table1.base_measure) AS base_measure,
                        sum(derived_table1.sales_qty) AS sales_qty,
                        sum(derived_table1.gts_val) AS gts_val,
                        sum(derived_table1.retrn_val) AS retrn_val,
                        sum(derived_table1.eff_val) AS eff_val,
                        sum(derived_table1.jgf_si_val) AS jgf_si_val,
                        sum(derived_table1.pmt_terms_val) AS pmt_terms_val,
                        sum(derived_table1.datains_val) AS datains_val,
                        sum(derived_table1.exp_adj_val) AS exp_adj_val,
                        sum(derived_table1.jgf_sd_val) AS jgf_sd_val,
                        sum(derived_table1.cogs_val) AS cogs_val,
                        sum(derived_table1.con_free_goods_val) AS con_free_goods_val
                    FROM (
                            SELECT CASE
                                    WHEN (
                                        (copa.co_cd IS NULL)
                                        AND ('747A' IS NULL)
                                    ) THEN '7470'::character varying
                                    WHEN (
                                        (copa.co_cd IS NULL)
                                        AND ('836A' IS NULL)
                                    ) THEN '8361'::character varying
                                    ELSE copa.co_cd
                                END AS cmp_id,
                                copa.caln_day AS cal_day,
                                NULL::character varying AS cal_month_id,
                                (
                                    (
                                        "substring"(
                                            ((copa.fisc_yr_per)::character varying)::text,
                                            1,
                                            4
                                        ) || "substring"(
                                            ((copa.fisc_yr_per)::character varying)::text,
                                            6,
                                            2
                                        )
                                    )
                                )::integer AS jj_month_id,
                                copa.cust_num AS cust_no,
                                copa.matl_num AS matl_id,
                                accnt.key_measure,
                                accnt.ciw_category,
                                accnt.ciw_account_group,
                                accnt.sap_account,
                                ead.acct_nm AS sap_account_nm,
                                copa.obj_crncy_co_obj AS local_ccy,
                                copa.amt_obj_crncy AS base_measure,
                                CASE
                                    WHEN (
                                        (accnt.key_measure)::text = ('Gross Trade Sales'::character varying)::text
                                    ) THEN copa.qty
                                    WHEN (
                                        (accnt.ciw_account_group)::text = ('Returns'::character varying)::text
                                    ) THEN (- copa.qty)
                                    ELSE ((0)::numeric)::numeric(18, 0)
                                END AS sales_qty,
                                CASE
                                    WHEN (
                                        (accnt.key_measure)::text = ('Gross Trade Sales'::character varying)::text
                                    ) THEN copa.amt_obj_crncy
                                    ELSE ((0)::numeric)::numeric(18, 0)
                                END AS gts_val,
                                CASE
                                    WHEN (
                                        (accnt.ciw_account_group)::text = ('Returns'::character varying)::text
                                    ) THEN copa.amt_obj_crncy
                                    ELSE ((0)::numeric)::numeric(18, 0)
                                END AS retrn_val,
                                CASE
                                    WHEN (
                                        (accnt.key_measure)::text = ('Efficiency'::character varying)::text
                                    ) THEN copa.amt_obj_crncy
                                    ELSE ((0)::numeric)::numeric(18, 0)
                                END AS eff_val,
                                CASE
                                    WHEN (
                                        (accnt.key_measure)::text = (
                                            'Joint Growth Fund: Shopper Indirect'::character varying
                                        )::text
                                    ) THEN copa.amt_obj_crncy
                                    ELSE ((0)::numeric)::numeric(18, 0)
                                END AS jgf_si_val,
                                CASE
                                    WHEN (
                                        (accnt.key_measure)::text = ('Payment Terms'::character varying)::text
                                    ) THEN copa.amt_obj_crncy
                                    ELSE ((0)::numeric)::numeric(18, 0)
                                END AS pmt_terms_val,
                                CASE
                                    WHEN (
                                        (accnt.key_measure)::text = ('Data and Insights'::character varying)::text
                                    ) THEN copa.amt_obj_crncy
                                    ELSE ((0)::numeric)::numeric(18, 0)
                                END AS datains_val,
                                CASE
                                    WHEN (
                                        (accnt.key_measure)::text = ('Expenses and Adjustments'::character varying)::text
                                    ) THEN copa.amt_obj_crncy
                                    ELSE ((0)::numeric)::numeric(18, 0)
                                END AS exp_adj_val,
                                CASE
                                    WHEN (
                                        (accnt.key_measure)::text = (
                                            'Joint Growth Fund: Shopper Direct'::character varying
                                        )::text
                                    ) THEN copa.amt_obj_crncy
                                    ELSE ((0)::numeric)::numeric(18, 0)
                                END AS jgf_sd_val,
                                CASE
                                    WHEN (
                                        (accnt.key_measure)::text = ('Cost of Goods Sold'::character varying)::text
                                    ) THEN copa.amt_obj_crncy
                                    ELSE ((0)::numeric)::numeric(18, 0)
                                END AS cogs_val,
                                CASE
                                    WHEN (
                                        (accnt.key_measure)::text = ('Consumer Free Goods'::character varying)::text
                                    ) THEN copa.amt_obj_crncy
                                    ELSE ((0)::numeric)::numeric(18, 0)
                                END AS con_free_goods_val
                            FROM ASPEDW_INTEGRATION.edw_copa_trans_fact copa,
                                PCFEDW_INTEGRATION.edw_ciw_accnt_lkp accnt,
                                (
                                    SELECT DISTINCT edw_account_dim.acct_num,
                                        edw_account_dim.acct_nm
                                    FROM ASPEDW_INTEGRATION.edw_account_dim
                                    WHERE (
                                            (edw_account_dim.bravo_acct_l1)::text <> (''::character varying)::text
                                        )
                                ) ead,
                                (
                                    SELECT DISTINCT derived_table1.sls_org,
                                        derived_table1.cmp_id
                                    FROM (
                                            (
                                                SELECT DISTINCT dly_sls_cust_attrb_lkp.sls_org,
                                                    dly_sls_cust_attrb_lkp.cmp_id
                                                FROM PCFEDW_INTEGRATION.dly_sls_cust_attrb_lkp
                                                UNION ALL
                                                SELECT '330A'::character varying AS "varchar",
                                                    '747A'::character varying AS "varchar"
                                            )
                                            UNION ALL
                                            SELECT '341A'::character varying AS "varchar",
                                                '836A'::character varying AS "varchar"
                                        ) derived_table1
                                ) lkp
                            WHERE (
                                    (
                                        (
                                            (
                                                (
                                                    (
                                                        ltrim(
                                                            (copa.acct_num)::text,
                                                            ('0'::character varying)::text
                                                        ) = (accnt.sap_account)::text
                                                    )
                                                    AND (
                                                        ltrim(
                                                            (ead.acct_num)::text,
                                                            ('0'::character varying)::text
                                                        ) = (accnt.sap_account)::text
                                                    )
                                                )
                                                AND ((copa.co_cd)::text = (lkp.cmp_id)::text)
                                            )
                                            AND ((copa.sls_org)::text = (lkp.sls_org)::text)
                                        )
                                        AND (
                                            (copa.acct_hier_shrt_desc)::text <> ('NTS'::character varying)::text
                                        )
                                    )
                                    AND (
                                        (copa.acct_hier_shrt_desc)::text <> ('FGC'::character varying)::text
                                    )
                                )
                        ) derived_table1
                    GROUP BY derived_table1.cmp_id,
                        derived_table1.cal_month_id,
                        derived_table1.jj_month_id,
                        derived_table1.cust_no,
                        derived_table1.matl_id,
                        derived_table1.key_measure,
                        derived_table1.ciw_category,
                        derived_table1.ciw_account_group,
                        derived_table1.sap_account,
                        derived_table1.sap_account_nm,
                        derived_table1.local_ccy
                ) derived_table2
            WHERE (
                    (
                        (derived_table2.local_ccy)::text = (d.from_ccy)::text
                    )
                    AND ((derived_table2.cmp_id)::text = (d.cmp_id)::text)
                )
        ) a
        LEFT JOIN PCFEDW_INTEGRATION.vw_sap_std_cost c ON (
            (
                ((c.matnr)::text = (a.matl_id)::text)
                AND ((c.cmp_no)::text = (a.cmp_id)::text)
            )
        )
    )
WHERE (
        (
            (
                ((a.jj_month_id)::numeric)::numeric(18, 0) = b.jj_mnth_id
            )
            AND ((a.local_ccy)::text = (b.from_ccy)::text)
        )
        AND (
            (b.to_ccy)::text = ('AUD'::character varying)::text
        )
    );
create or replace view VW_SAPBW_FACT(
	CMP_ID,
	CAL_MONTH_ID,
	JJ_MONTH_ID,
	CUST_NO,
	MATL_ID,
	KEY_MEASURE,
	CIW_CTGRY,
	CIW_ACCNT_GRP,
	SAP_ACCNT,
	SAP_ACCNT_NM,
	LOCAL_CCY,
	AUD_RATE,
	BASE_MEASURE,
	SALES_QTY,
	GTS_VAL,
	RETRN_VAL,
	GTS_LESS_RTRN_VAL,
	TERMS_VAL,
	BRND_DSCNT_VAL,
	TOT_TERMS_VAL,
	TS_VAL,
	TP_VAL,
	NTS_VAL,
	COGS_VAL,
	CON_FREE_GOODS_VAL,
	GP_VAL,
	PC_STD_COST_LC,
	STD_COST_LC,
	STD_COST_GP_VAL
) as
SELECT a.cmp_id,
    a.cal_month_id,
    a.jj_month_id,
    a.cust_no,
    a.matl_id,
    a.key_measure,
    a.ciw_category AS ciw_ctgry,
    a.ciw_account_group AS ciw_accnt_grp,
    a.sap_account AS sap_accnt,
    a.sap_account_nm AS sap_accnt_nm,
    a.local_ccy,
    b.exch_rate AS aud_rate,
    CASE
        WHEN (
            (
                (a.key_measure)::text = ('Cost of Goods Sold'::character varying)::text
            )
            OR (
                (a.key_measure IS NULL)
                AND ('Cost of Goods Sold' IS NULL)
            )
        ) THEN ((- a.base_measure) * d.exch_rate)
        WHEN (
            (
                (a.key_measure)::text = ('Consumer Free Goods'::character varying)::text
            )
            OR (
                (a.key_measure IS NULL)
                AND ('Consumer Free Goods' IS NULL)
            )
        ) THEN ((- a.base_measure) * d.exch_rate)
        ELSE a.base_measure
    END AS base_measure,
    a.sales_qty,
    a.gts_val,
    a.retrn_val,
    (a.gts_val - a.retrn_val) AS gts_less_rtrn_val,
    a.terms_val,
    a.brnd_dscnt_val,
    (a.terms_val + a.brnd_dscnt_val) AS tot_terms_val,
    (
        (a.gts_val - a.retrn_val) - (a.terms_val + a.brnd_dscnt_val)
    ) AS ts_val,
    a.tp_val,
    (
        (
            (a.gts_val - a.retrn_val) - (a.terms_val + a.brnd_dscnt_val)
        ) - a.tp_val
    ) AS nts_val,
    ((- a.cogs_val) * d.exch_rate) AS cogs_val,
    ((- a.con_free_goods_val) * d.exch_rate) AS con_free_goods_val,
    (
        (
            (
                (
                    (a.gts_val - a.retrn_val) - (a.terms_val + a.brnd_dscnt_val)
                ) - a.tp_val
            ) + (a.cogs_val * d.exch_rate)
        ) + (a.con_free_goods_val * d.exch_rate)
    ) AS gp_val,
    (
        COALESCE(c.std_cost_aud, ((0)::numeric)::numeric(18, 0)) * (((1)::numeric)::numeric(18, 0) / b.exch_rate)
    ) AS pc_std_cost_lc,
    (
        a.sales_qty * (
            COALESCE(c.std_cost_aud, ((0)::numeric)::numeric(18, 0)) * (((1)::numeric)::numeric(18, 0) / b.exch_rate)
        )
    ) AS std_cost_lc,
    (
        (
            (
                (a.gts_val - a.retrn_val) - (a.terms_val + a.brnd_dscnt_val)
            ) - a.tp_val
        ) - (
            a.sales_qty * (
                COALESCE(c.std_cost_aud, ((0)::numeric)::numeric(18, 0)) * (((1)::numeric)::numeric(18, 0) / b.exch_rate)
            )
        )
    ) AS std_cost_gp_val
FROM pcfedw_integration.vw_jjbr_curr_exch_dim b,
    pcfedw_integration.vw_actual_cogs_rate_dim d,
    (
        (
            SELECT derived_table1.cmp_id,
                derived_table1.cal_month_id,
                derived_table1.jj_month_id,
                derived_table1.cust_no,
                derived_table1.matl_id,
                derived_table1.key_measure,
                derived_table1.ciw_category,
                derived_table1.ciw_account_group,
                derived_table1.sap_account,
                derived_table1.sap_account_nm,
                derived_table1.local_ccy,
                sum(derived_table1.base_measure) AS base_measure,
                sum(derived_table1.sales_qty) AS sales_qty,
                sum(derived_table1.gts_val) AS gts_val,
                sum(derived_table1.retrn_val) AS retrn_val,
                sum(derived_table1.terms_val) AS terms_val,
                sum(derived_table1.brnd_dscnt_val) AS brnd_dscnt_val,
                sum(derived_table1.cogs_val) AS cogs_val,
                sum(derived_table1.con_free_goods_val) AS con_free_goods_val,
                sum(derived_table1.tp_val) AS tp_val
            FROM (
                    SELECT CASE
                            WHEN (
                                (
                                    (copa.co_cd)::text = ('747A'::character varying)::text
                                )
                                OR (
                                    (copa.co_cd IS NULL)
                                    AND ('747A' IS NULL)
                                )
                            ) THEN '7470'::character varying
                            WHEN (
                                (
                                    (copa.co_cd)::text = ('836A'::character varying)::text
                                )
                                OR (
                                    (copa.co_cd IS NULL)
                                    AND ('836A' IS NULL)
                                )
                            ) THEN '8361'::character varying
                            ELSE copa.co_cd
                        END AS cmp_id,
                        copa.caln_day AS cal_day,
                        NULL::character varying AS cal_month_id,
                        (
                            (
                                "substring"(
                                    ((copa.fisc_yr_per)::character varying)::text,
                                    1,
                                    4
                                ) || "substring"(
                                    ((copa.fisc_yr_per)::character varying)::text,
                                    6,
                                    2
                                )
                            )
                        )::integer AS jj_month_id,
                        copa.cust_num AS cust_no,
                        copa.matl_num AS matl_id,
                        accnt.key_measure,
                        accnt.ciw_category,
                        accnt.ciw_account_group,
                        accnt.sap_account,
                        ead.acct_nm AS sap_account_nm,
                        CASE
                            WHEN (
                                (
                                    (copa.obj_crncy_co_obj)::text = ('SGD'::character varying)::text
                                )
                                OR (
                                    (copa.obj_crncy_co_obj IS NULL)
                                    AND ('SGD' IS NULL)
                                )
                            ) THEN CASE
                                WHEN (
                                    (
                                        (copa.co_cd)::text = ('747A'::character varying)::text
                                    )
                                    OR (
                                        (copa.co_cd IS NULL)
                                        AND ('747A' IS NULL)
                                    )
                                ) THEN 'AUD'::character varying
                                WHEN (
                                    (
                                        (copa.co_cd)::text = ('836A'::character varying)::text
                                    )
                                    OR (
                                        (copa.co_cd IS NULL)
                                        AND ('836A' IS NULL)
                                    )
                                ) THEN 'NZD'::character varying
                                ELSE 'AUD'::character varying
                            END
                            ELSE copa.obj_crncy_co_obj
                        END AS local_ccy,
                        copa.amt_obj_crncy AS base_measure,
                        CASE
                            WHEN (
                                (accnt.key_measure)::text = ('Gross Trade Sales'::character varying)::text
                            ) THEN copa.qty
                            WHEN (
                                (accnt.key_measure)::text = ('Returns'::character varying)::text
                            ) THEN (- copa.qty)
                            ELSE ((0)::numeric)::numeric(18, 0)
                        END AS sales_qty,
                        CASE
                            WHEN (
                                (accnt.key_measure)::text = ('Gross Trade Sales'::character varying)::text
                            ) THEN copa.amt_obj_crncy
                            ELSE ((0)::numeric)::numeric(18, 0)
                        END AS gts_val,
                        CASE
                            WHEN (
                                (accnt.key_measure)::text = ('Returns'::character varying)::text
                            ) THEN copa.amt_obj_crncy
                            ELSE ((0)::numeric)::numeric(18, 0)
                        END AS retrn_val,
                        CASE
                            WHEN (
                                (accnt.key_measure)::text = ('Terms'::character varying)::text
                            ) THEN copa.amt_obj_crncy
                            ELSE ((0)::numeric)::numeric(18, 0)
                        END AS terms_val,
                        CASE
                            WHEN (
                                (accnt.key_measure)::text = ('Brand Discount'::character varying)::text
                            ) THEN copa.amt_obj_crncy
                            ELSE ((0)::numeric)::numeric(18, 0)
                        END AS brnd_dscnt_val,
                        CASE
                            WHEN (
                                (accnt.key_measure)::text = ('Cost of Goods Sold'::character varying)::text
                            ) THEN copa.amt_obj_crncy
                            ELSE ((0)::numeric)::numeric(18, 0)
                        END AS cogs_val,
                        CASE
                            WHEN (
                                (accnt.key_measure)::text = ('Consumer Free Goods'::character varying)::text
                            ) THEN copa.amt_obj_crncy
                            ELSE ((0)::numeric)::numeric(18, 0)
                        END AS con_free_goods_val,
                        CASE
                            WHEN (
                                (accnt.key_measure)::text = ('Trade Promotion'::character varying)::text
                            ) THEN copa.amt_obj_crncy
                            ELSE ((0)::numeric)::numeric(18, 0)
                        END AS tp_val
                    FROM ASPedw_integration.edw_copa_trans_fact copa,
                        pcfedw_integration.edw_ciw_accnt_map accnt,
                        (
                            SELECT DISTINCT edw_account_dim.acct_num,
                                edw_account_dim.acct_nm
                            FROM ASPedw_integration.edw_account_dim
                            WHERE (
                                    (edw_account_dim.bravo_acct_l1)::text <> (''::character varying)::text
                                )
                        ) ead,
                        (
                            (
                                SELECT DISTINCT dly_sls_cust_attrb_lkp.sls_org,
                                    dly_sls_cust_attrb_lkp.cmp_id
                                FROM pcfedw_integration.dly_sls_cust_attrb_lkp
                                UNION ALL
                                SELECT '330A'::character varying AS "varchar",
                                    '747A'::character varying AS "varchar"
                            )
                            UNION ALL
                            SELECT '341A'::character varying AS "varchar",
                                '836A'::character varying AS "varchar"
                        ) lkp
                    WHERE (
                            (
                                (
                                    (
                                        (
                                            ltrim(
                                                (copa.acct_num)::text,
                                                ('0'::character varying)::text
                                            ) = (accnt.sap_account)::text
                                        )
                                        AND (
                                            ltrim(
                                                (ead.acct_num)::text,
                                                ('0'::character varying)::text
                                            ) = (accnt.sap_account)::text
                                        )
                                    )
                                    AND ((copa.co_cd)::text = (lkp.cmp_id)::text)
                                )
                                AND (
                                    (copa.acct_hier_shrt_desc)::text <> ('NTS'::character varying)::text
                                )
                            )
                            AND (
                                (copa.acct_hier_shrt_desc)::text <> ('FG'::character varying)::text
                            )
                        )
                ) derived_table1
            GROUP BY derived_table1.cmp_id,
                derived_table1.cal_month_id,
                derived_table1.jj_month_id,
                derived_table1.cust_no,
                derived_table1.matl_id,
                derived_table1.key_measure,
                derived_table1.ciw_category,
                derived_table1.ciw_account_group,
                derived_table1.sap_account,
                derived_table1.sap_account_nm,
                derived_table1.local_ccy
        ) a
        LEFT JOIN pcfedw_integration.vw_sap_std_cost c ON (
            (
                ((c.matnr)::text = (a.matl_id)::text)
                AND ((c.cmp_no)::text = (a.cmp_id)::text)
            )
        )
    )
WHERE (
        (
            (
                (
                    (
                        ((a.jj_month_id)::numeric)::numeric(18, 0) = b.jj_mnth_id
                    )
                    AND ((a.local_ccy)::text = (b.from_ccy)::text)
                )
                AND (
                    ((a.jj_month_id)::numeric)::numeric(18, 0) = d.jj_mnth_id
                )
            )
            AND ((a.local_ccy)::text = (d.to_ccy)::text)
        )
        AND (
            (b.to_ccy)::text = ('AUD'::character varying)::text
        )
    );
create or replace view VW_SAP_STD_COST(
	MATNR,
	CMP_NO,
	LAEPR,
	PEINH,
	STPRS,
	STD_COST,
	EXCHNG_RATE,
	BWKEY,
	STD_COST_AUD
) as

SELECT derived_table1.matnr,
    derived_table1.cmp_no,
    derived_table1.laepr,
    derived_table1.peinh,
    derived_table1.stprs,
    derived_table1.std_cost,
    derived_table1.exchng_rate,
    derived_table1.bwkey,
    (
        derived_table1.std_cost * derived_table1.exchng_rate
    ) AS std_cost_aud
FROM (
        SELECT DISTINCT edw_ecc_standard_cost.matnr,
            edw_ecc_standard_cost.bwkey,
            CASE
                WHEN (
                    (edw_ecc_standard_cost.bwkey)::text = ('3300')::text
                ) THEN '7470'
                WHEN (
                    (edw_ecc_standard_cost.bwkey)::text = ('330A')::text
                ) THEN '7470'
                WHEN (
                    (edw_ecc_standard_cost.bwkey)::text = ('3410')::text
                ) THEN '8361'
                WHEN (
                    (edw_ecc_standard_cost.bwkey)::text = ('341A')::text
                ) THEN '8361'
                ELSE NULL
            END AS cmp_no,
            edw_ecc_standard_cost.laepr,
            edw_ecc_standard_cost.peinh,
            edw_ecc_standard_cost.stprs,
            (
                edw_ecc_standard_cost.stprs / edw_ecc_standard_cost.peinh
            ) AS std_cost,
            (
                SELECT vw_curr_exch_dim.exch_rate
                FROM PCFEDW_INTEGRATION.vw_curr_exch_dim
                WHERE (
                        (
                            (
                                (
                                    (vw_curr_exch_dim.rate_type)::text = ('DWBP')::text
                                )
                                AND (
                                    (vw_curr_exch_dim.from_ccy)::text = ('SGD')::text
                                )
                            )
                            AND (
                                (vw_curr_exch_dim.to_ccy)::text = ('AUD')::text
                            )
                        )
                        AND (
                            vw_curr_exch_dim.valid_date = (
                                SELECT "max"(vw_curr_exch_dim.valid_date) AS "max"
                                FROM PCFEDW_INTEGRATION.vw_curr_exch_dim
                                WHERE (
                                        (
                                            (
                                                (
                                                    (vw_curr_exch_dim.rate_type)::text = ('DWBP')::text
                                                )
                                                AND (
                                                    (vw_curr_exch_dim.from_ccy)::text = ('SGD')::text
                                                )
                                            )
                                            AND (
                                                (vw_curr_exch_dim.to_ccy)::text = ('AUD')::text
                                            )
                                        )
                                        AND (
                                            vw_curr_exch_dim.valid_date <= (
                                                (
                                                    (
                                                        to_char(
                                                            current_timestamp()::timestamp_ntz(9),
                                                            ('YYYYMMDD')::text
                                                        )
                                                    )::integer
                                                )::numeric
                                            )::numeric(18, 0)
                                        )
                                    )
                            )
                        )
                    )
            ) AS exchng_rate
        FROM aspEDW_INTEGRATION.edw_ecc_standard_cost
        WHERE (
                (
                    (
                        (
                            (
                                (
                                    (edw_ecc_standard_cost.bwkey)::text = ('3330')::text
                                )
                                OR (
                                    (edw_ecc_standard_cost.bwkey)::text = ('330A')::text
                                )
                            )
                            OR (
                                (edw_ecc_standard_cost.bwkey)::text = ('3410')::text
                            )
                        )
                        OR (
                            (edw_ecc_standard_cost.bwkey)::text = ('341A')::text
                        )
                    )
                    AND (
                        (edw_ecc_standard_cost.matnr)::text LIKE ('00%')::text
                    )
                )
                AND (
                    (edw_ecc_standard_cost.bwkey)::text LIKE ('%A')::text
                )
            )
    ) derived_table1;
create or replace view V_RPT_PROMO_ANALYSIS(
	CMP_ID,
	COUNTRY,
	CUST_NO,
	PROMO_NUMBER,
	PX_JJ_YEAR,
	PX_JJ_MNTH,
	MATL_ID,
	SKU_NAME,
	HIERARCHY_LONGNAME,
	PROMO_NAME,
	PROMO_START_DATE,
	PROMO_STOP_DATE,
	LOCAL_CCY,
	CUST_NM,
	CHANNEL_DESC,
	SALES_OFFICE_DESC,
	SALES_GRP_DESC,
	MATL_DESC,
	MEGA_BRND_DESC,
	BRND_DESC,
	VARIANT_DESC,
	FRAN_DESC,
	GRP_FRAN_DESC,
	PROD_FRAN_DESC,
	PROD_HIER_DESC,
	PROD_MAJOR_DESC,
	PROD_MINOR_DESC,
	PX_BASE_QTY,
	PX_BASE_GTS,
	PX_PROMO_QTY,
	PX_PROMO_GTS,
	PX_TOTAL_QTY,
	PX_TOTAL_GTS,
	PX_BASE_COGS,
	PX_PROMO_COGS,
	PX_TOTAL_COGS,
	PX_BASE_TERMS,
	PX_PROMO_TERMS,
	PX_TOTAL_TERMS,
	CASE_QTY,
	TOTAL_PLAN_SPEND,
	TOTAL_PAID,
	COMMITTED_SPEND,
	FINANCE_COGS
) as
SELECT a.cmp_id,
    a.country,
    a.cust_no,
    a.promo_number,
    a.px_jj_year,
    a.px_jj_mnth,
    a.matl_id,
    a.sku_name,
    a.hierarchy_longname,
    a.promo_name,
    a.promo_start_date,
    a.promo_stop_date,
    a.local_ccy,
    a.cust_nm,
    a.channel_desc,
    a.sales_office_desc,
    a.sales_grp_desc,
    a.matl_desc,
    a.mega_brnd_desc,
    a.brnd_desc,
    a.variant_desc,
    a.fran_desc,
    a.grp_fran_desc,
    a.prod_fran_desc,
    a.prod_hier_desc,
    a.prod_major_desc,
    a.prod_minor_desc,
    a.px_base_qty,
    a.px_base_gts,
    a.px_promo_qty,
    a.px_promo_gts,
    a.px_total_qty,
    a.px_total_gts,
    COALESCE(
        (
            (a.px_base_qty)::numeric * (
                COALESCE(b.std_cost_aud, (0)::numeric) * ((1)::numeric / d.exch_rate)
            )
        ),
        (0)::numeric
    ) AS px_base_cogs,
    COALESCE(
        (
            (a.px_promo_qty)::numeric * (
                COALESCE(b.std_cost_aud, (0)::numeric) * ((1)::numeric / d.exch_rate)
            )
        ),
        (0)::numeric
    ) AS px_promo_cogs,
    COALESCE(
        (
            (a.px_total_qty)::numeric * (
                COALESCE(b.std_cost_aud, (0)::numeric) * ((1)::numeric / d.exch_rate)
            )
        ),
        (0)::numeric
    ) AS px_total_cogs,
    a.px_base_terms,
    a.px_promo_terms,
    a.px_total_terms,
    a.case_qty,
    a.total_plan_spend,
    a.total_paid,
    a.committed_spend,
    e.cogs_per_unit AS finance_cogs
FROM PCFEDW_INTEGRATION.vw_jjbr_curr_exch_dim d,
    (
        (
            (
                SELECT derived_table2.cmp_id,
                    derived_table2.country,
                    derived_table2.cust_no,
                    derived_table2.promo_number,
                    derived_table2.px_jj_year,
                    derived_table2.px_jj_mnth,
                    derived_table2.matl_id,
                    derived_table2.promo_start_date,
                    derived_table2.promo_stop_date,
                    derived_table2.hierarchy_longname,
                    derived_table2.promo_name,
                    derived_table2.sku_name,
                    derived_table2.local_ccy,
                    derived_table2.cust_nm,
                    derived_table2.channel_desc,
                    derived_table2.sales_office_desc,
                    derived_table2.sales_grp_desc,
                    derived_table2.matl_desc,
                    derived_table2.mega_brnd_desc,
                    derived_table2.brnd_desc,
                    derived_table2.variant_desc,
                    derived_table2.fran_desc,
                    derived_table2.grp_fran_desc,
                    derived_table2.prod_fran_desc,
                    derived_table2.prod_hier_desc,
                    derived_table2.prod_major_desc,
                    derived_table2.prod_minor_desc,
                    "max"(derived_table2.case_qty) AS case_qty,
                    "max"(derived_table2.total_plan_spend) AS total_plan_spend,
                    "max"(derived_table2.total_paid) AS total_paid,
                    "max"(derived_table2.committed_spend) AS committed_spend,
                    "max"(derived_table2.px_base_qty) AS px_base_qty,
                    "max"(derived_table2.px_base_gts) AS px_base_gts,
                    "max"(derived_table2.px_promo_qty) AS px_promo_qty,
                    "max"(derived_table2.px_promo_gts) AS px_promo_gts,
                    "max"(derived_table2.px_total_qty) AS px_total_qty,
                    "max"(derived_table2.px_total_gts) AS px_total_gts,
                    sum(derived_table2.px_base_terms) AS px_base_terms,
                    sum(derived_table2.px_promo_terms) AS px_promo_terms,
                    sum(derived_table2.px_total_terms) AS px_total_terms
                FROM (
                        SELECT epf.cmp_id,
                            epf.country,
                            epf.cust_no,
                            epf.promo_number,
                            etd.jj_year AS px_jj_year,
                            etd.jj_mnth_id AS px_jj_mnth,
                            epf.matl_id,
                            epf.promo_start_date,
                            epf.promo_stop_date,
                            epf.hierarchy_longname,
                            epf.promo_name,
                            epf.sku_name,
                            epf.local_ccy,
                            epf.cust_nm,
                            epf.channel_desc,
                            epf.sales_office_desc,
                            epf.sales_grp_desc,
                            epf.matl_desc,
                            epf.mega_brnd_desc,
                            epf.brnd_desc,
                            epf.variant_desc,
                            epf.fran_desc,
                            epf.grp_fran_desc,
                            epf.prod_fran_desc,
                            epf.prod_hier_desc,
                            epf.prod_major_desc,
                            epf.prod_minor_desc,
                            epf.case_qty,
                            epf.total_plan_spend,
                            epf.total_paid,
                            epf.committed_spend,
                            COALESCE(epf.normal_qty, (0)::bigint) AS px_base_qty,
                            COALESCE(
                                (
                                    (epf.normal_qty)::double precision * epl.lp_price
                                ),
                                (0)::double precision
                            ) AS px_base_gts,
                            COALESCE(epf.promotional_qty, (0)::bigint) AS px_promo_qty,
                            COALESCE(
                                (
                                    (epf.promotional_qty)::double precision * epl.lp_price
                                ),
                                (0)::double precision
                            ) AS px_promo_gts,
                            COALESCE(epf.estimate_qty, (0)::bigint) AS px_total_qty,
                            COALESCE(
                                (
                                    (epf.estimate_qty)::double precision * epl.lp_price
                                ),
                                (0)::double precision
                            ) AS px_total_gts,
                            COALESCE(
                                (
                                    (
                                        (epf.normal_qty)::double precision * epl.lp_price
                                    ) * (epf.terms_percentage)::double precision
                                ),
                                (0)::double precision
                            ) AS px_base_terms,
                            COALESCE(
                                (
                                    (
                                        (epf.promotional_qty)::double precision * epl.lp_price
                                    ) * (epf.terms_percentage)::double precision
                                ),
                                (0)::double precision
                            ) AS px_promo_terms,
                            COALESCE(
                                (
                                    (
                                        (epf.estimate_qty)::double precision * epl.lp_price
                                    ) * (epf.terms_percentage)::double precision
                                ),
                                (0)::double precision
                            ) AS px_total_terms
                        FROM PCFEDW_INTEGRATION.edw_time_dim etd,
                            (
                                (
                                    SELECT pmf.ac_attribute AS cust_no,
                                        pmf.p_promonumber AS promo_number,
                                        to_date(pmf.p_startdate) AS promo_start_date,
                                        to_date(pmf.p_stopdate) AS promo_stop_date,
                                        pmf.hierarchy_longname,
                                        pmf.activity_longname AS promo_name,
                                        pmf.sku_longname AS sku_name,
                                        pmf.sku_stockcode AS matl_id,
                                        vcd.curr_cd AS local_ccy,
                                        pmf.estimate_qty AS case_qty,
                                        pws.planspend_total AS total_plan_spend,
                                        pws.paid_total AS total_paid,
                                        CASE
                                            WHEN (
                                                upper((pws.promotionitemstatus)::text) = 'CLOSED'::text
                                            ) THEN pws.paid_total
                                            ELSE CASE
                                                WHEN (pws.planspend_total > pws.paid_total) THEN pws.planspend_total
                                                ELSE pws.paid_total
                                            END
                                        END AS committed_spend,
                                        pmf.normal_qty,
                                        pmf.promotional_qty,
                                        pmf.estimate_qty,
                                        px_terms.terms_percentage,
                                        vcd.cmp_id,
                                        vcd.country,
                                        vcd.cust_nm,
                                        vcd.channel_desc,
                                        vcd.sales_office_desc,
                                        vcd.sales_grp_desc,
                                        vmd.matl_desc,
                                        vmd.mega_brnd_desc,
                                        vmd.brnd_desc,
                                        vmd.variant_desc,
                                        vmd.fran_desc,
                                        vmd.grp_fran_desc,
                                        vmd.prod_fran_desc,
                                        vmd.prod_hier_desc,
                                        vmd.prod_mjr_desc AS prod_major_desc,
                                        vmd.prod_mnr_desc AS prod_minor_desc
                                    FROM (
                                            (
                                                (
                                                    (
                                                        (
                                                            SELECT itg_px_master.ac_code,
                                                                itg_px_master.ac_longname,
                                                                itg_px_master.ac_attribute,
                                                                itg_px_master.p_promonumber,
                                                                itg_px_master.p_startdate,
                                                                itg_px_master.p_stopdate,
                                                                itg_px_master.promo_length,
                                                                itg_px_master.p_buystartdatedef,
                                                                itg_px_master.p_buystopdatedef,
                                                                itg_px_master.buyperiod_length,
                                                                itg_px_master.hierarchy_rowid,
                                                                itg_px_master.hierarchy_longname,
                                                                itg_px_master.activity_longname,
                                                                itg_px_master.confirmed_switch,
                                                                itg_px_master.closed_switch,
                                                                itg_px_master.sku_longname,
                                                                itg_px_master.sku_stockcode,
                                                                itg_px_master.sku_profitcentre,
                                                                itg_px_master.sku_attribute,
                                                                sum(itg_px_master.case_deal) AS case_deal,
                                                                sum(itg_px_master.planspend_total) AS planspend_total,
                                                                sum(itg_px_master.paid_total) AS paid_total,
                                                                sum(itg_px_master.p_deleted) AS p_deleted,
                                                                itg_px_master.transaction_attribute,
                                                                itg_px_master.promotionrowid,
                                                                sum(itg_px_master.case_quantity) AS estimate_qty,
                                                                avg(itg_px_master.normal_qty) AS normal_qty,
                                                                (
                                                                    sum(COALESCE(itg_px_master.case_quantity, 0)) - avg(COALESCE(itg_px_master.normal_qty, 0))
                                                                ) AS promotional_qty
                                                            FROM PCFITG_INTEGRATION.itg_px_master
                                                            GROUP BY itg_px_master.ac_code,
                                                                itg_px_master.ac_longname,
                                                                itg_px_master.ac_attribute,
                                                                itg_px_master.p_promonumber,
                                                                itg_px_master.p_startdate,
                                                                itg_px_master.p_stopdate,
                                                                itg_px_master.promo_length,
                                                                itg_px_master.p_buystartdatedef,
                                                                itg_px_master.p_buystopdatedef,
                                                                itg_px_master.buyperiod_length,
                                                                itg_px_master.hierarchy_rowid,
                                                                itg_px_master.hierarchy_longname,
                                                                itg_px_master.activity_longname,
                                                                itg_px_master.confirmed_switch,
                                                                itg_px_master.closed_switch,
                                                                itg_px_master.sku_longname,
                                                                itg_px_master.sku_stockcode,
                                                                itg_px_master.sku_profitcentre,
                                                                itg_px_master.sku_attribute,
                                                                itg_px_master.transaction_attribute,
                                                                itg_px_master.promotionrowid
                                                        ) pmf
                                                        LEFT JOIN PCFEDW_INTEGRATION.edw_vw_terms_master px_terms ON (
                                                            (
                                                                (
                                                                    ltrim((px_terms.cust_id)::text, '0'::text) = ltrim((pmf.ac_attribute)::text, '0'::text)
                                                                )
                                                            )
                                                        )
                                                    )
                                                    LEFT JOIN PCFEDW_INTEGRATION.vw_customer_dim vcd ON (
                                                        (
                                                            (
                                                                (pmf.ac_attribute)::text = ltrim((vcd.cust_no)::text, '0'::text)
                                                            )
                                                        )
                                                    )
                                                )
                                                LEFT JOIN PCFEDW_INTEGRATION.vw_material_dim vmd ON (
                                                    (
                                                        (
                                                            (pmf.sku_stockcode)::text = ltrim((vmd.matl_id)::text, '0'::text)
                                                        )
                                                    )
                                                )
                                            )
                                            LEFT JOIN (
                                                SELECT itg_px_weekly_sell.promotionrowid,
                                                    itg_px_weekly_sell.sku_stockcode,
                                                    itg_px_weekly_sell.promotionitemstatus,
                                                    sum(itg_px_weekly_sell.paid_total) AS paid_total,
                                                    sum(itg_px_weekly_sell.planspend_total) AS planspend_total
                                                FROM PCFITG_INTEGRATION.itg_px_weekly_sell
                                                GROUP BY itg_px_weekly_sell.promotionrowid,
                                                    itg_px_weekly_sell.sku_stockcode,
                                                    itg_px_weekly_sell.promotionitemstatus
                                            ) pws ON (
                                                (
                                                    (
                                                        COALESCE(pmf.promotionrowid, -999999) = COALESCE(pws.promotionrowid, -999999)
                                                    )
                                                    AND (
                                                        (
                                                            COALESCE(pmf.sku_stockcode, '-999999'::character varying)
                                                        )::text = (
                                                            COALESCE(pws.sku_stockcode, '-999999'::character varying)
                                                        )::text
                                                    )
                                                )
                                            )
                                        )
                                ) epf
                                LEFT JOIN (
                                    SELECT derived_table1.sku_stockcode,
                                        derived_table1.cmp_id,
                                        derived_table1.lp_price
                                    FROM (
                                            SELECT DISTINCT lp.sku_stockcode,
                                                lkp.cmp_id,
                                                lp.lp_price,
                                                rank() OVER(
                                                    PARTITION BY lp.sku_stockcode,
                                                    lkp.cmp_id
                                                    ORDER BY lp.lp_startdate DESC
                                                ) AS rank_number
                                            FROM PCFEDW_INTEGRATION.edw_px_listprice lp,
                                                (
                                                    SELECT DISTINCT dly_sls_cust_attrb_lkp.cmp_id,
                                                        dly_sls_cust_attrb_lkp.sls_org
                                                    FROM PCFEDW_INTEGRATION.dly_sls_cust_attrb_lkp
                                                ) lkp
                                            WHERE (
                                                    (
                                                        (lp.sales_org = (lkp.sls_org)::character(20))
                                                        AND (
                                                            to_date(lp.lp_startdate) <= to_date(CURRENT_TIMESTAMP()::TIMESTAMP_NTZ(9))
                                                        )
                                                    )
                                                    AND (
                                                        (
                                                            to_date(lp.lp_stopdate) > to_date(CURRENT_TIMESTAMP()::TIMESTAMP_NTZ(9))
                                                        )
                                                        OR (to_date(lp.lp_stopdate) IS NULL)
                                                    )
                                                )
                                        ) derived_table1
                                    WHERE (derived_table1.rank_number = 1)
                                ) epl ON (
                                    (
                                        ((epf.matl_id)::text = (epl.sku_stockcode)::text)
                                        AND ((epf.cmp_id)::text = (epl.cmp_id)::text)
                                    )
                                )
                            )
                        WHERE (
                                to_date(etd.cal_date) = to_date(
                                    (epf.promo_start_date)::timestamp without time zone
                                )
                            )
                    ) derived_table2
                GROUP BY derived_table2.cmp_id,
                    derived_table2.country,
                    derived_table2.cust_no,
                    derived_table2.promo_number,
                    derived_table2.px_jj_year,
                    derived_table2.px_jj_mnth,
                    derived_table2.matl_id,
                    derived_table2.promo_start_date,
                    derived_table2.promo_stop_date,
                    derived_table2.hierarchy_longname,
                    derived_table2.promo_name,
                    derived_table2.sku_name,
                    derived_table2.local_ccy,
                    derived_table2.cust_nm,
                    derived_table2.channel_desc,
                    derived_table2.sales_office_desc,
                    derived_table2.sales_grp_desc,
                    derived_table2.matl_desc,
                    derived_table2.mega_brnd_desc,
                    derived_table2.brnd_desc,
                    derived_table2.variant_desc,
                    derived_table2.fran_desc,
                    derived_table2.grp_fran_desc,
                    derived_table2.prod_fran_desc,
                    derived_table2.prod_hier_desc,
                    derived_table2.prod_major_desc,
                    derived_table2.prod_minor_desc
            ) a
            LEFT JOIN PCFEDW_INTEGRATION.vw_sap_std_cost b ON (
                (
                    (
                        (a.matl_id)::text = ltrim((b.matnr)::text, '0'::text)
                    )
                    AND ((a.cmp_id)::text = (b.cmp_no)::text)
                )
            )
        )
        LEFT JOIN PCFEDW_INTEGRATION.edw_vw_mds_cogs_rate_dim e ON (
            (
                ((a.matl_id)::text = (e.matl_id)::text)
                AND (a.px_jj_mnth = e.jj_mnth_id)
                AND (a.local_ccy = (e.crncy)::text)
            )
        )
    )
WHERE (
        (
            (
                ((d.to_ccy)::text = 'AUD'::text)
                AND (a.px_jj_mnth = d.jj_mnth_id)
            )
            AND (a.local_ccy = (d.from_ccy)::text)
        )
        AND (
            (a.px_jj_year)::double precision >= (
                date_part(
                    YEAR,
                    CURRENT_TIMESTAMP()
                ) - (
                    (
                        SELECT (itg_query_parameters.parameter_value)::integer AS parameter_value
                        FROM PCFITG_INTEGRATION.itg_query_parameters
                        WHERE (
                                (
                                    (itg_query_parameters.parameter_name)::text = 'PROMO_ROI_TDE_DATA_RETENTION_YEARS'::text
                                )
                                AND (
                                    (itg_query_parameters.country_code)::text = 'ANZ'::text
                                )
                            )
                    )
                )::double precision
            )
        )
    );
	
