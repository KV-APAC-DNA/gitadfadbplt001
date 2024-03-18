create or replace view PCFEDW_INTEGRATION.VW_SAP_STD_COST(
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

create or replace view PCFEDW_INTEGRATION.VW_SAPBW_FACT(
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
