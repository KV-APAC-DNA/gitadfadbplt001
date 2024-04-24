create or replace view PROD_DNA_CORE.PHLEDW_INTEGRATION.EDW_VW_PH_MATERIAL_DIM(
        cntry_key,
        sap_matl_num,
        sap_mat_desc,
        ean_num,
        sap_mat_type_cd,
        sap_mat_type_desc,
        sap_base_uom_cd,
        sap_prchse_uom_cd,
        sap_prod_sgmt_cd,
        sap_prod_sgmt_desc,
        sap_base_prod_cd,
        sap_base_prod_desc,
        sap_mega_brnd_cd,
        sap_mega_brnd_desc,
        sap_brnd_cd,
        sap_brnd_desc,
        sap_vrnt_cd,
        sap_vrnt_desc,
        sap_put_up_cd,
        sap_put_up_desc,
        sap_grp_frnchse_cd,
        sap_grp_frnchse_desc,
        sap_frnchse_cd,
        sap_frnchse_desc,
        sap_prod_frnchse_cd,
        sap_prod_frnchse_desc,
        sap_prod_mjr_cd,
        sap_prod_mjr_desc,
        sap_prod_mnr_cd,
        sap_prod_mnr_desc,
        sap_prod_hier_cd,
        sap_prod_hier_desc,
        gph_region,
        gph_reg_frnchse,
        gph_reg_frnchse_grp,
        gph_prod_frnchse,
        gph_prod_brnd,
        gph_prod_sub_brnd,
        gph_prod_vrnt,
        gph_prod_needstate,
        gph_prod_ctgry,
        gph_prod_subctgry,
        gph_prod_sgmnt,
        gph_prod_subsgmnt,
        gph_prod_put_up_cd,
        gph_prod_put_up_desc,
        gph_prod_size,
        gph_prod_size_uom,
        launch_dt,
        qty_shipper_pc,
        prft_ctr,
        shlf_life
    ) as (
        select ph.cntry_key as cntry_key,
            ph.sap_matl_num as sap_matl_num,
            ph.sap_mat_desc as sap_mat_desc,
            ph.ean_num as ean_num,
            ph.sap_mat_type_cd as sap_mat_type_cd,
            ph.sap_mat_type_desc as sap_mat_type_desc,
            ph.sap_base_uom_cd as sap_base_uom_cd,
            ph.sap_prchse_uom_cd as sap_prchse_uom_cd,
            ph.sap_prod_sgmt_cd as sap_prod_sgmt_cd,
            ph.sap_prod_sgmt_desc as sap_prod_sgmt_desc,
            ph.sap_base_prod_cd as sap_base_prod_cd,
            ph.sap_base_prod_desc as sap_base_prod_desc,
            ph.sap_mega_brnd_cd as sap_mega_brnd_cd,
            ph.sap_mega_brnd_desc as sap_mega_brnd_desc,
            ph.sap_brnd_cd as sap_brnd_cd,
            ph.sap_brnd_desc as sap_brnd_desc,
            ph.sap_vrnt_cd as sap_vrnt_cd,
            ph.sap_vrnt_desc as sap_vrnt_desc,
            ph.sap_put_up_cd as sap_put_up_cd,
            ph.sap_put_up_desc as sap_put_up_desc,
            ph.sap_grp_frnchse_cd as sap_grp_frnchse_cd,
            ph.sap_grp_frnchse_desc as sap_grp_frnchse_desc,
            ph.sap_frnchse_cd as sap_frnchse_cd,
            ph.sap_frnchse_desc as sap_frnchse_desc,
            ph.sap_prod_frnchse_cd as sap_prod_frnchse_cd,
            ph.sap_prod_frnchse_desc as sap_prod_frnchse_desc,
            ph.sap_prod_mjr_cd as sap_prod_mjr_cd,
            ph.sap_prod_mjr_desc as sap_prod_mjr_desc,
            ph.sap_prod_mnr_cd as sap_prod_mnr_cd,
            ph.sap_prod_mnr_desc as sap_prod_mnr_desc,
            ph.sap_prod_hier_cd as sap_prod_hier_cd,
            ph.sap_prod_hier_desc as sap_prod_hier_desc,
            ph.gph_region as gph_region,
            ph.gph_reg_frnchse as gph_reg_frnchse,
            ph.gph_reg_frnchse_grp as gph_reg_frnchse_grp,
            ph.gph_prod_frnchse as gph_prod_frnchse,
            ph.gph_prod_brnd as gph_prod_brnd,
            ph.gph_prod_sub_brnd as gph_prod_sub_brnd,
            ph.gph_prod_vrnt as gph_prod_vrnt,
            ph.gph_prod_needstate as gph_prod_needstate,
            ph.gph_prod_ctgry as gph_prod_ctgry,
            ph.gph_prod_subctgry as gph_prod_subctgry,
            ph.gph_prod_sgmnt as gph_prod_sgmnt,
            ph.gph_prod_subsgmnt as gph_prod_subsgmnt,
            ph.gph_prod_put_up_cd as gph_prod_put_up_cd,
            ph.gph_prod_put_up_desc as gph_prod_put_up_desc,
            ph.gph_prod_size as gph_prod_size,
            ph.gph_prod_size_uom as gph_prod_size_uom,
            ph.launch_dt as launch_dt,
            ph.qty_shipper_pc as qty_shipper_pc,
            ph.prft_ctr as prft_ctr,
            ph.shlf_life as shlf_life
        FROM (
                SELECT DISTINCT CAST(
                        (empd.cntry_key) AS VARCHAR(4)
                    ) AS cntry_key,
                    emd.matl_num AS sap_matl_num,
                    emd.matl_desc AS sap_mat_desc,
                    CAST(
                        (CAST(NULL AS VARCHAR)) AS VARCHAR(100)
                    ) AS ean_num,
                    emd.matl_type_cd AS sap_mat_type_cd,
                    emd.matl_type_desc AS sap_mat_type_desc,
                    emd.base_uom_cd AS sap_base_uom_cd,
                    emd.prch_uom_cd AS sap_prchse_uom_cd,
                    emd.prodh1 AS sap_prod_sgmt_cd,
                    emd.prodh1_txtmd AS sap_prod_sgmt_desc,
                    emd.prod_base AS sap_base_prod_cd,
                    emd.base_prod_desc AS sap_base_prod_desc,
                    emd.mega_brnd_cd AS sap_mega_brnd_cd,
                    emd.mega_brnd_desc AS sap_mega_brnd_desc,
                    emd.brnd_cd AS sap_brnd_cd,
                    emd.brnd_desc AS sap_brnd_desc,
                    emd.vrnt AS sap_vrnt_cd,
                    emd.varnt_desc AS sap_vrnt_desc,
                    emd.put_up AS sap_put_up_cd,
                    emd.put_up_desc AS sap_put_up_desc,
                    emd.prodh2 AS sap_grp_frnchse_cd,
                    emd.prodh2_txtmd AS sap_grp_frnchse_desc,
                    emd.prodh3 AS sap_frnchse_cd,
                    emd.prodh3_txtmd AS sap_frnchse_desc,
                    emd.prodh4 AS sap_prod_frnchse_cd,
                    emd.prodh4_txtmd AS sap_prod_frnchse_desc,
                    emd.prodh5 AS sap_prod_mjr_cd,
                    emd.prodh5_txtmd AS sap_prod_mjr_desc,
                    emd.prodh5 AS sap_prod_mnr_cd,
                    emd.prodh5_txtmd AS sap_prod_mnr_desc,
                    emd.prodh6 AS sap_prod_hier_cd,
                    emd.prodh6_txtmd AS sap_prod_hier_desc,
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
                    egph.size AS gph_prod_size,
                    egph.unit_of_measure AS gph_prod_size_uom,
                    CAST(CURRENT_TIMESTAMP() AS TIMESTAMPNTZ) AS launch_dt,
                    CAST(
                        (CAST(NULL AS VARCHAR)) AS VARCHAR(100)
                    ) AS qty_shipper_pc,
                    CAST(
                        (CAST(NULL AS VARCHAR)) AS VARCHAR(100)
                    ) AS prft_ctr,
                    CAST(
                        (CAST(NULL AS VARCHAR)) AS VARCHAR(100)
                    ) AS shlf_life
                FROM prod_dna_core.aspedw_integration.edw_material_dim AS emd,
                    prod_dna_core.aspedw_integration.edw_gch_producthierarchy AS egph,
                    (
                        SELECT DISTINCT edw_material_plant_dim.matl_num,
                            CASE
                                WHEN (
                                    (
                                        CAST('2300' AS TEXT) = CAST('PH' AS TEXT)
                                    )
                                    OR (
                                        (
                                            '2300' IS NULL
                                        )
                                        AND (
                                            'PH' IS NULL
                                        )
                                    )
                                ) THEN CAST('230A' AS TEXT)
                                ELSE CAST('PH' AS TEXT)
                            END AS cntry_key
                        FROM prod_dna_core.aspedw_integration.edw_material_plant_dim
                        WHERE (
                                (
                                    CAST(
                                        (
                                            edw_material_plant_dim.plnt
                                        ) AS TEXT
                                    ) = CAST('2300' AS TEXT)
                                )
                                OR (
                                    CAST(
                                        (
                                            edw_material_plant_dim.plnt
                                        ) AS TEXT
                                    ) = CAST('230A' AS TEXT)
                                )
                            )
                    ) AS empd
                WHERE (
                        (
                            (
                                (
                                    CAST(
                                        (
                                            emd.matl_num
                                        ) AS TEXT
                                    ) = CAST(
                                        (
                                            egph.materialnumber
                                        ) AS TEXT
                                    )
                                )
                                AND (
                                    CAST(
                                        (
                                            emd.matl_num
                                        ) AS TEXT
                                    ) = CAST(
                                        (
                                            empd.matl_num
                                        ) AS TEXT
                                    )
                                )
                            )
                            AND (
                                CAST(
                                    (
                                        emd.prod_hier_cd
                                    ) AS TEXT
                                ) <> CAST('' AS TEXT)
                            )
                        )
                        AND (
                            (
                                (
                                    (
                                        (
                                            (
                                                CAST(
                                                    (
                                                        emd.matl_type_cd
                                                    ) AS TEXT
                                                ) = CAST('FERT' AS TEXT)
                                            )
                                            OR (
                                                CAST(
                                                    (
                                                        emd.matl_type_cd
                                                    ) AS TEXT
                                                ) = CAST('HALB' AS TEXT)
                                            )
                                        )
                                        OR (
                                            CAST(
                                                (
                                                    emd.matl_type_cd
                                                ) AS TEXT
                                            ) = CAST('PROM' AS TEXT)
                                        )
                                    )
                                    OR (
                                        CAST(
                                            (
                                                emd.matl_type_cd
                                            ) AS TEXT
                                        ) = CAST('SAPR' AS TEXT)
                                    )
                                )
                                OR (
                                    CAST(
                                        (
                                            emd.matl_type_cd
                                        ) AS TEXT
                                    ) = CAST('ROH' AS TEXT)
                                )
                            )
                            OR (
                                CAST(
                                    (
                                        emd.matl_type_cd
                                    ) AS TEXT
                                ) = CAST('FER2' AS TEXT)
                            )
                        )
                    )
            ) AS ph
    );



create or replace view PROD_DNA_CORE.PHLEDW_ACCESS.EDW_VW_PH_POS_ANALYSIS_V2(
	"jj_year",
	"jj_qtr",
	"jj_mnth_id",
	"jj_mnth_no",
	"cntry_nm",
	"cust_cd",
	"cust_brnch_cd",
	"mt_cust_brnch_nm",
	"region_cd",
	"region_nm",
	"prov_cd",
	"prov_nm",
	"mncplty_cd",
	"mncplty_nm",
	"city_cd",
	"city_nm",
	"ae_nm",
	"ash_no",
	"ash_nm",
	"pms_nm",
	"item_cd",
	"mt_item_nm",
	"sold_to",
	"sold_to_nm",
	"region",
	"chnl_cd",
	"chnl_desc",
	"sub_chnl_cd",
	"sub_chnl_desc",
	"parent_customer_cd",
	"parent_customer",
	"account_grp",
	"trade_type",
	"sls_grp_desc",
	"sap_state_cd",
	"sap_sls_org",
	"sap_cmp_id",
	"sap_cntry_cd",
	"sap_cntry_nm",
	"sap_addr",
	"sap_region",
	"sap_city",
	"sap_post_cd",
	"sap_chnl_cd",
	"sap_chnl_desc",
	"sap_sls_office_cd",
	"sap_sls_office_desc",
	"sap_sls_grp_cd",
	"sap_sls_grp_desc",
	"sap_curr_cd",
	"gch_region",
	"gch_cluster",
	"gch_subcluster",
	"gch_market",
	"gch_retail_banner",
	"sku",
	"sku_desc",
	"sap_mat_type_cd",
	"sap_mat_type_desc",
	"sap_base_uom_cd",
	"sap_prchse_uom_cd",
	"sap_prod_sgmt_cd",
	"sap_prod_sgmt_desc",
	"sap_base_prod_cd",
	"sap_base_prod_desc",
	"sap_mega_brnd_cd",
	"sap_mega_brnd_desc",
	"sap_brnd_cd",
	"sap_brnd_desc",
	"sap_vrnt_cd",
	"sap_vrnt_desc",
	"sap_put_up_cd",
	"sap_put_up_desc",
	"sap_grp_frnchse_cd",
	"sap_grp_frnchse_desc",
	"sap_frnchse_cd",
	"sap_frnchse_desc",
	"sap_prod_frnchse_cd",
	"sap_prod_frnchse_desc",
	"sap_prod_mjr_cd",
	"sap_prod_mjr_desc",
	"sap_prod_mnr_cd",
	"sap_prod_mnr_desc",
	"sap_prod_hier_cd",
	"sap_prod_hier_desc",
	"global_mat_region",
	"global_prod_franchise",
	"global_prod_brand",
	"global_prod_variant",
	"global_prod_put_up_cd",
	"global_put_up_desc",
	"global_prod_sub_brand",
	"global_prod_need_state",
	"global_prod_category",
	"global_prod_subcategory",
	"global_prod_segment",
	"global_prod_subsegment",
	"global_prod_size",
	"global_prod_size_uom",
	"is_reg",
	"is_promo",
	"local_mat_promo_strt_period",
	"is_npi",
	"is_hero",
	"is_mcl",
	"local_mat_npi_strt_period",
	"pos_qty",
	"pos_gts",
	"pos_item_prc",
	"pos_tax",
	"pos_nts",
	"conv_factor",
	"jj_qty_pc",
	"jj_item_prc_per_pc",
	"jj_gts",
	"jj_vat_amt",
	"jj_nts"
) as
SELECT 
jj_year as "jj_year",
jj_qtr as "jj_qtr",
jj_mnth_id as "jj_mnth_id",
jj_mnth_no as "jj_mnth_no",
cntry_nm as "cntry_nm",
cust_cd as "cust_cd",
cust_brnch_cd as "cust_brnch_cd",
mt_cust_brnch_nm as "mt_cust_brnch_nm",
region_cd as "region_cd",
region_nm as "region_nm",
prov_cd as "prov_cd",
prov_nm as "prov_nm",
mncplty_cd as "mncplty_cd",
mncplty_nm as "mncplty_nm",
city_cd as "city_cd",
city_nm as "city_nm",
ae_nm as "ae_nm",
ash_no as "ash_no",
ash_nm as "ash_nm",
pms_nm as "pms_nm",
item_cd as "item_cd",
mt_item_nm as "mt_item_nm",
sold_to as "sold_to",
sold_to_nm as "sold_to_nm",
region as "region",
chnl_cd as "chnl_cd",
chnl_desc as "chnl_desc",
sub_chnl_cd as "sub_chnl_cd",
sub_chnl_desc as "sub_chnl_desc",
parent_customer_cd as "parent_customer_cd",
parent_customer as "parent_customer",
account_grp as "account_grp",
trade_type as "trade_type",
sls_grp_desc as "sls_grp_desc",
"sap_state_cd",
"sap_sls_org",
"sap_cmp_id",
"sap_cntry_cd",
"sap_cntry_nm",
"sap_addr",
"sap_region",
"sap_city",
"sap_post_cd",
"sap_chnl_cd",
"sap_chnl_desc",
"sap_sls_office_cd",
"sap_sls_office_desc",
"sap_sls_grp_cd",
"sap_sls_grp_desc",
"sap_curr_cd",
"gch_region",
"gch_cluster",
"gch_subcluster",
"gch_market",
"gch_retail_banner",
sku as "sku",
sku_desc as "sku_desc",
"sap_mat_type_cd",
"sap_mat_type_desc",
"sap_base_uom_cd",
"sap_prchse_uom_cd",
"sap_prod_sgmt_cd",
"sap_prod_sgmt_desc",
"sap_base_prod_cd",
"sap_base_prod_desc",
"sap_mega_brnd_cd",
"sap_mega_brnd_desc",
"sap_brnd_cd",
"sap_brnd_desc",
"sap_vrnt_cd",
"sap_vrnt_desc",
"sap_put_up_cd",
"sap_put_up_desc",
"sap_grp_frnchse_cd",
"sap_grp_frnchse_desc",
"sap_frnchse_cd",
"sap_frnchse_desc",
"sap_prod_frnchse_cd",
"sap_prod_frnchse_desc",
"sap_prod_mjr_cd",
"sap_prod_mjr_desc",
"sap_prod_mnr_cd",
"sap_prod_mnr_desc",
"sap_prod_hier_cd",
"sap_prod_hier_desc",
global_mat_region as "global_mat_region",
global_prod_franchise as "global_prod_franchise",
global_prod_brand as "global_prod_brand",
global_prod_variant as "global_prod_variant",
global_prod_put_up_cd as "global_prod_put_up_cd",
global_put_up_desc as "global_put_up_desc",
global_prod_sub_brand as "global_prod_sub_brand",
global_prod_need_state as "global_prod_need_state",
global_prod_category as "global_prod_category",
global_prod_subcategory as "global_prod_subcategory",
global_prod_segment as "global_prod_segment",
global_prod_subsegment as "global_prod_subsegment",
global_prod_size as "global_prod_size",
global_prod_size_uom as "global_prod_size_uom",
is_reg as "is_reg",
is_promo as "is_promo",
local_mat_promo_strt_period as "local_mat_promo_strt_period",
is_npi as "is_npi",
is_hero as "is_hero",
is_mcl as "is_mcl",
local_mat_npi_strt_period as "local_mat_npi_strt_period",
pos_qty as "pos_qty",
pos_gts as "pos_gts",
pos_item_prc as "pos_item_prc",
pos_tax as "pos_tax",
pos_nts as "pos_nts",
conv_factor as "conv_factor",
jj_qty_pc as "jj_qty_pc",
jj_item_prc_per_pc as "jj_item_prc_per_pc",
jj_gts as "jj_gts",
jj_vat_amt as "jj_vat_amt",
jj_nts as "jj_nts"
 FROM 
(SELECT veposf."year" AS jj_year,
    veposf.qrtr AS jj_qtr,
    veposf.jj_mnth_id,
    veposf.mnth_no AS jj_mnth_no,
    veposf.cntry_nm,
    veposf.cust_cd,
    veposf.cust_brnch_cd,
    veposf.brnch_nm AS mt_cust_brnch_nm,
    veposf.region_cd,
    veposf.region_nm,
    veposf.prov_cd,
    veposf.prov_nm,
    veposf.mncplty_cd,
    veposf.mncplty_nm,
    veposf.city_cd,
    veposf.city_nm,
    veposf.ae_nm,
    veposf.ash_no,
    veposf.ash_nm,
    veposf.pms_nm,
    veposf.item_cd,
    veposf.item_nm AS mt_item_nm,
    veposf.sold_to,
    veocd."sap_cust_nm" AS sold_to_nm,
    eocd.region,
    eocd.channel_cd AS chnl_cd,
    eocd.channel_desc AS chnl_desc,
    eocd.sub_chnl_cd,
    eocd.sub_chnl_desc,
    eocd.parent_cust_cd AS parent_customer_cd,
    eocd.parent_cust_nm AS parent_customer,
    eocd.rpt_grp_6_desc AS account_grp,
    'MODERN TRADE' AS trade_type,
    eocd.rpt_grp_2_desc AS sls_grp_desc,
    veocd."sap_state_cd",
    veocd."sap_sls_org",
    veocd."sap_cmp_id",
    veocd."sap_cntry_cd",
    veocd."sap_cntry_nm",
    veocd."sap_addr",
    veocd."sap_region",
    veocd."sap_city",
    veocd."sap_post_cd",
    veocd."sap_chnl_cd",
    veocd."sap_chnl_desc",
    veocd."sap_sls_office_cd",
    veocd."sap_sls_office_desc",
    veocd."sap_sls_grp_cd",
    veocd."sap_sls_grp_desc",
    veocd."sap_curr_cd",
    veocd."gch_region",
    veocd."gch_cluster",
    veocd."gch_subcluster",
    veocd."gch_market",
    veocd."gch_retail_banner",
    ltrim(
        (veomd."sap_matl_num")::text,
        ('0'::character varying)::text
    ) AS sku,
    veomd."sap_mat_desc" AS sku_desc,
    veomd."sap_mat_type_cd",
    veomd."sap_mat_type_desc",
    veomd."sap_base_uom_cd",
    veomd."sap_prchse_uom_cd",
    veomd."sap_prod_sgmt_cd",
    veomd."sap_prod_sgmt_desc",
    veomd."sap_base_prod_cd",
    veomd."sap_base_prod_desc",
    veomd."sap_mega_brnd_cd",
    veomd."sap_mega_brnd_desc",
    veomd."sap_brnd_cd",
    veomd."sap_brnd_desc",
    veomd."sap_vrnt_cd",
    veomd."sap_vrnt_desc",
    veomd."sap_put_up_cd",
    veomd."sap_put_up_desc",
    veomd."sap_grp_frnchse_cd",
    veomd."sap_grp_frnchse_desc",
    veomd."sap_frnchse_cd",
    veomd."sap_frnchse_desc",
    veomd."sap_prod_frnchse_cd",
    veomd."sap_prod_frnchse_desc",
    veomd."sap_prod_mjr_cd",
    veomd."sap_prod_mjr_desc",
    veomd."sap_prod_mnr_cd",
    veomd."sap_prod_mnr_desc",
    veomd."sap_prod_hier_cd",
    veomd."sap_prod_hier_desc",
    veomd."gph_region" AS global_mat_region,
    veomd."gph_prod_frnchse" AS global_prod_franchise,
    veomd."gph_prod_brnd" AS global_prod_brand,
    veomd."gph_prod_vrnt" AS global_prod_variant,
    veomd."gph_prod_put_up_cd" AS global_prod_put_up_cd,
    veomd."gph_prod_put_up_desc" AS global_put_up_desc,
    veomd."gph_prod_sub_brnd" AS global_prod_sub_brand,
    veomd."gph_prod_needstate" AS global_prod_need_state,
    veomd."gph_prod_ctgry" AS global_prod_category,
    veomd."gph_prod_subctgry" AS global_prod_subcategory,
    veomd."gph_prod_sgmnt" AS global_prod_segment,
    veomd."gph_prod_subsgmnt" AS global_prod_subsegment,
    veomd."gph_prod_size" AS global_prod_size,
    veomd."gph_prod_size_uom" AS global_prod_size_uom,
    CASE
        WHEN (
            upper((epmad.promo_reg_ind)::text) = ('REG'::character varying)::text
        ) THEN 'Y'::character varying
        ELSE 'N'::character varying
    END AS is_reg,
    CASE
        WHEN (
            upper((epmad.promo_reg_ind)::text) = ('PROMO'::character varying)::text
        ) THEN 'Y'::character varying
        ELSE 'N'::character varying
    END AS is_promo,
    epmad.promo_strt_period AS local_mat_promo_strt_period,
    CASE
        WHEN (
            (
                (
                    (epp2.status)::text = ('**'::character varying)::text
                )
                AND (veotd2.mnth_id >= epp2.launch_period)
            )
            AND (veotd2.mnth_id <= epp2.end_period)
        ) THEN 'Y'::character varying
        ELSE 'N'::character varying
    END AS is_npi,
    CASE
        WHEN (
            upper((epmad.hero_sku_ind)::text) = ('Y'::character varying)::text
        ) THEN 'HERO'::character varying
        ELSE 'NA'::character varying
    END AS is_hero,
    NULL  AS is_mcl,
    epp2.launch_period AS local_mat_npi_strt_period,
    veposf.pos_qty,
    veposf.pos_gts,
    veposf.pos_item_prc,
    veposf.pos_tax,
    veposf.pos_nts,
    veposf.conv_factor,
    veposf.jj_qty_pc,
    veposf.jj_item_prc_per_pc,
    veposf.jj_gts,
    veposf.jj_vat_amt,
    veposf.jj_nts
FROM (
        SELECT edw_vw_os_time_dim.mnth_id
        FROM  sgpedw_integration.edw_vw_os_time_dim
        WHERE (
                edw_vw_os_time_dim.cal_date = (CURRENT_TIMESTAMP::VARCHAR)::TIMESTAMP_NTZ
                
            )
    ) veotd2,
    (
        (
            (
                (
                    (
                        (
                            SELECT b."year",
                                b.qrtr,
                                b.qrtr_no,
                                b.mnth_no,
                                a.cntry_cd,
                                c.cntry_nm,
                                a.jj_mnth_id,
                                a.cust_cd,
                                a.cust_brnch_cd,
                                e.primary_soldto AS sold_to,
                                c.brnch_nm,
                                c.region_cd,
                                c.region_nm,
                                c.prov_cd,
                                c.prov_nm,
                                c.mncplty_cd,
                                c.mncplty_nm,
                                c.city_cd,
                                c.city_nm,
                                f.ae_nm,
                                f.ash_no,
                                f.ash_nm,
                                f.pms_nm,
                                a.item_cd,
                                d.item_nm,
                                d.sap_item_cd,
                                a.pos_qty,
                                a.pos_gts,
                                a.pos_item_prc,
                                a.pos_tax,
                                a.pos_nts,
                                a.conv_factor,
                                a.jj_qty_pc,
                                a.jj_item_prc_per_pc,
                                a.jj_gts,
                                a.jj_vat_amt,
                                a.jj_nts
                            FROM (
                                    SELECT DISTINCT edw_vw_os_time_dim."year",
                                        edw_vw_os_time_dim.qrtr_no,
                                        edw_vw_os_time_dim.qrtr,
                                        edw_vw_os_time_dim.mnth_id,
                                        edw_vw_os_time_dim.mnth_desc,
                                        edw_vw_os_time_dim.mnth_no
                                    FROM  sgpedw_integration.edw_vw_os_time_dim
                                ) b,
                                (
                                    (
                                        (
                                            (
                                                (
                                                    SELECT 'PH'::character varying AS cntry_cd,
                                                        'PHILIPPINES'::character varying AS cntry_nm,
                                                        itg_ph_pos_pg_sm_sales_fact.jj_mnth_id,
                                                        itg_ph_pos_pg_sm_sales_fact.cust_cd,
                                                        itg_ph_pos_pg_sm_sales_fact.item_cd,
                                                        itg_ph_pos_pg_sm_sales_fact.brnch_cd AS cust_brnch_cd,
                                                        itg_ph_pos_pg_sm_sales_fact.pos_qty,
                                                        itg_ph_pos_pg_sm_sales_fact.pos_gts,
                                                        itg_ph_pos_pg_sm_sales_fact.pos_item_prc,
                                                        itg_ph_pos_pg_sm_sales_fact.pos_tax,
                                                        itg_ph_pos_pg_sm_sales_fact.pos_nts,
                                                        itg_ph_pos_pg_sm_sales_fact.conv_factor,
                                                        itg_ph_pos_pg_sm_sales_fact.jj_qty_pc,
                                                        itg_ph_pos_pg_sm_sales_fact.jj_item_prc_per_pc,
                                                        itg_ph_pos_pg_sm_sales_fact.jj_gts,
                                                        itg_ph_pos_pg_sm_sales_fact.jj_vat_amt,
                                                        itg_ph_pos_pg_sm_sales_fact.jj_nts
                                                    FROM  PHLITG_INTEGRATION.itg_ph_pos_pg_sm_sales_fact
                                                ) a
                                                LEFT JOIN (
                                                    SELECT edw_vw_ph_pos_customer_dim.cntry_cd,
                                                        edw_vw_ph_pos_customer_dim.cntry_nm,
                                                        edw_vw_ph_pos_customer_dim.cust_cd,
                                                        edw_vw_ph_pos_customer_dim.cust_nm,
                                                        edw_vw_ph_pos_customer_dim.sold_to,
                                                        edw_vw_ph_pos_customer_dim.brnch_cd,
                                                        edw_vw_ph_pos_customer_dim.brnch_nm,
                                                        edw_vw_ph_pos_customer_dim.brnch_frmt,
                                                        edw_vw_ph_pos_customer_dim.brnch_typ,
                                                        edw_vw_ph_pos_customer_dim.dept_cd,
                                                        edw_vw_ph_pos_customer_dim.dept_nm,
                                                        edw_vw_ph_pos_customer_dim.address1,
                                                        edw_vw_ph_pos_customer_dim.address2,
                                                        edw_vw_ph_pos_customer_dim.region_cd,
                                                        edw_vw_ph_pos_customer_dim.region_nm,
                                                        edw_vw_ph_pos_customer_dim.prov_cd,
                                                        edw_vw_ph_pos_customer_dim.prov_nm,
                                                        edw_vw_ph_pos_customer_dim.city_cd,
                                                        edw_vw_ph_pos_customer_dim.city_nm,
                                                        edw_vw_ph_pos_customer_dim.mncplty_cd,
                                                        edw_vw_ph_pos_customer_dim.mncplty_nm
                                                    FROM  phledw_integration.edw_vw_ph_pos_customer_dim
                                                    WHERE (
                                                            (edw_vw_ph_pos_customer_dim.cntry_cd)::text = ('PH'::character varying)::text
                                                        )
                                                ) c ON (
                                                    (
                                                        ((c.brnch_cd)::text = (a.cust_brnch_cd)::text)
                                                        AND ((c.cust_cd)::text = (a.cust_cd)::text)
                                                    )
                                                )
                                            )
                                            LEFT JOIN (
                                                SELECT itg_mds_ph_ref_pos_primary_sold_to.cust_cd,
                                                    itg_mds_ph_ref_pos_primary_sold_to.primary_soldto,
                                                    itg_mds_ph_ref_pos_primary_sold_to.last_chg_datetime,
                                                    itg_mds_ph_ref_pos_primary_sold_to.effective_from,
                                                    itg_mds_ph_ref_pos_primary_sold_to.effective_to,
                                                    itg_mds_ph_ref_pos_primary_sold_to.active,
                                                    itg_mds_ph_ref_pos_primary_sold_to.crtd_dttm,
                                                    itg_mds_ph_ref_pos_primary_sold_to.updt_dttm
                                                FROM  PHLITG_INTEGRATION.itg_mds_ph_ref_pos_primary_sold_to
                                                WHERE (
                                                        (itg_mds_ph_ref_pos_primary_sold_to.active)::text = ('Y'::character varying)::text
                                                    )
                                            ) e ON (((e.cust_cd)::text = (a.cust_cd)::text))
                                        )
                                        LEFT JOIN (
                                            SELECT edw_vw_ph_pos_material_dim.cntry_cd,
                                                edw_vw_ph_pos_material_dim.cntry_nm,
                                                edw_vw_ph_pos_material_dim.jj_mnth_id,
                                                edw_vw_ph_pos_material_dim.cust_cd,
                                                edw_vw_ph_pos_material_dim.item_cd,
                                                edw_vw_ph_pos_material_dim.item_nm,
                                                edw_vw_ph_pos_material_dim.sap_item_cd,
                                                edw_vw_ph_pos_material_dim.bar_cd,
                                                edw_vw_ph_pos_material_dim.cust_sku_grp,
                                                edw_vw_ph_pos_material_dim.cust_conv_factor,
                                                edw_vw_ph_pos_material_dim.cust_item_prc,
                                                edw_vw_ph_pos_material_dim.lst_period,
                                                edw_vw_ph_pos_material_dim.early_bk_period,
                                                edw_vw_ph_pos_material_dim.eff_str_date,
                                                edw_vw_ph_pos_material_dim.eff_end_date
                                            FROM  phledw_integration.edw_vw_ph_pos_material_dim
                                            WHERE (
                                                    (edw_vw_ph_pos_material_dim.cntry_cd)::text = ('PH'::character varying)::text
                                                )
                                        ) d ON (
                                            (
                                                (
                                                    (
                                                        ltrim(
                                                            (d.item_cd)::text,
                                                            ('0'::character varying)::text
                                                        ) = ltrim(
                                                            (a.item_cd)::text,
                                                            ('0'::character varying)::text
                                                        )
                                                    )
                                                    AND ((d.jj_mnth_id)::text = (a.jj_mnth_id)::text)
                                                )
                                                AND ((d.cust_cd)::text = (a.cust_cd)::text)
                                            )
                                        )
                                    )
                                    LEFT JOIN (
                                        SELECT itg_mds_ph_pos_customers.cust_cd,
                                            itg_mds_ph_pos_customers.brnch_cd,
                                            itg_mds_ph_pos_customers.ae_nm,
                                            itg_mds_ph_pos_customers.ash_no,
                                            itg_mds_ph_pos_customers.ash_nm,
                                            itg_mds_ph_pos_customers.pms_nm
                                        FROM  PHLITG_INTEGRATION.itg_mds_ph_pos_customers
                                        WHERE (
                                                (
                                                    (
                                                        (itg_mds_ph_pos_customers.cust_cd)::text = ('MDC'::character varying)::text
                                                    )
                                                    OR (
                                                        (itg_mds_ph_pos_customers.cust_cd)::text = ('WAT'::character varying)::text
                                                    )
                                                )
                                                AND (
                                                    (itg_mds_ph_pos_customers.active)::text = ('Y'::character varying)::text
                                                )
                                            )
                                    ) f ON (
                                        (
                                            ((f.cust_cd)::text = (a.cust_cd)::text)
                                            AND ((f.brnch_cd)::text = (a.cust_brnch_cd)::text)
                                        )
                                    )
                                )
                            WHERE (b.mnth_id = (a.jj_mnth_id)::text)
                        ) veposf
                        LEFT JOIN (
                            SELECT EDW_VW_PH_MATERIAL_DIM.cntry_key,
                                EDW_VW_PH_MATERIAL_DIM."sap_matl_num",
                                EDW_VW_PH_MATERIAL_DIM."sap_mat_desc",
                                EDW_VW_PH_MATERIAL_DIM."ean_num",
                                EDW_VW_PH_MATERIAL_DIM."sap_mat_type_cd",
                                EDW_VW_PH_MATERIAL_DIM."sap_mat_type_desc",
                                EDW_VW_PH_MATERIAL_DIM."sap_base_uom_cd",
                                EDW_VW_PH_MATERIAL_DIM."sap_prchse_uom_cd",
                                EDW_VW_PH_MATERIAL_DIM."sap_prod_sgmt_cd",
                                EDW_VW_PH_MATERIAL_DIM."sap_prod_sgmt_desc",
                                EDW_VW_PH_MATERIAL_DIM."sap_base_prod_cd",
                                EDW_VW_PH_MATERIAL_DIM."sap_base_prod_desc",
                                EDW_VW_PH_MATERIAL_DIM."sap_mega_brnd_cd",
                                EDW_VW_PH_MATERIAL_DIM."sap_mega_brnd_desc",
                                EDW_VW_PH_MATERIAL_DIM."sap_brnd_cd",
                                EDW_VW_PH_MATERIAL_DIM."sap_brnd_desc",
                                EDW_VW_PH_MATERIAL_DIM."sap_vrnt_cd",
                                EDW_VW_PH_MATERIAL_DIM."sap_vrnt_desc",
                                EDW_VW_PH_MATERIAL_DIM."sap_put_up_cd",
                                EDW_VW_PH_MATERIAL_DIM."sap_put_up_desc",
                                EDW_VW_PH_MATERIAL_DIM."sap_grp_frnchse_cd",
                                EDW_VW_PH_MATERIAL_DIM."sap_grp_frnchse_desc",
                                EDW_VW_PH_MATERIAL_DIM."sap_frnchse_cd",
                                EDW_VW_PH_MATERIAL_DIM."sap_frnchse_desc",
                                EDW_VW_PH_MATERIAL_DIM."sap_prod_frnchse_cd",
                                EDW_VW_PH_MATERIAL_DIM."sap_prod_frnchse_desc",
                                EDW_VW_PH_MATERIAL_DIM."sap_prod_mjr_cd",
                                EDW_VW_PH_MATERIAL_DIM."sap_prod_mjr_desc",
                                EDW_VW_PH_MATERIAL_DIM."sap_prod_mnr_cd",
                                EDW_VW_PH_MATERIAL_DIM."sap_prod_mnr_desc",
                                EDW_VW_PH_MATERIAL_DIM."sap_prod_hier_cd",
                                EDW_VW_PH_MATERIAL_DIM."sap_prod_hier_desc",
                                EDW_VW_PH_MATERIAL_DIM."gph_region",
                                EDW_VW_PH_MATERIAL_DIM."gph_reg_frnchse",
                                EDW_VW_PH_MATERIAL_DIM."gph_reg_frnchse_grp",
                                EDW_VW_PH_MATERIAL_DIM."gph_prod_frnchse",
                                EDW_VW_PH_MATERIAL_DIM."gph_prod_brnd",
                                EDW_VW_PH_MATERIAL_DIM."gph_prod_sub_brnd",
                                EDW_VW_PH_MATERIAL_DIM."gph_prod_vrnt",
                                EDW_VW_PH_MATERIAL_DIM."gph_prod_needstate",
                                EDW_VW_PH_MATERIAL_DIM."gph_prod_ctgry",
                                EDW_VW_PH_MATERIAL_DIM."gph_prod_subctgry",
                                EDW_VW_PH_MATERIAL_DIM."gph_prod_sgmnt",
                                EDW_VW_PH_MATERIAL_DIM."gph_prod_subsgmnt",
                                EDW_VW_PH_MATERIAL_DIM."gph_prod_put_up_cd",
                                EDW_VW_PH_MATERIAL_DIM."gph_prod_put_up_desc",
                                EDW_VW_PH_MATERIAL_DIM."gph_prod_size",
                                EDW_VW_PH_MATERIAL_DIM."gph_prod_size_uom",
                                EDW_VW_PH_MATERIAL_DIM."launch_dt",
                                EDW_VW_PH_MATERIAL_DIM."qty_shipper_pc",
                                EDW_VW_PH_MATERIAL_DIM."prft_ctr",
                                EDW_VW_PH_MATERIAL_DIM."shlf_life"
                            FROM  PHLEDW_INTEGRATION.EDW_VW_PH_MATERIAL_DIM
                            WHERE (
                                    (EDW_VW_PH_MATERIAL_DIM.cntry_key)::text = ('PH'::character varying)::text
                                )
                        ) veomd ON (
                            (
                                upper(
                                    ltrim(
                                        (veomd."sap_matl_num")::text,
                                        ((0)::character varying)::text
                                    )
                                ) = ltrim(
                                    (veposf.sap_item_cd)::text,
                                    ('0'::character varying)::text
                                )
                            )
                        )
                    )
                    LEFT JOIN PHLEDW_INTEGRATION.edw_mv_ph_customer_dim eocd ON (
                        (
                            upper(trim((eocd.cust_id)::text)) = upper(trim((veposf.sold_to)::text))
                        )
                    )
                )
                LEFT JOIN (
                    SELECT edw_vw_ph_customer_dim."sap_cust_id",
                        edw_vw_ph_customer_dim."sap_cust_nm",
                        edw_vw_ph_customer_dim."sap_sls_org",
                        edw_vw_ph_customer_dim."sap_cmp_id",
                        edw_vw_ph_customer_dim."sap_cntry_cd",
                        edw_vw_ph_customer_dim."sap_cntry_nm",
                        edw_vw_ph_customer_dim."sap_addr",
                        edw_vw_ph_customer_dim."sap_region",
                        edw_vw_ph_customer_dim."sap_state_cd",
                        edw_vw_ph_customer_dim."sap_city",
                        edw_vw_ph_customer_dim."sap_post_cd",
                        edw_vw_ph_customer_dim."sap_chnl_cd",
                        edw_vw_ph_customer_dim."sap_chnl_desc",
                        edw_vw_ph_customer_dim."sap_sls_office_cd",
                        edw_vw_ph_customer_dim."sap_sls_office_desc",
                        edw_vw_ph_customer_dim."sap_sls_grp_cd",
                        edw_vw_ph_customer_dim."sap_sls_grp_desc",
                        edw_vw_ph_customer_dim."sap_curr_cd",
                        edw_vw_ph_customer_dim."sap_prnt_cust_key",
                        edw_vw_ph_customer_dim."sap_prnt_cust_desc",
                        edw_vw_ph_customer_dim."sap_cust_chnl_key",
                        edw_vw_ph_customer_dim."sap_cust_chnl_desc",
                        edw_vw_ph_customer_dim."sap_cust_sub_chnl_key",
                        edw_vw_ph_customer_dim."sap_sub_chnl_desc",
                        edw_vw_ph_customer_dim."sap_go_to_mdl_key",
                        edw_vw_ph_customer_dim."sap_go_to_mdl_desc",
                        edw_vw_ph_customer_dim."sap_bnr_key",
                        edw_vw_ph_customer_dim."sap_bnr_desc",
                        edw_vw_ph_customer_dim."sap_bnr_frmt_key",
                        edw_vw_ph_customer_dim."sap_bnr_frmt_desc",
                        edw_vw_ph_customer_dim."retail_env",
                        edw_vw_ph_customer_dim."gch_region",
                        edw_vw_ph_customer_dim."gch_cluster",
                        edw_vw_ph_customer_dim."gch_subcluster",
                        edw_vw_ph_customer_dim."gch_market",
                        edw_vw_ph_customer_dim."gch_retail_banner"
                    FROM  phledw_integration.edw_vw_ph_customer_dim
                    WHERE (
                            (edw_vw_ph_customer_dim."sap_cntry_cd")::text = ('PH'::character varying)::text
                        )
                ) veocd ON (
                    (
                        upper(
                            ltrim(
                                (veocd."sap_cust_id")::text,
                                ('0'::character varying)::text
                            )
                        ) = upper(trim((veposf.sold_to)::text))
                    )
                )
            )
            LEFT JOIN (
                SELECT itg_mds_ph_lav_product.item_cd,
                    itg_mds_ph_lav_product.promo_reg_ind,
                    itg_mds_ph_lav_product.hero_sku_ind,
                    itg_mds_ph_lav_product.promo_strt_period,
                    itg_mds_ph_lav_product.npi_strt_period
                FROM  PHLITG_INTEGRATION.itg_mds_ph_lav_product
                WHERE (
                        (itg_mds_ph_lav_product.active)::text = ('Y'::character varying)::text
                    )
            ) epmad ON (
                (
                    upper(trim((epmad.item_cd)::text)) = ltrim(
                        (veposf.sap_item_cd)::text,
                        ('0'::character varying)::text
                    )
                )
            )
        )
        LEFT JOIN (
            SELECT itg_mds_ph_pos_pricelist.status,
                itg_mds_ph_pos_pricelist.item_cd,
                min((itg_mds_ph_pos_pricelist.jj_mnth_id)::text) AS launch_period,
                min(
                    to_char(
                        add_months(
                            (
                                (
                                    concat(
                                        (itg_mds_ph_pos_pricelist.jj_mnth_id)::text,
                                        ('01'::character varying)::text
                                    )
                                )::date
                            )::timestamp without time zone,
                            (11)::bigint
                        ),
                        ('YYYYMM'::character varying)::text
                    )
                ) AS end_period
            FROM  PHLITG_INTEGRATION.itg_mds_ph_pos_pricelist
            WHERE (
                    (
                        (itg_mds_ph_pos_pricelist.status)::text = ('**'::character varying)::text
                    )
                    AND (
                        (itg_mds_ph_pos_pricelist.active)::text = ('Y'::character varying)::text
                    )
                )
            GROUP BY itg_mds_ph_pos_pricelist.status,
                itg_mds_ph_pos_pricelist.item_cd
        ) epp2 ON (
            (
                upper(trim((epp2.item_cd)::text)) = ltrim(
                    (veposf.sap_item_cd)::text,
                    ('0'::character varying)::text
                )
            )
        )
    ));
