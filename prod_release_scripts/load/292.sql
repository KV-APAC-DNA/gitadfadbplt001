ALTER TABLE meta_raw.PARAMETERS
ALTER COLUMN PARAMETER_VALUE SET DATA TYPE VARCHAR(5000);

INSERT INTO META_RAW.PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (6406,515,'J_Pac_Px_Master_Edw','ms_query','SELECT ac_Code,
       ac_longname,
       ac_attribute,
       p_promonumber,
       p_startdate,
       p_stopdate,
       DATEDIFF(wk,p_startdate,p_stopdate) AS datediff1,
       p_buystartdatedef,
       p_buystopdatedef,
       DATEDIFF(wk,p_buystartdatedef,p_buystopdatedef) AS datediff2,
       p.ph_rowid,
       p.ph_longname,
       pt_longname,
       p_confirmed,
       pisd_closed,
       sku_longname,
       sku_stockcode,
       sku_profitcentre,
       sku_attribute,
       gltt_rowid,
       gltt_longname,
       pisd_rate,
       pisd_quantity,
       pisd_amount,
       pisd_paidamount,
       p_deleted,
       gltt_attribute,
       p_rowid AS promotionrowid,
       NormalQTY as normal_qty
FROM account
  INNER JOIN promotion ON p_accountrowid = ac_rowid
  INNER JOIN activity ON p_activityrowid = pt_rowid
  INNER JOIN promoitemsku ON pis_promotionrowid = p_rowid
  INNER JOIN sku ON pis_skurowid = sku_rowid
  INNER JOIN producthierarchy c ON c.ph_skurowid = sku_rowid
  INNER JOIN producthierarchy p
          ON p.ph_visitleft > c.ph_visitleft
         AND p.ph_visitright < c.ph_visitright
  INNER JOIN promoitemskudetail ON pisd_promoitemskurowid = pis_rowid
  INNER JOIN gltype ON gltt_rowid = pisd_typerowid
  LEFT OUTER JOIN (SELECT distinct est_accountrowid,
                          est_skurowid,
                          gltt_rowid as est_gltt_rowid,
                          p_rowid AS est_p_rowid,
                          CONVERT (INT,ROUND(SUM((est_normal / CONVERT (FLOAT(2),7))*(DATEDIFF(d,IIF (p_buystartdatedef >= est_Date,p_buystartdatedef,Est_Date),IIF (p_buystopdatedef >= DATEADD(d,6,est_date),DATEADD(d,6,est_date),p_buystopdatedef)) +1)),0)) AS NormalQTY
                   FROM promotion
                     INNER JOIN promoitemsku ON pis_promotionrowid = p_rowid
                     INNER JOIN promoitemskudetail ON pisd_promoitemskurowid = pis_rowid
                     INNER JOIN estimate
                             ON est_accountrowid = p_accountrowid
                            AND est_skurowid = pis_skurowid
INNER JOIN gltype on gltt_rowid = pisd_typerowid
                   WHERE (est_date BETWEEN p_buystartdatedef AND p_buystopdatedef OR DATEADD(d,6,est_date) BETWEEN p_buystartdatedef AND p_buystopdatedef)
                   GROUP BY est_accountrowid,
                            est_skurowid,
                            p_rowid,
                            gltt_rowid) EST
               ON est_accountrowid = ac_rowid
              AND est_skurowid = sku_rowid
              AND est_p_rowid = p_rowid and gltt_rowid=est.est_gltt_rowid
WHERE c.ph_producthierarchyrowid = p.ph_rowid
AND   p_deleted = 0
AND   pis_deleted = 0
AND   pisd_deleted = 0
AND (CONVERT(DATETIME,p_startdate) >= CONVERT(DATETIME,DATEADD(MONTH, -42, getdate())));',FALSE,TRUE);
INSERT INTO META_RAW.PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (6407,515,'J_Pac_Px_Master_Edw','truncate_and_load','Y',FALSE,TRUE);
INSERT INTO META_RAW.PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (6408,515,'J_Pac_Px_Master_Edw','decide_source','sql_server_awswfqsgpw',FALSE,TRUE);
INSERT INTO META_RAW.PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (6409,516,'J_Pac_Px_Scan_Fact_Edw','container','pac',FALSE,TRUE);
INSERT INTO META_RAW.PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (6410,516,'J_Pac_Px_Scan_Fact_Edw','landing_file_path','sql_server/promax/px_scan/',FALSE,TRUE);
INSERT INTO META_RAW.PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (6411,516,'J_Pac_Px_Scan_Fact_Edw','landing_file_name','px_scan_file',FALSE,TRUE);
INSERT INTO META_RAW.PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (6412,516,'J_Pac_Px_Scan_Fact_Edw','target_table','SDL_PX_SCAN_DATA',FALSE,TRUE);
INSERT INTO META_RAW.PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (6413,516,'J_Pac_Px_Scan_Fact_Edw','target_schema','PCFSDL_RAW',FALSE,TRUE);
INSERT INTO META_RAW.PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (6414,516,'J_Pac_Px_Scan_Fact_Edw','ms_query','SELECT account.ac_shortname,
       account.ac_longname,
       account.ac_code,
       account.ac_attribute,
       scan.sc_date,
       scan.sc_scanvolume,
       scan.sc_scanvalue,
       scan.sc_scanprice,
       sku.sku_shortname,
       sku.sku_longname,
       sku.sku_tuncode,
       sku.sku_apncode,
       sku.sku_stockcode
FROM account account,
     scan scan,
     sku sku
WHERE scan.sc_skurowid = sku.sku_rowid
AND   account.ac_rowid = scan.sc_accountrowid
AND   sc_date >=DATEADD(MONTH, -18, getdate()) ;',FALSE,TRUE);
INSERT INTO META_RAW.PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (6415,516,'J_Pac_Px_Scan_Fact_Edw','truncate_and_load','Y',FALSE,TRUE);
INSERT INTO META_RAW.PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (6416,516,'J_Pac_Px_Scan_Fact_Edw','decide_source','sql_server_awswfqsgpw',FALSE,TRUE);
INSERT INTO META_RAW.PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (6417,517,'J_Pac_Px_Term_Plan_Edw','container','pac',FALSE,TRUE);
INSERT INTO META_RAW.PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (6418,517,'J_Pac_Px_Term_Plan_Edw','landing_file_path','sql_server/promax/px_term/',FALSE,TRUE);
INSERT INTO META_RAW.PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (6419,517,'J_Pac_Px_Term_Plan_Edw','landing_file_name','px_term_file',FALSE,TRUE);
INSERT INTO META_RAW.PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (6420,517,'J_Pac_Px_Term_Plan_Edw','target_table','SDL_PX_TERM_PLAN',FALSE,TRUE);
INSERT INTO META_RAW.PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (6421,517,'J_Pac_Px_Term_Plan_Edw','target_schema','PCFSDL_RAW',FALSE,TRUE);
INSERT INTO META_RAW.PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (6422,517,'J_Pac_Px_Term_Plan_Edw','ms_query','SELECT
AC_CODE,
AC_ATTRIBUTE,
AC_LONGNAME,
SKU_STOCKCODE,
SKU_ATTRIBUTE,
SKU_LONGNAME,
GLTT_LONGNAME,
BD_SHORTNAME,
BD_LONGNAME,
ASESS_ROWID AS ASPS_ROWID,
''0'' AS COL11,
ASESS_TYPE AS ASPS_TYPE,
SUM(ASESS_WEEK1) AS ASPS_MONTH1,
SUM(ASESS_WEEK2) AS ASPS_MONTH2,
SUM(ASESS_WEEK3) AS ASPS_MONTH3,
SUM(ASESS_WEEK4) AS ASPS_MONTH4,
SUM(ASESS_WEEK5) AS ASPS_MONTH5,
SUM(ASESS_WEEK6) AS ASPS_MONTH6,
SUM(ASESS_WEEK7) AS ASPS_MONTH7,
SUM(ASESS_WEEK8) AS ASPS_MONTH8,
SUM(ASESS_WEEK9) AS ASPS_MONTH9,
SUM(ASESS_WEEK10) AS ASPS_MONTH10,
SUM(ASESS_WEEK11) AS ASPS_MONTH11,
SUM(ASESS_WEEK12) AS ASPS_MONTH12,
GLTT_ROWID AS GLTT_ROWID
FROM ACCOUNTSKU_ESTIMATESPENDSUMMARY
INNER JOIN ACCOUNTSKUSUMMARY ON ASESS_ACCOUNTSKUSUMMARYROWID=ASS_ROWID
INNER JOIN GLTYPE ON ASESS_GLTYPEROWID=GLTT_ROWID
INNER JOIN SKU ON ASS_SKUROWID=SKU_ROWID
INNER JOIN ACCOUNT ON ASS_ACCOUNTROWID=AC_ROWID
INNER JOIN BUDGET ON ASS_BUDGETROWID=BD_ROWID
AND ASESS_TYPE=10   -- CONFIRMED SPEND
AND ASESS_DATETYPE=1
AND GLTT_TYPEID >= 20000 AND GLTT_TYPEID <= 21999
GROUP BY AC_CODE,
AC_ATTRIBUTE,
AC_LONGNAME,
SKU_STOCKCODE,
SKU_ATTRIBUTE,
SKU_LONGNAME,
GLTT_LONGNAME,
BD_SHORTNAME,
BD_LONGNAME,
ASESS_ROWID,
ASESS_TYPE,
GLTT_ROWID;',FALSE,TRUE);
INSERT INTO META_RAW.PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (6423,517,'J_Pac_Px_Term_Plan_Edw','truncate_and_load','Y',FALSE,TRUE);
INSERT INTO META_RAW.PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (6424,517,'J_Pac_Px_Term_Plan_Edw','decide_source','sql_server_awswfqsgpw',FALSE,TRUE);
INSERT INTO META_RAW.PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (6425,518,'J_Pac_Px_UOM_Edw','container','pac',FALSE,TRUE);
INSERT INTO META_RAW.PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (6426,518,'J_Pac_Px_UOM_Edw','landing_file_path','sql_server/promax/px_uom/',FALSE,TRUE);
INSERT INTO META_RAW.PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (6427,518,'J_Pac_Px_UOM_Edw','landing_file_name','px_uom_file',FALSE,TRUE);
INSERT INTO META_RAW.PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (6428,518,'J_Pac_Px_UOM_Edw','target_table','SDL_PX_UOM',FALSE,TRUE);
INSERT INTO META_RAW.PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (6429,518,'J_Pac_Px_UOM_Edw','target_schema','PCFSDL_RAW',FALSE,TRUE);
INSERT INTO META_RAW.PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (6430,518,'J_Pac_Px_UOM_Edw','ms_query','SELECT  sku_longname, sku_stockcode, sku_uom, sku_uompersaleable, sku_packspercase
 FROM sku',FALSE,TRUE);
INSERT INTO META_RAW.PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (6431,518,'J_Pac_Px_UOM_Edw','truncate_and_load','Y',FALSE,TRUE);
INSERT INTO META_RAW.PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (6432,518,'J_Pac_Px_UOM_Edw','decide_source','sql_server_awswfqsgpw',FALSE,TRUE);
INSERT INTO META_RAW.PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (6433,519,'J_Pac_PX_Master_weekly_sellin','container','pac',FALSE,TRUE);
INSERT INTO META_RAW.PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (6434,519,'J_Pac_PX_Master_weekly_sellin','landing_file_path','sql_server/promax/px_master_weekly/',FALSE,TRUE);
INSERT INTO META_RAW.PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (6435,519,'J_Pac_PX_Master_weekly_sellin','landing_file_name','px_master_weekly_file',FALSE,TRUE);
INSERT INTO META_RAW.PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (6436,519,'J_Pac_PX_Master_weekly_sellin','target_table','sdl_px_weekly_sell',FALSE,TRUE);
INSERT INTO META_RAW.PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (6437,519,'J_Pac_PX_Master_weekly_sellin','target_schema','PCFSDL_RAW',FALSE,TRUE);
INSERT INTO META_RAW.PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (6438,519,'J_Pac_PX_Master_weekly_sellin','ms_query','select  ac_code,ac_attribute,ac_longname,sku_stockcode,sku_attribute,sku_longname, gltt_longname, bd_shortname, bd_longname,                                                                  
accountsku_actualspendsummary.asas_rowid, \'0\' as COL11, accountsku_actualspendsummary.asas_type,                                                                           
accountsku_actualspendsummary.asas_month1, accountsku_actualspendsummary.asas_month2, accountsku_actualspendsummary.asas_month3,                                                                             
accountsku_actualspendsummary.asas_month4, accountsku_actualspendsummary.asas_month5, accountsku_actualspendsummary.asas_month6,                                                                             
accountsku_actualspendsummary.asas_month7, accountsku_actualspendsummary.asas_month8, accountsku_actualspendsummary.asas_month9,                                                                             
accountsku_actualspendsummary.asas_month10, accountsku_actualspendsummary.asas_month11, accountsku_actualspendsummary.asas_month12, GLTT_ROWID                                                                            
FROM gltype,account,sku,accountsku_actualspendsummary,accountskusummary,budget                                                                              
where ac_rowid=ass_accountrowid                                                                        
and sku_rowid=ass_skurowid                                                                    
and asas_gltyperowid=gltt_rowid                                                                            
and gltt_typeid >= 20000 and gltt_typeid <= 21999                                                                           
and asas_accountskusummaryrowid=ass_rowid                                                                               
and ass_budgetrowid=budget.bd_rowid                                                                              
and asas_type=0;',FALSE,TRUE);
INSERT INTO META_RAW.PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (6439,519,'J_Pac_PX_Master_weekly_sellin','truncate_and_load','Y',FALSE,TRUE);
INSERT INTO META_RAW.PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (6440,519,'J_Pac_PX_Master_weekly_sellin','decide_source','sql_server_awswfqsgpw',FALSE,TRUE);
INSERT INTO META_RAW.PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (6441,433,'pa_pharma_inv_chs_group','startRange','A1',FALSE,TRUE);
INSERT INTO META_RAW.PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (6442,441,'pa_pharma_inv_symbion_group','startRange','A1',FALSE,TRUE);
INSERT INTO META_RAW.PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (6443,443,'pa_pharma_inv_sigma_group','startRange','A1',FALSE,TRUE);
INSERT INTO META_RAW.PARAMETERS (PARAMETER_ID, PARAMETER_GROUP_ID,PARAMETER_GROUP_NAME,PARAMETER_NAME,PARAMETER_VALUE,IS_SENSITIVE,IS_ACTIVE) VALUES (6444,444,'pa_pharma_inv_api_group','startRange','A1',FALSE,TRUE);
