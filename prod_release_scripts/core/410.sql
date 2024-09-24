create table pcfedw_integration.edw_iri_scan_sales_agg_test as select * from pcfedw_integration.edw_iri_scan_sales_agg;

truncate table pcfedw_integration.edw_iri_scan_sales_agg_test;

insert into pcfedw_integration.edw_iri_scan_sales_agg_test
(SELECT iri.jj_year,
       iri.jj_mnth_id,
       etd.max_cal_date as wk_end_dt,
       iri.matl_id,
       iri.matl_desc,
       iri.iri_ean,
       iri.iri_market,
       iri.representative_cust_nm,
       iri.representative_cust_cd,
       iri.sales_grp_cd,
       iri.sales_grp_nm,
       iri.scan_sales + 
       LAG(iri.scan_sales,1) OVER (PARTITION BY iri.matl_id,iri.representative_cust_cd ORDER BY iri.matl_id,iri.jj_mnth_id,iri.representative_cust_cd) + 
       LAG(iri.scan_sales,2) OVER (PARTITION BY iri.matl_id,iri.representative_cust_cd ORDER BY iri.matl_id,iri.jj_mnth_id,iri.representative_cust_cd) AS scan_sales
FROM (SELECT jj_year,
             jj_mnth_id,
             MAX(wk_end_dt) wk_end_dt,
             matl_id,
             matl_desc,
             iri_ean,
             iri_market,
             representative_cust_nm,
             representative_cust_cd,
             sales_grp_cd,
             sales_grp_nm,
             SUM(scan_sales)   scan_sales
      FROM pcfedw_integration.vw_iri_scan_sales_analysis
      WHERE UPPER(iri_market) IN ('AU WOOLWORTHS SCAN','AU COLES GROUP SCAN','AU MY CHEMIST GROUP SCAN')
      GROUP BY jj_year,
               jj_mnth_id,
               matl_id,
               matl_desc,
               iri_ean,
               iri_market,
               representative_cust_nm,
               representative_cust_cd,
               sales_grp_cd,
               sales_grp_nm
      order by jj_year,
               jj_mnth_id,
               matl_id,
               matl_desc,
               iri_ean,
               iri_market,
               representative_cust_nm,
               representative_cust_cd,
               sales_grp_cd,
               sales_grp_nm
    ) iri left join (select jj_mnth_id, cal_date, max(cal_date) over (partition by jj_mnth_id) as max_cal_date from pcfedw_integration.edw_time_dim group by 1,2) etd on
     (iri.wk_end_dt::timestamp without time zone) = (etd.cal_date::date));
