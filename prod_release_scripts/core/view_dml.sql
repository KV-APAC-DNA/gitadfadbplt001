create or replace view aspedw_integration.edw_vw_greenlight_skus
as
(
  with edw_material_dim as (
      select * from aspedw_integration.edw_material_dim
  ),
  edw_sales_org_dim as (
      select * from aspedw_integration.edw_sales_org_dim
  ),
  edw_company_dim as (
      select * from aspedw_integration.edw_company_dim
  ),
  itg_mds_ap_greenlight_skus as (
      select * from aspedw_integration.itg_mds_ap_greenlight_skus
  ),
  edw_material_sales_dim as (
      select * from aspedw_integration.edw_material_sales_dim
  ),
  final as(
  select
    ms.sls_org,
    ms.dstr_chnl,
    ltrim(ms.matl_num, cast((cast((0) as varchar)) as text)) as matl_num,
    ms.base_unit,
    ms.matl_grp_1,
    ms.prod_hierarchy,
    ms.matl_grp_2,
    ms.matl_grp_3,
    ms.matl_grp_4,
    ms.matl_grp_5,
    ms.ean_num,
    ms.delv_plnt,
    ms.itm_cat_grp,
    ms.lcl_matl_grp_1,
    ms.lcl_matl_grp_2,
    ms.lcl_matl_grp_3,
    ms.lcl_matl_grp_4,
    ms.lcl_matl_grp_5,
    ms.lcl_matl_grp_6,
    ms.mstr_cd,
    ms.med_desc,
    coalesce(m.base_uom_cd, cast('N/A' as varchar)) as sap_base_uom_cd,
    coalesce(m.prch_uom_cd, cast('N/A' as varchar)) as sap_prchse_uom_cd,
    coalesce(m.matl_desc, cast('N/A' as varchar)) as matl_desc,
    coalesce(m.matl_grp_cd, cast('N/A' as varchar)) as matl_grp_cd,
    coalesce(m.prod_base, cast('N/A' as varchar)) as sap_base_prod_cd,
    coalesce(m.base_prod_desc, cast('N/A' as varchar)) as base_prod_desc,
    coalesce(m.mega_brnd_cd, cast('N/A' as varchar)) as sap_mega_brnd_cd,
    coalesce(m.mega_brnd_desc, cast('N/A' as varchar)) as mega_brnd_desc,
    coalesce(m.brnd_cd, cast('N/A' as varchar)) as sap_brnd_cd,
    coalesce(m.brnd_desc, cast('N/A' as varchar)) as brnd_desc,
    coalesce(m.vrnt, cast('N/A' as varchar)) as sap_vrnt_cd,
    coalesce(m.varnt_desc, cast('N/A' as varchar)) as varnt_desc,
    coalesce(m.put_up, cast('N/A' as varchar)) as sap_put_up_cd,
    coalesce(m.put_up_desc, cast('N/A' as varchar)) as put_up_desc,
    coalesce(m.prod_hier_cd, cast('N/A' as varchar)) as prod_hier_cd,
    coalesce(m.prodh1, cast('N/A' as varchar)) as prodh1,
    coalesce(m.prodh1_txtmd, cast('N/A' as varchar)) as prodh1_txtmd,
    coalesce(m.prodh2, cast('N/A' as varchar)) as prodh2,
    coalesce(m.prodh2_txtmd, cast('N/A' as varchar)) as prodh2_txtmd,
    coalesce(m.prodh3, cast('N/A' as varchar)) as prodh3,
    coalesce(m.prodh3_txtmd, cast('N/A' as varchar)) as prodh3_txtmd,
    coalesce(m.prodh4, cast('N/A' as varchar)) as prodh4,
    coalesce(m.prodh4_txtmd, cast('N/A' as varchar)) as prodh4_txtmd,
    coalesce(m.prodh5, cast('N/A' as varchar)) as prodh5,
    coalesce(m.prodh5_txtmd, cast('N/A' as varchar)) as prodh5_txtmd,
    coalesce(m.prodh6, cast('N/A' as varchar)) as prodh6,
    coalesce(m.prodh6_txtmd, cast('N/A' as varchar)) as prodh6_txtmd,
    coalesce(m.matl_type_cd, cast('N/A' as varchar)) as matl_type_cd,
    coalesce(m.matl_type_desc, cast('N/A' as varchar)) as matl_type_desc,
    coalesce(m.pka_franchise_cd, cast('N/A' as varchar)) as pka_franchise_cd,
    coalesce(m.pka_franchise_desc, cast('N/A' as varchar)) as pka_franchise_desc,
    coalesce(m.pka_brand_cd, cast('N/A' as varchar)) as pka_brand_cd,
    coalesce(m.pka_brand_desc, cast('N/A' as varchar)) as pka_brand_desc,
    coalesce(m.pka_sub_brand_cd, cast('N/A' as varchar)) as pka_sub_brand_cd,
    coalesce(m.pka_sub_brand_desc, cast('N/A' as varchar)) as pka_sub_brand_desc,
    coalesce(m.pka_variant_cd, cast('N/A' as varchar)) as pka_variant_cd,
    coalesce(m.pka_variant_desc, cast('N/A' as varchar)) as pka_variant_desc,
    coalesce(m.pka_sub_variant_cd, cast('N/A' as varchar)) as pka_sub_variant_cd,
    coalesce(m.pka_sub_variant_desc, cast('N/A' as varchar)) as pka_sub_variant_desc,
    coalesce(m.pka_flavor_cd, cast('N/A' as varchar)) as pka_flavor_cd,
    coalesce(m.pka_flavor_desc, cast('N/A' as varchar)) as pka_flavor_desc,
    coalesce(m.pka_ingredient_cd, cast('N/A' as varchar)) as pka_ingredient_cd,
    coalesce(m.pka_ingredient_desc, cast('N/A' as varchar)) as pka_ingredient_desc,
    coalesce(m.pka_application_cd, cast('N/A' as varchar)) as pka_application_cd,
    coalesce(m.pka_application_desc, cast('N/A' as varchar)) as pka_application_desc,
    coalesce(m.pka_length_cd, cast('N/A' as varchar)) as pka_length_cd,
    coalesce(m.pka_length_desc, cast('N/A' as varchar)) as pka_length_desc,
    coalesce(m.pka_shape_cd, cast('N/A' as varchar)) as pka_shape_cd,
    coalesce(m.pka_shape_desc, cast('N/A' as varchar)) as pka_shape_desc,
    coalesce(m.pka_spf_cd, cast('N/A' as varchar)) as pka_spf_cd,
    coalesce(m.pka_spf_desc, cast('N/A' as varchar)) as pka_spf_desc,
    coalesce(m.pka_cover_cd, cast('N/A' as varchar)) as pka_cover_cd,
    coalesce(m.pka_cover_desc, cast('N/A' as varchar)) as pka_cover_desc,
    coalesce(m.pka_form_cd, cast('N/A' as varchar)) as pka_form_cd,
    coalesce(m.pka_form_desc, cast('N/A' as varchar)) as pka_form_desc,
    coalesce(m.pka_size_cd, cast('N/A' as varchar)) as pka_size_cd,
    coalesce(m.pka_size_desc, cast('N/A' as varchar)) as pka_size_desc,
    coalesce(m.pka_character_cd, cast('N/A' as varchar)) as pka_character_cd,
    coalesce(m.pka_character_desc, cast('N/A' as varchar)) as pka_character_desc,
    coalesce(m.pka_package_cd, cast('N/A' as varchar)) as pka_package_cd,
    coalesce(m.pka_package_desc, cast('N/A' as varchar)) as pka_package_desc,
    coalesce(m.pka_attribute_13_cd, cast('N/A' as varchar)) as pka_attribute_13_cd,
    coalesce(m.pka_attribute_13_desc, cast('N/A' as varchar)) as pka_attribute_13_desc,
    coalesce(m.pka_attribute_14_cd, cast('N/A' as varchar)) as pka_attribute_14_cd,
    coalesce(m.pka_attribute_14_desc, cast('N/A' as varchar)) as pka_attribute_14_desc,
    coalesce(m.pka_sku_identification_cd, cast('N/A' as varchar)) as pka_sku_identification_cd,
    coalesce(m.pka_sku_identification_desc, cast('N/A' as varchar)) as pka_sku_identification_desc,
    coalesce(m.pka_one_time_relabeling_cd, cast('N/A' as varchar)) as pka_one_time_relabeling_cd,
    coalesce(m.pka_one_time_relabeling_desc, cast('N/A' as varchar)) as pka_one_time_relabeling_desc,
    coalesce(m.pka_product_key, cast('N/A' as varchar)) as pka_product_key,
    coalesce(m.pka_product_key_description, cast('N/A' as varchar)) as pka_product_key_description,
    coalesce(m.pka_product_key_description_2, cast('N/A' as varchar)) as pka_product_key_description_2,
    coalesce(m.pka_root_code, cast('N/A' as varchar)) as pka_root_code,
    coalesce(m.pka_root_code_desc_1, cast('N/A' as varchar)) as pka_root_code_desc_1,
    coalesce(s.sls_org_nm, cast('N/A' as varchar)) as sls_org_nm,
    coalesce(s.sls_org_co_cd, cast('N/A' as varchar)) as sls_org_co_cd,
    coalesce(s.ctry_key, cast('N/A' as varchar)) as ctry_key,
    coalesce(s.crncy_key, cast('N/A' as varchar)) as crncy_key,
    coalesce(c.co_cd, cast('N/A' as varchar)) as co_cd,
    coalesce(c.ctry_nm, cast('N/A' as varchar)) as ctry_nm,
    coalesce(c.ctry_group, cast('N/A' as varchar)) as ctry_group,
    coalesce(c."CLUSTER", cast('N/A' as varchar)) as "cluster",
    coalesce(g.market, cast('N/A' as varchar)) as market,
    coalesce(g.material_description, cast('N/A' as varchar)) as material_description,
    coalesce(g.product_key_description, cast('N/A' as varchar)) as product_key_description,
    coalesce(g.brand_name, cast('N/A' as varchar)) as brand_name,
    coalesce(g.package, cast('N/A' as varchar)) as package,
    coalesce(g.product_key, cast('N/A' as varchar)) as product_key,
    coalesce(g.greenlight_sku_flag, cast('N/A' as varchar)) as greenlight_sku_flag,
    coalesce(g.red_sku_flag, cast('N/A' as varchar)) as red_sku_flag,
    coalesce(g.root_code, cast('N/A' as varchar)) as root_code
  from (
    (
      (
        (
          (
            select distinct
              edw_material_sales_dim.sls_org,
              edw_material_sales_dim.dstr_chnl,
              ltrim(
                cast((
                  edw_material_sales_dim.matl_num
                ) as text),
                cast((
                  cast((
                    0
                  ) as varchar)
                ) as text)
              ) as matl_num,
              edw_material_sales_dim.base_unit,
              edw_material_sales_dim.matl_grp_1,
              edw_material_sales_dim.prod_hierarchy,
              edw_material_sales_dim.matl_grp_2,
              edw_material_sales_dim.matl_grp_3,
              edw_material_sales_dim.matl_grp_4,
              edw_material_sales_dim.matl_grp_5,
              edw_material_sales_dim.ean_num,
              edw_material_sales_dim.delv_plnt,
              edw_material_sales_dim.itm_cat_grp,
              edw_material_sales_dim.lcl_matl_grp_1,
              edw_material_sales_dim.lcl_matl_grp_2,
              edw_material_sales_dim.lcl_matl_grp_3,
              edw_material_sales_dim.lcl_matl_grp_4,
              edw_material_sales_dim.lcl_matl_grp_5,
              edw_material_sales_dim.lcl_matl_grp_6,
              edw_material_sales_dim.mstr_cd,
              edw_material_sales_dim.med_desc
            from edw_material_sales_dim
            where
              (
                trim(cast((
                  edw_material_sales_dim.matl_num
                ) as text)) <> cast((
                  cast('' as varchar)
                ) as text)
              )
          ) as ms
          left join edw_material_dim as m
            on (
              (
                ltrim(cast((
                  m.matl_num
                ) as text), cast((
                  cast((
                    0
                  ) as varchar)
                ) as text)) = ltrim(ms.matl_num, cast((
                  cast((
                    0
                  ) as varchar)
                ) as text))
              )
            )
        )
        left join edw_sales_org_dim as s
          on (
            (
              cast((
                s.sls_org
              ) as text) = cast((
                ms.sls_org
              ) as text)
            )
          )
      )
      left join edw_company_dim as c
        on (
          (
            cast((
              c.co_cd
            ) as text) = cast((
              s.sls_org_co_cd
            ) as text)
          )
        )
    )
    left join itg_mds_ap_greenlight_skus as g
      on (
        (
          (
            case
              when (
                (
                  upper(cast((
                    g.market
                  ) as text)) = cast((
                    cast('CHINA OTC'  as varchar)
                  ) as text)
                )
                or (
                  (
                    upper(cast((
                      g.market
                    ) as text)) is null
                  ) and (
                    'CHINA OTC' is null
                  )
                )
              )
              then cast((
                cast('OTC' as varchar)
              ) as text)
              when (
                (
                  upper(cast((
                    g.market
                  ) as text)) = cast((
                    cast('CHINA SIKNCARE' as varchar)
                  ) as text)
                )
                or (
                  (
                    upper(cast((
                      g.market
                    ) as text)) is null
                  )
                  and (
                    'CHINA SIKNCARE' is null
                  )
                )
              )
              then cast((
                cast('CHINA' as varchar)
              ) as text)
              else upper(cast((
                g.market
              ) as text))
            end = case
              when (
                (
                  upper(cast((
                    c.ctry_group
                  ) as text)) = cast((
                    cast('AUSTRALIA' as varchar)
                  ) as text)
                )
                or (
                  (
                    upper(cast((
                      c.ctry_group
                    ) as text)) is null
                  )
                  and (
                    'AUSTRALIA' is null
                  )
                )
              )
              then cast((
                cast('PACIFIC' as varchar)
              ) as text)
              when (
                (
                  upper(cast((
                    c.ctry_group
                  ) as text)) = cast((
                    cast('NEW ZEALAND' as varchar)
                  ) as text)
                )
                or (
                  (
                    upper(cast((
                      c.ctry_group
                    ) as text)) is null
                  )
                  and (
                    'NEW ZEALAND' is null
                  )
                )
              )
              then cast((
                cast('PACIFIC' as varchar)
              ) as text)
              else upper(cast((
                c.ctry_group
              ) as text))
            end
          )
          and (
            ltrim(
              cast((
                g.material_number
              ) as text),
              cast((
                cast((
                  0
                ) as varchar)
              ) as text)
            ) = ltrim(ms.matl_num, cast((
              cast((
                0
              ) as varchar)
            ) as text))
          )
        )
      )
  )
  )
  select * from final
);
------------------------------------------------------
create or replace view aspedw_integration.v_intrm_reg_crncy_exch_fiscper
as
(
  --Import CTE
  with edw_crncy_exch as (
      select * from aspedw_integration.edw_crncy_exch
  ),

  edw_calendar_dim as (
    select * from aspedw_integration.edw_calendar_dim
  ),
  --Logical CTE

  -- Final CTE
  final as (
  (
    select derived_table1.ex_rt_typ
    ,derived_table1.from_crncy
    ,derived_table1.to_crncy
    ,derived_table1.vld_from
    ,derived_table1.ex_rt
    ,derived_table1.fisc_per from (
    select a.ex_rt_typ
      ,a.from_crncy
      ,a.to_crncy
      ,calmonthstartdate.vld_from
      ,a.ex_rt
      ,calmonthstartdate.fisc_per
      ,rank() over (
        partition by a.from_crncy
        ,a.to_crncy
        ,calmonthstartdate.fisc_per order by calmonthstartdate.vld_from desc
        ) as latest_ex_rt_by_fisc_per
    FROM 
      (
        select drvd_crncy.ex_rt_typ
          ,drvd_crncy.from_crncy
          ,drvd_crncy.to_crncy
          ,cal.fisc_yr as "year"
          ,max(cast((
                case 
                  when (
                      (cast((drvd_crncy.to_crncy) as text) = cast((cast('USD' as varchar)) as text))
                      and (
                        (
                          (cast((drvd_crncy.from_crncy) as text) = cast((cast('IDR' as varchar)) as text))
                          or (cast((drvd_crncy.from_crncy) as text) = cast((cast('KRW' as varchar)) as text))
                          )
                        or (cast((drvd_crncy.from_crncy) as text) = cast((cast('VND' as varchar)) as text))
                        )
                      )
                    then (drvd_crncy.ex_rt / cast((cast((1000) as decimal)) as decimal(18, 0)))
                  when (
                      (cast((drvd_crncy.to_crncy) as text) = cast((cast('USD' as varchar)) as text))
                      and (cast((drvd_crncy.from_crncy) as text) = cast((cast('JPY' as varchar)) as text))
                      )
                    then (drvd_crncy.ex_rt / cast((cast((100) as decimal)) as decimal(18, 0)))
                  else drvd_crncy.ex_rt
                  end
                ) as decimal(20, 10))) as ex_rt
        from (
          (
            select distinct edw_crncy_exch.ex_rt_typ
              ,edw_crncy_exch.from_crncy
              ,edw_crncy_exch.to_crncy
              ,to_date(cast((cast(((cast((cast((99999999) as decimal)) as decimal(18, 0)) - cast((cast((edw_crncy_exch.vld_from) as decimal)) as decimal(18, 0)))) as varchar)) as text), cast((cast('YYYYMMDD' as varchar)) as text)) as vld_from
              ,edw_crncy_exch.ex_rt
              ,edw_crncy_exch.from_ratio
              ,edw_crncy_exch.to_ratio
            from edw_crncy_exch as edw_crncy_exch
            where (
                (
                  (
                    (cast((edw_crncy_exch.ex_rt_typ) as text) = cast((cast('BWAR' as varchar)) as text))
                    and (cast((edw_crncy_exch.from_crncy) as text) <> cast((cast('LKR' as varchar)) as text))
                    )
                  and (cast((edw_crncy_exch.from_crncy) as text) <> cast((cast('BDT' as varchar)) as text))
                  )
                and (cast((edw_crncy_exch.from_crncy) as text) <> cast((cast('NZD' as varchar)) as text))
                )
            ) as drvd_crncy join edw_calendar_dim as cal
            on (
                (
                  (drvd_crncy.vld_from = cal.cal_day)
                  and (cast((cal.fisc_yr_vrnt) as text) = cast((cast('J1' as varchar)) as text))
                  )
                )
          )
        group by drvd_crncy.ex_rt_typ
          ,drvd_crncy.from_crncy
          ,drvd_crncy.to_crncy
          ,substring(cast((cast((drvd_crncy.vld_from) as varchar)) as text), 1, 4)
          ,cal.fisc_yr
        ) as a join (
        select edw_calendar_dim.fisc_yr as "year"
          ,edw_calendar_dim.fisc_per
          ,min(edw_calendar_dim.cal_day) as vld_from
        from edw_calendar_dim
        where (cast((edw_calendar_dim.fisc_yr_vrnt) as text) = cast((cast('J1' as varchar)) as text))
        group by edw_calendar_dim.fisc_yr
          ,edw_calendar_dim.fisc_per
        ) as calmonthstartdate
        on (
            (
              (a."year" = calmonthstartdate."year")
              and (calmonthstartdate.vld_from <= to_variant(current_date ())::varchar)
              )
            )
      )
    as derived_table1 where (derived_table1.latest_ex_rt_by_fisc_per = 1)

  union all
    
    select distinct edw_crncy_exch.ex_rt_typ
    ,edw_crncy_exch.from_crncy
    ,edw_crncy_exch.from_crncy as to_crncy
    ,calmonthstartdate.vld_from
    ,1 as ex_rt
    ,calmonthstartdate.fisc_per from (
    edw_crncy_exch as edw_crncy_exch join (
      select edw_calendar_dim.fisc_yr as "year"
        ,edw_calendar_dim.fisc_per
        ,min(edw_calendar_dim.cal_day) as vld_from
      from edw_calendar_dim
      where (
          (cast((edw_calendar_dim.fisc_yr_vrnt) as text) = cast((cast('J1' as varchar)) as text))
          and (edw_calendar_dim.cal_day <= to_variant(current_date ())::varchar)
          )
      group by edw_calendar_dim.fisc_yr
        ,edw_calendar_dim.fisc_per
      ) as calmonthstartdate
      on ((1 = 1))
    ) where (cast((edw_crncy_exch.ex_rt_typ) as text) = cast((cast('BWAR' as varchar)) as text))
    )

  union all

  select drvd_crncy.ex_rt_typ
    ,drvd_crncy.from_crncy
    ,drvd_crncy.to_crncy
    ,drvd_crncy.vld_from
    ,cast((
        case 
          when (
              (cast((drvd_crncy.to_crncy) as text) = cast((cast('USD' as varchar)) as text))
              and (
                (
                  (cast((drvd_crncy.from_crncy) as text) = cast((cast('IDR' as varchar)) as text))
                  or (cast((drvd_crncy.from_crncy) as text) = cast((cast('KRW' as varchar)) as text))
                  )
                or (cast((drvd_crncy.from_crncy) as text) = cast((cast('VND' as varchar)) as text))
                )
              )
            then (drvd_crncy.ex_rt / cast((cast((1000) as decimal)) as decimal(18, 0)))
          when (
              (cast((drvd_crncy.to_crncy) as text) = cast((cast('USD' as varchar)) as text))
              and (cast((drvd_crncy.from_crncy) as text) = cast((cast('JPY' as varchar)) as text))
              )
            then (drvd_crncy.ex_rt / cast((cast((100) as decimal)) as decimal(18, 0)))
          else drvd_crncy.ex_rt
          end
        ) as decimal(20, 10)) as ex_rt
    ,drvd_crncy.fisc_per
  from (
    select distinct edw_crncy_exch.ex_rt_typ
      ,edw_crncy_exch.from_crncy
      ,edw_crncy_exch.to_crncy
      ,edw_crncy_exch.vld_from
      ,edw_crncy_exch.ex_rt
      ,edw_crncy_exch.fisc_per
      ,rank() over (
        partition by edw_crncy_exch.from_crncy
        ,edw_crncy_exch.to_crncy
        ,edw_crncy_exch.fisc_per order by edw_crncy_exch.vld_from
        ) as latest_ex_rt_by_fisc_per
    from (
      select a.ex_rt_typ
        ,a.from_crncy
        ,a.to_crncy
        ,a.ex_rt
        ,a.vld_from
        ,b.fisc_per
      from (
        (
          select a.ex_rt_typ
            ,a.from_crncy
            ,a.to_crncy
            ,a.ex_rt
            ,to_date(cast((cast(((cast((cast((99999999) as decimal)) as decimal(18, 0)) - cast((cast((a.vld_from) as decimal)) as decimal(18, 0)))) as varchar)) as text), cast((cast('YYYYMMDD' as varchar)) as text)) as vld_from
          from edw_crncy_exch as a
          where (
              (cast((a.ex_rt_typ) as text) = cast((cast('BWAR' as varchar)) as text))
              and (
                (
                  (cast((a.from_crncy) as text) = cast((cast('LKR' as varchar)) as text))
                  or (cast((a.from_crncy) as text) = cast((cast('BDT' as varchar)) as text))
                  )
                or (cast((a.from_crncy) as text) = cast((cast('NZD' as varchar)) as text))
                )
              )
          ) as a join edw_calendar_dim as b
          on ((a.vld_from = b.cal_day))
        )
      where (cast((b.fisc_yr_vrnt) as text) = cast((cast('J1' as varchar)) as text))
      
      union all
      
      select ex.ex_rt_typ
        ,ex.from_crncy
        ,ex.to_crncy
        ,ex.ex_rt
        ,ex.vld_from
        ,c.fisc_per
      from (
        (
          select a.ex_rt_typ
            ,a.from_crncy
            ,a.to_crncy
            ,a.ex_rt
            ,a.vld_from
            ,b.fisc_per
          from (
            (
              select a.ex_rt_typ
                ,a.from_crncy
                ,a.to_crncy
                ,a.ex_rt
                ,a.vld_from
              from (
                (
                  select edw_crncy_exch.ex_rt_typ
                    ,edw_crncy_exch.from_crncy
                    ,edw_crncy_exch.to_crncy
                    ,edw_crncy_exch.ex_rt
                    ,to_date(cast((cast(((cast((cast((99999999) as decimal)) as decimal(18, 0)) - cast((cast((edw_crncy_exch.vld_from) as decimal)) as decimal(18, 0)))) as varchar)) as text), cast((cast('YYYYMMDD' as varchar)) as text)) as vld_from
                  from edw_crncy_exch
                  where (
                      (cast((edw_crncy_exch.ex_rt_typ) as text) = cast((cast('BWAR' as varchar)) as text))
                      and (
                        (
                          (cast((edw_crncy_exch.from_crncy) as text) = cast((cast('LKR' as varchar)) as text))
                          or (cast((edw_crncy_exch.from_crncy) as text) = cast((cast('BDT' as varchar)) as text))
                          )
                        or (cast((edw_crncy_exch.from_crncy) as text) = cast((cast('NZD' as varchar)) as text))
                        )
                      )
                  ) as a join (
                  select edw_crncy_exch.from_crncy
                    ,edw_crncy_exch.to_crncy
                    ,max(to_date(cast((cast(((cast((cast((99999999) as decimal)) as decimal(18, 0)) - cast((cast((edw_crncy_exch.vld_from) as decimal)) as decimal(18, 0)))) as varchar)) as text), cast((cast('YYYYMMDD' as varchar)) as text))) as vld_from
                  from edw_crncy_exch
                  where (
                      (cast((edw_crncy_exch.ex_rt_typ) as text) = cast((cast('BWAR' as varchar)) as text))
                      and (
                        (
                          (cast((edw_crncy_exch.from_crncy) as text) = cast((cast('LKR' as varchar)) as text))
                          or (cast((edw_crncy_exch.from_crncy) as text) = cast((cast('BDT' as varchar)) as text))
                          )
                        or (cast((edw_crncy_exch.from_crncy) as text) = cast((cast('NZD' as varchar)) as text))
                        )
                      )
                  group by edw_crncy_exch.from_crncy
                    ,edw_crncy_exch.to_crncy
                  ) as b
                  on (
                      (
                        (
                          (cast((a.from_crncy) as text) = cast((b.from_crncy) as text))
                          and (cast((a.to_crncy) as text) = cast((b.to_crncy) as text))
                          )
                        and (a.vld_from = b.vld_from)
                        )
                      )
                )
              ) as a join edw_calendar_dim as b
              on ((a.vld_from = b.cal_day))
            )
          where (cast((b.fisc_yr_vrnt) as text) = cast((cast('J1' as varchar)) as text))
          ) as ex join (
          select distinct edw_calendar_dim.fisc_per
          from edw_calendar_dim
          where (
              (cast((edw_calendar_dim.fisc_yr_vrnt) as text) = cast((cast('J1' as varchar)) as text))
              and (edw_calendar_dim.cal_day <= to_variant(current_date ())::varchar)
              )
          ) as c
          on ((ex.fisc_per < c.fisc_per))
        )
      ) as edw_crncy_exch
    ) as drvd_crncy
  where (drvd_crncy.latest_ex_rt_by_fisc_per = 1)
  )


  --final select
  select * from final
);
---------------------------------------------
create or replace view aspedw_integration.v_edw_customer_sales_dim
as
(
  with edw_customer_sales_dim as(
      select * from aspedw_integration.edw_customer_sales_dim
  ),
  edw_code_descriptions as(
      select * from aspedw_integration.edw_code_descriptions    
  ),
  edw_code_descriptions_manual as(
      select * from aspedw_integration.edw_code_descriptions_manual    
  ),
  edw_subchnl_retail_env_mapping as(
      select * from aspedw_integration.edw_subchnl_retail_env_mapping
  ),
  transformed as(
  (
    SELECT
      cus_sales.clnt,
      cus_sales.cust_num,
      cus_sales.sls_org,
      cus_sales.dstr_chnl,
      cus_sales.div,
      cus_sales.obj_crt_prsn,
      cus_sales.rec_crt_dt,
      cus_sales.auth_grp,
      cus_sales.cust_del_flag,
      cus_sales.cust_stat_grp,
      cus_sales.cust_ord_blk,
      cus_sales.prc_pcdr_asgn,
      cus_sales.cust_grp,
      cus_sales.sls_dstrc,
      cus_sales.prc_grp,
      cus_sales.prc_list_typ,
      cus_sales.ord_prob_itm,
      cus_sales.incoterm1,
      cus_sales.incoterm2,
      cus_sales.cust_delv_blk,
      cus_sales.cmplt_delv_sls_ord,
      cus_sales.max_no_prtl_delv_allw_itm,
      cus_sales.prtl_delv_itm_lvl,
      cus_sales.ord_comb_in,
      cus_sales.btch_splt_allw,
      cus_sales.delv_prir,
      cus_sales.vend_acct_num,
      cus_sales.shipping_cond,
      cus_sales.bill_blk_cust,
      cus_sales.man_invc_maint,
      cus_sales.invc_dt,
      cus_sales.invc_list_sched,
      cus_sales.cost_est_in,
      cus_sales.val_lmt_cost_est,
      cus_sales.crncy_key,
      cus_sales.cust_clas,
      cus_sales.acct_asgnmt_grp,
      cus_sales.delv_plnt,
      cus_sales.sls_grp,
      cus_sales.sls_grp_desc,
      cus_sales.sls_ofc,
      cus_sales.sls_ofc_desc,
      cus_sales.itm_props,
      cus_sales.cust_grp1,
      cus_sales.cust_grp2,
      cus_sales.cust_grp3,
      cus_sales.cust_grp4,
      cus_sales.cust_grp5,
      cus_sales.cust_rebt_in,
      cus_sales.rebt_indx_cust_strt_prd,
      cus_sales.exch_rt_typ,
      cus_sales.prc_dtrmn_id,
      cus_sales.prod_attr_id1,
      cus_sales.prod_attr_id2,
      cus_sales.prod_attr_id3,
      cus_sales.prod_attr_id4,
      cus_sales.prod_attr_id5,
      cus_sales.prod_attr_id6,
      cus_sales.prod_attr_id7,
      cus_sales.prod_attr_id8,
      cus_sales.prod_attr_id9,
      cus_sales.prod_attr_id10,
      cus_sales.pymt_key_term,
      cus_sales.persnl_num,
      cus_sales.crt_dttm,
      cus_sales.updt_dttm,
      cus_sales.cur_sls_emp,
      cus_sales.lcl_cust_grp_1,
      cus_sales.lcl_cust_grp_2,
      cus_sales.lcl_cust_grp_3,
      cus_sales.lcl_cust_grp_4,
      cus_sales.lcl_cust_grp_5,
      cus_sales.lcl_cust_grp_6,
      cus_sales.lcl_cust_grp_7,
      cus_sales.lcl_cust_grp_8,
      cus_sales.prc_proc,
      cus_sales.par_del,
      cus_sales.max_num_pa,
      cus_sales.prnt_cust_key,
      cus_sales.bnr_key,
      cus_sales.bnr_frmt_key,
      cus_sales.go_to_mdl_key,
      cus_sales.chnl_key,
      cus_sales.sub_chnl_key,
      cus_sales.segmt_key,
      cus_sales.cust_set_1,
      cus_sales.cust_set_2,
      cus_sales.cust_set_3,
      cus_sales.cust_set_4,
      cus_sales.cust_set_5,
      cddes_pck.code_desc AS "parent customer",
      cddes_bnrkey.code_desc AS banner,
      cddes_bnrfmt.code_desc AS "banner format",
      cddes_chnl.code_desc AS channel,
      cddes_gtm.code_desc AS "go to model",
      cddes_subchnl.code_desc AS "sub channel",
      subchnl_retail_env.retail_env,
      ecdm.code_desc
    FROM (
      (
        (
          (
            (
              (
                (
                  (
                    edw_customer_sales_dim AS cus_sales
                      LEFT JOIN edw_code_descriptions AS cddes_pck
                        ON (
                          (
                            (
                              CAST((
                                cddes_pck.code_type
                              ) AS TEXT) = CAST((
                                CAST('Parent Customer Key' AS VARCHAR)
                              ) AS TEXT)
                            )
                            AND (
                              CAST((
                                cddes_pck.code
                              ) AS TEXT) = CAST((
                                cus_sales.prnt_cust_key
                              ) AS TEXT)
                            )
                          )
                        )
                  )
                  LEFT JOIN edw_code_descriptions AS cddes_bnrkey
                    ON (
                      (
                        (
                          CAST((
                            cddes_bnrkey.code_type
                          ) AS TEXT) = CAST((
                            CAST('Banner Key' AS VARCHAR)
                          ) AS TEXT)
                        )
                        AND (
                          CAST((
                            cddes_bnrkey.code
                          ) AS TEXT) = CAST((
                            cus_sales.bnr_key
                          ) AS TEXT)
                        )
                      )
                    )
                )
                LEFT JOIN edw_code_descriptions AS cddes_bnrfmt
                  ON (
                    (
                      (
                        CAST((
                          cddes_bnrfmt.code_type
                        ) AS TEXT) = CAST((
                          CAST('Banner Format Key' AS VARCHAR)
                        ) AS TEXT)
                      )
                      AND (
                        CAST((
                          cddes_bnrfmt.code
                        ) AS TEXT) = CAST((
                          cus_sales.bnr_frmt_key
                        ) AS TEXT)
                      )
                    )
                  )
              )
              LEFT JOIN edw_code_descriptions AS cddes_chnl
                ON (
                  (
                    (
                      CAST((
                        cddes_chnl.code_type
                      ) AS TEXT) = CAST((
                        CAST('Channel Key' AS VARCHAR)
                      ) AS TEXT)
                    )
                    AND (
                      CAST((
                        cddes_chnl.code
                      ) AS TEXT) = CAST((
                        cus_sales.chnl_key
                      ) AS TEXT)
                    )
                  )
                )
            )
            LEFT JOIN edw_code_descriptions AS cddes_gtm
              ON (
                (
                  (
                    CAST((
                      cddes_gtm.code_type
                    ) AS TEXT) = CAST((
                      CAST('Go To Model Key' AS VARCHAR)
                    ) AS TEXT)
                  )
                  AND (
                    CAST((
                      cddes_gtm.code
                    ) AS TEXT) = CAST((
                      cus_sales.go_to_mdl_key
                    ) AS TEXT)
                  )
                )
              )
          )
          LEFT JOIN edw_code_descriptions AS cddes_subchnl
            ON (
              (
                (
                  CAST((
                    cddes_subchnl.code_type
                  ) AS TEXT) = CAST((
                    CAST('Sub Channel Key' AS VARCHAR)
                  ) AS TEXT)
                )
                AND (
                  CAST((
                    cddes_subchnl.code
                  ) AS TEXT) = CAST((
                    cus_sales.sub_chnl_key
                  ) AS TEXT)
                )
              )
            )
        )
        LEFT JOIN edw_subchnl_retail_env_mapping AS subchnl_retail_env
          ON (
            (
              UPPER(CAST((
                subchnl_retail_env.sub_channel
              ) AS TEXT)) = UPPER(CAST((
                cddes_subchnl.code_desc
              ) AS TEXT))
            )
          )
      )
      LEFT JOIN (
        SELECT DISTINCT
          edw_code_descriptions_manual.source_type,
          edw_code_descriptions_manual.code_type,
          edw_code_descriptions_manual.code,
          edw_code_descriptions_manual.code_desc
        FROM edw_code_descriptions_manual
      ) AS ecdm
        ON (
          (
            CAST((
              cus_sales.cust_grp
            ) AS TEXT) = CAST((
              ecdm.code
            ) AS TEXT)
          )
        )
    )
    WHERE
      (
        NOT (
          (
            COALESCE(
              LTRIM(CAST((
                cus_sales.cust_num
              ) AS TEXT), CAST((
                CAST('0' AS VARCHAR)
              ) AS TEXT)),
              CAST((
                CAST('' AS VARCHAR)
              ) AS TEXT)
            ) || CAST((
              COALESCE(cus_sales.sls_org, CAST('' AS VARCHAR))
            ) AS TEXT)
          ) IN (
            SELECT
              (
                COALESCE(
                  LTRIM(
                    CAST((
                      edw_customer_sales_dim.cust_num
                    ) AS TEXT),
                    CAST((
                      CAST('0' AS VARCHAR)
                    ) AS TEXT)
                  ),
                  CAST((
                    CAST('' AS VARCHAR)
                  ) AS TEXT)
                ) || CAST((
                  COALESCE(edw_customer_sales_dim.sls_org, CAST('' AS VARCHAR))
                ) AS TEXT)
              )
            FROM edw_customer_sales_dim
            WHERE
              (
                (
                  (
                    (
                      (
                        (
                          (
                            LTRIM(
                              CAST((
                                edw_customer_sales_dim.cust_num
                              ) AS TEXT),
                              CAST((
                                CAST('0' AS VARCHAR)
                              ) AS TEXT)
                            ) = CAST((
                              CAST('134106' AS VARCHAR)
                            ) AS TEXT)
                          )
                          OR (
                            LTRIM(
                              CAST((
                                edw_customer_sales_dim.cust_num
                              ) AS TEXT),
                              CAST((
                                CAST('0' AS VARCHAR)
                              ) AS TEXT)
                            ) = CAST((
                              CAST('134258' AS VARCHAR)
                            ) AS TEXT)
                          )
                        )
                        OR (
                          LTRIM(
                            CAST((
                              edw_customer_sales_dim.cust_num
                            ) AS TEXT),
                            CAST((
                              CAST('0' AS VARCHAR)
                            ) AS TEXT)
                          ) = CAST((
                            CAST('134559' AS VARCHAR)
                          ) AS TEXT)
                        )
                      )
                      OR (
                        LTRIM(
                          CAST((
                            edw_customer_sales_dim.cust_num
                          ) AS TEXT),
                          CAST((
                            CAST('0' AS VARCHAR)
                          ) AS TEXT)
                        ) = CAST((
                          CAST('135353' AS VARCHAR)
                        ) AS TEXT)
                      )
                    )
                    OR (
                      LTRIM(
                        CAST((
                          edw_customer_sales_dim.cust_num
                        ) AS TEXT),
                        CAST((
                          CAST('0' AS VARCHAR)
                        ) AS TEXT)
                      ) = CAST((
                        CAST('135520' AS VARCHAR)
                      ) AS TEXT)
                    )
                  )
                  OR (
                    LTRIM(
                      CAST((
                        edw_customer_sales_dim.cust_num
                      ) AS TEXT),
                      CAST((
                        CAST('0' AS VARCHAR)
                      ) AS TEXT)
                    ) = CAST((
                      CAST('135117' AS VARCHAR)
                    ) AS TEXT)
                  )
                )
                AND (
                  CAST((
                    edw_customer_sales_dim.sls_org
                  ) AS TEXT) = CAST((
                    CAST('100A' AS VARCHAR)
                  ) AS TEXT)
                )
              )
          )
        )
      )
    UNION ALL
    SELECT
      cus_sales.clnt,
      cus_sales.cust_num,
      '100A' AS sls_org,
      '15' AS dstr_chnl,
      cus_sales.div,
      cus_sales.obj_crt_prsn,
      cus_sales.rec_crt_dt,
      cus_sales.auth_grp,
      cus_sales.cust_del_flag,
      cus_sales.cust_stat_grp,
      cus_sales.cust_ord_blk,
      cus_sales.prc_pcdr_asgn,
      cus_sales.cust_grp,
      cus_sales.sls_dstrc,
      cus_sales.prc_grp,
      cus_sales.prc_list_typ,
      cus_sales.ord_prob_itm,
      cus_sales.incoterm1,
      cus_sales.incoterm2,
      cus_sales.cust_delv_blk,
      cus_sales.cmplt_delv_sls_ord,
      cus_sales.max_no_prtl_delv_allw_itm,
      cus_sales.prtl_delv_itm_lvl,
      cus_sales.ord_comb_in,
      cus_sales.btch_splt_allw,
      cus_sales.delv_prir,
      cus_sales.vend_acct_num,
      cus_sales.shipping_cond,
      cus_sales.bill_blk_cust,
      cus_sales.man_invc_maint,
      cus_sales.invc_dt,
      cus_sales.invc_list_sched,
      cus_sales.cost_est_in,
      cus_sales.val_lmt_cost_est,
      cus_sales.crncy_key,
      cus_sales.cust_clas,
      cus_sales.acct_asgnmt_grp,
      cus_sales.delv_plnt,
      cus_sales.sls_grp,
      cus_sales.sls_grp_desc,
      cus_sales.sls_ofc,
      cus_sales.sls_ofc_desc,
      cus_sales.itm_props,
      cus_sales.cust_grp1,
      cus_sales.cust_grp2,
      cus_sales.cust_grp3,
      cus_sales.cust_grp4,
      cus_sales.cust_grp5,
      cus_sales.cust_rebt_in,
      cus_sales.rebt_indx_cust_strt_prd,
      cus_sales.exch_rt_typ,
      cus_sales.prc_dtrmn_id,
      cus_sales.prod_attr_id1,
      cus_sales.prod_attr_id2,
      cus_sales.prod_attr_id3,
      cus_sales.prod_attr_id4,
      cus_sales.prod_attr_id5,
      cus_sales.prod_attr_id6,
      cus_sales.prod_attr_id7,
      cus_sales.prod_attr_id8,
      cus_sales.prod_attr_id9,
      cus_sales.prod_attr_id10,
      cus_sales.pymt_key_term,
      cus_sales.persnl_num,
      cus_sales.crt_dttm,
      cus_sales.updt_dttm,
      cus_sales.cur_sls_emp,
      cus_sales.lcl_cust_grp_1,
      cus_sales.lcl_cust_grp_2,
      cus_sales.lcl_cust_grp_3,
      cus_sales.lcl_cust_grp_4,
      cus_sales.lcl_cust_grp_5,
      cus_sales.lcl_cust_grp_6,
      cus_sales.lcl_cust_grp_7,
      cus_sales.lcl_cust_grp_8,
      cus_sales.prc_proc,
      cus_sales.par_del,
      cus_sales.max_num_pa,
      cus_sales.prnt_cust_key,
      cus_sales.bnr_key,
      cus_sales.bnr_frmt_key,
      cus_sales.go_to_mdl_key,
      cus_sales.chnl_key,
      cus_sales.sub_chnl_key,
      cus_sales.segmt_key,
      cus_sales.cust_set_1,
      cus_sales.cust_set_2,
      cus_sales.cust_set_3,
      cus_sales.cust_set_4,
      cus_sales.cust_set_5,
      cddes_pck.code_desc AS "parent customer",
      cddes_bnrkey.code_desc AS banner,
      cddes_bnrfmt.code_desc AS "banner format",
      cddes_chnl.code_desc AS channel,
      cddes_gtm.code_desc AS "go to model",
      cddes_subchnl.code_desc AS "sub channel",
      subchnl_retail_env.retail_env,
      replace(NULL ,'UNKNOWN') AS code_desc
    FROM (
      (
        (
          (
            (
              (
                (
                  edw_customer_sales_dim AS cus_sales
                    LEFT JOIN edw_code_descriptions AS cddes_pck
                      ON (
                        (
                          (
                            CAST((
                              cddes_pck.code_type
                            ) AS TEXT) = CAST((
                              CAST('Parent Customer Key' AS VARCHAR)
                            ) AS TEXT)
                          )
                          AND (
                            CAST((
                              cddes_pck.code
                            ) AS TEXT) = CAST((
                              cus_sales.prnt_cust_key
                            ) AS TEXT)
                          )
                        )
                      )
                )
                LEFT JOIN edw_code_descriptions AS cddes_bnrkey
                  ON (
                    (
                      (
                        CAST((
                          cddes_bnrkey.code_type
                        ) AS TEXT) = CAST((
                          CAST('Banner Key' AS VARCHAR)
                        ) AS TEXT)
                      )
                      AND (
                        CAST((
                          cddes_bnrkey.code
                        ) AS TEXT) = CAST((
                          cus_sales.bnr_key
                        ) AS TEXT)
                      )
                    )
                  )
              )
              LEFT JOIN edw_code_descriptions AS cddes_bnrfmt
                ON (
                  (
                    (
                      CAST((
                        cddes_bnrfmt.code_type
                      ) AS TEXT) = CAST((
                        CAST('Banner Format Key' AS VARCHAR)
                      ) AS TEXT)
                    )
                    AND (
                      CAST((
                        cddes_bnrfmt.code
                      ) AS TEXT) = CAST((
                        cus_sales.bnr_frmt_key
                      ) AS TEXT)
                    )
                  )
                )
            )
            LEFT JOIN edw_code_descriptions AS cddes_chnl
              ON (
                (
                  (
                    CAST((
                      cddes_chnl.code_type
                    ) AS TEXT) = CAST((
                      CAST('Channel Key' AS VARCHAR)
                    ) AS TEXT)
                  )
                  AND (
                    CAST((
                      cddes_chnl.code
                    ) AS TEXT) = CAST((
                      cus_sales.chnl_key
                    ) AS TEXT)
                  )
                )
              )
          )
          LEFT JOIN edw_code_descriptions AS cddes_gtm
            ON (
              (
                (
                  CAST((
                    cddes_gtm.code_type
                  ) AS TEXT) = CAST((
                    CAST('Go To Model Key' AS VARCHAR)
                  ) AS TEXT)
                )
                AND (
                  CAST((
                    cddes_gtm.code
                  ) AS TEXT) = CAST((
                    cus_sales.go_to_mdl_key
                  ) AS TEXT)
                )
              )
            )
        )
        LEFT JOIN edw_code_descriptions AS cddes_subchnl
          ON (
            (
              (
                CAST((
                  cddes_subchnl.code_type
                ) AS TEXT) = CAST((
                  CAST('Sub Channel Key' AS VARCHAR)
                ) AS TEXT)
              )
              AND (
                CAST((
                  cddes_subchnl.code
                ) AS TEXT) = CAST((
                  cus_sales.sub_chnl_key
                ) AS TEXT)
              )
            )
          )
      )
      LEFT JOIN edw_subchnl_retail_env_mapping AS subchnl_retail_env
        ON (
          (
            UPPER(CAST((
              subchnl_retail_env.sub_channel
            ) AS TEXT)) = UPPER(CAST((
              cddes_subchnl.code_desc
            ) AS TEXT))
          )
        )
    )
    WHERE
      (
        (
          (
            (
              (
                (
                  (
                    CAST((
                      cus_sales.cust_num
                    ) AS TEXT) = CAST((
                      CAST('0000134106' AS VARCHAR)
                    ) AS TEXT)
                  )
                  OR (
                    CAST((
                      cus_sales.cust_num
                    ) AS TEXT) = CAST((
                      CAST('0000134258' AS VARCHAR)
                    ) AS TEXT)
                  )
                )
                OR (
                  CAST((
                    cus_sales.cust_num
                  ) AS TEXT) = CAST((
                    CAST('0000134559' AS VARCHAR)
                  ) AS TEXT)
                )
              )
              OR (
                CAST((
                  cus_sales.cust_num
                ) AS TEXT) = CAST((
                  CAST('0000135353' AS VARCHAR)
                ) AS TEXT)
              )
            )
            OR (
              CAST((
                cus_sales.cust_num
              ) AS TEXT) = CAST((
                CAST('0000135520' AS VARCHAR)
              ) AS TEXT)
            )
          )
          OR (
            CAST((
              cus_sales.cust_num
            ) AS TEXT) = CAST((
              CAST('0000135117' AS VARCHAR)
            ) AS TEXT)
          )
        )
        AND (
          CAST((
            cus_sales.sls_org
          ) AS TEXT) = CAST((
            CAST('8888' AS VARCHAR)
          ) AS TEXT)
        )
      )
  )
  UNION ALL
  SELECT
    cus_sales.clnt,
    cus_sales.cust_num,
    '100A' AS sls_org,
    '19' AS dstr_chnl,
    cus_sales.div,
    cus_sales.obj_crt_prsn,
    cus_sales.rec_crt_dt,
    cus_sales.auth_grp,
    cus_sales.cust_del_flag,
    cus_sales.cust_stat_grp,
    cus_sales.cust_ord_blk,
    cus_sales.prc_pcdr_asgn,
    cus_sales.cust_grp,
    cus_sales.sls_dstrc,
    cus_sales.prc_grp,
    cus_sales.prc_list_typ,
    cus_sales.ord_prob_itm,
    cus_sales.incoterm1,
    cus_sales.incoterm2,
    cus_sales.cust_delv_blk,
    cus_sales.cmplt_delv_sls_ord,
    cus_sales.max_no_prtl_delv_allw_itm,
    cus_sales.prtl_delv_itm_lvl,
    cus_sales.ord_comb_in,
    cus_sales.btch_splt_allw,
    cus_sales.delv_prir,
    cus_sales.vend_acct_num,
    cus_sales.shipping_cond,
    cus_sales.bill_blk_cust,
    cus_sales.man_invc_maint,
    cus_sales.invc_dt,
    cus_sales.invc_list_sched,
    cus_sales.cost_est_in,
    cus_sales.val_lmt_cost_est,
    cus_sales.crncy_key,
    cus_sales.cust_clas,
    cus_sales.acct_asgnmt_grp,
    cus_sales.delv_plnt,
    cus_sales.sls_grp,
    cus_sales.sls_grp_desc,
    cus_sales.sls_ofc,
    cus_sales.sls_ofc_desc,
    cus_sales.itm_props,
    cus_sales.cust_grp1,
    cus_sales.cust_grp2,
    cus_sales.cust_grp3,
    cus_sales.cust_grp4,
    cus_sales.cust_grp5,
    cus_sales.cust_rebt_in,
    cus_sales.rebt_indx_cust_strt_prd,
    cus_sales.exch_rt_typ,
    cus_sales.prc_dtrmn_id,
    cus_sales.prod_attr_id1,
    cus_sales.prod_attr_id2,
    cus_sales.prod_attr_id3,
    cus_sales.prod_attr_id4,
    cus_sales.prod_attr_id5,
    cus_sales.prod_attr_id6,
    cus_sales.prod_attr_id7,
    cus_sales.prod_attr_id8,
    cus_sales.prod_attr_id9,
    cus_sales.prod_attr_id10,
    cus_sales.pymt_key_term,
    cus_sales.persnl_num,
    cus_sales.crt_dttm,
    cus_sales.updt_dttm,
    cus_sales.cur_sls_emp,
    cus_sales.lcl_cust_grp_1,
    cus_sales.lcl_cust_grp_2,
    cus_sales.lcl_cust_grp_3,
    cus_sales.lcl_cust_grp_4,
    cus_sales.lcl_cust_grp_5,
    cus_sales.lcl_cust_grp_6,
    cus_sales.lcl_cust_grp_7,
    cus_sales.lcl_cust_grp_8,
    cus_sales.prc_proc,
    cus_sales.par_del,
    cus_sales.max_num_pa,
    cus_sales.prnt_cust_key,
    cus_sales.bnr_key,
    cus_sales.bnr_frmt_key,
    cus_sales.go_to_mdl_key,
    cus_sales.chnl_key,
    cus_sales.sub_chnl_key,
    cus_sales.segmt_key,
    cus_sales.cust_set_1,
    cus_sales.cust_set_2,
    cus_sales.cust_set_3,
    cus_sales.cust_set_4,
    cus_sales.cust_set_5,
    cddes_pck.code_desc AS "parent customer",
    cddes_bnrkey.code_desc AS banner,
    cddes_bnrfmt.code_desc AS "banner format",
    cddes_chnl.code_desc AS channel,
    cddes_gtm.code_desc AS "go to model",
    cddes_subchnl.code_desc AS "sub channel",
    subchnl_retail_env.retail_env,
    replace(NULL ,'UNKNOWN') AS code_desc
  FROM (
    (
      (
        (
          (
            (
              (
                edw_customer_sales_dim AS cus_sales
                  LEFT JOIN edw_code_descriptions AS cddes_pck
                    ON (
                      (
                        (
                          CAST((
                            cddes_pck.code_type
                          ) AS TEXT) = CAST((
                            CAST('Parent Customer Key' AS VARCHAR)
                          ) AS TEXT)
                        )
                        AND (
                          CAST((
                            cddes_pck.code
                          ) AS TEXT) = CAST((
                            cus_sales.prnt_cust_key
                          ) AS TEXT)
                        )
                      )
                    )
              )
              LEFT JOIN edw_code_descriptions AS cddes_bnrkey
                ON (
                  (
                    (
                      CAST((
                        cddes_bnrkey.code_type
                      ) AS TEXT) = CAST((
                        CAST('Banner Key' AS VARCHAR)
                      ) AS TEXT)
                    )
                    AND (
                      CAST((
                        cddes_bnrkey.code
                      ) AS TEXT) = CAST((
                        cus_sales.bnr_key
                      ) AS TEXT)
                    )
                  )
                )
            )
            LEFT JOIN edw_code_descriptions AS cddes_bnrfmt
              ON (
                (
                  (
                    CAST((
                      cddes_bnrfmt.code_type
                    ) AS TEXT) = CAST((
                      CAST('Banner Format Key' AS VARCHAR)
                    ) AS TEXT)
                  )
                  AND (
                    CAST((
                      cddes_bnrfmt.code
                    ) AS TEXT) = CAST((
                      cus_sales.bnr_frmt_key
                    ) AS TEXT)
                  )
                )
              )
          )
          LEFT JOIN edw_code_descriptions AS cddes_chnl
            ON (
              (
                (
                  CAST((
                    cddes_chnl.code_type
                  ) AS TEXT) = CAST((
                    CAST('Channel Key' AS VARCHAR)
                  ) AS TEXT)
                )
                AND (
                  CAST((
                    cddes_chnl.code
                  ) AS TEXT) = CAST((
                    cus_sales.chnl_key
                  ) AS TEXT)
                )
              )
            )
        )
        LEFT JOIN edw_code_descriptions AS cddes_gtm
          ON (
            (
              (
                CAST((
                  cddes_gtm.code_type
                ) AS TEXT) = CAST((
                  CAST('Go To Model Key' AS VARCHAR)
                ) AS TEXT)
              )
              AND (
                CAST((
                  cddes_gtm.code
                ) AS TEXT) = CAST((
                  cus_sales.go_to_mdl_key
                ) AS TEXT)
              )
            )
          )
      )
      LEFT JOIN edw_code_descriptions AS cddes_subchnl
        ON (
          (
            (
              CAST((
                cddes_subchnl.code_type
              ) AS TEXT) = CAST((
                CAST('Sub Channel Key' AS VARCHAR)
              ) AS TEXT)
            )
            AND (
              CAST((
                cddes_subchnl.code
              ) AS TEXT) = CAST((
                cus_sales.sub_chnl_key
              ) AS TEXT)
            )
          )
        )
    )
    LEFT JOIN edw_subchnl_retail_env_mapping AS subchnl_retail_env
      ON (
        (
          UPPER(CAST((
            subchnl_retail_env.sub_channel
          ) AS TEXT)) = UPPER(CAST((
            cddes_subchnl.code_desc
          ) AS TEXT))
        )
      )
  )
  WHERE
    (
      (
        (
          (
            (
              (
                (
                  CAST((
                    cus_sales.cust_num
                  ) AS TEXT) = CAST((
                    CAST('0000134106' AS VARCHAR)
                  ) AS TEXT)
                )
                OR (
                  CAST((
                    cus_sales.cust_num
                  ) AS TEXT) = CAST((
                    CAST('0000134258' AS VARCHAR)
                  ) AS TEXT)
                )
              )
              OR (
                CAST((
                  cus_sales.cust_num
                ) AS TEXT) = CAST((
                  CAST('0000134559' AS VARCHAR)
                ) AS TEXT)
              )
            )
            OR (
              CAST((
                cus_sales.cust_num
              ) AS TEXT) = CAST((
                CAST('0000135353' AS VARCHAR)
              ) AS TEXT)
            )
          )
          OR (
            CAST((
              cus_sales.cust_num
            ) AS TEXT) = CAST((
              CAST('0000135520' AS VARCHAR)
            ) AS TEXT)
          )
        )
        OR (
          CAST((
            cus_sales.cust_num
          ) AS TEXT) = CAST((
            CAST('0000135117' AS VARCHAR)
          ) AS TEXT)
        )
      )
      AND (
        CAST((
          cus_sales.sls_org
        ) AS TEXT) = CAST((
          CAST('8888' AS VARCHAR)
        ) AS TEXT)
      )
  )
  ),
  final as(
      select 
      clnt as "clnt",
      cust_num as "cust_num",
      sls_org as "sls_org",
      dstr_chnl as "dstr_chnl",
      div as "div",
      obj_crt_prsn as "obj_crt_prsn",
      rec_crt_dt as "rec_crt_dt",
      auth_grp as "auth_grp",
      cust_del_flag as "cust_del_flag",
      cust_stat_grp as "cust_stat_grp",
      cust_ord_blk as "cust_ord_blk",
      prc_pcdr_asgn as "prc_pcdr_asgn",
      cust_grp as "cust_grp",
      sls_dstrc as "sls_dstrc",
      prc_grp as "prc_grp",
      prc_list_typ as "prc_list_typ",
      ord_prob_itm as "ord_prob_itm",
      incoterm1 as "incoterm1",
      incoterm2 as "incoterm2",
      cust_delv_blk as "cust_delv_blk",
      cmplt_delv_sls_ord as "cmplt_delv_sls_ord",
      max_no_prtl_delv_allw_itm as "max_no_prtl_delv_allw_itm",
      prtl_delv_itm_lvl as "prtl_delv_itm_lvl",
      ord_comb_in as "ord_comb_in",
      btch_splt_allw as "btch_splt_allw",
      delv_prir as "delv_prir",
      vend_acct_num as "vend_acct_num",
      shipping_cond as "shipping_cond",
      bill_blk_cust as "bill_blk_cust",
      man_invc_maint as "man_invc_maint",
      invc_dt as "invc_dt",
      invc_list_sched as "invc_list_sched",
      cost_est_in as "cost_est_in",
      val_lmt_cost_est as "val_lmt_cost_est",
      crncy_key as "crncy_key",
      cust_clas as "cust_clas",
      acct_asgnmt_grp as "acct_asgnmt_grp",
      delv_plnt as "delv_plnt",
      sls_grp as "sls_grp",
      sls_grp_desc as "sls_grp_desc",
      sls_ofc as "sls_ofc",
      sls_ofc_desc as "sls_ofc_desc",
      itm_props as "itm_props",
      cust_grp1 as "cust_grp1",
      cust_grp2 as "cust_grp2",
      cust_grp3 as "cust_grp3",
      cust_grp4 as "cust_grp4",
      cust_grp5 as "cust_grp5",
      cust_rebt_in as "cust_rebt_in",
      rebt_indx_cust_strt_prd as "rebt_indx_cust_strt_prd",
      exch_rt_typ as "exch_rt_typ",
      prc_dtrmn_id as "prc_dtrmn_id",
      prod_attr_id1 as "prod_attr_id1",
      prod_attr_id2 as "prod_attr_id2",
      prod_attr_id3 as "prod_attr_id3",
      prod_attr_id4 as "prod_attr_id4",
      prod_attr_id5 as "prod_attr_id5",
      prod_attr_id6 as "prod_attr_id6",
      prod_attr_id7 as "prod_attr_id7",
      prod_attr_id8 as "prod_attr_id8",
      prod_attr_id9 as "prod_attr_id9",
      prod_attr_id10 as "prod_attr_id10",
      pymt_key_term as "pymt_key_term",
      persnl_num as "persnl_num",
      crt_dttm as "crt_dttm",
      updt_dttm as "updt_dttm",
      cur_sls_emp as "cur_sls_emp",
      lcl_cust_grp_1 as "lcl_cust_grp_1",
      lcl_cust_grp_2 as "lcl_cust_grp_2",
      lcl_cust_grp_3 as "lcl_cust_grp_3",
      lcl_cust_grp_4 as "lcl_cust_grp_4",
      lcl_cust_grp_5 as "lcl_cust_grp_5",
      lcl_cust_grp_6 as "lcl_cust_grp_6",
      lcl_cust_grp_7 as "lcl_cust_grp_7",
      lcl_cust_grp_8 as "lcl_cust_grp_8",
      prc_proc as "prc_proc",
      par_del as "par_del",
      max_num_pa as "max_num_pa",
      prnt_cust_key as "prnt_cust_key",
      bnr_key as "bnr_key",
      bnr_frmt_key as "bnr_frmt_key",
      go_to_mdl_key as "go_to_mdl_key",
      chnl_key as "chnl_key",
      sub_chnl_key as "sub_chnl_key",
      segmt_key as "segmt_key",
      cust_set_1 as "cust_set_1",
      cust_set_2 as "cust_set_2",
      cust_set_3 as "cust_set_3",
      cust_set_4 as "cust_set_4",
      cust_set_5 as "cust_set_5",
      "parent customer" as "parent customer",
      banner as "banner",
      "banner format" as "banner format",
      channel as "channel",
      "go to model" as "go to model",
      "sub channel" as "sub channel",
      retail_env as "retail_env",
      code_desc as "code_desc"
      from transformed
  )

  select * from final
);
----------------------------------------------------------------------------------

create or replace view aspedw_integration.v_rpt_copa
as
(
  with edw_copa_trans_fact as(
      select * from aspedw_integration.edw_copa_trans_fact
  ),
  edw_calendar_dim as(
      select * from aspedw_integration.edw_calendar_dim
  ),
  edw_company_dim as(
      select * from aspedw_integration.edw_company_dim
  ),
  edw_material_dim as(
      select * from aspedw_integration.edw_material_dim
  ),
  v_intrm_reg_crncy_exch_fiscper as(
      select * from aspedw_integration.v_intrm_reg_crncy_exch_fiscper
  ),
  edw_invoice_fact as(
      select * from aspedw_integration.edw_invoice_fact
  ),
  v_edw_customer_sales_dim as(
      select * from aspedw_integration.v_edw_customer_sales_dim
  ),

  transformed as(

    select
    main.prev_fisc_yr_per as "prev_fisc_yr_per",
    main.latest_date as "latest_date",
    main.latest_fisc_yrmnth as "latest_fisc_yrmnth",
    main.fisc_yr as "fisc_yr",
    main.fisc_yr_per as "fisc_yr_per",
    main.fisc_day as "fisc_day",
    main.ctry_nm as "ctry_nm",
    main."cluster",
    main.obj_crncy_co_obj as "obj_crncy_co_obj",
    mat.mega_brnd_desc as "b1 mega-brand",
    mat.brnd_desc as "b2 brand",
    mat.base_prod_desc as "b3 base product",
    mat.varnt_desc as "b4 variant",
    mat.put_up_desc as "b5 put-up",
    mat.prodh1_txtmd as "prod h1 operating group",
    mat.prodh2_txtmd as "prod h2 franchise group",
    mat.prodh3_txtmd as "prod h3 franchise",
    mat.prodh4_txtmd as "prod h4 product franchise",
    mat.prodh5_txtmd as "prod h5 product major",
    mat.prodh6_txtmd as "prod h6 product minor",
    cus_sales_extn."parent customer",
    cus_sales_extn.banner as "banner",
    cus_sales_extn."banner format",
    cus_sales_extn.channel as "channel",
    cus_sales_extn."go to model",
    cus_sales_extn."sub channel",
    cus_sales_extn.retail_env as "retail_env",
    SUM(main.nts_usd) as "nts_usd",
    SUM(main.nts_lcy) as "nts_lcy",
    SUM(main.gts_usd) as "gts_usd",
    SUM(main.gts_lcy) as "gts_lcy",
    SUM(main.eq_usd) as "eq_usd",
    SUM(main.eq_lcy) as "eq_lcy",
    main.from_crncy as "from_crncy",
    main.to_crncy as "to_crncy",
    SUM(main.nts_qty) as "nts_qty",
    SUM(main.gts_qty) as "gts_qty",
    SUM(main.eq_qty) as "eq_qty",
    SUM(main.ord_pc_qty) as "ord_pc_qty",
    SUM(main.unspp_qty) as "unspp_qty",
    main.cust_num as "cust_num"
  FROM (
    (
      (
        (
          SELECT
            cast((((
                  cast((
                    cast((
                      DATE_PART(
                        'YEAR',
                        (
                          TO_DATE(
                            (
                              cast((
                                cast((
                                  calendar.fisc_yr
                                ) as VARCHAR)
                              ) as TEXT) || SUBSTRING(cast((
                                cast((
                                  calendar.fisc_per
                                ) as VARCHAR)
                              ) as TEXT), 6)
                            ),
                            cast((
                              cast('yyyymm' as VARCHAR)
                            ) as TEXT)
                          ) -1
                        )
                      )
                    ) as VARCHAR)
                  ) as TEXT) || cast((
                    cast('0' as VARCHAR)
                  ) as TEXT)
                ) || cast((
                  cast((
                    DATE_PART(
                    'MONTH',
                      (
                        TO_DATE(
                          (
                            cast((
                              cast((
                                calendar.fisc_yr
                              ) AS VARCHAR)
                            ) AS TEXT) || SUBSTRING(cast((
                              cast((
                                calendar.fisc_per
                              ) AS VARCHAR)
                            ) AS TEXT), 6)
                          ),
                          cast((
                            cast('yyyymm' AS VARCHAR)
                          ) AS TEXT)
                        ) - 1
                      )
                    )
                  ) AS VARCHAR)
                ) AS TEXT)
              )
            ) AS VARCHAR) AS prev_fisc_yr_per,
              TO_CHAR(
            CONVERT_TIMEZONE(
              'Asia/Singapore',
              CURRENT_TIMESTAMP::TIMESTAMP_NTZ
            )::TIMESTAMP_NTZ,
            'YYYYMMDD'
        ) AS latest_date,
            cast((
              (
                cast((
                  cast((
                    calendar.fisc_yr
                  ) AS VARCHAR)
                ) AS TEXT) || SUBSTRING(cast((
                  cast((
                    calendar.fisc_per
                  ) AS VARCHAR)
                ) AS TEXT), 6)
              )
            ) AS VARCHAR) AS latest_fisc_yrmnth,
            copa.fisc_yr,
            copa.fisc_yr_per,
            TO_DATE(
              (
                (
                  SUBSTRING(cast((
                    cast((
                      copa.fisc_yr_per
                    ) AS VARCHAR)
                  ) AS TEXT), 6, 8) || cast((
                    cast('01' AS VARCHAR)
                  ) AS TEXT)
                ) || SUBSTRING(cast((
                  cast((
                    copa.fisc_yr_per
                  ) AS VARCHAR)
                ) AS TEXT), 1, 4)
              ),
              cast((
                cast('MMDDYYYY' AS VARCHAR)
              ) AS TEXT)
            ) AS fisc_day,
            CASE
              WHEN (
                (
                  (
                    (
                      (
                        (
                          (
                            LTRIM(
                              cast((
                                copa.cust_num
                              ) AS TEXT),
                              cast((
                                cast((
                                  0
                                ) AS VARCHAR)
                              ) AS TEXT)
                            ) = cast((
                              cast('134559' AS VARCHAR)
                            ) AS TEXT)
                          )
                          OR (
                            LTRIM(
                              cast((
                                copa.cust_num
                              ) AS TEXT),
                              cast((
                                cast((
                                  0
                                ) AS VARCHAR)
                              ) AS TEXT)
                            ) = cast((
                              cast('134106' AS VARCHAR)
                            ) AS TEXT)
                          )
                        )
                        OR (
                          LTRIM(
                            cast((
                              copa.cust_num
                            ) AS TEXT),
                            cast((
                              cast((
                                0
                              ) AS VARCHAR)
                            ) AS TEXT)
                          ) = cast((
                            cast('134258' AS VARCHAR)
                          ) AS TEXT)
                        )
                      )
                      OR (
                        LTRIM(
                          cast((
                            copa.cust_num
                          ) AS TEXT),
                          cast((
                            cast((
                              0
                            ) AS VARCHAR)
                          ) AS TEXT)
                        ) = cast((
                          cast('134855' AS VARCHAR)
                        ) AS TEXT)
                      )
                    )
                    AND (
                      LTRIM(
                        cast((
                          copa.acct_num
                        ) AS TEXT),
                        cast((
                          cast((
                            0
                          ) AS VARCHAR)
                        ) AS TEXT)
                      ) <> cast((
                        cast('403185' AS VARCHAR)
                      ) AS TEXT)
                    )
                  )
                  AND (
                    cast((
                      mat.mega_brnd_desc
                    ) AS TEXT) <> cast((
                      cast('Vogue Int''l' AS VARCHAR)
                    ) AS TEXT)
                  )
                )
                AND (
                  copa.fisc_yr = 2018
                )
              )
              THEN cast('China Selfcare' AS VARCHAR)
              ELSE cmp.ctry_group
            END AS ctry_nm,
            CASE
              WHEN (
                (
                  (
                    (
                      (
                        (
                          (
                            LTRIM(
                              cast((
                                copa.cust_num
                              ) AS TEXT),
                              cast((
                                cast((
                                  0
                                ) AS VARCHAR)
                              ) AS TEXT)
                            ) = cast((
                              cast('134559' AS VARCHAR)
                            ) AS TEXT)
                          )
                          OR (
                            LTRIM(
                              cast((
                                copa.cust_num
                              ) AS TEXT),
                              cast((
                                cast((
                                  0
                                ) AS VARCHAR)
                              ) AS TEXT)
                            ) = cast((
                              cast('134106' AS VARCHAR)
                            ) AS TEXT)
                          )
                        )
                        OR (
                          LTRIM(
                            cast((
                              copa.cust_num
                            ) AS TEXT),
                            cast((
                              cast((
                                0
                              ) AS VARCHAR)
                            ) AS TEXT)
                          ) = cast((
                            cast('134258' AS VARCHAR)
                          ) AS TEXT)
                        )
                      )
                      OR (
                        LTRIM(
                          cast((
                            copa.cust_num
                          ) AS TEXT),
                          cast((
                            cast((
                              0
                            ) AS VARCHAR)
                          ) AS TEXT)
                        ) = cast((
                          cast('134855' AS VARCHAR)
                        ) AS TEXT)
                      )
                    )
                    AND (
                      LTRIM(
                        cast((
                          copa.acct_num
                        ) AS TEXT),
                        cast((
                          cast((
                            0
                          ) AS VARCHAR)
                        ) AS TEXT)
                      ) <> cast((
                        cast('403185' AS VARCHAR)
                      ) AS TEXT)
                    )
                  )
                  AND (
                    cast((
                      mat.mega_brnd_desc
                    ) AS TEXT) <> cast((
                      cast('Vogue Int''l' AS VARCHAR)
                    ) AS TEXT)
                  )
                )
                AND (
                  copa.fisc_yr = 2018
                )
              )
              THEN cast('China' AS VARCHAR)
              ELSE cmp."cluster"
            END AS "cluster",
            CASE
              WHEN (
                cast((
                  cmp.ctry_group
                ) AS TEXT) = cast((
                  cast('India' AS VARCHAR)
                ) AS TEXT)
              )
              THEN cast('INR' AS VARCHAR)
              WHEN (
                cast((
                  cmp.ctry_group
                ) AS TEXT) = cast((
                  cast('Philippines' AS VARCHAR)
                ) AS TEXT)
              )
              THEN cast('PHP' AS VARCHAR)
              WHEN (
                (
                  cast((
                    cmp.ctry_group
                  ) AS TEXT) = cast((
                    cast('China Selfcare' AS VARCHAR)
                  ) AS TEXT)
                )
                OR (
                  cast((
                    cmp.ctry_group
                  ) AS TEXT) = cast((
                    cast('China Personal Care' AS VARCHAR)
                  ) AS TEXT)
                )
              )
              THEN cast('RMB' AS VARCHAR)
              ELSE copa.obj_crncy_co_obj
            END AS obj_crncy_co_obj,
            copa.matl_num,
            copa.co_cd,
            CASE
              WHEN (
                (
                  LTRIM(cast((
                    copa.cust_num
                  ) AS TEXT), cast((
                    cast('0' AS VARCHAR)
                  ) AS TEXT)) = cast((
                    cast('135520' AS VARCHAR)
                  ) AS TEXT)
                )
                AND (
                  (
                    cast((
                      copa.co_cd
                    ) AS TEXT) = cast((
                      cast('703A' AS VARCHAR)
                    ) AS TEXT)
                  )
                  OR (
                    cast((
                      copa.co_cd
                    ) AS TEXT) = cast((
                      cast('8888' AS VARCHAR)
                    ) AS TEXT)
                  )
                )
              )
              THEN cast('100A' AS VARCHAR)
              ELSE copa.sls_org
            END AS sls_org,
            CASE
              WHEN (
                (
                  LTRIM(cast((
                    copa.cust_num
                  ) AS TEXT), cast((
                    cast('0' AS VARCHAR)
                  ) AS TEXT)) = cast((
                    cast('135520' AS VARCHAR)
                  ) AS TEXT)
                )
                AND (
                  (
                    cast((
                      copa.co_cd
                    ) AS TEXT) = cast((
                      cast('703A' AS VARCHAR)
                    ) AS TEXT)
                  )
                  OR (
                    cast((
                      copa.co_cd
                    ) AS TEXT) = cast((
                      cast('8888' AS VARCHAR)
                    ) AS TEXT)
                  )
                )
              )
              THEN cast('15' AS VARCHAR)
              ELSE copa.dstr_chnl
            END AS dstr_chnl,
            copa.div,
            copa.cust_num,
            CASE
              WHEN (
                (
                  cast((
                    copa.acct_hier_shrt_desc
                  ) AS TEXT) = cast((
                    cast('NTS' AS VARCHAR)
                  ) AS TEXT)
                )
                AND (
                  cast((
                    exch_rate.to_crncy
                  ) AS TEXT) = cast((
                    cast('USD' AS VARCHAR)
                  ) AS TEXT)
                )
              )
              THEN SUM((
                copa.amt_obj_crncy * exch_rate.ex_rt
              ))
              ELSE cast((
                cast((
                  0
                ) AS DECIMAL)
              ) AS DECIMAL(18, 0))
            END AS nts_usd,
            CASE
              WHEN (
                (
                  cast((
                    copa.acct_hier_shrt_desc
                  ) AS TEXT) = cast((
                    cast('NTS' AS VARCHAR)
                  ) AS TEXT)
                )
                AND (
                  cast((
                    exch_rate.to_crncy
                  ) AS TEXT) = cast((
                    CASE
                      WHEN (
                        (
                          cast((
                            cmp.ctry_group
                          ) AS TEXT) = cast((
                            cast('India' AS VARCHAR)
                          ) AS TEXT)
                        )
                        OR (
                          (
                            cmp.ctry_group IS NULL
                          ) AND (
                            'India' IS NULL
                          )
                        )
                      )
                      THEN cast('INR' AS VARCHAR)
                      WHEN (
                        (
                          cast((
                            cmp.ctry_group
                          ) AS TEXT) = cast((
                            cast('Philippines' AS VARCHAR)
                          ) AS TEXT)
                        )
                        OR (
                          (
                            cmp.ctry_group IS NULL
                          ) AND (
                            'Philippines' IS NULL
                          )
                        )
                      )
                      THEN cast('PHP' AS VARCHAR)
                      WHEN (
                        (
                          cast((
                            cmp.ctry_group
                          ) AS TEXT) = cast((
                            cast('China Selfcare' AS VARCHAR)
                          ) AS TEXT)
                        )
                        OR (
                          (
                            cmp.ctry_group IS NULL
                          ) AND (
                            'China Selfcare' IS NULL
                          )
                        )
                      )
                      THEN cast('RMB' AS VARCHAR)
                      WHEN (
                        (
                          cast((
                            cmp.ctry_group
                          ) AS TEXT) = cast((
                            cast('China Personal Care' AS VARCHAR)
                          ) AS TEXT)
                        )
                        OR (
                          (
                            cmp.ctry_group IS NULL
                          ) AND (
                            'China Personal Care' IS NULL
                          )
                        )
                      )
                      THEN cast('RMB' AS VARCHAR)
                      ELSE copa.obj_crncy_co_obj
                    END
                  ) AS TEXT)
                )
              )
              THEN SUM((
                copa.amt_obj_crncy * exch_rate.ex_rt
              ))
              ELSE cast((
                cast((
                  0
                ) AS DECIMAL)
              ) AS DECIMAL(18, 0))
            END AS nts_lcy,
            CASE
              WHEN (
                (
                  cast((
                    copa.acct_hier_shrt_desc
                  ) AS TEXT) = cast((
                    cast('GTS' AS VARCHAR)
                  ) AS TEXT)
                )
                AND (
                  cast((
                    exch_rate.to_crncy
                  ) AS TEXT) = cast((
                    cast('USD' AS VARCHAR)
                  ) AS TEXT)
                )
              )
              THEN SUM((
                copa.amt_obj_crncy * exch_rate.ex_rt
              ))
              ELSE cast((
                cast((
                  0
                ) AS DECIMAL)
              ) AS DECIMAL(18, 0))
            END AS gts_usd,
            CASE
              WHEN (
                (
                  cast((
                    copa.acct_hier_shrt_desc
                  ) AS TEXT) = cast((
                    cast('GTS' AS VARCHAR)
                  ) AS TEXT)
                )
                AND (
                  cast((
                    exch_rate.to_crncy
                  ) AS TEXT) = cast((
                    CASE
                      WHEN (
                        (
                          cast((
                            cmp.ctry_group
                          ) AS TEXT) = cast((
                            cast('India' AS VARCHAR)
                          ) AS TEXT)
                        )
                        OR (
                          (
                            cmp.ctry_group IS NULL
                          ) AND (
                            'India' IS NULL
                          )
                        )
                      )
                      THEN cast('INR' AS VARCHAR)
                      WHEN (
                        (
                          cast((
                            cmp.ctry_group
                          ) AS TEXT) = cast((
                            cast('Philippines' AS VARCHAR)
                          ) AS TEXT)
                        )
                        OR (
                          (
                            cmp.ctry_group IS NULL
                          ) AND (
                            'Philippines' IS NULL
                          )
                        )
                      )
                      THEN cast('PHP' AS VARCHAR)
                      WHEN (
                        (
                          cast((
                            cmp.ctry_group
                          ) AS TEXT) = cast((
                            cast('China Selfcare' AS VARCHAR)
                          ) AS TEXT)
                        )
                        OR (
                          (
                            cmp.ctry_group IS NULL
                          ) AND (
                            'China Selfcare' IS NULL
                          )
                        )
                      )
                      THEN cast('RMB' AS VARCHAR)
                      WHEN (
                        (
                          cast((
                            cmp.ctry_group
                          ) AS TEXT) = cast((
                            cast('China Personal Care' AS VARCHAR)
                          ) AS TEXT)
                        )
                        OR (
                          (
                            cmp.ctry_group IS NULL
                          ) AND (
                            'China Personal Care' IS NULL
                          )
                        )
                      )
                      THEN cast('RMB' AS VARCHAR)
                      ELSE copa.obj_crncy_co_obj
                    END
                  ) AS TEXT)
                )
              )
              THEN SUM((
                copa.amt_obj_crncy * exch_rate.ex_rt
              ))
              ELSE cast((
                cast((
                  0
                ) AS DECIMAL)
              ) AS DECIMAL(18, 0))
            END AS gts_lcy,
            CASE
              WHEN (
                (
                  cast((
                    copa.acct_hier_shrt_desc
                  ) AS TEXT) = cast((
                    cast('EQ' AS VARCHAR)
                  ) AS TEXT)
                )
                AND (
                  cast((
                    exch_rate.to_crncy
                  ) AS TEXT) = cast((
                    cast('USD' AS VARCHAR)
                  ) AS TEXT)
                )
              )
              THEN SUM((
                copa.amt_obj_crncy * exch_rate.ex_rt
              ))
              ELSE cast((
                cast((
                  0
                ) AS DECIMAL)
              ) AS DECIMAL(18, 0))
            END AS eq_usd,
            CASE
              WHEN (
                (
                  cast((
                    copa.acct_hier_shrt_desc
                  ) AS TEXT) = cast((
                    cast('EQ' AS VARCHAR)
                  ) AS TEXT)
                )
                AND (
                  cast((
                    exch_rate.to_crncy
                  ) AS TEXT) = cast((
                    CASE
                      WHEN (
                        (
                          cast((
                            cmp.ctry_group
                          ) AS TEXT) = cast((
                            cast('India' AS VARCHAR)
                          ) AS TEXT)
                        )
                        OR (
                          (
                            cmp.ctry_group IS NULL
                          ) AND (
                            'India' IS NULL
                          )
                        )
                      )
                      THEN cast('INR' AS VARCHAR)
                      WHEN (
                        (
                          cast((
                            cmp.ctry_group
                          ) AS TEXT) = cast((
                            cast('Philippines' AS VARCHAR)
                          ) AS TEXT)
                        )
                        OR (
                          (
                            cmp.ctry_group IS NULL
                          ) AND (
                            'Philippines' IS NULL
                          )
                        )
                      )
                      THEN cast('PHP' AS VARCHAR)
                      WHEN (
                        (
                          cast((
                            cmp.ctry_group
                          ) AS TEXT) = cast((
                            cast('China Selfcare' AS VARCHAR)
                          ) AS TEXT)
                        )
                        OR (
                          (
                            cmp.ctry_group IS NULL
                          ) AND (
                            'China Selfcare' IS NULL
                          )
                        )
                      )
                      THEN cast('RMB' AS VARCHAR)
                      WHEN (
                        (
                          cast((
                            cmp.ctry_group
                          ) AS TEXT) = cast((
                            cast('China Personal Care' AS VARCHAR)
                          ) AS TEXT)
                        )
                        OR (
                          (
                            cmp.ctry_group IS NULL
                          ) AND (
                            'China Personal Care' IS NULL
                          )
                        )
                      )
                      THEN cast('RMB' AS VARCHAR)
                      ELSE copa.obj_crncy_co_obj
                    END
                  ) AS TEXT)
                )
              )
              THEN SUM((
                copa.amt_obj_crncy * exch_rate.ex_rt
              ))
              ELSE cast((
                cast(NULL AS DECIMAL)
              ) AS DECIMAL(18, 0))
            END AS eq_lcy,
            CASE
              WHEN (
                cast((
                  cmp.ctry_group
                ) AS TEXT) = cast((
                  cast('India' AS VARCHAR)
                ) AS TEXT)
              )
              THEN cast('INR' AS VARCHAR)
              WHEN (
                cast((
                  cmp.ctry_group
                ) AS TEXT) = cast((
                  cast('Philippines' AS VARCHAR)
                ) AS TEXT)
              )
              THEN cast('PHP' AS VARCHAR)
              WHEN (
                (
                  cast((
                    cmp.ctry_group
                  ) AS TEXT) = cast((
                    cast('China Selfcare' AS VARCHAR)
                  ) AS TEXT)
                )
                OR (
                  cast((
                    cmp.ctry_group
                  ) AS TEXT) = cast((
                    cast('China Personal Care' AS VARCHAR)
                  ) AS TEXT)
                )
              )
              THEN cast('RMB' AS VARCHAR)
              ELSE exch_rate.from_crncy
            END AS from_crncy,
            exch_rate.to_crncy,
            CASE
              WHEN (
                (
                  cast((
                    copa.acct_hier_shrt_desc
                  ) AS TEXT) = cast((
                    cast('NTS' AS VARCHAR)
                  ) AS TEXT)
                )
                AND (
                  cast((
                    exch_rate.to_crncy
                  ) AS TEXT) = cast((
                    cast('USD' AS VARCHAR)
                  ) AS TEXT)
                )
              )
              THEN SUM(copa.qty)
              ELSE cast((
                cast((
                  0
                ) AS DECIMAL)
              ) AS DECIMAL(18, 0))
            END AS nts_qty,
            CASE
              WHEN (
                (
                  cast((
                    copa.acct_hier_shrt_desc
                  ) AS TEXT) = cast((
                    cast('GTS' AS VARCHAR)
                  ) AS TEXT)
                )
                AND (
                  cast((
                    exch_rate.to_crncy
                  ) AS TEXT) = cast((
                    cast('USD' AS VARCHAR)
                  ) AS TEXT)
                )
              )
              THEN SUM(copa.qty)
              ELSE cast((
                cast((
                  0
                ) AS DECIMAL)
              ) AS DECIMAL(18, 0))
            END AS gts_qty,
            CASE
              WHEN (
                (
                  cast((
                    copa.acct_hier_shrt_desc
                  ) AS TEXT) = cast((
                    cast('EQ' AS VARCHAR)
                  ) AS TEXT)
                )
                AND (
                  cast((
                    exch_rate.to_crncy
                  ) AS TEXT) = cast((
                    cast('USD' AS VARCHAR)
                  ) AS TEXT)
                )
              )
              THEN SUM(copa.qty)
              ELSE cast((
                cast((
                  0
                ) AS DECIMAL)
              ) AS DECIMAL(18, 0))
            END AS eq_qty,
            0 AS ord_pc_qty,
            0 AS unspp_qty
          FROM (
            (
              (
                (
                  edw_copa_trans_fact AS copa
                    LEFT JOIN edw_calendar_dim AS calendar
                      ON (
                        (
                          calendar.cal_day = TO_DATE(
                            TO_CHAR(
                CONVERT_TIMEZONE(
                'Asia/Singapore',
            CURRENT_TIMESTAMP::TIMESTAMP_NTZ
                )::TIMESTAMP_NTZ,
                  'YYYY-MM-DD'
                  )::TEXT,
                  'YYYY-MM-DD'
                          )
                        )
                      )
                )
                LEFT JOIN edw_company_dim AS cmp
                  ON (
                    (
                      cast((
                        copa.co_cd
                      ) AS TEXT) = cast((
                        cmp.co_cd
                      ) AS TEXT)
                    )
                  )
              )
              LEFT JOIN edw_material_dim AS mat
                ON (
                  (
                    cast((
                      copa.matl_num
                    ) AS TEXT) = cast((
                      mat.matl_num
                    ) AS TEXT)
                  )
                )
            )
            LEFT JOIN ASPEDW_ACCESS.v_intrm_reg_crncy_exch_fiscper AS exch_rate
              ON (
                (
                  (
                    (
                      cast((
                        copa.obj_crncy_co_obj
                      ) AS TEXT) = cast((
                        exch_rate.from_crncy
                      ) AS TEXT)
                    )
                    AND (
                      copa.fisc_yr_per = exch_rate.fisc_per
                    )
                  )
                  AND CASE
                    WHEN (
                      cast((
                        exch_rate.to_crncy
                      ) AS TEXT) <> cast((
                        cast('USD' AS VARCHAR)
                      ) AS TEXT)
                    )
                    THEN (
                      cast((
                        exch_rate.to_crncy
                      ) AS TEXT) = cast((
                        CASE
                          WHEN (
                            (
                              cast((
                                cmp.ctry_group
                              ) AS TEXT) = cast((
                                cast('India' AS VARCHAR)
                              ) AS TEXT)
                            )
                            OR (
                              (
                                cmp.ctry_group IS NULL
                              ) AND (
                                'India' IS NULL
                              )
                            )
                          )
                          THEN cast('INR' AS VARCHAR)
                          WHEN (
                            (
                              cast((
                                cmp.ctry_group
                              ) AS TEXT) = cast((
                                cast('Philippines' AS VARCHAR)
                              ) AS TEXT)
                            )
                            OR (
                              (
                                cmp.ctry_group IS NULL
                              ) AND (
                                'Philippines' IS NULL
                              )
                            )
                          )
                          THEN cast('PHP' AS VARCHAR)
                          WHEN (
                            (
                              cast((
                                cmp.ctry_group
                              ) AS TEXT) = cast((
                                cast('China Selfcare' AS VARCHAR)
                              ) AS TEXT)
                            )
                            OR (
                              (
                                cmp.ctry_group IS NULL
                              ) AND (
                                'China Selfcare' IS NULL
                              )
                            )
                          )
                          THEN cast('RMB' AS VARCHAR)
                          WHEN (
                            (
                              cast((
                                cmp.ctry_group
                              ) AS TEXT) = cast((
                                cast('China Personal Care' AS VARCHAR)
                              ) AS TEXT)
                            )
                            OR (
                              (
                                cmp.ctry_group IS NULL
                              ) AND (
                                'China Personal Care' IS NULL
                              )
                            )
                          )
                          THEN cast('RMB' AS VARCHAR)
                          ELSE copa.obj_crncy_co_obj
                        END
                      ) AS TEXT)
                    )
                    ELSE (
                      cast((
                        exch_rate.to_crncy
                      ) AS TEXT) = cast((
                        cast('USD' AS VARCHAR)
                      ) AS TEXT)
                    )
                  END
                )
              )
          )
          WHERE
            (
              (
                (
                  (
                    cast((
                      copa.acct_hier_shrt_desc
                    ) AS TEXT) = cast((
                      cast('GTS' AS VARCHAR)
                    ) AS TEXT)
                  )
                  OR (
                    cast((
                      copa.acct_hier_shrt_desc
                    ) AS TEXT) = cast((
                      cast('NTS' AS VARCHAR)
                    ) AS TEXT)
                  )
                )
                OR (
                  cast((
                    copa.acct_hier_shrt_desc
                  ) AS TEXT) = cast((
                    cast('EQ' AS VARCHAR)
                  ) AS TEXT)
                )
              )
              AND (
                cast((
                  cast((
                    copa.fisc_yr_per
                  ) AS VARCHAR)
                ) AS TEXT) >= (
                  (
                    (
                      cast((
                        cast((
                          (
                            DATE_PART(YEAR, CURRENT_DATE()) - 2
                          )
                        ) AS VARCHAR)
                      ) AS TEXT) || cast((
                        cast((
                          0
                        ) AS VARCHAR)
                      ) AS TEXT)
                    ) || cast((
                      cast((
                        0
                      ) AS VARCHAR)
                    ) AS TEXT)
                  ) || cast((
                    cast((
                      1
                    ) AS VARCHAR)
                  ) AS TEXT)
                )
              )
            )
          GROUP BY
            calendar.fisc_yr,
            calendar.fisc_per,
            copa.fisc_yr,
            copa.fisc_yr_per,
            copa.obj_crncy_co_obj,
            copa.matl_num,
            copa.co_cd,
            copa.sls_org,
            copa.dstr_chnl,
            copa.div,
            copa.cust_num,
            copa.acct_num,
            copa.acct_hier_shrt_desc,
            exch_rate.from_crncy,
            exch_rate.to_crncy,
            cmp.ctry_group,
            cmp."cluster",
            mat.mega_brnd_desc
          UNION ALL
          SELECT
            cast((((
                  cast((
                    cast((
                      DATE_PART(
                        'YEAR',
                        (
                          TO_DATE(
                            (
                              cast((
                                cast((
                                  calendar1.fisc_yr
                                ) AS VARCHAR)
                              ) AS TEXT) || SUBSTRING(cast((
                                cast((
                                  calendar1.fisc_per
                                ) AS VARCHAR)
                              ) AS TEXT), 6)
                            ),
                            cast((
                              cast('yyyymm' AS VARCHAR)
                            ) AS TEXT)
                          ) -1
                        )
                      )
                    ) AS VARCHAR)
                  ) AS TEXT) || cast((
                    cast('0' AS VARCHAR)
                  ) AS TEXT)
                ) || cast((
                  cast((
                    DATE_PART(
                    'MONTH',
                      (
                        TO_DATE(
                          (
                            cast((
                              cast((
                                calendar1.fisc_yr
                              ) AS VARCHAR)
                            ) AS TEXT) || SUBSTRING(cast((
                              cast((
                                calendar1.fisc_per
                              ) AS VARCHAR)
                            ) AS TEXT), 6)
                          ),
                          cast((
                            cast('yyyymm' AS VARCHAR)
                          ) AS TEXT)
                        ) - 1
                      )
                    )
                  ) AS VARCHAR)
                ) AS TEXT)
              )
            ) AS VARCHAR) AS prev_fisc_yr_per,
            TO_CHAR(
            CONVERT_TIMEZONE(
              'Asia/Singapore',
              CURRENT_TIMESTAMP::TIMESTAMP_NTZ
            )::TIMESTAMP_NTZ,
            'YYYYMMDD'
        ) AS latest_date,
            cast((
              (
                cast((
                  cast((
                    calendar1.fisc_yr
                  ) AS VARCHAR)
                ) AS TEXT) || SUBSTRING(cast((
                  cast((
                    calendar1.fisc_per
                  ) AS VARCHAR)
                ) AS TEXT), 6)
              )
            ) AS VARCHAR) AS latest_fisc_yrmnth,
            cast((
              SUBSTRING(cast((
                cast((
                  cal.fisc_per
                ) AS VARCHAR)
              ) AS TEXT), 1, 4)
            ) AS INT) AS fisc_yr,
            cal.fisc_per AS fisc_yr_per,
            TO_DATE(
              (
                (
                  SUBSTRING(cast((
                    cast((
                      cal.fisc_per
                    ) AS VARCHAR)
                  ) AS TEXT), 6, 8) || cast((
                    cast('01' AS VARCHAR)
                  ) AS TEXT)
                ) || SUBSTRING(cast((
                  cast((
                    cal.fisc_per
                  ) AS VARCHAR)
                ) AS TEXT), 1, 4)
              ),
              cast((
                cast('MMDDYYYY' AS VARCHAR)
              ) AS TEXT)
            ) AS fisc_day,
            cmp.ctry_group AS ctry_nm,
            cmp."cluster",
            invc.curr_key AS obj_crncy_co_obj,
            invc.matl_num,
            invc.co_cd,
            invc.sls_org,
            invc.dstr_chnl,
            invc.div,
            invc.cust_num,
            SUM(0) AS nts_usd,
            SUM(0) AS nts_lcy,
            SUM(0) AS gts_usd,
            SUM(0) AS gts_lcy,
            SUM(0) AS eq_usd,
            SUM(0) AS eq_lcy,
            cast('N/A' AS VARCHAR) AS from_crncy,
            cast('N/A' AS VARCHAR) AS to_crncy,
            SUM(0) AS nts_qty,
            SUM(0) AS gts_qty,
            SUM(0) AS eq_qty,
            SUM(invc.ord_pc_qty) AS ord_pc_qty,
            SUM(invc.unspp_qty) AS unspp_qty
          FROM (
            (
              (
                edw_invoice_fact AS invc
                  LEFT JOIN edw_company_dim AS cmp
                    ON (
                      (
                        cast((
                          invc.co_cd
                        ) AS TEXT) = cast((
                          cmp.co_cd
                        ) AS TEXT)
                      )
                    )
              )
              LEFT JOIN edw_calendar_dim AS cal
                ON (
                  (
                    invc.rqst_delv_dt = cal.cal_day
                  )
                )
            )
            LEFT JOIN edw_calendar_dim AS calendar1
              ON (
                (
                  calendar1.cal_day = TO_DATE(
                      TO_CHAR(
              CONVERT_TIMEZONE(
              'Asia/Singapore',
          CURRENT_TIMESTAMP::TIMESTAMP_NTZ
          )::TIMESTAMP_NTZ,
          'YYYY-MM-DD'
            )::TEXT,
            'YYYY-MM-DD'
                  )
                )
              )
          )
          WHERE
            (
              cast((
                cast((
                  cal.fisc_per
                ) AS VARCHAR)
              ) AS TEXT) >= (
                (
                  (
                    cast((
                      cast((
                        (
                          DATE_PART(YEAR, CURRENT_DATE()) - 2
                        )
                      ) AS VARCHAR)
                    ) AS TEXT) || cast((
                      cast((
                        0
                      ) AS VARCHAR)
                    ) AS TEXT)
                  ) || cast((
                    cast((
                      0
                    ) AS VARCHAR)
                  ) AS TEXT)
                ) || cast((
                  cast((
                    1
                  ) AS VARCHAR)
                ) AS TEXT)
              )
            )
          GROUP BY
            calendar1.fisc_yr,
            calendar1.fisc_per,
            cal.fisc_per,
            cmp.ctry_group,
            cmp."cluster",
            invc.curr_key,
            invc.matl_num,
            invc.co_cd,
            invc.sls_org,
            invc.dstr_chnl,
            invc.div,
            invc.cust_num
        ) AS main
        LEFT JOIN edw_material_dim AS mat
          ON (
            (
              cast((
                main.matl_num
              ) AS TEXT) = cast((
                mat.matl_num
              ) AS TEXT)
            )
          )
      )
      JOIN edw_company_dim AS company
        ON (
          (
            cast((
              main.co_cd
            ) AS TEXT) = cast((
              company.co_cd
            ) AS TEXT)
          )
        )
    )
    LEFT JOIN ASPEDW_ACCESS.v_edw_customer_sales_dim AS cus_sales_extn
      ON (
        (
          (
            (
              (
                cast((
                  main.sls_org
                ) AS TEXT) = cast((
                  cus_sales_extn.sls_org
                ) AS TEXT)
              )
              AND (
                cast((
                  main.dstr_chnl
                ) AS TEXT) = cast((
                  cus_sales_extn.dstr_chnl
                ) AS TEXT)
              )
            )
            AND (
              cast((
                main.div
              ) AS TEXT) = cast((
                cus_sales_extn.div
              ) AS TEXT)
            )
          )
          AND (
            cast((
              main.cust_num
            ) AS TEXT) = cast((
              cus_sales_extn.cust_num
            ) AS TEXT)
          )
        )
      )
  )
  GROUP BY
    main.prev_fisc_yr_per,
    main.latest_date,
    main.latest_fisc_yrmnth,
    main.fisc_yr,
    main.fisc_yr_per,
    main.fisc_day,
    main.ctry_nm,
    main."cluster",
    main.obj_crncy_co_obj,
    mat.mega_brnd_desc,
    mat.brnd_desc,
    mat.base_prod_desc,
    mat.varnt_desc,
    mat.put_up_desc,
    mat.prodh1_txtmd,
    mat.prodh2_txtmd,
    mat.prodh3_txtmd,
    mat.prodh4_txtmd,
    mat.prodh5_txtmd,
    mat.prodh6_txtmd,
    cus_sales_extn."parent customer",
    cus_sales_extn.banner,
    cus_sales_extn."banner format",
    cus_sales_extn.channel,
    cus_sales_extn."go to model",
    cus_sales_extn."sub channel",
    cus_sales_extn.retail_env,
    main.from_crncy,
    main.to_crncy,
    main.cust_num
  )

  select * from transformed
);
create or replace view aspedw_integration.v_rpt_copa_ciw
as
(
  with edw_copa_trans_fact as(
      select * from aspedw_integration.edw_copa_trans_fact
  ),
  edw_company_dim as(
      select * from aspedw_integration.edw_company_dim
  ),
  edw_material_dim as(
      select * from aspedw_integration.edw_material_dim
  ),
  v_edw_customer_sales_dim as(
      select * from aspedw_integration.v_edw_customer_sales_dim
  ),
  v_intrm_reg_crncy_exch_fiscper as(
      select * from aspedw_integration.v_intrm_reg_crncy_exch_fiscper
  ),
  edw_acct_ciw_hier as(
      select * from aspedw_integration.edw_acct_ciw_hier
  ),
  edw_account_ciw_dim as(
      select * from aspedw_integration.edw_account_ciw_dim
  ),

  edw_gch_producthierarchy as(
      select * from aspedw_integration.edw_gch_producthierarchy
  ),

  transformed as(
  SELECT
    fact.fisc_yr AS "fisc_yr",
    fact.fisc_yr_per AS "fisc_yr_per",
    fact.fisc_day AS "fisc_day",
    CASE
      WHEN (
        (
          (
            (
              (
                (
                  (
                    LTRIM(
                      CAST((
                        fact.cust_num
                      ) AS TEXT),
                      CAST((
                        CAST((
                          0
                        ) AS VARCHAR)
                      ) AS TEXT)
                    ) = CAST((
                      CAST('134559' AS VARCHAR)
                    ) AS TEXT)
                  )
                  OR (
                    LTRIM(
                      CAST((
                        fact.cust_num
                      ) AS TEXT),
                      CAST((
                        CAST((
                          0
                        ) AS VARCHAR)
                      ) AS TEXT)
                    ) = CAST((
                      CAST('134106' AS VARCHAR)
                    ) AS TEXT)
                  )
                )
                OR (
                  LTRIM(
                    CAST((
                      fact.cust_num
                    ) AS TEXT),
                    CAST((
                      CAST((
                        0
                      ) AS VARCHAR)
                    ) AS TEXT)
                  ) = CAST((
                    CAST('134258' AS VARCHAR)
                  ) AS TEXT)
                )
              )
              OR (
                LTRIM(
                  CAST((
                    fact.cust_num
                  ) AS TEXT),
                  CAST((
                    CAST((
                      0
                    ) AS VARCHAR)
                  ) AS TEXT)
                ) = CAST((
                  CAST('134855' AS VARCHAR)
                ) AS TEXT)
              )
            )
            AND (
              LTRIM(
                CAST((
                  fact.acct_num
                ) AS TEXT),
                CAST((
                  CAST((
                    0
                  ) AS VARCHAR)
                ) AS TEXT)
              ) <> CAST((
                CAST('403185' AS VARCHAR)
              ) AS TEXT)
            )
          )
          AND (
            CAST((
              mat.mega_brnd_desc
            ) AS TEXT) <> CAST((
              CAST('Vogue Int''l' AS VARCHAR)
            ) AS TEXT)
          )
        )
        AND (
          fact.fisc_yr = 2018
        )
      )
      THEN CAST('China Selfcare' AS VARCHAR)
      ELSE fact.ctry_group
    END AS "ctry_nm",
    CASE
      WHEN (
        (
          (
            (
              (
                (
                  (
                    LTRIM(
                      CAST((
                        fact.cust_num
                      ) AS TEXT),
                      CAST((
                        CAST((
                          0
                        ) AS VARCHAR)
                      ) AS TEXT)
                    ) = CAST((
                      CAST('134559' AS VARCHAR)
                    ) AS TEXT)
                  )
                  OR (
                    LTRIM(
                      CAST((
                        fact.cust_num
                      ) AS TEXT),
                      CAST((
                        CAST((
                          0
                        ) AS VARCHAR)
                      ) AS TEXT)
                    ) = CAST((
                      CAST('134106' AS VARCHAR)
                    ) AS TEXT)
                  )
                )
                OR (
                  LTRIM(
                    CAST((
                      fact.cust_num
                    ) AS TEXT),
                    CAST((
                      CAST((
                        0
                      ) AS VARCHAR)
                    ) AS TEXT)
                  ) = CAST((
                    CAST('134258' AS VARCHAR)
                  ) AS TEXT)
                )
              )
              OR (
                LTRIM(
                  CAST((
                    fact.cust_num
                  ) AS TEXT),
                  CAST((
                    CAST((
                      0
                    ) AS VARCHAR)
                  ) AS TEXT)
                ) = CAST((
                  CAST('134855' AS VARCHAR)
                ) AS TEXT)
              )
            )
            AND (
              LTRIM(
                CAST((
                  fact.acct_num
                ) AS TEXT),
                CAST((
                  CAST((
                    0
                  ) AS VARCHAR)
                ) AS TEXT)
              ) <> CAST((
                CAST('403185' AS VARCHAR)
              ) AS TEXT)
            )
          )
          AND (
            CAST((
              mat.mega_brnd_desc
            ) AS TEXT) <> CAST((
              CAST('Vogue Int''l' AS VARCHAR)
            ) AS TEXT)
          )
        )
        AND (
          fact.fisc_yr = 2018
        )
      )
      THEN CAST('China' AS VARCHAR)
      ELSE fact."cluster"
    END AS "cluster",
    fact.obj_crncy_co_obj AS "obj_crncy_co_obj",
    fact.from_crncy AS "from_crncy",
    ciw.acct_nm AS "acct_nm",
    LTRIM(CAST((
      ciw.acct_num
    ) AS TEXT), CAST((
      CAST((
        0
      ) AS VARCHAR)
    ) AS TEXT)) AS "acct_num",
    ciw.ciw_desc AS "ciw_desc",
    ciw.ciw_bucket AS "ciw_bucket",
    csw.csw_desc AS "csw_desc",
    mat.mega_brnd_desc AS "b1 mega-brand",
    mat.brnd_desc AS "b2 brand",
    mat.base_prod_desc AS "b3 base product",
    mat.varnt_desc AS "b4 variant",
    mat.put_up_desc AS "b5 put-up",
    fact.cust_num AS "cust_num",
    cus_sales_extn."parent customer",
    cus_sales_extn.banner AS "banner",
    cus_sales_extn."banner format",
    cus_sales_extn.channel AS "channel",
    cus_sales_extn."go to model",
    cus_sales_extn."sub channel",
    cus_sales_extn.retail_env AS "retail_env",
    gph.gcph_franchise AS "gcph_franchise",
    gph.gcph_brand AS "gcph_brand",
    gph.gcph_subbrand AS "gcph_subbrand",
    gph.gcph_variant AS "gcph_variant",
    gph.put_up_description AS "put_up_description",
    gph.gcph_needstate AS "gcph_needstate",
    gph.gcph_category AS "gcph_category",
    gph.gcph_subcategory AS "gcph_subcategory",
    gph.gcph_segment AS "gcph_segment",
    gph.gcph_subsegment AS "gcph_subsegment",
    gch.gcch_total_enterprise AS "gcch_total_enterprise",
    gch.gcch_retail_banner AS "gcch_retail_banner",
    gch.primary_format AS "gcch_primary_format",
    gch.distributor_attribute AS "gcch_distributor_attribute",
    fact.acct_hier_shrt_desc AS "acct_hier_shrt_desc",
    SUM(fact.qty) AS "qty",
    SUM(fact.amt_lcy) AS "amt_lcy",
    SUM(fact.amt_usd) AS "amt_usd"
  FROM (
    (
      (
        (
          (
            (
              (
                SELECT
                  copa.fisc_yr,
                  copa.fisc_yr_per,
                  TO_DATE(
                    (
                      (
                        SUBSTRING(CAST((
                          CAST((
                            copa.fisc_yr_per
                          ) AS VARCHAR)
                        ) AS TEXT), 6, 8) || CAST((
                          CAST('01' AS VARCHAR)
                        ) AS TEXT)
                      ) || SUBSTRING(CAST((
                        CAST((
                          copa.fisc_yr_per
                        ) AS VARCHAR)
                      ) AS TEXT), 1, 4)
                    ),
                    CAST((
                      CAST('MMDDYYYY' AS VARCHAR)
                    ) AS TEXT)
                  ) AS fisc_day,
                  company.ctry_group,
                  company."cluster",
                  copa.acct_num,
                  copa.obj_crncy_co_obj,
                  copa.matl_num,
                  copa.co_cd,
                  CASE
                    WHEN (
                      (
                        LTRIM(CAST((
                          copa.cust_num
                        ) AS TEXT), CAST((
                          CAST('0' AS VARCHAR)
                        ) AS TEXT)) = CAST((
                          CAST('135520' AS VARCHAR)
                        ) AS TEXT)
                      )
                      AND (
                        (
                          CAST((
                            copa.co_cd
                          ) AS TEXT) = CAST((
                            CAST('703A' AS VARCHAR)
                          ) AS TEXT)
                        )
                        OR (
                          CAST((
                            copa.co_cd
                          ) AS TEXT) = CAST((
                            CAST('8888' AS VARCHAR)
                          ) AS TEXT)
                        )
                      )
                    )
                    THEN CAST('100A' AS VARCHAR)
                    ELSE copa.sls_org
                  END AS sls_org,
                  CASE
                    WHEN (
                      (
                        LTRIM(CAST((
                          copa.cust_num
                        ) AS TEXT), CAST((
                          CAST('0' AS VARCHAR)
                        ) AS TEXT)) = CAST((
                          CAST('135520' AS VARCHAR)
                        ) AS TEXT)
                      )
                      AND (
                        (
                          CAST((
                            copa.co_cd
                          ) AS TEXT) = CAST((
                            CAST('703A' AS VARCHAR)
                          ) AS TEXT)
                        )
                        OR (
                          CAST((
                            copa.co_cd
                          ) AS TEXT) = CAST((
                            CAST('8888' AS VARCHAR)
                          ) AS TEXT)
                        )
                      )
                    )
                    THEN CAST('15' AS VARCHAR)
                    ELSE copa.dstr_chnl
                  END AS dstr_chnl,
                  copa.div,
                  copa.cust_num,
                  CASE
                    WHEN (
                      CAST((
                        company.ctry_group
                      ) AS TEXT) = CAST((
                        CAST('India' AS VARCHAR)
                      ) AS TEXT)
                    )
                    THEN CAST('INR' AS VARCHAR)
                    WHEN (
                      CAST((
                        company.ctry_group
                      ) AS TEXT) = CAST((
                        CAST('Philippines' AS VARCHAR)
                      ) AS TEXT)
                    )
                    THEN CAST('PHP' AS VARCHAR)
                    WHEN (
                      (
                        CAST((
                          company.ctry_group
                        ) AS TEXT) = CAST((
                          CAST('China Selfcare' AS VARCHAR)
                        ) AS TEXT)
                      )
                      OR (
                        CAST((
                          company.ctry_group
                        ) AS TEXT) = CAST((
                          CAST('China Personal Care' AS VARCHAR)
                        ) AS TEXT)
                      )
                    )
                    THEN CAST('RMB' AS VARCHAR)
                    WHEN (
                      CAST((
                        company.ctry_group
                      ) AS TEXT) = CAST((
                        CAST('Australia' AS VARCHAR)
                      ) AS TEXT)
                    )
                    THEN CAST('AUD' AS VARCHAR)
                    WHEN (
                      CAST((
                        company.ctry_group
                      ) AS TEXT) = CAST((
                        CAST('New Zealand' AS VARCHAR)
                      ) AS TEXT)
                    )
                    THEN CAST('NZD' AS VARCHAR)
                    WHEN (
                      CAST((
                        company.ctry_group
                      ) AS TEXT) = CAST((
                        CAST('APSC Regional' AS VARCHAR)
                      ) AS TEXT)
                    )
                    THEN CAST('RMB' AS VARCHAR)
                    ELSE exch_rate.from_crncy
                  END AS from_crncy,
                  copa.acct_hier_shrt_desc,
                  CASE
                    WHEN (
                      CAST((
                        exch_rate.to_crncy
                      ) AS TEXT) = CAST((
                        CAST('USD' AS VARCHAR)
                      ) AS TEXT)
                    )
                    THEN SUM(copa.qty)
                    ELSE CAST((
                      CAST((
                        0
                      ) AS DECIMAL)
                    ) AS DECIMAL(18, 0))
                  END AS qty,
                  CASE
                    WHEN (
                      CAST((
                        exch_rate.to_crncy
                      ) AS TEXT) = CAST((
                        CASE
                          WHEN (
                            (
                              CAST((
                                company.ctry_group
                              ) AS TEXT) = CAST((
                                CAST('India' AS VARCHAR)
                              ) AS TEXT)
                            )
                            OR (
                              (
                                company.ctry_group IS NULL
                              ) AND (
                                'India' IS NULL
                              )
                            )
                          )
                          THEN CAST('INR' AS VARCHAR)
                          WHEN (
                            (
                              CAST((
                                company.ctry_group
                              ) AS TEXT) = CAST((
                                CAST('Philippines' AS VARCHAR)
                              ) AS TEXT)
                            )
                            OR (
                              (
                                company.ctry_group IS NULL
                              ) AND (
                                'Philippines' IS NULL
                              )
                            )
                          )
                          THEN CAST('PHP' AS VARCHAR)
                          WHEN (
                            (
                              CAST((
                                company.ctry_group
                              ) AS TEXT) = CAST((
                                CAST('China Selfcare' AS VARCHAR)
                              ) AS TEXT)
                            )
                            OR (
                              (
                                company.ctry_group IS NULL
                              ) AND (
                                'China Selfcare' IS NULL
                              )
                            )
                          )
                          THEN CAST('RMB' AS VARCHAR)
                          WHEN (
                            (
                              CAST((
                                company.ctry_group
                              ) AS TEXT) = CAST((
                                CAST('China Personal Care' AS VARCHAR)
                              ) AS TEXT)
                            )
                            OR (
                              (
                                company.ctry_group IS NULL
                              ) AND (
                                'China Personal Care' IS NULL
                              )
                            )
                          )
                          THEN CAST('RMB' AS VARCHAR)
                          WHEN (
                            (
                              CAST((
                                company.ctry_group
                              ) AS TEXT) = CAST((
                                CAST('Australia' AS VARCHAR)
                              ) AS TEXT)
                            )
                            OR (
                              (
                                company.ctry_group IS NULL
                              ) AND (
                                'Australia' IS NULL
                              )
                            )
                          )
                          THEN CAST('AUD' AS VARCHAR)
                          WHEN (
                            (
                              CAST((
                                company.ctry_group
                              ) AS TEXT) = CAST((
                                CAST('New Zealand' AS VARCHAR)
                              ) AS TEXT)
                            )
                            OR (
                              (
                                company.ctry_group IS NULL
                              ) AND (
                                'New Zealand' IS NULL
                              )
                            )
                          )
                          THEN CAST('NZD' AS VARCHAR)
                          WHEN (
                            (
                              CAST((
                                company.ctry_group
                              ) AS TEXT) = CAST((
                                CAST('APSC Regional' AS VARCHAR)
                              ) AS TEXT)
                            )
                            OR (
                              (
                                company.ctry_group IS NULL
                              ) AND (
                                'APSC Regional' IS NULL
                              )
                            )
                          )
                          THEN CAST('RMB' AS VARCHAR)
                          ELSE copa.obj_crncy_co_obj
                        END
                      ) AS TEXT)
                    )
                    THEN SUM((
                      copa.amt_obj_crncy * exch_rate.ex_rt
                    ))
                    ELSE CAST((
                      CAST((
                        0
                      ) AS DECIMAL)
                    ) AS DECIMAL(18, 0))
                  END AS amt_lcy,
                  CASE
                    WHEN (
                      CAST((
                        exch_rate.to_crncy
                      ) AS TEXT) = CAST((
                        CAST('USD' AS VARCHAR)
                      ) AS TEXT)
                    )
                    THEN SUM((
                      copa.amt_obj_crncy * exch_rate.ex_rt
                    ))
                    ELSE CAST((
                      CAST((
                        0
                      ) AS DECIMAL)
                    ) AS DECIMAL(18, 0))
                  END AS amt_usd
                FROM (
                  (
                    edw_copa_trans_fact AS copa
                      JOIN edw_company_dim AS company
                        ON (
                          (
                            CAST((
                              copa.co_cd
                            ) AS TEXT) = CAST((
                              company.co_cd
                            ) AS TEXT)
                          )
                        )
                  )
                  LEFT JOIN v_intrm_reg_crncy_exch_fiscper AS exch_rate
                    ON (
                      (
                        (
                          (
                            CAST((
                              copa.obj_crncy_co_obj
                            ) AS TEXT) = CAST((
                              exch_rate.from_crncy
                            ) AS TEXT)
                          )
                          AND (
                            copa.fisc_yr_per = exch_rate.fisc_per
                          )
                        )
                        AND CASE
                          WHEN (
                            CAST((
                              exch_rate.to_crncy
                            ) AS TEXT) <> CAST((
                              CAST('USD' AS VARCHAR)
                            ) AS TEXT)
                          )
                          THEN (
                            CAST((
                              exch_rate.to_crncy
                            ) AS TEXT) = CAST((
                              CASE
                                WHEN (
                                  (
                                    CAST((
                                      company.ctry_group
                                    ) AS TEXT) = CAST((
                                      CAST('India' AS VARCHAR)
                                    ) AS TEXT)
                                  )
                                  OR (
                                    (
                                      company.ctry_group IS NULL
                                    ) AND (
                                      'India' IS NULL
                                    )
                                  )
                                )
                                THEN CAST('INR' AS VARCHAR)
                                WHEN (
                                  (
                                    CAST((
                                      company.ctry_group
                                    ) AS TEXT) = CAST((
                                      CAST('Philippines' AS VARCHAR)
                                    ) AS TEXT)
                                  )
                                  OR (
                                    (
                                      company.ctry_group IS NULL
                                    ) AND (
                                      'Philippines' IS NULL
                                    )
                                  )
                                )
                                THEN CAST('PHP' AS VARCHAR)
                                WHEN (
                                  (
                                    CAST((
                                      company.ctry_group
                                    ) AS TEXT) = CAST((
                                      CAST('China Selfcare' AS VARCHAR)
                                    ) AS TEXT)
                                  )
                                  OR (
                                    (
                                      company.ctry_group IS NULL
                                    ) AND (
                                      'China Selfcare' IS NULL
                                    )
                                  )
                                )
                                THEN CAST('RMB' AS VARCHAR)
                                WHEN (
                                  (
                                    CAST((
                                      company.ctry_group
                                    ) AS TEXT) = CAST((
                                      CAST('China Personal Care' AS VARCHAR)
                                    ) AS TEXT)
                                  )
                                  OR (
                                    (
                                      company.ctry_group IS NULL
                                    ) AND (
                                      'China Personal Care' IS NULL
                                    )
                                  )
                                )
                                THEN CAST('RMB' AS VARCHAR)
                                WHEN (
                                  (
                                    CAST((
                                      company.ctry_group
                                    ) AS TEXT) = CAST((
                                      CAST('Australia' AS VARCHAR)
                                    ) AS TEXT)
                                  )
                                  OR (
                                    (
                                      company.ctry_group IS NULL
                                    ) AND (
                                      'Australia' IS NULL
                                    )
                                  )
                                )
                                THEN CAST('AUD' AS VARCHAR)
                                WHEN (
                                  (
                                    CAST((
                                      company.ctry_group
                                    ) AS TEXT) = CAST((
                                      CAST('New Zealand' AS VARCHAR)
                                    ) AS TEXT)
                                  )
                                  OR (
                                    (
                                      company.ctry_group IS NULL
                                    ) AND (
                                      'New Zealand' IS NULL
                                    )
                                  )
                                )
                                THEN CAST('NZD' AS VARCHAR)
                                WHEN (
                                  (
                                    CAST((
                                      company.ctry_group
                                    ) AS TEXT) = CAST((
                                      CAST('APSC Regional' AS VARCHAR)
                                    ) AS TEXT)
                                  )
                                  OR (
                                    (
                                      company.ctry_group IS NULL
                                    ) AND (
                                      'APSC Regional' IS NULL
                                    )
                                  )
                                )
                                THEN CAST('RMB' AS VARCHAR)
                                ELSE copa.obj_crncy_co_obj
                              END
                            ) AS TEXT)
                          )
                          ELSE (
                            CAST((
                              exch_rate.to_crncy
                            ) AS TEXT) = CAST((
                              CAST('USD' AS VARCHAR)
                            ) AS TEXT)
                          )
                        END
                      )
                    )
                )
                WHERE
                  (
                    (
                      CAST((
                        copa.acct_hier_shrt_desc
                      ) AS TEXT) = CAST((
                        CAST('NTS' AS VARCHAR)
                      ) AS TEXT)
                    )
                    AND (
                      CAST((
                        CAST((
                          copa.fisc_yr_per
                        ) AS VARCHAR)
                      ) AS TEXT) >= (
                        (
                          (
                            CAST((
                              CAST((
                                (
                                DATE_PART(YEAR, to_date('2020-02-28 06:02:34.194913')) - 2 
                                )
                              ) AS VARCHAR)
                            ) AS TEXT) || CAST((
                              CAST((
                                0
                              ) AS VARCHAR)
                            ) AS TEXT)
                          ) || CAST((
                            CAST((
                              0
                            ) AS VARCHAR)
                          ) AS TEXT)
                        ) || CAST((
                          CAST((
                            1
                          ) AS VARCHAR)
                        ) AS TEXT)
                      )
                    )
                  )
                GROUP BY
                  company.ctry_group,
                  company."cluster",
                  copa.fisc_yr,
                  copa.fisc_yr_per,
                  copa.obj_crncy_co_obj,
                  copa.acct_num,
                  copa.matl_num,
                  copa.co_cd,
                  copa.sls_org,
                  copa.dstr_chnl,
                  copa.div,
                  copa.cust_num,
                  copa.acct_hier_shrt_desc,
                  exch_rate.from_crncy,
                  exch_rate.to_crncy
              ) AS fact
              LEFT JOIN edw_acct_ciw_hier AS ciw
                ON (
                  (
                    (
                      LTRIM(
                        CAST((
                          fact.acct_num
                        ) AS TEXT),
                        CAST((
                          CAST((
                            0
                          ) AS VARCHAR)
                        ) AS TEXT)
                      ) = LTRIM(CAST((
                        ciw.acct_num
                      ) AS TEXT), CAST((
                        CAST((
                          0
                        ) AS VARCHAR)
                      ) AS TEXT))
                    )
                    AND (
                      CAST((
                        fact.acct_hier_shrt_desc
                      ) AS TEXT) = CAST((
                        ciw.measure_code
                      ) AS TEXT)
                    )
                  )
                )
            )
            LEFT JOIN (
              SELECT
                a.acct_num,
                b.csw_acct_hier_name AS csw_desc
              FROM (
                (
                  SELECT
                    edw_account_ciw_dim.acct_num,
                    edw_account_ciw_dim.bravo_acct_l3,
                    edw_account_ciw_dim.bravo_acct_l4
                  FROM edw_account_ciw_dim
                  WHERE
                    (
                      (
                        CAST((
                          edw_account_ciw_dim.chrt_acct
                        ) AS TEXT) = CAST((
                          CAST('CCOA' AS VARCHAR)
                        ) AS TEXT)
                      )
                      AND (
                        CAST((
                          edw_account_ciw_dim.bravo_acct_l2
                        ) AS TEXT) = CAST((
                          CAST('JJPLAC510001' AS VARCHAR)
                        ) AS TEXT)
                      )
                    )
                ) AS a
                LEFT JOIN (
                  (
                    (
                      (
                        SELECT
                          CAST('JJPLAC512200' AS VARCHAR) AS csw_acct_hier_no,
                          CAST('Sales Return' AS VARCHAR) AS csw_acct_hier_name
                        UNION ALL
                        SELECT
                          CAST('JJPLAC512001' AS VARCHAR) AS csw_acct_hier_no,
                          CAST('Sales Discount & Reserve' AS VARCHAR) AS csw_acct_hier_name
                      )
                      UNION ALL
                      SELECT
                        CAST('JJPLAC513001' AS VARCHAR) AS csw_acct_hier_no,
                        CAST('Sales Incentive' AS VARCHAR) AS csw_acct_hier_name
                    )
                    UNION ALL
                    SELECT
                      CAST('JJPLAC514001' AS VARCHAR) AS csw_acct_hier_no,
                      CAST('Promo & Trade related' AS VARCHAR) AS csw_acct_hier_name
                  )
                  UNION ALL
                  SELECT
                    CAST('JJPLAC511000' AS VARCHAR) AS csw_acct_hier_no,
                    CAST('Gross Trade Prod Sales' AS VARCHAR) AS csw_acct_hier_name
                ) AS b
                  ON (
                    CASE
                      WHEN (
                        CAST((
                          a.bravo_acct_l4
                        ) AS TEXT) = CAST((
                          CAST('JJPLAC512200' AS VARCHAR)
                        ) AS TEXT)
                      )
                      THEN (
                        CAST((
                          b.csw_acct_hier_no
                        ) AS TEXT) = CAST((
                          a.bravo_acct_l4
                        ) AS TEXT)
                      )
                      ELSE (
                        CAST((
                          b.csw_acct_hier_no
                        ) AS TEXT) = CAST((
                          a.bravo_acct_l3
                        ) AS TEXT)
                      )
                    END
                  )
              )
            ) AS csw
              ON (
                (
                  LTRIM(
                    CAST((
                      fact.acct_num
                    ) AS TEXT),
                    CAST((
                      CAST((
                        0
                      ) AS VARCHAR)
                    ) AS TEXT)
                  ) = LTRIM(CAST((
                    csw.acct_num
                  ) AS TEXT), CAST((
                    CAST((
                      0
                    ) AS VARCHAR)
                  ) AS TEXT))
                )
              )
          )
          LEFT JOIN edw_material_dim AS mat
            ON (
              (
                CAST((
                  fact.matl_num
                ) AS TEXT) = CAST((
                  mat.matl_num
                ) AS TEXT)
              )
            )
        )
        LEFT JOIN v_edw_customer_sales_dim AS cus_sales_extn
          ON (
            (
              (
                (
                  (
                    CAST((
                      fact.sls_org
                    ) AS TEXT) = CAST((
                      cus_sales_extn.sls_org
                    ) AS TEXT)
                  )
                  AND (
                    CAST((
                      fact.dstr_chnl
                    ) AS TEXT) = CAST((
                      cus_sales_extn.dstr_chnl
                    ) AS TEXT)
                  )
                )
                AND (
                  CAST((
                    fact.div
                  ) AS TEXT) = CAST((
                    cus_sales_extn.div
                  ) AS TEXT)
                )
              )
              AND (
                CAST((
                  fact.cust_num
                ) AS TEXT) = CAST((
                  cus_sales_extn.cust_num
                ) AS TEXT)
              )
            )
          )
      )
      LEFT JOIN edw_gch_producthierarchy AS gph
        ON (
          (
            CAST((
              fact.matl_num
            ) AS TEXT) = CAST((
              gph.materialnumber
            ) AS TEXT)
          )
        )
    )
    LEFT JOIN edw_gch_customerhierarchy AS gch
      ON (
        (
          CAST((
            fact.cust_num
          ) AS TEXT) = CAST((
            gch.customer
          ) AS TEXT)
        )
      )
  )
  GROUP BY
    fact.fisc_yr,
    fact.fisc_yr_per,
    fact.fisc_day,
    fact.ctry_group,
    fact."cluster",
    fact.obj_crncy_co_obj,
    fact.from_crncy,
    ciw.acct_nm,
    ciw.acct_num,
    ciw.ciw_desc,
    ciw.ciw_bucket,
    csw.csw_desc,
    mat.mega_brnd_desc,
    mat.brnd_desc,
    mat.base_prod_desc,
    mat.varnt_desc,
    mat.put_up_desc,
    fact.cust_num,
    fact.acct_num,
    cus_sales_extn."parent customer",
    cus_sales_extn.banner,
    cus_sales_extn."banner format",
    cus_sales_extn.channel,
    cus_sales_extn."go to model",
    cus_sales_extn."sub channel",
    cus_sales_extn.retail_env,
    gph.gcph_franchise,
    gph.gcph_brand,
    gph.gcph_subbrand,
    gph.gcph_variant,
    gph.put_up_description,
    gph.gcph_needstate,
    gph.gcph_category,
    gph.gcph_subcategory,
    gph.gcph_segment,
    gph.gcph_subsegment,
    gch.gcch_total_enterprise,
    gch.gcch_retail_banner,
    gch.primary_format,
    gch.distributor_attribute,
    fact.acct_hier_shrt_desc
  )

  select * from transformed
);
create or replace view aspedw_integration.v_rpt_copa_sku
as
(
  with edw_copa_trans_fact as(
      select * from aspedw_integration.edw_copa_trans_fact
  ),
  edw_company_dim as(
      select * from aspedw_integration.edw_company_dim
  ),
  edw_material_dim as(
      select * from aspedw_integration.edw_material_dim
  ),
  v_edw_customer_sales_dim as(
      select * from aspedw_integration.v_edw_customer_sales_dim
  ),
  edw_vw_greenlight_skus as(
      select * from aspedw_integration.edw_vw_greenlight_skus
  ),
  v_intrm_reg_crncy_exch_fiscper as(
      select * from aspedw_integration.v_intrm_reg_crncy_exch_fiscper
  ),
  transformed as(
  SELECT
    derived_table1.fisc_yr AS "fisc_yr",
    derived_table1.fisc_yr_per AS "fisc_yr_per",
    derived_table1.fisc_day AS "fisc_day",
    derived_table1.ctry_nm AS "ctry_nm",
    derived_table1."cluster" as "cluster",
    derived_table1.obj_crncy_co_obj AS "obj_crncy_co_obj",
    derived_table1."b1 mega-brand" as "b1 mega-brand",
    derived_table1."b2 brand" as "b2 brand",
    derived_table1."b3 base product" as "b3 base product",
    derived_table1."b4 variant" as "b4 variant",
    derived_table1."b5 put-up" as "b5 put-up",
    derived_table1."prod h1 operating group" as "prod h1 operating group",
    derived_table1."prod h2 franchise group" as "prod h2 franchise group",
    derived_table1."prod h3 franchise" as "prod h3 franchise",
    derived_table1."prod h4 product franchise" as "prod h4 product franchise",
    derived_table1."prod h5 product major" as "prod h5 product major",
    derived_table1."prod h6 product minor" as "prod h6 product minor",
    derived_table1.pka_franchise_desc AS "pka_franchise_desc",
    derived_table1.pka_brand_desc AS "pka_brand_desc",
    derived_table1.pka_sub_brand_desc AS "pka_sub_brand_desc",
    derived_table1.pka_variant_desc AS "pka_variant_desc",
    derived_table1.pka_sub_variant_desc AS "pka_sub_variant_desc",
    derived_table1.pka_flavor_desc AS "pka_flavor_desc",
    derived_table1.pka_ingredient_desc AS "pka_ingredient_desc",
    derived_table1.pka_application_desc AS "pka_application_desc",
    derived_table1.pka_length_desc AS "pka_length_desc",
    derived_table1.pka_shape_desc AS "pka_shape_desc",
    derived_table1.pka_spf_desc AS "pka_spf_desc",
    derived_table1.pka_cover_desc AS "pka_cover_desc",
    derived_table1.pka_form_desc AS "pka_form_desc",
    derived_table1.pka_size_desc AS "pka_size_desc",
    derived_table1.pka_character_desc AS "pka_character_desc",
    derived_table1.pka_package_desc AS "pka_package_desc",
    derived_table1.pka_attribute_13_desc AS "pka_attribute_13_desc",
    derived_table1.pka_attribute_14_desc AS "pka_attribute_14_desc",
    derived_table1.pka_sku_identification_desc AS "pka_sku_identification_desc",
    derived_table1.pka_one_time_relabeling_desc AS "pka_one_time_relabeling_desc",
    derived_table1.pka_product_key AS "pka_product_key",
    derived_table1.pka_product_key_description AS "pka_product_key_description",
    derived_table1.pka_product_key_description_2 AS "pka_product_key_description_2",
    derived_table1.pka_root_code AS "pka_root_code",
    derived_table1.pka_root_code_desc_1 AS "pka_root_code_desc_1",
    derived_table1.pka_root_code_desc_2 AS "pka_root_code_desc_2",
    derived_table1.greenlight_sku_flag AS "greenlight_sku_flag",
    derived_table1."parent customer" as "parent customer",
    derived_table1.banner AS "banner",
    derived_table1."banner format",
    derived_table1.channel AS "channel",
    derived_table1."go to model" as "go to model",
    derived_table1."sub channel" as "sub channel",
    derived_table1.retail_env AS "retail_env",
    SUM(derived_table1.nts_usd) AS "nts_usd",
    SUM(derived_table1.nts_lcy) AS "nts_lcy",
    SUM(derived_table1.gts_usd) AS "gts_usd",
    SUM(derived_table1.gts_lcy) AS "gts_lcy",
    SUM(derived_table1.eq_usd) AS "eq_usd",
    SUM(derived_table1.eq_lcy) AS "eq_lcy",
    derived_table1.from_crncy AS "from_crncy",
    derived_table1.to_crncy AS "to_crncy",
    SUM(derived_table1.nts_qty) AS "nts_qty",
    SUM(derived_table1.gts_qty) AS "gts_qty",
    SUM(derived_table1.eq_qty) AS "eq_qty",
    derived_table1."product code" as "product code",
    derived_table1."product name" as "product name"
  FROM (
    SELECT
      copa.fisc_yr,
      copa.fisc_yr_per,
      TO_DATE(
        (
          (
            SUBSTRING(CAST((
              CAST((
                copa.fisc_yr_per
              ) AS VARCHAR)
            ) AS TEXT), 6, 8) || CAST((
              CAST('01' AS VARCHAR)
            ) AS TEXT)
          ) || SUBSTRING(CAST((
            CAST((
              copa.fisc_yr_per
            ) AS VARCHAR)
          ) AS TEXT), 1, 4)
        ),
        CAST((
          CAST('MMDDYYYY' AS VARCHAR)
        ) AS TEXT)
      ) AS fisc_day,
      CASE
        WHEN (
          (
            (
              (
                (
                  (
                    LTRIM(
                      CAST((
                        copa.cust_num
                      ) AS TEXT),
                      CAST((
                        CAST((
                          0
                        ) AS VARCHAR)
                      ) AS TEXT)
                    ) = CAST((
                      CAST('134559' AS VARCHAR)
                    ) AS TEXT)
                  )
                  OR (
                    LTRIM(
                      CAST((
                        copa.cust_num
                      ) AS TEXT),
                      CAST((
                        CAST((
                          0
                        ) AS VARCHAR)
                      ) AS TEXT)
                    ) = CAST((
                      CAST('134106' AS VARCHAR)
                    ) AS TEXT)
                  )
                )
                OR (
                  LTRIM(
                    CAST((
                      copa.cust_num
                    ) AS TEXT),
                    CAST((
                      CAST((
                        0
                      ) AS VARCHAR)
                    ) AS TEXT)
                  ) = CAST((
                    CAST('134258' AS VARCHAR)
                  ) AS TEXT)
                )
              )
              OR (
                LTRIM(
                  CAST((
                    copa.cust_num
                  ) AS TEXT),
                  CAST((
                    CAST((
                      0
                    ) AS VARCHAR)
                  ) AS TEXT)
                ) = CAST((
                  CAST('134855' AS VARCHAR)
                ) AS TEXT)
              )
            )
            AND (
              LTRIM(
                CAST((
                  copa.acct_num
                ) AS TEXT),
                CAST((
                  CAST((
                    0
                  ) AS VARCHAR)
                ) AS TEXT)
              ) <> CAST((
                CAST('403185' AS VARCHAR)
              ) AS TEXT)
            )
          )
          AND (
            copa.fisc_yr = 2018
          )
        )
        THEN CAST('China Selfcare' AS VARCHAR)
        ELSE cmp.ctry_group
      END AS ctry_nm,
      CASE
        WHEN (
          (
            (
              (
                (
                  (
                    LTRIM(
                      CAST((
                        copa.cust_num
                      ) AS TEXT),
                      CAST((
                        CAST((
                          0
                        ) AS VARCHAR)
                      ) AS TEXT)
                    ) = CAST((
                      CAST('134559' AS VARCHAR)
                    ) AS TEXT)
                  )
                  OR (
                    LTRIM(
                      CAST((
                        copa.cust_num
                      ) AS TEXT),
                      CAST((
                        CAST((
                          0
                        ) AS VARCHAR)
                      ) AS TEXT)
                    ) = CAST((
                      CAST('134106' AS VARCHAR)
                    ) AS TEXT)
                  )
                )
                OR (
                  LTRIM(
                    CAST((
                      copa.cust_num
                    ) AS TEXT),
                    CAST((
                      CAST((
                        0
                      ) AS VARCHAR)
                    ) AS TEXT)
                  ) = CAST((
                    CAST('134258' AS VARCHAR)
                  ) AS TEXT)
                )
              )
              OR (
                LTRIM(
                  CAST((
                    copa.cust_num
                  ) AS TEXT),
                  CAST((
                    CAST((
                      0
                    ) AS VARCHAR)
                  ) AS TEXT)
                ) = CAST((
                  CAST('134855' AS VARCHAR)
                ) AS TEXT)
              )
            )
            AND (
              LTRIM(
                CAST((
                  copa.acct_num
                ) AS TEXT),
                CAST((
                  CAST((
                    0
                  ) AS VARCHAR)
                ) AS TEXT)
              ) <> CAST((
                CAST('403185' AS VARCHAR)
              ) AS TEXT)
            )
          )
          AND (
            copa.fisc_yr = 2018
          )
        )
        THEN CAST('China' AS VARCHAR)
        ELSE cmp."cluster"
      END AS "cluster",
      CASE
        WHEN (
          CAST((
            cmp.ctry_group
          ) AS TEXT) = CAST((
            CAST('India' AS VARCHAR)
          ) AS TEXT)
        )
        THEN CAST('INR' AS VARCHAR)
        WHEN (
          CAST((
            cmp.ctry_group
          ) AS TEXT) = CAST((
            CAST('Philippines' AS VARCHAR)
          ) AS TEXT)
        )
        THEN CAST('PHP' AS VARCHAR)
        WHEN (
          (
            CAST((
              cmp.ctry_group
            ) AS TEXT) = CAST((
              CAST('China Selfcare' AS VARCHAR)
            ) AS TEXT)
          )
          OR (
            CAST((
              cmp.ctry_group
            ) AS TEXT) = CAST((
              CAST('China Personal Care' AS VARCHAR)
            ) AS TEXT)
          )
        )
        THEN CAST('RMB' AS VARCHAR)
        ELSE copa.obj_crncy_co_obj
      END AS obj_crncy_co_obj,
      mat.mega_brnd_desc AS "b1 mega-brand",
      mat.brnd_desc AS "b2 brand",
      mat.base_prod_desc AS "b3 base product",
      mat.varnt_desc AS "b4 variant",
      mat.put_up_desc AS "b5 put-up",
      mat.prodh1_txtmd AS "prod h1 operating group",
      mat.prodh2_txtmd AS "prod h2 franchise group",
      mat.prodh3_txtmd AS "prod h3 franchise",
      mat.prodh4_txtmd AS "prod h4 product franchise",
      mat.prodh5_txtmd AS "prod h5 product major",
      mat.prodh6_txtmd AS "prod h6 product minor",
      mat.pka_franchise_desc,
      mat.pka_brand_desc,
      mat.pka_sub_brand_desc,
      mat.pka_variant_desc,
      mat.pka_sub_variant_desc,
      mat.pka_flavor_desc,
      mat.pka_ingredient_desc,
      mat.pka_application_desc,
      mat.pka_length_desc,
      mat.pka_shape_desc,
      mat.pka_spf_desc,
      mat.pka_cover_desc,
      mat.pka_form_desc,
      mat.pka_size_desc,
      mat.pka_character_desc,
      mat.pka_package_desc,
      mat.pka_attribute_13_desc,
      mat.pka_attribute_14_desc,
      mat.pka_sku_identification_desc,
      mat.pka_one_time_relabeling_desc,
      mat.pka_product_key,
      mat.pka_product_key_description,
      mat.pka_product_key_description_2,
      mat.pka_root_code,
      mat.pka_root_code_desc_1,
      mat.pka_root_code_desc_2,
      COALESCE(gn.greenlight_sku_flag, CAST('N/A' AS VARCHAR)) AS greenlight_sku_flag,
      cus_sales_extn."parent customer",
      cus_sales_extn.banner,
      cus_sales_extn."banner format",
      cus_sales_extn.channel,
      cus_sales_extn."go to model",
      cus_sales_extn."sub channel",
      cus_sales_extn.retail_env,
      CASE
        WHEN (
          (
            CAST((
              copa.acct_hier_shrt_desc
            ) AS TEXT) = CAST((
              CAST('NTS' AS VARCHAR)
            ) AS TEXT)
          )
          AND (
            CAST((
              exch_rate.to_crncy
            ) AS TEXT) = CAST((
              CAST('USD' AS VARCHAR)
            ) AS TEXT)
          )
        )
        THEN SUM((
          copa.amt_obj_crncy * exch_rate.ex_rt
        ))
        ELSE CAST((
          CAST((
            0
          ) AS DECIMAL)
        ) AS DECIMAL(18, 0))
      END AS nts_usd,
      CASE
        WHEN (
          (
            CAST((
              copa.acct_hier_shrt_desc
            ) AS TEXT) = CAST((
              CAST('NTS' AS VARCHAR)
            ) AS TEXT)
          )
          AND (
            CAST((
              exch_rate.to_crncy
            ) AS TEXT) = CAST((
              CASE
                WHEN (
                  (
                    CAST((
                      cmp.ctry_group
                    ) AS TEXT) = CAST((
                      CAST('India' AS VARCHAR)
                    ) AS TEXT)
                  )
                  OR (
                    (
                      cmp.ctry_group IS NULL
                    ) AND (
                      'India' IS NULL
                    )
                  )
                )
                THEN CAST('INR' AS VARCHAR)
                WHEN (
                  (
                    CAST((
                      cmp.ctry_group
                    ) AS TEXT) = CAST((
                      CAST('Philippines' AS VARCHAR)
                    ) AS TEXT)
                  )
                  OR (
                    (
                      cmp.ctry_group IS NULL
                    ) AND (
                      'Philippines' IS NULL
                    )
                  )
                )
                THEN CAST('PHP' AS VARCHAR)
                WHEN (
                  (
                    CAST((
                      cmp.ctry_group
                    ) AS TEXT) = CAST((
                      CAST('China Selfcare' AS VARCHAR)
                    ) AS TEXT)
                  )
                  OR (
                    (
                      cmp.ctry_group IS NULL
                    ) AND (
                      'China Selfcare' IS NULL
                    )
                  )
                )
                THEN CAST('RMB' AS VARCHAR)
                WHEN (
                  (
                    CAST((
                      cmp.ctry_group
                    ) AS TEXT) = CAST((
                      CAST('China Personal Care' AS VARCHAR)
                    ) AS TEXT)
                  )
                  OR (
                    (
                      cmp.ctry_group IS NULL
                    ) AND (
                      'China Personal Care' IS NULL
                    )
                  )
                )
                THEN CAST('RMB' AS VARCHAR)
                ELSE copa.obj_crncy_co_obj
              END
            ) AS TEXT)
          )
        )
        THEN SUM((
          copa.amt_obj_crncy * exch_rate.ex_rt
        ))
        ELSE CAST((
          CAST((
            0
          ) AS DECIMAL)
        ) AS DECIMAL(18, 0))
      END AS nts_lcy,
      CASE
        WHEN (
          (
            CAST((
              copa.acct_hier_shrt_desc
            ) AS TEXT) = CAST((
              CAST('GTS' AS VARCHAR)
            ) AS TEXT)
          )
          AND (
            CAST((
              exch_rate.to_crncy
            ) AS TEXT) = CAST((
              CAST('USD' AS VARCHAR)
            ) AS TEXT)
          )
        )
        THEN SUM((
          copa.amt_obj_crncy * exch_rate.ex_rt
        ))
        ELSE CAST((
          CAST((
            0
          ) AS DECIMAL)
        ) AS DECIMAL(18, 0))
      END AS gts_usd,
      CASE
        WHEN (
          (
            CAST((
              copa.acct_hier_shrt_desc
            ) AS TEXT) = CAST((
              CAST('GTS' AS VARCHAR)
            ) AS TEXT)
          )
          AND (
            CAST((
              exch_rate.to_crncy
            ) AS TEXT) = CAST((
              CASE
                WHEN (
                  (
                    CAST((
                      cmp.ctry_group
                    ) AS TEXT) = CAST((
                      CAST('India' AS VARCHAR)
                    ) AS TEXT)
                  )
                  OR (
                    (
                      cmp.ctry_group IS NULL
                    ) AND (
                      'India' IS NULL
                    )
                  )
                )
                THEN CAST('INR' AS VARCHAR)
                WHEN (
                  (
                    CAST((
                      cmp.ctry_group
                    ) AS TEXT) = CAST((
                      CAST('Philippines' AS VARCHAR)
                    ) AS TEXT)
                  )
                  OR (
                    (
                      cmp.ctry_group IS NULL
                    ) AND (
                      'Philippines' IS NULL
                    )
                  )
                )
                THEN CAST('PHP' AS VARCHAR)
                WHEN (
                  (
                    CAST((
                      cmp.ctry_group
                    ) AS TEXT) = CAST((
                      CAST('China Selfcare' AS VARCHAR)
                    ) AS TEXT)
                  )
                  OR (
                    (
                      cmp.ctry_group IS NULL
                    ) AND (
                      'China Selfcare' IS NULL
                    )
                  )
                )
                THEN CAST('RMB' AS VARCHAR)
                WHEN (
                  (
                    CAST((
                      cmp.ctry_group
                    ) AS TEXT) = CAST((
                      CAST('China Personal Care' AS VARCHAR)
                    ) AS TEXT)
                  )
                  OR (
                    (
                      cmp.ctry_group IS NULL
                    ) AND (
                      'China Personal Care' IS NULL
                    )
                  )
                )
                THEN CAST('RMB' AS VARCHAR)
                ELSE copa.obj_crncy_co_obj
              END
            ) AS TEXT)
          )
        )
        THEN SUM((
          copa.amt_obj_crncy * exch_rate.ex_rt
        ))
        ELSE CAST((
          CAST((
            0
          ) AS DECIMAL)
        ) AS DECIMAL(18, 0))
      END AS gts_lcy,
      CASE
        WHEN (
          (
            CAST((
              copa.acct_hier_shrt_desc
            ) AS TEXT) = CAST((
              CAST('EQ' AS VARCHAR)
            ) AS TEXT)
          )
          AND (
            CAST((
              exch_rate.to_crncy
            ) AS TEXT) = CAST((
              CAST('USD' AS VARCHAR)
            ) AS TEXT)
          )
        )
        THEN SUM((
          copa.amt_obj_crncy * exch_rate.ex_rt
        ))
        ELSE CAST((
          CAST((
            0
          ) AS DECIMAL)
        ) AS DECIMAL(18, 0))
      END AS eq_usd,
      CASE
        WHEN (
          (
            CAST((
              copa.acct_hier_shrt_desc
            ) AS TEXT) = CAST((
              CAST('EQ' AS VARCHAR)
            ) AS TEXT)
          )
          AND (
            CAST((
              exch_rate.to_crncy
            ) AS TEXT) = CAST((
              CASE
                WHEN (
                  (
                    CAST((
                      cmp.ctry_group
                    ) AS TEXT) = CAST((
                      CAST('India' AS VARCHAR)
                    ) AS TEXT)
                  )
                  OR (
                    (
                      cmp.ctry_group IS NULL
                    ) AND (
                      'India' IS NULL
                    )
                  )
                )
                THEN CAST('INR' AS VARCHAR)
                WHEN (
                  (
                    CAST((
                      cmp.ctry_group
                    ) AS TEXT) = CAST((
                      CAST('Philippines' AS VARCHAR)
                    ) AS TEXT)
                  )
                  OR (
                    (
                      cmp.ctry_group IS NULL
                    ) AND (
                      'Philippines' IS NULL
                    )
                  )
                )
                THEN CAST('PHP' AS VARCHAR)
                WHEN (
                  (
                    CAST(
                      cmp.ctry_group
                    AS TEXT) = CAST((
                      CAST('China Selfcare' AS VARCHAR)
                    ) AS TEXT)
                  )
                  OR (
                    (
                      cmp.ctry_group IS NULL
                    ) AND (
                      'China Selfcare' IS NULL
                    )
                  )
                )
                THEN CAST('RMB' AS VARCHAR)
                WHEN (
                  (
                    CAST((
                      cmp.ctry_group
                    ) AS TEXT) = CAST((
                      CAST('China Personal Care' AS VARCHAR)
                    ) AS TEXT)
                  )
                  OR (
                    (
                      cmp.ctry_group IS NULL
                    ) AND (
                      'China Personal Care' IS NULL
                    )
                  )
                )
                THEN CAST('RMB' AS VARCHAR)
                ELSE copa.obj_crncy_co_obj
              END
            ) AS TEXT)
          )
        )
        THEN SUM((
          copa.amt_obj_crncy * exch_rate.ex_rt
        ))
        ELSE CAST((
          CAST(NULL AS DECIMAL)
        ) AS DECIMAL(18, 0))
      END AS eq_lcy,
      CASE
        WHEN (
          CAST((
            cmp.ctry_group
          ) AS TEXT) = CAST((
            CAST('India' AS VARCHAR)
          ) AS TEXT)
        )
        THEN CAST('INR' AS VARCHAR)
        WHEN (
          CAST((
            cmp.ctry_group
          ) AS TEXT) = CAST((
            CAST('Philippines' AS VARCHAR)
          ) AS TEXT)
        )
        THEN CAST('PHP' AS VARCHAR)
        WHEN (
          (
            CAST((
              cmp.ctry_group
            ) AS TEXT) = CAST((
              CAST('China Selfcare' AS VARCHAR)
            ) AS TEXT)
          )
          OR (
            CAST((
              cmp.ctry_group
            ) AS TEXT) = CAST((
              CAST('China Personal Care' AS VARCHAR)
            ) AS TEXT)
          )
        )
        THEN CAST('RMB' AS VARCHAR)
        ELSE exch_rate.from_crncy
      END AS from_crncy,
      CAST('USD' AS VARCHAR) AS to_crncy,
      CASE
        WHEN (
          (
            CAST((
              copa.acct_hier_shrt_desc
            ) AS TEXT) = CAST((
              CAST('NTS' AS VARCHAR)
            ) AS TEXT)
          )
          AND (
            CAST((
              exch_rate.to_crncy
            ) AS TEXT) = CAST((
              CAST('USD' AS VARCHAR)
            ) AS TEXT)
          )
        )
        THEN SUM(copa.qty)
        ELSE CAST((
          CAST((
            0
          ) AS DECIMAL)
        ) AS DECIMAL(18, 0))
      END AS nts_qty,
      CASE
        WHEN (
          (
            CAST((
              copa.acct_hier_shrt_desc
            ) AS TEXT) = CAST((
              CAST('GTS' AS VARCHAR)
            ) AS TEXT)
          )
          AND (
            CAST((
              exch_rate.to_crncy
            ) AS TEXT) = CAST((
              CAST('USD' AS VARCHAR)
            ) AS TEXT)
          )
        )
        THEN SUM(copa.qty)
        ELSE CAST((
          CAST((
            0
          ) AS DECIMAL)
        ) AS DECIMAL(18, 0))
      END AS gts_qty,
      CASE
        WHEN (
          (
            CAST((
              copa.acct_hier_shrt_desc
            ) AS TEXT) = CAST((
              CAST('EQ' AS VARCHAR)
            ) AS TEXT)
          )
          AND (
            CAST((
              exch_rate.to_crncy
            ) AS TEXT) = CAST((
              CAST('USD' AS VARCHAR)
            ) AS TEXT)
          )
        )
        THEN SUM(copa.qty)
        ELSE CAST((
          CAST((
            0
          ) AS DECIMAL)
        ) AS DECIMAL(18, 0))
      END AS eq_qty,
      mat.matl_num AS "product code",
      mat.matl_desc AS "product name"
    FROM (
      (
        (
          (
            (
              edw_copa_trans_fact AS copa
                LEFT JOIN edw_material_dim AS mat
                  ON (
                    (
                      CAST((
                        copa.matl_num
                      ) AS TEXT) = CAST((
                        mat.matl_num
                      ) AS TEXT)
                    )
                  )
            )
            LEFT JOIN v_edw_customer_sales_dim AS cus_sales_extn
              ON (
                (
                  (
                    (
                      (
                        CAST((
                          cus_sales_extn.sls_org
                        ) AS TEXT) = CAST((
                          CASE
                            WHEN (
                              (
                                (
                                  copa.sls_org IS NULL
                                )
                                AND (
                                  LTRIM(CAST((
                                    copa.cust_num
                                  ) AS TEXT), CAST((
                                    CAST('0' AS VARCHAR)
                                  ) AS TEXT)) = CAST((
                                    CAST('135520' AS VARCHAR)
                                  ) AS TEXT)
                                )
                              )
                              AND (
                                CAST((
                                  copa.co_cd
                                ) AS TEXT) = CAST((
                                  CAST('703A' AS VARCHAR)
                                ) AS TEXT)
                              )
                            )
                            THEN CAST('100A' AS VARCHAR)
                            ELSE copa.sls_org
                          END
                        ) AS TEXT)
                      )
                      AND (
                        CAST((
                          cus_sales_extn.dstr_chnl
                        ) AS TEXT) = CAST((
                          CASE
                            WHEN (
                              (
                                (
                                  copa.dstr_chnl IS NULL
                                )
                                AND (
                                  LTRIM(CAST((
                                    copa.cust_num
                                  ) AS TEXT), CAST((
                                    CAST('0' AS VARCHAR)
                                  ) AS TEXT)) = CAST((
                                    CAST('135520' AS VARCHAR)
                                  ) AS TEXT)
                                )
                              )
                              AND (
                                CAST((
                                  copa.co_cd
                                ) AS TEXT) = CAST((
                                  CAST('703A' AS VARCHAR)
                                ) AS TEXT)
                              )
                            )
                            THEN CAST('15' AS VARCHAR)
                            ELSE copa.dstr_chnl
                          END
                        ) AS TEXT)
                      )
                    )
                    AND (
                      CAST((
                        copa.div
                      ) AS TEXT) = CAST((
                        cus_sales_extn.div
                      ) AS TEXT)
                    )
                  )
                  AND (
                    CAST((
                      copa.cust_num
                    ) AS TEXT) = CAST((
                      cus_sales_extn.cust_num
                    ) AS TEXT)
                  )
                )
              )
          )
          JOIN edw_company_dim AS cmp
            ON (
              (
                CAST((
                  copa.co_cd
                ) AS TEXT) = CAST((
                  cmp.co_cd
                ) AS TEXT)
              )
            )
        )
        LEFT JOIN (
          SELECT
            edw_vw_greenlight_skus.co_cd,
            edw_vw_greenlight_skus.matl_num,
            edw_vw_greenlight_skus.pka_product_key,
            edw_vw_greenlight_skus.greenlight_sku_flag
          FROM edw_vw_greenlight_skus
          GROUP BY
            edw_vw_greenlight_skus.co_cd,
            edw_vw_greenlight_skus.matl_num,
            edw_vw_greenlight_skus.pka_product_key,
            edw_vw_greenlight_skus.greenlight_sku_flag
        ) AS gn
          ON (
            (
              (
                LTRIM(
                  CAST((
                    copa.matl_num
                  ) AS TEXT),
                  CAST((
                    CAST((
                      0
                    ) AS VARCHAR)
                  ) AS TEXT)
                ) = LTRIM(gn.matl_num, CAST((
                  CAST((
                    0
                  ) AS VARCHAR)
                ) AS TEXT))
              )
              AND (
                CAST((
                  copa.co_cd
                ) AS TEXT) = CAST((
                  gn.co_cd
                ) AS TEXT)
              )
            )
          )
      )
      LEFT JOIN v_intrm_reg_crncy_exch_fiscper AS exch_rate
        ON (
          (
            (
              (
                CAST((
                  copa.obj_crncy_co_obj
                ) AS TEXT) = CAST((
                  exch_rate.from_crncy
                ) AS TEXT)
              )
              AND (
                copa.fisc_yr_per = exch_rate.fisc_per
              )
            )
            AND CASE
              WHEN (
                CAST((
                  exch_rate.to_crncy
                ) AS TEXT) <> CAST((
                  CAST('USD' AS VARCHAR)
                ) AS TEXT)
              )
              THEN (
                CAST((
                  exch_rate.to_crncy
                ) AS TEXT) = CAST((
                  CASE
                    WHEN (
                      (
                        CAST((
                          cmp.ctry_group
                        ) AS TEXT) = CAST((
                          CAST('India' AS VARCHAR)
                        ) AS TEXT)
                      )
                      OR (
                        (
                          cmp.ctry_group IS NULL
                        ) AND (
                          'India' IS NULL
                        )
                      )
                    )
                    THEN CAST('INR' AS VARCHAR)
                    WHEN (
                      (
                        CAST((
                          cmp.ctry_group
                        ) AS TEXT) = CAST((
                          CAST('Philippines' AS VARCHAR)
                        ) AS TEXT)
                      )
                      OR (
                        (
                          cmp.ctry_group IS NULL
                        ) AND (
                          'Philippines' IS NULL
                        )
                      )
                    )
                    THEN CAST('PHP' AS VARCHAR)
                    WHEN (
                      (
                        CAST((
                          cmp.ctry_group
                        ) AS TEXT) = CAST((
                          CAST('China Selfcare' AS VARCHAR)
                        ) AS TEXT)
                      )
                      OR (
                        (
                          cmp.ctry_group IS NULL
                        ) AND (
                          'China Selfcare' IS NULL
                        )
                      )
                    )
                    THEN CAST('RMB' AS VARCHAR)
                    WHEN (
                      (
                        CAST((
                          cmp.ctry_group
                        ) AS TEXT) = CAST((
                          CAST('China Personal Care' AS VARCHAR)
                        ) AS TEXT)
                      )
                      OR (
                        (
                          cmp.ctry_group IS NULL
                        ) AND (
                          'China Personal Care' IS NULL
                        )
                      )
                    )
                    THEN CAST('RMB' AS VARCHAR)
                    ELSE copa.obj_crncy_co_obj
                  END
                ) AS TEXT)
              )
              ELSE (
                CAST((
                  exch_rate.to_crncy
                ) AS TEXT) = CAST((
                  CAST('USD' AS VARCHAR)
                ) AS TEXT)
              )
            END
          )
        )
    )
    WHERE
      (
        (
          (
            (
              CAST((
                copa.acct_hier_shrt_desc
              ) AS TEXT) = CAST((
                CAST('GTS' AS VARCHAR)
              ) AS TEXT)
            )
            OR (
              CAST((
                copa.acct_hier_shrt_desc
              ) AS TEXT) = CAST((
                CAST('NTS' AS VARCHAR)
              ) AS TEXT)
            )
          )
          OR (
            CAST((
              copa.acct_hier_shrt_desc
            ) AS TEXT) = CAST((
              CAST('EQ' AS VARCHAR)
            ) AS TEXT)
          )
        )
        AND (
          CAST((
            CAST((
              copa.fisc_yr_per
            ) AS VARCHAR)
          ) AS TEXT) >= (
            (
              (
                CAST((
                  CAST((
                    (
                      DATE_PART(YEAR, CURRENT_DATE()) - 2
                    )
                  ) AS VARCHAR)
                ) AS TEXT) || CAST((
                  CAST((
                    0
                  ) AS VARCHAR)
                ) AS TEXT)
              ) || CAST((
                CAST((
                  0
                ) AS VARCHAR)
              ) AS TEXT)
            ) || CAST((
              CAST((
                1
              ) AS VARCHAR)
            ) AS TEXT)
          )
        )
      )
    GROUP BY
      copa.fisc_yr,
      copa.fisc_yr_per,
      copa.obj_crncy_co_obj,
      copa.cust_num,
      copa.acct_num,
      cmp.ctry_group,
      cmp."cluster",
      mat.mega_brnd_desc,
      mat.brnd_desc,
      mat.varnt_desc,
      mat.base_prod_desc,
      mat.put_up_desc,
      mat.prodh1_txtmd,
      mat.prodh2_txtmd,
      mat.prodh3_txtmd,
      mat.prodh4_txtmd,
      mat.prodh5_txtmd,
      mat.prodh6_txtmd,
      mat.pka_franchise_cd,
      mat.pka_franchise_desc,
      mat.pka_brand_cd,
      mat.pka_brand_desc,
      mat.pka_sub_brand_cd,
      mat.pka_sub_brand_desc,
      mat.pka_variant_cd,
      mat.pka_variant_desc,
      mat.pka_sub_variant_cd,
      mat.pka_sub_variant_desc,
      mat.pka_flavor_cd,
      mat.pka_flavor_desc,
      mat.pka_ingredient_cd,
      mat.pka_ingredient_desc,
      mat.pka_application_cd,
      mat.pka_application_desc,
      mat.pka_length_cd,
      mat.pka_length_desc,
      mat.pka_shape_cd,
      mat.pka_shape_desc,
      mat.pka_spf_cd,
      mat.pka_spf_desc,
      mat.pka_cover_cd,
      mat.pka_cover_desc,
      mat.pka_form_cd,
      mat.pka_form_desc,
      mat.pka_size_cd,
      mat.pka_size_desc,
      mat.pka_character_cd,
      mat.pka_character_desc,
      mat.pka_package_cd,
      mat.pka_package_desc,
      mat.pka_attribute_13_cd,
      mat.pka_attribute_13_desc,
      mat.pka_attribute_14_cd,
      mat.pka_attribute_14_desc,
      mat.pka_sku_identification_cd,
      mat.pka_sku_identification_desc,
      mat.pka_one_time_relabeling_cd,
      mat.pka_one_time_relabeling_desc,
      mat.pka_product_key,
      mat.pka_product_key_description,
      mat.pka_product_key_description_2,
      mat.pka_root_code,
      mat.pka_root_code_desc_1,
      mat.pka_root_code_desc_2,
      gn.greenlight_sku_flag,
      cus_sales_extn."parent customer",
      cus_sales_extn.banner,
      cus_sales_extn."banner format",
      cus_sales_extn.channel,
      cus_sales_extn."go to model",
      cus_sales_extn."sub channel",
      cus_sales_extn.retail_env,
      exch_rate.from_crncy,
      exch_rate.to_crncy,
      copa.acct_hier_shrt_desc,
      mat.matl_num,
      mat.matl_desc
  ) AS derived_table1
  GROUP BY
    derived_table1.fisc_yr,
    derived_table1.fisc_yr_per,
    derived_table1.fisc_day,
    derived_table1.ctry_nm,
    derived_table1."cluster",
    derived_table1.obj_crncy_co_obj,
    derived_table1."b1 mega-brand",
    derived_table1."b2 brand",
    derived_table1."b3 base product",
    derived_table1."b4 variant",
    derived_table1."b5 put-up",
    derived_table1."prod h1 operating group",
    derived_table1."prod h2 franchise group",
    derived_table1."prod h3 franchise",
    derived_table1."prod h4 product franchise",
    derived_table1."prod h5 product major",
    derived_table1."prod h6 product minor",
    derived_table1.pka_franchise_desc,
    derived_table1.pka_brand_desc,
    derived_table1.pka_sub_brand_desc,
    derived_table1.pka_variant_desc,
    derived_table1.pka_sub_variant_desc,
    derived_table1.pka_flavor_desc,
    derived_table1.pka_ingredient_desc,
    derived_table1.pka_application_desc,
    derived_table1.pka_length_desc,
    derived_table1.pka_shape_desc,
    derived_table1.pka_spf_desc,
    derived_table1.pka_cover_desc,
    derived_table1.pka_form_desc,
    derived_table1.pka_size_desc,
    derived_table1.pka_character_desc,
    derived_table1.pka_package_desc,
    derived_table1.pka_attribute_13_desc,
    derived_table1.pka_attribute_14_desc,
    derived_table1.pka_sku_identification_desc,
    derived_table1.pka_one_time_relabeling_desc,
    derived_table1.pka_product_key,
    derived_table1.pka_product_key_description,
    derived_table1.pka_product_key_description_2,
    derived_table1.pka_root_code,
    derived_table1.pka_root_code_desc_1,
    derived_table1.pka_root_code_desc_2,
    derived_table1.greenlight_sku_flag,
    derived_table1."parent customer",
    derived_table1.banner,
    derived_table1."banner format",
    derived_table1.channel,
    derived_table1."go to model",
    derived_table1."sub channel",
    derived_table1.retail_env,
    derived_table1.from_crncy,
    derived_table1.to_crncy,
    derived_table1."product code",
    derived_table1."product name"
  )
  select * from transformed
);


create or replace view aspedw_integration.edw_acct_ciw_hier
as
(
  with edw_account_ciw_dim as(
      select * from aspedw_integration.edw_account_ciw_dim
  ),
  edw_account_ciw_xref as (
    select * from aspedw_integration.edw_account_ciw_xref
  ),
  tranformed as(
  SELECT
    derived_table1.chrt_acct,
    derived_table1.acct_num,
    derived_table1.acct_nm,
    derived_table1.ciw_desc,
    derived_table1.ciw_code,
    derived_table1.ciw_bucket,
    derived_table1.ciw_acct_col,
    derived_table1.ciw_acct_no,
    derived_table1.ciw_acct_nm,
    derived_table1.measure_code,
    derived_table1.measure_name,
    derived_table1.multiplication_factor
  FROM (
    SELECT
      a.chrt_acct,
      a.acct_num,
      a.acct_nm,
      a.ciw_desc,
      a.ciw_code,
      a.ciw_bucket,
      a.ciw_acct_col,
      a.ciw_acct_no,
      a.ciw_acct_nm,
      b.measure_code,
      b.measure_name,
      b.multiplication_factor
    FROM (
      (
        (
          (
            (
              (
                SELECT
                  edw_account_ciw_dim.chrt_acct,
                  edw_account_ciw_dim.acct_num,
                  edw_account_ciw_dim.acct_nm,
                  edw_account_ciw_dim.ciw_acct_l3_txt AS ciw_desc,
                  CAST((
                    CASE
                      WHEN (
                        (
                          (
                            (
                              (
                                (
                                  SPLIT_PART(CAST((
                                    edw_account_ciw_dim.ciw_acct_l3
                                  ) AS TEXT), CAST('_' AS TEXT), 2) = CAST('PYT' AS TEXT)
                                )
                                OR (
                                  SPLIT_PART(CAST((
                                    edw_account_ciw_dim.ciw_acct_l3
                                  ) AS TEXT), CAST('_' AS TEXT), 2) = CAST('GSI' AS TEXT)
                                )
                              )
                              OR (
                                SPLIT_PART(CAST((
                                  edw_account_ciw_dim.ciw_acct_l3
                                ) AS TEXT), CAST('_' AS TEXT), 2) = CAST('GSD' AS TEXT)
                              )
                            )
                            OR (
                              SPLIT_PART(CAST((
                                edw_account_ciw_dim.ciw_acct_l3
                              ) AS TEXT), CAST('_' AS TEXT), 2) = CAST('EFF' AS TEXT)
                            )
                          )
                          OR (
                            SPLIT_PART(CAST((
                              edw_account_ciw_dim.ciw_acct_l3
                            ) AS TEXT), CAST('_' AS TEXT), 2) = CAST('DNI' AS TEXT)
                          )
                        )
                        OR (
                          SPLIT_PART(CAST((
                            edw_account_ciw_dim.ciw_acct_l3
                          ) AS TEXT), CAST('_' AS TEXT), 2) = CAST('CMNE' AS TEXT)
                        )
                      )
                      THEN COALESCE(
                        SPLIT_PART(
                          LTRIM(
                            SPLIT_PART(CAST((
                              edw_account_ciw_dim.ciw_acct_l5
                            ) AS TEXT), CAST('_' AS TEXT), 1),
                            CAST('JJPLAC' AS TEXT)
                          ),
                          CAST('-' AS TEXT),
                          1
                        ),
                        LTRIM(
                          SPLIT_PART(CAST((
                            edw_account_ciw_dim.ciw_acct_l4
                          ) AS TEXT), CAST('_' AS TEXT), 1),
                          CAST('JJPLAC' AS TEXT)
                        )
                      )
                      ELSE LTRIM(
                        SPLIT_PART(CAST((
                          edw_account_ciw_dim.ciw_acct_l4
                        ) AS TEXT), CAST('_' AS TEXT), 1),
                        CAST('JJPLAC' AS TEXT)
                      )
                    END
                  ) AS VARCHAR) AS ciw_code,
                  CASE
                    WHEN (
                      (
                        (
                          (
                            (
                              (
                                SPLIT_PART(CAST((
                                  edw_account_ciw_dim.ciw_acct_l3
                                ) AS TEXT), CAST('_' AS TEXT), 2) = CAST('PYT' AS TEXT)
                              )
                              OR (
                                SPLIT_PART(CAST((
                                  edw_account_ciw_dim.ciw_acct_l3
                                ) AS TEXT), CAST('_' AS TEXT), 2) = CAST('GSI' AS TEXT)
                              )
                            )
                            OR (
                              SPLIT_PART(CAST((
                                edw_account_ciw_dim.ciw_acct_l3
                              ) AS TEXT), CAST('_' AS TEXT), 2) = CAST('GSD' AS TEXT)
                            )
                          )
                          OR (
                            SPLIT_PART(CAST((
                              edw_account_ciw_dim.ciw_acct_l3
                            ) AS TEXT), CAST('_' AS TEXT), 2) = CAST('EFF' AS TEXT)
                          )
                        )
                        OR (
                          SPLIT_PART(CAST((
                            edw_account_ciw_dim.ciw_acct_l3
                          ) AS TEXT), CAST('_' AS TEXT), 2) = CAST('DNI' AS TEXT)
                        )
                      )
                      OR (
                        SPLIT_PART(CAST((
                          edw_account_ciw_dim.ciw_acct_l3
                        ) AS TEXT), CAST('_' AS TEXT), 2) = CAST('CMNE' AS TEXT)
                      )
                    )
                    THEN COALESCE(edw_account_ciw_dim.ciw_acct_l5_txt, edw_account_ciw_dim.ciw_acct_l4_txt)
                    ELSE edw_account_ciw_dim.ciw_acct_l4_txt
                  END AS ciw_bucket,
                  CAST('CIW_ACCT_L1' AS VARCHAR) AS ciw_acct_col,
                  edw_account_ciw_dim.ciw_acct_l1 AS ciw_acct_no,
                  edw_account_ciw_dim.ciw_acct_l1_txt AS ciw_acct_nm
                FROM edw_account_ciw_dim
                WHERE
                  (
                    LENGTH(CAST((
                      edw_account_ciw_dim.ciw_acct_l1
                    ) AS TEXT)) <> 0
                  )
                UNION ALL
                SELECT
                  edw_account_ciw_dim.chrt_acct,
                  edw_account_ciw_dim.acct_num,
                  edw_account_ciw_dim.acct_nm,
                  edw_account_ciw_dim.ciw_acct_l3_txt AS ciw_desc,
                  CAST((
                    CASE
                      WHEN (
                        (
                          (
                            (
                              (
                                (
                                  SPLIT_PART(CAST((
                                    edw_account_ciw_dim.ciw_acct_l3
                                  ) AS TEXT), CAST('_' AS TEXT), 2) = CAST('PYT' AS TEXT)
                                )
                                OR (
                                  SPLIT_PART(CAST((
                                    edw_account_ciw_dim.ciw_acct_l3
                                  ) AS TEXT), CAST('_' AS TEXT), 2) = CAST('GSI' AS TEXT)
                                )
                              )
                              OR (
                                SPLIT_PART(CAST((
                                  edw_account_ciw_dim.ciw_acct_l3
                                ) AS TEXT), CAST('_' AS TEXT), 2) = CAST('GSD' AS TEXT)
                              )
                            )
                            OR (
                              SPLIT_PART(CAST((
                                edw_account_ciw_dim.ciw_acct_l3
                              ) AS TEXT), CAST('_' AS TEXT), 2) = CAST('EFF' AS TEXT)
                            )
                          )
                          OR (
                            SPLIT_PART(CAST((
                              edw_account_ciw_dim.ciw_acct_l3
                            ) AS TEXT), CAST('_' AS TEXT), 2) = CAST('DNI' AS TEXT)
                          )
                        )
                        OR (
                          SPLIT_PART(CAST((
                            edw_account_ciw_dim.ciw_acct_l3
                          ) AS TEXT), CAST('_' AS TEXT), 2) = CAST('CMNE' AS TEXT)
                        )
                      )
                      THEN COALESCE(
                        SPLIT_PART(
                          LTRIM(
                            SPLIT_PART(CAST((
                              edw_account_ciw_dim.ciw_acct_l5
                            ) AS TEXT), CAST('_' AS TEXT), 1),
                            CAST('JJPLAC' AS TEXT)
                          ),
                          CAST('-' AS TEXT),
                          1
                        ),
                        LTRIM(
                          SPLIT_PART(CAST((
                            edw_account_ciw_dim.ciw_acct_l4
                          ) AS TEXT), CAST('_' AS TEXT), 1),
                          CAST('JJPLAC' AS TEXT)
                        )
                      )
                      ELSE LTRIM(
                        SPLIT_PART(CAST((
                          edw_account_ciw_dim.ciw_acct_l4
                        ) AS TEXT), CAST('_' AS TEXT), 1),
                        CAST('JJPLAC' AS TEXT)
                      )
                    END
                  ) AS VARCHAR) AS ciw_code,
                  CASE
                    WHEN (
                      (
                        (
                          (
                            (
                              (
                                SPLIT_PART(CAST((
                                  edw_account_ciw_dim.ciw_acct_l3
                                ) AS TEXT), CAST('_' AS TEXT), 2) = CAST('PYT' AS TEXT)
                              )
                              OR (
                                SPLIT_PART(CAST((
                                  edw_account_ciw_dim.ciw_acct_l3
                                ) AS TEXT), CAST('_' AS TEXT), 2) = CAST('GSI' AS TEXT)
                              )
                            )
                            OR (
                              SPLIT_PART(CAST((
                                edw_account_ciw_dim.ciw_acct_l3
                              ) AS TEXT), CAST('_' AS TEXT), 2) = CAST('GSD' AS TEXT)
                            )
                          )
                          OR (
                            SPLIT_PART(CAST((
                              edw_account_ciw_dim.ciw_acct_l3
                            ) AS TEXT), CAST('_' AS TEXT), 2) = CAST('EFF' AS TEXT)
                          )
                        )
                        OR (
                          SPLIT_PART(CAST((
                            edw_account_ciw_dim.ciw_acct_l3
                          ) AS TEXT), CAST('_' AS TEXT), 2) = CAST('DNI' AS TEXT)
                        )
                      )
                      OR (
                        SPLIT_PART(CAST((
                          edw_account_ciw_dim.ciw_acct_l3
                        ) AS TEXT), CAST('_' AS TEXT), 2) = CAST('CMNE' AS TEXT)
                      )
                    )
                    THEN COALESCE(edw_account_ciw_dim.ciw_acct_l5_txt, edw_account_ciw_dim.ciw_acct_l4_txt)
                    ELSE edw_account_ciw_dim.ciw_acct_l4_txt
                  END AS ciw_bucket,
                  CAST('CIW_ACCT_L2' AS VARCHAR) AS ciw_acct_col,
                  edw_account_ciw_dim.ciw_acct_l2 AS ciw_acct_no,
                  edw_account_ciw_dim.ciw_acct_l2_txt AS ciw_acct_nm
                FROM edw_account_ciw_dim
                WHERE
                  (
                    LENGTH(CAST((
                      edw_account_ciw_dim.ciw_acct_l2
                    ) AS TEXT)) <> 0
                  )
              )
              UNION ALL
              SELECT
                edw_account_ciw_dim.chrt_acct,
                edw_account_ciw_dim.acct_num,
                edw_account_ciw_dim.acct_nm,
                edw_account_ciw_dim.ciw_acct_l3_txt AS ciw_desc,
                CAST((
                  CASE
                    WHEN (
                      (
                        (
                          (
                            (
                              (
                                SPLIT_PART(CAST((
                                  edw_account_ciw_dim.ciw_acct_l3
                                ) AS TEXT), CAST('_' AS TEXT), 2) = CAST('PYT' AS TEXT)
                              )
                              OR (
                                SPLIT_PART(CAST((
                                  edw_account_ciw_dim.ciw_acct_l3
                                ) AS TEXT), CAST('_' AS TEXT), 2) = CAST('GSI' AS TEXT)
                              )
                            )
                            OR (
                              SPLIT_PART(CAST((
                                edw_account_ciw_dim.ciw_acct_l3
                              ) AS TEXT), CAST('_' AS TEXT), 2) = CAST('GSD' AS TEXT)
                            )
                          )
                          OR (
                            SPLIT_PART(CAST((
                              edw_account_ciw_dim.ciw_acct_l3
                            ) AS TEXT), CAST('_' AS TEXT), 2) = CAST('EFF' AS TEXT)
                          )
                        )
                        OR (
                          SPLIT_PART(CAST((
                            edw_account_ciw_dim.ciw_acct_l3
                          ) AS TEXT), CAST('_' AS TEXT), 2) = CAST('DNI' AS TEXT)
                        )
                      )
                      OR (
                        SPLIT_PART(CAST((
                          edw_account_ciw_dim.ciw_acct_l3
                        ) AS TEXT), CAST('_' AS TEXT), 2) = CAST('CMNE' AS TEXT)
                      )
                    )
                    THEN COALESCE(
                      SPLIT_PART(
                        LTRIM(
                          SPLIT_PART(CAST((
                            edw_account_ciw_dim.ciw_acct_l5
                          ) AS TEXT), CAST('_' AS TEXT), 1),
                          CAST('JJPLAC' AS TEXT)
                        ),
                        CAST('-' AS TEXT),
                        1
                      ),
                      LTRIM(
                        SPLIT_PART(CAST((
                          edw_account_ciw_dim.ciw_acct_l4
                        ) AS TEXT), CAST('_' AS TEXT), 1),
                        CAST('JJPLAC' AS TEXT)
                      )
                    )
                    ELSE LTRIM(
                      SPLIT_PART(CAST((
                        edw_account_ciw_dim.ciw_acct_l4
                      ) AS TEXT), CAST('_' AS TEXT), 1),
                      CAST('JJPLAC' AS TEXT)
                    )
                  END
                ) AS VARCHAR) AS ciw_code,
                CASE
                  WHEN (
                    (
                      (
                        (
                          (
                            (
                              SPLIT_PART(CAST((
                                edw_account_ciw_dim.ciw_acct_l3
                              ) AS TEXT), CAST('_' AS TEXT), 2) = CAST('PYT' AS TEXT)
                            )
                            OR (
                              SPLIT_PART(CAST((
                                edw_account_ciw_dim.ciw_acct_l3
                              ) AS TEXT), CAST('_' AS TEXT), 2) = CAST('GSI' AS TEXT)
                            )
                          )
                          OR (
                            SPLIT_PART(CAST((
                              edw_account_ciw_dim.ciw_acct_l3
                            ) AS TEXT), CAST('_' AS TEXT), 2) = CAST('GSD' AS TEXT)
                          )
                        )
                        OR (
                          SPLIT_PART(CAST((
                            edw_account_ciw_dim.ciw_acct_l3
                          ) AS TEXT), CAST('_' AS TEXT), 2) = CAST('EFF' AS TEXT)
                        )
                      )
                      OR (
                        SPLIT_PART(CAST((
                          edw_account_ciw_dim.ciw_acct_l3
                        ) AS TEXT), CAST('_' AS TEXT), 2) = CAST('DNI' AS TEXT)
                      )
                    )
                    OR (
                      SPLIT_PART(CAST((
                        edw_account_ciw_dim.ciw_acct_l3
                      ) AS TEXT), CAST('_' AS TEXT), 2) = CAST('CMNE' AS TEXT)
                    )
                  )
                  THEN COALESCE(edw_account_ciw_dim.ciw_acct_l5_txt, edw_account_ciw_dim.ciw_acct_l4_txt)
                  ELSE edw_account_ciw_dim.ciw_acct_l4_txt
                END AS ciw_bucket,
                CAST('CIW_ACCT_L3' AS VARCHAR) AS ciw_acct_col,
                edw_account_ciw_dim.ciw_acct_l3 AS ciw_acct_no,
                edw_account_ciw_dim.ciw_acct_l3_txt AS ciw_acct_nm
              FROM edw_account_ciw_dim
              WHERE
                (
                  LENGTH(CAST((
                    edw_account_ciw_dim.ciw_acct_l3
                  ) AS TEXT)) <> 0
                )
            )
            UNION ALL
            SELECT
              edw_account_ciw_dim.chrt_acct,
              edw_account_ciw_dim.acct_num,
              edw_account_ciw_dim.acct_nm,
              edw_account_ciw_dim.ciw_acct_l3_txt AS ciw_desc,
              CAST((
                CASE
                  WHEN (
                    (
                      (
                        (
                          (
                            (
                              SPLIT_PART(CAST((
                                edw_account_ciw_dim.ciw_acct_l3
                              ) AS TEXT), CAST('_' AS TEXT), 2) = CAST('PYT' AS TEXT)
                            )
                            OR (
                              SPLIT_PART(CAST((
                                edw_account_ciw_dim.ciw_acct_l3
                              ) AS TEXT), CAST('_' AS TEXT), 2) = CAST('GSI' AS TEXT)
                            )
                          )
                          OR (
                            SPLIT_PART(CAST((
                              edw_account_ciw_dim.ciw_acct_l3
                            ) AS TEXT), CAST('_' AS TEXT), 2) = CAST('GSD' AS TEXT)
                          )
                        )
                        OR (
                          SPLIT_PART(CAST((
                            edw_account_ciw_dim.ciw_acct_l3
                          ) AS TEXT), CAST('_' AS TEXT), 2) = CAST('EFF' AS TEXT)
                        )
                      )
                      OR (
                        SPLIT_PART(CAST((
                          edw_account_ciw_dim.ciw_acct_l3
                        ) AS TEXT), CAST('_' AS TEXT), 2) = CAST('DNI' AS TEXT)
                      )
                    )
                    OR (
                      SPLIT_PART(CAST((
                        edw_account_ciw_dim.ciw_acct_l3
                      ) AS TEXT), CAST('_' AS TEXT), 2) = CAST('CMNE' AS TEXT)
                    )
                  )
                  THEN COALESCE(
                    SPLIT_PART(
                      LTRIM(
                        SPLIT_PART(CAST((
                          edw_account_ciw_dim.ciw_acct_l5
                        ) AS TEXT), CAST('_' AS TEXT), 1),
                        CAST('JJPLAC' AS TEXT)
                      ),
                      CAST('-' AS TEXT),
                      1
                    ),
                    LTRIM(
                      SPLIT_PART(CAST((
                        edw_account_ciw_dim.ciw_acct_l4
                      ) AS TEXT), CAST('_' AS TEXT), 1),
                      CAST('JJPLAC' AS TEXT)
                    )
                  )
                  ELSE LTRIM(
                    SPLIT_PART(CAST((
                      edw_account_ciw_dim.ciw_acct_l4
                    ) AS TEXT), CAST('_' AS TEXT), 1),
                    CAST('JJPLAC' AS TEXT)
                  )
                END
              ) AS VARCHAR) AS ciw_code,
              CASE
                WHEN (
                  (
                    (
                      (
                        (
                          (
                            SPLIT_PART(CAST((
                              edw_account_ciw_dim.ciw_acct_l3
                            ) AS TEXT), CAST('_' AS TEXT), 2) = CAST('PYT' AS TEXT)
                          )
                          OR (
                            SPLIT_PART(CAST((
                              edw_account_ciw_dim.ciw_acct_l3
                            ) AS TEXT), CAST('_' AS TEXT), 2) = CAST('GSI' AS TEXT)
                          )
                        )
                        OR (
                          SPLIT_PART(CAST((
                            edw_account_ciw_dim.ciw_acct_l3
                          ) AS TEXT), CAST('_' AS TEXT), 2) = CAST('GSD' AS TEXT)
                        )
                      )
                      OR (
                        SPLIT_PART(CAST((
                          edw_account_ciw_dim.ciw_acct_l3
                        ) AS TEXT), CAST('_' AS TEXT), 2) = CAST('EFF' AS TEXT)
                      )
                    )
                    OR (
                      SPLIT_PART(CAST((
                        edw_account_ciw_dim.ciw_acct_l3
                      ) AS TEXT), CAST('_' AS TEXT), 2) = CAST('DNI' AS TEXT)
                    )
                  )
                  OR (
                    SPLIT_PART(CAST((
                      edw_account_ciw_dim.ciw_acct_l3
                    ) AS TEXT), CAST('_' AS TEXT), 2) = CAST('CMNE' AS TEXT)
                  )
                )
                THEN COALESCE(edw_account_ciw_dim.ciw_acct_l5_txt, edw_account_ciw_dim.ciw_acct_l4_txt)
                ELSE edw_account_ciw_dim.ciw_acct_l4_txt
              END AS ciw_bucket,
              CAST('CIW_ACCT_L4' AS VARCHAR) AS ciw_acct_col,
              edw_account_ciw_dim.ciw_acct_l4 AS ciw_acct_no,
              edw_account_ciw_dim.ciw_acct_l4_txt AS ciw_acct_nm
            FROM edw_account_ciw_dim
            WHERE
              (
                LENGTH(CAST((
                  edw_account_ciw_dim.ciw_acct_l4
                ) AS TEXT)) <> 0
              )
          )
          UNION ALL
          SELECT
            edw_account_ciw_dim.chrt_acct,
            edw_account_ciw_dim.acct_num,
            edw_account_ciw_dim.acct_nm,
            edw_account_ciw_dim.ciw_acct_l3_txt AS ciw_desc,
            CAST((
              CASE
                WHEN (
                  (
                    (
                      (
                        (
                          (
                            SPLIT_PART(CAST((
                              edw_account_ciw_dim.ciw_acct_l3
                            ) AS TEXT), CAST('_' AS TEXT), 2) = CAST('PYT' AS TEXT)
                          )
                          OR (
                            SPLIT_PART(CAST((
                              edw_account_ciw_dim.ciw_acct_l3
                            ) AS TEXT), CAST('_' AS TEXT), 2) = CAST('GSI' AS TEXT)
                          )
                        )
                        OR (
                          SPLIT_PART(CAST((
                            edw_account_ciw_dim.ciw_acct_l3
                          ) AS TEXT), CAST('_' AS TEXT), 2) = CAST('GSD' AS TEXT)
                        )
                      )
                      OR (
                        SPLIT_PART(CAST((
                          edw_account_ciw_dim.ciw_acct_l3
                        ) AS TEXT), CAST('_' AS TEXT), 2) = CAST('EFF' AS TEXT)
                      )
                    )
                    OR (
                      SPLIT_PART(CAST((
                        edw_account_ciw_dim.ciw_acct_l3
                      ) AS TEXT), CAST('_' AS TEXT), 2) = CAST('DNI' AS TEXT)
                    )
                  )
                  OR (
                    SPLIT_PART(CAST((
                      edw_account_ciw_dim.ciw_acct_l3
                    ) AS TEXT), CAST('_' AS TEXT), 2) = CAST('CMNE' AS TEXT)
                  )
                )
                THEN COALESCE(
                  SPLIT_PART(
                    LTRIM(
                      SPLIT_PART(CAST((
                        edw_account_ciw_dim.ciw_acct_l5
                      ) AS TEXT), CAST('_' AS TEXT), 1),
                      CAST('JJPLAC' AS TEXT)
                    ),
                    CAST('-' AS TEXT),
                    1
                  ),
                  LTRIM(
                    SPLIT_PART(CAST((
                      edw_account_ciw_dim.ciw_acct_l4
                    ) AS TEXT), CAST('_' AS TEXT), 1),
                    CAST('JJPLAC' AS TEXT)
                  )
                )
                ELSE LTRIM(
                  SPLIT_PART(CAST((
                    edw_account_ciw_dim.ciw_acct_l4
                  ) AS TEXT), CAST('_' AS TEXT), 1),
                  CAST('JJPLAC' AS TEXT)
                )
              END
            ) AS VARCHAR) AS ciw_code,
            CASE
              WHEN (
                (
                  (
                    (
                      (
                        (
                          SPLIT_PART(CAST((
                            edw_account_ciw_dim.ciw_acct_l3
                          ) AS TEXT), CAST('_' AS TEXT), 2) = CAST('PYT' AS TEXT)
                        )
                        OR (
                          SPLIT_PART(CAST((
                            edw_account_ciw_dim.ciw_acct_l3
                          ) AS TEXT), CAST('_' AS TEXT), 2) = CAST('GSI' AS TEXT)
                        )
                      )
                      OR (
                        SPLIT_PART(CAST((
                          edw_account_ciw_dim.ciw_acct_l3
                        ) AS TEXT), CAST('_' AS TEXT), 2) = CAST('GSD' AS TEXT)
                      )
                    )
                    OR (
                      SPLIT_PART(CAST((
                        edw_account_ciw_dim.ciw_acct_l3
                      ) AS TEXT), CAST('_' AS TEXT), 2) = CAST('EFF' AS TEXT)
                    )
                  )
                  OR (
                    SPLIT_PART(CAST((
                      edw_account_ciw_dim.ciw_acct_l3
                    ) AS TEXT), CAST('_' AS TEXT), 2) = CAST('DNI' AS TEXT)
                  )
                )
                OR (
                  SPLIT_PART(CAST((
                    edw_account_ciw_dim.ciw_acct_l3
                  ) AS TEXT), CAST('_' AS TEXT), 2) = CAST('CMNE' AS TEXT)
                )
              )
              THEN COALESCE(edw_account_ciw_dim.ciw_acct_l5_txt, edw_account_ciw_dim.ciw_acct_l4_txt)
              ELSE edw_account_ciw_dim.ciw_acct_l4_txt
            END AS ciw_bucket,
            CAST('CIW_ACCT_L5' AS VARCHAR) AS ciw_acct_col,
            edw_account_ciw_dim.ciw_acct_l5 AS ciw_acct_no,
            edw_account_ciw_dim.ciw_acct_l5_txt AS ciw_acct_nm
          FROM edw_account_ciw_dim
          WHERE
            (
              LENGTH(CAST((
                edw_account_ciw_dim.ciw_acct_l5
              ) AS TEXT)) <> 0
            )
        )
        UNION ALL
        SELECT
          edw_account_ciw_dim.chrt_acct,
          edw_account_ciw_dim.acct_num,
          edw_account_ciw_dim.acct_nm,
          edw_account_ciw_dim.ciw_acct_l3_txt AS ciw_desc,
          CAST((
            CASE
              WHEN (
                (
                  (
                    (
                      (
                        (
                          SPLIT_PART(CAST((
                            edw_account_ciw_dim.ciw_acct_l3
                          ) AS TEXT), CAST('_' AS TEXT), 2) = CAST('PYT' AS TEXT)
                        )
                        OR (
                          SPLIT_PART(CAST((
                            edw_account_ciw_dim.ciw_acct_l3
                          ) AS TEXT), CAST('_' AS TEXT), 2) = CAST('GSI' AS TEXT)
                        )
                      )
                      OR (
                        SPLIT_PART(CAST((
                          edw_account_ciw_dim.ciw_acct_l3
                        ) AS TEXT), CAST('_' AS TEXT), 2) = CAST('GSD' AS TEXT)
                      )
                    )
                    OR (
                      SPLIT_PART(CAST((
                        edw_account_ciw_dim.ciw_acct_l3
                      ) AS TEXT), CAST('_' AS TEXT), 2) = CAST('EFF' AS TEXT)
                    )
                  )
                  OR (
                    SPLIT_PART(CAST((
                      edw_account_ciw_dim.ciw_acct_l3
                    ) AS TEXT), CAST('_' AS TEXT), 2) = CAST('DNI' AS TEXT)
                  )
                )
                OR (
                  SPLIT_PART(CAST((
                    edw_account_ciw_dim.ciw_acct_l3
                  ) AS TEXT), CAST('_' AS TEXT), 2) = CAST('CMNE' AS TEXT)
                )
              )
              THEN COALESCE(
                SPLIT_PART(
                  LTRIM(
                    SPLIT_PART(CAST((
                      edw_account_ciw_dim.ciw_acct_l5
                    ) AS TEXT), CAST('_' AS TEXT), 1),
                    CAST('JJPLAC' AS TEXT)
                  ),
                  CAST('-' AS TEXT),
                  1
                ),
                LTRIM(
                  SPLIT_PART(CAST((
                    edw_account_ciw_dim.ciw_acct_l4
                  ) AS TEXT), CAST('_' AS TEXT), 1),
                  CAST('JJPLAC' AS TEXT)
                )
              )
              ELSE LTRIM(
                SPLIT_PART(CAST((
                  edw_account_ciw_dim.ciw_acct_l4
                ) AS TEXT), CAST('_' AS TEXT), 1),
                CAST('JJPLAC' AS TEXT)
              )
            END
          ) AS VARCHAR) AS ciw_code,
          CASE
            WHEN (
              (
                (
                  (
                    (
                      (
                        SPLIT_PART(CAST((
                          edw_account_ciw_dim.ciw_acct_l3
                        ) AS TEXT), CAST('_' AS TEXT), 2) = CAST('PYT' AS TEXT)
                      )
                      OR (
                        SPLIT_PART(CAST((
                          edw_account_ciw_dim.ciw_acct_l3
                        ) AS TEXT), CAST('_' AS TEXT), 2) = CAST('GSI' AS TEXT)
                      )
                    )
                    OR (
                      SPLIT_PART(CAST((
                        edw_account_ciw_dim.ciw_acct_l3
                      ) AS TEXT), CAST('_' AS TEXT), 2) = CAST('GSD' AS TEXT)
                    )
                  )
                  OR (
                    SPLIT_PART(CAST((
                      edw_account_ciw_dim.ciw_acct_l3
                    ) AS TEXT), CAST('_' AS TEXT), 2) = CAST('EFF' AS TEXT)
                  )
                )
                OR (
                  SPLIT_PART(CAST((
                    edw_account_ciw_dim.ciw_acct_l3
                  ) AS TEXT), CAST('_' AS TEXT), 2) = CAST('DNI' AS TEXT)
                )
              )
              OR (
                SPLIT_PART(CAST((
                  edw_account_ciw_dim.ciw_acct_l3
                ) AS TEXT), CAST('_' AS TEXT), 2) = CAST('CMNE' AS TEXT)
              )
            )
            THEN COALESCE(edw_account_ciw_dim.ciw_acct_l5_txt, edw_account_ciw_dim.ciw_acct_l4_txt)
            ELSE edw_account_ciw_dim.ciw_acct_l4_txt
          END AS ciw_bucket,
          CAST('CIW_ACCT_L6' AS VARCHAR) AS ciw_acct_col,
          edw_account_ciw_dim.ciw_acct_l6 AS ciw_acct_no,
          edw_account_ciw_dim.ciw_acct_l6_txt AS ciw_acct_nm
        FROM edw_account_ciw_dim
        WHERE
          (
            LENGTH(CAST((
              edw_account_ciw_dim.ciw_acct_l6
            ) AS TEXT)) <> 0
          )
      ) AS a
      JOIN edw_account_ciw_xref AS b
        ON (
          (
            (
              UPPER(CAST((
                a.ciw_acct_col
              ) AS TEXT)) = UPPER(CAST((
                b.lookup_col_name
              ) AS TEXT))
            )
            AND (
              CAST((
                a.ciw_acct_no
              ) AS TEXT) = CAST((
                b.lookup_value
              ) AS TEXT)
            )
          )
        )
    )
  ) AS derived_table1

  ),
  final as(
      select 	
      chrt_acct as "chrt_acct",
      acct_num as "acct_num",
      acct_nm as "acct_nm",
      ciw_desc as "ciw_desc",
      ciw_code as "ciw_code",
      ciw_bucket as "ciw_bucket",
      ciw_acct_col as "ciw_acct_col",
      ciw_acct_no as "ciw_acct_no",
      ciw_acct_nm as "ciw_acct_nm",
      measure_code as "measure_code",
      measure_name as "measure_name",
      multiplication_factor as "multiplication_factor" 
      from tranformed
  )
  select * from final
);
