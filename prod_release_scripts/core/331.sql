delete from prod_dna_core.ntaitg_integration.itg_ims
    where ctry_cd = 'HK' and dstr_cd = '110256' and to_date(crt_dttm) in ('2024-09-03');
    delete from prod_dna_core.ntaitg_integration.itg_ims_temp
    where ctry_cd = 'HK' and dstr_cd = '110256' and to_date(crt_dttm) in ('2024-09-03');
