update prod_dna_core.thaitg_integration.sdl_th_mt_bigc_raw set crt_dttm=to_timestamp(substring(split_part(file_name, '_',3),0,14),'yyyymmddhh24miss')  where crt_dttm is null;
