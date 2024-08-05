update prod_dna_core.inditg_integration.itg_pos_offtake_fact
set file_upload_date = to_date(right(replace(source_file_name,right(source_file_name,5),''),8),'yyyymmdd')
where to_date(right(replace(source_file_name,right(source_file_name,5),''),8),'yyyymmdd') <> file_upload_date
