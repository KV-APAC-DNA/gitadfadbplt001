update thasdl_raw.sdl_pop6_th_pops_test
set customer = case when sales_group_name = 'GT BigMinimart' then 'GT Big MM' else sales_group_name end where  country like 'Thai%';
