truncate table pcfitg_integration.itg_perenso_over_and_above_points;
 
 
insert into pcfitg_integration.itg_perenso_over_and_above_points

select 

    display_type::varchar(256) as oa_display_type,

    points::number(10,3) as points,

    run_id::number(14,0) as run_id,

    current_timestamp()::timestamp_ntz(9) as create_dt,

    current_timestamp()::timestamp_ntz(9) as update_dt

from PROD_DNA_LOAD.pcfsdl_raw.sdl_over_and_above_points;
 
