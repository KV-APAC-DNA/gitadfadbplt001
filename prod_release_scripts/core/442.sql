delete from  ntaitg_integration.itg_pos  where   ctry_cd = 'KR' and pos_dt = '2024-10-02'
and src_sys_cd = 'Emart';

insert into ntaitg_integration.itg_pos
select * from ntaitg_integration.itg_pos_hist_load  where  ctry_cd = 'KR' and pos_dt = '2024-10-02'
and src_sys_cd = 'Emart';
