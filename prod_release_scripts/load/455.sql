update meta_raw.process 
set IS_ACTIVE = FALSE
where process_id in(77) and phase_id = 3;
