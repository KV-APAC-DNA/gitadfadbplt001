update meta_raw.historical_obj_metadata
set ISACTIVE = FALSE;

update meta_raw.historical_obj_metadata
set ISACTIVE = TRUE
WHERE MARKET in ('NORTH_ASIA','India') and source_schema in ('NA_EDW','NA_ITG','IN_EDW','IN_ITG');
