create table phledw_integration.edw_ph_sellout_analysis_bkp as select * from phledw_integration.edw_ph_sellout_analysis;

Drop table phledw_integration.edw_ph_sellout_analysis;

create table phledw_integration.edw_ph_sellout_analysis as select * from phledw_integration.edw_vw_ph_sellout_analysis;
