delete from phlitg_integration.itg_ph_tbl_surveyanswers where filename = 'ISE_tbl_SurveyAnswers_PH_20240802020003.csv'
and run_id = '20240803022216';

create table phlitg_integration.ITG_MDS_PH_REF_POS_PRIMARY_SOLD_TO_bkp as select * from phlitg_integration.ITG_MDS_PH_REF_POS_PRIMARY_SOLD_TO;
