update PARAMETERS
set parameter_group_name = 'edw_rpt_regional_sellout_offtake_CN_group'
where parameter_group_id = '2233';
update PARAMETERS
set parameter_value = 'edw_rpt_regional_sellout_offtake_CN'
where parameter_id = '27283';
update PARAMETERS
set parameter_value = 'sql_server/MDS_Reverse_Sync/edw_rpt_regional_sellout_offtake_CN/'
where parameter_id = '27282';;
update PARAMETERS
set parameter_value = 'dbo.RS_rg_edw_rpt_regional_sellout_offtake_CN'
where parameter_id = '27284';

update META_RAW.PROCESS
set process_name = 'edw_rpt_regional_sellout_offtake_CN'
where process_id = '2233';
