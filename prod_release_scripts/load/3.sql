update meta_raw.parameters
set PARAMETER_VALUE = 'LTM,LTJ,SLM,SLJ,HDC,SGM,HYUNDAI DDM,HYUNDAI COEX,DONGWHA'
where parameter_group_id =25 and PARAMETER_NAME = 'sheet_names'
