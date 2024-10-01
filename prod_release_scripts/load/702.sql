UPDATE META_RAW.PARAMETERS SET  PARAMETER_VALUE='SELECT * FROM [MDS].[MDM].[MY_PS_Targets] where ValidationStatus<>''Validation Failed''' WHERE PARAMETER_ID='25371';

UPDATE META_RAW.PARAMETERS SET  PARAMETER_VALUE='SELECT * FROM mdm.KR_Keyword_Classifications where ValidationStatus<>''Validation Failed''' WHERE PARAMETER_ID='12613';

UPDATE META_RAW.PARAMETERS SET  PARAMETER_VALUE='SELECT * FROM MDS.mdm.KR_TP_Target where ValidationStatus<>''Validation Failed''' WHERE PARAMETER_ID='12589';
