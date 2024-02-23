delete from aspitg_integration.itg_copa_trans
where file_name='SAP_BW_COPA10_20240221_020315.csv';
--=============================
DELETE FROM PROD_DNA_CORE.ASPITG_INTEGRATION.ITG_INVC_SLS
WHERE FILE_NAME IN(
'SAP_BW_ZC_SD_20240221_011039.csv',
'SAP_BW_ZC_SD_20240220_011101.csv');
