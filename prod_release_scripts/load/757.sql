TRUNCATE TABLE IDNSDL_RAW.SDL_MDS_ID_distributor_customer_update_ADFTemp;
CALL ASPSDL_RAW.ADLSTOTABLEDATALOAD('IDNSDL_RAW','SDL_MDS_ID_distributor_customer_update_ADFTemp','qa/sql_server/MDS/MDS_Adhoc/','ID_distributor_customer_update_ADFTemp');
