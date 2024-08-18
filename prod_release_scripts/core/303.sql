UPDATE PROD_DNA_CORE.NTAITG_INTEGRATION.itg_ims_dstr_cust_attr
                    SET store_type = t2.store_type,
                        hq = t2.hq
                    FROM PROD_DNA_CORE.NTAITG_INTEGRATION.itg_tw_ims_dstr_customer_mapping t2
                        JOIN PROD_DNA_CORE.NTAITG_INTEGRATION.itg_ims_dstr_cust_attr t1 ON 
                        RTRIM(LTRIM (t1.dstr_cust_cd, '0')) = RTRIM(LTRIM (t2.distributors_customer_code, '0'))
                        AND RTRIM(t1.dstr_cd) = RTRIM(t2.distributor_code)
                        AND t1.ctry_cd = 'TW';
