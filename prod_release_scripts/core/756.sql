create table PROD_DNA_CORE.PHLITG_INTEGRATION.itg_ph_dms_sellout_sales_fact_20241213 as select * from PROD_DNA_CORE.PHLITG_INTEGRATION.itg_ph_dms_sellout_sales_fact;

delete

from PROD_DNA_CORE.PHLITG_INTEGRATION.itg_ph_dms_sellout_sales_fact

where DSTRBTR_GRP_CD = '016'

and ((DSTRBTR_CUST_ID = '000000500225'

     and INVOICE_NO ='SII152824')

     or (DSTRBTR_CUST_ID = '000000530082'

     and INVOICE_NO = 'SII152807')

     or (DSTRBTR_CUST_ID = '000000530119'

     and INVOICE_NO ='SII152801'));
