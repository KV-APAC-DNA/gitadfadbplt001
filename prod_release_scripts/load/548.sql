delete from PROD_DNA_LOAD.META_RAW.HISTORICAL_OBJ_METADATA
where 
target_table in ('EDW_SKU_RECOM_SPIKE_MSL_SYNC','EDW_RETAILER_DIM_SYNC','EDW_PRODUCT_DIM_SYNC','EDW_CUSTOMER_DIM_SYNC','ITG_IN_MDS_CHANNEL_MAPPING_SYNC','ITG_UDCDETAILS_SYNC');