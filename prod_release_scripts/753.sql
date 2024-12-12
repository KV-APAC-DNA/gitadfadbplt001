INSERT  OVERWRITE INTO JPDCLEDW_INTEGRATION.dm_user_attr_sfcc_v_prev
select  * from JPDCLEDW_INTEGRATION.JPDCLEDW_INTEGRATION.dm_user_attr_sfcc_v_prev1;

commit;

INSERT  OVERWRITE INTO JPDCLEDW_INTEGRATION.dm_kesai_mart_dly_general_prev
select  * from JPDCLEDW_INTEGRATION.JPDCLEDW_INTEGRATION.dm_kesai_mart_dly_general_prev1;

commit;

INSERT  OVERWRITE INTO JPDCLEDW_INTEGRATION.dm_user_attr_prev
select  * from JPDCLEDW_INTEGRATION.JPDCLEDW_INTEGRATION.dm_user_attr_prev1;

commit;

INSERT  OVERWRITE INTO JPDCLEDW_INTEGRATION.kr_new_stage_point_prev
select  * from JPDCLEDW_INTEGRATION.JPDCLEDW_INTEGRATION.kr_new_stage_point_prev1;

commit;


INSERT  OVERWRITE INTO JPDCLEDW_INTEGRATION.teikikeiyaku_data_mart_uni_prev
select  * from JPDCLEDW_INTEGRATION.JPDCLEDW_INTEGRATION.teikikeiyaku_data_mart_uni_prev1;

commit;

