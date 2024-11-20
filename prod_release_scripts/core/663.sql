alter table IDNITG_INTEGRATION.itg_id_ps_promotion_competitor modify column description varchar(2000);

delete from IDNITG_INTEGRATION.itg_id_ps_promotion_competitor where input_date like '2023%';
