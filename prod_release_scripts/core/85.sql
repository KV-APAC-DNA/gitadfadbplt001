CREATE OR REPLACE TABLE ASPITG_INTEGRATION.ITG_SFMC_INVOICE_DATA (
	cntry_cd varchar(10),
	purchase_date timestamp_ntz(9),
	channel varchar(200),
	product varchar(200),
	status varchar(50),
	created_date timestamp_ntz(9),
	completed_date timestamp_ntz(9),
	subscriber_key varchar(100),
	points number(18,0),
	show_record varchar(20),
	qty number(20,4),
	invoice_type varchar(200),
	seller_nm varchar(255),
	product_category varchar(200),
	website_unique_id varchar(150),
	invoice_num varchar(50),
	epsilon_price_per_unit number(20,4),
	epsilon_amount number(20,4),
	epsilon_total_amount number(20,4),
	file_name varchar(255),
	crtd_dttm timestamp_ntz(9),
	updt_dttm timestamp_ntz(9)
);

CREATE OR REPLACE VIEW ASPEDW_ACCESS.ITG_SFMC_INVOICE_DATA AS 
SELECT 
cntry_cd AS "cntry_cd",
purchase_date AS "purchase_date",
channel  AS "channel",
product  AS "product",
status AS "status",
created_date AS "created_date",
completed_date AS "completed_date",
subscriber_key  AS "subscriber_key",
points  AS "points",
show_record AS "show_record",
qty  AS "qty",
invoice_type  AS "invoice_type",
seller_nm  AS "seller_nm",
product_category  AS "product_category",
website_unique_id  AS "website_unique_id",
invoice_num AS "invoice_num",
epsilon_price_per_unit  AS "epsilon_price_per_unit",
epsilon_amount  AS "epsilon_amount",
epsilon_total_amount  AS "epsilon_total_amount",
file_name  AS "file_name",
crtd_dttm AS "crtd_dttm",
updt_dttm AS "updt_dttm"
FROM ASPITG_INTEGRATION.ITG_SFMC_REDEMPTION_DATA;

create or replace TABLE ASPITG_INTEGRATION.ITG_SFMC_REDEMPTION_DATA (
	CNTRY_CD VARCHAR(10),
	PROD_NM VARCHAR(255),
	REDEEMED_POINTS NUMBER(20,4),
	QTY NUMBER(20,4),
	REDEEMED_DATE TIMESTAMP_NTZ(9),
	STATUS VARCHAR(100),
	COMPLETED_DATE TIMESTAMP_NTZ(9),
	SUBSCRIBER_KEY VARCHAR(100),
	CREATED_DATE TIMESTAMP_NTZ(9),
	ORDER_NUM VARCHAR(50),
	WEBSITE_UNIQUE_ID VARCHAR(50),
	FILE_NAME VARCHAR(255),
	CRTD_DTTM TIMESTAMP_NTZ(9),
	UPDT_DTTM TIMESTAMP_NTZ(9)
);

CREATE OR REPLACE VIEW ASPEDW_ACCESS.ITG_SFMC_REDEMPTION_DATA AS 
SELECT 
cntry_cd AS "cntry_cd",
prod_nm AS "prod_nm",
redeemed_points AS "redeemed_points",
qty AS "qty",
redeemed_date AS "redeemed_date",
status AS "status",
completed_date AS "completed_date",
subscriber_key AS "subscriber_key",
created_date AS "created_date",
order_num AS "order_num",
website_unique_id AS "website_unique_id",
file_name AS "file_name",
crtd_dttm AS "crtd_dttm",
updt_dtt AS "updt_dttm"
FROM ASPITG_INTEGRATION.ITG_SFMC_REDEMPTION_DATA;

create view prod_dna_core.aspedw_access.edw_jb_tw_invoice_record_detail as 
select c.epsilon_amount as "amount", c.channel as "channel", c.completed_date as "completed_date", c.created_date as "created_date", c.invoice_num as "invoice_number", c.invoice_type as "invoice_type", c.points as "points", c.epsilon_price_per_unit as "price_per_unit", c.product as "product", c.product_category as "product_category", c.purchase_date as "purchase_date", c.qty as "quantity", c.seller_nm as "seller_name", c.show_record as "show_record", c.status as "status", c.subscriber_key as "subscriber_key", c.website_unique_id as "website_unique_id", d.full_name as "full_name", d.email as "email" from prod_dna_core.aspitg_integration.itg_sfmc_invoice_data c left join prod_dna_core.aspitg_integration.itg_sfmc_consumer_master d on c.subscriber_key = d.subscriber_key where c.subscriber_key like 'JB_TW_%';

create view prod_dna_core.aspedw_access.edw_jb_tw_invoice_record_master as
select c.epsilon_amount as "amount", c.channel as "channel", c.completed_date as "completed_date", c.created_date as "created_date", c.invoice_num as "invoice_number", c.invoice_type as "invoice_type", c.points as "points", c.epsilon_price_per_unit as "price_per_unit", c.product as "product", c.product_category as "product_category", c.purchase_date as "purchase_date", c.qty as "quantity", c.seller_nm as "seller_name", c.show_record as "show_record", c.status as "status", c.subscriber_key as "subscriber_key", c.website_unique_id as "website_unique_id", d.full_name as "full_name", d.email as "email" from prod_dna_core.aspitg_integration.itg_sfmc_invoice_data c left join prod_dna_core.aspitg_integration.itg_sfmc_consumer_master d on c.subscriber_key = d.subscriber_key where c.subscriber_key like 'Regaine_TW_%';

create view prod_dna_core.aspedw_access.edw_regaine_tw_invoice_record_detail as
select any_value(c.epsilon_total_amount) as "amount", any_value(c.channel) as "channel", any_value(c.completed_date) as "completed_date", any_value(c.created_date) as "created_date", c.invoice_num as "invoice_number", any_value(c.invoice_type) as "invoice_type", sum(c.points) as "points", any_value(c.product) as "product", any_value(c.product_category) as "product_category", any_value(c.purchase_date) as "purchase_date", any_value(c.qty) as "quantity", any_value(c.seller_nm) as "seller_name", any_value(c.show_record) as "show_record", any_value(c.status) as "status", any_value(c.subscriber_key) as "subscriber_key", min(c.website_unique_id) as "website_unique_id", any_value(d.full_name) as "full_name", any_value(d.email) as "email" from prod_dna_core.aspitg_integration.itg_sfmc_invoice_data c left join prod_dna_core.aspitg_integration.itg_sfmc_consumer_master d on c.subscriber_key = d.subscriber_key where c.subscriber_key like 'JB_TW_%' and c.status = 'Approved' group by c.invoice_num;

create view prod_dna_core.aspedw_access.edw_regaine_tw_invoice_record_master as
select any_value(c.epsilon_total_amount) as "amount", any_value(c.channel) as "channel", any_value(c.completed_date) as "completed_date", any_value(c.created_date) as "created_date", c.invoice_num as "invoice_number", any_value(c.invoice_type) as "invoice_type", sum(c.points) as "points", any_value(c.product) as "product", any_value(c.product_category) as "product_category", any_value(c.purchase_date) as "purchase_date", any_value(c.qty) as "quantity", any_value(c.seller_nm) as "seller_name", any_value(c.show_record) as "show_record", any_value(c.status) as "status", any_value(c.subscriber_key) as "subscriber_key", min(c.website_unique_id) as "website_unique_id", any_value(d.full_name) as "full_name", any_value(d.email) as "email" from prod_dna_core.aspitg_integration.itg_sfmc_invoice_data c left join prod_dna_core.aspitg_integration.itg_sfmc_consumer_master d on c.subscriber_key = d.subscriber_key where c.subscriber_key like 'Regaine_TW_%' and c.status = 'Approved' group by c.invoice_num;
