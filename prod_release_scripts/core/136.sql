create or replace view PROD_DNA_CORE.ASPEDW_ACCESS.EDW_VW_JB_TW_INVOICE_RECORD_MASTER(
	"amount",
	"channel",
	"completed_date",
	"created_date",
	"invoice_number",
	"invoice_type",
	"points",
	"price_per_unit",
	"product",
	"product_category",
	"purchase_date",
	"quantity",
	"seller_name",
	"show_record",
	"status",
	"subscriber_key",
	"website_unique_id",
	"full_name",
	"email"
) as
select c.epsilon_amount as "amount", c.channel as "channel", c.completed_date as "completed_date", c.created_date as "created_date", c.invoice_num as "invoice_number", c.invoice_type as "invoice_type", c.points as "points", c.epsilon_price_per_unit as "price_per_unit", c.product as "product", c.product_category as "product_category", c.purchase_date as "purchase_date", c.qty as "quantity", c.seller_nm as "seller_name", c.show_record as "show_record", c.status as "status", c.subscriber_key as "subscriber_key", c.website_unique_id as "website_unique_id", d.full_name as "full_name", d.email as "email" from prod_dna_core.aspitg_integration.itg_sfmc_invoice_data c left join prod_dna_core.aspitg_integration.itg_sfmc_consumer_master d on c.subscriber_key = d.subscriber_key where c.subscriber_key like 'JB_TW_%';

create or replace view PROD_DNA_CORE.ASPEDW_ACCESS.EDW_VW_REGAINE_TW_INVOICE_RECORD_DETAIL(
	"amount",
	"channel",
	"completed_date",
	"created_date",
	"invoice_number",
	"invoice_type",
	"points",
	"product",
	"product_category",
	"purchase_date",
	"quantity",
	"seller_name",
	"show_record",
	"status",
	"subscriber_key",
	"website_unique_id",
	"full_name",
	"email"
) as
select any_value(c.epsilon_total_amount) as "amount", any_value(c.channel) as "channel", any_value(c.completed_date) as "completed_date", any_value(c.created_date) as "created_date", c.invoice_num as "invoice_number", any_value(c.invoice_type) as "invoice_type", sum(c.points) as "points", any_value(c.product) as "product", any_value(c.product_category) as "product_category", any_value(c.purchase_date) as "purchase_date", any_value(c.qty) as "quantity", any_value(c.seller_nm) as "seller_name", any_value(c.show_record) as "show_record", any_value(c.status) as "status", any_value(c.subscriber_key) as "subscriber_key", min(c.website_unique_id) as "website_unique_id", any_value(d.full_name) as "full_name", any_value(d.email) as "email" from prod_dna_core.aspitg_integration.itg_sfmc_invoice_data c left join prod_dna_core.aspitg_integration.itg_sfmc_consumer_master d on c.subscriber_key = d.subscriber_key where c.subscriber_key like 'Regaine_TW_%' and c.status = 'Approved' group by c.invoice_num;
