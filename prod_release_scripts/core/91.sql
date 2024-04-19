create view prod_dna_core.aspedw_access.itg_sfmc_children_data
as 
select   
    cntry_cd as "cntry_cd",
    parent_key as "parent_key",
    child_nm as "child_nm",
    child_birth_mnth as "child_birth_mnth",
    child_birth_year as "child_birth_year",
    child_gender as "child_gender",
    child_number as "child_number",
    file_name as "file_name",
    crtd_dttm as "crtd_dttm",
    updt_dttm as "updt_dttm"
from prod_dna_core.aspitg_integration.itg_sfmc_children_data;

create view prod_dna_core.aspedw_access.itg_sfmc_consumer_master
as 
select   
cntry_cd as "cntry_cd",
first_name as "first_name",
last_name as "last_name",
mobile_num as "mobile_num",
mobile_cntry_cd as "mobile_cntry_cd",
birthday_mnth as "birthday_mnth",
birthday_year as "birthday_year",
address_1 as "address_1",
address_2 as "address_2",
address_city as "address_city",
address_zipcode as "address_zipcode",
subscriber_key as "subscriber_key",
website_unique_id as "website_unique_id",
source as "source",
medium as "medium",
brand as "brand",
address_cntry as "address_cntry",
campaign_id as "campaign_id",
created_date as "created_date",
updated_date as "updated_date",
unsubscribe_date as "unsubscribe_date",
email as "email",
full_name as "full_name",
last_logon_time as "last_logon_time",
remaining_points as "remaining_points",
redeemed_points as "redeemed_points",
total_points as "total_points",
gender as "gender",
line_id as "line_id",
line_name as "line_name",
line_email as "line_email",
line_channel_id as "line_channel_id",
address_region as "address_region",
tier as "tier",
opt_in_for_communication as "opt_in_for_communication",
file_name as "file_name",
crtd_dttm as "crtd_dttm",
updt_dttm as "updt_dttm",
have_kid as "have_kid",
age as "age",
valid_from as "valid_from",
valid_to as "valid_to",
delete_flag as "delete_flag",
subscriber_status as "subscriber_status",
opt_in_for_jnj_communication as "opt_in_for_jnj_communication",
opt_in_for_campaign as "opt_in_for_campaign"
from prod_dna_core.aspitg_integration.itg_sfmc_consumer_master;

create view prod_dna_core.aspedw_access.itg_sfmc_open_data
as 
select   
    cntry_cd as "cntry_cd",
    oyb_account_id as "oyb_account_id",
    job_id as "job_id",
    list_id as "list_id",
    batch_id as "batch_id",
    subscriber_id as "subscriber_id",
    subscriber_key as "subscriber_key",
    email_name as "email_name",
    email_subject as "email_subject",
    bcc_email as "bcc_email",
    email_id as "email_id",
    event_date as "event_date",
    domain as "domain",
    is_unique as "is_unique",
    file_name as "file_name",
    crtd_dttm as "crtd_dttm",
    updt_dttm as "updt_dttm"
from prod_dna_core.aspitg_integration.itg_sfmc_open_data;

create view prod_dna_core.aspedw_access.itg_sfmc_sent_data
as 
select   
    cntry_cd as "cntry_cd",
    oyb_account_id as "oyb_account_id",
    job_id as "job_id",
    list_id as "list_id",
    batch_id as "batch_id",
    subscriber_id as "subscriber_id",
    subscriber_key as "subscriber_key",
    event_date as "event_date",
    domain as "domain",
    triggerer_send_definition_object_id as "triggerer_send_definition_object_id",
    triggered_send_customer_key as "triggered_send_customer_key",
    email_name as "email_name",
    email_subject as "email_subject",
    email_id as "email_id",
    file_name as "file_name",
    crtd_dttm as "crtd_dttm",
    updt_dttm as "updt_dttm"
from prod_dna_core.aspitg_integration.itg_sfmc_sent_data;

create view prod_dna_core.aspedw_access.itg_sfmc_click_data
as 
select   
    cntry_cd as "cntry_cd",
    oyb_account_id as "oyb_account_id",
    job_id as "job_id",
    list_id as "list_id",
    batch_id as "batch_id",
    subscriber_id as "subscriber_id",
    subscriber_key as "subscriber_key",
    event_date as "event_date",
    domain as "domain",
    url as "url",
    link_name as "link_name",
    link_content as "link_content",
    is_unique as "is_unique",
    email_name as "email_name",
    email_subject as "email_subject",
    file_name as "file_name",
    crtd_dttm as "crtd_dttm",
    updt_dttm as "updt_dttm"
from prod_dna_core.aspitg_integration.itg_sfmc_click_data;

drop view aspedw_access.edw_jb_tw_invoice_record_detail;
drop view aspedw_access.edw_jb_tw_invoice_record_master;
drop view aspedw_access.edw_regaine_tw_invoice_record_detail;
drop view aspedw_access.edw_regaine_tw_invoice_record_master;

create view prod_dna_core.aspedw_access.edw_vw_jb_tw_invoice_record_detail as 
select c.epsilon_amount as "amount", c.channel as "channel", c.completed_date as "completed_date", c.created_date as "created_date", c.invoice_num as "invoice_number", c.invoice_type as "invoice_type", c.points as "points", c.epsilon_price_per_unit as "price_per_unit", c.product as "product", c.product_category as "product_category", c.purchase_date as "purchase_date", c.qty as "quantity", c.seller_nm as "seller_name", c.show_record as "show_record", c.status as "status", c.subscriber_key as "subscriber_key", c.website_unique_id as "website_unique_id", d.full_name as "full_name", d.email as "email" from prod_dna_core.aspitg_integration.itg_sfmc_invoice_data c left join prod_dna_core.aspitg_integration.itg_sfmc_consumer_master d on c.subscriber_key = d.subscriber_key where c.subscriber_key like 'JB_TW_%';

create view prod_dna_core.aspedw_access.edw_vw_jb_tw_invoice_record_master as
select c.epsilon_amount as "amount", c.channel as "channel", c.completed_date as "completed_date", c.created_date as "created_date", c.invoice_num as "invoice_number", c.invoice_type as "invoice_type", c.points as "points", c.epsilon_price_per_unit as "price_per_unit", c.product as "product", c.product_category as "product_category", c.purchase_date as "purchase_date", c.qty as "quantity", c.seller_nm as "seller_name", c.show_record as "show_record", c.status as "status", c.subscriber_key as "subscriber_key", c.website_unique_id as "website_unique_id", d.full_name as "full_name", d.email as "email" from prod_dna_core.aspitg_integration.itg_sfmc_invoice_data c left join prod_dna_core.aspitg_integration.itg_sfmc_consumer_master d on c.subscriber_key = d.subscriber_key where c.subscriber_key like 'Regaine_TW_%';

create view prod_dna_core.aspedw_access.edw_vw_regaine_tw_invoice_record_detail as
select any_value(c.epsilon_total_amount) as "amount", any_value(c.channel) as "channel", any_value(c.completed_date) as "completed_date", any_value(c.created_date) as "created_date", c.invoice_num as "invoice_number", any_value(c.invoice_type) as "invoice_type", sum(c.points) as "points", any_value(c.product) as "product", any_value(c.product_category) as "product_category", any_value(c.purchase_date) as "purchase_date", any_value(c.qty) as "quantity", any_value(c.seller_nm) as "seller_name", any_value(c.show_record) as "show_record", any_value(c.status) as "status", any_value(c.subscriber_key) as "subscriber_key", min(c.website_unique_id) as "website_unique_id", any_value(d.full_name) as "full_name", any_value(d.email) as "email" from prod_dna_core.aspitg_integration.itg_sfmc_invoice_data c left join prod_dna_core.aspitg_integration.itg_sfmc_consumer_master d on c.subscriber_key = d.subscriber_key where c.subscriber_key like 'JB_TW_%' and c.status = 'Approved' group by c.invoice_num;

create view prod_dna_core.aspedw_access.edw_vw_regaine_tw_invoice_record_master as
select any_value(c.epsilon_total_amount) as "amount", any_value(c.channel) as "channel", any_value(c.completed_date) as "completed_date", any_value(c.created_date) as "created_date", c.invoice_num as "invoice_number", any_value(c.invoice_type) as "invoice_type", sum(c.points) as "points", any_value(c.product) as "product", any_value(c.product_category) as "product_category", any_value(c.purchase_date) as "purchase_date", any_value(c.qty) as "quantity", any_value(c.seller_nm) as "seller_name", any_value(c.show_record) as "show_record", any_value(c.status) as "status", any_value(c.subscriber_key) as "subscriber_key", min(c.website_unique_id) as "website_unique_id", any_value(d.full_name) as "full_name", any_value(d.email) as "email" from prod_dna_core.aspitg_integration.itg_sfmc_invoice_data c left join prod_dna_core.aspitg_integration.itg_sfmc_consumer_master d on c.subscriber_key = d.subscriber_key where c.subscriber_key like 'Regaine_TW_%' and c.status = 'Approved' group by c.invoice_num;
