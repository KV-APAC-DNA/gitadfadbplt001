create view prod_dna_core.hcposeedw_access.vw_rep_time_on_off_territory as
select country as "country",
    time_on_off as "time_on_off",
    date as "date",
    year as "year",
    month as "month",
    quarter as "quarter",
    jnj_year as "jnj_year",
    jnj_month as "jnj_month",
    jnj_quarter as "jnj_quarter",
    my_year as "my_year",
    my_month as "my_month",
    my_quarter as "my_quarter",
    reason as "reason",
    hours_off as "hours_off",
    hours_on as "hours_on",
    working_days as "working_days",
    duration as "duration",
    l3_wwid as "l3_wwid",
    l3_username as "l3_username",
    l3_manager_name as "l3_manager_name",
    l2_wwid as "l2_wwid",
    l2_username as "l2_username",
    l2_manager_name as "l2_manager_name",
    l1_wwid as "l1_wwid",
    l1_username as "l1_username",
    l1_manager_name as "l1_manager_name",
    sales_rep_ntid as "sales_rep_ntid",
    sales_rep as "sales_rep",
    organization_l1_name as "organization_l1_name",
    organization_l2_name as "organization_l2_name",
    organization_l3_name as "organization_l3_name",
    organization_l4_name as "organization_l4_name",
    organization_l5_name as "organization_l5_name",
    flag as "flag"
from prod_dna_core.hcposeedw_integration.vw_rep_time_on_off_territory;
create view prod_dna_core.hcposeedw_access.vw_rep_call_activity as
select jnj_year as "jnj_year",
    jnj_month as "jnj_month",
    jnj_quarter as "jnj_quarter",
    date_year as "date_year",
    date_month as "date_month",
    date_quarter as "date_quarter",
    my_year as "my_year",
    my_month as "my_month",
    my_quarter as "my_quarter",
    country as "country",
    sector as "sector",
    l3_wwid as "l3_wwid",
    l3_username as "l3_username",
    l3_manager_name as "l3_manager_name",
    l2_wwid as "l2_wwid",
    l2_username as "l2_username",
    l2_manager_name as "l2_manager_name",
    l1_wwid as "l1_wwid",
    l1_username as "l1_username",
    l1_manager_name as "l1_manager_name",
    organization_l1_name as "organization_l1_name",
    organization_l2_name as "organization_l2_name",
    organization_l3_name as "organization_l3_name",
    organization_l4_name as "organization_l4_name",
    organization_l5_name as "organization_l5_name",
    sales_rep as "sales_rep",
    working_days as "working_days",
    total_cnt_call as "total_cnt_call",
    total_cnt_edetailing_calls as "total_cnt_edetailing_calls",
    total_cnt_call_delay as "total_cnt_call_delay",
    sales_rep_ntid as "sales_rep_ntid",
    total_cnt_call_delay_sub as "total_cnt_call_delay_sub",
    total_cnt_clm_flg as "total_cnt_clm_flg",
    total_prnt_cnt_clm_flg as "total_prnt_cnt_clm_flg",
    total_cnt_submitted_calls as "total_cnt_submitted_calls",
    cnt_total_time_on as "cnt_total_time_on",
    cnt_total_time_off as "cnt_total_time_off",
    total_active as "total_active",
    detailed_products as "detailed_products",
    total_sbmtd_calls_key_message as "total_sbmtd_calls_key_message"
from prod_dna_core.hcposeedw_integration.vw_rep_call_activity;
create view prod_dna_core.hcpedw_access.edw_hcp360_kpi_rpt as
select country as "country",
    source_system as "source_system",
    channel as "channel",
    activity_type as "activity_type",
    hcp_master_id as "hcp_master_id",
    employee_id as "employee_id",
    brand as "brand",
    brand_category as "brand_category",
    speciality as "speciality",
    core_noncore as "core_noncore",
    classification as "classification",
    territory as "territory",
    zone as "zone",
    hcp_created_date as "hcp_created_date",
    activity_date as "activity_date",
    call_source_id as "call_source_id",
    product_indication_name as "product_indication_name",
    no_of_prescription_units as "no_of_prescription_units",
    first_prescription_date as "first_prescription_date",
    planned_call_cnt as "planned_call_cnt",
    call_duration as "call_duration",
    prescription_id as "prescription_id",
    noofprescritions as "noofprescritions",
    noofprescribers as "noofprescribers",
    email_name as "email_name",
    is_unique as "is_unique",
    email_delivered_flag as "email_delivered_flag",
    crt_dttm as "crt_dttm",
    updt_dttm as "updt_dttm",
    target_value as "target_value",
    target_kpi as "target_kpi",
    report_brand_reference as "report_brand_reference",
    diagnosis as "diagnosis",
    region_hq as "region_hq",
    email_activity_type as "email_activity_type",
    hcp_id as "hcp_id",
    transaction_flag as "transaction_flag",
    iqvia_brand as "iqvia_brand",
    iqvia_pack_description as "iqvia_pack_description",
    iqvia_product_description as "iqvia_product_description",
    iqvia_pack_volume as "iqvia_pack_volume",
    iqvia_input_brand as "iqvia_input_brand",
    mat_noofprescritions as "mat_noofprescritions",
    mat_noofprescribers as "mat_noofprescribers",
    field_rep_active as "field_rep_active",
    mat_totalprescritions_by_brand as "mat_totalprescritions_by_brand",
    mat_totalprescribers_by_brand as "mat_totalprescribers_by_brand",
    mat_totalprescritions_jnj_brand as "mat_totalprescritions_jnj_brand",
    totalprescritions_by_brand as "totalprescritions_by_brand",
    totalprescribers_by_brand as "totalprescribers_by_brand",
    totalprescritions_jnj_brand as "totalprescritions_jnj_brand",
    call_type as "call_type",
    email_subject as "email_subject",
    totalprescritions_by_speciality as "totalprescritions_by_speciality",
    totalprescribers_by_speciality as "totalprescribers_by_speciality",
    totalprescritions_jnj_speciality as "totalprescritions_jnj_speciality",
    totalprescritions_by_indication as "totalprescritions_by_indication",
    totalprescribers_by_indication as "totalprescribers_by_indication",
    totalprescritions_jnj_indication as "totalprescritions_jnj_indication",
    year_month as "year_month",
    devicecategory as "devicecategory",
    channelgrouping as "channelgrouping",
    visitor_country as "visitor_country",
    new_visitors as "new_visitors",
    repeat_visitors as "repeat_visitors",
    all_visitor as "all_visitor",
    unique_pageviews as "unique_pageviews",
    total_downloads as "total_downloads",
    pages as "pages",
    page_sessions as "page_sessions",
    total_page_views as "total_page_views",
    total_bounces as "total_bounces",
    total_session_duration as "total_session_duration",
    sessions as "sessions",
    territory_id as "territory_id",
    region as "region",
    sales_unit as "sales_unit",
    sales_value as "sales_value",
    totalsales_unit_by_brand as "totalsales_unit_by_brand",
    totalsales_value_by_brand as "totalsales_value_by_brand",
    totalsales_unit_by_jnj_brand as "totalsales_unit_by_jnj_brand",
    totalsales_value_by_jnj_brand as "totalsales_value_by_jnj_brand",
    medical_event_id as "medical_event_id",
    event_name as "event_name",
    event_type as "event_type",
    event_role as "event_role",
    attendee_status as "attendee_status",
    event_location as "event_location",
    survey_question as "survey_question",
    survey_response as "survey_response",
    attendee_name as "attendee_name",
    key_message as "key_message",
    sales_channel as "sales_channel",
    sales_area as "sales_area",
    sales_rep_count as "sales_rep_count",
    survey_name as "survey_name",
    sample_id as "sample_id",
    customer_code as "customer_code",
    customer_name as "customer_name",
    retailer_code as "retailer_code",
    retailer_name as "retailer_name",
    retailer_category_cd as "retailer_category_cd",
    retailer_category_name as "retailer_category_name",
    num_buying_retailer as "num_buying_retailer",
    is_active as "is_active",
    event_status as "event_status",
    category as "category",
    udc_avbabybodydocq42019 as "udc_avbabybodydocq42019",
    udc_babyprofesionalcac2019 as "udc_babyprofesionalcac2019",
    variant_name as "variant_name"
from prod_dna_core.hcpedw_integration.edw_hcp360_kpi_rpt;
create view prod_dna_core.hcpedw_access.edw_hcp360_dwnld_kpi_rpt as
select brand as "brand",
    ventasys_id as "ventasys_id",
    ventasys_name as "ventasys_name",
    ventasys_mobile as "ventasys_mobile",
    ventasys_email as "ventasys_email",
    veeva_id as "veeva_id",
    veeva_name as "veeva_name",
    veeva_mobile as "veeva_mobile",
    veeva_email as "veeva_email",
    sfmc_id as "sfmc_id",
    sfmc_name as "sfmc_name",
    sfmc_mobile as "sfmc_mobile",
    sfmc_email as "sfmc_email",
    master_hcp_key as "master_hcp_key",
    source_system as "source_system",
    hcp_id as "hcp_id",
    customer_name as "customer_name",
    cell_phone as "cell_phone",
    email as "email",
    account_source_id as "account_source_id",
    ventasys_team_name as "ventasys_team_name",
    ventasys_custid as "ventasys_custid",
    subscriber_key as "subscriber_key",
    data_source as "data_source",
    country_code as "country_code",
    region as "region",
    zone as "zone",
    area as "area",
    classification as "classification",
    speciality as "speciality",
    core_noncore as "core_noncore",
    year as "year",
    month as "month",
    hcp_created_date as "hcp_created_date",
    territory_id as "territory_id",
    region_hq as "region_hq",
    is_active as "is_active",
    is_active_msr as "is_active_msr",
    first_prescription_date as "first_prescription_date",
    prescriber_type as "prescriber_type",
    total_prescriptions as "total_prescriptions",
    prescription_units as "prescription_units",
    unique_product_type as "unique_product_type",
    planned_visits as "planned_visits",
    actual_visits as "actual_visits",
    phone_connects as "phone_connects",
    video_connects as "video_connects",
    f2f_connects as "f2f_connects",
    avg_prod_detailed as "avg_prod_detailed",
    lbl_given as "lbl_given",
    samples_given as "samples_given",
    emails_sent as "emails_sent",
    emails_delivered as "emails_delivered",
    emails_opened as "emails_opened",
    emails_clicked as "emails_clicked",
    emails_unique_clicked as "emails_unique_clicked",
    emails_unsusbscribed as "emails_unsusbscribed",
    emails_forwarded as "emails_forwarded",
    average_call_duration as "average_call_duration",
    events_registered as "events_registered",
    events_attended as "events_attended",
    events_as_speaker as "events_as_speaker",
    avg_key_msgs_delivered as "avg_key_msgs_delivered",
    master_id as "master_id",
    activity_date as "activity_date",
    prescription_id as "prescription_id"
from prod_dna_core.hcpedw_integration.edw_hcp360_dwnld_kpi_rpt;
create view prod_dna_core.hcposeedw_access.rep_level_kpi as
select country as "country",
    jnj_date_year as "jnj_date_year",
    jnj_date_month as "jnj_date_month",
    jnj_date_quarter as "jnj_date_quarter",
    date_year as "date_year",
    date_month as "date_month",
    date_quarter as "date_quarter",
    my_date_year as "my_date_year",
    my_date_month as "my_date_month",
    my_date_quarter as "my_date_quarter",
    sector as "sector",
    l3_username as "l3_username",
    l3_manager_name as "l3_manager_name",
    l2_wwid as "l2_wwid",
    l2_username as "l2_username",
    l2_manager_name as "l2_manager_name",
    l1_wwid as "l1_wwid",
    l1_username as "l1_username",
    l1_manager_name as "l1_manager_name",
    sales_rep_ntid as "sales_rep_ntid",
    organization_l1_name as "organization_l1_name",
    organization_l2_name as "organization_l2_name",
    organization_l3_name as "organization_l3_name",
    organization_l4_name as "organization_l4_name",
    organization_l5_name as "organization_l5_name",
    sales_rep as "sales_rep",
    working_days as "working_days",
    total_calls as "total_calls",
    total_cnt_call_delay as "total_cnt_call_delay",
    total_call_edetailing as "total_call_edetailing",
    call_total_active_user as "call_total_active_user",
    total_calls_with_product as "total_calls_with_product",
    total_sbmtd_calls_key_message as "total_sbmtd_calls_key_message",
    total_key_message as "total_key_message",
    total_call_classification_a as "total_call_classification_a",
    total_call_classification_b as "total_call_classification_b",
    total_call_classification_c as "total_call_classification_c",
    total_call_classification_d as "total_call_classification_d",
    total_call_classification_u as "total_call_classification_u",
    total_call_classification_z as "total_call_classification_z",
    total_call_classification_no_product as "total_call_classification_no_product",
    total_detailing as "total_detailing",
    coaching_team as "coaching_team",
    coaching_status as "coaching_status",
    total_coaching_report as "total_coaching_report",
    total_coaching_visit as "total_coaching_visit",
    coaching_manager as "coaching_manager",
    coaching_sales_rep as "coaching_sales_rep",
    planned_calls as "planned_calls",
    attainment as "attainment",
    actual_calls as "actual_calls",
    cpa_100 as "cpa_100",
    target_cpa_status as "target_cpa_status",
    product_cpa_status as "product_cpa_status",
    planned_call_detail_count as "planned_call_detail_count",
    cycle_plan_detail_attainment as "cycle_plan_detail_attainment",
    actual_call_detail_count as "actual_call_detail_count",
    cfa_100 as "cfa_100",
    cfa_33 as "cfa_33",
    cfa_66 as "cfa_66"
from prod_dna_core.hcposeedw_integration.rep_level_kpi;
create view prod_dna_core.hcposeedw_access.vw_rep_cycle_plan as
select date as "date",
    year as "year",
    month as "month",
    quarter as "quarter",
    country as "country",
    hcp_name as "hcp_name",
    hcp_key as "hcp_key",
    hcp_source_id as "hcp_source_id",
    specialty as "specialty",
    sector as "sector",
    business_account as "business_account",
    plan as "plan",
    attainment as "attainment",
    actual as "actual",
    cpa_100 as "cpa_100",
    hcp_count as "hcp_count",
    cpa_status as "cpa_status",
    l3_wwid as "l3_wwid",
    l3_username as "l3_username",
    l3_manager_name as "l3_manager_name",
    l2_wwid as "l2_wwid",
    l2_username as "l2_username",
    l2_manager_name as "l2_manager_name",
    l1_wwid as "l1_wwid",
    l1_username as "l1_username",
    l1_manager_name as "l1_manager_name",
    sales_rep_ntid as "sales_rep_ntid",
    sales_rep as "sales_rep",
    classification_type as "classification_type",
    plan_product as "plan_product",
    actual_product as "actual_product",
    attainment_product as "attainment_product",
    product as "product",
    organization_l1_name as "organization_l1_name",
    organization_l2_name as "organization_l2_name",
    organization_l3_name as "organization_l3_name",
    organization_l4_name as "organization_l4_name",
    organization_l5_name as "organization_l5_name",
    total_active as "total_active",
    hcp_customer_code_2 as "hcp_customer_code_2",
    account_segmentation as "account_segmentation",
    business_account_id as "business_account_id"
from prod_dna_core.hcposeedw_integration.vw_rep_cycle_plan;
create view prod_dna_core.hcposeedw_access.vw_rep_cycle_plan_product as
select date as "date",
    year as "year",
    month as "month",
    quarter as "quarter",
    country as "country",
    hcp_name as "hcp_name",
    hcp_key as "hcp_key",
    hcp_source_id as "hcp_source_id",
    specialty as "specialty",
    sector as "sector",
    business_account as "business_account",
    plan as "plan",
    attainment as "attainment",
    actual as "actual",
    cpa_100 as "cpa_100",
    hcp_count as "hcp_count",
    cpa_status as "cpa_status",
    l3_wwid as "l3_wwid",
    l3_username as "l3_username",
    l3_manager_name as "l3_manager_name",
    l2_wwid as "l2_wwid",
    l2_username as "l2_username",
    l2_manager_name as "l2_manager_name",
    l1_wwid as "l1_wwid",
    l1_username as "l1_username",
    l1_manager_name as "l1_manager_name",
    sales_rep_ntid as "sales_rep_ntid",
    sales_rep as "sales_rep",
    classification_type as "classification_type",
    plan_product as "plan_product",
    actual_product as "actual_product",
    attainment_product as "attainment_product",
    product as "product",
    organization_l1_name as "organization_l1_name",
    organization_l2_name as "organization_l2_name",
    organization_l3_name as "organization_l3_name",
    organization_l4_name as "organization_l4_name",
    organization_l5_name as "organization_l5_name",
    total_active as "total_active",
    hcp_customer_code_2 as "hcp_customer_code_2",
    account_segmentation as "account_segmentation",
    business_account_id as "business_account_id",
    cyc_count as "cyc_count"
from prod_dna_core.hcposeedw_integration.vw_rep_cycle_plan_product;
create view prod_dna_core.hcposeedw_access.vw_user_dashboard as
select year as "year",
    month as "month",
    country as "country",
    sector as "sector",
    usr_name as "usr_name",
    last30_cnt as "last30_cnt",
    total_sales_manager as "total_sales_manager",
    total_msl as "total_msl",
    total_marketing as "total_marketing",
    total_others as "total_others",
    license_qty as "license_qty",
    wwid as "wwid",
    manager as "manager",
    profile_category as "profile_category",
    role as "role",
    company_name as "company_name",
    last_login_date as "last_login_date",
    active_flag as "active_flag",
    license_type as "license_type",
    last30_flag_sales_rep as "last30_flag_sales_rep",
    last30_flag_sales_manager as "last30_flag_sales_manager",
    last30_flag_total_msl as "last30_flag_total_msl",
    last30_flag_total_marketing as "last30_flag_total_marketing",
    last30_flag_total_others as "last30_flag_total_others",
    total_sales_rep as "total_sales_rep",
    monthly_active_login as "monthly_active_login",
    profile_name as "profile_name"
from prod_dna_core.hcposeedw_integration.vw_user_dashboard;
create view prod_dna_core.hcpedw_access.edw_hcp360_iqvia_kpi_rpt as
select brand as "brand",
    ventasys_id as "ventasys_id",
    ventasys_name as "ventasys_name",
    ventasys_mobile as "ventasys_mobile",
    ventasys_email as "ventasys_email",
    veeva_id as "veeva_id",
    veeva_name as "veeva_name",
    veeva_mobile as "veeva_mobile",
    veeva_email as "veeva_email",
    sfmc_id as "sfmc_id",
    sfmc_name as "sfmc_name",
    sfmc_mobile as "sfmc_mobile",
    sfmc_email as "sfmc_email",
    master_hcp_key as "master_hcp_key",
    source_system as "source_system",
    hcp_id as "hcp_id",
    customer_name as "customer_name",
    cell_phone as "cell_phone",
    email as "email",
    account_source_id as "account_source_id",
    ventasys_team_name as "ventasys_team_name",
    ventasys_custid as "ventasys_custid",
    subscriber_key as "subscriber_key",
    data_source as "data_source",
    country_code as "country_code",
    region as "region",
    zone as "zone",
    area as "area",
    classification as "classification",
    speciality as "speciality",
    core_noncore as "core_noncore",
    year as "year",
    month as "month",
    hcp_created_date as "hcp_created_date",
    territory_id as "territory_id",
    region_hq as "region_hq",
    is_active as "is_active",
    is_active_msr as "is_active_msr",
    first_prescription_date as "first_prescription_date",
    prescriber_type as "prescriber_type",
    total_prescriptions as "total_prescriptions",
    prescription_units as "prescription_units",
    unique_product_type as "unique_product_type",
    planned_visits as "planned_visits",
    actual_visits as "actual_visits",
    phone_connects as "phone_connects",
    video_connects as "video_connects",
    f2f_connects as "f2f_connects",
    avg_prod_detailed as "avg_prod_detailed",
    lbl_given as "lbl_given",
    samples_given as "samples_given",
    emails_sent as "emails_sent",
    emails_delivered as "emails_delivered",
    emails_opened as "emails_opened",
    emails_clicked as "emails_clicked",
    emails_unique_clicked as "emails_unique_clicked",
    emails_unsusbscribed as "emails_unsusbscribed",
    emails_forwarded as "emails_forwarded",
    average_call_duration as "average_call_duration",
    events_registered as "events_registered",
    events_attended as "events_attended",
    events_as_speaker as "events_as_speaker",
    avg_key_msgs_delivered as "avg_key_msgs_delivered",
    master_id as "master_id",
    activity_date as "activity_date",
    prescription_id as "prescription_id",
    all_visitor as "all_visitor",
    total_session_duration as "total_session_duration",
    sessions as "sessions",
    total_bounces as "total_bounces",
    sales_value as "sales_value",
    iqvia_brand as "iqvia_brand",
    brand_category as "brand_category",
    report_brand_reference as "report_brand_reference",
    iqvia_product_description as "iqvia_product_description",
    totalprescriptions_iqvia as "totalprescriptions_iqvia",
    totalprescriptions_by_brand as "totalprescriptions_by_brand",
    totalprescriptions_jnj_brand as "totalprescriptions_jnj_brand",
    num_buying_retailer as "num_buying_retailer",
    num_buying_retailer_ytd as "num_buying_retailer_ytd",
    num_buying_retailer_mat as "num_buying_retailer_mat",
    consent_flag as "consent_flag",
    projected_doctors as "projected_doctors",
    consent_date as "consent_date",
    ventasys_hcp_id as "ventasys_hcp_id"
from prod_dna_core.hcpedw_integration.edw_hcp360_iqvia_kpi_rpt;