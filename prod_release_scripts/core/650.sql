DELETE FROM PROD_DNA_CORE.ASPITG_INTEGRATION.itg_sfmc_click_data 
WHERE CNTRY_CD='PH' AND UPDT_DTTM = '2024-11-13 21:01:03.591';


INSERT INTO PROD_DNA_CORE.ASPITG_INTEGRATION.itg_sfmc_click_data(
with 
source_ph as
(
    select * from PROD_DNA_LOAD.phlsdl_raw.sdl_ph_sfmc_click_data
    where file_name not in (
        select distinct file_name from PROD_DNA_CORE.phlwks_integration.TRATBL_sdl_ph_sfmc_click_data__test_null__ff
        union all
        select distinct file_name from PROD_DNA_CORE.phlwks_integration.TRATBL_sdl_ph_sfmc_click_data__test_duplicate__ff
        union all
        select distinct file_name from PROD_DNA_CORE.phlwks_integration.TRATBL_sdl_ph_sfmc_click_data__test_lookup__ff
    )
),
final as
(
    SELECT
        'PH'::varchar(10) as cntry_cd,
        oyb_account_id::varchar(50) as oyb_account_id,
        job_id::varchar(50) as job_id,
        list_id::varchar(50) as list_id,
        batch_id::varchar(50) as batch_id,
        subscriber_id::varchar(50) as subscriber_id,
        subscriber_key::varchar(100) as subscriber_key,
        event_date::timestamp_ntz(9) as event_date,
        domain::varchar(50) as domain,
        url::varchar(1000) as url,
        LEFT(link_name,200)::varchar(200) as link_name,
        link_content::varchar(1000) as link_content,
        is_unique::varchar(10) as is_unique,
        email_name::varchar(100) as email_name,
        trim(email_subject)::varchar(200) as email_subject,
        file_name::varchar(255) as file_name,
        crtd_dttm::timestamp_ntz(9) as crtd_dttm,
        current_timestamp::timestamp_ntz(9) as updt_dttm,
    from source_ph
   -- where rnk=1
)
select * from final);
