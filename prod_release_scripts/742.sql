insert into PROD_DNA_CORE.JPDCLITG_INTEGRATION.sfmc_bounces
with source as(
    select *, dense_rank() over(partition by subscriberkey order by file_name desc) as rnk 
    from PROD_DNA_LOAD.jpdclsdl_raw.sfmc_bounces
    --qualify rnk =1
),
final as(
    select 
        clientid::number(38,0) as clientid,
        sendid::number(38,0) as sendid,
        subscriberkey::varchar(300) as subscriberkey,
        emailaddress::varchar(300) as emailaddress,
        subscriberid::number(38,0) as subscriberid,
        listid::number(38,0) as listid,
        eventdate::timestamp_ntz(9) as eventdate,
        eventtype::varchar(20) as eventtype,
        bouncecategory::varchar(60) as bouncecategory,
        smtpcode::number(18,0) as smtpcode,
        bouncereason::varchar(65535) as bouncereason,
        batchid::number(38,0) as batchid,
        triggeredsendexternalkey::varchar(100) as triggeredsendexternalkey,
        NULL::varchar(10) as source_file_date,
        current_timestamp()::timestamp_ntz(9) as inserted_date,
        NULL::varchar(10) as inserted_by,
        current_timestamp()::timestamp_ntz(9) as updated_date,
        NULL::varchar(100) as updated_by,
        file_name::varchar(255) as file_name
    from source
)
select * from final;

commit;




insert into PROD_DNA_CORE.JPDCLITG_INTEGRATION.sfmc_clicks
with source as(
    select *, dense_rank() over(partition by subscriberkey order by file_name desc) as rnk 
    from PROD_DNA_LOAD.jpdclsdl_raw.sfmc_clicks
    --qualify rnk =1
),
final as(
    select 
        clientid::number(38,0) as clientid,
        sendid::number(38,0) as sendid,
        subscriberkey::varchar(300) as subscriberkey,
        emailaddress::varchar(300) as emailaddress,
        subscriberid::number(38,0) as subscriberid,
        listid::number(38,0) as listid,
        eventdate::timestamp_ntz(9) as eventdate,
        eventtype::varchar(20) as eventtype,
        sendurlid::number(38,0) as sendurlid,
        urlid::number(38,0) as urlid,
        url::varchar(65535) as url,
        alias::varchar(65535) as alias,
        batchid::number(38,0) as batchid,
        triggeredsendexternalkey::varchar(100) as triggeredsendexternalkey,
        NULL::varchar(10) as source_file_date,
        current_timestamp()::timestamp_ntz(9) as inserted_date,
        NULL::varchar(10) as inserted_by,
        current_timestamp()::timestamp_ntz(9) as updated_date,
        NULL::varchar(100) as updated_by,
        file_name::varchar(255) as file_name
    from source
)
select * from final;

commit;


insert into PROD_DNA_CORE.JPDCLITG_INTEGRATION.sfmc_notsent
with source as(
    select * , dense_rank() over(partition by subscriberkey order by file_name desc) as rnk
    from PROD_DNA_LOAD.jpdclsdl_raw.sfmc_notsent
    --qualify rnk =1
),
final as(
    select 
        clientid::number(38,0) as clientid,
        sendid::number(38,0) as sendid,
        subscriberkey::varchar(300) as subscriberkey,
        emailaddress::varchar(300) as emailaddress,
        subscriberid::number(38,0) as subscriberid,
        listid::number(38,0) as listid,
        eventdate::timestamp_ntz(9) as eventdate,
        eventtype::varchar(20) as eventtype,
        batchid::number(38,0) as batchid,
        triggeredsendexternalkey::varchar(100) as triggeredsendexternalkey,
        reason::varchar(256) as reason,
        NULL::varchar(10) as source_file_date,
        current_timestamp()::timestamp_ntz(9) as inserted_date,
        NULL::varchar(10) as inserted_by,
        current_timestamp()::timestamp_ntz(9) as updated_date,
        NULL::varchar(100) as updated_by,
        file_name::varchar(255) as file_name
    from source
)
select * from final;

commit;


insert into PROD_DNA_CORE.JPDCLITG_INTEGRATION.sfmc_opens
with source as(
    select *, dense_rank() over(partition by subscriberkey order by file_name desc) as rnk 
    from PROD_DNA_LOAD.jpdclsdl_raw.sfmc_opens
    --qualify rnk =1
),
final as(
    select 
        clientid::number(38,0) as clientid,
        sendid::number(38,0) as sendid,
        subscriberkey::varchar(300) as subscriberkey,
        emailaddress::varchar(300) as emailaddress,
        subscriberid::number(38,0) as subscriberid,
        listid::number(38,0) as listid,
        eventdate::timestamp_ntz(9) as eventdate,
        eventtype::varchar(20) as eventtype,
        batchid::number(38,0) as batchid,
        triggeredsendexternalkey::varchar(100) as triggeredsendexternalkey,
        NULL::varchar(10) as source_file_date,
        current_timestamp()::timestamp_ntz(9) as inserted_date,
        NULL::varchar(10) as inserted_by,
        current_timestamp()::timestamp_ntz(9) as updated_date,
        NULL::varchar(100) as updated_by,
        file_name::varchar(255) as file_name
    from source
)
select * from final;

commit;


insert into PROD_DNA_CORE.JPDCLITG_INTEGRATION.sfmc_sendjobs
with source as(
    select *, dense_rank() over(partition by clientid order by file_name desc) as rnk 
    from PROD_DNA_LOAD.jpdclsdl_raw.sfmc_sendjobs
    --qualify rnk =1
),
final as(
    select 
        clientid::number(38,0) as clientid,
        sendid::number(38,0) as sendid,
        fromname::varchar(150) as fromname,
        fromemail::varchar(100) as fromemail,
        schedtime::timestamp_ntz(9) as schedtime,
        senttime::timestamp_ntz(9) as senttime,
        subject::varchar(300) as subject,
        emailname::varchar(100) as emailname,
        triggeredsendexternalkey::varchar(100) as triggeredsendexternalkey,
        senddefinitionexternalkey::varchar(100) as senddefinitionexternalkey,
        jobstatus::varchar(30) as jobstatus,
        previewurl::varchar(65535) as previewurl,
        ismultipart::varchar(65535) as ismultipart,
        additional::varchar(50) as additional,
        NULL::varchar(10) as source_file_date,
        current_timestamp()::timestamp_ntz(9) as inserted_date,
        NULL::varchar(10) as inserted_by,
        current_timestamp()::timestamp_ntz(9) as updated_date,
        NULL::varchar(100) as updated_by,
        file_name::varchar(255) as file_name
    from source
)
select * from final;

commit;


insert into PROD_DNA_CORE.JPDCLITG_INTEGRATION.sfmc_sent
with source as
(
    select *, dense_rank() over(partition by subscriberkey order by file_name desc) as rnk 
    from PROD_DNA_LOAD.jpdclsdl_raw.sfmc_sent
    --qualify rnk =1
),
final as
(
    select 
		clientid::number(38,0) as clientid,
        sendid::number(38,0) as sendid,
        subscriberkey::varchar(300) as subscriberkey,
        emailaddress::varchar(300) as emailaddress,
        subscriberid::number(38,0) as subscriberid,
        listid::number(38,0) as listid,
        eventdate::timestamp_ntz(9) as eventdate,
        eventtype::varchar(20) as eventtype,
        batchid::number(38,0) as batchid,
        triggeredsendexternalkey::varchar(100) as triggeredsendexternalkey,
        null::varchar(30) as source_file_date,
        current_timestamp()::timestamp_ntz(9) as inserted_date,
        null::varchar(100) as inserted_by,
        current_timestamp()::timestamp_ntz(9) as updated_date,
        null::varchar(9) as updated_by,
        file_name::varchar(255) as file_name
    from source
)
select * from final;

commit;

insert into PROD_DNA_CORE.JPDCLITG_INTEGRATION.sfmc_unsubs
with source as
(
    select *, dense_rank() over(partition by subscriberkey order by file_name desc) as rnk 
    from PROD_DNA_LOAD.jpdclsdl_raw.sfmc_unsubs
    --qualify rnk =1
),
final as
(
    select 
		clientid::number(38,0) as clientid,
        sendid::number(38,0) as sendid,
        subscriberkey::varchar(300) as subscriberkey,
        emailaddress::varchar(300) as emailaddress,
        subscriberid::number(38,0) as subscriberid,
        listid::number(38,0) as listid,
        eventdate::timestamp_ntz(9) as eventdate,
        eventtype::varchar(20) as eventtype,
        batchid::number(38,0) as batchid,
        triggeredsendexternalkey::varchar(100) as triggeredsendexternalkey,
        null::varchar(30) as source_file_date,
        current_timestamp()::timestamp_ntz(9) as inserted_date,
        null::varchar(100) as inserted_by,
        current_timestamp()::timestamp_ntz(9) as updated_date,
        null::varchar(9) as updated_by,
        file_name::varchar(255) as file_name
    from source
)
select * from final;


commit;