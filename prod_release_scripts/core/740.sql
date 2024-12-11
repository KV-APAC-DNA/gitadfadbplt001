insert into PROD_DNA_CORE.JPDCLITG_INTEGRATION.contact_hist
with source as
(
    select * from PROD_DNA_LOAD.jpdclsdl_raw.contact_hist
),
final as
(
    select * from source
)
select * from final;

commit;