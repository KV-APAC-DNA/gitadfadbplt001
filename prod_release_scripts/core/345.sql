create or replace view PROD_DNA_CORE.CORE_INTEGRATION.VW_FAILED_TESTS_ON_MODELS(
	MODEL_NAME,
	RUN_STARTED_AT,
	QUERY_COMPLETED_AT,
	STATUS
) as (
with failed_tests_models as (
select replace(replace(split(split(node_id,'.')[2],'__')[0],'TRATBL_'),'"') as model_name,* from core_integration.test_executions where status in ('fail','warn')
order by run_started_at desc
)
select model_name,run_started_at,query_completed_at,status from failed_tests_models 
);
