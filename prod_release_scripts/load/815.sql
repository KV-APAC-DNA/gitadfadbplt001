
update  prod_dna_load.meta_raw.PARAMETERS 
set 
parameter_value='Tier 2 Replenishment E3 Buyers Report'
where parameter_group_id=443
and parameter_id in (5816,5817);
