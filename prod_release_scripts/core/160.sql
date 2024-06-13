------------------ t1 ------------------------------------

create table prod_dna_core.thaitg_integration.itg_th_gt_route_insert_temp
as
select distinct *
from prod_dna_core.thaitg_integration.itg_th_gt_route 
where filename = 'RouteHdr_SPC_20240409135217_20240410020841.txt';

delete
from prod_dna_core.thaitg_integration.itg_th_gt_route
where filename in ('RouteHdr_SPC_20240409223048_20240410021110.txt','RouteHdr_SPC_20240409135217_20240410020841.txt');

insert into prod_dna_core.thaitg_integration.itg_th_gt_route
select * from prod_dna_core.thaitg_integration.itg_th_gt_route_insert_temp;

--------------------- t2 -------------------------

create table prod_dna_core.thaitg_integration.itg_th_gt_route_detail_insert_temp
as
select distinct * from prod_dna_core.thaitg_integration.itg_th_gt_route_detail 
where filename = 'RouteDtl_SPC_20240409135218_20240410020736.txt';

delete
from prod_dna_core.thaitg_integration.itg_th_gt_route_detail 
where filename in ('RouteDtl_SPC_20240409223049_20240410021024.txt','RouteDtl_SPC_20240409135218_20240410020736.txt');

insert into prod_dna_core.thaitg_integration.itg_th_gt_route_detail
select * from prod_dna_core.thaitg_integration.itg_th_gt_route_detail_insert_temp;