create table aspitg_integration.itg_pop6_displays_bkp_20240906 as select * from aspitg_integration.itg_pop6_displays;

update  aspitg_integration.itg_pop6_displays a set field_code = 'Local_DISPLAY_QN_YESNO'
from 
aspitg_integration.itg_pop6_executed_visits exe,
aspitg_integration.itg_pop6_pops pops
where exe.visit_id = a.visit_id 
and exe.visit_date like '2024-02%'
and pops.popdb_id = exe.popdb_id
and pops.active = 'Y'
and a.cntry_cd = 'TH'
and upper(response) = 'NO'
and upper(pops.customer) in ('BIG C', 'GT SUPER' , 'GT SUPER VIP' , 'LOTUS''S' , 'THE MALL' , 'TOPS', 'WATSONS')
and field_code = 'PS_DISPLAY_QN_YESNO';

update aspitg_integration.itg_pop6_displays a 
set field_code = 'Local_DISPLAY_QN_YESNO'
from aspitg_integration.itg_pop6_displays b
 where a.visit_id  = b.visit_id 
 and  b.field_label = 'เป็นร้านค้าที่บริษัทฯ มีการเช่าพื้นที่หรือไม่'  
 and b.field_code = 'PS_DISPLAY_QN_YESNO'
and upper(b.response) = 'NO'
and a.field_code = 'PS_DISPLAY_QN_YESNO'
;
