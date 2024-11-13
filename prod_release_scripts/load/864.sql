update prod_dna_load.meta_raw.parameters
set parameter_value='SELECT 
Id,
IsDeleted,
Name,
RecordTypeId,
CreatedDate,
CreatedById,
LastModifiedDate,
LastModifiedById,
SystemModstamp,
MayEdit,
IsLocked,
LastViewedDate,
LastReferencedDate,
Inventory_Impact_Quantity_vod__c,
Lot_vod__c,
Discrepancy_vod__c,
Adjust_For_vod__c,
Transfer_To_Name_vod__c,
Signature_vod__c,
Received_vod__c,
Signature_Date_vod__c,
Transferred_From_vod__c,
Call_Name_vod__c,
Receipt_Comments_vod__c,
Transferred_Date_vod__c,
Submitted_Date_vod__c,
Transfer_To_vod__c,
Shipment_ID_vod__c,
Address_Line_1_vod__c,
Zip_vod__c,
Status_vod__c,
City_vod__c,
Adjusted_Date_vod__c,
Confirmed_Quantity_vod__c,
U_M_vod__c,
Disclaimer_vod__c,
Disbursed_To_vod__c,
Ref_Transaction_Id_vod__c,
Comments_vod__c,
Sample_vod__c,
Unlock_vod__c,
Quantity_vod__c,
Transferred_From_Name_vod__c,
Type_vod__c,
Lot_Name_vod__c,
Reason_vod__c,
Group_Transaction_Id_vod__c,
zvod_Sample_Lines_vod__c,
Address_Line_2_vod__c,
State_vod__c,
Return_To_vod__c,
Sample_Card_vod__c,
ASSMCA_vod__c,
Account_vod__c,
Call_Date_vod__c,
Call_Datetime_vod__c,
DEA_Expiration_Date_vod__c,
DEA_vod__c,
Sample_Card_Reason_vod__c,
Zip_4_vod__c,
License_vod__c,
Request_Receipt_vod__c,
Group_Identifier_vod__c,
Credentials_vod__c,
Salutation_vod__c,
Manufacturer_vod__c,
Distributor_vod__c,
JJ_Core_Country__c,
JJ_LegacyID__c
FROM
    Sample_Transaction_vod__c where CALENDAR_YEAR(CreatedDate ) = 2024 and CALENDAR_QUARTER(CreatedDate)>=3'
where parameter_group_id=2225 AND PARAMETER_NAME='ms_query';

update prod_DNA_LOAD.META_RAW.PROCESS
set is_incremental='TRUE'
where parameter_group_id=2225;

update prod_dna_load.meta_raw.parameters
set is_active='TRUE'
where parameter_group_id=2225 and PARAMETER_NAME='next_incremental_value';

update prod_dna_load.meta_raw.parameters
set is_active='TRUE'
where parameter_group_id=2225 and PARAMETER_NAME='incremental_filter';
