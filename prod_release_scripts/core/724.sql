create table PROD_DNA_CORE.HCPITG_INTEGRATION.ITG_HCP360_IN_VENTASYS_HCP_MASTER_backup_05_12_2024 
as select * from PROD_DNA_CORE.HCPITG_INTEGRATION.ITG_HCP360_IN_VENTASYS_HCP_MASTER;

create table PROD_DNA_CORE.HCPITG_INTEGRATION.SDL_HCP360_IN_VENTASYS_HCP_MASTER_RAW_backup_05_12_2024 
as select * from PROD_DNA_CORE.HCPITG_INTEGRATION.SDL_HCP360_IN_VENTASYS_HCP_MASTER_RAW;

create table PROD_DNA_CORE.HCPEDW_INTEGRATION.EDW_HCP360_IN_VENTASYS_HCP_DIM_backup_05_12_2024 
as select * from PROD_DNA_CORE.HCPEDW_INTEGRATION.EDW_HCP360_IN_VENTASYS_HCP_DIM;


alter table PROD_DNA_CORE.HCPITG_INTEGRATION.ITG_HCP360_IN_VENTASYS_HCP_MASTER alter column CUST_NAME type VARCHAR (150);
alter table PROD_DNA_CORE.HCPITG_INTEGRATION.SDL_HCP360_IN_VENTASYS_HCP_MASTER_RAW alter column CUST_NAME type VARCHAR (150);
alter table PROD_DNA_CORE.HCPEDW_INTEGRATION.EDW_HCP360_IN_VENTASYS_HCP_DIM alter column CUSTOMER_NAME type VARCHAR (150);
commit