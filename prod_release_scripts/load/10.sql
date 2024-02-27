use database prod_dna_load;
create or replace stage MYSSDL_RAW.PROD_LOAD_STAGE_ADLS
storage_integration = PROD_DNA_LOAD_AZURE28_SI
url = 'azure://dlsadbplt001.blob.core.windows.net/mys/';
