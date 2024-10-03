DECLARE

StageName VARCHAR(150) DEFAULT 'PROD_LOAD_STAGE_ADLS_IDN';
TargetTableName VARCHAR(250) DEFAULT 'SDL_DISTRIBUTOR_ALTRYX_CUST_DIM_MDS_SYNC';
FilePath VARCHAR(300) DEFAULT 'prd/sql_server/MDS_Reverse_Sync/MDS_Adhoc';
SchemaName VARCHAR(150) DEFAULT 'IDNITG_INTEGRATION';
dbname VARCHAR(15) DEFAULT 'PROD_DNA_CORE';

query2 VARCHAR(500) DEFAULT 
'
COPY INTO @PROD_DNA_LOAD.IDNSDL_RAW.'||StageName||'/'||FilePath||'/'||TargetTableName|| ' FROM '||dbname||'.'||SchemaName||'.'||TargetTableName ||' OVERWRITE=TRUE header=true file_format= (TYPE=csv, compression = none, FIELD_DELIMITER='''',RECORD_DELIMITER=''\\r\\n'')';

table_path string;
rowcount int;

BEGIN

EXECUTE IMMEDIATE query2;
table_path := dbname||'.'||SchemaName||'.'||TargetTableName;
SELECT COUNT(*) INTO :rowcount  FROM identifier(:table_path); 

    
RETURN :rowcount;
END;
