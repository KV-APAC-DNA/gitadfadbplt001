DROP PROCEDURE PROD_DNA_LOAD.IDNSDL_RAW.MDSTEMPADLSDATALOAD (VARCHAR, VARCHAR, VARCHAR);

CREATE OR REPLACE PROCEDURE PROD_DNA_LOAD.IDNSDL_RAW.ADLSTOTABLEDATALOAD (TargetTableName VARCHAR(250), FilePath VARCHAR(250), FileName VARCHAR(250), StageName VARCHAR(100) )
RETURNS integer
LANGUAGE SQL
AS
$$
DECLARE
db VARCHAR DEFAULT 'PROD_DNA_LOAD';
SchemaName VARCHAR(150) DEFAULT 'IDNSDL_RAW';

--TargetTableName VARCHAR(250) DEFAULT concat('SDL_MDS_',TableName);
query VARCHAR DEFAULT 
'
COPY INTO '||db||'.'||SchemaName||'.'||TargetTableName||'
FROM (
	SELECT *
	FROM ''@'||StageName||'''
)
FILES = ('''||FilePath||FileName||'.csv'') 
file_format= (TYPE=CSV,FIELD_DELIMITER="",RECORD_DELIMITER="\r\n",SKIP_HEADER=1)
ON_ERROR=ABORT_STATEMENT ;';

table_path string;
rowcount int;

BEGIN

    EXECUTE IMMEDIATE query;
    
    table_path := :DB || '.' || :SchemaName || '.' || :TargetTableName;
    SELECT COUNT(*) INTO :rowcount  FROM identifier(:table_path); 
    
RETURN :rowcount;
END;
$$;
