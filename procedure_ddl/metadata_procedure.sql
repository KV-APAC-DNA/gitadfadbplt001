CREATE OR REPLACE PROCEDURE META_RAW.CHECK_DEPENDENT_JOB_STATUS(PARAM ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE SQL
EXECUTE AS OWNER
AS 
$$
    DECLARE var_processes resultset;
        var_process resultset;
        var_dep_process resultset;
        var_cat varchar;
        var_sql varchar;
        dep_proc_id varchar;
        proc_id varchar;
        return_val varchar;
        proc_time datetime;
        proc_status varchar;
        dep_proc_time datetime;
        dep_proc_status varchar;
    BEGIN
        return_val := 'PROCEED';
        var_cat := :PARAM[0];
        var_sql := 'SELECT P.PROCESS_ID, D.VALUE
            FROM USECASE U
            JOIN PROCESS P
            ON U.USECASE_ID = P.USECASE_ID
            CROSS JOIN TABLE(SPLIT_TO_TABLE(NULLIF(TRIM(DEPENDS_ON),''''),'','')) D
            WHERE U.CATEGORY = '''||:var_cat||'''
        ';
        var_processes:= (execute immediate :var_sql);

        FOR val_row in var_processes DO
            dep_proc_id := val_row.VALUE;
            proc_id := val_row.PROCESS_ID;
            var_sql := 'SELECT MOMENT, PROCESS_STATUS
                        FROM PROCESS_AUDIT_LOG 
                        WHERE PROCESS_ID = '||:proc_id||'
                        QUALIFY ROW_NUMBER() OVER(ORDER BY MOMENT DESC) = 1;';

            var_process := (execute immediate :var_sql);
            proc_status := 'NONE';
            FOR proc_res in var_process DO
                proc_time := proc_res.MOMENT;
                proc_status := proc_res.PROCESS_STATUS;
            END FOR;
            
            var_sql := 'SELECT MOMENT, PROCESS_STATUS
                        FROM PROCESS_AUDIT_LOG 
                        WHERE PROCESS_ID = '||:dep_proc_id||'
                        QUALIFY ROW_NUMBER() OVER(ORDER BY MOMENT DESC) = 1;';

            var_dep_process := (execute immediate :var_sql);

            dep_proc_status := 'NONE';
            FOR proc_res in var_dep_process DO
                dep_proc_time := proc_res.MOMENT;
                dep_proc_status := proc_res.PROCESS_STATUS;
            END FOR;
            
            IF (dep_proc_status = 'NONE') THEN
                return_val := :return_val || ',STOP:'||:dep_proc_id ;
            ELSEIF (dep_proc_status = 'START') THEN
                return_val := :return_val ||',WAIT:'||:dep_proc_id ;
            ELSEIF (dep_proc_status = 'FINISH') THEN
                IF (proc_status = 'NONE') THEN
                    CONTINUE;
                ELSEIF ((DATEDIFF('min',dep_proc_time,current_timestamp())/60) < 24) THEN
                    CONTINUE;
                ELSEIF (proc_time < dep_proc_time) THEN
                    CONTINUE;
                ELSE
                    return_val := :return_val || ',NOTIFY:'||:dep_proc_id;
                END IF;
            ELSE
                CONTINUE;
            END IF;
        END FOR;
        RETURN :return_val;
    END;
$$;


CREATE OR REPLACE PROCEDURE META_RAW.SP_GET_FULLQUERY("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS 
$$

import snowflake.snowpark as snowpark
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, DateType, DoubleType,TimestampType
from snowflake.snowpark.functions import replace
import pandas as pd
from datetime import datetime
import pytz

def main(session: snowpark.Session, Param): 
    orig_query       = Param[0]
    param_list      = Param[1]
    val_list = Param[2]
    val_list = val_list.replace(":null",":\"null\"")
    val_dict = eval(val_list)
    params = param_list.split(',')
    return_query = orig_query
    for param in params:
        return_query= return_query.replace('{'+param+'}', val_dict[param])
    return return_query
$$;