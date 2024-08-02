CREATE OR REPLACE FUNCTION PROD_DNA_CORE.JPDCLEDW_INTEGRATION.ENCRYPTION("COL" VARCHAR(16777216))
RETURNS VARCHAR(20)
LANGUAGE SQL
AS '
    case when col is not null and col!='''' and length(col)<=10
    then
    CASE WHEN LENGTH(col)= 10 
            THEN SUBSTR(col, 7, 1) || SUBSTR(col, 5, 1) || SUBSTR(col, 8, 1) || SUBSTR(col, 6, 1) || SUBSTR(col, 2, 1) || SUBSTR(col, 4, 1) || SUBSTR(col, 1, 1) || SUBSTR(col, 3, 1) || SUBSTR(col, 9, 1) || SUBSTR(col, 10, 1)
            WHEN LENGTH(col) = 9 
            THEN SUBSTR(col, 6, 1) || SUBSTR(col, 4, 1) || SUBSTR(col, 7, 1) || SUBSTR(col, 5, 1) || SUBSTR(col, 1, 1) || SUBSTR(col, 3, 1) || ''0'' || SUBSTR(col, 2, 1) || SUBSTR(col, 8, 1) || SUBSTR(col, 9, 1)                 
            WHEN LENGTH(col) = 8 
            THEN SUBSTR(col, 5, 1) || SUBSTR(col, 3, 1) || SUBSTR(col, 6, 1) || SUBSTR(col, 4, 1) || ''0'' || SUBSTR(col, 2, 1) || ''0'' || SUBSTR(col, 1, 1) || SUBSTR(col, 7, 1) || SUBSTR(col, 8, 1)                 
            WHEN LENGTH(col) = 7 
            THEN SUBSTR(col, 4, 1) || SUBSTR(col, 2, 1) || SUBSTR(col, 5, 1) || SUBSTR(col, 3, 1) || ''0'' || SUBSTR(col, 1, 1) || ''0'' || ''0'' || SUBSTR(col, 6, 1) || SUBSTR(col, 7, 1)                 
            WHEN LENGTH(col) = 6 
            THEN SUBSTR(col, 3, 1) || SUBSTR(col, 1, 1) || SUBSTR(col, 4, 1) || SUBSTR(col, 2, 1) || ''0'' || ''0'' || ''0'' || ''0'' || SUBSTR(col, 5, 1) || SUBSTR(col, 6, 1)         WHEN LENGTH(col) = 5 
            THEN SUBSTR(col, 2, 1) || ''0'' || SUBSTR(col, 3, 1) || SUBSTR(col, 1, 1) || ''0'' || ''0'' || ''0'' || ''0'' || SUBSTR(col, 4, 1) || SUBSTR(col, 5, 1)                   
            WHEN LENGTH(col) = 4 
            THEN SUBSTR(col, 1, 1) || ''0'' || SUBSTR(col, 2, 1) || ''0'' || ''0'' || ''0'' || ''0'' || ''0'' || SUBSTR(col, 3, 1) || SUBSTR(col, 4, 1)                 
            WHEN LENGTH(col) = 3 
            THEN col=''0'' || ''0'' || SUBSTR(col, 1, 1) || ''0'' || ''0'' || ''0'' || ''0'' || ''0'' || SUBSTR(col, 2, 1) || SUBSTR(col, 3, 1)                 
            WHEN LENGTH(col) = 2 
            THEN col=''0'' || ''0'' || ''0'' || ''0'' || ''0'' || ''0'' || ''0'' || ''0'' || SUBSTR(col, 1, 1) || SUBSTR(col, 2, 1)                 
            WHEN LENGTH(col) = 1 
            THEN col=''0'' || ''0'' || ''0'' || ''0'' || ''0'' || ''0'' || ''0'' || ''0'' || ''0'' || SUBSTR(col, 1, 1)
            ELSE ''''             
            END

        else ''''
    End


';
