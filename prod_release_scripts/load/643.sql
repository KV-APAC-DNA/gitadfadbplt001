Create or replace FILE FORMAT ASPSDL_RAW.CSV_FILE_FORMAT_CLAVIS_SOH_GZIP
  type = 'csv'
  encoding = 'UTF-8'
  field_delimiter = '\u0001'
  error_on_column_count_mismatch=false
  skip_header = 1
  NULL_IF=('')
  empty_field_as_null = FALSE
  ;
