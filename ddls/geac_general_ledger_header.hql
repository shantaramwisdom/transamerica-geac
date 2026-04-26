DROP TABLE IF EXISTS ${databasename}.geac_general_ledger_header;

CREATE EXTERNAL TABLE ${databasename}.geac_general_ledger_header
(
 recorded_timestamp timestamp,
 source_system_name string,
 original_cycle_date date,
 original_batch_id int,
 transaction_number string,
 source_system_nm string,
 ledger_name string,
 event_type_code string,
 subledger_short_name string,
 transaction_date date,
 gl_reversal_date date,
 gl_application_area_code string,
 gl_source_code string,
 secondary_ledger_code string,
 data_type string
)
PARTITIONED BY (
 cycle_date date,
 batch_id int
)
ROW FORMAT SERDE
 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
 '${s3bucketname}/${projectname}/curated/erpdw/geac/geac_general_ledger_header';
MSCK REPAIR TABLE ${databasename}.geac_general_ledger_header SYNC PARTITIONS;
