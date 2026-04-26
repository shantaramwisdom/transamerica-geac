DROP TABLE IF EXISTS ${databasename}.geac_general_ledger_line_item;

CREATE EXTERNAL TABLE ${databasename}.geac_general_ledger_line_item
(
 recorded_timestamp timestamp,
 source_system_name string,
 original_cycle_date date,
 original_batch_id int,
 activity_accounting_id string,
 transaction_number string,
 line_number int,
 default_amount decimal(18,2),
 debit_credit_indicator string,
 gl_full_source_code string,
 orig_gl_source_document_id string,
 orig_gl_company string,
 orig_gl_account string,
 orig_gl_center string,
 orig_gl_description_1 string,
 orig_gl_description_2 string,
 orig_gl_description_3 string,
 orig_gl_project_code string
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
 '${s3bucketname}/${projectname}/curated/erpdw/geac/geac_general_ledger_line_item';
MSCK REPAIR TABLE ${databasename}.geac_general_ledger_line_item SYNC PARTITIONS;
