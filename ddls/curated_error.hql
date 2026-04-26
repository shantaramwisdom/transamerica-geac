DROP TABLE IF EXISTS ${databasename}.curated_error;
CREATE EXTERNAL TABLE ${databasename}.curated_error
(
 recorded_timestamp           TIMESTAMP(0),
    original_recorded_timestamp  TIMESTAMP(0),
    original_cycle_date          DATE,
    original_batch_id            INT,
    error_classification_name    VARCHAR,   -- financedwcurated.string
    error_message                VARCHAR,   -- financedwcurated.string
    error_record                 VARCHAR,   -- financedwcurated.string
    error_record_aging_days      INT,
    table_name                   VARCHAR,   -- financedwcurated.string
    reprocess_flag               VARCHAR    -- financedwcurated.string)
PARTITIONED BY (
table_name string,
reprocess_flag string)
ROW FORMAT SERDE
'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
'${s3bucketname}/${projectname}/orchestration/curated_error';
MSCK REPAIR TABLE ${databasename}.curated_error SYNC PARTITIONS;
