DROP TABLE IF EXISTS ${databasename}.curateddeletes;
CREATE EXTERNAL TABLE ${databasename}.curateddeletes
(
recorded_timestamp timestamp,
documentid string,
pointofviewtsupdate timestamp,
batch_id int
)
PARTITIONED BY (
domain_name string,
source_system string,
batch_frequency string,
cycle_date date)
ROW FORMAT SERDE
'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
'${s3bucketname}/${projectname}/orchestration/curateddeletes'
TBLPROPERTIES (
'parquet.compression'='SNAPPY');
MSCK REPAIR TABLE ${databasename}.curateddeletes SYNC PARTITIONS;
