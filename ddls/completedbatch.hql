DROP TABLE IF EXISTS ${databasename}.completedbatch;
CREATE EXTERNAL TABLE ${databasename}.completedbatch
(
batch_id bigint,
cycle_date date,
recorded_timestamp timestamp
)
PARTITIONED BY (
domain_name string,
source_system string,
batch_frequency string)
ROW FORMAT SERDE
'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES (
'field.delim'='|',
'serialization.format'='|')
STORED AS INPUTFORMAT
'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
'${s3bucketname}/${projectname}/orchestration/completedbatch';
MSCK REPAIR TABLE ${databasename}.completedbatch SYNC PARTITIONS;
