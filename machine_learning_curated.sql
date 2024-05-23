CREATE EXTERNAL TABLE IF NOT EXISTS `dev`.`machine_learning_curated` (
  `sensorReadingTime` bigint,
  `serialNumber` string,
  `distanceFromObject` int,
  `timeStamp` bigint,
  `x` bigint,
  `y` bigint,
  `z` bigint
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
  'ignore.malformed.json' = 'FALSE',
  'dots.in.keys' = 'FALSE',
  'case.insensitive' = 'TRUE',
  'mapping' = 'TRUE'
)
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://alphatest1111/machine_learning_curated/'
TBLPROPERTIES ('classification' = 'json');