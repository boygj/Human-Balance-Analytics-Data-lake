CREATE EXTERNAL TABLE IF NOT EXISTS `dev`.`customer_curated` (
  `email` string,
  `shareWithPublicAsOfDate` bigint,
  `birthDay` string,
  `customerName` string,
  `shareWithResearchAsOfDate` bigint,
  `registrationDate` bigint,
  `serialNumber` string,
  `lastUpdateDate` bigint,
  `phone` string,
  `shareWithFriendsAsOfDate` bigint
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
  'ignore.malformed.json' = 'FALSE',
  'dots.in.keys' = 'FALSE',
  'case.insensitive' = 'TRUE',
  'mapping' = 'TRUE'
)
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://alphatest1111/customer/curated/'
TBLPROPERTIES ('classification' = 'json');