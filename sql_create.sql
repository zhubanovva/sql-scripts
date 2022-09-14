CREATE TABLE db_name.agg_daily(
`imei` bigint COMMENT 'imei of a device', 
`subs_no` string, 
`device_type` string COMMENT 'device type', 
`server_ip` string, 
`count_session` int, 
`session_duration` int,
`flag_ind` tinyint, 
`sat_type` int, 
`protocol_category` int COMMENT 'Enum:   Protocol Classification layer 1', 
`application` int COMMENT 'Enum:   Protocol Classification layer 2', 
`sub_application` int COMMENT 'Enum:   Protocol Classification layer 3', 
`egn_sub_protocol` int COMMENT 'Enum:   Protocol Classification layer 4', 
`destination_name` string COMMENT 'Uniform   Resource   Locator', 
`technology` string COMMENT 'network technology',
`lac` int,
`ci` int,
`lan_rtt` bigint, 
`dp_rtt` bigint,
`offline_rg` bigint, 
`online_rg` bigint,
`url_category_id` bigint, 
`url_sub_category_id` bigint, 
`use_agent` string,
`hours_list` string
)
PARTITIONED BY ( 
`date` date)
CLUSTERED BY ( 
subs_no) 
SORTED BY ( 
server_ip ASC) 
INTO 32 BUCKETS
ROW FORMAT SERDE 
'org.apache.hadoop.hive.ql.io.orc.OrcSerde' 
STORED AS INPUTFORMAT 
'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' 
OUTPUTFORMAT 
'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
LOCATION                                           
'hdfs://hdp/warehouse/tablespace/managed/hive/db_name.db/agg_daily'
