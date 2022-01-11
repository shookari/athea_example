LEFT_TBL="sampletbl"
RIGHT_TBL="csnotbl"
JOIN_VIEW="csno_join"
SCHEMA="romedatabase"

CREATE_SAMPLE_TBL = f'''CREATE EXTERNAL TABLE IF NOT EXISTS {SCHEMA}.{LEFT_TBL} (
  csno string,
  pos_cnt int,
  neg_cnt int
)
PARTITIONED BY (reg_date string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
WITH SERDEPROPERTIES (
  'serialization.format' = ',',
  'field.delim' = ','
) LOCATION 's3://athena-test-buck-rome/{LEFT_TBL}/'
TBLPROPERTIES ('has_encrypted_data'='false');'''

CREATE_SAMPLE_PARTITION = f'''ALTER TABLE {SCHEMA}.{LEFT_TBL} ADD
  PARTITION (reg_date = '2022-01-11') LOCATION 's3://athena-test-buck-rome/{LEFT_TBL}/2022_01_11/';'''

DROP_SAMPLE_TBL=f'''DROP TABLE IF EXISTS {SCHEMA}.{LEFT_TBL}'''

CREATE_CSNO_TBL=f'''CREATE EXTERNAL TABLE IF NOT EXISTS {SCHEMA}.{RIGHT_TBL} (
  csno string,
  name string
)
PARTITIONED BY (reg_date string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
WITH SERDEPROPERTIES (
  'serialization.format' = ',',
  'field.delim' = ','
) LOCATION 's3://athena-test-buck-rome/{RIGHT_TBL}/'
TBLPROPERTIES ('has_encrypted_data'='false'); '''

CREATE_CSNO_PARTITION=f'''ALTER TABLE {SCHEMA}.{RIGHT_TBL} ADD 
  PARTITION (reg_date = '2022-01-11') LOCATION 's3://athena-test-buck-rome/csnotbl/2022_01_11/';'''

DROP_CSNO_TBL=f'''DROP TABLE IF EXISTS {SCHEMA}.{RIGHT_TBL}'''

CREATE_VIEW=f'''CREATE VIEW {SCHEMA}.{JOIN_VIEW} 
AS SELECT  {LEFT_TBL}.csno as csno, {LEFT_TBL}.pos_cnt as pos_cnt, {RIGHT_TBL}.name AS name
FROM {SCHEMA}.{LEFT_TBL} 
INNER JOIN {SCHEMA}.{RIGHT_TBL} ON {LEFT_TBL}.csno={RIGHT_TBL}.csno;'''

DROP_VIEW=f'''DROP VIEW IF EXISTS {SCHEMA}.{JOIN_VIEW}'''

SHOW_TABLES=f'''show tables in {SCHEMA}'''
SHOW_VIEWS=f'''show views in {SCHEMA}'''