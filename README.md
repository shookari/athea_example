## DDL

### Table 및 Partition 생성.

sampletbl

```sql
# create table
CREATE EXTERNAL TABLE IF NOT EXISTS romedatabase.sampletbl (
  csno string,
  pos_cnt int,
  neg_cnt int
)
PARTITIONED BY (reg_date string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
WITH SERDEPROPERTIES (
  'serialization.format' = ',',
  'field.delim' = ','
) LOCATION 's3://athena-test-buck-rome/sampletbl/'
TBLPROPERTIES ('has_encrypted_data'='false');

# create partition
ALTER TABLE sampletbl ADD 
  PARTITION (reg_date = '2022-01-11') LOCATION 's3://athena-test-buck-rome/sampletbl/2022_01_11/';

```

csnotbl

```sql
# table 
CREATE EXTERNAL TABLE IF NOT EXISTS romedatabase.csnotbl (
  csno string,
  name string
)
PARTITIONED BY (reg_date string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
WITH SERDEPROPERTIES (
  'serialization.format' = ',',
  'field.delim' = ','
) LOCATION 's3://athena-test-buck-rome/csnotbl/'
TBLPROPERTIES ('has_encrypted_data'='false');

# create partition
ALTER TABLE csnotbl ADD 
  PARTITION (reg_date = '2022-01-11') LOCATION 's3://athena-test-buck-rome/csnotbl/2022_01_11/';
```



## DML

#### Insert

```sql
# sampletbl
insert into romedatabase.sampletbl (reg_date, csno, pos_cnt, neg_cnt) VALUES('2022-01-11', '1980', 51, 49);

# csnotbl
insert into romedatabase.csnotbl (reg_date, csno, name) VALUES('2022-01-11', '1980', 'kim');
```



### select

```
select * from sampletbl;
select * from csnotbl;
```



## CTAS with join

### View

```sql
# create view (athena 는 presto engine 사용으로 join 시 더 큰 table 을 먼저(왼쪽) 에 둔다.)
CREATE VIEW csno_join
AS SELECT  sampletbl.csno as csno, sampletbl.pos_cnt as pos_cnt, csnotbl.name AS name
FROM sampletbl
INNER JOIN csnotbl ON sampletbl.csno=csnotbl.csno

# select
select * from csno_join;

# drop
drop view csno_join;
```



## Release

```sql
# drop partition (table 삭제 시 같이 삭제됨)
ALTER TABLE csnotbl DROP PARTITION (reg_date = '2022-01-11');
ALTER TABLE sampletbl DROP PARTITION (reg_date = '2022-01-11');

# drop table
drop table sampletbl;
drop table csnotbl;

# drop view
drop view csno_join;
```





### Check Query

```
# table 리스트
show tables;

# partitions
show partitions {table_name}

# view
show views;
```



## Reference

https://bringyourowndatalabs.workshop.aws/en/athena/54_storing_sql_join_results.html

https://pypi.org/project/pyathena/

https://github.com/ramdesh/athena-python-examples/blob/main/athena_boto3_example.py

https://aws.amazon.com/ko/blogs/korea/top-10-performance-tuning-tips-for-amazon-athena/



## Next

Pandas.to_sql => https://github.com/ramdesh/athena-python-examples/blob/main/athena_boto3_example.py

Performance

