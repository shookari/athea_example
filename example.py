from sqlalchemy.engine import *
import pandas as pd
import ddl_dml

AWS_ACCESS_KEY = "{key}"
AWS_SECRET_KEY = "{secret_key}"
SCHEMA_NAME = "default"
S3_STAGING_DIR = "s3://bucket_address"
AWS_REGION = "ap-northeast-2"

conn_str = (f'awsathena+rest://{AWS_ACCESS_KEY}:{AWS_SECRET_KEY}'
            f'@athena.{AWS_REGION}.amazonaws.com:443/"{SCHEMA_NAME}?'
            f's3_staging_dir={S3_STAGING_DIR}')
engine = create_engine(conn_str)


def execute_query(ddl):
    with engine.connect() as con:
        result=con.execute(ddl)
    print(result)

def insert_to_sql(df, tbl_name):
    df.to_sql(tbl_name, engine, schema=ddl_dml.SCHEMA, index=False, if_exists="append")


# TODO : FIX ERROR
# def insert_to_sql(engine):
#     from concurrent.futures.process import ProcessPoolExecutor
#     from pyathena.pandas.util import to_sql
#     from pyathena import connect
#     with connect(s3_staging_dir="s3://athena-test-buck-rome/sampletbl",  region_name=AWS_REGION) as con:
#         df = pd.DataFrame({"a": [1, 2, 3, 4, 5], "reg_date":"2022-01-11"})
#         to_sql(df, "sampletbl", con, "s3://athena-test-buck-rome/sampletbl/2022_01_11/", schema="romedatabase", index=False, if_exists="append",
#                executor_class=ProcessPoolExecutor, max_workers=1)

def query(tbl_name):
    with engine.connect() as con:
       df = pd.read_sql_query(f"select * from {ddl_dml.SCHEMA}.{tbl_name}", con)
       print(df.to_dict())


def create_sample_table_partition():
    execute_query(ddl_dml.CREATE_SAMPLE_TBL)
    execute_query(ddl_dml.CREATE_SAMPLE_PARTITION)
    execute_query(ddl_dml.CREATE_CSNO_TBL)
    execute_query(ddl_dml.CREATE_CSNO_PARTITION)
    execute_query(ddl_dml.CREATE_VIEW)


def insert_sample_data():
    df = pd.DataFrame([{"reg_date": "2022-01-11", "csno": "1981", "pos_cnt": 30, "neg_cnt": 70}])
    insert_to_sql(df, "sampletbl")

    df = pd.DataFrame([{"reg_date": "2022-01-11", "csno": "1981", "name": 'park'}])
    insert_to_sql(df, "csnotbl")

def create_view_join():
    execute_query(ddl_dml.CREATE_VIEW)

def query_sample_data():
    query(ddl_dml.LEFT_TBL)
    query(ddl_dml.RIGHT_TBL)
    query(ddl_dml.JOIN_VIEW)


def release_all():
    execute_query(ddl_dml.DROP_VIEW)
    execute_query(ddl_dml.DROP_SAMPLE_TBL)
    execute_query(ddl_dml.DROP_CSNO_TBL)

def check_query(query):
    execute_query(query)

if __name__ == "__main__":
    release_all()
    create_sample_table_partition(); check_query(ddl_dml.SHOW_TABLES)
    insert_sample_data()
    query_sample_data()
    release_all()