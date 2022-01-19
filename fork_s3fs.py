from s3fs import S3FileSystem
import s3fs
from multiprocessing import Process
import pandas as pd
from pandas import DataFrame
import fastparquet as fp

AWS_ACCESS_KEY = "AKIAXKASUCZNAZ27PIP2"
AWS_SECRET_KEY = "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"

class S3FSHandler:
    def __init__(self, key, secret):
        self.s3 = s3fs.S3FileSystem(key=key, secret=secret)
        self.fs = s3fs.core.S3FileSystem(key=key, secret=secret)
    def file_upload(self, bucket, filename, df):
        df.to_parquet(f'{bucket}/{filename}', compression='snappy')
#        with self.s3.open(bucket, 'wb') as ofp:
#            df.to_parquet(ofp, f'{bucket}/{filename}', compression='snappy')
    def file_read(self,bucket) -> DataFrame:
        s3_path = 's3://athena-test-buck-rome/*.parquet'
        all_paths_from_s3 = self.fs.glob(path=s3_path)

        myopen = self.s3.open
        # use s3fs as the filesystem
        fp_obj = fp.ParquetFile(all_paths_from_s3, open_with=myopen)
        # convert to pandas dataframe
        df = fp_obj.to_pandas()
        return df
def runner(handler):
    df = pd.DataFrame({"a": [1, 2, 3, 4, 5]})
    handler.file_upload('s3://athena-test-buck-rome','test.parquet', df)

    rsdf = handler.file_read('s3://athena-test-buck-rome')
    print(rsdf)


class Executor:
    def __init__(self):
        self.handler=S3FSHandler(AWS_ACCESS_KEY, AWS_SECRET_KEY)
        print("yes")

    def run(self):
        proc = Process(target=runner, args=(self.handler,))
        proc.start()
        proc.join()



procs=[]
if __name__ == '__main__':
#    Executor().run()
    eval('Executor')().run()


