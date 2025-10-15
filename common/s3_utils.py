import boto3
from io import StringIO
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from common.config import ConfigLoader
class S3Connector:
    def __init__(self, path, s3_key):
        self.config = ConfigLoader(path).get(s3_key)
        self.aws_access_key = self.config['AWS_ACCESS_KEY']
        self.aws_secret_key = self.config['AWS_SECRET_KEY']
        self.aws_region = self.config['AWS_REGION']
        # Tạo S3 client
        self.s3 = boto3.client(
            's3',
            aws_access_key_id=self.aws_access_key,
            aws_secret_access_key=self.aws_secret_key,
            region_name=self.aws_region
        )
    def test_connection(self, bucket_name=None):
        """
        Kiểm tra xem có kết nối được tới S3 không.
        Nếu bucket_name được cung cấp, sẽ thử list object trong bucket đó.
        """
        try:
            # Gọi API đơn giản để kiểm tra credentials
            self.s3.list_buckets()
            print("✅ AWS credentials hợp lệ.")

            if bucket_name:
                # Kiểm tra quyền truy cập bucket cụ thể
                response = self.s3.list_objects_v2(Bucket=bucket_name, MaxKeys=1)
                if "Contents" in response:
                    print(f"✅ Kết nối thành công tới bucket: {bucket_name}")
                else:
                    print(f"ℹ️  Bucket {bucket_name} trống nhưng có thể truy cập được.")
            return True

        except self.s3.exceptions.ClientError as e:
            print(f"❌ Lỗi khi kết nối tới S3: {e}")
            return False
        except Exception as e:
            print(f"❌ Lỗi không xác định: {e}")
            return False


    def upload_dataframe(self, df, bucket, key):
        """Upload pandas DataFrame lên S3"""
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False)
        self.s3.put_object(Bucket=bucket, Key=key, Body=csv_buffer.getvalue())
        print(f"✅ Uploaded {key} to bucket {bucket}")

    def check_exists(self, bucket, folder):
        """Kiểm tra file có tồn tại trên S3 hay không"""
        try:
            if self.s3.list_objects_v2(Bucket=bucket, Prefix=folder, MaxKeys=1)['KeyCount'] >= 1:
                return True
            else:
                return False
        except self.s3.exceptions.ClientError as e:
            print(e)
            if e.response['Error']['Code'] == "404":
                return False
            else:
                raise e

    def download_file(self, bucket, key, local_path):
        """Tải file từ S3 về"""
        self.s3.download_file(bucket, key, local_path)
        print(f"⬇️  Downloaded {key} → {local_path}")

    def read_parquet_spark(self, spark: SparkSession, bucket: str, key: str) -> DataFrame:
        """Đọc parquet từ S3 bằng Spark"""
        s3_path = f"s3a://{bucket}/{key}"

        # Cấu hình credential cho Spark
        hadoop_conf = spark._jsc.hadoopConfiguration()
        hadoop_conf.set("fs.s3a.access.key", self.aws_access_key)
        hadoop_conf.set("fs.s3a.secret.key", self.aws_secret_key)
        hadoop_conf.set("fs.s3a.endpoint", f"s3.{self.aws_region}.amazonaws.com")

        print(f"📖 Reading Parquet from {s3_path}")
        return spark.read.parquet(s3_path)

    def write_parquet_spark(self, df: DataFrame, bucket: str, key: str, mode="overwrite", partition_col=None):
        """Ghi DataFrame Spark xuống S3"""
        s3_path = f"s3a://{bucket}/{key}"
        hadoop_conf = df.sparkSession._jsc.hadoopConfiguration()
        hadoop_conf.set("fs.s3a.access.key", self.aws_access_key)
        hadoop_conf.set("fs.s3a.secret.key", self.aws_secret_key)
        hadoop_conf.set("fs.s3a.endpoint", f"s3.{self.aws_region}.amazonaws.com")

        writer = df.write.mode(mode)
        if partition_col:
            writer = writer.partitionBy(partition_col)

        writer.parquet(s3_path)
        print(f"✅ Wrote Parquet to {s3_path} (mode={mode})")

    def query_data_spark(self, spark, table_list, query):
        """
            spark (SparkSession): The Spark session to use for executing the query.
            table_list (list of tuples): List of (name, DataFrame) pairs to register as temporary views.
            query (str): The SQL query to execute.

            Returns:
            DataFrame: The result of the SQL query as a Spark DataFrame.
        """
        for name, df in table_list:
            df.createOrReplaceTempView(name)
        result = spark.sql(query)
        return result