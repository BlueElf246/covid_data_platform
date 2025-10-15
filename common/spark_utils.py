from pyspark.sql import SparkSession
from common.config import ConfigLoader
class SparkConnector:
    """
    Class quản lý kết nối SparkSession — dùng chung cho Ingestion / Transformation / Load.
    """

    def __init__(self, app_name="spark_app", s3_config=None, path=None):
        """
        :param app_name: Tên ứng dụng Spark (hiển thị trong UI)
        :param s3_config: dict chứa thông tin AWS (tùy chọn)
        """
        self.app_name = app_name
        if s3_config is not None:
            config = ConfigLoader(path).get(s3_config)
        self.s3_config = config or {}
        self.spark = None

    def connect(self):
        """
        Khởi tạo và trả về SparkSession, tự động cấu hình S3 nếu có.
        """
        builder = (
            SparkSession.builder
            .appName(self.app_name)
            .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        )

        # Nếu có cấu hình AWS thì set cho Spark
        if self.s3_config:
            builder = builder \
                .config("spark.hadoop.fs.s3a.access.key", self.s3_config.get("AWS_ACCESS_KEY")) \
                .config("spark.hadoop.fs.s3a.secret.key", self.s3_config.get("AWS_SECRET_KEY")) \
                .config("spark.hadoop.fs.s3a.endpoint", f"s3.{self.s3_config.get('AWS_REGION', 'ap-southeast-1')}.amazonaws.com") \
                .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

        self.spark = builder.getOrCreate()
        print(f"✅ SparkSession '{self.app_name}' đã được khởi tạo.")
        return self.spark

    def stop(self):
        """Đóng SparkSession khi không dùng nữa."""
        if self.spark:
            self.spark.stop()
            print("🛑 SparkSession đã được dừng.")
