import os
import yaml
import psycopg2   # hoặc cx_Oracle nếu bạn dùng Oracle
from common.config import ConfigLoader
import pandas as pd
class DBHelper:
    """Class để kết nối và thao tác với Database."""

    def __init__(self, path, db_key="database"):
        # Đọc config
        self.config = ConfigLoader(path).get(db_key)
        self.conn = None
        self.cursor = None

    def connect(self):
        """Khởi tạo kết nối DB."""
        if not self.config:
            raise ValueError("Không tìm thấy thông tin database trong config.yaml")

        try:
            self.conn = psycopg2.connect(
                host=self.config["host"],
                port=self.config.get("port", 5432),
                user=self.config["user"],
                password=self.config["password"],
                dbname=self.config["dbname"],
            )
            self.cursor = self.conn.cursor()
            print(f"✅ Kết nối thành công tới {self.config['type']}")
        except Exception as e:
            raise ConnectionError(f"❌ Không thể kết nối DB: {e}")

    def execute_query(self, query, params=None, fetch=False):
        """Thực thi truy vấn SQL."""
        if not self.conn:
            self.connect()

        try:
            self.cursor.execute(query, params or ())
            if fetch:
                columns = [col[0] for col in self.cursor.description]
                rows = self.cursor.fetchall()
                return pd.DataFrame(rows, columns=columns)
            else:
                self.conn.commit()
                print("✅ Query executed successfully.")
        except Exception as e:
            self.conn.rollback()
            print(f"⚠️ Lỗi khi thực thi query: {e}")
            raise e
    def read_postgres_with_spark(self, spark, query):
        PG_HOST = self.config["host"]
        PG_PORT = self.config.get("port", 5432)
        PG_DB = self.config["dbname"]
        PG_USER = self.config["user"]
        PG_PASSWORD = self.config["password"]

        try:
            df = spark.read.jdbc(
                url=f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}",
                table=query,
                properties={"user": PG_USER, "password": PG_PASSWORD, "driver": "org.postgresql.Driver"}
            )
            return df
        except Exception as e:
            print("❌ Lỗi khi kết nối:", e)
    def close(self):
        """Đóng kết nối DB."""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
        print("🔒 Đã đóng kết nối DB.")

    def write_to_postgres_with_spark(self, df, table_name, mode="append"):
        PG_HOST = self.config["host"]
        PG_PORT = self.config.get("port", 5432)
        PG_DB = self.config["dbname"]
        PG_USER = self.config["user"]
        PG_PASSWORD = self.config["password"]

        try:
            df.write.jdbc(
                url=f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}",
                table=table_name,
                mode=mode,  # "append" hoặc "overwrite"
                properties={
                    "user": PG_USER,
                    "password": PG_PASSWORD,
                    "driver": "org.postgresql.Driver"
                }
            )
            print(f"✅ Đã ghi DataFrame vào bảng '{table_name}' (mode={mode})")
        except Exception as e:
            print(f"⚠️ Lỗi khi ghi DataFrame vào PostgreSQL: {e}")
