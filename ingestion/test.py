import sys, os


project_root = os.path.abspath("/u01/datle/project_root")

# Thêm vào PYTHONPATH
if project_root not in sys.path:
    sys.path.append(project_root)
from common.s3_utils import S3Connector
path='/u01/datle/project_root/configs/ingestion_config.yaml'
s3 = S3Connector(path,'data_lake_aws')
bucket = 'covid192406'
target_table='bronze_zone/covid1'
# target_table='bronze_zone/covid1/part-00000-694c6bd3-ac7c-4d7c-9ff0-af023b1fa9a8-c000.snappy.parquet'
r = s3.s3.list_objects_v2(Bucket=bucket, Prefix=target_table, MaxKeys=1)
print(r['KeyCount'])