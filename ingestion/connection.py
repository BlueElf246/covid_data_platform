import sys, os


project_root = os.path.abspath("/u01/datle/project_root")

# Thêm vào PYTHONPATH
if project_root not in sys.path:
    sys.path.append(project_root)


from common.db_utils import DBHelper
from common.s3_utils import S3Connector
if __name__ == "__main__":
    path='/u01/datle/project_root/configs/ingestion_config.yaml'
    db = DBHelper(path,'data_source_1')
    db.connect()

    s3 = S3Connector(path,'data_lake_aws')
    s3.test_connection('covid192406')

    db = DBHelper(path,'data_warehouse')
    db.connect()