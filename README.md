# My Awesome Project
This project is designed to automate data synchronization between Oracle databases using GoldenGate.

.
├── common
│   ├── config.py
│   ├── db_utils.py
│   ├── __init__.py
│   ├── logger.py
│   ├── s3_utils.py
│   └── spark_utils.py
├── configs
│   ├── config_quality_check.yaml
│   ├── ingestion_config.yaml
│   ├── load_config.yaml
│   └── transform_config.yaml
├── dags
│   ├── dags.py
│   └── utils_dags.py
├── data_quality
│   ├── data_quality_check.py
│   └── utils_check.py
├── images
│   ├── Data Flows.png
│   ├── ELT tool.png
│   ├── Scripts.png
│   └── tree.rtf
├── ingestion
│   ├── connection.py
│   ├── __init__.py
│   ├── load_landing.py
│   ├── test.py
│   └── utils_ingest.py
├── load
│   ├── __init__.py
│   ├── load_to_dwh.py
│   └── utils_load.py
├── requirements.txt
├── STRUCTURE.txt
├── test.py
└── transformation
    ├── __init__.py
    ├── load_to_silver.py
    ├── transform_data.py
    └── utils_transform.py


# Clone repo
git clone https://github.com/yourname/project.git
cd project

# Cài đặt dependencies
pip install -r requirements.txt
