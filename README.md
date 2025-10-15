# 🦠 Covid-19 Data Platform

A personal **Data Engineering** project focused on building a **Data Lake** and **Data Warehouse** for COVID-19 analytics.  
This platform enables **automated data pipelines** with **daily updates**, supporting downstream analytics and dashboarding in **Power BI**.  

Through this project, you’ll learn how to:
- Design and build a **Data Lake** and **Data Warehouse** architecture  
- Apply **data modeling** (Star Schema, Medallion Architecture)  
- Implement **incremental loading** for fact and dimension tables  
- Orchestrate end-to-end pipelines using **Apache Airflow**

---

## ⚙️ Technologies

| Layer | Technology | Description |
|-------|-------------|-------------|
| **Data Processing** | Apache Spark (SparkSQL) | Transform and process large datasets |
| **Data Orchestration** | Apache Airflow | Automate and schedule ETL workflows |
| **Data Storage** | AWS S3 | Data Lake (Bronze → Silver → Gold) |
|  | AWS Redshift | Data Warehouse for analytics |
| **Data Modeling** | Star Schema | Fact and Dimension tables for BI |

---

## 🔄 Data Flow

<img src="https://github.com/BlueElf246/covid_data_platform/blob/main/images/Data%20Flows.png?raw=true" alt="image" width="700" height="700">

---

## 🧰 ETL Architecture

<img src="https://github.com/BlueElf246/covid_data_platform/blob/main/images/ELT%20tool.png?raw=true" alt="image" width=“1000” height=“1000”>

---

## 📁 Project Structure

```bash
.
├── common
│   ├── config.py
│   ├── db_utils.py
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
│   ├── load_landing.py
│   └── utils_ingest.py
├── load
│   ├── load_to_dwh.py
│   └── utils_load.py
├── transformation
│   ├── load_to_silver.py
│   ├── transform_data.py
│   └── utils_transform.py
├── requirements.txt
└── STRUCTURE.txt
```
# How To Run
This project can be run idenpendently with or without airflow. 
Refer configs_sample and rename into configs
## Clone repo
```
git clone https://github.com/BlueElf246/covid_data_platform.git
cd project
```
## Install dependencies
```
python3 -m venv venv
pip install -r requirements.txt
```
## Install Airflow
```
Please follow the instruction
install_airflow.txt
```
## Run with airflow
Run the 'load_DWH' dag from airflow
## Run without airflow
Run seperate .py files

<img src="https://github.com/BlueElf246/covid_data_platform/blob/main/images/Scripts.png?raw=true" alt="image" width=“500” height=“500”>




