import json
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta



# =========================
# 1) 요청 보낼 JSON Payload
# =========================
payload = {
    "Domain": "Unity/Collect",
    "Environ": "dev",
    "Creator": "PTR.Prime.Unity.batch.RealPriceA",
    "JobGroup": "",
    "JobName": "",
    "BeginTime": "",
    "EndTime": "",
    "StatusCode": "",
    "StatusText": "",
    "LogKey": "",
    "Task": [
        {
            "TaskSeq": 1,
            "TaskGroup": "mois_go_kr",
            "TaskName": "t_legal_dong",
            "ProcessModel": "In/Out",
            "TaskType": "TaskCommand",
            "TaskDictionary": [],
            "TaskQuery": [],
            "ServerName": "krServer24",
            "Version": "1.0.9211.36117",
            "BeginTime": "2025-12-10T11:26:44.073+09:00",
            "EndTime": "",
            "RunTimeout": 100,
            "StatusCode": "",
            "StatusText": ""
        }
    ]
}

API_URL = "http://localhost:8082/jobs/collect/execute"


import logging

def post_execute_job():
    logger = logging.getLogger("airflow.task")
    headers = {"Content-Type": "application/json"}

    logger.info(f"[POST] {API_URL}")
    try:
        r = requests.post(API_URL, headers=headers, data=json.dumps(payload), timeout=120)
        logger.info(f"[HTTP] {r.status_code}")
        try:
            logger.info(json.dumps(r.json(), ensure_ascii=False, indent=2))
        except Exception:
            logger.error(r.text)
        r.raise_for_status()
    except Exception as e:
        logger.error(f"Error occurred: {e}", exc_info=True)
        raise


# Airflow DAG 설정
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='법정동_DAG',
    
    default_args=default_args,
    description='법정동 처리 DAG',
    schedule_interval='@daily',  # 필요시 수정
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['법정동'],
) as dag:
    run_post_execute_job = PythonOperator(
        task_id='post_execute_job',
        python_callable=post_execute_job,
    )
