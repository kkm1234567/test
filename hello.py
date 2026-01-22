from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def print_hello_start():
	print("hello_start 실행")

def print_hello1():
	print("hello1 실행")

def print_hello2():
	print("hello2 실행")

def print_hello3():
	print("hello3 실행")

def print_hello_end():
	print("hello_end 실행")

with DAG(
	dag_id="hello_parallel",
	start_date=datetime(2024, 1, 1),
	schedule_interval=None,
	catchup=False,
) as dag:
	hello_start = PythonOperator(task_id="hello_start", python_callable=print_hello_start)
	hello1 = PythonOperator(task_id="hello1", python_callable=print_hello1)
	hello2 = PythonOperator(task_id="hello2", python_callable=print_hello2)
	hello3 = PythonOperator(task_id="hello3", python_callable=print_hello3)
	hello_end = PythonOperator(task_id="hello_end", python_callable=print_hello_end)

	hello_start >> [hello1, hello2, hello3] >> hello_end
