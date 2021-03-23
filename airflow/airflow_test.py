import os

from datetime import datetime, timedelta

from airflow import DAG
import airflow.operators.bash
import airflow.operators.python

OUTPUT_ONE_PATH = "output_one.txt"
OUTPUT_TWO_PATH = "output_two.txt"


def decorate_file(input_path, output_path):
    with open(input_path, "r") as in_file:
        line = in_file.read()

    with open(output_path, "w") as out_file:
        out_file.write("My "+line)


default_args = {
    "owner": "lorenzo",
    "depends_on_past": False,
    "start_date": datetime(2018, 9, 12),
    "email": ["l.peppoloni@gmail.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}


dag = DAG(
    "simple_dag",
    default_args=default_args,
    schedule_interval="0 12 * * *",
    )

t1 = bash(
    task_id="print_file",
    bash_command='echo "pipeline" > {}'.format(OUTPUT_ONE_PATH),
    dag=dag)

t2 = PythonOperator(
    task_id="decorate_file",
    python_callable=decorate_file,
    op_kwargs={"input_path": OUTPUT_ONE_PATH, "output_path": OUTPUT_TWO_PATH},
    dag=dag)

t1 >> t2