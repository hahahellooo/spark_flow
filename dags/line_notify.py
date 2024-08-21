from datetime import datetime, timedelta
from textwrap import dedent
from pprint import pprint
import sys
import os

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

from airflow.operators.python import (
        PythonOperator, 
        BranchPythonOperator, 
        PythonVirtualenvOperator,

)

with DAG(
    'line_notify',
    default_args={
        'depends_on_past': False,
        'retries': 1,
        'retry_delay': timedelta(seconds=3),
    },
    max_active_runs=1,
    max_active_tasks=3,
    description='pyspark_movie',
    schedule="10 2 * * *",
    start_date=datetime(2024, 8 ,1),
    end_date=datetime(2024,8,10),
    catchup=True,
    tags=['pyspark', 'movie', 'api', 'atm'],
) as dag:
    
    bash_job = BashOperator(
            task_id="bash.job",
            bash_command="""
            echo "bash.job"
            # 0 또는 1을 랜덤하게 생성
            RANDOM_NUM=$RANDOM
            REMAIN=$(( RANDOM_NUM % 2 ))
            echo "RANDOM_NUM:$RANDOM_NUM, REMAIN:$REMAIN"

            if [ $REMAIN -eq 0 ]; then
                echo "작업이 성공했습니다."
                exit 0
            else
                echo "작업이 실패했습니다."
                exit 1
            fi
            """
     )

    notify_success = BashOperator(
            task_id="notify.success",
            bash_command=""" 
            echo "notify.success"
            echo "dag_id:{{ dag_run.dag_id }}"
            curl -X POST -H 'Authorization: Bearer UuAPZM7msPnFaJt5wXTUx34JqYKO7n3AUlLq4b3eyZ4' -F 'message={{ dag_run.dag_id }} success' https://notify-api.line.me/api/notify
            """
     )

    notify_fail = BashOperator(
            task_id="notify.fail",
            bash_command="""
            echo "notify.fail"
            echo "dag_id:{{ dag_run.dag_id }}"
            curl -X POST -H 'Authorization: Bearer UuAPZM7msPnFaJt5wXTUx34JqYKO7n3AUlLq4b3eyZ4' -F 'message={{ dag_run.dag_id }} fail' https://notify-api.line.me/api/notify
            """,
            trigger_rule="one_failed"
     )
        
    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')

    start >> bash_job
    bash_job >> [notify_success, notify_fail] >> end


