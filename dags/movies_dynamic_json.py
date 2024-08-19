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
    'pyspark_movie',
    default_args={
        'depends_on_past': False,
        'retries': 1,
        'retry_delay': timedelta(seconds=3),
    },
    max_active_runs=1,
    max_active_tasks=3,
    description='pyspark_movie',
    schedule="10 2 * * *",
    start_date=datetime(2015, 1 ,1),
    end_date=datetime(2016,1,1),
    catchup=True,
    tags=['pyspark', 'movie', 'api', 'atm'],
) as dag:

    get_data = BashOperator(
            task_id="get.data",
            bash_command="""
            echo "get.data"
            """
    )

    parsing_parquet = BashOperator(
        task_id='parsing.parquet',
        bash_command="""
        echo "parsing.parquet"
        """

    )

    select_parquet = BashOperator(
        task_id='select.parquet',
        bash_command="""
        echo "select.parquet"                                                             """

    )
    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')

    start >> get_data  >> parsing_parquet >> select_parquet >> end


