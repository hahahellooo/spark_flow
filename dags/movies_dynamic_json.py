from datetime import datetime, timedelta
from textwrap import dedent
from pprint import pprint
import sys
import os
import json

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
    'dynamic_json',
    default_args={
        'depends_on_past': False,
        'retries': 1,
        'retry_delay': timedelta(seconds=3),
    },
    max_active_runs=1,
    max_active_tasks=3,
    description='pyspark_movie',
    schedule="@yearly",
    start_date=datetime(2014, 1 ,1),
    end_date=datetime(2024,12,31),
    catchup=True,
    tags=['pyspark', 'movie', 'api', 'atm'],
) as dag:

    def get_data(year):
        from spark_flow.movies_dynamic_json_m import save_movies, save_json
        data = save_movies(year)
        file_path = f'/home/hahahellooo/data/movies_page/year={year}/data.json'
        d = save_json(data, file_path)
        print(d, file_path)

        return d

    
    get_data = PythonVirtualenvOperator(
            task_id="get.data",
            python_callable=get_data,
            requirements=["git+https://github.com/hahahellooo/spark_flow.git@0.2.0/airflowdag"],
            system_site_packages=False,
            op_args=["{{logical_date.strftime('%Y')}}"]
    )

    parsing_parquet = BashOperator(
        task_id='parsing.parquet',
        bash_command="""
            $SPARK_HOME/bin/spark-submit /home/hahahellooo/code/spark_flow/py/pasing_parquet.py {{logical_date.strftime('%Y')}}
        """

    )

    select_parquet = BashOperator(
        task_id='select.parquet',
        bash_command="""
            $SPARK_HOME/bin/spark-submit /home/hahahellooo/code/spark_flow/py/select_parquet.py {{logical_date.strftime('%Y')}}
        """

    )
    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')

    start >> get_data  >> parsing_parquet >> select_parquet >> end


