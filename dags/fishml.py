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
    'fishml',
    default_args={
        'depends_on_past': False,
        'retries': 1,
        'retry_delay': timedelta(seconds=3),
    },
    max_active_runs=1,
    max_active_tasks=3,
    description='fishml',
    schedule="* * 1 * *",
    start_date=datetime(2015, 1 ,1),
    end_date=datetime(2015,1,3),
    catchup=True,
    tags=['ml', 'csv', 'agg'],
) as dag:

   # def re_partition(ds_nodash):
   #     from spark_flow.pyspark_m import re_partition
   #     re_partition(ds_nodash)
    
    load_csv = BashOperator(
        task_id='load.csv',
        bash_command="""
        echo "load_csv"
        """
        #python_callable= load_csv,
    ) 


    predict = BashOperator(
            task_id="predict",
            bash_command="""
            echo "predict"
            """
     )

    agg = BashOperator(
        task_id='agg',
        bash_command="""
        echo "agg"
        """

        )
    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')

    start >> load_csv  >> predict >> agg >> end


