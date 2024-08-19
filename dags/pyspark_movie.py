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

    def re_partition(ds_nodash):
        from spark_flow.pyspark_m import re_partition
        re_partition(ds_nodash)
    
    re_partition = PythonOperator(
        task_id='re.partition',
        python_callable= re_partition,
    ) 


    join_df = BashOperator(
            task_id="join.df",
            bash_command="""
             $SPARK_HOME/bin/spark-submit $AIRFLOW_HOME/py/movie_join_df.py {{ ds_nodash }} "JOIN_DF_APP"
            """
     )

    agg_df = BashOperator(
        task_id='agg.df',
        bash_command="""
        $SPARK_HOME/bin/spark-submit $AIRFLOW_HOME/py/agg_movie.py {{ ds_nodash }} "AGG_DF_APP"
        """

        )
    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')

    start >> re_partition  >> join_df >> agg_df >> end


