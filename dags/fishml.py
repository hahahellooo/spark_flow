from datetime import datetime, timedelta
import sys
import os
import pandas as pd
# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
import shutil
import sklearn
from airflow.models import Variable
# Operators; we need this to operate!
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
import os
from sklearn.neighbors import KNeighborsClassifier
import pickle
from fishmlserv.model.manager import get_model_path
from airflow.operators.python import (
        PythonOperator, 
        BranchPythonOperator, 
        PythonVirtualenvOperator,

)

with DAG(
    'predict_fishml',
    default_args={
        'depends_on_past': False,
        'retries': 1,
        'retry_delay': timedelta(seconds=3),
    },
    max_active_runs=1,
    max_active_tasks=3,
    description='predict_fishml',
    schedule='@once',
    start_date=datetime(2024, 9, 2),
    end_date=datetime(2024,9,3),
    catchup=True,
    tags=['ml', 'csv', 'agg'],
) as dag:

   # def re_partition(ds_nodash):
   #     from spark_flow.pyspark_m import re_partition
   #     re_partition(ds_nodash)
    def save_parquet_fish():
        par_path='/home/hahahellooo/data/fish_parquet/fish_data.parquet'
        if not os.path.exists(par_path):
            os.makedirs(os.path.dirname(par_path), exist_ok = True)
            # mkdir 폴더1개만 생성가능(중간폴더 없으면 에러)
            # makedirs 최종경로에 이르기까지 중간 경로의 폴더들을 자동생성
            # exist_ok = True 파일 경로가 이미 있으면 건너뛰기
        df = pd.read_csv('/home/hahahellooo/data/fish_data_100k.csv')
        df.columns = ['Length', 'Weight', 'Label']
        def func1(x):
            if x == "Bream":
                return 0
            else:
                return 1
        df['Target'] = df['Label'].map(func1)
        print(df.head(5))
        # func1의 함수가 적용된 결과값이 target이라는 새로운 컬럼에 저장
        # map() 메서드를 사용하여 Series의 각 요소를 매핑하여 변환
            # mkdir 폴더1개만 생성가능(중간폴더 없으면 에러)
            # makedirs 최종경로에 이르기까지 중간 경로의 폴더들을 자동생성
            # exist_ok = True 파일 경로가 이미 있으면 건너뛰기
        df.to_parquet('/home/hahahellooo/data/fish_parquet/fish_data.parquet', index = False)
        return True

    def predict():
        df = pd.read_parquet('/home/hahahellooo/data/fish_parquet/fish_data.parquet')
        features = df[['Length', 'Weight']]
        target = df['Target']
        model_path = get_model_path()
        # 'b' 바이너리 모드
        # 바이너리 데이터를 처리할 때 사용(이 경우, pickle로 직렬화된 객체)
        with open(model_path, 'rb') as f:
            # pickle은 python에서 객체를 직렬화하고 역직렬화하는데 사용하는 모듈
            fish_model = pickle.load(f)
        
        prediction = fish_model.predict(features)

        result_df = pd.DataFrame({'target': target, 'Result':prediction})
        result_path = '/home/hahahellooo/data/fish_parquet/result.parquet'
        if os.path.exists(result_path):
            return True
        else:
            os.makedirs(os.path.dirname(result_path), exist_ok = True)
        result_df.to_parquet(result_path, index = False) # 예측 결과 파일 parquet 저장
        return True

    def agg():
        pre_result = pd.read_parquet('/home/hahahellooo/data/fish_parquet/result.parquet')
        print(pre_result.columns)
        print(pre_result.head())
        agg_result = pre_result.groupby('Result').size()
        print(agg_result)
        return True

    load_csv = PythonOperator(
        task_id='load.csv',
        python_callable= save_parquet_fish
    ) 


    predict = PythonOperator(
            task_id="predict",
            python_callable=predict
     )

    agg = PythonOperator(
        task_id='agg',
        python_callable=agg

        )
    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')

    start >> load_csv  >> predict >> agg >> end


