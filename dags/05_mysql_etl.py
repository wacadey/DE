'''
- etl 가볍게 적용, 데이터등 더미 구성, 적제는 mysql 임시 진행
- 데이터 소규모 -> pandas 사용
- 1개의 DAG에서 ETL 처리
- 필요 패키지 : 로컬 PC기반 apache-airflow-providers-mysql pandas
  - 가상환경 (cmd 터미널 오픈 : vscode에서)
  (airflow) > pip install apache-airflow-providers-mysql pandas
'''
# 1. 모듈
from airflow import DAG
from airflow.operators.python import PythonOperator
# 범용 sql 처리 오퍼레이터
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
# Load 처리시 데이터 밀어 넣기시 활용
from airflow.providers.mysql.hooks.mysql import MySqlHook
from datetime import datetime, timedelta
import logging
import pendulum
# 데이터
import json
import random
import pandas as pd
import os

# 실습 기본 구성 틀 작성
# 2. 전역변수
KST = pendulum.timezone("Asia/Seoul")
# extract 한 데이터를 임시로 저장하는 위치
DATA_PATH = "/opt/airflow/dags/data"
os.makedirs(DATA_PATH, exist_ok=True)

# 콜백함수
def _extract(**kwargs):
  '''
    - 스마트팩토리에 설치된 오븐 센서에서 데이터가 발생했고, 데이터 레이크(s3)에 저장되어있다 가정
    - s3에서 가져왔다 가정    
  '''
  # 더미 데이터 구성 = [ {}, {}, ... ]
  data = [
    {
      "sensor_id"   : f"SENSOR_{i+1}",      # 장비 ID
      "timestamp"   : datetime.now().strftime("%Y-%m-%d %H:%M:%S"), # 데이터 생성 시간
      "temperature" : round( random.uniform(20.0, 150.0), 2),       # 온도 허용 범위 
      "status"      : "on"                  # on or off
    }
    for i in range(10)
  ]
  # 데이터를 다음 task에 전달 -> 데이터 전달 or 데이터 저장(리눅스 경로상)후 path 전달
  # 파일명 sensor_data_20260813.json : 20260813=>"ds_nodash" 활용
  # /opt/airflow/dags/data/sensor_data_DAG수행일.json
  file_full_path = f"{DATA_PATH}/sensor_data_{kwargs['ds_nodash']}.json"
  with open(file_full_path, 'w') as f:
    json.dump( data, f )
  
  logging.info( f'extract 한 데이터 {data}'  )
  logging.info( f'extract 한 데이터 파일 경로 {file_full_path}'  )
  # XCOM을 통해 전달
  return file_full_path

def _transform(**kwargs):
  # 1. _extract가 전달한 내용 XCom을 통해서 획득
  ti              = kwargs["ti"]
  json_file_path  = ti.xcom_pull(task_ids="extract")
  logging.info( f'전달한 데이터 파일 경로 {json_file_path}'  )

  # 2. transform => 데이터 clean, 전처리(단위변경, 파생변수, ....)
  #    섭씨 온도를 화씨 온도로 계산 -> 파생변수 추가 -> pandas의 DataFrame 활용
  #    섭씨 온도 100도 이하(<=)만 센서가 정상, 그 이상은 이상탐지의 대상으로 간주 -> 이상치 제거(컨셉)
  # 2-1. json file -> load -> DataFrame
  df = pd.read_json( json_file_path )
  # 2-2. 이상치 제거(컨셉) -> clean 작업의 범주, 100도 이하만(조건->블리언) 추출(인덱싱):블리언 인덱싱
  target_df = df[ df['temperature'] <= 100  ].copy()
  # 2-3. 파생 변수 생성 -> 섭씨 => 화씨
  #      °F = (°C × 9/5) + 32
  target_df["temperature_f"] = (target_df['temperature'] * 9/5) + 32

  logging.info( f'가공된 데이터 (rows, cols) {target_df.shape}'  )
  # 3. 전처리된 내용 [v]csv|parquet|... 저장
  csv_file_path = f"{DATA_PATH}/preprocessing_data_{kwargs['ds_nodash']}.csv"
  target_df.to_csv(csv_file_path, index=False)
  logging.info( f'가공된 데이터 저장 {csv_file_path}'  )

  # 4. 반환, XCOM 전달, 개시
  return csv_file_path


def _load(**kwargs):
  # 1. csv 경로 획득
  ti             = kwargs["ti"]
  csv_file_path  = ti.xcom_pull(task_ids="transform")
  logging.info( f'전달한 데이터 파일 경로 {csv_file_path}'  )
  
  # 2. csv -> df 로드
  df = pd.read_csv( csv_file_path )

  # 3. mysql 연결 -> 데이터 삽입
  hooks = MySqlHook(mysql_conn_id="mysql_default")
  try:
    with hooks.get_conn() as conn:
      pass
  except Exception as e:
    logging.error(f'sql  에러 { e }')
  pass

# DAG 정의
with DAG( 
  dag_id      = "05_mysql_etl",
  description = "etl 수행하여 mysql에 온도 센서 데이터 적제",
  default_args= {
    "owner"           : "aic-de1-admin",    
    "retries"         : 1,
    "retry_delay"     : timedelta(minutes=1)
  },
  schedule_interval = "@daily",
  start_date  = pendulum.datetime( 2026,6,29, tz=KST ),
  catchup     = False,
  tags        = ['etl', 'mysql']
) as dag:
  
  # task 정의 
  task_create_table = SQLExecuteQueryOperator(
    task_id         = "create_table",
    # 접속 정보 설정 -> 대시보드 > admin > connection 구성한 값 설정 ->id값
    conn_id         = "mysql_default",
    # 테이블이 없을때만 구성
    sql             = '''
      CREATE TABLE IF NOT EXISTS sensor_readings (
          id INT AUTO_INCREMENT PRIMARY KEY,
          sensor_id VARCHAR(50),
          timestamp DATETIME,
          temperature_c FLOAT,
          temperature_f FLOAT,
          created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      );
    '''
  )
  task_extract      = PythonOperator(
    task_id         = "extract",
    python_callable = _extract
  )
  task_transform    = PythonOperator(
    task_id         = "transform",
    python_callable = _transform
  )
  task_load         = PythonOperator(
    task_id         = "load",
    python_callable = _load
  )

  # 의존성
  task_create_table >> task_extract >> task_transform >> task_load