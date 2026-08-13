'''
- airflow 내부 정보 접근, 출력시 jinja 활용, 내부 정보 접근시 macro 활용
'''
# 1. 모듈
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import logging
import pendulum

# 2. 전역변수
KST = pendulum.timezone("Asia/Seoul")

# 3. DAG
with DAG(
  dag_id      = "03_basics_context_jinja",
  description = "macro을 이용하여 context 접근, jinja를 통해 표현",
  default_args= {
    "owner"           : "aic-de1-admin",    
    "retries"         : 1,
    "retry_delay"     : timedelta(minutes=1)
  },
  # 매일 오전 9시 00분에 스케줄 작동
  schedule_interval = "0 9 * * *", # cron 방식으로 표기 (분, 시, 일, 월, 주)
  # 수행 시작 시간 서울 시간대 타임존 조정
  start_date  = pendulum.datetime( 2026,6,29, tz=KST ),
  catchup     = False,
  tags        = ['macro', 'context', 'jinja']
) as dag:

  # 4. 오퍼레이터

  # 5. 의존성(수행순서)