'''
- 고객 정보가 담긴 데이터베이스가 있음
  CREATE TABLE IF NOT EXISTS customers (
      user_id VARCHAR(50) PRIMARY KEY,
      income INT DEFAULT NULL,
      loan_amt INT DEFAULT NULL,
      credit_score INT DEFAULT NULL,
      grade VARCHAR(10) DEFAULT NULL,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
  )
- 매일 고객이 가입, 업무, 등등 디비 내용이 갱신 (설정)
- 다음날 00시 01분 00초에 고객 디비 가져와서(extract) -> 신용평가(api 서버 요청) -> 평가결과 획득 -> 고객정보 업데이트
  - 갱신 주기는 변경될 수 있다(회사별 상이)
- 배치 데이터 프로세스 작업 - airflow
  - Task 정리 (PythonOperator)
    - t1 : mysql 사용, 테이블이 없으면 생성, 고객 데이터 더미 입력(매번 수행-해쉬(UUID)적용)
      - 원래 배치 작업에서는 필요 없는 작없임
    - t2 : 고객 데이터 획득 (DB -> DAG의 TI) => XCOM 게시 df or dict
    - t3 : XCOM 데이터 획득 => API 호출 => 서버 고객데이터 전송 => 신용평가 진행 => 응답 => XCOM 게시
    - t4 : XCOM 데이터 획득 => 신용평가 결과 => 고객 디비 업데이트
'''
# 뼈대(구조) 구성
# 1. 모듈
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from datetime import datetime, timedelta
import logging
import pendulum
import random
# API 호출용
import requests

# 전역변수
KST       = pendulum.timezone("Asia/Seoul")
# airflow 내부에서 api 서버 호출 => host 정보는 서비스명(DOCKER-COMPOSE.YML 내에 services 내에 헤당서비스명)
API_URL   = 'http://ai-api-server:8000/predict'

# 3-1. 콜백함수
def _create_dummy_data(**kwargs):
  pass
def _extract_user_data(**kwargs):
  pass
def _api_service_call(**kwargs):
  pass
def _load_user_credit(**kwargs):
  pass

# 2. DAG
with DAG( 
  dag_id      = "07_api_server_used",
  description = "특정 주기 단위로 고객 신용 정보 업데이트",
  default_args= {
    "owner"           : "aic-de1-admin",    
    "retries"         : 1,
    "retry_delay"     : timedelta(minutes=1)
  },
  schedule_interval = "@daily", # 00시 01분 00초에 참고용
  start_date  = pendulum.datetime( 2026,6,29, tz=KST ),
  catchup     = False,
  tags        = ['etl', 'api']
) as dag:
  # 3. Task
  t1 = PythonOperator()
  t2 = PythonOperator()
  t3 = PythonOperator()
  t4 = PythonOperator()
  
  # 4. 의존성
  t1 >> t2 >> t3 >> t4