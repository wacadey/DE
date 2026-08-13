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
# 콜백함수

# DAG 정의

  # task 정의 
  task_create_table = 
  task_extract      = 
  task_transform    = 
  task_load         = 

  # 의존성