'''
- PythonOperator 사용 패턴
- task간 통신 -> XCom 사용 (airflow 내부 컨텍스트 공간을 접근(엑세스), 게시판) -> task 상호 대화(통신)
- 공간의 한계 -> 공유 데이터는 raw 데이터가 아닌 raw 데이터에 접근 가능한 정보/작은 규모 raw 가능
'''

# 1. 모듈 가져오기
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import logging # 레벨별 로그 출력 (에러, 경고, 정보, 디버깅,..)

# 2-1. 콜백함수 정의
def _extract_cb(**kwargs):
  '''
  - kwargs : airflow가 작업하기 전에 내부 정보(context)를 접근할수 있는 내용 엔트리포인트
    - context : airflow가 주입한 정보(task 정보, 시간, ... 메타 정보). task 전달데이터    
  '''
  # 1. 컨텍스트(airflow 내부에 정보 저장공간)에서 특정 정보 추출, 키는 정의 되어 있음
  #    ti : <TaskInstance: 02_basics_python.extract_task manual__2026-08-12T06:37:53.765360+00:00 [running]>
  ti        = kwargs['ti']
  ds        = kwargs['ds']
  ds_nodash = kwargs['ds_nodash']
  run_id    = kwargs['run_id']  

  logging.info("=== Extract 작업 ===")
  logging.info(f" ti = {ti}")
  '''
    ds = 2026-08-12         <- task 수행 시간
    ds_nodash = 20260812    <- task 수행 시간 (- 제거됨)
    run_id    = manual__2026-08-12T06:40:55.384119+00:00
  '''
  logging.info(f" ds = {ds}")
  logging.info(f" ds_nodash = {ds_nodash}")
  logging.info(f" run_id    = {run_id}")

  # 정보 전달 -> XCOM 게시판에 본 task가 글을 작성하는것
  # XCOM을 통해서 특정 데이터를 push 하는 행위 -> 반환 행위 (return)
  return f"ds = {ds} ds_nodash = {ds_nodash} run_id    = {run_id}"

def _transform_cb(**kwargs):
  '''
  - kwargs을 통해서 다른 task가 XCOM으로 전달한 데이터(순서상 건더 뛰어도 관계 없음)
    - airflow conext 정보 획득 => "ti" => 전달된 데이터 접근(획득)
  '''
  # 1. ti 객체 획득
  ti = kwargs["ti"]

  # 2. XCOM을 통해서 데이터 획득
  #    "extract_task" 라는  id를 가진 Task의 게시물을 가져온다
  data = ti.xcom_pull(task_ids="extract_task")

  # 3. 데이터 확인
  logging.info("=== transform ===")
  logging.info(f"data = {data}")
  pass


# 2. DAG 정의
with DAG(
  dag_id      = "02_basics_python",
  description = "파이썬 task, XCOM 사용",
  default_args= {
    "owner"           : "aic-de1-admin",    
    "retries"         : 1,
    "retry_delay"     : timedelta(minutes=1)
  },
  schedule_interval = "@once", # 수동으로 한번 수행, 주기성 x
  start_date  = datetime(2026,6,29),
  catchup     = False,
  tags        = ['python', 'xcom']
) as dag: 
  # `ET`L
  # 3. Operator 정의 
  extract_task   = PythonOperator(
    task_id         = "extract_task",
    python_callable = _extract_cb # 콜백함수(실제 처리하는 업무 정의한 함수, 내부(_)에서만 사용)
  )
  transform_task = PythonOperator(
    task_id         = "transform_task",
    python_callable = _transform_cb
  )

  # 4. 의존성 정의
  extract_task >> transform_task