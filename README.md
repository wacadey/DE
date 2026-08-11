# 데이터 생명주기
```
┌──────────────────────────────────────────────────────────────┐
│                 DATA ENGINEERING LIFECYCLE                   │
└──────────────────────────────────────────────────────────────┘

1. DATA GENERATION  : 로그/이벤트/생성/활동/디비..데이터 (더미생성,서비스통과생성,센서)
   데이터 발생
        │
        ▼
2. DATA INGESTION   : firehose > kinesis/kafka, .. 서비스
   데이터 수집 / 유입
        │
        ▼
3. DATA STORAGE     : s3 (데이터 레이크)등...
   원본 저장
        │
        ▼
4. DATA PROCESSING : ETL,ELT, 메달리온아킥텍처(브론즈,실버,골드), pandas/polars/spark,s3(중간 저장)
   정제 / 변환 / 집계
        │
        ▼
5. DATA MODELING   : 대시보드용 관제, 인사이트도출, 모델학습,... => 목적, athena(SQL), openseach(ELK,..),..
   분석 가능한 데이터 구조화
        │
        ▼
6. DATA SERVING    : 실제 서비스 파트로 데이터 공급
   조회 / 분석 / 활용

────────────────────────────────────────────────────────────
전체 과정에 걸쳐

7. Orchestration : 오케스트레이션 , airflow(배치 프로세싱)
8. Observability : 그라파나/프로메테우스/ ELK에서는 키바나, opensearh 대시보드등 제품 활용
```

# DATA GENERATION 
- 데이터가 어디서 발생하는가? 
  - 도메인결정 -> 실제 데이터 샘플 획득, 불량률 획득 -> 구성 (프로젝트)
  - 형태
    - [v]최종 형태를 더미로 구성하여 적제
      - 파이썬 코드 작성(faker 패키지 활용)
    - 실제 요청 구성 -> 서비스 통과 -> 결과 -> 적제
      - 최종 프로젝트 형태
  - 방식
    - [v]ECS Fargate + K6(..) -> 클라이언트의 요청 구현
      - 미니 프로젝트 구성
    - AWS Distributed Load Testing -> 대규모 트레픽 발생

- 도메인  
  - 어플리케이션 (웹, 앱)
  - IOT 센서
  - 게임 서버
  - 금융 시스템
  - 데이터베이스
  - ...

- 데이터 발생 (요청 -> 응답)
```
┌──────────────────────────────────────────────────────────────────┐
│                    TRAFFIC GENERATOR                             │
└──────────────────────────────────────────────────────────────────┘

                       ECS Fargate
                            │
                            ▼
                           k6
                            │
                            │ HTTP / HTTPS Request
                            ▼
                       Public ALB
                            │
                            ▼
                    Dummy Service
                      FastAPI
                            │
             ┌──────────────┼──────────────┐
             │              │              │
             ▼              ▼              ▼
        GET Request    POST Request    Error Request
             │              │              │
             └──────────────┼──────────────┘
                            ▼
                       Response
                            │
                            ▼
                           k6
```

- 데이터 트레픽 => 적제 형태
```
┌──────────────────────────────────────────────────────────────────┐
│                    LOG INGESTION                                 │
└──────────────────────────────────────────────────────────────────┘

                     HTTP Request
                          │
                          ▼
                    Dummy Service
                      FastAPI
                          │
                          │
                   Log / Event 생성
                          │
          ┌───────────────┼────────────────┐
          │               │                │
          ▼               ▼                ▼
      Access Log     Business Event     Error Log
          │               │                │
          └───────────────┼────────────────┘
                          │
                          ▼
                   CloudWatch Logs
                          │
                          ▼
                 Amazon Data Firehose : 1분단위, 100MB단위등 기준으로 데이터를 모아서 전달
                          │
                          ▼
                      S3 BRONZE     : 적제 위치 (메달리온 아킥텍처 기준 브론즈:원본데이터형태)
                  ─────────────────
                       Raw Log
                       Raw Event
                       JSON / GZIP
                       Partition
```
- s3 저장 샘플링 - 검색을 위해서 파티션작업(열단위 검색유리->검색속도빠름), 저장형턔(`파케이`,압축,json 않좋음, )
  - s3, Athena(서버리스 형태로 raw에 sql 수행 처리 가능), 
```
S3
└─ bronze/
   ├─ access/
   │   └─ year=2026/month=08/day=11/
   │
   ├─ event/
   │   └─ year=2026/month=08/day=11/
   │
   └─ error/
       └─ year=2026/month=08/day=11/
```
- 전체 구성 확장
```
┌──────────────────────────────────────────────────────────────────────┐
│                       LOG GENERATOR SYSTEM                           │
└──────────────────────────────────────────────────────────────────────┘


                     [ Traffic Generator ]

                        ECS Fargate
                             │
                             ▼
                            k6
                             │
                  HTTP / HTTPS Request
                             │
                             ▼
                        Public ALB
                             │
                             ▼
                       Dummy Service
                         FastAPI
                             │
                 ┌───────────┼───────────┐
                 │           │           │
                 ▼           ▼           ▼
              Access      Business     Error
               Log         Event        Log
                 │           │           │
                 └───────────┼───────────┘
                             │
                             ▼
                      CloudWatch Logs
                             │
                             ▼
                    Amazon Data Firehose
                             │
                             ▼

┌──────────────────────────────────────────────────────────────────────┐
│                         DATA LAKE                                    │
└──────────────────────────────────────────────────────────────────────┘

                          S3 BRONZE
                    ───────────────────

                     Raw Access Log
                     Raw Event Log
                     Raw Error Log
                     JSON / GZIP
                     Date Partition

                             │
                             │
                             ▼

                    다음 Pipeline Chapter

                             │
              ┌──────────────┼───────────────┐
              ▼              ▼               ▼
           Airflow       Kafka/MSK        Kinesis
           Batch         Streaming       Streaming -> Flink/spark streamming 으로 처리
              │
              ▼
       Pandas / Polars / Spark
              │
              ▼
          S3 SILVER
              │
              ▼
           S3 GOLD
```


# DATA INGESTION (발생한 데이터를 어떻게 가져오는가?)
- 수집 방식에 따라 파이프라인 유형 (최초 발생 => 저장)
```
                    DATA INGESTION

                         Source <- 데이터 발생
                           │
        ┌──────────────────┼───────────────────┐────────────┐
        │                  │                   │            │
        ▼                  ▼                   ▼            ▼
 [V]Log / Event      [V]Streaming             CDC          [V]Batch 
 Ingestion             Ingestion           Ingestion     Ingestion
        │                  │                   │            │
        ▼                  ▼                   ▼            ▼
 CloudWatch      Kafka/MSK / Kinesis       DMS / Debezium  CSV/API/DB Export
 Firehose
        │                  │                   │            │
        └──────────────────┼───────────────────┘────────────┘
                           ▼
                       S3 BRONZE <- raw 데이터 저장
```

- Log / Event Ingestion Pipeline
  - 로그, 이벤트를 수집하여 저장
  - 실시간 분석 x, 데이터를 안전하게 쌓는것 목표 => 사용시간은 상이함(당일, 한달후, 주간, 즉시성은 아님)
```
┌─────────────────────────────────────────────────────┐
│          01. LOG / EVENT INGESTION PIPELINE         │
└─────────────────────────────────────────────────────┘

                로그제너레이터
                      │
                      ▼
               CloudWatch Logs
                      │
                      ▼
             Amazon Data Firehose : 시간 혹은 용량 단위 데이터를 묶어서 전송 -> 딜레이 발생
                      │
                      ▼
                 S3 BRONZE
                      │
                 Raw Data 저장
```

- Batch processing Ingestion pipeline
  - 쌓여있는 데이터를 일정 주기로 처리 -> Airflow
```
┌─────────────────────────────────────────────────────┐
│             02. BATCH DATA PIPELINE                 │
└─────────────────────────────────────────────────────┘

                 S3 BRONZE
                      │
                      ▼
                   Airflow : 스케줄 작성 -> DAG 작성 -> 데이터 전처리
                      │
          ┌───────────┼───────────┐
          ▼           ▼           ▼
       Pandas(소)  Polars(중)   Spark(대)   <- 데이터 처리 규모에 따라 분류
    10(3)기가이내    수십기가    수백기가,테라급
          │           │           │
          └───────────┼───────────┘
                      ▼
                 S3 SILVER
                      │
                      ▼
                  S3 GOLD
                      │
                      ▼
              Athena / Redshift
```

- Real-time Streamming Pipeline
- 데이터가 들어오는 동시에 즉시 처리
- 공장 설비/생산시 이상 탐지(감지), 금융 이상 거래 감지, 게임, 실시간 주문량, ...
```
┌─────────────────────────────────────────────────────┐
│          03. REAL-TIME STREAMING PIPELINE           │
└─────────────────────────────────────────────────────┘

                 로그제너레이터
                      │
          ┌───────────┴────────────┐
          ▼                        ▼
  [V] Kafka / MSK          [V] Kinesis Data Streams
          │                        │
          └───────────┬────────────┘
                      ▼
                Spark Streaming
                     또는
                 [V] Flink
                      │
              Window / Aggregate
              Filter / Detection
                      │
          ┌───────────┼────────────┐
          ▼           ▼            ▼
  [v]OpenSearch(ELK, EFK)  S3 Silver   Alert/Event
```

- ETL Pipeline
- Extact Treasfrom Load, 데이터 엔지니어의 전통적인 유형
- 메달리온 아킥텍처 구조에서 사용 -> 각 레이어에서 데이터 형태 설명
```
┌─────────────────────────────────────────────────────┐
│                 04. ETL PIPELINE                    │
└─────────────────────────────────────────────────────┘

             Source / S3 BRONZE
                      │
                  Extract
                      │
                      ▼
              Pandas / Polars
               Spark / Glue (etl, 데이터베이스, 테이블, 크롤러등 활용)
                      │
                  Transform
                      │
                      ▼
                   Load
                      │
                      ▼
                 S3 SILVER
                      │
                      ▼
                  S3 GOLD
```  


- ELT Pipeline
- 금융 프로젝트 활용, 저장후 SQL 변환 처리, 제조활용가능
```
┌─────────────────────────────────────────────────────┐
│                 05. ELT PIPELINE                    │
└─────────────────────────────────────────────────────┘

                 Data Source
                      │
                   Extract
                      │
                      ▼
                     Load
                      │
                      ▼
             S3 / Redshift
                      │
                      ▼
                  SQL Transform
                      │
                      ▼
              Silver / Gold
                      │
                      ▼
             Analytics Mart
```