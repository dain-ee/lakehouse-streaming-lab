# Lakehouse Streaming Lab

Kafka + Spark Structured Streaming + Iceberg 기반의  
실시간 Lakehouse 파이프라인 실습 프로젝트입니다.

단순 기능 테스트 수준이 아니라,  
실제 데이터 플랫폼 운영 환경과 유사한 구조를 목표로 구성했습니다.

---

# 프로젝트 목적

기존 Kafka 운영 및 구축 경험을 기반으로  
다음 영역까지 확장하는 것을 목표로 합니다.

- 실시간 데이터 파이프라인 구축
- Streaming ETL 처리
- Iceberg 기반 Lakehouse 아키텍처 이해
- AWS Glue Catalog / Athena 연동
- Spark Structured Streaming 운영 경험
- 데이터 적재 및 상태(Current) 테이블 관리
- 모니터링 및 장애 분석

---

# 아키텍처

```text
Kafka Topic (orders_raw)
        ↓
Spark Structured Streaming
        ↓
Iceberg Bronze Table (orders_bronze)
        ↓
Current Merge Processing
        ↓
Iceberg Current Table (orders_current)
        ↓
Athena Query
```

---

# 사용 기술 스택

| 영역 | 기술 |
|---|---|
| Messaging | Apache Kafka 3.9.2 |
| Streaming | Apache Spark 3.5.8 |
| Table Format | Apache Iceberg 1.10.1 |
| Catalog | AWS Glue Catalog |
| Storage | Amazon S3 |
| Query Engine | Amazon Athena |
| Monitoring | Spark History Server, Kafka UI |

---

# 디렉토리 구조

```text
jobs/
 ├── orders_raw_to_bronze_current.py
 └── orders_metrics_5m.py

scripts/
 ├── start-kafka.sh
 ├── run-orders-bronze-current.sh
 └── run-orders-metrics-5m.sh

sql/
 ├── create_orders_tables.sql
 └── create_metrics_table.sql
```

---

# 테이블 구성

## Bronze Layer

### orders_bronze

Kafka 원본 이벤트를 저장하는 Append-Only 테이블

특징:

- 이벤트 원본 보존
- CDC/Event Sourcing 형태
- 재처리 가능

---

## Current Layer

### orders_current

주문의 최신 상태(Current State)를 유지하는 테이블

특징:

- MERGE 기반 상태 관리
- 최신 event_time 기준 업데이트
- Athena 조회 최적화

---

# 실행 방법

## Kafka 시작

```bash
./scripts/start-kafka.sh
```

## Bronze Pipeline 실행

```bash
./scripts/run-orders-bronze-current.sh
```

## Metrics Pipeline 실행

```bash
./scripts/run-orders-metrics-5m.sh
```

---

## Producer Application

`producer/src/order_event_producer.py`는 주문 라이프사이클 이벤트를 Kafka `orders_raw` 토픽으로 발행하는 테스트용 Producer Application입니다.

생성 이벤트:

- ORDER_CREATED
- PAYMENT_COMPLETED
- SHIPMENT_STARTED
- SHIPMENT_DELIVERED

실행 방법:

```bash
pip3 install -r producer/requirements.txt
python3 producer/src/order_event_producer.py
```

---

# 모니터링

현재 아래 구성으로 모니터링 중입니다.

- Spark History Server
- Spark Structured Streaming UI
- Kafka UI

---

# 향후 확장 계획

- Payment / Shipment Topic 추가
- DLQ 처리
- Schema Evolution 테스트
- Airflow 기반 스케줄링
- 데이터 품질 검증
- Iceberg Compaction 자동화
- Dev / Prod 환경 분리
- CI/CD 구성
