# 📊 안산시 외국인 보육료 정책 감사 - 데이터 파이프라인

## 🏗️ 아키텍처 개요

Databricks의 **Medallion Architecture** (Bronze-Silver-Gold) 패턴을 채택하여 데이터 품질과 신뢰성을 보장합니다.

```
┌──────────────────────────────────────────────────────────────────────┐
│                          DATA PIPELINE                                │
├──────────────────────────────────────────────────────────────────────┤
│                                                                       │
│  ┌─────────────┐    ┌─────────────┐    ┌─────────────┐               │
│  │   SOURCES   │    │   SOURCES   │    │   SOURCES   │               │
│  │  KOSIS API  │    │ 정책 문서   │    │ 언론 보도   │               │
│  └──────┬──────┘    └──────┬──────┘    └──────┬──────┘               │
│         │                  │                  │                       │
│         └──────────────────┼──────────────────┘                       │
│                            ▼                                          │
│  ┌─────────────────────────────────────────────────────────────┐     │
│  │                    BRONZE LAYER (Raw)                        │     │
│  │  • 원본 데이터 그대로 저장 (Immutable)                       │     │
│  │  • Parquet + JSON 포맷                                       │     │
│  │  • 수집 메타데이터 포함                                      │     │
│  └──────────────────────────────┬──────────────────────────────┘     │
│                                 │ Transform                           │
│                                 ▼                                     │
│  ┌─────────────────────────────────────────────────────────────┐     │
│  │                   SILVER LAYER (Cleaned)                     │     │
│  │  • 데이터 정제 및 스키마 통일                                │     │
│  │  • 품질 검증 (NULL, 중복 체크)                               │     │
│  │  • 비즈니스 규칙 적용                                        │     │
│  └──────────────────────────────┬──────────────────────────────┘     │
│                                 │ Aggregate                           │
│                                 ▼                                     │
│  ┌─────────────────────────────────────────────────────────────┐     │
│  │                    GOLD LAYER (Analytics)                    │     │
│  │  • 분석용 집계 데이터                                        │     │
│  │  • 대시보드 마트                                             │     │
│  │  • KPI 계산                                                  │     │
│  └──────────────────────────────┬──────────────────────────────┘     │
│                                 │ Serve                               │
│                                 ▼                                     │
│  ┌─────────────────────────────────────────────────────────────┐     │
│  │                   SERVING LAYER (Dashboard)                  │     │
│  │  • Chart.js 호환 JSON                                        │     │
│  │  • 실시간 API (향후)                                         │     │
│  └─────────────────────────────────────────────────────────────┘     │
│                                                                       │
└──────────────────────────────────────────────────────────────────────┘
```

## 📁 디렉토리 구조

```
pipeline/
├── __init__.py           # 패키지 초기화
├── config.py             # 파이프라인 설정 (경로, 스키마, 소스)
├── warehouse.py          # DuckDB 데이터 웨어하우스 관리
├── ingestion.py          # Bronze 레이어 데이터 수집
├── transformation.py     # Silver/Gold 레이어 변환
├── orchestrator.py       # 전체 파이프라인 오케스트레이션
├── warehouse.duckdb      # DuckDB 데이터베이스 파일
│
└── data_lake/            # 데이터 레이크 (파일 기반)
    ├── bronze/           # 원본 데이터
    │   ├── kosis/        # KOSIS API 데이터
    │   ├── policy_documents/  # 정책 문서
    │   └── news_articles/     # 언론 보도
    │
    ├── silver/           # 정제된 데이터
    │   ├── population/   # 인구 통계
    │   ├── childcare/    # 보육 정책
    │   └── estimates/    # 추정 데이터
    │
    └── gold/             # 분석용 집계 데이터
        ├── analytics/    # 분석 마트
        └── dashboard/    # 대시보드용 데이터
```

## 🛠️ 기술 스택

| 레이어 | 기술 | 용도 |
|--------|------|------|
| **Storage** | Parquet | 컬럼 기반 분석 최적화 |
| **Storage** | JSON | 메타데이터, 대시보드 서빙 |
| **Warehouse** | DuckDB | 로컬 OLAP 데이터 웨어하우스 |
| **ETL** | Python + Pandas | 데이터 변환 |
| **Orchestration** | Python CLI | 파이프라인 실행 관리 |

## 🚀 사용 방법

### 전체 파이프라인 실행
```bash
cd pipeline
python orchestrator.py run
```

### 단계별 실행
```bash
# 초기화만
python orchestrator.py init

# 데이터 수집만 (Bronze)
python orchestrator.py ingest

# 변환만 (Silver/Gold)
python orchestrator.py transform

# 대시보드 데이터 생성만
python orchestrator.py serve
```

### 상태 확인
```bash
python orchestrator.py status
```

## 📊 데이터 테이블

### Bronze Layer (원본)
| 테이블 | 설명 |
|--------|------|
| `raw_kosis_data` | KOSIS API 원본 응답 |
| `raw_policy_data` | 정책 문서 추출 데이터 |

### Silver Layer (정제)
| 테이블 | 설명 |
|--------|------|
| `foreigner_population` | 외국인 인구 현황 (연도별) |
| `childcare_support` | 보육료 지원 금액 |
| `unregistered_children` | 미등록 아동 추정치 |

### Gold Layer (집계)
| 테이블 | 설명 |
|--------|------|
| `ansan_foreigner_trend` | 안산시 외국인 추이 (YoY 포함) |
| `gyeonggi_comparison` | 경기도 시군구 비교 |
| `support_gap_analysis` | 지원 격차 분석 |

## 🔗 데이터 소스

1. **KOSIS API** (국가통계포털)
   - 행정안전부 외국인주민 현황
   - API Key: 설정 파일 참조

2. **안산시 정책 문서**
   - 문서번호: 3-3-48
   - 외국인 보육료 지원 정책

3. **언론 보도**
   - 경인일보, 동아일보, 연합뉴스 등

## 📈 확장 계획

- [ ] Apache Airflow 기반 스케줄링
- [ ] 실시간 API 서빙 레이어
- [ ] 데이터 품질 대시보드
- [ ] 증분 수집 (Incremental Ingestion)
- [ ] CDC (Change Data Capture) 지원

---

**작성일**: 2026-01-15  
**버전**: 1.0.0
