"""
안산시 외국인 보육료 정책 감사 - 데이터 파이프라인 설정
Medallion Architecture (Bronze-Silver-Gold) 기반

기술 스택:
- DuckDB: 로컬 OLAP 데이터 웨어하우스
- Python: ETL 오케스트레이션
- Parquet: 데이터 레이크 포맷
- JSON: Dashboard Serving 레이어
"""

import os
from pathlib import Path
from datetime import datetime

# =============================================
# 프로젝트 루트 설정
# =============================================
PROJECT_ROOT = Path(__file__).parent.absolute()

# =============================================
# 데이터 레이어 경로 (Medallion Architecture)
# =============================================
DATA_LAKE_ROOT = PROJECT_ROOT / "data_lake"

# Bronze Layer: 원본 데이터 (Raw, Immutable)
BRONZE_LAYER = DATA_LAKE_ROOT / "bronze"

# Silver Layer: 정제된 데이터 (Cleaned, Validated)
SILVER_LAYER = DATA_LAKE_ROOT / "silver"

# Gold Layer: 집계/분석용 데이터 (Aggregated, Analytics-Ready)
GOLD_LAYER = DATA_LAKE_ROOT / "gold"

# Serving Layer: Dashboard용 데이터
SERVING_LAYER = PROJECT_ROOT / "dashboard" / "data"

# =============================================
# DuckDB 데이터 웨어하우스 설정
# =============================================
WAREHOUSE_DB_PATH = PROJECT_ROOT / "warehouse.duckdb"

# =============================================
# 데이터 소스 설정
# =============================================
DATA_SOURCES = {
    "kosis": {
        "name": "KOSIS 국가통계포털",
        "base_url": "https://kosis.kr/openapi",
        "api_key": "YmI2OGI0NGFhMzkzZjIyODVlMjI2NDI2MDI1YjFkZjc=",
        "tables": {
            "foreigner_population": {
                "org_id": "110",  # 행정안전부
                "description": "외국인주민 현황"
            }
        }
    },
    "policy_documents": {
        "name": "안산시 정책 문서",
        "source_type": "manual",
        "description": "공식 정책 문서에서 추출한 데이터"
    },
    "news_articles": {
        "name": "언론 보도",
        "source_type": "manual",
        "description": "언론 보도에서 추출한 데이터"
    }
}

# =============================================
# 스키마 정의
# =============================================
SCHEMAS = {
    "bronze": {
        "raw_kosis_data": """
            source_id VARCHAR,
            table_id VARCHAR,
            raw_data JSON,
            ingested_at TIMESTAMP,
            source_url VARCHAR
        """,
        "raw_policy_data": """
            document_id VARCHAR,
            document_name VARCHAR,
            raw_content TEXT,
            extracted_at TIMESTAMP,
            source_path VARCHAR
        """
    },
    "silver": {
        "foreigner_population": """
            region VARCHAR,
            year INTEGER,
            total_population INTEGER,
            foreign_population INTEGER,
            foreign_ratio DOUBLE,
            updated_at TIMESTAMP,
            source VARCHAR
        """,
        "childcare_support": """
            age_group VARCHAR,
            support_type VARCHAR,
            dobi_amount INTEGER,
            sibi_amount INTEGER,
            total_amount INTEGER,
            effective_date DATE,
            source VARCHAR
        """,
        "unregistered_children": """
            region VARCHAR,
            estimation_type VARCHAR,
            source VARCHAR,
            count_min INTEGER,
            count_max INTEGER,
            estimation_date DATE,
            notes TEXT
        """
    },
    "gold": {
        "ansan_foreigner_trend": """
            year INTEGER PRIMARY KEY,
            total_population INTEGER,
            foreign_population INTEGER,
            ratio DOUBLE,
            yoy_growth DOUBLE
        """,
        "gyeonggi_comparison": """
            region VARCHAR PRIMARY KEY,
            foreign_ratio DOUBLE,
            foreign_count INTEGER,
            pilot_program BOOLEAN,
            rank INTEGER
        """,
        "support_gap_analysis": """
            category VARCHAR,
            registered_support INTEGER,
            unregistered_support INTEGER,
            gap_amount INTEGER,
            gap_percentage DOUBLE
        """
    }
}

# =============================================
# 파이프라인 설정
# =============================================
PIPELINE_CONFIG = {
    "schedule": {
        "ingestion": "daily",  # 데이터 수집 주기
        "transformation": "daily",  # 변환 주기
        "serving": "on_change"  # 변경 시 즉시
    },
    "retention": {
        "bronze": 365,  # 원본 데이터 1년 보관
        "silver": 180,  # 정제 데이터 6개월
        "gold": 90  # 집계 데이터 3개월
    },
    "quality_checks": {
        "null_threshold": 0.1,  # 10% 이상 NULL 시 경고
        "duplicate_check": True,
        "schema_validation": True
    }
}

# =============================================
# 디렉토리 초기화 함수
# =============================================
def init_data_lake():
    """데이터 레이크 디렉토리 구조 생성"""
    directories = [
        BRONZE_LAYER / "kosis",
        BRONZE_LAYER / "policy_documents",
        BRONZE_LAYER / "news_articles",
        SILVER_LAYER / "population",
        SILVER_LAYER / "childcare",
        SILVER_LAYER / "estimates",
        GOLD_LAYER / "analytics",
        GOLD_LAYER / "dashboard",
        SERVING_LAYER
    ]
    
    for directory in directories:
        directory.mkdir(parents=True, exist_ok=True)
        print(f"✅ Created: {directory}")
    
    return True


def get_config():
    """설정 정보 반환"""
    return {
        "project_root": str(PROJECT_ROOT),
        "data_lake_root": str(DATA_LAKE_ROOT),
        "warehouse_db": str(WAREHOUSE_DB_PATH),
        "layers": {
            "bronze": str(BRONZE_LAYER),
            "silver": str(SILVER_LAYER),
            "gold": str(GOLD_LAYER),
            "serving": str(SERVING_LAYER)
        },
        "data_sources": DATA_SOURCES,
        "schemas": SCHEMAS,
        "pipeline": PIPELINE_CONFIG
    }


if __name__ == "__main__":
    print("=" * 60)
    print("📊 데이터 파이프라인 설정 초기화")
    print("=" * 60)
    
    init_data_lake()
    
    config = get_config()
    print("\n📁 데이터 레이크 구조:")
    for layer, path in config["layers"].items():
        print(f"  {layer.upper()}: {path}")
    
    print("\n🔗 데이터 소스:")
    for source_id, source_info in config["data_sources"].items():
        print(f"  - {source_info['name']}")
    
    print("\n✅ 설정 초기화 완료")
