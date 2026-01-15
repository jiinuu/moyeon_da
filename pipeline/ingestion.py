"""
데이터 수집 (Ingestion) 모듈
Bronze Layer로 원본 데이터 수집

데이터 소스:
- KOSIS API: 국가통계포털
- 정책 문서: 수동 추출
- 언론 보도: 수동 추출
"""

import requests
import json
import re
import time
from pathlib import Path
from datetime import datetime
from typing import Optional, Dict, List, Any
import pandas as pd

from config import (
    BRONZE_LAYER,
    DATA_SOURCES,
    init_data_lake
)
from warehouse import DataWarehouse


class KOSISIngestion:
    """KOSIS API 데이터 수집 클래스"""
    
    def __init__(self):
        self.config = DATA_SOURCES["kosis"]
        self.api_key = self.config["api_key"]
        self.base_url = self.config["base_url"]
    
    def _clean_json(self, raw_text: str) -> Optional[Dict]:
        """KOSIS JSON 응답 정리 (따옴표 없는 키 처리)"""
        try:
            # 따옴표 없는 키에 따옴표 추가
            fixed_text = re.sub(
                r'([{,])\s*([a-zA-Z_][a-zA-Z0-9_]*)\s*:', 
                r'\1"\2":', 
                raw_text
            )
            return json.loads(fixed_text)
        except:
            return None
    
    def fetch_table(self, org_id: str, tbl_id: str, 
                    itm_id: str = "ALL", obj_l1: str = "ALL",
                    prd_cnt: int = 10) -> Optional[pd.DataFrame]:
        """KOSIS 통계표 데이터 수집"""
        
        endpoint = f"{self.base_url}/Param/statisticsParameterData.do"
        
        params = {
            "method": "getList",
            "apiKey": self.api_key,
            "itmId": itm_id,
            "objL1": obj_l1,
            "format": "json",
            "jsonVD": "Y",
            "prdSe": "Y",
            "newEstPrdCnt": str(prd_cnt),
            "orgId": org_id,
            "tblId": tbl_id
        }
        
        try:
            query_string = "&".join([f"{k}={v}" for k, v in params.items()])
            full_url = f"{endpoint}?{query_string}"
            
            print(f"📡 Fetching: {tbl_id}")
            
            response = requests.get(full_url, timeout=30)
            data = self._clean_json(response.text)
            
            if data and isinstance(data, list):
                df = pd.DataFrame(data)
                df.columns = [col.upper() for col in df.columns]
                print(f"✅ Fetched {len(df)} rows from {tbl_id}")
                return df
            elif data and 'err' in str(data):
                print(f"⚠️ API Error: {data}")
                return None
            else:
                return None
                
        except Exception as e:
            print(f"❌ Fetch failed: {e}")
            return None
    
    def save_to_bronze(self, tbl_id: str, data: pd.DataFrame, 
                       source_url: str = "") -> Path:
        """Bronze 레이어에 원본 데이터 저장 (Parquet + JSON)"""
        
        output_dir = BRONZE_LAYER / "kosis"
        output_dir.mkdir(parents=True, exist_ok=True)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Parquet 저장 (분석용)
        parquet_path = output_dir / f"{tbl_id}_{timestamp}.parquet"
        data.to_parquet(parquet_path, index=False)
        
        # JSON 저장 (메타데이터 포함)
        json_path = output_dir / f"{tbl_id}_{timestamp}.json"
        metadata = {
            "table_id": tbl_id,
            "ingested_at": datetime.now().isoformat(),
            "source_url": source_url,
            "row_count": len(data),
            "columns": list(data.columns),
            "data": data.to_dict(orient="records")
        }
        
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(metadata, f, ensure_ascii=False, indent=2)
        
        print(f"💾 Saved to Bronze: {parquet_path.name}")
        
        return parquet_path
    
    def ingest_table(self, org_id: str, tbl_id: str, **kwargs) -> bool:
        """단일 테이블 수집 파이프라인"""
        
        # 데이터 수집
        df = self.fetch_table(org_id, tbl_id, **kwargs)
        
        if df is None or len(df) == 0:
            return False
        
        # Bronze 저장
        self.save_to_bronze(tbl_id, df)
        
        # 데이터 웨어하우스에도 저장
        with DataWarehouse() as dw:
            dw.insert_bronze_data("raw_kosis_data", {
                "source_id": f"kosis_{tbl_id}",
                "table_id": tbl_id,
                "raw_data": df.to_dict(orient="records"),
                "source_url": f"{self.base_url}/Param/statisticsParameterData.do"
            })
            
            dw.log_pipeline_run(
                pipeline_name=f"kosis_ingestion_{tbl_id}",
                layer="bronze",
                status="success",
                rows_processed=len(df)
            )
        
        return True


class PolicyDocumentIngestion:
    """정책 문서 데이터 수집 클래스"""
    
    def __init__(self):
        self.config = DATA_SOURCES["policy_documents"]
    
    def ingest_manual_data(self, document_id: str, document_name: str,
                           data: Dict[str, Any], source_path: str = "") -> bool:
        """수동 추출 데이터 수집"""
        
        output_dir = BRONZE_LAYER / "policy_documents"
        output_dir.mkdir(parents=True, exist_ok=True)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # JSON 저장
        json_path = output_dir / f"{document_id}_{timestamp}.json"
        metadata = {
            "document_id": document_id,
            "document_name": document_name,
            "extracted_at": datetime.now().isoformat(),
            "source_path": source_path,
            "data": data
        }
        
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(metadata, f, ensure_ascii=False, indent=2)
        
        print(f"💾 Saved policy document to Bronze: {json_path.name}")
        
        # 데이터 웨어하우스에도 저장
        with DataWarehouse() as dw:
            dw.insert_bronze_data("raw_policy_data", {
                "document_id": document_id,
                "document_name": document_name,
                "raw_content": json.dumps(data, ensure_ascii=False),
                "source_path": source_path
            })
            
            dw.log_pipeline_run(
                pipeline_name=f"policy_ingestion_{document_id}",
                layer="bronze",
                status="success",
                rows_processed=1
            )
        
        return True


def ingest_ansan_policy_data():
    """안산시 정책 문서 데이터 수집"""
    
    ingestion = PolicyDocumentIngestion()
    
    # 정책 문서에서 추출한 데이터
    policy_data = {
        "policy_name": "외국인가정의 안전한 보육환경 조성",
        "document_number": "3-3-48",
        "department": "안산시 여성보육과",
        "contact": "031-481-3323",
        
        "support_target": "관내 어린이집 재원 등록외국인 아동 (0~5세)",
        "residence_requirement": "아동+보호자(1명) 경기도 및 안산시 90일 초과 거주",
        
        "support_amounts": {
            "age_0_2": {"dobi": 100000, "sibi": 160000, "total": 260000},
            "age_3_5": {"dobi": 100000, "sibi": 180000, "total": 280000}
        },
        
        "extended_care": {
            "age_0": 3000,
            "age_1_2": 2000,
            "age_3_5": 1000
        },
        
        "performance_2024": {
            "childcare_recipients": 2144,
            "extended_care_recipients": 1434,
            "childcare_spent": 3938000000,
            "extended_care_spent": 188000000,
            "reference_date": "2024-08-31"
        },
        
        "budget_2025": {
            "total": 7284000000,
            "dobi": 1056000000,
            "sibi": 6228000000
        }
    }
    
    ingestion.ingest_manual_data(
        document_id="ansan_childcare_policy_2025",
        document_name="외국인 보육료 지원(안산시 정책).pdf",
        data=policy_data,
        source_path="외국인 보육료 지원(안산시 정책).pdf"
    )
    
    return True


def ingest_foreigner_statistics():
    """외국인 현황 통계 데이터 수집"""
    
    ingestion = PolicyDocumentIngestion()
    
    # 외국인 현황 데이터 (각종 출처에서 수집)
    statistics_data = {
        "ansan_foreigner_trend": [
            {"year": 2018, "total_population": 705000, "foreign_population": 78500, "ratio": 11.1},
            {"year": 2019, "total_population": 710000, "foreign_population": 82000, "ratio": 11.5},
            {"year": 2020, "total_population": 715000, "foreign_population": 79000, "ratio": 11.0},
            {"year": 2021, "total_population": 718000, "foreign_population": 85000, "ratio": 11.8},
            {"year": 2022, "total_population": 722000, "foreign_population": 90000, "ratio": 12.5},
            {"year": 2023, "total_population": 726000, "foreign_population": 93500, "ratio": 12.9},
            {"year": 2024, "total_population": 730000, "foreign_population": 96300, "ratio": 13.2}
        ],
        
        "gyeonggi_comparison": [
            {"region": "안산시", "foreign_ratio": 13.2, "foreign_count": 96300, "pilot_program": False},
            {"region": "시흥시", "foreign_ratio": 10.1, "foreign_count": 47500, "pilot_program": False},
            {"region": "화성시", "foreign_ratio": 7.0, "foreign_count": 63000, "pilot_program": True},
            {"region": "수원시", "foreign_ratio": 4.8, "foreign_count": 58000, "pilot_program": False},
            {"region": "안성시", "foreign_ratio": 5.0, "foreign_count": 9500, "pilot_program": True},
            {"region": "이천시", "foreign_ratio": 4.0, "foreign_count": 8800, "pilot_program": True}
        ],
        
        "wongok_multicultural": {
            "total_residents": 20191,
            "foreign_residents": 18014,
            "korean_residents": 2177,
            "foreign_ratio": 89.2,
            "wongok_elementary": {
                "total_students": 449,
                "immigrant_background": 443,
                "ratio": 98.6
            }
        },
        
        "unregistered_children": {
            "moj_official_2025": 6169,
            "civil_society_low": 10000,
            "civil_society_high": 20000,
            "ansan_estimate_min": 814,
            "ansan_estimate_max": 2640,
            "ansan_estimate_mid": 1700
        },
        
        "sources": [
            {"name": "안산시청", "date": "2024-01", "type": "official"},
            {"name": "법무부 출입국통계", "date": "2025-01", "type": "official"},
            {"name": "경기도청 보도자료", "date": "2025-12", "type": "official"},
            {"name": "경인일보", "date": "2024-01", "type": "news"},
            {"name": "동아일보", "date": "2024-01", "type": "news"}
        ]
    }
    
    ingestion.ingest_manual_data(
        document_id="foreigner_statistics_2024",
        document_name="외국인 현황 통계 종합",
        data=statistics_data,
        source_path="multiple_sources"
    )
    
    return True


def run_full_ingestion():
    """전체 데이터 수집 파이프라인 실행"""
    
    print("=" * 60)
    print("📥 Bronze Layer 데이터 수집 시작")
    print("=" * 60)
    
    # 데이터 레이크 초기화
    init_data_lake()
    
    # 정책 문서 데이터 수집
    print("\n📄 정책 문서 데이터 수집...")
    ingest_ansan_policy_data()
    
    # 외국인 현황 통계 수집
    print("\n📊 외국인 현황 통계 수집...")
    ingest_foreigner_statistics()
    
    print("\n" + "=" * 60)
    print("✅ Bronze Layer 데이터 수집 완료")
    print("=" * 60)
    
    return True


if __name__ == "__main__":
    run_full_ingestion()
