"""
데이터 변환 (Transformation) 모듈
Bronze → Silver → Gold 레이어 변환

단계:
1. Bronze → Silver: 정제, 스키마 통일, 품질 검증
2. Silver → Gold: 집계, 분석용 마트 생성
"""

import json
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Any, Optional
import pandas as pd

from config import (
    BRONZE_LAYER,
    SILVER_LAYER,
    GOLD_LAYER,
    SERVING_LAYER,
    PIPELINE_CONFIG
)
from warehouse import DataWarehouse


class BronzeToSilver:
    """Bronze → Silver 변환 클래스"""
    
    def __init__(self):
        self.dw = None
    
    def transform_foreigner_population(self) -> pd.DataFrame:
        """외국인 인구 데이터 정제"""
        
        # Bronze에서 원본 데이터 로드
        bronze_path = BRONZE_LAYER / "policy_documents"
        
        # 가장 최신 통계 파일 찾기
        files = list(bronze_path.glob("foreigner_statistics_*.json"))
        if not files:
            print("⚠️ No foreigner statistics found in Bronze")
            return pd.DataFrame()
        
        latest_file = max(files, key=lambda x: x.stat().st_mtime)
        
        with open(latest_file, "r", encoding="utf-8") as f:
            raw_data = json.load(f)
        
        # 데이터 추출 및 정제
        trend_data = raw_data["data"]["ansan_foreigner_trend"]
        
        df = pd.DataFrame(trend_data)
        df["updated_at"] = datetime.now()
        df["source"] = "안산시청/행정안전부"
        df["region"] = "안산시"
        
        # 컬럼 순서 정리
        df = df[["region", "year", "total_population", "foreign_population", 
                 "ratio", "updated_at", "source"]]
        df.columns = ["region", "year", "total_population", "foreign_population",
                      "foreign_ratio", "updated_at", "source"]
        
        print(f"✅ Transformed foreigner_population: {len(df)} rows")
        
        return df
    
    def transform_childcare_support(self) -> pd.DataFrame:
        """보육료 지원 데이터 정제"""
        
        bronze_path = BRONZE_LAYER / "policy_documents"
        files = list(bronze_path.glob("ansan_childcare_policy_*.json"))
        
        if not files:
            print("⚠️ No childcare policy found in Bronze")
            return pd.DataFrame()
        
        latest_file = max(files, key=lambda x: x.stat().st_mtime)
        
        with open(latest_file, "r", encoding="utf-8") as f:
            raw_data = json.load(f)
        
        policy = raw_data["data"]
        support_amounts = policy["support_amounts"]
        
        records = []
        
        # 연령별 지원금
        for age_group, amounts in support_amounts.items():
            records.append({
                "age_group": age_group.replace("age_", "").replace("_", "~") + "세",
                "support_type": "보육료",
                "dobi_amount": amounts["dobi"],
                "sibi_amount": amounts["sibi"],
                "total_amount": amounts["total"],
                "effective_date": "2025-01-01",
                "source": "안산시 정책 문서 3-3-48"
            })
        
        # 미등록 아동 (지원 없음)
        records.append({
            "age_group": "미등록 (0~5세)",
            "support_type": "보육료",
            "dobi_amount": 0,
            "sibi_amount": 0,
            "total_amount": 0,
            "effective_date": "2025-01-01",
            "source": "안산시 정책 문서 3-3-48 (대상 제외)"
        })
        
        df = pd.DataFrame(records)
        
        print(f"✅ Transformed childcare_support: {len(df)} rows")
        
        return df
    
    def transform_unregistered_children(self) -> pd.DataFrame:
        """미등록 아동 추정 데이터 정제"""
        
        bronze_path = BRONZE_LAYER / "policy_documents"
        files = list(bronze_path.glob("foreigner_statistics_*.json"))
        
        if not files:
            return pd.DataFrame()
        
        latest_file = max(files, key=lambda x: x.stat().st_mtime)
        
        with open(latest_file, "r", encoding="utf-8") as f:
            raw_data = json.load(f)
        
        estimates = raw_data["data"]["unregistered_children"]
        
        records = [
            {
                "region": "전국",
                "estimation_type": "법무부 공식",
                "source": "법무부 출입국통계",
                "count_min": estimates["moj_official_2025"],
                "count_max": estimates["moj_official_2025"],
                "estimation_date": "2025-01-01",
                "notes": "19세 이하 미등록 이주아동 (국내출생 미포함)"
            },
            {
                "region": "전국",
                "estimation_type": "시민단체 추정",
                "source": "이주아동권리보장 연대",
                "count_min": estimates["civil_society_low"],
                "count_max": estimates["civil_society_high"],
                "estimation_date": "2025-01-01",
                "notes": "국내출생 포함 추정"
            },
            {
                "region": "안산시",
                "estimation_type": "비율 적용 추정",
                "source": "법무부 × 안산시 외국인 비율 13.2%",
                "count_min": estimates["ansan_estimate_min"],
                "count_max": estimates["ansan_estimate_max"],
                "estimation_date": "2025-01-01",
                "notes": f"중간값: {estimates['ansan_estimate_mid']}명"
            }
        ]
        
        df = pd.DataFrame(records)
        
        print(f"✅ Transformed unregistered_children: {len(df)} rows")
        
        return df
    
    def save_to_silver(self, table_name: str, df: pd.DataFrame):
        """Silver 레이어에 저장"""
        
        if df.empty:
            print(f"⚠️ Empty DataFrame, skipping {table_name}")
            return
        
        # Parquet 저장
        output_dir = SILVER_LAYER / "population"
        output_dir.mkdir(parents=True, exist_ok=True)
        
        parquet_path = output_dir / f"{table_name}.parquet"
        df.to_parquet(parquet_path, index=False)
        
        print(f"💾 Saved to Silver: {parquet_path}")
        
        return parquet_path
    
    def run(self):
        """Bronze → Silver 전체 변환 실행"""
        
        print("\n🔄 Bronze → Silver 변환 시작")
        
        # 외국인 인구 데이터
        fp_df = self.transform_foreigner_population()
        self.save_to_silver("foreigner_population", fp_df)
        
        # 보육료 지원 데이터
        cs_df = self.transform_childcare_support()
        self.save_to_silver("childcare_support", cs_df)
        
        # 미등록 아동 추정
        uc_df = self.transform_unregistered_children()
        self.save_to_silver("unregistered_children", uc_df)
        
        print("✅ Bronze → Silver 변환 완료")
        
        return True


class SilverToGold:
    """Silver → Gold 변환 클래스 (집계/분석)"""
    
    def __init__(self):
        self.silver_path = SILVER_LAYER / "population"
    
    def aggregate_ansan_trend(self) -> pd.DataFrame:
        """안산시 외국인 추이 집계"""
        
        parquet_path = self.silver_path / "foreigner_population.parquet"
        
        if not parquet_path.exists():
            print("⚠️ foreigner_population.parquet not found")
            return pd.DataFrame()
        
        df = pd.read_parquet(parquet_path)
        
        # 안산시 데이터만 필터링
        df = df[df["region"] == "안산시"].copy()
        
        # YoY 성장률 계산
        df = df.sort_values("year")
        df["yoy_growth"] = df["foreign_population"].pct_change() * 100
        df["yoy_growth"] = df["yoy_growth"].fillna(0).round(2)
        
        # 필요한 컬럼만 선택
        result = df[["year", "total_population", "foreign_population", 
                     "foreign_ratio", "yoy_growth"]]
        result.columns = ["year", "total_population", "foreign_population", 
                          "ratio", "yoy_growth"]
        
        print(f"✅ Aggregated ansan_foreigner_trend: {len(result)} rows")
        
        return result
    
    def aggregate_gyeonggi_comparison(self) -> pd.DataFrame:
        """경기도 시군구 비교 집계"""
        
        bronze_path = BRONZE_LAYER / "policy_documents"
        files = list(bronze_path.glob("foreigner_statistics_*.json"))
        
        if not files:
            return pd.DataFrame()
        
        latest_file = max(files, key=lambda x: x.stat().st_mtime)
        
        with open(latest_file, "r", encoding="utf-8") as f:
            raw_data = json.load(f)
        
        comparison_data = raw_data["data"]["gyeonggi_comparison"]
        
        df = pd.DataFrame(comparison_data)
        
        # 순위 계산
        df = df.sort_values("foreign_ratio", ascending=False)
        df["rank"] = range(1, len(df) + 1)
        
        result = df[["region", "foreign_ratio", "foreign_count", 
                     "pilot_program", "rank"]]
        
        print(f"✅ Aggregated gyeonggi_comparison: {len(result)} rows")
        
        return result
    
    def aggregate_support_gap(self) -> pd.DataFrame:
        """지원 격차 분석 집계"""
        
        parquet_path = self.silver_path / "childcare_support.parquet"
        
        if not parquet_path.exists():
            return pd.DataFrame()
        
        df = pd.read_parquet(parquet_path)
        
        # 등록 vs 미등록 비교
        registered_avg = df[~df["age_group"].str.contains("미등록")]["total_amount"].mean()
        unregistered = df[df["age_group"].str.contains("미등록")]["total_amount"].iloc[0]
        
        records = [
            {
                "category": "평균 월 지원금",
                "registered_support": int(registered_avg),
                "unregistered_support": int(unregistered),
                "gap_amount": int(registered_avg - unregistered),
                "gap_percentage": 100.0
            },
            {
                "category": "0~2세 월 지원금",
                "registered_support": 260000,
                "unregistered_support": 0,
                "gap_amount": 260000,
                "gap_percentage": 100.0
            },
            {
                "category": "3~5세 월 지원금",
                "registered_support": 280000,
                "unregistered_support": 0,
                "gap_amount": 280000,
                "gap_percentage": 100.0
            }
        ]
        
        result = pd.DataFrame(records)
        
        print(f"✅ Aggregated support_gap_analysis: {len(result)} rows")
        
        return result
    
    def save_to_gold(self, table_name: str, df: pd.DataFrame):
        """Gold 레이어에 저장"""
        
        if df.empty:
            return
        
        output_dir = GOLD_LAYER / "analytics"
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Parquet 저장
        parquet_path = output_dir / f"{table_name}.parquet"
        df.to_parquet(parquet_path, index=False)
        
        # JSON도 저장 (대시보드용)
        json_path = output_dir / f"{table_name}.json"
        df.to_json(json_path, orient="records", force_ascii=False, indent=2)
        
        print(f"💾 Saved to Gold: {parquet_path}")
        
        return parquet_path
    
    def run(self):
        """Silver → Gold 전체 집계 실행"""
        
        print("\n🔄 Silver → Gold 집계 시작")
        
        # 안산시 외국인 추이
        trend_df = self.aggregate_ansan_trend()
        self.save_to_gold("ansan_foreigner_trend", trend_df)
        
        # 경기도 비교
        comparison_df = self.aggregate_gyeonggi_comparison()
        self.save_to_gold("gyeonggi_comparison", comparison_df)
        
        # 지원 격차 분석
        gap_df = self.aggregate_support_gap()
        self.save_to_gold("support_gap_analysis", gap_df)
        
        print("✅ Silver → Gold 집계 완료")
        
        return True


class ServingLayer:
    """Dashboard Serving 레이어 생성"""
    
    def __init__(self):
        self.gold_path = GOLD_LAYER / "analytics"
        self.serving_path = SERVING_LAYER
    
    def generate_dashboard_data(self):
        """대시보드용 통합 데이터 생성"""
        
        self.serving_path.mkdir(parents=True, exist_ok=True)
        
        # Gold 레이어에서 데이터 로드
        dashboard_data = {}
        
        # 트렌드 데이터
        trend_path = self.gold_path / "ansan_foreigner_trend.json"
        if trend_path.exists():
            with open(trend_path, "r", encoding="utf-8") as f:
                dashboard_data["trend"] = json.load(f)
        
        # 비교 데이터
        comparison_path = self.gold_path / "gyeonggi_comparison.json"
        if comparison_path.exists():
            with open(comparison_path, "r", encoding="utf-8") as f:
                dashboard_data["comparison"] = json.load(f)
        
        # 지원 격차 분석
        gap_path = self.gold_path / "support_gap_analysis.json"
        if gap_path.exists():
            with open(gap_path, "r", encoding="utf-8") as f:
                dashboard_data["gap_analysis"] = json.load(f)
        
        # 기존 데이터와 병합
        existing_chart_data_path = self.serving_path / "chart_data.json"
        if existing_chart_data_path.exists():
            with open(existing_chart_data_path, "r", encoding="utf-8") as f:
                existing_data = json.load(f)
            
            # 기존 데이터 유지하면서 새 데이터 추가
            for key, value in existing_data.items():
                if key not in dashboard_data:
                    dashboard_data[key] = value
        
        # 통합 데이터 저장
        output_path = self.serving_path / "chart_data.json"
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(dashboard_data, f, ensure_ascii=False, indent=2)
        
        print(f"✅ Generated dashboard data: {output_path}")
        
        # 메타데이터 저장
        metadata = {
            "generated_at": datetime.now().isoformat(),
            "data_sources": ["gold.ansan_foreigner_trend", "gold.gyeonggi_comparison", 
                             "gold.support_gap_analysis"],
            "total_keys": len(dashboard_data)
        }
        
        metadata_path = self.serving_path / "metadata.json"
        with open(metadata_path, "w", encoding="utf-8") as f:
            json.dump(metadata, f, ensure_ascii=False, indent=2)
        
        return output_path
    
    def run(self):
        """Serving 레이어 생성 실행"""
        
        print("\n🔄 Serving Layer 생성 시작")
        
        self.generate_dashboard_data()
        
        print("✅ Serving Layer 생성 완료")
        
        return True


def run_full_transformation():
    """전체 변환 파이프라인 실행"""
    
    print("=" * 60)
    print("🔄 데이터 변환 파이프라인 시작")
    print("=" * 60)
    
    # Bronze → Silver
    bronze_to_silver = BronzeToSilver()
    bronze_to_silver.run()
    
    # Silver → Gold
    silver_to_gold = SilverToGold()
    silver_to_gold.run()
    
    # Serving Layer
    serving = ServingLayer()
    serving.run()
    
    print("\n" + "=" * 60)
    print("✅ 데이터 변환 파이프라인 완료")
    print("=" * 60)
    
    return True


if __name__ == "__main__":
    run_full_transformation()
