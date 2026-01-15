"""
데이터 웨어하우스 관리 모듈
DuckDB 기반 OLAP 데이터 웨어하우스

기능:
- 테이블 생성/관리
- 데이터 적재
- 쿼리 실행
- 메타데이터 관리
"""

import duckdb
from pathlib import Path
from datetime import datetime
from typing import Optional, Dict, List, Any
import json

from config import (
    WAREHOUSE_DB_PATH,
    SCHEMAS,
    BRONZE_LAYER,
    SILVER_LAYER,
    GOLD_LAYER
)


class DataWarehouse:
    """DuckDB 기반 데이터 웨어하우스 관리 클래스"""
    
    def __init__(self, db_path: Optional[Path] = None):
        self.db_path = db_path or WAREHOUSE_DB_PATH
        self.conn = None
    
    def connect(self):
        """데이터베이스 연결"""
        self.conn = duckdb.connect(str(self.db_path))
        print(f"✅ Connected to: {self.db_path}")
        return self
    
    def disconnect(self):
        """연결 종료"""
        if self.conn:
            self.conn.close()
            self.conn = None
            print("🔌 Disconnected from database")
    
    def __enter__(self):
        return self.connect()
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.disconnect()
    
    # =============================================
    # 스키마 및 테이블 관리
    # =============================================
    
    def init_schemas(self):
        """모든 스키마(레이어) 초기화"""
        layers = ['bronze', 'silver', 'gold']
        
        for layer in layers:
            self.conn.execute(f"CREATE SCHEMA IF NOT EXISTS {layer}")
            print(f"✅ Schema created: {layer}")
        
        # 메타데이터 스키마
        self.conn.execute("CREATE SCHEMA IF NOT EXISTS metadata")
        
        # 파이프라인 실행 로그 테이블
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS metadata.pipeline_runs (
                run_id VARCHAR PRIMARY KEY,
                pipeline_name VARCHAR,
                layer VARCHAR,
                started_at TIMESTAMP,
                completed_at TIMESTAMP,
                status VARCHAR,
                rows_processed INTEGER,
                error_message TEXT
            )
        """)
        
        # 데이터 품질 로그 테이블
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS metadata.quality_checks (
                check_id VARCHAR PRIMARY KEY,
                table_name VARCHAR,
                check_type VARCHAR,
                check_result VARCHAR,
                details JSON,
                checked_at TIMESTAMP
            )
        """)
        
        print("✅ Metadata tables created")
        return self
    
    def create_table(self, layer: str, table_name: str, schema: str):
        """테이블 생성"""
        full_table_name = f"{layer}.{table_name}"
        
        self.conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {full_table_name} (
                {schema}
            )
        """)
        print(f"✅ Table created: {full_table_name}")
        return self
    
    def init_all_tables(self):
        """모든 테이블 초기화"""
        for layer, tables in SCHEMAS.items():
            for table_name, schema in tables.items():
                self.create_table(layer, table_name, schema)
        
        return self
    
    # =============================================
    # 데이터 적재 (Loading)
    # =============================================
    
    def insert_bronze_data(self, table_name: str, data: Dict[str, Any]):
        """Bronze 레이어에 원본 데이터 적재"""
        import uuid
        
        if table_name == "raw_kosis_data":
            self.conn.execute("""
                INSERT INTO bronze.raw_kosis_data 
                (source_id, table_id, raw_data, ingested_at, source_url)
                VALUES (?, ?, ?, ?, ?)
            """, [
                data.get("source_id", str(uuid.uuid4())),
                data.get("table_id"),
                json.dumps(data.get("raw_data", {})),
                datetime.now(),
                data.get("source_url", "")
            ])
        
        elif table_name == "raw_policy_data":
            self.conn.execute("""
                INSERT INTO bronze.raw_policy_data
                (document_id, document_name, raw_content, extracted_at, source_path)
                VALUES (?, ?, ?, ?, ?)
            """, [
                data.get("document_id", str(uuid.uuid4())),
                data.get("document_name"),
                data.get("raw_content", ""),
                datetime.now(),
                data.get("source_path", "")
            ])
        
        return self
    
    def insert_silver_data(self, table_name: str, records: List[Dict]):
        """Silver 레이어에 정제된 데이터 적재"""
        import pandas as pd
        
        df = pd.DataFrame(records)
        
        # 기존 데이터 삭제 후 적재 (UPSERT)
        self.conn.execute(f"DELETE FROM silver.{table_name}")
        self.conn.execute(f"INSERT INTO silver.{table_name} SELECT * FROM df")
        
        print(f"✅ Inserted {len(records)} rows into silver.{table_name}")
        return self
    
    def insert_gold_data(self, table_name: str, records: List[Dict]):
        """Gold 레이어에 집계 데이터 적재"""
        import pandas as pd
        
        df = pd.DataFrame(records)
        
        # 기존 데이터 삭제 후 적재
        self.conn.execute(f"DELETE FROM gold.{table_name}")
        self.conn.execute(f"INSERT INTO gold.{table_name} SELECT * FROM df")
        
        print(f"✅ Inserted {len(records)} rows into gold.{table_name}")
        return self
    
    # =============================================
    # 쿼리 실행
    # =============================================
    
    def query(self, sql: str):
        """SQL 쿼리 실행 및 DataFrame 반환"""
        return self.conn.execute(sql).df()
    
    def execute(self, sql: str):
        """SQL 실행 (반환값 없음)"""
        self.conn.execute(sql)
        return self
    
    # =============================================
    # 메타데이터 관리
    # =============================================
    
    def log_pipeline_run(self, pipeline_name: str, layer: str, 
                         status: str, rows_processed: int = 0, 
                         error_message: str = None):
        """파이프라인 실행 로그 기록"""
        import uuid
        
        self.conn.execute("""
            INSERT INTO metadata.pipeline_runs
            (run_id, pipeline_name, layer, started_at, completed_at, 
             status, rows_processed, error_message)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, [
            str(uuid.uuid4()),
            pipeline_name,
            layer,
            datetime.now(),
            datetime.now(),
            status,
            rows_processed,
            error_message
        ])
        return self
    
    def log_quality_check(self, table_name: str, check_type: str,
                          check_result: str, details: Dict = None):
        """데이터 품질 검사 로그 기록"""
        import uuid
        
        self.conn.execute("""
            INSERT INTO metadata.quality_checks
            (check_id, table_name, check_type, check_result, details, checked_at)
            VALUES (?, ?, ?, ?, ?, ?)
        """, [
            str(uuid.uuid4()),
            table_name,
            check_type,
            check_result,
            json.dumps(details or {}),
            datetime.now()
        ])
        return self
    
    # =============================================
    # 유틸리티
    # =============================================
    
    def get_table_stats(self, full_table_name: str) -> Dict:
        """테이블 통계 조회"""
        try:
            count = self.conn.execute(
                f"SELECT COUNT(*) FROM {full_table_name}"
            ).fetchone()[0]
            
            return {
                "table": full_table_name,
                "row_count": count
            }
        except Exception as e:
            return {"table": full_table_name, "error": str(e)}
    
    def export_to_parquet(self, table_name: str, output_path: Path):
        """테이블을 Parquet 파일로 내보내기"""
        self.conn.execute(f"""
            COPY {table_name} TO '{output_path}' (FORMAT PARQUET)
        """)
        print(f"✅ Exported {table_name} to {output_path}")
        return self
    
    def export_to_json(self, table_name: str, output_path: Path):
        """테이블을 JSON 파일로 내보내기"""
        df = self.query(f"SELECT * FROM {table_name}")
        df.to_json(output_path, orient="records", force_ascii=False, indent=2)
        print(f"✅ Exported {table_name} to {output_path}")
        return self
    
    def list_tables(self) -> Dict[str, List[str]]:
        """모든 스키마의 테이블 목록 조회"""
        result = {}
        
        for schema in ['bronze', 'silver', 'gold', 'metadata']:
            try:
                tables = self.conn.execute(f"""
                    SELECT table_name 
                    FROM information_schema.tables 
                    WHERE table_schema = '{schema}'
                """).fetchall()
                result[schema] = [t[0] for t in tables]
            except:
                result[schema] = []
        
        return result


def init_warehouse():
    """데이터 웨어하우스 초기화"""
    with DataWarehouse() as dw:
        dw.init_schemas()
        dw.init_all_tables()
        
        print("\n📊 테이블 목록:")
        tables = dw.list_tables()
        for schema, table_list in tables.items():
            print(f"  {schema}: {table_list}")
    
    return True


if __name__ == "__main__":
    print("=" * 60)
    print("🗄️ 데이터 웨어하우스 초기화")
    print("=" * 60)
    
    init_warehouse()
    
    print("\n✅ 데이터 웨어하우스 초기화 완료")
