"""
파이프라인 오케스트레이터
전체 ETL 파이프라인 실행 및 관리

실행 순서:
1. 데이터 레이크 초기화
2. 데이터 웨어하우스 초기화
3. Bronze: 데이터 수집 (Ingestion)
4. Silver: 데이터 정제 (Transformation)
5. Gold: 데이터 집계 (Aggregation)
6. Serving: Dashboard 데이터 생성
"""

import sys
from pathlib import Path
from datetime import datetime
import argparse

# 프로젝트 루트를 Python 경로에 추가
PIPELINE_DIR = Path(__file__).parent
sys.path.insert(0, str(PIPELINE_DIR))

from config import init_data_lake, get_config
from warehouse import DataWarehouse, init_warehouse
from ingestion import run_full_ingestion
from transformation import run_full_transformation


def run_pipeline(stages: list = None, verbose: bool = True):
    """
    전체 파이프라인 실행
    
    Args:
        stages: 실행할 단계 목록 (None이면 전체 실행)
                가능한 값: ['init', 'ingest', 'transform', 'serve']
        verbose: 상세 로그 출력 여부
    """
    
    all_stages = ['init', 'ingest', 'transform', 'serve']
    stages = stages or all_stages
    
    start_time = datetime.now()
    
    print("=" * 70)
    print("🚀 안산시 외국인 보육료 정책 감사 - 데이터 파이프라인")
    print("=" * 70)
    print(f"📅 실행 시작: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"📋 실행 단계: {stages}")
    print("=" * 70)
    
    results = {}
    
    try:
        # 1. 초기화 단계
        if 'init' in stages:
            print("\n" + "─" * 50)
            print("📁 [1/4] 데이터 레이크 및 웨어하우스 초기화")
            print("─" * 50)
            
            init_data_lake()
            init_warehouse()
            
            results['init'] = {'status': 'success'}
            print("✅ 초기화 완료")
        
        # 2. 수집 단계 (Bronze)
        if 'ingest' in stages:
            print("\n" + "─" * 50)
            print("📥 [2/4] Bronze Layer - 데이터 수집")
            print("─" * 50)
            
            run_full_ingestion()
            
            results['ingest'] = {'status': 'success'}
            print("✅ 데이터 수집 완료")
        
        # 3. 변환 단계 (Silver + Gold)
        if 'transform' in stages:
            print("\n" + "─" * 50)
            print("🔄 [3/4] Silver/Gold Layer - 데이터 변환")
            print("─" * 50)
            
            run_full_transformation()
            
            results['transform'] = {'status': 'success'}
            print("✅ 데이터 변환 완료")
        
        # 4. 서빙 단계
        if 'serve' in stages:
            print("\n" + "─" * 50)
            print("📊 [4/4] Serving Layer - Dashboard 데이터 생성")
            print("─" * 50)
            
            # transformation.py에서 이미 처리됨
            from transformation import ServingLayer
            serving = ServingLayer()
            serving.run()
            
            results['serve'] = {'status': 'success'}
            print("✅ Dashboard 데이터 생성 완료")
        
    except Exception as e:
        print(f"\n❌ 파이프라인 실행 중 오류 발생: {e}")
        results['error'] = str(e)
        
        # 오류 로깅
        try:
            with DataWarehouse() as dw:
                dw.log_pipeline_run(
                    pipeline_name="full_pipeline",
                    layer="error",
                    status="failed",
                    error_message=str(e)
                )
        except:
            pass
    
    # 실행 완료 요약
    end_time = datetime.now()
    duration = end_time - start_time
    
    print("\n" + "=" * 70)
    print("📋 파이프라인 실행 완료 요약")
    print("=" * 70)
    print(f"⏱️  소요 시간: {duration}")
    print(f"📊 실행 결과:")
    
    for stage, result in results.items():
        status = result.get('status', 'unknown')
        status_icon = "✅" if status == 'success' else "❌"
        print(f"    {status_icon} {stage}: {status}")
    
    # 데이터 레이크 현황 출력
    config = get_config()
    print(f"\n📁 데이터 레이크 위치: {config['data_lake_root']}")
    print(f"🗄️  웨어하우스 DB: {config['warehouse_db']}")
    
    print("=" * 70)
    
    return results


def show_status():
    """파이프라인 상태 확인"""
    
    print("=" * 60)
    print("📊 데이터 파이프라인 상태")
    print("=" * 60)
    
    config = get_config()
    
    # 데이터 레이크 상태
    print("\n📁 데이터 레이크:")
    for layer, path in config["layers"].items():
        layer_path = Path(path)
        if layer_path.exists():
            files = list(layer_path.rglob("*"))
            file_count = len([f for f in files if f.is_file()])
            print(f"  ✅ {layer.upper()}: {file_count} files")
        else:
            print(f"  ⚠️ {layer.upper()}: Not initialized")
    
    # 웨어하우스 상태
    print("\n🗄️  데이터 웨어하우스:")
    db_path = Path(config["warehouse_db"])
    if db_path.exists():
        print(f"  ✅ DB exists: {db_path}")
        
        try:
            with DataWarehouse() as dw:
                tables = dw.list_tables()
                for schema, table_list in tables.items():
                    if table_list:
                        print(f"  📋 {schema}: {len(table_list)} tables")
        except Exception as e:
            print(f"  ⚠️ Cannot read DB: {e}")
    else:
        print(f"  ⚠️ DB not found")
    
    # 최근 파이프라인 실행 로그
    print("\n📜 최근 파이프라인 실행:")
    try:
        with DataWarehouse() as dw:
            runs = dw.query("""
                SELECT pipeline_name, layer, status, completed_at, rows_processed
                FROM metadata.pipeline_runs
                ORDER BY completed_at DESC
                LIMIT 5
            """)
            if not runs.empty:
                for _, row in runs.iterrows():
                    status_icon = "✅" if row['status'] == 'success' else "❌"
                    print(f"  {status_icon} {row['pipeline_name']} ({row['layer']}): "
                          f"{row['rows_processed']} rows")
            else:
                print("  ℹ️ No pipeline runs recorded")
    except Exception as e:
        print(f"  ⚠️ Cannot read logs: {e}")
    
    print("=" * 60)


def main():
    """메인 CLI 엔트리포인트"""
    
    parser = argparse.ArgumentParser(
        description="안산시 외국인 보육료 정책 감사 - 데이터 파이프라인"
    )
    
    parser.add_argument(
        "command",
        choices=["run", "status", "init", "ingest", "transform", "serve"],
        help="실행할 명령"
    )
    
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="상세 로그 출력"
    )
    
    args = parser.parse_args()
    
    if args.command == "run":
        run_pipeline(verbose=args.verbose)
    
    elif args.command == "status":
        show_status()
    
    elif args.command == "init":
        run_pipeline(stages=['init'], verbose=args.verbose)
    
    elif args.command == "ingest":
        run_pipeline(stages=['init', 'ingest'], verbose=args.verbose)
    
    elif args.command == "transform":
        run_pipeline(stages=['transform'], verbose=args.verbose)
    
    elif args.command == "serve":
        run_pipeline(stages=['serve'], verbose=args.verbose)


if __name__ == "__main__":
    # 인자 없이 실행하면 전체 파이프라인 실행
    if len(sys.argv) == 1:
        run_pipeline()
    else:
        main()
