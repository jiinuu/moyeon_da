import requests
import json
import re
import pandas as pd
import duckdb
import time

API_KEY = "YmI2OGI0NGFhMzkzZjIyODVlMjI2NDI2MDI1YjFkZjc="

def clean_and_parse_kosis_json(raw_text):
    try:
        fixed_text = re.sub(r'([{,])\s*([a-zA-Z_][a-zA-Z0-9_]*)\s*:', r'\1"\2":', raw_text)
        return json.loads(fixed_text)
    except:
        return None

def fetch_bulk_data():
    # 1. DB에서 행안부 통계표 리스트 추출
    con = duckdb.connect('foreign_policy.db')
    target_tables = con.execute("""
        SELECT TBL_ID, TBL_NM 
        FROM silver_stat_catalog 
        WHERE ORG_NM = '행정안전부'
    """).df()
    
    print(f"📂 총 {len(target_tables)}개의 행안부 통계표 수집을 시작합니다.")

    base_url = "https://kosis.kr/openapi/Param/statisticsParameterData.do"
    
    for _, row in target_tables.iterrows():
        tbl_id = row['TBL_ID']
        tbl_nm = row['TBL_NM']
        
        print(f"\n📡 [{tbl_id}] {tbl_nm} 수집 시도 중...")
        
        # 행안부 데이터는 대부분 itmId='ALL', objL1='ALL'로 호출 가능합니다.
        # (특수한 TX_11025_A001_A는 이미 수집했으므로 예외처리하거나 포함시킵니다.)
        params = {
            "method": "getList",
            "apiKey": API_KEY,
            "itmId": "ALL+",
            "objL1": "ALL+",
            "objL2": "ALL+",
            "objL3": "ALL+",
            "format": "json",
            "jsonVD": "Y",
            "prdSe": "Y",
            "newEstPrdCnt": "1", # 벌크 수집 시에는 용량 관계상 최신 1년치만 우선 수집
            "orgId": "110",
            "tblId": tbl_id
        }

        try:
            query_string = "&".join([f"{k}={v}" for k, v in params.items()])
            full_url = f"{base_url}?{query_string}"
            
            response = requests.get(full_url)
            data = clean_and_parse_kosis_json(response.text)
            
            if data and isinstance(data, list):
                df = pd.DataFrame(data)
                df.columns = [col.upper() for col in df.columns]
                
                # 테이블명: silver_행안부_테이블ID
                table_name = f"silver_bulk_{tbl_id.lower()}"
                con.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM df")
                
                print(f"✅ 저장 완료: {table_name} ({len(df)}행)")
            else:
                # 'ALL'로 안 되는 경우 (에러 21 등) 알림
                print(f"⚠️ {tbl_id}: 구체적인 파라미터가 필요한 통계표입니다. (건너뜀)")
                
        except Exception as e:
            print(f"❌ {tbl_id} 처리 중 오류: {e}")
        
        time.sleep(0.5) # 서버 부하 방지

    con.close()
    print("\n🚀 행정안전부 벌크 수집 프로세스가 완료되었습니다.")

if __name__ == "__main__":
    fetch_bulk_data()