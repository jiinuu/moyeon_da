"""
안산시 다문화 보육 데이터 플랫폼 - 어드민 대시보드 서버
DuckDB 기반 SQL 쿼리 인터페이스 + 데이터베이스 현황 시각화

사용법:
    python admin_server.py

기능:
    - SQL 쿼리 실행 및 결과 시각화
    - 데이터베이스 스키마 탐색
    - ERD 다이어그램 생성
    - 테이블 통계 대시보드
"""

from flask import Flask, render_template_string, request, jsonify
import duckdb
from pathlib import Path
import json
from datetime import datetime

app = Flask(__name__)

# 데이터베이스 경로
DB_PATH = Path(__file__).parent / "pipeline" / "warehouse.duckdb"

def get_db_connection():
    """DuckDB 연결 반환"""
    return duckdb.connect(str(DB_PATH), read_only=True)

def get_database_schema():
    """전체 데이터베이스 스키마 조회"""
    conn = get_db_connection()
    
    schema_info = {}
    
    # 모든 스키마 조회
    schemas = conn.execute("""
        SELECT DISTINCT table_schema 
        FROM information_schema.tables 
        WHERE table_schema NOT IN ('information_schema', 'pg_catalog')
    """).fetchall()
    
    for (schema_name,) in schemas:
        schema_info[schema_name] = {}
        
        # 해당 스키마의 테이블 조회
        tables = conn.execute(f"""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = '{schema_name}'
        """).fetchall()
        
        for (table_name,) in tables:
            # 컬럼 정보 조회
            columns = conn.execute(f"""
                SELECT column_name, data_type, is_nullable
                FROM information_schema.columns
                WHERE table_schema = '{schema_name}' 
                AND table_name = '{table_name}'
                ORDER BY ordinal_position
            """).fetchall()
            
            # 레코드 수 조회
            try:
                count = conn.execute(
                    f"SELECT COUNT(*) FROM {schema_name}.{table_name}"
                ).fetchone()[0]
            except:
                count = 0
            
            schema_info[schema_name][table_name] = {
                "columns": [
                    {"name": c[0], "type": c[1], "nullable": c[2] == 'YES'}
                    for c in columns
                ],
                "row_count": count
            }
    
    conn.close()
    return schema_info

def execute_query(sql):
    """SQL 쿼리 실행"""
    conn = get_db_connection()
    try:
        result = conn.execute(sql).fetchdf()
        columns = list(result.columns)
        data = result.to_dict('records')
        return {"success": True, "columns": columns, "data": data, "row_count": len(data)}
    except Exception as e:
        return {"success": False, "error": str(e)}
    finally:
        conn.close()

# HTML 템플릿
ADMIN_TEMPLATE = """
<!DOCTYPE html>
<html lang="ko">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>데이터 어드민 | 안산시 다문화 보육 플랫폼</title>
    <link href="https://fonts.googleapis.com/css2?family=Noto+Sans+KR:wght@300;400;500;600;700&family=JetBrains+Mono:wght@400;500&display=swap" rel="stylesheet">
    <style>
        * {
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }
        
        :root {
            --bg-primary: #0f0f17;
            --bg-secondary: #1a1a2e;
            --bg-tertiary: #16213e;
            --text-primary: #f8fafc;
            --text-secondary: #94a3b8;
            --accent: #6366f1;
            --accent-light: #818cf8;
            --success: #22c55e;
            --warning: #f59e0b;
            --danger: #ef4444;
            --border: rgba(255,255,255,0.08);
        }
        
        body {
            font-family: 'Noto Sans KR', sans-serif;
            background: var(--bg-primary);
            color: var(--text-primary);
            min-height: 100vh;
        }
        
        .app {
            display: grid;
            grid-template-columns: 280px 1fr;
            min-height: 100vh;
        }
        
        /* 사이드바 */
        .sidebar {
            background: var(--bg-secondary);
            border-right: 1px solid var(--border);
            padding: 24px;
            overflow-y: auto;
        }
        
        .sidebar-header {
            margin-bottom: 32px;
        }
        
        .sidebar-header h1 {
            font-size: 1.25rem;
            font-weight: 600;
            margin-bottom: 8px;
        }
        
        .sidebar-header p {
            font-size: 0.8125rem;
            color: var(--text-secondary);
        }
        
        .schema-section {
            margin-bottom: 24px;
        }
        
        .schema-title {
            font-size: 0.75rem;
            font-weight: 600;
            color: var(--text-secondary);
            text-transform: uppercase;
            letter-spacing: 0.05em;
            margin-bottom: 12px;
            display: flex;
            align-items: center;
            gap: 8px;
        }
        
        .schema-badge {
            display: inline-flex;
            align-items: center;
            justify-content: center;
            width: 20px;
            height: 20px;
            border-radius: 4px;
            font-size: 0.6875rem;
            font-weight: 700;
        }
        
        .schema-badge.bronze { background: #92400e; }
        .schema-badge.silver { background: #475569; }
        .schema-badge.gold { background: #d97706; }
        .schema-badge.metadata { background: #6366f1; }
        
        .table-list {
            list-style: none;
        }
        
        .table-item {
            display: flex;
            align-items: center;
            justify-content: space-between;
            padding: 10px 12px;
            border-radius: 8px;
            cursor: pointer;
            transition: all 0.2s;
            margin-bottom: 4px;
        }
        
        .table-item:hover {
            background: rgba(99, 102, 241, 0.1);
        }
        
        .table-item.active {
            background: rgba(99, 102, 241, 0.2);
            border-left: 3px solid var(--accent);
        }
        
        .table-name {
            font-size: 0.875rem;
            font-family: 'JetBrains Mono', monospace;
        }
        
        .table-count {
            font-size: 0.75rem;
            color: var(--text-secondary);
            background: rgba(255,255,255,0.05);
            padding: 2px 8px;
            border-radius: 10px;
        }
        
        /* 메인 콘텐츠 */
        .main {
            padding: 24px;
            overflow-y: auto;
        }
        
        .main-header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 24px;
        }
        
        .tabs {
            display: flex;
            gap: 8px;
        }
        
        .tab {
            padding: 10px 20px;
            border-radius: 8px;
            border: 1px solid var(--border);
            background: transparent;
            color: var(--text-secondary);
            cursor: pointer;
            font-size: 0.875rem;
            font-weight: 500;
            transition: all 0.2s;
        }
        
        .tab:hover {
            background: rgba(255,255,255,0.05);
        }
        
        .tab.active {
            background: var(--accent);
            border-color: var(--accent);
            color: white;
        }
        
        /* SQL 에디터 */
        .sql-editor {
            margin-bottom: 24px;
        }
        
        .sql-textarea {
            width: 100%;
            height: 150px;
            background: var(--bg-tertiary);
            border: 1px solid var(--border);
            border-radius: 12px;
            padding: 16px;
            color: var(--text-primary);
            font-family: 'JetBrains Mono', monospace;
            font-size: 0.875rem;
            resize: vertical;
            outline: none;
        }
        
        .sql-textarea:focus {
            border-color: var(--accent);
        }
        
        .sql-actions {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-top: 12px;
        }
        
        .run-btn {
            padding: 12px 24px;
            background: var(--accent);
            border: none;
            border-radius: 8px;
            color: white;
            font-weight: 600;
            cursor: pointer;
            display: flex;
            align-items: center;
            gap: 8px;
            transition: all 0.2s;
        }
        
        .run-btn:hover {
            background: var(--accent-light);
        }
        
        .query-templates {
            display: flex;
            gap: 8px;
        }
        
        .template-btn {
            padding: 8px 12px;
            background: rgba(255,255,255,0.05);
            border: 1px solid var(--border);
            border-radius: 6px;
            color: var(--text-secondary);
            font-size: 0.75rem;
            cursor: pointer;
            transition: all 0.2s;
        }
        
        .template-btn:hover {
            background: rgba(255,255,255,0.1);
            border-color: var(--accent);
        }
        
        /* 결과 테이블 */
        .results {
            background: var(--bg-secondary);
            border: 1px solid var(--border);
            border-radius: 12px;
            overflow: hidden;
        }
        
        .results-header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            padding: 16px 20px;
            background: rgba(255,255,255,0.02);
            border-bottom: 1px solid var(--border);
        }
        
        .results-info {
            font-size: 0.875rem;
            color: var(--text-secondary);
        }
        
        .results-table-wrapper {
            overflow-x: auto;
            max-height: 400px;
            overflow-y: auto;
        }
        
        .results-table {
            width: 100%;
            border-collapse: collapse;
        }
        
        .results-table th,
        .results-table td {
            padding: 12px 16px;
            text-align: left;
            border-bottom: 1px solid var(--border);
            font-size: 0.875rem;
        }
        
        .results-table th {
            background: rgba(255,255,255,0.02);
            font-weight: 600;
            color: var(--text-secondary);
            position: sticky;
            top: 0;
        }
        
        .results-table td {
            font-family: 'JetBrains Mono', monospace;
            font-size: 0.8125rem;
        }
        
        .results-table tr:hover td {
            background: rgba(99, 102, 241, 0.05);
        }
        
        /* ERD 다이어그램 */
        .erd-container {
            background: var(--bg-secondary);
            border: 1px solid var(--border);
            border-radius: 12px;
            padding: 24px;
        }
        
        .erd-layer {
            margin-bottom: 32px;
        }
        
        .erd-layer-title {
            display: flex;
            align-items: center;
            gap: 12px;
            margin-bottom: 16px;
            padding-bottom: 12px;
            border-bottom: 1px solid var(--border);
        }
        
        .erd-layer-badge {
            padding: 6px 14px;
            border-radius: 20px;
            font-size: 0.75rem;
            font-weight: 600;
        }
        
        .erd-layer-badge.bronze { background: linear-gradient(135deg, #92400e, #b45309); }
        .erd-layer-badge.silver { background: linear-gradient(135deg, #475569, #64748b); }
        .erd-layer-badge.gold { background: linear-gradient(135deg, #d97706, #f59e0b); }
        .erd-layer-badge.metadata { background: linear-gradient(135deg, #4f46e5, #6366f1); }
        
        .erd-tables {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(280px, 1fr));
            gap: 16px;
        }
        
        .erd-table {
            background: var(--bg-tertiary);
            border: 1px solid var(--border);
            border-radius: 10px;
            overflow: hidden;
        }
        
        .erd-table-header {
            background: rgba(99, 102, 241, 0.1);
            padding: 12px 16px;
            font-weight: 600;
            font-family: 'JetBrains Mono', monospace;
            font-size: 0.875rem;
            display: flex;
            justify-content: space-between;
            align-items: center;
        }
        
        .erd-row-count {
            font-size: 0.75rem;
            color: var(--text-secondary);
            font-weight: 400;
        }
        
        .erd-columns {
            padding: 8px 0;
        }
        
        .erd-column {
            display: flex;
            justify-content: space-between;
            align-items: center;
            padding: 8px 16px;
            font-size: 0.8125rem;
        }
        
        .erd-column:hover {
            background: rgba(255,255,255,0.02);
        }
        
        .erd-column-name {
            font-family: 'JetBrains Mono', monospace;
        }
        
        .erd-column-type {
            font-size: 0.6875rem;
            color: var(--accent-light);
            background: rgba(99, 102, 241, 0.1);
            padding: 2px 8px;
            border-radius: 4px;
        }
        
        /* 통계 카드 */
        .stats-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 16px;
            margin-bottom: 24px;
        }
        
        .stat-card {
            background: var(--bg-secondary);
            border: 1px solid var(--border);
            border-radius: 12px;
            padding: 20px;
        }
        
        .stat-label {
            font-size: 0.8125rem;
            color: var(--text-secondary);
            margin-bottom: 8px;
        }
        
        .stat-value {
            font-size: 1.75rem;
            font-weight: 700;
        }
        
        .stat-value.accent { color: var(--accent); }
        .stat-value.success { color: var(--success); }
        .stat-value.warning { color: var(--warning); }
        
        /* 에러 메시지 */
        .error-box {
            background: rgba(239, 68, 68, 0.1);
            border: 1px solid rgba(239, 68, 68, 0.3);
            border-radius: 8px;
            padding: 16px;
            color: var(--danger);
            margin-bottom: 16px;
        }
        
        /* 반응형 */
        @media (max-width: 768px) {
            .app {
                grid-template-columns: 1fr;
            }
            
            .sidebar {
                display: none;
            }
        }
    </style>
</head>
<body>
    <div class="app">
        <!-- 사이드바 -->
        <aside class="sidebar">
            <div class="sidebar-header">
                <h1>🗄️ 데이터 어드민</h1>
                <p>안산시 다문화 보육 플랫폼</p>
            </div>
            
            <div id="schema-tree">
                <!-- JavaScript로 동적 생성 -->
                <p style="color: var(--text-secondary); font-size: 0.875rem;">
                    스키마 로딩 중...
                </p>
            </div>
        </aside>
        
        <!-- 메인 콘텐츠 -->
        <main class="main">
            <div class="main-header">
                <div class="tabs">
                    <button class="tab active" onclick="showTab('query')">SQL 쿼리</button>
                    <button class="tab" onclick="showTab('erd')">ERD 다이어그램</button>
                    <button class="tab" onclick="showTab('stats')">통계</button>
                </div>
            </div>
            
            <!-- SQL 쿼리 탭 -->
            <div id="tab-query" class="tab-content">
                <div class="sql-editor">
                    <textarea id="sql-input" class="sql-textarea" placeholder="SELECT * FROM gold.ansan_foreigner_trend LIMIT 10;">SELECT * FROM gold.ansan_foreigner_trend;</textarea>
                    
                    <div class="sql-actions">
                        <div class="query-templates">
                            <button class="template-btn" onclick="insertTemplate('trend')">📈 외국인 추이</button>
                            <button class="template-btn" onclick="insertTemplate('comparison')">🗺️ 지역 비교</button>
                            <button class="template-btn" onclick="insertTemplate('gap')">📊 지원 격차</button>
                            <button class="template-btn" onclick="insertTemplate('tables')">📋 전체 테이블</button>
                        </div>
                        <button class="run-btn" onclick="runQuery()">
                            ▶ 실행
                        </button>
                    </div>
                </div>
                
                <div id="query-results" class="results" style="display: none;">
                    <div class="results-header">
                        <span class="results-info" id="results-info"></span>
                    </div>
                    <div class="results-table-wrapper">
                        <table class="results-table" id="results-table">
                        </table>
                    </div>
                </div>
                
                <div id="query-error" class="error-box" style="display: none;"></div>
            </div>
            
            <!-- ERD 다이어그램 탭 -->
            <div id="tab-erd" class="tab-content" style="display: none;">
                <div id="erd-diagram" class="erd-container">
                    <!-- JavaScript로 동적 생성 -->
                </div>
            </div>
            
            <!-- 통계 탭 -->
            <div id="tab-stats" class="tab-content" style="display: none;">
                <div id="stats-container">
                    <!-- JavaScript로 동적 생성 -->
                </div>
            </div>
        </main>
    </div>
    
    <script>
        // 전역 변수
        let schemaData = {};
        
        // 페이지 로드 시 실행
        document.addEventListener('DOMContentLoaded', () => {
            loadSchema();
        });
        
        // 스키마 로드
        async function loadSchema() {
            try {
                const response = await fetch('/api/schema');
                schemaData = await response.json();
                renderSchemaTree();
                renderERD();
                renderStats();
            } catch (error) {
                console.error('Schema load failed:', error);
            }
        }
        
        // 스키마 트리 렌더링
        function renderSchemaTree() {
            const container = document.getElementById('schema-tree');
            let html = '';
            
            const layerOrder = ['bronze', 'silver', 'gold', 'metadata'];
            const layerNames = {
                bronze: 'Bronze (원본)',
                silver: 'Silver (정제)',
                gold: 'Gold (집계)',
                metadata: 'Metadata'
            };
            
            for (const layer of layerOrder) {
                if (!schemaData[layer]) continue;
                
                html += `
                    <div class="schema-section">
                        <div class="schema-title">
                            <span class="schema-badge ${layer}">${layer[0].toUpperCase()}</span>
                            ${layerNames[layer] || layer}
                        </div>
                        <ul class="table-list">
                `;
                
                for (const [tableName, tableInfo] of Object.entries(schemaData[layer])) {
                    html += `
                        <li class="table-item" onclick="selectTable('${layer}', '${tableName}')">
                            <span class="table-name">${tableName}</span>
                            <span class="table-count">${tableInfo.row_count}</span>
                        </li>
                    `;
                }
                
                html += '</ul></div>';
            }
            
            container.innerHTML = html;
        }
        
        // ERD 다이어그램 렌더링
        function renderERD() {
            const container = document.getElementById('erd-diagram');
            let html = '';
            
            const layerOrder = ['bronze', 'silver', 'gold', 'metadata'];
            const layerNames = {
                bronze: 'Bronze Layer (원본 데이터)',
                silver: 'Silver Layer (정제된 데이터)',
                gold: 'Gold Layer (분석용 집계)',
                metadata: 'Metadata (메타데이터)'
            };
            
            for (const layer of layerOrder) {
                if (!schemaData[layer] || Object.keys(schemaData[layer]).length === 0) continue;
                
                html += `
                    <div class="erd-layer">
                        <div class="erd-layer-title">
                            <span class="erd-layer-badge ${layer}">${layerNames[layer]}</span>
                        </div>
                        <div class="erd-tables">
                `;
                
                for (const [tableName, tableInfo] of Object.entries(schemaData[layer])) {
                    html += `
                        <div class="erd-table">
                            <div class="erd-table-header">
                                ${tableName}
                                <span class="erd-row-count">${tableInfo.row_count} rows</span>
                            </div>
                            <div class="erd-columns">
                    `;
                    
                    for (const col of tableInfo.columns) {
                        html += `
                            <div class="erd-column">
                                <span class="erd-column-name">${col.name}</span>
                                <span class="erd-column-type">${col.type}</span>
                            </div>
                        `;
                    }
                    
                    html += '</div></div>';
                }
                
                html += '</div></div>';
            }
            
            container.innerHTML = html;
        }
        
        // 통계 렌더링
        function renderStats() {
            const container = document.getElementById('stats-container');
            
            let totalTables = 0;
            let totalRows = 0;
            let totalColumns = 0;
            
            for (const tables of Object.values(schemaData)) {
                for (const tableInfo of Object.values(tables)) {
                    totalTables++;
                    totalRows += tableInfo.row_count;
                    totalColumns += tableInfo.columns.length;
                }
            }
            
            container.innerHTML = `
                <div class="stats-grid">
                    <div class="stat-card">
                        <div class="stat-label">총 스키마 수</div>
                        <div class="stat-value accent">${Object.keys(schemaData).length}</div>
                    </div>
                    <div class="stat-card">
                        <div class="stat-label">총 테이블 수</div>
                        <div class="stat-value success">${totalTables}</div>
                    </div>
                    <div class="stat-card">
                        <div class="stat-label">총 레코드 수</div>
                        <div class="stat-value warning">${totalRows.toLocaleString()}</div>
                    </div>
                    <div class="stat-card">
                        <div class="stat-label">총 컬럼 수</div>
                        <div class="stat-value accent">${totalColumns}</div>
                    </div>
                </div>
                
                <div class="erd-container">
                    <h3 style="margin-bottom: 16px;">레이어별 데이터 현황</h3>
                    ${getLayerStats()}
                </div>
            `;
        }
        
        function getLayerStats() {
            const layers = ['bronze', 'silver', 'gold', 'metadata'];
            let html = '<div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 16px;">';
            
            for (const layer of layers) {
                if (!schemaData[layer]) continue;
                
                const tables = Object.keys(schemaData[layer]).length;
                const rows = Object.values(schemaData[layer]).reduce((sum, t) => sum + t.row_count, 0);
                
                html += `
                    <div style="background: var(--bg-tertiary); padding: 16px; border-radius: 10px;">
                        <div style="font-weight: 600; margin-bottom: 8px; text-transform: uppercase; letter-spacing: 0.05em; font-size: 0.75rem; color: var(--text-secondary);">${layer}</div>
                        <div style="font-size: 1.25rem; font-weight: 700;">${tables} 테이블</div>
                        <div style="font-size: 0.875rem; color: var(--text-secondary);">${rows.toLocaleString()} 레코드</div>
                    </div>
                `;
            }
            
            html += '</div>';
            return html;
        }
        
        // 탭 전환
        function showTab(tabName) {
            // 모든 탭 비활성화
            document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
            document.querySelectorAll('.tab-content').forEach(c => c.style.display = 'none');
            
            // 선택된 탭 활성화
            event.target.classList.add('active');
            document.getElementById(`tab-${tabName}`).style.display = 'block';
        }
        
        // 테이블 선택
        function selectTable(schema, table) {
            document.getElementById('sql-input').value = `SELECT * FROM ${schema}.${table} LIMIT 100;`;
            
            // 활성 상태 표시
            document.querySelectorAll('.table-item').forEach(el => el.classList.remove('active'));
            event.target.closest('.table-item').classList.add('active');
        }
        
        // 쿼리 템플릿
        function insertTemplate(type) {
            const templates = {
                trend: 'SELECT year, total_population, foreign_population, ratio, yoy_growth FROM gold.ansan_foreigner_trend ORDER BY year;',
                comparison: 'SELECT region, foreign_ratio, foreign_count, pilot_program, rank FROM gold.gyeonggi_comparison ORDER BY rank;',
                gap: 'SELECT * FROM gold.support_gap_analysis;',
                tables: "SELECT table_schema, table_name FROM information_schema.tables WHERE table_schema IN ('bronze', 'silver', 'gold', 'metadata');"
            };
            
            document.getElementById('sql-input').value = templates[type] || '';
        }
        
        // 쿼리 실행
        async function runQuery() {
            const sql = document.getElementById('sql-input').value;
            const resultsDiv = document.getElementById('query-results');
            const errorDiv = document.getElementById('query-error');
            
            // 초기화
            resultsDiv.style.display = 'none';
            errorDiv.style.display = 'none';
            
            try {
                const response = await fetch('/api/query', {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({sql})
                });
                
                const result = await response.json();
                
                if (result.success) {
                    // 결과 표시
                    resultsDiv.style.display = 'block';
                    document.getElementById('results-info').textContent = 
                        `${result.row_count}개 행 반환`;
                    
                    // 테이블 렌더링
                    const table = document.getElementById('results-table');
                    let html = '<thead><tr>';
                    
                    for (const col of result.columns) {
                        html += `<th>${col}</th>`;
                    }
                    html += '</tr></thead><tbody>';
                    
                    for (const row of result.data) {
                        html += '<tr>';
                        for (const col of result.columns) {
                            const value = row[col];
                            html += `<td>${value !== null ? value : '<em style="color:var(--text-secondary)">NULL</em>'}</td>`;
                        }
                        html += '</tr>';
                    }
                    html += '</tbody>';
                    
                    table.innerHTML = html;
                } else {
                    errorDiv.style.display = 'block';
                    errorDiv.textContent = result.error;
                }
            } catch (error) {
                errorDiv.style.display = 'block';
                errorDiv.textContent = 'Request failed: ' + error.message;
            }
        }
        
        // 키보드 단축키
        document.getElementById('sql-input').addEventListener('keydown', (e) => {
            if ((e.ctrlKey || e.metaKey) && e.key === 'Enter') {
                runQuery();
            }
        });
    </script>
</body>
</html>
"""

@app.route('/')
def index():
    return render_template_string(ADMIN_TEMPLATE)

@app.route('/api/schema')
def api_schema():
    schema = get_database_schema()
    return jsonify(schema)

@app.route('/api/query', methods=['POST'])
def api_query():
    data = request.get_json()
    sql = data.get('sql', '')
    
    # 간단한 보안 체크 (읽기 전용)
    sql_upper = sql.upper().strip()
    if any(keyword in sql_upper for keyword in ['INSERT', 'UPDATE', 'DELETE', 'DROP', 'CREATE', 'ALTER']):
        return jsonify({"success": False, "error": "읽기 전용 모드입니다. SELECT 쿼리만 허용됩니다."})
    
    result = execute_query(sql)
    return jsonify(result)

@app.route('/api/tables')
def api_tables():
    conn = get_db_connection()
    tables = conn.execute("""
        SELECT table_schema, table_name 
        FROM information_schema.tables 
        WHERE table_schema IN ('bronze', 'silver', 'gold', 'metadata')
        ORDER BY table_schema, table_name
    """).fetchall()
    conn.close()
    
    return jsonify([{"schema": t[0], "table": t[1]} for t in tables])


if __name__ == '__main__':
    print("=" * 60)
    print("🗄️ 안산시 다문화 보육 데이터 플랫폼 - 어드민 서버")
    print("=" * 60)
    print(f"📊 데이터베이스: {DB_PATH}")
    print(f"🌐 접속 주소: http://localhost:5000")
    print("=" * 60)
    
    app.run(host='0.0.0.0', port=5000, debug=True)
