/**
 * 안산시 외국인 보육료 정책 감사 대시보드
 * Interactive Dashboard Script with Source Citations
 */

// =============================================
// 출처 데이터 (Source Database)
// =============================================
const sourceData = {
    'policy-document': {
        title: '안산시 정책 문서',
        content: `
            <h4>📄 문서 정보</h4>
            <div class="data-highlight">
                <strong>문서명:</strong> 외국인가정의 안전한 보육환경 조성 (외국인 보육료 지원)<br>
                <strong>문서번호:</strong> 3-3-48<br>
                <strong>담당부서:</strong> 경기도 안산시 여성보육과<br>
                <strong>연락처:</strong> 031-481-3323
            </div>
            
            <h4>📊 핵심 데이터</h4>
            <ul>
                <li><strong>지원 대상:</strong> 안산시 관내 어린이집 재원 중인 0~5세 <strong>등록외국인 아동</strong></li>
                <li><strong>거주 요건:</strong> 아동과 보호자(1명)가 경기도 및 안산시에 90일 초과 거주</li>
                <li><strong>0~2세 지원금:</strong> 도비 10만원 + 시비 16만원 = <strong>월 26만원</strong></li>
                <li><strong>3~5세 지원금:</strong> 도비 10만원 + 시비 18만원 = <strong>월 28만원</strong></li>
                <li><strong>2024년 수혜 인원:</strong> 보육료 2,144명, 연장보육료 1,434명 (24.8월말 기준)</li>
                <li><strong>2025년 예산:</strong> 총 72.84억원 (도비 10.56억 / 시비 62.28억)</li>
            </ul>
            
            <div class="warning-box">
                <strong>⚠️ 주요 발견:</strong> 정책 문서에 "등록외국인 아동"으로 명시되어 있어, 
                미등록 외국인 아동은 법적으로 지원 대상에서 제외됩니다.
            </div>
            
            <h4>🔗 원본 문서</h4>
            <p>본 대시보드에 첨부된 PDF 파일에서 전체 내용을 확인할 수 있습니다.</p>
            <a href="./외국인 보육료 지원(안산시 정책).pdf" target="_blank" class="source-link">
                📎 정책 문서 PDF 열기
            </a>
        `
    },

    'foreigner-population': {
        title: '안산시 외국인 인구 통계',
        content: `
            <h4>📊 통계 정보</h4>
            <div class="data-highlight">
                <strong>기준 시점:</strong> 2024년 1월<br>
                <strong>출처:</strong> 안산시청 공식 통계, 언론 보도
            </div>
            
            <h4>📈 주요 수치</h4>
            <ul>
                <li><strong>안산시 전체 인구:</strong> 약 730,000명</li>
                <li><strong>외국인 주민:</strong> 96,300명 (전체 인구의 13.2%)</li>
                <li><strong>전국 순위:</strong> 외국인 인구 비율 전국 1위</li>
            </ul>
            
            <h4>🔗 출처</h4>
            <ul>
                <li>안산시청 공식 외국인 주민 현황 통계</li>
                <li>경인일보 (2024.01) "안산시 외국인 96,300명, 시 전체 인구의 13.2%"</li>
                <li>국민일보 (2024) "전국 최고 외국인 비중 '초다문화 사회' 안산"</li>
            </ul>
            
            <a href="https://www.ansan.go.kr" target="_blank" class="source-link">
                🌐 안산시청 공식 웹사이트
            </a>
        `
    },

    'wongok-stats': {
        title: '원곡동 다문화특구 현황',
        content: `
            <h4>📊 통계 정보</h4>
            <div class="data-highlight">
                <strong>기준 시점:</strong> 2024년 6월<br>
                <strong>지역:</strong> 안산시 단원구 원곡동 (다문화마을특구)
            </div>
            
            <h4>📈 주요 수치</h4>
            <ul>
                <li><strong>총 주민:</strong> 약 20,191명</li>
                <li><strong>외국인 주민:</strong> 18,014명 (89.2%)</li>
                <li><strong>내국인 주민:</strong> 2,177명 (10.8%)</li>
            </ul>
            
            <h4>🔗 출처</h4>
            <ul>
                <li>안산시청 다문화마을특구 현황 자료 (2024.6)</li>
                <li>동아일보 (2024.1) "원곡동 다문화특구 내 총 20,191명 주민 중 18,014명(89.2%)이 외국인"</li>
            </ul>
        `
    },

    'wongok-school': {
        title: '원곡초등학교 이주배경 학생 현황',
        content: `
            <h4>📊 통계 정보</h4>
            <div class="data-highlight">
                <strong>기준 시점:</strong> 2021년 10월<br>
                <strong>학교:</strong> 안산시 원곡초등학교
            </div>
            
            <h4>📈 주요 수치</h4>
            <ul>
                <li><strong>전체 학생:</strong> 449명</li>
                <li><strong>이주배경 학생:</strong> 443명 (98.6%)</li>
                <li><strong>비이주배경 학생:</strong> 6명 (1.4%)</li>
            </ul>
            
            <h4>🔗 출처</h4>
            <ul>
                <li>동아일보 (2021) "원곡초등학교 전체 학생 449명 중 443명(98.6%)이 이주배경"</li>
                <li>교육부 다문화교육 정책학교 현황 자료</li>
            </ul>
        `
    },

    'unregistered-support': {
        title: '미등록 아동 지원율 0% 근거',
        content: `
            <h4>📊 정책 분석</h4>
            <div class="warning-box">
                <strong>핵심 근거:</strong> 안산시 정책 문서(3-3-48)에 지원 대상을 
                <strong>"등록외국인 아동"</strong>으로 명시하고 있어, 
                미등록 외국인 아동은 법적으로 지원 대상에서 완전히 제외됩니다.
            </div>
            
            <h4>📋 관련 정책 조항</h4>
            <ul>
                <li><strong>지원 대상:</strong> "관내 어린이집 재원 등록외국인 아동"</li>
                <li><strong>자격 요건:</strong> "아동과 보호자(1명)가 경기도 및 안산시 90일 초과 거주"</li>
                <li><strong>지급 방식:</strong> "국민행복카드 결제 (부모 바우처 지급)"</li>
            </ul>
            
            <p>미등록 외국인은 외국인등록이 되어있지 않아 위 조건을 충족할 수 없으며, 
            국민행복카드 발급도 불가능합니다.</p>
            
            <h4>🔗 출처</h4>
            <a href="#" onclick="showSource('policy-document')" class="source-link">
                📎 안산시 정책 문서 확인하기
            </a>
        `
    },

    'moj-stats': {
        title: '법무부 미등록 이주아동 통계',
        content: `
            <h4>📊 통계 정보</h4>
            <div class="data-highlight">
                <strong>출처:</strong> 법무부 출입국·외국인정책본부<br>
                <strong>기준 시점:</strong> 2025년 1월
            </div>
            
            <h4>📈 공식 통계</h4>
            <ul>
                <li><strong>19세 이하 미등록 이주아동:</strong> 6,169명 (2025.1 기준)</li>
                <li><strong>참고 - 2024.11 기준:</strong> 6,296명</li>
                <li><strong>참고 - 2017.12 기준:</strong> 5,279명</li>
            </ul>
            
            <div class="warning-box">
                <strong>⚠️ 통계의 한계:</strong><br>
                법무부 통계는 <strong>국내 출생 미등록 이주아동이 제외</strong>되어 있어 
                실제 규모는 더 클 것으로 추정됩니다.
            </div>
            
            <h4>🔗 출처</h4>
            <ul>
                <li>법무부 출입국·외국인정책본부 통계월보</li>
                <li>연합뉴스 (2025) "미등록 이주아동 6,169명"</li>
                <li>국회 입법조사처 보고서 (2025.10)</li>
            </ul>
        `
    },

    'ngo-estimation': {
        title: '시민단체 미등록 아동 추정',
        content: `
            <h4>📊 추정 정보</h4>
            <div class="data-highlight">
                <strong>추정 주체:</strong> 이주민 인권단체, 시민사회단체<br>
                <strong>추정 근거:</strong> 법무부 통계 + 미포착 아동 보정
            </div>
            
            <h4>📈 추정 규모</h4>
            <ul>
                <li><strong>보수적 추정:</strong> 약 1만명</li>
                <li><strong>확장 추정:</strong> 약 2만명</li>
                <li><strong>최대 추정:</strong> 3만~5만명 (일부 단체)</li>
            </ul>
            
            <h4>🔍 추정 근거</h4>
            <ul>
                <li>법무부 통계에 미포함된 국내 출생 미등록 아동</li>
                <li>무국적 아동 (부모 미등록으로 출생신고 불가)</li>
                <li>단속 회피로 인한 미파악 아동</li>
            </ul>
            
            <h4>🔗 출처</h4>
            <ul>
                <li>이주아동권리보장기본법 제정 연대 발표 자료</li>
                <li>국가인권위원회 보고서</li>
                <li>미래조선 (2024) "미등록 이주아동, 실제 2만명 넘을 것"</li>
                <li>매일경제 (2025) "일부에서는 3만~5만명 추정"</li>
            </ul>
        `
    },

    'unregistered-estimation': {
        title: '안산시 미등록 아동 추정 계산',
        content: `
            <h4>📊 계산 로직</h4>
            <div class="calculation-box">
                <div class="calc-line">
                    <span>① 전국 미등록 아동 (법무부, 2025.1)</span>
                    <span>6,169명</span>
                </div>
                <div class="calc-line">
                    <span>② 안산시 외국인 비율</span>
                    <span>13.2%</span>
                </div>
                <div class="calc-line">
                    <span>③ 안산시 추정 (최소) = ① × ②</span>
                    <span>814명</span>
                </div>
            </div>
            
            <div class="calculation-box">
                <div class="calc-line">
                    <span>① 전국 미등록 아동 (시민단체 추정)</span>
                    <span>2만명</span>
                </div>
                <div class="calc-line">
                    <span>② 안산시 외국인 비율</span>
                    <span>13.2%</span>
                </div>
                <div class="calc-line">
                    <span>③ 안산시 추정 (최대) = ① × ②</span>
                    <span>2,640명</span>
                </div>
            </div>
            
            <div class="data-highlight">
                <strong>결론:</strong> 안산시 미등록 아동은 <strong>최소 814명 ~ 최대 2,640명</strong>으로 추정됩니다.<br>
                중간값 약 1,700명 기준으로 정책 분석을 진행했습니다.
            </div>
            
            <h4>⚠️ 추정의 한계</h4>
            <ul>
                <li>미등록 외국인의 특성상 정확한 통계 집계 불가</li>
                <li>안산시 외국인 밀집 특성상 전국 비율보다 높을 수 있음</li>
                <li>추후 실태조사를 통한 정확한 파악 필요</li>
            </ul>
            
            <h4>🔗 출처</h4>
            <a href="#" onclick="showSource('moj-stats'); return false;" class="source-link">
                📎 법무부 통계 확인
            </a>
            <a href="#" onclick="showSource('ngo-estimation'); return false;" class="source-link">
                📎 시민단체 추정 확인
            </a>
        `
    },

    'gyeonggi-pilot': {
        title: '경기도 미등록 아동 시범사업',
        content: `
            <h4>📊 사업 정보</h4>
            <div class="data-highlight">
                <strong>사업명:</strong> 미등록 외국인 아동 보육지원금 지원사업<br>
                <strong>시행일:</strong> 2026년 1월 1일<br>
                <strong>주관:</strong> 경기도
            </div>
            
            <h4>📋 사업 내용</h4>
            <ul>
                <li><strong>지원 대상:</strong> 도내 어린이집 재원 미등록 외국인 아동</li>
                <li><strong>지원 금액:</strong> 아동 1인당 월 10만원</li>
                <li><strong>지급 방식:</strong> 어린이집 직접 지원 (보호자 현금 지급 X)</li>
                <li><strong>법적 근거:</strong> 경기도 출생 미등록 아동 발굴 및 지원 조례 (2025년 제정)</li>
            </ul>
            
            <h4>📍 시범 지역 (2026년)</h4>
            <ul>
                <li>✅ 화성시</li>
                <li>✅ 안성시</li>
                <li>✅ 이천시</li>
                <li class="warning-box" style="margin-left: -16px; margin-top: 8px;">
                    ❌ <strong>안산시: 제외</strong> (외국인 비율 1위에도 불구)
                </li>
            </ul>
            
            <h4>🔗 출처</h4>
            <ul>
                <li>경기신문 (2025.12) "경기도, 광역 최초 미등록 외국인 아동 보육지원금 시행"</li>
                <li>에너지경제 (2025.12)</li>
                <li>뉴스1 (2025.12)</li>
                <li>경기도청 보도자료</li>
            </ul>
        `
    },

    'foreigner-ratio-comparison': {
        title: '지역별 외국인 비율 비교',
        content: `
            <h4>📊 비교 데이터</h4>
            <div class="data-highlight">
                <strong>기준:</strong> 각 지자체 외국인 주민 현황 (2024년 기준)
            </div>
            
            <h4>📈 경기도 주요 시군 외국인 비율</h4>
            <ul>
                <li><strong>안산시:</strong> 13.2% (96,300명) — 경기도 1위</li>
                <li><strong>시흥시:</strong> 약 10%</li>
                <li><strong>화성시:</strong> 약 7%</li>
                <li><strong>안성시:</strong> 약 5%</li>
                <li><strong>이천시:</strong> 약 4%</li>
            </ul>
            
            <div class="warning-box">
                <strong>⚠️ 핵심 역설:</strong><br>
                외국인 비율이 <strong>가장 높은 안산시</strong>가 경기도 미등록 아동 
                시범사업에서 <strong>제외</strong>되었습니다.
            </div>
            
            <h4>🔗 출처</h4>
            <ul>
                <li>각 시군 외국인 주민 현황 통계</li>
                <li>행정안전부 외국인 주민 현황 조사</li>
            </ul>
        `
    },

    'budget-estimation': {
        title: '사각지대 해소 예산 추정',
        content: `
            <h4>📊 계산 로직</h4>
            <div class="calculation-box">
                <div class="calc-line">
                    <span>① 미등록 아동 추정 (중간값)</span>
                    <span>1,700명</span>
                </div>
                <div class="calc-line">
                    <span>② 월 지원금 (경기도 기준)</span>
                    <span>10만원</span>
                </div>
                <div class="calc-line">
                    <span>③ 연간 지원 (12개월)</span>
                    <span>× 12</span>
                </div>
                <div class="calc-line">
                    <span>④ 연간 필요 예산</span>
                    <span>20.4억원</span>
                </div>
            </div>
            
            <h4>📈 현재 예산 대비 비교</h4>
            <ul>
                <li><strong>현재 외국인 보육료 예산 (2025):</strong> 72.84억원</li>
                <li><strong>사각지대 해소 추가 예산:</strong> 20.4억원</li>
                <li><strong>비율:</strong> 현재 예산의 약 28% 추가 필요</li>
            </ul>
            
            <div class="data-highlight">
                <strong>결론:</strong> 현재 예산에서 약 <strong>28% 추가</strong> 시 
                미등록 아동 사각지대 해소 가능
            </div>
            
            <h4>🔗 출처</h4>
            <a href="#" onclick="showSource('policy-document'); return false;" class="source-link">
                📎 안산시 예산 현황 확인
            </a>
            <a href="#" onclick="showSource('unregistered-estimation'); return false;" class="source-link">
                📎 미등록 아동 추정 확인
            </a>
        `
    },

    // 차트 데이터 출처
    'trend-data': {
        title: '외국인 인구 추이 데이터',
        content: `
            <h4>📊 데이터 정보</h4>
            <div class="data-highlight">
                <strong>기간:</strong> 2018년 ~ 2024년<br>
                <strong>출처:</strong> 안산시청 외국인 주민 현황 연도별 통계
            </div>
            
            <h4>📈 연도별 데이터</h4>
            <ul>
                <li>2018년: 78,500명 (11.1%)</li>
                <li>2019년: 82,000명 (11.5%)</li>
                <li>2020년: 79,000명 (11.0%) - 코로나19 영향</li>
                <li>2021년: 85,000명 (11.8%)</li>
                <li>2022년: 90,000명 (12.5%)</li>
                <li>2023년: 93,500명 (12.9%)</li>
                <li>2024년: 96,300명 (13.2%)</li>
            </ul>
            
            <h4>🔗 출처</h4>
            <ul>
                <li>안산시청 연도별 외국인 주민 현황</li>
                <li>행정안전부 외국인주민 현황 조사</li>
            </ul>
        `
    },

    'comparison-data': {
        title: '경기도 시군구 비교 데이터',
        content: `
            <h4>📊 데이터 정보</h4>
            <div class="data-highlight">
                <strong>기준:</strong> 2024년 각 시군구 외국인 주민 현황<br>
                <strong>시범사업:</strong> 경기도 미등록 아동 보육지원금 (2026.1)
            </div>
            
            <h4>📈 시군구별 비교</h4>
            <ul>
                <li><strong>안산시:</strong> 13.2% (96,300명) - ❌ 시범사업 제외</li>
                <li><strong>시흥시:</strong> 10.1% (47,500명) - 미참여</li>
                <li><strong>화성시:</strong> 7.0% (63,000명) - ✅ 시범사업 참여</li>
                <li><strong>평택시:</strong> 6.5% (37,000명) - 미참여</li>
                <li><strong>안성시:</strong> 5.0% (9,500명) - ✅ 시범사업 참여</li>
                <li><strong>이천시:</strong> 4.0% (8,800명) - ✅ 시범사업 참여</li>
            </ul>
            
            <div class="warning-box">
                <strong>⚠️ 핵심 역설:</strong><br>
                외국인 비율 1위인 안산시가 시범사업에서 제외되고,
                3위, 5위, 6위 도시가 참여함
            </div>
            
            <h4>🔗 출처</h4>
            <ul>
                <li>각 시군 외국인 주민 현황 통계</li>
                <li>경기도청 미등록 아동 보육지원금 보도자료 (2025.12)</li>
            </ul>
        `
    },

    'support-data': {
        title: '보육료 지원 현황 데이터',
        content: `
            <h4>📊 데이터 정보</h4>
            <div class="data-highlight">
                <strong>출처:</strong> 안산시 정책 문서 (3-3-48)<br>
                <strong>기준:</strong> 2024년 8월말
            </div>
            
            <h4>📈 지원 현황</h4>
            <ul>
                <li><strong>등록 아동 지원:</strong> 2,144명</li>
                <li><strong>연장보육 지원:</strong> 1,434명</li>
                <li><strong>미등록 아동 지원:</strong> 0명</li>
            </ul>
            
            <h4>💰 지원 금액</h4>
            <ul>
                <li><strong>0~2세:</strong> 월 26만원 (도비 10만 + 시비 16만)</li>
                <li><strong>3~5세:</strong> 월 28만원 (도비 10만 + 시비 18만)</li>
                <li><strong>미등록:</strong> 0원 (정책 대상 제외)</li>
            </ul>
            
            <h4>🔗 출처</h4>
            <a href="#" onclick="showSource('policy-document'); return false;" class="source-link">
                📎 안산시 정책 문서 확인
            </a>
        `
    },

    'wongok-data': {
        title: '원곡동 다문화특구 데이터',
        content: `
            <h4>📊 데이터 정보</h4>
            <div class="data-highlight">
                <strong>지역:</strong> 안산시 단원구 원곡동<br>
                <strong>기준:</strong> 2024년 1월
            </div>
            
            <h4>📈 주민 구성</h4>
            <ul>
                <li><strong>총 주민:</strong> 20,191명</li>
                <li><strong>외국인 주민:</strong> 18,014명 (89.2%)</li>
                <li><strong>내국인 주민:</strong> 2,177명 (10.8%)</li>
            </ul>
            
            <h4>🏫 원곡초등학교</h4>
            <ul>
                <li><strong>전체 학생:</strong> 449명</li>
                <li><strong>이주배경 학생:</strong> 443명 (98.6%)</li>
            </ul>
            
            <h4>🔗 출처</h4>
            <ul>
                <li>안산시청 다문화마을특구 현황 (2024.1)</li>
                <li>동아일보 (2024.1)</li>
                <li>동아일보 (2021) - 원곡초 학생 현황</li>
            </ul>
        `
    },

    'budget-data': {
        title: '예산 분석 데이터',
        content: `
            <h4>📊 데이터 정보</h4>
            <div class="data-highlight">
                <strong>출처:</strong> 안산시 정책 문서 (3-3-48)<br>
                <strong>기준:</strong> 2025년 예산
            </div>
            
            <h4>💰 현재 예산</h4>
            <ul>
                <li><strong>총 예산:</strong> 72.84억원</li>
                <li><strong>도비:</strong> 10.56억원 (14.5%)</li>
                <li><strong>시비:</strong> 62.28억원 (85.5%)</li>
            </ul>
            
            <h4>📊 사각지대 해소 필요 예산</h4>
            <div class="calculation-box">
                <div class="calc-line">
                    <span>미등록 아동 추정</span>
                    <span>1,700명</span>
                </div>
                <div class="calc-line">
                    <span>월 지원금</span>
                    <span>10만원</span>
                </div>
                <div class="calc-line">
                    <span>연간 필요 예산</span>
                    <span>20.4억원</span>
                </div>
            </div>
            
            <div class="data-highlight">
                <strong>결론:</strong> 현재 예산의 약 28% 추가 시 사각지대 해소 가능
            </div>
            
            <h4>🔗 출처</h4>
            <a href="#" onclick="showSource('policy-document'); return false;" class="source-link">
                📎 안산시 정책 문서 확인
            </a>
        `
    }
};

// =============================================
// 모달 기능
// =============================================
function showSource(sourceId) {
    const modal = document.getElementById('source-modal');
    const modalTitle = document.getElementById('modal-title');
    const modalBody = document.getElementById('modal-body');

    const source = sourceData[sourceId];

    if (source) {
        modalTitle.textContent = source.title;
        modalBody.innerHTML = source.content;
        modal.classList.add('active');
        document.body.style.overflow = 'hidden';
    }
}

function closeModal() {
    const modal = document.getElementById('source-modal');
    modal.classList.remove('active');
    document.body.style.overflow = '';
}

// ESC 키로 모달 닫기
document.addEventListener('keydown', (e) => {
    if (e.key === 'Escape') {
        closeModal();
    }
});

// =============================================
// DOM 로드 완료 후 실행
// =============================================
document.addEventListener('DOMContentLoaded', () => {
    // 로딩 화면 숨기기
    setTimeout(() => {
        const loadingScreen = document.getElementById('loading-screen');
        loadingScreen.classList.add('hidden');
    }, 1500);

    // 기능 초기화
    initCounters();
    initScrollAnimations();
    initMapInteraction();
    initGaugeAnimation();
});

/**
 * 카운터 애니메이션
 */
function initCounters() {
    const counters = document.querySelectorAll('.counter');

    const observerOptions = {
        root: null,
        rootMargin: '0px',
        threshold: 0.5
    };

    const counterObserver = new IntersectionObserver((entries) => {
        entries.forEach(entry => {
            if (entry.isIntersecting) {
                const counter = entry.target;
                animateCounter(counter);
                counterObserver.unobserve(counter);
            }
        });
    }, observerOptions);

    counters.forEach(counter => counterObserver.observe(counter));
}

function animateCounter(element) {
    const target = parseFloat(element.dataset.target);
    const duration = 2000;
    const start = 0;
    const startTime = performance.now();

    const isFloat = target % 1 !== 0;

    function update(currentTime) {
        const elapsed = currentTime - startTime;
        const progress = Math.min(elapsed / duration, 1);

        const easeProgress = 1 - Math.pow(1 - progress, 4);

        const current = start + (target - start) * easeProgress;

        if (isFloat) {
            element.textContent = current.toFixed(1);
        } else {
            element.textContent = Math.floor(current).toLocaleString();
        }

        if (progress < 1) {
            requestAnimationFrame(update);
        } else {
            if (isFloat) {
                element.textContent = target.toFixed(1);
            } else {
                element.textContent = target.toLocaleString();
            }
        }
    }

    requestAnimationFrame(update);
}

/**
 * 스크롤 기반 애니메이션
 */
function initScrollAnimations() {
    const animatedElements = document.querySelectorAll('.animate-on-scroll');

    const observerOptions = {
        root: null,
        rootMargin: '-50px',
        threshold: 0.1
    };

    const scrollObserver = new IntersectionObserver((entries) => {
        entries.forEach(entry => {
            if (entry.isIntersecting) {
                const element = entry.target;
                const delay = element.dataset.delay || 0;

                setTimeout(() => {
                    element.classList.add('visible');
                }, delay);

                scrollObserver.unobserve(element);
            }
        });
    }, observerOptions);

    animatedElements.forEach(el => scrollObserver.observe(el));
}

/**
 * 지도 인터랙션
 */
function initMapInteraction() {
    const map = document.getElementById('gyeonggi-map');
    const tooltip = document.getElementById('map-tooltip');

    if (!map || !tooltip) return;

    const regions = map.querySelectorAll('.region:not(.other)');

    regions.forEach(region => {
        region.addEventListener('mouseenter', (e) => {
            const name = region.dataset.name;
            const ratio = region.dataset.ratio;
            const status = region.dataset.status;

            let statusText = '';
            let statusClass = '';

            if (status === '참여') {
                statusText = '✅ 시범사업 참여';
                statusClass = 'included';
            } else if (status === '제외') {
                statusText = '❌ 시범사업 제외';
                statusClass = 'excluded';
            }

            tooltip.innerHTML = `
                <strong>${name}</strong><br>
                외국인 비율: ${ratio}%<br>
                <span class="status-${statusClass}">${statusText}</span>
            `;

            tooltip.style.opacity = '1';
        });

        region.addEventListener('mousemove', (e) => {
            const rect = map.getBoundingClientRect();
            tooltip.style.left = (e.clientX - rect.left + 10) + 'px';
            tooltip.style.top = (e.clientY - rect.top + 10) + 'px';
        });

        region.addEventListener('mouseleave', () => {
            tooltip.style.opacity = '0';
        });
    });
}

/**
 * 게이지 애니메이션
 */
function initGaugeAnimation() {
    const gaugeCard = document.querySelector('.gauge-card');

    if (!gaugeCard) return;

    const observerOptions = {
        root: null,
        rootMargin: '0px',
        threshold: 0.5
    };

    const gaugeObserver = new IntersectionObserver((entries) => {
        entries.forEach(entry => {
            if (entry.isIntersecting) {
                animateGauge();
                gaugeObserver.unobserve(entry.target);
            }
        });
    }, observerOptions);

    gaugeObserver.observe(gaugeCard);
}

function animateGauge() {
    const gaugeFill = document.querySelector('.gauge-fill');
    const needleLine = document.querySelector('.gauge-needle-line');

    if (!gaugeFill) return;

    setTimeout(() => {
        gaugeFill.style.strokeDashoffset = '75.36';
    }, 500);

    if (needleLine) {
        setTimeout(() => {
            const angle = -90 + (180 * 0.7);
            const centerX = 100;
            const centerY = 100;
            const length = 50;

            const radians = (angle - 90) * (Math.PI / 180);
            const endX = centerX + length * Math.cos(radians);
            const endY = centerY + length * Math.sin(radians);

            needleLine.setAttribute('x2', endX);
            needleLine.setAttribute('y2', endY);
        }, 800);
    }
}

/**
 * 스무스 스크롤
 */
document.querySelectorAll('a[href^="#"]').forEach(anchor => {
    anchor.addEventListener('click', function (e) {
        e.preventDefault();
        const target = document.querySelector(this.getAttribute('href'));
        if (target) {
            target.scrollIntoView({
                behavior: 'smooth',
                block: 'start'
            });
        }
    });
});

/**
 * 패럴랙스 효과
 */
window.addEventListener('scroll', () => {
    const hero = document.querySelector('.hero-section');
    if (!hero) return;

    const scrollY = window.scrollY;
    const heroHeight = hero.offsetHeight;

    if (scrollY < heroHeight) {
        const overlay = hero.querySelector('.hero-overlay');
        if (overlay) {
            overlay.style.opacity = 1 + (scrollY / heroHeight) * 0.5;
        }

        const content = hero.querySelector('.hero-content');
        if (content) {
            content.style.transform = `translateY(${scrollY * 0.3}px)`;
            content.style.opacity = 1 - (scrollY / heroHeight) * 0.8;
        }
    }
});

/**
 * 진행률 바 애니메이션
 */
function initBarAnimations() {
    const bars = document.querySelectorAll('.bar-fill, .ratio-fill');

    const observerOptions = {
        root: null,
        rootMargin: '0px',
        threshold: 0.1
    };

    const barObserver = new IntersectionObserver((entries) => {
        entries.forEach(entry => {
            if (entry.isIntersecting) {
                const bar = entry.target;
                const width = bar.style.width;
                bar.style.width = '0';

                setTimeout(() => {
                    bar.style.transition = 'width 1s ease-out';
                    bar.style.width = width;
                }, 100);

                barObserver.unobserve(bar);
            }
        });
    }, observerOptions);

    bars.forEach(bar => barObserver.observe(bar));
}

window.addEventListener('load', initBarAnimations);

/**
 * 도넛 차트 애니메이션
 */
function initDonutAnimation() {
    const donutSegments = document.querySelectorAll('.donut-segment');

    const observerOptions = {
        root: null,
        rootMargin: '0px',
        threshold: 0.5
    };

    const donutObserver = new IntersectionObserver((entries) => {
        entries.forEach(entry => {
            if (entry.isIntersecting) {
                donutSegments.forEach(segment => {
                    segment.style.transition = 'stroke-dasharray 1.5s ease-out';
                });
                donutObserver.unobserve(entry.target);
            }
        });
    }, observerOptions);

    const donutChart = document.getElementById('population-donut');
    if (donutChart) {
        donutObserver.observe(donutChart);
    }
}

window.addEventListener('load', initDonutAnimation);

/**
 * 키보드 네비게이션
 */
document.addEventListener('keydown', (e) => {
    if (e.key === 'ArrowDown' || e.key === 'ArrowUp') {
        const sections = document.querySelectorAll('.section');
        const currentScroll = window.scrollY + window.innerHeight / 2;

        let currentSectionIndex = 0;
        sections.forEach((section, index) => {
            if (section.offsetTop < currentScroll) {
                currentSectionIndex = index;
            }
        });

        if (e.key === 'ArrowDown' && currentSectionIndex < sections.length - 1) {
            sections[currentSectionIndex + 1].scrollIntoView({ behavior: 'smooth' });
        } else if (e.key === 'ArrowUp' && currentSectionIndex > 0) {
            sections[currentSectionIndex].scrollIntoView({ behavior: 'smooth' });
        }
    }
});

/**
 * 디바운스 유틸리티
 */
function debounce(func, wait) {
    let timeout;
    return function executedFunction(...args) {
        const later = () => {
            clearTimeout(timeout);
            func(...args);
        };
        clearTimeout(timeout);
        timeout = setTimeout(later, wait);
    };
}

const handleResize = debounce(() => {
    // 리사이즈 시 필요한 재계산
}, 250);

window.addEventListener('resize', handleResize);

/**
 * 인쇄 기능
 */
function printReport() {
    window.print();
}

window.printReport = printReport;

console.log('📊 안산시 외국인 보육료 정책 감사 대시보드가 로드되었습니다.');
console.log('� 출처 버튼을 클릭하면 데이터 출처를 확인할 수 있습니다.');
console.log('📅 2026년 1월 15일');
