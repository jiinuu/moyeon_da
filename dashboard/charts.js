/**
 * 안산시 외국인 보육료 정책 감사 대시보드
 * Chart.js 기반 데이터 시각화
 */

// 차트 데이터 로드
let chartData = null;

// 데이터 로드 함수
async function loadChartData() {
    try {
        const response = await fetch('./data/chart_data.json');
        chartData = await response.json();
        console.log('📊 차트 데이터 로드 완료:', chartData);
        return chartData;
    } catch (error) {
        console.error('❌ 데이터 로드 실패:', error);
        // 폴백 데이터
        return getFallbackData();
    }
}

// 폴백 데이터 (JSON 로드 실패 시)
function getFallbackData() {
    return {
        trend: [
            { year: 2018, foreign_population: 78500, ratio: 11.1 },
            { year: 2019, foreign_population: 82000, ratio: 11.5 },
            { year: 2020, foreign_population: 79000, ratio: 11.0 },
            { year: 2021, foreign_population: 85000, ratio: 11.8 },
            { year: 2022, foreign_population: 90000, ratio: 12.5 },
            { year: 2023, foreign_population: 93500, ratio: 12.9 },
            { year: 2024, foreign_population: 96300, ratio: 13.2 }
        ],
        comparison: [
            { region: "안산시", foreign_ratio: 13.2, pilot_program: false },
            { region: "시흥시", foreign_ratio: 10.1, pilot_program: false },
            { region: "화성시", foreign_ratio: 7.0, pilot_program: true },
            { region: "수원시", foreign_ratio: 4.8, pilot_program: false },
            { region: "안성시", foreign_ratio: 5.0, pilot_program: true },
            { region: "이천시", foreign_ratio: 4.0, pilot_program: true }
        ],
        support: {
            registered_children_supported: 2144,
            estimated_unregistered_min: 814,
            estimated_unregistered_max: 2640
        },
        perception: [
            { category: "외국인 지원 수준", perception: "선도적", reality: "미등록 아동 0% 지원", gap_severity: "critical" },
            { category: "시범사업 참여", perception: "당연히 포함", reality: "경기도 사업에서 제외", gap_severity: "critical" },
            { category: "정책 형평성", perception: "내외국인 차별 없음", reality: "'등록' 외국인만 대상", gap_severity: "high" },
            { category: "미등록 아동 규모", perception: "소수 (수십명)", reality: "최소 814명 ~ 최대 2,640명", gap_severity: "critical" }
        ],
        wongok: {
            foreign_residents: 18014,
            korean_residents: 2177,
            foreign_ratio: 89.2
        },
        budget: {
            current_budget: 7284000000,
            additional_needed_for_unregistered: 2040000000
        },
        timeline: [
            { date: "2018.07", event: "외국인아동 보육료 지원 시작", type: "positive" },
            { date: "2024.12", event: "행안부 우수상 수상", type: "positive" },
            { date: "2026.01", event: "경기도 시범사업에서 제외", type: "negative" }
        ]
    };
}

// 차트 색상 팔레트
const colors = {
    primary: '#6366f1',
    primaryLight: '#818cf8',
    success: '#22c55e',
    warning: '#f59e0b',
    danger: '#ef4444',
    muted: '#64748b',
    background: 'rgba(255, 255, 255, 0.03)'
};

// Chart.js 전역 설정
Chart.defaults.color = '#94a3b8';
Chart.defaults.font.family = "'Noto Sans KR', sans-serif";

/**
 * 외국인 인구 추이 차트
 */
function createTrendChart(data) {
    const ctx = document.getElementById('trend-chart');
    if (!ctx) return;

    const trendData = data.trend;

    new Chart(ctx, {
        type: 'line',
        data: {
            labels: trendData.map(d => d.year),
            datasets: [
                {
                    label: '외국인 인구 (명)',
                    data: trendData.map(d => d.foreign_population),
                    borderColor: colors.primary,
                    backgroundColor: `${colors.primary}20`,
                    fill: true,
                    tension: 0.4,
                    yAxisID: 'y'
                },
                {
                    label: '외국인 비율 (%)',
                    data: trendData.map(d => d.ratio),
                    borderColor: colors.warning,
                    backgroundColor: 'transparent',
                    borderDash: [5, 5],
                    tension: 0.4,
                    yAxisID: 'y1'
                }
            ]
        },
        options: {
            responsive: true,
            interaction: {
                mode: 'index',
                intersect: false,
            },
            plugins: {
                legend: {
                    position: 'top',
                },
                tooltip: {
                    backgroundColor: 'rgba(15, 15, 23, 0.9)',
                    titleColor: '#fff',
                    bodyColor: '#94a3b8',
                    borderColor: 'rgba(255, 255, 255, 0.1)',
                    borderWidth: 1
                }
            },
            scales: {
                y: {
                    type: 'linear',
                    display: true,
                    position: 'left',
                    title: {
                        display: true,
                        text: '외국인 인구 (명)'
                    },
                    grid: {
                        color: 'rgba(255, 255, 255, 0.05)'
                    }
                },
                y1: {
                    type: 'linear',
                    display: true,
                    position: 'right',
                    title: {
                        display: true,
                        text: '비율 (%)'
                    },
                    grid: {
                        drawOnChartArea: false,
                    },
                    min: 10,
                    max: 15
                },
                x: {
                    grid: {
                        color: 'rgba(255, 255, 255, 0.05)'
                    }
                }
            }
        }
    });
}

/**
 * 경기도 시군구 비교 차트
 */
function createComparisonChart(data) {
    const ctx = document.getElementById('comparison-chart');
    if (!ctx) return;

    const compData = data.comparison;

    // 외국인 비율 순으로 정렬
    const sorted = [...compData].sort((a, b) => b.foreign_ratio - a.foreign_ratio);

    new Chart(ctx, {
        type: 'bar',
        data: {
            labels: sorted.map(d => d.region),
            datasets: [{
                label: '외국인 비율 (%)',
                data: sorted.map(d => d.foreign_ratio),
                backgroundColor: sorted.map(d => {
                    if (d.region === '안산시') return colors.danger;
                    if (d.pilot_program) return colors.success;
                    return colors.muted;
                }),
                borderColor: sorted.map(d => {
                    if (d.region === '안산시') return colors.danger;
                    if (d.pilot_program) return colors.success;
                    return colors.muted;
                }),
                borderWidth: 1,
                borderRadius: 4
            }]
        },
        options: {
            responsive: true,
            indexAxis: 'y',
            plugins: {
                legend: {
                    display: false
                },
                tooltip: {
                    backgroundColor: 'rgba(15, 15, 23, 0.9)',
                    callbacks: {
                        afterLabel: function (context) {
                            const item = sorted[context.dataIndex];
                            if (item.region === '안산시') return '❌ 시범사업 제외';
                            if (item.pilot_program) return '✅ 시범사업 참여';
                            return '';
                        }
                    }
                }
            },
            scales: {
                x: {
                    grid: {
                        color: 'rgba(255, 255, 255, 0.05)'
                    },
                    title: {
                        display: true,
                        text: '외국인 비율 (%)'
                    }
                },
                y: {
                    grid: {
                        display: false
                    }
                }
            }
        }
    });
}

/**
 * 보육료 지원 현황 차트
 */
function createSupportChart(data) {
    const ctx = document.getElementById('support-chart');
    if (!ctx) return;

    const supportData = data.support;

    new Chart(ctx, {
        type: 'bar',
        data: {
            labels: ['등록 아동\n(지원 중)', '미등록 아동\n(추정)', '미등록 아동\n(지원)'],
            datasets: [{
                label: '아동 수 (명)',
                data: [
                    supportData.registered_children_supported,
                    Math.round((supportData.estimated_unregistered_min + supportData.estimated_unregistered_max) / 2),
                    0
                ],
                backgroundColor: [
                    colors.success,
                    colors.warning,
                    colors.danger
                ],
                borderColor: [
                    colors.success,
                    colors.warning,
                    colors.danger
                ],
                borderWidth: 2,
                borderRadius: 8
            }]
        },
        options: {
            responsive: true,
            plugins: {
                legend: {
                    display: false
                },
                tooltip: {
                    backgroundColor: 'rgba(15, 15, 23, 0.9)'
                }
            },
            scales: {
                y: {
                    beginAtZero: true,
                    grid: {
                        color: 'rgba(255, 255, 255, 0.05)'
                    },
                    title: {
                        display: true,
                        text: '아동 수 (명)'
                    }
                },
                x: {
                    grid: {
                        display: false
                    }
                }
            }
        }
    });
}

/**
 * 원곡동 도넛 차트
 */
function createWongokChart(data) {
    const ctx = document.getElementById('wongok-chart');
    if (!ctx) return;

    const wongok = data.wongok;

    new Chart(ctx, {
        type: 'doughnut',
        data: {
            labels: ['외국인 주민', '내국인 주민'],
            datasets: [{
                data: [wongok.foreign_residents, wongok.korean_residents],
                backgroundColor: [colors.primary, colors.muted],
                borderColor: ['transparent', 'transparent'],
                borderWidth: 0
            }]
        },
        options: {
            responsive: true,
            cutout: '70%',
            plugins: {
                legend: {
                    position: 'bottom'
                },
                tooltip: {
                    backgroundColor: 'rgba(15, 15, 23, 0.9)',
                    callbacks: {
                        label: function (context) {
                            const total = wongok.foreign_residents + wongok.korean_residents;
                            const percentage = ((context.raw / total) * 100).toFixed(1);
                            return `${context.label}: ${context.raw.toLocaleString()}명 (${percentage}%)`;
                        }
                    }
                }
            }
        },
        plugins: [{
            id: 'centerText',
            afterDraw: function (chart) {
                const ctx = chart.ctx;
                const centerX = (chart.chartArea.left + chart.chartArea.right) / 2;
                const centerY = (chart.chartArea.top + chart.chartArea.bottom) / 2;

                ctx.save();
                ctx.textAlign = 'center';
                ctx.textBaseline = 'middle';
                ctx.font = 'bold 32px "Noto Sans KR"';
                ctx.fillStyle = colors.primaryLight;
                ctx.fillText('89.2%', centerX, centerY - 10);
                ctx.font = '14px "Noto Sans KR"';
                ctx.fillStyle = '#94a3b8';
                ctx.fillText('외국인', centerX, centerY + 20);
                ctx.restore();
            }
        }]
    });
}

/**
 * 예산 차트
 */
function createBudgetChart(data) {
    const ctx = document.getElementById('budget-chart');
    if (!ctx) return;

    const budget = data.budget;
    const current = budget.current_budget / 100000000; // 억원 변환
    const additional = budget.additional_needed_for_unregistered / 100000000;

    new Chart(ctx, {
        type: 'bar',
        data: {
            labels: ['현재 예산', '추가 필요'],
            datasets: [{
                label: '예산 (억원)',
                data: [current, additional],
                backgroundColor: [colors.primary, colors.success],
                borderColor: [colors.primary, colors.success],
                borderWidth: 2,
                borderRadius: 8
            }]
        },
        options: {
            responsive: true,
            plugins: {
                legend: {
                    display: false
                },
                tooltip: {
                    backgroundColor: 'rgba(15, 15, 23, 0.9)',
                    callbacks: {
                        label: function (context) {
                            return `${context.raw.toFixed(1)}억원`;
                        }
                    }
                }
            },
            scales: {
                y: {
                    beginAtZero: true,
                    grid: {
                        color: 'rgba(255, 255, 255, 0.05)'
                    },
                    title: {
                        display: true,
                        text: '예산 (억원)'
                    }
                },
                x: {
                    grid: {
                        display: false
                    }
                }
            }
        }
    });
}

/**
 * 선입견 vs 현실 카드 생성
 */
function createPerceptionCards(data) {
    const container = document.getElementById('perception-cards');
    if (!container) return;

    const perceptions = data.perception;

    container.innerHTML = perceptions.map(item => `
        <div class="perception-card ${item.gap_severity}">
            <div class="perception-header">
                <span class="perception-category">${item.category}</span>
                <span class="severity-badge ${item.gap_severity}">
                    ${item.gap_severity === 'critical' ? '⚠️ 심각' : '⚡ 높음'}
                </span>
            </div>
            <div class="perception-compare">
                <div class="perception-box before">
                    <div class="perception-label">당신의 생각</div>
                    <div class="perception-value">${item.perception}</div>
                </div>
                <div class="perception-arrow">→</div>
                <div class="perception-box after">
                    <div class="perception-label">실제 현실</div>
                    <div class="perception-value">${item.reality}</div>
                </div>
            </div>
        </div>
    `).join('');
}

/**
 * 타임라인 생성
 */
function createTimeline(data) {
    const container = document.getElementById('policy-timeline');
    if (!container) return;

    const timeline = data.timeline;

    container.innerHTML = timeline.map(item => `
        <div class="timeline-event ${item.type}">
            <div class="timeline-date">${item.date}</div>
            <div class="timeline-text">${item.event}</div>
        </div>
    `).join('');
}

/**
 * 모든 차트 초기화
 */
async function initAllCharts() {
    const data = await loadChartData();

    // 차트 생성
    createTrendChart(data);
    createComparisonChart(data);
    createSupportChart(data);
    createWongokChart(data);
    createBudgetChart(data);

    // 동적 컨텐츠 생성
    createPerceptionCards(data);
    createTimeline(data);

    console.log('✅ 모든 차트 초기화 완료');
}

// 페이지 로드 시 실행
document.addEventListener('DOMContentLoaded', () => {
    // 약간의 딜레이 후 차트 초기화 (로딩 애니메이션용)
    setTimeout(initAllCharts, 500);
});
