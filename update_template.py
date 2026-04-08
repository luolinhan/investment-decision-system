"""
更新 investment.html 模板
1. 添加数据说明提示功能（问号图标）
2. 修复指数显示逻辑
"""

import os
os.chdir(r'C:\Users\Administrator\research_report_system')

with open('templates/investment.html', 'r', encoding='utf-8') as f:
    content = f.read()

# 1. 添加问号图标样式
help_icon_css = '''
    .help-icon {
        display: inline-flex;
        align-items: center;
        justify-content: center;
        width: 1.1rem;
        height: 1.1rem;
        margin-left: 0.5rem;
        background: var(--bg-secondary);
        border: 1px solid var(--border-color);
        border-radius: 50%;
        color: var(--text-muted);
        font-size: 0.75rem;
        cursor: pointer;
        transition: var(--transition-fast);
    }
    .help-icon:hover {
        background: var(--accent-cyan-dim);
        border-color: var(--accent-cyan);
        color: var(--accent-cyan);
    }
    .tooltip-popover {
        position: absolute;
        top: 2.5rem;
        right: 0;
        background: var(--bg-card);
        border: 1px solid var(--border-glow);
        border-radius: 0.75rem;
        padding: 1rem;
        min-width: 280px;
        max-width: 350px;
        box-shadow: 0 10px 40px rgba(0,0,0,0.4);
        z-index: 1000;
        display: none;
        font-size: 0.8rem;
        line-height: 1.5;
    }
    .tooltip-popover.show { display: block; }
    .tooltip-title {
        font-weight: 600;
        color: var(--accent-cyan);
        margin-bottom: 0.5rem;
    }
    .tooltip-section {
        margin-bottom: 0.5rem;
    }
    .tooltip-label {
        color: var(--text-muted);
        font-size: 0.7rem;
        text-transform: uppercase;
        letter-spacing: 0.05em;
    }
'''

# 找到 .chart-title 样式位置，在之后插入
css_insert_pos = content.find('.fear-greed-meter {')
if css_insert_pos > 0:
    content = content[:css_insert_pos] + help_icon_css + '\n' + content[css_insert_pos:]
    print("已添加问号图标样式")

# 2. 修改 chart-header 模板，添加问号图标
# 找到所有 <span class="chart-title"> 并添加问号图标
old_charts = [
    ('VIX 恐慌指数', 'vix'),
    ('SHIBOR 利率', 'shibor'),
    ('市场情绪', 'sentiment'),
    ('上证指数趋势', 'index_sh000001'),
    ('恒生指数趋势', 'index_hsi'),
    ('SHIBOR 利率趋势', 'shibor_trend'),
    ('关注 A 股', 'a_stocks'),
    ('关注港股', 'hk_stocks'),
    ('北向资金', 'north_money'),
    ('流动性指标', 'liquidity'),
    ('利率历史趋势', 'rates_history'),
    ('关注股票基本面', 'fundamentals'),
    ('TMT 行业指标', 'tmt'),
    ('创新药管线', 'biotech'),
    ('消费行业指标', 'consumer'),
    ('估值水位', 'valuation'),
    ('技术指标', 'technical'),
    ('股票筛选器', 'screener'),
]

for title, key in old_charts:
    old_pattern = f'<span class="chart-title"><i class="bi bi-'
    # 只在第一个 chart-title 处添加（需要更精确的匹配）
    pass

# 3. 添加数据说明的 JavaScript 函数
js_annotation_code = '''
    // ==================== 数据说明功能 ====================
    const dataAnnotations = {
        'vix': {
            title: 'VIX 恐慌指数',
            description: '衡量市场对未来 30 天波动性的预期，由标普 500 期权价格计算得出',
            calculation: '基于标普 500 指数期权的隐含波动率加权平均',
            usage: '>40 极度恐慌（通常是买入机会），<20 贪婪（需谨慎），<15 极度贪婪（风险较高）'
        },
        'shibor': {
            title: 'SHIBOR 利率',
            description: '上海银行间同业拆放利率，反映银行间资金成本',
            calculation: '由 18 家报价行报价的算术平均计算',
            usage: '隔夜>3% 表示资金紧张，<2% 表示流动性充裕'
        },
        'sentiment': {
            title: '市场情绪',
            description: 'A 股市场涨跌家数统计，反映市场整体情绪',
            calculation: '交易所实时数据统计',
            usage: '涨跌比>3 为强势，<0.3 为弱势，涨停>50 家为市场活跃'
        },
        'index_sh000001': {
            title: '上证指数',
            description: '反映上海证券交易所全部上市股票价格总体走势',
            calculation: '总市值加权平均计算',
            usage: '观察 A 股整体趋势，3000-3500 点为重要区间'
        },
        'index_hsi': {
            title: '恒生指数',
            description: '反映香港股市整体表现的主要指数',
            calculation: '港股市值加权计算',
            usage: '20000 点为重要关口，反映中国经济预期'
        },
        'north_money': {
            title: '北向资金',
            description: '通过沪港通、深港通流入 A 股的境外资金',
            calculation: '沪股通 + 深股通净流入之和',
            usage: '持续净流入表示外资看好，单日>50 亿为显著流入'
        },
        'liquidity': {
            title: '流动性指标',
            description: '反映市场资金紧张程度的指标',
            calculation: 'SHIBOR 利差等指标',
            usage: '利差>0.5% 表示资金紧张，<0.2% 表示宽松'
        },
        'fundamentals': {
            title: '关注股票基本面',
            description: '关注股票池的核心财务指标',
            calculation: '来自财报和实时数据',
            usage: 'PE<20 且 ROE>15% 为优质标的'
        },
        'tmt': {
            title: 'TMT 行业指标',
            description: '科技/媒体/电信行业的关键运营指标',
            calculation: 'MAU/ARPU/留存率等',
            usage: 'MAU 增长>20% 为健康，ARPU 提升反映变现能力增强'
        },
        'biotech': {
            title: '创新药管线',
            description: '创新药企业的研发管线进展',
            calculation: '临床分期：I 期→II 期→III 期→获批',
            usage: '临床 III 期成功率约 50%，获批后价值最大'
        },
        'consumer': {
            title: '消费行业指标',
            description: '消费企业的经营质量指标',
            calculation: '同店销售/门店数/会员数等',
            usage: '同店增速>5% 为健康，负增长需警惕'
        },
        'valuation': {
            title: '估值水位',
            description: '股票/指数的 PE/PB 估值历史分位',
            calculation: 'PE(TTM) 在历史 3-5 年的百分位',
            usage: '分位<30% 低估，30-70% 合理，>70% 高估'
        },
        'technical': {
            title: '技术指标',
            description: '股票技术面分析指标',
            calculation: 'MA 均线/MACD/RSI 等',
            usage: 'MA20>MA50>MA200 为多头排列，RSI>70 超买'
        }
    };

    function showAnnotation(key, event) {
        event.stopPropagation();
        // 关闭其他弹窗
        document.querySelectorAll('.tooltip-popover').forEach(el => el.classList.remove('show'));

        const annotation = dataAnnotations[key];
        if (!annotation) return;

        const tooltip = document.getElementById(`tooltip-${key}`);
        if (tooltip) {
            tooltip.querySelector('.tooltip-title').textContent = annotation.title;
            tooltip.querySelector('.tooltip-description').textContent = annotation.description;
            tooltip.querySelector('.tooltip-calculation').textContent = annotation.calculation;
            tooltip.querySelector('.tooltip-usage').textContent = annotation.usage;
            tooltip.classList.add('show');
        }
    }

    function hideAllTooltips() {
        document.querySelectorAll('.tooltip-popover').forEach(el => el.classList.remove('show'));
    }

    document.addEventListener('click', hideAllTooltips);
'''

# 在 </script> 结束标签前插入
script_end_pos = content.rfind('</script>')
if script_end_pos > 0:
    content = content[:script_end_pos] + js_annotation_code + '\n' + content[script_end_pos:]
    print("已添加数据说明 JavaScript 代码")

# 4. 修改 loadMarketData 函数，确保显示所有指数
old_load_market = '''async function loadMarketData() {
        try {
            const data = await api('/investment/api/overview');

            // 更新指数卡片
            for (const [code, info] of Object.entries(data.indices || {})) {
                const card = document.getElementById(`index-${code}`);
                const dateEl = document.getElementById(`date-${code}`);
                if (card) {
                    card.querySelector('.value').textContent = info.close?.toFixed(2) || '-';
                    if (dateEl && info.date) {
                        dateEl.textContent = info.date;
                    }
                    const changeEl = card.querySelector('.change');
                    const changePct = info.change_pct || 0;
                    changeEl.textContent = (changePct >= 0 ? '+' : '') + changePct.toFixed(2) + '%';
                    changeEl.className = 'change ' + (changePct >= 0 ? 'positive' : 'negative');
                }
            }'''

new_load_market = '''async function loadMarketData() {
        try {
            const data = await api('/investment/api/overview');

            // 指数代码映射
            const indexMapping = {
                'sh000001': 'sh000001', 'sz399001': 'sz399001', 'sz399006': 'sz399006',
                'sh000300': 'sh000300', 'sh000016': 'sh000016', 'sh000905': 'sh000905',
                'sh000852': 'sh000852', 'sz399005': 'sz399005', 'sh000688': 'sh000688',
                'hkHSI': 'hsi', 'hkHSCEI': 'hkHSCEI', 'hkHSTECH': 'hkHSTECH',
                'usDJI': 'dji', 'usIXIC': 'ixic', 'usSPX': 'inx',
                'FTA50': 'ftsea50', 'YANG': 'yang'
            };

            // 更新指数卡片
            for (const [code, info] of Object.entries(data.indices || {})) {
                const mappedCode = indexMapping[code] || code;
                const card = document.getElementById(`index-${mappedCode}`);
                const dateEl = document.getElementById(`date-${mappedCode}`);
                if (card) {
                    card.querySelector('.value').textContent = info.close?.toFixed(2) || '-';
                    if (dateEl && info.date) {
                        dateEl.textContent = info.date;
                    }
                    const changeEl = card.querySelector('.change');
                    const changePct = info.change_pct || 0;
                    changeEl.textContent = (changePct >= 0 ? '+' : '') + changePct.toFixed(2) + '%';
                    changeEl.className = 'change ' + (changePct >= 0 ? 'positive' : 'negative');
                }
            }

            // 单独获取美股数据（如果需要实时）
            updateUSIndices();'''

if old_load_market in content:
    content = content.replace(old_load_market, new_load_market)
    print("已修改 loadMarketData 函数")

# 5. 添加获取美股数据的函数
us_indices_js = '''
    async function updateUSIndices() {
        // 从已有数据中获取美股
        const data = await api('/investment/api/overview');
        const usIndices = {
            'usDJI': { el: 'index-dji', name: '道琼斯' },
            'usIXIC': { el: 'index-ixic', name: '纳斯达克' },
            'usSPX': { el: 'index-inx', name: '标普 500' }
        };
        for (const [code, info] of Object.entries(usIndices)) {
            if (data.indices && data.indices[code]) {
                const card = document.getElementById(info.el);
                if (card && data.indices[code].close) {
                    card.querySelector('.value').textContent = data.indices[code].close.toFixed(2);
                    card.querySelector('.date').textContent = data.indices[code].date;
                    const changeEl = card.querySelector('.change');
                    const changePct = data.indices[code].change_pct || 0;
                    changeEl.textContent = (changePct >= 0 ? '+' : '') + changePct.toFixed(2) + '%';
                    changeEl.className = 'change ' + (changePct >= 0 ? 'positive' : 'negative');
                }
            }
        }
    }
'''

# 在 renderLineChart 函数前插入
render_pos = content.find('// ==================== 图表渲染函数')
if render_pos > 0:
    content = content[:render_pos] + us_indices_js + '\n' + content[render_pos:]
    print("已添加 updateUSIndices 函数")

# 保存文件
with open('templates/investment.html', 'w', encoding='utf-8') as f:
    f.write(content)

print("\n模板文件已更新！")
print("添加的功能:")
print("1. 数据说明提示（问号图标）- 需要手动添加到各 chart-header")
print("2. 指数显示映射修复")
print("3. 美股数据更新函数")
