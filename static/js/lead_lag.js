(function () {
    const API_PREFIX = '/investment/api/lead-lag';
    const ENDPOINTS = {
        v2: {
            decisionCenter: `${API_PREFIX}/decision-center`,
            opportunityQueue: `${API_PREFIX}/opportunity-queue`,
            whatChanged: `${API_PREFIX}/what-changed`,
            eventFrontline: `${API_PREFIX}/event-frontline`,
            avoidBoard: `${API_PREFIX}/avoid-board`,
            transmissionWorkspace: `${API_PREFIX}/transmission-workspace`,
            replayDiagnostics: `${API_PREFIX}/replay-diagnostics`,
            memoryActions: `${API_PREFIX}/research-memory/actions`,
            sectorEvidence: `${API_PREFIX}/sector-evidence`,
            sourceQualityLineage: `${API_PREFIX}/source-quality-lineage`,
            reportCenter: `${API_PREFIX}/report-center`,
            opportunityUniverse: `${API_PREFIX}/opportunity-universe`,
        },
        v1: {
            overview: `${API_PREFIX}/overview`,
            models: `${API_PREFIX}/models`,
            opportunities: `${API_PREFIX}/opportunities`,
            crossMarket: `${API_PREFIX}/cross-market-map`,
            transmission: `${API_PREFIX}/industry-transmission`,
            liquidity: `${API_PREFIX}/liquidity`,
            thesis: `${API_PREFIX}/sector-thesis`,
            events: `${API_PREFIX}/events-calendar`,
            validation: `${API_PREFIX}/replay-validation`,
            memory: `${API_PREFIX}/obsidian-memory`,
        },
    };

    const state = {
        filters: {
            as_of: '',
            region: 'all',
            regime: 'all',
            family: 'all',
            q: '',
            include_research_facing: false,
            include_sample: false,
            live_only: false,
            archived_only: false,
        },
        data: {},
        v2Fallbacks: new Set(),
        lastUpdatedAt: null,
        loading: false,
    };

    const EMPTY_OVERVIEW = {
        headline: '领先-传导 Alpha 引擎',
        summary: '等待领先-传导接口返回。',
        flags: [],
        metrics: [],
        regions: ['all', 'CN', 'HK', 'US'],
        regimes: ['all', 'pre_trigger', 'first_baton', 'second_baton', 'validation_baton', 'crowded', 'invalidated'],
        as_of: '',
        status_text: '等待加载',
    };

    const LABELS = {
        all: '全部',
        CN: '中国资产',
        HK: '港股',
        US: '美股',
        A: 'A股',
        ETF: 'ETF',
        CrossMarket: '跨市场',
        pre_trigger: '预触发',
        first_baton: '第一棒',
        second_baton: '第二棒',
        third_baton: '第三棒',
        validation_baton: '验证棒',
        crowded: '拥挤',
        invalidated: '已失效',
        latent: '潜伏',
        triggered: '已触发',
        validating: '验证中',
        decaying: '衰减',
        actionable: '可执行',
        watch_only: '观察',
        insufficient_evidence: '证据不足',
        live: '实时',
        cached: '缓存',
        sample_fallback: '样例回退',
        live_fusion: '实时融合',
        market_facing: '可交易催化',
        'market-facing': '可交易催化',
        research_facing: '研究背景',
        'research-facing': '研究背景',
        conservative: '保守',
        balanced: '均衡',
        aggressive: '积极',
        no_new_risk: '不新增风险',
        leadership_breadth: '龙头扩散模型',
        event_spillover: '事件溢出模型',
        transmission_graph: '传导图谱模型',
        liquidity_dispersion: '流动性与拥挤模型',
        earnings_revision: '业绩修正模型',
        policy_surprise: '政策意外模型',
        valuation_gap: '估值差模型',
        replay_validation: '历史回放模型',
        breadth_thrust: '市场宽度模型',
        memory_alignment: '研究记忆模型',
        leader: '领先资产',
        bridge: '桥接资产',
        local_mapping: '本地映射',
        proxy: '同赛道代理',
        hedge: '对冲',
        live_official: '官方 live',
        live_public: '公开 live',
        live_media: '媒体 live',
        user_curated: '人工整理',
        generated_inference: '模型推断',
        sample_demo: '样例数据',
        fallback_placeholder: '回退占位',
        cross_market_mapping: '跨市场映射',
        industry_transmission: '产业传导',
        customer_capex_spillover: '客户资本开支外溢',
        price_spread_pass_through: '价差传导',
        inventory_destocking_cycle: '库存周期',
        policy_credit_fiscal: '政策/信用/财政',
        external_liquidity_bridge: '外部流动性桥',
        earnings_revision: '业绩修正',
        event_calendar: '事件日历',
        clinical_approval_bd: '临床/审批/BD',
        crowding_short_squeeze: '拥挤/挤空',
        valuation_gap: '估值差',
        seasonal_calendar: '季节性',
        entity_specific_dislocation: '个体错位',
        confirmed: '已确认',
        partial: '部分确认',
        missing: '缺失',
        stale: '陈旧',
        sample_only: '样例',
    };

    const CHAIN_LABELS = [
        ['result', '结果'],
        ['thinking', '思路'],
        ['strategy', '策略'],
        ['evidence', '依据'],
        ['data', '数据'],
    ];

    function displayLabel(value) {
        if (!hasValue(value)) return '';
        const text = String(value);
        return LABELS[text] || LABELS[text.replaceAll('-', '_')] || text;
    }

    function escapeHtml(value) {
        return String(value ?? '')
            .replaceAll('&', '&amp;')
            .replaceAll('<', '&lt;')
            .replaceAll('>', '&gt;')
            .replaceAll('"', '&quot;')
            .replaceAll("'", '&#39;');
    }

    function hasValue(value) {
        if (value === null || value === undefined) return false;
        if (Array.isArray(value)) return value.length > 0;
        if (typeof value === 'object') return Object.keys(value).length > 0;
        const normalized = String(value).trim().toLowerCase();
        return normalized !== '' && normalized !== '-' && normalized !== 'n/a' && normalized !== 'na' && normalized !== 'unknown';
    }

    function compact(value) {
        return hasValue(value) ? String(value).trim() : '';
    }

    function asArray(value) {
        if (Array.isArray(value)) return value.filter(hasValue);
        if (hasValue(value)) return [value];
        return [];
    }

    function asPercent(value) {
        if (!hasValue(value)) return '';
        if (typeof value === 'string') return value.includes('%') ? value : value;
        const number = Number(value);
        if (!Number.isFinite(number)) return '';
        return number <= 1 ? `${Math.round(number * 100)}%` : `${Math.round(number)}%`;
    }

    function scoreValue(...values) {
        for (const value of values) {
            if (hasValue(value)) return value;
        }
        return '';
    }

    function formatDate(value) {
        if (!hasValue(value)) return '';
        const date = new Date(value);
        if (Number.isNaN(date.getTime())) return String(value);
        return date.toLocaleString('zh-CN', { month: '2-digit', day: '2-digit', hour: '2-digit', minute: '2-digit' });
    }

    function toneClass(value) {
        const normalized = String(value || '').toLowerCase();
        if (/(actionable|positive|bull|long|up|high|supportive|confirm|green|强|多|正|good|excellent|balanced|aggressive)/.test(normalized)) return 'positive';
        if (/(watch|warning|mixed|neutral|flat|mid|medium|pre_trigger|validating|balanced|中|平|观望|acceptable)/.test(normalized)) return 'warning';
        if (/(insufficient|invalidated|negative|bear|short|down|risk|red|weak|poor|crowded|conservative|no_new_risk|missing|stale|弱|空|负)/.test(normalized)) return 'negative';
        return 'neutral';
    }

    function cloneEmpty() {
        const tpl = document.getElementById('leadLagEmptyState');
        return tpl ? tpl.content.cloneNode(true) : document.createTextNode('');
    }

    function renderEmpty(id) {
        const root = document.getElementById(id);
        if (!root) return;
        root.innerHTML = '';
        root.appendChild(cloneEmpty());
    }

    function setText(id, value) {
        const node = document.getElementById(id);
        if (node) node.textContent = hasValue(value) ? String(value) : '';
    }

    function setLoading(isLoading) {
        state.loading = isLoading;
        const btn = document.getElementById('leadLagRefreshBtn');
        if (btn) {
            btn.disabled = isLoading;
            btn.innerHTML = isLoading
                ? '<i class="bi bi-arrow-repeat"></i> 加载中'
                : '<i class="bi bi-arrow-clockwise"></i> 刷新';
        }
        setText('globalStatusText', isLoading ? '加载中' : buildStatusText());
    }

    function buildStatusText() {
        const overview = state.data.overview || {};
        const fallbackCount = state.v2Fallbacks.size;
        if (fallbackCount) return `V2 未完全接入，${fallbackCount} 个操作区块使用 V1 兼容推导`;
        if (overview.status_text) return overview.status_text;
        if (state.lastUpdatedAt) return `最近刷新 ${formatDate(state.lastUpdatedAt)}`;
        return '待加载';
    }

    function buildQuery(extra = {}) {
        const params = new URLSearchParams();
        Object.entries({ ...state.filters, ...extra }).forEach(([key, value]) => {
            if (value && value !== 'all' && value !== false) params.set(key, value);
        });
        const query = params.toString();
        return query ? `?${query}` : '';
    }

    async function fetchJSON(url, extra = {}) {
        const response = await fetch(`${url}${buildQuery(extra)}`, {
            headers: { 'Content-Type': 'application/json' },
        });
        if (!response.ok) throw new Error(`HTTP ${response.status}`);
        return response.json();
    }

    function unwrapPayload(payload, fallback) {
        if (payload && typeof payload === 'object' && 'data' in payload) return payload.data ?? fallback;
        return payload ?? fallback;
    }

    async function fetchSection(url, fallback, extra = {}) {
        try {
            return unwrapPayload(await fetchJSON(url, extra), fallback);
        } catch (error) {
            console.warn(`[lead-lag] failed to load ${url}`, error);
            return fallback;
        }
    }

    async function fetchV2WithFallback(key, v2Url, v1Url, fallback, mapper, extra = {}) {
        try {
            const payload = unwrapPayload(await fetchJSON(v2Url, extra), fallback);
            state.v2Fallbacks.delete(key);
            return payload;
        } catch (error) {
            console.warn(`[lead-lag] V2 ${key} unavailable, using V1 compatibility`, error);
            state.v2Fallbacks.add(key);
            const v1Payload = await fetchSection(v1Url, fallback, extra);
            return mapper(v1Payload);
        }
    }

    function fillSelect(id, items, currentValue) {
        const select = document.getElementById(id);
        if (!select) return;
        const normalized = Array.from(new Set((items || []).filter(hasValue)));
        const values = normalized.includes('all') ? normalized : ['all', ...normalized];
        select.innerHTML = values.map((item) => `
            <option value="${escapeHtml(item)}">${escapeHtml(displayLabel(item))}</option>
        `).join('');
        select.value = currentValue && values.includes(currentValue) ? currentValue : 'all';
    }

    function v2Missing(key, reason) {
        return {
            generation_status: 'insufficient_evidence',
            missing_confirmations: [`缺少验证：${reason}`, `V2 ${key} 接口尚未返回正式契约`],
            missing_evidence_reason: `前端已回退到 V1 兼容数据，${reason}`,
            cache_status: 'sample_fallback',
        };
    }

    function assetLabel(asset) {
        if (!asset || typeof asset !== 'object') return '';
        const name = compact(asset.name || asset.asset_name || asset.label);
        const code = compact(asset.code || asset.symbol || asset.asset_code);
        const market = compact(asset.market_zh || displayLabel(asset.market));
        return [name, code, market].filter(Boolean).join(' / ');
    }

    function mapV1Opportunity(item) {
        const missing = [];
        if (!hasValue(item.driver)) missing.push('缺少验证：driver 未返回');
        if (!hasValue(item.confirmation)) missing.push('缺少验证：确认信号未返回');
        if (!hasValue(item.risk)) missing.push('缺少验证：失效条件未返回');
        if (!hasValue(item.asset_code) && !hasValue(item.asset_name)) missing.push('缺少验证：本地资产映射未返回');

        const status = missing.length ? 'insufficient_evidence' : (item.stage || item.baton || 'watch_only');
        return {
            id: item.id || item.opportunity_id || item.title || item.name,
            generation_status: status,
            thesis: item.rationale || item.summary || item.title || item.name,
            region: item.market || 'CrossMarket',
            sector: item.sector || item.sector_key || item.title,
            model_family: item.model_family || 'V1兼容推导',
            leader_asset: item.leader_asset,
            bridge_asset: item.bridge_asset,
            local_asset: item.local_asset || {
                code: item.asset_code,
                name: item.asset_name || item.name,
                market: item.market,
                role: 'local_mapping',
            },
            baton_stage: item.baton || item.stage,
            why_now: item.why_now || item.rationale || item.summary,
            driver: item.driver,
            confirmations: asArray(item.confirmations || item.confirmation),
            missing_confirmations: missing.length ? missing : ['缺少验证：V2 机会卡关键字段尚未接入'],
            missing_evidence_reason: missing.join('；') || 'V1 机会列表不含完整 V2 机会卡契约',
            risk: item.risk,
            invalidation_rules: asArray(item.invalidation_rules || item.risk),
            expected_lag_days: item.expected_lag_days,
            actionability_score: item.actionability_score,
            tradability_score: item.tradability_score,
            evidence_completeness: item.evidence_completeness,
            freshness_score: item.freshness_score,
            decision_priority_score: item.decision_priority_score || item.score,
            historical_replay_summary: item.historical_replay_summary,
            source_count: asArray(item.evidence_sources).length,
            cache_status: 'sample_fallback',
            last_update: item.updated_at,
        };
    }

    function mapV1DecisionCenter(overview, opportunities) {
        const queue = opportunities.map(mapV1Opportunity);
        const top = queue.slice(0, 3).map((item, index) => ({
            rank: index + 1,
            sector: item.sector || item.region || '未映射赛道',
            thesis: item.thesis || '缺少验证：V1 未返回 thesis',
            reason: item.why_now || '缺少验证：V1 未返回 why_now',
            opportunity_id: item.id,
        }));
        const avoid = queue.filter((item) => {
            const score = Number(item.decision_priority_score);
            return item.generation_status === 'insufficient_evidence' || item.baton_stage === 'crowded' || (Number.isFinite(score) && score < 65);
        });

        return {
            ...v2Missing('decision-center', '决策中心需要标题、主结论、风险预算与失效条件'),
            as_of: overview.as_of,
            headline: overview.headline || 'V1 兼容决策视图',
            main_conclusion: overview.summary || '缺少验证：V2 决策中心未接入，当前仅能展示 V1 总览推导。',
            do_not_do_today: avoid.slice(0, 3).map((item) => item.thesis || item.id),
            top_directions: top,
            risk_budget: {
                label: avoid.length ? 'conservative' : 'balanced',
                reason: avoid.length
                    ? '缺少验证：部分机会卡缺少 V2 确认链或拥挤度诊断'
                    : 'V1 数据未提供完整风险预算，暂按平衡观察处理',
            },
            key_invalidations: queue.flatMap((item) => item.invalidation_rules || []).slice(0, 4),
            next_check_time: overview.as_of,
            source_count: Number(overview.live_event_count || 0) + Number(overview.live_research_count || 0),
            cache_status: overview.source || 'sample_fallback',
        };
    }

    function mapV1WhatChanged(overview) {
        return {
            ...v2Missing('what-changed', '今日变化需要对比时间、新增信号、升降级和宏观变化'),
            as_of: overview.as_of,
            since: overview.as_of,
            new_signals: asArray(overview.flags),
            upgraded_opportunities: [],
            downgraded_or_invalidated: [],
            crowding_up: [],
            macro_external_policy_changes: [
                '缺少验证：V1 总览未返回昨日对比、宏观外部与港股桥接变化',
            ],
        };
    }

    function classifyEvent(item) {
        if (item.event_class === 'market-facing' || item.event_class === 'research-facing') return item.event_class;
        const text = `${item.title || ''} ${item.type || ''} ${item.event_type || ''}`.toLowerCase();
        const assets = asArray(item.asset_mapping || item.related_assets || item.related_symbols);
        const developerNoise = /(sdk|github|framework|benchmark|open-source|developer|repo|tooling)/.test(text);
        if (developerNoise && !assets.length) return 'research-facing';
        return assets.length || hasValue(item.sector_key) || hasValue(item.type) ? 'market-facing' : 'research-facing';
    }

    function mapV1Events(rows) {
        return asArray(rows).map((item) => ({
            ...item,
            event_id: item.event_id || item.id || item.title,
            event_class: classifyEvent(item),
            event_type: item.event_type || item.type || 'other',
            sector_mapping: item.sector_mapping || asArray(item.sector || item.sector_key || item.type).map((sector) => ({ sector, score: item.confidence || item.priority_score })),
            asset_mapping: item.asset_mapping || item.related_assets || [],
            watch_items: asArray(item.watch_items || item.watch || item.notes),
            expected_path: item.expected_path || [],
            invalidation: asArray(item.invalidation || item.risk),
            relevance_score: item.relevance_score || item.priority_score || item.confidence,
            source: item.source || { url: item.source_url },
            effective_time: item.effective_time || item.date || item.event_date || item.window,
            missing_confirmations: ['缺少验证：V1 事件未返回完整 EventRelevance 打分、路径和失效条件'],
            missing_evidence_reason: '事件前线接口尚未接入时由事件日历推导',
        }));
    }

    function mapV1EventFrontline(rows) {
        return mapV1Events(rows).filter((item) => item.event_class === 'market-facing');
    }

    function mapV1AvoidBoard(opportunities) {
        const queue = opportunities.map(mapV1Opportunity);
        const rows = queue.filter((item) => {
            const score = Number(item.decision_priority_score);
            return item.generation_status === 'insufficient_evidence'
                || item.baton_stage === 'crowded'
                || item.baton_stage === 'invalidated'
                || (Number.isFinite(score) && score < 65);
        });
        if (rows.length) {
            return rows.map((item) => ({
                id: item.id,
                thesis: item.thesis,
                reason_type: item.baton_stage === 'crowded' ? 'crowded' : 'incomplete_evidence',
                reason: item.missing_evidence_reason || item.risk || '缺少验证：未返回 V2 avoid reason',
                evidence: item.missing_confirmations || item.confirmations || [],
                related_assets: [item.local_asset].filter(hasValue),
                next_review_time: item.last_update,
                source_count: item.source_count,
                last_update: item.last_update,
            }));
        }
        return [{
            id: 'v1_avoid_missing',
            thesis: '缺少验证：不要追高列表无法由 V1 数据完整生成',
            reason_type: 'incomplete_evidence',
            reason: '不要追高接口尚未接入；V1 机会列表缺少拥挤度、失效触发和流动性错配诊断。',
            evidence: ['缺少验证：拥挤度阈值', '缺少验证：失效触发', '缺少验证：流动性错配'],
            related_assets: [],
            next_review_time: '',
            source_count: 0,
            last_update: '',
        }];
    }

    function renderStatusChip(value) {
        if (!hasValue(value)) return '';
        return `<span class="signal-chip signal-${toneClass(value)}">${escapeHtml(displayLabel(value))}</span>`;
    }

    function renderTags(items, className = 'memory-tag') {
        return asArray(items).map((item) => `<span class="${className}">${escapeHtml(item)}</span>`).join('');
    }

    function renderMissing(reasons) {
        const items = asArray(reasons);
        if (!items.length) return '';
        return `
            <div class="missing-evidence">
                ${items.map((item) => `<div>${escapeHtml(item)}</div>`).join('')}
            </div>
        `;
    }

    function renderOptionalMeta(label, value) {
        if (!hasValue(value)) return '';
        return `
            <div class="meta-pair">
                <div class="meta-label">${escapeHtml(label)}</div>
                <div class="meta-value">${escapeHtml(value)}</div>
            </div>
        `;
    }

    function renderList(items) {
        const values = asArray(items);
        if (!values.length) return renderMissing(['缺少验证：该列表未返回']);
        return values.map((item) => `<div class="compact-item">${escapeHtml(item)}</div>`).join('');
    }

    function renderDecisionCenter() {
        const decision = state.data.decisionCenter || {};
        setText('decisionCenterTitle', decision.headline || '缺少验证：决策中心未返回标题');
        setText('decisionCenterConclusion', decision.main_conclusion || decision.summary || decision.missing_evidence_reason || '缺少验证：决策中心未返回主结论');
        setText('decisionRiskBudget', displayLabel(decision.risk_budget?.label) || '缺少验证');
        setText('decisionRiskReason', decision.risk_budget?.reason || decision.missing_evidence_reason);
        setText('decisionNextCheck', formatDate(decision.next_check_time) || '缺少验证');

        const flags = document.getElementById('decisionCenterFlags');
        if (flags) {
            const tags = [
                decision.cache_status ? `缓存: ${displayLabel(decision.cache_status)}` : '',
                decision.source_count ? `证据源: ${decision.source_count}` : '',
                ...asArray(decision.missing_confirmations).slice(0, 2),
            ].filter(hasValue);
            flags.innerHTML = renderTags(tags.length ? tags : ['缺少验证：V2 决策状态未返回']);
        }

        const topDirections = document.getElementById('decisionTopDirections');
        if (topDirections) {
            const rows = asArray(decision.top_directions);
            topDirections.innerHTML = rows.length ? rows.map((item) => `
                <div class="decision-item">
                    <div class="decision-rank">${escapeHtml(item.rank || '')}</div>
                    <div>
                        <div class="decision-title">${escapeHtml(item.sector || item.thesis || '缺少验证：方向未命名')}</div>
                        <div class="decision-copy">${escapeHtml(item.reason || item.thesis || '缺少验证：方向原因未返回')}</div>
                    </div>
                </div>
            `).join('') : renderMissing(['缺少验证：top_directions 未返回']);
        }

        const doNotDo = document.getElementById('decisionDoNotDo');
        if (doNotDo) doNotDo.innerHTML = renderList(decision.do_not_do_today);

        const invalidations = document.getElementById('decisionInvalidations');
        if (invalidations) invalidations.innerHTML = renderList(decision.key_invalidations);
    }

    function renderDecisionChain(chain) {
        if (!chain || typeof chain !== 'object') return renderMissing(['缺少验证：研判链路未返回']);
        return `
            <div class="decision-chain">
                ${CHAIN_LABELS.map(([key, label]) => {
                    const value = chain[key];
                    if (key === 'evidence') {
                        return `
                            <div class="chain-step">
                                <div class="chain-label">${label}</div>
                                <div class="chain-value">${renderList(value)}</div>
                            </div>
                        `;
                    }
                    if (key === 'data') {
                        const points = asArray(value);
                        return `
                            <div class="chain-step chain-step-data">
                                <div class="chain-label">${label}</div>
                                <div class="chain-data-grid">
                                    ${points.map((point) => `
                                        <div class="chain-data-item">
                                            <div class="chain-data-label">${escapeHtml(point.label || '')}</div>
                                            <div class="chain-data-value">${escapeHtml(point.value ?? '')}</div>
                                            <div class="chain-data-explain">${escapeHtml(point.explain || '')}</div>
                                        </div>
                                    `).join('') || renderMissing(['缺少验证：数据点未返回'])}
                                </div>
                            </div>
                        `;
                    }
                    return `
                        <div class="chain-step">
                            <div class="chain-label">${label}</div>
                            <div class="chain-value">${escapeHtml(value || '缺少验证：该环节未返回')}</div>
                        </div>
                    `;
                }).join('')}
            </div>
        `;
    }

    function renderStockPool(stockPool) {
        const rows = asArray(stockPool);
        if (!rows.length) return renderMissing(['缺少验证：股票池未返回']);
        return `
            <div class="stock-pool-preview">
                ${rows.slice(0, 5).map((stock) => `
                    <span class="stock-chip">${escapeHtml(stock.name || stock.code)} <em>${escapeHtml(stock.code || '')}</em></span>
                `).join('')}
            </div>
            <details class="stock-pool-details">
                <summary>查看完整股票池与下钻信息（${rows.length}）</summary>
                <div class="stock-pool-table">
                    <div class="stock-pool-head">名称</div>
                    <div class="stock-pool-head">代码</div>
                    <div class="stock-pool-head">角色</div>
                    <div class="stock-pool-head">关键分数</div>
                    ${rows.map((stock) => {
                        const factor = stock.factor_snapshot || {};
                        const score = hasValue(factor.total) ? factor.total : stock.decision_priority_score;
                        const risk = hasValue(factor.risk) ? `风险 ${factor.risk}` : displayLabel(stock.generation_status);
                        return `
                            <div class="stock-cell">
                                <div class="stock-name">${escapeHtml(stock.name || stock.code)}</div>
                                <div class="stock-sub">${escapeHtml([stock.market_zh || displayLabel(stock.market), stock.sector_zh || stock.sector].filter(hasValue).join(' / '))}</div>
                            </div>
                            <div class="stock-cell stock-code">${escapeHtml(stock.code)}</div>
                            <div class="stock-cell">${escapeHtml(stock.role_zh || displayLabel(stock.role))}</div>
                            <div class="stock-cell">
                                <div>${escapeHtml(score ?? '')}</div>
                                <div class="stock-sub">${escapeHtml(risk)}</div>
                                <details class="stock-detail">
                                    <summary>下钻</summary>
                                    <div class="stock-detail-grid">
                                        ${renderOptionalMeta('看它的原因', stock.reason)}
                                        ${renderOptionalMeta('传导阶段', stock.baton_stage_zh || displayLabel(stock.baton_stage))}
                                        ${renderOptionalMeta('下次检查', formatDate(stock.next_review_time))}
                                        ${renderOptionalMeta('证据源数', stock.source_count)}
                                        ${renderOptionalMeta('质量', factor.quality)}
                                        ${renderOptionalMeta('成长', factor.growth)}
                                        ${renderOptionalMeta('估值', factor.valuation)}
                                        ${renderOptionalMeta('资金', factor.flow)}
                                        ${renderOptionalMeta('技术', factor.technical)}
                                        ${renderOptionalMeta('风险', factor.risk)}
                                    </div>
                                    <div class="stock-detail-copy">${escapeHtml(stock.basic_info?.data_quality || '')}</div>
                                    <div class="stock-detail-copy">${renderList(stock.invalidation_rules)}</div>
                                </details>
                            </div>
                        `;
                    }).join('')}
                </div>
            </details>
        `;
    }

    function deriveModelGroups(cards) {
        const groups = new Map();
        asArray(cards).forEach((card) => {
            asArray(card.model_discoveries).forEach((discovery) => {
                const key = discovery.model_id || card.model_family || 'unknown';
                if (!groups.has(key)) {
                    groups.set(key, {
                        model_id: key,
                        model_name_zh: discovery.model_name_zh || displayLabel(key),
                        model_explain_zh: discovery.model_explain_zh || '',
                        opportunities: [],
                    });
                }
                groups.get(key).opportunities.push({
                    id: card.id,
                    thesis: card.thesis,
                    sector_zh: card.sector_zh || card.sector,
                    local_asset: card.local_asset,
                    baton_stage_zh: card.baton_stage_zh || displayLabel(card.baton_stage),
                    generation_status_zh: card.generation_status_zh || displayLabel(card.generation_status),
                    decision_priority_score: card.decision_priority_score,
                    model_strength: discovery.strength,
                    model_conclusion: discovery.conclusion,
                });
            });
        });
        return Array.from(groups.values()).sort((a, b) => b.opportunities.length - a.opportunities.length);
    }

    function renderModelDiscoveryGroups() {
        const groups = asArray(state.data.opportunityQueueGroups).length
            ? asArray(state.data.opportunityQueueGroups)
            : deriveModelGroups(state.data.opportunityQueue);
        setText('modelDiscoverySubtitle', groups.length ? `当前 ${groups.length} 个模型产生可跟踪发现` : '缺少验证：模型发现分组未返回');
        setText('modelDiscoveryCount', groups.length);
        if (!groups.length) return renderEmpty('modelDiscoveryGroups');
        const root = document.getElementById('modelDiscoveryGroups');
        if (!root) return;
        root.innerHTML = groups.map((group) => `
            <div class="model-group-card">
                <div class="card-title-row">
                    <div>
                        <div class="card-title">${escapeHtml(group.model_name_zh || displayLabel(group.model_id))}</div>
                        <div class="card-copy">${escapeHtml(group.model_explain_zh || '')}</div>
                    </div>
                    <div class="ll-panel-badge">${escapeHtml(group.count || asArray(group.opportunities).length)}</div>
                </div>
                <div class="model-opportunity-list">
                    ${asArray(group.opportunities).slice(0, 4).map((item) => `
                        <div class="model-opportunity-row">
                            <div>
                                <div class="model-opportunity-title">${escapeHtml(item.sector_zh || item.sector || item.thesis)}</div>
                                <div class="model-opportunity-copy">${escapeHtml(item.model_conclusion || item.thesis || '')}</div>
                                <div class="stock-sub">${escapeHtml(assetLabel(item.local_asset))}</div>
                            </div>
                            <div class="model-score">
                                <b>${escapeHtml(item.model_strength || item.decision_priority_score || '')}</b>
                                <span>${escapeHtml(item.baton_stage_zh || displayLabel(item.baton_stage))}</span>
                                <span>${escapeHtml(item.generation_status_zh || displayLabel(item.generation_status))}</span>
                            </div>
                        </div>
                    `).join('')}
                </div>
            </div>
        `).join('');
    }

    function opportunityCardIndex() {
        const index = new Map();
        asArray(state.data.opportunityQueue).forEach((card) => {
            if (hasValue(card.id)) index.set(String(card.id), card);
        });
        return index;
    }

    function sourceCardsForParent(parent, index) {
        return asArray(parent.source_card_ids).map((id) => index.get(String(id))).filter(Boolean);
    }

    function renderChecklist(checklist, summary = {}) {
        const rows = asArray(checklist);
        if (rows.length) {
            return `
                <div class="evidence-checklist">
                    ${rows.map((item) => `
                        <div class="checklist-item checklist-${toneClass(item.status)}">
                            <span>${escapeHtml(item.label || item.key)}</span>
                            <b>${escapeHtml(displayLabel(item.status))}</b>
                        </div>
                    `).join('')}
                </div>
            `;
        }
        const confirmed = asArray(summary.confirmed);
        const missing = asArray(summary.missing);
        const sampleOnly = asArray(summary.sample_only);
        if (!confirmed.length && !missing.length && !sampleOnly.length) return renderMissing(['缺少验证：Evidence Checklist 未返回']);
        return `
            <div class="evidence-checklist">
                ${confirmed.map((item) => `<div class="checklist-item checklist-positive"><span>${escapeHtml(item)}</span><b>已确认</b></div>`).join('')}
                ${missing.map((item) => `<div class="checklist-item checklist-negative"><span>${escapeHtml(item)}</span><b>缺失</b></div>`).join('')}
                ${sampleOnly.map((item) => `<div class="checklist-item checklist-warning"><span>${escapeHtml(item)}</span><b>样例</b></div>`).join('')}
            </div>
        `;
    }

    function renderDataSummary(rows) {
        const items = asArray(rows);
        if (!items.length) return renderMissing(['缺少验证：原始数据点未返回']);
        return `
            <div class="raw-data-grid">
                ${items.slice(0, 8).map((item) => `
                    <div class="raw-data-item">
                        <div class="raw-data-label">${escapeHtml(item.metric_name || item.label || item.title || '数据点')}</div>
                        <div class="raw-data-value">${escapeHtml(scoreValue(item.latest_value, item.value, item.metric_value))}</div>
                        <div class="raw-data-meta">
                            ${hasValue(item.previous_value) ? `上期 ${escapeHtml(item.previous_value)}` : ''}
                            ${hasValue(item.delta) ? ` / 变化 ${escapeHtml(item.delta)}` : ''}
                        </div>
                        <div class="raw-data-source">${escapeHtml([formatDate(item.as_of), item.source_name, item.citation_count ? `引用 ${item.citation_count}` : '', item.archived_link_count ? `归档 ${item.archived_link_count}` : ''].filter(hasValue).join(' / '))}</div>
                    </div>
                `).join('')}
            </div>
        `;
    }

    function renderEvidencePanel(rows) {
        const items = asArray(rows);
        if (!items.length) return renderMissing(['缺少验证：Evidence Panel 未返回']);
        return `
            <div class="evidence-panel-grid">
                ${items.slice(0, 6).map((item) => `
                    <div class="evidence-panel-item">
                        <div class="card-title-row">
                            <div>
                                <div class="evidence-title">${escapeHtml(item.title || item.source_name || '证据')}</div>
                                <div class="card-kicker">${escapeHtml([displayLabel(item.data_source_class), item.source_name, item.source_type].filter(hasValue).join(' / '))}</div>
                            </div>
                            ${renderStatusChip(item.archive_status || (item.local_archive_path ? 'confirmed' : 'missing'))}
                        </div>
                        <div class="raw-data-grid evidence-values">
                            ${renderBuilderMetric('最新值', scoreValue(item.latest_value, item.metric_value))}
                            ${renderBuilderMetric('上期', item.previous_value)}
                            ${renderBuilderMetric('变化', item.delta)}
                            ${renderBuilderMetric('时间', formatDate(item.as_of || item.published_at || item.fetched_at))}
                        </div>
                        ${hasValue(item.summary) ? `<div class="card-copy">${escapeHtml(item.summary)}</div>` : ''}
                        ${hasValue(item.quote_text) ? `<blockquote class="evidence-quote">${escapeHtml(item.quote_text)}</blockquote>` : ''}
                        <div class="evidence-links">
                            ${hasValue(item.original_link) ? `<a class="memory-link" href="${escapeHtml(item.original_link)}" target="_blank" rel="noreferrer">原始链接</a>` : ''}
                            ${hasValue(item.local_archive_path) ? `<span class="archive-path">本地归档：${escapeHtml(item.local_archive_path)}</span>` : '<span class="archive-path archive-missing">本地归档缺失</span>'}
                        </div>
                    </div>
                `).join('')}
            </div>
        `;
    }

    function renderChildVariants(variants) {
        const rows = asArray(variants);
        if (!rows.length) return renderMissing(['缺少验证：子载体列表未返回']);
        return `
            <details class="variant-details" open>
                <summary>子载体列表（${rows.length}）</summary>
                <div class="variant-table">
                    <div class="stock-pool-head">名称/代码</div>
                    <div class="stock-pool-head">角色</div>
                    <div class="stock-pool-head">交易质量</div>
                    <div class="stock-pool-head">风险/操作</div>
                    ${rows.map((item) => {
                        const factor = item.local_factor_snapshot || {};
                        return `
                            <div class="stock-cell">
                                <div class="stock-name">${escapeHtml(item.name || item.ticker)}</div>
                                <div class="stock-sub">${escapeHtml([displayLabel(item.market), item.ticker].filter(hasValue).join(' / '))}</div>
                            </div>
                            <div class="stock-cell">${escapeHtml(item.role_zh || displayLabel(item.role))}</div>
                            <div class="stock-cell">
                                <div>${escapeHtml(item.liquidity_score ?? '')}</div>
                                <div class="stock-sub">${escapeHtml([
                                    hasValue(item.recent_relative_strength) ? `强弱 ${item.recent_relative_strength}` : '',
                                    hasValue(item.crowding_score) ? `拥挤 ${item.crowding_score}` : '',
                                    hasValue(factor.technical) ? `技术 ${factor.technical}` : '',
                                ].filter(hasValue).join(' / '))}</div>
                            </div>
                            <div class="stock-cell">
                                <div class="stock-sub">${escapeHtml(item.variant_risk || item.variant_notes || '')}</div>
                                <button class="ll-inline-link" type="button" data-dossier-type="instrument" data-dossier-id="${escapeHtml(item.ticker || item.instrument_id)}">下钻</button>
                            </div>
                        `;
                    }).join('')}
                </div>
            </details>
        `;
    }

    function renderParentThesisCards(parents) {
        const index = opportunityCardIndex();
        return asArray(parents).map((parent) => {
            const sourceCards = sourceCardsForParent(parent, index);
            const firstCard = sourceCards[0] || {};
            const checklist = sourceCards.flatMap((card) => asArray(card.evidence_checklist));
            const evidenceRows = sourceCards.flatMap((card) => asArray(card.evidence_panel));
            const dataRows = asArray(parent.data_summary).length
                ? asArray(parent.data_summary)
                : sourceCards.flatMap((card) => asArray(card.data_summary));
            const blockers = asArray(parent.execution_blockers);
            return `
                <div class="opportunity-card parent-thesis-card">
                    <div class="card-title-row">
                        <div>
                            <div class="card-title">${escapeHtml(parent.thesis_title || '缺少验证：母 thesis 未命名')}</div>
                            <div class="card-kicker">${escapeHtml([parent.sector_zh || parent.sector, displayLabel(parent.family), displayLabel(parent.current_stage)].filter(hasValue).join(' / '))}</div>
                        </div>
                        <div class="parent-card-score">
                            <span>${escapeHtml(scoreValue(parent.decision_priority_score, parent.actionability_score))}</span>
                            ${renderStatusChip(parent.is_executable ? 'actionable' : parent.generation_status)}
                        </div>
                    </div>
                    <div class="card-copy">${escapeHtml(parent.why_now || parent.reasoning || '缺少验证：why_now 未返回')}</div>
                    ${renderMissing(blockers)}
                    <div class="meta-row operator-meta">
                        ${renderOptionalMeta('可执行度', parent.actionability_score)}
                        ${renderOptionalMeta('可交易性', parent.tradability_score)}
                        ${renderOptionalMeta('证据完整度', asPercent(parent.evidence_completeness))}
                        ${renderOptionalMeta('新鲜度', asPercent(parent.freshness_score))}
                        ${renderOptionalMeta('证据源', parent.source_count)}
                        ${renderOptionalMeta('Live / Sample', `${parent.live_source_count || 0} / ${parent.sample_source_count || 0}`)}
                        ${renderOptionalMeta('引用 / 归档', `${parent.citation_count || 0} / ${parent.archived_link_count || 0}`)}
                        ${renderOptionalMeta('下次复核', formatDate(parent.next_review_time))}
                    </div>
                    <div class="parent-chain-grid">
                        <div class="operator-card-section">
                            <div class="section-label">结果</div>
                            <div class="chain-value">${escapeHtml(parent.result || firstCard.generation_status_zh || '缺少验证：结果未返回')}</div>
                        </div>
                        <div class="operator-card-section">
                            <div class="section-label">思路</div>
                            <div class="chain-value">${escapeHtml(parent.reasoning || firstCard.driver || '缺少验证：思路未返回')}</div>
                        </div>
                        <div class="operator-card-section">
                            <div class="section-label">策略</div>
                            <div class="chain-value">${escapeHtml(parent.strategy || '缺少验证：策略未返回')}</div>
                        </div>
                    </div>
                    <div class="operator-card-section">
                        <div class="section-label">证据清单</div>
                        ${renderChecklist(checklist, parent.evidence_summary)}
                    </div>
                    <div class="operator-card-section">
                        <div class="section-label">数据层</div>
                        ${renderDataSummary(dataRows)}
                    </div>
                    <div class="operator-card-section">
                        <div class="section-label">Evidence Panel</div>
                        ${renderEvidencePanel(evidenceRows)}
                    </div>
                    <div class="operator-card-section">
                        <div class="section-label">子载体</div>
                        ${renderChildVariants(parent.child_variants)}
                    </div>
                    <div class="operator-card-section">
                        <div class="section-label">失效条件</div>
                        ${renderList(parent.invalidation_rules)}
                    </div>
                    <div class="parent-actions">
                        <button class="ll-inline-link" type="button" data-dossier-type="sector" data-dossier-id="${escapeHtml(parent.sector || '')}">赛道 Dossier</button>
                    </div>
                </div>
            `;
        }).join('');
    }

    function renderFlatOpportunityCards(rows) {
        return rows.map((item) => {
            const missing = asArray(item.missing_confirmations);
            const localAsset = assetLabel(item.local_asset);
            const bridgeAsset = assetLabel(item.bridge_asset);
            const leaderAsset = assetLabel(item.leader_asset);
            const lag = item.expected_lag_days && typeof item.expected_lag_days === 'object'
                ? `${item.expected_lag_days.min ?? ''}-${item.expected_lag_days.max ?? ''}d`
                : item.expected_lag_days;
            return `
                <div class="opportunity-card operator-card">
                    <div class="card-title-row">
                        <div>
                            <div class="card-title">${escapeHtml(item.thesis || item.title || '缺少验证：机会 thesis 未返回')}</div>
                            <div class="card-kicker">${escapeHtml([displayLabel(item.region), item.sector_zh || item.sector, item.model_name_zh || displayLabel(item.model_family)].filter(hasValue).join(' / '))}</div>
                        </div>
                        <div class="opportunity-score">${escapeHtml(scoreValue(item.decision_priority_score, item.score, item.actionability_score))}</div>
                    </div>
                    <div class="card-copy">${escapeHtml(item.why_now || item.driver || item.missing_evidence_reason || '缺少验证：why_now 未返回')}</div>
                    ${renderMissing(missing)}
                    <div class="meta-row operator-meta">
                        ${renderOptionalMeta('传导棒位', item.baton_stage_zh || displayLabel(item.baton_stage))}
                        ${renderOptionalMeta('领先资产', leaderAsset)}
                        ${renderOptionalMeta('桥接资产', bridgeAsset)}
                        ${renderOptionalMeta('本地映射', localAsset)}
                        ${renderOptionalMeta('预期时滞', lag)}
                        ${renderOptionalMeta('可执行度', scoreValue(item.actionability_score, item.decision_priority_score))}
                        ${renderOptionalMeta('可交易性', item.tradability_score)}
                        ${renderOptionalMeta('证据完整度', asPercent(item.evidence_completeness))}
                        ${renderOptionalMeta('新鲜度', asPercent(item.freshness_score))}
                    </div>
                    <div class="operator-card-section">
                        <div class="section-label">确认与缺口</div>
                        ${renderList(item.confirmations?.length ? item.confirmations : missing)}
                    </div>
                    <div class="operator-card-section">
                        <div class="section-label">失效条件</div>
                        ${renderList(item.invalidation_rules?.length ? item.invalidation_rules : item.risk)}
                    </div>
                    <div class="operator-card-section">
                        <div class="section-label">研判链路</div>
                        ${renderDecisionChain(item.decision_chain)}
                    </div>
                    <div class="operator-card-section">
                        <div class="section-label">证据清单</div>
                        ${renderChecklist(item.evidence_checklist, item.evidence_summary)}
                    </div>
                    <div class="operator-card-section">
                        <div class="section-label">Evidence Panel</div>
                        ${renderEvidencePanel(item.evidence_panel)}
                    </div>
                    <div class="operator-card-section">
                        <div class="section-label">股票池与下钻</div>
                        ${renderStockPool(item.stock_pool)}
                    </div>
                </div>
            `;
        }).join('');
    }

    function renderOpportunityGovernance() {
        const payload = state.data.opportunityQueuePayload || {};
        const blocked = asArray(state.data.blockedCards);
        const root = document.getElementById('opportunityGovernance');
        setText('governanceSubtitle', `默认可见 ${asArray(state.data.opportunityQueue).length} / 原始 ${payload.raw_count || 0}，挡下 ${blocked.length} 张样例/弱来源卡`);
        if (!root) return;
        root.innerHTML = `
            <div class="governance-summary-grid">
                ${renderBuilderMetric('默认队列', payload.count ?? asArray(state.data.opportunityQueue).length)}
                ${renderBuilderMetric('原始机会', payload.raw_count)}
                ${renderBuilderMetric('被挡机会', blocked.length)}
                ${renderBuilderMetric('当前过滤', [
                    state.filters.live_only ? 'live-only' : '',
                    state.filters.archived_only ? '已归档' : '',
                    state.filters.include_sample ? '含样例' : '隐藏样例',
                    state.filters.family !== 'all' ? displayLabel(state.filters.family) : '',
                ].filter(hasValue).join(' / '))}
            </div>
            ${blocked.slice(0, 5).map((item) => `
                <div class="governance-card">
                    <div class="card-title">${escapeHtml(item.thesis || item.title || item.id)}</div>
                    <div class="card-kicker">${escapeHtml([displayLabel(item.data_source_class), displayLabel(item.generation_status)].filter(hasValue).join(' / '))}</div>
                    ${renderList(item.execution_blockers || item.missing_confirmations)}
                </div>
            `).join('') || renderMissing(['当前过滤下没有被挡样例/回退卡'])}
        `;
    }

    function renderOpportunityQueue() {
        const rows = asArray(state.data.opportunityQueue);
        const parents = asArray(state.data.parentThesisCards);
        const visibleCount = parents.length || rows.length;
        setText('opportunityQueueSubtitle', visibleCount ? `当前 ${visibleCount} 张母 thesis / 机会卡，按“结果-思路-策略-依据-数据”展开` : '缺少验证：机会队列未返回机会卡');
        setText('opportunityQueueCount', visibleCount);
        if (!visibleCount) {
            renderEmpty('opportunityQueue');
            renderOpportunityGovernance();
            return;
        }
        const root = document.getElementById('opportunityQueue');
        if (root) root.innerHTML = parents.length ? renderParentThesisCards(parents) : renderFlatOpportunityCards(rows);
        renderOpportunityGovernance();
    }

    function renderWhatChanged() {
        const payload = state.data.whatChanged || {};
        const sections = [
            ['新增信号', payload.new_signals],
            ['上调机会', asArray(payload.upgraded_opportunities).map((item) => `${item.opportunity_id || item.thesis || '机会'}：${item.reason || '缺少验证：升级原因未返回'}`)],
            ['降级或失效', asArray(payload.downgraded_or_invalidated).map((item) => `${item.opportunity_id || item.thesis || '机会'}：${item.reason || '缺少验证：降级原因未返回'}`)],
            ['拥挤上升', asArray(payload.crowding_up).map((item) => `${item.thesis || '方向'}：${item.reason || '缺少验证：拥挤原因未返回'}`)],
            ['宏观/外部/政策', payload.macro_external_policy_changes],
        ];
        const root = document.getElementById('whatChanged');
        if (!root) return;
        root.innerHTML = sections.map(([title, items]) => `
            <div class="change-card">
                <div class="section-label">${escapeHtml(title)}</div>
                ${renderList(items)}
            </div>
        `).join('');
    }

    function renderEventFrontline() {
        const rows = asArray(state.data.eventFrontline).filter((item) => classifyEvent(item) === 'market-facing');
        setText('eventFrontlineSubtitle', rows.length ? `默认显示 ${rows.length} 个可交易催化事件` : '缺少验证：无可交易催化事件返回');
        if (!rows.length) return renderEmpty('eventFrontline');
        document.getElementById('eventFrontline').innerHTML = rows.map((item) => {
            const watchItems = asArray(item.watch_items || item.watch);
            const invalidation = asArray(item.invalidation || item.invalidation_rules);
            const assets = asArray(item.asset_mapping || item.related_assets).map(assetLabel).filter(Boolean);
            return `
                <div class="timeline-card operator-event">
                    <div class="timeline-head">
                        <div>
                            <div class="timeline-title">${escapeHtml(item.title || '缺少验证：事件标题未返回')}</div>
                            <div class="card-kicker">${escapeHtml([displayLabel(item.event_type), displayLabel(item.event_class)].filter(hasValue).join(' / '))}</div>
                        </div>
                        <div class="timeline-date">${escapeHtml(formatDate(item.effective_time || item.date || item.window))}</div>
                    </div>
                    <div class="timeline-copy">${escapeHtml(item.expected_path?.[0]?.relation || item.base_case || item.notes || item.missing_evidence_reason || '缺少验证：事件路径未返回')}</div>
                    ${renderMissing(item.missing_confirmations)}
                    <div class="meta-row operator-meta">
                        ${renderOptionalMeta('相关性', item.relevance_score)}
                        ${renderOptionalMeta('中国映射', item.china_mapping_score)}
                        ${renderOptionalMeta('可交易性', item.tradability_score)}
                        ${renderOptionalMeta('相关资产', assets.join(', '))}
                    </div>
                    <div class="operator-card-section">
                        <div class="section-label">观察项</div>
                        ${renderList(watchItems.length ? watchItems : ['缺少验证：watch_items 未返回'])}
                    </div>
                    <div class="operator-card-section">
                        <div class="section-label">失效条件</div>
                        ${renderList(invalidation.length ? invalidation : ['缺少验证：invalidation 未返回'])}
                    </div>
                </div>
            `;
        }).join('');
    }

    function renderAvoidBoard() {
        const rows = asArray(state.data.avoidBoard);
        setText('avoidBoardSubtitle', rows.length ? `当前 ${rows.length} 个不追方向` : '缺少验证：不追列表未返回');
        if (!rows.length) return renderEmpty('avoidBoard');
        document.getElementById('avoidBoard').innerHTML = rows.map((item) => `
            <div class="avoid-card">
                <div class="card-title-row">
                    <div>
                        <div class="card-title">${escapeHtml(item.thesis || '缺少验证：不追主题未返回')}</div>
                        <div class="card-kicker">${escapeHtml(displayLabel(item.reason_type || 'incomplete_evidence'))}</div>
                    </div>
                    ${renderStatusChip(item.reason_type)}
                </div>
                <div class="card-copy">${escapeHtml(item.reason || '缺少验证：不追原因未返回')}</div>
                ${renderList(item.evidence)}
                <div class="meta-row operator-meta">
                    ${renderOptionalMeta('下次复核', formatDate(item.next_review_time))}
                    ${renderOptionalMeta('证据源数', item.source_count)}
                    ${renderOptionalMeta('更新时间', formatDate(item.last_update))}
                </div>
            </div>
        `).join('');
    }

    function renderOverviewControls() {
        const overview = state.data.overview || EMPTY_OVERVIEW;
        setText('globalStatusText', buildStatusText());
        fillSelect('leadLagRegion', overview.regions || EMPTY_OVERVIEW.regions, state.filters.region);
        fillSelect('leadLagRegime', overview.regimes || EMPTY_OVERVIEW.regimes, state.filters.regime);
        const family = document.getElementById('leadLagFamily');
        if (family) family.value = state.filters.family || 'all';
        const liveOnly = document.getElementById('leadLagLiveOnly');
        if (liveOnly) liveOnly.checked = Boolean(state.filters.live_only);
        const archivedOnly = document.getElementById('leadLagArchivedOnly');
        if (archivedOnly) archivedOnly.checked = Boolean(state.filters.archived_only);
        const includeSample = document.getElementById('leadLagIncludeSample');
        if (includeSample) includeSample.checked = Boolean(state.filters.include_sample);
        const asOfInput = document.getElementById('leadLagAsOf');
        if (asOfInput && !state.filters.as_of && overview.as_of) {
            state.filters.as_of = overview.as_of;
            asOfInput.value = overview.as_of;
        }
    }

    function renderModels() {
        const rows = asArray(state.data.models);
        setText('modelLibrarySubtitle', rows.length ? `共 ${rows.length} 个模型模板与状态标签` : '未返回模型定义');
        setText('modelLibraryCount', rows.length);
        if (!rows.length) return renderEmpty('modelLibraryList');
        document.getElementById('modelLibraryList').innerHTML = rows.map((item) => `
            <div class="model-card">
                <div class="card-title-row">
                    <div class="card-title">${escapeHtml(item.name || item.model || 'Unnamed Model')}</div>
                    ${renderStatusChip(item.status || item.tone)}
                </div>
                ${hasValue(item.summary || item.description) ? `<div class="card-copy">${escapeHtml(item.summary || item.description)}</div>` : ''}
                <div class="meta-row builder-meta">
                    ${renderOptionalMeta('领先窗口', item.lead_window || item.lead)}
                    ${renderOptionalMeta('覆盖范围', item.universe)}
                    ${renderOptionalMeta('置信度', item.confidence)}
                </div>
            </div>
        `).join('');
    }

    function renderCrossMarket() {
        const rows = asArray(state.data.crossMarket);
        setText('crossMarketSubtitle', rows.length ? `显示 ${rows.length} 个跨市场节点` : '等待跨市场映射');
        if (!rows.length) return renderEmpty('crossMarketMap');
        document.getElementById('crossMarketMap').innerHTML = rows.map((item) => `
            <div class="map-node">
                <div class="card-title-row">
                    <div class="node-title">${escapeHtml(item.name || item.asset || 'Node')}</div>
                    ${renderStatusChip(item.tone)}
                </div>
                ${hasValue(item.summary) ? `<div class="node-copy">${escapeHtml(item.summary)}</div>` : ''}
                <div class="node-metrics">
                    ${renderBuilderMetric('信号', item.signal)}
                    ${renderBuilderMetric('时滞', item.lag)}
                </div>
            </div>
        `).join('');
    }

    function renderBuilderMetric(label, value) {
        if (!hasValue(value)) return '';
        return `
            <div class="node-metric">
                <div class="node-metric-label">${escapeHtml(label)}</div>
                <div class="node-metric-value">${escapeHtml(value)}</div>
            </div>
        `;
    }

    function renderTransmission() {
        const workspace = state.data.transmissionWorkspace || {};
        const workspaceEdges = asArray(workspace.edges);
        const workspacePaths = asArray(workspace.baton_paths);
        if (workspaceEdges.length || workspacePaths.length) {
            const bottlenecks = asArray(workspace.current_bottlenecks);
            setText('industryTransmissionSubtitle', `图谱 ${workspaceEdges.length} 条边，卡点 ${bottlenecks.length} 个`);
            document.getElementById('industryTransmission').innerHTML = `
                <div class="diagnostic-summary">
                    ${Object.entries(workspace.edge_status_summary || {}).map(([key, value]) => renderBuilderMetric(key, value)).join('')}
                    ${renderBuilderMetric('来源', workspace.source)}
                    ${renderBuilderMetric('缓存状态', displayLabel(workspace.cache_status))}
                </div>
                ${workspacePaths.slice(0, 4).map((path) => `
                    <div class="lane-card">
                        <div class="card-title-row">
                            <div class="lane-title">${escapeHtml(path.sector || '传导路径')}</div>
                            ${renderStatusChip(path.validation_target ? 'validating' : '缺验证')}
                        </div>
                        <div class="lane-steps">
                            ${asArray(path.path).map((step, index) => `
                                <div class="lane-step">
                                    <div class="lane-index">${index + 1}</div>
                                    <div class="lane-step-title">${escapeHtml(step)}</div>
                                </div>
                            `).join('')}
                        </div>
                    </div>
                `).join('')}
                ${bottlenecks.slice(0, 5).map((item) => `
                    <div class="lane-card bottleneck-card">
                        <div class="card-title-row">
                            <div class="lane-title">${escapeHtml(item.item_id || item.type)}</div>
                            ${renderStatusChip(item.status)}
                        </div>
                        <div class="lane-copy">${escapeHtml(item.reason || '等待验证')}</div>
                    </div>
                `).join('')}
            `;
            return;
        }
        const rows = asArray(state.data.transmission);
        setText('industryTransmissionSubtitle', rows.length ? `已装载 ${rows.length} 条传导链` : '等待行业传导数据');
        if (!rows.length) return renderEmpty('industryTransmission');
        document.getElementById('industryTransmission').innerHTML = rows.map((item) => `
            <div class="lane-card">
                <div class="card-title-row">
                    <div class="lane-title">${escapeHtml(item.name || item.driver || 'Transmission')}</div>
                    ${renderStatusChip(item.signal)}
                </div>
                ${hasValue(item.summary) ? `<div class="lane-copy">${escapeHtml(item.summary)}</div>` : ''}
                <div class="lane-steps">
                    ${asArray(item.steps).map((step, index) => `
                        <div class="lane-step">
                            <div class="lane-index">${index + 1}</div>
                            <div>
                                <div class="lane-step-title">${escapeHtml(typeof step === 'object' ? (step.title || step.node || step.name) : step)}</div>
                                ${typeof step === 'object' && hasValue(step.note || step.summary) ? `<div class="lane-step-copy">${escapeHtml(step.note || step.summary)}</div>` : ''}
                            </div>
                        </div>
                    `).join('')}
                </div>
            </div>
        `).join('');
    }

    function renderLiquidity() {
        const rows = asArray(state.data.liquidity);
        setText('liquiditySubtitle', rows.length ? `外部与港股流动性共 ${rows.length} 个观测项` : '等待流动性面板');
        if (!rows.length) return renderEmpty('liquidityPanel');
        document.getElementById('liquidityPanel').innerHTML = rows.map((item) => `
            <div class="liquidity-card">
                <div class="card-title-row">
                    <div class="card-title">${escapeHtml(item.name || item.metric || 'Liquidity')}</div>
                    ${renderStatusChip(item.tone || item.signal)}
                </div>
                ${hasValue(item.value) ? `<div class="liquidity-value">${escapeHtml(item.value)}</div>` : ''}
                ${hasValue(item.note || item.summary) ? `<div class="liquidity-footnote">${escapeHtml(item.note || item.summary)}</div>` : ''}
            </div>
        `).join('');
    }

    function renderThesis() {
        const rows = asArray(state.data.sectorEvidence).length ? asArray(state.data.sectorEvidence) : asArray(state.data.thesis);
        setText('sectorThesisSubtitle', rows.length ? `当前 ${rows.length} 个赛道深证据层` : '等待赛道深证据');
        if (!rows.length) return renderEmpty('sectorThesis');
        document.getElementById('sectorThesis').innerHTML = rows.map((item) => {
            const layers = asArray(item.evidence_layers);
            const leader = asArray(item.leader_assets).map(assetLabel).filter(Boolean).slice(0, 3).join(', ');
            const local = asArray(item.local_assets).map(assetLabel).filter(Boolean).slice(0, 4).join(', ');
            if (layers.length) {
                return `
                    <div class="thesis-card deep-evidence-card">
                        <div class="card-title-row">
                            <div>
                                <div class="thesis-title">${escapeHtml(item.sector_name || item.sector_key || '赛道')}</div>
                                <div class="card-kicker">${escapeHtml([item.mode, item.cache_status].filter(hasValue).join(' / '))}</div>
                            </div>
                            ${renderStatusChip(item.action_readiness?.label || item.cache_status)}
                        </div>
                        <div class="meta-row builder-meta">
                            ${renderOptionalMeta('证据完整度', asPercent(item.evidence_completeness))}
                            ${renderOptionalMeta('领先资产', leader)}
                            ${renderOptionalMeta('本地资产', local)}
                            ${renderOptionalMeta('更新时间', formatDate(item.last_update))}
                        </div>
                        <div class="evidence-layer-list">
                            ${layers.map((layer) => `
                                <div class="evidence-layer">
                                    <div class="card-title-row">
                                        <div class="lane-step-title">${escapeHtml(layer.layer_name || layer.layer_key)}</div>
                                        ${renderStatusChip(layer.status)}
                                    </div>
                                    ${hasValue(layer.description) ? `<div class="lane-step-copy">${escapeHtml(layer.description)}</div>` : ''}
                                </div>
                            `).join('')}
                        </div>
                        <div class="operator-card-section">
                            <div class="section-label">缺口 / 失效</div>
                            ${renderList(asArray(item.missing_validation).length ? item.missing_validation : item.invalidation_rules)}
                        </div>
                    </div>
                `;
            }
            return `
                <div class="thesis-card">
                    <div class="card-title-row">
                        <div class="thesis-title">${escapeHtml(item.sector || item.name || item.title || 'Sector')}</div>
                        ${renderStatusChip(item.conviction || item.tone || item.crowding)}
                    </div>
                    ${hasValue(item.thesis || item.summary) ? `<div class="thesis-copy">${escapeHtml(item.thesis || item.summary)}</div>` : ''}
                    <div class="thesis-meta">
                        ${renderTags(asArray(item.evidence).map((value) => `证据: ${value}`))}
                        ${hasValue(item.invalidation) ? renderTags([`失效: ${item.invalidation}`]) : ''}
                        ${hasValue(item.crowding) ? renderTags([`拥挤: ${item.crowding}`]) : ''}
                    </div>
                </div>
            `;
        }).join('');
    }

    function renderEvents() {
        const includeResearch = state.filters.include_research_facing;
        const rows = mapV1Events(asArray(state.data.events)).filter((item) => includeResearch || classifyEvent(item) === 'market-facing');
        setText('eventCalendarSubtitle', includeResearch ? '显示可交易催化与研究背景事件' : '仅显示可交易催化；勾选后查看研究背景');
        if (!rows.length) return renderEmpty('eventCalendar');
        document.getElementById('eventCalendar').innerHTML = rows.map((item) => `
            <div class="timeline-card">
                <div class="timeline-head">
                    <div>
                        <div class="timeline-title">${escapeHtml(item.title || 'Event')}</div>
                        <div class="card-kicker">${escapeHtml(displayLabel(classifyEvent(item)))}</div>
                    </div>
                    <div class="timeline-date">${escapeHtml(formatDate(item.effective_time || item.date || item.window))}</div>
                </div>
                ${hasValue(item.notes || item.summary || item.base_case) ? `<div class="timeline-copy">${escapeHtml(item.notes || item.summary || item.base_case)}</div>` : ''}
                <div class="meta-row builder-meta">
                    ${renderOptionalMeta('类型', displayLabel(item.event_type || item.type))}
                    ${renderOptionalMeta('负责人', item.owner)}
                    ${renderOptionalMeta('置信度', item.confidence)}
                </div>
            </div>
        `).join('');
    }

    function renderValidation() {
        const diagnostics = state.data.replayDiagnostics || {};
        const horizons = asArray(diagnostics.horizon_distribution);
        if (horizons.length) {
            const failures = asArray(diagnostics.failure_mode_ranking);
            const transitions = asArray(diagnostics.stage_transition_performance);
            setText('replayValidationSubtitle', `回放样本 ${diagnostics.sample_size || 0}，含 horizon / failure / stage diagnostics`);
            document.getElementById('replayValidation').innerHTML = `
                ${horizons.map((item) => `
                    <div class="validation-card">
                        <div class="validation-head">
                            <div class="card-title">${escapeHtml(item.horizon_days)}日分布</div>
                            <div class="validation-score">${escapeHtml(asPercent(item.hit_rate))}</div>
                        </div>
                        <div class="validation-stats">
                            ${renderValidationStat('样本数', item.cases)}
                            ${renderValidationStat('胜率', asPercent(item.win_rate))}
                            ${renderValidationStat('Alpha bps', item.avg_net_alpha_bps)}
                        </div>
                    </div>
                `).join('')}
                <div class="validation-card">
                    <div class="section-label">失败模式</div>
                    ${renderList(failures.slice(0, 5).map((item) => `${item.failure_mode}: ${item.weighted_cases || item.count}`))}
                </div>
                <div class="validation-card">
                    <div class="section-label">阶段迁移</div>
                    ${renderList(transitions.map((item) => `${item.transition}: ${asPercent(item.hit_rate)} / ${item.cases || 0} 个样本`))}
                </div>
            `;
            return;
        }
        const rows = asArray(state.data.validation);
        setText('replayValidationSubtitle', rows.length ? `当前展示 ${rows.length} 条回放验证记录` : '等待回放验证');
        if (!rows.length) return renderEmpty('replayValidation');
        document.getElementById('replayValidation').innerHTML = rows.map((item) => `
            <div class="validation-card">
                <div class="validation-head">
                    <div class="card-title">${escapeHtml(item.title || item.case || item.reference || 'Validation')}</div>
                    ${hasValue(item.verdict || item.outcome || item.score) ? `<div class="validation-score">${escapeHtml(item.verdict || item.outcome || item.score)}</div>` : ''}
                </div>
                ${hasValue(item.note || item.summary || item.reason) ? `<div class="validation-copy">${escapeHtml(item.note || item.summary || item.reason)}</div>` : ''}
                <div class="validation-stats">
                    ${renderValidationStat('命中率', item.hit_rate)}
                    ${renderValidationStat('领先窗口', item.lead_window)}
                    ${renderValidationStat('失败模式', item.failure_mode)}
                </div>
            </div>
        `).join('');
    }

    function renderValidationStat(label, value) {
        if (!hasValue(value)) return '';
        return `
            <div class="validation-stat">
                <div class="validation-stat-label">${escapeHtml(label)}</div>
                <div class="validation-stat-value">${escapeHtml(value)}</div>
            </div>
        `;
    }

    function renderMemory() {
        const actions = state.data.memoryActions || {};
        const actionRows = [
            ['Thesis 摘要', actions.thesis_summary],
            ['历史胜例', actions.prior_wins],
            ['历史败例', actions.prior_failures],
            ['典型陷阱', actions.typical_trap],
            ['相似案例', actions.similar_cases],
            ['复盘记录', actions.review_notes],
        ].filter(([, rows]) => asArray(rows).length);
        if (actionRows.length) {
            setText('obsidianSubtitle', `行动记忆 ${actionRows.length} 类，缺口 ${asArray(actions.missing_memory).length} 个`);
            document.getElementById('obsidianMemory').innerHTML = `
                ${actionRows.map(([title, rows]) => `
                    <div class="memory-card action-memory-card">
                        <div class="memory-head">
                            <div class="memory-title">${escapeHtml(title)}</div>
                            ${renderStatusChip(actions.cache_status)}
                        </div>
                        ${asArray(rows).slice(0, 4).map((item) => `
                            <div class="memory-action-row">
                                <div class="memory-copy">${escapeHtml(item.title || item.path || item)}</div>
                                ${hasValue(item.path) ? `<div class="card-kicker">${escapeHtml(item.path)}</div>` : ''}
                            </div>
                        `).join('')}
                    </div>
                `).join('')}
                ${asArray(actions.mapped_opportunities).slice(0, 4).map((item) => `
                    <div class="memory-card">
                        <div class="section-label">映射机会</div>
                        <div class="memory-title">${escapeHtml(item.opportunity_id || item.thesis)}</div>
                        ${renderTags(asArray(item.memory_hits).map((hit) => hit.title || hit.path || hit.memory_type))}
                    </div>
                `).join('')}
            `;
            return;
        }
        const rows = asArray(state.data.memory);
        setText('obsidianSubtitle', rows.length ? `命中 ${rows.length} 条研究记忆` : '等待 Obsidian 研究记忆');
        if (!rows.length) return renderEmpty('obsidianMemory');
        document.getElementById('obsidianMemory').innerHTML = rows.map((item) => {
            const links = asArray(item.links).map((link) => {
                if (typeof link === 'string') return { url: link, label: link };
                return link;
            });
            return `
                <div class="memory-card">
                    <div class="memory-head">
                        <div class="memory-title">${escapeHtml(item.title || item.note || 'Research Note')}</div>
                        <div class="memory-date">${escapeHtml(formatDate(item.updated_at || item.date))}</div>
                    </div>
                    ${hasValue(item.summary || item.excerpt || item.notes) ? `<div class="memory-copy">${escapeHtml(item.summary || item.excerpt || item.notes)}</div>` : ''}
                    <div class="memory-tags">${renderTags(item.tags)}</div>
                    <div class="memory-links">
                        ${links.map((link) => `
                            <a class="memory-link" href="${escapeHtml(link.url || '#')}" target="_blank" rel="noreferrer">${escapeHtml(link.label || link.url || 'Reference')}</a>
                        `).join('')}
                    </div>
                </div>
            `;
        }).join('');
    }

    function renderObjectMetrics(object, labelMap = {}) {
        const entries = Object.entries(object || {}).filter(([, value]) => hasValue(value));
        if (!entries.length) return renderMissing(['缺少验证：指标未返回']);
        return `
            <div class="diagnostic-summary">
                ${entries.map(([key, value]) => renderBuilderMetric(labelMap[key] || displayLabel(key), typeof value === 'object' ? JSON.stringify(value) : value)).join('')}
            </div>
        `;
    }

    function renderSourceQualityLineage() {
        const payload = state.data.sourceQualityLineage || {};
        const lineage = payload.lineage || {};
        const vault = payload.evidence_vault || {};
        const alerts = asArray(lineage.mapping_pollution_alerts);
        setText('sourceQualitySubtitle', `文档 ${vault.document_count ?? 0}，归档 ${vault.archived_document_count ?? 0}，解析失败率 ${asPercent(vault.parse_failure_rate) || '缺少验证'}`);
        const root = document.getElementById('sourceQualityLineage');
        if (!root) return;
        root.innerHTML = `
            <div class="source-quality-grid">
                <div class="source-quality-card">
                    <div class="section-label">Evidence Vault</div>
                    ${renderObjectMetrics({
                        document_count: vault.document_count,
                        archived_document_count: vault.archived_document_count,
                        parse_failure_rate: asPercent(vault.parse_failure_rate),
                        schema_version: vault.schema_version,
                    }, {
                        document_count: '文档数',
                        archived_document_count: '已归档',
                        parse_failure_rate: '解析失败率',
                        schema_version: 'Schema',
                    })}
                </div>
                <div class="source-quality-card">
                    <div class="section-label">Live / Sample</div>
                    ${renderObjectMetrics(lineage.live_vs_sample, {
                        live: 'Live',
                        sample_or_fallback: '样例/回退',
                        generated_inference: '模型推断',
                    })}
                </div>
                <div class="source-quality-card">
                    <div class="section-label">来源等级</div>
                    ${renderObjectMetrics(vault.reliability_tiers)}
                </div>
                <div class="source-quality-card">
                    <div class="section-label">数据类别</div>
                    ${renderObjectMetrics(vault.data_source_classes || lineage.source_class_counts)}
                </div>
                <div class="source-quality-card">
                    <div class="section-label">事件类别</div>
                    ${renderObjectMetrics(lineage.event_class_counts)}
                </div>
                <div class="source-quality-card">
                    <div class="section-label">报警</div>
                    ${renderList([
                        lineage.blocked_executable_count ? `被阻止可执行机会：${lineage.blocked_executable_count}` : '',
                        lineage.cards_without_archive_count ? `缺本地归档机会：${lineage.cards_without_archive_count}` : '',
                        lineage.stale_next_review_count ? `过期复核点：${lineage.stale_next_review_count}` : '',
                    ].filter(hasValue))}
                </div>
            </div>
            <div class="alert-stack">
                ${alerts.slice(0, 8).map((item) => `
                    <div class="governance-card">
                        <div class="card-title">${escapeHtml(item.opportunity_id || '映射污染报警')}</div>
                        <div class="card-copy">${escapeHtml(item.reason || '')}</div>
                    </div>
                `).join('') || renderMissing(['暂无映射污染报警'])}
            </div>
        `;
    }

    function renderReportCenter() {
        const payload = state.data.reportCenter || {};
        const reports = asArray(payload.reports);
        setText('reportCenterSubtitle', reports.length ? `当前 ${reports.length} 份报告；全文检索 ${payload.full_text_search ? '已启用' : '未确认'}` : '暂无报告记录或报告中心未初始化');
        const root = document.getElementById('reportCenter');
        if (!root) return;
        if (!reports.length) {
            renderEmpty('reportCenter');
            return;
        }
        root.innerHTML = `
            <div class="report-list">
                ${reports.map((report) => `
                    <div class="report-card">
                        <div class="card-title-row">
                            <div>
                                <div class="card-title">${escapeHtml(report.title || report.report_id || '未命名报告')}</div>
                                <div class="card-kicker">${escapeHtml([displayLabel(report.report_type), formatDate(report.generated_at), report.as_of_date].filter(hasValue).join(' / '))}</div>
                            </div>
                            ${renderStatusChip(report.local_path ? 'confirmed' : 'missing')}
                        </div>
                        <div class="archive-path">${escapeHtml(report.local_path || '缺少本地报告路径')}</div>
                    </div>
                `).join('')}
            </div>
            ${payload.export_target ? `<div class="report-export-note">${escapeHtml(payload.export_target)}</div>` : ''}
        `;
    }

    function renderOpportunityUniverse() {
        const payload = state.data.opportunityUniverse || {};
        const counts = payload.counts || {};
        const sectors = asArray(payload.sectors);
        setText('opportunityUniverseSubtitle', `Registry ${payload.status || 'unknown'}，行业模板 ${counts.sector_registry || sectors.length || 0}，实体 ${counts.entity_registry || 0}，标的 ${counts.instrument_registry || 0}`);
        const root = document.getElementById('opportunityUniverse');
        if (!root) return;
        root.innerHTML = `
            <div class="universe-counts">
                ${renderObjectMetrics(counts, {
                    sector_registry: '行业',
                    theme_registry: '主题',
                    entity_registry: '实体',
                    instrument_registry: '标的',
                    mapping_registry: '映射',
                    model_registry: '模型',
                    thesis_registry: 'Thesis',
                    event_template_registry: '事件模板',
                })}
            </div>
            <div class="sector-registry-grid">
                ${sectors.map((sector) => `
                    <button class="sector-registry-card" type="button" data-dossier-type="sector" data-dossier-id="${escapeHtml(sector.sector_id)}">
                        <span>${escapeHtml(sector.name_zh || sector.sector_id)}</span>
                        <em>${escapeHtml(sector.sector_id)}</em>
                    </button>
                `).join('') || renderMissing(['机会宇宙注册表未返回 sector_registry'])}
            </div>
        `;
    }

    function renderDossier(payload = null, type = '') {
        const root = document.getElementById('dossierPreview');
        if (!root) return;
        if (!payload) {
            root.innerHTML = `
                <div class="ll-empty">
                    <i class="bi bi-search"></i>
                    <div class="ll-empty-title">选择一个赛道、实体或标的</div>
                    <div class="ll-empty-copy">机会卡子载体和机会宇宙模板都可以打开 Dossier。</div>
                </div>
            `;
            return;
        }
        const sector = payload.sector || {};
        const entity = payload.entity || {};
        const instrument = payload.instrument || {};
        const title = sector.name_zh || entity.name_zh || instrument.name_zh || instrument.ticker || payload.sector_id || payload.entity_id || payload.instrument_id || 'Dossier';
        const reports = asArray(payload.related_reports);
        const evidence = asArray(payload.related_evidence);
        const opportunities = asArray(payload.current_opportunities || payload.related_opportunity_cards);
        const instruments = asArray(payload.instruments);
        root.innerHTML = `
            <div class="dossier-card">
                <div class="card-title-row">
                    <div>
                        <div class="card-title">${escapeHtml(title)}</div>
                        <div class="card-kicker">${escapeHtml(displayLabel(type))}</div>
                    </div>
                    ${renderStatusChip('confirmed')}
                </div>
                <div class="meta-row builder-meta">
                    ${renderOptionalMeta('Sector', sector.sector_id || entity.sector_ids || instrument.sector_id)}
                    ${renderOptionalMeta('Entity', entity.entity_id || instrument.entity_id)}
                    ${renderOptionalMeta('Market', instrument.market)}
                    ${renderOptionalMeta('Ticker', instrument.ticker)}
                </div>
                ${instruments.length ? `
                    <div class="operator-card-section">
                        <div class="section-label">相关上市载体</div>
                        ${renderList(instruments.map((item) => [item.name_zh || item.ticker, item.ticker, displayLabel(item.market)].filter(hasValue).join(' / ')))}
                    </div>
                ` : ''}
                ${opportunities.length ? `
                    <div class="operator-card-section">
                        <div class="section-label">相关机会</div>
                        ${renderList(opportunities.slice(0, 6).map((item) => item.thesis || item.thesis_title || item.id))}
                    </div>
                ` : ''}
                ${evidence.length ? `
                    <div class="operator-card-section">
                        <div class="section-label">相关证据</div>
                        ${renderEvidencePanel(evidence)}
                    </div>
                ` : ''}
                ${reports.length ? `
                    <div class="operator-card-section">
                        <div class="section-label">相关报告</div>
                        ${renderList(reports.slice(0, 6).map((item) => [item.title, item.local_path].filter(hasValue).join(' / ')))}
                    </div>
                ` : ''}
                ${payload.chain_map ? `
                    <div class="operator-card-section">
                        <div class="section-label">传导路径位置</div>
                        ${renderObjectMetrics(payload.chain_map)}
                    </div>
                ` : ''}
            </div>
        `;
    }

    function dossierEndpoint(type, id) {
        const encoded = encodeURIComponent(id || '');
        if (type === 'sector') return `${API_PREFIX}/dossier/sector/${encoded}`;
        if (type === 'entity') return `${API_PREFIX}/dossier/entity/${encoded}`;
        return `${API_PREFIX}/dossier/instrument/${encoded}`;
    }

    async function loadDossier(type, id) {
        if (!hasValue(id)) return;
        setText('dossierSubtitle', `加载 ${displayLabel(type)}：${id}`);
        activateLeadLagTab('universe');
        try {
            const payload = await fetchSection(dossierEndpoint(type, id), {});
            renderDossier(payload, type);
            setText('dossierSubtitle', `${displayLabel(type)} Dossier 已加载：${id}`);
        } catch (error) {
            console.warn('[lead-lag] dossier load failed', error);
            setText('dossierSubtitle', `Dossier 加载失败：${id}`);
        }
    }

    function extractList(payload, keys = []) {
        if (Array.isArray(payload)) return payload.filter(hasValue);
        if (!payload || typeof payload !== 'object') return [];
        for (const key of keys) {
            if (Array.isArray(payload[key])) return payload[key].filter(hasValue);
        }
        return [];
    }

    function normalizePayload(payload) {
        const opportunityPayload = payload.opportunityQueue || {};
        const opportunityPayloadIsList = Array.isArray(opportunityPayload);
        return {
            overview: payload.overview || EMPTY_OVERVIEW,
            decisionCenter: payload.decisionCenter || {},
            opportunityQueuePayload: opportunityPayloadIsList ? { cards: opportunityPayload, count: opportunityPayload.length } : opportunityPayload,
            opportunityQueue: extractList(opportunityPayload, ['cards', 'items']),
            parentThesisCards: opportunityPayloadIsList ? [] : extractList(opportunityPayload, ['parent_thesis_cards']),
            blockedCards: opportunityPayloadIsList ? [] : extractList(opportunityPayload, ['blocked_cards', 'sample_cards']),
            opportunityQueueGroups: opportunityPayloadIsList ? [] : extractList(opportunityPayload, ['model_groups', 'groups']),
            whatChanged: payload.whatChanged || {},
            eventFrontline: extractList(payload.eventFrontline, ['events', 'items']),
            avoidBoard: extractList(payload.avoidBoard, ['items']),
            transmissionWorkspace: payload.transmissionWorkspace || {},
            replayDiagnostics: payload.replayDiagnostics || {},
            memoryActions: payload.memoryActions || {},
            sectorEvidence: extractList(payload.sectorEvidence, ['sectors', 'items']),
            models: asArray(payload.models),
            crossMarket: asArray(payload.crossMarket),
            transmission: asArray(payload.transmission),
            liquidity: asArray(payload.liquidity),
            thesis: asArray(payload.thesis),
            events: asArray(payload.events),
            validation: asArray(payload.validation),
            memory: asArray(payload.memory),
            sourceQualityLineage: payload.sourceQualityLineage || {},
            reportCenter: payload.reportCenter || {},
            opportunityUniverse: payload.opportunityUniverse || {},
        };
    }

    async function loadLeadLag() {
        setLoading(true);
        state.v2Fallbacks = new Set();

        const [overview, v1Opportunities, v1Events] = await Promise.all([
            fetchSection(ENDPOINTS.v1.overview, EMPTY_OVERVIEW),
            fetchSection(ENDPOINTS.v1.opportunities, []),
            fetchSection(ENDPOINTS.v1.events, []),
        ]);

        const [
            decisionCenter,
            opportunityQueue,
            whatChanged,
            eventFrontline,
            avoidBoard,
            models,
            crossMarket,
            transmission,
            liquidity,
            thesis,
            transmissionWorkspace,
            replayDiagnostics,
            memoryActions,
            sectorEvidence,
            validation,
            memory,
            sourceQualityLineage,
            reportCenter,
            opportunityUniverse,
        ] = await Promise.all([
            fetchV2WithFallback('decision-center', ENDPOINTS.v2.decisionCenter, ENDPOINTS.v1.overview, {}, (payload) => mapV1DecisionCenter(payload, v1Opportunities)),
            fetchV2WithFallback('opportunity-queue', ENDPOINTS.v2.opportunityQueue, ENDPOINTS.v1.opportunities, [], (payload) => asArray(payload).map(mapV1Opportunity)),
            fetchV2WithFallback('what-changed', ENDPOINTS.v2.whatChanged, ENDPOINTS.v1.overview, {}, mapV1WhatChanged),
            fetchV2WithFallback('event-frontline', ENDPOINTS.v2.eventFrontline, ENDPOINTS.v1.events, [], mapV1EventFrontline, { include_research_facing: state.filters.include_research_facing }),
            fetchV2WithFallback('avoid-board', ENDPOINTS.v2.avoidBoard, ENDPOINTS.v1.opportunities, [], mapV1AvoidBoard),
            fetchSection(ENDPOINTS.v1.models, []),
            fetchSection(ENDPOINTS.v1.crossMarket, []),
            fetchSection(ENDPOINTS.v1.transmission, []),
            fetchSection(ENDPOINTS.v1.liquidity, []),
            fetchSection(ENDPOINTS.v1.thesis, []),
            fetchSection(ENDPOINTS.v2.transmissionWorkspace, {}),
            fetchSection(ENDPOINTS.v2.replayDiagnostics, {}),
            fetchSection(ENDPOINTS.v2.memoryActions, {}),
            fetchSection(ENDPOINTS.v2.sectorEvidence, {}),
            fetchSection(ENDPOINTS.v1.validation, []),
            fetchSection(ENDPOINTS.v1.memory, []),
            fetchSection(ENDPOINTS.v2.sourceQualityLineage, {}),
            fetchSection(ENDPOINTS.v2.reportCenter, {}),
            fetchSection(ENDPOINTS.v2.opportunityUniverse, {}),
        ]);

        state.data = normalizePayload({
            overview,
            decisionCenter,
            opportunityQueue,
            whatChanged,
            eventFrontline,
            avoidBoard,
            models,
            crossMarket,
            transmission,
            liquidity,
            thesis,
            transmissionWorkspace,
            replayDiagnostics,
            memoryActions,
            sectorEvidence,
            events: v1Events,
            validation,
            memory,
            sourceQualityLineage,
            reportCenter,
            opportunityUniverse,
        });
        state.lastUpdatedAt = new Date().toISOString();

        renderOverviewControls();
        renderDecisionCenter();
        renderModelDiscoveryGroups();
        renderOpportunityQueue();
        renderWhatChanged();
        renderEventFrontline();
        renderAvoidBoard();
        renderModels();
        renderCrossMarket();
        renderTransmission();
        renderLiquidity();
        renderThesis();
        renderEvents();
        renderValidation();
        renderMemory();
        renderSourceQualityLineage();
        renderReportCenter();
        renderOpportunityUniverse();
        renderDossier();
        setLoading(false);
    }

    function activateLeadLagTab(tabId) {
        const target = tabId || 'decision';
        document.querySelectorAll('[data-ll-tab]').forEach((button) => {
            const active = button.getAttribute('data-ll-tab') === target;
            button.classList.toggle('is-active', active);
            button.setAttribute('aria-selected', String(active));
        });
        document.querySelectorAll('[data-ll-tab-panel]').forEach((panel) => {
            panel.classList.toggle('is-active', panel.getAttribute('data-ll-tab-panel') === target);
        });
        try {
            localStorage.setItem('lead-lag-active-tab', target);
        } catch (error) {
            // Ignore storage errors.
        }
    }

    function bindTabEvents() {
        document.querySelectorAll('[data-ll-tab]').forEach((button) => {
            button.addEventListener('click', () => activateLeadLagTab(button.getAttribute('data-ll-tab')));
        });
        document.querySelectorAll('[data-ll-tab-jump]').forEach((button) => {
            button.addEventListener('click', () => activateLeadLagTab(button.getAttribute('data-ll-tab-jump')));
        });
        try {
            const stored = localStorage.getItem('lead-lag-active-tab');
            if (stored) activateLeadLagTab(stored);
        } catch (error) {
            // Ignore storage errors.
        }
    }

    function bindEvents() {
        const refreshBtn = document.getElementById('leadLagRefreshBtn');
        const asOf = document.getElementById('leadLagAsOf');
        const region = document.getElementById('leadLagRegion');
        const regime = document.getElementById('leadLagRegime');
        const family = document.getElementById('leadLagFamily');
        const search = document.getElementById('leadLagSearch');
        const includeResearch = document.getElementById('includeResearchFacing');
        const includeSample = document.getElementById('leadLagIncludeSample');
        const liveOnly = document.getElementById('leadLagLiveOnly');
        const archivedOnly = document.getElementById('leadLagArchivedOnly');
        let searchTimer = null;

        if (refreshBtn) refreshBtn.addEventListener('click', loadLeadLag);
        bindTabEvents();
        document.addEventListener('click', (event) => {
            const target = event.target instanceof Element ? event.target : event.target?.parentElement;
            const trigger = target?.closest('[data-dossier-type][data-dossier-id]');
            if (!trigger) return;
            loadDossier(trigger.getAttribute('data-dossier-type'), trigger.getAttribute('data-dossier-id'));
        });
        if (asOf) {
            asOf.addEventListener('change', (event) => {
                state.filters.as_of = event.target.value;
                loadLeadLag();
            });
        }
        if (region) {
            region.addEventListener('change', (event) => {
                state.filters.region = event.target.value;
                loadLeadLag();
            });
        }
        if (regime) {
            regime.addEventListener('change', (event) => {
                state.filters.regime = event.target.value;
                loadLeadLag();
            });
        }
        if (family) {
            family.addEventListener('change', (event) => {
                state.filters.family = event.target.value;
                loadLeadLag();
            });
        }
        if (search) {
            search.addEventListener('input', (event) => {
                window.clearTimeout(searchTimer);
                searchTimer = window.setTimeout(() => {
                    state.filters.q = event.target.value.trim();
                    loadLeadLag();
                }, 250);
            });
        }
        if (includeResearch) {
            includeResearch.addEventListener('change', (event) => {
                state.filters.include_research_facing = event.target.checked;
                loadLeadLag();
            });
        }
        if (includeSample) {
            includeSample.addEventListener('change', (event) => {
                state.filters.include_sample = event.target.checked;
                loadLeadLag();
            });
        }
        if (liveOnly) {
            liveOnly.addEventListener('change', (event) => {
                state.filters.live_only = event.target.checked;
                loadLeadLag();
            });
        }
        if (archivedOnly) {
            archivedOnly.addEventListener('change', (event) => {
                state.filters.archived_only = event.target.checked;
                loadLeadLag();
            });
        }
    }

    document.addEventListener('DOMContentLoaded', () => {
        bindEvents();
        loadLeadLag();
    });
})();
