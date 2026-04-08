(function () {
  const DEFAULT_SENSITIVITY = "integrated";
  const DEFAULT_LOOKBACK_DAYS = 30;

  const els = {
    stockQuery: document.getElementById("stockQuery"),
    dateFrom: document.getElementById("dateFrom"),
    dateTo: document.getElementById("dateTo"),
    runSearch: document.getElementById("runSearch"),
    resetSearch: document.getElementById("resetSearch"),
    exportCsv: document.getElementById("exportCsv"),
    exportRawCsv: document.getElementById("exportRawCsv"),
    stockBrief: document.getElementById("stockBrief"),
    statNews: document.getElementById("statNews"),
    statEvents: document.getElementById("statEvents"),
    statDirect: document.getElementById("statDirect"),
    statImplicit: document.getElementById("statImplicit"),
    reasonStrip: document.getElementById("reasonStrip"),
    intelBoard: document.getElementById("intelBoard"),
    timelineBoard: document.getElementById("timelineBoard"),
    results: document.getElementById("results")
  };

  const state = {
    profile: null,
    overview: null,
    meta: null,
    events: [],
    evidenceRows: [],
    visibleEvidenceRows: [],
    requestToken: 0,
    fullDataReady: false
  };

  // 标签映射集中定义，避免在函数内重复构造对象。
  const FAMILY_LABELS = {
    fundamental: "基本面",
    chain: "产业链",
    macro: "宏观地缘",
    narrative: "市场叙事"
  };
  const SOURCE_TYPE_LABELS = {
    cninfoAnnouncement: "巨潮资讯公告",
    stockNews: "新浪个股资讯",
    industryNews: "新浪行业资讯",
    bulletin: "新浪公司公告",
    eastmoneyFocus: "东方财富焦点资讯",
    eastmoneyFastNews: "东方财富快讯",
    csMarketNews: "中证网财经要闻",
    search360: "360 检索补召回",
    sogouSearch: "搜狗检索补召回",
    nmpaOfficial: "国家药监局公告",
    nhsaOfficial: "国家医保局官方",
    miitOfficial: "工信部官方",
    ndrcOfficial: "国家发改委官方"
  };
  const RELATION_TIER_LABELS = {
    hard: "硬关联",
    semi: "半显式关联",
    mapped: "映射关联",
    soft: "软关联",
    watch: "候选观察"
  };
  const RELATION_TIER_CLASSES = {
    hard: "hard",
    semi: "semi",
    mapped: "mapped",
    soft: "soft",
    watch: "watch"
  };
  const EVIDENCE_TYPE_LABELS = {
    fact: "事实",
    quant: "量化",
    policy: "政策",
    path: "传导",
    impact: "影响",
    narrative: "叙事"
  };
  const EVIDENCE_TYPE_CLASSES = {
    fact: "fact",
    quant: "quant",
    policy: "policy",
    path: "path",
    impact: "impact",
    narrative: "narrative"
  };
  const ROUTE_TAG_LABELS = {
    medical: "医药",
    technology: "科技",
    semiconductor: "半导体",
    shipping: "航运",
    travel: "出行",
    new_energy: "新能源",
    energy: "能源",
    commodity: "大宗商品",
    defense: "军工",
    finance: "金融",
    consumer: "消费",
    media: "传媒",
    property: "地产链",
    infrastructure: "基建",
    industrial: "制造业",
    macro_sensitive: "宏观敏感",
    company_news: "公司线索",
    policy: "政策敏感"
  };

  function familyLabel(family) {
    return FAMILY_LABELS[family] || "候选信号";
  }

  function sourceTypeLabel(sourceType) {
    return SOURCE_TYPE_LABELS[sourceType] || sourceType;
  }

  function relationTierLabel(tier) {
    return RELATION_TIER_LABELS[tier] || "候选观察";
  }

  function relationTierClass(tier) {
    return RELATION_TIER_CLASSES[tier] || "watch";
  }

  function evidenceTypeLabel(type) {
    return EVIDENCE_TYPE_LABELS[type] || "线索";
  }

  function evidenceTypeClass(type) {
    return EVIDENCE_TYPE_CLASSES[type] || "fact";
  }

  function routeTagLabel(tag) {
    return ROUTE_TAG_LABELS[tag] || tag;
  }

  function escapeHtml(text) {
    return String(text || "")
      .replaceAll("&", "&amp;")
      .replaceAll("<", "&lt;")
      .replaceAll(">", "&gt;")
      .replaceAll('"', "&quot;")
      .replaceAll("'", "&#39;");
  }

  function downloadBlob(blob, filename) {
    const link = document.createElement("a");
    link.href = URL.createObjectURL(blob);
    link.download = filename;
    link.click();
    URL.revokeObjectURL(link.href);
  }

  async function readErrorMessage(response) {
    try {
      const payload = await response.json();
      return asText(payload && payload.error);
    } catch (_error) {
      return "";
    }
  }

  async function downloadExportFile(url, filename) {
    const response = await fetch(url, {
      method: "POST",
      headers: {
        "Content-Type": "application/json"
      },
      body: JSON.stringify({
        filename,
        rows: state.visibleEvidenceRows
      })
    });

    if (!response.ok) {
      const errorMessage = await readErrorMessage(response);
      throw new Error(errorMessage || "导出接口不可用。");
    }

    const blob = await response.blob();
    downloadBlob(blob, filename);
  }

  function asText(value) {
    return value == null ? "" : String(value).trim();
  }

  function toNumber(value, fallback = 0) {
    const result = Number(value);
    return Number.isFinite(result) ? result : fallback;
  }

  function ensureArray(value) {
    if (Array.isArray(value)) {
      return value.filter((item) => item != null && item !== "");
    }

    if (value == null || value === "") {
      return [];
    }

    return [value];
  }

  function normalizeCountItem(item) {
    if (!item) {
      return null;
    }

    if (typeof item === "string") {
      return {
        key: item,
        value: item,
        count: 0
      };
    }

    if (typeof item === "object") {
      const key = asText(item.key || item.value || item.label);
      if (!key) {
        return null;
      }

      return {
        key,
        value: asText(item.value || item.key || item.label),
        count: toNumber(item.count)
      };
    }

    return null;
  }

  function normalizeEvidenceKey(text) {
    return asText(text)
      .replace(/[\r\n\t]+/g, " ")
      .replace(/\s{2,}/g, " ")
      .trim();
  }

  function uniqueTextValues(values) {
    // 保留原顺序去重，避免同一来源/标题在页面重复出现。
    const seen = new Set();
    const result = [];

    ensureArray(values).forEach((value) => {
      const text = asText(value);
      if (!text) {
        return;
      }

      const key = text.replace(/\s+/g, " ").trim();
      if (!key || seen.has(key)) {
        return;
      }

      seen.add(key);
      result.push(text);
    });

    return result;
  }

  function joinExportValues(values, take = 0) {
    const normalized = uniqueTextValues(values);
    return (take > 0 ? normalized.slice(0, take) : normalized).join("\n");
  }

  function normalizeSupportingSource(source) {
    if (!source || typeof source !== "object") {
      return null;
    }

    const normalized = {
      publishedAt: asText(source.published_at || source.publishedAt),
      sourceType: asText(source.source_type || source.sourceType),
      sourceLabel: asText(source.source_label || source.sourceLabel),
      sourceSite: asText(source.source_site || source.sourceSite),
      eventTitle: asText(source.event_title || source.eventTitle),
      articleTitle: asText(source.article_title || source.articleTitle),
      url: asText(source.url),
      originUrl: asText(source.origin_url || source.originUrl)
    };

    if (!Object.values(normalized).some(Boolean)) {
      return null;
    }

    return normalized;
  }

  function supportingSourceKey(source) {
    const normalized = normalizeSupportingSource(source);
    if (!normalized) {
      return "";
    }

    return [
      normalized.sourceLabel,
      normalized.sourceSite,
      normalized.articleTitle,
      normalized.url,
      normalized.originUrl
    ].join("||").toLowerCase();
  }

  function mergeSupportingSourceMap(target, source) {
    const normalized = normalizeSupportingSource(source);
    const key = supportingSourceKey(normalized);
    if (!normalized || !key) {
      return;
    }

    const existing = target.get(key);
    if (!existing) {
      target.set(key, normalized);
      return;
    }

    if (normalized.publishedAt > existing.publishedAt) {
      existing.publishedAt = normalized.publishedAt;
    }

    [
      "sourceType",
      "sourceLabel",
      "sourceSite",
      "eventTitle",
      "articleTitle",
      "url",
      "originUrl"
    ].forEach((field) => {
      if (!existing[field] && normalized[field]) {
        existing[field] = normalized[field];
      }
    });
  }

  function entrySupportingSources(entry) {
    // 统一来源去重，保证“每条证据 -> 全来源”展示稳定。
    const grouped = new Map();

    ensureArray(entry && entry.supportingSources)
      .map(normalizeSupportingSource)
      .filter(Boolean)
      .forEach((source) => {
        mergeSupportingSourceMap(grouped, source);
      });

    if (!grouped.size && entry) {
      mergeSupportingSourceMap(grouped, {
        publishedAt: entry.publishedAt || "",
        sourceType: entry.sourceType || "",
        sourceLabel: entry.sourceLabel || "",
        sourceSite: entry.sourceSite || "",
        eventTitle: entry.eventTitle || "",
        articleTitle: entry.articleTitle || "",
        url: entry.url || "",
        originUrl: entry.originUrl || ""
      });
    }

    return [...grouped.values()].sort((left, right) => {
      const timeGap = String(right.publishedAt || "").localeCompare(String(left.publishedAt || ""));
      if (timeGap !== 0) {
        return timeGap;
      }

      return String(left.articleTitle || "").localeCompare(String(right.articleTitle || ""));
    });
  }

  function evidenceSourceLabels(entry) {
    const labels = uniqueTextValues([
      ...entrySupportingSources(entry).map((source) => source.sourceLabel || sourceTypeLabel(source.sourceType)),
      ...ensureArray(entry && entry.sourceLabels),
      asText(entry && entry.sourceLabel)
    ]);

    if (labels.length) {
      return labels;
    }

    return uniqueTextValues([entry && entry.sourceType ? sourceTypeLabel(entry.sourceType) : ""]);
  }

  function evidenceSourceSites(entry) {
    return uniqueTextValues([
      ...entrySupportingSources(entry).map((source) => source.sourceSite),
      ...ensureArray(entry && entry.sourceSites),
      asText(entry && entry.sourceSite)
    ]);
  }

  function evidenceArticleTitles(entry) {
    return uniqueTextValues([
      ...entrySupportingSources(entry).map((source) => source.articleTitle),
      ...ensureArray(entry && entry.articleTitles),
      asText(entry && entry.articleTitle)
    ]);
  }

  function evidenceEventTitles(entry) {
    return uniqueTextValues([
      ...entrySupportingSources(entry).map((source) => source.eventTitle),
      ...ensureArray(entry && entry.eventTitles),
      asText(entry && entry.eventTitle)
    ]);
  }

  function evidenceSourceLinks(entry) {
    const values = [];

    entrySupportingSources(entry).forEach((source) => {
      if (source.url) {
        values.push(source.url);
      }
      if (source.originUrl && source.originUrl !== source.url) {
        values.push(source.originUrl);
      }
    });

    return uniqueTextValues([
      ...values,
      ...ensureArray(entry && entry.sourceUrls),
      ...ensureArray(entry && entry.originUrls),
      asText(entry && entry.url),
      asText(entry && entry.originUrl)
    ]);
  }

  function evidenceSourceSummary(entry, take = 3) {
    const labels = evidenceSourceLabels(entry);
    if (!labels.length) {
      return "暂无";
    }

    if (labels.length <= take) {
      return labels.join(" / ");
    }

    return `${labels.slice(0, take).join(" / ")} 等 ${labels.length} 个来源`;
  }

  function renderPlainTags(values, emptyText = "暂无") {
    const normalized = uniqueTextValues(values);
    if (!normalized.length) {
      return `<span class="tag muted">${escapeHtml(emptyText)}</span>`;
    }

    return normalized
      .map((value) => `<span class="tag">${escapeHtml(value)}</span>`)
      .join("");
  }

  function renderSupportingSourceLinks(entry) {
    const sources = entrySupportingSources(entry);
    if (!sources.length) {
      return `<span class="tag muted">暂无来源链接</span>`;
    }

    return sources
      .map((source, index) => {
        const href = source.url || source.originUrl;
        const labelParts = uniqueTextValues([
          source.sourceLabel || sourceTypeLabel(source.sourceType),
          source.articleTitle
        ]);
        const label = labelParts.join(" | ") || `来源 ${index + 1}`;

        if (!href) {
          return `<span class="tag">${escapeHtml(label)}</span>`;
        }

        return `<a class="tag" href="${escapeHtml(href)}" target="_blank" rel="noreferrer">${escapeHtml(label)}</a>`;
      })
      .join("");
  }

  function applyDefaultDates(force = false) {
    if (!force && els.dateFrom.value && els.dateTo.value) {
      return;
    }

    const end = new Date();
    const start = new Date();
    start.setDate(start.getDate() - DEFAULT_LOOKBACK_DAYS);

    els.dateTo.value = end.toISOString().slice(0, 10);
    els.dateFrom.value = start.toISOString().slice(0, 10);
  }

  function countLookup(items) {
    const map = {};
    ensureArray(items).forEach((item) => {
      const normalized = normalizeCountItem(item);
      if (normalized && normalized.key) {
        map[normalized.key] = normalized.count || 0;
      }
    });
    return map;
  }

  function buildOverviewSentence(overview) {
    if (!overview) {
      return "等待查询后生成情报摘要。";
    }

    const dominantFamily = familyLabel(overview.dominantFamily);
    const relation = relationTierLabel(overview.dominantRelation);
    const scenarioHits = overview.scenarioHits || 0;
    return `当前窗口以${dominantFamily}信号为主，关联结构以${relation}为主，其中场景扩展命中 ${scenarioHits} 条。`;
  }

  function formatEvidenceMix(items) {
    const normalized = ensureArray(items).map(normalizeCountItem).filter(Boolean);
    if (!normalized.length) {
      return "暂无";
    }

    return normalized.map((item) => `${item.value} x${item.count || 0}`).join(" / ");
  }

  function formatTopPills(items, cssClass = "tag") {
    const normalizedItems = ensureArray(items).map(normalizeCountItem).filter(Boolean);

    if (!normalizedItems.length) {
      return `<span class="${cssClass} muted">暂无</span>`;
    }

    return normalizedItems
      .map((item) => `<span class="${cssClass}">${escapeHtml(item.value)}${item.count ? ` x${escapeHtml(item.count)}` : ""}</span>`)
      .join("");
  }

  function renderChipCloud(items, options = {}) {
    const {
      emptyText = "暂无",
      mapper = (value) => value,
      take = 12,
      className = "tag"
    } = options;

    const values = ensureArray(items)
      .map((item) => asText(item))
      .filter(Boolean)
      .slice(0, take);

    if (!values.length) {
      return `<span class="${className} muted">${escapeHtml(emptyText)}</span>`;
    }

    return values
      .map((value) => `<span class="${className}">${escapeHtml(mapper(value))}</span>`)
      .join("");
  }

  function buildBrief(profile, sources) {
    const routeTags = ensureArray(profile.routeTags).slice(0, 8);
    const sourceItems = ensureArray(sources);
    const keyFactors = [...new Set([...ensureArray(profile.sensitiveFactors), ...ensureArray(profile.themes)])].slice(0, 12);
    const entityItems = [...new Set([...ensureArray(profile.aliases), ...ensureArray(profile.subsidiaries), ...ensureArray(profile.products)])].slice(0, 10);
    els.stockBrief.classList.remove("empty");
    els.stockBrief.innerHTML = `
      <div class="brief-top">
        <div class="brief-head">
          <strong>${escapeHtml(profile.name)}（${escapeHtml(profile.code)}）</strong>
          <p>${escapeHtml(profile.description || "当前为实时解析出的个股画像。")}</p>
        </div>
        <div class="brief-side">
          <span class="brief-badge">${escapeHtml(profile.industry || "未识别行业")}</span>
          <span class="brief-badge">有效源 ${escapeHtml(sourceItems.length)}</span>
        </div>
      </div>
      <div class="brief-grid brief-grid-compact">
        <section class="brief-card">
          <span class="filter-label">自动路由画像</span>
          <div class="chip-cloud">${renderChipCloud(routeTags, { mapper: routeTagLabel, emptyText: "暂无路由", className: "tag" })}</div>
        </section>
        <section class="brief-card">
          <span class="filter-label">关键因子</span>
          <div class="chip-cloud">${renderChipCloud(keyFactors, { emptyText: "暂无因子", take: 12, className: "tag" })}</div>
        </section>
        <section class="brief-card">
          <span class="filter-label">政策观察与数据源</span>
          <div class="chip-cloud">${renderChipCloud(profile.policyAuthorities, { emptyText: "暂无监管画像", take: 8, className: "tag" })}</div>
          <div class="chip-cloud">${renderChipCloud(sourceItems, { emptyText: "实时多源", take: 8, className: "tag" })}</div>
        </section>
      </div>
      <details class="profile-details">
        <summary>展开扩展画像</summary>
        <div class="profile-details-body">
          <section class="brief-card">
            <span class="filter-label">实体与产品</span>
            <div class="chip-cloud">${renderChipCloud(entityItems, { emptyText: "暂无关键实体 / 产品", take: 10, className: "tag" })}</div>
          </section>
          <section class="brief-card">
            <span class="filter-label">场景扩展词</span>
            <div class="chip-cloud">${renderChipCloud(profile.scenarioQueries, { emptyText: "暂无场景扩展词", take: 12, className: "tag" })}</div>
          </section>
        </div>
      </details>
    `;
  }

  function renderReasonStrip(events) {
    if (!els.reasonStrip) {
      return;
    }
    const count = ensureArray(events).length;
    els.reasonStrip.innerHTML = `
      <span class="reason-tag muted">
        ${escapeHtml(buildOverviewSentence(state.overview))}${count ? ` 当前共归并为 ${escapeHtml(count)} 个事件。` : ""}
      </span>
    `;
  }

  function summarize(stats) {
    els.statNews.textContent = String(toNumber(stats && stats.candidateNews));
    els.statEvents.textContent = String(toNumber(stats && stats.events));
    els.statDirect.textContent = String(toNumber(stats && stats.directRelated));
    els.statImplicit.textContent = String(toNumber(stats && stats.mappedRelated));
  }

  function clearQueryState() {
    state.profile = null;
    state.overview = null;
    state.meta = null;
    state.events = [];
    state.evidenceRows = [];
    state.visibleEvidenceRows = [];
    state.fullDataReady = false;
  }

  // 统一证据排序，保证人工浏览时顺序稳定：时间 > 关联层级 > 分值。
  function sortEvidenceEntries(entries) {
    return [...entries].sort((left, right) => {
      const timeGap = String(right.publishedAt || "").localeCompare(String(left.publishedAt || ""));
      if (timeGap !== 0) {
        return timeGap;
      }

      const tierGap = toNumber(right.relationTierRank) - toNumber(left.relationTierRank);
      if (tierGap !== 0) {
        return tierGap;
      }

      const scoreGap = (toNumber(right.score) + toNumber(right.detailScore)) - (toNumber(left.score) + toNumber(left.detailScore));
      if (scoreGap !== 0) {
        return scoreGap;
      }

      return String(left.text || "").localeCompare(String(right.text || ""));
    });
  }

  function summarizeEvidence(entries) {
    const evidenceEntries = ensureArray(entries);
    const eventKeys = new Set();
    let directCount = 0;
    let softCount = 0;

    evidenceEntries.forEach((entry) => {
      evidenceEventTitles(entry).forEach((title) => {
        const key = normalizeEvidenceKey(title);
        if (key) {
          eventKeys.add(key);
        }
      });

      if (["hard", "semi"].includes(entry.relationTier)) {
        directCount += 1;
      }

      if (["mapped", "soft"].includes(entry.relationTier) || entry.signalFamily === "narrative") {
        softCount += 1;
      }
    });

    els.statNews.textContent = String(evidenceEntries.length);
    els.statEvents.textContent = String(eventKeys.size);
    els.statDirect.textContent = String(directCount);
    els.statImplicit.textContent = String(softCount);
  }

  function syncExportState() {
    if (!els.exportCsv && !els.exportRawCsv) {
      return;
    }

    const hasRows = state.visibleEvidenceRows.length > 0;
    const canExport = state.fullDataReady && hasRows;

    if (els.exportCsv) {
      els.exportCsv.disabled = !canExport;
      els.exportCsv.textContent = !state.fullDataReady ? "查询完成后可导出" : hasRows ? "导出 Excel" : "暂无证据可导出";
    }

    if (els.exportRawCsv) {
      els.exportRawCsv.disabled = !canExport;
      els.exportRawCsv.textContent = !state.fullDataReady ? "查询完成后可导出" : hasRows ? "导出 CSV" : "暂无证据可导出";
    }
  }

  function renderIntelBoard(overview, meta) {
    if (!overview) {
      els.intelBoard.innerHTML = "";
      return;
    }

    const relationCounts = countLookup(overview.relationCounts);
    const queryMeta = meta
      ? `${meta.cacheHit ? "缓存命中" : "实时抓取"}${meta.querySeconds != null ? ` / ${meta.querySeconds}s` : ""}`
      : "实时抓取";
    const evidenceStatus = meta
      ? `原始候选 ${toNumber(meta.rawCandidateCount)} / 去重后 ${toNumber(meta.dedupedCandidateCount)} / 正文确认 ${toNumber(meta.confirmedItems)}`
      : "等待查询";

    els.intelBoard.innerHTML = `
      <div class="intel-grid intel-grid-compact">
        <article class="intel-card">
          <span class="intel-label">抓取状态</span>
          <strong>${escapeHtml(queryMeta)}</strong>
          <p>${escapeHtml(evidenceStatus)}，前台主视图只展示正文确认后的证据。</p>
        </article>
        <article class="intel-card">
          <span class="intel-label">结构摘要</span>
          <strong>${escapeHtml(relationTierLabel(overview.dominantRelation))}</strong>
          <p>${escapeHtml(buildOverviewSentence(overview))}</p>
        </article>
        <article class="intel-card">
          <span class="intel-label">来源覆盖</span>
          <strong>${escapeHtml(`${toNumber(overview.sourceCount)} 个来源点`)}</strong>
          <p>硬关联 ${escapeHtml(relationCounts.hard || 0)} / 半显式 ${escapeHtml(relationCounts.semi || 0)} / 映射 ${escapeHtml(relationCounts.mapped || 0)}。</p>
        </article>
      </div>
      <div class="intel-meta-grid">
        <section class="intel-meta-card">
          <span class="filter-label">高频证据类型</span>
          <div class="match-meta">${formatTopPills(overview.topEvidenceTypes, "tag")}</div>
        </section>
        <section class="intel-meta-card">
          <span class="filter-label">重点站点</span>
          <div class="match-meta">${formatTopPills(overview.topSites, "tag")}</div>
        </section>
        <section class="intel-meta-card">
          <span class="filter-label">主要路径</span>
          <div class="match-meta">${formatTopPills(overview.topPaths, "tag")}</div>
        </section>
      </div>
    `;
  }

  function normalizeBackendEvidenceRows(rows) {
    // 后端字段统一转成前端可直接渲染的结构。
    return ensureArray(rows)
      .map((row, index) => ({
        key: `backend-${asText(row.row_id) || index + 1}`,
        text: asText(row.evidence_text),
        evidenceType: asText(row.evidence_type) || "fact",
        evidenceLabel: asText(row.evidence_label) || evidenceTypeLabel(row.evidence_type),
        detailScore: toNumber(row.detail_score),
        relationTier: asText(row.relation_tier),
        relationTierLabel: asText(row.relation_tier_label) || relationTierLabel(row.relation_tier),
        relationTierRank: toNumber(row.relation_tier_rank),
        relationReason: asText(row.relation_reason),
        score: toNumber(row.score),
        signalFamily: asText(row.signal_family),
        publishedAt: asText(row.published_at),
        contentMode: row.evidence_confirmed === false ? "summary" : "fulltext",
        evidenceConfirmed: row.evidence_confirmed !== false,
        sourceLabel: asText(row.source_label),
        sourceSite: asText(row.source_site),
        sourceType: asText(row.source_type),
        queryContext: asText(row.query_context),
        eventTitle: asText(row.event_title),
        articleTitle: asText(row.article_title),
        effectiveText: asText(row.evidence_text),
        rawText: "",
        url: asText(row.url),
        originUrl: asText(row.origin_url),
        scenarioTerms: ensureArray(row.scenario_terms),
        matchedEntities: ensureArray(row.matched_entities),
        paths: ensureArray(row.paths),
        reasons: ensureArray(row.reasons),
        sourceSites: ensureArray(row.source_sites),
        sourceLabels: ensureArray(row.source_labels),
        sourceUrls: ensureArray(row.source_urls),
        originUrls: ensureArray(row.origin_urls),
        articleTitles: ensureArray(row.article_titles),
        eventTitles: ensureArray(row.event_titles),
        supportingSources: ensureArray(row.supporting_sources).map(normalizeSupportingSource).filter(Boolean),
        occurrenceCount: toNumber(row.occurrence_count, 1)
      }))
      .filter((entry) => entry.text);
  }

  function getEvidenceEntries() {
    return state.evidenceRows;
  }

  function applyQueryPayload(data) {
    state.profile = data.profile || null;
    state.overview = data.overview || null;
    state.meta = data.meta || null;
    state.events = ensureArray(data.events);
    state.evidenceRows = normalizeBackendEvidenceRows(data.evidenceRows);
    state.fullDataReady = Boolean(data && data.ok);

    buildBrief(data.profile, ensureArray((data.stats || {}).sources));
    summarize(data.stats || {});
    renderReasonStrip(state.events);
    renderIntelBoard(data.overview || null, data.meta || null);
    renderTimeline(data.overview || null);
    renderResults(state.events);
    syncExportState();
  }

  async function fetchQueryPayload(query, from, to, detailLevel = "full") {
    const params = new URLSearchParams({
      stockQuery: query,
      dateFrom: from,
      dateTo: to,
      sensitivity: DEFAULT_SENSITIVITY,
      detailLevel
    });

    const response = await fetch(`/api/query?${params.toString()}`);
    const data = await response.json();

    if (!response.ok || !data.ok) {
      throw new Error(data.error || "查询失败");
    }

    return data;
  }

  function renderTimeline(overview) {
    const timeline = ensureArray(overview && overview.timeline);

    if (timeline.length < 2) {
      els.timelineBoard.innerHTML = "";
      return;
    }

    const visibleTimeline = timeline.slice(-8);
    const maxCount = Math.max(...visibleTimeline.map((item) => toNumber(item.count)), 1);

    els.timelineBoard.innerHTML = `
      <details class="timeline-panel">
        <summary>查看证据时间线</summary>
        <div class="timeline-panel-body">
          ${visibleTimeline
            .map((item) => {
              const width = `${Math.max(8, Math.round((toNumber(item.count) / maxCount) * 100))}%`;
              return `
                <div class="timeline-row">
                  <span>${escapeHtml(item.date || item.name || "-")}</span>
                  <div class="timeline-bar"><i style="width:${width}"></i></div>
                  <strong>${escapeHtml(toNumber(item.count))}</strong>
                </div>
              `;
            })
            .join("")}
        </div>
      </details>
    `;
  }

  function renderEmptyState(text) {
    els.results.innerHTML = `
      <section class="empty-state">
        <h3>还没有确认证据</h3>
        <p>${escapeHtml(text)}</p>
      </section>
    `;
  }

  function buildEvidenceCsvRows(entries) {
    // 导出以“证据”为单位，汇总该证据的全部来源信息。
    return ensureArray(entries).map((entry, index) => {
      const sourceLabels = evidenceSourceLabels(entry);
      const sourceSites = evidenceSourceSites(entry);
      const articles = evidenceArticleTitles(entry);
      const sourceLinks = evidenceSourceLinks(entry);

      return {
        序号: index + 1,
        股票代码: (state.profile && state.profile.code) || "",
        股票名称: (state.profile && state.profile.name) || "",
        证据时间: entry.publishedAt || "",
        证据文本: asText(entry.text || ""),
        关联层级: entry.relationTierLabel || relationTierLabel(entry.relationTier) || "",
        证据类型: entry.evidenceLabel || evidenceTypeLabel(entry.evidenceType) || "",
        来源渠道: joinExportValues(sourceLabels),
        来源站点: joinExportValues(sourceSites),
        来源文章: joinExportValues(articles),
        原始链接: joinExportValues(sourceLinks)
      };
    });
  }

  function summarizeEvidenceEntries(entries) {
    const relationTiers = {};
    const evidenceTypes = {};
    const sources = {};
    const eventKeys = new Set();

    ensureArray(entries).forEach((entry) => {
      const relationTier = asText(entry.relationTier);
      const evidenceType = asText(entry.evidenceType);
      const sourcesForEntry = evidenceSourceLabels(entry);

      if (relationTier) {
        relationTiers[relationTier] = (relationTiers[relationTier] || 0) + 1;
      }
      if (evidenceType) {
        evidenceTypes[evidenceType] = (evidenceTypes[evidenceType] || 0) + 1;
      }
      sourcesForEntry.forEach((source) => {
        sources[source] = (sources[source] || 0) + 1;
      });

      evidenceEventTitles(entry).forEach((title) => {
        const key = normalizeEvidenceKey(title);
        if (key) {
          eventKeys.add(key);
        }
      });
    });

    const toCountItems = (map, labeler) =>
      Object.entries(map)
        .map(([key, count]) => ({
          key,
          value: labeler(key),
          count
        }))
        .sort((a, b) => b.count - a.count);

    return {
      eventCount: eventKeys.size,
      relationTiers: toCountItems(relationTiers, relationTierLabel),
      evidenceTypes: toCountItems(evidenceTypes, evidenceTypeLabel),
      sources: toCountItems(sources, (value) => value)
    };
  }

  function renderResults(events) {
    if (!events.length) {
      state.visibleEvidenceRows = [];
      renderEmptyState("当前条件下没有筛出确认证据，你可以放宽日期范围，或者换一只股票再试。");
      return;
    }

    const evidenceEntries = getEvidenceEntries();
    const sortedEvidenceEntries = sortEvidenceEntries(evidenceEntries);

    state.visibleEvidenceRows = buildEvidenceCsvRows(sortedEvidenceEntries);
    summarizeEvidence(sortedEvidenceEntries);

    if (!sortedEvidenceEntries.length) {
      renderEmptyState("当前条件下没有确认证据，换一个日期范围或股票再试。");
      return;
    }

    const evidenceSummary = summarizeEvidenceEntries(sortedEvidenceEntries);

    const summaryBanner = `
      <section class="evidence-summary-card">
        <div class="evidence-summary-grid">
          <article class="summary-metric">
            <span class="filter-label">前台单位</span>
            <strong>${escapeHtml(evidenceEntries.length)}</strong>
            <p>一张卡片 = 一条有效证据，不是整篇文章。</p>
          </article>
          <article class="summary-metric">
            <span class="filter-label">覆盖事件</span>
            <strong>${escapeHtml(evidenceSummary.eventCount)}</strong>
            <p>便于按证据粒度扫，再回看它对应哪些事件。</p>
          </article>
          <article class="summary-metric">
            <span class="filter-label">主要来源</span>
            <strong>${escapeHtml(formatEvidenceMix(evidenceSummary.sources.slice(0, 3)))}</strong>
            <p>已按时间倒序展开，便于先扫近因，再回看关联层级。</p>
          </article>
        </div>
        <div class="evidence-summary-meta">
          <div>
            <span class="filter-label">关联层级</span>
            <div class="match-meta">${formatTopPills(evidenceSummary.relationTiers.slice(0, 5), "tag")}</div>
          </div>
          <div>
            <span class="filter-label">证据类型</span>
            <div class="match-meta">${formatTopPills(evidenceSummary.evidenceTypes.slice(0, 5), "tag")}</div>
          </div>
          <div>
            <span class="filter-label">主要来源</span>
            <div class="match-meta">${formatTopPills(evidenceSummary.sources.slice(0, 6), "tag")}</div>
          </div>
        </div>
      </section>
    `;

    // 结果区只渲染证据流，不再渲染来源/类型筛选控件。
    els.results.innerHTML = summaryBanner + `
      <section class="evidence-day-list">
        ${sortedEvidenceEntries
          .map((entry) => {
        const sourceLabels = evidenceSourceLabels(entry);
        const sourceSites = evidenceSourceSites(entry);
        const sourceLinks = evidenceSourceLinks(entry);
        const sourceCount = entrySupportingSources(entry).length || Math.max(sourceLabels.length, sourceSites.length, sourceLinks.length, 1);
        const pathSummary = ensureArray(entry.paths).slice(0, 2).join(" / ") || "暂无";
        const entitySummary = ensureArray(entry.matchedEntities).slice(0, 6).join("、") || "暂无";
        const sourceSiteSummary = sourceSites.join(" / ") || "暂无";
        const eventTitles = evidenceEventTitles(entry);
        const eventSummary = eventTitles.slice(0, 3).join(" / ") || "暂无";
        const eventSummaryFull = eventTitles.join(" / ") || "暂无";
        const articleTitles = evidenceArticleTitles(entry);
        const articleSummary = articleTitles.slice(0, 3).join(" / ") || "暂无";
        const articleSummaryFull = articleTitles.join(" / ") || "暂无";
        const primaryLink = entry.url || sourceLinks[0] || "";
        const originLink = entry.originUrl && entry.originUrl !== primaryLink
          ? `<a class="link subtle-link" href="${escapeHtml(entry.originUrl)}" target="_blank" rel="noreferrer">聚合入口</a>`
          : "";
        const entityTags = ensureArray(entry.matchedEntities)
          .slice(0, 8)
          .map((entity) => `<span class="tag">${escapeHtml(entity)}</span>`)
          .join("");
        const pathTags = ensureArray(entry.paths)
          .slice(0, 4)
          .map((path) => `<span class="tag">${escapeHtml(path)}</span>`)
          .join("");
        const compactTags = [
          ...ensureArray(entry.matchedEntities).slice(0, 4).map((entity) => `<span class="tag">${escapeHtml(entity)}</span>`),
          ...ensureArray(entry.paths).slice(0, 2).map((path) => `<span class="tag">${escapeHtml(path)}</span>`)
        ].join("");

        return `
          <article class="evidence-stream-card">
            <div class="meta-row">
              <span class="evidence-badge ${evidenceTypeClass(entry.evidenceType)}">${escapeHtml(entry.evidenceLabel || "线索")}</span>
              <span class="relation-pill ${relationTierClass(entry.relationTier)}">${escapeHtml(entry.relationTierLabel || relationTierLabel(entry.relationTier))}</span>
              <span class="tag">${escapeHtml(`来源 ${sourceCount}`)}</span>
              <span class="tag">${escapeHtml(entry.publishedAt || "-")}</span>
            </div>

            <div class="evidence-quote">${escapeHtml(entry.text || "")}</div>

            <div class="evidence-chip-row">
              ${renderPlainTags(sourceLabels, "暂无来源")}
            </div>

            <div class="evidence-context-grid">
              <div class="context-card">
                <span class="filter-label">为什么相关</span>
                <p>${escapeHtml(entry.relationReason || "当前结果基于正文命中和路径映射生成。")}</p>
                <p class="mini-note">${escapeHtml(pathSummary)}</p>
              </div>
              <div class="context-card">
                <span class="filter-label">来源与落点</span>
                <p>${escapeHtml(`${entry.queryContext || "综合线索"} / ${evidenceSourceSummary(entry)}`)}</p>
                <p class="mini-note">${escapeHtml(`${articleSummary} / ${eventSummary}`)}</p>
              </div>
            </div>

            <div class="evidence-chip-row">
              ${compactTags || `<span class="tag muted">暂无补充标签</span>`}
            </div>
            <div class="evidence-footer">
              <div class="article-link-row">
                <span class="filter-label">原始链接</span>
                ${primaryLink
                  ? `<a class="link" href="${escapeHtml(primaryLink)}" target="_blank" rel="noreferrer">打开主来源</a>`
                  : `<span class="tag muted">暂无主链接</span>`}
                ${originLink}
              </div>
              <details class="system-notes">
              <summary>查看溯源细节</summary>
              <div class="system-meta">
                <div class="detail-grid detail-grid-wide">
                  <div class="detail-card">
                    <span class="filter-label">来源渠道</span>
                    <p>${escapeHtml(sourceLabels.join(" / ") || "暂无")}</p>
                  </div>
                  <div class="detail-card">
                    <span class="filter-label">对应事件</span>
                    <p>${escapeHtml(eventSummaryFull)}</p>
                  </div>
                  <div class="detail-card">
                    <span class="filter-label">命中实体</span>
                    <p>${escapeHtml(entitySummary)}</p>
                  </div>
                  <div class="detail-card">
                    <span class="filter-label">来源文章</span>
                    <p>${escapeHtml(articleSummaryFull)}</p>
                  </div>
                  <div class="detail-card">
                    <span class="filter-label">来源站点</span>
                    <p>${escapeHtml(sourceSiteSummary)}</p>
                  </div>
                  <div class="detail-card">
                    <span class="filter-label">来源链接</span>
                    <p>${escapeHtml(`${sourceLinks.length} 条`)}</p>
                  </div>
                </div>
                <div class="match-meta">
                  ${renderSupportingSourceLinks(entry)}
                </div>
                <div class="match-meta">
                  ${pathTags || `<span class="tag muted">暂无路径</span>`}
                  ${entityTags}
                  <span class="tag">合并 ${escapeHtml(entry.occurrenceCount || 1)} 条</span>
                </div>
              </div>
              </details>
            </div>
          </article>
        `;
      })
          .join("")}
      </section>
    `;
  }

  async function runSearch() {
    const query = els.stockQuery.value.trim();
    const from = els.dateFrom.value;
    const to = els.dateTo.value;

    if (!query) {
      renderEmptyState("先输入一只 A 股股票名称或代码，我再帮你拉出这段时间的确认证据。");
      return;
    }

    state.requestToken += 1;
    // token 用于忽略慢请求回包，防止旧结果覆盖新结果。
    const requestToken = state.requestToken;
    state.fullDataReady = false;
    syncExportState();
    els.runSearch.disabled = true;
    els.runSearch.textContent = "抓取正文中...";
    renderEmptyState("正在抓取原始正文并提炼有效证据，请稍等。");

    try {
      const fullData = await fetchQueryPayload(query, from, to, "full");
      if (requestToken !== state.requestToken) {
        return;
      }

      applyQueryPayload(fullData);
    } catch (error) {
      if (requestToken !== state.requestToken) {
        return;
      }

      clearQueryState();
      summarize({});
      renderReasonStrip([]);
      renderIntelBoard(null, null);
      renderTimeline(null);
      els.stockBrief.classList.add("empty");
      els.stockBrief.textContent = "实时查询失败。你可以确认网络是否可用，或者稍后重试。";
      renderEmptyState(error.message || "查询失败");
      syncExportState();
    } finally {
      if (requestToken === state.requestToken) {
        els.runSearch.disabled = false;
        els.runSearch.textContent = "开始筛选";
      }
    }
  }

  async function exportCsv() {
    if (!state.visibleEvidenceRows.length || !state.profile) {
      window.alert("先完成一次实时查询，再导出 Excel。");
      return;
    }

    const excelFilename = `${state.profile.code}_${state.profile.name}_${els.dateFrom.value || "start"}_${els.dateTo.value || "end"}_evidence.xlsx`;
    try {
      await downloadExportFile("/api/export/xlsx", excelFilename);
    } catch (error) {
      window.alert(`${error.message || "Excel 导出失败。"}\n请重启网站服务后重试，不再自动回退 CSV。`);
    }
  }

  async function exportRawCsv() {
    if (!state.visibleEvidenceRows.length || !state.profile) {
      window.alert("先完成一次实时查询，再导出 CSV。");
      return;
    }

    const csvFilename = `${state.profile.code}_${state.profile.name}_${els.dateFrom.value || "start"}_${els.dateTo.value || "end"}_evidence.csv`;
    try {
      await downloadExportFile("/api/export/csv", csvFilename);
    } catch (error) {
      window.alert(error.message || "CSV 导出失败。");
    }
  }

  function resetSearch() {
    els.stockQuery.value = "";
    applyDefaultDates(true);
    state.requestToken += 1;
    clearQueryState();
    summarize({});
    renderReasonStrip([]);
    renderIntelBoard(null, null);
    renderTimeline(null);
    els.stockBrief.classList.add("empty");
    els.stockBrief.textContent = "输入股票后，这里会显示实时解析出的股票画像、敏感因子和当前查询所使用的数据源。";
    renderEmptyState("输入一只股票和日期范围后，这里会按确认证据流整理实时候选影响信息。");
    syncExportState();
  }

  function init() {
    applyDefaultDates();
    renderEmptyState("输入一只股票和日期范围后，这里会按有效文本证据流整理实时候选影响信息。");
    syncExportState();

    els.runSearch.addEventListener("click", runSearch);
    els.resetSearch.addEventListener("click", resetSearch);
    els.exportCsv.addEventListener("click", exportCsv);
    if (els.exportRawCsv) {
      els.exportRawCsv.addEventListener("click", exportRawCsv);
    }
    els.stockQuery.addEventListener("keydown", (event) => {
      if (event.key === "Enter") {
        event.preventDefault();
        runSearch();
      }
    });
  }

  init();
})();

