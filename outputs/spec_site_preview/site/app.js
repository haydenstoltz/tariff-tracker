const eventMarkerPlugin = {
  id: "eventMarker",
  afterDraw(chart, args, pluginOptions) {
    if (!pluginOptions) return;

    const index = pluginOptions.index;
    if (index === undefined || index === null || index < 0) return;

    const { ctx, chartArea, scales } = chart;
    if (!chartArea || !scales.x) return;

    const x = scales.x.getPixelForValue(index);

    ctx.save();
    ctx.strokeStyle = "#111";
    ctx.lineWidth = 1.25;
    ctx.setLineDash([5, 5]);
    ctx.beginPath();
    ctx.moveTo(x, chartArea.top);
    ctx.lineTo(x, chartArea.bottom);
    ctx.stroke();
    ctx.setLineDash([]);

    ctx.fillStyle = "#111";
    ctx.font = "12px Arial";
    ctx.textAlign = "left";
    ctx.fillText(pluginOptions.label || "Tariff effective", x + 6, chartArea.top + 14);
    ctx.restore();
  }
};

Chart.register(eventMarkerPlugin);

let chart = null;
let tariffs = [];
let cases = [];
let summaries = {};
let selectedCaseId = "";
let selectedEventId = "";

function byId(id) {
  return document.getElementById(id);
}

function valueOf(id, fallback = "") {
  const node = byId(id);
  return node ? node.value : fallback;
}

function setIfOptionExists(id, value) {
  const node = byId(id);
  if (!node) return;
  const exists = [...node.options].some(opt => opt.value === value);
  if (exists) node.value = value;
}

function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function sourceLinkHtml(label, url) {
  const cleanLabel = escapeHtml(label || "Source");
  const cleanUrl = String(url || "").trim();
  if (!cleanUrl) return cleanLabel;
  return `<a class="table-source-link" href="${cleanUrl}" target="_blank" rel="noopener noreferrer">${cleanLabel}</a>`;
}

function fmtNumber(value, digits = 3) {
  if (value === null || value === undefined || value === "") return "—";
  const n = Number(value);
  if (Number.isNaN(n)) return "—";
  return n.toFixed(digits);
}

function fmtInteger(value) {
  if (value === null || value === undefined || value === "") return "—";
  return String(value);
}

function fmtText(value, fallback = "—") {
  if (value === null || value === undefined || value === "") return fallback;
  return String(value);
}

function prettyStatus(value) {
  if (!value) return "—";
  return String(value)
    .split("_")
    .filter(Boolean)
    .map(part => part.charAt(0).toUpperCase() + part.slice(1))
    .join(" ");
}

function confidenceClass(value) {
  const v = String(value || "").trim().toLowerCase();
  if (v === "high") return "conf-high";
  if (v === "medium") return "conf-medium";
  if (v === "low") return "conf-low";
  return "";
}

function confidenceRank(value) {
  const v = String(value || "").trim().toLowerCase();
  if (v === "high") return 3;
  if (v === "medium") return 2;
  if (v === "low") return 1;
  return 0;
}

function signClass(value) {
  const v = String(value || "").trim().toLowerCase();
  if (v === "positive") return "sign-positive";
  if (v === "negative") return "sign-negative";
  return "sign-mixed";
}

function statusGroup(value) {
  const v = String(value || "").trim().toLowerCase();
  if (v.startsWith("active")) return "current";
  if (v.startsWith("paused")) return "paused";
  if (v.startsWith("invalidated")) return "invalidated";
  if (
    v.includes("historical") ||
    v.startsWith("expired") ||
    v.startsWith("terminated") ||
    v.startsWith("superseded")
  ) {
    return "historical";
  }
  return "other";
}

function stageClass(value) {
  const v = String(value || "").trim().toLowerCase();
  if (v === "consumer") return "sign-positive";
  if (v === "upstream") return "conf-medium";
  if (v === "downstream" || v === "retail" || v === "multi_stage") return "sign-mixed";
  if (v === "import") return "conf-low";
  return "";
}

function destroyChart() {
  if (chart) {
    chart.destroy();
    chart = null;
  }
}

function clearDownloads() {
  const downloadJson = byId("downloadJson");
  const downloadCsv = byId("downloadCsv");
  if (downloadJson) {
    downloadJson.removeAttribute("href");
    downloadJson.removeAttribute("download");
  }
  if (downloadCsv) {
    downloadCsv.removeAttribute("href");
    downloadCsv.removeAttribute("download");
  }
}

function resetStatsAndDiagnostics() {
  const ids = [
    "m3", "m6", "m12", "direction",
    "preEventGapStd", "peakPostGap", "peakPostGapMonth",
    "placeboN3", "placeboP3", "placeboN6", "placeboP6"
  ];
  ids.forEach(id => {
    const node = byId(id);
    if (node) node.textContent = "—";
  });
}

function getEventById(eventId) {
  return tariffs.find(event => event.event_id === eventId) || null;
}

function getCasesForEvent(eventId) {
  return cases
    .filter(c => c.event_id === eventId)
    .sort((a, b) => {
      const aStage = Number(a.stage_order ?? 9999);
      const bStage = Number(b.stage_order ?? 9999);
      if (aStage !== bStage) return aStage - bStage;

      const aOrder = Number(a.display_order ?? 9999);
      const bOrder = Number(b.display_order ?? 9999);
      return aOrder - bOrder;
    });
}

function getCaseById(caseId) {
  return cases.find(c => c.case_id === caseId) || null;
}

function buildEventMeta(event) {
  const parts = [
    fmtText(event.authority, ""),
    fmtText(event.country_scope || event.country, ""),
    fmtText(event.product_scope, ""),
    event.effective_date ? `Effective ${event.effective_date}` : "",
    event.rate_summary ? `Rate ${event.rate_summary}` : "",
    event.status_bucket ? `Status ${prettyStatus(event.status_bucket)}` : ""
  ].filter(Boolean);

  return parts.join(" | ");
}

function buildEventSearchText(event) {
  return [
    event.title,
    event.authority,
    event.country_scope || event.country,
    event.product_scope,
    event.rate_summary,
    event.status_bucket,
    event.notes,
    event.legal_source_label,
    event.case_coverage_status,
    event.incidence_priority,
    event.candidate_stage,
    event.candidate_notes
  ]
    .map(v => String(v || "").toLowerCase())
    .join(" ");
}

function getUrlState() {
  const params = new URLSearchParams(window.location.search);
  return {
    eventId: params.get("event") || "",
    caseId: params.get("case") || "",
    status: params.get("reg_status") || "all",
    authority: params.get("reg_authority") || "all",
    coverage: params.get("reg_coverage") || "all",
    coverageStatus: params.get("reg_covstatus") || "all",
    priority: params.get("reg_priority") || "all",
    search: params.get("reg_q") || "",
    portfolioSort: params.get("portfolio_sort") || "m6_desc",
    registrySort: params.get("registry_sort") || "effective_desc",
    queuePriority: params.get("queue_priority") || "actionable",
    queueStage: params.get("queue_stage") || "all"
  };
}

function updateUrlState() {
  const params = new URLSearchParams();

  if (selectedEventId) params.set("event", selectedEventId);
  if (selectedCaseId) params.set("case", selectedCaseId);

  const statusValue = valueOf("registryStatusFilter", "all");
  const authorityValue = valueOf("registryAuthorityFilter", "all");
  const coverageValue = valueOf("registryCoverageFilter", "all");
  const coverageStatusValue = valueOf("registryCoverageStatusFilter", "all");
  const priorityValue = valueOf("registryPriorityFilter", "all");
  const searchValue = String(valueOf("registrySearchInput", "")).trim();
  const portfolioSortValue = valueOf("portfolioSort", "m6_desc");
  const registrySortValue = valueOf("registrySort", "effective_desc");
  const queuePriorityValue = valueOf("queuePriorityFilter", "actionable");
  const queueStageValue = valueOf("queueStageFilter", "all");

  if (statusValue !== "all") params.set("reg_status", statusValue);
  if (authorityValue !== "all") params.set("reg_authority", authorityValue);
  if (coverageValue !== "all") params.set("reg_coverage", coverageValue);
  if (coverageStatusValue !== "all") params.set("reg_covstatus", coverageStatusValue);
  if (priorityValue !== "all") params.set("reg_priority", priorityValue);
  if (searchValue) params.set("reg_q", searchValue);
  if (portfolioSortValue !== "m6_desc") params.set("portfolio_sort", portfolioSortValue);
  if (registrySortValue !== "effective_desc") params.set("registry_sort", registrySortValue);
  if (queuePriorityValue !== "actionable") params.set("queue_priority", queuePriorityValue);
  if (queueStageValue !== "all") params.set("queue_stage", queueStageValue);

  const query = params.toString();
  const nextUrl = query ? `${window.location.pathname}?${query}` : window.location.pathname;
  window.history.replaceState({}, "", nextUrl);
}

function renderEventHeader(event) {
  const title = byId("eventTitle");
  const meta = byId("eventMeta");
  const source = byId("eventSource");

  if (title) title.textContent = event?.title || "Tariff event not found";
  if (meta) meta.textContent = event ? buildEventMeta(event) : "";

  if (source) {
    if (!event) {
      source.textContent = "";
    } else {
      source.innerHTML = `Legal source: ${sourceLinkHtml(
        fmtText(event.legal_source_label, "Source"),
        event.legal_source_url
      )}`;
    }
  }
}

function renderEventCaseTable(eventId) {
  const tbody = byId("eventCaseTableBody");
  if (!tbody) return;

  tbody.innerHTML = "";

  const event = getEventById(eventId);
  if (!event) {
    const tr = document.createElement("tr");
    tr.innerHTML = `<td colspan="9">No event selected.</td>`;
    tbody.appendChild(tr);
    return;
  }

  const eventCases = getCasesForEvent(eventId);

  if (eventCases.length === 0) {
    const tr = document.createElement("tr");
    tr.innerHTML = `<td colspan="9">No live cases are currently mapped to this event.</td>`;
    tbody.appendChild(tr);
    return;
  }

  eventCases.forEach(c => {
    const summary = summaries[c.case_id] || {};
    const tr = document.createElement("tr");
    tr.className = "is-clickable";
    tr.dataset.caseId = c.case_id;

    if (c.case_id === selectedCaseId) tr.classList.add("is-selected");

    const noteParts = [];
    if (c.rationale_short) noteParts.push(c.rationale_short);
    if (c.caveat) noteParts.push(`Caveat: ${c.caveat}`);
    const note = noteParts.join(" ");

    tr.innerHTML = `
      <td>${escapeHtml(fmtText(c.case_name, ""))}</td>
      <td><span class="badge ${stageClass(c.case_stage)}">${escapeHtml(prettyStatus(c.case_stage))}</span></td>
      <td>${escapeHtml(fmtText(c.source_type, ""))}</td>
      <td><span class="badge ${confidenceClass(c.confidence_tier)}">${escapeHtml(fmtText(c.confidence_tier, ""))}</span></td>
      <td class="num">${escapeHtml(fmtNumber(summary.m3))}</td>
      <td class="num">${escapeHtml(fmtNumber(summary.m6))}</td>
      <td class="num">${escapeHtml(fmtNumber(summary.m12))}</td>
      <td><span class="badge ${signClass(summary.sign)}">${escapeHtml(fmtText(summary.sign, ""))}</span></td>
      <td>${escapeHtml(note)}</td>
    `;

    tr.addEventListener("click", () => {
      const caseSelect = byId("caseSelect");
      if (caseSelect) caseSelect.value = c.case_id;
      renderCase(c.case_id);
      byId("caseTitle")?.scrollIntoView({ behavior: "smooth", block: "start" });
    });

    tbody.appendChild(tr);
  });
}

function renderNoEvents(message = "No tariff events found") {
  renderEventHeader({
    title: message,
    authority: "",
    country_scope: "",
    product_scope: "",
    effective_date: "",
    rate_summary: "",
    status_bucket: "",
    legal_source_label: "",
    legal_source_url: ""
  });

  const clearIds = ["caseTitle", "caseMeta", "caseCaveat", "robustnessNote", "methodNote"];
  clearIds.forEach(id => {
    const node = byId(id);
    if (node) node.textContent = "";
  });

  const eventCaseBody = byId("eventCaseTableBody");
  if (eventCaseBody) {
    eventCaseBody.innerHTML = `<tr><td colspan="9">No event selected.</td></tr>`;
  }

  resetStatsAndDiagnostics();
  clearDownloads();
  destroyChart();

  const caseSelect = byId("caseSelect");
  if (caseSelect) {
    caseSelect.innerHTML = "";
    caseSelect.disabled = true;
  }
}

function renderEventOnly(event) {
  if (!event) {
    renderNoEvents("Tariff event not found");
    return;
  }

  selectedCaseId = "";
  selectedEventId = event.event_id;

  renderEventHeader(event);
  renderEventCaseTable(event.event_id);

  byId("caseTitle").textContent = "No incidence cases yet";
  byId("caseMeta").textContent =
    `Status: ${prettyStatus(event.status_bucket)} | Coverage: ${prettyStatus(event.case_coverage_status)} | Priority: ${prettyStatus(event.incidence_priority)} | Stage plan: ${prettyStatus(event.candidate_stage)}`;

  byId("caseCaveat").textContent =
    event.candidate_notes || event.notes || "This event is in the legal registry but does not yet have a mapped pass-through case.";

  byId("robustnessNote").textContent =
    event.legal_source_label ? `Legal source label: ${event.legal_source_label}` : "";

  byId("methodNote").textContent =
    "This event is visible in the tariff registry, but no treatment-control incidence case is currently mapped to it.";

  resetStatsAndDiagnostics();
  clearDownloads();
  destroyChart();
  highlightPortfolioRow("");
  highlightRegistryRow(selectedEventId);
  highlightBuildQueueRow(selectedEventId);
  updateUrlState();
}

function highlightPortfolioRow(caseId) {
  selectedCaseId = caseId || "";

  document.querySelectorAll("#portfolioTableBody tr").forEach(row => {
    row.classList.toggle("is-selected", row.dataset.caseId === selectedCaseId);
  });

  document.querySelectorAll("#eventCaseTableBody tr").forEach(row => {
    row.classList.toggle("is-selected", row.dataset.caseId === selectedCaseId);
  });
}

function highlightRegistryRow(eventId) {
  selectedEventId = eventId || "";
  document.querySelectorAll("#registryTableBody tr").forEach(row => {
    row.classList.toggle("is-selected", row.dataset.eventId === selectedEventId);
  });
}

function highlightBuildQueueRow(eventId) {
  document.querySelectorAll("#buildQueueTableBody tr").forEach(row => {
    row.classList.toggle("is-selected", row.dataset.eventId === eventId);
  });
}

function selectEventAndCase(eventId, caseId = "") {
  const eventSelect = byId("eventSelect");
  selectedEventId = eventId || "";
  selectedCaseId = caseId || "";

  if (eventSelect) eventSelect.value = eventId;
  populateCaseSelect(eventId, caseId);
}

function sortedPortfolioCases() {
  const sortValue = valueOf("portfolioSort", "m6_desc");
  const rows = [...cases];

  rows.sort((a, b) => {
    const eventA = getEventById(a.event_id);
    const eventB = getEventById(b.event_id);
    const summaryA = summaries[a.case_id] || {};
    const summaryB = summaries[b.case_id] || {};

    if (sortValue === "event_asc") {
      return fmtText(eventA?.title, "").localeCompare(fmtText(eventB?.title, ""));
    }
    if (sortValue === "event_desc") {
      return fmtText(eventB?.title, "").localeCompare(fmtText(eventA?.title, ""));
    }
    if (sortValue === "authority_asc") {
      return fmtText(eventA?.authority, "").localeCompare(fmtText(eventB?.authority, ""));
    }
    if (sortValue === "m12_desc") {
      return Number(summaryB.m12 ?? -Infinity) - Number(summaryA.m12 ?? -Infinity);
    }
    if (sortValue === "confidence_desc") {
      const confDiff = confidenceRank(b.confidence_tier) - confidenceRank(a.confidence_tier);
      if (confDiff !== 0) return confDiff;
      return Number(summaryB.m6 ?? -Infinity) - Number(summaryA.m6 ?? -Infinity);
    }

    return Number(summaryB.m6 ?? -Infinity) - Number(summaryA.m6 ?? -Infinity);
  });

  return rows;
}

function renderPortfolioTable() {
  const tbody = byId("portfolioTableBody");
  if (!tbody) return;
  tbody.innerHTML = "";

  const liveCases = sortedPortfolioCases();

  if (liveCases.length === 0) {
    const tr = document.createElement("tr");
    tr.innerHTML = `<td colspan="10">No live cases found.</td>`;
    tbody.appendChild(tr);
    updateUrlState();
    return;
  }

  liveCases.forEach(c => {
    const event = getEventById(c.event_id);
    const summary = summaries[c.case_id] || {};
    const tr = document.createElement("tr");
    tr.className = "is-clickable";
    tr.dataset.caseId = c.case_id;

    if (c.case_id === selectedCaseId) tr.classList.add("is-selected");

    tr.innerHTML = `
      <td>${escapeHtml(fmtText(event?.title, ""))}</td>
      <td>${escapeHtml(fmtText(c.case_name, ""))}<br><span class="muted">${escapeHtml(prettyStatus(c.case_stage))}</span></td>
      <td>${escapeHtml(fmtText(event?.authority, ""))}</td>
      <td>${escapeHtml(fmtText(c.source_type, ""))}</td>
      <td><span class="badge ${confidenceClass(c.confidence_tier)}">${escapeHtml(fmtText(c.confidence_tier, ""))}</span></td>
      <td class="num">${escapeHtml(fmtNumber(summary.m3))}</td>
      <td class="num">${escapeHtml(fmtNumber(summary.m6))}</td>
      <td class="num">${escapeHtml(fmtNumber(summary.m12))}</td>
      <td><span class="badge ${signClass(summary.sign)}">${escapeHtml(fmtText(summary.sign, ""))}</span></td>
      <td><span class="muted">${escapeHtml(prettyStatus(event?.status_bucket))}</span></td>
    `;

    tr.addEventListener("click", () => {
      selectEventAndCase(c.event_id, c.case_id);
      byId("eventSelect")?.scrollIntoView({ behavior: "smooth", block: "start" });
    });

    tbody.appendChild(tr);
  });

  updateUrlState();
}

function populateRegistryAuthorityFilter() {
  const select = byId("registryAuthorityFilter");
  if (!select) return;

  const currentValue = select.value || "all";
  const authorities = [...new Set(tariffs.map(t => fmtText(t.authority, "")).filter(Boolean))].sort();

  select.innerHTML = `<option value="all">All authorities</option>`;
  authorities.forEach(authority => {
    const option = document.createElement("option");
    option.value = authority;
    option.textContent = authority;
    select.appendChild(option);
  });

  setIfOptionExists("registryAuthorityFilter", currentValue);
}

function populateRegistryCoverageStatusFilter() {
  const select = byId("registryCoverageStatusFilter");
  if (!select) return;

  const currentValue = select.value || "all";
  const statuses = [...new Set(tariffs.map(t => fmtText(t.case_coverage_status, "")).filter(Boolean))].sort();

  select.innerHTML = `<option value="all">All coverage statuses</option>`;
  statuses.forEach(status => {
    const option = document.createElement("option");
    option.value = status;
    option.textContent = prettyStatus(status);
    select.appendChild(option);
  });

  setIfOptionExists("registryCoverageStatusFilter", currentValue);
}

function populateQueueStageFilter() {
  const select = byId("queueStageFilter");
  if (!select) return;

  const currentValue = select.value || "all";
  const stages = [...new Set(tariffs.map(t => fmtText(t.candidate_stage, "")).filter(Boolean))].sort();

  select.innerHTML = `<option value="all">All stages</option>`;
  stages.forEach(stage => {
    const option = document.createElement("option");
    option.value = stage;
    option.textContent = prettyStatus(stage);
    select.appendChild(option);
  });

  setIfOptionExists("queueStageFilter", currentValue);
}

function renderRegistrySummary() {
  byId("summaryTotalEvents").textContent = fmtInteger(tariffs.length);
  byId("summaryCurrentEvents").textContent = fmtInteger(tariffs.filter(event => statusGroup(event.status_bucket) === "current").length);
  byId("summaryHistoricalEvents").textContent = fmtInteger(tariffs.filter(event => statusGroup(event.status_bucket) === "historical").length);
  byId("summaryInvalidatedEvents").textContent = fmtInteger(tariffs.filter(event => statusGroup(event.status_bucket) === "invalidated").length);
  byId("summaryMappedEvents").textContent = fmtInteger(tariffs.filter(event => event.has_live_cases).length);
  byId("summaryUnmappedEvents").textContent = fmtInteger(tariffs.filter(event => !event.has_live_cases).length);
}

function filteredRegistryEvents() {
  const statusValue = valueOf("registryStatusFilter", "all");
  const authorityValue = valueOf("registryAuthorityFilter", "all");
  const coverageValue = valueOf("registryCoverageFilter", "all");
  const coverageStatusValue = valueOf("registryCoverageStatusFilter", "all");
  const priorityValue = valueOf("registryPriorityFilter", "all");
  const searchValue = String(valueOf("registrySearchInput", "")).trim().toLowerCase();
  const sortValue = valueOf("registrySort", "effective_desc");

  const rows = tariffs.filter(event => {
    if (statusValue !== "all" && statusGroup(event.status_bucket) !== statusValue) return false;
    if (authorityValue !== "all" && fmtText(event.authority, "") !== authorityValue) return false;
    if (coverageValue === "with_cases" && !event.has_live_cases) return false;
    if (coverageValue === "without_cases" && event.has_live_cases) return false;
    if (coverageStatusValue !== "all" && fmtText(event.case_coverage_status, "") !== coverageStatusValue) return false;
    if (priorityValue !== "all" && fmtText(event.incidence_priority, "") !== priorityValue) return false;
    if (searchValue && !buildEventSearchText(event).includes(searchValue)) return false;
    return true;
  });

  rows.sort((a, b) => {
    if (sortValue === "effective_asc") return fmtText(a.effective_date, "").localeCompare(fmtText(b.effective_date, ""));
    if (sortValue === "title_asc") return fmtText(a.title, "").localeCompare(fmtText(b.title, ""));
    if (sortValue === "authority_asc") return fmtText(a.authority, "").localeCompare(fmtText(b.authority, ""));
    if (sortValue === "cases_desc") return Number(b.live_case_count ?? 0) - Number(a.live_case_count ?? 0);
    if (sortValue === "status_asc") return prettyStatus(a.status_bucket).localeCompare(prettyStatus(b.status_bucket));
    return fmtText(b.effective_date, "").localeCompare(fmtText(a.effective_date, ""));
  });

  return rows;
}

function actionableQueueRows() {
  const priorityFilter = String(valueOf("queuePriorityFilter", "actionable")).trim().toLowerCase();
  const stageFilter = String(valueOf("queueStageFilter", "all")).trim().toLowerCase();

  const rows = tariffs.filter(event => {
    const hasLiveCases = Boolean(event.has_live_cases);
    const coverageStatus = String(event.case_coverage_status || "").trim().toLowerCase();
    const priority = String(event.incidence_priority || "").trim().toLowerCase();
    const stage = String(event.candidate_stage || "").trim().toLowerCase();

    const isAlreadyMapped =
      hasLiveCases ||
      coverageStatus === "mapped_live" ||
      coverageStatus === "mapped_archived";

    if (isAlreadyMapped) return false;
    if (priorityFilter === "actionable" && !["high", "medium"].includes(priority)) return false;
    if (priorityFilter !== "actionable" && priorityFilter !== "all" && priority !== priorityFilter) return false;
    if (stageFilter !== "all" && stage !== stageFilter) return false;

    return true;
  });

  const rank = value => {
    const v = String(value || "").trim().toLowerCase();
    if (v === "high") return 3;
    if (v === "medium") return 2;
    if (v === "low") return 1;
    return 0;
  };

  rows.sort((a, b) => {
    const priorityDiff = rank(b.incidence_priority) - rank(a.incidence_priority);
    if (priorityDiff !== 0) return priorityDiff;
    return fmtText(b.effective_date, "").localeCompare(fmtText(a.effective_date, ""));
  });

  return rows;
}

function renderRegistryTable() {
  const tbody = byId("registryTableBody");
  if (!tbody) return;
  tbody.innerHTML = "";

  const rows = filteredRegistryEvents();

  if (rows.length === 0) {
    const tr = document.createElement("tr");
    tr.innerHTML = `<td colspan="10">No tariff events match the current filters.</td>`;
    tbody.appendChild(tr);
    updateUrlState();
    return;
  }

  rows.forEach(event => {
    const tr = document.createElement("tr");
    tr.className = "is-clickable";
    tr.dataset.eventId = event.event_id;

    if (event.event_id === selectedEventId) tr.classList.add("is-selected");

    tr.innerHTML = `
      <td>${escapeHtml(fmtText(event.title, ""))}</td>
      <td>${escapeHtml(fmtText(event.authority, ""))}</td>
      <td>${escapeHtml(fmtText(event.country_scope || event.country, ""))} | ${escapeHtml(fmtText(event.product_scope, ""))}</td>
      <td>${escapeHtml(fmtText(event.effective_date, ""))}</td>
      <td><span class="muted">${escapeHtml(prettyStatus(event.status_bucket))}</span></td>
      <td>${escapeHtml(event.has_live_cases ? fmtInteger(event.live_case_count) : "0")}</td>
      <td><span class="muted">${escapeHtml(prettyStatus(event.case_coverage_status))}</span></td>
      <td><span class="badge ${confidenceClass(event.incidence_priority)}">${escapeHtml(prettyStatus(event.incidence_priority))}</span></td>
      <td><span class="muted">${escapeHtml(prettyStatus(event.candidate_stage))}</span></td>
      <td>${sourceLinkHtml(fmtText(event.legal_source_label, "Source"), event.legal_source_url)}</td>
    `;

    tr.addEventListener("click", () => {
      selectEventAndCase(event.event_id, "");
      byId("eventSelect")?.scrollIntoView({ behavior: "smooth", block: "start" });
    });

    tbody.appendChild(tr);
  });

  updateUrlState();
}

function renderBuildQueueTable() {
  const tbody = byId("buildQueueTableBody");
  if (!tbody) return;
  tbody.innerHTML = "";

  const rows = actionableQueueRows();

  if (rows.length === 0) {
    const tr = document.createElement("tr");
    tr.innerHTML = `<td colspan="8">No build-queue events match the current filters.</td>`;
    tbody.appendChild(tr);
    updateUrlState();
    return;
  }

  rows.forEach(event => {
    const tr = document.createElement("tr");
    tr.className = "is-clickable";
    tr.dataset.eventId = event.event_id;

    if (event.event_id === selectedEventId) tr.classList.add("is-selected");

    tr.innerHTML = `
      <td>${escapeHtml(fmtText(event.title, ""))}</td>
      <td>${escapeHtml(fmtText(event.authority, ""))}</td>
      <td>${escapeHtml(fmtText(event.effective_date, ""))}</td>
      <td><span class="badge ${stageClass(event.candidate_stage)}">${escapeHtml(prettyStatus(event.candidate_stage))}</span></td>
      <td><span class="badge ${confidenceClass(event.incidence_priority)}">${escapeHtml(prettyStatus(event.incidence_priority))}</span></td>
      <td><span class="muted">${escapeHtml(prettyStatus(event.case_coverage_status))}</span></td>
      <td>${escapeHtml(fmtText(event.candidate_notes, ""))}</td>
      <td>${sourceLinkHtml(fmtText(event.legal_source_label, "Source"), event.legal_source_url)}</td>
    `;

    tr.addEventListener("click", () => {
      selectEventAndCase(event.event_id, "");
      byId("eventSelect")?.scrollIntoView({ behavior: "smooth", block: "start" });
    });

    tbody.appendChild(tr);
  });

  updateUrlState();
}

function populateCaseSelect(eventId, preferredCaseId = "") {
  const caseSelect = byId("caseSelect");
  const selectedEvent = getEventById(eventId);

  if (!selectedEvent || !caseSelect) {
    renderNoEvents("Tariff event not found");
    highlightPortfolioRow("");
    highlightRegistryRow("");
    highlightBuildQueueRow("");
    return;
  }

  selectedEventId = eventId;
  renderEventCaseTable(eventId);
  highlightRegistryRow(eventId);
  highlightBuildQueueRow(eventId);

  const eventCases = getCasesForEvent(eventId);
  caseSelect.innerHTML = "";

  if (eventCases.length === 0) {
    const placeholder = document.createElement("option");
    placeholder.value = "";
    placeholder.textContent = "No mapped cases yet";
    caseSelect.appendChild(placeholder);
    caseSelect.disabled = true;
    renderEventOnly(selectedEvent);
    highlightPortfolioRow("");
    return;
  }

  caseSelect.disabled = false;

  eventCases.forEach(c => {
    const option = document.createElement("option");
    option.value = c.case_id;
    option.textContent = `${c.case_name} — ${prettyStatus(c.case_stage)}`;
    caseSelect.appendChild(option);
  });

  const targetCaseId =
    preferredCaseId && eventCases.some(c => c.case_id === preferredCaseId)
      ? preferredCaseId
      : eventCases[0].case_id;

  caseSelect.value = targetCaseId;
  selectedCaseId = targetCaseId;
  renderCase(targetCaseId);
}

async function renderCase(caseId) {
  try {
    const selectedCase = getCaseById(caseId);
    if (!selectedCase) {
      const currentEventId = valueOf("eventSelect", "");
      renderEventOnly(getEventById(currentEventId));
      highlightPortfolioRow("");
      return;
    }

    const selectedEvent = getEventById(selectedCase.event_id);
    const summary = summaries[caseId];

    if (!selectedEvent || !summary) {
      renderEventOnly(selectedEvent);
      highlightPortfolioRow("");
      return;
    }

    selectedCaseId = caseId;
    selectedEventId = selectedCase.event_id;

    renderEventHeader(selectedEvent);
    renderEventCaseTable(selectedEventId);

    const caseMetaParts = [
      `Stage: ${prettyStatus(selectedCase.case_stage)}`,
      fmtText(selectedCase.source_type, ""),
      `Treatment: ${fmtText(selectedCase.treatment_label)}`,
      `Control: ${fmtText(selectedCase.control_label)}`
    ];

    if (selectedCase.confidence_tier) {
      caseMetaParts.push(`Confidence: ${selectedCase.confidence_tier}`);
    }

    byId("caseTitle").textContent = selectedCase.case_name;
    byId("caseMeta").textContent = caseMetaParts.join(" | ");

    const caveatParts = [];
    if (selectedCase.rationale_short) caveatParts.push(`Rationale: ${selectedCase.rationale_short}`);
    if (selectedCase.caveat) caveatParts.push(`Caveat: ${selectedCase.caveat}`);
    if (selectedCase.stage_notes) caveatParts.push(`Stage note: ${selectedCase.stage_notes}`);
    byId("caseCaveat").textContent = caveatParts.join(" ");

    byId("robustnessNote").textContent = selectedCase.robustness_note || "";
    byId("methodNote").textContent = selectedCase.method_note || "";

    byId("m3").textContent = fmtNumber(summary.m3);
    byId("m6").textContent = fmtNumber(summary.m6);
    byId("m12").textContent = fmtNumber(summary.m12);
    byId("direction").textContent = fmtText(summary.sign);

    byId("preEventGapStd").textContent = fmtNumber(summary.pre_event_gap_std_pp);
    byId("peakPostGap").textContent = fmtNumber(summary.peak_post_gap_pp);
    byId("peakPostGapMonth").textContent = fmtText(summary.peak_post_gap_month);
    byId("placeboN3").textContent = fmtInteger(summary.placebo_n_3m);
    byId("placeboP3").textContent = fmtNumber(summary.placebo_p_abs_3m);
    byId("placeboN6").textContent = fmtInteger(summary.placebo_n_6m);
    byId("placeboP6").textContent = fmtNumber(summary.placebo_p_abs_6m);

    const downloadJson = byId("downloadJson");
    const downloadCsv = byId("downloadCsv");
    if (downloadJson) {
      downloadJson.href = selectedCase.chart_file;
      downloadJson.download = `${caseId}.json`;
    }
    if (downloadCsv) {
      downloadCsv.href = selectedCase.csv_file;
      downloadCsv.download = `${caseId}.csv`;
    }

    const chartRes = await fetch(selectedCase.chart_file);
    if (!chartRes.ok) throw new Error(`Failed to load chart data for ${caseId}`);

    const chartData = await chartRes.json();
    const eventMonth = (selectedEvent.effective_date || "").slice(0, 7);
    const eventIndex = chartData.labels.indexOf(eventMonth);
    const ctx = byId("incidenceChart");

    destroyChart();

    chart = new Chart(ctx, {
      type: "line",
      data: {
        labels: chartData.labels,
        datasets: [
          {
            label: selectedCase.treatment_label,
            data: chartData.treatment,
            yAxisID: "y",
            tension: 0.15
          },
          {
            label: selectedCase.control_label,
            data: chartData.control,
            yAxisID: "y",
            tension: 0.15
          },
          {
            label: "Relative Effect",
            data: chartData.relative_effect,
            yAxisID: "y1",
            borderDash: [6, 6],
            tension: 0.15
          }
        ]
      },
      options: {
        responsive: true,
        interaction: { mode: "index", intersect: false },
        plugins: {
          legend: { position: "top" },
          eventMarker: {
            index: eventIndex,
            label: eventMonth ? `Tariff effective: ${eventMonth}` : "Tariff effective"
          }
        },
        scales: {
          y: {
            type: "linear",
            position: "left",
            title: { display: true, text: "Rebased Price Index" }
          },
          y1: {
            type: "linear",
            position: "right",
            grid: { drawOnChartArea: false },
            title: { display: true, text: "Relative Effect (pp)" }
          },
          x: {
            title: { display: true, text: "Month" }
          }
        }
      }
    });

    highlightPortfolioRow(caseId);
    highlightRegistryRow(selectedEventId);
    highlightBuildQueueRow(selectedEventId);
    updateUrlState();
  } catch (err) {
    console.error(err);
    const currentEventId = valueOf("eventSelect", "");
    renderEventOnly(getEventById(currentEventId));
    byId("caseTitle").textContent = "Failed to render incidence case";
    highlightPortfolioRow("");
  }
}

function applyInitialUrlState() {
  const state = getUrlState();

  setIfOptionExists("registryStatusFilter", state.status);
  setIfOptionExists("registryAuthorityFilter", state.authority);
  setIfOptionExists("registryCoverageFilter", state.coverage);
  setIfOptionExists("registryCoverageStatusFilter", state.coverageStatus);
  setIfOptionExists("registryPriorityFilter", state.priority);
  setIfOptionExists("portfolioSort", state.portfolioSort);
  setIfOptionExists("registrySort", state.registrySort);
  setIfOptionExists("queuePriorityFilter", state.queuePriority);
  setIfOptionExists("queueStageFilter", state.queueStage);

  const searchInput = byId("registrySearchInput");
  if (searchInput) searchInput.value = state.search;

  return state;
}

function flashCopyButton(message) {
  const button = byId("copyViewLink");
  if (!button) return;
  const original = "Copy current view link";
  button.textContent = message;
  window.setTimeout(() => {
    button.textContent = original;
  }, 1200);
}

async function copyCurrentViewLink() {
  updateUrlState();
  const url = window.location.href;

  try {
    await navigator.clipboard.writeText(url);
    flashCopyButton("Link copied");
    return;
  } catch (_) {
    const textarea = document.createElement("textarea");
    textarea.value = url;
    textarea.setAttribute("readonly", "");
    textarea.style.position = "absolute";
    textarea.style.left = "-9999px";
    document.body.appendChild(textarea);
    textarea.select();
    try {
      document.execCommand("copy");
      flashCopyButton("Link copied");
    } catch {
      flashCopyButton("Copy failed");
    } finally {
      document.body.removeChild(textarea);
    }
  }
}

function bindCopyButton() {
  byId("copyViewLink")?.addEventListener("click", copyCurrentViewLink);
}

function bindSorts() {
  byId("portfolioSort")?.addEventListener("change", renderPortfolioTable);
  byId("registrySort")?.addEventListener("change", renderRegistryTable);
}

function bindRegistryFilters() {
  byId("registryStatusFilter")?.addEventListener("change", renderRegistryTable);
  byId("registryAuthorityFilter")?.addEventListener("change", renderRegistryTable);
  byId("registryCoverageFilter")?.addEventListener("change", renderRegistryTable);
  byId("registryCoverageStatusFilter")?.addEventListener("change", renderRegistryTable);
  byId("registryPriorityFilter")?.addEventListener("change", renderRegistryTable);
  byId("registrySearchInput")?.addEventListener("input", renderRegistryTable);
}

function bindQueueFilters() {
  byId("queuePriorityFilter")?.addEventListener("change", renderBuildQueueTable);
  byId("queueStageFilter")?.addEventListener("change", renderBuildQueueTable);
}

function csvEscape(value) {
  const str = String(value ?? "");
  if (str.includes('"') || str.includes(",") || str.includes("\n")) {
    return `"${str.replace(/"/g, '""')}"`;
  }
  return str;
}

function rowsToCsv(rows) {
  if (!rows.length) return "";
  const headers = Object.keys(rows[0]);
  const lines = [headers.join(",")];
  rows.forEach(row => {
    lines.push(headers.map(h => csvEscape(row[h])).join(","));
  });
  return lines.join("\n");
}

function triggerCsvDownload(filename, rows) {
  const csv = rowsToCsv(rows);
  const blob = new Blob([csv], { type: "text/csv;charset=utf-8;" });
  const url = URL.createObjectURL(blob);
  const link = document.createElement("a");
  link.href = url;
  link.download = filename;
  document.body.appendChild(link);
  link.click();
  document.body.removeChild(link);
  URL.revokeObjectURL(url);
}

function downloadPortfolioCsv() {
  const rows = sortedPortfolioCases().map(c => {
    const event = getEventById(c.event_id);
    const summary = summaries[c.case_id] || {};
    return {
      event_title: fmtText(event?.title, ""),
      case_name: fmtText(c.case_name, ""),
      case_stage: fmtText(c.case_stage, ""),
      authority: fmtText(event?.authority, ""),
      source_type: fmtText(c.source_type, ""),
      confidence_tier: fmtText(c.confidence_tier, ""),
      effect_3m_pp: fmtText(summary.m3, ""),
      effect_6m_pp: fmtText(summary.m6, ""),
      effect_12m_pp: fmtText(summary.m12, ""),
      sign: fmtText(summary.sign, ""),
      status_bucket: fmtText(event?.status_bucket, ""),
      event_id: fmtText(c.event_id, ""),
      case_id: fmtText(c.case_id, "")
    };
  });

  triggerCsvDownload("portfolio_live_cases.csv", rows);
}

function downloadRegistryCsv() {
  const rows = filteredRegistryEvents().map(event => ({
    event_id: fmtText(event.event_id, ""),
    event_title: fmtText(event.title, ""),
    authority: fmtText(event.authority, ""),
    country_scope: fmtText(event.country_scope || event.country, ""),
    product_scope: fmtText(event.product_scope, ""),
    effective_date: fmtText(event.effective_date, ""),
    status_bucket: fmtText(event.status_bucket, ""),
    live_case_count: fmtText(event.live_case_count, ""),
    case_coverage_status: fmtText(event.case_coverage_status, ""),
    incidence_priority: fmtText(event.incidence_priority, ""),
    candidate_stage: fmtText(event.candidate_stage, ""),
    candidate_notes: fmtText(event.candidate_notes, ""),
    currently_active: fmtText(event.currently_active, ""),
    historical_flag: fmtText(event.historical_flag, ""),
    rate_summary: fmtText(event.rate_summary, ""),
    legal_source_label: fmtText(event.legal_source_label, ""),
    legal_source_url: fmtText(event.legal_source_url, "")
  }));

  triggerCsvDownload("registry_filtered_events.csv", rows);
}

function downloadBuildQueueCsv() {
  const rows = actionableQueueRows().map(event => ({
    event_id: fmtText(event.event_id, ""),
    event_title: fmtText(event.title, ""),
    authority: fmtText(event.authority, ""),
    effective_date: fmtText(event.effective_date, ""),
    candidate_stage: fmtText(event.candidate_stage, ""),
    incidence_priority: fmtText(event.incidence_priority, ""),
    case_coverage_status: fmtText(event.case_coverage_status, ""),
    candidate_notes: fmtText(event.candidate_notes, ""),
    legal_source_label: fmtText(event.legal_source_label, ""),
    legal_source_url: fmtText(event.legal_source_url, "")
  }));

  triggerCsvDownload("build_queue.csv", rows);
}

function bindCsvButtons() {
  byId("downloadPortfolioCsv")?.addEventListener("click", downloadPortfolioCsv);
  byId("downloadRegistryCsv")?.addEventListener("click", downloadRegistryCsv);
  byId("downloadBuildQueueCsv")?.addEventListener("click", downloadBuildQueueCsv);
}

async function loadData() {
  try {
    const [tariffsRes, casesRes, summaryRes] = await Promise.all([
      fetch("./data/tariffs.json"),
      fetch("./data/cases.json"),
      fetch("./data/summary.json")
    ]);

    if (!tariffsRes.ok || !casesRes.ok || !summaryRes.ok) {
      throw new Error("Failed to load site data files.");
    }

    tariffs = await tariffsRes.json();
    cases = await casesRes.json();
    summaries = await summaryRes.json();

    const eventSelect = byId("eventSelect");
    const caseSelect = byId("caseSelect");

    if (eventSelect) {
      eventSelect.innerHTML = "";
      tariffs.forEach(event => {
        const option = document.createElement("option");
        option.value = event.event_id;
        option.textContent = event.has_live_cases
          ? event.title
          : `${event.title} — no cases yet`;
        eventSelect.appendChild(option);
      });

      eventSelect.addEventListener("change", e => {
        selectEventAndCase(e.target.value, "");
      });
    }

    if (caseSelect) {
      caseSelect.addEventListener("change", e => {
        if (!e.target.value) {
          const currentEvent = getEventById(valueOf("eventSelect", ""));
          renderEventOnly(currentEvent);
          highlightPortfolioRow("");
          return;
        }
        renderCase(e.target.value);
      });
    }

    renderRegistrySummary();
    populateRegistryAuthorityFilter();
    populateRegistryCoverageStatusFilter();
    populateQueueStageFilter();

    const initialState = applyInitialUrlState();

    bindRegistryFilters();
    bindQueueFilters();
    bindSorts();
    bindCopyButton();
    bindCsvButtons();

    renderPortfolioTable();
    renderRegistryTable();
    renderBuildQueueTable();

    const eventCaseBody = byId("eventCaseTableBody");
    if (eventCaseBody) {
      eventCaseBody.innerHTML = `<tr><td colspan="9">Select an event to compare attached cases.</td></tr>`;
    }

    if (tariffs.length > 0) {
      const initialEventId = tariffs.some(t => t.event_id === initialState.eventId)
        ? initialState.eventId
        : tariffs[0].event_id;

      selectedEventId = initialEventId;
      if (eventSelect) eventSelect.value = initialEventId;
      populateCaseSelect(initialEventId, initialState.caseId);
    } else {
      renderNoEvents("No tariff events found");
    }
  } catch (err) {
    console.error(err);
    renderNoEvents("Failed to load site data");
  }
}

loadData();