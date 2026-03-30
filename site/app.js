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

function fmtNumber(value, digits = 3) {
  if (value === null || value === undefined || value === "") return "—";
  return Number(value).toFixed(digits);
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
  const v = String(value || "").toLowerCase();
  if (v === "high") return "conf-high";
  if (v === "medium") return "conf-medium";
  if (v === "low") return "conf-low";
  return "";
}

function confidenceRank(value) {
  const v = String(value || "").toLowerCase();
  if (v === "high") return 3;
  if (v === "medium") return 2;
  if (v === "low") return 1;
  return 0;
}

function signClass(value) {
  const v = String(value || "").toLowerCase();
  if (v === "positive") return "sign-positive";
  if (v === "negative") return "sign-negative";
  return "sign-mixed";
}

function statusGroup(value) {
  const v = String(value || "").toLowerCase();
  if (v.startsWith("active")) return "current";
  if (v.startsWith("paused")) return "paused";
  if (v.startsWith("invalidated")) return "invalidated";
  if (v.includes("historical") || v.startsWith("expired") || v.startsWith("terminated") || v.startsWith("superseded")) {
    return "historical";
  }
  return "other";
}

function destroyChart() {
  if (chart) {
    chart.destroy();
    chart = null;
  }
}

function clearDownloads() {
  const downloadJson = document.getElementById("downloadJson");
  const downloadCsv = document.getElementById("downloadCsv");

  downloadJson.removeAttribute("href");
  downloadJson.removeAttribute("download");
  downloadCsv.removeAttribute("href");
  downloadCsv.removeAttribute("download");
}

function resetStatsAndDiagnostics() {
  document.getElementById("m3").textContent = "—";
  document.getElementById("m6").textContent = "—";
  document.getElementById("m12").textContent = "—";
  document.getElementById("direction").textContent = "—";
  document.getElementById("preEventGapStd").textContent = "—";
  document.getElementById("peakPostGap").textContent = "—";
  document.getElementById("peakPostGapMonth").textContent = "—";
  document.getElementById("placeboN3").textContent = "—";
  document.getElementById("placeboP3").textContent = "—";
  document.getElementById("placeboN6").textContent = "—";
  document.getElementById("placeboP6").textContent = "—";
}

function getEventById(eventId) {
  return tariffs.find(event => event.event_id === eventId) || null;
}

function getCasesForEvent(eventId) {
  return cases
    .filter(c => c.event_id === eventId)
    .sort((a, b) => {
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
    event.legal_source_label
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
    search: params.get("reg_q") || "",
    portfolioSort: params.get("portfolio_sort") || "m6_desc",
    registrySort: params.get("registry_sort") || "effective_desc"
  };
}

function updateUrlState() {
  const params = new URLSearchParams();

  if (selectedEventId) params.set("event", selectedEventId);
  if (selectedCaseId) params.set("case", selectedCaseId);

  const statusValue = document.getElementById("registryStatusFilter")?.value || "all";
  const authorityValue = document.getElementById("registryAuthorityFilter")?.value || "all";
  const coverageValue = document.getElementById("registryCoverageFilter")?.value || "all";
  const searchValue = document.getElementById("registrySearchInput")?.value.trim() || "";
  const portfolioSortValue = document.getElementById("portfolioSort")?.value || "m6_desc";
  const registrySortValue = document.getElementById("registrySort")?.value || "effective_desc";

  if (statusValue !== "all") params.set("reg_status", statusValue);
  if (authorityValue !== "all") params.set("reg_authority", authorityValue);
  if (coverageValue !== "all") params.set("reg_coverage", coverageValue);
  if (searchValue) params.set("reg_q", searchValue);
  if (portfolioSortValue !== "m6_desc") params.set("portfolio_sort", portfolioSortValue);
  if (registrySortValue !== "effective_desc") params.set("registry_sort", registrySortValue);

  const query = params.toString();
  const nextUrl = query ? `${window.location.pathname}?${query}` : window.location.pathname;
  window.history.replaceState({}, "", nextUrl);
}

function renderEventHeader(event) {
  document.getElementById("eventTitle").textContent = event?.title || "Tariff event not found";
  document.getElementById("eventMeta").textContent = event ? buildEventMeta(event) : "";
}

function renderNoEvents(message = "No tariff events found") {
  renderEventHeader({
    title: message,
    authority: "",
    country_scope: "",
    product_scope: "",
    effective_date: "",
    rate_summary: "",
    status_bucket: ""
  });
  document.getElementById("caseTitle").textContent = "";
  document.getElementById("caseMeta").textContent = "";
  document.getElementById("caseCaveat").textContent = "";
  document.getElementById("robustnessNote").textContent = "";
  document.getElementById("methodNote").textContent = "";
  resetStatsAndDiagnostics();
  clearDownloads();
  destroyChart();

  const caseSelect = document.getElementById("caseSelect");
  caseSelect.innerHTML = "";
  caseSelect.disabled = true;
}

function renderEventOnly(event) {
  if (!event) {
    renderNoEvents("Tariff event not found");
    return;
  }

  selectedCaseId = "";
  selectedEventId = event.event_id;
  renderEventHeader(event);

  document.getElementById("caseTitle").textContent = "No incidence cases yet";
  document.getElementById("caseMeta").textContent =
    `Status: ${prettyStatus(event.status_bucket)} | Currently active: ${fmtText(event.currently_active)} | Historical: ${fmtText(event.historical_flag)}`;

  document.getElementById("caseCaveat").textContent =
    event.notes || "This event is in the legal registry but does not yet have a mapped pass-through case.";

  document.getElementById("robustnessNote").textContent =
    event.legal_source_label ? `Legal source: ${event.legal_source_label}` : "";

  document.getElementById("methodNote").textContent =
    "This event is visible in the tariff registry, but no treatment-control incidence case is currently mapped to it.";

  resetStatsAndDiagnostics();
  clearDownloads();
  destroyChart();
  highlightPortfolioRow("");
  highlightRegistryRow(selectedEventId);
  updateUrlState();
}

function highlightPortfolioRow(caseId) {
  selectedCaseId = caseId || "";
  document.querySelectorAll("#portfolioTableBody tr").forEach(row => {
    row.classList.toggle("is-selected", row.dataset.caseId === selectedCaseId);
  });
}

function highlightRegistryRow(eventId) {
  selectedEventId = eventId || "";
  document.querySelectorAll("#registryTableBody tr").forEach(row => {
    row.classList.toggle("is-selected", row.dataset.eventId === selectedEventId);
  });
}

function selectEventAndCase(eventId, caseId = "") {
  const eventSelect = document.getElementById("eventSelect");

  selectedEventId = eventId || "";
  selectedCaseId = caseId || "";

  eventSelect.value = eventId;
  populateCaseSelect(eventId, caseId);
}

function sortedPortfolioCases() {
  const sortValue = document.getElementById("portfolioSort").value;
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
      return (Number(summaryB.m12 ?? -Infinity) - Number(summaryA.m12 ?? -Infinity));
    }
    if (sortValue === "confidence_desc") {
      const confDiff = confidenceRank(b.confidence_tier) - confidenceRank(a.confidence_tier);
      if (confDiff !== 0) return confDiff;
      return (Number(summaryB.m6 ?? -Infinity) - Number(summaryA.m6 ?? -Infinity));
    }

    return (Number(summaryB.m6 ?? -Infinity) - Number(summaryA.m6 ?? -Infinity));
  });

  return rows;
}

function renderPortfolioTable() {
  const tbody = document.getElementById("portfolioTableBody");
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

    if (c.case_id === selectedCaseId) {
      tr.classList.add("is-selected");
    }

    tr.innerHTML = `
      <td>${fmtText(event?.title)}</td>
      <td>${fmtText(c.case_name)}</td>
      <td>${fmtText(event?.authority)}</td>
      <td>${fmtText(c.source_type)}</td>
      <td><span class="badge ${confidenceClass(c.confidence_tier)}">${fmtText(c.confidence_tier)}</span></td>
      <td class="num">${fmtNumber(summary.m3)}</td>
      <td class="num">${fmtNumber(summary.m6)}</td>
      <td class="num">${fmtNumber(summary.m12)}</td>
      <td><span class="badge ${signClass(summary.sign)}">${fmtText(summary.sign)}</span></td>
      <td><span class="muted">${prettyStatus(event?.status_bucket)}</span></td>
    `;

    tr.addEventListener("click", () => {
      selectEventAndCase(c.event_id, c.case_id);
      document.getElementById("eventSelect").scrollIntoView({ behavior: "smooth", block: "start" });
    });

    tbody.appendChild(tr);
  });

  updateUrlState();
}

function populateRegistryAuthorityFilter() {
  const select = document.getElementById("registryAuthorityFilter");
  const currentValue = select.value || "all";
  const authorities = [...new Set(tariffs.map(t => fmtText(t.authority, "")).filter(Boolean))].sort();

  select.innerHTML = `<option value="all">All authorities</option>`;
  authorities.forEach(authority => {
    const option = document.createElement("option");
    option.value = authority;
    option.textContent = authority;
    select.appendChild(option);
  });

  if ([...select.options].some(opt => opt.value === currentValue)) {
    select.value = currentValue;
  }
}

function renderRegistrySummary() {
  const totalEvents = tariffs.length;
  const currentEvents = tariffs.filter(event => statusGroup(event.status_bucket) === "current").length;
  const historicalEvents = tariffs.filter(event => statusGroup(event.status_bucket) === "historical").length;
  const invalidatedEvents = tariffs.filter(event => statusGroup(event.status_bucket) === "invalidated").length;
  const mappedEvents = tariffs.filter(event => event.has_live_cases).length;
  const unmappedEvents = tariffs.filter(event => !event.has_live_cases).length;

  document.getElementById("summaryTotalEvents").textContent = fmtInteger(totalEvents);
  document.getElementById("summaryCurrentEvents").textContent = fmtInteger(currentEvents);
  document.getElementById("summaryHistoricalEvents").textContent = fmtInteger(historicalEvents);
  document.getElementById("summaryInvalidatedEvents").textContent = fmtInteger(invalidatedEvents);
  document.getElementById("summaryMappedEvents").textContent = fmtInteger(mappedEvents);
  document.getElementById("summaryUnmappedEvents").textContent = fmtInteger(unmappedEvents);
}

function filteredRegistryEvents() {
  const statusValue = document.getElementById("registryStatusFilter").value;
  const authorityValue = document.getElementById("registryAuthorityFilter").value;
  const coverageValue = document.getElementById("registryCoverageFilter").value;
  const searchValue = document.getElementById("registrySearchInput").value.trim().toLowerCase();
  const sortValue = document.getElementById("registrySort").value;

  const rows = tariffs.filter(event => {
    if (statusValue !== "all" && statusGroup(event.status_bucket) !== statusValue) {
      return false;
    }

    if (authorityValue !== "all" && fmtText(event.authority, "") !== authorityValue) {
      return false;
    }

    if (coverageValue === "with_cases" && !event.has_live_cases) {
      return false;
    }

    if (coverageValue === "without_cases" && event.has_live_cases) {
      return false;
    }

    if (searchValue && !buildEventSearchText(event).includes(searchValue)) {
      return false;
    }

    return true;
  });

  rows.sort((a, b) => {
    if (sortValue === "effective_asc") {
      return fmtText(a.effective_date, "").localeCompare(fmtText(b.effective_date, ""));
    }
    if (sortValue === "title_asc") {
      return fmtText(a.title, "").localeCompare(fmtText(b.title, ""));
    }
    if (sortValue === "authority_asc") {
      return fmtText(a.authority, "").localeCompare(fmtText(b.authority, ""));
    }
    if (sortValue === "cases_desc") {
      return Number(b.live_case_count ?? 0) - Number(a.live_case_count ?? 0);
    }
    if (sortValue === "status_asc") {
      return prettyStatus(a.status_bucket).localeCompare(prettyStatus(b.status_bucket));
    }

    return fmtText(b.effective_date, "").localeCompare(fmtText(a.effective_date, ""));
  });

  return rows;
}

function renderRegistryTable() {
  const tbody = document.getElementById("registryTableBody");
  tbody.innerHTML = "";

  const rows = filteredRegistryEvents();

  if (rows.length === 0) {
    const tr = document.createElement("tr");
    tr.innerHTML = `<td colspan="6">No tariff events match the current filters.</td>`;
    tbody.appendChild(tr);
    updateUrlState();
    return;
  }

  rows.forEach(event => {
    const tr = document.createElement("tr");
    tr.className = "is-clickable";
    tr.dataset.eventId = event.event_id;

    if (event.event_id === selectedEventId) {
      tr.classList.add("is-selected");
    }

    tr.innerHTML = `
      <td>${fmtText(event.title)}</td>
      <td>${fmtText(event.authority)}</td>
      <td>${fmtText(event.country_scope || event.country)} | ${fmtText(event.product_scope)}</td>
      <td>${fmtText(event.effective_date)}</td>
      <td><span class="muted">${prettyStatus(event.status_bucket)}</span></td>
      <td>${event.has_live_cases ? fmtInteger(event.live_case_count) : "0"}</td>
    `;

    tr.addEventListener("click", () => {
      selectEventAndCase(event.event_id, "");
      document.getElementById("eventSelect").scrollIntoView({ behavior: "smooth", block: "start" });
    });

    tbody.appendChild(tr);
  });

  updateUrlState();
}

function populateCaseSelect(eventId, preferredCaseId = "") {
  const caseSelect = document.getElementById("caseSelect");
  const selectedEvent = getEventById(eventId);

  if (!selectedEvent) {
    renderNoEvents("Tariff event not found");
    highlightPortfolioRow("");
    highlightRegistryRow("");
    return;
  }

  selectedEventId = eventId;
  highlightRegistryRow(eventId);

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
    option.textContent = c.case_name;
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
      const currentEventId = document.getElementById("eventSelect").value;
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

    const caseMetaParts = [
      fmtText(selectedCase.source_type, ""),
      `Treatment: ${fmtText(selectedCase.treatment_label)}`,
      `Control: ${fmtText(selectedCase.control_label)}`
    ];

    if (selectedCase.confidence_tier) {
      caseMetaParts.push(`Confidence: ${selectedCase.confidence_tier}`);
    }

    document.getElementById("caseTitle").textContent = selectedCase.case_name;
    document.getElementById("caseMeta").textContent = caseMetaParts.join(" | ");

    const caveatParts = [];
    if (selectedCase.rationale_short) {
      caveatParts.push(`Rationale: ${selectedCase.rationale_short}`);
    }
    if (selectedCase.caveat) {
      caveatParts.push(`Caveat: ${selectedCase.caveat}`);
    }
    document.getElementById("caseCaveat").textContent = caveatParts.join(" ");

    document.getElementById("robustnessNote").textContent = selectedCase.robustness_note || "";
    document.getElementById("methodNote").textContent = selectedCase.method_note || "";

    document.getElementById("m3").textContent = fmtNumber(summary.m3);
    document.getElementById("m6").textContent = fmtNumber(summary.m6);
    document.getElementById("m12").textContent = fmtNumber(summary.m12);
    document.getElementById("direction").textContent = fmtText(summary.sign);

    document.getElementById("preEventGapStd").textContent = fmtNumber(summary.pre_event_gap_std_pp);
    document.getElementById("peakPostGap").textContent = fmtNumber(summary.peak_post_gap_pp);
    document.getElementById("peakPostGapMonth").textContent = fmtText(summary.peak_post_gap_month);
    document.getElementById("placeboN3").textContent = fmtInteger(summary.placebo_n_3m);
    document.getElementById("placeboP3").textContent = fmtNumber(summary.placebo_p_abs_3m);
    document.getElementById("placeboN6").textContent = fmtInteger(summary.placebo_n_6m);
    document.getElementById("placeboP6").textContent = fmtNumber(summary.placebo_p_abs_6m);

    const downloadJson = document.getElementById("downloadJson");
    const downloadCsv = document.getElementById("downloadCsv");

    downloadJson.href = selectedCase.chart_file;
    downloadJson.download = `${caseId}.json`;

    downloadCsv.href = selectedCase.csv_file;
    downloadCsv.download = `${caseId}.csv`;

    const chartRes = await fetch(selectedCase.chart_file);
    if (!chartRes.ok) {
      throw new Error(`Failed to load chart data for ${caseId}`);
    }

    const chartData = await chartRes.json();

    const eventMonth = (selectedEvent.effective_date || "").slice(0, 7);
    const eventIndex = chartData.labels.indexOf(eventMonth);

    const ctx = document.getElementById("incidenceChart");

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
        interaction: {
          mode: "index",
          intersect: false
        },
        plugins: {
          legend: {
            position: "top"
          },
          eventMarker: {
            index: eventIndex,
            label: eventMonth ? `Tariff effective: ${eventMonth}` : "Tariff effective"
          }
        },
        scales: {
          y: {
            type: "linear",
            position: "left",
            title: {
              display: true,
              text: "Rebased Price Index"
            }
          },
          y1: {
            type: "linear",
            position: "right",
            grid: {
              drawOnChartArea: false
            },
            title: {
              display: true,
              text: "Relative Effect (pp)"
            }
          },
          x: {
            title: {
              display: true,
              text: "Month"
            }
          }
        }
      }
    });

    highlightPortfolioRow(caseId);
    highlightRegistryRow(selectedEventId);
    updateUrlState();
  } catch (err) {
    console.error(err);
    const currentEventId = document.getElementById("eventSelect").value;
    renderEventOnly(getEventById(currentEventId));
    document.getElementById("caseTitle").textContent = "Failed to render incidence case";
    highlightPortfolioRow("");
  }
}

function applyInitialUrlState() {
  const state = getUrlState();

  const statusSelect = document.getElementById("registryStatusFilter");
  const authoritySelect = document.getElementById("registryAuthorityFilter");
  const coverageSelect = document.getElementById("registryCoverageFilter");
  const searchInput = document.getElementById("registrySearchInput");
  const portfolioSort = document.getElementById("portfolioSort");
  const registrySort = document.getElementById("registrySort");

  if ([...statusSelect.options].some(opt => opt.value === state.status)) {
    statusSelect.value = state.status;
  }

  if ([...authoritySelect.options].some(opt => opt.value === state.authority)) {
    authoritySelect.value = state.authority;
  }

  if ([...coverageSelect.options].some(opt => opt.value === state.coverage)) {
    coverageSelect.value = state.coverage;
  }

  if ([...portfolioSort.options].some(opt => opt.value === state.portfolioSort)) {
    portfolioSort.value = state.portfolioSort;
  }

  if ([...registrySort.options].some(opt => opt.value === state.registrySort)) {
    registrySort.value = state.registrySort;
  }

  searchInput.value = state.search;

  return state;
}

function flashCopyButton(message) {
  const button = document.getElementById("copyViewLink");
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
  document.getElementById("copyViewLink").addEventListener("click", copyCurrentViewLink);
}

function bindSorts() {
  document.getElementById("portfolioSort").addEventListener("change", renderPortfolioTable);
  document.getElementById("registrySort").addEventListener("change", renderRegistryTable);
}

function bindRegistryFilters() {
  document.getElementById("registryStatusFilter").addEventListener("change", renderRegistryTable);
  document.getElementById("registryAuthorityFilter").addEventListener("change", renderRegistryTable);
  document.getElementById("registryCoverageFilter").addEventListener("change", renderRegistryTable);
  document.getElementById("registrySearchInput").addEventListener("input", renderRegistryTable);
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
    currently_active: fmtText(event.currently_active, ""),
    historical_flag: fmtText(event.historical_flag, ""),
    rate_summary: fmtText(event.rate_summary, ""),
    legal_source_label: fmtText(event.legal_source_label, ""),
    legal_source_url: fmtText(event.legal_source_url, "")
  }));

  triggerCsvDownload("registry_filtered_events.csv", rows);
}

function bindCsvButtons() {
  document.getElementById("downloadPortfolioCsv").addEventListener("click", downloadPortfolioCsv);
  document.getElementById("downloadRegistryCsv").addEventListener("click", downloadRegistryCsv);
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

    const eventSelect = document.getElementById("eventSelect");
    const caseSelect = document.getElementById("caseSelect");

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

    caseSelect.addEventListener("change", e => {
      if (!e.target.value) {
        const currentEvent = getEventById(eventSelect.value);
        renderEventOnly(currentEvent);
        highlightPortfolioRow("");
        return;
      }
      renderCase(e.target.value);
    });

    renderRegistrySummary();
    populateRegistryAuthorityFilter();
    const initialState = applyInitialUrlState();
    bindRegistryFilters();
    bindSorts();
    bindCopyButton();
    bindCsvButtons();
    renderPortfolioTable();
    renderRegistryTable();

    if (tariffs.length > 0) {
      const initialEventId = tariffs.some(t => t.event_id === initialState.eventId)
        ? initialState.eventId
        : tariffs[0].event_id;

      selectedEventId = initialEventId;
      eventSelect.value = initialEventId;
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