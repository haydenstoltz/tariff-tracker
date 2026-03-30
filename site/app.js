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

function signClass(value) {
  const v = String(value || "").toLowerCase();
  if (v === "positive") return "sign-positive";
  if (v === "negative") return "sign-negative";
  return "sign-mixed";
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
}

function selectEventAndCase(eventId, caseId = "") {
  const eventSelect = document.getElementById("eventSelect");
  const caseSelect = document.getElementById("caseSelect");

  selectedCaseId = caseId || "";
  eventSelect.value = eventId;
  populateCaseSelect(eventId);

  if (caseId) {
    caseSelect.value = caseId;
    renderCase(caseId);
  } else {
    highlightPortfolioRow("");
  }
}

function highlightPortfolioRow(caseId) {
  selectedCaseId = caseId || "";
  document.querySelectorAll("#portfolioTableBody tr").forEach(row => {
    row.classList.toggle("is-selected", row.dataset.caseId === selectedCaseId);
  });
}

function renderPortfolioTable() {
  const tbody = document.getElementById("portfolioTableBody");
  tbody.innerHTML = "";

  const liveCases = [...cases].sort((a, b) => {
    const eventA = getEventById(a.event_id);
    const eventB = getEventById(b.event_id);

    const titleA = eventA?.title || "";
    const titleB = eventB?.title || "";

    if (titleA < titleB) return -1;
    if (titleA > titleB) return 1;

    const orderA = Number(a.display_order ?? 9999);
    const orderB = Number(b.display_order ?? 9999);
    return orderA - orderB;
  });

  if (liveCases.length === 0) {
    const tr = document.createElement("tr");
    tr.innerHTML = `<td colspan="10">No live cases found.</td>`;
    tbody.appendChild(tr);
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
}

function populateCaseSelect(eventId) {
  const caseSelect = document.getElementById("caseSelect");
  const selectedEvent = getEventById(eventId);

  if (!selectedEvent) {
    renderNoEvents("Tariff event not found");
    highlightPortfolioRow("");
    return;
  }

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

  selectedCaseId = eventCases[0].case_id;
  renderCase(eventCases[0].case_id);
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

    selectedCaseId = caseId;
    highlightPortfolioRow(caseId);
  } catch (err) {
    console.error(err);
    const currentEventId = document.getElementById("eventSelect").value;
    renderEventOnly(getEventById(currentEventId));
    document.getElementById("caseTitle").textContent = "Failed to render incidence case";
    highlightPortfolioRow("");
  }
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
      populateCaseSelect(e.target.value);
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

    renderPortfolioTable();

    if (tariffs.length > 0) {
      eventSelect.value = tariffs[0].event_id;
      populateCaseSelect(tariffs[0].event_id);
    } else {
      renderNoEvents("No tariff events found");
    }
  } catch (err) {
    console.error(err);
    renderNoEvents("Failed to load site data");
  }
}

loadData();