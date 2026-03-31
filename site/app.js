const chartPalette = {
  treatment: "#1f5fbf",
  control: "#7a8699",
  relative: "#b42318",
  postWindowFill: "rgba(31, 95, 191, 0.06)",
  marker: "#1f2937",
  markerFill: "#ffffff"
};

const postEventWindowPlugin = {
  id: "postEventWindow",
  beforeDatasetsDraw(chart, args, pluginOptions) {
    if (!pluginOptions) return;

    const index = pluginOptions.index;
    if (index === undefined || index === null || index < 0) return;

    const { ctx, chartArea, scales } = chart;
    if (!chartArea || !scales.x) return;

    const startX = scales.x.getPixelForValue(index);

    ctx.save();
    ctx.fillStyle = pluginOptions.fillStyle || chartPalette.postWindowFill;
    ctx.fillRect(
      startX,
      chartArea.top,
      chartArea.right - startX,
      chartArea.bottom - chartArea.top
    );
    ctx.restore();
  }
};

const eventMarkerPlugin = {
  id: "eventMarker",
  afterDraw(chart, args, pluginOptions) {
    if (!pluginOptions) return;

    const index = pluginOptions.index;
    if (index === undefined || index === null || index < 0) return;

    const { ctx, chartArea, scales } = chart;
    if (!chartArea || !scales.x) return;

    const x = scales.x.getPixelForValue(index);
    const label = pluginOptions.label || "Tariff effective";

    ctx.save();

    ctx.strokeStyle = pluginOptions.strokeStyle || chartPalette.marker;
    ctx.lineWidth = 1.5;
    ctx.setLineDash([6, 6]);
    ctx.beginPath();
    ctx.moveTo(x, chartArea.top);
    ctx.lineTo(x, chartArea.bottom);
    ctx.stroke();
    ctx.setLineDash([]);

    ctx.font = "12px Inter, Arial, sans-serif";
    const textWidth = ctx.measureText(label).width;
    const padX = 8;
    const padY = 5;
    const boxWidth = textWidth + padX * 2;
    const boxHeight = 22;
    const boxX = Math.min(x + 8, chartArea.right - boxWidth);
    const boxY = chartArea.top + 8;

    ctx.fillStyle = pluginOptions.boxFill || "rgba(255, 255, 255, 0.96)";
    ctx.strokeStyle = pluginOptions.boxStroke || "rgba(31, 41, 55, 0.15)";
    ctx.lineWidth = 1;
    ctx.beginPath();
    ctx.roundRect(boxX, boxY, boxWidth, boxHeight, 8);
    ctx.fill();
    ctx.stroke();

    ctx.fillStyle = pluginOptions.textColor || chartPalette.marker;
    ctx.textAlign = "left";
    ctx.textBaseline = "middle";
    ctx.fillText(label, boxX + padX, boxY + boxHeight / 2);

    ctx.restore();
  }
};

Chart.register(postEventWindowPlugin, eventMarkerPlugin);

let chart = null;
let tariffs = [];
let cases = [];
let summaries = {};
let selectedCaseId = "";
let selectedEventId = "";
let officialFeed = [];
let selectedMapCountry = "";
let worldAtlasPromise = null;

function byId(id) {
  return document.getElementById(id);
}

async function fetchOptionalJson(url, fallback = []) {
  try {
    const res = await fetch(url, { cache: "no-store" });
    if (!res.ok) return fallback;
    return await res.json();
  } catch {
    return fallback;
  }
}

function setText(id, value) {
  const node = byId(id);
  if (node) node.textContent = value;
}

function hasFeedWorkspace() {
  return Boolean(byId("eventSelect") && byId("caseSelect"));
}

function openFeedPage(eventId = "", caseId = "") {
  const params = new URLSearchParams();
  if (eventId) params.set("event", eventId);
  if (caseId) params.set("case", caseId);
  const query = params.toString();
  window.location.href = query ? `./index.html?${query}` : "./index.html";
}


let activeShellSection = "feedSection";

function setSectionTabActive(sectionId) {
  activeShellSection = sectionId || "feedSection";

  document.querySelectorAll(".section-tab").forEach(button => {
    button.classList.toggle(
      "is-active",
      button.dataset.sectionTarget === activeShellSection
    );
  });
}

function scrollToShellSection(sectionId) {
  const node = byId(sectionId);
  if (!node) return;
  setSectionTabActive(sectionId);
  node.scrollIntoView({ behavior: "smooth", block: "start" });
}

function bindSectionNav() {
  const buttons = [...document.querySelectorAll(".section-tab")];
  if (!buttons.length) return;

  buttons.forEach(button => {
    button.addEventListener("click", () => {
      scrollToShellSection(button.dataset.sectionTarget || "feedSection");
    });
  });

  const observedIds = [
    "feedSection",
    "mapSection",
    "eventRegistrySection",
    "caseLibrarySection",
    "buildQueueSection"
  ]
    .map(id => byId(id))
    .filter(Boolean);

  if (!observedIds.length || !("IntersectionObserver" in window)) return;

  const observer = new IntersectionObserver(
    entries => {
      const visible = entries
        .filter(entry => entry.isIntersecting)
        .sort((a, b) => b.intersectionRatio - a.intersectionRatio);

      if (visible.length) {
        setSectionTabActive(visible[0].target.id);
      }
    },
    {
      root: null,
      threshold: [0.2, 0.45, 0.7],
      rootMargin: "-12% 0px -55% 0px"
    }
  );

  observedIds.forEach(node => observer.observe(node));
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


function finiteNumbers(values) {
  return (values || [])
    .map(v => Number(v))
    .filter(v => Number.isFinite(v));
}

function paddedExtent(values, padRatio = 0.08) {
  const nums = finiteNumbers(values);
  if (!nums.length) return { min: 0, max: 1 };

  let min = Math.min(...nums);
  let max = Math.max(...nums);

  if (min === max) {
    const bump = Math.abs(min) || 1;
    return {
      min: min - bump * 0.5,
      max: max + bump * 0.5
    };
  }

  const span = max - min;
  const pad = span * padRatio;

  return {
    min: min - pad,
    max: max + pad
  };
}


function yesish(value) {
  const v = String(value || "").trim().toLowerCase();
  return v === "yes" || v === "true" || v === "1";
}

function metricClass(value) {
  if (value === null || value === undefined || value === "") return "metric-neutral";
  const n = Number(value);
  if (Number.isNaN(n)) return "metric-neutral";
  if (n > 0) return "metric-positive";
  if (n < 0) return "metric-negative";
  return "metric-neutral";
}

function metricPillHtml(value, digits = 3) {
  return `<span class="metric-pill ${metricClass(value)}">${escapeHtml(fmtNumber(value, digits))}</span>`;
}

function truncateText(value, maxLength = 140) {
  const text = fmtText(value, "");
  if (!text) return "";
  if (text.length <= maxLength) return text;
  return `${text.slice(0, maxLength - 1).trimEnd()}…`;
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

function priorityRank(value) {
  const v = String(value || "").trim().toLowerCase();
  if (v === "high") return 3;
  if (v === "medium") return 2;
  if (v === "low") return 1;
  return 0;
}

function currentnessRank(event) {
  const group = statusGroup(event?.status_bucket);
  if (group === "current") return 3;
  if (group === "paused") return 2;
  if (group === "other") return 1;
  if (group === "historical") return 0;
  if (group === "invalidated") return -1;
  return 0;
}

function eventLeadCase(eventId) {
  const eventCases = getCasesForEvent(eventId);
  if (!eventCases.length) return null;

  return [...eventCases].sort((a, b) => {
    const primaryDiff = Number(yesish(b.primary_case_flag)) - Number(yesish(a.primary_case_flag));
    if (primaryDiff !== 0) return primaryDiff;

    const confidenceDiff = confidenceRank(b.confidence_tier) - confidenceRank(a.confidence_tier);
    if (confidenceDiff !== 0) return confidenceDiff;

    const aM6 = Math.abs(Number(summaries[a.case_id]?.m6 ?? 0));
    const bM6 = Math.abs(Number(summaries[b.case_id]?.m6 ?? 0));
    if (bM6 !== aM6) return bM6 - aM6;

    const aOrder = Number(a.display_order ?? 9999);
    const bOrder = Number(b.display_order ?? 9999);
    return aOrder - bOrder;
  })[0];
}

function rankedFeedEvents() {
  return [...tariffs].sort(compareRankedEvents);
}

function populateFeedAuthorityFilter() {
  const select = byId("feedAuthorityFilter");
  if (!select) return;

  const currentValue = select.value || "all";
  const authorities = [
    ...new Set(
      currentFeedRows()
        .map(item => fmtText(item.authority, ""))
        .filter(Boolean)
    )
  ].sort();

  select.innerHTML = `<option value="all">All authorities</option>`;
  authorities.forEach(authority => {
    const option = document.createElement("option");
    option.value = authority;
    option.textContent = authority;
    select.appendChild(option);
  });

  setIfOptionExists("feedAuthorityFilter", currentValue);
}

function filteredFeedEvents() {
  const searchValue = String(valueOf("feedSearchInput", "")).trim().toLowerCase();
  const statusValue = valueOf("feedStatusFilter", "all");
  const authorityValue = valueOf("feedAuthorityFilter", "all");
  const priorityValue = valueOf("feedPriorityFilter", "all");
  const evidenceValue = valueOf("feedEvidenceFilter", "all");
  const sortValue = valueOf("feedSort", "ranked");

  const rows = currentFeedRows().filter(item => {
    if (searchValue && !buildFeedSearchText(item).includes(searchValue)) return false;
    if (statusValue !== "all" && statusGroup(item.status_bucket) !== statusValue) return false;
    if (authorityValue !== "all" && fmtText(item.authority, "") !== authorityValue) return false;
    if (priorityValue !== "all" && fmtText(item.incidence_priority, "") !== priorityValue) return false;
    if (evidenceValue === "with_cases" && !feedHasTrackedCase(item)) return false;
    if (evidenceValue === "without_cases" && feedHasTrackedCase(item)) return false;
    return true;
  });

  rows.sort((a, b) => {
    if (sortValue === "effective_desc" || sortValue === "announced_desc") {
      return fmtText(b.latest_item_date || b.display_date, "").localeCompare(fmtText(a.latest_item_date || a.display_date, ""));
    }
    if (sortValue === "cases_desc") {
      const caseDiff = Number(b.matched_live_case_count || 0) - Number(a.matched_live_case_count || 0);
      if (caseDiff !== 0) return caseDiff;
    }
    return compareFeedRows(a, b);
  });

  return rows;
}

function bindFeedFilters() {
  [
    "feedSearchInput",
    "feedStatusFilter",
    "feedAuthorityFilter",
    "feedPriorityFilter",
    "feedEvidenceFilter",
    "feedSort"
  ].forEach(id => {
    byId(id)?.addEventListener(id === "feedSearchInput" ? "input" : "change", renderIntelFeed);
  });
}

function compareRankedEvents(a, b) {
  const currentDiff = currentnessRank(b) - currentnessRank(a);
  if (currentDiff !== 0) return currentDiff;

  const priorityDiff = priorityRank(b.incidence_priority) - priorityRank(a.incidence_priority);
  if (priorityDiff !== 0) return priorityDiff;

  const evidenceGapDiff = Number(!b.has_live_cases) - Number(!a.has_live_cases);
  if (evidenceGapDiff !== 0) return evidenceGapDiff;

  const bDate = fmtText(b.effective_date || b.announced_date, "");
  const aDate = fmtText(a.effective_date || a.announced_date, "");
  const dateDiff = bDate.localeCompare(aDate);
  if (dateDiff !== 0) return dateDiff;

  return fmtText(a.title, "").localeCompare(fmtText(b.title, ""));
}

function normalizeMapCountryName(value) {
  const raw = String(value || "").trim();
  if (!raw) return "";

  const aliases = {
    "United States": "United States of America",
    "US": "United States of America",
    "USA": "United States of America",
    "UK": "United Kingdom",
    "UAE": "United Arab Emirates",
    "Czech Republic": "Czechia",
    "Ivory Coast": "Côte d'Ivoire",
    "Democratic Republic of Congo": "Democratic Republic of the Congo",
    "Republic of Congo": "Republic of the Congo",
    "South Korea": "South Korea",
    "Russia": "Russia",
    "China": "China"
  };

  return aliases[raw] || raw;
}

function isMapGlobalScope(value) {
  const v = String(value || "").trim();
  if (!v) return true;

  const lower = v.toLowerCase();
  if (
    lower === "global" ||
    lower === "world" ||
    lower === "worldwide" ||
    lower === "multi-country" ||
    lower === "multiple countries" ||
    lower.includes("global") ||
    lower.includes("multi-country") ||
    lower.includes("multiple countries") ||
    lower.includes("european union")
  ) {
    return true;
  }

  if (v.includes(",") || v.includes(";") || v.includes("/") || lower.includes(" and ")) {
    return true;
  }

  return false;
}

function filteredMapEvents() {
  const statusValue = valueOf("mapStatusFilter", "all");
  const authorityValue = valueOf("mapAuthorityFilter", "all");
  const priorityValue = valueOf("mapPriorityFilter", "all");

  return tariffs.filter(event => {
    if (statusValue !== "all" && statusGroup(event.status_bucket) !== statusValue) return false;
    if (authorityValue !== "all" && fmtText(event.authority, "") !== authorityValue) return false;
    if (priorityValue !== "all" && fmtText(event.incidence_priority, "") !== priorityValue) return false;
    return true;
  }).sort(compareRankedEvents);
}

function groupMapEventsByCountry(events) {
  const countryBuckets = new Map();
  const globalEvents = [];

  events.forEach(event => {
    const rawCountry = fmtText(event.country_scope || event.country, "");

    if (isMapGlobalScope(rawCountry)) {
      globalEvents.push(event);
      return;
    }

    const countryName = normalizeMapCountryName(rawCountry);
    if (!countryName) {
      globalEvents.push(event);
      return;
    }

    if (!countryBuckets.has(countryName)) {
      countryBuckets.set(countryName, []);
    }
    countryBuckets.get(countryName).push(event);
  });

  return { countryBuckets, globalEvents };
}

function populateMapAuthorityFilter() {
  const select = byId("mapAuthorityFilter");
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

  setIfOptionExists("mapAuthorityFilter", currentValue);
}

function getWorldAtlas() {
  if (!worldAtlasPromise) {
    worldAtlasPromise = d3.json("https://cdn.jsdelivr.net/npm/world-atlas@2/countries-110m.json");
  }
  return worldAtlasPromise;
}

function mapCountryFill(events) {
  if (!events || !events.length) return "#eef3f7";

  const topPriority = Math.max(...events.map(event => priorityRank(event.incidence_priority)));
  const topCurrentness = Math.max(...events.map(event => currentnessRank(event)));
  const count = events.length;

  if (topCurrentness >= 3 && topPriority >= 3) return "#1f5fbf";
  if (topPriority >= 3) return "#356fca";
  if (topPriority >= 2 || count >= 2) return "#6f97da";
  return "#bdd0ef";
}

function renderMapInspector(countryName, countryEvents, globalEvents) {
  setText("mapSelectionTitle", countryName || "No country selected");
  setText(
    "mapSelectionMeta",
    countryEvents.length
      ? `${countryEvents.length} visible event${countryEvents.length === 1 ? "" : "s"} for this country`
      : "No country-specific events match the current filters"
  );

  const selectionList = byId("mapSelectionList");
  const globalList = byId("mapGlobalList");

  if (selectionList) {
    selectionList.innerHTML = "";
    if (!countryEvents.length) {
      selectionList.innerHTML = `<div class="map-event-empty">No matching country events.</div>`;
    } else {
      countryEvents.forEach(event => {
        const leadCase = eventLeadCase(event.event_id);
        const card = document.createElement("button");
        card.type = "button";
        card.className = "map-event-card";
        card.innerHTML = `
          <div class="map-event-card-top">
            <span class="badge status-${statusGroup(event.status_bucket)}">${escapeHtml(prettyStatus(event.status_bucket))}</span>
            <span class="badge ${confidenceClass(event.incidence_priority)}">${escapeHtml(prettyStatus(event.incidence_priority))}</span>
          </div>
          <div class="map-event-card-title">${escapeHtml(fmtText(event.title, ""))}</div>
          <div class="map-event-card-meta">
            ${escapeHtml(fmtText(event.authority, ""))} | ${escapeHtml(fmtText(event.effective_date || event.announced_date, "—"))}
          </div>
          <div class="map-event-card-note">
            ${leadCase ? escapeHtml(`Lead case: ${leadCase.case_name}`) : "No live case yet"}
          </div>
        `;
        card.addEventListener("click", () => {
          openFeedPage(event.event_id, leadCase?.case_id || "");
        });
        selectionList.appendChild(card);
      });
    }
  }

  if (globalList) {
    globalList.innerHTML = "";
    if (!globalEvents.length) {
      globalList.innerHTML = `<div class="map-event-empty">No global or multi-country events in the current filter set.</div>`;
    } else {
      globalEvents.slice(0, 10).forEach(event => {
        const leadCase = eventLeadCase(event.event_id);
        const card = document.createElement("button");
        card.type = "button";
        card.className = "map-event-card global-card";
        card.innerHTML = `
          <div class="map-event-card-top">
            <span class="badge status-${statusGroup(event.status_bucket)}">${escapeHtml(prettyStatus(event.status_bucket))}</span>
            <span class="badge ${confidenceClass(event.incidence_priority)}">${escapeHtml(prettyStatus(event.incidence_priority))}</span>
          </div>
          <div class="map-event-card-title">${escapeHtml(fmtText(event.title, ""))}</div>
          <div class="map-event-card-meta">
            ${escapeHtml(fmtText(event.authority, ""))} | ${escapeHtml(fmtText(event.country_scope || event.country, ""))}
          </div>
          <div class="map-event-card-note">
            ${leadCase ? escapeHtml(`Lead case: ${leadCase.case_name}`) : "No live case yet"}
          </div>
        `;
        card.addEventListener("click", () => {
          openFeedPage(event.event_id, leadCase?.case_id || "");
        });
        globalList.appendChild(card);
      });
    }
  }
}

async function renderMapPage() {
  const svgNode = byId("tariffMapSvg");
  if (!svgNode || typeof d3 === "undefined" || typeof topojson === "undefined") return;

  populateMapAuthorityFilter();

  const filtered = filteredMapEvents();
  const { countryBuckets, globalEvents } = groupMapEventsByCountry(filtered);

  setText("mapVisibleEvents", fmtInteger(filtered.length));
  setText("mapVisibleCountries", fmtInteger(countryBuckets.size));
  setText(
    "mapVisibleLiveCases",
    fmtInteger(filtered.reduce((sum, event) => sum + Number(event.live_case_count || 0), 0))
  );
  setText("mapGlobalEvents", fmtInteger(globalEvents.length));

  const countryNames = [...countryBuckets.keys()].sort();
  if (!countryNames.includes(selectedMapCountry)) {
    selectedMapCountry = countryNames[0] || "";
  }

  const selectedCountryEvents = selectedMapCountry
    ? (countryBuckets.get(selectedMapCountry) || []).sort(compareRankedEvents)
    : [];

  renderMapInspector(selectedMapCountry || "No mapped country", selectedCountryEvents, globalEvents);

  const atlas = await getWorldAtlas();
  const countries = topojson.feature(atlas, atlas.objects.countries).features;

  const width = Math.max(svgNode.clientWidth || 860, 860);
  const height = 520;

  const svg = d3.select(svgNode);
  svg.selectAll("*").remove();
  svg.attr("viewBox", `0 0 ${width} ${height}`);

  const projection = d3.geoNaturalEarth1()
    .fitExtent([[12, 12], [width - 12, height - 12]], {
      type: "FeatureCollection",
      features: countries
    });

  const path = d3.geoPath(projection);

  svg.append("rect")
    .attr("x", 0)
    .attr("y", 0)
    .attr("width", width)
    .attr("height", height)
    .attr("fill", "#f8fbff");

  svg.append("g")
    .selectAll("path")
    .data(countries)
    .join("path")
    .attr("d", path)
    .attr("class", d => {
      const countryName = normalizeMapCountryName(d.properties.name);
      return countryName === selectedMapCountry ? "map-country is-selected" : "map-country";
    })
    .attr("fill", d => {
      const countryName = normalizeMapCountryName(d.properties.name);
      return mapCountryFill(countryBuckets.get(countryName));
    })
    .append("title")
    .text(d => {
      const countryName = normalizeMapCountryName(d.properties.name);
      const events = countryBuckets.get(countryName) || [];
      return events.length
        ? `${countryName}: ${events.length} visible event${events.length === 1 ? "" : "s"}`
        : countryName;
    });

  svg.selectAll(".map-country")
    .on("click", function(event, d) {
      const countryName = normalizeMapCountryName(d.properties.name);
      if (!countryBuckets.has(countryName)) return;
      selectedMapCountry = countryName;
      renderMapPage();
    });
}

function bindMapFilters() {
  byId("mapStatusFilter")?.addEventListener("change", () => {
    selectedMapCountry = "";
    renderMapPage();
  });
  byId("mapAuthorityFilter")?.addEventListener("change", () => {
    selectedMapCountry = "";
    renderMapPage();
  });
  byId("mapPriorityFilter")?.addEventListener("change", () => {
    selectedMapCountry = "";
    renderMapPage();
  });
}

function feedStatusBadgeClass(event) {
  return `status-${statusGroup(event?.status_bucket)}`;
}

function renderIntelFeed() {
  const grid = byId("intelFeedGrid");
  if (!grid) return;

  const rows = filteredFeedEvents().slice(0, 18);

  if (!rows.length) {
    grid.innerHTML = `<div class="intel-feed-empty">No official-source tariff items match the current feed filters.</div>`;
    return;
  }

  grid.innerHTML = "";

  rows.forEach(item => {
    const matchedEvent = feedHasTrackedEvent(item) ? getEventById(item.matched_event_id) : null;
    const matchedCase = item.matched_case_id ? getCaseById(item.matched_case_id) : null;
    const summary = matchedCase ? (summaries[matchedCase.case_id] || {}) : null;

    const card = document.createElement("article");
    card.className = "intel-feed-card";
    card.dataset.eventId = matchedEvent?.event_id || "";

    if (matchedEvent && matchedEvent.event_id === selectedEventId) {
      card.classList.add("is-selected");
    }

    const intelSourceBadge = `<span class="badge source-badge">${escapeHtml(fmtText(item.source_family, "Official source"))}</span>`;
    const evidenceLine = feedHasTrackedEvent(item)
      ? `${fmtInteger(item.matched_live_case_count || 0)} live case${Number(item.matched_live_case_count || 0) === 1 ? "" : "s"} attached`
      : "Official-source item not yet mapped to a tracker event";

    const evidenceHtml = feedHasTrackedEvent(item)
      ? `
        <div class="intel-feed-evidence">
          <div class="intel-feed-evidence-head">
            <span class="intel-feed-evidence-label">Tracker link</span>
            <span class="badge ${confidenceClass(item.incidence_priority)}">${escapeHtml(prettyStatus(item.incidence_priority))}</span>
          </div>
          <div class="intel-feed-evidence-title">${escapeHtml(fmtText(item.matched_event_title, ""))}</div>
          <div class="intel-feed-evidence-meta">
            ${matchedCase ? `${escapeHtml(fmtText(item.matched_case_name, ""))} | 6m ${fmtNumber(summary?.m6)}` : "No mapped live case yet"}
          </div>
        </div>
      `
      : `
        <div class="intel-feed-evidence intel-feed-evidence-empty">
          <div class="intel-feed-evidence-head">
            <span class="intel-feed-evidence-label">Official-source intel</span>
            ${intelSourceBadge}
          </div>
          <div class="intel-feed-evidence-title">Standalone feed item</div>
          <div class="intel-feed-evidence-meta">Opens the primary official source directly.</div>
        </div>
      `;

    card.innerHTML = `
      <div class="intel-feed-card-top">
        <div class="intel-feed-card-badges">
          <span class="badge ${feedStatusBadgeClass(item)}">${escapeHtml(prettyStatus(item.status_bucket))}</span>
          <span class="badge ${confidenceClass(item.incidence_priority)}">${escapeHtml(prettyStatus(item.incidence_priority))}</span>
          ${intelSourceBadge}
        </div>
        <div class="intel-feed-card-date">
          ${escapeHtml(fmtText(item.display_date || item.latest_item_date, "—"))}
        </div>
      </div>

      <h3>${escapeHtml(fmtText(item.normalized_title, ""))}</h3>

      <p class="intel-feed-scope">
        ${escapeHtml(fmtText(item.authority, ""))} | ${escapeHtml(fmtText(item.country_scope, ""))} | ${escapeHtml(fmtText(item.product_scope, ""))}
      </p>

      <p class="intel-feed-summary">
        ${escapeHtml(fmtText(item.notes, "No additional summary yet."))}
      </p>

      <div class="intel-feed-meta">
        <span>${escapeHtml(evidenceLine)}</span>
        <span>${sourceLinkHtml(fmtText(item.primary_source_label, "Source"), item.primary_source_url)}</span>
      </div>

      ${evidenceHtml}
    `;

    card.addEventListener("click", () => {
      if (feedHasTrackedEvent(item)) {
        if (item.matched_case_id) {
          selectEventAndCase(item.matched_event_id, item.matched_case_id);
        } else {
          selectEventAndCase(item.matched_event_id, "");
        }
        byId("eventSelect")?.scrollIntoView({ behavior: "smooth", block: "start" });
        renderIntelFeed();
        return;
      }

      const url = String(item.primary_source_url || "").trim();
      if (url) {
        window.open(url, "_blank", "noopener,noreferrer");
      }
    });

    grid.appendChild(card);
  });
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

function buildFeedSearchText(item) {
  return [
    item.normalized_title,
    item.authority,
    item.country_scope,
    item.product_scope,
    item.notes,
    item.primary_source_label,
    item.source_labels,
    item.matched_keywords,
    item.matched_event_title,
    item.matched_case_name,
    item.source_family,
    item.event_type
  ]
    .map(v => String(v || "").toLowerCase())
    .join(" ");
}

function feedHasTrackedCase(item) {
  return Number(item.matched_live_case_count || 0) > 0 || Boolean(String(item.matched_case_id || "").trim());
}

function feedHasTrackedEvent(item) {
  return Boolean(String(item.matched_event_id || "").trim());
}

function trackerEventsAsFeedRows() {
  return rankedFeedEvents().map(event => {
    const leadCase = eventLeadCase(event.event_id);
    return {
      feed_id: `tracker_${event.event_id}`,
      normalized_title: event.title,
      authority: event.authority,
      country_scope: event.country_scope || event.country,
      product_scope: event.product_scope,
      status_bucket: event.status_bucket,
      incidence_priority: event.incidence_priority,
      event_type: "tracker_event",
      display_date: event.effective_date || event.announced_date || "",
      latest_item_date: event.effective_date || event.announced_date || "",
      primary_source_label: event.legal_source_label,
      primary_source_url: event.legal_source_url,
      source_family: "Tracker",
      source_count: "1",
      source_labels: event.legal_source_label || "",
      matched_keywords: "",
      raw_hit_count: "1",
      notes: event.candidate_notes || event.notes || event.rate_summary || "",
      matched_event_id: event.event_id,
      matched_event_title: event.title,
      matched_case_id: leadCase?.case_id || "",
      matched_case_name: leadCase?.case_name || "",
      matched_live_case_count: String(event.live_case_count || 0),
      matched_score: "tracker",
      match_basis: "tracker"
    };
  });
}

function feedStatusRank(value) {
  return {
    current: 3,
    paused: 2,
    other: 1,
    historical: 0,
    invalidated: -1
  }[String(value || "").trim().toLowerCase()] ?? 0;
}

function compareFeedRows(a, b) {
  const statusDiff = feedStatusRank(b.status_bucket) - feedStatusRank(a.status_bucket);
  if (statusDiff !== 0) return statusDiff;

  const priorityDiff = priorityRank(b.incidence_priority) - priorityRank(a.incidence_priority);
  if (priorityDiff !== 0) return priorityDiff;

  const matchedDiff = Number(feedHasTrackedEvent(b)) - Number(feedHasTrackedEvent(a));
  if (matchedDiff !== 0) return matchedDiff;

  const caseDiff = Number(b.matched_live_case_count || 0) - Number(a.matched_live_case_count || 0);
  if (caseDiff !== 0) return caseDiff;

  const dateDiff = fmtText(b.latest_item_date || b.display_date, "").localeCompare(fmtText(a.latest_item_date || a.display_date, ""));
  if (dateDiff !== 0) return dateDiff;

  return fmtText(a.normalized_title, "").localeCompare(fmtText(b.normalized_title, ""));
}

function currentFeedRows() {
  return officialFeed.length ? [...officialFeed] : trackerEventsAsFeedRows();
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
  const grid = byId("eventCaseGrid");
  if (!grid) return;

  grid.innerHTML = "";

  const event = getEventById(eventId);
  if (!event) {
    grid.innerHTML = `<div class="event-case-empty">No event selected.</div>`;
    return;
  }

  const eventCases = getCasesForEvent(eventId);

  if (eventCases.length === 0) {
    grid.innerHTML = `<div class="event-case-empty">No live cases are currently mapped to this event.</div>`;
    return;
  }

  eventCases.forEach(c => {
    const summary = summaries[c.case_id] || {};
    const card = document.createElement("article");
    card.className = "event-case-card";
    card.dataset.caseId = c.case_id;
    if (c.case_id === selectedCaseId) card.classList.add("is-selected");

    const noteParts = [];
    if (c.rationale_short) noteParts.push(c.rationale_short);
    if (c.caveat) noteParts.push(`Caveat: ${c.caveat}`);
    const fullNote = noteParts.join(" ");
    const shortNote = truncateText(fullNote, 180);
    const primaryBadge = yesish(c.primary_case_flag)
      ? `<span class="badge primary-badge">Primary case</span>`
      : "";

    card.innerHTML = `
      <div class="event-case-top">
        <div class="event-case-heading">
          <div class="table-title-row">
            <span class="table-title">${escapeHtml(fmtText(c.case_name, ""))}</span>
            ${primaryBadge}
          </div>
          <div class="table-subrow">
            ${escapeHtml(fmtText(c.treatment_label, ""))} vs ${escapeHtml(fmtText(c.control_label, ""))}
          </div>
        </div>

        <div class="event-case-badges">
          <span class="badge ${stageClass(c.case_stage)}">${escapeHtml(prettyStatus(c.case_stage))}</span>
          <span class="badge ${confidenceClass(c.confidence_tier)}">${escapeHtml(fmtText(c.confidence_tier, ""))}</span>
          <span class="badge">${escapeHtml(fmtText(c.source_type, ""))}</span>
        </div>
      </div>

      <div class="event-case-metrics">
        <div class="event-case-metric">
          <span>3m</span>
          ${metricPillHtml(summary.m3)}
        </div>
        <div class="event-case-metric">
          <span>6m</span>
          ${metricPillHtml(summary.m6)}
        </div>
        <div class="event-case-metric">
          <span>12m</span>
          ${metricPillHtml(summary.m12)}
        </div>
        <div class="event-case-metric">
          <span>Sign</span>
          <span class="badge ${signClass(summary.sign)}">${escapeHtml(fmtText(summary.sign, ""))}</span>
        </div>
      </div>

      <div class="event-case-note" title="${escapeHtml(fullNote)}">
        ${escapeHtml(shortNote || "—")}
      </div>

      <div class="event-case-footer">
        <span class="event-case-open">Open case</span>
      </div>
    `;

    card.addEventListener("click", () => {
      const caseSelect = byId("caseSelect");
      if (caseSelect) caseSelect.value = c.case_id;
      renderCase(c.case_id);
      byId("caseTitle")?.scrollIntoView({ behavior: "smooth", block: "start" });
    });

    grid.appendChild(card);
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

  const eventCaseGrid = byId("eventCaseGrid");
  if (eventCaseGrid) {
    eventCaseGrid.innerHTML = `<div class="event-case-empty">No event selected.</div>`;
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
  renderIntelFeed();

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

  document.querySelectorAll("#eventCaseGrid .event-case-card").forEach(card => {
    card.classList.toggle("is-selected", card.dataset.caseId === selectedCaseId);
  });

  document.querySelectorAll("#portfolioStageGrid .portfolio-stage-slot.is-filled").forEach(node => {
    node.classList.toggle("is-selected", node.dataset.caseId === selectedCaseId);
  });

  document.querySelectorAll("#portfolioStageGrid .portfolio-stage-card").forEach(card => {
    card.classList.toggle("is-selected", card.dataset.eventId === selectedEventId);
  });
}

function highlightRegistryRow(eventId) {
  selectedEventId = eventId || "";

  document.querySelectorAll("#registryTableBody tr").forEach(row => {
    row.classList.toggle("is-selected", row.dataset.eventId === selectedEventId);
  });

  document.querySelectorAll("#portfolioStageGrid .portfolio-stage-card").forEach(card => {
    card.classList.toggle("is-selected", card.dataset.eventId === selectedEventId);
  });

  document.querySelectorAll("#intelFeedGrid .intel-feed-card").forEach(card => {
    card.classList.toggle("is-selected", card.dataset.eventId === selectedEventId);
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


const PORTFOLIO_STAGE_SLOTS = [
  { key: "import", label: "Import" },
  { key: "upstream", label: "Upstream" },
  { key: "downstream", label: "Downstream / retail" },
  { key: "consumer", label: "Consumer" }
];

function isPrimaryFlag(value) {
  const v = String(value || "").trim().toLowerCase();
  return v === "yes" || v === "true" || v === "1";
}

function portfolioStageSlot(value) {
  const v = String(value || "").trim().toLowerCase();
  if (v === "import") return "import";
  if (v === "upstream") return "upstream";
  if (v === "consumer") return "consumer";
  if (v === "downstream" || v === "retail") return "downstream";
  return "downstream";
}

function preferredCaseForStage(stageCases) {
  return [...stageCases].sort((a, b) => {
    const primaryDiff = Number(isPrimaryFlag(b.primary_case_flag)) - Number(isPrimaryFlag(a.primary_case_flag));
    if (primaryDiff !== 0) return primaryDiff;

    const confidenceDiff = confidenceRank(b.confidence_tier) - confidenceRank(a.confidence_tier);
    if (confidenceDiff !== 0) return confidenceDiff;

    const aM6 = Math.abs(Number(summaries[a.case_id]?.m6 ?? 0));
    const bM6 = Math.abs(Number(summaries[b.case_id]?.m6 ?? 0));
    if (bM6 !== aM6) return bM6 - aM6;

    const aOrder = Number(a.display_order ?? 9999);
    const bOrder = Number(b.display_order ?? 9999);
    if (aOrder !== bOrder) return aOrder - bOrder;

    return fmtText(a.case_name, "").localeCompare(fmtText(b.case_name, ""));
  })[0];
}

function groupedPortfolioEvents() {
  const groups = new Map();

  cases.forEach(c => {
    const event = getEventById(c.event_id);
    if (!event) return;

    if (!groups.has(event.event_id)) {
      groups.set(event.event_id, {
        event,
        stageCases: {
          import: [],
          upstream: [],
          downstream: [],
          consumer: []
        }
      });
    }

    const group = groups.get(event.event_id);
    const slot = portfolioStageSlot(c.case_stage);
    group.stageCases[slot].push(c);
  });

  return [...groups.values()].sort((a, b) => {
    const dateDiff = fmtText(b.event.effective_date, "").localeCompare(fmtText(a.event.effective_date, ""));
    if (dateDiff !== 0) return dateDiff;
    return fmtText(a.event.title, "").localeCompare(fmtText(b.event.title, ""));
  });
}

function renderPortfolioStageGrid() {
  const grid = byId("portfolioStageGrid");
  if (!grid) return;

  const groups = groupedPortfolioEvents();

  if (!groups.length) {
    grid.innerHTML = `<div class="portfolio-stage-empty">No live cases found.</div>`;
    return;
  }

  grid.innerHTML = "";

  groups.forEach(group => {
    const allCases = Object.values(group.stageCases).flat();
    const defaultCase = allCases.length ? preferredCaseForStage(allCases) : null;

    const stageHtml = PORTFOLIO_STAGE_SLOTS.map(slot => {
      const stageCases = group.stageCases[slot.key] || [];

      if (!stageCases.length) {
        return `
          <div class="portfolio-stage-slot is-empty">
            <div class="portfolio-stage-slot-head">
              <span class="portfolio-stage-name">${slot.label}</span>
            </div>
            <div class="portfolio-stage-slot-empty">No mapped case</div>
          </div>
        `;
      }

      const mainCase = preferredCaseForStage(stageCases);
      const summary = summaries[mainCase.case_id] || {};
      const extraCount = stageCases.length > 1
        ? `<span class="portfolio-stage-count">+${stageCases.length - 1} more</span>`
        : "";

      return `
        <button
          type="button"
          class="portfolio-stage-slot is-filled"
          data-case-id="${mainCase.case_id}"
        >
          <div class="portfolio-stage-slot-head">
            <span class="portfolio-stage-name">${slot.label}</span>
            ${extraCount}
          </div>
          <div class="portfolio-stage-case">${escapeHtml(fmtText(mainCase.case_name, ""))}</div>
          <div class="portfolio-stage-slot-meta">
            ${metricPillHtml(summary.m6)}
            <span class="badge ${confidenceClass(mainCase.confidence_tier)}">${escapeHtml(fmtText(mainCase.confidence_tier, ""))}</span>
          </div>
        </button>
      `;
    }).join("");

    const card = document.createElement("article");
    card.className = "portfolio-stage-card";
    card.dataset.eventId = group.event.event_id;

    if (group.event.event_id === selectedEventId) {
      card.classList.add("is-selected");
    }

    card.innerHTML = `
      <div class="portfolio-stage-card-top">
        <div class="portfolio-stage-card-copy">
          <h3>${escapeHtml(fmtText(group.event.title, ""))}</h3>
          <p class="meta portfolio-stage-meta">
            ${escapeHtml(fmtText(group.event.authority, ""))} | Effective ${escapeHtml(fmtText(group.event.effective_date, "—"))} | ${escapeHtml(prettyStatus(group.event.status_bucket))}
          </p>
        </div>
        <div class="portfolio-stage-card-badges">
          <span class="badge cases-live">${allCases.length} case${allCases.length === 1 ? "" : "s"}</span>
        </div>
      </div>

      <div class="portfolio-stage-lane">
        ${stageHtml}
      </div>
    `;

    card.addEventListener("click", () => {
      if (hasFeedWorkspace()) {
        if (defaultCase) {
          selectEventAndCase(group.event.event_id, defaultCase.case_id);
        } else {
          selectEventAndCase(group.event.event_id, "");
        }
        byId("eventSelect")?.scrollIntoView({ behavior: "smooth", block: "start" });
        return;
      }

      openFeedPage(group.event.event_id, defaultCase?.case_id || "");
    });

    grid.appendChild(card);

    card.querySelectorAll(".portfolio-stage-slot.is-filled").forEach(node => {
      node.addEventListener("click", evt => {
        evt.stopPropagation();
        const caseId = node.dataset.caseId || "";

        if (hasFeedWorkspace()) {
          selectEventAndCase(group.event.event_id, caseId);
          byId("eventSelect")?.scrollIntoView({ behavior: "smooth", block: "start" });
          return;
        }

        openFeedPage(group.event.event_id, caseId);
      });
    });
  });
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
  renderPortfolioStageGrid();

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

    const primaryBadge = yesish(c.primary_case_flag)
      ? `<span class="badge primary-badge">Primary</span>`
      : "";

    tr.innerHTML = `
      <td>
        <div class="table-title-row">
          <span class="table-title">${escapeHtml(fmtText(event?.title, ""))}</span>
        </div>
        <div class="table-subrow">Effective ${escapeHtml(fmtText(event?.effective_date, "—"))}</div>
      </td>
      <td>
        <div class="table-title-row">
          <span class="table-title">${escapeHtml(fmtText(c.case_name, ""))}</span>
          ${primaryBadge}
        </div>
        <div class="table-subrow">${escapeHtml(prettyStatus(c.case_stage))}</div>
      </td>
      <td>${escapeHtml(fmtText(event?.authority, ""))}</td>
      <td>${escapeHtml(fmtText(c.source_type, ""))}</td>
      <td><span class="badge ${confidenceClass(c.confidence_tier)}">${escapeHtml(fmtText(c.confidence_tier, ""))}</span></td>
      <td class="num">${metricPillHtml(summary.m3)}</td>
      <td class="num">${metricPillHtml(summary.m6)}</td>
      <td class="num">${metricPillHtml(summary.m12)}</td>
      <td><span class="badge ${signClass(summary.sign)}">${escapeHtml(fmtText(summary.sign, ""))}</span></td>
      <td><span class="badge status-${statusGroup(event?.status_bucket)}">${escapeHtml(prettyStatus(event?.status_bucket))}</span></td>
    `;

    tr.addEventListener("click", () => {
      if (hasFeedWorkspace()) {
        selectEventAndCase(c.event_id, c.case_id);
        byId("eventSelect")?.scrollIntoView({ behavior: "smooth", block: "start" });
        return;
      }

      openFeedPage(c.event_id, c.case_id);
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
  setText("summaryTotalEvents", fmtInteger(tariffs.length));
  setText(
    "summaryCurrentEvents",
    fmtInteger(tariffs.filter(event => statusGroup(event.status_bucket) === "current").length)
  );
  setText(
    "summaryHistoricalEvents",
    fmtInteger(tariffs.filter(event => statusGroup(event.status_bucket) === "historical").length)
  );
  setText(
    "summaryInvalidatedEvents",
    fmtInteger(tariffs.filter(event => statusGroup(event.status_bucket) === "invalidated").length)
  );
  setText(
    "summaryMappedEvents",
    fmtInteger(tariffs.filter(event => event.has_live_cases).length)
  );
  setText(
    "summaryUnmappedEvents",
    fmtInteger(tariffs.filter(event => !event.has_live_cases).length)
  );
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

    const liveCaseBadgeClass = event.has_live_cases ? "cases-live" : "cases-empty";

    tr.innerHTML = `
      <td>
        <div class="table-title-row">
          <span class="table-title">${escapeHtml(fmtText(event.title, ""))}</span>
        </div>
        <div class="table-subrow">${escapeHtml(fmtText(event.country_scope || event.country, ""))} | ${escapeHtml(fmtText(event.product_scope, ""))}</div>
      </td>
      <td>${escapeHtml(fmtText(event.authority, ""))}</td>
      <td>${escapeHtml(fmtText(event.effective_date, ""))}</td>
      <td><span class="badge status-${statusGroup(event.status_bucket)}">${escapeHtml(prettyStatus(event.status_bucket))}</span></td>
      <td><span class="badge ${liveCaseBadgeClass}">${escapeHtml(fmtInteger(event.live_case_count || 0))} case${Number(event.live_case_count || 0) === 1 ? "" : "s"}</span></td>
      <td><span class="badge">${escapeHtml(prettyStatus(event.case_coverage_status))}</span></td>
      <td><span class="badge ${confidenceClass(event.incidence_priority)}">${escapeHtml(prettyStatus(event.incidence_priority))}</span></td>
      <td><span class="badge ${stageClass(event.candidate_stage)}">${escapeHtml(prettyStatus(event.candidate_stage))}</span></td>
      <td class="table-note-cell" title="${escapeHtml(fmtText(event.candidate_notes, ""))}">${escapeHtml(truncateText(event.candidate_notes, 120) || "—")}</td>
      <td>${sourceLinkHtml(fmtText(event.legal_source_label, "Source"), event.legal_source_url)}</td>
    `;

    tr.addEventListener("click", () => {
      if (hasFeedWorkspace()) {
        selectEventAndCase(event.event_id, "");
        byId("eventSelect")?.scrollIntoView({ behavior: "smooth", block: "start" });
        return;
      }

      openFeedPage(event.event_id, "");
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
      <td>
        <div class="table-title-row">
          <span class="table-title">${escapeHtml(fmtText(event.title, ""))}</span>
        </div>
        <div class="table-subrow">${escapeHtml(fmtText(event.country_scope || event.country, ""))} | ${escapeHtml(fmtText(event.product_scope, ""))}</div>
      </td>
      <td>${escapeHtml(fmtText(event.authority, ""))}</td>
      <td>${escapeHtml(fmtText(event.effective_date, ""))}</td>
      <td><span class="badge ${stageClass(event.candidate_stage)}">${escapeHtml(prettyStatus(event.candidate_stage))}</span></td>
      <td><span class="badge ${confidenceClass(event.incidence_priority)}">${escapeHtml(prettyStatus(event.incidence_priority))}</span></td>
      <td><span class="badge">${escapeHtml(prettyStatus(event.case_coverage_status))}</span></td>
      <td class="table-note-cell" title="${escapeHtml(fmtText(event.candidate_notes, ""))}">${escapeHtml(truncateText(event.candidate_notes, 150) || "—")}</td>
      <td>${sourceLinkHtml(fmtText(event.legal_source_label, "Source"), event.legal_source_url)}</td>
    `;

    tr.addEventListener("click", () => {
      if (hasFeedWorkspace()) {
        selectEventAndCase(event.event_id, "");
        byId("eventSelect")?.scrollIntoView({ behavior: "smooth", block: "start" });
        return;
      }

      openFeedPage(event.event_id, "");
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
  renderIntelFeed();
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

    const leftExtent = paddedExtent(
      [...chartData.treatment, ...chartData.control],
      0.06
    );
    const rightExtent = paddedExtent(chartData.relative_effect, 0.12);

    chart = new Chart(ctx, {
      type: "line",
      data: {
        labels: chartData.labels,
        datasets: [
          {
            label: selectedCase.treatment_label,
            data: chartData.treatment,
            yAxisID: "y",
            tension: 0.2,
            borderWidth: 2.4,
            borderColor: chartPalette.treatment,
            backgroundColor: chartPalette.treatment,
            pointBackgroundColor: chartPalette.treatment,
            pointBorderColor: "#ffffff",
            pointBorderWidth: 1.5,
            pointRadius(context) {
              return context.dataIndex === eventIndex ? 4 : 0;
            },
            pointHoverRadius: 4
          },
          {
            label: selectedCase.control_label,
            data: chartData.control,
            yAxisID: "y",
            tension: 0.2,
            borderWidth: 2.2,
            borderColor: chartPalette.control,
            backgroundColor: chartPalette.control,
            pointBackgroundColor: chartPalette.control,
            pointBorderColor: "#ffffff",
            pointBorderWidth: 1.5,
            pointRadius(context) {
              return context.dataIndex === eventIndex ? 4 : 0;
            },
            pointHoverRadius: 4
          },
          {
            label: "Relative Effect",
            data: chartData.relative_effect,
            yAxisID: "y1",
            tension: 0.18,
            borderWidth: 2.4,
            borderDash: [7, 5],
            borderColor: chartPalette.relative,
            backgroundColor: chartPalette.relative,
            pointBackgroundColor: chartPalette.relative,
            pointBorderColor: "#ffffff",
            pointBorderWidth: 1.5,
            pointRadius(context) {
              return context.dataIndex === eventIndex ? 4 : 0;
            },
            pointHoverRadius: 4
          }
        ]
      },
      options: {
        responsive: true,
        maintainAspectRatio: true,
        aspectRatio: 2.35,
        interaction: {
          mode: "index",
          intersect: false
        },
        plugins: {
          legend: {
            position: "top",
            labels: {
              usePointStyle: true,
              boxWidth: 10,
              boxHeight: 10
            }
          },
          title: {
            display: true,
            text: selectedCase.case_name,
            font: {
              size: 18,
              weight: "700"
            },
            padding: {
              bottom: 4
            }
          },
          subtitle: {
            display: true,
            text: `${selectedCase.treatment_label} vs ${selectedCase.control_label} | Event month ${eventMonth || "—"}`,
            color: "#667085",
            font: {
              size: 12
            },
            padding: {
              bottom: 12
            }
          },
          tooltip: {
            callbacks: {
              title(items) {
                return items?.[0]?.label || "";
              },
              label(context) {
                const datasetLabel = context.dataset.label || "";
                const raw = context.raw;
                const digits = context.dataset.yAxisID === "y1" ? 3 : 2;
                const suffix = context.dataset.yAxisID === "y1" ? " pp" : "";
                return `${datasetLabel}: ${fmtNumber(raw, digits)}${suffix}`;
              }
            }
          },
          postEventWindow: {
            index: eventIndex,
            fillStyle: chartPalette.postWindowFill
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
            min: leftExtent.min,
            max: leftExtent.max,
            title: {
              display: true,
              text: "Rebased price index"
            },
            ticks: {
              callback(value) {
                return fmtNumber(value, 1);
              }
            },
            grid: {
              color: "rgba(15, 23, 42, 0.08)"
            }
          },
          y1: {
            type: "linear",
            position: "right",
            min: rightExtent.min,
            max: rightExtent.max,
            title: {
              display: true,
              text: "Relative effect (pp)"
            },
            ticks: {
              callback(value) {
                return `${fmtNumber(value, 1)}`;
              }
            },
            grid: {
              drawOnChartArea: false,
              color(context) {
                return Number(context.tick?.value) === 0
                  ? "rgba(180, 35, 24, 0.35)"
                  : "rgba(15, 23, 42, 0.08)";
              }
            }
          },
          x: {
            title: {
              display: true,
              text: "Month"
            },
            ticks: {
              maxRotation: 0,
              autoSkip: false,
              callback: function(value, index, ticks) {
                const label = this.getLabelForValue(value);
                if (!ticks || ticks.length <= 8) return label;
                const every = Math.ceil(ticks.length / 8);
                return index % every === 0 ? label : "";
              }
            },
            grid: {
              color: "rgba(15, 23, 42, 0.05)"
            }
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
    const [tariffsRes, casesRes, summaryRes, officialFeedJson] = await Promise.all([
      fetch("./data/tariffs.json", { cache: "no-store" }),
      fetch("./data/cases.json", { cache: "no-store" }),
      fetch("./data/summary.json", { cache: "no-store" }),
      fetchOptionalJson("./data/tariff_feed.json", [])
    ]);

    tariffs = await tariffsRes.json();
    cases = await casesRes.json();
    summaries = await summaryRes.json();
    officialFeed = Array.isArray(officialFeedJson) ? officialFeedJson : [];
    
    if (!tariffsRes.ok || !casesRes.ok || !summaryRes.ok) {
      throw new Error("Failed to load site data files.");
    }

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
    populateFeedAuthorityFilter();
    bindFeedFilters();

    const initialState = applyInitialUrlState();

    bindRegistryFilters();
    bindQueueFilters();
    bindMapFilters();
    bindSorts();
    bindCopyButton();
    bindCsvButtons();
    bindSectionNav();

    renderPortfolioTable();
    renderIntelFeed();
    await renderMapPage();
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

      const initialCaseId = cases.some(c => c.case_id === initialState.caseId)
        ? initialState.caseId
        : "";

      selectedEventId = initialEventId;
      selectedCaseId = initialCaseId;

      if (hasFeedWorkspace()) {
        if (eventSelect) eventSelect.value = initialEventId;
        populateCaseSelect(initialEventId, initialCaseId);
      } else {
        highlightPortfolioRow(initialCaseId);
        highlightRegistryRow(initialEventId);
        highlightBuildQueueRow(initialEventId);
        updateUrlState();
      }
    } else {
      renderNoEvents("No tariff events found");
    }
  } catch (err) {
    console.error(err);
    renderNoEvents("Failed to load site data");
  }
}

loadData();