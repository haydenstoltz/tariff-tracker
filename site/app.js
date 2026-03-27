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

function fmtNumber(value, digits = 3) {
  if (value === null || value === undefined || value === "") return "—";
  return Number(value).toFixed(digits);
}

function fmtInteger(value) {
  if (value === null || value === undefined || value === "") return "—";
  return String(value);
}

function clearDisplay(message = "No case mappings found") {
  document.getElementById("eventTitle").textContent = message;
  document.getElementById("eventMeta").textContent = "";
  document.getElementById("caseTitle").textContent = "";
  document.getElementById("caseMeta").textContent = "";
  document.getElementById("caseCaveat").textContent = "";
  document.getElementById("robustnessNote").textContent = "";
  document.getElementById("m3").textContent = "";
  document.getElementById("m6").textContent = "";
  document.getElementById("m12").textContent = "";
  document.getElementById("direction").textContent = "";
  document.getElementById("preEventGapStd").textContent = "";
  document.getElementById("peakPostGap").textContent = "";
  document.getElementById("peakPostGapMonth").textContent = "";
  document.getElementById("placeboN3").textContent = "";
  document.getElementById("placeboP3").textContent = "";
  document.getElementById("placeboN6").textContent = "";
  document.getElementById("placeboP6").textContent = "";
  document.getElementById("methodNote").textContent = "";

  const downloadJson = document.getElementById("downloadJson");
  const downloadCsv = document.getElementById("downloadCsv");

  downloadJson.removeAttribute("href");
  downloadJson.removeAttribute("download");
  downloadCsv.removeAttribute("href");
  downloadCsv.removeAttribute("download");

  if (chart) {
    chart.destroy();
    chart = null;
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
      option.textContent = event.title;
      eventSelect.appendChild(option);
    });

    eventSelect.addEventListener("change", e => {
      populateCaseSelect(e.target.value);
    });

    caseSelect.addEventListener("change", e => {
      renderCase(e.target.value);
    });

    if (tariffs.length > 0) {
      populateCaseSelect(tariffs[0].event_id);
    } else {
      clearDisplay("No tariff events found");
    }
  } catch (err) {
    console.error(err);
    clearDisplay("Failed to load site data");
  }
}

function populateCaseSelect(eventId) {
  const caseSelect = document.getElementById("caseSelect");
  const eventCases = cases.filter(c => c.event_id === eventId);

  caseSelect.innerHTML = "";

  eventCases.forEach(c => {
    const option = document.createElement("option");
    option.value = c.case_id;
    option.textContent = c.case_name;
    caseSelect.appendChild(option);
  });

  if (eventCases.length > 0) {
    renderCase(eventCases[0].case_id);
  } else {
    clearDisplay("No case mappings found");
  }
}

async function renderCase(caseId) {
  try {
    const selectedCase = cases.find(c => c.case_id === caseId);
    if (!selectedCase) {
      clearDisplay("Case not found");
      return;
    }

    const selectedEvent = tariffs.find(t => t.event_id === selectedCase.event_id);
    const summary = summaries[caseId];

    if (!selectedEvent || !summary) {
      clearDisplay("Missing event or summary data");
      return;
    }

    document.getElementById("eventTitle").textContent = selectedEvent.title;
    document.getElementById("eventMeta").textContent =
      `${selectedEvent.authority} | ${selectedEvent.country} | Effective ${selectedEvent.effective_date}`;

    document.getElementById("caseTitle").textContent = selectedCase.case_name;
    document.getElementById("caseMeta").textContent =
      `${selectedCase.source_type} | Treatment: ${selectedCase.treatment_label} | Control: ${selectedCase.control_label}`;

    document.getElementById("caseCaveat").textContent = selectedCase.caveat || "";
    document.getElementById("robustnessNote").textContent = selectedCase.robustness_note || "";
    document.getElementById("m3").textContent = fmtNumber(summary.m3);
    document.getElementById("m6").textContent = fmtNumber(summary.m6);
    document.getElementById("m12").textContent = fmtNumber(summary.m12);
    document.getElementById("direction").textContent = summary.sign || "—";

    document.getElementById("preEventGapStd").textContent = fmtNumber(summary.pre_event_gap_std_pp);
    document.getElementById("peakPostGap").textContent = fmtNumber(summary.peak_post_gap_pp);
    document.getElementById("peakPostGapMonth").textContent = summary.peak_post_gap_month || "—";
    document.getElementById("placeboN3").textContent = fmtInteger(summary.placebo_n_3m);
    document.getElementById("placeboP3").textContent = fmtNumber(summary.placebo_p_abs_3m);
    document.getElementById("placeboN6").textContent = fmtInteger(summary.placebo_n_6m);
    document.getElementById("placeboP6").textContent = fmtNumber(summary.placebo_p_abs_6m);
    document.getElementById("methodNote").textContent = selectedCase.method_note || "";

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

    const eventMonth = selectedEvent.effective_date.slice(0, 7);
    const eventIndex = chartData.labels.indexOf(eventMonth);

    const ctx = document.getElementById("incidenceChart");

    if (chart) {
      chart.destroy();
    }

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
            label: `Tariff effective: ${eventMonth}`
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
  } catch (err) {
    console.error(err);
    clearDisplay("Failed to render case");
  }
}

loadData();