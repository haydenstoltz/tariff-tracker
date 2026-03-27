let chart = null;
let tariffs = [];
let cases = [];
let summaries = {};

async function loadData() {
  const [tariffsRes, casesRes, summaryRes] = await Promise.all([
    fetch("./data/tariffs.json"),
    fetch("./data/cases.json"),
    fetch("./data/summary.json")
  ]);

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
    clearDisplay();
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
    clearDisplay();
  }
}

function clearDisplay() {
  document.getElementById("eventTitle").textContent = "No case mappings found";
  document.getElementById("eventMeta").textContent = "";
  document.getElementById("caseTitle").textContent = "";
  document.getElementById("caseMeta").textContent = "";
  document.getElementById("caseCaveat").textContent = "";
  document.getElementById("m3").textContent = "";
  document.getElementById("m6").textContent = "";
  document.getElementById("m12").textContent = "";
  document.getElementById("direction").textContent = "";

  if (chart) {
    chart.destroy();
    chart = null;
  }
}

async function renderCase(caseId) {
  const selectedCase = cases.find(c => c.case_id === caseId);
  if (!selectedCase) {
    clearDisplay();
    return;
  }

  const selectedEvent = tariffs.find(t => t.event_id === selectedCase.event_id);
  const summary = summaries[caseId];

  document.getElementById("eventTitle").textContent = selectedEvent.title;
  document.getElementById("eventMeta").textContent =
    `${selectedEvent.authority} | ${selectedEvent.country} | Effective ${selectedEvent.effective_date}`;

  document.getElementById("caseTitle").textContent = selectedCase.case_name;
  document.getElementById("caseMeta").textContent =
    `${selectedCase.source_type} | Treatment: ${selectedCase.treatment_label} | Control: ${selectedCase.control_label}`;

  document.getElementById("caseCaveat").textContent = selectedCase.caveat || "";
  document.getElementById("m3").textContent = summary.m3.toFixed(3);
  document.getElementById("m6").textContent = summary.m6.toFixed(3);
  document.getElementById("m12").textContent = summary.m12.toFixed(3);
  document.getElementById("direction").textContent = summary.sign;

  const chartRes = await fetch(selectedCase.chart_file);
  const chartData = await chartRes.json();

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
          yAxisID: "y"
        },
        {
          label: selectedCase.control_label,
          data: chartData.control,
          yAxisID: "y"
        },
        {
          label: "Relative Effect",
          data: chartData.relative_effect,
          yAxisID: "y1",
          borderDash: [6, 6]
        }
      ]
    },
    options: {
      responsive: true,
      interaction: {
        mode: "index",
        intersect: false
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
}

loadData();