let chart;

async function loadData() {
  const [eventsRes, resultsRes] = await Promise.all([
    fetch("./data/events.json"),
    fetch("./data/results.json")
  ]);

  const events = await eventsRes.json();
  const results = await resultsRes.json();

  const select = document.getElementById("eventSelect");
  const title = document.getElementById("eventTitle");
  const meta = document.getElementById("eventMeta");
  const m3 = document.getElementById("m3");
  const m6 = document.getElementById("m6");
  const m12 = document.getElementById("m12");

  events.forEach(event => {
    const option = document.createElement("option");
    option.value = event.event_id;
    option.textContent = event.title;
    select.appendChild(option);
  });

  function render(eventId) {
    const event = events.find(e => e.event_id === eventId);
    const result = results[eventId];

    title.textContent = event.title;
    meta.textContent = `${event.authority} | ${event.country} | Effective ${event.effective_date}`;
    m3.textContent = result.incidence[0].toFixed(3);
    m6.textContent = result.incidence[1].toFixed(3);
    m12.textContent = result.incidence[2].toFixed(3);

    const ctx = document.getElementById("incidenceChart");

    if (chart) chart.destroy();

    chart = new Chart(ctx, {
      type: "line",
      data: {
        labels: result.months,
        datasets: [{
          label: "Relative Effect",
          data: result.incidence
        }]
      },
      options: {
        responsive: true,
        scales: {
          x: { title: { display: true, text: "Months After Tariff" } },
          y: { title: { display: true, text: "Incidence (pp)" } }
        }
      }
    });
  }

  select.addEventListener("change", e => render(e.target.value));
  render(events[0].event_id);
}

loadData();
