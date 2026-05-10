const lifecycleStates = ["Searching", "Reserved", "Waiting", "Charging", "Completed"];

const state = {
  socket: null,
  payload: null,
  connected: false,
};

const els = {
  simTime: document.getElementById("sim-time"),
  activeVehicles: document.getElementById("active-vehicles"),
  activeSessions: document.getElementById("active-sessions"),
  pendingReservations: document.getElementById("pending-reservations"),
  freeWindows: document.getElementById("free-windows"),
  playPause: document.getElementById("play-pause"),
  speedSelect: document.getElementById("speed-select"),
  resetSim: document.getElementById("reset-sim"),
  stationTimelines: document.getElementById("station-timelines"),
  decisionBody: document.getElementById("decision-body"),
  lifecycleColumns: document.getElementById("lifecycle-columns"),
  eventStream: document.getElementById("event-stream"),
  connectionState: document.getElementById("connection-state"),
};

function fmtTime(value) {
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return "--:--";
  return date.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" });
}

function fmtWindow(start, end) {
  return `${fmtTime(start)}-${fmtTime(end)}`;
}

function setConnection(connected) {
  state.connected = connected;
  els.connectionState.textContent = connected ? "Realtime" : "Polling";
  els.connectionState.classList.toggle("connected", connected);
}

async function getDashboard() {
  const response = await fetch("/api/dashboard", { cache: "no-store" });
  if (!response.ok) throw new Error("Dashboard unavailable");
  return response.json();
}

async function postJson(url, payload = {}) {
  const response = await fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
  if (!response.ok) throw new Error(`${url} failed`);
  return response.json();
}

function render(payload) {
  state.payload = payload;
  renderControls(payload.clock || {});
  renderMetrics(payload.metrics || {});
  renderStations(payload.stations || []);
  renderDecision(payload.decision || {});
  renderLifecycle(payload.vehicles || []);
  renderEvents(payload.events || []);
}

function renderControls(clock) {
  els.simTime.textContent = fmtTime(clock.current_time);
  els.playPause.textContent = clock.paused || !clock.running ? "Play" : "Pause";
  if (clock.speed) els.speedSelect.value = String(clock.speed);
}

function renderMetrics(metrics) {
  els.activeVehicles.textContent = metrics.active_vehicles || 0;
  els.activeSessions.textContent = metrics.active_sessions || 0;
  els.pendingReservations.textContent = metrics.pending_reservations || 0;
  els.freeWindows.textContent = metrics.free_windows || 0;
}

function renderStations(stations) {
  if (!stations.length) {
    els.stationTimelines.innerHTML = `<div class="empty-state">No station timeline state available.</div>`;
    return;
  }

  els.stationTimelines.replaceChildren(...stations.map(renderStation));
}

function renderStation(station) {
  const details = document.createElement("details");
  details.className = "station-panel";
  details.open = true;

  const summary = document.createElement("summary");
  const busyCount = station.chargers.filter((charger) => charger.state !== "free").length;
  summary.innerHTML = `
    <div>
      <span class="station-overline">${station.location || "Coordination zone"}</span>
      <strong>${station.station_name}</strong>
    </div>
    <span>${busyCount}/${station.charger_count} active</span>
  `;
  details.appendChild(summary);

  const axis = document.createElement("div");
  axis.className = "time-axis";
  axis.appendChild(document.createElement("span"));
  for (const tick of station.time_axis || []) {
    const label = document.createElement("span");
    label.textContent = fmtTime(tick);
    axis.appendChild(label);
  }
  details.appendChild(axis);

  const rows = document.createElement("div");
  rows.className = "charger-rows";
  for (const charger of station.chargers || []) {
    rows.appendChild(renderChargerRow(charger));
  }
  details.appendChild(rows);

  const windows = document.createElement("div");
  windows.className = "free-window-strip";
  const freeWindows = station.free_windows || [];
  windows.append(
    ...freeWindows.slice(0, 6).map((window) => {
      const pill = document.createElement("span");
      pill.textContent = `C${window.charger_id} ${fmtWindow(window.start, window.end)}`;
      return pill;
    }),
  );
  if (!freeWindows.length) {
    const pill = document.createElement("span");
    pill.textContent = "No reusable window in horizon";
    windows.appendChild(pill);
  }
  details.appendChild(windows);
  return details;
}

function renderChargerRow(charger) {
  const row = document.createElement("div");
  row.className = "charger-row";

  const label = document.createElement("div");
  label.className = "charger-label";
  label.innerHTML = `<strong>C${charger.charger_id}</strong><span>${charger.state}</span>`;
  row.appendChild(label);

  const track = document.createElement("div");
  track.className = "timeline-track";
  for (const segment of charger.segments || []) {
    const block = document.createElement("span");
    block.className = `segment ${segment.status}`;
    block.title = `${fmtWindow(segment.start, segment.end)} ${segment.status}${segment.vehicle_id ? ` ${segment.vehicle_id}` : ""}`;
    track.appendChild(block);
  }
  row.appendChild(track);
  return row;
}

function renderDecision(decision) {
  const candidates = decision.candidate_stations || [];
  const selected = decision.selected_station;
  const rows = candidates.map((candidate) => `
    <div class="candidate-row ${candidate.station_id === selected ? "selected" : ""}">
      <span>${candidate.station_name}</span>
      <strong>${candidate.wait_minutes >= 999 ? "No fit" : `${candidate.wait_minutes} min`}</strong>
      <em>score ${candidate.score}</em>
    </div>
  `).join("");

  els.decisionBody.innerHTML = `
    <div class="decision-summary">
      <span>Final Allocation</span>
      <strong>${selected ? `Station ${selected}` : "Pending"}</strong>
    </div>
    <div class="candidate-list">${rows || `<div class="empty-state">Waiting for candidate analysis.</div>`}</div>
    <p class="reasoning">${decision.reasoning || "Waiting for the next coordinator decision."}</p>
  `;
}

function renderLifecycle(vehicles) {
  els.lifecycleColumns.replaceChildren(...lifecycleStates.map((status) => {
    const column = document.createElement("div");
    column.className = "lifecycle-column";
    const items = vehicles.filter((vehicle) => vehicle.status === status).slice(0, 8);
    column.innerHTML = `<h3>${status}<span>${items.length}</span></h3>`;
    if (!items.length) {
      const empty = document.createElement("div");
      empty.className = "mini-empty";
      empty.textContent = "None";
      column.appendChild(empty);
      return column;
    }
    for (const vehicle of items) {
      const card = document.createElement("div");
      card.className = "vehicle-chip";
      card.innerHTML = `
        <strong>${vehicle.vehicle_id}</strong>
        <span>${vehicle.assigned_station ? `S${vehicle.assigned_station} C${vehicle.assigned_charger}` : `SOC ${vehicle.soc || "--"}%`}</span>
      `;
      column.appendChild(card);
    }
    return column;
  }));
}

function renderEvents(events) {
  if (!events.length) {
    els.eventStream.innerHTML = `<div class="empty-state">No operational events yet.</div>`;
    return;
  }
  els.eventStream.replaceChildren(...events.slice(0, 50).map((event) => {
    const row = document.createElement("div");
    row.className = "event-row";
    row.innerHTML = `
      <time>${fmtTime(event.timestamp)}</time>
      <div>
        <strong>${String(event.event_type || "").replaceAll("_", " ")}</strong>
        <p>${event.message || event.event_message || ""}</p>
      </div>
    `;
    return row;
  }));
}

function connectWebSocket() {
  const protocol = window.location.protocol === "https:" ? "wss" : "ws";
  const socket = new WebSocket(`${protocol}://${window.location.host}/api/ws`);
  state.socket = socket;

  socket.addEventListener("open", () => setConnection(true));
  socket.addEventListener("message", (event) => {
    const message = JSON.parse(event.data);
    if (message.type === "snapshot") render(message.payload);
  });
  socket.addEventListener("close", () => {
    setConnection(false);
    setTimeout(connectWebSocket, 2500);
  });
  socket.addEventListener("error", () => setConnection(false));
}

async function refreshFallback() {
  if (state.connected) return;
  try {
    render(await getDashboard());
  } catch (error) {
    els.connectionState.textContent = "Offline";
  }
}

els.playPause.addEventListener("click", async () => {
  const clock = state.payload?.clock || {};
  if (clock.paused || !clock.running) {
    await postJson("/api/simulation/start", { speed: Number(els.speedSelect.value) });
  } else {
    await postJson("/api/simulation/pause");
  }
  render(await getDashboard());
});

els.speedSelect.addEventListener("change", async () => {
  await postJson("/api/simulation/speed", { speed: Number(els.speedSelect.value) });
});

els.resetSim.addEventListener("click", async () => {
  await postJson("/api/simulation/reset");
  render(await getDashboard());
});

connectWebSocket();
refreshFallback();
setInterval(refreshFallback, 1500);
