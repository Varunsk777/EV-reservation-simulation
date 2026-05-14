const lifecycleStates = ["Searching", "Reserved", "Waiting", "Charging", "Completed", "Released"];
const BASE_TICK_SECONDS = 3.2;
const MIN_TICK_SECONDS = 0.45;
const knownSegmentStates = new Set(["available", "reserved", "waiting", "charging", "completed", "fault"]);

const state = {
  socket: null,
  payload: null,
  connected: false,
  clockAnchor: null,
  animationFrame: null,
};

const els = {
  simTime: document.getElementById("sim-time"),
  activeVehicles: document.getElementById("active-vehicles"),
  activeSessions: document.getElementById("active-sessions"),
  pendingReservations: document.getElementById("pending-reservations"),
  freeWindows: document.getElementById("free-windows"),
  adaptiveReallocations: document.getElementById("adaptive-reallocations"),
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
  renderDecision(payload.decision || {}, payload.adaptive_recommendations || []);
  renderLifecycle(payload.vehicles || []);
  renderEvents(payload.events || []);
}

function renderControls(clock) {
  els.simTime.textContent = fmtTime(clock.current_time);
  els.playPause.textContent = clock.paused || !clock.running ? "Play" : "Pause";
  if (clock.speed) els.speedSelect.value = String(clock.speed);
  syncClockAnchor(clock);
  ensureTimelineAnimation();
}

function renderMetrics(metrics) {
  els.activeVehicles.textContent = metrics.active_vehicles || 0;
  els.activeSessions.textContent = metrics.active_sessions || 0;
  els.pendingReservations.textContent = metrics.pending_reservations || 0;
  els.freeWindows.textContent = metrics.free_windows || 0;
  els.adaptiveReallocations.textContent = metrics.adaptive_reallocations || 0;
}

function renderStations(stations) {
  if (!stations.length) {
    els.stationTimelines.innerHTML = `<div class="empty-state">No station timeline state available.</div>`;
    return;
  }

  const simNowMs = getSimNowMs();
  els.stationTimelines.replaceChildren(...stations.map((station) => renderStation(station, simNowMs)));
}

function renderStation(station, simNowMs) {
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

  const timeline = document.createElement("div");
  timeline.className = "station-timeline-shell";

  const axis = document.createElement("div");
  axis.className = "time-axis";
  axis.appendChild(document.createElement("span"));
  for (const tick of station.time_axis || []) {
    const label = document.createElement("span");
    label.textContent = fmtTime(tick);
    axis.appendChild(label);
  }
  timeline.appendChild(axis);

  const rows = document.createElement("div");
  rows.className = "charger-rows";
  const { startMs, endMs } = getStationTimeBounds(station);
  for (const charger of station.chargers || []) {
    const overlays = (station.adaptive_overlays || []).filter((item) => Number(item.charger_id) === Number(charger.charger_id));
    rows.appendChild(renderChargerRow(charger, simNowMs, overlays, startMs, endMs));
  }
  timeline.appendChild(rows);

  const cursor = document.createElement("div");
  cursor.className = "current-time-cursor";
  cursor.dataset.startMs = String(startMs);
  cursor.dataset.endMs = String(endMs);
  timeline.appendChild(cursor);
  details.appendChild(timeline);

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

function renderChargerRow(charger, simNowMs, overlays = [], boundsStartMs = 0, boundsEndMs = 1) {
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
    const status = normalizeSegmentStatus(segment.status);
    block.className = `segment ${status} ${segmentPhase(segment.start, segment.end, simNowMs)}`;
    block.dataset.startMs = String(Date.parse(segment.start));
    block.dataset.endMs = String(Date.parse(segment.end));
    block.dataset.status = status;
    if (status === "charging" && isCurrentSegment(segment.start, segment.end, simNowMs)) {
      block.classList.add("active");
    }
    block.title = `${fmtWindow(segment.start, segment.end)} ${segment.status}${segment.vehicle_id ? ` ${segment.vehicle_id}` : ""}`;
    track.appendChild(block);
  }
  for (const overlay of overlays) {
    const marker = document.createElement("span");
    const startMs = Date.parse(overlay.start);
    const endMs = Date.parse(overlay.end);
    const total = Math.max(1, boundsEndMs - boundsStartMs);
    const left = Math.max(0, Math.min(100, ((startMs - boundsStartMs) / total) * 100));
    const right = Math.max(0, Math.min(100, ((endMs - boundsStartMs) / total) * 100));
    marker.className = `adaptive-overlay ${overlay.type === "requested" ? "requested" : "suggested"}`;
    marker.style.left = `${left}%`;
    marker.style.width = `${Math.max(3, right - left)}%`;
    marker.title = `${overlay.type === "requested" ? "Requested overlap" : "Suggested window"} ${fmtWindow(overlay.start, overlay.end)}`;
    track.appendChild(marker);
  }
  row.appendChild(track);
  return row;
}

function renderDecision(decision, recommendations = []) {
  const candidates = decision.candidate_stations || [];
  const selected = decision.selected_station;
  const rows = candidates.map((candidate) => `
    <div class="candidate-row ${candidate.station_id === selected ? "selected" : ""}">
      <span>${candidate.station_name}</span>
      <strong>${candidate.wait_minutes >= 999 ? "No fit" : `${candidate.wait_minutes} min`}</strong>
      <em>score ${candidate.score}${candidate.rejection_reason ? ` • ${candidate.rejection_reason.replaceAll("_", " ")}` : ""}</em>
    </div>
  `).join("");

  const recommendationCards = recommendations.slice(0, 3).map(renderRecommendationCard).join("");
  els.decisionBody.innerHTML = `
    <div class="decision-summary">
      <span>Final Allocation</span>
      <strong>${selected ? `Station ${selected}` : "Pending"}</strong>
    </div>
    ${recommendationCards ? `<div class="recommendation-stack">${recommendationCards}</div>` : ""}
    <div class="candidate-list">${rows || `<div class="empty-state">Waiting for candidate analysis.</div>`}</div>
    <p class="reasoning">${decision.reasoning || "Waiting for the next coordinator decision."}</p>
  `;
}

function renderRecommendationCard(recommendation) {
  const requested = recommendation.requested || {};
  const suggested = recommendation.suggested || {};
  return `
    <article class="recommendation-card">
      <div class="recommendation-head">
        <span>Adaptive Scheduling</span>
        <strong>${recommendation.summary || "Preferred interval unavailable"}</strong>
      </div>
      <div class="recommendation-window">
        <span>Suggested Best Window</span>
        <strong>${suggested.station_name || "Station"} • C${suggested.charger_id || "-"}</strong>
        <em>${fmtWindow(suggested.start, suggested.end)}</em>
      </div>
      <div class="recommendation-meta">
        <span>Estimated Delay: +${recommendation.estimated_delay_minutes || 0} min</span>
        <span>Optimization Score: ${recommendation.optimization_score || 0}</span>
      </div>
      <p>${requested.station_name || "Preferred station"} C${requested.charger_id || "-"} unavailable.</p>
    </article>
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
      const phase = vehicle.coordination_phase ? ` · ${vehicle.coordination_phase}` : "";
      card.innerHTML = `
        <strong>${vehicle.vehicle_id}</strong>
        <span>${vehicle.assigned_station ? `S${vehicle.assigned_station} C${vehicle.assigned_charger}` : `SOC ${vehicle.soc || "--"}%`}${phase}</span>
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
    if (event.event_type === "adaptive_allocation") row.classList.add("adaptive");
    row.innerHTML = `
      <time>${fmtTime(event.timestamp)}</time>
      <div>
        <strong>${formatEventType(event.event_type)}</strong>
        <p>${event.message || event.event_message || ""}</p>
      </div>
    `;
    return row;
  }));
}

function tickMinutes(speed) {
  return Math.max(3, Math.min(18, 3 + Number(speed) * 2));
}

function tickDelay(speed) {
  return Math.max(MIN_TICK_SECONDS, BASE_TICK_SECONDS / Math.max(1, Number(speed)));
}

function simulationMinutesPerSecond(speed) {
  return tickMinutes(speed) / tickDelay(speed);
}

function syncClockAnchor(clock) {
  const parsed = Date.parse(clock.current_time);
  if (Number.isNaN(parsed)) return;
  const speed = Number(clock.speed || 1);
  state.clockAnchor = {
    simMs: parsed,
    speed,
    paused: Boolean(clock.paused || !clock.running),
    realMs: performance.now(),
  };
}

function getSimNowMs() {
  if (!state.clockAnchor) return Date.now();
  if (state.clockAnchor.paused) return state.clockAnchor.simMs;
  const elapsedSeconds = Math.max(0, (performance.now() - state.clockAnchor.realMs) / 1000);
  return state.clockAnchor.simMs + elapsedSeconds * simulationMinutesPerSecond(state.clockAnchor.speed) * 60_000;
}

function getStationTimeBounds(station) {
  const firstCharger = station.chargers?.[0];
  const firstSegment = firstCharger?.segments?.[0];
  const lastSegment = firstCharger?.segments?.[firstCharger?.segments?.length - 1];
  const axis = station.time_axis || [];
  const startMs = Date.parse(firstSegment?.start || axis[0] || Date.now());
  const endMs = Date.parse(lastSegment?.end || axis[axis.length - 1] || Date.now() + 1);
  return { startMs, endMs };
}

function segmentPhase(start, end, nowMs) {
  const startMs = Date.parse(start);
  const endMs = Date.parse(end);
  if (Number.isNaN(startMs) || Number.isNaN(endMs)) return "future";
  if (endMs <= nowMs) return "past";
  if (startMs > nowMs) return "future";
  return "current";
}

function isCurrentSegment(start, end, nowMs) {
  const startMs = Date.parse(start);
  const endMs = Date.parse(end);
  return !Number.isNaN(startMs) && !Number.isNaN(endMs) && startMs <= nowMs && nowMs < endMs;
}

function normalizeSegmentStatus(status) {
  return knownSegmentStates.has(status) ? status : "available";
}

function updateTimelineDynamics(simNowMs) {
  document.querySelectorAll(".current-time-cursor").forEach((cursor) => {
    const startMs = Number(cursor.dataset.startMs);
    const endMs = Number(cursor.dataset.endMs);
    const span = Math.max(1, endMs - startMs);
    const ratio = Math.max(0, Math.min(1, (simNowMs - startMs) / span));
    cursor.style.left = `${ratio * 100}%`;
  });

  document.querySelectorAll(".timeline-track .segment").forEach((segment) => {
    const startMs = Number(segment.dataset.startMs);
    const endMs = Number(segment.dataset.endMs);
    const status = segment.dataset.status || "available";
    const past = endMs <= simNowMs;
    const current = startMs <= simNowMs && simNowMs < endMs;
    segment.classList.toggle("past", past);
    segment.classList.toggle("current", current);
    segment.classList.toggle("future", !past && !current);
    segment.classList.toggle("active", status === "charging" && current);
  });
}

function ensureTimelineAnimation() {
  if (state.animationFrame) return;
  const loop = () => {
    if (state.payload) {
      updateTimelineDynamics(getSimNowMs());
    }
    state.animationFrame = window.requestAnimationFrame(loop);
  };
  state.animationFrame = window.requestAnimationFrame(loop);
}

function formatEventType(eventType) {
  const labels = {
    slot_reserved: "Reserved",
    vehicle_waiting: "Waiting",
    charging_started: "Charging started",
    charging_completed: "Charging completed",
    slot_released: "Charger released",
    reservation_created: "Reservation created",
    reservation_cancelled: "Reservation cancelled",
    coordinator_decision: "Coordinator decision",
    queue_updated: "Queue updated",
    conflict_detected: "Conflict detected",
    adaptive_allocation: "Adaptive Allocation",
    rerouting: "Rerouting",
    queue_overflow: "Queue overflow",
    priority_preempted: "Priority preempted",
    allocating: "Allocating",
    conflict_escalating: "Conflict escalating",
    allocation_retry: "Allocation retry",
    station_congestion: "Station congestion",
  };
  return labels[eventType] || String(eventType || "").replaceAll("_", " ");
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
