const POLL_INTERVAL_MS = 1000;
const DASHBOARD_ENDPOINTS = ["/api/dashboard"];
const START_SIMULATION_ENDPOINT = "/api/simulation/start";

const state = {
  endpoint: null,
  payloadHash: "",
};

const els = {
  stationsGrid: document.getElementById("stations-grid"),
  requestsFeed: document.getElementById("requests-feed"),
  logsList: document.getElementById("logs-list"),
  stationCount: document.getElementById("station-count"),
  statTotal: document.getElementById("stat-total"),
  statSuccess: document.getElementById("stat-success"),
  statFailed: document.getElementById("stat-failed"),
  connectionStatus: document.getElementById("connection-status"),
  lastUpdated: document.getElementById("last-updated"),
  startButton: document.getElementById("start-simulation-button"),
  iterationsInput: document.getElementById("iterations-input"),
  stationTemplate: document.getElementById("station-card-template"),
  feedTemplate: document.getElementById("feed-item-template"),
  logTemplate: document.getElementById("log-item-template"),
};

async function fetchDashboardData() {
  const endpoints = state.endpoint ? [state.endpoint] : DASHBOARD_ENDPOINTS;

  for (const endpoint of endpoints) {
    try {
      const response = await fetch(endpoint, { cache: "no-store" });
      if (!response.ok) {
        continue;
      }

      const payload = await response.json();
      state.endpoint = endpoint;
      return payload;
    } catch (error) {
      continue;
    }
  }

  throw new Error("Unable to reach dashboard endpoint.");
}

function normalizePayload(payload) {
  return {
    stations: Array.isArray(payload.stations) ? payload.stations : [],
    requests: Array.isArray(payload.requests) ? payload.requests : [],
    logs: Array.isArray(payload.logs) ? payload.logs : [],
    stats: {
      totalRequests: payload.stats?.totalRequests ?? 0,
      success: payload.stats?.success ?? 0,
      failed: payload.stats?.failed ?? 0,
    },
    simulation: {
      running: Boolean(payload.simulation?.running),
    },
  };
}

function formatTime(value) {
  if (!value) {
    return "--";
  }

  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return value;
  }

  return date.toLocaleTimeString([], {
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
  });
}

function setConnectionStatus(statusText, connected) {
  els.connectionStatus.textContent = statusText;
  els.connectionStatus.parentElement.style.borderColor = connected
    ? "rgba(56, 189, 248, 0.16)"
    : "rgba(239, 68, 68, 0.24)";
  els.connectionStatus.previousElementSibling.style.background = connected ? "#38bdf8" : "#ef4444";
  els.connectionStatus.previousElementSibling.style.boxShadow = connected
    ? "0 0 0 6px rgba(56, 189, 248, 0.15)"
    : "0 0 0 6px rgba(239, 68, 68, 0.15)";
}

function renderEmptyState(message) {
  const empty = document.createElement("div");
  empty.className = "empty-state";
  empty.textContent = message;
  return empty;
}

function renderStations(stations) {
  const fragment = document.createDocumentFragment();

  if (!stations.length) {
    fragment.appendChild(renderEmptyState("No station data available yet."));
  }

  for (const station of stations) {
    const node = els.stationTemplate.content.firstElementChild.cloneNode(true);
    const slotBadges = node.querySelector(".slot-badges");
    const stationId = station.station_id ?? "--";
    const slots = Array.isArray(station.slots) ? station.slots : [];
    const availableCount = slots.filter((slot) => String(slot.status).toLowerCase() === "available").length;

    node.querySelector(".station-title").textContent = `Station ${stationId}`;
    node.querySelector(".station-summary").textContent = `${availableCount}/${slots.length} available`;

    if (!slots.length) {
      slotBadges.appendChild(renderEmptyState("No slots"));
    }

    for (const slot of slots) {
      const badge = document.createElement("span");
      const status = String(slot.status ?? "pending").toLowerCase();
      const slotId = slot.slot_id ?? "--";

      badge.className = `slot-badge ${status === "available" ? "available" : status === "occupied" ? "occupied" : "pending"}`;
      badge.textContent = `Slot ${slotId} - ${status}`;
      slotBadges.appendChild(badge);
    }

    fragment.appendChild(node);
  }

  els.stationsGrid.replaceChildren(fragment);
  els.stationCount.textContent = `${stations.length} Station${stations.length === 1 ? "" : "s"}`;
}

function renderRequests(requests) {
  const fragment = document.createDocumentFragment();

  if (!requests.length) {
    fragment.appendChild(renderEmptyState("No live requests yet."));
  }

  for (const request of requests) {
    const node = els.feedTemplate.content.firstElementChild.cloneNode(true);
    const statusNode = node.querySelector(".feed-status");
    const statusLabel = request.success ? "SUCCESS" : "FAILED";

    node.querySelector(".feed-main").textContent =
      `Vehicle ${request.vehicle_id} -> Station ${request.station_id} -> ${statusLabel}`;
    statusNode.textContent = statusLabel;
    statusNode.classList.add(request.success ? "success" : "failure");
    fragment.appendChild(node);
  }

  els.requestsFeed.replaceChildren(fragment);
}

function renderLogs(logs) {
  const fragment = document.createDocumentFragment();

  if (!logs.length) {
    fragment.appendChild(renderEmptyState("No logs available."));
  }

  for (const entry of logs) {
    const node = els.logTemplate.content.firstElementChild.cloneNode(true);
    node.querySelector(".log-time").textContent = formatTime(entry.timestamp);
    node.querySelector(".log-message").textContent = entry.message;
    fragment.appendChild(node);
  }

  els.logsList.replaceChildren(fragment);
}

function renderStats(stats) {
  els.statTotal.textContent = stats.totalRequests;
  els.statSuccess.textContent = stats.success;
  els.statFailed.textContent = stats.failed;
}

function renderDashboard(payload) {
  const normalized = normalizePayload(payload);
  renderStations(normalized.stations);
  renderRequests(normalized.requests);
  renderLogs(normalized.logs);
  renderStats(normalized.stats);
  els.startButton.disabled = normalized.simulation.running;
  els.startButton.textContent = normalized.simulation.running ? "Simulation Running" : "Start Simulation";
  els.lastUpdated.textContent = `Last updated ${new Date().toLocaleTimeString()}`;
}

async function refreshDashboard() {
  try {
    const payload = await fetchDashboardData();
    const nextHash = JSON.stringify(payload);

    if (nextHash !== state.payloadHash) {
      renderDashboard(payload);
      state.payloadHash = nextHash;
    } else {
      els.lastUpdated.textContent = `Last checked ${new Date().toLocaleTimeString()}`;
    }

    setConnectionStatus("Live", true);
  } catch (error) {
    if (!state.payloadHash) {
      els.stationsGrid.replaceChildren(renderEmptyState("Waiting for dashboard data..."));
      els.requestsFeed.replaceChildren(renderEmptyState("Feed unavailable."));
      els.logsList.replaceChildren(renderEmptyState("Logs unavailable."));
    }

    setConnectionStatus("Disconnected", false);
    els.lastUpdated.textContent = "Retrying connection...";
  }
}

async function startSimulation() {
  const iterations = Number(els.iterationsInput.value || 12);
  els.startButton.disabled = true;
  els.startButton.textContent = "Starting...";

  try {
    const response = await fetch(START_SIMULATION_ENDPOINT, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify({ iterations }),
    });

    const payload = await response.json();
    if (!response.ok || !payload.started) {
      throw new Error(payload.message || "Unable to start simulation.");
    }

    els.lastUpdated.textContent = payload.message;
    await refreshDashboard();
  } catch (error) {
    els.startButton.disabled = false;
    els.startButton.textContent = "Start Simulation";
    els.lastUpdated.textContent = error.message;
  }
}

els.startButton.addEventListener("click", startSimulation);

refreshDashboard();
setInterval(refreshDashboard, POLL_INTERVAL_MS);
