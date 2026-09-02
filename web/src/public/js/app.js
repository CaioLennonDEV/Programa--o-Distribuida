/**
 * AgroSense Mesh – Dashboard Frontend
 * Conecta via WebSocket ao backend Fastify e atualiza a UI em tempo real.
 */

// ─── Conexão WebSocket ───────────────────────────────────────────────────────
const WS_URL    = `ws://${location.host}/ws`;
const dot       = document.getElementById('ws-dot');
const wsLabel   = document.getElementById('ws-label');
const lamportEl = document.getElementById('lamport-terminal');

let ws = null;
let reconnectTimer = null;

function connect() {
  ws = new WebSocket(WS_URL);

  ws.onopen = () => {
    dot.classList.add('connected');
    wsLabel.textContent = 'Conectado';
    addLamportEntry('sistema', 'WebSocket conectado', '✓');
    if (reconnectTimer) { clearTimeout(reconnectTimer); reconnectTimer = null; }
  };

  ws.onmessage = (event) => {
    try {
      const msg = JSON.parse(event.data);
      handleMessage(msg);
    } catch (e) {
      console.error('WS parse error:', e);
    }
  };

  ws.onclose = () => {
    dot.classList.remove('connected');
    wsLabel.textContent = 'Desconectado – reconectando...';
    reconnectTimer = setTimeout(connect, 3000);
  };

  ws.onerror = (err) => {
    console.error('WS error:', err);
  };
}

// ─── Roteamento de Eventos ───────────────────────────────────────────────────
function handleMessage(msg) {
  switch (msg.event) {
    case 'worker-status':
      updateWorkerCards(msg.data);
      break;
    case 'irrigation-logs':
      renderIrrigationTable(msg.data);
      break;
  }
}

// ─── Atualização dos Cards de Workers ────────────────────────────────────────
function updateWorkerCards(workers) {
  // Reseta todos os cards primeiro
  for (let i = 1; i <= 3; i++) {
    const card   = document.getElementById(`worker-card-${i}`);
    const badge  = document.getElementById(`badge-${i}`);
    const crown  = document.getElementById(`crown-${i}`);
    if (card)  { card.className = 'worker-card'; }
    if (badge) { badge.className = 'status-badge'; badge.textContent = 'OFFLINE'; }
    if (crown) { crown.style.display = 'none'; }
  }

  if (!Array.isArray(workers)) return;

  workers.forEach(w => {
    const id     = w.node_id ? w.node_id.replace(/\D/g, '') : null;
    if (!id) return;

    const card      = document.getElementById(`worker-card-${id}`);
    const badge     = document.getElementById(`badge-${id}`);
    const crown     = document.getElementById(`crown-${id}`);
    const statusTxt = document.getElementById(`status-text-${id}`);
    const lamport   = document.getElementById(`lamport-${id}`);

    if (!card) return;

    const status = (w.status ?? 'ACTIVE').toUpperCase();

    card.className = 'worker-card ' + status.toLowerCase();
    badge.className = 'status-badge ' + status.toLowerCase();
    badge.textContent = status;

    if (statusTxt) statusTxt.textContent = status;
    if (lamport)   lamport.textContent   = w.lamport ?? '—';
    if (crown)     crown.style.display   = status === 'LEADER' ? 'inline' : 'none';

    // Adiciona evento de Lamport no terminal
    addLamportEntry(
      `worker-${id}`,
      `status → ${status}`,
      `L=${w.lamport ?? 0}`
    );
  });
}

// ─── Terminal de Logs de Lamport ─────────────────────────────────────────────
function addLamportEntry(source, event, value) {
  const now  = new Date().toLocaleTimeString('pt-BR', { hour12: false });
  const entry = document.createElement('div');
  entry.className = 'log-entry';
  entry.innerHTML =
    `<span class="ts">[${now}] ${source}</span> ` +
    `<span class="event">${event}</span> ` +
    `<span class="value">${value}</span>`;

  lamportEl.insertBefore(entry, lamportEl.firstChild);

  // Mantém somente os últimos 80 logs
  while (lamportEl.children.length > 80) {
    lamportEl.removeChild(lamportEl.lastChild);
  }
}

// ─── Tabela de Irrigação ─────────────────────────────────────────────────────
function renderIrrigationTable(logs) {
  const container = document.getElementById('irrigation-container');
  if (!container) return;

  if (!logs || logs.length === 0) {
    container.innerHTML = `
      <div class="empty-state">
        <span class="icon">🌱</span>
        Nenhuma ativação de irrigação registrada ainda.
      </div>`;
    return;
  }

  const rows = logs.map(row => {
    const moisture    = parseFloat(row.moisture ?? 0).toFixed(1);
    const temp        = parseFloat(row.temperature ?? 0).toFixed(1);
    const moistureClass = parseFloat(moisture) < 40 ? 'moisture-low' : 'moisture-ok';
    const ts          = row.activated_at
      ? new Date(row.activated_at).toLocaleString('pt-BR')
      : '—';

    return `
      <tr>
        <td>${row.sensor_id ?? '—'}</td>
        <td><span class="zone-badge">${row.zone ?? '—'}</span></td>
        <td class="${moistureClass}">${moisture}%</td>
        <td>${temp}°C</td>
        <td>${row.lamport_time ?? '—'}</td>
        <td>Worker-${row.worker_id ?? '—'}</td>
        <td>${ts}</td>
      </tr>`;
  }).join('');

  container.innerHTML = `
    <table class="irr-table" aria-label="Logs de irrigação">
      <thead>
        <tr>
          <th>Sensor</th>
          <th>Zona</th>
          <th>Umidade</th>
          <th>Temp.</th>
          <th>Lamport</th>
          <th>Worker Líder</th>
          <th>Ativado em</th>
        </tr>
      </thead>
      <tbody>${rows}</tbody>
    </table>`;

  // Log no terminal de Lamport
  const latest = logs[0];
  if (latest) {
    addLamportEntry(
      `worker-${latest.worker_id}`,
      `irrigação ativada zona=${latest.zone}`,
      `L=${latest.lamport_time}`
    );
  }
}

// ─── Polling de logs de irrigação via REST (fallback se WS não enviar) ───────
async function pollIrrigation() {
  try {
    const res  = await fetch('/api/irrigation-logs');
    if (!res.ok) return;
    const data = await res.json();
    renderIrrigationTable(data);
  } catch { /* silencia erros de rede */ }
}

// ─── Start ───────────────────────────────────────────────────────────────────
connect();
setInterval(pollIrrigation, 5000);
