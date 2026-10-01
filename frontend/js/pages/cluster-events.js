/* 集群事件时间线 Cluster events */
Components.init('cluster-events');
const C = Components;

const KIND_META = {
  node_registered: { label: '节点注册 Registered', cls: 'good', level: 'INFO' },
  node_reregistered: { label: '节点重注册 Reregistered', cls: 'aqua', level: 'INFO' },
  node_lost: { label: '节点失联 Lost', cls: 'warn', level: 'WARN' },
  node_reclaimed: { label: '节点回收 Reclaimed', cls: 'bad', level: 'WARN' },
  task_reassigned: { label: '任务改派 Reassigned', cls: 'warn', level: 'WARN' },
};

let poller = C.poll(render, 2500);

function fmtDateTime(ms) {
  if (!ms) return '-';
  const d = new Date(ms);
  return d.toLocaleString('zh-CN', { hour12: false });
}

function kindBadge(kind) {
  const meta = KIND_META[kind] || { label: kind, cls: 'muted' };
  return `<span class="badge ${meta.cls}">${C.esc(meta.label)}</span>`;
}

function kindOptions(initialOnly) {
  return Object.entries(KIND_META).map(([value, meta]) =>
    `<option value="${value}">${C.esc(meta.label)}</option>`
  ).join('');
}

async function render() {
  const params = new URLSearchParams();
  const search = document.getElementById('search').value.trim();
  const kind = document.getElementById('kind').value;
  const workerId = document.getElementById('worker_id').value.trim();
  const jobId = document.getElementById('job_id').value.trim();
  if (search) params.set('q', search);
  if (kind) params.set('kind', kind);
  if (workerId) params.set('worker_id', workerId);
  if (jobId) params.set('job_id', jobId);
  params.set('limit', '2000');

  let d;
  try {
    d = await API.get('/api/cluster/events?' + params.toString());
  } catch (e) {
    document.getElementById('events').innerHTML = C.empty('无法加载集群事件 Cannot load cluster events');
    return;
  }

  const kindSel = document.getElementById('kind');
  if (!kindSel.options.length || kindSel.options.length === 1) {
    kindSel.insertAdjacentHTML('beforeend', kindOptions());
    kindSel.value = kind;
  }

  const events = d.events || [];
  const byKind = {};
  events.forEach(e => { byKind[e.kind] = (byKind[e.kind] || 0) + 1; });
  const stats = [
    ['事件总数 Total', events.length, ''],
    ['节点注册 Registered', (byKind.node_registered || 0) + (byKind.node_reregistered || 0), 'good'],
    ['节点失联 Lost', byKind.node_lost || 0, 'warn'],
    ['节点回收 Reclaimed', byKind.node_reclaimed || 0, 'bad'],
    ['任务改派 Reassigned', byKind.task_reassigned || 0, 'warn'],
  ];
  document.getElementById('stats').innerHTML = stats.map(([label, value, cls]) =>
    `<div class="stat"><div class="label">${label}</div><div class="value ${cls}">${C.fmtNum(value)}</div></div>`
  ).join('');

  document.getElementById('event-count').textContent =
    `${events.length} 条 events · 最新在前 newest first（事件本身按时间记录）`;

  // Display newest first for post-incident review while preserving each row's
  // exact created_ms. The API returns chronological order, so reverse here.
  const rows = events.slice().reverse();
  document.getElementById('events').innerHTML = rows.length ? C.table([
    {
      key: 'created_ms',
      label: '发生时间 Time',
      render: r => `<span class="tabular">${C.esc(fmtDateTime(r.created_ms))}</span>`,
      width: '180px',
    },
    { key: 'kind', label: '类型 Type', render: r => kindBadge(r.kind), width: '170px' },
    { key: 'message', label: '事件 Event', render: r => `<div>${C.esc(r.message)}</div>${detailHtml(r)}` },
    {
      key: 'worker_id',
      label: '节点 Worker',
      render: r => r.worker_id ? `<span class="mono small">${C.esc(r.worker_id)}</span>` : '-',
      width: '170px',
    },
    {
      key: 'task_id',
      label: '作业 / 任务 Job / Task',
      render: r => r.job_id || r.task_id
        ? `<div class="mono small">${r.job_id ? C.esc(r.job_id) : '-'}</div><div class="mono small muted">${r.task_id ? C.esc(r.task_id) : ''}</div>`
        : '-',
      width: '210px',
    },
  ], rows) : C.empty('暂无集群事件 No cluster events yet');
}

function detailHtml(event) {
  const detail = event.detail || {};
  const entries = Object.entries(detail).filter(([, v]) => v !== null && v !== '');
  if (!entries.length) return '';
  const text = entries.map(([k, v]) => `${k}=${typeof v === 'object' ? JSON.stringify(v) : v}`).join(' · ');
  return `<div class="small muted mono" style="margin-top:3px">${C.esc(text)}</div>`;
}

function debounce(fn, ms) {
  let t;
  return () => { clearTimeout(t); t = setTimeout(fn, ms); };
}

document.getElementById('refresh').addEventListener('click', render);
['search', 'worker_id', 'job_id'].forEach(id =>
  document.getElementById(id).addEventListener('input', debounce(render, 350))
);
document.getElementById('kind').addEventListener('change', render);
document.getElementById('auto').addEventListener('change', (e) => {
  poller.stop();
  if (e.target.checked) {
    poller = C.poll(render, 2500);
    poller.start();
  }
});
poller.start();
