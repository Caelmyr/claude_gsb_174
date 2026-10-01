/* 集群事件 Cluster Events — node lifecycle & reassignment timeline */
Components.init('events');
const C = Components;

const KIND_LABEL = {
  worker_registered: '节点注册 Registered',
  worker_dead: '节点失联/回收 Dead',
  worker_recovered: '节点恢复 Recovered',
  task_reassigned: '任务改派 Reassigned',
};
const KIND_CLASS = {
  worker_registered: 'good',
  worker_dead: 'bad',
  worker_recovered: 'aqua',
  task_reassigned: 'warn',
};

let kindLabels = {};

function fmtDateTime(ms) {
  if (!ms) return '-';
  return new Date(ms).toLocaleString('zh-CN', { hour12: false });
}

function kindBadge(kind) {
  const cls = KIND_CLASS[kind] || 'muted';
  const label = kindLabels[kind] || KIND_LABEL[kind] || kind;
  return `<span class="badge ${cls}">${C.esc(label)}</span>`;
}

function syncKindOptions(kinds) {
  const sel = document.getElementById('kind');
  const current = sel.value;
  const known = Array.from(new Set([...(kinds || []), ...Object.keys(KIND_LABEL)])).sort();
  const wanted = [''].concat(known);
  const existing = Array.from(sel.options).map(o => o.value);
  if (JSON.stringify(wanted) === JSON.stringify(existing)) return;
  sel.innerHTML = '<option value="">类型 Kind: 全部 All</option>' +
    known.map(k => `<option value="${C.esc(k)}">${C.esc(kindLabels[k] || KIND_LABEL[k] || k)}</option>`).join('');
  sel.value = current;
}

async function render() {
  const params = new URLSearchParams();
  const kind = document.getElementById('kind').value;
  const level = document.getElementById('level').value;
  const worker = document.getElementById('worker').value.trim();
  const search = document.getElementById('search').value.trim();
  if (kind) params.set('kind', kind);
  if (level) params.set('level', level);
  if (worker) params.set('worker', worker);
  if (search) params.set('q', search);
  params.set('limit', '1000');

  let d;
  try { d = await API.get('/api/cluster/events?' + params.toString()); } catch (e) { return; }
  kindLabels = d.kind_labels || {};
  syncKindOptions(d.kinds);
  const events = d.records || [];

  const byKind = {};
  events.forEach(e => byKind[e.kind] = (byKind[e.kind] || 0) + 1);
  document.getElementById('stats').innerHTML = [
    { label: '事件总数 Total', value: d.total },
    { label: '节点注册 Registered', value: byKind.worker_registered || 0, cls: 'good' },
    { label: '失联/回收 Dead', value: byKind.worker_dead || 0, cls: byKind.worker_dead ? 'bad' : '' },
    { label: '节点恢复 Recovered', value: byKind.worker_recovered || 0 },
    { label: '任务改派 Reassigned', value: byKind.task_reassigned || 0, cls: byKind.task_reassigned ? 'bad' : '' },
  ].map(s => `<div class="stat"><div class="label">${s.label}</div><div class="value ${s.cls || ''}">${C.fmtNum(s.value)}</div></div>`).join('');

  document.getElementById('event-count').textContent =
    `匹配 ${events.length} / ${d.total} 条 · 共扫描 ${d.scanned} 条`;

  document.getElementById('events').innerHTML = events.length ? C.table([
    { key: 'ts_ms', label: '时间 Time', render: r => `<span class="mono small">${fmtDateTime(r.ts_ms)}</span>`, width: '170px' },
    { key: 'kind', label: '类型 Kind', render: r => kindBadge(r.kind) },
    { key: 'level', label: '级别 Level', render: r => `<span class="small lvl-${C.esc(r.level)}">${C.esc(r.level)}</span>` },
    { key: 'worker', label: '节点 Worker', render: r => r.worker_id
        ? `<b>${C.esc(r.worker_name || r.worker_id)}</b><div class="small muted mono">${C.esc(r.worker_id)}</div>` : '-' },
    { key: 'message', label: '事件 Event', render: r => C.esc(r.message) +
        (r.job_id ? `<div class="small muted mono">job ${C.esc(r.job_id)}${r.task_id ? ' · task ' + C.esc(r.task_id) : ''}</div>` : '') },
  ], events) : C.empty('暂无集群事件 No cluster events yet');
}

document.getElementById('refresh').addEventListener('click', render);
document.getElementById('kind').addEventListener('change', render);
document.getElementById('level').addEventListener('change', render);
document.getElementById('worker').addEventListener('input', debounce(render, 400));
document.getElementById('search').addEventListener('input', debounce(render, 400));

function debounce(fn, ms) {
  let t; return () => { clearTimeout(t); t = setTimeout(fn, ms); };
}

C.poll(render, 2500).start();
render();
