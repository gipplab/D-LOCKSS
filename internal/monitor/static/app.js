'use strict';

// The dashboard polls /api/summary while the tab is visible and fetches
// lists and details on demand. Views live in the URL hash (#file/<cid>,
// #node/<id>, #key/<peer ID>, #search/<text>, ...), so links can be shared
// and the browser's back button works.

const POLL_MS = 5000;
const STALE_AFTER_S = 30;
const PAGE = 50;

const $ = (id) => document.getElementById(id);
const enc = encodeURIComponent;

// h builds an element; attributes starting with "on" are event handlers.
function h(tag, attrs, ...children) {
  const el = document.createElement(tag);
  for (const [k, v] of Object.entries(attrs || {})) {
    if (v == null || v === false) continue;
    if (k === 'class') el.className = v;
    else if (k.startsWith('on')) el.addEventListener(k.slice(2), v);
    else el.setAttribute(k, v === true ? '' : v);
  }
  for (const c of children.flat(Infinity)) {
    if (c == null || c === false) continue;
    el.append(c instanceof Node ? c : document.createTextNode(String(c)));
  }
  return el;
}

const fmt = (n) => Number(n || 0).toLocaleString('en-US');
const nowS = () => Date.now() / 1000;
const plural = (n, one, many) => fmt(n) + ' ' + (n === 1 ? one : many);
const short = (s) => (s && s.length > 22 ? s.slice(0, 10) + '...' + s.slice(-6) : s || '');
const shardLabel = (s) => (s === '' ? 'root' : s);

function ago(sec) {
  sec = Math.max(0, Math.round(sec));
  if (sec < 60) return sec + ' s';
  if (sec < 3600) return Math.round(sec / 60) + ' min';
  if (sec < 86400) return Math.round(sec / 3600) + ' h';
  return Math.round(sec / 86400) + ' d';
}

function duration(sec) {
  const d = Math.floor(sec / 86400), hr = Math.floor((sec % 86400) / 3600), m = Math.floor((sec % 3600) / 60);
  if (d) return d + 'd ' + hr + 'h';
  if (hr) return hr + 'h ' + m + 'm';
  return m + 'm';
}

async function getJSON(url, options) {
  let r;
  try {
    r = await fetch(url, options);
  } catch (e) {
    throw new Error('The monitor cannot be reached.');
  }
  const body = await r.json().catch(() => ({}));
  if (!r.ok) throw new Error(body.error || r.statusText);
  return body;
}

function toast(msg) {
  const t = $('toast');
  t.textContent = msg;
  t.classList.add('show');
  setTimeout(() => t.classList.remove('show'), 1200);
}

function copyButton(text) {
  return h('button', { type: 'button', onclick: () => navigator.clipboard.writeText(text).then(() => toast('Copied')) }, 'Copy');
}

// copyable shows an ID with a Copy button and any further actions.
const copyable = (id, ...actions) => [h('span', { class: 'cid' }, id), h('span', { class: 'actions' }, copyButton(id), ...actions)];
const joined = (items) => items.map((item, i) => [i ? ', ' : '', item]);
const nodeByID = (id) => summary && summary.nodes.find((n) => n.id === id);
const nodeName = (id) => (nodeByID(id) || {}).name || short(id);
// nodeLink links a node the monitor knows; any other ID, such as an earlier
// identity of a node, is shown as plain text.
const nodeLink = (id, name) => (nodeByID(id)
  ? h('a', { href: '#node/' + enc(id) }, name || short(id))
  : h('span', { class: 'muted', title: id + ': not a node the monitor knows' }, short(id)));
const fieldChip = (field, sub, count) => h('a', { class: 'chip ' + (sub ? 'sub' : 'field'), href: '#field/' + enc(field) + (sub ? '/' + enc(sub) : '') },
  sub || field, count == null ? null : h('span', { class: 'n' }, fmt(count)));

const badge = (status) => h('span', { class: 'badge ' + status }, status.replace('_', ' '));

const notIndexed = { pending: 'not indexed yet', skipped: 'cannot be indexed', failed: 'indexing failed' };
// A file without an indexer title is named by its CID.
const fileTitle = (f) => f.title || h('span', { class: 'cid' }, f.cid);
const indexNote = (f) => (f.title ? null : h('span', { class: 'muted' }, notIndexed[f.index] || notIndexed.pending));

let summary = null;
let lastOK = 0;
let treeFor = '';

async function poll() {
  if (document.hidden) return;
  try {
    summary = await getJSON('/api/summary');
    lastOK = Date.now();
    renderSummary(summary);
  } catch (e) {
    // renderLive shows how old the data is.
  }
  renderLive();
}

// renderLive colors the status dot; the age of the data is in its tooltip.
// Screen readers hear only when the state changes.
function renderLive() {
  const el = $('live');
  if (!lastOK) return;
  const age = (Date.now() - lastOK) / 1000;
  const stale = age > STALE_AFTER_S;
  el.title = stale ? 'No data for ' + ago(age) + ', showing the last view' : 'Live, updated ' + ago(age) + ' ago';
  const state = stale ? 'stale' : 'ok';
  if (!el.classList.contains(state)) {
    el.className = 'live ' + state;
    el.firstElementChild.textContent = stale ? 'No new data, showing the last view' : 'Live';
  }
  document.body.classList.toggle('stale', stale);
}

function renderSummary(s) {
  $('topic').textContent = 'Topic ' + s.topic + ', prefix ' + s.prefix + ', ' + plural(s.shards, 'shard', 'shards');
  renderHealth(s);
  renderNodes(s);
  renderHistogram(s);
  renderIndexLine(s);
  const treeKey = s.shards + ':' + s.nodes.length;
  if ($('internals').open && treeKey !== treeFor) loadTree();
  treeFor = treeKey;
  $('warming').hidden = !s.warming_up;
}

function renderHealth(s) {
  const f = s.files;
  const count = (cls, n, label, route) => h('a', { class: 'count ' + cls, href: '#' + route }, h('b', {}, fmt(n)), ' ' + label);
  $('health-line').replaceChildren(
    h('span', {}, h('span', { class: 'big' }, fmt(f.total)), ' ', f.total === 1 ? 'file' : 'files'),
    count('ok', f.at_target, 'at target (' + plural(s.target, 'copy', 'copies') + ')', 'list/at_target'),
    count('warn', f.below, 'below target', 'list/below'),
    count('bad', f.critical, 'critical (one copy or none)', 'list/critical'),
  );
  const bar = $('health-bar');
  const pct = (n) => (f.total ? (n / f.total) * 100 : 0) + '%';
  bar.replaceChildren(
    h('span', { class: 'ok', style: 'width:' + pct(f.at_target) }),
    h('span', { class: 'warn', style: 'width:' + pct(f.below) }),
    h('span', { class: 'bad', style: 'width:' + pct(f.critical) }),
  );
  bar.setAttribute('aria-label', fmt(f.at_target) + ' at target, ' + fmt(f.below) + ' below, ' + fmt(f.critical) + ' critical');

  const live = s.nodes.filter((n) => n.status !== 'offline');
  const parts = [h('span', {}, h('b', {}, fmt(live.length)), ' ', live.length === 1 ? 'node' : 'nodes')];
  for (const status of ['online', 'full', 'behind', 'silent', 'offline']) {
    const group = s.nodes.filter((n) => n.status === status);
    if (!group.length) continue;
    const names = status === 'online' || status === 'full' ? null : [': ', joined(group.map((n) => [
      nodeLink(n.id, n.name),
      status === 'behind' ? ' (' + fmt(n.held) + ' of ' + fmt(n.expected) + ')' : '',
    ]))];
    parts.push(h('span', {}, badge(status), ' ', fmt(group.length), names));
  }
  $('nodes-line').replaceChildren(...parts);
}

function renderNodes(s) {
  const rows = s.nodes.map((n) => {
    const offline = n.status === 'offline';
    const ratio = n.expected ? Math.min(1, n.held / n.expected) : 1;
    return h('tr', {},
      h('td', {}, nodeLink(n.id, n.name), h('div', { class: 'id' }, short(n.id))),
      h('td', {}, badge(n.status)),
      h('td', {}, offline ? '-' : h('div', { class: 'held' },
        fmt(n.held) + ' / ' + fmt(n.expected),
        h('div', { class: 'mini' + (n.status === 'behind' ? ' warn' : '') }, h('span', { style: 'width:' + ratio * 100 + '%' })))),
      h('td', { class: 'shard-col' }, shardLabel(n.shard)),
      h('td', {}, ago(nowS() - n.last_seen) + ' ago'),
      h('td', {}, offline ? '-' : duration(nowS() - n.first_seen)),
    );
  });
  $('node-rows').replaceChildren(...rows);
  $('node-rows').closest('table').classList.toggle('hide-shard', s.shards <= 1);
}

function renderHistogram(s) {
  const d = s.files.distribution;
  const max = Math.max(1, ...d);
  $('histogram').replaceChildren(...d.map((n, i) => {
    const cls = i >= s.target ? 'seg-ok' : i <= 1 ? 'seg-bad' : 'seg-warn';
    const label = i === d.length - 1 ? i + '+' : String(i);
    return h('a', { class: 'col', href: '#level/' + i, title: plural(n, 'file', 'files') + ' with ' + label + ' copies' },
      h('em', {}, n ? fmt(n) : ''),
      h('span', { class: cls, style: 'height:' + Math.round((n / max) * 100) + '%' }),
      h('small', {}, label));
  }));
}

// The collection is fetched when Browse opens and for the subtopics of a
// field view, again only if more files were indexed since. It is not redrawn
// while open, so the chips do not change under the mouse.
let collectionFor = -1;
let collection = [];

function renderIndexLine(s) {
  const i = s.index;
  $('index-line').textContent = !i.enabled
    ? 'The indexer is off: no LLM API key is set.'
    : 'Indexed ' + fmt(i.indexed) + ' of ' + plural(s.files.total, 'file', 'files') + ': ' + fmt(i.pending) + ' pending, ' +
      fmt(i.skipped) + ' cannot be indexed, ' + fmt(i.failed) + ' failed. ' +
      fmt(i.daily_remaining) + ' of ' + fmt(i.daily_limit) + ' LLM requests left today.';
}

async function loadCollection() {
  if (!summary || summary.index.indexed === collectionFor) return;
  collectionFor = summary.index.indexed;
  try {
    collection = (await getJSON('/api/collection')).fields || [];
  } catch (e) {
    return;
  }
  $('fields').replaceChildren(...(collection.length
    ? collection.map((f) => fieldChip(f.name, null, f.count))
    : [h('span', { class: 'muted small' }, 'No file is indexed yet.')]));
}

function showView(title, ...content) {
  const view = $('view');
  const first = view.hidden;
  view.hidden = false;
  $('view-title').replaceChildren(title);
  $('view-body').replaceChildren(...content.flat().filter((c) => c != null && c !== false));
  if (first) view.scrollIntoView({ behavior: 'smooth', block: 'start' });
}

function closeView() {
  history.pushState(null, '', location.pathname + location.search);
  $('view').hidden = true;
}

function go(route) {
  if (location.hash === '#' + route) showRoute();
  else location.hash = route;
}

function showRoute() {
  const raw = location.hash.slice(1);
  if (!raw) {
    $('view').hidden = true;
    return;
  }
  const [kind, ...args] = raw.split('/').map(decodeURIComponent);
  const arg = args[0] || '';
  switch (kind) {
    case 'file': return showFile(arg);
    case 'node': return showNode(arg);
    case 'key': return showList({ key: arg }, 'Files signed by ' + short(arg), keyHeader(arg));
    case 'search': return showList({ q: arg }, 'Files about "' + arg + '"');
    case 'list': return showList({ health: arg }, { at_target: 'Files at target', below: 'Files below target', critical: 'Critical files' }[arg] || 'Files');
    case 'level': return showList({ level: arg }, 'Files with ' + (arg === '10' ? '10 or more' : arg) + (arg === '1' ? ' copy' : ' copies'));
    case 'field': return loadCollection().then(() =>
      showList({ field: arg, sub: args[1] }, args[1] ? arg + ' > ' + args[1] : arg, fieldHeader(arg, args[1])));
    case 'holding': return showList({ node: arg }, 'Files on ' + nodeName(arg));
    case 'missing': return showList({ missing: arg }, 'Files ' + nodeName(arg) + ' lacks');
    default: return showView('Not found', h('p', {}, 'There is nothing at this address.'));
  }
}

function keyHeader(key) {
  return h('p', { class: 'small' }, 'Key ', copyable(key));
}

function fieldHeader(field, sub) {
  if (sub) return h('p', { class: 'small' }, h('a', { href: '#field/' + enc(field) }, 'All of ' + field));
  const f = collection.find((x) => x.name.toLowerCase() === field.toLowerCase());
  if (!f || !f.subs || !f.subs.length) return null;
  return h('div', { class: 'chips' }, f.subs.map((s) => fieldChip(field, s.name, s.count)));
}

async function showList(params, title, header, offset = 0) {
  const qs = new URLSearchParams();
  for (const [k, v] of Object.entries(params)) if (v) qs.set(k, v);
  qs.set('offset', offset);
  qs.set('limit', PAGE);
  showView(title, header, h('p', { class: 'muted' }, 'Loading...'));
  let d;
  try {
    d = await getJSON('/api/files?' + qs);
  } catch (e) {
    return showView(title, header, h('p', { class: 'error' }, e.message));
  }
  const pager = d.total > d.limit ? h('div', { class: 'pager' },
    h('span', { class: 'muted small' }, fmt(d.offset + 1) + '-' + fmt(Math.min(d.offset + d.limit, d.total)) + ' of ' + fmt(d.total)),
    h('span', { class: 'buttons' },
      h('button', { type: 'button', disabled: d.offset === 0, onclick: () => showList(params, title, header, Math.max(0, d.offset - PAGE)) }, 'Previous'),
      h('button', { type: 'button', disabled: d.offset + d.limit >= d.total, onclick: () => showList(params, title, header, d.offset + PAGE) }, 'Next'))) : null;
  showView(title, header,
    h('p', { class: 'muted small' }, plural(d.total, 'file', 'files')),
    d.files.length ? h('ul', { class: 'rows' }, d.files.map(fileRow)) : h('p', { class: 'muted' }, 'No files.'),
    pager);
}

function classification(f) {
  return [f.field && fieldChip(f.field), f.sub && fieldChip(f.field || '', f.sub)];
}

function fileRow(f) {
  return h('li', {},
    h('div', { class: 'main' },
      h('a', { class: 'title', href: '#file/' + enc(f.cid) }, fileTitle(f)),
      h('div', { class: 'meta' },
        classification(f),
        indexNote(f),
        f.uploaders ? h('span', {}, plural(f.uploaders, 'uploader', 'uploaders')) : null,
        f.title ? h('span', { class: 'cid' }, f.cid) : null)),
    h('div', { class: 'copies' }, h('b', {}, f.copies + ' / ' + f.target), ' copies', h('div', {}, badge(f.health))));
}

const fact = (label, ...value) => [h('dt', {}, label), h('dd', {}, ...value)];

async function showFile(cid) {
  showView('File', h('p', { class: 'muted' }, 'Loading...'));
  let f;
  try {
    f = await getJSON('/api/file?cid=' + enc(cid));
  } catch (e) {
    return showView('File', h('p', { class: 'error' }, e.message));
  }
  const holders = f.holder_list.length ? joined(f.holder_list.map((n) => nodeLink(n.id, n.name))) : 'no node';
  const uploaders = f.uploader_keys.length ? joined(f.uploader_keys.map((k) => h('a', { href: '#key/' + enc(k) }, short(k)))) : 'none verified';
  const details = h('div', { id: 'file-details', hidden: true },
    h('dl', { class: 'facts' },
      fact('Copies', f.copies + ' of ' + f.target + ' ', badge(f.health)),
      fact('Shard', shardLabel(f.shard)),
      fact('File CID', f.payload ? copyable(f.payload) : 'not known yet'),
      fact('Held by', holders),
      fact('Uploaders', uploaders),
      f.indexed_at ? fact('Indexed', new Date(f.indexed_at * 1000).toISOString().slice(0, 10)) : null),
    h('h3', {}, f.manifest_list.length === 1 ? 'Manifest' : 'Manifests'),
    h('div', { class: 'table-wrap' }, h('table', {},
      h('thead', {}, h('tr', {}, h('th', {}, 'CID'), h('th', {}, 'Ingested by'), h('th', {}, 'Announced by'))),
      h('tbody', {}, f.manifest_list.map((m) => h('tr', {},
        h('td', {}, copyable(m.cid, h('a', { class: 'button', href: '/api/manifest?cid=' + enc(m.cid), target: '_blank', rel: 'noopener' }, 'Inspect'))),
        h('td', {}, m.ingester ? nodeLink(m.ingester, m.ingester_name) : '-'),
        h('td', {}, plural(m.holders, 'node', 'nodes'))))))));
  showView(fileTitle(f),
    h('div', { class: 'chips' }, classification(f), f.niche && h('span', { class: 'chip niche' }, f.niche), indexNote(f)),
    f.keywords && f.keywords.length ? h('div', { class: 'chips', style: 'margin-top:6px' },
      f.keywords.map((k) => h('a', { class: 'chip kw', href: '#search/' + enc(k) }, k))) : null,
    h('div', { class: 'toolbar' },
      f.payload ? h('a', { class: 'button primary', href: '/ipfs/' + enc(f.payload), target: '_blank', rel: 'noopener' }, 'Open file') : null,
      h('button', { type: 'button', 'aria-expanded': 'false', 'aria-controls': 'file-details', onclick: (e) => {
        details.hidden = !details.hidden;
        e.currentTarget.setAttribute('aria-expanded', String(!details.hidden));
      } }, 'Details')),
    details);
}

function showNode(id) {
  const n = nodeByID(id);
  if (!n) return showView('Node', h('p', {}, 'This node is not known.'));
  const out = h('div', {});
  showView(nodeName(id),
    h('dl', { class: 'facts' },
      fact('Status', badge(n.status)),
      fact('Node ID', copyable(n.id)),
      n.helper ? fact('Helper ID', h('span', { class: 'cid' }, n.helper)) : null,
      fact('Shard', shardLabel(n.shard)),
      n.role && n.role !== 'ACTIVE' ? fact('Role', n.role) : null,
      n.status === 'offline' ? null : fact('Files of its shard', fmt(n.held) + ' of ' + fmt(n.expected)),
      fact('Last seen', ago(nowS() - n.last_seen) + ' ago'),
      n.status === 'offline' ? null : fact('Uptime', duration(nowS() - n.first_seen))),
    h('div', { class: 'chips' },
      h('a', { class: 'chip', href: '#holding/' + enc(n.id) }, 'Files it holds'),
      n.expected > n.held && n.status !== 'offline' ? h('a', { class: 'chip', href: '#missing/' + enc(n.id) }, 'Files it lacks (' + fmt(n.expected - n.held) + ')') : null,
      n.status === 'offline' ? null : h('button', { type: 'button', onclick: () => identify(n.id, out) }, 'Identify')),
    out);
}

async function identify(id, out) {
  out.replaceChildren(h('p', { class: 'muted' }, 'Asking the node...'));
  try {
    const d = await getJSON('/api/identify?peer=' + enc(id));
    out.replaceChildren(h('dl', { class: 'facts' },
      fact('Connected', d.connected ? 'yes' : 'no'),
      fact('Agent', d.agent_version || '-'),
      fact('Protocol', d.protocol_version || '-'),
      fact('Addresses', d.addresses && d.addresses.length ? d.addresses.map((a) => h('div', {}, a)) : '-')));
  } catch (e) {
    out.replaceChildren(h('p', { class: 'error' }, e.message));
  }
}

const looksLikeID = (q) => !/\s/.test(q) && q.length > 30;
let suggestTimer = 0;
let suggestIndex = -1;

async function find(q) {
  q = q.trim();
  if (!q) return;
  hideSuggest();
  try {
    const r = await getJSON('/api/lookup?q=' + enc(q));
    const routes = { file: 'file/', node: 'node/', uploader: 'key/', search: 'search/' };
    if (routes[r.kind]) go(routes[r.kind] + enc(r.id));
    else showView('Not found', h('p', {}, r.message || 'Nothing found.'));
  } catch (e) {
    showView('Not found', h('p', { class: 'error' }, e.message));
  }
}

function hideSuggest() {
  $('suggest').hidden = true;
  suggestIndex = -1;
}

async function suggest() {
  const q = $('find-input').value.trim();
  if (q.length < 2 || looksLikeID(q)) return hideSuggest();
  let items = [];
  try {
    items = (await getJSON('/api/keyword-suggest?q=' + enc(q))).suggestions || [];
  } catch (e) {
    return hideSuggest();
  }
  const list = $('suggest');
  list.replaceChildren(...items.slice(0, 8).map((s) => h('li', { role: 'option', onmousedown: (e) => { e.preventDefault(); pick(s.keyword); } },
    h('span', {}, s.keyword), h('span', { class: 'muted' }, fmt(s.cid_count)))));
  suggestIndex = -1;
  list.hidden = items.length === 0;
}

function pick(keyword) {
  $('find-input').value = keyword;
  hideSuggest();
  go('search/' + enc(keyword));
}

function moveSuggest(step) {
  const items = [...$('suggest').children];
  if ($('suggest').hidden || !items.length) return false;
  suggestIndex = (suggestIndex + step + items.length) % items.length;
  items.forEach((li, i) => li.setAttribute('aria-selected', String(i === suggestIndex)));
  return true;
}

function openSettings() {
  const i = (summary && summary.index) || {};
  const on = !!i.settings_enabled;
  const using = i.enabled ? 'The indexer uses ' + i.provider + ' (' + i.model + ').' : 'No LLM API key is set.';
  $('settings-help').textContent = on
    ? using + ' Enter the password from DLOCKSS_MONITOR_PASSWORD to change the settings.'
    : using + ' Settings are off: set DLOCKSS_MONITOR_PASSWORD on the monitor host to change them here.';
  $('set-provider').value = i.provider === 'saia' ? 'saia' : 'google';
  $('set-gateway').value = i.gateway || '';
  $('set-key').value = '';
  $('set-password').value = '';
  $('set-error').textContent = '';
  for (const id of ['set-password', 'set-provider', 'set-base', 'set-key', 'set-gateway', 'settings-save']) $(id).disabled = !on;
  syncProvider();
  $('settings').showModal();
}

function syncProvider() {
  $('set-base-row').hidden = $('set-provider').value !== 'saia';
}

async function saveSettings() {
  const body = {
    password: $('set-password').value,
    provider: $('set-provider').value,
    api_key: $('set-key').value,
    api_base: $('set-base').value,
    gateway: $('set-gateway').value,
  };
  try {
    await getJSON('/api/keyword-api-key', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) });
  } catch (e) {
    $('set-error').textContent = e.message;
    return;
  }
  $('settings').close();
  toast('Settings saved');
  poll();
}

async function loadTree() {
  try {
    $('shard-tree').replaceChildren(treeNode(await getJSON('/api/shard-tree')));
  } catch (e) {
    $('shard-tree').replaceChildren(h('p', { class: 'error small' }, e.message));
  }
}

function treeNode(n) {
  return h('div', { class: 'tree-node' },
    h('div', { class: 'tree-box' }, h('b', {}, n.shard_id === '' ? 'ROOT' : n.shard_id), h('div', { class: 'muted small' }, plural(n.node_count, 'node', 'nodes'))),
    n.children && n.children.length ? h('div', { class: 'tree-children' }, n.children.map(treeNode)) : null);
}

$('view-close').addEventListener('click', closeView);
$('find-form').addEventListener('submit', (e) => {
  e.preventDefault();
  const items = [...$('suggest').children];
  if (!$('suggest').hidden && suggestIndex >= 0 && items[suggestIndex]) pick(items[suggestIndex].firstChild.textContent);
  else find($('find-input').value);
});
$('find-input').addEventListener('input', () => {
  clearTimeout(suggestTimer);
  suggestTimer = setTimeout(suggest, 200);
});
$('find-input').addEventListener('keydown', (e) => {
  if (e.key === 'ArrowDown' && moveSuggest(1)) e.preventDefault();
  else if (e.key === 'ArrowUp' && moveSuggest(-1)) e.preventDefault();
  else if (e.key === 'Escape') hideSuggest();
});
$('find-input').addEventListener('blur', () => setTimeout(hideSuggest, 150));
$('settings-open').addEventListener('click', openSettings);
$('browse-toggle').addEventListener('click', () => {
  const open = $('browse').hidden;
  $('browse').hidden = !open;
  $('browse-toggle').setAttribute('aria-expanded', String(open));
  if (open) loadCollection();
});
$('settings-cancel').addEventListener('click', () => $('settings').close());
$('settings-save').addEventListener('click', saveSettings);
$('set-provider').addEventListener('change', syncProvider);
$('internals').addEventListener('toggle', () => { if ($('internals').open) loadTree(); });
window.addEventListener('hashchange', showRoute);
document.addEventListener('visibilitychange', () => { if (!document.hidden) poll(); });
setInterval(poll, POLL_MS);
setInterval(() => { if (!document.hidden) renderLive(); }, 1000);
poll().then(showRoute);
