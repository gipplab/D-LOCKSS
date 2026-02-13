package main

// dashboardHTML is the full D-LOCKSS Monitor web UI (Chart.js, shard tree, node table).
const dashboardHTML = `<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <title>D-LOCKSS Network Monitor</title>
    <script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
    <style>
        body { font-family: "Courier New", Courier, monospace; margin: 20px; background: #f8f9fa; color: #333; }
        .container { max-width: 1600px; margin: 0 auto; }
        h1 { color: #222; text-transform: uppercase; border-bottom: 2px solid #333; padding-bottom: 10px; }
        .stats { display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 15px; margin: 20px 0; }
        .stat-card { background: white; padding: 15px; border: 1px solid #333; }
        .stat-value { font-size: 2em; font-weight: 700; color: #000; }
        .stat-label { color: #555; font-size: 0.8em; text-transform: uppercase; margin-top: 5px; }
        .charts { display: grid; grid-template-columns: repeat(auto-fit, minmax(400px, 1fr)); gap: 20px; margin: 20px 0; }
        .chart-container { background: white; padding: 20px; border: 1px solid #333; }
        .shard-tree-section { background: white; padding: 20px; margin: 20px 0; border: 1px solid #333; overflow-x: auto; }
        .tree-chart { display: flex; justify-content: center; padding: 20px 0; min-width: max-content; }
        .tree-node { display: flex; flex-direction: column; align-items: center; position: relative; padding: 20px 5px 0 5px; }
        .tree-node::before, .tree-node::after { content: ''; position: absolute; top: 0; right: 50%; border-top: 1px solid #333; width: 50%; height: 20px; }
        .tree-node::after { right: auto; left: 50%; border-left: 1px solid #333; }
        .tree-node:only-child::after, .tree-node:only-child::before { display: none; }
        .tree-node:first-child::before, .tree-node:last-child::after { border: 0 none; }
        .tree-node:last-child::before { border-right: 1px solid #333; border-radius: 0 5px 0 0; }
        .tree-node:first-child::after { border-radius: 5px 0 0 0; }
        .node-content { border: 1px solid #333; padding: 5px 10px; text-align: center; background: #fff; z-index: 2; font-size: 0.85em; min-width: 80px; box-shadow: 2px 2px 0px #eee; }
        .node-content:hover { background: #f5f5f5; }
        .node-id { font-weight: bold; color: #000; margin-bottom: 2px; }
        .node-count { font-size: 0.8em; color: #555; }
        .node-children { display: flex; flex-direction: row; padding-top: 20px; position: relative; }
        .node-children::before { content: ''; position: absolute; top: 0; left: 50%; border-left: 1px solid #333; width: 0; height: 20px; }
        .node-table { background: white; padding: 20px; margin: 20px 0; border: 1px solid #333; }
        table { width: 100%; border-collapse: collapse; font-size: 0.9em; }
        th { text-align: left; padding: 10px; border-bottom: 2px solid #333; background: #eee; text-transform: uppercase; font-size: 0.8em; }
        td { padding: 10px; border-bottom: 1px solid #ddd; }
        .status-text { font-weight: bold; font-size: 0.8em; }
        .status-online { color: green; }
        .btn-text { background: none; border: 1px solid #999; cursor: pointer; font-family: inherit; font-size: 0.8em; padding: 2px 6px; text-transform: uppercase; color: #333; }
        .btn-text:hover { background: #eee; color: #000; border-color: #333; }
        .btn-save { border-color: green; color: green; }
        .btn-cancel { border-color: red; color: red; }
        .shard-badge { background: #eee; padding: 2px 6px; font-size: 0.9em; border: 1px solid #ccc; }
        .alias-input { font-family: inherit; padding: 4px; border: 1px solid #333; width: 120px; }
        .stat-card.clickable { cursor: pointer; }
        .stat-card.clickable:hover { background: #f0f0f0; }
        .modal-overlay { position: fixed; top: 0; left: 0; right: 0; bottom: 0; background: rgba(0,0,0,0.5); display: flex; align-items: center; justify-content: center; z-index: 1000; }
        .modal { background: white; border: 2px solid #333; max-width: 90vw; max-height: 85vh; overflow: hidden; display: flex; flex-direction: column; }
        .modal-header { padding: 15px 20px; border-bottom: 1px solid #333; display: flex; justify-content: space-between; align-items: center; }
        .modal-body { padding: 20px; overflow-y: auto; flex: 1; }
        .cid-row { display: flex; align-items: center; gap: 12px; padding: 8px 0; border-bottom: 1px solid #eee; font-size: 0.85em; flex-wrap: wrap; }
        .cid-row .cid-value { font-family: monospace; word-break: break-all; flex: 1; min-width: 200px; }
        .cid-row .cid-meta { color: #666; font-size: 0.9em; }
        .cid-row .cid-actions { display: flex; gap: 6px; flex-shrink: 0; }
        .cid-payload { margin-top: 4px; font-size: 0.8em; color: #06A77D; }
        .load-spinner { display: inline-block; width: 14px; height: 14px; border: 2px solid #ccc; border-top-color: #06A77D; border-radius: 50%; animation: spin 0.8s linear infinite; vertical-align: middle; }
        @keyframes spin { to { transform: rotate(360deg); } }
    </style>
</head>
<body>
    <div class="container">
        <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom: 20px;">
            <h1>D-LOCKSS Monitor</h1>
            <div style="font-size: 0.9em; text-align: right;">
                <div>SYSTEM STATUS: <span class="status-text status-online">[ONLINE]</span></div>
                <div id="root-topic-row" style="font-size: 0.75em; color: #666; margin-top: 4px; word-break: break-all;">
                    Root topic: <span id="root-topic-value">--</span>
                    <button class="btn-text" id="root-topic-edit-btn" style="margin-left: 6px; font-size: 0.9em;">EDIT</button>
                </div>
                <div id="root-topic-edit-row" style="display: none; margin-top: 4px;">
                    <input type="text" id="root-topic-input" placeholder="e.g. dlockss-v0.0.2" style="padding: 4px; font-family: inherit; font-size: 0.85em; width: 160px; border: 1px solid #333;" title="Topic prefix (full topic: {prefix}-creative-commons-shard-)">
                    <button class="btn-text btn-save" id="root-topic-save-btn">SAVE</button>
                    <button class="btn-text btn-cancel" id="root-topic-cancel-btn">CANCEL</button>
                </div>
            </div>
        </div>
        <div class="stats">
            <div class="stat-card clickable" id="total-nodes-card" title="Click to jump to Network Nodes"><div class="stat-value" id="total-nodes">--</div><div class="stat-label">Total Nodes</div></div>
            <div class="stat-card"><div class="stat-value" id="total-pinned">--</div><div class="stat-label">Total Pinned</div></div>
            <div class="stat-card clickable" id="unique-cids-card" title="Click to view all CIDs"><div class="stat-value" id="unique-files">--</div><div class="stat-label">Unique CIDs</div></div>
            <div class="stat-card clickable" id="total-shards-card" title="Click to jump to Shard Topology"><div class="stat-value" id="total-shards">--</div><div class="stat-label">Active Shards</div></div>
        </div>
        <div class="charts">
            <div class="chart-container" style="cursor: pointer;"><h3 style="margin-top:0; text-transform:uppercase; font-size:1em;">Replication Status</h3><p style="font-size:0.75em; color:#666; margin:0 0 10px 0;">Network-wide (all shards). Nodes unpin files that no longer belong to their shard after a split.</p><canvas id="replicationChart"></canvas><div id="replicationByShard" style="font-size:0.8em; margin-top:10px; color:#555;"></div></div>
            <div class="chart-container" style="cursor: pointer;"><h3 style="margin-top:0; text-transform:uppercase; font-size:1em;">Pinned Files per Node</h3><canvas id="filesChart"></canvas></div>
            <div class="chart-container" style="cursor: pointer;"><h3 style="margin-top:0; text-transform:uppercase; font-size:1em;">Shard Distribution</h3><canvas id="shardChart"></canvas></div>
        </div>
        <div class="shard-tree-section" id="shard-topology-section"><h3 style="margin-top:0; text-transform:uppercase; font-size:1em;">Shard Topology Chart</h3><div id="shardTreeContainer" class="tree-chart"></div></div>
        <div class="node-table" id="network-nodes-section">
            <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:15px;">
                <h3 style="margin:0; text-transform:uppercase; font-size:1em;">Network Nodes</h3>
                <input type="text" id="nodeSearch" placeholder="SEARCH ID/REGION/SHARD..." style="padding: 8px; border: 1px solid #333; width: 300px; font-family:inherit;" onkeyup="debouncedFilter()">
            </div>
            <table id="nodeTable"><thead><tr><th style="width: 80px;">Action</th><th>Peer ID</th><th>Region</th><th>Shard</th><th>Peers</th><th>Pinned</th><th>Known</th><th>Uptime</th><th>Last Seen</th></tr></thead><tbody id="nodeTableBody"></tbody></table>
        </div>
    </div>
    <div id="cids-modal" class="modal-overlay" style="display: none;">
        <div class="modal" style="width: 700px;">
            <div class="modal-header"><h3 style="margin:0; text-transform: uppercase; font-size: 1em;">Unique CIDs</h3><input type="text" id="cids-modal-search" placeholder="Filter CIDs..." style="padding: 6px; font-family: inherit; font-size: 0.85em; width: 180px; border: 1px solid #333;"><button class="btn-text" id="cids-modal-close">CLOSE</button></div>
            <div class="modal-body"><div id="cids-modal-list"></div></div>
        </div>
    </div>
    <div id="node-files-modal" class="modal-overlay" style="display: none;">
        <div class="modal" style="width: 700px;">
            <div class="modal-header"><h3 style="margin:0; text-transform: uppercase; font-size: 1em;" id="node-files-modal-title">Pinned Files</h3><input type="text" id="node-files-modal-search" placeholder="Filter CIDs..." style="padding: 6px; font-family: inherit; font-size: 0.85em; width: 180px; border: 1px solid #333;"><button class="btn-text" id="node-files-modal-close">CLOSE</button></div>
            <div class="modal-body"><div id="node-files-modal-list"></div></div>
        </div>
    </div>
    <div id="replication-modal" class="modal-overlay" style="display: none;">
        <div class="modal" style="width: 700px;">
            <div class="modal-header"><h3 style="margin:0; text-transform: uppercase; font-size: 1em;" id="replication-modal-title">Files at Replication Level</h3><input type="text" id="replication-modal-search" placeholder="Filter CIDs..." style="padding: 6px; font-family: inherit; font-size: 0.85em; width: 180px; border: 1px solid #333;"><button class="btn-text" id="replication-modal-close">CLOSE</button></div>
            <div class="modal-body"><div id="replication-modal-list"></div></div>
        </div>
    </div>
    <div id="shard-nodes-modal" class="modal-overlay" style="display: none;">
        <div class="modal" style="width: 700px;">
            <div class="modal-header"><h3 style="margin:0; text-transform: uppercase; font-size: 1em;" id="shard-nodes-modal-title">Nodes in Shard</h3><input type="text" id="shard-nodes-modal-search" placeholder="Filter nodes..." style="padding: 6px; font-family: inherit; font-size: 0.85em; width: 180px; border: 1px solid #333;"><button class="btn-text" id="shard-nodes-modal-close">CLOSE</button></div>
            <div class="modal-body"><div id="shard-nodes-modal-list"></div></div>
        </div>
    </div>
    <script>
        let filesChart, shardChart, replicationChart;
        const ALIASES_STORAGE_KEY = 'dlockss_node_aliases';
        let currentlyEditingPeerID = null;
        const filesCtx = document.getElementById('filesChart').getContext('2d');
        const shardCtx = document.getElementById('shardChart').getContext('2d');
        const replicationCtx = document.getElementById('replicationChart').getContext('2d');
        let searchTimeout;
        function escapeHtml(text) { const div = document.createElement('div'); div.textContent = text; return div.innerHTML; }
        function escapeJs(text) { if (!text) return ''; return text.replace(/\\/g, '\\\\').replace(/'/g, "\\'").replace(/"/g, '\\"'); }
        function loadAliases() { try { return JSON.parse(localStorage.getItem(ALIASES_STORAGE_KEY) || '{}'); } catch { return {}; } }
        function saveAliases(aliases) { localStorage.setItem(ALIASES_STORAGE_KEY, JSON.stringify(aliases)); }
        function setAlias(peerID, alias) { const aliases = loadAliases(); if (alias && alias.trim()) aliases[peerID] = alias.trim(); else delete aliases[peerID]; saveAliases(aliases); currentlyEditingPeerID = null; updateDashboard(); }
        function getAliases() { return loadAliases(); }
        function editAlias(peerID, currentAlias, btn) {
            const row = btn.closest('tr'); const displayDiv = row.querySelector('.alias-display-container');
            if (!displayDiv) return; currentlyEditingPeerID = peerID;
            displayDiv.innerHTML = '';
            const input = document.createElement('input'); input.type = 'text'; input.className = 'alias-input'; input.value = currentAlias || ''; input.placeholder = 'LABEL...';
            const saveBtn = document.createElement('button'); saveBtn.className = 'btn-text btn-save'; saveBtn.textContent = 'SAVE'; saveBtn.onclick = () => setAlias(peerID, input.value);
            const cancelBtn = document.createElement('button'); cancelBtn.className = 'btn-text btn-cancel'; cancelBtn.textContent = 'CANCEL'; cancelBtn.onclick = () => { currentlyEditingPeerID = null; updateDashboard(); };
            displayDiv.appendChild(input); displayDiv.appendChild(saveBtn); displayDiv.appendChild(cancelBtn); input.focus();
            input.onkeypress = (e) => { if(e.key === 'Enter') setAlias(peerID, input.value); else if(e.key === 'Escape') cancelBtn.click(); };
        }
        function restoreEditingState() { if (currentlyEditingPeerID) { const row = document.querySelector('tr[data-peer-id="' + currentlyEditingPeerID.toLowerCase() + '"]'); if (row) { const btn = row.querySelector('.alias-edit-btn'); const aliases = getAliases(); if (btn) editAlias(currentlyEditingPeerID, aliases[currentlyEditingPeerID] || '', btn); } } }
        function initCharts() {
            Chart.defaults.font.family = '"Courier New", Courier, monospace'; Chart.defaults.color = '#333';
            replicationChart = new Chart(replicationCtx, { type: 'bar', data: { labels: ['0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '10+'], datasets: [{ label: 'Files', data: [0,0,0,0,0,0,0,0,0,0,0], backgroundColor: ['#D00000','#F18F01','#FFC300','#FFD60A','#FFE66D','#06A77D','#06A77D','#06A77D','#06A77D','#06A77D','#06A77D'], borderRadius: 0 }] }, options: { responsive: true, plugins: { legend: { display: false } }, scales: { y: { beginAtZero: true }, x: {} }, onClick: function(evt, elements, chart) { if (elements.length && elements[0].index >= 0 && elements[0].index <= 10) { showReplicationModal(elements[0].index); } } } });
            filesChart = new Chart(filesCtx, { type: 'bar', data: { labels: [], datasets: [{ label: 'Pinned Files', data: [], backgroundColor: '#333', borderRadius: 0 }] }, options: { responsive: true, animation: false, plugins: { legend: { display: false } }, onClick: function(evt, elements, chart) { if (elements.length && window._chartNodeOrder && window._chartNodeOrder[elements[0].index]) { showNodeFilesModal(window._chartNodeOrder[elements[0].index]); } } } });
            shardChart = new Chart(shardCtx, { type: 'doughnut', data: { labels: [], datasets: [{ data: [], backgroundColor: [] }] }, options: { responsive: true, animation: false, cutout: '60%', plugins: { legend: { position: 'right' } }, onClick: function(evt, elements, chart) { if (elements.length && chart.data && chart.data.labels && chart.data.labels[elements[0].index] !== undefined) { showShardNodesModal(chart.data.labels[elements[0].index]); } } } });
        }
        function renderShardTreeChart(node, container) {
            const nodeDiv = document.createElement('div'); nodeDiv.className = 'tree-node';
            const contentDiv = document.createElement('div'); contentDiv.className = 'node-content'; contentDiv.style.cursor = 'pointer'; contentDiv.title = 'Click to view nodes';
            const label = node.shard_id === "" ? "ROOT" : node.shard_id;
            contentDiv.innerHTML = '<div class="node-id">' + escapeHtml(label) + '</div><div class="node-count">' + node.node_count + ' NODES</div>';
            contentDiv.onclick = function() { showShardNodesModal(node.shard_id); };
            nodeDiv.appendChild(contentDiv);
            if (node.children && node.children.length > 0) { const childrenDiv = document.createElement('div'); childrenDiv.className = 'node-children'; node.children.forEach(child => { renderShardTreeChart(child, childrenDiv); }); nodeDiv.appendChild(childrenDiv); }
            container.appendChild(nodeDiv);
        }
        function updateShardTree() { fetch('/api/shard-tree').then(r=>r.json()).then(tree => { const c = document.getElementById('shardTreeContainer'); c.innerHTML = ''; renderShardTreeChart(tree, c); }); }
        function updateDashboard() {
            const q = document.getElementById('nodeSearch').value;
            const url = '/api/nodes?t=' + Date.now() + (q ? '&q=' + encodeURIComponent(q) : '');
            fetch(url).then(r=>r.json()).then(data => {
                const aliases = getAliases();
                const nodes = Object.values(data).map(n => n.data).sort((a, b) => a.peer_id.localeCompare(b.peer_id));
                const meta = data;
                document.getElementById('total-nodes').textContent = nodes.length;
                const pinnedVal = n => (n.storage.pinned_in_shard != null) ? n.storage.pinned_in_shard : n.storage.pinned_files;
                document.getElementById('total-pinned').textContent = nodes.reduce((s,n) => s + (pinnedVal(n)||0), 0).toLocaleString();
                fetch('/api/unique-cids').then(r=>r.json()).then(data => { document.getElementById('unique-files').textContent = (data.count || 0).toLocaleString(); }).catch(() => { const uCids = new Set(); nodes.forEach(n => (n.storage.known_cids||[]).forEach(c => uCids.add(c))); document.getElementById('unique-files').textContent = uCids.size.toLocaleString(); });
                document.getElementById('total-shards').textContent = new Set(nodes.map(n => n.current_shard)).size;
                fetch('/api/replication?t=' + Date.now()).then(r=>r.json()).then(data => { const dist = data.replication_distribution || [0,0,0,0,0,0,0,0,0,0,0]; if (replicationChart && replicationChart.data && replicationChart.data.datasets && replicationChart.data.datasets[0]) { replicationChart.data.datasets[0].data = Array.isArray(dist) ? dist : [0,0,0,0,0,0,0,0,0,0,0]; replicationChart.update(); } const byShard = data.files_at_target_per_shard || {}; const el = document.getElementById('replicationByShard'); if (el && Object.keys(byShard).length > 0) { const parts = Object.entries(byShard).sort((a,b)=>String(a[0]).localeCompare(String(b[0]))).map(([s,c]) => (s === '' ? 'ROOT' : s) + ': ' + c + ' files'); el.textContent = 'Files at target per shard: ' + parts.join(', '); } else if (el) el.textContent = ''; }).catch(() => { if (replicationChart && replicationChart.data && replicationChart.data.datasets && replicationChart.data.datasets[0]) { replicationChart.data.datasets[0].data = [0,0,0,0,0,0,0,0,0,0,0]; replicationChart.update(); } });
                window._chartNodeOrder = nodes.map(n => n.peer_id);
                filesChart.data.labels = nodes.map(n => aliases[n.peer_id] || n.peer_id.slice(-6));
                filesChart.data.datasets[0].data = nodes.map(n => pinnedVal(n) || 0);
                filesChart.update();
                const sCounts = {}; nodes.forEach(n => sCounts[n.current_shard] = (sCounts[n.current_shard]||0)+1); shardChart.data.labels = Object.keys(sCounts); shardChart.data.datasets[0].data = Object.values(sCounts);
                const colorPalette = ['#FF6B6B','#4ECDC4','#45B7D1','#FFA07A','#98D8C8','#F7DC6F','#BB8FCE','#85C1E2','#F8B739','#52BE80','#EC7063','#5DADE2','#58D68D','#F4D03F','#AF7AC5','#85C1E9','#F1948A','#73C6B6','#F9E79F','#A569BD'];
                shardChart.data.datasets[0].backgroundColor = Object.keys(sCounts).map((_,i) => colorPalette[i % colorPalette.length]); shardChart.update();
                const tbody = document.getElementById('nodeTableBody');
                tbody.innerHTML = nodes.map(n => {
                    const m = meta[n.peer_id]; const alias = aliases[n.peer_id] || ''; const lastSeen = Math.floor((Date.now()/1000) - m.last_seen);
                    const peerIdEscaped = escapeJs(n.peer_id); const aliasEscaped = escapeJs(alias); const peerIdHtml = escapeHtml(n.peer_id); const aliasHtml = escapeHtml(alias); const regionHtml = escapeHtml(m.region || '-'); const shardHtml = escapeHtml(n.current_shard);
                    const pinned = (n.storage.pinned_in_shard != null) ? n.storage.pinned_in_shard : n.storage.pinned_files;
                    return '<tr data-peer-id="' + escapeHtml(n.peer_id.toLowerCase()) + '"><td><button class="btn-text alias-edit-btn" onclick="editAlias(\'' + peerIdEscaped + '\', \'' + aliasEscaped + '\', this)">EDIT</button></td><td class="peer-id-cell"><div class="alias-display-container"><div style="font-weight:600;">' + (aliasHtml || peerIdHtml.slice(0,12) + '...') + '</div><div style="font-size:0.8em; color:#666;">' + peerIdHtml + '</div></div></td><td>' + (m.region ? regionHtml : '<span style="color:#999" title="Geo lookup pending or unavailable">-</span>') + '</td><td><span class="shard-badge">' + shardHtml + '</span></td><td>' + n.peers_in_shard + '</td><td>' + pinned + '</td><td>' + n.storage.known_files + '</td><td>' + Math.floor(n.uptime_seconds/60) + 'm</td><td><span class="status-text status-online">[ACTIVE]</span> ' + lastSeen + 's ago</td></tr>';
                }).join('');
                restoreEditingState();
            });
        }
        function debouncedFilter() { clearTimeout(searchTimeout); searchTimeout = setTimeout(() => { updateDashboard(); }, 300); }
        function loadRootTopic() { fetch('/api/root-topic').then(r=>r.json()).then(d => { const el = document.getElementById('root-topic-value'); if (el && d.root_topic) el.textContent = d.root_topic; if (d.topic_prefix) window._currentTopicPrefix = d.topic_prefix; }).catch(() => {}); }
        function showTopicEdit() { document.getElementById('root-topic-row').style.display = 'none'; document.getElementById('root-topic-edit-row').style.display = 'block'; document.getElementById('root-topic-input').value = window._currentTopicPrefix || ''; document.getElementById('root-topic-input').focus(); }
        function hideTopicEdit() { document.getElementById('root-topic-edit-row').style.display = 'none'; document.getElementById('root-topic-row').style.display = ''; }
        function saveTopicPrefix() {
            const prefix = document.getElementById('root-topic-input').value.trim();
            fetch('/api/root-topic', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ topic_prefix: prefix || undefined }) })
                .then(r => { if (!r.ok) throw new Error(r.statusText); return r.json(); })
                .then(d => { window._currentTopicPrefix = d.topic_prefix; document.getElementById('root-topic-value').textContent = d.root_topic; hideTopicEdit(); updateDashboard(); updateShardTree(); })
                .catch(e => alert('Failed to switch topic: ' + e.message));
        }
        document.getElementById('root-topic-edit-btn').onclick = showTopicEdit;
        document.getElementById('root-topic-save-btn').onclick = saveTopicPrefix;
        document.getElementById('root-topic-cancel-btn').onclick = hideTopicEdit;
        document.getElementById('root-topic-input').onkeydown = function(e) { if (e.key === 'Enter') saveTopicPrefix(); else if (e.key === 'Escape') hideTopicEdit(); };
        const GATEWAY = 'https://ipfs.io';
        function showCidsModal() {
            document.getElementById('cids-modal').style.display = 'flex';
            fetch('/api/unique-cids?t=' + Date.now()).then(r=>r.json()).then(data => {
                const list = document.getElementById('cids-modal-list');
                const cids = data.cids || [];
                list.innerHTML = cids.length === 0 ? '<p style="color:#666;">No CIDs yet. CIDs appear when nodes announce pinned files.</p>' : cids.map((entry, i) => {
                    const cid = entry.cid || entry.CID || entry; const shard = (entry.shard || entry.Shard || '') === '' ? 'root' : (entry.shard || entry.Shard); const replicas = entry.replicas ?? entry.Replicas ?? 0;
                    return '<div class="cid-row" data-cid="' + escapeHtml(cid) + '"><div class="cid-value">' + escapeHtml(cid) + '</div><div class="cid-meta">Shard: ' + escapeHtml(shard) + ' | Replicas: ' + replicas + '</div><div class="cid-actions"><button class="btn-text" onclick="loadManifest(\'' + escapeJs(cid) + '\', this.parentElement)">LOAD MANIFEST</button></div></div>';
                }).join('');
            }).catch(() => { document.getElementById('cids-modal-list').innerHTML = '<p style="color:#999;">Failed to load CIDs.</p>'; });
        }
        function hideCidsModal() { document.getElementById('cids-modal').style.display = 'none'; }
        function showNodeFilesModal(peerId) {
            const aliases = loadAliases();
            const label = aliases[peerId] || peerId.slice(-6);
            document.getElementById('node-files-modal-title').textContent = 'Pinned files: ' + label;
            document.getElementById('node-files-modal').style.display = 'flex';
            document.getElementById('node-files-modal-search').value = '';
            fetch('/api/node-files?peer=' + encodeURIComponent(peerId) + '&t=' + Date.now()).then(r=>r.json()).then(data => {
                const list = document.getElementById('node-files-modal-list');
                const cids = data.cids || [];
                list.innerHTML = cids.length === 0 ? '<p style="color:#666;">No pinned files for this node.</p>' : cids.map((entry, i) => {
                    const cid = entry.cid || entry.CID || entry; const shard = (entry.shard || entry.Shard || '') === '' ? 'root' : (entry.shard || entry.Shard); const replicas = entry.replicas ?? entry.Replicas ?? 0;
                    return '<div class="cid-row" data-cid="' + escapeHtml(cid) + '"><div class="cid-value">' + escapeHtml(cid) + '</div><div class="cid-meta">Shard: ' + escapeHtml(shard) + ' | Replicas: ' + replicas + '</div><div class="cid-actions"><button class="btn-text" onclick="loadManifest(\'' + escapeJs(cid) + '\', this.parentElement)">LOAD MANIFEST</button></div></div>';
                }).join('');
            }).catch(() => { document.getElementById('node-files-modal-list').innerHTML = '<p style="color:#999;">Failed to load node files.</p>'; });
        }
        function hideNodeFilesModal() { document.getElementById('node-files-modal').style.display = 'none'; }
        function showReplicationModal(level) {
            const label = level === 10 ? '10+' : String(level);
            document.getElementById('replication-modal-title').textContent = 'Files at ' + label + ' replicas';
            document.getElementById('replication-modal').style.display = 'flex';
            document.getElementById('replication-modal-search').value = '';
            fetch('/api/replication-cids?level=' + level + '&t=' + Date.now()).then(r=>r.json()).then(data => {
                const list = document.getElementById('replication-modal-list');
                const cids = data.cids || [];
                list.innerHTML = cids.length === 0 ? '<p style="color:#666;">No files at this replication level.</p>' : cids.map((entry, i) => {
                    const cid = entry.cid || entry.CID || entry; const shard = (entry.shard || entry.Shard || '') === '' ? 'root' : (entry.shard || entry.Shard); const replicas = entry.replicas ?? entry.Replicas ?? 0;
                    return '<div class="cid-row" data-cid="' + escapeHtml(cid) + '"><div class="cid-value">' + escapeHtml(cid) + '</div><div class="cid-meta">Shard: ' + escapeHtml(shard) + ' | Replicas: ' + replicas + '</div><div class="cid-actions"><button class="btn-text" onclick="loadManifest(\'' + escapeJs(cid) + '\', this.parentElement)">LOAD MANIFEST</button></div></div>';
                }).join('');
            }).catch(() => { document.getElementById('replication-modal-list').innerHTML = '<p style="color:#999;">Failed to load CIDs.</p>'; });
        }
        function hideReplicationModal() { document.getElementById('replication-modal').style.display = 'none'; }
        function showShardNodesModal(shardId) {
            const label = shardId === "" ? "ROOT" : shardId;
            document.getElementById('shard-nodes-modal-title').textContent = 'Nodes in shard: ' + label;
            document.getElementById('shard-nodes-modal').style.display = 'flex';
            document.getElementById('shard-nodes-modal-search').value = '';
            fetch('/api/shard-nodes?shard=' + encodeURIComponent(shardId) + '&t=' + Date.now()).then(r=>r.json()).then(data => {
                const list = document.getElementById('shard-nodes-modal-list');
                const nodes = data.nodes || {};
                const aliases = loadAliases();
                const entries = Object.entries(nodes).map(([pid, m]) => ({ peer_id: pid, meta: m }));
                if (entries.length === 0) { list.innerHTML = '<p style="color:#666;">No nodes in this shard.</p>'; return; }
                list.innerHTML = entries.map(({ peer_id, meta }) => {
                    const n = meta.data;
                    const alias = aliases[peer_id] || '';
                    const lastSeen = Math.floor((Date.now()/1000) - (meta.last_seen || 0));
                    const peerIdHtml = escapeHtml(peer_id); const aliasHtml = escapeHtml(alias); const regionHtml = escapeHtml(meta.region || '-');
                    const pinned = (n.storage.pinned_in_shard != null) ? n.storage.pinned_in_shard : n.storage.pinned_files;
                    const searchText = (alias + ' ' + peer_id + ' ' + (meta.region||'')).toLowerCase();
                    return '<div class="cid-row node-row" data-peer-id="' + escapeHtml(peer_id.toLowerCase()) + '" data-search="' + escapeHtml(searchText) + '"><div class="cid-value">' + (aliasHtml || peerIdHtml) + '</div><div class="cid-meta">' + (alias ? peerIdHtml + ' | ' : '') + 'Region: ' + regionHtml + ' | Pinned: ' + pinned + ' | Known: ' + (n.storage.known_files||0) + ' | Last seen: ' + lastSeen + 's ago</div></div>';
                }).join('');
            }).catch(() => { document.getElementById('shard-nodes-modal-list').innerHTML = '<p style="color:#999;">Failed to load nodes.</p>'; });
        }
        function hideShardNodesModal() { document.getElementById('shard-nodes-modal').style.display = 'none'; }
        function viewManifestJson(manifestCid) {
            const manifest = (window._manifests || {})[manifestCid];
            if (!manifest) return;
            const json = JSON.stringify(manifest, null, 2);
            const blob = new Blob([json], { type: 'application/json' });
            const url = URL.createObjectURL(blob);
            window.open(url);
        }
        function loadManifest(manifestCid, actionsEl) {
            if (actionsEl.dataset.loading) return;
            actionsEl.dataset.loading = '1';
            actionsEl.innerHTML = '<span class="load-spinner"></span>';
            fetch('/api/manifest-payload?cid=' + encodeURIComponent(manifestCid)).then(r => r.json()).then(obj => {
                const payloadCid = obj.payload_cid;
                if (payloadCid) {
                    window._manifests = window._manifests || {};
                    window._manifests[manifestCid] = obj.manifest;
                    const payloadUrl = GATEWAY + '/ipfs/' + payloadCid + '/';
                    actionsEl.innerHTML = '<button class="btn-text" onclick="viewManifestJson(\'' + escapeJs(manifestCid) + '\')">VIEW MANIFEST</button><a href="' + payloadUrl + '" target="_blank" rel="noopener" class="btn-text">VIEW PAYLOAD</a>';
                } else {
                    actionsEl.innerHTML = '<button class="btn-text" onclick="loadManifest(\'' + escapeJs(manifestCid) + '\', this.parentElement)">LOAD MANIFEST</button>';
                    alert(obj.error || 'Payload not found in manifest');
                }
                delete actionsEl.dataset.loading;
            }).catch(() => {
                actionsEl.innerHTML = '<button class="btn-text" onclick="loadManifest(\'' + escapeJs(manifestCid) + '\', this.parentElement)">LOAD MANIFEST</button>';
                delete actionsEl.dataset.loading;
                alert('Failed to load manifest');
            });
        }
        document.getElementById('unique-cids-card').onclick = showCidsModal;
        document.getElementById('total-nodes-card').onclick = function() { document.getElementById('network-nodes-section').scrollIntoView({ behavior: 'smooth' }); };
        document.getElementById('total-shards-card').onclick = function() { document.getElementById('shard-topology-section').scrollIntoView({ behavior: 'smooth' }); };
        document.getElementById('cids-modal-close').onclick = hideCidsModal;
        document.getElementById('cids-modal').onclick = function(e) { if (e.target === this) hideCidsModal(); };
        document.getElementById('cids-modal-search').oninput = function() {
            const q = this.value.toLowerCase();
            document.querySelectorAll('#cids-modal-list .cid-row').forEach(row => {
                row.style.display = (!q || row.dataset.cid.toLowerCase().includes(q)) ? '' : 'none';
            });
        };
        document.getElementById('node-files-modal-close').onclick = hideNodeFilesModal;
        document.getElementById('node-files-modal').onclick = function(e) { if (e.target === this) hideNodeFilesModal(); };
        document.getElementById('node-files-modal-search').oninput = function() {
            const q = this.value.toLowerCase();
            document.querySelectorAll('#node-files-modal-list .cid-row').forEach(row => {
                row.style.display = (!q || row.dataset.cid.toLowerCase().includes(q)) ? '' : 'none';
            });
        };
        document.getElementById('replication-modal-close').onclick = hideReplicationModal;
        document.getElementById('replication-modal').onclick = function(e) { if (e.target === this) hideReplicationModal(); };
        document.getElementById('replication-modal-search').oninput = function() {
            const q = this.value.toLowerCase();
            document.querySelectorAll('#replication-modal-list .cid-row').forEach(row => {
                row.style.display = (!q || row.dataset.cid.toLowerCase().includes(q)) ? '' : 'none';
            });
        };
        document.getElementById('shard-nodes-modal-close').onclick = hideShardNodesModal;
        document.getElementById('shard-nodes-modal').onclick = function(e) { if (e.target === this) hideShardNodesModal(); };
        document.getElementById('shard-nodes-modal-search').oninput = function() {
            const q = this.value.toLowerCase();
            document.querySelectorAll('#shard-nodes-modal-list .node-row').forEach(row => {
                const search = (row.dataset.search || row.dataset.peerId || '').toLowerCase();
                row.style.display = (!q || search.includes(q)) ? '' : 'none';
            });
        };
        initCharts(); updateDashboard(); updateShardTree(); loadRootTopic();
        // Refresh every 1s so UI stays close to monitor state (monitor itself updates when nodes send heartbeats).
        setInterval(() => { if(!document.getElementById('nodeSearch').value && currentlyEditingPeerID === null) { updateDashboard(); updateShardTree(); } }, 1000);
    </script>
</body>
</html>
`
