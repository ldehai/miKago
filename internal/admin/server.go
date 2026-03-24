// Package admin provides an HTTP admin interface with a built-in Web Dashboard,
// a Prometheus-compatible /metrics endpoint, and JSON metrics endpoints.
// When --cluster-admin peers are configured, the dashboard aggregates all brokers.
package admin

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"sort"
	"sync"
	"time"

	"github.com/andy/mikago/internal/broker"
	"github.com/andy/mikago/internal/metrics"
)

// portOf extracts the port string from a "host:port" address.
func portOf(addr string) string {
	_, port, err := net.SplitHostPort(addr)
	if err != nil {
		return addr
	}
	return port
}

// ─── JSON API types ───────────────────────────────────────────────────────────

type apiResponse struct {
	BrokerID          int32           `json:"broker_id"`
	TimestampMs       int64           `json:"timestamp_ms"`
	ProduceRequests   int64           `json:"produce_requests"`
	FetchRequests     int64           `json:"fetch_requests"`
	MessagesIn        int64           `json:"messages_in"`
	MessagesOut       int64           `json:"messages_out"`
	BytesIn           int64           `json:"bytes_in"`
	BytesOut          int64           `json:"bytes_out"`
	ActiveConnections int64           `json:"active_connections"`
	RaftTerm          int64           `json:"raft_term"`
	RaftElections     int64           `json:"raft_elections"`
	ProduceP50Ms      float64         `json:"produce_p50_ms"`
	ProduceP95Ms      float64         `json:"produce_p95_ms"`
	ProduceP99Ms      float64         `json:"produce_p99_ms"`
	FetchP50Ms        float64         `json:"fetch_p50_ms"`
	FetchP95Ms        float64         `json:"fetch_p95_ms"`
	FetchP99Ms        float64         `json:"fetch_p99_ms"`
	Topics            []topicInfo     `json:"topics"`
	ConsumerGroups    []groupInfo     `json:"consumer_groups"`
}

type topicInfo struct {
	Name       string          `json:"name"`
	Partitions []partitionInfo `json:"partitions"`
}

type partitionInfo struct {
	ID           int32 `json:"id"`
	HWM          int64 `json:"hwm"`
	LeaderBroker int32 `json:"leader_broker_id"`
	MessagesIn   int64 `json:"messages_in"`
	BytesIn      int64 `json:"bytes_in"`
	BytesOut     int64 `json:"bytes_out"`
}

type groupInfo struct {
	GroupID         string `json:"group_id"`
	Topic           string `json:"topic"`
	Partition       int32  `json:"partition"`
	CommittedOffset int64  `json:"committed_offset"`
	HWM             int64  `json:"hwm"`
	Lag             int64  `json:"lag"`
}

// brokerEntry is one node's entry in the cluster response.
type brokerEntry struct {
	BrokerID int32        `json:"broker_id"`
	AdminURL string       `json:"admin_url"`
	Online   bool         `json:"online"`
	Error    string       `json:"error,omitempty"`
	Metrics  *apiResponse `json:"metrics,omitempty"`
}

// ─── Server ───────────────────────────────────────────────────────────────────

// Server is the HTTP admin server. It is completely independent of the Kafka
// protocol port and adds zero overhead to the message hot path.
type Server struct {
	addr       string
	store      *metrics.Store
	broker     *broker.Broker
	peerURLs   []string     // other brokers' admin base URLs e.g. "http://host:8081"
	httpClient *http.Client // used to fan-out to peer admin endpoints
	httpSrv    *http.Server
}

// New creates an admin Server. Pass peer admin base-URLs (e.g. "http://host:8081")
// to enable the cluster-wide dashboard; pass nil for single-node mode.
func New(addr string, store *metrics.Store, b *broker.Broker, peerURLs []string) *Server {
	return &Server{
		addr:     addr,
		store:    store,
		broker:   b,
		peerURLs: peerURLs,
		httpClient: &http.Client{
			Timeout: 2 * time.Second,
		},
	}
}

// Start registers handlers and begins serving. It returns immediately;
// the server runs in background goroutines and stops when ctx is cancelled.
func (s *Server) Start(ctx context.Context) {
	mux := http.NewServeMux()
	mux.HandleFunc("/", s.handleDashboard)
	mux.HandleFunc("/metrics", s.handlePrometheus)
	mux.HandleFunc("/api/metrics", s.handleAPIMetrics)
	mux.HandleFunc("/api/cluster", s.handleCluster)

	s.httpSrv = &http.Server{Addr: s.addr, Handler: mux}

	go func() {
		log.Printf("[admin] Dashboard  → http://localhost:%s/", portOf(s.addr))
		log.Printf("[admin] Prometheus → http://localhost:%s/metrics", portOf(s.addr))
		log.Printf("[admin] JSON API   → http://localhost:%s/api/metrics", portOf(s.addr))
		if len(s.peerURLs) > 0 {
			log.Printf("[admin] Cluster    → http://localhost:%s/api/cluster  (%d peer(s))", portOf(s.addr), len(s.peerURLs))
		}
		if err := s.httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("[admin] HTTP server error: %v", err)
		}
	}()

	go func() {
		<-ctx.Done()
		shutCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = s.httpSrv.Shutdown(shutCtx)
	}()
}

// ─── Handlers ─────────────────────────────────────────────────────────────────

func (s *Server) handleDashboard(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	fmt.Fprint(w, dashboardHTML)
}

func (s *Server) handlePrometheus(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	fmt.Fprint(w, s.store.FormatPrometheus())
}

func (s *Server) handleAPIMetrics(w http.ResponseWriter, r *http.Request) {
	resp := s.buildAPIResponse()
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(resp)
}

// handleCluster fans out to all known admin peers concurrently and returns a
// unified cluster snapshot. This is what the multi-broker dashboard polls.
func (s *Server) handleCluster(w http.ResponseWriter, r *http.Request) {
	selfMetrics := s.buildAPIResponse()
	entries := make([]brokerEntry, 0, 1+len(s.peerURLs))

	// Self entry (always online, no HTTP round-trip needed)
	entries = append(entries, brokerEntry{
		BrokerID: s.broker.Config.BrokerID,
		AdminURL: fmt.Sprintf("http://localhost:%s", portOf(s.addr)),
		Online:   true,
		Metrics:  &selfMetrics,
	})

	// Fan out to peers concurrently
	if len(s.peerURLs) > 0 {
		var mu sync.Mutex
		var wg sync.WaitGroup
		for _, u := range s.peerURLs {
			wg.Add(1)
			go func(url string) {
				defer wg.Done()
				entry := brokerEntry{AdminURL: url}
				resp, err := s.httpClient.Get(url + "/api/metrics")
				if err != nil {
					entry.Error = err.Error()
					mu.Lock()
					entries = append(entries, entry)
					mu.Unlock()
					return
				}
				defer resp.Body.Close()
				var m apiResponse
				if err := json.NewDecoder(resp.Body).Decode(&m); err != nil {
					entry.Error = "decode error: " + err.Error()
				} else {
					entry.Online = true
					entry.BrokerID = m.BrokerID
					entry.Metrics = &m
				}
				mu.Lock()
				entries = append(entries, entry)
				mu.Unlock()
			}(u)
		}
		wg.Wait()
	}

	// Sort by broker ID for stable ordering
	sort.Slice(entries, func(i, j int) bool { return entries[i].BrokerID < entries[j].BrokerID })

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(entries)
}

// buildAPIResponse assembles a full metrics response from the metrics store and broker state.
func (s *Server) buildAPIResponse() apiResponse {
	snap := s.store.Snapshot()

	resp := apiResponse{
		BrokerID:          s.broker.Config.BrokerID,
		TimestampMs:       time.Now().UnixMilli(),
		ProduceRequests:   snap.ProduceRequests,
		FetchRequests:     snap.FetchRequests,
		MessagesIn:        snap.MessagesIn,
		MessagesOut:       snap.MessagesOut,
		BytesIn:           snap.BytesIn,
		BytesOut:          snap.BytesOut,
		ActiveConnections: snap.ActiveConnections,
		RaftTerm:          snap.RaftTerm,
		RaftElections:     snap.RaftElections,
		ProduceP50Ms:      snap.ProduceP50Ms,
		ProduceP95Ms:      snap.ProduceP95Ms,
		ProduceP99Ms:      snap.ProduceP99Ms,
		FetchP50Ms:        snap.FetchP50Ms,
		FetchP95Ms:        snap.FetchP95Ms,
		FetchP99Ms:        snap.FetchP99Ms,
	}

	// Build per-metric lookup keyed by "topic/partitionID"
	partMetrics := make(map[string]metrics.PartitionSnapshot, len(snap.Partitions))
	for _, ps := range snap.Partitions {
		partMetrics[fmt.Sprintf("%s/%d", ps.Topic, ps.Partition)] = ps
	}

	// Topics + partitions
	allTopics := s.broker.TopicManager.AllTopics()
	sort.Slice(allTopics, func(i, j int) bool { return allTopics[i].Name < allTopics[j].Name })
	for _, t := range allTopics {
		ti := topicInfo{Name: t.Name}
		for _, p := range t.Partitions {
			hwm := p.HighWaterMark()
			leaderID := s.broker.Controller.GetLeader(t.Name, p.ID())
			key := fmt.Sprintf("%s/%d", t.Name, p.ID())
			pm := partMetrics[key]
			ti.Partitions = append(ti.Partitions, partitionInfo{
				ID:           p.ID(),
				HWM:          hwm,
				LeaderBroker: leaderID,
				MessagesIn:   pm.MessagesIn,
				BytesIn:      pm.BytesIn,
				BytesOut:     pm.BytesOut,
			})
		}
		resp.Topics = append(resp.Topics, ti)
	}

	// Consumer groups + lag
	allOffsets := s.broker.GroupManager.AllGroupOffsets()
	type gKey struct {
		group, topic string
		partition    int32
	}
	type gEntry struct {
		k               gKey
		committed, hwm  int64
	}
	var gEntries []gEntry
	for groupID, topics := range allOffsets {
		for topicName, partitions := range topics {
			for partID, committed := range partitions {
				var hwm int64
				t := s.broker.TopicManager.GetTopic(topicName)
				if t != nil && int(partID) < len(t.Partitions) {
					hwm = t.Partitions[partID].HighWaterMark()
				}
				gEntries = append(gEntries, gEntry{
					k:         gKey{groupID, topicName, partID},
					committed: committed,
					hwm:       hwm,
				})
			}
		}
	}
	sort.Slice(gEntries, func(i, j int) bool {
		a, b := gEntries[i].k, gEntries[j].k
		if a.group != b.group {
			return a.group < b.group
		}
		if a.topic != b.topic {
			return a.topic < b.topic
		}
		return a.partition < b.partition
	})
	for _, e := range gEntries {
		lag := e.hwm - e.committed
		if lag < 0 {
			lag = 0
		}
		resp.ConsumerGroups = append(resp.ConsumerGroups, groupInfo{
			GroupID:         e.k.group,
			Topic:           e.k.topic,
			Partition:       e.k.partition,
			CommittedOffset: e.committed,
			HWM:             e.hwm,
			Lag:             lag,
		})
	}

	return resp
}

// ─── Embedded Dashboard HTML ──────────────────────────────────────────────────

const dashboardHTML = `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>miKago Admin</title>
<style>
*{box-sizing:border-box;margin:0;padding:0}
:root{
  --bg:#0d1117;--surface:#161b22;--surface2:#21262d;
  --border:#30363d;--text:#c9d1d9;--muted:#8b949e;
  --accent:#58a6ff;--green:#3fb950;--orange:#d29922;--red:#f85149;--purple:#bc8cff;
}
body{background:var(--bg);color:var(--text);font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Helvetica,Arial,sans-serif;font-size:14px;line-height:1.5}
.header{background:var(--surface);border-bottom:1px solid var(--border);padding:12px 24px;display:flex;align-items:center;gap:12px}
.header h1{font-size:17px;font-weight:600}
.header .sub{font-size:11px;color:var(--muted);margin-top:1px}
.refresh{margin-left:auto;font-size:11px;color:var(--muted)}
.container{max-width:1440px;margin:0 auto;padding:18px 24px}

/* broker tabs */
.broker-bar{display:flex;gap:8px;margin-bottom:20px;flex-wrap:wrap;align-items:center}
.broker-tab{display:flex;align-items:center;gap:7px;padding:6px 14px;border-radius:20px;border:1px solid var(--border);background:var(--surface);cursor:pointer;font-size:12px;font-weight:500;transition:all .15s}
.broker-tab:hover{border-color:var(--accent);color:var(--accent)}
.broker-tab.active{background:rgba(88,166,255,.12);border-color:var(--accent);color:var(--accent)}
.broker-tab.offline{opacity:.5;cursor:default}
.broker-tab.offline:hover{border-color:var(--border);color:var(--text)}
.dot{width:7px;height:7px;border-radius:50%;flex-shrink:0}
.dot.on{background:var(--green);box-shadow:0 0 5px var(--green)}
.dot.off{background:var(--red)}
.broker-label{font-size:10px;color:var(--muted);margin-left:auto;padding-left:8px}

/* cards */
.cards{display:grid;grid-template-columns:repeat(auto-fill,minmax(150px,1fr));gap:10px;margin-bottom:20px}
.card{background:var(--surface);border:1px solid var(--border);border-radius:8px;padding:14px 16px}
.card .label{font-size:10px;color:var(--muted);text-transform:uppercase;letter-spacing:.5px;margin-bottom:5px}
.card .value{font-size:24px;font-weight:600;font-variant-numeric:tabular-nums}
.value.accent{color:var(--accent)}.value.green{color:var(--green)}.value.orange{color:var(--orange)}.value.purple{color:var(--purple)}

/* cluster node summary strip */
.cluster-strip{display:grid;grid-template-columns:repeat(auto-fill,minmax(220px,1fr));gap:10px;margin-bottom:20px}
.node-card{background:var(--surface);border:1px solid var(--border);border-radius:8px;padding:12px 14px;display:flex;flex-direction:column;gap:4px}
.node-card.offline{opacity:.55}
.node-header{display:flex;align-items:center;gap:7px;font-weight:600;font-size:13px;margin-bottom:4px}
.node-stat{display:flex;justify-content:space-between;font-size:11px;color:var(--muted)}
.node-stat span:last-child{color:var(--text);font-variant-numeric:tabular-nums}

/* charts */
.charts{display:grid;grid-template-columns:1fr 1fr 1fr;gap:14px;margin-bottom:20px}
@media(max-width:960px){.charts{grid-template-columns:1fr}}
.chart-box{background:var(--surface);border:1px solid var(--border);border-radius:8px;padding:14px}
.chart-box h3{font-size:11px;color:var(--muted);text-transform:uppercase;letter-spacing:.5px;margin-bottom:10px}
canvas{width:100%!important;display:block}

/* tables */
.tables{display:grid;grid-template-columns:1fr 1fr;gap:14px}
@media(max-width:960px){.tables{grid-template-columns:1fr}}
.table-box{background:var(--surface);border:1px solid var(--border);border-radius:8px;overflow:hidden}
.table-box h3{font-size:11px;color:var(--muted);text-transform:uppercase;letter-spacing:.5px;padding:12px 14px;border-bottom:1px solid var(--border);background:var(--surface2)}
table{width:100%;border-collapse:collapse}
th{text-align:left;font-size:11px;color:var(--muted);font-weight:500;padding:7px 12px;border-bottom:1px solid var(--border);background:var(--surface2)}
td{padding:7px 12px;border-bottom:1px solid var(--border);font-size:12px;font-variant-numeric:tabular-nums}
tr:last-child td{border-bottom:none}
tr:hover td{background:var(--surface2)}
.badge{display:inline-block;padding:1px 7px;border-radius:20px;font-size:10px;font-weight:600}
.badge.green{background:rgba(63,185,80,.15);color:var(--green)}
.badge.orange{background:rgba(210,153,34,.15);color:var(--orange)}
.badge.red{background:rgba(248,81,73,.15);color:var(--red)}
.badge.blue{background:rgba(88,166,255,.15);color:var(--accent)}
.empty{color:var(--muted);text-align:center;padding:18px!important}
.section-title{font-size:11px;color:var(--muted);text-transform:uppercase;letter-spacing:.5px;margin-bottom:10px;margin-top:4px}
</style>
</head>
<body>
<div class="header">
  <span style="font-size:20px">&#x26A1;</span>
  <div>
    <h1>miKago Admin</h1>
    <div class="sub" id="hdr-sub">Connecting...</div>
  </div>
  <div class="refresh">Updated: <span id="last-updated" style="color:var(--text);font-weight:500">--</span></div>
</div>

<div class="container">

  <!-- Broker selector tabs (hidden when single broker) -->
  <div class="broker-bar" id="broker-bar" style="display:none"></div>

  <!-- Cluster node summary (hidden when single broker) -->
  <div id="cluster-strip-wrap" style="display:none">
    <div class="section-title">Cluster Nodes</div>
    <div class="cluster-strip" id="cluster-strip"></div>
  </div>

  <!-- Overview cards (current broker) -->
  <div class="cards">
    <div class="card"><div class="label">Connections</div><div class="value accent" id="c-conn">--</div></div>
    <div class="card"><div class="label">Topics</div><div class="value" id="c-topics">--</div></div>
    <div class="card"><div class="label">Partitions</div><div class="value" id="c-parts">--</div></div>
    <div class="card"><div class="label">Raft Term</div><div class="value green" id="c-term">--</div></div>
    <div class="card"><div class="label">Elections</div><div class="value purple" id="c-elect">--</div></div>
    <div class="card"><div class="label">Produce req/s</div><div class="value accent" id="c-prate">--</div></div>
    <div class="card"><div class="label">Fetch req/s</div><div class="value accent" id="c-frate">--</div></div>
    <div class="card"><div class="label">Bytes In/s</div><div class="value" id="c-bin">--</div></div>
  </div>

  <!-- Charts -->
  <div class="charts">
    <div class="chart-box"><h3>Request Rate (req/s)</h3><canvas id="chart-requests" data-h="130"></canvas></div>
    <div class="chart-box"><h3>Throughput (bytes/s)</h3><canvas id="chart-bytes" data-h="130"></canvas></div>
    <div class="chart-box"><h3>Produce Latency (ms)</h3><canvas id="chart-latency" data-h="130"></canvas></div>
  </div>

  <!-- Tables -->
  <div class="tables">
    <div class="table-box">
      <h3>Partitions</h3>
      <table><thead><tr><th>Topic</th><th>Part</th><th>Leader</th><th>HWM</th><th>Msgs In</th><th>Bytes In</th></tr></thead>
      <tbody id="partition-tbody"><tr><td colspan="6" class="empty">Loading...</td></tr></tbody></table>
    </div>
    <div class="table-box">
      <h3>Consumer Group Lag</h3>
      <table><thead><tr><th>Group</th><th>Topic</th><th>Part</th><th>Committed</th><th>HWM</th><th>Lag</th></tr></thead>
      <tbody id="group-tbody"><tr><td colspan="6" class="empty">Loading...</td></tr></tbody></table>
    </div>
  </div>

</div><!-- /container -->

<script>
(function() {
  var HIST = 60, INTERVAL = 2000;
  function mkS() { var a=[]; for(var i=0;i<HIST;i++) a.push(0); return a; }

  // Per-broker history keyed by broker_id
  var history = {};
  function getHist(id) {
    if (!history[id]) history[id] = {
      pRate:mkS(), fRate:mkS(), bIn:mkS(), bOut:mkS(), p50:mkS(), p95:mkS(), p99:mkS(),
      prev: null
    };
    return history[id];
  }

  var selectedBroker = null; // null = auto (first online)
  var lastTs = 0;

  function push(arr, v) { arr.push(v); arr.shift(); }

  function fmtNum(n) {
    n = n||0;
    if (n>=1e9) return (n/1e9).toFixed(1)+'G';
    if (n>=1e6) return (n/1e6).toFixed(1)+'M';
    if (n>=1e3) return (n/1e3).toFixed(1)+'K';
    return n.toFixed(0);
  }
  function fmtBytes(n) {
    n=n||0;
    if (n>=1073741824) return (n/1073741824).toFixed(1)+' GiB';
    if (n>=1048576)    return (n/1048576).toFixed(1)+' MiB';
    if (n>=1024)       return (n/1024).toFixed(1)+' KiB';
    return n+' B';
  }
  function setText(id, v) { var e=document.getElementById(id); if(e) e.textContent=v; }

  // ── Chart renderer ────────────────────────────────────────────────────────
  function drawChart(canvasId, lines) {
    var canvas = document.getElementById(canvasId);
    if (!canvas) return;
    var dpr = window.devicePixelRatio||1;
    var rect = canvas.getBoundingClientRect();
    var W = rect.width||300, H = parseInt(canvas.dataset.h)||130;
    canvas.width = W*dpr; canvas.height = H*dpr;
    var ctx = canvas.getContext('2d');
    ctx.scale(dpr, dpr);
    var pad={t:6,r:6,b:28,l:42};
    var gW=W-pad.l-pad.r, gH=H-pad.t-pad.b;
    ctx.fillStyle='#161b22'; ctx.fillRect(0,0,W,H);
    var maxV=0.001;
    lines.forEach(function(l){l.data.forEach(function(v){if(v>maxV)maxV=v;});});
    ctx.strokeStyle='#21262d'; ctx.lineWidth=1;
    for(var gi=0;gi<=4;gi++){var gy=pad.t+(gH/4)*gi;ctx.beginPath();ctx.moveTo(pad.l,gy);ctx.lineTo(pad.l+gW,gy);ctx.stroke();}
    ctx.fillStyle='#8b949e'; ctx.font='9px monospace'; ctx.textAlign='right';
    ctx.fillText(fmtNum(maxV),pad.l-3,pad.t+8);
    ctx.fillText(fmtNum(maxV*0.5),pad.l-3,pad.t+gH/2+4);
    ctx.fillText('0',pad.l-3,pad.t+gH+4);
    lines.forEach(function(l){
      ctx.beginPath();
      l.data.forEach(function(v,i){
        var x=pad.l+(gW/(HIST-1))*i, y=pad.t+gH-(v/maxV)*gH;
        if(i===0)ctx.moveTo(x,y);else ctx.lineTo(x,y);
      });
      ctx.strokeStyle=l.color; ctx.lineWidth=1.5; ctx.stroke();
      ctx.lineTo(pad.l+gW,pad.t+gH); ctx.lineTo(pad.l,pad.t+gH); ctx.closePath();
      ctx.globalAlpha=0.07; ctx.fillStyle=l.color; ctx.fill(); ctx.globalAlpha=1;
    });
    var lx=pad.l; ctx.textAlign='left'; ctx.font='10px sans-serif';
    lines.forEach(function(l){
      ctx.fillStyle=l.color; ctx.fillRect(lx,H-pad.b+8,7,7);
      ctx.fillStyle='#8b949e'; ctx.fillText(l.label,lx+10,H-pad.b+16);
      lx+=ctx.measureText(l.label).width+22;
    });
  }

  // ── Broker tabs ───────────────────────────────────────────────────────────
  function renderBrokerTabs(entries) {
    var bar = document.getElementById('broker-bar');
    if (entries.length <= 1) { bar.style.display='none'; return; }
    bar.style.display='flex';
    bar.innerHTML = '<span class="broker-label">Broker:</span>';
    entries.forEach(function(e) {
      var div = document.createElement('div');
      div.className = 'broker-tab' + (e.online?'':' offline') + (selectedBroker===e.broker_id?' active':'');
      div.innerHTML = '<span class="dot '+(e.online?'on':'off')+'"></span>Node '+e.broker_id;
      if (e.online) {
        div.onclick = function() { selectedBroker = e.broker_id; refresh(); };
      }
      bar.appendChild(div);
    });
  }

  // ── Cluster node strip ────────────────────────────────────────────────────
  function renderClusterStrip(entries) {
    var wrap = document.getElementById('cluster-strip-wrap');
    var strip = document.getElementById('cluster-strip');
    if (entries.length <= 1) { wrap.style.display='none'; return; }
    wrap.style.display='block';
    strip.innerHTML = entries.map(function(e) {
      if (!e.online || !e.metrics) {
        return '<div class="node-card offline">'
          +'<div class="node-header"><span class="dot off"></span>Node '+e.broker_id+' <span style="color:var(--red);font-size:11px;margin-left:auto">offline</span></div>'
          +'<div class="node-stat"><span>URL</span><span>'+e.admin_url+'</span></div>'
          +'</div>';
      }
      var m = e.metrics;
      var active = selectedBroker===e.broker_id ? 'border-color:var(--accent)' : '';
      return '<div class="node-card" style="cursor:pointer;'+active+'" onclick="selectBroker('+e.broker_id+')">'
        +'<div class="node-header"><span class="dot on"></span>Node '+e.broker_id
        +' <span style="font-size:10px;color:var(--muted);margin-left:auto;font-weight:400">term '+m.raft_term+'</span></div>'
        +'<div class="node-stat"><span>Connections</span><span>'+m.active_connections+'</span></div>'
        +'<div class="node-stat"><span>Produce req/s</span><span>--</span></div>'
        +'<div class="node-stat"><span>Topics</span><span>'+(m.topics?m.topics.length:0)+'</span></div>'
        +'</div>';
    }).join('');
  }

  window.selectBroker = function(id) { selectedBroker=id; refresh(); };

  // ── Detail view for selected broker ──────────────────────────────────────
  function renderDetail(entry, dt) {
    if (!entry || !entry.metrics) return;
    var m = entry.metrics;
    var id = entry.broker_id;
    var h = getHist(id);

    var sec = dt>0 ? dt/1000 : 1;
    var pRate=0, fRate=0, bIn=0, bOut=0;
    if (h.prev) {
      pRate = Math.max(0,(m.produce_requests-h.prev.produce_requests)/sec);
      fRate = Math.max(0,(m.fetch_requests  -h.prev.fetch_requests)  /sec);
      bIn   = Math.max(0,(m.bytes_in        -h.prev.bytes_in)        /sec);
      bOut  = Math.max(0,(m.bytes_out       -h.prev.bytes_out)       /sec);
    }
    h.prev = m;
    push(h.pRate, pRate); push(h.fRate, fRate);
    push(h.bIn,  bIn);    push(h.bOut, bOut);
    push(h.p50, m.produce_p50_ms||0);
    push(h.p95, m.produce_p95_ms||0);
    push(h.p99, m.produce_p99_ms||0);

    var topics = m.topics||[];
    var parts=0; topics.forEach(function(t){parts+=t.partitions.length;});

    setText('c-conn',   m.active_connections);
    setText('c-topics', topics.length);
    setText('c-parts',  parts);
    setText('c-term',   m.raft_term||'N/A');
    setText('c-elect',  m.raft_elections||0);
    setText('c-prate',  fmtNum(pRate)+'/s');
    setText('c-frate',  fmtNum(fRate)+'/s');
    setText('c-bin',    fmtBytes(bIn)+'/s');

    drawChart('chart-requests',[{data:h.pRate,color:'#58a6ff',label:'Produce req/s'},{data:h.fRate,color:'#3fb950',label:'Fetch req/s'}]);
    drawChart('chart-bytes',   [{data:h.bIn, color:'#58a6ff',label:'In/s'},{data:h.bOut,color:'#f78166',label:'Out/s'}]);
    drawChart('chart-latency', [{data:h.p99, color:'#f85149',label:'P99'},{data:h.p95,color:'#d29922',label:'P95'},{data:h.p50,color:'#3fb950',label:'P50'}]);

    // Partition table
    var ptBody = document.getElementById('partition-tbody');
    if (ptBody) {
      var rows=[];
      topics.forEach(function(t){
        (t.partitions||[]).forEach(function(p){
          var isL = p.leader_broker_id===m.broker_id;
          rows.push('<tr><td>'+t.name+'</td><td>'+p.id+'</td>'
            +'<td>'+p.leader_broker_id+(isL?' <span class="badge blue">leader</span>':'')+'</td>'
            +'<td>'+p.hwm.toLocaleString()+'</td>'
            +'<td>'+p.messages_in.toLocaleString()+'</td>'
            +'<td>'+fmtBytes(p.bytes_in)+'</td></tr>');
        });
      });
      ptBody.innerHTML = rows.length?rows.join(''):'<tr><td colspan="6" class="empty">No partitions yet</td></tr>';
    }

    // Consumer group table
    var grpBody = document.getElementById('group-tbody');
    if (grpBody) {
      var rows=[];
      (m.consumer_groups||[]).forEach(function(g){
        var cls=g.lag>10000?'red':g.lag>1000?'orange':'green';
        rows.push('<tr><td>'+g.group_id+'</td><td>'+g.topic+'</td><td>'+g.partition+'</td>'
          +'<td>'+g.committed_offset.toLocaleString()+'</td>'
          +'<td>'+g.hwm.toLocaleString()+'</td>'
          +'<td><span class="badge '+cls+'">'+g.lag.toLocaleString()+'</span></td></tr>');
      });
      grpBody.innerHTML = rows.length?rows.join(''):'<tr><td colspan="6" class="empty">No consumer groups</td></tr>';
    }
  }

  // ── Main refresh loop ─────────────────────────────────────────────────────
  function refresh() {
    fetch('/api/cluster')
      .then(function(r){return r.json();})
      .then(function(entries) {
        var now = Date.now();
        var dt = lastTs ? (now-lastTs) : 0;
        lastTs = now;

        // Auto-select first online broker
        if (selectedBroker === null) {
          for (var i=0; i<entries.length; i++) {
            if (entries[i].online) { selectedBroker=entries[i].broker_id; break; }
          }
        }

        renderBrokerTabs(entries);
        renderClusterStrip(entries);

        var sel = null;
        entries.forEach(function(e){ if(e.broker_id===selectedBroker) sel=e; });
        if (!sel) sel = entries[0];
        if (sel) {
          var sub = 'Broker #'+sel.broker_id;
          if (entries.length>1) sub += ' of '+entries.length+'-node cluster';
          setText('hdr-sub', sub);
          renderDetail(sel, dt);
        }

        setText('last-updated', new Date().toLocaleTimeString());
      })
      .catch(function(err){ setText('last-updated','error'); });
  }

  refresh();
  setInterval(refresh, INTERVAL);
  window.addEventListener('resize', refresh);
})();
</script>
</body>
</html>`
