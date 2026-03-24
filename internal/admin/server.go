// Package admin provides an HTTP admin interface with a built-in Web Dashboard,
// a Prometheus-compatible /metrics endpoint, and a JSON /api/metrics endpoint.
package admin

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sort"
	"time"

	"github.com/andy/mikago/internal/broker"
	"github.com/andy/mikago/internal/metrics"
)

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

// ─── Server ───────────────────────────────────────────────────────────────────

// Server is the HTTP admin server. It is completely independent of the Kafka
// protocol port and adds zero overhead to the hot path.
type Server struct {
	addr    string
	store   *metrics.Store
	broker  *broker.Broker
	httpSrv *http.Server
}

// New creates an admin Server.
func New(addr string, store *metrics.Store, b *broker.Broker) *Server {
	return &Server{addr: addr, store: store, broker: b}
}

// Start registers handlers and begins serving. It returns immediately;
// the server runs in background goroutines and stops when ctx is cancelled.
func (s *Server) Start(ctx context.Context) {
	mux := http.NewServeMux()
	mux.HandleFunc("/", s.handleDashboard)
	mux.HandleFunc("/metrics", s.handlePrometheus)
	mux.HandleFunc("/api/metrics", s.handleAPIMetrics)

	s.httpSrv = &http.Server{Addr: s.addr, Handler: mux}

	go func() {
		log.Printf("[admin] Dashboard listening on http://%s", s.addr)
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
	type gKey struct{ group, topic string; partition int32 }
	type gEntry struct {
		k               gKey
		committed, hwm  int64
	}
	var entries []gEntry
	for groupID, topics := range allOffsets {
		for topicName, partitions := range topics {
			for partID, committed := range partitions {
				var hwm int64
				t := s.broker.TopicManager.GetTopic(topicName)
				if t != nil && int(partID) < len(t.Partitions) {
					hwm = t.Partitions[partID].HighWaterMark()
				}
				entries = append(entries, gEntry{
					k:         gKey{groupID, topicName, partID},
					committed: committed,
					hwm:       hwm,
				})
			}
		}
	}
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].k.group != entries[j].k.group {
			return entries[i].k.group < entries[j].k.group
		}
		if entries[i].k.topic != entries[j].k.topic {
			return entries[i].k.topic < entries[j].k.topic
		}
		return entries[i].k.partition < entries[j].k.partition
	})
	for _, e := range entries {
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
  --accent:#58a6ff;--green:#3fb950;--orange:#d29922;--red:#f85149;
  --purple:#bc8cff;
}
body{background:var(--bg);color:var(--text);font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Helvetica,Arial,sans-serif;font-size:14px;line-height:1.5}
a{color:var(--accent);text-decoration:none}
.header{background:var(--surface);border-bottom:1px solid var(--border);padding:12px 24px;display:flex;align-items:center;gap:16px}
.header h1{font-size:18px;font-weight:600;color:var(--text)}
.header .sub{font-size:12px;color:var(--muted);margin-top:2px}
.logo{font-size:22px}
.status-dot{width:8px;height:8px;border-radius:50%;background:var(--green);display:inline-block;margin-right:6px;box-shadow:0 0 6px var(--green)}
.container{max-width:1400px;margin:0 auto;padding:20px 24px}
.cards{display:grid;grid-template-columns:repeat(auto-fill,minmax(170px,1fr));gap:12px;margin-bottom:24px}
.card{background:var(--surface);border:1px solid var(--border);border-radius:8px;padding:16px}
.card .label{font-size:11px;color:var(--muted);text-transform:uppercase;letter-spacing:.6px;margin-bottom:6px}
.card .value{font-size:26px;font-weight:600;color:var(--text);font-variant-numeric:tabular-nums}
.card .value.accent{color:var(--accent)}
.card .value.green{color:var(--green)}
.card .value.orange{color:var(--orange)}
.charts{display:grid;grid-template-columns:1fr 1fr 1fr;gap:16px;margin-bottom:24px}
@media(max-width:900px){.charts{grid-template-columns:1fr}}
.chart-box{background:var(--surface);border:1px solid var(--border);border-radius:8px;padding:16px}
.chart-box h3{font-size:12px;color:var(--muted);text-transform:uppercase;letter-spacing:.6px;margin-bottom:12px}
canvas{width:100%!important;display:block}
.tables{display:grid;grid-template-columns:1fr 1fr;gap:16px;margin-bottom:24px}
@media(max-width:900px){.tables{grid-template-columns:1fr}}
.table-box{background:var(--surface);border:1px solid var(--border);border-radius:8px;overflow:hidden}
.table-box h3{font-size:12px;color:var(--muted);text-transform:uppercase;letter-spacing:.6px;padding:14px 16px;border-bottom:1px solid var(--border);background:var(--surface2)}
table{width:100%;border-collapse:collapse}
th{text-align:left;font-size:11px;color:var(--muted);font-weight:500;padding:8px 12px;border-bottom:1px solid var(--border);background:var(--surface2)}
td{padding:8px 12px;border-bottom:1px solid var(--border);font-size:12px;font-variant-numeric:tabular-nums;color:var(--text)}
tr:last-child td{border-bottom:none}
tr:hover td{background:var(--surface2)}
.badge{display:inline-block;padding:1px 7px;border-radius:20px;font-size:10px;font-weight:600;letter-spacing:.3px}
.badge.green{background:rgba(63,185,80,.15);color:var(--green)}
.badge.orange{background:rgba(210,153,34,.15);color:var(--orange)}
.badge.red{background:rgba(248,81,73,.15);color:var(--red)}
.badge.blue{background:rgba(88,166,255,.15);color:var(--accent)}
.empty{color:var(--muted);text-align:center;padding:20px!important}
.refresh{margin-left:auto;font-size:11px;color:var(--muted)}
#last-updated{font-weight:500;color:var(--text)}
</style>
</head>
<body>
<div class="header">
  <span class="logo">&#x26A1;</span>
  <div>
    <h1>miKago Admin</h1>
    <div class="sub">Broker <span id="hdr-broker-id">--</span> &nbsp;|&nbsp; Kafka-compatible message broker</div>
  </div>
  <div class="refresh">Last updated: <span id="last-updated">--</span></div>
</div>

<div class="container">
  <!-- Overview cards -->
  <div class="cards">
    <div class="card">
      <div class="label">Connections</div>
      <div class="value accent" id="c-connections">--</div>
    </div>
    <div class="card">
      <div class="label">Topics</div>
      <div class="value" id="c-topics">--</div>
    </div>
    <div class="card">
      <div class="label">Partitions</div>
      <div class="value" id="c-partitions">--</div>
    </div>
    <div class="card">
      <div class="label">Raft Term</div>
      <div class="value green" id="c-raft-term">--</div>
    </div>
    <div class="card">
      <div class="label">Elections</div>
      <div class="value" id="c-elections">--</div>
    </div>
    <div class="card">
      <div class="label">Produce req/s</div>
      <div class="value accent" id="c-produce-rate">--</div>
    </div>
    <div class="card">
      <div class="label">Fetch req/s</div>
      <div class="value accent" id="c-fetch-rate">--</div>
    </div>
    <div class="card">
      <div class="label">Throughput In</div>
      <div class="value" id="c-bytes-in">--</div>
    </div>
  </div>

  <!-- Charts -->
  <div class="charts">
    <div class="chart-box">
      <h3>Request Rate (req/s)</h3>
      <canvas id="chart-requests" height="140"></canvas>
    </div>
    <div class="chart-box">
      <h3>Throughput (bytes/s)</h3>
      <canvas id="chart-bytes" height="140"></canvas>
    </div>
    <div class="chart-box">
      <h3>Produce Latency (ms)</h3>
      <canvas id="chart-latency" height="140"></canvas>
    </div>
  </div>

  <!-- Tables -->
  <div class="tables">
    <div class="table-box">
      <h3>Partitions</h3>
      <table>
        <thead><tr>
          <th>Topic</th><th>Part</th><th>Leader</th><th>HWM</th><th>Msgs In</th><th>Bytes In</th>
        </tr></thead>
        <tbody id="partition-tbody"><tr><td colspan="6" class="empty">Loading...</td></tr></tbody>
      </table>
    </div>
    <div class="table-box">
      <h3>Consumer Group Lag</h3>
      <table>
        <thead><tr>
          <th>Group</th><th>Topic</th><th>Part</th><th>Committed</th><th>HWM</th><th>Lag</th>
        </tr></thead>
        <tbody id="group-tbody"><tr><td colspan="6" class="empty">Loading...</td></tr></tbody>
      </table>
    </div>
  </div>
</div>

<script>
(function() {
  var HIST = 60;
  var INTERVAL = 2000;

  // Data series: each is an array of HIST floats (oldest first)
  function mkSeries() { var a=[]; for(var i=0;i<HIST;i++) a.push(0); return a; }
  var series = {
    produceRate: mkSeries(), fetchRate: mkSeries(),
    bytesIn: mkSeries(), bytesOut: mkSeries(),
    p50: mkSeries(), p95: mkSeries(), p99: mkSeries()
  };

  var prev = null;

  function push(arr, val) { arr.push(val); arr.shift(); }

  function fmtNum(n) {
    if (n >= 1e9) return (n/1e9).toFixed(1)+'G';
    if (n >= 1e6) return (n/1e6).toFixed(1)+'M';
    if (n >= 1e3) return (n/1e3).toFixed(1)+'K';
    return n.toFixed(0);
  }
  function fmtBytes(n) {
    if (n >= 1073741824) return (n/1073741824).toFixed(1)+' GiB';
    if (n >= 1048576) return (n/1048576).toFixed(1)+' MiB';
    if (n >= 1024) return (n/1024).toFixed(1)+' KiB';
    return n+' B';
  }

  function setText(id, val) {
    var el = document.getElementById(id);
    if (el) el.textContent = val;
  }

  // Draw a multi-series line chart on a canvas element.
  // lines: [{data:[], color:'#hex', label:'name'}]
  function drawChart(canvasId, lines) {
    var canvas = document.getElementById(canvasId);
    if (!canvas) return;
    var dpr = window.devicePixelRatio || 1;
    var rect = canvas.getBoundingClientRect();
    var W = rect.width || 300, H = 140;
    canvas.width = W * dpr; canvas.height = H * dpr;
    var ctx = canvas.getContext('2d');
    ctx.scale(dpr, dpr);

    var pad = {t:8, r:8, b:30, l:44};
    var gW = W - pad.l - pad.r;
    var gH = H - pad.t - pad.b;

    // background
    ctx.fillStyle = '#161b22';
    ctx.fillRect(0, 0, W, H);

    // find max
    var maxV = 0.001;
    lines.forEach(function(l) { l.data.forEach(function(v) { if(v>maxV) maxV=v; }); });

    // grid
    ctx.strokeStyle = '#21262d'; ctx.lineWidth = 1;
    for (var gi = 0; gi <= 4; gi++) {
      var gy = pad.t + (gH/4)*gi;
      ctx.beginPath(); ctx.moveTo(pad.l, gy); ctx.lineTo(pad.l+gW, gy); ctx.stroke();
    }

    // y-axis labels
    ctx.fillStyle = '#8b949e'; ctx.font = '9px monospace'; ctx.textAlign = 'right';
    ctx.fillText(fmtNum(maxV), pad.l-3, pad.t+8);
    ctx.fillText(fmtNum(maxV*0.5), pad.l-3, pad.t+gH/2+4);
    ctx.fillText('0', pad.l-3, pad.t+gH+4);

    // series lines + fill
    lines.forEach(function(l) {
      ctx.beginPath();
      l.data.forEach(function(v, i) {
        var x = pad.l + (gW/(HIST-1))*i;
        var y = pad.t + gH - (v/maxV)*gH;
        if (i===0) ctx.moveTo(x,y); else ctx.lineTo(x,y);
      });
      ctx.strokeStyle = l.color; ctx.lineWidth = 1.5; ctx.stroke();

      // fill under
      ctx.lineTo(pad.l+gW, pad.t+gH);
      ctx.lineTo(pad.l, pad.t+gH);
      ctx.closePath();
      ctx.globalAlpha = 0.06; ctx.fillStyle = l.color; ctx.fill();
      ctx.globalAlpha = 1;
    });

    // legend at bottom
    var lx = pad.l;
    ctx.textAlign = 'left'; ctx.font = '10px sans-serif';
    lines.forEach(function(l) {
      ctx.fillStyle = l.color;
      ctx.fillRect(lx, H-pad.b+10, 8, 8);
      ctx.fillStyle = '#8b949e';
      ctx.fillText(l.label, lx+11, H-pad.b+18);
      lx += ctx.measureText(l.label).width + 24;
    });
  }

  function updateCards(data, dt) {
    setText('hdr-broker-id', data.broker_id);
    setText('c-connections', data.active_connections);
    var topics = data.topics || [];
    setText('c-topics', topics.length);
    var parts = 0; topics.forEach(function(t){ parts += t.partitions.length; });
    setText('c-partitions', parts);
    setText('c-raft-term', data.raft_term || 'N/A');
    setText('c-elections', data.raft_elections || 0);

    var produceRate = 0, fetchRate = 0, bytesInRate = 0, bytesOutRate = 0;
    if (prev && dt > 0) {
      var sec = dt / 1000;
      produceRate = Math.max(0, (data.produce_requests - prev.produce_requests) / sec);
      fetchRate   = Math.max(0, (data.fetch_requests   - prev.fetch_requests)   / sec);
      bytesInRate = Math.max(0, (data.bytes_in         - prev.bytes_in)         / sec);
      bytesOutRate= Math.max(0, (data.bytes_out        - prev.bytes_out)        / sec);
    }
    setText('c-produce-rate', fmtNum(produceRate));
    setText('c-fetch-rate',   fmtNum(fetchRate));
    setText('c-bytes-in',     fmtBytes(bytesInRate)+'/s');

    push(series.produceRate, produceRate);
    push(series.fetchRate, fetchRate);
    push(series.bytesIn, bytesInRate);
    push(series.bytesOut, bytesOutRate);
    push(series.p50, data.produce_p50_ms || 0);
    push(series.p95, data.produce_p95_ms || 0);
    push(series.p99, data.produce_p99_ms || 0);
  }

  function updatePartitionTable(data) {
    var tbody = document.getElementById('partition-tbody');
    if (!tbody) return;
    var topics = data.topics || [];
    var rows = [];
    topics.forEach(function(t) {
      (t.partitions || []).forEach(function(p) {
        var isLeader = p.leader_broker_id === data.broker_id;
        var leaderBadge = isLeader ? ' <span class="badge blue">leader</span>' : '';
        rows.push('<tr><td>'+t.name+'</td><td>'+p.id+'</td>'
          +'<td>'+p.leader_broker_id+leaderBadge+'</td>'
          +'<td>'+p.hwm.toLocaleString()+'</td>'
          +'<td>'+p.messages_in.toLocaleString()+'</td>'
          +'<td>'+fmtBytes(p.bytes_in)+'</td></tr>');
      });
    });
    tbody.innerHTML = rows.length ? rows.join('') : '<tr><td colspan="6" class="empty">No partitions yet</td></tr>';
  }

  function updateGroupTable(data) {
    var tbody = document.getElementById('group-tbody');
    if (!tbody) return;
    var groups = data.consumer_groups || [];
    var rows = [];
    groups.forEach(function(g) {
      var cls = g.lag > 10000 ? 'red' : g.lag > 1000 ? 'orange' : 'green';
      rows.push('<tr><td>'+g.group_id+'</td><td>'+g.topic+'</td><td>'+g.partition+'</td>'
        +'<td>'+g.committed_offset.toLocaleString()+'</td>'
        +'<td>'+g.hwm.toLocaleString()+'</td>'
        +'<td><span class="badge '+cls+'">'+g.lag.toLocaleString()+'</span></td></tr>');
    });
    tbody.innerHTML = rows.length ? rows.join('') : '<tr><td colspan="6" class="empty">No consumer groups</td></tr>';
  }

  var lastTs = 0;

  function refresh() {
    fetch('/api/metrics')
      .then(function(r){ return r.json(); })
      .then(function(data) {
        var now = Date.now();
        var dt = lastTs ? (now - lastTs) : 0;
        lastTs = now;

        updateCards(data, dt);
        updatePartitionTable(data);
        updateGroupTable(data);

        drawChart('chart-requests', [
          {data: series.produceRate, color: '#58a6ff', label: 'Produce req/s'},
          {data: series.fetchRate,   color: '#3fb950', label: 'Fetch req/s'}
        ]);
        drawChart('chart-bytes', [
          {data: series.bytesIn,  color: '#58a6ff', label: 'In/s'},
          {data: series.bytesOut, color: '#f78166', label: 'Out/s'}
        ]);
        drawChart('chart-latency', [
          {data: series.p99, color: '#f85149', label: 'P99 ms'},
          {data: series.p95, color: '#d29922', label: 'P95 ms'},
          {data: series.p50, color: '#3fb950', label: 'P50 ms'}
        ]);

        setText('last-updated', new Date().toLocaleTimeString());
        prev = data;
      })
      .catch(function(err) {
        setText('last-updated', 'error: ' + err.message);
      });
  }

  refresh();
  setInterval(refresh, INTERVAL);
  window.addEventListener('resize', function() {
    if (prev) {
      drawChart('chart-requests', [{data:series.produceRate,color:'#58a6ff',label:'Produce req/s'},{data:series.fetchRate,color:'#3fb950',label:'Fetch req/s'}]);
      drawChart('chart-bytes',    [{data:series.bytesIn,color:'#58a6ff',label:'In/s'},{data:series.bytesOut,color:'#f78166',label:'Out/s'}]);
      drawChart('chart-latency',  [{data:series.p99,color:'#f85149',label:'P99 ms'},{data:series.p95,color:'#d29922',label:'P95 ms'},{data:series.p50,color:'#3fb950',label:'P50 ms'}]);
    }
  });
})();
</script>
</body>
</html>`
