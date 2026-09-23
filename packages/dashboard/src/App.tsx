import { useState, useEffect } from 'react';
import { BrowserRouter as Router, Routes, Route, Link, useLocation } from 'react-router-dom';
import { QueryClient, QueryClientProvider, useQuery, useQueryClient } from '@tanstack/react-query';
import {
  LayoutDashboard, Network, AlertCircle, ShieldAlert, History,
  Activity, ChevronRight, X, CheckCircle, Brain
} from 'lucide-react';

const queryClient = new QueryClient({
  defaultOptions: { queries: { retry: 1, staleTime: 5000 } },
});

const API = (window as any).__ANALYTICS_URL__ || 'http://localhost:3002';
const WS_URL = API.replace('http', 'ws').replace('https', 'wss');

// ─── Colour helpers ───────────────────────────────────────────────────────────
function severityColor(sev: string) {
  const map: Record<string, string> = { critical: '#ef4444', high: '#f97316', medium: '#eab308', low: '#22c55e' };
  return map[sev] || '#6b7280';
}

function scoreColor(score: number) {
  if (score >= 90) return '#22c55e';
  if (score >= 70) return '#eab308';
  return '#ef4444';
}

function formatMs(ms: number | string) {
  const n = parseFloat(ms as string);
  return isNaN(n) ? '—' : `${Math.round(n)}ms`;
}

function timeAgo(iso: string) {
  const diff = (Date.now() - new Date(iso).getTime()) / 1000;
  if (diff < 60) return `${Math.round(diff)}s ago`;
  if (diff < 3600) return `${Math.round(diff / 60)}m ago`;
  return `${Math.round(diff / 3600)}h ago`;
}

// ─── Live WS hook ─────────────────────────────────────────────────────────────
function useLiveMetrics() {
  const qc = useQueryClient();
  useEffect(() => {
    let ws: WebSocket;
    let reconnectTimer: ReturnType<typeof setTimeout>;

    function connect() {
      try {
        ws = new WebSocket(WS_URL);
        ws.onmessage = (event) => {
          try {
            const msg = JSON.parse(event.data);
            if (msg.type === 'metrics') {
              qc.setQueryData(['metrics'], (old: any[]) => {
                const updated = [...(old || [])];
                const idx = updated.findIndex(m => m.route_template === msg.data.route_template);
                if (idx >= 0) updated[idx] = { ...updated[idx], ...msg.data };
                else updated.unshift(msg.data);
                return updated;
              });
            }
            if (msg.type === 'health_score') {
              qc.setQueryData(['health'], msg.data);
            }
            if (msg.type === 'incident_opened') {
              qc.invalidateQueries({ queryKey: ['incidents'] });
            }
          } catch (_) {}
        };
        ws.onerror = () => {};
        ws.onclose = () => {
          reconnectTimer = setTimeout(connect, 3000);
        };
      } catch (_) {}
    }

    connect();
    return () => {
      clearTimeout(reconnectTimer);
      ws?.close();
    };
  }, [qc]);
}

// ─── Sidebar ──────────────────────────────────────────────────────────────────
const NAV_ITEMS = [
  { to: '/', icon: <LayoutDashboard size={18} />, label: 'Overview' },
  { to: '/endpoints', icon: <Network size={18} />, label: 'Endpoints' },
  { to: '/incidents', icon: <AlertCircle size={18} />, label: 'Incidents' },
  { to: '/security', icon: <ShieldAlert size={18} />, label: 'Security' },
  { to: '/deployments', icon: <History size={18} />, label: 'Deployments' },
];

const ActivityIcon = () => (
  <svg xmlns="http://www.w3.org/2000/svg" width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
    <path d="M22 12h-4l-3 9L9 3l-3 9H2"/>
  </svg>
);

const Sidebar = () => {
  const location = useLocation();
  return (
    <div className="w-60 border-r border-border bg-card h-screen flex flex-col shrink-0">
      <div className="p-4 border-b border-border">
        <h1 className="text-base font-bold text-primary flex items-center gap-2">
          <ActivityIcon /> API Intelligence
        </h1>
        <p className="text-[10px] text-muted-foreground mt-0.5 ml-7">Observability Platform</p>
      </div>
      <nav className="flex-1 p-3 space-y-0.5">
        {NAV_ITEMS.map(item => {
          const active = location.pathname === item.to || (item.to !== '/' && location.pathname.startsWith(item.to));
          return (
            <Link key={item.to} to={item.to}
              className={`flex items-center gap-3 px-3 py-2 rounded-md text-sm transition-colors ${active ? 'bg-primary/10 text-primary font-medium' : 'text-muted-foreground hover:bg-muted hover:text-foreground'}`}>
              {item.icon} {item.label}
            </Link>
          );
        })}
      </nav>
      <div className="p-3 border-t border-border text-[10px] text-muted-foreground">
        <div>Analytics API: {API}</div>
      </div>
    </div>
  );
};

// ─── Shared Components ────────────────────────────────────────────────────────
const Card = ({ title, value, subtitle, color }: { title: string; value: string; subtitle: string; color?: string }) => (
  <div className="bg-card p-5 rounded-xl border border-border shadow-sm">
    <p className="text-xs font-medium text-muted-foreground uppercase tracking-wide">{title}</p>
    <div className="mt-2 text-2xl font-bold" style={{ color: color || 'inherit' }}>{value}</div>
    <p className="mt-0.5 text-xs text-muted-foreground">{subtitle}</p>
  </div>
);

const Badge = ({ label, color }: { label: string; color: string }) => (
  <span className="inline-block px-2 py-0.5 rounded-full text-[11px] font-semibold" style={{ background: color + '22', color }}>
    {label}
  </span>
);

const PageHeader = ({ title, subtitle }: { title: string; subtitle?: string }) => (
  <div className="mb-6">
    <h2 className="text-xl font-semibold text-foreground">{title}</h2>
    {subtitle && <p className="text-sm text-muted-foreground mt-1">{subtitle}</p>}
  </div>
);

// ─── Overview Page ────────────────────────────────────────────────────────────
const Overview = () => {
  useLiveMetrics();
  const { data: metrics = [], isLoading: metricsLoading } = useQuery({
    queryKey: ['metrics'],
    queryFn: () => fetch(`${API}/api/metrics`).then(r => { if (!r.ok) throw new Error('API Error'); return r.json(); }),
    refetchInterval: 10_000,
  });
  const { data: health } = useQuery({
    queryKey: ['health'],
    queryFn: () => fetch(`${API}/api/health`).then(r => { if (!r.ok) throw new Error('API Error'); return r.json(); }),
    refetchInterval: 30_000,
  });
  const { data: anomalies = [] } = useQuery({
    queryKey: ['anomalies-recent'],
    queryFn: () => fetch(`${API}/api/anomalies?limit=5&minutes=60`).then(r => { if (!r.ok) throw new Error('API Error'); return r.json(); }),
    refetchInterval: 10_000,
  });

  const overallScore = health?.overall_score ?? 100;

  return (
    <div className="p-6">
      <PageHeader title="Platform Overview" subtitle="Real-time API intelligence and health metrics" />

      <div className="grid grid-cols-2 lg:grid-cols-4 gap-4 mb-6">
        <Card title="System Health" value={`${overallScore.toFixed(1)}%`} subtitle="Overall score" color={scoreColor(overallScore)} />
        <Card title="Performance" value={`${(health?.performance_score ?? 100).toFixed(1)}%`} subtitle="Latency score" color={scoreColor(health?.performance_score ?? 100)} />
        <Card title="Reliability" value={`${(health?.reliability_score ?? 100).toFixed(1)}%`} subtitle="Error rate score" color={scoreColor(health?.reliability_score ?? 100)} />
        <Card title="Active Routes" value={String(metrics.length)} subtitle="Monitored endpoints" />
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-3 gap-4">
        <div className="lg:col-span-2 bg-card border border-border rounded-xl shadow-sm">
          <div className="p-4 border-b border-border flex items-center justify-between">
            <h3 className="font-semibold text-sm">Route Metrics (last 30 min)</h3>
            <Activity size={16} className="text-muted-foreground" />
          </div>
          <div className="overflow-auto">
            {metricsLoading ? (
              <div className="p-4 space-y-2">{[...Array(4)].map((_, i) => <div key={i} className="h-8 bg-muted rounded animate-pulse" />)}</div>
            ) : (
              <table className="w-full text-left text-sm">
                <thead className="text-[11px] text-muted-foreground uppercase">
                  <tr className="border-b border-border">
                    <th className="px-4 py-2">Route</th>
                    <th className="px-4 py-2 text-right">Requests</th>
                    <th className="px-4 py-2 text-right">P95</th>
                    <th className="px-4 py-2 text-right">5xx</th>
                  </tr>
                </thead>
                <tbody>
                  {metrics.map((m: any, i: number) => (
                    <tr key={i} className="border-b border-border/50 last:border-0 hover:bg-muted/30 transition-colors">
                      <td className="px-4 py-2.5 font-mono text-primary text-xs">{m.route_template}</td>
                      <td className="px-4 py-2.5 text-right text-muted-foreground">{parseInt(m.request_count).toLocaleString()}</td>
                      <td className="px-4 py-2.5 text-right" style={{ color: parseFloat(m.p95_latency_ms) > 500 ? '#ef4444' : parseFloat(m.p95_latency_ms) > 200 ? '#f97316' : '#22c55e' }}>
                        {formatMs(m.p95_latency_ms)}
                      </td>
                      <td className="px-4 py-2.5 text-right text-destructive">{m.status_5xx || 0}</td>
                    </tr>
                  ))}
                  {metrics.length === 0 && (
                    <tr><td colSpan={4} className="px-4 py-6 text-center text-muted-foreground text-sm">No traffic yet — start the traffic generator</td></tr>
                  )}
                </tbody>
              </table>
            )}
          </div>
        </div>

        <div className="bg-card border border-border rounded-xl shadow-sm">
          <div className="p-4 border-b border-border flex items-center justify-between">
            <h3 className="font-semibold text-sm">Recent Anomalies</h3>
            <AlertCircle size={16} className="text-muted-foreground" />
          </div>
          <div className="p-3 space-y-2">
            {anomalies.length === 0 ? (
              <p className="text-center py-6 text-muted-foreground text-sm">No anomalies — all clear ✓</p>
            ) : anomalies.map((a: any) => (
              <div key={a.id} className="p-3 rounded-lg border border-border bg-muted/20">
                <div className="flex items-center justify-between mb-1">
                  <Badge label={a.type.replace(/_/g, ' ')} color={severityColor(a.severity)} />
                  <span className="text-[11px] text-muted-foreground">{timeAgo(a.detected_at)}</span>
                </div>
                {a.route_template && <p className="text-[11px] font-mono text-primary">{a.route_template}</p>}
              </div>
            ))}
          </div>
        </div>
      </div>
    </div>
  );
};

// ─── Endpoints Page ───────────────────────────────────────────────────────────
const Endpoints = () => {
  const { data: metrics = [] } = useQuery({
    queryKey: ['metrics'],
    queryFn: () => fetch(`${API}/api/metrics`).then(r => { if (!r.ok) throw new Error('API Error'); return r.json(); }),
    refetchInterval: 10_000,
  });

  return (
    <div className="p-6">
      <PageHeader title="API Endpoints" subtitle="Route-level performance breakdown" />
      <div className="bg-card border border-border rounded-xl shadow-sm overflow-hidden">
        <table className="w-full text-sm">
          <thead className="bg-muted/50 text-[11px] uppercase text-muted-foreground">
            <tr>
              <th className="px-4 py-3 text-left">Route</th>
              <th className="px-4 py-3 text-right">Requests</th>
              <th className="px-4 py-3 text-right">P50</th>
              <th className="px-4 py-3 text-right">P95</th>
              <th className="px-4 py-3 text-right">P99</th>
              <th className="px-4 py-3 text-right">Errors</th>
              <th className="px-4 py-3 text-right">Error %</th>
              <th className="px-4 py-3 text-right">2xx</th>
              <th className="px-4 py-3 text-right">4xx</th>
              <th className="px-4 py-3 text-right">5xx</th>
            </tr>
          </thead>
          <tbody>
            {metrics.map((m: any, i: number) => {
              const errPct = m.request_count > 0 ? (m.error_count / m.request_count * 100).toFixed(1) : '0.0';
              return (
                <tr key={i} className="border-t border-border hover:bg-muted/20 transition-colors">
                  <td className="px-4 py-3 font-mono text-primary text-xs">{m.route_template}</td>
                  <td className="px-4 py-3 text-right">{parseInt(m.request_count).toLocaleString()}</td>
                  <td className="px-4 py-3 text-right text-muted-foreground">{formatMs(m.p50_latency_ms)}</td>
                  <td className="px-4 py-3 text-right font-medium" style={{ color: parseFloat(m.p95_latency_ms) > 500 ? '#ef4444' : parseFloat(m.p95_latency_ms) > 200 ? '#f97316' : 'inherit' }}>
                    {formatMs(m.p95_latency_ms)}
                  </td>
                  <td className="px-4 py-3 text-right text-muted-foreground">{formatMs(m.p99_latency_ms)}</td>
                  <td className="px-4 py-3 text-right text-destructive">{m.error_count || 0}</td>
                  <td className="px-4 py-3 text-right" style={{ color: parseFloat(errPct) > 10 ? '#ef4444' : 'inherit' }}>{errPct}%</td>
                  <td className="px-4 py-3 text-right text-green-500">{m.status_2xx || 0}</td>
                  <td className="px-4 py-3 text-right text-yellow-500">{m.status_4xx || 0}</td>
                  <td className="px-4 py-3 text-right text-red-500">{m.status_5xx || 0}</td>
                </tr>
              );
            })}
            {metrics.length === 0 && (
              <tr><td colSpan={10} className="px-4 py-10 text-center text-muted-foreground">No endpoint data yet</td></tr>
            )}
          </tbody>
        </table>
      </div>
    </div>
  );
};

// ─── Incidents Page ───────────────────────────────────────────────────────────
const IncidentDetail = ({ incident, onClose }: { incident: any; onClose: () => void }) => {
  const qc = useQueryClient();
  const [runningRCA, setRunningRCA] = useState(false);

  const rca = incident.rca_raw ? (() => { try { return JSON.parse(incident.rca_raw); } catch { return null; } })() : null;

  async function triggerRCA() {
    setRunningRCA(true);
    try {
      await fetch(`${API}/api/incidents/${incident.id}/rca`, { method: 'POST' });
      qc.invalidateQueries({ queryKey: ['incidents'] });
    } finally {
      setRunningRCA(false);
    }
  }

  return (
    <div className="fixed inset-0 bg-black/50 z-50 flex items-center justify-center p-4">
      <div className="bg-card border border-border rounded-xl w-full max-w-2xl max-h-[90vh] overflow-auto shadow-xl">
        <div className="p-4 border-b border-border flex items-center justify-between sticky top-0 bg-card">
          <h3 className="font-semibold">Incident #{incident.id}</h3>
          <button onClick={onClose} className="text-muted-foreground hover:text-foreground"><X size={18} /></button>
        </div>
        <div className="p-4 space-y-4">
          <div>
            <p className="text-sm font-medium">{incident.title}</p>
            <p className="text-xs text-muted-foreground mt-1">Opened {timeAgo(incident.opened_at)}{incident.closed_at ? ` · Closed ${timeAgo(incident.closed_at)}` : ' · Open'}</p>
          </div>

          {incident.deployment_version && (
            <div className="bg-yellow-500/10 border border-yellow-500/30 rounded-lg p-3">
              <p className="text-xs font-semibold text-yellow-600">⚠ Linked Deployment</p>
              <p className="text-sm mt-1">{incident.deployment_version} — {incident.deployment_description}</p>
              {incident.deployed_at && <p className="text-xs text-muted-foreground">Deployed {timeAgo(incident.deployed_at)}</p>}
            </div>
          )}

          {incident.rca_summary && (
            <div className="bg-blue-500/10 border border-blue-500/30 rounded-lg p-3">
              <div className="flex items-center gap-2 mb-2">
                <Brain size={14} className="text-blue-400" />
                <p className="text-xs font-semibold text-blue-400">AI Root Cause Analysis</p>
                {rca?.conclusion?.confidence && <Badge label={rca.conclusion.confidence + ' confidence'} color="#3b82f6" />}
              </div>
              <p className="text-sm whitespace-pre-wrap">{incident.rca_summary}</p>
            </div>
          )}

          {rca?.tool_trace && rca.tool_trace.length > 0 && (
            <div>
              <p className="text-xs font-semibold text-muted-foreground mb-2">Tool Call Trace ({rca.tool_trace.length} calls)</p>
              <div className="space-y-2">
                {rca.tool_trace.map((t: any, i: number) => (
                  <div key={i} className="bg-muted/30 rounded-lg p-2 text-xs">
                    <span className="font-mono text-primary">{t.tool}</span>
                    <span className="text-muted-foreground ml-2">{JSON.stringify(t.args).substring(0, 60)}</span>
                    <p className="text-muted-foreground mt-1 truncate">{String(t.result).substring(0, 120)}...</p>
                  </div>
                ))}
              </div>
            </div>
          )}

          <div className="flex gap-2 pt-2">
            <button onClick={triggerRCA} disabled={runningRCA}
              className="flex items-center gap-2 px-3 py-1.5 bg-blue-600 text-white rounded-lg text-sm hover:bg-blue-700 disabled:opacity-50 transition-colors">
              <Brain size={14} />
              {runningRCA ? 'Investigating...' : (incident.rca_summary ? 'Re-run RCA' : 'Run RCA')}
            </button>
            {!incident.closed_at && (
              <button onClick={async () => { await fetch(`${API}/api/incidents/${incident.id}/close`, { method: 'PATCH' }); qc.invalidateQueries({ queryKey: ['incidents'] }); onClose(); }}
                className="flex items-center gap-2 px-3 py-1.5 bg-muted text-muted-foreground rounded-lg text-sm hover:bg-muted/80 transition-colors">
                <CheckCircle size={14} /> Close Incident
              </button>
            )}
          </div>
        </div>
      </div>
    </div>
  );
};

const Incidents = () => {
  const [selected, setSelected] = useState<any>(null);
  const { data: incidents = [] } = useQuery({
    queryKey: ['incidents'],
    queryFn: () => fetch(`${API}/api/incidents?limit=50`).then(r => { if (!r.ok) throw new Error('API Error'); return r.json(); }),
    refetchInterval: 15_000,
  });

  return (
    <div className="p-6">
      {selected && <IncidentDetail incident={selected} onClose={() => { setSelected(null); }} />}
      <PageHeader title="Incidents" subtitle="Automatically detected and correlated operational issues" />
      <div className="bg-card border border-border rounded-xl shadow-sm overflow-hidden">
        <table className="w-full text-sm">
          <thead className="bg-muted/50 text-[11px] uppercase text-muted-foreground">
            <tr>
              <th className="px-4 py-3 text-left">#</th>
              <th className="px-4 py-3 text-left">Title</th>
              <th className="px-4 py-3 text-left">Deployment</th>
              <th className="px-4 py-3 text-left">RCA</th>
              <th className="px-4 py-3 text-left">Status</th>
              <th className="px-4 py-3 text-left">Opened</th>
              <th className="px-4 py-3"></th>
            </tr>
          </thead>
          <tbody>
            {incidents.map((inc: any) => (
              <tr key={inc.id} className="border-t border-border hover:bg-muted/20 transition-colors cursor-pointer" onClick={() => setSelected(inc)}>
                <td className="px-4 py-3 text-muted-foreground text-xs">#{inc.id}</td>
                <td className="px-4 py-3 font-medium max-w-xs truncate">{inc.title}</td>
                <td className="px-4 py-3 text-xs">{inc.deployment_version ? <Badge label={inc.deployment_version} color="#f59e0b" /> : <span className="text-muted-foreground">—</span>}</td>
                <td className="px-4 py-3">{inc.rca_summary ? <Badge label="Done" color="#22c55e" /> : <span className="text-muted-foreground text-xs">Pending</span>}</td>
                <td className="px-4 py-3">{inc.closed_at ? <Badge label="Closed" color="#6b7280" /> : <Badge label="Open" color="#ef4444" />}</td>
                <td className="px-4 py-3 text-xs text-muted-foreground">{timeAgo(inc.opened_at)}</td>
                <td className="px-4 py-3"><ChevronRight size={14} className="text-muted-foreground" /></td>
              </tr>
            ))}
            {incidents.length === 0 && (
              <tr><td colSpan={7} className="px-4 py-10 text-center text-muted-foreground">No incidents — all clear</td></tr>
            )}
          </tbody>
        </table>
      </div>
    </div>
  );
};

// ─── Security Page ────────────────────────────────────────────────────────────
const Security = () => {
  const { data: events = [] } = useQuery({
    queryKey: ['security'],
    queryFn: () => fetch(`${API}/api/security`).then(r => r.json()),
    refetchInterval: 15_000,
  });

  return (
    <div className="p-6">
      <PageHeader title="Security Events" subtitle="Login floods, endpoint enumeration, and abnormal client behavior" />
      <div className="bg-card border border-border rounded-xl shadow-sm overflow-hidden">
        <table className="w-full text-sm">
          <thead className="bg-muted/50 text-[11px] uppercase text-muted-foreground">
            <tr>
              <th className="px-4 py-3 text-left">Type</th>
              <th className="px-4 py-3 text-left">Subtype</th>
              <th className="px-4 py-3 text-left">Client</th>
              <th className="px-4 py-3 text-left">Severity</th>
              <th className="px-4 py-3 text-left">Details</th>
              <th className="px-4 py-3 text-left">Detected</th>
            </tr>
          </thead>
          <tbody>
            {events.map((e: any) => (
              <tr key={e.id} className="border-t border-border hover:bg-muted/20">
                <td className="px-4 py-3 font-mono text-xs">{e.type}</td>
                <td className="px-4 py-3 text-xs">{e.details?.subtype || '—'}</td>
                <td className="px-4 py-3 font-mono text-xs text-muted-foreground">{e.details?.client || '—'}</td>
                <td className="px-4 py-3"><Badge label={e.severity} color={severityColor(e.severity)} /></td>
                <td className="px-4 py-3 text-xs text-muted-foreground max-w-xs truncate">{JSON.stringify(e.details).substring(0, 80)}</td>
                <td className="px-4 py-3 text-xs text-muted-foreground">{timeAgo(e.detected_at)}</td>
              </tr>
            ))}
            {events.length === 0 && (
              <tr><td colSpan={6} className="px-4 py-10 text-center text-muted-foreground">No security events detected</td></tr>
            )}
          </tbody>
        </table>
      </div>
    </div>
  );
};

// ─── Deployments Page ─────────────────────────────────────────────────────────
const Deployments = () => {
  const qc = useQueryClient();
  const [showForm, setShowForm] = useState(false);
  const [form, setForm] = useState({ version: '', commit_sha: '', description: '' });
  const [submitting, setSubmitting] = useState(false);

  const { data: deployments = [] } = useQuery({
    queryKey: ['deployments'],
    queryFn: () => fetch(`${API}/api/deployments`).then(r => r.json()),
    refetchInterval: 30_000,
  });

  async function handleDeploy() {
    if (!form.version) return;
    setSubmitting(true);
    await fetch(`${API}/api/deployments`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(form),
    });
    qc.invalidateQueries({ queryKey: ['deployments'] });
    setForm({ version: '', commit_sha: '', description: '' });
    setShowForm(false);
    setSubmitting(false);
  }

  return (
    <div className="p-6">
      <div className="flex items-center justify-between mb-6">
        <PageHeader title="Deployments" subtitle="Deployment log with linked incident correlation" />
        <button onClick={() => setShowForm(true)}
          className="px-3 py-1.5 bg-primary text-primary-foreground rounded-lg text-sm hover:opacity-90 transition-opacity">
          + Simulate Deploy
        </button>
      </div>

      {showForm && (
        <div className="bg-card border border-border rounded-xl p-4 mb-4 shadow-sm">
          <h4 className="text-sm font-semibold mb-3">Simulate Deployment</h4>
          <div className="grid grid-cols-3 gap-3">
            <input className="px-3 py-1.5 bg-muted rounded-lg text-sm border border-border focus:outline-none"
              placeholder="Version (e.g. v2.1.0)" value={form.version} onChange={e => setForm(f => ({ ...f, version: e.target.value }))} />
            <input className="px-3 py-1.5 bg-muted rounded-lg text-sm border border-border focus:outline-none"
              placeholder="Commit SHA (optional)" value={form.commit_sha} onChange={e => setForm(f => ({ ...f, commit_sha: e.target.value }))} />
            <input className="px-3 py-1.5 bg-muted rounded-lg text-sm border border-border focus:outline-none"
              placeholder="Description" value={form.description} onChange={e => setForm(f => ({ ...f, description: e.target.value }))} />
          </div>
          <div className="flex gap-2 mt-3">
            <button onClick={handleDeploy} disabled={submitting || !form.version}
              className="px-3 py-1.5 bg-primary text-primary-foreground rounded-lg text-sm disabled:opacity-50">
              {submitting ? 'Creating...' : 'Record Deployment'}
            </button>
            <button onClick={() => setShowForm(false)} className="px-3 py-1.5 bg-muted text-muted-foreground rounded-lg text-sm">Cancel</button>
          </div>
        </div>
      )}

      <div className="bg-card border border-border rounded-xl shadow-sm overflow-hidden">
        <table className="w-full text-sm">
          <thead className="bg-muted/50 text-[11px] uppercase text-muted-foreground">
            <tr>
              <th className="px-4 py-3 text-left">Version</th>
              <th className="px-4 py-3 text-left">Commit</th>
              <th className="px-4 py-3 text-left">Description</th>
              <th className="px-4 py-3 text-right">Incidents</th>
              <th className="px-4 py-3 text-left">Deployed</th>
            </tr>
          </thead>
          <tbody>
            {deployments.map((d: any) => (
              <tr key={d.id} className="border-t border-border hover:bg-muted/20">
                <td className="px-4 py-3 font-mono text-primary text-xs font-bold">{d.version}</td>
                <td className="px-4 py-3 font-mono text-xs text-muted-foreground">{d.commit_sha || '—'}</td>
                <td className="px-4 py-3 text-xs max-w-xs truncate">{d.description || '—'}</td>
                <td className="px-4 py-3 text-right">
                  {parseInt(d.incident_count) > 0
                    ? <Badge label={`${d.incident_count} incidents`} color="#ef4444" />
                    : <span className="text-muted-foreground text-xs">—</span>}
                </td>
                <td className="px-4 py-3 text-xs text-muted-foreground">{timeAgo(d.deployed_at)}</td>
              </tr>
            ))}
            {deployments.length === 0 && (
              <tr><td colSpan={5} className="px-4 py-10 text-center text-muted-foreground">No deployments recorded — click "Simulate Deploy" to add one</td></tr>
            )}
          </tbody>
        </table>
      </div>
    </div>
  );
};

// ─── App ──────────────────────────────────────────────────────────────────────
function App() {
  useEffect(() => {
    document.documentElement.classList.add('dark');
  }, []);

  return (
    <QueryClientProvider client={queryClient}>
      <Router>
        <div className="flex min-h-screen bg-background text-foreground">
          <Sidebar />
          <main className="flex-1 overflow-auto">
            <Routes>
              <Route path="/" element={<Overview />} />
              <Route path="/endpoints" element={<Endpoints />} />
              <Route path="/incidents" element={<Incidents />} />
              <Route path="/security" element={<Security />} />
              <Route path="/deployments" element={<Deployments />} />
            </Routes>
          </main>
        </div>
      </Router>
    </QueryClientProvider>
  );
}

export default App;
