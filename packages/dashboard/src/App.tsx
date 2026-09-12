import React from 'react';
import { BrowserRouter as Router, Routes, Route, Link } from 'react-router-dom';
import { QueryClient, QueryClientProvider, useQuery } from '@tanstack/react-query';
import { LayoutDashboard, Network, AlertCircle, ShieldAlert, History } from 'lucide-react';

const queryClient = new QueryClient();

const Sidebar = () => (
  <div className="w-64 border-r border-border bg-card h-screen flex flex-col">
    <div className="p-4 border-b border-border">
      <h1 className="text-xl font-bold text-primary flex items-center gap-2">
        <ActivityIcon /> API Intel
      </h1>
    </div>
    <nav className="flex-1 p-4 space-y-2">
      <NavItem to="/" icon={<LayoutDashboard size={20} />} label="Overview" />
      <NavItem to="/endpoints" icon={<Network size={20} />} label="Endpoints" />
      <NavItem to="/incidents" icon={<AlertCircle size={20} />} label="Incidents" />
      <NavItem to="/security" icon={<ShieldAlert size={20} />} label="Security" />
      <NavItem to="/deployments" icon={<History size={20} />} label="Deployments" />
    </nav>
  </div>
);

const NavItem = ({ to, icon, label }: { to: string, icon: React.ReactNode, label: string }) => (
  <Link to={to} className="flex items-center gap-3 px-3 py-2 rounded-md hover:bg-muted text-muted-foreground hover:text-foreground transition-colors">
    {icon} {label}
  </Link>
);

const ActivityIcon = () => (
  <svg xmlns="http://www.w3.org/2000/svg" width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
    <path d="M22 12h-4l-3 9L9 3l-3 9H2"/>
  </svg>
);

const Overview = () => {
  const { data, isLoading } = useQuery({
    queryKey: ['metrics'],
    queryFn: async () => {
      const res = await fetch('http://localhost:3002/api/metrics');
      if (!res.ok) throw new Error('Failed to fetch metrics');
      return res.json();
    },
    refetchInterval: 5000,
  });

  React.useEffect(() => {
    const ws = new WebSocket('ws://localhost:3002');
    ws.onmessage = (event) => {
      try {
        const metric = JSON.parse(event.data);
        queryClient.setQueryData(['metrics'], (old: any) => {
          if (!old) return [metric];
          const updated = [...old];
          const idx = updated.findIndex((m: any) => m.route_template === metric.route_template);
          if (idx >= 0) {
            updated[idx] = metric;
          } else {
            updated.push(metric);
          }
          return updated;
        });
      } catch(e) {}
    };
    return () => ws.close();
  }, []);

  return (
    <div className="p-8">
      <h2 className="text-2xl font-semibold mb-6 text-foreground">Platform Overview</h2>
      <div className="grid grid-cols-1 md:grid-cols-3 gap-6 mb-8">
        <Card title="System Health" value="98.5%" subtitle="Overall Score" />
        <Card title="Avg Latency" value="45ms" subtitle="Global P95" />
        <Card title="Active Anomalies" value="0" subtitle="Last 24h" />
      </div>

      <div className="bg-card border border-border rounded-lg shadow-sm">
        <div className="p-4 border-b border-border">
          <h3 className="font-semibold text-lg text-foreground">Recent Endpoints</h3>
        </div>
        <div className="p-4">
          {isLoading ? (
            <div className="animate-pulse space-y-3">
              <div className="h-10 bg-muted rounded"></div>
              <div className="h-10 bg-muted rounded"></div>
              <div className="h-10 bg-muted rounded"></div>
            </div>
          ) : (
            <table className="w-full text-left">
              <thead>
                <tr className="text-muted-foreground border-b border-border">
                  <th className="pb-3 font-medium">Route</th>
                  <th className="pb-3 font-medium text-right">Requests</th>
                  <th className="pb-3 font-medium text-right">P95 Latency</th>
                  <th className="pb-3 font-medium text-right">Errors (5xx)</th>
                </tr>
              </thead>
              <tbody>
                {data?.map((m: any, i: number) => (
                  <tr key={i} className="border-b border-border/50 last:border-0 text-sm">
                    <td className="py-3 font-mono text-primary">{m.route_template}</td>
                    <td className="py-3 text-right">{m.request_count}</td>
                    <td className="py-3 text-right">{Math.round(m.p95_latency_ms)}ms</td>
                    <td className="py-3 text-right text-destructive">{m.status_5xx}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}
        </div>
      </div>
    </div>
  );
};

const Card = ({ title, value, subtitle }: { title: string, value: string, subtitle: string }) => (
  <div className="bg-card p-6 rounded-lg border border-border shadow-sm">
    <h3 className="text-sm font-medium text-muted-foreground">{title}</h3>
    <div className="mt-2 text-3xl font-bold text-foreground">{value}</div>
    <p className="mt-1 text-xs text-muted-foreground">{subtitle}</p>
  </div>
);

const Placeholder = ({ title }: { title: string }) => (
  <div className="p-8">
    <h2 className="text-2xl font-semibold mb-4">{title}</h2>
    <p className="text-muted-foreground">This section is under construction.</p>
  </div>
);

function App() {
  // Force dark mode
  React.useEffect(() => {
    document.documentElement.classList.add('dark');
  }, []);

  return (
    <QueryClientProvider client={queryClient}>
      <Router>
        <div className="flex min-h-screen bg-background">
          <Sidebar />
          <main className="flex-1 overflow-auto">
            <Routes>
              <Route path="/" element={<Overview />} />
              <Route path="/endpoints" element={<Placeholder title="Endpoints" />} />
              <Route path="/incidents" element={<Placeholder title="Incidents" />} />
              <Route path="/security" element={<Placeholder title="Security" />} />
              <Route path="/deployments" element={<Placeholder title="Deployments" />} />
            </Routes>
          </main>
        </div>
      </Router>
    </QueryClientProvider>
  );
}

export default App;
