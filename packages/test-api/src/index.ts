import express, { Request, Response, NextFunction } from 'express';
import { Pool } from 'pg';

const app = express();
app.use(express.json());

const pool = new Pool({
  user: process.env.POSTGRES_USER || 'api_monitor',
  host: process.env.POSTGRES_HOST || 'localhost',
  database: process.env.POSTGRES_DB || 'api_monitor',
  password: process.env.POSTGRES_PASSWORD || 'password',
  port: parseInt(process.env.POSTGRES_PORT || '5432', 10),
});

// Fault state
const activeFaults: Record<string, { targetRoute?: string, rate?: number }> = {};

// Fault Middleware
app.use(async (req: Request, res: Response, next: NextFunction) => {
  const routePath = req.path;

  // Latency Spike
  if (activeFaults['latency_spike'] && (!activeFaults['latency_spike'].targetRoute || routePath.startsWith(activeFaults['latency_spike'].targetRoute))) {
    await new Promise(resolve => setTimeout(resolve, 3000));
  }

  // Error Spike
  if (activeFaults['error_spike'] && (!activeFaults['error_spike'].targetRoute || routePath.startsWith(activeFaults['error_spike'].targetRoute))) {
    const rate = activeFaults['error_spike'].rate || 0.5;
    if (Math.random() < rate) {
      return res.status(500).json({ error: 'Internal Server Error (Injected)' });
    }
  }

  next();
});

// Routes
app.post('/login', (req, res) => {
  const { username, password } = req.body;
  if (username === 'admin' && password === 'admin') {
    return res.json({ token: 'fake-jwt-token' });
  }
  res.status(401).json({ error: 'Unauthorized' });
});

app.get('/orders', async (req, res) => {
  try {
    const result = await pool.query('SELECT * FROM test_api.orders LIMIT 50');
    res.json(result.rows);
  } catch (err: any) {
    res.status(500).json({ error: err.message });
  }
});

app.get('/orders/:id', async (req, res) => {
  try {
    const id = parseInt(req.params.id, 10);
    const result = await pool.query('SELECT * FROM test_api.orders WHERE id = $1', [id]);
    
    // Schema violation fault
    if (activeFaults['schema_violation']) {
       // Return a string instead of number for total_amount, missing status
       return res.json({ id: result.rows[0].id, total_amount: "bad_type_amount" });
    }

    if (result.rows.length === 0) return res.status(404).json({ error: 'Not found' });
    res.json(result.rows[0]);
  } catch (err: any) {
    res.status(500).json({ error: err.message });
  }
});

app.get('/users/:id', async (req, res) => {
  try {
    const id = parseInt(req.params.id, 10);
    // Slow query fault
    let query = 'SELECT * FROM test_api.users WHERE id = $1';
    
    if (activeFaults['slow_query']) {
       // A deliberately bad query to bypass index and cause sequential scan on users and orders
       query = 'SELECT u.*, COUNT(o.id) as order_count FROM test_api.users u LEFT JOIN test_api.orders o ON u.id::TEXT = o.user_id::TEXT WHERE u.id::TEXT = $1::TEXT GROUP BY u.id';
    }

    const result = await pool.query(query, [id]);
    if (result.rows.length === 0) return res.status(404).json({ error: 'Not found' });
    res.json(result.rows[0]);
  } catch (err: any) {
    res.status(500).json({ error: err.message });
  }
});

app.get('/products', (req, res) => {
  res.json([
    { id: 1, name: 'Product A', price: 100 },
    { id: 2, name: 'Product B', price: 200 }
  ]);
});

// Control Endpoint
app.post('/_fault/:type', (req, res) => {
  const type = req.params.type;
  const { action, targetRoute, rate } = req.body; // action: 'enable' | 'disable'

  if (action === 'enable') {
    activeFaults[type] = { targetRoute, rate };
    res.json({ message: `Fault ${type} enabled`, state: activeFaults });
  } else if (action === 'disable') {
    delete activeFaults[type];
    res.json({ message: `Fault ${type} disabled`, state: activeFaults });
  } else {
    res.status(400).json({ error: 'Invalid action' });
  }
});

const PORT = process.env.PORT || 3001;
app.listen(PORT, () => {
  console.log(`Test API listening on port ${PORT}`);
});
