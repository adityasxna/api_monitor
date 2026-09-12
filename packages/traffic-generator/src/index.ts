import axios from 'axios';

const GATEWAY_URL = process.env.GATEWAY_URL || 'http://localhost:3000';
const STEADY_STATE_RPS = parseInt(process.env.RPS || '10', 10);

const routes = [
  { path: '/orders', weight: 4 },
  { path: '/products', weight: 3 },
  { path: '/users/1', weight: 2 },
  { path: '/orders/1', weight: 1 },
];

function getRandomRoute() {
  const totalWeight = routes.reduce((sum, r) => sum + r.weight, 0);
  let random = Math.random() * totalWeight;
  for (const route of routes) {
    if (random < route.weight) return route.path;
    random -= route.weight;
  }
  return routes[0].path;
}

async function sendRequest() {
  const path = getRandomRoute();
  try {
    await axios.get(`${GATEWAY_URL}${path}`, { timeout: 5000 });
  } catch (err: any) {
    // Expected to fail sometimes depending on fault injection
  }
}

let active = true;

async function startTraffic() {
  console.log(`Starting steady-state traffic generator to ${GATEWAY_URL} at ~${STEADY_STATE_RPS} RPS`);
  
  const delayMs = 1000 / STEADY_STATE_RPS;
  
  while (active) {
    sendRequest(); // async fire-and-forget
    await new Promise(resolve => setTimeout(resolve, delayMs));
  }
}

startTraffic();

process.on('SIGINT', () => {
  active = false;
  console.log('Stopping traffic generator...');
  setTimeout(() => process.exit(0), 1000);
});
