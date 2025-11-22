# AOS-Farm Frontend (Lab GUI)

## Overview

The **Lab GUI** is a simple web interface for managing and running test scenarios. It provides three main lab views (E2E, AAM, DCL) plus run history and monitoring.

## Features

- **E2E Lab**: Run end-to-end scenarios through the full autonomOS pipeline
- **AAM Lab**: Test Adaptive API Mesh independently
- **DCL Lab**: Test Data Connectivity Layer independently
- **Run History**: View past runs with filtering and details
- **Live Monitoring**: Real-time run status and metrics

## Technology Stack

**Recommended Options**:

1. **React + TypeScript** (Modern, type-safe)
2. **Vue 3 + TypeScript** (Lightweight, reactive)
3. **Svelte** (Minimal bundle size)
4. **Vanilla JS + Web Components** (No framework dependency)

Choose based on team preference. The backend API is framework-agnostic.

---

## Project Structure

```
frontend/
├── src/
│   ├── components/          # Reusable UI components
│   │   ├── ScenarioCard.tsx
│   │   ├── RunStatusBadge.tsx
│   │   ├── MetricsDisplay.tsx
│   │   └── ...
│   ├── views/               # Main page views
│   │   ├── E2ELab.tsx
│   │   ├── AAMLab.tsx
│   │   ├── DCLLab.tsx
│   │   └── RunHistory.tsx
│   ├── api/                 # API client for backend
│   │   ├── client.ts
│   │   ├── scenarios.ts
│   │   └── runs.ts
│   ├── hooks/               # Custom React hooks (if React)
│   │   ├── useScenarios.ts
│   │   ├── useRuns.ts
│   │   └── useRunStatus.ts
│   ├── types/               # TypeScript type definitions
│   │   ├── scenario.ts
│   │   ├── run.ts
│   │   └── metrics.ts
│   └── App.tsx              # Main app component
├── public/                  # Static assets
│   ├── index.html
│   └── favicon.ico
├── package.json
└── vite.config.ts / webpack.config.js
```

---

## Setup

### Prerequisites

- Node.js 18+
- npm or yarn

### Installation

```bash
cd frontend
npm install
```

### Configuration

Create `.env.local`:

```bash
VITE_API_BASE_URL=http://localhost:3001
# or REACT_APP_API_BASE_URL for Create React App
```

### Development

```bash
npm run dev
# App runs on http://localhost:3000
```

### Build

```bash
npm run build
# Production build in dist/ or build/
```

---

## Main Views

### 1. E2E Lab

**Path**: `/e2e`

**Features**:
- List of E2E scenarios with tags and descriptions
- Scenario selection dropdown or cards
- Optional scale and duration controls
- **Run** button
- Live run status (pending/running/success/failed)
- Metrics display (AOD, AAM, DCL, Agents)

**Mockup**:
```
┌────────────────────────────────────────────────────┐
│  E2E Lab                                           │
├────────────────────────────────────────────────────┤
│                                                    │
│  Scenarios:                                        │
│  ┌───────────────────────────────────────────┐    │
│  │  [X] Small Clean Enterprise                │    │
│  │      Tags: small, clean, baseline          │    │
│  │                                             │    │
│  │  [ ] Medium Chaotic Environment            │    │
│  │      Tags: medium, high-chaos              │    │
│  └───────────────────────────────────────────┘    │
│                                                    │
│  Scale: [Small ▼]  Duration: [Default ▼]          │
│                                                    │
│  [Run E2E Scenario]                                │
│                                                    │
│  Current Run:                                      │
│  ┌───────────────────────────────────────────┐    │
│  │  Status: Running                           │    │
│  │  AOD: ✓ Complete (98 assets)               │    │
│  │  AAM: ⟳ Running (2/3 connectors)           │    │
│  │  DCL: ⏸ Pending                            │    │
│  │  Agents: ⏸ Pending                         │    │
│  └───────────────────────────────────────────┘    │
└────────────────────────────────────────────────────┘
```

### 2. AAM Lab

**Path**: `/aam`

**Features**:
- List of AAM-specific scenarios
- Scenario selection
- Run button
- AAM-specific metrics (connector availability, retries, errors, drift)

**Key Metrics**:
- Connector availability %
- Error rate %
- Average latency
- Retry count
- Schema drift events detected

### 3. DCL Lab

**Path**: `/dcl`

**Features**:
- List of DCL-specific scenarios
- Scenario selection
- Run button
- DCL-specific metrics (mapping coverage, conflicts, unmapped fields)

**Key Metrics**:
- Mapping coverage %
- Conflicts detected/resolved
- Unmapped fields
- Drift events
- Data quality score

### 4. Run History

**Path**: `/history`

**Features**:
- Table of past runs
- Columns:
  - Timestamp
  - Scenario name
  - Type (E2E, AAM, DCL)
  - Status (success/failed)
  - Duration
  - Key metrics summary
- Filters:
  - By scenario
  - By type
  - By status
  - By date range
- Click row to view full run details

**Mockup**:
```
┌────────────────────────────────────────────────────────────────┐
│  Run History                                                   │
├────────────────────────────────────────────────────────────────┤
│  Filters: Type [All ▼] Status [All ▼] Date [Last 7 days ▼]    │
├────────────────────────────────────────────────────────────────┤
│  Time      │ Scenario         │ Type │ Status  │ Duration │    │
├────────────┼──────────────────┼──────┼─────────┼──────────┤    │
│  14:35:23  │ Small Clean      │ E2E  │ Success │ 5m 23s   │    │
│  13:22:10  │ High Latency     │ AAM  │ Success │ 2m 15s   │    │
│  12:05:45  │ Conflict Test    │ DCL  │ Failed  │ 3m 02s   │    │
└────────────────────────────────────────────────────────────────┘
```

---

## Components

### ScenarioCard

Displays a scenario with description, tags, and select button:

```tsx
interface ScenarioCardProps {
  scenario: Scenario;
  selected: boolean;
  onSelect: (id: string) => void;
}

function ScenarioCard({ scenario, selected, onSelect }: ScenarioCardProps) {
  return (
    <div className={`scenario-card ${selected ? 'selected' : ''}`}
         onClick={() => onSelect(scenario.id)}>
      <h3>{scenario.name}</h3>
      <p>{scenario.description}</p>
      <div className="tags">
        {scenario.tags.map(tag => <span className="tag">{tag}</span>)}
      </div>
    </div>
  );
}
```

### RunStatusBadge

Shows run status with color coding:

```tsx
interface RunStatusBadgeProps {
  status: 'pending' | 'running' | 'success' | 'failed';
}

function RunStatusBadge({ status }: RunStatusBadgeProps) {
  const colors = {
    pending: 'gray',
    running: 'blue',
    success: 'green',
    failed: 'red'
  };

  return (
    <span className={`badge badge-${colors[status]}`}>
      {status.toUpperCase()}
    </span>
  );
}
```

### MetricsDisplay

Shows metrics for a completed run:

```tsx
interface MetricsDisplayProps {
  metrics: RunMetrics;
}

function MetricsDisplay({ metrics }: MetricsDisplayProps) {
  return (
    <div className="metrics">
      {metrics.aod && (
        <div className="metric">
          <h4>AOD</h4>
          <p>Assets: {metrics.aod.assets_discovered}</p>
          <p>Errors: {metrics.aod.errors}</p>
        </div>
      )}
      {metrics.aam && (
        <div className="metric">
          <h4>AAM</h4>
          <p>Availability: {metrics.aam.availability_pct}%</p>
          <p>Retries: {metrics.aam.retry_count}</p>
        </div>
      )}
      {/* DCL, Agents metrics... */}
    </div>
  );
}
```

---

## API Client

### client.ts

Base HTTP client:

```typescript
const API_BASE_URL = import.meta.env.VITE_API_BASE_URL || 'http://localhost:3001';

async function request<T>(
  endpoint: string,
  options?: RequestInit
): Promise<T> {
  const response = await fetch(`${API_BASE_URL}${endpoint}`, {
    headers: {
      'Content-Type': 'application/json',
      ...options?.headers
    },
    ...options
  });

  if (!response.ok) {
    const error = await response.json();
    throw new Error(error.error?.message || 'Request failed');
  }

  return response.json();
}

export const api = {
  get: <T>(endpoint: string) => request<T>(endpoint),
  post: <T>(endpoint: string, body: any) =>
    request<T>(endpoint, { method: 'POST', body: JSON.stringify(body) }),
  put: <T>(endpoint: string, body: any) =>
    request<T>(endpoint, { method: 'PUT', body: JSON.stringify(body) }),
  delete: <T>(endpoint: string) =>
    request<T>(endpoint, { method: 'DELETE' })
};
```

### scenarios.ts

Scenario API methods:

```typescript
export async function listScenarios(filters?: {
  type?: string;
  module?: string;
  tags?: string[];
}): Promise<Scenario[]> {
  const params = new URLSearchParams();
  if (filters?.type) params.set('type', filters.type);
  if (filters?.module) params.set('module', filters.module);

  const result = await api.get<{ scenarios: Scenario[] }>(
    `/api/scenarios?${params}`
  );
  return result.scenarios;
}

export async function getScenario(id: string): Promise<Scenario> {
  return api.get<Scenario>(`/api/scenarios/${id}`);
}
```

### runs.ts

Run API methods:

```typescript
export async function startRun(
  scenarioId: string,
  overrides?: any
): Promise<Run> {
  return api.post<Run>('/api/runs', { scenario_id: scenarioId, overrides });
}

export async function listRuns(filters?: {
  scenario_id?: string;
  type?: string;
  status?: string;
}): Promise<Run[]> {
  const params = new URLSearchParams(filters as any);
  const result = await api.get<{ runs: Run[] }>(`/api/runs?${params}`);
  return result.runs;
}

export async function getRunStatus(runId: string): Promise<RunStatus> {
  return api.get<RunStatus>(`/api/runs/${runId}/status`);
}

export async function getRunMetrics(runId: string): Promise<RunMetrics> {
  return api.get<RunMetrics>(`/api/runs/${runId}/metrics`);
}
```

---

## State Management

### Using React Hooks (Example)

```typescript
function useRunStatus(runId: string | null, pollInterval = 2000) {
  const [status, setStatus] = useState<RunStatus | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<Error | null>(null);

  useEffect(() => {
    if (!runId) return;

    const poll = async () => {
      try {
        setLoading(true);
        const data = await getRunStatus(runId);
        setStatus(data);
        setError(null);

        // Stop polling if run is complete
        if (data.status === 'success' || data.status === 'failed') {
          clearInterval(intervalId);
        }
      } catch (err) {
        setError(err as Error);
      } finally {
        setLoading(false);
      }
    };

    poll(); // Initial fetch
    const intervalId = setInterval(poll, pollInterval);

    return () => clearInterval(intervalId);
  }, [runId, pollInterval]);

  return { status, loading, error };
}
```

---

## Styling

### CSS Framework Options

1. **Tailwind CSS** (utility-first)
2. **Material-UI / MUI** (React components)
3. **Bootstrap** (classic framework)
4. **Custom CSS** (full control)

### Example Styling (Tailwind)

```tsx
function E2ELab() {
  return (
    <div className="container mx-auto p-4">
      <h1 className="text-3xl font-bold mb-4">E2E Lab</h1>

      <div className="grid grid-cols-1 md:grid-cols-2 gap-4 mb-6">
        {scenarios.map(scenario => (
          <ScenarioCard key={scenario.id} scenario={scenario} />
        ))}
      </div>

      <button className="bg-blue-500 text-white px-6 py-2 rounded hover:bg-blue-600">
        Run Scenario
      </button>
    </div>
  );
}
```

---

## Testing

### Unit Tests (Jest + React Testing Library)

```typescript
test('ScenarioCard renders correctly', () => {
  const scenario = {
    id: 'test-1',
    name: 'Test Scenario',
    description: 'Test description',
    tags: ['test']
  };

  render(<ScenarioCard scenario={scenario} selected={false} onSelect={jest.fn()} />);

  expect(screen.getByText('Test Scenario')).toBeInTheDocument();
  expect(screen.getByText('Test description')).toBeInTheDocument();
});
```

### E2E Tests (Playwright or Cypress)

```typescript
test('Run E2E scenario', async ({ page }) => {
  await page.goto('http://localhost:3000/e2e');

  // Select scenario
  await page.click('text=Small Clean Enterprise');

  // Click run
  await page.click('button:has-text("Run E2E Scenario")');

  // Wait for completion
  await page.waitForSelector('text=Success', { timeout: 60000 });

  // Verify metrics displayed
  await expect(page.locator('text=AOD')).toBeVisible();
});
```

---

## Deployment

### Build for Production

```bash
npm run build
```

### Static Hosting (Netlify, Vercel, S3)

1. Build the app
2. Upload `dist/` or `build/` to hosting
3. Configure environment variables (API_BASE_URL)
4. Set up redirects for SPA routing

### Nginx Configuration

```nginx
server {
  listen 80;
  root /var/www/aos-farm-ui;
  index index.html;

  location / {
    try_files $uri /index.html;
  }

  location /api {
    proxy_pass http://localhost:3001;
  }
}
```

---

## Future Enhancements

- **Real-time Updates**: WebSocket for live run updates
- **Charts and Graphs**: Visualize metrics over time
- **Scenario Builder**: Visual scenario editor
- **Comparison View**: Compare results from multiple runs
- **Export Results**: Download run data as JSON/CSV
- **Dark Mode**: Theme toggle
