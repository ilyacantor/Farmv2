# Python / FastAPI Backend Quick Start

## Prerequisites

- Python 3.10+
- pip
- Supabase account with project created

## Setup

### 1. Create Virtual Environment

```bash
cd backend
python -m venv venv

# Activate virtual environment
# On Linux/Mac:
source venv/bin/activate
# On Windows:
# venv\Scripts\activate
```

### 2. Install Dependencies

```bash
pip install -r requirements.txt
```

### 3. Configure Environment

```bash
cp ../config/.env.example .env
```

### 4. Get Supabase Connection Details

**Go to your Supabase Dashboard:**

1. Navigate to: **Project Settings** → **Database**
2. Find the **Connection String** section
3. Copy the **Connection Pooler** string (recommended for FastAPI):
   ```
   postgresql://postgres.[PROJECT-REF]:[YOUR-PASSWORD]@aws-0-[REGION].pooler.supabase.com:6543/postgres
   ```

**Update your `.env` file:**

```bash
# Supabase Configuration
SUPABASE_URL=https://your-project-ref.supabase.co
SUPABASE_KEY=your-service-role-key  # From API settings

# Optional: Direct database URL (if you need it)
SUPABASE_DB_URL=postgresql://postgres.[PROJECT-REF]:[PASSWORD]@db.[PROJECT-REF].supabase.co:5432/postgres
```

**Connection Types:**

- **Connection Pooler** (Port 6543): Best for serverless/FastAPI
  - IPv4: `aws-0-[region].pooler.supabase.com:6543`

- **Direct Connection** (Port 5432): For long-lived connections
  - IPv4: `db.[project-ref].supabase.co:5432`

### 5. Apply Database Migrations

```bash
cd ../database

# If using Supabase CLI:
supabase link --project-ref your-project-ref
supabase db push

# Or manually via SQL Editor in Supabase Dashboard:
# Copy and paste each migration file (001, 002, 003, 004, 005) in order
```

### 6. Run the Server

```bash
cd ../backend

# Development mode (with auto-reload)
python src/main.py

# Or using uvicorn directly:
uvicorn src.main:app --reload --port 3001
```

**Server will start on:** `http://localhost:3001`

## Test the API

### Health Check

```bash
curl http://localhost:3001/health
```

### Interactive API Docs

Open in browser:
- **Swagger UI:** `http://localhost:3001/docs`
- **ReDoc:** `http://localhost:3001/redoc`

### Sync Scenarios to Database

```python
# In Python shell or create a script:
from src.services.scenario_service import ScenarioService
import asyncio

async def sync():
    service = ScenarioService()
    await service.sync_scenarios_to_db()

asyncio.run(sync())
```

### List Scenarios

```bash
curl http://localhost:3001/api/scenarios
```

### Create a Test Run

```bash
curl -X POST http://localhost:3001/api/runs \
  -H "Content-Type: application/json" \
  -d '{"scenario_id": "e2e-small-clean"}'
```

## Project Structure

```
backend/
├── src/
│   ├── main.py              # FastAPI app entry point
│   ├── config.py            # Settings and configuration
│   ├── api/
│   │   └── routes/          # API endpoints
│   │       ├── scenarios.py
│   │       └── runs.py
│   ├── db/
│   │   └── supabase.py      # Database client
│   ├── models/              # Pydantic models
│   │   ├── scenario.py
│   │   └── run.py
│   └── services/            # Business logic
│       ├── scenario_service.py
│       └── run_service.py
├── requirements.txt         # Python dependencies
└── .env                     # Environment configuration
```

## Database Connection Notes

### IPv4 vs IPv6

Supabase **primarily uses IPv4** for database connections:

- **Connection Pooler:** IPv4 only
- **Direct Connection:** IPv4 (some regions may support IPv6)

If you need IPv6 support, check:
1. Supabase Dashboard → Project Settings → Database
2. Look for IPv6 address (if available in your region)

### Connection Pooler vs Direct

**Use Connection Pooler (recommended):**
- Port: `6543`
- Best for: Serverless, FastAPI, short-lived connections
- Mode: Transaction pooling
- Format: `*.pooler.supabase.com:6543`

**Use Direct Connection:**
- Port: `5432`
- Best for: Long-running processes, migrations
- Format: `db.*.supabase.co:5432`

## Next Steps

1. ✅ Server is running
2. ⬜ Implement orchestrator logic
3. ⬜ Implement synthetic data engine
4. ⬜ Implement synthetic HTTP services
5. ⬜ Add chaos engine
6. ⬜ Connect to autonomOS services (AOD, AAM, DCL, Agents)

## Troubleshooting

### Cannot connect to Supabase

Check:
- ✅ `SUPABASE_URL` is correct
- ✅ `SUPABASE_KEY` is the **service role key** (not anon key)
- ✅ Firewall allows outbound connections to Supabase
- ✅ Database is running (check Supabase dashboard)

### Import errors

```bash
# Make sure you're in the backend directory
cd backend

# And virtual environment is activated
source venv/bin/activate

# Reinstall dependencies
pip install -r requirements.txt
```

### Port already in use

```bash
# Change port in .env:
FARM_PORT=3002

# Or specify when running:
uvicorn src.main:app --port 3002
```
