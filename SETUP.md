# AOS-Farm Setup Guide

This guide will walk you through setting up the AOS-Farm system from scratch.

## Prerequisites

- Python 3.10 or higher
- Git
- Supabase account with a project created
- Access to autonomOS services (AOD, AAM, DCL, Agent Orchestrator)

---

## Step 1: Get Your Supabase Service Role Key

You've already got your database connection string. Now you need the **Service Role Key**:

1. Go to your Supabase Dashboard
2. Navigate to **Settings** → **API**
3. Scroll to **Project API keys**
4. Copy the **`service_role`** key (NOT the `anon` key)
   - ⚠️ Keep this secret! It has full database access

---

## Step 2: Clone and Setup Backend

```bash
# Navigate to the repository
cd AOS-Farm/backend

# Create virtual environment
python -m venv venv

# Activate virtual environment
# On Linux/Mac:
source venv/bin/activate
# On Windows:
# venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

---

## Step 3: Configure Environment

The `.env` file has been created for you with your database connection.

**Edit the file and add your Service Role Key:**

```bash
nano .env  # or use any text editor
```

**Update this line:**
```bash
SUPABASE_KEY=YOUR_SERVICE_ROLE_KEY_HERE
```

Replace `YOUR_SERVICE_ROLE_KEY_HERE` with the service role key you copied from the Supabase dashboard.

---

## Step 4: Apply Database Migrations

### Option A: Using Supabase CLI (Recommended)

```bash
# Install Supabase CLI if you haven't
npm install -g supabase

# Navigate to database directory
cd ../database

# Link to your project
supabase link --project-ref fdqldiqmskdihefzjalf

# Apply migrations
supabase db push
```

### Option B: Manual via Supabase Dashboard

1. Go to your Supabase Dashboard
2. Navigate to **SQL Editor**
3. Run each migration file in order:
   - Copy contents of `database/migrations/001_create_core_tables.sql`
   - Paste and run in SQL Editor
   - Repeat for migrations 002, 003, 004, and 005

---

## Step 5: Run Setup Script

This script will:
- Test your database connection
- Sync scenario definitions to the database

```bash
cd ../backend
python scripts/setup.py
```

**Expected output:**
```
============================================================
AOS-Farm Backend Setup
============================================================
Testing Supabase connection...
✅ Database connection successful!
   Found 0 scenarios in database

Syncing scenarios from JSON files to database...
✅ Scenarios synced successfully!

Available scenarios (6):
  - e2e-small-clean (e2e)
  - e2e-medium-chaotic (e2e)
  - aam-high-latency (module)
  - aam-schema-drift (module)
  - dcl-conflict-resolution (module)
  - dcl-data-quality (module)

============================================================
✅ Setup completed successfully!
============================================================
```

---

## Step 6: Start the Backend

```bash
python src/main.py
```

**You should see:**
```
INFO:     Started server process
INFO:     Waiting for application startup.
INFO:     Starting AOS-Farm backend...
INFO:     Environment: Development
INFO:     Database connection established
INFO:     Application startup complete.
INFO:     Uvicorn running on http://0.0.0.0:3001
```

---

## Step 7: Test the API

### In a new terminal:

```bash
# Health check
curl http://localhost:3001/health

# List scenarios
curl http://localhost:3001/api/scenarios

# Get specific scenario
curl http://localhost:3001/api/scenarios/e2e-small-clean

# Create a test run
curl -X POST http://localhost:3001/api/runs \
  -H "Content-Type: application/json" \
  -d '{"scenario_id": "e2e-small-clean"}'
```

### In your browser:

- **Swagger UI:** http://localhost:3001/docs
- **ReDoc:** http://localhost:3001/redoc

---

## Troubleshooting

### "Database connection failed"

**Check:**
1. ✅ `.env` file exists in `backend/` directory
2. ✅ `SUPABASE_KEY` is the **service role key** (not anon key)
3. ✅ `SUPABASE_URL` matches your project: `https://fdqldiqmskdihefzjalf.supabase.co`
4. ✅ Database migrations have been applied
5. ✅ Your IP is not blocked by Supabase firewall

### "Table 'farm_scenarios' does not exist"

**Solution:** Apply database migrations (Step 4)

### "Import errors" or "Module not found"

**Solution:**
```bash
# Make sure you're in the backend directory
cd backend

# Make sure virtual environment is activated
source venv/bin/activate  # or venv\Scripts\activate

# Reinstall dependencies
pip install -r requirements.txt
```

### Port 3001 already in use

**Solution:** Change the port in `.env`:
```bash
FARM_PORT=3002
```

---

## Next Steps

Now that your backend is running:

1. **Configure autonomOS Integration**
   - Update service URLs in `.env`:
     ```bash
     AOD_BASE_URL=http://your-aod-service:port
     AAM_BASE_URL=http://your-aam-service:port
     DCL_BASE_URL=http://your-dcl-service:port
     AGENT_ORCH_BASE_URL=http://your-agent-service:port
     ```

2. **Implement Remaining Components**
   - Orchestrator logic (E2E and module runners)
   - Synthetic data engine
   - Synthetic HTTP services
   - Chaos engine

3. **Set up Frontend**
   - See `frontend/README.md` for instructions

4. **Run Your First Scenario**
   - Use the API or frontend to run `e2e-small-clean`

---

## Project Structure

```
AOS-Farm/
├── backend/
│   ├── src/
│   │   ├── main.py          # FastAPI app
│   │   ├── config.py        # Configuration
│   │   ├── api/             # API routes
│   │   ├── db/              # Database client
│   │   ├── models/          # Pydantic models
│   │   └── services/        # Business logic
│   ├── scripts/
│   │   └── setup.py         # Setup script
│   ├── .env                 # Your configuration (DO NOT COMMIT)
│   └── requirements.txt     # Python dependencies
├── database/
│   └── migrations/          # SQL migrations
├── scenarios/               # Scenario definitions
└── docs/                    # Documentation
```

---

## Security Notes

⚠️ **Important:**

1. **Never commit `.env` file** - It contains sensitive credentials
2. **Service Role Key** has full database access - keep it secret
3. **Use anon key** in frontend applications, not service role key
4. **Enable RLS policies** in production for additional security

---

## Support

For issues:
1. Check the troubleshooting section above
2. Review logs in terminal where backend is running
3. Check Supabase Dashboard → Database → Logs
4. See documentation in `docs/` directory
