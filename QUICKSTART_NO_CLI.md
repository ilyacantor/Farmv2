# AOS-Farm Quick Start (No CLI/Local Development)

## 🎯 Your Project is Ready to Deploy!

All configuration is complete. Follow these 3 simple steps to get AOS-Farm running in the cloud.

---

## Step 1: Setup Database (5 minutes)

1. **Go to Supabase SQL Editor:**
   - https://supabase.com/dashboard/project/fdqldiqmskdihefzjalf/sql

2. **Run the setup script:**
   - Open this file from your repository: `database/SETUP_ALL_MIGRATIONS.sql`
   - Copy the entire contents
   - Paste into the SQL Editor
   - Click **"Run"** button

3. **Verify it worked:**
   - Run this query:
   ```sql
   SELECT * FROM schema_version ORDER BY version;
   ```
   - You should see 5 rows (versions 1-5)

✅ **Database is now ready!**

---

## Step 2: Deploy Backend (10 minutes)

### Option A: Deploy to Render (Recommended - Free)

1. **Go to Render:**
   - https://render.com/
   - Sign up with GitHub (free)

2. **Create New Web Service:**
   - Click **"New +"** → **"Web Service"**
   - Connect your GitHub account
   - Select the **`AOS-Farm`** repository
   - Select branch: `claude/setup-aos-farm-repo-01XF7AinKLbGKy58HF3uSKoV`

3. **Configure Service:**
   ```
   Name: aos-farm-backend
   Region: Oregon (US West)
   Root Directory: backend
   Runtime: Python 3
   Build Command: pip install -r requirements.txt
   Start Command: uvicorn src.main:app --host 0.0.0.0 --port $PORT
   Instance Type: Free
   ```

4. **Add Environment Variables:**

   Click **"Advanced"** → **"Add Environment Variable"**

   Add these variables one by one:

   | Key | Value |
   |-----|-------|
   | `SUPABASE_URL` | `https://fdqldiqmskdihefzjalf.supabase.co` |
   | `SUPABASE_KEY` | `eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImZkcWxkaXFtc2tkaWhlZnpqYWxmIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc2Mzc3NDEzMywiZXhwIjoyMDc5MzUwMTMzfQ.wfkylsbFYqneoFWCaVeu7M2iv1yBdSfjBmVUjyJkXKs` |
   | `LOG_LEVEL` | `info` |
   | `DEV_MODE` | `false` |
   | `CORS_ORIGINS` | `["*"]` |

5. **Click "Create Web Service"**

6. **Wait for deployment** (2-3 minutes)
   - Render will show build logs
   - When complete, you'll see "Live" status

7. **Your backend URL:**
   ```
   https://aos-farm-backend.onrender.com
   ```
   (or whatever name you chose)

✅ **Backend is now live!**

---

## Step 3: Sync Scenarios & Test (5 minutes)

### Sync Scenarios to Database

**Option A: Use Render Shell**

1. Go to your Render service dashboard
2. Click the **"Shell"** tab
3. Run this command:
   ```bash
   python scripts/setup.py
   ```

**Option B: Use your deployed API** (if you add this endpoint)

1. Send a POST request to:
   ```
   POST https://aos-farm-backend.onrender.com/api/admin/sync-scenarios
   ```

### Test Your Deployment

Open these URLs in your browser:

1. **Health Check:**
   ```
   https://aos-farm-backend.onrender.com/health
   ```
   Should return: `{"status": "healthy", ...}`

2. **API Documentation:**
   ```
   https://aos-farm-backend.onrender.com/docs
   ```
   Should show Swagger UI with all endpoints

3. **List Scenarios:**
   ```
   https://aos-farm-backend.onrender.com/api/scenarios
   ```
   Should return 6 scenarios

4. **Create a Test Run:**

   Use the Swagger UI at `/docs` or send:
   ```bash
   curl -X POST https://aos-farm-backend.onrender.com/api/runs \
     -H "Content-Type: application/json" \
     -d '{"scenario_id": "e2e-small-clean"}'
   ```

✅ **Everything is working!**

---

## What You Have Now

✅ **Database:** Fully configured Supabase with all tables
✅ **Backend:** Python/FastAPI deployed and running
✅ **API:** RESTful API with automatic documentation
✅ **Scenarios:** 6 example test scenarios loaded
✅ **HTTPS:** Automatic SSL certificate

---

## Next Steps

### 1. Configure autonomOS Services (Optional)

Once you have AOD, AAM, DCL, and Agent Orchestrator deployed:

1. Go to Render → Your service → **Environment**
2. Add these variables:
   ```
   AOD_BASE_URL=https://your-aod-service.com
   AAM_BASE_URL=https://your-aam-service.com
   DCL_BASE_URL=https://your-dcl-service.com
   AGENT_ORCH_BASE_URL=https://your-agents-service.com
   ```
3. Click **"Save Changes"** (service will restart)

### 2. Deploy Frontend (Future)

When you're ready for the web UI:

1. Deploy to **Vercel** or **Netlify**
2. Set environment variable:
   ```
   VITE_API_BASE_URL=https://aos-farm-backend.onrender.com
   ```
3. Update CORS in backend:
   ```
   CORS_ORIGINS=["https://your-frontend.vercel.app"]
   ```

### 3. Implement Missing Components

Your backend is running but needs these implementations:

- ⬜ **Orchestrator Logic** - E2E and module test runners
- ⬜ **Synthetic Data Engine** - Generate test data
- ⬜ **Synthetic HTTP Services** - Fake CRM/ERP endpoints
- ⬜ **Chaos Engine** - Inject failures and drift

These can be developed and deployed incrementally.

---

## Deployment URLs

Save these for reference:

- **Supabase Dashboard:** https://supabase.com/dashboard/project/fdqldiqmskdihefzjalf
- **Backend (Render):** https://aos-farm-backend.onrender.com
- **API Docs:** https://aos-farm-backend.onrender.com/docs

---

## Troubleshooting

### "Service Unavailable" or "Application Error"

1. Check **Logs** in Render dashboard
2. Verify all environment variables are set
3. Make sure database setup completed successfully

### "Table does not exist"

1. Run `database/SETUP_ALL_MIGRATIONS.sql` again in Supabase SQL Editor
2. Verify with: `SELECT * FROM schema_version;`

### API returns empty scenarios

1. Use Render Shell to run: `python scripts/setup.py`
2. Or check if scenarios were synced: `SELECT * FROM farm_scenarios;` in Supabase

---

## Alternative Deployment Platforms

See `DEPLOYMENT.md` for guides on:
- **Railway** - Similar to Render
- **Google Cloud Run** - Containerized deployment
- **Vercel** - Serverless functions

All work without CLI or local development!

---

## Support

- **Documentation:** See `docs/` directory in repository
- **API Spec:** `docs/api-spec.md`
- **Architecture:** `docs/architecture.md`
- **Scenarios Guide:** `docs/scenarios.md`
