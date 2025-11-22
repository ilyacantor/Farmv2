# AOS-Farm Cloud Deployment Guide

This guide explains how to deploy AOS-Farm backend without local development or CLI tools.

## Option 1: Deploy to Render (Recommended - Free Tier Available)

### Step 1: Apply Database Migrations (One-Time Setup)

Since you don't use CLI, do this via Supabase Dashboard:

1. **Go to Supabase SQL Editor:**
   - https://supabase.com/dashboard/project/fdqldiqmskdihefzjalf/sql

2. **Run each migration file in order:**

   **Migration 1 - Core Tables:**
   - Open `database/migrations/001_create_core_tables.sql` from this repository
   - Copy entire contents
   - Paste into SQL Editor
   - Click "Run"

   **Migration 2 - Synthetic Assets:**
   - Open `database/migrations/002_create_synthetic_assets.sql`
   - Copy, paste, run

   **Migration 3 - Synthetic Business:**
   - Open `database/migrations/003_create_synthetic_business.sql`
   - Copy, paste, run

   **Migration 4 - Synthetic Events:**
   - Open `database/migrations/004_create_synthetic_events.sql`
   - Copy, paste, run

   **Migration 5 - Row Level Security:**
   - Open `database/migrations/005_enable_row_level_security.sql`
   - Copy, paste, run

3. **Verify migrations:**
   ```sql
   SELECT * FROM schema_version ORDER BY version;
   ```
   You should see versions 1-5.

### Step 2: Deploy Backend to Render

1. **Go to Render:** https://render.com

2. **Sign up / Log in** (free tier available)

3. **Create New Web Service:**
   - Click "New +" → "Web Service"
   - Connect your GitHub account
   - Select the `AOS-Farm` repository
   - Branch: `claude/setup-aos-farm-repo-01XF7AinKLbGKy58HF3uSKoV` (or your main branch)

4. **Configure Service:**
   ```
   Name: aos-farm-backend
   Region: Oregon (US West) - same as your Supabase
   Branch: claude/setup-aos-farm-repo-01XF7AinKLbGKy58HF3uSKoV
   Root Directory: backend
   Runtime: Python 3
   Build Command: pip install -r requirements.txt
   Start Command: uvicorn src.main:app --host 0.0.0.0 --port $PORT
   Instance Type: Free
   ```

5. **Add Environment Variables:**

   Click "Advanced" → "Add Environment Variable"

   Add each of these:
   ```
   SUPABASE_URL=https://fdqldiqmskdihefzjalf.supabase.co
   SUPABASE_KEY=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImZkcWxkaXFtc2tkaWhlZnpqYWxmIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc2Mzc3NDEzMywiZXhwIjoyMDc5MzUwMTMzfQ.wfkylsbFYqneoFWCaVeu7M2iv1yBdSfjBmVUjyJkXKs
   FARM_PORT=10000
   LOG_LEVEL=info
   DEV_MODE=false
   CORS_ORIGINS=["https://your-frontend-url.com"]
   ```

6. **Click "Create Web Service"**

   Render will automatically:
   - Clone your repository
   - Install dependencies
   - Start the server

7. **Your backend URL will be:**
   ```
   https://aos-farm-backend.onrender.com
   ```

### Step 3: Test Deployment

Once deployed, test:
```bash
curl https://aos-farm-backend.onrender.com/health
curl https://aos-farm-backend.onrender.com/api/scenarios
```

Or visit in browser:
- https://aos-farm-backend.onrender.com/docs

---

## Option 2: Deploy to Railway

1. **Go to Railway:** https://railway.app

2. **Sign up / Log in** (free trial available)

3. **New Project:**
   - Click "New Project"
   - Select "Deploy from GitHub repo"
   - Select `AOS-Farm` repository

4. **Configure:**
   - Root Directory: `backend`
   - Start Command: `uvicorn src.main:app --host 0.0.0.0 --port $PORT`

5. **Add Environment Variables:**
   Same as Render (see above)

6. **Deploy**

---

## Option 3: Deploy to Vercel (with Serverless Functions)

1. **Create `vercel.json` in repository root:**

```json
{
  "builds": [
    {
      "src": "backend/src/main.py",
      "use": "@vercel/python"
    }
  ],
  "routes": [
    {
      "src": "/(.*)",
      "dest": "backend/src/main.py"
    }
  ]
}
```

2. **Deploy:**
   - Go to https://vercel.com
   - Import `AOS-Farm` repository
   - Add environment variables
   - Deploy

---

## Option 4: Google Cloud Run (Containerized)

### Create `backend/Dockerfile`:

```dockerfile
FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENV PORT=8080
CMD uvicorn src.main:app --host 0.0.0.0 --port $PORT
```

### Deploy:

1. **Go to Google Cloud Console**
2. **Cloud Run** → **Create Service**
3. **Deploy from GitHub:**
   - Connect repository
   - Select Dockerfile
   - Set environment variables
   - Deploy

---

## After Deployment: Sync Scenarios

Once your backend is deployed, sync scenarios to database:

### Method 1: Via API (If you add an admin endpoint)

Create a one-time setup endpoint in your deployed backend.

### Method 2: Via Supabase SQL Editor

Run this SQL to insert scenarios manually:

```sql
-- You can copy this from backend/scripts/setup.py
-- Or I can help generate the SQL INSERT statements
```

### Method 3: Use Render Shell (for Render deployment)

1. Go to your Render service
2. Click "Shell" tab
3. Run:
   ```bash
   python scripts/setup.py
   ```

---

## Frontend Deployment (When Ready)

For the frontend (React/Vue), you can use:

- **Vercel** - Best for React/Next.js
- **Netlify** - Good for any static site
- **Render** - Static sites
- **Cloudflare Pages** - Fast global CDN

Just set:
```
VITE_API_BASE_URL=https://aos-farm-backend.onrender.com
```

---

## Recommended: Start with Render

**Why Render?**
- ✅ Free tier available
- ✅ Easy GitHub integration
- ✅ Automatic HTTPS
- ✅ Near your Supabase region (US West)
- ✅ Can run setup script via Shell tab
- ✅ Persistent storage (if needed later)

**Cost:** Free for hobby projects, $7/month for production

---

## Security Notes

⚠️ **Important:**

1. **Don't commit `.env` file** - Use platform environment variables
2. **Service Role Key** should only be in server-side environment
3. **Frontend should use `anon` key**, not service role key
4. **Enable Supabase RLS** for additional security
5. **Add API rate limiting** before going to production

---

## Next Steps

1. ✅ Apply database migrations via Supabase SQL Editor
2. ✅ Deploy backend to Render (or your preferred platform)
3. ✅ Run setup script to sync scenarios
4. ⬜ Configure autonomOS service URLs
5. ⬜ Deploy frontend
6. ⬜ Test end-to-end

---

## Need Help?

If you encounter issues:
1. Check deployment logs in your platform
2. Check Supabase logs: Dashboard → Logs
3. Verify environment variables are set correctly
4. Test health endpoint first: `/health`
