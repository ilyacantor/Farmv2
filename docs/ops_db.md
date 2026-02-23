# Database Operations Guide

## Overview

AOS Farm uses a centralized database management module (`src/farm/db.py`) that provides:
- Singleton connection pool with conservative settings for Supabase pooler
- Exponential backoff on connection failures
- Circuit breaker to prevent connection storms
- Concurrency control via semaphore
- Graceful degradation when database is unavailable

## Environment Variables

All variables are optional with sensible defaults optimized for Supabase:

| Variable | Default | Description |
|----------|---------|-------------|
| `SUPABASE_DB_URL` | - | Primary database URL (takes priority) |
| `DATABASE_URL` | - | Fallback database URL |
| `DB_POOL_MIN` | 0 | Minimum pool size |
| `DB_POOL_MAX` | 2 | Maximum pool size (keep small for Supabase pooler) |
| `DB_CONNECT_TIMEOUT` | 30 | Connection timeout in seconds |
| `DB_COMMAND_TIMEOUT` | 15 | Command timeout in seconds |
| `DB_MAX_INACTIVE_LIFETIME` | 10 | Max idle connection lifetime in seconds |
| `DB_BACKOFF_BASE` | 10 | Initial backoff delay in seconds |
| `DB_BACKOFF_CAP` | 120 | Maximum backoff delay in seconds |
| `DB_FAIL_THRESHOLD` | 8 | Failures before circuit breaker trips |
| `DB_COOLDOWN_SECONDS` | 180 | Circuit breaker cooldown period in seconds |
| `DB_CONCURRENCY` | 2 | Max concurrent DB operations |
| `DB_SIMULATE_DOWN` | false | Force DB failures for testing |

## Circuit Breaker Behavior

The circuit breaker prevents connection storms by:

1. **Counting failures**: Each connection timeout/error increments the failure count
2. **Exponential backoff**: Wait time doubles with each failure (10s → 20s → 40s → ... capped at 120s)
3. **Cooldown period**: After 8 failures, enters 180s cooldown where all DB calls fail fast with 503
4. **Auto-recovery**: After cooldown expires, allows retry; success resets the circuit

## Graceful Degradation

When the database is unavailable:

1. **Startup**: Farm starts successfully even if DB is down
2. **API routes**: Return 503 with `Retry-After` header
3. **Static pages**: Continue to load (HTML/CSS/JS)
4. **Health endpoint**: `/api/health` returns degraded status

Example 503 response:
```json
{
  "error": "Database connection timeout. Backing off 20s.",
  "retry_after": 20,
  "request_id": "abc12345",
  "path": "/api/snapshots",
  "type": "DBUnavailable"
}
```

## Health Check

The `/api/health` endpoint returns database status:

```json
// Healthy
{"status": "healthy", "db": "Healthy"}

// Degraded (503 status)
{"status": "degraded", "db": "Circuit breaker cooldown (120s remaining)"}
```

## Testing DB Unavailability

Set `DB_SIMULATE_DOWN=true` to force all database operations to fail:

```bash
DB_SIMULATE_DOWN=true python -m uvicorn src.main:app --host 0.0.0.0 --port 5000
```

This allows testing:
- Backoff behavior triggers correctly
- Circuit breaker enters cooldown after threshold
- API endpoints return 503 (not 500)
- Static UI continues to load

## Troubleshooting

### "MaxClientsInSessionMode" Error

This occurs when the Supabase session pooler is saturated.

**Cause**: Rapid restarts during development leave orphaned connections.

**Solutions**:
1. Wait 2-5 minutes for connections to auto-expire
2. Manually terminate from Supabase dashboard: Database → Connections → Terminate all
3. Farm will automatically retry with backoff

### Connection Timeouts

**Cause**: Pool creation or connection acquisition takes too long.

**Solutions**:
1. Increase `DB_CONNECT_TIMEOUT` (default: 30s)
2. Reduce `DB_POOL_MAX` to 1 during heavy debugging
3. Check Supabase dashboard for pooler status

### Circuit Breaker Won't Reset

**Cause**: Cooldown period hasn't expired.

**Solutions**:
1. Wait for cooldown (check `/api/health` for remaining time)
2. Restart the application (resets circuit breaker state)
3. Reduce `DB_COOLDOWN_SECONDS` during development

## Best Practices

1. **Keep pool sizes small**: Supabase pooler already pools; app pool should be max 2
2. **Avoid long transactions**: Quick in-and-out to release connections
3. **Use single worker in dev**: `--workers 1` to prevent multiple pools
4. **Monitor health endpoint**: Integrate `/api/health` into monitoring
5. **Don't fight the backoff**: If circuit breaker trips, wait or fix root cause
