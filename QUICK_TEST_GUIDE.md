# Quick Testing Guide - PostgreSQL Integration Fix

## Pre-Test Checklist

- [ ] PostgreSQL server running
- [ ] Backend dependencies installed
- [ ] Database `coordinator_sim` exists
- [ ] Backend not currently running

## Test Procedure

### Step 1: Start Backend with Debug Logging

```bash
cd d:\FYP\Coordinator Sim
python -m app.main
```

**Expected**: Server starts on http://127.0.0.1:8000

### Step 2: Open Frontend in Browser

Navigate to: `http://127.0.0.1:8000`

**Expected**: Dashboard loads showing 3 stations with charging points and time slots

### Step 3: Manual Slot Occupancy Test

1. Click on any **GREEN (🟢) slot** to toggle it OCCUPIED (🔴)
2. **Watch browser console** for logs (F12 → Console)
3. **Watch backend terminal** for Python logs
4. **Expected logs in backend**:
   ```
   INFO:reservation_service:occupy_time_slot called: point_id=X time_slot=HH:MM ...
   DEBUG:reservation_repo:create_slot_reservation: slot_column=point_id ...
   INFO:reservation_service:slot reservation created successfully: reservation_id=Y ...
   ```

### Step 4: Verify Database Insert

Open a new terminal and run:

```bash
psql -U <your_postgres_user> -d coordinator_sim -c "
SELECT * FROM core.reservations 
WHERE point_id = <the_point_id_you_clicked>
ORDER BY reserved_at DESC LIMIT 1;"
```

**Expected output**:
```
reservation_id | vehicle_id | point_id | status    | scheduled_start           | scheduled_end             | reserved_at
               | NULL       | <id>     | CONFIRMED | 2026-04-27 05:00:00+00    | 2026-04-27 05:30:00+00    | now
```

### Step 5: Run Simulation

1. Click **"Start Simulation"** button
2. Watch 30-60 slots toggle automatically
3. **Observe logs** - should show many slot occupancy operations
4. **Check activity log** on dashboard - should show all changes

### Step 6: Verify All Inserts

After simulation completes:

```bash
psql -U <your_postgres_user> -d coordinator_sim -c "
SELECT COUNT(*) as total_reservations FROM core.reservations WHERE status = 'CONFIRMED';"
```

**Expected**: Count increased by ~30-60 (simulation steps)

### Step 7: Check for Duplicate Prevention

Try clicking the same slot twice in 30 seconds:

1. Click a FREE slot → turns OCCUPIED, logs "created: true"
2. Click the same slot again (while still showing OCCUPIED) → should toggle back to FREE (no DB call)
3. Click it again (now FREE) → DB call made, logs "created: true"

**Expected behavior**: No duplicate reservations in DB for same point_id + time

## What to Look For

### ✅ Success Indicators

- Slot colors update immediately on UI
- Browser console shows POST /occupy-slot with 200 response
- Backend logs show INFO and DEBUG messages (not ERROR)
- New rows appear in core.reservations table
- Timestamps are properly formatted (TIMESTAMPTZ format)

### ❌ Failure Indicators

**Backend log shows**:
```
ERROR:reservation_service:POSTGRES ERROR during slot occupancy: ...
```

→ Check error message for specific PostgreSQL error

**API returns**:
```json
{"detail": "database error: <error message>"}
```

→ Check browser console Network tab for full response

**Backend log shows**:
```
error in create_slot_reservation: ...
```

→ Check if point_id exists in charging_points table

**Database returns no new rows**:
→ Check if connection parameters are correct
→ Check if `ensure_schema()` ran successfully on startup

## Debugging Commands

### View all reservations for a point
```sql
SELECT * FROM core.reservations WHERE point_id = 1 ORDER BY reserved_at DESC;
```

### Count reservations by status
```sql
SELECT status, COUNT(*) FROM core.reservations GROUP BY status;
```

### Find duplicate entries
```sql
SELECT point_id, scheduled_start, COUNT(*) 
FROM core.reservations 
WHERE status = 'CONFIRMED' 
GROUP BY point_id, scheduled_start 
HAVING COUNT(*) > 1;
```

### Clear test data (CAREFUL!)
```sql
DELETE FROM core.reservations;
ALTER SEQUENCE core.reservations_reservation_id_seq RESTART WITH 1;
```

### View database connection info
```bash
python -c "from config.settings import POSTGRES_CONFIG; print(POSTGRES_CONFIG)"
```

## Performance Notes

- First API call may be slightly slower (column detection)
- Subsequent calls should be <100ms
- Database commits are immediate
- No batching - each slot is one transaction

## Common Issues & Quick Fixes

| Issue | Fix |
|-------|-----|
| 500 error when clicking slots | Check PostgreSQL is running; check credentials |
| Slots toggle UI but no DB entry | Check logs for "database error" messages |
| "Column not found" error | Ensure `ensure_schema()` ran successfully |
| Duplicate entries in DB | Old code had bug; fix now prevents it |
| Timestamps wrong timezone | Check your system timezone settings |

## Success Criteria

✅ **PASS** if:
1. Clicking slot adds row to core.reservations
2. Backend logs show no ERROR level messages
3. vehicle_id = NULL in database
4. point_id matches the clicked point
5. scheduled_start & scheduled_end are 30 minutes apart
6. status = CONFIRMED

❌ **FAIL** if:
1. Slots toggle UI but no database entry
2. Backend shows errors when clicking slots
3. Duplicate entries in DB for same slot/time
4. Timestamps are incorrect or not timezone-aware
