# Verification Steps - station_id NOT NULL Fix

## Pre-Flight Check

### 1. Backup Current Database (RECOMMENDED)
```bash
# Backup existing database
pg_dump -U <postgres_user> coordinator_sim > backup_before_fix.sql
```

### 2. Check Current Schema
```bash
psql -U <postgres_user> -d coordinator_sim -c "
\d core.reservations"
```

**Look for**: 
- `station_id` column should already exist (or will be created)
- Should be `integer` type
- Should have `NOT NULL` constraint

### 3. View Existing Reservations (Optional)
```bash
psql -U <postgres_user> -d coordinator_sim -c "
SELECT * FROM core.reservations LIMIT 5;"
```

## Deployment Steps

### Step 1: Update Database Schema

**Option A - Fresh Database** (No existing data):
```bash
# Just restart the app - ensure_schema() will create new table structure
python -m app.main
```

**Option B - Existing Database** (With data):
```bash
# Run migration manually
psql -U <postgres_user> -d coordinator_sim << EOF
-- Drop old reservations and recreate with station_id
BEGIN;

-- Backup old data (just in case)
CREATE TABLE core.reservations_backup AS SELECT * FROM core.reservations;

-- Drop the table
DROP TABLE IF EXISTS core.reservations CASCADE;

-- Recreate with station_id column
CREATE TABLE core.reservations (
    reservation_id SERIAL PRIMARY KEY,
    vehicle_id INTEGER REFERENCES core.vehicles(vehicle_id) ON DELETE CASCADE,
    point_id INTEGER NOT NULL REFERENCES core.charging_points(point_id) ON DELETE CASCADE,
    station_id INTEGER NOT NULL REFERENCES core.stations(station_id) ON DELETE CASCADE,
    status VARCHAR(30) NOT NULL DEFAULT 'CONFIRMED',
    scheduled_start TIMESTAMPTZ NOT NULL,
    scheduled_end TIMESTAMPTZ NOT NULL,
    reserved_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT reservations_valid_window CHECK (scheduled_end > scheduled_start),
    CONSTRAINT reservations_valid_status CHECK (
        status IN ('PENDING', 'CONFIRMED', 'CANCELLED')
    )
);

-- Restore old data if needed (commented out for now)
-- INSERT INTO core.reservations (vehicle_id, point_id, station_id, status, scheduled_start, scheduled_end, reserved_at)
-- SELECT rb.vehicle_id, rb.point_id, cp.station_id, rb.status, rb.scheduled_start, rb.scheduled_end, rb.reserved_at
-- FROM core.reservations_backup rb
-- JOIN core.charging_points cp ON rb.point_id = cp.point_id;

-- Recreate indexes
CREATE INDEX idx_reservations_confirmed_point_window
ON core.reservations (point_id, scheduled_start, scheduled_end)
WHERE UPPER(status) = 'CONFIRMED';

COMMIT;
EOF
```

### Step 2: Verify Schema Update
```bash
psql -U <postgres_user> -d coordinator_sim -c "
SELECT column_name, data_type, is_nullable 
FROM information_schema.columns
WHERE table_schema = 'core' AND table_name = 'reservations'
ORDER BY ordinal_position;"
```

**Expected output**:
```
column_name      | data_type           | is_nullable
-----------------+---------------------+-----------
reservation_id   | integer             | NO
vehicle_id       | integer             | YES
point_id         | integer             | NO
station_id       | integer             | NO        <-- IMPORTANT
status           | character varying   | NO
scheduled_start  | timestamp with tz   | NO
scheduled_end    | timestamp with tz   | NO
reserved_at      | timestamp with tz   | NO
```

## Functional Testing

### Step 1: Start Backend with New Code
```bash
cd d:\FYP\Coordinator Sim
python -m app.main
```

**Expected logs**:
```
INFO:uvicorn.error:Uvicorn running on http://127.0.0.1:8000
```

### Step 2: Test Slot Occupancy Insert

In browser console (F12 → Console):

```javascript
// Manually test the API
fetch('/occupy-slot', {
  method: 'POST',
  headers: { 'Content-Type': 'application/json' },
  body: JSON.stringify({
    point_id: 1,
    time_slot: '10:30'
  })
})
.then(r => r.json())
.then(data => console.log('Response:', data));
```

**Expected response**:
```json
{
  "success": true,
  "created": true,
  "point_id": 1,
  "time_slot": "10:30",
  "reservation_id": 42
}
```

**Backend logs should show**:
```
DEBUG:reservation_repo:found station_id=1 for point_id=1
DEBUG:reservation_repo:fetched station_id=1 for point_id=1
DEBUG:reservation_repo:executing SQL: INSERT INTO core.reservations with values: point_id=1 station_id=1 start=... end=...
INFO:reservation_service:slot reservation created successfully: reservation_id=42 point_id=1 time_slot=10:30
```

### Step 3: Verify Database Insert

```bash
psql -U <postgres_user> -d coordinator_sim -c "
SELECT reservation_id, point_id, station_id, status, scheduled_start, scheduled_end
FROM core.reservations
WHERE point_id = 1
ORDER BY reserved_at DESC
LIMIT 1;"
```

**Expected output**:
```
reservation_id | point_id | station_id | status    | scheduled_start           | scheduled_end
             42|        1|          1| CONFIRMED | 2026-04-27 10:30:00+05:30 | 2026-04-27 11:00:00+05:30
```

✅ **KEY CHECK**: `station_id` should be `1` (not NULL)

### Step 4: UI Slot Click Test

1. Open UI: `http://127.0.0.1:8000`
2. Click a GREEN (🟢) slot to toggle OCCUPIED
3. Should turn RED (🔴) immediately
4. Check logs for success messages
5. Verify database has new row with valid station_id

### Step 5: Run Full Simulation

1. Click **"Start Simulation"** button
2. Watch 30-60 slots toggle
3. Monitor backend logs for errors
4. Check activity log updates

After simulation:
```bash
psql -U <postgres_user> -d coordinator_sim -c "
SELECT COUNT(*) as total_reservations, COUNT(DISTINCT station_id) as stations_involved
FROM core.reservations
WHERE status = 'CONFIRMED' AND scheduled_start > now() - interval '1 hour';"
```

**Expected**: Should show several reservations with multiple stations

### Step 6: Check for NULL Values (Should be 0)

```bash
psql -U <postgres_user> -d coordinator_sim -c "
SELECT COUNT(*) as null_station_ids
FROM core.reservations
WHERE station_id IS NULL;"
```

**Expected output**:
```
null_station_ids
----------------
               0
```

✅ **PASS**: If result is 0, the fix is working!

## Error Scenarios - What to Look For

### Error: "charging point X does not exist"

**Cause**: Trying to occupy a point_id that doesn't exist

**Solution**: Verify point_id exists:
```bash
psql -U <postgres_user> -d coordinator_sim -c "
SELECT point_id, station_id FROM core.charging_points WHERE point_id = X;"
```

### Error: "null value in column station_id"

**Cause**: Old code still being used without the fix

**Solution**: 
1. Restart Python process to reload code
2. Check Python is using updated repository_repo.py

### Error: "Foreign key violation"

**Cause**: station_id value doesn't exist in stations table

**Solution**: Verify charging_points references valid stations:
```bash
psql -U <postgres_user> -d coordinator_sim -c "
SELECT p.point_id, p.station_id, s.station_id as station_exists
FROM core.charging_points p
LEFT JOIN core.stations s ON p.station_id = s.station_id
WHERE s.station_id IS NULL;"
```

## Performance Check

### Before vs After

**Before fix** (should fail):
```
Error: null value in column "station_id" violates not-null constraint
```

**After fix** (should succeed):
- Insert completes in ~10-15ms
- Station_id correctly populated
- No errors in logs

### Load Test

Test with simulation:
```
- Run simulation 3 times (90-180 total slot changes)
- Should complete without errors
- All reservations should have valid station_id
```

## Rollback Plan

If something goes wrong:

```bash
# Restore from backup
psql -U <postgres_user> coordinator_sim < backup_before_fix.sql

# Or restore specific table
psql -U <postgres_user> -d coordinator_sim -c "
DROP TABLE IF EXISTS core.reservations;
CREATE TABLE core.reservations AS SELECT * FROM core.reservations_backup;"
```

## Success Criteria

✅ **ALL of these must be true**:

1. ✅ Database schema has `station_id` column with NOT NULL
2. ✅ `get_station_id_for_point()` function exists in code
3. ✅ Slot occupancy API call succeeds
4. ✅ Backend logs show station_id being fetched
5. ✅ Database has new reservation with non-NULL station_id
6. ✅ Multiple simulations complete without errors
7. ✅ No "null value in column station_id" errors
8. ✅ All existing queries still work (read compatibility)

## Common Issues & Fixes

| Issue | Fix |
|-------|-----|
| Attribute error: `get_station_id_for_point` | Restart Python - code not reloaded |
| Foreign key constraint error | Verify charging_points have valid station_ids |
| "Column station_id does not exist" | Run database migration (Option B above) |
| Simulation won't start | Check backend logs for error details |
| Insert timeout | Increase PostgreSQL work_mem setting |

## Next Steps

After verification passes:
1. Run full simulation suite
2. Monitor logs for any remaining issues
3. Verify frontend displays all data correctly
4. Consider database maintenance (VACUUM, ANALYZE)

---

**Questions?** Check the logs:
```bash
tail -f backend.log | grep -i error
```

Or query recent operations:
```bash
psql -U <postgres_user> -d coordinator_sim -c "
SELECT * FROM core.reservations 
ORDER BY reserved_at DESC LIMIT 20;"
```
