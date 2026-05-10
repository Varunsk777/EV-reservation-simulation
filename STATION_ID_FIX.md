# PostgreSQL NOT NULL Constraint Fix - station_id

## Problem
When inserting into the `reservations` table, getting error:
```
null value in column "station_id" violates not-null constraint
```

This occurred because:
1. The database schema has a `station_id` column that is NOT NULL
2. The insert queries were not providing a `station_id` value
3. No step was fetching `station_id` from `charging_points` using `point_id`

## Solution Implemented

### 1. Updated Database Schema
**File**: `core/database.py`

Added `station_id` column to reservations table definition:
```sql
CREATE TABLE IF NOT EXISTS core.reservations (
    reservation_id SERIAL PRIMARY KEY,
    vehicle_id INTEGER REFERENCES core.vehicles(vehicle_id) ON DELETE CASCADE,
    point_id INTEGER NOT NULL REFERENCES core.charging_points(point_id) ON DELETE CASCADE,
    station_id INTEGER NOT NULL REFERENCES core.stations(station_id) ON DELETE CASCADE,  -- ADDED
    status VARCHAR(30) NOT NULL DEFAULT 'CONFIRMED',
    scheduled_start TIMESTAMPTZ NOT NULL,
    scheduled_end TIMESTAMPTZ NOT NULL,
    reserved_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT reservations_valid_window CHECK (scheduled_end > scheduled_start),
    CONSTRAINT reservations_valid_status CHECK (
        status IN ('PENDING', 'CONFIRMED', 'CANCELLED')
    )
);
```

### 2. Added Helper Function
**File**: `repositories/reservation_repo.py`

New function to fetch `station_id` from `point_id`:
```python
def get_station_id_for_point(conn: PgConnection, point_id: int) -> int | None:
    """Fetch station_id from charging_points table using point_id."""
    import logging
    logger = logging.getLogger(__name__)
    
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT station_id
                FROM core.charging_points
                WHERE point_id = %s;
                """,
                (point_id,),
            )
            row = cur.fetchone()
        
        if row is None:
            logger.error("point_id %s not found in charging_points table", point_id)
            return None
        
        station_id = int(row[0])
        logger.debug("found station_id=%s for point_id=%s", station_id, point_id)
        return station_id
    except Exception as e:
        logger.error("error fetching station_id for point_id=%s: %s", point_id, str(e), exc_info=True)
        raise
```

**Features**:
- Uses parameterized query (safe from SQL injection)
- Returns None if point_id not found (can be checked)
- Comprehensive error logging
- Debug logging for traceability

### 3. Updated Insert Functions

#### A. `create_slot_reservation()` - Slot Occupancy Inserts

**Before**:
```python
INSERT INTO core.reservations (
    vehicle_id,
    point_id,
    status,
    scheduled_start,
    scheduled_end,
    reserved_at
)
VALUES (NULL, ?, 'CONFIRMED', ?, ?, CURRENT_TIMESTAMP)
```

**After**:
```python
# 1. Fetch station_id from charging_points
station_id = get_station_id_for_point(conn, point_id)
if station_id is None:
    raise ValueError(f"charging point {point_id} does not exist or has no station_id")

# 2. Include station_id in INSERT
INSERT INTO core.reservations (
    vehicle_id,
    point_id,
    station_id,
    status,
    scheduled_start,
    scheduled_end,
    reserved_at
)
VALUES (NULL, ?, ?, 'CONFIRMED', ?, ?, CURRENT_TIMESTAMP)
```

#### B. `create_reservation()` - General Reservations

Same fix applied - fetches `station_id` before insert and includes it in the query values.

#### C. `list_reservations()` - Query Results

Updated SELECT to include `station_id`:
```python
SELECT
    reservation_id,
    vehicle_id,
    point_id,
    station_id,              # ADDED
    status,
    scheduled_start,
    scheduled_end,
    reserved_at
FROM core.reservations
```

### 4. Error Handling

All functions now:
- Check if `station_id` fetch returns None (point_id doesn't exist)
- Raise ValueError with descriptive message if validation fails
- Log all operations at appropriate levels (INFO/DEBUG/ERROR)
- Include exception chain (`exc_info=True`) for debugging

## Files Modified

1. **core/database.py**
   - Added `station_id` column to `reservations` table schema
   - Added foreign key constraint to `stations` table

2. **repositories/reservation_repo.py**
   - Added `get_station_id_for_point()` helper function
   - Updated `create_reservation()` to fetch and use station_id
   - Updated `create_slot_reservation()` to fetch and use station_id
   - Updated `list_reservations()` to include station_id in SELECT

## Insert Flow Now

```
1. receive point_id from API
   ↓
2. fetch station_id from charging_points WHERE point_id = ?
   ↓
3. validate station_id is not None
   ↓
4. INSERT INTO reservations (vehicle_id, point_id, station_id, ...)
   ↓
5. return reservation_id
```

## Example Logs

**Successful Flow**:
```
DEBUG:reservation_repo:found station_id=1 for point_id=5
DEBUG:reservation_repo:create_slot_reservation: fetched station_id=1 for point_id=5
DEBUG:reservation_repo:executing SQL: INSERT INTO core.reservations with values: point_id=5 station_id=1 start=... end=...
```

**Error Flow**:
```
ERROR:reservation_repo:point_id 999 not found in charging_points table
ERROR:reservation_repo:error in create_slot_reservation: charging point 999 does not exist or has no station_id
```

## Parameterized Queries

All queries use parameterized values (`%s` placeholders) with separate value tuples:

```python
# SAFE - parameterized
cur.execute(
    """SELECT station_id FROM core.charging_points WHERE point_id = %s;""",
    (point_id,)
)

# NOT SAFE - string concatenation (not used)
cur.execute(f"SELECT station_id FROM core.charging_points WHERE point_id = {point_id};")
```

## Performance Notes

- Adds one SELECT query per reservation insert (point_id → station_id lookup)
- Query is indexed on `point_id` (primary key), so very fast (<1ms)
- No N+1 problem - each reservation knows its point_id
- Can be optimized later with caching if needed

## Testing

After applying changes:

1. **Fresh Database**: Run the app - `ensure_schema()` will create tables with station_id
2. **Existing Database**: Run this migration:
   ```sql
   ALTER TABLE core.reservations 
   ADD COLUMN station_id INTEGER NOT NULL REFERENCES core.stations(station_id);
   ```
3. **Test Insert**: Click a slot in UI
   - Should NOT see "null value in column station_id" error
   - Should see new reservation with station_id populated

## Backward Compatibility

⚠️ **Breaking Change**: 
- Old code that inserts without station_id will fail (NOT NULL constraint)
- This is intentional - ensures data integrity

✅ **Positive**:
- All existing queries still work (backward compatible at read level)
- API responses now include station_id (additional information)
- Better referential integrity with stations table

## Related Code Locations

- API endpoint: `api/routes.py` → `occupy_slot()` endpoint
- Service layer: `services/reservation_service.py` → `occupy_time_slot()`
- Repository layer: `repositories/reservation_repo.py` → Insert functions
- Schema: `core/database.py` → `ensure_schema()` function
