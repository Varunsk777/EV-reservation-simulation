CREATE SCHEMA core;
CREATE SCHEMA cache;
CREATE SCHEMA logs;
CREATE SCHEMA idempotency;
CREATE EXTENSION IF NOT EXISTS btree_gist;
SET search_path TO core;

CREATE TABLE users (
    user_id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    email TEXT UNIQUE NOT NULL,
    phone TEXT UNIQUE,
    password_hash TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE operators (
    operator_id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    contact_number TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE stations (
    station_id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    location TEXT,
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    operator_id INT REFERENCES operators(operator_id),
    operating_hours TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE charging_slots (
    slot_id SERIAL PRIMARY KEY,
    station_id INT REFERENCES stations(station_id) ON DELETE CASCADE,
    slot_number INT NOT NULL,
    power_rating FLOAT CHECK (power_rating > 0),

    UNIQUE (station_id, slot_number)
);

CREATE TABLE vehicles (
    vehicle_id SERIAL PRIMARY KEY,
    user_id INT REFERENCES users(user_id),
    registration_number TEXT UNIQUE NOT NULL,
    vehicle_type TEXT,
    battery_capacity FLOAT CHECK (battery_capacity > 0),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE reservations (
    reservation_id SERIAL PRIMARY KEY,
    vehicle_id INT REFERENCES vehicles(vehicle_id),
    slot_id INT REFERENCES charging_slots(slot_id),

    status TEXT CHECK (
        status IN (
            'pending','confirmed','charging',
            'completed','cancelled','expired','no_show'
        )
    ) DEFAULT 'pending',

    reserved_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    expires_at TIMESTAMP,

    scheduled_start TIMESTAMP,
    scheduled_end TIMESTAMP,

    actual_start TIMESTAMP,
    actual_end TIMESTAMP
);

ALTER TABLE reservations
ADD CONSTRAINT no_overlap
EXCLUDE USING gist (
    slot_id WITH =,
    tsrange(scheduled_start, scheduled_end) WITH &&
)
WHERE (status IN ('confirmed','charging'));


CREATE TABLE charging_sessions (
    session_id SERIAL PRIMARY KEY,
    reservation_id INT REFERENCES reservations(reservation_id),
    station_id INT REFERENCES stations(station_id),
    energy_consumed FLOAT,
    started_at TIMESTAMP,
    ended_at TIMESTAMP,
    cost FLOAT,
    status TEXT
);


SET search_path TO cache;
CREATE TABLE station_state_cache (
    station_id INT PRIMARY KEY,
    available_slots INT,
    avg_wait_time FLOAT,
    queue_length INT,
    last_updated TIMESTAMP
);

SET search_path TO logs;
CREATE TABLE station_events (
    event_id SERIAL PRIMARY KEY,
    station_id INT,
    slot_id INT,
    event_type TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

SET search_path TO idempotency;
CREATE TABLE idempotency_keys (
    key TEXT PRIMARY KEY,
    response JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

SELECT table_schema, table_name
FROM information_schema.tables
ORDER BY table_schema;

-- core schema
SET search_path TO core;

CREATE INDEX idx_station_location ON stations(latitude, longitude);
CREATE INDEX idx_slots_station ON charging_slots(station_id);
CREATE INDEX idx_reservations_slot ON reservations(slot_id);
CREATE INDEX idx_reservations_time ON reservations(scheduled_start, scheduled_end);
CREATE INDEX idx_vehicle_user ON vehicles(user_id);

ALTER TABLE charging_slots ALTER COLUMN station_id SET NOT NULL;
ALTER TABLE reservations ALTER COLUMN slot_id SET NOT NULL;
ALTER TABLE reservations ALTER COLUMN vehicle_id SET NOT NULL;

INSERT INTO core.stations (
    name, location, latitude, longitude, operator_id
)
VALUES (
    'Station A',
    'Chennai',
    13.08,
    80.27,
    NULL
)
RETURNING station_id;


INSERT INTO core.charging_slots (
    station_id,
    slot_number,
    power_rating
)
VALUES
(1, 1, 50),
(1, 2, 60),
(1, 3, 30);


INSERT INTO core.users (name, email, password_hash)
VALUES ('Varun', 'varun@test.com', '123')
RETURNING user_id;

INSERT INTO core.vehicles (
    user_id,
    registration_number,
    vehicle_type,
    battery_capacity
)
VALUES (
    1,
    'TN09AB1234',
    'car',
    60
)
RETURNING vehicle_id;




