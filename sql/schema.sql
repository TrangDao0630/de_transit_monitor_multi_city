-- =================================================================
-- Schema for Real-Time Transit Data
-- =================================================================

CREATE TABLE IF NOT EXISTS real_time_trip_updates (
    ingestion_timestamp_utc TEXT NOT NULL,
    agency TEXT NOT NULL,
    line_group TEXT,
    trip_id TEXT NOT NULL,
    route_id TEXT,
    direction_id INTEGER,
    current_status TEXT,
    arrival_delay_seconds INTEGER,
    departure_delay_seconds INTEGER,
    predicted_arrival_time_utc TEXT,
    predicted_departure_time_utc TEXT,
    last_update_timestamp_feed TEXT,
    PRIMARY KEY (ingestion_timestamp_utc, agency, trip_id)
);

-- =================================================================
-- Schema for Static GTFS Data
-- =================================================================

CREATE TABLE IF NOT EXISTS routes (
    agency_id TEXT NOT NULL,
    route_id TEXT NOT NULL,
    route_short_name TEXT,
    route_long_name TEXT,
    route_type INTEGER,
    PRIMARY KEY (agency_id, route_id)
);

CREATE TABLE IF NOT EXISTS stops (
    agency_id TEXT NOT NULL,
    stop_id TEXT NOT NULL,
    stop_name TEXT,
    stop_lat REAL,
    stop_lon REAL,
    PRIMARY KEY (agency_id, stop_id)
);

CREATE TABLE IF NOT EXISTS trips (
    agency_id TEXT NOT NULL,
    route_id TEXT NOT NULL,
    service_id TEXT NOT NULL,
    trip_id TEXT NOT NULL,
    trip_headsign TEXT,
    direction_id INTEGER,
    PRIMARY KEY (agency_id, trip_id),
    FOREIGN KEY (agency_id, route_id) REFERENCES routes(agency_id, route_id)
);

CREATE TABLE IF NOT EXISTS stop_times (
    agency_id TEXT NOT NULL,
    trip_id TEXT NOT NULL,
    arrival_time TEXT,
    departure_time TEXT,
    stop_id TEXT NOT NULL,
    stop_sequence INTEGER,
    PRIMARY KEY (agency_id, trip_id, stop_sequence),
    FOREIGN KEY (agency_id, trip_id) REFERENCES trips(agency_id, trip_id),
    FOREIGN KEY (agency_id, stop_id) REFERENCES stops(agency_id, stop_id)
);

-- =================================================================
-- Schema for Analysis Results
-- =================================================================

CREATE TABLE IF NOT EXISTS on_time_performance (
    agency_id TEXT,
    route_id TEXT,
    route_short_name TEXT,
    route_long_name TEXT,
    average_delay_minutes REAL,
    total_trips INTEGER,
    PRIMARY KEY (agency_id, route_id)
);
