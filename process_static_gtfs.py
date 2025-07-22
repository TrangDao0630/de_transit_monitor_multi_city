# process_static_gtfs.py
import zipfile
import pandas as pd
import sqlite3
import os

# --- Configuration ---
STATIC_GTFS_FILES = {
    "mta_nyc_data.zip": { 
        "agency_id": "MTA_NYC",
        "route_file": "mta_nyc_data/routes.txt",
        "stops_file": "mta_nyc_data/stops.txt",
        "trips_file": "mta_nyc_data/trips.txt",
        "stop_times_file": "mta_nyc_data/stop_times.txt"
    },
    "bart_sf_data.zip": { 
        "agency_id": "BART_SF",
        "route_file": "bart_sf_data/routes.txt",
        "stops_file": "bart_sf_data/stops.txt",
        "trips_file": "bart_sf_data/trips.txt",
        "stop_times_file": "bart_sf_data/stop_times.txt"
    }
}
DB_FILE = "transit_performance_data.db" # Unified database for static & real-time
# We'll merge real-time data into this DB later, or you can copy your existing RT DB

def create_static_gtfs_tables(conn):
    """Creates tables for static GTFS data."""
    # routes table
    conn.execute("""
        CREATE TABLE IF NOT EXISTS routes (
            agency_id TEXT NOT NULL,
            route_id TEXT NOT NULL,
            route_short_name TEXT,
            route_long_name TEXT,
            route_type INTEGER,
            PRIMARY KEY (agency_id, route_id)
        );
    """)
    # stops table
    conn.execute("""
        CREATE TABLE IF NOT EXISTS stops (
            agency_id TEXT NOT NULL,
            stop_id TEXT NOT NULL,
            stop_name TEXT,
            stop_lat REAL,
            stop_lon REAL,
            PRIMARY KEY (agency_id, stop_id)
        );
    """)
    # trips table
    conn.execute("""
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
    """)
    # stop_times table
    conn.execute("""
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
    """)
    conn.commit()
    print("Static GTFS tables checked/created.")

def load_static_gtfs_file(zip_path, filename, agency_id, conn, table_name, index_cols=None):
    """Loads a single GTFS file (e.g., routes.txt) from a zip into SQLite."""
    try:
        with zipfile.ZipFile(zip_path, 'r') as z:
            with z.open(filename) as f:
                df = pd.read_csv(f)
                
                # Add agency_id column
                df['agency_id'] = agency_id
                
                # Select only columns present in the table schema to avoid errors
                # and reorder to match the table's column order
                if table_name == 'routes':
                    cols = ['agency_id', 'route_id', 'route_short_name', 'route_long_name', 'route_type']
                elif table_name == 'stops':
                    cols = ['agency_id', 'stop_id', 'stop_name', 'stop_lat', 'stop_lon']
                elif table_name == 'trips':
                    cols = ['agency_id', 'route_id', 'service_id', 'trip_id', 'trip_headsign', 'direction_id']
                elif table_name == 'stop_times':
                    cols = ['agency_id', 'trip_id', 'arrival_time', 'departure_time', 'stop_id', 'stop_sequence']
                else:
                    cols = df.columns.tolist() # Fallback

                df_to_load = df[[col for col in cols if col in df.columns]].copy()
                
                # Drop duplicates based on the primary key for the table
                if index_cols:
                    df_to_load.drop_duplicates(subset=index_cols, inplace=True)

                df_to_load.to_sql(table_name, conn, if_exists='append', index=False)
                print(f"Loaded {len(df_to_load)} records from {filename} into {table_name}.")
    except KeyError:
        print(f"Warning: {filename} not found in {zip_path}. Skipping.")
    except Exception as e:
        print(f"Error loading {filename} from {zip_path}: {e}")

def main():
    print("--- Starting Static GTFS Data Pipeline ---")
    conn = None
    try:
        conn = sqlite3.connect(DB_FILE)
        create_static_gtfs_tables(conn)

        for zip_name, config in STATIC_GTFS_FILES.items():
            zip_path = os.path.join(os.getcwd(), zip_name)
            agency_id = config['agency_id']
            
            print(f"\nProcessing static GTFS for {agency_id} from {zip_name}...")

            # Load routes.txt
            load_static_gtfs_file(zip_path, config['route_file'], agency_id, conn, 'routes', ['agency_id', 'route_id'])
            # Load stops.txt
            load_static_gtfs_file(zip_path, config['stops_file'], agency_id, conn, 'stops', ['agency_id', 'stop_id'])
            # Load trips.txt
            load_static_gtfs_file(zip_path, config['trips_file'], agency_id, conn, 'trips', ['agency_id', 'trip_id'])
            # Load stop_times.txt
            load_static_gtfs_file(zip_path, config['stop_times_file'], agency_id, conn, 'stop_times', ['agency_id', 'trip_id', 'stop_sequence'])
            
            conn.commit() # Commit after each agency's data

    except Exception as e:
        print(f"An error occurred during the main static GTFS pipeline execution: {e}")
    finally:
        if conn:
            conn.close()
            print("Database connection closed.")

if __name__ == "__main__":
    main()