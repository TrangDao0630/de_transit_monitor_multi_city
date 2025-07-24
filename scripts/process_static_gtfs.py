import zipfile
import pandas as pd
import sqlite3
import os

# --- Robust Path Configuration ---
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)

# --- Configuration ---
STATIC_GTFS_CONFIG = {
    "mta_nyc_data.zip": {
        "agency_id": "MTA_NYC",
        "files": {
            "routes": "mta_nyc_data/routes.txt",
            "stops": "mta_nyc_data/stops.txt",
            "trips": "mta_nyc_data/trips.txt",
            "stop_times": "mta_nyc_data/stop_times.txt"
        }
    },
    "bart_sf_data.zip": {
        "agency_id": "BART_SF",
        "files": {
            "routes": "bart_sf_data/routes.txt",
            "stops": "bart_sf_data/stops.txt",
            "trips": "bart_sf_data/trips.txt",
            "stop_times": "bart_sf_data/stop_times.txt"
        }
    }
}
# Using absolute paths for robustness
DB_FILE = os.path.join(PROJECT_ROOT, "transit_performance_data.db")
SCHEMA_FILE = os.path.join(PROJECT_ROOT, "sql/schema.sql")
DATA_DIR = os.path.join(PROJECT_ROOT, "data")


def setup_database(conn):
    """
    Sets up the database by executing the master schema file.
    """
    try:
        with open(SCHEMA_FILE, 'r') as f:
            conn.executescript(f.read())
        print(f"Database schema from '{SCHEMA_FILE}' processed successfully.")
    except sqlite3.Error as e:
        print(f"Error setting up database from schema: {e}")
    except FileNotFoundError:
        print(f"Error: Schema file not found at '{SCHEMA_FILE}'. Please ensure the file exists.")
        raise

def load_static_gtfs_file(zip_path, filename, agency_id, conn, table_name):
    """
    Loads a single GTFS file, selects only the columns that exist in the DB table,
    and loads them into SQLite.
    """
    # Define the columns that exist in our database tables
    table_columns = {
        'routes': ['agency_id', 'route_id', 'route_short_name', 'route_long_name', 'route_type'],
        'stops': ['agency_id', 'stop_id', 'stop_name', 'stop_lat', 'stop_lon'],
        'trips': ['agency_id', 'route_id', 'service_id', 'trip_id', 'trip_headsign', 'direction_id'],
        'stop_times': ['agency_id', 'trip_id', 'arrival_time', 'departure_time', 'stop_id', 'stop_sequence']
    }

    primary_keys = {
        'routes': ['agency_id', 'route_id'],
        'stops': ['agency_id', 'stop_id'],
        'trips': ['agency_id', 'trip_id'],
        'stop_times': ['agency_id', 'trip_id', 'stop_sequence']
    }

    try:
        with zipfile.ZipFile(zip_path, 'r') as z:
            with z.open(filename) as f:
                df = pd.read_csv(f, dtype=str)
                df['agency_id'] = agency_id
                df.rename(columns=lambda x: x.strip(), inplace=True)

                # Get the list of expected columns for this table from our schema definition
                expected_cols = table_columns.get(table_name)
                if not expected_cols:
                    print(f"Warning: No column schema defined for table '{table_name}'. Skipping.")
                    return

                # ** THE FIX IS HERE **
                # Filter the DataFrame to only include columns that are both in the file AND in our schema
                cols_to_load = [col for col in expected_cols if col in df.columns]
                df_filtered = df[cols_to_load].copy()

                # Drop duplicates based on the primary key
                pk = primary_keys.get(table_name)
                if pk:
                    df_filtered.drop_duplicates(subset=pk, inplace=True)

                df_filtered.to_sql(table_name, conn, if_exists='append', index=False)
                print(f"Successfully loaded {len(df_filtered)} records from {filename} into {table_name}.")

    except KeyError:
        print(f"Warning: '{filename}' not found in {zip_path}. Skipping.")
    except Exception as e:
        print(f"Error loading {filename} from {zip_path}: {e}")


def main():
    print("--- Starting Static GTFS Data Pipeline ---")
    conn = None
    try:
        conn = sqlite3.connect(DB_FILE)
        setup_database(conn)

        for zip_name, config in STATIC_GTFS_CONFIG.items():
            zip_path = os.path.join(DATA_DIR, zip_name)
            agency_id = config['agency_id']

            if not os.path.exists(zip_path):
                print(f"Warning: Data file not found at {zip_path}. Skipping.")
                continue

            print(f"\nProcessing static GTFS for {agency_id} from {zip_name}...")

            for table, file_path in config['files'].items():
                load_static_gtfs_file(zip_path, file_path, agency_id, conn, table)

            conn.commit()

    except Exception as e:
        print(f"An error occurred during the main static GTFS pipeline execution: {e}")
    finally:
        if conn:
            conn.close()
            print("Database connection closed.")

if __name__ == "__main__":
    main()
