import requests
from google.transit import gtfs_realtime_pb2
import datetime
import pandas as pd
import sqlite3
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# --- Robust Path Configuration ---
# Get the absolute path of the directory where the script is located
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
# Get the project root directory (which is one level up from the 'scripts' directory)
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)


# --- Configuration ---
RT_FEED_CONFIG = [
    {"agency": "BART", "feed_type": "TripUpdate", "url": "http://api.bart.gov/gtfsrt/tripupdate.aspx"},
    {"agency": "BART", "feed_type": "Alerts", "url": "http://api.bart.gov/gtfsrt/alerts.aspx"},

    # MTA Subway Feeds
    {"agency": "MTA", "feed_type": "TripUpdate", "line_group": "ACE", "url": "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-ace"},
    {"agency": "MTA", "feed_type": "TripUpdate", "line_group": "BDFM", "url": "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-bdfm"},
    {"agency": "MTA", "feed_type": "TripUpdate", "line_group": "G", "url": "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-g"},
    {"agency": "MTA", "feed_type": "TripUpdate", "line_group": "JZ", "url": "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-jz"},
    {"agency": "MTA", "feed_type": "TripUpdate", "line_group": "NQRW", "url": "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-nqrw"},
    {"agency": "MTA", "feed_type": "TripUpdate", "line_group": "L", "url": "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-l"},
    {"agency": "MTA", "feed_type": "TripUpdate", "line_group": "1234567S", "url": "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs"},
    {"agency": "MTA", "feed_type": "TripUpdate", "line_group": "SIR", "url": "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-si"},
]

# Construct absolute paths for database and schema files
DB_FILE = os.path.join(PROJECT_ROOT, "transit_realtime_data.db")
SCHEMA_FILE = os.path.join(PROJECT_ROOT, "sql/schema.sql")
MTA_API_KEY = os.getenv('MTA_API_KEY')
BART_API_KEY = os.getenv('BART_API_KEY')

def setup_database(conn):
    """
    Sets up the database by executing the schema file.
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

def fetch_and_parse_gtfs_rt(feed_url, headers=None):
    """
    Fetches GTFS-RT data from a URL and parses it.
    Returns a gtfs_realtime_pb2.FeedMessage object or None on error.
    """
    try:
        response = requests.get(feed_url, headers=headers, timeout=15)
        response.raise_for_status()

        feed = gtfs_realtime_pb2.FeedMessage()
        feed.ParseFromString(response.content)
        return feed
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data from {feed_url}: {e}")
        return None
    except Exception as e:
        print(f"Error parsing Protobuf from {feed_url}: {e}")
        return None

def process_trip_updates(feed, agency, line_group_passed=None):
    """
    Processes TripUpdate entities from a GTFS-RT feed.
    Returns a list of dictionaries with extracted data.
    """
    extracted_data = []
    ingestion_time = datetime.datetime.now(datetime.timezone.utc)

    for entity in feed.entity:
        if entity.HasField('trip_update'):
            tu = entity.trip_update

            trip_id = tu.trip.trip_id
            route_id = tu.trip.route_id
            direction_id = tu.trip.direction_id

            arrival_delay = None
            departure_delay = None
            predicted_arrival_time = None
            predicted_departure_time = None

            if tu.stop_time_update:
                for stu in tu.stop_time_update:
                    arrival_delay = stu.arrival.delay if stu.arrival.HasField('delay') else None
                    departure_delay = stu.departure.delay if stu.departure.HasField('delay') else None
                    predicted_arrival_time = datetime.datetime.fromtimestamp(stu.arrival.time, tz=datetime.timezone.utc) if stu.arrival.HasField('time') and stu.arrival.time != 0 else None
                    predicted_departure_time = datetime.datetime.fromtimestamp(stu.departure.time, tz=datetime.timezone.utc) if stu.departure.HasField('time') and stu.departure.time != 0 else None
                    if arrival_delay is not None or departure_delay is not None or predicted_arrival_time is not None or predicted_departure_time is not None:
                        break

            extracted_data.append({
                "ingestion_timestamp_utc": ingestion_time.isoformat(),
                "agency": agency,
                "line_group": line_group_passed,
                "trip_id": trip_id,
                "route_id": route_id,
                "direction_id": direction_id,
                "current_status": tu.trip.schedule_relationship if tu.trip.HasField('schedule_relationship') else None,
                "arrival_delay_seconds": arrival_delay,
                "departure_delay_seconds": departure_delay,
                "predicted_arrival_time_utc": predicted_arrival_time.isoformat() if predicted_arrival_time else None,
                "predicted_departure_time_utc": predicted_departure_time.isoformat() if predicted_departure_time else None,
                "last_update_timestamp_feed": datetime.datetime.fromtimestamp(tu.timestamp, tz=datetime.timezone.utc).isoformat() if tu.HasField('timestamp') else None
            })
    return extracted_data

def store_rt_data(df, conn):
    """Stores real-time trip update data into the SQLite database."""
    if df.empty:
        print("No real-time data to store.")
        return

    try:
        df.to_sql('real_time_trip_updates', conn, if_exists='append', index=False)
        print(f"Successfully loaded {len(df)} real-time trip updates.")
    except sqlite3.IntegrityError:
        print(f"Warning: Duplicate entry for real-time data detected. Skipping.")
    except sqlite3.Error as e:
        print(f"Error storing real-time data: {e}")

def main():
    print("--- Starting Real-time GTFS Data Pipeline ---")

    conn = None
    try:
        conn = sqlite3.connect(DB_FILE)
        setup_database(conn)

        total_records_fetched = 0
        for config in RT_FEED_CONFIG:
            # ** THE FIX IS HERE **
            # Use .get() for safer dictionary access and check for essential keys
            agency = config.get('agency')
            feed_url = config.get('url')
            feed_type = config.get('feed_type')

            if not all([agency, feed_url, feed_type]):
                print(f"Warning: Skipping invalid or incomplete config entry: {config}")
                continue # Move to the next item in the loop

            line_group = config.get('line_group')

            print(f"\nFetching {feed_type} data for {agency} ({line_group if line_group else 'all lines'})...")

            headers = {}
            if agency == "MTA" and MTA_API_KEY:
                headers['x-api-key'] = MTA_API_KEY
            elif agency == "MTA":
                 print(f"Warning: MTA_API_KEY not found. Proceeding without it.")

            feed_message = fetch_and_parse_gtfs_rt(feed_url, headers=headers)

            if feed_message and feed_type == "TripUpdate":
                extracted_updates = process_trip_updates(feed_message, agency, line_group_passed=line_group)
                if extracted_updates:
                    df_updates = pd.DataFrame(extracted_updates)
                    store_rt_data(df_updates, conn)
                    total_records_fetched += len(df_updates)
                else:
                    print(f"No trip update entities found in {agency} ({line_group}) feed.")
            elif not feed_message:
                print(f"Failed to fetch/parse {agency} ({line_group}) feed.")

        print(f"\n--- Real-time GTFS Data Pipeline Finished. Total trip updates fetched: {total_records_fetched} ---")

    except Exception as e:
        print(f"An error occurred during the main pipeline execution: {e}")
    finally:
        if conn:
            conn.close()
            print("Database connection closed.")

if __name__ == "__main__":
    main()
