import requests
from google.transit import gtfs_realtime_pb2
import datetime
import pandas as pd
import sqlite3
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# --- Configuration ---
RT_FEED_CONFIG = [
    {"agency": "BART", "feed_type": "TripUpdate", "url": "http://api.bart.gov/gtfsrt/tripupdate.aspx"},
    {"agency": "BART", "feed_type": "Alerts", "url": "http://api.bart.gov/gtfsrt/alerts.aspx"}, # Alerts feed (optional for later)
    
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

DB_FILE = "transit_realtime_data.db"
MTA_API_KEY = os.getenv('MTA_API_KEY')
BART_API_KEY = os.getenv('BART_API_KEY')

def fetch_and_parse_gtfs_rt(feed_url, headers=None):
    """
    Fetches GTFS-RT data from a URL and parses it.
    Returns a gtfs_realtime_pb2.FeedMessage object or None on error.
    """
    try:
        response = requests.get(feed_url, headers=headers, timeout=15) # Increased timeout to 15s
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

def process_trip_updates(feed, agency, line_group_passed=None): # Renamed line_group to line_group_passed
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
            
            # Initialize with None
            arrival_delay = None
            departure_delay = None
            predicted_arrival_time = None
            predicted_departure_time = None
            
            # Iterate through stop_time_updates to find delay/predicted time
            # We'll take the delay/time from the first stop_time_update that has it
            if tu.stop_time_update:
                for stu in tu.stop_time_update:
                    # Capture delay if present, otherwise it will be None
                    arrival_delay = stu.arrival.delay if stu.arrival.HasField('delay') else None
                    departure_delay = stu.departure.delay if stu.departure.HasField('delay') else None
                    
                    # Capture predicted time if present
                    predicted_arrival_time = datetime.datetime.fromtimestamp(stu.arrival.time, tz=datetime.timezone.utc) if stu.arrival.HasField('time') and stu.arrival.time != 0 else None
                    predicted_departure_time = datetime.datetime.fromtimestamp(stu.departure.time, tz=datetime.timezone.utc) if stu.departure.HasField('time') and stu.departure.time != 0 else None
                    
                    # If we found at least one delay or predicted time for this stop, we can break
                    # For a full system, you'd process all stop_time_updates
                    if arrival_delay is not None or departure_delay is not None or predicted_arrival_time is not None or predicted_departure_time is not None:
                        break # Found something useful, move to next trip_update entity

            extracted_data.append({
                "ingestion_timestamp_utc": ingestion_time.isoformat(),
                "agency": agency,
                "line_group": line_group_passed, # Use the passed line_group
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

def create_rt_data_table(conn):
    """
    Creates the real_time_trip_updates table in the SQLite database if it doesn't exist.
    """
    create_table_sql = """
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
    """
    try:
        cursor = conn.cursor()
        cursor.execute(create_table_sql)
        conn.commit()
        print(f"SQLite table 'real_time_trip_updates' processed successfully.")
    except sqlite3.Error as e:
        print(f"Error creating table: {e}")

def store_rt_data(df, conn):
    """Stores real-time trip update data into the SQLite database."""
    if df.empty:
        print("No real-time data to store.")
        return

    try:
        df.to_sql('real_time_trip_updates', conn, if_exists='append', index=False)
        print(f"Successfully loaded {len(df)} real-time trip updates.")
    except sqlite3.IntegrityError:
        print(f"Warning: Duplicate entry for real-time data detected (likely same ingestion_timestamp, agency, trip_id). Skipping.")
    except sqlite3.Error as e:
        print(f"Error storing real-time data: {e}")
    except Exception as e:
        print(f"An unexpected error occurred during real-time data storage: {e}")

def main():
    print("--- Starting Real-time GTFS Data Pipeline ---")

    conn = None
    try:
        conn = sqlite3.connect(DB_FILE)
        create_rt_data_table(conn)

        total_records_fetched = 0
        for config in RT_FEED_CONFIG:
            agency = config['agency']
            feed_url = config['url']
            feed_type = config['feed_type']
            line_group = config.get('line_group') # This gets the line_group from config

            print(f"\nFetching {feed_type} data for {agency} ({line_group if line_group else 'all lines'})...")
            
            headers = {}
            if agency == "MTA":
                # MTA often requires X-Api-Key for some feeds even if documentation states free
                if MTA_API_KEY:
                    headers['x-api-key'] = MTA_API_KEY
                else:
                    print(f"Warning: MTA_API_KEY not found in .env. Proceeding for {agency} without it, but fetch might fail.")
            elif agency == "BART":
                # BART's GTFS-RT feeds appear to be free and don't typically use x-api-key headers for tripupdate.aspx
                pass

            feed_message = fetch_and_parse_gtfs_rt(feed_url, headers=headers)

            if feed_message:
                if feed_type == "TripUpdate":
                    # Pass the correct line_group to the processing function
                    extracted_updates = process_trip_updates(feed_message, agency, line_group_passed=line_group)
                    if extracted_updates:
                        df_updates = pd.DataFrame(extracted_updates)
                        store_rt_data(df_updates, conn)
                        total_records_fetched += len(df_updates)
                    else:
                        print(f"No trip update entities found in {agency} ({line_group}) feed.")
            else:
                print(f"Failed to fetch/parse {agency} ({line_group}) feed.")

        print(f"\n--- Real-time GTFS Data Pipeline Finished. Total trip updates fetched: {total_records_fetched} ---")

    except Exception as e:
        print(f"An error occurred during the main real-time pipeline execution: {e}")
    finally:
        if conn:
            conn.close()
            print("Database connection closed.")

if __name__ == "__main__":
    main()