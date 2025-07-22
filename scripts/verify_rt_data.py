import sqlite3
import pandas as pd
import os

DB_FILE = "transit_realtime_data.db"
# Ensure you are in the correct directory. VS Code terminal usually starts in project root.
# If not, add: os.chdir('path/to/your/project/folder')

conn = None
try:
    conn = sqlite3.connect(DB_FILE)
    print(f"Connected to SQLite database: {DB_FILE}")

    # Query to view a sample of real-time trip updates
    # Focus on delays and line_group, and check for BART data
    query_data = "SELECT ingestion_timestamp_utc, agency, line_group, trip_id, route_id, arrival_delay_seconds, departure_delay_seconds, predicted_arrival_time_utc FROM real_time_trip_updates ORDER BY ingestion_timestamp_utc DESC LIMIT 20;"
    print("\n--- Sample Real-time Trip Updates (Top 20, checking delays) ---")
    df_data = pd.read_sql_query(query_data, conn)
    print(df_data)

    # Query to count total real-time records
    query_count = "SELECT COUNT(*) AS total_records FROM real_time_trip_updates;"
    print("\n--- Total Real-time Records ---")
    df_count = pd.read_sql_query(query_count, conn)
    print(df_count)

    # Count records per agency/line group (crucial for BART)
    query_agency_counts = """
    SELECT
        agency,
        line_group,
        COUNT(*) AS record_count,
        SUM(CASE WHEN arrival_delay_seconds IS NOT NULL THEN 1 ELSE 0 END) AS records_with_arrival_delay,
        SUM(CASE WHEN departure_delay_seconds IS NOT NULL THEN 1 ELSE 0 END) AS records_with_departure_delay
    FROM
        real_time_trip_updates
    GROUP BY
        agency, line_group
    ORDER BY
        agency, line_group;
    """
    print("\n--- Real-time Record Counts by Agency/Line Group (with delay counts) ---")
    df_agency_counts = pd.read_sql_query(query_agency_counts, conn)
    print(df_agency_counts)

except sqlite3.Error as e:
    print(f"Error accessing SQLite database '{DB_FILE}': {e}")
except Exception as e:
    print(f"An unexpected error occurred: {e}")
finally:
    if conn:
        conn.close()
        print("\nDatabase connection closed.")