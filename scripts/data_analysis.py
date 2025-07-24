import sqlite3
import pandas as pd
import os

# --- Robust Path Configuration ---
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)

# --- Configuration ---
REALTIME_DB = os.path.join(PROJECT_ROOT, "transit_realtime_data.db")
PERFORMANCE_DB = os.path.join(PROJECT_ROOT, "transit_performance_data.db")


def analyze_on_time_performance():
    """
    Analyzes real-time trip data to calculate on-time performance metrics
    and saves the results to the performance database.
    """
    rt_conn = sqlite3.connect(REALTIME_DB)
    perf_conn = sqlite3.connect(PERFORMANCE_DB)

    print("Reading data from databases...")
    trips_df = pd.read_sql_query("SELECT * FROM trips", perf_conn)
    routes_df = pd.read_sql_query("SELECT * FROM routes", perf_conn)
    rt_updates_query = """
    SELECT * FROM (
        SELECT *, ROW_NUMBER() OVER(PARTITION BY trip_id ORDER BY ingestion_timestamp_utc DESC) as rn
        FROM real_time_trip_updates
    ) WHERE rn = 1;
    """
    rt_updates_df = pd.read_sql_query(rt_updates_query, rt_conn)

    # Standardize agency_id values to ensure they match
    agency_mapping = {"MTA": "MTA_NYC", "BART": "BART_SF"}
    if 'agency' in rt_updates_df.columns:
        rt_updates_df.rename(columns={'agency': 'agency_id'}, inplace=True)
        rt_updates_df['agency_id'] = rt_updates_df['agency_id'].replace(agency_mapping)

    print(f"\nFound {len(rt_updates_df)} unique real-time trip updates.")
    print(f"Found {len(trips_df)} static trips.")
    
    if rt_updates_df.empty or trips_df.empty:
        print("Source data is empty. Cannot perform analysis. Exiting.")
        return

    print("\nMerging and analyzing...")
    merged_df = pd.merge(rt_updates_df, trips_df, on=['agency_id', 'trip_id'], how='inner', suffixes=('_rt', '_static'))
    
    print(f"\nFound {len(merged_df)} matching trips after merging.")
    if merged_df.empty:
        print("No matching trip_ids found between real-time and static data. Cannot continue analysis.")
        return
        
    # --- THE FIX IS HERE ---
    # After the first merge, the 'route_id' from trips_df is now 'route_id_static'.
    # We rename it back to 'route_id' so the next merge can find it.
    if 'route_id_static' in merged_df.columns:
        merged_df.rename(columns={'route_id_static': 'route_id'}, inplace=True)

    # Now merge with routes to get the route names
    final_df = pd.merge(merged_df, routes_df, on=['agency_id', 'route_id'], how='inner')

    final_df['arrival_delay_seconds'] = pd.to_numeric(final_df['arrival_delay_seconds']).fillna(0)
    
    performance_summary = final_df.groupby(['agency_id', 'route_id', 'route_short_name', 'route_long_name']).agg(
        average_delay_minutes=('arrival_delay_seconds', lambda x: x.mean() / 60),
        total_trips=('trip_id', 'nunique')
    ).reset_index()

    performance_summary['average_delay_minutes'] = performance_summary['average_delay_minutes'].round(2)

    if performance_summary.empty:
        print("\nAnalysis resulted in an empty summary. Nothing to save.")
        return

    print("\n--- On-Time Performance Summary ---")
    print(performance_summary)

    try:
        print("\nSaving analysis results to 'on_time_performance' table...")
        performance_summary.to_sql('on_time_performance', perf_conn, if_exists='replace', index=False)
        print("Successfully saved performance summary.")
    except Exception as e:
        print(f"Error saving results to database: {e}")

    rt_conn.close()
    perf_conn.close()
    print("\nDatabase connections closed.")


def add_on_time_performance_to_schema():
    """Adds the on_time_performance table to the schema file if it doesn't exist."""
    schema_path = os.path.join(PROJECT_ROOT, "sql/schema.sql")
    on_time_performance_sql = """
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
"""
    try:
        with open(schema_path, 'r+') as f:
            content = f.read()
            if "on_time_performance" not in content:
                f.write(on_time_performance_sql)
                print("Added 'on_time_performance' table to sql/schema.sql")
    except FileNotFoundError:
        print(f"Error: Could not find schema file at {schema_path}")


if __name__ == "__main__":
    add_on_time_performance_to_schema()
    analyze_on_time_performance()
