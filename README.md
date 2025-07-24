# Multi-City Transit Performance Pipeline

![Python Version](https://img.shields.io/badge/python-3.12-blue.svg)
![License](https://img.shields.io/badge/license-MIT-green.svg)

An end-to-end data engineering pipeline that ingests, processes, and analyzes real-time and static public transit data from MTA (NYC) and BART (SF).

---

### Key Features
- **ETL Pipeline:** Complete pipeline to fetch data from APIs, process static files, and load into a SQLite database.
- **Data Modeling:** Uses a clean, centralized SQL schema (`sql/schema.sql`) to define the database structure.
- **Data Quality Analysis:** Identifies and documents a real-world `trip_id` mismatch issue with the MTA data feeds.
- **Performance Analysis:** Includes a script to join the datasets and calculate on-time performance metrics for BART.

### How to Run

1.  **Clone & Setup:**
    ```bash
   git clone https://github.com/TrangDao0630/de_transit_monitor_multi_city.git
    cd de_transit_monitor_multi_city
    pip install -r requirements.txt
    ```

2.  **Download Static Data:**
    Download the static GTFS `.zip` files from the official agency websites and place them in the `data/` directory. (Note: The `data` folder is not tracked by Git).
    * *Example links (you will need to find the current official URLs):*
    * [MTA GTFS Data](https://new.mta.info/developers)
    * [BART GTFS Data](https://www.bart.gov/schedules/developers/gtfs)

3.  **Execute Pipeline:**
    Run the scripts in order from your terminal.
    ```bash
    python scripts/fetch_realtime_gtfs.py
    python scripts/process_static_gtfs.py
    python scripts/analyze_data.py
    ```

### Key Finding: MTA Data Discrepancy

A key finding was a data consistency issue with the MTA feeds. The `trip_id` values in the real-time and static feeds do not directly correspond, preventing a direct join for performance analysis without a more complex data reconciliation step. This highlights a common real-world data engineering challenge.

