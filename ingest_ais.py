#!/usr/bin/env python3
"""
AIS vessel position ingestion from BigQuery.
Pulls recent vessel positions every 60 seconds and caches in local SQLite.

Cost: ~$13/month at 1 poll/minute (scans ~50-100MB per query from partitioned table).
At $0.01/query with >37K queries/month, this is profitable.
"""
import sqlite3
import time
import logging
import os
import sys
import subprocess
import json
from datetime import datetime

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'transit.db')
LOG_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'logs')

BQ_PROJECT = os.environ.get('BQ_PROJECT', 'x-guard-173415')
BQ_CLI = os.environ.get('BQ_CLI', '/snap/bin/bq')
POLL_INTERVAL = 60  # seconds


def setup_logging():
    os.makedirs(LOG_DIR, exist_ok=True)
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
    fh = logging.FileHandler(os.path.join(LOG_DIR, 'ingest_ais.log'))
    fh.setFormatter(fmt)
    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(fmt)
    logger.addHandler(fh)
    logger.addHandler(sh)


def setup_tables(conn):
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS ais_vessels (
        mmsi TEXT NOT NULL,
        name TEXT,
        imo TEXT,
        callsign TEXT,
        ship_type INTEGER,
        ship_type_name TEXT,
        latitude REAL,
        longitude REAL,
        course REAL,
        speed REAL,
        heading REAL,
        nav_status TEXT,
        destination TEXT,
        draught REAL,
        length REAL,
        width REAL,
        timestamp INTEGER,
        source TEXT,
        collected_at TEXT DEFAULT (datetime('now')),
        PRIMARY KEY (mmsi, timestamp)
    )''')

    c.execute('CREATE INDEX IF NOT EXISTS idx_ais_geo ON ais_vessels (latitude, longitude, timestamp)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_ais_mmsi ON ais_vessels (mmsi, timestamp)')
    conn.commit()


# Ship type mapping
SHIP_TYPES = {
    30: 'Fishing', 31: 'Towing', 32: 'Towing Large', 33: 'Dredging',
    34: 'Diving', 35: 'Military', 36: 'Sailing', 37: 'Pleasure Craft',
    40: 'HSC', 50: 'Pilot', 51: 'SAR', 52: 'Tug', 53: 'Port Tender',
    55: 'Law Enforcement', 58: 'Medical',
}
for i in range(60, 70): SHIP_TYPES[i] = 'Passenger'
for i in range(70, 80): SHIP_TYPES[i] = 'Cargo'
for i in range(80, 90): SHIP_TYPES[i] = 'Tanker'

NAV_STATUS = {
    0: 'Under way using engine', 1: 'At anchor', 2: 'Not under command',
    3: 'Restricted manoeuvrability', 4: 'Constrained by draught',
    5: 'Moored', 6: 'Aground', 7: 'Engaged in fishing',
    8: 'Under way sailing', 11: 'Power-driven vessel towing astern',
    12: 'Power-driven vessel pushing ahead', 14: 'AIS-SART', 15: 'Not defined',
}


def poll_bq():
    """Pull recent AIS positions from BigQuery using bq CLI."""
    # Simple position query — no JOIN to avoid complex escaping issues with bq CLI
    query = "SELECT MMSI, ST_Y(Position) as lat, ST_X(Position) as lon, COG, SOG, TrueHeading, NavigationalStatus, UNIX_SECONDS(RecvTime) as unix_ts FROM ais.ShipPosition WHERE RecvTime > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 5 MINUTE) AND Position IS NOT NULL QUALIFY ROW_NUMBER() OVER (PARTITION BY MMSI ORDER BY RecvTime DESC) = 1"

    try:
        result = subprocess.run(
            [BQ_CLI, 'query', '--project_id', BQ_PROJECT,
             '--use_legacy_sql=false', '--format=json', '--max_rows=10000',
             query],
            capture_output=True, text=True, timeout=30
        )

        if result.returncode != 0:
            logging.error(f"BQ query failed: {result.stderr[:200]}")
            return 0

        rows = json.loads(result.stdout) if result.stdout.strip() else []

    except subprocess.TimeoutExpired:
        logging.error("BQ query timed out")
        return 0
    except json.JSONDecodeError as e:
        logging.error(f"BQ JSON parse error: {e}")
        return 0

    if not rows:
        return 0

    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    count = 0

    for row in rows:
        mmsi = str(row.get('MMSI', ''))
        nav = int(row['NavigationalStatus']) if row.get('NavigationalStatus') else None
        nav_name = NAV_STATUS.get(nav, str(nav)) if nav is not None else None

        lat = float(row['lat']) if row.get('lat') else None
        lon = float(row['lon']) if row.get('lon') else None
        if lat is None or lon is None:
            continue

        heading_raw = row.get('TrueHeading')
        heading = int(heading_raw) if heading_raw and heading_raw != '511' and heading_raw != 511 else None

        c.execute('''INSERT OR REPLACE INTO ais_vessels
            (mmsi, latitude, longitude, course, speed, heading, nav_status,
             timestamp, source)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)''', (
            mmsi,
            lat, lon,
            float(row['COG']) if row.get('COG') else None,
            float(row['SOG']) if row.get('SOG') else None,
            heading,
            nav_name,
            int(row['unix_ts']) if row.get('unix_ts') else None,
            'motioninfo-ais-network',
        ))
        count += 1

    conn.commit()

    # Cleanup old data (>2 hours)
    c.execute("DELETE FROM ais_vessels WHERE timestamp < (strftime('%s', 'now') - 7200)")
    cleaned = c.rowcount
    conn.commit()
    conn.close()

    if cleaned:
        logging.info(f"Cleaned {cleaned} old AIS records")

    return count


def main():
    setup_logging()
    logging.info("AIS ingestion daemon starting (BigQuery source)")

    conn = sqlite3.connect(DB_PATH)
    setup_tables(conn)
    conn.close()

    while True:
        count = poll_bq()
        logging.info(f"AIS poll: {count} vessel positions from BigQuery")
        time.sleep(POLL_INTERVAL)


if __name__ == '__main__':
    main()
