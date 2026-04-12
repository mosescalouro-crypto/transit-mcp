#!/usr/bin/env python3
"""
Download and load GTFS static data (routes, stops) for enrichment.
Only loads route names/colors and stop names/positions — not the full schedule.
"""
import sqlite3
import csv
import io
import os
import zipfile
import requests
import logging
import sys

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'transit.db')

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s",
                    handlers=[logging.StreamHandler(sys.stdout)])

# GTFS static feed URLs (zip files)
STATIC_FEEDS = {
    'mbta': 'https://cdn.mbta.com/MBTA_GTFS.zip',
    'mta': 'http://web.mta.info/developers/data/nyct/subway/google_transit.zip',
    'bart': 'https://www.bart.gov/dev/schedules/google_transit.zip',
    'septa': 'https://www3.septa.org/developer/gtfs/septa-gtfs.zip',
    'marta': 'https://itsmarta.com/MARTA-GTFS-Latest.zip',
    'mn-metro': 'https://svc.metrotransit.org/mtgtfs/gtfs.zip',
}


def setup_static_tables(conn):
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS routes (
        feed_id TEXT NOT NULL,
        route_id TEXT NOT NULL,
        route_short_name TEXT,
        route_long_name TEXT,
        route_type INTEGER,
        route_color TEXT,
        route_text_color TEXT,
        PRIMARY KEY (feed_id, route_id)
    )''')

    c.execute('''CREATE TABLE IF NOT EXISTS stops (
        feed_id TEXT NOT NULL,
        stop_id TEXT NOT NULL,
        stop_name TEXT,
        stop_lat REAL,
        stop_lon REAL,
        parent_station TEXT,
        PRIMARY KEY (feed_id, stop_id)
    )''')

    c.execute('CREATE INDEX IF NOT EXISTS idx_stops_geo ON stops (stop_lat, stop_lon)')
    conn.commit()


def load_csv_from_zip(zip_data, filename):
    """Extract and parse a CSV file from a GTFS zip."""
    try:
        zf = zipfile.ZipFile(io.BytesIO(zip_data))
        with zf.open(filename) as f:
            text = io.TextIOWrapper(f, encoding='utf-8-sig')
            return list(csv.DictReader(text))
    except (KeyError, zipfile.BadZipFile):
        return []


def load_feed(conn, feed_prefix, url):
    """Download a GTFS zip and load routes + stops."""
    logging.info(f"Downloading {feed_prefix} from {url}")
    try:
        resp = requests.get(url, timeout=60)
        resp.raise_for_status()
    except Exception as e:
        logging.error(f"  Failed to download {feed_prefix}: {e}")
        return

    zip_data = resp.content
    logging.info(f"  Downloaded {len(zip_data) / 1024:.0f} KB")

    c = conn.cursor()

    # Load routes
    routes = load_csv_from_zip(zip_data, 'routes.txt')
    route_count = 0
    for r in routes:
        c.execute('''INSERT OR REPLACE INTO routes
            (feed_id, route_id, route_short_name, route_long_name, route_type, route_color, route_text_color)
            VALUES (?, ?, ?, ?, ?, ?, ?)''', (
            feed_prefix,
            r.get('route_id', ''),
            r.get('route_short_name', ''),
            r.get('route_long_name', ''),
            int(r.get('route_type', 0)) if r.get('route_type') else None,
            r.get('route_color', ''),
            r.get('route_text_color', ''),
        ))
        route_count += 1

    # Load stops
    stops = load_csv_from_zip(zip_data, 'stops.txt')
    stop_count = 0
    for s in stops:
        lat = s.get('stop_lat', '')
        lon = s.get('stop_lon', '')
        c.execute('''INSERT OR REPLACE INTO stops
            (feed_id, stop_id, stop_name, stop_lat, stop_lon, parent_station)
            VALUES (?, ?, ?, ?, ?, ?)''', (
            feed_prefix,
            s.get('stop_id', ''),
            s.get('stop_name', ''),
            float(lat) if lat else None,
            float(lon) if lon else None,
            s.get('parent_station', ''),
        ))
        stop_count += 1

    conn.commit()
    logging.info(f"  Loaded {route_count} routes, {stop_count} stops for {feed_prefix}")


def main():
    conn = sqlite3.connect(DB_PATH)
    setup_static_tables(conn)

    for feed_prefix, url in STATIC_FEEDS.items():
        load_feed(conn, feed_prefix, url)

    # Summary
    c = conn.cursor()
    c.execute('SELECT COUNT(*) FROM routes')
    total_routes = c.fetchone()[0]
    c.execute('SELECT COUNT(*) FROM stops')
    total_stops = c.fetchone()[0]
    c.execute('SELECT COUNT(DISTINCT feed_id) FROM routes')
    total_feeds = c.fetchone()[0]

    logging.info(f"DONE: {total_routes} routes, {total_stops} stops from {total_feeds} feeds")
    conn.close()


if __name__ == '__main__':
    main()
