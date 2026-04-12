#!/usr/bin/env python3
"""Set up the transit MCP database schema."""
import sqlite3
import os

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'transit.db')

def setup():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    c.execute('''CREATE TABLE IF NOT EXISTS feeds (
        id TEXT PRIMARY KEY,
        name TEXT NOT NULL,
        agency TEXT,
        url_vehicles TEXT,
        url_trips TEXT,
        url_alerts TEXT,
        api_key TEXT,
        poll_interval_s INTEGER DEFAULT 15,
        enabled INTEGER DEFAULT 1,
        last_poll TEXT,
        last_error TEXT
    )''')

    c.execute('''CREATE TABLE IF NOT EXISTS vehicles (
        feed_id TEXT NOT NULL,
        vehicle_id TEXT NOT NULL,
        trip_id TEXT,
        route_id TEXT,
        latitude REAL,
        longitude REAL,
        bearing REAL,
        speed REAL,
        stop_id TEXT,
        current_status TEXT,
        timestamp INTEGER,
        collected_at TEXT DEFAULT (datetime('now')),
        PRIMARY KEY (feed_id, vehicle_id, timestamp)
    )''')

    c.execute('''CREATE TABLE IF NOT EXISTS alerts (
        feed_id TEXT NOT NULL,
        alert_id TEXT,
        cause TEXT,
        effect TEXT,
        header TEXT,
        description TEXT,
        severity TEXT,
        active_from INTEGER,
        active_until INTEGER,
        route_ids TEXT,
        stop_ids TEXT,
        collected_at TEXT DEFAULT (datetime('now'))
    )''')

    c.execute('''CREATE TABLE IF NOT EXISTS predictions (
        feed_id TEXT NOT NULL,
        trip_id TEXT NOT NULL,
        route_id TEXT,
        stop_id TEXT,
        arrival_time INTEGER,
        departure_time INTEGER,
        delay_s INTEGER,
        schedule_relationship TEXT,
        collected_at TEXT DEFAULT (datetime('now'))
    )''')

    # Index for geo queries
    c.execute('''CREATE INDEX IF NOT EXISTS idx_vehicles_geo
        ON vehicles (latitude, longitude, timestamp)''')
    c.execute('''CREATE INDEX IF NOT EXISTS idx_vehicles_feed_ts
        ON vehicles (feed_id, timestamp)''')
    c.execute('''CREATE INDEX IF NOT EXISTS idx_alerts_feed
        ON alerts (feed_id, collected_at)''')

    # Seed feeds
    feeds = [
        # MBTA
        ('mbta', 'MBTA Boston', 'Massachusetts Bay Transportation Authority',
         'https://cdn.mbta.com/realtime/VehiclePositions.pb',
         'https://cdn.mbta.com/realtime/TripUpdates.pb',
         'https://cdn.mbta.com/realtime/Alerts.pb',
         None),
        # NYC MTA Subway lines
        ('mta-ace', 'NYC Subway A/C/E', 'MTA New York City Transit',
         'https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-ace',
         None, None, None),
        ('mta-bdfm', 'NYC Subway B/D/F/M', 'MTA New York City Transit',
         'https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-bdfm',
         None, None, None),
        ('mta-g', 'NYC Subway G', 'MTA New York City Transit',
         'https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-g',
         None, None, None),
        ('mta-jz', 'NYC Subway J/Z', 'MTA New York City Transit',
         'https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-jz',
         None, None, None),
        ('mta-nqrw', 'NYC Subway N/Q/R/W', 'MTA New York City Transit',
         'https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-nqrw',
         None, None, None),
        ('mta-l', 'NYC Subway L', 'MTA New York City Transit',
         'https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-l',
         None, None, None),
        ('mta-1234567', 'NYC Subway 1/2/3/4/5/6/7', 'MTA New York City Transit',
         'https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs',
         None, None, None),
        ('mta-sir', 'NYC Staten Island Railway', 'MTA New York City Transit',
         'https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-si',
         None, None, None),
        # BART
        ('bart', 'BART San Francisco', 'Bay Area Rapid Transit',
         None,
         'http://api.bart.gov/gtfsrt/tripupdate.aspx',
         'http://api.bart.gov/gtfsrt/alerts.aspx',
         None),
        # SEPTA
        ('septa', 'SEPTA Philadelphia', 'Southeastern Pennsylvania Transportation Authority',
         'https://www3.septa.org/api/GTFSRT/septa-pa-us/Vehicle/rtVehiclePosition.pb',
         'https://www3.septa.org/api/GTFSRT/septa-pa-us/Trip/rtTripUpdate.pb',
         'https://www3.septa.org/api/GTFSRT/septa-pa-us/Alert/rtServiceAlert.pb',
         None),
    ]

    for feed_id, name, agency, url_v, url_t, url_a, key in feeds:
        c.execute('''INSERT OR IGNORE INTO feeds
            (id, name, agency, url_vehicles, url_trips, url_alerts, api_key)
            VALUES (?, ?, ?, ?, ?, ?, ?)''',
            (feed_id, name, agency, url_v, url_t, url_a, key))

    conn.commit()
    conn.close()
    print(f"Database ready at {DB_PATH}")
    print(f"Seeded {len(feeds)} feeds")

if __name__ == '__main__':
    setup()
