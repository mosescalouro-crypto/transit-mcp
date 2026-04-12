#!/usr/bin/env python3
"""
Transit GTFS-RT Ingestion Daemon.
Polls all enabled feeds and writes vehicle positions, trip updates, and alerts to SQLite.
"""
import sqlite3
import time
import logging
import os
import sys
import json
from datetime import datetime, timezone

import requests
from google.transit import gtfs_realtime_pb2

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'transit.db')
LOG_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'logs')

# Status enum mapping
VEHICLE_STATUS = {
    0: 'INCOMING_AT',
    1: 'STOPPED_AT',
    2: 'IN_TRANSIT_TO',
}

SCHEDULE_REL = {
    0: 'SCHEDULED',
    1: 'ADDED',
    2: 'UNSCHEDULED',
    3: 'CANCELED',
    5: 'DUPLICATED',
    6: 'DELETED',
}


def setup_logging():
    os.makedirs(LOG_DIR, exist_ok=True)
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
    fh = logging.FileHandler(os.path.join(LOG_DIR, 'ingest.log'))
    fh.setFormatter(fmt)
    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(fmt)
    logger.addHandler(fh)
    logger.addHandler(sh)


def fetch_protobuf(url, api_key=None):
    """Fetch and parse a GTFS-RT protobuf feed."""
    headers = {}
    if api_key:
        headers['x-api-key'] = api_key
    resp = requests.get(url, headers=headers, timeout=15)
    resp.raise_for_status()
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(resp.content)
    return feed


def ingest_vehicles(conn, feed_id, feed_msg):
    """Extract vehicle positions from a GTFS-RT FeedMessage."""
    c = conn.cursor()
    count = 0
    for entity in feed_msg.entity:
        if not entity.HasField('vehicle'):
            continue
        v = entity.vehicle
        pos = v.position if v.HasField('position') else None
        trip = v.trip if v.HasField('trip') else None

        c.execute('''INSERT OR REPLACE INTO vehicles
            (feed_id, vehicle_id, trip_id, route_id, latitude, longitude,
             bearing, speed, stop_id, current_status, timestamp)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''', (
            feed_id,
            v.vehicle.id if v.HasField('vehicle') and v.vehicle.id else entity.id,
            trip.trip_id if trip else None,
            trip.route_id if trip else None,
            pos.latitude if pos else None,
            pos.longitude if pos else None,
            pos.bearing if pos and pos.bearing else None,
            pos.speed if pos and pos.speed else None,
            v.stop_id if v.stop_id else None,
            VEHICLE_STATUS.get(v.current_status, str(v.current_status)) if v.current_status else None,
            v.timestamp if v.timestamp else int(time.time()),
        ))
        count += 1
    conn.commit()
    return count


def ingest_trips(conn, feed_id, feed_msg):
    """Extract trip updates (predictions) from a GTFS-RT FeedMessage."""
    c = conn.cursor()
    count = 0
    for entity in feed_msg.entity:
        if not entity.HasField('trip_update'):
            continue
        tu = entity.trip_update
        trip = tu.trip if tu.HasField('trip') else None

        for stu in tu.stop_time_update:
            arrival = stu.arrival if stu.HasField('arrival') else None
            departure = stu.departure if stu.HasField('departure') else None

            c.execute('''INSERT INTO predictions
                (feed_id, trip_id, route_id, stop_id, arrival_time, departure_time,
                 delay_s, schedule_relationship)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)''', (
                feed_id,
                trip.trip_id if trip else None,
                trip.route_id if trip else None,
                stu.stop_id if stu.stop_id else None,
                arrival.time if arrival and arrival.time else None,
                departure.time if departure and departure.time else None,
                arrival.delay if arrival and arrival.delay else (departure.delay if departure and departure.delay else None),
                SCHEDULE_REL.get(stu.schedule_relationship, None) if stu.schedule_relationship else None,
            ))
            count += 1
    conn.commit()
    return count


def ingest_alerts(conn, feed_id, feed_msg):
    """Extract service alerts from a GTFS-RT FeedMessage."""
    c = conn.cursor()
    count = 0
    for entity in feed_msg.entity:
        if not entity.HasField('alert'):
            continue
        a = entity.alert

        # Collect affected route and stop IDs
        route_ids = []
        stop_ids = []
        for ie in a.informed_entity:
            if ie.route_id:
                route_ids.append(ie.route_id)
            if ie.stop_id:
                stop_ids.append(ie.stop_id)

        # Get active period
        active_from = None
        active_until = None
        if a.active_period:
            ap = a.active_period[0]
            active_from = ap.start if ap.start else None
            active_until = ap.end if ap.end else None

        header = ''
        if a.header_text and a.header_text.translation:
            header = a.header_text.translation[0].text

        desc = ''
        if a.description_text and a.description_text.translation:
            desc = a.description_text.translation[0].text

        c.execute('''INSERT INTO alerts
            (feed_id, alert_id, cause, effect, header, description, severity,
             active_from, active_until, route_ids, stop_ids)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''', (
            feed_id,
            entity.id,
            str(a.cause) if a.cause else None,
            str(a.effect) if a.effect else None,
            header,
            desc,
            str(a.severity_level) if a.severity_level else None,
            active_from,
            active_until,
            json.dumps(route_ids) if route_ids else None,
            json.dumps(stop_ids) if stop_ids else None,
        ))
        count += 1
    conn.commit()
    return count


def cleanup_old_data(conn, max_age_hours=24):
    """Remove vehicle positions older than max_age_hours."""
    c = conn.cursor()
    cutoff = int(time.time()) - (max_age_hours * 3600)
    c.execute('DELETE FROM vehicles WHERE timestamp < ?', (cutoff,))
    v_deleted = c.rowcount
    c.execute("DELETE FROM predictions WHERE collected_at < datetime('now', ?)",
              (f'-{max_age_hours} hours',))
    p_deleted = c.rowcount
    c.execute("DELETE FROM alerts WHERE collected_at < datetime('now', '-7 days')")
    a_deleted = c.rowcount
    conn.commit()
    if v_deleted or p_deleted or a_deleted:
        logging.info(f"Cleanup: removed {v_deleted} vehicles, {p_deleted} predictions, {a_deleted} alerts")


def poll_once():
    """Poll all enabled feeds once."""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('SELECT id, name, url_vehicles, url_trips, url_alerts, api_key FROM feeds WHERE enabled = 1')
    feeds = c.fetchall()

    total_vehicles = 0
    total_trips = 0
    total_alerts = 0
    errors = 0

    for feed_id, name, url_v, url_t, url_a, api_key in feeds:
        try:
            # Vehicle positions
            if url_v:
                feed_msg = fetch_protobuf(url_v, api_key)
                count = ingest_vehicles(conn, feed_id, feed_msg)
                total_vehicles += count

            # Trip updates
            if url_t:
                feed_msg = fetch_protobuf(url_t, api_key)
                count = ingest_trips(conn, feed_id, feed_msg)
                total_trips += count

            # Alerts
            if url_a:
                feed_msg = fetch_protobuf(url_a, api_key)
                count = ingest_alerts(conn, feed_id, feed_msg)
                total_alerts += count

            c.execute("UPDATE feeds SET last_poll = datetime('now'), last_error = NULL WHERE id = ?",
                      (feed_id,))
            conn.commit()

        except Exception as e:
            errors += 1
            logging.warning(f"Feed {feed_id} ({name}): {e}")
            c.execute("UPDATE feeds SET last_error = ? WHERE id = ?", (str(e)[:200], feed_id))
            conn.commit()

    conn.close()
    return total_vehicles, total_trips, total_alerts, errors, len(feeds)


def main():
    setup_logging()
    logging.info("Transit GTFS-RT ingestion daemon starting")

    poll_interval = 15  # seconds
    cleanup_counter = 0

    while True:
        try:
            vehicles, trips, alerts, errors, total_feeds = poll_once()
            logging.info(f"Poll: {vehicles} vehicles, {trips} predictions, {alerts} alerts "
                         f"from {total_feeds} feeds ({errors} errors)")

            # Cleanup every 100 polls (~25 minutes)
            cleanup_counter += 1
            if cleanup_counter >= 100:
                conn = sqlite3.connect(DB_PATH)
                cleanup_old_data(conn)
                conn.close()
                cleanup_counter = 0

        except Exception as e:
            logging.error(f"Poll cycle failed: {e}")

        time.sleep(poll_interval)


if __name__ == '__main__':
    main()
