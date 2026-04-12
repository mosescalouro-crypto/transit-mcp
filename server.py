#!/usr/bin/env python3
"""
Transit MCP Server with x402 payment protocol.

Free tier: /health, /api/stats, /api/feeds, /mcp/tools
Paid tier: /api/vehicles, /api/alerts, /api/predictions (x402 USDC on Base)
"""
import sqlite3
import json
import os
import math
import asyncio
from datetime import datetime, timezone
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.responses import StreamingResponse
from fastapi.responses import JSONResponse, HTMLResponse
from fastapi.middleware.cors import CORSMiddleware

# x402 imports
from x402.http import FacilitatorConfig, HTTPFacilitatorClient, PaymentOption
from x402.http.middleware.fastapi import PaymentMiddlewareASGI
from x402.http.types import RouteConfig
from x402.mechanisms.evm.exact import ExactEvmServerScheme
from x402.server import x402ResourceServer

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'transit.db')
PORT = int(os.environ.get('TRANSIT_MCP_PORT', 8400))

# x402 config
PAY_TO_ADDRESS = os.environ.get('X402_PAY_TO', '0x0000000000000000000000000000000000000000')
PRICE_PER_QUERY = os.environ.get('X402_PRICE', '$0.001')  # $0.001 per query = ~1000 queries per dollar
NETWORK = os.environ.get('X402_NETWORK', 'eip155:84532')  # Base Sepolia testnet (switch to 8453 for mainnet)
FACILITATOR_URL = os.environ.get('X402_FACILITATOR', 'https://x402.org/facilitator')

# Rate limiting (free tier)
MAX_FREE_PER_MINUTE = 10
request_counts = {}


def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def haversine_km(lat1, lon1, lat2, lon2):
    R = 6371
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat/2)**2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon/2)**2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))


def check_free_rate_limit(request: Request):
    """Check if request is within free tier rate limit."""
    # If x402 payment header is present, skip rate limiting
    if request.headers.get('PAYMENT-SIGNATURE') or request.headers.get('X-PAYMENT'):
        return True

    ip = request.client.host if request.client else 'unknown'
    now = datetime.now(timezone.utc)
    minute_key = now.strftime('%Y%m%d%H%M')
    key = f"{ip}:{minute_key}"

    count = request_counts.get(key, 0)
    if count >= MAX_FREE_PER_MINUTE:
        return False

    old_keys = [k for k in request_counts if not k.endswith(minute_key)]
    for k in old_keys:
        del request_counts[k]

    request_counts[key] = count + 1
    return True


# --- App setup ---

@asynccontextmanager
async def lifespan(app: FastAPI):
    print(f"Transit MCP Server v0.2.0 starting on port {PORT}")
    print(f"x402 payments: {NETWORK}, price: {PRICE_PER_QUERY}, pay_to: {PAY_TO_ADDRESS}")
    print(f"Facilitator: {FACILITATOR_URL}")
    yield
    print("Transit MCP Server shutting down")

app = FastAPI(
    title="Transit MCP API",
    description="Real-time transit telemetry for AI agents. Paid via x402 (USDC on Base).",
    version="0.2.0",
    lifespan=lifespan,
)

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["PAYMENT-REQUIRED", "PAYMENT-RESPONSE"],
)

# x402 payment middleware
facilitator = HTTPFacilitatorClient(FacilitatorConfig(url=FACILITATOR_URL))
x402_server = x402ResourceServer(facilitator)
x402_server.register(NETWORK, ExactEvmServerScheme())

payment_option = PaymentOption(
    scheme="exact",
    pay_to=PAY_TO_ADDRESS,
    price=PRICE_PER_QUERY,
    network=NETWORK,
)

x402_routes = {
    "GET /api/vehicles": RouteConfig(
        accepts=[payment_option],
        description="Real-time vehicle positions near a location",
    ),
    "GET /api/alerts": RouteConfig(
        accepts=[payment_option],
        description="Active transit service alerts",
    ),
    "GET /api/predictions": RouteConfig(
        accepts=[payment_option],
        description="Arrival/departure predictions for stops",
    ),
    "GET /api/airports": RouteConfig(
        accepts=[payment_option],
        description="Search transit stops by location or name",
    ),
}

app.add_middleware(PaymentMiddlewareASGI, routes=x402_routes, server=x402_server)


# --- Free endpoints ---

@app.get("/health")
async def health():
    return {"status": "ok", "service": "transit-mcp", "version": "0.2.0",
            "payment": {"protocol": "x402", "network": NETWORK, "price": PRICE_PER_QUERY}}


@app.get("/api/stats")
async def stats():
    conn = get_db()
    c = conn.cursor()
    result = {}

    c.execute('SELECT COUNT(*) FROM feeds WHERE enabled = 1')
    result['active_feeds'] = c.fetchone()[0]

    c.execute("SELECT COUNT(*) FROM vehicles WHERE timestamp > (strftime('%s', 'now') - 300)")
    result['vehicles_last_5min'] = c.fetchone()[0]

    c.execute('SELECT COUNT(*) FROM vehicles')
    result['vehicles_total'] = c.fetchone()[0]

    c.execute("SELECT COUNT(*) FROM alerts WHERE collected_at > datetime('now', '-24 hours')")
    result['alerts_24h'] = c.fetchone()[0]

    c.execute("SELECT COUNT(*) FROM predictions WHERE collected_at > datetime('now', '-1 hour')")
    result['predictions_1h'] = c.fetchone()[0]

    result['payment'] = {
        'protocol': 'x402',
        'network': NETWORK,
        'price_per_query': PRICE_PER_QUERY,
        'free_tier': f'{MAX_FREE_PER_MINUTE} requests/min (no payment needed)',
    }

    conn.close()
    return result


@app.get("/api/feeds")
async def feeds():
    conn = get_db()
    c = conn.cursor()
    c.execute('SELECT id, name, agency, enabled, last_poll, last_error FROM feeds ORDER BY name')
    feeds_list = []
    for row in c.fetchall():
        feeds_list.append({
            'id': row['id'],
            'name': row['name'],
            'agency': row['agency'],
            'enabled': bool(row['enabled']),
            'last_poll': row['last_poll'],
            'status': 'error' if row['last_error'] else 'ok',
        })
    conn.close()
    return {"count": len(feeds_list), "feeds": feeds_list}


@app.get("/mcp/tools")
async def mcp_tools():
    tools = [
        {
            'name': 'get_vehicles_near',
            'description': 'Get real-time transit vehicle positions near a location. Returns buses, trains, ferries within a radius. Costs $0.001 USDC per query via x402.',
            'inputSchema': {
                'type': 'object',
                'properties': {
                    'lat': {'type': 'number', 'description': 'Latitude'},
                    'lon': {'type': 'number', 'description': 'Longitude'},
                    'radius_km': {'type': 'number', 'description': 'Search radius in km (default 2)', 'default': 2},
                    'limit': {'type': 'integer', 'description': 'Max results (default 50, max 200)', 'default': 50},
                },
                'required': ['lat', 'lon'],
            },
        },
        {
            'name': 'get_alerts',
            'description': 'Get active transit service alerts and disruptions. Filter by feed or route. Costs $0.001 USDC per query via x402.',
            'inputSchema': {
                'type': 'object',
                'properties': {
                    'feed': {'type': 'string', 'description': 'Feed ID (e.g. mbta, mta-ace, septa)'},
                    'route': {'type': 'string', 'description': 'Route ID to filter alerts'},
                    'limit': {'type': 'integer', 'description': 'Max results', 'default': 50},
                },
            },
        },
        {
            'name': 'get_predictions',
            'description': 'Get arrival/departure predictions for a stop or route. Costs $0.001 USDC per query via x402.',
            'inputSchema': {
                'type': 'object',
                'properties': {
                    'feed': {'type': 'string', 'description': 'Feed ID'},
                    'stop': {'type': 'string', 'description': 'Stop ID'},
                    'route': {'type': 'string', 'description': 'Route ID'},
                    'limit': {'type': 'integer', 'description': 'Max results', 'default': 20},
                },
            },
        },
        {
            'name': 'search_stops',
            'description': 'Search transit stops/stations by name or location. Costs $0.001 USDC per query via x402.',
            'inputSchema': {
                'type': 'object',
                'properties': {
                    'q': {'type': 'string', 'description': 'Search by stop name'},
                    'lat': {'type': 'number', 'description': 'Latitude for proximity search'},
                    'lon': {'type': 'number', 'description': 'Longitude for proximity search'},
                    'radius_km': {'type': 'number', 'description': 'Search radius in km', 'default': 50},
                    'limit': {'type': 'integer', 'description': 'Max results', 'default': 20},
                },
            },
        },
        {
            'name': 'stream_vehicles',
            'description': 'SSE stream of real-time vehicle positions near a location. Free (rate limited).',
            'inputSchema': {
                'type': 'object',
                'properties': {
                    'lat': {'type': 'number', 'description': 'Latitude'},
                    'lon': {'type': 'number', 'description': 'Longitude'},
                    'radius_km': {'type': 'number', 'description': 'Search radius in km', 'default': 2},
                    'interval': {'type': 'integer', 'description': 'Update interval in seconds (5-60)', 'default': 15},
                },
                'required': ['lat', 'lon'],
            },
        },
        {
            'name': 'list_feeds',
            'description': 'List all available transit data feeds and their status. Free, no payment required.',
            'inputSchema': {'type': 'object', 'properties': {}},
        },
        {
            'name': 'get_stats',
            'description': 'Get current database statistics. Free, no payment required.',
            'inputSchema': {'type': 'object', 'properties': {}},
        },
    ]
    return {
        'tools': tools,
        'payment': {
            'protocol': 'x402',
            'network': NETWORK,
            'facilitator': FACILITATOR_URL,
            'price_per_query': PRICE_PER_QUERY,
            'free_endpoints': ['/health', '/api/stats', '/api/feeds', '/mcp/tools'],
        },
    }


# --- Paid endpoints (x402 protected) ---

@app.get("/api/vehicles")
async def vehicles_near(request: Request, lat: float = 0, lon: float = 0,
                        radius_km: float = 2, limit: int = 50):
    limit = min(limit, 200)
    if not (-90 <= lat <= 90 and -180 <= lon <= 180):
        return JSONResponse({'error': 'lat/lon out of range'}, status_code=400)

    dlat = radius_km / 111.0
    dlon = radius_km / (111.0 * max(math.cos(math.radians(lat)), 0.01))

    conn = get_db()
    c = conn.cursor()
    c.execute('''
        SELECT v.feed_id, v.vehicle_id, v.trip_id, v.route_id,
               v.latitude, v.longitude, v.bearing, v.speed,
               v.stop_id, v.current_status, v.timestamp,
               f.name as feed_name, f.agency,
               r.route_short_name, r.route_long_name, r.route_color,
               s.stop_name
        FROM vehicles v
        JOIN feeds f ON f.id = v.feed_id
        LEFT JOIN routes r ON r.feed_id = CASE
            WHEN v.feed_id LIKE 'mta-%' THEN 'mta'
            WHEN v.feed_id LIKE 'septa%' THEN 'septa'
            ELSE v.feed_id END
            AND r.route_id = v.route_id
        LEFT JOIN stops s ON s.feed_id = CASE
            WHEN v.feed_id LIKE 'mta-%' THEN 'mta'
            WHEN v.feed_id LIKE 'septa%' THEN 'septa'
            ELSE v.feed_id END
            AND s.stop_id = v.stop_id
        WHERE v.latitude BETWEEN ? AND ?
          AND v.longitude BETWEEN ? AND ?
          AND v.timestamp > (strftime('%s', 'now') - 300)
        ORDER BY v.timestamp DESC
    ''', (lat - dlat, lat + dlat, lon - dlon, lon + dlon))

    results = []
    for row in c.fetchall():
        dist = haversine_km(lat, lon, row['latitude'], row['longitude'])
        if dist <= radius_km:
            route_name = row['route_short_name'] or row['route_long_name'] or row['route_id']
            item = {
                'feed': row['feed_id'],
                'feed_name': row['feed_name'],
                'agency': row['agency'],
                'vehicle_id': row['vehicle_id'],
                'route': row['route_id'],
                'route_name': route_name,
                'trip': row['trip_id'],
                'lat': row['latitude'],
                'lon': row['longitude'],
                'bearing': row['bearing'],
                'speed': row['speed'],
                'stop': row['stop_id'],
                'stop_name': row['stop_name'],
                'status': row['current_status'],
                'timestamp': row['timestamp'],
                'distance_km': round(dist, 2),
            }
            if row['route_color']:
                item['route_color'] = f"#{row['route_color']}"
            results.append(item)

    results.sort(key=lambda x: x['distance_km'])
    results = results[:limit]
    conn.close()

    return {
        'query': {'lat': lat, 'lon': lon, 'radius_km': radius_km},
        'count': len(results),
        'vehicles': results,
    }


@app.get("/api/alerts")
async def alerts(request: Request, feed: str = None, route: str = None, limit: int = 50):
    limit = min(limit, 200)
    conn = get_db()
    c = conn.cursor()

    query = '''SELECT a.*, f.name as feed_name FROM alerts a
               JOIN feeds f ON f.id = a.feed_id
               WHERE a.collected_at > datetime('now', '-24 hours')'''
    params = []

    if feed:
        query += ' AND a.feed_id = ?'
        params.append(feed)
    if route:
        query += ' AND a.route_ids LIKE ?'
        params.append(f'%"{route}"%')

    query += ' ORDER BY a.collected_at DESC LIMIT ?'
    params.append(limit)

    c.execute(query, params)
    results = []
    for row in c.fetchall():
        results.append({
            'feed': row['feed_id'],
            'feed_name': row['feed_name'],
            'alert_id': row['alert_id'],
            'header': row['header'],
            'description': row['description'],
            'cause': row['cause'],
            'effect': row['effect'],
            'routes': json.loads(row['route_ids']) if row['route_ids'] else [],
            'stops': json.loads(row['stop_ids']) if row['stop_ids'] else [],
            'active_from': row['active_from'],
            'active_until': row['active_until'],
        })

    conn.close()
    return {'count': len(results), 'alerts': results}


@app.get("/api/predictions")
async def predictions(request: Request, feed: str = None, stop: str = None,
                      route: str = None, limit: int = 20):
    limit = min(limit, 100)
    conn = get_db()
    c = conn.cursor()

    query = '''SELECT p.*, f.name as feed_name FROM predictions p
               JOIN feeds f ON f.id = p.feed_id
               WHERE p.collected_at > datetime('now', '-15 minutes')'''
    params = []

    if feed:
        query += ' AND p.feed_id = ?'
        params.append(feed)
    if stop:
        query += ' AND p.stop_id = ?'
        params.append(stop)
    if route:
        query += ' AND p.route_id = ?'
        params.append(route)

    query += ' ORDER BY p.arrival_time ASC LIMIT ?'
    params.append(limit)

    c.execute(query, params)
    results = []
    for row in c.fetchall():
        results.append({
            'feed': row['feed_id'],
            'feed_name': row['feed_name'],
            'trip': row['trip_id'],
            'route': row['route_id'],
            'stop': row['stop_id'],
            'arrival': row['arrival_time'],
            'departure': row['departure_time'],
            'delay_seconds': row['delay_s'],
            'schedule': row['schedule_relationship'],
        })

    conn.close()
    return {'count': len(results), 'predictions': results}


# --- SSE Streaming ---

@app.get("/api/stream/vehicles")
async def stream_vehicles(request: Request, lat: float = 0, lon: float = 0,
                          radius_km: float = 2, interval: int = 15):
    """Server-Sent Events stream of vehicle positions. Updates every interval seconds."""
    interval = max(5, min(interval, 60))

    if not (-90 <= lat <= 90 and -180 <= lon <= 180):
        return JSONResponse({'error': 'lat/lon out of range'}, status_code=400)

    dlat = radius_km / 111.0
    dlon = radius_km / (111.0 * max(math.cos(math.radians(lat)), 0.01))

    async def event_generator():
        while True:
            if await request.is_disconnected():
                break

            conn = get_db()
            c = conn.cursor()
            c.execute('''
                SELECT v.feed_id, v.vehicle_id, v.route_id,
                       v.latitude, v.longitude, v.bearing, v.speed,
                       v.current_status, v.timestamp,
                       f.name as feed_name,
                       r.route_short_name, r.route_long_name
                FROM vehicles v
                JOIN feeds f ON f.id = v.feed_id
                LEFT JOIN routes r ON r.feed_id = CASE
                    WHEN v.feed_id LIKE 'mta-%%' THEN 'mta'
                    WHEN v.feed_id LIKE 'septa%%' THEN 'septa'
                    ELSE v.feed_id END
                    AND r.route_id = v.route_id
                WHERE v.latitude BETWEEN ? AND ?
                  AND v.longitude BETWEEN ? AND ?
                  AND v.timestamp > (strftime('%%s', 'now') - 300)
                ORDER BY v.timestamp DESC LIMIT 100
            ''', (lat - dlat, lat + dlat, lon - dlon, lon + dlon))

            vehicles = []
            for row in c.fetchall():
                dist = haversine_km(lat, lon, row['latitude'], row['longitude'])
                if dist <= radius_km:
                    vehicles.append({
                        'feed': row['feed_id'],
                        'vehicle_id': row['vehicle_id'],
                        'route': row['route_id'],
                        'route_name': row['route_short_name'] or row['route_long_name'] or row['route_id'],
                        'lat': row['latitude'],
                        'lon': row['longitude'],
                        'bearing': row['bearing'],
                        'speed': row['speed'],
                        'status': row['current_status'],
                        'timestamp': row['timestamp'],
                        'distance_km': round(dist, 2),
                    })
            conn.close()

            data = json.dumps({'count': len(vehicles), 'vehicles': vehicles})
            yield f"data: {data}\n\n"
            await asyncio.sleep(interval)

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


# --- Airport Directory (from Neptune via aero-collectors) ---

@app.get("/api/airports")
async def airports_near(request: Request, lat: float = None, lon: float = None,
                        radius_km: float = 50, q: str = None, limit: int = 20):
    """Search airports by location or name. Uses GTFS stops table for now."""
    limit = min(limit, 100)
    conn = get_db()
    c = conn.cursor()

    # Check if we have a stops table (we do from GTFS static data)
    # For a dedicated airport endpoint, we'd connect to Neptune or the aero SQLite
    # For now, search GTFS stops which includes transit stations
    if q:
        c.execute('''SELECT feed_id, stop_id, stop_name, stop_lat, stop_lon
                     FROM stops WHERE stop_name LIKE ? LIMIT ?''',
                  (f'%{q}%', limit))
    elif lat is not None and lon is not None:
        dlat = radius_km / 111.0
        dlon = radius_km / (111.0 * max(math.cos(math.radians(lat)), 0.01))
        c.execute('''SELECT feed_id, stop_id, stop_name, stop_lat, stop_lon
                     FROM stops
                     WHERE stop_lat BETWEEN ? AND ?
                       AND stop_lon BETWEEN ? AND ?
                     LIMIT ?''',
                  (lat - dlat, lat + dlat, lon - dlon, lon + dlon, limit))
    else:
        conn.close()
        return JSONResponse({'error': 'provide lat/lon or q parameter'}, status_code=400)

    results = []
    for row in c.fetchall():
        item = {
            'feed': row['feed_id'],
            'stop_id': row['stop_id'],
            'name': row['stop_name'],
            'lat': row['stop_lat'],
            'lon': row['stop_lon'],
        }
        if lat is not None and lon is not None and row['stop_lat'] and row['stop_lon']:
            item['distance_km'] = round(haversine_km(lat, lon, row['stop_lat'], row['stop_lon']), 2)
        results.append(item)

    if lat is not None:
        results.sort(key=lambda x: x.get('distance_km', 999))

    conn.close()
    return {'count': len(results), 'stops': results}


# --- Landing page ---

@app.get("/", response_class=HTMLResponse)
async def landing():
    conn = get_db()
    c = conn.cursor()
    c.execute('SELECT COUNT(*) FROM feeds WHERE enabled = 1')
    feed_count = c.fetchone()[0]
    c.execute("SELECT COUNT(*) FROM vehicles WHERE timestamp > (strftime('%s', 'now') - 300)")
    vehicle_count = c.fetchone()[0]
    c.execute("SELECT COUNT(*) FROM alerts WHERE collected_at > datetime('now', '-24 hours')")
    alert_count = c.fetchone()[0]
    conn.close()

    return f"""<!DOCTYPE html>
<html>
<head>
    <title>Transit MCP API</title>
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <style>
        body {{ font-family: system-ui, -apple-system, sans-serif; max-width: 800px; margin: 0 auto; padding: 20px; background: #0a0a1a; color: #e0e0e0; }}
        h1 {{ color: #00d4ff; }}
        h2 {{ color: #00d4ff; margin-top: 2em; }}
        a {{ color: #00d4ff; }}
        code {{ background: #1a1a2e; padding: 2px 6px; border-radius: 3px; font-size: 0.9em; }}
        pre {{ background: #1a1a2e; padding: 16px; border-radius: 8px; overflow-x: auto; border: 1px solid #2a2a4e; }}
        .stat {{ display: inline-block; text-align: center; padding: 20px; margin: 10px; background: #1a1a2e; border-radius: 8px; border: 1px solid #2a2a4e; min-width: 150px; }}
        .stat h3 {{ color: #00d4ff; margin: 0; font-size: 2em; }}
        .stat p {{ color: #888; margin: 5px 0 0 0; }}
        .free {{ color: #4caf50; }}
        .paid {{ color: #ff9800; }}
        .badge {{ display: inline-block; padding: 2px 8px; border-radius: 4px; font-size: 0.8em; font-weight: bold; }}
        .badge.free {{ background: #1b3a1b; color: #4caf50; }}
        .badge.paid {{ background: #3a2a0a; color: #ff9800; }}
    </style>
</head>
<body>
    <h1>Transit MCP API</h1>
    <p>Real-time transit telemetry for AI agents. MBTA, NYC Subway, BART, SEPTA and more.</p>
    <p>Paid queries via <a href="https://github.com/x402-foundation/x402">x402</a> protocol (USDC on Base). Free tier: 10 requests/min.</p>

    <div>
        <div class="stat"><h3>{feed_count}</h3><p>Live Feeds</p></div>
        <div class="stat"><h3>{vehicle_count:,}</h3><p>Vehicles Now</p></div>
        <div class="stat"><h3>{alert_count}</h3><p>Active Alerts</p></div>
    </div>

    <h2>Endpoints</h2>

    <h3><span class="badge free">FREE</span> <code>GET /health</code></h3>
    <p>Health check and payment info.</p>

    <h3><span class="badge free">FREE</span> <code>GET /api/stats</code></h3>
    <p>Live database statistics.</p>

    <h3><span class="badge free">FREE</span> <code>GET /api/feeds</code></h3>
    <p>List all transit feeds and their status.</p>

    <h3><span class="badge free">FREE</span> <code>GET /mcp/tools</code></h3>
    <p>MCP tool definitions for agent discovery.</p>

    <h3><span class="badge paid">x402 {PRICE_PER_QUERY}</span> <code>GET /api/vehicles?lat=42.35&amp;lon=-71.08&amp;radius_km=2</code></h3>
    <p>Real-time vehicle positions near a location.</p>

    <h3><span class="badge paid">x402 {PRICE_PER_QUERY}</span> <code>GET /api/alerts?feed=mbta</code></h3>
    <p>Active service alerts and disruptions.</p>

    <h3><span class="badge paid">x402 {PRICE_PER_QUERY}</span> <code>GET /api/predictions?feed=mbta&amp;stop=70154</code></h3>
    <p>Arrival/departure predictions.</p>

    <h2>Payment</h2>
    <p>Paid endpoints use the <a href="https://github.com/x402-foundation/x402">x402 protocol</a>.</p>
    <ul>
        <li><strong>Price:</strong> {PRICE_PER_QUERY} USDC per query</li>
        <li><strong>Network:</strong> {NETWORK}</li>
        <li><strong>Facilitator:</strong> <code>{FACILITATOR_URL}</code></li>
        <li><strong>Free tier:</strong> {MAX_FREE_PER_MINUTE} requests/minute without payment</li>
    </ul>

    <h3>Python client example</h3>
    <pre>from x402.http.clients import x402HttpxClient
from x402 import x402Client
from x402.mechanisms.evm import EthAccountSigner
from x402.mechanisms.evm.exact.register import register_exact_evm_client
from eth_account import Account

account = Account.from_key("YOUR_PRIVATE_KEY")
client = x402Client()
register_exact_evm_client(client, EthAccountSigner(account))

async with x402HttpxClient(client) as http:
    r = await http.get("http://transit.motioninfo.com:8400/api/vehicles?lat=42.35&amp;lon=-71.08")
    print(r.json())</pre>

    <h2>Source Data</h2>
    <p>Ingesting GTFS-Realtime feeds every 15 seconds from MBTA, MTA NYC Subway, BART, SEPTA, and more.</p>
    <p style="color: #666; margin-top: 3em;">&copy; 2026 MotionInfo. Powered by <a href="https://github.com/x402-foundation/x402">x402</a>.</p>
</body>
</html>"""


if __name__ == '__main__':
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=PORT)
