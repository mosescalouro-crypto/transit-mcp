# Transit MCP API

Real-time transit telemetry for AI agents. Ingests GTFS-Realtime feeds from 25+ transit agencies and serves normalized vehicle positions, predictions, and alerts via a REST API with [x402](https://github.com/x402-foundation/x402) crypto payments (USDC on Base).

**Live:** [transit.motioninfo.com:8400](http://transit.motioninfo.com:8400)

## What it does

- Polls 25 GTFS-RT feeds every 15 seconds (MBTA, NYC Subway, BART, SEPTA, and more)
- Normalizes vehicle positions, trip predictions, and service alerts into a single queryable database
- Enriches responses with human-readable route and stop names from GTFS static data
- Serves data via REST API with x402 payment protocol (USDC on Base mainnet)
- SSE streaming endpoint for real-time vehicle position updates

## Endpoints

| Endpoint | Auth | Description |
|----------|------|-------------|
| `GET /health` | Free | Health check |
| `GET /api/stats` | Free | Live statistics |
| `GET /api/feeds` | Free | Feed list and status |
| `GET /mcp/tools` | Free | MCP tool definitions for agent discovery |
| `GET /api/vehicles?lat=X&lon=Y&radius_km=Z` | x402 $0.001 | Vehicles near a location |
| `GET /api/alerts?feed=mbta` | x402 $0.001 | Service alerts |
| `GET /api/predictions?feed=mbta&stop=70154` | x402 $0.001 | Arrival predictions |
| `GET /api/airports?q=back+bay` | x402 $0.001 | Search stops/stations |
| `GET /api/stream/vehicles?lat=X&lon=Y` | Free | SSE real-time stream |

## Payment

Paid endpoints use the [x402 protocol](https://github.com/x402-foundation/x402). When an agent hits a paid endpoint without payment, it receives HTTP 402 with a `PAYMENT-REQUIRED` header containing a USDC invoice on Base. The agent signs a payment authorization, re-sends the request with a `PAYMENT-SIGNATURE` header, and receives the data.

- **Price:** $0.001 USDC per query
- **Network:** Base mainnet (eip155:8453)
- **Free tier:** 10 requests/minute without payment

## Setup

```bash
python3 -m venv /opt/transit-mcp
source /opt/transit-mcp/bin/activate
pip install gtfs-realtime-bindings requests aiohttp fastapi uvicorn 'x402[fastapi,evm]'

python setup_db.py        # Create database and seed feeds
python load_static.py     # Download GTFS static data (routes, stops)
python ingest.py          # Start polling (run as daemon)
python server.py          # Start API server
```

## Environment

| Variable | Default | Description |
|----------|---------|-------------|
| `TRANSIT_MCP_PORT` | 8400 | API server port |
| `X402_PAY_TO` | (required) | EVM address to receive USDC payments. The server refuses to start without a real address. |
| `X402_PRICE_TRANSIT` | $0.001 | Price per transit query |
| `X402_PRICE_AIS` | $0.01 | Price per AIS query |
| `X402_NETWORK` | eip155:8453 | Base mainnet. Set `eip155:84532` only for Base Sepolia testing. |
| `X402_FACILITATOR` | https://x402.org/facilitator | x402 facilitator URL |

## Architecture

```
GTFS-RT Feeds (25 agencies)
    ↓ poll every 15s (ingest.py)
SQLite Database (transit.db)
    ↓ query
FastAPI Server (server.py)
    ↓ x402 middleware
AI Agent (pays USDC on Base)
```

## License

Proprietary. Copyright 2026 MotionInfo.
