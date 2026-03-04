from __future__ import annotations

import asyncio
import json
import os
import signal
from dataclasses import dataclass
from typing import Any, Dict, List

import httpx
import uvicorn
import websockets
from fastapi import FastAPI

# -----------------------------
# Config
# -----------------------------

POLYMARKET_WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"  # market channel [web:275][web:283]

# Minimum USD size to treat the trade as "whale"
WHALE_MIN_USD = float(os.getenv("POLYMARKET_WHALE_MIN_USD", "1000"))

PORT = int(os.getenv("PORT", "8000"))  # Render will set PORT


@dataclass
class WhaleTrade:
    market_id: str
    market_question: str
    price: float
    size: float
    side: str
    trader: str
    notional: float
    ts: int


# In-memory store of recent whale trades
_recent_whales: List[WhaleTrade] = []
_MAX_STORED = 100

app = FastAPI(title="Polymarket Whale Worker")


def _add_whale(trade: WhaleTrade) -> None:
    _recent_whales.append(trade)
    if len(_recent_whales) > _MAX_STORED:
        del _recent_whales[0 : len(_recent_whales) - _MAX_STORED]


@app.get("/whales/latest")
async def whales_latest() -> Dict[str, Any]:
    """Return latest whale trades as JSON for the Telegram bot."""
    return {
        "min_usd": WHALE_MIN_USD,
        "count": len(_recent_whales),
        "trades": [
            {
                "market_id": t.market_id,
                "market_question": t.market_question,
                "price": t.price,
                "size": t.size,
                "side": t.side,
                "trader": t.trader,
                "notional": t.notional,
                "ts": t.ts,
            }
            for t in reversed(_recent_whales)
        ],
    }


# -----------------------------
# Helper: fetch market questions
# -----------------------------

async def _load_market_map() -> Dict[str, str]:
    """
    Build a map market_id -> question via Gamma markets API,
    so we can attach human-readable names to trades. [web:248][web:259]
    """
    url = "https://gamma-api.polymarket.com/markets"
    params = {
        "active": True,
        "closed": False,
        "archived": False,
        "limit": 500,
    }
    async with httpx.AsyncClient(timeout=15.0) as client:
        resp = await client.get(url, params=params)
        resp.raise_for_status()
        markets = resp.json()

    mapping: Dict[str, str] = {}
    for m in markets:
        mid = m.get("marketId") or m.get("id")
        q = m.get("question") or m.get("slug") or "Unknown market"
        if mid:
            mapping[str(mid)] = q
    return mapping


# -----------------------------
# WebSocket consumer
# -----------------------------

async def _ws_consumer() -> None:
    """
    Connects to the Polymarket market WebSocket channel, listens for trades,
    and records "whale" trades above WHALE_MIN_USD. [web:159][web:275][web:281]
    """
    market_map = await _load_market_map()

    while True:
        try:
            async with websockets.connect(POLYMARKET_WS_URL, ping_interval=20, ping_timeout=20) as ws:
                # Subscribe to all trades (market channel)
                sub_msg = json.dumps(
                    {
                        "type": "subscribe",
                        "channel": "trades",
                        "markets": "all",
                    }
                )
                await ws.send(sub_msg)

                async for raw in ws:
                    try:
                        msg = json.loads(raw)
                    except json.JSONDecodeError:
                        continue

                    # Expected shape: { "type": "trade", "data": {...} } [web:275][web:272]
                    if msg.get("type") != "trade":
                        continue

                    data = msg.get("data") or {}

                    market_id = str(data.get("marketId") or data.get("market_id") or "")
                    price = float(data.get("price", 0.0))
                    size = float(data.get("size", 0.0))
                    side = str(data.get("side") or "").upper()
                    trader = str(data.get("trader") or data.get("owner") or "")
                    ts = int(data.get("timestamp", 0))

                    notional = price * size

                    if notional < WHALE_MIN_USD:
                        continue
                    if not market_id or not trader:
                        continue

                    question = market_map.get(market_id, "Unknown market")

                    trade = WhaleTrade(
                        market_id=market_id,
                        market_question=question,
                        price=price,
                        size=size,
                        side=side,
                        trader=trader,
                        notional=notional,
                        ts=ts,
                    )
                    _add_whale(trade)
        except Exception as exc:
            # Log and reconnect after delay
            print(f"[ws] error: {exc}, reconnecting in 5s", flush=True)
            await asyncio.sleep(5)


async def _background_runner() -> None:
    await _ws_consumer()


def main() -> None:
    loop = asyncio.get_event_loop()

    # Start background WS task
    loop.create_task(_background_runner())

    # Run FastAPI via Uvicorn
    config = uvicorn.Config(app, host="0.0.0.0", port=PORT, log_level="info")
    server = uvicorn.Server(config)

    # Graceful shutdown
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, server.handle_exit, sig, None)

    loop.run_until_complete(server.serve())


if __name__ == "__main__":
    main()
