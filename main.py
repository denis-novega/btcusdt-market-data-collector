#!/usr/bin/env python3
"""
BTCUSDT Binance Spot market data collector.

What this file does:
- Subscribes to BTCUSDT spot WebSocket streams:
  * trade
  * aggTrade
  * bookTicker
  * partial depth (top-N)
  * diff depth
- Writes every stream into partitioned Parquet files.
- Reconstructs and maintains a local order book from diff depth + REST snapshot.
- Periodically writes local order book snapshots to Parquet as a separate dataset.

Why it is structured this way:
- Raw collection and research should be separated.
- The local order book must be synchronized correctly; otherwise depth data becomes invalid.
- Parquet is a good archival / research format for later replay, aggregation, and feature generation.

Install:
    pip install aiohttp websockets pyarrow

Run:
    python main.py --data-dir ./data

Notes:
- This collector uses a combined Binance WebSocket stream and requests microsecond timestamps.
- It is intentionally single-symbol and single-purpose: BTCUSDT spot only.
- It stores depth arrays as JSON strings in Parquet for simplicity and schema stability.
- For charting / UI, keep that in a separate process that reads the Parquet dataset
  or consumes a normalized live stream from this collector.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import json
import logging
import os
import signal
import time
import uuid
from collections import defaultdict, deque
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Deque, Dict, List, Optional, Tuple

import aiohttp
import pyarrow as pa
import pyarrow.parquet as pq
import websockets
from websockets.exceptions import ConnectionClosed


# -----------------------------
# Configuration
# -----------------------------

SYMBOL = "BTCUSDT"
SYMBOL_LOWER = SYMBOL.lower()
BINANCE_WS_BASE = "wss://stream.binance.com:9443/stream"
BINANCE_REST_BASE = "https://api.binance.com"

TRADE_STREAM = f"{SYMBOL_LOWER}@trade"
AGG_TRADE_STREAM = f"{SYMBOL_LOWER}@aggTrade"
BOOK_TICKER_STREAM = f"{SYMBOL_LOWER}@bookTicker"
PARTIAL_DEPTH_LEVELS = 20
PARTIAL_DEPTH_STREAM = f"{SYMBOL_LOWER}@depth{PARTIAL_DEPTH_LEVELS}@100ms"
DIFF_DEPTH_STREAM = f"{SYMBOL_LOWER}@depth@100ms"

ALL_STREAMS = [
    TRADE_STREAM,
    AGG_TRADE_STREAM,
    BOOK_TICKER_STREAM,
    PARTIAL_DEPTH_STREAM,
    DIFF_DEPTH_STREAM,
]

# Only needed for local order book bootstrap.
REST_DEPTH_SNAPSHOT_LIMIT = 5000

# Parquet flush / file rotation defaults.
DEFAULT_FLUSH_INTERVAL_SEC = 2.0
DEFAULT_MAX_RECORDS_PER_STREAM = 10_000
DEFAULT_BOOK_SNAPSHOT_INTERVAL_SEC = 1.0
DEFAULT_BOOK_SNAPSHOT_LEVELS = 50
DEFAULT_MAX_SESSION_SECONDS = 23 * 60 * 60 + 55 * 60  # 23h55m proactive reconnect


# -----------------------------
# Utility helpers
# -----------------------------


def utc_now_us() -> int:
    return time.time_ns() // 1_000



def date_hour_from_us(ts_us: int) -> Tuple[str, str]:
    dt = datetime.fromtimestamp(ts_us / 1_000_000, tz=timezone.utc)
    return dt.strftime("%Y-%m-%d"), dt.strftime("%H")



def json_dumps(obj: Any) -> str:
    return json.dumps(obj, separators=(",", ":"), ensure_ascii=False)



def atomic_write_parquet(table: pa.Table, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    pq.write_table(table, tmp_path, compression="zstd")
    os.replace(tmp_path, path)


# -----------------------------
# Parquet writer
# -----------------------------


class StreamParquetWriter:
    """
    Buffered Parquet sink.

    It keeps separate buffers per dataset/stream and writes partitioned files like:
      data/
        stream=trade/date=2026-03-26/hour=14/part-....parquet
        stream=aggTrade/...
        stream=bookTicker/...
        stream=partialDepth/...
        stream=diffDepth/...
        stream=localBook/...
    """

    def __init__(
        self,
        root_dir: Path,
        flush_interval_sec: float,
        max_records_per_stream: int,
    ) -> None:
        self.root_dir = root_dir
        self.flush_interval_sec = flush_interval_sec
        self.max_records_per_stream = max_records_per_stream
        self.buffers: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        self.last_flush_monotonic: float = time.monotonic()
        self._lock = asyncio.Lock()

    async def add(self, dataset: str, record: Dict[str, Any]) -> None:
        async with self._lock:
            self.buffers[dataset].append(record)
            if len(self.buffers[dataset]) >= self.max_records_per_stream:
                self._flush_dataset_locked(dataset)

    async def periodic_flush_loop(self, stop_event: asyncio.Event) -> None:
        while not stop_event.is_set():
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=self.flush_interval_sec)
            except asyncio.TimeoutError:
                pass
            await self.flush_all()

    async def flush_all(self) -> None:
        async with self._lock:
            for dataset in list(self.buffers.keys()):
                self._flush_dataset_locked(dataset)
            self.last_flush_monotonic = time.monotonic()

    def _flush_dataset_locked(self, dataset: str) -> None:
        rows = self.buffers.get(dataset)
        if not rows:
            return

        grouped: Dict[Tuple[str, str], List[Dict[str, Any]]] = defaultdict(list)
        for row in rows:
            ts_us = int(row["ts_local_us"])
            date_part, hour_part = date_hour_from_us(ts_us)
            grouped[(date_part, hour_part)].append(row)

        for (date_part, hour_part), group_rows in grouped.items():
            table = pa.Table.from_pylist(group_rows)
            out_dir = self.root_dir / f"stream={dataset}" / f"date={date_part}" / f"hour={hour_part}"
            part_name = f"part-{utc_now_us()}-{uuid.uuid4().hex}.parquet"
            atomic_write_parquet(table, out_dir / part_name)

        self.buffers[dataset].clear()


# -----------------------------
# Local order book manager
# -----------------------------


@dataclass
class LocalBookState:
    bids: Dict[float, float]
    asks: Dict[float, float]
    update_id: int
    synced: bool = False


class LocalOrderBook:
    """
    Maintains a local order book using Binance Spot diff depth rules.
    """

    def __init__(
        self,
        symbol: str,
        rest_base: str,
        writer: StreamParquetWriter,
        session: aiohttp.ClientSession,
        snapshot_interval_sec: float,
        snapshot_levels: int,
    ) -> None:
        self.symbol = symbol
        self.rest_base = rest_base
        self.writer = writer
        self.session = session
        self.snapshot_interval_sec = snapshot_interval_sec
        self.snapshot_levels = snapshot_levels

        self.state = LocalBookState(bids={}, asks={}, update_id=0, synced=False)
        self.buffered_events: Deque[Dict[str, Any]] = deque()
        self._sync_lock = asyncio.Lock()
        self._last_snapshot_monotonic = 0.0

    async def on_diff_event(self, event: Dict[str, Any]) -> None:
        """
        Called for every diff depth event.
        Buffers before initial sync; applies after sync.
        """
        if not self.state.synced:
            self.buffered_events.append(event)
            await self._ensure_synced()
            return

        await self._apply_or_resync(event)

    async def _ensure_synced(self) -> None:
        if self.state.synced:
            return
        async with self._sync_lock:
            if self.state.synced:
                return
            if not self.buffered_events:
                return
            await self._sync_from_snapshot()

    async def _sync_from_snapshot(self) -> None:
        while True:
            first_u = int(self.buffered_events[0]["U"])
            snapshot = await self._fetch_snapshot()
            last_update_id = int(snapshot["lastUpdateId"])

            # Binance rule: if snapshot lastUpdateId < first buffered U, fetch again.
            if last_update_id < first_u:
                await asyncio.sleep(0.25)
                continue

            bids = {float(price): float(qty) for price, qty in snapshot["bids"] if float(qty) != 0.0}
            asks = {float(price): float(qty) for price, qty in snapshot["asks"] if float(qty) != 0.0}
            self.state = LocalBookState(bids=bids, asks=asks, update_id=last_update_id, synced=True)

            # Discard buffered events where u <= snapshot lastUpdateId.
            while self.buffered_events and int(self.buffered_events[0]["u"]) <= last_update_id:
                self.buffered_events.popleft()

            # The first remaining event must cover lastUpdateId within [U, u].
            if self.buffered_events:
                first_event = self.buffered_events[0]
                U = int(first_event["U"])
                u = int(first_event["u"])
                if not (U <= last_update_id + 1 <= u or U <= last_update_id <= u):
                    # Restart sync if the range is wrong.
                    self.state.synced = False
                    await asyncio.sleep(0.25)
                    continue

            break

        while self.buffered_events:
            event = self.buffered_events.popleft()
            await self._apply_or_resync(event)

        logging.info("Local order book synchronized at update_id=%s", self.state.update_id)

    async def _fetch_snapshot(self) -> Dict[str, Any]:
        url = f"{self.rest_base}/api/v3/depth"
        params = {
            "symbol": self.symbol,
            "limit": REST_DEPTH_SNAPSHOT_LIMIT,
        }
        async with self.session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=10)) as resp:
            resp.raise_for_status()
            return await resp.json()

    async def _apply_or_resync(self, event: Dict[str, Any]) -> None:
        u = int(event["u"])
        U = int(event["U"])

        if u < self.state.update_id:
            return

        if U > self.state.update_id + 1:
            logging.warning(
                "Missed diff depth updates: U=%s, local_update_id=%s. Resynchronizing order book.",
                U,
                self.state.update_id,
            )
            self.state.synced = False
            self.buffered_events.clear()
            self.buffered_events.append(event)
            await self._ensure_synced()
            return

        self._apply_depth_update(event)
        await self._maybe_emit_snapshot(event)

    def _apply_depth_update(self, event: Dict[str, Any]) -> None:
        for price_str, qty_str in event.get("b", []):
            price = float(price_str)
            qty = float(qty_str)
            if qty == 0.0:
                self.state.bids.pop(price, None)
            else:
                self.state.bids[price] = qty

        for price_str, qty_str in event.get("a", []):
            price = float(price_str)
            qty = float(qty_str)
            if qty == 0.0:
                self.state.asks.pop(price, None)
            else:
                self.state.asks[price] = qty

        self.state.update_id = int(event["u"])

    def _top_levels(self) -> Tuple[List[List[float]], List[List[float]], Optional[Tuple[float, float]], Optional[Tuple[float, float]]]:
        top_bids = sorted(self.state.bids.items(), key=lambda x: x[0], reverse=True)[: self.snapshot_levels]
        top_asks = sorted(self.state.asks.items(), key=lambda x: x[0])[: self.snapshot_levels]
        best_bid = top_bids[0] if top_bids else None
        best_ask = top_asks[0] if top_asks else None
        bids_list = [[float(p), float(q)] for p, q in top_bids]
        asks_list = [[float(p), float(q)] for p, q in top_asks]
        return bids_list, asks_list, best_bid, best_ask

    async def _maybe_emit_snapshot(self, event: Dict[str, Any]) -> None:
        now_mono = time.monotonic()
        if now_mono - self._last_snapshot_monotonic < self.snapshot_interval_sec:
            return
        self._last_snapshot_monotonic = now_mono

        bids_list, asks_list, best_bid, best_ask = self._top_levels()
        ts_local_us = utc_now_us()
        ts_event_us = int(event.get("E", ts_local_us))

        record = {
            "ts_local_us": ts_local_us,
            "ts_event_us": ts_event_us,
            "symbol": self.symbol,
            "update_id": int(self.state.update_id),
            "best_bid_price": None if best_bid is None else float(best_bid[0]),
            "best_bid_qty": None if best_bid is None else float(best_bid[1]),
            "best_ask_price": None if best_ask is None else float(best_ask[0]),
            "best_ask_qty": None if best_ask is None else float(best_ask[1]),
            "bids_json": json_dumps(bids_list),
            "asks_json": json_dumps(asks_list),
        }
        await self.writer.add("localBook", record)


# -----------------------------
# Binance collector
# -----------------------------


class BinanceBtcCollector:
    def __init__(
        self,
        data_dir: Path,
        flush_interval_sec: float = DEFAULT_FLUSH_INTERVAL_SEC,
        max_records_per_stream: int = DEFAULT_MAX_RECORDS_PER_STREAM,
        book_snapshot_interval_sec: float = DEFAULT_BOOK_SNAPSHOT_INTERVAL_SEC,
        book_snapshot_levels: int = DEFAULT_BOOK_SNAPSHOT_LEVELS,
        max_session_seconds: int = DEFAULT_MAX_SESSION_SECONDS,
    ) -> None:
        self.data_dir = data_dir
        self.writer = StreamParquetWriter(
            root_dir=data_dir,
            flush_interval_sec=flush_interval_sec,
            max_records_per_stream=max_records_per_stream,
        )
        self.book_snapshot_interval_sec = book_snapshot_interval_sec
        self.book_snapshot_levels = book_snapshot_levels
        self.stop_event = asyncio.Event()
        self.max_session_seconds = max_session_seconds
        self.session: Optional[aiohttp.ClientSession] = None
        self.local_book: Optional[LocalOrderBook] = None

    @property
    def ws_url(self) -> str:
        streams = "/".join(ALL_STREAMS)
        return f"{BINANCE_WS_BASE}?streams={streams}&timeUnit=MICROSECOND"

    async def run(self) -> None:
        timeout = aiohttp.ClientTimeout(total=15)
        connector = aiohttp.TCPConnector(ssl=True, limit=100)
        async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
            self.session = session
            self.local_book = LocalOrderBook(
                symbol=SYMBOL,
                rest_base=BINANCE_REST_BASE,
                writer=self.writer,
                session=session,
                snapshot_interval_sec=self.book_snapshot_interval_sec,
                snapshot_levels=self.book_snapshot_levels,
            )

            flusher_task = asyncio.create_task(self.writer.periodic_flush_loop(self.stop_event), name="parquet-flusher")
            ws_task = asyncio.create_task(self._ws_loop(), name="binance-ws")

            done, pending = await asyncio.wait(
                [flusher_task, ws_task],
                return_when=asyncio.FIRST_EXCEPTION,
            )

            for task in done:
                exc = task.exception()
                if exc:
                    logging.exception("Task %s failed", task.get_name(), exc_info=exc)
                    self.stop_event.set()

            for task in pending:
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await task

            await self.writer.flush_all()

    async def shutdown(self) -> None:
        self.stop_event.set()

    async def _ws_loop(self) -> None:
        assert self.local_book is not None
        backoff = 1.0

        while not self.stop_event.is_set():
            session_reconnect_task: Optional[asyncio.Task] = None
            try:
                self._mark_book_unsynced()
                logging.info("Connecting to %s", self.ws_url)
                async with websockets.connect(
                    self.ws_url,
                    max_size=None,
                    ping_interval=None,  # Binance pings; websockets still auto-replies to server pings.
                    close_timeout=5,
                    open_timeout=15,
                ) as ws:
                    logging.info("Connected to Binance combined stream")
                    backoff = 1.0

                    if self.max_session_seconds > 0:
                        session_reconnect_task = asyncio.create_task(
                            self._close_ws_before_limit(ws, self.max_session_seconds),
                            name="session-reconnector",
                        )

                    async for message in ws:
                        if self.stop_event.is_set():
                            break
                        await self._handle_ws_message(message)
            except (ConnectionClosed, OSError, aiohttp.ClientError, asyncio.TimeoutError) as exc:
                if self.stop_event.is_set():
                    break
                logging.warning("WebSocket loop error: %s. Reconnecting in %.1fs", exc, backoff)
                self._mark_book_unsynced()
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2.0, 30.0)
            except Exception:
                logging.exception("Unexpected fatal error in WebSocket loop")
                raise
            finally:
                if session_reconnect_task is not None:
                    session_reconnect_task.cancel()
                    with contextlib.suppress(asyncio.CancelledError):
                        await session_reconnect_task
                self._mark_book_unsynced()

    async def _close_ws_before_limit(self, ws: websockets.WebSocketClientProtocol, max_session_seconds: int) -> None:
        await asyncio.sleep(max_session_seconds)
        if self.stop_event.is_set():
            return
        logging.info("Closing WebSocket proactively after %ss to avoid Binance 24h disconnect", max_session_seconds)
        await ws.close(code=1000, reason="proactive session rotation")

    def _mark_book_unsynced(self) -> None:
        if self.local_book is None:
            return
        self.local_book.state.synced = False
        self.local_book.buffered_events.clear()

    async def _handle_ws_message(self, message: str) -> None:
        payload = json.loads(message)
        stream_name = payload.get("stream")
        data = payload.get("data")
        if not stream_name or not data:
            return

        ts_local_us = utc_now_us()

        if stream_name == TRADE_STREAM:
            record = self._normalize_trade(data, ts_local_us)
            await self.writer.add("trade", record)
            return

        if stream_name == AGG_TRADE_STREAM:
            record = self._normalize_agg_trade(data, ts_local_us)
            await self.writer.add("aggTrade", record)
            return

        if stream_name == BOOK_TICKER_STREAM:
            record = self._normalize_book_ticker(data, ts_local_us)
            await self.writer.add("bookTicker", record)
            return

        if stream_name == PARTIAL_DEPTH_STREAM:
            record = self._normalize_partial_depth(data, ts_local_us)
            await self.writer.add("partialDepth", record)
            return

        if stream_name == DIFF_DEPTH_STREAM:
            record = self._normalize_diff_depth(data, ts_local_us)
            await self.writer.add("diffDepth", record)
            assert self.local_book is not None
            await self.local_book.on_diff_event(data)
            return

    def _normalize_trade(self, data: Dict[str, Any], ts_local_us: int) -> Dict[str, Any]:
        return {
            "ts_local_us": ts_local_us,
            "stream": TRADE_STREAM,
            "symbol": data["s"],
            "event_type": data["e"],
            "event_time_us": int(data["E"]),
            "trade_time_us": int(data["T"]),
            "trade_id": int(data["t"]),
            "price": float(data["p"]),
            "qty": float(data["q"]),
            "is_buyer_maker": bool(data["m"]),
        }

    def _normalize_agg_trade(self, data: Dict[str, Any], ts_local_us: int) -> Dict[str, Any]:
        return {
            "ts_local_us": ts_local_us,
            "stream": AGG_TRADE_STREAM,
            "symbol": data["s"],
            "event_type": data["e"],
            "event_time_us": int(data["E"]),
            "trade_time_us": int(data["T"]),
            "agg_trade_id": int(data["a"]),
            "first_trade_id": int(data["f"]),
            "last_trade_id": int(data["l"]),
            "price": float(data["p"]),
            "qty": float(data["q"]),
            "is_buyer_maker": bool(data["m"]),
        }

    def _normalize_book_ticker(self, data: Dict[str, Any], ts_local_us: int) -> Dict[str, Any]:
        return {
            "ts_local_us": ts_local_us,
            "stream": BOOK_TICKER_STREAM,
            "symbol": data["s"],
            "update_id": int(data["u"]),
            "best_bid_price": float(data["b"]),
            "best_bid_qty": float(data["B"]),
            "best_ask_price": float(data["a"]),
            "best_ask_qty": float(data["A"]),
        }

    def _normalize_partial_depth(self, data: Dict[str, Any], ts_local_us: int) -> Dict[str, Any]:
        return {
            "ts_local_us": ts_local_us,
            "stream": PARTIAL_DEPTH_STREAM,
            "symbol": SYMBOL,
            "last_update_id": int(data["lastUpdateId"]),
            "levels": PARTIAL_DEPTH_LEVELS,
            "bids_json": json_dumps(data.get("bids", [])),
            "asks_json": json_dumps(data.get("asks", [])),
        }

    def _normalize_diff_depth(self, data: Dict[str, Any], ts_local_us: int) -> Dict[str, Any]:
        return {
            "ts_local_us": ts_local_us,
            "stream": DIFF_DEPTH_STREAM,
            "symbol": data["s"],
            "event_type": data["e"],
            "event_time_us": int(data["E"]),
            "first_update_id": int(data["U"]),
            "final_update_id": int(data["u"]),
            "bids_json": json_dumps(data.get("b", [])),
            "asks_json": json_dumps(data.get("a", [])),
        }


# -----------------------------
# CLI / entrypoint
# -----------------------------


def setup_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s | %(levelname)s | %(message)s",
    )



def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="BTCUSDT Binance Spot collector -> Parquet")
    parser.add_argument("--data-dir", type=Path, default=Path("./data"), help="Root output directory")
    parser.add_argument("--flush-seconds", type=float, default=DEFAULT_FLUSH_INTERVAL_SEC, help="Periodic Parquet flush interval")
    parser.add_argument(
        "--max-records-per-stream",
        type=int,
        default=DEFAULT_MAX_RECORDS_PER_STREAM,
        help="Flush when stream buffer reaches this many records",
    )
    parser.add_argument(
        "--book-snapshot-seconds",
        type=float,
        default=DEFAULT_BOOK_SNAPSHOT_INTERVAL_SEC,
        help="Emit local order book snapshots every N seconds",
    )
    parser.add_argument(
        "--book-snapshot-levels",
        type=int,
        default=DEFAULT_BOOK_SNAPSHOT_LEVELS,
        help="Top N levels per side to write for local order book snapshots",
    )
    parser.add_argument(
        "--max-session-seconds",
        type=int,
        default=DEFAULT_MAX_SESSION_SECONDS,
        help="Proactively rotate the Binance WebSocket session before the 24h server limit; set 0 to disable",
    )
    parser.add_argument("--log-level", type=str, default="INFO", help="DEBUG, INFO, WARNING, ERROR")
    return parser.parse_args()


async def async_main() -> None:
    args = parse_args()
    setup_logging(args.log_level)

    collector = BinanceBtcCollector(
        data_dir=args.data_dir,
        flush_interval_sec=args.flush_seconds,
        max_records_per_stream=args.max_records_per_stream,
        book_snapshot_interval_sec=args.book_snapshot_seconds,
        book_snapshot_levels=args.book_snapshot_levels,
        max_session_seconds=args.max_session_seconds,
    )

    loop = asyncio.get_running_loop()
    stop_called = False

    def _request_shutdown() -> None:
        nonlocal stop_called
        if stop_called:
            return
        stop_called = True
        logging.info("Shutdown signal received")
        asyncio.create_task(collector.shutdown())

    for sig_name in ("SIGINT", "SIGTERM"):
        sig = getattr(signal, sig_name, None)
        if sig is not None:
            with contextlib.suppress(NotImplementedError):
                loop.add_signal_handler(sig, _request_shutdown)

    try:
        await collector.run()
    finally:
        await collector.shutdown()


if __name__ == "__main__":
    asyncio.run(async_main())
