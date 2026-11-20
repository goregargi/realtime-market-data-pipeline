"""
Market data producer.

Streams live trades from the Coinbase Exchange WebSocket feed (public, no API key)
into Kafka. Each message is normalized to a stable schema before publishing.
Messages that cannot be parsed or validated are routed to a dead-letter topic
with the failure reason, so nothing is silently dropped.

A `simulate` source is included for offline demos. It generates a random-walk
trade stream and deliberately injects malformed and late events.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import random
import signal
import time
import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta

import websockets
from confluent_kafka import Producer

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s [producer] %(message)s",
)
log = logging.getLogger(__name__)

COINBASE_WS_URL = "wss://ws-feed.exchange.coinbase.com"
TRADE_MESSAGE_TYPES = {"match", "last_match"}


@dataclass
class Config:
    bootstrap_servers: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    raw_topic: str = os.getenv("TOPIC_TRADES_RAW", "trades.raw")
    dlq_topic: str = os.getenv("TOPIC_TRADES_DLQ", "trades.dlq")
    source: str = os.getenv("SOURCE", "coinbase")
    products: list[str] = field(
        default_factory=lambda: [
            p.strip() for p in os.getenv("PRODUCTS", "BTC-USD,ETH-USD,SOL-USD").split(",") if p.strip()
        ]
    )
    simulate_rate: float = float(os.getenv("SIMULATE_TRADES_PER_SEC", "25"))
    stats_interval_sec: int = int(os.getenv("STATS_INTERVAL_SEC", "30"))


class ValidationError(ValueError):
    """Raised when an incoming message does not meet the trade schema."""


def normalize_coinbase_trade(msg: dict) -> dict:
    """Convert a Coinbase `match` message into the pipeline's trade schema."""
    required = ("trade_id", "product_id", "price", "size", "side", "time")
    missing = [k for k in required if msg.get(k) in (None, "")]
    if missing:
        raise ValidationError(f"missing fields: {', '.join(missing)}")

    try:
        price = float(msg["price"])
        size = float(msg["size"])
    except (TypeError, ValueError) as exc:
        raise ValidationError(f"non-numeric price or size: {exc}") from exc

    if price <= 0 or size <= 0:
        raise ValidationError(f"non-positive price or size: price={price} size={size}")
    if msg["side"] not in ("buy", "sell"):
        raise ValidationError(f"unknown side: {msg['side']}")

    return {
        "trade_id": int(msg["trade_id"]),
        "product_id": msg["product_id"],
        "price": price,
        "size": size,
        "side": msg["side"],
        "trade_time": msg["time"],
        "ingest_time": datetime.now(UTC).isoformat(),
        "source": "coinbase",
    }


class KafkaPublisher:
    """Thin wrapper around confluent-kafka with delivery tracking and counters."""

    def __init__(self, cfg: Config) -> None:
        self.cfg = cfg
        self.producer = Producer(
            {
                "bootstrap.servers": cfg.bootstrap_servers,
                "client.id": f"market-producer-{uuid.uuid4().hex[:6]}",
                "acks": "all",
                "enable.idempotence": True,
                "linger.ms": 20,
                "compression.type": "lz4",
            }
        )
        self.sent = 0
        self.dead_lettered = 0
        self.failed = 0

    def _on_delivery(self, err, _msg) -> None:
        if err is not None:
            self.failed += 1
            log.error("delivery failed: %s", err)

    def publish_trade(self, trade: dict) -> None:
        self.producer.produce(
            self.cfg.raw_topic,
            key=trade["product_id"],
            value=json.dumps(trade),
            on_delivery=self._on_delivery,
        )
        self.sent += 1
        self.producer.poll(0)

    def publish_dead_letter(self, payload: str, reason: str) -> None:
        envelope = {
            "reason": reason,
            "payload": payload,
            "layer": "producer",
            "received_at": datetime.now(UTC).isoformat(),
        }
        self.producer.produce(self.cfg.dlq_topic, value=json.dumps(envelope), on_delivery=self._on_delivery)
        self.dead_lettered += 1
        self.producer.poll(0)

    def handle_raw(self, raw: str) -> None:
        """Parse, validate and route a single raw message."""
        try:
            msg = json.loads(raw)
        except json.JSONDecodeError as exc:
            self.publish_dead_letter(raw, f"invalid json: {exc}")
            return

        if msg.get("type") not in TRADE_MESSAGE_TYPES:
            return  # subscriptions, heartbeats and other control messages

        try:
            self.publish_trade(normalize_coinbase_trade(msg))
        except ValidationError as exc:
            self.publish_dead_letter(raw, str(exc))

    def flush(self) -> None:
        remaining = self.producer.flush(10)
        if remaining:
            log.warning("%d messages were not delivered before shutdown", remaining)


async def stream_coinbase(cfg: Config, publisher: KafkaPublisher, stop: asyncio.Event) -> None:
    """Consume the Coinbase matches channel with automatic reconnect."""
    backoff = 1
    subscribe = {"type": "subscribe", "product_ids": cfg.products, "channels": ["matches"]}

    while not stop.is_set():
        try:
            async with websockets.connect(COINBASE_WS_URL, ping_interval=20, max_size=2**22) as ws:
                await ws.send(json.dumps(subscribe))
                log.info("connected to Coinbase, subscribed to %s", ", ".join(cfg.products))
                backoff = 1
                while not stop.is_set():
                    raw = await asyncio.wait_for(ws.recv(), timeout=60)
                    publisher.handle_raw(raw)
        except (TimeoutError, OSError, websockets.WebSocketException) as exc:
            if stop.is_set():
                break
            log.warning("connection lost (%s), reconnecting in %ss", exc, backoff)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60)


async def stream_simulated(cfg: Config, publisher: KafkaPublisher, stop: asyncio.Event) -> None:
    """Random-walk trade generator with injected bad and late events."""
    prices = {"BTC-USD": 95_000.0, "ETH-USD": 3_400.0, "SOL-USD": 180.0}
    prices = {p: prices.get(p, 100.0) for p in cfg.products}
    trade_id = int(time.time() * 1000)
    delay = 1.0 / cfg.simulate_rate
    log.info("simulating %.0f trades/sec for %s", cfg.simulate_rate, ", ".join(cfg.products))

    while not stop.is_set():
        product = random.choice(cfg.products)
        prices[product] *= 1 + random.gauss(0, 0.0004)
        trade_id += 1
        event_time = datetime.now(UTC)

        roll = random.random()
        if roll < 0.01:  # malformed message
            raw = json.dumps({"type": "match", "product_id": product, "price": "N/A"})
        else:
            if roll < 0.015:  # very late, beyond the watermark, dropped by Spark
                event_time -= timedelta(minutes=5)
            elif roll < 0.03:  # late but within the watermark, still counted
                event_time -= timedelta(seconds=90)
            size = round(random.expovariate(1 / (0.05 if "BTC" in product else 1.5)), 6) or 0.0001
            if random.random() < 0.005:  # occasional burst to trigger volume spikes
                size *= 40
            raw = json.dumps(
                {
                    "type": "match",
                    "trade_id": trade_id,
                    "product_id": product,
                    "price": f"{prices[product]:.2f}",
                    "size": f"{size:.6f}",
                    "side": random.choice(["buy", "sell"]),
                    "time": event_time.isoformat(),
                }
            )
        publisher.handle_raw(raw)
        await asyncio.sleep(delay)


async def report_stats(cfg: Config, publisher: KafkaPublisher, stop: asyncio.Event) -> None:
    while not stop.is_set():
        await asyncio.sleep(cfg.stats_interval_sec)
        log.info(
            "published=%d dead_lettered=%d delivery_failures=%d",
            publisher.sent,
            publisher.dead_lettered,
            publisher.failed,
        )


async def main() -> None:
    cfg = Config()
    publisher = KafkaPublisher(cfg)
    stop = asyncio.Event()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)

    source = stream_simulated if cfg.source == "simulate" else stream_coinbase
    tasks = [
        asyncio.create_task(source(cfg, publisher, stop)),
        asyncio.create_task(report_stats(cfg, publisher, stop)),
    ]
    await stop.wait()
    log.info("shutting down")
    for t in tasks:
        t.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
    publisher.flush()


if __name__ == "__main__":
    asyncio.run(main())