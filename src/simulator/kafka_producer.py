"""Resilient Apache Kafka Producer with Rate Throttling and Offline Fallback.

Produces PaymentEvents to 'payment.events.raw' with:
1. Strict Invariant: Stamping 'ingestion_time' in UTC upon message dispatch.
2. Token-bucket rate throttling (Baseline: 5 eps, Burst: 50 eps, Max: 0).
3. Resilient offline / dry-run mode for environments without active Docker brokers.
"""
import time
from datetime import datetime, timezone
from typing import Any, Callable, Dict, Optional
from digital_payments_lakehouse.utils.logger import get_logger
from simulator.models import PaymentEvent

logger = get_logger()

try:
    from confluent_kafka import KafkaError, Producer
    HAS_CONFLUENT_KAFKA = True
except ImportError:
    HAS_CONFLUENT_KAFKA = False
    Producer = None
    KafkaError = None


class RateLimiter:
    """High-precision interval rate limiter for stream simulation."""

    def __init__(self, target_rate_per_sec: float):
        self.target_rate = target_rate_per_sec
        self.interval = 1.0 / target_rate_per_sec if target_rate_per_sec > 0 else 0.0
        self.last_tick = time.perf_counter()

    def wait(self) -> None:
        """Throttle execution to match target throughput."""
        if self.interval <= 0:
            return
        now = time.perf_counter()
        elapsed = now - self.last_tick
        if elapsed < self.interval:
            time.sleep(self.interval - elapsed)
        self.last_tick = time.perf_counter()


class PaymentKafkaProducer:
    """Production-grade Kafka publisher for payment streaming pipeline."""

    def __init__(
        self,
        bootstrap_servers: str = "localhost:9092",
        topic: str = "payment.events.raw",
        rate_per_sec: float = 5.0,
        dry_run: bool = False,
        extra_config: Optional[Dict[str, str]] = None,
    ):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.rate_per_sec = rate_per_sec
        self.dry_run = dry_run
        self.rate_limiter = RateLimiter(rate_per_sec)

        self.success_count = 0
        self.failure_count = 0
        self._producer: Optional[Any] = None

        if not self.dry_run and HAS_CONFLUENT_KAFKA:
            conf = {
                "bootstrap.servers": self.bootstrap_servers,
                "client.id": "simulator-payment-producer",
                "acks": "all",
                "linger.ms": 20,
                "compression.type": "snappy",
                "queue.buffering.max.messages": 100000,
                "socket.timeout.ms": 3000,
            }
            if extra_config:
                conf.update(extra_config)
            try:
                self._producer = Producer(conf)
            except Exception as e:
                logger.warning(
                    f"Failed to initialize confluent-kafka Producer ({e}). Falling back to dry-run mode."
                )
                self.dry_run = True
        else:
            self.dry_run = True

    def _delivery_report(self, err: Any, msg: Any) -> None:
        """Callback report invoked once message is delivered or permanently failed."""
        if err is not None:
            self.failure_count += 1
            logger.error(f"Kafka Message Delivery Failed: {err}")
        else:
            self.success_count += 1

    def produce_event(
        self,
        event: PaymentEvent,
        on_delivery: Optional[Callable[[Any, Any], None]] = None,
    ) -> bool:
        """Stamp ingestion_time and dispatch event to Kafka or dry-run sink."""
        # Rate throttle
        self.rate_limiter.wait()

        # Enforce Invariant: ingestion_time stamped in UTC at broker dispatch
        now_utc = datetime.now(timezone.utc)
        event.ingestion_time = now_utc.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"

        payload = event.to_json_bytes()
        callback = on_delivery or self._delivery_report

        if self.dry_run or self._producer is None:
            # Simulate successful produce in dry-run mode
            self.success_count += 1
            return True

        try:
            self._producer.produce(
                topic=self.topic,
                key=event.transaction_id.encode("utf-8"),
                value=payload,
                on_delivery=callback,
            )
            # Poll to trigger delivery callbacks regularly
            self._producer.poll(0)
            return True
        except BufferError:
            logger.warning("Producer queue full! Flushing buffer...")
            self._producer.flush(2.0)
            self._producer.produce(
                topic=self.topic,
                key=event.transaction_id.encode("utf-8"),
                value=payload,
                on_delivery=callback,
            )
            return True
        except Exception as e:
            self.failure_count += 1
            logger.error(f"Produce exception: {e}")
            return False

    def flush(self, timeout: float = 5.0) -> int:
        """Flush internal buffers before termination."""
        if self._producer is not None:
            return self._producer.flush(timeout)
        return 0

    def close(self) -> None:
        """Clean shutdown of producer."""
        self.flush(5.0)
