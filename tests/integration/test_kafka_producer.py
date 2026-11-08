"""Integration tests for PaymentKafkaProducer and RateLimiter."""
import time
from datetime import datetime, timezone
import pytest
from simulator.generator import PaymentGenerator
from simulator.kafka_producer import PaymentKafkaProducer, RateLimiter
from simulator.models import PaymentEvent


def test_rate_limiter_throttling_accuracy():
    """Verify AC-4: Rate limiter enforces target rate within tolerance."""
    target_rate = 50.0  # 50 events per second
    limiter = RateLimiter(target_rate_per_sec=target_rate)

    start = time.perf_counter()
    num_events = 25
    for _ in range(num_events):
        limiter.wait()
    duration = time.perf_counter() - start

    expected_duration = (num_events - 1) * (1.0 / target_rate)
    # Tolerance margin of 20% on Windows timer resolution
    assert abs(duration - expected_duration) < 0.15


def test_producer_stamps_ingestion_time_utc():
    """Verify producer stamps ISO-8601 UTC ingestion_time onto PaymentEvent."""
    gen = PaymentGenerator(seed=20260930)
    events, _ = gen.generate_transaction()
    test_event = events[0]
    assert test_event.ingestion_time is None

    producer = PaymentKafkaProducer(dry_run=True, rate_per_sec=0)
    producer.produce_event(test_event)

    assert test_event.ingestion_time is not None
    assert test_event.ingestion_time.endswith("Z")
    dt = datetime.fromisoformat(test_event.ingestion_time.replace("Z", "+00:00"))
    assert dt.tzinfo is not None
    assert dt.utcoffset().total_seconds() == 0


def test_producer_dry_run_batch_execution():
    """Verify AC-2 in offline mode: 100% success rate with zero delivery failures."""
    gen = PaymentGenerator(seed=20260930)
    events, _ = gen.generate_batch(count=100)

    producer = PaymentKafkaProducer(dry_run=True, rate_per_sec=0)
    for ev in events:
        success = producer.produce_event(ev)
        assert success is True

    assert producer.success_count == len(events)
    assert producer.failure_count == 0
    flushed = producer.flush()
    assert flushed == 0

