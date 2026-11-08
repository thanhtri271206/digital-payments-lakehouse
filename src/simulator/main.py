"""CLI Entrypoint for the Digital Payments Simulator and Ground Truth G0 Generator.

Usage:
    uv run python -m simulator.main --seed 20260930 --count 1000 --rate 50 --mode both
    uv run python -m simulator.main --seed 20260930 --count 500 --export-g0-only
"""
import json
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional
import typer
from digital_payments_lakehouse.config.settings import get_config
from digital_payments_lakehouse.utils.logger import get_logger, setup_logger
from simulator.generator import PaymentGenerator
from simulator.ground_truth import GroundTruthExporter
from simulator.kafka_producer import PaymentKafkaProducer

app = typer.Typer(help="Digital Payments Lakehouse Simulator CLI")


@app.command()
def run(
    seed: int = typer.Option(20260930, "--seed", "-s", help="Deterministic random seed"),
    count: int = typer.Option(1000, "--count", "-c", help="Number of payment transactions to generate"),
    rate: float = typer.Option(50.0, "--rate", "-r", help="Events per second throttling rate (0 for unthrottled)"),
    mode: str = typer.Option("file", "--mode", "-m", help="Execution mode: 'kafka', 'file', or 'both'"),
    export_g0_only: bool = typer.Option(False, "--export-g0-only", help="Export Ground Truth G0 file only"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Simulate Kafka emission without live broker"),
    output_dir: str = typer.Option("data/ground_truth", "--output-dir", "-o", help="Directory for Ground Truth files"),
    events_dir: str = typer.Option("data/events_raw", "--events-dir", help="Directory for file-mode events export"),
) -> None:
    """Run payment simulation workload with deterministic fault injection and G0 ground truth."""
    logger = get_logger()
    logger.info(
        f"Starting Payment Simulation: seed={seed}, count={count}, rate={rate} eps, mode={mode}, dry_run={dry_run}"
    )

    start_time = time.perf_counter()
    config = get_config()

    # 1. Initialize Generator
    generator = PaymentGenerator(seed=seed, config=config)
    events, gt_records = generator.generate_batch(count=count)
    logger.info(f"Generated {len(events)} events across {len(gt_records)} transactions.")

    # 2. Export Ground Truth G0
    exporter = GroundTruthExporter(output_dir=output_dir)
    g0_path = exporter.export_records(gt_records, seed=seed)
    logger.info(f"Ground Truth G0 exported successfully to: {g0_path}")

    if export_g0_only:
        logger.info("Export G0 only requested. Completed.")
        return

    # 3. Output Events
    events_written = 0
    if mode in ("file", "both"):
        ev_dir = Path(events_dir)
        ev_dir.mkdir(parents=True, exist_ok=True)
        ev_path = ev_dir / f"events_seed_{seed}.jsonl"
        with open(ev_path, "w", encoding="utf-8") as f:
            for ev in events:
                f.write(ev.model_dump_json() + "\n")
        events_written = len(events)
        logger.info(f"Raw events exported to file: {ev_path}")

    events_produced = 0
    if mode in ("kafka", "both"):
        producer = PaymentKafkaProducer(
            bootstrap_servers="localhost:9092",
            topic="payment.events.raw",
            rate_per_sec=rate,
            dry_run=dry_run,
        )
        logger.info(f"Publishing events to Kafka topic 'payment.events.raw' (dry_run={producer.dry_run})...")
        for ev in events:
            producer.produce_event(ev)
        producer.flush(5.0)
        events_produced = producer.success_count
        logger.info(f"Published {events_produced} events to Kafka. Failed: {producer.failure_count}")

    duration = time.perf_counter() - start_time
    summary = {
        "seed": seed,
        "transactions_generated": len(gt_records),
        "events_generated": len(events),
        "events_file_written": events_written,
        "events_kafka_produced": events_produced,
        "g0_file": str(g0_path),
        "duration_seconds": round(duration, 3),
        "status": "SUCCESS",
    }
    logger.info(f"Simulation Run Summary: {json.dumps(summary)}")


if __name__ == "__main__":
    app()

