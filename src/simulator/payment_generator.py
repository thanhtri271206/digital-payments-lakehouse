"""Payment Simulator Module.

Generates realistic payment event streams adhering to the Monotonic State Rank lifecycle
and injects anomalies (out-of-order, duplicates, late events, missing terminals,
poison records, terminal conflicts) according to project specifications (SRS / config.yaml).
Outputs Ground Truth (G0) to MinIO/local and streams events to Kafka or stdout.
"""

from __future__ import annotations

import argparse
import copy
from datetime import datetime, timedelta, timezone
import io
import json
import logging
from pathlib import Path
import random
from typing import Any, Optional
import uuid

from faker import Faker
import yaml

from simulator.models import (
    AnomalyType,
    EventType,
    GroundTruthRecord,
    LIFECYCLE_RANKS,
    PaymentChannel,
    PaymentEvent,
    PaymentMethod,
)

logger = logging.getLogger(__name__)


class PaymentSimulator:
    """Deterministic Simulator for Digital Payment Events and Ground Truth (G0)."""

    def __init__(
        self,
        config_path: str | Path = "config/config.yaml",
        seed: int = 20260930,
    ) -> None:
        """Initialize the simulator with configuration and deterministic random generator.

        Args:
            config_path: Path to config.yaml configuration file.
            seed: Deterministic random seed (default 20260930).
        """
        self.config_path = Path(config_path)
        self.config = self._load_config(self.config_path)

        # Fallback to config default_seed if specified, but respect explicit seed argument
        cfg_seed = (
            self.config.get("simulator", {}).get("default_seed")
            if isinstance(self.config, dict)
            else None
        )
        self.seed = seed if seed is not None else (cfg_seed or 20260930)

        # Initialize deterministic RNG and Faker
        self.rng = random.Random(self.seed)
        Faker.seed(self.seed)
        self.faker = Faker()

        # Ingestion rates & anomaly probabilities from config
        rates = self.config.get("simulator", {}).get("rates", {}) if isinstance(self.config, dict) else {}
        self.duplicate_rate = rates.get("duplicate_event_rate", 0.005)
        self.late_short_rate = rates.get("late_event_short_rate", 0.010)
        self.late_long_rate = rates.get("late_event_long_rate", 0.002)
        self.out_of_order_rate = rates.get("out_of_order_rate", 0.005)
        self.missing_terminal_rate = rates.get("missing_terminal_rate", 0.003)
        self.poison_record_rate = rates.get("poison_record_rate", 0.002)
        self.terminal_conflict_rate = rates.get("terminal_conflict_rate", 0.001)

        # Connection configs
        kafka_cfg = self.config.get("kafka", {}) if isinstance(self.config, dict) else {}
        self.kafka_bootstrap_servers = kafka_cfg.get("bootstrap_servers", "localhost:9092")
        self.kafka_topic_payment_events = kafka_cfg.get("topic_payment_events", "payment.events.raw")

        minio_cfg = self.config.get("minio", {}) if isinstance(self.config, dict) else {}
        self.minio_endpoint = minio_cfg.get("endpoint_url", "http://localhost:9000")
        self.minio_access_key = minio_cfg.get("access_key", "admin")
        self.minio_secret_key = minio_cfg.get("secret_key", "password123")
        self.minio_ground_truth_bucket = minio_cfg.get("bucket_ground_truth", "ground-truth")

        # Fixed master entity pools for realistic relationships
        self.users = [f"usr_{1000 + i}" for i in range(200)]
        self.merchants = [f"mer_{100 + i}" for i in range(30)]
        self.channels = [c.value for c in PaymentChannel]
        self.methods = [m.value for m in PaymentMethod]
        self.currency = "VND"

    def _load_config(self, path: Path) -> dict[str, Any]:
        """Safely load YAML configuration file."""
        if not path.exists():
            logger.warning("Config file %s not found. Using default configurations.", path)
            return {}
        try:
            with open(path, "r", encoding="utf-8") as f:
                return yaml.safe_load(f) or {}
        except Exception as e:
            logger.error("Failed to parse YAML config at %s: %s", path, e)
            return {}

    def _deterministic_uuid(self) -> str:
        """Generate a reproducible UUIDv4 using self.rng."""
        return str(uuid.UUID(int=self.rng.getrandbits(128), version=4))

    def _select_anomaly(self, force_anomaly: str | None = None) -> str:
        """Select anomaly type based on configured probabilities or override."""
        if force_anomaly is not None:
            return force_anomaly.upper()

        p = self.rng.random()
        cum_dup = self.duplicate_rate
        cum_late_short = cum_dup + self.late_short_rate
        cum_late_long = cum_late_short + self.late_long_rate
        cum_ooo = cum_late_long + self.out_of_order_rate
        cum_missing = cum_ooo + self.missing_terminal_rate
        cum_poison = cum_missing + self.poison_record_rate
        cum_conflict = cum_poison + self.terminal_conflict_rate

        if p < cum_dup:
            return AnomalyType.DUPLICATE.value
        if p < cum_late_short:
            return AnomalyType.LATE_SHORT.value
        if p < cum_late_long:
            return AnomalyType.LATE_LONG.value
        if p < cum_ooo:
            return AnomalyType.OUT_OF_ORDER.value
        if p < cum_missing:
            return AnomalyType.MISSING_TERMINAL.value
        if p < cum_poison:
            return AnomalyType.POISON_RECORD.value
        if p < cum_conflict:
            return AnomalyType.TERMINAL_CONFLICT.value
        return AnomalyType.NONE.value

    def generate_transaction(
        self,
        base_time: datetime | None = None,
        force_anomaly: str | None = None,
        as_dict: bool = False,
    ) -> tuple[list[PaymentEvent | dict[str, Any]], GroundTruthRecord]:
        """Generate an end-to-end payment transaction event sequence with optional fault injection.

        Follows monotonic lifecycle rank:
          Rank 1: INITIATED
          Rank 2: PAYMENT_METHOD_SELECTED
          Rank 3: AUTHORIZED
          Rank 4: PROCESSING
          Rank 5: COMPLETED / FAILED / CANCELLED / EXPIRED
          (Optional Rank 6: REFUNDED)

        Injected anomalies:
          - OUT_OF_ORDER: Swaps position of 2 events before emission.
          - DUPLICATE: Replicates one event in the chain with the identical event_id.
          - LATE_SHORT: Shifts event_time back ~30s-60s vs ingestion_time (within watermark).
          - LATE_LONG: Shifts event_time back > 150s vs ingestion_time (breaches 2m watermark -> DLQ).
          - MISSING_TERMINAL: Halts sequence at PROCESSING (triggers STUCK > 5m).
          - POISON_RECORD: Deliberately sets amount_minor = -50000.
          - TERMINAL_CONFLICT: Emits both COMPLETED and FAILED for the same transaction_id.

        Args:
            base_time: Timestamp to use as transaction initiation time (default UTC now).
            force_anomaly: Force a specific anomaly type (e.g. 'OUT_OF_ORDER', 'DUPLICATE').
            as_dict: If True, return events as list of dicts instead of PaymentEvent instances.

        Returns:
            A tuple of (events_list, GroundTruthRecord).
        """
        anomaly = self._select_anomaly(force_anomaly)
        tx_id = self._deterministic_uuid()
        user_id = self.rng.choice(self.users)
        merchant_id = self.rng.choice(self.merchants)
        channel = self.rng.choice(self.channels)
        method = self.rng.choice(self.methods)

        # Standard amount in integer minor units (VND)
        amount_choices = [
            10_000, 20_000, 50_000, 100_000, 200_000,
            350_000, 500_000, 1_000_000, 2_500_000, 5_000_000,
        ]
        true_amount_minor = self.rng.choice(amount_choices)

        # For POISON_RECORD: deliberately set amount_minor = -50000
        event_amount_minor = -50000 if anomaly == AnomalyType.POISON_RECORD.value else true_amount_minor

        t0 = base_time if base_time is not None else datetime.now(timezone.utc)
        if t0.tzinfo is None:
            t0 = t0.replace(tzinfo=timezone.utc)

        events: list[PaymentEvent] = []
        meta: dict[str, Any] = {}
        expected_resolution = "NORMAL_SETTLEMENT"
        final_state = "COMPLETED"

        # --- Step 1: INITIATED (Rank 1) ---
        t_evt_1 = t0
        t_ing_1 = t_evt_1 + timedelta(milliseconds=self.rng.randint(20, 100))
        events.append(
            PaymentEvent(
                event_id=self._deterministic_uuid(),
                transaction_id=tx_id,
                event_type=EventType.INITIATED.value,
                event_time=t_evt_1,
                ingestion_time=t_ing_1,
                user_id=user_id,
                merchant_id=merchant_id,
                payment_channel=channel,
                payment_method=method,
                amount_minor=event_amount_minor,
                currency=self.currency,
                status_reason=None,
                producer_version="v1.0",
            )
        )

        # --- Step 2: PAYMENT_METHOD_SELECTED (Rank 2) ---
        t_evt_2 = t_evt_1 + timedelta(seconds=self.rng.uniform(0.5, 2.0))
        t_ing_2 = t_evt_2 + timedelta(milliseconds=self.rng.randint(20, 100))
        events.append(
            PaymentEvent(
                event_id=self._deterministic_uuid(),
                transaction_id=tx_id,
                event_type=EventType.PAYMENT_METHOD_SELECTED.value,
                event_time=t_evt_2,
                ingestion_time=t_ing_2,
                user_id=user_id,
                merchant_id=merchant_id,
                payment_channel=channel,
                payment_method=method,
                amount_minor=event_amount_minor,
                currency=self.currency,
                status_reason=None,
                producer_version="v1.0",
            )
        )

        # --- Step 3: AUTHORIZED (Rank 3) ---
        t_evt_3 = t_evt_2 + timedelta(seconds=self.rng.uniform(0.5, 3.0))
        t_ing_3 = t_evt_3 + timedelta(milliseconds=self.rng.randint(20, 100))
        events.append(
            PaymentEvent(
                event_id=self._deterministic_uuid(),
                transaction_id=tx_id,
                event_type=EventType.AUTHORIZED.value,
                event_time=t_evt_3,
                ingestion_time=t_ing_3,
                user_id=user_id,
                merchant_id=merchant_id,
                payment_channel=channel,
                payment_method=method,
                amount_minor=event_amount_minor,
                currency=self.currency,
                status_reason=None,
                producer_version="v1.0",
            )
        )

        # --- Step 4: PROCESSING (Rank 4) ---
        t_evt_4 = t_evt_3 + timedelta(seconds=self.rng.uniform(0.5, 2.0))
        t_ing_4 = t_evt_4 + timedelta(milliseconds=self.rng.randint(20, 100))
        events.append(
            PaymentEvent(
                event_id=self._deterministic_uuid(),
                transaction_id=tx_id,
                event_type=EventType.PROCESSING.value,
                event_time=t_evt_4,
                ingestion_time=t_ing_4,
                user_id=user_id,
                merchant_id=merchant_id,
                payment_channel=channel,
                payment_method=method,
                amount_minor=event_amount_minor,
                currency=self.currency,
                status_reason=None,
                producer_version="v1.0",
            )
        )

        # --- Step 5: Terminal States (Rank 5) & Anomalies Handling ---
        if anomaly == AnomalyType.MISSING_TERMINAL.value:
            # Dừng chuỗi sự kiện tại PROCESSING (không sinh event Rank 5) -> triggers STUCK > 5m
            final_state = EventType.PROCESSING.value
            expected_resolution = "STUCK_PROCESSING"
            meta["detail"] = "Missing terminal event; transaction stops at PROCESSING"

        elif anomaly == AnomalyType.TERMINAL_CONFLICT.value:
            # Phát cả COMPLETED lẫn FAILED cho cùng một transaction_id
            t_evt_5a = t_evt_4 + timedelta(seconds=self.rng.uniform(1.0, 3.0))
            t_ing_5a = t_evt_5a + timedelta(milliseconds=self.rng.randint(20, 100))
            e_completed = PaymentEvent(
                event_id=self._deterministic_uuid(),
                transaction_id=tx_id,
                event_type=EventType.COMPLETED.value,
                event_time=t_evt_5a,
                ingestion_time=t_ing_5a,
                user_id=user_id,
                merchant_id=merchant_id,
                payment_channel=channel,
                payment_method=method,
                amount_minor=event_amount_minor,
                currency=self.currency,
                status_reason=None,
                producer_version="v1.0",
            )
            events.append(e_completed)

            t_evt_5b = t_evt_5a + timedelta(seconds=self.rng.uniform(1.0, 3.0))
            t_ing_5b = t_evt_5b + timedelta(milliseconds=self.rng.randint(20, 100))
            e_failed = PaymentEvent(
                event_id=self._deterministic_uuid(),
                transaction_id=tx_id,
                event_type=EventType.FAILED.value,
                event_time=t_evt_5b,
                ingestion_time=t_ing_5b,
                user_id=user_id,
                merchant_id=merchant_id,
                payment_channel=channel,
                payment_method=method,
                amount_minor=event_amount_minor,
                currency=self.currency,
                status_reason="BANK_ISSUER_REVERSAL_CONFLICT",
                producer_version="v1.0",
            )
            events.append(e_failed)

            final_state = "CONFLICT_COMPLETED_FAILED"
            expected_resolution = "LIFECYCLE_EXCEPTION"
            meta["detail"] = "Conflicting terminal events: COMPLETED followed by FAILED"

        else:
            # Normal terminal generation or other anomalies
            t_evt_5 = t_evt_4 + timedelta(seconds=self.rng.uniform(1.0, 4.0))
            t_ing_5 = t_evt_5 + timedelta(milliseconds=self.rng.randint(20, 100))

            term_roll = self.rng.random()
            if term_roll < 0.92:
                term_type = EventType.COMPLETED.value
                reason = None
            elif term_roll < 0.97:
                term_type = EventType.FAILED.value
                reason = self.rng.choice(
                    ["INSUFFICIENT_FUNDS", "BANK_NETWORK_TIMEOUT", "ISSUER_DECLINED"]
                )
            else:
                term_type = EventType.CANCELLED.value
                reason = "USER_CANCELLED"

            events.append(
                PaymentEvent(
                    event_id=self._deterministic_uuid(),
                    transaction_id=tx_id,
                    event_type=term_type,
                    event_time=t_evt_5,
                    ingestion_time=t_ing_5,
                    user_id=user_id,
                    merchant_id=merchant_id,
                    payment_channel=channel,
                    payment_method=method,
                    amount_minor=event_amount_minor,
                    currency=self.currency,
                    status_reason=reason,
                    producer_version="v1.0",
                )
            )
            final_state = term_type

            # Optional small probability of REFUNDED (Rank 6) from COMPLETED
            if term_type == EventType.COMPLETED.value and self.rng.random() < 0.01:
                t_evt_6 = t_evt_5 + timedelta(seconds=self.rng.uniform(5.0, 30.0))
                t_ing_6 = t_evt_6 + timedelta(milliseconds=self.rng.randint(20, 100))
                events.append(
                    PaymentEvent(
                        event_id=self._deterministic_uuid(),
                        transaction_id=tx_id,
                        event_type=EventType.REFUNDED.value,
                        event_time=t_evt_6,
                        ingestion_time=t_ing_6,
                        user_id=user_id,
                        merchant_id=merchant_id,
                        payment_channel=channel,
                        payment_method=method,
                        amount_minor=event_amount_minor,
                        currency=self.currency,
                        status_reason="CUSTOMER_REFUND_REQUEST",
                        producer_version="v1.0",
                    )
                )
                final_state = EventType.REFUNDED.value

        # --- Apply specific post-generation anomalies ---

        if anomaly == AnomalyType.OUT_OF_ORDER.value:
            # Hoán đổi vị trí của 2 sự kiện trước khi xuất ra
            if len(events) >= 2:
                idx = self.rng.randint(0, len(events) - 2)
                e1 = events[idx]
                e2 = events[idx + 1]

                # Swap order in list
                events[idx], events[idx + 1] = e2, e1
                # Swap ingestion times so that e2 is ingested before e1 (simulating network/pipeline arrival inversion)
                e2_ing = e2.ingestion_time
                e2.ingestion_time = e1.ingestion_time
                e1.ingestion_time = e2_ing

                meta["swapped_events"] = (e1.event_type, e2.event_type)
                meta["swapped_indices"] = (idx, idx + 1)
            expected_resolution = "WATERMARK_REORDERED"

        elif anomaly == AnomalyType.DUPLICATE.value:
            # Nhân bản 1 sự kiện bất kỳ trong chuỗi với cùng event_id
            target_idx = self.rng.randint(0, len(events) - 1)
            original_event = events[target_idx]
            dup_event = copy.deepcopy(original_event)

            # Re-ingest duplicate event with slightly delayed ingestion time
            dup_event.ingestion_time = original_event.ingestion_time + timedelta(
                milliseconds=self.rng.randint(50, 500)
            )

            # Insert duplicated event into the stream (adjacent or near the end)
            insert_pos = min(len(events), target_idx + 1 + self.rng.randint(0, 1))
            events.insert(insert_pos, dup_event)

            meta["duplicated_event_id"] = original_event.event_id
            meta["duplicated_event_type"] = original_event.event_type
            expected_resolution = "WATERMARK_DEDUPLICATED"

        elif anomaly == AnomalyType.LATE_SHORT.value:
            # Đặt event_time lùi lại khoảng 30s–60s so với ingestion_time (within watermark 120s)
            delay_sec = self.rng.uniform(30.0, 60.0)
            for ev in events:
                ev.event_time = ev.ingestion_time - timedelta(seconds=delay_sec)
            meta["lateness_seconds"] = round(delay_sec, 2)
            meta["within_watermark"] = True
            expected_resolution = "WITHIN_WATERMARK_PROCESSED"

        elif anomaly == AnomalyType.LATE_LONG.value:
            # Đặt event_time lùi lại > 150s (để vượt qua Watermark 2 phút: 120s -> DLQ)
            delay_sec = self.rng.uniform(160.0, 300.0)
            for ev in events:
                ev.event_time = ev.ingestion_time - timedelta(seconds=delay_sec)
            meta["lateness_seconds"] = round(delay_sec, 2)
            meta["within_watermark"] = False
            expected_resolution = "ROUTED_TO_DLQ"

        elif anomaly == AnomalyType.POISON_RECORD.value:
            # Cố tình gán amount_minor = -50000
            meta["poison_value"] = event_amount_minor
            expected_resolution = "ROUTED_TO_QUARANTINE"

        is_anomaly = anomaly != AnomalyType.NONE.value
        gt = GroundTruthRecord(
            transaction_id=tx_id,
            user_id=user_id,
            merchant_id=merchant_id,
            payment_channel=channel,
            payment_method=method,
            amount_minor=true_amount_minor,
            currency=self.currency,
            initiated_at=t0,
            final_state=final_state,
            is_anomaly=is_anomaly,
            anomaly_types=[anomaly] if is_anomaly else [],
            expected_resolution=expected_resolution,
            event_count=len(events),
            metadata=meta,
            created_at=datetime.now(timezone.utc),
        )

        if as_dict:
            return [e.to_dict() for e in events], gt
        return events, gt

    def generate_batch(
        self,
        n_transactions: int,
        base_time: datetime | None = None,
        interleave_events: bool = True,
        export_gt_path: str | Path | None = None,
        minio_bucket: str | None = None,
        minio_key: str | None = None,
        kafka_topic: str | None = None,
        dry_run: bool = False,
        as_dict: bool = True,
    ) -> tuple[list[dict[str, Any] | PaymentEvent], list[GroundTruthRecord]]:
        """Generate a batch of transactions, export Ground Truth, and publish to Kafka/stdout.

        Args:
            n_transactions: Number of payment transactions to generate.
            base_time: Starting timestamp.
            interleave_events: If True, interleave events based on ingestion time to simulate concurrent streams.
            export_gt_path: Local or S3 path to save Ground Truth (JSON or Parquet).
            minio_bucket: If provided, uploads Ground Truth to this MinIO bucket.
            minio_key: Object name in MinIO (defaults to ground_truth_YYYYMMDD_HHMMSS.parquet).
            kafka_topic: Target Kafka topic to stream events (e.g. 'payment.events.raw').
            dry_run: If True, prints events to stdout instead of sending to Kafka.
            as_dict: Return events as dicts.

        Returns:
            Tuple of (all_events, ground_truth_records).
        """
        all_events: list[PaymentEvent] = []
        all_ground_truth: list[GroundTruthRecord] = []

        curr_time = base_time if base_time is not None else datetime.now(timezone.utc)
        if curr_time.tzinfo is None:
            curr_time = curr_time.replace(tzinfo=timezone.utc)

        # Baseline throughput ~ 5 events/sec (or dt ~ 0.2s between transactions)
        for _ in range(n_transactions):
            tx_events, gt = self.generate_transaction(base_time=curr_time, as_dict=False)
            all_events.extend(tx_events)  # type: ignore[arg-type]
            all_ground_truth.append(gt)
            # Advance clock slightly for subsequent transactions
            curr_time += timedelta(milliseconds=self.rng.randint(100, 300))

        # Interleave events by ingestion_time while preserving out-of-order swapped ordering
        if interleave_events:
            # Stable sort by ingestion_time
            all_events.sort(key=lambda e: e.ingestion_time)

        # Export Ground Truth if path or MinIO bucket is requested
        if export_gt_path is not None or minio_bucket is not None:
            self._export_ground_truth(
                ground_truth_records=all_ground_truth,
                local_path=export_gt_path,
                minio_bucket=minio_bucket,
                minio_key=minio_key,
            )

        # Convert to dicts if required for serialization/output
        event_payloads = [e.to_dict() for e in all_events]

        # Publish to Kafka or Dry Run
        if dry_run:
            self._handle_dry_run(event_payloads, all_ground_truth)
        elif kafka_topic is not None:
            self._publish_to_kafka(event_payloads, topic=kafka_topic)

        final_events: list[dict[str, Any] | PaymentEvent] = (
            event_payloads if as_dict else all_events
        )
        return final_events, all_ground_truth

    def _export_ground_truth(
        self,
        ground_truth_records: list[GroundTruthRecord],
        local_path: str | Path | None = None,
        minio_bucket: str | None = None,
        minio_key: str | None = None,
    ) -> None:
        """Export Ground Truth records to local JSON/Parquet and/or MinIO."""
        records_data = [gt.to_dict() for gt in ground_truth_records]

        # 1. Local Export
        if local_path is not None:
            path = Path(local_path)
            path.parent.mkdir(parents=True, exist_ok=True)
            suffix = path.suffix.lower()

            if suffix == ".parquet":
                self._save_parquet(records_data, path)
                logger.info("Exported %d Ground Truth records to Parquet at %s", len(records_data), path)
            else:
                with open(path, "w", encoding="utf-8") as f:
                    json.dump(records_data, f, indent=2, default=str)
                logger.info("Exported %d Ground Truth records to JSON at %s", len(records_data), path)

        # 2. MinIO S3 Export
        if minio_bucket is not None:
            self._upload_to_minio(records_data, bucket=minio_bucket, key=minio_key)

    def _save_parquet(self, data: list[dict[str, Any]], target_file_or_buffer: Any) -> None:
        """Helper to write dictionaries to Parquet format using pyarrow with strict schema."""
        try:
            import pyarrow as pa
            import pyarrow.parquet as pq

            gt_schema = pa.schema([
                ("transaction_id", pa.string()),
                ("user_id", pa.string()),
                ("merchant_id", pa.string()),
                ("payment_channel", pa.string()),
                ("payment_method", pa.string()),
                ("amount_minor", pa.int64()),
                ("currency", pa.string()),
                ("initiated_at", pa.string()),
                ("final_state", pa.string()),
                ("is_anomaly", pa.bool_()),
                ("anomaly_types", pa.list_(pa.string())),
                ("expected_resolution", pa.string()),
                ("event_count", pa.int64()),
                ("metadata", pa.string()),
                ("created_at", pa.string()),
            ])

            # Sanitize records: convert nested dicts (like metadata) to JSON string for Parquet compatibility
            sanitized = []
            for item in data:
                row = dict(item)
                if isinstance(row.get("metadata"), dict):
                    row["metadata"] = json.dumps(row["metadata"]) if row["metadata"] else "{}"
                if isinstance(row.get("initiated_at"), datetime):
                    row["initiated_at"] = row["initiated_at"].isoformat()
                if isinstance(row.get("created_at"), datetime):
                    row["created_at"] = row["created_at"].isoformat()
                sanitized.append(row)

            table = pa.Table.from_pylist(sanitized, schema=gt_schema)
            pq.write_table(table, target_file_or_buffer)
        except Exception as e:
            logger.error("Failed to write Parquet: %s", e)
            raise

    def _upload_to_minio(
        self,
        records_data: list[dict[str, Any]],
        bucket: str,
        key: str | None = None,
    ) -> None:
        """Upload Ground Truth records to MinIO S3."""
        try:
            import boto3
            from botocore.client import Config

            s3_client = boto3.client(
                "s3",
                endpoint_url=self.minio_endpoint,
                aws_access_key_id=self.minio_access_key,
                aws_secret_access_key=self.minio_secret_key,
                config=Config(signature_version="s3v4"),
            )

            timestamp_str = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
            object_key = key or f"ground_truth_{timestamp_str}.parquet"

            parquet_buffer = io.BytesIO()
            self._save_parquet(records_data, parquet_buffer)
            parquet_buffer.seek(0)

            s3_client.put_object(
                Bucket=bucket,
                Key=object_key,
                Body=parquet_buffer.getvalue(),
            )
            logger.info("Successfully uploaded Ground Truth to MinIO: s3://%s/%s", bucket, object_key)
        except Exception as e:
            logger.warning(
                "Could not upload Ground Truth to MinIO (endpoint=%s, bucket=%s): %s",
                self.minio_endpoint,
                bucket,
                e,
            )

    def _publish_to_kafka(self, events: list[dict[str, Any]], topic: str) -> None:
        """Stream events to Kafka topic using confluent-kafka producer."""
        try:
            from confluent_kafka import Producer

            conf = {
                "bootstrap.servers": self.kafka_bootstrap_servers,
                "client.id": "payment-simulator-producer",
            }
            producer = Producer(conf)

            delivered_count = 0

            def delivery_report(err: Any, msg: Any) -> None:
                nonlocal delivered_count
                if err is not None:
                    logger.error("Message delivery failed: %s", err)
                else:
                    delivered_count += 1

            for ev in events:
                tx_id = ev["transaction_id"]
                payload = json.dumps(ev, default=str).encode("utf-8")
                producer.produce(
                    topic=topic,
                    key=tx_id.encode("utf-8"),
                    value=payload,
                    callback=delivery_report,
                )
                producer.poll(0)

            producer.flush(timeout=15.0)
            logger.info("Delivered %d/%d events to Kafka topic '%s'", delivered_count, len(events), topic)
        except Exception as e:
            logger.error("Kafka publishing error: %s", e)
            raise

    def _handle_dry_run(
        self,
        events: list[dict[str, Any]],
        ground_truth: list[GroundTruthRecord],
    ) -> None:
        """Print summary and sample events to stdout for inspection."""
        anomaly_counts: dict[str, int] = {}
        for gt in ground_truth:
            for anom in gt.anomaly_types or ["NONE"]:
                anomaly_counts[anom] = anomaly_counts.get(anom, 0) + 1

        print("=" * 70)
        print(" PAYMENT SIMULATOR [DRY-RUN EXECUTION SUMMARY]")
        print("=" * 70)
        print(f"Total Transactions: {len(ground_truth)}")
        print(f"Total Events Generated: {len(events)}")
        print("Anomaly Breakdown:")
        for anom, cnt in sorted(anomaly_counts.items()):
            pct = (cnt / len(ground_truth)) * 100
            print(f"  - {anom:<22}: {cnt:>5} ({pct:.1f}%)")
        print("-" * 70)
        print("Sample First 3 Emitted Events:")
        for i, ev in enumerate(events[:3], 1):
            print(f"[{i}] Event: {ev['event_type']} | Tx: {ev['transaction_id'][:8]}.. | Amount: {ev['amount_minor']} {ev['currency']} | Time: {ev['event_time']}")
        print("=" * 70)


def main() -> None:
    """CLI Entrypoint for the Payment Simulator."""
    parser = argparse.ArgumentParser(description="Payment Simulator for Digital Payments Lakehouse")
    parser.add_argument("--config", default="config/config.yaml", help="Path to config.yaml")
    parser.add_argument("--seed", type=int, default=20260930, help="Deterministic RNG seed")
    parser.add_argument("--n-tx", type=int, default=50, help="Number of transactions to generate")
    parser.add_argument("--kafka-topic", default="payment.events.raw", help="Target Kafka topic")
    parser.add_argument("--export-gt", default=None, help="Local export path for Ground Truth (.json or .parquet)")
    parser.add_argument("--minio-bucket", default=None, help="MinIO bucket to upload Ground Truth")
    parser.add_argument("--dry-run", action="store_true", help="Print dry-run output to stdout instead of sending to Kafka")

    args = parser.parse_args()

    simulator = PaymentSimulator(config_path=args.config, seed=args.seed)
    simulator.generate_batch(
        n_transactions=args.n_tx,
        export_gt_path=args.export_gt,
        minio_bucket=args.minio_bucket,
        kafka_topic=args.kafka_topic if not args.dry_run else None,
        dry_run=args.dry_run,
    )


if __name__ == "__main__":
    main()
