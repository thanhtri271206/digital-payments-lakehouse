"""Ground Truth (G0) Exporter and Verification Module.

Exports the uncorrupted objective reality of transactions into JSON Lines format (G0_events_seed_{seed}.jsonl)
to serve as the benchmark for Precision and Recall evaluation across downstream DQ and 3-Way Reconciliation.
"""
import gzip
import json
from pathlib import Path
from typing import List, Optional, Union
from simulator.models import GroundTruthRecord


class GroundTruthExporter:
    """Exports Ground Truth G0 records to local storage and validates consistency."""

    def __init__(self, output_dir: Union[str, Path] = "data/ground_truth"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def get_export_path(self, seed: int, compressed: bool = False) -> Path:
        """Derive standard deterministic filename for ground truth file."""
        ext = ".jsonl.gz" if compressed else ".jsonl"
        return self.output_dir / f"G0_events_seed_{seed}{ext}"

    def export_records(
        self,
        records: List[GroundTruthRecord],
        seed: int,
        compressed: bool = False,
    ) -> Path:
        """Serialize Ground Truth records into deterministic JSON Lines file."""
        file_path = self.get_export_path(seed=seed, compressed=compressed)

        if compressed:
            with gzip.open(file_path, "wt", encoding="utf-8") as f:
                for rec in records:
                    f.write(rec.model_dump_json() + "\n")
        else:
            with open(file_path, "w", encoding="utf-8") as f:
                for rec in records:
                    f.write(rec.model_dump_json() + "\n")

        return file_path

    @staticmethod
    def load_records(file_path: Union[str, Path]) -> List[GroundTruthRecord]:
        """Read and validate Ground Truth records from JSON Lines file."""
        path = Path(file_path)
        records: List[GroundTruthRecord] = []

        is_gzip = path.suffix == ".gz"
        open_fn = gzip.open if is_gzip else open

        with open_fn(path, "rt", encoding="utf-8") as f:
            for line in f:
                stripped = line.strip()
                if stripped:
                    data = json.loads(stripped)
                    records.append(GroundTruthRecord.model_validate(data))

        return records

