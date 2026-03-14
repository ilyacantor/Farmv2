"""JSONL writer/reader for semantic triples."""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import List

from src.output.triple_format import SemanticTriple


class TripleWriter:
    """Write and read semantic triples as JSONL (one triple per line)."""

    def __init__(self, output_dir: str):
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)

    def write(
        self,
        triples: List[SemanticTriple],
        run_id: str,
        tenant_id: str,
    ) -> str:
        """Write triples to {output_dir}/{run_id}_triples.jsonl.

        Each line: one triple as JSON with run_id and tenant_id added.
        Returns the file path.
        """
        filename = f"{run_id}_triples.jsonl"
        filepath = os.path.join(self.output_dir, filename)

        with open(filepath, "w", encoding="utf-8") as f:
            for triple in triples:
                record = triple.to_dict()
                record["run_id"] = run_id
                record["tenant_id"] = tenant_id
                f.write(json.dumps(record, ensure_ascii=False) + "\n")

        return filepath

    @staticmethod
    def read(filepath: str) -> List[dict]:
        """Read triples from a JSONL file."""
        triples = []
        with open(filepath, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    triples.append(json.loads(line))
        return triples
