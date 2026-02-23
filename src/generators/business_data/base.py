"""
Base class for business data generators.

Provides common utilities for ID generation, date handling, and DCL payload
formatting that all source-system generators use.
"""

import random
import string
import uuid
from datetime import datetime, date, timedelta
from typing import Any, Dict, List, Optional


class BaseBusinessGenerator:
    """Base class for all business data source-system generators."""

    SOURCE_SYSTEM: str = ""  # Override in subclass
    PIPE_PREFIX: str = ""  # Override in subclass

    def __init__(self, seed: int = 42):
        self.seed = seed
        self._rng = random.Random(seed)

    def _sf_id(self, prefix: str = "00X") -> str:
        """Generate a Salesforce-style 18-character ID."""
        chars = string.ascii_letters + string.digits
        suffix = "".join(self._rng.choices(chars, k=15))
        return prefix + suffix

    def _ns_id(self) -> int:
        """Generate a NetSuite-style integer internal_id."""
        return self._rng.randint(10000, 9999999)

    def _cb_id(self, prefix: str = "sub") -> str:
        """Generate a Chargebee/Stripe-style string ID."""
        return f"{prefix}_{''.join(self._rng.choices(string.ascii_lowercase + string.digits, k=14))}"

    def _wd_id(self, prefix: str = "WRK") -> str:
        """Generate a Workday-style ID."""
        return f"{prefix}-{self._rng.randint(10000, 99999)}"

    def _uuid(self) -> str:
        """Generate a deterministic UUID-like string."""
        return str(uuid.UUID(int=self._rng.getrandbits(128), version=4))

    def _date_in_quarter(self, quarter: str) -> str:
        """Generate a random date within a quarter. Returns ISO date string."""
        year = int(quarter[:4])
        q = int(quarter[-1])
        month_start = (q - 1) * 3 + 1
        start = date(year, month_start, 1)
        if month_start + 2 <= 12:
            end_month = month_start + 2
            end_year = year
        else:
            end_month = (month_start + 2 - 1) % 12 + 1
            end_year = year + 1
        # Last day of quarter
        if end_month == 12:
            end = date(end_year, 12, 31)
        else:
            end = date(end_year, end_month + 1, 1) - timedelta(days=1)
        days = (end - start).days
        return (start + timedelta(days=self._rng.randint(0, days))).isoformat()

    def _timestamp_in_quarter(self, quarter: str) -> str:
        """Generate a random ISO timestamp within a quarter."""
        d = self._date_in_quarter(quarter)
        hour = self._rng.randint(0, 23)
        minute = self._rng.randint(0, 59)
        second = self._rng.randint(0, 59)
        return f"{d}T{hour:02d}:{minute:02d}:{second:02d}Z"

    def _quarter_start_date(self, quarter: str) -> str:
        """First day of a quarter as ISO date."""
        year = int(quarter[:4])
        q = int(quarter[-1])
        month = (q - 1) * 3 + 1
        return date(year, month, 1).isoformat()

    def _quarter_end_date(self, quarter: str) -> str:
        """Last day of a quarter as ISO date."""
        year = int(quarter[:4])
        q = int(quarter[-1])
        end_month = q * 3
        if end_month == 12:
            return date(year, 12, 31).isoformat()
        next_month = end_month + 1
        return (date(year, next_month, 1) - timedelta(days=1)).isoformat()

    def _posting_period(self, quarter: str) -> str:
        """Generate a posting period string like 'Jan 2024' within the quarter."""
        year = int(quarter[:4])
        q = int(quarter[-1])
        months = ["Jan", "Feb", "Mar", "Apr", "May", "Jun",
                   "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
        month_idx = (q - 1) * 3 + self._rng.randint(0, 2)
        return f"{months[month_idx]} {year}"

    def _weighted_choice(self, choices: List[Any], weights: List[float]) -> Any:
        """Pick from choices with given weights."""
        return self._rng.choices(choices, weights=weights, k=1)[0]

    def _pick(self, items: List[Any]) -> Any:
        """Pick a random item from a list."""
        return self._rng.choice(items)

    def _maybe_null(self, value: Any, null_pct: float = 0.03) -> Optional[Any]:
        """Return None with given probability, otherwise return value."""
        if self._rng.random() < null_pct:
            return None
        return value

    def format_dcl_payload(
        self,
        pipe_id: str,
        run_id: str,
        run_timestamp: str,
        schema_fields: List[Dict[str, Any]],
        data: List[Dict[str, Any]],
        time_range_start: str = "2024-01-01",
        time_range_end: str = "2026-12-31",
    ) -> Dict[str, Any]:
        """Format data as a DCL ingest payload."""
        return {
            "meta": {
                "source_system": self.SOURCE_SYSTEM,
                "pipe_id": pipe_id,
                "run_id": run_id,
                "run_timestamp": run_timestamp,
                "schema_version": "1.0",
                "record_count": len(data),
                "time_range": {
                    "start": time_range_start,
                    "end": time_range_end,
                },
            },
            "schema": {
                "fields": schema_fields,
            },
            "data": data,
        }
