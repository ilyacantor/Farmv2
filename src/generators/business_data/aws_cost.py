"""
AWS Cost Explorer data generator.

Produces AWS Cost Explorer API-shaped cost line items that sum to the
profile's cloud_spend (in millions) per quarter, distributed across
realistic services, regions, and accounts.
"""

from datetime import date, timedelta
from typing import Any, Dict, List, Tuple

from src.generators.business_data.base import BaseBusinessGenerator
from src.generators.business_data.profile import BusinessProfile


# ---------------------------------------------------------------------------
# Reference data
# ---------------------------------------------------------------------------

_SERVICES: List[Dict[str, Any]] = [
    # service, weight, primary usage_type, usage_unit, avg_unit_cost
    {"service": "Amazon EC2", "weight": 0.35, "usage_types": [
        ("BoxUsage:m5.xlarge", "Hours", 0.192),
        ("BoxUsage:m5.2xlarge", "Hours", 0.384),
        ("BoxUsage:c5.xlarge", "Hours", 0.170),
        ("BoxUsage:r5.xlarge", "Hours", 0.252),
        ("BoxUsage:t3.medium", "Hours", 0.0416),
        ("EBS:VolumeUsage.gp3", "GB-Mo", 0.08),
        ("DataTransfer-Out-Bytes", "GB", 0.09),
    ]},
    {"service": "Amazon RDS", "weight": 0.15, "usage_types": [
        ("InstanceUsage:db.r5.xlarge", "Hours", 0.48),
        ("InstanceUsage:db.r5.2xlarge", "Hours", 0.96),
        ("RDS:GP2-Storage", "GB-Mo", 0.115),
        ("RDS:BackupUsage", "GB-Mo", 0.095),
    ]},
    {"service": "Amazon S3", "weight": 0.10, "usage_types": [
        ("TimedStorage-ByteHrs", "GB-Mo", 0.023),
        ("Requests-Tier1", "Requests", 0.000005),
        ("Requests-Tier2", "Requests", 0.0000004),
        ("DataTransfer-Out-Bytes", "GB", 0.09),
    ]},
    {"service": "Amazon CloudFront", "weight": 0.08, "usage_types": [
        ("DataTransfer-Out-Bytes", "GB", 0.085),
        ("Requests-Tier1-HTTPS", "10K-Requests", 0.01),
    ]},
    {"service": "AWS Lambda", "weight": 0.07, "usage_types": [
        ("Lambda-GB-Second", "Seconds", 0.0000166667),
        ("Request", "Requests", 0.0000002),
    ]},
    {"service": "Amazon EKS", "weight": 0.08, "usage_types": [
        ("AmazonEKS-Hours:perCluster", "Hours", 0.10),
        ("BoxUsage:m5.xlarge", "Hours", 0.192),
        ("NatGateway-Hours", "Hours", 0.045),
        ("NatGateway-Bytes", "GB", 0.045),
    ]},
    {"service": "Amazon ElastiCache", "weight": 0.04, "usage_types": [
        ("NodeUsage:cache.r6g.xlarge", "Hours", 0.361),
        ("NodeUsage:cache.r6g.large", "Hours", 0.181),
    ]},
    {"service": "Amazon SQS", "weight": 0.02, "usage_types": [
        ("Requests", "Requests", 0.0000004),
    ]},
    {"service": "Amazon SNS", "weight": 0.01, "usage_types": [
        ("DeliveryAttempts-HTTP", "Notifications", 0.0000006),
        ("Requests-Tier1", "Requests", 0.0000005),
    ]},
    {"service": "AWS CloudWatch", "weight": 0.03, "usage_types": [
        ("MetricMonitorUsage", "Metrics", 0.30),
        ("TimedStorage-ByteHrs", "GB", 0.03),
        ("DataScanned-Bytes", "GB", 0.005),
    ]},
    {"service": "Amazon DynamoDB", "weight": 0.03, "usage_types": [
        ("WriteCapacityUnit-Hrs", "Hours", 0.00065),
        ("ReadCapacityUnit-Hrs", "Hours", 0.00013),
        ("TimedStorage-ByteHrs", "GB-Mo", 0.25),
    ]},
    {"service": "AWS Secrets Manager", "weight": 0.005, "usage_types": [
        ("SmSecrets", "Secrets", 0.40),
        ("SmApiCalls", "API-Calls", 0.05),
    ]},
    {"service": "Amazon Route 53", "weight": 0.005, "usage_types": [
        ("HostedZone", "Zones", 0.50),
        ("DNS-Queries", "Queries", 0.0000004),
    ]},
    {"service": "AWS WAF", "weight": 0.01, "usage_types": [
        ("WebACL", "ACLs", 5.0),
        ("Rule", "Rules", 1.0),
        ("Request", "Requests", 0.0000006),
    ]},
    {"service": "Amazon Kinesis", "weight": 0.02, "usage_types": [
        ("ShardHour", "Hours", 0.015),
        ("PUT-Payload-Units", "Units", 0.000014),
    ]},
]

_REGIONS = [
    ("us-east-1", 0.45),
    ("us-west-2", 0.25),
    ("eu-west-1", 0.15),
    ("ap-southeast-1", 0.10),
    ("eu-central-1", 0.03),
    ("ap-northeast-1", 0.02),
]

_ACCOUNTS = [
    ("112233445566", "prod"),
    ("223344556677", "staging"),
    ("334455667788", "dev"),
    ("445566778899", "shared-services"),
]

_ACCOUNT_COST_WEIGHTS = [0.65, 0.15, 0.10, 0.10]


class AWSCostGenerator(BaseBusinessGenerator):
    """Generates AWS Cost Explorer-shaped cost line item data."""

    SOURCE_SYSTEM = "aws_cost_explorer"
    PIPE_PREFIX = "aws_cost"

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _billing_periods_for_quarter(self, quarter: str) -> List[str]:
        """Return billing period strings (YYYY-MM) for each month in the quarter."""
        year = int(quarter[:4])
        q = int(quarter[-1])
        start_month = (q - 1) * 3 + 1
        periods = []
        for offset in range(3):
            m = start_month + offset
            y = year
            if m > 12:
                m -= 12
                y += 1
            periods.append(f"{y}-{m:02d}")
        return periods

    def _distribute_cost(
        self,
        total_cost: float,
        num_items: int,
    ) -> List[float]:
        """Distribute total_cost across num_items with realistic variance.

        Uses a Dirichlet-like approach: generate random weights, normalise,
        then scale to total_cost.
        """
        if num_items <= 0:
            return []
        raw = [max(self._rng.expovariate(1.0), 0.001) for _ in range(num_items)]
        total_raw = sum(raw)
        return [total_cost * (r / total_raw) for r in raw]

    # ------------------------------------------------------------------
    # Generation
    # ------------------------------------------------------------------

    def generate(self, profile: BusinessProfile) -> Dict[str, Any]:
        """Return ``{"cost_line_items": <DCL>}``."""
        run_id = self._uuid()
        run_ts = __import__("datetime").datetime.utcnow().isoformat() + "Z"

        all_items: List[Dict[str, Any]] = []

        for qm in profile.quarters:
            quarter = qm.quarter
            # cloud_spend is in millions; convert to actual dollars
            quarter_budget = qm.cloud_spend * 1_000_000
            billing_periods = self._billing_periods_for_quarter(quarter)

            # Target ~800-900 line items per quarter
            target_items = self._rng.randint(800, 900)

            # ----- Step 1: Allocate budget to services by weight ------
            service_weights = [s["weight"] for s in _SERVICES]
            total_w = sum(service_weights)
            service_budgets = [
                quarter_budget * (w / total_w) for w in service_weights
            ]
            # Add jitter per service
            service_budgets = [
                b * self._rng.uniform(0.90, 1.10) for b in service_budgets
            ]
            # Normalise back to total
            budget_sum = sum(service_budgets)
            service_budgets = [
                b * (quarter_budget / budget_sum) for b in service_budgets
            ]

            # ----- Step 2: Allocate items per service proportionally ---
            service_item_counts: List[int] = []
            for i, svc in enumerate(_SERVICES):
                weight_share = service_budgets[i] / quarter_budget
                items_for_svc = max(int(target_items * weight_share), 2)
                service_item_counts.append(items_for_svc)

            # Adjust to hit target total
            diff = target_items - sum(service_item_counts)
            if diff > 0:
                # Add extras to the largest service
                service_item_counts[0] += diff
            elif diff < 0:
                # Trim from largest
                service_item_counts[0] = max(service_item_counts[0] + diff, 2)

            # ----- Step 3: Generate line items per service -------------
            for svc_idx, svc_def in enumerate(_SERVICES):
                svc_name = svc_def["service"]
                svc_budget = service_budgets[svc_idx]
                num_items = service_item_counts[svc_idx]
                usage_types = svc_def["usage_types"]

                # Distribute this service's budget across its line items
                item_costs = self._distribute_cost(svc_budget, num_items)

                for item_cost in item_costs:
                    # Pick a usage type for this line item
                    usage_type_info = self._pick(usage_types)
                    usage_type, usage_unit, unit_cost = usage_type_info

                    # Compute a plausible usage_quantity from cost / unit_cost
                    if unit_cost > 0:
                        usage_quantity = item_cost / unit_cost
                    else:
                        usage_quantity = self._rng.uniform(100, 100000)
                    usage_quantity = round(usage_quantity, 2)

                    # unblended_cost is the base; blended slightly higher
                    unblended_cost = round(item_cost, 4)
                    markup = self._rng.uniform(1.02, 1.05)
                    blended_cost = round(unblended_cost * markup, 4)

                    # Region
                    region = self._weighted_choice(
                        [r[0] for r in _REGIONS],
                        [r[1] for r in _REGIONS],
                    )

                    # Account
                    account_idx = self._weighted_choice(
                        list(range(len(_ACCOUNTS))),
                        _ACCOUNT_COST_WEIGHTS,
                    )
                    account_id = _ACCOUNTS[account_idx][0]

                    # Billing period
                    billing_period = self._pick(billing_periods)

                    line_item = {
                        "billing_period": billing_period,
                        "service": svc_name,
                        "usage_type": usage_type,
                        "region": region,
                        "account_id": account_id,
                        "blended_cost": blended_cost,
                        "unblended_cost": unblended_cost,
                        "usage_quantity": usage_quantity,
                        "usage_unit": usage_unit,
                    }
                    all_items.append(line_item)

        # --- Schema definition -----------------------------------------
        schema: List[Dict[str, Any]] = [
            {"name": "billing_period", "type": "string"},
            {"name": "service", "type": "string"},
            {"name": "usage_type", "type": "string"},
            {"name": "region", "type": "string"},
            {"name": "account_id", "type": "string"},
            {"name": "blended_cost", "type": "number"},
            {"name": "unblended_cost", "type": "number"},
            {"name": "usage_quantity", "type": "number"},
            {"name": "usage_unit", "type": "string"},
        ]

        payload = self.format_dcl_payload(
            pipe_id=f"{self.PIPE_PREFIX}_line_items",
            run_id=run_id,
            run_timestamp=run_ts,
            schema_fields=schema,
            data=all_items,
        )

        return {
            "cost_line_items": payload,
        }
