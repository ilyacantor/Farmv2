"""
Scenario generator for DCL/BLL/NLQ ground truth system.

Generates deterministic multi-domain data for validating intent resolution
and aggregation correctness. All data is generated on-the-fly from seed.
"""
import random
import hashlib
from datetime import datetime, timedelta
from collections import defaultdict
from typing import Optional

from src.models.scenarios import (
    ScaleEnum,
    InvoiceStatus,
    AssetStatusEnum,
    RegionEnum,
    VendorCategory,
    CurrencyEnum,
    TimeRange,
    EntityCounts,
    PathologyInfo,
    CurrencyRates,
    ScenarioManifest,
    Customer,
    Invoice,
    Vendor,
    AssetStatus,
    RevenueMetric,
    MonthlyRevenue,
    RevenueMoMMetric,
    CustomerRevenue,
    TopCustomersMetric,
    VendorSpendItem,
    VendorSpendMetric,
    ResourceHealthMetric,
    TotalRevenueResponse,
    DateRange,
)


SCALE_CONFIGS = {
    ScaleEnum.small: {
        "customers_min": 50, "customers_max": 80,
        "vendors_min": 20, "vendors_max": 30,
        "invoices_min": 200, "invoices_max": 400,
        "assets_min": 100, "assets_max": 150,
    },
    ScaleEnum.medium: {
        "customers_min": 80, "customers_max": 150,
        "vendors_min": 30, "vendors_max": 40,
        "invoices_min": 400, "invoices_max": 700,
        "assets_min": 150, "assets_max": 300,
    },
    ScaleEnum.large: {
        "customers_min": 150, "customers_max": 200,
        "vendors_min": 40, "vendors_max": 50,
        "invoices_min": 700, "invoices_max": 1000,
        "assets_min": 300, "assets_max": 400,
    },
}

CUSTOMER_NAMES = [
    "Acme Corporation", "TechFlow Inc", "DataSync Solutions", "CloudBridge Ltd",
    "NetWorks Pro", "InfoStream Systems", "DigiPipe Industries", "FlowLogic LLC",
    "StreamForce Co", "PipeLink Technologies", "SyncMaster Corp", "DataPulse Inc",
    "ByteWave Systems", "CodeCraft Solutions", "NexGen Software", "AlphaLogic Corp",
    "BetaSoft Industries", "GammaData Inc", "DeltaTech Solutions", "EpsilonNet Corp",
    "ZetaCloud Services", "OmegaFlow Inc", "SigmaSoft LLC", "ThetaTech Corp",
    "IotaByte Solutions", "KappaData Systems", "LambdaLogic Inc", "MuTech Industries",
    "NuCloud Corp", "XiStream Solutions", "PiData LLC", "RhoSoft Inc",
    "TauTech Systems", "UpsilonNet Corp", "PhiLogic Industries", "ChiFlow Solutions",
    "PsiData Corp", "OmicronSoft Inc", "GlobalTech Partners", "MetroData Corp",
    "CityNet Solutions", "RegionalTech Inc", "NationalData Corp", "Continental Systems",
    "PacificRim Tech", "AtlanticData Corp", "EuroTech Solutions", "AsiaPacific Systems",
]

VENDOR_NAMES = [
    ("Microsoft", VendorCategory.software),
    ("Amazon Web Services", VendorCategory.cloud),
    ("Google Cloud", VendorCategory.cloud),
    ("Salesforce", VendorCategory.software),
    ("Oracle", VendorCategory.software),
    ("SAP", VendorCategory.software),
    ("IBM", VendorCategory.consulting),
    ("Accenture", VendorCategory.consulting),
    ("Deloitte", VendorCategory.consulting),
    ("Cisco", VendorCategory.infrastructure),
    ("Dell Technologies", VendorCategory.hardware),
    ("HP Enterprise", VendorCategory.hardware),
    ("VMware", VendorCategory.software),
    ("ServiceNow", VendorCategory.software),
    ("Workday", VendorCategory.software),
    ("Snowflake", VendorCategory.cloud),
    ("Databricks", VendorCategory.cloud),
    ("Splunk", VendorCategory.software),
    ("Palo Alto Networks", VendorCategory.infrastructure),
    ("CrowdStrike", VendorCategory.software),
    ("Okta", VendorCategory.software),
    ("Twilio", VendorCategory.services),
    ("Stripe", VendorCategory.services),
    ("Atlassian", VendorCategory.software),
    ("Slack", VendorCategory.software),
    ("Zoom", VendorCategory.software),
    ("DocuSign", VendorCategory.software),
    ("Box", VendorCategory.cloud),
    ("Dropbox", VendorCategory.cloud),
    ("HubSpot", VendorCategory.software),
    ("Zendesk", VendorCategory.software),
    ("Monday.com", VendorCategory.software),
    ("Asana", VendorCategory.software),
    ("Notion", VendorCategory.software),
    ("Figma", VendorCategory.software),
    ("Miro", VendorCategory.software),
    ("GitLab", VendorCategory.software),
    ("GitHub", VendorCategory.software),
    ("Datadog", VendorCategory.software),
    ("New Relic", VendorCategory.software),
    ("PagerDuty", VendorCategory.software),
    ("Cloudflare", VendorCategory.infrastructure),
    ("Fastly", VendorCategory.infrastructure),
    ("Akamai", VendorCategory.infrastructure),
    ("Verizon", VendorCategory.telecom),
    ("AT&T", VendorCategory.telecom),
    ("Lumen", VendorCategory.telecom),
    ("Staples", VendorCategory.office),
    ("Office Depot", VendorCategory.office),
    ("CDW", VendorCategory.hardware),
]

ASSET_NAMES = [
    "auth-service", "billing-api", "data-pipeline", "user-service",
    "notification-service", "search-api", "analytics-engine", "report-generator",
    "file-processor", "email-sender", "payment-gateway", "inventory-manager",
    "order-service", "customer-portal", "admin-dashboard", "api-gateway",
    "cache-layer", "queue-processor", "scheduler-service", "logging-aggregator",
    "metrics-collector", "config-server", "secret-manager", "load-balancer",
    "cdn-origin", "backup-service", "disaster-recovery", "audit-log",
    "compliance-checker", "security-scanner", "vulnerability-tracker", "asset-manager",
    "license-tracker", "cost-optimizer", "budget-monitor", "resource-planner",
    "capacity-manager", "performance-monitor", "health-checker", "alert-manager",
    "incident-responder", "runbook-executor", "deployment-pipeline", "artifact-store",
    "container-registry", "image-builder", "test-runner", "code-analyzer",
    "documentation-server", "wiki-service", "knowledge-base", "support-portal",
]


def parse_time_window(time_window: Optional[str], reference_date: Optional[datetime] = None) -> tuple[Optional[datetime], Optional[datetime], str]:
    """Parse a time window string into start/end dates and a human-readable period name.

    Args:
        time_window: Time window string (e.g., "last_year", "this_quarter", "q1", "2024")
        reference_date: Reference date for relative calculations (defaults to Dec 31, 2025)

    Returns:
        Tuple of (start_date, end_date, period_description)
        Returns (None, None, "All Time") if time_window is None or empty
    """
    if not time_window:
        return None, None, "All Time"

    # Use end of 2025 as reference since our data ends there
    ref = reference_date or datetime(2025, 12, 31)
    current_year = ref.year
    current_month = ref.month
    current_quarter = (current_month - 1) // 3 + 1

    time_window_lower = time_window.lower().strip()

    # Year-based windows
    if time_window_lower == "last_year":
        start = datetime(current_year - 1, 1, 1)
        end = datetime(current_year - 1, 12, 31, 23, 59, 59)
        return start, end, f"Last Year ({current_year - 1})"

    if time_window_lower == "this_year":
        start = datetime(current_year, 1, 1)
        end = datetime(current_year, 12, 31, 23, 59, 59)
        return start, end, f"This Year ({current_year})"

    if time_window_lower == "ytd":
        start = datetime(current_year, 1, 1)
        end = ref
        return start, end, f"Year to Date ({current_year})"

    # Specific year (e.g., "2024", "2025")
    if time_window_lower.isdigit() and len(time_window_lower) == 4:
        year = int(time_window_lower)
        start = datetime(year, 1, 1)
        end = datetime(year, 12, 31, 23, 59, 59)
        return start, end, str(year)

    # Quarter-based windows
    quarter_map = {"q1": 1, "q2": 2, "q3": 3, "q4": 4}
    if time_window_lower in quarter_map:
        q = quarter_map[time_window_lower]
        # Assume current year for explicit quarters
        start_month = (q - 1) * 3 + 1
        end_month = q * 3
        start = datetime(current_year, start_month, 1)
        # Last day of end_month
        if end_month == 12:
            end = datetime(current_year, 12, 31, 23, 59, 59)
        else:
            end = datetime(current_year, end_month + 1, 1) - timedelta(seconds=1)
        return start, end, f"Q{q} {current_year}"

    if time_window_lower == "this_quarter":
        start_month = (current_quarter - 1) * 3 + 1
        end_month = current_quarter * 3
        start = datetime(current_year, start_month, 1)
        if end_month == 12:
            end = datetime(current_year, 12, 31, 23, 59, 59)
        else:
            end = datetime(current_year, end_month + 1, 1) - timedelta(seconds=1)
        return start, end, f"Q{current_quarter} {current_year}"

    if time_window_lower == "last_quarter":
        if current_quarter == 1:
            # Last quarter of previous year
            start = datetime(current_year - 1, 10, 1)
            end = datetime(current_year - 1, 12, 31, 23, 59, 59)
            return start, end, f"Q4 {current_year - 1}"
        else:
            prev_q = current_quarter - 1
            start_month = (prev_q - 1) * 3 + 1
            end_month = prev_q * 3
            start = datetime(current_year, start_month, 1)
            end = datetime(current_year, end_month + 1, 1) - timedelta(seconds=1)
            return start, end, f"Q{prev_q} {current_year}"

    # Month-based windows
    if time_window_lower == "this_month":
        start = datetime(current_year, current_month, 1)
        if current_month == 12:
            end = datetime(current_year, 12, 31, 23, 59, 59)
        else:
            end = datetime(current_year, current_month + 1, 1) - timedelta(seconds=1)
        return start, end, f"{start.strftime('%B %Y')}"

    if time_window_lower == "last_month":
        if current_month == 1:
            start = datetime(current_year - 1, 12, 1)
            end = datetime(current_year - 1, 12, 31, 23, 59, 59)
        else:
            start = datetime(current_year, current_month - 1, 1)
            end = datetime(current_year, current_month, 1) - timedelta(seconds=1)
        return start, end, f"{start.strftime('%B %Y')}"

    # Unknown time window - return None to indicate invalid
    return None, None, "All Time"


class ScenarioGenerator:
    """Deterministic scenario generator for ground truth validation."""
    
    def __init__(self, scenario_id: str, seed: int, scale: ScaleEnum = ScaleEnum.medium):
        self.scenario_id = scenario_id
        self.seed = seed
        self.scale = scale
        self.rng = random.Random(seed)
        
        self.time_range = TimeRange(
            start_date="2024-01-01",
            end_date="2025-12-31"
        )
        
        self._customers: list[Customer] = []
        self._vendors: list[Vendor] = []
        self._invoices: list[Invoice] = []
        self._assets: list[AssetStatus] = []
        
        self._revenue_by_customer: dict[str, float] = {}
        self._spend_by_vendor: dict[str, tuple[float, int]] = {}
        self._monthly_revenue: dict[str, float] = {}
        
        self._pathologies = PathologyInfo()
        self._currency_rates = CurrencyRates()
        
        self._generated = False
    
    def _generate_id(self, prefix: str, index: int) -> str:
        """Generate deterministic ID based on prefix and index."""
        hash_input = f"{self.seed}:{prefix}:{index}"
        hash_val = hashlib.md5(hash_input.encode()).hexdigest()[:8]
        return f"{prefix}-{hash_val}"
    
    def _random_date_in_month(self, year: int, month: int) -> str:
        """Generate a random date within the specified year and month."""
        days_in_month = {
            1: 31, 2: 28, 3: 31, 4: 30, 5: 31, 6: 30,
            7: 31, 8: 31, 9: 30, 10: 31, 11: 30, 12: 31
        }
        # Handle leap year for February
        if month == 2 and year % 4 == 0 and (year % 100 != 0 or year % 400 == 0):
            max_day = 29
        else:
            max_day = days_in_month[month]

        day = self.rng.randint(1, max_day)
        dt = datetime(year, month, day, self.rng.randint(0, 23), self.rng.randint(0, 59))
        return dt.isoformat() + "Z"

    def _random_date_in_range(self, month: int) -> str:
        """Generate a random date within the specified month (legacy support, assumes 2025)."""
        return self._random_date_in_month(2025, month)
    
    def _generate_customers(self):
        """Generate customer records with hierarchy and pathologies."""
        config = SCALE_CONFIGS[self.scale]
        count = self.rng.randint(config["customers_min"], config["customers_max"])
        
        used_names = []
        duplicate_count = 0
        
        for i in range(count):
            if i < len(CUSTOMER_NAMES):
                base_name = CUSTOMER_NAMES[i]
            else:
                base_name = f"Enterprise {i - len(CUSTOMER_NAMES) + 1} Corp"
            
            if self.rng.random() < 0.05 and used_names:
                name = self.rng.choice(used_names)
                duplicate_count += 1
            else:
                name = base_name
                used_names.append(name)
            
            parent_id = None
            if i > 10 and self.rng.random() < 0.2:
                parent_idx = self.rng.randint(0, min(i - 1, 10))
                parent_id = self._customers[parent_idx].customer_id
            
            region = self.rng.choice(list(RegionEnum))
            # Customers created across the full 2-year range
            created_year = self.rng.choice([2024, 2025])
            created_month = self.rng.randint(1, 12)

            customer = Customer(
                customer_id=self._generate_id("CUST", i),
                name=name,
                region=region,
                parent_customer_id=parent_id,
                created_at=self._random_date_in_month(created_year, created_month)
            )
            self._customers.append(customer)
        
        self._pathologies.duplicate_customer_names = duplicate_count
    
    def _generate_vendors(self):
        """Generate vendor records."""
        config = SCALE_CONFIGS[self.scale]
        count = self.rng.randint(config["vendors_min"], config["vendors_max"])
        
        shuffled_vendors = list(VENDOR_NAMES)
        self.rng.shuffle(shuffled_vendors)
        
        for i in range(count):
            if i < len(shuffled_vendors):
                name, category = shuffled_vendors[i]
            else:
                name = f"Vendor {i + 1} Inc"
                category = self.rng.choice(list(VendorCategory))
            
            vendor = Vendor(
                vendor_id=self._generate_id("VEND", i),
                name=name,
                category=category
            )
            self._vendors.append(vendor)
    
    def _generate_invoices(self):
        """Generate invoice records with refunds and pathologies.

        Invoices are distributed across 24 months (Jan 2024 - Dec 2025)
        with realistic distribution patterns.
        """
        config = SCALE_CONFIGS[self.scale]
        count = self.rng.randint(config["invoices_min"], config["invoices_max"])

        # Scale up invoice count for 2-year range (was designed for 3 months)
        # Multiply by ~8 to maintain similar monthly density
        count = count * 8

        # Generate list of (year, month) tuples for 24 months
        months_in_range = [(2024, m) for m in range(1, 13)] + [(2025, m) for m in range(1, 13)]
        per_month = count // 24

        currency_variance = 0
        refund_count = 0
        orphaned_refs = 0

        for i in range(count):
            # Determine which month this invoice belongs to
            month_idx = min(i // per_month, 23) if per_month > 0 else self.rng.randint(0, 23)
            year, month = months_in_range[month_idx]

            customer = self.rng.choice(self._customers)
            vendor = self.rng.choice(self._vendors)

            currency = CurrencyEnum.USD
            if self.rng.random() < 0.15:
                currency = self.rng.choice([CurrencyEnum.EUR, CurrencyEnum.GBP, CurrencyEnum.JPY])
                currency_variance += 1

            base_amount = self.rng.uniform(100, 50000)
            amount = round(base_amount, 2)

            is_refund = False
            original_invoice_id = None
            if self.rng.random() < 0.10 and i > 20:
                is_refund = True
                refund_count += 1
                amount = -abs(amount * self.rng.uniform(0.1, 1.0))
                amount = round(amount, 2)

                if self.rng.random() < 0.1:
                    original_invoice_id = self._generate_id("INV", 999999)
                    orphaned_refs += 1
                else:
                    ref_idx = self.rng.randint(0, i - 1)
                    original_invoice_id = self._invoices[ref_idx].invoice_id

            invoice_date = self._random_date_in_month(year, month)
            due_date_dt = datetime.fromisoformat(invoice_date.rstrip("Z")) + timedelta(days=30)
            due_date = due_date_dt.isoformat() + "Z"

            if is_refund:
                status = InvoiceStatus.paid
            else:
                status = self.rng.choice([InvoiceStatus.paid, InvoiceStatus.pending, InvoiceStatus.overdue])

            invoice = Invoice(
                invoice_id=self._generate_id("INV", i),
                customer_id=customer.customer_id,
                vendor_id=vendor.vendor_id,
                amount=amount,
                currency=currency,
                invoice_date=invoice_date,
                due_date=due_date,
                status=status,
                is_refund=is_refund,
                original_invoice_id=original_invoice_id
            )
            self._invoices.append(invoice)

        self._pathologies.currency_variance_count = currency_variance
        self._pathologies.refund_invoices = refund_count
        self._pathologies.orphaned_references = orphaned_refs
    
    def _generate_assets(self):
        """Generate asset records with zombie/orphan distribution."""
        config = SCALE_CONFIGS[self.scale]
        count = self.rng.randint(config["assets_min"], config["assets_max"])
        
        stale_count = 0
        
        shuffled_names = list(ASSET_NAMES)
        self.rng.shuffle(shuffled_names)
        
        for i in range(count):
            if i < len(shuffled_names):
                name = shuffled_names[i]
            else:
                name = f"service-{i - len(shuffled_names) + 1}"
            
            roll = self.rng.random()
            if roll < 0.70:
                status = AssetStatusEnum.active
                days_back = self.rng.randint(0, 30)
            elif roll < 0.85:
                status = AssetStatusEnum.zombie
                days_back = self.rng.randint(90, 365)
                stale_count += 1
            else:
                status = AssetStatusEnum.orphan
                days_back = self.rng.randint(60, 180)
            
            last_activity = datetime(2025, 12, 31) - timedelta(days=days_back)
            last_activity_str = last_activity.isoformat() + "Z"
            
            governed = status == AssetStatusEnum.active or self.rng.random() < 0.3
            
            asset = AssetStatus(
                asset_id=self._generate_id("ASSET", i),
                name=name,
                status=status,
                last_activity_at=last_activity_str,
                governed=governed
            )
            self._assets.append(asset)
        
        self._pathologies.stale_timestamps = stale_count
    
    def _compute_metrics(self):
        """Pre-compute all ground truth metrics."""
        self._revenue_by_customer = defaultdict(float)
        for inv in self._invoices:
            if not inv.is_refund:
                rate = getattr(self._currency_rates, inv.currency.value)
                usd_amount = inv.amount * rate
                self._revenue_by_customer[inv.customer_id] += usd_amount
        
        self._spend_by_vendor: dict[str, list] = defaultdict(lambda: [0.0, 0])
        for inv in self._invoices:
            rate = getattr(self._currency_rates, inv.currency.value)
            usd_amount = abs(inv.amount) * rate
            self._spend_by_vendor[inv.vendor_id][0] += usd_amount
            self._spend_by_vendor[inv.vendor_id][1] += 1
        
        self._monthly_revenue = defaultdict(float)
        for inv in self._invoices:
            if not inv.is_refund:
                month = inv.invoice_date[:7]
                rate = getattr(self._currency_rates, inv.currency.value)
                usd_amount = inv.amount * rate
                self._monthly_revenue[month] += usd_amount
    
    def generate(self) -> ScenarioManifest:
        """Generate all scenario data and return manifest."""
        if self._generated:
            return self.get_manifest()
        
        self._generate_customers()
        self._generate_vendors()
        self._generate_invoices()
        self._generate_assets()
        self._compute_metrics()
        
        self._generated = True
        return self.get_manifest()
    
    def get_manifest(self) -> ScenarioManifest:
        """Return the scenario manifest."""
        zombie_count = sum(1 for a in self._assets if a.status == AssetStatusEnum.zombie)
        orphan_count = sum(1 for a in self._assets if a.status == AssetStatusEnum.orphan)
        
        return ScenarioManifest(
            scenario_id=self.scenario_id,
            seed=self.seed,
            scale=self.scale,
            created_at=datetime.utcnow().isoformat() + "Z",
            time_range=self.time_range,
            entity_counts=EntityCounts(
                invoices=len(self._invoices),
                customers=len(self._customers),
                vendors=len(self._vendors),
                assets=len(self._assets),
                zombies=zombie_count,
                orphans=orphan_count
            ),
            pathologies=self._pathologies,
            currency_rates=self._currency_rates
        )
    
    def get_revenue_metric(self) -> RevenueMetric:
        """Return total revenue metric."""
        total = sum(self._revenue_by_customer.values())
        return RevenueMetric(
            total_revenue=round(total, 2),
            currency="USD",
            period_start=self.time_range.start_date,
            period_end=self.time_range.end_date
        )
    
    def get_revenue_mom(self) -> RevenueMoMMetric:
        """Return month-over-month revenue metric."""
        months_sorted = sorted(self._monthly_revenue.keys())
        result = []
        
        prev_revenue = None
        for month in months_sorted:
            revenue = round(self._monthly_revenue[month], 2)
            
            delta_pct = None
            delta_abs = None
            if prev_revenue is not None and prev_revenue > 0:
                delta_abs = round(revenue - prev_revenue, 2)
                delta_pct = round((delta_abs / prev_revenue) * 100, 2)
            
            result.append(MonthlyRevenue(
                month=month,
                revenue=revenue,
                delta_pct=delta_pct,
                delta_abs=delta_abs
            ))
            prev_revenue = revenue
        
        return RevenueMoMMetric(months=result)

    def get_total_revenue(self, time_window: Optional[str] = None) -> TotalRevenueResponse:
        """Return total revenue metric with optional time filtering.

        Args:
            time_window: Optional time filter. Supported values:
                - "last_year" / "this_year" / "ytd" (year-to-date)
                - "last_quarter" / "this_quarter" / "q1" / "q2" / "q3" / "q4"
                - "last_month" / "this_month"
                - Specific year: "2024", "2025"
                - None: returns all-time total

        Returns:
            TotalRevenueResponse with revenue, period info, and date range
        """
        start_date, end_date, period_name = parse_time_window(time_window)

        # Filter invoices by date range
        total_revenue = 0.0
        transaction_count = 0

        for inv in self._invoices:
            if inv.is_refund:
                continue

            # Parse invoice date
            inv_date_str = inv.invoice_date.rstrip("Z")
            try:
                inv_date = datetime.fromisoformat(inv_date_str)
            except ValueError:
                continue

            # Apply time filter if specified
            if start_date and end_date:
                if not (start_date <= inv_date <= end_date):
                    continue

            # Convert to USD
            rate = getattr(self._currency_rates, inv.currency.value)
            usd_amount = inv.amount * rate
            total_revenue += usd_amount
            transaction_count += 1

        # Build response
        if start_date and end_date:
            date_range = DateRange(
                start=start_date.strftime("%Y-%m-%d"),
                end=end_date.strftime("%Y-%m-%d")
            )
            time_window_applied = time_window
        else:
            date_range = DateRange(
                start=self.time_range.start_date,
                end=self.time_range.end_date
            )
            time_window_applied = None

        return TotalRevenueResponse(
            total_revenue=round(total_revenue, 2),
            period=period_name,
            transaction_count=transaction_count,
            time_window_applied=time_window_applied,
            date_range=date_range
        )

    def get_top_customers(self, limit: int = 10, time_window: Optional[str] = None) -> TopCustomersMetric:
        """Return top customers by revenue with optional time filtering.

        Args:
            limit: Number of top customers to return (default 10)
            time_window: Optional time filter. Supported values:
                - "last_year" / "this_year" / "ytd" (year-to-date)
                - "last_quarter" / "this_quarter" / "q1" / "q2" / "q3" / "q4"
                - "last_month" / "this_month"
                - Specific year: "2024", "2025"
                - None: returns all-time totals

        Returns:
            TopCustomersMetric with customers ranked by revenue in the time period
        """
        start_date, end_date, _ = parse_time_window(time_window)

        # Compute revenue by customer for the specified time window
        revenue_by_customer: dict[str, float] = defaultdict(float)

        for inv in self._invoices:
            if inv.is_refund:
                continue

            # Apply time filter if specified
            if start_date and end_date:
                inv_date_str = inv.invoice_date.rstrip("Z")
                try:
                    inv_date = datetime.fromisoformat(inv_date_str)
                except ValueError:
                    continue
                if not (start_date <= inv_date <= end_date):
                    continue

            # Convert to USD
            rate = getattr(self._currency_rates, inv.currency.value)
            usd_amount = inv.amount * rate
            revenue_by_customer[inv.customer_id] += usd_amount

        total_revenue = sum(revenue_by_customer.values())

        sorted_customers = sorted(
            revenue_by_customer.items(),
            key=lambda x: x[1],
            reverse=True
        )[:limit]

        customer_map = {c.customer_id: c.name for c in self._customers}

        result = []
        for cust_id, revenue in sorted_customers:
            pct = round((revenue / total_revenue) * 100, 2) if total_revenue > 0 else 0
            result.append(CustomerRevenue(
                customer_id=cust_id,
                name=customer_map.get(cust_id, "Unknown"),
                revenue=round(revenue, 2),
                percent_of_total=pct
            ))

        return TopCustomersMetric(customers=result)
    
    def get_vendor_spend(self) -> VendorSpendMetric:
        """Return vendor spend breakdown."""
        vendor_map = {v.vendor_id: v.name for v in self._vendors}
        
        result = []
        for vendor_id, (spend, count) in self._spend_by_vendor.items():
            result.append(VendorSpendItem(
                vendor_id=vendor_id,
                name=vendor_map.get(vendor_id, "Unknown"),
                total_spend=round(spend, 2),
                invoice_count=count
            ))
        
        result.sort(key=lambda x: x.total_spend, reverse=True)
        return VendorSpendMetric(vendors=result)
    
    def get_resource_health(self) -> ResourceHealthMetric:
        """Return resource health metrics."""
        active_count = sum(1 for a in self._assets if a.status == AssetStatusEnum.active)
        zombie_count = sum(1 for a in self._assets if a.status == AssetStatusEnum.zombie)
        orphan_count = sum(1 for a in self._assets if a.status == AssetStatusEnum.orphan)
        
        zombie_ids = [a.asset_id for a in self._assets if a.status == AssetStatusEnum.zombie]
        orphan_ids = [a.asset_id for a in self._assets if a.status == AssetStatusEnum.orphan]
        
        return ResourceHealthMetric(
            active_count=active_count,
            zombie_count=zombie_count,
            orphan_count=orphan_count,
            zombie_ids=zombie_ids,
            orphan_ids=orphan_ids
        )
    
    def get_invoices(self) -> list[Invoice]:
        """Get all invoices in the scenario."""
        if not self._generated:
            self.generate()
        return self._invoices

    def get_customers(self) -> list[Customer]:
        """Get all customers in the scenario."""
        if not self._generated:
            self.generate()
        return self._customers

    def get_vendors(self) -> list[Vendor]:
        """Get all vendors in the scenario."""
        if not self._generated:
            self.generate()
        return self._vendors

    def get_invoice(self, invoice_id: str) -> Optional[Invoice]:
        """Get a specific invoice by ID."""
        for inv in self._invoices:
            if inv.invoice_id == invoice_id:
                return inv
        return None
    
    def verify_invoice(self, submitted: dict) -> dict:
        """Verify a submitted invoice against ground truth."""
        invoice_id = submitted.get("invoice_id")
        if not invoice_id:
            return {
                "is_valid": False,
                "invoice_id": "",
                "mismatches": [{"field": "invoice_id", "error": "Missing invoice_id"}],
                "message": "Invalid submission: missing invoice_id"
            }
        
        ground_truth = self.get_invoice(invoice_id)
        if not ground_truth:
            return {
                "is_valid": False,
                "invoice_id": invoice_id,
                "mismatches": [{"field": "invoice_id", "error": "Invoice not found"}],
                "message": f"Invoice {invoice_id} not found in scenario"
            }
        
        mismatches = []
        gt_dict = ground_truth.model_dump()
        
        for field, expected in gt_dict.items():
            if field in submitted:
                actual = submitted[field]
                if actual != expected:
                    mismatches.append({
                        "field": field,
                        "expected": expected,
                        "actual": actual
                    })
        
        is_valid = len(mismatches) == 0
        message = "Invoice verified successfully" if is_valid else f"Found {len(mismatches)} mismatches"
        
        return {
            "is_valid": is_valid,
            "invoice_id": invoice_id,
            "mismatches": mismatches,
            "message": message
        }


_scenario_cache: dict[str, ScenarioGenerator] = {}


def get_or_create_scenario(scenario_id: str, seed: int, scale: ScaleEnum = ScaleEnum.medium) -> ScenarioGenerator:
    """Get a cached scenario or create a new one."""
    cache_key = f"{scenario_id}:{seed}"
    
    if cache_key not in _scenario_cache:
        generator = ScenarioGenerator(scenario_id, seed, scale)
        generator.generate()
        _scenario_cache[cache_key] = generator
    
    return _scenario_cache[cache_key]


def clear_scenario_cache():
    """Clear the scenario cache."""
    _scenario_cache.clear()
