# Farm → DCL Contract Reference

> **Read-only reference.** Documents every field name, dimension value, metric key,
> and unit label that Farm pushes to DCL. Any Farm change that alters this document
> requires a coordinated update across Farm, DCL, and NLQ.
>
> Generated from source: `src/generators/` — branch `dev`

---

## Table of Contents

1. [Ground Truth Manifest Envelope](#1-ground-truth-manifest-envelope)
2. [Quarterly Metrics — v2.0 (113 scalars per quarter)](#2-quarterly-metrics--v20)
3. [Quarterly Metrics — v1.0 Legacy (23 per quarter)](#3-quarterly-metrics--v10-legacy)
4. [Dimensional Breakdowns (21 dimensions)](#4-dimensional-breakdowns)
5. [Dimension Value Enums](#5-dimension-value-enums)
6. [Expected Conflicts](#6-expected-conflicts)
7. [Source System Pipe Schemas](#7-source-system-pipe-schemas)
8. [Enterprise Snapshot Schema](#8-enterprise-snapshot-schema)
9. [SOR Scoring](#9-sor-scoring)
10. [Unit Labels](#10-unit-labels)

---

## 1. Ground Truth Manifest Envelope

Source: `src/generators/ground_truth.py:65-76`

```
{
  "manifest_version": "2.0" | "1.0",
  "run_id":           string,
  "generated_at":     ISO 8601 timestamp,
  "source_systems":   [string],
  "record_counts":    { pipe_id → int },
  "ground_truth": {
    "<quarter_key>":      { ...metric dicts... },   // e.g. "2024-Q1"
    "dimensional_truth":  { ...13 dimension dicts... },
    "expected_conflicts": [ ...conflict dicts... ]
  }
}
```

Quarter key format: `"YYYY-QN"` — 12 quarters from `2024-Q1` through `2026-Q4`.

Manifest validation (`validate_manifest_completeness`, line 424) requires every quarter present plus these core metrics: `revenue`, `arr`, `pipeline`, `win_rate`, `customer_count`, `headcount`, `attrition_rate`, `support_tickets`, `csat`, `sprint_velocity`, `gross_margin_pct`, `nrr`, `gross_churn_pct`. Also requires `dimensional_truth` with at least `revenue_by_region`, `pipeline_by_stage`, `headcount_by_department`; and an `expected_conflicts` list.

---

## 2. Quarterly Metrics — v2.0

Source: `src/generators/ground_truth.py:85-209`

Each metric is a dict: `{"value": <num>, "unit": "<unit>", "primary_source": "<source>"[, "corroborating_source": "<source>"]}`.

### ARR Waterfall

| Metric Key | Unit | Precision | Primary Source | Line |
|---|---|---|---|---|
| `beginning_arr` | `millions_usd` | 2 | `chargebee` | 93 |
| `new_arr` | `millions_usd` | 2 | `chargebee` | 94 |
| `new_logo_arr` | `millions_usd` | 2 | `salesforce+chargebee` | 95 |
| `expansion_arr` | `millions_usd` | 2 | `chargebee` | 96 |
| `churned_arr` | `millions_usd` | 2 | `chargebee` | 97 |
| `arr` | `millions_usd` | 2 | `chargebee` | 98 |
| `mrr` | `millions_usd` | 4 | `chargebee` | 99 |

### Revenue Decomposition

| Metric Key | Unit | Precision | Primary Source | Corroborating | Line |
|---|---|---|---|---|---|
| `revenue` | `millions_usd` | 2 | `netsuite` | `salesforce` | 102 |
| `new_logo_revenue` | `millions_usd` | 2 | `salesforce` | — | 103 |
| `expansion_revenue` | `millions_usd` | 2 | `chargebee` | — | 104 |
| `renewal_revenue` | `millions_usd` | 2 | `chargebee` | — | 105 |

### P&L

| Metric Key | Unit | Precision | Primary Source | Line |
|---|---|---|---|---|
| `cogs` | `millions_usd` | 2 | `netsuite` | 108 |
| `gross_profit` | `millions_usd` | 2 | `netsuite` | 109 |
| `gross_margin_pct` | `percent` | 1 | `netsuite` | 110 |
| `sm_expense` | `millions_usd` | 2 | `netsuite` | 111 |
| `rd_expense` | `millions_usd` | 2 | `netsuite` | 112 |
| `ga_expense` | `millions_usd` | 2 | `netsuite` | 113 |
| `opex` | `millions_usd` | 2 | `netsuite` | 114 |
| `ebitda` | `millions_usd` | 2 | `netsuite` | 115 |
| `ebitda_margin_pct` | `percent` | 1 | `netsuite` | 116 |
| `da_expense` | `millions_usd` | 2 | `netsuite` | 117 |
| `operating_profit` | `millions_usd` | 2 | `netsuite` | 118 |
| `operating_margin_pct` | `percent` | 1 | `netsuite` | 119 |
| `tax_expense` | `millions_usd` | 2 | `netsuite` | 120 |
| `net_income` | `millions_usd` | 2 | `netsuite` | 121 |
| `net_margin_pct` | `percent` | 1 | `netsuite` | 122 |
| `sga` | `millions_usd` | 2 | `netsuite` | — |

### Balance Sheet

| Metric Key | Unit | Precision | Primary Source | Line |
|---|---|---|---|---|
| `cash` | `millions_usd` | 2 | `netsuite` | 125 |
| `ar` | `millions_usd` | 2 | `netsuite` | 126 |
| `unbilled_revenue` | `millions_usd` | 2 | `netsuite` | 127 |
| `prepaid_expenses` | `millions_usd` | 2 | `netsuite` | 128 |
| `pp_e` | `millions_usd` | 2 | `netsuite` | 129 |
| `intangibles` | `millions_usd` | 2 | `netsuite` | 130 |
| `goodwill` | `millions_usd` | 2 | `netsuite` | 131 |
| `total_assets` | `millions_usd` | 2 | `netsuite` | 132 |
| `ap` | `millions_usd` | 2 | `netsuite` | 133 |
| `accrued_expenses` | `millions_usd` | 2 | `netsuite` | 134 |
| `deferred_revenue` | `millions_usd` | 2 | `netsuite` | 135 |
| `deferred_revenue_current` | `millions_usd` | 2 | `netsuite` | 136 |
| `deferred_revenue_lt` | `millions_usd` | 2 | `netsuite` | 137 |
| `total_liabilities` | `millions_usd` | 2 | `netsuite` | 138 |
| `retained_earnings` | `millions_usd` | 2 | `netsuite` | 139 |
| `stockholders_equity` | `millions_usd` | 2 | `netsuite` | 140 |

### Cash Flow

| Metric Key | Unit | Precision | Primary Source | Line |
|---|---|---|---|---|
| `cfo` | `millions_usd` | 2 | `netsuite` | 143 |
| `capex` | `millions_usd` | 2 | `netsuite` | 144 |
| `fcf` | `millions_usd` | 2 | `netsuite` | 145 |

### SaaS Metrics

| Metric Key | Unit | Precision | Primary Source | Line |
|---|---|---|---|---|
| `nrr` | `percent` | 1 | `chargebee` | 148 |
| `gross_churn_pct` | `percent` | 1 | `chargebee` | 149 |
| `logo_churn_pct` | `percent` | 1 | `salesforce` | 150 |
| `acv` | `millions_usd` | 4 | `salesforce` | 151 |
| `ltv` | `millions_usd` | 2 | `computed` | 152 |
| `cac` | `millions_usd` | 4 | `computed` | 153 |
| `ltv_cac_ratio` | `ratio` | 1 | `computed` | 154 |
| `magic_number` | `ratio` | 2 | `computed` | 155 |
| `burn_multiple` | `ratio` | 2 | `computed` | 156 |
| `rule_of_40` | `percent` | 1 | `computed` | 157 |
| `revenue_per_employee` | `millions_usd` | 4 | `computed` | 158 |
| `arr_per_employee` | `millions_usd` | 4 | `computed` | 159 |

### Pipeline

| Metric Key | Unit | Precision | Primary Source | Line |
|---|---|---|---|---|
| `pipeline` | `millions_usd` | 2 | `salesforce` | 162 |
| `win_rate` | `percent` | 1 | `salesforce` | 163 |
| `sales_cycle_days` | `days` | 0 | `salesforce` | 164 |
| `avg_deal_size` | `millions_usd` | 4 | `salesforce` | 165 |
| `quota_attainment` | `percent` | 1 | `salesforce` | 166 |
| `bookings` | `millions_usd` | 2 | `salesforce` | — |
| `qualified_pipeline` | `millions_usd` | 2 | `salesforce` | — |
| `reps_at_quota_pct` | `percent` | 1 | `salesforce` | — |

### Customer Metrics

| Metric Key | Unit | Precision | Primary Source | Line |
|---|---|---|---|---|
| `customer_count` | `count` | int | `salesforce` | 169 |
| `new_customers` | `count` | int | `salesforce` | 170 |
| `churned_customers` | `count` | int | `chargebee` | 171 |

### People

| Metric Key | Unit | Precision | Primary Source | Line |
|---|---|---|---|---|
| `headcount` | `count` | int | `workday` | 174 |
| `new_hires` | `count` | int | `workday` | 175 |
| `terminations` | `count` | int | `workday` | 176 |
| `attrition_rate` | `percent` | 1 | `workday` | 177 |
| `engineering_headcount` | `count` | int | `workday` | 178 |
| `sales_headcount` | `count` | int | `workday` | 179 |
| `open_roles` | `count` | int | `workday` | — |
| `cost_per_employee` | `millions_usd` | 4 | `computed` | — |
| `offer_acceptance_rate_pct` | `percent` | 1 | `workday` | — |
| `training_hours_per_employee` | `hours` | 1 | `workday` | — |
| `internal_mobility_rate_pct` | `percent` | 1 | `workday` | — |
| `span_of_control` | `ratio` | 1 | `workday` | — |
| `cs_headcount` | `count` | int | `workday` | — |
| `marketing_headcount` | `count` | int | `workday` | — |
| `product_headcount` | `count` | int | `workday` | — |
| `finance_headcount` | `count` | int | `workday` | — |
| `ga_headcount` | `count` | int | `workday` | — |

### Support

| Metric Key | Unit | Precision | Primary Source | Line |
|---|---|---|---|---|
| `support_tickets` | `count` | int | `zendesk` | 182 |
| `csat` | `score_5` | 2 | `zendesk` | 183 |
| `nps` | `score` | int | `zendesk` | 184 |
| `first_response_hours` | `hours` | 1 | `zendesk` | 185 |
| `resolution_hours` | `hours` | 1 | `zendesk` | 186 |

### Engineering

| Metric Key | Unit | Precision | Primary Source | Line |
|---|---|---|---|---|
| `sprint_velocity` | `story_points` | 1 | `jira` | 189 |
| `story_points` | `points` | 2 | `jira` | 190 |
| `features_shipped` | `count` | int | `jira` | 191 |
| `tech_debt_pct` | `percent` | 3 | `jira` | 192 |
| `code_coverage_pct` | `percent` | 1 | `jira` | — |
| `deployment_success_pct` | `percent` | 1 | `datadog` | — |
| `lead_time_days` | `days` | 1 | `jira` | — |
| `change_failure_rate` | `percent` | 1 | `datadog` | — |
| `bug_escape_rate` | `percent` | 1 | `jira` | — |
| `engineering_utilization` | `percent` | 1 | `jira` | — |

### Infrastructure

| Metric Key | Unit | Precision | Primary Source | Line |
|---|---|---|---|---|
| `cloud_spend` | `millions_usd` | 2 | `aws_cost_explorer` | 195 |
| `cloud_spend_pct_revenue` | `percent` | 2 | `aws_cost_explorer` | 196 |
| `p1_incidents` | `count` | int | `datadog` | 197 |
| `p2_incidents` | `count` | int | `datadog` | 198 |
| `incident_count` | `count` | int | `datadog` | 199 |
| `mttr_p1_hours` | `hours` | 1 | `datadog` | 200 |
| `mttr_p2_hours` | `hours` | 1 | `datadog` | 201 |
| `uptime_pct` | `percent` | 2 | `datadog` | 202 |
| `downtime_hours` | `hours` | 1 | `datadog` | 203 |
| `api_requests_millions` | `count` | 2 | `datadog` | — |
| `security_vulns` | `count` | int | `datadog` | — |
| `critical_bugs` | `count` | int | `jira` | — |

### Meta

| Key | Type | Line |
|---|---|---|
| `is_forecast` | boolean | 206 |

**Total: 113 scalar metrics + 1 boolean meta flag = 114 keys per quarter.**

---

## 3. Quarterly Metrics — v1.0 Legacy

Source: `src/generators/ground_truth.py:317-347`

Same dict structure as v2.0 but a 23-metric subset with no rounding applied (raw values except where noted).

| Metric Key | Unit | Precision | Primary Source | Corroborating | Line |
|---|---|---|---|---|---|
| `revenue` | `millions_usd` | 2 | `netsuite` | `salesforce` | 323 |
| `arr` | `millions_usd` | 2 | `chargebee` | — | 324 |
| `pipeline` | `millions_usd` | 2 | `salesforce` | — | 325 |
| `win_rate` | `percent` | raw | `salesforce` | — | 326 |
| `customer_count` | `count` | int | `salesforce` | — | 327 |
| `headcount` | `count` | int | `workday` | — | 328 |
| `attrition_rate` | `percent` | raw | `workday` | — | 329 |
| `support_tickets` | `count` | int | `zendesk` | — | 330 |
| `csat` | `score_5` | raw | `zendesk` | — | 331 |
| `sprint_velocity` | `story_points` | raw | `jira` | — | 332 |
| `gross_margin_pct` | `percent` | raw | `netsuite` | — | 333 |
| `nrr` | `percent` | raw | `chargebee` | — | 334 |
| `gross_churn_pct` | `percent` | raw | `chargebee` | — | 335 |
| `cloud_spend` | `millions_usd` | 2 | `aws_cost_explorer` | — | 336 |
| `incident_count` | `count` | int | `datadog` | — | 337 |
| `mttr_hours` | `hours` | raw | `datadog` | — | 338 |
| `new_customers` | `count` | int | `salesforce` | — | 339 |
| `churned_customers` | `count` | int | `chargebee` | — | 340 |
| `new_hires` | `count` | int | `workday` | — | 341 |
| `terminations` | `count` | int | `workday` | — | 342 |
| `mrr` | `millions_usd` | 4 | `chargebee` | — | 343 |
| `cogs` | `millions_usd` | 2 | `netsuite` | — | 344 |
| `opex` | `millions_usd` | 2 | `netsuite` | — | 345 |

**Note:** v1.0 uses `mttr_hours` (single combined MTTR); v2.0 splits into `mttr_p1_hours` + `mttr_p2_hours`.

---

## 4. Dimensional Breakdowns

Source: `src/generators/ground_truth.py:212-246`

Each dimension is a dict with `"source"` key plus per-quarter breakdowns. Values are `_r(v)` (rounded to 2 decimals) except `customers_by_segment` and `headcount_by_department` which are raw integers.

### v2.0 — 13 dimensions

| Dimension Key | Source | Breakdown Keys | Value Type | Line |
|---|---|---|---|---|
| `revenue_by_region` | `netsuite+salesforce` | `AMER`, `EMEA`, `APAC` | float | 232 |
| `revenue_by_segment` | `salesforce` | `Enterprise`, `Mid-Market`, `SMB` | float | 233 |
| `arr_by_region` | `chargebee` | `AMER`, `EMEA`, `APAC` | float | 234 |
| `arr_by_segment` | `chargebee` | `Enterprise`, `Mid-Market`, `SMB` | float | 235 |
| `pipeline_by_stage` | `salesforce` | `Lead`, `Qualified`, `Proposal`, `Negotiation`, `Closed-Won` | float | 236 |
| `pipeline_by_region` | `salesforce` | `AMER`, `EMEA`, `APAC` | float | 237 |
| `customers_by_segment` | `salesforce` | `Enterprise`, `Mid-Market`, `SMB` | int | 238 |
| `bookings_by_segment` | `salesforce+chargebee` | `Enterprise`, `Mid-Market`, `SMB` | float | 239 |
| `churn_by_segment` | `chargebee` | `Enterprise`, `Mid-Market`, `SMB` | float | 240 |
| `cogs_breakdown` | `netsuite` | `hosting`, `support_staff`, `professional_services`, `licenses`, `payment_processing` | float | 241 |
| `opex_breakdown` | `netsuite` | `sales_and_marketing`, `research_and_development`, `general_and_administrative` | float | 242 |
| `headcount_by_department` | `workday` | `Engineering`, `Product`, `Sales`, `Marketing`, `Customer Success`, `G&A` | int | 243 |
| `new_logo_revenue_by_region` | `salesforce` | `AMER`, `EMEA`, `APAC` | float | 244 |

### Rep-Level Dimensions

| Dimension Key | Source | Per Quarter | Breakdown Keys | Value Type |
|---|---|---|---|---|
| `quota_by_rep` | `salesforce` | 36 reps | `rep_id`, `rep_name`, `region`, `quota`, `attainment`, `quota_attainment_pct` | float |
| `pipeline_by_rep` | `salesforce` | 36 reps | `rep_id`, `rep_name`, `pipeline_value`, `deal_count`, `avg_deal_size` | float |
| `win_rate_by_rep` | `salesforce` | 36 reps | `rep_id`, `rep_name`, `opportunities`, `won`, `win_rate_pct` | float |
| `top_deals` | `salesforce` | top 10 deals | `deal_id`, `account_name`, `region`, `segment`, `amount`, `stage`, `close_date`, `rep_id`, `rep_name` | float |
| `stalled_deals` | `salesforce` | 3-8 deals (Proposal/Negotiation 60+ days) | `deal_id`, `account_name`, `days_in_stage`, `stage`, `amount`, `rep_id` | float |

### Department-Level Dimensions

| Dimension Key | Source | Per Quarter | Breakdown Keys | Value Type |
|---|---|---|---|---|
| `attrition_by_department` | `workday` | 6 departments | `attrition_count`, `attrition_rate_pct` | float |
| `engagement_by_department` | `workday` | 6 departments | `engagement_score` | float |
| `time_to_fill_by_department` | `workday` | 6 departments | `time_to_fill_days` | float |

### v1.0 Legacy — 3 dimensions

Source: `src/generators/ground_truth.py:350-370`

| Dimension Key | Source | Line |
|---|---|---|
| `revenue_by_region` | `netsuite+salesforce` | 367 |
| `pipeline_by_stage` | `salesforce` | 368 |
| `headcount_by_department` | `workday` | 369 |

---

## 5. Dimension Value Enums

All dimension value strings, exactly as they appear in the financial model and generators.

### Regions
Source: `farm_config.yaml` schema section, `financial_model.py:761-765`
```
"AMER"  "EMEA"  "APAC"
```

### Segments
Source: `financial_model.py:771-787`
```
"Enterprise"  "Mid-Market"  "SMB"
```

### Pipeline Stages
Source: `farm_config.yaml` schema section, `financial_model.py:477-483`
```
"Lead"  "Qualified"  "Proposal"  "Negotiation"  "Closed-Won"
```

### Departments (v2.0 / financial model)
Source: `financial_model.py:561-568`
```
"Engineering"  "Product"  "Sales"  "Marketing"  "Customer Success"  "G&A"
```

### Departments (v1.0 / profile.py)
Source: `profile.py:85`
```
"Engineering"  "Product"  "Marketing"  "CS"  "G&A"  "Sales"
```

**Important:** v2.0 uses `"Customer Success"`, v1.0 uses `"CS"`.

### COGS Components
Source: `financial_model.py:498-504`
```
"hosting"  "support_staff"  "professional_services"  "licenses"  "payment_processing"
```

### OpEx Components
Source: `financial_model.py:511-515`
```
"sales_and_marketing"  "research_and_development"  "general_and_administrative"
```

### Quarter Labels
12-quarter range: `2024-Q1` through `2026-Q4`.

---

## 6. Expected Conflicts

Source: `src/generators/ground_truth.py:249-310`

Three conflict types per quarter (v2.0), yielding 36 conflict records for 12 quarters.

### Revenue Timing Conflict

```json
{
  "metric": "revenue",
  "period": "<quarter>",
  "salesforce_value": "<revenue * 1.05>",
  "netsuite_value": "<revenue>",
  "delta_pct": 5.0,
  "root_cause": "rev_rec_timing",
  "explanation": "Salesforce books on close date, NetSuite recognizes on rev rec schedule start. ~$N in late-quarter deals recognized in following quarter."
}
```

### Headcount Contractor Conflict

```json
{
  "metric": "headcount",
  "period": "<quarter>",
  "workday_value": "<headcount + 3>",
  "reporting_value": "<headcount>",
  "delta": 3,
  "root_cause": "contractor_classification",
  "explanation": "Workday includes 3 contractors classified as contingent workers. Standard reporting excludes them."
}
```

### CSAT Missing Data Conflict

```json
{
  "metric": "csat",
  "period": "<quarter>",
  "ground_truth_value": "<csat>",
  "zendesk_reported_value": "<csat * 0.98>",
  "delta_pct": 4.0,
  "root_cause": "missing_satisfaction_data",
  "explanation": "~4.0% of solved tickets have no satisfaction rating. Zendesk averages only rated responses, slightly underreporting overall CSAT."
}
```

### v1.0 Legacy Conflicts

Source: `src/generators/ground_truth.py:373-404`

Same revenue timing conflict for all quarters. Headcount contractor conflict only when `headcount > 240`. No CSAT conflict.

---

## 7. Source System Pipe Schemas

### 7.1 Salesforce

Source: `src/generators/business_data/salesforce.py`

Generator tiers: **Tier 1**

**Pipe: `sf_users`** (line 243)

| Field | Type | Semantic Hint |
|---|---|---|
| `Id` | string | `is_key:True` |
| `Name` | string | — |
| `Role` | string | — |
| `Region__c` | string | `region` |
| `IsActive` | boolean | — |
| `HireDate` | date | — |

**Pipe: `sf_accounts`** (line 250)

| Field | Type | Semantic Hint |
|---|---|---|
| `Id` | string | `is_key:True` |
| `Name` | string | `account_name` |
| `Industry` | string | `industry` |
| `AnnualRevenue` | number | `annual_revenue` |
| `NumberOfEmployees` | number | — |
| `BillingCountry` | string | `country` |
| `Type` | string | — |
| `OwnerId` | string | — |

**Pipe: `sf_opportunities`** (line 257)

| Field | Type | Semantic Hint |
|---|---|---|
| `Id` | string | `is_key:True` |
| `Name` | string | — |
| `AccountId` | string | `account_reference` |
| `Amount` | number | `deal_value` |
| `StageName` | string | `pipeline_stage` |
| `CloseDate` | date | `close_date` |
| `OwnerId` | string | `sales_rep` |
| `Region__c` | string | `region` |
| `Segment__c` | string | `segment` |
| `ForecastCategory` | string | `forecast_category` |
| `IsClosed` | boolean | — |
| `IsWon` | boolean | — |
| `CreatedDate` | datetime | `created_date` |

Categorical values:

- **StageName**: `Prospecting`, `Qualification`, `Needs Analysis`, `Value Proposition`, `Id. Decision Makers`, `Perception Analysis`, `Proposal/Price Quote`, `Negotiation/Review`, `Closed Won`, `Closed Lost`
- **ForecastCategory**: `Pipeline`, `Best Case`, `Commit`, `Closed`, `Omitted`
- **Segment__c**: `Enterprise`, `Mid-Market`, `SMB`
- **Region__c**: `AMER`, `EMEA`, `APAC`
- **Account Type**: `Customer`, `Prospect`, `Partner`
- **Industry**: `Technology`, `Financial Services`, `Healthcare`, `Manufacturing`, `Retail`, `Media`, `Education`, `Professional Services`, `Energy`, `Government`

---

### 7.2 NetSuite

Source: `src/generators/business_data/netsuite.py`

Generator tiers: **Tier 1**

**Pipe: `ns-erp-001-invoices`** (line 807)

| Field | Type | Semantic Hint |
|---|---|---|
| `internal_id` | number | `is_key:True` |
| `tran_id` | string | — |
| `entity_id` | string | `customer_reference` |
| `tran_date` | date | `transaction_date` |
| `amount` | number | `revenue` |
| `currency` | string | — |
| `status` | string | — |
| `subsidiary` | string | — |
| `department` | string | — |
| `class_segment` | string | — |
| `posting_period` | string | `fiscal_period` |

**Pipe: `ns-erp-001-rev-schedules`** (line 814)

| Field | Type | Semantic Hint |
|---|---|---|
| `internal_id` | number | `is_key:True` |
| `source_tran_id` | string | — |
| `rev_rec_start` | date | `recognition_start` |
| `rev_rec_end` | date | `recognition_end` |
| `amount` | number | `recognized_revenue` |
| `schedule_type` | string | — |

**Pipe: `ns-erp-001-gl-entries`** (line 820)

| Field | Type | Semantic Hint |
|---|---|---|
| `internal_id` | number | `is_key:True` |
| `tran_date` | date | `transaction_date` |
| `account_number` | string | — |
| `account_name` | string | — |
| `debit` | number | — |
| `credit` | number | — |
| `department` | string | — |
| `class_segment` | string | — |
| `posting_period` | string | `fiscal_period` |

**Pipe: `ns-erp-001-ar`** (line 827)

| Field | Type | Semantic Hint |
|---|---|---|
| `internal_id` | number | `is_key:True` |
| `entity_id` | string | `customer_reference` |
| `due_date` | date | — |
| `amount_due` | number | `receivable_amount` |
| `amount_paid` | number | — |
| `days_outstanding` | number | — |
| `aging_bucket` | string | — |

**Pipe: `ns-erp-001-ap`** (line 834)

| Field | Type | Semantic Hint |
|---|---|---|
| `internal_id` | number | `is_key:True` |
| `vendor_id` | string | `vendor_reference` |
| `due_date` | date | — |
| `amount` | number | `payable_amount` |
| `status` | string | — |

Categorical values:

- **Invoice status**: `Paid In Full`, `Open`, `Partially Paid`
- **Currency**: `USD`, `EUR`, `GBP`
- **Subsidiary**: `AOS Corp - US`, `AOS Corp - EMEA`, `AOS Corp - APAC`
- **Department**: `Sales`, `Engineering`, `Marketing`, `CS`, `G&A`, `Product`
- **Class segment**: `SaaS`, `Professional Services`, `Support`, `Training`
- **Aging bucket**: `Current`, `1-30`, `31-60`, `61-90`, `90+`
- **AP vendor**: `AWS`, `Google Cloud`, `Azure`, `Datadog`, `Snowflake`, `WeWork`, `Salesforce`, `Okta`, `GitHub`, `Slack`, `Gusto`, `Brex`, `Stripe`, `HubSpot`, `Zoom`
- **AP status**: `Paid`, `Open`, `Pending Approval`
- **Rev schedule type**: `Straight-Line`, `Milestone`, `Usage-Based`
- **GL accounts**: `4000` Product Revenue, `4100` Service Revenue, `4200` Subscription Revenue, `5000` Cost of Revenue, `5100` Hosting Costs, `6000`-`6600` OpEx, `7000` Depreciation, `1000` Cash, `1100` AR, `2000` AP, `2100` Deferred Revenue

---

### 7.3 Chargebee

Source: `src/generators/business_data/chargebee.py`

Generator tiers: **Tier 1**

**Pipe: `cb_main_subscriptions`** (line 136)

| Field | Type | Semantic Hint |
|---|---|---|
| `id` | string | `is_key:True` |
| `customer_id` | string | `customer_reference` |
| `plan_id` | string | `plan_reference` |
| `plan_amount` | number | `plan_price` |
| `currency` | string | — |
| `status` | string | `subscription_status` |
| `started_at` | datetime | `subscription_start` |
| `current_term_start` | datetime | — |
| `current_term_end` | datetime | — |
| `mrr` | number | `monthly_recurring_revenue` |
| `cancelled_at` | datetime | — |

**Pipe: `cb_main_invoices`** (line 143)

| Field | Type | Semantic Hint |
|---|---|---|
| `id` | string | `is_key:True` |
| `subscription_id` | string | — |
| `customer_id` | string | `customer_reference` |
| `date` | date | `invoice_date` |
| `total` | number | `invoice_total` |
| `amount_paid` | number | — |
| `status` | string | — |
| `line_items` | array | — |

Categorical values:

- **Plan IDs**: `starter-monthly`, `starter-annual`, `professional-monthly`, `professional-annual`, `enterprise-monthly`, `enterprise-annual`, `enterprise-custom`
- **Plan amounts**: 299 (starter-mo), 2990 (starter-yr), 999 (pro-mo), 9990 (pro-yr), 4999 (ent-mo), 49990 (ent-yr), 0 (custom)
- **Subscription status**: `active`, `cancelled`, `non_renewing`, `paused`
- **Invoice status**: `paid`, `payment_due`, `voided`, `not_paid`
- **Currency**: `USD`, `EUR`, `GBP`
- **Line item types**: `plan`, `addon`
- **Addon names**: `Premium Support`, `Additional Seats`, `API Access Pack`, `Data Export Module`

---

### 7.4 Workday

Source: `src/generators/business_data/workday.py`

Generator tiers: **Tier 2**

**Pipe: `wd-workers-001`** (line 368)

| Field | Type | Semantic Hint |
|---|---|---|
| `Worker_ID` | string | `is_key:True` |
| `Legal_Name` | string | — |
| `Business_Title` | string | — |
| `Supervisory_Organization` | string | `department` |
| `Hire_Date` | date | `hire_date` |
| `Termination_Date` | date | — |
| `Worker_Status` | string | `employment_status` |
| `Management_Level` | string | — |
| `Location` | string | `location` |
| `Cost_Center` | string | — |
| `Annual_Base_Pay` | number | `compensation` |
| `Pay_Currency` | string | — |

**Pipe: `wd-positions-001`** (line 374)

| Field | Type | Semantic Hint |
|---|---|---|
| `Position_ID` | string | `is_key:True` |
| `Position_Title` | string | — |
| `Job_Family` | string | — |
| `Job_Profile` | string | — |
| `Supervisory_Organization` | string | `department` |
| `Worker_Count` | number | — |
| `Is_Filled` | boolean | — |

**Pipe: `wd-timeoff-001`** (line 381)

| Field | Type | Semantic Hint |
|---|---|---|
| `Worker_ID` | string | — |
| `Leave_Type` | string | — |
| `Start_Date` | date | — |
| `End_Date` | date | — |
| `Status` | string | — |

Categorical values:

- **Supervisory_Organization**: `Engineering`, `Product`, `Marketing`, `CS`, `G&A`, `Sales`
- **Management levels**: `Individual Contributor`, `Team Lead`, `Manager`, `Senior Manager`, `Director`, `VP`, `C-Suite`
- **Locations**: `San Francisco, CA`, `New York, NY`, `Austin, TX`, `Seattle, WA`, `Denver, CO`, `Chicago, IL`, `Boston, MA`, `Atlanta, GA`, `London, UK`, `Berlin, DE`, `Dublin, IE`, `Sydney, AU`, `Singapore, SG`, `Tokyo, JP`
- **Cost centers**: `CC-ENG-100/200/300`, `CC-PRD-100`, `CC-MKT-100/200`, `CC-CS-100/200`, `CC-GA-100/200/300`, `CC-SAL-100/200`
- **Leave types**: `Vacation`, `Sick`, `Personal`, `Parental`, `Bereavement`, `Jury Duty`
- **Leave status**: `Approved`, `Completed`, `Pending`, `Denied`
- **Pay currencies**: `USD`, `EUR`, `GBP`, `AUD`, `SGD`, `JPY`
- **Job families by dept**: Engineering (Software Engineering, DevOps, QA, Data Engineering, Security), Product (Product Management, Product Design, UX Research), Marketing (Growth Marketing, Content, Demand Gen, Brand), CS (Customer Success, Technical Support, Solutions Engineering, Onboarding), G&A (Finance, HR, Legal, IT, Operations, Recruiting), Sales (Account Executive, SDR/BDR, Sales Engineering, Sales Operations)

---

### 7.5 Zendesk

Source: `src/generators/business_data/zendesk.py`

Generator tiers: **Tier 2**

**Pipe: `zendesk_tickets`** (line 307)

| Field | Type | Semantic Hint |
|---|---|---|
| `id` | number | `is_key:True` |
| `subject` | string | — |
| `description` | string | — |
| `requester_id` | number | `customer_reference` |
| `assignee_id` | number | — |
| `group_id` | number | — |
| `priority` | string | — |
| `status` | string | — |
| `ticket_type` | string | — |
| `created_at` | datetime | — |
| `updated_at` | datetime | — |
| `solved_at` | datetime | — |
| `satisfaction_rating` | string | — |
| `tags` | array | — |
| `custom_fields` | array | — |

**Pipe: `zendesk_organizations`** (line 314)

| Field | Type | Semantic Hint |
|---|---|---|
| `id` | number | `is_key:True` |
| `name` | string | — |
| `domain_names` | array | — |
| `tags` | array | — |

Categorical values:

- **Priority**: `low`, `normal`, `high`, `urgent`
- **Status**: `new`, `open`, `pending`, `solved`, `closed`
- **Ticket type**: `problem`, `incident`, `question`, `task`
- **Satisfaction rating**: `good`, `offered`, `bad`, `null`
- **Product tags**: `billing`, `api`, `onboarding`, `performance`, `integration`, `authentication`, `dashboard`, `reporting`, `export`, `sso`, `webhooks`, `permissions`, `notifications`, `mobile`, `data-sync`
- **Custom field tiers**: `enterprise`, `pro`, `starter`
- **Custom field stages**: `new`, `renewal`, `expansion`

---

### 7.6 Jira

Source: `src/generators/business_data/jira_gen.py`

Generator tiers: **Tier 3**

**Pipe: `jira_issues`** (line 374)

| Field | Type | Semantic Hint |
|---|---|---|
| `key` | string | `is_key:True` |
| `summary` | string | — |
| `issuetype` | string | — |
| `status` | string | — |
| `priority` | string | — |
| `assignee` | string | — |
| `reporter` | string | — |
| `project` | string | — |
| `created` | datetime | — |
| `resolutiondate` | datetime | — |
| `story_points` | number | — |
| `sprint` | string | — |
| `epic_link` | string | — |
| `labels` | array | — |
| `components` | array | — |

**Pipe: `jira_sprints`** (line 381)

| Field | Type | Semantic Hint |
|---|---|---|
| `id` | number | `is_key:True` |
| `name` | string | — |
| `state` | string | — |
| `startDate` | datetime | — |
| `endDate` | datetime | — |
| `goal` | string | — |

Categorical values:

- **Projects**: `ENG`, `PLAT`, `INFRA`, `DATA`
- **Issue types**: `Story`, `Bug`, `Task`, `Epic`, `Sub-task`
- **Status**: `To Do`, `In Progress`, `In Review`, `Done`, `Closed`
- **Priority**: `Highest`, `High`, `Medium`, `Low`, `Lowest`
- **Story points**: `1`, `2`, `3`, `5`, `8`, `13`
- **Sprint state**: `closed`, `active`, `future`
- **Labels**: `tech-debt`, `customer-facing`, `security`, `performance`, `scalability`, `ux`, `documentation`, `compliance`, `migration`, `observability`, `reliability`, `cost-optimization`
- **Components**: `Backend`, `Frontend`, `API`, `Database`, `Infrastructure`, `DevOps`, `Security`, `Data`, `Mobile`, `Platform`

---

### 7.7 AWS Cost Explorer

Source: `src/generators/business_data/aws_cost.py`

Generator tiers: **Tier 3**

**Pipe: `aws_cost_line_items`** (line 279)

| Field | Type | Semantic Hint |
|---|---|---|
| `billing_period` | string | — |
| `service` | string | — |
| `usage_type` | string | — |
| `region` | string | — |
| `account_id` | string | — |
| `blended_cost` | number | — |
| `unblended_cost` | number | — |
| `usage_quantity` | number | — |
| `usage_unit` | string | — |

Categorical values:

- **Services** (15 total, with cost weights): `Amazon EC2` (0.35), `Amazon RDS` (0.15), `Amazon S3` (0.10), `Amazon CloudFront` (0.08), `AWS Lambda` (0.07), `Amazon EKS` (0.08), `Amazon ElastiCache` (0.04), `Amazon SQS` (0.02), `Amazon SNS` (0.01), `AWS CloudWatch` (0.03), `Amazon DynamoDB` (0.03), `AWS Secrets Manager` (0.005), `Amazon Route 53` (0.005), `AWS WAF` (0.01), `Amazon Kinesis` (0.02)
- **Regions**: `us-east-1` (0.45), `us-west-2` (0.25), `eu-west-1` (0.15), `ap-southeast-1` (0.10), `eu-central-1` (0.03), `ap-northeast-1` (0.02)
- **Accounts**: `112233445566` (prod), `223344556677` (staging), `334455667788` (dev), `445566778899` (shared-services)
- **Usage units**: `Hours`, `GB-Mo`, `GB`, `Requests`, `10K-Requests`, `Seconds`, `Metrics`, `Secrets`, `API-Calls`, `Zones`, `Queries`, `ACLs`, `Rules`, `Units`, `Notifications`
- **Billing period format**: `YYYY-MM`

---

### 7.8 Datadog

Source: `src/generators/business_data/datadog_gen.py`

Generator tiers: **Tier 3**

**Pipe: `datadog_incidents`** (line 306)

| Field | Type | Semantic Hint |
|---|---|---|
| `id` | string | `is_key:True` |
| `title` | string | — |
| `severity` | string | — |
| `status` | string | — |
| `created` | datetime | — |
| `resolved` | datetime | — |
| `time_to_detect` | number | — |
| `time_to_resolve` | number | — |
| `services` | array | — |
| `teams` | array | — |

**Pipe: `datadog_slos`** (line 313)

| Field | Type | Semantic Hint |
|---|---|---|
| `id` | string | `is_key:True` |
| `name` | string | — |
| `target_threshold` | number | — |
| `timeframe` | string | — |
| `status` | string | — |
| `error_budget_remaining` | number | — |

Categorical values:

- **Severity**: `SEV-1`, `SEV-2`, `SEV-3`, `SEV-4`
- **Incident status**: `active`, `stable`, `resolved`
- **SLO status**: `met`, `warning`, `breached`
- **SLO timeframes**: `7d`, `30d`, `90d`
- **Services**: `api-gateway`, `auth-service`, `payment-processor`, `search-service`, `notification-engine`, `data-pipeline`, `cdn`, `worker-queue`, `cache-layer`, `ml-inference`
- **Teams**: `platform`, `backend`, `frontend`, `data`, `security`, `sre`
- **`time_to_detect`**: minutes
- **`time_to_resolve`**: minutes (nullable for unresolved)

---

## 8. Enterprise Snapshot Schema

Source: `src/models/planes.py`

### 8.1 Snapshot Envelope

```
SnapshotResponse {
  meta:   SnapshotMeta
  planes: AllPlanes
}
```

**SnapshotMeta** fields (lines 369-381):

| Field | Type | Values |
|---|---|---|
| `schema_version` | string | `"farm.v1"` |
| `snapshot_id` | string | UUID |
| `tenant_id` | string | |
| `seed` | int | |
| `scale` | ScaleEnum | `small`, `medium`, `large`, `enterprise`, `mega` |
| `enterprise_profile` | EnterpriseProfileEnum | `modern_saas`, `regulated_finance`, `healthcare_provider`, `global_manufacturing` |
| `realism_profile` | RealismProfileEnum | `clean`, `typical`, `messy` |
| `created_at` | string | ISO 8601 |
| `counts` | dict | see 8.2 |
| `fabric_planes` | list[FabricPlaneInfo] | plane_type, vendor, is_healthy |
| `sors` | list[SORInfo] | domain, sor_name, sor_type, confidence |
| `industry` | string | IndustryVertical value |
| `snapshot_as_of` | string | alias for created_at |

### 8.2 Planes (AllPlanes)

8 planes in every snapshot (lines 340-348):

| Plane | Model | Collection Key | Record Model |
|---|---|---|---|
| `discovery` | `DiscoveryPlane` | `observations` | `DiscoveryObservation` |
| `idp` | `IdPPlane` | `objects` | `IdPObject` |
| `cmdb` | `CMDBPlane` | `cis` | `CMDBConfigItem` |
| `cloud` | `CloudPlane` | `resources` | `CloudResource` |
| `endpoint` | `EndpointPlane` | `devices` + `installed_apps` | `EndpointDevice` + `EndpointInstalledApp` |
| `network` | `NetworkPlane` | `dns` + `proxy` + `certs` | `NetworkDNS` + `NetworkProxy` + `NetworkCert` |
| `finance` | `FinancePlane` | `vendors` + `contracts` + `transactions` | `FinanceVendor` + `FinanceContract` + `FinanceTransaction` |
| `security` | `SecurityPlane` | `attestations` | `SecurityAttestation` |

### 8.3 Record Models — Field Reference

**DiscoveryObservation** (lines 163-174):
`observation_id`, `observed_at`, `source`, `observed_name`, `observed_uri`, `hostname`, `domain`, `vendor_hint`, `category_hint`, `environment_hint`, `raw`

- **source** (SourceEnum): `browser`, `dns`, `proxy`, `endpoint`, `cloud_api`, `network_scan`, `saas_audit_log`
- **category_hint** (CategoryHintEnum): `saas`, `service`, `database`, `infra`, `unknown`
- **environment_hint** (EnvironmentHintEnum): `prod`, `staging`, `dev`, `unknown`

**IdPObject** (lines 177-187):
`idp_id`, `name`, `idp_type`, `external_ref`, `has_sso`, `has_scim`, `vendor`, `last_login_at`, `domain`, `canonical_domain`

- **idp_type** (IdPTypeEnum): `application`, `service_principal`

**CMDBConfigItem** (lines 190-207):
`ci_id`, `name`, `ci_type`, `lifecycle`, `owner`, `owner_email`, `vendor`, `external_ref`, `canonical_domain`, `is_system_of_record`, `data_tier`, `data_domain`, `description`, `integrates_via`, `fabric_vendor`, `depends_on`

- **ci_type** (CITypeEnum): `app`, `service`, `database`, `infra`
- **lifecycle** (LifecycleEnum): `prod`, `staging`, `dev`
- **integrates_via**: `ipaas`, `api_gateway`, `event_bus`, `data_warehouse`
- **data_tier**: `gold`, etc.

**CloudResource** (lines 210-217):
`cloud_id`, `cloud_provider`, `resource_type`, `name`, `uri`, `region`, `tags`

- **cloud_provider** (CloudProviderEnum): `aws`, `azure`, `gcp`

**EndpointDevice** (lines 220-226):
`device_id`, `device_type`, `hostname`, `os`, `owner_email`, `last_seen_at`

- **device_type**: `laptop`, `desktop`, `server`, `mobile`, `tablet`
- **os**: `macOS 14.2`, `Windows 11`, `Windows 10`, `Ubuntu 22.04`, `CentOS 8`, `iOS 17`, `Android 14`

**EndpointInstalledApp** (lines 229-235):
`install_id`, `device_id`, `app_name`, `vendor`, `version`, `installed_at`

**NetworkDNS** (lines 238-242):
`dns_id`, `queried_domain`, `source_device`, `timestamp`

**NetworkProxy** (lines 245-250):
`proxy_id`, `url`, `domain`, `user_email`, `timestamp`

**NetworkCert** (lines 253-257):
`cert_id`, `domain`, `issuer`, `not_after`

- **issuer**: `DigiCert`, `Let's Encrypt`, `Comodo`, `GoDaddy`, `Amazon Trust Services`

**FinanceVendor** (lines 260-264):
`vendor_id`, `vendor_name`, `domain`, `annual_spend`

**FinanceContract** (lines 267-277):
`contract_id`, `vendor_name`, `product`, `start_date`, `end_date`, `owner_email`, `domain`, `annual_value`, `contract_type`, `contract_term_years`

**FinanceTransaction** (lines 280-288):
`txn_id`, `vendor_name`, `amount`, `currency`, `date`, `payment_type`, `is_recurring`, `memo`

- **payment_type** (PaymentTypeEnum): `invoice`, `expense`, `card`

**SecurityAttestation** (lines 324-333):
`attestation_id`, `asset_name`, `domain`, `vendor`, `attestation_date`, `attester_email`, `attestation_type`, `valid_until`, `notes`

### 8.4 Snapshot Counts Dict

Keys returned in `meta.counts` (from enterprise.py:1823-1837):

```
discovery_observations, idp_objects, cmdb_cis, cloud_resources,
endpoint_devices, endpoint_installed_apps, network_dns, network_proxy,
network_certs, finance_vendors, finance_contracts, finance_transactions,
security_attestations
```

### 8.5 Fabric Plane Enums

Source: `src/models/fabric.py`

**FabricPlaneType**: `ipaas`, `api_gateway`, `event_bus`, `data_warehouse`

**FabricRoute**: `via_ipaas`, `via_gateway`, `via_bus`, `via_warehouse`, `via_direct`

**IndustryVertical**: `default`, `finance`, `healthcare`, `manufacturing`, `logistics`, `tech_saas`, `retail`, `media`, `government`

**EnterprisePreset**: `preset_6_scrappy`, `preset_8_ipaas`, `preset_9_platform`, `preset_11_warehouse`

### 8.6 Fabric Plane Vendors

Source: `farm_config.yaml` vendors section

| Plane | Vendors |
|---|---|
| `ipaas` | `workato`, `mulesoft`, `boomi`, `tray.io`, `celigo`, `sap_integration_suite` |
| `api_gateway` | `kong`, `apigee`, `aws_api_gateway`, `azure_api_management` |
| `event_bus` | `kafka`, `confluent`, `eventbridge`, `rabbitmq`, `pulsar`, `azure_event_hubs` |
| `data_warehouse` | `snowflake`, `bigquery`, `redshift`, `databricks`, `synapse` |

---

## 9. SOR Scoring

Source: `src/services/sor_scoring.py`

### 9.1 Data Domains

```
customer  employee  financial  product  identity  it_assets
```

### 9.2 SOR Likelihood Levels

```
high  medium  low  none
```

### 9.3 Signal Weights

| Signal | Weight | Trigger |
|---|---|---|
| `cmdb_authoritative` | +40 | `is_system_of_record`, `data_tier=gold`, or `authoritative` flag |
| `known_sor_vendor` | +30 | Domain matches known SOR vendor list |
| `middleware_exporter` | +25 | Asset appears as data SOURCE in middleware routes |
| `enterprise_sso_scim` | +20 | Both SSO and SCIM enabled |
| `enterprise_contract` | +15 | Annual spend >= $50K or contract_type = enterprise |
| `high_corroboration` | +10 | Discovered across 4+ sources |
| `edge_app_penalty` | -20 | Niche TLD (.io, .app, .dev, .ai, .co) + single discovery source |

**MAX_SCORE**: 140 (sum of positive weights)

### 9.4 Confidence Thresholds

| Likelihood | Range |
|---|---|
| `high` | confidence >= 0.75 |
| `medium` | confidence >= 0.50 |
| `low` | confidence > 0, < 0.50 |
| `none` | confidence = 0 |

### 9.5 SOR Vendors by Domain

Source: `farm_config.yaml` vendors.sor_vendors_by_domain

| Data Domain | Vendor Domains |
|---|---|
| `customer` | `salesforce.com`, `hubspot.com`, `dynamics.com`, `dynamics365.com`, `zoho.com`, `pipedrive.com`, `freshworks.com`, `zendesk.com` |
| `employee` | `workday.com`, `adp.com`, `bamboohr.com`, `namely.com`, `paylocity.com`, `paychex.com`, `gusto.com`, `rippling.com`, `successfactors.com`, `ultipro.com`, `dayforce.com` |
| `financial` | `netsuite.com`, `quickbooks.com`, `xero.com`, `sage.com`, `intacct.com`, `freshbooks.com`, `oracle.com`, `sap.com` |
| `product` | `sap.com`, `oracle.com`, `epicor.com`, `infor.com`, `dynamics.com`, `netsuite.com` |
| `identity` | `okta.com`, `onelogin.com`, `auth0.com`, `ping.com`, `duo.com` |
| `it_assets` | `servicenow.com`, `freshservice.com`, `manageengine.com` |

### 9.6 Niche TLDs (Edge App Penalty)

```
.io  .app  .dev  .ai  .co
```

---

## 10. Unit Labels

Every unit string that appears in any ground truth metric, with its interpretation.

| Unit String | Meaning | Example Metrics |
|---|---|---|
| `millions_usd` | US dollars in millions (e.g. 83.6 = $83.6M) | `arr`, `revenue`, `cogs`, `ebitda`, `cash`, `pipeline`, `acv`, `cac`, `ltv` |
| `percent` | Percentage value (e.g. 32.0 = 32%) | `gross_margin_pct`, `ebitda_margin_pct`, `win_rate`, `attrition_rate`, `nrr`, `rule_of_40` |
| `count` | Integer count | `customer_count`, `headcount`, `new_hires`, `support_tickets`, `features_shipped`, `p1_incidents` |
| `ratio` | Dimensionless ratio | `ltv_cac_ratio`, `magic_number`, `burn_multiple` |
| `days` | Calendar days | `sales_cycle_days` |
| `hours` | Clock hours | `first_response_hours`, `resolution_hours`, `mttr_p1_hours`, `downtime_hours` |
| `score_5` | Score on 1-5 scale | `csat` |
| `score` | Integer score (NPS: -100 to +100) | `nps` |
| `story_points` | Agile story points per sprint | `sprint_velocity` |
| `points` | Total story points in quarter | `story_points` |

---

## Appendix: Generator Tiers

Source: `src/generators/business_data_orchestrator.py:33-35`

| Tier | Systems | Notes |
|---|---|---|
| Tier 1 | `salesforce`, `netsuite`, `chargebee` | Core revenue/finance systems |
| Tier 2 | `workday`, `zendesk` | People and support |
| Tier 3 | `jira`, `datadog`, `aws_cost_explorer` | Engineering and infra |

All tiers active by default. Order matters for record counts and cross-system references.
