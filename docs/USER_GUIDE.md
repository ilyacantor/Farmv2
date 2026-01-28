# AOS Farm Operator Guide

Welcome to AOS Farm, the Test Oracle for AutonomOS. This guide will help you use the platform to generate test data, run validations, and stress test your systems.

---

## Getting Started

When you open AOS Farm, you'll see a navigation bar at the top with these tabs:

| Tab | Module | Purpose |
|-----|--------|---------|
| **Overview** | - | System status and quick actions |
| **AOD** | AutonomOS Discover | Test data discovery and reconciliation |
| **AOA** | AutonomOS Agents | Stress test agent orchestration |
| **NLQ** | Natural Language Query | Ground truth datasets for query validation |
| **DCL** | Data Contract Library | Toxic stream testing and repair verification |
| **Guide** | - | This documentation |

---

## AOD Tab: Discovery Testing

The AOD (AutonomOS Discover) tab is your workspace for testing data discovery systems.

### Creating a Snapshot

A snapshot is a synthetic dataset representing an enterprise's systems, integrations, and data flows.

1. In the left panel, find the **Snapshots** section
2. Click **Generate New Snapshot**
3. Configure your snapshot:
   - **Organization Name**: Give it a memorable name (e.g., "TestCorp Q4")
   - **Preset**: Choose an enterprise archetype:
     - *iPaaS-Centric*: Heavy use of integration platforms like MuleSoft
     - *Warehouse-Centric*: Data warehouse as the integration hub
     - *Platform-Oriented*: Balanced modern platform approach
     - *API Gateway*: API-first architecture
     - *Event-Driven*: Streaming and event bus focus
     - *Scrappy*: Direct connections (small business style)
   - **Scale**: Small, Medium, or Large dataset size
   - **Seed** (optional): A number for reproducible results
4. Click **Generate**
5. Wait for the snapshot to appear in the list

### Running a Reconciliation

A reconciliation checks if AOD correctly discovered what was expected.

1. Select a snapshot from the dropdown
2. Paste AOD's discovery results (or use test input)
3. Click **Run Reconciliation**
4. Review the results:
   - **Green**: Matches expected
   - **Yellow**: Partial match
   - **Red**: Missing or incorrect

### Understanding Grading Metrics

| Metric | What it measures |
|--------|------------------|
| Accuracy | Overall correctness percentage |
| Precision | How many found items were correct |
| Recall | How many expected items were found |
| F1 Score | Balance between precision and recall |

---

## AOA Tab: Agent Stress Testing

The AOA (AutonomOS Agents) tab tests how your agent orchestration system handles load and chaos.

### Generating an Agent Fleet

1. Go to the **AOA** tab
2. Set the number of agents to generate
3. Optionally set a seed for reproducible results
4. Click **Generate Fleet**

This creates synthetic agent profiles with different capabilities, permissions, and behaviors.

### Generating Workflows

1. Choose a workflow pattern:
   - *Linear*: Step-by-step tasks
   - *DAG*: Branching decision trees
   - *Parallel*: Concurrent task execution
   - *Saga*: Multi-step with rollback capabilities
2. Set the number of nodes
3. Click **Generate Workflow**

### Running a Stress Test

1. Generate both a fleet and workflows
2. Configure chaos injection (optional):
   - *Tool Timeout*: Simulates slow responses
   - *Agent Conflict*: Multiple agents claiming same task
   - *Memory Pressure*: High resource usage
   - *Network Issues*: Connection problems
3. Click **Run Stress Test**
4. Monitor results in the stress test history

---

## NLQ Tab: Query Ground Truth

The NLQ (Natural Language Query) tab provides ground truth datasets for validating query systems.

### Generating a Scenario

1. Go to the **NLQ** tab
2. Choose a scale (Small, Medium, or Large)
3. Optionally enter a seed number for reproducibility
4. Click **Generate Scenario**

The scenario includes customers, invoices, vendors, and assets spanning multiple months.

### Viewing Ground Truth Metrics

After generating a scenario, you'll see five metric cards showing the "correct answers":

1. **Total Revenue**: Combined revenue for the period
2. **Month-over-Month**: Revenue trend between months
3. **Top Customers**: Highest revenue customers by rank
4. **Vendor Spend**: Spending breakdown by vendor
5. **Resource Health**: Active vs. zombie vs. orphan assets

### How to Use for NLQ Validation

1. Generate a scenario with a specific seed
2. Run your NLQ query (e.g., "Who are our top 5 customers?")
3. Compare your system's answer to Farm's ground truth
4. The results should match

---

## DCL Tab: Data Contract Testing

The DCL (Data Contract Library) tab tests how your data ingestion system handles bad data.

### Setting Up

1. Go to the **DCL** tab
2. If no scenario is loaded, click **Quick Generate** to create one
3. The scenario provides the source data for testing

### Starting a Toxic Stream

A toxic stream is data with intentional errors to test DCL's resilience.

1. Adjust the **Chaos Rate** slider (0-100%)
   - 0% = clean data
   - 20% = occasional errors
   - 50% = heavy chaos
2. Select which chaos types to include:
   - **Missing Fields**: Required fields omitted
   - **Duplicates**: Same record repeated
   - **Invalid Currency**: Wrong currency codes
   - **Stale Timestamps**: Very old dates
   - **Orphan Refs**: References to non-existent records
3. Click **Start Stream**
4. A new tab opens with the streaming data

### The DCL Test Flow

The integration flow panel shows the expected workflow:

1. **Ingest Toxic Stream** - DCL receives bad data
2. **DCL Detects Chaos** - Identifies problems via metadata flags
3. **Fetch Source** - Retrieves pristine data from Farm
4. **Repair Record** - DCL fixes the data
5. **Verify Fix** - Confirms repair matches source

### Looking Up Source Data

When DCL detects a bad record, it needs the correct version:

1. Enter the Invoice ID in the lookup field
2. Click **Lookup**
3. The pristine (correct) record appears
4. DCL uses this to repair the damaged record

### Verifying Repairs

After DCL repairs a record, verify it's correct:

1. Paste DCL's repaired record in the text box
2. Click **Verify Repair**
3. Results show:
   - **VALID**: Repair matches source
   - **INVALID**: Lists the mismatched fields

---

## Tips for Success

### Reproducibility
- Use the same **seed** number to generate identical datasets
- This is essential for comparing results across test runs
- Document which seeds you use for important tests

### Understanding Results
- **100% accuracy** is the goal but not always realistic
- Focus on improving the metrics that matter most
- Compare results over time to track improvements

### Cross-Module Testing
- Generate a scenario in NLQ, then use it in DCL
- Scenarios are shared across tabs once generated
- The same scenario can test both query accuracy and data repair

---

## Troubleshooting

**Scenario not loading?**
Scenarios are stored in memory. If the server restarted, generate a new one.

**Metrics showing zeros?**
Make sure you've generated a scenario first.

**Toxic stream not showing errors?**
Check that the chaos rate slider is above 0%.

**DCL verification failing?**
Ensure all required fields match exactly (case-sensitive).

---

## Quick Reference

| What you want to do | Where to go |
|---------------------|-------------|
| Create test data for discovery | AOD > Generate Snapshot |
| Check discovery accuracy | AOD > Validation Lab |
| Stress test agents | AOA tab |
| Get query ground truth | NLQ tab |
| Test data ingestion | DCL tab |
| View this guide | Guide tab |

---

## Module Summary

| Module | Full Name | Tests |
|--------|-----------|-------|
| **AOD** | AutonomOS Discover | Data discovery accuracy |
| **AOA** | AutonomOS Agents | Agent orchestration resilience |
| **NLQ** | Natural Language Query | Query result correctness |
| **DCL** | Data Contract Library | Data ingestion and repair |

---

## Need Help?

Contact your AutonomOS platform administrator for additional support.
