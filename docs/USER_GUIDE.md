# AOS Farm Operator Guide

Welcome to AOS Farm, the Test Oracle for AutonomOS. This guide will help you use the platform to generate test data, run validations, and stress test your systems.

---

## Getting Started

When you open AOS Farm, you'll see a navigation bar at the top with these tabs:

- **Overview** - System status and quick actions
- **Console** - Main workspace for snapshots and reconciliations
- **Agents** - Stress testing for agent orchestration systems
- **Scenarios** - Ground truth datasets for data validation
- **Guide** - This documentation

---

## Console Tab: Snapshots & Validation

The Console is your main workspace for managing test data.

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

### Viewing Snapshot Details

Click any snapshot in the list to see:
- Entity counts (apps, integrations, users, assets)
- Data quality metrics
- Generated timestamp

### Deleting Snapshots

To remove old snapshots:
1. Click the red **Erase All** button (removes all snapshots)
2. Confirm when prompted

---

## Console Tab: Validation Lab

The Validation Lab compares actual system results against expected outcomes.

### Running a Reconciliation

A reconciliation checks if your discovery system (AOD) found what it should have found.

1. Select a snapshot from the dropdown
2. Paste your system's results (or use the test input)
3. Click **Run Reconciliation**
4. Review the results:
   - **Green**: Matches expected
   - **Yellow**: Partial match
   - **Red**: Missing or incorrect

### Understanding Results

Each reconciliation shows:
- **Accuracy**: Overall correctness percentage
- **Precision**: How many found items were correct
- **Recall**: How many expected items were found
- **Mismatches**: Specific items that didn't match

---

## Agents Tab: Stress Testing

Use this tab to test how your agent orchestration system handles load and chaos.

### Generating an Agent Fleet

1. Go to the **Agents** tab
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

## Scenarios Tab: Data Validation

Scenarios provide ground truth datasets for testing data systems.

### Generating a Scenario

1. Go to the **Scenarios** tab
2. Choose a scale (Small, Medium, or Large)
3. Optionally enter a seed number for reproducibility
4. Click **Generate Scenario**

The scenario includes customers, invoices, vendors, and assets spanning multiple months.

### Viewing Metrics

After generating a scenario, you'll see five metric cards:

1. **Total Revenue**: Combined revenue for the period
2. **Month-over-Month**: Revenue trend between months
3. **Top Customers**: Highest revenue customers
4. **Vendor Spend**: Spending by vendor
5. **Resource Health**: Active vs. zombie vs. orphan assets

These metrics are the "correct answers" your data systems should produce.

### Testing with Toxic Data

To test how your system handles bad data:

1. Adjust the **Chaos Rate** slider (0-100%)
2. Click **Start Stream**
3. A new tab opens with streaming data containing errors:
   - Missing required fields
   - Duplicate records
   - Invalid currency codes
   - Outdated timestamps
   - References to non-existent records

### Verifying Repairs

If your system repairs bad data:

1. Enter the invoice ID in the **Verify Repair** section
2. Paste your repaired record
3. Click **Verify**
4. See if your repair matches the expected values

---

## Tips for Success

### Reproducibility
- Use the same **seed** number to generate identical datasets
- This is useful for comparing results across test runs

### Interpreting Results
- **100% accuracy** is the goal but not always realistic
- Focus on improving the metrics that matter most to your use case
- Compare results over time to track improvements

### Troubleshooting

**Scenario not loading?**
Scenarios are stored in memory. If the server restarted, generate a new one.

**Metrics showing zeros?**
Make sure you've generated a scenario first.

**Stream not showing errors?**
Check that the chaos rate slider is above 0%.

---

## Quick Reference

| What you want to do | Where to go |
|---------------------|-------------|
| Create test data | Console > Generate Snapshot |
| Check system accuracy | Console > Validation Lab |
| Stress test agents | Agents tab |
| Validate data systems | Scenarios tab |
| View this guide | Guide tab |

---

## Need Help?

Contact your AutonomOS platform administrator for additional support.
