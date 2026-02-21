# AutonomOS (AOS) — Agent Constitution
> Version: 3.0 | Updated: February 2026 | Owner: Ilya (CEO)

---

## WHO YOU ARE TALKING TO
Ilya is the CEO and technical lead. He is NOT a developer. He reasons architecturally, not syntactically.
- **Never show raw code diffs or stack traces without a plain-English summary first**
- **Never propose environment setup steps** — everything runs in Replit (easing to Claude Code CLI)
- **Never add tech debt, workarounds, or shortcuts** — Ilya will find them and it will be unpleasant
- **Always fix root causes** — patches and band-aids are forbidden
- If a fix requires a RACI boundary decision, surface it to Ilya before touching code

---

## PLATFORM IN ONE PARAGRAPH
AutonomOS is an AI-native enterprise operating system that sits on top of existing enterprise systems (Salesforce, SAP, AWS, etc.) without replacing them. It discovers what exists (AOD), understands how to connect (AAM), maps everything to business meaning (DCL), lets humans and AI agents query it in plain English (NLQ), and governs the agents doing work (AOA). The moat is the semantic data layer — not the runtime. MEI/Convergence is a product extension of AOS applied to multi-entity M&A scenarios; ~90% shared technology.

---

## MODULE RACI — THE LAW

| Module | Owns | NEVER touches |
|--------|------|---------------|
| **AOD** | Discovery, classification (Governed/Shadow/Zombie), SOR detection, Fabric Plane hints, ConnectionCandidate generation | Pipe blueprints, data extraction, semantic mapping |
| **AAM** | Pipe blueprint creation, work order dispatch, drift detection, self-healing | Data movement, fabric plane inference (that's AOD's job), semantic mapping |
| **DCL** | Semantic catalog, ontology, schema-on-write validation, MCP exposure, entity resolution | Discovery logic, connection logic, NLQ response formatting |
| **NLQ** | Intent resolution (3-tier), persona filtering, query dispatch to DCL, output rendering | Semantic mapping, data storage, agent orchestration |
| **AOA** | Agent identity, policy enforcement, HITL workflows, budget tracking, observability | Semantic mapping, discovery, NLQ query handling |
| **Farm** | Synthetic data generation, financial models, test oracle | Production data, live connections |

**RACI VIOLATION = STOP AND FLAG.** If a fix requires Module A to implement logic that belongs to Module B, do not implement it. Surface the architectural conflict with a clear description and proposed resolution.

---

## CURRENT PRIORITIES (Feb 2026)

### P1 — DCL Semantic Catalog Expansion
Goal: Expand from ~38 metrics to 60,000+ real enterprise data elements.
- Ontology currently has 37 metrics, 29 dimensions, 13 bindings
- MCP integration in progress — DCL must expose semantic catalog via MCP tools: `concept_lookup`, `semantic_export`, `query`, `provenance`
- Current 3-tier normalization funnel must handle 60k+ elements without performance regression
- Target: any MCP-compatible agent can query DCL without custom integration

### P2 — NLQ Demo Stability & UX
Goal: NLQ must work reliably in demo mode without human hand-holding.
Known issues (fix these before anything else in NLQ):
- KPI boxes are too large — reduce size, tighten layout
- Clicking EBITDA KPI should trigger a trend/revenue chart — this is broken or unreliable
- Dashboard auto-generation from "build me a CFO dashboard" must be consistent
- Tier 1 free cache hit rate target: 60-70% — if below this, something is wrong

### P3 — DCL→NLQ E2E Throughput
Goal: Complete the real data pipeline handoff — currently clunky and manual.
- DCL and NLQ are 90% connected; the last 10% is the blocking issue
- Expanding data catalog from 38 to 60,000 elements depends on this path working cleanly
- Pushing/pulling metadata dumps manually is not acceptable — must be automated pipeline
- When this works, it's the demo-worthy moment: real data, real semantics, real query

### P4 — Consolidated Demo UX
Goal: Single AOS demo file housing all modules via iframes + AOA panel.
- Currently modules demo independently — investor/customer demo requires switching between tabs
- MEI/Convergence demo is credible once AOS demo works — don't build separately yet
- AOA governance panel must be integrated into the consolidated view

---

## WHAT "DONE" MEANS
Every completed task must satisfy ALL four:
1. **Semantics preserved** — behavior matches real-world meaning
2. **No cheating** — no silent fallbacks, no optional-everything, no demo-only paths
3. **Proof is real** — failure-before / success-after evidence
4. **Negative test included** — confirm the bad behavior can't return

---

## TECH STACK QUICK REFERENCE

| Module | Backend | Frontend | DB | Notes |
|--------|---------|----------|----|-------|
| NLQ | FastAPI/Python | React 18 | Supabase PG | Claude (Tier 3 LLM) |
| DCL | FastAPI/Python | React 18 | Supabase PG + Pinecone | Gemini 2.5 Flash + OpenAI |
| AOD | FastAPI/Python | React 18 | Supabase PG | — |
| AAM | FastAPI/Python | React 18 | SQLite | — |
| AOA | FastAPI/Python | React 18 | Supabase PG | Portkey AI gateway |
| Farm | FastAPI/Python | React 18 | Supabase PG | — |
| FinOps | Express/Node | React 18 | Neon PG + Pinecone | **Anomaly: Node backend** |

**Separate repos** per module. Port 5000 frontend / 8000 backend (Replit standard).
Secrets in Replit Secrets panel — never in .env files.

---

## FORBIDDEN PATTERNS
- Tests that pass while the real feature fails
- Silent fallbacks that hide errors
- Permissive schemas to avoid contract mismatches  
- Converting errors into empty results
- Any shortcut that works in demo but breaks in production
- Band-aids, patches, workarounds
- Adding features to Module X that belong to Module Y (RACI violation)
- FinOps pattern propagating to other modules (Node.js anomaly — don't spread it)

---

## MEI / CONVERGENCE NOTE
MEI (Multi-Entity Intelligence) = Convergence product. Lives under AOS umbrella.
- ~90% AOS technology reused
- Primary use case: M&A post-close semantic reconciliation across heterogeneous systems
- The insight: reconciling two companies' systems is architecturally identical to reconciling multiple ERP instances — which AOS already solves
- Do NOT build a separate MEI demo until the core AOS demo (P4) is working
- MEI demo will be a product extension narrative on top of AOS, not a separate stack

---

## AGENT TEAM INSTRUCTIONS
When spawning sub-agents or agent teams for AOS work:
- Each agent must declare which module it is working on at the start of every message
- Before proposing any cross-module change, agents must check the RACI table above
- Prefer Sonnet for implementation tasks; use Opus only for architecture decisions and complex debugging
- All agents report RACI violations to the lead — do not silently implement workarounds
