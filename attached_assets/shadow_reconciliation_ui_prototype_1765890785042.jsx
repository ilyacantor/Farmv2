import React, { useMemo, useState } from "react";

// Single-file UI prototype (React + Tailwind)
// This version consumes your backend reconciliation JSON shape.
// Drop this into any React/Tailwind app as a page/component.

type ReasonCode = string;
type RCAHint = string;

type AnalysisItemBase = {
  asset_key: string;
  headline?: string;
  farm_detail?: string;
  aod_detail?: string;
  farm_reason_codes?: ReasonCode[];
  aod_reason_codes?: ReasonCode[];
  rca_hint?: RCAHint;
};

type AodExplain = {
  present_in_aod: boolean;
  decision: "UNKNOWN_KEY" | "NOT_ADMITTED" | "ADMITTED_NOT_SHADOW" | "ADMITTED_NOT_ZOMBIE";
  reason_codes: ReasonCode[];
};

type MissedItem = AnalysisItemBase & {
  aod_explain?: AodExplain;
};

type FarmInvestigation = {
  conclusion?: string;
  findings?: string[];
};

type FalsePositiveItem = AnalysisItemBase & {
  farm_investigation?: FarmInvestigation;
};

type ReconciliationPayload = {
  reconciliation_id: string;
  snapshot_id: string;
  tenant_id: string;
  aod_run_id: string;
  status: "PASS" | "WARN" | "FAIL";
  analysis: {
    summary: {
      farm_shadows: number;
      farm_zombies: number;
      aod_shadows: number;
      aod_zombies: number;
    };
    matched_shadows: AnalysisItemBase[];
    matched_zombies: AnalysisItemBase[];
    missed_shadows: MissedItem[];
    missed_zombies: MissedItem[];
    false_positive_shadows: FalsePositiveItem[];
    false_positive_zombies: FalsePositiveItem[];
    payload_health?: {
      has_issues: boolean;
      shadow_count_mismatch: boolean;
      zombie_count_mismatch: boolean;
    };
    verdict?: string;
    accuracy?: number;
  };
};

type Row = {
  key: string; // canonical key
  bucket: "confirmed" | "aod_only" | "farm_only";
  headline?: string;
  farmDetail?: string;
  aodDetail?: string;
  farmCodes?: ReasonCode[];
  aodCodes?: ReasonCode[];
  rca?: RCAHint;
  // extras
  aodExplain?: AodExplain;
  farmInvestigation?: FarmInvestigation;
};

const Pill: React.FC<{ children: React.ReactNode; tone?: "neutral" | "good" | "warn" | "bad" }> = ({
  children,
  tone = "neutral",
}) => {
  const cls =
    tone === "good"
      ? "bg-emerald-500/10 text-emerald-300 ring-1 ring-emerald-500/20"
      : tone === "warn"
        ? "bg-amber-500/10 text-amber-300 ring-1 ring-amber-500/20"
        : tone === "bad"
          ? "bg-rose-500/10 text-rose-300 ring-1 ring-rose-500/20"
          : "bg-white/5 text-slate-200 ring-1 ring-white/10";
  return <span className={`inline-flex items-center rounded-full px-2 py-0.5 text-xs ${cls}`}>{children}</span>;
};

const Card: React.FC<{
  title: string;
  subtitle?: string;
  children: React.ReactNode;
  tone?: "neutral" | "good" | "warn" | "bad";
}> = ({ title, subtitle, children, tone = "neutral" }) => {
  const ring =
    tone === "good"
      ? "ring-emerald-500/20"
      : tone === "warn"
        ? "ring-amber-500/20"
        : tone === "bad"
          ? "ring-rose-500/20"
          : "ring-white/10";

  return (
    <div className={`rounded-2xl bg-white/5 p-4 shadow-sm ring-1 ${ring}`}>
      <div>
        <div className="text-sm font-semibold text-slate-100">{title}</div>
        {subtitle ? <div className="mt-0.5 text-xs text-slate-400">{subtitle}</div> : null}
      </div>
      <div className="mt-3">{children}</div>
    </div>
  );
};

const Metric: React.FC<{ label: string; value: number | string; hint?: string; tone?: "neutral" | "good" | "warn" | "bad" }> = ({
  label,
  value,
  hint,
  tone = "neutral",
}) => {
  const toneCls =
    tone === "good"
      ? "text-emerald-200"
      : tone === "warn"
        ? "text-amber-200"
        : tone === "bad"
          ? "text-rose-200"
          : "text-slate-100";
  return (
    <div className="rounded-xl bg-black/20 p-3 ring-1 ring-white/10">
      <div className="text-xs text-slate-400">{label}</div>
      <div className={`mt-1 text-2xl font-semibold ${toneCls}`}>{value}</div>
      {hint ? <div className="mt-1 text-xs text-slate-500">{hint}</div> : null}
    </div>
  );
};

const SectionHeader: React.FC<{ title: string; count: number; tone?: "neutral" | "good" | "warn" | "bad" }> = ({
  title,
  count,
  tone = "neutral",
}) => (
  <div className="flex items-center justify-between">
    <div className="text-sm font-semibold text-slate-100">{title}</div>
    <Pill tone={tone}>{count}</Pill>
  </div>
);

function bucketTone(bucket: Row["bucket"]): "neutral" | "good" | "warn" | "bad" {
  if (bucket === "confirmed") return "good";
  if (bucket === "aod_only") return "bad";
  return "warn";
}

function statusTone(status: ReconciliationPayload["status"]): "neutral" | "good" | "warn" | "bad" {
  if (status === "PASS") return "good";
  if (status === "WARN") return "warn";
  return "bad";
}

function groupByRca(rows: Row[]) {
  const map = new Map<string, Row[]>();
  for (const r of rows) {
    const key = r.rca ?? "(no rca)";
    map.set(key, [...(map.get(key) ?? []), r]);
  }
  // put bigger groups first
  return [...map.entries()]
    .map(([rca, items]) => ({ rca, items }))
    .sort((a, b) => b.items.length - a.items.length);
}

function Codes({ codes }: { codes?: string[] }) {
  if (!codes || codes.length === 0) return <span className="text-slate-500">—</span>;
  return (
    <div className="flex flex-wrap gap-2">
      {codes.map((c) => (
        <Pill key={c}>{c}</Pill>
      ))}
    </div>
  );
}

// --- SAMPLE DATA ---
// Replace this with real data fetched from your backend.
const sample: ReconciliationPayload = {
  reconciliation_id: "uuid",
  snapshot_id: "uuid",
  tenant_id: "BlueDynamics-ZHGT",
  aod_run_id: "run_297e61a26683",
  status: "WARN",
  analysis: {
    summary: { farm_shadows: 3, farm_zombies: 2, aod_shadows: 4, aod_zombies: 1 },
    matched_shadows: [
      {
        asset_key: "calendly.com",
        headline: "Both agree: shadow",
        farm_detail: "Farm expected shadow",
        aod_detail: "AOD found shadow",
        farm_reason_codes: ["HAS_DISCOVERY", "NO_IDP", "HAS_ONGOING_FINANCE"],
        aod_reason_codes: ["HAS_DISCOVERY", "NO_IDP"],
        rca_hint: "UNGOVERNED_WITH_SPEND",
      },
    ],
    matched_zombies: [],
    missed_shadows: [
      {
        asset_key: "trello.com",
        headline: "Farm expected shadow, AOD missed",
        farm_detail: "Farm expected shadow",
        aod_detail: "AOD decision: UNKNOWN_KEY",
        farm_reason_codes: ["HAS_DISCOVERY", "NO_IDP", "RECENT_ACTIVITY"],
        aod_reason_codes: [],
        rca_hint: "NOT_INGESTED",
        aod_explain: { present_in_aod: false, decision: "UNKNOWN_KEY", reason_codes: ["NO_CANDIDATE"] },
      },
    ],
    missed_zombies: [],
    false_positive_shadows: [
      {
        asset_key: "slack.com",
        headline: "AOD flagged shadow, Farm disagrees",
        aod_reason_codes: ["NO_IDP", "HAS_FINANCE"],
        rca_hint: "SOR_MISMATCH",
        farm_investigation: {
          conclusion: "Asset is governed - not shadow IT",
          findings: ["Found in IdP: Slack", "AOD claims NO_IDP but Farm found IdP record"],
        },
      },
    ],
    false_positive_zombies: [],
    payload_health: { has_issues: false, shadow_count_mismatch: false, zombie_count_mismatch: false },
    verdict: "NEEDS WORK - AOD missed 2 expected anomalies",
    accuracy: 75.0,
  },
};

export default function ShadowReconciliationPrototype() {
  const [view, setView] = useState<"asset" | "rca">("asset");

  // Replace with real backend data
  const reconciliation = sample;
  const analysis = reconciliation.analysis;

  const rows = useMemo<Row[]>(() => {
    const out: Row[] = [];

    for (const it of analysis.matched_shadows ?? []) {
      out.push({
        key: it.asset_key,
        bucket: "confirmed",
        headline: it.headline,
        farmDetail: it.farm_detail,
        aodDetail: it.aod_detail,
        farmCodes: it.farm_reason_codes,
        aodCodes: it.aod_reason_codes,
        rca: it.rca_hint,
      });
    }

    for (const it of analysis.false_positive_shadows ?? []) {
      out.push({
        key: it.asset_key,
        bucket: "aod_only",
        headline: it.headline,
        farmDetail: it.farm_detail,
        aodDetail: it.aod_detail,
        farmCodes: it.farm_reason_codes,
        aodCodes: it.aod_reason_codes,
        rca: it.rca_hint,
        farmInvestigation: it.farm_investigation,
      });
    }

    for (const it of analysis.missed_shadows ?? []) {
      out.push({
        key: it.asset_key,
        bucket: "farm_only",
        headline: it.headline,
        farmDetail: it.farm_detail,
        aodDetail: it.aod_detail,
        farmCodes: it.farm_reason_codes,
        aodCodes: it.aod_reason_codes,
        rca: it.rca_hint,
        aodExplain: it.aod_explain,
      });
    }

    return out;
  }, [analysis]);

  const confirmed = rows.filter((r) => r.bucket === "confirmed");
  const aodOnly = rows.filter((r) => r.bucket === "aod_only");
  const farmOnly = rows.filter((r) => r.bucket === "farm_only");

  const expected = analysis.summary.farm_shadows;
  const found = analysis.summary.aod_shadows;
  const correct = confirmed.length;
  const falsePos = aodOnly.length;
  const missed = farmOnly.length;
  const misaligned = falsePos + missed;

  const misalignedRows = rows.filter((r) => r.bucket !== "confirmed");
  const byRca = useMemo(() => groupByRca(misalignedRows), [misalignedRows]);

  return (
    <div className="min-h-screen bg-slate-950 text-slate-200">
      <div className="mx-auto max-w-7xl px-4 py-10">
        <div className="flex flex-col gap-2 md:flex-row md:items-end md:justify-between">
          <div>
            <div className="flex items-center gap-3">
              <div className="text-2xl font-semibold text-slate-50">Shadow Reconciliation</div>
              <Pill tone={statusTone(reconciliation.status)}>{reconciliation.status}</Pill>
              {analysis.payload_health?.has_issues ? <Pill tone="warn">Payload health issues</Pill> : null}
            </div>
            <div className="mt-1 text-sm text-slate-400">
              Farm = expected. AOD = actual. Single-source analysis drives both summary + detail.
            </div>
            {analysis.verdict ? <div className="mt-2 text-sm text-slate-300">{analysis.verdict}</div> : null}
          </div>

          <div className="mt-3 flex items-center gap-2 rounded-2xl bg-white/5 p-1 ring-1 ring-white/10 md:mt-0">
            <button
              onClick={() => setView("asset")}
              className={`rounded-xl px-3 py-1.5 text-sm ${view === "asset" ? "bg-white/10 text-slate-50" : "text-slate-300 hover:bg-white/5"}`}
            >
              View by Asset
            </button>
            <button
              onClick={() => setView("rca")}
              className={`rounded-xl px-3 py-1.5 text-sm ${view === "rca" ? "bg-white/10 text-slate-50" : "text-slate-300 hover:bg-white/5"}`}
            >
              View by Root Cause
            </button>
          </div>
        </div>

        {/* Scorecard */}
        <div className="mt-6 grid grid-cols-2 gap-3 md:grid-cols-7">
          <Metric label="Expected (Farm)" value={expected} hint="Expected shadows" />
          <Metric label="Found (AOD)" value={found} hint="AOD shadows" />
          <Metric label="Correct" value={correct} hint="Matched" tone="good" />
          <Metric label="Misaligned" value={misaligned} hint="FP + missed" tone="warn" />
          <Metric label="False Positives" value={falsePos} hint="AOD-only" tone="bad" />
          <Metric label="Missed" value={missed} hint="Farm-only" tone="warn" />
          <Metric
            label="Accuracy"
            value={typeof analysis.accuracy === "number" ? `${analysis.accuracy.toFixed(1)}%` : "—"}
            hint="From analysis"
          />
        </div>

        {/* Payload health callout */}
        {analysis.payload_health ? (
          <div className="mt-4 rounded-2xl bg-black/20 p-4 text-sm ring-1 ring-white/10">
            <div className="flex flex-wrap items-center gap-2">
              <Pill tone={analysis.payload_health.has_issues ? "warn" : "good"}>
                Payload Health: {analysis.payload_health.has_issues ? "Issues" : "OK"}
              </Pill>
              {analysis.payload_health.shadow_count_mismatch ? <Pill tone="warn">Shadow count mismatch</Pill> : null}
              {analysis.payload_health.zombie_count_mismatch ? <Pill tone="warn">Zombie count mismatch</Pill> : null}
              <Pill>tenant: {reconciliation.tenant_id}</Pill>
              <Pill>run: {reconciliation.aod_run_id}</Pill>
            </div>
          </div>
        ) : null}

        {view === "asset" ? (
          <div className="mt-8 grid grid-cols-1 gap-4 md:grid-cols-3">
            {/* AOD-only */}
            <div className="rounded-3xl bg-white/3 p-4 ring-1 ring-white/10">
              <SectionHeader title="AOD-Only (False Positives)" count={aodOnly.length} tone="bad" />
              <div className="mt-3 space-y-3">
                {aodOnly.map((r) => (
                  <Card key={r.key} title={r.key} subtitle={r.headline} tone={bucketTone(r.bucket)}>
                    <div className="flex flex-wrap gap-2">
                      <Pill tone="bad">AOD-only</Pill>
                      {r.rca ? <Pill tone="warn">RCA: {r.rca}</Pill> : null}
                    </div>
                    <div className="mt-3 space-y-3 text-sm">
                      <div>
                        <div className="text-xs text-slate-400">AOD detail</div>
                        <div className="text-slate-200">{r.aodDetail ?? "—"}</div>
                        <div className="mt-2 text-xs text-slate-400">AOD reason codes</div>
                        <Codes codes={r.aodCodes} />
                      </div>
                      <div>
                        <div className="text-xs text-slate-400">Farm detail</div>
                        <div className="text-slate-200">{r.farmDetail ?? "—"}</div>
                        <div className="mt-2 text-xs text-slate-400">Farm reason codes</div>
                        <Codes codes={r.farmCodes} />
                      </div>
                      {r.farmInvestigation ? (
                        <div className="rounded-xl bg-black/20 p-3 ring-1 ring-white/10">
                          <div className="text-xs font-semibold text-slate-200">Farm investigation</div>
                          {r.farmInvestigation.conclusion ? (
                            <div className="mt-1 text-sm text-slate-200">{r.farmInvestigation.conclusion}</div>
                          ) : null}
                          {r.farmInvestigation.findings && r.farmInvestigation.findings.length > 0 ? (
                            <ul className="mt-2 list-disc space-y-1 pl-5 text-xs text-slate-400">
                              {r.farmInvestigation.findings.map((f, i) => (
                                <li key={i}>{f}</li>
                              ))}
                            </ul>
                          ) : null}
                        </div>
                      ) : null}
                    </div>
                  </Card>
                ))}
                {aodOnly.length === 0 ? (
                  <div className="rounded-2xl bg-black/20 p-4 text-sm text-slate-400 ring-1 ring-white/10">No AOD-only items.</div>
                ) : null}
              </div>
            </div>

            {/* Confirmed */}
            <div className="rounded-3xl bg-white/3 p-4 ring-1 ring-white/10">
              <SectionHeader title="Confirmed Shadows" count={confirmed.length} tone="good" />
              <div className="mt-3 space-y-3">
                {confirmed.map((r) => (
                  <Card key={r.key} title={r.key} subtitle={r.headline} tone="good">
                    <div className="flex flex-wrap gap-2">
                      <Pill tone="good">Matched</Pill>
                      {r.rca ? <Pill>Hint: {r.rca}</Pill> : null}
                    </div>
                    <div className="mt-3 space-y-3 text-sm">
                      <div>
                        <div className="text-xs text-slate-400">AOD detail</div>
                        <div className="text-slate-200">{r.aodDetail ?? "—"}</div>
                        <div className="mt-2 text-xs text-slate-400">AOD reason codes</div>
                        <Codes codes={r.aodCodes} />
                      </div>
                      <div>
                        <div className="text-xs text-slate-400">Farm detail</div>
                        <div className="text-slate-200">{r.farmDetail ?? "—"}</div>
                        <div className="mt-2 text-xs text-slate-400">Farm reason codes</div>
                        <Codes codes={r.farmCodes} />
                      </div>
                    </div>
                  </Card>
                ))}
                {confirmed.length === 0 ? (
                  <div className="rounded-2xl bg-black/20 p-4 text-sm text-slate-400 ring-1 ring-white/10">No confirmed items.</div>
                ) : null}
              </div>
            </div>

            {/* Missed */}
            <div className="rounded-3xl bg-white/3 p-4 ring-1 ring-white/10">
              <SectionHeader title="Missed by AOD" count={farmOnly.length} tone="warn" />
              <div className="mt-3 space-y-3">
                {farmOnly.map((r) => (
                  <Card key={r.key} title={r.key} subtitle={r.headline} tone={bucketTone(r.bucket)}>
                    <div className="flex flex-wrap gap-2">
                      <Pill tone="warn">Missed</Pill>
                      {r.rca ? <Pill tone="warn">RCA: {r.rca}</Pill> : null}
                    </div>
                    <div className="mt-3 space-y-3 text-sm">
                      <div>
                        <div className="text-xs text-slate-400">Farm detail</div>
                        <div className="text-slate-200">{r.farmDetail ?? "—"}</div>
                        <div className="mt-2 text-xs text-slate-400">Farm reason codes</div>
                        <Codes codes={r.farmCodes} />
                      </div>
                      <div>
                        <div className="text-xs text-slate-400">AOD detail</div>
                        <div className="text-slate-200">{r.aodDetail ?? "—"}</div>
                        <div className="mt-2 text-xs text-slate-400">AOD reason codes</div>
                        <Codes codes={r.aodCodes} />
                      </div>

                      {r.aodExplain ? (
                        <div className="rounded-xl bg-black/20 p-3 ring-1 ring-white/10">
                          <div className="text-xs font-semibold text-slate-200">AOD explain-nonflag</div>
                          <div className="mt-1 text-xs text-slate-400">
                            present_in_aod: <span className="text-slate-200">{String(r.aodExplain.present_in_aod)}</span>
                          </div>
                          <div className="mt-1 text-xs text-slate-400">
                            decision: <span className="text-slate-200">{r.aodExplain.decision}</span>
                          </div>
                          <div className="mt-2 text-xs text-slate-400">reason_codes</div>
                          <Codes codes={r.aodExplain.reason_codes} />
                        </div>
                      ) : null}
                    </div>
                  </Card>
                ))}
                {farmOnly.length === 0 ? (
                  <div className="rounded-2xl bg-black/20 p-4 text-sm text-slate-400 ring-1 ring-white/10">No missed items.</div>
                ) : null}
              </div>
            </div>
          </div>
        ) : (
          <div className="mt-8 rounded-3xl bg-white/3 p-4 ring-1 ring-white/10">
            <div className="flex items-center justify-between">
              <div>
                <div className="text-sm font-semibold text-slate-100">Misalignment grouped by Root Cause</div>
                <div className="mt-1 text-sm text-slate-400">This is what you fix first.</div>
              </div>
              <Pill tone="warn">{misalignedRows.length} total</Pill>
            </div>

            <div className="mt-4 grid grid-cols-1 gap-3 md:grid-cols-2">
              {byRca.map((g) => (
                <div key={g.rca} className="rounded-2xl bg-black/20 p-4 ring-1 ring-white/10">
                  <div className="flex items-center justify-between">
                    <div className="text-sm font-semibold text-slate-100">{g.rca}</div>
                    <Pill tone="warn">{g.items.length}</Pill>
                  </div>
                  <div className="mt-3 space-y-2">
                    {g.items.map((r) => (
                      <div key={r.key} className="rounded-xl bg-white/5 p-3 ring-1 ring-white/10">
                        <div className="flex items-center justify-between gap-3">
                          <div className="text-sm font-medium text-slate-100">{r.key}</div>
                          <Pill tone={bucketTone(r.bucket)}>{r.bucket === "aod_only" ? "AOD-only" : "Missed"}</Pill>
                        </div>
                        <div className="mt-1 text-xs text-slate-400">{r.headline ?? "—"}</div>
                      </div>
                    ))}
                  </div>
                </div>
              ))}

              {byRca.length === 0 ? (
                <div className="rounded-2xl bg-black/20 p-4 text-sm text-slate-400 ring-1 ring-white/10">No misalignments.</div>
              ) : null}
            </div>
          </div>
        )}

        <div className="mt-10 rounded-3xl bg-white/3 p-5 text-sm text-slate-300 ring-1 ring-white/10">
          <div className="font-semibold text-slate-100">UI invariants</div>
          <ul className="mt-2 list-disc space-y-1 pl-5 text-slate-400">
            <li>Counts and lists come from the same backend analysis source (no frontend re-matching).</li>
            <li>Reconciliation joins only on canonical keys (asset_key). Names are presentation.</li>
            <li>Reason codes explain what; RCA hint explains where. No prose required.</li>
          </ul>
        </div>
      </div>
    </div>
  );
}
