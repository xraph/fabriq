"use client";

import { ArrowRight, Layers3, ShieldCheck, LineChart } from "lucide-react";
import { motion } from "motion/react";
import Link from "next/link";
import { docsRoute } from "@/lib/shared";

const facets = [
  {
    label: "Redacted read model",
    title: "Projected, never copied",
    body: "A consumer projects each committed event into a columnar store through the entity's allow-list. PII exposure is bounded by field allow-listing per entity — a redacted subset, not a full-fidelity mirror of tenant data.",
    icon: Layers3,
  },
  {
    label: "Operator-only trust boundary",
    title: "One deliberate exception",
    body: "The single place fabriq co-locates many tenants in one store. Deliberately no RLS — every table carries tenant_id and every query filters or groups by it. Reproject to re-apply a tightened allow-list; purge to erase a tenant on offboarding.",
    icon: ShieldCheck,
  },
  {
    label: "Freshness, ops & SQL",
    title: "Steerable from the console",
    body: "The admin console tracks per-tenant lag, runs backfill / reconcile / reproject as async jobs, and opens a read-only SQL surface over facts and events — with example queries — so operators explore the fleet without touching a per-tenant database.",
    icon: LineChart,
  },
];

export function Analytics() {
  return (
    <section className="relative border-t border-hairline py-24 md:py-32">
      <div className="mx-auto w-full max-w-6xl px-6">
        <div className="max-w-2xl">
          <span className="font-mono text-xs uppercase tracking-[0.35em] text-accent">
            Cross-tenant analytics
          </span>
          <h2 className="mt-5 font-display text-4xl font-semibold leading-[1.05] tracking-tight text-ink-strong md:text-6xl">
            Fleet-wide reporting,{" "}
            <em className="not-italic text-accent">isolation intact.</em>
          </h2>
          <p className="mt-6 text-lg leading-relaxed font-light text-ink-muted">
            No fabriq tenancy mode permits cross-tenant queries — so
            &ldquo;orders fleet-wide, by status&rdquo; has nowhere to run. The
            analytics sink is the one opt-in exception: a redacted, allow-listed
            projection of the event stream, co-located for operator-only
            reporting and reproducible from source at any time.
          </p>
        </div>

        <div className="mt-16 grid grid-cols-1 gap-6 md:grid-cols-3">
          {facets.map((facet, i) => (
            <motion.div
              key={facet.label}
              initial={{ opacity: 0, y: 16 }}
              whileInView={{ opacity: 1, y: 0 }}
              viewport={{ once: true, margin: "-60px" }}
              transition={{ duration: 0.45, delay: i * 0.08, ease: "easeOut" }}
              className="group flex flex-col gap-4 rounded-xl border border-hairline bg-surface-raised p-6 transition-colors duration-300 hover:border-accent/30"
            >
              <div className="flex size-11 items-center justify-center rounded-xl border border-accent/25 bg-accent/10 text-accent">
                <facet.icon className="size-5" />
              </div>
              <span className="font-mono text-[10px] uppercase tracking-[0.25em] text-ink-faint">
                {facet.label}
              </span>
              <h3 className="text-lg font-semibold tracking-tight text-ink-strong">
                {facet.title}
              </h3>
              <p className="text-sm leading-relaxed text-ink-muted">
                {facet.body}
              </p>
            </motion.div>
          ))}
        </div>

        <div className="mt-12 flex flex-col items-start gap-4 border-t border-hairline pt-10 sm:flex-row sm:items-center sm:justify-between">
          <p className="max-w-xl text-pretty leading-relaxed text-ink-muted">
            Backed by DuckDB, Postgres, or ClickHouse — selected by DSN. Off by
            default; gated behind the{" "}
            <code className="font-mono text-ink">analytics.read</code> /{" "}
            <code className="font-mono text-ink">analytics.admin</code>{" "}
            capabilities.
          </p>
          <Link
            href={`${docsRoute}/analytics-sink`}
            className="flex shrink-0 items-center gap-2 font-mono text-sm font-medium text-accent transition hover:opacity-80"
          >
            Read the analytics sink doc <ArrowRight size={16} />
          </Link>
        </div>
      </div>
    </section>
  );
}
