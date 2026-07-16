"use client";

import { ArrowRight, Gauge, Layers, Sigma } from "lucide-react";
import { motion, useInView, useReducedMotion } from "motion/react";
import Link from "next/link";
import { useRef } from "react";
import { docsRoute } from "@/lib/shared";

type Token = { t: string; c?: keyof typeof tone };

const tone = {
  cm: "italic text-stone-500",
  fn: "text-ember-300",
  muted: "text-stone-500",
  str: "text-ember-200",
} as const;

const code: Token[][] = [
  [{ t: "// schemaless ingest — no registration, no outbox", c: "cm" }],
  [
    { t: "f.Analytics().", c: "muted" },
    { t: "Track", c: "fn" },
    { t: "(ctx, []" },
    { t: "query.", c: "muted" },
    { t: "AnalyticsEvent{" },
  ],
  [
    { t: "    {Name: " },
    { t: '"order_placed"', c: "str" },
    { t: ", At: now, Props: map[string]any{" },
  ],
  [
    { t: "        " },
    { t: '"amount"', c: "str" },
    { t: ": 42.50, " },
    { t: '"status"', c: "str" },
    { t: ': "paid"', c: "str" },
    { t: "," },
  ],
  [{ t: "    }}," }],
  [{ t: "})" }],
  [{ t: "" }],
  [{ t: "// measures × dimensions × time buckets", c: "cm" }],
  [
    { t: "f.Analytics().", c: "muted" },
    { t: "Query", c: "fn" },
    { t: "(ctx, " },
    { t: "query.", c: "muted" },
    { t: "AnalyticsQuery{" },
  ],
  [
    { t: "    Source:     " },
    { t: '"daily_revenue"', c: "str" },
    { t: ", // a materialized metric", c: "cm" },
  ],
  [{ t: "    TimeBucket: 24 * time.Hour," }],
  [
    { t: "    Filter:     " },
    { t: "query.", c: "muted" },
    { t: "Where{query." },
    { t: "Eq", c: "fn" },
    { t: "(" },
    { t: '"status"', c: "str" },
    { t: ", " },
    { t: '"paid"', c: "str" },
    { t: ")}," },
  ],
  [{ t: "}, &rows)" }],
];

const LINE_STEP = 0.07;
const LEAD = 0.2;
const STITCH_AT = LEAD + code.length * LINE_STEP + 0.15;

const facets = [
  {
    label: "Track",
    title: "Events, no schema up front",
    body: "A bulk, fire-and-forget ingest path straight into the tenant's own events table — no registration to start tracking a new event name or prop, and an optional DedupKey makes a retry idempotent.",
    icon: Layers,
  },
  {
    label: "Query",
    title: "A cube, not hand-written SQL",
    body: "Measures grouped by dimensions, optionally bucketed over time: count, sum, avg, min, max, count_distinct, percentile — with Having over output aliases. Aggregate schemaless events, an opted-in entity's projected facts, or a declared metric by name. QueryRaw stays open for the rest, read-only and RLS-scoped.",
    icon: Sigma,
  },
  {
    label: "Materialized rollups",
    title: "Pre-aggregated, still exact",
    body: "Opt a metric in with RollupSpec and a leader-elected maintainer seals completed buckets in the background. Query stitches the sealed rollup with a live tail, so results stay as current as a live query — additive measures roll up exactly; count_distinct and percentile ride HyperLogLog and t-digest sketches. Any shape the rollup can't serve falls through to the live path instead of erroring.",
    icon: Gauge,
  },
];

export function Analytics() {
  const ref = useRef<HTMLDivElement>(null);
  const inView = useInView(ref, { once: true, margin: "-60px" });
  const reduce = useReducedMotion();

  return (
    <section className="relative border-t border-hairline py-24 md:py-32">
      <div className="mx-auto w-full max-w-6xl px-6">
        <div className="max-w-2xl">
          <span className="font-mono text-xs uppercase tracking-[0.35em] text-accent">
            Tenant analytics
          </span>
          <h2 className="mt-5 font-display text-4xl font-semibold leading-[1.05] tracking-tight text-ink-strong md:text-6xl">
            Product analytics,{" "}
            <em className="not-italic text-accent">
              in the tenant&rsquo;s own database.
            </em>
          </h2>
          <p className="mt-6 text-lg leading-relaxed font-light text-ink-muted">
            <code className="font-mono text-base text-ink">f.Analytics()</code>{" "}
            is scoped to the caller&rsquo;s tenant like every other port. Track
            custom events, aggregate them — and the tenant&rsquo;s own domain
            entities — through one typed cube, and let a metric materialize
            itself in the background. There&rsquo;s no second store to run: the
            tables live inside the tenant&rsquo;s own database, schema, or
            shard, under the same row-level security as everything else.
          </p>
        </div>

        <div className="mt-16 grid grid-cols-1 items-start gap-8 lg:grid-cols-[minmax(0,1fr)_minmax(0,1.05fr)] lg:gap-12">
          <motion.div
            ref={ref}
            initial={{ opacity: 0, y: 14 }}
            animate={inView ? { opacity: 1, y: 0 } : {}}
            transition={{ duration: 0.5, ease: [0.22, 1, 0.36, 1] }}
            className="relative overflow-hidden rounded-2xl border border-white/10 bg-night-950/90 text-left shadow-2xl shadow-ember-950/40 backdrop-blur-xl lg:sticky lg:top-24"
          >
            <div
              aria-hidden
              className="pointer-events-none absolute inset-0 bg-[repeating-linear-gradient(90deg,rgba(190,242,100,0.05)_0,rgba(190,242,100,0.05)_1px,transparent_1px,transparent_22px),repeating-linear-gradient(0deg,rgba(190,242,100,0.04)_0,rgba(190,242,100,0.04)_1px,transparent_1px,transparent_22px)] [mask-image:radial-gradient(ellipse_at_top_right,#000,transparent_72%)]"
            />
            <div
              aria-hidden
              className="pointer-events-none absolute -top-12 -right-10 size-44 rounded-full bg-ember-500/20 blur-3xl"
            />

            <div className="relative">
              <div className="flex items-center gap-2.5 border-b border-white/10 px-4 py-3">
                <span className="font-mono text-[11px] uppercase tracking-[0.22em] text-stone-400">
                  analytics plane
                </span>
                <span className="ml-auto inline-flex items-center gap-1.5 rounded-full border border-white/10 bg-white/[0.04] px-2 py-0.5">
                  <span className="relative inline-flex size-1.5 rounded-full bg-ember-400" />
                  <span className="font-mono text-[10px] tracking-[0.2em] text-stone-400">
                    RLS-SCOPED
                  </span>
                </span>
              </div>

              <div className="overflow-x-auto px-5 py-5 font-mono text-[12px] leading-[1.75]">
                {code.map((line, i) => (
                  <motion.div
                    // biome-ignore lint/suspicious/noArrayIndexKey: lines are static
                    key={i}
                    initial={{ opacity: 0, x: -8 }}
                    animate={inView ? { opacity: 1, x: 0 } : {}}
                    transition={{
                      delay: reduce ? 0 : LEAD + i * LINE_STEP,
                      duration: 0.35,
                      ease: "easeOut",
                    }}
                    className="whitespace-pre"
                  >
                    {line.map((tok, j) => (
                      <span
                        // biome-ignore lint/suspicious/noArrayIndexKey: tokens are static
                        key={j}
                        className={tok.c ? tone[tok.c] : "text-stone-300"}
                      >
                        {tok.t}
                      </span>
                    ))}
                    {line.length === 1 && line[0].t === "" && <>&nbsp;</>}
                  </motion.div>
                ))}
              </div>

              <div className="border-t border-white/10 px-5 py-4">
                <motion.div
                  initial={{ opacity: 0 }}
                  animate={inView ? { opacity: 1 } : {}}
                  transition={{ delay: reduce ? 0 : STITCH_AT, duration: 0.4 }}
                  className="mb-3 flex items-center gap-2 font-mono text-[10px] uppercase tracking-[0.25em] text-stone-500"
                >
                  <span className="text-ember-300">└─</span> served by stitching
                </motion.div>
                <div className="grid grid-cols-2 gap-2">
                  {[
                    { name: "sealed rollup", note: "pre-aggregated" },
                    { name: "live tail", note: "up to now" },
                  ].map((chip, i) => (
                    <motion.div
                      key={chip.name}
                      initial={{ opacity: 0, y: 6 }}
                      animate={inView ? { opacity: 1, y: 0 } : {}}
                      transition={{
                        delay: reduce ? 0 : STITCH_AT + 0.2 + i * 0.1,
                        duration: 0.3,
                      }}
                      className="rounded-lg border border-white/10 bg-white/[0.03] px-2.5 py-2"
                    >
                      <div className="flex items-center gap-1.5">
                        <span
                          aria-hidden
                          className="size-1.5 rounded-full bg-ember-400"
                        />
                        <span className="font-mono text-[11px] font-medium text-stone-200">
                          {chip.name}
                        </span>
                      </div>
                      <span className="mt-0.5 block font-mono text-[9px] uppercase tracking-[0.15em] text-stone-600">
                        {chip.note}
                      </span>
                    </motion.div>
                  ))}
                </div>
              </div>
            </div>
          </motion.div>

          <div className="flex flex-col gap-4">
            {facets.map((facet, i) => (
              <motion.div
                key={facet.label}
                initial={{ opacity: 0, y: 16 }}
                whileInView={{ opacity: 1, y: 0 }}
                viewport={{ once: true, margin: "-60px" }}
                transition={{
                  duration: 0.45,
                  delay: i * 0.08,
                  ease: "easeOut",
                }}
                className="group flex flex-col gap-3 rounded-xl border border-hairline bg-surface-raised p-6 transition-colors duration-300 hover:border-accent/30"
              >
                <div className="flex items-center gap-3">
                  <div className="flex size-9 shrink-0 items-center justify-center rounded-lg border border-accent/25 bg-accent/10 text-accent">
                    <facet.icon className="size-4" />
                  </div>
                  <span className="font-mono text-[10px] uppercase tracking-[0.25em] text-ink-faint">
                    {facet.label}
                  </span>
                </div>
                <h3 className="text-lg font-semibold tracking-tight text-ink-strong">
                  {facet.title}
                </h3>
                <p className="text-sm leading-relaxed text-ink-muted">
                  {facet.body}
                </p>
              </motion.div>
            ))}
          </div>
        </div>

        <div className="mt-12 flex flex-col items-start gap-4 border-t border-hairline pt-10 sm:flex-row sm:items-center sm:justify-between">
          <p className="max-w-xl text-pretty leading-relaxed text-ink-muted">
            Off by default; one flag —{" "}
            <code className="font-mono text-ink">insights.enabled</code> — turns
            it on, with no separate DSN to configure. Sketch-backed rollups need
            the <code className="font-mono text-ink">timescaledb_toolkit</code>{" "}
            extension.
          </p>
          <Link
            href={`${docsRoute}/analytics`}
            className="flex shrink-0 items-center gap-2 font-mono text-sm font-medium text-accent transition hover:opacity-80"
          >
            Read the analytics doc <ArrowRight size={16} />
          </Link>
        </div>
      </div>
    </section>
  );
}
