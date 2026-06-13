"use client";

import { motion } from "motion/react";

const stats = [
  { value: "1", label: "Write path into the stores" },
  { value: "4", label: "Engines behind one facade" },
  { value: "0", label: "Direct writes to projections" },
  { value: "100%", label: "Access tenant-scoped" },
];

export function Stats() {
  return (
    <section className="relative overflow-hidden border-t border-hairline py-24 md:py-32">
      <div
        aria-hidden
        className="pointer-events-none absolute inset-0 bg-[radial-gradient(125%_125%_at_50%_10%,transparent_55%,rgba(101,163,13,0.10)_100%)] dark:bg-[radial-gradient(125%_125%_at_50%_10%,transparent_40%,rgba(77,124,15,0.3)_100%)]"
      />
      <div className="relative mx-auto w-full max-w-6xl px-6">
        <div className="grid items-center gap-20 lg:grid-cols-2 lg:gap-24">
          <div className="space-y-10">
            <span className="inline-flex rounded-full border border-hairline bg-surface-raised px-4 py-1.5 font-mono text-xs uppercase tracking-[0.25em] text-ink-muted">
              Why a fabric
            </span>
            <h2 className="font-display text-5xl font-semibold leading-[1.02] tracking-tight text-ink-strong md:text-7xl">
              Invariants,
              <br />
              <em className="not-italic text-ink-muted">not promises.</em>
            </h2>
            <div className="grid grid-cols-2 gap-x-12 gap-y-14 pt-8">
              {stats.map((stat, i) => (
                <motion.div
                  key={stat.label}
                  initial={{ opacity: 0, x: -10 }}
                  whileInView={{ opacity: 1, x: 0 }}
                  viewport={{ once: true }}
                  transition={{
                    duration: 0.3,
                    delay: i * 0.05,
                    ease: "easeOut",
                  }}
                >
                  <div className="mb-2 text-5xl font-semibold tracking-tighter text-accent">
                    {stat.value}
                  </div>
                  <div className="font-mono text-xs uppercase tracking-[0.2em] text-ink-muted">
                    {stat.label}
                  </div>
                </motion.div>
              ))}
            </div>
          </div>

          <div className="group relative">
            <div className="pointer-events-none absolute -inset-20 bg-gradient-to-tr from-accent/15 to-transparent opacity-0 blur-3xl transition-opacity duration-1000 group-hover:opacity-100" />
            <div className="relative z-10 space-y-8">
              <p className="text-xl leading-relaxed font-medium text-pretty text-ink md:text-2xl">
                Most stacks bolt a search index and a graph onto a primary
                database and hope the glue holds. Drift creeps in quietly,
                tenancy leaks through a forgotten filter, and rebuilding a
                projection becomes a weekend with a runbook.
              </p>
              <div className="h-px w-20 bg-accent/50" />
              <p className="text-lg leading-relaxed text-pretty text-ink-muted">
                Fabriq makes the guarantees structural. Writes pass through one
                door and leave a versioned trail. Projections are derived,
                disposable, and rebuilt blue-green while reads keep flowing. And
                when an engine disagrees with the source of truth, the
                reconciler notices — and heals it through the same front door.
              </p>
            </div>
          </div>
        </div>
      </div>
    </section>
  );
}
