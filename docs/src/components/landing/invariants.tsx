"use client";

import { Fingerprint, GitCommitHorizontal, RefreshCcw } from "lucide-react";
import { motion } from "motion/react";

const cards = [
  {
    label: "Transactional outbox",
    title: "One write, one event",
    body: "Every command commits inside a Postgres transaction that appends exactly one versioned event. A leader-elected relay publishes it to Redis Streams — nothing written twice, nothing lost.",
    icon: GitCommitHorizontal,
  },
  {
    label: "Structural tenancy",
    title: "Tenant-scoped, structurally",
    body: "Tenant rides on context and is stamped into every engine — row-level security in Postgres, graph-per-tenant, index routing, key prefixes. Cross-tenant reads don’t fail politely. They can’t happen.",
    icon: Fingerprint,
  },
  {
    label: "Derived projections",
    title: "Always rebuildable",
    body: "Graph and search are projections, never written directly. Blue-green rebuilds swap in atomically, and a reconciler detects drift and heals it through the same outbox.",
    icon: RefreshCcw,
  },
];

export function Invariants() {
  return (
    <section className="relative border-t border-hairline py-24 md:py-32">
      <div
        aria-hidden
        className="absolute inset-0 bg-[repeating-linear-gradient(45deg,rgba(31,24,19,0.05)_0px,rgba(31,24,19,0.05)_1px,transparent_1px,transparent_8px)] [mask-image:radial-gradient(ellipse_80%_50%_at_50%_0%,#000_70%,transparent_110%)] dark:bg-[repeating-linear-gradient(45deg,#221c17_0px,#221c17_1px,transparent_1px,transparent_8px)]"
      />
      <div className="relative mx-auto w-full max-w-6xl px-6">
        <div className="flex flex-col gap-10 border-b border-hairline pb-12 md:flex-row md:items-end md:justify-between">
          <h2 className="font-display text-5xl font-semibold leading-[1.02] tracking-tight text-ink-strong md:text-7xl">
            Three invariants.
            <br />
            <em className="not-italic text-ink-muted">Zero drift.</em>
          </h2>
          <p className="max-w-xs font-mono text-xs uppercase leading-relaxed tracking-[0.25em] text-ink-muted">
            Not conventions. Not review checklists. Structural properties of the
            fabric.
          </p>
        </div>

        <div className="mt-16 grid grid-cols-1 gap-6 md:grid-cols-3">
          {cards.map((card, i) => (
            <motion.div
              key={card.title}
              initial={{ opacity: 0, y: 24 }}
              whileInView={{ opacity: 1, y: 0 }}
              viewport={{ once: true, margin: "-80px" }}
              transition={{ duration: 0.5, delay: i * 0.1, ease: "easeOut" }}
              className="group relative overflow-hidden rounded-2xl border border-hairline bg-surface-raised p-10 transition-colors duration-500 hover:border-accent/40"
            >
              <div className="absolute inset-0 bg-gradient-to-br from-accent/[0.10] to-transparent opacity-0 transition-opacity duration-700 group-hover:opacity-100" />
              <div className="relative z-10 flex h-full flex-col gap-12">
                <div className="flex size-12 items-center justify-center rounded-xl border border-accent/25 bg-accent/10 text-accent">
                  <card.icon className="size-5" />
                </div>
                <div className="space-y-4">
                  <span className="font-mono text-[10px] uppercase tracking-[0.3em] text-ink-muted">
                    {card.label}
                  </span>
                  <h3 className="text-2xl font-semibold tracking-tight text-ink-strong">
                    {card.title}
                  </h3>
                  <p className="text-sm leading-relaxed text-ink-muted">
                    {card.body}
                  </p>
                </div>
              </div>
            </motion.div>
          ))}
        </div>
      </div>
    </section>
  );
}
