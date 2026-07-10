"use client";

import { motion } from "motion/react";

const surfaces = [
  "Entities & schemas",
  "Text / semantic / hybrid search",
  "Graph & spatial",
  "Files & CRDT docs",
  "Outbox & live",
  "Recall & distillation",
  "Read-only SQL",
  "Analytics — freshness & SQL",
  "Runtime plugins",
];

export function Console() {
  return (
    <section className="relative border-t border-hairline py-24 md:py-32">
      <div className="mx-auto w-full max-w-6xl px-6">
        <span className="font-mono text-xs uppercase tracking-[0.35em] text-accent">
          The console
        </span>
        <h2 className="mt-5 max-w-2xl font-display text-4xl font-semibold leading-[1.05] tracking-tight text-ink-strong md:text-6xl">
          Every subsystem,{" "}
          <em className="not-italic text-accent">one dashboard.</em>
        </h2>
        <p className="mt-6 max-w-2xl text-base leading-relaxed text-ink-muted">
          A mountable, plugin-based web console over a running fabriq — one
          interactive surface per subsystem, all tenant-scoped. It reaches an
          instance through the same adminapi any client uses: a portable{" "}
          <code className="font-mono text-ink">fabriq://</code> connection
          string, verified by opt-in API keys and a username/password login.
        </p>

        <motion.div
          initial={{ opacity: 0, y: 24 }}
          whileInView={{ opacity: 1, y: 0 }}
          viewport={{ once: true, margin: "-80px" }}
          transition={{ duration: 0.6, ease: "easeOut" }}
          className="mt-12 overflow-hidden rounded-xl border border-hairline bg-ink-strong/5 shadow-2xl ring-1 ring-hairline"
        >
          {/* eslint-disable-next-line @next/next/no-img-element */}
          <img
            src="/admin-console.png"
            alt="The fabriq admin console — overview, entity browser, search, graph, files, events, and more"
            loading="lazy"
            className="block w-full"
          />
        </motion.div>

        <ul className="mt-10 flex flex-wrap gap-2">
          {surfaces.map((s) => (
            <li
              key={s}
              className="rounded-full border border-hairline px-3 py-1 font-mono text-xs text-ink-muted"
            >
              {s}
            </li>
          ))}
        </ul>

        <div className="mt-10">
          <a
            href="/docs/admin-console"
            className="inline-flex items-center gap-2 font-mono text-sm font-medium text-accent hover:underline"
          >
            Explore the console →
          </a>
        </div>
      </div>
    </section>
  );
}
