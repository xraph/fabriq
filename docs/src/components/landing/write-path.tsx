"use client";

import { motion } from "motion/react";

const steps = [
  {
    num: "01",
    title: "f.Exec(cmd)",
    body: "A command enters the facade — validated against the registry, tenant and traceparent stamped on the envelope.",
  },
  {
    num: "02",
    title: "postgres commit",
    body: "State and exactly one versioned event commit atomically — the outbox lives in the same transaction.",
  },
  {
    num: "03",
    title: "leader relay",
    body: "A leader-elected relay wakes on LISTEN/NOTIFY and publishes the event to Redis Streams, in order.",
  },
  {
    num: "04",
    title: "woven outward",
    body: "Consumer groups project the event into graph and search, and push live deltas to subscribers — one truth, every engine.",
  },
];

export function WritePath() {
  return (
    <section className="relative border-t border-hairline py-24 md:py-32">
      <div className="mx-auto w-full max-w-6xl px-6">
        <span className="font-mono text-xs uppercase tracking-[0.35em] text-accent">
          The write path
        </span>
        <h2 className="mt-5 max-w-2xl font-display text-4xl font-semibold leading-[1.05] tracking-tight text-ink-strong md:text-6xl">
          Follow a write{" "}
          <em className="not-italic text-accent">through the loom.</em>
        </h2>

        {/* the rail — an ember pulse travels the full path */}
        <div className="relative mt-20 hidden h-px bg-hairline md:block">
          {steps.map((s, i) => (
            <span
              key={s.num}
              aria-hidden
              className="absolute top-1/2 size-1.5 -translate-y-1/2 rotate-45 bg-ink-faint"
              style={{ left: `${(i / steps.length) * 100}%` }}
            />
          ))}
          <span
            aria-hidden
            className="absolute top-1/2 size-2 -translate-y-1/2 animate-rail-pulse rounded-full bg-ember-500 shadow-[0_0_14px_4px_rgba(132,204,22,0.45)] motion-reduce:animate-none"
          />
        </div>

        <ol className="mt-6 grid gap-12 md:mt-10 md:grid-cols-4 md:gap-8">
          {steps.map((step, i) => (
            <motion.li
              key={step.num}
              initial={{ opacity: 0, y: 18 }}
              whileInView={{ opacity: 1, y: 0 }}
              viewport={{ once: true, margin: "-80px" }}
              transition={{ duration: 0.5, delay: i * 0.12, ease: "easeOut" }}
              className="border-l border-hairline pl-6 md:border-l-0 md:pl-0"
            >
              <span className="font-mono text-xs font-medium text-accent">
                {step.num}
              </span>
              <h3 className="mt-3 font-mono text-sm font-medium uppercase tracking-[0.2em] text-ink">
                {step.title}
              </h3>
              <p className="mt-3 text-sm leading-relaxed text-ink-muted">
                {step.body}
              </p>
            </motion.li>
          ))}
        </ol>
      </div>
    </section>
  );
}
