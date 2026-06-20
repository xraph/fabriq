"use client";

import { ArrowRight, Brain, PenLine, Radio } from "lucide-react";
import { motion } from "motion/react";
import Link from "next/link";
import { docsRoute } from "@/lib/shared";

const capabilities = [
  {
    method: "tk.Recall()",
    title: "Fused recall",
    body: "One query fans out across vector, search, and graph, fuses the rankings, and returns a token-budgeted context pack — the ranked slice the model needs, never the whole corpus.",
    icon: Brain,
  },
  {
    method: "tk.Remember()",
    title: "Guarded memory",
    body: "Agents form new memory under a deny-by-default policy, scoped by tenant and vetoed by the same lifecycle hooks as every write. New rows auto-embed on commit.",
    icon: PenLine,
  },
  {
    method: "tk.Watch()",
    title: "Live awareness",
    body: "Subscribe to a scope and receive conflated deltas as the world changes — the agent stays current without polling.",
    icon: Radio,
  },
];

export function Agents() {
  return (
    <section className="relative border-t border-hairline py-24 md:py-32">
      <div className="mx-auto w-full max-w-6xl px-6">
        <div className="max-w-2xl">
          <span className="font-mono text-xs uppercase tracking-[0.35em] text-accent">
            Agent data fabric
          </span>
          <h2 className="mt-5 font-display text-4xl font-semibold leading-[1.05] tracking-tight text-ink-strong md:text-6xl">
            A brain your agents{" "}
            <em className="not-italic text-accent">think through.</em>
          </h2>
          <p className="mt-6 text-lg leading-relaxed font-light text-ink-muted">
            The same facade is an AI agent&apos;s brain. The agent toolkit
            retrieves the relevant slice of a gigabyte-scale corpus on demand,
            forms memory under policy, and stays current — riding the same
            tenancy, eventing, and projection invariants as every other write.
          </p>
        </div>

        <div className="mt-16 grid grid-cols-1 gap-6 md:grid-cols-3">
          {capabilities.map((cap, i) => (
            <motion.div
              key={cap.method}
              initial={{ opacity: 0, y: 16 }}
              whileInView={{ opacity: 1, y: 0 }}
              viewport={{ once: true, margin: "-60px" }}
              transition={{ duration: 0.45, delay: i * 0.08, ease: "easeOut" }}
              className="group flex flex-col gap-4 rounded-xl border border-hairline bg-surface-raised p-6 transition-colors duration-300 hover:border-accent/30"
            >
              <div className="flex size-11 items-center justify-center rounded-xl border border-accent/25 bg-accent/10 text-accent">
                <cap.icon className="size-5" />
              </div>
              <span className="font-mono text-sm font-medium text-ink group-hover:text-accent">
                {cap.method}
              </span>
              <h3 className="text-lg font-semibold tracking-tight text-ink-strong">
                {cap.title}
              </h3>
              <p className="text-sm leading-relaxed text-ink-muted">
                {cap.body}
              </p>
            </motion.div>
          ))}
        </div>

        <div className="mt-12 flex flex-col items-start gap-4 border-t border-hairline pt-10 sm:flex-row sm:items-center sm:justify-between">
          <p className="max-w-xl text-pretty leading-relaxed text-ink-muted">
            Exposed in-process to Go agents, and over{" "}
            <span className="text-ink">MCP</span> to any agent — the same tool
            handlers behind both.
          </p>
          <Link
            href={`${docsRoute}/agent-toolkit`}
            className="flex shrink-0 items-center gap-2 font-mono text-sm font-medium text-accent transition hover:opacity-80"
          >
            Explore the agent toolkit <ArrowRight size={16} />
          </Link>
        </div>
      </div>
    </section>
  );
}
