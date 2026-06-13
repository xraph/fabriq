"use client";

import { motion } from "motion/react";
import { cn } from "@/lib/cn";

const ports = [
  {
    method: "f.Exec()",
    body: "The only write path. Commands in a Postgres transaction, exactly one versioned event each.",
    engine: "Transactional outbox",
    highlight: true,
  },
  {
    method: "f.Relational()",
    body: "Typed gets, filtered lists, and pagination against the source of truth.",
    engine: "Postgres · RLS",
  },
  {
    method: "f.Timeseries()",
    body: "Bulk telemetry ingest and windowed reads on hypertables.",
    engine: "TimescaleDB",
  },
  {
    method: "f.Vector()",
    body: "Similarity search over embeddings with HNSW indexes.",
    engine: "pgvector",
  },
  {
    method: "f.Graph()",
    body: "openCypher traversals with one-shot, batched hydration.",
    engine: "FalkorDB",
  },
  {
    method: "f.Search()",
    body: "Full-text multi-match over declared fields, alias-swap rebuilds.",
    engine: "Elasticsearch",
  },
  {
    method: "f.Document()",
    body: "CRDT documents that materialize into ordinary versioned entities.",
    engine: "Merge engine",
  },
  {
    method: "f.Subscribe()",
    body: "Conflated live deltas with Last-Event-ID resume over SSE.",
    engine: "Redis Streams",
  },
];

export function Ports() {
  return (
    <section className="relative border-t border-hairline py-24 md:py-32">
      <div className="mx-auto w-full max-w-6xl px-6">
        <div className="max-w-2xl">
          <span className="font-mono text-xs uppercase tracking-[0.35em] text-accent">
            Capability ports
          </span>
          <h2 className="mt-5 font-display text-4xl font-semibold leading-[1.05] tracking-tight text-ink-strong md:text-6xl">
            Every shape of data.{" "}
            <em className="not-italic text-accent">One facade.</em>
          </h2>
          <p className="mt-6 text-lg leading-relaxed font-light text-ink-muted">
            Reads go through typed ports. Writes go through Exec — and nowhere
            else. The fabric decides which engine serves which shape; your code
            imports one package.
          </p>
        </div>

        <div className="mt-16 grid grid-cols-1 gap-4 sm:grid-cols-2 lg:grid-cols-4">
          {ports.map((port, i) => (
            <motion.div
              key={port.method}
              initial={{ opacity: 0, y: 16 }}
              whileInView={{ opacity: 1, y: 0 }}
              viewport={{ once: true, margin: "-60px" }}
              transition={{ duration: 0.45, delay: i * 0.06, ease: "easeOut" }}
              className={cn(
                "group flex flex-col gap-3 rounded-xl border bg-surface-raised p-6 transition-colors duration-300",
                port.highlight
                  ? "border-accent/40 bg-accent/[0.06]"
                  : "border-hairline hover:border-accent/30",
              )}
            >
              <span
                className={cn(
                  "font-mono text-sm font-medium",
                  port.highlight
                    ? "text-accent"
                    : "text-ink group-hover:text-accent",
                )}
              >
                {port.method}
              </span>
              <p className="text-sm leading-relaxed text-ink-muted">
                {port.body}
              </p>
              <span className="mt-auto pt-3 font-mono text-[10px] uppercase tracking-[0.25em] text-ink-faint">
                {port.engine}
              </span>
            </motion.div>
          ))}
        </div>
      </div>
    </section>
  );
}
