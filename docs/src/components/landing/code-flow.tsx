"use client";

import { motion, useInView, useReducedMotion } from "motion/react";
import { useRef } from "react";

type Token = { t: string; c?: keyof typeof tone };

const tone = {
  cm: "italic text-stone-500",
  fn: "text-ember-300",
  muted: "text-stone-500",
  str: "text-ember-200",
} as const;

// One write — the only mutation in the system.
const code: Token[][] = [
  [{ t: "// one write — exactly one versioned event", c: "cm" }],
  [
    { t: "res, err := " },
    { t: "f.Exec", c: "fn" },
    { t: "(ctx, " },
    { t: "command.", c: "muted" },
    { t: "Command{" },
  ],
  [{ t: "    Entity:  " }, { t: '"order"', c: "str" }, { t: "," }],
  [{ t: "    Op:      " }, { t: "command.", c: "muted" }, { t: "OpCreate," }],
  [
    { t: "    Payload: &Order{Ref: " },
    { t: '"ord_1042"', c: "str" },
    { t: "}," },
  ],
  [{ t: "})" }],
];

const engines = [
  { name: "Postgres", note: "source of truth" },
  { name: "Redis", note: "live deltas" },
  { name: "FalkorDB", note: "graph" },
  { name: "Elastic", note: "search" },
];

// Timeline: code lines stream first, then the fan-out chips.
const LINE_STEP = 0.16;
const LEAD = 0.25;
const FANOUT_AT = LEAD + code.length * LINE_STEP + 0.15;

export function CodeFlow() {
  const ref = useRef<HTMLDivElement>(null);
  const inView = useInView(ref, { once: true, margin: "-60px" });
  const reduce = useReducedMotion();

  // perpetual wave that sweeps each engine "in step"
  const dotAnim = reduce
    ? undefined
    : { opacity: [0.4, 1, 0.4], scale: [1, 1.3, 1] };
  const overlayAnim = reduce ? undefined : { opacity: [0, 0.9, 0] };
  const waveT = (i: number) =>
    reduce
      ? undefined
      : {
          duration: 1,
          times: [0, 0.3, 1],
          repeat: Number.POSITIVE_INFINITY,
          repeatDelay: 2,
          delay: FANOUT_AT + 0.9 + i * 0.42,
          ease: "easeInOut" as const,
        };

  return (
    <motion.div
      ref={ref}
      initial={{ opacity: 0, y: 14 }}
      animate={inView ? { opacity: 1, y: 0 } : {}}
      transition={{ duration: 0.5, ease: [0.22, 1, 0.36, 1] }}
      className="relative w-full overflow-hidden rounded-2xl border border-white/10 bg-night-950/90 text-left shadow-2xl shadow-ember-950/40 backdrop-blur-xl"
    >
      {/* woven grid + ember bloom — fabric, not a generic terminal */}
      <div
        aria-hidden
        className="pointer-events-none absolute inset-0 bg-[repeating-linear-gradient(90deg,rgba(190,242,100,0.05)_0,rgba(190,242,100,0.05)_1px,transparent_1px,transparent_22px),repeating-linear-gradient(0deg,rgba(190,242,100,0.04)_0,rgba(190,242,100,0.04)_1px,transparent_1px,transparent_22px)] [mask-image:radial-gradient(ellipse_at_top_right,#000,transparent_72%)]"
      />
      <div
        aria-hidden
        className="pointer-events-none absolute -top-12 -right-10 size-44 rounded-full bg-ember-500/20 blur-3xl"
      />

      <div className="relative">
        {/* branded header — the write path, live */}
        <div className="flex items-center gap-2.5 border-b border-white/10 px-4 py-3">
          <svg
            aria-hidden="true"
            viewBox="0 0 24 24"
            className="size-4 shrink-0"
            fill="none"
          >
            <path
              d="M9 3v18M15 3v18"
              stroke="currentColor"
              className="text-stone-600"
              strokeWidth="2"
              strokeLinecap="round"
            />
            <path
              d="M3 9h18M3 15h18"
              stroke="#84cc16"
              strokeWidth="2"
              strokeLinecap="round"
            />
          </svg>
          <span className="font-mono text-[11px] uppercase tracking-[0.22em] text-stone-400">
            write path
          </span>
          <span className="ml-auto inline-flex items-center gap-1.5 rounded-full border border-white/10 bg-white/[0.04] px-2 py-0.5">
            <span className="relative flex size-1.5">
              <span className="absolute inline-flex h-full w-full animate-ping rounded-full bg-ember-400 opacity-60 motion-reduce:animate-none" />
              <span className="relative inline-flex size-1.5 rounded-full bg-ember-400" />
            </span>
            <span className="font-mono text-[10px] tracking-[0.2em] text-stone-400">
              EXEC
            </span>
          </span>
        </div>

        {/* code — lines stream in */}
        <div className="px-5 py-5 font-mono text-[13px] leading-[1.75]">
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
              {i === code.length - 1 && (
                <span className="ml-1 inline-block h-[1.05em] w-[7px] translate-y-[2px] animate-caret bg-ember-300 align-middle" />
              )}
            </motion.div>
          ))}
        </div>

        {/* fan-out — write propagates to every engine, in step */}
        <div className="border-t border-white/10 px-5 py-4">
          <motion.div
            initial={{ opacity: 0 }}
            animate={inView ? { opacity: 1 } : {}}
            transition={{ delay: reduce ? 0 : FANOUT_AT, duration: 0.4 }}
            className="mb-3 flex items-center gap-2 font-mono text-[10px] uppercase tracking-[0.25em] text-stone-500"
          >
            <span className="text-ember-300">└─</span> fans out — in step
          </motion.div>
          <div className="grid grid-cols-2 gap-2 sm:grid-cols-4">
            {engines.map((e, i) => (
              <motion.div
                key={e.name}
                initial={{ opacity: 0, y: 6 }}
                animate={inView ? { opacity: 1, y: 0 } : {}}
                transition={{
                  delay: reduce ? 0 : FANOUT_AT + 0.2 + i * 0.1,
                  duration: 0.3,
                }}
                className="relative overflow-hidden rounded-lg border border-white/10 bg-white/[0.03] px-2.5 py-2"
              >
                <motion.span
                  aria-hidden
                  className="pointer-events-none absolute inset-0 bg-gradient-to-t from-ember-500/25 to-transparent"
                  initial={{ opacity: 0 }}
                  animate={overlayAnim}
                  transition={waveT(i)}
                />
                <div className="relative">
                  <div className="flex items-center gap-1.5">
                    <motion.span
                      aria-hidden
                      className="size-1.5 rounded-full bg-ember-400"
                      initial={{ opacity: reduce ? 1 : 0.4 }}
                      animate={dotAnim}
                      transition={waveT(i)}
                    />
                    <span className="font-mono text-[11px] font-medium text-stone-200">
                      {e.name}
                    </span>
                  </div>
                  <span className="mt-0.5 block font-mono text-[9px] uppercase tracking-[0.15em] text-stone-600">
                    {e.note}
                  </span>
                </div>
              </motion.div>
            ))}
          </div>
        </div>
      </div>
    </motion.div>
  );
}
