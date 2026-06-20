import {
  Arrow,
  Chip,
  Figure,
  INK_FAINT,
  Label,
  MONO,
  Node,
} from "../diagram-kit";

/** The proj:distill worker: a debounced, per-tenant single-flight run. */
export function DistillWorkerDiagram() {
  const steps: { label: string; accent?: boolean }[] = [
    { label: "1 · collect dirty L0 nodes" },
    { label: "2 · ContentHash short-circuit" },
    { label: "3 · Guard.GuardIngest", accent: true },
    { label: "4 · Summarizer.Summarize", accent: true },
    { label: "5 · Guard.GuardEmit", accent: true },
    { label: "6 · CAS write → SummaryRef" },
    { label: "7 · recompute L1 parents" },
    { label: "8 · recompute L2 root (once)" },
  ];
  const sx = 130;
  const sw = 300;
  const sh = 34;
  const top = 156;
  const pitch = 44;
  return (
    <Figure
      viewBox="0 0 560 588"
      title="The proj:distill worker"
      desc="A write event on the shared stream marks an L0 node dirty; after a debounce window a per-tenant single-flight run collects dirty L0 nodes, short-circuits unchanged ones by ContentHash, runs the ingest guard, summarizes, runs the emit guard, writes the summary to the CAS, and recomputes the L1 parents and the L2 root once."
    >
      <Node
        x={170}
        y={20}
        w={220}
        h={48}
        title="event stream"
        sub="same as proj:embed"
      />
      <Arrow x1={280} y1={68} x2={280} y2={104} />
      <text x={296} y={92} fontSize="11" fontFamily={MONO} fill={INK_FAINT}>
        debounce · 5s default
      </text>

      <rect
        x={70}
        y={110}
        width={420}
        height={418}
        rx={16}
        fill="var(--surface-sunken)"
        stroke="var(--accent)"
        strokeWidth={1.4}
      />
      <text
        x={90}
        y={136}
        fontSize="12"
        fontFamily={MONO}
        fill="var(--ink-muted)"
      >
        per-tenant single-flight run
      </text>

      {steps.map((s, i) => {
        const y = top + i * pitch;
        return (
          <g key={s.label}>
            {i > 0 ? <Arrow x1={280} y1={y - 10} x2={280} y2={y} /> : null}
            <Chip
              x={sx}
              y={y}
              w={sw}
              h={sh}
              label={s.label}
              accent={s.accent}
            />
          </g>
        );
      })}
      <Label
        x={280}
        y={552}
        text="Unchanged subtrees short-circuit — no Summarizer call."
        anchor="middle"
        muted
      />
    </Figure>
  );
}
