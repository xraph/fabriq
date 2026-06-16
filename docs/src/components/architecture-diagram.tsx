// Theme-aware architecture diagrams for the docs. Pure SVG driven by the site's
// CSS variables (--surface-*, --ink-*, --accent), so light/dark flip for free —
// no theme detection, no client JS.

const SANS = "var(--font-sans)";
const MONO = "var(--font-mono)";
const INK = "var(--ink)";
const INK_STRONG = "var(--ink-strong)";
const INK_MUTED = "var(--ink-muted)";
const INK_FAINT = "var(--ink-faint)";
const RAISED = "var(--surface-raised)";
const SUNKEN = "var(--surface-sunken)";
const LINE = "var(--hairline)";
const ACCENT = "var(--accent)";

function Chip({
  x,
  y,
  w,
  h,
  label,
}: {
  x: number;
  y: number;
  w: number;
  h: number;
  label: string;
}) {
  return (
    <g>
      <rect
        x={x}
        y={y}
        width={w}
        height={h}
        rx={9}
        fill={RAISED}
        stroke={LINE}
      />
      <text
        x={x + w / 2}
        y={y + h / 2 + 4}
        textAnchor="middle"
        fontFamily={MONO}
        fontSize="12.5"
        fill={INK}
      >
        {label}
      </text>
    </g>
  );
}

function TierTag({ x, y, text }: { x: number; y: number; text: string }) {
  return (
    <text
      x={x}
      y={y}
      fontFamily={SANS}
      fontSize="10.5"
      fontWeight={600}
      letterSpacing="0.12em"
      fill={INK_FAINT}
    >
      {text.toUpperCase()}
    </text>
  );
}

function Arrow({
  x1,
  y1,
  x2,
  y2,
  accent,
  dashed,
}: {
  x1: number;
  y1: number;
  x2: number;
  y2: number;
  accent?: boolean;
  dashed?: boolean;
}) {
  return (
    <line
      x1={x1}
      y1={y1}
      x2={x2}
      y2={y2}
      stroke={accent ? ACCENT : INK_FAINT}
      strokeWidth={1.5}
      strokeDasharray={dashed ? "4 4" : undefined}
      markerEnd={accent ? "url(#fabriq-arrow-accent)" : "url(#fabriq-arrow)"}
    />
  );
}

function Defs() {
  return (
    <defs>
      <marker
        id="fabriq-arrow"
        viewBox="0 0 10 10"
        refX="8"
        refY="5"
        markerWidth="6"
        markerHeight="6"
        orient="auto-start-reverse"
      >
        <path
          d="M2 1L8 5L2 9"
          fill="none"
          stroke={INK_FAINT}
          strokeWidth={1.4}
          strokeLinecap="round"
          strokeLinejoin="round"
        />
      </marker>
      <marker
        id="fabriq-arrow-accent"
        viewBox="0 0 10 10"
        refX="8"
        refY="5"
        markerWidth="6.5"
        markerHeight="6.5"
        orient="auto-start-reverse"
      >
        <path
          d="M2 1L8 5L2 9"
          fill="none"
          stroke={ACCENT}
          strokeWidth={1.6}
          strokeLinecap="round"
          strokeLinejoin="round"
        />
      </marker>
    </defs>
  );
}

/** The layered fabric: facade → core kernel → ports → adapters → engines. */
export function ArchitectureDiagram() {
  const coreChips = [
    "registry",
    "command",
    "event",
    "projection",
    "subscribe",
    "query",
  ];
  const cw = 116;
  const cgap = 12;
  const cStart = 60;

  const adapters = ["postgres · grove", "redis", "falkordb", "elastic"];
  const aw = 176;
  const agap = 14;
  const aStart = 52;

  const engines = [
    {
      title: "Postgres",
      line2: "+ Timescale · pgvector",
      role: "source of truth · outbox",
      accent: true,
    },
    { title: "Redis Streams", line2: "", role: "event fan-out", accent: false },
    { title: "FalkorDB", line2: "", role: "graph projection", accent: false },
    {
      title: "Elasticsearch",
      line2: "",
      role: "search projection",
      accent: false,
    },
  ];
  const ew = 184;
  const egap = 12;

  return (
    <figure className="my-6 not-prose">
      <svg
        viewBox="0 0 860 540"
        width="100%"
        role="img"
        aria-labelledby="fabriq-arch-t fabriq-arch-d"
        style={{ maxWidth: "100%", height: "auto" }}
        fontFamily={SANS}
      >
        <title id="fabriq-arch-t">The layered fabriq architecture</title>
        <desc id="fabriq-arch-d">
          Application code holds one facade over the engine-agnostic core
          kernel, the capability ports, the adapter dialects, and the backing
          engines — Postgres being the source of truth.
        </desc>
        <Defs />

        {/* Application */}
        <TierTag x={20} y={26} text="application" />
        <rect
          x={300}
          y={14}
          width={260}
          height={52}
          rx={12}
          fill={RAISED}
          stroke={LINE}
        />
        <text
          x={430}
          y={36}
          textAnchor="middle"
          fontSize="14"
          fontWeight={600}
          fill={INK_STRONG}
        >
          Your application
        </text>
        <text
          x={430}
          y={53}
          textAnchor="middle"
          fontSize="11.5"
          fill={INK_MUTED}
          fontFamily={MONO}
        >
          one facade — query.Fabric
        </text>
        <Arrow x1={430} y1={66} x2={430} y2={98} />

        {/* Core kernel */}
        <TierTag x={20} y={118} text="core" />
        <rect
          x={40}
          y={102}
          width={780}
          height={118}
          rx={16}
          fill={SUNKEN}
          stroke={ACCENT}
          strokeWidth={1.4}
        />
        <text x={60} y={126} fontSize="12" fill={INK_MUTED} fontFamily={MONO}>
          fabriq · core/ — engine- &amp; domain-agnostic kernel
        </text>
        {coreChips.map((c, i) => (
          <Chip
            key={c}
            x={cStart + i * (cw + cgap)}
            y={138}
            w={cw}
            h={40}
            label={c}
          />
        ))}
        <text x={60} y={206} fontSize="11" fill={INK_FAINT}>
          + lifecycle hooks · validate · upcasters · dynamic entities ·
          structural tenancy
        </text>
        <Arrow x1={430} y1={220} x2={430} y2={246} />

        {/* Ports */}
        <TierTag x={20} y={264} text="ports" />
        <rect
          x={40}
          y={248}
          width={780}
          height={30}
          rx={9}
          fill={RAISED}
          stroke={LINE}
          strokeDasharray="5 4"
        />
        <text
          x={430}
          y={267}
          textAnchor="middle"
          fontSize="11.5"
          fill={INK_MUTED}
        >
          capability ports · one typed port per capability — no shared query DSL
        </text>
        <Arrow x1={430} y1={278} x2={430} y2={302} />

        {/* Adapters */}
        <TierTag x={20} y={322} text="adapters" />
        <rect
          x={40}
          y={306}
          width={780}
          height={58}
          rx={16}
          fill="none"
          stroke={LINE}
        />
        {adapters.map((a, i) => (
          <Chip
            key={a}
            x={aStart + i * (aw + agap)}
            y={320}
            w={aw}
            h={32}
            label={a}
          />
        ))}
        <Arrow x1={430} y1={364} x2={430} y2={390} dashed />

        {/* Engines */}
        <TierTag x={20} y={410} text="engines" />
        {engines.map((e, i) => {
          const ex = 40 + i * (ew + egap);
          return (
            <g key={e.title}>
              <rect
                x={ex}
                y={394}
                width={ew}
                height={96}
                rx={12}
                fill={RAISED}
                stroke={e.accent ? ACCENT : LINE}
                strokeWidth={e.accent ? 1.4 : 1}
              />
              {e.accent ? (
                <rect
                  x={ex}
                  y={394}
                  width={ew}
                  height={4}
                  rx={2}
                  fill={ACCENT}
                />
              ) : null}
              <text
                x={ex + 14}
                y={426}
                fontSize="13.5"
                fontWeight={600}
                fill={INK_STRONG}
              >
                {e.title}
              </text>
              {e.line2 ? (
                <text x={ex + 14} y={444} fontSize="11" fill={INK_MUTED}>
                  {e.line2}
                </text>
              ) : null}
              <text
                x={ex + 14}
                y={476}
                fontSize="11"
                fontFamily={MONO}
                fill={e.accent ? ACCENT : INK_MUTED}
              >
                {e.role}
              </text>
            </g>
          );
        })}
      </svg>
    </figure>
  );
}

function Node({
  x,
  y,
  w,
  h,
  title,
  sub,
  accent,
}: {
  x: number;
  y: number;
  w: number;
  h: number;
  title: string;
  sub?: string;
  accent?: boolean;
}) {
  return (
    <g>
      <rect
        x={x}
        y={y}
        width={w}
        height={h}
        rx={11}
        fill={RAISED}
        stroke={accent ? ACCENT : LINE}
        strokeWidth={accent ? 1.4 : 1}
      />
      {accent ? (
        <rect x={x} y={y} width={w} height={4} rx={2} fill={ACCENT} />
      ) : null}
      <text
        x={x + w / 2}
        y={sub ? y + h / 2 - 2 : y + h / 2 + 4}
        textAnchor="middle"
        fontSize="12.5"
        fontWeight={600}
        fill={INK_STRONG}
      >
        {title}
      </text>
      {sub ? (
        <text
          x={x + w / 2}
          y={y + h / 2 + 13}
          textAnchor="middle"
          fontSize="10.5"
          fontFamily={MONO}
          fill={INK_MUTED}
        >
          {sub}
        </text>
      ) : null}
    </g>
  );
}

/** The data lifecycle: every runtime use case from a write to projections and deltas. */
export function DataLifecycleDiagram() {
  const targets = [
    { label: "FalkorDB · graph", x: 64 },
    { label: "Elasticsearch · search", x: 200 },
    { label: "pgvector · vector", x: 336 },
  ];
  return (
    <figure className="my-6 not-prose">
      <svg
        viewBox="0 0 860 420"
        width="100%"
        role="img"
        aria-labelledby="fabriq-flow-t fabriq-flow-d"
        style={{ maxWidth: "100%", height: "auto" }}
        fontFamily={SANS}
      >
        <title id="fabriq-flow-t">The fabriq data lifecycle</title>
        <desc id="fabriq-flow-d">
          A command writes a row and an outbox event in one transaction; the
          leader-elected relay publishes to Redis Streams, which fans out to the
          projection engine (graph, search, vector) and the subscription hub
          (subscriptions and live queries over SSE). Reads bypass the stream
          through capability ports.
        </desc>
        <Defs />

        {/* Write pipeline */}
        <TierTag x={20} y={26} text="write → event" />
        <Node x={24} y={44} w={122} h={50} title="Exec" sub="command plane" />
        <Arrow x1={146} y1={69} x2={178} y2={69} accent />
        <Node
          x={178}
          y={36}
          w={196}
          h={66}
          title="Postgres"
          sub="row + outbox · one tx"
          accent
        />
        <text
          x={276}
          y={120}
          textAnchor="middle"
          fontSize="10.5"
          fill={INK_FAINT}
        >
          in-tx: lifecycle hooks · validate
        </text>
        <Arrow x1={374} y1={69} x2={406} y2={69} accent />
        <Node
          x={406}
          y={44}
          w={112}
          h={50}
          title="Relay"
          sub="leader-elected"
        />
        <Arrow x1={518} y1={69} x2={550} y2={69} accent />
        <Node
          x={550}
          y={36}
          w={158}
          h={66}
          title="Redis Streams"
          sub="event fan-out"
        />

        {/* Fan-out to the two consumer lanes */}
        <Arrow x1={585} y1={102} x2={216} y2={172} />
        <Arrow x1={648} y1={102} x2={662} y2={172} />

        {/* Lane A — projections */}
        <TierTag x={20} y={162} text="projections" />
        <Node
          x={120}
          y={176}
          w={192}
          h={44}
          title="Projection engine"
          sub="version-gated apply"
        />
        <Arrow x1={160} y1={220} x2={128} y2={250} />
        <Arrow x1={216} y1={220} x2={264} y2={250} />
        <Arrow x1={270} y1={220} x2={400} y2={250} />
        {targets.map((t) => (
          <Chip key={t.label} x={t.x} y={252} w={132} h={34} label={t.label} />
        ))}
        <text x={64} y={312} fontSize="10.5" fill={INK_FAINT}>
          reconciler · blue-green rebuild keep projections converged
        </text>

        {/* Lane B — delta plane */}
        <TierTag x={566} y={162} text="delta plane" />
        <Node
          x={560}
          y={176}
          w={210}
          h={44}
          title="Subscription hub"
          sub="conflate · resume"
        />
        <Arrow x1={665} y1={220} x2={665} y2={250} />
        <rect
          x={560}
          y={252}
          width={210}
          height={34}
          rx={9}
          fill={RAISED}
          stroke={LINE}
        />
        <text x={665} y={273} textAnchor="middle" fontSize="11.5" fill={INK}>
          Subscriptions · Live queries
        </text>
        <text
          x={665}
          y={312}
          textAnchor="middle"
          fontSize="10.5"
          fill={INK_FAINT}
        >
          SSE → clients (deltas)
        </text>

        {/* Reads */}
        <rect
          x={40}
          y={350}
          width={780}
          height={40}
          rx={12}
          fill={SUNKEN}
          stroke={LINE}
          strokeDasharray="5 4"
        />
        <text
          x={430}
          y={374}
          textAnchor="middle"
          fontSize="11.5"
          fill={INK_MUTED}
        >
          Reads bypass the stream — application → capability ports → each engine
          directly
        </text>
      </svg>
    </figure>
  );
}
