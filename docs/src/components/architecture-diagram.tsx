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
  accent,
}: {
  x: number;
  y: number;
  w: number;
  h: number;
  label: string;
  accent?: boolean;
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
        stroke={accent ? ACCENT : LINE}
        strokeWidth={accent ? 1.4 : 1}
      />
      <text
        x={x + w / 2}
        y={y + h / 2 + 4}
        textAnchor="middle"
        fontFamily={MONO}
        fontSize="12.5"
        fill={accent ? ACCENT : INK}
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

/** The layered fabric: application + agents → core kernel → ports → adapters → engines. */
export function ArchitectureDiagram() {
  // Core kernel: kernel concerns (row 1) + capability surfaces (row 2).
  const coreRow1 = ["registry", "command", "event", "projection", "subscribe"];
  const coreRow2 = ["query", "cache", "blob", "agent"];
  const cw = 120;
  const cgap = 14;
  const cStart = 60;

  const adapters = [
    "postgres · grove",
    "redis",
    "falkordb",
    "elastic",
    "trove",
    "grove-kv",
  ];
  const aw = 130;
  const agap = 12;
  const aStart = 60;

  const engines = [
    {
      title: "Postgres",
      line2: "+ Timescale · pgvector · PostGIS",
      role: "source of truth · outbox",
      accent: true,
    },
    { title: "Redis", line2: "", role: "event fan-out · cache", accent: false },
    { title: "FalkorDB", line2: "", role: "graph projection", accent: false },
    {
      title: "Elasticsearch",
      line2: "",
      role: "search projection",
      accent: false,
    },
    {
      title: "Blob store",
      line2: "filesystem · S3 · trove",
      role: "CAS · file plane",
      accent: false,
    },
  ];
  const ew = 164;
  const egap = 14;
  const eStart = 42;

  return (
    <figure className="my-6 not-prose">
      <svg
        viewBox="0 0 960 560"
        width="100%"
        role="img"
        aria-labelledby="fabriq-arch-t fabriq-arch-d"
        style={{ maxWidth: "100%", height: "auto" }}
        fontFamily={SANS}
      >
        <title id="fabriq-arch-t">The layered fabriq architecture</title>
        <desc id="fabriq-arch-d">
          Application code and AI agents hold one facade over the
          engine-agnostic core kernel — registry, command, event, projection,
          subscribe, query, cache, blob, and agent — which reaches the
          capability ports, the adapter dialects (including trove for blob CAS
          and grove-kv for the cache), and the backing engines, with Postgres
          the source of truth.
        </desc>
        <Defs />

        {/* Application + agents */}
        <TierTag x={20} y={28} text="application" />
        <rect
          x={232}
          y={16}
          width={228}
          height={54}
          rx={12}
          fill={RAISED}
          stroke={LINE}
        />
        <text
          x={346}
          y={40}
          textAnchor="middle"
          fontSize="14"
          fontWeight={600}
          fill={INK_STRONG}
        >
          Your application
        </text>
        <text
          x={346}
          y={58}
          textAnchor="middle"
          fontSize="11.5"
          fill={INK_MUTED}
          fontFamily={MONO}
        >
          one facade · query.Fabric
        </text>
        <rect
          x={492}
          y={16}
          width={228}
          height={54}
          rx={12}
          fill={RAISED}
          stroke={ACCENT}
          strokeWidth={1.4}
        />
        <rect x={492} y={16} width={228} height={4} rx={2} fill={ACCENT} />
        <text
          x={606}
          y={40}
          textAnchor="middle"
          fontSize="14"
          fontWeight={600}
          fill={INK_STRONG}
        >
          AI agents
        </text>
        <text
          x={606}
          y={58}
          textAnchor="middle"
          fontSize="11.5"
          fill={INK_MUTED}
          fontFamily={MONO}
        >
          agent toolkit · recall · distill
        </text>
        <Arrow x1={346} y1={70} x2={346} y2={102} />
        <Arrow x1={606} y1={70} x2={606} y2={102} accent />

        {/* Core kernel */}
        <TierTag x={20} y={126} text="core" />
        <rect
          x={40}
          y={106}
          width={880}
          height={158}
          rx={16}
          fill={SUNKEN}
          stroke={ACCENT}
          strokeWidth={1.4}
        />
        <text x={60} y={130} fontSize="12" fill={INK_MUTED} fontFamily={MONO}>
          fabriq · core/ — engine- &amp; domain-agnostic kernel
        </text>
        {coreRow1.map((c, i) => (
          <Chip
            key={c}
            x={cStart + i * (cw + cgap)}
            y={142}
            w={cw}
            h={38}
            label={c}
          />
        ))}
        {coreRow2.map((c, i) => (
          <Chip
            key={c}
            x={cStart + i * (cw + cgap)}
            y={190}
            w={cw}
            h={38}
            label={c}
            accent={c === "cache" || c === "blob" || c === "agent"}
          />
        ))}
        <text x={60} y={250} fontSize="11" fill={INK_FAINT}>
          + lifecycle hooks · validate · upcasters · dynamic entities ·
          structural tenancy
        </text>
        <Arrow x1={480} y1={264} x2={480} y2={288} />

        {/* Ports */}
        <TierTag x={20} y={306} text="ports" />
        <rect
          x={40}
          y={288}
          width={880}
          height={30}
          rx={9}
          fill={RAISED}
          stroke={LINE}
          strokeDasharray="5 4"
        />
        <text
          x={480}
          y={307}
          textAnchor="middle"
          fontSize="11.5"
          fill={INK_MUTED}
        >
          capability ports — relational · graph · search · vector · spatial ·
          timeseries · document · cache · blob (one typed port each, no shared
          DSL)
        </text>
        <Arrow x1={480} y1={318} x2={480} y2={342} />

        {/* Adapters */}
        <TierTag x={20} y={360} text="adapters" />
        <rect
          x={40}
          y={342}
          width={880}
          height={58}
          rx={16}
          fill="none"
          stroke={LINE}
        />
        {adapters.map((a, i) => (
          <Chip
            key={a}
            x={aStart + i * (aw + agap)}
            y={356}
            w={aw}
            h={32}
            label={a}
          />
        ))}
        <Arrow x1={480} y1={400} x2={480} y2={426} dashed />

        {/* Engines */}
        <TierTag x={20} y={446} text="engines" />
        {engines.map((e, i) => {
          const ex = eStart + i * (ew + egap);
          return (
            <g key={e.title}>
              <rect
                x={ex}
                y={430}
                width={ew}
                height={100}
                rx={12}
                fill={RAISED}
                stroke={e.accent ? ACCENT : LINE}
                strokeWidth={e.accent ? 1.4 : 1}
              />
              {e.accent ? (
                <rect
                  x={ex}
                  y={430}
                  width={ew}
                  height={4}
                  rx={2}
                  fill={ACCENT}
                />
              ) : null}
              <text
                x={ex + 14}
                y={462}
                fontSize="13.5"
                fontWeight={600}
                fill={INK_STRONG}
              >
                {e.title}
              </text>
              {e.line2 ? (
                <text x={ex + 14} y={480} fontSize="10.5" fill={INK_MUTED}>
                  {e.line2}
                </text>
              ) : null}
              <text
                x={ex + 14}
                y={512}
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

/** The data lifecycle: a write fans out to projections, agent workers, and the delta plane. */
export function DataLifecycleDiagram() {
  const projTargets = [
    { name: "FalkorDB", role: "graph", x: 40 },
    { name: "Elasticsearch", role: "search", x: 142 },
    { name: "pgvector", role: "vector", x: 244 },
  ];
  return (
    <figure className="my-6 not-prose">
      <svg
        viewBox="0 0 960 470"
        width="100%"
        role="img"
        aria-labelledby="fabriq-flow-t fabriq-flow-d"
        style={{ maxWidth: "100%", height: "auto" }}
        fontFamily={SANS}
      >
        <title id="fabriq-flow-t">The fabriq data lifecycle</title>
        <desc id="fabriq-flow-d">
          A command writes a row and an outbox event in one transaction; the
          leader-elected relay publishes to Redis Streams, which fans out to
          independent consumers — the projection engine (graph, search, vector),
          the opt-in cross-tenant analytics sink (a deny-by-default, redacted
          read model), the agent workers (auto-embed to pgvector and auto-distill
          to the CAS digest tree), and the subscription hub (subscriptions and
          live queries over SSE). Reads bypass the stream through capability
          ports, with a read-through cache.
        </desc>
        <Defs />

        {/* Write pipeline */}
        <TierTag x={20} y={26} text="write → event" />
        <Node x={24} y={44} w={120} h={50} title="Exec" sub="command plane" />
        <Arrow x1={144} y1={69} x2={176} y2={69} accent />
        <Node
          x={176}
          y={36}
          w={190}
          h={66}
          title="Postgres"
          sub="row + outbox · one tx"
          accent
        />
        <text
          x={271}
          y={120}
          textAnchor="middle"
          fontSize="10.5"
          fill={INK_FAINT}
        >
          in-tx: lifecycle hooks · validate
        </text>
        <Arrow x1={366} y1={69} x2={398} y2={69} accent />
        <Node
          x={398}
          y={44}
          w={110}
          h={50}
          title="Relay"
          sub="leader-elected"
        />
        <Arrow x1={508} y1={69} x2={540} y2={69} accent />
        <Node
          x={540}
          y={36}
          w={170}
          h={66}
          title="Redis Streams"
          sub="event fan-out"
        />

        {/* Fan-out to the three consumer lanes */}
        <Arrow x1={580} y1={102} x2={170} y2={156} />
        <Arrow x1={620} y1={102} x2={484} y2={156} />
        <Arrow x1={660} y1={102} x2={800} y2={156} />

        {/* Lane A — projections */}
        <TierTag x={20} y={150} text="projections" />
        <Node
          x={40}
          y={158}
          w={300}
          h={42}
          title="Projection engine"
          sub="version-gated apply"
        />
        <Arrow x1={190} y1={200} x2={86} y2={230} />
        <Arrow x1={190} y1={200} x2={188} y2={230} />
        <Arrow x1={190} y1={200} x2={290} y2={230} />
        {projTargets.map((t) => (
          <g key={t.name}>
            <rect
              x={t.x}
              y={232}
              width={92}
              height={40}
              rx={9}
              fill={RAISED}
              stroke={LINE}
            />
            <text
              x={t.x + 46}
              y={250}
              textAnchor="middle"
              fontFamily={MONO}
              fontSize="10.5"
              fill={INK}
            >
              {t.name}
            </text>
            <text
              x={t.x + 46}
              y={263}
              textAnchor="middle"
              fontSize="9"
              fill={INK_FAINT}
            >
              {t.role}
            </text>
          </g>
        ))}
        <text x={40} y={294} fontSize="10" fill={INK_FAINT}>
          reconciler · blue-green rebuild keep projections converged
        </text>
        <text x={40} y={310} fontSize="10" fill={ACCENT}>
          + proj:analytics → redacted cross-tenant read-model (opt-in)
        </text>

        {/* Lane B — agent workers */}
        <TierTag x={372} y={150} text="agent workers" />
        <Node
          x={372}
          y={158}
          w={130}
          h={42}
          title="proj:embed"
          sub="embed → vector"
          accent
        />
        <Node
          x={516}
          y={158}
          w={130}
          h={42}
          title="proj:distill"
          sub="summarize → tree"
          accent
        />
        <Arrow x1={437} y1={200} x2={437} y2={230} accent />
        <Arrow x1={581} y1={200} x2={581} y2={230} accent />
        <rect
          x={372}
          y={232}
          width={130}
          height={40}
          rx={9}
          fill={RAISED}
          stroke={LINE}
        />
        <text
          x={437}
          y={250}
          textAnchor="middle"
          fontFamily={MONO}
          fontSize="10.5"
          fill={INK}
        >
          pgvector
        </text>
        <text x={437} y={263} textAnchor="middle" fontSize="9" fill={INK_FAINT}>
          embeddings
        </text>
        <rect
          x={516}
          y={232}
          width={130}
          height={40}
          rx={9}
          fill={RAISED}
          stroke={LINE}
        />
        <text
          x={581}
          y={250}
          textAnchor="middle"
          fontFamily={MONO}
          fontSize="10.5"
          fill={INK}
        >
          CAS · trove
        </text>
        <text x={581} y={263} textAnchor="middle" fontSize="9" fill={INK_FAINT}>
          summary blobs
        </text>
        <text x={372} y={294} fontSize="10" fill={INK_FAINT}>
          auto-embed + auto-distill ride the same event stream
        </text>

        {/* Lane C — delta plane */}
        <TierTag x={700} y={150} text="delta plane" />
        <Node
          x={700}
          y={158}
          w={220}
          h={42}
          title="Subscription hub"
          sub="conflate · resume"
        />
        <Arrow x1={810} y1={200} x2={810} y2={232} />
        <rect
          x={700}
          y={232}
          width={220}
          height={40}
          rx={9}
          fill={RAISED}
          stroke={LINE}
        />
        <text x={810} y={257} textAnchor="middle" fontSize="11.5" fill={INK}>
          Subscriptions · Live queries
        </text>
        <text
          x={810}
          y={294}
          textAnchor="middle"
          fontSize="10"
          fill={INK_FAINT}
        >
          SSE → clients (deltas)
        </text>

        {/* Reads */}
        <rect
          x={40}
          y={330}
          width={880}
          height={42}
          rx={12}
          fill={SUNKEN}
          stroke={LINE}
          strokeDasharray="5 4"
        />
        <text
          x={480}
          y={356}
          textAnchor="middle"
          fontSize="11.5"
          fill={INK_MUTED}
        >
          Reads bypass the stream — application → capability ports (read-through
          cache) → each engine directly
        </text>
      </svg>
    </figure>
  );
}
