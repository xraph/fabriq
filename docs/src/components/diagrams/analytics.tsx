import {
  ACCENT,
  Arrow,
  Figure,
  INK_FAINT,
  Label,
  LINE,
  Node,
  SUNKEN,
  TierTag,
} from "../diagram-kit";

/**
 * The cross-tenant analytics sink: each tenant's own outbox is relayed to the
 * shared event stream, and a third projection (proj:analytics) redacts and
 * writes a denormalized, tenant-tagged read model into ONE analytics database —
 * the one place fabriq deliberately co-locates data from many tenants.
 */
export function AnalyticsSinkDiagram() {
  const tenants = [
    { name: "tenant: acme", y: 44 },
    { name: "tenant: globex", y: 132 },
  ];
  return (
    <Figure
      viewBox="0 0 980 340"
      title="The cross-tenant analytics sink"
      desc="Each tenant's dedicated database appends to its own outbox; the catalog sweeper relays every tenant's outbox to one shared Redis stream. A proj:analytics consumer applies each event through a deny-by-default, field-redacted applier and writes a denormalized read model — a version-gated latest-state fact table and an append-only event log, every row tagged with tenant_id — into one shared analytics Postgres. Graph and search consume the same stream; analytics is the third projection. The analytics store sits behind a documented trust boundary: a separate database and credential, operator-only, no RLS."
    >
      {/* Per-tenant source databases, each with its own outbox */}
      <TierTag x={30} y={30} text="per-tenant source" />
      {tenants.map((t) => (
        <Node
          key={t.name}
          x={30}
          y={t.y}
          w={168}
          h={62}
          title={t.name}
          sub="db · outbox"
        />
      ))}

      {/* Sweeper / relay drains every tenant outbox */}
      <Arrow x1={198} y1={75} x2={244} y2={104} />
      <Arrow x1={198} y1={163} x2={244} y2={124} />
      <Node
        x={244}
        y={86}
        w={128}
        h={56}
        title="Sweeper"
        sub="relay per tenant"
      />

      {/* Shared event stream */}
      <Arrow x1={372} y1={114} x2={412} y2={114} accent />
      <Node
        x={412}
        y={86}
        w={150}
        h={56}
        title="Redis stream"
        sub="shared · tenant_id"
      />
      <Label
        x={487}
        y={166}
        text="graph + search consume the same stream"
        anchor="middle"
        size={9.5}
      />

      {/* The third projection: proj:analytics applier (redaction boundary) */}
      <Arrow x1={562} y1={114} x2={602} y2={114} accent />
      <Node
        x={602}
        y={86}
        w={158}
        h={56}
        title="proj:analytics"
        sub="applier · redact"
        accent
      />
      <Label
        x={681}
        y={166}
        text="deny-by-default · Include/Hash allow-list"
        anchor="middle"
        size={9.5}
      />

      {/* Trust boundary around the co-located analytics store */}
      <rect
        x={786}
        y={64}
        width={168}
        height={210}
        rx={12}
        fill={SUNKEN}
        stroke={ACCENT}
        strokeWidth={1.2}
        strokeDasharray="5 4"
      />
      <text
        x={870}
        y={82}
        textAnchor="middle"
        fontSize="9.5"
        fill={ACCENT}
        fontWeight={600}
      >
        trust boundary
      </text>

      <Arrow x1={760} y1={114} x2={800} y2={114} accent />
      <Node
        x={800}
        y={96}
        w={140}
        h={54}
        title="Analytics"
        sub="Postgres · tenant_id"
        accent
      />
      {/* The two tables inside the boundary */}
      <rect
        x={800}
        y={166}
        width={140}
        height={38}
        rx={9}
        fill={SUNKEN}
        stroke={LINE}
      />
      <text x={870} y={189} textAnchor="middle" fontSize="10" fill={INK_FAINT}>
        facts (latest, gated)
      </text>
      <rect
        x={800}
        y={214}
        width={140}
        height={38}
        rx={9}
        fill={SUNKEN}
        stroke={LINE}
      />
      <text x={870} y={237} textAnchor="middle" fontSize="10" fill={INK_FAINT}>
        events (append log)
      </text>

      {/* Footnote */}
      <rect
        x={30}
        y={290}
        width={924}
        height={38}
        rx={11}
        fill={SUNKEN}
        stroke={LINE}
        strokeDasharray="5 4"
      />
      <text
        x={492}
        y={314}
        textAnchor="middle"
        fontSize="11"
        fill={INK_FAINT}
      >
        Operator-only, no RLS, separate database + credential — fleet-wide
        reporting without touching any per-tenant database.
      </text>
    </Figure>
  );
}
