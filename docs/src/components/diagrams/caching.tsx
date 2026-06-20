import { Arrow, Figure, Label, Node, TierTag } from "../diagram-kit";

/** Two read layers: a result-set (id-list) cache in front of a per-row cache. */
export function CacheLayersDiagram() {
  return (
    <Figure
      viewBox="0 0 720 190"
      title="Two-level cache"
      desc="A List call resolves a cached id-list keyed by a query fingerprint (the result-set layer), then hydrates each id through the per-row cache (the row layer)."
    >
      <Node x={20} y={68} w={160} h={56} title="repo.List" sub="(ctx, q)" />
      <Arrow x1={180} y1={96} x2={216} y2={96} />
      <TierTag x={216} y={54} text="result-set layer" />
      <Node
        x={216}
        y={68}
        w={228}
        h={56}
        title="id-list cache"
        sub="fingerprint(q) → [ids]"
      />
      <Arrow x1={444} y1={96} x2={480} y2={96} />
      <TierTag x={480} y={54} text="row layer · warm" />
      <Node
        x={480}
        y={68}
        w={220}
        h={56}
        title="GetMany(ids)"
        sub="id → row, per id"
      />
      <Label
        x={20}
        y={162}
        text="A miss at either layer falls through to Postgres and back-fills on the way out."
        muted
      />
    </Figure>
  );
}

/** Write-driven invalidation: a post-commit hook busts id-lists and evicts the row. */
export function CacheInvalidationDiagram() {
  return (
    <Figure
      viewBox="0 0 720 210"
      title="Write-driven cache invalidation"
      desc="On commit, a post-commit hook bumps the entity's cache generation (busting its cached id-lists) and evicts just the changed row — so a read right after a write never sees stale data."
    >
      <Node x={20} y={36} w={130} h={50} title="f.Exec(…)" />
      <Arrow x1={150} y1={61} x2={186} y2={61} accent />
      <Node x={186} y={36} w={110} h={50} title="commit" accent />
      <Arrow x1={296} y1={61} x2={332} y2={61} accent />
      <Node x={332} y={36} w={180} h={50} title="post-commit hook" accent />
      <Arrow x1={422} y1={86} x2={400} y2={132} />
      <Arrow x1={422} y1={86} x2={604} y2={132} />
      <Node
        x={300}
        y={134}
        w={200}
        h={52}
        title="bump generation"
        sub="busts cached id-lists"
      />
      <Node
        x={510}
        y={134}
        w={200}
        h={52}
        title="evict row(aggID)"
        sub="drops the changed row"
      />
    </Figure>
  );
}
