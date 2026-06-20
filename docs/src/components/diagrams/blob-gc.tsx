import { Arrow, Figure, Label, Node } from "../diagram-kit";

/** The leader-elected blob-CAS garbage collector. */
export function BlobGcDiagram() {
  return (
    <Figure
      viewBox="0 0 540 330"
      title="Blob-CAS garbage collector"
      desc="One replica holds advisory lock 1004 and runs runBlobGC on a ticker; each tick calls gcBlobAll, which reconciles every tenant's blob CAS with repair enabled."
    >
      <Node
        x={150}
        y={20}
        w={240}
        h={52}
        title="advisory lock 1004"
        sub="lockKeyBlobGC"
        accent
      />
      <Arrow x1={270} y1={72} x2={270} y2={100} />
      <Node
        x={150}
        y={100}
        w={240}
        h={52}
        title="runBlobGC"
        sub="ticker · ReconcileInterval"
      />
      <Arrow x1={270} y1={152} x2={270} y2={180} />
      <Node x={150} y={180} w={240} h={46} title="gcBlobAll" />
      <Arrow x1={270} y1={226} x2={270} y2={254} />
      <Node
        x={150}
        y={254}
        w={240}
        h={52}
        title="rec.Reconcile(repair)"
        sub="per tenant"
      />
      <Label
        x={270}
        y={324}
        text="Leader-elected — exactly one replica collects across all tenants."
        anchor="middle"
        muted
      />
    </Figure>
  );
}
