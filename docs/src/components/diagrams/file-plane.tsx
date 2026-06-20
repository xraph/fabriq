import { Arrow, Figure, Label, Node } from "../diagram-kit";

/** The file plane's layering: config → DI provider → trove adapter → core port. */
export function FilePlaneLayeringDiagram() {
  return (
    <Figure
      viewBox="0 0 640 380"
      title="File-plane layering"
      desc="Storage config flows through the forgeext DI provider to the trove adapter (capability detection plus the CAS path backed by the blob_cas Postgres table) and is exposed as the pure core/blob.Store port — no storage-engine type crosses that boundary."
    >
      <Node
        x={170}
        y={20}
        w={300}
        h={52}
        title="config.yaml"
        sub="storageDriver · bucket · enableCas"
      />
      <Arrow x1={320} y1={72} x2={320} y2={100} />
      <Node
        x={170}
        y={100}
        w={300}
        h={52}
        title="forgeext storage provider"
        sub="DI · vessel.Provide"
      />
      <Arrow x1={320} y1={152} x2={320} y2={180} />
      <Node
        x={170}
        y={180}
        w={300}
        h={60}
        title="adapters/trove.Adapter"
        sub="trovestore · caps · CAS"
      />
      <Label x={486} y={214} text="CAS → blob_cas (Postgres)" muted />
      <Arrow x1={320} y1={240} x2={320} y2={268} accent />
      <Node
        x={170}
        y={268}
        w={300}
        h={52}
        title="core/blob.Store"
        sub="pure fabriq port"
        accent
      />
      <Label
        x={320}
        y={346}
        text="No storage-engine type crosses the port."
        anchor="middle"
        muted
      />
    </Figure>
  );
}
