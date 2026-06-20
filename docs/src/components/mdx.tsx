import { Step, Steps } from "fumadocs-ui/components/steps";
import { Tab, Tabs } from "fumadocs-ui/components/tabs";
import defaultMdxComponents from "fumadocs-ui/mdx";
import type { MDXComponents } from "mdx/types";
import {
  ArchitectureDiagram,
  DataLifecycleDiagram,
} from "./architecture-diagram";
import { BlobGcDiagram } from "./diagrams/blob-gc";
import {
  CacheInvalidationDiagram,
  CacheLayersDiagram,
} from "./diagrams/caching";
import { DistillWorkerDiagram } from "./diagrams/distill-worker";
import { FilePlaneLayeringDiagram } from "./diagrams/file-plane";

export function getMDXComponents(components?: MDXComponents) {
  return {
    ...defaultMdxComponents,
    Step,
    Steps,
    Tab,
    Tabs,
    ArchitectureDiagram,
    DataLifecycleDiagram,
    BlobGcDiagram,
    CacheInvalidationDiagram,
    CacheLayersDiagram,
    DistillWorkerDiagram,
    FilePlaneLayeringDiagram,
    ...components,
  } satisfies MDXComponents;
}

export const useMDXComponents = getMDXComponents;

declare global {
  type MDXProvidedComponents = ReturnType<typeof getMDXComponents>;
}
