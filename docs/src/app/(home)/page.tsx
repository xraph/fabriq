import type { Metadata } from "next";
import { Agents } from "@/components/landing/agents";
import { Analytics } from "@/components/landing/analytics";
import { AnalyticsSink } from "@/components/landing/analytics-sink";
import { Console } from "@/components/landing/console";
import { BentoFooter } from "@/components/landing/footer";
import { LandingHero } from "@/components/landing/hero";
import { Invariants } from "@/components/landing/invariants";
import { Ports } from "@/components/landing/ports";
import { Stats } from "@/components/landing/stats";
import { WritePath } from "@/components/landing/write-path";

export const metadata: Metadata = {
  title: "Fabriq — one write path, every engine in step",
  description:
    "Fabriq is a standalone data fabric for Go — and the brain your AI agents think through. Commands commit once through a transactional outbox, then fan out to relational, time-series, vector, graph, search, and file engines, versioned and tenant-scoped. Agents reach the same fabric through fused recall, guarded memory, and live awareness.",
};

export default function HomePage() {
  return (
    <main className="flex-1">
      <LandingHero />
      <Invariants />
      <WritePath />
      <Ports />
      <Console />
      <Analytics />
      <AnalyticsSink />
      <Agents />
      <Stats />
      <BentoFooter />
    </main>
  );
}
