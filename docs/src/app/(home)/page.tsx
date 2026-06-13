import type { Metadata } from "next";
import { BentoFooter } from "@/components/landing/footer";
import { LandingHero } from "@/components/landing/hero";
import { Invariants } from "@/components/landing/invariants";
import { Ports } from "@/components/landing/ports";
import { Stats } from "@/components/landing/stats";
import { WritePath } from "@/components/landing/write-path";

export const metadata: Metadata = {
  title: "Fabriq — one write path, every engine in step",
  description:
    "Fabriq is a standalone data fabric for Go: commands commit once through a transactional outbox, then fan out to relational, time-series, vector, graph, and search engines — versioned, tenant-scoped, always rebuildable.",
};

export default function HomePage() {
  return (
    <main className="flex-1">
      <LandingHero />
      <Invariants />
      <WritePath />
      <Ports />
      <Stats />
      <BentoFooter />
    </main>
  );
}
