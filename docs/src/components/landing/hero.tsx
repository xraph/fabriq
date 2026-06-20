"use client";

import { ShaderGradient, ShaderGradientCanvas } from "@shadergradient/react";
import { ArrowRight } from "lucide-react";
import Link from "next/link";
import { useTheme } from "next-themes";
import { Suspense, useEffect, useRef, useState } from "react";
import { TimelineAnimation } from "@/components/ui/timeline-animation";
import { docsRoute, gitConfig } from "@/lib/shared";
import { CodeFlow } from "./code-flow";
import { reveal } from "./reveal";

export function LandingHero() {
  const timelineRef = useRef<HTMLDivElement>(null);
  const { resolvedTheme } = useTheme();
  const [mounted, setMounted] = useState(false);
  useEffect(() => setMounted(true), []);
  const isLight = resolvedTheme === "light";
  // Render the sphere in both themes — only once we know which palette to use.
  const showShader = mounted && Boolean(resolvedTheme);
  // Copper core in both; fades to cream in light, to night in dark.
  // Light: bright lime wash. Dark: deep lime → black so white text stays legible.
  const shader = isLight
    ? {
        color1: "#84cc16",
        color2: "#bef264",
        color3: "#f7fee7",
        brightness: 1.2,
      }
    : {
        color1: "#4d7c0f",
        color2: "#365314",
        color3: "#0a0908",
        brightness: 1.15,
      };
  // In light mode the sphere is a muted wash so cream shows through (readable).
  const shaderOpacity = isLight ? 0.5 : 1;

  return (
    <section ref={timelineRef} className="relative overflow-hidden bg-surface">
      {/* hero backdrop — warm copper bloom (light) / shader sphere (dark) */}
      <div aria-hidden className="absolute inset-0">
        <div className="absolute inset-0 bg-[radial-gradient(ellipse_75%_55%_at_50%_-8%,rgba(132,204,22,0.20),transparent_62%),radial-gradient(ellipse_55%_45%_at_82%_18%,rgba(101,163,13,0.10),transparent_55%)]" />
        {showShader && (
          <Suspense fallback={null}>
            <ShaderGradientCanvas
              key={resolvedTheme}
              style={{
                position: "absolute",
                top: 0,
                left: 0,
                width: "100%",
                height: "115%",
                opacity: shaderOpacity,
              }}
              pixelDensity={1}
              pointerEvents="none"
            >
              <ShaderGradient
                animate="on"
                type="sphere"
                wireframe={false}
                shader="defaults"
                uTime={0}
                uSpeed={0.3}
                uStrength={0.4}
                uDensity={0.8}
                uFrequency={5.5}
                uAmplitude={7}
                positionX={0}
                positionY={0}
                positionZ={0}
                rotationX={0}
                rotationY={0}
                rotationZ={140}
                color1={shader.color1}
                color2={shader.color2}
                color3={shader.color3}
                reflection={0}
                cAzimuthAngle={250}
                cPolarAngle={140}
                cDistance={1.5}
                cameraZoom={12.5}
                lightType="3d"
                brightness={shader.brightness}
                envPreset="dawn"
                grain="on"
                toggleAxis={false}
                zoomOut={false}
                hoverState=""
                enableTransition={false}
              />
            </ShaderGradientCanvas>
          </Suspense>
        )}
        {/* settle the backdrop into the page */}
        <div className="absolute inset-0 bg-gradient-to-b from-transparent via-transparent to-surface" />
      </div>

      <div className="relative mx-auto grid min-h-[calc(100svh-3.5rem)] w-full max-w-6xl items-center gap-12 px-6 py-20 lg:grid-cols-[1.1fr_0.9fr] lg:py-24">
        {/* left — message */}
        <div className="flex flex-col items-start text-left">
          <TimelineAnimation
            timelineRef={timelineRef}
            animationNum={0}
            customVariants={reveal}
            className="flex items-center gap-2.5 rounded-2xl border border-ember-300/70 bg-ember-100/70 p-1 pr-3 backdrop-blur-lg dark:border-ember-700/50 dark:bg-ember-900/40"
          >
            <span className="rounded-lg bg-ember-500 px-2 py-0.5 font-mono text-[10px] font-semibold tracking-[0.12em] text-night-950">
              NEW
            </span>
            <span className="font-mono text-xs text-ember-800 dark:text-ember-100/90">
              Agent toolkit — the data fabric as an AI agent&apos;s brain
            </span>
          </TimelineAnimation>

          <TimelineAnimation
            timelineRef={timelineRef}
            as="h1"
            animationNum={1}
            customVariants={reveal}
            className="mt-8 mb-6 font-display text-5xl font-semibold leading-[1.02] tracking-tight text-ink-strong sm:text-6xl lg:text-[4.5rem] xl:text-[5.25rem]"
          >
            One write path.
            <br />
            Every engine, <em className="not-italic text-accent">in step.</em>
          </TimelineAnimation>

          <TimelineAnimation
            timelineRef={timelineRef}
            as="p"
            animationNum={2}
            customVariants={reveal}
            className="max-w-xl text-lg leading-relaxed font-light text-ink-muted md:text-xl"
          >
            Fabriq is a standalone data fabric for Go — and the brain your AI
            agents think through. Commands commit once through a transactional
            outbox, then fan out to relational, time-series, vector, graph,
            search, and file engines — versioned, tenant-scoped, always
            rebuildable. Agents reach that same fabric through fused recall,
            guarded memory, and a live watch stream.
          </TimelineAnimation>

          <div className="mt-10 flex flex-col items-start gap-4 sm:flex-row">
            <TimelineAnimation
              timelineRef={timelineRef}
              animationNum={3}
              customVariants={reveal}
            >
              <Link
                href={docsRoute}
                className="flex items-center gap-2 rounded-md bg-ink-strong px-6 py-3 font-semibold text-surface transition hover:opacity-90"
              >
                Read the docs <ArrowRight size={18} />
              </Link>
            </TimelineAnimation>
            <TimelineAnimation
              timelineRef={timelineRef}
              animationNum={4}
              customVariants={reveal}
            >
              <a
                href={`https://github.com/${gitConfig.user}/${gitConfig.repo}`}
                className="flex items-center gap-2.5 rounded-md border border-black/10 bg-black/[0.04] px-6 py-3 font-semibold text-ink backdrop-blur-md transition hover:bg-black/[0.07] dark:border-white/15 dark:bg-white/10 dark:text-stone-100 dark:hover:bg-white/20"
              >
                <svg
                  aria-hidden="true"
                  viewBox="0 0 24 24"
                  className="size-4 fill-current"
                >
                  <path d="M12 .5C5.65.5.5 5.65.5 12c0 5.08 3.29 9.39 7.86 10.91.58.11.79-.25.79-.55 0-.27-.01-1.17-.02-2.12-3.2.7-3.88-1.36-3.88-1.36-.52-1.33-1.28-1.68-1.28-1.68-1.04-.71.08-.7.08-.7 1.15.08 1.76 1.19 1.76 1.19 1.03 1.76 2.69 1.25 3.35.96.1-.75.4-1.25.73-1.54-2.55-.29-5.24-1.28-5.24-5.69 0-1.26.45-2.28 1.19-3.09-.12-.29-.52-1.46.11-3.05 0 0 .97-.31 3.17 1.18a11 11 0 0 1 5.78 0c2.2-1.49 3.17-1.18 3.17-1.18.63 1.59.23 2.76.11 3.05.74.81 1.19 1.83 1.19 3.09 0 4.42-2.7 5.39-5.27 5.68.41.36.78 1.06.78 2.14 0 1.54-.01 2.78-.01 3.16 0 .3.21.67.8.55A11.51 11.51 0 0 0 23.5 12C23.5 5.65 18.35.5 12 .5Z" />
                </svg>
                View on GitHub
              </a>
            </TimelineAnimation>
          </div>
        </div>

        {/* right — the write, illustrated */}
        <div className="w-full">
          <CodeFlow />
        </div>
      </div>
    </section>
  );
}
