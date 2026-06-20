// Shared theme-aware SVG primitives for docs diagrams. Pure SVG driven by the
// site's CSS variables (--surface-*, --ink-*, --accent), so light/dark flip for
// free — no theme detection, no client JS. Compose these into per-page diagrams
// (see components/diagrams/*) and register the diagram in components/mdx.tsx.

import type { ReactNode } from "react";

export const SANS = "var(--font-sans)";
export const MONO = "var(--font-mono)";
export const INK = "var(--ink)";
export const INK_STRONG = "var(--ink-strong)";
export const INK_MUTED = "var(--ink-muted)";
export const INK_FAINT = "var(--ink-faint)";
export const RAISED = "var(--surface-raised)";
export const SUNKEN = "var(--surface-sunken)";
export const LINE = "var(--hairline)";
export const ACCENT = "var(--accent)";

/** Arrowhead marker defs. Render once per <svg>; IDs are stable + identical
 *  across diagrams, so duplicate inline copies are harmless. */
export function Defs() {
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

/** Figure + responsive SVG wrapper with an accessible title/desc and arrow defs. */
export function Figure({
  viewBox,
  title,
  desc,
  children,
}: {
  viewBox: string;
  title: string;
  desc: string;
  children: ReactNode;
}) {
  const id = title.replace(/[^a-z0-9]+/gi, "-").toLowerCase();
  return (
    <figure className="my-6 not-prose">
      <svg
        viewBox={viewBox}
        width="100%"
        role="img"
        aria-labelledby={`${id}-t ${id}-d`}
        style={{ maxWidth: "100%", height: "auto" }}
        fontFamily={SANS}
      >
        <title id={`${id}-t`}>{title}</title>
        <desc id={`${id}-d`}>{desc}</desc>
        <Defs />
        {children}
      </svg>
    </figure>
  );
}

/** A small uppercase tier/lane label. */
export function TierTag({
  x,
  y,
  text,
}: {
  x: number;
  y: number;
  text: string;
}) {
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

/** A compact monospace chip (a small labelled box). */
export function Chip({
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

/** A titled node (title + optional monospace subtitle), accent for emphasis. */
export function Node({
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

/** A straight arrow between two points; accent or dashed variants available. */
export function Arrow({
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

/** Plain caption/annotation text. */
export function Label({
  x,
  y,
  text,
  anchor = "start",
  size = 11,
  muted,
}: {
  x: number;
  y: number;
  text: string;
  anchor?: "start" | "middle" | "end";
  size?: number;
  muted?: boolean;
}) {
  return (
    <text
      x={x}
      y={y}
      textAnchor={anchor}
      fontSize={size}
      fill={muted ? INK_MUTED : INK_FAINT}
    >
      {text}
    </text>
  );
}
