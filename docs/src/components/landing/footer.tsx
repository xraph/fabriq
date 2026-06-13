import { ArrowRight } from "lucide-react";
import Link from "next/link";
import { docsRoute, gitConfig } from "@/lib/shared";

const repoUrl = `https://github.com/${gitConfig.user}/${gitConfig.repo}`;

const exploreLinks = [
  { text: "Documentation", href: docsRoute },
  {
    text: "Operations runbook",
    href: `${repoUrl}/blob/main/docs/OPERATIONS.md`,
  },
  {
    text: "Migration discipline",
    href: `${repoUrl}/blob/main/docs/MIGRATIONS.md`,
  },
];

const connectLinks = [
  { text: "GitHub", href: repoUrl },
  { text: "Issues", href: `${repoUrl}/issues` },
  { text: "grove", href: "https://github.com/xraph/grove" },
  { text: "forge", href: "https://github.com/xraph/forge" },
];

export function BentoFooter() {
  return (
    <footer className="border-t border-hairline py-20">
      <div className="mx-auto w-full max-w-6xl px-6">
        <div className="grid grid-cols-1 gap-4 md:grid-cols-4">
          {/* main card */}
          <div className="flex flex-col justify-between rounded-2xl border border-hairline bg-surface-raised p-8 md:col-span-2 md:row-span-2">
            <div>
              <div className="mb-6 flex size-11 items-center justify-center rounded-xl border border-accent/25 bg-accent/10">
                <svg
                  aria-hidden="true"
                  className="size-6"
                  viewBox="0 0 32 32"
                  fill="none"
                  xmlns="http://www.w3.org/2000/svg"
                >
                  <path
                    d="M7 2v28M17 2v28"
                    stroke="currentColor"
                    className="text-ink-faint"
                    strokeWidth="2.5"
                    strokeLinecap="round"
                  />
                  <path
                    d="M2 9h28M2 21h28"
                    stroke="#84cc16"
                    strokeWidth="2.5"
                    strokeLinecap="round"
                  />
                  <rect
                    x="5.75"
                    y="7.75"
                    width="2.5"
                    height="2.5"
                    fill="#84cc16"
                  />
                  <rect
                    x="15.75"
                    y="19.75"
                    width="2.5"
                    height="2.5"
                    fill="#84cc16"
                  />
                </svg>
              </div>
              <h3 className="mb-4 font-display text-3xl font-semibold leading-[1.1] tracking-tight text-ink-strong md:text-4xl">
                Weave a data plane{" "}
                <em className="not-italic text-accent">you can trust.</em>
              </h3>
              <p className="max-w-md text-pretty leading-relaxed text-ink-muted">
                Start with Postgres and Redis. Add graph, search, vectors, and
                live documents when you need them — the invariants come
                standard.
              </p>
            </div>
            <div className="mt-12 flex flex-wrap gap-4">
              <Link
                href={docsRoute}
                className="flex items-center gap-2 rounded-lg bg-ember-500 px-6 py-2.5 text-sm font-semibold text-night-950 transition hover:bg-ember-400"
              >
                Get started <ArrowRight size={16} />
              </Link>
              <a
                href={repoUrl}
                className="rounded-lg border border-hairline px-6 py-2.5 text-sm font-semibold text-ink transition hover:bg-ink/[0.06]"
              >
                View source
              </a>
            </div>
          </div>

          {/* install card */}
          <div className="flex flex-col justify-between gap-6 rounded-2xl border border-hairline bg-surface-raised p-6">
            <span className="font-mono text-[10px] uppercase tracking-[0.3em] text-ink-muted">
              Install
            </span>
            <div className="space-y-3">
              <code className="block rounded-lg border border-white/10 bg-night-950 px-3.5 py-3 font-mono text-xs break-all text-ember-200">
                go get github.com/{gitConfig.user}/{gitConfig.repo}
              </code>
              <p className="font-mono text-[10px] uppercase tracking-[0.2em] text-ink-faint">
                Go 1.25+ · Built on grove &amp; forge
              </p>
            </div>
          </div>

          {/* connect card */}
          <div className="flex flex-col justify-between gap-6 rounded-2xl border border-hairline bg-surface-raised p-6">
            <span className="font-mono text-[10px] uppercase tracking-[0.3em] text-ink-muted">
              Connect
            </span>
            <div className="flex flex-wrap gap-2">
              {connectLinks.map((link) => (
                <a
                  key={link.text}
                  href={link.href}
                  className="rounded-full border border-hairline bg-surface-sunken px-3.5 py-1.5 text-xs font-medium text-ink transition hover:border-accent/40 hover:text-accent"
                >
                  {link.text}
                </a>
              ))}
            </div>
          </div>

          {/* explore card */}
          <div className="flex flex-col justify-between gap-6 rounded-2xl border border-hairline bg-surface-raised p-6">
            <span className="font-mono text-[10px] uppercase tracking-[0.3em] text-ink-muted">
              Explore
            </span>
            <ul className="space-y-2.5">
              {exploreLinks.map((link) => (
                <li key={link.text}>
                  <Link
                    href={link.href}
                    className="text-sm text-ink transition hover:text-accent"
                  >
                    {link.text}
                  </Link>
                </li>
              ))}
            </ul>
          </div>

          {/* legal card */}
          <div className="flex items-center justify-between rounded-2xl border border-hairline bg-surface-raised p-6">
            <span className="text-xs text-ink-muted">
              Part of the Forge ecosystem
            </span>
            <span className="font-mono text-xs text-ink-faint">
              © 2026 Fabriq
            </span>
          </div>
        </div>
      </div>
    </footer>
  );
}
