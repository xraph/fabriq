import type { BaseLayoutProps } from "fumadocs-ui/layouts/shared";
import { appName, docsRoute, gitConfig } from "./shared";

export function baseOptions(): BaseLayoutProps {
  return {
    nav: {
      title: (
        <span className="inline-flex items-center gap-2.5">
          <svg
            aria-hidden="true"
            className="size-5"
            viewBox="0 0 32 32"
            fill="none"
            xmlns="http://www.w3.org/2000/svg"
          >
            <path
              d="M7 2v28M17 2v28"
              stroke="currentColor"
              strokeWidth="2.5"
              strokeLinecap="round"
              className="text-stone-500"
            />
            <path
              d="M2 9h28M2 21h28"
              stroke="#d1702e"
              strokeWidth="2.5"
              strokeLinecap="round"
            />
            <rect x="5.75" y="7.75" width="2.5" height="2.5" fill="#d1702e" />
            <rect x="15.75" y="19.75" width="2.5" height="2.5" fill="#d1702e" />
          </svg>
          <span className="font-semibold tracking-tight">{appName}</span>
        </span>
      ),
    },
    links: [
      {
        text: "Docs",
        url: docsRoute,
      },
    ],
    githubUrl: `https://github.com/${gitConfig.user}/${gitConfig.repo}`,
  };
}
