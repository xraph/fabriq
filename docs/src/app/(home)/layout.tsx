import { HomeLayout } from "fumadocs-ui/layouts/home";
import { baseOptions } from "@/lib/layout.shared";

export default function Layout({ children }: LayoutProps<"/">) {
  return (
    <div className="landing flex flex-1 flex-col bg-surface text-ink">
      <HomeLayout {...baseOptions()}>{children}</HomeLayout>
    </div>
  );
}
