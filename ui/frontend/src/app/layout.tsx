import type { Metadata } from "next";
import "./globals.css";
import { TopBar } from "@/components/TopBar";

export const metadata: Metadata = {
  title: "DeSimulator — Defily",
  description: "Narrative-driven cross-asset market simulation, by Defily",
};

export default function RootLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  return (
    <html lang="en">
      <body>
        <TopBar />
        <main className="min-h-screen">{children}</main>
      </body>
    </html>
  );
}
