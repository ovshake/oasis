import type { Config } from "tailwindcss";
import { tokens } from "./src/lib/tokens";

const config: Config = {
  content: ["./src/**/*.{ts,tsx}"],
  theme: {
    extend: {
      colors: {
        bg: tokens.color.bg,
        panel: tokens.color.panel,
        "panel-alt": tokens.color.panelAlt,
        border: tokens.color.border,
        "border-bright": tokens.color.borderBright,
        text: tokens.color.text,
        dim: tokens.color.dim,
        bullish: tokens.color.bullish,
        bearish: tokens.color.bearish,
        warn: tokens.color.warn,
        cyan: tokens.color.cyan,
        purple: tokens.color.purple,
      },
      fontFamily: {
        mono: tokens.fontFamily.mono.split(","),
      },
      boxShadow: {
        glow: tokens.glow.live,
        "glow-bull": tokens.glow.bull,
        "glow-bear": tokens.glow.bear,
      },
    },
  },
  plugins: [],
};
export default config;
