// Design tokens for the OASIS Crypto Sim terminal UI.
// Semantic coloring enforced: bull/bear/warn are DATA colors;
// cyan/purple are CHROME (borders, indicators); text/dim are neutral.
// NEVER hex-literal in components — always tokens.*.

export const tokens = {
  color: {
    bg:           "#0a0e14",
    panel:        "#121823",
    panelAlt:     "#0f1520",
    border:       "#1e2838",
    borderBright: "#2d3a52",
    text:         "#e4ecf7",
    dim:          "#7f8ea8",
    bullish:      "#00ff88",
    bearish:      "#ff3355",
    warn:         "#ffaa00",
    cyan:         "#00ddff",
    purple:       "#a855f7",
  },
  fontFamily: {
    mono: '"JetBrains Mono", "IBM Plex Mono", monospace',
    sans: '"Inter", system-ui, sans-serif',
  },
  glow: {
    live:   "0 0 12px rgba(0,221,255,0.3)",
    bull:   "0 0 8px rgba(0,255,136,0.25)",
    bear:   "0 0 8px rgba(255,51,85,0.25)",
  },
} as const;

export type Token = typeof tokens;
