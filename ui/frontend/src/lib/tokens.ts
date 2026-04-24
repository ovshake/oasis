// Design tokens for the DeSimulator terminal UI.
//
// Palette sourced from defily.ai (the parent brand):
//   body bg          rgb(1, 1, 16)      -> #010110  deep near-black navy
//   panel indigo     rgb(14, 14, 51)    -> #0e0e33  Defily panel bg
//   primary purple   rgb(122, 47, 244)  -> #7a2ff4  Defily signature
//   text white       rgb(255, 255, 255) -> #ffffff
//   muted lavender   rgb(132, 126, 156) -> #847e9c  Defily dim text
//
// Semantic coloring enforced: bull/bear/warn are DATA colors; `cyan` and
// `purple` token names are CHROME (borders, indicators, branding) — the
// NAMES are kept from the original palette but VALUES now resolve to
// Defily's purple-family so components don't need a rename.
// NEVER hex-literal in components — always tokens.*.

export const tokens = {
  color: {
    bg:           "#010110",  // Defily body near-black navy
    panel:        "#0e0e33",  // Defily deep indigo panel
    panelAlt:     "#060620",  // slightly darker than bg for alt rows
    border:       "#1a1a44",  // subtle indigo border
    borderBright: "#2e2e5e",  // brighter indigo for emphasis
    text:         "#ffffff",  // Defily primary white
    dim:          "#847e9c",  // Defily muted lavender-gray
    bullish:      "#00ff88",  // green — universal trading semantic
    bearish:      "#ff3355",  // red — universal trading semantic
    warn:         "#ffaa00",  // amber — universal warning
    cyan:         "#7a2ff4",  // (was cyan) Defily signature purple — PRIMARY brand
    purple:       "#c84de8",  // (was violet) Defily magenta — secondary highlight
  },
  fontFamily: {
    mono: '"JetBrains Mono", "IBM Plex Mono", monospace',
    sans: '"Inter", system-ui, sans-serif',
  },
  glow: {
    live:   "0 0 16px rgba(122,47,244,0.45)",    // Defily purple glow
    bull:   "0 0 8px rgba(0,255,136,0.25)",
    bear:   "0 0 8px rgba(255,51,85,0.25)",
    brand:  "0 0 24px rgba(122,47,244,0.55)",    // stronger wash for the logo
  },
} as const;

export type Token = typeof tokens;
