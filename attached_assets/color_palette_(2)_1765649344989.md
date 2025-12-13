# AutonomOS Color Palette

This document defines the official color system used across the AutonomOS platform frontend.

## Brand Colors

### Primary Accent - Cyan
Used for primary interactions, highlights, and brand identity.

| Token | Hex | Usage |
|-------|-----|-------|
| `cyan-400` | #22D3EE | Primary text highlights, icons, active states |
| `cyan-500` | #06B6D4 | Focus rings, borders, interactive elements |
| `cyan-600` | #0891B2 | Buttons, active tabs, CTAs |

### Secondary Accent - Purple
Used for secondary elements, examples, and visual hierarchy.

| Token | Hex | Usage |
|-------|-----|-------|
| `purple-400` | #C084FC | Section headers, example labels |
| `purple-500` | #A855F7 | Accent icons, decorative elements |
| `purple-600` | #9333EA | Secondary buttons, gradient endpoints |

### Tertiary Accent - Blue
Used for informational elements, mode indicators, and links.

| Token | Hex | Usage |
|-------|-----|-------|
| `blue-400` | #60A5FA | Source labels, informational text |
| `blue-500` | #3B82F6 | Focus states, mode toggles |
| `blue-600` | #2563EB | AAM mode indicator, information badges |

## Semantic Colors

### Success - Green
| Token | Hex | Usage |
|-------|-----|-------|
| `green-400` | #4ADE80 | Success checkmarks, positive status |
| `green-500` | #22C55E | Success badges, confirmation states |
| `emerald-400` | #34D399 | Alternative success, live indicators |
| `emerald-500` | #10B981 | Environment mode (production) |

### Warning - Amber
| Token | Hex | Usage |
|-------|-----|-------|
| `amber-400` | #FBBF24 | Warning indicators, dev mode pulse |
| `amber-500` | #F59E0B | Warning badges, attention states |
| `yellow-400` | #FACC15 | Caution indicators |

### Error - Red
| Token | Hex | Usage |
|-------|-----|-------|
| `red-400` | #F87171 | Error messages, failed states |
| `red-500` | #EF4444 | Error badges, destructive actions |

### Info - Teal
| Token | Hex | Usage |
|-------|-----|-------|
| `teal-400` | #2DD4BF | Legend items, supplementary info |
| `teal-500` | #14B8A6 | Teal accents, AAM indicators |
| `teal-700` | #0F766E | Border accents |

## Neutral Colors

### Slate (Primary Neutral)
Used for backgrounds, panels, and subtle UI elements.

| Token | Hex | Usage |
|-------|-----|-------|
| `slate-400` | #94A3B8 | Secondary text, placeholders |
| `slate-500` | #64748B | Muted text, disabled states |
| `slate-700` | #334155 | Borders, dividers |
| `slate-800` | #1E293B | Panel backgrounds, cards |
| `slate-900` | #0F172A | Deep backgrounds, code blocks |

### Gray (Alternative Neutral)
Used for form elements, subtle backgrounds, and borders.

| Token | Hex | Usage |
|-------|-----|-------|
| `gray-400` | #9CA3AF | Placeholder text |
| `gray-700` | #374151 | Borders, subtle dividers |
| `gray-800` | #1F2937 | Input backgrounds, cards |
| `gray-900` | #111827 | Deep backgrounds |

### Base Colors
| Token | Hex | Usage |
|-------|-----|-------|
| `black` | #000000 | Page background, deep sections |
| `white` | #FFFFFF | Primary text, headings |

## Typography

### Font Family
- **Primary:** Quicksand (sans-serif)

### Text Colors
| Context | Token | Usage |
|---------|-------|-------|
| Primary text | `text-white` | Headings, body text |
| Secondary text | `text-gray-400` / `text-slate-400` | Labels, descriptions |
| Muted text | `text-gray-500` / `text-slate-500` | Hints, disabled text |
| Accent text | `text-cyan-400` | Highlights, emphasis |
| Link text | `text-cyan-400` hover:`text-cyan-300` | Clickable links |

## Gradient Patterns

### Brand Gradients
```css
/* Cyan to Purple */
bg-gradient-to-r from-cyan-600/20 to-purple-600/20

/* Blue to Purple (AAM) */
bg-gradient-to-r from-purple-600/20 to-blue-600/20

/* Multi-color sections */
bg-gradient-to-br from-purple-900/20 via-blue-900/20 to-green-900/20

/* Teal to Cyan (legends) */
bg-gradient-to-br from-teal-950 to-cyan-950

/* Blue to Purple (graphs) */
bg-gradient-to-br from-blue-900/20 to-purple-900/20
```

## Opacity Conventions

| Pattern | Usage |
|---------|-------|
| `/20` | Subtle backgrounds, hover states |
| `/30` | Border accents, light overlays |
| `/40` | Card backgrounds, panels |
| `/50` | Focus rings, prominent overlays |
| `/60` | Semi-transparent cards, modals |

## Navigation & Menu

### Sidebar (LeftNav)
```
Container: bg-gray-900 border-r border-gray-800
Nav item default: text-gray-400
Nav item hover: hover:bg-gray-800 hover:text-gray-200
Nav item active: bg-blue-600 text-white
Nav item pressed: active:bg-gray-700
Highlight ring: ring-1 ring-blue-500/30
```

### Top Bar
```
Container: bg-gray-900 border-b border-gray-800
Nav item default: text-gray-400
Nav item hover: hover:bg-gray-800 hover:text-gray-200
Nav item active: bg-blue-600 text-white
Help CTA pulse: ring-2 ring-[#0BCAD9]/50 shadow-lg shadow-[#0BCAD9]/30 animate-pulse
```

### Mobile Menu
```
Backdrop: bg-black/60
Panel: bg-gray-900 border-l border-gray-800
Close button: text-gray-400 hover:bg-gray-800
Divider: border-gray-800
```

### Auth Buttons (in nav)
```
Login: bg-gray-800 hover:bg-gray-700 text-gray-200
Sign Up: bg-cyan-600 hover:bg-cyan-500 text-white
```

### Notification Badge
```
Indicator dot: bg-red-500
Icon: text-gray-400
```

## Component Patterns

### Cards & Panels
```
Background: bg-slate-800/40 or bg-gray-800
Border: border-slate-700 or border-gray-700
Hover: hover:border-cyan-500/50
```

### Buttons
```
Primary: bg-cyan-600 text-white hover:bg-cyan-500
Secondary: bg-gray-800 text-gray-300 hover:bg-gray-700
Success: bg-green-600/20 border-green-500/40 text-green-300
```

### Form Inputs
```
Background: bg-gray-800 or bg-slate-900/60
Border: border-gray-700 or border-slate-700
Focus: focus:border-cyan-500 focus:ring-cyan-500/50
```

### Status Indicators
```
Active/Success: bg-green-400 or bg-emerald-400
Warning/Dev Mode: bg-amber-400 animate-pulse
Info/AAM Mode: bg-blue-400
```

## Mobile Considerations

### Touch Highlight
```css
-webkit-tap-highlight-color: rgba(11, 202, 217, 0.3); /* Cyan */
```

### Minimum Contrast
- Ensure `cyan-400` on `slate-800` backgrounds for accessibility
- Use `text-white` for primary content on dark backgrounds
- Avoid pure `gray-600` on `gray-900` (insufficient contrast)

## Dark Mode

The platform uses a dark-first design. All color tokens assume a dark background context. Light mode is not currently supported.

---

*Last Updated: December 2024*
