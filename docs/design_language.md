# Design Language Document: Tracing Insights F1 Web

This document provides a comprehensive overview of the design patterns, tokens, and code conventions used in the Tracing Insights F1 Web codebase. It is optimized for use by AI/LLMs during code generation.

---

## 1. Design Tokens

### 🎨 Color Palette
The system uses **DaisyUI v4** semantic tokens within **Tailwind CSS v3**. The primary theme is `mytheme`, but team-specific themes are also defined.

**Standard Semantic Tokens:**
- `--p` (`primary`): `#00ff00` (Tracing Insights Green)
- `--s` (`secondary`): `#ff00ff`
- `--a` (`accent`): `#F471B5`
- `--n` (`neutral`): `#1E293B`
- `--b1` (`base-100`): `#002451` (Deep Navy)
- `--in` (`info`): `#0CA5E9`
- `--su` (`success`): `green`
- `--wa` (`warning`): `orange`
- `--er` (`error`): [red](file:///c:/Users/haris/Documents/GitHub/f1-webv2/hf/templates/base.html#938-949)

**Team Themes (Sample Values):**
- `mercedes`: primary `#00d2be`
- `ferrari`: primary `#e6194b`
- `mclaren`: primary `#f58231`
- `redbull`: primary `#ffe119`

### Typography
The system uses a mix of display and body fonts.
- **Display/Metal Heading**: `Bebas Neue` (`font-metalfont`, `font-sans`). High-impact, uppercase style for titles and callouts.
- **Body Text**: `Roboto` (`font-roboto`). Used for general readability and paragraphs.
- **Logo/Script**: `Great Vibes` (`font-logofont`). Used sparingly for branding.

### Spacing & Borders
- **Spacing Scale**: Standard Tailwind scale (e.g., `p-4`, `m-8`, `gap-6`).
- **Border Radius**: `rounded-lg` (default for cards), `rounded-full` (for pills/buttons), `rounded-box` (standard DaisyUI containers).
- **Shadows**: `shadow-xl` (standard), `shadow-primary/20` (glow effect for primary elements).

---

## 2. Layout System

### Containers
- **Primary Container**: `container mx-auto max-w-1200`
- **Article/Detail**: `mx-auto max-w-2xl` (for reading focus)

### Grid System
- **Listings**: Standard responsive grid pattern:
  ```html
  <div class="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-8">
  ```

### Breakpoints
The system adheres to standard Tailwind breakpoints:
- `sm`: 640px
- [md](file:///c:/Users/haris/Documents/GitHub/f1-webv2/repo.md): 768px
- `lg`: 1024px
- `xl`: 1280px

---

## 3. Component Patterns

### Cards (Tracing Insights Style)
Used for listing blog posts and analysis items.
- **Structure**: `card h-full bg-[#011627]/90 shadow-xl border border-primary/40 overflow-hidden`
- **Interaction**: `hover:border-primary transition-all duration-300 transform hover:-translate-y-2 hover:shadow-primary/20 hover:shadow-2xl`
- **Internal Elements**: `figure` for images (with `group-hover:scale-110`), `card-body` for content.

### Buttons
- **Standard**: `btn` (DaisyUI).
- **Primary**: `btn btn-primary text-base-100`.
- **Transitions**: `transform transition hover:scale-105 hover:shadow-md`.

### Selects & Controls
- **TomSelect Implementation**: Standard selection UI used for year/event/driver filtering.
  ```javascript
  new TomSelect("#id", { plugins: ["dropdown_input", "remove_button"], ... });
  ```

### Banners / Notifications
- **Animations**:
  ```html
  <section class="opacity-0 -translate-y-2.5 transition-all ease-out bg-black ...">
  ```
- **States**: Uses `.hidden` or `opacity-0` with JavaScript for toggling.

---

## 4. Motion & Interaction

### Standard Motion
- **Transitions**: `transition-all duration-300 ease-in-out` (applied to cards, links, and buttons).
- **Banner Entrance**: `opacity-0 -translate-y-2.5` transitioning to `opacity-100 translate-y-0`.

### Micro-interactions
- **Hover Scale**: `group-hover:scale-110` for images in cards.
- **Vertical Hover**: `hover:-translate-y-2` for interactive cards.
- **Pulse**: `animate-ping` (standard DaisyUI) for live indicators.

---

## 5. Code Conventions

### Django Template Structure
- **Base Extension**: `{% extends "base.html" %}`.
- **Block Organization**:
  - `head`: For page-specific meta tags and stylesheets.
  - `content`: Primary page body.
  - `site_header`: (Optional) Overrides for navigation.

### Component Co-location
- Small reusable HTML snippets are located in `hf/templates/components/`.
- JavaScript state often resides in `<script>` tags at the bottom of templates or in `hf/static/js/`.

---

## 6. Usage Rules (LLM Instructions)

1. **Prefer Semi-Custom Cards**: When creating cards, always wrap a DaisyUI `.card` in a `group` and apply `hover:-translate-y-2 border-primary/40 hover:border-primary`.
2. **Standard Containers**: For main page content, wrap in `<div class="container mx-auto max-w-1200">`. For long-form text, use `max-w-2xl`.
3. **Typography Roles**: Use `font-metalfont` (Bebas Neue) for H1-H3 headers and `font-roboto` for body content.
4. **Semantic Colors Only**: Never hardcode colors like `#00ff00`. Use `text-primary`, `bg-primary`, or `border-secondary`.
5. **DaisyUI Hierarchy**: Use `.tabs-boxed` for tabbed navigation and `.navbar` for headers.
6. **Animated Entrance**: For banners or new sections, use `opacity-0 -translate-y-2.5` with a `DOMContentLoaded` script to trigger `opacity-100 translate-y-0`.

---

## ⚠️ Inconsistencies

- **Container Max-Widths**: Inconsistent usage of `max-w-1200` vs `max-w-7xl` across listing pages. **Rule**: Prefer `max-w-1200` for the main dashboard and `max-w-2xl` for articles.
- **Card Styling**: Some cards use `bg-[#011627]/90` while others use `bg-base-100/20`. **Rule**: Use `bg-[#011627]/90` for primary content cards.
- **Inconsistent Buttons**: Some buttons use manual `bg-secondary` instead of the DaisyUI `.btn-secondary` component class. **Rule**: Always use `.btn`.
