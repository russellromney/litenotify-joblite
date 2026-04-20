# honker.dev

Astro Starlight site for Honker. Landing + docs in one.

## Develop

```bash
cd site
npm install
npm run dev       # http://localhost:4321
```

## Build

```bash
npm run build     # outputs to dist/
npm run preview   # preview the built site
```

## Deploy

`dist/` is a static bundle — deploy to Cloudflare Pages, Vercel, Netlify, or any static host. Point `honker.dev` at it.

## Structure

```
src/
├── content/
│   └── docs/
│       ├── index.mdx           # Landing page
│       ├── getting-started.mdx
│       ├── guides/             # One page per feature
│       └── reference/          # SQL function reference
├── styles/
│   └── custom.css              # Brand colors
└── content.config.ts
```

To add a page, drop a new `.mdx` file in `src/content/docs/` and add it to the sidebar in `astro.config.mjs`.
