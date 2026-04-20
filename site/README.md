# honker.dev

Astro Starlight site for Honker. Landing + docs, deployed to Cloudflare Pages.

## Develop

```bash
cd site
npm install
npm run dev       # http://localhost:4321
```

## Build

```bash
npm run build     # outputs to dist/
npm run preview   # preview the built site locally
```

## Deploy to Cloudflare Pages

One-time setup:

```bash
npx wrangler login                                       # browser auth
npx wrangler pages project create honker \
  --production-branch=main                                # creates the project
```

Then point `honker.dev` at the Pages project in the Cloudflare dashboard
(Pages → honker → Custom domains → Set up a domain → `honker.dev`).

### Regular deploys

```bash
npm run deploy              # production → honker.dev
npm run deploy:preview      # preview → <hash>.honker.pages.dev
```

Each deploy runs `astro build` then `wrangler pages deploy dist`. The
`--commit-dirty=true` flag lets uncommitted changes deploy (useful
during iteration; tighten later by removing the flag and only deploying
from clean HEAD).

### CI / automated deploys

Cloudflare Pages has a built-in Git integration too: connect the repo
in the Pages dashboard, set the build command to `cd site && npm ci && npm run build`
and output directory to `site/dist/`. Push-to-deploy, no wrangler needed
in CI. Either flow works; the manual wrangler path is useful for
testing changes without a commit.

## Structure

```
src/
├── content/
│   └── docs/
│       ├── index.mdx           # Landing page (splash template)
│       ├── getting-started.mdx
│       ├── 404.md
│       ├── guides/             # One page per feature
│       │   ├── queues.mdx
│       │   ├── streams.mdx
│       │   ├── pubsub.mdx
│       │   └── scheduler.mdx
│       └── reference/
│           └── extension.mdx   # SQL function reference
├── styles/
│   └── custom.css              # Brand colors
└── content.config.ts
```

Add a page: drop a new `.mdx` file in `src/content/docs/` and register
it in the sidebar in `astro.config.mjs`.
