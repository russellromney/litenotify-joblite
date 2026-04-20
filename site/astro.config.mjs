// @ts-check
import { defineConfig } from 'astro/config';
import starlight from '@astrojs/starlight';

// https://astro.build/config
export default defineConfig({
  site: 'https://honker.dev',
  integrations: [
    starlight({
      title: 'Honker',
      description:
        'Durable queues, streams, pub/sub, and scheduler on SQLite. One file, zero servers.',
      social: [
        { icon: 'github', label: 'GitHub', href: 'https://github.com/russellromney/honker' },
      ],
      sidebar: [
        {
          label: 'Start here',
          items: [
            { label: 'What is Honker?', slug: 'index' },
            { label: 'Getting started', slug: 'getting-started' },
          ],
        },
        {
          label: 'Guides',
          items: [
            { label: 'Queues', slug: 'guides/queues' },
            { label: 'Streams', slug: 'guides/streams' },
            { label: 'Pub/Sub', slug: 'guides/pubsub' },
            { label: 'Scheduler', slug: 'guides/scheduler' },
          ],
        },
        {
          label: 'Reference',
          items: [
            { label: 'SQL functions (extension)', slug: 'reference/extension' },
          ],
        },
      ],
      customCss: ['./src/styles/custom.css'],
    }),
  ],
});
