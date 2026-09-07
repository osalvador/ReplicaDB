import { defineConfig } from 'astro/config';
import mdx from '@astrojs/mdx';
import sitemap from '@astrojs/sitemap';
import starlight from '@astrojs/starlight';
import starlightOpenAPI, { createOpenAPISidebarGroup } from 'starlight-openapi';
import { finalizeApiReference } from './scripts/finalize-api-reference.mjs';

const openApiSidebarGroup = createOpenAPISidebarGroup();

/** @type {import('astro').AstroIntegration} */
const apiReferenceFinalizer = {
  name: 'replicadb-api-tag-titles',
  hooks: {
    'astro:build:done': async ({ dir }) => finalizeApiReference(dir)
  }
};

export default defineConfig({
  site: 'https://osalvador.github.io/ReplicaDB',
  base: '/ReplicaDB',
  build: {
    format: 'directory'
  },
  integrations: [
    apiReferenceFinalizer,
    starlight({
      title: 'ReplicaDB Documentation',
      description: 'Documentation for the ReplicaDB CLI and managed server.',
      favicon: '/favicon.svg',
      customCss: ['./src/styles/custom.css'],
      defaultLocale: 'root',
      locales: {
        root: {
          label: 'English',
          lang: 'en'
        }
      },
      sidebar: [
        {
          label: 'Start here',
          items: [
            'index',
            {
              label: 'Getting started',
              items: [
                'getting-started/choose-cli-or-server',
                'getting-started/cli-quickstart',
                'getting-started/server-quickstart',
                'getting-started/concepts'
              ]
            },
            {
              label: 'CLI',
              items: [
                'cli',
                'cli/installation',
                'cli/configuration',
                'cli/replication-modes',
                'cli/parallelism',
                'cli/filtering-and-queries',
                'cli/multi-table',
                'cli/incremental-watermarks',
                'cli/performance',
                'cli/troubleshooting'
              ]
            },
            {
              label: 'Reference',
              items: [
                'reference/cli-options',
                'reference/example-options-files'
              ]
            },
            {
              label: 'Connectors',
              items: [
                'connectors',
                'connectors/oracle',
                'connectors/postgresql',
                'connectors/mysql-mariadb',
                'connectors/sql-server',
                'connectors/db2',
                'connectors/sqlite',
                'connectors/mongodb',
                'connectors/csv',
                'connectors/amazon-s3',
                'connectors/kafka',
                'connectors/denodo',
                'connectors/generic-jdbc'
              ]
            },
            {
              label: 'Server',
              items: [
                'server',
                'server/installation',
                'server/sign-in-and-profile',
                'server/dashboard',
                'server/datasources',
                'server/jobs',
                'server/schedules',
                'server/runs-and-diagnostics',
                'server/users',
                'server/permissions',
                'server/audit',
                'server/errors-and-empty-states'
              ]
            },
            {
              label: 'Architecture',
              items: [
                'architecture/overview',
                'architecture/core-and-server-boundaries',
                'architecture/distributed-topology',
                'architecture/run-lifecycle',
                'architecture/dispatch-and-recovery',
                'architecture/concurrency-and-fencing',
                'architecture/scheduling-and-ha',
                'architecture/scaling-and-fairness',
                'architecture/security-boundaries'
              ]
            },
            {
              label: 'Operations',
              items: [
                'operations',
                'operations/local-server',
                'operations/distributed-deployment',
                'operations/configuration',
                'operations/capacity-planning',
                'operations/health-and-metrics',
                'operations/security-and-tls',
                'operations/key-management',
                'operations/backups-and-restore',
                'operations/upgrades',
                'operations/failure-recovery',
                'operations/troubleshooting',
                'reference/environment-variables'
              ]
            },
            {
              label: 'API reference',
              items: ['api-introduction', openApiSidebarGroup]
            }
          ]
        }
      ],
      social: [
        {
          icon: 'github',
          label: 'GitHub',
          href: 'https://github.com/osalvador/ReplicaDB'
        }
      ],
      editLink: {
        baseUrl: 'https://github.com/osalvador/ReplicaDB/edit/master/docs/'
      },
      plugins: [
        starlightOpenAPI([{
          base: 'api',
          schema: './openapi/replicadb-server.json',
          sidebar: {
            label: 'API reference',
            group: openApiSidebarGroup,
            operations: { labels: 'summary', sort: 'document' },
            tags: { sort: 'document' }
          }
        }])
      ]
    }),
    mdx(),
    sitemap()
  ]
});
