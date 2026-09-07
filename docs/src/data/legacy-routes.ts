export interface LegacyRoute {
  path: string;
  canonicalPath: string;
  title: string;
  description: string;
  fragments: Record<string, string>;
}

export const legacyRoutes: LegacyRoute[] = [
  {
    path: '/server.html',
    canonicalPath: '/server/',
    title: 'ReplicaDB Server documentation moved',
    description: 'The managed-server guide now lives in the ReplicaDB documentation portal.',
    fragments: {}
  },
  {
    path: '/docs/docs.html',
    canonicalPath: '/cli/',
    title: 'ReplicaDB CLI documentation moved',
    description: 'The standalone CLI reference now lives in focused guides in the documentation portal.',
    fragments: {
      '21-replication-mode': '/cli/replication-modes/',
      '22-controlling-parallelism': '/cli/parallelism/',
      '32-connecting-to-a-database-server': '/connectors/generic-jdbc/',
      '33-selecting-the-data-to-replicate': '/cli/filtering-and-queries/',
      '412-supported-data-types-for-csv-file-as-source': '/connectors/csv/',
      '413-predefined-csv-formats': '/connectors/csv/'
    }
  },
  {
    path: '/docs/user-guide.html',
    canonicalPath: '/cli/',
    title: 'ReplicaDB user guide moved',
    description: 'The ReplicaDB user guide now lives in the documentation portal.',
    fragments: {}
  }
];

export const preservedStaticRoutes = [
  '/wizard/index.html',
  '/markdown/converter.html'
] as const;

export function findLegacyRoute(pathname: string) {
  return legacyRoutes.find((route) => route.path === pathname);
}