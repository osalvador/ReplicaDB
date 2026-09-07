# Documentation contribution guide

The Astro Starlight portal under `docs/src/content/docs` is the canonical
source for user-facing CLI, connector, server, architecture, operations, and
API guidance. Root and module READMEs retain repository setup and development
procedures; `RELEASE_GUIDE.md` retains release asset/checksum/tagging
procedures; `DEPLOYMENT.md` retains script-required deployment invariants.

## Generated artifact updates

- OpenAPI: run `bash docs/scripts/update-openapi.sh`, then run the docs check
  and regenerate frontend types from that exact file with
  `OPENAPI_SCHEMA_FILE="$PWD/docs/openapi/replicadb-server.json" npm --prefix replicadb-server/frontend run generate:api-types`.
- Screenshots: start the isolated local server and run
  `npm --prefix replicadb-server/frontend run test:e2e:docs`. The dedicated
  `playwright.docs.config.ts` writes curated public images under
  `docs/src/assets/screenshots/server` and checks baselines under frontend E2E
  snapshots. The generic `test:e2e` command excludes both documentation
  captures and visual regression so it cannot alter public documentation assets.
- Connector matrix: update `docs/src/data/connector-capabilities.json` and
  its connector page, then run the Node and Java capability contracts.
- Architecture defaults: update the relevant architecture/operations page
  and rerun `npm --prefix docs run test:all` after changing Java/YAML behavior.
- Portal verification: run `npm --prefix docs run check`, `build`, `validate`,
  and `test`; run the documentation Playwright suite for navigation, responsive,
  or rendered API changes.

Never commit credentials, keyrings, lease tokens, resolved datasource security,
or generated dependency/build directories.
