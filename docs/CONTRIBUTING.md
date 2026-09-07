# Documentation contribution guide

The Astro Starlight portal under `docs/src/content/docs` is the canonical
source for user-facing CLI, connector, server, architecture, operations, and
API guidance. Root and module READMEs retain repository setup and development
procedures; `RELEASE_GUIDE.md` retains release asset/checksum/tagging
procedures; `DEPLOYMENT.md` retains script-required deployment invariants.

## Generated artifact updates

- OpenAPI: run `bash docs/scripts/update-openapi.sh`, then run the docs check
  and OpenAPI contract tests.
- Screenshots: start the isolated local server and run
  `npm --prefix replicadb-server/frontend run test:e2e:docs`; curated images
  belong under `docs/src/assets/screenshots/server` and regression baselines
  stay under frontend E2E snapshots.
- Connector matrix: update `docs/src/data/connector-capabilities.json` and
  its connector page, then run the Node and Java capability contracts.
- Architecture defaults: update the relevant architecture/operations page
  and rerun `npm --prefix docs run test:all` after changing Java/YAML behavior.

Never commit credentials, keyrings, lease tokens, resolved datasource security,
or generated dependency/build directories.