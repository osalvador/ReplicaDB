# GitHub Pages cutover

This portal is built as a static Astro artifact and deployed through GitHub
Actions. It is served under `/ReplicaDB` and does not require Jekyll, Bundler,
Java, PostgreSQL, credentials, or a running ReplicaDB server.

## Settings checklist

- [ ] Settings > Pages > Source is `GitHub Actions`.
- [ ] The `github-pages` environment exists and requires the repository’s normal
      deployment protections.
- [ ] Repository variable `DOCS_PAGES_SOURCE` is `actions`.
- [ ] The expected URL is `https://osalvador.github.io/ReplicaDB/`.
- [ ] The first manual workflow run completed and its Pages URL was checked.
- [ ] `/server.html`, `/docs/docs.html`, `/wizard/`, and `/markdown/` work from
      the production-shaped artifact.
- [ ] A last-known-good artifact name and workflow run ID are recorded below.

## Rollback

Rollback by redeploying the last-known-good Pages artifact through the same
workflow or by reverting the cutover/source commit and dispatching the
workflow again. Do not restore a generated Jekyll branch or mix Jekyll and
Astro ownership.

Last-known-good run: _record after the first production dry run._

Last-known-good artifact: _record after the first production dry run._