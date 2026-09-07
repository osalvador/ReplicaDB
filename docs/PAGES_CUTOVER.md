# GitHub Pages cutover

This portal is built as a static Astro artifact and deployed through GitHub
Actions. It is served under `/ReplicaDB` and does not require Jekyll, Bundler,
Java, PostgreSQL, credentials, or a running ReplicaDB server.

## Settings checklist

- [x] Settings > Pages > Source is `GitHub Actions`.
- [x] The `github-pages` environment exists and requires the repository’s normal
      deployment protections.
- [x] Repository variable `DOCS_PAGES_SOURCE` is `actions`.
- [x] The expected URL is `https://osalvador.github.io/ReplicaDB/`.
- [x] The first manual workflow run completed and its Pages URL was checked.
- [x] `/server.html`, `/docs/docs.html`, `/docs/user-guide.html`, `/wizard/`,
      and `/markdown/` work from the production-shaped artifact.
- [x] A last-known-good artifact name and workflow run ID are recorded below.

## Rollback

Rollback by redeploying the last-known-good Pages artifact through the same
workflow or by reverting the cutover/source commit and dispatching the
workflow again. Do not restore a generated Jekyll branch or mix Jekyll and
Astro ownership.

Last-known-good run: `34096224736` (`Documentation Pages`, successful)

Last-known-good artifact: `github-pages` (2,060,786 bytes)
