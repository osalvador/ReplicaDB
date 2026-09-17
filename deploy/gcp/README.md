# ReplicaDB Cloud Run bundle

This bundle is the self-managed Cloud Run deployment adapter for ReplicaDB. It
uses `gcloud` and existing Google Cloud resources; it is not a Google Cloud
Marketplace installer.

## Commands

```sh
./deploy.sh preflight --project PROJECT_ID --mode simple
./deploy.sh deploy --project PROJECT_ID --mode simple \
  --cloud-sql-instance INSTANCE --network NETWORK --subnet SUBNET
./deploy.sh deploy --project PROJECT_ID --mode simple --public-access \
  --cloud-sql-instance INSTANCE --network NETWORK --subnet SUBNET
./deploy.sh verify --project PROJECT_ID --deployment-id DEPLOYMENT_ID
./deploy.sh destroy --project PROJECT_ID --deployment-id DEPLOYMENT_ID
scripts/phase5-gcp-frontend-smoke.sh --project PROJECT_ID \
  --region REGION --service SERVICE_NAME
```

`simple` runs API-local execution. `distributed` runs the API without local
execution and adds a private Worker Pool. Distributed deployments require
`--worker-instances` of at least one.

The default image is the versioned public release image
`osalvador/replicadb-server:1.0.0`. Production deployments should provide an
explicit `sha256:` digest. The bundle never uses `latest` implicitly.
Use `--artifact-registry-image` to mirror the resolved image into a customer
registry. `latest` requires the explicit `--allow-latest` override.

Cloud SQL creation is opt-in and billable. The deployer displays a redacted
summary and accepts only the literal confirmation `CREATE CLOUD SQL`. Existing
Cloud SQL and Secret Manager resources can be supplied instead. Secret values,
database URLs, passwords, key material, and identity tokens are never printed
or stored in deployment state.

Copy `config.example.env` to a local ignored `config.env` for repeatable input.
The extracted server release package invokes the same script from
`deploy/gcp/deploy.sh`; no repository checkout is required.

Cloud Run ingress is private by default. `--public-access` (or
`REPLICADB_PUBLIC_ACCESS=true`) changes only the API service to public ingress
and grants `allUsers` the Cloud Run Invoker role; the Worker Pool remains
private. Spring Security still protects ReplicaDB API resources, so public
access exposes the login and frontend shell, not anonymous job data. Remove
the flag to restore private ingress; `destroy` removes the public binding
before deleting the service.

Public deployments require `REPLICADB_API_MIN_INSTANCES=1` (the default) or
an explicit `--api-min-instances` value of at least one. This keeps Quartz
running while the service has no browser traffic; it also means one Cloud Run
instance is billed continuously. Private deployments may use zero instances
for scale-to-zero smoke tests when scheduled execution is not required.

After enabling public access, run the repository smoke command above. It is
read-only and checks `/`, `/login`, CSRF cookie initialization, and the
unauthenticated protected-jobs boundary. It does not mutate IAM or deployment
resources. Credentialed Playwright execution is opt-in and requires a
username plus a password file supplied through environment variables.

## Marketplace boundary

This bundle does not publish or install a Marketplace product. A Marketplace
Container Image Product distributes an image, a GKE Marketplace App packages a
Kubernetes application, and Marketplace SaaS integrates a hosted service and
its commercial onboarding. None of those labels should be inferred from this
self-managed Cloud Run bundle.
