# ReplicaDB Cloud Run bundle

This bundle is the self-managed Cloud Run deployment adapter for ReplicaDB. It
uses `gcloud` and existing Google Cloud resources; it is not a Google Cloud
Marketplace installer.

## Commands

```sh
./deploy.sh preflight --project PROJECT_ID --mode simple
./deploy.sh deploy --project PROJECT_ID --mode simple \
  --cloud-sql-instance INSTANCE --network NETWORK --subnet SUBNET
./deploy.sh verify --project PROJECT_ID --deployment-id DEPLOYMENT_ID
./deploy.sh destroy --project PROJECT_ID --deployment-id DEPLOYMENT_ID
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

## Marketplace boundary

This bundle does not publish or install a Marketplace product. A Marketplace
Container Image Product distributes an image, a GKE Marketplace App packages a
Kubernetes application, and Marketplace SaaS integrates a hosted service and
its commercial onboarding. None of those labels should be inferred from this
self-managed Cloud Run bundle.