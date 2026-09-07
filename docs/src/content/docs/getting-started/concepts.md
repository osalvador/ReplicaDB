---
title: Core concepts
description: A concise glossary for ReplicaDB CLI and managed-server guides.
---

# Core concepts

These terms keep the standalone and managed documentation precise.

## Source

The system, table, query, or file that provides rows to a replication.

## Sink

The system, table, or storage location that receives rows from a replication.

## Job

A managed-server definition that combines source and sink datasources with
replication settings, filters, retries, and scheduling choices.

## Task

A unit of work inside a run, commonly a table or source-to-sink pair that can
be processed with the configured parallelism.

## Run

One execution of a managed job or one CLI invocation. A run has an outcome,
diagnostics, and, in the server, durable state.

## Attempt

One try within a run. Retries create a new attempt while preserving the run's
history and outcome chain.

## Datasource

A managed-server connection profile that stores non-secret connector settings
and protects sensitive security values before persistence.

## Watermark

A source value used by incremental replication to bound the next read. The CLI
reports a successful watermark for external orchestration; a failed or
cancelled run does not advance it. In the managed server, only successful run
finalization commits the next watermark for a later attempt.

## API

The authenticated HTTP interface used by the managed frontend and automation.
It returns resource data, pagination, and structured errors; it is not part of
the standalone CLI contract.

## Worker

A managed-server process that executes claimed work. Workers renew leases and
are fenced from finalizing work after ownership expires.
