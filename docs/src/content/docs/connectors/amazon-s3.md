---
title: Amazon S3 connector
description: Amazon S3 sink behavior, object layout, and credential guidance.
---

# Amazon S3

Use the `s3:` scheme. S3 is sink-only and supports complete replication. It
does not provide table merge or complete-atomic behavior; choose the object
layout and replacement convention in the job that consumes the output.

Use the AWS runtime credential chain, workload identity, or an environment
managed profile. Do not place access keys in a connection string, options file,
JSON example, or documentation screenshot. Confirm bucket, prefix, region,
encryption, and object-write permissions before a large transfer.