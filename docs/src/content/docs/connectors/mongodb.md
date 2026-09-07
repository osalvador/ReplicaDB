---
title: MongoDB connector
description: MongoDB and MongoDB SRV source/sink capabilities and caveats.
---

# MongoDB

Use `mongodb:` or `mongodb+srv:`. MongoDB supports source and sink roles for
complete and incremental modes. The sink does not support complete-atomic
mode. Documents are read through an aggregation pipeline and written through
bulk operations; map representative nested, null, and binary values before a
production transfer.

SRV discovery, TLS, and authentication are properties of the deployment and
connection string. Resolve security through the runtime credential chain and
keep it out of committed examples.