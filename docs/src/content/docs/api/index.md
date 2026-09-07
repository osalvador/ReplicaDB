---
title: API reference
description: Read-only reference for the authenticated ReplicaDB server API.
slug: api-introduction
---

# API reference

The endpoint pages below are generated from the tested local Springdoc schema.
They describe the session and CSRF model, permissions, pagination,
idempotency, and RFC 7807 problem details.

The public documentation does not proxy live requests. The API uses
same-origin session cookies and CSRF protection, so interactive calls should be
made from an authenticated deployment or a controlled local client rather than
sending credentials through a third-party documentation host.