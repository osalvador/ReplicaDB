---
title: Sign in and profile
description: Use the authenticated session and understand profile boundaries.
---

# Sign in and profile

`/login` is the only public frontend route. The sign-in flow obtains CSRF
protection, submits the credentials to the session API, and redirects an
authenticated user to the control plane. Anonymous users who visit a protected
route are redirected back to sign in.

The navigation exposes `My profile` and `Logout`. Profile displays the current
username and role. Password self-service is intentionally unavailable: the
disabled fields explain that account self-management is not enabled. An ADMIN
can reset another user's password from [Users](/ReplicaDB/server/users/).

The frontend must never receive resolved datasource security values. Session
cookies and CSRF handling belong to the same origin as the API; do not copy
them into documentation examples or third-party request tools.