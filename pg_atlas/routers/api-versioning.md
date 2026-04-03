# API Versioning Strategy

## Current State (v0 Development)

All endpoints are served from the root URL `/` with no version prefix. The API is under active
development and breaking changes are expected. The OpenAPI spec at `/openapi.json` is the single
source of truth for the current API contract.

## Future Versioning: FastAPI Sub-Apps

When the API is feature-complete and refined from real-world usage, we will release a **v1.0.0**
stable interface. The unversioned API remains available for bleeding-edge development.

### Architecture

```txt
main app (/)           ← dev/latest (unversioned), may break at any time
├── v1 sub-app (/v1/)  ← first stable release
└── v2 sub-app (/v2/)  ← second stable release (when needed)
```

Each stable version is a **FastAPI sub-app** mounted at its version prefix. Stable sub-apps contain
thin wrapper routers with compatibility shims that translate requests/responses to the current
internal models. They do **not** re-export the same router objects — the isolation is explicit.

### Concurrent Versions

At most **2 stable versions** are maintained concurrently, plus the unversioned dev API:

- **Unversioned (`/`)**: latest development state. Breaking changes happen here first. The
  pg-atlas-frontend and pg-atlas-sdk dev branches consume this endpoint.
- **Stable (`/v1/`, `/v2/`)**: backwards-compatible. Only additive changes (new fields, new
  endpoints) are allowed. Existing fields and endpoint contracts are frozen.

### Shared Infrastructure

All versions share:

- The same PostgreSQL database and schema
- The same SQLAlchemy models and session management
- The same background computation pipeline (metrics, bootstrapper)

Only the API response shapes and endpoint signatures differ between versions. This keeps a single
`main` branch and a single deployment.

### Breaking Change Policy

When a new stable version (`v(N+1)`) is released:

1. The previous version (`v(N)`) enters a **6-month sunset period**.
2. Responses from the deprecated version include `Deprecation` and `Sunset` headers per
   [RFC 8594](https://www.rfc-editor.org/rfc/rfc8594).
3. After the sunset date, the deprecated version returns `410 Gone` for all requests.
4. The deprecated sub-app code is removed from the codebase.

### SDK Version Tie-In

The pg-atlas-sdk major version tracks the API stable version:

| SDK Version | API Version | Status |
| ----------- | ----------- | ------ |
| `@pg-atlas/sdk@0.x` | `/` | pre-v1 |
| `@pg-atlas/sdk@1.x` | `/v1/` | Stable |
| `@pg-atlas/sdk@2.x` | `/v2/` | Stable |
| `@pg-atlas/sdk@latest` | `/` | Dev (`main`) |

### Triggering Event for v1.0.0

The v1.0.0 stable release is triggered when:

1. All planned read-only endpoints are implemented and tested.
2. The pg-atlas-frontend has consumed the API in production for at least one SCF award round.
3. The pg-atlas-sdk has been used by at least one external consumer.
4. No breaking changes have been needed for at least 4 weeks.

### Why Not Version the API in Git Branches

Another strategy — separate deployment per version via DO App Platform ingress rules and long-lived
git branches — was considered but rejected:

- **DB schema divergence risk**: branches can diverge on migrations, leading to incompatible
  schemas against the shared database. This is catastrophic and easy to overlook.
- **Maintenance burden**: cherry-picking fixes across long-lived branches is error-prone.
- **No code sharing**: common logic must be duplicated or extracted into a shared package.

The chosen strategy keeps everything in a single `main` branch with explicit compatibility code,
making divergence visible in code review.
