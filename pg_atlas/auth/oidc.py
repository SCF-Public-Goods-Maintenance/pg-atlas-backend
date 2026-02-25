"""GitHub OIDC token verification for PG Atlas write endpoints.

The SBOM action submits an RS256-signed JWT issued by GitHub's OIDC provider
with the PG Atlas API URL as the audience. This module verifies that token and
extracts the repository identity claims used for audit logging and attribution.

Usage (FastAPI dependency injection):

    from pg_atlas.auth.oidc import verify_github_oidc_token

    @router.post("/ingest/sbom")
    async def ingest_sbom(claims: Annotated[dict, Depends(verify_github_oidc_token)]):
        ...

Author: SCF Public Goods Maintenance <https://github.com/SCF-Public-Goods-Maintenance>
"""

from __future__ import annotations

import logging
from typing import Annotated, Any

import jwt
from cachetools import TTLCache
from fastapi import Header, HTTPException, status
from jwt import PyJWKClient, PyJWKClientError

from pg_atlas.config import settings

logger = logging.getLogger(__name__)

GITHUB_OIDC_ISSUER = "https://token.actions.githubusercontent.com"
GITHUB_JWKS_URL = f"{GITHUB_OIDC_ISSUER}/.well-known/jwks"

# In-process JWKS cache keyed by JWKS URL. A TTLCache with a single entry is
# used so that key rotation (rare in practice) is picked up within the TTL
# window without hammering GitHub's OIDC endpoint on every request.
# Thread/coroutine safety: cachetools.TTLCache is not inherently async-safe for
# concurrent cache misses, but the write is idempotent (same JWKS response),
# so a brief thundering-herd on expiry is harmless.
_jwks_cache: TTLCache[str, PyJWKClient] = TTLCache(
    maxsize=4,
    ttl=settings.JWKS_CACHE_TTL_SECONDS,
)


def _get_jwks_client() -> PyJWKClient:
    """Return a cached PyJWKClient for GitHub's OIDC JWKS endpoint.

    Creates a new client (and fetches the JWKS) on cache miss; returns the
    cached client on cache hit. The TTL is controlled by
    PG_ATLAS_JWKS_CACHE_TTL_SECONDS (default 1 hour).
    """
    cached: PyJWKClient | None = _jwks_cache.get(GITHUB_JWKS_URL)
    if cached is not None:
        return cached

    logger.debug("JWKS cache miss — fetching from %s", GITHUB_JWKS_URL)
    client = PyJWKClient(GITHUB_JWKS_URL)
    _jwks_cache[GITHUB_JWKS_URL] = client
    return client


async def verify_github_oidc_token(
    authorization: Annotated[str | None, Header()] = None,
) -> dict[str, Any]:
    """FastAPI dependency that verifies a GitHub OIDC Bearer token.

    Extracts the Bearer token from the Authorization header, verifies it
    against GitHub's published JWKS, and returns the decoded claims dict.

    Raises:
        HTTPException 401: if the Authorization header is missing or malformed.
        HTTPException 403: if the token signature, issuer, audience, or expiry
            is invalid, or if JWKS retrieval fails.

    Returns:
        dict: Decoded JWT claims. Guaranteed keys after successful verification:
            - ``repository``: "owner/repo" string identifying the submitting repo.
            - ``actor``: GitHub username that triggered the workflow run.
    """
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing or malformed Authorization header. Expected: Bearer <oidc-token>",
            headers={"WWW-Authenticate": "Bearer"},
        )

    token = authorization.removeprefix("Bearer ")

    try:
        jwks_client = _get_jwks_client()
        signing_key = jwks_client.get_signing_key_from_jwt(token)
    except (PyJWKClientError, Exception) as exc:
        logger.warning("JWKS retrieval or key selection failed: %s", exc)
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Unable to retrieve or select JWKS signing key.",
        ) from exc

    try:
        claims: dict[str, Any] = jwt.decode(
            token,
            signing_key,
            algorithms=["RS256"],
            audience=settings.API_URL,
            issuer=GITHUB_OIDC_ISSUER,
            options={"require": ["exp", "iss", "aud", "repository", "actor"]},
        )
    except jwt.ExpiredSignatureError as exc:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="OIDC token has expired.",
        ) from exc
    except jwt.InvalidTokenError as exc:
        logger.warning("OIDC token validation failed: %s", exc)
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=f"OIDC token validation failed: {exc}",
        ) from exc

    return claims
