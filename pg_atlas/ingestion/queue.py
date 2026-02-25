"""
SBOM submission queue stub for PG Atlas.

In v0 (A3), the queue is a logging stub that records the submission and returns
a structured result. The A8 processing pipeline will replace this stub with a
Celery task dispatch that triggers schema validation, dependency extraction,
repo and edge upserts, and NetworkX graph reload.

Author: SCF Public Goods Maintenance <https://github.com/SCF-Public-Goods-Maintenance>
"""

from __future__ import annotations

import logging
from typing import Any

from pg_atlas.ingestion.spdx import ParsedSbom

logger = logging.getLogger(__name__)


def queue_sbom(sbom: ParsedSbom, claims: dict[str, Any]) -> dict[str, Any]:
    """
    Enqueue a validated SBOM submission for downstream processing.

    Currently a stub: logs the submission details and returns a structured
    response. No persistent queue is involved yet.

    Args:
        sbom: Validated ParsedSbom returned by parse_and_validate_spdx.
        claims: Decoded OIDC JWT claims dict. Must contain ``repository``
            (e.g. "owner/repo") and ``actor`` (triggering GitHub user).

    Returns:
        dict: Response payload for the 202 Accepted response, containing
            ``message``, ``repository``, and ``package_count``.
    """
    # TODO A8: replace this stub with a Celery task dispatch:
    #   from pg_atlas.workers.tasks import process_sbom
    #   task = process_sbom.delay(raw_spdx_bytes, claims)
    #   return {"message": "queued", "task_id": task.id, ...}
    repository: str = claims["repository"]
    actor: str = claims["actor"]

    logger.info(
        "SBOM submission received: repository=%s actor=%s packages=%d",
        repository,
        actor,
        sbom.package_count,
    )

    return {
        "message": "queued",
        "repository": repository,
        "package_count": sbom.package_count,
    }
