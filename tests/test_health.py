"""Tests for GET /health.

Author: SCF Public Goods Maintenance <https://github.com/SCF-Public-Goods-Maintenance>
"""

from httpx import AsyncClient


async def test_health_returns_200(async_client: AsyncClient) -> None:
    """GET /health should return 200 OK."""
    response = await async_client.get("/health")
    assert response.status_code == 200


async def test_health_response_shape(async_client: AsyncClient) -> None:
    """GET /health response body should contain status and version fields."""
    response = await async_client.get("/health")
    body = response.json()
    assert body["status"] == "ok"
    assert "version" in body
    assert isinstance(body["version"], str)
