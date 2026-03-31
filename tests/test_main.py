"""Tests for the Dataset Marketplace API."""
from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest
from fastapi.testclient import TestClient

from src.main import app


@pytest.fixture()
def client():
    with TestClient(app) as c:
        yield c


# ---------------------------------------------------------------------------
# /health
# ---------------------------------------------------------------------------


def test_health(client):
    resp = client.get("/health")
    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "ok"
    assert body["datasets_count"] > 0  # seeded data


# ---------------------------------------------------------------------------
# GET /datasets
# ---------------------------------------------------------------------------


def test_list_datasets_default(client):
    resp = client.get("/datasets")
    assert resp.status_code == 200
    data = resp.json()
    assert data["total"] >= 8
    assert len(data["items"]) <= 20


def test_list_datasets_filter_by_category(client):
    resp = client.get("/datasets", params={"category": "finance"})
    assert resp.status_code == 200
    data = resp.json()
    assert all(item["category"] == "finance" for item in data["items"])


def test_list_datasets_search(client):
    resp = client.get("/datasets", params={"search": "S&P 500"})
    assert resp.status_code == 200
    data = resp.json()
    assert data["total"] >= 1


# ---------------------------------------------------------------------------
# GET /datasets/{id}
# ---------------------------------------------------------------------------


def test_get_dataset_existing(client):
    resp = client.get("/datasets/ds_sp500_ohlcv")
    assert resp.status_code == 200
    data = resp.json()
    assert data["id"] == "ds_sp500_ohlcv"
    assert "sample_rows" in data
    assert len(data["sample_rows"]) > 0


def test_get_dataset_not_found(client):
    resp = client.get("/datasets/nonexistent-ds")
    assert resp.status_code == 404


# ---------------------------------------------------------------------------
# POST /datasets
# ---------------------------------------------------------------------------


@patch("src.main.register_resource", new_callable=AsyncMock, return_value="res_test_001")
def test_publish_dataset(mock_register, client):
    payload = {
        "name": "Test Dataset",
        "description": "A dataset for testing purposes only",
        "category": "science",
        "schema": [{"name": "col1", "type": "string", "nullable": True}],
        "sample_rows": [{"col1": "value1"}],
        "price_full": 9.99,
        "price_per_query": 0.001,
        "tags": ["test"],
    }
    resp = client.post(
        "/datasets",
        json=payload,
        headers={"x-mainlayer-key": "test-api-key"},
    )
    assert resp.status_code == 201
    data = resp.json()
    assert data["mainlayer_resource_id"] == "res_test_001"
    assert "id" in data


def test_publish_dataset_requires_key(client):
    resp = client.post("/datasets", json={"name": "X"})
    assert resp.status_code == 422  # missing header


# ---------------------------------------------------------------------------
# GET /datasets/{id}/download
# ---------------------------------------------------------------------------


@patch("src.main.charge_download", new_callable=AsyncMock, return_value="pay_abc123")
def test_download_dataset(mock_charge, client):
    resp = client.get(
        "/datasets/ds_sp500_ohlcv/download",
        headers={"x-mainlayer-token": "buyer-token"},
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["payment_id"] == "pay_abc123"
    assert "download_url" in data


def test_download_requires_token(client):
    resp = client.get("/datasets/ds_sp500_ohlcv/download")
    assert resp.status_code == 422
