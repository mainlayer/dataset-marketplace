"""
Dataset Marketplace — FastAPI application.

Endpoints:
  GET  /datasets                    List available datasets
  GET  /datasets/{id}               Dataset detail
  POST /datasets                    Publish a new dataset
  GET  /datasets/{id}/download      Download dataset (requires Mainlayer payment)
  GET  /health                      Health check
"""
from __future__ import annotations

import hashlib
import json
import logging
from typing import Optional

from fastapi import FastAPI, Header, HTTPException, Query, status
from fastapi.middleware.cors import CORSMiddleware

from .billing import charge_download, register_resource
from .datasets_db import DatasetStore, dataset_store
from .models import (
    DatasetCategory,
    DatasetDetail,
    DatasetPublishRequest,
    DatasetPublishResponse,
    DatasetSummary,
    ErrorResponse,
    HealthResponse,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="Dataset Marketplace",
    description="Buy and sell datasets. Payments powered by Mainlayer.",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

_store: DatasetStore = dataset_store


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------


@app.get("/health", response_model=HealthResponse, tags=["system"])
async def health() -> HealthResponse:
    return HealthResponse(status="ok", version="1.0.0", datasets_count=_store.count())


@app.get("/datasets", tags=["datasets"])
async def list_datasets(
    category: Optional[DatasetCategory] = Query(None),
    tag: Optional[str] = Query(None),
    search: Optional[str] = Query(None),
    page: int = Query(1, ge=1),
    per_page: int = Query(20, ge=1, le=100),
) -> dict:
    """
    Browse all available datasets. Free to list — payment required to download.
    """
    results = _store.list_all(category=category, tag=tag, search=search)
    total = len(results)
    start = (page - 1) * per_page
    items = results[start : start + per_page]

    summaries = []
    for r in items:
        summaries.append(
            DatasetSummary(
                id=r["id"],
                name=r["name"],
                description=r["description"],
                category=r["category"],
                tags=r.get("tags", []),
                price_full=r["price_full"],
                price_per_query=r.get("price_per_query", 0.002),
                row_count=r.get("row_count"),
                size_bytes=r.get("size_bytes"),
                column_count=len(r.get("schema", [])),
                created_at=r["created_at"],
                mainlayer_resource_id=r.get("mainlayer_resource_id"),
            ).model_dump()
        )

    return {"items": summaries, "total": total, "page": page, "per_page": per_page}


@app.get("/datasets/{dataset_id}", response_model=DatasetDetail, tags=["datasets"])
async def get_dataset(dataset_id: str) -> DatasetDetail:
    """Get full metadata and sample rows for a dataset (free)."""
    raw = _store.get(dataset_id)
    if not raw:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Dataset not found.")
    return DatasetDetail(**raw)


@app.post(
    "/datasets",
    response_model=DatasetPublishResponse,
    status_code=status.HTTP_201_CREATED,
    tags=["datasets"],
)
async def publish_dataset(
    payload: DatasetPublishRequest,
    x_mainlayer_key: str = Header(..., description="Your Mainlayer API key for revenue routing"),
) -> DatasetPublishResponse:
    """
    Publish a new dataset. Revenue from downloads is routed to your Mainlayer account.
    """
    resource_id = await register_resource(
        dataset_id="pending",
        name=payload.name,
        price_full=payload.price_full,
    )

    ds_data = payload.model_dump()
    ds_data["mainlayer_resource_id"] = resource_id

    ds_id = _store.add(ds_data)
    raw = _store.get(ds_id)

    detail = DatasetDetail(**raw)
    logger.info("Dataset published: %s (resource=%s)", ds_id, resource_id)

    return DatasetPublishResponse(
        id=ds_id,
        mainlayer_resource_id=resource_id,
        message="Dataset published successfully.",
        dataset=detail,
    )


@app.get("/datasets/{dataset_id}/download", tags=["datasets"])
async def download_dataset(
    dataset_id: str,
    x_mainlayer_token: str = Header(..., description="Mainlayer payment token"),
) -> dict:
    """
    Download the full dataset. Requires a valid Mainlayer payment token.

    The `x-mainlayer-token` header is charged for the dataset price.
    """
    raw = _store.get(dataset_id)
    if not raw:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Dataset not found.")

    resource_id = raw.get("mainlayer_resource_id", f"res_{dataset_id}")
    payment_id = await charge_download(
        resource_id=resource_id,
        amount_usd=raw["price_full"],
        buyer_token=x_mainlayer_token,
        dataset_id=dataset_id,
    )

    logger.info("Dataset download charged: dataset=%s payment=%s", dataset_id, payment_id)

    # In production, return a pre-signed URL or stream the data
    return {
        "dataset_id": dataset_id,
        "payment_id": payment_id,
        "download_url": f"https://datasets.mainlayer.fr/{dataset_id}/data.parquet",
        "expires_in_seconds": 3600,
        "row_count": raw.get("row_count"),
        "size_bytes": raw.get("size_bytes"),
    }


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("src.main:app", host="0.0.0.0", port=8001, reload=True)
