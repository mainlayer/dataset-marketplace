"""
Mainlayer billing integration for the Dataset Marketplace.

Every download or query requires a Mainlayer payment token.
"""
from __future__ import annotations

import logging
import os

import httpx
from fastapi import HTTPException, status

logger = logging.getLogger(__name__)

MAINLAYER_BASE_URL = os.getenv("MAINLAYER_BASE_URL", "https://api.mainlayer.fr")
MAINLAYER_API_KEY = os.getenv("MAINLAYER_API_KEY", "")
_TIMEOUT = 10.0


def _auth_headers() -> dict[str, str]:
    return {
        "Authorization": f"Bearer {MAINLAYER_API_KEY}",
        "Content-Type": "application/json",
        "User-Agent": "dataset-marketplace/1.0",
    }


async def charge_download(
    resource_id: str,
    amount_usd: float,
    buyer_token: str,
    dataset_id: str,
) -> str:
    """
    Charge the buyer for a full dataset download.

    Returns the Mainlayer payment ID on success.
    Raises HTTP 402 if payment fails.
    """
    if not MAINLAYER_API_KEY:
        logger.warning("MAINLAYER_API_KEY not set — skipping payment in dev mode")
        return "dev-payment-id"

    payload = {
        "resource_id": resource_id,
        "amount": amount_usd,
        "currency": "usd",
        "metadata": {"dataset_id": dataset_id, "type": "full_download"},
    }
    headers = {**_auth_headers(), "X-Buyer-Token": buyer_token}

    async with httpx.AsyncClient(timeout=_TIMEOUT) as client:
        try:
            resp = await client.post(
                f"{MAINLAYER_BASE_URL}/payments",
                json=payload,
                headers=headers,
            )
            resp.raise_for_status()
        except httpx.HTTPStatusError as exc:
            raise HTTPException(
                status_code=status.HTTP_402_PAYMENT_REQUIRED,
                detail=f"Payment failed: {exc.response.text}",
            ) from exc
        except httpx.RequestError as exc:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Could not reach payment API.",
            ) from exc

    return resp.json().get("id", "unknown")


async def charge_query(
    resource_id: str,
    price_per_query: float,
    buyer_token: str,
    dataset_id: str,
) -> str:
    """
    Charge the buyer for a single dataset query.

    Returns the Mainlayer payment ID on success.
    """
    return await charge_download(
        resource_id=resource_id,
        amount_usd=price_per_query,
        buyer_token=buyer_token,
        dataset_id=dataset_id,
    )


async def register_resource(dataset_id: str, name: str, price_full: float) -> str:
    """
    Register a dataset as a Mainlayer resource.

    Returns the new resource ID.
    """
    if not MAINLAYER_API_KEY:
        return f"res_{dataset_id}"

    payload = {
        "name": name,
        "description": f"Dataset: {name}",
        "price": price_full,
        "currency": "usd",
        "metadata": {"dataset_id": dataset_id},
    }

    async with httpx.AsyncClient(timeout=_TIMEOUT) as client:
        try:
            resp = await client.post(
                f"{MAINLAYER_BASE_URL}/resources",
                json=payload,
                headers=_auth_headers(),
            )
            resp.raise_for_status()
            return resp.json().get("id", f"res_{dataset_id}")
        except httpx.RequestError:
            return f"res_{dataset_id}"
