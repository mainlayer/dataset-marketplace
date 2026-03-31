"""
Example: Purchase and download a dataset.

Usage:
    MAINLAYER_TOKEN=<your-token> python examples/buy_dataset.py ds_sp500_ohlcv
"""
import os
import sys

import httpx

BASE_URL = "http://localhost:8001"
TOKEN = os.getenv("MAINLAYER_TOKEN", "demo-token")


def main(dataset_id: str) -> None:
    # 1. Inspect the dataset first (free)
    resp = httpx.get(f"{BASE_URL}/datasets/{dataset_id}")
    if resp.status_code == 404:
        print(f"Dataset '{dataset_id}' not found.")
        return
    resp.raise_for_status()
    ds = resp.json()
    print(f"Dataset: {ds['name']}")
    print(f"Price: ${ds['price_full']}")
    print(f"Rows: {ds.get('row_count', 'N/A')}")

    # 2. Download (Mainlayer payment required)
    print("\nInitiating purchase...")
    resp = httpx.get(
        f"{BASE_URL}/datasets/{dataset_id}/download",
        headers={"x-mainlayer-token": TOKEN},
    )
    if resp.status_code == 402:
        print("Payment required. Fund your Mainlayer account at https://mainlayer.fr")
        return
    resp.raise_for_status()
    result = resp.json()

    print(f"Payment ID: {result['payment_id']}")
    print(f"Download URL: {result['download_url']}")
    print(f"Link expires in: {result['expires_in_seconds']}s")


if __name__ == "__main__":
    ds = sys.argv[1] if len(sys.argv) > 1 else "ds_sp500_ohlcv"
    main(ds)
