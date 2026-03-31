# dataset-marketplace
![CI](https://github.com/mainlayer/dataset-marketplace/actions/workflows/ci.yml/badge.svg) ![License](https://img.shields.io/badge/license-MIT-blue)

A platform where anyone can list datasets and sell access to AI agents. Browse for free, pay with Mainlayer to download.

## Install

```bash
pip install mainlayer fastapi uvicorn httpx
```

## Quickstart

```python
import httpx

# Browse datasets — no auth needed
datasets = httpx.get("http://localhost:8001/datasets").json()
for ds in datasets["items"]:
    print(ds["name"], "$" + str(ds["price_full"]))

# Download a dataset — requires Mainlayer token
resp = httpx.get(
    "http://localhost:8001/datasets/ds_sp500_ohlcv/download",
    headers={"x-mainlayer-token": "your-token"},
)
print(resp.json()["download_url"])
```

## Features

- Pre-seeded with 8 realistic datasets (finance, NLP, healthcare, e-commerce, etc.)
- Free browsing with sample rows; gated download via Mainlayer payment
- Publish your own datasets and earn revenue
- Category and full-text search

## Run locally

```bash
MAINLAYER_API_KEY=... uvicorn src.main:app --port 8001 --reload
```

📚 [mainlayer.fr](https://mainlayer.fr)
