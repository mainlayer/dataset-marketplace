"""
Example: Browse and inspect datasets.

Usage:
    python examples/list_dataset.py
"""
import httpx

BASE_URL = "http://localhost:8001"


def main() -> None:
    # List all datasets
    resp = httpx.get(f"{BASE_URL}/datasets", params={"per_page": 5})
    resp.raise_for_status()
    data = resp.json()
    print(f"Found {data['total']} datasets\n")

    for ds in data["items"]:
        print(f"[{ds['id']}] {ds['name']}")
        print(f"  Category: {ds['category']} | Rows: {ds.get('row_count', 'N/A')}")
        print(f"  Price: ${ds['price_full']} (full) / ${ds['price_per_query']} per query")
        print()

    # Get detail for first dataset
    if data["items"]:
        first_id = data["items"][0]["id"]
        detail = httpx.get(f"{BASE_URL}/datasets/{first_id}").json()
        print(f"Sample rows for '{detail['name']}':")
        for row in detail.get("sample_rows", [])[:2]:
            print(" ", row)


if __name__ == "__main__":
    main()
