"""
In-memory dataset store seeded with 8 realistic sample datasets.

In production, replace with a real database (PostgreSQL, etc.).
"""

from __future__ import annotations

import uuid
from copy import deepcopy
from datetime import datetime, timezone
from typing import Dict, List, Optional

from .models import ColumnSchema, DatasetCategory, DatasetDetail, DatasetSummary

# ---------------------------------------------------------------------------
# Seed data helpers
# ---------------------------------------------------------------------------

_NOW = datetime(2026, 1, 15, 9, 0, 0, tzinfo=timezone.utc)


def _ts(days_ago: int = 0) -> datetime:
    from datetime import timedelta
    return datetime(2026, 1, 15, 9, 0, 0, tzinfo=timezone.utc) - timedelta(days=days_ago)


# ---------------------------------------------------------------------------
# Raw dataset definitions
# ---------------------------------------------------------------------------

_SEED_DATASETS: List[Dict] = [
    # ------------------------------------------------------------------
    # 1. S&P 500 Daily OHLCV
    # ------------------------------------------------------------------
    {
        "id": "ds_sp500_ohlcv",
        "name": "S&P 500 Daily OHLCV (2010–2025)",
        "description": (
            "Daily open, high, low, close, and volume data for all S&P 500 constituents "
            "from January 2010 through December 2025. Adjusted for splits and dividends. "
            "Ideal for backtesting, risk modeling, and quantitative research."
        ),
        "category": DatasetCategory.finance,
        "tags": ["stocks", "equities", "ohlcv", "backtesting", "quantitative"],
        "schema": [
            ColumnSchema(name="ticker", type="string", description="Stock ticker symbol", nullable=False),
            ColumnSchema(name="date", type="datetime", description="Trading date (YYYY-MM-DD)", nullable=False),
            ColumnSchema(name="open", type="float", description="Opening price (USD)", nullable=False),
            ColumnSchema(name="high", type="float", description="Intraday high price (USD)", nullable=False),
            ColumnSchema(name="low", type="float", description="Intraday low price (USD)", nullable=False),
            ColumnSchema(name="close", type="float", description="Adjusted closing price (USD)", nullable=False),
            ColumnSchema(name="volume", type="integer", description="Shares traded", nullable=False),
            ColumnSchema(name="market_cap_usd", type="float", description="Market capitalization at close", nullable=True),
        ],
        "sample_rows": [
            {"ticker": "AAPL", "date": "2025-12-31", "open": 248.50, "high": 251.20, "low": 247.80, "close": 250.10, "volume": 52_341_000, "market_cap_usd": 3_810_000_000_000},
            {"ticker": "MSFT", "date": "2025-12-31", "open": 441.00, "high": 445.75, "low": 439.50, "close": 443.80, "volume": 18_920_000, "market_cap_usd": 3_296_000_000_000},
            {"ticker": "NVDA", "date": "2025-12-31", "open": 138.20, "high": 142.50, "low": 137.10, "close": 141.30, "volume": 245_810_000, "market_cap_usd": 3_458_000_000_000},
        ],
        "price_full": 49.99,
        "price_per_query": 0.002,
        "row_count": 3_876_250,
        "size_bytes": 412_000_000,
        "mainlayer_resource_id": "res_sp500_ohlcv_001",
        "created_at": _ts(180),
    },
    # ------------------------------------------------------------------
    # 2. Global Clinical Trials
    # ------------------------------------------------------------------
    {
        "id": "ds_clinical_trials",
        "name": "Global Clinical Trials Registry (2000–2025)",
        "description": (
            "Structured data from 450,000+ registered clinical trials worldwide. "
            "Includes trial phase, indication, sponsor, enrollment, status, and "
            "primary endpoints. Sourced from ClinicalTrials.gov and WHO ICTRP."
        ),
        "category": DatasetCategory.healthcare,
        "tags": ["clinical-trials", "pharma", "medical", "research", "FDA"],
        "schema": [
            ColumnSchema(name="nct_id", type="string", description="ClinicalTrials.gov identifier", nullable=False),
            ColumnSchema(name="title", type="string", description="Official study title", nullable=False),
            ColumnSchema(name="phase", type="string", description="Trial phase (Phase 1/2/3/4)", nullable=True),
            ColumnSchema(name="status", type="string", description="Recruitment status", nullable=False),
            ColumnSchema(name="sponsor", type="string", description="Lead sponsor organization", nullable=False),
            ColumnSchema(name="indication", type="string", description="Medical condition studied", nullable=False),
            ColumnSchema(name="enrollment", type="integer", description="Number of participants enrolled", nullable=True),
            ColumnSchema(name="start_date", type="datetime", description="Trial start date", nullable=True),
            ColumnSchema(name="completion_date", type="datetime", description="Actual or estimated completion", nullable=True),
            ColumnSchema(name="primary_outcome", type="string", description="Primary endpoint description", nullable=True),
        ],
        "sample_rows": [
            {"nct_id": "NCT05923847", "title": "Phase 3 Study of GLP-1 Agonist in T2DM", "phase": "Phase 3", "status": "Recruiting", "sponsor": "NovoNordisk A/S", "indication": "Type 2 Diabetes Mellitus", "enrollment": 3200, "start_date": "2024-03-01", "completion_date": "2026-09-01", "primary_outcome": "HbA1c reduction at 52 weeks"},
            {"nct_id": "NCT05801234", "title": "Immunotherapy Combo in NSCLC (PD-L1+)", "phase": "Phase 2", "status": "Active, not recruiting", "sponsor": "Merck Sharp & Dohme", "indication": "Non-Small Cell Lung Cancer", "enrollment": 480, "start_date": "2023-07-15", "completion_date": "2025-12-31", "primary_outcome": "Objective Response Rate (ORR)"},
            {"nct_id": "NCT05674521", "title": "CAR-T Cell Therapy for Relapsed DLBCL", "phase": "Phase 1/2", "status": "Completed", "sponsor": "University of Pennsylvania", "indication": "Diffuse Large B-Cell Lymphoma", "enrollment": 85, "start_date": "2021-04-01", "completion_date": "2024-10-15", "primary_outcome": "Complete remission rate at 6 months"},
        ],
        "price_full": 29.99,
        "price_per_query": 0.002,
        "row_count": 452_000,
        "size_bytes": 198_000_000,
        "mainlayer_resource_id": "res_clinical_trials_001",
        "created_at": _ts(90),
    },
    # ------------------------------------------------------------------
    # 3. Global E-Commerce Transactions
    # ------------------------------------------------------------------
    {
        "id": "ds_ecommerce_txns",
        "name": "Global E-Commerce Transactions (Synthetic, 2023–2025)",
        "description": (
            "10 million anonymized synthetic e-commerce transactions across 180 countries. "
            "Includes product category, price, payment method, device type, and session data. "
            "Synthetically generated to mirror real distributions — safe for ML training."
        ),
        "category": DatasetCategory.ecommerce,
        "tags": ["transactions", "retail", "consumer", "ML-training", "synthetic"],
        "schema": [
            ColumnSchema(name="transaction_id", type="string", description="Unique transaction UUID", nullable=False),
            ColumnSchema(name="timestamp", type="datetime", description="Transaction UTC timestamp", nullable=False),
            ColumnSchema(name="country_code", type="string", description="ISO 3166-1 alpha-2 country code", nullable=False),
            ColumnSchema(name="category", type="string", description="Product category", nullable=False),
            ColumnSchema(name="product_id", type="string", description="Internal product identifier", nullable=False),
            ColumnSchema(name="quantity", type="integer", description="Units purchased", nullable=False),
            ColumnSchema(name="unit_price_usd", type="float", description="Price per unit (USD)", nullable=False),
            ColumnSchema(name="total_usd", type="float", description="Order total (USD)", nullable=False),
            ColumnSchema(name="payment_method", type="string", description="e.g. credit_card, paypal, apple_pay", nullable=False),
            ColumnSchema(name="device_type", type="string", description="desktop, mobile, tablet", nullable=False),
            ColumnSchema(name="is_returned", type="boolean", description="Whether the order was returned", nullable=False),
        ],
        "sample_rows": [
            {"transaction_id": "txn_a1b2c3d4", "timestamp": "2025-11-28T14:32:17Z", "country_code": "US", "category": "Electronics", "product_id": "prod_8821", "quantity": 1, "unit_price_usd": 1299.99, "total_usd": 1299.99, "payment_method": "credit_card", "device_type": "desktop", "is_returned": False},
            {"transaction_id": "txn_e5f6g7h8", "timestamp": "2025-11-28T09:11:05Z", "country_code": "DE", "category": "Apparel", "product_id": "prod_3312", "quantity": 3, "unit_price_usd": 45.00, "total_usd": 135.00, "payment_method": "paypal", "device_type": "mobile", "is_returned": True},
            {"transaction_id": "txn_i9j0k1l2", "timestamp": "2025-11-27T22:45:59Z", "country_code": "JP", "category": "Home & Garden", "product_id": "prod_7740", "quantity": 2, "unit_price_usd": 89.50, "total_usd": 179.00, "payment_method": "apple_pay", "device_type": "mobile", "is_returned": False},
        ],
        "price_full": 39.99,
        "price_per_query": 0.002,
        "row_count": 10_000_000,
        "size_bytes": 2_800_000_000,
        "mainlayer_resource_id": "res_ecommerce_txns_001",
        "created_at": _ts(60),
    },
    # ------------------------------------------------------------------
    # 4. Reddit Posts NLP Corpus
    # ------------------------------------------------------------------
    {
        "id": "ds_reddit_nlp",
        "name": "Reddit Posts NLP Corpus (2022–2024)",
        "description": (
            "5 million Reddit posts from 200 subreddits, pre-cleaned and tokenized. "
            "Includes sentiment scores, topic labels, toxicity scores, and engagement metrics. "
            "Ideal for NLP fine-tuning, topic modeling, and sentiment analysis."
        ),
        "category": DatasetCategory.nlp,
        "tags": ["reddit", "NLP", "sentiment", "text", "social-media", "LLM"],
        "schema": [
            ColumnSchema(name="post_id", type="string", description="Reddit post ID", nullable=False),
            ColumnSchema(name="subreddit", type="string", description="Subreddit name (without r/)", nullable=False),
            ColumnSchema(name="created_utc", type="datetime", description="Post creation time (UTC)", nullable=False),
            ColumnSchema(name="title", type="string", description="Post title", nullable=False),
            ColumnSchema(name="selftext", type="string", description="Post body text (null for link posts)", nullable=True),
            ColumnSchema(name="score", type="integer", description="Net upvotes at time of collection", nullable=False),
            ColumnSchema(name="num_comments", type="integer", description="Comment count", nullable=False),
            ColumnSchema(name="sentiment_label", type="string", description="positive / neutral / negative", nullable=True),
            ColumnSchema(name="sentiment_score", type="float", description="Sentiment score -1.0 to 1.0", nullable=True),
            ColumnSchema(name="toxicity_score", type="float", description="Perspective API toxicity 0.0–1.0", nullable=True),
            ColumnSchema(name="topic_label", type="string", description="LDA-derived topic label", nullable=True),
        ],
        "sample_rows": [
            {"post_id": "17xk9ab", "subreddit": "MachineLearning", "created_utc": "2024-08-12T16:20:00Z", "title": "New RLHF technique reduces hallucinations by 40% on TruthfulQA", "selftext": "We release a new paper showing...", "score": 2841, "num_comments": 193, "sentiment_label": "positive", "sentiment_score": 0.78, "toxicity_score": 0.02, "topic_label": "reinforcement_learning"},
            {"post_id": "18ab3cd", "subreddit": "personalfinance", "created_utc": "2024-09-03T08:05:00Z", "title": "Is it a bad time to buy a house with 7% rates?", "selftext": "I have 20% down, good credit...", "score": 4120, "num_comments": 812, "sentiment_label": "neutral", "sentiment_score": -0.05, "toxicity_score": 0.04, "topic_label": "real_estate"},
            {"post_id": "19cd5ef", "subreddit": "worldnews", "created_utc": "2024-10-21T11:33:00Z", "title": "EU announces landmark AI liability framework", "selftext": None, "score": 9832, "num_comments": 2104, "sentiment_label": "neutral", "sentiment_score": 0.12, "toxicity_score": 0.08, "topic_label": "politics_regulation"},
        ],
        "price_full": 24.99,
        "price_per_query": 0.002,
        "row_count": 5_000_000,
        "size_bytes": 8_400_000_000,
        "mainlayer_resource_id": "res_reddit_nlp_001",
        "created_at": _ts(45),
    },
    # ------------------------------------------------------------------
    # 5. World Air Quality Index
    # ------------------------------------------------------------------
    {
        "id": "ds_air_quality",
        "name": "World Air Quality Index — Hourly (2015–2025)",
        "description": (
            "Hourly AQI readings from 12,000+ monitoring stations across 90 countries. "
            "Includes PM2.5, PM10, NO2, O3, SO2, CO measurements with geocoordinates. "
            "Great for environmental research, climate ML, and urban planning."
        ),
        "category": DatasetCategory.science,
        "tags": ["air-quality", "AQI", "pollution", "climate", "geospatial", "environment"],
        "schema": [
            ColumnSchema(name="station_id", type="string", description="Monitoring station identifier", nullable=False),
            ColumnSchema(name="station_name", type="string", description="Station common name", nullable=False),
            ColumnSchema(name="city", type="string", description="City name", nullable=False),
            ColumnSchema(name="country_code", type="string", description="ISO 3166-1 alpha-2", nullable=False),
            ColumnSchema(name="latitude", type="float", description="Station latitude", nullable=False),
            ColumnSchema(name="longitude", type="float", description="Station longitude", nullable=False),
            ColumnSchema(name="timestamp", type="datetime", description="Reading timestamp (UTC)", nullable=False),
            ColumnSchema(name="aqi", type="integer", description="Composite Air Quality Index", nullable=True),
            ColumnSchema(name="pm25", type="float", description="PM2.5 concentration µg/m³", nullable=True),
            ColumnSchema(name="pm10", type="float", description="PM10 concentration µg/m³", nullable=True),
            ColumnSchema(name="no2", type="float", description="NO₂ concentration µg/m³", nullable=True),
            ColumnSchema(name="o3", type="float", description="Ozone concentration µg/m³", nullable=True),
        ],
        "sample_rows": [
            {"station_id": "CHN-BJ-001", "station_name": "Beijing Dongcheng", "city": "Beijing", "country_code": "CN", "latitude": 39.9290, "longitude": 116.4170, "timestamp": "2025-12-31T12:00:00Z", "aqi": 87, "pm25": 28.4, "pm10": 51.2, "no2": 42.1, "o3": 18.3},
            {"station_id": "IND-DEL-004", "station_name": "Delhi IGI Airport", "city": "New Delhi", "country_code": "IN", "latitude": 28.5665, "longitude": 77.1031, "timestamp": "2025-12-31T12:00:00Z", "aqi": 214, "pm25": 120.5, "pm10": 198.3, "no2": 68.7, "o3": 8.1},
            {"station_id": "USA-LA-012", "station_name": "Los Angeles Downtown", "city": "Los Angeles", "country_code": "US", "latitude": 34.0522, "longitude": -118.2437, "timestamp": "2025-12-31T12:00:00Z", "aqi": 52, "pm25": 11.2, "pm10": 22.8, "no2": 31.4, "o3": 55.6},
        ],
        "price_full": 34.99,
        "price_per_query": 0.002,
        "row_count": 1_051_200_000,
        "size_bytes": 185_000_000_000,
        "mainlayer_resource_id": "res_air_quality_001",
        "created_at": _ts(30),
    },
    # ------------------------------------------------------------------
    # 6. US Patent Grants (1976–2025)
    # ------------------------------------------------------------------
    {
        "id": "ds_us_patents",
        "name": "US Patent Grants — Full Text (1976–2025)",
        "description": (
            "Structured metadata and full claim text for 8.2 million US patents granted by the USPTO "
            "from 1976 to 2025. Includes CPC classification codes, inventor data, assignees, "
            "citation graphs, and abstract embeddings (768-dim)."
        ),
        "category": DatasetCategory.government,
        "tags": ["patents", "USPTO", "IP", "innovation", "legal", "R&D"],
        "schema": [
            ColumnSchema(name="patent_number", type="string", description="USPTO patent number", nullable=False),
            ColumnSchema(name="title", type="string", description="Patent title", nullable=False),
            ColumnSchema(name="abstract", type="string", description="Patent abstract", nullable=True),
            ColumnSchema(name="grant_date", type="datetime", description="Grant date", nullable=False),
            ColumnSchema(name="filing_date", type="datetime", description="Original filing date", nullable=False),
            ColumnSchema(name="assignee", type="string", description="Primary assignee (company/individual)", nullable=True),
            ColumnSchema(name="inventors", type="string", description="Comma-separated inventor names", nullable=True),
            ColumnSchema(name="cpc_codes", type="string", description="Comma-separated CPC classification codes", nullable=True),
            ColumnSchema(name="citation_count", type="integer", description="Forward citation count", nullable=False),
            ColumnSchema(name="claim_count", type="integer", description="Number of claims", nullable=False),
        ],
        "sample_rows": [
            {"patent_number": "US11982456B2", "title": "Transformer-based language model with sparse attention", "abstract": "A neural language model architecture incorporating sparse attention mechanisms...", "grant_date": "2025-06-17", "filing_date": "2023-02-28", "assignee": "Google LLC", "inventors": "John A. Smith, Sarah B. Lee", "cpc_codes": "G06N3/0455, G06F40/30", "citation_count": 42, "claim_count": 18},
            {"patent_number": "US11876543B1", "title": "Solid-state battery with ceramic electrolyte interface", "abstract": "Methods and compositions for fabricating solid-state lithium batteries...", "grant_date": "2025-01-07", "filing_date": "2022-09-14", "assignee": "QuantumScape Corporation", "inventors": "Maria C. Garcia, Wei Zhang", "cpc_codes": "H01M10/0562, H01M10/058", "citation_count": 17, "claim_count": 24},
            {"patent_number": "US11934217B2", "title": "mRNA delivery system using ionizable lipid nanoparticles", "abstract": "Lipid nanoparticle formulations for delivery of mRNA therapeutics...", "grant_date": "2025-03-25", "filing_date": "2021-11-30", "assignee": "Moderna Inc.", "inventors": "Aisha M. Patel, Robert K. Johnson", "cpc_codes": "A61K31/7105, A61K9/127", "citation_count": 89, "claim_count": 31},
        ],
        "price_full": 44.99,
        "price_per_query": 0.002,
        "row_count": 8_200_000,
        "size_bytes": 94_000_000_000,
        "mainlayer_resource_id": "res_us_patents_001",
        "created_at": _ts(20),
    },
    # ------------------------------------------------------------------
    # 7. Worldwide Restaurant Reviews
    # ------------------------------------------------------------------
    {
        "id": "ds_restaurant_reviews",
        "name": "Worldwide Restaurant Reviews (Synthetic, 50M reviews)",
        "description": (
            "50 million synthetic restaurant reviews across 120 countries with star ratings, "
            "review text, cuisine type, price tier, and geolocation. "
            "Synthetically generated with realistic distributions for safe commercial use."
        ),
        "category": DatasetCategory.social,
        "tags": ["restaurants", "reviews", "NLP", "hospitality", "geospatial", "synthetic"],
        "schema": [
            ColumnSchema(name="review_id", type="string", description="Unique review identifier", nullable=False),
            ColumnSchema(name="restaurant_id", type="string", description="Restaurant identifier", nullable=False),
            ColumnSchema(name="restaurant_name", type="string", description="Restaurant name", nullable=False),
            ColumnSchema(name="cuisine", type="string", description="Primary cuisine type", nullable=True),
            ColumnSchema(name="price_tier", type="integer", description="1=budget, 2=mid, 3=upscale, 4=fine dining", nullable=True),
            ColumnSchema(name="city", type="string", description="City", nullable=False),
            ColumnSchema(name="country_code", type="string", description="ISO country code", nullable=False),
            ColumnSchema(name="rating", type="float", description="Star rating 1.0–5.0", nullable=False),
            ColumnSchema(name="review_text", type="string", description="Review body", nullable=True),
            ColumnSchema(name="visit_date", type="datetime", description="Date of visit", nullable=True),
            ColumnSchema(name="helpful_votes", type="integer", description="Count of 'helpful' votes", nullable=False),
        ],
        "sample_rows": [
            {"review_id": "rev_001a", "restaurant_id": "rest_xy912", "restaurant_name": "Sakura Sushi Bar", "cuisine": "Japanese", "price_tier": 3, "city": "San Francisco", "country_code": "US", "rating": 4.7, "review_text": "Exceptional omakase experience. The toro was melt-in-your-mouth perfect.", "visit_date": "2025-10-14", "helpful_votes": 23},
            {"review_id": "rev_002b", "restaurant_id": "rest_ab423", "restaurant_name": "Le Petit Bistro", "cuisine": "French", "price_tier": 2, "city": "Lyon", "country_code": "FR", "rating": 3.9, "review_text": "Solid boeuf bourguignon, service was a bit slow on a busy Saturday.", "visit_date": "2025-09-22", "helpful_votes": 8},
            {"review_id": "rev_003c", "restaurant_id": "rest_cd881", "restaurant_name": "Taco Fuego", "cuisine": "Mexican", "price_tier": 1, "city": "Mexico City", "country_code": "MX", "rating": 4.9, "review_text": "Best al pastor tacos I've ever had. Street food heaven.", "visit_date": "2025-11-05", "helpful_votes": 41},
        ],
        "price_full": 19.99,
        "price_per_query": 0.002,
        "row_count": 50_000_000,
        "size_bytes": 38_000_000_000,
        "mainlayer_resource_id": "res_restaurant_reviews_001",
        "created_at": _ts(15),
    },
    # ------------------------------------------------------------------
    # 8. Global Real Estate Listings
    # ------------------------------------------------------------------
    {
        "id": "ds_real_estate",
        "name": "Global Real Estate Listings (2020–2025)",
        "description": (
            "3.5 million residential real estate listings from 40 major metro areas across "
            "North America, Europe, Asia, and Australia. Includes price, size, bedrooms, "
            "amenities, energy rating, and days-on-market history."
        ),
        "category": DatasetCategory.ecommerce,
        "tags": ["real-estate", "housing", "property", "pricing", "geospatial"],
        "schema": [
            ColumnSchema(name="listing_id", type="string", description="Unique listing identifier", nullable=False),
            ColumnSchema(name="city", type="string", description="City", nullable=False),
            ColumnSchema(name="country_code", type="string", description="ISO country code", nullable=False),
            ColumnSchema(name="latitude", type="float", description="Property latitude", nullable=True),
            ColumnSchema(name="longitude", type="float", description="Property longitude", nullable=True),
            ColumnSchema(name="property_type", type="string", description="apartment, house, condo, townhouse", nullable=False),
            ColumnSchema(name="bedrooms", type="integer", description="Number of bedrooms", nullable=True),
            ColumnSchema(name="bathrooms", type="float", description="Number of bathrooms", nullable=True),
            ColumnSchema(name="sqft", type="integer", description="Interior area in square feet", nullable=True),
            ColumnSchema(name="list_price_usd", type="float", description="Listing price in USD", nullable=False),
            ColumnSchema(name="price_per_sqft_usd", type="float", description="Price per sq ft", nullable=True),
            ColumnSchema(name="energy_rating", type="string", description="Energy efficiency rating A–G", nullable=True),
            ColumnSchema(name="days_on_market", type="integer", description="Days listed before sale/removal", nullable=True),
            ColumnSchema(name="list_date", type="datetime", description="Date listed", nullable=False),
        ],
        "sample_rows": [
            {"listing_id": "lst_sf_00123", "city": "San Francisco", "country_code": "US", "latitude": 37.7749, "longitude": -122.4194, "property_type": "condo", "bedrooms": 2, "bathrooms": 2.0, "sqft": 1050, "list_price_usd": 1_250_000, "price_per_sqft_usd": 1190.48, "energy_rating": "B", "days_on_market": 12, "list_date": "2025-08-01"},
            {"listing_id": "lst_ldn_00987", "city": "London", "country_code": "GB", "latitude": 51.5074, "longitude": -0.1278, "property_type": "apartment", "bedrooms": 1, "bathrooms": 1.0, "sqft": 650, "list_price_usd": 820_000, "price_per_sqft_usd": 1261.54, "energy_rating": "C", "days_on_market": 28, "list_date": "2025-07-15"},
            {"listing_id": "lst_tok_04421", "city": "Tokyo", "country_code": "JP", "latitude": 35.6762, "longitude": 139.6503, "property_type": "apartment", "bedrooms": 3, "bathrooms": 1.0, "sqft": 900, "list_price_usd": 680_000, "price_per_sqft_usd": 755.56, "energy_rating": "A", "days_on_market": 5, "list_date": "2025-11-20"},
        ],
        "price_full": 29.99,
        "price_per_query": 0.002,
        "row_count": 3_500_000,
        "size_bytes": 4_200_000_000,
        "mainlayer_resource_id": "res_real_estate_001",
        "created_at": _ts(5),
    },
]


# ---------------------------------------------------------------------------
# In-memory store
# ---------------------------------------------------------------------------


class DatasetStore:
    """Thread-safe (GIL-protected) in-memory dataset registry."""

    def __init__(self) -> None:
        self._store: Dict[str, Dict] = {}
        self._seed()

    def _seed(self) -> None:
        for raw in _SEED_DATASETS:
            self._store[raw["id"]] = deepcopy(raw)

    # ------------------------------------------------------------------
    # CRUD
    # ------------------------------------------------------------------

    def add(self, data: Dict) -> str:
        ds_id = data.get("id") or f"ds_{uuid.uuid4().hex[:12]}"
        data["id"] = ds_id
        if "created_at" not in data or data["created_at"] is None:
            data["created_at"] = datetime.now(timezone.utc)
        self._store[ds_id] = data
        return ds_id

    def get(self, ds_id: str) -> Optional[Dict]:
        return self._store.get(ds_id)

    def list_all(
        self,
        category: Optional[DatasetCategory] = None,
        tag: Optional[str] = None,
        search: Optional[str] = None,
    ) -> List[Dict]:
        results = list(self._store.values())

        if category is not None:
            results = [r for r in results if r.get("category") == category]

        if tag is not None:
            results = [r for r in results if tag.lower() in [t.lower() for t in r.get("tags", [])]]

        if search is not None:
            kw = search.lower()
            results = [
                r for r in results
                if kw in r["name"].lower() or kw in r["description"].lower()
            ]

        return results

    def count(self) -> int:
        return len(self._store)

    def categories(self) -> Dict[str, int]:
        counts: Dict[str, int] = {}
        for ds in self._store.values():
            cat = ds.get("category", "other")
            if isinstance(cat, DatasetCategory):
                cat = cat.value
            counts[cat] = counts.get(cat, 0) + 1
        return counts


# Singleton instance used by the application
dataset_store = DatasetStore()
