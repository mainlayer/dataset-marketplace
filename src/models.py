"""
Pydantic models for the Dataset Marketplace.
"""

from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field, validator


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------


class PricingModel(str, Enum):
    one_time = "one_time"
    pay_per_call = "pay_per_call"


class DatasetCategory(str, Enum):
    finance = "finance"
    healthcare = "healthcare"
    ecommerce = "ecommerce"
    social = "social"
    geospatial = "geospatial"
    science = "science"
    government = "government"
    nlp = "nlp"
    computer_vision = "computer_vision"
    other = "other"


class QueryOperator(str, Enum):
    eq = "eq"
    neq = "neq"
    gt = "gt"
    gte = "gte"
    lt = "lt"
    lte = "lte"
    contains = "contains"
    startswith = "startswith"


# ---------------------------------------------------------------------------
# Dataset models
# ---------------------------------------------------------------------------


class ColumnSchema(BaseModel):
    name: str = Field(..., description="Column name")
    type: str = Field(..., description="Data type: string, integer, float, boolean, datetime")
    description: Optional[str] = Field(None, description="Human-readable description of the column")
    nullable: bool = Field(True, description="Whether the column can contain null values")


class DatasetPublishRequest(BaseModel):
    name: str = Field(..., min_length=3, max_length=120, description="Dataset display name")
    description: str = Field(..., min_length=10, max_length=2000, description="Detailed description")
    category: DatasetCategory = Field(..., description="Dataset category")
    schema: List[ColumnSchema] = Field(..., min_items=1, description="Column definitions")
    sample_rows: List[Dict[str, Any]] = Field(
        ..., min_items=1, max_items=10, description="Preview rows (max 10)"
    )
    price_full: float = Field(..., gt=0, description="One-time price for full dataset download (USD)")
    price_per_query: float = Field(
        0.002, gt=0, description="Price per SQL query (USD), default $0.002"
    )
    tags: Optional[List[str]] = Field(default_factory=list, description="Searchable tags")
    row_count: Optional[int] = Field(None, description="Total number of rows in the dataset")
    size_bytes: Optional[int] = Field(None, description="Approximate size in bytes")


class DatasetSummary(BaseModel):
    id: str
    name: str
    description: str
    category: DatasetCategory
    tags: List[str]
    price_full: float
    price_per_query: float
    row_count: Optional[int]
    size_bytes: Optional[int]
    column_count: int
    created_at: datetime
    mainlayer_resource_id: Optional[str] = None


class DatasetDetail(DatasetSummary):
    schema: List[ColumnSchema]
    sample_rows: List[Dict[str, Any]]


class DatasetPublishResponse(BaseModel):
    id: str
    mainlayer_resource_id: str
    message: str
    dataset: DatasetDetail


# ---------------------------------------------------------------------------
# Query models
# ---------------------------------------------------------------------------


class FilterCondition(BaseModel):
    column: str = Field(..., description="Column to filter on")
    operator: QueryOperator = Field(..., description="Comparison operator")
    value: Any = Field(..., description="Value to compare against")


class QueryRequest(BaseModel):
    select: Optional[List[str]] = Field(
        None, description="Columns to return; omit for all columns"
    )
    filters: Optional[List[FilterCondition]] = Field(
        default_factory=list, description="Filter conditions (AND-joined)"
    )
    order_by: Optional[str] = Field(None, description="Column name to sort by")
    order_dir: Optional[str] = Field("asc", description="Sort direction: asc or desc")
    limit: Optional[int] = Field(100, ge=1, le=10000, description="Maximum rows to return")
    offset: Optional[int] = Field(0, ge=0, description="Row offset for pagination")

    @validator("order_dir")
    def validate_order_dir(cls, v: Optional[str]) -> Optional[str]:
        if v is not None and v not in ("asc", "desc"):
            raise ValueError("order_dir must be 'asc' or 'desc'")
        return v


class QueryResponse(BaseModel):
    dataset_id: str
    columns: List[str]
    rows: List[Dict[str, Any]]
    total_matched: int
    returned: int
    offset: int
    query_cost_usd: float
    payment_id: str


# ---------------------------------------------------------------------------
# Payment / Mainlayer models
# ---------------------------------------------------------------------------


class PaymentStatus(str, Enum):
    pending = "pending"
    completed = "completed"
    failed = "failed"
    refunded = "refunded"


class PaymentRecord(BaseModel):
    payment_id: str
    dataset_id: str
    pricing_model: PricingModel
    amount_usd: float
    status: PaymentStatus
    mainlayer_transaction_id: Optional[str] = None
    created_at: datetime
    api_key_hash: str  # hashed for storage, never the raw key


# ---------------------------------------------------------------------------
# Generic responses
# ---------------------------------------------------------------------------


class ErrorResponse(BaseModel):
    error: str
    detail: Optional[str] = None
    status_code: int


class HealthResponse(BaseModel):
    status: str
    version: str
    datasets_count: int
