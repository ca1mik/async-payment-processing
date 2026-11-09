from datetime import datetime
from decimal import Decimal
from typing import Annotated, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, HttpUrl, JsonValue


class PaymentInput(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    amount: Annotated[Decimal, Field(gt=0, max_digits=14, decimal_places=2)]
    currency: Literal["RUB", "USD", "EUR"]
    description: Annotated[str, Field(min_length=1, max_length=500)]
    metadata: dict[str, JsonValue] = Field(default_factory=dict)
    webhook_url: HttpUrl


class PaymentAccepted(BaseModel):
    payment_id: UUID
    status: Literal["pending", "succeeded", "failed"]
    created_at: datetime


class PaymentDetail(PaymentAccepted):
    amount: Decimal
    currency: str
    description: str
    metadata: dict[str, JsonValue]
    webhook_url: str
    webhook_state: str
    processed_at: datetime | None


class PaymentJob(BaseModel):
    model_config = ConfigDict(extra="forbid")

    payment_id: UUID
    attempt: Annotated[int, Field(strict=True, ge=1, le=3)]
