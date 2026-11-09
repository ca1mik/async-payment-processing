from datetime import datetime
from decimal import Decimal
from typing import Any
from uuid import UUID

from sqlalchemy import (
    CheckConstraint,
    ForeignKey,
    Index,
    Numeric,
    String,
    UniqueConstraint,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy.types import DateTime

from app.config import settings


class Base(DeclarativeBase):
    pass


class Payment(Base):
    __tablename__ = "payments"
    __table_args__ = (
        CheckConstraint("amount > 0", name="payment_positive_amount"),
        CheckConstraint("currency IN ('RUB', 'USD', 'EUR')", name="payment_currency"),
        CheckConstraint("status IN ('pending', 'succeeded', 'failed')", name="payment_status"),
        CheckConstraint(
            "webhook_state IN ('pending', 'delivered', 'dead_lettered')",
            name="payment_webhook_state",
        ),
        CheckConstraint("attempt BETWEEN 1 AND 3", name="payment_attempt"),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True)
    amount: Mapped[Decimal] = mapped_column(Numeric(14, 2))
    currency: Mapped[str] = mapped_column(String(3))
    description: Mapped[str] = mapped_column(String(500))
    payment_metadata: Mapped[dict[str, Any]] = mapped_column("metadata", JSONB)
    status: Mapped[str] = mapped_column(String(16), server_default="pending")
    idempotency_key: Mapped[str] = mapped_column(String(128), unique=True)
    request_hash: Mapped[str] = mapped_column(String(64))
    webhook_url: Mapped[str] = mapped_column(String(2048))
    webhook_state: Mapped[str] = mapped_column(String(16), server_default="pending")
    attempt: Mapped[int] = mapped_column(server_default="1")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    processed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class Outbox(Base):
    __tablename__ = "outbox"
    __table_args__ = (
        UniqueConstraint("payment_id", "topic", "attempt", name="outbox_unique_job"),
        Index(
            "outbox_pending", "available_at", "id", postgresql_where=text("published_at IS NULL")
        ),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True)
    payment_id: Mapped[UUID] = mapped_column(ForeignKey("payments.id"))
    topic: Mapped[str] = mapped_column(String(64))
    attempt: Mapped[int]
    available_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    published_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


engine = create_async_engine(settings().database_url, pool_pre_ping=True)
sessions = async_sessionmaker(engine, expire_on_commit=False)
