import hashlib
import json
from contextlib import asynccontextmanager
from decimal import Decimal
from secrets import compare_digest
from typing import Annotated
from uuid import UUID, uuid4

from fastapi import APIRouter, Depends, FastAPI, Header, HTTPException
from fastapi.security import APIKeyHeader
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert

from app.config import settings
from app.db import Outbox, Payment, engine, sessions
from app.schemas import PaymentAccepted, PaymentDetail, PaymentInput

api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)


def authenticate(key: Annotated[str | None, Depends(api_key_header)]) -> None:
    expected = settings().api_key.get_secret_value().encode()
    if not key or not compare_digest(key.encode(), expected):
        raise HTTPException(401, "Invalid API key", headers={"WWW-Authenticate": "APIKey"})


def canonical_request(body: PaymentInput) -> dict:
    data = body.model_dump(mode="json")
    data["amount"] = format(body.amount.quantize(Decimal("0.01")), "f")
    return data


def accepted(payment: Payment) -> PaymentAccepted:
    return PaymentAccepted(
        payment_id=payment.id, status=payment.status, created_at=payment.created_at
    )


router = APIRouter(prefix="/api/v1/payments", dependencies=[Depends(authenticate)])


@router.post(
    "",
    status_code=202,
    response_model=PaymentAccepted,
    responses={409: {"description": "Key conflict"}},
)
async def create_payment(
    body: PaymentInput,
    key: Annotated[
        str, Header(alias="Idempotency-Key", min_length=1, max_length=128, pattern=r"^[!-~]+$")
    ],
) -> PaymentAccepted:
    if body.webhook_url.username is not None or body.webhook_url.password is not None:
        raise HTTPException(422, "Webhook URL must not contain credentials")
    if not settings().permits_webhook(body.webhook_url.host):
        raise HTTPException(422, "Webhook host is not allowed")
    data = canonical_request(body)
    try:
        encoded = json.dumps(data, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
    except ValueError as exc:
        raise HTTPException(422, "Metadata must be valid JSON") from exc
    fingerprint = hashlib.sha256(encoded).hexdigest()
    async with sessions.begin() as db:
        statement = (
            insert(Payment)
            .values(
                id=uuid4(),
                amount=body.amount,
                currency=body.currency,
                description=body.description,
                payment_metadata=body.metadata,
                webhook_url=str(body.webhook_url),
                idempotency_key=key,
                request_hash=fingerprint,
            )
            .on_conflict_do_nothing(index_elements=[Payment.idempotency_key])
            .returning(Payment)
        )
        payment = (await db.execute(statement)).scalar_one_or_none()
        if payment is None:
            payment = (
                await db.execute(select(Payment).where(Payment.idempotency_key == key))
            ).scalar_one()
            if payment.request_hash != fingerprint:
                raise HTTPException(409, "Idempotency-Key was used with a different request")
        else:
            db.add(Outbox(id=uuid4(), payment_id=payment.id, topic="payments.new", attempt=1))
        return accepted(payment)


@router.get("/{payment_id}", response_model=PaymentDetail)
async def get_payment(payment_id: UUID) -> PaymentDetail:
    async with sessions() as db:
        payment = await db.get(Payment, payment_id)
        if payment is None:
            raise HTTPException(404, "Payment not found")
        return PaymentDetail(
            **accepted(payment).model_dump(),
            amount=payment.amount,
            currency=payment.currency,
            description=payment.description,
            metadata=payment.payment_metadata,
            webhook_url=payment.webhook_url,
            webhook_state=payment.webhook_state,
            processed_at=payment.processed_at,
        )


@asynccontextmanager
async def lifespan(app: FastAPI):
    yield
    await engine.dispose()


app = FastAPI(
    title="Payment processing", docs_url=None, redoc_url=None, openapi_url=None, lifespan=lifespan
)
app.include_router(router)
