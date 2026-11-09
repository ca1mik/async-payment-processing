import asyncio
import logging
import random
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import httpx
from faststream import AckPolicy
from faststream.rabbit import Channel, RabbitBroker, RabbitQueue
from faststream.rabbit.annotations import RabbitMessage
from faststream.rabbit.message import RabbitMessage as IncomingRabbitMessage
from pamqp.commands import Basic
from pydantic import HttpUrl, ValidationError
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.db import Outbox, Payment, engine, sessions
from app.schemas import PaymentJob

log = logging.getLogger(__name__)
NEW = RabbitQueue(
    "payments.new",
    durable=True,
    arguments={"x-dead-letter-exchange": "", "x-dead-letter-routing-key": "payments.dlq"},
)
DEAD = RabbitQueue("payments.dlq", durable=True)
broker = RabbitBroker(
    settings().rabbitmq_url.get_secret_value(),
    default_channel=Channel(prefetch_count=1, publisher_confirms=True, on_return_raises=True),
    graceful_timeout=15,
)


def schedule_failure(db: AsyncSession, payment: Payment) -> None:
    now = datetime.now(UTC)
    if payment.attempt == 3:
        payment.webhook_state = "dead_lettered"
        db.add(Outbox(id=uuid4(), payment_id=payment.id, topic=DEAD.name, attempt=3))
        return
    delay = settings().retry_base_seconds * 2 ** (payment.attempt - 1)
    payment.attempt += 1
    db.add(
        Outbox(
            id=uuid4(),
            payment_id=payment.id,
            topic=NEW.name,
            attempt=payment.attempt,
            available_at=now + timedelta(seconds=delay),
        )
    )


def current_attempt(payment: Payment, job: PaymentJob) -> bool:
    return payment.attempt == job.attempt and payment.webhook_state == "pending"


async def simulate_gateway() -> str:
    await asyncio.sleep(random.uniform(2, 5))
    return "succeeded" if random.random() < 0.9 else "failed"


async def send_webhook(payment: Payment) -> None:
    url = HttpUrl(payment.webhook_url)
    if not settings().permits_webhook(url.host):
        raise httpx.InvalidURL("Webhook host is not allowed")
    event_id = f"payment-result-{payment.id}"
    payload = {
        "event_id": event_id,
        "payment_id": str(payment.id),
        "status": payment.status,
        "amount": str(payment.amount),
        "currency": payment.currency,
        "metadata": payment.payment_metadata,
        "processed_at": payment.processed_at.isoformat(),
    }
    async with httpx.AsyncClient(
        timeout=settings().webhook_timeout, follow_redirects=False, trust_env=False
    ) as client:
        response = await client.post(
            str(url),
            json=payload,
            headers={"Idempotency-Key": event_id},
        )
        response.raise_for_status()


async def process(job: PaymentJob) -> bool:
    # Commit the gateway result before notifying anyone. A webhook retry
    # or a redelivered job must never run the gateway after this commit.
    async with sessions.begin() as db:
        payment = await db.get(Payment, job.payment_id, with_for_update=True)
        if payment is None:
            return False
        if not current_attempt(payment, job):
            return True
        if payment.status == "pending":
            payment.status = await simulate_gateway()
            payment.processed_at = datetime.now(UTC)

    async with sessions.begin() as db:
        payment = await db.get(Payment, job.payment_id, with_for_update=True)
        if payment is None:
            return False
        if not current_attempt(payment, job):
            return True
        try:
            await send_webhook(payment)
        except (httpx.HTTPError, httpx.InvalidURL) as exc:
            log.warning(
                "Webhook attempt %s failed for payment %s (%s)",
                job.attempt,
                job.payment_id,
                type(exc).__name__,
            )
            schedule_failure(db, payment)
        else:
            payment.webhook_state = "delivered"
    return True


def raw_body(message: IncomingRabbitMessage) -> bytes:
    return message.raw_message.body


@broker.subscriber(NEW, ack_policy=AckPolicy.MANUAL, decoder=raw_body, no_reply=True)
async def consume(body: bytes, message: RabbitMessage) -> None:
    try:
        job = PaymentJob.model_validate_json(body)
    except ValidationError:
        log.warning("Malformed payment message")
        await message.reject()
        return
    try:
        handled = await process(job)
    except Exception:
        log.exception("Payment job will be redelivered: %s", job.payment_id)
        await asyncio.sleep(1)
        await message.nack(requeue=True)
        return
    if handled:
        await message.ack()
    else:
        await message.reject()


async def publish_one() -> bool:
    async with sessions.begin() as db:
        event = (
            await db.scalars(
                select(Outbox)
                .where(Outbox.published_at.is_(None), Outbox.available_at <= func.now())
                .order_by(Outbox.available_at, Outbox.id)
                .with_for_update(skip_locked=True)
                .limit(1)
            )
        ).first()
        if event is None:
            return False
        body = {"payment_id": str(event.payment_id), "attempt": event.attempt}
        if event.topic == DEAD.name:
            body["reason"] = "webhook_attempts_exhausted"
        confirmation = await broker.publish(
            body,
            routing_key=event.topic,
            message_id=str(event.id),
            persist=True,
            mandatory=True,
            timeout=5,
        )
        if type(confirmation) is not Basic.Ack:
            raise RuntimeError("RabbitMQ did not confirm the publication")
        event.published_at = datetime.now(UTC)
    return True


async def run() -> None:
    try:
        async with broker:
            await broker.declare_queue(DEAD)
            await broker.start()
            while True:
                try:
                    if await publish_one():
                        continue
                except Exception:
                    log.exception("Outbox publication will be retried")
                await asyncio.sleep(settings().outbox_poll_interval)
    finally:
        await engine.dispose()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    logging.getLogger("httpx").setLevel(logging.WARNING)
    asyncio.run(run())
