"""Run with a fresh Compose project using the demo webhook profile."""

import asyncio
import json
import sys
from uuid import UUID

import aio_pika
import httpx
from sqlalchemy import select

from app.config import settings
from app.db import Outbox, Payment, engine, sessions

ROOT = "/api/v1/payments"
SCENARIOS = {"ok": "delivered", "retry": "delivered", "fail": "dead_lettered"}
BASE_BODY = {"amount": "120.50", "currency": "RUB", "description": "Compose smoke"}


def body(scenario: str) -> dict:
    return {**BASE_BODY, "webhook_url": f"http://webhook:8000/hook/{scenario}"}


async def seed(client: httpx.AsyncClient) -> None:
    for scenario in SCENARIOS:
        headers = {"Idempotency-Key": f"smoke-{scenario}"}
        requests = await asyncio.gather(
            *(client.post(ROOT, json=body(scenario), headers=headers) for _ in range(5))
        )
        assert all(result.status_code == 202 for result in requests), [r.text for r in requests]
        assert len({result.json()["payment_id"] for result in requests}) == 1
        assert all(result.json()["status"] == "pending" for result in requests)
        conflict = await client.post(
            ROOT, json={**body(scenario), "amount": "121"}, headers=headers
        )
        assert conflict.status_code == 409, conflict.text

    async with sessions() as db:
        events = (await db.scalars(select(Outbox))).all()
        assert len(events) == 3
        assert all(event.attempt == 1 and event.published_at is None for event in events)
    print("PASS: concurrent idempotency and committed outbox with worker offline")


async def verify(client: httpx.AsyncClient) -> None:
    async with sessions() as db:
        rows = (await db.scalars(select(Payment))).all()
        ids = {row.idempotency_key.removeprefix("smoke-"): row.id for row in rows}

    async with httpx.AsyncClient(base_url="http://webhook:8000") as callback:
        for _ in range(120):
            states = {}
            for scenario, payment_id in ids.items():
                response = await client.get(f"{ROOT}/{payment_id}")
                response.raise_for_status()
                states[scenario] = response.json()["webhook_state"]
            if states == SCENARIOS:
                break
            await asyncio.sleep(0.5)
        else:
            raise AssertionError(f"Worker did not finish: {states}")

        for scenario, expected_count in {"ok": 1, "retry": 3, "fail": 3}.items():
            response = await callback.get(f"/events/{scenario}")
            response.raise_for_status()
            calls = response.json()
            assert len(calls) == expected_count, calls
            assert len({call["key"] for call in calls}) == 1, calls
            assert all(call["data"] == calls[0]["data"] for call in calls), calls
            assert calls[0]["data"]["status"] in {"succeeded", "failed"}, calls
            assert calls[0]["data"]["amount"] == "120.50", calls
            if expected_count == 3:
                times = [call["time"] for call in calls]
                assert times[1] - times[0] >= 0.9 and times[2] - times[1] >= 1.9, times

    connection = await aio_pika.connect(settings().rabbitmq_url.get_secret_value())
    async with connection:
        channel = await connection.channel()
        queue = await channel.get_queue("payments.dlq", ensure=True)
        for _ in range(30):
            delivery = await queue.get(fail=False)
            if delivery:
                break
            await asyncio.sleep(0.5)
        else:
            raise AssertionError("Missing dead letter")
        async with delivery.process():
            assert json.loads(delivery.body) == {
                "payment_id": str(ids["fail"]),
                "attempt": 3,
                "reason": "webhook_attempts_exhausted",
            }
        await channel.default_exchange.publish(
            aio_pika.Message(body=b"{bad JSON", content_type="application/json"),
            routing_key="payments.new",
        )
        for _ in range(30):
            delivery = await queue.get(fail=False)
            if delivery:
                break
            await asyncio.sleep(0.5)
        else:
            raise AssertionError("Invalid RabbitMQ message was not dead-lettered")
        async with delivery.process():
            assert delivery.body == b"{bad JSON"

    async with sessions() as db:
        events = (await db.scalars(select(Outbox).where(Outbox.payment_id == ids["fail"]))).all()
        assert len(events) == 4 and all(event.published_at for event in events)
        assert all(isinstance(event.payment_id, UUID) for event in events)
    print("PASS: real processing, immutable callback payload, 3 attempts, backoff and RabbitMQ DLQ")


async def main() -> None:
    action = {"seed": seed, "verify": verify}[sys.argv[1]]
    try:
        async with httpx.AsyncClient(
            base_url="http://127.0.0.1:8000",
            headers={"X-API-Key": settings().api_key.get_secret_value()},
        ) as client:
            await action(client)
    finally:
        await engine.dispose()


if __name__ == "__main__":
    asyncio.run(main())
