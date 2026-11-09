import unittest
from unittest.mock import AsyncMock, patch
from uuid import uuid4

from fastapi.testclient import TestClient
from faststream.rabbit import TestRabbitBroker

from app.api import app, canonical_request
from app.config import settings
from app.schemas import PaymentInput, PaymentJob
from app.worker import NEW, broker


class ApiContract(unittest.TestCase):
    def setUp(self):
        self.client = TestClient(app)
        self.headers = {
            "X-API-Key": settings().api_key.get_secret_value(),
            "Idempotency-Key": "test",
        }
        self.body = {
            "amount": "100.00",
            "currency": "RUB",
            "description": "Test",
            "webhook_url": "http://webhook:8000/hook/ok",
        }

    def test_both_endpoints_require_api_key(self):
        self.assertEqual(self.client.post("/api/v1/payments", json=self.body).status_code, 401)
        self.assertEqual(self.client.get(f"/api/v1/payments/{uuid4()}").status_code, 401)

    def test_invalid_create_requests_never_reach_database(self):
        for field, value in (
            ("amount", "1.001"),
            ("amount", "0"),
            ("currency", "GBP"),
            ("webhook_url", "http://untrusted.example/hook"),
            ("webhook_url", "http://user:password@webhook/hook"),
        ):
            with self.subTest(field=field, value=value):
                response = self.client.post(
                    "/api/v1/payments", headers=self.headers, json={**self.body, field: value}
                )
                self.assertEqual(response.status_code, 422)
        headers = {"X-API-Key": self.headers["X-API-Key"]}
        self.assertEqual(
            self.client.post("/api/v1/payments", headers=headers, json=self.body).status_code, 422
        )

    def test_equivalent_decimal_has_canonical_representation(self):
        first = PaymentInput.model_validate(self.body)
        second = PaymentInput.model_validate({**self.body, "amount": 100})
        self.assertEqual(canonical_request(first), canonical_request(second))


class BrokerContract(unittest.IsolatedAsyncioTestCase):
    async def test_dispatches_valid_messages_and_rejects_malformed_body(self):
        job = PaymentJob(payment_id=uuid4(), attempt=1)
        async with TestRabbitBroker(broker) as test_broker:
            with patch("app.worker.process", new_callable=AsyncMock, return_value=True) as process:
                await test_broker.publish(job.model_dump(mode="json"), queue=NEW)
                await test_broker.publish(
                    b"{invalid-json", queue=NEW, content_type="application/json"
                )
                process.assert_awaited_once_with(job)
