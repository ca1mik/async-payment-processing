"""Create payments and transactional outbox."""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB

revision = "0001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "payments",
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column("amount", sa.Numeric(14, 2), nullable=False),
        sa.Column("currency", sa.String(3), nullable=False),
        sa.Column("description", sa.String(500), nullable=False),
        sa.Column("metadata", JSONB(), nullable=False),
        sa.Column("status", sa.String(16), server_default="pending", nullable=False),
        sa.Column("idempotency_key", sa.String(128), nullable=False, unique=True),
        sa.Column("request_hash", sa.String(64), nullable=False),
        sa.Column("webhook_url", sa.String(2048), nullable=False),
        sa.Column("webhook_state", sa.String(16), server_default="pending", nullable=False),
        sa.Column("attempt", sa.Integer(), server_default="1", nullable=False),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
        sa.Column("processed_at", sa.DateTime(timezone=True)),
        sa.CheckConstraint("amount > 0", name="payment_positive_amount"),
        sa.CheckConstraint("currency IN ('RUB', 'USD', 'EUR')", name="payment_currency"),
        sa.CheckConstraint("status IN ('pending', 'succeeded', 'failed')", name="payment_status"),
        sa.CheckConstraint(
            "webhook_state IN ('pending', 'delivered', 'dead_lettered')",
            name="payment_webhook_state",
        ),
        sa.CheckConstraint("attempt BETWEEN 1 AND 3", name="payment_attempt"),
    )
    op.create_table(
        "outbox",
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column("payment_id", sa.Uuid(), sa.ForeignKey("payments.id"), nullable=False),
        sa.Column("topic", sa.String(64), nullable=False),
        sa.Column("attempt", sa.Integer(), nullable=False),
        sa.Column(
            "available_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
        sa.Column("published_at", sa.DateTime(timezone=True)),
        sa.UniqueConstraint("payment_id", "topic", "attempt", name="outbox_unique_job"),
    )
    op.create_index(
        "outbox_pending",
        "outbox",
        ["available_at", "id"],
        postgresql_where=sa.text("published_at IS NULL"),
    )


def downgrade():
    op.drop_table("outbox")
    op.drop_table("payments")
