CREATE TABLE IF NOT EXISTS notifications (
    id UUID PRIMARY KEY,
    idempotency_key VARCHAR(128) UNIQUE NOT NULL,
    user_id VARCHAR(64) NOT NULL,
    priority VARCHAR(32) NOT NULL,
    content TEXT NOT NULL,
    destination_phone VARCHAR(32),
    destination_device_token VARCHAR(256),
    status VARCHAR(32) NOT NULL,
    delivered_channel VARCHAR(32),
    delivered_provider VARCHAR(64),
    failure_reason TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_notifications_user_date ON notifications (user_id, created_at);
CREATE INDEX IF NOT EXISTS idx_notifications_status ON notifications (status);
