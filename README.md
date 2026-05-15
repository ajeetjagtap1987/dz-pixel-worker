# dz-pixel-worker

Background consumer that reads pixel events from Redis Stream (`pixel:events:stream`) and batch-inserts them into ClickHouse (`events.pixel_hits`).

Designed to run on **Fargate Spot** — handles interruptions cleanly because Redis Stream consumer groups guarantee at-least-once delivery via XACK.

## Environment Variables

| Variable | Default | Purpose |
|---|---|---|
| `PORT` | `8080` | Health check port only |
| `REDIS_HOST` | `localhost` | Redis host |
| `REDIS_PASSWORD` | — | Redis password |
| `CLICKHOUSE_HOST` | `localhost` | ClickHouse host |
| `CLICKHOUSE_PORT` | `9000` | Native TCP port |
| `CLICKHOUSE_DB` | `events` | Database |
| `CLICKHOUSE_USER` | `pixel_app` | Username |
| `CLICKHOUSE_PASSWORD` | — | Password |

## Endpoints

- `GET /healthz` → 200 status (required for ECS health check)
- No public endpoints — this is a background worker

## Schema (auto-created)

```sql
CREATE TABLE events.pixel_hits (
    ts         DateTime,
    user_id    String,
    event      String,
    ip         String,
    user_agent String,
    referrer   String
) ENGINE = MergeTree()
  PARTITION BY toYYYYMM(ts)
  ORDER BY (ts, user_id)
  TTL ts + INTERVAL 6 MONTH;
```

## Spot interruption behavior

When AWS reclaims the Spot task, the in-flight batch is NOT acked. Another worker (or a new task) reads the pending entries via `XREADGROUP`. No data loss.
