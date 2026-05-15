package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/redis/go-redis/v9"
)

const (
	streamName     = "pixel:events:stream"
	consumerGroup  = "worker-group"
	consumerName   = "worker-1"
	batchSize      = 100
	batchTimeoutMs = 2000
)

var (
	rdb *redis.Client
	ch  driver.Conn
)

func main() {
	// --- Redis ---
	rdb = redis.NewClient(&redis.Options{
		Addr:     fmt.Sprintf("%s:%s", env("REDIS_HOST", "localhost"), env("REDIS_PORT", "6379")),
		Password: os.Getenv("REDIS_PASSWORD"),
	})

	// --- ClickHouse ---
	var err error
	ch, err = clickhouse.Open(&clickhouse.Options{
		Addr: []string{fmt.Sprintf("%s:%s", env("CLICKHOUSE_HOST", "localhost"), env("CLICKHOUSE_PORT", "9000"))},
		Auth: clickhouse.Auth{
			Database: env("CLICKHOUSE_DB", "events"),
			Username: env("CLICKHOUSE_USER", "pixel_app"),
			Password: os.Getenv("CLICKHOUSE_PASSWORD"),
		},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		log.Printf("WARN clickhouse open: %v", err)
	} else if err := ch.Ping(context.Background()); err != nil {
		log.Printf("WARN clickhouse ping: %v", err)
	} else {
		log.Println("clickhouse: connected")
		if err := ensureSchema(); err != nil {
			log.Printf("WARN schema: %v", err)
		}
	}

	// Ensure consumer group exists (idempotent)
	ctx := context.Background()
	if err := rdb.XGroupCreateMkStream(ctx, streamName, consumerGroup, "0").Err(); err != nil {
		if err.Error() != "BUSYGROUP Consumer Group name already exists" {
			log.Printf("WARN xgroup create: %v", err)
		}
	}

	// --- Health check server (so ECS can probe) ---
	go func() {
		mux := http.NewServeMux()
		mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]string{
				"status":  "ok",
				"service": "worker-svc",
			})
		})
		addr := ":" + env("PORT", "8080")
		log.Printf("health server on %s", addr)
		_ = http.ListenAndServe(addr, mux)
	}()

	// --- Graceful shutdown ---
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)

	log.Println("worker-svc consuming from", streamName)
	runCtx, cancel := context.WithCancel(context.Background())
	go func() {
		<-stop
		log.Println("shutdown signal received")
		cancel()
	}()

	consume(runCtx)
	log.Println("worker-svc exited cleanly")
}

func consume(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		// Read up to batchSize entries, blocking up to batchTimeoutMs
		streams, err := rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    consumerGroup,
			Consumer: consumerName,
			Streams:  []string{streamName, ">"},
			Count:    batchSize,
			Block:    time.Duration(batchTimeoutMs) * time.Millisecond,
		}).Result()

		if err != nil {
			if err == redis.Nil || err == context.Canceled {
				continue
			}
			log.Printf("xreadgroup error: %v", err)
			time.Sleep(time.Second)
			continue
		}

		for _, s := range streams {
			ids := make([]string, 0, len(s.Messages))
			batch := make([]event, 0, len(s.Messages))
			for _, m := range s.Messages {
				batch = append(batch, parseMessage(m))
				ids = append(ids, m.ID)
			}
			if err := writeBatch(ctx, batch); err != nil {
				log.Printf("write batch (%d events): %v", len(batch), err)
				continue
			}
			// ACK after successful write
			if err := rdb.XAck(ctx, streamName, consumerGroup, ids...).Err(); err != nil {
				log.Printf("xack error: %v", err)
			} else {
				log.Printf("processed %d events", len(batch))
			}
		}
	}
}

type event struct {
	User      string
	Event     string
	IP        string
	UserAgent string
	Referrer  string
	TS        time.Time
}

func parseMessage(m redis.XMessage) event {
	get := func(k string) string {
		if v, ok := m.Values[k]; ok {
			return fmt.Sprint(v)
		}
		return ""
	}
	tsStr := get("ts")
	var ts time.Time
	if tsStr != "" {
		var sec int64
		fmt.Sscanf(tsStr, "%d", &sec)
		ts = time.Unix(sec, 0)
	} else {
		ts = time.Now()
	}
	return event{
		User:      get("user"),
		Event:     get("event"),
		IP:        get("ip"),
		UserAgent: get("userAgent"),
		Referrer:  get("referrer"),
		TS:        ts,
	}
}

func ensureSchema() error {
	q := `CREATE TABLE IF NOT EXISTS events.pixel_hits (
		ts          DateTime,
		user_id     String,
		event       String,
		ip          String,
		user_agent  String,
		referrer    String
	) ENGINE = MergeTree()
	  PARTITION BY toYYYYMM(ts)
	  ORDER BY (ts, user_id)
	  TTL ts + INTERVAL 6 MONTH`
	return ch.Exec(context.Background(), q)
}

func writeBatch(ctx context.Context, events []event) error {
	if ch == nil || len(events) == 0 {
		return nil
	}
	batch, err := ch.PrepareBatch(ctx, "INSERT INTO events.pixel_hits (ts, user_id, event, ip, user_agent, referrer)")
	if err != nil {
		return err
	}
	for _, e := range events {
		if err := batch.Append(e.TS, e.User, e.Event, e.IP, e.UserAgent, e.Referrer); err != nil {
			return err
		}
	}
	return batch.Send()
}

func env(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}
