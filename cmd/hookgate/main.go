// Command hookgate runs any combination of hookgate's roles in one process:
//
//	ROLES=api,relay,processor,sink   (default: all four)
//
// Split them across deployments to scale each independently, e.g. several
// api pods behind a load balancer and a processor deployment sized to the
// queue backlog.
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"slices"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Ahmedkholaif/hookgate/internal/api"
	"github.com/Ahmedkholaif/hookgate/internal/broker"
	"github.com/Ahmedkholaif/hookgate/internal/store"
	"github.com/Ahmedkholaif/hookgate/internal/worker"
)

func main() {
	level := slog.LevelInfo
	if os.Getenv("LOG_LEVEL") == "debug" {
		level = slog.LevelDebug
	}
	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: level}))
	if err := run(log); err != nil {
		log.Error("fatal", "err", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	roles := worker.SplitList(getenv("ROLES", "api,relay,processor,sink"))
	eventsTopic := getenv("EVENTS_TOPIC", "hookgate-events")

	poolCfg, err := pgxpool.ParseConfig(getenv("DATABASE_URL", "postgres://postgres:postgres@localhost:5432/hookgate?sslmode=disable"))
	if err != nil {
		return err
	}
	// pgx defaults to max(4, CPUs) connections, far fewer than the handlers a
	// worker runs concurrently; size the pool for them.
	poolCfg.MaxConns = int32(getint("DB_MAX_CONNS", 20))
	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		return err
	}
	defer pool.Close()
	if err := store.Migrate(ctx, pool); err != nil {
		return err
	}
	st := store.New(pool)

	var b broker.Broker
	if slices.ContainsFunc(roles, func(r string) bool { return r != "api" }) {
		if b, err = broker.Open(ctx); err != nil {
			return err
		}
		defer b.Close()
	}

	// Every process serves /healthz (for probes); only the api role serves the API.
	var handler http.Handler = api.HealthOnly(st)
	if slices.Contains(roles, "api") {
		handler = api.New(st, api.Config{
			EventsTopic: eventsTopic, APIKey: os.Getenv("API_KEY"), Actor: worker.Producer + "/api",
		}, log).Handler()
	}
	if slices.Contains(roles, "api") && os.Getenv("API_KEY") == "" {
		log.Warn("API_KEY is not set: the admin API (/v1/*) is open to anyone who can reach it")
	}
	srv := &http.Server{Addr: getenv("ADDR", ":8080"), Handler: handler, ReadHeaderTimeout: 5 * time.Second}
	errc := make(chan error, 1)
	go func() { errc <- srv.ListenAndServe() }()

	var wg sync.WaitGroup
	for _, role := range roles {
		switch role {
		case "api": // served above
		case "relay":
			r := worker.NewRelay(st, b, worker.RelayConfig{
				BatchSize:    getint("RELAY_BATCH", 100),
				PollInterval: time.Duration(getint("RELAY_POLL_MS", 200)) * time.Millisecond,
				Lease:        30 * time.Second,
			}, log)
			wg.Go(func() { r.Run(ctx) })
			if days := getint("RETENTION_DAYS", 14); os.Getenv("RETENTION_DAYS") != "0" {
				j := &worker.Janitor{St: st, Retention: time.Duration(days) * 24 * time.Hour, Every: 10 * time.Minute, Log: log.With("role", "janitor")}
				wg.Go(func() { j.Run(ctx) })
			}
		case "processor":
			p := worker.NewProcessor(st, b, worker.ProcessorConfig{
				EventsTopic:    eventsTopic,
				Concurrency:    getint("PROCESSOR_CONCURRENCY", 16),
				MaxAttempts:    getint("MAX_ATTEMPTS", 5),
				RetryBase:      time.Duration(getint("RETRY_BASE_MS", 1000)) * time.Millisecond,
				RetryMax:       time.Duration(getint("RETRY_MAX_MS", 300_000)) * time.Millisecond,
				ChaosFailTypes: worker.SplitList(os.Getenv("CHAOS_FAIL_TYPES")),
			}, log)
			wg.Go(func() { p.Run(ctx) })
		case "sink":
			topics := worker.SplitList(os.Getenv("SINK_TOPICS"))
			if len(topics) == 0 {
				log.Info("sink: SINK_TOPICS is empty; nothing to consume")
				continue
			}
			s := worker.NewSink(st, b, topics, getint("SINK_CONCURRENCY", 16), log)
			wg.Go(func() { s.Run(ctx) })
		default:
			return errors.New("unknown role " + strconv.Quote(role) + " (want api, relay, processor, sink)")
		}
	}
	brokerName := "none"
	if b != nil {
		brokerName = b.Name()
	}
	log.Info("hookgate started", "roles", roles, "broker", brokerName, "events_topic", eventsTopic)

	select {
	case err := <-errc:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			stop()
			wg.Wait()
			return err
		}
	case <-ctx.Done():
	}
	log.Info("shutting down: draining in-flight work")
	sctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	srv.Shutdown(sctx)
	stop()
	wg.Wait()
	return nil
}

func getenv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func getint(k string, def int) int {
	if n, err := strconv.Atoi(os.Getenv(k)); err == nil && n > 0 {
		return n
	}
	return def
}
