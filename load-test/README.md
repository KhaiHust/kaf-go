# Load Test — kaf-go vs Apache Kafka

Side-by-side benchmark infrastructure for `kaf-go` against Apache Kafka 3.8.
See `../docs/load_test_plan.md` for the full design rationale and test matrix.

## Layout

```
load-test/
  Dockerfile.kaf-go              # multi-stage build of broker → distroless
  Dockerfile.loadgen             # multi-stage build of loadgen → distroless
  docker-compose.kaf.yml         # 3 kaf-go brokers + monitoring stack
  docker-compose.kafka.yml       # 3 Apache Kafka brokers + matching stack
  Makefile                       # one-shot helpers
  loadgen/
    go.mod
    main.go                      # franz-go workload generator with /metrics
  monitoring/
    prometheus-kaf.yml
    prometheus-kafka.yml
    jmx-kafka.yml                # JMX → Prometheus rules for Kafka
    grafana/
      datasources/prometheus.yml
      dashboards/
        provisioning.yml
        system.json               # cAdvisor + loadgen panels
        throughput.json           # kaf-go produce/fetch rates, connections
        latency.json              # kaf-go produce/fetch/purgatory percentiles
        replication.json          # ISR size, LEO−HWM gap, replica lag, shrink/expand
        idempotence.json          # dedup hits, validate errors, snapshot writes, PID blocks
```

## Prereqs (one-time)

1. **Docker Desktop** memory cap ≥ 16 GiB — Settings → Resources. Default 8 GiB will OOM.
2. **macOS / M-series**: native ARM64 images are used for everything; no Rosetta.
3. **Free ports**: 3000, 8080, 8090, 9090, 9092-9094, 19092-19094, 9200. Broker `/metrics` listens on `:9100` inside the network (not host-exposed; Prometheus reaches `kafN:9100` via Docker DNS).

## Quick start — kaf-go cluster

```bash
cd load-test
make up                            # build + start brokers + monitoring
make status                        # confirm 7 containers running
make logs                          # tail broker logs (Ctrl-C to detach)

open http://localhost:3000         # Grafana, anonymous viewer
open http://localhost:9090         # Prometheus
open http://localhost:8080         # cAdvisor

make loadgen                       # 60 s baseline (acks=-1, idempotent, 1 KiB)
make down                          # stop, preserve volumes
make nuke                          # stop + wipe volumes
```

Five dashboards are auto-provisioned under the **kaf-go** folder in Grafana:

- **kaf-go load test — system & client** — cAdvisor (CPU/RAM/IO/net) + loadgen client-side throughput and latency.
- **kaf-go — broker throughput** — produce/fetch records and bytes per broker; cluster totals; open connections.
- **kaf-go — broker latency** — server-side produce p50/p95/p99 by `acks`, fetch p50/p95/p99 by `role` (consumer|replica), purgatory wait, loadgen overlay.
- **kaf-go — replication & ISR** — ISR size, LEO−HWM gap, replica lag in offsets, shrink/expand events, min-ISR violations (`kaf_purgatory_fail_all_total`).
- **kaf-go — idempotent producer** — dup short-circuit rate, validate-error breakdown by code, snapshot writes/min, cumulative PID blocks leased.

Verify the broker scrape after `make up`:

```bash
curl -s http://localhost:9090/api/v1/targets | jq '.data.activeTargets[] | select(.labels.job=="kaf-brokers")'
# or directly inside the network:
docker compose -p kaf -f docker-compose.kaf.yml exec prometheus wget -qO- http://kaf1:9100/metrics | grep '^kaf_'
```

## Quick start — Apache Kafka comparison

**Tear down kaf-go first** — running both clusters at once contends for IO bandwidth and skews results.

```bash
make down                          # stop kaf-go
make kafka-up                      # start Apache Kafka 3.8 KRaft cluster
make kafka-loadgen                 # same baseline against Kafka
make kafka-down                    # stop Kafka
```

Same Grafana stack, same loadgen metrics — directly comparable side-by-side via the time-range picker.

## Test matrix

The `loadgen` flags map directly to the matrix in `docs/load_test_plan.md` §8:

```bash
# Phase 2 — acks sweep
docker compose -p kaf -f docker-compose.kaf.yml --profile bench run --rm loadgen \
  -brokers=kaf1:9092,kaf2:9092,kaf3:9092 \
  -topic=loadtest -workers=8 -record-size=1024 \
  -acks=0 -duration=60s -warmup=10s

# Phase 3 — partition sweep (run after creating the topic with N partitions)
docker compose -p kaf -f docker-compose.kaf.yml --profile bench run --rm loadgen \
  -topic=loadtest-12 -workers=8 -duration=60s

# Phase 5 — idempotence off
docker compose -p kaf -f docker-compose.kaf.yml --profile bench run --rm loadgen \
  -acks=-1 -idempotent=false -duration=60s
```

Topics are auto-created at default partition count by the kaf-go `cmd/client`; for non-default partition counts, run the existing `cmd/client` once with `--create-topic --partitions N`.

## Architecture diagram

```
                                   ┌────────────┐
                                   │  loadgen   │ franz-go
                                   │  :8090     │ → /metrics
                                   └──────┬─────┘
                                          │ Produce/Fetch
                  ┌───────────────────────┼───────────────────────┐
                  │                       │                       │
            ┌─────▼────┐           ┌──────▼─────┐         ┌───────▼──┐
            │ kaf1     │◄─────────►│  kaf2      │◄───────►│  kaf3    │
            │ :9092 api│           │  :9092 api │         │  :9092 api│
            │ :9100 /m │           │  :9100 /m  │         │  :9100 /m│
            └────┬─────┘           └──────┬─────┘         └────┬─────┘
                 │                        │                    │
                 └────────────────┬───────┴────────────────────┘
                                  │ scrape
                          ┌───────▼────────┐    ┌──────────┐
                          │   Prometheus   │◄───┤cAdvisor  │ container CPU/RAM/IO
                          │     :9090      │    └──────────┘
                          │                │◄───┤node_exp  │ host metrics
                          └───────┬────────┘    └──────────┘
                                  │
                          ┌───────▼────────┐
                          │    Grafana     │
                          │     :3000      │
                          └────────────────┘
```

## Cleanup

```bash
make nuke                          # kaf-go: stop + delete volumes
make kafka-nuke                    # Kafka: same
docker image rm kaf-go:local loadgen:local apache/kafka:3.8.0
docker volume prune                # everything else
```

## Milestone plan

See `docs/load_test_plan.md` §12. As of 2026-04-28 the kaf-go side of **M7** has landed (clean 30-min Phase 6 soak: 1.005 B records, 558 K msg/s, zero invariant violations, surfaced + fixed a hot-path INFO logging bug in `server/conn.go`). Kafka soak partial — all 3 brokers SIGSEGV on ARM64 JVM ~5 min into the run; chaos scenarios still pending.

- ✅ **M1 — Cluster scaffold** — kaf-go compose stack and Dockerfiles landed
- ✅ **M2 — Advertised-host plumbing** — `-advertised-host` flag landed (cross-container reachability)
- ✅ **M3 — Kafka comparison cluster** — compose written; live verification pending first run
- ✅ **M4 — Workload generator** — end-to-end loadgen verified against kaf-go; four real bring-up bugs fixed during this milestone (`NOT_CONTROLLER` error-code mismatch, distroless volume permissions, cAdvisor v0.55+ on OrbStack, loadgen scrape alias)
- ✅ **M5 — Broker metrics + dashboards** — `prometheus/client_golang` wired into kaf-go per §4; four broker-side dashboards (throughput, latency, replication, idempotence) auto-provisioned
- ✅ **M6 — Baseline + acks sweep** — Phase 1 baseline + Phase 2 acks sweep run with the new dashboards (`docs/load_test_results.md`)
- 🟡 **M7 — Full matrix + chaos + report** — Phases 3/4/5 head-to-head ✅; Phase 6 kaf-go 30-min soak ✅ (`docs/load_test_results.md` §"Phase 6"); Phase 6 Kafka partial (~4 min, JVM crash); chaos scenarios §10 still ⏳
