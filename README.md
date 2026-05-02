# wis2-gc-message-analysis

Monitors WIS2 Global Cache fidelity by comparing notification messages published by the WIS2 Global Broker (origin) against the corresponding messages re-published by a Global Cache. Messages are matched by compound key (`data_id` + `pubtime`) and checked for illegal differences.

## How it works

1. Subscribes to two MQTT brokers over MQTTS:
   - **Origin**: `mqtts://globalbroker.meteo.fr:8883` — topic `origin/a/wis2/#`
   - **Cache**: `mqtts://wis2cache.globaldata.nws.noaa.gov:8883` — topic `cache/a/wis2/#`
2. Filters to messages where `notification-type=data` and `data-policy=core`.
3. Origin messages are stored in Redis (TTL 600 s) keyed by `data_id|pubtime`.
4. When a cache message arrives, its matching origin message is retrieved and the pair is compared.
5. Origin messages with no cache match after 600 s are logged and counted as missed.

### Permitted differences between matched pairs

| Field | Reason |
|-------|--------|
| `id` | Always a unique UUID per message |
| `properties.global-cache` | Added only by Global Caches |
| `links[rel=canonical\|update].href` | Points to the cache host instead of the origin host |

Any other difference is flagged as illegal and printed as a unified diff.

## Deployment

```bash
docker compose up -d
```

The `traefik` network must already exist (external).

To restrict to a single WIS2 Node, set `command` in `docker-compose.yml`:

```yaml
gc-analysis:
  command: ["--centre-id", "ca-eccc-msc"]
```

Or pass arguments directly:

```bash
docker run --rm --network traefik gc-analysis --centre-id ca-eccc-msc
```

## Prometheus

Add the following scrape job to your `prometheus.yml`:

```yaml
- job_name: 'gc-analysis'
  scheme: http
  metrics_path: '/metrics'
  static_configs:
    - targets: ['gc-analysis:8000']
```

### Metrics

| Metric | Labels | Description |
|--------|--------|-------------|
| `matched_messages_total` | `centre_id` | Origin+cache pairs successfully matched |
| `messages_with_illegal_differences_total` | `centre_id` | Matched pairs with illegal differences |
| `missed_messages_total` | `centre_id` | Origin messages with no cache match after 600 s |

## CLI reference

```
python -m src.main --help
```

| Flag | Default | Description |
|------|---------|-------------|
| `--centre-id CENTRE_ID` | *(all)* | Restrict to one WIS2 Node |
| `--metrics-port PORT` | `8000` | Prometheus metrics port |
| `--redis-host HOST` | `redis` | Redis hostname |
| `--redis-port PORT` | `6379` | Redis port |
