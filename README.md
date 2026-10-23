# relay-loadtest

Load tests [Relay](https://github.com/getsentry/relay), Sentry's ingestion
service, on its own: no Sentry, Kafka or Redis. It is for finding out where
Relay spends its time, on a laptop or in CI, and for checking whether a change
makes it faster.

Sentry's own [ingest-load-tester](https://github.com/getsentry/ingest-load-tester)
tests the whole Relay and Sentry chain at production scale. It used to ship a
fake Sentry for testing Relay alone, but removed it in May 2026
([#62](https://github.com/getsentry/ingest-load-tester/pull/62)) to focus on
real Sentry instances. This repository covers that gap and complements the
tool rather than replacing it.

## What is in it

- **`fake-upstream`** plays Sentry for a Relay in managed mode. It speaks the
  same protocol as the `mini_sentry` fixture in Relay's integration tests:
  relay registration, project configs (version 3, with the global config),
  public keys and outcomes. Every envelope Relay forwards is decompressed and
  its items are counted by type, then dropped. `GET /stats` returns the counts.
- **`blast`** sends envelopes at a target rate. The pool of envelopes is built
  once, deterministically from a seed, and replayed. There are five kinds:
  errors with deep stack traces, breadcrumbs, request headers and values the
  default scrubbing rules should catch; transactions with 20 to 100 spans of
  SQL and HTTP calls; sessions; cron check-ins; and logs. Projects are picked
  with a Zipf distribution, so a few projects get most of the traffic. Response
  latency is recorded with [DDSketch](https://github.com/tmjoris/funnel/tree/main/crates/sketch)
  and per-sender sketches are merged at the end.
- **`scripts/run.py`** starts a fresh fake upstream and Relay for every rate,
  warms up so every project config is fetched, measures Relay's idle CPU, runs
  the blaster, and reports envelopes accepted per second, response latency,
  what reached the upstream, the outcomes Relay reported, and Relay's CPU and
  peak memory.

## Running

```sh
cargo build --release
# Relay, built from a checkout of getsentry/relay:
#   git submodule update --init --recursive && cargo build --release -p relay
python3 scripts/run.py --relay-bin ~/relay/target/release/relay --rates 200,500,1000
```

Useful options:

| option | effect |
|---|---|
| `--mix errors=1` | send only one kind of envelope, to cost each kind separately |
| `--user-agents 5000` | draw from 5,000 user agents with a Zipf distribution |
| `--unique-user-agents --pool 20000` | give every envelope a user agent no other envelope has |
| `--relay-cpus 0` | pin Relay to CPU 0 with `taskset` |
| `--perf out.data --call-graph lbr` | record Relay with `perf` during the run |
| `--statsd 127.0.0.1:8125` | send Relay's internal metrics (timers, counters) to a StatsD server |

Relay's internal metrics can go to [Funnel](https://github.com/tmjoris/funnel),
which accepts StatsD, and be queried there, for example the p99 of
`sentry.relay.signature.create.duration` during a run.

### Profiling

Relay's release build keeps full debug information, but has no frame
pointers, and `perf`'s DWARF unwinding lost the stack for 68% of samples on
the 700 MB Relay binary. On Intel CPUs from Haswell onwards, last branch
record call stacks (`--call-graph lbr`) recorded a stack for every sample. To
fold stacks into a flame graph:

```sh
perf script -i out.data | inferno-collapse-perf | inferno-flamegraph > flame.svg
```

`perf` needs `kernel.perf_event_paranoid` at 1 or lower.

## What it found

Measured on a 4-thread Intel i5-6200U laptop against Relay 26.10.0, with Relay
unpinned and the blaster and fake upstream on the same machine. The harness on
its own moves 15,900 envelopes per second on this machine, well above what
Relay reached.

**Cost per envelope.** At 300 envelopes per second of one kind:

| kind | Relay CPU per envelope |
|---|---|
| transaction | 7.5 ms |
| error | 3.3 ms |
| log | 1.8 ms |
| check-in | 1.2 ms |
| session | 1.1 ms |

Relay kept up with about 900 envelopes per second of mixed traffic. Sent as
fast as possible, it accepted around 10,000 per second while forwarding a few
hundred, and its memory grew to 3 GB in 15 seconds.

**User agent parsing.** For error events, 35% of Relay's CPU went to parsing
user agents, 10 points of it in the inbound filters parsing the user agent a
second time ([getsentry/relay#5027](https://github.com/getsentry/relay/issues/5027)).
Relay's parser, `uaparser`, tries the uap-core rules one by one. `ua-parser`
from the uap-rust project prefilters them, and on uap-core's 17,813 distinct
test user agents it parses browser, OS and device in 15 µs instead of 276 µs,
with identical results on all 18,210 test cases. Switched over in Relay
(branch [`perf/ua-prefilter`](https://github.com/tmjoris/relay/tree/perf/ua-prefilter)),
CPU for the same traffic fell as follows, in two rounds each:

| traffic | Relay today | with `ua-parser` | change |
|---|---|---|---|
| errors, 200/s | 0.74 cores | 0.51 cores | −31% |
| transactions, 200/s | 1.60 | 1.31 | −18% |
| mixed, 300/s | 1.21 | 0.97 | −20% |
| mixed, 5,000 user agents | 1.23 | 0.98 | −20% |
| mixed, every user agent unique | 1.25 | 0.99 | −21% |

With every user agent unique, parsing went from 35% to 2.7% of Relay's CPU for
errors. A cache of parsed user agents was tried first: it saved as much when
user agents repeat, but only 9% when they are all unique, and on top of the
faster parser it would save at most the remaining 2.7%.

**Signatures.** A managed Relay signs every request it forwards with Ed25519,
53 µs each in the running Relay, about 4% of its CPU for small envelopes. The
signature covers no data, only a timestamp (getsentry/relay#4772), so it could
be reused for a short time; this has not been pursued.

## Limits

- One laptop: absolute numbers will differ on servers, and the blaster and
  Relay compete for the same CPUs. Relative comparisons between builds, run
  alternately, are the reliable part.
- Relay runs in managed mode without processing, so Kafka, Redis and the
  processing-only steps are not exercised.
- The synthetic payloads are realistic in shape but not taken from real
  traffic; in particular, real SQL and user agents vary more.

## License

MIT
