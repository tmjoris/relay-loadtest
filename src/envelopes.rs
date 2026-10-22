//! Envelopes shaped like what Sentry SDKs send, so Relay does its real
//! per-item work: parsing, normalization, PII scrubbing, inbound filters
//! and span processing.
//!
//! Errors carry deep stack traces with source context and local variables,
//! many breadcrumbs, request headers and user data, including values the
//! default scrubbing rules must catch (an email, a card number, a password
//! variable, an `Authorization` header, an `api_key` query parameter).
//! Transactions carry dozens of spans with SQL and HTTP descriptions, which
//! Relay normalizes. Sessions, cron check-ins and logs are small and
//! frequent. Projects are picked with a Zipf distribution, so a few projects
//! get most of the traffic, as in production.

use chrono::{DateTime, Utc};
use rand::rngs::StdRng;
use rand::seq::IndexedRandom;
use rand::{Rng, SeedableRng};
use rand_distr::{Distribution, LogNormal, Zipf};
use serde_json::{Value, json};

use crate::projects::Project;

/// Relative weights of each envelope kind.
#[derive(Clone, Copy, Debug)]
pub struct Mix {
    pub errors: u32,
    pub transactions: u32,
    pub sessions: u32,
    pub check_ins: u32,
    pub logs: u32,
}

impl Default for Mix {
    fn default() -> Self {
        Mix {
            errors: 30,
            transactions: 30,
            sessions: 25,
            check_ins: 5,
            logs: 10,
        }
    }
}

/// A ready-to-send envelope.
pub struct Prepared {
    pub project_id: u64,
    pub public_key: String,
    pub kind: &'static str,
    pub body: Vec<u8>,
}

/// Builds `n` envelopes with timestamps near `now`.
pub fn pool(
    projects: &[Project],
    mix: Mix,
    n: usize,
    seed: u64,
    now: DateTime<Utc>,
) -> Vec<Prepared> {
    let mut g = Gen {
        rng: StdRng::seed_from_u64(seed),
        now: now.timestamp_millis() as f64 / 1000.0,
    };
    let zipf = Zipf::new(projects.len() as f64, 1.1).expect("valid zipf");
    let kinds = [
        ("error", mix.errors),
        ("transaction", mix.transactions),
        ("session", mix.sessions),
        ("check_in", mix.check_ins),
        ("log", mix.logs),
    ];
    let total: u32 = kinds.iter().map(|k| k.1).sum();
    assert!(total > 0, "mix needs a positive weight");
    (0..n)
        .map(|_| {
            let project = &projects[zipf.sample(&mut g.rng) as usize - 1];
            let mut pick = g.rng.random_range(0..total);
            let kind = kinds
                .iter()
                .find(|(_, w)| {
                    if pick < *w {
                        true
                    } else {
                        pick -= w;
                        false
                    }
                })
                .expect("a kind")
                .0;
            let dsn = format!("http://{}@relay/{}", project.public_key, project.id);
            let body = match kind {
                "error" => g.error_envelope(&dsn),
                "transaction" => g.transaction_envelope(&dsn),
                "session" => g.session_envelope(&dsn),
                "check_in" => g.check_in_envelope(&dsn),
                _ => g.log_envelope(&dsn),
            };
            Prepared {
                project_id: project.id,
                public_key: project.public_key.clone(),
                kind,
                body,
            }
        })
        .collect()
}

/// Serializes an envelope: a header line, then each item's header (with
/// its `length`) and payload.
pub fn envelope(header: &Value, items: &[(Value, Vec<u8>)]) -> Vec<u8> {
    let mut out = serde_json::to_vec(header).expect("header serializes");
    out.push(b'\n');
    for (item_header, payload) in items {
        let mut h = item_header.clone();
        h["length"] = json!(payload.len());
        out.extend(serde_json::to_vec(&h).expect("item header serializes"));
        out.push(b'\n');
        out.extend(payload);
        out.push(b'\n');
    }
    out
}

const ROUTES: &[&str] = &[
    "/api/orders/{id}",
    "/api/users/{id}/profile",
    "/api/cart",
    "/api/search",
    "/checkout",
    "/api/products/{slug}",
    "/api/payments/{id}/capture",
    "/health",
];
const RELEASES: &[&str] = &["backend@1.42.0", "backend@1.42.1", "frontend@3.7.2"];
const ENVIRONMENTS: &[&str] = &["production", "production", "production", "staging"];
const EXCEPTIONS: &[(&str, &str)] = &[
    (
        "KeyError",
        "'user_id' not found in session for jane.doe@example.com",
    ),
    (
        "ValueError",
        "invalid literal for int() with base 10: 'abc'",
    ),
    (
        "DatabaseError",
        "deadlock detected while updating orders row 9182",
    ),
    (
        "TimeoutError",
        "upstream payments-api did not respond within 30s",
    ),
    (
        "TypeError",
        "Cannot read properties of undefined (reading 'total')",
    ),
];
const USER_AGENTS: &[&str] = &[
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_5) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.5 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64; rv:131.0) Gecko/20100101 Firefox/131.0",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 18_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15E148",
];
const QUERIES: &[&str] = &[
    "SELECT o.id, o.total, o.status FROM orders o WHERE o.user_id = 4821 AND o.created_at > '2026-01-01' ORDER BY o.created_at DESC LIMIT 20",
    "SELECT * FROM products WHERE slug = 'blue-mug-12oz' AND deleted_at IS NULL",
    "UPDATE carts SET updated_at = now(), item_count = 3 WHERE id = 'c-91f2' AND user_id = 4821",
    "INSERT INTO events (user_id, kind, payload) VALUES (4821, 'checkout', '{\"items\": 3}')",
    "SELECT u.id, u.email FROM users u JOIN sessions s ON s.user_id = u.id WHERE s.token = 'a91f0c22'",
];

struct Gen {
    rng: StdRng,
    now: f64,
}

impl Gen {
    fn hex(&mut self, bytes: usize) -> String {
        (0..bytes)
            .map(|_| format!("{:02x}", self.rng.random::<u8>()))
            .collect()
    }

    /// A UUID from the seeded generator, so pools are reproducible.
    fn uuid(&mut self) -> String {
        uuid::Builder::from_random_bytes(self.rng.random())
            .into_uuid()
            .to_string()
    }

    fn pick<'a>(&mut self, items: &'a [&'a str]) -> &'a str {
        items.choose(&mut self.rng).expect("non-empty")
    }

    fn rfc3339(&self, ts: f64) -> String {
        DateTime::from_timestamp_millis((ts * 1000.0) as i64)
            .expect("in range")
            .to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
    }

    fn header(&mut self, dsn: &str) -> Value {
        json!({"event_id": self.hex(16), "dsn": dsn, "sent_at": self.rfc3339(self.now)})
    }

    fn user(&mut self) -> Value {
        let id = self.rng.random_range(1..50_000);
        json!({
            "id": id.to_string(),
            "email": format!("user{id}@example.com"),
            "username": format!("user{id}"),
            "ip_address": format!("203.0.113.{}", id % 250),
        })
    }

    fn request(&mut self, route: &str) -> Value {
        json!({
            "url": format!("https://shop.example.com{}", route.replace("{id}", "4821").replace("{slug}", "blue-mug")),
            "method": "GET",
            "query_string": "page=2&api_key=sk_live_51HxT9e&sort=desc",
            "headers": {
                "User-Agent": self.pick(USER_AGENTS),
                "Cookie": "sessionid=a91f0c22; csrftoken=6b1e33e0",
                "Authorization": "Bearer eyJhbGciOiJIUzI1NiJ9.e30.ZRrHA1JJJW8opsbCGfG_HACGpVUMN_a9IV7pAx_Zmeo",
                "Accept-Language": "en-US,en;q=0.9",
            },
            "env": {"REMOTE_ADDR": "203.0.113.7"},
        })
    }

    fn frame(&mut self, i: usize, javascript: bool) -> Value {
        let lineno = self.rng.random_range(10..900);
        if javascript {
            json!({
                "filename": format!("https://shop.example.com/static/js/chunk-{}.js", i % 7),
                "abs_path": format!("https://shop.example.com/static/js/chunk-{}.js", i % 7),
                "function": format!("handler{i}"),
                "lineno": 1,
                "colno": lineno * 13,
                "in_app": !i.is_multiple_of(3),
            })
        } else {
            json!({
                "filename": format!("app/services/module_{}.py", i % 11),
                "abs_path": format!("/srv/app/services/module_{}.py", i % 11),
                "module": format!("app.services.module_{}", i % 11),
                "function": format!("process_step_{i}"),
                "lineno": lineno,
                "context_line": format!("    result = handler.run(order_id, user={})", i),
                "pre_context": ["def run(self):", "    order_id = self.order.id", "    handler = self.handler"],
                "post_context": ["    return result", "", "def cancel(self):"],
                "in_app": !i.is_multiple_of(4),
                "vars": {
                    "order_id": "9182",
                    "password": "hunter2",
                    "card": "4242 4242 4242 4242",
                    "self": "<OrderService object at 0x7f3a2c1b9e50>",
                },
            })
        }
    }

    fn error_envelope(&mut self, dsn: &str) -> Vec<u8> {
        let javascript = self.rng.random_bool(0.4);
        let route = self.pick(ROUTES);
        let (ty, value) = *EXCEPTIONS.choose(&mut self.rng).expect("non-empty");
        let frames: Vec<Value> = (0..self.rng.random_range(10..40))
            .map(|i| self.frame(i, javascript))
            .collect();
        let crumbs: Vec<Value> = (0..self.rng.random_range(10..50))
            .map(|i| {
                json!({
                    "timestamp": self.now - 60.0 + i as f64,
                    "category": if i % 3 == 0 { "query" } else { "http" },
                    "message": if i % 3 == 0 { QUERIES[i % QUERIES.len()].to_string() } else { format!("GET https://api.example.com/v1/items/{i}") },
                    "level": "info",
                    "data": {"status_code": 200, "duration_ms": i * 3},
                })
            })
            .collect();
        let header = self.header(dsn);
        let event = json!({
            "event_id": header["event_id"],
            "timestamp": self.now,
            "platform": if javascript { "javascript" } else { "python" },
            "level": "error",
            "logger": "app.views",
            "server_name": format!("web-{}", self.rng.random_range(1..40)),
            "release": self.pick(RELEASES),
            "environment": self.pick(ENVIRONMENTS),
            "transaction": route,
            "exception": {"values": [{
                "type": ty,
                "value": value,
                "mechanism": {"type": "generic", "handled": false},
                "stacktrace": {"frames": frames},
            }]},
            "breadcrumbs": {"values": crumbs},
            "request": self.request(route),
            "user": self.user(),
            "contexts": {
                "os": {"name": "Linux", "version": "6.8.0"},
                "runtime": {"name": if javascript { "node" } else { "CPython" }, "version": "3.12.4"},
                "trace": {"trace_id": self.hex(16), "span_id": self.hex(8), "op": "http.server"},
            },
            "tags": {"customer_tier": "gold", "region": "eu-west-1", "feature_flag": "new-checkout"},
            "extra": {"cart_size": 3, "credit_card": "4242 4242 4242 4242", "session_token": "a91f0c22"},
            "sdk": {"name": if javascript { "sentry.javascript.browser" } else { "sentry.python" }, "version": "9.1.0"},
        });
        envelope(
            &header,
            &[(
                json!({"type": "event"}),
                serde_json::to_vec(&event).unwrap(),
            )],
        )
    }

    fn transaction_envelope(&mut self, dsn: &str) -> Vec<u8> {
        let route = self.pick(ROUTES);
        let trace_id = self.hex(16);
        let root = self.hex(8);
        let duration = LogNormal::new(-1.5f64, 0.8).unwrap().sample(&mut self.rng);
        let start = self.now - duration;
        let n = self.rng.random_range(20..100);
        let spans: Vec<Value> = (0..n)
            .map(|i| {
                let s = start + duration * i as f64 / n as f64;
                let (op, description) = match i % 4 {
                    0 | 1 => ("db", QUERIES[i % QUERIES.len()].to_string()),
                    2 => (
                        "http.client",
                        format!(
                            "GET https://payments.internal/v2/charges/ch_{}?expand=customer",
                            1000 + i
                        ),
                    ),
                    _ => ("cache.get", format!("cache:user:{}:profile", 4821 + i)),
                };
                json!({
                    "span_id": self.hex(8),
                    "parent_span_id": root,
                    "trace_id": trace_id,
                    "op": op,
                    "description": description,
                    "start_timestamp": s,
                    "timestamp": s + duration / n as f64 * 0.8,
                    "status": "ok",
                    "data": {"db.system": "postgresql", "thread.name": "MainThread"},
                })
            })
            .collect();
        let header = self.header(dsn);
        let event = json!({
            "type": "transaction",
            "event_id": header["event_id"],
            "transaction": format!("GET {route}"),
            "transaction_info": {"source": "route"},
            "start_timestamp": start,
            "timestamp": self.now,
            "platform": "python",
            "release": self.pick(RELEASES),
            "environment": self.pick(ENVIRONMENTS),
            "contexts": {"trace": {"trace_id": trace_id, "span_id": root, "op": "http.server", "status": "ok"}},
            "spans": spans,
            "measurements": {"lcp": {"value": 1830.0, "unit": "millisecond"}, "cls": {"value": 0.04}},
            "request": self.request(route),
            "user": self.user(),
            "sdk": {"name": "sentry.python", "version": "2.19.0"},
        });
        envelope(
            &header,
            &[(
                json!({"type": "transaction"}),
                serde_json::to_vec(&event).unwrap(),
            )],
        )
    }

    fn session_envelope(&mut self, dsn: &str) -> Vec<u8> {
        let header = json!({"dsn": dsn, "sent_at": self.rfc3339(self.now)});
        let status = *["ok", "exited", "exited", "crashed", "errored"]
            .choose(&mut self.rng)
            .expect("non-empty");
        let session = json!({
            "sid": self.uuid(),
            "did": format!("user{}", self.rng.random_range(1..50_000)),
            "init": true,
            "started": self.rfc3339(self.now - 30.0),
            "timestamp": self.rfc3339(self.now),
            "status": status,
            "errors": u32::from(status != "ok" && status != "exited"),
            "duration": 30.0,
            "attrs": {"release": self.pick(RELEASES), "environment": self.pick(ENVIRONMENTS)},
        });
        envelope(
            &header,
            &[(
                json!({"type": "session"}),
                serde_json::to_vec(&session).unwrap(),
            )],
        )
    }

    fn check_in_envelope(&mut self, dsn: &str) -> Vec<u8> {
        let header = json!({"dsn": dsn, "sent_at": self.rfc3339(self.now)});
        let check_in = json!({
            "check_in_id": self.hex(16),
            "monitor_slug": format!("nightly-job-{}", self.rng.random_range(1..20)),
            "status": if self.rng.random_bool(0.95) { "ok" } else { "error" },
            "duration": self.rng.random_range(1.0..300.0),
            "environment": "production",
        });
        envelope(
            &header,
            &[(
                json!({"type": "check_in"}),
                serde_json::to_vec(&check_in).unwrap(),
            )],
        )
    }

    fn log_envelope(&mut self, dsn: &str) -> Vec<u8> {
        let header = json!({"dsn": dsn, "sent_at": self.rfc3339(self.now)});
        let trace_id = self.hex(16);
        let n = self.rng.random_range(5..50);
        let items: Vec<Value> = (0..n)
            .map(|i| {
                let level = ["info", "info", "warn", "error"][i % 4];
                json!({
                    "timestamp": self.now - (n - i) as f64 * 0.01,
                    "trace_id": trace_id,
                    "level": level,
                    "body": format!("checkout step {i} finished for order 9182 user jane.doe@example.com"),
                    "attributes": {
                        "service": {"value": "api", "type": "string"},
                        "step": {"value": i, "type": "integer"},
                        "duration_ms": {"value": i as f64 * 1.5, "type": "double"},
                    },
                })
            })
            .collect();
        envelope(
            &header,
            &[(
                json!({"type": "log", "item_count": n, "content_type": "application/vnd.sentry.items.log+json"}),
                serde_json::to_vec(&json!({"items": items})).unwrap(),
            )],
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::projects::projects;

    #[test]
    fn pool_is_deterministic_and_parseable() {
        let p = projects(50);
        let now = DateTime::from_timestamp(1_792_000_000, 0).unwrap();
        let a = pool(&p, Mix::default(), 200, 7, now);
        let b = pool(&p, Mix::default(), 200, 7, now);
        let digest = |pool: &[Prepared]| {
            use std::hash::{Hash, Hasher};
            let mut h = std::collections::hash_map::DefaultHasher::new();
            pool.iter().for_each(|e| e.body.hash(&mut h));
            h.finish()
        };
        assert_eq!(digest(&a), digest(&b), "same seed, same pool");
        for e in &a {
            // Header line is JSON; every item header declares a length that
            // matches its payload.
            let mut rest = &e.body[..];
            let nl = rest.iter().position(|&c| c == b'\n').unwrap();
            serde_json::from_slice::<Value>(&rest[..nl]).unwrap();
            rest = &rest[nl + 1..];
            while !rest.is_empty() {
                let nl = rest.iter().position(|&c| c == b'\n').unwrap();
                let h: Value = serde_json::from_slice(&rest[..nl]).unwrap();
                let len = h["length"].as_u64().unwrap() as usize;
                serde_json::from_slice::<Value>(&rest[nl + 1..nl + 1 + len]).unwrap();
                rest = &rest[nl + 1 + len + 1..];
            }
        }
        let kinds: std::collections::BTreeSet<_> = a.iter().map(|e| e.kind).collect();
        assert_eq!(kinds.len(), 5);
        // Zipf: the first project gets far more than its 2% share.
        let top = a.iter().filter(|e| e.project_id == 1).count();
        assert!(top > 20, "{top}");
    }
}
