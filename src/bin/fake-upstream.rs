//! A stand-in for Sentry as Relay's upstream, speaking the protocol of the
//! `mini_sentry` fixture in Relay's integration tests: relay registration,
//! project configs (version 3, with the global config), public keys and
//! outcomes. Envelopes Relay forwards are decompressed, their items counted
//! by type and then dropped. `GET /stats` returns the counts.
//!
//!     fake-upstream [--listen 127.0.0.1:7000] [--projects 100]

use std::collections::{BTreeMap, HashMap};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use axum::body::Bytes;
use axum::extract::{DefaultBodyLimit, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::routing::{get, post};
use axum::{Json, Router};
use clap::Parser;
use relay_loadtest::projects::{self, Project};
use serde::Deserialize;
use serde_json::{Value, json};

#[derive(Parser)]
struct Args {
    #[arg(long, default_value = "127.0.0.1:7000")]
    listen: SocketAddr,
    /// Number of projects to serve configs for; must match the blaster.
    #[arg(long, default_value_t = 100)]
    projects: u64,
}

#[derive(Default)]
struct Counters {
    envelopes: AtomicU64,
    bytes_compressed: AtomicU64,
    bytes_uncompressed: AtomicU64,
    config_requests: AtomicU64,
    outcome_requests: AtomicU64,
    bad_envelopes: AtomicU64,
    items: Mutex<BTreeMap<String, u64>>,
    outcomes: Mutex<BTreeMap<String, u64>>,
}

struct AppState {
    by_key: HashMap<String, Project>,
    global: Value,
    counters: Counters,
}

type Shared = Arc<AppState>;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let state = Arc::new(AppState {
        by_key: projects::projects(args.projects)
            .into_iter()
            .map(|p| (p.public_key.clone(), p))
            .collect(),
        global: projects::global_config(),
        counters: Counters::default(),
    });
    let app = Router::new()
        .route("/api/0/relays/register/challenge/", post(challenge))
        .route("/api/0/relays/register/response/", post(response))
        .route(
            "/api/0/relays/live/",
            get(|| async { Json(json!({"is_healthy": true})) }),
        )
        .route("/api/0/relays/projectconfigs/", post(project_configs))
        .route(
            "/api/0/relays/publickeys/",
            post(|| async { Json(json!({"public_keys": {}, "relays": {}})) }),
        )
        .route("/api/0/relays/outcomes/", post(outcomes))
        .route("/api/0/relays/metrics/", post(|| async { Json(json!({})) }))
        .route("/api/{project}/envelope/", post(envelope))
        .route("/stats", get(stats))
        .layer(DefaultBodyLimit::max(256 << 20))
        .with_state(state);
    let listener = tokio::net::TcpListener::bind(args.listen).await?;
    eprintln!(
        "fake upstream on http://{} serving {} projects",
        listener.local_addr()?,
        args.projects
    );
    axum::serve(listener, app)
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await?;
    Ok(())
}

async fn challenge(Json(body): Json<Value>) -> Json<Value> {
    Json(json!({"token": "loadtest-token", "relay_id": body["relay_id"]}))
}

async fn response(Json(body): Json<Value>) -> Json<Value> {
    Json(json!({"relay_id": body["relay_id"]}))
}

#[derive(Deserialize)]
struct Version {
    version: Option<String>,
}

async fn project_configs(
    State(state): State<Shared>,
    Query(v): Query<Version>,
    Json(body): Json<Value>,
) -> Result<Json<Value>, StatusCode> {
    if v.version.as_deref() != Some("3") {
        return Err(StatusCode::BAD_REQUEST);
    }
    state
        .counters
        .config_requests
        .fetch_add(1, Ordering::Relaxed);
    let mut configs = serde_json::Map::new();
    for key in body["publicKeys"].as_array().into_iter().flatten() {
        if let Some(p) = key.as_str().and_then(|k| state.by_key.get(k)) {
            configs.insert(p.public_key.clone(), projects::project_config(p));
        }
    }
    let mut out = json!({"configs": configs, "pending": [], "unchanged": []});
    if body["global"].as_bool() == Some(true) {
        out["global"] = state.global.clone();
    }
    Ok(Json(out))
}

async fn outcomes(State(state): State<Shared>, Json(body): Json<Value>) -> Json<Value> {
    let c = &state.counters;
    c.outcome_requests.fetch_add(1, Ordering::Relaxed);
    let mut counts = c.outcomes.lock().unwrap();
    for o in body["outcomes"].as_array().into_iter().flatten() {
        let key = format!(
            "{}:{}",
            outcome_name(o["outcome"].as_u64().unwrap_or(99)),
            o["reason"].as_str().unwrap_or("-")
        );
        *counts.entry(key).or_default() += o["quantity"].as_u64().unwrap_or(1);
    }
    Json(json!({}))
}

fn outcome_name(code: u64) -> &'static str {
    match code {
        0 => "accepted",
        1 => "filtered",
        2 => "rate_limited",
        3 => "invalid",
        4 => "abuse",
        5 => "client_discard",
        6 => "cardinality_limited",
        _ => "other",
    }
}

async fn envelope(
    State(state): State<Shared>,
    headers: HeaderMap,
    body: Bytes,
) -> (StatusCode, Json<Value>) {
    let c = &state.counters;
    c.bytes_compressed
        .fetch_add(body.len() as u64, Ordering::Relaxed);
    let data = match headers
        .get("content-encoding")
        .and_then(|v| v.to_str().ok())
    {
        Some("zstd") => match zstd::decode_all(&body[..]) {
            Ok(d) => d,
            Err(_) => {
                c.bad_envelopes.fetch_add(1, Ordering::Relaxed);
                return (StatusCode::BAD_REQUEST, Json(json!({"detail": "bad zstd"})));
            }
        },
        _ => body.to_vec(),
    };
    c.bytes_uncompressed
        .fetch_add(data.len() as u64, Ordering::Relaxed);
    match item_types(&data) {
        Some(types) => {
            c.envelopes.fetch_add(1, Ordering::Relaxed);
            let mut items = c.items.lock().unwrap();
            for (ty, n) in types {
                *items.entry(ty).or_default() += n;
            }
            (StatusCode::OK, Json(json!({})))
        }
        None => {
            c.bad_envelopes.fetch_add(1, Ordering::Relaxed);
            (
                StatusCode::BAD_REQUEST,
                Json(json!({"detail": "bad envelope"})),
            )
        }
    }
}

/// Item types in an envelope and how many each holds (`item_count` for
/// containers such as logs, otherwise 1).
fn item_types(data: &[u8]) -> Option<Vec<(String, u64)>> {
    let line = |rest: &[u8]| rest.iter().position(|&b| b == b'\n').unwrap_or(rest.len());
    let mut rest = data;
    let end = line(rest);
    rest = rest.get(end + 1..).unwrap_or(&[]);
    let mut out = Vec::new();
    while !rest.iter().all(u8::is_ascii_whitespace) {
        let end = line(rest);
        let header: Value = serde_json::from_slice(&rest[..end]).ok()?;
        rest = rest.get(end + 1..).unwrap_or(&[]);
        let len = match header["length"].as_u64() {
            Some(len) => len as usize,
            None => line(rest),
        };
        rest = rest.get(len..)?;
        if rest.first() == Some(&b'\n') {
            rest = &rest[1..];
        }
        let ty = header["type"].as_str()?.to_owned();
        out.push((ty, header["item_count"].as_u64().unwrap_or(1)));
    }
    Some(out)
}

async fn stats(State(state): State<Shared>) -> Json<Value> {
    let c = &state.counters;
    let load = |a: &AtomicU64| a.load(Ordering::Relaxed);
    Json(json!({
        "envelopes": load(&c.envelopes),
        "bad_envelopes": load(&c.bad_envelopes),
        "bytes_compressed": load(&c.bytes_compressed),
        "bytes_uncompressed": load(&c.bytes_uncompressed),
        "config_requests": load(&c.config_requests),
        "outcome_requests": load(&c.outcome_requests),
        "items": *c.items.lock().unwrap(),
        "outcomes": *c.outcomes.lock().unwrap(),
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counts_items_with_and_without_length() {
        let env = b"{\"event_id\":\"x\"}\n{\"type\":\"event\",\"length\":7}\n{\"a\":1}\n{\"type\":\"log\",\"item_count\":12,\"length\":2}\n{}\n{\"type\":\"attachment\"}\nabc\n";
        assert_eq!(
            item_types(env).unwrap(),
            [
                ("event".into(), 1),
                ("log".into(), 12),
                ("attachment".into(), 1)
            ]
        );
        assert!(item_types(b"{}\n{\"type\":\"event\",\"length\":99}\nshort").is_none());
        assert_eq!(item_types(b"{}\n").unwrap(), []);
    }
}
