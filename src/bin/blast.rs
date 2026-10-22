//! Sends envelopes to Relay at a target rate and reports what happened.
//!
//!     blast [--relay http://127.0.0.1:3000] [--rate 2000] [--duration 30]
//!           [--concurrency 64] [--projects 100] [--pool 5000]
//!           [--mix errors=30,transactions=30,sessions=25,check_ins=5,logs=10]
//!
//! The envelope pool is built once, then replayed. Envelope `i` is due at
//! `start + i / rate`; with `--rate 0` every sender goes as fast as Relay
//! answers. Each sender records response latency in its own DDSketch and
//! the sketches are merged at the end, so percentiles stay within 1%.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use clap::Parser;
use funnel_sketch::DDSketch;
use relay_loadtest::envelopes::{self, Mix, Prepared};
use relay_loadtest::projects;
use serde_json::json;

#[derive(Parser)]
struct Args {
    #[arg(long, default_value = "http://127.0.0.1:3000")]
    relay: String,
    /// Envelopes per second; 0 sends as fast as Relay answers.
    #[arg(long, default_value_t = 1000)]
    rate: u64,
    #[arg(long, default_value_t = 30)]
    duration: u64,
    /// Requests in flight at once.
    #[arg(long, default_value_t = 64)]
    concurrency: usize,
    #[arg(long, default_value_t = 100)]
    projects: u64,
    /// Distinct envelopes to build before sending.
    #[arg(long, default_value_t = 5000)]
    pool: usize,
    #[arg(long, default_value_t = 7)]
    seed: u64,
    #[arg(
        long,
        default_value = "errors=30,transactions=30,sessions=25,check_ins=5,logs=10"
    )]
    mix: String,
}

fn parse_mix(s: &str) -> anyhow::Result<Mix> {
    let mut mix = Mix {
        errors: 0,
        transactions: 0,
        sessions: 0,
        check_ins: 0,
        logs: 0,
    };
    for part in s.split(',').filter(|p| !p.is_empty()) {
        let (k, v) = part
            .split_once('=')
            .ok_or_else(|| anyhow::anyhow!("mix entries look like errors=30"))?;
        let v: u32 = v.parse()?;
        match k {
            "errors" => mix.errors = v,
            "transactions" => mix.transactions = v,
            "sessions" => mix.sessions = v,
            "check_ins" => mix.check_ins = v,
            "logs" => mix.logs = v,
            _ => anyhow::bail!("unknown kind {k}"),
        }
    }
    Ok(mix)
}

#[derive(Default)]
struct Tally {
    statuses: BTreeMap<u16, u64>,
    errors: u64,
    bytes: u64,
    kinds: BTreeMap<&'static str, u64>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let mix = parse_mix(&args.mix)?;
    let built = Instant::now();
    let projects = projects::projects(args.projects);
    let pool: Arc<Vec<Prepared>> = Arc::new(envelopes::pool(
        &projects,
        mix,
        args.pool,
        args.seed,
        chrono::Utc::now(),
    ));
    let pool_bytes: usize = pool.iter().map(|e| e.body.len()).sum();
    eprintln!(
        "built {} envelopes ({:.1} MB, avg {:.1} KB) in {:.1}s",
        pool.len(),
        pool_bytes as f64 / 1e6,
        pool_bytes as f64 / pool.len() as f64 / 1e3,
        built.elapsed().as_secs_f64()
    );

    let client = reqwest::Client::builder()
        .pool_max_idle_per_host(args.concurrency)
        .timeout(Duration::from_secs(30))
        .build()?;
    let next = Arc::new(AtomicU64::new(0));
    let start = Instant::now();
    let deadline = start + Duration::from_secs(args.duration);
    let mut tasks = Vec::new();
    for _ in 0..args.concurrency {
        let (client, pool, next) = (client.clone(), pool.clone(), next.clone());
        let base = args.relay.trim_end_matches('/').to_owned();
        let rate = args.rate;
        tasks.push(tokio::spawn(async move {
            let mut latency = DDSketch::default();
            let mut tally = Tally::default();
            loop {
                let i = next.fetch_add(1, Ordering::Relaxed);
                if rate > 0 {
                    let due = start + Duration::from_secs_f64(i as f64 / rate as f64);
                    if due >= deadline {
                        break;
                    }
                    tokio::time::sleep_until(due.into()).await;
                } else if Instant::now() >= deadline {
                    break;
                }
                let e = &pool[i as usize % pool.len()];
                let sent = Instant::now();
                let result = client
                    .post(format!("{base}/api/{}/envelope/", e.project_id))
                    .header("content-type", "application/x-sentry-envelope")
                    .header(
                        "x-sentry-auth",
                        format!(
                            "Sentry sentry_version=7, sentry_key={}, sentry_client=relay-loadtest/0.1",
                            e.public_key
                        ),
                    )
                    .body(e.body.clone())
                    .send()
                    .await;
                match result {
                    Ok(resp) => {
                        *tally.statuses.entry(resp.status().as_u16()).or_default() += 1;
                        let _ = resp.bytes().await;
                        latency.add(sent.elapsed().as_secs_f64() * 1e3);
                        tally.bytes += e.body.len() as u64;
                        *tally.kinds.entry(e.kind).or_default() += 1;
                    }
                    Err(_) => tally.errors += 1,
                }
            }
            (latency, tally)
        }));
    }

    let mut latency = DDSketch::default();
    let mut total = Tally::default();
    for t in tasks {
        let (l, tally) = t.await?;
        latency.merge(&l).expect("same sketch config");
        for (s, n) in tally.statuses {
            *total.statuses.entry(s).or_default() += n;
        }
        for (k, n) in tally.kinds {
            *total.kinds.entry(k).or_default() += n;
        }
        total.errors += tally.errors;
        total.bytes += tally.bytes;
    }
    let secs = start.elapsed().as_secs_f64();
    let answered: u64 = total.statuses.values().sum();
    let ok: u64 = total
        .statuses
        .iter()
        .filter(|(s, _)| (200..300).contains(*s))
        .map(|(_, n)| n)
        .sum();
    let q = |p: f64| latency.quantile(p).map(|v| (v * 100.0).round() / 100.0);
    let report = json!({
        "seconds": (secs * 100.0).round() / 100.0,
        "sent": answered + total.errors,
        "accepted": ok,
        "accepted_per_sec": (ok as f64 / secs).round(),
        "mb_per_sec": (total.bytes as f64 / secs / 1e5).round() / 10.0,
        "statuses": total.statuses,
        "connection_errors": total.errors,
        "latency_ms": {"p50": q(0.5), "p90": q(0.9), "p99": q(0.99), "p999": q(0.999), "max": latency.max()},
        "kinds": total.kinds,
    });
    println!("{}", serde_json::to_string_pretty(&report)?);
    Ok(())
}
