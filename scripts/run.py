#!/usr/bin/env python3
"""Load tests a Relay binary in isolation against the fake upstream.

    cargo build --release
    python3 scripts/run.py --relay-bin ~/relay/target/release/relay \\
        [--rates 500,1000,2000,0] [--duration 20] [--relay-cpus 0,1]

For each rate it starts a fresh fake upstream and Relay (managed mode,
credentials generated the way Relay's integration tests do), sends a short
warm-up so every project config is fetched, then runs the blaster and
reports: envelopes Relay accepted per second, its response latency, what
reached the upstream, the outcomes Relay reported (filtered, invalid, ...),
Relay's CPU and its peak memory. Linux only: it reads /proc.
"""
import argparse, json, os, shutil, signal, subprocess, tempfile, threading, time, urllib.request

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
BIN = os.path.join(ROOT, "target", "release")
UPSTREAM, RELAY = "127.0.0.1:7000", "127.0.0.1:3000"


def get(url, timeout=5):
    with urllib.request.urlopen(url, timeout=timeout) as r:
        return json.load(r) if r.headers.get("content-type", "").startswith("application/json") else r.read()


def wait(url, seconds=30):
    deadline = time.time() + seconds
    while time.time() < deadline:
        try:
            get(url, timeout=1)
            return
        except OSError:
            time.sleep(0.1)
    raise SystemExit(f"{url} did not come up")


def cpu_seconds(pid):
    with open(f"/proc/{pid}/stat") as f:
        fields = f.read().rsplit(")", 1)[1].split()
    return (int(fields[11]) + int(fields[12])) / os.sysconf("SC_CLK_TCK")


def rss_mb(pid):
    with open(f"/proc/{pid}/status") as f:
        for line in f:
            if line.startswith("VmRSS:"):
                return int(line.split()[1]) / 1024
    return 0.0


def relay_config(dir, args):
    config = {
        "relay": {"mode": "managed", "upstream": f"http://{UPSTREAM}/",
                  "host": RELAY.split(":")[0], "port": int(RELAY.split(":")[1])},
        "logging": {"level": "warn"},
        "limits": {"max_concurrent_requests": 1000},
        # Post outcomes to the upstream instead of folding them into
        # periodic client reports, so drops show up within the run.
        "outcomes": {"emit_outcomes": True},
    }
    if args.statsd:
        # Relay's own internal metrics (timers, counters) over StatsD.
        config["metrics"] = {"statsd": args.statsd}
    with open(os.path.join(dir, "config.yml"), "w") as f:
        json.dump(config, f)  # YAML is a superset of JSON
    subprocess.run([args.relay_bin, "-c", dir, "credentials", "generate"],
                   check=True, capture_output=True)


def blast(rate, duration, args):
    out = subprocess.run(
        [os.path.join(BIN, "blast"), "--relay", f"http://{RELAY}", "--rate", str(rate),
         "--duration", str(duration), "--concurrency", str(args.concurrency),
         "--projects", str(args.projects), "--mix", args.mix,
         "--user-agents", str(args.user_agents), "--pool", str(args.pool)],
        check=True, capture_output=True, text=True)
    return json.loads(out.stdout)


def run(rate, args):
    dir = tempfile.mkdtemp(prefix="relay-loadtest-")
    procs = []
    try:
        procs.append(subprocess.Popen(
            [os.path.join(BIN, "fake-upstream"), "--listen", UPSTREAM, "--projects", str(args.projects)],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL))
        wait(f"http://{UPSTREAM}/stats")
        relay_config(dir, args)
        pin = ["taskset", "-c", args.relay_cpus] if args.relay_cpus else []
        relay = subprocess.Popen(pin + [args.relay_bin, "-c", dir, "run"],
                                 stdout=subprocess.DEVNULL, stderr=open(os.path.join(dir, "relay.log"), "w"))
        procs.append(relay)
        wait(f"http://{RELAY}/api/relay/healthcheck/ready/")
        blast(200, 3, args)  # warm-up: fetches every project's config
        time.sleep(2)
        # Baseline: Relay's CPU with no traffic at all.
        idle0 = cpu_seconds(relay.pid)
        time.sleep(args.idle)
        idle_cores = (cpu_seconds(relay.pid) - idle0) / args.idle

        up0, cpu0, t0 = get(f"http://{UPSTREAM}/stats"), cpu_seconds(relay.pid), time.time()
        peak = [0.0]
        done = threading.Event()

        def sample():
            while not done.is_set():
                peak[0] = max(peak[0], rss_mb(relay.pid))
                time.sleep(0.2)

        threading.Thread(target=sample, daemon=True).start()
        perf = None
        if args.perf:
            # DWARF unwinding: Rust release builds have no frame pointers,
            # and Relay's release profile keeps full debug info.
            perf = subprocess.Popen(
                ["perf", "record", "-F", str(args.perf_freq), "--call-graph", args.call_graph,
                 "-p", str(relay.pid), "-o", args.perf],
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            time.sleep(1)
        report = blast(rate, args.duration, args)
        if perf:
            perf.send_signal(signal.SIGINT)
            perf.wait()
        cpu_busy = cpu_seconds(relay.pid) - cpu0
        busy_secs = time.time() - t0
        time.sleep(args.drain)  # let Relay finish forwarding and report outcomes
        done.set()
        up1 = get(f"http://{UPSTREAM}/stats")
        items = {k: v - up0["items"].get(k, 0) for k, v in up1["items"].items()}
        outcomes = {k: v - up0["outcomes"].get(k, 0) for k, v in up1["outcomes"].items()}
        return {
            "target": rate,
            "report": report,
            "forwarded_envelopes": up1["envelopes"] - up0["envelopes"],
            "forwarded_items": {k: v for k, v in items.items() if v},
            "outcomes": {k: v for k, v in outcomes.items() if v},
            "upstream_mb": (up1["bytes_compressed"] - up0["bytes_compressed"]) / 1e6,
            "relay_cpu_cores": cpu_busy / busy_secs,
            "relay_idle_cores": idle_cores,
            "relay_rss_mb": peak[0],
        }
    finally:
        for p in reversed(procs):
            p.terminate()
            p.wait()
        if args.keep:
            print("relay dir:", dir)
        else:
            shutil.rmtree(dir, ignore_errors=True)


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--relay-bin", required=True)
    p.add_argument("--rates", default="500,1000,2000,0")
    p.add_argument("--duration", type=int, default=20)
    p.add_argument("--drain", type=int, default=5)
    p.add_argument("--idle", type=int, default=5, help="seconds to measure Relay's idle CPU")
    p.add_argument("--concurrency", type=int, default=64)
    p.add_argument("--projects", type=int, default=100)
    p.add_argument("--mix", default="errors=30,transactions=30,sessions=25,check_ins=5,logs=10")
    p.add_argument("--pool", type=int, default=5000, help="distinct envelopes to replay")
    p.add_argument("--user-agents", type=int, default=4,
                   help="distinct user agents in the traffic, Zipf-distributed")
    p.add_argument("--relay-cpus", default="", help="pin Relay to these CPUs with taskset")
    p.add_argument("--json", action="store_true", help="print full results as JSON")
    p.add_argument("--statsd", default="", help="send Relay's internal metrics here, e.g. 127.0.0.1:8125")
    p.add_argument("--perf", default="", help="record Relay with perf into this file during the run")
    p.add_argument("--perf-freq", type=int, default=499)
    p.add_argument("--call-graph", default="dwarf,16384",
                   help="perf call-graph mode: dwarf,SIZE, lbr (Intel), or fp with a frame-pointer build")
    p.add_argument("--keep", action="store_true", help="keep Relay's config dir and log")
    args = p.parse_args()

    print(f"{os.cpu_count()} CPUs; relay pinned to {args.relay_cpus or 'all'}; "
          f"{args.projects} projects; {args.duration}s per rate; mix {args.mix}")
    print(f"{'target/s':>9} {'accepted/s':>11} {'p50 ms':>7} {'p99 ms':>7} {'non-2xx':>8} "
          f"{'fwd env/s':>10} {'CPU cores':>9} {'idle':>5} {'RSS MB':>7}  outcomes")
    results = []
    for rate in [int(r) for r in args.rates.split(",")]:
        r = run(rate, args)
        results.append(r)
        rep = r["report"]
        bad = sum(n for s, n in rep["statuses"].items() if not s.startswith("2")) + rep["connection_errors"]
        print(f"{rate or 'max':>9} {rep['accepted_per_sec']:>11.0f} {rep['latency_ms']['p50']:>7} "
              f"{rep['latency_ms']['p99']:>7} {bad:>8} {r['forwarded_envelopes'] / rep['seconds']:>10.0f} "
              f"{r['relay_cpu_cores']:>9.2f} {r['relay_idle_cores']:>5.2f} {r['relay_rss_mb']:>7.0f}  "
              f"{r['outcomes'] or '-'}")
    if args.json:
        print(json.dumps(results, indent=2))


if __name__ == "__main__":
    main()
