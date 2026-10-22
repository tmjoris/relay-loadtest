//! Projects and their configs, derived from a count so that the fake
//! upstream and the blaster agree without talking to each other.

use serde_json::{Value, json};

#[derive(Clone, Debug)]
pub struct Project {
    pub id: u64,
    /// The DSN public key: 32 hex characters.
    pub public_key: String,
}

/// Projects 1..=n with stable keys.
pub fn projects(n: u64) -> Vec<Project> {
    (1..=n)
        .map(|id| Project {
            id,
            public_key: format!(
                "{:032x}",
                id.wrapping_mul(0x9e37_79b9_7f4a_7c15) as u128 | (1u128 << 124)
            ),
        })
        .collect()
}

/// A project config like the "full" one in Relay's integration tests
/// (`mini_sentry.full_project_config`): data scrubbing and PII rules on,
/// inbound filters on, so Relay does the per-event work it does for a real
/// customer project.
pub fn project_config(project: &Project) -> Value {
    json!({
        "projectId": project.id,
        "slug": format!("project-{}", project.id),
        "organizationId": 1,
        "publicKeys": [{
            "publicKey": project.public_key,
            "numericId": project.id,
            "isEnabled": true,
        }],
        "rev": format!("rev-{}", project.id),
        "disabled": false,
        "config": {
            "allowedDomains": ["*"],
            "trustedRelays": [],
            "trustedRelaySettings": {},
            "piiConfig": {
                "rules": {},
                "applications": {
                    "$string": ["@email", "@mac", "@creditcard", "@userpath"],
                    "$object": ["@password"],
                },
            },
            "excludeFields": [],
            "scrubIpAddresses": true,
            "sensitiveFields": ["api_key", "session_token"],
            "scrubDefaults": true,
            "scrubData": true,
            "filterSettings": {
                "browserExtensions": {"isEnabled": true},
                "localhost": {"isEnabled": true},
                "webCrawlers": {"isEnabled": true},
                "legacyBrowsers": {"isEnabled": true, "options": ["ie_pre_9"]},
            },
            "groupingConfig": {
                "id": "newstyle:2023-01-11",
                "enhancements": "eJybzDhxY05qemJypZWRgaGlroGxrqHRBABbEwcC",
            },
            "blacklistedIps": ["127.43.33.22"],
        },
    })
}

/// The global config Relay's integration tests use (`GLOBAL_CONFIG` in
/// `mini_sentry.py`).
pub fn global_config() -> Value {
    let measurements: Vec<Value> = [
        ("app_start_cold", "millisecond"),
        ("app_start_warm", "millisecond"),
        ("cls", "none"),
        ("fcp", "millisecond"),
        ("fid", "millisecond"),
        ("fp", "millisecond"),
        ("frames_frozen_rate", "ratio"),
        ("frames_frozen", "none"),
        ("frames_slow_rate", "ratio"),
        ("frames_slow", "none"),
        ("frames_total", "none"),
        ("inp", "millisecond"),
        ("lcp", "millisecond"),
        ("stall_count", "none"),
        ("stall_longest_time", "millisecond"),
        ("stall_percentage", "ratio"),
        ("stall_total_time", "millisecond"),
        ("ttfb.requesttime", "millisecond"),
        ("ttfb", "millisecond"),
        ("time_to_full_display", "millisecond"),
        ("time_to_initial_display", "millisecond"),
    ]
    .iter()
    .map(|(name, unit)| json!({"name": name, "unit": unit}))
    .collect();
    json!({
        "measurements": {"builtinMeasurements": measurements, "maxCustomMeasurements": 10},
        "filters": {"version": 1, "filters": []},
        "options": {
            "relay.span-usage-metric": true,
            "relay.session.processing.rollout": 1.0,
            "relay.endpoint-fetch-config.enabled": true,
        },
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keys_are_stable_unique_hex() {
        let a = projects(100);
        assert_eq!(a[0].public_key, projects(1)[0].public_key);
        assert!(a.iter().all(
            |p| p.public_key.len() == 32 && p.public_key.bytes().all(|b| b.is_ascii_hexdigit())
        ));
        let mut keys: Vec<_> = a.iter().map(|p| &p.public_key).collect();
        keys.sort();
        keys.dedup();
        assert_eq!(keys.len(), 100);
    }
}
