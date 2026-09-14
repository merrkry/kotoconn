use anyhow::Result;
use hdrhistogram::Histogram;
use serde_json::{Value, json};
use std::time::Duration;

pub struct Stats {
    pub sent_bytes: u64,
    pub received_bytes: u64,
    pub operations: u64,
    pub connections: u64,
    pub sent_datagrams: u64,
    pub received_datagrams: u64,
    pub duplicates: u64,
    pub reordered: u64,
    pub missing_sequence_sample: Vec<u64>,
    pub connect: Histogram<u64>,
    pub first_response: Histogram<u64>,
    pub latency: Histogram<u64>,
    pub scheduled: Histogram<u64>,
}

impl Stats {
    pub fn new() -> Result<Self> {
        Ok(Self {
            sent_bytes: 0,
            received_bytes: 0,
            operations: 0,
            connections: 0,
            sent_datagrams: 0,
            received_datagrams: 0,
            duplicates: 0,
            reordered: 0,
            missing_sequence_sample: Vec::new(),
            connect: Histogram::new(3)?,
            first_response: Histogram::new(3)?,
            latency: Histogram::new(3)?,
            scheduled: Histogram::new(3)?,
        })
    }

    pub fn json(&self, flow: usize, kind: &str, seconds: f64) -> Value {
        json!({
            "flow": flow, "kind": kind, "seconds": seconds,
            "sent_bytes": self.sent_bytes, "received_bytes": self.received_bytes,
            "operations": self.operations, "connections": self.connections,
            "sent_datagrams": self.sent_datagrams, "received_datagrams": self.received_datagrams,
            "lost_datagrams": self.sent_datagrams - self.received_datagrams,
            "duplicates": self.duplicates, "reordered": self.reordered,
            "missing_sequence_sample": self.missing_sequence_sample,
            "connect_us": histogram(&self.connect), "latency_us": histogram(&self.latency),
            "first_response_us": histogram(&self.first_response),
            "scheduled_latency_us": histogram(&self.scheduled),
        })
    }
}

pub fn record(histogram: &mut Histogram<u64>, duration: Duration) -> Result<()> {
    histogram.record(u64::try_from(duration.as_micros())?.max(1))?;
    Ok(())
}

fn histogram(histogram: &Histogram<u64>) -> Value {
    json!({
        "count": histogram.len(), "p50": histogram.value_at_quantile(0.5),
        "p95": histogram.value_at_quantile(0.95), "p99": histogram.value_at_quantile(0.99),
        "max": histogram.max(),
        "buckets": histogram.iter_recorded().map(|v| (v.value_iterated_to(), v.count_since_last_iteration())).collect::<Vec<_>>(),
    })
}
