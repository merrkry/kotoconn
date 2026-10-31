//! Policy log events, emitted through the daemon's tracing subscriber.

use crate::native::invalid;
use rquickjs::{Ctx, FromJs, Result, Value};
use ts_rs::TS;

#[derive(TS)]
#[ts(rename_all = "lowercase")]
pub(crate) enum LogLevel {
    Error,
    Warn,
    Info,
    Debug,
    Trace,
}

impl<'js> FromJs<'js> for LogLevel {
    fn from_js(ctx: &Ctx<'js>, value: Value<'js>) -> Result<Self> {
        match String::from_js(ctx, value)?.as_str() {
            "error" => Ok(Self::Error),
            "warn" => Ok(Self::Warn),
            "info" => Ok(Self::Info),
            "debug" => Ok(Self::Debug),
            "trace" => Ok(Self::Trace),
            _ => Err(invalid("expected error, warn, info, debug or trace")),
        }
    }
}

/// Events use the `policy` target and the span of the call that runs them.
pub(crate) fn log(level: LogLevel, message: &str) {
    match level {
        LogLevel::Error => tracing::error!(target: "policy", "{message}"),
        LogLevel::Warn => tracing::warn!(target: "policy", "{message}"),
        LogLevel::Info => tracing::info!(target: "policy", "{message}"),
        LogLevel::Debug => tracing::debug!(target: "policy", "{message}"),
        LogLevel::Trace => tracing::trace!(target: "policy", "{message}"),
    }
}
