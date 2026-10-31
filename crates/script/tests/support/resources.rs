// A resource manager that records creations instead of starting native resources.
#![allow(dead_code)]

use kotoconn_config::*;
use kotoconn_script::{ModuleSource, NativeFuture, Resources, Script};
use std::{
    collections::HashMap,
    num::NonZeroU64,
    sync::{Arc, Mutex},
    time::Duration,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Ending {
    Abort,
    Drain(Duration),
}

#[derive(Default)]
pub struct Recorder {
    created: Mutex<Vec<(ResourceId, ResourceConfig)>>,
    ended: Mutex<HashMap<ResourceId, Ending>>,
}

impl Recorder {
    pub fn created(&self) -> Vec<(ResourceId, ResourceConfig)> {
        self.created.lock().unwrap().clone()
    }

    pub fn ended(&self, id: ResourceId) -> Option<Ending> {
        self.ended.lock().unwrap().get(&id).copied()
    }

    /// The resolve handler of the first dialer.
    pub fn resolve_handler(&self) -> ResolveHandler {
        self.created()
            .into_iter()
            .find_map(|(_, config)| match config {
                ResourceConfig::Dialer(dialer) => Some(dialer.outbound.resolve_handler),
                _ => None,
            })
            .expect("a dialer was created")
    }

    /// Forget recorded configurations, dropping their handler references.
    pub fn clear(&self) {
        self.created.lock().unwrap().clear();
    }
}

impl Resources for Recorder {
    /// Rejects references to resources that were never created or have ended.
    fn create(&self, config: ResourceConfig) -> NativeFuture<Result<ResourceId, String>> {
        let referenced = match &config {
            ResourceConfig::Dialer(DialerConfig {
                carrier: CarrierConfig::Dialer(id),
                ..
            }) => Some(ResourceId::from(*id)),
            ResourceConfig::DnsService(service) => Some(service.dialer.into()),
            _ => None,
        };
        let open = |id| {
            self.created
                .lock()
                .unwrap()
                .iter()
                .any(|(created, _)| *created == id)
                && !self.ended.lock().unwrap().contains_key(&id)
        };
        if referenced.is_some_and(|id| !open(id)) {
            return Box::pin(async { Err("referenced resource is closed".to_owned()) });
        }

        let mut created = self.created.lock().unwrap();
        let id = ResourceId(NonZeroU64::new(created.len() as u64 + 1).unwrap());
        created.push((id, config));
        Box::pin(async move { Ok(id) })
    }

    fn close(&self, id: ResourceId) -> Result<(), String> {
        self.ended.lock().unwrap().insert(id, Ending::Abort);
        Ok(())
    }

    fn drain(&self, id: ResourceId, timeout: Duration) -> Result<(), String> {
        self.ended
            .lock()
            .unwrap()
            .entry(id)
            .or_insert(Ending::Drain(timeout));
        Ok(())
    }

    fn released(&self, _: ResourceId) -> NativeFuture<()> {
        Box::pin(async {})
    }

    fn dns_query(
        &self,
        _: DnsServiceId,
        _: DnsRequest,
    ) -> NativeFuture<Result<DnsResponse, String>> {
        Box::pin(async { Err("DNS queries are not recorded".to_owned()) })
    }
}

pub async fn start(
    sources: impl ModuleSource + 'static,
) -> Result<(Script, Arc<Recorder>), kotoconn_script::Error> {
    let recorder = Arc::new(Recorder::default());
    let script = Script::new(sources, recorder.clone(), || false).await?;
    script.evaluate("main.ts").await?;
    Ok((script, recorder))
}

pub async fn load(source: &str) -> (Script, Arc<Recorder>) {
    start(HashMap::from([("main.ts".to_owned(), source.to_owned())]))
        .await
        .unwrap()
}

/// Registers `handler` as the resolve handler of a dialer, so tests can call it.
pub fn with_resolver(handler: &str) -> String {
    format!(
        "await k.dialer({{carrier: k.host_network({{}}), outbound: {{
            resolve_handler: {handler}, implementation: k.direct_outbound({{}})
        }}}});"
    )
}
