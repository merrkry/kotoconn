use kotoconn_daemon::Daemon;
use kotoconn_protocol::Scope;
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Duration,
};
use tracing::{Instrument, Subscriber};
use tracing_subscriber::{Layer, layer::Context, prelude::*, registry::LookupSpan};

type CapturedEvent = (String, Vec<String>);

#[derive(Clone, Default)]
struct Events(Arc<Mutex<Vec<CapturedEvent>>>);

impl<S: Subscriber + for<'a> LookupSpan<'a>> Layer<S> for Events {
    fn on_event(&self, event: &tracing::Event<'_>, ctx: Context<'_, S>) {
        let names = ctx
            .event_scope(event)
            .map(|scope| {
                scope
                    .from_root()
                    .map(|span| span.name().to_owned())
                    .collect()
            })
            .unwrap_or_default();
        self.0
            .lock()
            .unwrap()
            .push((event.metadata().target().to_owned(), names));
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_calls_and_spawned_work_keep_their_parent_spans() {
    let events = Events::default();
    tracing::subscriber::set_global_default(tracing_subscriber::registry().with(events.clone()))
        .unwrap();
    let daemon = Daemon::start_with_sources(
        "main.ts".into(),
        HashMap::from([("main.ts".into(),
            "import { kotoconn as k } from '@kotoconn/bindings'; k.resolve_handler(async () => []);".into())]),
        Duration::from_secs(3),
    ).await.unwrap();
    let id = *daemon
        .policy()
        .config()
        .resolve_handlers
        .iter()
        .next()
        .unwrap();

    let first = tracing::info_span!("first_request");
    let second = tracing::info_span!("second_request");
    let (a, b) = tokio::join!(
        daemon
            .policy()
            .resolve(id, "first.example".into())
            .instrument(first.clone()),
        daemon
            .policy()
            .resolve(id, "second.example".into())
            .instrument(second.clone()),
    );
    a.unwrap();
    b.unwrap();

    let scope = Scope::new();
    for span in [first, second] {
        span.in_scope(|| {
            scope.spawn(async {
                tracing::info!(target: "spawned_work", "task ran");
                Ok(())
            })
        })
        .unwrap();
    }
    scope.wait().await;
    daemon.shutdown().await.unwrap();

    let events = events.0.lock().unwrap();
    for request in ["first_request", "second_request"] {
        assert!(
            events.iter().any(
                |(target, spans)| target == "kotoconn_daemon" && spans == &[request, "resolve"]
            ),
            "{events:?}"
        );
        assert!(
            events
                .iter()
                .any(|(target, spans)| target == "spawned_work" && spans == &[request]),
            "{events:?}"
        );
    }
}
