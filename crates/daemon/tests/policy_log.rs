use kotoconn_daemon::{Daemon, Shutdown};
use std::{
    collections::HashMap,
    fmt::Write,
    sync::{Arc, Mutex},
    time::Duration,
};
use tracing::{Level, Subscriber, field::Field};
use tracing_subscriber::{Layer, layer::Context, prelude::*};

const TIMEOUT: Duration = Duration::from_secs(3);

/// Level and formatted fields of each `policy` event.
#[derive(Clone, Default)]
struct Events(Arc<Mutex<Vec<(Level, String)>>>);

impl<S: Subscriber> Layer<S> for Events {
    fn on_event(&self, event: &tracing::Event<'_>, _: Context<'_, S>) {
        if event.metadata().target() != "policy" {
            return;
        }

        let mut fields = String::new();
        event.record(&mut |field: &Field, value: &dyn std::fmt::Debug| {
            let _ = write!(fields, "{}={value:?} ", field.name());
        });
        self.0
            .lock()
            .unwrap()
            .push((*event.metadata().level(), fields));
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn policy_logs_and_reports_rejections_left_unhandled_after_queued_jobs() {
    let events = Events::default();
    let _subscriber =
        tracing::subscriber::set_default(tracing_subscriber::registry().with(events.clone()));

    let daemon = tokio::time::timeout(
        TIMEOUT,
        Daemon::start_with_sources(
            "main.ts".into(),
            HashMap::from([(
                "main.ts".into(),
                r#"
                import { kotoconn as k } from '@kotoconn/bindings';
                k.resolve_handler(async () => {
                    k.log('info', 'resolving');
                    Promise.reject(Error('lost'));
                    // A handler attached by a later job still observes the rejection.
                    const late = Promise.reject(Error('late'));
                    await null;
                    late.catch(() => {});
                    return [];
                });
            "#
                .into(),
            )]),
            TIMEOUT,
        ),
    )
    .await
    .unwrap()
    .unwrap();
    let id = *daemon
        .policy()
        .config()
        .resolve_handlers
        .iter()
        .next()
        .unwrap();

    daemon.policy().resolve(id, "example".into()).await.unwrap();
    // Shutdown drains the job queue and the native tasks that report rejections.
    assert_eq!(daemon.shutdown().await.unwrap(), Shutdown::Drained);

    let events = events.0.lock().unwrap().clone();
    assert_eq!(events.len(), 2, "{events:?}");
    assert_eq!(events[0], (Level::INFO, "message=resolving ".into()));
    assert_eq!(events[1].0, Level::WARN);
    assert!(
        events[1].1.contains("unhandled promise rejection"),
        "{events:?}"
    );
    assert!(events[1].1.contains("lost"), "{events:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn describing_a_reason_can_reject_or_handle_promises() {
    let events = Events::default();
    let _subscriber =
        tracing::subscriber::set_default(tracing_subscriber::registry().with(events.clone()));
    let daemon = Daemon::start_with_sources(
        "main.ts".into(),
        HashMap::from([(
            "main.ts".into(),
            r#"
            import { kotoconn as k } from '@kotoconn/bindings';
            k.resolve_handler(() => {
                Promise.reject({
                    toString() {
                        Promise.reject('nested rejection');
                        return 'outer rejection';
                    },
                });

                let reject;
                const handled = new Promise((_, fail) => reject = fail);
                reject({
                    toString() {
                        handled.catch(() => {});
                        return 'handled rejection';
                    },
                });
                return [];
            });
            "#
            .into(),
        )]),
        TIMEOUT,
    )
    .await
    .unwrap();
    let id = *daemon
        .policy()
        .config()
        .resolve_handlers
        .iter()
        .next()
        .unwrap();
    daemon.policy().resolve(id, "example".into()).await.unwrap();
    daemon.shutdown().await.unwrap();

    let events = events.0.lock().unwrap();
    let reported = |reason| events.iter().any(|(_, fields)| fields.contains(reason));
    assert!(reported("nested rejection"), "{events:?}");
    assert!(reported("outer rejection"), "{events:?}");
    assert!(!reported("handled rejection"), "{events:?}");
}
