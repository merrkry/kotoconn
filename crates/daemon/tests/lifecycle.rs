use kotoconn_config::ResolveHandlerId;
use kotoconn_daemon::{Daemon, Error, Policy, Shutdown};
use kotoconn_script::ModuleSource;
use std::{collections::HashMap, time::Duration};

const TIMEOUT: Duration = Duration::from_secs(3);

async fn start(source: &str, grace: Duration) -> Daemon {
    tokio::time::timeout(
        TIMEOUT,
        Daemon::start_with_sources(
            "main.ts".into(),
            HashMap::from([("main.ts".into(), source.into())]),
            grace,
        ),
    )
    .await
    .unwrap()
    .unwrap()
}

fn handler(policy: &Policy) -> ResolveHandlerId {
    *policy.config().resolve_handlers.iter().next().unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shutdown_drains_accepted_calls_and_closes_existing_handles() {
    fn shared<T: Send + Sync>() {}
    shared::<Policy>();
    shared::<Daemon>();
    let (entered, waiting) = tokio::sync::oneshot::channel();
    let (release, resume) = std::sync::mpsc::channel();
    let sources = GatedSource {
        entered: Some(entered),
        resume,
        modules: HashMap::from([
            (
                "main.ts".into(),
                r#"
                import { kotoconn as k } from '@kotoconn/bindings';
                k.resolve_handler(async () => {
                    await import('./gate.ts');
                    return [k.ip('127.0.0.1')];
                });
            "#
                .into(),
            ),
            ("gate.ts".into(), "export {};".into()),
        ]),
    };
    let daemon = Daemon::start_with_sources("main.ts".into(), sources, TIMEOUT)
        .await
        .unwrap();
    let policy = daemon.policy().clone();
    let id = handler(&policy);
    let request = tokio::spawn({
        let policy = policy.clone();
        async move { policy.resolve(id, "work".into()).await }
    });
    tokio::time::timeout(TIMEOUT, waiting)
        .await
        .unwrap()
        .unwrap();

    // Hold an accepted call at a controlled source read until shutdown is requested.
    let shutdown = daemon.shutdown();
    tokio::pin!(shutdown);
    tokio::select! {
        biased;
        result = &mut shutdown => panic!("shutdown skipped an active call: {result:?}"),
        _ = std::future::ready(()) => {}
    }
    assert!(matches!(
        policy.resolve(id, "work".into()).await,
        Err(Error::Closed)
    ));
    release.send(()).unwrap();
    assert_eq!(
        tokio::time::timeout(TIMEOUT, shutdown)
            .await
            .unwrap()
            .unwrap(),
        Shutdown::Drained
    );
    assert_eq!(request.await.unwrap().unwrap().len(), 1);
    assert!(matches!(
        policy.resolve(id, "work".into()).await,
        Err(Error::Closed)
    ));
    assert_eq!(daemon.shutdown().await.unwrap(), Shutdown::Drained);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn deadline_cancels_unsettled_promises_and_interrupts_running_js() {
    for body in ["await new Promise(() => {});", "while (true) {}"] {
        let source = format!(
            r#"
            import {{ kotoconn as k }} from '@kotoconn/bindings';
            k.resolve_handler(async () => {{ {body} return []; }});
        "#
        );
        let daemon = start(&source, Duration::from_millis(30)).await;
        let policy = daemon.policy().clone();
        let id = handler(&policy);
        let request = policy.resolve(id, "work".into());
        tokio::pin!(request);
        // Poll once to enqueue the call before shutdown, without a scheduling delay.
        tokio::select! {
            biased;
            result = &mut request => panic!("handler unexpectedly completed: {result:?}"),
            _ = std::future::ready(()) => {}
        }
        assert_eq!(
            tokio::time::timeout(TIMEOUT, daemon.shutdown())
                .await
                .unwrap()
                .unwrap(),
            Shutdown::TimedOut
        );
        assert!(request.await.is_err());
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn file_sources_load_imports_and_preserve_startup_errors() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().join("policy");
    std::fs::create_dir(&root).unwrap();
    let entry = root.join("main.ts");
    std::fs::write(root.join("value.ts"), "export const value: number = 42;").unwrap();
    std::fs::write(
        &entry,
        "import { value } from './value.ts'; if (value !== 42) throw Error('wrong import');",
    )
    .unwrap();
    let daemon = Daemon::start(&entry, TIMEOUT).await.unwrap();
    assert_eq!(daemon.shutdown().await.unwrap(), Shutdown::Drained);
    drop(daemon);

    std::fs::write(root.join("value.ts"), "throw Error('startup failed');").unwrap();
    std::fs::write(&entry, "import './value.ts';").unwrap();
    let error = Daemon::start(&entry, TIMEOUT).await.err().unwrap();
    assert!(error.to_string().contains("startup failed"), "{error}");

    #[cfg(unix)]
    {
        let outside = directory.path().join("outside.ts");
        std::fs::write(&outside, "export {};").unwrap();
        std::os::unix::fs::symlink(outside, root.join("link.ts")).unwrap();
        std::fs::write(&entry, "import './link.ts';").unwrap();
        let error = Daemon::start(&entry, TIMEOUT).await.err().unwrap();
        assert!(
            error.to_string().contains("outside the source root"),
            "{error}"
        );
    }
}

// The sender lives in the test, so unwinding also releases a blocked source read.
struct GatedSource {
    entered: Option<tokio::sync::oneshot::Sender<()>>,
    resume: std::sync::mpsc::Receiver<()>,
    modules: HashMap<String, String>,
}

impl ModuleSource for GatedSource {
    fn read(&mut self, name: &str) -> std::io::Result<String> {
        if name == "gate.ts"
            && let Some(entered) = self.entered.take()
        {
            let _ = entered.send(());
            self.resume.recv().map_err(std::io::Error::other)?;
        }
        self.modules.read(name)
    }
}
