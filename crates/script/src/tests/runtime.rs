use crate::{Script, task};
use rquickjs::{Function, function::Async};
use std::{cell::Cell, collections::HashMap, time::Duration};
use tokio::sync::oneshot;

struct Pending {
    script: Script,
    started: oneshot::Receiver<()>,
    release: oneshot::Sender<()>,
    finished: oneshot::Receiver<u32>,
    dropped: oneshot::Receiver<()>,
}

struct OnDrop(Option<oneshot::Sender<()>>);

impl Drop for OnDrop {
    fn drop(&mut self) {
        let _ = self.0.take().unwrap().send(());
    }
}

// Install a sample operation without any application resources or daemon code.
async fn pending() -> Pending {
    let script = Script::load(
        "main.ts",
        HashMap::from([("main.ts".into(), "export {};".into())]),
    )
    .await
    .unwrap();
    let (started, start) = oneshot::channel();
    let (release, released) = oneshot::channel();
    let (finish, finished) = oneshot::channel();
    let (drop, dropped) = oneshot::channel();
    let operation = Cell::new(Some((started, released, drop)));
    let finish = Cell::new(Some(finish));
    script
        .context
        .with(|ctx| {
            ctx.globals()
                .set(
                    "native",
                    Function::new(
                        ctx.clone(),
                        Async(move || {
                            let (started, released, drop) = operation.take().unwrap();
                            task::run(async move {
                                let _drop = OnDrop(Some(drop));
                                let _ = started.send(());
                                released.await.unwrap();
                                42_u32
                            })
                        }),
                    )
                    .unwrap(),
                )
                .unwrap();
            ctx.globals()
                .set(
                    "done",
                    Function::new(ctx.clone(), move |value: u32| {
                        let _ = finish.take().unwrap().send(value);
                    })
                    .unwrap(),
                )
                .unwrap();
            ctx.eval::<(), _>("native().then(done);").unwrap();
        })
        .await;
    Pending {
        script,
        started: start,
        release,
        finished,
        dropped,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn idle_drains_unawaited_native_work_and_its_js_continuation() {
    tokio::time::timeout(Duration::from_secs(3), async {
        let Pending {
            script,
            started,
            release,
            finished,
            ..
        } = pending().await;
        tokio::join!(script.idle(), async {
            started.await.unwrap();
            release.send(()).unwrap();
        });
        assert_eq!(finished.await.unwrap(), 42);
    })
    .await
    .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn driver_runs_unawaited_native_work_without_a_handler_call() {
    tokio::time::timeout(Duration::from_secs(3), async {
        let Pending {
            script,
            started,
            release,
            finished,
            ..
        } = pending().await;
        tokio::select! {
            _ = script.drive() => panic!("driver exited while its runtime was alive"),
            value = async {
                started.await.unwrap();
                release.send(()).unwrap();
                finished.await.unwrap()
            } => assert_eq!(value, 42),
        }
    })
    .await
    .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dropping_script_cancels_pending_native_tasks() {
    tokio::time::timeout(Duration::from_secs(3), async {
        let Pending {
            script,
            started,
            release,
            dropped,
            ..
        } = pending().await;
        tokio::select! {
            _ = script.drive() => panic!("driver exited while its runtime was alive"),
            result = started => result.unwrap(),
        }
        drop(script);
        dropped.await.unwrap();
        // Hold the release sender until cancellation is observed.
        drop(release);
    })
    .await
    .unwrap();
}
