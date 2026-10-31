//! Reports promise rejections that no handler has observed once the job queue drains.
//! During startup they are collected instead, so they can fail startup.

use rquickjs::{Ctx, Exception, Value, convert::Coerced, runtime::RejectionTracker};
use std::{
    cell::{Cell, RefCell},
    future::poll_fn,
    hash::{DefaultHasher, Hash, Hasher},
    rc::Rc,
    task::{Poll, Waker},
};

struct Rejections {
    // Promise identities rather than references, so tracking never keeps a promise alive.
    // A collected promise's identity can be reused; only its newest entry can belong
    // to a live promise, so a handled notification removes that one.
    pending: RefCell<Vec<Rejection>>,
    scheduled: Cell<bool>,
    /// Unhandled reasons while startup runs; `None` once it has finished.
    startup: RefCell<Option<Vec<String>>>,
    /// Woken once scheduled reports have run.
    settled: RefCell<Vec<Waker>>,
}

struct Rejection {
    promise: u64,
    /// `None` while the reason is being described.
    reason: Option<String>,
}

#[derive(Clone)]
pub(crate) struct Tracker(Rc<Rejections>);

impl Tracker {
    pub fn new() -> Self {
        Self(Rc::new(Rejections {
            pending: RefCell::default(),
            scheduled: Cell::new(false),
            startup: RefCell::new(Some(Vec::new())),
            settled: RefCell::default(),
        }))
    }

    /// Completes once the rejections reported so far have been reported or collected.
    /// The report runs as a JS job, so the caller must keep the runtime driven.
    pub async fn settled(&self) {
        poll_fn(|cx| {
            if self.0.scheduled.get() {
                self.0.settled.borrow_mut().push(cx.waker().clone());
                Poll::Pending
            } else {
                Poll::Ready(())
            }
        })
        .await;
    }

    /// Ends startup and returns the reasons collected until now; later ones are logged.
    pub fn finish_startup(&self) -> Vec<String> {
        self.0.startup.take().unwrap_or_default()
    }

    pub fn handler(&self) -> RejectionTracker {
        let rejections = self.0.clone();

        Box::new(move |ctx, promise, reason, handled| {
            let id = identity(&promise);

            if handled {
                let mut pending = rejections.pending.borrow_mut();
                if let Some(index) = pending.iter().rposition(|entry| entry.promise == id) {
                    pending.remove(index);
                }
                return;
            }

            // Describing can run JS, such as a `toString` that rejects another promise or
            // handles this one, so the entry exists before and is not borrowed during it.
            rejections.pending.borrow_mut().push(Rejection {
                promise: id,
                reason: None,
            });
            let reason = describe(&ctx, reason);
            if let Some(entry) = rejections
                .pending
                .borrow_mut()
                .iter_mut()
                .rev()
                .find(|entry| entry.promise == id && entry.reason.is_none())
            {
                entry.reason = Some(reason);
            }

            if !rejections.scheduled.replace(true) {
                let rejections = rejections.clone();
                let jobs = ctx.clone();
                ctx.spawn(async move {
                    // Queued jobs can still attach handlers, as `await` does after a yield.
                    while jobs.execute_pending_job() {}

                    rejections.scheduled.set(false);
                    for entry in rejections.pending.take() {
                        debug_assert!(entry.reason.is_some(), "described before jobs run");
                        let reason = entry.reason.unwrap_or_default();

                        match rejections.startup.borrow_mut().as_mut() {
                            Some(reasons) => reasons.push(reason),
                            None => {
                                tracing::warn!(target: "policy", %reason, "unhandled promise rejection")
                            }
                        }
                    }

                    for waker in rejections.settled.take() {
                        waker.wake();
                    }
                });
            }
        })
    }
}

fn identity(promise: &Value<'_>) -> u64 {
    let mut hasher = DefaultHasher::new();
    promise.hash(&mut hasher);
    hasher.finish()
}

fn describe<'js>(ctx: &Ctx<'js>, reason: Value<'js>) -> String {
    if let Some(exception) = reason.as_object().cloned().and_then(Exception::from_object) {
        return exception.to_string();
    }

    match reason.get::<Coerced<String>>() {
        Ok(Coerced(text)) => text,
        Err(_) => {
            // Conversion can throw, e.g. for an object whose toString throws.
            let _ = ctx.catch();
            "<unprintable value>".into()
        }
    }
}
