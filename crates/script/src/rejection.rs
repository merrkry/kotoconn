//! Reports promise rejections that no handler has observed once the job queue drains.

use rquickjs::{Ctx, Exception, Value, convert::Coerced, runtime::RejectionTracker};
use std::{
    cell::{Cell, RefCell},
    hash::{DefaultHasher, Hash, Hasher},
    rc::Rc,
};

#[derive(Default)]
struct Rejections {
    // Promise identities rather than references, so tracking never keeps a promise alive.
    // A collected promise's identity can be reused; only its newest entry can belong
    // to a live promise, so a handled notification removes that one.
    pending: RefCell<Vec<(u64, String)>>,
    scheduled: Cell<bool>,
}

pub(crate) fn tracker() -> RejectionTracker {
    let rejections = Rc::new(Rejections::default());

    Box::new(move |ctx, promise, reason, handled| {
        let id = identity(&promise);

        if handled {
            let mut pending = rejections.pending.borrow_mut();
            if let Some(index) = pending.iter().rposition(|(pending, _)| *pending == id) {
                pending.remove(index);
            }
            return;
        }

        rejections
            .pending
            .borrow_mut()
            .push((id, describe(&ctx, reason)));

        if !rejections.scheduled.replace(true) {
            let rejections = rejections.clone();
            let jobs = ctx.clone();
            ctx.spawn(async move {
                // Queued jobs can still attach handlers, as `await` does after a yield.
                while jobs.execute_pending_job() {}

                rejections.scheduled.set(false);
                for (_, reason) in rejections.pending.take() {
                    tracing::warn!(target: "policy", %reason, "unhandled promise rejection");
                }
            });
        }
    })
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
