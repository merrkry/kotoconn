use std::future::Future;

/// Run owned native work on Tokio. Dropping the waiter cancels the native task.
/// JS values are converted by the caller after the result returns to its thread.
pub(crate) async fn run<T: Send + 'static>(
    future: impl Future<Output = T> + Send + 'static,
) -> rquickjs::Result<T> {
    tokio_util::task::AbortOnDropHandle::new(tokio::spawn(future))
        .await
        .map_err(|error| {
            rquickjs::Error::new_from_js_message("native task", "result", error.to_string())
        })
}
