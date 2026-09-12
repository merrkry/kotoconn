use anyhow::{Result, bail};
use std::{future::Future, sync::Arc};
use tokio_util::{sync::CancellationToken, task::TaskTracker};

/// Cloneable close handle. Cancellation follows carrier ancestry; completion includes
/// registered descendant tasks. A handle does not own or lock an I/O state machine.
#[derive(Clone, Debug)]
pub struct Scope {
    token: CancellationToken,
    trackers: Arc<Vec<TaskTracker>>,
}
impl Default for Scope {
    fn default() -> Self {
        Self::new()
    }
}
impl Scope {
    pub fn new() -> Self {
        let tracker = TaskTracker::new();
        tracker.close();
        Self {
            token: CancellationToken::new(),
            trackers: Arc::new(vec![tracker]),
        }
    }
    pub fn child(&self) -> Self {
        let mut trackers = self.trackers.as_ref().clone();
        let tracker = TaskTracker::new();
        tracker.close();
        trackers.push(tracker);
        Self {
            token: self.token.child_token(),
            trackers: Arc::new(trackers),
        }
    }
    /// Include this connection's work in the caller's completion notification.
    /// This does not change carrier cancellation ancestry or create a new owner.
    pub fn tracked_by(mut self, caller: &Scope) -> Self {
        let mut trackers = self.trackers.as_ref().clone();
        for tracker in caller.trackers.iter() {
            if !trackers
                .iter()
                .any(|existing| TaskTracker::ptr_eq(existing, tracker))
            {
                trackers.insert(0, tracker.clone());
            }
        }
        self.trackers = Arc::new(trackers);
        self
    }
    pub fn close(&self) {
        self.token.cancel();
    }
    pub fn is_closed(&self) -> bool {
        self.token.is_cancelled()
    }
    pub async fn cancelled(&self) {
        self.token.cancelled().await;
    }
    pub async fn wait(&self) {
        self.trackers.last().expect("scope tracker").wait().await;
    }
    pub async fn run<T>(&self, work: impl Future<Output = Result<T>>) -> Result<T> {
        tokio::select! {
            biased;
            _ = self.cancelled() => bail!("connection closed"),
            result = work => result,
        }
    }
    /// Registers before dispatch so close + wait cannot miss an admitted task.
    pub fn spawn(&self, work: impl Future<Output = Result<()>> + Send + 'static) -> Result<()> {
        let guards: Vec<_> = self.trackers.iter().map(TaskTracker::token).collect();
        if self.is_closed() {
            bail!("connection scope closed");
        }
        let scope = self.clone();
        tokio::spawn(async move {
            let _guards = guards;
            if let Err(error) = scope.run(work).await
                && !scope.is_closed()
            {
                eprintln!("connection: {error:#}");
            }
        });
        Ok(())
    }
}
