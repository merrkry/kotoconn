use crate::{Activity, Scope};
use anyhow::Result;
use std::{future::Future, time::Duration};

/// Owns a destination-specific UDP session's idle deadline and termination.
/// Forwarding drivers share the activity record, not the lifetime owner.
pub struct UdpSession {
    scope: Scope,
    activity: Activity,
    idle: Duration,
}

impl UdpSession {
    pub fn new(scope: Scope, idle: Duration) -> Self {
        debug_assert!(!idle.is_zero());

        Self {
            scope,
            activity: Activity::default(),
            idle,
        }
    }

    pub fn activity(&self) -> Activity {
        self.activity.clone()
    }

    /// Idle expiry terminates setup and forwarding alike by dropping their future.
    pub async fn run(self, work: impl Future<Output = Result<()>>) -> Result<()> {
        let result = self
            .scope
            .run(async {
                tokio::select! {
                    biased;
                    _ = self.activity.until_idle(self.idle) => {
                        tracing::debug!("UDP session idle timeout");
                        Ok(())
                    }
                    result = work => result,
                }
            })
            .await;

        if let Err(error) = &result
            && !self.scope.is_closed()
        {
            tracing::warn!(error = %format_args!("{error:#}"), "UDP session failed");
        }

        result
    }
}

impl Drop for UdpSession {
    fn drop(&mut self) {
        self.scope.close();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::oneshot;

    #[tokio::test(start_paused = true)]
    async fn idle_and_close_drop_pending_session_work() {
        for explicit_close in [false, true] {
            let scope = Scope::new();
            let session = UdpSession::new(scope.clone(), Duration::from_secs(10));
            let activity = session.activity();
            let (sender, receiver) = oneshot::channel::<()>();
            let mut running = Box::pin(session.run(async move {
                let _guard = sender;
                std::future::pending::<Result<()>>().await
            }));
            assert!(futures_util::poll!(running.as_mut()).is_pending());

            if explicit_close {
                scope.close();
            } else {
                tokio::time::advance(Duration::from_secs(9)).await;
                activity.record();
                tokio::time::advance(Duration::from_secs(1)).await;
                assert!(futures_util::poll!(running.as_mut()).is_pending());
                tokio::time::advance(Duration::from_secs(9)).await;
            }

            let result = running.await;
            assert_eq!(result.is_err(), explicit_close);
            assert!(scope.is_closed());
            assert!(receiver.await.is_err(), "termination retained session work");
        }
    }
}
