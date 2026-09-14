use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};
use tokio::time::Instant;

/// Records successful forwarding without waking the idle timer for each packet.
#[derive(Clone)]
pub struct Activity(Arc<State>);

struct State {
    epoch: Instant,
    last: AtomicU64,
}

impl Default for Activity {
    fn default() -> Self {
        Self(Arc::new(State {
            epoch: Instant::now(),
            last: AtomicU64::new(0),
        }))
    }
}

impl Activity {
    pub fn record(&self) {
        let elapsed = self.0.epoch.elapsed().as_nanos().min(u64::MAX as u128) as u64;
        // Multiple directions can complete a batch concurrently.
        self.0.last.fetch_max(elapsed, Ordering::Relaxed);
    }

    pub async fn until_idle(&self, idle: Duration) {
        loop {
            let deadline =
                self.0.epoch + Duration::from_nanos(self.0.last.load(Ordering::Relaxed)) + idle;
            if Instant::now() >= deadline {
                return;
            }
            tokio::time::sleep_until(deadline).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(start_paused = true)]
    async fn activity_extends_only_its_own_deadline() {
        let a = Activity::default();
        let b = Activity::default();
        tokio::time::advance(Duration::from_secs(9)).await;
        a.record();
        b.until_idle(Duration::from_secs(10)).await;
        assert_eq!(b.0.epoch.elapsed(), Duration::from_secs(10));
        a.until_idle(Duration::from_secs(10)).await;
        assert_eq!(a.0.epoch.elapsed(), Duration::from_secs(19));
    }
}
