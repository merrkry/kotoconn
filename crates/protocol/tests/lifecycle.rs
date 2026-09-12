use anyhow::Result;
use kotoconn_protocol::{Scope, stream_task};
use std::time::Duration;
use tokio::{io::AsyncReadExt, sync::oneshot};

#[tokio::test]
async fn parent_close_interrupts_pending_setup_and_waits_for_resource_drop() -> Result<()> {
    let parent = Scope::new();
    let child = parent.child();
    let sibling = Scope::new();
    let (entered, ready) = oneshot::channel();
    let (released, dropped) = oneshot::channel();
    struct Resource(Option<oneshot::Sender<()>>);
    impl Drop for Resource {
        fn drop(&mut self) {
            let _ = self.0.take().unwrap().send(());
        }
    }
    let mut stream = stream_task(child, async move {
        let _resource = Resource(Some(released));
        let _ = entered.send(());
        std::future::pending().await
    })?;
    ready.await?;
    parent.close();
    tokio::time::timeout(Duration::from_secs(1), parent.wait()).await?;
    dropped.await?;
    assert_eq!(stream.read(&mut [0]).await?, 0);
    assert!(!sibling.is_closed());
    assert!(
        parent
            .spawn(async { panic!("closed scope admitted work") })
            .is_err()
    );
    Ok(())
}

#[tokio::test]
async fn dropping_an_unread_stream_cancels_its_setup_without_closing_siblings() -> Result<()> {
    let parent = Scope::new();
    let child = parent.child();
    let sibling = parent.child();
    let (entered, ready) = oneshot::channel();
    let stream = stream_task(child.clone(), async move {
        let _ = entered.send(());
        std::future::pending().await
    })?;
    ready.await?;
    drop(stream);
    tokio::time::timeout(Duration::from_secs(1), child.wait()).await?;
    assert!(child.is_closed());
    assert!(!parent.is_closed());
    assert!(!sibling.is_closed());
    Ok(())
}
