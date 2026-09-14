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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn close_revokes_an_established_stream_even_when_its_handle_is_idle() -> Result<()> {
    use tokio::io::AsyncWriteExt;
    let scope = Scope::new();
    let (socket, mut peer) = tokio::io::duplex(16);
    let mut stream = stream_task(scope.clone(), async move {
        Ok(Box::pin(socket) as kotoconn_protocol::BoxStream)
    })?;
    stream.write_all(b"ready").await?;
    let mut message = [0; 5];
    peer.read_exact(&mut message).await?;
    assert_eq!(&message, b"ready");

    scope.close();
    tokio::time::timeout(Duration::from_secs(1), scope.wait()).await?;
    assert_eq!(peer.read(&mut [0]).await?, 0);
    assert_eq!(stream.read(&mut [0]).await?, 0);
    Ok(())
}
