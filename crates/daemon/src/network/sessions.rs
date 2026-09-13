use anyhow::{Result, anyhow};
use kotoconn_protocol::{Scope, Target, TransportProtocol};
use std::collections::HashMap;
use tokio::sync::{mpsc, oneshot};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct SessionId(pub u64);

#[derive(Clone, Debug)]
pub struct SessionHandle {
    pub id: SessionId,
    pub destination: Target,
    pub protocol: TransportProtocol,
    control: Scope,
}

impl SessionHandle {
    pub fn close(&self) {
        self.control.close();
    }

    pub async fn wait(&self) {
        self.control.wait().await;
    }
}

enum Command {
    Register(
        Target,
        TransportProtocol,
        Scope,
        mpsc::UnboundedSender<Command>,
        oneshot::Sender<Registration>,
    ),
    Remove(SessionId),
    List(oneshot::Sender<Vec<SessionHandle>>),
}

#[derive(Clone)]
pub(crate) struct Sessions {
    tx: mpsc::UnboundedSender<Command>,
}

pub(super) struct Registration {
    pub(super) id: SessionId,
    tx: mpsc::UnboundedSender<Command>,
}

impl Drop for Registration {
    fn drop(&mut self) {
        let _ = self.tx.send(Command::Remove(self.id));
    }
}

impl Sessions {
    pub fn new() -> (Self, impl Future<Output = ()> + Send) {
        let (tx, mut rx) = mpsc::unbounded_channel();

        (Self { tx }, async move {
            let mut sessions = HashMap::new();
            let mut next = 0;

            while let Some(command) = rx.recv().await {
                match command {
                    Command::Register(destination, protocol, control, tx, reply) => {
                        next += 1;
                        let handle = SessionHandle {
                            id: SessionId(next),
                            destination,
                            protocol,
                            control,
                        };
                        sessions.insert(handle.id, handle.clone());
                        let _ = reply.send(Registration { id: handle.id, tx });
                    }
                    Command::Remove(id) => {
                        sessions.remove(&id);
                    }
                    Command::List(reply) => {
                        let _ = reply.send(sessions.values().cloned().collect());
                    }
                }
            }
        })
    }

    pub(super) async fn register(
        &self,
        destination: Target,
        protocol: TransportProtocol,
        scope: Scope,
    ) -> Result<Registration> {
        let (tx, rx) = oneshot::channel();

        self.tx.send(Command::Register(
            destination,
            protocol,
            scope,
            self.tx.clone(),
            tx,
        ))?;

        Ok(rx.await?)
    }

    pub async fn list(&self) -> Result<Vec<SessionHandle>> {
        let (tx, rx) = oneshot::channel();

        self.tx
            .send(Command::List(tx))
            .map_err(|_| anyhow!("session registry closed"))?;

        Ok(rx.await?)
    }
}
