use core::fmt::Display;
use core::future::Future;
use core::num::NonZeroUsize;

use std::collections::{hash_map, HashMap};
use std::sync::Arc;

use anyhow::{bail, Context as _};
use bytes::Bytes;
use futures::{SinkExt as _, StreamExt as _};
use protocol::{ClientOp, ClientOpEncoder, ConnectOptions, InfoOptions, ServerOp, ServerOpDecoder};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::ToSocketAddrs;
use tokio::sync::{mpsc, oneshot, watch};
use tokio::{select, try_join};
use tokio_util::codec::{FramedRead, FramedWrite};
use tracing::{debug, error, warn};

pub mod protocol;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Message {
    pub subject: Bytes,
    pub reply: Bytes,
    pub headers: Bytes,
    pub payload: Bytes,
}

pub enum Command {
    Connect(ConnectOptions),
    Publish {
        subject: Bytes,
        reply: Bytes,
        headers: Bytes,
        payload: Bytes,
    },
    Subscribe {
        subject: Bytes,
        group: Bytes,
        sid: Bytes,
        tx: mpsc::Sender<Message>,
    },
    Unsubscribe {
        sid: Bytes,
        max: Option<NonZeroUsize>,
    },
    Flush,
    ServerInfo(oneshot::Sender<watch::Receiver<Arc<InfoOptions>>>),
    Batch(Box<[Command]>),
}

pub struct Conn<I, O> {
    rx: FramedRead<I, ServerOpDecoder>,
    tx: FramedWrite<O, ClientOpEncoder>,
    info: watch::Sender<Arc<InfoOptions>>,
}

#[derive(Debug)]
enum ConnError {
    ClientCommand(anyhow::Error),
    ReceiveServerOperation(std::io::Error),
    HandleServerOperation(anyhow::Error),
    Flush(std::io::Error),
}

impl core::error::Error for ConnError {}

impl Display for ConnError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConnError::ClientCommand(err) => {
                write!(f, "failed to handle client command: {err:?}")
            }
            ConnError::ReceiveServerOperation(err) => {
                write!(f, "failed to receive server operation: {err:?}")
            }
            ConnError::HandleServerOperation(err) => {
                write!(f, "failed to handle server operation: {err:?}")
            }
            ConnError::Flush(err) => {
                write!(f, "failed to flush: {err:?}")
            }
        }
    }
}

impl Conn<OwnedReadHalf, OwnedWriteHalf> {
    pub async fn connect_tcp(addr: impl ToSocketAddrs) -> anyhow::Result<(Self, Arc<InfoOptions>)> {
        let (tx, mut rx) = protocol::connect_tcp(addr).await?;
        let op = rx
            .next()
            .await
            .context("server stream unexpectedly finished")?;
        let info = match op.context("failed to receive server info")? {
            ServerOp::Info(opts) => opts,
            _ => bail!("server did not send INFO"),
        };
        let info = Arc::new(info);
        let (info_tx, _) = watch::channel(Arc::clone(&info));
        Ok((
            Conn {
                rx,
                tx,
                info: info_tx,
            },
            info,
        ))
    }
}

impl<I, O> Conn<I, O>
where
    I: AsyncRead + Unpin,
    O: AsyncWrite + Unpin,
{
    async fn handle_op(
        &mut self,
        subs: &mut HashMap<Bytes, (mpsc::Sender<Message>, Option<usize>)>,
        op: ServerOp,
    ) -> anyhow::Result<bool> {
        match op {
            ServerOp::Info(opts) => {
                self.info.send_replace(Arc::new(opts));
                Ok(false)
            }
            ServerOp::Msg {
                subject,
                sid,
                reply,
                payload,
            } => {
                let mut sub = match subs.entry(sid) {
                    hash_map::Entry::Occupied(sub) => sub,
                    hash_map::Entry::Vacant(sub) => {
                        bail!("received MSG for unknown sid: {:?}", sub.key());
                    }
                };
                let (tx, max) = sub.get_mut();
                if let Err(_) = tx
                    .send(Message {
                        subject,
                        reply,
                        headers: Bytes::default(),
                        payload,
                    })
                    .await
                {
                    debug!(sid = ?sub.key(), "removing unused subscription");
                    sub.remove();
                    return Ok(false);
                }
                if *max == Some(1) {
                    debug!(sid = ?sub.key(), "removing exhausted subscription");
                    sub.remove();
                    return Ok(false);
                }
                *max = max.map(|max| max.saturating_sub(1));
                Ok(false)
            }
            ServerOp::Hmsg {
                subject,
                sid,
                reply,
                headers,
                payload,
            } => {
                let mut sub = match subs.entry(sid) {
                    hash_map::Entry::Occupied(sub) => sub,
                    hash_map::Entry::Vacant(sub) => {
                        bail!("received HMSG for unknown sid: {:?}", sub.key());
                    }
                };
                let (tx, max) = sub.get_mut();
                if let Err(_) = tx
                    .send(Message {
                        subject,
                        reply,
                        headers,
                        payload,
                    })
                    .await
                {
                    debug!(sid = ?sub.key(), "removing unused subscription");
                    sub.remove();
                    return Ok(false);
                }
                if *max == Some(1) {
                    debug!(sid = ?sub.key(), "removing exhausted subscription");
                    sub.remove();
                    return Ok(false);
                }
                *max = max.map(|max| max.saturating_sub(1));
                Ok(false)
            }
            ServerOp::Ping => {
                self.tx.feed(ClientOp::Pong).await?;
                Ok(true)
            }
            ServerOp::Pong => Ok(false),
            ServerOp::Ok => Ok(false),
            ServerOp::Err => {
                // TODO: Implement
                bail!("received an error")
            }
        }
    }

    async fn handle_command(
        &mut self,
        subs: &mut HashMap<Bytes, (mpsc::Sender<Message>, Option<usize>)>,
        cmd: Command,
    ) -> anyhow::Result<Option<bool>> {
        match cmd {
            Command::ServerInfo(tx) => {
                if let Err(_) = tx.send(self.info.subscribe()) {
                    warn!("server info receiver dropped");
                }
                Ok(None)
            }
            Command::Connect(opts) => {
                self.tx.feed(ClientOp::Connect(opts)).await?;
                Ok(Some(true))
            }
            Command::Subscribe {
                subject,
                group,
                sid,
                tx,
            } => {
                self.tx
                    .feed(ClientOp::Sub {
                        subject,
                        group,
                        sid: sid.clone(),
                    })
                    .await?;
                subs.insert(sid, (tx, None));
                Ok(Some(true))
            }
            Command::Unsubscribe { sid, max } => {
                let mut sub = match subs.entry(sid.clone()) {
                    hash_map::Entry::Occupied(sub) => sub,
                    hash_map::Entry::Vacant(sub) => {
                        bail!(
                            "attempted to unsubscribe from a stream with unknown sid: {:?}",
                            sub.key()
                        );
                    }
                };
                if let Some(n) = max {
                    let (_, max) = sub.get_mut();
                    self.tx
                        .feed(ClientOp::Unsub {
                            sid,
                            max: Bytes::from(n.to_string()),
                        })
                        .await?;
                    *max = Some(n.into());
                } else {
                    self.tx
                        .feed(ClientOp::Unsub {
                            sid,
                            max: Bytes::default(),
                        })
                        .await?;
                    sub.remove();
                }
                Ok(Some(true))
            }
            Command::Publish {
                subject,
                reply,
                headers,
                payload,
            } => {
                let op = if headers.is_empty() {
                    ClientOp::Pub {
                        subject,
                        reply,
                        payload,
                    }
                } else {
                    ClientOp::Hpub {
                        subject,
                        reply,
                        headers,
                        payload,
                    }
                };
                self.tx.feed(op).await?;
                Ok(Some(true))
            }
            Command::Flush => {
                self.tx.flush().await?;
                Ok(Some(false))
            }
            Command::Batch(cmds) => {
                let mut has_data = None;
                for cmd in cmds {
                    if let Some(ok) = Box::pin(self.handle_command(subs, cmd)).await? {
                        has_data = Some(ok);
                    }
                }
                Ok(has_data)
            }
        }
    }

    async fn run(
        &mut self,
        cmds: &mut mpsc::Receiver<Command>,
        errs: mpsc::Sender<ConnError>,
    ) -> anyhow::Result<()> {
        let mut subs = HashMap::default();
        let mut has_data = false;
        loop {
            select! {
                Some(op) = self.rx.next() => {
                    let op = match op {
                        Ok(op) => op,
                        Err(err) => {
                            errs.send(ConnError::ReceiveServerOperation(err)).await?;
                            continue;
                        },
                    };
                    match self.handle_op(&mut subs, op).await {
                        Ok(true) => {
                            has_data = true;
                        },
                        Ok(false) => {},
                        Err(err) => {
                            errs.send(ConnError::HandleServerOperation(err)).await?;
                        },
                    }
                },
                Some(cmd) = cmds.recv() => {
                    match self.handle_command(&mut subs, cmd).await {
                        Ok(Some(true)) => {
                            has_data = true;
                        },
                        Ok(Some(false)) => {
                            has_data = false;
                        },
                        Ok(None) => {},
                        Err(err) => {
                            errs.send(ConnError::ClientCommand(err)).await?;
                        },
                    }
                },
                res = self.tx.flush(), if has_data => {
                    match res {
                        Ok(()) => {
                            has_data = false;
                        },
                        Err(err) => {
                            errs.send(ConnError::Flush(err)).await?;
                        },
                    }
                }
            };
        }
    }
}

pub trait Connector {
    type Error: core::error::Error + Send + Sync + 'static;

    fn connect(
        self,
        opts: &InfoOptions,
    ) -> impl Future<Output = Result<ConnectOptions, Self::Error>>;
}

#[derive(Clone, Debug)]
pub struct Client {
    commands: mpsc::Sender<Command>,
}

impl Client {
    pub async fn new(
        mut conn: Conn<impl AsyncRead + Unpin, impl AsyncWrite + Unpin>,
        opts: ConnectOptions,
    ) -> anyhow::Result<(Self, impl Future<Output = anyhow::Result<()>>)> {
        let (cmd_tx, mut cmd_rx) = mpsc::channel(8192);
        let (err_tx, mut err_rx) = mpsc::channel(64);
        conn.tx
            .feed(ClientOp::Connect(opts))
            .await
            .context("failed to send connect operation")?;
        Ok((Self { commands: cmd_tx }, async move {
            try_join!(conn.run(&mut cmd_rx, err_tx), async move {
                while let Some(err) = err_rx.recv().await {
                    error!(?err, "encountered connection handling error");
                }
                Ok(())
            })?;
            Ok(())
        }))
    }

    #[must_use]
    pub fn commands(&self) -> &mpsc::Sender<Command> {
        &self.commands
    }

    #[must_use]
    pub async fn server_info(&self) -> Option<watch::Receiver<Arc<InfoOptions>>> {
        let (tx, rx) = oneshot::channel();
        self.commands.send(Command::ServerInfo(tx)).await.ok()?;
        rx.await.ok()
    }

    pub async fn subscribe(
        &self,
        subject: impl Into<Bytes>,
        group: impl Into<Bytes>,
        sid: impl Into<Bytes>,
        tx: mpsc::Sender<Message>,
    ) -> Result<(), mpsc::error::SendError<Command>> {
        self.commands
            .send(Command::Subscribe {
                subject: subject.into(),
                group: group.into(),
                sid: sid.into(),
                tx,
            })
            .await
    }

    pub async fn unsubscribe(
        &self,
        sid: impl Into<Bytes>,
        max: Option<NonZeroUsize>,
    ) -> Result<(), mpsc::error::SendError<Command>> {
        self.commands
            .send(Command::Unsubscribe {
                sid: sid.into(),
                max,
            })
            .await
    }

    pub async fn publish(
        &self,
        subject: impl Into<Bytes>,
        reply: impl Into<Bytes>,
        headers: impl Into<Bytes>,
        payload: impl Into<Bytes>,
    ) -> Result<(), mpsc::error::SendError<Command>> {
        self.commands
            .send(Command::Publish {
                subject: subject.into(),
                reply: reply.into(),
                headers: headers.into(),
                payload: payload.into(),
            })
            .await
    }

    pub async fn flush(&self) -> Result<(), mpsc::error::SendError<Command>> {
        self.commands.send(Command::Flush).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test_log::test(tokio::test)]
    async fn connect() -> anyhow::Result<()> {
        let (conn, _) = Conn::connect_tcp("localhost:4222").await?;
        let (clt, io) = Client::new(conn, ConnectOptions::default()).await?;
        let io = tokio::spawn(io);
        let info = clt
            .server_info()
            .await
            .context("failed to get server info")?;
        let info = Arc::clone(&info.borrow());
        eprintln!("info: {info:?}");
        let (tx, mut rx) = mpsc::channel(128);
        let subject = Bytes::from("test");
        clt.subscribe(subject.clone(), "", "0", tx)
            .await
            .context("failed to subscribe")?;
        let cmds = clt.commands();
        clt.publish(subject.clone(), "", "", "hello")
            .await
            .context("failed to publish `hello`")?;
        cmds.send(Command::Publish {
            subject: subject.clone(),
            headers: Bytes::default(),
            reply: Bytes::default(),
            payload: Bytes::from("bye\r\n\r\n"),
        })
        .await
        .context("failed to publish `bye`")?;
        let msg = rx.recv().await.context("failed to receive `hello`")?;
        assert_eq!(
            msg,
            Message {
                subject: subject.clone(),
                reply: Bytes::default(),
                headers: Bytes::default(),
                payload: Bytes::from("hello"),
            }
        );
        let msg = rx.recv().await.context("failed to receive `bye`")?;
        assert_eq!(
            msg,
            Message {
                subject: subject.clone(),
                reply: Bytes::default(),
                headers: Bytes::default(),
                payload: Bytes::from("bye\r\n\r\n"),
            }
        );
        clt.unsubscribe("0", None)
            .await
            .context("failed to unsubscribe")?;
        clt.flush().await.context("failed to flush")?;
        io.abort();
        Ok(())
    }
}
