//! Accepted connections, once their first message says what they are for, are counted and go to
//! the part of the client that owns that kind: a message connection, a transfer a download
//! waits for, a child for the distributed tree, or the answer to a connection request the
//! client had the server relay.

use soulseek_proto::PeerConnectionType;

use super::Client;
use crate::conn::{Conn, ConnKind};
use crate::distributed::Command;
use crate::error::Error;
use crate::inbound::InboundConnection;
use crate::limit::Permit;
use crate::log::LogLimiter;
use crate::peers::{Answer, Solicitation, Via};

static CROWDED_ADDRESS: LogLimiter = LogLimiter::new();

impl Client {
  /// Hands a classified connection to its owner. A connection nothing takes is closed with the
  /// reason, which it logs once. `unidentified` is the connection's place among those not
  /// identified yet; a peer introducing itself keeps it until the server's word on its address
  /// is in and the connection is kept.
  pub(super) async fn route_inbound(&self, inbound: InboundConnection, unidentified: Permit) {
    self.inner.inbound.send_modify(|counted| *counted += 1);
    let _unidentified = matches!(inbound, InboundConnection::Peer { .. }).then_some(unidentified);
    let routed = match inbound {
      InboundConnection::Peer { username, conn } => self.introduced(&username, conn).await,
      InboundConnection::Transfer {
        username,
        token,
        conn,
      } => {
        self
          .accept_transfer_connection(&username, token, conn)
          .await
      }
      InboundConnection::Distributed { username, conn } => {
        self.distributed_command(Command::ChildConnecting { username, conn });
        Ok(())
      }
      InboundConnection::PierceFirewall { token, conn } => self.pierced(token, conn).await,
    };
    if let Err(error) = routed {
      tracing::debug!(%error, "inbound connection not taken");
    }
  }

  /// A peer introduced itself on `conn` as `username`. The connection counts against its
  /// address from here, while the server is asked about it, until it is kept; one past the
  /// address's bound is refused.
  async fn introduced(&self, username: &str, conn: Conn) -> Result<(), Error> {
    let remote = conn.handle().remote();
    let limit = self.options().limits.max_message_connections_per_address;
    let Some(arrival) = self.peers().arrive(remote.ip(), limit) else {
      if let Some(suppressed) = CROWDED_ADDRESS.allow() {
        tracing::debug!(
          username,
          %remote,
          limit,
          suppressed,
          "too many message connections from one address, refused"
        );
      }
      conn
        .handle()
        .disconnect("too many message connections from this address", None);
      return Ok(());
    };
    let vouched = self.vouched(username, &conn).await;
    self.add_or_update_message_connection(
      username,
      conn,
      ConnKind::INBOUND | ConnKind::DIRECT,
      vouched,
      Some(arrival),
    )
  }

  /// Whether `conn`, introducing itself as `username`, comes from the address the server gives
  /// for the user, so it is theirs and replaces what the client had. With one of theirs open
  /// the server is asked; with none, only what it said already counts, the address cached for
  /// the user or the one it relayed a request of theirs with that the client is still
  /// answering, and a connection those do not confirm is served beside until the client needs
  /// one to the user.
  async fn vouched(&self, username: &str, conn: &Conn) -> bool {
    let ip = conn.handle().remote().ip();
    if self.peers().open(username).is_some() {
      return self.server_gives(username, ip).await;
    }
    self.server_gave(username, ip) || self.peers().relayed_from(username, ip)
  }

  /// A peer connected because the server relayed one of the client's requests: a solicitation
  /// still waiting takes it, or it is the searcher answers ride on the request for, who gets
  /// those kept now and the rest as they come to be kept. One that comes after its solicitor
  /// stopped waiting is taken as [`Client::late_answer`] says. Anything else is closed, a
  /// request answered once included. The ticket it answers with went only to the user it was
  /// relayed to, so it vouches for the connection.
  pub(crate) async fn pierced(&self, token: u32, conn: Conn) -> Result<(), Error> {
    let answered = || self.responder().answered(token);
    if let Some((solicitation, answer)) = self.peers().solicitations.take(token, answered) {
      let conn = match answer {
        Answer::Awaited => match self.waiter().offer(&solicitation.key(token), conn) {
          Ok(()) => return Ok(()),
          // the solicitor stopped waiting a moment ago
          Err(conn) => conn,
        },
        Answer::Late => conn,
      };
      return self.late_answer(token, &solicitation, conn).await;
    }
    if let Some(username) = self.responder().claim(token) {
      self.add_or_update_message_connection(&username, conn, Via::Indirect.kind(), true, None)?;
      self.try_respond_cached(token).await;
      return Ok(());
    }
    let error = Error::connection(
      crate::error::ConnectionKind::Other,
      format!(
        "unknown PierceFirewall attempt with token {token} from {}",
        conn.handle().remote()
      ),
    );
    conn.handle().disconnect(error.to_string(), Some(error));
    Ok(())
  }

  /// A user answered a solicitation after its solicitor stopped waiting, usually because
  /// dialing the user directly won meanwhile. Some clients then count the answering
  /// connection as their connection to this client and drop the one this client dialed, so for
  /// messages it replaces this client's, as when the user introduces itself on a new
  /// connection. A transfer already runs on the connection that won, and a parent candidate
  /// keeps the connection this client dialed as its link to this client, so those are closed.
  async fn late_answer(
    &self,
    token: u32,
    solicitation: &Solicitation,
    conn: Conn,
  ) -> Result<(), Error> {
    let username = &solicitation.username;
    let remote = conn.handle().remote();
    match solicitation.connection_type {
      PeerConnectionType::Peer => {
        tracing::debug!(
          username,
          token,
          %remote,
          "late answer to a message connection request taken"
        );
        self.add_or_update_message_connection(username, conn, Via::Indirect.kind(), true, None)?;
        // a search answer for the user kept under this token, its delivery having failed after
        // dialing won, goes out now
        self.try_respond_cached(token).await;
        Ok(())
      }
      PeerConnectionType::Transfer => {
        tracing::debug!(
          username,
          token,
          %remote,
          "late answer to a transfer connection request refused"
        );
        conn
          .handle()
          .disconnect("transfer connection request already answered", None);
        Ok(())
      }
      PeerConnectionType::Distributed => {
        tracing::debug!(
          username,
          token,
          %remote,
          "late answer to a distributed connection request refused"
        );
        conn
          .handle()
          .disconnect("distributed connection request already answered", None);
        Ok(())
      }
    }
  }
}
