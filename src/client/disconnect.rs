//! Ending the server session. The state moves to disconnecting, waits on the server are failed
//! and the session's resources are taken in one step under the session lock; stopping the
//! listener, closing the server connection, tearing down the distributed tree, canceling the
//! client's searches and forgetting the connection requests the server relayed follow, and the
//! state ends at disconnected. Message and transfer connections to other users, and the
//! transfers on them, are not part of the session and go on.

use tokio::task::JoinHandle;
use tracing::Instrument;
use uuid::Uuid;

use super::session::{Edge, Released};
use super::{Client, Inner};
use crate::conn::Disconnect;
use crate::error::{ClientError, ConnectionErrorKind};
use crate::lock;
use crate::state::ClientState;
use crate::waiter::Scope;

/// What ended the session, which decides whether a connect in progress is canceled
pub(super) enum Cause {
  /// the application asked: every `connect` called before it reports `Canceled`, the one under
  /// way and the ones still waiting their turn
  Requested,
  /// a `connect` ends the session it began, because it failed or was dropped before it logged
  /// in; the connects waiting their turn go on
  Abandoned,
  /// the server dropped the client; a `connect` in progress fails on its own
  Kicked,
  /// this server connection closed; ignored if it is no longer the session's connection
  ServerLost(Uuid),
}

/// The second half of a disconnect, after the state has moved to disconnecting
pub(super) struct Teardown {
  message: String,
  error: Option<ClientError>,
  released: Released,
}

impl Client {
  /// Ends the server session and returns once it has ended.
  ///
  /// `message` and `error` are the reason the
  /// [`Event::Disconnected`](crate::Event::Disconnected) carries; the message defaults to the
  /// error's. A disconnect already under way is waited for instead. Every [`Client::connect`]
  /// call made before this one ends with [`ClientError::Canceled`], whether it is under way or
  /// still waiting for its turn.
  ///
  /// # Cancel safety
  ///
  /// The teardown runs on a task of its own, so dropping this call's future does not leave the
  /// client disconnecting.
  pub async fn disconnect(&self, message: Option<&str>, error: Option<ClientError>) {
    self.end_session(message, error, Cause::Requested).await;
  }

  /// ends the session for `cause` and returns once it has ended, as `disconnect` does
  pub(super) async fn end_session(
    &self,
    message: Option<&str>,
    error: Option<ClientError>,
    cause: Cause,
  ) {
    let Some(teardown) = self.inner.begin_disconnect(message, error, cause) else {
      self.teardown_ended().await;
      return;
    };
    // the other error is the runtime shutting down, which drops the teardown with every task
    if let Err(e) = teardown.spawn(self).await
      && e.is_panic()
    {
      std::panic::resume_unwind(e.into_panic());
    }
  }

  /// waits until no teardown is under way
  async fn teardown_ended(&self) {
    let mut state = self.inner.state.subscribe();
    // the client holds the sender for as long as `self` lives, so this cannot fail
    let _ = state
      .wait_for(|state| *state != ClientState::Disconnecting)
      .await;
  }

  /// the server connection closed by itself: a read or write failed, or the remote closed it
  pub(super) async fn server_disconnected(&self, id: Uuid, reason: Disconnect) {
    if let Some(teardown) =
      self
        .inner
        .begin_disconnect(Some(&reason.message), reason.error, Cause::ServerLost(id))
    {
      teardown.finish(self).await;
    }
  }
}

impl Inner {
  /// Moves to disconnecting and takes the session's resources, or returns `None` when there is
  /// nothing to disconnect.
  pub(super) fn begin_disconnect(
    &self,
    message: Option<&str>,
    error: Option<ClientError>,
    cause: Cause,
  ) -> Option<Teardown> {
    let mut session = self.session();
    if let Cause::ServerLost(id) = cause
      && session.stage.link().map(|link| link.server.handle().id()) != Some(id)
    {
      return None;
    }
    // ahead of the edge, which a session disconnecting or disconnected refuses while a connect
    // can still wait there for its turn
    if let Cause::Requested = cause {
      std::mem::take(&mut session.connects).cancel();
    }
    let message = match (message, &error) {
      (Some(message), _) => message.to_owned(),
      (None, Some(error)) => error.to_string(),
      (None, None) => "client disconnected".to_owned(),
    };
    // refused when already disconnected or disconnecting: there is nothing to tear down
    let released = self
      .transition(&mut session, Edge::Disconnect, message.clone(), None)
      .ok()?;
    let faulted = self.waiter.fault_scope(
      &Scope::Server,
      &ClientError::connection(
        ConnectionErrorKind::Other,
        format!("server connection disconnected: {message}"),
      ),
    );
    if faulted > 0 {
      tracing::debug!(faulted, "failed the waits on the server");
    }
    Some(Teardown {
      message,
      error,
      released,
    })
  }
}

impl Teardown {
  /// Runs the teardown on a task of its own: it is the only way out of disconnecting, so it
  /// must reach its end whether or not anyone waits for it.
  pub(super) fn spawn(self, client: &Client) -> JoinHandle<()> {
    let client = client.clone();
    tokio::spawn(async move { self.finish(&client).await }.in_current_span())
  }

  async fn finish(self, client: &Client) {
    let inner = &*client.inner;
    let Self {
      message,
      error,
      released: Released { listener, server },
    } = self;
    if let Some(listener) = listener {
      let options = client.options();
      if options.listen_while_disconnected && inner.check_not_shut_down().is_ok() {
        *lock(&inner.kept_listener) = Some(listener);
      } else {
        listener.stop().await;
      }
    }
    if let Some(server) = server {
      server.handle().disconnect(message.clone(), error.clone());
    }
    client.tear_down_distributed().await;
    if client.options().cancel_searches_on_disconnect {
      client.searches().cancel_all();
    }
    // answers to those requests would now be taken for a session that no longer exists
    client.peers().solicitations.clear();
    let mut session = inner.session();
    match &error {
      None | Some(ClientError::Canceled | ClientError::Shutdown) => {
        tracing::info!(message, "disconnected from server");
      }
      // the application hears of it with the `Disconnected` event and tells of it as it handles
      // it: a disconnect it asked for, a connect that failed or a session that was lost
      Some(error) => tracing::debug!(message, %error, "disconnected from server"),
    }
    inner
      .transition(&mut session, Edge::Finish, message, error)
      .expect("only the teardown that began a disconnect ends it");
  }
}
