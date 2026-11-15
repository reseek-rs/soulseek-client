//! Changing the options while the client runs. The new options replace the old in one step; the
//! listener moves after that, before the server is told the listen port, and if the new address
//! cannot be bound the listener options go back to what they were and the old listener keeps
//! running.

use std::sync::Arc;

use super::Client;
use crate::error::ClientError;
use crate::options::ClientOptions;
use crate::patch::{self, ClientOptionsPatch};

impl Client {
  /// Applies `patch` to the options in effect.
  ///
  /// Returns whether some of the change only takes effect after reconnecting, which is never
  /// the case while disconnected. While connected, the server is told the listen port and
  /// distributed status again. A listener that cannot be moved keeps the old listener options;
  /// the rest of the patch stays applied.
  ///
  /// # Errors
  ///
  /// Returns [`ClientError::Shutdown`] if the client has been shut down. Returns
  /// [`ClientError::InvalidArgument`] if the patched options do not validate. Returns
  /// [`ClientError::Listen`] if the listener cannot be moved. Returns [`ClientError::Client`]
  /// wrapping the cause if, while connected, the server cannot be told the listen port.
  pub async fn reconfigure_options(&self, patch: ClientOptionsPatch) -> Result<bool, ClientError> {
    self.inner.check_not_shut_down()?;
    let _reconfiguring = self.inner.state_lock.lock().await;
    let old = self.inner.options();
    let new = Arc::new(patch.apply(&old)?);
    let connected = self.state().is_logged_in();
    let reconnect_required = connected && patch::reconnect_required(&old, &new);
    self.inner.set_options(new.clone());
    self.responder().resize(
      new.limits.search_response_cache_size,
      new.limits.search_response_cache_lifetime,
    );
    self.inner.endpoints.resize(
      new.limits.user_endpoint_cache_size,
      new.limits.user_endpoint_cache_lifetime,
    );
    self.distributed().resize_search_memory(
      new.limits.distributed_search_memory,
      new.limits.distributed_search_memory_lifetime,
    );
    // the child limit and whether the client takes children may have changed
    self.distributed_command(crate::distributed::Command::Changed);

    let listener_changed = patch::listener_changed(&old, &new);
    if listener_changed || !new.listen_while_disconnected {
      self.stop_kept_listener().await;
    }
    let listener = if listener_changed {
      self.move_listener(&old, &new).await
    } else {
      Ok(())
    };

    // the client's speed bounds the children only while the server's advice is followed, and is
    // asked for at login only then
    if connected
      && new.follow_server_child_limits
      && !old.follow_server_child_limits
      && let Some(username) = self.username()
    {
      self.ask_own_speed(&username);
    }
    if connected {
      self.send_configuration_messages().await.map_err(|error| {
        tracing::warn!(%error, "failed to send the reconfigured options to the server");
        ClientError::client(
          format!(
            "failed to send the reconfigured options to the server: {error}; the new options are in effect and the same patch can be applied again"
          ),
          Some(error),
        )
      })?;
      if reconnect_required {
        tracing::warn!("some of the changed options take effect only after reconnecting");
      }
    }
    listener?;
    tracing::info!(reconnect_required, "options reconfigured");
    Ok(reconnect_required)
  }

  /// Stops or rebinds a running listener for the new options; a stopped listener is left to the
  /// next connect.
  async fn move_listener(
    &self,
    old: &ClientOptions,
    new: &Arc<ClientOptions>,
  ) -> Result<(), ClientError> {
    let running = self.inner.session().stage.listener().and_then(Option::take);
    let Some(running) = running else {
      return Ok(());
    };
    let serving = self.state().is_logged_in();
    if !new.enable_listener {
      running.stop().await;
      return Ok(());
    }
    // bound next to the old one when the port differs; on the same port the old one must
    // close first, and comes back if the new one still cannot bind
    let (moved, error) = match self.start_listener(new, serving) {
      Ok(listener) => {
        running.stop().await;
        (listener, None)
      }
      Err(_) if old.listen_port == new.listen_port => {
        running.stop().await;
        match self.start_listener(new, serving) {
          Ok(listener) => (listener, None),
          Err(error) => match self.start_listener(old, serving) {
            Ok(listener) => (listener, Some(error)),
            Err(restore) => {
              tracing::error!(%error, %restore, "the listener could not be moved or restored");
              self.revert_listener_options(old, new);
              return Err(error);
            }
          },
        }
      }
      Err(error) => (running, Some(error)),
    };
    if let Some(error) = &error {
      tracing::warn!(%error, "listener not moved; listener options reverted");
      self.revert_listener_options(old, new);
    }
    // a disconnect while the listener moved took the session; the listener goes with it
    let left = match self.inner.session().stage.listener() {
      Some(place) => place.replace(moved),
      None => Some(moved),
    };
    if let Some(left) = left {
      left.stop().await;
    }
    error.map_or(Ok(()), Err)
  }

  fn revert_listener_options(&self, old: &ClientOptions, new: &ClientOptions) {
    self.inner.set_options(Arc::new(ClientOptions {
      enable_listener: old.enable_listener,
      listen_ip_address: old.listen_ip_address,
      listen_port: old.listen_port,
      listen_backlog: old.listen_backlog,
      ..new.clone()
    }));
  }
}
