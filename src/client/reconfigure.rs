//! Changing the configuration while the client runs. The new one replaces the old in one step;
//! the listener moves after that, before the server is told the listen port, and if the new
//! address cannot be bound the listener settings go back to what they were and the old listener
//! keeps running.

use std::sync::Arc;

use super::Client;
use crate::config::{Applied, Config, ConfigBuilder};
use crate::error::Error;
#[cfg(doc)]
use crate::error::ErrorKind;

impl Client {
  /// Changes the configuration in effect.
  ///
  /// `edit` is given a builder that starts from the configuration in effect and returns the
  /// one to build its replacement from, as
  /// `client.reconfigure(|config| config.listen_port(50_001))`. Returns what the change did:
  /// [`Applied`] names the part that waits for the next connect, which none does while
  /// disconnected, and the part that failed.
  ///
  /// While connected, the server is told the listen port and distributed status again. A
  /// listener that cannot be moved keeps the old listener settings, with the error in
  /// [`Applied::listener_error`]; the rest of the change stays applied. A lower concurrency
  /// limit lets what already runs finish, and starts nothing new until fewer run than the new
  /// limit.
  ///
  /// # Errors
  ///
  /// Fails only when nothing changed. Returns [`ErrorKind::Shutdown`] if the client has been
  /// shut down. Returns [`ErrorKind::InvalidArgument`] if the new configuration does not
  /// validate or changes a setting that is fixed for the client's lifetime.
  ///
  /// # Cancel safety
  ///
  /// A listener move under way when the call's future is dropped finishes on a task of its
  /// own. The server may then not be told the new listen port.
  pub async fn reconfigure(
    &self,
    edit: impl FnOnce(ConfigBuilder) -> ConfigBuilder,
  ) -> Result<Applied, Error> {
    self.inner.check_not_shut_down()?;
    let reconfiguring = self.inner.state_lock.clone().lock_owned().await;
    let old = self.inner.options();
    let new = edit(ConfigBuilder::from(Config::clone(&old))).build()?;
    new.validate_change(&old)?;
    let new = Arc::new(new);
    let connected = self.state().is_logged_in();
    let reconnect_required = connected && new.reconnect_required(&old);
    self.inner.set_options(new.clone());
    self.responder().resize(
      new.limits.max_cached_search_responses,
      new.limits.search_response_cache_lifetime,
    );
    self.inner.addresses.resize(
      new.limits.max_cached_addresses,
      new.limits.address_cache_lifetime,
    );
    self.network().resize_search_memory(
      new.limits.distributed_search_memory,
      new.limits.distributed_search_memory_lifetime,
    );
    // the child limit and whether the client takes children may have changed
    self.distributed_command(crate::distributed::Command::Changed);

    let listener_changed = new.listener_changed(&old);
    if listener_changed || !new.listen_while_disconnected {
      self.stop_kept_listener().await;
    }
    let (listener, _reconfiguring) = if listener_changed {
      // on a task of its own, holding the lock: the running listener is out of the session
      // while it moves, so a dropped call must not end the move half done
      let client = self.clone();
      let (old, new) = (old.clone(), new.clone());
      let moving = tokio::spawn(async move {
        let moved = client.move_listener(&old, &new).await;
        (moved, reconfiguring)
      });
      match moving.await {
        Ok(moved) => moved,
        Err(error) if error.is_panic() => std::panic::resume_unwind(error.into_panic()),
        // the runtime is shutting down and took the task with it
        Err(_) => return Err(Error::shutdown()),
      }
    } else {
      (Ok(()), reconfiguring)
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
    let server_error = if connected {
      if reconnect_required {
        tracing::warn!("some of the changed settings take effect only after reconnecting");
      }
      self.send_configuration_messages().await.err().map(|error| {
        tracing::warn!(%error, "failed to send the new configuration to the server");
        error.context("failed to send the new configuration to the server")
      })
    } else {
      None
    };
    tracing::info!(reconnect_required, "configuration changed");
    Ok(Applied {
      after_reconnect: reconnect_required,
      listener_error: listener.err(),
      server_error,
    })
  }

  /// Stops or rebinds a running listener for the new options; a stopped listener is left to the
  /// next connect.
  async fn move_listener(&self, old: &Config, new: &Arc<Config>) -> Result<(), Error> {
    // no session: the next connect starts the listener the options ask for
    let Some(running) = self.inner.session().stage.listener().map(Option::take) else {
      return Ok(());
    };
    let serving = self.state().is_logged_in();
    let Some(running) = running else {
      // turned on during a session that has none running
      if !new.listener {
        return Ok(());
      }
      return match self.start_listener(new, serving) {
        Ok(started) => {
          self.place_listener(started).await;
          Ok(())
        }
        Err(error) => {
          tracing::warn!(%error, "listener not started; listener options reverted");
          self.revert_listener_options(old, new);
          Err(error)
        }
      };
    };
    if !new.listener {
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
    self.place_listener(moved).await;
    error.map_or(Ok(()), Err)
  }

  /// Puts `listener` in the session; a disconnect while it was being bound took the session, and
  /// the listener goes with it.
  async fn place_listener(&self, listener: crate::listener::Listener) {
    let left = match self.inner.session().stage.listener() {
      Some(place) => place.replace(listener),
      None => Some(listener),
    };
    if let Some(left) = left {
      left.stop().await;
    }
  }

  fn revert_listener_options(&self, old: &Config, new: &Config) {
    self.inner.set_options(Arc::new(Config {
      listener: old.listener,
      listen_address: old.listen_address,
      listen_port: old.listen_port,
      listen_backlog: old.listen_backlog,
      ..new.clone()
    }));
  }
}
