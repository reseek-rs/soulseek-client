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
  /// `client.reconfigure(|config| config.listen_port(50_001))`. Returns [`Applied::Now`] if the
  /// whole change is in effect, or [`Applied::AfterReconnect`] if some of it waits for the next
  /// connect, which is never the case while disconnected.
  ///
  /// While connected, the server is told the listen port and distributed status again. A
  /// listener that cannot be moved keeps the old listener settings; the rest of the change
  /// stays applied. A lower concurrency limit lets what already runs finish, and starts nothing
  /// new until fewer run than the new limit.
  ///
  /// # Errors
  ///
  /// Returns [`ErrorKind::Shutdown`] if the client has been shut down. Returns
  /// [`ErrorKind::InvalidArgument`] if the new configuration does not validate or changes a setting
  /// that is fixed for the client's lifetime. Returns [`ErrorKind::Listen`] if the listener cannot
  /// be moved. Returns the error of the write, such as an [`ErrorKind::Connection`], if, while
  /// connected, the server cannot be told the listen port; the change is in effect, and applying it
  /// again tells the server.
  pub async fn reconfigure(
    &self,
    edit: impl FnOnce(ConfigBuilder) -> ConfigBuilder,
  ) -> Result<Applied, Error> {
    self.inner.check_not_shut_down()?;
    let _reconfiguring = self.inner.state_lock.lock().await;
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
        tracing::warn!(%error, "failed to send the new configuration to the server");
        error.context("the new configuration is in effect, but the server was not told it")
      })?;
      if reconnect_required {
        tracing::warn!("some of the changed settings take effect only after reconnecting");
      }
    }
    listener?;
    tracing::info!(reconnect_required, "configuration changed");
    Ok(if reconnect_required {
      Applied::AfterReconnect
    } else {
      Applied::Now
    })
  }

  /// Stops or rebinds a running listener for the new options; a stopped listener is left to the
  /// next connect.
  async fn move_listener(&self, old: &Config, new: &Arc<Config>) -> Result<(), Error> {
    let running = self.inner.session().stage.listener().and_then(Option::take);
    let Some(running) = running else {
      return Ok(());
    };
    let serving = self.state().is_logged_in();
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
