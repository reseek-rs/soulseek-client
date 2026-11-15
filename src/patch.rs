//! A partial change to the client options and what applying it implies: whether the listener
//! must be rebound and whether the change only takes effect after reconnecting.

use std::net::IpAddr;

use crate::error::ClientError;
use crate::options::{ClientLimits, ClientOptions, ConnectionOptions};
use crate::resolvers::Resolvers;

/// The options that can change while the client runs. Each field replaces the [`ClientOptions`]
/// field of the same name, which says what it does; `None` leaves it as it is. The options fixed
/// for the client's lifetime (message timeout, server ping interval, event capacity, protocol versions) have no field
/// here. A lower concurrency limit lets what already runs finish, and starts nothing new until
/// fewer run than the new limit.
#[derive(Clone, Debug, Default)]
pub struct ClientOptionsPatch {
  /// see [`ClientOptions::enable_listener`]
  pub enable_listener: Option<bool>,
  /// see [`ClientOptions::listen_ip_address`]
  pub listen_ip_address: Option<IpAddr>,
  /// see [`ClientOptions::listen_port`]
  pub listen_port: Option<u16>,
  /// see [`ClientOptions::advertised_port`]; `Some(None)` goes back to the listen port
  pub advertised_port: Option<Option<u16>>,
  /// see [`ClientOptions::listen_backlog`]
  pub listen_backlog: Option<u32>,
  /// see [`ClientOptions::enable_distributed_network`]
  pub enable_distributed_network: Option<bool>,
  /// see [`ClientOptions::accept_distributed_children`]
  pub accept_distributed_children: Option<bool>,
  /// see [`ClientOptions::distributed_child_limit`]
  pub distributed_child_limit: Option<u32>,
  /// see [`ClientOptions::follow_server_child_limits`]
  pub follow_server_child_limits: Option<bool>,
  /// see [`ClientOptions::transfer_response_timeout`]
  pub transfer_response_timeout: Option<std::time::Duration>,
  /// see [`ClientOptions::send_place_when_queued`]
  pub send_place_when_queued: Option<bool>,
  /// see [`ClientOptions::upload_notice_timeout`]
  pub upload_notice_timeout: Option<std::time::Duration>,
  /// see [`ClientOptions::unasked_offer_timeout`]
  pub unasked_offer_timeout: Option<std::time::Duration>,
  /// see [`ClientOptions::check_names`]
  pub check_names: Option<bool>,
  /// its inactivity timeout is turned off, as for the options the client starts with
  pub server_connection_options: Option<ConnectionOptions>,
  /// see [`ClientOptions::peer_connection_options`]
  pub peer_connection_options: Option<ConnectionOptions>,
  /// see [`ClientOptions::transfer_connection_options`]
  pub transfer_connection_options: Option<ConnectionOptions>,
  /// see [`ClientOptions::incoming_connection_options`]
  pub incoming_connection_options: Option<ConnectionOptions>,
  /// see [`ClientOptions::distributed_connection_options`]
  pub distributed_connection_options: Option<ConnectionOptions>,
  /// see [`ClientOptions::limits`]
  pub limits: Option<ClientLimits>,
  /// Replaces every callback: one left unset is removed. To change one, start from the
  /// resolvers in [`Client::options`](crate::Client::options).
  pub resolvers: Option<Resolvers>,
}

impl ClientOptionsPatch {
  /// `current` with every set field replaced, validated as a whole
  pub(crate) fn apply(self, current: &ClientOptions) -> Result<ClientOptions, ClientError> {
    let Self {
      enable_listener,
      listen_ip_address,
      listen_port,
      advertised_port,
      listen_backlog,
      enable_distributed_network,
      accept_distributed_children,
      distributed_child_limit,
      follow_server_child_limits,
      transfer_response_timeout,
      send_place_when_queued,
      upload_notice_timeout,
      unasked_offer_timeout,
      check_names,
      server_connection_options,
      peer_connection_options,
      transfer_connection_options,
      incoming_connection_options,
      distributed_connection_options,
      limits,
      resolvers,
    } = self;
    let mut next = current.clone();
    set(&mut next.enable_listener, enable_listener);
    set(&mut next.listen_ip_address, listen_ip_address);
    set(&mut next.listen_port, listen_port);
    set(&mut next.advertised_port, advertised_port);
    set(&mut next.listen_backlog, listen_backlog);
    set(
      &mut next.enable_distributed_network,
      enable_distributed_network,
    );
    set(
      &mut next.accept_distributed_children,
      accept_distributed_children,
    );
    set(&mut next.distributed_child_limit, distributed_child_limit);
    set(
      &mut next.follow_server_child_limits,
      follow_server_child_limits,
    );
    set(
      &mut next.transfer_response_timeout,
      transfer_response_timeout,
    );
    set(&mut next.send_place_when_queued, send_place_when_queued);
    set(&mut next.upload_notice_timeout, upload_notice_timeout);
    set(&mut next.unasked_offer_timeout, unasked_offer_timeout);
    set(&mut next.check_names, check_names);
    if let Some(options) = server_connection_options {
      next.server_connection_options = ConnectionOptions {
        inactivity_timeout: None,
        ..options
      };
    }
    set(&mut next.peer_connection_options, peer_connection_options);
    set(
      &mut next.transfer_connection_options,
      transfer_connection_options,
    );
    set(
      &mut next.incoming_connection_options,
      incoming_connection_options,
    );
    set(
      &mut next.distributed_connection_options,
      distributed_connection_options,
    );
    set(&mut next.limits, limits);
    set(&mut next.resolvers, resolvers);
    next.validate()?;
    Ok(next)
  }
}

fn set<T>(field: &mut T, value: Option<T>) {
  if let Some(value) = value {
    *field = value;
  }
}

/// the listener's socket has to be bound again for the change to take effect
pub(crate) fn listener_changed(old: &ClientOptions, new: &ClientOptions) -> bool {
  old.enable_listener != new.enable_listener
    || old.listen_ip_address != new.listen_ip_address
    || old.listen_port != new.listen_port
    || old.listen_backlog != new.listen_backlog
}

/// The change only takes effect on a new server session: the distributed network or children
/// being turned off (the server and the tree already count on us), or the server or distributed
/// connections' options changing. Turning things on, and the peer, transfer and incoming
/// options, apply to what happens next without reconnecting.
pub(crate) fn reconnect_required(old: &ClientOptions, new: &ClientOptions) -> bool {
  (old.enable_distributed_network && !new.enable_distributed_network)
    || (old.accept_distributed_children && !new.accept_distributed_children)
    || old.distributed_connection_options != new.distributed_connection_options
    || old.effective_server_connection_options() != new.effective_server_connection_options()
}

#[cfg(test)]
mod tests {
  use std::time::Duration;

  use super::*;
  use crate::options::KeepAlive;

  #[test]
  fn reconnect_required_only_for_turning_things_off_or_server_and_distributed_options() {
    let old = ClientOptions::default();
    let patched = |patch: ClientOptionsPatch| patch.apply(&old).unwrap();

    let off = patched(ClientOptionsPatch {
      enable_distributed_network: Some(false),
      ..ClientOptionsPatch::default()
    });
    assert!(reconnect_required(&old, &off));
    assert!(!reconnect_required(&off, &old), "turning it back on");

    let no_children = patched(ClientOptionsPatch {
      accept_distributed_children: Some(false),
      ..ClientOptionsPatch::default()
    });
    assert!(reconnect_required(&old, &no_children));

    let peer = patched(ClientOptionsPatch {
      peer_connection_options: Some(ConnectionOptions {
        connect_timeout: Duration::from_secs(3),
        ..ConnectionOptions::default()
      }),
      ..ClientOptionsPatch::default()
    });
    assert!(!reconnect_required(&old, &peer));

    let keepalive = patched(ClientOptionsPatch {
      server_connection_options: Some(ConnectionOptions {
        keepalive: KeepAlive {
          probes: 5,
          ..old.server_connection_options.keepalive.clone()
        },
        ..old.server_connection_options.clone()
      }),
      ..ClientOptionsPatch::default()
    });
    assert!(reconnect_required(&old, &keepalive));
  }

  #[test]
  fn server_options_from_a_patch_never_time_out() {
    let old = ClientOptions::default();
    let new = ClientOptionsPatch {
      server_connection_options: Some(ConnectionOptions {
        inactivity_timeout: Some(Duration::from_secs(30)),
        ..old.server_connection_options.clone()
      }),
      ..ClientOptionsPatch::default()
    }
    .apply(&old)
    .unwrap();
    assert_eq!(new.server_connection_options.inactivity_timeout, None);
    assert!(
      !reconnect_required(&old, &new),
      "identical once the timeout is forced off"
    );
  }

  #[test]
  fn invalid_patch_is_rejected_whole() {
    let old = ClientOptions::default();
    let result = ClientOptionsPatch {
      listen_port: Some(0),
      ..ClientOptionsPatch::default()
    }
    .apply(&old);
    assert!(matches!(result, Err(ClientError::InvalidArgument(_))));
  }
}
