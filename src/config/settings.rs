//! Deserializing the configuration: each of `Config`, `Connection` and `Limits` is read as a map
//! of settings onto a value that already holds its defaults, so a key left out keeps the value
//! it had. The sections of a `Config` are read onto that section's own defaults.

use std::fmt;

use serde::de::{DeserializeSeed, Deserializer, Error, MapAccess, Visitor};

use super::{Config, Connection, Limits};

/// A type read as a map of its settings
trait Settings {
  const NAME: &'static str;
  const KEYS: &'static [&'static str];

  /// reads the value of the setting `key` from `map` into `self`
  fn set<'de, A: MapAccess<'de>>(&mut self, key: &str, map: &mut A) -> Result<(), A::Error>;
}

/// Implements `Settings` from the names of the fields. A plain field is replaced by the value
/// read, a `sections` field is itself read as settings onto the value it holds.
macro_rules! settings {
  ($type:ident { $($field:ident),* $(,)? } $(sections { $($section:ident),* $(,)? })?) => {
    impl Settings for $type {
      const NAME: &'static str = stringify!($type);
      const KEYS: &'static [&'static str] =
        &[$(stringify!($field),)* $($(stringify!($section),)*)?];

      fn set<'de, A: MapAccess<'de>>(&mut self, key: &str, map: &mut A) -> Result<(), A::Error> {
        match key {
          $(stringify!($field) => self.$field = map.next_value()?,)*
          $($(stringify!($section) => map.next_value_seed(Onto(&mut self.$section))?,)*)?
          _ => return Err(Error::unknown_field(key, Self::KEYS)),
        }
        Ok(())
      }
    }
  };
}

settings!(Config {
  listener,
  listen_while_disconnected,
  listen_address,
  listen_port,
  advertised_port,
  listen_backlog,
  distributed_network,
  parent_must_relay_search,
  distributed_children,
  max_distributed_children,
  follow_server_child_limits,
  cancel_searches_on_disconnect,
  check_names,
  request_timeout,
  server_ping_interval,
  event_capacity,
  transfer_response_timeout,
  transfer_accept_timeout,
  send_place_when_queued,
  upload_notice_timeout,
  unasked_offer_timeout,
  protocol_major_version,
  protocol_minor_version,
  server_host,
  server_port,
} sections {
  server_connection,
  peer_connection,
  transfer_connection,
  incoming_connection,
  distributed_connection,
  limits,
});

settings!(Connection {
  read_buffer_size,
  write_buffer_size,
  write_queue_size,
  connect_timeout,
  inactivity_timeout,
  write_timeout,
  proxy,
  keepalive,
  max_segment_size,
});

settings!(Limits {
  decode,
  max_concurrent_peer_connects,
  max_cached_peer_connections,
  max_message_connections_per_address,
  max_unidentified_connections,
  max_concurrent_search_answers,
  search_answer_timeout,
  max_cached_search_responses,
  search_response_cache_lifetime,
  max_cached_addresses,
  address_cache_lifetime,
  distributed_parent_negotiation_timeout,
  distributed_search_memory,
  distributed_search_memory_lifetime,
  distributed_status_debounce,
  distributed_status_max_age,
  distributed_parent_retry,
});

/// Reads a map of settings onto the value borrowed, setting only the keys the map has
struct Onto<'a, T>(&'a mut T);

impl<'de, T: Settings> DeserializeSeed<'de> for Onto<'_, T> {
  type Value = ();

  fn deserialize<D: Deserializer<'de>>(self, deserializer: D) -> Result<(), D::Error> {
    deserializer.deserialize_struct(T::NAME, T::KEYS, self)
  }
}

impl<'de, T: Settings> Visitor<'de> for Onto<'_, T> {
  type Value = ();

  fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    write!(f, "a map of the settings of `{}`", T::NAME)
  }

  fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<(), A::Error> {
    while let Some(key) = map.next_key::<String>()? {
      self.0.set(&key, &mut map)?;
    }
    Ok(())
  }
}

/// Deserializes what [`Serialize`](serde::Serialize) writes, from a self-describing format, and
/// checks it as [`ConfigBuilder::build`](super::ConfigBuilder::build) does.
///
/// A setting left out keeps its default, also inside a kind of connection or the limits, where
/// the default is that of the kind: a `server_connection` that names only its connect timeout
/// keeps the server connection's write timeout and keep-alive. An optional setting is turned
/// off with a null where the format has one. TOML has none, so a setting read from TOML is
/// turned off on `ConfigBuilder::from(config)`.
///
/// The handler and the transfer governor and reporter are not serialized, so the configuration
/// has none until the application sets them on `ConfigBuilder::from(config)`.
impl<'de> serde::Deserialize<'de> for Config {
  fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
    let mut builder = Config::builder();
    Onto(&mut builder.config).deserialize(deserializer)?;
    builder.build().map_err(Error::custom)
  }
}

/// Deserializes what [`Serialize`](serde::Serialize) writes, from a self-describing format. A
/// setting left out keeps the default of [`Connection::default`]. The settings are checked only
/// as part of a [`Config`].
impl<'de> serde::Deserialize<'de> for Connection {
  fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
    over_default(deserializer)
  }
}

/// Deserializes what [`Serialize`](serde::Serialize) writes, from a self-describing format. A
/// limit left out keeps the default of [`Limits::default`], while the decode limits are read
/// whole. The limits are checked only as part of a [`Config`].
impl<'de> serde::Deserialize<'de> for Limits {
  fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
    over_default(deserializer)
  }
}

fn over_default<'de, T: Settings + Default, D: Deserializer<'de>>(
  deserializer: D,
) -> Result<T, D::Error> {
  let mut settings = T::default();
  Onto(&mut settings).deserialize(deserializer)?;
  Ok(settings)
}

#[cfg(test)]
mod tests {
  use std::time::Duration;

  use serde_json::{Value, from_value, json, to_value};

  use super::*;
  use crate::config::{KeepAlive, Proxy};

  fn refused(json: Value) -> String {
    from_value::<Config>(json).unwrap_err().to_string()
  }

  #[test]
  fn a_config_round_trips_with_its_connections_limits_and_proxy() {
    let config = Config::builder()
      .server("example.org", 2242)
      .listen_port(50_300)
      .server_ping_interval(None)
      .peer_connection(|connection| {
        connection
          .connect_timeout(Duration::from_secs(5))
          .keepalive(KeepAlive::new(
            Duration::from_secs(20),
            Duration::from_secs(4),
            2,
          ))
          .proxy(Proxy::new("proxy.example", 1080).credentials("user", "secret"))
      })
      .limits(|limits| {
        limits
          .max_cached_addresses(500)
          .address_cache_lifetime(Duration::from_millis(1500))
      })
      .build()
      .unwrap();

    let json = to_value(&config).unwrap();
    assert_eq!(json["server_host"], "example.org");
    assert_eq!(json["server_port"], 2242);
    assert_eq!(json["listen_address"], "0.0.0.0");
    assert_eq!(json["listen_port"], 50_300);
    assert_eq!(json["server_ping_interval"], Value::Null);
    assert_eq!(json["request_timeout"], json!({ "secs": 5, "nanos": 0 }));
    assert_eq!(
      json["peer_connection"],
      json!({
        "read_buffer_size": 16_384,
        "write_buffer_size": 16_384,
        "write_queue_size": 250,
        "connect_timeout": { "secs": 5, "nanos": 0 },
        "inactivity_timeout": { "secs": 15, "nanos": 0 },
        "write_timeout": null,
        "proxy": {
          "address": "proxy.example",
          "port": 1080,
          "credentials": { "username": "user", "password": "secret" },
        },
        "keepalive": {
          "idle": { "secs": 20, "nanos": 0 },
          "interval": { "secs": 4, "nanos": 0 },
          "probes": 2,
        },
        "max_segment_size": null,
      })
    );
    assert_eq!(json["limits"]["max_cached_addresses"], 500);
    assert_eq!(
      json["limits"]["address_cache_lifetime"],
      json!({ "secs": 1, "nanos": 500_000_000 })
    );
    // the handler and the transfer callbacks are not settings
    assert!(json.get("handler").is_none() && json.get("transfer_defaults").is_none());

    let back: Config = from_value(json.clone()).unwrap();
    assert_eq!(back.server_ping_interval(), None);
    assert_eq!(back.peer_connection(), config.peer_connection());
    assert_eq!(back.limits(), config.limits());
    assert_eq!(to_value(&back).unwrap(), json);
  }

  #[test]
  fn a_setting_left_out_keeps_its_default() {
    let config: Config = from_value(json!({ "listen_port": 50_300 })).unwrap();
    assert_eq!(config.listen_port(), 50_300);
    assert_eq!(config.server_ping_interval(), Some(Duration::from_secs(60)));
    assert_eq!(
      config.server_connection(),
      Config::default().server_connection()
    );
  }

  #[test]
  fn a_section_keeps_the_defaults_of_its_own_kind_of_connection() {
    let config: Config = from_value(json!({
      "server_connection": { "connect_timeout": { "secs": 3, "nanos": 0 } },
      "distributed_connection": { "write_queue_size": 100 },
    }))
    .unwrap();

    let server = config.server_connection();
    assert_eq!(server.connect_timeout(), Duration::from_secs(3));
    assert_eq!(server.write_timeout(), Some(Duration::from_secs(30)));
    assert_eq!(
      server.keepalive(),
      Some(&KeepAlive::new(
        Duration::from_secs(10),
        Duration::from_secs(2),
        3
      ))
    );
    assert_eq!(server.inactivity_timeout(), None);

    let distributed = config.distributed_connection();
    assert_eq!(distributed.write_queue_size(), 100);
    assert_eq!(
      distributed.inactivity_timeout(),
      Some(Duration::from_secs(60))
    );
  }

  #[test]
  fn one_limit_set_keeps_the_other_limits() {
    let config: Config = from_value(json!({ "limits": { "max_cached_addresses": 500 } })).unwrap();
    let expected = Limits {
      max_cached_addresses: 500,
      ..Limits::default()
    };
    assert_eq!(config.limits(), &expected);
  }

  #[test]
  fn a_null_turns_an_optional_setting_off() {
    let config: Config = from_value(json!({
      "server_ping_interval": null,
      "server_connection": { "write_timeout": null, "keepalive": null },
    }))
    .unwrap();
    assert_eq!(config.server_ping_interval(), None);
    assert_eq!(config.server_connection().write_timeout(), None);
    assert_eq!(config.server_connection().keepalive(), None);
  }

  #[test]
  fn an_unknown_key_is_refused_at_the_top_and_inside_a_section() {
    assert!(refused(json!({ "listen_prot": 50_300 })).starts_with("unknown field `listen_prot`"));
    assert!(
      refused(json!({ "peer_connection": { "conect_timeout": 1 } }))
        .starts_with("unknown field `conect_timeout`")
    );
    assert!(
      refused(json!({ "limits": { "max_cached_adresses": 1 } }))
        .starts_with("unknown field `max_cached_adresses`")
    );
  }

  #[test]
  fn a_value_out_of_range_is_refused_at_the_top_and_inside_a_section() {
    assert!(
      refused(json!({ "listen_port": 0 })).starts_with("listen_port: must be between 1 and 65535")
    );
    assert!(
      refused(json!({ "server_connection": { "write_queue_size": 0 } }))
        .starts_with("server_connection.write_queue_size: must be between 1 and")
    );
    assert!(
      refused(json!({ "peer_connection": { "keepalive": {
        "idle": { "secs": 10, "nanos": 0 },
        "interval": { "secs": 2, "nanos": 0 },
        "probes": 0,
      } } }))
      .starts_with("peer_connection.keepalive.probes: must be at least 1")
    );
    assert!(
      refused(json!({ "limits": { "max_cached_addresses": 0 } }))
        .starts_with("limits.max_cached_addresses: must be greater than or equal to 1")
    );
  }

  #[test]
  fn a_connection_or_limits_alone_is_read_over_its_default() {
    let connection: Connection = from_value(json!({ "write_queue_size": 100 })).unwrap();
    let expected = Connection {
      write_queue_size: 100,
      ..Connection::default()
    };
    assert_eq!(connection, expected);
    assert!(from_value::<Connection>(json!({ "write_queu_size": 100 })).is_err());

    let limits: Limits = from_value(json!({ "max_cached_addresses": 500 })).unwrap();
    assert_eq!(limits.max_cached_addresses(), 500);
    assert_eq!(
      limits.max_cached_peer_connections(),
      Limits::default().max_cached_peer_connections()
    );
  }
}
