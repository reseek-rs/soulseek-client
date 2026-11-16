//! Other users on soulfind: their address, watching them, their status as asked for and as
//! pushed, their statistics as asked for and as pushed to members of a shared room, and the
//! client's own statistics as pushed to it.

use std::net::SocketAddr;

use crate::support::soulfind;
use crate::support::{free_port, options, user_key};
use soulseek_client::ErrorKind;
use soulseek_client::Event;
use soulseek_proto::server::{
  GetPeerAddressRequest, GetPeerAddressResponse, PeerAddressObfuscation,
};
use soulseek_proto::{RoomType, ServerCode, ServerRequest, UserPresence};

#[tokio::test]
async fn the_address_of_an_online_user_and_of_an_unknown_one() {
  let Some(server) = soulfind::server() else {
    return;
  };
  let port = free_port();
  let bob = server
    .login("bob", options().advertised_port(Some(port)))
    .await;
  let alice = server.login("alice", options()).await;
  let address = alice.client.users().address(&bob.username).await.unwrap();
  let public_ip = bob.client.server_info().own_public_ip.unwrap();
  assert_eq!(address, SocketAddr::from((public_ip, port)));
  // asked again within the minute, the address comes from the cache
  assert_eq!(
    alice.client.users().address(&bob.username).await.unwrap(),
    address
  );
  let nobody = soulfind::username("nobody");
  let error = alice.client.users().address(&nobody).await.unwrap_err();
  assert!(
    error.kind() == ErrorKind::UserOffline
      && error.to_string() == format!("user {nobody} appears to be offline"),
    "{error:?}"
  );
}

#[tokio::test]
async fn the_address_reply_keeps_its_obfuscation_trailer() {
  let Some(server) = soulfind::server() else {
    return;
  };
  let bob = server.login("bob", options()).await;
  let alice = server.login("alice", options()).await;
  let address: GetPeerAddressResponse = alice
    .ask(
      user_key(ServerCode::GetPeerAddress, &bob.username),
      ServerRequest::GetPeerAddress(GetPeerAddressRequest {
        username: bob.username.clone(),
      }),
    )
    .await;
  // soulfind sends the trailer, empty; it is kept and never dialed
  assert_eq!(
    address.obfuscation,
    Some(PeerAddressObfuscation {
      obfuscation_type: 0,
      obfuscated_port: 0
    })
  );
}

#[tokio::test]
async fn watching_an_online_user_and_a_missing_one() {
  let Some(server) = soulfind::server() else {
    return;
  };
  let bob = server.login("bob", options()).await;
  let alice = server.login("alice", options()).await;
  let watched = alice.client.users().watch(&bob.username).await.unwrap();
  assert_eq!(watched.status, UserPresence::Online);
  assert!(watched.country_code.is_some(), "present for an online user");
  let missing = soulfind::username("missing");
  let error = alice.client.users().watch(&missing).await.unwrap_err();
  assert!(
    error.kind() == ErrorKind::UserNotFound
      && error.to_string() == format!("user {missing} does not exist"),
    "{error:?}"
  );
  alice.client.users().unwatch(&bob.username).await.unwrap();
}

#[tokio::test]
async fn status_is_raised_when_asked_for_and_when_a_watched_user_goes_away() {
  let Some(server) = soulfind::server() else {
    return;
  };
  let bob = server.login("bob", options()).await;
  let mut alice = server.login("alice", options()).await;
  let status = alice.client.users().status(&bob.username).await.unwrap();
  assert_eq!(status.username, bob.username);
  assert_eq!(status.presence, UserPresence::Online);
  assert!(!status.is_privileged);
  let raised = alice
    .event(|event| match event {
      Event::User(soulseek_client::user::Event::StatusChanged {
        status: raised,
        asked,
      }) if raised.username == status.username => Some((raised, asked)),
      _ => None,
    })
    .await;
  assert_eq!(raised, (status.clone(), true));

  alice.client.users().watch(&bob.username).await.unwrap();
  bob
    .client
    .account()
    .set_status(UserPresence::Away)
    .await
    .unwrap();
  let pushed = alice
    .event(|event| match event {
      Event::User(soulseek_client::user::Event::StatusChanged {
        status: pushed,
        asked,
      }) if pushed.username == status.username => Some((pushed, asked)),
      _ => None,
    })
    .await;
  assert_eq!(pushed.0.presence, UserPresence::Away);
  assert!(!pushed.1, "pushed, not asked for");
}

#[tokio::test]
async fn statistics_are_raised_when_asked_for_and_pushed_to_room_members() {
  let Some(server) = soulfind::server() else {
    return;
  };
  let room = soulfind::username("stats");
  let bob = server.login("bob", options()).await;
  let mut alice = server.login("alice", options()).await;
  let statistics = alice
    .client
    .users()
    .statistics(&bob.username)
    .await
    .unwrap();
  assert_eq!(statistics.username, bob.username);
  let raised = alice
    .event(|event| match event {
      Event::User(soulseek_client::user::Event::StatisticsChanged {
        statistics: raised,
        asked,
      }) if raised.username == statistics.username => Some((raised, asked)),
      _ => None,
    })
    .await;
  assert_eq!(raised, (statistics, true));

  for user in [&alice, &bob] {
    user
      .client
      .rooms()
      .join(&room, RoomType::Public)
      .await
      .unwrap();
  }
  bob.client.account().set_shared_counts(3, 30).await.unwrap();
  let (pushed, asked) = alice
    .event(|event| match event {
      Event::User(soulseek_client::user::Event::StatisticsChanged {
        statistics: pushed,
        asked,
      }) if pushed.username == bob.username && pushed.stats.file_count == 30 => {
        Some((pushed, asked))
      }
      _ => None,
    })
    .await;
  assert_eq!(pushed.stats.directory_count, 3);
  assert!(!asked, "pushed, not asked for");
  bob
    .client
    .account()
    .report_upload_speed(2048)
    .await
    .unwrap();
  let pushed = alice
    .event(|event| match event {
      Event::User(soulseek_client::user::Event::StatisticsChanged {
        statistics: pushed, ..
      }) if pushed.username == bob.username && pushed.stats.average_speed > 0 => Some(pushed),
      _ => None,
    })
    .await;
  assert_eq!(pushed.stats.average_speed, 2048);
  assert!(matches!(
    bob.client.account().report_upload_speed(0).await,
    Err(matched) if matched.kind() == ErrorKind::InvalidArgument
  ));
}

/// The client's own upload speed bounds its distributed children. It is asked for once after
/// login, and the server sends it again, unasked and without a watch, each time it changes
#[tokio::test]
async fn our_own_statistics_are_pushed_when_we_report_an_upload_speed() {
  let Some(server) = soulfind::server() else {
    return;
  };
  let mut bob = server.login("bob", options()).await;
  let name = bob.username.clone();
  let own = async |bob: &mut soulfind::User| {
    bob
      .event(|event| match event {
        Event::User(soulseek_client::user::Event::StatisticsChanged { statistics, asked })
          if statistics.username == name =>
        {
          Some((statistics.stats.average_speed, asked))
        }
        _ => None,
      })
      .await
  };
  assert_eq!(own(&mut bob).await, (0, true));
  bob
    .client
    .account()
    .report_upload_speed(2048)
    .await
    .unwrap();
  assert_eq!(own(&mut bob).await, (2048, false));
}
