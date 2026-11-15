//! Message connections between two clients on soulfind: made directly, made through the server's
//! relay when the other side cannot be dialed, shared by concurrent callers, cached, replaced
//! on request or when the cache is full, and carrying peer messages both ways.

use crate::support::PATIENCE;
use crate::support::soulfind::{self, Soulfind, User};
use soulseek_client::{Client, ClientError, ClientOptions, ConnKind, MessageConn, WaitKey};
use soulseek_proto::peer::UserInfo;
use soulseek_proto::{PeerCode, PeerMessage};
use tokio_util::sync::CancellationToken;

async fn connect(from: &Client, to: &User) -> MessageConn {
  tokio::time::timeout(
    PATIENCE,
    from.get_or_add_message_connection(&to.username, None, &CancellationToken::new()),
  )
  .await
  .expect("the connection is made")
  .unwrap()
}

/// asks the other end of `conn` for its user info, proving both ends read and answer
async fn info_over(client: &Client, conn: &MessageConn, username: &str) -> UserInfo {
  let reply = client.waiter().wait::<UserInfo>(
    WaitKey::Peer {
      code: PeerCode::InfoResponse,
      username: username.into(),
    },
    None,
    CancellationToken::new(),
  );
  conn.write(PeerMessage::InfoRequest.encode()).await.unwrap();
  tokio::time::timeout(PATIENCE, reply)
    .await
    .expect("the peer answers")
    .unwrap()
}

/// The message connections `client` holds once it holds one for `username`. A connection the
/// user opened is theirs once the server relays their request to connect, which a dial sends
/// beside it, with the address it came from.
async fn once_holding(client: &Client, username: &str) -> Vec<(String, std::net::SocketAddr)> {
  tokio::time::timeout(PATIENCE, async {
    loop {
      let held = client.message_connections();
      if held.iter().any(|(user, _)| user == username) {
        return held;
      }
      tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
  })
  .await
  .expect("the connection is taken as the user's")
}

/// a wait on `username`'s user info that nothing will answer, registered now
fn unanswered_info(
  client: &Client,
  username: &str,
) -> impl Future<Output = Result<UserInfo, ClientError>> {
  client.waiter().wait::<UserInfo>(
    WaitKey::Peer {
      code: PeerCode::InfoResponse,
      username: username.into(),
    },
    Some(PATIENCE * 6),
    CancellationToken::new(),
  )
}

/// the wait fails as soon as the connection to `username` closes, long before its own timeout
async fn fails_on_close(
  wait: impl Future<Output = Result<UserInfo, ClientError>>,
  username: &str,
  reason: &str,
) {
  let error = tokio::time::timeout(PATIENCE, wait)
    .await
    .expect("the wait ends when the connection closes")
    .unwrap_err();
  assert_eq!(
    error.to_string(),
    format!("The message connection to {username} closed: {reason}")
  );
}

async fn two(
  server: &std::sync::Arc<Soulfind>,
  alice: ClientOptions,
  bob: ClientOptions,
) -> (User, User) {
  (
    server.login("alice", alice).await,
    server.login("bob", bob).await,
  )
}

#[tokio::test]
async fn a_direct_connection_carries_messages_both_ways() {
  let Some(server) = soulfind::server() else {
    return;
  };
  let (alice, bob) = two(&server, soulfind::listening(), soulfind::listening()).await;
  let conn = connect(&bob.client, &alice).await;
  assert_eq!(conn.handle().kind(), ConnKind::OUTBOUND | ConnKind::DIRECT);
  assert_eq!(conn.handle().username(), Some(alice.username.as_str()));
  let info = info_over(&bob.client, &conn, &alice.username).await;
  assert_eq!(info.upload_permitted, Some(0));
  // alice answers on it, and takes it as bob's
  let alices = once_holding(&alice.client, &bob.username).await;
  assert_eq!(alices.len(), 1);
  let back = connect(&alice.client, &bob).await;
  assert_eq!(back.handle().kind(), ConnKind::INBOUND | ConnKind::DIRECT);
  info_over(&alice.client, &back, &bob.username).await;
}

#[tokio::test]
async fn a_user_that_cannot_be_dialed_is_reached_through_the_server() {
  let Some(server) = soulfind::server() else {
    return;
  };
  let (alice, bob) = two(&server, soulfind::unreachable(), soulfind::listening()).await;
  let conn = connect(&bob.client, &alice).await;
  assert_eq!(
    conn.handle().kind(),
    ConnKind::OUTBOUND | ConnKind::INDIRECT
  );
  info_over(&bob.client, &conn, &alice.username).await;
  let back = connect(&alice.client, &bob).await;
  assert_eq!(back.handle().kind(), ConnKind::INBOUND | ConnKind::INDIRECT);
  info_over(&alice.client, &back, &bob.username).await;
}

#[tokio::test]
async fn concurrent_callers_share_one_attempt_and_later_ones_the_cache() {
  let Some(server) = soulfind::server() else {
    return;
  };
  let (alice, bob) = two(&server, soulfind::listening(), soulfind::listening()).await;
  let (first, second) = tokio::join!(connect(&bob.client, &alice), connect(&bob.client, &alice));
  assert_eq!(first.handle().id(), second.handle().id());
  let third = connect(&bob.client, &alice).await;
  assert_eq!(third.handle().id(), first.handle().id());
  info_over(&bob.client, &first, &alice.username).await;
  assert_eq!(once_holding(&alice.client, &bob.username).await.len(), 1);
}

#[tokio::test]
async fn a_full_cache_evicts_the_idlest_connection() {
  let Some(server) = soulfind::server() else {
    return;
  };
  let mut options = soulfind::listening();
  options.limits.max_cached_peer_connections = 1;
  let (alice, bob) = two(&server, soulfind::listening(), options).await;
  let carol = server.login("carol", soulfind::listening()).await;
  let evicted = connect(&bob.client, &alice).await;
  let pending = unanswered_info(&bob.client, &alice.username);
  let kept = connect(&bob.client, &carol).await;
  let reason = tokio::time::timeout(PATIENCE, evicted.handle().disconnected())
    .await
    .unwrap();
  assert_eq!(reason.message, "Evicted");
  fails_on_close(pending, &alice.username, "Evicted").await;
  info_over(&bob.client, &kept, &carol.username).await;
}

#[tokio::test]
async fn an_offline_user_cannot_be_connected_to() {
  let Some(server) = soulfind::server() else {
    return;
  };
  let bob = server.login("bob", soulfind::listening()).await;
  let error = bob
    .client
    .get_or_add_message_connection(&soulfind::username("gone"), None, &CancellationToken::new())
    .await
    .unwrap_err();
  assert!(matches!(error, ClientError::UserOffline(_)), "{error:?}");
}
