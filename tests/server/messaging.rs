//! Private messages and the public chat feed on soulfind: delivery, the acknowledgment the
//! application sends that stops the server replaying a message at every login, and messages
//! kept for an offline user.

use crate::support::options;
use crate::support::soulfind::{self, PASSWORD, User};
use soulseek_client::{Error, Event, PrivateMessage};
use soulseek_proto::RoomType;

async fn message(to: &User, from: &User, text: &str) {
  from
    .client
    .users()
    .message(&to.username, text)
    .await
    .unwrap();
}

async fn received(user: &mut User) -> PrivateMessage {
  user
    .event(|event| match event {
      Event::PrivateMessage(message) => Some(message),
      _ => None,
    })
    .await
}

/// logs `user` out and in again on the same client
async fn relogin(server: &soulfind::Soulfind, user: &User) {
  user.client.disconnect().await;
  server
    .connect(&user.client, &user.username, PASSWORD)
    .await
    .unwrap();
}

#[tokio::test]
async fn an_unacknowledged_message_is_replayed_at_the_next_login() {
  let Some(server) = soulfind::server() else {
    return;
  };
  let alice = server.login("alice", options()).await;
  let mut bob = server.login("bob", options()).await;
  message(&bob, &alice, "hello").await;
  let live = received(&mut bob).await;
  assert_eq!(live.username, alice.username);
  assert_eq!(live.message, "hello");
  assert!(!live.replayed);

  relogin(&server, &bob).await;
  let replayed = received(&mut bob).await;
  assert_eq!(replayed.id, live.id);
  assert_eq!(replayed.message, "hello");
  assert!(replayed.replayed);
}

#[tokio::test]
async fn a_message_to_an_offline_user_is_delivered_at_their_login() {
  let Some(server) = soulfind::server() else {
    return;
  };
  let alice = server.login("alice", options()).await;
  let mut bob = server.login("bob", options()).await;
  bob.client.disconnect().await;
  message(&bob, &alice, "while you were away").await;
  // the server handles alice's messages in order, so it has stored the message once it answers
  // the next one
  alice.settle().await;
  server
    .connect(&bob.client, &bob.username, PASSWORD)
    .await
    .unwrap();
  let stored = received(&mut bob).await;
  assert_eq!(stored.message, "while you were away");
  assert!(stored.replayed);
}

#[tokio::test]
async fn the_public_chat_feed_carries_lines_from_rooms_we_are_not_in() {
  let Some(server) = soulfind::server() else {
    return;
  };
  let room = soulfind::username("public");
  let alice = server.login("alice", options()).await;
  let mut carol = server.login("carol", options()).await;
  alice
    .client
    .rooms()
    .join(&room, RoomType::Public)
    .await
    .unwrap();
  carol.client.rooms().start_public_chat().await.unwrap();
  carol.settle().await;
  alice
    .client
    .rooms()
    .say(&room, "hello world")
    .await
    .unwrap();
  let raised = carol
    .event(|event| match event {
      Event::Room(soulseek_client::room::Event::PublicChat {
        room_name,
        username,
        message,
      }) => Some((room_name, username, message)),
      _ => None,
    })
    .await;
  assert_eq!(raised, (room, alice.username.clone(), "hello world".into()));
  carol.client.rooms().stop_public_chat().await.unwrap();
}

#[tokio::test]
async fn a_message_the_application_acknowledges_later_is_not_replayed() {
  let Some(server) = soulfind::server() else {
    return;
  };
  let alice = server.login("alice", options()).await;
  let mut bob = server.login("bob", options()).await;
  message(&bob, &alice, "first").await;
  let first = received(&mut bob).await;
  bob
    .client
    .users()
    .acknowledge_message(first.id)
    .await
    .unwrap();
  bob.settle().await;
  relogin(&server, &bob).await;
  message(&bob, &alice, "second").await;
  assert_eq!(received(&mut bob).await.message, "second");
}

#[tokio::test]
async fn a_message_needs_a_recipient_and_a_text() {
  let Some(server) = soulfind::server() else {
    return;
  };
  let alice = server.login("alice", options()).await;
  for (username, text) in [(" ", "hi"), ("bob", "")] {
    assert!(matches!(
      alice.client.users().message(username, text).await,
      Err(Error::InvalidArgument(_))
    ));
  }
  // a text of only whitespace is still a message
  alice.client.users().message("bob", " ").await.unwrap();
}
