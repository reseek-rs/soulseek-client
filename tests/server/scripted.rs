//! Server messages soulfind never sends, delivered by a scripted server: privilege notifications,
//! the distributed tree's tuning values, the shapes older or newer servers send, codes nothing
//! handles, malformed frames and unread bytes, statuses timed against the client's requests for
//! them, and the stream of one topic each event reaches.

use crate::support::{Logs, Session};
use soulseek_client::Event;
use soulseek_proto::server::*;
use soulseek_proto::{ServerRequest, ServerResponse};
use std::time::Duration;

#[tokio::test]
async fn privilege_notifications_are_raised_with_what_acknowledging_needs() {
  let mut session = Session::start().await;
  session
    .server
    .send(&ServerResponse::AddPrivilegedUser(AddPrivilegedUser {
      username: "bob".into(),
    }))
    .await;
  session
    .server
    .send(&ServerResponse::NotifyPrivileges(NotifyPrivileges {
      id: 9,
      username: "carol".into(),
    }))
    .await;
  for expected in [("bob".into(), None), ("carol".into(), Some(9))] {
    let raised = session
      .event(|event| match event {
        Event::Session(soulseek_client::session::Event::PrivilegeNotification { username, id }) => {
          Some((username, id))
        }
        _ => None,
      })
      .await;
    assert_eq!(raised, expected);
  }
}

#[tokio::test]
async fn tuning_values_merge_into_the_server_info() {
  let mut session = Session::start().await;
  session
    .server
    .send(&ServerResponse::ParentMinSpeed(ParentMinSpeed { speed: 1 }))
    .await;
  session
    .server
    .send(&ServerResponse::ParentSpeedRatio(ParentSpeedRatio {
      ratio: 50,
    }))
    .await;
  session
    .server
    .send(&ServerResponse::WishlistInterval(WishlistInterval {
      seconds: 720,
    }))
    .await;
  let merged = session
    .event(|event| match event {
      Event::Session(soulseek_client::session::Event::ServerInfo(info))
        if info.wishlist_interval.is_some() =>
      {
        Some(info)
      }
      _ => None,
    })
    .await;
  let mut expected = soulseek_client::session::ServerInfo::default();
  expected.parent_min_speed = Some(1);
  expected.parent_speed_ratio = Some(50);
  expected.wishlist_interval = Some(Duration::from_secs(720));
  expected.is_supporter = Some(true);
  expected.own_public_ip = Some(crate::support::PUBLIC_IP);
  assert_eq!(merged, expected);
  assert_eq!(session.client.server_info(), expected);
}

#[tokio::test]
async fn obsolete_tuning_values_are_stored_and_not_used() {
  let logs = Logs::capture();
  let mut session = Session::start().await;
  for response in [
    ServerResponse::ParentInactivityTimeout(ParentInactivityTimeout { seconds: 300 }),
    ServerResponse::SearchInactivityTimeout(SearchInactivityTimeout { seconds: 30 }),
    ServerResponse::MinimumParentsInCache(MinimumParentsInCache { count: 10 }),
    ServerResponse::DistributedAliveInterval(DistributedAliveInterval { seconds: 60 }),
  ] {
    session.server.send(&response).await;
  }
  session.settle().await;
  let info = session.client.server_info();
  assert_eq!(
    info.parent_inactivity_timeout,
    Some(Duration::from_secs(300))
  );
  assert_eq!(
    info.search_inactivity_timeout,
    Some(Duration::from_secs(30))
  );
  assert_eq!(info.minimum_parents_in_cache, Some(10));
  assert_eq!(
    info.distributed_alive_interval,
    Some(Duration::from_secs(60))
  );
  assert_eq!(
    logs.lines_with(&["obsolete tuning value", "code=86"]).len(),
    1
  );
}

#[tokio::test]
async fn unhandled_and_obsolete_codes_are_logged_and_the_connection_stays_up() {
  let logs = Logs::capture();
  let mut session = Session::start().await;
  let mut events = session.client.events();
  // a code the server echoes that nothing handles, a code nobody knows, an obsolete code with a
  // decoder and one without
  session
    .server
    .send_bytes(&[8, 0, 0, 0, 2, 0, 0, 0, 1, 0, 0, 0])
    .await;
  session
    .server
    .send_bytes(&[4, 0, 0, 0, 0x0f, 0x27, 0, 0])
    .await;
  session
    .server
    .send(&ServerResponse::IgnoreUser(IgnoreUser {
      username: "bob".into(),
    }))
    .await;
  session.server.send_bytes(&[4, 0, 0, 0, 58, 0, 0, 0]).await;
  session.settle().await;
  assert_eq!(
    logs
      .lines_with(&["unhandled server message", "code=2 "])
      .len(),
    1
  );
  assert_eq!(
    logs
      .lines_with(&["unhandled server message", "code=9999"])
      .len(),
    1
  );
  assert_eq!(
    logs
      .lines_with(&["obsolete server message", "code=11", "IgnoreUser"])
      .len(),
    1
  );
  assert_eq!(
    logs
      .lines_with(&["obsolete server message", "code=58", "AdminCommand"])
      .len(),
    1
  );
  // the application gets each as it came
  let mut unhandled = Vec::new();
  while let Some(event) = crate::support::try_next(&mut events) {
    if let Event::Session(soulseek_client::session::Event::Unhandled { code, frame }) = event {
      unhandled.push((code, frame.len()));
    }
  }
  assert_eq!(unhandled, [(2, 12), (9999, 8), (11, 15), (58, 8)]);
  assert_eq!(
    session.client.state(),
    soulseek_client::session::State::LoggedIn
  );
}

#[tokio::test]
async fn a_malformed_message_is_dropped_with_one_warning_a_minute_and_the_connection_stays_up() {
  let logs = Logs::capture();
  let mut session = Session::start().await;
  // GetStatus cut off inside its presence
  let mut payload = 7u32.to_le_bytes().to_vec();
  payload.extend(3u32.to_le_bytes());
  payload.extend(b"bob");
  payload.extend([2, 0]);
  let mut frame = (payload.len() as u32).to_le_bytes().to_vec();
  frame.extend(payload);
  session.server.send_bytes(&frame).await;
  session.server.send_bytes(&frame).await;
  session.settle().await;
  assert_eq!(
    session.client.state(),
    soulseek_client::session::State::LoggedIn
  );
  let warned = logs.lines_with(&["WARN", "server message not read", "GetStatus"]);
  assert_eq!(warned.len(), 1, "{}", logs.text());
  let rejected = logs.lines_with(&["DEBUG", "message rejected", "code=7 "]);
  assert_eq!(rejected.len(), 2, "the decoder tells each: {}", logs.text());
}

#[tokio::test]
async fn unread_bytes_are_reported_at_trace() {
  let logs = Logs::capture();
  let mut session = Session::start().await;
  let mut frame = ServerResponse::GlobalAdminMessage(GlobalAdminMessage {
    message: "with a tail".into(),
  })
  .encode();
  frame.extend([1, 2, 3]);
  let len = (frame.len() - 4) as u32;
  frame[..4].copy_from_slice(&len.to_le_bytes());
  session.server.send_bytes(&frame).await;
  session.settle().await;
  assert_eq!(
    logs
      .lines_with(&["bytes past its fields", "code=66", "unread=3"])
      .len(),
    1
  );
}

/// soulfind drops a private message holding a line break without telling either side, so the
/// line breaks are carried here, both ways, by a server that does not
#[tokio::test]
async fn a_private_message_keeps_its_line_breaks() {
  let mut session = Session::start().await;
  session
    .client
    .users()
    .message("bob", "one\ntwo\r\nthree")
    .await
    .unwrap();
  assert_eq!(
    session.server.request().await,
    ServerRequest::PrivateMessage(PrivateMessageRequest {
      username: "bob".into(),
      message: "one\ntwo\r\nthree".into(),
    })
  );
  session
    .server
    .send(&ServerResponse::PrivateMessage(PrivateMessageResponse {
      id: 4,
      timestamp: 1_700_000_000,
      username: "bob".into(),
      message: "four\nfive".into(),
      is_new: true,
    }))
    .await;
  let received = session
    .event(|event| match event {
      Event::PrivateMessage(message) => Some(message),
      _ => None,
    })
    .await;
  assert_eq!(received.message, "four\nfive");
}

/// bob's status as the server sends it, asked for or not
fn bob_is(presence: soulseek_proto::UserPresence) -> ServerResponse {
  ServerResponse::GetStatus(UserStatus {
    username: "bob".into(),
    presence,
    is_privileged: false,
  })
}

/// The presence of the next status of bob `session` raises, and whether it was asked for.
async fn bob_raised(session: &mut Session) -> (soulseek_proto::UserPresence, bool) {
  session
    .event(|event| match event {
      Event::User(soulseek_client::user::Event::StatusChanged { status, asked })
        if status.username == "bob" =>
      {
        Some((status.presence, asked))
      }
      _ => None,
    })
    .await
}

fn asks_for_bob(request: ServerRequest) -> Option<()> {
  matches!(request, ServerRequest::GetStatus(asked) if asked.username == "bob").then_some(())
}

#[tokio::test]
async fn a_status_is_raised_as_asked_only_while_a_request_for_it_waits() {
  use soulseek_proto::UserPresence;

  let mut session = Session::start().await;
  let client = session.client.clone();
  let mut asking = Box::pin(client.users().status("bob"));
  tokio::select! {
    biased;
    _ = &mut asking => panic!("answered before the server was asked"),
    () = session.server.request_matching(asks_for_bob) => {}
  }
  session.server.send(&bob_is(UserPresence::Online)).await;
  assert_eq!(bob_raised(&mut session).await, (UserPresence::Online, true));
  assert_eq!(asking.await.unwrap().presence, UserPresence::Online);

  // a request given up stops waiting as it is dropped, so what comes next is pushed
  let mut given_up = Box::pin(client.users().status("bob"));
  tokio::select! {
    biased;
    _ = &mut given_up => panic!("answered before the server was asked"),
    () = session.server.request_matching(asks_for_bob) => {}
  }
  drop(given_up);
  session.server.send(&bob_is(UserPresence::Away)).await;
  assert_eq!(bob_raised(&mut session).await, (UserPresence::Away, false));
}

#[tokio::test]
async fn of_two_status_requests_one_dropped_after_its_reply_leaves_the_others_reply_asked() {
  use soulseek_proto::UserPresence;

  let mut session = Session::start().await;
  let client = session.client.clone();
  let mut first = Box::pin(client.users().status("bob"));
  let mut second = Box::pin(client.users().status("bob"));
  for asking in [&mut first, &mut second] {
    tokio::select! {
      biased;
      _ = asking => panic!("answered before the server was asked"),
      () = session.server.request_matching(asks_for_bob) => {}
    }
  }
  // the first request's reply comes and is raised before the request is polled again
  session.server.send(&bob_is(UserPresence::Online)).await;
  assert_eq!(bob_raised(&mut session).await, (UserPresence::Online, true));
  drop(first);
  session.server.send(&bob_is(UserPresence::Online)).await;
  assert_eq!(
    bob_raised(&mut session).await,
    (UserPresence::Online, true),
    "the second request still waits, so its reply is raised as asked"
  );
  assert_eq!(second.await.unwrap().presence, UserPresence::Online);
}

#[tokio::test]
async fn a_room_name_goes_to_the_server_unchecked_when_the_options_say_so() {
  let mut options = crate::support::options();
  options = options.check_names(false);
  let mut session = Session::with_options(options).await;
  let joining = tokio::spawn({
    let client = session.client.clone();
    async move {
      client
        .rooms()
        .join("two  spaces", soulseek_proto::RoomType::Public)
        .await
    }
  });
  let asked = session
    .server
    .request_matching(|request| match request {
      ServerRequest::JoinRoom(join) => Some(join.room),
      _ => None,
    })
    .await;
  assert_eq!(asked, "two  spaces");
  joining.abort();
}

/// the next item of `events`, which is an event
async fn first<T>(events: &mut soulseek_client::Events<T>) -> T {
  tokio::time::timeout(crate::support::PATIENCE, events.next())
    .await
    .expect("an event arrives")
    .expect("the client is there")
    .expect("no event was dropped")
}

#[tokio::test]
async fn a_stream_of_one_topic_gets_that_topics_events_only() {
  let mut session = Session::start().await;
  let users = session.client.users();
  let (mut statuses, mut messages) = (users.events(), users.messages());
  let mut sessions = session.client.session_events();
  let mut rooms = session.client.rooms().events();
  session
    .server
    .send(&ServerResponse::NotifyPrivileges(NotifyPrivileges {
      id: 9,
      username: "carol".into(),
    }))
    .await;
  session
    .server
    .send(&bob_is(soulseek_proto::UserPresence::Away))
    .await;
  session
    .server
    .send(&ServerResponse::PrivateMessage(PrivateMessageResponse {
      id: 4,
      timestamp: 1_700_000_000,
      username: "bob".into(),
      message: "hi".into(),
      is_new: true,
    }))
    .await;
  assert!(matches!(
    first(&mut sessions).await,
    soulseek_client::session::Event::PrivilegeNotification { id: Some(9), .. }
  ));
  assert!(matches!(
    first(&mut statuses).await,
    soulseek_client::user::Event::StatusChanged { status, .. } if status.username == "bob"
  ));
  assert_eq!(first(&mut messages).await.message, "hi");
  // none of it was about a room
  let mut room = std::pin::pin!(rooms.next());
  assert!(crate::support::poll_once(&mut room).await.is_pending());
}

#[tokio::test]
async fn a_room_entered_says_whether_a_join_waited_for_it() {
  let mut session = Session::start().await;
  let mut rooms = session.client.rooms().events();
  // a reply that comes with no join waiting, as one after the join gave up does
  let room = RoomData {
    name: "late".into(),
    users: Vec::new(),
    private: None,
  };
  session.server.send(&ServerResponse::JoinRoom(room)).await;
  match first(&mut rooms).await {
    soulseek_client::room::Event::Entered { room, asked } => {
      assert_eq!((room.name.as_str(), asked), ("late", false));
    }
    other => panic!("{other:?}"),
  }
}

#[tokio::test]
async fn what_one_server_said_is_forgotten_at_another() {
  let mut session = Session::start().await;
  session
    .server
    .send(&ServerResponse::MinimumParentsInCache(
      MinimumParentsInCache { count: 10 },
    ))
    .await;
  session.settle().await;
  assert_eq!(
    session.client.server_info().minimum_parents_in_cache,
    Some(10)
  );
  session.client.disconnect().await;
  let other = crate::support::FakeServer::start().await;
  crate::support::aim(&session.client, other.address()).await;
  let connecting = {
    let client = session.client.clone();
    tokio::spawn(async move {
      client
        .connect(crate::support::USERNAME, crate::support::PASSWORD)
        .await
    })
  };
  other.accept().await.accept_login().await;
  connecting.await.unwrap().unwrap();
  assert_eq!(session.client.server_info().minimum_parents_in_cache, None);
}
