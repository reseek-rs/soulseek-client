//! Finding and keeping a parent in the distributed tree, against a scripted server and scripted
//! candidates: the candidate nearest the top of its branch is adopted once it showed it is a
//! working part of the tree, broken candidates are dropped, the parent's searches go on to the
//! children and are answered, and losing the parent, a reset from the server or a long time
//! without one all lead back to asking the server.

use std::time::Duration;

use crate::support::peer::{FakePeer, PeerSide};
use crate::support::tree::{
  Status, approached, branch_information, branch_level, branch_root, child_requests, join_as_child,
  net_info, search, status, wrapped_search,
};
use crate::support::{
  FakeServer, Logs, PATIENCE, Session, USERNAME, listening_options, next_event,
};
use soulseek_client::handler::{self, SearchReply};
use soulseek_client::{Config, Event, Handler};
use soulseek_proto::distributed::Ping;
use soulseek_proto::{DistributedMessage, ServerResponse};
use tokio::sync::mpsc;

fn parent_options() -> Config {
  let mut options = listening_options();
  options = options
    .limits(|limits| limits.distributed_parent_negotiation_timeout(Duration::from_millis(300)));
  options
}

async fn session() -> Session {
  Session::with_options(parent_options()).await
}

/// answers no search and tells the receiver the token of every search it is asked to answer
struct Asking(mpsc::UnboundedSender<u32>);

impl Handler for Asking {
  async fn search(
    &self,
    _client: &soulseek_client::Client,
    search: &handler::Search,
  ) -> Option<SearchReply> {
    // the test may have stopped listening
    let _ = self.0.send(search.token);
    None
  }
}

/// `options` with an [`Asking`] handler, and the receiver it tells
fn asking(options: Config) -> (Config, mpsc::UnboundedReceiver<u32>) {
  let (tell, asked) = mpsc::unbounded_channel();
  (options.handler(Asking(tell)), asked)
}

/// the token of the next search the client was asked to answer
async fn next_asked(asked: &mut mpsc::UnboundedReceiver<u32>) -> u32 {
  tokio::time::timeout(PATIENCE, asked.recv())
    .await
    .expect("a search reaches the handler")
    .expect("the handler lives as long as the client")
}

/// a candidate that answers as a parent at `level` under `root`
async fn answers(candidate: &FakePeer, level: i32, root: &str) -> PeerSide {
  let mut connection = approached(candidate).await;
  let mut frames = branch_level(level);
  frames.extend(branch_root(root));
  frames.extend(search("someone", 1, "warm up"));
  connection.send(&frames).await;
  connection
}

async fn adopted(session: &mut Session) -> soulseek_client::distributed::Parent {
  session
    .event(|event| match event {
      Event::Distributed(soulseek_client::distributed::Event::ParentAdopted(parent)) => {
        Some(parent)
      }
      _ => None,
    })
    .await
}

#[tokio::test]
async fn durations_too_long_to_add_to_the_clock_stop_nothing() {
  let mut options = parent_options();
  options = options.server_ping_interval(Some(Duration::MAX));
  options = options.upload_notice_timeout(Duration::MAX);
  options = options.unasked_offer_timeout(Duration::MAX);
  options = options.limits(|limits| limits.distributed_parent_retry(Duration::MAX));
  options = options.limits(|limits| limits.distributed_status_debounce(Duration::MAX));
  options = options.limits(|limits| limits.distributed_status_max_age(Duration::MAX));
  options = options.limits(|limits| limits.distributed_search_memory_lifetime(Duration::MAX));
  options = options.limits(|limits| limits.search_response_cache_lifetime(Duration::MAX));
  options = options.limits(|limits| limits.address_cache_lifetime(Duration::MAX));
  let (options, mut asked) = asking(options);
  let mut session = Session::with_options(options).await;
  // the tree's task runs: it adopts a parent and passes its searches on
  let near = FakePeer::start().await;
  session.server.send(&net_info(&[("near", &near)])).await;
  let mut parent = answers(&near, 1, "top").await;
  assert_eq!(adopted(&mut session).await.username, "near");
  parent.send(&search("bob", 40, "first")).await;
  parent.send(&search("bob", 40, "first")).await;
  parent.send(&search("bob", 41, "second")).await;
  assert_eq!(next_asked(&mut asked).await, 40);
  assert_eq!(next_asked(&mut asked).await, 41);
  // a child joining goes through the debounced status, and the tree still runs after it
  let mut child = join_as_child(session.listen_address(), "child").await;
  assert_eq!(branch_information(&mut child).await, (2, "top".into()));
  parent.send(&search("bob", 42, "third")).await;
  assert_eq!(next_asked(&mut asked).await, 42);
}

#[tokio::test]
async fn a_branch_root_that_differs_only_in_case_is_told_to_the_server() {
  let mut options = parent_options();
  options = options.limits(|limits| limits.distributed_status_debounce(Duration::from_millis(50)));
  let mut session = Session::with_options(options).await;
  let near = FakePeer::start().await;
  session.server.send(&net_info(&[("near", &near)])).await;
  let mut parent = answers(&near, 1, "top").await;
  adopted(&mut session).await;
  assert_eq!(status(&mut session.server).await.root, "top");
  // usernames are case-sensitive: this is another root
  parent.send(&branch_root("TOP")).await;
  assert_eq!(status(&mut session.server).await.root, "TOP");
}

#[tokio::test]
async fn the_application_chooses_its_parent_and_can_let_it_go() {
  // the candidates come nearest the top first; this one wants the one named "deep"
  struct Deep;
  impl Handler for Deep {
    fn choose_parent(&self, candidates: &[soulseek_client::distributed::Parent]) -> Option<usize> {
      assert!(
        candidates
          .windows(2)
          .all(|pair| pair[0].branch_level <= pair[1].branch_level)
      );
      candidates
        .iter()
        .position(|candidate| candidate.username == "deep")
    }
  }
  let mut session = Session::with_options(parent_options().handler(Deep)).await;
  let (near, deep) = (FakePeer::start().await, FakePeer::start().await);
  session
    .server
    .send(&net_info(&[("near", &near), ("deep", &deep)]))
    .await;
  let (mut near, _deep) = tokio::join!(answers(&near, 1, "top"), answers(&deep, 4, "top"));
  let parent = adopted(&mut session).await;
  assert_eq!((parent.username.as_str(), parent.branch_level), ("deep", 4));
  near.closed().await;

  session.client.distributed().drop_parent();
  let lost = session
    .event(|event| match event {
      Event::Distributed(soulseek_client::distributed::Event::ParentDisconnected(parent)) => {
        Some(parent.username)
      }
      _ => None,
    })
    .await;
  assert_eq!(lost, "deep");
}

#[tokio::test]
async fn a_candidate_need_not_pass_on_a_search_when_the_options_say_so() {
  let mut options = parent_options();
  options = options.parent_must_relay_search(false);
  let mut session = Session::with_options(options).await;
  let quiet = FakePeer::start().await;
  session.server.send(&net_info(&[("quiet", &quiet)])).await;
  let mut connection = approached(&quiet).await;
  let mut frames = branch_level(2);
  frames.extend(branch_root("top"));
  connection.send(&frames).await;
  assert_eq!(adopted(&mut session).await.username, "quiet");
}

#[tokio::test]
async fn the_candidate_nearest_the_top_is_adopted_and_the_others_let_go() {
  let mut session = session().await;
  let (far, near, broken) = (
    FakePeer::start().await,
    FakePeer::start().await,
    FakePeer::start().await,
  );
  session
    .server
    .send(&net_info(&[
      ("far", &far),
      ("near", &near),
      ("broken", &broken),
    ]))
    .await;
  let (mut far, mut near, mut broken) = tokio::join!(
    answers(&far, 3, "top"),
    answers(&near, 1, "top"),
    answers(&broken, -1, "top"),
  );
  let parent = adopted(&mut session).await;
  assert_eq!(
    (
      parent.username.as_str(),
      parent.branch_level,
      parent.branch_root.as_str()
    ),
    ("near", 1, "top")
  );
  assert!(!parent.is_branch_root);
  far.closed().await;
  broken.closed().await;
  assert_eq!(
    status(&mut session.server).await,
    Status {
      level: 2,
      root: "top".into(),
      accepts_children: true,
      needs_parent: false,
    }
  );
  let info = session.client.distributed().info();
  assert_eq!(info.parent.unwrap().username, "near");
  assert_eq!((info.branch_level, info.branch_root.as_str()), (2, "top"));
  // an older parent's ping carries a token; it is not answered and the parent stays
  near
    .send(&DistributedMessage::Ping(Ping { token: Some(7) }).encode())
    .await;
  assert!(near.quiet_for(Duration::from_millis(100)).await);
  assert!(session.client.distributed().info().parent.is_some());
}

#[tokio::test]
async fn branch_news_a_candidate_sends_before_its_adoption_is_not_lost() {
  let mut session = session().await;
  let (quick, slow) = (FakePeer::start().await, FakePeer::start().await);
  session
    .server
    .send(&net_info(&[("quick", &quick), ("slow", &slow)]))
    .await;
  let (mut quick, _slow) = tokio::join!(answers(&quick, 1, "top"), approached(&slow));
  // quick moves in its branch while the round still waits for slow, which never answers
  let mut news = branch_level(4);
  news.extend(branch_root("higher"));
  quick.send(&news).await;
  assert_eq!(adopted(&mut session).await.username, "quick");
  let branch = || {
    let info = session.client.distributed().info();
    (info.branch_level, info.branch_root)
  };
  let caught_up = tokio::time::timeout(Duration::from_secs(2), async {
    while branch() != (5, "higher".into()) {
      tokio::time::sleep(Duration::from_millis(10)).await;
    }
  })
  .await;
  assert!(caught_up.is_ok(), "our branch stayed {:?}", branch());
}

#[tokio::test]
async fn candidates_that_break_the_rules_are_dropped() {
  let logs = Logs::capture();
  let mut session = session().await;
  let (silent, server_root, negative) = (
    FakePeer::start().await,
    FakePeer::start().await,
    FakePeer::start().await,
  );
  session
    .server
    .send(&net_info(&[
      ("silent", &silent),
      ("fake", &server_root),
      ("negative", &negative),
    ]))
    .await;
  let (mut silent, mut server_root, mut negative) = tokio::join!(
    approached(&silent),
    approached(&server_root),
    approached(&negative),
  );
  // level and root but no search within the negotiation timeout
  let mut frames = branch_level(1);
  frames.extend(branch_root("top"));
  silent.send(&frames).await;
  // the reserved name can root no branch
  let mut frames = branch_level(1);
  frames.extend(branch_root("server"));
  server_root.send(&frames).await;
  negative.send(&branch_level(-1)).await;
  for candidate in [&mut silent, &mut server_root, &mut negative] {
    candidate.closed().await;
  }
  logs
    .wait_for(&["failed to connect to any of the available parent candidates"])
    .await;
  for reason in [
    "one or more required messages was not received",
    "invalid branch root",
    "invalid branch level",
  ] {
    assert_eq!(
      logs.lines_with(&["connection closed", reason]).len(),
      1,
      "{reason}"
    );
  }
  assert!(session.client.distributed().info().parent.is_none());
}

#[tokio::test]
async fn a_root_candidate_need_not_name_itself() {
  let mut session = session().await;
  let root = FakePeer::start().await;
  session.server.send(&net_info(&[("old qt", &root)])).await;
  let mut connection = approached(&root).await;
  // an older client at the top of its branch sends no root, and its search wrapped with a
  // four-byte inner code
  let mut frames = branch_level(0);
  frames.extend(wrapped_search("someone", 1, "warm up"));
  connection.send(&frames).await;
  let parent = adopted(&mut session).await;
  assert_eq!(parent.branch_root, "old qt");
  assert!(parent.is_branch_root);
  assert_eq!(status(&mut session.server).await.level, 1);
}

/// a client with an adopted parent and one child below it
async fn with_parent_and_child() -> (Session, PeerSide, PeerSide) {
  with_parent_and_child_on(parent_options()).await
}

/// [`with_parent_and_child`] for a client made with `options`
async fn with_parent_and_child_on(options: Config) -> (Session, PeerSide, PeerSide) {
  let mut session = Session::with_options(options).await;
  let candidate = FakePeer::start().await;
  session
    .server
    .send(&net_info(&[("parent", &candidate)]))
    .await;
  let parent = answers(&candidate, 1, "top").await;
  adopted(&mut session).await;
  status(&mut session.server).await;
  let mut child = join_as_child(session.listen_address(), "child").await;
  assert_eq!(branch_information(&mut child).await, (2, "top".into()));
  (session, parent, child)
}

#[tokio::test]
async fn the_parents_searches_go_on_to_the_children_and_are_answered() {
  let logs = Logs::capture();
  let (options, mut asked) = asking(parent_options());
  let (session, mut parent, mut child) = with_parent_and_child_on(options).await;
  let theirs = search("bob", 21, "jazz");
  parent.send(&theirs).await;
  assert_eq!(
    child
      .frame(soulseek_proto::frame::FrameKind::Distributed)
      .await,
    theirs
  );
  assert_eq!(next_asked(&mut asked).await, 21);

  // the client's own search goes on and is asked of the handler like any other; a repeat of
  // the last frame and a search with a bad identifier go nowhere
  let ours = search(USERNAME, 22, "mine");
  let mut bad = search("bob", 23, "bad");
  bad[5] = 0;
  let after = search("bob", 24, "after");
  let mut frames = ours.clone();
  frames.extend(ours.clone());
  frames.extend(bad);
  frames.extend(after.clone());
  parent.send(&frames).await;
  assert_eq!(
    child
      .frame(soulseek_proto::frame::FrameKind::Distributed)
      .await,
    ours
  );
  assert_eq!(
    child
      .frame(soulseek_proto::frame::FrameKind::Distributed)
      .await,
    after
  );
  assert_eq!(next_asked(&mut asked).await, 22);
  assert_eq!(next_asked(&mut asked).await, 24);
  assert_eq!(logs.lines_with(&["bad identifier dropped"]).len(), 1);

  // a repeat is remembered past the search that came between
  let between = search("bob", 26, "between");
  let mut frames = between.clone();
  frames.extend(after);
  parent.send(&frames).await;
  parent.send(&search("bob", 27, "last")).await;
  assert_eq!(
    child
      .frame(soulseek_proto::frame::FrameKind::Distributed)
      .await,
    between
  );
  assert_eq!(
    child
      .frame(soulseek_proto::frame::FrameKind::Distributed)
      .await,
    search("bob", 27, "last")
  );
  assert_eq!(next_asked(&mut asked).await, 26);
  assert_eq!(next_asked(&mut asked).await, 27);

  // a patch that shortens the memory lets a repeat go on once it is past
  session
    .client
    .reconfigure(|config| {
      config.limits(|limits| limits.distributed_search_memory_lifetime(Duration::from_millis(20)))
    })
    .await
    .unwrap();
  tokio::time::sleep(Duration::from_millis(100)).await;
  parent.send(&between).await;
  assert_eq!(
    child
      .frame(soulseek_proto::frame::FrameKind::Distributed)
      .await,
    between
  );
  assert_eq!(next_asked(&mut asked).await, 26);

  // an old parent forwarding the server's wrapper: the child gets the search inside
  parent.send(&wrapped_search("carol", 25, "wrapped")).await;
  assert_eq!(
    child.read_distributed().await,
    DistributedMessage::decode(&search("carol", 25, "wrapped")).unwrap()
  );
  assert!(
    session
      .client
      .distributed()
      .info()
      .average_broadcast_latency
      .is_some()
  );
}

#[tokio::test]
async fn a_change_in_the_parents_branch_reaches_the_children_and_the_server() {
  let (mut session, mut parent, mut child) = with_parent_and_child().await;
  let mut frames = branch_level(4);
  frames.extend(branch_root("higher"));
  parent.send(&frames).await;
  // when the tree task takes both at once, the children hear only the latest
  let mut heard = branch_information(&mut child).await;
  if heard == (5, "top".into()) {
    heard = branch_information(&mut child).await;
  }
  assert_eq!(heard, (5, "higher".into()));
  // the server hears it once the changes have settled
  tokio::time::pause();
  tokio::time::advance(Duration::from_secs(6)).await;
  tokio::time::resume();
  let reported = status(&mut session.server).await;
  assert_eq!((reported.level, reported.root.as_str()), (5, "higher"));
}

#[tokio::test]
async fn a_parent_flooding_its_branch_does_not_crowd_out_the_server() {
  let logs = Logs::capture();
  let (mut options, mut asked) = asking(parent_options());
  options = options.server_connection(|connection| connection.write_queue_size(4096));
  let mut session = Session::on(FakeServer::with_receive_buffer(4096).await, options).await;
  session.stall_server_writes();
  let candidate = FakePeer::start().await;
  session
    .server
    .send(&net_info(&[("parent", &candidate)]))
    .await;
  let mut parent = answers(&candidate, 1, "top").await;
  // the tree task now waits on the status it writes for the adoption
  adopted(&mut session).await;
  // many more changes than the command queue holds, then a search that shows all were read
  let mut frames: Vec<u8> = (0..5000).flat_map(|n| branch_level(1 + n % 2)).collect();
  frames.extend(branch_level(7));
  frames.extend(search("bob", 30, "after"));
  parent.send(&frames).await;
  assert_eq!(next_asked(&mut asked).await, 30);
  session.server.send(&ServerResponse::DistributedReset).await;
  let Session { server, events, .. } = &mut session;
  let lost = next_event(events, |event| match event {
    Event::Distributed(soulseek_client::distributed::Event::ParentDisconnected(parent)) => {
      Some(parent.branch_level)
    }
    _ => None,
  });
  let lost = tokio::select! {
    lost = lost => lost,
    () = server.discard() => unreachable!("the server reads until the client closes"),
  };
  // the tree followed the parent to its last level and then carried out the reset
  assert_eq!(lost, 7);
  assert!(logs.lines_with(&["command queue is full"]).is_empty());
}

#[tokio::test]
async fn losing_the_parent_tells_the_children_and_asks_the_server_again() {
  let (mut session, parent, mut child) = with_parent_and_child().await;
  drop(parent);
  let lost = session
    .event(|event| match event {
      Event::Distributed(soulseek_client::distributed::Event::ParentDisconnected(parent)) => {
        Some(parent)
      }
      _ => None,
    })
    .await;
  assert_eq!((lost.username.as_str(), lost.branch_level), ("parent", 1));
  assert_eq!(branch_information(&mut child).await, (0, USERNAME.into()));
  // no candidates are left, so the server is asked for some
  let reported = status(&mut session.server).await;
  assert!(reported.needs_parent);
  assert_eq!(reported.level, 0);
}

#[tokio::test]
async fn a_reset_from_the_server_drops_the_tree_and_starts_over() {
  let (mut session, mut parent, mut child) = with_parent_and_child().await;
  session.server.send(&ServerResponse::DistributedReset).await;
  parent.closed().await;
  child.closed().await;
  let mut parent_gone = false;
  let mut child_gone = false;
  while !(parent_gone && child_gone) {
    session
      .event(|event| match event {
        Event::Distributed(soulseek_client::distributed::Event::ParentDisconnected(_)) => {
          parent_gone = true;
          Some(())
        }
        Event::Distributed(soulseek_client::distributed::Event::ChildDisconnected { .. }) => {
          child_gone = true;
          Some(())
        }
        _ => None,
      })
      .await;
  }
  assert_eq!(
    status(&mut session.server).await,
    Status {
      level: 0,
      root: USERNAME.into(),
      accepts_children: false,
      needs_parent: true,
    }
  );
  assert!(session.client.distributed().info().children.is_empty());
}

#[tokio::test]
async fn without_parent_or_branch_the_server_is_asked_again_every_fifteen_minutes() {
  let logs = Logs::capture();
  let mut session = session().await;
  tokio::time::pause();
  tokio::time::advance(Duration::from_secs(15 * 60)).await;
  tokio::time::resume();
  // the status has not changed, and is sent again all the same
  assert!(status(&mut session.server).await.needs_parent);
  assert_eq!(
    logs
      .lines_with(&["no distributed parent; asking the server for candidates"])
      .len(),
    1
  );
}

#[tokio::test]
async fn a_parent_lost_while_the_server_is_slow_is_still_replaced() {
  let logs = Logs::capture();
  let mut options = parent_options();
  options = options.server_connection(|connection| connection.write_queue_size(4096));
  let mut session = Session::on(FakeServer::with_receive_buffer(4096).await, options).await;
  session.stall_server_writes();
  let (first, second) = (FakePeer::start().await, FakePeer::start().await);
  session
    .server
    .send(&net_info(&[("first", &first), ("second", &second)]))
    .await;
  let (lost, _) = tokio::join!(answers(&first, 1, "top"), answers(&second, 2, "top"));
  // the tree task now waits on the status it writes for the adoption
  assert_eq!(adopted(&mut session).await.username, "first");
  session.server.send_bytes(&child_requests(1100)).await;
  logs
    .wait_for(&["distributed network command queue is full, command dropped"])
    .await;
  let asked_anew = || {
    logs
      .lines_with(&["requesting a new list of candidates"])
      .len()
  };
  // counted before the parent goes, so handling its loss is seen however soon it comes
  let before = asked_anew();
  drop(lost);
  logs
    .wait_for(&[
      "ParentLost",
      "distributed network command queue is full, waiting",
    ])
    .await;
  let Session { server, events, .. } = &mut session;
  // the loss is handled once the queue drains: the offer it came from is used up, so the
  // client asks the server for candidates anew
  let handled = tokio::time::timeout(PATIENCE, async {
    while asked_anew() == before {
      tokio::time::sleep(Duration::from_millis(10)).await;
    }
  });
  tokio::select! {
    handled = handled => handled.expect("the lost parent is handled"),
    () = server.discard() => unreachable!("the server reads until the client closes"),
  }
  server.send(&net_info(&[("second", &second)])).await;
  let replaced = async {
    let _second = answers(&second, 2, "top").await;
    next_event(events, |event| match event {
      Event::Distributed(soulseek_client::distributed::Event::ParentAdopted(parent)) => {
        Some(parent.username)
      }
      _ => None,
    })
    .await
  };
  let replaced = tokio::select! {
    replaced = replaced => replaced,
    () = server.discard() => unreachable!("the server reads until the client closes"),
  };
  assert_eq!(replaced, "second");
}
