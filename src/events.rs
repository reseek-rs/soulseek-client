//! What the client tells the application: every state change, every pushed server message the
//! application acts on, and how its transfers and browses go. Events are grouped by what they
//! are about, one enum per topic in that topic's module. A subscriber that falls more than the
//! `event_capacity` setting behind is told how many events it missed and goes on from the oldest
//! one still held.

use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::SystemTime;

use futures_core::Stream;
use tokio::sync::broadcast;
use tokio::sync::broadcast::error::RecvError;
use tokio_util::sync::ReusableBoxFuture;

use crate::{distributed, room, search, session, transfer, user};

/// One thing that happened in the client.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum Event {
  /// Something happened to the server session.
  Session(session::Event),
  /// The server told the client something about a user.
  User(user::Event),
  /// Something happened in a chat room.
  Room(room::Event),
  /// The client's place in the distributed search tree changed.
  Distributed(distributed::Event),
  /// Something happened to a search.
  Search(search::Event),
  /// Something happened to a transfer.
  Transfer(transfer::Event),
  /// A private message to the client, live or replayed at login.
  PrivateMessage(PrivateMessage),
  /// The subscriber fell behind and this many events were dropped before the next one.
  Lagged(u64),
}

/// Private message to the client.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct PrivateMessage {
  /// Id to acknowledge the message with.
  ///
  /// Acknowledging the message stops the server replaying it at login.
  pub id: u32,
  /// When the server says the message was sent.
  pub timestamp: SystemTime,
  /// User who sent the message.
  pub username: String,
  /// What the user wrote.
  pub message: String,
  /// Whether the server is replaying it from its store of unacknowledged messages.
  pub replayed: bool,
}

type Received = (Result<Event, RecvError>, broadcast::Receiver<Event>);

/// Events of a client, from the moment the stream was made.
///
/// Made by [`Client::events`](crate::Client::events), which gives every event as an [`Event`],
/// and by the methods that give the events of one topic, such as
/// [`Rooms::events`](crate::room::Rooms::events), which give that topic's own type.
///
/// A stream that falls further behind than
/// [`Config::event_capacity`](crate::Config::event_capacity) loses the events dropped and goes
/// on from the oldest one still held. A stream of every event is told with an
/// [`Event::Lagged`] holding the number dropped. A stream of one topic has no such event, so the
/// loss is only logged. The stream ends when the last clone of the client is dropped.
pub struct Events<T = Event> {
  next: ReusableBoxFuture<'static, Received>,
  /// what the stream yields of an event, or `None` for one it leaves out
  pick: fn(Event) -> Option<T>,
}

impl Events {
  pub(crate) fn new(receiver: broadcast::Receiver<Event>) -> Self {
    Self::picking(receiver, Some)
  }
}

impl<T> Events<T> {
  /// a stream of what `pick` takes from each event
  pub(crate) fn picking(
    receiver: broadcast::Receiver<Event>,
    pick: fn(Event) -> Option<T>,
  ) -> Self {
    Self {
      next: ReusableBoxFuture::new(receive(receiver)),
      pick,
    }
  }

  /// Returns the next event, or `None` once the client is gone.
  ///
  /// # Cancel safety
  ///
  /// This method is cancel safe. An event not returned stays for the next call.
  pub async fn next(&mut self) -> Option<T> {
    std::future::poll_fn(|cx| Pin::new(&mut *self).poll_next(cx)).await
  }
}

async fn receive(mut receiver: broadcast::Receiver<Event>) -> Received {
  let received = receiver.recv().await;
  (received, receiver)
}

impl<T> Stream for Events<T> {
  type Item = T;

  fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<T>> {
    loop {
      let (received, receiver) = std::task::ready!(self.next.poll(cx));
      self.next.set(receive(receiver));
      let picked = match received {
        Ok(event) => (self.pick)(event),
        Err(RecvError::Lagged(missed)) => {
          let told = (self.pick)(Event::Lagged(missed));
          if told.is_none() {
            tracing::warn!(missed, "event stream fell behind, events dropped");
          }
          told
        }
        Err(RecvError::Closed) => return Poll::Ready(None),
      };
      if picked.is_some() {
        return Poll::Ready(picked);
      }
    }
  }
}

impl<T> std::fmt::Debug for Events<T> {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    f.debug_struct("Events").finish_non_exhaustive()
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[tokio::test]
  async fn a_stream_that_fell_behind_is_told_how_far_and_goes_on_then_ends() {
    let (sender, receiver) = broadcast::channel(2);
    let mut events = Events::new(receiver);
    for missed in 0..5 {
      sender.send(Event::Lagged(missed)).unwrap();
    }
    // three were dropped, and the two still held follow
    assert!(matches!(events.next().await, Some(Event::Lagged(3))));
    assert!(matches!(events.next().await, Some(Event::Lagged(3))));
    assert!(matches!(events.next().await, Some(Event::Lagged(4))));
    drop(sender);
    assert!(events.next().await.is_none());
  }

  #[tokio::test]
  async fn a_stream_of_one_topic_leaves_the_others_out_and_goes_on_past_a_loss() {
    let (sender, receiver) = broadcast::channel(2);
    let mut promotions = Events::picking(receiver, |event| match event {
      Event::Distributed(event) => Some(event),
      _ => None,
    });
    sender.send(Event::Lagged(0)).unwrap();
    sender
      .send(Event::Distributed(distributed::Event::Promoted))
      .unwrap();
    assert!(matches!(
      promotions.next().await,
      Some(distributed::Event::Promoted)
    ));
    // the first is dropped unread; the stream is not told, and yields the one still held
    for _ in 0..2 {
      sender.send(Event::Lagged(0)).unwrap();
    }
    sender
      .send(Event::Distributed(distributed::Event::Demoted))
      .unwrap();
    assert!(matches!(
      promotions.next().await,
      Some(distributed::Event::Demoted)
    ));
    drop(sender);
    assert!(promotions.next().await.is_none());
  }
}
