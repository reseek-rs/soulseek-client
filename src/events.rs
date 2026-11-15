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
/// Made by [`Client::events`](crate::Client::events). Each stream gets every event. One that
/// falls further behind than the event capacity gets an [`Event::Lagged`] with the number of
/// events dropped, then the oldest event still held. The stream ends when the last clone of
/// the client is dropped.
pub struct Events {
  next: ReusableBoxFuture<'static, Received>,
}

impl Events {
  pub(crate) fn new(receiver: broadcast::Receiver<Event>) -> Self {
    Self {
      next: ReusableBoxFuture::new(receive(receiver)),
    }
  }

  /// Returns the next event, or `None` once the client is gone.
  ///
  /// # Cancel safety
  ///
  /// This method is cancel safe. An event not returned stays for the next call.
  pub async fn next(&mut self) -> Option<Event> {
    std::future::poll_fn(|cx| Pin::new(&mut *self).poll_next(cx)).await
  }
}

async fn receive(mut receiver: broadcast::Receiver<Event>) -> Received {
  let received = receiver.recv().await;
  (received, receiver)
}

impl Stream for Events {
  type Item = Event;

  fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Event>> {
    let (received, receiver) = std::task::ready!(self.next.poll(cx));
    self.next.set(receive(receiver));
    Poll::Ready(match received {
      Ok(event) => Some(event),
      Err(RecvError::Lagged(missed)) => Some(Event::Lagged(missed)),
      Err(RecvError::Closed) => None,
    })
  }
}

impl std::fmt::Debug for Events {
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
}
