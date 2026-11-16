//! What the client tells the application: every state change, every pushed server message the
//! application acts on, and how its transfers and browses go. Events are grouped by what they
//! are about, one enum per topic in that topic's module. A subscriber that falls more than the
//! `event_capacity` setting behind is told how many events it missed, of every topic, and goes on
//! from the oldest one still held.

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
}

/// The events an [`Events`] stream missed because it fell behind.
///
/// Holds the number dropped, counted over every topic: a stream of one topic may have missed
/// fewer of its own. The stream goes on from the oldest event still held.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
#[error("the event stream fell behind and missed {0} events")]
pub struct Lagged(pub u64);

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
/// Each item is an event, or a [`Lagged`] in place of the events dropped once the stream falls
/// further behind than [`Config::event_capacity`](crate::Config::event_capacity); the stream
/// goes on from the oldest one still held. The stream ends when the last clone of the client is
/// dropped.
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
  pub async fn next(&mut self) -> Option<Result<T, Lagged>> {
    std::future::poll_fn(|cx| Pin::new(&mut *self).poll_next(cx)).await
  }
}

async fn receive(mut receiver: broadcast::Receiver<Event>) -> Received {
  let received = receiver.recv().await;
  (received, receiver)
}

impl<T> Stream for Events<T> {
  type Item = Result<T, Lagged>;

  fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
    loop {
      let (received, receiver) = std::task::ready!(self.next.poll(cx));
      self.next.set(receive(receiver));
      match received {
        Ok(event) => {
          if let Some(picked) = (self.pick)(event) {
            return Poll::Ready(Some(Ok(picked)));
          }
        }
        Err(RecvError::Lagged(missed)) => return Poll::Ready(Some(Err(Lagged(missed)))),
        Err(RecvError::Closed) => return Poll::Ready(None),
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

  fn promoted() -> Event {
    Event::Distributed(distributed::Event::Promoted)
  }

  #[tokio::test]
  async fn a_stream_that_fell_behind_is_told_how_far_and_goes_on_then_ends() {
    let (sender, receiver) = broadcast::channel(2);
    let mut events = Events::new(receiver);
    for _ in 0..5 {
      sender.send(promoted()).unwrap();
    }
    // three were dropped, and the two still held follow
    assert_eq!(events.next().await.unwrap().unwrap_err(), Lagged(3));
    assert!(matches!(
      events.next().await,
      Some(Ok(Event::Distributed(_)))
    ));
    assert!(matches!(
      events.next().await,
      Some(Ok(Event::Distributed(_)))
    ));
    drop(sender);
    assert!(events.next().await.is_none());
  }

  #[tokio::test]
  async fn a_stream_of_one_topic_leaves_the_others_out_and_is_told_of_a_loss() {
    let (sender, receiver) = broadcast::channel(2);
    let mut promotions = Events::picking(receiver, |event| match event {
      Event::Distributed(event) => Some(event),
      _ => None,
    });
    let other = || Event::Session(session::Event::Kicked);
    sender.send(other()).unwrap();
    sender.send(promoted()).unwrap();
    assert!(matches!(
      promotions.next().await,
      Some(Ok(distributed::Event::Promoted))
    ));
    for _ in 0..2 {
      sender.send(other()).unwrap();
    }
    sender
      .send(Event::Distributed(distributed::Event::Demoted))
      .unwrap();
    assert_eq!(promotions.next().await.unwrap().unwrap_err(), Lagged(1));
    assert!(matches!(
      promotions.next().await,
      Some(Ok(distributed::Event::Demoted))
    ));
    drop(sender);
    assert!(promotions.next().await.is_none());
  }
}
