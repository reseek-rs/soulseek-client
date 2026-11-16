//! Where a transfer is in its life and the moves it may make. A transfer starts queued here and
//! only moves forward: a download asks the peer and may wait in its queue, an upload offers the
//! file, then the transfer connection is made, bytes move, and it completes with one outcome.
//! Any state but a completed one may complete. A completed transfer is named by its outcome.

use std::fmt;
use std::str::FromStr;

/// Stage of a transfer in its life.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[non_exhaustive]
pub enum State {
  /// Waiting in the client.
  ///
  /// A download waits until it asks the peer, an upload for the application's slot.
  QueuedLocally,
  /// The request was written to the peer.
  ///
  /// For an upload, the peer's answer is awaited.
  Requested,
  /// The peer has the request in its queue.
  QueuedRemotely,
  /// Accepted.
  ///
  /// The transfer connection is being made or the start offset exchanged.
  Initializing,
  /// Bytes are moving.
  InProgress,
  /// Over, with how it ended.
  Completed(Outcome),
}

/// How a completed transfer ended.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[non_exhaustive]
pub enum Outcome {
  /// Every byte moved.
  Succeeded,
  /// Canceled by the caller, the slot wait or a client shutdown.
  Canceled,
  /// A wait, a connect or the transfer connection's inactivity timed out.
  TimedOut,
  /// Any other failure.
  Errored,
  /// The peer refused the transfer.
  Rejected,
  /// The peer reported a different size for a download.
  ///
  /// Nothing settled which size is right.
  Aborted,
}

/// A move the state machine does not allow
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Refused {
  from: State,
  to: State,
}

impl fmt::Display for Refused {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    write!(
      f,
      "a transfer cannot move from {} to {}",
      self.from, self.to
    )
  }
}

impl State {
  /// every state, in the order a transfer can go through them
  const ALL: [Self; 11] = [
    Self::QueuedLocally,
    Self::Requested,
    Self::QueuedRemotely,
    Self::Initializing,
    Self::InProgress,
    Self::Completed(Outcome::Succeeded),
    Self::Completed(Outcome::Canceled),
    Self::Completed(Outcome::TimedOut),
    Self::Completed(Outcome::Errored),
    Self::Completed(Outcome::Rejected),
    Self::Completed(Outcome::Aborted),
  ];

  /// Returns `true` if the transfer is over.
  pub fn is_terminal(self) -> bool {
    matches!(self, Self::Completed(_))
  }

  /// Returns `true` if the transfer is over and every byte moved.
  pub fn succeeded(self) -> bool {
    self == Self::Completed(Outcome::Succeeded)
  }

  /// Whether a transfer in this state may move to `next`. Requested goes on to Initializing
  /// for an upload only: a download passes through QueuedRemotely even when its peer sends at
  /// once.
  fn allows(self, next: Self) -> bool {
    use State::*;
    match (self, next) {
      (Completed(_), _) => false,
      (_, Completed(_)) => true,
      (QueuedLocally, Requested | Initializing)
      | (Requested, QueuedRemotely | Initializing)
      | (QueuedRemotely, Initializing)
      | (Initializing, InProgress) => true,
      _ => false,
    }
  }

  /// `next`, when a transfer in this state may move to it
  pub(crate) fn to(self, next: Self) -> Result<Self, Refused> {
    if self.allows(next) {
      Ok(next)
    } else {
      Err(Refused {
        from: self,
        to: next,
      })
    }
  }
}

impl State {
  /// Returns the state's name, such as `"queued_remotely"` or `"succeeded"`.
  pub fn as_str(self) -> &'static str {
    match self {
      Self::QueuedLocally => "queued_locally",
      Self::Requested => "requested",
      Self::QueuedRemotely => "queued_remotely",
      Self::Initializing => "initializing",
      Self::InProgress => "in_progress",
      Self::Completed(Outcome::Succeeded) => "succeeded",
      Self::Completed(Outcome::Canceled) => "canceled",
      Self::Completed(Outcome::TimedOut) => "timed_out",
      Self::Completed(Outcome::Errored) => "errored",
      Self::Completed(Outcome::Rejected) => "rejected",
      Self::Completed(Outcome::Aborted) => "aborted",
    }
  }
}

impl fmt::Display for State {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.write_str(self.as_str())
  }
}

/// reads the strings `Display` writes
impl FromStr for State {
  type Err = String;

  fn from_str(text: &str) -> Result<Self, Self::Err> {
    Self::ALL
      .into_iter()
      .find(|state| state.as_str() == text)
      .ok_or_else(|| format!("not a transfer state: {text}"))
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use Outcome::*;
  use State::*;

  #[test]
  fn transfer_states_are_written_as_their_names_and_read_back() {
    let names = [
      "queued_locally",
      "requested",
      "queued_remotely",
      "initializing",
      "in_progress",
      "succeeded",
      "canceled",
      "timed_out",
      "errored",
      "rejected",
      "aborted",
    ];
    for (state, name) in State::ALL.into_iter().zip(names) {
      assert_eq!(state.as_str(), name);
      assert_eq!(name.parse::<State>(), Ok(state));
    }
    for text in ["none", "queued", "completed", "exploded", "Succeeded"] {
      assert!(text.parse::<State>().is_err(), "{text}");
    }
  }

  #[test]
  fn a_transfer_moves_only_forward_and_never_out_of_completed() {
    let forward = [
      (QueuedLocally, Requested),
      (QueuedLocally, Initializing),
      (Requested, QueuedRemotely),
      (Requested, Initializing),
      (QueuedRemotely, Initializing),
      (Initializing, InProgress),
    ];
    let open = [
      QueuedLocally,
      Requested,
      QueuedRemotely,
      Initializing,
      InProgress,
    ];
    for from in State::ALL {
      for to in State::ALL {
        let ends = open.contains(&from) && matches!(to, Completed(_));
        let allowed = forward.contains(&(from, to)) || ends;
        assert_eq!(from.to(to).is_ok(), allowed, "{from} to {to}");
      }
    }
    assert_eq!(
      Completed(Succeeded)
        .to(Completed(Canceled))
        .unwrap_err()
        .to_string(),
      "a transfer cannot move from succeeded to canceled"
    );
  }
}
