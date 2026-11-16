//! What every transfer task shares: stopping any step when the caller cancels or the peer fails
//! the transfer, turning the error that ended it into its final state, and what a transfer's
//! task comes to for the caller that awaits it.

use std::future::{Future, pending};

use tokio_util::sync::CancellationToken;

use super::RemoteFailures;
use super::record::{Tracker, Transfer};
use super::state::{Outcome, State};
use crate::error::Error;

/// what the task's handle comes to; a task that panicked fails the transfer
pub(super) fn joined(
  result: Result<Result<Transfer, Error>, tokio::task::JoinError>,
) -> Result<Transfer, Error> {
  result.unwrap_or_else(|error| {
    tracing::error!(%error, "transfer task failed");
    Err(Error::operation(
      format!("the transfer task failed: {error}"),
      None,
    ))
  })
}

/// Ends a transfer's steps early: when the caller cancels, or when the peer refused or failed a
/// download
pub(super) struct Stop {
  cancel: CancellationToken,
  remote_failure: Option<RemoteFailures>,
}

impl Stop {
  pub(super) fn new(cancel: CancellationToken, remote_failure: Option<RemoteFailures>) -> Self {
    Self {
      cancel,
      remote_failure,
    }
  }

  pub(super) fn cancel(&self) -> &CancellationToken {
    &self.cancel
  }

  /// Runs `step` unless the transfer is stopped first, which fails it with `Canceled` or the
  /// peer's refusal or failure
  pub(super) async fn run<T>(
    &mut self,
    step: impl Future<Output = Result<T, Error>>,
  ) -> Result<T, Error> {
    let Self {
      cancel,
      remote_failure,
    } = self;
    tokio::select! {
      biased;
      () = cancel.cancelled() => Err(Error::Canceled),
      error = failed(remote_failure) => Err(error),
      result = step => result,
    }
  }
}

/// resolves with the peer's refusal or failure once one arrives; never for an upload
async fn failed(remote_failure: &mut Option<RemoteFailures>) -> Error {
  let Some(receiver) = remote_failure else {
    return pending().await;
  };
  let failure = receiver
    .wait_for(Option::is_some)
    .await
    .map(|error| error.clone());
  match failure {
    Ok(Some(error)) => error,
    // the transfer's entry is gone, so nothing can fail it any more
    _ => pending().await,
  }
}

/// how a transfer ended by `error` ended
pub(super) fn outcome(error: &Error) -> Outcome {
  match error {
    Error::Canceled => Outcome::Canceled,
    Error::Timeout(_) => Outcome::TimedOut,
    Error::TransferRejected(_) => Outcome::Rejected,
    Error::SizeMismatch { .. } => Outcome::Aborted,
    _ => Outcome::Errored,
  }
}

/// Reports how a transfer ended: the last progress, if bytes ever moved, then the final state.
/// Returns the final snapshot, or the error.
pub(super) fn finish(tracker: &Tracker, result: Result<(), Error>) -> Result<Transfer, Error> {
  let in_progress = tracker.snapshot().start_time.is_some();
  if in_progress {
    tracker.emit_progress(tracker.bytes_transferred());
  }
  // a transfer completes once, here, and any state but a completed one may complete
  let ended = "a transfer completes once";
  match result {
    Ok(()) => {
      tracker
        .emit_state(State::Completed(Outcome::Succeeded), None)
        .expect(ended);
      Ok(tracker.snapshot())
    }
    Err(error) => {
      tracker
        .emit_state(State::Completed(outcome(&error)), Some(error.clone()))
        .expect(ended);
      Err(error)
    }
  }
}

#[cfg(test)]
mod tests {
  use tokio::sync::watch;

  use super::*;

  #[test]
  fn each_error_ends_in_its_state() {
    let cases = [
      (Error::Canceled, Outcome::Canceled),
      (Error::Timeout("late".into()), Outcome::TimedOut),
      (Error::TransferRejected("Banned".into()), Outcome::Rejected),
      (
        Error::SizeMismatch {
          local: 1,
          remote: 2,
        },
        Outcome::Aborted,
      ),
      (Error::TransferFailed("failed".into()), Outcome::Errored),
      (Error::UserOffline("gone".into()), Outcome::Errored),
      (
        Error::connection(crate::error::ConnectionKind::Read, "closed"),
        Outcome::Errored,
      ),
    ];
    for (error, state) in cases {
      assert_eq!(outcome(&error), state, "{error}");
    }
  }

  #[tokio::test]
  async fn a_stop_ends_a_step_with_the_peers_failure() {
    let (fail, failures) = watch::channel(None);
    let mut stop = Stop::new(CancellationToken::new(), Some(failures));
    fail.send_replace(Some(Error::TransferRejected("Banned".into())));
    let result: Result<(), _> = stop.run(pending()).await;
    assert!(matches!(result, Err(Error::TransferRejected(reason)) if reason == "Banned"));
  }
}
