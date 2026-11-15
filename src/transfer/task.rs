//! What every transfer task shares: stopping any step when the caller cancels or the peer fails
//! the transfer, turning the error that ended it into its final state, the handle a caller awaits,
//! and returning early once a transfer reached the state the caller waits for.

use std::future::{Future, pending};
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use super::RemoteFailures;
use super::options::TransferStateChanged;
use super::record::{Tracker, Transfer};
use super::state::{Outcome, State};
use crate::error::Error;
use crate::lock;

/// Transfer running on a task of its own.
///
/// Awaiting it gives the final snapshot, or the error that ended the transfer. Dropping it leaves
/// the transfer running. The transfer is canceled with the token it was started with.
pub struct TransferTask {
  state: TaskState,
}

enum TaskState {
  Running(JoinHandle<Result<Transfer, Error>>),
  Done(Option<Box<Result<Transfer, Error>>>),
}

impl TransferTask {
  pub(super) fn running(handle: JoinHandle<Result<Transfer, Error>>) -> Self {
    Self {
      state: TaskState::Running(handle),
    }
  }

  fn done(result: Result<Transfer, Error>) -> Self {
    Self {
      state: TaskState::Done(Some(Box::new(result))),
    }
  }
}

impl Future for TransferTask {
  type Output = Result<Transfer, Error>;

  fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
    match &mut self.state {
      TaskState::Running(handle) => Pin::new(handle).poll(cx).map(joined),
      TaskState::Done(result) => Poll::Ready(
        *result
          .take()
          .expect("a transfer task is not polled after it completed"),
      ),
    }
  }
}

/// what the task's handle comes to; a task that panicked fails the transfer
fn joined(
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

/// A state callback that resolves the receiver with true once the transfer reaches `target` or
/// a state past it, as a download whose uploader offers the file before it is asked for skips
/// the peer's queue; with false once it ends without succeeding
pub(super) fn observer(target: State) -> (TransferStateChanged, oneshot::Receiver<bool>) {
  let (signal, reached) = oneshot::channel();
  let signal = Arc::new(Mutex::new(Some(signal)));
  let observer: TransferStateChanged = Arc::new(move |_, transfer: &Transfer| {
    let reached = if transfer.state.is_terminal() && !transfer.state.succeeded() {
      false
    } else if transfer.state.at_or_past(target) {
      true
    } else {
      return;
    };
    if let Some(signal) = lock(&signal).take() {
      // the caller stopped waiting; the transfer goes on
      let _ = signal.send(reached);
    }
  });
  (observer, reached)
}

/// Waits until the transfer reached the state its observer watches, then hands back its task.
/// A transfer that fails first, or ends before any state, reports its own error.
pub(super) async fn enqueued(
  mut handle: JoinHandle<Result<Transfer, Error>>,
  reached: oneshot::Receiver<bool>,
) -> Result<TransferTask, Error> {
  tokio::select! {
    reached = reached => match reached {
      Ok(true) => Ok(TransferTask::running(handle)),
      // failed, or the task ended without reaching any state; its result says how
      Ok(false) | Err(_) => joined(handle.await).map(|transfer| TransferTask::done(Ok(transfer))),
    },
    result = &mut handle => joined(result).map(|transfer| TransferTask::done(Ok(transfer))),
  }
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
  use crate::error::ConnectionErrorKind;

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
          source: None,
        },
        Outcome::Aborted,
      ),
      (Error::TransferFailed("failed".into()), Outcome::Errored),
      (Error::UserOffline("gone".into()), Outcome::Errored),
      (
        Error::connection(ConnectionErrorKind::Read, "closed"),
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

  #[tokio::test]
  async fn enqueued_reports_a_task_that_failed_before_any_state() {
    let handle = tokio::spawn(async { Err(Error::DuplicateToken(7)) });
    let (_observer, reached) = observer(State::QueuedRemotely);
    assert!(matches!(
      enqueued(handle, reached).await,
      Err(Error::DuplicateToken(7))
    ));
  }
}
