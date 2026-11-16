//! A search as callers see it: its state, the snapshot its responses return, and the options
//! that bound and filter it.

use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use soulseek_proto::SearchQuery;
use soulseek_proto::peer::{File, SearchResponse};

use super::Scope;

/// Stage of a search in its life.
///
/// A search is [`Requested`](Self::Requested) when made, then [`InProgress`](Self::InProgress),
/// then [`Completed`](Self::Completed) with how it ended.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum State {
  /// Accepted, with its token taken.
  Requested,
  /// Being sent to the server, or sent.
  ///
  /// Responses are taken in this state.
  InProgress,
  /// Over.
  Completed(Outcome),
}

/// How a search ended.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum Outcome {
  /// The caller canceled, or the client disconnected.
  Canceled,
  /// No response came for the idle timeout.
  ///
  /// This is the usual end of a search.
  Ended,
  /// The search ran for its maximum duration.
  MaximumDurationReached,
  /// As many responses as the search wanted arrived.
  ResponseLimitReached,
  /// As many files as the search wanted arrived.
  FileLimitReached,
  /// The request could not be sent.
  Errored,
}

impl State {
  /// Returns the state's name, such as `"in_progress"` or `"ended"`.
  ///
  /// A completed search is named by its outcome.
  pub fn as_str(self) -> &'static str {
    match self {
      Self::Requested => "requested",
      Self::InProgress => "in_progress",
      Self::Completed(Outcome::Canceled) => "canceled",
      Self::Completed(Outcome::Ended) => "ended",
      Self::Completed(Outcome::MaximumDurationReached) => "maximum_duration_reached",
      Self::Completed(Outcome::ResponseLimitReached) => "response_limit_reached",
      Self::Completed(Outcome::FileLimitReached) => "file_limit_reached",
      Self::Completed(Outcome::Errored) => "errored",
    }
  }

  /// Returns `true` if the search is over.
  pub fn is_terminal(self) -> bool {
    matches!(self, Self::Completed(_))
  }

  /// Whether a search moves from this state to `next`: in order, and never again once
  /// completed.
  pub(crate) fn leads_to(self, next: Self) -> bool {
    matches!(
      (self, next),
      (Self::Requested, Self::InProgress) | (Self::InProgress, Self::Completed(_))
    )
  }
}

impl fmt::Display for State {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.write_str(self.as_str())
  }
}

/// Search as it was at one moment.
#[derive(Clone, Debug)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Search {
  /// Text as it was sent.
  pub text: String,
  /// Terms and exclusions of the text.
  pub query: SearchQuery,
  /// Recipients of the search.
  pub scope: Scope,
  /// Token its responses name.
  pub token: u32,
  /// Stage of the search in its life.
  pub state: State,
  /// Number of responses taken.
  pub response_count: u64,
  /// Number of downloadable files in the responses taken.
  ///
  /// This is what the file limit counts.
  pub file_count: u64,
  /// Number of files in the responses taken that their sharers keep to a few users.
  pub locked_file_count: u64,
}

/// Filter that decides whether to keep a response.
///
/// Called with `(response)`. Returns whether to keep it.
pub(crate) type ResponseFilter = Arc<dyn Fn(&SearchResponse) -> bool + Send + Sync>;

/// Filter that decides whether to keep a file in its response.
///
/// Called with `(file)`. Returns whether to keep it.
pub(crate) type FileFilter = Arc<dyn Fn(&File) -> bool + Send + Sync>;

/// Bounds and filters of one search.
#[derive(Clone)]
pub(crate) struct Options {
  /// Time without a response taken after which the search ends.
  ///
  /// Defaults to 15 seconds.
  pub(crate) idle_timeout: Duration,
  /// Time after the search was sent at which it ends.
  ///
  /// The search ends then however many responses still come, as
  /// [`Outcome::MaximumDurationReached`]. If unset, only `idle_timeout` and the limits
  /// end it. Unset by default.
  pub(crate) maximum_duration: Option<Duration>,
  /// Whether to take one response from each user and drop any further one.
  ///
  /// Defaults to `true`.
  pub(crate) one_response_per_user: bool,
  /// Number of responses taken at which the search ends.
  ///
  /// Defaults to 250.
  pub(crate) response_limit: u32,
  /// Number of downloadable files at which the search ends.
  ///
  /// Counts the files the responses taken hold. Defaults to 25000.
  pub(crate) file_limit: u32,
  /// Whether to apply the filters below.
  ///
  /// If `false`, they are skipped; one response per user and the limits still apply. Defaults
  /// to `true`.
  pub(crate) filter_responses: bool,
  /// Fewest files a response may hold, locked ones included.
  ///
  /// A response with fewer is dropped. Defaults to 1.
  pub(crate) minimum_response_file_count: u32,
  /// Most uploads a responding peer may have queued.
  ///
  /// A response from a peer with more is dropped. A peer that does not know its queue length,
  /// by [`queue_count`](soulseek_proto::queue_count), is kept. Defaults to [`u32::MAX`].
  pub(crate) maximum_peer_queue_length: u32,
  /// Slowest upload speed a responding peer may have, in bytes per second.
  ///
  /// A response from a slower peer is dropped. Defaults to 0.
  pub(crate) minimum_peer_upload_speed: u32,
  /// Whether to leave terms of one character out of the query.
  ///
  /// Defaults to `false`.
  pub(crate) remove_single_character_search_terms: bool,
  /// Filter that keeps a response only when it returns `true`.
  ///
  /// Runs after the filters above. Unset by default.
  pub(crate) response_filter: Option<ResponseFilter>,
  /// Filter applied to both file lists of a response.
  ///
  /// A response left with fewer files than `minimum_response_file_count` is then dropped. Unset
  /// by default.
  pub(crate) file_filter: Option<FileFilter>,
}

impl Default for Options {
  fn default() -> Self {
    Self {
      idle_timeout: Duration::from_secs(15),
      maximum_duration: None,
      one_response_per_user: true,
      response_limit: 250,
      file_limit: 25_000,
      filter_responses: true,
      minimum_response_file_count: 1,
      maximum_peer_queue_length: u32::MAX,
      minimum_peer_upload_speed: 0,
      remove_single_character_search_terms: false,
      response_filter: None,
      file_filter: None,
    }
  }
}

/// shows which callbacks are set; the callbacks themselves have nothing to print
impl fmt::Debug for Options {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.debug_struct("Options")
      .field("idle_timeout", &self.idle_timeout)
      .field("maximum_duration", &self.maximum_duration)
      .field("one_response_per_user", &self.one_response_per_user)
      .field("response_limit", &self.response_limit)
      .field("file_limit", &self.file_limit)
      .field("filter_responses", &self.filter_responses)
      .field(
        "minimum_response_file_count",
        &self.minimum_response_file_count,
      )
      .field("maximum_peer_queue_length", &self.maximum_peer_queue_length)
      .field("minimum_peer_upload_speed", &self.minimum_peer_upload_speed)
      .field(
        "remove_single_character_search_terms",
        &self.remove_single_character_search_terms,
      )
      .field("response_filter", &self.response_filter.is_some())
      .field("file_filter", &self.file_filter.is_some())
      .finish()
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn a_search_moves_only_forward_and_ends_once() {
    use Outcome::*;
    use State::*;
    let states = [
      Requested,
      InProgress,
      Completed(Canceled),
      Completed(Ended),
      Completed(ResponseLimitReached),
      Completed(FileLimitReached),
      Completed(Errored),
    ];
    let allowed: Vec<(State, State)> = std::iter::once((Requested, InProgress))
      .chain(states[2..].iter().map(|end| (InProgress, *end)))
      .collect();
    for from in states {
      for to in states {
        assert_eq!(
          from.leads_to(to),
          allowed.contains(&(from, to)),
          "{from} to {to}"
        );
      }
    }
  }

  #[cfg(feature = "serde")]
  #[test]
  fn a_search_round_trips_with_its_scope_and_outcome() {
    use serde_json::{from_value, json, to_value};

    let search = Search {
      text: "daft punk -live".into(),
      query: SearchQuery {
        terms: vec!["daft".into(), "punk".into()],
        exclusions: vec!["live".into()],
      },
      scope: Scope::Rooms(vec!["lobby".into()]),
      token: 7,
      state: State::Completed(Outcome::Ended),
      response_count: 3,
      file_count: 12,
      locked_file_count: 1,
    };
    let json = to_value(&search).unwrap();
    assert_eq!(
      json,
      json!({
        "text": "daft punk -live",
        "query": { "terms": ["daft", "punk"], "exclusions": ["live"] },
        "scope": { "Rooms": ["lobby"] },
        "token": 7,
        "state": { "Completed": "Ended" },
        "response_count": 3,
        "file_count": 12,
        "locked_file_count": 1,
      })
    );
    let back: Search = from_value(json).unwrap();
    assert_eq!(
      (back.query, back.scope, back.state, back.file_count),
      (search.query, search.scope, search.state, 12)
    );
    assert_eq!(to_value(Scope::Network).unwrap(), json!("Network"));
    assert_eq!(to_value(State::InProgress).unwrap(), json!("InProgress"));
  }
}
