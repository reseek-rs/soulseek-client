//! Searches as the tests start them: the settings of one as a value, and a search run to its end.

use std::sync::Arc;
use std::time::Duration;

use soulseek_client::search::{Request, Scope, Search, Token};
use soulseek_client::{Client, Error};
use soulseek_proto::peer::{File, SearchResponse};

/// decides whether a search keeps a response or a file
pub type Keep<T> = Arc<dyn Fn(&T) -> bool + Send + Sync>;

/// The settings of a search, each as [`Request`] names it. The defaults are the client's.
#[derive(Clone)]
pub struct SearchOptions {
  pub search_timeout: Duration,
  pub maximum_duration: Option<Duration>,
  pub one_response_per_user: bool,
  pub response_limit: u32,
  pub file_limit: u32,
  pub filter_responses: bool,
  pub minimum_response_file_count: u32,
  pub maximum_peer_queue_length: u32,
  pub minimum_peer_upload_speed: u32,
  pub remove_single_character_search_terms: bool,
  pub response_filter: Option<Keep<SearchResponse>>,
  pub file_filter: Option<Keep<File>>,
}

impl Default for SearchOptions {
  fn default() -> Self {
    Self {
      search_timeout: Duration::from_secs(15),
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

impl SearchOptions {
  /// `request` with these settings
  pub fn on(self, request: Request<'_>) -> Request<'_> {
    let mut request = request
      .timeout(self.search_timeout)
      .maximum_duration(self.maximum_duration)
      .one_response_per_user(self.one_response_per_user)
      .response_limit(self.response_limit)
      .file_limit(self.file_limit)
      .filter_responses(self.filter_responses)
      .minimum_response_file_count(self.minimum_response_file_count)
      .maximum_peer_queue_length(self.maximum_peer_queue_length)
      .minimum_peer_upload_speed(self.minimum_peer_upload_speed)
      .remove_single_character_terms(self.remove_single_character_search_terms);
    if let Some(keep) = self.response_filter {
      request = request.response_filter(move |response| keep(response));
    }
    if let Some(keep) = self.file_filter {
      request = request.file_filter(move |file| keep(file));
    }
    request
  }
}

/// Searches for `text` and returns the search as it ended with every response it took
pub async fn run(
  client: &Client,
  text: &str,
  scope: Scope,
  options: SearchOptions,
  token: Option<Token>,
) -> Result<(Search, Vec<SearchResponse>), Error> {
  let mut request = options.on(client.search(text).scope(scope));
  if let Some(token) = token {
    request = request.token(token);
  }
  Ok(request.await?.all().await)
}
