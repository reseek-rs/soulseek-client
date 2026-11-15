//! Interests: the things a user says they like or hate, and what the server recommends from them.

/// The interests of a client and what the server recommends from them.
///
/// Made by [`Client::interests`](crate::Client::interests).
#[derive(Clone, Copy, Debug)]
pub struct Interests<'a> {
  pub(crate) client: &'a crate::Client,
}
