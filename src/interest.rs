//! Interests and what the server recommends from them.
//!
//! Interests are the things a user says they like or hate.

/// The interests of a client and what the server recommends from them.
///
/// Made by [`Client::interests`](crate::Client::interests).
#[derive(Clone, Copy, Debug)]
pub struct Interests<'a> {
  pub(crate) client: &'a crate::Client,
}
