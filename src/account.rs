//! The client's own account on the server: its status, what it says it shares, its password and
//! its privileges.

/// The account a client is logged in with.
///
/// Made by [`Client::account`](crate::Client::account).
#[derive(Clone, Copy, Debug)]
pub struct Account<'a> {
  pub(crate) client: &'a crate::Client,
}
