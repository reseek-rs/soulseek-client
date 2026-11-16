//! Settling the size of a download when the uploader names one: an absent size or the same size
//! keeps the client's, the caller's resolver settles any other, and without one the 0 that old
//! 32-bit clients report for a file of 2 GiB or more keeps the client's while anything else
//! aborts.

use super::options::SizeMismatchResolver;
use super::record::Transfer;
use crate::error::{Error, Repr};

/// 2 GiB: below it an old client's signed 32-bit size holds a file's true size; at or above it,
/// such a client reports 0
const OVERFLOWED_SIZE: u64 = 1 << 31;

/// The size to download when the peer reports `remote`. No size from the peer, or the same
/// size, keeps the client's; a resolver settles any other; without one, the 0 an old client
/// reports for a file of 2 GiB or more keeps the client's, and anything else aborts.
pub(super) fn resolve_size(
  transfer: &Transfer,
  remote: Option<u64>,
  resolver: Option<&SizeMismatchResolver>,
) -> Result<u64, Error> {
  let local = transfer.size;
  let Some(remote) = remote.filter(|remote| *remote != local) else {
    return Ok(local);
  };
  let mismatch = Error::from(Repr::SizeMismatch { local, remote });
  match resolver {
    Some(resolve) => resolve(transfer, remote)
      .filter(|size| *size >= transfer.start_offset)
      .ok_or(mismatch),
    None if remote == 0 && local >= OVERFLOWED_SIZE => Ok(local),
    None => Err(mismatch),
  }
}

#[cfg(test)]
mod tests {
  use std::sync::Arc;

  use super::*;
  use crate::transfer::record::{Direction, Record};

  fn transfer(size: u64, start_offset: u64) -> Transfer {
    Record::new(
      Direction::Download,
      "bob",
      &"a.flac".into(),
      1,
      size,
      start_offset,
    )
    .snapshot()
  }

  #[test]
  fn an_absent_or_equal_size_keeps_ours() {
    assert_eq!(resolve_size(&transfer(100, 0), None, None).unwrap(), 100);
    assert_eq!(
      resolve_size(&transfer(100, 0), Some(100), None).unwrap(),
      100
    );
  }

  #[test]
  fn zero_from_an_old_client_keeps_ours_only_from_two_gibibytes() {
    let at_limit = transfer(OVERFLOWED_SIZE, 0);
    assert_eq!(
      resolve_size(&at_limit, Some(0), None).unwrap(),
      OVERFLOWED_SIZE
    );
    let below = transfer(OVERFLOWED_SIZE - 1, 0);
    assert!(matches!(
      resolve_size(&below, Some(0), None)
        .as_ref()
        .map_err(Error::repr),
      Err(Repr::SizeMismatch { remote: 0, .. })
    ));
  }

  #[test]
  fn another_size_aborts_unless_the_resolver_settles_it() {
    assert!(matches!(
      resolve_size(&transfer(100, 0), Some(120), None)
        .as_ref()
        .map_err(Error::repr),
      Err(Repr::SizeMismatch {
        local: 100,
        remote: 120,
        ..
      })
    ));
    let take_remote: SizeMismatchResolver = Arc::new(|_, remote| Some(remote));
    assert_eq!(
      resolve_size(&transfer(100, 0), Some(120), Some(&take_remote)).unwrap(),
      120
    );
    // a size below the start offset, or none, still aborts
    let too_small: SizeMismatchResolver = Arc::new(|_, _| Some(10));
    assert!(resolve_size(&transfer(100, 50), Some(120), Some(&too_small)).is_err());
    let refuse: SizeMismatchResolver = Arc::new(|_, _| None);
    assert!(matches!(
      resolve_size(&transfer(100, 0), Some(120), Some(&refuse))
        .as_ref()
        .map_err(Error::repr),
      Err(Repr::SizeMismatch {
        local: 100,
        remote: 120
      })
    ));
  }
}
