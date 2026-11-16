//! Interests: the ones the client likes and hates, what the server recommends from them, and
//! users whose interests are like the client's. Every list comes back as the server sent it.

use crate::interest::Interests;
use soulseek_proto::server::{
  GetInterestRecommendationsRequest, GetInterestRecommendationsResponse,
  GetInterestSimilarUsersRequest, GetInterestSimilarUsersResponse, GetPersonalSimilarUsersResponse,
  GetUserInterestsRequest, HatedInterestAdd, HatedInterestRemove, LikedInterestAdd,
  LikedInterestRemove, Recommendations, SimilarUser, UserInterests,
};
use soulseek_proto::{ServerCode, ServerRequest};

use super::request::{interest_key, non_blank, user_key};
use crate::error::Error;
#[cfg(doc)]
use crate::error::ErrorKind;
use crate::waiter::WaitKey;

impl Interests<'_> {
  /// Adds `interest` to the ones the client likes.
  ///
  /// # Errors
  ///
  /// Returns [`ErrorKind::InvalidArgument`] if `interest` is empty or only whitespace. Returns
  /// [`ErrorKind::Shutdown`] if the client has been shut down. Returns
  /// [`ErrorKind::InvalidOperation`] if the client is not logged in. Returns an
  /// [`ErrorKind::Connection`] if the request could not be written.
  pub async fn like(self, interest: &str) -> Result<(), Error> {
    non_blank("interest", interest)?;
    self
      .client
      .tell(
        &format!("add liked interest {interest}"),
        ServerRequest::LikedInterestAdd(LikedInterestAdd {
          interest: interest.to_owned(),
        }),
      )
      .await
  }

  /// Adds `interest` to the ones the client hates.
  ///
  /// # Errors
  ///
  /// Returns [`ErrorKind::InvalidArgument`] if `interest` is empty or only whitespace. Returns
  /// [`ErrorKind::Shutdown`] if the client has been shut down. Returns
  /// [`ErrorKind::InvalidOperation`] if the client is not logged in. Returns an
  /// [`ErrorKind::Connection`] if the request could not be written.
  pub async fn hate(self, interest: &str) -> Result<(), Error> {
    non_blank("interest", interest)?;
    self
      .client
      .tell(
        &format!("add hated interest {interest}"),
        ServerRequest::HatedInterestAdd(HatedInterestAdd {
          interest: interest.to_owned(),
        }),
      )
      .await
  }

  /// Removes `interest` from the ones the client likes.
  ///
  /// # Errors
  ///
  /// Returns [`ErrorKind::InvalidArgument`] if `interest` is empty or only whitespace. Returns
  /// [`ErrorKind::Shutdown`] if the client has been shut down. Returns
  /// [`ErrorKind::InvalidOperation`] if the client is not logged in. Returns an
  /// [`ErrorKind::Connection`] if the request could not be written.
  pub async fn unlike(self, interest: &str) -> Result<(), Error> {
    non_blank("interest", interest)?;
    self
      .client
      .tell(
        &format!("remove liked interest {interest}"),
        ServerRequest::LikedInterestRemove(LikedInterestRemove {
          interest: interest.to_owned(),
        }),
      )
      .await
  }

  /// Removes `interest` from the ones the client hates.
  ///
  /// # Errors
  ///
  /// Returns [`ErrorKind::InvalidArgument`] if `interest` is empty or only whitespace. Returns
  /// [`ErrorKind::Shutdown`] if the client has been shut down. Returns
  /// [`ErrorKind::InvalidOperation`] if the client is not logged in. Returns an
  /// [`ErrorKind::Connection`] if the request could not be written.
  pub async fn unhate(self, interest: &str) -> Result<(), Error> {
    non_blank("interest", interest)?;
    self
      .client
      .tell(
        &format!("remove hated interest {interest}"),
        ServerRequest::HatedInterestRemove(HatedInterestRemove {
          interest: interest.to_owned(),
        }),
      )
      .await
  }

  /// Returns the interests `username` likes and hates.
  ///
  /// # Errors
  ///
  /// Returns [`ErrorKind::InvalidArgument`] if `username` is empty or only whitespace. Returns
  /// [`ErrorKind::Shutdown`] if the client has been shut down. Returns
  /// [`ErrorKind::InvalidOperation`] if the client is not logged in. Returns [`ErrorKind::Timeout`]
  /// if the server does not answer in time. Returns an [`ErrorKind::Connection`] if the request
  /// could not be written or the server connection closed before the answer.
  pub async fn of(self, username: &str) -> Result<UserInterests, Error> {
    non_blank("username", username)?;
    self
      .client
      .ask(
        &format!("get interests for user {username}"),
        user_key(ServerCode::GetUserInterests, username),
        ServerRequest::GetUserInterests(GetUserInterestsRequest {
          username: username.to_owned(),
        }),
      )
      .await
  }

  /// Returns the interests the client likes and hates.
  ///
  /// The interests are as the server has them.
  ///
  /// # Errors
  ///
  /// Returns [`ErrorKind::Shutdown`] if the client has been shut down. Returns
  /// [`ErrorKind::InvalidOperation`] if the client's username is not yet known or the client is not
  /// logged in. Returns [`ErrorKind::Timeout`] if the server does not answer in time. Returns an
  /// [`ErrorKind::Connection`] if the request could not be written or the server connection closed
  /// before the answer.
  pub async fn own(self) -> Result<UserInterests, Error> {
    let username = self.client.username().ok_or_else(|| {
      Error::invalid_operation("the username for the current user is not yet known".into())
    })?;
    self.client.interests().of(&username).await
  }

  /// Returns the interests recommended across the whole network.
  ///
  /// # Errors
  ///
  /// Returns [`ErrorKind::Shutdown`] if the client has been shut down. Returns
  /// [`ErrorKind::InvalidOperation`] if the client is not logged in. Returns [`ErrorKind::Timeout`]
  /// if the server does not answer in time. Returns an [`ErrorKind::Connection`] if the request
  /// could not be written or the server connection closed before the answer.
  pub async fn global_recommendations(self) -> Result<Recommendations, Error> {
    self
      .client
      .ask(
        "get global recommendations",
        WaitKey::Server(ServerCode::GetGlobalRecommendations),
        ServerRequest::GetGlobalRecommendations,
      )
      .await
  }

  /// Returns the interests recommended for the client.
  ///
  /// They are recommended from the ones the client likes and hates.
  ///
  /// # Errors
  ///
  /// Returns [`ErrorKind::Shutdown`] if the client has been shut down. Returns
  /// [`ErrorKind::InvalidOperation`] if the client is not logged in. Returns [`ErrorKind::Timeout`]
  /// if the server does not answer in time. Returns an [`ErrorKind::Connection`] if the request
  /// could not be written or the server connection closed before the answer.
  pub async fn recommendations(self) -> Result<Recommendations, Error> {
    self
      .client
      .ask(
        "get personal recommendations",
        WaitKey::Server(ServerCode::GetPersonalRecommendations),
        ServerRequest::GetPersonalRecommendations,
      )
      .await
  }

  /// Returns the interests recommended to those who like `interest`.
  ///
  /// # Errors
  ///
  /// Returns [`ErrorKind::InvalidArgument`] if `interest` is empty or only whitespace. Returns
  /// [`ErrorKind::Shutdown`] if the client has been shut down. Returns
  /// [`ErrorKind::InvalidOperation`] if the client is not logged in. Returns [`ErrorKind::Timeout`]
  /// if the server does not answer in time. Returns an [`ErrorKind::Connection`] if the request
  /// could not be written or the server connection closed before the answer.
  pub async fn recommendations_for(
    self,
    interest: &str,
  ) -> Result<GetInterestRecommendationsResponse, Error> {
    non_blank("interest", interest)?;
    self
      .client
      .ask(
        &format!("get recommendations for interest {interest}"),
        interest_key(ServerCode::GetInterestRecommendations, interest),
        ServerRequest::GetInterestRecommendations(GetInterestRecommendationsRequest {
          interest: interest.to_owned(),
        }),
      )
      .await
  }

  /// Returns the users whose interests are like the client's.
  ///
  /// Each comes with how alike.
  ///
  /// # Errors
  ///
  /// Returns [`ErrorKind::Shutdown`] if the client has been shut down. Returns
  /// [`ErrorKind::InvalidOperation`] if the client is not logged in. Returns [`ErrorKind::Timeout`]
  /// if the server does not answer in time. Returns an [`ErrorKind::Connection`] if the request
  /// could not be written or the server connection closed before the answer.
  pub async fn similar_users(self) -> Result<Vec<SimilarUser>, Error> {
    let response: GetPersonalSimilarUsersResponse = self
      .client
      .ask(
        "get similar users",
        WaitKey::Server(ServerCode::GetPersonalSimilarUsers),
        ServerRequest::GetPersonalSimilarUsers,
      )
      .await?;
    Ok(response.users)
  }

  /// Returns the users who like `interest`.
  ///
  /// # Errors
  ///
  /// Returns [`ErrorKind::InvalidArgument`] if `interest` is empty or only whitespace. Returns
  /// [`ErrorKind::Shutdown`] if the client has been shut down. Returns
  /// [`ErrorKind::InvalidOperation`] if the client is not logged in. Returns [`ErrorKind::Timeout`]
  /// if the server does not answer in time. Returns an [`ErrorKind::Connection`] if the request
  /// could not be written or the server connection closed before the answer.
  pub async fn similar_users_for(
    self,
    interest: &str,
  ) -> Result<GetInterestSimilarUsersResponse, Error> {
    non_blank("interest", interest)?;
    self
      .client
      .ask(
        &format!("get similar users for interest {interest}"),
        interest_key(ServerCode::GetInterestSimilarUsers, interest),
        ServerRequest::GetInterestSimilarUsers(GetInterestSimilarUsersRequest {
          interest: interest.to_owned(),
        }),
      )
      .await
  }
}
