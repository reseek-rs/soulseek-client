//! Interests: the ones the client likes and hates, what the server recommends from them, and
//! users whose interests are like the client's. Every list comes back as the server sent it.

use soulseek_proto::server::{
  GetInterestRecommendationsRequest, GetInterestRecommendationsResponse,
  GetInterestSimilarUsersRequest, GetInterestSimilarUsersResponse, GetPersonalSimilarUsersResponse,
  GetUserInterestsRequest, HatedInterestAdd, HatedInterestRemove, LikedInterestAdd,
  LikedInterestRemove, Recommendations, SimilarUser, UserInterests,
};
use soulseek_proto::{ServerCode, ServerRequest};

use super::Client;
use super::request::{interest_key, non_blank, user_key};
use crate::error::ClientError;
use crate::waiter::WaitKey;

impl Client {
  /// adds `interest` to the ones we like
  pub async fn add_liked_interest(&self, interest: &str) -> Result<(), ClientError> {
    non_blank("interest", interest)?;
    self
      .tell(
        &format!("add liked interest {interest}"),
        ServerRequest::LikedInterestAdd(LikedInterestAdd {
          interest: interest.to_owned(),
        }),
      )
      .await
  }

  /// adds `interest` to the ones we hate
  pub async fn add_hated_interest(&self, interest: &str) -> Result<(), ClientError> {
    non_blank("interest", interest)?;
    self
      .tell(
        &format!("add hated interest {interest}"),
        ServerRequest::HatedInterestAdd(HatedInterestAdd {
          interest: interest.to_owned(),
        }),
      )
      .await
  }

  /// removes `interest` from the ones we like
  pub async fn remove_liked_interest(&self, interest: &str) -> Result<(), ClientError> {
    non_blank("interest", interest)?;
    self
      .tell(
        &format!("remove liked interest {interest}"),
        ServerRequest::LikedInterestRemove(LikedInterestRemove {
          interest: interest.to_owned(),
        }),
      )
      .await
  }

  /// removes `interest` from the ones we hate
  pub async fn remove_hated_interest(&self, interest: &str) -> Result<(), ClientError> {
    non_blank("interest", interest)?;
    self
      .tell(
        &format!("remove hated interest {interest}"),
        ServerRequest::HatedInterestRemove(HatedInterestRemove {
          interest: interest.to_owned(),
        }),
      )
      .await
  }

  /// the interests `username` likes and hates
  pub async fn get_user_interests(&self, username: &str) -> Result<UserInterests, ClientError> {
    non_blank("username", username)?;
    self
      .ask(
        &format!("get interests for user {username}"),
        user_key(ServerCode::GetUserInterests, username),
        ServerRequest::GetUserInterests(GetUserInterestsRequest {
          username: username.to_owned(),
        }),
      )
      .await
  }

  /// the interests we like and hate, as the server has them
  pub async fn get_interests(&self) -> Result<UserInterests, ClientError> {
    let username = self.username().ok_or_else(|| {
      ClientError::InvalidOperation("The username for the current user is not yet known".into())
    })?;
    self.get_user_interests(&username).await
  }

  /// interests recommended across the whole network
  pub async fn get_global_recommendations(&self) -> Result<Recommendations, ClientError> {
    self
      .ask(
        "get global recommendations",
        WaitKey::Server(ServerCode::GetGlobalRecommendations),
        ServerRequest::GetGlobalRecommendations,
      )
      .await
  }

  /// interests recommended for us from the ones we like and hate
  pub async fn get_personal_recommendations(&self) -> Result<Recommendations, ClientError> {
    self
      .ask(
        "get personal recommendations",
        WaitKey::Server(ServerCode::GetPersonalRecommendations),
        ServerRequest::GetPersonalRecommendations,
      )
      .await
  }

  /// interests recommended to those who like `interest`
  pub async fn get_interest_recommendations(
    &self,
    interest: &str,
  ) -> Result<GetInterestRecommendationsResponse, ClientError> {
    non_blank("interest", interest)?;
    self
      .ask(
        &format!("get recommendations for interest {interest}"),
        interest_key(ServerCode::GetInterestRecommendations, interest),
        ServerRequest::GetInterestRecommendations(GetInterestRecommendationsRequest {
          interest: interest.to_owned(),
        }),
      )
      .await
  }

  /// users whose interests are like ours, with how alike
  pub async fn get_similar_users(&self) -> Result<Vec<SimilarUser>, ClientError> {
    let response: GetPersonalSimilarUsersResponse = self
      .ask(
        "get similar users",
        WaitKey::Server(ServerCode::GetPersonalSimilarUsers),
        ServerRequest::GetPersonalSimilarUsers,
      )
      .await?;
    Ok(response.users)
  }

  /// users who like `interest`
  pub async fn get_interest_similar_users(
    &self,
    interest: &str,
  ) -> Result<GetInterestSimilarUsersResponse, ClientError> {
    non_blank("interest", interest)?;
    self
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
