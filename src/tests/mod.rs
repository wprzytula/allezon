use crate::types;

mod dataset;
mod test_data;
mod utils;

#[tokio::test]
async fn test_simple() {
    let test_data = test_data::TestData::from_env().await;
    test_data.clear().await;

    let cookie = utils::random_string(10);

    test_data
        .create_user_tags_for_user(cookie.clone(), 1, Some(types::Action::Buy))
        .await;
    test_data
        .create_user_tags_for_user(cookie.clone(), 1, Some(types::Action::View))
        .await;
    test_data.check_user_profile(cookie.clone(), 200).await;
}

#[tokio::test]
async fn test_200_last() {
    let test_data = test_data::TestData::from_env().await;
    test_data.clear().await;

    let cookie = utils::random_string(10);

    test_data
        .create_user_tags_for_user(cookie.clone(), 201, Some(types::Action::Buy))
        .await;
    test_data.check_user_profile(cookie.clone(), 200).await;
}
