mod client;
mod dataset;
mod test_data;

mod tests {
    use super::test_data;

    #[tokio::test]
    async fn test_simple() {
        let test_data = test_data::TestData::from_env();
        test_data.clear().await;

        let cookie = "cookie".to_string();

        test_data.create_user_tags_for_user(cookie.clone(), 1).await;
        test_data.check_user_profile(cookie.clone()).await;
    }

    #[tokio::test]
    async fn test_200_last() {
        let test_data = test_data::TestData::from_env();
        test_data.clear().await;

        let cookie = "cookie".to_string();

        test_data
            .create_user_tags_for_user(cookie.clone(), 201)
            .await;
        test_data.check_user_profile(cookie.clone()).await;
    }
}
