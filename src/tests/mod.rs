use crate::{
    endpoints::{Aggregate, Aggregates},
    types::{Action, TimeRange, UtcMinute},
};

// mod client;
mod dataset;
mod test_data;
mod utils;

#[tokio::test]
async fn test_simple() {
    let test_data = test_data::TestData::from_env().await;
    test_data.clear().await;

    let cookie = utils::random_string(10);

    test_data
        .create_user_tags_for_user(cookie.clone(), 1, Some(Action::Buy))
        .await;
    test_data
        .create_user_tags_for_user(cookie.clone(), 1, Some(Action::View))
        .await;
    test_data.check_user_profile(cookie.clone(), 200).await;
}

#[tokio::test]
async fn test_200_last() {
    let test_data = test_data::TestData::from_env().await;
    test_data.clear().await;

    let cookie = utils::random_string(10);

    test_data
        .create_user_tags_for_user(cookie.clone(), 201, Some(Action::Buy))
        .await;
    test_data.check_user_profile(cookie.clone(), 200).await;
}

#[tokio::test]
async fn test_complex() {
    let test_data = test_data::TestData::from_env().await;
    test_data.clear().await;

    let timestamp = chrono::Utc::now();
    let timestamp_trunc = UtcMinute::from(timestamp);
    let timestamp_trunc_next = timestamp_trunc.next();
    let timerange = TimeRange {
        from: timestamp_trunc.inner(),
        to: timestamp_trunc_next.inner(),
    };
    let action = Action::Buy;

    test_data
        .create_user_tags_for_timestamp(timestamp, 201, Some(action))
        .await;
    test_data
        .compare_aggregates(
            timerange,
            action,
            Aggregates {
                fst: Some(Aggregate::Count),
                snd: Some(Aggregate::SumPrice),
            },
            None,
            None,
            None,
        )
        .await;
}
