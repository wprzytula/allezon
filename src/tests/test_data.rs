use std::collections::HashSet;

use pretty_assertions::assert_eq;

use super::client;
use super::dataset;
use crate::types;

pub struct TestData {
    scylla_client: client::Client,
    mock_client: client::Client,
    dataset: dataset::DataSet,
}

impl TestData {
    pub fn new(scylla_url: String, mock_url: String) -> Self {
        Self {
            scylla_client: client::Client::new(scylla_url),
            mock_client: client::Client::new(mock_url),
            dataset: dataset::DataSet::new(),
        }
    }

    pub fn from_env() -> Self {
        Self::new(
            std::env::var("SCYLLA_URL").expect("SCYLLA_URL env variable is not set"),
            std::env::var("MOCK_URL").expect("MOCK_URL env variable is not set"),
        )
    }

    pub async fn create_user_tags_for_user(&self, cookie: String, user_tags_number: usize) {
        for _ in 0..user_tags_number {
            let user_tag = self.dataset.random_user_tag(dataset::UserTagConfig {
                cookie: Some(cookie.clone()),
                ..Default::default()
            });
            self.scylla_client
                .use_case_1(&user_tag)
                .await
                .unwrap();
            self.mock_client.use_case_1(&user_tag).await.unwrap();
        }
    }

    fn check_user_profile_correct(profile: &types::UserProfile) {
        assert!(profile.buys.len() <= 200);
        assert!(profile.views.len() <= 200);

        for i in 1..profile.buys.len() {
            assert!(profile.buys[i - 1].time >= profile.buys[i].time);
        }

        for i in 1..profile.views.len() {
            assert!(profile.views[i - 1].time >= profile.views[i].time);
        }
    }

    fn vectors_the_same(v1: Vec<types::UserTag>, v2: Vec<types::UserTag>) {
        assert_eq!(v1.len(), v2.len());

        let set1 = v1.into_iter().collect::<HashSet<_>>();

        for v in v2 {
            assert!(set1.contains(&v));
        }
    }

    pub async fn check_user_profile(&self, cookie: String, limit: usize) {
        let time_now = chrono::Utc::now();
        let time_from = time_now - chrono::Duration::days(1);
        let time_to = time_now;

        let mock_profile = self
            .mock_client
            .last_tags_by_cookie(cookie.as_str(), time_from, time_to, limit)
            .await;

        let scylla_profile = self
            .scylla_client
            .last_tags_by_cookie(cookie.as_str(), time_from, time_to, 200)
            .await;

        utils::check_user_profile(&mock_profile, time_from, time_to, limit);
        utils::check_user_profile(&scylla_profile, time_from, time_to, limit);

        assert_eq!(mock_profile.cookie, cookie);
        assert_eq!(scylla_profile.cookie, cookie);

        Self::vectors_the_same(mock_profile.buys, scylla_profile.buys);
        Self::vectors_the_same(mock_profile.views, scylla_profile.views);
    }

    pub async fn clear(&self) {
        self.scylla_client.clear().await;
        self.mock_client.clear().await;
    }
}
