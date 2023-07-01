use std::collections::HashSet;

use pretty_assertions::assert_eq;

use super::dataset;
use crate::mock;
use crate::scylla;
use crate::types;
use crate::types::System;
use crate::utils;

pub struct TestData {
    scylla_client: scylla::Session,
    mock_client: mock::System,
    dataset: dataset::DataSet,
}

impl TestData {
    pub async fn new(scylla_url: &str) -> Self {
        Self {
            scylla_client: scylla::Session::new(scylla_url).await,
            mock_client: mock::System::new(),
            dataset: dataset::DataSet::new(),
        }
    }

    pub async fn from_env() -> Self {
        Self::new(
            std::env::var("SCYLLA_URL")
                .expect("SCYLLA_URL env variable is not set")
                .as_str(),
        )
        .await
    }

    pub async fn create_user_tags_for_user(
        &self,
        cookie: String,
        user_tags_number: usize,
        action: Option<types::Action>,
    ) {
        let now = chrono::Utc::now();
        for i in 0..user_tags_number {
            let user_tag = self.dataset.random_user_tag(dataset::UserTagConfig {
                cookie: Some(cookie.clone()),
                action,
                time: Some(now - chrono::Duration::milliseconds(i as i64)),
            });
            self.scylla_client.register_user_tag(user_tag.clone()).await;
            self.mock_client.register_user_tag(user_tag.clone()).await;
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
