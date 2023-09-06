use std::collections::HashSet;

use chrono::DateTime;
use chrono::Utc;
use pretty_assertions::assert_eq;

use super::dataset;
use crate::endpoints::Aggregates;
use crate::mock;
use crate::scylla;
use crate::types;
use crate::types::Action;
use crate::types::System;
use crate::types::TimeRange;
use crate::utils;

pub struct TestData {
    scylla_client: scylla::Session,
    mock_client: mock::System,
    dataset: dataset::DataSet,
}

impl TestData {
    pub async fn new(scylla_url: &str) -> Self {
        Self {
            scylla_client: scylla::Session::new(scylla_url, 3).await,
            mock_client: mock::System::new(),
            dataset: dataset::DataSet::new(),
        }
    }

    pub async fn from_env() -> Self {
        let _ = tracing_subscriber::fmt::try_init();
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

    pub async fn create_user_tags_for_timestamp(
        &self,
        timestamp: DateTime<Utc>,
        user_tags_number: usize,
        action: Option<types::Action>,
    ) {
        for _i in 0..user_tags_number {
            let user_tag = self.dataset.random_user_tag(dataset::UserTagConfig {
                action,
                time: Some(timestamp),
                ..Default::default()
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

    pub async fn compare_aggregates(
        &self,
        timerange: TimeRange,
        action: Action,
        _aggregates: Aggregates,
        origin: Option<&str>,
        brand_id: Option<&str>,
        category_id: Option<&str>,
    ) {
        let time_from = timerange.from;
        let time_to = timerange.to;
        let mock_buckets = self
            .mock_client
            .select_bucket_stats(time_from, time_to, action, origin, brand_id, category_id)
            .await;
        let scylla_buckets = self
            .scylla_client
            .select_bucket_stats(time_from, time_to, action, origin, brand_id, category_id)
            .await;
        assert_eq!(mock_buckets.len(), scylla_buckets.len());
        mock_buckets
            .into_iter()
            .zip(scylla_buckets)
            .for_each(|(mock_bucket, scylla_bucket)| {
                // dbg!(&scylla_bucket);
                assert_eq!(mock_bucket, scylla_bucket);
            });
    }

    pub async fn clear(&self) {
        self.scylla_client.clear().await;
        self.mock_client.clear().await;
    }
}
