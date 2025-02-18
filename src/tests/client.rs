use crate::types;
use chrono::{DateTime, Utc};

pub struct Client {
    client: reqwest::Client,
    url: String,
}

impl Client {
    pub fn new(url: String) -> Self {
        Self {
            client: reqwest::Client::new(),
            url,
        }
    }

    pub async fn use_case_1(&self, user_tag: &types::UserTag) -> Result<(), reqwest::Error> {
        let url = format!("http://{}/user_tags", self.url);
        self.client.post(url).json(user_tag).send().await?;
        Ok(())
    }

    pub async fn use_case_2(
        &self,
        cookie: String,
        time_from: DateTime<Utc>,
        time_to: DateTime<Utc>,
        limit: usize,
    ) -> Result<types::UserProfile, reqwest::Error> {
        let url = format!(
            "http://{}/user_profiles/{}?time_range={}&limit={}",
            self.url,
            cookie,
            types::TimeRange {
                from: time_from,
                to: time_to
            },
            limit
        );
        let response = self.client.post(url).send().await?;

        response.json().await
    }

    pub async fn use_case3(
        time_from: DateTime<Utc>,
        time_to: DateTime<Utc>,
        action: types::Action,
        origin: Option<&str>,
        brand_id: Option<&str>,
        category_id: Option<&str>,
        aggregates: Aggregates,
    ) -> Result<types::BucketsResponse, reqwest::Error> {
        let mut url = format!("https://{}/aggregates/?time_range={}_{}&action=\"{}\"", self.url, time_from, time_to, action);


    }

    pub async fn clear(&self) {
        let url = format!("http://{}/clear", self.url);
        self.client.post(url).send().await.unwrap();
    }
}
