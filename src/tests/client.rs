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

        Ok(response.json().await?)
    }

    pub async fn clear(&self) {
        let url = format!("http://{}/clear", self.url);
        self.client.post(url).send().await.unwrap();
    }
}
