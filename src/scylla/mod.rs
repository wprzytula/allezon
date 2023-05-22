use chrono::{DateTime, Utc};
use scylla::macros::{FromUserType, IntoUserType};
use scylla::prepared_statement::PreparedStatement;
use scylla::IntoTypedRows;

use crate::types;

pub struct Session {
    session: scylla::Session,
    insert_user_tag: PreparedStatement,
    select_last_tags_by_cookie: PreparedStatement,
    delete_old_tags_by_cookie: PreparedStatement,
}

#[derive(FromUserType, IntoUserType, Debug)]
struct ProductInfo {
    pub product_id: String,
    pub brand_id: String,
    pub category_id: String,
    pub price: i32,
}

#[derive(FromUserType, IntoUserType, Debug)]
struct UserTag {
    pub country: String,
    pub device: String,
    pub origin: String,
    pub product_info: ProductInfo,
}

impl UserTag {
    pub fn new(user_tag: types::UserTag) -> Result<Self, serde_json::Error> {
        Ok(Self {
            country: user_tag.country,
            device: serde_json::to_string(&user_tag.device)?,
            origin: user_tag.origin,
            product_info: ProductInfo {
                product_id: user_tag.product_info.product_id,
                brand_id: user_tag.product_info.brand_id,
                category_id: user_tag.product_info.category_id,
                price: user_tag.product_info.price,
            },
        })
    }

    pub fn into_user_tag(
        self,
        cookie: String,
        time: DateTime<Utc>,
        action: String,
    ) -> Result<types::UserTag, serde_json::Error> {
        Ok(types::UserTag {
            time,
            country: self.country,
            cookie,
            action: serde_json::from_str(&action)?,
            device: serde_json::from_str(&self.device)?,
            origin: self.origin,
            product_info: types::ProductInfo {
                product_id: self.product_info.product_id,
                brand_id: self.product_info.brand_id,
                category_id: self.product_info.category_id,
                price: self.product_info.price,
            },
        })
    }
}

impl Session {
    pub async fn prepare(session: &scylla::Session) {
        session.query("CREATE KEYSPACE IF NOT EXISTS allezon WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }", ()).await.unwrap();
        session.use_keyspace("allezon", false).await.unwrap();
        session
            .query(
                "CREATE TYPE IF NOT EXISTS product_info (product_id text, brand_id text, category_id text, price int)",
                (),
            )
            .await
            .unwrap();
        session.query("CREATE TYPE IF NOT EXISTS user_tag (country text, device text, origin text, product_info frozen<product_info>)", ()).await.unwrap();
        session.query("CREATE TABLE IF NOT EXISTS user_tags (cookie text, action text, time timestamp, tag frozen<user_tag>, PRIMARY KEY ((cookie, action), time)) WITH CLUSTERING ORDER BY (time DESC)", ()).await.unwrap();
    }

    pub async fn new(uri: &str) -> Self {
        let session = scylla::SessionBuilder::new()
            .known_node(uri)
            .build()
            .await
            .expect("Failed to create Scylla session");

        Self::prepare(&session).await;

        Session {
            insert_user_tag: session
                .prepare("INSERT INTO user_tags (cookie, action, time, tag) VALUES (?, ?, ?, ?)")
                .await
                .expect("Failed to prepare insert_user_tag"),
            select_last_tags_by_cookie: session
                .prepare("SELECT time, tag FROM user_tags WHERE cookie = ? AND action = ? AND time >= ? AND time <= ? ORDER BY time DESC LIMIT 200")
                .await
                .expect("Failed to prepare select_last_tags_by_cookie"),
            delete_old_tags_by_cookie: session
                .prepare("DELETE FROM user_tags WHERE cookie = ? AND action = ? AND time < ?")
                .await
                .expect("Failed to prepare delete_old_tags_by_cookie"),
            session,
        }
    }

    pub async fn register_user_tag(&self, user_tag: types::UserTag) {
        let user_tag_time = user_tag.time;
        let user_tag_cookie = user_tag.cookie.clone();
        let user_tag_action =
            serde_json::to_string(&user_tag.action).expect("Failed to serialize user tag action");
        let db_user_tag = UserTag::new(user_tag).expect("Failed to create UserTag");

        self.session
            .execute(
                &self.insert_user_tag,
                (user_tag_cookie, user_tag_action, user_tag_time, db_user_tag),
            )
            .await
            .expect("Failed to insert user tag");
    }

    pub async fn last_tags_by_cookie(
        &self,
        cookie: &str,
        time_from: DateTime<Utc>,
        time_to: DateTime<Utc>,
        limit: usize,
    ) -> types::UserProfile {
        let load_action = |action: types::Action| async move {
            let action_string = serde_json::to_string(&action).unwrap();

            let user_tags = self
                .session
                .execute(
                    &self.select_last_tags_by_cookie,
                    (cookie, action_string.clone(), time_from, time_to),
                )
                .await
                .expect("Failed to select last tags by cookie")
                .rows
                .map(|rows| {
                    rows.into_typed::<(DateTime<Utc>, UserTag)>()
                        .map(|result| {
                            let (time, user_tag) = result.expect("Failed to get user tag");
                            user_tag
                                .into_user_tag(cookie.to_string(), time, action_string.clone())
                                .expect("Failed to convert user tag")
                        })
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default();

            if let Some(oldest) = user_tags.last() {
                self.session
                    .execute(
                        &self.delete_old_tags_by_cookie,
                        (cookie, action_string.clone(), oldest.time),
                    )
                    .await
                    .expect("Failed to delete old tags by cookie");
            }

            user_tags.into_iter().take(limit).collect::<Vec<_>>()
        };

        types::UserProfile {
            cookie: cookie.to_string(),
            views: load_action(types::Action::View).await,
            buys: load_action(types::Action::Buy).await,
        }
    }
}
