use async_trait::async_trait;
use chrono::{DateTime, Utc};
use scylla::batch::{Batch, BatchStatement, BatchType};
use scylla::macros::{FromUserType, IntoUserType};
use scylla::prepared_statement::PreparedStatement;
use scylla::IntoTypedRows;
use tracing::debug;

use crate::types::{Action, UtcMinute};
use crate::{types, utils};

pub struct Session {
    session: scylla::Session,
    // use case 1
    insert_user_tag: PreparedStatement,
    update_bucket_stats: Batch,

    // use case 2
    select_last_tags_by_cookie: PreparedStatement,
    delete_old_tags_by_cookie: PreparedStatement,

    // use case 3
    select_bucket_stats_all: PreparedStatement,
    select_bucket_stats_origin: PreparedStatement,
    select_bucket_stats_brand: PreparedStatement,
    select_bucket_stats_category: PreparedStatement,
    select_bucket_stats_origin_brand: PreparedStatement,
    select_bucket_stats_origin_category: PreparedStatement,
    select_bucket_stats_brand_category: PreparedStatement,
    select_bucket_stats_origin_brand_category: PreparedStatement,
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
        session
            .query("TRUNCATE TABLE user_tags", &[])
            .await
            .unwrap();
        // TODO: as TTL is not applicable to counter columns, add a task that deletes old entries each hour
        session.query("CREATE TABLE IF NOT EXISTS buckets_obc (bucket timestamp, action text, origin text, brand_id text, category_id text, count counter, sum counter, PRIMARY KEY((bucket, action), origin, brand_id, category_id))", ()).await.unwrap();
        session
            .query("TRUNCATE TABLE buckets_obc", &[])
            .await
            .unwrap();
        session.query("CREATE TABLE IF NOT EXISTS buckets_co (bucket timestamp, action text, origin text,  category_id text, count counter, sum counter, PRIMARY KEY((bucket, action), category_id, origin))", ()).await.unwrap();
        session
            .query("TRUNCATE TABLE buckets_co", &[])
            .await
            .unwrap();
        session.query("CREATE TABLE IF NOT EXISTS buckets_bc (bucket timestamp, action text, brand_id text, category_id text, count counter, sum counter, PRIMARY KEY((bucket, action), brand_id, category_id))", ()).await.unwrap();
        session
            .query("TRUNCATE TABLE buckets_bc", &[])
            .await
            .unwrap();
    }

    pub async fn new(uri: &str) -> Self {
        let session = scylla::SessionBuilder::new()
            .known_node(uri)
            .build()
            .await
            .expect("Failed to create Scylla session");

        Self::prepare(&session).await;

        Self {
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

            select_bucket_stats_all: session
                .prepare("SELECT SUM(count), SUM(sum) FROM buckets_bc WHERE bucket = ? AND action = ?")
                .await
                .expect("Failed to prepare select_bucket_stats_all"),
            select_bucket_stats_origin: session
                .prepare("SELECT SUM(count), SUM(sum) FROM buckets_obc WHERE bucket = ? AND action = ? AND origin = ?")
                .await
                .expect("Failed to prepare select_bucket_stats_origin"),
            select_bucket_stats_brand: session
                .prepare("SELECT SUM(count), SUM(sum) FROM buckets_bc WHERE bucket = ? AND action = ? AND brand_id = ?")
                .await
                .expect("Failed to prepare select_bucket_stats_brand"),
            select_bucket_stats_category: session
                .prepare("SELECT SUM(count), SUM(sum) FROM buckets_co WHERE bucket = ? AND action = ? AND category_id = ?")
                .await
                .expect("Failed to prepare select_bucket_stats_category"),
            select_bucket_stats_origin_brand: session
                .prepare("SELECT SUM(count), SUM(sum) FROM buckets_obc WHERE bucket = ? AND action = ? AND origin = ? AND brand_id = ?")
                .await
                .expect("Failed to prepare select_bucket_stats_origin_brand"),
            select_bucket_stats_origin_category: session
                .prepare("SELECT count, sum FROM buckets_co WHERE bucket = ? AND action = ? AND origin = ? AND category_id = ?")
                .await
                .expect("Failed to prepare select_bucket_stats_origin_category"),
            select_bucket_stats_brand_category: session
                .prepare("SELECT count, sum FROM buckets_bc WHERE bucket = ? AND action = ? AND brand_id = ? AND category_id = ?")
                .await
                .expect("Failed to prepare select_bucket_stats_brand_category"),
            select_bucket_stats_origin_brand_category: session
                .prepare("SELECT count, sum FROM buckets_obc WHERE bucket = ? AND action = ? AND origin = ? AND brand_id = ? AND category_id = ?")
                .await
                .expect("Failed to prepare select_bucket_stats_origin_brand_category"),

            update_bucket_stats: {
                Batch::new_with_statements(BatchType::Counter, [
                    session
                        .prepare("UPDATE buckets_obc SET count = count + 1, sum = sum + ? WHERE bucket = ? AND action = ? AND origin = ? AND brand_id = ? AND category_id = ?")
                        .await
                        .expect("Failed to prepare update_bucket_stats_obc"),
                    session
                        .prepare("UPDATE buckets_co SET count = count + 1, sum = sum + ? WHERE bucket = ? AND action = ? AND origin = ? AND category_id = ?")
                        .await
                        .expect("Failed to prepare update_bucket_stats_co"),
                    session
                        .prepare("UPDATE buckets_bc SET count = count + 1, sum = sum + ? WHERE bucket = ? AND action = ? AND brand_id = ? AND category_id = ?")
                        .await
                        .expect("Failed to prepare update_bucket_stats_bc"),
                ].into_iter().map(BatchStatement::PreparedStatement).collect())
            },

            session,

        }
    }

    async fn update_bucket_stats(
        &self,
        bucket: UtcMinute,
        action: Action,
        origin: &str,
        brand_id: &str,
        category_id: &str,
        price: i64,
    ) {
        debug!("Updating bucket stats for bucket {}", bucket);
        self.session
            .batch(
                &self.update_bucket_stats,
                (
                    // obc
                    (
                        price,
                        bucket.inner(),
                        action.to_string(),
                        origin,
                        brand_id,
                        category_id,
                    ),
                    // co
                    (
                        price,
                        bucket.inner(),
                        action.to_string(),
                        category_id,
                        origin,
                    ),
                    // bc
                    (
                        price,
                        bucket.inner(),
                        action.to_string(),
                        brand_id,
                        category_id,
                    ),
                ),
            )
            .await
            .unwrap();
    }
}

#[async_trait]
impl types::System for Session {
    async fn register_user_tag(&self, user_tag: types::UserTag) {
        let user_tag_time = user_tag.time;
        let user_tag_cookie = user_tag.cookie.clone();
        let user_tag_action =
            serde_json::to_string(&user_tag.action).expect("Failed to serialize user tag action");

        self.update_bucket_stats(
            user_tag_time.into(),
            user_tag.action,
            &user_tag.origin,
            &user_tag.product_info.brand_id,
            &user_tag.product_info.category_id,
            user_tag.product_info.price as i64,
        )
        .await;

        let db_user_tag = UserTag::new(user_tag).expect("Failed to create UserTag");

        self.session
            .execute(
                &self.insert_user_tag,
                (user_tag_cookie, user_tag_action, user_tag_time, db_user_tag),
            )
            .await
            .expect("Failed to insert user tag");
    }

    async fn last_tags_by_cookie<'a>(
        &'a self,
        cookie: &'a str,
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

        let profile = types::UserProfile {
            cookie: cookie.to_string(),
            views: load_action(types::Action::View).await,
            buys: load_action(types::Action::Buy).await,
        };

        utils::check_user_profile(&profile, time_from, time_to, limit);
        profile
    }

    async fn clear(&self) {
        self.session
            .query("TRUNCATE user_tags", ())
            .await
            .expect("Failed to clear user tags");
    }
}
