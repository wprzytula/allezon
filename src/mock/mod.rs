use std::collections::{BTreeMap, BTreeSet};

use async_trait::async_trait;
use chrono::{DateTime, Timelike, Utc};
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

use crate::{
    types::{self, Action, Bucket, UserProfile, UserTag, UtcMinute},
    utils,
};

const MAX_TAGS_BY_COOKIE: usize = 200;

#[derive(Debug)]
struct SystemData {
    // // For 3rd use case - aggregates.
    tags_by_timestamp: BTreeMap<DateTime<Utc>, Vec<UserTag>>,

    // For 2nd use case - user profiles.
    tags_by_cookie: BTreeMap<String, UserProfileInner>,
}

#[derive(Debug)]
pub struct System {
    data: RwLock<SystemData>,
}

#[derive(Clone, Debug, Serialize)]
struct UserProfileInner {
    cookie: String,
    views: BTreeSet<UserTagByTime>,
    buys: BTreeSet<UserTagByTime>,
}

#[derive(Clone, Debug)]
struct UserTagByTime(UserTag);
impl PartialEq for UserTagByTime {
    fn eq(&self, other: &Self) -> bool {
        self.0.time == other.0.time
    }
}
impl Eq for UserTagByTime {}
impl PartialOrd for UserTagByTime {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.0.time.partial_cmp(&other.0.time)
    }
}
impl Ord for UserTagByTime {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0.time.cmp(&other.0.time)
    }
}
impl From<UserTag> for UserTagByTime {
    fn from(tag: UserTag) -> Self {
        UserTagByTime(tag)
    }
}
impl Serialize for UserTagByTime {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.0.serialize(serializer)
    }
}
impl<'de> Deserialize<'de> for UserTagByTime {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        UserTag::deserialize(deserializer).map(Self)
    }
}

impl Default for System {
    fn default() -> Self {
        Self::new()
    }
}

impl System {
    pub fn new() -> Self {
        Self {
            data: RwLock::new(SystemData {
                tags_by_timestamp: Default::default(),
                tags_by_cookie: Default::default(),
            }),
        }
    }
}

#[async_trait]
impl types::System for System {
    async fn register_user_tag(&self, tag: types::UserTag) {
        let mut data = self.data.write().await;

        data.tags_by_timestamp
            .entry(tag.time)
            .or_default()
            .push(tag.clone());

        data.tags_by_cookie
            .entry(tag.cookie.clone())
            .and_modify(|user_profile| {
                let set = match tag.action {
                    Action::View => &mut user_profile.views,
                    Action::Buy => &mut user_profile.buys,
                };
                set.insert(tag.clone().into());
                if set.len() > MAX_TAGS_BY_COOKIE {
                    set.pop_first();
                }
            })
            .or_insert_with(|| {
                let set =
                    std::iter::once(UserTagByTime::from(tag.clone())).collect::<BTreeSet<_>>();
                let (views, buys) = match tag.action {
                    Action::View => (set, BTreeSet::new()),
                    Action::Buy => (BTreeSet::new(), set),
                };
                UserProfileInner {
                    cookie: tag.cookie.clone(),
                    views,
                    buys,
                }
            });
    }

    async fn last_tags_by_cookie<'a>(
        &'a self,
        cookie: &'a str,
        time_from: DateTime<Utc>,
        time_to: DateTime<Utc>,
        limit: usize,
    ) -> UserProfile {
        assert!(limit <= MAX_TAGS_BY_COOKIE);

        let data = self.data.read().await;

        let profile = data
            .tags_by_cookie
            .get(cookie)
            .map(|profile| {
                fn filtered_iter<'a>(
                    iter: impl Iterator<Item = &'a UserTagByTime> + DoubleEndedIterator,
                    time_from: DateTime<Utc>,
                    time_to: DateTime<Utc>,
                    limit: usize,
                ) -> impl Iterator<Item = &'a UserTag> {
                    iter.map(|tag| &tag.0)
                        .rev()
                        .skip_while(move |tag| tag.time > time_to)
                        .take_while(move |tag| tag.time >= time_from)
                        .take(limit)
                }

                let views = filtered_iter(profile.views.iter(), time_from, time_to, limit)
                    .cloned()
                    .collect();
                let buys = filtered_iter(profile.buys.iter(), time_from, time_to, limit)
                    .cloned()
                    .collect();
                UserProfile {
                    cookie: cookie.into(),
                    views,
                    buys,
                }
            })
            .unwrap_or_else(|| UserProfile {
                cookie: cookie.into(),
                views: Default::default(),
                buys: Default::default(),
            });

        utils::check_user_profile(&profile, time_from, time_to, limit);
        profile
    }

    async fn select_bucket_stats(
        &self,
        time_from: DateTime<Utc>,
        time_to: DateTime<Utc>,
        action: Action,
        origin: Option<&str>,
        brand_id: Option<&str>,
        category_id: Option<&str>,
    ) -> Vec<Bucket> {
        let time_from = UtcMinute::from(time_from);
        let time_to = UtcMinute::from(time_to);
        assert!(time_from < time_to);
        let read_guard = self.data.read().await;
        let range = read_guard
            .tags_by_timestamp
            .range(time_from.inner()..time_to.inner());

        struct BucketIter<'a, It: Iterator<Item = (&'a DateTime<Utc>, &'a Vec<UserTag>)>> {
            min_curr: UtcMinute,
            min_to: UtcMinute,
            it: std::iter::Peekable<It>,
            action: Action,
            origin: Option<&'a str>,
            brand_id: Option<&'a str>,
            category_id: Option<&'a str>,
        }

        impl<'a, It: Iterator<Item = (&'a DateTime<Utc>, &'a Vec<UserTag>)>> BucketIter<'a, It> {
            fn new(
                time_from: UtcMinute,
                time_to: UtcMinute,
                it: It,
                action: Action,
                origin: Option<&'a str>,
                brand_id: Option<&'a str>,
                category_id: Option<&'a str>,
            ) -> Self {
                Self {
                    min_curr: time_from,
                    min_to: time_to,
                    it: it.peekable(),
                    action,
                    origin,
                    brand_id,
                    category_id,
                }
            }
        }
        impl<'a, It: Iterator<Item = (&'a DateTime<Utc>, &'a Vec<UserTag>)>> Iterator
            for BucketIter<'a, It>
        {
            type Item = Bucket;

            fn next(&mut self) -> Option<Self::Item> {
                // Find out what bucket we are in
                let bucket_minute: UtcMinute = self.min_curr;

                // Stop condition
                if bucket_minute >= self.min_to {
                    return None;
                }

                let mut count = 0;
                let mut sum_price = 0;

                while let Some((&datetime, tags)) = self.it.peek() {
                    println!(
                        "datetime: {} min, bucket: {} min.",
                        datetime.minute(),
                        bucket_minute.inner().minute()
                    );
                    println!("datetime: {}, bucket: {}.", datetime, bucket_minute.inner());
                    match UtcMinute::from(datetime).cmp(&bucket_minute) {
                        std::cmp::Ordering::Less => unreachable!("BTreeMap iter invariant!"),
                        std::cmp::Ordering::Greater => break, // this belongs already to the next bucket
                        std::cmp::Ordering::Equal => {
                            for tag in *tags {
                                if self.action == tag.action
                                    && self
                                        .origin
                                        .map(|origin| origin == tag.origin)
                                        .unwrap_or(true)
                                    && self
                                        .brand_id
                                        .map(|brand_id| brand_id == tag.product_info.brand_id)
                                        .unwrap_or(true)
                                    && self
                                        .category_id
                                        .map(|category_id| {
                                            category_id == tag.product_info.category_id
                                        })
                                        .unwrap_or(true)
                                {
                                    count += 1;
                                    sum_price += tag.product_info.price;
                                }
                            }

                            // Advance
                            self.it.next().unwrap();
                        }
                    }
                }
                self.min_curr = self.min_curr.next();

                Some(Bucket {
                    minute: bucket_minute,
                    count,
                    sum_price,
                })
            }
        }

        BucketIter::new(
            time_from,
            time_to,
            range,
            action,
            origin,
            brand_id,
            category_id,
        )
        .collect()
    }

    async fn clear(&self) {
        let mut data = self.data.write().await;
        data.tags_by_cookie = Default::default();
    }
}

#[cfg(test)]
pub mod tests {
    use std::vec;

    use chrono::{NaiveDate, NaiveDateTime};

    use crate::types::{Device, ProductInfo, System, UtcMinute};

    use super::*;

    pub struct TestMinutes {
        pub minute_middle: UtcMinute,
        pub minute_earlier: UtcMinute,
        pub _minute_later: UtcMinute,
        pub minute_after: UtcMinute,
    }

    fn default_product_info() -> ProductInfo {
        ProductInfo {
            product_id: "0123".to_owned(),
            brand_id: "2137".to_owned(),
            category_id: "42".to_owned(),
            price: 0,
        }
    }
    fn default_tag() -> UserTag {
        UserTag {
            time: DateTime::<Utc>::MIN_UTC,
            cookie: "cookie".to_owned(),
            country: "PL".to_owned(),
            device: Device::Pc,
            action: Action::Buy,
            origin: "CHRL".to_owned(),
            product_info: default_product_info(),
        }
    }

    fn moment_middle() -> DateTime<Utc> {
        let naive_date: NaiveDate = NaiveDate::from_ymd_opt(2000, 1, 1).unwrap();
        let naive_moment: NaiveDateTime = naive_date.and_hms_opt(21, 37, 42).unwrap();
        let moment_middle: DateTime<Utc> = DateTime::from_utc(naive_moment, Utc);
        moment_middle
    }

    pub async fn build_system_and_register_tags() -> (super::System, TestMinutes) {
        let system = super::System::new();

        let minute_middle: UtcMinute = UtcMinute::from(moment_middle());

        let moment_later = moment_middle()
            .checked_add_signed(chrono::Duration::seconds(2))
            .unwrap();
        let minute_later = UtcMinute::from(moment_later);

        let moment_earlier = moment_middle()
            .checked_sub_signed(chrono::Duration::minutes(3) + chrono::Duration::seconds(1))
            .unwrap();
        let minute_earlier = UtcMinute::from(moment_earlier);

        let minute_after = UtcMinute::from(moment_later + chrono::Duration::minutes(1));

        let tags_min_zero = [
            UserTag {
                time: moment_middle(),
                action: Action::Buy,
                product_info: ProductInfo {
                    price: 20,
                    ..default_product_info()
                },
                ..default_tag()
            },
            UserTag {
                time: moment_middle() + chrono::Duration::seconds(2),
                action: Action::Buy,
                product_info: ProductInfo {
                    price: 30,
                    ..default_product_info()
                },
                ..default_tag()
            },
        ];

        let tags = tags_min_zero.into_iter();
        for tag in tags {
            system.register_user_tag(tag).await;
        }

        let minutes = TestMinutes {
            minute_middle,
            minute_earlier,
            _minute_later: minute_later,
            minute_after,
        };
        (system, minutes)
    }

    #[tokio::test]
    async fn use_case_2_profile_contains_valid_tags() {
        let (system, minutes) = build_system_and_register_tags().await;

        // limit higher than number of available entries
        let user_profile = system
            .last_tags_by_cookie(
                "cookie",
                minutes.minute_middle.inner(),
                minutes.minute_after.inner(),
                100,
            )
            .await;
        assert!(user_profile.views.is_empty());
        assert_eq!(
            user_profile.buys,
            vec![
                UserTag {
                    time: moment_middle() + chrono::Duration::seconds(2),
                    action: Action::Buy,
                    product_info: ProductInfo {
                        price: 30,
                        ..default_product_info()
                    },
                    ..default_tag()
                },
                UserTag {
                    time: moment_middle(),
                    action: Action::Buy,
                    product_info: ProductInfo {
                        price: 20,
                        ..default_product_info()
                    },
                    ..default_tag()
                },
            ]
        );

        // limit lower than number of available entries
        let user_profile = system
            .last_tags_by_cookie(
                "cookie",
                minutes.minute_middle.inner(),
                minutes.minute_after.inner(),
                1,
            )
            .await;
        assert_eq!(
            user_profile.buys,
            vec![UserTag {
                time: moment_middle() + chrono::Duration::seconds(2),
                action: Action::Buy,
                product_info: ProductInfo {
                    price: 30,
                    ..default_product_info()
                },
                ..default_tag()
            },]
        );
    }
}
