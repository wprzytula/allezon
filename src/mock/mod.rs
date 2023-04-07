use std::collections::{BTreeMap, BinaryHeap};

use chrono::{DateTime, Timelike, Utc};
use either::Either;

use crate::types::{Action, UserTag, UtcMinute};

const MAX_TAGS_BY_COOKIE: usize = 200;

#[derive(Debug)]
pub struct System {
    // For 3rd use case - aggregates.
    tags_by_timestamp: BTreeMap<DateTime<Utc>, Vec<UserTag>>,

    // For 2nd use case - user profiles.
    tags_by_cookie: BTreeMap<String, UserProfile>,
}

#[derive(Debug)]
struct UserProfile {
    views: BinaryHeap<UserTagByTime>,
    buys: BinaryHeap<UserTagByTime>,
}

impl UserProfile {
    fn iter<'a>(
        &'a self,
    ) -> UserProfileIter<impl Iterator<Item = &'a UserTag>, impl Iterator<Item = &'a UserTag>> {
        UserProfileIter {
            views: self.views.iter().map(|tag_by_time| &tag_by_time.0),
            buys: self
                .buys
                .iter()
                .map(|tag_by_time: &UserTagByTime| &tag_by_time.0),
        }
    }
}

pub struct UserProfileIter<'a, It1, It2>
where
    It1: Iterator<Item = &'a UserTag>,
    It2: Iterator<Item = &'a UserTag>,
{
    views: It1,
    buys: It2,
}

impl<'a, It1, It2> UserProfileIter<'a, It1, It2>
where
    It1: Iterator<Item = &'a UserTag>,
    It2: Iterator<Item = &'a UserTag>,
{
    fn either_right<It: Iterator<Item = &'a UserTag>>(
        self,
    ) -> UserProfileIter<'a, Either<It, It1>, Either<It, It2>> {
        UserProfileIter {
            views: Either::Right(self.views),
            buys: Either::Right(self.buys),
        }
    }
}

impl<'a> UserProfileIter<'a, std::iter::Empty<&'a UserTag>, std::iter::Empty<&'a UserTag>> {
    fn new_empty() -> Self {
        Self {
            views: std::iter::empty(),
            buys: std::iter::empty(),
        }
    }

    fn either_left<It1: Iterator<Item = &'a UserTag>, It2: Iterator<Item = &'a UserTag>>(
        self,
    ) -> UserProfileIter<
        'a,
        Either<std::iter::Empty<&'a UserTag>, It1>,
        Either<std::iter::Empty<&'a UserTag>, It2>,
    > {
        UserProfileIter {
            views: Either::Left(self.views),
            buys: Either::Left(self.buys),
        }
    }
}

#[derive(Debug)]
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

impl Default for System {
    fn default() -> Self {
        Self::new()
    }
}

impl System {
    pub fn new() -> Self {
        Self {
            tags_by_timestamp: Default::default(),
            tags_by_cookie: Default::default(),
        }
    }

    pub fn register_user_tag(&mut self, tag: UserTag) {
        self.tags_by_timestamp
            .entry(tag.time)
            .or_default()
            .push(tag.clone());

        self.tags_by_cookie
            .entry(tag.cookie.clone())
            .and_modify(|user_profile| {
                let heap = match tag.action {
                    Action::View => &mut user_profile.views,
                    Action::Buy => &mut user_profile.buys,
                };
                heap.push(tag.clone().into());
                if heap.len() > 200 {
                    heap.pop().unwrap();
                }
            })
            .or_insert_with(|| {
                let heap =
                    std::iter::once(UserTagByTime::from(tag.clone())).collect::<BinaryHeap<_>>();
                let (views, buys) = match tag.action {
                    Action::View => (heap, BinaryHeap::new()),
                    Action::Buy => (BinaryHeap::new(), heap),
                };
                UserProfile { views, buys }
            });
    }

    pub fn last_tags_by_cookie<'a>(
        &'a self,
        cookie: &str,
        limit: usize,
    ) -> UserProfileIter<'a, impl Iterator<Item = &'a UserTag>, impl Iterator<Item = &'a UserTag>>
    {
        assert!(limit <= MAX_TAGS_BY_COOKIE);
        self.tags_by_cookie
            .get(cookie)
            .map(|heap| heap.iter().either_right())
            .unwrap_or(UserProfileIter::new_empty().either_left())
    }

    pub fn aggregate<'a>(
        &'a self,
        time_from: UtcMinute,
        time_to: UtcMinute,
        action: Action,
        origin: Option<&'a str>,
        brand_id: Option<&'a str>,
        category_id: Option<&'a str>,
    ) -> impl Iterator<Item = Bucket> + 'a {
        assert!(time_from < time_to);
        let range = self.tags_by_timestamp.range(time_from.inner()..time_to.inner());
        println!("Range:\n{:#?}", range);

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
                // (*self.it.peek()?.0).into();

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
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Bucket {
    pub minute: UtcMinute,
    pub count: usize,
    pub sum_price: i32,
}

#[cfg(test)]
mod tests {
    use chrono::NaiveDate;

    use crate::types::{Device, ProductInfo};

    use super::*;

    #[test]
    fn use_case_3_aggregates_properly() {
        let mut system = System::new();
        let default_product_info = ProductInfo {
            product_id: "0123".to_owned(),
            brand_id: "2137".to_owned(),
            category_id: "42".to_owned(),
            price: 0,
        };
        let default_tag = UserTag {
            time: DateTime::<Utc>::MIN_UTC,
            cookie: "cookie".to_owned(),
            country: "PL".to_owned(),
            device: Device::Pc,
            action: Action::Buy,
            origin: "CHRL".to_owned(),
            product_info: default_product_info.clone(),
        };

        let naive_date = NaiveDate::from_ymd_opt(2000, 1, 1).unwrap();
        let naive_moment = naive_date.and_hms_opt(21, 37, 42).unwrap();

        let moment_middle = DateTime::from_utc(naive_moment, Utc);
        let minute_middle = UtcMinute::from(moment_middle);

        let moment_later = moment_middle
            .checked_add_signed(chrono::Duration::seconds(2))
            .unwrap();
        let _minute_later = UtcMinute::from(moment_later);

        let moment_earlier = moment_middle
            .checked_sub_signed(chrono::Duration::minutes(3) + chrono::Duration::seconds(1))
            .unwrap();
        let minute_earlier = UtcMinute::from(moment_earlier);

        let minute_after = UtcMinute::from(moment_later + chrono::Duration::minutes(1));

        let tags_min_zero = [
            UserTag {
                time: moment_middle,
                action: Action::Buy,
                product_info: ProductInfo {
                    price: 20,
                    ..default_product_info.clone()
                },
                ..default_tag.clone()
            },
            UserTag {
                time: moment_middle,
                action: Action::Buy,
                product_info: ProductInfo {
                    price: 30,
                    ..default_product_info.clone()
                },
                ..default_tag.clone()
            },
        ];

        let tags = tags_min_zero.into_iter();
        for tag in tags {
            system.register_user_tag(tag);
        }

        // println!("{:#?}", system);

        assert_eq!(
            system
                .aggregate(minute_earlier, minute_after, Action::Buy, None, None, None)
                .collect::<Vec<_>>(),
            vec![
                Bucket{ minute: minute_middle.with_added_minutes(-3), count: 0, sum_price: 0 },
                Bucket{ minute: minute_middle.with_added_minutes(-2), count: 0, sum_price: 0 },
                Bucket{ minute: minute_middle.with_added_minutes(-1), count: 0, sum_price: 0 },
                Bucket {
                    minute: minute_middle,
                    count: 2,
                    sum_price: 50
                },
            ]
        );
    }
}
