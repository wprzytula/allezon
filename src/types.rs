use std::str::FromStr;

use chrono::{DateTime, DurationRound, NaiveDateTime, Utc};
use serde::de::{Error, Visitor};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone)]
#[cfg_attr(test, derive(PartialEq, Eq))]
pub struct UserTag {
    pub time: DateTime<Utc>, // format: "2022-03-22T12:15:00.000Z"
    //   millisecond precision
    //   with 'Z' suffix
    pub cookie: String,
    pub country: String,
    pub device: Device,
    pub action: Action,
    pub origin: String,
    pub product_info: ProductInfo,
}

#[derive(Deserialize, Serialize, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum Device {
    Pc,
    Mobile,
    Tv,
}

#[derive(Deserialize, Serialize, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum Action {
    View,
    Buy,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
#[cfg_attr(test, derive(PartialEq, Eq))]
pub struct ProductInfo {
    pub product_id: String,
    pub brand_id: String,
    pub category_id: String,
    pub price: i32,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct UtcMinute(DateTime<Utc>);
impl From<DateTime<Utc>> for UtcMinute {
    fn from(time: DateTime<Utc>) -> Self {
        Self(time.duration_trunc(chrono::Duration::seconds(60)).unwrap())
    }
}
impl UtcMinute {
    pub fn inner(self) -> DateTime<Utc> {
        self.0
    }

    pub fn next(self) -> Self {
        Self(
            self.inner()
                .checked_add_signed(chrono::Duration::seconds(60))
                .unwrap(),
        )
    }

    pub fn with_added_minutes(self, count: i64) -> Self {
        let minutes: chrono::Duration = chrono::Duration::minutes(count.abs());
        match count.cmp(&0) {
            std::cmp::Ordering::Less => Self(self.0.checked_sub_signed(minutes).unwrap()),
            std::cmp::Ordering::Equal => self,
            std::cmp::Ordering::Greater => Self(self.0.checked_add_signed(minutes).unwrap()),
        }
    }
}

#[derive(Clone, Copy)]
pub struct TimeRange {
    pub from: DateTime<Utc>,
    pub to: DateTime<Utc>,
}

struct TimeRangeVisitor;

impl<'de> Visitor<'de> for TimeRangeVisitor {
    type Value = TimeRange;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("time_range")
    }

    fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
    where
        E: Error,
    {
        let (time_from, time_to) = v
            .split_once('_')
            .ok_or(E::custom("expected underscore after first DateTime"))?;
        let from = DateTime::from_utc(NaiveDateTime::from_str(time_from).map_err(E::custom)?, Utc);
        let to = DateTime::from_utc(NaiveDateTime::from_str(time_to).map_err(E::custom)?, Utc);
        Ok(Self::Value { from, to })
    }
}

impl<'de> Deserialize<'de> for TimeRange {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_str(TimeRangeVisitor)
    }
}

#[cfg(test)]
mod tests {
    use chrono::{Datelike, Timelike};

    use super::*;

    #[test]
    fn utc_minute_preserves_lower_grained_time_and_truncates_seconds() {
        let now = Utc::now();
        let utc_minute: UtcMinute = now.into();
        assert_eq!(now.year(), utc_minute.inner().year());
        assert_eq!(now.minute(), utc_minute.inner().minute());
        assert_eq!(utc_minute.inner().second(), 0);
        assert_eq!(utc_minute.inner().nanosecond(), 0);
    }

    #[test]
    fn deserialize_time_range() {
        let _: TimeRange =
            serde_json::from_str("\"2022-03-22T12:15:00.000_2022-03-22T12:30:00.000\"").unwrap();
    }

    #[test]
    fn deserialize_user_tag() {
        let tag_str = r#"
        {
            "time": "2022-03-22T12:15:00.000Z",
            "cookie": "user",
            "country": "PL",
            "device": "PC",
            "action": "VIEW",
            "origin": "Rawa",
            "product_info": {
                "product_id": "pineapple",
                "brand_id": "apple",
                "category_id": "fruit",
                "price": 50
            }
        }"#;
        let _: UserTag = serde_json::from_str(tag_str).unwrap();
    }
}
