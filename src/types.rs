use chrono::{DateTime, DurationRound, Utc};
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Debug, Clone)]
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

#[derive(Deserialize, Serialize, Debug, Clone, Copy)]
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
}
