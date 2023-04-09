use std::{
    fmt::Display,
    sync::{Arc, RwLock},
};

use axum::{
    extract::{Json, Path, Query, State},
    routing::{get, post},
    Router,
};
use reqwest::StatusCode;
use serde::{de::Visitor, Deserialize, Serialize};

use crate::{
    mock::{Bucket, System},
    types::{Action, TimeRange, UserProfile, UserTag},
};

pub fn build_router(initial_system: System) -> Router {
    Router::new()
        .route("/echo", get(|| async { "ECHO!" }))
        .route("/user_tags", post(use_case_1))
        .route("/user_profiles/:cookie", post(use_case_2))
        .route("/aggregates", post(use_case_3))
        .with_state(Arc::new(RwLock::new(initial_system)))
}

// `StatusCode` implement `IntoResponse` and therefore
// `Result<Status, StatusCode>` also implements `IntoResponse`
#[axum_macros::debug_handler] // <- this provides better error messages
async fn use_case_1(
    State(system): State<Arc<RwLock<System>>>, // extract state in this handler
    Query(_params): Query<()>,                 // this asserts that the params are empty
    Json(tag): Json<UserTag>,
) -> Result<StatusCode, StatusCode> {
    system.write().unwrap().register_user_tag(tag);

    Ok(StatusCode::NO_CONTENT)
}

#[cfg_attr(test, derive(Serialize))]
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct UseCase2Params {
    time_range: TimeRange,
    limit: Option<i32>,
}

#[axum_macros::debug_handler] // <- this provides better error messages
async fn use_case_2(
    State(system): State<Arc<RwLock<System>>>, // extract state in this handler
    Path(cookie): Path<String>,
    Query(params): Query<UseCase2Params>,
) -> Result<Json<UserProfile>, (StatusCode, String)> {
    let UseCase2Params {
        time_range: TimeRange {
            from: time_from,
            to: time_to,
        },
        limit,
    } = params;

    if let Some(limit) = limit {
        if limit < 0 || limit > 200 {
            return Err((
                StatusCode::BAD_REQUEST,
                "'limit' out of accepted bounds '[0, 200]'".to_owned(),
            ));
        }
    }

    let system_guard = system.read().unwrap();
    let user_profile = system_guard.last_tags_by_cookie(
        &cookie,
        time_from,
        time_to,
        limit.unwrap_or(200) as usize,
    );

    Ok(Json(user_profile))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
enum Aggregate {
    Count,
    SumPrice,
}

impl Aggregate {
    fn display(&self) -> &'static str {
        match self {
            Aggregate::Count => "count",
            Aggregate::SumPrice => "sum_price",
        }
    }
}

#[derive(Debug, Clone)]
struct Aggregates {
    fst: Option<Aggregate>,
    snd: Option<Aggregate>,
}

#[derive(Debug)]
struct RepeatedAggregate(Aggregate);
impl Display for RepeatedAggregate {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "repeated aggregate: {:?}", self.0)
    }
}

impl Aggregates {
    fn new() -> Self {
        Self {
            fst: None,
            snd: None,
        }
    }

    fn add(&mut self, new_agg: Aggregate) -> Result<(), RepeatedAggregate> {
        match self.fst {
            None => {
                self.fst = Some(new_agg);
                Ok(())
            }
            Some(agg) if agg == new_agg => Err(RepeatedAggregate(new_agg)),
            Some(_) => {
                if self.snd.is_some() {
                    Err(RepeatedAggregate(new_agg))
                } else {
                    self.snd = Some(new_agg);
                    Ok(())
                }
            }
        }
    }
}

#[derive(Debug, Clone)]
struct UseCase3Params {
    time_range: TimeRange,
    action: Action,
    aggregates: Aggregates,
    origin: Option<String>,
    brand_id: Option<String>,
    category_id: Option<String>,
}

use std::fmt;

use serde::de::{self, Deserializer, MapAccess};

impl<'de> Deserialize<'de> for UseCase3Params {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(field_identifier, rename_all = "snake_case")]
        enum Field {
            TimeRange,
            Action,
            Aggregates,
            Origin,
            BrandId,
            CategoryId,
        }

        struct UseCase3ParamsVisitor;
        impl<'de> Visitor<'de> for UseCase3ParamsVisitor {
            type Value = UseCase3Params;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("struct UseCase3Params")
            }

            fn visit_map<V>(self, mut map: V) -> Result<UseCase3Params, V::Error>
            where
                V: MapAccess<'de>,
            {
                let mut action = None;
                let mut origin = None;
                let mut time_range = None;
                let mut brand_id = None;
                let mut category_id = None;
                let mut aggregates = Aggregates::new();
                while let Some(key) = map.next_key()? {
                    match key {
                        Field::Action => {
                            if action.is_some() {
                                return Err(de::Error::duplicate_field("action"));
                            }
                            action = Some(map.next_value()?);
                        }
                        Field::Origin => {
                            if origin.is_some() {
                                return Err(de::Error::duplicate_field("origin"));
                            }
                            origin = Some(map.next_value()?);
                        }
                        Field::TimeRange => {
                            if time_range.is_some() {
                                return Err(de::Error::duplicate_field("time_range"));
                            }
                            time_range = Some(map.next_value()?);
                        }
                        Field::BrandId => {
                            if brand_id.is_some() {
                                return Err(de::Error::duplicate_field("brand_id"));
                            }
                            brand_id = Some(map.next_value()?);
                        }
                        Field::CategoryId => {
                            if category_id.is_some() {
                                return Err(de::Error::duplicate_field("category_id"));
                            }
                            category_id = Some(map.next_value()?);
                        }
                        Field::Aggregates => aggregates
                            .add(map.next_value()?)
                            .map_err(de::Error::custom)?,
                    }
                }
                let action = action.ok_or_else(|| de::Error::missing_field("action"))?;
                let time_range =
                    time_range.ok_or_else(|| de::Error::missing_field("time_range"))?;
                Ok(UseCase3Params {
                    time_range,
                    action,
                    aggregates,
                    origin,
                    brand_id,
                    category_id,
                })
            }
        }

        const FIELDS: &'static [&'static str] = &[
            "origin",
            "action",
            "time_range",
            "brand_id",
            "category_id",
            "aggregates",
        ];
        deserializer.deserialize_struct("UseCase3Params", FIELDS, UseCase3ParamsVisitor)
    }
}

///////// Example of response for use case 3
// {
//     "columns": ["1m_bucket", "action", "brand_id", "sum_price", "count"],
//     "rows": [
//       ["2022-03-01T00:05:00", "BUY", "Nike", "1000", "3"],
//       ["2022-03-01T00:06:00", "BUY", "Nike", "1500", "4"],
//       ["2022-03-01T00:07:00", "BUY", "Nike", "1200", "2"]
// }
#[derive(Serialize)]
struct UseCase3Response {
    /*
    ▪ First column is called "1m_bucket" .
    ▪ Bucket values have format: 2022-03-01T00:05:00
    ▪ They represent bucket start (second precision, full
    minutes).
    ▪ Only start of the bucket is needed, because bucket size is
    fixed (1 minute).
    ▪ Buckets are inclusive at their beginnings and exclusive at
    their ends.
    ▪ Filter columns are in the following order: "action", "origin",
    "brand_id", "category_id" .
    ▪ Include only those with not-null values (i.e. present in the
    query, but with the order defined above).
    ▪ Aggregate columns are listed in the order from the query.
    ▪ ALL VALUES ARE STRINGS (including aggregates: count,
    sum_price).
    */
    columns: Vec<String>,
    rows: Vec<Vec<String>>,
}
impl UseCase3Response {
    fn new(params: UseCase3Params, buckets: impl Iterator<Item = Bucket>) -> Self {
        let UseCase3Params {
            action,
            aggregates: Aggregates { fst, snd },
            origin,
            brand_id,
            category_id,
            ..
        } = params;

        // ▪ First column is called "1m_bucket".
        // Action is mandatory as well.
        let mut columns = vec!["1m_bucket".to_owned(), "action".to_owned()];

        // ▪ Filter columns are in the following order: "action", "origin", "brand_id", "category_id".
        if origin.is_some() {
            columns.push("origin".to_owned());
        }
        if brand_id.is_some() {
            columns.push("brand_id".to_owned());
        }
        if category_id.is_some() {
            columns.push("category_id".to_owned());
        }

        for agg in [fst, snd] {
            if let Some(aggregate) = agg {
                columns.push(aggregate.display().to_owned());
            }
        }

        let rows = buckets
            .map(
                |Bucket {
                     minute,
                     count,
                     sum_price,
                 }| {
                    let mut columns =
                        vec![minute.inner().naive_utc().to_string(), action.to_string()];

                    // ▪ Filter columns are in the following order: "action", "origin", "brand_id", "category_id".
                    if let Some(origin) = origin.clone() {
                        columns.push(origin);
                    }
                    if let Some(brand_id) = brand_id.clone() {
                        columns.push(brand_id);
                    }
                    if let Some(category_id) = category_id.clone() {
                        columns.push(category_id);
                    }

                    for agg in [fst, snd] {
                        if let Some(aggregate) = agg {
                            let agg_val = match aggregate {
                                Aggregate::Count => count.to_string(),
                                Aggregate::SumPrice => sum_price.to_string(),
                            };
                            columns.push(agg_val);
                        }
                    }

                    columns
                },
            )
            .collect();

        Self { columns, rows }
    }
}

#[axum_macros::debug_handler] // <- this provides better error messages
async fn use_case_3(
    State(system): State<Arc<RwLock<System>>>, // extract state in this handler
    // params: Result<Query<UseCase3Params>, QueryRejection>, <-- for debug
    Query(params): Query<UseCase3Params>,
) -> Result<Json<UseCase3Response>, StatusCode> {
    dbg!(&params);

    // let Query(params) = params.map_err(|_| StatusCode::BAD_REQUEST)?; <-- for debug

    let UseCase3Params {
        time_range: TimeRange {
            from: time_from,
            to: time_to,
        },
        action,
        origin,
        brand_id,
        category_id,
        ..
    } = params.clone();

    let system_guard = system.read().unwrap();
    let bucket_iterator = system_guard.aggregate(
        time_from.into(),
        time_to.into(),
        action,
        origin.as_deref(),
        brand_id.as_deref(),
        category_id.as_deref(),
    );

    let response = UseCase3Response::new(params, bucket_iterator);

    Ok(Json(response))
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;

    use tokio::sync::oneshot;
    use tracing::{info, instrument::WithSubscriber};

    use crate::mock::tests::build_system_and_register_tags;

    use super::*;

    #[tokio::test]
    async fn simplest_echo() {
        let router = build_router(System::new());
        tokio::spawn(
            axum::Server::bind(&SocketAddr::from(([127, 0, 0, 4], 9042)))
                .serve(router.into_make_service()),
        );

        let client = reqwest::Client::new();
        client
            .get("http://127.0.0.4:9042/echo")
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap();
    }

    // In order to see the logs, run the tests with:
    // $ RUST_LOG=<level> cargo test
    // where level \in {trace, debug, info, warn, error}
    fn init_logger() {
        let _ = tracing_subscriber::fmt::fmt()
            .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
            .without_time()
            .try_init();
    }

    #[tokio::test]
    async fn test_use_case_1() {
        init_logger();
        let (tx, rx) = oneshot::channel::<()>();
        let router = build_router(System::new());
        let server = axum::Server::bind(&SocketAddr::from(([127, 0, 0, 5], 9042)))
            .serve(router.into_make_service())
            .with_graceful_shutdown(async move {
                let _ = rx.await;
            })
            .with_current_subscriber();

        let request_fut = async {
            let client = reqwest::Client::new();
            let response = client
                .post("http://127.0.0.5:9042/user_tags")
                .body(
                    r#"
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
                }"#,
                )
                .header("Content-Type", "application/json")
                .send()
                .await
                .unwrap();
            tx.send(()).unwrap();
            response.error_for_status().unwrap();
        };

        let _ = futures::future::join(server, request_fut).await;
    }

    #[tokio::test]
    async fn test_use_case_2() {
        init_logger();
        let (tx, rx) = oneshot::channel::<()>();
        let (system, test_minutes) = build_system_and_register_tags();
        let router = build_router(system);
        let server = axum::Server::bind(&SocketAddr::from(([127, 0, 0, 6], 9042)))
            .serve(router.into_make_service())
            .with_graceful_shutdown(async move {
                let _ = rx.await;
            })
            .with_current_subscriber();

        let request_fut = async {
            let client = reqwest::Client::new();
            let response = client
                .post("http://127.0.0.6:9042/user_profiles/cookie")
                .query(&UseCase2Params {
                    limit: Some(1),
                    time_range: TimeRange {
                        from: test_minutes.minute_middle.inner(),
                        to: test_minutes.minute_after.inner(),
                    },
                })
                .send()
                .await
                .unwrap();
            tx.send(()).unwrap();
            let result = response.error_for_status().unwrap();
            println!("{}", result.text().await.unwrap());
        };

        let _ = futures::future::join(server, request_fut).await;
    }

    #[tokio::test]
    async fn test_use_case_3() {
        init_logger();
        let (tx, rx) = oneshot::channel::<()>();
        let (system, test_minutes) = build_system_and_register_tags();
        let router = build_router(system);
        let server = axum::Server::bind(&SocketAddr::from(([127, 0, 0, 3], 9042)))
            .serve(router.into_make_service())
            .with_graceful_shutdown(async move {
                let _ = rx.await;
            })
            .with_current_subscriber();

        let request_fut = async {
            let client = reqwest::Client::new();
            let time_range = TimeRange {
                from: test_minutes.minute_earlier.inner(),
                to: test_minutes.minute_after.inner(),
            }
            .to_string();
            dbg!(&time_range);
            let response = client
                .post("http://127.0.0.3:9042/aggregates")
                .query(&[
                    ("time_range", time_range.as_str()),
                    ("action", "BUY"),
                    ("aggregates", "count"),
                    ("aggregates", "sum_price"),
                ])
                .send()
                .await
                .unwrap();
            tx.send(()).unwrap();
            let result = response.error_for_status();
            let text = result.unwrap().text().await.unwrap();
            info!("#### Received response:\n{}", &text);
            info!(
                "{:#?}",
                serde_json::from_str::<serde_json::Value>(&text).unwrap()
            );
        };

        let _ = futures::future::join(server, request_fut).await;
    }
}
