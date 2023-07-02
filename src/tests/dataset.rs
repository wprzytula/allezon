use chrono::prelude::*;
use rand::{seq::SliceRandom, Rng};
use std::collections::HashSet;

use super::utils::random_string;
use crate::types;

pub struct DataSet {
    cookies: Vec<String>,
    countries: Vec<String>,
    origins: Vec<String>,
    brands: Vec<String>,
    categories: Vec<String>,
    devices: Vec<types::Device>,
    actions: Vec<types::Action>,
}

#[derive(Default)]
pub struct UserTagConfig {
    pub cookie: Option<String>,
    pub action: Option<types::Action>,
    pub time: Option<DateTime<Utc>>,
}

fn init_data_set(size: usize) -> Vec<String> {
    let mut data_set = HashSet::new();

    while data_set.len() < size {
        data_set.insert(random_string(10));
    }

    data_set.into_iter().collect()
}

impl DataSet {
    pub fn new() -> Self {
        Self {
            cookies: init_data_set(1_000),
            countries: init_data_set(1_000),
            origins: init_data_set(1_000),
            brands: init_data_set(250),
            categories: init_data_set(67),
            devices: vec![types::Device::Pc, types::Device::Tv, types::Device::Mobile],
            actions: vec![types::Action::Buy, types::Action::View],
        }
    }

    pub fn random_user_tag(&self, config: UserTagConfig) -> types::UserTag {
        let rng = &mut rand::thread_rng();
        let cookie = config
            .cookie
            .unwrap_or_else(|| self.cookies.choose(rng).unwrap().clone());
        let action = config
            .action
            .unwrap_or_else(|| *self.actions.choose(rng).unwrap());
        let time = config.time.unwrap_or_else(|| Utc::now());
        let correct_time = DateTime::<Utc>::from_utc(
            time.naive_utc()
                .with_nanosecond(time.nanosecond() - time.nanosecond() % 1_000_000)
                .unwrap(),
            Utc,
        );

        types::UserTag {
            time: correct_time,
            cookie,
            country: self.countries.choose(rng).unwrap().clone(),
            device: self.devices.choose(rng).unwrap().clone(),
            action,
            origin: self.origins.choose(rng).unwrap().clone(),
            product_info: types::ProductInfo {
                product_id: random_string(100),
                brand_id: self.brands.choose(rng).unwrap().clone(),
                category_id: self.categories.choose(rng).unwrap().clone(),
                price: rng.gen_range(0..1000),
            },
        }
    }
}
