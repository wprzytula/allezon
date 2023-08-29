use chrono::{DateTime, Utc};

use crate::types;

pub fn check_user_profile(
    user_profile: &types::UserProfile,
    time_from: DateTime<Utc>,
    time_to: DateTime<Utc>,
    limit: usize,
) {
    check_user_tags_vector(&user_profile.buys, time_from, time_to, limit);
    check_user_tags_vector(&user_profile.views, time_from, time_to, limit);
}

fn check_user_tags_vector(
    user_tags: &Vec<types::UserTag>,
    time_from: DateTime<Utc>,
    time_to: DateTime<Utc>,
    limit: usize,
) {
    assert!(user_tags.len() <= limit);

    for i in 1..user_tags.len() {
        assert!(user_tags[i - 1].time >= user_tags[i].time);
    }

    for user_tag in user_tags {
        assert!(user_tag.time >= time_from);
        assert!(user_tag.time < time_to);
    }
}
