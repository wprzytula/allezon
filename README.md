Usage:
1. Run server with:
$ cargo run
It listens on 127.0.0.5:9042. To test functionality, these are example operations to issue:

`❯ http --json POST 127.0.0.5:9042/user_tags time="2022-03-22T12:15:00.000Z" cookie="cookie" country="PL" device="PC" action="BUY" origin="CHRL" product_info:='{"product_id": "pineapple", "brand_id": "apple", "category_id": "fruit", "price": 50}'`

`❯ http --json POST 127.0.0.5:9042/user_tags time="2022-03-22T12:15:00.000Z" cookie="cookie" country="PL" device="PC" action="VIEW" origin="CHRL" product_info:='{"product_id": "pineapple", "brand_id": "apple", "category_id": "fruit", "price": 50}'

`❯ http POST 127.0.0.5:9042/aggregates\?time_range="2022-03-22T12:15:00_2022-03-22T12:16:00"\&action="VIEW"`

`❯ http POST 127.0.0.5:9042/user_profiles/cookie\?time_range="2022-03-22T12:15:00_2022-03-22T12:16:00"\&limit=3`

`❯ http POST 127.0.0.5:9042/aggregates\?time_range="2022-03-22T12:15:00_2022-03-22T12:16:00"\&action="VIEW"`

`❯ http POST 127.0.0.5:9042/aggregates\?time_range="2022-03-22T12:15:00_2022-03-22T12:16:00"\&action="VIEW"\&aggregates="count"\&aggregates="sum_price"`

`❯ http --json POST 127.0.0.5:9042/user_tags time="2022-03-22T12:17:00.000Z" cookie="cookie" country="PL" device="PC" action="VIEW" origin="CHRL" product_info:='{"product_id": "pineapple", "brand_id": "pear", "category_id": "fruit", "price": 30}'`

`❯ http POST 127.0.0.5:9042/aggregates\?time_range="2022-03-22T12:15:00_2022-03-22T12:18:00"\&action="VIEW"\&aggregates="count"\&aggregates="sum_price"`
