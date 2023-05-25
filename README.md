## Usage
Run the server with:
```shell
cargo run -- -a [listen address] -p [listen port] -s [scylla url]
```
It listens on `[listen address]:[listen port]`. To test functionality, these are example operations to issue:

```shell
http --json POST 127.0.0.1:9042/user_tags time="2022-03-22T12:15:00.000Z" cookie="cookie" country="PL" device="PC" action="BUY" origin="CHRL" product_info:='{"product_id": "pineapple", "brand_id": "apple", "category_id": "fruit", "price": 50}'
```

```shell
http --json POST 127.0.0.1:9042/user_tags time="2022-03-22T12:15:00.000Z" cookie="cookie" country="PL" device="PC" action="VIEW" origin="CHRL" product_info:='{"product_id": "pineapple", "brand_id": "apple", "category_id": "fruit", "price": 50}'
```

```shell
http POST 127.0.0.1:9042/aggregates\?time_range="2022-03-22T12:15:00_2022-03-22T12:16:00"\&action="VIEW"
```

```shell
http POST 127.0.0.1:9042/user_profiles/cookie\?time_range="2022-03-22T12:15:00_2022-03-22T12:16:00"\&limit=3
```

```shell
http POST 127.0.0.1:9042/aggregates\?time_range="2022-03-22T12:15:00_2022-03-22T12:16:00"\&action="VIEW"
```

```shell
http POST 127.0.0.1:9042/aggregates\?time_range="2022-03-22T12:15:00_2022-03-22T12:16:00"\&action="VIEW"\&aggregates="count"\&aggregates="sum_price"
```

```shell
http --json POST 127.0.0.1:9042/user_tags time="2022-03-22T12:17:00.000Z" cookie="cookie" country="PL" device="PC" action="VIEW" origin="CHRL" product_info:='{"product_id": "pineapple", "brand_id": "pear", "category_id": "fruit", "price": 30}'
```

```shell
http POST 127.0.0.1:9042/aggregates\?time_range="2022-03-22T12:15:00_2022-03-22T12:18:00"\&action="VIEW"\&aggregates="count"\&aggregates="sum_price"
```

## Testing
Setup
1. Scylla cluster, for example:
```docker
docker run  -p 9042:9042 scylladb/scylla --smp 1 --authenticator AllowAllAuthenticator --skip-wait-for-gossip-to-settle 0
```
2. Server with:
```shell
cargo run -- -a localhost -p [server listen port] -s localhost:9042
```
3. Mock server (to compare results) with :
```shell
cargo run -- -a localhost -p [mock listen port] -m
```

Run tests with:
```shell
SERVER_URL="localhost:[server listen port]" MOCK_URL="localhost:[mock listen port]" cargo test
```
