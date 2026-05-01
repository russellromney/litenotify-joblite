# honker-extension

SQLite loadable extension for [Honker](https://honker.dev). Adds every `honker_*` SQL scalar function (queues, streams, scheduler, pub/sub, rate limits, locks, results) to any SQLite 3.9+ client.

## Install

From crates.io (builds `libhonker_ext.dylib` / `.so` for your platform):

```bash
cargo install honker-extension
# or build from source:
cargo build --release -p honker-extension
# → target/release/libhonker_ext.{dylib,so}
```

Prebuilt binaries per platform are available at [GitHub releases](https://github.com/russellromney/honker/releases/latest).

## Use

```sql
.load ./libhonker_ext
SELECT honker_bootstrap();

-- Queues
SELECT honker_enqueue('emails', '{"to":"alice"}', NULL, NULL, 0, 3, NULL);
SELECT honker_claim_batch('emails', 'worker-1', 32, 300);
SELECT honker_ack_batch('[1,2,3]', 'worker-1');

-- Streams (durable pub/sub)
SELECT honker_stream_publish('orders', 'k', '{"id":42}');
SELECT honker_stream_read_since('orders', 0, 1000);

-- pg_notify-style pub/sub
SELECT notify('orders', '{"id":42}');
```

Full SQL reference: [honker.dev/reference/extension](https://honker.dev/reference/extension/).

## License

Dual-licensed under MIT or Apache-2.0, at your option.
