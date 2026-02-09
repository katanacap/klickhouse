# Klickhouse (Fork by Katana Capital)

NOTICE: This repository is a fork of [Protryon/klickhouse](https://github.com/Protryon/klickhouse). Since the upstream repository has not been updated for some time, we at Katana Capital decided to maintain our own fork in an open-source manner. We extend our deepest gratitude to the original author for creating this wonderful crate, and hope they are not inconvenienced by our decision to continue development under our organization.

## About the Project

Klickhouse is a pure Rust SDK for working with ClickHouse via its native protocol in asynchronous environments. The aim is to reduce boilerplate code while maximizing performance.

## Example Usage

See [example usage](https://github.com/katanacap/klickhouse/blob/master/klickhouse/examples/basic.rs).

## Supported Enum Types

ClickHouse `Enum8` and `Enum16` are fully supported. You can map them to `String`, raw `i8`/`i16`, or directly to a Rust enum:

```rust
// Option 1: Map to String
#[derive(klickhouse::Row)]
struct MyRow {
    status: String, // reads/writes enum name, e.g. "active"
}

// Option 2: Map to a Rust enum with #[derive(ClickhouseEnum)]
#[derive(klickhouse::ClickhouseEnum, Debug, PartialEq, Clone)]
#[klickhouse(rename_all = "snake_case")]
enum Status {
    Active,                          // -> "active"
    Inactive,                        // -> "inactive"
    #[klickhouse(rename = "removed")]
    Deleted,                         // -> "removed"
}

#[derive(klickhouse::Row)]
struct MyRow2 {
    status: Status, // maps to Enum8('active'=1, 'inactive'=2, 'removed'=3)
}
```

Supported `rename_all` rules: `lowercase`, `UPPERCASE`, `PascalCase`, `camelCase`, `snake_case`, `SCREAMING_SNAKE_CASE`, `kebab-case`, `SCREAMING-KEBAB-CASE`.

## Running the tests

A Clickhouse server is required to run the integration tests. One can be started easily in a Docker container:

```sh
$ docker run  --rm --name clickhouse -p 19000:9000 --ulimit nofile=262144:262144 clickhouse
$ export KLICKHOUSE_TEST_ADDR=127.0.0.1:19000
$ # export KLICKHOUSE_TEST_USER=default
$ # export KLICKHOUSE_TEST_PASSWORD=default
$ # export KLICKHOUSE_TEST_DATABASE=default
$ cargo nextest run
```

(running the tests simultaneously with `cargo test` is currently not suported, due to loggers initializations.)

## Feature flags

- `derive`: Enable [klickhouse_derive], providing a derive macro for the [Row] trait. Default.
- `compression`: `lz4` compression for client/server communication. Default.
- `serde`: Derivation of [serde::Serialize] and [serde::Deserialize] on various objects, and JSON support. Default.
- `tls`: TLS support via [tokio-rustls](https://crates.io/crates/tokio-rustls).
- `refinery`: Migrations via [refinery](https://crates.io/crates/refinery).
- `geo-types`: Conversion of geo types to/from the [geo-types](https://crates.io/crates/geo-types) crate.
- `bb8`: Enables a `ConnectionManager` managed by bb8

## Credit

`klickhouse_derive` was made by copy/paste/simplify of `serde_derive` to get maximal functionality and performance at lowest time-cost. In a prototype, `serde` was directly used, but this was abandoned due to lock-in of `serde`'s data model.
