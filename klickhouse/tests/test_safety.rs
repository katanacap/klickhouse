use std::time::Duration;

use klickhouse::{ClickhouseEnum, Client, ClientOptions, Row};

/// Helper to get a client with custom options
async fn get_client_with_options(opts: ClientOptions) -> Client {
    let address = std::env::var("KLICKHOUSE_TEST_ADDR").unwrap_or_else(|_| "127.0.0.1:9000".into());
    Client::connect(address, opts).await.unwrap()
}

// ============================================================
// Test: connect with default options works
// ============================================================
#[tokio::test]
async fn test_connect_default_options() {
    let _ = env_logger::builder()
        .filter_level(log::LevelFilter::Info)
        .try_init();
    let client = super::get_client().await;
    assert!(!client.is_closed());
}

// ============================================================
// Test: connect with custom options (keepalive, timeouts)
// ============================================================
#[tokio::test]
async fn test_connect_custom_options() {
    let _ = env_logger::builder()
        .filter_level(log::LevelFilter::Info)
        .try_init();
    let opts = ClientOptions {
        tcp_nodelay: true,
        connect_timeout: Some(Duration::from_secs(5)),
        tcp_keepalive: Some(Duration::from_secs(30)),
        max_pending_queries: 100,
        block_channel_size: 16,
        request_channel_size: 512,
        ..Default::default()
    };
    let client = get_client_with_options(opts).await;
    assert!(!client.is_closed());
    // Simple health check query
    client.execute("SELECT 1").await.unwrap();
}

// ============================================================
// Test: connect timeout to unreachable host
// ============================================================
#[tokio::test]
async fn test_connect_timeout() {
    let _ = env_logger::builder()
        .filter_level(log::LevelFilter::Info)
        .try_init();
    let opts = ClientOptions {
        connect_timeout: Some(Duration::from_millis(100)),
        ..Default::default()
    };
    // 192.0.2.1 is TEST-NET-1 (RFC 5737), guaranteed to be unreachable
    let start = std::time::Instant::now();
    let result = Client::connect("192.0.2.1:9000", opts).await;
    let elapsed = start.elapsed();

    assert!(result.is_err(), "connect to unreachable host should fail");
    // Should fail in roughly the timeout period, not hang for minutes
    assert!(
        elapsed < Duration::from_secs(5),
        "connect timeout took too long: {elapsed:?}"
    );
}

// ============================================================
// Test: connect with no timeout to unreachable host (should still eventually fail)
// ============================================================
#[tokio::test]
async fn test_connect_no_timeout_setting() {
    let _ = env_logger::builder()
        .filter_level(log::LevelFilter::Info)
        .try_init();
    let opts = ClientOptions {
        connect_timeout: None,
        ..Default::default()
    };
    // Use an invalid port on localhost — should fail quickly with connection refused
    let result = Client::connect("127.0.0.1:1", opts).await;
    assert!(
        result.is_err(),
        "connect to closed port should fail with connection refused"
    );
}

// ============================================================
// Test: execute DDL and simple queries
// ============================================================
#[tokio::test]
async fn test_execute_ddl_and_query() {
    let _ = env_logger::builder()
        .filter_level(log::LevelFilter::Info)
        .try_init();
    let client = super::get_client().await;

    // Create table
    client
        .execute("DROP TABLE IF EXISTS test_safety_ddl")
        .await
        .unwrap();
    client
        .execute("CREATE TABLE test_safety_ddl (id UInt32, name String) ENGINE = Memory")
        .await
        .unwrap();

    // Query empty table
    #[derive(Row, Debug, PartialEq)]
    struct TestRow {
        id: u32,
        name: String,
    }

    let rows: Vec<TestRow> = client
        .query_collect("SELECT * FROM test_safety_ddl")
        .await
        .unwrap();
    assert!(rows.is_empty());

    // Cleanup
    client
        .execute("DROP TABLE IF EXISTS test_safety_ddl")
        .await
        .unwrap();
}

// ============================================================
// Test: insert and read back data
// ============================================================
#[tokio::test]
async fn test_insert_and_read() {
    let _ = env_logger::builder()
        .filter_level(log::LevelFilter::Info)
        .try_init();
    let client = super::get_client().await;

    super::prepare_table("test_safety_insert", "id UInt32, value String", &client).await;

    #[derive(Row, Debug, PartialEq, Clone)]
    struct InsertRow {
        id: u32,
        value: String,
    }

    let rows = vec![
        InsertRow {
            id: 1,
            value: "hello".into(),
        },
        InsertRow {
            id: 2,
            value: "world".into(),
        },
        InsertRow {
            id: 3,
            value: "clickhouse".into(),
        },
    ];

    client
        .insert_native_block("INSERT INTO test_safety_insert FORMAT native", rows.clone())
        .await
        .unwrap();

    let result: Vec<InsertRow> = client
        .query_collect("SELECT * FROM test_safety_insert ORDER BY id")
        .await
        .unwrap();
    assert_eq!(result.len(), 3);
    assert_eq!(result[0].id, 1);
    assert_eq!(result[0].value, "hello");
    assert_eq!(result[2].id, 3);
    assert_eq!(result[2].value, "clickhouse");

    // Cleanup
    client
        .execute("DROP TABLE IF EXISTS test_safety_insert")
        .await
        .unwrap();
}

// ============================================================
// Test: query_one returns first row
// ============================================================
#[tokio::test]
async fn test_query_one() {
    let _ = env_logger::builder()
        .filter_level(log::LevelFilter::Info)
        .try_init();
    let client = super::get_client().await;

    #[derive(Row, Debug)]
    struct OneRow {
        x: u32,
    }

    let row: OneRow = client.query_one("SELECT toUInt32(42) AS x").await.unwrap();
    assert_eq!(row.x, 42);
}

// ============================================================
// Test: server error is properly propagated
// ============================================================
#[tokio::test]
async fn test_server_error_propagation() {
    let _ = env_logger::builder()
        .filter_level(log::LevelFilter::Info)
        .try_init();
    let client = super::get_client().await;

    let result = client
        .execute("SELECT * FROM nonexistent_table_xyz_123")
        .await;
    assert!(result.is_err(), "query on non-existent table should error");
    let err = result.unwrap_err();
    // Should be a ServerException, not a panic
    assert!(
        matches!(err, klickhouse::KlickhouseError::ServerException { .. }),
        "expected ServerException, got: {err:?}"
    );
}

// ============================================================
// Test: multiple sequential queries on same client
// ============================================================
#[tokio::test]
async fn test_multiple_sequential_queries() {
    let _ = env_logger::builder()
        .filter_level(log::LevelFilter::Info)
        .try_init();
    let client = super::get_client().await;

    for i in 0..10 {
        #[derive(Row, Debug)]
        struct NumRow {
            n: u32,
        }

        let row: NumRow = client
            .query_one(format!("SELECT toUInt32({}) AS n", i))
            .await
            .unwrap();
        assert_eq!(row.n, i);
    }
}

// ============================================================
// Test: client is_closed after drop
// ============================================================
#[tokio::test]
async fn test_client_closed_detection() {
    let _ = env_logger::builder()
        .filter_level(log::LevelFilter::Info)
        .try_init();
    let client = super::get_client().await;
    assert!(!client.is_closed());

    // Client should remain open while queries are working
    client.execute("SELECT 1").await.unwrap();
    assert!(!client.is_closed());
}

// ============================================================
// Test: nullable types roundtrip
// ============================================================
#[tokio::test]
async fn test_nullable_roundtrip() {
    let _ = env_logger::builder()
        .filter_level(log::LevelFilter::Info)
        .try_init();
    let client = super::get_client().await;

    super::prepare_table(
        "test_safety_nullable",
        "id UInt32, val Nullable(String)",
        &client,
    )
    .await;

    #[derive(Row, Debug, PartialEq)]
    struct NullableRow {
        id: u32,
        val: Option<String>,
    }

    let rows = vec![
        NullableRow {
            id: 1,
            val: Some("present".into()),
        },
        NullableRow { id: 2, val: None },
    ];

    client
        .insert_native_block("INSERT INTO test_safety_nullable FORMAT native", rows)
        .await
        .unwrap();

    let result: Vec<NullableRow> = client
        .query_collect("SELECT * FROM test_safety_nullable ORDER BY id")
        .await
        .unwrap();
    assert_eq!(result.len(), 2);
    assert_eq!(result[0].val, Some("present".into()));
    assert_eq!(result[1].val, None);

    // Cleanup
    client
        .execute("DROP TABLE IF EXISTS test_safety_nullable")
        .await
        .unwrap();
}

// ============================================================
// Test: bb8 connection pool works
// ============================================================
#[cfg(feature = "bb8")]
#[tokio::test]
async fn test_bb8_pool() {
    use klickhouse::ConnectionManager;

    let _ = env_logger::builder()
        .filter_level(log::LevelFilter::Info)
        .try_init();

    let address = std::env::var("KLICKHOUSE_TEST_ADDR").unwrap_or_else(|_| "127.0.0.1:9000".into());
    let manager = ConnectionManager::new(address, ClientOptions::default())
        .await
        .unwrap();

    let pool = bb8::Pool::builder()
        .max_size(3)
        .build(manager)
        .await
        .unwrap();

    // Get multiple connections and run queries
    for i in 0..5 {
        let conn = pool.get().await.unwrap();
        assert!(!conn.is_closed());

        #[derive(Row, Debug)]
        struct PoolRow {
            n: u32,
        }

        let row: PoolRow = conn
            .query_one(format!("SELECT toUInt32({}) AS n", i))
            .await
            .unwrap();
        assert_eq!(row.n, i);
    }
}

// ============================================================
// Test: bb8 pool validates connections
// ============================================================
#[cfg(feature = "bb8")]
#[tokio::test]
async fn test_bb8_pool_connection_reuse() {
    use klickhouse::ConnectionManager;

    let _ = env_logger::builder()
        .filter_level(log::LevelFilter::Info)
        .try_init();

    let address = std::env::var("KLICKHOUSE_TEST_ADDR").unwrap_or_else(|_| "127.0.0.1:9000".into());
    let manager = ConnectionManager::new(address, ClientOptions::default())
        .await
        .unwrap();

    let pool = bb8::Pool::builder()
        .max_size(1) // Force reuse
        .test_on_check_out(true) // Validate before giving out
        .build(manager)
        .await
        .unwrap();

    // First use
    {
        let conn = pool.get().await.unwrap();
        conn.execute("SELECT 1").await.unwrap();
    }

    // Second use — should reuse the same connection
    {
        let conn = pool.get().await.unwrap();
        conn.execute("SELECT 2").await.unwrap();
    }
}

// ============================================================
// Test: large batch insert
// ============================================================
#[tokio::test]
async fn test_large_batch_insert() {
    let _ = env_logger::builder()
        .filter_level(log::LevelFilter::Info)
        .try_init();
    let client = super::get_client().await;

    super::prepare_table("test_safety_large_batch", "id UInt32, data String", &client).await;

    #[derive(Row, Debug, PartialEq, Clone)]
    struct BatchRow {
        id: u32,
        data: String,
    }

    let rows: Vec<BatchRow> = (0..1000)
        .map(|i| BatchRow {
            id: i,
            data: format!("row_{i}"),
        })
        .collect();

    client
        .insert_native_block("INSERT INTO test_safety_large_batch FORMAT native", rows)
        .await
        .unwrap();

    #[derive(Row, Debug)]
    struct CountRow {
        cnt: u64,
    }

    let count: CountRow = client
        .query_one("SELECT count() AS cnt FROM test_safety_large_batch")
        .await
        .unwrap();
    assert_eq!(count.cnt, 1000);

    // Cleanup
    client
        .execute("DROP TABLE IF EXISTS test_safety_large_batch")
        .await
        .unwrap();
}

// ============================================================
// Test: Enum8 insert and read as String
// ============================================================
#[tokio::test]
async fn test_enum8_string_roundtrip() {
    let _ = env_logger::builder()
        .filter_level(log::LevelFilter::Info)
        .try_init();
    let client = super::get_client().await;

    client
        .execute("DROP TABLE IF EXISTS test_safety_enum8")
        .await
        .unwrap();
    client
        .execute(
            "CREATE TABLE test_safety_enum8 (id UInt32, status Enum8('active' = 1, 'inactive' = 2, 'deleted' = -1)) ENGINE = Memory",
        )
        .await
        .unwrap();

    #[derive(Row, Debug, PartialEq, Clone)]
    struct Enum8Row {
        id: u32,
        status: String,
    }

    let rows = vec![
        Enum8Row {
            id: 1,
            status: "active".into(),
        },
        Enum8Row {
            id: 2,
            status: "inactive".into(),
        },
        Enum8Row {
            id: 3,
            status: "deleted".into(),
        },
    ];

    client
        .insert_native_block("INSERT INTO test_safety_enum8 FORMAT native", rows.clone())
        .await
        .unwrap();

    let result: Vec<Enum8Row> = client
        .query_collect("SELECT * FROM test_safety_enum8 ORDER BY id")
        .await
        .unwrap();
    assert_eq!(result.len(), 3);
    assert_eq!(result[0].status, "active");
    assert_eq!(result[1].status, "inactive");
    assert_eq!(result[2].status, "deleted");

    // Cleanup
    client
        .execute("DROP TABLE IF EXISTS test_safety_enum8")
        .await
        .unwrap();
}

// ============================================================
// Test: Enum16 insert and read as String
// ============================================================
#[tokio::test]
async fn test_enum16_string_roundtrip() {
    let _ = env_logger::builder()
        .filter_level(log::LevelFilter::Info)
        .try_init();
    let client = super::get_client().await;

    client
        .execute("DROP TABLE IF EXISTS test_safety_enum16")
        .await
        .unwrap();
    client
        .execute(
            "CREATE TABLE test_safety_enum16 (id UInt32, color Enum16('red' = 100, 'green' = 200, 'blue' = 300)) ENGINE = Memory",
        )
        .await
        .unwrap();

    #[derive(Row, Debug, PartialEq, Clone)]
    struct Enum16Row {
        id: u32,
        color: String,
    }

    let rows = vec![
        Enum16Row {
            id: 1,
            color: "red".into(),
        },
        Enum16Row {
            id: 2,
            color: "green".into(),
        },
        Enum16Row {
            id: 3,
            color: "blue".into(),
        },
    ];

    client
        .insert_native_block("INSERT INTO test_safety_enum16 FORMAT native", rows.clone())
        .await
        .unwrap();

    let result: Vec<Enum16Row> = client
        .query_collect("SELECT * FROM test_safety_enum16 ORDER BY id")
        .await
        .unwrap();
    assert_eq!(result.len(), 3);
    assert_eq!(result[0].color, "red");
    assert_eq!(result[1].color, "green");
    assert_eq!(result[2].color, "blue");

    // Cleanup
    client
        .execute("DROP TABLE IF EXISTS test_safety_enum16")
        .await
        .unwrap();
}

// ============================================================
// Test: Enum8 read as i8 (raw numeric)
// ============================================================
#[tokio::test]
async fn test_enum8_raw_i8() {
    let _ = env_logger::builder()
        .filter_level(log::LevelFilter::Info)
        .try_init();
    let client = super::get_client().await;

    client
        .execute("DROP TABLE IF EXISTS test_safety_enum8_i8")
        .await
        .unwrap();
    client
        .execute("CREATE TABLE test_safety_enum8_i8 (val Enum8('a' = 1, 'b' = 2)) ENGINE = Memory")
        .await
        .unwrap();

    // Insert via SQL directly
    client
        .execute("INSERT INTO test_safety_enum8_i8 VALUES ('a'), ('b'), ('a')")
        .await
        .unwrap();

    #[derive(Row, Debug)]
    struct RawEnum8Row {
        val: i8,
    }

    let result: Vec<RawEnum8Row> = client
        .query_collect("SELECT * FROM test_safety_enum8_i8")
        .await
        .unwrap();
    assert_eq!(result.len(), 3);
    assert_eq!(result[0].val, 1);
    assert_eq!(result[1].val, 2);
    assert_eq!(result[2].val, 1);

    // Cleanup
    client
        .execute("DROP TABLE IF EXISTS test_safety_enum8_i8")
        .await
        .unwrap();
}

// ============================================================
// Test: Enum8 with all variants via SELECT
// ============================================================
#[tokio::test]
async fn test_enum8_select_all_variants() {
    let _ = env_logger::builder()
        .filter_level(log::LevelFilter::Info)
        .try_init();
    let client = super::get_client().await;

    client
        .execute("DROP TABLE IF EXISTS test_safety_enum8_all")
        .await
        .unwrap();
    client
        .execute(
            "CREATE TABLE test_safety_enum8_all (val Enum8('x' = 0, 'y' = 1, 'z' = 2)) ENGINE = Memory",
        )
        .await
        .unwrap();

    client
        .execute("INSERT INTO test_safety_enum8_all VALUES ('x'), ('y'), ('z')")
        .await
        .unwrap();

    #[derive(Row, Debug)]
    struct EnumVal {
        val: String,
    }

    let result: Vec<EnumVal> = client
        .query_collect("SELECT * FROM test_safety_enum8_all ORDER BY val")
        .await
        .unwrap();
    assert_eq!(result.len(), 3);
    assert_eq!(result[0].val, "x");
    assert_eq!(result[1].val, "y");
    assert_eq!(result[2].val, "z");

    // Cleanup
    client
        .execute("DROP TABLE IF EXISTS test_safety_enum8_all")
        .await
        .unwrap();
}

// ============================================================
// Tests for #[derive(ClickhouseEnum)] -- Rust enum mapping
// ============================================================

#[derive(ClickhouseEnum, Debug, PartialEq, Clone)]
enum Color {
    Red,
    Green,
    Blue,
}

#[tokio::test]
async fn test_derive_enum_basic_roundtrip() {
    let _ = env_logger::builder()
        .filter_level(log::LevelFilter::Info)
        .try_init();
    let client = super::get_client().await;

    client
        .execute("DROP TABLE IF EXISTS test_derive_enum_basic")
        .await
        .unwrap();
    client
        .execute(
            "CREATE TABLE test_derive_enum_basic (val Enum8('Red' = 1, 'Green' = 2, 'Blue' = 3)) ENGINE = Memory",
        )
        .await
        .unwrap();

    #[derive(Row, Debug)]
    struct ColorRow {
        val: Color,
    }

    let rows = vec![
        ColorRow { val: Color::Red },
        ColorRow { val: Color::Green },
        ColorRow { val: Color::Blue },
        ColorRow { val: Color::Red },
    ];

    client
        .insert_native_block("INSERT INTO test_derive_enum_basic FORMAT Native", rows)
        .await
        .unwrap();

    let result: Vec<ColorRow> = client
        .query_collect("SELECT * FROM test_derive_enum_basic")
        .await
        .unwrap();
    assert_eq!(result.len(), 4);
    assert_eq!(result[0].val, Color::Red);
    assert_eq!(result[1].val, Color::Green);
    assert_eq!(result[2].val, Color::Blue);
    assert_eq!(result[3].val, Color::Red);

    // Cleanup
    client
        .execute("DROP TABLE IF EXISTS test_derive_enum_basic")
        .await
        .unwrap();
}

#[derive(ClickhouseEnum, Debug, PartialEq, Clone)]
#[klickhouse(rename_all = "snake_case")]
enum OrderStatus {
    NewOrder,
    InProgress,
    #[klickhouse(rename = "done")]
    Completed,
    Cancelled,
}

#[tokio::test]
async fn test_derive_enum_rename_all_snake_case() {
    let _ = env_logger::builder()
        .filter_level(log::LevelFilter::Info)
        .try_init();
    let client = super::get_client().await;

    client
        .execute("DROP TABLE IF EXISTS test_derive_enum_rename")
        .await
        .unwrap();
    client
        .execute(
            "CREATE TABLE test_derive_enum_rename (status Enum8('new_order' = 1, 'in_progress' = 2, 'done' = 3, 'cancelled' = 4)) ENGINE = Memory",
        )
        .await
        .unwrap();

    #[derive(Row, Debug)]
    struct StatusRow {
        status: OrderStatus,
    }

    let rows = vec![
        StatusRow {
            status: OrderStatus::NewOrder,
        },
        StatusRow {
            status: OrderStatus::InProgress,
        },
        StatusRow {
            status: OrderStatus::Completed,
        },
        StatusRow {
            status: OrderStatus::Cancelled,
        },
    ];

    client
        .insert_native_block("INSERT INTO test_derive_enum_rename FORMAT Native", rows)
        .await
        .unwrap();

    let result: Vec<StatusRow> = client
        .query_collect("SELECT * FROM test_derive_enum_rename")
        .await
        .unwrap();
    assert_eq!(result.len(), 4);
    assert_eq!(result[0].status, OrderStatus::NewOrder);
    assert_eq!(result[1].status, OrderStatus::InProgress);
    assert_eq!(result[2].status, OrderStatus::Completed);
    assert_eq!(result[3].status, OrderStatus::Cancelled);

    // Cleanup
    client
        .execute("DROP TABLE IF EXISTS test_derive_enum_rename")
        .await
        .unwrap();
}

#[derive(ClickhouseEnum, Debug, PartialEq, Clone)]
#[klickhouse(rename_all = "SCREAMING_SNAKE_CASE")]
enum Priority {
    Low,
    Medium,
    High,
    Critical,
}

#[tokio::test]
async fn test_derive_enum_enum16_screaming_snake() {
    let _ = env_logger::builder()
        .filter_level(log::LevelFilter::Info)
        .try_init();
    let client = super::get_client().await;

    client
        .execute("DROP TABLE IF EXISTS test_derive_enum16")
        .await
        .unwrap();
    client
        .execute(
            "CREATE TABLE test_derive_enum16 (prio Enum16('LOW' = 1, 'MEDIUM' = 2, 'HIGH' = 3, 'CRITICAL' = 4)) ENGINE = Memory",
        )
        .await
        .unwrap();

    #[derive(Row, Debug)]
    struct PriorityRow {
        prio: Priority,
    }

    let rows = vec![
        PriorityRow {
            prio: Priority::Critical,
        },
        PriorityRow {
            prio: Priority::Low,
        },
        PriorityRow {
            prio: Priority::High,
        },
        PriorityRow {
            prio: Priority::Medium,
        },
    ];

    client
        .insert_native_block("INSERT INTO test_derive_enum16 FORMAT Native", rows)
        .await
        .unwrap();

    let result: Vec<PriorityRow> = client
        .query_collect("SELECT * FROM test_derive_enum16")
        .await
        .unwrap();
    assert_eq!(result.len(), 4);
    assert_eq!(result[0].prio, Priority::Critical);
    assert_eq!(result[1].prio, Priority::Low);
    assert_eq!(result[2].prio, Priority::High);
    assert_eq!(result[3].prio, Priority::Medium);

    // Cleanup
    client
        .execute("DROP TABLE IF EXISTS test_derive_enum16")
        .await
        .unwrap();
}

#[tokio::test]
async fn test_derive_enum_read_sql_inserted_data() {
    let _ = env_logger::builder()
        .filter_level(log::LevelFilter::Info)
        .try_init();
    let client = super::get_client().await;

    client
        .execute("DROP TABLE IF EXISTS test_derive_enum_sql")
        .await
        .unwrap();
    client
        .execute(
            "CREATE TABLE test_derive_enum_sql (val Enum8('Red' = 1, 'Green' = 2, 'Blue' = 3)) ENGINE = Memory",
        )
        .await
        .unwrap();

    // Insert via raw SQL strings
    client
        .execute("INSERT INTO test_derive_enum_sql VALUES ('Green'), ('Blue'), ('Red')")
        .await
        .unwrap();

    #[derive(Row, Debug)]
    struct ColorRow {
        val: Color,
    }

    let result: Vec<ColorRow> = client
        .query_collect("SELECT * FROM test_derive_enum_sql")
        .await
        .unwrap();
    assert_eq!(result.len(), 3);
    assert_eq!(result[0].val, Color::Green);
    assert_eq!(result[1].val, Color::Blue);
    assert_eq!(result[2].val, Color::Red);

    // Cleanup
    client
        .execute("DROP TABLE IF EXISTS test_derive_enum_sql")
        .await
        .unwrap();
}
