use std::time::Duration;

use klickhouse::{Client, ClientOptions, Row};

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
