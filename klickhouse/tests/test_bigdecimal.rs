#![cfg(feature = "bigdecimal")]
use bigdecimal::{BigDecimal, Num};

#[derive(klickhouse::Row, Debug, Default, PartialEq, Clone)]
pub struct TestType {
    d_u8: BigDecimal,
    d_u16: BigDecimal,
    d_u32: BigDecimal,
    d_u64: BigDecimal,
    d_u128: BigDecimal,
    d_u256: BigDecimal,
}

#[tokio::test]
async fn test_client() {
    let _ = env_logger::builder()
        .filter_level(log::LevelFilter::Info)
        .try_init();
    let client = super::get_client().await;

    super::prepare_table(
        "test_bigdecimal",
        r"
        d_u8 UInt8 default 0,
        d_u16 UInt16 default 0,
        d_u32 UInt32 default 0,
        d_u64 UInt64 default 0,
        d_u128 UInt128 default 0,
        d_u256 UInt256 default 0,
    ",
        &client,
    )
    .await;

    let block = TestType {
        d_u8: BigDecimal::from(255u8),
        d_u16: BigDecimal::from(65_535u16),
        d_u32: BigDecimal::from(4_294_967_295u32),
        d_u64: BigDecimal::from(18_446_744_073_709_551_615u64),
        d_u128: BigDecimal::from(340_282_366_920_938_463_463_374_607_431_768_211_455u128),
        d_u256: BigDecimal::from_str_radix(
            "57896044618658097711785492504343953926634992332820282019728792003956564819966",
            10,
        )
        .expect("cant parse u256"),
    };

    client
        .insert_native_block(
            "INSERT INTO test_bigdecimal (d_u8, d_u16, d_u32, d_u64, d_u128, d_u256) FORMAT Native",
            vec![block.clone()],
        )
        .await
        .unwrap();

    let block2 = client
        .query_one::<TestType>("SELECT * from test_bigdecimal")
        .await
        .unwrap();

    assert_eq!(block.d_u8, block2.d_u8);
    assert_eq!(block.d_u16, block2.d_u16);
    assert_eq!(block.d_u32, block2.d_u32);
    assert_eq!(block.d_u64, block2.d_u64);
    assert_eq!(block.d_u128, block2.d_u128);
    assert_eq!(block.d_u256, block2.d_u256);
}
