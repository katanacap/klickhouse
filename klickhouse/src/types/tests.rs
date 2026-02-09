use std::io::Cursor;

use crate::Result;
use crate::{
    i256,
    types::{DeserializerState, SerializerState},
    u256,
    values::{self, Value},
    Date, DateTime, DynDateTime64,
};
use uuid::Uuid;

use super::Type;

async fn roundtrip_values(type_: &Type, values: &[Value]) -> Result<Vec<Value>> {
    let mut output = vec![];

    let mut state = SerializerState {};
    type_.serialize_prefix(&mut output, &mut state).await?;
    type_
        .serialize_column(values.to_vec(), &mut output, &mut state)
        .await?;
    for x in &output {
        print!("{x:02X}");
    }
    println!();
    let mut input = Cursor::new(output);
    let mut state = DeserializerState {};
    type_.deserialize_prefix(&mut input, &mut state).await?;
    let deserialized = type_
        .deserialize_column(&mut input, values.len(), &mut state)
        .await?;

    Ok(deserialized)
}

#[tokio::test]
async fn roundtrip_u8() {
    let values = &[Value::UInt8(12), Value::UInt8(24), Value::UInt8(30)];
    assert_eq!(
        &values[..],
        roundtrip_values(&Type::UInt8, &values[..]).await.unwrap()
    );
}

#[tokio::test]
async fn roundtrip_u16() {
    let values = &[Value::UInt16(12), Value::UInt16(24), Value::UInt16(30000)];
    assert_eq!(
        &values[..],
        roundtrip_values(&Type::UInt16, &values[..]).await.unwrap()
    );
}

#[tokio::test]
async fn roundtrip_u32() {
    let values = &[Value::UInt32(12), Value::UInt32(24), Value::UInt32(900000)];
    assert_eq!(
        &values[..],
        roundtrip_values(&Type::UInt32, &values[..]).await.unwrap()
    );
}

#[tokio::test]
async fn roundtrip_u64() {
    let values = &[
        Value::UInt64(12),
        Value::UInt64(24),
        Value::UInt64(9000000000),
    ];
    assert_eq!(
        &values[..],
        roundtrip_values(&Type::UInt64, &values[..]).await.unwrap()
    );
}

#[tokio::test]
async fn roundtrip_u128() {
    let values = &[
        Value::UInt128(12),
        Value::UInt128(24),
        Value::UInt128(9000000000),
        Value::UInt128(9000000000u128 * 9000000000),
    ];
    assert_eq!(
        &values[..],
        roundtrip_values(&Type::UInt128, &values[..]).await.unwrap()
    );
}

#[tokio::test]
async fn roundtrip_u256() {
    let values = &[
        Value::UInt256(u256([0u8; 32])),
        Value::UInt256(u256([7u8; 32])),
    ];
    assert_eq!(
        &values[..],
        roundtrip_values(&Type::UInt256, &values[..]).await.unwrap()
    );
}

#[tokio::test]
async fn roundtrip_i8() {
    let values = &[
        Value::Int8(12),
        Value::Int8(24),
        Value::Int8(30),
        Value::Int8(-30),
    ];
    assert_eq!(
        &values[..],
        roundtrip_values(&Type::Int8, &values[..]).await.unwrap()
    );
}

#[tokio::test]
async fn roundtrip_i16() {
    let values = &[
        Value::Int16(12),
        Value::Int16(24),
        Value::Int16(30000),
        Value::Int16(-30000),
    ];
    assert_eq!(
        &values[..],
        roundtrip_values(&Type::Int16, &values[..]).await.unwrap()
    );
}

#[tokio::test]
async fn roundtrip_i32() {
    let values = &[
        Value::Int32(12),
        Value::Int32(24),
        Value::Int32(900000),
        Value::Int32(-900000),
    ];
    assert_eq!(
        &values[..],
        roundtrip_values(&Type::Int32, &values[..]).await.unwrap()
    );
}

#[tokio::test]
async fn roundtrip_i64() {
    let values = &[
        Value::Int64(12),
        Value::Int64(24),
        Value::Int64(9000000000),
        Value::Int64(-9000000000),
    ];
    assert_eq!(
        &values[..],
        roundtrip_values(&Type::Int64, &values[..]).await.unwrap()
    );
}

#[tokio::test]
async fn roundtrip_i128() {
    let values = &[
        Value::Int128(12),
        Value::Int128(24),
        Value::Int128(9000000000),
        Value::Int128(9000000000i128 * 9000000000),
        Value::Int128(-9000000000i128 * 9000000000),
    ];
    assert_eq!(
        &values[..],
        roundtrip_values(&Type::Int128, &values[..]).await.unwrap()
    );
}

#[tokio::test]
async fn roundtrip_i256() {
    let values = &[
        Value::Int256(i256([0u8; 32])),
        Value::Int256(i256([7u8; 32])),
    ];
    assert_eq!(
        &values[..],
        roundtrip_values(&Type::Int256, &values[..]).await.unwrap()
    );
}

#[tokio::test]
async fn roundtrip_f32() {
    let values = &[
        Value::Float32(1.0_f32),
        Value::Float32(0.0_f32),
        Value::Float32(100.0_f32),
        Value::Float32(100000.0_f32),
        Value::Float32(1000000.0_f32),
        Value::Float32(-1000000.0_f32),
        Value::Float32(f32::NAN),
        Value::Float32(f32::INFINITY),
        Value::Float32(f32::NEG_INFINITY),
    ];
    assert_eq!(
        &values[..],
        roundtrip_values(&Type::Float32, &values[..]).await.unwrap()
    );
}

#[tokio::test]
async fn roundtrip_f64() {
    let values = &[
        Value::Float64(1.0_f64),
        Value::Float64(0.0_f64),
        Value::Float64(100.0_f64),
        Value::Float64(100000.0_f64),
        Value::Float64(1000000.0_f64),
        Value::Float64(-1000000.0_f64),
        Value::Float64(f64::NAN),
        Value::Float64(f64::INFINITY),
        Value::Float64(f64::NEG_INFINITY),
    ];
    assert_eq!(
        &values[..],
        roundtrip_values(&Type::Float64, &values[..]).await.unwrap()
    );
}

#[tokio::test]
async fn roundtrip_d32() {
    let values = &[
        Value::Decimal32(5, 12),
        Value::Decimal32(5, 24),
        Value::Decimal32(5, 900000),
        Value::Decimal32(5, -900000),
    ];
    assert_eq!(
        &values[..],
        roundtrip_values(&Type::Decimal32(5), &values[..])
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn roundtrip_d64() {
    let values = &[
        Value::Decimal64(5, 12),
        Value::Decimal64(5, 24),
        Value::Decimal64(5, 9000000000),
        Value::Decimal64(5, -9000000000),
    ];
    assert_eq!(
        &values[..],
        roundtrip_values(&Type::Decimal64(5), &values[..])
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn roundtrip_d128() {
    let values = &[
        Value::Decimal128(5, 12),
        Value::Decimal128(5, 24),
        Value::Decimal128(5, 9000000000),
        Value::Decimal128(5, 9000000000i128 * 9000000000),
        Value::Decimal128(5, -9000000000i128 * 9000000000),
    ];
    assert_eq!(
        &values[..],
        roundtrip_values(&Type::Decimal128(5), &values[..])
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn roundtrip_d256() {
    let values = &[
        Value::Decimal256(5, i256([0u8; 32])),
        Value::Decimal256(5, i256([7u8; 32])),
    ];
    assert_eq!(
        &values[..],
        roundtrip_values(&Type::Decimal256(5), &values[..])
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn roundtrip_null_int() {
    let values = &[
        Value::UInt32(35),
        Value::UInt32(90),
        Value::Null,
        Value::UInt32(120),
        Value::UInt32(10000),
        Value::Null,
    ];
    assert_eq!(
        &values[..],
        roundtrip_values(&Type::Nullable(Box::new(Type::UInt32)), &values[..])
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn roundtrip_string() {
    let values = &[
        Value::string(""),
        Value::string("t"),
        Value::string("test"),
        Value::string("TESTST"),
        Value::string("日本語"),
    ];
    assert_eq!(
        &values[..],
        roundtrip_values(&Type::String, &values[..]).await.unwrap()
    );
    assert_eq!(
        &values[..],
        roundtrip_values(&Type::FixedString(32), &values[..])
            .await
            .unwrap()
    );
    assert_ne!(
        &values[..],
        roundtrip_values(&Type::FixedString(3), &values[..])
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn roundtrip_null_string() {
    let values = &[
        Value::string(""),
        Value::Null,
        Value::string("t"),
        Value::string("test"),
        Value::Null,
        Value::string("TESTST"),
        Value::string("日本語"),
        Value::Null,
    ];
    assert_eq!(
        &values[..],
        roundtrip_values(&Type::Nullable(Box::new(Type::String)), &values[..])
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn roundtrip_uuid() {
    let values = &[
        Value::Uuid(Uuid::from_u128(0)),
        Value::Uuid(Uuid::from_u128(1)),
        Value::Uuid(Uuid::from_u128(456345634563456)),
    ];
    assert_eq!(
        &values[..],
        roundtrip_values(&Type::Uuid, &values[..]).await.unwrap()
    );
}

#[tokio::test]
async fn roundtrip_date() {
    let values = &[
        Value::Date(Date(0)),
        Value::Date(Date(3234)),
        Value::Date(Date(45345)),
    ];
    assert_eq!(
        &values[..],
        roundtrip_values(&Type::Date, &values[..]).await.unwrap()
    );
}

#[tokio::test]
async fn roundtrip_datetime() {
    let values = &[
        Value::DateTime(DateTime(chrono_tz::UTC, 0)),
        Value::DateTime(DateTime(chrono_tz::UTC, 323463434)),
        Value::DateTime(DateTime(chrono_tz::UTC, 45345345)),
    ];
    assert_eq!(
        &values[..],
        roundtrip_values(&Type::DateTime(chrono_tz::UTC), &values[..])
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn roundtrip_datetime64() {
    let values = &[
        Value::DateTime64(DynDateTime64(chrono_tz::UTC, 0, 3)),
        Value::DateTime64(DynDateTime64(chrono_tz::UTC, 32346345634, 3)),
        Value::DateTime64(DynDateTime64(chrono_tz::UTC, 4534564345, 3)),
    ];
    assert_eq!(
        &values[..],
        roundtrip_values(&Type::DateTime64(3, chrono_tz::UTC), &values[..])
            .await
            .unwrap()
    );
}

// Enum8 and Enum16 wire-format roundtrip tests

#[tokio::test]
async fn roundtrip_enum8() {
    let type_ = Type::Enum8(vec![
        ("hello".to_string(), 1),
        ("world".to_string(), 2),
        ("foo".to_string(), -1),
    ]);
    let values = &[Value::Enum8(1), Value::Enum8(2), Value::Enum8(-1)];
    assert_eq!(
        &values[..],
        roundtrip_values(&type_, &values[..]).await.unwrap()
    );
}

#[tokio::test]
async fn roundtrip_enum16() {
    let type_ = Type::Enum16(vec![
        ("alpha".to_string(), 100),
        ("beta".to_string(), 200),
        ("gamma".to_string(), -300),
    ]);
    let values = &[Value::Enum16(100), Value::Enum16(200), Value::Enum16(-300)];
    assert_eq!(
        &values[..],
        roundtrip_values(&type_, &values[..]).await.unwrap()
    );
}

// Enum type parsing tests

#[test]
fn parse_enum8_basic() {
    let t: Type = "Enum8('hello' = 1, 'world' = 2)".parse().unwrap();
    match &t {
        Type::Enum8(entries) => {
            assert_eq!(entries.len(), 2);
            assert_eq!(entries[0], ("hello".to_string(), 1));
            assert_eq!(entries[1], ("world".to_string(), 2));
        }
        other => panic!("expected Enum8, got {other:?}"),
    }
}

#[test]
fn parse_enum16_basic() {
    let t: Type = "Enum16('foo' = 100, 'bar' = 200)".parse().unwrap();
    match &t {
        Type::Enum16(entries) => {
            assert_eq!(entries.len(), 2);
            assert_eq!(entries[0], ("foo".to_string(), 100));
            assert_eq!(entries[1], ("bar".to_string(), 200));
        }
        other => panic!("expected Enum16, got {other:?}"),
    }
}

#[test]
fn parse_enum8_negative_values() {
    let t: Type = "Enum8('a' = -128, 'b' = 127)".parse().unwrap();
    match &t {
        Type::Enum8(entries) => {
            assert_eq!(entries[0], ("a".to_string(), -128));
            assert_eq!(entries[1], ("b".to_string(), 127));
        }
        other => panic!("expected Enum8, got {other:?}"),
    }
}

#[test]
fn parse_enum8_escaped_quote() {
    let t: Type = "Enum8('it\\'s' = 1, 'normal' = 2)".parse().unwrap();
    match &t {
        Type::Enum8(entries) => {
            assert_eq!(entries[0].0, "it's");
            assert_eq!(entries[0].1, 1);
            assert_eq!(entries[1].0, "normal");
        }
        other => panic!("expected Enum8, got {other:?}"),
    }
}

#[test]
fn parse_enum8_value_out_of_range() {
    let result: std::result::Result<Type, _> = "Enum8('a' = 200)".parse();
    assert!(result.is_err());
}

#[test]
fn parse_enum16_value_out_of_range() {
    let result: std::result::Result<Type, _> = "Enum16('a' = 100000)".parse();
    assert!(result.is_err());
}

#[test]
fn parse_enum_roundtrip_display() {
    let original = "Enum8('hello' = 1, 'world' = 2)";
    let t: Type = original.parse().unwrap();
    let displayed = t.to_string();
    let reparsed: Type = displayed.parse().unwrap();
    assert_eq!(t, reparsed);
}

#[test]
fn parse_enum16_roundtrip_display() {
    let original = "Enum16('alpha' = 1000, 'beta' = 2000)";
    let t: Type = original.parse().unwrap();
    let displayed = t.to_string();
    let reparsed: Type = displayed.parse().unwrap();
    assert_eq!(t, reparsed);
}

#[test]
fn enum8_validate_value_valid() {
    let t = Type::Enum8(vec![("a".to_string(), 1), ("b".to_string(), 2)]);
    assert!(t.validate_value(&Value::Enum8(1)).is_ok());
    assert!(t.validate_value(&Value::Enum8(2)).is_ok());
}

#[test]
fn enum8_validate_value_invalid() {
    let t = Type::Enum8(vec![("a".to_string(), 1), ("b".to_string(), 2)]);
    assert!(t.validate_value(&Value::Enum8(3)).is_err());
}

#[test]
fn enum_with_comma_in_name() {
    // ClickHouse allows commas in enum names since they're quoted
    let t: Type = "Enum8('hello, world' = 1, 'foo' = 2)".parse().unwrap();
    match &t {
        Type::Enum8(entries) => {
            assert_eq!(entries[0].0, "hello, world");
            assert_eq!(entries[1].0, "foo");
        }
        other => panic!("expected Enum8, got {other:?}"),
    }
}

// nested skipped

#[tokio::test]
async fn roundtrip_array() {
    let values = &[
        Value::Array(vec![]),
        Value::Array(vec![Value::UInt32(0)]),
        Value::Array(vec![Value::UInt32(1), Value::UInt32(2), Value::UInt32(3)]),
    ];
    assert_eq!(
        &values[..],
        roundtrip_values(&Type::Array(Box::new(Type::UInt32)), &values[..])
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn roundtrip_array2() {
    let values = &[
        Value::Array(vec![Value::Array(vec![])]),
        Value::Array(vec![Value::Array(vec![Value::UInt32(1)])]),
        Value::Array(vec![
            Value::Array(vec![Value::UInt32(2)]),
            Value::Array(vec![Value::UInt32(3)]),
        ]),
        Value::Array(vec![
            Value::Array(vec![Value::UInt32(4), Value::UInt32(5)]),
            Value::Array(vec![Value::UInt32(6), Value::UInt32(7)]),
        ]),
        Value::Array(vec![Value::Array(vec![Value::UInt32(8)])]),
    ];
    assert_eq!(
        &values[..],
        roundtrip_values(
            &Type::Array(Box::new(Type::Array(Box::new(Type::UInt32)))),
            &values[..]
        )
        .await
        .unwrap()
    );
}

#[tokio::test]
async fn roundtrip_tuple() {
    let values = &[
        Value::Tuple(vec![Value::UInt32(1), Value::UInt16(2)]),
        Value::Tuple(vec![Value::UInt32(3), Value::UInt16(4)]),
        Value::Tuple(vec![Value::UInt32(4), Value::UInt16(5)]),
    ];
    assert_eq!(
        &values[..],
        roundtrip_values(&Type::Tuple(vec![Type::UInt32, Type::UInt16]), &values[..])
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn roundtrip_2tuple() {
    let values = &[
        Value::Tuple(vec![
            Value::UInt32(1),
            Value::Tuple(vec![Value::UInt32(1), Value::UInt16(2)]),
        ]),
        Value::Tuple(vec![
            Value::UInt32(3),
            Value::Tuple(vec![Value::UInt32(3), Value::UInt16(4)]),
        ]),
        Value::Tuple(vec![
            Value::UInt32(4),
            Value::Tuple(vec![Value::UInt32(4), Value::UInt16(5)]),
        ]),
    ];
    assert_eq!(
        &values[..],
        roundtrip_values(
            &Type::Tuple(vec![
                Type::UInt32,
                Type::Tuple(vec![Type::UInt32, Type::UInt16])
            ]),
            &values[..]
        )
        .await
        .unwrap()
    );
}

#[tokio::test]
async fn roundtrip_array_tuple() {
    let values = &[
        Value::Array(vec![
            Value::Tuple(vec![Value::UInt32(1), Value::UInt16(2)]),
            Value::Tuple(vec![Value::UInt32(3), Value::UInt16(4)]),
        ]),
        Value::Array(vec![Value::Tuple(vec![Value::UInt32(5), Value::UInt16(6)])]),
        Value::Array(vec![]),
    ];
    assert_eq!(
        &values[..],
        roundtrip_values(
            &Type::Array(Box::new(Type::Tuple(vec![Type::UInt32, Type::UInt16]))),
            &values[..]
        )
        .await
        .unwrap()
    );
}

#[tokio::test]
async fn roundtrip_tuple_array() {
    let values = &[
        Value::Tuple(vec![Value::Array(vec![]), Value::Array(vec![])]),
        Value::Tuple(vec![
            Value::Array(vec![Value::UInt32(1)]),
            Value::Array(vec![]),
        ]),
        Value::Tuple(vec![
            Value::Array(vec![]),
            Value::Array(vec![Value::UInt16(2)]),
        ]),
        Value::Tuple(vec![
            Value::Array(vec![Value::UInt32(3)]),
            Value::Array(vec![Value::UInt16(4)]),
        ]),
        Value::Tuple(vec![
            Value::Array(vec![Value::UInt32(5), Value::UInt32(6)]),
            Value::Array(vec![Value::UInt16(7), Value::UInt16(8)]),
        ]),
    ];
    assert_eq!(
        &values[..],
        roundtrip_values(
            &Type::Tuple(vec![
                Type::Array(Box::new(Type::UInt32)),
                Type::Array(Box::new(Type::UInt16))
            ]),
            &values[..]
        )
        .await
        .unwrap()
    );
}

#[tokio::test]
async fn roundtrip_array_nulls() {
    let values = &[
        Value::Array(vec![]),
        Value::Array(vec![Value::Null]),
        Value::Array(vec![Value::UInt32(0), Value::Null]),
        Value::Array(vec![Value::Null, Value::UInt32(0)]),
        Value::Array(vec![
            Value::Null,
            Value::Null,
            Value::UInt32(1),
            Value::UInt32(2),
            Value::Null,
            Value::UInt32(3),
        ]),
    ];
    assert_eq!(
        &values[..],
        roundtrip_values(
            &Type::Array(Box::new(Type::Nullable(Box::new(Type::UInt32)))),
            &values[..]
        )
        .await
        .unwrap()
    );
}

#[tokio::test]
async fn roundtrip_map() {
    let values = &[
        Value::Map(vec![], vec![]),
        Value::Map(vec![Value::UInt32(1)], vec![Value::UInt16(2)]),
        Value::Map(
            vec![Value::UInt32(5), Value::UInt32(3)],
            vec![Value::UInt16(6), Value::UInt16(4)],
        ),
    ];
    assert_eq!(
        &values[..],
        roundtrip_values(
            &Type::Map(Box::new(Type::UInt32), Box::new(Type::UInt16)),
            &values[..]
        )
        .await
        .unwrap()
    );
}

#[tokio::test]
async fn roundtrip_low_cardinality_string() {
    let values = &[
        Value::string(""),
        Value::string("abc"),
        Value::string("abc"),
        Value::string("bcd"),
        Value::string("bcd2"),
        Value::string("abc"),
        Value::string("abc"),
        Value::string("abc"),
        Value::string("abc"),
        Value::string("abc"),
        Value::string("abc"),
        Value::string("abc"),
        Value::string("abc"),
        Value::string("abc"),
    ];
    assert_eq!(
        &values[..],
        roundtrip_values(&Type::LowCardinality(Box::new(Type::String)), &values[..])
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn roundtrip_low_cardinality_string_array() {
    let values = &[
        Value::Array(vec![]),
        Value::Array(vec![Value::string("")]),
        Value::Array(vec![Value::string("abc")]),
        Value::Array(vec![Value::string("abc")]),
        Value::Array(vec![Value::string("bcd")]),
        Value::Array(vec![Value::string("bcd2")]),
        Value::Array(vec![Value::string("abc")]),
        Value::Array(vec![Value::string("abc")]),
        Value::Array(vec![Value::string("abc")]),
        Value::Array(vec![Value::string("abc")]),
        Value::Array(vec![Value::string("abc")]),
        Value::Array(vec![Value::string("abc")]),
        Value::Array(vec![Value::string("abc")]),
        Value::Array(vec![Value::string("abc")]),
        Value::Array(vec![Value::string("abc")]),
    ];
    assert_eq!(
        &values[..],
        roundtrip_values(
            &Type::Array(Box::new(Type::LowCardinality(Box::new(Type::String)))),
            &values[..]
        )
        .await
        .unwrap()
    );
}

#[tokio::test]
async fn roundtrip_low_cardinality_string_map() {
    let values = &[
        Value::Map(vec![Value::string("")], vec![Value::UInt32(1)]),
        Value::Map(vec![Value::string("abc")], vec![Value::UInt32(1)]),
        Value::Map(vec![Value::string("abc")], vec![Value::UInt32(1)]),
        Value::Map(vec![Value::string("bcd")], vec![Value::UInt32(1)]),
        Value::Map(vec![Value::string("bcd2")], vec![Value::UInt32(1)]),
        Value::Map(vec![Value::string("abc")], vec![Value::UInt32(1)]),
        Value::Map(vec![Value::string("abc")], vec![Value::UInt32(1)]),
        Value::Map(vec![Value::string("abc")], vec![Value::UInt32(1)]),
        Value::Map(vec![Value::string("abc")], vec![Value::UInt32(1)]),
        Value::Map(vec![Value::string("abc")], vec![Value::UInt32(1)]),
        Value::Map(vec![Value::string("abc")], vec![Value::UInt32(1)]),
        Value::Map(vec![Value::string("abc")], vec![Value::UInt32(1)]),
        Value::Map(vec![Value::string("abc")], vec![Value::UInt32(1)]),
        Value::Map(vec![Value::string("abc")], vec![Value::UInt32(1)]),
    ];
    assert_eq!(
        &values[..],
        roundtrip_values(
            &Type::Map(
                Box::new(Type::LowCardinality(Box::new(Type::String))),
                Box::new(Type::UInt32)
            ),
            &values[..]
        )
        .await
        .unwrap()
    );
}

#[tokio::test]
async fn roundtrip_low_cardinality_string_null() {
    let values = &[
        Value::string(""),
        Value::Null,
        Value::string("abc"),
        Value::string("abc"),
        Value::string("bcd"),
        Value::string("bcd2"),
        Value::Null,
        Value::string("abc"),
        Value::string("abc"),
        Value::string("abc"),
        Value::string("abc"),
        Value::Null,
        Value::string("abc"),
        Value::string("abc"),
        Value::string("abc"),
        Value::string("abc"),
        Value::Null,
        Value::string("abc"),
    ];
    assert_eq!(
        &values[..],
        roundtrip_values(
            &Type::LowCardinality(Box::new(Type::Nullable(Box::new(Type::String)))),
            &values[..]
        )
        .await
        .unwrap()
    );
}

#[tokio::test]
async fn roundtrip_low_cardinality_array_null() {
    let values = &[
        Value::Array(vec![Value::string("")]),
        Value::Array(vec![Value::Null]),
        Value::Array(vec![Value::string("abc")]),
        Value::Array(vec![Value::string("abc")]),
        Value::Array(vec![Value::string("bcd")]),
        Value::Array(vec![Value::string("bcd2")]),
        Value::Array(vec![Value::Null]),
        Value::Array(vec![Value::string("abc")]),
        Value::Array(vec![Value::string("abc")]),
        Value::Array(vec![Value::string("abc")]),
        Value::Array(vec![Value::string("abc")]),
        Value::Array(vec![Value::Null]),
        Value::Array(vec![Value::string("abc")]),
        Value::Array(vec![Value::string("abc")]),
        Value::Array(vec![Value::string("abc")]),
        Value::Array(vec![Value::string("abc")]),
        Value::Array(vec![Value::Null]),
        Value::Array(vec![Value::string("abc")]),
    ];
    assert_eq!(
        &values[..],
        roundtrip_values(
            &Type::Array(Box::new(Type::LowCardinality(Box::new(Type::Nullable(
                Box::new(Type::String)
            ))))),
            &values[..]
        )
        .await
        .unwrap()
    );
}

#[tokio::test]
async fn roundtrip_array_null() {
    let values = &[
        Value::Array(vec![Value::string("")]),
        Value::Array(vec![Value::Null]),
        Value::Array(vec![Value::string("abc")]),
        Value::Array(vec![Value::string("abc")]),
        Value::Array(vec![Value::string("bcd")]),
        Value::Array(vec![Value::string("bcd2")]),
        Value::Array(vec![Value::Null]),
        Value::Array(vec![Value::string("abc")]),
        Value::Array(vec![Value::string("abc")]),
        Value::Array(vec![Value::string("abc")]),
        Value::Array(vec![Value::string("abc")]),
        Value::Array(vec![Value::Null]),
        Value::Array(vec![Value::string("abc")]),
        Value::Array(vec![Value::string("abc")]),
        Value::Array(vec![Value::string("abc")]),
        Value::Array(vec![Value::string("abc")]),
        Value::Array(vec![Value::Null]),
        Value::Array(vec![Value::string("abc")]),
    ];
    assert_eq!(
        &values[..],
        roundtrip_values(
            &Type::Array(Box::new(Type::Nullable(Box::new(Type::String)))),
            &values[..]
        )
        .await
        .unwrap()
    );
}

#[tokio::test]
async fn roundtrip_geo() {
    // Points
    let point = |x| values::Point([x, x + 2.0]);
    let values = &[Value::Point(point(1.0)), Value::Point(point(3.0))];
    assert_eq!(
        &values[..],
        roundtrip_values(&Type::Point, &values[..]).await.unwrap()
    );
    // Ring
    let ring = |x| values::Ring(vec![point(x), point(2.0 * x)]);
    let values = &[Value::Ring(ring(1.0)), Value::Ring(ring(3.0))];
    assert_eq!(
        &values[..],
        roundtrip_values(&Type::Ring, &values[..]).await.unwrap()
    );
    // Polygon
    let polygon = |x| values::Polygon(vec![ring(x), ring(2.0 * x)]);
    let values = &[Value::Polygon(polygon(1.0)), Value::Polygon(polygon(3.0))];
    assert_eq!(
        &values[..],
        roundtrip_values(&Type::Polygon, &values[..]).await.unwrap()
    );
    // Multipolygon
    let multipolygon = |x| values::MultiPolygon(vec![polygon(x), polygon(2.0 * x)]);
    let values = &[
        Value::MultiPolygon(multipolygon(1.0)),
        Value::MultiPolygon(multipolygon(3.0)),
    ];
    assert_eq!(
        &values[..],
        roundtrip_values(&Type::MultiPolygon, &values[..])
            .await
            .unwrap()
    );
}
