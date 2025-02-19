use crate::{u256, unexpected_type, FromSql, KlickhouseError, Result, ToSql, Type, Value};
use bigdecimal::num_bigint::{BigInt, ToBigInt};
use bigdecimal::{BigDecimal, ToPrimitive};

impl FromSql for BigDecimal {
    fn from_sql(type_: &Type, value: Value) -> Result<Self> {
        // fn out_of_range(name: &str) -> KlickhouseError {
        //     KlickhouseError::DeserializeError(format!("{name} out of bounds"))
        // }

        match value {
            Value::Int8(i) => Ok(BigDecimal::from(i).with_scale(0)),
            Value::Int16(i) => Ok(BigDecimal::from(i).with_scale(0)),
            Value::Int32(i) => Ok(BigDecimal::from(i).with_scale(0)),
            Value::Int64(i) => Ok(BigDecimal::from(i).with_scale(0)),
            Value::Int128(i) => Ok(BigDecimal::from(i).with_scale(0)),

            // Value::Int256(i) => {
            //     Ok(
            //         BigDecimal::from(
            //             BigInt::from_signed_bytes_be(i.0.as_slice())
            //         ).with_scale(0)
            //     )
            // }
            
            Value::UInt8(i) => Ok(BigDecimal::from(i).with_scale(0)),
            Value::UInt16(i) => Ok(BigDecimal::from(i).with_scale(0)),
            Value::UInt32(i) => Ok(BigDecimal::from(i).with_scale(0)),
            Value::UInt64(i) => Ok(BigDecimal::from(i).with_scale(0)),
            Value::UInt128(i) => Ok(BigDecimal::from(i).with_scale(0)),

            Value::UInt256(i) => {
                // println!("UInt256 bytes len : {:?}", i.0.len());
                Ok(BigDecimal::from(BigInt::from_bytes_be(
                    bigdecimal::num_bigint::Sign::Plus,
                    i.0.as_slice(),
                ))
                .with_scale(0))
            }
            Value::Decimal32(precision, value) => {
                Ok(BigDecimal::new(value.into(), precision as i64))
            }
            Value::Decimal64(precision, value) => {
                Ok(BigDecimal::new(value.into(), precision as i64))
            }
            Value::Decimal128(precision, value) => {
                Ok(BigDecimal::new(value.into(), precision as i64))
            }
            _ => Err(unexpected_type(type_)),
        }
    }
}

impl ToSql for BigDecimal {
    fn to_sql(self, type_hint: Option<&Type>) -> Result<Value> {
        fn out_of_range(name: &str) -> KlickhouseError {
            KlickhouseError::SerializeError(format!("{name} out of bounds for rust_decimal"))
        }

        match type_hint {
            None => {
                if let Some(bi) = self.to_bigint() {
                    if BigDecimal::from(bi) != self {
                        return Err(out_of_range("Decimal128"));
                    }
                } else {
                    return Err(out_of_range("Decimal128"));
                }
                self.to_i128()
                    .map(|val| Value::Decimal128(self.fractional_digit_count() as usize, val))
                    .ok_or_else(|| out_of_range("Decimal128"))
            }
            Some(Type::Decimal32(precision))
                if *precision as i64 >= self.fractional_digit_count() =>
            {
                self.to_i32()
                    .map(|val| Value::Decimal32(*precision, val))
                    .ok_or_else(|| out_of_range("Decimal32"))
            }
            Some(Type::Decimal64(precision))
                if *precision as i64 >= self.fractional_digit_count() =>
            {
                self.to_i64()
                    .map(|val| Value::Decimal64(*precision, val))
                    .ok_or_else(|| out_of_range("Decimal64"))
            }
            Some(Type::Decimal128(precision))
                if *precision as i64 >= self.fractional_digit_count() =>
            {
                self.to_i128()
                    .map(|val| Value::Decimal128(*precision, val))
                    .ok_or_else(|| out_of_range("Decimal128"))
            }
            Some(Type::UInt8) => self
                .to_u8()
                .map(Value::UInt8)
                .ok_or_else(|| out_of_range("UInt8")),
            Some(Type::Int8) => self
                .to_i8()
                .map(Value::Int8)
                .ok_or_else(|| out_of_range("Int8")),
            Some(Type::UInt16) => self
                .to_u16()
                .map(Value::UInt16)
                .ok_or_else(|| out_of_range("UInt16")),
            Some(Type::Int16) => self
                .to_i16()
                .map(Value::Int16)
                .ok_or_else(|| out_of_range("Int16")),
            Some(Type::UInt32) => self
                .to_u32()
                .map(Value::UInt32)
                .ok_or_else(|| out_of_range("UInt32")),
            Some(Type::Int32) => self
                .to_i32()
                .map(Value::Int32)
                .ok_or_else(|| out_of_range("Int32")),
            Some(Type::UInt64) => self
                .to_u64()
                .map(Value::UInt64)
                .ok_or_else(|| out_of_range("UInt64")),
            Some(Type::Int64) => self
                .to_i64()
                .map(Value::Int64)
                .ok_or_else(|| out_of_range("Int64")),
            Some(Type::UInt128) => self
                .to_u128()
                .map(Value::UInt128)
                .ok_or_else(|| out_of_range("UInt128")),
            Some(Type::Int128) => self
                .to_i128()
                .map(Value::Int128)
                .ok_or_else(|| out_of_range("Int128")),
            Some(Type::UInt256) => {
                let val = self
                    .to_bigint()
                    .map(|val| val.to_bytes_be())
                    .map(|val| val.1)
                    .ok_or_else(|| out_of_range("UInt256"))?;

                // println!("Uint256 bytes len : {:?}", val.len());

                let val = pad_with_zeros(&val, 32).map_err(|_| out_of_range("UInt256"))?;

                Ok(Value::UInt256(u256(val)))
            }
            // Some(Type::Int256) => {
            //     let val = self
            //         .to_bigint()
            //         .map(|val| val.to_signed_bytes_be())
            //         .ok_or_else(|| out_of_range("Int256"))?;

            //     println!("Int256 bytes len : {:?}", val.len());

            //     let val = pad_with_zeros(&val, 32).map_err(|_| out_of_range("Int256"))?;

            //     Ok(Value::Int256(i256(val)))
            // }
            Some(x) => Err(KlickhouseError::SerializeError(format!(
                "unexpected type: {}",
                x
            ))),
        }
    }
}

fn pad_with_zeros(slice: &[u8], length: usize) -> Result<[u8; 32], &'static str> {
    if slice.len() > length {
        return Err("slice length is greater than the required length");
    }

    let mut array = [0u8; 32];
    let start = length - slice.len();
    array[start..].copy_from_slice(slice);
    Ok(array)
}

#[cfg(test)]
mod tests {
    use super::*;
    // use bigdecimal::{BigDecimal, FromPrimitive, ToPrimitive};
    use bigdecimal::num_bigint::ToBigInt;
    use std::str::FromStr;

    #[test]
    fn test_from_sql_integers() {
        // For Int8
        let typ = Type::Int8;
        let result = BigDecimal::from_sql(&typ, Value::Int8(42)).unwrap();
        let expected = BigDecimal::from(42);
        assert_eq!(result, expected);

        // For Int16
        let typ = Type::Int16;
        let result = BigDecimal::from_sql(&typ, Value::Int16(1234)).unwrap();
        let expected = BigDecimal::from(1234);
        assert_eq!(result, expected);

        // For UInt8
        let typ = Type::UInt8;
        let result = BigDecimal::from_sql(&typ, Value::UInt8(200)).unwrap();
        let expected = BigDecimal::from(200);
        assert_eq!(result, expected);
    }

    // Test for from_sql with Decimal* types
    #[test]
    fn test_from_sql_decimals() {
        // For Decimal32: the value 1234 with precision 2 means the number 12.34
        let typ = Type::Decimal32(2);
        let result = BigDecimal::from_sql(&typ, Value::Decimal32(2, 1234)).unwrap();
        let expected = BigDecimal::from_str("12.34").unwrap();
        assert_eq!(result, expected);

        // For Decimal64: the value 123456 with precision 3 means 123.456
        let typ = Type::Decimal64(3);
        let result = BigDecimal::from_sql(&typ, Value::Decimal64(3, 123456)).unwrap();
        let expected = BigDecimal::from_str("123.456").unwrap();
        assert_eq!(result, expected);
    }

    // Test for to_sql without type_hint (None) – uses the branch for Decimal128
    #[test]
    fn test_to_sql_none_decimal128() {
        let bd = BigDecimal::from(987654321);
        let result = bd.to_sql(None).unwrap();
        // Expect that the number without a fractional part will be converted to Decimal128 with scale = fractional_digit_count (0)
        let expected = Value::Decimal128(0, 987654321);
        assert_eq!(result, expected);
    }

    // Test for successful conversion to Decimal32
    #[test]
    fn test_to_sql_decimal32_success() {
        let bd = BigDecimal::from(1234);
        let type_hint = Type::Decimal32(2);
        let result = bd.to_sql(Some(&type_hint)).unwrap();
        let expected = Value::Decimal32(2, 1234);
        assert_eq!(result, expected);
    }

    // Test for case when the number has a fractional part, but the precision for Decimal32 is insufficient
    #[test]
    fn test_to_sql_decimal32_out_of_range() {
        // Here the number has 3 decimal places, and the precision is specified as 2 → error
        let bd = BigDecimal::from_str("123.456").unwrap();
        let err = bd.to_sql(Some(&Type::Decimal32(2)));
        assert!(err.is_err());
    }

    // Test for successful conversion to UInt8
    #[test]
    fn test_to_sql_uint8_success() {
        let bd = BigDecimal::from(255);
        let result = bd.to_sql(Some(&Type::UInt8)).unwrap();
        let expected = Value::UInt8(255);
        assert_eq!(result, expected);
    }

    // Test for case when the value is out of range for Int8
    #[test]
    fn test_to_sql_int8_out_of_range() {
        // For i8, the acceptable range is [-128, 127]. The number 200 does not fit → error.
        let bd = BigDecimal::from(200);
        let err = bd.to_sql(Some(&Type::Int8));
        assert!(err.is_err());
    }

    // Test for checking the pad_with_zeros function (successful filling with zeros to the required length)
    #[test]
    fn test_pad_with_zeros_success() {
        let slice = vec![1, 2, 3];
        let padded = pad_with_zeros(&slice, 32).unwrap();
        let mut expected = [0u8; 32];
        expected[32 - slice.len()..].copy_from_slice(&slice);
        assert_eq!(padded, expected);
    }

    // Test for pad_with_zeros – case when the length of the original slice is greater than the required
    #[test]
    fn test_pad_with_zeros_error() {
        let slice = vec![0u8; 33];
        let err = pad_with_zeros(&slice, 32);
        assert!(err.is_err());
    }

    // Test for successful conversion to UInt256.
    #[test]
    fn test_to_sql_uint256_success() {
        let bd = BigDecimal::from(123456789);
        let result = bd.clone().to_sql(Some(&Type::UInt256)).unwrap();

        // Get BigInt, convert to little-endian and fill with zeros to 32 bytes
        let bigint = bd.to_bigint().unwrap();
        let (_, bytes) = bigint.to_bytes_be();
        let padded = pad_with_zeros(&bytes, 32).unwrap();
        let expected = Value::UInt256(u256(padded));

        assert_eq!(result, expected);
    }

    // Test for to_sql with None and fractional number – should return an error
    #[test]
    fn test_to_sql_none_fractional_fail() {
        let bd = BigDecimal::from_str("123.456").unwrap();
        let err = bd.to_sql(None);
        assert!(err.is_err());
    }
}
