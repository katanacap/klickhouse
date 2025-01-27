use crate::{i256, u256, unexpected_type, FromSql, KlickhouseError, Result, ToSql, Type, Value};
use bigdecimal::num_bigint::{BigInt, ToBigInt};
use bigdecimal::{BigDecimal, ToPrimitive};

impl FromSql for BigDecimal {
    fn from_sql(type_: &Type, value: Value) -> Result<Self> {
        fn out_of_range(name: &str) -> KlickhouseError {
            KlickhouseError::DeserializeError(format!("{name} out of bounds for rust_decimal"))
        }

        match value {
            Value::Int8(i) => Ok(BigDecimal::from(i).with_scale(0)),
            Value::Int16(i) => Ok(BigDecimal::from(i).with_scale(0)),
            Value::Int32(i) => Ok(BigDecimal::from(i).with_scale(0)),
            Value::Int64(i) => Ok(BigDecimal::from(i).with_scale(0)),
            Value::Int128(i) => Ok(BigDecimal::from(i).with_scale(0)),
            Value::Int256(i) => {
                Ok(BigDecimal::from(BigInt::from_signed_bytes_le(i.0.as_slice())).with_scale(0))
            }
            Value::UInt8(i) => Ok(BigDecimal::from(i).with_scale(0)),
            Value::UInt16(i) => Ok(BigDecimal::from(i).with_scale(0)),
            Value::UInt32(i) => Ok(BigDecimal::from(i).with_scale(0)),
            Value::UInt64(i) => Ok(BigDecimal::from(i).with_scale(0)),
            Value::UInt128(i) => Ok(BigDecimal::from(i).with_scale(0)),
            Value::UInt256(i) => {
                Ok(BigDecimal::from(BigInt::from_signed_bytes_le(i.0.as_slice())).with_scale(0))
            }
            Value::Decimal32(precision, value) => {
                Ok(BigDecimal::from(value).with_scale(precision as i64))
            }
            Value::Decimal64(precision, value) => {
                Ok(BigDecimal::from(value).with_scale(precision as i64))
            }
            Value::Decimal128(precision, value) => {
                Ok(BigDecimal::from(value).with_scale(precision as i64))
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
            None => self
                .to_i128()
                .map(|val| Value::Decimal128(self.fractional_digit_count() as usize, val))
                .ok_or_else(|| out_of_range("Decimal128")),
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
                .map(|val| Value::UInt8(val))
                .ok_or_else(|| out_of_range("UInt8")),
            Some(Type::Int8) => self
                .to_i8()
                .map(|val| Value::Int8(val))
                .ok_or_else(|| out_of_range("Int8")),
            Some(Type::UInt16) => self
                .to_u16()
                .map(|val| Value::UInt16(val))
                .ok_or_else(|| out_of_range("UInt16")),
            Some(Type::Int16) => self
                .to_i16()
                .map(|val| Value::Int16(val))
                .ok_or_else(|| out_of_range("Int16")),
            Some(Type::UInt32) => self
                .to_u32()
                .map(|val| Value::UInt32(val))
                .ok_or_else(|| out_of_range("UInt32")),
            Some(Type::Int32) => self
                .to_i32()
                .map(|val| Value::Int32(val))
                .ok_or_else(|| out_of_range("Int32")),
            Some(Type::UInt64) => self
                .to_u64()
                .map(|val| Value::UInt64(val))
                .ok_or_else(|| out_of_range("UInt64")),
            Some(Type::Int64) => self
                .to_i64()
                .map(|val| Value::Int64(val))
                .ok_or_else(|| out_of_range("Int64")),
            Some(Type::UInt128) => self
                .to_u128()
                .map(|val| Value::UInt128(val))
                .ok_or_else(|| out_of_range("UInt128")),
            Some(Type::Int128) => self
                .to_i128()
                .map(|val| Value::Int128(val))
                .ok_or_else(|| out_of_range("Int128")),
            Some(Type::UInt256) => {
                let val = self
                    .to_bigint()
                    .map(|val| val.to_bytes_le())
                    .map(|val| val.1)
                    .ok_or_else(|| out_of_range("UInt256"))?;

                let val = pad_with_zeros(&val, 32).map_err(|_| out_of_range("UInt256"))?;
                Ok(Value::UInt256(u256(val)))
            }
            Some(Type::Int256) => {
                let val = self
                    .to_bigint()
                    .map(|val| val.to_bytes_le())
                    .map(|val| val.1)
                    .ok_or_else(|| out_of_range("Int256"))?;

                let val = pad_with_zeros(&val, 32).map_err(|_| out_of_range("Int256"))?;
                Ok(Value::Int256(i256(val.into())))
            }
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
