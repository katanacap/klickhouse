use std::future::Future;

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use crate::{KlickhouseError, Result};

use crate::protocol::MAX_STRING_SIZE;

pub trait ClickhouseRead: AsyncRead + Unpin + Send + Sync {
    fn read_var_uint(&mut self) -> impl Future<Output = Result<u64>> + Send;

    fn read_string(&mut self) -> impl Future<Output = Result<Vec<u8>>> + Send;

    fn read_utf8_string(&mut self) -> impl Future<Output = Result<String>> + Send {
        async { Ok(String::from_utf8(self.read_string().await?)?) }
    }
}

impl<T: AsyncRead + Unpin + Send + Sync> ClickhouseRead for T {
    async fn read_var_uint(&mut self) -> Result<u64> {
        let mut out = 0u64;
        for i in 0..9u64 {
            let mut octet = [0u8];
            self.read_exact(&mut octet[..]).await?;
            out |= ((octet[0] & 0x7F) as u64) << (7 * i);
            if (octet[0] & 0x80) == 0 {
                break;
            }
        }
        Ok(out)
    }

    async fn read_string(&mut self) -> Result<Vec<u8>> {
        let len = self.read_var_uint().await?;
        if len as usize > MAX_STRING_SIZE {
            return Err(KlickhouseError::ProtocolError(format!(
                "string too large: {} > {}",
                len, MAX_STRING_SIZE
            )));
        }
        if len == 0 {
            return Ok(vec![]);
        }
        let mut buf = vec![0u8; len as usize];
        self.read_exact(&mut buf).await?;
        Ok(buf)
    }
}

pub trait ClickhouseWrite: AsyncWrite + Unpin + Send + Sync + 'static {
    fn write_var_uint(&mut self, value: u64) -> impl Future<Output = Result<()>> + Send;

    fn write_string(
        &mut self,
        value: impl AsRef<[u8]> + Send,
    ) -> impl Future<Output = Result<()>> + Send;
}

impl<T: AsyncWrite + Unpin + Send + Sync + 'static> ClickhouseWrite for T {
    async fn write_var_uint(&mut self, mut value: u64) -> Result<()> {
        for _ in 0..9u64 {
            let mut byte = value & 0x7F;
            if value > 0x7F {
                byte |= 0x80;
            }
            self.write_all(&[byte as u8]).await?;
            value >>= 7;
            if value == 0 {
                break;
            }
        }
        Ok(())
    }

    async fn write_string(&mut self, value: impl AsRef<[u8]> + Send) -> Result<()> {
        let value = value.as_ref();
        self.write_var_uint(value.len() as u64).await?;
        self.write_all(value).await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    async fn read_var_uint_from(data: &[u8]) -> Result<u64> {
        let mut cursor = Cursor::new(data.to_vec());
        <Cursor<Vec<u8>> as ClickhouseRead>::read_var_uint(&mut cursor).await
    }

    async fn read_string_from(data: &[u8]) -> Result<Vec<u8>> {
        let mut cursor = Cursor::new(data.to_vec());
        <Cursor<Vec<u8>> as ClickhouseRead>::read_string(&mut cursor).await
    }

    async fn write_var_uint_to(value: u64) -> Vec<u8> {
        let mut buf: Vec<u8> = vec![];
        <Vec<u8> as ClickhouseWrite>::write_var_uint(&mut buf, value)
            .await
            .unwrap();
        buf
    }

    async fn write_string_to(value: &[u8]) -> Vec<u8> {
        let mut buf: Vec<u8> = vec![];
        <Vec<u8> as ClickhouseWrite>::write_string(&mut buf, value)
            .await
            .unwrap();
        buf
    }

    #[tokio::test]
    async fn test_read_string_empty() {
        // var_uint 0 means empty string
        let result = read_string_from(&[0u8]).await.unwrap();
        assert!(result.is_empty());
    }

    #[tokio::test]
    async fn test_read_string_normal() {
        let payload = b"hello";
        let mut buf = vec![payload.len() as u8]; // var_uint for small values is just the byte
        buf.extend_from_slice(payload);
        let result = read_string_from(&buf).await.unwrap();
        assert_eq!(result, b"hello");
    }

    #[tokio::test]
    async fn test_read_string_too_large() {
        // Encode a var_uint that exceeds MAX_STRING_SIZE
        let mut buf: Vec<u8> = vec![];
        let mut value: u64 = MAX_STRING_SIZE as u64 + 1;
        for _ in 0..9 {
            let mut byte = value & 0x7F;
            if value > 0x7F {
                byte |= 0x80;
            }
            buf.push(byte as u8);
            value >>= 7;
            if value == 0 {
                break;
            }
        }
        let result = read_string_from(&buf).await;
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            KlickhouseError::ProtocolError(_)
        ));
    }

    #[tokio::test]
    async fn test_var_uint_roundtrip() {
        let test_values: Vec<u64> = vec![0, 1, 127, 128, 255, 256, 16383, 16384, u64::MAX >> 1];
        for &value in &test_values {
            let encoded = write_var_uint_to(value).await;
            let read_back = read_var_uint_from(&encoded).await.unwrap();
            assert_eq!(value, read_back, "var_uint roundtrip failed for {value}");
        }
    }

    #[tokio::test]
    async fn test_write_string_roundtrip() {
        let test_strings: Vec<&[u8]> = vec![b"", b"a", b"hello world", &[0xFF; 300]];
        for original in &test_strings {
            let encoded = write_string_to(original).await;
            let read_back = read_string_from(&encoded).await.unwrap();
            assert_eq!(*original, &read_back[..]);
        }
    }
}
