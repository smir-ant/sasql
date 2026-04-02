//! Binary encode/decode for PostgreSQL types.
//!
//! All decoding operates on raw byte slices (from the arena or wire buffer).
//! Encoding appends big-endian bytes to a `Vec<u8>`.
//!
//! PostgreSQL binary format is big-endian for all numeric types.

use crate::DriverError;

// --- Encode trait ---

/// Encode a Rust value into PostgreSQL binary format.
///
/// Implementations append the binary representation to `buf`. The length prefix
/// is handled by the caller (wire protocol layer), not the encoder.
///
/// # Example
///
/// ```
/// use bsql_driver::Encode;
///
/// let mut buf = Vec::new();
/// 42i32.encode_binary(&mut buf);
/// assert_eq!(buf, &[0, 0, 0, 42]);
/// ```
pub trait Encode {
    /// Append the binary-encoded value to `buf`.
    fn encode_binary(&self, buf: &mut Vec<u8>);

    /// The PostgreSQL OID for this type.
    fn type_oid(&self) -> u32;
}

// --- Encode implementations ---

impl Encode for bool {
    #[inline]
    fn encode_binary(&self, buf: &mut Vec<u8>) {
        buf.push(if *self { 1 } else { 0 });
    }

    #[inline]
    fn type_oid(&self) -> u32 {
        16 // bool
    }
}

impl Encode for i16 {
    #[inline]
    fn encode_binary(&self, buf: &mut Vec<u8>) {
        buf.extend_from_slice(&self.to_be_bytes());
    }

    #[inline]
    fn type_oid(&self) -> u32 {
        21 // int2
    }
}

impl Encode for i32 {
    #[inline]
    fn encode_binary(&self, buf: &mut Vec<u8>) {
        buf.extend_from_slice(&self.to_be_bytes());
    }

    #[inline]
    fn type_oid(&self) -> u32 {
        23 // int4
    }
}

impl Encode for i64 {
    #[inline]
    fn encode_binary(&self, buf: &mut Vec<u8>) {
        buf.extend_from_slice(&self.to_be_bytes());
    }

    #[inline]
    fn type_oid(&self) -> u32 {
        20 // int8
    }
}

impl Encode for f32 {
    #[inline]
    fn encode_binary(&self, buf: &mut Vec<u8>) {
        buf.extend_from_slice(&self.to_be_bytes());
    }

    #[inline]
    fn type_oid(&self) -> u32 {
        700 // float4
    }
}

impl Encode for f64 {
    #[inline]
    fn encode_binary(&self, buf: &mut Vec<u8>) {
        buf.extend_from_slice(&self.to_be_bytes());
    }

    #[inline]
    fn type_oid(&self) -> u32 {
        701 // float8
    }
}

impl Encode for &str {
    #[inline]
    fn encode_binary(&self, buf: &mut Vec<u8>) {
        buf.extend_from_slice(self.as_bytes());
    }

    #[inline]
    fn type_oid(&self) -> u32 {
        25 // text
    }
}

impl Encode for String {
    #[inline]
    fn encode_binary(&self, buf: &mut Vec<u8>) {
        buf.extend_from_slice(self.as_bytes());
    }

    #[inline]
    fn type_oid(&self) -> u32 {
        25 // text
    }
}

impl Encode for &[u8] {
    #[inline]
    fn encode_binary(&self, buf: &mut Vec<u8>) {
        buf.extend_from_slice(self);
    }

    #[inline]
    fn type_oid(&self) -> u32 {
        17 // bytea
    }
}

impl Encode for Vec<u8> {
    #[inline]
    fn encode_binary(&self, buf: &mut Vec<u8>) {
        buf.extend_from_slice(self);
    }

    #[inline]
    fn type_oid(&self) -> u32 {
        17 // bytea
    }
}

impl Encode for u32 {
    #[inline]
    fn encode_binary(&self, buf: &mut Vec<u8>) {
        buf.extend_from_slice(&self.to_be_bytes());
    }

    #[inline]
    fn type_oid(&self) -> u32 {
        26 // oid
    }
}

// --- Decode functions ---

/// Decode a boolean from binary format (1 byte: 0x00 = false, 0x01 = true).
///
/// # Errors
///
/// Returns `DriverError::Protocol` if the data is not exactly 1 byte.
#[inline]
pub fn decode_bool(data: &[u8]) -> Result<bool, DriverError> {
    if data.len() != 1 {
        return Err(DriverError::Protocol(format!(
            "bool: expected 1 byte, got {}",
            data.len()
        )));
    }
    Ok(data[0] != 0)
}

/// Decode a 16-bit integer from binary format (2 bytes, big-endian).
#[inline]
pub fn decode_i16(data: &[u8]) -> Result<i16, DriverError> {
    if data.len() != 2 {
        return Err(DriverError::Protocol(format!(
            "i16: expected 2 bytes, got {}",
            data.len()
        )));
    }
    Ok(i16::from_be_bytes([data[0], data[1]]))
}

/// Decode a 32-bit integer from binary format (4 bytes, big-endian).
#[inline]
pub fn decode_i32(data: &[u8]) -> Result<i32, DriverError> {
    if data.len() != 4 {
        return Err(DriverError::Protocol(format!(
            "i32: expected 4 bytes, got {}",
            data.len()
        )));
    }
    Ok(i32::from_be_bytes([data[0], data[1], data[2], data[3]]))
}

/// Decode a 64-bit integer from binary format (8 bytes, big-endian).
#[inline]
pub fn decode_i64(data: &[u8]) -> Result<i64, DriverError> {
    if data.len() != 8 {
        return Err(DriverError::Protocol(format!(
            "i64: expected 8 bytes, got {}",
            data.len()
        )));
    }
    Ok(i64::from_be_bytes([
        data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7],
    ]))
}

/// Decode a 32-bit float from binary format (4 bytes, big-endian IEEE 754).
#[inline]
pub fn decode_f32(data: &[u8]) -> Result<f32, DriverError> {
    if data.len() != 4 {
        return Err(DriverError::Protocol(format!(
            "f32: expected 4 bytes, got {}",
            data.len()
        )));
    }
    Ok(f32::from_be_bytes([data[0], data[1], data[2], data[3]]))
}

/// Decode a 64-bit float from binary format (8 bytes, big-endian IEEE 754).
#[inline]
pub fn decode_f64(data: &[u8]) -> Result<f64, DriverError> {
    if data.len() != 8 {
        return Err(DriverError::Protocol(format!(
            "f64: expected 8 bytes, got {}",
            data.len()
        )));
    }
    Ok(f64::from_be_bytes([
        data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7],
    ]))
}

/// Decode a UTF-8 string from binary format (variable length).
///
/// Returns the string slice directly — zero-copy when data lives in the arena.
#[inline]
pub fn decode_str(data: &[u8]) -> Result<&str, DriverError> {
    std::str::from_utf8(data)
        .map_err(|e| DriverError::Protocol(format!("invalid UTF-8 in text column: {e}")))
}

/// Decode raw bytes (bytea) — identity function, zero-copy.
#[inline]
pub fn decode_bytes(data: &[u8]) -> &[u8] {
    data
}

/// Decode a UUID from binary format (exactly 16 bytes).
#[inline]
pub fn decode_uuid(data: &[u8]) -> Result<[u8; 16], DriverError> {
    if data.len() != 16 {
        return Err(DriverError::Protocol(format!(
            "uuid: expected 16 bytes, got {}",
            data.len()
        )));
    }
    let mut uuid = [0u8; 16];
    uuid.copy_from_slice(data);
    Ok(uuid)
}

/// Encode a parameter value into the wire buffer with its 4-byte length prefix.
///
/// NULL values get a length of -1 with no data.
pub fn encode_param(buf: &mut Vec<u8>, param: &dyn Encode) {
    let start = buf.len();
    buf.extend_from_slice(&[0u8; 4]); // placeholder for length
    param.encode_binary(buf);
    let data_len = (buf.len() - start - 4) as i32;
    buf[start..start + 4].copy_from_slice(&data_len.to_be_bytes());
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- Encode round-trips ---

    #[test]
    fn bool_roundtrip() {
        let mut buf = Vec::new();
        true.encode_binary(&mut buf);
        assert_eq!(decode_bool(&buf).unwrap(), true);

        buf.clear();
        false.encode_binary(&mut buf);
        assert_eq!(decode_bool(&buf).unwrap(), false);
    }

    #[test]
    fn i16_roundtrip() {
        let mut buf = Vec::new();
        12345i16.encode_binary(&mut buf);
        assert_eq!(decode_i16(&buf).unwrap(), 12345);

        buf.clear();
        (-1i16).encode_binary(&mut buf);
        assert_eq!(decode_i16(&buf).unwrap(), -1);

        buf.clear();
        i16::MIN.encode_binary(&mut buf);
        assert_eq!(decode_i16(&buf).unwrap(), i16::MIN);

        buf.clear();
        i16::MAX.encode_binary(&mut buf);
        assert_eq!(decode_i16(&buf).unwrap(), i16::MAX);
    }

    #[test]
    fn i32_roundtrip() {
        let mut buf = Vec::new();
        42i32.encode_binary(&mut buf);
        assert_eq!(buf, &[0, 0, 0, 42]);
        assert_eq!(decode_i32(&buf).unwrap(), 42);

        buf.clear();
        i32::MAX.encode_binary(&mut buf);
        assert_eq!(decode_i32(&buf).unwrap(), i32::MAX);

        buf.clear();
        i32::MIN.encode_binary(&mut buf);
        assert_eq!(decode_i32(&buf).unwrap(), i32::MIN);
    }

    #[test]
    fn i64_roundtrip() {
        let mut buf = Vec::new();
        1234567890123i64.encode_binary(&mut buf);
        assert_eq!(decode_i64(&buf).unwrap(), 1234567890123);
    }

    #[test]
    fn f32_roundtrip() {
        let mut buf = Vec::new();
        3.14f32.encode_binary(&mut buf);
        let decoded = decode_f32(&buf).unwrap();
        assert!((decoded - 3.14).abs() < f32::EPSILON);
    }

    #[test]
    fn f64_roundtrip() {
        let mut buf = Vec::new();
        std::f64::consts::PI.encode_binary(&mut buf);
        let decoded = decode_f64(&buf).unwrap();
        assert!((decoded - std::f64::consts::PI).abs() < f64::EPSILON);
    }

    #[test]
    fn str_roundtrip() {
        let mut buf = Vec::new();
        "hello world".encode_binary(&mut buf);
        assert_eq!(decode_str(&buf).unwrap(), "hello world");
    }

    #[test]
    fn string_roundtrip() {
        let mut buf = Vec::new();
        let s = String::from("test string");
        s.encode_binary(&mut buf);
        assert_eq!(decode_str(&buf).unwrap(), "test string");
    }

    #[test]
    fn bytes_roundtrip() {
        let mut buf = Vec::new();
        let data: &[u8] = &[0xDE, 0xAD, 0xBE, 0xEF];
        data.encode_binary(&mut buf);
        assert_eq!(decode_bytes(&buf), data);
    }

    #[test]
    fn vec_u8_roundtrip() {
        let mut buf = Vec::new();
        let data = vec![1u8, 2, 3, 4, 5];
        data.encode_binary(&mut buf);
        assert_eq!(decode_bytes(&buf), &[1, 2, 3, 4, 5]);
    }

    #[test]
    fn u32_encode() {
        let mut buf = Vec::new();
        42u32.encode_binary(&mut buf);
        assert_eq!(buf, &[0, 0, 0, 42]);
    }

    #[test]
    fn uuid_roundtrip() {
        let uuid_bytes: [u8; 16] = [
            0x55, 0x0e, 0x84, 0x00, 0xe2, 0x9b, 0x41, 0xd4, 0xa7, 0x16, 0x44, 0x66, 0x55, 0x44,
            0x00, 0x00,
        ];
        let decoded = decode_uuid(&uuid_bytes).unwrap();
        assert_eq!(decoded, uuid_bytes);
    }

    // --- Error cases ---

    #[test]
    fn decode_bool_wrong_length() {
        assert!(decode_bool(&[]).is_err());
        assert!(decode_bool(&[0, 0]).is_err());
    }

    #[test]
    fn decode_i32_wrong_length() {
        assert!(decode_i32(&[0, 0, 0]).is_err());
        assert!(decode_i32(&[0, 0, 0, 0, 0]).is_err());
    }

    #[test]
    fn decode_i64_wrong_length() {
        assert!(decode_i64(&[0; 7]).is_err());
        assert!(decode_i64(&[0; 9]).is_err());
    }

    #[test]
    fn decode_f32_wrong_length() {
        assert!(decode_f32(&[0; 3]).is_err());
    }

    #[test]
    fn decode_f64_wrong_length() {
        assert!(decode_f64(&[0; 7]).is_err());
    }

    #[test]
    fn decode_str_invalid_utf8() {
        assert!(decode_str(&[0xFF, 0xFE]).is_err());
    }

    #[test]
    fn decode_uuid_wrong_length() {
        assert!(decode_uuid(&[0; 15]).is_err());
        assert!(decode_uuid(&[0; 17]).is_err());
    }

    #[test]
    fn empty_str_decode() {
        assert_eq!(decode_str(&[]).unwrap(), "");
    }

    #[test]
    fn empty_bytes_decode() {
        assert_eq!(decode_bytes(&[]).len(), 0);
    }

    // --- Type OIDs ---

    #[test]
    fn type_oids_correct() {
        assert_eq!(true.type_oid(), 16);
        assert_eq!(0i16.type_oid(), 21);
        assert_eq!(0i32.type_oid(), 23);
        assert_eq!(0i64.type_oid(), 20);
        assert_eq!(0f32.type_oid(), 700);
        assert_eq!(0f64.type_oid(), 701);
        assert_eq!("".type_oid(), 25);
        assert_eq!(String::new().type_oid(), 25);
        let b: &[u8] = &[];
        assert_eq!(b.type_oid(), 17);
        assert_eq!(Vec::<u8>::new().type_oid(), 17);
        assert_eq!(0u32.type_oid(), 26);
    }

    // --- Encode param with length prefix ---

    #[test]
    fn encode_param_i32() {
        let mut buf = Vec::new();
        encode_param(&mut buf, &42i32);
        // 4 bytes length (=4) + 4 bytes data
        assert_eq!(buf.len(), 8);
        let len = i32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]);
        assert_eq!(len, 4);
        let val = i32::from_be_bytes([buf[4], buf[5], buf[6], buf[7]]);
        assert_eq!(val, 42);
    }

    #[test]
    fn encode_param_str() {
        let mut buf = Vec::new();
        encode_param(&mut buf, &"hello");
        // 4 bytes length (=5) + 5 bytes data
        assert_eq!(buf.len(), 9);
        let len = i32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]);
        assert_eq!(len, 5);
        assert_eq!(&buf[4..], b"hello");
    }
}
