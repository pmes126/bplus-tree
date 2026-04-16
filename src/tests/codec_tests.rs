//! Tests for codec encode/decode roundtrips, order preservation, and error handling.

use crate::codec::kv::{KeyCodec, ValueCodec};

// ---------------------------------------------------------------------------
// u64 key codec
// ---------------------------------------------------------------------------

#[test]
fn u64_key_roundtrip() {
    for val in [0u64, 1, 42, u64::MAX / 2, u64::MAX] {
        let bytes = KeyCodec::encode(&val);
        let decoded = <u64 as KeyCodec>::decode(&bytes).unwrap();
        assert_eq!(val, decoded, "roundtrip failed for {val}");
    }
}

#[test]
fn u64_key_preserves_order() {
    let values: Vec<u64> = vec![0, 1, 2, 100, 1000, u64::MAX - 1, u64::MAX];
    let encoded: Vec<Vec<u8>> = values.iter().map(KeyCodec::encode).collect();

    for i in 0..encoded.len() - 1 {
        assert!(
            encoded[i] < encoded[i + 1],
            "encoded order violated: {} (0x{}) should be < {} (0x{})",
            values[i],
            hex(&encoded[i]),
            values[i + 1],
            hex(&encoded[i + 1]),
        );
    }
}

#[test]
fn u64_key_decode_wrong_length_returns_error() {
    let short = vec![0u8; 4];
    assert!(<u64 as KeyCodec>::decode(&short).is_err());

    let long = vec![0u8; 16];
    assert!(<u64 as KeyCodec>::decode(&long).is_err());

    let empty: Vec<u8> = vec![];
    assert!(<u64 as KeyCodec>::decode(&empty).is_err());
}

// ---------------------------------------------------------------------------
// i64 key codec — sign-bit flip for lexicographic ordering
// ---------------------------------------------------------------------------

#[test]
fn i64_key_roundtrip() {
    for val in [i64::MIN, -1000, -1, 0, 1, 1000, i64::MAX] {
        let bytes = KeyCodec::encode(&val);
        let decoded = <i64 as KeyCodec>::decode(&bytes).unwrap();
        assert_eq!(val, decoded, "roundtrip failed for {val}");
    }
}

#[test]
fn i64_key_preserves_order() {
    let values: Vec<i64> = vec![i64::MIN, -1000, -1, 0, 1, 1000, i64::MAX];
    let encoded: Vec<Vec<u8>> = values.iter().map(KeyCodec::encode).collect();

    for i in 0..encoded.len() - 1 {
        assert!(
            encoded[i] < encoded[i + 1],
            "encoded order violated: {} should come before {}",
            values[i],
            values[i + 1],
        );
    }
}

#[test]
fn i64_negative_encodes_before_positive() {
    let neg = KeyCodec::encode(&-1i64);
    let pos = KeyCodec::encode(&1i64);
    assert!(neg < pos, "encoded -1 should sort before encoded 1");
}

#[test]
fn i64_key_decode_wrong_length_returns_error() {
    assert!(<i64 as KeyCodec>::decode(&[0u8; 4]).is_err());
    assert!(<i64 as KeyCodec>::decode(&[]).is_err());
}

// ---------------------------------------------------------------------------
// String key codec
// ---------------------------------------------------------------------------

#[test]
fn string_key_roundtrip() {
    for s in ["", "hello", "café", "日本語", "a\0b"] {
        let val = s.to_string();
        let bytes = KeyCodec::encode(&val);
        let decoded = <String as KeyCodec>::decode(&bytes).unwrap();
        assert_eq!(val, decoded);
    }
}

#[test]
fn string_key_preserves_lexicographic_order() {
    let values = [
        "aaa".to_string(),
        "aab".to_string(),
        "ab".to_string(),
        "b".to_string(),
        "ba".to_string(),
    ];
    let encoded: Vec<Vec<u8>> = values.iter().map(KeyCodec::encode).collect();

    for i in 0..encoded.len() - 1 {
        assert!(
            encoded[i] < encoded[i + 1],
            "encoded '{}' should come before '{}'",
            values[i],
            values[i + 1],
        );
    }
}

#[test]
fn string_decode_invalid_utf8_returns_error() {
    let invalid = vec![0xFF, 0xFE, 0xFD];
    assert!(<String as KeyCodec>::decode(&invalid).is_err());
}

// ---------------------------------------------------------------------------
// Vec<u8> key codec
// ---------------------------------------------------------------------------

#[test]
fn bytes_key_roundtrip() {
    for val in [vec![], vec![0u8], vec![1, 2, 3], vec![0xFF; 100]] {
        let bytes = <Vec<u8> as KeyCodec>::encode(&val);
        let decoded = <Vec<u8> as KeyCodec>::decode(&bytes).unwrap();
        assert_eq!(val, decoded);
    }
}

// ---------------------------------------------------------------------------
// Value codecs
// ---------------------------------------------------------------------------

#[test]
fn u64_value_roundtrip() {
    for val in [0u64, 1, 42, u64::MAX / 2, u64::MAX] {
        let bytes = <u64 as ValueCodec>::encode(&val);
        assert_eq!(bytes.len(), 8, "u64 value should encode to 8 bytes");
        let decoded = <u64 as ValueCodec>::decode(&bytes).unwrap();
        assert_eq!(val, decoded, "u64 value roundtrip failed for {val}");
    }
}

#[test]
fn string_value_roundtrip() {
    let val = "hello world".to_string();
    let bytes = <String as ValueCodec>::encode(&val);
    let decoded = <String as ValueCodec>::decode(&bytes).unwrap();
    assert_eq!(val, decoded);
}

#[test]
fn bytes_value_roundtrip() {
    let val = vec![10u8, 20, 30];
    let bytes = <Vec<u8> as ValueCodec>::encode(&val);
    let decoded = <Vec<u8> as ValueCodec>::decode(&bytes).unwrap();
    assert_eq!(val, decoded);
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect::<String>()
}
