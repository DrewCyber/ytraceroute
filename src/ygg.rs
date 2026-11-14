//! Yggdrasil identity primitives: keys, coordinate paths and IPv6 address derivation.
//!
//! Ported from yggdrasil-go `src/address/address.go` (v0.5.x).

use std::fmt;
use std::net::Ipv6Addr;

/// ed25519 public key of a Yggdrasil node.
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Key(pub [u8; 32]);

impl serde::Serialize for Key {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(&self.hex())
    }
}

impl<'de> serde::Deserialize<'de> for Key {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let s = String::deserialize(d)?;
        Key::from_hex(&s).map_err(serde::de::Error::custom)
    }
}

impl Key {
    pub fn from_hex(s: &str) -> anyhow::Result<Self> {
        let bytes = hex::decode(s.trim())?;
        if bytes.len() != 32 {
            anyhow::bail!("invalid public key length {} (want 32 bytes)", bytes.len());
        }
        let mut k = [0u8; 32];
        k.copy_from_slice(&bytes);
        Ok(Key(k))
    }

    pub fn hex(&self) -> String {
        hex::encode(self.0)
    }

    /// Short human-friendly form: first 4 bytes.
    pub fn short(&self) -> String {
        hex::encode(&self.0[..4])
    }
}

impl fmt::Display for Key {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.hex())
    }
}

impl fmt::Debug for Key {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Key({})", self.short())
    }
}

/// Tree coordinates: switch-port path from the root down to a node.
/// Not zero-terminated in memory (wire format adds the terminator).
pub type Coords = Vec<u64>;

/// Compute the node IPv6 address for a key (AddrForKey in address.go).
///
/// 0x02 prefix, then the count of leading 1-bits in the bitwise-inverse of
/// the key, then the remaining inverted bits, MSB first, truncated to 128 bits.
pub fn addr_for_key(key: &Key) -> Ipv6Addr {
    let mut buf = key.0;
    for b in buf.iter_mut() {
        *b = !*b;
    }
    let mut addr = [0u8; 16];
    addr[0] = 0x02;
    let mut ones: u8 = 0;
    let mut done = false;
    let mut bits: u8 = 0;
    let mut nbits = 0usize;
    let mut temp: Vec<u8> = Vec::with_capacity(32);
    for idx in 0..8 * buf.len() {
        let bit = (buf[idx / 8] & (0x80 >> (idx % 8))) >> (7 - (idx % 8));
        if !done {
            if bit != 0 {
                ones += 1;
                continue;
            }
            done = true;
            continue; // skip the first 0 bit as well
        }
        bits = (bits << 1) | bit;
        nbits += 1;
        if nbits == 8 {
            nbits = 0;
            temp.push(bits);
        }
    }
    addr[1] = ones;
    let n = addr.len() - 2; // 14 bytes fit
    addr[2..].copy_from_slice(&temp[..n]);
    Ipv6Addr::from(addr)
}

/// Compute the routed /64 subnet for a key (SubnetForKey in address.go):
/// like the node address but with the last bit of the prefix byte set and
/// truncated to 64 bits.
pub fn subnet_for_key(key: &Key) -> Ipv6Addr {
    let a = addr_for_key(key).octets();
    let mut snet = [0u8; 16];
    snet[..8].copy_from_slice(&a[..8]);
    snet[0] |= 0x01;
    Ipv6Addr::from(snet)
}

/// Length of the common prefix of two coordinate paths.
pub fn common_prefix_len(a: &[u64], b: &[u64]) -> usize {
    let n = a.len().min(b.len());
    let mut i = 0;
    while i < n && a[i] == b[i] {
        i += 1;
    }
    i
}

/// Treespace distance between two coordinate paths:
/// `len(a) + len(b) - 2*common_prefix` (ironwood `_getDist`).
/// Assumes a single root (coordinates from different roots are not
/// comparable; the real implementation returns infinity then).
pub fn tree_dist(a: &[u64], b: &[u64]) -> u64 {
    (a.len() + b.len()) as u64 - 2 * common_prefix_len(a, b) as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    // Live vectors captured from the test VM (yggdrasil-go v0.5.15-era node).
    #[test]
    fn addr_vectors_from_vm() {
        let cases = [
            (
                "7c2be3eef1478433576efeed1d25a61178ea86a4e46d73e2cfdf7c702e616656",
                "201:f50:7044:3ae1:ef32:a244:44b:8b69",
                "301:f50:7044:3ae1::",
            ),
            (
                "6fb340827e09286da895ac4e943288c49ee3e80d4a06e94c7af3d36a9d34324b",
                "201:4132:fdf6:7db:5e49:5da9:4ec5:af35",
                "301:4132:fdf6:7db::",
            ),
            (
                "00000000000193f3eeb75460f85b1ba9b6f1718626aed6f8ef373ff78d5eb75c",
                "22f:6c0c:1148:ab9f:7a4:e456:490e:8e79",
                "32f:6c0c:1148:ab9f::",
            ),
        ];
        for (k, addr, snet) in cases {
            let key = Key::from_hex(k).unwrap();
            assert_eq!(addr_for_key(&key).to_string(), addr, "addr for {k}");
            assert_eq!(subnet_for_key(&key).to_string(), snet, "subnet for {k}");
        }
    }

    #[test]
    fn dist_basics() {
        assert_eq!(tree_dist(&[], &[]), 0);
        assert_eq!(tree_dist(&[1, 2], &[]), 2);
        assert_eq!(tree_dist(&[1, 2], &[1, 2]), 0);
        assert_eq!(tree_dist(&[1, 2], &[1, 3, 4]), 3);
        assert_eq!(tree_dist(&[1, 2, 3], &[1, 2]), 1);
        assert_eq!(tree_dist(&[5], &[1, 2, 3]), 4);
    }
}
