use rand::SeedableRng;
use rand_chacha::ChaCha8Rng;
use sha2::{Digest, Sha256};

#[derive(Debug)]
pub struct Prng(ChaCha8Rng);

impl Prng {
    pub fn from_seed(seed: u64) -> Self {
        let mut h = Sha256::new();
        h.update(b"dst_framework::Prng::v1");
        h.update(seed.to_le_bytes());
        let arr: [u8; 32] = h.finalize().into();
        Self(ChaCha8Rng::from_seed(arr))
    }

    pub fn derive_stream(master_seed: u64, salt: &[u8]) -> Self {
        let mut h = Sha256::new();
        h.update(b"dst_framework::derive_stream::v1");
        h.update(master_seed.to_le_bytes());
        h.update(salt);
        let arr: [u8; 32] = h.finalize().into();
        Self(ChaCha8Rng::from_seed(arr))
    }

    pub fn inner_mut(&mut self) -> &mut ChaCha8Rng {
        &mut self.0
    }

    pub fn inner(&self) -> &ChaCha8Rng {
        &self.0
    }
}

impl rand::RngCore for Prng {
    fn next_u32(&mut self) -> u32 {
        self.0.next_u32()
    }

    fn next_u64(&mut self) -> u64 {
        self.0.next_u64()
    }

    fn fill_bytes(&mut self, dest: &mut [u8]) {
        self.0.fill_bytes(dest);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::RngCore;

    #[test]
    fn deterministic_from_seed() {
        let mut a = Prng::from_seed(7);
        let mut b = Prng::from_seed(7);
        assert_eq!(a.next_u64(), b.next_u64());
        assert_eq!(a.next_u64(), b.next_u64());
    }

    #[test]
    fn different_seeds_diverge() {
        let mut a = Prng::from_seed(1);
        let mut b = Prng::from_seed(2);
        assert_ne!(a.next_u64(), b.next_u64());
    }

    #[test]
    fn derive_stream_independent_from_parent() {
        let mut parent = Prng::from_seed(99);
        let mut child = Prng::derive_stream(99, b"child-0");
        assert_ne!(parent.next_u64(), child.next_u64());
    }

    #[test]
    fn derive_stream_different_salts_diverge() {
        let mut a = Prng::derive_stream(99, b"site-a");
        let mut b = Prng::derive_stream(99, b"site-b");
        assert_ne!(a.next_u64(), b.next_u64());
    }

    #[test]
    fn derive_stream_empty_salt_still_independent() {
        let mut parent = Prng::from_seed(7);
        let mut child = Prng::derive_stream(7, b"");
        assert_ne!(parent.next_u64(), child.next_u64());
    }
}
