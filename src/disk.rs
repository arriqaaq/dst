use std::time::Duration;

use crate::error::Error;
use crate::prng::Prng;

use rand::Rng;

pub trait Disk {
    fn append(&mut self, data: &[u8]) -> Result<u64, Error>;
    fn sync(&mut self) -> Result<(), Error>;
    fn read(&self, offset: u64, len: usize) -> Result<Vec<u8>, Error>;
    fn available_bytes(&self) -> u64;
}

#[derive(Debug)]
pub struct MemDisk {
    data: Vec<u8>,
    synced_len: usize,
    capacity: u64,
    fault_probability: f64,
    delay_range: (Duration, Duration),
    rng: Prng,
}

impl MemDisk {
    pub fn new(capacity: u64, seed: u64) -> Self {
        Self {
            data: Vec::new(),
            synced_len: 0,
            capacity,
            fault_probability: 0.0,
            delay_range: (Duration::ZERO, Duration::ZERO),
            rng: Prng::from_seed(seed),
        }
    }

    pub fn with_fault_probability(mut self, p: f64) -> Self {
        self.fault_probability = p;
        self
    }

    pub fn with_delay_range(mut self, min: Duration, max: Duration) -> Self {
        self.delay_range = (min, max);
        self
    }

    fn should_fault(&mut self) -> bool {
        if self.fault_probability <= 0.0 {
            return false;
        }
        self.rng.inner_mut().random_bool(self.fault_probability)
    }

    pub fn crash(&mut self) {
        self.data.truncate(self.synced_len);
    }

    pub fn synced_len(&self) -> usize {
        self.synced_len
    }

    pub fn total_len(&self) -> usize {
        self.data.len()
    }
}

impl Disk for MemDisk {
    fn append(&mut self, data: &[u8]) -> Result<u64, Error> {
        if self.should_fault() {
            return Err(Error::Io("simulated disk write fault".into()));
        }
        let remaining = self.capacity as usize - self.data.len();
        if data.len() > remaining {
            return Err(Error::Io("disk full".into()));
        }
        let offset = self.data.len() as u64;
        self.data.extend_from_slice(data);
        Ok(offset)
    }

    fn sync(&mut self) -> Result<(), Error> {
        if self.should_fault() {
            return Err(Error::Io("simulated disk sync fault".into()));
        }
        self.synced_len = self.data.len();
        Ok(())
    }

    fn read(&self, offset: u64, len: usize) -> Result<Vec<u8>, Error> {
        let start = offset as usize;
        let end = start + len;
        if end > self.data.len() {
            return Err(Error::Io("read past end of disk".into()));
        }
        Ok(self.data[start..end].to_vec())
    }

    fn available_bytes(&self) -> u64 {
        self.capacity - self.data.len() as u64
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn basic_append_and_read() {
        let mut disk = MemDisk::new(1024, 7);
        let offset = disk.append(b"hello").unwrap();
        assert_eq!(offset, 0);
        let data = disk.read(0, 5).unwrap();
        assert_eq!(data, b"hello");
    }

    #[test]
    fn crash_discards_unsynced() {
        let mut disk = MemDisk::new(1024, 7);
        disk.append(b"synced").unwrap();
        disk.sync().unwrap();
        disk.append(b"unsynced").unwrap();
        assert_eq!(disk.total_len(), 14);
        disk.crash();
        assert_eq!(disk.total_len(), 6);
    }

    #[test]
    fn disk_full() {
        let mut disk = MemDisk::new(5, 7);
        disk.append(b"12345").unwrap();
        assert!(disk.append(b"x").is_err());
    }

    #[test]
    fn disk_fault_deterministic_same_seed() {
        fn fault_trace(seed: u64) -> Vec<bool> {
            let mut disk = MemDisk::new(1 << 20, seed).with_fault_probability(0.3);
            (0u32..200)
                .map(|i| disk.append(&i.to_le_bytes()).is_err())
                .collect()
        }
        let a = fault_trace(123);
        let b = fault_trace(123);
        assert_eq!(a, b, "disk fault draw is not seed-deterministic");
        assert!(a.iter().any(|&x| x));
        assert!(a.iter().any(|&x| !x));
    }
}
