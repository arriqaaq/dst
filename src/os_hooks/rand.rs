
use std::fs::File;
use std::io::{self, Read};

use rand::{RngCore, SeedableRng, rngs::StdRng};
use spin::Mutex;

static RNG_CELL: Mutex<Option<StdRng>> = Mutex::new(None);

pub fn set_os_rng(rng: StdRng) {
    *RNG_CELL.lock() = Some(rng);
}

pub fn set_os_rng_from_seed(seed: u64) {
    set_os_rng(StdRng::seed_from_u64(seed));
}

fn fill_from_rng(dest: &mut [u8]) -> bool {
    let mut guard = RNG_CELL.lock();
    if let Some(ref mut rng) = *guard {
        rng.fill_bytes(dest);
        true
    } else {
        false
    }
}

fn fill_with_dev_urandom(dest: &mut [u8]) -> io::Result<()> {
    let mut file = File::open("/dev/urandom")?;
    file.read_exact(dest)?;
    Ok(())
}

#[unsafe(no_mangle)]
#[inline(never)]
unsafe extern "C" fn getrandom(buf: *mut u8, buflen: usize, _flags: u32) -> isize {
    if !buf.is_null() && buflen > 0 {
        let dest = unsafe { std::slice::from_raw_parts_mut(buf, buflen) };
        if !fill_from_rng(dest) && fill_with_dev_urandom(dest).is_err() {
            return -1;
        }
        buflen as isize
    } else {
        -1
    }
}

#[unsafe(no_mangle)]
#[cfg(target_os = "macos")]
#[inline(never)]
unsafe extern "C" fn CCRandomGenerateBytes(buf: *mut u8, buflen: usize) -> i32 {
    if unsafe { getrandom(buf, buflen, 0) } as i32 != -1 {
        0
    } else {
        -1
    }
}

#[unsafe(no_mangle)]
#[inline(never)]
unsafe extern "C" fn getentropy(buf: *mut u8, buflen: usize) -> i32 {
    if buflen > 256 {
        return -1;
    }
    match unsafe { getrandom(buf, buflen, 0) } {
        -1 => -1,
        _ => 0,
    }
}

#[cfg(test)]
#[test]
fn hooked_bytes_match_std_rng_stream() {
    use rand::{RngCore, SeedableRng};

    set_os_rng_from_seed(7);
    let mut got = [0u8; 32];
    assert!(fill_from_rng(&mut got));

    let mut expected = StdRng::seed_from_u64(7);
    let mut want = [0u8; 32];
    expected.fill_bytes(&mut want);

    assert_eq!(got, want);
}
