use std::mem::MaybeUninit;

use compio_buf::{IoBuf, IoBufMut, SetLen};

/// An owned wrapper around a raw `*mut u8` slice that implements compio's
/// `IoBuf` / `IoBufMut` traits.
///
/// This allows passing a sub-region of the FUSE ring entry payload buffer
/// directly to `file.read_at()`, so disk cache reads land in the payload
/// without an intermediate allocation or copy.
///
/// # Safety contract
///
/// The caller must ensure that:
/// - The pointed-to memory is valid and writable for `capacity` bytes.
/// - The memory outlives the `SliceMut` (and any async I/O operation
///   that borrows it via `read_at`).
/// - No other code accesses the region while an I/O is in flight.
///
/// These invariants hold for the FUSE ring entry payload: each entry is
/// mmap'd, pinned for the lifetime of the queue, and used by exactly one
/// request at a time.
pub struct SliceMut {
    ptr: *mut u8,
    capacity: usize,
    len: usize,
}

// SAFETY: The raw pointer refers to long-lived mmap'd memory that is not
// accessed concurrently. Each SliceMut is used within a single compio
// task on a single thread.
unsafe impl Send for SliceMut {}

impl SliceMut {
    /// Create a new `SliceMut` over `buf[..capacity]`.
    ///
    /// `len` is set to 0, treating the entire buffer as uninitialized
    /// capacity available for I/O.
    ///
    /// # Safety
    ///
    /// The caller must guarantee the pointer is valid for `capacity` bytes
    /// and that the memory outlives this `SliceMut` and any I/O using it.
    pub unsafe fn new(ptr: *mut u8, capacity: usize) -> Self {
        Self {
            ptr,
            capacity,
            len: 0,
        }
    }
}

impl IoBuf for SliceMut {
    fn as_init(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.ptr, self.len) }
    }
}

impl SetLen for SliceMut {
    unsafe fn set_len(&mut self, len: usize) {
        debug_assert!(len <= self.capacity);
        self.len = len;
    }
}

impl IoBufMut for SliceMut {
    fn as_uninit(&mut self) -> &mut [MaybeUninit<u8>] {
        unsafe { std::slice::from_raw_parts_mut(self.ptr as *mut MaybeUninit<u8>, self.capacity) }
    }
}
