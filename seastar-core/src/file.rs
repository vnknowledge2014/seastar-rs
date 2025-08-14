//! File I/O operations for Seastar-RS
//!
//! Provides high-performance file operations with support for direct I/O.

use std::path::Path;
use std::os::unix::io::{RawFd, AsRawFd};
use crate::{Error, Result};

use libc::{self, c_int, c_void, off_t, size_t};

/// File open flags
#[derive(Debug, Clone, Copy)]
pub struct OpenFlags {
    pub read: bool,
    pub write: bool,
    pub create: bool,
    pub truncate: bool,
    pub direct_io: bool,
}

impl Default for OpenFlags {
    fn default() -> Self {
        Self {
            read: true,
            write: false,
            create: false,
            truncate: false,
            direct_io: true,
        }
    }
}

impl OpenFlags {
    /// Create flags for read-only access
    pub fn read_only() -> Self {
        Self {
            read: true,
            write: false,
            create: false,
            truncate: false,
            direct_io: true,
        }
    }
    
    /// Create flags for write-only access
    pub fn write_only() -> Self {
        Self {
            read: false,
            write: true,
            create: true,
            truncate: false,
            direct_io: true,
        }
    }
    
    /// Create flags for read-write access
    pub fn read_write() -> Self {
        Self {
            read: true,
            write: true,
            create: true,
            truncate: false,
            direct_io: true,
        }
    }
    
    /// Disable direct I/O (for compatibility)
    pub fn without_direct_io(mut self) -> Self {
        self.direct_io = false;
        self
    }
    
    /// Enable truncation on open
    pub fn truncate(mut self) -> Self {
        self.truncate = true;
        self
    }
    
    /// Convert to libc flags
    fn to_libc_flags(&self) -> c_int {
        let mut flags = 0;
        
        if self.read && self.write {
            flags |= libc::O_RDWR;
        } else if self.write {
            flags |= libc::O_WRONLY;
        } else {
            flags |= libc::O_RDONLY;
        }
        
        if self.create {
            flags |= libc::O_CREAT;
        }
        
        if self.truncate {
            flags |= libc::O_TRUNC;
        }
        
        if self.direct_io {
            #[cfg(target_os = "linux")]
            {
                flags |= libc::O_DIRECT;
            }
            #[cfg(not(target_os = "linux"))]
            {
                // O_DIRECT is not available on macOS/BSD, 
                // we'll use regular I/O with manual alignment
            }
        }
        
        flags |= libc::O_NONBLOCK;
        flags
    }
}

/// File handle for async operations
pub struct File {
    fd: RawFd,
    flags: OpenFlags,
}

/// File I/O statistics
#[derive(Debug, Default, Clone)]
pub struct FileStats {
    pub reads: u64,
    pub writes: u64,
    pub bytes_read: u64,
    pub bytes_written: u64,
    pub read_ops_completed: u64,
    pub write_ops_completed: u64,
}

/// Direct I/O buffer alignment requirements
const DIO_ALIGNMENT: usize = 512;

impl File {
    /// Open a file with the specified flags
    pub async fn open<P: AsRef<Path>>(path: P, flags: OpenFlags) -> Result<Self> {
        let path = path.as_ref();
        let path_cstring = std::ffi::CString::new(path.as_os_str().as_encoded_bytes())
            .map_err(|_| Error::InvalidArgument("Invalid path".to_string()))?;
        
        let libc_flags = flags.to_libc_flags();
        let mode = libc::S_IRUSR | libc::S_IWUSR | libc::S_IRGRP | libc::S_IROTH;
        
        // Open file with libc
        let fd = unsafe {
            libc::open(path_cstring.as_ptr(), libc_flags, mode as libc::c_uint)
        };
        
        if fd < 0 {
            return Err(std::io::Error::last_os_error().into());
        }
        
        Ok(Self {
            fd,
            flags,
        })
    }
    
    /// Read data from the file
    pub async fn read(&self, buffer: &mut [u8], offset: u64) -> Result<usize> {
        self.validate_buffer_for_dio(buffer)?;
        
        #[cfg(target_os = "linux")]
        {
            self.read_with_io_uring(buffer, offset).await
        }
        
        #[cfg(not(target_os = "linux"))]
        {
            self.read_with_libc(buffer, offset).await
        }
    }
    
    /// Write data to the file
    pub async fn write(&self, buffer: &[u8], offset: u64) -> Result<usize> {
        self.validate_buffer_for_dio(buffer)?;
        
        #[cfg(target_os = "linux")]
        {
            self.write_with_io_uring(buffer, offset).await
        }
        
        #[cfg(not(target_os = "linux"))]
        {
            self.write_with_libc(buffer, offset).await
        }
    }
    
    /// Flush any pending writes
    pub async fn flush(&self) -> Result<()> {
        let result = unsafe { libc::fsync(self.fd) };
        
        if result < 0 {
            Err(std::io::Error::last_os_error().into())
        } else {
            Ok(())
        }
    }
    
    /// Close the file
    pub async fn close(self) -> Result<()> {
        let result = unsafe { libc::close(self.fd) };
        
        if result < 0 {
            Err(std::io::Error::last_os_error().into())
        } else {
            Ok(())
        }
    }
    
    /// Get file size
    pub async fn size(&self) -> Result<u64> {
        let mut stat: libc::stat = unsafe { std::mem::zeroed() };
        let result = unsafe { libc::fstat(self.fd, &mut stat) };
        
        if result < 0 {
            Err(std::io::Error::last_os_error().into())
        } else {
            Ok(stat.st_size as u64)
        }
    }
    
    /// Get file descriptor
    pub fn fd(&self) -> RawFd {
        self.fd
    }
    
    /// Get open flags
    pub fn flags(&self) -> OpenFlags {
        self.flags
    }
    
    /// Check if buffer is properly aligned for direct I/O
    fn validate_buffer_for_dio(&self, buffer: &[u8]) -> Result<()> {
        if !self.flags.direct_io {
            return Ok(());
        }
        
        let buffer_addr = buffer.as_ptr() as usize;
        if buffer_addr % DIO_ALIGNMENT != 0 {
            return Err(Error::InvalidArgument(
                format!("Buffer not aligned to {} bytes for direct I/O", DIO_ALIGNMENT)
            ));
        }
        
        if buffer.len() % DIO_ALIGNMENT != 0 {
            return Err(Error::InvalidArgument(
                format!("Buffer size not aligned to {} bytes for direct I/O", DIO_ALIGNMENT)
            ));
        }
        
        Ok(())
    }
    
    #[cfg(target_os = "linux")]
    async fn read_with_io_uring(&self, buffer: &mut [u8], offset: u64) -> Result<usize> {
        // For now, fall back to libc until we implement proper io_uring integration
        tokio::task::yield_now().await;
        
        let result = unsafe {
            libc::pread(
                self.fd,
                buffer.as_mut_ptr() as *mut c_void,
                buffer.len() as size_t,
                offset as off_t,
            )
        };
        
        if result < 0 {
            Err(std::io::Error::last_os_error().into())
        } else {
            Ok(result as usize)
        }
    }
    
    #[cfg(target_os = "linux")]
    async fn write_with_io_uring(&self, buffer: &[u8], offset: u64) -> Result<usize> {
        // For now, fall back to libc until we implement proper io_uring integration
        tokio::task::yield_now().await;
        
        let result = unsafe {
            libc::pwrite(
                self.fd,
                buffer.as_ptr() as *const c_void,
                buffer.len() as size_t,
                offset as off_t,
            )
        };
        
        if result < 0 {
            Err(std::io::Error::last_os_error().into())
        } else {
            Ok(result as usize)
        }
    }
    
    #[cfg(not(target_os = "linux"))]
    async fn read_with_libc(&self, buffer: &mut [u8], offset: u64) -> Result<usize> {
        // Fallback to regular libc pread for non-Linux systems
        tokio::task::yield_now().await; // Cooperative yielding
        
        let result = unsafe {
            libc::pread(
                self.fd,
                buffer.as_mut_ptr() as *mut c_void,
                buffer.len() as size_t,
                offset as off_t,
            )
        };
        
        if result < 0 {
            Err(std::io::Error::last_os_error().into())
        } else {
            Ok(result as usize)
        }
    }
    
    #[cfg(not(target_os = "linux"))]
    async fn write_with_libc(&self, buffer: &[u8], offset: u64) -> Result<usize> {
        // Fallback to regular libc pwrite for non-Linux systems
        tokio::task::yield_now().await; // Cooperative yielding
        
        let result = unsafe {
            libc::pwrite(
                self.fd,
                buffer.as_ptr() as *const c_void,
                buffer.len() as size_t,
                offset as off_t,
            )
        };
        
        if result < 0 {
            Err(std::io::Error::last_os_error().into())
        } else {
            Ok(result as usize)
        }
    }
}

impl Drop for File {
    fn drop(&mut self) {
        unsafe {
            libc::close(self.fd);
        }
    }
}

impl AsRawFd for File {
    fn as_raw_fd(&self) -> RawFd {
        self.fd
    }
}

/// Open a file for direct I/O
pub async fn open_file_dma<P: AsRef<Path>>(path: P, flags: OpenFlags) -> Result<File> {
    File::open(path, flags).await
}

/// Aligned buffer for direct I/O operations
pub struct AlignedBuffer {
    data: Vec<u8>,
    capacity: usize,
}

impl AlignedBuffer {
    /// Create a new aligned buffer
    pub fn new(size: usize) -> Result<Self> {
        let aligned_size = (size + DIO_ALIGNMENT - 1) & !(DIO_ALIGNMENT - 1);
        
        // Allocate aligned memory using posix_memalign
        let mut ptr: *mut c_void = std::ptr::null_mut();
        let result = unsafe {
            libc::posix_memalign(&mut ptr, DIO_ALIGNMENT, aligned_size)
        };
        
        if result != 0 {
            return Err(Error::OutOfMemory);
        }
        
        // Create Vec from aligned memory
        let data = unsafe {
            Vec::from_raw_parts(ptr as *mut u8, size, aligned_size)
        };
        
        Ok(Self {
            data,
            capacity: aligned_size,
        })
    }
    
    /// Create a new aligned buffer with specific capacity
    pub fn with_capacity(capacity: usize) -> Result<Self> {
        let aligned_capacity = (capacity + DIO_ALIGNMENT - 1) & !(DIO_ALIGNMENT - 1);
        
        let mut ptr: *mut c_void = std::ptr::null_mut();
        let result = unsafe {
            libc::posix_memalign(&mut ptr, DIO_ALIGNMENT, aligned_capacity)
        };
        
        if result != 0 {
            return Err(Error::OutOfMemory);
        }
        
        let data = unsafe {
            Vec::from_raw_parts(ptr as *mut u8, 0, aligned_capacity)
        };
        
        Ok(Self {
            data,
            capacity: aligned_capacity,
        })
    }
    
    /// Get buffer as mutable slice
    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        &mut self.data
    }
    
    /// Get buffer as slice
    pub fn as_slice(&self) -> &[u8] {
        &self.data
    }
    
    /// Get buffer length
    pub fn len(&self) -> usize {
        self.data.len()
    }
    
    /// Check if buffer is empty
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }
    
    /// Get buffer capacity
    pub fn capacity(&self) -> usize {
        self.capacity
    }
    
    /// Resize buffer
    pub fn resize(&mut self, new_len: usize) {
        if new_len <= self.capacity {
            unsafe {
                self.data.set_len(new_len);
            }
        }
    }
    
    /// Clear buffer
    pub fn clear(&mut self) {
        self.data.clear();
    }
}

impl std::ops::Deref for AlignedBuffer {
    type Target = [u8];
    
    fn deref(&self) -> &Self::Target {
        &self.data
    }
}

impl std::ops::DerefMut for AlignedBuffer {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.data
    }
}

/// File operations utilities
pub struct FileUtils;

impl FileUtils {
    /// Check if a file exists
    pub async fn exists<P: AsRef<Path>>(path: P) -> bool {
        let path_cstring = match std::ffi::CString::new(path.as_ref().as_os_str().as_encoded_bytes()) {
            Ok(cstring) => cstring,
            Err(_) => return false,
        };
        
        let result = unsafe {
            libc::access(path_cstring.as_ptr(), libc::F_OK)
        };
        
        result == 0
    }
    
    /// Get file size
    pub async fn size<P: AsRef<Path>>(path: P) -> Result<u64> {
        let path_cstring = std::ffi::CString::new(path.as_ref().as_os_str().as_encoded_bytes())
            .map_err(|_| Error::InvalidArgument("Invalid path".to_string()))?;
        
        let mut stat: libc::stat = unsafe { std::mem::zeroed() };
        let result = unsafe {
            libc::stat(path_cstring.as_ptr(), &mut stat)
        };
        
        if result < 0 {
            Err(std::io::Error::last_os_error().into())
        } else {
            Ok(stat.st_size as u64)
        }
    }
    
    /// Remove a file
    pub async fn remove<P: AsRef<Path>>(path: P) -> Result<()> {
        let path_cstring = std::ffi::CString::new(path.as_ref().as_os_str().as_encoded_bytes())
            .map_err(|_| Error::InvalidArgument("Invalid path".to_string()))?;
        
        let result = unsafe {
            libc::unlink(path_cstring.as_ptr())
        };
        
        if result < 0 {
            Err(std::io::Error::last_os_error().into())
        } else {
            Ok(())
        }
    }
    
    /// Copy file contents
    pub async fn copy<P: AsRef<Path>>(from: P, to: P) -> Result<u64> {
        let source = File::open(&from, OpenFlags::read_only()).await?;
        let dest = File::open(&to, OpenFlags::write_only().truncate()).await?;
        
        let file_size = source.size().await?;
        let mut copied = 0u64;
        let buffer_size = 1024 * 1024; // 1MB buffer
        
        let mut buffer = AlignedBuffer::new(buffer_size)?;
        
        while copied < file_size {
            let to_read = std::cmp::min(buffer_size, (file_size - copied) as usize);
            buffer.resize(to_read);
            
            let bytes_read = source.read(buffer.as_mut_slice(), copied).await?;
            if bytes_read == 0 {
                break;
            }
            
            let bytes_written = dest.write(&buffer.as_slice()[..bytes_read], copied).await?;
            copied += bytes_written as u64;
        }
        
        dest.flush().await?;
        Ok(copied)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[tokio::test]
    async fn test_aligned_buffer() {
        let mut buffer = AlignedBuffer::new(1024).unwrap();
        assert_eq!(buffer.len(), 1024);
        assert!(buffer.capacity() >= 1024);
        
        // Test alignment
        let addr = buffer.as_slice().as_ptr() as usize;
        assert_eq!(addr % DIO_ALIGNMENT, 0);
    }
    
    #[tokio::test]
    async fn test_open_flags() {
        let flags = OpenFlags::read_write().truncate();
        assert!(flags.read);
        assert!(flags.write);
        assert!(flags.truncate);
        assert!(flags.direct_io);
    }
}