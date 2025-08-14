//! Memory-mapped file support for Seastar-RS
//!
//! Provides high-performance memory-mapped file operations for zero-copy I/O

use std::fs::{File, OpenOptions};
use std::io::{Write, Seek, SeekFrom};
use std::path::Path;
use std::sync::Arc;
use memmap2::{Mmap, MmapMut, MmapOptions};
use crate::{Result, Error};

/// Memory-mapped file for read-only operations
pub struct MappedFile {
    _file: File,
    mmap: Mmap,
    size: usize,
}

impl MappedFile {
    /// Open a file for memory-mapped reading
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let file = File::open(&path)
            .map_err(|e| Error::Io(format!("Failed to open file for mmap: {}", e)))?;
        
        let metadata = file.metadata()
            .map_err(|e| Error::Io(format!("Failed to get file metadata: {}", e)))?;
        
        if metadata.len() == 0 {
            return Err(Error::InvalidArgument("Cannot mmap empty file".to_string()));
        }
        
        let mmap = unsafe {
            MmapOptions::new()
                .map(&file)
                .map_err(|e| Error::Io(format!("Failed to create memory map: {}", e)))?
        };
        
        Ok(Self {
            _file: file,
            mmap,
            size: metadata.len() as usize,
        })
    }
    
    /// Get the size of the mapped file
    pub fn len(&self) -> usize {
        self.size
    }
    
    /// Check if the mapped file is empty
    pub fn is_empty(&self) -> bool {
        self.size == 0
    }
    
    /// Get a slice of the entire mapped file
    pub fn as_slice(&self) -> &[u8] {
        &self.mmap[..]
    }
    
    /// Get a slice of a portion of the mapped file
    pub fn slice(&self, start: usize, len: usize) -> Result<&[u8]> {
        if start + len > self.size {
            return Err(Error::InvalidArgument(format!(
                "Slice bounds {}..{} exceed file size {}",
                start, start + len, self.size
            )));
        }
        
        Ok(&self.mmap[start..start + len])
    }
    
    /// Read data at a specific offset
    pub fn read_at(&self, offset: usize, buf: &mut [u8]) -> Result<usize> {
        if offset >= self.size {
            return Ok(0);
        }
        
        let available = std::cmp::min(buf.len(), self.size - offset);
        let src = &self.mmap[offset..offset + available];
        buf[..available].copy_from_slice(src);
        
        Ok(available)
    }
    
    /// Find the first occurrence of a byte pattern
    pub fn find(&self, pattern: &[u8]) -> Option<usize> {
        self.mmap.windows(pattern.len()).position(|window| window == pattern)
    }
    
    /// Find all occurrences of a byte pattern
    pub fn find_all(&self, pattern: &[u8]) -> Vec<usize> {
        let mut positions = Vec::new();
        let mut start = 0;
        
        while start < self.size {
            if let Some(pos) = self.mmap[start..].windows(pattern.len())
                .position(|window| window == pattern) {
                let absolute_pos = start + pos;
                positions.push(absolute_pos);
                start = absolute_pos + 1;
            } else {
                break;
            }
        }
        
        positions
    }
    
    /// Count occurrences of a byte
    pub fn count_byte(&self, byte: u8) -> usize {
        self.mmap.iter().filter(|&&b| b == byte).count()
    }
    
    /// Create an iterator over lines (split by newline)
    pub fn lines(&self) -> LineIterator {
        LineIterator::new(&self.mmap)
    }
    
    /// Check if the file starts with the given pattern
    pub fn starts_with(&self, pattern: &[u8]) -> bool {
        self.mmap.len() >= pattern.len() && &self.mmap[..pattern.len()] == pattern
    }
    
    /// Check if the file ends with the given pattern
    pub fn ends_with(&self, pattern: &[u8]) -> bool {
        self.mmap.len() >= pattern.len() && 
        &self.mmap[self.mmap.len() - pattern.len()..] == pattern
    }
}

/// Memory-mapped file for read-write operations
pub struct MappedFileMut {
    _file: File,
    mmap: MmapMut,
    size: usize,
}

impl MappedFileMut {
    /// Open or create a file for memory-mapped reading/writing
    pub fn create<P: AsRef<Path>>(path: P, size: usize) -> Result<Self> {
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)
            .map_err(|e| Error::Io(format!("Failed to create file for mmap: {}", e)))?;
        
        // Set the file size
        file.seek(SeekFrom::Start(size as u64 - 1))
            .map_err(|e| Error::Io(format!("Failed to seek in file: {}", e)))?;
        file.write_all(&[0])
            .map_err(|e| Error::Io(format!("Failed to write to file: {}", e)))?;
        file.flush()
            .map_err(|e| Error::Io(format!("Failed to flush file: {}", e)))?;
        
        let mmap = unsafe {
            MmapOptions::new()
                .map_mut(&file)
                .map_err(|e| Error::Io(format!("Failed to create mutable memory map: {}", e)))?
        };
        
        Ok(Self {
            _file: file,
            mmap,
            size,
        })
    }
    
    /// Open an existing file for memory-mapped reading/writing
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .map_err(|e| Error::Io(format!("Failed to open file for mmap: {}", e)))?;
        
        let metadata = file.metadata()
            .map_err(|e| Error::Io(format!("Failed to get file metadata: {}", e)))?;
        
        if metadata.len() == 0 {
            return Err(Error::InvalidArgument("Cannot mmap empty file".to_string()));
        }
        
        let mmap = unsafe {
            MmapOptions::new()
                .map_mut(&file)
                .map_err(|e| Error::Io(format!("Failed to create mutable memory map: {}", e)))?
        };
        
        Ok(Self {
            _file: file,
            mmap,
            size: metadata.len() as usize,
        })
    }
    
    /// Get the size of the mapped file
    pub fn len(&self) -> usize {
        self.size
    }
    
    /// Check if the mapped file is empty
    pub fn is_empty(&self) -> bool {
        self.size == 0
    }
    
    /// Get a read-only slice of the entire mapped file
    pub fn as_slice(&self) -> &[u8] {
        &self.mmap[..]
    }
    
    /// Get a mutable slice of the entire mapped file
    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        &mut self.mmap[..]
    }
    
    /// Get a read-only slice of a portion of the mapped file
    pub fn slice(&self, start: usize, len: usize) -> Result<&[u8]> {
        if start + len > self.size {
            return Err(Error::InvalidArgument(format!(
                "Slice bounds {}..{} exceed file size {}",
                start, start + len, self.size
            )));
        }
        
        Ok(&self.mmap[start..start + len])
    }
    
    /// Get a mutable slice of a portion of the mapped file
    pub fn slice_mut(&mut self, start: usize, len: usize) -> Result<&mut [u8]> {
        if start + len > self.size {
            return Err(Error::InvalidArgument(format!(
                "Slice bounds {}..{} exceed file size {}",
                start, start + len, self.size
            )));
        }
        
        Ok(&mut self.mmap[start..start + len])
    }
    
    /// Write data at a specific offset
    pub fn write_at(&mut self, offset: usize, data: &[u8]) -> Result<usize> {
        if offset >= self.size {
            return Err(Error::InvalidArgument("Write offset exceeds file size".to_string()));
        }
        
        let available = std::cmp::min(data.len(), self.size - offset);
        let dst = &mut self.mmap[offset..offset + available];
        dst.copy_from_slice(&data[..available]);
        
        Ok(available)
    }
    
    /// Read data at a specific offset
    pub fn read_at(&self, offset: usize, buf: &mut [u8]) -> Result<usize> {
        if offset >= self.size {
            return Ok(0);
        }
        
        let available = std::cmp::min(buf.len(), self.size - offset);
        let src = &self.mmap[offset..offset + available];
        buf[..available].copy_from_slice(src);
        
        Ok(available)
    }
    
    /// Fill a range with a specific byte value
    pub fn fill(&mut self, start: usize, len: usize, byte: u8) -> Result<()> {
        if start + len > self.size {
            return Err(Error::InvalidArgument("Fill range exceeds file size".to_string()));
        }
        
        self.mmap[start..start + len].fill(byte);
        Ok(())
    }
    
    /// Copy data from one region to another within the mapped file
    pub fn copy_within(&mut self, src: std::ops::Range<usize>, dest: usize) -> Result<()> {
        if src.end > self.size || dest + (src.end - src.start) > self.size {
            return Err(Error::InvalidArgument("Copy range exceeds file size".to_string()));
        }
        
        self.mmap.copy_within(src, dest);
        Ok(())
    }
    
    /// Flush changes to disk
    pub fn flush(&self) -> Result<()> {
        self.mmap.flush()
            .map_err(|e| Error::Io(format!("Failed to flush memory map: {}", e)))
    }
    
    /// Flush a specific range to disk
    pub fn flush_range(&self, offset: usize, len: usize) -> Result<()> {
        if offset + len > self.size {
            return Err(Error::InvalidArgument("Flush range exceeds file size".to_string()));
        }
        
        self.mmap.flush_range(offset, len)
            .map_err(|e| Error::Io(format!("Failed to flush memory map range: {}", e)))
    }
}

/// Iterator over lines in a memory-mapped file
pub struct LineIterator<'a> {
    data: &'a [u8],
    position: usize,
}

impl<'a> LineIterator<'a> {
    fn new(data: &'a [u8]) -> Self {
        Self { data, position: 0 }
    }
}

impl<'a> Iterator for LineIterator<'a> {
    type Item = &'a [u8];
    
    fn next(&mut self) -> Option<Self::Item> {
        if self.position >= self.data.len() {
            return None;
        }
        
        let start = self.position;
        
        // Find next newline
        while self.position < self.data.len() && self.data[self.position] != b'\n' {
            self.position += 1;
        }
        
        let line = &self.data[start..self.position];
        
        // Skip the newline for next iteration
        if self.position < self.data.len() {
            self.position += 1;
        }
        
        Some(line)
    }
}

/// Shared memory-mapped file that can be safely shared across threads
#[derive(Clone)]
pub struct SharedMappedFile {
    inner: Arc<MappedFile>,
}

impl SharedMappedFile {
    /// Create a shared memory-mapped file from a path
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let mapped_file = MappedFile::open(path)?;
        Ok(Self {
            inner: Arc::new(mapped_file),
        })
    }
    
    /// Get the size of the mapped file
    pub fn len(&self) -> usize {
        self.inner.len()
    }
    
    /// Check if the mapped file is empty
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }
    
    /// Get a slice of the entire mapped file
    pub fn as_slice(&self) -> &[u8] {
        self.inner.as_slice()
    }
    
    /// Get a slice of a portion of the mapped file
    pub fn slice(&self, start: usize, len: usize) -> Result<&[u8]> {
        self.inner.slice(start, len)
    }
    
    /// Read data at a specific offset
    pub fn read_at(&self, offset: usize, buf: &mut [u8]) -> Result<usize> {
        self.inner.read_at(offset, buf)
    }
    
    /// Find the first occurrence of a byte pattern
    pub fn find(&self, pattern: &[u8]) -> Option<usize> {
        self.inner.find(pattern)
    }
    
    /// Create an iterator over lines
    pub fn lines(&self) -> LineIterator {
        self.inner.lines()
    }
}

/// Memory-mapped buffer for temporary data
pub struct MappedBuffer {
    mmap: MmapMut,
    size: usize,
}

impl MappedBuffer {
    /// Create an anonymous memory-mapped buffer
    pub fn new(size: usize) -> Result<Self> {
        let mmap = MmapOptions::new()
            .len(size)
            .map_anon()
            .map_err(|e| Error::Io(format!("Failed to create anonymous memory map: {}", e)))?;
        
        Ok(Self { mmap, size })
    }
    
    /// Get the size of the buffer
    pub fn len(&self) -> usize {
        self.size
    }
    
    /// Check if the buffer is empty
    pub fn is_empty(&self) -> bool {
        self.size == 0
    }
    
    /// Get a slice of the entire buffer
    pub fn as_slice(&self) -> &[u8] {
        &self.mmap[..]
    }
    
    /// Get a mutable slice of the entire buffer
    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        &mut self.mmap[..]
    }
    
    /// Fill the buffer with a specific byte value
    pub fn fill(&mut self, byte: u8) {
        self.mmap.fill(byte);
    }
    
    /// Copy data into the buffer
    pub fn copy_from_slice(&mut self, src: &[u8]) -> Result<()> {
        if src.len() > self.size {
            return Err(Error::InvalidArgument("Source data too large for buffer".to_string()));
        }
        
        self.mmap[..src.len()].copy_from_slice(src);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::{NamedTempFile, tempdir};
    
    #[test]
    fn test_mapped_file_read() {
        let mut temp_file = NamedTempFile::new().unwrap();
        let test_data = b"Hello, memory-mapped world!";
        temp_file.write_all(test_data).unwrap();
        temp_file.flush().unwrap();
        
        let mapped_file = MappedFile::open(temp_file.path()).unwrap();
        
        assert_eq!(mapped_file.len(), test_data.len());
        assert_eq!(mapped_file.as_slice(), test_data);
        assert!(mapped_file.starts_with(b"Hello"));
        assert!(mapped_file.ends_with(b"world!"));
        
        let mut buf = vec![0; 5];
        let bytes_read = mapped_file.read_at(7, &mut buf).unwrap();
        assert_eq!(bytes_read, 5);
        assert_eq!(&buf, b"memor");
    }
    
    #[test]
    fn test_mapped_file_mut() {
        let temp_dir = tempdir().unwrap();
        let file_path = temp_dir.path().join("test_mmap_mut.txt");
        
        // Create and write to mutable mapped file
        {
            let mut mapped_file = MappedFileMut::create(&file_path, 100).unwrap();
            
            assert_eq!(mapped_file.len(), 100);
            
            let test_data = b"Test data for mutable mapping";
            mapped_file.write_at(0, test_data).unwrap();
            mapped_file.flush().unwrap();
            
            // Verify the write
            let mut buf = vec![0; test_data.len()];
            mapped_file.read_at(0, &mut buf).unwrap();
            assert_eq!(&buf, test_data);
        }
        
        // Read back the file
        let mapped_file = MappedFile::open(&file_path).unwrap();
        assert_eq!(mapped_file.len(), 100);
        
        let expected = b"Test data for mutable mapping";
        assert_eq!(&mapped_file.as_slice()[..expected.len()], expected);
    }
    
    #[test]
    fn test_line_iterator() {
        let mut temp_file = NamedTempFile::new().unwrap();
        let test_data = b"line1\nline2\nline3\n";
        temp_file.write_all(test_data).unwrap();
        temp_file.flush().unwrap();
        
        let mapped_file = MappedFile::open(temp_file.path()).unwrap();
        let lines: Vec<&[u8]> = mapped_file.lines().collect();
        
        assert_eq!(lines.len(), 3);
        assert_eq!(lines[0], b"line1");
        assert_eq!(lines[1], b"line2");
        assert_eq!(lines[2], b"line3");
    }
    
    #[test]
    fn test_find_operations() {
        let mut temp_file = NamedTempFile::new().unwrap();
        let test_data = b"The quick brown fox jumps over the lazy dog. The fox is quick.";
        temp_file.write_all(test_data).unwrap();
        temp_file.flush().unwrap();
        
        let mapped_file = MappedFile::open(temp_file.path()).unwrap();
        
        assert_eq!(mapped_file.find(b"fox"), Some(16));
        
        let fox_positions = mapped_file.find_all(b"fox");
        assert_eq!(fox_positions, vec![16, 49]);
        
        let space_count = mapped_file.count_byte(b' ');
        assert_eq!(space_count, 12);
    }
    
    #[test]
    fn test_mapped_buffer() {
        let mut buffer = MappedBuffer::new(1024).unwrap();
        
        assert_eq!(buffer.len(), 1024);
        assert!(!buffer.is_empty());
        
        buffer.fill(0x42);
        assert_eq!(buffer.as_slice()[0], 0x42);
        assert_eq!(buffer.as_slice()[1023], 0x42);
        
        let test_data = b"Test data in anonymous buffer";
        buffer.copy_from_slice(test_data).unwrap();
        assert_eq!(&buffer.as_slice()[..test_data.len()], test_data);
    }
    
    #[test]
    fn test_shared_mapped_file() {
        let mut temp_file = NamedTempFile::new().unwrap();
        let test_data = b"Shared mapping test data";
        temp_file.write_all(test_data).unwrap();
        temp_file.flush().unwrap();
        
        let shared_file = SharedMappedFile::open(temp_file.path()).unwrap();
        let cloned_file = shared_file.clone();
        
        assert_eq!(shared_file.len(), cloned_file.len());
        assert_eq!(shared_file.as_slice(), cloned_file.as_slice());
        
        // Both should reference the same underlying data
        assert_eq!(shared_file.as_slice(), test_data);
        assert_eq!(cloned_file.as_slice(), test_data);
    }
    
    #[test]
    fn test_error_handling() {
        // Test opening non-existent file
        let result = MappedFile::open("/nonexistent/file.txt");
        assert!(result.is_err());
        
        // Test invalid slice bounds
        let mut temp_file = NamedTempFile::new().unwrap();
        temp_file.write_all(b"small").unwrap();
        temp_file.flush().unwrap();
        
        let mapped_file = MappedFile::open(temp_file.path()).unwrap();
        let result = mapped_file.slice(0, 100);
        assert!(result.is_err());
    }
}