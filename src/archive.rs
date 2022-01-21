use crate::{Result, MAX_COMPRESSED_CHUNK_SIZE};
use async_std::fs::File;
use async_std::prelude::*;
use async_std::sync::Arc;
use async_std::task;
use minicbor_derive::{Decode, Encode};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

#[derive(Debug, Clone, Encode, Decode)]
pub struct ArchiveLocation {
    #[n(0)]
    archive_set: usize,
    #[n(1)]
    set_offset: usize,
}

impl ArchiveLocation {
    pub fn archive_set(&self) -> usize {
        self.archive_set
    }
    pub fn set_offset(&self) -> usize {
        self.set_offset
    }
}

#[derive(Clone)]
pub struct Archive {
    limit: usize,
    archive: String,
    record_type: String,
    active_tasks: Arc<AtomicUsize>,
    waiting_tasks: Arc<AtomicUsize>,
    read_buffer: Option<Arc<Vec<u8>>>,
    read_serial_number: usize,
    read_offset: usize,
    write_buffer: Vec<u8>,
    write_serial_number: usize,
}

impl std::fmt::Debug for Archive {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let wbuf_len = self.write_buffer.len();
        let rbuf_len = self.write_buffer.len();
        f.debug_struct("Archive")
            .field("wbuf.len()", &wbuf_len)
            .field("rbuf.len()", &rbuf_len)
            .field("limit", &self.limit)
            .field("archive", &self.archive)
            .field("record_type", &self.record_type)
            .field("read_serial_number", &self.read_serial_number)
            .field("read_offset", &self.read_offset)
            .field("write_serial_number", &self.write_serial_number)
            .field("active_tasks", &self.active_tasks)
            .field("waiting_tasks", &self.waiting_tasks)
            .finish()
    }
}

impl Archive {
    pub fn new(archive: &String, record_type: String, limit: usize) -> Self {
        Archive {
            write_buffer: Vec::new(),
            write_serial_number: 0,
            read_buffer: None,
            read_serial_number: 0,
            read_offset: 0,
            limit,
            archive: archive.to_string(),
            record_type,
            active_tasks: Arc::new(AtomicUsize::new(0)),
            waiting_tasks: Arc::new(AtomicUsize::new(0)),
        }
    }

    pub fn task_counts(&self) -> (usize, usize) {
        // Measure waiting tasks first to avoid missing one
        let w = self.waiting_tasks.load(Ordering::SeqCst);
        let a = self.active_tasks.load(Ordering::SeqCst);
        (a, a + w)
    }

    pub fn record_type(&self) -> &str {
        &self.record_type
    }

    pub fn set_write_serial_number(&mut self, num: usize) {
        self.write_serial_number = num;
    }

    pub fn get_read_serial_number(&mut self) -> usize {
        self.read_serial_number
    }

    pub fn write_location(&self) -> ArchiveLocation {
        // if we will overrun, bump to next set
        if self.write_buffer.len() + MAX_COMPRESSED_CHUNK_SIZE > self.limit {
            ArchiveLocation {
                archive_set: self.write_serial_number + 1,
                set_offset: 0,
            }
        } else {
            ArchiveLocation {
                archive_set: self.write_serial_number,
                set_offset: self.write_buffer.len(),
            }
        }
    }

    pub fn write(&mut self, v: &[u8]) -> Result<()> {
        // we need to move on to next set since we may have told
        // someone through self.write_lcoation() that we were going
        // to.  Also, make sure we are not exceeding it!  This is
        // easy with LZ4 compression as there is a fixed worst case size
        assert!(v.len() < MAX_COMPRESSED_CHUNK_SIZE);
        if self.write_buffer.len() + MAX_COMPRESSED_CHUNK_SIZE > self.limit {
            self.flush()?;
        }
        self.write_buffer.extend_from_slice(&v);
        Ok(())
    }

    pub fn flush(&mut self) -> Result<()> {
        if self.write_buffer.len() > 0 {
            // create name for this physical file
            let name = format!(
                "{}/{:04}_{}.cbor",
                self.archive, self.write_serial_number, self.record_type
            );

            // bump waiting count before spawn so we see it
            self.waiting_tasks.fetch_add(1, Ordering::SeqCst);
            // launch async task to do actual write
            crate::spawn_and_log_error(write_file(
                name,
                self.write_buffer.clone(),
                self.active_tasks.clone(),
                self.waiting_tasks.clone(),
            ));

            // reset buffer
            self.write_serial_number += 1;
            self.write_buffer = Vec::new();
        }
        Ok(())
    }

    /// async function to finish off the write
    ///
    ///   1. flush out the remaining data
    ///   2. wait for all subtasks invoved in write to finish
    pub async fn finish(&mut self) -> Result<()> {
        self.flush()?;

        while self.task_counts().1 > 0 {
            task::sleep(Duration::from_millis(200)).await;
        }
        Ok(())
    }

    /// Read a set of data from the archive
    pub fn read(&mut self, len: usize) -> Result<Option<&[u8]>> {
        if self.read_buffer.is_none()
            || self.read_buffer.as_ref().unwrap().len() < self.read_offset + len
        {
            if self.read_buffer.is_some() {
                self.read_serial_number += 1;
            }
            self.read_offset = 0;
            self.read_buffer = None;
            let name = format!(
                "{}/{:04}_{}.cbor",
                self.archive, self.read_serial_number, self.record_type
            );
            self.read_buffer = read_file(name)?;
        }
        if let Some(buf) = &self.read_buffer {
            let offset = self.read_offset;
            self.read_offset += len;
            if offset + len > buf.len() {
                Ok(None)
            } else {
                Ok(Some(&buf[offset..offset + len]))
            }
        } else {
            Ok(None)
        }
    }

    /// seek to a specific location to read next
    pub fn seek(&mut self, location: ArchiveLocation) -> Result<()> {
        self.read_buffer = None;
        self.read_serial_number = location.archive_set;
        self.read_offset = location.set_offset;
        Ok(())
    }
}

pub async fn write_file(
    name: String,
    v: Vec<u8>,
    active_tasks: Arc<AtomicUsize>,
    waiting_tasks: Arc<AtomicUsize>,
) -> Result<()> {
    // The increment to waiting tasks is done in caller before spawn
    // to ensure that count is correct
    while active_tasks.load(Ordering::SeqCst) >= 4 {
        task::sleep(Duration::from_millis(500)).await;
    }
    waiting_tasks.fetch_sub(1, Ordering::SeqCst);
    active_tasks.fetch_add(1, Ordering::SeqCst);
    let mut f = File::create(name).await?;
    f.write_all(&v).await?;
    // force close before decrement to ensure actually done
    drop(f);
    active_tasks.fetch_sub(1, Ordering::SeqCst);
    Ok(())
}

use std::io::Read;
pub fn read_file(name: String) -> Result<Option<Arc<Vec<u8>>>> {
    if let Ok(mut f) = std::fs::File::open(name) {
        let mut buf: Vec<u8> = Vec::new();
        f.read_to_end(&mut buf)?;
        Ok(Some(Arc::new(buf)))
    } else {
        Ok(None)
    }
}
