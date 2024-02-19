use crate::archive::{Archive, ArchiveLocation};
use crate::Result;
use lz4::block::{compress, decompress};
use minicbor_derive::{Decode, Encode};
use std::io::{Error, ErrorKind};
use std::marker::PhantomData;

#[derive(Clone)]
struct ReadBuf {
    data: Option<Vec<u8>>,
    pos: usize,
}

impl ReadBuf {
    fn new() -> Self {
        ReadBuf { data: None, pos: 0 }
    }

    fn new_with_data(data: &[u8]) -> Self {
        ReadBuf {
            data: Some(data.to_vec()),
            pos: 0,
        }
    }

    fn extend_from_slice(&mut self, data: &[u8]) {
        match &mut self.data {
            Some(current_data) => current_data.extend_from_slice(data),
            None => panic!("only call with data"),
        }
    }

    fn is_none(&self) -> bool {
        self.data.is_none()
    }

    #[allow(dead_code)]
    fn is_some(&self) -> bool {
        self.data.is_none()
    }

    fn len_left(&self) -> usize {
        match &self.data {
            Some(data) => data.len() - self.pos,
            None => 0,
        }
    }

    #[allow(dead_code)]
    fn len_total(&self) -> usize {
        match &self.data {
            Some(data) => data.len(),
            None => 0,
        }
    }

    fn get_slice(&mut self, len: usize) -> Vec<u8> {
        let pos = self.pos;
        let data = match &self.data {
            Some(data) => data,
            None => panic!("only call get_slice when data is present"),
        };
        if len > data.len() - pos {
            panic!("only call get_slice with length availale");
        }
        let ret = data[pos..pos + len].to_vec();
        if len == data.len() - pos {
            self.data = None;
            self.pos = 0
        } else {
            self.pos += len;
        }
        ret
    }
}

#[derive(Debug, Clone, Encode, Decode)]
pub struct RecordLocation {
    #[n(0)]
    archive_location: ArchiveLocation,
    #[n(1)]
    uncompressed_offset: usize,
}

impl RecordLocation {
    pub fn archive_set(&self) -> usize {
        self.archive_location.archive_set()
    }
    pub fn set_offset(&self) -> usize {
        self.archive_location.set_offset()
    }
    pub fn uncompressed_offset(&self) -> usize {
        self.uncompressed_offset
    }
}

#[derive(Clone)]
pub struct Record<T> {
    write_buffer: Vec<u8>,
    read_buffer: ReadBuf,
    limit: usize,
    read_offset: usize,
    archive: Archive,
    _marker: PhantomData<T>,
}

impl<T> std::fmt::Debug for Record<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let wbuf_len = self.write_buffer.len();
        let rbuf_len = self.write_buffer.len();
        f.debug_struct("Record")
            .field("wbuf.len()", &wbuf_len)
            .field("rbuf.len()", &rbuf_len)
            .field("limit", &self.limit)
            .field("read_offset", &self.read_offset)
            .field("archive", &self.archive)
            .finish()
    }
}

impl<T> Record<T> {
    /// Create a new record reader/writer
    pub fn new(archive: &str, record_type: String, file_limit: usize, record_limit: usize) -> Self {
        Record {
            write_buffer: Vec::new(),
            read_buffer: ReadBuf::new(),
            read_offset: 0,
            limit: record_limit,
            archive: Archive::new(archive, record_type, file_limit),
            _marker: PhantomData,
        }
    }

    pub fn task_counts(&self) -> (usize, usize) {
        self.archive.task_counts()
    }

    pub fn archive_set_write_serial_number(&mut self, num: usize) {
        self.archive.set_write_serial_number(num);
    }

    pub fn archive_get_read_serial_number(&mut self) -> usize {
        self.archive.get_read_serial_number()
    }

    /// push an item into the record, checking for need to flush
    ///
    ///   Note: can push any size item, even larger than record or
    ///   archive_set size, but this may not be efficient
    pub fn push(&mut self, v: Vec<u8>) -> Result<RecordLocation> {
        // if bigger than our size, we will be split so may as well
        // finish this record off
        if v.len() <= self.limit && (self.write_buffer.len() + v.len() > self.limit) {
            self.flush()?;
        }
        let ret = RecordLocation {
            archive_location: self.archive.write_location(),
            uncompressed_offset: self.write_buffer.len(),
        };
        // first push the length of the request
        self.write_buffer
            .extend_from_slice(&usize_to_slice_u8(v.len()));

        // start by writing full record segments, flushing each
        let mut vpos = 0;
        while (v.len() - vpos) > self.limit {
            let space = self.limit - self.write_buffer.len();
            self.write_buffer
                .extend_from_slice(&v[vpos..(vpos + space)]);
            self.flush()?;
            vpos += space;
        }
        self.write_buffer.extend_from_slice(&v[vpos..]);
        Ok(ret)
    }

    /// take a record full of data and move it to archive
    ///
    ///   We also can compress here
    pub fn flush(&mut self) -> Result<()> {
        if self.write_buffer.len() > 0 {
            let compressed = compress(&self.write_buffer, None, true)?;
            // write compressed length
            self.archive.write(&usize_to_slice_u8(compressed.len()))?;
            // write compressed data
            self.archive.write(&compressed)?;
            self.write_buffer = Vec::new();
        }
        Ok(())
    }

    /// async finish off any outstanding records
    ///
    ///  1. flush any records we have at this level
    ///  2. call finish at the archive level
    pub async fn finish(&mut self) -> Result<()> {
        self.flush()?;
        self.archive.finish().await
    }

    /// pull an item from the next record
    pub fn pull(&mut self) -> Result<Option<Vec<u8>>> {
        if self.read_buffer.is_none() {
            self.read_next_record()?;
        }

        if self.read_buffer.is_none() {
            return Ok(None);
        }

        let bytes_to_get = slice_u8_to_usize(&self.read_buffer.get_slice(4));

        while bytes_to_get > self.read_buffer.len_left() {
            self.read_next_record()?;
        }

        Ok(Some(self.read_buffer.get_slice(bytes_to_get)))
    }

    pub fn read_next_record(&mut self) -> Result<()> {
        if let Some(clenbuf) = self.archive.read(4)? {
            let clen = slice_u8_to_usize(clenbuf);
            if let Some(cbuf) = self.archive.read(clen)? {
                let ucbuf = decompress(cbuf, None)?;
                if self.read_buffer.is_none() {
                    self.read_buffer = ReadBuf::new_with_data(&ucbuf);
                } else {
                    self.read_buffer.extend_from_slice(&ucbuf);
                }
            } else {
                return Err(std::boxed::Box::new(Error::new(
                    ErrorKind::Other,
                    "No data in record after length?",
                )));
            }
        }
        Ok(())
    }
    /// seek to a specific record
    pub fn seek(&mut self, location: ArchiveLocation) -> Result<()> {
        self.archive.seek(location)?;
        Ok(())
    }
    /// backup an archive
    pub async fn backup(&self) -> Result<()> {
        self.archive.backup().await?;
        Ok(())
    }
}

fn slice_u8_to_usize(b: &[u8]) -> usize {
    (b[0] as usize) | (b[1] as usize) << 8 | (b[2] as usize) << 16 | (b[3] as usize) << 24
}

fn usize_to_slice_u8(len: usize) -> Vec<u8> {
    let mut v = Vec::new();
    v.push((len & 0xff) as u8);
    v.push(((len >> 8) & 0xff) as u8);
    v.push(((len >> 16) & 0xff) as u8);
    v.push(((len >> 24) & 0xff) as u8);
    v
}
