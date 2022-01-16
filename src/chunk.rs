//! chunk functions for wayback

use crate::{
    record::{Record, RecordLocation},
    ItemReadWrite, Result, ARCHIVE_SIZE, CHUNK_SIZE,
};
use async_std::fs::File;
use async_std::path::PathBuf;
use async_std::prelude::*;
use async_std::sync::Arc;
use dashmap::DashMap;
use std::io::{Error, ErrorKind};

pub type ChunkHash = u64;
pub type ChunkIndex = DashMap<Arc<ChunkHash>, RecordLocation>;
pub type ChunkTuple = (Arc<ChunkHash>, RecordLocation);

#[derive(Debug, Clone)]
pub struct ChunkStore {
    index: Arc<ChunkIndex>,
    record: crate::record::Record<ChunkTuple>,
}

impl ChunkStore {
    pub fn new(archive: &String) -> Self {
        ChunkStore {
            index: Arc::new(ChunkIndex::new()),
            record: crate::record::Record::new(
                archive,
                "chunk".to_string(),
                ARCHIVE_SIZE,
                CHUNK_SIZE,
            ),
        }
    }

    pub fn index(&self) -> &ChunkIndex {
        &self.index
    }

    pub async fn add_file(&self, path: &PathBuf, len: u64) -> Result<Vec<ChunkHash>> {
        let mut ret: Vec<ChunkHash> = Vec::new();

        match File::open(path).await {
            Ok(mut f) => {
                let mut pos = 0;
                // first we store full CHUNK_SIZE chunks until only partial one left
                while pos + CHUNK_SIZE < len as usize {
                    let mut buf = vec![0; CHUNK_SIZE];
                    f.read_exact(&mut buf).await?;
                    ret.push(seahash::hash(&buf));
                    pos += CHUNK_SIZE;
                }

                let mut buf = Vec::new();
                f.read_to_end(&mut buf).await?;
                ret.push(seahash::hash(&buf));
            }
            Err(e) => eprintln!("{} while adding file", e),
        }
        Ok(ret)
    }

    pub async fn write(&self) -> Result<()> {
        let mut record = self.record.clone();
        for item in self.index.iter() {
            record.write_item(&(item.key().clone(), item.value().clone()))?;
        }
        record.finish().await?;
        Ok(())
    }

    pub async fn read(&self) -> Result<()> {
        let mut record = self.record.clone();
        loop {
            match record.read_item() {
                Ok(Some((i0, i1))) => {
                    self.index.insert(i0, i1);
                }
                Ok(None) => {
                    break;
                }
                Err(e) => {
                    eprintln!("{}", e);
                    break;
                }
            }
        }
        Ok(())
    }
}

impl ItemReadWrite for Record<Arc<Vec<u8>>> {
    type T = Arc<Vec<u8>>;
    fn write_item(&mut self, item: &Self::T) -> Result<RecordLocation> {
        let loc = self.push(item.to_vec())?;
        Ok(loc)
    }
    fn read_item(&mut self) -> Result<Option<Self::T>> {
        if let Some(t) = self.pull()? {
            Ok(Some(Arc::new(t.to_vec())))
        } else {
            Ok(None)
        }
    }
}

impl ItemReadWrite for Record<ChunkTuple> {
    type T = ChunkTuple;
    fn write_item(&mut self, item: &Self::T) -> Result<RecordLocation> {
        let loc = self.push(minicbor::to_vec(item.0.as_ref())?)?;
        self.push(minicbor::to_vec(&item.1)?)?;
        Ok(loc)
    }
    fn read_item(&mut self) -> Result<Option<Self::T>> {
        if let Some(v0) = &self.pull()? {
            let i0 = minicbor::decode(v0)?;
            if let Some(v1) = &self.pull()? {
                let i1 = minicbor::decode(v1)?;
                Ok(Some((Arc::new(i0), i1)))
            } else {
                Err(std::boxed::Box::new(Error::new(
                    ErrorKind::Other,
                    "Out of data half way through read_item?",
                )))
            }
        } else {
            Ok(None)
        }
    }
}
