//! file functions for wayback

use crate::{
    record::Record,
    record::RecordLocation,
    ItemReadWrite, Result, ARCHIVE_SIZE, RECORD_SIZE, CHUNK_SIZE,
};
use async_std::fs::{Metadata, File};
use async_std::path::PathBuf;
use async_std::prelude::*;
use async_std::sync::Arc;
use dashmap::DashMap;
use minicbor_derive::{Decode, Encode};
use std::io::{Error, ErrorKind};
use std::time::UNIX_EPOCH;

pub type ChunkHash = u64;
// inspired by github:://rsdy/zerostash/libzerostash/file.rs

#[derive(Hash, Clone, Eq, PartialEq, Default, Debug, Encode, Decode)]
pub struct Entry {
    #[n(0)]
    perm: u32,
    #[n(1)]
    uid: u32,
    #[n(2)]
    gid: u32,

    #[n(3)]
    mod_secs: u64,
    #[n(4)]
    mod_nanos: u32,

    #[n(5)]
    is_file: bool,
    #[n(6)]
    is_dir: bool,

    #[n(7)]
    len: u64,
    #[n(8)]
    name: String,
}

impl Entry {
    pub fn new_from_path_meta(path: &PathBuf, metadata: &Metadata) -> Result<Self> {
        use std::os::unix::fs::{MetadataExt, PermissionsExt};

        let mtime = metadata.modified()?.duration_since(UNIX_EPOCH)?;
        let perms = metadata.permissions();
        Ok(Entry {
            perm: perms.mode(),
            uid: metadata.uid(),
            gid: metadata.gid(),

            mod_secs: mtime.as_secs(),
            mod_nanos: mtime.subsec_nanos(),

            is_file: metadata.is_file(),
            is_dir: metadata.is_dir(),

            len: metadata.len(),
            name: path.to_str().unwrap().to_string(),
        })
    }
}

pub type FileIndex = DashMap<Arc<Entry>, Vec<ChunkHash>>;
pub type FileTuple = (Arc<Entry>, Vec<ChunkHash>);

#[derive(Clone, Debug)]
pub struct FileStore {
    index: Arc<FileIndex>,
    record: crate::record::Record<FileTuple>,
}

impl FileStore {
    pub fn new(archive: &String) -> Self {
        FileStore {
            index: Arc::new(FileIndex::new()),
            record: crate::record::Record::new(
                archive,
                "file".to_string(),
                ARCHIVE_SIZE,
                RECORD_SIZE,
            ),
        }
    }

    pub fn index(&self) -> &FileIndex {
        &self.index
    }

    pub async fn add_file(
        &self,
        path: &PathBuf,
        metadata: &Metadata,
    ) -> Result<()> {
        let entry = Entry::new_from_path_meta(path, metadata)?;
        // see if we already have this, if so, we are done
        if self.index.contains_key(&entry) {
            return Ok(());
        }

        let chunks = if entry.is_file {
            hash_file(path, entry.len).await?
        } else if entry.is_dir {
            Vec::new()
        } else {
            Vec::new() // Sym link, need to figure it out
        };

        self.index.insert(Arc::new(entry), chunks);
        Ok(())
    }

    pub async fn write(&self) -> Result<()> {
        let mut record = self.record.clone();
        for item in self.index.iter() {
            record.write_item(&(item.key().clone(), item.value().to_vec()))?;
        }
        record.finish().await?;
        Ok(())
    }

    pub async fn read(&self) -> Result<()> {
        let mut record = self.record.clone();
        loop {
            match record.read_item() {
                Ok(Some((i0, i1))) => {
                    //println!("got {}, {} chunks", i0.name, i1.len());
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

async fn hash_file(path: &PathBuf, len:u64) -> Result<Vec<ChunkHash>> {
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


impl ItemReadWrite for Record<FileTuple> {
    type T = FileTuple;
    fn write_item(&mut self, item: &Self::T) -> Result<RecordLocation> {
        let loc = self.push(minicbor::to_vec(&item.0.as_ref())?)?;
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
