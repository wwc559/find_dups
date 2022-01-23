//! file functions for wayback

use crate::{
    record::Record, record::RecordLocation, Config, ItemReadWrite, Result, ARCHIVE_SIZE,
    CHUNK_SIZE, RECORD_SIZE,
};
use async_std::fs::{File, Metadata};
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

pub type FileIndex = DashMap<Arc<Entry>, ChunkHash>;
pub type HashIndex = DashMap<ChunkHash, Vec<Arc<Entry>>>;
pub type FileTuple = (Arc<Entry>, ChunkHash);

#[derive(Clone, Debug)]
pub struct FileStore {
    index: Arc<FileIndex>,
    hindex: Arc<HashIndex>,
    record: crate::record::Record<FileTuple>,
    config: Config,
}

impl FileStore {
    pub fn new(archive: &String, config: Config) -> Self {
        FileStore {
            index: Arc::new(FileIndex::new()),
            hindex: Arc::new(HashIndex::new()),
            record: crate::record::Record::new(
                archive,
                "file".to_string(),
                ARCHIVE_SIZE,
                RECORD_SIZE,
            ),
            config: config,
        }
    }

    pub fn index(&self) -> &FileIndex {
        &self.index
    }

    pub async fn add_file(&self, path: &PathBuf, metadata: &Metadata) -> Result<()> {
        let entry = Entry::new_from_path_meta(path, metadata)?;
        // see if we already have this
        if self.index.contains_key(&entry) {
            // if we are checking, we need to see if there are at least 2 entries
            if self.config.present || self.config.missing {
                let hash = self.index.get(&entry).unwrap();
                let files = self.hindex.get(&hash).unwrap();
                if files.len() >= 2 {
                    if self.config.present {
                        if self.config.verbose > 1 {
                            println!("{} is present in archive", entry.name);
                        } else {
                            println!("{}", entry.name);
                        }
                    } else if self.config.duplicate {
                        let names: Vec<String> = files.iter().map(|f| f.name.clone()).collect();
                        if self.config.verbose > 1 {
                            println!("Archive files matching: {}", names.join(", "));
                        } else {
                            println!("{}", names.join("\n"));
                        }
                    }
                }
                if files.len() < 2 && self.config.missing {
                    if self.config.verbose > 1 {
                        println!("{} is not present in archive", entry.name);
                    } else {
                        println!("{}", entry.name);
                    }
                }
            }
        } else {
            let hash = if entry.is_file {
                let vec = hash_file(path, entry.len).await?;
                vec.iter().fold(entry.len, |acc, x| acc ^ x)
            } else if entry.is_dir {
                0
            } else {
                0
            };

            // if we are checking, we need to see if it is already in the hash
            if self.config.present || self.config.missing || self.config.duplicate {
                let is_present = self.hindex.contains_key(&hash);
                if is_present {
                    if self.config.present {
                        if self.config.verbose > 1 {
                            println!("{} is present in archive", entry.name);
                        } else {
                            println!("{}", entry.name);
                        }
                    } else if self.config.duplicate {
                        let files = self.hindex.get(&hash).unwrap();
                        let names: Vec<String> = files.iter().map(|f| f.name.clone()).collect();
                        if self.config.verbose > 1 {
                            println!("Archive files matching: {}", names.join(", "));
                        } else {
                            println!("{}", names.join("\n"));
                        }
                    }
                    if self.config.verbose > 1 {
                        println!("{} is present in archive", entry.name);
                    } else {
                        println!("{}", entry.name);
                    }
                }
                if !is_present && self.config.missing {
                    if self.config.verbose > 1 {
                        println!("{} is not present in archive", entry.name);
                    } else {
                        println!("{}", entry.name);
                    }
                }
            }

            if self.config.injest {
                self.index.insert(Arc::new(entry), hash);
            }
        }
        Ok(())
    }

    pub async fn write(&self) -> Result<()> {
        let mut record = self.record.clone();
        for item in self.index.iter() {
            record.write_item(&(item.key().clone(), *item.value()))?;
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
                    self.index.insert(i0.clone(), i1);
                    let newval = if self.hindex.contains_key(&i1) {
                        let (_key, mut vec) = self.hindex.remove(&i1).unwrap();
                        vec.push(i0);
                        vec
                    } else {
                        vec![i0]
                    };
                    self.hindex.insert(i1, newval);
                }
                Ok(None) => {
                    break;
                }
                Err(e) => {
                    eprintln!("read file::Entry:{}", e);
                    break;
                }
            }
        }
        Ok(())
    }

    pub async fn report(&self) -> Result<()> {
        let mut ndup_big = 0;
        let mut ndup = 0;
        let mut total_size = 0;
        for item in self.hindex.iter() {
            let files = item.value();
            if files.len() > 1 {
                if self.config.duplicate && self.config.injest {
                    // if we are not checking and are reporting duplicates
                    // do so here
                    let names: Vec<String> = files.iter().map(|f| f.name.clone()).collect();
                    if self.config.verbose > 1 {
                        println!("Archive duplicates: {}", names.join(", "));
                    } else {
                        println!("{}", names.join("\n"));
                    }
                }
                ndup += 1;
                total_size += files[0].len * (files.len() - 1) as u64;
                if files[0].len > 1000000 {
                    ndup_big += 1;
                }
            }
        }
        println!(
            "{} dup, {} dup big, {} total Gbytes dup",
            ndup,
            ndup_big,
            total_size / (1000 * 1000 * 1000)
        );
        return Ok(());
    }
}

async fn hash_file(path: &PathBuf, len: u64) -> Result<Vec<ChunkHash>> {
    let mut ret: Vec<ChunkHash> = Vec::new();
    let mut f = File::open(path).await?;
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
