//! directory broker and support functions for wayback

use crate::{chunk::ChunkStore, file::FileStore, Config, Result};
use async_std::fs;
use async_std::io;
use async_std::path::PathBuf;
use async_std::prelude::*;
use futures::channel::mpsc::{Receiver, Sender};
use futures::SinkExt;
use std::time::Instant;

#[derive(Debug)]
pub enum DirBrokerMessage {
    NewDir { path: PathBuf, depth: usize },
    Error { e: io::Error },
    Report,
    Done { files: usize, dirs: usize },
    //ObjDone,
}

pub async fn dir_broker_loop(
    config: Config,
    mut incoming_messages: Receiver<DirBrokerMessage>,
) -> Result<()> {
    let mut todo: Vec<(PathBuf, usize)> = Vec::new();
    let mut active_count: usize = 0;
    let mut error_count: usize = 0;
    let mut dir_count: usize = 0;
    let mut file_count: usize = 0;
    let start = Instant::now();
    let chunk_store = ChunkStore::new(&config.archive);
    let file_store = FileStore::new(&config.archive);

    file_store.read().await?;
    //chunk_store.read().await?;

    let initial_files = file_store.index().len();
    println!("read nfile:{}", initial_files);

    loop {
        // wait for a message from someone ... can we hang here???
        if let Some(msg) = incoming_messages.next().await {
            match msg {
                DirBrokerMessage::NewDir { path, depth } => {
                    todo.push((path, depth));
                }
                DirBrokerMessage::Error { e: _e } => {
                    active_count -= 1;
                    error_count += 1;
                }
                DirBrokerMessage::Done { files, dirs: _dirs } => {
                    active_count -= 1;
                    file_count += files;
                }
                DirBrokerMessage::Report => {
                    println!(
                        "processed files:{} dirs:{} nfiles:{} err:{} fps:{:.1} active:{}",
                        file_count,
                        dir_count,
                        file_store.index().len() - initial_files,
                        error_count,
                        (file_store.index().len() - initial_files) as f64 * 1000.0
                            / start.elapsed().as_millis() as f64,
                        active_count,
                    );
                }
            }
        }

        // if we are not to busy, launch some work
        while !todo.is_empty() && active_count < 50 {
            let (path, depth) = todo.pop().unwrap();
            crate::spawn_and_log_error(process_dir(
                path,
                depth,
                file_store.clone(),
                chunk_store.clone(),
                config.dir_broker_sender.clone(),
            ));
            active_count += 1;
            dir_count += 1;
        }

        // if we are done, finish up
        if active_count == 0 && todo.is_empty() {
            let nfiles = file_store.index().len();
            println!(
                "completed injest, scanned {} files in {} dirs with {} new entries, {} errors in {} seconds",
                file_count,
                dir_count,
                nfiles-initial_files,
                error_count,
                start.elapsed().as_millis() as f64 / 1000.0
            );
            if nfiles > initial_files {
                let last_report = Instant::now();
                file_store.write().await?;
                //chunk_store.write().await?;
                println!(
                    "wrote file store in {} seconds",
                    last_report.elapsed().as_millis() as f64 / 1000.0
                );
            }
            return Ok(());
        }
    }
}

pub async fn process_dir(
    path: PathBuf,
    depth: usize,
    file_store: FileStore,
    chunk_store: ChunkStore,
    mut dir_broker_sender: Sender<DirBrokerMessage>,
) -> Result<()> {
    let mut dir = match fs::read_dir(path).await {
        Ok(r) => r,
        Err(e) => {
            dir_broker_sender
                .send(DirBrokerMessage::Error { e })
                .await?;
            return Ok(());
        }
    };

    let mut files: usize = 0;
    let mut dirs: usize = 0;

    while let Some(res) = dir.next().await {
        let entry = res?;
        let metadata = entry.metadata().await?;
        if metadata.is_dir() {
            dir_broker_sender
                .send(DirBrokerMessage::NewDir {
                    path: entry.path(),
                    depth: depth + 1,
                })
                .await?;
            dirs += 1;
        } else {
            file_store
                .add_file(&entry.path(), &metadata, &chunk_store)
                .await?;
            files += 1;
            /*
                println!(
            "{} {:?} {:?}",
            entry.path().to_string_lossy(),
            metadata.file_type(),
            metadata.modified()
                 );
                */
        }
    }
    dir_broker_sender
        .send(DirBrokerMessage::Done { files, dirs })
        .await?;
    Ok(())
}
