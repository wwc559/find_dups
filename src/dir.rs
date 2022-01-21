//! directory broker and support functions for wayback

use crate::{file::FileStore, Config, Result};
use async_std::fs;
use async_std::io;
use async_std::path::PathBuf;
use async_std::prelude::*;
use futures::channel::mpsc::{Receiver, Sender};
use futures::SinkExt;
use std::time::Instant;

#[derive(Debug)]
pub enum DirBrokerMessage {
    NewDir {
        path: PathBuf,
        depth: usize,
    },
    Error {
        e: io::Error,
    },
    Report,
    Done {
        files: usize,
        dirs: usize,
        errors: usize,
    },
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
    let file_store = FileStore::new(&config.archive, config.clone());

    if config.verbose > 0 {
        eprintln!("reading file archive");
    }

    file_store.read().await?;
    let initial_files = file_store.index().len();

    if config.verbose > 0 {
        eprintln!("initial_files: {}", initial_files);
    }

    let mut last_change_event = Instant::now();
    let mut last_file_count = 0;
    let mut last_nfiles = 0;
    let mut last_dir_count = 0;

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
                DirBrokerMessage::Done {
                    files,
                    dirs: _dirs,
                    errors,
                } => {
                    active_count -= 1;
                    error_count += errors;
                    file_count += files;
                }
                DirBrokerMessage::Report => {
                    let nfiles = file_store.index().len();
                    if nfiles > last_nfiles
                        || file_count > last_file_count
                        || dir_count > last_dir_count
                    {
                        last_nfiles = nfiles;
                        last_dir_count = dir_count;
                        last_file_count = file_count;
                        last_change_event = Instant::now();
                    }
                    if (active_count > 0 || nfiles > initial_files) && config.verbose > 0 {
                        eprintln!(
                            "files:{} dirs:{} nfiles:{} err:{} fps:{:.1} active:{}",
                            file_count,
                            dir_count,
                            file_store.index().len() - initial_files,
                            error_count,
                            (file_store.index().len() - initial_files) as f64 * 1000.0
                                / start.elapsed().as_millis() as f64,
                            active_count,
                        );
                    }
                    if last_change_event.elapsed().as_secs() > config.timeout {
                        eprintln!("stall detected, exiting");
                        if config.injest && nfiles > initial_files {
                            let last_report = Instant::now();
                            file_store.write().await?;
                            eprintln!(
                                "wrote file store in {} seconds",
                                last_report.elapsed().as_millis() as f64 / 1000.0
                            );
                        }
                        return Ok(());
                    }
                }
            }
        }

        // if we are not to busy, launch some work
        while !todo.is_empty() && active_count < config.concurrency {
            let (path, depth) = todo.pop().unwrap();
            crate::spawn_and_log_error(process_dir(
                path,
                depth,
                file_store.clone(),
                config.dir_broker_sender.clone(),
            ));
            active_count += 1;
            dir_count += 1;
        }

        // if we are done, finish up
        if active_count == 0 && todo.is_empty() {
            let nfiles = file_store.index().len();
            eprintln!(
                "completed {}: {} files in {} dirs with {} new entries, {} errors in {} seconds",
                if config.injest { "injest" } else { "check" },
                file_count,
                dir_count,
                nfiles - initial_files,
                error_count,
                start.elapsed().as_millis() as f64 / 1000.0
            );
            if config.injest && nfiles > initial_files {
                let last_report = Instant::now();
                file_store.write().await?;
                eprintln!(
                    "wrote file store in {} seconds",
                    last_report.elapsed().as_millis() as f64 / 1000.0
                );
            }
            if config.report {
                file_store.report().await?;
            }
            return Ok(());
        }
    }
}

pub async fn process_dir(
    path: PathBuf,
    depth: usize,
    file_store: FileStore,
    mut dir_broker_sender: Sender<DirBrokerMessage>,
) -> Result<()> {
    let mut dir = match fs::read_dir(path).await {
        Ok(r) => r,
        Err(e) => {
            if let Some(inner) = e.get_ref() {
                eprintln!("read_dir: {}", inner);
            }
            dir_broker_sender
                .send(DirBrokerMessage::Error { e })
                .await?;
            return Ok(());
        }
    };

    let mut files: usize = 0;
    let mut dirs: usize = 0;
    let mut errors: usize = 0;

    while let Some(res) = dir.next().await {
        let entry = res?;
        match entry.metadata().await {
            Ok(metadata) => {
                if metadata.is_dir() {
                    dir_broker_sender
                        .send(DirBrokerMessage::NewDir {
                            path: entry.path(),
                            depth: depth + 1,
                        })
                        .await?;
                    dirs += 1;
                } else {
                    match file_store.add_file(&entry.path(), &metadata).await {
                        Ok(()) => files += 1,
                        Err(e) => {
                            errors += 1;
                            eprintln!("add_file: {:?} ({})", e, entry.path().to_str().unwrap());
                        }
                    }
                }
            }
            Err(e) => {
                errors += 1;
                eprintln!("metadata: {:?} ({})", e, entry.path().to_str().unwrap());
            }
        }
    }
    dir_broker_sender
        .send(DirBrokerMessage::Done {
            files,
            dirs,
            errors,
        })
        .await?;
    Ok(())
}
