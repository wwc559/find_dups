//! top level library for finddupes

#![warn(
    //missing_docs,
    future_incompatible,
    missing_debug_implementations,
    rust_2018_idioms,
)]

use crate::dir::{dir_broker_loop, DirBrokerMessage};
use async_std::path::PathBuf;
use async_std::prelude::*;
use async_std::task;
use clap::ArgMatches;
use futures::channel::mpsc::{channel, Receiver, Sender};
use futures::SinkExt;
use std::time::Duration;

pub mod archive;
pub mod chunk;
pub mod dir;
pub mod file;
pub mod record;

pub type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

pub const RECORD_SIZE: usize = 64 * 1024;
pub const CHUNK_SIZE: usize = 64 * 1024;
pub const MAX_COMPRESSED_CHUNK_SIZE: usize = (64 * 1024) + 384; // allow for LZ4 worst case
pub const ARCHIVE_SIZE: usize = 4 * 1024 * 1024;

#[derive(Clone, Debug)]
pub struct Config {
    archive: String,
    dir_broker_sender: Sender<DirBrokerMessage>,
    //obj_broker_sender: Sender<ObjBrokerMessage>,
}

impl Config {
    pub fn new(matches: &ArgMatches) -> (Self, Receiver<DirBrokerMessage>) {
        let (dir_broker_sender, dir_broker_receiver) = channel(100);
        //let (obj_broker_sender, obj_broker_receiver) = channel(100);
        let archive: String = matches
            .value_of("archive")
            .expect("need to specify archive")
            .to_string();
        (
            Config {
                archive: archive.to_string(),
                dir_broker_sender,
                //obj_broker_sender,
            },
            dir_broker_receiver,
            //obj_broker_receiver,
        )
    }
}

pub trait ItemReadWrite {
    type T;
    fn write_item(&mut self, item: &Self::T) -> Result<record::RecordLocation>;
    fn read_item(&mut self) -> Result<Option<Self::T>>;
}

pub async fn launch_brokers(
    config: Config,
    dir_receiver: Receiver<DirBrokerMessage>,
    injests: Vec<&str>,
) -> Result<()> {
    let mut sender = config.dir_broker_sender.clone();
    for injest in injests {
        sender
            .send(DirBrokerMessage::NewDir {
                path: PathBuf::from(injest),
                depth: 0,
            })
            .await?
    }
    let d = spawn_and_log_error(dir_broker_loop(config.clone(), dir_receiver));
    let t = spawn_and_log_error(timer_broker_loop(config.clone()));
    d.await;
    t.cancel().await;
    Ok(())
}

/// Timer loop, simply sends Report messages to other loops
/// periodcially.  Need to mark as allow unreachable because this task
/// is simply canceled after other loops exit.
#[allow(unreachable_code)]
pub async fn timer_broker_loop(config: Config) -> Result<()> {
    loop {
        task::sleep(Duration::from_millis(1500)).await;
        config
            .dir_broker_sender
            .clone()
            .send(DirBrokerMessage::Report)
            .await?;
    }
    Ok(())
}

pub fn spawn_and_log_error<F>(fut: F) -> task::JoinHandle<()>
where
    F: Future<Output = Result<()>> + Send + 'static,
{
    task::spawn(async move {
        if let Err(e) = fut.await {
            eprintln!("{}", e)
        }
    })
}
