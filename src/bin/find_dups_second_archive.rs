use async_std::task;
use clap::{app_from_crate, arg};

use find_dups::{file::FileStore, Config};

fn main() {
    let matches = app_from_crate!()
        .arg(
            arg!(-m --missing "Report second archive files which are missing from archive")
                .required(false)
                .conflicts_with("present"),
        )
        .arg(
            arg!(-p --present "Report second archive files which are present in archive")
                .required(false)
                .conflicts_with("missing"),
        )
        .arg(
            arg!(-a --archive <path> "Path to archive")
                .required(false)
                .default_value("/tmp/finddups"),
        )
        .arg(arg!(-s --second_archive <path> "Path to second archive").required(false))
        .arg(arg!(-r --report "Produce a report summarizing duplicate files").required(false))
        .arg(arg!(-v --verbose ... "increase verbosity level").required(false))
        .get_matches();

    // Get the configuration
    let (config, _dir_receiver) = Config::new(&matches);

    let file_store1 = FileStore::new(matches.value_of("archive").unwrap(), config.clone());
    let file_store2 = FileStore::new(matches.value_of("second_archive").unwrap(), config.clone());

    task::block_on(async {
        file_store1.read().await.expect("fs1 read");
        file_store2.read().await.expect("fs2 read")
    });

    file_store1
        .find_dups_second_archive(&file_store2)
        .expect("find_dups_second_archive");

    // All done!
}
